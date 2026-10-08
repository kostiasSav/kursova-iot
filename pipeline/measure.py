#!/usr/bin/env python3
"""
Стадія 2 — обов'язкові вимірювання конвеєра (п. 4.2 методичних вказівок).

Один запуск вимірює все, що треба навести у звіті:

  1. обсяг даних у форматах CSV і Parquet та коефіцієнт стиснення;
  2. пропускну здатність завантаження у ваше сховище, записів/с;
  3. розмір сховища на диску;
  4. час повного сканування та вибірки за одним пристроєм;
  5. пікове споживання оперативної пам'яті;
  6. межу пам'яті: скільки байтів займає один запис, коли ВЕСЬ файл
     прочитано у pandas, і скільки записів поміститься в оперативну
     пам'ять цього комп'ютера. З ключем --scale-test — ще й дослід
     з 1×, 2× і 4× копіями даних: видно, що пам'ять росте лінійно.

Результат — таблиця Markdown (вставляється у пояснювальну записку як є),
яка друкується на екран і зберігається у measure_<сховище>.md, плюс ті самі
числа у measure_<сховище>.csv.

Запуск із кореня репозиторію:
    python pipeline/measure.py --storage "DuckDB + Parquet" --data tuning.parquet
    python pipeline/measure.py --data variant_047.parquet    # сховище з variant_047.json
    python pipeline/measure.py --storage InfluxDB --data variant_047.parquet --scale-test

InfluxDB і TimescaleDB працюють у Docker, тож для них спершу:
    docker compose -f pipeline/docker-compose.yml up -d

Увага: як і storage.load_parquet(), скрипт ЗАМІНЮЄ дані у сховищі (bucket iot
у InfluxDB, таблиця readings у TimescaleDB) вмістом файла --data. Міряйте
свій варіант — тоді після вимірювань у сховищі лишиться саме він.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import json
import multiprocessing
import platform
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

try:
    import psutil
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.dataset as pads
    import pyarrow.parquet as pq
    from storage import (FLUX_MAX, FLUX_MIN, TABLE, StorageError,
                         normalise_kind, open_storage)
except ImportError as exc:
    sys.exit(f"Не встановлено пакет «{exc.name}». У своєму venv виконайте:\n"
             f"    pip install -r requirements.txt psutil")

BATCH_ROWS = 500_000          # порція читання, як у storage.py
SCALE_COPIES = (1, 2, 4)      # скільки копій даних бере --scale-test

# «Повне сканування» — запит, якому треба прочитати КОЖЕН запис: кількість
# і середнє значення за кожним показником. Рахує саме сховище, а в Python
# повертається лише рядок на показник — так і працює аналітика у сховищі.
SCAN_SQL = f"SELECT metric, count(*) AS n, avg(value) AS mean FROM {TABLE} GROUP BY metric"


# ======================================================================
# Пам'ять
# ======================================================================

class PeakMemory:
    """Раз на 5 мс зчитує пам'ять процесу (RSS) і запам'ятовує максимум.

    psutil однаково працює на Windows, macOS і Linux; модуль resource,
    яким пікову пам'ять міряють у підручниках, на Windows відсутній.
    """

    def __init__(self, interval: float = 0.005) -> None:
        self._proc = psutil.Process()
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self.start = self.peak = self._proc.memory_info().rss

    def _watch(self) -> None:
        while not self._stop.is_set():
            self.peak = max(self.peak, self._proc.memory_info().rss)
            self._stop.wait(self._interval)

    def __enter__(self) -> "PeakMemory":
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        self._thread.join()
        self.peak = max(self.peak, self._proc.memory_info().rss)


def pandas_footprint(path: str) -> dict:
    """Читає ВЕСЬ файл у pandas і міряє, скільки пам'яті це забрало.

    Запускається в окремому чистому процесі, тож пам'ять, яку раніше
    зайняло сховище, у вимірювання не потрапляє.
    """
    import pandas as pd

    # «Холостий» запуск: фільтр не пропускає жодного рядка, тож дані не
    # читаються, але pandas довантажує свої модулі — інакше їхні мегабайти
    # записалися б на рахунок даних.
    pd.read_parquet(path, filters=[("ts", "<", 0)])
    started = time.perf_counter()
    with PeakMemory() as mem:
        frame = pd.read_parquet(path)
    return {"rows": len(frame), "seconds": time.perf_counter() - started,
            "frame_bytes": int(frame.memory_usage(deep=True).sum()),
            "peak_bytes": mem.peak - mem.start}


def in_fresh_process(func, *args):
    """Виконує func(*args) в окремому, щойно запущеному процесі Python."""
    ctx = multiprocessing.get_context("spawn")     # однаково на всіх ОС
    with ProcessPoolExecutor(max_workers=1, mp_context=ctx) as pool:
        return pool.submit(func, *args).result()


def copies_file(data: Path, tmp: Path, copies: int) -> Path:
    """Parquet із copies копіями набору поспіль — для --scale-test."""
    if copies == 1:
        return data
    source = pq.ParquetFile(data)
    out = tmp / f"{data.stem}_x{copies}.parquet"
    with pq.ParquetWriter(out, source.schema_arrow, compression="zstd") as writer:
        for _ in range(copies):
            for group in range(source.num_row_groups):   # по одній групі рядків
                writer.write_table(source.read_row_group(group))
    return out


def run_pandas(data: Path, tmp: Path, scale_test: bool) -> tuple[dict, list[dict]]:
    try:
        full = in_fresh_process(pandas_footprint, str(data))
        scale = []
        for copies in (SCALE_COPIES if scale_test else ()):
            path = copies_file(data, tmp, copies)
            one = full if copies == 1 else in_fresh_process(pandas_footprint, str(path))
            scale.append({**one, "copies": copies, "file_bytes": path.stat().st_size})
            if path != data:
                path.unlink()                  # копії великі — прибираємо одразу
    except ImportError:
        raise StorageError("Для оцінки пам'яті потрібен pandas:  pip install pandas") from None
    except (MemoryError, BrokenProcessPool):
        raise StorageError("Процес, що читав файл у pandas, завершився аварійно — найімовірніше, "
                           "забракло оперативної пам'яті. Це і є межа, про яку п. 4.2: "
                           "запишіть, на якому обсязі вона настала.") from None
    return full, scale


# ======================================================================
# Вимірювання сховища
# ======================================================================

def csv_vs_parquet(data: Path, tmp: Path) -> dict:
    """Записує копію набору у CSV порціями, міряє обидва файли і видаляє копію."""
    csv_file = tmp / f"{data.stem}.csv"
    reader = pq.ParquetFile(data)

    def write(quoting: str) -> None:
        options = pacsv.WriteOptions(quoting_style=quoting)
        with pacsv.CSVWriter(csv_file, reader.schema_arrow, write_options=options) as out:
            for batch in reader.iter_batches(batch_size=BATCH_ROWS):
                out.write_batch(batch)

    started = time.perf_counter()
    try:
        write("none")          # звичайний CSV без лапок, як із pandas.to_csv
    except pa.ArrowInvalid:
        write("needed")        # у назвах є кома чи лапки — тоді з лапками
    seconds = time.perf_counter() - started
    csv_bytes = csv_file.stat().st_size
    csv_file.unlink()
    parquet_bytes = data.stat().st_size
    return {"csv_bytes": csv_bytes, "parquet_bytes": parquet_bytes,
            "ratio": csv_bytes / parquet_bytes, "csv_seconds": seconds}


def device_rows(data: Path, device: str | None) -> tuple[str, dict[str, int]]:
    """Пристрій для вибірки і скільки записів кожного показника він має у Parquet."""
    dataset = pads.dataset(data)
    if device is None:   # типово — пристрій із першого рядка файла
        first = next(pq.ParquetFile(data).iter_batches(batch_size=1, columns=["device_id"]))
        device = str(first.column(0)[0].as_py())
    found = dataset.to_table(columns=["metric"], filter=pads.field("device_id") == device)
    if found.num_rows == 0:
        known = dataset.to_table(columns=["device_id"]).column(0).unique()
        known = sorted(str(d) for d in known.to_pylist())
        near = difflib.get_close_matches(device, known, n=3, cutoff=0.5)
        raise StorageError(f"Пристрою {device!r} немає у {data.name}. "
                           f"Можливо, ви мали на увазі: {', '.join(near or known[:5])}")
    counts = found.group_by("metric").aggregate([("metric", "count")])
    return device, dict(zip(map(str, counts["metric"].to_pylist()),
                            counts["metric_count"].to_pylist()))


def full_scan(st) -> int:
    """Повне сканування в сховищі; повертає кількість рядків результату."""
    if st.kind == "influxdb":
        # Адаптер InfluxDB не має публічного «сирого» запиту, тож беремо його
        # внутрішні _bucket і _stream — той самий клієнт, яким працює series().
        flux = (f'from(bucket: "{st._bucket}") '
                f'|> range(start: {FLUX_MIN}, stop: {FLUX_MAX}) '
                f'|> filter(fn: (r) => r._field == "value") '
                f'|> group(columns: ["_measurement"]) |> mean()')
        return len(list(st._stream(flux)))
    # DuckDB, SQLite і psycopg (TimescaleDB) однаково вміють execute().fetchall()
    return len(st.connection.execute(SCAN_SQL).fetchall())


def server_size(st) -> tuple[int, str]:
    """Розмір даних у сховищі-сервері (InfluxDB чи TimescaleDB у Docker)."""
    if st.kind == "timescaledb":
        try:
            size = st.connection.execute(f"SELECT hypertable_size('{TABLE}')").fetchone()[0]
        except Exception as exc:
            raise StorageError(f"Не вдалося виміряти таблицю {TABLE} — дані завантажені?\n"
                               f"Деталі: {exc}") from exc
        return int(size or 0), f"hypertable «{TABLE}» разом з індексами"
    # InfluxDB сам рахує свої файли і показує їх на сторінці /metrics:
    # TSM — стиснені дані, WAL — журнал щойно записаного.
    try:
        bucket = st._client.buckets_api().find_bucket_by_name(st._bucket)
        with urllib.request.urlopen(f"{st._url}/metrics", timeout=30) as response:
            lines = response.read().decode("utf-8").splitlines()
    except Exception as exc:
        raise StorageError(st._conn_hint(exc)) from exc
    if bucket is None:
        raise StorageError(f"У InfluxDB немає bucket «{st._bucket}» — спершу завантажте дані.")
    tsm = wal = 0.0
    for line in lines:
        if f'bucket="{bucket.id}"' in line:
            if line.startswith("storage_tsm_files_disk_bytes{"):
                tsm += float(line.rsplit(" ", 1)[1])
            elif line.startswith("storage_wal_size{"):
                wal += float(line.rsplit(" ", 1)[1])
    return int(tsm + wal), f"bucket «{st._bucket}»: TSM {mb(tsm)} + журнал WAL {mb(wal)}"


def drop_influx_bucket(st) -> None:
    """Видаляє bucket цілком; load_parquet() створить його наново.

    load_parquet() і сам очищає bucket, але InfluxDB при цьому лише позначає
    старі точки видаленими, а файли з ними лишаються на диску ще години —
    і повторне вимірювання показало б завищений розмір.
    """
    try:
        api = st._client.buckets_api()
        bucket = api.find_bucket_by_name(st._bucket)
        if bucket is not None:
            api.delete_bucket(bucket)
    except Exception as exc:
        raise StorageError(st._conn_hint(exc)) from exc


def server_options(kind: str, args: argparse.Namespace) -> dict:
    if kind == "influxdb":
        return {"url": args.url, "bucket": args.bucket}
    return {"dsn": args.dsn} if args.dsn else {}


def timed(func, repeat: int):
    """Виконує func() repeat разів; повертає результат і медіану часу."""
    times, result = [], None
    for _ in range(max(1, repeat)):
        started = time.perf_counter()
        result = func()
        times.append(time.perf_counter() - started)
    return result, statistics.median(times)


def run_storage(name: str, kind: str, args: argparse.Namespace, data: Path, tmp: Path,
                device: str, metrics: list[str]) -> dict:
    """Завантаження у сховище і запити до нього; повертає виміряні числа."""
    if kind == "duckdb":
        # materialise=True: дані переносяться у файл бази, тож є що міряти
        options = {"path": tmp / "measure.duckdb", "materialise": True}
    elif kind == "sqlite":
        options = {"path": tmp / "measure.sqlite"}
    else:
        options = server_options(kind, args)

    pa.default_memory_pool().release_unused()   # повернути ОС пам'ять попередніх кроків
    out: dict = {}
    with PeakMemory() as mem:
        st = open_storage(name, **options)
        try:
            if kind == "influxdb":
                drop_influx_bucket(st)
            started = time.perf_counter()
            out["loaded"] = st.load_parquet(data)
            out["load_s"] = time.perf_counter() - started
            print("[3/4] Запити до сховища…", flush=True)
            out["scan_rows"], out["scan_s"] = timed(lambda: full_scan(st), args.repeat)
            out["device_rows"], out["device_s"] = timed(
                lambda: sum(len(st.series(device, m)[0]) for m in metrics), args.repeat)
            if kind in ("influxdb", "timescaledb"):
                out["size"], out["size_note"] = server_size(st)
        finally:
            st.close()
    if kind in ("duckdb", "sqlite"):
        db = options["path"]       # міряємо після close(), коли все записано у файл
        out["size"] = sum(f.stat().st_size for f in (db, db.with_name(db.name + ".wal"))
                          if f.exists())
        out["size_note"] = f"файл бази {kind}"
    out["peak"], out["start"] = mem.peak, mem.start
    return out


# ======================================================================
# Опис машини і форматування чисел
# ======================================================================

def cpu_name() -> str:
    """Назва процесора: кожна ОС зберігає її по-своєму."""
    try:
        if sys.platform == "win32":
            import winreg
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                 r"HARDWARE\DESCRIPTION\System\CentralProcessor\0")
            return str(winreg.QueryValueEx(key, "ProcessorNameString")[0]).strip()
        if sys.platform == "darwin":
            return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                  capture_output=True, text=True, check=True).stdout.strip()
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.lower().startswith(("model name", "hardware", "model\t")):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return platform.processor() or platform.machine()


def os_name() -> str:
    if sys.platform == "darwin":
        return f"macOS {platform.mac_ver()[0]}"
    if sys.platform == "win32":
        return f"Windows {platform.release()} ({platform.version()})"
    try:
        return platform.freedesktop_os_release().get("PRETTY_NAME", "Linux")
    except OSError:
        return f"Linux {platform.release()}"


def machine() -> str:
    cores = psutil.cpu_count(logical=False) or "?"
    ram_gb = psutil.virtual_memory().total / 2**30
    return (f"{cpu_name()} ({cores} ядер / {psutil.cpu_count()} потоків), "
            f"RAM {num(ram_gb, 1)} ГБ, {os_name()} ({platform.machine()}), "
            f"Python {platform.python_version()}")


def num(x: float, digits: int = 0) -> str:
    """Число в українському записі: 1 234 567,8."""
    return f"{x:,.{digits}f}".replace(",", " ").replace(".", ",")


def mb(n: float) -> str:
    return f"{num(n / 1e6, 1)} МБ"


def many(n: float) -> str:
    return f"{num(n / 1e9, 1)} млрд" if n >= 1e9 else f"{num(n / 1e6, 0)} млн"


# ======================================================================
# Звіт
# ======================================================================

def report(name: str, kind: str, data: Path, host: str, rows: int, device: str,
           sizes: dict, st: dict, full: dict, scale: list[dict], repeat: int) -> tuple:
    """Таблиця Markdown і рядки CSV (ключ, назва, число, одиниця)."""
    ram = psutil.virtual_memory().total
    per_row = full["peak_bytes"] / full["rows"]
    fit = ram / per_row
    # (ключ для CSV, назва, як показати у Markdown, число для CSV, одиниця)
    table = [
        ("rows", "Записів у наборі", num(rows), rows, ""),
        ("csv_bytes", "Обсяг у CSV", mb(sizes["csv_bytes"]), sizes["csv_bytes"], "B"),
        ("parquet_bytes", "Обсяг у Parquet", mb(sizes["parquet_bytes"]),
         sizes["parquet_bytes"], "B"),
        ("compression_ratio", "Коефіцієнт стиснення (CSV / Parquet)",
         num(sizes["ratio"], 1), round(sizes["ratio"], 2), ""),
        ("load_rows_per_s", "Пропускна здатність завантаження",
         f"{num(st['loaded'] / st['load_s'])} записів/с "
         f"({num(st['loaded'])} за {num(st['load_s'], 2)} с)",
         round(st["loaded"] / st["load_s"]), "rows/s"),
        ("storage_bytes", "Розмір сховища на диску",
         f"{mb(st['size'])} ({st['size_note']})", st["size"], "B"),
        ("full_scan_s", "Повне сканування (кількість і середнє за кожним показником)",
         f"{num(st['scan_s'], 3)} с", round(st["scan_s"], 4), "s"),
        ("device_query_s", f"Вибірка за одним пристроєм ({device}, "
                           f"записів: {num(st['device_rows'])})",
         f"{num(st['device_s'], 3)} с", round(st["device_s"], 4), "s"),
        ("peak_rss_bytes", "Пікова пам'ять процесу Python (завантаження і запити)",
         f"{mb(st['peak'])} (до завантаження {mb(st['start'])})", st["peak"], "B"),
        ("pandas_peak_bytes_per_row", "Весь файл у pandas: пам'ять на один запис",
         f"{num(full['frame_bytes'] / full['rows'], 1)} Б у DataFrame, "
         f"до {num(per_row, 1)} Б під час читання", round(per_row, 1), "B/row"),
        ("rows_fit_in_ram", f"Межа: вміститься у {num(ram / 2**30, 1)} ГБ RAM",
         f"≈ {many(fit)} записів ≈ {num(fit / rows)}× цей набір", round(fit), "rows"),
    ]
    md = [f"### Вимірювання стадії 2: {name}, {data.name}", "",
          f"Тестова машина: {host}.", "", "| Показник | Значення |", "|---|---|"]
    md += [f"| {label} | {shown} |" for _, label, shown, _, _ in table]
    scan = ('Flux: `group(columns: ["_measurement"]) |> mean()` по всьому bucket'
            if kind == "influxdb" else f"`{SCAN_SQL}`")
    md += ["", f"Час запитів — медіана з {repeat} запусків. Повне сканування — {scan}; "
               f"вибірка пристрою — `series()` для кожного його показника. "
               f"Запис CSV тривав {num(sizes['csv_seconds'], 2)} с."]
    if kind in ("influxdb", "timescaledb"):
        md.append("Пам'ять самого сервера бази у Docker сюди не входить — "
                  "її показує `docker stats`.")
    if kind == "influxdb":
        md.append("Щойно записане InfluxDB тримає у журналі WAL і приблизно через 10 хв "
                  "без нових записів ущільнює у файли TSM — розмір зменшиться. Тоді "
                  "виміряйте ще раз: `python pipeline/measure.py --storage InfluxDB "
                  "--size-only`.")
    if scale:
        md += ["", "Дослід із масштабуванням (--scale-test): той самий набір, повторений "
                   "кілька разів в одному файлі, читається у pandas цілком.", "",
               "| Копій | Записів | Файл Parquet | DataFrame у пам'яті | Пік під час читання "
               "| Пік на запис |", "|---|---|---|---|---|---|"]
        md += [f"| {s['copies']}× | {num(s['rows'])} | {mb(s['file_bytes'])} "
               f"| {mb(s['frame_bytes'])} | {mb(s['peak_bytes'])} "
               f"| {num(s['peak_bytes'] / s['rows'], 1)} Б |" for s in scale]
        # Пряма через точки: сталі витрати + однакова ціна кожного запису
        line = statistics.linear_regression([s["rows"] for s in scale],
                                            [s["peak_bytes"] for s in scale])
        limit = (ram - line.intercept) / line.slope
        md += ["", f"Пам'ять росте лінійно: ≈ {mb(line.intercept)} + {num(line.slope, 1)} Б "
                   f"на кожен запис. За цією прямою {num(ram / 2**30, 1)} ГБ RAM "
                   f"закінчаться на ≈ {many(limit)} записів."]
        table += [(f"scale_{s['copies']}x_peak_bytes", f"{s['copies']}×: пік під час читання",
                   "", s["peak_bytes"], "B") for s in scale]
        table += [("scale_bytes_per_row", "Нахил прямої: байтів на запис", "",
                   round(line.slope, 1), "B/row"),
                  ("scale_rows_limit", "Межа RAM за прямою", "", round(limit), "rows")]
    table += [("pandas_frame_bytes_per_row", "DataFrame: байтів на запис", "",
               round(full["frame_bytes"] / full["rows"], 1), "B/row"),
              ("machine", "Тестова машина", "", host, ""),
              ("storage", "Сховище", "", name, ""),
              ("data", "Файл даних", "", data.name, "")]
    return md, [(key, label, value, unit) for key, label, _, value, unit in table]


# ======================================================================
# Головна частина
# ======================================================================

def pick_storage(args: argparse.Namespace, data: Path) -> str:
    """--storage, а якщо його нема — поле assigned_stack.storage з маніфесту."""
    if args.storage:
        return args.storage
    manifest = Path(args.manifest) if args.manifest else data.with_suffix(".json")
    if manifest.is_file():
        name = json.loads(manifest.read_text(encoding="utf-8")).get(
            "assigned_stack", {}).get("storage")
        if name:
            print(f"Сховище взято з маніфесту {manifest.name}: {name}")
            return name
    raise StorageError('Не вказано сховище. Додайте --storage з назвою з маніфесту '
                       '(поле assigned_stack.storage), наприклад:\n'
                       '    python pipeline/measure.py --storage "DuckDB + Parquet" '
                       f'--data {data.name}')


def size_only(name: str, kind: str, args: argparse.Namespace) -> int:
    """--size-only: лише розмір уже завантажених даних (InfluxDB, TimescaleDB)."""
    if kind not in ("influxdb", "timescaledb"):
        raise StorageError("--size-only потрібен лише для InfluxDB і TimescaleDB: файл "
                           "DuckDB чи SQLite вимірюється під час звичайного запуску.")
    st = open_storage(name, **server_options(kind, args))
    try:
        size, note = server_size(st)
    finally:
        st.close()
    print(f"Розмір сховища на диску: {mb(size)} ({note})")
    return 0


def measure(args: argparse.Namespace) -> int:
    data = Path(args.data).expanduser()
    if not data.is_file() and not args.size_only:     # --size-only файл даних не читає
        raise StorageError(
            f"Не знайдено файл з даними: {data.resolve()}\n"
            f"Покладіть tuning.parquet або свій variant_NNN.parquet у поточну теку "
            f"чи вкажіть шлях через --data.\nФайли видає керівник (посилання — у "
            f"таблиці призначень).")
    name = pick_storage(args, data)
    kind = normalise_kind(name)
    if args.size_only:
        return size_only(name, kind, args)

    rows = pq.ParquetFile(data).metadata.num_rows
    device, expected = device_rows(data, args.device)
    host = machine()
    print(f"Машина: {host}")
    print(f"Дані: {data.name}, {num(rows)} записів.  Сховище: {name}\n")

    # Тимчасова тека — тут же, на тому самому диску, що й дані (а не в /tmp,
    # який у деяких Linux лежить в оперативній пам'яті). Зникає наприкінці.
    with tempfile.TemporaryDirectory(prefix="measure_", dir=Path.cwd(),
                                     ignore_cleanup_errors=True) as tmp_name:
        tmp = Path(tmp_name)
        print("[1/4] CSV проти Parquet…", flush=True)
        sizes = csv_vs_parquet(data, tmp)
        print("[2/4] Завантаження у сховище…", flush=True)
        st = run_storage(name, kind, args, data, tmp, device, list(expected))
        print("[4/4] Весь файл у pandas (в окремому процесі)…", flush=True)
        full, scale = run_pandas(data, tmp, args.scale_test)

    if st["device_rows"] != sum(expected.values()):
        print(f"  УВАГА: сховище повернуло {num(st['device_rows'])} записів пристрою "
              f"{device}, а у Parquet їх {num(sum(expected.values()))}.", file=sys.stderr)
    md, rows_csv = report(name, kind, data, host, rows, device, sizes, st, full, scale,
                          args.repeat)
    text = "\n".join(md)
    print("\n" + text + "\n")
    out = Path(args.out or f"measure_{kind}.csv")
    out.with_suffix(".md").write_text(text + "\n", encoding="utf-8")
    # utf-8-sig — щоб Excel на Windows правильно показав кирилицю
    with out.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(["key", "label", "value", "unit"])
        writer.writerows(rows_csv)
    print(f"Збережено: {out.with_suffix('.md')} і {out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", default="tuning.parquet",
                        help="файл Parquet (типово tuning.parquet у поточній теці)")
    parser.add_argument("--storage", help='назва сховища з маніфесту, напр. "DuckDB + Parquet"; '
                                          "якщо не вказано — береться з маніфесту")
    parser.add_argument("--manifest", help="маніфест (типово — файл .json поруч із --data)")
    parser.add_argument("--device", help="пристрій для вибірки (типово — з першого рядка файла)")
    parser.add_argument("--repeat", type=int, default=3,
                        help="скільки разів повторити кожен запит (типово 3, береться медіана)")
    parser.add_argument("--scale-test", action="store_true",
                        help="ще й дослід із 1×, 2× і 4× копіями даних у pandas")
    parser.add_argument("--size-only", action="store_true",
                        help="лише розмір сховища, без завантаження (InfluxDB, TimescaleDB)")
    parser.add_argument("--out", help="файл CSV з результатом (типово measure_<сховище>.csv)")
    parser.add_argument("--url", default="http://localhost:8086",
                        help="InfluxDB: адреса сервера, якщо ви змінили порт")
    parser.add_argument("--bucket", default="iot",
                        help="InfluxDB: bucket (типово iot); його вміст буде замінено")
    parser.add_argument("--dsn", help="TimescaleDB: рядок підʼєднання, якщо ви змінили порт, "
                                      'напр. "host=localhost port=5433 user=student '
                                      'password=kursova2025 dbname=iot"')
    args = parser.parse_args(argv)
    try:
        return measure(args)
    except StorageError as exc:
        sys.stdout.flush()
        print(f"\nПОМИЛКА\n{exc}\n", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nПерервано користувачем.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
