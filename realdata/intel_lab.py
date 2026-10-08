#!/usr/bin/env python3
"""
Стадія 4 — реальні дані Intel Berkeley Research Lab.

Що робить скрипт:
  1. завантажує data.txt.gz (34 МБ) і mote_locs.txt у поточну теку, якщо
     їх там ще немає;
  2. розбирає 2,3 млн рядків журналу, не падаючи на обірваних рядках;
  3. записує intel_lab.parquet у тому самому «довгому» форматі, що й ваш
     варіант (ts, device_id, metric, value), і маніфест intel_lab.json;
  4. друкує звіт про якість даних — числа для розділу «проблеми реальних
     даних» вашого звіту (повна таблиця за вузлами — у intel_lab_motes.csv).

Після цього з файлом працює все, що ви написали на стадіях 2–3:
    python ../pipeline/storage.py --kind duckdb --parquet intel_lab.parquet
    python ../detect/example.py --data intel_lab.parquet --manifest intel_lab.json
    python clean.py          # таблиця «до/після» очищення

Про час. Дата й час у файлі — МІСЦЕВИЙ час лабораторії (Берклі,
Каліфорнія), без зазначення поясу. У курсі ts — секунди епохи UTC, тому
скрипт переводить час із поясу America/Los_Angeles: до 4 квітня 2004 р.
це UTC-8, після переходу на літній час (4 квітня, 02:00) — UTC-7.
Стовпець epoch — НЕ час: це номер вимірювання вузла, який ще й
починається спочатку після перезапуску вузла. Для міток часу його не
використовуємо.

Запуск:
    python intel_lab.py
    python intel_lab.py --tz UTC      # залишити години такими, як у файлі
"""

from __future__ import annotations

import argparse
import gzip
import json
import ssl
import sys
import time
import urllib.error
import urllib.request
import zlib
from pathlib import Path

try:
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    sys.exit(f"Не встановлено пакет {exc.name}. Виконайте:\n"
             f"    pip install numpy pandas pyarrow")

BASE_URL = "https://db.csail.mit.edu/labdata/"
DATA_FILE = "data.txt.gz"
LOCS_FILE = "mote_locs.txt"

COLUMNS = ["date", "time", "epoch", "moteid",
           "temperature", "humidity", "light", "voltage"]
METRICS = ["temperature", "humidity", "light", "voltage"]
UNITS = {"temperature": "C", "humidity": "%", "light": "lx", "voltage": "V"}

# Розряд батареї. Коли напруга падає приблизно від 2,4 до 2,3 В, температура
# «повзе» вгору, а нижче 2,3 В здебільшого застигає на 122,153 °C — стелі
# давача. Перевірте це самі: рисунок `python clean.py --plot mote-07`.
LOW_VOLTAGE = 2.3
GAP_S = 600                   # перерва довша за 10 хв (≈ 20 пропущених вимірювань)
CHUNK_ROWS = 250_000          # читаємо порціями: так потрібно менше пам'яті
PHYSICAL = {                  # фізично можливі межі для приміщення лабораторії
    "temperature": (0.0, 50.0),
    "humidity": (0.0, 100.0),
    "light": (0.0, 100_000.0),
    "voltage": (2.0, 3.3),
}

# Така сама схема, як у variant_NNN.parquet: текстові стовпці закодовані
# словником, тож код зі стадій 2–3 читає цей файл без жодних змін.
SCHEMA = pa.schema([
    ("ts", pa.int64()),
    ("device_id", pa.dictionary(pa.int32(), pa.string())),
    ("metric", pa.dictionary(pa.int32(), pa.string())),
    ("value", pa.float32()),
])


def num(n: float) -> str:
    """1234567 -> '1 234 567'."""
    return f"{int(n):,}".replace(",", " ")


def pct(part: float, whole: float) -> str:
    return f"{100 * part / whole:.1f} %" if whole else "—"


# ======================================================================
# 1. Завантаження
# ======================================================================

def download(name: str, folder: Path) -> Path:
    """Завантажує файл у теку folder, якщо його там ще немає."""
    target = folder / name
    if target.exists() and target.stat().st_size > 0:
        return target

    url = BASE_URL + name
    part = target.with_name(name + ".part")     # недокачане не сплутаєш з готовим
    print(f"Завантажую {url}")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp, open(part, "wb") as out:
            total = int(resp.headers.get("Content-Length") or 0)
            done, started = 0, time.perf_counter()
            while block := resp.read(256 * 1024):
                out.write(block)
                done += len(block)
                if total > 1_000_000:
                    speed = done / 1e6 / max(time.perf_counter() - started, 1e-6)
                    print(f"\r  {name}: {done / 1e6:.1f} з {total / 1e6:.1f} МБ "
                          f"({100 * done / total:.0f} %), {speed:.1f} МБ/с   ",
                          end="", flush=True)
        if total and done != total:
            raise OSError(f"отримано {num(done)} байтів замість {num(total)}")
        size = f"{done / 1e6:.1f} МБ" if done >= 1e6 else f"{done / 1e3:.1f} КБ"
        print(f"\r  {name}: готово, {size}{' ' * 30}")
    except (urllib.error.URLError, OSError) as exc:
        part.unlink(missing_ok=True)
        reason = getattr(exc, "reason", exc)
        hint = ""
        if isinstance(reason, ssl.SSLError):
            hint = ("\nСхоже на проблему із сертифікатами. На macOS з Python від\n"
                    "python.org двічі клацніть «Install Certificates.command» у теці\n"
                    "«Програми / Python 3.x» і запустіть скрипт ще раз.")
        sys.exit(f"\nНе вдалося завантажити {url}\nПричина: {reason}{hint}\n\n"
                 f"Завантажте обидва файли вручну у браузері:\n"
                 f"    {BASE_URL}{DATA_FILE}\n    {BASE_URL}{LOCS_FILE}\n"
                 f"і покладіть їх у теку {folder.resolve()}\n"
                 f"(розпаковувати data.txt.gz не потрібно).")
    part.replace(target)
    return target


def find_or_download(folder: Path) -> tuple[Path, Path]:
    # Safari і деякі архіватори самі розпаковують .gz у data.txt — приймаємо і його.
    plain = folder / "data.txt"
    if not (folder / DATA_FILE).exists() and plain.exists():
        data = plain
    else:
        data = download(DATA_FILE, folder)
    return data, download(LOCS_FILE, folder)


# ======================================================================
# 2. Розбір
# ======================================================================

def read_locations(path: Path) -> dict[int, tuple[float, float]]:
    """mote_locs.txt: «номер x y» — координати в метрах від кута лабораторії."""
    locs = {}
    for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
        parts = line.split()
        if len(parts) == 3:
            locs[int(parts[0])] = (float(parts[1]), float(parts[2]))
    if not locs:
        sys.exit(f"У файлі {path} немає координат вузлів — завантажте його ще раз.")
    return locs


def count_lines(path: Path) -> int:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as f:
        return sum(block.count(b"\n") for block in iter(lambda: f.read(1 << 20), b""))


def read_raw(path: Path, tz: str) -> tuple[pd.DataFrame, dict]:
    """Читає журнал порціями у таблицю «один рядок файла — один рядок»."""
    stats = {"lines": 0, "parsed": 0, "no_mote": 0, "bad_time": 0}
    parts = []
    try:
        stats["lines"] = count_lines(path)
        # Значення розділені ОДНИМ пробілом, а відсутнє значення — це порожнє
        # місце між двома пробілами. Рядок
        #     2004-03-30 08:11:56.681052 53671 3    2.1997
        # означає: температури, вологості й освітленості немає, напруга 2.1997.
        # Тому sep=" ", а не sep=r"\s+": останній «склеїв» би пробіли, і
        # напруга опинилася б у стовпці температури.
        reader = pd.read_csv(path, sep=" ", header=None, names=COLUMNS,
                             dtype={"date": str, "time": str},
                             on_bad_lines="skip", chunksize=CHUNK_ROWS)
        for chunk in reader:
            stats["parsed"] += len(chunk)
            parts.append(parse_chunk(chunk, tz, stats))
    except (EOFError, OSError, zlib.error) as exc:
        sys.exit(f"Файл {path} пошкоджений або завантажений не до кінця ({exc}).\n"
                 f"Видаліть його і запустіть скрипт ще раз.")
    stats["skipped"] = stats["lines"] - stats["parsed"]
    raw = pd.concat(parts, ignore_index=True)
    if raw.empty:
        sys.exit(f"У файлі {path} не знайдено жодного показання — це не журнал "
                 f"Intel Lab\n(можливо, браузер зберіг сторінку з помилкою). "
                 f"Видаліть файл і запустіть скрипт ще раз.")
    # У файлі рядки впорядковано за (вузол, epoch), а не за часом
    return raw.sort_values(["mote", "ts"], kind="stable", ignore_index=True), stats


def parse_chunk(chunk: pd.DataFrame, tz: str, stats: dict) -> pd.DataFrame:
    # Якщо десь у числовому стовпці трапиться сміття, воно стане NaN
    for col in ["epoch", "moteid", *METRICS]:
        chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

    # Рядок без номера вузла не можна приписати жодному пристрою
    mote = chunk["moteid"]
    no_mote = mote.isna() | (mote <= 0) | (mote != mote.round())
    stats["no_mote"] += int(no_mote.sum())
    chunk = chunk[~no_mote]

    # Частки секунди бувають різної довжини або їх немає зовсім —
    # формат ISO8601 приймає всі варіанти
    local = pd.to_datetime(chunk["date"] + " " + chunk["time"],
                           format="ISO8601", errors="coerce")
    utc = local.dt.tz_localize(tz, ambiguous="NaT", nonexistent="shift_forward")
    ok = utc.notna()
    stats["bad_time"] += int((~ok).sum())
    chunk, utc = chunk[ok], utc[ok]

    # Секунди епохи (частки секунди відкидаємо). Ділимо на Timedelta, а не
    # робимо astype("int64"): pandas 2 і pandas 3 зберігають час у різних
    # одиницях (нано- та мікросекундах), і число вийшло б різним.
    ts = (utc - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)
    return pd.DataFrame({
        "ts": ts.astype("int64"),
        "mote": chunk["moteid"].astype("int64"),
        "epoch": chunk["epoch"],
        **{m: chunk[m].astype("float32") for m in METRICS},
    })


def device_name(mote: int) -> str:
    return f"mote-{mote:02d}"


def to_long_table(raw: pd.DataFrame) -> pa.Table:
    """Один рядок файла (4 показники) -> до 4 рядків довгого формату."""
    order = np.lexsort((raw["mote"].to_numpy(), raw["ts"].to_numpy()))
    wide = raw.iloc[order]                         # за часом, потім за вузлом
    values = wide[METRICS].to_numpy(dtype=np.float32)          # n × 4

    # Порожнє поле не стає рядком зі значенням NaN — такого рядка просто
    # немає, як і у вашому варіанті. np.nonzero обходить таблицю рядок за
    # рядком, тож порядок (час, вузол, показник) зберігається.
    row, col = np.nonzero(~np.isnan(values))

    # Назви пристроїв і показників зберігаємо словником (як у variant_NNN):
    # кожна назва записана один раз, а в рядку — лише її номер.
    motes = np.sort(wide["mote"].unique())
    dev_code = np.searchsorted(motes, wide["mote"].to_numpy())[row].astype(np.int32)
    return pa.table({
        "ts": wide["ts"].to_numpy()[row],
        "device_id": pa.DictionaryArray.from_arrays(
            dev_code, pa.array([device_name(m) for m in motes])),
        "metric": pa.DictionaryArray.from_arrays(col.astype(np.int32), pa.array(METRICS)),
        "value": values[row, col],
    }, schema=SCHEMA)


def write_manifest(path: Path, locs: dict, raw: pd.DataFrame, tz: str) -> None:
    """Маніфест у форматі variant_NNN.json — для коду, якому він потрібен."""
    start = pd.Timestamp(int(raw["ts"].min()), unit="s", tz="UTC")
    days = (int(raw["ts"].max()) - int(raw["ts"].min())) / 86400
    manifest = {
        "schema_version": "1.0",
        "variant": None,
        "dataset": "Intel Berkeley Research Lab, 2004-02-28 .. 2004-04-05",
        "source": BASE_URL + "labdata.html",
        "source_timezone": tz,
        "period": {"start": start.isoformat(), "days": round(days, 1)},
        "columns": {"ts": "epoch seconds (UTC)", "device_id": "string",
                    "metric": "string", "value": "float32"},
        "metrics": {m: {"unit": UNITS[m]} for m in METRICS},
        "zones": ["lab"],
        "gateways": ["base-station"],
        # Перелік того, що МАЄ бути в мережі (mote_locs.txt), а не того,
        # що є у даних — так само, як у вашому маніфесті.
        "devices": [{"device_id": device_name(m), "type": "mica2dot",
                     "zone": "lab", "gateway": "base-station",
                     "metrics": METRICS, "interval_s": 31,
                     "x_m": x, "y_m": y} for m, (x, y) in sorted(locs.items())],
    }
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


# ======================================================================
# 3. Звіт про якість
# ======================================================================

def per_mote_table(raw: pd.DataFrame, locs: dict) -> pd.DataFrame:
    gap = raw.groupby("mote")["ts"].diff()
    g = raw.assign(gap=gap).groupby("mote")
    table = pd.DataFrame({
        "readings": g.size(),
        "first_utc": pd.to_datetime(g["ts"].min(), unit="s"),
        "last_utc": pd.to_datetime(g["ts"].max(), unit="s"),
        "gaps_over_10min": g["gap"].apply(lambda s: int((s > GAP_S).sum())),
        "longest_gap_h": (g["gap"].max() / 3600).round(1),
        "low_voltage_share": g["voltage"].apply(lambda s: round(float((s < LOW_VOLTAGE).mean()), 3)),
        "temp_over_50": g["temperature"].apply(lambda s: int((s > 50).sum())),
        "light_missing": g["light"].apply(lambda s: int(s.isna().sum())),
    })
    table.index = [device_name(m) for m in table.index]
    table.index.name = "device_id"
    table["in_mote_locs"] = [int(i.split("-")[1]) in locs for i in table.index]
    return table


def report(raw: pd.DataFrame, stats: dict, locs: dict, motes: pd.DataFrame, tz: str) -> None:
    n = len(raw)
    t0, t1 = int(raw["ts"].min()), int(raw["ts"].max())
    utc = lambda t: pd.Timestamp(t, unit="s", tz="UTC")
    print("\n=== Звіт про якість даних ===")
    print(f"Рядків у файлі          : {num(stats['lines'])}")
    print(f"  не розібрано          : {num(stats['skipped'])}")
    print(f"  без номера вузла      : {num(stats['no_mote'])} (відкинуто)")
    print(f"  з нерозпізнаним часом : {num(stats['bad_time'])} (відкинуто)")
    print(f"Придатних рядків        : {num(n)}")
    print(f"Період, UTC             : {utc(t0):%Y-%m-%d %H:%M} – {utc(t1):%Y-%m-%d %H:%M} "
          f"({(t1 - t0) / 86400:.1f} доби)")
    print(f"  у поясі {tz}: {utc(t0).tz_convert(tz):%Y-%m-%d %H:%M} – "
          f"{utc(t1).tz_convert(tz):%Y-%m-%d %H:%M}")

    known = motes[motes["in_mote_locs"]]
    absent = sorted(set(locs) - {int(i.split("-")[1]) for i in known.index})
    few = known[known["readings"] < 5000]["readings"]
    print(f"\nВузлів у mote_locs.txt  : {len(locs)}; з даними: {len(known)}"
          + (f"; без жодного запису: {', '.join(map(device_name, absent))}" if absent else ""))
    print(f"  записів на вузол      : мін {num(known['readings'].min())}, "
          f"медіана {num(known['readings'].median())}, макс {num(known['readings'].max())}")
    if len(few):
        print("  дуже мало записів     : "
              + ", ".join(f"{d} ({num(c)})" for d, c in few.items()))
    rogue = motes[~motes["in_mote_locs"]]["readings"]
    if len(rogue):
        print("Вузли, яких НЕМАЄ в mote_locs.txt: "
              + ", ".join(f"{d} ({num(c)})" for d, c in rogue.items()))

    print("\nПорожні поля (показання відсутнє):")
    for m in METRICS:
        print(f"  {m:<12}: {num(raw[m].isna().sum())}")
    no_light = motes[motes["light_missing"] == motes["readings"]].index.tolist()
    if no_light:
        print(f"  освітленості немає взагалі у: {', '.join(no_light)}")

    gaps = raw.groupby("mote")["ts"].diff()
    print(f"\nІнтервал між записами вузла: медіана {gaps.median():.0f} с "
          f"(за документацією 31 с)")
    print(f"  перерв > {GAP_S // 60} хв      : {num((gaps > GAP_S).sum())} "
          f"(на вузол: медіана {known['gaps_over_10min'].median():.0f}, "
          f"макс {known['gaps_over_10min'].max()})")
    print(f"  найдовша перерва      : {known['longest_gap_h'].max():.1f} год "
          f"({known['longest_gap_h'].idxmax()})")
    # Перерви, коли мовчали ВСІ вузли одночасно, — це збій бази, а не вузлів
    every = np.unique(raw["ts"].to_numpy())
    silent = np.diff(every)
    for i in np.where(silent > 6 * 3600)[0]:
        print(f"  мовчала вся мережа    : з {utc(every[i]):%Y-%m-%d %H:%M} UTC, "
              f"{silent[i] / 3600:.1f} год")

    low = raw["voltage"] < LOW_VOLTAGE
    hot = raw["temperature"] > 50
    print(f"\nРозряд батарей: показань з напругою < {LOW_VOLTAGE} В: {num(low.sum())} "
          f"({pct(low.sum(), n)})")
    print(f"  серед них температура > 50 °C: {pct((low & hot).sum(), low.sum())}; "
          f"серед решти: {pct((~low & hot).sum(), (~low).sum())}")
    print(f"  значення 122.153 °C (стеля давача): {num((raw['temperature'].round(3) == 122.153).sum())}")

    print("\nПоза фізичними межами:")
    for m, (lo, hi) in PHYSICAL.items():
        v = raw[m]
        print(f"  {m:<12} [{lo:g}, {hi:g}]: нижче {num((v < lo).sum())}, вище {num((v > hi).sum())}"
              f"   (мін {v.min():g}, макс {v.max():g})")

    dup = raw.duplicated(["mote", "ts"])
    same = raw.duplicated(["mote", "ts", *METRICS])
    reused = raw.duplicated(["mote", "epoch"])
    print(f"\nДублікати: той самий вузол і та сама секунда: {num(dup.sum())} "
          f"(з них повністю однакових: {num(same.sum())})")
    print(f"  повторів номера epoch у того самого вузла: {num(reused.sum())} — після\n"
          f"  перезапуску вузла лічильник іде спочатку, тож epoch не є ні часом, ні ключем")


# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="intel_lab.parquet",
                    help="куди записати Parquet (типово intel_lab.parquet)")
    ap.add_argument("--tz", default="America/Los_Angeles",
                    help="часовий пояс, у якому записано час у файлі "
                         "(типово America/Los_Angeles — місцевий час лабораторії; "
                         "UTC — залишити години як у файлі)")
    args = ap.parse_args()

    try:
        pd.Timestamp("2004-03-01").tz_localize(args.tz)
    except Exception:
        sys.exit(f"Невідомий часовий пояс {args.tz!r}. Приклади: America/Los_Angeles, UTC.\n"
                 f"Якщо пояс правильний, а ви на Windows, встановіть базу поясів:\n"
                 f"    pip install tzdata")

    started = time.perf_counter()
    folder = Path.cwd()
    data_path, locs_path = find_or_download(folder)
    locs = read_locations(locs_path)

    print(f"Читаю {data_path.name} (2,3 млн рядків)…")
    raw, stats = read_raw(data_path, args.tz)
    motes = per_mote_table(raw, locs)
    report(raw, stats, locs, motes, args.tz)

    table = to_long_table(raw)
    out = Path(args.out)
    pq.write_table(table, out, compression="zstd", row_group_size=512_000)
    write_manifest(out.with_suffix(".json"), locs, raw, args.tz)
    motes.to_csv(out.with_name(out.stem + "_motes.csv"))

    print(f"\nЗаписано {out}: рядків {num(table.num_rows)}, "
          f"{out.stat().st_size / 1e6:.1f} МБ")
    print("  за показниками: " + ", ".join(
        f"{m} {num(raw[m].notna().sum())}" for m in METRICS))
    print(f"Записано {out.with_suffix('.json')} і {out.stem}_motes.csv")
    print(f"Готово за {time.perf_counter() - started:.0f} с.")


if __name__ == "__main__":
    main()
