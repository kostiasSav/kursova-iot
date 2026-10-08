#!/usr/bin/env python3
"""
Storage adapters for the IoT coursework — one interface, four backends.

Every student is assigned one of four storage stacks, but the analysis built on
top of it must stay the same.  This module hides all four behind a single
`Storage` interface, so that стадія 3 (детектор) and стадія 4 (дашборд) never
contain a backend-specific line:

    from storage import open_storage

    st = open_storage("duckdb", database="iot.duckdb")
    st.load_parquet("../../out/public/variant_007.parquet")
    ts, value = st.series("temp-01-lob", "temperature")
    st.close()

The loaders never hold the dataset in memory.  A variant is 5-7 million rows;
materialised as Python objects that is well over a gigabyte, and on a laptop
with 8 GB it is the difference between a load that finishes and one that swaps
for half an hour.  Every loader therefore walks the Parquet file in row-group
batches — there is no `pq.read_table(...).to_pandas()` anywhere below, and
there must not be one in student code either.

Usage (self-test and bulk load from the shell)

  python storage.py --kind duckdb      --parquet ../../out/public/variant_007.parquet
  python storage.py --kind sqlite      --parquet ... --db iot.sqlite
  python storage.py --kind influxdb    --parquet ...     # потрібен docker compose up -d
  python storage.py --kind timescaledb --parquet ...     # потрібен docker compose up -d
"""

from __future__ import annotations

import abc
import argparse
import difflib
import importlib
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Iterator, NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# One batch is the unit of memory the loaders are allowed to hold.  500k rows
# is roughly one row group of a generated variant (the generator writes
# row_group_size=512_000), so a batch costs ~30 MB of Python objects at worst.
DEFAULT_BATCH_ROWS = 500_000

TABLE = "readings"

# The two dtypes every backend must return, whatever it stores internally.
# Analysis code is written once against these and must not have to care that
# InfluxDB keeps doubles and TimescaleDB keeps `real`.
TS_DTYPE = np.int64
VALUE_DTYPE = np.float32

# Flux has no "all of time": range() is mandatory and its default start is
# relative to now.  Variant data is months old, so every query needs explicit
# absolute bounds or it silently returns nothing.
FLUX_MIN = "1970-01-01T00:00:00Z"
FLUX_MAX = "2100-01-01T00:00:00Z"


class StorageError(RuntimeError):
    """
    Anything a student can realistically get wrong: a missing driver, a server
    that is not up, a typo in a path.  Carries a message that says what to do,
    and the CLI below prints it without a traceback — a traceback teaches a
    second-year nothing except that the code is broken.
    """


# ==========================================================================
# small shared helpers
# ==========================================================================

def _num(n: int) -> str:
    """Thousands-separated number for Ukrainian output (1 234 567)."""
    return f"{n:,}".replace(",", " ")


def _import_driver(module: str, pip_name: str, note: str = "") -> ModuleType:
    """Import a backend driver or explain, in Ukrainian, how to install it."""
    try:
        return importlib.import_module(module)
    except ImportError as exc:                     # noqa: PERF203 - one-shot
        tail = f"\n{note}" if note else ""
        raise StorageError(
            f"Не встановлено драйвер «{module}», без нього це сховище не працює.\n"
            f"Встановіть його у ваш venv:\n"
            f"    pip install {pip_name}{tail}"
        ) from exc


def to_epoch(value: Any) -> int | None:
    """
    Coerce whatever a student passes as a time bound into epoch seconds.

    Accepting datetime and ISO strings as well as ints is not politeness: the
    manifest and the ground truth both carry ISO timestamps, so the natural
    thing to write is `st.series(dev, m, start="2025-09-14T00:00:00+00:00")`.
    """
    if value is None:
        return None
    if isinstance(value, bool):                    # bool is an int subclass
        raise StorageError("Межа часу не може бути True/False.")
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, str):
        text = value.strip()
        if text.lstrip("-").isdigit():
            return int(text)
        try:
            return to_epoch(datetime.fromisoformat(text.replace("Z", "+00:00")))
        except ValueError as exc:
            raise StorageError(
                f"Не вдалося розпізнати час {value!r}. "
                f"Використайте epoch-секунди (1756684800), datetime "
                f"або рядок ISO 8601 ('2025-09-01T00:00:00Z')."
            ) from exc
    raise StorageError(f"Непідтримуваний тип межі часу: {type(value).__name__}.")


def _to_rfc3339(epoch: int) -> str:
    """Flux and the Influx delete API speak RFC-3339, not epoch seconds."""
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Chunk(NamedTuple):
    """One memory-bounded slice of the Parquet file."""

    ts: np.ndarray          # int64, epoch seconds
    device_id: list[str]
    metric: list[str]
    value: np.ndarray       # float32


def _labels(col: pa.Array) -> list[str]:
    """
    Materialise a label column without allocating one Python string per row.

    device_id and metric are dictionary-encoded in the file: a batch of 500k
    rows carries at most a few dozen distinct strings plus an int32 code per
    row.  Decoding the dictionary once and indexing it costs one pointer per
    row (~4 MB) instead of one new str object per row (~35 MB).
    """
    if pa.types.is_dictionary(col.type) and col.null_count == 0:
        values = col.dictionary.to_pylist()
        return [values[code] for code in col.indices.to_numpy(zero_copy_only=False)]
    return col.to_pylist()


def read_chunks(path: str | Path,
                batch_rows: int = DEFAULT_BATCH_ROWS) -> Iterator[Chunk]:
    """
    Stream a telemetry Parquet file in batches of at most `batch_rows` rows.

    This is the one place that touches Parquet, so it is also the one place
    that has to be memory-disciplined.  pyarrow decompresses a row group at a
    time; nothing here ever sees the whole file.
    """
    file = Path(path).expanduser()
    if not file.is_file():
        raise StorageError(
            f"Не знайдено файл з даними: {file}\n"
            f"Перевірте шлях — він має вказувати на ваш variant_NNN.parquet."
        )
    if batch_rows < 1:
        raise StorageError("batch_rows має бути додатним числом.")

    try:
        reader = pq.ParquetFile(file)
    except pa.ArrowInvalid as exc:
        raise StorageError(
            f"Файл {file} не є коректним Parquet-файлом.\n"
            f"Якщо ви завантажували його з GitHub Releases — перезавантажте."
        ) from exc

    expected = {"ts", "device_id", "metric", "value"}
    missing = expected - set(reader.schema_arrow.names)
    if missing:
        raise StorageError(
            f"У файлі {file.name} бракує колонок: {', '.join(sorted(missing))}.\n"
            f"Очікувана схема: ts (int64), device_id, metric, value (float32)."
        )

    for batch in reader.iter_batches(batch_size=batch_rows,
                                     columns=["ts", "device_id", "metric", "value"]):
        yield Chunk(
            ts=batch.column("ts").to_numpy(zero_copy_only=False).astype(TS_DTYPE, copy=False),
            device_id=_labels(batch.column("device_id")),
            metric=_labels(batch.column("metric")),
            value=batch.column("value").to_numpy(zero_copy_only=False).astype(VALUE_DTYPE, copy=False),
        )


def parquet_rows(path: str | Path) -> int:
    """Row count straight from the Parquet footer — no data is read."""
    return int(pq.ParquetFile(Path(path).expanduser()).metadata.num_rows)


def _as_arrays(ts: Any, value: Any) -> tuple[np.ndarray, np.ndarray]:
    """Normalise whatever a backend returned into the contract dtypes."""
    return (np.asarray(ts, dtype=TS_DTYPE), np.asarray(value, dtype=VALUE_DTYPE))


def _tally(chunk: Chunk, devices: set[str], per_metric: Counter) -> None:
    """
    Accumulate the summary counts while a batch is already in hand.

    `SELECT metric, count(*) ... GROUP BY metric` over millions of rows costs
    seconds every time it runs, and the картка результатів needs it on every
    run.  Counting during the load is one extra C-level pass over data that is
    already in memory, and the answer can then be stored alongside the data.
    """
    devices.update(chunk.device_id)
    per_metric.update(chunk.metric)


# ==========================================================================
# the interface every backend implements
# ==========================================================================

class Storage(abc.ABC):
    """
    Uniform access to one variant's telemetry.

    Four rules every implementation keeps, so that analysis code is genuinely
    portable between them:

      * series() returns (int64 epoch seconds, float32 values), sorted by ts;
      * the time window is half-open, start <= ts < end;
      * devices() and metrics() are sorted;
      * load_parquet() is idempotent — it rebuilds the dataset from scratch,
        so running it twice never gives you the data twice.
    """

    kind: str = "?"

    def __init__(self, *, progress: bool = True) -> None:
        self._progress = progress
        self._closed = False

    # -- the contract -------------------------------------------------------

    @abc.abstractmethod
    def load_parquet(self, path: str | Path,
                     batch_rows: int = DEFAULT_BATCH_ROWS) -> int:
        """Load a variant file into the backend; returns the rows loaded."""

    @abc.abstractmethod
    def series(self, device_id: str, metric: str,
               start: Any = None, end: Any = None) -> tuple[np.ndarray, np.ndarray]:
        """One device/metric time series as (ts, value), ascending by ts."""

    @abc.abstractmethod
    def devices(self) -> list[str]:
        """Device ids actually present in the data (not in the manifest)."""

    @abc.abstractmethod
    def metrics(self) -> list[str]:
        """Metric names present in the data."""

    @abc.abstractmethod
    def row_count(self) -> int:
        """Total rows stored."""

    @abc.abstractmethod
    def rows_per_metric(self) -> dict[str, int]:
        """Rows per metric — goes straight into the картка результатів."""

    @abc.abstractmethod
    def close(self) -> None:
        """Release the connection.  Safe to call twice."""

    # -- shared plumbing ----------------------------------------------------

    def __enter__(self) -> "Storage":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"<{type(self).__name__} kind={self.kind!r}>"

    def _window(self, start: Any, end: Any) -> tuple[int | None, int | None]:
        lo, hi = to_epoch(start), to_epoch(end)
        if lo is not None and hi is not None and hi <= lo:
            raise StorageError(
                f"Порожній інтервал: end ({hi}) має бути більшим за start ({lo})."
            )
        return lo, hi

    def _explain_empty(self, device_id: str, metric: str) -> None:
        """
        Turn an empty series into a useful message when the name was wrong.

        A typo in a device_id is the single easiest mistake to make here and
        the hardest to notice: a wrong name is not an error in any of the four
        backends, it is simply a query that matches nothing, and the student
        spends an evening debugging a detector that was handed zero points.
        Only ever called on the empty path, so it costs nothing in normal use —
        and an empty result from a time window that genuinely has no data stays
        empty, without complaint.
        """
        wrong: list[tuple[str, str, list[str]]] = []
        if device_id not in (known := self.devices()):
            wrong.append(("device_id", device_id, known))
        if metric not in (known := self.metrics()):
            wrong.append(("metric", metric, known))
        if not wrong:
            return
        lines = []
        for field, given, options in wrong:
            near = difflib.get_close_matches(given, options, n=3, cutoff=0.5)
            hint = (f" Можливо, ви мали на увазі: {', '.join(near)}."
                    if near else f" Доступні значення: {', '.join(options[:8])}…")
            lines.append(f"  невідомий {field}: {given!r}.{hint}")
        raise StorageError(
            "Запит не повернув жодної точки, бо у даних немає таких назв:\n"
            + "\n".join(lines)
            + "\n  Повний перелік: storage.devices() і storage.metrics().")

    def _tick(self, done: int, total: int, started: float) -> None:
        if not self._progress:
            return
        elapsed = max(time.perf_counter() - started, 1e-9)
        share = f"{100.0 * done / total:5.1f}%" if total else "  ?  "
        print(f"\r  [{self.kind}] завантажено {_num(done)} з {_num(total)} рядків "
              f"({share}, {_num(int(done / elapsed))} рядків/с)",
              end="", file=sys.stderr, flush=True)

    def _tick_done(self, rows: int, started: float, note: str = "") -> None:
        if not self._progress:
            return
        elapsed = time.perf_counter() - started
        print(f"\r  [{self.kind}] завантажено {_num(rows)} рядків за "
              f"{elapsed:.1f} с{(' — ' + note) if note else ''}"
              f"{' ' * 20}", file=sys.stderr, flush=True)

    def _say(self, message: str) -> None:
        if self._progress:
            print(f"  [{self.kind}] {message}", file=sys.stderr, flush=True)

    def _warn(self, message: str) -> None:
        """Not gated by `progress`: a warning here changes what the student
        must write in the картка результатів."""
        print(f"\n  [{self.kind}] УВАГА: {message}\n", file=sys.stderr, flush=True)


# ==========================================================================
# 1. DuckDB over Parquet
# ==========================================================================

class DuckDBStorage(Storage):
    """
    DuckDB reading the Parquet file in place.

    The default does not copy the data at all: `load_parquet` defines a view
    over the file and DuckDB pushes filters and projections down into the
    Parquet row groups at query time.  For this workload that is strictly
    better than importing — the file is already columnar, already compressed,
    already sorted by ts, and a copy would only add 20-30 MB and a load step.

    Pass materialise=True to get a real table instead.  It is slower to build
    and larger on disk, but it survives the Parquet file being moved, and it
    is worth measuring both ways in розділ "обґрунтування вибору стека".
    """

    kind = "duckdb"

    def __init__(self, database: str | Path = ":memory:", *,
                 path: str | Path | None = None,
                 materialise: bool = False, threads: int | None = None,
                 memory_limit: str | None = None, progress: bool = True) -> None:
        super().__init__(progress=progress)
        duckdb = _import_driver("duckdb", "duckdb")
        # `path` is the spelling the README and SQLiteStorage use, `database`
        # is DuckDB's own. Accept both rather than make a student debug a
        # TypeError over a keyword name.
        self._database = str(path if path is not None else database)
        self._materialise = materialise
        try:
            self._con = duckdb.connect(self._database)
        except Exception as exc:                   # duckdb.IOException and friends
            raise StorageError(
                f"Не вдалося відкрити базу DuckDB {self._database!r}.\n"
                f"Можливо, файл уже відкритий іншим процесом (ноутбук Jupyter?).\n"
                f"Деталі: {exc}"
            ) from exc
        if threads:
            self._con.execute(f"SET threads = {int(threads)}")
        if memory_limit:
            # Caps DuckDB's working set and lets it spill to disk instead of
            # being killed by the OS — the useful knob on an 8 GB laptop.
            self._con.execute(f"SET memory_limit = '{memory_limit}'")

    @property
    def connection(self) -> Any:
        """Escape hatch for stage 3: raw SQL against the same connection."""
        return self._con

    def load_parquet(self, path: str | Path,
                     batch_rows: int = DEFAULT_BATCH_ROWS) -> int:
        # batch_rows is deliberately unused: DuckDB streams row groups itself
        # and never materialises the file. The parameter stays for interface
        # symmetry, so student code can call every backend the same way.
        del batch_rows
        file = Path(path).expanduser().resolve()
        if not file.is_file():
            raise StorageError(f"Не знайдено файл з даними: {file}")
        literal = str(file).replace("'", "''")
        started = time.perf_counter()

        projection = (f"SELECT ts, CAST(device_id AS VARCHAR) AS device_id, "
                      f"       CAST(metric AS VARCHAR) AS metric, value "
                      f"FROM read_parquet('{literal}')")
        kind, note = (("TABLE", "таблиця") if self._materialise
                      else ("VIEW", "подання (view) без копіювання даних"))
        try:
            # CREATE OR REPLACE only replaces an object of the *same* kind, and
            # DROP VIEW IF EXISTS still errors when the name is a table (IF
            # EXISTS guards the name, not the kind). A student who ran once
            # with materialise=True and once without would otherwise get
            # "use DROP TABLE to delete table readings", so ask the catalogue
            # what is actually there and drop that.
            for (existing,) in self._con.execute(
                    "SELECT table_type FROM information_schema.tables "
                    "WHERE table_name = ?", [TABLE]).fetchall():
                self._con.execute(
                    f"DROP {'VIEW' if existing == 'VIEW' else 'TABLE'} {TABLE}")
            self._con.execute(f"CREATE {kind} {TABLE} AS {projection}")
        except Exception as exc:
            raise StorageError(
                f"Не вдалося прочитати {file.name} у DuckDB.\n"
                f"Переконайтеся, що це справжній Parquet-файл варіанта, а "
                f"--db вказує на базу DuckDB (а не на .sqlite).\n"
                f"Деталі: {exc}") from exc

        rows = self.row_count()
        self._tick_done(rows, started, note)
        return rows

    def series(self, device_id: str, metric: str,
               start: Any = None, end: Any = None) -> tuple[np.ndarray, np.ndarray]:
        lo, hi = self._window(start, end)
        where = ["device_id = ?", "metric = ?"]
        args: list[Any] = [device_id, metric]
        if lo is not None:
            where.append("ts >= ?")
            args.append(lo)
        if hi is not None:
            where.append("ts < ?")
            args.append(hi)
        out = self._query(
            f"SELECT ts, value FROM {TABLE} WHERE {' AND '.join(where)} ORDER BY ts",
            args)
        ts, value = _as_arrays(out["ts"], out["value"])
        if len(ts) == 0:
            self._explain_empty(device_id, metric)
        return ts, value

    def devices(self) -> list[str]:
        return [str(v) for v in self._query(
            f"SELECT DISTINCT device_id FROM {TABLE} ORDER BY 1")["device_id"]]

    def metrics(self) -> list[str]:
        return [str(v) for v in self._query(
            f"SELECT DISTINCT metric FROM {TABLE} ORDER BY 1")["metric"]]

    def row_count(self) -> int:
        return int(self._query(f"SELECT count(*) AS n FROM {TABLE}")["n"][0])

    def rows_per_metric(self) -> dict[str, int]:
        out = self._query(
            f"SELECT metric, count(*) AS n FROM {TABLE} GROUP BY metric ORDER BY metric")
        return {str(m): int(n) for m, n in zip(out["metric"], out["n"])}

    def close(self) -> None:
        if not self._closed:
            self._con.close()
            self._closed = True

    def _query(self, sql: str, args: list[Any] | None = None) -> dict[str, np.ndarray]:
        """Run SQL and get numpy columns back — no pandas round trip."""
        try:
            return self._con.execute(sql, args or []).fetchnumpy()
        except Exception as exc:
            if TABLE in str(exc) and "does not exist" in str(exc):
                raise StorageError(
                    "Дані ще не завантажені. Спочатку викличте "
                    "load_parquet(шлях_до_parquet)."
                ) from exc
            raise StorageError(
                f"Помилка запиту DuckDB.\nЗапит: {sql}\nДеталі: {exc}") from exc


# ==========================================================================
# 2. SQLite
# ==========================================================================

class SQLiteStorage(Storage):
    """
    Plain SQLite, one wide table plus one composite index.

    SQLite has no bulk loader, so this is the backend where the batching rule
    is not advice but a hard requirement: inserting 5-7 million rows means
    5-7 million Python tuples, and they have to be created and thrown away a
    batch at a time.

    Two choices worth defending in the report:

      * the index is built *after* the insert, not before.  Maintaining a
        B-tree during a sequential load roughly triples the load time;
      * the loader turns off the journal and fsync.  The database is derived
        data that can be rebuilt from the Parquet in minutes, so durability
        during the load buys nothing and costs a lot.  Both are restored
        afterwards.
    """

    kind = "sqlite"

    # One B-tree in (device_id, metric, ts) order turns series() into a single
    # range scan: the rows a query wants are contiguous in the index.
    _INDEX = f"CREATE INDEX IF NOT EXISTS idx_series ON {TABLE} (device_id, metric, ts)"

    def __init__(self, path: str | Path = "iot.sqlite", *,
                 progress: bool = True) -> None:
        super().__init__(progress=progress)
        self._path = Path(path).expanduser()
        # sqlite3.connect() is lazy — it does not look at the file until the
        # first statement runs, so "this is not a SQLite database" surfaces
        # here and not at connect(). Both have to be inside the same guard, or
        # a student who points --db at the wrong file gets a traceback.
        try:
            self._con = sqlite3.connect(self._path, isolation_level=None)
            self._con.execute("PRAGMA user_version")
        # OperationalError is a subclass of DatabaseError, so it has to be
        # caught first: "cannot open" and "not a database" need different advice.
        except (sqlite3.OperationalError, OSError) as exc:
            raise StorageError(
                f"Не вдалося відкрити базу SQLite {self._path}.\n"
                f"Перевірте, що тека існує і доступна для запису.\n"
                f"Деталі: {exc}") from exc
        except sqlite3.DatabaseError as exc:
            raise StorageError(
                f"Файл {self._path} не є базою SQLite (або пошкоджений).\n"
                f"Вкажіть інший шлях через --db, наприклад --db iot.sqlite.\n"
                f"Деталі: {exc}") from exc
        # Negative cache_size is KiB, not pages, so this is a 64 MiB page
        # cache. Measured on the 5.4M-row tuning set, a bigger cache is not a
        # faster load: the insert is a pure sequential append and the index
        # build spills to disk anyway, so all a large cache buys is more dirty
        # pages to flush at once. 64 MiB -> 11.4 s / 339 MB peak;
        # 256 MiB -> 12.4 s / 765 MB peak. temp_store is left at the default
        # (FILE) for the same reason: the index sorter belongs on disk.
        self._con.execute("PRAGMA cache_size = -65536")

    @property
    def connection(self) -> sqlite3.Connection:
        """Escape hatch for stage 3: raw SQL against the same connection."""
        return self._con

    def load_parquet(self, path: str | Path,
                     batch_rows: int = DEFAULT_BATCH_ROWS) -> int:
        total = parquet_rows(path)
        started = time.perf_counter()

        self._con.execute("PRAGMA journal_mode = OFF")
        self._con.execute("PRAGMA synchronous = OFF")
        # Rebuild from scratch: a student who reruns the loader must not end up
        # with the dataset twice.
        self._con.execute(f"DROP TABLE IF EXISTS {TABLE}")
        self._con.execute("DROP TABLE IF EXISTS meta_metric")
        self._con.execute("DROP TABLE IF EXISTS meta_device")
        self._con.execute(f"""
            CREATE TABLE {TABLE} (
                ts        INTEGER NOT NULL,
                device_id TEXT    NOT NULL,
                metric    TEXT    NOT NULL,
                value     REAL    NOT NULL
            )""")

        insert = f"INSERT INTO {TABLE} (ts, device_id, metric, value) VALUES (?, ?, ?, ?)"
        seen_devices: set[str] = set()
        per_metric: Counter = Counter()
        done = 0

        for chunk in read_chunks(path, batch_rows):
            _tally(chunk, seen_devices, per_metric)
            self._con.execute("BEGIN")
            # zip() stays lazy, so the batch is never duplicated as a list of
            # tuples on top of the columns it was built from.
            self._con.executemany(insert, zip(chunk.ts.tolist(), chunk.device_id,
                                              chunk.metric, chunk.value.tolist()))
            self._con.execute("COMMIT")
            done += len(chunk.ts)
            self._tick(done, total, started)

        self._say("будую індекс (device_id, metric, ts)…")
        self._con.execute(self._INDEX)
        self._write_summary(sorted(seen_devices), per_metric)
        self._con.execute("ANALYZE")
        # Back to a single self-contained file the student can copy or submit.
        self._con.execute("PRAGMA journal_mode = DELETE")
        self._con.execute("PRAGMA synchronous = FULL")

        size_mb = self._path.stat().st_size / 1e6 if self._path.exists() else 0.0
        self._tick_done(done, started, f"файл {size_mb:.0f} МБ")
        return done

    def series(self, device_id: str, metric: str,
               start: Any = None, end: Any = None) -> tuple[np.ndarray, np.ndarray]:
        lo, hi = self._window(start, end)
        sql = [f"SELECT ts, value FROM {TABLE} WHERE device_id = ? AND metric = ?"]
        args: list[Any] = [device_id, metric]
        if lo is not None:
            sql.append("AND ts >= ?")
            args.append(lo)
        if hi is not None:
            sql.append("AND ts < ?")
            args.append(hi)
        sql.append("ORDER BY ts")
        rows = self._fetch(" ".join(sql), args)
        if not rows:
            self._explain_empty(device_id, metric)
        n = len(rows)
        return (np.fromiter((r[0] for r in rows), dtype=TS_DTYPE, count=n),
                np.fromiter((r[1] for r in rows), dtype=VALUE_DTYPE, count=n))

    def devices(self) -> list[str]:
        if self._has_summary("meta_device"):
            return [r[0] for r in self._fetch("SELECT device_id FROM meta_device ORDER BY 1")]
        return [r[0] for r in self._fetch(
            f"SELECT DISTINCT device_id FROM {TABLE} ORDER BY 1")]

    def metrics(self) -> list[str]:
        if self._has_summary("meta_metric"):
            return [r[0] for r in self._fetch("SELECT metric FROM meta_metric ORDER BY 1")]
        return [r[0] for r in self._fetch(
            f"SELECT DISTINCT metric FROM {TABLE} ORDER BY 1")]

    def row_count(self) -> int:
        if self._has_summary("meta_metric"):
            return int(self._fetch("SELECT COALESCE(sum(rows), 0) FROM meta_metric")[0][0])
        return int(self._fetch(f"SELECT count(*) FROM {TABLE}")[0][0])

    def rows_per_metric(self) -> dict[str, int]:
        if self._has_summary("meta_metric"):
            return {r[0]: int(r[1]) for r in
                    self._fetch("SELECT metric, rows FROM meta_metric ORDER BY 1")}
        return {r[0]: int(r[1]) for r in self._fetch(
            f"SELECT metric, count(*) FROM {TABLE} GROUP BY metric ORDER BY 1")}

    def close(self) -> None:
        if not self._closed:
            self._con.close()
            self._closed = True

    def _write_summary(self, device_ids: list[str], per_metric: dict[str, int]) -> None:
        """
        Persist the counts collected during the load.

        `SELECT metric, count(*) GROUP BY metric` over 5 million rows takes
        seconds every time it is called, and the картка результатів calls it on
        every run. The counts were already known while loading, so store them.
        """
        self._con.execute("CREATE TABLE meta_metric (metric TEXT PRIMARY KEY, rows INTEGER NOT NULL)")
        self._con.execute("CREATE TABLE meta_device (device_id TEXT PRIMARY KEY)")
        self._con.execute("BEGIN")
        self._con.executemany("INSERT INTO meta_metric VALUES (?, ?)",
                              sorted(per_metric.items()))
        self._con.executemany("INSERT INTO meta_device VALUES (?)",
                              ((d,) for d in device_ids))
        self._con.execute("COMMIT")

    def _has_summary(self, name: str) -> bool:
        return bool(self._fetch(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", [name]))

    def _fetch(self, sql: str, args: list[Any] | None = None) -> list[tuple]:
        try:
            return self._con.execute(sql, args or []).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                raise StorageError(
                    "Дані ще не завантажені. Спочатку викличте "
                    "load_parquet(шлях_до_parquet)."
                ) from exc
            raise StorageError(
                f"Помилка запиту SQLite.\nЗапит: {sql}\nДеталі: {exc}") from exc


# ==========================================================================
# 3. InfluxDB 2.x
# ==========================================================================

class InfluxDBStorage(Storage):
    """
    InfluxDB 2.x over the HTTP API (influxdb-client).

    Schema mapping, which is the whole design decision here:

        measurement = metric        temperature, co2, pkt_sent, …
        tag         device_id       ~50 values, well inside Influx's comfort
        field       value           one float field per point

    Metric-as-measurement keeps tag cardinality at the number of devices.  The
    tempting alternative — one measurement with metric as a second tag — makes
    the series cardinality devices × metrics and makes `metrics()` a tag scan
    instead of a cheap metadata lookup.

    Loading goes through raw line protocol rather than `Point` objects: a Point
    costs about 30x more to build than the string it turns into, and at 5
    million points that is the difference between minutes and tens of minutes.
    Writes are synchronous in fixed-size chunks so that a slow server applies
    back-pressure instead of letting an async queue grow without bound.

    One property of InfluxDB that the other three backends do not share: a
    point is identified by (measurement, tag set, field key, timestamp), so two
    rows with the same device_id + metric + ts are not two points — the second
    overwrites the first.  Variant data does contain a few such collisions (the
    clock_skew incident shifts timestamps onto each other), so row_count() here
    can come out a handful of rows below the Parquet row count.  The loader
    checks for exactly this and says so; it is a property of the database, not
    a fault in the load.
    """

    kind = "influxdb"

    def __init__(self, url: str = "http://localhost:8086",
                 token: str = "kursova-dev-token",
                 org: str = "kursova", bucket: str = "iot", *,
                 timeout_s: int = 300, chunk_lines: int = 25_000,
                 value_decimals: int | None = 3, progress: bool = True) -> None:
        super().__init__(progress=progress)
        client_mod = _import_driver(
            "influxdb_client", "influxdb-client",
            note="І не забудьте підняти сервер:  docker compose -f pipeline/docker-compose.yml up -d")
        self._api = _import_driver("influxdb_client.client.write_api", "influxdb-client")
        self._bucket, self._org = bucket, org
        self._url = url
        self._precision = client_mod.WritePrecision.S
        self._chunk_lines = max(1, chunk_lines)
        # The file stores float32 rounded to at most 3 decimals, so emitting
        # the short decimal form instead of the full float64 expansion of a
        # float32 (21.340000152587891) halves the payload and still round-trips
        # exactly back to the same float32.  None = full precision.
        self._decimals = value_decimals
        try:
            self._client = client_mod.InfluxDBClient(
                url=url, token=token, org=org, timeout=timeout_s * 1000)
        except Exception as exc:
            raise StorageError(self._conn_hint(exc)) from exc

    def load_parquet(self, path: str | Path,
                     batch_rows: int = DEFAULT_BATCH_ROWS) -> int:
        total = parquet_rows(path)
        self._ensure_bucket()
        self._wipe()
        started = time.perf_counter()

        write_api = self._client.write_api(write_options=self._api.SYNCHRONOUS)
        done = 0
        try:
            for chunk in read_chunks(path, batch_rows):
                lines = self._to_line_protocol(chunk)
                for i in range(0, len(lines), self._chunk_lines):
                    self._write(write_api, lines[i:i + self._chunk_lines])
                done += len(chunk.ts)
                self._tick(done, total, started)
        finally:
            write_api.close()
        self._tick_done(done, started)
        self._check_overwrites(done)
        return done

    def _check_overwrites(self, sent: int) -> None:
        """
        Tell the student when InfluxDB collapsed rows that share a timestamp.

        Silently losing a handful of rows is the single most confusing thing
        that can happen on this backend: the картка результатів compares the
        row count against the fingerprint, and the student has no way to guess
        why their number is seven short.
        """
        stored = self.row_count()
        if stored == sent:
            return
        self._warn(
            f"у сховищі {_num(stored)} точок замість {_num(sent)} прочитаних "
            f"(різниця {_num(sent - stored)}).\n"
            f"  InfluxDB вважає точку унікальною за (measurement, теги, "
            f"timestamp), тому рядки з однаковими device_id + metric + ts\n"
            f"  перезаписують один одного. У ваших даних такі збіги створює "
            f"інцидент зі зсувом годинника.\n"
            f"  Це не помилка завантаження. У картці результатів наводьте "
            f"число, яке повернув load_parquet() — стільки рядків у Parquet.")

    def series(self, device_id: str, metric: str,
               start: Any = None, end: Any = None) -> tuple[np.ndarray, np.ndarray]:
        lo, hi = self._window(start, end)
        flux = f"""
            from(bucket: "{self._bucket}")
              |> range(start: {_to_rfc3339(lo) if lo is not None else FLUX_MIN},
                       stop:  {_to_rfc3339(hi) if hi is not None else FLUX_MAX})
              |> filter(fn: (r) => r._measurement == "{_esc_flux(metric)}")
              |> filter(fn: (r) => r.device_id == "{_esc_flux(device_id)}")
              |> filter(fn: (r) => r._field == "value")
              |> keep(columns: ["_time", "_value"])
              |> sort(columns: ["_time"])
        """
        ts: list[int] = []
        values: list[float] = []
        for record in self._stream(flux):
            ts.append(int(record.get_time().timestamp()))
            values.append(record.get_value())
        if not ts:
            self._explain_empty(device_id, metric)
        return _as_arrays(ts, values)

    def devices(self) -> list[str]:
        # schema.tagValues defaults to the last 30 days; variant data is months
        # old, so the explicit start is not optional.
        flux = f"""
            import "influxdata/influxdb/schema"
            schema.tagValues(bucket: "{self._bucket}", tag: "device_id",
                             start: {FLUX_MIN})
        """
        return sorted(str(r.get_value()) for r in self._stream(flux))

    def metrics(self) -> list[str]:
        flux = f"""
            import "influxdata/influxdb/schema"
            schema.measurements(bucket: "{self._bucket}", start: {FLUX_MIN})
        """
        return sorted(str(r.get_value()) for r in self._stream(flux))

    def row_count(self) -> int:
        return sum(self.rows_per_metric().values())

    def rows_per_metric(self) -> dict[str, int]:
        flux = f"""
            from(bucket: "{self._bucket}")
              |> range(start: {FLUX_MIN}, stop: {FLUX_MAX})
              |> filter(fn: (r) => r._field == "value")
              |> group(columns: ["_measurement"])
              |> count()
        """
        out: dict[str, int] = {}
        for record in self._stream(flux):
            out[str(record.values.get("_measurement"))] = int(record.get_value())
        return dict(sorted(out.items()))

    def close(self) -> None:
        if not self._closed:
            self._client.close()
            self._closed = True

    # -- internals ----------------------------------------------------------

    def _to_line_protocol(self, chunk: Chunk) -> list[str]:
        values = chunk.value.astype(np.float64)
        if self._decimals is not None:
            values = np.round(values, self._decimals)
        return [f"{_esc_lp(metric)},device_id={_esc_lp(device)} value={value} {ts}"
                for metric, device, value, ts
                in zip(chunk.metric, chunk.device_id, values.tolist(), chunk.ts.tolist())]

    def _write(self, write_api: Any, lines: list[str]) -> None:
        try:
            write_api.write(bucket=self._bucket, org=self._org,
                            record=lines, write_precision=self._precision)
        except Exception as exc:
            raise StorageError(
                f"InfluxDB відхилив запис {len(lines)} точок.\n"
                f"Найчастіші причини: невірний токен, не той bucket, "
                f"або сервер не встигає.\nДеталі: {exc}") from exc

    def _ensure_bucket(self) -> None:
        """Create the bucket with infinite retention if it is not there yet."""
        try:
            buckets = self._client.buckets_api()
            if buckets.find_bucket_by_name(self._bucket) is not None:
                return
            # retention_rules=[] means "keep forever". With a finite retention
            # the 2025 data would be expired the moment it lands.
            buckets.create_bucket(bucket_name=self._bucket, org=self._org,
                                  retention_rules=[])
            self._say(f"створено bucket «{self._bucket}»")
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(self._conn_hint(exc)) from exc

    def _wipe(self) -> None:
        """Make the load idempotent: drop whatever a previous run wrote."""
        try:
            self._client.delete_api().delete(
                start=datetime(1970, 1, 1, tzinfo=timezone.utc),
                stop=datetime(2100, 1, 1, tzinfo=timezone.utc),
                predicate="", bucket=self._bucket, org=self._org)
        except Exception as exc:
            # Not fatal: an empty bucket is the normal case, and a student who
            # loads twice gets duplicate points rather than a failed run.
            self._say(f"попередження: не вдалося очистити bucket ({exc}); "
                      f"якщо завантажуєте повторно — видаліть bucket вручну")

    def _stream(self, flux: str) -> Iterator[Any]:
        try:
            yield from self._client.query_api().query_stream(flux, org=self._org)
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(self._conn_hint(exc, flux)) from exc

    def _conn_hint(self, exc: Exception, flux: str = "") -> str:
        # Flattened to one line: a pretty-printed Flux block buries the part of
        # the message the student can actually act on.
        tail = f"\nЗапит Flux: {' '.join(flux.split())}" if flux else ""
        return (f"Не вдалося звернутися до InfluxDB за адресою {self._url}.\n"
                f"Перевірте, що сервер запущено:\n"
                f"    docker compose -f pipeline/docker-compose.yml up -d\n"
                f"    docker compose -f pipeline/docker-compose.yml ps\n"
                f"і що токен/організація/bucket збігаються з docker-compose.yml.\n"
                f"Деталі: {exc}{tail}")


def _esc_lp(text: str) -> str:
    """Escape a line-protocol measurement name or tag value."""
    return text.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")


def _esc_flux(text: str) -> str:
    """Escape a Flux string literal."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


# ==========================================================================
# 4. TimescaleDB (PostgreSQL)
# ==========================================================================

class TimescaleDBStorage(Storage):
    """
    PostgreSQL + TimescaleDB: a hypertable loaded with binary COPY.

    Design decisions worth writing up:

      * ts is `timestamptz`, not a bigint of epoch seconds.  Timescale supports
        both, but timestamptz is what time_bucket(), Grafana's Postgres data
        source and every tutorial expect, and the course's dashboard stage
        depends on that.  The cost is building one datetime per row during the
        load, which the batching keeps bounded.
      * the load uses COPY ... FORMAT BINARY, not INSERT.  INSERT of 5 million
        rows through psycopg is dominated by per-statement overhead; binary
        COPY sends the same rows as a single framed stream, with no text
        parsing on the server side.
      * the secondary index is created after the COPY, for the same reason as
        in SQLite.  Timescale's own index on ts is created with the
        hypertable and left alone.
    """

    kind = "timescaledb"

    def __init__(self, dsn: str | None = None, *, host: str = "localhost",
                 port: int = 5432, user: str = "student",
                 password: str = "kursova2025", dbname: str = "iot",
                 chunk_interval_days: int = 7, progress: bool = True) -> None:
        super().__init__(progress=progress)
        self._psycopg = _import_driver(
            "psycopg", '"psycopg[binary]"',
            note="І не забудьте підняти сервер:  docker compose -f pipeline/docker-compose.yml up -d")
        self._dsn = dsn or (f"host={host} port={port} user={user} "
                            f"password={password} dbname={dbname}")
        self._where = dsn or f"{host}:{port}/{dbname}"
        self._chunk_days = max(1, chunk_interval_days)
        try:
            self._con = self._psycopg.connect(self._dsn, autocommit=True)
        except Exception as exc:
            raise StorageError(self._conn_hint(exc)) from exc

    @property
    def connection(self) -> Any:
        """Escape hatch for stage 3: raw SQL against the same connection."""
        return self._con

    def load_parquet(self, path: str | Path,
                     batch_rows: int = DEFAULT_BATCH_ROWS) -> int:
        total = parquet_rows(path)
        self._prepare_schema()
        started = time.perf_counter()

        copy_sql = (f"COPY {TABLE} (ts, device_id, metric, value) "
                    f"FROM STDIN (FORMAT BINARY)")
        seen_devices: set[str] = set()
        per_metric: Counter = Counter()
        done = 0

        try:
            with self._con.cursor() as cur:
                for chunk in read_chunks(path, batch_rows):
                    _tally(chunk, seen_devices, per_metric)
                    # One COPY per batch, not one for the whole file: a failure
                    # halfway through then costs one batch, and the server gets
                    # a commit boundary it can checkpoint at.
                    with cur.copy(copy_sql) as copy:
                        copy.set_types(["timestamptz", "text", "text", "float4"])
                        for row in self._rows(chunk):
                            copy.write_row(row)
                    done += len(chunk.ts)
                    self._tick(done, total, started)
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(
                f"Помилка під час COPY у TimescaleDB після {_num(done)} рядків.\n"
                f"Деталі: {exc}") from exc

        self._say("будую індекс (device_id, metric, ts)…")
        with self._con.cursor() as cur:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_series "
                        f"ON {TABLE} (device_id, metric, ts DESC)")
            self._write_summary(cur, sorted(seen_devices), per_metric)
            cur.execute(f"ANALYZE {TABLE}")
        self._tick_done(done, started)
        return done

    def series(self, device_id: str, metric: str,
               start: Any = None, end: Any = None) -> tuple[np.ndarray, np.ndarray]:
        lo, hi = self._window(start, end)
        sql = [f"SELECT extract(epoch FROM ts)::bigint, value FROM {TABLE} "
               f"WHERE device_id = %s AND metric = %s"]
        args: list[Any] = [device_id, metric]
        if lo is not None:
            sql.append("AND ts >= to_timestamp(%s)")
            args.append(lo)
        if hi is not None:
            sql.append("AND ts < to_timestamp(%s)")
            args.append(hi)
        sql.append("ORDER BY ts")
        rows = self._fetch(" ".join(sql), args)
        if not rows:
            self._explain_empty(device_id, metric)
        n = len(rows)
        return (np.fromiter((r[0] for r in rows), dtype=TS_DTYPE, count=n),
                np.fromiter((r[1] for r in rows), dtype=VALUE_DTYPE, count=n))

    def devices(self) -> list[str]:
        if self._has_summary("meta_device"):
            return [r[0] for r in self._fetch("SELECT device_id FROM meta_device ORDER BY 1")]
        return [r[0] for r in self._fetch(
            f"SELECT DISTINCT device_id FROM {TABLE} ORDER BY 1")]

    def metrics(self) -> list[str]:
        if self._has_summary("meta_metric"):
            return [r[0] for r in self._fetch("SELECT metric FROM meta_metric ORDER BY 1")]
        return [r[0] for r in self._fetch(
            f"SELECT DISTINCT metric FROM {TABLE} ORDER BY 1")]

    def row_count(self) -> int:
        if self._has_summary("meta_metric"):
            return int(self._fetch("SELECT COALESCE(sum(rows), 0) FROM meta_metric")[0][0])
        return int(self._fetch(f"SELECT count(*) FROM {TABLE}")[0][0])

    def rows_per_metric(self) -> dict[str, int]:
        if self._has_summary("meta_metric"):
            return {r[0]: int(r[1]) for r in
                    self._fetch("SELECT metric, rows FROM meta_metric ORDER BY 1")}
        return {r[0]: int(r[1]) for r in self._fetch(
            f"SELECT metric, count(*) FROM {TABLE} GROUP BY metric ORDER BY 1")}

    def close(self) -> None:
        if not self._closed:
            self._con.close()
            self._closed = True

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _rows(chunk: Chunk) -> Iterator[tuple]:
        """Generator, so a batch is never duplicated as a list of tuples."""
        utc = timezone.utc
        return zip((datetime.fromtimestamp(t, utc) for t in chunk.ts.tolist()),
                   chunk.device_id, chunk.metric, chunk.value.tolist())

    def _prepare_schema(self) -> None:
        try:
            with self._con.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
                # Rebuild from scratch so a rerun cannot double the dataset.
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                cur.execute("DROP TABLE IF EXISTS meta_metric")
                cur.execute("DROP TABLE IF EXISTS meta_device")
                cur.execute(f"""
                    CREATE TABLE {TABLE} (
                        ts        timestamptz NOT NULL,
                        device_id text        NOT NULL,
                        metric    text        NOT NULL,
                        value     real        NOT NULL
                    )""")
                # A variant covers ~60 days; 7-day chunks give ~9 chunks, which
                # is enough for chunk exclusion to help without drowning the
                # planner in partitions.
                cur.execute(
                    f"SELECT create_hypertable('{TABLE}', 'ts', "
                    f"chunk_time_interval => INTERVAL '{self._chunk_days} days')")
        except Exception as exc:
            if "timescaledb" in str(exc).lower():
                raise StorageError(
                    f"Розширення TimescaleDB недоступне у цій базі.\n"
                    f"Переконайтеся, що запущено саме образ timescale/timescaledb "
                    f"з docker-compose.yml, а не звичайний PostgreSQL.\n"
                    f"Деталі: {exc}") from exc
            raise StorageError(self._conn_hint(exc)) from exc

    def _write_summary(self, cur: Any, device_ids: list[str],
                       per_metric: dict[str, int]) -> None:
        """Same reasoning as SQLiteStorage._write_summary."""
        cur.execute("CREATE TABLE meta_metric (metric text PRIMARY KEY, rows bigint NOT NULL)")
        cur.execute("CREATE TABLE meta_device (device_id text PRIMARY KEY)")
        with cur.copy("COPY meta_metric (metric, rows) FROM STDIN (FORMAT BINARY)") as copy:
            copy.set_types(["text", "int8"])
            for name, count in sorted(per_metric.items()):
                copy.write_row((name, count))
        with cur.copy("COPY meta_device (device_id) FROM STDIN (FORMAT BINARY)") as copy:
            copy.set_types(["text"])
            for device in device_ids:
                copy.write_row((device,))

    def _has_summary(self, name: str) -> bool:
        return bool(self._fetch("SELECT to_regclass(%s) IS NOT NULL", [name])[0][0])

    def _fetch(self, sql: str, args: list[Any] | None = None) -> list[tuple]:
        try:
            with self._con.cursor() as cur:
                cur.execute(sql, args or [])
                return cur.fetchall()
        except Exception as exc:
            if "does not exist" in str(exc) and TABLE in str(exc):
                raise StorageError(
                    "Дані ще не завантажені. Спочатку викличте "
                    "load_parquet(шлях_до_parquet)."
                ) from exc
            raise StorageError(
                f"Помилка запиту TimescaleDB.\nЗапит: {sql}\nДеталі: {exc}") from exc

    def _conn_hint(self, exc: Exception) -> str:
        return (f"Не вдалося підʼєднатися до TimescaleDB ({self._where}).\n"
                f"Перевірте, що сервер запущено:\n"
                f"    docker compose -f pipeline/docker-compose.yml up -d\n"
                f"    docker compose -f pipeline/docker-compose.yml ps\n"
                f"і що логін/пароль/база збігаються з docker-compose.yml.\n"
                f"Деталі: {exc}")


# ==========================================================================
# factory
# ==========================================================================

_BACKENDS: dict[str, type[Storage]] = {
    "duckdb": DuckDBStorage,
    "sqlite": SQLiteStorage,
    "influxdb": InfluxDBStorage,
    "timescaledb": TimescaleDBStorage,
}

# The variant manifest spells the stack out in prose ("DuckDB + Parquet"), and
# that is the string a student has in front of them, so accept it verbatim
# along with the obvious short forms.
_ALIASES = {
    "duckdb + parquet": "duckdb", "duck": "duckdb", "duckdb+parquet": "duckdb",
    "sqlite + parquet": "sqlite", "sqlite3": "sqlite", "sqlite+parquet": "sqlite",
    "influx": "influxdb", "influxdb2": "influxdb", "influxdb 2.x": "influxdb",
    "timescale": "timescaledb", "timescaledb + postgresql": "timescaledb",
    "postgres": "timescaledb", "postgresql": "timescaledb",
}


def normalise_kind(kind: str) -> str:
    """Map whatever the manifest or the student typed onto a backend key."""
    key = " ".join(str(kind).strip().lower().split())
    key = _ALIASES.get(key, key)
    if key not in _BACKENDS:
        raise StorageError(
            f"Невідоме сховище {kind!r}.\n"
            f"Доступні варіанти: {', '.join(sorted(_BACKENDS))}.\n"
            f"Назву свого сховища дивіться у файлі variant_NNN.json, "
            f"поле assigned_stack.storage.")
    return key


def open_storage(kind: str, **kw: Any) -> Storage:
    """
    Open the storage backend assigned to this variant.

        open_storage("duckdb", database="iot.duckdb")
        open_storage("sqlite", path="iot.sqlite")
        open_storage("influxdb", url="http://localhost:8086", bucket="iot")
        open_storage("timescaledb", host="localhost", port=5432)

    Every keyword is optional; the defaults match docker-compose.yml, so on a
    machine where `docker compose up -d` has been run, open_storage(kind) alone
    is enough.

    The two file-backed stores take either `path` or `database` for the file
    name. They are assigned, not chosen, so a student only ever reads the
    example for the one backend they were given — and would otherwise hit a
    TypeError purely because the other one spells the argument differently.
    """
    key = normalise_kind(kind)
    backend = _BACKENDS[key]

    if key in ("duckdb", "sqlite"):
        own = "database" if key == "duckdb" else "path"
        other = "path" if key == "duckdb" else "database"
        if other in kw and own not in kw:
            kw[own] = kw.pop(other)

    try:
        return backend(**kw)
    except StorageError:
        raise
    except TypeError as exc:
        raise StorageError(
            f"Невірні параметри для сховища «{kind}»: {exc}\n"
            f"Перевірте назви аргументів у docstring класу {backend.__name__}."
        ) from exc


# ==========================================================================
# self-test CLI
# ==========================================================================

def _peak_rss_mb() -> float:
    """Peak memory of this process, in MB (NaN if it can't be measured)."""
    try:
        import resource                       # POSIX only
    except ImportError:                       # Windows: psutil's peak working set
        try:
            import psutil
        except ImportError:
            return float("nan")
        mem = psutil.Process().memory_info()
        return getattr(mem, "peak_wset", mem.rss) / 1e6
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports kibibytes.
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3


def _expected(path: str | Path, device_id: str, metric: str) -> tuple[int, int]:
    """
    Ground truth for one series, computed straight from the Parquet.

    The self-test compares every backend against this, so a loader that drops,
    duplicates or mangles rows is caught immediately instead of surfacing as a
    strange anomaly three stages later.
    """
    rows, ts_sum = 0, 0
    for chunk in read_chunks(path, 500_000):
        hit = np.fromiter((d == device_id and m == metric
                           for d, m in zip(chunk.device_id, chunk.metric)),
                          dtype=bool, count=len(chunk.ts))
        rows += int(hit.sum())
        ts_sum += int(chunk.ts[hit].sum())
    return rows, ts_sum


def _pick_pair(path: str | Path) -> tuple[str, str]:
    """A (device_id, metric) pair that is guaranteed to exist in the file."""
    chunk = next(read_chunks(path, 10_000))
    return chunk.device_id[0], chunk.metric[0]


def _selftest(args: argparse.Namespace) -> int:
    kind = normalise_kind(args.kind)
    options: dict[str, Any] = {"progress": not args.quiet}
    if kind == "duckdb" and args.db:
        options["database"] = args.db
    if kind == "duckdb" and args.materialise:
        options["materialise"] = True
    if kind == "sqlite":
        options["path"] = args.db or "iot.sqlite"
    # Only needed when the student had to move a port in docker-compose.yml.
    if kind == "influxdb" and args.url:
        options["url"] = args.url
    if kind == "timescaledb" and args.dsn:
        options["dsn"] = args.dsn

    print(f"\n=== Сховище: {kind} ===")
    storage = open_storage(kind, **options)
    try:
        started = time.perf_counter()
        rows = storage.load_parquet(args.parquet, batch_rows=args.batch_rows)
        load_s = time.perf_counter() - started

        expected_rows = parquet_rows(args.parquet)
        ok = rows == expected_rows
        print(f"  рядків завантажено : {_num(rows)} з {_num(expected_rows)} "
              f"{'✓' if ok else '✗ НЕ ЗБІГАЄТЬСЯ'}")
        print(f"  час завантаження   : {load_s:.2f} с "
              f"({_num(int(rows / max(load_s, 1e-9)))} рядків/с)")

        started = time.perf_counter()
        devices = storage.devices()
        metrics = storage.metrics()
        meta_s = time.perf_counter() - started
        print(f"  пристроїв / метрик : {len(devices)} / {len(metrics)}  ({meta_s:.2f} с)")

        started = time.perf_counter()
        per_metric = storage.rows_per_metric()
        card_s = time.perf_counter() - started
        print(f"  rows_per_metric    : {len(per_metric)} метрик, "
              f"сума {_num(sum(per_metric.values()))}  ({card_s:.2f} с)")

        started = time.perf_counter()
        total = storage.row_count()
        count_s = time.perf_counter() - started
        print(f"  row_count()        : {_num(total)}  ({count_s:.2f} с)")

        fallback_device, fallback_metric = _pick_pair(args.parquet)
        device_id = args.device or fallback_device
        metric = args.metric or fallback_metric
        started = time.perf_counter()
        ts, value = storage.series(device_id, metric)
        query_s = time.perf_counter() - started
        print(f"  series({device_id}, {metric})")
        print(f"    точок            : {_num(len(ts))}  ({query_s * 1000:.0f} мс)")
        print(f"    dtype            : {ts.dtype} / {value.dtype}")

        want_rows, want_ts_sum = _expected(args.parquet, device_id, metric)
        exact = (len(ts) == want_rows and int(ts.sum()) == want_ts_sum
                 and bool(np.all(np.diff(ts) >= 0)))
        print(f"    звірка з Parquet : {'✓ збігається' if exact else '✗ РОЗБІЖНІСТЬ'} "
              f"(очікувано {_num(want_rows)} точок)")

        if len(ts) > 2:
            half = int(ts[len(ts) // 2])
            started = time.perf_counter()
            ts2, _ = storage.series(device_id, metric, start=half)
            window_s = time.perf_counter() - started
            want = int((ts >= half).sum())
            print(f"    вікно start=ts   : {_num(len(ts2))} точок "
                  f"{'✓' if len(ts2) == want else '✗'}  ({window_s * 1000:.0f} мс)")

        print(f"  пікова памʼять     : {_peak_rss_mb():.0f} МБ")
        return 0 if (ok and exact) else 1
    finally:
        storage.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Завантаження варіанта у сховище + самоперевірка адаптера.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kind", required=True,
                        help="duckdb | sqlite | influxdb | timescaledb")
    parser.add_argument("--parquet", required=True, help="шлях до variant_NNN.parquet")
    parser.add_argument("--db", default=None, help="файл БД для duckdb/sqlite")
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS,
                        help=f"рядків у пакеті (типово {DEFAULT_BATCH_ROWS})")
    parser.add_argument("--materialise", action="store_true",
                        help="duckdb: копіювати у таблицю замість подання над Parquet")
    parser.add_argument("--url", default=None,
                        help="influxdb: адреса сервера, якщо змінили порт")
    parser.add_argument("--dsn", default=None,
                        help="timescaledb: рядок підʼєднання, якщо змінили порт")
    parser.add_argument("--device", default=None, help="device_id для тестового запиту")
    parser.add_argument("--metric", default=None, help="метрика для тестового запиту")
    parser.add_argument("--quiet", action="store_true", help="без індикатора прогресу")
    args = parser.parse_args(argv)

    try:
        return _selftest(args)
    except StorageError as exc:
        # The whole point of StorageError: a student sees instructions, not a
        # stack trace they cannot read.
        sys.stdout.flush()          # keep the report above the error message
        print(f"\nПОМИЛКА\n{exc}\n", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nПерервано користувачем.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
