#!/usr/bin/env python3
"""
Стадія 2 — обробка даних: pandas порціями (chunked).

Усі чотири приклади в цій теці рахують з одного файла Parquet одне й те саме:

  1. добову статистику за кожним пристроєм і показником — кількість
     записів, середнє, мінімум, максимум       -> daily_stats.parquet
  2. кількість повідомлень від кожного пристрою за кожну годину
                                                -> hourly_messages.parquet

Як це працює тут: файл читається порціями по 500 000 рядків (pyarrow
iter_batches), тож увесь файл ніколи не лежить у пам'яті. Для кожної порції
pandas рахує ЧАСТКОВІ підсумки — кількість, суму, мінімум, максимум, — а
наприкінці часткові підсумки однієї групи складаються. Середнє ділиться лише
в самому кінці (сума / кількість): середнє з середніх по порціях було б
неправильним, бо одна доба може потрапити у дві порції.

Час — UTC, як і в самих даних (ts — секунди епохи UTC).

Запуск із кореня репозиторію:
    python processing/pandas_chunked.py --data tuning.parquet
    python processing/pandas_chunked.py --data variant_047.parquet --out results --head 10
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from peakmem import peak_rss_mb      # файл поруч: пам'ять на всіх ОС

try:
    import pandas as pd
    import pyarrow.parquet as pq
except ImportError as exc:
    sys.exit(f"Не встановлено пакет «{exc.name}». Виконайте:  pip install pandas pyarrow")

BATCH_ROWS = 500_000      # більша порція — трохи швидше, але більше пам'яті
DAY_KEYS = ["device_id", "metric", "day"]
HOUR_KEYS = ["device_id", "hour"]


def process(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Один прохід файлом порціями; повертає (добова статистика, повідомлення за годину)."""
    daily_parts, hourly_parts = [], []
    reader = pq.ParquetFile(path)
    for batch in reader.iter_batches(batch_size=BATCH_ROWS,
                                     columns=["ts", "device_id", "metric", "value"]):
        df = batch.to_pandas()                    # device_id і metric -> category
        when = pd.to_datetime(df["ts"], unit="s")  # секунди епохи -> дата й час UTC
        df["day"] = when.dt.floor("D")
        df["hour"] = when.dt.floor("h")
        df["value"] = df["value"].astype("float64")

        part = (df.groupby(DAY_KEYS, observed=True)["value"]
                  .agg(["count", "sum", "min", "max"]).reset_index())
        daily_parts.append(part.astype({"device_id": str, "metric": str}))
        part = df.groupby(HOUR_KEYS, observed=True).size().rename("messages").reset_index()
        hourly_parts.append(part.astype({"device_id": str}))

    # Складаємо часткові підсумки: кількості й суми додаються, з мінімумів
    # береться мінімум, з максимумів — максимум.
    daily = (pd.concat(daily_parts, ignore_index=True)
               .groupby(DAY_KEYS, as_index=False)
               .agg(count=("count", "sum"), total=("sum", "sum"),
                    min=("min", "min"), max=("max", "max")))
    daily["mean"] = daily.pop("total") / daily["count"]
    daily["day"] = daily["day"].dt.date
    daily = daily[["device_id", "metric", "day", "count", "mean", "min", "max"]]

    hourly = (pd.concat(hourly_parts, ignore_index=True)
                .groupby(HOUR_KEYS, as_index=False)["messages"].sum())
    return daily, hourly


def num(n: float) -> str:
    """Число з пробілами між тисячами: 1 157 147."""
    return f"{n:,.0f}".replace(",", " ")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="tuning.parquet",
                    help="файл Parquet (типово tuning.parquet у поточній теці)")
    ap.add_argument("--out", default="results", help="тека для результатів (типово results)")
    ap.add_argument("--head", type=int, default=5, help="скільки рядків показати (типово 5)")
    args = ap.parse_args()

    path = Path(args.data)
    if not path.is_file():
        print(f"Не знайдено файл {path.resolve()}\nПокладіть tuning.parquet або свій "
              f"variant_NNN.parquet у поточну теку чи вкажіть шлях через --data.",
              file=sys.stderr)
        return 2

    rows = pq.ParquetFile(path).metadata.num_rows
    print(f"pandas {pd.__version__}, порції по {num(BATCH_ROWS)} рядків; "
          f"файл {path.name}: {num(rows)} записів")
    started = time.perf_counter()
    daily, hourly = process(path)
    seconds = time.perf_counter() - started
    print(f"Обчислено за {seconds:.2f} с ({num(rows / seconds)} записів/с); "
          f"пікова пам'ять процесу {peak_rss_mb():.0f} МБ\n")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, frame in (("daily_stats", daily), ("hourly_messages", hourly)):
        frame.to_parquet(out / f"{name}.parquet", index=False)
        print(f"{name}: {len(frame)} рядків -> {out / (name + '.parquet')}")
        print(frame.head(args.head).to_string(index=False), "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
