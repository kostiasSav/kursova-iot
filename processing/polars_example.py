#!/usr/bin/env python3
"""
Стадія 2 — обробка даних: Polars, ліниве сканування (lazy scan).

Усі чотири приклади в цій теці рахують з одного файла Parquet одне й те саме:

  1. добову статистику за кожним пристроєм і показником — кількість
     записів, середнє, мінімум, максимум       -> daily_stats.parquet
  2. кількість повідомлень від кожного пристрою за кожну годину
                                                -> hourly_messages.parquet

Як це працює тут: pl.scan_parquet() нічого не читає одразу, а лише описує
запит (LazyFrame). Polars спершу будує план — бере з файла тільки потрібні
стовпці, — а виконує його потоковим рушієм (engine="streaming"): дані
проходять порціями, і весь файл не мусить уміщатися в пам'ять. Обидва
результати рахуються одним викликом collect_all().

Час — UTC, як і в самих даних (ts — секунди епохи UTC).

Запуск із кореня репозиторію:
    python processing/polars_example.py --data tuning.parquet
    python processing/polars_example.py --data variant_047.parquet --out results --head 10
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from peakmem import peak_rss_mb      # файл поруч: пам'ять на всіх ОС

try:
    import polars as pl
except ImportError:
    sys.exit("Не встановлено Polars. Виконайте:  pip install polars")


def plan(path: Path) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Описує обидва запити; дані ще не читаються."""
    lf = pl.scan_parquet(path).with_columns(
        pl.col("device_id").cast(pl.String),
        pl.col("metric").cast(pl.String),
        pl.col("value").cast(pl.Float64),
        when=pl.from_epoch("ts", time_unit="s"),     # секунди епохи -> дата й час UTC
    )
    daily = (lf.group_by("device_id", "metric", day=pl.col("when").dt.date())
               .agg(count=pl.len().cast(pl.Int64),
                    mean=pl.col("value").mean(),
                    min=pl.col("value").min(),
                    max=pl.col("value").max())
               .sort("device_id", "metric", "day"))
    hourly = (lf.group_by("device_id", hour=pl.col("when").dt.truncate("1h"))
                .agg(messages=pl.len().cast(pl.Int64))
                .sort("device_id", "hour"))
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

    rows = pl.scan_parquet(path).select(pl.len()).collect().item()
    print(f"Polars {pl.__version__}, ліниве сканування + потоковий рушій; "
          f"файл {path.name}: {num(rows)} записів")
    started = time.perf_counter()
    daily, hourly = pl.collect_all(plan(path), engine="streaming")
    seconds = time.perf_counter() - started
    print(f"Обчислено за {seconds:.2f} с ({num(rows / seconds)} записів/с); "
          f"пікова пам'ять процесу {peak_rss_mb():.0f} МБ\n")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, frame in (("daily_stats", daily), ("hourly_messages", hourly)):
        frame.write_parquet(out / f"{name}.parquet")
        print(f"{name}: {frame.height} рядків -> {out / (name + '.parquet')}")
        print(frame.head(args.head), "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
