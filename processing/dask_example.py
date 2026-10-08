#!/usr/bin/env python3
"""
Стадія 2 — обробка даних: Dask DataFrame.

Усі чотири приклади в цій теці рахують з одного файла Parquet одне й те саме:

  1. добову статистику за кожним пристроєм і показником — кількість
     записів, середнє, мінімум, максимум       -> daily_stats.parquet
  2. кількість повідомлень від кожного пристрою за кожну годину
                                                -> hourly_messages.parquet

Як це працює тут: Dask ділить файл на розділи (partitions) — тут один
розділ = одна група рядків Parquet, близько 500 000 записів. Кожен розділ —
звичайний pandas DataFrame. Код нижче лише описує обчислення, а
dask.compute() виконує його: розділи обробляються паралельно на кількох
ядрах, і в пам'яті одночасно лежить лише кілька розділів, а не весь файл.

Час — UTC, як і в самих даних (ts — секунди епохи UTC).

Запуск із кореня репозиторію:
    python processing/dask_example.py --data tuning.parquet
    python processing/dask_example.py --data variant_047.parquet --out results --head 10
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from peakmem import peak_rss_mb      # файл поруч: пам'ять на всіх ОС

try:
    import dask
    import dask.dataframe as dd
    import pyarrow.parquet as pq
except ImportError:
    sys.exit('Не встановлено Dask. Виконайте:  pip install "dask[dataframe]"')


def plan(path: Path):
    """Описує обидва обчислення; дані ще не читаються."""
    ddf = dd.read_parquet(path, columns=["ts", "device_id", "metric", "value"],
                          split_row_groups=True)          # розділ = група рядків
    when = dd.to_datetime(ddf["ts"], unit="s")            # секунди епохи -> час UTC
    ddf = ddf.assign(device_id=ddf["device_id"].astype(str),
                     metric=ddf["metric"].astype(str),
                     value=ddf["value"].astype("float64"),
                     day=when.dt.floor("D"), hour=when.dt.floor("h"))
    daily = (ddf.groupby(["device_id", "metric", "day"])["value"]
                .agg(["count", "mean", "min", "max"]).reset_index())
    hourly = ddf.groupby(["device_id", "hour"]).size().rename("messages").reset_index()
    return daily, hourly, ddf.npartitions


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
    daily, hourly, parts = plan(path)
    print(f"Dask {dask.__version__}, розділів: {parts}; "
          f"файл {path.name}: {num(rows)} записів")
    started = time.perf_counter()
    daily, hourly = dask.compute(daily, hourly)          # тут усе й рахується
    seconds = time.perf_counter() - started
    print(f"Обчислено за {seconds:.2f} с ({num(rows / seconds)} записів/с); "
          f"пікова пам'ять процесу {peak_rss_mb():.0f} МБ\n")

    # Результати вже маленькі — звичайні pandas DataFrame
    daily = daily.sort_values(["device_id", "metric", "day"], ignore_index=True)
    daily["day"] = daily["day"].dt.date
    daily["count"] = daily["count"].astype("int64")
    hourly = hourly.sort_values(["device_id", "hour"], ignore_index=True)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, frame in (("daily_stats", daily), ("hourly_messages", hourly)):
        frame.to_parquet(out / f"{name}.parquet", index=False)
        print(f"{name}: {len(frame)} рядків -> {out / (name + '.parquet')}")
        print(frame.head(args.head).to_string(index=False), "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
