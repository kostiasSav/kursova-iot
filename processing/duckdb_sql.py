#!/usr/bin/env python3
"""
Стадія 2 — обробка даних: DuckDB SQL просто над файлом Parquet.

Усі чотири приклади в цій теці рахують з одного файла Parquet одне й те саме:

  1. добову статистику за кожним пристроєм і показником — кількість
     записів, середнє, мінімум, максимум       -> daily_stats.parquet
  2. кількість повідомлень від кожного пристрою за кожну годину
                                                -> hourly_messages.parquet

Як це працює тут: жодного завантаження немає — read_parquet() у SQL читає
файл напряму. DuckDB бере з файла лише потрібні стовпці, обробляє дані
порціями на всіх ядрах процесора і, якщо пам'яті забракне, сам скидає
проміжні дані на диск. Базу даних не створюємо: з'єднання живе лише в
пам'яті цього процесу.

Час — UTC, як і в самих даних: to_timestamp(ts) AT TIME ZONE 'UTC'.
Без «AT TIME ZONE 'UTC'» DuckDB показав би час у часовому поясі вашого
комп'ютера (Київ = UTC+3), і доби зсунулися б на три години.

Запуск із кореня репозиторію:
    python processing/duckdb_sql.py --data tuning.parquet
    python processing/duckdb_sql.py --data variant_047.parquet --out results --head 10
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from peakmem import peak_rss_mb      # файл поруч: пам'ять на всіх ОС

try:
    import duckdb
except ImportError:
    sys.exit("Не встановлено DuckDB. Виконайте:  pip install duckdb")

DAILY_SQL = """
    SELECT CAST(device_id AS VARCHAR)                         AS device_id,
           CAST(metric AS VARCHAR)                            AS metric,
           CAST(to_timestamp(ts) AT TIME ZONE 'UTC' AS DATE)  AS day,
           count(*)                                           AS count,
           avg(value)                                         AS mean,
           CAST(min(value) AS DOUBLE)                         AS min,
           CAST(max(value) AS DOUBLE)                         AS max
    FROM read_parquet($path)
    GROUP BY ALL
    ORDER BY ALL
"""

HOURLY_SQL = """
    SELECT CAST(device_id AS VARCHAR)                               AS device_id,
           date_trunc('hour', to_timestamp(ts) AT TIME ZONE 'UTC')  AS hour,
           count(*)                                                 AS messages
    FROM read_parquet($path)
    GROUP BY ALL
    ORDER BY ALL
"""


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

    con = duckdb.connect()                     # база лише в пам'яті, без файла
    params = {"path": str(path)}
    rows = con.execute("SELECT count(*) FROM read_parquet($path)", params).fetchone()[0]
    print(f"DuckDB {duckdb.__version__}, SQL над Parquet; "
          f"файл {path.name}: {num(rows)} записів")
    started = time.perf_counter()
    con.execute(f"CREATE TABLE daily_stats AS {DAILY_SQL}", params)
    con.execute(f"CREATE TABLE hourly_messages AS {HOURLY_SQL}", params)
    seconds = time.perf_counter() - started
    print(f"Обчислено за {seconds:.2f} с ({num(rows / seconds)} записів/с); "
          f"пікова пам'ять процесу {peak_rss_mb():.0f} МБ\n")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name in ("daily_stats", "hourly_messages"):
        table = con.table(name)
        table.write_parquet(str(out / f"{name}.parquet"))
        print(f"{name}: {len(table)} рядків -> {out / (name + '.parquet')}")
        table.limit(args.head).show()
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
