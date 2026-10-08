#!/usr/bin/env python3
"""
Готує дані для дашбордів Grafana (стадія 6).

Grafana читає бази даних, але не файли JSON. Тому скрипт кладе у ваше
сховище поруч із телеметрією три невеликі таблиці:
  devices  — пристрої з маніфесту: тип, зона, шлюз, інтервал;
  units    — одиниці вимірювання показників;
  findings — ваші знахідки з findings.json (для шкали аномалій).
Для SQLite і DuckDB скрипт ще й переносить саму телеметрію у файл
iot.sqlite у корені репозиторію (Grafana вміє читати SQLite, а DuckDB — ні)
і рахує таблицю hourly — підсумки за годину для швидкого огляду мережі.

Змінили findings.json або дані — запустіть скрипт ще раз і оновіть
сторінку Grafana.

Запуск (з кореня репозиторію):
    python dashboard/grafana/prepare.py --manifest variant_047.json \\
        --data variant_047.parquet --findings findings.json

Сховище береться з маніфесту (поле assigned_stack.storage). Інше можна
вказати явно: --storage sqlite | duckdb | influxdb | timescaledb.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]          # корінь репозиторію
sys.path.insert(0, str(REPO))                       # щоб знайти pipeline/storage.py
from pipeline.storage import (StorageError, normalise_kind,  # noqa: E402
                              open_storage, parquet_rows)

# Саме цей файл бачить Grafana (див. docker-compose.yml: корінь -> /kursova).
SQLITE_FILE = REPO / "iot.sqlite"
DASHBOARD_BUCKET = "dashboard"                      # окремий bucket InfluxDB

# Класи в порядку розділу 7. Номер класу — це «значення» на шкалі
# аномалій у Grafana; назву і колір йому задає дашборд.
CLASSES = ["stuck_at", "outage", "spike_storm", "rogue_device",
           "drift", "clock_skew", "beaconing", "energy_anomaly",
           "zone_incoherence", "gateway_group_loss", "exfil_pattern", "sensor_swap"]
COMPLEXITY = ["прості"] * 4 + ["середні"] * 4 + ["складні"] * 4
MAX_SCORED = 15                                     # оцінюються 15 знахідок (п. 6.2)


def to_epoch(value) -> int:
    """Час зі findings.json: секунди епохи або рядок ISO-8601."""
    if isinstance(value, (int, float)):
        return int(value)
    moment = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp())


def read_findings(path: Path) -> list[dict]:
    """findings.json -> список рядків для таблиці findings."""
    if not path.is_file():
        print(f"  Файл {path} не знайдено — шкала аномалій буде порожньою.\n"
              f"  (Для пробного запуску: --findings examples/findings.example.json)")
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for no, f in enumerate(doc.get("findings") or [], start=1):
        devices = f.get("devices") or []
        devices = [devices] if isinstance(devices, str) else [str(d) for d in devices]
        cls = str(f.get("class", "?"))
        code = CLASSES.index(cls) + 1 if cls in CLASSES else 99
        start, end = to_epoch(f["start"]), to_epoch(f["end"])
        pad = max(3600, (end - start) // 2)         # вікно для «відкрити графік»
        more = f" +{len(devices) - 2}" if len(devices) > 2 else ""
        rows.append({
            "no": no, "class": cls, "class_code": code,
            "complexity": COMPLEXITY[code - 1] if code != 99 else "невідомий клас",
            "devices": ",".join(devices), "first_device": devices[0] if devices else "",
            "start_ts": start, "end_ts": end, "hours": round((end - start) / 3600, 1),
            "confidence": float(f.get("confidence") or 0.0), "scored": 0,
            "evidence": str(f.get("evidence", "")),
            # Номер з нулем попереду (№01): так рядки шкали в Grafana йдуть по порядку.
            "label": f"№{no:02d} {cls} · {', '.join(devices[:2])}{more}",
            "zoom_from_ms": (start - pad) * 1000, "zoom_to_ms": (end + pad) * 1000,
        })
    # Буде оцінено 15 знахідок з найвищою впевненістю (за рівних — вищі у файлі).
    for row in sorted(rows, key=lambda r: (-r["confidence"], r["no"]))[:MAX_SCORED]:
        row["scored"] = 1
    return rows


# ----------------------------------------------------------------------
# SQLite і TimescaleDB: звичайні SQL-таблиці
# ----------------------------------------------------------------------

def write_sql(con, kind: str, manifest: dict, findings: list[dict]) -> None:
    """Перестворює таблиці devices, units і findings (повторний запуск безпечний)."""
    time_type = "INTEGER" if kind == "sqlite" else "timestamptz"
    mark = "?" if kind == "sqlite" else "%s"
    cur = con.cursor()
    for table in ("devices", "units", "findings"):
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute("CREATE TABLE devices (device_id TEXT PRIMARY KEY, type TEXT, zone TEXT, "
                "gateway TEXT, interval_s INTEGER, metrics TEXT)")
    cur.execute("CREATE TABLE units (metric TEXT PRIMARY KEY, unit TEXT)")
    cur.execute(f"CREATE TABLE findings (no INTEGER PRIMARY KEY, class TEXT, "
                f"class_code INTEGER, complexity TEXT, devices TEXT, first_device TEXT, "
                f"start_ts {time_type}, end_ts {time_type}, hours REAL, confidence REAL, "
                f"scored INTEGER, evidence TEXT, label TEXT, zoom_from_ms BIGINT, "
                f"zoom_to_ms BIGINT)")
    cur.executemany(f"INSERT INTO devices VALUES ({', '.join([mark] * 6)})", [
        (d["device_id"], d.get("type"), d.get("zone"), d.get("gateway"),
         d.get("interval_s"), ",".join(d.get("metrics", []))) for d in manifest["devices"]])
    cur.executemany(f"INSERT INTO units VALUES ({mark}, {mark})", [
        (m, v.get("unit", "")) for m, v in manifest.get("metrics", {}).items()])
    columns = list(findings[0]) if findings else []
    for row in findings:
        values = dict(row)
        if kind != "sqlite":                        # у TimescaleDB час — timestamptz
            for key in ("start_ts", "end_ts"):
                values[key] = datetime.fromtimestamp(row[key], timezone.utc)
        cur.execute(f"INSERT INTO findings ({', '.join(columns)}) "
                    f"VALUES ({', '.join([mark] * len(columns))})",
                    [values[c] for c in columns])
    if kind == "sqlite":
        con.commit()


def build_hourly(con) -> None:
    """SQLite: підсумки за кожну годину для оглядових панелей Grafana.

    Огляд мережі рахує повідомлення всіх пристроїв за весь період. Над 5–7 млн
    рядків SQLite робить це секундами на кожній панелі, а через Docker на
    Windows — ще повільніше. Таблиця hourly має ~100 тис. рядків, тож огляд
    відкривається швидко. Перебудовується при кожному запуску prepare.py.
    """
    con.execute("DROP TABLE IF EXISTS hourly")
    con.execute("CREATE TABLE hourly AS "
                "SELECT device_id, ts / 3600 * 3600 AS hour, count(*) AS n, "
                "       min(ts) AS first_ts, max(ts) AS last_ts "
                "FROM readings GROUP BY device_id, hour")
    con.execute("CREATE INDEX idx_hourly ON hourly (hour)")


# ----------------------------------------------------------------------
# InfluxDB: окремий bucket «dashboard», щоб не змішувати з телеметрією
# ----------------------------------------------------------------------

def write_influx(url: str, manifest: dict, findings: list[dict]) -> None:
    try:
        from influxdb_client import InfluxDBClient, Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS
    except ImportError:
        sys.exit("Не встановлено influxdb-client. Виконайте:  pip install influxdb-client")
    # Ті самі доступи, що в pipeline/docker-compose.yml і storage.py.
    with InfluxDBClient(url=url, token="kursova-dev-token", org="kursova") as client:
        buckets = client.buckets_api()
        if buckets.find_bucket_by_name(DASHBOARD_BUCKET) is None:
            buckets.create_bucket(bucket_name=DASHBOARD_BUCKET, org="kursova",
                                  retention_rules=[])        # зберігати назавжди
        client.delete_api().delete(datetime(1970, 1, 1, tzinfo=timezone.utc),
                                   datetime(2100, 1, 1, tzinfo=timezone.utc), "",
                                   bucket=DASHBOARD_BUCKET, org="kursova")
        start = to_epoch(manifest.get("period", {}).get("start", "2025-09-01T00:00:00Z"))
        points = []
        for d in manifest["devices"]:
            points.append(Point("devices").tag("device_id", d["device_id"])
                          .field("type", d.get("type", "")).field("zone", d.get("zone", ""))
                          .field("gateway", d.get("gateway", ""))
                          .field("interval_s", int(d.get("interval_s") or 0))
                          .time(start, WritePrecision.S))
        for metric, info in manifest.get("metrics", {}).items():
            points.append(Point("units").tag("metric", metric)
                          .field("unit", info.get("unit", "")).time(start, WritePrecision.S))
        for row in findings:
            point = Point("findings").tag("label", row["label"]).time(row["start_ts"],
                                                                      WritePrecision.S)
            for key, value in row.items():
                if key not in ("label", "start_ts"):
                    point.field(key, value)
            points.append(point)
            # Друга точка в момент завершення: на шкалі аномалій тут смуга обривається.
            points.append(Point("findings_end").tag("label", row["label"])
                          .field("class_code", 0).time(row["end_ts"], WritePrecision.S))
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=DASHBOARD_BUCKET, org="kursova", record=points)


# ----------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", default="tuning.json",
                    help="маніфест JSON (типово tuning.json)")
    ap.add_argument("--findings", default="findings.json",
                    help="ваші знахідки (типово findings.json)")
    ap.add_argument("--data", default=None,
                    help="файл Parquet; потрібен, якщо телеметрії у сховищі ще немає")
    ap.add_argument("--storage", default=None,
                    help="sqlite | duckdb | influxdb | timescaledb (типово — з маніфесту)")
    ap.add_argument("--reload", action="store_true",
                    help="завантажити телеметрію з --data заново")
    ap.add_argument("--db", default=str(SQLITE_FILE),
                    help="SQLite/DuckDB: файл бази (типово iot.sqlite у корені репозиторію; "
                         "саме його читає Grafana)")
    ap.add_argument("--url", default="http://localhost:8086",
                    help="InfluxDB: адреса сервера, якщо ви змінили порт")
    ap.add_argument("--dsn", default=None,
                    help="TimescaleDB: рядок підʼєднання, якщо ви змінили порт")
    args = ap.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.is_file():
        sys.exit(f"Не знайдено маніфест {manifest_path.resolve()}.\n"
                 f"Вкажіть свій файл: --manifest variant_NNN.json")
    if args.data and not Path(args.data).is_file():
        sys.exit(f"Не знайдено файл з даними {Path(args.data).resolve()}.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    try:
        kind = normalise_kind(args.storage or manifest["assigned_stack"]["storage"])
    except (KeyError, StorageError) as exc:
        sys.exit(f"Не вдалося визначити сховище: {exc}\nВкажіть його явно: --storage sqlite")
    try:
        findings = read_findings(Path(args.findings))
    except (ValueError, KeyError, TypeError) as exc:
        sys.exit(f"Не вдалося прочитати {args.findings}: {exc}\n"
                 f"Перевірте файл:  python selfcheck.py --findings {args.findings}")
    print(f"Сховище: {kind}; пристроїв у маніфесті: {len(manifest['devices'])}; "
          f"знахідок: {len(findings)}", flush=True)

    try:
        if kind in ("sqlite", "duckdb"):
            if kind == "duckdb":
                print("Grafana не читає DuckDB, тому телеметрія переноситься у SQLite.",
                      flush=True)
            if Path(args.db).resolve() != SQLITE_FILE:
                print(f"Увага: Grafana читає лише {SQLITE_FILE}, а не {args.db}.")
            storage = open_storage("sqlite", path=args.db)
        elif kind == "timescaledb":
            storage = open_storage("timescaledb", dsn=args.dsn)
        else:
            storage = open_storage("influxdb", url=args.url)
        try:
            rows = storage.row_count()
        except StorageError as exc:
            if "не завантажені" not in str(exc):    # сервер недоступний тощо
                raise
            rows = 0                                # таблиці з даними ще немає
        # У SQLite кількість рядків має точно збігатися з Parquet: інакше
        # у файлі лежить інший набір (наприклад, навчальний замість варіанта).
        stale = kind in ("sqlite", "duckdb") and args.data and rows != parquet_rows(args.data)
        if args.data and (args.reload or rows == 0 or stale):
            print(f"Завантажую {args.data} у сховище (до кількох хвилин)…", flush=True)
            rows = storage.load_parquet(args.data)
        elif rows == 0:
            sys.exit("У сховищі ще немає телеметрії. Додайте --data variant_NNN.parquet")
        print(f"Телеметрії у сховищі: {rows:,} рядків.".replace(",", " "))
        if kind == "influxdb":
            storage.close()
            write_influx(args.url, manifest, findings)
        else:
            write_sql(storage.connection, "timescaledb" if kind == "timescaledb" else "sqlite",
                      manifest, findings)
            if kind != "timescaledb":
                print("Рахую підсумки за годину (таблиця hourly)…", flush=True)
                build_hourly(storage.connection)
            storage.close()
    except StorageError as exc:
        sys.exit(f"\nПОМИЛКА\n{exc}")
    except Exception as exc:                        # noqa: BLE001 - пояснюємо будь-яку помилку
        sys.exit(f"\nПОМИЛКА: не вдалося записати таблиці для Grafana.\nДеталі: {exc}")

    print("Таблиці devices, units"
          + (", hourly" if kind in ("sqlite", "duckdb") else "")
          + " і findings записано"
          + (f" (bucket «{DASHBOARD_BUCKET}»)." if kind == "influxdb" else "."))
    print("\nТепер запустіть Grafana (якщо ще не запущена):\n"
          "    docker compose -f dashboard/grafana/docker-compose.yml up -d\n"
          "і відкрийте http://localhost:3000")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
