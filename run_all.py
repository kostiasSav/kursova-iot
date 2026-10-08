#!/usr/bin/env python3
"""
Увесь ваш конвеєр однією командою — для репетиції і для захисту.

    python run_all.py --data tuning.parquet --manifest tuning.json --storage duckdb

--storage — ВАШЕ призначене сховище (з вашого variant_NNN.json). На захисті
маніфест незнайомого набору містить чужий стек, тож сховище задаєте ви.

Кроки:
  1. завантаження даних у ваше призначене сховище (pipeline/storage.py);
  2. картка результатів — числа з вашого сховища;
  3. пошук інцидентів — ВАША функція find_incidents() з файла hunt.py;
  4. запис findings.json і перевірка через selfcheck.py.

Кроки 1, 2 і 4 уже готові. Крок 3 — ваша робота зі стадії 5: створіть поруч
файл hunt.py з функцією find_incidents (заготовку скрипт надрукує, якщо файла
ще немає).
"""

from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

from make_findings import fingerprint
from pipeline.storage import StorageError, normalise_kind, open_storage

HUNT_STUB = '''
Файла hunt.py ще немає. Створіть його поруч із run_all.py: скопіюйте все між
рисками і впишіть свій пошук зі стадії 5.
------------------------------------------------------------------------
def find_incidents(storage, manifest):
    """storage — відкрите й заповнене сховище; manifest — маніфест (словник)."""
    findings = []
    # Приклад одного запису:
    # findings.append({
    #     "devices": ["temp-03-lob"],
    #     "class": "stuck_at",
    #     "start": "2025-09-14T08:30:00Z",
    #     "end": "2025-09-14T16:00:00Z",
    #     "confidence": 0.8,
    #     "evidence": "температура не змінювалася 7,5 год",
    # })
    return findings
------------------------------------------------------------------------
'''

REQUIRED = ("devices", "class", "start", "end", "confidence")


def step(n: int, title: str) -> float:
    print(f"\n[{n}/4] {title}")
    return time.time()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="файл набору даних (.parquet)")
    ap.add_argument("--manifest", required=True, help="маніфест (.json)")
    ap.add_argument("--storage", required=True,
                    help="ВАШЕ призначене сховище (з вашого variant_NNN.json): "
                         "duckdb, sqlite, influxdb або timescaledb")
    ap.add_argument("--url", help="InfluxDB: адреса, якщо ви змінили порт (типово http://localhost:8086)")
    ap.add_argument("--dsn", help='TimescaleDB: рядок підʼєднання, якщо ви змінили порт, '
                                  'напр. "host=localhost port=5433 user=student password=kursova2025 dbname=iot"')
    ap.add_argument("--out", default="findings.json")
    ap.add_argument("--skip-load", action="store_true",
                    help="дані вже у сховищі — не завантажувати повторно")
    args = ap.parse_args()

    for p in (args.data, args.manifest):
        if not Path(p).exists():
            sys.exit(f"Файл {p} не знайдено. Ви в папці проєкту?")
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    kind = args.storage
    theirs = manifest.get("assigned_stack", {}).get("storage")
    try:
        if theirs and normalise_kind(theirs) != normalise_kind(kind):
            print(f"У маніфесті цього набору записано сховище «{theirs}». Це нормально:\n"
                  f"конвеєр працює з вашим сховищем «{kind}», як і має бути на захисті.")
    except StorageError:
        pass
    options = {}
    if args.url:
        options["url"] = args.url
    if args.dsn:
        options["dsn"] = args.dsn

    start = time.time()

    t = step(1, f"Завантаження у сховище «{kind}»")
    try:
        key = normalise_kind(kind)
        storage = open_storage(kind, **{k: v for k, v in options.items()
                                        if (k, key) in (("url", "influxdb"), ("dsn", "timescaledb"))})
        if not args.skip_load:
            rows = storage.load_parquet(args.data)
            print(f"  завантажено {rows:,} записів за {time.time() - t:.1f} с".replace(",", " "))
    except StorageError as exc:
        sys.exit(f"  Сховище недоступне: {exc}\n"
                 f"  Якщо це InfluxDB чи TimescaleDB — чи запущено Docker і контейнери (стадія 2)?")

    with storage:
        step(2, "Картка результатів")
        fp = fingerprint(Path(args.data), manifest)
        stored = storage.row_count()
        print(f"  Кількість записів          : {stored:,}".replace(",", " "))
        print(f"  Пристроїв у маніфесті      : {len(manifest['devices'])}")
        print(f"  Пристроїв у даних          : {len(storage.devices())}")
        print(f"  SHA-256 переліку пристроїв : {fp['device_list_sha256']}")
        if stored != fp["rows"]:
            print(f"  УВАГА: у файлі {fp['rows']:,} записів, а в сховищі {stored:,}.".replace(",", " "))
            print("  InfluxDB зливає рядки з однаковими пристроєм, показником і часом\n"
                  "  (див. попередження вище); для інших сховищ це втрата даних —\n"
                  "  перевірте завантаження. У картку вписуйте число з файла.")

        t = step(3, "Пошук інцидентів (hunt.py)")
        try:
            hunt = importlib.import_module("hunt")
        except ModuleNotFoundError as exc:
            if exc.name != "hunt":
                raise
            print(HUNT_STUB)
            findings = []
        else:
            findings = hunt.find_incidents(storage, manifest) or []
            print(f"  знайдено {len(findings)} за {time.time() - t:.1f} с")

    bad = [i for i, f in enumerate(findings) if not all(k in f for k in REQUIRED)]
    if bad:
        sys.exit(f"  У знахідках {bad} бракує полів; потрібні: {', '.join(REQUIRED)}")

    step(4, "findings.json і самоперевірка")
    findings.sort(key=lambda f: float(f["confidence"]), reverse=True)
    doc = {"variant": manifest["variant"], "fingerprint": fp, "findings": findings}
    Path(args.out).write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    sys.stdout.flush()                       # щоб вивід selfcheck не обігнав наш
    subprocess.run([sys.executable, "selfcheck.py", "--findings", args.out,
                    "--data", args.data, "--manifest", args.manifest])

    print(f"\nГотово за {time.time() - start:.0f} с. Знахідки за класами:")
    for cls, n in Counter(f["class"] for f in findings).most_common():
        print(f"  {cls:<20} {n}")
    if not findings:
        print("  (жодної — ваш пошук ще не записано у hunt.py)")


if __name__ == "__main__":
    main()
