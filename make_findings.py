#!/usr/bin/env python3
"""
Збирає findings.json із простої таблиці знахідок.

Ви ведете знахідки в CSV-файлі (його зручно редагувати в Excel, LibreOffice
чи VS Code), а цей скрипт:
  - перераховує картку результатів (fingerprint) з ваших даних;
  - перетворює таблицю на findings.json у форматі п. 6.2 методичних вказівок;
  - упорядковує знахідки за спаданням упевненості (оцінюються лише 15 перших).

Колонки CSV (перший рядок — саме ці назви):
    devices     пристрої через крапку з комою: temp-03-lob;temp-05-lob
    class       клас інциденту з розділу 7, наприклад stuck_at
    start       початок, наприклад 2025-09-14T08:30:00Z
    end         кінець у тому самому форматі
    confidence  упевненість від 0 до 1, наприклад 0.8
    evidence    коротко: на чому ґрунтується висновок

Приклад:
    python make_findings.py --csv my_findings.csv \\
        --data variant_047.parquet --manifest variant_047.json

Після цього обов'язково запустіть selfcheck.py — команду скрипт надрукує.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

COLUMNS = ["devices", "class", "start", "end", "confidence", "evidence"]


def fingerprint(data: Path, manifest: dict) -> dict:
    """Ті самі три числа, що друкує selfcheck.py."""
    ids = sorted(d["device_id"] for d in manifest["devices"])
    tbl = pq.read_table(data, columns=["device_id"])
    return {
        "rows": tbl.num_rows,
        "devices_observed": len(pc.unique(tbl["device_id"].combine_chunks().cast("string"))),
        "device_list_sha256": hashlib.sha256("\n".join(ids).encode()).hexdigest(),
    }


def read_text_any(path: Path) -> str:
    """Текст CSV у UTF-8 (VS Code, «CSV UTF-8» з Excel) або у cp1251 —
    так зберігає звичайний «CSV» Excel з українською мовою Windows."""
    raw = path.read_bytes()
    try:
        return raw.decode("utf-8-sig")          # -sig: Excel додає невидимий BOM
    except UnicodeDecodeError:
        print(f"Увага: {path} не в UTF-8 — читаю як cp1251 (так зберігає Excel). "
              f"Надалі зберігайте як «CSV UTF-8».")
        return raw.decode("cp1251")


def read_rows(path: Path) -> list[dict]:
    text = read_text_any(path)
    first = text.splitlines()[0] if text.strip() else ""
    # Excel з українською локаллю зберігає CSV через крапку з комою
    delim = ";" if first.count(";") > first.count(",") else ","
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=delim)
    missing = [c for c in COLUMNS if c not in (reader.fieldnames or [])]
    if missing:
        sys.exit(f"У {path} немає колонок: {', '.join(missing)}.\n"
                 f"Перший рядок має бути: {','.join(COLUMNS)}")
    return list(reader)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, help="ваша таблиця знахідок")
    ap.add_argument("--data", required=True, help="ваш variant_NNN.parquet")
    ap.add_argument("--manifest", required=True, help="ваш variant_NNN.json")
    ap.add_argument("--out", default="findings.json")
    args = ap.parse_args()

    for p in (args.csv, args.data, args.manifest):
        if not Path(p).exists():
            sys.exit(f"Файл {p} не знайдено. Перевірте, що ви в папці проєкту.")

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    findings = []
    for n, row in enumerate(read_rows(Path(args.csv)), start=2):
        devices = [d.strip() for d in row["devices"].replace(",", ";").split(";") if d.strip()]
        try:
            confidence = float(row["confidence"].replace(",", "."))
        except ValueError:
            sys.exit(f"Рядок {n}: упевненість {row['confidence']!r} — не число")
        findings.append({
            "devices": devices,
            "class": row["class"].strip(),
            "start": row["start"].strip(),
            "end": row["end"].strip(),
            "confidence": confidence,
            "evidence": row["evidence"].strip(),
        })

    findings.sort(key=lambda f: f["confidence"], reverse=True)
    doc = {
        "variant": manifest["variant"],
        "fingerprint": fingerprint(Path(args.data), manifest),
        "findings": findings,
    }
    Path(args.out).write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Записано {args.out} (знахідок: {len(findings)}, варіант {doc['variant']})")
    if len(findings) > 15:
        print(f"Увага: оцінюються лише 15 знахідок з найвищою упевненістю, "
              f"решта {len(findings) - 15} не враховується.")
    print("\nТепер перевірте файл:")
    print(f"  python selfcheck.py --findings {args.out} --data {args.data} --manifest {args.manifest}")


if __name__ == "__main__":
    main()
