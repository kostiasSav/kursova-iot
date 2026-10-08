#!/usr/bin/env python3
"""
Перевірка findings.json перед здачею.

Що робить:
  - перевіряє структуру файла і допустимість назв класів;
  - перераховує картку результатів із ваших власних даних і порівнює
    з тим, що ви задекларували;
  - попереджає про очевидні проблеми (порожній перелік, понад 15 знахідок,
    інтервали поза межами набору, дублікати).

Чого НЕ робить: не повідомляє, чи правильні ваші знахідки. Еталонні
відповіді є лише у керівника. Нульова кількість помилок тут означає
тільки те, що файл придатний до перевірки.

Приклад:
    python selfcheck.py --findings findings.json \\
        --data variant_047.parquet --manifest variant_047.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

# Повний перелік класів — розділ 7 методичних вказівок
CLASSES = {
    "stuck_at", "outage", "spike_storm", "rogue_device",
    "drift", "clock_skew", "beaconing", "energy_anomaly",
    "zone_incoherence", "gateway_group_loss", "exfil_pattern", "sensor_swap",
}

errors: list[str] = []
warnings: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


def warn(msg: str) -> None:
    warnings.append(msg)


def parse_time(x, where: str) -> int | None:
    if isinstance(x, (int, float)):
        return int(x)
    try:
        s = str(x).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        err(f"{where}: не вдалося розібрати час {x!r} "
            f"(очікується ISO-8601 або секунди епохи)")
        return None


def check_structure(doc: dict) -> list[tuple[int, int, int]]:
    if not isinstance(doc.get("variant"), int):
        err("поле 'variant' відсутнє або не є цілим числом")

    fp = doc.get("fingerprint")
    if not isinstance(fp, dict):
        err("блок 'fingerprint' відсутній")
    else:
        for key in ("rows", "devices_observed", "device_list_sha256"):
            if key not in fp:
                err(f"fingerprint: немає поля '{key}'")

    findings = doc.get("findings")
    if not isinstance(findings, list):
        err("поле 'findings' відсутнє або не є списком")
        return []

    if not findings:
        warn("перелік знахідок порожній")
    elif len(findings) > 15:
        warn(f"{len(findings)} знахідок — оцінюються лише 15 із найвищим "
             f"'confidence', решта не враховується (у варіанті приховано "
             f"5–8 інцидентів)")

    windows: list[tuple[int, int, int]] = []
    devsets: dict[int, set[str]] = {}
    for i, f in enumerate(findings):
        where = f"findings[{i}]"
        if not isinstance(f, dict):
            err(f"{where}: має бути об'єктом")
            continue

        devs = f.get("devices") or f.get("device") or f.get("device_id")
        if not devs:
            err(f"{where}: не вказано жодного пристрою")
        elif isinstance(devs, str):
            devs = [devs]
        elif not isinstance(devs, list):
            err(f"{where}: 'devices' має бути рядком або списком")
            devs = []
        devsets[i] = {str(d) for d in devs} if isinstance(devs, list) else set()

        cls = f.get("class")
        if not cls:
            err(f"{where}: не вказано клас")
        elif cls not in CLASSES:
            err(f"{where}: невідомий клас {cls!r}. Допустимі: "
                f"{', '.join(sorted(CLASSES))}")

        if "start" not in f or "end" not in f:
            err(f"{where}: потрібні поля 'start' і 'end'")
            continue
        s = parse_time(f["start"], where + ".start")
        e = parse_time(f["end"], where + ".end")
        if s is None or e is None:
            continue
        if e <= s:
            err(f"{where}: 'end' не пізніше за 'start'")
            continue
        if e - s < 60:
            warn(f"{where}: інтервал коротший за хвилину — ймовірно помилка")

        conf = f.get("confidence")
        if conf is not None and not (0.0 <= float(conf) <= 1.0):
            err(f"{where}: 'confidence' має бути в межах 0..1")

        windows.append((i, s, e))

    # Дві знахідки зі спільним пристроєм і суттєвим перекриттям у часі майже
    # напевно описують той самий інцидент: зарахується щонайбільше одна з них,
    # решта стане хибними спрацюваннями (п. 6.2 методичних вказівок).
    for a, (i, s1, e1) in enumerate(windows):
        for j, s2, e2 in windows[a + 1:]:
            inter = max(0, min(e1, e2) - max(s1, s2))
            union = (e1 - s1) + (e2 - s2) - inter
            if (devsets.get(i, set()) & devsets.get(j, set())
                    and union > 0 and inter / union >= 0.25):
                warn(f"findings[{i}] і findings[{j}]: спільні пристрої та "
                     f"перекриття інтервалів — імовірно, той самий інцидент; "
                     f"зарахується щонайбільше одна з них")

    return windows


def check_against_data(doc: dict, data: Path, manifest: Path,
                       windows: list[tuple[int, int, int]]) -> None:
    man = json.loads(manifest.read_text(encoding="utf-8"))
    tbl = pq.read_table(data, columns=["ts", "device_id"])

    ids = sorted(d["device_id"] for d in man["devices"])
    real = {
        "rows": tbl.num_rows,
        "devices_observed": len(set(tbl["device_id"].to_pylist())),
        "device_list_sha256": hashlib.sha256("\n".join(ids).encode()).hexdigest(),
    }

    if doc.get("variant") != man.get("variant"):
        err(f"номер варіанта у findings ({doc.get('variant')}) не збігається "
            f"з маніфестом ({man.get('variant')})")

    declared = doc.get("fingerprint") or {}
    for key, value in real.items():
        if key not in declared:
            continue
        if str(declared[key]) != str(value):
            err(f"fingerprint.{key}: задекларовано {declared[key]!r}, "
                f"у ваших даних {value!r}")

    ts = tbl["ts"].to_numpy()
    lo, hi = int(ts.min()), int(ts.max())
    for i, s, e in windows:
        if e < lo or s > hi:
            warn(f"findings[{i}]: інтервал цілком поза межами набору даних "
                 f"({datetime.fromtimestamp(lo, timezone.utc).date()} – "
                 f"{datetime.fromtimestamp(hi, timezone.utc).date()})")

    print("Картка результатів, перерахована з ваших даних:")
    print(f"  Кількість записів          : {real['rows']:,}".replace(",", " "))
    print(f"  Пристроїв у маніфесті      : {len(ids)}")
    print(f"  Пристроїв у даних          : {real['devices_observed']}")
    print(f"  SHA-256 переліку пристроїв : {real['device_list_sha256']}")
    if real["devices_observed"] != len(ids):
        print(f"  (розбіжність на {abs(real['devices_observed'] - len(ids))} "
              f"— це не помилка генерації)")
    stack = man.get("assigned_stack", {})
    if stack:
        print("  Призначений стек           : " +
              ", ".join(f"{k}={v}" for k, v in stack.items()))
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--findings", required=True)
    ap.add_argument("--data", help="ваш variant_NNN.parquet")
    ap.add_argument("--manifest", help="ваш variant_NNN.json")
    args = ap.parse_args()

    try:
        doc = json.loads(Path(args.findings).read_text(encoding="utf-8"))
    except Exception as exc:
        sys.exit(f"не вдалося прочитати {args.findings}: {exc}")

    windows = check_structure(doc)

    if args.data and args.manifest:
        check_against_data(doc, Path(args.data), Path(args.manifest), windows)
    else:
        warn("не вказано --data і --manifest: картку результатів не перевірено")

    n = len(doc.get("findings") or [])
    print(f"Знахідок у файлі: {n}")
    for w in warnings:
        print(f"  ПОПЕРЕДЖЕННЯ: {w}")
    for e in errors:
        print(f"  ПОМИЛКА: {e}")

    if errors:
        print(f"\nПомилок: {len(errors)} — файл НЕ придатний до здачі.")
        sys.exit(1)
    print("\nФайл придатний до перевірки."
          "\nЦе не означає, що знахідки правильні — лише що формат коректний.")


if __name__ == "__main__":
    main()
