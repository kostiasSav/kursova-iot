#!/usr/bin/env python3
"""
Готує файли для здачі в Google Classroom (посібник, крок 7.10).

    python make_submission.py --name Іваненко
    python make_submission.py --name Іваненко --variant 47

У папці проєкту з'являються два файли з прізвищем і номером варіанта в назві:
  Іваненко_047_код.zip        ваш код і результати, але без наборів даних, баз,
                              записів трафіку, віртуального середовища, папки
                              docs/ і секретів брокера (broker/certs/, broker/passwd);
  Іваненко_047_findings.json  копія findings.json, якщо його зібрано саме для
                              цього варіанта.

Номер варіанта береться з назви variant_NNN.json у папці проєкту. Якщо таких
файлів кілька або жодного, вкажіть його ключем --variant.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import shutil
import sys
import textwrap
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Каталоги, які не потрапляють в архів, де б вони не лежали.
SKIP_DIRS = {".venv", "venv", ".git", "__pycache__", ".ipynb_checkpoints", ".pytest_cache",
             ".mypy_cache", "node_modules"}
# Шляхи від папки проєкту: матеріали курсу і секрети брокера (стадія 1).
SKIP_PATHS = {"docs", "broker/certs", "broker/passwd"}
# Файли за назвою: дані, бази, записи трафіку (у записі «до» видно пароль),
# ключі, архіви і копії знахідок, які робить цей скрипт.
SKIP_FILES = ("*.parquet", "variant_*.json", "tuning*.json", "data.txt*", "mote_locs.txt",
              "*.pcap", "*.pcapng", "*.duckdb", "*.duckdb.wal", "*.sqlite", "*.sqlite-*",
              "*.sqlite3", "*.db", "*.key", "*.zip", "*_findings.json", "*.pyc", ".DS_Store")
# Пропущене, яке не варто перелічувати у виводі.
QUIET = ("__pycache__", ".ipynb_checkpoints", ".pytest_cache", ".mypy_cache", "*.pyc", ".DS_Store")
MAX_MB = 20
SHOW = 12               # скільки пропущених назв показати


def plural(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def mb(size: int) -> str:
    return f"{size / 1024 / 1024:.1f}".replace(".", ",") + " МБ"


def clean_name(raw: str) -> str:
    name = "_".join(raw.split())
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", name).strip("._")
    if not name:
        sys.exit("Вкажіть прізвище: --name Іваненко")
    return name


def find_variant(given: int | None) -> tuple[int, str]:
    if given is not None:
        return given, "ключ --variant"
    found = sorted(p.name for p in HERE.glob("variant_*.json")
                   if re.fullmatch(r"variant_\d+\.json", p.name))
    if len(found) == 1:
        return int(re.search(r"\d+", found[0]).group()), f"з назви {found[0]}"
    if not found:
        sys.exit("У папці проєкту немає файлу variant_NNN.json (крок 0.6).\n"
                 "Вкажіть номер варіанта: python make_submission.py --name Іваненко --variant 47")
    sys.exit(f"У папці проєкту кілька маніфестів: {', '.join(found)}.\n"
             "Вкажіть свій варіант: python make_submission.py --name Іваненко --variant 47")


def collect(root: Path) -> tuple[list[Path], list[str]]:
    """Файли для архіву і перелік пропущеного (шляхи від папки проєкту)."""
    keep: list[Path] = []
    skipped: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        here = Path(dirpath)
        rel_dir = here.relative_to(root).as_posix()
        prefix = "" if rel_dir == "." else rel_dir + "/"
        for name in sorted(dirnames):
            rel = prefix + name
            if name in SKIP_DIRS or rel in SKIP_PATHS:
                dirnames.remove(name)
                if name not in QUIET:
                    skipped.append(rel + "/")
        dirnames.sort()
        for name in sorted(filenames):
            rel = prefix + name
            path = here / name
            if rel in SKIP_PATHS or any(fnmatch.fnmatch(name, p) for p in SKIP_FILES):
                if not any(fnmatch.fnmatch(name, p) for p in QUIET):
                    skipped.append(rel)
                continue
            try:
                size = path.stat().st_size
            except OSError:
                skipped.append(f"{rel} (не читається)")
                continue
            if size > MAX_MB * 1024 * 1024:
                skipped.append(f"{rel} ({mb(size)}, понад {MAX_MB} МБ)")
            else:
                keep.append(path)
    return keep, skipped


def write_zip(archive: Path, files: list[Path], root: Path, top: str, skipped: list[str]) -> int:
    """Пише архів і повертає, скільки файлів у ньому."""
    part = archive.with_name(archive.name + ".part")
    written = 0
    with zipfile.ZipFile(part, "w", zipfile.ZIP_DEFLATED, compresslevel=6,
                         strict_timestamps=False) as z:
        for path in files:
            rel = path.relative_to(root).as_posix()
            try:
                z.write(path, f"{top}/{rel}")
                written += 1
            except OSError:                 # файл відкритий іншою програмою тощо
                skipped.append(f"{rel} (не читається)")
    part.replace(archive)
    return written


def copy_findings(variant: int, dest: Path) -> tuple[bool, str]:
    src = HERE / "findings.json"
    if not src.is_file():
        return False, ("findings.json немає в папці проєкту: зберіть його (стадія 5) і "
                       "запустіть цей скрипт ще раз.")
    try:
        doc = json.loads(src.read_text(encoding="utf-8"))
        got = int(doc["variant"])
        count = len(doc["findings"])
    except (ValueError, KeyError, TypeError) as exc:
        return False, (f"findings.json не схожий на файл знахідок ({type(exc).__name__}): "
                       "зберіть його заново (стадія 5).")
    if got != variant:
        what = "навчального набору" if got == 0 else f"варіанта {got:03d}"
        return False, (f"findings.json зібрано для {what}, а не для варіанта {variant:03d}: мабуть, "
                       "його перезаписав run_all.py (крок 8.2). Зберіть його заново для свого "
                       "варіанта (стадія 5) і запустіть цей скрипт ще раз.")
    shutil.copyfile(src, dest)
    return True, (f"{dest.name} (копія findings.json: варіант {variant:03d}, {count} "
                  f"{plural(count, 'знахідка', 'знахідки', 'знахідок')}).")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True,
                    help="ваше прізвище, як у списку групи (якщо на курсі є однофамілець, "
                         "то й ім'я: --name \"Іваненко Олена\")")
    ap.add_argument("--variant", type=int, help="номер варіанта, якщо в папці не один variant_NNN.json")
    args = ap.parse_args()

    name = clean_name(args.name)
    variant, source = find_variant(args.variant)
    stem = f"{name}_{variant:03d}"
    print(f"Варіант {variant:03d} ({source}), прізвище в назвах файлів: {name}")
    print()

    files, skipped = collect(HERE)
    archive = HERE / f"{stem}_код.zip"
    count = write_zip(archive, files, HERE, f"kursova-iot-{variant:03d}", skipped)
    print(f"Архів коду: {archive.name} ({count} {plural(count, 'файл', 'файли', 'файлів')}, "
          f"{mb(archive.stat().st_size)}).")
    if skipped:
        skipped.sort(key=lambda s: not s.endswith(")"))     # завеликі й нечитані — першими
        shown = ", ".join(skipped[:SHOW]) + (f" і ще {len(skipped) - SHOW}" if len(skipped) > SHOW else "")
        print(textwrap.fill(shown, width=78, initial_indent="  Не ввійшли: ", subsequent_indent="    ",
                            break_on_hyphens=False, break_long_words=False))

    copy = HERE / f"{stem}_findings.json"
    ok, message = copy_findings(variant, copy)
    print(("Знахідки: " if ok else "\nУВАГА. ") + message)
    if not ok:
        if copy.is_file():
            print(f"Стару копію {copy.name} не змінено.")
        return 1

    print()
    print(f"Обидва файли — у папці проєкту ({HERE}).")
    print("Здайте в Google Classroom, у завдання курсового проєкту (посібник, крок 7.10):")
    print(f"  {archive.name}")
    print(f"  {copy.name}")
    print(f"  {stem}_записка.pdf (або .docx) — так назвіть свою записку")
    print("Перед здачею ще раз перевірте findings.json через selfcheck.py (стадія 5).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
