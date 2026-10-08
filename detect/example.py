#!/usr/bin/env python3
"""
Стадія 3 — приклад детекторів на навчальному наборі.

Це НЕ розв'язок завдання. Тут реалізовано два найпростіші детектори з
дванадцяти потрібних класів, щоб показати схему роботи:

    завантажити -> побудувати ряди -> знайти кандидатів -> оцінити якість

Відповіді до навчального набору відомі (`tuning_truth.json`), тому тут
можна виміряти точність і повноту й налаштувати пороги. У власному
варіанті відповідей не буде.

Запуск:
    python example.py --data tuning.parquet --manifest tuning.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


# ======================================================================
# Допоміжні функції
# ======================================================================

def mad(x: np.ndarray) -> float:
    """Медіанне абсолютне відхилення, масштабоване до сигми.

    Чому не стандартне відхилення: один викид у тисячу значень зміщує
    сигму настільки, що решта викидів перестає його перевищувати. Медіана
    та MAD такого не помічають — саме тому в аномаліях користуються ними.
    """
    if len(x) == 0:
        return 1.0
    return float(1.4826 * np.median(np.abs(x - np.median(x)))) or 1.0


def runs_of(mask: np.ndarray) -> list[tuple[int, int]]:
    """Неперервні проміжки [початок, кінець), де mask дорівнює True."""
    if not mask.any():
        return []
    d = np.diff(mask.astype(np.int8))
    starts = list(np.where(d == 1)[0] + 1)
    ends = list(np.where(d == -1)[0] + 1)
    if mask[0]:
        starts.insert(0, 0)
    if mask[-1]:
        ends.append(len(mask))
    return list(zip(starts, ends))


def iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), timezone.utc).isoformat()


# ======================================================================
# Завантаження
# ======================================================================

class Dataset:
    """Індекс (пристрій, показник) -> відсортовані ряди ts і value."""

    def __init__(self, parquet: Path, manifest: Path):
        tbl = pq.read_table(parquet).unify_dictionaries()
        ts = tbl["ts"].to_numpy()
        val = tbl["value"].to_numpy()

        # Стовпці device_id і metric у файлі закодовані словником.
        # Працюємо з цілими кодами: мільйони рядків Python зайняли б
        # сотні мегабайтів і кілька хвилин лише на перетворення.
        dev_arr = tbl["device_id"].combine_chunks()
        met_arr = tbl["metric"].combine_chunks()
        dev_code = dev_arr.indices.to_numpy(zero_copy_only=False)
        met_code = met_arr.indices.to_numpy(zero_copy_only=False)
        dev_names = dev_arr.dictionary.to_pylist()
        met_names = met_arr.dictionary.to_pylist()

        self.manifest = json.loads(manifest.read_text())
        self.info = {d["device_id"]: d for d in self.manifest["devices"]}
        self.observed = sorted({dev_names[c] for c in np.unique(dev_code)})

        self.index: dict[tuple[str, str], tuple[np.ndarray, np.ndarray]] = {}
        key = dev_code.astype(np.int64) * (len(met_names) + 1) + met_code
        order = np.lexsort((ts, key))
        key_s, ts_s, val_s = key[order], ts[order], val[order]
        dev_s, met_s = dev_code[order], met_code[order]
        bounds = np.where(key_s[1:] != key_s[:-1])[0] + 1
        for a, b in zip(np.r_[0, bounds], np.r_[bounds, len(key_s)]):
            self.index[(dev_names[dev_s[a]], met_names[met_s[a]])] = (ts_s[a:b], val_s[a:b])

        self.rows = len(ts)

    def series(self, device: str, metric: str):
        return self.index.get((device, metric), (np.array([]), np.array([])))

    def metrics_of(self, device: str) -> list[str]:
        return [m for (d, m) in self.index if d == device]


# ======================================================================
# Детектор 1 — застрягле значення (stuck_at)
# ======================================================================

def detect_stuck_at(ds: Dataset, min_hours: float = 2.0) -> list[dict]:
    """Давач, що повторює те саме значення.

    Хитрість у тому, що повтори бувають і в справного давача: значення
    округлені, і сусідні відліки нерідко збігаються. Тому поріг береться
    не абсолютний, а відносно того, як часто цей конкретний ряд
    повторюється сам по собі.
    """
    out = []
    for (dev, metric), (ts, val) in ds.index.items():
        if len(val) < 60 or len(np.unique(val)) < 50:
            continue                      # дискретний сигнал: повтори нормальні

        same = np.r_[False, np.diff(val) == 0]
        rr = runs_of(same)
        if not rr:
            continue
        typical = np.percentile([b - a for a, b in rr], 99) if len(rr) > 20 else 3

        for a, b in rr:
            dur = ts[b - 1] - ts[max(a - 1, 0)]
            if (b - a) < max(40, 6 * typical) or dur < min_hours * 3600:
                continue
            out.append({
                "devices": [dev], "class": "stuck_at",
                "start": iso(ts[max(a - 1, 0)]), "end": iso(ts[b - 1]),
                "confidence": 0.9,
                "evidence": f"{metric} не змінюється {dur / 3600:.1f} год "
                            f"({b - a} відліків поспіль)",
            })
    return out


# ======================================================================
# Детектор 2 — неможливі показання (spike_storm)
# ======================================================================

# Фізичні межі. Значення за ними — не «рідкісне», а неможливе, і питання
# лише в тому, чому давач його надіслав. Цю таблицю ви складаєте самі,
# виходячи з того, що вимірює кожен показник.
PHYSICAL = {
    "temperature": (-40.0, 85.0),
    "humidity": (0.0, 100.0),
    "co2": (350.0, 40000.0),
    "illuminance": (0.0, 150000.0),
    "power_w": (0.0, 50000.0),
    "pressure_hpa": (850.0, 1100.0),
    "sound_db": (0.0, 194.0),
    "vib_rms": (0.0, 200.0),
    "flow_lpm": (0.0, 500.0),
    "rssi_dbm": (-120.0, 0.0),
}


def detect_spike_storm(ds: Dataset, min_count: int = 8) -> list[dict]:
    out = []
    for (dev, metric), (ts, val) in ds.index.items():
        lo, hi = PHYSICAL.get(metric, (-np.inf, np.inf))
        bad = (val < lo) | (val > hi)
        if bad.sum() < min_count:
            continue

        # Окремі викиди об'єднуємо в один інцидент, якщо вони поруч у часі
        idx = np.where(bad)[0]
        groups, cur = [], [idx[0]]
        for i in idx[1:]:
            if ts[i] - ts[cur[-1]] <= 3600:
                cur.append(i)
            else:
                groups.append(cur)
                cur = [i]
        groups.append(cur)

        for g in groups:
            if len(g) < min_count:
                continue
            out.append({
                "devices": [dev], "class": "spike_storm",
                "start": iso(ts[g[0]]), "end": iso(ts[g[-1]]),
                "confidence": 0.9,
                "evidence": f"{len(g)} показань {metric} поза фізично "
                            f"можливим діапазоном [{lo:g}, {hi:g}]",
            })
    return out


# ======================================================================
# Оцінювання — та сама логіка, що в керівника
# ======================================================================

MIN_IOU = 0.25
MIN_DEVICE_SHARE = 0.2   # щонайменше п'ята частина пристроїв знахідки — з інциденту
MAX_FINDINGS = 15        # стільки знахідок оцінюється у поданому findings.json


def to_epoch(x) -> int:
    if isinstance(x, (int, float)):
        return int(x)
    return int(datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp())


def evaluate(findings: list[dict], truth: dict, only: set[str] | None = None) -> None:
    incidents = truth["incidents"]
    if only:
        incidents = [i for i in incidents if i["class"] in only]

    pairs = []
    for fi, f in enumerate(findings):
        fs, fe = to_epoch(f["start"]), to_epoch(f["end"])
        devs = set(f["devices"])
        for ii, inc in enumerate(incidents):
            shared = devs & set(inc["devices"])
            if not shared or len(shared) / len(devs) < MIN_DEVICE_SHARE:
                continue
            lo, hi = max(fs, inc["start_s"]), min(fe, inc["end_s"])
            inter = max(0, hi - lo)
            union = (fe - fs) + (inc["end_s"] - inc["start_s"]) - inter
            if union > 0 and inter / union >= MIN_IOU:
                pairs.append((inter / union, fi, ii))

    pairs.sort(reverse=True)
    used_f, used_i, matched = set(), set(), []
    for ov, fi, ii in pairs:
        if fi in used_f or ii in used_i:
            continue
        used_f.add(fi)
        used_i.add(ii)
        matched.append((fi, ii, ov))

    tp = len(matched)
    fp = len(findings) - tp
    fn = len(incidents) - tp
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0

    print(f"  знайдено {tp}, хибних {fp}, пропущено {fn}")
    print(f"  точність {prec:.2f}   повнота {rec:.2f}   F1 {f1:.2f}")
    if len(findings) > MAX_FINDINGS:
        print(f"  увага: у поданому findings.json оцінюються лише {MAX_FINDINGS} "
              f"знахідок із найвищим confidence")
    for ii, inc in enumerate(incidents):
        if ii not in used_i:
            print(f"    ПРОПУЩЕНО  {inc['id']} {inc['class']} "
                  f"{inc['start'][:16]} {','.join(inc['devices'])[:30]}")


# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="tuning.parquet")
    ap.add_argument("--manifest", default="tuning.json")
    ap.add_argument("--truth", default="tuning_truth.json")
    ap.add_argument("--out", help="записати знахідки у findings.json")
    args = ap.parse_args()

    ds = Dataset(Path(args.data), Path(args.manifest))
    print(f"завантажено {ds.rows:,} записів, {len(ds.observed)} пристроїв\n"
          .replace(",", " "))

    findings = []
    for name, fn, cls in (("stuck_at", detect_stuck_at, "stuck_at"),
                          ("spike_storm", detect_spike_storm, "spike_storm")):
        got = fn(ds)
        findings += got
        print(f"детектор {name}: {len(got)} кандидатів")

    truth_path = Path(args.truth)
    if truth_path.exists():
        truth = json.loads(truth_path.read_text())
        print("\nЯкість на навчальному наборі (лише реалізовані класи):")
        evaluate(findings, truth, only={"stuck_at", "spike_storm"})
        print("\nУсі 12 класів:")
        evaluate(findings, truth)
    else:
        print(f"\n{truth_path} не знайдено — оцінку пропущено "
              f"(у власному варіанті так і буде)")

    if args.out:
        Path(args.out).write_text(json.dumps(
            {"variant": ds.manifest["variant"], "findings": findings},
            indent=2, ensure_ascii=False))
        print(f"\nзаписано {args.out}")

    print("\nДалі: решта десять класів із розділу 7 методичних вказівок,"
          "\nплюс детектор, призначений вам у маніфесті.")


if __name__ == "__main__":
    main()
