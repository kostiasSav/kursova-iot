#!/usr/bin/env python3
"""
Стадія 4 — «до/після»: як попередня обробка змінює кількість тривог.

Бере intel_lab.parquet (його створює intel_lab.py), запускає простий
детектор (ковзна робастна z-оцінка: медіана/MAD) на температурі кожного
вузла і рахує тривоги. Потім по одному додає кроки обробки й після
КОЖНОГО кроку рахує тривоги знову. Надрукована таблиця — це таблиця
«зміна кількості хибних спрацювань до і після доопрацювання» для звіту.

Детектор тут навмисно простий — такий, який зазвичай пишуть на
навчальному наборі, де дані рівномірні й чисті. Щоб отримати таку саму
таблицю для свого детектора, замініть функцію detect() (вхід і вихід ті
самі) і запустіть скрипт ще раз.

Що записує:
    intel_lab_clean.parquet   дані після кроків 1–4, той самий формат
    alarms.csv                епізоди тривог до обробки (before) і після (after)

Запуск (з тієї самої теки, де запускали intel_lab.py):
    python clean.py
    python clean.py --metric humidity
    python clean.py --min-voltage 2.4       # інший поріг розряду батареї
    python clean.py --detector baseline     # детектор стадії 3 (../detect/baseline.py)
    python clean.py --detector ewma         # призначений метод стадії 3 (../detect/methods/):
                                            # ewma, stl_esd, matrix_profile, isolation_forest, lof
    python clean.py --detector matrix_profile --method-args "--window 6h"
                                            # параметри методу — як у його власному --help
    python clean.py --plot mote-07          # рисунок для звіту (потрібен matplotlib)
"""

from __future__ import annotations

import argparse
import importlib
import json
import shlex
import sys
import time
import warnings
from collections import Counter
from pathlib import Path

from intel_lab import LOW_VOLTAGE, PHYSICAL, SCHEMA, UNITS, num   # файл поруч

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ----------------------------------------------------------------------
# Детектор
# ----------------------------------------------------------------------
WINDOW = 120          # точок у вікні: 120 × 31 с ≈ 1 год
Z_MAX = 5.0           # поріг робастної z-оцінки
# Менших змін не помічаємо: це шум і точність давача, а не аномалія.
# Без цієї «підлоги» вночі, коли температура стоїть, MAD≈0 і тривогу
# викликає будь-яка зміна на 0,01 °C.
MIN_SPREAD = {"temperature": 0.1, "humidity": 0.5, "light": 5.0, "voltage": 0.01}
EPISODE_GAP_S = 1800  # тривоги одного вузла ближче ніж 30 хв — один епізод

# ----------------------------------------------------------------------
# Обробка для детектора (кроки 5–7)
# ----------------------------------------------------------------------
GAP_S = 900           # перерва довша за 15 хв розрізає ряд на шматки
NOMINAL_S = 31        # як часто вузол мав надсилати показання
RESAMPLE_S = 300      # 5-хвилинні медіани
COMMON_BIN_S = 1800   # «одночасно» = у ту саму півгодину
COMMON_SHARE = 0.25   # … у щонайменше чверті вузлів, що тоді звітували
LAB_TZ = "America/Los_Angeles"


def detect(ts: np.ndarray, val: np.ndarray, window: int = WINDOW,
           z_max: float = Z_MAX, min_spread: float = 0.1) -> np.ndarray:
    """Ковзна робастна z-оцінка.

    Точка — тривога, якщо вона відхиляється від медіани ПОПЕРЕДНІХ
    `window` точок більше ніж на z_max «робастних сигм» (1,4826·MAD).
    Вхід: час і значення одного вузла, впорядковані за часом.
    Вихід: масив True/False тієї самої довжини.
    """
    s = pd.Series(val, dtype="float64")
    med = s.rolling(window, min_periods=window // 2).median().shift(1)
    dev = (s - med).abs()
    mad = dev.rolling(window, min_periods=window // 2).median().shift(1)
    z = dev / (1.4826 * mad.clip(lower=min_spread))
    return (z > z_max).to_numpy()


def kit_baseline():
    """Той самий інтерфейс, що й detect(), але всередині — детектор стадії 3
    з ../detect/baseline.py (вікно 24 год за часом, поріг |z| з --z)."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "detect"))
    try:
        from baseline import rolling_robust_z
    except Exception as exc:
        sys.exit(f"Не вдалося підключити detect/baseline.py ({exc}).\n"
                 f"Запустіть без --detector baseline — буде вбудований детектор.")

    def detect_baseline(ts, val, window=None, z_max=10.0, min_spread=None):
        x = pd.Series(val, index=pd.to_datetime(ts, unit="s"))
        return (rolling_robust_z(x, "24h").abs() > z_max).to_numpy()
    return detect_baseline


METHODS = ("ewma", "stl_esd", "matrix_profile", "isolation_forest", "lof")


def kit_method(name: str, metric: str, method_args: str = ""):
    """Той самий інтерфейс, що й detect(), але всередині — метод стадії 3 з
    ../detect/methods/<name>.py. Параметри — типові або з --method-args
    (той самий рядок, що й у командному рядку самого методу).

    Метод працює на рівномірній сітці (5 хв; STL — 15 хв), тож позначки сітки
    переносимо назад на показання, що потрапили в позначені клітинки. Шматок
    ряду, закороткий для методу (STL — менше двох діб, LOF — менше 301 точки
    сітки), лишається без тривог; скільки таких було — в кінці звіту.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "detect" / "methods"))
    try:
        common = importlib.import_module("common")
        method = importlib.import_module(name)
    except ImportError as exc:
        sys.exit(f"Не вдалося підключити detect/methods/{name}.py ({exc}).\n"
                 f"Запустіть без --detector {name} — буде вбудований детектор.")
    common.require(method.PACKAGES)              # бракує пакета — кажемо одразу, який
    parser = method.make_parser()
    parser.prog = f"{name}.py"                   # помилка в --method-args — від імені методу
    args = parser.parse_args(shlex.split(method_args))
    common.check_durations(parser, args)
    args.step = "5min" if args.step == "auto" else args.step   # 31 с -> 5 хв, як auto
    step_s = int(pd.Timedelta(args.step).total_seconds())
    skipped, notes = Counter(), Counter()

    def detect_method(ts, val, window=None, z_max=None, min_spread=None):
        x = pd.Series(val, index=pd.to_datetime(ts, unit="s", utc=True), name=metric)
        # Попередження бібліотек збираємо й покажемо один раз у кінці,
        # а не сотні разів — метод викликається для кожного шматка ряду.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                res = method.score_series(common.make_signal(x, args.signal, args.step), args,
                                          say=common.quiet)
            except common.TooShort:
                res = None
        notes.update(str(w.message).splitlines()[0] for w in caught)
        if res is None:
            skipped["chunks"] += 1
            skipped["points"] += len(ts)
            return np.zeros(len(ts), dtype=bool)
        cells = res.flagged.index[res.flagged.to_numpy()]
        cells_s = (cells - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(seconds=1)
        return np.isin(ts // step_s * step_s, np.asarray(cells_s))

    signal = "" if args.signal == "value" else f"; сигнал {args.signal}"
    detect_method.title = (f"detect/methods/{name}.py ({method.describe(args)}; "
                           f"сітка {args.step}{signal})")
    detect_method.skipped, detect_method.notes = skipped, notes
    return detect_method


# ======================================================================
# Кроки очищення даних (1–4). Кожен: таблиця -> таблиця.
# ======================================================================

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """1. Той самий вузол, показник і секунда — лишаємо перший запис.

    Тривог це майже не змінює, але без цього кроку зламається крок 3
    (об'єднання за вузлом і часом розмножить рядки), а InfluxDB мовчки
    перезапише одну точку іншою.
    """
    return df.drop_duplicates(["device_id", "metric", "ts"], keep="first")


def keep_known(df: pd.DataFrame, known: set[str]) -> pd.DataFrame:
    """2. Лише вузли з маніфесту (mote_locs.txt).

    mote-55…58 у переліку немає, а mote-6485, mote-33117, mote-65407 —
    пошкоджені пакети (по одному запису). Незареєстрований пристрій —
    окрема знахідка для звіту, а не тривога детектора температури.
    """
    return df[df["device_id"].isin(known)]


def drop_low_voltage(df: pd.DataFrame, min_v: float) -> pd.DataFrame:
    """3. Прибираємо ВСІ показники вузла в ті секунди, коли напруга < min_v.

    Коли батарея сідає, давач температури показує 30…122 °C, а вологість
    стає від'ємною. Це не аномалія середовища, а несправне живлення.
    Напруга — окремий рядок довгого формату, тому приєднуємо її за
    (вузол, час): у кожному рядку файла всі показники мають той самий ts.
    """
    volt = (df[df["metric"] == "voltage"][["device_id", "ts", "value"]]
            .rename(columns={"value": "volt"}))
    df = df.merge(volt, on=["device_id", "ts"], how="left")
    return df[~(df["volt"] < min_v)].drop(columns="volt")


def physical_range(df: pd.DataFrame) -> pd.DataFrame:
    """4. Значення поза фізичними межами (таблиця PHYSICAL в intel_lab.py).

    Температура 385 °C чи вологість −8983 % неможливі за жодних умов —
    це збій давача або пакета, а не подія, яку має шукати детектор.
    """
    keep = pd.Series(True, index=df.index)
    for metric, (lo, hi) in PHYSICAL.items():
        keep &= (df["metric"] != metric) | df["value"].between(lo, hi)
    return df[keep]


# ======================================================================
# Запуск детектора і обробка для нього (кроки 5–7)
# ======================================================================

def run_detector(df: pd.DataFrame, metric: str, window: int, z_max: float,
                 split_gaps: bool = False, resample: bool = False,
                 detector=detect) -> dict:
    """detector() для кожного вузла -> {вузол: (ts, значення, тривоги)}."""
    sub = df[df["metric"] == metric].sort_values(["device_id", "ts"], kind="stable")
    out = {}
    for device, g in sub.groupby("device_id", observed=True):
        ts, val = g["ts"].to_numpy(), g["value"].to_numpy(dtype="float64")
        w = window
        if resample:
            # 6. Медіана за 5 хв: поодинокі викиди зникають, ряд стає
            #    рівномірним. Вікно перераховуємо, щоб воно, як і раніше,
            #    охоплювало ≈ 1 год (120 × 31 с ≈ 12 × 5 хв).
            s = (pd.Series(val, index=pd.to_datetime(ts, unit="s"))
                 .resample(f"{RESAMPLE_S}s").median().dropna())
            ts = np.asarray((s.index - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1))
            val = s.to_numpy()
            w = max(4, round(window * NOMINAL_S / RESAMPLE_S))
        flags = np.zeros(len(val), dtype=bool)
        # 5. Після перерви > 15 хв вікно починається заново: не порівнюємо
        #    ранок з учорашнім вечором. Перші w/2 точок шматка детектор
        #    лише «розігрівається», тривог на них немає.
        cuts = np.where(np.diff(ts) > GAP_S)[0] + 1 if split_gaps else np.array([], int)
        for a, b in zip(np.r_[0, cuts], np.r_[cuts, len(val)]):
            flags[a:b] = detector(ts[a:b], val[a:b], window=w, z_max=z_max,
                                  min_spread=MIN_SPREAD.get(metric, 0.0))
        out[device] = (ts, val, flags)
    return out


def suppress_common(alarms: dict) -> dict:
    """7. Тривоги, які в ту саму півгодину піднімає щонайменше чверть
    вузлів, — це подія всієї будівлі (увімкнули опалення чи вентиляцію),
    а не несправність окремого вузла. Такі тривоги прибираємо.
    """
    reporting, alarmed = Counter(), Counter()
    for ts, _, flags in alarms.values():
        reporting.update(np.unique(ts // COMMON_BIN_S).tolist())
        alarmed.update(np.unique(ts[flags] // COMMON_BIN_S).tolist())
    common = [b for b, n in alarmed.items() if n >= COMMON_SHARE * reporting[b]]
    return {dev: (ts, val, flags & ~np.isin(ts // COMMON_BIN_S, common))
            for dev, (ts, val, flags) in alarms.items()}


# ======================================================================
# Підрахунок
# ======================================================================

def episodes(ts: np.ndarray, flags: np.ndarray) -> list[tuple[int, int, int]]:
    """Сусідні тривоги одного вузла -> епізоди (початок, кінець, точок)."""
    t = ts[flags]
    if len(t) == 0:
        return []
    cuts = np.where(np.diff(t) > EPISODE_GAP_S)[0] + 1
    return [(int(p[0]), int(p[-1]), len(p)) for p in np.split(t, cuts)]


def episodes_table(alarms: dict, stage: str) -> pd.DataFrame:
    rows = [(stage, dev, a, b, n) for dev, (ts, _, f) in alarms.items()
            for a, b, n in episodes(ts, f)]
    t = pd.DataFrame(rows, columns=["stage", "device_id", "start_ts", "end_ts", "points"])
    start = pd.to_datetime(t["start_ts"], unit="s", utc=True)
    t["start_utc"] = start.dt.strftime("%Y-%m-%d %H:%M")
    t["end_utc"] = pd.to_datetime(t["end_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    # Місцевий час лабораторії: саме в ньому видно розклад будівлі
    t["start_local"] = start.dt.tz_convert(LAB_TZ).dt.strftime("%Y-%m-%d %H:%M %a")
    return t


def most_common(values: pd.Series, n: int) -> pd.Series:
    """Найчастіші значення; за рівної кількості — у порядку зростання."""
    return values.value_counts().sort_index().sort_values(ascending=False, kind="stable").head(n)


def top_hours(table: pd.DataFrame, n: int = 4) -> str:
    hours = pd.to_datetime(table["start_ts"], unit="s", utc=True).dt.tz_convert(LAB_TZ).dt.hour
    return ", ".join(f"{h:02d}:00–{h:02d}:59 — {100 * c / len(hours):.0f} %"
                     for h, c in most_common(hours, n).items())


def plot_device(device: str, volt: pd.DataFrame, before: dict, after: dict,
                metric: str, min_v: float, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")                 # без вікна: одразу у файл
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    plt.rcParams.update({"axes.grid": True, "grid.color": "#e1e0d9", "grid.linewidth": 0.6,
                         "axes.edgecolor": "#c3c2b7", "axes.titlelocation": "left"})
    # Три графіки один під одним зі спільною віссю часу. Напругу не малюємо
    # на другій осі Y того самого графіка: дві шкали на одному полі
    # «вигадують» зв'язок, якого може не бути.
    fig, (top, mid, bottom) = plt.subplots(3, 1, figsize=(11, 8), sharex=True,
                                           gridspec_kw={"height_ratios": [3, 1.3, 3]})
    unit = UNITS[metric].replace("C", "°C")
    nothing = (np.array([], dtype=np.int64), np.array([]), np.array([], dtype=bool))
    for ax, (ts, val, flags), title in (
            (top, before.get(device, nothing), "до обробки (сирі дані)"),
            (bottom, after.get(device, nothing), "після кроків 1–7 (5-хвилинні медіани)")):
        t = pd.to_datetime(ts, unit="s")
        ax.plot(t, val, ".", ms=1, color="#898781", label=f"{metric}, {unit}")
        ax.plot(t[flags], val[flags], "o", ms=5, color="#d03b3b", mec="#fcfcfb",
                mew=0.8, label=f"тривога (епізодів: {len(episodes(ts, flags))})")
        ax.set_title(f"{device} — {title}")
        ax.set_ylabel(unit)
        ax.legend(loc="upper left", markerscale=2, framealpha=1)

    mid.plot(pd.to_datetime(volt["ts"], unit="s"), volt["value"], ".", ms=1, color="#2a78d6")
    mid.axhline(min_v, lw=0.8, color="#52514e")
    mid.annotate(f"поріг {min_v:g} В", (0.005, min_v), xycoords=("axes fraction", "data"),
                 xytext=(0, 3), textcoords="offset points", color="#52514e", fontsize=8)
    mid.set_title("напруга батареї (сирі дані)")
    mid.set_ylabel("В")

    bottom.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    bottom.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
    bottom.xaxis.set_minor_locator(mdates.DayLocator())
    bottom.set_xlabel("дата (UTC), позначки — понеділки")
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    print(f"Рисунок: {out}")


# ======================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default="intel_lab.parquet", help="файл від intel_lab.py")
    ap.add_argument("--manifest", default="intel_lab.json", help="маніфест від intel_lab.py")
    ap.add_argument("--metric", default="temperature",
                    help="показник: temperature, humidity, light, voltage")
    ap.add_argument("--min-voltage", type=float, default=LOW_VOLTAGE,
                    help=f"поріг розряду батареї, В (типово {LOW_VOLTAGE})")
    ap.add_argument("--detector", choices=["simple", "baseline", *METHODS], default="simple",
                    help="simple — вбудований detect() (типово); baseline — детектор "
                         "стадії 3 з ../detect/baseline.py; ewma, stl_esd, matrix_profile, "
                         "isolation_forest, lof — методи з ../detect/methods/ з їхніми "
                         "типовими параметрами")
    ap.add_argument("--method-args", default="", metavar='"ПАРАМЕТРИ"',
                    help="параметри методу для ewma, stl_esd, matrix_profile, isolation_forest, "
                         "lof — як у командному рядку самого методу, напр. "
                         "--method-args \"--lam 0.05 --L 7\" (діє і --step)")
    ap.add_argument("--window", type=int, default=WINDOW,
                    help=f"точок у вікні детектора simple (типово {WINDOW}, близько 1 год)")
    ap.add_argument("--z", type=float, default=None,
                    help=f"поріг z-оцінки (типово {Z_MAX:g} для simple, 10 для baseline)")
    ap.add_argument("--out", default="intel_lab_clean.parquet",
                    help="куди записати очищені дані")
    ap.add_argument("--plot", metavar="DEVICE", help="намалювати вузол, напр. mote-07")
    args = ap.parse_args()

    started = time.perf_counter()
    data, manifest = Path(args.data), Path(args.manifest)
    for p in (data, manifest):
        if not p.exists():
            sys.exit(f"Не знайдено {p}. Спершу запустіть у цій самій теці:\n"
                     f"    python intel_lab.py")
    known = {d["device_id"] for d in json.loads(manifest.read_text(encoding="utf-8"))["devices"]}
    if args.metric not in PHYSICAL:
        sys.exit(f"Невідомий показник {args.metric!r}. Можна: {', '.join(PHYSICAL)}")
    if args.plot:
        if args.plot not in known:
            sys.exit(f"Вузла {args.plot!r} немає в маніфесті. Приклад: --plot mote-07")
        try:
            import matplotlib  # noqa: F401  (перевіряємо заздалегідь, а не через 20 с)
        except ImportError:
            sys.exit("Для рисунка потрібен matplotlib:  pip install matplotlib")
    lo, hi = PHYSICAL[args.metric]
    if args.detector in METHODS:
        detector = kit_method(args.detector, args.metric, args.method_args)
        print(f"Детектор: {detector.title}; показник: {args.metric}")
        if args.z is not None or args.window != WINDOW:
            print("(--z і --window діють лише для simple і baseline; параметри методу "
                  "задавайте через --method-args)")
        print()
    else:
        if args.detector == "baseline":
            detector, args.z = kit_baseline(), args.z or 10.0
            print(f"Детектор: detect/baseline.py (вікно 24 год), поріг |z| > {args.z:g}; "
                  f"показник: {args.metric}")
        else:
            detector, args.z = detect, args.z or Z_MAX
            print(f"Детектор: ковзна робастна z-оцінка, вікно {args.window} точок, "
                  f"поріг {args.z:g}; показник: {args.metric}")
        if args.method_args:
            print("(--method-args діє лише для ewma, stl_esd, matrix_profile, "
                  "isolation_forest, lof; тут — --window і --z)")
        print()

    df = pq.read_table(data).to_pandas()
    # Для рисунка — напруга вибраного вузла з сирих даних
    volt = df[(df["device_id"] == args.plot) & (df["metric"] == "voltage")]
    # «Сміття» — показання, які приберуть кроки 2–4: вузол не з mote_locs.txt,
    # напруга нижче порогу в ту саму секунду, значення поза фізичними межами.
    low = df[(df["metric"] == "voltage") & (df["value"] < args.min_voltage)]
    low_ts = {str(d): np.unique(g["ts"].to_numpy()) for d, g in low.groupby("device_id", observed=True)}

    def on_junk(alarms: dict) -> int:
        """Скільки позначених точок припадає на сміття."""
        n = 0
        for dev, (ts, val, flags) in alarms.items():
            t, v = ts[flags], val[flags]
            junk = ~((v >= lo) & (v <= hi))
            if str(dev) not in known:
                junk[:] = True
            elif str(dev) in low_ts:
                junk |= np.isin(t, low_ts[str(dev)])
            n += int(junk.sum())
        return n

    rows: list[tuple] = []

    def record(name: str, table: pd.DataFrame, alarms: dict, readings: bool = True) -> None:
        values = table.loc[table["metric"] == args.metric, "value"]
        impossible = int((~values.between(lo, hi)).sum())
        eps = [episodes(ts, f) for ts, _, f in alarms.values()]
        # У рядках 6–7 точки — 5-хвилинні медіани вже очищених даних: сміття там немає.
        rows.append((name, sum(len(ts) for ts, _, _ in alarms.values()), impossible,
                     sum(int(f.sum()) for _, _, f in alarms.values()),
                     sum(len(e) for e in eps), sum(1 for e in eps if e),
                     on_junk(alarms) if readings else 0))
        print(f"  {name:<44} епізодів: {rows[-1][4]:>6}", flush=True)

    det = {"metric": args.metric, "window": args.window, "z_max": args.z,
           "detector": detector}
    before = run_detector(df, **det)
    record("0. сирі дані", df, before)
    df = drop_duplicates(df)
    record("1. без дублікатів (вузол+показник+секунда)", df, run_detector(df, **det))
    df = keep_known(df, known)
    record("2. лише вузли з mote_locs.txt", df, run_detector(df, **det))
    df = drop_low_voltage(df, args.min_voltage)
    record(f"3. без показань при напрузі < {args.min_voltage:g} В", df, run_detector(df, **det))
    df = physical_range(df)
    record(f"4. фізичні межі ({args.metric} {lo:g}…{hi:g})", df, run_detector(df, **det))
    record("5. вікно не перетинає перерв > 15 хв", df,
           run_detector(df, **det, split_gaps=True))
    resampled = run_detector(df, **det, split_gaps=True, resample=True)
    record("6. + 5-хвилинні медіани", df, resampled, readings=False)
    after = suppress_common(resampled)
    record("7. + без подій усієї будівлі", df, after, readings=False)

    # ------------------------------------------------------------------
    print(f"\n{'Крок':<44} {'Показань':>10} {'Неможл.':>8} {'Точок':>9} "
          f"{'На смітті':>10} {'Епізодів':>9} {'Вузлів':>7}")
    for name, n, bad, pts, eps, motes, junk in rows:
        share = f"{100 * junk / pts:.0f} %" if pts else "–"
        print(f"{name:<44} {num(n):>10} {num(bad):>8} {num(pts):>9} "
              f"{share:>10} {num(eps):>9} {motes:>7}")
    first, last = rows[0][4], rows[-1][4]
    print(f"\nЕпізодів тривог: {num(first)} -> {num(last)} "
          f"({100 * (last - first) / first:+.0f} %)" if first else "")
    if args.detector in ("simple", "baseline"):
        print("Неможл. — значення поза фізичними межами, що лишилися в даних. Детектор\n"
              "їх майже не помічає: ковзна медіана швидко звикає до «застиглих»\n"
              "неможливих значень (122 °C, -4 %) і далі вважає їх нормою.")
    elif args.detector == "ewma":
        print("Неможл. — значення поза фізичними межами, що лишилися в даних. EWMA\n"
              "порівнює кожну точку з «нормою» першого тижня ряду, тож неможливі\n"
              "значення позначає майже всі — звідси великі «Точок» і «На смітті».")
    else:
        print("Неможл. — значення поза фізичними межами, що лишилися в даних.\n"
              "Цей метод щоразу позначає приблизно сталу частку кожного ряду, тож після\n"
              "очищення тривоги не зникають, а переходять на інші точки: дивіться, ЩО\n"
              "позначено («На смітті»), а не лише скільки. Після кроку 5 кожен шматок\n"
              "ряду дістає власну частку — тому епізодів там може стати більше.")
    print("Показань у рядках 6–7 — це 5-хвилинні медіани, тож «Точок» там\n"
          "не порівнюйте з рядками 0–5; порівнюйте «Епізодів».\n"
          "На смітті — частка «Точок» на показаннях, які прибирають кроки 2–4\n"
          "(вузол не з mote_locs.txt, розряд батареї, поза межами); з кроку 4 — 0.")

    table_before, table_after = episodes_table(before, "before"), episodes_table(after, "after")
    if len(table_before) and len(table_after):
        print(f"\nКоли починаються епізоди (місцевий час лабораторії):")
        print(f"  до обробки   : {top_hours(table_before)}")
        print(f"  після обробки: {top_hours(table_after)}")
        worst = most_common(table_after["device_id"].astype(str), 5)
        print("  найбільше після обробки: "
              + ", ".join(f"{d} ({c})" for d, c in worst.items()))

    # ------------------------------------------------------------------
    clean = df[["ts", "device_id", "metric", "value"]].copy()
    clean["device_id"] = clean["device_id"].cat.remove_unused_categories()
    table = pa.Table.from_pandas(clean, preserve_index=False)
    pq.write_table(table.cast(SCHEMA).replace_schema_metadata(None), args.out,
                   compression="zstd", row_group_size=512_000)
    pd.concat([table_before, table_after]).to_csv("alarms.csv", index=False)
    print(f"\nЗаписано {args.out} (рядків: {num(len(clean))}) і alarms.csv "
          f"(епізодів до: {num(len(table_before))}, після: {num(len(table_after))})")

    if args.plot:
        plot_device(args.plot, volt, before, after, args.metric, args.min_voltage,
                    Path(f"{args.plot}_{args.metric}.png"))
    if args.detector in METHODS and detector.skipped:
        print(f"Закороткі для методу шматки рядів (найчастіше після кроку 5) лишилися без "
              f"тривог: {num(detector.skipped['chunks'])} шматків, "
              f"{num(detector.skipped['points'])} показань за всі кроки разом.")
    if args.detector in METHODS:
        for text, times in detector.notes.most_common(3):
            print(f"Попередження бібліотеки ({num(times)} разів): {text}")
    print(f"Готово за {time.perf_counter() - started:.0f} с.")


if __name__ == "__main__":
    main()
