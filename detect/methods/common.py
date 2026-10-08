#!/usr/bin/env python3
"""
Спільна «обв'язка» для детекторів стадії 3 (методичні вказівки, п. 4.3).

Тут немає жодного методу виявлення аномалій — лише те, що однакове для
скриптів у цій теці і для ../baseline.py:

  1. завантажити ОДИН ряд (пристрій + показник) з Parquet-файлу курсу;
  2. перетворити його на сигнал: самі значення (value), швидкість зростання
     лічильника (rate) або інтервали між повідомленнями (interval);
  3. звести сигнал на рівномірну сітку часу (наприклад, по 5 хвилин);
  4. об'єднати позначені точки у вікна — кандидати в інциденти;
  5. надрукувати вікна, намалювати графік, зберегти кандидатів у JSON;
  6. порівняти вікна з відповідями навчального набору (tuning_truth.json)
     за тими самими правилами, що й автоматична перевірка (п. 6.2).

Самостійно цей файл не запускається.
"""

from __future__ import annotations

import argparse
import difflib
import importlib
import json
import re
import sys
from pathlib import Path
from typing import NamedTuple

try:
    import numpy as np
    import pandas as pd
    import pyarrow.parquet as pq
except ImportError as exc:
    sys.exit(f"Не встановлено пакет «{exc.name}». Встановіть залежності у ваш venv:\n"
             f"    pip install -r requirements.txt")

# Windows: якщо вивід перенаправлено у файл, консольне кодування може не мати
# якогось символу. Нехай такий символ стане «?», а не зупинить програму.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(errors="replace")

# Той самий перелік, що в selfcheck.py (розділ 7 методичних вказівок)
CLASSES = ("stuck_at", "outage", "spike_storm", "rogue_device", "drift", "clock_skew",
           "beaconing", "energy_anomaly", "zone_incoherence", "gateway_group_loss",
           "exfil_pattern", "sensor_swap")
MIN_IOU = 0.25          # правило зарахування з п. 6.2: перекриття інтервалів (Жаккар)

SIGNALS = {"value": "значення",
           "rate": "приріст за хвилину",
           "interval": "інтервал між повідомленнями, с"}


def need(module: str, pip_name: str):
    """Імпортувати бібліотеку або пояснити, як її встановити."""
    try:
        return importlib.import_module(module)
    except ImportError:
        sys.exit(f"Не встановлено пакет «{pip_name}». Встановіть його у ваш venv:\n"
                 f"    pip install {pip_name}")


def require(packages: tuple) -> None:
    """Наперед перевірити пакети методу: пари (модуль, назва для pip)."""
    for module, pip_name in packages:
        need(module, pip_name)


# ======================================================================
# Спільна «форма» методів: score_series(x, args, say) -> Scored
# ======================================================================

class Scored(NamedTuple):
    """Що повертає ядро кожного методу (функція score_series)."""
    score: pd.Series                # оцінка «незвичності» кожної точки сітки
    flagged: pd.Series              # True — точку позначено
    limits: tuple = ()              # пороги для нижньої панелі графіка
    name: str = "оцінка"            # підпис оцінки на графіку


class TooShort(SystemExit):
    """Замало даних для методу. У командному рядку це звичайне завершення
    з поясненням; realdata/clean.py натомість пропускає такий шматок ряду."""


def quiet(*_args, **_kwargs) -> None:
    """Замість print, коли метод викликають сотні разів (realdata/clean.py)."""


def grid_step(x: pd.Series | pd.DataFrame) -> pd.Timedelta:
    """Крок рівномірної сітки, на якій лежить сигнал."""
    if len(x) < 2:
        raise TooShort("Замало точок на сітці.")
    return x.index[1] - x.index[0]


# ======================================================================
# Командний рядок: спільні параметри всіх детекторів
# ======================================================================

def parser(doc: str, method: str, grid: bool = True, step: str = "auto") -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=doc, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.set_defaults(method=method, label=None)
    g = ap.add_argument_group("дані")
    g.add_argument("--data", default="tuning.parquet", help="Parquet-файл (типово tuning.parquet у поточній теці)")
    g.add_argument("--device", help="пристрій, напр. temp-06-lob (без нього скрипт покаже перелік)")
    g.add_argument("--metric", help="показник цього пристрою, напр. temperature")
    g.add_argument("--signal", choices=list(SIGNALS), default="value",
                   help="що аналізувати: value — значення; rate — приріст лічильника за хвилину "
                        "(pkt_sent, pkt_lost); interval — інтервали між повідомленнями")
    if grid:
        g.add_argument("--step", default=step,
                       help=f"крок сітки часу: 5min, 15min, 1h або auto — за частотою "
                            f"повідомлень (типово {step})")
    w = ap.add_argument_group("вікна і результати")
    w.add_argument("--gap", default="1h", help="позначки, ближчі за --gap, — одне вікно (типово 1h)")
    w.add_argument("--min-run", type=int, default=3, help="мінімум позначених точок у вікні (типово 3)")
    w.add_argument("--png", help="файл графіка (типово <метод>_<пристрій>_<показник>.png)")
    w.add_argument("--json", help="зберегти вікна як кандидатів у findings (разом з --class)")
    w.add_argument("--class", dest="cls", choices=CLASSES, metavar="КЛАС",
                   help="клас для --json, один з: " + ", ".join(CLASSES))
    w.add_argument("--evaluate", metavar="TRUTH", help="порівняти з відповідями, напр. tuning_truth.json")
    return ap


def share(low: float, high: float):
    """Тип для argparse: число в межах (low, high], інакше звичайна помилка параметра."""
    def number(text: str) -> float:
        value = float(text)
        if not low < value <= high:
            raise argparse.ArgumentTypeError(f"має бути в межах ({low:g}, {high:g}]")
        return value
    return number


def check_durations(ap: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Тривалості (5min, 2h, 1D) — з одиницею і не коротші за хвилину; записуємо
    їх в однаковому вигляді. Помилка — звичайна помилка argparse."""
    for name in ("gap", "step", "window", "period"):
        value = getattr(args, name, None)
        if value in (None, "auto"):
            continue
        try:
            seconds = int(pd.Timedelta(value).total_seconds())
        except ValueError:
            seconds = 0
        if seconds < 60:
            ap.error(f"--{name} {value!r}: потрібна тривалість з одиницею, щонайменше 1 хвилина "
                     f"(приклади: 5min, 30min, 2h, 1D)")
        unit = next(u for u in ((86400, "D"), (3600, "h"), (60, "min"), (1, "s"))
                    if seconds % u[0] == 0)
        setattr(args, name, f"{seconds // unit[0]}{unit[1]}")


def parse(ap: argparse.ArgumentParser) -> argparse.Namespace:
    """Розібрати параметри і одразу перевірити те, у чому найлегше помилитися."""
    args = ap.parse_args()
    if args.json and not args.cls:
        ap.error("для --json вкажіть ще й --class — який клас інциденту ви бачите у цих вікнах")
    check_durations(ap, args)
    args.truth = load_truth(args.evaluate, args.data) if args.evaluate else None
    need("matplotlib", "matplotlib")          # графік потрібен завжди — перевіряємо одразу
    return args


# ======================================================================
# Завантаження одного ряду
# ======================================================================

def _parquet(path: str) -> Path:
    p = Path(path).expanduser()
    if not p.is_file():
        sys.exit(f"Не знайдено файл з даними: {p.resolve()}\n"
                 f"Навчальний набір tuning.parquet (і свій variant_NNN.parquet) завантажте за "
                 f"посиланням від керівника і покладіть у поточну теку або вкажіть шлях через --data.")
    return p


def _pairs(path: Path) -> pd.DataFrame:
    """Усі пари (пристрій, показник), що є у файлі, — для підказок."""
    df = pq.read_table(path, columns=["device_id", "metric"]).to_pandas()
    return df.astype(str).drop_duplicates().sort_values(["device_id", "metric"])


def _stop_with_list(path: Path, device: str | None, what: str) -> None:
    pairs = _pairs(path)
    devices = sorted(pairs["device_id"].unique())
    if device and device not in devices:
        what = f"Пристрою {device!r} у файлі {path.name} немає."
    print(what)
    if device in devices:
        print(f"Показники пристрою {device}: "
              f"{', '.join(pairs.loc[pairs['device_id'] == device, 'metric'])}")
    else:
        print(f"Пристрої та їхні показники у {path.name}:")
        for dev, grp in pairs.groupby("device_id"):
            print(f"  {dev:<18} {', '.join(grp['metric'])}")
        near = difflib.get_close_matches(device or "", devices, n=3, cutoff=0.5)
        if near:
            print(f"Можливо, ви мали на увазі: {', '.join(near)}")
    sys.exit(2)


def load_raw(path: str, device: str | None, metric: str | None, signal: str = "value") -> pd.Series:
    """Один ряд як pandas.Series: індекс — час (UTC), значення — value."""
    p = _parquet(path)
    if not device:
        _stop_with_list(p, None, "Вкажіть пристрій: --device <назва>.")
    if not metric:
        mets = _pairs(p).query("device_id == @device")["metric"].tolist()
        if len(mets) != 1:
            _stop_with_list(p, device, "Вкажіть показник: --metric <назва>.")
        metric = mets[0]
    # Фільтр виконує pyarrow: у пам'ять потрапляють лише рядки цього ряду
    tbl = pq.read_table(p, columns=["ts", "value"],
                        filters=[("device_id", "==", device), ("metric", "==", metric)])
    if tbl.num_rows == 0:
        _stop_with_list(p, device, f"У файлі немає ряду {device} / {metric}.")
    s = pd.Series(tbl["value"].to_numpy().astype("float64"),
                  index=pd.to_datetime(tbl["ts"].to_numpy(), unit="s", utc=True),
                  name=metric).sort_index()
    if signal == "value" and s.nunique() < 10:
        print(f"Увага: різних значень у ряді лише {s.nunique()} (дискретний сигнал) — "
              f"відхилення від «норми» тут малоінформативні. Спробуйте --signal interval.")
    elif signal == "value" and (s.diff() < 0).mean() < 0.01 and (s.diff() > 0).mean() > 0.5:
        print(f"Підказка: {metric} лише зростає — це накопичувальний лічильник. "
              f"Аналізуйте його швидкість: --signal rate")
    return s


def load(args: argparse.Namespace) -> pd.Series:
    """load_raw з параметрів командного рядка (і запам'ятати назву показника)."""
    raw = load_raw(args.data, args.device, args.metric, args.signal)
    args.metric = raw.name
    return raw


def make_signal(raw: pd.Series, signal: str, step: str | None = None) -> pd.Series:
    """Перетворити сирий ряд на сигнал; якщо задано step — звести на сітку."""
    name = raw.name if signal == "value" else f"{raw.name}:{signal}"
    if signal == "interval":
        # скільки секунд минуло від попереднього повідомлення цього ряду
        x = raw.index.to_series().diff().dt.total_seconds().iloc[1:]
        x = x if step is None else x.resample(step).mean()
    elif signal == "rate" and step is None:
        sec = raw.index.to_series().diff().dt.total_seconds()
        x = (raw.diff() / sec * 60).replace([np.inf, -np.inf], np.nan).dropna()
    elif signal == "rate":
        # Між повідомленнями лічильник зростає приблизно рівномірно, тому його
        # значення на межах сітки оцінюємо лінійною інтерполяцією, а потім
        # беремо приріст за кожен крок [t, t + step).
        raw = raw[~raw.index.duplicated()]
        grid = pd.date_range(raw.index[0].ceil(step), raw.index[-1].floor(step), freq=step)
        c = raw.reindex(raw.index.union(grid)).interpolate("time").reindex(grid)
        x = c.diff().shift(-1) / (pd.Timedelta(step).total_seconds() / 60)
    else:
        x = raw if step is None else raw.resample(step).mean()
    return x.rename(name)


def pick_step(step: str, *raws: pd.Series) -> str:
    """auto: крок сітки — щонайменше 5 хв і вдвічі довший за типовий інтервал."""
    if step != "auto":
        return step
    dt = max(r.index.to_series().diff().dt.total_seconds().median() for r in raws)
    for minutes in (5, 10, 15, 30, 60):
        if minutes * 60 >= 2 * dt:
            return f"{minutes}min"
    return "1h"


def intro(args: argparse.Namespace, raw: pd.Series, x: pd.Series | pd.DataFrame,
          step: str | None = None) -> None:
    grid = f"сітка {step}: {len(x)} точок" if step else f"{len(x)} точок без сітки"
    print(f"{args.device} / {args.label or args.metric}: {len(raw)} повідомлень, "
          f"{raw.index[0]:%Y-%m-%d} – {raw.index[-1]:%Y-%m-%d}; "
          f"сигнал: {SIGNALS[args.signal]}; {grid}")


def load_many(args: argparse.Namespace) -> tuple[pd.DataFrame, str]:
    """--metrics a,b — кілька показників ОДНОГО пристрою на спільній сітці.

    Сигнал можна задати окремо для кожного показника через двокрапку:
    --metrics pkt_sent:rate,pkt_lost:rate,free_heap
    """
    specs = []
    for item in args.metrics.split(","):
        name, _, sig = item.strip().partition(":")
        if (sig or args.signal) not in SIGNALS:
            sys.exit(f"Невідомий сигнал {sig!r} у --metrics; можна: {', '.join(SIGNALS)}")
        specs.append((name, sig or args.signal))
    raws = [load_raw(args.data, args.device, name, sig) for name, sig in specs]
    step = pick_step(args.step, *raws)
    frame = pd.concat([make_signal(r, s, step) for (_, s), r in zip(specs, raws)], axis=1)
    args.label = "+".join(frame.columns)
    print(f"{args.device} / {args.label}: {', '.join(str(len(r)) for r in raws)} повідомлень; "
          f"сітка {step}: {len(frame)} точок")
    return frame, step


def load_frame(args: argparse.Namespace) -> pd.DataFrame:
    """Один показник (--metric) або кілька (--metrics a,b) — таблиця на сітці."""
    if getattr(args, "metrics", None):
        return load_many(args)[0]
    raw = load(args)
    step = pick_step(args.step, raw)
    x = make_signal(raw, args.signal, step)
    intro(args, raw, x, step)
    return x.to_frame()


def rolling_features(frame: pd.DataFrame, window: str) -> pd.DataFrame:
    """Ознаки для IF/LOF: для кожного показника — значення, відхилення від
    рухомої медіани за `window` і рухомий розкид. Рядки з пропусками — геть."""
    feats = {}
    for col in frame.columns:
        x = frame[col]
        feats[col] = x
        feats[f"{col} - медіана"] = x - x.rolling(window, center=True, min_periods=1).median()
        feats[f"{col} розкид"] = x.rolling(window, center=True, min_periods=1).std(ddof=0)
    return pd.DataFrame(feats).dropna()


# ======================================================================
# Вікна-кандидати
# ======================================================================

def merge_windows(score: pd.Series, flagged: pd.Series, gap: str, min_run: int) -> list[dict]:
    """Сусідні позначені точки (між ними не більше gap) — одне вікно."""
    mask = flagged.to_numpy()
    pad = score.index.to_series().diff().median()   # вікно охоплює і останній крок
    out: list[dict] = []
    for t, v in zip(score.index[mask], score.to_numpy()[mask]):
        if out and t - out[-1]["last"] <= pd.Timedelta(gap):
            w = out[-1]
            w["last"], w["points"] = t, w["points"] + 1
            if abs(v) > abs(w["peak"]):          # пік — найбільше відхилення за модулем
                w["peak"], w["peak_time"] = v, t
        else:
            out.append({"start": t, "last": t, "points": 1, "peak": v, "peak_time": t})
    windows = [w for w in out if w["points"] >= min_run]   # поодинокі позначки — шум
    for w in windows:
        w["end"] = w.pop("last") + pad
    return windows


def _hm(t: pd.Timestamp) -> str:
    return t.strftime("%Y-%m-%d %H:%M")


def print_windows(windows: list[dict], n_flagged: int, n_total: int) -> None:
    print(f"\nПозначено точок: {n_flagged} з {n_total}. Вікон-кандидатів: {len(windows)}")
    if windows:
        print(f"  {'№':>2}  {'початок (UTC)':<17} {'кінець (UTC)':<17} {'тривалість':>10} "
              f"{'точок':>6} {'пік оцінки':>11}")
    for i, w in enumerate(windows, 1):
        hours = (w["end"] - w["start"]).total_seconds() / 3600
        print(f"  {i:>2}  {_hm(w['start']):<17} {_hm(w['end']):<17} {hours:>6.1f} год "
              f"{w['points']:>6} {w['peak']:>11.3g}")


# ======================================================================
# Графік
# ======================================================================

INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e1e0d9", "#fcfcfb"
LINE, FLAG, WIN, TRUTH = "#2a78d6", "#d03b3b", "#eb6834", "#1baf7a"


def save_plot(path: str, title: str, signal: pd.DataFrame, score: pd.Series,
              flagged: pd.Series, windows: list[dict], limits: tuple, score_name: str,
              truth: list[dict]) -> None:
    mpl = need("matplotlib", "matplotlib")
    mpl.use("Agg")                       # малюємо одразу у файл, без вікна
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    n = signal.shape[1]
    fig, axes = plt.subplots(n + 1, 1, figsize=(12, 2.4 * n + 2.6), sharex=True, facecolor=SURFACE,
                             gridspec_kw={"height_ratios": [3] * n + [2]})
    hit = signal.index.isin(score.index[flagged.to_numpy()])
    for ax, col in zip(axes, signal.columns):
        ax.plot(signal.index, signal[col], color=LINE, lw=0.7, label=col)
        ax.plot(signal.index[hit], signal[col].to_numpy()[hit], "o", ms=2.5, color=FLAG,
                label="позначені точки")
        ax.set_ylabel(col, color=MUTED)
    axes[-1].plot(score.index, score, color=MUTED, lw=0.7)
    for lim in limits:
        axes[-1].axhline(lim, color=FLAG, lw=1, ls="--", label="поріг" if lim == limits[0] else None)
    top = max([abs(v) for v in limits] or [0])
    if top and np.nanmax(np.abs(score.to_numpy())) > 20 * top:
        axes[-1].set_yscale("symlog", linthresh=top)   # величезні піки не «з'їдають» поріг
        if np.nanmin(score.to_numpy()) >= 0:
            axes[-1].set_ylim(bottom=0)
    axes[-1].set_ylabel(score_name, color=MUTED)
    for ax in axes:
        for k, w in enumerate(windows):
            ax.axvspan(w["start"], w["end"], color=WIN, alpha=0.18, lw=0,
                       label="вікна-кандидати" if k == 0 else None)
        for k, inc in enumerate(truth):      # справжні інциденти — смуга вгорі
            ax.axvspan(pd.Timestamp(inc["start"]), pd.Timestamp(inc["end"]), ymin=0.93,
                       color=TRUTH, alpha=0.9, lw=0, label="інцидент (відповіді)" if k == 0 else None)
        ax.set_facecolor(SURFACE)
        ax.set_axisbelow(True)              # сітка під смугами, а не поверх них
        ax.grid(color=GRID, lw=0.6)
        ax.tick_params(colors=MUTED, labelsize=8)
        for side in ax.spines.values():
            side.set_color("#c3c2b7")
    axes[0].legend(loc="upper left", fontsize=8, ncol=4, framealpha=0.9)
    if limits:
        axes[-1].legend(loc="upper left", fontsize=8)
    axes[-1].xaxis.set_major_formatter(mdates.ConciseDateFormatter(axes[-1].xaxis.get_major_locator()))
    axes[0].set_title(title, color=INK, fontsize=11, loc="left")
    fig.tight_layout()
    fig.savefig(path, dpi=100)
    plt.close(fig)
    print(f"\nГрафік: {Path(path).resolve()}")


# ======================================================================
# Порівняння з відповідями — ті самі правила, що в автоматичній перевірці
# ======================================================================

def load_truth(path: str, data: str) -> dict:
    p = Path(path)
    if not p.is_file():
        sys.exit(f"Не знайдено {p}. Відповіді є лише до навчального набору (tuning_truth.json); "
                 f"для власного варіанта --evaluate не працює.")
    truth = json.loads(p.read_text(encoding="utf-8"))
    rows = pq.ParquetFile(_parquet(data)).metadata.num_rows
    if truth.get("fingerprint", {}).get("rows") not in (None, rows):
        sys.exit(f"{p.name} описує інший набір даних (у ньому {truth['fingerprint']['rows']} "
                 f"рядків, у {Path(data).name} — {rows}). Порівнювати немає з чим.")
    return truth


def iou(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Перекриття двох інтервалів часу за мірою Жаккара: спільне / об'єднання."""
    inter = max(0.0, min(a[1], b[1]) - max(a[0], b[0]))
    union = (a[1] - a[0]) + (b[1] - b[0]) - inter
    return inter / union if union > 0 else 0.0


def evaluate(windows: list[dict], device: str, truth: dict, cls: str | None) -> None:
    # Знахідка з одним пристроєм проходить правило «щонайменше п'ята частина
    # пристроїв знахідки — з інциденту» лише тоді, коли цей пристрій входить
    # в інцидент. Тому порівнюємо тільки з такими інцидентами.
    incs = [i for i in truth["incidents"] if device in i["devices"]]
    spans = [(w["start"].timestamp(), w["end"].timestamp()) for w in windows]
    cands = sorted(((iou(s, (inc["start_s"], inc["end_s"])), wi, ii)
                    for wi, s in enumerate(spans) for ii, inc in enumerate(incs)), reverse=True)
    used_w, used_i, pairs = set(), set(), []
    for ov, wi, ii in cands:                 # жадібно, від найкращого перекриття
        if ov >= MIN_IOU and wi not in used_w and ii not in used_i:
            used_w.add(wi)
            used_i.add(ii)
            pairs.append((wi, ii, ov))

    print(f"\nПорівняння з відповідями для {device}: інцидентів за участю пристрою — {len(incs)}")
    for wi, ii, ov in sorted(pairs):
        inc = incs[ii]
        tail = ("" if not cls or cls == inc["class"]
                else f"  (ваш клас {cls}: зарахується з коефіцієнтом 0,7)")
        print(f"  ЗНАЙДЕНО   вікно {wi + 1:<3} = {inc['id']} {inc['class']:<18} IoU {ov:.2f}{tail}")
    for wi, s in enumerate(spans):
        if wi not in used_w:
            best = max((iou(s, (i["start_s"], i["end_s"])) for i in incs), default=0.0)
            print(f"  ХИБНЕ      вікно {wi + 1:<3} {_hm(windows[wi]['start'])} - "
                  f"{_hm(windows[wi]['end'])}  (найбільше перекриття з інцидентом {best:.2f})")
    for ii, inc in enumerate(incs):
        if ii not in used_i:
            best = max((iou(s, (inc["start_s"], inc["end_s"])) for s in spans), default=0.0)
            print(f"  ПРОПУЩЕНО  {inc['id']} {inc['class']:<18} {inc['start'][:16]} - "
                  f"{inc['end'][:16]}  (найкраще IoU {best:.2f}, потрібно {MIN_IOU})")
    tp = len(pairs)
    prec = f"{tp / len(windows):.2f}" if windows else "-"
    rec = f"{tp / len(incs):.2f}" if incs else "-"
    print(f"  Точність {prec} ({tp} з {len(windows)} вікон справжні)   "
          f"Повнота {rec} ({tp} з {len(incs)} інцидентів знайдено)")
    print("  Нагадування: у findings.json кожне хибне вікно знімає 5 % балів (сумарно до 30 %).")


# ======================================================================
# Підсумок: однаковий кінець для всіх детекторів
# ======================================================================

def report(args: argparse.Namespace, signal: pd.Series | pd.DataFrame, score: pd.Series,
           flagged: pd.Series, limits: tuple = (), score_name: str = "оцінка") -> list[dict]:
    """Вікна -> таблиця -> графік -> (JSON) -> (порівняння з відповідями)."""
    if not flagged.index.equals(score.index):
        flagged = flagged.reindex(score.index, fill_value=False)
    flagged = flagged.astype(bool)
    windows = merge_windows(score, flagged, args.gap, args.min_run)
    print_windows(windows, int(flagged.sum()), int(score.notna().sum()))

    label = args.label or args.metric
    # у багатовимірному режимі сигнал уже видно в назвах стовпчиків (pkt_sent:rate)
    what = label if ":" in label else f"{label} ({SIGNALS[args.signal]})"
    truth = args.truth
    own = [i for i in truth["incidents"] if args.device in i["devices"]] if truth else []
    frame = signal.to_frame() if isinstance(signal, pd.Series) else signal
    png = args.png or re.sub(r"[^\w.-]", "-", f"{args.method}_{args.device}_{label}.png")
    save_plot(png, f"{args.method}: {args.device} / {what}", frame, score, flagged, windows,
              limits, score_name, own)

    if args.json:
        entries = [{"devices": [args.device], "class": args.cls,
                    "start": w["start"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end": w["end"].strftime("%Y-%m-%dT%H:%M:%SZ"), "confidence": 0.5,
                    "evidence": f"{args.method}: {what}, {w['points']} позначених точок, "
                                f"пік оцінки {w['peak']:.3g} о {_hm(w['peak_time'])} UTC"}
                   for w in windows]
        Path(args.json).write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Кандидатів записано у {args.json}: {len(entries)}. Це ще НЕ findings.json: "
              f"перегляньте кожне вікно на графіку, виправте confidence та evidence і лише тоді "
              f"переносьте у свій findings.json.")
    if truth:
        evaluate(windows, args.device, truth, args.cls)
    return windows
