#!/usr/bin/env python3
"""
Стадія 3 — Matrix Profile, матричний профіль (Yeh et al., 2016; бібліотека stumpy).

Беремо кожен фрагмент ряду довжиною --window (наприклад, 2 години) і шукаємо
найсхожіший на нього фрагмент деінде в ряді. Відстань до цього «двійника»
і є матричним профілем (MP). Порівнюються ФОРМИ: кожен фрагмент спершу
нормується (z-нормалізація), тож абсолютний рівень і масштаб не важливі.

Аномалії бувають двох протилежних видів (--find):
  discords — фрагмент не схожий ні на що інше в ряді (MP великий): небачена
             форма, сплеск, обрив. Оцінка — робастна z-оцінка MP: на скільки
             «стійких сигм» MP більший за типовий для цього ряду (поріг 8).
  repeats  — фрагмент надто точно повторює інший (MP майже 0): застрягле
             значення або надто регулярні інтервали між повідомленнями
             (--signal interval). Оцінка — «регулярність» 1 - MP / sqrt(2m),
             де m — кількість точок у фрагменті; 1 = точна копія (поріг 0.95).

Майже пласкі фрагменти (розкид < 10 % типового для ряду) stumpy вважає
сталими: після z-нормалізації від них лишився б самий шум, і вони виглядали б
«незвичними» без жодної причини.

Перший запуск повільний: stumpy компілює свій код (numba), це 10–60 с.

Працює з ОДНИМ рядом, який обираєте ви. Приклади (з кореня репозиторію):
    python detect/methods/matrix_profile.py --data tuning.parquet --device power-05-mee --metric power_w
    python detect/methods/matrix_profile.py --device vibration-06-lob --metric vib_rms --signal interval --find repeats
"""

from __future__ import annotations

import common

pd, np = common.pd, common.np

PACKAGES = (("stumpy", "stumpy"),)


def threshold_of(args) -> float:
    """Поріг оцінки: заданий у --threshold або типовий для режиму --find."""
    if args.threshold is not None:
        return args.threshold
    return 8.0 if args.find == "discords" else 0.95


def score_series(x: pd.Series, args, say=print) -> common.Scored:
    """Ядро методу: сигнал на рівномірній сітці -> оцінка і позначки.

    Цю саму функцію викликає realdata/clean.py для кожного вузла Intel Lab.
    """
    stumpy = common.need("stumpy", "stumpy")
    step = common.grid_step(x)
    m_len = int(pd.Timedelta(args.window) / step)   # точок у фрагменті
    if m_len < 4 or len(x) < 4 * m_len:
        raise common.TooShort(f"--window {args.window} при кроці {args.step} дає {m_len} точок: "
                              f"потрібно щонайменше 4 і не більше чверті ряду.")
    values = x.to_numpy(dtype=float)
    spread = 1.4826 * np.nanmedian(np.abs(values - np.nanmedian(values))) or np.nanstd(values) or 1.0
    flat = x.rolling(m_len).std().to_numpy()[m_len - 1:] < 0.1 * spread

    say(f"Рахую матричний профіль: фрагменти завдовжки {args.window} (точок у кожному: {m_len})...")
    # Фрагменти з пропусками stumpy ділить на нуль — це очікувано (їх однаково
    # не оцінюємо), тож попередження numpy про ділення на нуль вимикаємо.
    with np.errstate(divide="ignore", invalid="ignore"):
        mp = stumpy.stump(values, m_len, T_A_subseq_isconstant=flat)
    dist = mp[:, 0].astype(float)                   # стовпець 0 — відстань до «двійника»
    dist[~np.isfinite(dist)] = np.nan               # фрагменти з пропусками не оцінюємо
    # MP[i] описує фрагмент x[i : i + m]; відносимо його до середини фрагмента
    times = x.index[:len(dist)] + (m_len // 2) * step
    profile = pd.Series(dist, index=times)

    threshold = threshold_of(args)
    if args.find == "discords":
        center = profile.median()
        # Якщо більшість фрагментів пласкі, MP у них 0 і MAD теж 0 — тоді
        # беремо звичайне стандартне відхилення.
        mad = 1.4826 * (profile - center).abs().median()
        mad = mad if mad > 0 else (profile.std() or 1.0)
        score, name = (profile - center) / mad, "z-оцінка MP"
    else:
        score, name = 1 - profile / np.sqrt(2 * m_len), "регулярність"
    return common.Scored(score, score > threshold, (threshold,), name)


def describe(args) -> str:
    """Головні параметри одним рядком (його друкує realdata/clean.py)."""
    return f"Matrix Profile: {args.find}, фрагмент {args.window}, поріг {threshold_of(args):g}"


def make_parser():
    ap = common.parser(__doc__, "matrix_profile")
    m = ap.add_argument_group("параметри методу")
    m.add_argument("--window", default="2h",
                   help="довжина фрагмента (типово 2h) — приблизно тривалість подій, які шукаєте")
    m.add_argument("--find", choices=["discords", "repeats"], default="discords",
                   help="discords — незвичні фрагменти (типово); repeats — надто точні повтори")
    m.add_argument("--threshold", type=float,
                   help="поріг оцінки (типово 8 для discords, 0.95 для repeats)")
    return ap


def main() -> None:
    args = common.parse(make_parser())
    common.require(PACKAGES)

    raw = common.load(args)
    args.step = common.pick_step(args.step, raw)    # фактичний крок замість auto
    x = common.make_signal(raw, args.signal, args.step)
    common.intro(args, raw, x, args.step)
    res = score_series(x, args)

    # Як читати результат: discords — пік оцінки показує, наскільки фрагмент
    # «ні на що не схожий»; repeats — 1.0 означає точну копію іншого фрагмента.
    # Вікно позначає середини фрагментів, тому воно буває на --window коротшим
    # або довшим за саму подію. На сигналах, що постійно перемикаються (світло
    # вдень-вночі, вібрація увімк./вимк.), кожен фрагмент трохи унікальний —
    # discords там дають багато хибних вікон: збільште --threshold або --window.
    common.report(args, x, res.score, res.flagged, res.limits, res.name)


if __name__ == "__main__":
    main()
