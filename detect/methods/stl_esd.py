#!/usr/bin/env python3
"""
Стадія 3 — STL + ESD: сезонна декомпозиція і узагальнений ESD-тест
(Cleveland et al., 1990; Rosner, 1983; Hochenbaum et al., 2017).

1. STL розкладає сигнал на три складові:

       сигнал = тренд + сезонна складова (добовий цикл, --period) + залишок

   Тренд і добовий цикл — це «нормальна» поведінка будівлі. Усе незвичне
   лишається в залишку.
2. Узагальнений ESD-тест (generalized ESD) шукає викиди в залишку: по черзі
   вилучає найдальшу від центру точку і перевіряє, чи вона «надто далека»
   для вибірки такого розміру (критичне значення з t-розподілу, рівень
   значущості --alpha). Викидами вважаються перші k вилучених точок, де k —
   найбільший крок, на якому перевірка ще спрацювала. Більше ніж --max-share
   від усіх точок тест не шукає.
   Як у методі S-H-ESD (Twitter), центр і розкид — медіана і MAD, а не
   середнє і σ: інакше серія з десятків викидів «маскує» сама себе.

Що ловить: сплески і серії неможливих значень, незвичне значення для цього
часу доби (наприклад, споживання вночі). Чого не ловить: повільний дрейф —
STL охоче записує його у тренд. Розклад «знає» лише про добовий цикл, тому
вихідні дні часто дають хибні спрацювання (спробуйте --period 7D на довгому
ряду).

Працює з ОДНИМ рядом, який обираєте ви. Приклади (з кореня репозиторію):
    python detect/methods/stl_esd.py --data tuning.parquet --device power-05-mee --metric power_w
    python detect/methods/stl_esd.py --device power-05-mee --metric power_w --evaluate tuning_truth.json
    python detect/methods/stl_esd.py --device power-04-wor --metric power_w --alpha 0.01
"""

from __future__ import annotations

import common

pd, np = common.pd, common.np

PACKAGES = (("statsmodels.tsa.seasonal", "statsmodels"), ("scipy.stats", "scipy"))


def generalized_esd(r: pd.Series, max_k: int, alpha: float) -> tuple[pd.Index, float]:
    """Мітки часу викидів і критичне значення, на якому тест зупинився."""
    t_dist = common.need("scipy.stats", "scipy").t
    values = r.to_numpy()
    n = len(values)
    left = np.ones(n, dtype=bool)                   # точки, ще не вилучені
    removed, k, crit_k, crit_1 = [], 0, np.nan, np.nan
    for i in range(1, max_k + 1):
        v = values[left]
        center = np.median(v)
        spread = 1.4826 * np.median(np.abs(v - center)) or (np.std(v) or 1e-9)
        dist = np.where(left, np.abs(values - center) / spread, -1.0)
        j = int(np.argmax(dist))
        # критичне значення λ_i (Rosner, 1983)
        p = 1 - alpha / (2 * (n - i + 1))
        t = t_dist.ppf(p, n - i - 1)
        crit = (n - i) * t / np.sqrt((n - i - 1 + t * t) * (n - i + 1))
        removed.append(j)
        left[j] = False
        crit_1 = crit if i == 1 else crit_1
        if dist[j] > crit:                           # i-та точка ще «надто далека»
            k, crit_k = i, crit
    return r.index[removed[:k]], (crit_k if k else crit_1)


def score_series(x: pd.Series, args, say=print) -> common.Scored:
    """Ядро методу: сигнал на рівномірній сітці -> оцінка і позначки.

    Цю саму функцію викликає realdata/clean.py для кожного вузла Intel Lab.
    """
    STL = common.need("statsmodels.tsa.seasonal", "statsmodels").STL
    period = int(pd.Timedelta(args.period) / common.grid_step(x))
    if len(x) < 2 * period:
        raise common.TooShort(f"Для STL потрібно щонайменше два періоди даних ({args.period} x 2).")
    # STL не терпить пропусків: заповнюємо їх інтерполяцією, але потім
    # заповнені точки не оцінюємо — там немає справжніх даних.
    missing = x.isna()
    filled = x.interpolate("time", limit_direction="both")
    parts = STL(filled.to_numpy(), period=period, robust=True).fit()
    resid = pd.Series(parts.resid, index=x.index)

    real = resid[~missing]
    max_k = max(1, int(args.max_share * len(real)))
    outliers, crit = generalized_esd(real, max_k, args.alpha)
    say(f"STL: період {args.period} = {period} кроків; ESD знайшов викидів: "
        f"{len(outliers)} (шукав не більше {max_k})")

    # Оцінка для графіка і таблиці — «відстань від центру в MAD», як у тесті
    # (лише без поступового вилучення точок, тому межа на графіку наближена).
    center = real.median()
    mad = 1.4826 * (real - center).abs().median() or real.std() or 1.0
    score = (resid - center) / mad
    score[missing] = np.nan
    flagged = pd.Series(score.index.isin(outliers), index=score.index)
    return common.Scored(score, flagged, (-crit, crit), "залишок, MAD")


def describe(args) -> str:
    """Головні параметри одним рядком (його друкує realdata/clean.py)."""
    return (f"STL + ESD: період {args.period}, α={args.alpha:g}, "
            f"не більше {100 * args.max_share:g} % точок")


def make_parser():
    # Крок 15 хв: усереднення гасить швидкі перемикання (компресори, насоси),
    # і залишок STL стає спокійнішим. Дрібніший крок — більше хибних викидів.
    ap = common.parser(__doc__, "stl_esd", step="15min")
    m = ap.add_argument_group("параметри методу")
    m.add_argument("--period", default="1D", help="період сезонності: 1D — доба (типово), 7D — тиждень")
    m.add_argument("--alpha", type=float, default=0.05,
                   help="рівень значущості ESD (типово 0.05; менше — суворіше)")
    m.add_argument("--max-share", type=float, default=0.05,
                   help="найбільша частка точок, яку тест може назвати викидами (типово 0.05)")
    return ap


def main() -> None:
    args = common.parse(make_parser())
    common.require(PACKAGES)

    raw = common.load(args)
    args.step = common.pick_step(args.step, raw)    # фактичний крок замість auto
    x = common.make_signal(raw, args.signal, args.step)
    common.intro(args, raw, x, args.step)
    res = score_series(x, args)

    # Як читати результат: зверху ряд і позначені викиди, знизу — залишок
    # після вилучення тренду й добового циклу (у MAD). Пунктир — критичне
    # значення, на якому тест зупинився. Якщо позначено забагато —
    # зменште --alpha або --max-share; якщо інцидент тягнеться годинами,
    # а позначено лише його краї — збільште --max-share.
    common.report(args, x, res.score, res.flagged, res.limits, res.name)


if __name__ == "__main__":
    main()
