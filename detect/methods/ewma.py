#!/usr/bin/env python3
"""
Стадія 3 — EWMA control chart, контрольна карта експоненційно зваженого
ковзного середнього (Roberts, 1959).

Ідея. Окремий відлік шумний, і невеликий, але СТАЛИЙ зсув у ньому не видно.
EWMA накопичує відхилення від норми, поступово «забуваючи» старі:

    e_t = λ * r_t + (1 - λ) * e_(t-1)

де r_t — відхилення сигналу від типового значення для цього часу доби.
Малий λ (0.05) — довга пам'ять: помічає повільний дрейф, але реагує пізніше.
Великий λ (0.3) — коротка пам'ять, майже звичайний поріг на кожну точку.
Тривога — коли e_t виходить за контрольні межі ±L * σ_e.

Кроки:
  1. сигнал зводиться на рівномірну сітку часу (--step);
  2. «типовий профіль доби» рахується за перші --ref-days діб — еталонний
     період, який ви вважаєте нормальним (перевірте це на графіку!), окремо
     для будніх і вихідних: розпорядок будівлі в них різний, і без цього
     кожна субота виглядала б аномалією (спробуйте --profile day);
  3. r = сигнал - профіль;  e = EWMA(r);
  4. σ_e — робастний розкид e в еталонному періоді; межі ±L * σ_e.

Чому σ_e беремо з даних, а не за класичною формулою σ * sqrt(λ / (2 - λ)):
формула припускає незалежні відліки, а сусідні відліки температури чи
потужності сильно пов'язані між собою. З класичною формулою межі виходять
надто вузькими, і тривога лунає майже постійно. Варто показати це у звіті.

Що ловить: повільний дрейф, тривалий зсув рівня (зокрема швидкості
лічильника, --signal rate), незвичне споживання. Чого не ловить: поодинокі
короткі сплески — згладжування їх «розмазує» (для них є baseline.py).

Працює з ОДНИМ рядом, який обираєте ви. Приклади (з кореня репозиторію):
    python detect/methods/ewma.py --data tuning.parquet --device temp-06-lob --metric temperature
    python detect/methods/ewma.py --device temp-06-lob --metric temperature --evaluate tuning_truth.json
    python detect/methods/ewma.py --device gw-02 --metric pkt_sent --signal rate
"""

from __future__ import annotations

import common

pd, np = common.pd, common.np

PACKAGES: tuple = ()          # лише numpy і pandas — вони вже є в усіх


def daily_profile(x: pd.Series, ref: pd.Series, mode: str) -> pd.Series:
    """Типове значення для кожного часу доби — медіана за еталонні дні."""
    if mode == "none":
        return pd.Series(ref.median(), index=x.index)
    weekend = ref.index.dayofweek >= 5
    split = mode == "workdays" and weekend.any() and not weekend.all()

    def key(idx: pd.DatetimeIndex):
        minute = idx.hour * 60 + idx.minute                 # хвилина доби
        return minute + 10000 * (idx.dayofweek >= 5) if split else minute

    typical = ref.groupby(key(ref.index)).median()
    return pd.Series(typical.reindex(key(x.index)).to_numpy(), index=x.index)


def score_series(x: pd.Series, args, say=print) -> common.Scored:
    """Ядро методу: сигнал на рівномірній сітці -> оцінка і позначки.

    Цю саму функцію викликає realdata/clean.py для кожного вузла Intel Lab.
    """
    ref_end = x.index[0] + pd.Timedelta(days=args.ref_days)
    ref = x[x.index < ref_end].dropna()
    if len(ref) < 10:
        raise common.TooShort("В еталонному періоді замало даних: збільште --ref-days.")
    say(f"Еталонний період: {ref.index[0]:%Y-%m-%d %H:%M} – {ref_end:%Y-%m-%d %H:%M} UTC")

    r = x - daily_profile(x, ref, args.profile)
    r = r - r[r.index < ref_end].median()           # у нормі відхилення в середньому 0

    # Сама EWMA — кілька рядків. Старт з нуля (з «норми»); пропуск у даних
    # (NaN) не змінює e, а просто переносить попереднє значення далі.
    e, prev = np.zeros(len(r)), 0.0
    for i, value in enumerate(r.to_numpy()):
        if not np.isnan(value):
            prev = args.lam * value + (1 - args.lam) * prev
        e[i] = prev
    e = pd.Series(e, index=r.index)

    e_ref = e[e.index < ref_end]
    sigma = 1.4826 * float((e_ref - e_ref.median()).abs().median())
    if not sigma > 0:
        raise common.TooShort("Сигнал в еталонному періоді сталий (σ_e = 0) — EWMA тут не допоможе.")
    score = e / sigma                               # EWMA у одиницях σ_e
    return common.Scored(score, score.abs() > args.L, (-args.L, args.L), "EWMA, σ_e")


def describe(args) -> str:
    """Головні параметри одним рядком (його друкує realdata/clean.py)."""
    return (f"EWMA: λ={args.lam:g}, L={args.L:g}, еталон — перші {args.ref_days:g} діб, "
            f"профіль {args.profile}")


def make_parser():
    ap = common.parser(__doc__, "ewma")
    m = ap.add_argument_group("параметри методу")
    m.add_argument("--lam", type=common.share(0, 1), default=0.1,
                   help="λ, вага нової точки, 0 < λ <= 1 (типово 0.1; менше — довша пам'ять)")
    m.add_argument("--L", type=float, default=5.0,
                   help="ширина контрольних меж у σ_e (типово 5)")
    m.add_argument("--ref-days", type=float, default=7,
                   help="скільки перших діб вважати еталоном «норми» (типово 7)")
    m.add_argument("--profile", choices=["workdays", "day", "none"], default="workdays",
                   help="типовий профіль доби: окремо для будніх і вихідних (типово), "
                        "один для всіх днів, або без профілю")
    return ap


def main() -> None:
    args = common.parse(make_parser())

    raw = common.load(args)
    args.step = common.pick_step(args.step, raw)    # фактичний крок замість auto
    x = common.make_signal(raw, args.signal, args.step)
    common.intro(args, raw, x, args.step)
    res = score_series(x, args)

    # Як читати результат: «пік оцінки» — найбільше відхилення EWMA у вікні,
    # у σ_e; знак показує напрям зсуву (+ вгору, - вниз). На нижньому графіку
    # видно, як EWMA повільно «виповзає» за межу при дрейфі і різко — при
    # стрибку. Вікно, що починається пізніше за інцидент, — нормально: EWMA
    # потрібен час, щоб накопичити відхилення (менший --L або більший --lam
    # скорочують затримку, але додають хибних тривог).
    common.report(args, x, res.score, res.flagged, res.limits, res.name)


if __name__ == "__main__":
    main()
