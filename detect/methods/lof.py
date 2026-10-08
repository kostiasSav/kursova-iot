#!/usr/bin/env python3
"""
Стадія 3 — Local Outlier Factor, локальний коефіцієнт викиду
(Breunig et al., 2000; scikit-learn).

Ідея. Для кожної точки знаходимо --neighbors найближчих сусідів у просторі
ознак і порівнюємо щільність навколо точки зі щільністю навколо її сусідів.
LOF близько 1 — точка у звичайному оточенні; 1.5–2 і більше — вона лежить
у розрідженій області: далі від сусідів, ніж сусіди одне від одного.
«Локальний» означає, що порівняння йде з найближчим оточенням, а не з усім
набором: точка поруч зі щільним скупченням може бути викидом, навіть якщо
деінде є ще розрідженіші області (наприклад, нічні й денні режими роботи).

Ознаки — ті самі, що в isolation_forest.py: для кожного показника на сітці
значення, відхилення від рухомої медіани за --window і рухомий розкид.
Один показник (--metric) або кілька показників пристрою (--metrics a,b).

Важлива пастка. Якщо аномальних точок багато і вони схожі між собою
(інцидент триває годинами), вони утворюють ВЛАСНЕ щільне скупчення, і LOF
вважає їх нормальними. --neighbors має бути більшим за кількість точок
такого інциденту на сітці: типові 300 сусідів покривають ~25 год при кроці
5 хв або ~50 год при кроці 10 хв. Спробуйте --neighbors 20 і порівняйте.

--contamination — частка точок, яку метод ОБОВ'ЯЗКОВО позначить, тож і на
справному пристрої з'являться позначки: дивіться на величину LOF.

Працює з ОДНИМ пристроєм, який обираєте ви. Приклади (з кореня репозиторію):
    python detect/methods/lof.py --data tuning.parquet --device power-05-mee --metric power_w
    python detect/methods/lof.py --device gw-02 --metrics pkt_sent,pkt_lost --signal rate --contamination 0.05
"""

from __future__ import annotations

import common

pd = common.pd

PACKAGES = (("sklearn.neighbors", "scikit-learn"), ("sklearn.preprocessing", "scikit-learn"))


def score_series(x: pd.Series | pd.DataFrame, args, say=print) -> common.Scored:
    """Ядро методу: один показник (Series) або кілька (DataFrame) на сітці ->
    оцінка і позначки. Цю саму функцію викликає realdata/clean.py."""
    neighbors = common.need("sklearn.neighbors", "scikit-learn")
    preprocessing = common.need("sklearn.preprocessing", "scikit-learn")
    frame = x.to_frame() if isinstance(x, pd.Series) else x
    X = common.rolling_features(frame, args.window)
    say(f"Ознак: {X.shape[1]} ({', '.join(map(str, X.columns))}); рядків без пропусків: {len(X)}")
    if len(X) <= args.neighbors:
        raise common.TooShort(f"Точок ({len(X)}) менше, ніж --neighbors ({args.neighbors}).")
    # LOF міряє відстані, тому ознаки обов'язково зводимо до спільного масштабу
    Z = preprocessing.RobustScaler().fit_transform(X)

    lof = neighbors.LocalOutlierFactor(n_neighbors=args.neighbors,
                                       contamination=args.contamination).fit(Z)
    score = pd.Series(-lof.negative_outlier_factor_, index=X.index)   # це і є LOF
    cut = -lof.offset_                 # поріг, за яким позначено частку contamination
    return common.Scored(score, score > cut, (cut,), "LOF")


def describe(args) -> str:
    """Головні параметри одним рядком (його друкує realdata/clean.py)."""
    return (f"LOF: сусідів {args.neighbors}, contamination {args.contamination:g}, "
            f"вікно ознак {args.window}")


def make_parser():
    ap = common.parser(__doc__, "lof")
    m = ap.add_argument_group("параметри методу")
    m.add_argument("--metrics", help="кілька показників пристрою через кому (багатовимірний режим); "
                                     "сигнал для окремого показника: pkt_sent:rate")
    m.add_argument("--neighbors", type=int, default=300,
                   help="кількість сусідів (типово 300; має бути більшою за довжину інциденту в точках)")
    m.add_argument("--contamination", type=common.share(0, 0.5), default=0.02,
                   help="частка точок, які позначити аномальними (типово 0.02 = 2 %%)")
    m.add_argument("--window", default="2h", help="вікно для рухомих ознак (типово 2h)")
    return ap


def main() -> None:
    args = common.parse(make_parser())
    common.require(PACKAGES)

    frame = common.load_frame(args)                  # таблиця: стовпчик на показник
    res = score_series(frame, args)

    # Як читати результат: «пік оцінки» — найбільший LOF у вікні. Близько 1–1.3 —
    # точку позначено лише тому, що метод мусив позначити 2 % точок; 2 і більше —
    # точка справді «відірвана» від сусідів. Якщо довгий інцидент позначено
    # лише по краях — збільште --neighbors.
    common.report(args, frame, res.score, res.flagged, res.limits, res.name)


if __name__ == "__main__":
    main()
