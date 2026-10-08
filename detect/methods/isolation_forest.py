#!/usr/bin/env python3
"""
Стадія 3 — Isolation Forest, ліс ізоляції (Liu, Ting, Zhou, 2008; scikit-learn).

Ідея. Будуємо багато випадкових дерев (--trees). Кожне ділить точки
випадковими розрізами за випадковими ознаками, доки кожна точка не опиниться
окремо. Рідкісну, не схожу на решту точку ізолювати легко — їй вистачає
кількох розрізів; звичайну, оточену сусідами, — важко. Оцінка аномальності
(від 0 до 1) тим більша, чим коротший у середньому шлях до ізоляції;
близько 0.5 і нижче — нічого особливого.

Ознаки — для кожного показника на сітці --step:
    значення;  відхилення від рухомої медіани за --window;  рухомий розкид.
Одновимірний режим — один показник (--metric). Багатовимірний — кілька
показників одного пристрою (--metrics a,b): тоді аномальними стають і
незвичні ПОЄДНАННЯ значень, кожне з яких окремо виглядає нормально.

--contamination — частка точок, яку метод ОБОВ'ЯЗКОВО позначить. Тому навіть
на справному пристрої з'являться позначки: дивіться на величину оцінки і на
графік, а не лише на сам факт позначення. Довгий інцидент, що займає більшу
частку ряду, ніж --contamination, буде позначено лише частково.

Працює з ОДНИМ пристроєм, який обираєте ви. Приклади (з кореня репозиторію):
    python detect/methods/isolation_forest.py --data tuning.parquet --device power-05-mee --metric power_w
    python detect/methods/isolation_forest.py --device gw-01 --metrics pkt_sent,pkt_lost --signal rate
    python detect/methods/isolation_forest.py --device temp-06-lob --metrics temperature,humidity
"""

from __future__ import annotations

import math

import common

pd = common.pd

PACKAGES = (("sklearn.ensemble", "scikit-learn"), ("sklearn.preprocessing", "scikit-learn"))


def score_series(x: pd.Series | pd.DataFrame, args, say=print) -> common.Scored:
    """Ядро методу: один показник (Series) або кілька (DataFrame) на сітці ->
    оцінка і позначки. Цю саму функцію викликає realdata/clean.py."""
    ensemble = common.need("sklearn.ensemble", "scikit-learn")
    preprocessing = common.need("sklearn.preprocessing", "scikit-learn")
    frame = x.to_frame() if isinstance(x, pd.Series) else x
    X = common.rolling_features(frame, args.window)
    say(f"Ознак: {X.shape[1]} ({', '.join(map(str, X.columns))}); рядків без пропусків: {len(X)}")
    least = math.ceil(1 / args.contamination)       # інакше позначати нічого
    if len(X) < least:
        raise common.TooShort(f"Рядків без пропусків {len(X)}: для --contamination "
                              f"{args.contamination:g} потрібно щонайменше {least}.")
    # Масштабуємо ознаки (медіана і міжквартильний розмах), щоб жодна не
    # переважала лише через одиниці виміру (Вт проти °C).
    Z = preprocessing.RobustScaler().fit_transform(X)

    # random_state=0 — щоб повторний запуск дав той самий результат (для звіту)
    forest = ensemble.IsolationForest(n_estimators=args.trees, contamination=args.contamination,
                                      random_state=0).fit(Z)
    score = pd.Series(-forest.score_samples(Z), index=X.index)   # більше — аномальніше
    cut = -forest.offset_              # поріг, за яким позначено частку contamination
    return common.Scored(score, score > cut, (cut,), "аномальність")


def describe(args) -> str:
    """Головні параметри одним рядком (його друкує realdata/clean.py)."""
    return (f"Isolation Forest: contamination {args.contamination:g}, "
            f"вікно ознак {args.window}, дерев {args.trees}")


def make_parser():
    ap = common.parser(__doc__, "isolation_forest")
    m = ap.add_argument_group("параметри методу")
    m.add_argument("--metrics", help="кілька показників пристрою через кому (багатовимірний режим); "
                                     "сигнал для окремого показника: pkt_sent:rate")
    m.add_argument("--contamination", type=common.share(0, 0.5), default=0.02,
                   help="частка точок, які позначити аномальними (типово 0.02 = 2 %%)")
    m.add_argument("--window", default="2h", help="вікно для рухомих ознак (типово 2h)")
    m.add_argument("--trees", type=int, default=200, help="кількість дерев (типово 200)")
    return ap


def main() -> None:
    args = common.parse(make_parser())
    common.require(PACKAGES)

    frame = common.load_frame(args)                  # таблиця: стовпчик на показник
    res = score_series(frame, args)

    # Як читати результат: «пік оцінки» близько 0.5 — сумнівна аномалія, яку
    # метод позначив лише тому, що мусив позначити 2 % точок; 0.7 і більше —
    # точка справді вирізняється. У багатовимірному режимі подивіться, на якій
    # панелі графіка видно відхилення: це підкаже, який показник «винен».
    common.report(args, frame, res.score, res.flagged, res.limits, res.name)


if __name__ == "__main__":
    main()
