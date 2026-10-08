#!/usr/bin/env python3
"""
Стадія 3 — базовий статистичний детектор (п. 4.3, детектор № 1).

Рухома робастна z-оцінка. Кожне значення порівнюємо з медіаною значень за
попередні --window (типово добу) і ділимо відхилення на MAD того самого
вікна — «стійкий до викидів» замінник стандартного відхилення:

    z = (x - медіана_вікна) / (1.4826 * MAD_вікна)

Точку позначаємо, якщо |z| > --threshold (типово 10). Класичні 3–3.5 для
справжніх даних IoT замалі: у них «важкі хвости», і тривоги лунали б щогодини.
Поодинокі позначки — це шум, тому вікно-кандидат лишаємо, лише якщо в ньому
щонайменше --min-run позначених точок (позначки, між якими не більше --gap,
зливаються в одне вікно).

Чому медіана і MAD, а не середнє і σ: один викид на тисячу значень зсуває σ
настільки, що решта викидів перестає її перевищувати. Медіана й MAD цього
майже не помічають (див. також mad() у example.py).

Що ловить: різкі сплески, серії неможливих значень, раптові стрибки рівня
(перші години після стрибка). Чого НЕ ловить: повільний дрейф (вікно «пливе»
разом із ним) і застрягле значення (відхилень немає взагалі).

Детектор працює з ОДНИМ рядом, який обираєте ви, на сирих повідомленнях без
сітки. Пошук інцидентів у власному варіанті — ваша робота: переглядайте ряди,
читайте графіки й вирішуйте самі.

Приклади (з кореня репозиторію):
    python detect/baseline.py --data tuning.parquet --device power-05-mee --metric power_w
    python detect/baseline.py --device power-05-mee --metric power_w --evaluate tuning_truth.json
    python detect/baseline.py --device gw-01 --metric pkt_lost --signal rate --gap 3h
"""

from __future__ import annotations

import sys
from pathlib import Path

# Спільні функції (завантаження, вікна, графік, оцінювання) — у methods/common.py
sys.path.insert(0, str(Path(__file__).resolve().parent / "methods"))
import common  # noqa: E402

pd, np = common.pd, common.np


def rolling_robust_z(x: pd.Series, window: str) -> pd.Series:
    """z-оцінка кожної точки відносно попередніх `window` значень."""
    # closed="left": у «норму» входять лише МИНУЛІ значення, сама точка — ні.
    # Так працював би детектор у реальному часі: він не знає майбутнього.
    med = x.rolling(window, closed="left", min_periods=30).median()
    # MAD вікна (наближено): медіана відхилень від рухомої медіани
    mad = (x - med).abs().rolling(window, closed="left", min_periods=30).median()
    # Якщо у вікні всі значення однакові, MAD = 0 і ділити немає на що.
    # Нижня межа — десята частина типового розкиду всього ряду (MAD, а якщо
    # ряд здебільшого сталий і MAD = 0 — звичайне стандартне відхилення).
    spread = float((x - x.median()).abs().median()) or float(x.std()) or 1.0
    return (x - med) / (1.4826 * mad.clip(lower=0.1 * spread))


def main() -> None:
    ap = common.parser(__doc__, "baseline", grid=False)
    m = ap.add_argument_group("параметри методу")
    m.add_argument("--window", default="24h",
                   help="довжина рухомого вікна — скільки минулого вважати «нормою» (типово 24h)")
    m.add_argument("--threshold", type=float, default=10.0,
                   help="поріг |z|: що більший, то менше позначок (типово 10)")
    args = common.parse(ap)

    raw = common.load(args)
    x = common.make_signal(raw, args.signal)          # без сітки: кожне повідомлення
    common.intro(args, raw, x)

    z = rolling_robust_z(x, args.window)
    score = z.abs()
    flagged = score > args.threshold

    # Як читати результат: у таблиці — вікна, де |z| часто перевищував поріг;
    # «пік оцінки» — найбільший |z| у вікні (у скільки «сигм» точка відхилилась
    # від норми минулих годин). На графіку: зверху ряд і позначені точки,
    # знизу |z| і поріг. Якщо вікон забагато — підніміть --threshold або
    # --min-run; якщо реальний сплеск пропущено — зменште --threshold.
    common.report(args, x, score, flagged, limits=(args.threshold,), score_name="|z|")


if __name__ == "__main__":
    main()
