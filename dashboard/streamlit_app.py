#!/usr/bin/env python3
"""
Стадія 6 — дашборд на Streamlit.

Три сторінки (перемикаються в лівій панелі):
  1. Огляд мережі — скільки пристроїв на зв'язку, теплова карта
     «пристрій × година», зведення за зонами або шлюзами;
  2. Пристрій     — показники одного пристрою в часі, вікна знахідок
                    з findings.json зафарбовано;
  3. Аномалії     — знахідки з findings.json на шкалі часу і таблицею.

Дані читає DuckDB прямо з файла Parquet і повертає лише підсумки
(кількість за годину, середнє за крок), тому дашборд швидкий і на 7 млн
записів: увесь файл у пам'ять не завантажується.

Запуск (з кореня репозиторію, у вашому venv):
    streamlit run dashboard/streamlit_app.py
    streamlit run dashboard/streamlit_app.py -- --data variant_047.parquet \\
        --manifest variant_047.json --findings findings.json

Два дефіси «--» обов'язкові: усе, що після них, Streamlit передає цьому
скрипту. Дашборд відкриється у браузері: http://localhost:8501
Шляхи до файлів можна змінити і просто в лівій панелі.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import streamlit as st
    from streamlit import runtime
except ImportError:
    sys.exit("Не встановлено Streamlit. Виконайте у своєму venv:\n"
             "    pip install streamlit plotly duckdb pandas")

# ----------------------------------------------------------------------
# Параметри командного рядка
# ----------------------------------------------------------------------

parser = argparse.ArgumentParser(
    description="Дашборд стадії 6 (Streamlit). Запуск: "
                "streamlit run dashboard/streamlit_app.py -- --data ФАЙЛ.parquet")
parser.add_argument("--data", default="tuning.parquet",
                    help="файл телеметрії Parquet (типово tuning.parquet)")
parser.add_argument("--manifest", default="tuning.json",
                    help="маніфест JSON (типово tuning.json)")
parser.add_argument("--findings", default="findings.json",
                    help="ваші знахідки (типово findings.json)")
ARGS, _unknown = parser.parse_known_args()

if not runtime.exists():
    sys.exit("Цей файл запускається через Streamlit, а не через python:\n"
             "    streamlit run dashboard/streamlit_app.py")

try:
    import duckdb
    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError as exc:
    st.error(f"Не встановлено пакет «{exc.name}». Виконайте у своєму venv:\n\n"
             f"    pip install streamlit plotly duckdb pandas")
    st.stop()

# ----------------------------------------------------------------------
# Класи інцидентів (розділ 7 методичних вказівок) і кольори
# ----------------------------------------------------------------------

# Колір знахідки — за складністю класу: три кольори легко розрізнити,
# дванадцять — ні. Назву класу завжди підписано поруч.
CLASS_GROUP = {
    "stuck_at": "прості", "outage": "прості",
    "spike_storm": "прості", "rogue_device": "прості",
    "drift": "середні", "clock_skew": "середні",
    "beaconing": "середні", "energy_anomaly": "середні",
    "zone_incoherence": "складні", "gateway_group_loss": "складні",
    "exfil_pattern": "складні", "sensor_swap": "складні",
}
GROUP_COLOR = {"прості": "#2a78d6", "середні": "#eb6834",
               "складні": "#1baf7a", "невідомий клас": "#898781"}
INK = "#52514e"                     # колір ліній даних
# «% від звичайного»: червоне — повідомлень менше, сіре — як завжди,
# синє — більше, ніж зазвичай.
DIVERGING = [[0.0, "#a83232"], [0.25, "#e8807c"], [0.5, "#f0efec"],
             [0.75, "#6da7ec"], [1.0, "#184f95"]]
SEQUENTIAL = [[0.0, "#f0efec"], [0.5, "#5598e7"], [1.0, "#104281"]]
MAX_SCORED = 15                     # оцінюються лише 15 знахідок (п. 6.2)
NICE_STEPS = [30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400]

# ----------------------------------------------------------------------
# Запити DuckDB. read_parquet читає файл порціями і лише потрібні стовпці;
# назад повертаються сотні чи тисячі рядків підсумків, а не мільйони.
# ----------------------------------------------------------------------

SQL_SUMMARY = """
    SELECT count(*) AS rows, count(DISTINCT device_id) AS devices,
           min(ts) AS t0, max(ts) AS t1
    FROM read_parquet($path)"""

SQL_HOURLY = """
    SELECT CAST(device_id AS VARCHAR) AS device_id,
           ts // 3600 * 3600          AS hour,
           count(*)                   AS n
    FROM read_parquet($path)
    GROUP BY ALL"""

SQL_DEVICES = """
    SELECT CAST(device_id AS VARCHAR) AS device_id, count(*) AS n,
           min(ts) AS first_ts, max(ts) AS last_ts
    FROM read_parquet($path)
    GROUP BY ALL"""

SQL_SERIES = """
    SELECT CAST(metric AS VARCHAR) AS metric,
           ts // $step * $step AS t,
           avg(value) AS mean, min(value) AS lo, max(value) AS hi
    FROM read_parquet($path)
    WHERE device_id = $device AND ts >= $t0 AND ts < $t1
    GROUP BY ALL
    ORDER BY metric, t"""

SQL_METRIC_STATS = """
    SELECT metric, count(*) AS n, min(ts) AS first_ts, max(ts) AS last_ts,
           median(dt) AS median_dt
    FROM (SELECT CAST(metric AS VARCHAR) AS metric, ts,
                 ts - lag(ts) OVER (PARTITION BY metric ORDER BY ts) AS dt
          FROM read_parquet($path)
          WHERE device_id = $device)
    GROUP BY metric
    ORDER BY metric"""


def run_sql(sql: str, **params) -> pd.DataFrame:
    """Виконує запит у DuckDB. Нове з'єднання на кожен запит — так безпечно,
    коли дашборд відкрито в кількох вкладках браузера."""
    with duckdb.connect() as con:
        return con.execute(sql, params).df()


# Аргумент mtime (час зміни файла) — частина ключа кешу: якщо ви
# перезапишете файл, дашборд сам його перечитає.

@st.cache_data(show_spinner="Читаю дані…")
def load_summary(path: str, mtime: float) -> dict:
    return run_sql(SQL_SUMMARY, path=path).iloc[0].to_dict()


@st.cache_data(show_spinner="Рахую повідомлення за годинами…")
def load_hourly(path: str, mtime: float) -> pd.DataFrame:
    return run_sql(SQL_HOURLY, path=path)


@st.cache_data(show_spinner=False)
def load_device_totals(path: str, mtime: float) -> pd.DataFrame:
    return run_sql(SQL_DEVICES, path=path)


@st.cache_data(show_spinner="Читаю ряди пристрою…")
def load_series(path: str, mtime: float, device: str,
                t0: int, t1: int, step: int) -> pd.DataFrame:
    return run_sql(SQL_SERIES, path=path, device=device, t0=t0, t1=t1, step=step)


@st.cache_data(show_spinner=False)
def load_metric_stats(path: str, mtime: float, device: str) -> pd.DataFrame:
    return run_sql(SQL_METRIC_STATS, path=path, device=device)


# ----------------------------------------------------------------------
# Допоміжні функції
# ----------------------------------------------------------------------

def num(n: float) -> str:
    """1157147 -> «1 157 147»."""
    return f"{n:,.0f}".replace(",", " ")


def to_dt(epoch) -> datetime:
    """Секунди епохи -> datetime в UTC (без поясу, щоб графіки не зсувались)."""
    return datetime.fromtimestamp(int(epoch), timezone.utc).replace(tzinfo=None)


def to_epoch(moment: datetime) -> int:
    return int(moment.replace(tzinfo=timezone.utc).timestamp())


def fmt_dt(epoch) -> str:
    return "—" if pd.isna(epoch) else to_dt(epoch).strftime("%d.%m.%Y %H:%M")


def parse_time(value) -> int:
    """Час зі findings.json: секунди епохи або рядок ISO-8601."""
    if isinstance(value, (int, float)):
        return int(value)
    moment = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp())


def nice_step(span_s: int, points: int = 1500) -> int:
    """Крок усереднення, щоб на графіку було не більше ~1500 точок."""
    return next((s for s in NICE_STEPS if span_s / s <= points), NICE_STEPS[-1])


def step_label(step: int) -> str:
    if step < 60:
        return f"{step} с"
    return f"{step // 60} хв" if step < 3600 else f"{step // 3600} год"


def time_axis(fig: go.Figure, **kwargs) -> None:
    """Дати на осі часу числами (08.09, 08.09 14:00), без англійських місяців."""
    fig.update_xaxes(tickformatstops=[
        dict(dtickrange=[None, 86_400_000], value="%d.%m\n%H:%M"),
        dict(dtickrange=[86_400_000, None], value="%d.%m"),
    ], **kwargs)


def file_mtime(path: Path) -> float:
    return path.stat().st_mtime if path.is_file() else 0.0


@st.cache_data(show_spinner=False)
def load_manifest(path: str, mtime: float) -> dict | None:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


@st.cache_data(show_spinner=False)
def load_findings(path: str, mtime: float) -> tuple[pd.DataFrame | None, str]:
    """findings.json -> таблиця знахідок. Друге значення — пояснення, якщо
    файла немає або його не вдалося прочитати."""
    file = Path(path)
    if not file.is_file():
        return None, (f"Файл знахідок «{path}» не знайдено, тому шкала порожня. "
                      f"Цей файл ви створите на стадії 5 (make_findings.py). Щоб "
                      f"подивитися, як це виглядатиме, вкажіть у лівій панелі "
                      f"examples/findings.example.json.")
    try:
        doc = json.loads(file.read_text(encoding="utf-8"))
        rows = []
        for i, f in enumerate(doc.get("findings") or [], start=1):
            devices = f.get("devices") or []
            devices = [devices] if isinstance(devices, str) else [str(d) for d in devices]
            cls = str(f.get("class", "?"))
            rows.append({
                "no": i, "cls": cls, "group": CLASS_GROUP.get(cls, "невідомий клас"),
                "devices": devices,
                "start": parse_time(f["start"]), "end": parse_time(f["end"]),
                "confidence": float(f.get("confidence") or 0.0),
                "evidence": str(f.get("evidence", "")),
            })
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        return None, (f"Не вдалося прочитати {path}: {exc}. Перевірте файл командою:  "
                      f"python selfcheck.py --findings {path}")
    table = pd.DataFrame(rows, columns=["no", "cls", "group", "devices", "start",
                                        "end", "confidence", "evidence"])
    # Оцінюються 15 знахідок з найвищою впевненістю (за рівних — ті, що вище у файлі).
    ranked = table.sort_values(["confidence", "no"], ascending=[False, True])
    table["scored"] = table["no"].isin(ranked["no"].head(MAX_SCORED))
    return table, ""


def build_inventory(manifest: dict | None, totals: pd.DataFrame) -> pd.DataFrame:
    """Пристрої з маніфесту + ті, що є лише в даних, з їхніми лічильниками."""
    listed = pd.DataFrame((manifest or {}).get("devices", []),
                          columns=["device_id", "type", "zone", "gateway", "interval_s"])
    listed["in_manifest"] = True
    inv = listed.merge(totals, on="device_id", how="outer")
    inv["in_manifest"] = inv["in_manifest"].eq(True)
    inv[["zone", "gateway"]] = inv[["zone", "gateway"]].fillna("немає в маніфесті")
    inv["type"] = inv["type"].fillna("?")
    inv["n"] = inv["n"].fillna(0).astype(int)
    return inv.sort_values(["zone", "device_id"]).reset_index(drop=True)


def hourly_grid(hourly: pd.DataFrame, keys: list[str], t0: int, t1: int) -> pd.DataFrame:
    """Таблиця «рядок × година» з нулями там, де повідомлень не було."""
    hours = list(range(t0 // 3600 * 3600, t1 // 3600 * 3600 + 3600, 3600))
    grid = hourly.pivot_table(index="key", columns="hour", values="n",
                              aggfunc="sum", fill_value=0)
    return grid.reindex(index=keys, columns=hours, fill_value=0)


def percent_of_usual(grid: pd.DataFrame) -> pd.DataFrame:
    """Кількість за годину у % від звичайної для цього рядка
    (медіани тих годин, коли повідомлення були)."""
    usual = grid.where(grid > 0).median(axis=1)
    return grid.div(usual, axis=0).mul(100).fillna(0)


def heatmap(grid: pd.DataFrame, relative: bool, row_height: int = 14) -> go.Figure:
    pct = percent_of_usual(grid)
    fig = go.Figure(go.Heatmap(
        z=(pct.clip(upper=200) if relative else grid).values,
        x=[to_dt(h) for h in grid.columns], y=list(grid.index),
        text=grid.values, customdata=pct.round(0).values, ygap=1,
        colorscale=DIVERGING if relative else SEQUENTIAL,
        zmin=0, zmax=200 if relative else max(1, int(grid.values.max())),
        colorbar=dict(title="% від<br>звичайного" if relative else "повідомлень<br>за годину",
                      thickness=12),
        hovertemplate="%{y}<br>%{x|%d.%m.%Y %H:00} UTC<br>"
                      "%{text} повідомлень (%{customdata:.0f}% від звичайного)"
                      "<extra></extra>"))
    fig.update_yaxes(autorange="reversed", tickfont=dict(size=10))
    time_axis(fig)
    fig.update_layout(height=90 + row_height * len(grid.index),
                      margin=dict(l=10, r=10, t=10, b=10))
    return fig


# ----------------------------------------------------------------------
# Бічна панель: файли (спільні для всіх сторінок)
# ----------------------------------------------------------------------

st.set_page_config(page_title="IoT-дашборд", layout="wide")

# Streamlit забуває стан віджета, коли ви переходите на іншу сторінку.
# Цей рядок зберігає обраний пристрій і період між сторінками.
for _key in ("device", "range"):
    if _key in st.session_state:
        st.session_state[_key] = st.session_state[_key]

with st.sidebar:
    st.markdown("**Файли**")
    data_path = Path(st.text_input("Дані (Parquet)", ARGS.data,
                                   help="tuning.parquet або ваш variant_NNN.parquet"))
    manifest_path = Path(st.text_input("Маніфест (JSON)", ARGS.manifest))
    findings_path = Path(st.text_input("Знахідки (findings.json)", ARGS.findings))
    st.caption(f"Відносні шляхи рахуються від теки, з якої запущено команду: "
               f"{Path.cwd()}")
    st.caption("Увесь час на графіках — UTC.")

if not data_path.is_file():
    st.error(f"Не знайдено файл з даними: {data_path.resolve()}\n\n"
             f"Покладіть tuning.parquet (навчальний набір) або свій variant_NNN.parquet "
             f"у теку, з якої запускаєте команду, або вкажіть повний шлях у лівій "
             f"панелі (чи параметром --data).")
    st.stop()

DATA, MTIME = str(data_path), file_mtime(data_path)
try:
    SUMMARY = load_summary(DATA, MTIME)
except duckdb.Error as exc:
    st.error(f"DuckDB не зміг прочитати {data_path}. Це точно файл Parquet з "
             f"телеметрією (стовпці ts, device_id, metric, value)?\n\nДеталі: {exc}")
    st.stop()

MANIFEST = load_manifest(str(manifest_path), file_mtime(manifest_path))
if MANIFEST is None:
    st.sidebar.warning(f"Маніфест «{manifest_path}» не знайдено або він пошкоджений: "
                       f"зони, шлюзи та одиниці вимірювання не показуються.")
UNITS = {m: v.get("unit", "") for m, v in (MANIFEST or {}).get("metrics", {}).items()}
T0, T1 = int(SUMMARY["t0"]), int(SUMMARY["t1"])
INVENTORY = build_inventory(MANIFEST, load_device_totals(DATA, MTIME))
FINDINGS, FINDINGS_MSG = load_findings(str(findings_path), file_mtime(findings_path))


# ----------------------------------------------------------------------
# Сторінка 1. Огляд мережі
# ----------------------------------------------------------------------

def page_overview() -> None:
    st.title("Огляд мережі")
    inv = INVENTORY
    online = inv["last_ts"].fillna(0) >= T1 - 3600

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Записів", num(SUMMARY["rows"]))
    c2.metric("Пристроїв у даних", int(SUMMARY["devices"]),
              help=f"У маніфесті зареєстровано {int(inv['in_manifest'].sum())}.")
    c3.metric("На зв'язку наприкінці", f"{int(online.sum())} з {len(inv)}",
              help=f"Надсилали дані протягом останньої години набору "
                   f"(до {fmt_dt(T1)} UTC).")
    c4.metric("Період", f"{(T1 - T0) / 86400:.0f} діб",
              help=f"{fmt_dt(T0)} – {fmt_dt(T1)} UTC")

    st.subheader("Повідомлення від кожного пристрою за кожну годину")
    relative = st.radio(
        "Показати", ["% від звичайного для пристрою", "кількість повідомлень"],
        horizontal=True, label_visibility="collapsed") != "кількість повідомлень"
    st.caption("Сірий — як зазвичай, червоний — повідомлень менше або немає зовсім, "
               "синій — більше, ніж зазвичай. «Звичайне» — медіана тих годин, коли "
               "пристрій надсилав дані. Наведіть курсор на клітинку, щоб побачити числа.")
    hourly = load_hourly(DATA, MTIME)
    label = {r.device_id: f"{r.device_id} · {r.zone}" for r in inv.itertuples()}
    grid = hourly_grid(hourly.assign(key=hourly["device_id"].map(label)),
                       [label[d] for d in inv["device_id"]], T0, T1)
    st.plotly_chart(heatmap(grid, relative), key="hm_devices")

    st.subheader("Зведення за групами")
    by = st.radio("Групувати за", ["zone", "gateway"], horizontal=True,
                  format_func={"zone": "зонами", "gateway": "шлюзами"}.get)
    group_of = dict(zip(inv["device_id"], inv[by]))
    group_grid = hourly_grid(hourly.assign(key=hourly["device_id"].map(group_of)),
                             sorted(inv[by].unique()), T0, T1)
    st.plotly_chart(heatmap(group_grid, relative, row_height=26), key="hm_groups")

    # Доступність — частка годин, коли пристрій надсилав хоч щось.
    inv = inv.assign(availability=(grid.values > 0).mean(axis=1) * 100, online=online)
    summary = inv.groupby(by).agg(
        manifest=("in_manifest", "sum"), observed=("n", lambda s: int((s > 0).sum())),
        messages=("n", "sum"), availability=("availability", "mean"),
        last=("last_ts", "max")).reset_index()
    summary["last"] = summary["last"].map(fmt_dt)
    st.dataframe(summary, hide_index=True, column_config={
        by: "Зона" if by == "zone" else "Шлюз",
        "manifest": "Пристроїв у маніфесті", "observed": "Пристроїв у даних",
        "messages": st.column_config.NumberColumn("Повідомлень", format="%d"),
        "availability": st.column_config.NumberColumn(
            "Доступність, %", format="%.1f",
            help="Середня частка годин, коли пристрої групи надсилали дані"),
        "last": "Останнє (UTC)"})

    with st.expander("Усі пристрої"):
        table = inv.assign(first=inv["first_ts"].map(fmt_dt), last=inv["last_ts"].map(fmt_dt))
        st.dataframe(table[["device_id", "type", "zone", "gateway", "in_manifest", "n",
                            "first", "last", "availability", "online"]],
                     hide_index=True, column_config={
                         "device_id": "Пристрій", "type": "Тип", "zone": "Зона",
                         "gateway": "Шлюз", "in_manifest": "У маніфесті",
                         "n": st.column_config.NumberColumn("Повідомлень", format="%d"),
                         "first": "Перше (UTC)", "last": "Останнє (UTC)",
                         "availability": st.column_config.NumberColumn(
                             "Доступність, %", format="%.1f"),
                         "online": "На зв'язку наприкінці"})


# ----------------------------------------------------------------------
# Сторінка 2. Пристрій
# ----------------------------------------------------------------------

def finding_window(f, full: tuple[datetime, datetime]) -> tuple[datetime, datetime]:
    """Період навколо знахідки: сама знахідка плюс по половині її тривалості
    (щонайменше година) з обох боків — щоб було видно «до» і «після»."""
    pad = max(3600, (f.end - f.start) // 2)
    return max(full[0], to_dt(f.start - pad)), min(full[1], to_dt(f.end + pad))


def page_device() -> None:
    st.title("Пристрій")
    inv = INVENTORY.set_index("device_id")
    devices = list(inv.index)
    full = (to_dt(T0), to_dt(T1 // 3600 * 3600 + 3600))

    # Посилання з таблиці аномалій відкриває цю сторінку як ?device=…&finding=…
    wanted = st.query_params.get("device")
    if st.session_state.get("device") not in devices:
        st.session_state["device"] = wanted if wanted in devices else devices[0]
    rng = st.session_state.get("range")
    if not (isinstance(rng, tuple) and full[0] <= rng[0] < rng[1] <= full[1]):
        st.session_state["range"] = full

    own = pd.DataFrame(columns=["no", "cls", "group", "start", "end"])
    if FINDINGS is not None:
        own = FINDINGS[FINDINGS["devices"].map(lambda ds: st.session_state["device"] in ds)]
    link = (wanted, st.query_params.get("finding"))
    if link[1] and st.session_state.get("link_used") != link:
        st.session_state["link_used"] = link        # застосовуємо посилання один раз
        match = own[own["no"].astype(str) == link[1]]
        if not match.empty:
            st.session_state["range"] = finding_window(match.iloc[0], full)
            st.session_state[f"zoom_{wanted}"] = int(match.iloc[0]["no"])

    def zoom(key: str) -> None:
        no = st.session_state[key]
        st.session_state["range"] = (full if no is None else
                                     finding_window(own[own["no"] == no].iloc[0], full))

    titles = {f.no: f"№{f.no} {f.cls} · {to_dt(f.start):%d.%m %H:%M}"
              for f in own.itertuples()}
    col1, col2, col3 = st.columns([2, 2, 3])
    device = col1.selectbox(
        "Пристрій", devices, key="device",
        format_func=lambda d: f"{d} · {inv.at[d, 'zone']} · {inv.at[d, 'gateway']}")
    # Окремий вибір знахідки для кожного пристрою (у кожного свої знахідки).
    col2.selectbox("Наблизити до знахідки", [None] + list(titles), key=f"zoom_{device}",
                   on_change=zoom, args=(f"zoom_{device}",),
                   format_func=lambda no: titles.get(no, "увесь період"))
    col3.slider("Період (UTC)", min_value=full[0], max_value=full[1],
                step=timedelta(hours=1), format="DD.MM.YYYY HH:mm", key="range")
    st.query_params["device"] = device
    start, end = st.session_state["range"]
    t0, t1 = to_epoch(start), to_epoch(end)

    row = inv.loc[device]
    st.markdown(f"Тип **{row['type']}**, зона **{row['zone']}**, шлюз **{row['gateway']}**"
                + (f", інтервал за маніфестом **{int(row['interval_s'])} с**"
                   if pd.notna(row["interval_s"]) else "")
                + ("" if row["in_manifest"] else ". **Цього пристрою немає в маніфесті.**"))

    step = nice_step(t1 - t0)
    series = load_series(DATA, MTIME, device, t0, t1, step)
    if series.empty:
        st.warning("За обраний період від цього пристрою немає жодного запису.")
        return
    metrics = sorted(series["metric"].unique())
    fig = make_subplots(rows=len(metrics), cols=1, shared_xaxes=True,
                        vertical_spacing=0.08 / max(1, len(metrics) - 1),
                        subplot_titles=[f"{m}, {UNITS[m]}" if UNITS.get(m) else m
                                        for m in metrics])
    for i, metric in enumerate(metrics, start=1):
        s = series[series["metric"] == metric]
        t = s["t"].map(to_dt)
        # Смуга мінімум…максимум за крок: так видно і викиди, і «застиглі» значення.
        fig.add_trace(go.Scatter(x=t, y=s["hi"], mode="lines", line=dict(width=0),
                                 hoverinfo="skip", showlegend=False), row=i, col=1)
        fig.add_trace(go.Scatter(x=t, y=s["lo"], mode="lines", line=dict(width=0),
                                 fill="tonexty", fillcolor="rgba(82,81,78,0.18)",
                                 hoverinfo="skip", showlegend=False), row=i, col=1)
        fig.add_trace(go.Scatter(
            x=t, y=s["mean"], mode="lines", line=dict(color=INK, width=2),
            customdata=s[["lo", "hi"]].values, showlegend=False, name=metric,
            hovertemplate="%{x|%d.%m.%Y %H:%M}<br>середнє %{y:.2f}"
                          "<br>мін %{customdata[0]:.2f}, макс %{customdata[1]:.2f}"
                          "<extra></extra>"), row=i, col=1)
    for f in own.itertuples():
        fig.add_vrect(x0=to_dt(f.start), x1=to_dt(f.end), fillcolor=GROUP_COLOR[f.group],
                      opacity=0.18, line_width=0, row="all", col=1)
        fig.add_annotation(x=to_dt(max(f.start, t0)), y=1, xref="x", yref="y domain",
                           text=f"№{f.no} {f.cls}", showarrow=False, xanchor="left",
                           yanchor="top", font=dict(size=11, color="#0b0b0b"),
                           bgcolor="rgba(255,255,255,0.75)")
    time_axis(fig, range=[start, end])
    fig.update_layout(height=80 + 210 * len(metrics), margin=dict(l=10, r=10, t=30, b=10),
                      hovermode="x unified")
    st.caption(f"Лінія — середнє за {step_label(step)}, сіра смуга — від мінімуму до "
               f"максимуму за той самий крок. Кольорові вікна — знахідки з findings.json "
               f"(синій — прості класи, помаранчевий — середні, зелений — складні). "
               f"Виділіть ділянку мишею, щоб збільшити; подвійний клік повертає назад.")
    st.plotly_chart(fig, key="device_chart")

    stats = load_metric_stats(DATA, MTIME, device)
    stats = stats.assign(unit=stats["metric"].map(UNITS).fillna(""),
                         first=stats["first_ts"].map(fmt_dt),
                         last=stats["last_ts"].map(fmt_dt))
    st.dataframe(stats[["metric", "unit", "n", "first", "last", "median_dt"]],
                 hide_index=True, column_config={
                     "metric": "Показник", "unit": "Одиниця",
                     "n": st.column_config.NumberColumn("Записів за весь період", format="%d"),
                     "first": "Перший (UTC)", "last": "Останній (UTC)",
                     "median_dt": st.column_config.NumberColumn(
                         "Інтервал між записами, с (медіана)", format="%.0f")})


# ----------------------------------------------------------------------
# Сторінка 3. Аномалії
# ----------------------------------------------------------------------

def finding_label(r) -> str:
    """Підпис рядка шкали: номер, клас і до двох пристроїв."""
    more = f" +{len(r.devices) - 2}" if len(r.devices) > 2 else ""
    return f"№{r.no} {r.cls} · {', '.join(r.devices[:2])}{more}"


def timeline(table: pd.DataFrame) -> go.Figure:
    """Знахідки як відрізки на шкалі часу: один рядок — одна знахідка,
    згори вниз у порядку початку."""
    fig = go.Figure()
    table = table.sort_values("start")
    for group, color in GROUP_COLOR.items():
        part = table[table["group"] == group]
        if part.empty:
            continue
        labels = [finding_label(r) for r in part.itertuples()]
        hover = [f"<b>№{r.no} {r.cls}</b><br>{', '.join(r.devices)}<br>"
                 f"{fmt_dt(r.start)} – {fmt_dt(r.end)} UTC<br>"
                 f"впевненість {r.confidence:.2f}" for r in part.itertuples()]
        fig.add_trace(go.Bar(
            base=[to_dt(s) for s in part["start"]],
            x=[(e - s) * 1000 for s, e in zip(part["start"], part["end"])],
            y=labels, orientation="h", marker_color=color, width=0.6,
            name=f"класи: {group}", hovertext=hover, hoverinfo="text"))
        # Ромб посередині — щоб і коротку знахідку було видно на шкалі в 60 діб.
        fig.add_trace(go.Scatter(
            x=[to_dt((s + e) // 2) for s, e in zip(part["start"], part["end"])],
            y=labels, mode="markers", showlegend=False, hovertext=hover,
            hoverinfo="text", marker=dict(symbol="diamond", size=10, color=color,
                                          line=dict(color="white", width=2))))
    fig.update_xaxes(type="date", range=[to_dt(T0), to_dt(T1)])
    time_axis(fig)
    fig.update_yaxes(autorange="reversed", type="category", categoryorder="array",
                     categoryarray=[finding_label(r) for r in table.itertuples()])
    fig.update_layout(height=150 + 32 * max(len(table), 1), barmode="overlay",
                      margin=dict(l=10, r=10, t=40, b=10),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0))
    return fig


def page_anomalies() -> None:
    st.title("Аномалії")
    if FINDINGS is None:
        st.info(FINDINGS_MSG)
        st.plotly_chart(timeline(pd.DataFrame(columns=["no", "cls", "group", "devices",
                                                       "start", "end", "confidence"])),
                        key="timeline_empty")
        return

    table = FINDINGS
    c1, c2, c3 = st.columns(3)
    c1.metric("Знахідок у файлі", len(table))
    c2.metric("Різних класів", table["cls"].nunique())
    c3.metric("Буде оцінено", int(table["scored"].sum()),
              help="Оцінюються не більше 15 знахідок з найвищою впевненістю (п. 6.2).")
    unknown = sorted(set(table["cls"]) - set(CLASS_GROUP))
    if unknown:
        st.warning(f"Невідомі назви класів: {', '.join(unknown)}. Назва має точно "
                   f"збігатися з розділом 7 методичних вказівок.")

    classes = sorted(table["cls"].unique())
    chosen = st.multiselect("Класи", classes, default=classes)
    table = table[table["cls"].isin(chosen)]
    if table.empty:
        return
    st.plotly_chart(timeline(table), key="timeline")

    view = table.assign(
        devices_text=table["devices"].map(", ".join),
        start_text=table["start"].map(fmt_dt), end_text=table["end"].map(fmt_dt),
        hours=(table["end"] - table["start"]) / 3600,
        link=[f"device?device={ds[0]}&finding={no}" if ds else None
              for ds, no in zip(table["devices"], table["no"])],
    ).sort_values("start")
    st.dataframe(view[["no", "cls", "group", "devices_text", "start_text", "end_text",
                       "hours", "confidence", "scored", "link", "evidence"]],
                 hide_index=True, column_config={
                     "no": "№", "cls": "Клас", "group": "Складність",
                     "devices_text": "Пристрої",
                     "start_text": "Початок (UTC)", "end_text": "Кінець (UTC)",
                     "hours": st.column_config.NumberColumn("Тривалість, год", format="%.1f"),
                     "confidence": st.column_config.NumberColumn("Впевненість", format="%.2f"),
                     "scored": "Буде оцінено",
                     "link": st.column_config.LinkColumn("Графік", display_text="відкрити"),
                     "evidence": "Обґрунтування"})


pages = st.navigation([
    st.Page(page_overview, title="Огляд мережі", icon=":material/lan:", default=True),
    st.Page(page_device, title="Пристрій", icon=":material/sensors:", url_path="device"),
    st.Page(page_anomalies, title="Аномалії", icon=":material/timeline:",
            url_path="anomalies"),
])
pages.run()
