#!/usr/bin/env python3
"""
Стадія 6 — дашборд на Plotly Dash.

Три сторінки (посилання вгорі):
  1. Огляд мережі — скільки пристроїв на зв'язку, теплова карта
     «пристрій × година», зведення за зонами або шлюзами;
  2. Пристрій     — показники одного пристрою в часі, вікна знахідок
                    з findings.json зафарбовано;
  3. Аномалії     — знахідки з findings.json на шкалі часу і таблицею.

Дані читає DuckDB прямо з файла Parquet і повертає лише підсумки
(кількість за годину, середнє за крок), тому дашборд швидкий і на 7 млн
записів: увесь файл у пам'ять не завантажується.

Запуск (з кореня репозиторію, у вашому venv):
    python dashboard/dash_app.py
    python dashboard/dash_app.py --data variant_047.parquet \\
        --manifest variant_047.json --findings findings.json

Потім відкрийте у браузері http://localhost:8050 . Зупинка — Ctrl+C.
Файл findings.json перечитується сам: оновіть сторінку після змін.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs

try:
    import dash
    import duckdb
    import pandas as pd
    import plotly.graph_objects as go
    from dash import Dash, Input, Output, State, dcc, html
    from plotly.subplots import make_subplots
except ImportError as exc:
    sys.exit(f"Не встановлено пакет «{exc.name}». Виконайте у своєму venv:\n"
             f"    pip install dash plotly duckdb pandas")
if int(dash.__version__.split(".")[0]) < 4:
    sys.exit(f"Потрібен Dash 4 або новіший (у вас {dash.__version__}). Виконайте:\n"
             f"    pip install -U dash")

# ----------------------------------------------------------------------
# Параметри командного рядка
# ----------------------------------------------------------------------

parser = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--data", default="tuning.parquet",
                    help="файл телеметрії Parquet (типово tuning.parquet)")
parser.add_argument("--manifest", default="tuning.json",
                    help="маніфест JSON (типово tuning.json)")
parser.add_argument("--findings", default="findings.json",
                    help="ваші знахідки (типово findings.json)")
parser.add_argument("--port", type=int, default=8050, help="порт (типово 8050)")
ARGS = parser.parse_args()

DATA = Path(ARGS.data)
if not DATA.is_file():
    sys.exit(f"Не знайдено файл з даними: {DATA.resolve()}\n"
             f"Покладіть tuning.parquet (навчальний набір) або свій variant_NNN.parquet "
             f"у поточну теку чи вкажіть шлях через --data.")

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
# Підписи всередині випадних списків (потрібен Dash 4 або новіший).
DROPDOWN_LABELS = {"search": "Пошук", "clear_search": "Очистити пошук",
                   "no_options_found": "Нічого не знайдено", "select_all": "Обрати всі",
                   "deselect_all": "Зняти всі", "selected_count": "обрано: {num_selected}",
                   "clear_selection": "Очистити"}

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


def mtime(path: Path) -> float:
    """Час зміни файла — частина ключа кешу: змінений файл перечитається."""
    return path.stat().st_mtime if path.is_file() else 0.0


@lru_cache(maxsize=4)
def load_summary(path: str, mtime: float) -> dict:
    return run_sql(SQL_SUMMARY, path=path).iloc[0].to_dict()


@lru_cache(maxsize=4)
def load_hourly(path: str, mtime: float) -> pd.DataFrame:
    return run_sql(SQL_HOURLY, path=path)


@lru_cache(maxsize=4)
def load_device_totals(path: str, mtime: float) -> pd.DataFrame:
    return run_sql(SQL_DEVICES, path=path)


@lru_cache(maxsize=64)
def load_series(path: str, mtime: float, device: str,
                t0: int, t1: int, step: int) -> pd.DataFrame:
    return run_sql(SQL_SERIES, path=path, device=device, t0=t0, t1=t1, step=step)


@lru_cache(maxsize=64)
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


def load_manifest() -> dict | None:
    try:
        return json.loads(Path(ARGS.manifest).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


@lru_cache(maxsize=4)
def load_findings(path: str, mtime: float) -> tuple[pd.DataFrame | None, str]:
    """findings.json -> таблиця знахідок. Друге значення — пояснення, якщо
    файла немає або його не вдалося прочитати."""
    file = Path(path)
    if not file.is_file():
        return None, (f"Файл знахідок «{path}» не знайдено, тому шкала порожня. "
                      f"Цей файл ви створите на стадії 5 (make_findings.py). Щоб "
                      f"подивитися, як це виглядатиме, перезапустіть дашборд з "
                      f"--findings examples/findings.example.json")
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


def findings() -> tuple[pd.DataFrame | None, str]:
    path = Path(ARGS.findings)
    return load_findings(str(path), mtime(path))


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


def table(df: pd.DataFrame, columns: dict[str, str]) -> html.Table:
    """Проста HTML-таблиця. columns — {назва стовпця: заголовок}."""
    head = html.Thead(html.Tr([html.Th(title) for title in columns.values()]))
    body = html.Tbody([html.Tr([html.Td(row[c]) for c in columns])
                       for _, row in df.iterrows()])
    return html.Table([head, body], className="tbl")


def tile(label: str, value, note: str = "") -> html.Div:
    return html.Div([html.Div(label, className="tile-label"),
                     html.Div(value, className="tile-value"),
                     html.Div(note, className="tile-note")], className="tile")


# ----------------------------------------------------------------------
# Дані, спільні для всіх сторінок
# ----------------------------------------------------------------------

try:
    SUMMARY = load_summary(str(DATA), mtime(DATA))
except duckdb.Error as exc:
    sys.exit(f"DuckDB не зміг прочитати {DATA}. Це точно файл Parquet з телеметрією "
             f"(стовпці ts, device_id, metric, value)?\nДеталі: {exc}")
MANIFEST = load_manifest()
if MANIFEST is None:
    print(f"Увага: маніфест «{ARGS.manifest}» не знайдено або він пошкоджений — "
          f"зони, шлюзи та одиниці вимірювання не показуватимуться.")
UNITS = {m: v.get("unit", "") for m, v in (MANIFEST or {}).get("metrics", {}).items()}
T0, T1 = int(SUMMARY["t0"]), int(SUMMARY["t1"])
INVENTORY = build_inventory(MANIFEST, load_device_totals(str(DATA), mtime(DATA)))


# ----------------------------------------------------------------------
# Сторінка 1. Огляд мережі
# ----------------------------------------------------------------------

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
    fig.update_layout(height=90 + row_height * len(grid.index), template="plotly_white",
                      margin=dict(l=10, r=10, t=10, b=10))
    return fig


def overview_layout() -> html.Div:
    inv = INVENTORY
    online = int((inv["last_ts"].fillna(0) >= T1 - 3600).sum())
    return html.Div([
        html.H1("Огляд мережі"),
        html.Div([
            tile("Записів", num(SUMMARY["rows"])),
            tile("Пристроїв у даних", int(SUMMARY["devices"]),
                 f"у маніфесті {int(inv['in_manifest'].sum())}"),
            tile("На зв'язку наприкінці", f"{online} з {len(inv)}",
                 f"дані за останню годину (до {fmt_dt(T1)} UTC)"),
            tile("Період", f"{(T1 - T0) / 86400:.0f} діб",
                 f"{fmt_dt(T0)} – {fmt_dt(T1)} UTC"),
        ], className="tiles"),
        html.H2("Повідомлення від кожного пристрою за кожну годину"),
        dcc.RadioItems(id="mode", value="pct", inline=True, className="radio", options=[
            {"label": "% від звичайного для пристрою", "value": "pct"},
            {"label": "кількість повідомлень", "value": "count"}]),
        html.P("Сірий — як зазвичай, червоний — повідомлень менше або немає зовсім, "
               "синій — більше, ніж зазвичай. «Звичайне» — медіана тих годин, коли "
               "пристрій надсилав дані. Наведіть курсор на клітинку, щоб побачити числа.",
               className="note"),
        dcc.Loading(dcc.Graph(id="hm-devices")),
        html.H2("Зведення за групами"),
        dcc.RadioItems(id="group-by", value="zone", inline=True, className="radio", options=[
            {"label": "за зонами", "value": "zone"},
            {"label": "за шлюзами", "value": "gateway"}]),
        dcc.Loading(dcc.Graph(id="hm-groups")),
        html.Div(id="group-table"),
        html.Details([html.Summary("Усі пристрої"), html.Div(id="device-table")]),
    ])


# ----------------------------------------------------------------------
# Сторінка 2. Пристрій
# ----------------------------------------------------------------------

def period_options(device: str) -> list[dict]:
    """Варіанти для списку «Період»: увесь набір, кожна знахідка цього
    пристрою, кожна доба."""
    options = [{"label": "увесь період", "value": "all"}]
    table_, _ = findings()
    if table_ is not None:
        for f in table_[table_["devices"].map(lambda ds: device in ds)].itertuples():
            options.append({"label": f"знахідка №{f.no} {f.cls} · {to_dt(f.start):%d.%m %H:%M}",
                            "value": f"f{f.no}"})
    for day in range(T0 // 86400 * 86400, T1, 86400):
        options.append({"label": f"доба {to_dt(day):%d.%m.%Y}", "value": f"d{day}"})
    return options


def period_bounds(period: str) -> tuple[int, int]:
    """Значення зі списку «Період» -> (початок, кінець) у секундах епохи."""
    end_all = T1 // 3600 * 3600 + 3600
    if period.startswith("d"):
        return int(period[1:]), int(period[1:]) + 86400
    table_, _ = findings()
    if period.startswith("f") and table_ is not None:
        match = table_[table_["no"] == int(period[1:])]
        if not match.empty:
            f = match.iloc[0]
            start, end = int(f.start), int(f.end)
            pad = max(3600, (end - start) // 2)       # трохи «до» і «після»
            return max(T0, start - pad), min(end_all, end + pad)
    return T0, end_all


def device_layout(params: dict) -> html.Div:
    devices = list(INVENTORY["device_id"])
    device = params.get("device") if params.get("device") in devices else devices[0]
    options = period_options(device)
    wanted = f"f{params['finding']}" if "finding" in params else "all"
    period = wanted if wanted in [o["value"] for o in options] else "all"
    zones = dict(zip(INVENTORY["device_id"], INVENTORY["zone"] + " · " + INVENTORY["gateway"]))
    return html.Div([
        html.H1("Пристрій"),
        html.Div([
            html.Div([html.Label("Пристрій"), dcc.Dropdown(
                id="device", value=device, clearable=False, labels=DROPDOWN_LABELS,
                options=[{"label": f"{d} · {zones[d]}", "value": d} for d in devices])],
                className="control"),
            html.Div([html.Label("Період"), dcc.Dropdown(
                id="period", value=period, clearable=False, options=options,
                labels=DROPDOWN_LABELS)],
                className="control"),
        ], className="controls"),
        html.P(id="device-info"),
        html.P(id="device-note", className="note"),
        dcc.Loading(dcc.Graph(id="device-chart")),
        html.Div(id="metric-table"),
    ])


def device_figure(device: str, t0: int, t1: int) -> tuple[go.Figure, str]:
    step = nice_step(t1 - t0)
    series = load_series(str(DATA), mtime(DATA), device, t0, t1, step)
    metrics = sorted(series["metric"].unique())
    fig = make_subplots(rows=max(1, len(metrics)), cols=1, shared_xaxes=True,
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
    table_, _ = findings()
    if table_ is not None and metrics:
        for f in table_[table_["devices"].map(lambda ds: device in ds)].itertuples():
            fig.add_vrect(x0=to_dt(f.start), x1=to_dt(f.end), fillcolor=GROUP_COLOR[f.group],
                          opacity=0.18, line_width=0, row="all", col=1)
            fig.add_annotation(x=to_dt(max(f.start, t0)), y=1, xref="x", yref="y domain",
                               text=f"№{f.no} {f.cls}", showarrow=False, xanchor="left",
                               yanchor="top", font=dict(size=11, color="#0b0b0b"),
                               bgcolor="rgba(255,255,255,0.75)")
    time_axis(fig, range=[to_dt(t0), to_dt(t1)])
    fig.update_layout(height=80 + 210 * max(1, len(metrics)), template="plotly_white",
                      margin=dict(l=10, r=10, t=30, b=10), hovermode="x unified")
    note = (f"Лінія — середнє за {step_label(step)}, сіра смуга — від мінімуму до максимуму "
            f"за той самий крок. Кольорові вікна — знахідки з findings.json (синій — прості "
            f"класи, помаранчевий — середні, зелений — складні). Виділіть ділянку мишею, "
            f"щоб збільшити; подвійний клік повертає назад.")
    if not metrics:
        note = "За обраний період від цього пристрою немає жодного запису."
    return fig, note


# ----------------------------------------------------------------------
# Сторінка 3. Аномалії
# ----------------------------------------------------------------------

def finding_label(r) -> str:
    """Підпис рядка шкали: номер, клас і до двох пристроїв."""
    more = f" +{len(r.devices) - 2}" if len(r.devices) > 2 else ""
    return f"№{r.no} {r.cls} · {', '.join(r.devices[:2])}{more}"


def timeline(table_: pd.DataFrame) -> go.Figure:
    """Знахідки як відрізки на шкалі часу: один рядок — одна знахідка,
    згори вниз у порядку початку."""
    fig = go.Figure()
    table_ = table_.sort_values("start")
    for group, color in GROUP_COLOR.items():
        part = table_[table_["group"] == group]
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
                     categoryarray=[finding_label(r) for r in table_.itertuples()])
    fig.update_layout(height=150 + 32 * max(len(table_), 1), barmode="overlay",
                      template="plotly_white", margin=dict(l=10, r=10, t=40, b=10),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0))
    return fig


def anomalies_layout() -> html.Div:
    table_, message = findings()
    if table_ is None:
        empty = pd.DataFrame(columns=["no", "cls", "group", "devices", "start", "end",
                                      "confidence"])
        return html.Div([html.H1("Аномалії"), html.P(message, className="info"),
                         dcc.Graph(figure=timeline(empty))])
    classes = sorted(table_["cls"].unique())
    unknown = sorted(set(classes) - set(CLASS_GROUP))
    return html.Div([
        html.H1("Аномалії"),
        html.Div([
            tile("Знахідок у файлі", len(table_)),
            tile("Різних класів", len(classes)),
            tile("Буде оцінено", int(table_["scored"].sum()),
                 "не більше 15 з найвищою впевненістю (п. 6.2)"),
        ], className="tiles"),
        html.P(f"Невідомі назви класів: {', '.join(unknown)}. Назва має точно збігатися "
               f"з розділом 7 методичних вказівок.", className="info") if unknown else None,
        html.Label("Класи"),
        dcc.Checklist(id="classes", options=classes, value=classes, inline=True,
                      className="radio"),
        dcc.Graph(id="timeline"),
        html.Div(id="findings-table"),
    ])


# ----------------------------------------------------------------------
# Застосунок: вигляд, навігація і реакції на дії користувача (callbacks)
# ----------------------------------------------------------------------

app = Dash(__name__, title="IoT-дашборд", suppress_callback_exceptions=True)
app.index_string = """<!DOCTYPE html>
<html><head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
  body { margin: 0; background: #f9f9f7; color: #0b0b0b;
         font-family: system-ui, -apple-system, "Segoe UI", sans-serif; font-size: 15px; }
  .page { max-width: 1400px; margin: 0 auto; padding: 0 24px 40px; }
  .top { display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap;
         padding: 14px 0 10px; border-bottom: 1px solid #e1e0d9; }
  .top b { font-size: 18px; margin-right: 18px; }
  .top a { padding: 6px 12px; border-radius: 6px; color: #52514e; text-decoration: none; }
  .top a.active { background: #e3eefc; color: #104281; font-weight: 600; }
  .files { color: #898781; font-size: 13px; margin-left: auto; }
  h1 { font-size: 28px; margin: 18px 0 8px; } h2 { font-size: 20px; margin: 26px 0 6px; }
  .tiles { display: flex; gap: 14px; flex-wrap: wrap; }
  .tile { background: #fcfcfb; border: 1px solid #e1e0d9; border-radius: 8px;
          padding: 10px 16px; min-width: 180px; }
  .tile-label { color: #52514e; font-size: 13px; } .tile-value { font-size: 28px; font-weight: 600; }
  .tile-note { color: #898781; font-size: 12px; }
  .note { color: #52514e; font-size: 13px; }
  .info { background: #e3eefc; color: #104281; padding: 12px 16px; border-radius: 8px; }
  .radio label { margin-right: 16px; }
  .controls { display: flex; gap: 16px; flex-wrap: wrap; }
  .control { flex: 1 1 320px; } .control label { font-size: 13px; color: #52514e; }
  .tbl { border-collapse: collapse; width: 100%; background: #fcfcfb; font-size: 14px; }
  .tbl th { text-align: left; color: #52514e; font-weight: 500; border-bottom: 1px solid #c3c2b7; }
  .tbl th, .tbl td { padding: 6px 10px; } .tbl tr + tr td { border-top: 1px solid #eeede8; }
  details { margin-top: 16px; } summary { cursor: pointer; color: #52514e; }
</style></head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>"""

PAGES = {"/": "Огляд мережі", "/device": "Пристрій", "/anomalies": "Аномалії"}
app.layout = html.Div([dcc.Location(id="url"), html.Div(id="nav"), html.Div(id="page")],
                      className="page")


@app.callback(Output("nav", "children"), Output("page", "children"),
              Input("url", "pathname"), Input("url", "search"))
def show_page(pathname: str, search: str):
    """Обирає сторінку за адресою: /, /device?device=…&finding=…, /anomalies."""
    params = {k: v[0] for k, v in parse_qs((search or "").lstrip("?")).items()}
    pathname = pathname if pathname in PAGES else "/"
    found = {True: "", False: " (не знайдено)"}
    nav = html.Div([html.B("IoT-дашборд")]
                   + [dcc.Link(title, href=path, className="active" if path == pathname else "")
                      for path, title in PAGES.items()]
                   + [html.Span(f"дані: {ARGS.data} · маніфест: {ARGS.manifest}"
                                f"{found[MANIFEST is not None]} · знахідки: {ARGS.findings}"
                                f"{found[Path(ARGS.findings).is_file()]} · час UTC",
                                className="files")], className="top")
    if pathname == "/device":
        return nav, device_layout(params)
    if pathname == "/anomalies":
        return nav, anomalies_layout()
    return nav, overview_layout()


@app.callback(Output("hm-devices", "figure"), Input("mode", "value"))
def update_device_heatmap(mode: str):
    hourly = load_hourly(str(DATA), mtime(DATA))
    label = {r.device_id: f"{r.device_id} · {r.zone}" for r in INVENTORY.itertuples()}
    grid = hourly_grid(hourly.assign(key=hourly["device_id"].map(label)),
                       [label[d] for d in INVENTORY["device_id"]], T0, T1)
    return heatmap(grid, mode == "pct")


@app.callback(Output("hm-groups", "figure"), Output("group-table", "children"),
              Output("device-table", "children"),
              Input("mode", "value"), Input("group-by", "value"))
def update_groups(mode: str, by: str):
    inv = INVENTORY
    hourly = load_hourly(str(DATA), mtime(DATA))
    group_of = dict(zip(inv["device_id"], inv[by]))
    group_grid = hourly_grid(hourly.assign(key=hourly["device_id"].map(group_of)),
                             sorted(inv[by].unique()), T0, T1)
    # Доступність — частка годин, коли пристрій надсилав хоч щось.
    device_grid = hourly_grid(hourly.assign(key=hourly["device_id"]),
                              list(inv["device_id"]), T0, T1)
    inv = inv.assign(availability=(device_grid.values > 0).mean(axis=1) * 100)
    summary = inv.groupby(by).agg(
        manifest=("in_manifest", "sum"), observed=("n", lambda s: int((s > 0).sum())),
        messages=("n", "sum"), availability=("availability", "mean"),
        last=("last_ts", "max")).reset_index()
    summary = summary.assign(messages=summary["messages"].map(num),
                             availability=summary["availability"].map("{:.1f}".format),
                             last=summary["last"].map(fmt_dt))
    groups = table(summary, {by: "Зона" if by == "zone" else "Шлюз",
                             "manifest": "Пристроїв у маніфесті",
                             "observed": "Пристроїв у даних", "messages": "Повідомлень",
                             "availability": "Доступність, %",
                             "last": "Останнє повідомлення (UTC)"})
    devices = inv.assign(
        n=inv["n"].map(num), availability=inv["availability"].map("{:.1f}".format),
        in_manifest=inv["in_manifest"].map({True: "так", False: "ні"}),
        online=(inv["last_ts"].fillna(0) >= T1 - 3600).map({True: "так", False: "ні"}),
        first=inv["first_ts"].map(fmt_dt), last=inv["last_ts"].map(fmt_dt),
        link=[dcc.Link("графік", href=f"/device?device={d}") for d in inv["device_id"]])
    devices = table(devices, {"device_id": "Пристрій", "type": "Тип", "zone": "Зона",
                              "gateway": "Шлюз", "in_manifest": "У маніфесті",
                              "n": "Повідомлень", "first": "Перше (UTC)",
                              "last": "Останнє (UTC)", "availability": "Доступність, %",
                              "online": "На зв'язку наприкінці", "link": ""})
    return heatmap(group_grid, mode == "pct", row_height=26), groups, devices


@app.callback(Output("period", "options"), Output("period", "value"),
              Input("device", "value"), State("period", "value"))
def update_periods(device: str, period: str):
    """Інший пристрій — інші знахідки у списку «Період»."""
    options = period_options(device)
    return options, period if period in [o["value"] for o in options] else "all"


@app.callback(Output("device-chart", "figure"), Output("device-info", "children"),
              Output("device-note", "children"), Output("metric-table", "children"),
              Input("device", "value"), Input("period", "value"))
def update_device(device: str, period: str):
    t0, t1 = period_bounds(period or "all")
    fig, note = device_figure(device, t0, t1)
    row = INVENTORY.set_index("device_id").loc[device]
    info = [f"Тип {row['type']}, зона {row['zone']}, шлюз {row['gateway']}"
            + (f", інтервал за маніфестом {int(row['interval_s'])} с"
               if pd.notna(row["interval_s"]) else "")
            + f". Показано {fmt_dt(t0)} – {fmt_dt(t1)} UTC.",
            html.B("" if row["in_manifest"] else " Цього пристрою немає в маніфесті.")]
    stats = load_metric_stats(str(DATA), mtime(DATA), device)
    stats = stats.assign(unit=stats["metric"].map(UNITS).fillna(""), n=stats["n"].map(num),
                         first=stats["first_ts"].map(fmt_dt), last=stats["last_ts"].map(fmt_dt),
                         median_dt=stats["median_dt"].map(
                             lambda v: "—" if pd.isna(v) else f"{v:.0f}"))
    metrics = table(stats, {"metric": "Показник", "unit": "Одиниця",
                            "n": "Записів за весь період", "first": "Перший (UTC)",
                            "last": "Останній (UTC)",
                            "median_dt": "Інтервал між записами, с (медіана)"})
    return fig, info, note, metrics


@app.callback(Output("timeline", "figure"), Output("findings-table", "children"),
              Input("classes", "value"))
def update_anomalies(classes: list[str]):
    table_, message = findings()
    if table_ is None:                      # файл зник, поки дашборд працював
        return go.Figure(), html.P(message, className="info")
    table_ = table_[table_["cls"].isin(classes or [])].sort_values("start")
    view = table_.assign(
        devices_text=table_["devices"].map(", ".join),
        start_text=table_["start"].map(fmt_dt), end_text=table_["end"].map(fmt_dt),
        hours=((table_["end"] - table_["start"]) / 3600).map("{:.1f}".format),
        conf=table_["confidence"].map("{:.2f}".format),
        scored_text=table_["scored"].map({True: "так", False: "ні"}),
        link=[dcc.Link("відкрити", href=f"/device?device={ds[0]}&finding={no}") if ds else ""
              for ds, no in zip(table_["devices"], table_["no"])])
    return timeline(table_), table(view, {
        "no": "№", "cls": "Клас", "group": "Складність", "devices_text": "Пристрої",
        "start_text": "Початок (UTC)", "end_text": "Кінець (UTC)", "hours": "Тривалість, год",
        "conf": "Впевненість", "scored_text": "Буде оцінено", "link": "Графік",
        "evidence": "Обґрунтування"})


def port_is_free(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


if __name__ == "__main__":
    if not port_is_free(ARGS.port):
        sys.exit(f"Порт {ARGS.port} зайнятий — мабуть, дашборд уже запущено в іншому "
                 f"вікні терміналу. Закрийте його (Ctrl+C) або запустіть з іншим портом:\n"
                 f"    python dashboard/dash_app.py --port {ARGS.port + 1}")
    print(f"Дашборд: http://localhost:{ARGS.port}   (зупинка — Ctrl+C)")
    app.run(host="127.0.0.1", port=ARGS.port, debug=False)
