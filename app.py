from __future__ import annotations

from datetime import date, datetime, time, timedelta
import re

import altair as alt
import pandas as pd
import streamlit as st
import yfinance as yf


st.set_page_config(
    page_title="Manual Weekly Stock Signal Tracker",
    page_icon="📈",
    layout="wide",
)


DEFAULT_TEXT = """1   PHM       SELL      DOWN      95          STRONG    NORMAL      -1.000    1.43      -0.000    2026-04-27T22:58:34+00:00
2   EXPE      SELL      DOWN      88          STRONG    EVENTFUL    -0.970    -4.18     -0.000    2026-04-27T22:43:27+00:00
3   FCX       SELL      DOWN      95          STRONG    EVENTFUL    -1.000    0.41      -0.000    2026-04-27T22:44:58+00:00
4   TSLA      SELL      DOWN      67          MODERATE  NORMAL      -1.000    0.72      -0.000    2026-04-27T23:02:44+00:00
5   XOM       SELL      DOWN      61          MODERATE  NORMAL      -0.638    1.63      -0.000    2026-04-27T22:43:41+00:00
6   VLTO      SELL      DOWN      54          MODERATE  NORMAL      -0.560    -2.82     -0.000    2026-04-27T23:04:54+00:00
7   BLK       BUY       UP        51          MODERATE  NORMAL      0.535     4.22      -0.000    2026-04-27T22:34:05+00:00
8   MCO       BUY       UP        48          MODERATE  NORMAL      0.495     3.46      -0.000    2026-04-27T22:53:56+00:00
9   SMCI      SELL      DOWN      50          MODERATE  NORMAL      -0.631    -7.38     -0.000    2026-04-27T23:01:47+00:00
10  RCL       SELL      DOWN      45          MODERATE  NORMAL      -0.472    -3.04     -0.000    2026-04-27T23:00:02+00:00
11  BA        BUY       UP        46          MODERATE  NORMAL      0.444     3.27      -0.000    2026-04-27T22:34:22+00:00
12  NCLH      SELL      DOWN      50          MODERATE  EVENTFUL    -0.514    -2.80     -0.000    2026-04-27T22:55:20+00:00
13  VST       WATCH     UP        42          WEAK      NORMAL      0.455     4.64      -0.000    2026-04-27T23:05:39+00:00
14  COF       WATCH     UP        40          WEAK      NORMAL      0.428     5.55      -0.000    2026-04-27T22:35:36+00:00
15  CARR      WATCH     UP        43          WEAK      NORMAL      0.452     3.17      -0.000    2026-04-27T22:35:49+00:00
16  MSCI      WATCH     UP        43          WEAK      NORMAL      0.443     2.87      -0.000    2026-04-27T22:54:18+00:00
17  APTV      WATCH     UP        39          WEAK      NORMAL      0.407     4.83      -0.000    2026-04-27T22:32:17+00:00
18  EL        WATCH     UP        39          WEAK      NORMAL      0.389     4.99      -0.000    2026-04-27T22:43:01+00:00
19  ALLE      WATCH     UP        35          WEAK      NORMAL      0.421     6.07      -0.000    2026-04-27T22:30:17+00:00
20  DOV       WATCH     UP        43          WEAK      NORMAL      0.439     2.22      -0.000    2026-04-27T22:41:12+00:00
21  ARE       WATCH     UP        38          WEAK      NORMAL      0.385     4.88      -0.000    2026-04-27T22:30:08+00:00
22  AXP       WATCH     UP        38          WEAK      NORMAL      0.396     4.55      -0.000    2026-04-27T22:31:06+00:00
23  ROK       WATCH     UP        39          WEAK      NORMAL      0.418     3.78      -0.000    2026-04-27T22:59:44+00:00
24  RF        WATCH     UP        42          WEAK      NORMAL      0.394     2.80      -0.000    2026-04-27T22:59:22+00:00
25  NFLX      WATCH     DOWN      35          WEAK      NORMAL      -0.377    -5.86     -0.000    2026-04-27T22:54:32+00:00"""


# =========================
# CSS
# =========================

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1300px;
    }

    .chart-wrap {
        background: #ffffff;
        border: 1px solid rgba(17, 24, 39, 0.08);
        border-radius: 24px;
        padding: 20px 20px 12px 20px;
        box-shadow: 0 10px 28px rgba(17, 24, 39, 0.06);
        margin-bottom: 24px;
    }

    .chart-title-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 18px;
        margin-bottom: 4px;
    }

    .chart-big-number {
        font-size: 36px;
        font-weight: 800;
        letter-spacing: -1.1px;
        color: #111827;
        line-height: 1;
        margin-bottom: 6px;
    }

    .chart-change-green {
        color: #00C805;
        font-size: 15px;
        font-weight: 800;
    }

    .chart-change-red {
        color: #ef4444;
        font-size: 15px;
        font-weight: 800;
    }

    .chart-small-note {
        color: #6b7280;
        font-size: 13px;
        font-weight: 600;
    }

    div[role="radiogroup"] {
        background: #f9fafb;
        border: 1px solid rgba(17, 24, 39, 0.07);
        padding: 4px;
        border-radius: 999px;
        width: fit-content;
        margin-bottom: 10px;
    }

    div[role="radiogroup"] label {
        padding: 4px 10px;
        border-radius: 999px;
        color: #6b7280;
        font-weight: 700;
    }

    div[role="radiogroup"] label:has(input:checked) {
        background: white;
        color: #00C805;
        box-shadow: 0 1px 5px rgba(17, 24, 39, 0.12);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================
# CORE HELPERS
# =========================

def this_weeks_monday(today: date | None = None) -> date:
    today = today or date.today()
    return today - timedelta(days=today.weekday())


def yahoo_symbol(ticker: str) -> str:
    return str(ticker).strip().upper().replace(".", "-")


def parse_model_output(raw_text: str) -> pd.DataFrame:
    rows = []

    for line in raw_text.splitlines():
        line = line.strip()

        if not line or line.lower().startswith("rank"):
            continue

        parts = re.split(r"\s+", line)

        if len(parts) < 11:
            continue

        try:
            rows.append(
                {
                    "Rank": int(parts[0]),
                    "Ticker": parts[1].upper(),
                    "Action": parts[2].upper(),
                    "Direction": parts[3].upper(),
                    "Conviction": int(float(parts[4])),
                    "Edge": parts[5].upper(),
                    "Regime": parts[6].upper(),
                    "Model Score": float(parts[7]),
                    "Expected Move %": float(parts[8]),
                    "Setup Score": float(parts[9]),
                    "Run Timestamp": parts[10],
                }
            )
        except Exception:
            continue

    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_prices(tickers: tuple[str, ...], monday_date: date) -> pd.DataFrame:
    records = []
    start_date = monday_date
    end_date = datetime.now().date() + timedelta(days=1)

    for ticker in tickers:
        symbol = yahoo_symbol(ticker)
        monday_price = None
        current_price = None
        source = "N/A"
        error = ""

        try:
            intraday = yf.download(
                symbol,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                interval="30m",
                auto_adjust=True,
                progress=False,
                prepost=False,
                threads=False,
            )

            if intraday is not None and not intraday.empty:
                if isinstance(intraday.columns, pd.MultiIndex):
                    intraday.columns = [c[0] for c in intraday.columns]

                intraday = intraday.rename(columns=str.lower)
                intraday.index = pd.to_datetime(intraday.index).tz_localize(None)

                monday_rows = intraday[intraday.index.date == monday_date]

                if not monday_rows.empty and "close" in monday_rows.columns:
                    window_start = datetime.combine(monday_date, time(11, 30))
                    window_end = datetime.combine(monday_date, time(12, 30))

                    noon_window = monday_rows[
                        (monday_rows.index >= window_start)
                        & (monday_rows.index <= window_end)
                    ]

                    if not noon_window.empty:
                        monday_price = float(noon_window["close"].dropna().mean())
                        source = "Monday 11:30-12:30 avg"
                    elif not monday_rows["close"].dropna().empty:
                        monday_price = float(monday_rows["close"].dropna().iloc[0])
                        source = "Monday first intraday price"

                if "close" in intraday.columns and not intraday["close"].dropna().empty:
                    current_price = float(intraday["close"].dropna().iloc[-1])

            if monday_price is None or current_price is None:
                daily = yf.download(
                    symbol,
                    start=start_date.isoformat(),
                    end=end_date.isoformat(),
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )

                if daily is not None and not daily.empty:
                    if isinstance(daily.columns, pd.MultiIndex):
                        daily.columns = [c[0] for c in daily.columns]

                    daily = daily.rename(columns=str.lower)

                    if "close" in daily.columns and not daily["close"].dropna().empty:
                        if monday_price is None:
                            monday_price = float(daily["close"].dropna().iloc[0])
                            source = "Monday daily close fallback"

                        if current_price is None:
                            current_price = float(daily["close"].dropna().iloc[-1])

        except Exception as exc:
            error = str(exc)

        records.append(
            {
                "Ticker": ticker,
                "Monday Reference Price": monday_price,
                "Current Price": current_price,
                "Reference Price Source": source,
                "Price Error": error,
            }
        )

    return pd.DataFrame(records)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_position_return_series(
    model_rows: tuple[tuple[str, str, str], ...],
    monday_date: date,
) -> pd.DataFrame:
    """
    Builds portfolio return lines.

    BUY/UP = long return.
    SELL/DOWN = short return, so returns are inverted.
    WATCH is ignored.
    """

    if not model_rows:
        return pd.DataFrame()

    start_date = monday_date
    end_date = datetime.now().date() + timedelta(days=1)

    buy_series = []
    sell_series = []

    for ticker, action, direction in model_rows:
        action = str(action).upper()
        direction = str(direction).upper()

        valid_buy = action == "BUY" and direction == "UP"
        valid_sell = action == "SELL" and direction == "DOWN"

        if not (valid_buy or valid_sell):
            continue

        symbol = yahoo_symbol(ticker)

        try:
            df = yf.download(
                symbol,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                interval="30m",
                auto_adjust=True,
                progress=False,
                prepost=False,
                threads=False,
            )

            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            df = df.rename(columns=str.lower)
            df.index = pd.to_datetime(df.index).tz_localize(None)

            if "close" not in df.columns or df["close"].dropna().empty:
                continue

            monday_rows = df[df.index.date == monday_date]

            if monday_rows.empty:
                continue

            window_start = datetime.combine(monday_date, time(11, 30))
            window_end = datetime.combine(monday_date, time(12, 30))

            noon_window = monday_rows[
                (monday_rows.index >= window_start)
                & (monday_rows.index <= window_end)
            ]

            if not noon_window.empty:
                ref_price = float(noon_window["close"].dropna().mean())
            else:
                ref_price = float(monday_rows["close"].dropna().iloc[0])

            long_pct = (df["close"] - ref_price) / ref_price * 100

            if valid_buy:
                long_pct.name = ticker
                buy_series.append(long_pct)

            if valid_sell:
                short_pct = -long_pct
                short_pct.name = ticker
                sell_series.append(short_pct)

        except Exception:
            continue

    chart_parts = []

    if buy_series:
        buy_df = pd.concat(buy_series, axis=1).sort_index()
        chart_parts.append(buy_df.mean(axis=1).rename("BUY basket"))

    if sell_series:
        sell_df = pd.concat(sell_series, axis=1).sort_index()
        chart_parts.append(sell_df.mean(axis=1).rename("SELL short basket"))

    if buy_series or sell_series:
        all_series = buy_series + sell_series
        combined_df = pd.concat(all_series, axis=1).sort_index()
        chart_parts.append(combined_df.mean(axis=1).rename("Combined active basket"))

    if not chart_parts:
        return pd.DataFrame()

    chart_df = pd.concat(chart_parts, axis=1).sort_index()
    return chart_df.dropna(how="all")


def filter_chart_range(chart_df: pd.DataFrame, selected_range: str) -> pd.DataFrame:
    if chart_df.empty:
        return chart_df

    chart_df = chart_df.copy()
    chart_df.index = pd.to_datetime(chart_df.index).tz_localize(None)

    latest_time = chart_df.index.max()

    if selected_range == "LIVE":
        start = latest_time - timedelta(hours=6)
        return chart_df[chart_df.index >= start]

    if selected_range == "1D":
        start = latest_time - timedelta(days=1)
        return chart_df[chart_df.index >= start]

    if selected_range == "2D":
        start = latest_time - timedelta(days=2)
        return chart_df[chart_df.index >= start]

    if selected_range == "1W":
        start = latest_time - timedelta(days=7)
        return chart_df[chart_df.index >= start]

    return chart_df


def add_tracking_columns(model_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    out = model_df.merge(price_df, on="Ticker", how="left")

    out["Change Since Monday %"] = (
        (out["Current Price"] - out["Monday Reference Price"])
        / out["Monday Reference Price"]
        * 100
    )

    def actual_direction(change):
        if pd.isna(change):
            return "N/A"
        if change > 0:
            return "UP"
        if change < 0:
            return "DOWN"
        return "FLAT"

    out["Actual Direction So Far"] = out["Change Since Monday %"].apply(actual_direction)

    def correct(row):
        predicted = row["Direction"]
        actual = row["Actual Direction So Far"]

        if actual == "N/A":
            return "N/A"

        if predicted == "UP" and actual == "UP":
            return "YES"
        if predicted == "DOWN" and actual == "DOWN":
            return "YES"
        if predicted == "NEUTRAL" and actual == "FLAT":
            return "YES"

        return "NO"

    out["Correct So Far"] = out.apply(correct, axis=1)

    return out


def build_short_calculator_table(tracker_df: pd.DataFrame) -> pd.DataFrame:
    return tracker_df[
        (tracker_df["Action"] == "SELL")
        & (tracker_df["Direction"] == "DOWN")
        & (tracker_df["Monday Reference Price"].notna())
        & (tracker_df["Current Price"].notna())
    ].copy()


def build_what_if(tracker_df: pd.DataFrame) -> pd.DataFrame:
    """
    One-share what-if summary.

    BUY/UP = buy 1 share at Monday reference price.
    SELL/DOWN = short 1 share at Monday reference price.
    WATCH is ignored.
    """

    strategies = [
        (
            "BUY 1 share of each BUY/UP stock",
            (tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"),
        ),
        (
            "SHORT 1 share of each SELL/DOWN stock",
            (tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"),
        ),
        (
            "Combined active calls: 1 share each BUY/UP + SELL/DOWN",
            (
                ((tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"))
                | ((tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"))
            ),
        ),
    ]

    rows = []

    for name, mask in strategies:
        basket = tracker_df[mask].copy()

        total_entry_value = 0.0
        total_current_value = 0.0
        total_pnl = 0.0
        valid_positions = 0

        for _, row in basket.iterrows():
            monday_price = row.get("Monday Reference Price")
            current_price = row.get("Current Price")

            if pd.isna(monday_price) or pd.isna(current_price):
                continue

            monday_price = float(monday_price)
            current_price = float(current_price)

            action = str(row.get("Action", "")).upper()
            direction = str(row.get("Direction", "")).upper()

            if action == "BUY" and direction == "UP":
                pnl = current_price - monday_price
                current_position_value = current_price

            elif action == "SELL" and direction == "DOWN":
                pnl = monday_price - current_price
                current_position_value = monday_price + pnl

            else:
                continue

            total_entry_value += monday_price
            total_current_value += current_position_value
            total_pnl += pnl
            valid_positions += 1

        percent_return = (total_pnl / total_entry_value * 100) if total_entry_value else None

        rows.append(
            {
                "Strategy": name,
                "Stocks": valid_positions,
                "Monday Entry Value": total_entry_value if valid_positions else None,
                "Current Position Value": total_current_value if valid_positions else None,
                "Dollar P/L": total_pnl if valid_positions else None,
                "Percent Return": percent_return,
            }
        )

    return pd.DataFrame(rows)


def build_what_if_positions(tracker_df: pd.DataFrame, side: str) -> pd.DataFrame:
    """
    One-share detail table.

    BUY/UP = buy 1 share at Monday reference price.
    SELL/DOWN = short 1 share at Monday reference price.
    """

    side = side.upper()

    if side == "BUY":
        basket = tracker_df[
            (tracker_df["Action"] == "BUY")
            & (tracker_df["Direction"] == "UP")
            & (tracker_df["Change Since Monday %"].notna())
        ].copy()
        position_type = "Long"

    elif side == "SELL":
        basket = tracker_df[
            (tracker_df["Action"] == "SELL")
            & (tracker_df["Direction"] == "DOWN")
            & (tracker_df["Change Since Monday %"].notna())
        ].copy()
        position_type = "Short"

    else:
        basket = tracker_df[
            (
                ((tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"))
                | ((tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"))
            )
            & (tracker_df["Change Since Monday %"].notna())
        ].copy()
        position_type = "Mixed"

    if basket.empty:
        return pd.DataFrame(
            columns=[
                "Ticker",
                "Position",
                "Direction",
                "Monday Price",
                "Current Price",
                "Shares / Shorted Shares",
                "Stock Move %",
                "Position Return %",
                "Dollar P/L",
                "Correct So Far",
            ]
        )

    rows = []

    for _, row in basket.iterrows():
        action = str(row.get("Action", "")).upper()
        direction = str(row.get("Direction", "")).upper()
        stock_move = float(row["Change Since Monday %"])

        monday_price = row.get("Monday Reference Price")
        current_price = row.get("Current Price")

        if pd.isna(monday_price) or pd.isna(current_price):
            continue

        monday_price = float(monday_price)
        current_price = float(current_price)
        shares = 1.0

        if action == "BUY" and direction == "UP":
            pos_return = stock_move
            pos_label = "Long"
            pnl = current_price - monday_price

        elif action == "SELL" and direction == "DOWN":
            pos_return = -stock_move
            pos_label = "Short"
            pnl = monday_price - current_price

        else:
            continue

        rows.append(
            {
                "Ticker": row["Ticker"],
                "Position": pos_label if position_type == "Mixed" else position_type,
                "Direction": direction,
                "Monday Price": monday_price,
                "Current Price": current_price,
                "Shares / Shorted Shares": shares,
                "Stock Move %": stock_move,
                "Position Return %": pos_return,
                "Dollar P/L": pnl,
                "Correct So Far": row.get("Correct So Far"),
            }
        )

    return pd.DataFrame(rows)


def build_summary(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    active = df[df["Action"].isin(["BUY", "SELL"])].copy()
    valid = active[active["Correct So Far"].isin(["YES", "NO"])].copy()

    if valid.empty:
        return pd.DataFrame(
            columns=[
                group_col,
                "Stocks",
                "Correct",
                "Accuracy %",
                "Avg Change Since Monday %",
            ]
        )

    valid["Correct Flag"] = (valid["Correct So Far"] == "YES").astype(int)

    out = (
        valid.groupby(group_col)
        .agg(
            Stocks=("Ticker", "count"),
            Correct=("Correct Flag", "sum"),
            **{"Avg Change Since Monday %": ("Change Since Monday %", "mean")},
        )
        .reset_index()
    )

    out["Accuracy %"] = out["Correct"] / out["Stocks"] * 100

    return out[
        [
            group_col,
            "Stocks",
            "Correct",
            "Accuracy %",
            "Avg Change Since Monday %",
        ]
    ]


def make_portfolio_chart(filtered_chart_df: pd.DataFrame, chart_range: str):
    chart_data = filtered_chart_df.reset_index()

    first_col = chart_data.columns[0]
    chart_data = chart_data.rename(columns={first_col: "Time"})

    chart_data = (
        chart_data
        .melt(id_vars="Time", var_name="Basket", value_name="Return %")
        .dropna()
    )

    if chart_data.empty:
        return None, None, None

    latest_rows = (
        chart_data.sort_values("Time")
        .groupby("Basket", as_index=False)
        .tail(1)
        .sort_values("Basket")
    )

    combined_latest = latest_rows[
        latest_rows["Basket"] == "Combined active basket"
    ]

    if not combined_latest.empty:
        latest_return = float(combined_latest["Return %"].iloc[0])
    else:
        latest_return = float(latest_rows["Return %"].mean())

    first_time = chart_data["Time"].min()
    last_time = chart_data["Time"].max()

    if chart_range == "LIVE":
        time_format = "%I %p"
        tick_count = 6

    elif chart_range == "1D":
        time_format = "%I %p"
        tick_count = 8

    elif chart_range == "2D":
        time_format = "%I %p"
        tick_count = 7

    elif chart_range == "1W":
        time_format = "%a"
        tick_count = 5

    else:
        time_format = "%b %d"
        tick_count = 6

    line_chart = (
        alt.Chart(chart_data)
        .mark_line(
            strokeWidth=3,
            interpolate="monotone",
        )
        .encode(
            x=alt.X(
                "Time:T",
                title=None,
                axis=alt.Axis(
                    format=time_format,
                    tickCount=tick_count,
                    grid=False,
                    labelColor="#6b7280",
                    labelFontSize=12,
                ),
            ),
            y=alt.Y(
                "Return %:Q",
                title=None,
                scale=alt.Scale(zero=False),
                axis=alt.Axis(
                    grid=True,
                    gridColor="#eef2f7",
                    labelColor="#9ca3af",
                    labelFontSize=12,
                ),
            ),
            color=alt.Color(
                "Basket:N",
                title=None,
                scale=alt.Scale(
                    domain=[
                        "Combined active basket",
                        "BUY basket",
                        "SELL short basket",
                    ],
                    range=[
                        "#00C805",
                        "#2563eb",
                        "#ef4444",
                    ],
                ),
                legend=alt.Legend(
                    orient="top",
                    direction="horizontal",
                    labelFontSize=12,
                    symbolSize=120,
                ),
            ),
            tooltip=[
                alt.Tooltip("Time:T", title="Time"),
                alt.Tooltip("Basket:N", title="Basket"),
                alt.Tooltip("Return %:Q", title="Return %", format=".2f"),
            ],
        )
        .properties(height=360)
    )

    zero_line = (
        alt.Chart(pd.DataFrame({"Return %": [0]}))
        .mark_rule(
            color="#9ca3af",
            strokeDash=[5, 5],
            strokeWidth=1.5,
        )
        .encode(y="Return %:Q")
    )

    chart = (
        line_chart + zero_line
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        domain=False,
        ticks=False,
    )

    return chart, latest_return, (first_time, last_time)


def style_tracker(df: pd.DataFrame):
    def color_action(value):
        value = str(value).upper()

        if value == "BUY":
            return "background-color: #15803d; color: white; font-weight: 700;"
        if value == "SELL":
            return "background-color: #b91c1c; color: white; font-weight: 700;"
        if value == "WATCH":
            return "background-color: #ca8a04; color: black; font-weight: 700;"

        return ""

    def color_correct(value):
        value = str(value).upper()

        if value == "YES":
            return "background-color: #15803d; color: white; font-weight: 700;"
        if value == "NO":
            return "background-color: #b91c1c; color: white; font-weight: 700;"
        if value == "N/A":
            return "background-color: #6b7280; color: white;"

        return ""

    def color_direction(value):
        value = str(value).upper()

        if value == "UP":
            return "background-color: #dcfce7; color: #14532d; font-weight: 700;"
        if value == "DOWN":
            return "background-color: #fee2e2; color: #7f1d1d; font-weight: 700;"
        if value == "NEUTRAL":
            return "background-color: #e5e7eb; color: #111827; font-weight: 700;"

        return ""

    def color_return(value):
        try:
            if value > 0:
                return "color: #15803d; font-weight: 700;"
            if value < 0:
                return "color: #b91c1c; font-weight: 700;"
        except Exception:
            pass

        return ""

    return (
        df.style
        .map(color_action, subset=["Action"])
        .map(color_direction, subset=["Direction", "Actual Direction So Far"])
        .map(color_correct, subset=["Correct So Far"])
        .map(color_return, subset=["Change Since Monday %"])
        .format(
            {
                "Monday Reference Price": "{:.2f}",
                "Current Price": "{:.2f}",
                "Change Since Monday %": "{:.2f}%",
                "Model Score": "{:.3f}",
                "Expected Move %": "{:.2f}",
                "Setup Score": "{:.3f}",
            },
            na_rep="",
        )
    )


def style_money(df: pd.DataFrame):
    format_map = {}

    for col in [
        "Starting Capital",
        "Current Value",
        "Dollar P/L",
        "Monday Price",
        "Current Price",
        "Monday Entry Value",
        "Current Position Value",
    ]:
        if col in df.columns:
            format_map[col] = "${:,.2f}"

    for col in [
        "Percent Return",
        "Accuracy %",
        "Avg Change Since Monday %",
        "Stock Move %",
        "Position Return %",
    ]:
        if col in df.columns:
            format_map[col] = "{:.2f}%"

    if "Shares / Shorted Shares" in df.columns:
        format_map["Shares / Shorted Shares"] = "{:,.4f}"

    def color_num(value):
        try:
            if value > 0:
                return "color: #15803d; font-weight: 700;"
            if value < 0:
                return "color: #b91c1c; font-weight: 700;"
        except Exception:
            pass

        return ""

    styled = df.style.format(format_map, na_rep="")

    for col in [
        "Dollar P/L",
        "Percent Return",
        "Accuracy %",
        "Avg Change Since Monday %",
        "Stock Move %",
        "Position Return %",
    ]:
        if col in df.columns:
            styled = styled.map(color_num, subset=[col])

    return styled


# =========================
# APP
# =========================

st.title("Manual Weekly Stock Signal Tracker")
st.caption(
    "Tracks this week's model output against this week's Monday reference price, not daily percent change."
)

if "raw_model_output" not in st.session_state:
    st.session_state["raw_model_output"] = DEFAULT_TEXT

tab_dashboard, tab_input = st.tabs(["Dashboard", "Paste model output"])

with tab_input:
    st.session_state["raw_model_output"] = st.text_area(
        "Paste model output",
        value=st.session_state["raw_model_output"],
        height=360,
    )
    st.info("After editing this box, go back to the Dashboard tab. The tracker updates automatically.")

model_df = parse_model_output(st.session_state["raw_model_output"])
monday_date = this_weeks_monday()

with tab_dashboard:
    st.caption(f"Reference period: this week's Monday ({monday_date}) to now.")

    if model_df.empty:
        st.warning("No valid rows found. Paste your model output in the second tab.")
        st.stop()

    tickers = tuple(
        model_df["Ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )

    model_rows = tuple(
        model_df[["Ticker", "Action", "Direction"]]
        .dropna()
        .astype(str)
        .itertuples(index=False, name=None)
    )

    with st.spinner("Fetching prices and calculating performance..."):
        price_df = fetch_prices(tickers, monday_date)
        tracker_df = add_tracking_columns(model_df, price_df)
        portfolio_chart_df = fetch_position_return_series(model_rows, monday_date)

    active_valid = tracker_df[
        (tracker_df["Action"].isin(["BUY", "SELL"]))
        & (tracker_df["Correct So Far"].isin(["YES", "NO"]))
    ].copy()

    active_correct = int((active_valid["Correct So Far"] == "YES").sum()) if not active_valid.empty else 0
    active_total = int(len(active_valid))
    active_accuracy = active_correct / active_total * 100 if active_total else 0

    what_if_df = build_what_if(tracker_df)

    combined_row = what_if_df[
        what_if_df["Strategy"] == "Combined active calls: 1 share each BUY/UP + SELL/DOWN"
    ]

    if not combined_row.empty and pd.notna(combined_row["Dollar P/L"].iloc[0]):
        combined_pnl = float(combined_row["Dollar P/L"].iloc[0])
        combined_return = float(combined_row["Percent Return"].iloc[0])
    else:
        combined_pnl = 0.0
        combined_return = 0.0

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Active calls", active_total)
    c2.metric("Active correct", active_correct)
    c3.metric("Active accuracy", f"{active_accuracy:.1f}%")
    c4.metric("1-share P/L", f"${combined_pnl:,.2f}", f"{combined_return:.2f}%")

    st.subheader("Portfolio performance")

    chart_range = st.radio(
        "Range",
        ["LIVE", "1D", "2D", "1W"],
        horizontal=True,
        label_visibility="collapsed",
    )

    filtered_chart_df = filter_chart_range(portfolio_chart_df, chart_range)

    st.caption(
        "Equal-weight basket returns. BUY/UP is treated as long. SELL/DOWN is treated as short. WATCH is ignored."
    )

    if filtered_chart_df.empty:
        st.info("No portfolio chart data available yet.")
    else:
        chart, latest_return, time_window = make_portfolio_chart(filtered_chart_df, chart_range)

        if chart is None:
            st.info("No portfolio chart data available yet.")
        else:
            first_time, last_time = time_window

            change_class = "chart-change-green" if latest_return >= 0 else "chart-change-red"
            change_sign = "+" if latest_return >= 0 else ""

            st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)

            st.markdown(
                f"""
                <div class="chart-title-row">
                    <div>
                        <div class="chart-big-number">{change_sign}{latest_return:.2f}%</div>
                        <div class="{change_class}">Combined active basket</div>
                    </div>
                    <div class="chart-small-note">
                        {first_time.strftime("%b %d, %I:%M %p")} → {last_time.strftime("%b %d, %I:%M %p")}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            st.altair_chart(chart, use_container_width=True)

            st.markdown("</div>", unsafe_allow_html=True)

    st.subheader("Position details")
    st.caption(
        "Assumes 1 share bought for each BUY/UP call and 1 share shorted for each SELL/DOWN call at Monday's reference price."
    )

    detail_buy, detail_sell, detail_combined = st.tabs(
        ["BUY/UP positions", "SELL/DOWN short positions", "Combined active positions"]
    )

    with detail_buy:
        st.dataframe(
            style_money(build_what_if_positions(tracker_df, "BUY")),
            use_container_width=True,
            hide_index=True,
        )

    with detail_sell:
        st.dataframe(
            style_money(build_what_if_positions(tracker_df, "SELL")),
            use_container_width=True,
            hide_index=True,
        )

    with detail_combined:
        st.dataframe(
            style_money(build_what_if_positions(tracker_df, "COMBINED")),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Robinhood bearish trade calculators")
    st.caption(
        "For SELL/DOWN calls. The paper short calculator shows classic short math. "
        "The put calculator is the cleaner Robinhood-style bearish setup if your account is approved for options."
    )

    short_df = build_short_calculator_table(tracker_df)

    if short_df.empty:
        st.info("No valid SELL/DOWN stocks available yet.")
    else:
        calc_tab_short, calc_tab_put = st.tabs(
            ["Paper short calculator", "Put option calculator"]
        )

        with calc_tab_short:
            selected_short_ticker = st.selectbox(
                "Select stock to short",
                short_df["Ticker"].tolist(),
                key="short_calc_ticker",
            )

            selected_row = short_df[short_df["Ticker"] == selected_short_ticker].iloc[0]

            monday_price = float(selected_row["Monday Reference Price"])
            current_price = float(selected_row["Current Price"])

            col_a, col_b, col_c = st.columns(3)

            with col_a:
                short_amount = st.number_input(
                    "Dollar amount to short",
                    min_value=0.0,
                    value=1000.0,
                    step=100.0,
                    key="short_calc_amount",
                )

            with col_b:
                target_cover_price = st.number_input(
                    "Target cover price",
                    min_value=0.01,
                    value=current_price,
                    step=1.0,
                    key="short_calc_target_price",
                )

            with col_c:
                st.metric("Monday short price", f"${monday_price:,.2f}")
                st.metric("Current price", f"${current_price:,.2f}")

            shares_shorted = short_amount / monday_price if monday_price else 0.0

            current_pnl = shares_shorted * (monday_price - current_price)
            target_pnl = shares_shorted * (monday_price - target_cover_price)

            current_return_pct = (current_pnl / short_amount * 100) if short_amount else 0.0
            target_return_pct = (target_pnl / short_amount * 100) if short_amount else 0.0

            calc_cols = st.columns(4)

            calc_cols[0].metric("Shares shorted", f"{shares_shorted:,.4f}")
            calc_cols[1].metric(
                "P/L if covered now",
                f"${current_pnl:,.2f}",
                f"{current_return_pct:.2f}%",
            )
            calc_cols[2].metric(
                "P/L at target cover",
                f"${target_pnl:,.2f}",
                f"{target_return_pct:.2f}%",
            )
            calc_cols[3].metric("Target cover price", f"${target_cover_price:,.2f}")

            st.caption(
                "Paper short logic: short at the Monday price, then buy back later. "
                "If the stock falls, the short gains. If the stock rises, the short loses."
            )

        with calc_tab_put:
            selected_put_ticker = st.selectbox(
                "Select stock for put option",
                short_df["Ticker"].tolist(),
                key="put_calc_ticker",
            )

            selected_put_row = short_df[short_df["Ticker"] == selected_put_ticker].iloc[0]

            monday_price = float(selected_put_row["Monday Reference Price"])
            current_price = float(selected_put_row["Current Price"])

            put_col_a, put_col_b, put_col_c, put_col_d = st.columns(4)

            with put_col_a:
                contracts = st.number_input(
                    "Contracts",
                    min_value=1,
                    value=1,
                    step=1,
                    key="put_contracts",
                )

            with put_col_b:
                premium_paid = st.number_input(
                    "Premium paid per share",
                    min_value=0.01,
                    value=1.00,
                    step=0.05,
                    key="put_premium_paid",
                )

            with put_col_c:
                current_premium = st.number_input(
                    "Current premium per share",
                    min_value=0.00,
                    value=1.00,
                    step=0.05,
                    key="put_current_premium",
                )

            with put_col_d:
                target_premium = st.number_input(
                    "Target premium per share",
                    min_value=0.00,
                    value=1.50,
                    step=0.05,
                    key="put_target_premium",
                )

            total_cost = contracts * premium_paid * 100
            current_value = contracts * current_premium * 100
            target_value = contracts * target_premium * 100

            current_pnl = current_value - total_cost
            target_pnl = target_value - total_cost

            current_return_pct = (current_pnl / total_cost * 100) if total_cost else 0.0
            target_return_pct = (target_pnl / total_cost * 100) if total_cost else 0.0

            put_metrics = st.columns(5)

            put_metrics[0].metric("Stock Monday price", f"${monday_price:,.2f}")
            put_metrics[1].metric("Stock current price", f"${current_price:,.2f}")
            put_metrics[2].metric("Total option cost", f"${total_cost:,.2f}")
            put_metrics[3].metric(
                "P/L now",
                f"${current_pnl:,.2f}",
                f"{current_return_pct:.2f}%",
            )
            put_metrics[4].metric(
                "P/L at target",
                f"${target_pnl:,.2f}",
                f"{target_return_pct:.2f}%",
            )

            st.caption(
                "Put option logic: one contract usually controls 100 shares. "
                "If the put premium rises after the stock falls, the option position gains. "
                "Max loss for a bought put is the premium paid, but options can expire worthless."
            )

    st.subheader("Tracker")
    st.dataframe(
        style_tracker(tracker_df),
        use_container_width=True,
        hide_index=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Accuracy by action")
        st.dataframe(
            style_money(build_summary(tracker_df, "Action")),
            use_container_width=True,
            hide_index=True,
        )

    with col2:
        st.subheader("Accuracy by direction")
        st.dataframe(
            style_money(build_summary(tracker_df, "Direction")),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Download results")
    st.download_button(
        "Download tracker CSV",
        data=tracker_df.to_csv(index=False).encode("utf-8"),
        file_name=f"manual_weekly_tracker_{monday_date}.csv",
        mime="text/csv",
    )
