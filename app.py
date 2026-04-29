from __future__ import annotations

from datetime import date, datetime, time, timedelta
import re

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
def fetch_position_return_series(model_rows: tuple[tuple[str, str, str], ...], monday_date: date) -> pd.DataFrame:
    """
    Builds Robinhood-style portfolio lines.

    BUY/UP = long return
    SELL/DOWN = short return, so returns are inverted
    WATCH is ignored
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

        if not ((action == "BUY" and direction == "UP") or (action == "SELL" and direction == "DOWN")):
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

            if action == "BUY" and direction == "UP":
                long_pct.name = ticker
                buy_series.append(long_pct)

            if action == "SELL" and direction == "DOWN":
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

    now = datetime.now()

    if selected_range == "Today":
        start = datetime.combine(now.date(), time(0, 0))
        return chart_df[chart_df.index >= start]

    if selected_range == "Week":
        start = datetime.combine(this_weeks_monday(now.date()), time(0, 0))
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


def build_what_if(tracker_df: pd.DataFrame, starting_capital: float) -> pd.DataFrame:
    strategies = [
        (
            "BUY all BUY/UP stocks",
            (tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"),
        ),
        (
            "SHORT all SELL/DOWN stocks",
            (tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"),
        ),
        (
            "Combined active calls: BUY/UP + SELL/DOWN",
            (
                ((tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"))
                | ((tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"))
            ),
        ),
    ]

    rows = []

    for name, mask in strategies:
        basket = tracker_df[mask].copy()
        returns = []

        for _, row in basket.iterrows():
            change = row.get("Change Since Monday %")
            if pd.isna(change):
                continue

            action = str(row.get("Action", "")).upper()
            direction = str(row.get("Direction", "")).upper()

            if action == "BUY" and direction == "UP":
                returns.append(float(change))
            elif action == "SELL" and direction == "DOWN":
                returns.append(float(-change))

        if returns:
            percent_return = sum(returns) / len(returns)
            current_value = starting_capital * (1 + percent_return / 100)
            pnl = current_value - starting_capital
            stocks = len(returns)
            dollars_per_stock = starting_capital / stocks
        else:
            percent_return = None
            current_value = None
            pnl = None
            stocks = 0
            dollars_per_stock = None

        rows.append(
            {
                "Strategy": name,
                "Stocks": stocks,
                "Starting Capital": starting_capital,
                "Dollars Per Stock": dollars_per_stock,
                "Current Value": current_value,
                "Dollar P/L": pnl,
                "Percent Return": percent_return,
            }
        )

    return pd.DataFrame(rows)


def build_what_if_positions(tracker_df: pd.DataFrame, starting_capital: float, side: str) -> pd.DataFrame:
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
                "Stock Move %",
                "Position Return %",
                "Starting $",
                "Current $",
                "Dollar P/L",
                "Correct So Far",
            ]
        )

    dollars_per_stock = starting_capital / len(basket)
    rows = []

    for _, row in basket.iterrows():
        action = str(row.get("Action", "")).upper()
        direction = str(row.get("Direction", "")).upper()
        stock_move = float(row["Change Since Monday %"])

        if action == "BUY" and direction == "UP":
            pos_return = stock_move
            pos_label = "Long"
        elif action == "SELL" and direction == "DOWN":
            pos_return = -stock_move
            pos_label = "Short"
        else:
            continue

        current_value = dollars_per_stock * (1 + pos_return / 100)
        pnl = current_value - dollars_per_stock

        rows.append(
            {
                "Ticker": row["Ticker"],
                "Position": pos_label if position_type == "Mixed" else position_type,
                "Direction": direction,
                "Monday Price": row.get("Monday Reference Price"),
                "Current Price": row.get("Current Price"),
                "Stock Move %": stock_move,
                "Position Return %": pos_return,
                "Starting $": dollars_per_stock,
                "Current $": current_value,
                "Dollar P/L": pnl,
                "Correct So Far": row.get("Correct So Far"),
            }
        )

    return pd.DataFrame(rows)


def build_summary(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    # WATCH is intentionally excluded from summaries.
    active = df[df["Action"].isin(["BUY", "SELL"])].copy()
    valid = active[active["Correct So Far"].isin(["YES", "NO"])].copy()

    if valid.empty:
        return pd.DataFrame(columns=[group_col, "Stocks", "Correct", "Accuracy %", "Avg Change Since Monday %"])

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
    return out[[group_col, "Stocks", "Correct", "Accuracy %", "Avg Change Since Monday %"]]


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

    for col in ["Starting Capital", "Dollars Per Stock", "Current Value", "Dollar P/L", "Starting $", "Current $", "Monday Price", "Current Price"]:
        if col in df.columns:
            format_map[col] = "${:,.2f}"

    for col in ["Percent Return", "Accuracy %", "Avg Change Since Monday %", "Stock Move %", "Position Return %"]:
        if col in df.columns:
            format_map[col] = "{:.2f}%"

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

    for col in ["Dollar P/L", "Percent Return", "Accuracy %", "Avg Change Since Monday %", "Stock Move %", "Position Return %"]:
        if col in df.columns:
            styled = styled.map(color_num, subset=[col])

    return styled


st.title("Manual Weekly Stock Signal Tracker")
st.caption("Tracks this week's model output against this week's Monday reference price, not daily percent change.")

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

    starting_capital = st.number_input(
        "What-if starting capital",
        min_value=100.0,
        max_value=10_000_000.0,
        value=10_000.0,
        step=100.0,
    )

    if model_df.empty:
        st.warning("No valid rows found. Paste your model output in the second tab.")
        st.stop()

    tickers = tuple(model_df["Ticker"].dropna().astype(str).str.upper().unique().tolist())
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

    what_if_df = build_what_if(tracker_df, starting_capital)
    combined_row = what_if_df[what_if_df["Strategy"] == "Combined active calls: BUY/UP + SELL/DOWN"]

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
    c4.metric("What-if P/L", f"${combined_pnl:,.2f}", f"{combined_return:.2f}%")

    st.subheader("Portfolio performance")
    chart_range = st.radio(
        "Range",
        ["Today", "Week", "All time"],
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
        import altair as alt

        chart_data = filtered_chart_df.reset_index()
        
        first_col = chart_data.columns[0]
        chart_data = chart_data.rename(columns={first_col: "Time"})
        
        chart_data = (
            chart_data
            .melt(id_vars="Time", var_name="Basket", value_name="Return %")
            .dropna()
        )
        line_chart = (
            alt.Chart(chart_data)
            .mark_line(strokeWidth=2.5)
            .encode(
                x=alt.X("Time:T", title="Time"),
                y=alt.Y("Return %:Q", title="Return % since Monday"),
                color=alt.Color("Basket:N", title="Basket"),
                tooltip=[
                    alt.Tooltip("Time:T", title="Time"),
                    alt.Tooltip("Basket:N", title="Basket"),
                    alt.Tooltip("Return %:Q", title="Return %", format=".2f"),
                ],
            )
        )

        zero_line = (
            alt.Chart(pd.DataFrame({"Return %": [0]}))
            .mark_rule(color="gray", strokeDash=[6, 4], strokeWidth=2)
            .encode(y="Return %:Q")
        )

        st.altair_chart(line_chart + zero_line, use_container_width=True)

    st.subheader("What-if portfolio")
    st.caption(
        "This answers: if the starting capital was split equally across the model's active calls on Monday, "
        "what would it be worth now?"
    )

    st.dataframe(style_money(what_if_df), use_container_width=True, hide_index=True)

    detail_buy, detail_sell, detail_combined = st.tabs(
        ["BUY/UP details", "SELL/DOWN short details", "Combined details"]
    )

    with detail_buy:
        st.dataframe(
            style_money(build_what_if_positions(tracker_df, starting_capital, "BUY")),
            use_container_width=True,
            hide_index=True,
        )

    with detail_sell:
        st.dataframe(
            style_money(build_what_if_positions(tracker_df, starting_capital, "SELL")),
            use_container_width=True,
            hide_index=True,
        )

    with detail_combined:
        st.dataframe(
            style_money(build_what_if_positions(tracker_df, starting_capital, "COMBINED")),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Tracker")
    st.dataframe(style_tracker(tracker_df), use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Accuracy by action")
        st.dataframe(style_money(build_summary(tracker_df, "Action")), use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Accuracy by direction")
        st.dataframe(style_money(build_summary(tracker_df, "Direction")), use_container_width=True, hide_index=True)

    st.subheader("Download results")
    st.download_button(
        "Download tracker CSV",
        data=tracker_df.to_csv(index=False).encode("utf-8"),
        file_name=f"manual_weekly_tracker_{monday_date}.csv",
        mime="text/csv",
    )
