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
12  NCLH      SELL      DOWN      50          MODERATE  EVENTFUL    -0.514    -2.80     -0.000    2026-04-27T22:55:20+00:00"""


def yahoo_symbol(ticker: str) -> str:
    return str(ticker).strip().upper().replace(".", "-")


def parse_model_output(raw_text: str) -> pd.DataFrame:
    rows = []

    for line in raw_text.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("rank"):
            continue

        parts = re.split(r"\s+", line)

        # Rank Ticker Action Direction Conviction Edge Regime Score ExpMove% Setup Timestamp
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


def build_what_if(tracker_df: pd.DataFrame, starting_capital: float, include_watch: bool) -> pd.DataFrame:
    strategies = [
        (
            "BUY all BUY/UP stocks",
            (tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"),
            "active",
        ),
        (
            "SHORT all SELL/DOWN stocks",
            (tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"),
            "active",
        ),
        (
            "Combined active calls: BUY/UP + SELL/DOWN",
            (
                ((tracker_df["Action"] == "BUY") & (tracker_df["Direction"] == "UP"))
                | ((tracker_df["Action"] == "SELL") & (tracker_df["Direction"] == "DOWN"))
            ),
            "active",
        ),
    ]

    if include_watch:
        strategies += [
            (
                "WATCH/UP paper basket",
                (tracker_df["Action"] == "WATCH") & (tracker_df["Direction"] == "UP"),
                "watch",
            ),
            (
                "WATCH/DOWN paper basket",
                (tracker_df["Action"] == "WATCH") & (tracker_df["Direction"] == "DOWN"),
                "watch",
            ),
        ]

    rows = []

    for name, mask, kind in strategies:
        basket = tracker_df[mask].copy()
        returns = []

        for _, row in basket.iterrows():
            change = row.get("Change Since Monday %")
            if pd.isna(change):
                continue

            action = str(row.get("Action", "")).upper()
            direction = str(row.get("Direction", "")).upper()

            if action in {"BUY", "WATCH"} and direction == "UP":
                returns.append(float(change))
            elif action in {"SELL", "WATCH"} and direction == "DOWN":
                returns.append(float(-change))

        if returns:
            percent_return = sum(returns) / len(returns)
            current_value = starting_capital * (1 + percent_return / 100)
            pnl = current_value - starting_capital
            stocks = len(returns)
        else:
            percent_return = None
            current_value = None
            pnl = None
            stocks = 0

        rows.append(
            {
                "Strategy": name,
                "Stocks": stocks,
                "Starting Capital": starting_capital,
                "Current Value": current_value,
                "Dollar P/L": pnl,
                "Percent Return": percent_return,
            }
        )

    return pd.DataFrame(rows)


def build_summary(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    valid = df[df["Correct So Far"].isin(["YES", "NO"])].copy()

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
        if value == "BUY":
            return "background-color: #d9ead3; font-weight: bold"
        if value == "SELL":
            return "background-color: #f4cccc; font-weight: bold"
        if value == "WATCH":
            return "background-color: #fff2cc"
        return ""

    def color_correct(value):
        if value == "YES":
            return "background-color: #d9ead3; font-weight: bold"
        if value == "NO":
            return "background-color: #f4cccc; font-weight: bold"
        return ""

    def color_return(value):
        try:
            if value > 0:
                return "color: #137333; font-weight: bold"
            if value < 0:
                return "color: #a50e0e; font-weight: bold"
        except Exception:
            pass
        return ""

    return (
        df.style
        .map(color_action, subset=["Action"])
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

    for col in ["Starting Capital", "Current Value", "Dollar P/L"]:
        if col in df.columns:
            format_map[col] = "${:,.2f}"

    for col in ["Percent Return", "Accuracy %", "Avg Change Since Monday %"]:
        if col in df.columns:
            format_map[col] = "{:.2f}%"

    def color_num(value):
        try:
            if value > 0:
                return "color: #137333; font-weight: bold"
            if value < 0:
                return "color: #a50e0e; font-weight: bold"
        except Exception:
            pass
        return ""

    styled = df.style.format(format_map, na_rep="")

    for col in ["Dollar P/L", "Percent Return", "Accuracy %", "Avg Change Since Monday %"]:
        if col in df.columns:
            styled = styled.map(color_num, subset=[col])

    return styled


st.title("Manual Weekly Stock Signal Tracker")
st.caption("Paste this week's model output. The tracker compares every stock to the Monday reference price, not daily percent change.")

with st.sidebar:
    st.header("Settings")
    monday_date = st.date_input("Monday reference date", value=date.today())
    starting_capital = st.number_input(
        "What-if starting capital",
        min_value=100.0,
        max_value=10_000_000.0,
        value=10_000.0,
        step=100.0,
    )
    include_watch = st.toggle("Include WATCH paper baskets", value=True)

raw_text = st.text_area(
    "Paste model output",
    value=DEFAULT_TEXT,
    height=320,
)

model_df = parse_model_output(raw_text)

if model_df.empty:
    st.warning("No valid rows found. Paste your model output table.")
    st.stop()

st.subheader("Parsed model output")
st.dataframe(model_df, use_container_width=True, hide_index=True)

if st.button("Fetch prices and calculate performance", type="primary"):
    tickers = tuple(model_df["Ticker"].dropna().astype(str).str.upper().unique().tolist())

    with st.spinner("Fetching Yahoo Finance prices..."):
        price_df = fetch_prices(tickers, monday_date)

    tracker_df = add_tracking_columns(model_df, price_df)

    valid = tracker_df[tracker_df["Correct So Far"].isin(["YES", "NO"])].copy()
    correct = int((valid["Correct So Far"] == "YES").sum()) if not valid.empty else 0
    total = int(len(valid))
    accuracy = correct / total * 100 if total else 0
    avg_change = float(valid["Change Since Monday %"].mean()) if total else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tracked names", total)
    c2.metric("Correct so far", correct)
    c3.metric("Accuracy so far", f"{accuracy:.1f}%")
    c4.metric("Avg change since Monday", f"{avg_change:.2f}%")

    st.subheader("Tracker")
    st.dataframe(style_tracker(tracker_df), use_container_width=True, hide_index=True)

    st.subheader("What-if portfolio")
    what_if_df = build_what_if(tracker_df, starting_capital, include_watch)
    st.dataframe(style_money(what_if_df), use_container_width=True, hide_index=True)

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
else:
    st.info("Click the button after pasting this week's model output.")
