from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path
import json
import re

import pandas as pd
import streamlit as st
import yfinance as yf


st.set_page_config(
    page_title="Manual Weekly Stock Signal Tracker",
    page_icon="📈",
    layout="wide",
)


DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

ARCHIVE_FILE = DATA_DIR / "weekly_archive.csv"
NOTES_FILE = DATA_DIR / "weekly_notes.json"
PUT_TRACKER_FILE = DATA_DIR / "put_tracker.csv"


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


SECTOR_MAP = {
    "PHM": "Housing", "RCL": "Travel", "NCLH": "Travel", "EXPE": "Travel", "BA": "Industrials",
    "FCX": "Materials", "XOM": "Energy", "TSLA": "Auto/EV", "VLTO": "Industrials",
    "BLK": "Financials", "MCO": "Financials", "COF": "Financials", "AXP": "Financials", "RF": "Financials",
    "SMCI": "Technology", "VST": "Utilities/Energy", "CARR": "Industrials", "MSCI": "Financial Data",
    "APTV": "Auto Suppliers", "EL": "Consumer", "ALLE": "Industrials", "DOV": "Industrials",
    "ARE": "Real Estate", "ROK": "Industrials", "NFLX": "Communication Services",
}


def this_weeks_monday(today: date | None = None) -> date:
    today = today or date.today()
    return today - timedelta(days=today.weekday())


def yahoo_symbol(ticker: str) -> str:
    return str(ticker).strip().upper().replace(".", "-")


def sector_for_ticker(ticker: str) -> str:
    return SECTOR_MAP.get(str(ticker).upper(), "Other")


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
                    "Sector": sector_for_ticker(parts[1].upper()),
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
    out["Position Return %"] = out.apply(position_return_pct, axis=1)
    out["Risk Flags"] = out.apply(make_risk_flags, axis=1)
    return out


def position_return_pct(row: pd.Series) -> float | None:
    change = row.get("Change Since Monday %")
    if pd.isna(change):
        return None

    action = str(row.get("Action", "")).upper()
    direction = str(row.get("Direction", "")).upper()

    if action == "BUY" and direction == "UP":
        return float(change)

    if action == "SELL" and direction == "DOWN":
        return float(-change)

    return None


def make_risk_flags(row: pd.Series) -> str:
    flags = []

    action = str(row.get("Action", "")).upper()
    edge = str(row.get("Edge", "")).upper()
    regime = str(row.get("Regime", "")).upper()

    conviction = row.get("Conviction")
    expected_move = row.get("Expected Move %")
    change = row.get("Change Since Monday %")
    setup = row.get("Setup Score")

    try:
        if pd.notna(conviction) and int(conviction) >= 80:
            flags.append("High conviction")
        elif pd.notna(conviction) and int(conviction) < 50 and action in {"BUY", "SELL"}:
            flags.append("Lower conviction")
    except Exception:
        pass

    try:
        if pd.notna(expected_move) and abs(float(expected_move)) >= 5:
            flags.append("Large expected move")
    except Exception:
        pass

    try:
        if pd.notna(change) and abs(float(change)) >= 3:
            flags.append("Large live move")
    except Exception:
        pass

    try:
        if pd.notna(setup) and float(setup) < -0.10:
            flags.append("Crowded setup")
    except Exception:
        pass

    if regime == "EVENTFUL":
        flags.append("Eventful")
    if edge == "WEAK" and action in {"BUY", "SELL"}:
        flags.append("Weak edge")

    return ", ".join(flags) if flags else "None"


def build_what_if(tracker_df: pd.DataFrame) -> pd.DataFrame:
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
                "Ticker", "Position", "Direction", "Sector", "Monday Price", "Current Price",
                "Shares / Shorted Shares", "Stock Move %", "Position Return %",
                "Dollar P/L", "Correct So Far", "Risk Flags",
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
                "Sector": row.get("Sector", "Other"),
                "Monday Price": monday_price,
                "Current Price": current_price,
                "Shares / Shorted Shares": shares,
                "Stock Move %": stock_move,
                "Position Return %": pos_return,
                "Dollar P/L": pnl,
                "Correct So Far": row.get("Correct So Far"),
                "Risk Flags": row.get("Risk Flags"),
            }
        )

    return pd.DataFrame(rows)


def build_conviction_summary(df: pd.DataFrame) -> pd.DataFrame:
    active = df[df["Action"].isin(["BUY", "SELL"])].copy()
    valid = active[active["Correct So Far"].isin(["YES", "NO"])].copy()

    if valid.empty:
        return pd.DataFrame(
            columns=["Conviction Bucket", "Stocks", "Correct", "Accuracy %", "Avg Position Return %"]
        )

    valid["Correct Flag"] = (valid["Correct So Far"] == "YES").astype(int)

    def bucket(value):
        try:
            value = int(value)
        except Exception:
            return "Unknown"

        if value >= 80:
            return "80+"
        if value >= 60:
            return "60-79"
        if value >= 45:
            return "45-59"
        return "Below 45"

    valid["Conviction Bucket"] = valid["Conviction"].apply(bucket)

    out = (
        valid.groupby("Conviction Bucket")
        .agg(
            Stocks=("Ticker", "count"),
            Correct=("Correct Flag", "sum"),
            **{"Avg Position Return %": ("Position Return %", "mean")},
        )
        .reset_index()
    )

    out["Accuracy %"] = out["Correct"] / out["Stocks"] * 100

    order = ["80+", "60-79", "45-59", "Below 45", "Unknown"]
    out["Order"] = out["Conviction Bucket"].apply(lambda x: order.index(x) if x in order else 99)
    out = out.sort_values("Order").drop(columns=["Order"])

    return out[["Conviction Bucket", "Stocks", "Correct", "Accuracy %", "Avg Position Return %"]]


def build_sector_summary(df: pd.DataFrame) -> pd.DataFrame:
    active = df[df["Action"].isin(["BUY", "SELL"])].copy()
    valid = active[active["Correct So Far"].isin(["YES", "NO"])].copy()

    if valid.empty:
        return pd.DataFrame(columns=["Sector", "Stocks", "Correct", "Accuracy %", "Avg Position Return %"])

    valid["Correct Flag"] = (valid["Correct So Far"] == "YES").astype(int)

    out = (
        valid.groupby("Sector")
        .agg(
            Stocks=("Ticker", "count"),
            Correct=("Correct Flag", "sum"),
            **{"Avg Position Return %": ("Position Return %", "mean")},
        )
        .reset_index()
    )

    out["Accuracy %"] = out["Correct"] / out["Stocks"] * 100
    return out[["Sector", "Stocks", "Correct", "Accuracy %", "Avg Position Return %"]].sort_values(
        ["Stocks", "Accuracy %"], ascending=[False, False]
    )


def build_best_worst(df: pd.DataFrame) -> pd.DataFrame:
    active = df[
        df["Action"].isin(["BUY", "SELL"])
        & df["Position Return %"].notna()
    ].copy()

    if active.empty:
        return pd.DataFrame(columns=["Category", "Ticker", "Action", "Direction", "Position Return %", "Dollar P/L"])

    active["Dollar P/L 1 Share"] = active.apply(
        lambda r: (r["Current Price"] - r["Monday Reference Price"])
        if r["Action"] == "BUY"
        else (r["Monday Reference Price"] - r["Current Price"]),
        axis=1,
    )

    rows = []

    best = active.sort_values("Position Return %", ascending=False).iloc[0]
    worst = active.sort_values("Position Return %", ascending=True).iloc[0]

    rows.append({
        "Category": "Best call",
        "Ticker": best["Ticker"],
        "Action": best["Action"],
        "Direction": best["Direction"],
        "Position Return %": best["Position Return %"],
        "Dollar P/L": best["Dollar P/L 1 Share"],
    })

    rows.append({
        "Category": "Worst call",
        "Ticker": worst["Ticker"],
        "Action": worst["Action"],
        "Direction": worst["Direction"],
        "Position Return %": worst["Position Return %"],
        "Dollar P/L": worst["Dollar P/L 1 Share"],
    })

    for label, action in [("Best BUY", "BUY"), ("Worst BUY", "BUY"), ("Best SELL", "SELL"), ("Worst SELL", "SELL")]:
        sub = active[active["Action"] == action]
        if sub.empty:
            continue

        row = sub.sort_values("Position Return %", ascending=("Worst" in label)).iloc[0]

        rows.append({
            "Category": label,
            "Ticker": row["Ticker"],
            "Action": row["Action"],
            "Direction": row["Direction"],
            "Position Return %": row["Position Return %"],
            "Dollar P/L": row["Dollar P/L 1 Share"],
        })

    return pd.DataFrame(rows)


def build_spy_comparison(tracker_df: pd.DataFrame, monday_date: date) -> pd.DataFrame:
    what_if_df = build_what_if(tracker_df)
    combined = what_if_df[what_if_df["Strategy"].str.contains("Combined active", na=False)]

    model_return = None
    if not combined.empty:
        model_return = combined["Percent Return"].iloc[0]

    spy_prices = fetch_prices(("SPY",), monday_date)
    spy_return = None

    if not spy_prices.empty:
        row = spy_prices.iloc[0]
        if pd.notna(row["Monday Reference Price"]) and pd.notna(row["Current Price"]):
            spy_return = (row["Current Price"] - row["Monday Reference Price"]) / row["Monday Reference Price"] * 100

    out = pd.DataFrame(
        [
            {"Benchmark": "Model combined active basket", "Return %": model_return},
            {"Benchmark": "SPY buy-and-hold", "Return %": spy_return},
        ]
    )

    if model_return is not None and spy_return is not None:
        out.loc[len(out)] = {"Benchmark": "Model minus SPY", "Return %": model_return - spy_return}

    return out


def load_notes() -> dict:
    if NOTES_FILE.exists():
        try:
            return json.loads(NOTES_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_notes(notes: dict) -> None:
    NOTES_FILE.write_text(json.dumps(notes, indent=2))


def load_archive() -> pd.DataFrame:
    if ARCHIVE_FILE.exists():
        try:
            return pd.read_csv(ARCHIVE_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def save_week_to_archive(tracker_df: pd.DataFrame, week_start: date, note: str) -> None:
    archive = load_archive()
    to_save = tracker_df.copy()
    to_save["Week Start"] = week_start.isoformat()
    to_save["Saved At"] = datetime.now().isoformat(timespec="seconds")
    to_save["Weekly Note"] = note

    if not archive.empty:
        archive = archive[archive["Week Start"] != week_start.isoformat()]
        combined = pd.concat([archive, to_save], ignore_index=True)
    else:
        combined = to_save

    combined.to_csv(ARCHIVE_FILE, index=False)


def build_win_rate_over_time(archive: pd.DataFrame) -> pd.DataFrame:
    if archive.empty or "Week Start" not in archive.columns:
        return pd.DataFrame()

    active = archive[
        archive["Action"].isin(["BUY", "SELL"])
        & archive["Correct So Far"].isin(["YES", "NO"])
    ].copy()

    if active.empty:
        return pd.DataFrame()

    active["Correct Flag"] = (active["Correct So Far"] == "YES").astype(int)

    out = (
        active.groupby("Week Start")
        .agg(
            Stocks=("Ticker", "count"),
            Correct=("Correct Flag", "sum"),
            **{"Avg Position Return %": ("Position Return %", "mean")},
        )
        .reset_index()
    )

    out["Accuracy %"] = out["Correct"] / out["Stocks"] * 100
    return out.sort_values("Week Start")


def load_put_tracker() -> pd.DataFrame:
    if PUT_TRACKER_FILE.exists():
        try:
            return pd.read_csv(PUT_TRACKER_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def save_put_trade(row: dict) -> None:
    existing = load_put_tracker()
    new = pd.DataFrame([row])
    combined = pd.concat([existing, new], ignore_index=True) if not existing.empty else new
    combined.to_csv(PUT_TRACKER_FILE, index=False)


def build_short_calculator_table(tracker_df: pd.DataFrame) -> pd.DataFrame:
    return tracker_df[
        (tracker_df["Action"] == "SELL")
        & (tracker_df["Direction"] == "DOWN")
        & (tracker_df["Monday Reference Price"].notna())
        & (tracker_df["Current Price"].notna())
    ].copy()


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
                "Position Return %": "{:.2f}%",
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
        "Starting Capital", "Current Value", "Dollar P/L", "Monday Price",
        "Current Price", "Monday Entry Value", "Current Position Value",
        "Total Cost", "Current Option Value", "Target Option Value",
    ]:
        if col in df.columns:
            format_map[col] = "${:,.2f}"

    for col in [
        "Percent Return", "Accuracy %", "Avg Change Since Monday %",
        "Stock Move %", "Position Return %", "Avg Position Return %",
        "Return %",
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
        "Dollar P/L", "Percent Return", "Accuracy %", "Avg Change Since Monday %",
        "Stock Move %", "Position Return %", "Avg Position Return %", "Return %",
    ]:
        if col in df.columns:
            styled = styled.map(color_num, subset=[col])

    return styled


st.title("Manual Weekly Stock Signal Tracker")
st.caption("Tracks this week's model output against this week's Monday reference price, not daily percent change.")

if "raw_model_output" not in st.session_state:
    st.session_state["raw_model_output"] = DEFAULT_TEXT

tab_dashboard, tab_input, tab_archive, tab_notes, tab_puts = st.tabs(
    ["Dashboard", "Paste model output", "Archive", "Weekly notes", "Put tracker"]
)

with tab_input:
    st.session_state["raw_model_output"] = st.text_area(
        "Paste model output",
        value=st.session_state["raw_model_output"],
        height=360,
    )
    st.info("After editing this box, go back to the Dashboard tab. The tracker updates automatically.")

model_df = parse_model_output(st.session_state["raw_model_output"])
monday_date = this_weeks_monday()

if model_df.empty:
    with tab_dashboard:
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

what_if_df = build_what_if(tracker_df)
combined_row = what_if_df[what_if_df["Strategy"].str.contains("Combined active", na=False)]

if not combined_row.empty and pd.notna(combined_row["Dollar P/L"].iloc[0]):
    combined_pnl = float(combined_row["Dollar P/L"].iloc[0])
    combined_return = float(combined_row["Percent Return"].iloc[0])
else:
    combined_pnl = 0.0
    combined_return = 0.0

active_valid = tracker_df[
    (tracker_df["Action"].isin(["BUY", "SELL"]))
    & (tracker_df["Correct So Far"].isin(["YES", "NO"]))
].copy()

active_correct = int((active_valid["Correct So Far"] == "YES").sum()) if not active_valid.empty else 0
active_total = int(len(active_valid))
active_accuracy = active_correct / active_total * 100 if active_total else 0

with tab_dashboard:
    st.caption(f"Reference period: this week's Monday ({monday_date}) to now.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active calls", active_total)
    c2.metric("Active correct", active_correct)
    c3.metric("Active accuracy", f"{active_accuracy:.1f}%")
    c4.metric("1-share P/L", f"${combined_pnl:,.2f}", f"{combined_return:.2f}%")

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

    st.subheader("Best and worst calls")
    st.dataframe(style_money(build_best_worst(tracker_df)), use_container_width=True, hide_index=True)

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
        calc_tab_short, calc_tab_put = st.tabs(["Paper short calculator", "Put option calculator"])

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
            calc_cols[1].metric("P/L if covered now", f"${current_pnl:,.2f}", f"{current_return_pct:.2f}%")
            calc_cols[2].metric("P/L at target cover", f"${target_pnl:,.2f}", f"{target_return_pct:.2f}%")
            calc_cols[3].metric("Target cover price", f"${target_cover_price:,.2f}")

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
            put_metrics[3].metric("P/L now", f"${current_pnl:,.2f}", f"{current_return_pct:.2f}%")
            put_metrics[4].metric("P/L at target", f"${target_pnl:,.2f}", f"{target_return_pct:.2f}%")

            save_col, _ = st.columns([1, 3])
            with save_col:
                if st.button("Save put trade", key="save_put_trade"):
                    save_put_trade(
                        {
                            "Saved At": datetime.now().isoformat(timespec="seconds"),
                            "Week Start": monday_date.isoformat(),
                            "Ticker": selected_put_ticker,
                            "Contracts": contracts,
                            "Premium Paid": premium_paid,
                            "Current Premium": current_premium,
                            "Target Premium": target_premium,
                            "Total Cost": total_cost,
                            "Current Option Value": current_value,
                            "Target Option Value": target_value,
                            "P/L Now": current_pnl,
                            "P/L At Target": target_pnl,
                        }
                    )
                    st.success("Put trade saved.")

    st.subheader("Tracker")
    st.dataframe(style_tracker(tracker_df), use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Accuracy by conviction")
        st.dataframe(style_money(build_conviction_summary(tracker_df)), use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Sector grouping")
        st.dataframe(style_money(build_sector_summary(tracker_df)), use_container_width=True, hide_index=True)

    st.subheader("Model vs SPY")
    st.dataframe(style_money(build_spy_comparison(tracker_df, monday_date)), use_container_width=True, hide_index=True)

    st.subheader("Download results")
    st.download_button(
        "Download tracker CSV",
        data=tracker_df.to_csv(index=False).encode("utf-8"),
        file_name=f"manual_weekly_tracker_{monday_date}.csv",
        mime="text/csv",
    )

with tab_archive:
    st.subheader("Weekly result archive")

    notes = load_notes()
    current_note = notes.get(monday_date.isoformat(), "")

    if st.button("Save this week to archive"):
        save_week_to_archive(tracker_df, monday_date, current_note)
        st.success("Saved this week to archive.")

    archive_df = load_archive()

    if archive_df.empty:
        st.info("No archived weeks yet.")
    else:
        win_df = build_win_rate_over_time(archive_df)

        if not win_df.empty:
            st.subheader("Win rate over time")
            st.line_chart(win_df.set_index("Week Start")[["Accuracy %", "Avg Position Return %"]])

        st.subheader("Archived rows")
        st.dataframe(archive_df, use_container_width=True, hide_index=True)

        st.download_button(
            "Download archive CSV",
            data=archive_df.to_csv(index=False).encode("utf-8"),
            file_name="weekly_archive.csv",
            mime="text/csv",
        )

with tab_notes:
    st.subheader("Weekly notes")

    notes = load_notes()
    week_key = monday_date.isoformat()

    note_text = st.text_area(
        f"Note for week starting {week_key}",
        value=notes.get(week_key, ""),
        height=220,
    )

    if st.button("Save note"):
        notes[week_key] = note_text
        save_notes(notes)
        st.success("Note saved.")

    st.subheader("Saved notes")
    if notes:
        notes_df = pd.DataFrame(
            [{"Week Start": k, "Note": v} for k, v in sorted(notes.items(), reverse=True)]
        )
        st.dataframe(notes_df, use_container_width=True, hide_index=True)
    else:
        st.info("No saved notes yet.")

with tab_puts:
    st.subheader("Saved put option tracker")

    put_df = load_put_tracker()

    if put_df.empty:
        st.info("No saved put trades yet. Save one from the Put option calculator on the Dashboard.")
    else:
        st.dataframe(style_money(put_df), use_container_width=True, hide_index=True)

        st.download_button(
            "Download put tracker CSV",
            data=put_df.to_csv(index=False).encode("utf-8"),
            file_name="put_tracker.csv",
            mime="text/csv",
        )
