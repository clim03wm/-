from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "Rank",
    "Ticker",
    "Action",
    "Direction",
    "Conviction",
    "Edge",
    "Regime",
    "Model Score",
    "Expected Move %",
    "Setup Score",
    "Monday Reference Price",
    "Forecast Window",
    "Run Timestamp",
]


def load_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    df = pd.read_csv(path)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    out = df[REQUIRED_COLUMNS].copy()
    out["Ticker"] = out["Ticker"].astype(str).str.upper()
    out["Action"] = out["Action"].astype(str).str.upper()
    out["Direction"] = out["Direction"].astype(str).str.upper()
    return out


def add_tracking_columns(pred_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    out = pred_df.merge(price_df, on="Ticker", how="left")

    missing_ref = out["Monday Reference Price"].isna() | (out["Monday Reference Price"] == 0)
    out.loc[missing_ref, "Monday Reference Price"] = out.loc[missing_ref, "Fallback Monday Price"]

    out["Change Since Monday %"] = (
        (out["Current Price"] - out["Monday Reference Price"])
        / out["Monday Reference Price"]
        * 100
    )

    def actual_direction(change: float) -> str:
        if pd.isna(change):
            return "N/A"
        if change > 0:
            return "UP"
        if change < 0:
            return "DOWN"
        return "FLAT"

    out["Actual Direction So Far"] = out["Change Since Monday %"].apply(actual_direction)

    def correct(row: pd.Series) -> str:
        pred = str(row.get("Direction", "")).upper()
        actual = str(row.get("Actual Direction So Far", "N/A")).upper()
        if actual == "N/A":
            return "N/A"
        if pred == "UP" and actual == "UP":
            return "YES"
        if pred == "DOWN" and actual == "DOWN":
            return "YES"
        if pred == "NEUTRAL" and actual == "FLAT":
            return "YES"
        return "NO"

    out["Correct So Far"] = out.apply(correct, axis=1)
    return out


def build_what_if_portfolios(df: pd.DataFrame, starting_capital: float, include_watch: bool) -> pd.DataFrame:
    strategies = [
        ("BUY all BUY/UP stocks", (df["Action"] == "BUY") & (df["Direction"] == "UP")),
        ("SHORT all SELL/DOWN stocks", (df["Action"] == "SELL") & (df["Direction"] == "DOWN")),
        (
            "Combined active calls",
            ((df["Action"] == "BUY") & (df["Direction"] == "UP"))
            | ((df["Action"] == "SELL") & (df["Direction"] == "DOWN")),
        ),
    ]
    if include_watch:
        strategies += [
            ("WATCH/UP paper basket", (df["Action"] == "WATCH") & (df["Direction"] == "UP")),
            ("WATCH/DOWN paper basket", (df["Action"] == "WATCH") & (df["Direction"] == "DOWN")),
        ]

    rows = []
    for name, mask in strategies:
        basket = df[mask].copy()
        returns: list[float] = []
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

        avg_return = sum(returns) / len(returns) if returns else None
        current_value = starting_capital * (1 + avg_return / 100) if avg_return is not None else None
        pnl = current_value - starting_capital if current_value is not None else None
        rows.append(
            {
                "Strategy": name,
                "Stocks": len(returns),
                "Starting Capital": starting_capital,
                "Current Value": current_value,
                "Dollar P/L": pnl,
                "Percent Return": avg_return,
            }
        )
    return pd.DataFrame(rows)


def build_summary(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    valid = df[df["Correct So Far"].isin(["YES", "NO"])].copy()
    if valid.empty or group_col not in valid.columns:
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


def style_summary(df: pd.DataFrame):
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
