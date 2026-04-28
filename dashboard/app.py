from __future__ import annotations

from datetime import date
from pathlib import Path

import streamlit as st

from dashboard_utils import (
    add_tracking_columns,
    build_summary,
    build_what_if_portfolios,
    load_predictions,
    style_summary,
    style_tracker,
)
from price_utils import fetch_current_prices

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LATEST_PREDICTIONS = DATA_DIR / "latest_predictions.csv"

st.set_page_config(page_title="Weekly Stock Signal Dashboard", page_icon="📈", layout="wide")

st.title("Weekly Stock Signal Dashboard")
st.caption("Tracks model output against the Monday reference price. This is not daily percent change.")

with st.sidebar:
    st.header("Dashboard settings")
    monday_date = st.date_input("Monday reference date", value=date.today())
    starting_capital = st.number_input("What-if starting capital", min_value=100.0, value=10_000.0, step=100.0)
    include_watch = st.toggle("Include WATCH paper baskets", value=True)
    st.divider()
    st.write("Expected file:")
    st.code(str(LATEST_PREDICTIONS.relative_to(ROOT)))

pred_df = load_predictions(LATEST_PREDICTIONS)

if pred_df.empty:
    st.warning("No latest predictions found yet. Run: python scripts/run_weekly_model.py --universe custom")
    st.stop()

st.subheader("Latest model output")
st.dataframe(pred_df, use_container_width=True, hide_index=True)

if st.button("Refresh current prices", type="primary"):
    tickers = tuple(pred_df["Ticker"].dropna().astype(str).unique().tolist())
    with st.spinner("Fetching current prices..."):
        price_df = fetch_current_prices(tickers, monday_date=monday_date)
    tracker_df = add_tracking_columns(pred_df, price_df)

    valid = tracker_df[tracker_df["Correct So Far"].isin(["YES", "NO"])]
    tracked = len(valid)
    correct = int((valid["Correct So Far"] == "YES").sum()) if tracked else 0
    accuracy = correct / tracked * 100 if tracked else 0.0
    avg_change = float(valid["Change Since Monday %"].mean()) if tracked else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tracked stocks", tracked)
    c2.metric("Correct so far", correct)
    c3.metric("Accuracy so far", f"{accuracy:.1f}%")
    c4.metric("Avg change since Monday", f"{avg_change:.2f}%")

    st.subheader("Live tracker")
    st.dataframe(style_tracker(tracker_df), use_container_width=True, hide_index=True)

    st.subheader("What-if portfolio")
    portfolio_df = build_what_if_portfolios(tracker_df, starting_capital, include_watch)
    st.dataframe(style_summary(portfolio_df), use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Accuracy by action")
        st.dataframe(style_summary(build_summary(tracker_df, "Action")), use_container_width=True, hide_index=True)
    with col2:
        st.subheader("Accuracy by direction")
        st.dataframe(style_summary(build_summary(tracker_df, "Direction")), use_container_width=True, hide_index=True)

    st.subheader("Download")
    st.download_button(
        "Download tracker CSV",
        data=tracker_df.to_csv(index=False).encode("utf-8"),
        file_name=f"weekly_tracker_{monday_date}.csv",
        mime="text/csv",
    )
else:
    st.info("Click refresh to compare current prices against the Monday reference price.")
