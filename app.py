import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import altair as alt
from datetime import datetime, timedelta


# =========================
# PAGE SETUP
# =========================

st.set_page_config(
    page_title="AI Stock Signal Dashboard",
    page_icon="📈",
    layout="wide",
)


# =========================
# CSS
# =========================

st.markdown(
    """
    <style>
    .main {
        background-color: #ffffff;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }

    .top-title {
        font-size: 34px;
        font-weight: 800;
        color: #111827;
        letter-spacing: -1px;
        margin-bottom: 4px;
    }

    .sub-text {
        font-size: 15px;
        color: #6B7280;
        margin-bottom: 24px;
    }

    .chart-card {
        background: white;
        border: 1px solid rgba(17, 24, 39, 0.08);
        border-radius: 24px;
        padding: 24px 24px 16px 24px;
        box-shadow: 0 10px 28px rgba(17, 24, 39, 0.06);
        margin-bottom: 24px;
    }

    .chart-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 18px;
        margin-bottom: 8px;
    }

    .ticker-label {
        font-size: 15px;
        font-weight: 700;
        color: #6B7280;
        margin-bottom: 6px;
    }

    .chart-price {
        font-size: 42px;
        font-weight: 800;
        letter-spacing: -1.4px;
        color: #111827;
        line-height: 1;
        margin-bottom: 8px;
    }

    .chart-change-up {
        color: #00C805;
        font-size: 16px;
        font-weight: 700;
        margin-bottom: 8px;
    }

    .chart-change-down {
        color: #FF5000;
        font-size: 16px;
        font-weight: 700;
        margin-bottom: 8px;
    }

    .metric-card {
        background: white;
        border: 1px solid rgba(17, 24, 39, 0.08);
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 6px 18px rgba(17, 24, 39, 0.05);
    }

    .metric-title {
        font-size: 13px;
        color: #6B7280;
        font-weight: 700;
        margin-bottom: 6px;
    }

    .metric-value {
        font-size: 24px;
        color: #111827;
        font-weight: 800;
    }

    .buy-pill {
        background: rgba(0, 200, 5, 0.12);
        color: #008A03;
        padding: 5px 10px;
        border-radius: 999px;
        font-weight: 800;
        font-size: 13px;
    }

    .sell-pill {
        background: rgba(255, 80, 0, 0.12);
        color: #C2410C;
        padding: 5px 10px;
        border-radius: 999px;
        font-weight: 800;
        font-size: 13px;
    }

    .watch-pill {
        background: rgba(107, 114, 128, 0.14);
        color: #374151;
        padding: 5px 10px;
        border-radius: 999px;
        font-weight: 800;
        font-size: 13px;
    }

    div[role="radiogroup"] {
        background: #F9FAFB;
        border: 1px solid rgba(17, 24, 39, 0.06);
        padding: 4px;
        border-radius: 999px;
        width: fit-content;
        margin-bottom: 12px;
    }

    div[role="radiogroup"] label {
        padding: 4px 10px;
        border-radius: 999px;
        color: #6B7280;
        font-weight: 600;
    }

    div[role="radiogroup"] label:has(input:checked) {
        background: white;
        color: #00C805;
        box-shadow: 0 1px 5px rgba(17, 24, 39, 0.12);
    }

    .stDataFrame {
        border-radius: 16px;
        overflow: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================
# HELPERS
# =========================

@st.cache_data(ttl=300)
def get_price_history(ticker: str, range_label: str) -> pd.DataFrame:
    ticker = ticker.upper().strip()

    if range_label == "LIVE":
        period = "1d"
        interval = "5m"

    elif range_label == "1D":
        period = "1d"
        interval = "15m"

    elif range_label == "2D":
        period = "5d"
        interval = "30m"

    elif range_label == "1W":
        period = "7d"
        interval = "1h"

    else:
        period = "7d"
        interval = "1h"

    try:
        raw = yf.download(
            ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )

        if raw.empty:
            return pd.DataFrame()

        raw = raw.reset_index()

        if "Datetime" in raw.columns:
            time_col = "Datetime"
        elif "Date" in raw.columns:
            time_col = "Date"
        else:
            time_col = raw.columns[0]

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [
                col[0] if isinstance(col, tuple) else col
                for col in raw.columns
            ]

        df = raw[[time_col, "Close"]].copy()
        df.columns = ["timestamp", "price"]

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df.dropna(subset=["timestamp", "price"])
        df = df.sort_values("timestamp")

        if range_label == "2D":
            latest = df["timestamp"].max()
            df = df[df["timestamp"] >= latest - pd.Timedelta(days=2)]

        if range_label == "1W":
            latest = df["timestamp"].max()
            df = df[df["timestamp"] >= latest - pd.Timedelta(days=7)]

        return df

    except Exception:
        return pd.DataFrame()


def make_robinhood_chart(df: pd.DataFrame, range_label: str):
    if df.empty:
        return None

    first_price = float(df["price"].iloc[0])
    last_price = float(df["price"].iloc[-1])

    change = last_price - first_price
    change_pct = (change / first_price) * 100 if first_price else 0

    is_up = change >= 0
    line_color = "#00C805" if is_up else "#FF5000"

    if range_label == "LIVE":
        label_format = "%-I %p"
        tick_count = 6

    elif range_label == "1D":
        label_format = "%-I %p"
        tick_count = 8

    elif range_label == "2D":
        label_format = "%-I %p"
        tick_count = 7

    elif range_label == "1W":
        label_format = "%a"
        tick_count = 5

    else:
        label_format = "%b %-d"
        tick_count = 6

    base = alt.Chart(df).encode(
        x=alt.X(
            "timestamp:T",
            axis=alt.Axis(
                title=None,
                format=label_format,
                grid=False,
                labelColor="#6B7280",
                labelFontSize=12,
                tickCount=tick_count,
            ),
        ),
        y=alt.Y(
            "price:Q",
            axis=alt.Axis(
                title=None,
                grid=True,
                gridColor="rgba(17, 24, 39, 0.08)",
                labelColor="#9CA3AF",
                labelFontSize=12,
            ),
            scale=alt.Scale(zero=False),
        ),
    )

    line = base.mark_line(
        color=line_color,
        strokeWidth=3,
        interpolate="monotone",
    )

    hover = base.mark_circle(
        color=line_color,
        size=45,
        opacity=0,
    ).encode(
        tooltip=[
            alt.Tooltip("timestamp:T", title="Time"),
            alt.Tooltip("price:Q", title="Price", format="$,.2f"),
        ]
    )

    chart = (
        line + hover
    ).properties(
        height=340
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        domain=False,
        ticks=False,
    )

    return chart, last_price, change, change_pct, is_up


def action_pill(action):
    action = str(action).upper()

    if action == "BUY":
        return '<span class="buy-pill">BUY</span>'

    if action == "SELL":
        return '<span class="sell-pill">SELL</span>'

    return '<span class="watch-pill">WATCH</span>'


def make_demo_predictions():
    data = [
        {
            "Ticker": "NVDA",
            "Action": "WATCH",
            "Direction": "UP",
            "Conviction": 64,
            "Regime": "EVENTFUL",
            "Monday Price": 111.50,
            "Current Price": 116.20,
        },
        {
            "Ticker": "TSLA",
            "Action": "SELL",
            "Direction": "DOWN",
            "Conviction": 72,
            "Regime": "EVENTFUL",
            "Monday Price": 251.80,
            "Current Price": 244.10,
        },
        {
            "Ticker": "AAPL",
            "Action": "WATCH",
            "Direction": "NEUTRAL",
            "Conviction": 49,
            "Regime": "NORMAL",
            "Monday Price": 182.40,
            "Current Price": 183.10,
        },
        {
            "Ticker": "MSFT",
            "Action": "BUY",
            "Direction": "UP",
            "Conviction": 69,
            "Regime": "NORMAL",
            "Monday Price": 412.30,
            "Current Price": 419.80,
        },
        {
            "Ticker": "AMD",
            "Action": "WATCH",
            "Direction": "UP",
            "Conviction": 57,
            "Regime": "EVENTFUL",
            "Monday Price": 143.20,
            "Current Price": 145.40,
        },
    ]

    df = pd.DataFrame(data)
    df["$ Change"] = df["Current Price"] - df["Monday Price"]
    df["% Change"] = (df["$ Change"] / df["Monday Price"]) * 100

    return df


def style_prediction_table(df):
    styled = df.copy()

    styled["Action"] = styled["Action"].apply(
        lambda x: f"🟢 BUY" if x == "BUY" else f"🔴 SELL" if x == "SELL" else "⚪ WATCH"
    )

    styled["$ Change"] = styled["$ Change"].map("${:,.2f}".format)
    styled["% Change"] = styled["% Change"].map("{:+.2f}%".format)
    styled["Monday Price"] = styled["Monday Price"].map("${:,.2f}".format)
    styled["Current Price"] = styled["Current Price"].map("${:,.2f}".format)

    return styled


# =========================
# HEADER
# =========================

st.markdown('<div class="top-title">AI Stock Signal Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-text">Monday close to Friday close model tracker with cleaner Robinhood-style charts.</div>',
    unsafe_allow_html=True,
)


# =========================
# SIDEBAR
# =========================

with st.sidebar:
    st.header("Controls")

    ticker = st.text_input(
        "Ticker",
        value="AAPL",
        placeholder="AAPL",
    ).upper().strip()

    st.caption("Chart ranges are simplified so the graph stays clean.")

    st.divider()

    st.caption("LIVE = hourly look")
    st.caption("1D = each hour")
    st.caption("2D = every 4 hours")
    st.caption("1W = one label per day")


# =========================
# MAIN CHART
# =========================

range_label = st.radio(
    "Chart Range",
    ["LIVE", "1D", "2D", "1W"],
    horizontal=True,
    label_visibility="collapsed",
    key="chart_range_selector",
)

price_df = get_price_history(ticker, range_label)
chart_result = make_robinhood_chart(price_df, range_label)

if chart_result is None:
    st.warning("No chart data found. Try another ticker.")
else:
    chart, last_price, change, change_pct, is_up = chart_result

    change_class = "chart-change-up" if is_up else "chart-change-down"
    change_sign = "+" if change >= 0 else ""

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="chart-header">
            <div>
                <div class="ticker-label">{ticker}</div>
                <div class="chart-price">${last_price:,.2f}</div>
                <div class="{change_class}">
                    {change_sign}${change:,.2f} · {change_sign}{change_pct:.2f}%
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# MODEL SIGNAL SUMMARY
# =========================

predictions_df = make_demo_predictions()

selected_row = predictions_df[predictions_df["Ticker"] == ticker]

if selected_row.empty:
    selected_signal = {
        "Action": "WATCH",
        "Direction": "NEUTRAL",
        "Conviction": 50,
        "Regime": "NORMAL",
    }
else:
    selected_signal = selected_row.iloc[0].to_dict()


col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Model Action</div>
            <div class="metric-value">{selected_signal["Action"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Direction</div>
            <div class="metric-value">{selected_signal["Direction"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Conviction</div>
            <div class="metric-value">{selected_signal["Conviction"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col4:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">Regime</div>
            <div class="metric-value">{selected_signal["Regime"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================
# TABLE
# =========================

st.subheader("Model Prediction Tracker")

table_df = style_prediction_table(predictions_df)

st.dataframe(
    table_df,
    use_container_width=True,
    hide_index=True,
)


# =========================
# WHAT IF TABLE
# =========================

st.subheader("Per-Stock What If Result")

what_if_df = predictions_df.copy()

what_if_df["Trade Type"] = np.where(
    what_if_df["Action"] == "SELL",
    "Short",
    np.where(what_if_df["Action"] == "BUY", "Long", "No Trade"),
)

what_if_df["Dollar Result Per Share"] = np.where(
    what_if_df["Trade Type"] == "Long",
    what_if_df["Current Price"] - what_if_df["Monday Price"],
    np.where(
        what_if_df["Trade Type"] == "Short",
        what_if_df["Monday Price"] - what_if_df["Current Price"],
        0,
    ),
)

what_if_df["Return %"] = np.where(
    what_if_df["Trade Type"] == "Long",
    ((what_if_df["Current Price"] - what_if_df["Monday Price"]) / what_if_df["Monday Price"]) * 100,
    np.where(
        what_if_df["Trade Type"] == "Short",
        ((what_if_df["Monday Price"] - what_if_df["Current Price"]) / what_if_df["Monday Price"]) * 100,
        0,
    ),
)

what_if_display = what_if_df[
    [
        "Ticker",
        "Action",
        "Trade Type",
        "Monday Price",
        "Current Price",
        "Dollar Result Per Share",
        "Return %",
    ]
].copy()

what_if_display["Monday Price"] = what_if_display["Monday Price"].map("${:,.2f}".format)
what_if_display["Current Price"] = what_if_display["Current Price"].map("${:,.2f}".format)
what_if_display["Dollar Result Per Share"] = what_if_display["Dollar Result Per Share"].map("${:+,.2f}".format)
what_if_display["Return %"] = what_if_display["Return %"].map("{:+.2f}%".format)

st.dataframe(
    what_if_display,
    use_container_width=True,
    hide_index=True,
)


# =========================
# FOOTER
# =========================

st.caption(
    "This dashboard is for tracking model output only. It is not financial advice."
)
