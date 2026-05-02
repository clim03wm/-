# =========================
# ROBINHOOD-STYLE PRICE CHART
# No Plotly needed
# =========================

import streamlit as st
import pandas as pd
import altair as alt


# =========================
# SETTINGS
# =========================
# Change these only if your dataframe uses different column names.
TIME_COL = "timestamp"
PRICE_COL = "price"


# =========================
# CSS
# =========================

st.markdown(
    """
    <style>
    .chart-card {
        background: white;
        border: 1px solid rgba(17, 24, 39, 0.08);
        border-radius: 22px;
        padding: 22px 22px 16px 22px;
        box-shadow: 0 10px 28px rgba(17, 24, 39, 0.06);
        margin-bottom: 24px;
    }

    .chart-price {
        font-size: 38px;
        font-weight: 800;
        letter-spacing: -1px;
        color: #111827;
        line-height: 1;
        margin-bottom: 8px;
    }

    .chart-change-up {
        color: #00C805;
        font-size: 16px;
        font-weight: 700;
        margin-bottom: 12px;
    }

    .chart-change-down {
        color: #FF5000;
        font-size: 16px;
        font-weight: 700;
        margin-bottom: 12px;
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
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================
# HELPER FUNCTIONS
# =========================

def filter_chart_range(df, range_label, time_col):
    chart_df = df.copy()

    chart_df[time_col] = pd.to_datetime(chart_df[time_col], errors="coerce")
    chart_df = chart_df.dropna(subset=[time_col])
    chart_df = chart_df.sort_values(time_col)

    if chart_df.empty:
        return chart_df

    latest_time = chart_df[time_col].max()

    if range_label == "LIVE":
        return chart_df[chart_df[time_col] >= latest_time - pd.Timedelta(hours=6)]

    if range_label == "1D":
        return chart_df[chart_df[time_col] >= latest_time - pd.Timedelta(days=1)]

    if range_label == "2D":
        return chart_df[chart_df[time_col] >= latest_time - pd.Timedelta(days=2)]

    if range_label == "1W":
        return chart_df[chart_df[time_col] >= latest_time - pd.Timedelta(days=7)]

    return chart_df


def make_clean_chart(chart_df, range_label, time_col, price_col):
    df = chart_df.copy()

    if df.empty:
        return None

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
    df = df.dropna(subset=[time_col, price_col])
    df = df.sort_values(time_col)

    if df.empty:
        return None

    first_price = float(df[price_col].iloc[0])
    last_price = float(df[price_col].iloc[-1])

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
            f"{time_col}:T",
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
            f"{price_col}:Q",
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

    points = base.mark_circle(
        color=line_color,
        size=30,
        opacity=0,
    ).encode(
        tooltip=[
            alt.Tooltip(f"{time_col}:T", title="Time"),
            alt.Tooltip(f"{price_col}:Q", title="Price", format="$,.2f"),
        ]
    )

    chart = (
        line + points
    ).properties(
        height=330
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        domain=False,
        ticks=False,
    )

    return chart, last_price, change, change_pct, is_up


# =========================
# GRAPH UI
# =========================

range_label = st.radio(
    "Chart Range",
    ["LIVE", "1D", "2D", "1W"],
    horizontal=True,
    label_visibility="collapsed",
    key="chart_range_selector",
)

chart_df = filter_chart_range(
    df,
    range_label=range_label,
    time_col=TIME_COL,
)

chart_result = make_clean_chart(
    chart_df,
    range_label=range_label,
    time_col=TIME_COL,
    price_col=PRICE_COL,
)

if chart_result is None:
    st.warning("No chart data available.")
else:
    chart, last_price, change, change_pct, is_up = chart_result

    change_class = "chart-change-up" if is_up else "chart-change-down"
    change_sign = "+" if change >= 0 else ""

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="chart-price">${last_price:,.2f}</div>
        <div class="{change_class}">
            {change_sign}${change:,.2f} · {change_sign}{change_pct:.2f}%
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)
