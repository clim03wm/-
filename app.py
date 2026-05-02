# =========================
# ROBINHOOD-STYLE PRICE CHART
# =========================

import streamlit as st
import pandas as pd
import plotly.graph_objects as go


def make_robinhood_chart(
    chart_df,
    range_label="1D",
    time_col="timestamp",
    price_col="price",
):
    df = chart_df.copy()

    if df.empty:
        return go.Figure()

    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values(time_col)

    first_price = float(df[price_col].iloc[0])
    last_price = float(df[price_col].iloc[-1])

    change = last_price - first_price
    change_pct = (change / first_price) * 100 if first_price else 0

    is_up = change >= 0
    line_color = "#00C805" if is_up else "#FF5000"
    fill_color = "rgba(0, 200, 5, 0.08)" if is_up else "rgba(255, 80, 0, 0.08)"

    if range_label in ["LIVE", "1D"]:
        tickformat = "%-I %p"
        dtick = 60 * 60 * 1000

    elif range_label == "2D":
        tickformat = "%-I %p"
        dtick = 4 * 60 * 60 * 1000

    elif range_label == "1W":
        tickformat = "%a"
        dtick = 24 * 60 * 60 * 1000

    else:
        tickformat = "%b %-d"
        dtick = None

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df[price_col],
            mode="lines",
            line=dict(
                color=line_color,
                width=2.6,
                shape="spline",
                smoothing=0.7,
            ),
            fill="tozeroy",
            fillcolor=fill_color,
            hovertemplate=(
                "<b>$%{y:,.2f}</b><br>"
                "%{x|%b %d, %-I:%M %p}"
                "<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        height=340,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="white",
        plot_bgcolor="white",
        showlegend=False,
        hovermode="x unified",
        font=dict(
            family="Inter, Arial, sans-serif",
            color="#111827",
            size=13,
        ),
        xaxis=dict(
            showgrid=False,
            showline=False,
            zeroline=False,
            tickformat=tickformat,
            dtick=dtick,
            tickfont=dict(color="#6B7280", size=12),
            ticks="",
            fixedrange=True,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(17, 24, 39, 0.08)",
            showline=False,
            zeroline=False,
            tickfont=dict(color="#9CA3AF", size=12),
            ticks="",
            fixedrange=True,
        ),
        hoverlabel=dict(
            bgcolor="white",
            bordercolor="rgba(17, 24, 39, 0.12)",
            font=dict(color="#111827", size=13),
        ),
    )

    return fig, last_price, change, change_pct, is_up


def filter_chart_range(df, range_label, time_col="timestamp"):
    chart_df = df.copy()

    chart_df[time_col] = pd.to_datetime(chart_df[time_col])
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


st.markdown(
    """
    <style>
    .chart-card {
        background: white;
        border: 1px solid rgba(17, 24, 39, 0.07);
        border-radius: 22px;
        padding: 22px 22px 12px 22px;
        box-shadow: 0 10px 28px rgba(17, 24, 39, 0.06);
        margin-bottom: 22px;
    }

    .chart-top-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 16px;
        margin-bottom: 8px;
    }

    .chart-price {
        font-size: 38px;
        font-weight: 800;
        letter-spacing: -1.2px;
        color: #111827;
        line-height: 1;
        margin-bottom: 8px;
    }

    .chart-change-up {
        color: #00C805;
        font-size: 16px;
        font-weight: 700;
    }

    .chart-change-down {
        color: #FF5000;
        font-size: 16px;
        font-weight: 700;
    }

    div[role="radiogroup"] {
        background: #F9FAFB;
        border: 1px solid rgba(17, 24, 39, 0.06);
        padding: 4px;
        border-radius: 999px;
        width: fit-content;
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

    div[data-testid="stPlotlyChart"] {
        margin-top: -6px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================
# USE THIS PART WHERE YOUR OLD GRAPH WAS
# =========================

# Your dataframe must have:
# timestamp column = time/date
# price column = stock price/current value
#
# If your current dataframe uses different names, change these:
TIME_COL = "timestamp"
PRICE_COL = "price"

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

fig, last_price, change, change_pct, is_up = make_robinhood_chart(
    chart_df,
    range_label=range_label,
    time_col=TIME_COL,
    price_col=PRICE_COL,
)

change_class = "chart-change-up" if is_up else "chart-change-down"
change_sign = "+" if change >= 0 else ""

st.markdown('<div class="chart-card">', unsafe_allow_html=True)

st.markdown(
    f"""
    <div class="chart-top-row">
        <div>
            <div class="chart-price">${last_price:,.2f}</div>
            <div class="{change_class}">
                {change_sign}${change:,.2f} · {change_sign}{change_pct:.2f}%
            </div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.plotly_chart(
    fig,
    use_container_width=True,
    config={
        "displayModeBar": False,
        "scrollZoom": False,
    },
)

st.markdown("</div>", unsafe_allow_html=True)
