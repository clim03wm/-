from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pandas as pd
import streamlit as st
import yfinance as yf


def yahoo_symbol(ticker: str) -> str:
    return str(ticker).strip().upper().replace(".", "-")


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [col[0] for col in df.columns]
    return df.rename(columns=str.lower)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_current_prices(tickers: tuple[str, ...], monday_date: date | None = None) -> pd.DataFrame:
    records = []
    end_date = datetime.now().date() + timedelta(days=1)

    for ticker in tickers:
        symbol = yahoo_symbol(ticker)
        current_price = None
        fallback_monday_price = None
        fallback_source = "N/A"
        error = ""

        try:
            if monday_date is not None:
                intraday = yf.download(
                    symbol,
                    start=monday_date.isoformat(),
                    end=end_date.isoformat(),
                    interval="30m",
                    auto_adjust=True,
                    progress=False,
                    prepost=False,
                    threads=False,
                )

                if intraday is not None and not intraday.empty:
                    intraday = _flatten_columns(intraday)
                    intraday.index = pd.to_datetime(intraday.index).tz_localize(None)
                    monday_rows = intraday[intraday.index.date == monday_date]
                    if not monday_rows.empty and "close" in monday_rows.columns:
                        window_start = datetime.combine(monday_date, time(11, 30))
                        window_end = datetime.combine(monday_date, time(12, 30))
                        noon_window = monday_rows[(monday_rows.index >= window_start) & (monday_rows.index <= window_end)]
                        if not noon_window.empty:
                            fallback_monday_price = float(noon_window["close"].dropna().mean())
                            fallback_source = "dashboard fallback: Monday 11:30-12:30 avg"
                        else:
                            fallback_monday_price = float(monday_rows["close"].dropna().iloc[0])
                            fallback_source = "dashboard fallback: Monday first intraday"

                    if "close" in intraday.columns and not intraday["close"].dropna().empty:
                        current_price = float(intraday["close"].dropna().iloc[-1])

            if current_price is None:
                daily = yf.download(
                    symbol,
                    period="5d",
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )
                if daily is not None and not daily.empty:
                    daily = _flatten_columns(daily)
                    if "close" in daily.columns and not daily["close"].dropna().empty:
                        current_price = float(daily["close"].dropna().iloc[-1])

        except Exception as exc:
            error = str(exc)

        records.append(
            {
                "Ticker": ticker,
                "Current Price": current_price,
                "Fallback Monday Price": fallback_monday_price,
                "Fallback Source": fallback_source,
                "Price Error": error,
            }
        )

    return pd.DataFrame(records)
