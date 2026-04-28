from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from app.database import init_db
from app.pipeline import normalize_ticker, run_pipeline

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _deep_find_value(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = _deep_find_value(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _deep_find_value(value, key)
            if found is not None:
                return found
    return None


def _load_universe(name: str) -> list[str]:
    mapping = {
        "top50": DATA_DIR / "tickers_top50.txt",
        "custom": DATA_DIR / "tickers_custom.txt",
    }
    path = mapping[name]
    if not path.exists():
        raise FileNotFoundError(f"Missing universe file: {path}")
    return [
        normalize_ticker(line.strip())
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _setup_score(final: dict[str, Any], full_result: dict[str, Any]) -> float:
    if "fresh_setup_score" in final:
        return _safe_float(final.get("fresh_setup_score"), 0.0)
    overext = _deep_find_value(full_result, "overextension")
    if isinstance(overext, dict):
        return -_safe_float(overext.get("score_penalty"), 0.0)
    fresh = _deep_find_value(full_result, "fresh_setup")
    if isinstance(fresh, dict):
        return _safe_float(fresh.get("bullish_bonus"), 0.0) - _safe_float(fresh.get("bearish_bonus"), 0.0)
    return 0.0


def _screen_rank_score(row: dict[str, Any]) -> float:
    action = str(row.get("Action", "WATCH")).upper()
    direction = str(row.get("Direction", "NEUTRAL")).upper()
    conviction = _safe_int(row.get("Conviction"))
    raw = abs(_safe_float(row.get("Model Score")))
    exp = abs(_safe_float(row.get("Expected Move %")))
    setup = _safe_float(row.get("Setup Score"), 0.0)

    score = 0.0
    if action in {"BUY", "SELL"}:
        score += 22
    elif action == "WATCH":
        score += 8
    if direction in {"UP", "DOWN"}:
        score += 5
    score += conviction * 0.50
    score += raw * 16
    score += exp * 1.2
    score += setup * 38
    return score


def run_weekly_model(universe: str, limit: int | None, output_limit: int | None) -> pd.DataFrame:
    init_db()
    tickers = _load_universe(universe)
    if limit:
        tickers = tickers[:limit]

    rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    for idx, ticker in enumerate(tickers, start=1):
        ticker = normalize_ticker(ticker)
        print(f"[{idx}/{len(tickers)}] Running {ticker}...")
        try:
            result = run_pipeline(ticker)
            final = dict(result["final_output"])
            debug = result.get("debug", {})
            current_row = debug.get("current_row", {}) if isinstance(debug, dict) else {}

            row = {
                "Rank": 0,
                "Ticker": normalize_ticker(str(final.get("ticker", ticker))),
                "Action": str(final.get("final_action", "WATCH")).upper(),
                "Direction": str(final.get("forecast_direction", "NEUTRAL")).upper(),
                "Conviction": _safe_int(final.get("conviction_score")),
                "Edge": str(final.get("estimated_edge", "WEAK")).upper(),
                "Regime": str(final.get("primary_regime", "N/A")).upper(),
                "Model Score": _safe_float(final.get("raw_signal_score")),
                "Expected Move %": _safe_float(final.get("expected_move_pct")),
                "Setup Score": _setup_score(final, result),
                "Monday Reference Price": _safe_float(final.get("monday_close_reference"), _safe_float(current_row.get("monday_close"), 0.0)),
                "Forecast Window": final.get("forecast_window", "N/A"),
                "Run Timestamp": final.get("run_timestamp", "N/A"),
            }
            row["Screen Rank Score"] = _screen_rank_score(row)
            rows.append(row)
        except Exception as exc:
            print(f"Failed {ticker}: {exc}")
            failures.append({"Ticker": ticker, "Error": str(exc)})

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Screen Rank Score", ascending=False).reset_index(drop=True)
        if output_limit:
            df = df.head(output_limit).copy()
        df["Rank"] = range(1, len(df) + 1)

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).date().isoformat()
    latest_csv = DATA_DIR / "latest_predictions.csv"
    latest_json = DATA_DIR / "latest_predictions.json"
    history_csv = HISTORY_DIR / f"{today}_predictions.csv"
    failures_csv = HISTORY_DIR / f"{today}_failures.csv"

    df.to_csv(latest_csv, index=False)
    df.to_csv(history_csv, index=False)
    latest_json.write_text(df.to_json(orient="records", indent=2))

    if failures:
        pd.DataFrame(failures).to_csv(failures_csv, index=False)

    print(f"Saved {len(df)} rows to {latest_csv}")
    print(f"Saved history file to {history_csv}")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Run weekly model and save dashboard-ready CSV files.")
    parser.add_argument("--universe", choices=["top50", "custom"], default="custom")
    parser.add_argument("--limit", type=int, default=None, help="Limit how many tickers to run.")
    parser.add_argument("--output-limit", type=int, default=25, help="How many ranked rows to save for dashboard.")
    args = parser.parse_args()

    run_weekly_model(args.universe, args.limit, args.output_limit)


if __name__ == "__main__":
    main()
