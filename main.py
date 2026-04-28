from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from app.database import init_db
from app.pipeline import normalize_ticker, run_pipeline
from app.utils import logger

LINE = "─" * 98
ALIAS_GROUPS = [frozenset({"GOOG", "GOOGL"})]


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


def _enrich_screen_row(final: dict, full_result: dict) -> dict:
    """
    Pull ranking-only diagnostics out of the full pipeline result.
    This keeps final_output unchanged while letting the screen rank better.
    """
    diagnostic_keys = [
        "fresh_setup_score",
        "prev_5d_return",
        "prev_20d_return",
        "rsi_14",
        "zscore_20",
        "overextension",
    ]

    for key in diagnostic_keys:
        if key not in final:
            value = _deep_find_value(full_result, key)
            if value is not None:
                final[key] = value

    fresh = final.get("fresh_setup_score")
    if fresh is None:
        overext = final.get("overextension")
        if isinstance(overext, dict):
            penalty = _safe_float(overext.get("score_penalty"), 0.0)
            final["fresh_setup_score"] = -penalty
        else:
            final["fresh_setup_score"] = 0.0

    return final


def _print_result(result: dict) -> None:
    r = result["final_output"]

    print()
    print(LINE)
    print(f" {r['ticker']}   {r['final_action']}   {r['forecast_direction']}")
    print(LINE)
    print(f"{'Window':26} {r.get('forecast_window', 'N/A')}")
    print(f"{'Run timestamp':26} {r.get('run_timestamp', 'N/A')}")
    print(f"{'Primary regime':26} {r.get('primary_regime', 'N/A')}")
    print(f"{'Model validity':26} {r.get('model_validity', 'N/A')}")
    print(f"{'Conviction score':26} {r.get('conviction_score', 0)}")
    print(f"{'Expected move %':26} {r.get('expected_move_pct', 0)}")
    print(f"{'Raw signal score':26} {r.get('raw_signal_score', 0)}")
    print(
        f"{'Prob UP / NEUTRAL / DOWN':26} "
        f"{_safe_float(r.get('probability_up')):.2f} / "
        f"{_safe_float(r.get('probability_neutral')):.2f} / "
        f"{_safe_float(r.get('probability_down')):.2f}"
    )

    print()
    print("Drivers")
    for item in r.get("drivers", []) or ["None"]:
        print(f"  • {item}")

    print()
    print("Conflicts")
    for item in r.get("conflicts", []) or ["None"]:
        print(f"  • {item}")

    print()
    print("Risk flags")
    for item in r.get("risk_flags", []) or ["None"]:
        print(f"  • {item}")

    print()
    print("Reason")
    print(f"  {r.get('reason', 'No reason returned.')}")
    print(LINE)
    print()


def _load_universe(name: str) -> list[str]:
    root = Path(__file__).resolve().parent / "data"

    mapping = {
        "top50": root / "tickers_top50.txt",
        "custom": root / "tickers_custom.txt",
    }

    path = mapping[name]

    if not path.exists():
        raise FileNotFoundError(f"Missing universe file: {path}")

    return [
        normalize_ticker(line.strip())
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _dedupe_alias_results(results: list[dict]) -> list[dict]:
    chosen: list[dict] = []
    used_groups: set[tuple[str, ...]] = set()
    used_tickers: set[str] = set()

    for result in results:
        ticker = normalize_ticker(result["ticker"])
        result["ticker"] = ticker

        group = next((grp for grp in ALIAS_GROUPS if ticker in grp), None)

        if group:
            group_key = tuple(sorted(group))

            if group_key in used_groups:
                continue

            group_items = [
                item
                for item in results
                if normalize_ticker(item["ticker"]) in group
            ]

            best = max(
                group_items,
                key=lambda item: (
                    _safe_int(item.get("conviction_score")),
                    abs(_safe_float(item.get("raw_signal_score"))),
                    abs(_safe_float(item.get("expected_move_pct"))),
                ),
            )

            best["ticker"] = normalize_ticker(best["ticker"])
            chosen.append(best)
            used_groups.add(group_key)
            used_tickers.add(best["ticker"])

        elif ticker not in used_tickers:
            chosen.append(result)
            used_tickers.add(ticker)

    return chosen


def _passes_display_filter(row: dict, show_all: bool = False) -> bool:
    if show_all:
        return True

    action = str(row.get("final_action", "WATCH")).upper()
    direction = str(row.get("forecast_direction", "NEUTRAL")).upper()
    edge = str(row.get("estimated_edge", "WEAK")).upper()
    conviction = _safe_int(row.get("conviction_score"))
    score = abs(_safe_float(row.get("raw_signal_score")))
    expected_move = abs(_safe_float(row.get("expected_move_pct")))
    fresh = _safe_float(row.get("fresh_setup_score"), 0.0)

    # Always show real calls.
    if action in {"BUY", "SELL"}:
        return True

    # Hide crowded continuation WATCH names.
    # This is what removes NVDA/AMD-style "already ran" names from the main screen.
    if action == "WATCH" and fresh < -0.12:
        return False

    # Show clean fresh setups first.
    if fresh >= 0.04 and direction in {"UP", "DOWN"}:
        return True

    # Keep meaningful non-BUY/SELL ideas.
    if edge in {"MODERATE", "STRONG"}:
        return True

    # Directional WATCH names need more than weak momentum.
    if direction in {"UP", "DOWN"} and conviction >= 28 and expected_move >= 1.0 and fresh >= -0.05:
        return True

    if direction in {"UP", "DOWN"} and score >= 0.30 and expected_move >= 1.5 and fresh >= 0.0:
        return True

    return False

def _setup_rank_score(row: dict) -> float:
    action = str(row.get("final_action", "WATCH")).upper()
    direction = str(row.get("forecast_direction", "NEUTRAL")).upper()

    conviction = _safe_int(row.get("conviction_score"))
    fresh = _safe_float(row.get("fresh_setup_score"), 0.0)
    raw = abs(_safe_float(row.get("raw_signal_score"), 0.0))
    expected_move_pct = abs(_safe_float(row.get("expected_move_pct"), 0.0))

    prev_5d = abs(_safe_float(row.get("prev_5d_return"), 0.0))
    prev_20d = abs(_safe_float(row.get("prev_20d_return"), 0.0))
    rsi = _safe_float(row.get("rsi_14"), 50.0)
    z20 = abs(_safe_float(row.get("zscore_20"), 0.0))

    score = 0.0

    # Real calls still rank well, but action alone no longer controls the table.
    if action in {"BUY", "SELL"}:
        score += 22
    elif action == "WATCH":
        score += 8

    if direction in {"UP", "DOWN"}:
        score += 5

    score += conviction * 0.50
    score += raw * 16
    score += expected_move_pct * 1.2
    score += fresh * 38

    # Push down crowded continuation after a stretched prior move.
    if prev_5d >= 0.10:
        score -= 12
    elif prev_5d >= 0.07:
        score -= 6

    if prev_20d >= 0.25:
        score -= 16
    elif prev_20d >= 0.16:
        score -= 8

    if rsi >= 90:
        score -= 18
    elif rsi >= 84:
        score -= 9

    if z20 >= 2.4:
        score -= 12
    elif z20 >= 1.8:
        score -= 6

    return score


def _sort_results(results: list[dict]) -> list[dict]:
    return sorted(
        results,
        key=lambda row: (
            _setup_rank_score(row),
            _safe_float(row.get("fresh_setup_score"), 0.0),
            _safe_int(row.get("conviction_score")),
            abs(_safe_float(row.get("raw_signal_score"))),
        ),
        reverse=True,
    )


def _print_screen_table(results: list[dict]) -> None:
    print()
    print(
        f"{'Rank':<4}"
        f"{'Ticker':<10}"
        f"{'Action':<10}"
        f"{'Direction':<10}"
        f"{'Conviction':<12}"
        f"{'Edge':<10}"
        f"{'Regime':<12}"
        f"{'Score':<10}"
        f"{'ExpMove%':<10}"
        f"{'Setup':<10}"
        f"{'Timestamp':<24}"
    )
    print("-" * 134)

    for i, r in enumerate(results, start=1):
        print(
            f"{i:<4}"
            f"{r.get('ticker', 'N/A'):<10}"
            f"{r.get('final_action', 'WATCH'):<10}"
            f"{r.get('forecast_direction', 'NEUTRAL'):<10}"
            f"{_safe_int(r.get('conviction_score')):<12}"
            f"{r.get('estimated_edge', 'WEAK'):<10}"
            f"{r.get('primary_regime', 'N/A'):<12}"
            f"{_safe_float(r.get('raw_signal_score')):<10.3f}"
            f"{_safe_float(r.get('expected_move_pct')):<10.2f}"
            f"{_safe_float(r.get('fresh_setup_score')):<10.3f}"
            f"{str(r.get('run_timestamp', 'N/A')):<24}"
        )

    print()


def _screen(universe: str, limit: int | None = None, show_all: bool = False) -> None:
    init_db()

    tickers = _load_universe(universe)

    if limit:
        tickers = tickers[:limit]

    total = len(tickers)

    print()
    print(f"Running fresh model on {total} tickers from universe='{universe}'...")
    print("The screen prints the actual pipeline final_output. It does not overwrite actions or directions.")
    print()

    results: list[dict] = []
    failures: list[tuple[str, str]] = []

    for idx, ticker in enumerate(tickers, start=1):
        ticker = normalize_ticker(ticker)
        print(f"[{idx}/{total}] Working on {ticker}...")

        try:
            out = run_pipeline(ticker)
            final = dict(out["final_output"])
            final = _enrich_screen_row(final, out)

            final["ticker"] = normalize_ticker(final.get("ticker", ticker))

            results.append(final)

            print(
                f"         Done: {final['ticker']} | "
                f"{final.get('final_action', 'WATCH')} | "
                f"{final.get('forecast_direction', 'NEUTRAL')} | "
                f"conviction={final.get('conviction_score', 0)} | "
                f"score={_safe_float(final.get('raw_signal_score')):.3f} | "
                f"setup={_safe_float(final.get('fresh_setup_score')):.3f} | "
                f"timestamp={final.get('run_timestamp', 'N/A')}"
            )

        except Exception as exc:
            logger.warning("Failed on %s: %s", ticker, exc)
            failures.append((ticker, str(exc)))
            print(f"         Failed: {ticker} | {exc}")

    if failures and not results:
        raise RuntimeError(f"All tickers failed. First few errors: {failures[:5]}")

    results = _dedupe_alias_results(results)
    results = _sort_results(results)

    filtered = [
        row
        for row in results
        if _passes_display_filter(row, show_all=show_all)
    ]

    if not filtered:
        print()
        print("No names passed the display filter.")
        print("Run again with --all to print every ticker.")
        if failures:
            print()
            print("Some tickers failed:")
            for ticker, err in failures[:10]:
                print(f"  • {ticker}: {err}")
        print()
        return

    _print_screen_table(filtered[:25])

    if failures:
        print("Some tickers failed:")
        for ticker, err in failures[:10]:
            print(f"  • {ticker}: {err}")
        print()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Weekly multi-agent stock signal app")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db")

    run_p = sub.add_parser("run")
    run_p.add_argument("--ticker", type=str, default=None)

    dbg_p = sub.add_parser("debug-run")
    dbg_p.add_argument("--ticker", type=str, default=None)

    screen_p = sub.add_parser("screen")
    screen_p.add_argument("--universe", choices=["top50", "custom"], default="top50")
    screen_p.add_argument("--limit", type=int, default=None)
    screen_p.add_argument(
        "--all",
        action="store_true",
        help="Print every ticker instead of only names that pass the display filter.",
    )

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "init-db":
        init_db()
        print("Database initialized.")
        return

    if args.command == "run":
        init_db()
        ticker = normalize_ticker(args.ticker or "AAPL")
        print(f"Running fresh model for {ticker}...")
        result = run_pipeline(ticker)
        print(f"Finished {ticker}.")
        _print_result(result)
        return

    if args.command == "debug-run":
        init_db()
        ticker = normalize_ticker(args.ticker or "AAPL")
        print(f"Running fresh debug model for {ticker}...")
        result = run_pipeline(ticker)
        print(f"Finished {ticker}.")
        print(json.dumps(result, indent=2, default=str))
        return

    if args.command == "screen":
        _screen(args.universe, args.limit, show_all=args.all)
        return


if __name__ == "__main__":
    main()
