from __future__ import annotations

import sqlite3
from typing import Any

from app.config import settings
from app.utils import ensure_parent, from_json, to_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    run_timestamp TEXT NOT NULL,
    forecast_window TEXT NOT NULL,
    prediction_json TEXT NOT NULL,
    debug_json TEXT,
    monday_close REAL,
    friday_close REAL,
    actual_return REAL,
    actual_direction TEXT,
    is_correct_direction INTEGER
);
"""


def connect() -> sqlite3.Connection:
    ensure_parent(settings.db_abspath)
    conn = sqlite3.connect(settings.db_abspath)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def save_run(
    ticker: str,
    run_timestamp: str,
    forecast_window: str,
    prediction: dict[str, Any],
    debug_payload: dict[str, Any],
) -> int:
    init_db()
    with connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO runs (ticker, run_timestamp, forecast_window, prediction_json, debug_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker, run_timestamp, forecast_window, to_json(prediction), to_json(debug_payload)),
        )
        conn.commit()
        return int(cur.lastrowid)


def load_recent_runs(limit: int = 20) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()

    out = []
    for row in rows:
        item = dict(row)
        item["prediction_json"] = from_json(item["prediction_json"], {})
        item["debug_json"] = from_json(item["debug_json"], {})
        out.append(item)
    return out
