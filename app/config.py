from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")


def _bool(name: str, default: str) -> bool:
    return os.getenv(name, default).lower() == "true"


@dataclass(frozen=True)
class Settings:
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-flash-latest")
    gemini_api_version: str = os.getenv("GEMINI_API_VERSION", "v1beta")
    use_gemini_text_agents: bool = _bool("USE_GEMINI_TEXT_AGENTS", "true")

    default_ticker: str = os.getenv("DEFAULT_TICKER", "AAPL")
    database_path: str = os.getenv("DATABASE_PATH", "data/stock_signals.db")
    log_level: str = os.getenv("LOG_LEVEL", "WARNING").upper()

    training_years: int = int(os.getenv("TRAINING_YEARS", "8"))
    neutral_band: float = float(os.getenv("NEUTRAL_BAND", "0.006"))
    min_training_rows: int = int(os.getenv("MIN_TRAINING_ROWS", "60"))

    news_enabled: bool = _bool("USE_NEWS_MODIFIER", "true")
    polymarket_enabled: bool = _bool("USE_POLYMARKET_MODIFIER", "true")
    truthsocial_enabled: bool = _bool("USE_TRUTHSOCIAL_MODIFIER", "true")
    sector_readthrough_enabled: bool = _bool("USE_SECTOR_READTHROUGH_MODIFIER", "true")

    news_weight: float = float(os.getenv("NEWS_WEIGHT", "0.22"))
    polymarket_weight: float = float(os.getenv("POLYMARKET_WEIGHT", "0.08"))
    truthsocial_weight: float = float(os.getenv("TRUTHSOCIAL_WEIGHT", "0.06"))
    sector_readthrough_weight: float = float(os.getenv("SECTOR_READTHROUGH_WEIGHT", "0.20"))

    buy_score_min: float = float(os.getenv("BUY_SCORE_MIN", "0.18"))
    sell_score_min: float = float(os.getenv("SELL_SCORE_MIN", "0.18"))
    neutral_score_band: float = float(os.getenv("NEUTRAL_SCORE_BAND", "0.07"))
    buy_expected_move_min: float = float(os.getenv("BUY_EXPECTED_MOVE_MIN", "0.006"))
    sell_expected_move_min: float = float(os.getenv("SELL_EXPECTED_MOVE_MIN", "0.006"))

    screen_buy_quantile: float = float(os.getenv("SCREEN_BUY_QUANTILE", "0.82"))
    screen_sell_quantile: float = float(os.getenv("SCREEN_SELL_QUANTILE", "0.18"))
    screen_min_abs_score: float = float(os.getenv("SCREEN_MIN_ABS_SCORE", "0.07"))

    @property
    def db_abspath(self) -> Path:
        path = Path(self.database_path)
        if path.is_absolute():
            return path
        return ROOT_DIR / path


settings = Settings()