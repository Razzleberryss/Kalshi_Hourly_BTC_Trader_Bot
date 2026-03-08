"""
config.py — Centralized configuration for the Kalshi BTC Hourly Trader Bot.

All tunable parameters are sourced from environment variables (loaded via .env)
with safe defaults. Call config.validate() before any API calls or trading logic.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env file early so os.getenv picks up values
load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API / Connection
# ---------------------------------------------------------------------------
# Base URL is hardcoded per spec — never sourced from env to prevent misconfig.
# Kalshi uses the same REST API v2 endpoint for both demo and prod accounts;
# the difference lies in the API key used, not the URL.
KALSHI_BASE_URL: str = "https://api.elections.kalshi.com/trade-api/v2"

KALSHI_ENV: str = os.getenv("KALSHI_ENV", "demo")  # "demo" or "prod"

# Credentials (required — no defaults; validate() will catch missing values)
KALSHI_API_KEY_ID: str = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH: str = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")

# BTC Hourly series ticker — verify via API; likely "KXBTCH"
BTC_SERIES_TICKER_HOURLY: str = os.getenv("BTC_SERIES_TICKER_HOURLY", "KXBTCH")

# ---------------------------------------------------------------------------
# Trading mode
# ---------------------------------------------------------------------------
DRY_RUN: bool = os.getenv("DRY_RUN", "true").strip().lower() not in ("false", "0", "no")

# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------
STOP_LOSS_CENTS: int = int(os.getenv("STOP_LOSS_CENTS", "35"))
TAKE_PROFIT_CENTS: int = int(os.getenv("TAKE_PROFIT_CENTS", "22"))
MAX_HOLD_MINUTES: int = int(os.getenv("MAX_HOLD_MINUTES", "50"))

MAX_DOLLARS_PER_TRADE: int = int(os.getenv("MAX_DOLLARS_PER_TRADE", "25"))
MAX_OPEN_POSITIONS: int = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
MAX_TOTAL_EXPOSURE_CENTS: int = int(os.getenv("MAX_TOTAL_EXPOSURE_CENTS", "7500"))

MAX_DAILY_LOSS_CENTS: int = int(os.getenv("MAX_DAILY_LOSS_CENTS", "5000"))
MAX_DAILY_TRADES: int = int(os.getenv("MAX_DAILY_TRADES", "20"))

# ---------------------------------------------------------------------------
# Strategy parameters
# ---------------------------------------------------------------------------
MIN_CONFIDENCE: float = float(os.getenv("MIN_CONFIDENCE", "0.12"))
MIN_EDGE_CENTS: int = int(os.getenv("MIN_EDGE_CENTS", "3"))
MAX_SPREAD_CENTS: int = int(os.getenv("MAX_SPREAD_CENTS", "5"))
LOOKBACK_HOURS: int = int(os.getenv("LOOKBACK_HOURS", "3"))
TIME_TO_EXPIRY_MIN_MINUTES: int = int(os.getenv("TIME_TO_EXPIRY_MIN_MINUTES", "5"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
TRADES_CSV_PATH: str = os.getenv("TRADES_CSV_PATH", "trades_hourly.csv")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
VERBOSE_LOGGING: bool = os.getenv("VERBOSE_LOGGING", "false").strip().lower() in (
    "true",
    "1",
    "yes",
)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate() -> None:
    """
    Validate all required configuration values.

    Raises:
        ValueError: If any required value is missing or logically invalid.

    Should be called once at bot startup, *before* any API calls or trading
    logic runs.
    """
    errors: list[str] = []

    # --- Required credentials ---
    if not KALSHI_API_KEY_ID:
        errors.append(
            "KALSHI_API_KEY_ID is not set. "
            "Add it to your .env file (see .env.example)."
        )

    if not KALSHI_PRIVATE_KEY_PATH:
        errors.append(
            "KALSHI_PRIVATE_KEY_PATH is not set. "
            "Set it to the path of your RSA private key PEM file."
        )
    elif not Path(KALSHI_PRIVATE_KEY_PATH).is_file():
        errors.append(
            f"KALSHI_PRIVATE_KEY_PATH='{KALSHI_PRIVATE_KEY_PATH}' does not exist "
            "or is not a file. Generate a key with: "
            "openssl genrsa -out private_key.pem 2048"
        )

    # --- Risk limits must be positive ---
    if MAX_DAILY_LOSS_CENTS <= 0:
        errors.append(
            f"MAX_DAILY_LOSS_CENTS must be > 0, got {MAX_DAILY_LOSS_CENTS}."
        )
    if MAX_OPEN_POSITIONS <= 0:
        errors.append(
            f"MAX_OPEN_POSITIONS must be > 0, got {MAX_OPEN_POSITIONS}."
        )
    if STOP_LOSS_CENTS <= 0:
        errors.append(
            f"STOP_LOSS_CENTS must be > 0, got {STOP_LOSS_CENTS}."
        )
    if TAKE_PROFIT_CENTS <= 0:
        errors.append(
            f"TAKE_PROFIT_CENTS must be > 0, got {TAKE_PROFIT_CENTS}."
        )

    # --- Warn on defaults that should be explicitly configured ---
    if BTC_SERIES_TICKER_HOURLY == "KXBTCH":
        logger.warning(
            "BTC_SERIES_TICKER_HOURLY is using default value 'KXBTCH'. "
            "Verify the correct ticker via the Kalshi API before live trading."
        )

    if KALSHI_ENV not in ("demo", "prod"):
        errors.append(
            f"KALSHI_ENV must be 'demo' or 'prod', got '{KALSHI_ENV}'."
        )

    if errors:
        formatted = "\n  - ".join([""] + errors)
        raise ValueError(
            f"Configuration validation failed:{formatted}\n\n"
            "See .env.example for the full list of required variables."
        )

    logger.info(
        "Configuration validated successfully. "
        "env=%s dry_run=%s ticker=%s",
        KALSHI_ENV,
        DRY_RUN,
        BTC_SERIES_TICKER_HOURLY,
    )
