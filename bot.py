"""
bot.py — Main entry point for the Kalshi BTC Hourly Trader Bot.

Startup sequence:
  1. Load .env
  2. Set up logging
  3. Print ASCII banner
  4. Validate config (raises on bad config, exits cleanly)
  5. Safety warning for live prod mode
  6. Initialise client, risk manager, strategy
  7. Recover open positions from CSV
  8. Main loop: run strategy cycle every hour
"""

import logging
import sys
import time
from datetime import datetime, timezone

import config
from kalshi_client import KalshiClient
from risk_manager import RiskManager
from strategy import HourlyStrategy


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    """Configure root logger before anything else runs."""
    level_str = config.LOG_LEVEL if config.LOG_LEVEL else "INFO"
    if config.VERBOSE_LOGGING:
        level_str = "DEBUG"

    numeric_level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


# ---------------------------------------------------------------------------
# Banners
# ---------------------------------------------------------------------------

_ASCII_BANNER = r"""
╔══════════════════════════════════════════════════════════════╗
║       KALSHI BTC HOURLY TRADER BOT                          ║
║       Automated Prediction-Market Trading System            ║
╚══════════════════════════════════════════════════════════════╝
"""

_LIVE_WARNING_BANNER = """
╔══════════════════════════════════════════════════════════════╗
║  ⚠  WARNING: LIVE PRODUCTION TRADING MODE  ⚠               ║
║                                                              ║
║  DRY_RUN=false + KALSHI_ENV=prod                            ║
║  REAL MONEY will be placed on Kalshi markets.               ║
║                                                              ║
║  Press Ctrl+C within 5 seconds to abort.                    ║
╚══════════════════════════════════════════════════════════════╝
"""


# ---------------------------------------------------------------------------
# Main loop helpers
# ---------------------------------------------------------------------------

def _seconds_until_next_hour() -> int:
    """Return seconds remaining until the top of the next UTC hour."""
    now = datetime.now(timezone.utc)
    seconds_elapsed = now.minute * 60 + now.second
    return max(60, 3600 - seconds_elapsed)  # At least 60 s to avoid tight loops


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Bot entry point."""
    # 1. Load .env is already done at config module import time via load_dotenv().

    # 2. Setup logging
    _setup_logging()
    logger = logging.getLogger(__name__)

    # 3. Banner
    print(_ASCII_BANNER)

    # 4. Validate config — exit(1) on misconfiguration
    try:
        config.validate()
    except ValueError as exc:
        logging.getLogger("config").error("Configuration error:\n%s", exc)
        sys.exit(1)

    # 5. Live mode safety warning
    if config.KALSHI_ENV == "prod" and not config.DRY_RUN:
        print(_LIVE_WARNING_BANNER)
        logger.warning(
            "LIVE PRODUCTION MODE — sleeping 5 seconds before starting. "
            "Press Ctrl+C to abort."
        )
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Startup aborted by user.")
            sys.exit(0)

    # 6. Log mode
    mode_label = "[DRY-RUN MODE]" if config.DRY_RUN else "[LIVE TRADING MODE]"
    logger.info(
        "%s  env=%s  series=%s",
        mode_label,
        config.KALSHI_ENV,
        config.BTC_SERIES_TICKER_HOURLY,
    )

    # 7. Initialise components
    client = KalshiClient(
        api_key_id=config.KALSHI_API_KEY_ID,
        private_key_path=config.KALSHI_PRIVATE_KEY_PATH,
        base_url=config.KALSHI_BASE_URL,
    )
    risk_manager = RiskManager(cfg=config)
    strategy = HourlyStrategy(client=client, cfg=config, risk_manager=risk_manager)

    # 8. Recover state
    risk_manager.load_trades_from_csv()

    logger.info("Bot initialised successfully. Entering main loop.")

    # 9. Main loop
    try:
        while True:
            cycle_start = datetime.now(timezone.utc)
            logger.info("Cycle starting at %s", cycle_start.isoformat())

            try:
                strategy.run_cycle(dry_run=config.DRY_RUN)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    "Unhandled exception in run_cycle — will retry next hour: %s",
                    exc,
                    exc_info=True,
                )

            sleep_secs = _seconds_until_next_hour()
            logger.info(
                "Cycle complete. Sleeping %d seconds until next hourly cycle.",
                sleep_secs,
            )
            time.sleep(sleep_secs)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user (KeyboardInterrupt). Exiting cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
