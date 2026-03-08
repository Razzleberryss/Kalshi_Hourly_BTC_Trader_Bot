"""
bot.py — Production main loop for the Kalshi BTC Hourly Trader Bot.

Startup sequence:
  1. Load .env (done at config import time via load_dotenv)
  2. Set up logging
  3. Print ASCII banner
  4. Call config.validate() — halt immediately if any required env vars are missing
  5. If KALSHI_ENV=prod AND DRY_RUN=false, print a large WARNING banner and sleep 5 s
  6. Log "Starting Kalshi BTC Hourly Trader Bot | Mode: DRY-RUN | Env: demo/prod"
  7. Initialise KalshiClient, RiskManager, HourlyBTCStrategy
  8. Recover open positions from CSV
  9. Install SIGTERM handler

Main loop (runs every POLL_INTERVAL_SECONDS, default 60 s):
  - Fetch active hourly BTC markets via get_active_btc_hourly_markets()
  - For each market:
      a. check_time_to_expiry() — skip if < 5 min
      b. get_orderbook()
      c. check_spread() — skip if too wide
      d. get_signal() — skip if "none" or confidence < MIN_CONFIDENCE
      e. skip if expected_net_edge_cents < MIN_EDGE_CENTS
      f. check_before_trade() — skip if any limit hit
      g. place_order() (respects DRY_RUN flag)
      h. record_trade_open() on success
  - Check all open positions:
      a. stop-loss  b. take-profit  c. expiry  d. signal_reversal  e. max_hold
  - Check daily loss circuit breaker — halt loop if breached
  - Sleep POLL_INTERVAL_SECONDS

Graceful shutdown:
  - Catch KeyboardInterrupt and SIGTERM
  - Close all open positions before exit
"""

import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import config
from kalshi_client import KalshiClient
from risk_manager import RiskManager
from strategy import HourlyBTCStrategy

logger = logging.getLogger(__name__)


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
# SIGTERM handler
# ---------------------------------------------------------------------------

# Shared flag — set by SIGTERM handler to trigger graceful shutdown
_shutdown_requested: bool = False


def _request_shutdown(signum: int, frame: Any) -> None:  # noqa: ANN001
    """SIGTERM handler: set flag so the main loop exits cleanly."""
    global _shutdown_requested  # noqa: PLW0603
    _shutdown_requested = True
    logger.warning("SIGTERM received — requesting graceful shutdown.")


# ---------------------------------------------------------------------------
# PnL helper
# ---------------------------------------------------------------------------

def _calc_current_pnl(position: dict, orderbook: dict) -> int:
    """
    Estimate unrealised PnL for an open position based on current mid-price.

    Args:
        position:  Position dict with keys 'side', 'entry_price', 'quantity'.
        orderbook: Current orderbook dict with 'yes' and 'no' level lists.

    Returns:
        Estimated PnL per contract in cents (negative = loss).
    """
    yes_levels = orderbook.get("yes", [])
    no_levels = orderbook.get("no", [])

    entry_price = position["entry_price"]

    if yes_levels and no_levels:
        best_bid_yes = yes_levels[0][0]
        best_bid_no = no_levels[0][0]
        # Integer division is intentional: Kalshi prices are whole cents and
        # the mid-price is compared against cent-denominated thresholds.
        mid_price = (best_bid_yes + (100 - best_bid_no)) // 2
    else:
        mid_price = entry_price  # No data — assume break-even

    side = position["side"]
    if side == "yes":
        return mid_price - entry_price
    return entry_price - mid_price


# ---------------------------------------------------------------------------
# Shutdown helper
# ---------------------------------------------------------------------------

def _close_all_positions(
    client: KalshiClient,
    risk_manager: RiskManager,
    dry_run: bool,
) -> None:
    """
    Attempt to close every open position tracked by the risk manager.

    Called on graceful shutdown (KeyboardInterrupt / SIGTERM).

    Args:
        client:       KalshiClient for placing close orders.
        risk_manager: RiskManager holding open position state.
        dry_run:      If True, log only — do not send real close orders.
    """
    positions = risk_manager.get_open_positions()
    if not positions:
        logger.info("No open positions to close on shutdown.")
        return

    logger.info("Closing %d open position(s) on shutdown…", len(positions))
    for pos in positions:
        market_id = pos["market_ticker"]

        # Best-effort close: use live orderbook if available, else entry price
        orderbook = client.get_orderbook(market_id)
        if orderbook:
            yes_levels = orderbook.get("yes", [])
            no_levels = orderbook.get("no", [])
            if pos["side"] == "yes":
                close_price = yes_levels[0][0] if yes_levels else pos["entry_price"]
            else:
                close_price = no_levels[0][0] if no_levels else pos["entry_price"]
        else:
            close_price = pos["entry_price"]

        result = client.close_position(
            market_id=market_id,
            side=pos["side"],
            quantity=pos["quantity"],
            price=close_price,
            dry_run=dry_run,
        )
        if result is not None or dry_run:
            risk_manager.record_trade_close(
                market_ticker=market_id,
                exit_price=close_price,
                exit_reason="shutdown",
                timestamp=datetime.now(timezone.utc),
            )
            logger.info("Closed position %s on shutdown.", market_id)
        else:
            logger.warning("Failed to close position %s on shutdown.", market_id)


# ---------------------------------------------------------------------------
# Single-cycle logic (extracted for testability)
# ---------------------------------------------------------------------------

def run_one_cycle(
    client: KalshiClient,
    risk_manager: RiskManager,
    strategy: HourlyBTCStrategy,
    dry_run: bool,
    price_history: list,
) -> bool:
    """
    Execute one full trading cycle.

    Steps:
      1. Fetch active BTC hourly markets.
      2. Check exit conditions for every open position.
      3. Check daily loss circuit breaker.
      4. Evaluate entry opportunities for each market.

    Args:
        client:        KalshiClient instance.
        risk_manager:  RiskManager instance.
        strategy:      HourlyBTCStrategy instance.
        dry_run:       If True, simulate orders — do not send to Kalshi.
        price_history: Rolling list of BTC mid-prices, oldest → newest.
                       Updated externally; may be empty on first cycle.

    Returns:
        True  — continue the main loop.
        False — daily loss circuit breaker fired; main loop should exit.
    """
    now = datetime.now(timezone.utc)
    logger.info(
        "=== cycle start | %s | dry_run=%s | open=%d ===",
        now.isoformat(),
        dry_run,
        len(risk_manager.open_positions),
    )

    # ------------------------------------------------------------------
    # Step 1: Fetch active BTC hourly markets
    # ------------------------------------------------------------------
    markets = client.get_active_btc_hourly_markets()
    if not markets:
        logger.warning(
            "No active BTC hourly markets found — skipping entry evaluation."
        )

    market_map: dict = {
        m["ticker"]: m for m in (markets or []) if "ticker" in m
    }
    if markets:
        logger.info("Fetched %d active market(s).", len(market_map))

    # ------------------------------------------------------------------
    # Step 2: Check exit conditions for every open position
    # ------------------------------------------------------------------
    for pos in risk_manager.get_open_positions():
        market_id = pos["market_ticker"]
        market = market_map.get(market_id)

        # Fetch current orderbook for PnL and signal reversal checks
        orderbook = client.get_orderbook(market_id)
        if orderbook is None:
            logger.warning(
                "Cannot fetch orderbook for open position %s — skipping exit check.",
                market_id,
            )
            continue

        exit_reason: str | None = None

        # a. Stop-loss
        pnl = _calc_current_pnl(pos, orderbook)
        if pnl <= -config.STOP_LOSS_CENTS:
            exit_reason = "stop_loss"

        # b. Take-profit
        elif pnl >= config.TAKE_PROFIT_CENTS:
            exit_reason = "take_profit"

        # c. Time-to-expiry: force close if market has left the active list or
        #    strategy says fewer than TIME_TO_EXPIRY_MIN_MINUTES remain
        elif market is None:
            logger.warning(
                "Position %s not in active markets — forcing expiry close.", market_id
            )
            exit_reason = "expiry"
        elif not strategy.check_time_to_expiry(market, current_time=now):
            exit_reason = "expiry"

        # d. Signal reversal — only checked when no earlier trigger fired
        if exit_reason is None:
            sig_result = strategy.get_signal(market_id, orderbook, price_history)
            new_side = sig_result.get("signal", "none")
            if (
                new_side not in ("none", None)
                and new_side != pos["side"]
                and sig_result.get("confidence", 0.0) >= config.MIN_CONFIDENCE
            ):
                exit_reason = "signal_reversal"

        # e. Max hold time — only checked when no earlier trigger fired
        if exit_reason is None:
            try:
                entry_dt = datetime.fromisoformat(pos["entry_time"])
                if entry_dt.tzinfo is None:
                    entry_dt = entry_dt.replace(tzinfo=timezone.utc)
                held_minutes = (now - entry_dt).total_seconds() / 60.0
                if held_minutes >= config.MAX_HOLD_MINUTES:
                    exit_reason = "max_hold"
            except (ValueError, KeyError):
                pass  # Cannot determine hold time — skip

        if exit_reason is None:
            logger.debug("Position %s — hold (no exit trigger).", market_id)
            continue

        # Determine close price (best bid for our side; fallback to entry price)
        yes_levels = orderbook.get("yes", [])
        no_levels = orderbook.get("no", [])
        if pos["side"] == "yes":
            close_price = yes_levels[0][0] if yes_levels else pos["entry_price"]
        else:
            close_price = no_levels[0][0] if no_levels else pos["entry_price"]

        logger.info(
            "Closing position %s: side=%s reason=%s exit_price=%d¢ pnl=%d¢",
            market_id,
            pos["side"],
            exit_reason,
            close_price,
            pnl,
        )

        result = client.close_position(
            market_id=market_id,
            side=pos["side"],
            quantity=pos["quantity"],
            price=close_price,
            dry_run=dry_run,
        )
        if result is not None or dry_run:
            risk_manager.record_trade_close(
                market_ticker=market_id,
                exit_price=close_price,
                exit_reason=exit_reason,
                timestamp=now,
            )

    # ------------------------------------------------------------------
    # Step 3: Daily loss circuit breaker
    # ------------------------------------------------------------------
    if risk_manager.daily_loss_cents >= config.MAX_DAILY_LOSS_CENTS:
        logger.critical(
            "DAILY LOSS LIMIT HIT (%d¢ >= %d¢) — halting trading loop.",
            risk_manager.daily_loss_cents,
            config.MAX_DAILY_LOSS_CENTS,
        )
        return False  # Signal main loop to exit

    # ------------------------------------------------------------------
    # Step 4: Evaluate entry opportunities
    # ------------------------------------------------------------------
    if not market_map:
        logger.info(
            "=== cycle end | open=%d | daily_trades=%d | daily_loss=%d¢ ===",
            len(risk_manager.open_positions),
            risk_manager.daily_trades,
            risk_manager.daily_loss_cents,
        )
        return True

    balance_cents = client.get_balance()
    logger.info("Available balance: %d¢", balance_cents)

    for ticker, market in market_map.items():
        # Skip if we already hold a position in this market
        if risk_manager.get_open_position(ticker) is not None:
            logger.debug("%s — already holding position, skipping entry.", ticker)
            continue

        # a. Time-to-expiry guard
        if not strategy.check_time_to_expiry(market, current_time=now):
            logger.debug("%s — too close to expiry, skipping.", ticker)
            continue

        # b. Fetch orderbook
        orderbook = client.get_orderbook(ticker)
        if orderbook is None:
            logger.debug("%s — no orderbook, skipping.", ticker)
            continue

        # c. Spread check
        if not strategy.check_spread(orderbook):
            logger.debug("%s — spread too wide, skipping.", ticker)
            continue

        # d. Get signal
        sig_result = strategy.get_signal(ticker, orderbook, price_history)
        signal_side = sig_result.get("signal", "none")
        confidence = sig_result.get("confidence", 0.0)
        entry_price = sig_result.get("entry_price_cents", 50)
        net_edge = sig_result.get("expected_net_edge_cents", 0.0)

        # e. Confidence / signal filter
        if signal_side == "none" or confidence < config.MIN_CONFIDENCE:
            logger.debug(
                "%s — no signal (side=%s confidence=%.3f), skipping.",
                ticker,
                signal_side,
                confidence,
            )
            continue

        # f. Net edge filter
        if net_edge < config.MIN_EDGE_CENTS:
            logger.debug(
                "%s — net edge %.1f¢ < %d¢ min, skipping.",
                ticker,
                net_edge,
                config.MIN_EDGE_CENTS,
            )
            continue

        # Compute order size
        max_dollars = risk_manager.compute_max_trade_size_dollars(balance_cents)
        if max_dollars <= 0:
            logger.warning("%s — max_dollars=%d, skipping.", ticker, max_dollars)
            continue

        quantity = max(1, (max_dollars * 100) // entry_price)
        order_cost_cents = quantity * entry_price

        # g. Risk manager pre-trade check
        ok, reason = risk_manager.check_before_trade(
            market_ticker=ticker,
            side=signal_side,
            quantity=quantity,
            price_cents=entry_price,
            balance_cents=balance_cents,
        )
        if not ok:
            logger.info("%s — risk check failed: %s", ticker, reason)
            continue

        logger.info(
            "Entry: market=%s side=%s entry=%d¢ net_edge=%.1f¢ "
            "confidence=%.3f qty=%d",
            ticker,
            signal_side,
            entry_price,
            net_edge,
            confidence,
            quantity,
        )

        # h. Place order
        result = client.place_order(
            market_id=ticker,
            side=signal_side,
            quantity=quantity,
            price=entry_price,
            dry_run=dry_run,
        )

        # i. Record trade open on success
        if result is not None:
            risk_manager.record_trade_open(
                market_ticker=ticker,
                side=signal_side,
                quantity=quantity,
                entry_price=entry_price,
                timestamp=datetime.now(timezone.utc),
            )

    logger.info(
        "=== cycle end | open=%d | daily_trades=%d | daily_loss=%d¢ ===",
        len(risk_manager.open_positions),
        risk_manager.daily_trades,
        risk_manager.daily_loss_cents,
    )
    return True


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Bot entry point."""
    # 1. Load .env is already done at config module import time via load_dotenv().

    # 2. Setup logging (must happen before any log calls)
    _setup_logging()

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

    # 6. Log startup mode
    mode_label = "DRY-RUN" if config.DRY_RUN else "LIVE"
    logger.info(
        "Starting Kalshi BTC Hourly Trader Bot | Mode: %s | Env: %s",
        mode_label,
        config.KALSHI_ENV,
    )

    # 7. Initialise components
    client = KalshiClient(
        api_key_id=config.KALSHI_API_KEY_ID,
        private_key_path=config.KALSHI_PRIVATE_KEY_PATH,
        base_url=config.KALSHI_BASE_URL,
        trades_csv_path=config.TRADES_CSV_PATH,
        btc_series_ticker=config.BTC_SERIES_TICKER_HOURLY,
    )
    risk_manager = RiskManager(cfg=config)
    strategy = HourlyBTCStrategy(cfg=config)

    # 8. Recover open positions from CSV
    risk_manager.load_trades_from_csv()

    # 9. Install SIGTERM handler for graceful shutdown
    signal.signal(signal.SIGTERM, _request_shutdown)

    logger.info(
        "Bot initialised. Entering main loop (poll_interval=%ds).",
        config.POLL_INTERVAL_SECONDS,
    )

    # Rolling BTC price history (mid-prices from orderbooks), oldest → newest.
    # Collected across cycles; passed to strategy.get_signal() for momentum.
    price_history: list = []
    max_history_len = max(config.LOOKBACK_HOURS * 4, 20)

    # 10. Main loop
    try:
        while not _shutdown_requested:
            try:
                should_continue = run_one_cycle(
                    client=client,
                    risk_manager=risk_manager,
                    strategy=strategy,
                    dry_run=config.DRY_RUN,
                    price_history=price_history,
                )
                # Trim price history to avoid unbounded growth
                if len(price_history) > max_history_len:
                    price_history[:] = price_history[-max_history_len:]
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    "Unhandled exception in run_one_cycle — will retry next cycle: %s",
                    exc,
                    exc_info=True,
                )
                should_continue = True

            if not should_continue:
                break

            logger.info(
                "Sleeping %d seconds until next cycle.",
                config.POLL_INTERVAL_SECONDS,
            )
            # Sleep in 1-second increments so SIGTERM / KeyboardInterrupt
            # interrupts quickly rather than waiting the full interval.
            for _ in range(config.POLL_INTERVAL_SECONDS):
                if _shutdown_requested:
                    break
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user (KeyboardInterrupt).")

    # 11. Graceful shutdown
    logger.info("Shutting down — closing all open positions.")
    _close_all_positions(client, risk_manager, dry_run=config.DRY_RUN)
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
