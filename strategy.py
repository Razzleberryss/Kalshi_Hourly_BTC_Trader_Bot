"""
strategy.py — Hourly signal logic, fee calculation, and trade cycle orchestration
for the Kalshi BTC Hourly Trader Bot.

Strategy overview:
  - Uses order-book imbalance as the primary signal.
  - Applies a minimum confidence filter (MIN_CONFIDENCE).
  - Checks spread width (MAX_SPREAD_CENTS) and net edge after fees (MIN_EDGE_CENTS).
  - Enforces time-to-expiry guard (TIME_TO_EXPIRY_MIN_MINUTES).
  - Delegates all risk checks to RiskManager before placing orders.
"""

import logging
import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kalshi_client import KalshiClient
    from risk_manager import RiskManager

logger = logging.getLogger(__name__)


class HourlyStrategy:
    """
    Hourly BTC signal logic and trade cycle orchestration.

    Args:
        client:       A KalshiClient instance.
        cfg:          The config module.
        risk_manager: A RiskManager instance.
    """

    def __init__(
        self,
        client: "KalshiClient",
        cfg: Any,
        risk_manager: "RiskManager",
    ) -> None:
        self._client = client
        self._cfg = cfg
        self._rm = risk_manager

    # ------------------------------------------------------------------
    # Fee & edge calculations
    # ------------------------------------------------------------------

    def calc_fee(self, price: int) -> int:
        """
        Calculate Kalshi trading fee per contract.

        Formula: fee = max(1, ceil(0.07 * price * (100 - price) / 100))

        Args:
            price: Contract price in cents (1–99).

        Returns:
            Fee in cents (always >= 1).
        """
        return max(1, math.ceil(0.07 * price * (100 - price) / 100))

    def calc_net_edge(
        self, gross_pnl_cents: int, entry_price: int, exit_price: int
    ) -> int:
        """
        Calculate net edge after entry and exit fees.

        Args:
            gross_pnl_cents: Raw PnL before fees (positive = profit).
            entry_price:     Entry price in cents.
            exit_price:      Expected exit price in cents.

        Returns:
            Net edge in cents.
        """
        entry_fee = self.calc_fee(entry_price)
        exit_fee = self.calc_fee(exit_price)
        return gross_pnl_cents - entry_fee - exit_fee

    # ------------------------------------------------------------------
    # Time helpers
    # ------------------------------------------------------------------

    def minutes_to_expiry(self, market: dict) -> float:
        """
        Return minutes until market close_time, relative to UTC now.

        Args:
            market: Market dict from the Kalshi API.

        Returns:
            Minutes remaining (may be negative if already expired).
        """
        close_time_str = market.get("close_time") or market.get("expiration_time", "")
        if not close_time_str:
            # Unknown — assume plenty of time to be safe; let upstream handle
            logger.debug(
                "Market %s has no close_time; assuming 60 minutes.",
                market.get("ticker", "?"),
            )
            return 60.0

        try:
            # Kalshi returns ISO-8601 with trailing 'Z' or '+00:00'
            close_dt = datetime.fromisoformat(
                close_time_str.replace("Z", "+00:00")
            )
            now_utc = datetime.now(timezone.utc)
            delta_seconds = (close_dt - now_utc).total_seconds()
            return delta_seconds / 60.0
        except ValueError as exc:
            logger.warning(
                "Could not parse close_time '%s': %s", close_time_str, exc
            )
            return 60.0  # safe fallback

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------

    def get_signal(self, orderbook: dict) -> tuple[str | None, float]:
        """
        Derive a directional signal from the order book.

        Logic:
          - Uses best bid/ask on YES and NO sides.
          - YES signal: best_ask_yes < 40 AND NO-side has more volume
            (market pricing YES as unlikely → potential mis-pricing).
          - NO  signal: best_ask_no < 40 (i.e. best_bid_yes > 60) AND YES-side
            has more volume.
          - Confidence = |imbalance ratio| based on top-of-book volumes.

        Args:
            orderbook: Orderbook dict from get_orderbook().

        Returns:
            (side, confidence) where side is "yes"/"no"/None.
        """
        yes_levels: list[list[int]] = orderbook.get("yes", [])
        no_levels: list[list[int]] = orderbook.get("no", [])

        # Best ask on each side = lowest ask price (first ask level, price desc from bids)
        # Kalshi orderbook format: [[price, size], ...]
        # "yes" levels are bids sorted desc; "no" levels are bids sorted desc.
        # Best bid YES = yes_levels[0][0]; implied best ask NO = 100 - yes_levels[0][0]
        # Best bid NO  = no_levels[0][0];  implied best ask YES = 100 - no_levels[0][0]

        if not yes_levels or not no_levels:
            return (None, 0.0)

        best_bid_yes = yes_levels[0][0]
        best_bid_no = no_levels[0][0]

        best_ask_yes = 100 - best_bid_no   # Implied by NO bids
        best_ask_no = 100 - best_bid_yes   # Implied by YES bids

        # Total top-of-book volume for imbalance calculation
        yes_top_volume = yes_levels[0][1] if len(yes_levels[0]) > 1 else 1
        no_top_volume = no_levels[0][1] if len(no_levels[0]) > 1 else 1
        total_volume = yes_top_volume + no_top_volume

        if total_volume == 0:
            return (None, 0.0)

        imbalance = (no_top_volume - yes_top_volume) / total_volume  # > 0 → NO heavy

        # YES signal: cheap ask on YES side, more selling pressure on NO
        if best_ask_yes < 40 and imbalance > 0:
            confidence = min(1.0, abs(imbalance))
            if confidence >= self._cfg.MIN_CONFIDENCE:
                logger.debug(
                    "YES signal: ask_yes=%d¢ imbalance=%.3f confidence=%.3f",
                    best_ask_yes,
                    imbalance,
                    confidence,
                )
                return ("yes", confidence)

        # NO signal: cheap ask on NO side, more selling pressure on YES
        if best_ask_no < 40 and imbalance < 0:
            confidence = min(1.0, abs(imbalance))
            if confidence >= self._cfg.MIN_CONFIDENCE:
                logger.debug(
                    "NO signal: ask_no=%d¢ imbalance=%.3f confidence=%.3f",
                    best_ask_no,
                    imbalance,
                    confidence,
                )
                return ("no", confidence)

        return (None, 0.0)

    # ------------------------------------------------------------------
    # Exit logic
    # ------------------------------------------------------------------

    def should_exit(
        self, position: dict, market: dict, orderbook: dict
    ) -> tuple[bool, str]:
        """
        Determine whether an open position should be closed.

        Checks (in order):
          1. Stop-loss
          2. Take-profit
          3. Max hold time
          4. Time-to-expiry
          5. Signal reversal

        Args:
            position:  Position dict from RiskManager.open_positions.
            market:    Market dict from the Kalshi API.
            orderbook: Current orderbook dict.

        Returns:
            (True, reason) or (False, "").
        """
        cfg = self._cfg

        side = position["side"]
        entry_price = position["entry_price"]

        # Current mid-price estimate
        yes_levels: list[list[int]] = orderbook.get("yes", [])
        no_levels: list[list[int]] = orderbook.get("no", [])

        if yes_levels and no_levels:
            best_bid_yes = yes_levels[0][0]
            best_bid_no = no_levels[0][0]
            # Integer division is intentional: all Kalshi prices are in whole
            # cents and the mid-price is used for PnL comparisons against
            # cent-denominated stop/take-profit thresholds.
            mid_price = (best_bid_yes + (100 - best_bid_no)) // 2
        # Mid-price = average of best YES bid and implied YES ask
        # (implied YES ask = 100 - best NO bid, since YES + NO = 100).
        else:
            mid_price = entry_price  # Can't determine — stay put unless other trigger

        # Current unrealised PnL
        if side == "yes":
            current_pnl = mid_price - entry_price
        else:
            current_pnl = entry_price - mid_price

        # 1. Stop-loss
        if current_pnl <= -cfg.STOP_LOSS_CENTS:
            return (True, "stop_loss")

        # 2. Take-profit
        if current_pnl >= cfg.TAKE_PROFIT_CENTS:
            return (True, "take_profit")

        # 3. Max hold time
        try:
            entry_dt = datetime.fromisoformat(position["entry_time"])
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.replace(tzinfo=timezone.utc)
            held_minutes = (
                datetime.now(timezone.utc) - entry_dt
            ).total_seconds() / 60.0
            if held_minutes >= cfg.MAX_HOLD_MINUTES:
                return (True, "max_hold_time")
        except (ValueError, KeyError):
            pass  # Can't determine hold time — skip this check

        # 4. Time-to-expiry
        tte = self.minutes_to_expiry(market)
        if tte < cfg.TIME_TO_EXPIRY_MIN_MINUTES:
            return (True, "expiry")

        # 5. Signal reversal
        new_side, confidence = self.get_signal(orderbook)
        if new_side is not None and new_side != side and confidence >= cfg.MIN_CONFIDENCE:
            return (True, "signal_reversal")

        return (False, "")

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    def run_cycle(self, dry_run: bool) -> None:
        """
        Execute one full trading cycle.

        Steps:
          1. Fetch all open BTC hourly markets.
          2. For each position held by this bot: check exit conditions.
          3. For each market: evaluate entry opportunity.

        Args:
            dry_run: If True, orders are simulated — not sent to Kalshi.
        """
        cfg = self._cfg
        client = self._client
        rm = self._rm

        logger.info(
            "=== run_cycle start | dry_run=%s | open_positions=%d ===",
            dry_run,
            len(rm.open_positions),
        )

        # --- Step 1: Fetch markets ---
        markets = client.get_markets(cfg.BTC_SERIES_TICKER_HOURLY)
        if not markets:
            logger.warning(
                "No open markets found for series %s — skipping cycle.",
                cfg.BTC_SERIES_TICKER_HOURLY,
            )
            return

        market_map: dict[str, dict] = {m["ticker"]: m for m in markets if "ticker" in m}
        logger.info("Fetched %d open market(s).", len(market_map))

        # --- Step 2: Check exits for open positions ---
        for market_id, position in list(rm.open_positions.items()):
            market = market_map.get(market_id)
            if market is None:
                # Market no longer in open list — assume expired, force close.
                # Use entry_price as exit_price so PnL stats are not skewed by
                # a spurious 0-cent exit record.
                logger.warning(
                    "Position %s not found in open markets — forcing expiry close.",
                    market_id,
                )
                exit_price = position["entry_price"]
                rm.record_exit(market_id, exit_price, "market_gone",
                               datetime.now(timezone.utc))
                continue

            orderbook = client.get_orderbook(market_id)
            if orderbook is None:
                logger.warning(
                    "Could not fetch orderbook for open position %s — skipping exit check.",
                    market_id,
                )
                continue

            should_close, reason = self.should_exit(position, market, orderbook)
            if not should_close:
                logger.debug("Position %s — hold (no exit trigger).", market_id)
                continue

            # Determine close price (best bid for our side).
            # Use best available bid; fall back to entry_price so PnL records are
            # meaningful rather than recording a misleading 0.
            yes_levels = orderbook.get("yes", [])
            no_levels = orderbook.get("no", [])

            if position["side"] == "yes":
                close_price = yes_levels[0][0] if yes_levels else position["entry_price"]
            else:
                close_price = no_levels[0][0] if no_levels else position["entry_price"]

            logger.info(
                "Closing position %s: side=%s reason=%s exit_price=%d¢",
                market_id,
                position["side"],
                reason,
                close_price,
            )

            result = client.close_position(
                market_id=market_id,
                side=position["side"],
                quantity=position["quantity"],
                price=close_price,
                dry_run=dry_run,
            )

            if result is not None or dry_run:
                rm.record_exit(
                    market_id=market_id,
                    exit_price=close_price,
                    exit_reason=reason,
                    exit_time=datetime.now(timezone.utc),
                )

        # --- Step 3: Evaluate entries ---
        balance_cents = client.get_balance()
        logger.info("Available balance: %d¢", balance_cents)

        for ticker, market in market_map.items():
            # Skip if we already hold this market
            if rm.get_open_position(ticker) is not None:
                logger.debug("%s — already holding position, skipping entry.", ticker)
                continue

            # Time-to-expiry guard
            tte = self.minutes_to_expiry(market)
            if tte < cfg.TIME_TO_EXPIRY_MIN_MINUTES:
                logger.debug(
                    "%s — %.1f min to expiry < %d min threshold, skipping.",
                    ticker,
                    tte,
                    cfg.TIME_TO_EXPIRY_MIN_MINUTES,
                )
                continue

            # Fetch orderbook
            orderbook = client.get_orderbook(ticker)
            if orderbook is None:
                logger.debug("%s — no orderbook, skipping.", ticker)
                continue

            yes_levels = orderbook.get("yes", [])
            no_levels = orderbook.get("no", [])

            if not yes_levels or not no_levels:
                logger.debug("%s — thin book, skipping.", ticker)
                continue

            best_bid_yes = yes_levels[0][0]
            best_bid_no = no_levels[0][0]
            best_ask_yes = 100 - best_bid_no
            best_ask_no = 100 - best_bid_yes

            # Spread check
            spread = best_ask_yes - best_bid_yes
            if spread > cfg.MAX_SPREAD_CENTS:
                logger.debug(
                    "%s — spread %d¢ > %d¢ max, skipping.",
                    ticker,
                    spread,
                    cfg.MAX_SPREAD_CENTS,
                )
                continue

            # Signal
            signal_side, confidence = self.get_signal(orderbook)
            if signal_side is None:
                logger.debug("%s — no signal (confidence too low), skipping.", ticker)
                continue

            # Entry price = best ask for the signal side
            if signal_side == "yes":
                entry_price = best_ask_yes
                take_profit_price = entry_price + cfg.TAKE_PROFIT_CENTS
            else:
                entry_price = best_ask_no
                take_profit_price = entry_price + cfg.TAKE_PROFIT_CENTS

            # Clamp prices to valid range
            entry_price = max(1, min(99, entry_price))
            take_profit_price = max(1, min(99, take_profit_price))

            # Net edge check
            gross_pnl = cfg.TAKE_PROFIT_CENTS  # Expected gross profit at take-profit
            net_edge = self.calc_net_edge(gross_pnl, entry_price, take_profit_price)
            if net_edge < cfg.MIN_EDGE_CENTS:
                logger.debug(
                    "%s — net edge %d¢ < %d¢ min, skipping.",
                    ticker,
                    net_edge,
                    cfg.MIN_EDGE_CENTS,
                )
                continue

            # Compute order size
            max_dollars = rm.compute_max_trade_size_dollars(balance_cents)
            if max_dollars <= 0:
                logger.warning(
                    "%s — computed max_dollars=%d, skipping.", ticker, max_dollars
                )
                continue

            max_cents = max_dollars * 100
            quantity = max(1, max_cents // entry_price)
            order_cost_cents = quantity * entry_price

            # Risk manager check
            ok, reason = rm.can_trade(balance_cents, order_cost_cents)
            if not ok:
                logger.info("%s — risk check failed: %s", ticker, reason)
                continue

            # Confidence log
            logger.info(
                "Entry opportunity: market=%s side=%s entry=%d¢ "
                "net_edge=%d¢ confidence=%.3f qty=%d",
                ticker,
                signal_side,
                entry_price,
                net_edge,
                confidence,
                quantity,
            )

            # Place order
            if signal_side == "yes":
                result = client.place_order_yes(
                    ticker, quantity, entry_price, dry_run
                )
            else:
                result = client.place_order_no(
                    ticker, quantity, entry_price, dry_run
                )

            if result is not None:
                rm.record_entry(
                    market_id=ticker,
                    side=signal_side,
                    quantity=quantity,
                    entry_price=entry_price,
                    entry_time=datetime.now(timezone.utc),
                )

        logger.info(
            "=== run_cycle end | open_positions=%d | daily_trades=%d ===",
            len(rm.open_positions),
            rm.daily_trades,
        )


class HourlyBTCStrategy:
    """
    Hourly BTC signal logic implementing the full interface for the trading bot.
    Reads all parameters from cfg (config module or namespace).

    This class exposes the explicit public API used by the main loop:
      - get_signal(market_ticker, orderbook, price_history) -> dict
      - calculate_net_edge(entry_price, exit_price, side) -> float
      - check_spread(orderbook) -> bool
      - check_time_to_expiry(market) -> bool
    """

    def __init__(self, cfg: Any) -> None:
        """
        Initialise the strategy with a config object.

        Args:
            cfg: Config module or SimpleNamespace with all required attributes:
                 MIN_CONFIDENCE, MIN_EDGE_CENTS, MAX_SPREAD_CENTS,
                 LOOKBACK_HOURS, TIME_TO_EXPIRY_MIN_MINUTES,
                 MOMENTUM_THRESHOLD, TAKE_PROFIT_CENTS.
        """
        self._cfg = cfg

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_signal(
        self,
        market_ticker: str,
        orderbook: dict,
        price_history: list,
    ) -> dict:
        """
        Derive a directional signal combining momentum and order-book skew.

        Args:
            market_ticker: Kalshi market ticker (used for logging).
            orderbook:     Orderbook dict with "yes" and "no" level lists,
                           each entry being [price_cents, size].
            price_history: List of BTC prices (floats), oldest → newest.
                           Each entry represents one hour of data.

        Returns:
            Dict with keys:
              signal ("yes" | "no" | "none"), confidence (0.0–1.0),
              entry_price_cents (int), expected_net_edge_cents (float).
        """
        cfg = self._cfg

        _none = {
            "signal": "none",
            "confidence": 0.0,
            "entry_price_cents": 50,
            "expected_net_edge_cents": 0.0,
        }

        # ---- Spread guard ------------------------------------------------
        if not self.check_spread(orderbook):
            logger.debug("%s — spread too wide, no signal.", market_ticker)
            return _none

        yes_levels: list = orderbook.get("yes", [])
        no_levels: list = orderbook.get("no", [])

        # ---- Momentum signal (from price_history) ------------------------
        window = price_history[-cfg.LOOKBACK_HOURS:] if price_history else []

        momentum_direction: str | None = None
        momentum_score: float = 0.0

        if len(window) >= 2:
            oldest_price = window[0]
            latest_price = window[-1]

            if oldest_price != 0:
                pct_change = (latest_price - oldest_price) / oldest_price * 100.0
            else:
                pct_change = 0.0

            threshold = cfg.MOMENTUM_THRESHOLD
            if pct_change > threshold:
                momentum_direction = "yes"
                momentum_score = min(1.0, pct_change / (threshold * 3))
            elif pct_change < -threshold:
                momentum_direction = "no"
                momentum_score = min(1.0, abs(pct_change) / (threshold * 3))

        # ---- Skew signal (from orderbook) --------------------------------
        skew_direction: str | None = None
        skew_score: float = 0.0

        if yes_levels and no_levels:
            yes_top_vol = yes_levels[0][1] if len(yes_levels[0]) > 1 else 0
            no_top_vol = no_levels[0][1] if len(no_levels[0]) > 1 else 0
            total_vol = yes_top_vol + no_top_vol

            if total_vol > 0:
                # > 0 means NO-heavy → favour YES
                imbalance = (no_top_vol - yes_top_vol) / total_vol
                if imbalance > 0:
                    skew_direction = "yes"
                    skew_score = min(1.0, abs(imbalance))
                elif imbalance < 0:
                    skew_direction = "no"
                    skew_score = min(1.0, abs(imbalance))

        # ---- Combine signals ---------------------------------------------
        if momentum_direction is not None and skew_direction == momentum_direction:
            # Both agree — apply agreement bonus
            combined_direction = momentum_direction
            confidence = min(1.0, (momentum_score + skew_score) / 2 * 1.2)
        elif momentum_direction is not None and (
            skew_direction is None or skew_direction != momentum_direction
        ):
            # Only momentum fires (or skew disagrees)
            combined_direction = momentum_direction
            confidence = momentum_score * 0.7
        elif skew_direction is not None and (
            momentum_direction is None or momentum_direction != skew_direction
        ):
            # Only skew fires (or momentum disagrees)
            combined_direction = skew_direction
            confidence = skew_score * 0.5
        else:
            logger.debug("%s — no signal: neither momentum nor skew fired.", market_ticker)
            return _none

        # ---- Confidence threshold ----------------------------------------
        if confidence < cfg.MIN_CONFIDENCE:
            logger.debug(
                "%s — confidence %.3f below threshold %.3f, no signal.",
                market_ticker,
                confidence,
                cfg.MIN_CONFIDENCE,
            )
            return _none

        # ---- Entry price -------------------------------------------------
        if combined_direction == "yes":
            raw_entry = (100 - no_levels[0][0]) if no_levels else 50
        else:
            raw_entry = (100 - yes_levels[0][0]) if yes_levels else 50
        entry_price_cents = max(1, min(99, raw_entry))

        # ---- Net edge check ----------------------------------------------
        take_profit_price = max(1, min(99, entry_price_cents + cfg.TAKE_PROFIT_CENTS))
        net_edge = self.calculate_net_edge(entry_price_cents, take_profit_price, combined_direction)
        if net_edge < cfg.MIN_EDGE_CENTS:
            logger.debug(
                "%s — net edge %.1f¢ below minimum %d¢, no signal.",
                market_ticker,
                net_edge,
                cfg.MIN_EDGE_CENTS,
            )
            return _none

        logger.info(
            "%s — signal=%s confidence=%.3f entry=%d¢ net_edge=%.1f¢",
            market_ticker,
            combined_direction,
            confidence,
            entry_price_cents,
            net_edge,
        )

        return {
            "signal": combined_direction,
            "confidence": confidence,
            "entry_price_cents": entry_price_cents,
            "expected_net_edge_cents": net_edge,
        }

    def calculate_net_edge(self, entry_price: int, exit_price: int, side: str) -> float:
        """
        Calculate net edge after entry and exit fees.

        Fee formula: max(1, ceil(0.07 * price * (100 - price) / 100))
        Gross PnL = exit_price - entry_price (same for both sides in Kalshi).

        Args:
            entry_price: Entry price in cents (1–99).
            exit_price:  Expected exit price in cents (1–99).
            side:        "yes" or "no" (currently symmetric; reserved for future use).

        Returns:
            Net edge in cents as a float.
        """
        entry_fee = max(1, math.ceil(0.07 * entry_price * (100 - entry_price) / 100))
        exit_fee = max(1, math.ceil(0.07 * exit_price * (100 - exit_price) / 100))
        gross_pnl = exit_price - entry_price
        net_edge = gross_pnl - entry_fee - exit_fee
        return float(net_edge)

    def check_spread(self, orderbook: dict) -> bool:
        """
        Return True if the YES-side spread is within the configured maximum.

        Spread = best_ask_yes - best_bid_yes
               = (100 - best_bid_no) - best_bid_yes

        Args:
            orderbook: Orderbook dict with "yes" and "no" level lists.

        Returns:
            True if spread <= MAX_SPREAD_CENTS, False otherwise.
        """
        yes_levels: list = orderbook.get("yes", [])
        no_levels: list = orderbook.get("no", [])

        if not yes_levels or not no_levels:
            logger.warning("check_spread: empty yes or no levels in orderbook.")
            return False

        best_bid_yes = yes_levels[0][0]
        best_bid_no = no_levels[0][0]
        best_ask_yes = 100 - best_bid_no
        spread = best_ask_yes - best_bid_yes

        if spread > self._cfg.MAX_SPREAD_CENTS:
            logger.warning(
                "check_spread: spread %d¢ exceeds max %d¢.",
                spread,
                self._cfg.MAX_SPREAD_CENTS,
            )
            return False

        return True

    def check_time_to_expiry(self, market: dict) -> bool:
        """
        Return True if there is enough time before market expiry to trade.

        Args:
            market: Market dict from the Kalshi API containing "close_time"
                    or "expiration_time" in ISO-8601 format.

        Returns:
            False if fewer than TIME_TO_EXPIRY_MIN_MINUTES remain, True otherwise.
            Returns True (safe default) when close_time is missing or unparseable.
        """
        close_time = market.get("close_time") or market.get("expiration_time", "")

        if not close_time:
            logger.warning(
                "check_time_to_expiry: market %s has no close_time — allowing trade.",
                market.get("ticker", "?"),
            )
            return True

        try:
            close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            minutes_remaining = (
                close_dt - datetime.now(timezone.utc)
            ).total_seconds() / 60.0
        except ValueError as exc:
            logger.warning(
                "check_time_to_expiry: could not parse close_time '%s': %s — allowing trade.",
                close_time,
                exc,
            )
            return True

        if minutes_remaining < self._cfg.TIME_TO_EXPIRY_MIN_MINUTES:
            logger.warning(
                "check_time_to_expiry: only %.1f min remain (min=%d) — blocking trade.",
                minutes_remaining,
                self._cfg.TIME_TO_EXPIRY_MIN_MINUTES,
            )
            return False

        return True
