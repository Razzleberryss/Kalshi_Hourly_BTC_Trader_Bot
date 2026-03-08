"""
risk_manager.py — Risk controls and trade journal for the Kalshi BTC Hourly Trader Bot.

The RiskManager:
- Enforces pre-trade risk checks (position limits, daily loss, balance, etc.)
- Tracks open positions opened by THIS bot (via trades_hourly.csv)
- Resets daily counters at midnight UTC
- Persists trade records to CSV for auditability

NOTE: It tracks ONLY positions opened by this bot. Pre-existing positions from
other bots or manual trading are NOT included in risk calculations.
"""

import csv
import logging
import math
import os
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# 20% safety buffer: require balance >= order_cost * BALANCE_SAFETY_MULTIPLIER
# before placing any trade, to cover potential fee or slippage overage.
BALANCE_SAFETY_MULTIPLIER: float = 1.2

# CSV column order for trades_hourly.csv
_CSV_COLUMNS = [
    "timestamp_open",       # Entry timestamp (UTC ISO-8601)
    "market_ticker",
    "side",
    "quantity",
    "entry_price_cents",
    "timestamp_close",      # Exit timestamp (UTC ISO-8601); empty until closed
    "exit_price_cents",
    "pnl_cents",
    "exit_reason",
]


class RiskManager:
    """
    Enforces risk limits and maintains a persistent trade journal.

    Args:
        cfg: The config module (config.py) — passed as a module reference so
             tests can inject a mock easily.
    """

    def __init__(self, cfg: Any) -> None:
        self._cfg = cfg

        # Open positions: market_id → position info dict
        self.open_positions: dict[str, dict] = {}

        # Daily counters (reset at midnight UTC)
        self.daily_loss_cents: int = 0
        self.daily_trades: int = 0
        self.daily_reset_date: date = datetime.now(timezone.utc).date()

    # ------------------------------------------------------------------
    # Startup recovery
    # ------------------------------------------------------------------

    def load_trades_from_csv(self) -> None:
        """
        Recover open positions from the trade CSV on startup.

        Rows without an exit_price_cents are considered still-open positions
        and are loaded back into self.open_positions so risk limits are
        enforced correctly after a bot restart.
        """
        csv_path = self._cfg.TRADES_CSV_PATH
        if not os.path.isfile(csv_path):
            logger.info("No trade CSV found at %s — starting fresh.", csv_path)
            return

        loaded = 0
        try:
            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    # Parse entry timestamp for daily-counter accumulation
                    try:
                        row_date = datetime.fromisoformat(
                            row["timestamp_open"]
                        ).date()
                    except (ValueError, KeyError):
                        row_date = None

                    if row.get("exit_price_cents", "").strip() in ("", "None"):
                        # Still open — restore to in-memory map
                        market_ticker = row.get("market_ticker", "")
                        if market_ticker:
                            self.open_positions[market_ticker] = {
                                "market_ticker": market_ticker,
                                "side": row.get("side", "yes"),
                                "quantity": int(row.get("quantity", 1)),
                                "entry_price": int(
                                    row.get("entry_price_cents", 50)
                                ),
                                "entry_time": row.get("timestamp_open", ""),
                            }
                            loaded += 1

                    # Accumulate today's daily counters
                    today = datetime.now(timezone.utc).date()
                    if row_date == today:
                        self.daily_trades += 1
                        try:
                            pnl = int(row.get("pnl_cents", 0) or 0)
                            if pnl < 0:
                                self.daily_loss_cents += abs(pnl)
                        except ValueError:
                            pass

        except (OSError, csv.Error) as exc:
            logger.warning("Could not load trade CSV: %s", exc)
            return

        logger.info(
            "Loaded %d open position(s) from %s. "
            "Daily trades=%d daily_loss=%d¢",
            loaded,
            csv_path,
            self.daily_trades,
            self.daily_loss_cents,
        )

    # ------------------------------------------------------------------
    # Core risk checks
    # ------------------------------------------------------------------

    def check_before_trade(
        self,
        market_ticker: str,
        side: str,
        quantity: int,
        price_cents: int,
        balance_cents: int = 0,
    ) -> tuple[bool, str]:
        """
        Check whether a new trade is allowed under current risk limits.

        Enforces in order:
          1. available_balance >= order_cost * 1.2
          2. open positions < MAX_OPEN_POSITIONS
          3. total exposure < MAX_TOTAL_EXPOSURE_CENTS
          4. daily loss < MAX_DAILY_LOSS_CENTS  (circuit breaker — halts all trading)
          5. daily trade count < MAX_DAILY_TRADES

        Args:
            market_ticker: Market ticker being traded.
            side:          "yes" or "no".
            quantity:      Number of contracts.
            price_cents:   Per-contract price in cents.
            balance_cents: Current available balance in cents.

        Returns:
            (True, "ok") if all checks pass.
            (False, reason_string) on the first failing check.
        """
        self._reset_daily_counters_if_needed()

        cfg = self._cfg
        order_cost_cents = quantity * price_cents

        # 1. Balance check
        min_required = int(order_cost_cents * BALANCE_SAFETY_MULTIPLIER)
        if balance_cents < min_required:
            reason = (
                f"Insufficient balance: {balance_cents}¢ < "
                f"{min_required}¢ (1.2× order cost of {order_cost_cents}¢)"
            )
            logger.info(
                "Trade blocked [%s side=%s]: %s", market_ticker, side, reason
            )
            return (False, reason)

        # 2. Open positions count check
        if len(self.open_positions) >= cfg.MAX_OPEN_POSITIONS:
            reason = (
                f"Max open positions reached: {len(self.open_positions)} "
                f">= {cfg.MAX_OPEN_POSITIONS}"
            )
            logger.info(
                "Trade blocked [%s side=%s]: %s", market_ticker, side, reason
            )
            return (False, reason)

        # 3. Total exposure check
        total_exposure = sum(
            p["entry_price"] * p["quantity"]
            for p in self.open_positions.values()
        )
        if total_exposure + order_cost_cents >= cfg.MAX_TOTAL_EXPOSURE_CENTS:
            reason = (
                f"Max total exposure would be exceeded: current={total_exposure}¢ "
                f"+ new={order_cost_cents}¢ > {cfg.MAX_TOTAL_EXPOSURE_CENTS}¢"
            )
            logger.info(
                "Trade blocked [%s side=%s]: %s", market_ticker, side, reason
            )
            return (False, reason)

        # 4. Daily loss circuit breaker
        if self.daily_loss_cents >= cfg.MAX_DAILY_LOSS_CENTS:
            reason = (
                f"Daily loss limit reached: {self.daily_loss_cents}¢ "
                f">= {cfg.MAX_DAILY_LOSS_CENTS}¢"
            )
            logger.info(
                "Trade blocked [%s side=%s]: %s", market_ticker, side, reason
            )
            return (False, reason)

        # 5. Daily trade count check
        if self.daily_trades >= cfg.MAX_DAILY_TRADES:
            reason = (
                f"Daily trade limit reached: {self.daily_trades} "
                f">= {cfg.MAX_DAILY_TRADES}"
            )
            logger.info(
                "Trade blocked [%s side=%s]: %s", market_ticker, side, reason
            )
            return (False, reason)

        return (True, "ok")

    def can_trade(self, balance_cents: int, order_cost_cents: int) -> tuple[bool, str]:
        """
        Backward-compatible wrapper around check_before_trade().

        Called by strategy.py with (balance_cents, order_cost_cents).
        Prefer check_before_trade() for new code.
        """
        return self.check_before_trade(
            market_ticker="",
            side="yes",
            quantity=1,
            price_cents=order_cost_cents,
            balance_cents=balance_cents,
        )

    def compute_max_trade_size_dollars(self, balance_cents: int) -> int:
        """
        Return the maximum dollar amount to risk on a single trade.

        Uses the lesser of:
          - config.MAX_DOLLARS_PER_TRADE
          - 25% of current balance
        """
        balance_dollars = balance_cents / 100
        dynamic_max = int(balance_dollars * 0.25)
        return min(self._cfg.MAX_DOLLARS_PER_TRADE, dynamic_max)

    # ------------------------------------------------------------------
    # Position tracking
    # ------------------------------------------------------------------

    def record_trade_open(
        self,
        market_ticker: str,
        side: str,
        quantity: int,
        entry_price: int,
        timestamp: datetime,
    ) -> None:
        """
        Record a new open position and append to the trade CSV.

        Args:
            market_ticker: Market ticker.
            side:          "yes" or "no".
            quantity:      Contracts purchased.
            entry_price:   Fill price in cents.
            timestamp:     Entry timestamp (UTC).
        """
        entry_ts = timestamp.isoformat()
        self.open_positions[market_ticker] = {
            "market_ticker": market_ticker,
            "side": side,
            "quantity": quantity,
            "entry_price": entry_price,
            "entry_time": entry_ts,
        }
        self.daily_trades += 1

        self._append_trade_csv(
            {
                "timestamp_open": entry_ts,
                "market_ticker": market_ticker,
                "side": side,
                "quantity": quantity,
                "entry_price_cents": entry_price,
                "timestamp_close": "",
                "exit_price_cents": "",
                "pnl_cents": "",
                "exit_reason": "",
            }
        )
        logger.info(
            "Recorded entry: market=%s side=%s qty=%d price=%d¢",
            market_ticker,
            side,
            quantity,
            entry_price,
        )

    def record_entry(
        self,
        market_id: str,
        side: str,
        quantity: int,
        entry_price: int,
        entry_time: datetime,
    ) -> None:
        """Backward-compatible alias for record_trade_open(). Used by strategy.py."""
        self.record_trade_open(market_id, side, quantity, entry_price, entry_time)

    def record_trade_close(
        self,
        market_ticker: str,
        exit_price: int,
        exit_reason: str,
        timestamp: datetime,
    ) -> int:
        """
        Record a position exit, update the CSV, and adjust daily loss tracker.

        Args:
            market_ticker: Market ticker.
            exit_price:    Exit fill price in cents.
            exit_reason:   Human-readable exit reason (e.g. "stop_loss").
            timestamp:     Exit timestamp (UTC).

        Returns:
            Realised PnL in cents (negative = loss).
        """
        pos = self.open_positions.pop(market_ticker, None)
        if pos is None:
            logger.warning(
                "record_trade_close called for unknown position: %s", market_ticker
            )
            return 0

        entry_price = pos["entry_price"]
        quantity = pos["quantity"]
        side = pos["side"]

        if side == "yes":
            gross_pnl = (exit_price - entry_price) * quantity
        else:
            # NO: profit when YES price falls
            gross_pnl = (entry_price - exit_price) * quantity

        # Fee formula mirrors strategy.HourlyStrategy.calc_fee() exactly.
        # Defined inline to avoid a circular import between the two modules.
        fee_per_contract = max(
            1, math.ceil(0.07 * entry_price * (100 - entry_price) / 100)
        )
        exit_fee_per_contract = max(
            1, math.ceil(0.07 * exit_price * (100 - exit_price) / 100)
        )
        net_pnl = gross_pnl - (fee_per_contract + exit_fee_per_contract) * quantity

        if net_pnl < 0:
            self.daily_loss_cents += abs(net_pnl)

        self._update_trade_csv_exit(
            market_ticker=market_ticker,
            exit_price=exit_price,
            pnl_cents=net_pnl,
            exit_reason=exit_reason,
            exit_time=timestamp,
        )

        logger.info(
            "Recorded exit: market=%s side=%s entry=%d¢ exit=%d¢ "
            "pnl=%d¢ reason=%s",
            market_ticker,
            side,
            entry_price,
            exit_price,
            net_pnl,
            exit_reason,
        )
        return net_pnl

    def record_exit(
        self,
        market_id: str,
        exit_price: int,
        exit_reason: str,
        exit_time: datetime,
    ) -> int:
        """Backward-compatible alias for record_trade_close(). Used by strategy.py."""
        return self.record_trade_close(market_id, exit_price, exit_reason, exit_time)

    def get_open_positions(self) -> list[dict]:
        """
        Return a list of all open positions opened by this bot.

        Returns only positions that have no exit yet (from in-memory state
        backed by trades_hourly.csv). Does NOT reflect the Kalshi account total.
        """
        return list(self.open_positions.values())

    def get_open_position(self, market_ticker: str) -> dict | None:
        """Return the position dict for a single market, or None if not held."""
        return self.open_positions.get(market_ticker)

    def get_daily_pnl(self) -> int:
        """
        Sum pnl_cents for all trades closed today (UTC date).

        Reads from trades_hourly.csv so the value survives bot restarts.

        Returns:
            Net PnL in cents for today's closed trades (can be negative).
        """
        csv_path = self._cfg.TRADES_CSV_PATH
        if not os.path.isfile(csv_path):
            return 0

        today = datetime.now(timezone.utc).date()
        total = 0
        try:
            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    close_ts = row.get("timestamp_close", "").strip()
                    if not close_ts or close_ts == "None":
                        continue
                    try:
                        close_date = datetime.fromisoformat(close_ts).date()
                    except ValueError:
                        continue
                    if close_date != today:
                        continue
                    try:
                        pnl_str = row.get("pnl_cents", "").strip()
                        if pnl_str:
                            total += int(pnl_str)
                    except ValueError:
                        pass
        except (OSError, csv.Error) as exc:
            logger.warning("Could not read trade CSV for daily PnL: %s", exc)

        return total

    def reset_daily_counters(self) -> None:
        """
        Explicitly reset daily trade count and loss tracker.

        Called automatically at midnight UTC by _reset_daily_counters_if_needed(),
        but can also be invoked directly (e.g. in tests or manual overrides).
        """
        logger.info(
            "Resetting daily counters (prev: trades=%d loss=%d¢).",
            self.daily_trades,
            self.daily_loss_cents,
        )
        self.daily_reset_date = datetime.now(timezone.utc).date()
        self.daily_trades = 0
        self.daily_loss_cents = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reset_daily_counters_if_needed(self) -> None:
        """Reset daily trade/loss counters at midnight UTC."""
        today = datetime.now(timezone.utc).date()
        if today != self.daily_reset_date:
            self.reset_daily_counters()

    def _append_trade_csv(self, row: dict) -> None:
        """Append a single row to the trade journal CSV."""
        csv_path = self._cfg.TRADES_CSV_PATH
        write_header = not os.path.isfile(csv_path) or os.path.getsize(csv_path) == 0
        try:
            with open(csv_path, "a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
                if write_header:
                    writer.writeheader()
                writer.writerow({col: row.get(col, "") for col in _CSV_COLUMNS})
        except OSError as exc:
            logger.error("Could not write to trade CSV %s: %s", csv_path, exc)

    def _update_trade_csv_exit(
        self,
        market_ticker: str,
        exit_price: int,
        pnl_cents: int,
        exit_reason: str,
        exit_time: datetime,
    ) -> None:
        """
        Update the last open row for market_ticker with exit details.

        Reads the entire CSV, updates the matching row in-memory, and rewrites
        the file.  Suitable for the low-frequency trading cadence of this bot.
        """
        csv_path = self._cfg.TRADES_CSV_PATH
        if not os.path.isfile(csv_path):
            logger.warning(
                "_update_trade_csv_exit: CSV %s not found", csv_path
            )
            return

        try:
            with open(csv_path, newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
        except (OSError, csv.Error) as exc:
            logger.error("Could not read trade CSV for update: %s", exc)
            return

        # Find the last open row for this market_ticker and update it
        updated = False
        for row in reversed(rows):
            if (
                row.get("market_ticker") == market_ticker
                and row.get("exit_price_cents", "").strip() in ("", "None")
            ):
                row["exit_price_cents"] = str(exit_price)
                row["timestamp_close"] = exit_time.isoformat()
                row["pnl_cents"] = str(pnl_cents)
                row["exit_reason"] = exit_reason
                updated = True
                break

        if not updated:
            logger.warning(
                "_update_trade_csv_exit: no open row found for %s", market_ticker
            )
            return

        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(rows)
        except OSError as exc:
            logger.error("Could not rewrite trade CSV: %s", exc)
