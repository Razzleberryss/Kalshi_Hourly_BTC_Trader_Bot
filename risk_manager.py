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
import os
from datetime import date, datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# CSV column order
_CSV_COLUMNS = [
    "timestamp",
    "market_id",
    "side",
    "quantity",
    "entry_price",
    "exit_price",
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

        Rows without an exit_price are considered still-open positions and are
        loaded back into self.open_positions so risk limits are enforced
        correctly after a bot restart.
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
                    # Only re-load rows from today (UTC) that are still open
                    try:
                        row_date = datetime.fromisoformat(row["timestamp"]).date()
                    except (ValueError, KeyError):
                        row_date = None

                    if row.get("exit_price", "").strip() in ("", "None"):
                        # Still open
                        market_id = row.get("market_id", "")
                        if market_id:
                            self.open_positions[market_id] = {
                                "market_id": market_id,
                                "side": row.get("side", "yes"),
                                "quantity": int(row.get("quantity", 1)),
                                "entry_price": int(row.get("entry_price", 50)),
                                "entry_time": row.get("timestamp", ""),
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

    def can_trade(self, balance_cents: int, order_cost_cents: int) -> tuple[bool, str]:
        """
        Check whether a new trade is allowed under current risk limits.

        Args:
            balance_cents:    Current available balance in cents.
            order_cost_cents: Total cost of the prospective order in cents.

        Returns:
            (True, "ok") if all checks pass.
            (False, reason_string) on the first failing check.
        """
        self._reset_daily_counters_if_needed()

        cfg = self._cfg

        if self.daily_loss_cents >= cfg.MAX_DAILY_LOSS_CENTS:
            return (
                False,
                f"Daily loss limit reached: {self.daily_loss_cents}¢ "
                f">= {cfg.MAX_DAILY_LOSS_CENTS}¢",
            )

        if self.daily_trades >= cfg.MAX_DAILY_TRADES:
            return (
                False,
                f"Daily trade limit reached: {self.daily_trades} "
                f">= {cfg.MAX_DAILY_TRADES}",
            )

        if len(self.open_positions) >= cfg.MAX_OPEN_POSITIONS:
            return (
                False,
                f"Max open positions reached: {len(self.open_positions)} "
                f">= {cfg.MAX_OPEN_POSITIONS}",
            )

        total_exposure = sum(
            p["entry_price"] * p["quantity"]
            for p in self.open_positions.values()
        )
        if total_exposure >= cfg.MAX_TOTAL_EXPOSURE_CENTS:
            return (
                False,
                f"Max total exposure reached: {total_exposure}¢ "
                f">= {cfg.MAX_TOTAL_EXPOSURE_CENTS}¢",
            )

        min_required = int(order_cost_cents * 1.2)
        if balance_cents < min_required:
            return (
                False,
                f"Insufficient balance: {balance_cents}¢ < "
                f"{min_required}¢ (1.2× order cost of {order_cost_cents}¢)",
            )

        return (True, "ok")

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

    def record_entry(
        self,
        market_id: str,
        side: str,
        quantity: int,
        entry_price: int,
        entry_time: datetime,
    ) -> None:
        """
        Record a new open position and append to the trade CSV.

        Args:
            market_id:   Market ticker.
            side:        "yes" or "no".
            quantity:    Contracts purchased.
            entry_price: Fill price in cents.
            entry_time:  Entry timestamp (UTC).
        """
        entry_ts = entry_time.isoformat()
        self.open_positions[market_id] = {
            "market_id": market_id,
            "side": side,
            "quantity": quantity,
            "entry_price": entry_price,
            "entry_time": entry_ts,
        }
        self.daily_trades += 1

        self._append_trade_csv(
            {
                "timestamp": entry_ts,
                "market_id": market_id,
                "side": side,
                "quantity": quantity,
                "entry_price": entry_price,
                "exit_price": "",
                "pnl_cents": "",
                "exit_reason": "",
            }
        )
        logger.info(
            "Recorded entry: market=%s side=%s qty=%d price=%d¢",
            market_id,
            side,
            quantity,
            entry_price,
        )

    def record_exit(
        self,
        market_id: str,
        exit_price: int,
        exit_reason: str,
        exit_time: datetime,
    ) -> int:
        """
        Record a position exit, update the CSV, and adjust daily loss tracker.

        Args:
            market_id:   Market ticker.
            exit_price:  Exit fill price in cents.
            exit_reason: Human-readable exit reason (e.g. "stop_loss").
            exit_time:   Exit timestamp (UTC).

        Returns:
            Realised PnL in cents (negative = loss).
        """
        pos = self.open_positions.pop(market_id, None)
        if pos is None:
            logger.warning(
                "record_exit called for unknown position: %s", market_id
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

        # Approximate fees (see strategy.calc_fee for the exact formula)
        fee_per_contract = max(
            1, int(0.07 * entry_price * (100 - entry_price) / 100 + 0.9999)
        )
        exit_fee_per_contract = max(
            1, int(0.07 * exit_price * (100 - exit_price) / 100 + 0.9999)
        )
        net_pnl = gross_pnl - (fee_per_contract + exit_fee_per_contract) * quantity

        if net_pnl < 0:
            self.daily_loss_cents += abs(net_pnl)

        self._update_trade_csv_exit(
            market_id=market_id,
            exit_price=exit_price,
            pnl_cents=net_pnl,
            exit_reason=exit_reason,
            exit_time=exit_time,
        )

        logger.info(
            "Recorded exit: market=%s side=%s entry=%d¢ exit=%d¢ "
            "pnl=%d¢ reason=%s",
            market_id,
            side,
            entry_price,
            exit_price,
            net_pnl,
            exit_reason,
        )
        return net_pnl

    def get_open_position(self, market_id: str) -> dict | None:
        """Return the position dict for a market, or None if not held."""
        return self.open_positions.get(market_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reset_daily_counters_if_needed(self) -> None:
        """Reset daily trade/loss counters at midnight UTC."""
        today = datetime.now(timezone.utc).date()
        if today != self.daily_reset_date:
            logger.info(
                "New trading day (%s) — resetting daily counters "
                "(prev: trades=%d loss=%d¢).",
                today,
                self.daily_trades,
                self.daily_loss_cents,
            )
            self.daily_reset_date = today
            self.daily_trades = 0
            self.daily_loss_cents = 0

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
        market_id: str,
        exit_price: int,
        pnl_cents: int,
        exit_reason: str,
        exit_time: datetime,
    ) -> None:
        """
        Update the last open row for market_id with exit details.

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

        # Find the last open row for this market_id and update it
        updated = False
        for row in reversed(rows):
            if (
                row.get("market_id") == market_id
                and row.get("exit_price", "").strip() in ("", "None")
            ):
                row["exit_price"] = str(exit_price)
                row["pnl_cents"] = str(pnl_cents)
                row["exit_reason"] = exit_reason
                row["timestamp"] = exit_time.isoformat()
                updated = True
                break

        if not updated:
            logger.warning(
                "_update_trade_csv_exit: no open row found for %s", market_id
            )
            return

        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(rows)
        except OSError as exc:
            logger.error("Could not rewrite trade CSV: %s", exc)
