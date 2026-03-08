"""
tests/test_risk_manager.py — Unit tests for RiskManager.

All tests use a temporary CSV file so they are fully isolated from each other
and from any real trades_hourly.csv on disk.

Coverage:
  - Trade blocked when daily loss limit is hit
  - Trade blocked when max open positions is hit
  - Trade blocked when insufficient balance
  - CSV correctly written on open and updated on close
  - get_daily_pnl() correctly sums only today's closed trades
  - get_open_positions() returns only bot-opened, still-open positions
  - reset_daily_counters() resets trades and loss in-memory
  - load_trades_from_csv() recovers open positions after a restart
"""

import csv
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from risk_manager import RiskManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CSV_COLUMNS = [
    "timestamp_open",
    "market_ticker",
    "side",
    "quantity",
    "entry_price_cents",
    "timestamp_close",
    "exit_price_cents",
    "pnl_cents",
    "exit_reason",
]


def _make_cfg(tmp_path, **overrides):
    """Return a minimal config-like namespace pointing at a temp CSV."""
    defaults = dict(
        TRADES_CSV_PATH=str(tmp_path / "trades_hourly.csv"),
        MAX_OPEN_POSITIONS=3,
        MAX_TOTAL_EXPOSURE_CENTS=10_000,
        MAX_DAILY_LOSS_CENTS=5_000,
        MAX_DAILY_TRADES=20,
        MAX_DOLLARS_PER_TRADE=25,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _read_csv(csv_path: str) -> list[dict]:
    """Read all rows from the trade CSV as a list of dicts."""
    if not os.path.isfile(csv_path):
        return []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def cfg(tmp_path):
    return _make_cfg(tmp_path)


@pytest.fixture()
def rm(cfg):
    return RiskManager(cfg=cfg)


# ---------------------------------------------------------------------------
# check_before_trade — balance check
# ---------------------------------------------------------------------------


class TestCheckBeforeTradeBalance:
    def test_blocked_when_insufficient_balance(self, rm):
        """Trade must be blocked when balance < order_cost * 1.2."""
        # order_cost = 10 * 50 = 500¢; 1.2× = 600¢; balance of 599¢ should fail
        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=10,
            price_cents=50,
            balance_cents=599,
        )
        assert ok is False
        assert "Insufficient balance" in reason

    def test_allowed_at_exact_threshold(self, rm):
        """Trade must be allowed when balance == order_cost * 1.2 exactly."""
        # order_cost = 1 * 50 = 50¢; 1.2× = 60¢
        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=1,
            price_cents=50,
            balance_cents=60,
        )
        assert ok is True
        assert reason == "ok"

    def test_allowed_with_ample_balance(self, rm):
        """Trade must be allowed when balance is well above the threshold."""
        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=2,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is True, reason


# ---------------------------------------------------------------------------
# check_before_trade — open positions limit
# ---------------------------------------------------------------------------


class TestCheckBeforeTradeOpenPositions:
    def test_blocked_when_max_positions_reached(self, rm, cfg):
        """Trade must be blocked when open positions == MAX_OPEN_POSITIONS."""
        now = _now_utc()
        # Fill up to the limit
        for i in range(cfg.MAX_OPEN_POSITIONS):
            rm.record_trade_open(
                market_ticker=f"KXBTCH-MKT{i}",
                side="yes",
                quantity=1,
                entry_price=30,
                timestamp=now,
            )

        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-NEW",
            side="yes",
            quantity=1,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is False
        assert "Max open positions" in reason

    def test_allowed_when_one_below_limit(self, rm, cfg):
        """Trade must be allowed when open positions is one below the limit."""
        now = _now_utc()
        for i in range(cfg.MAX_OPEN_POSITIONS - 1):
            rm.record_trade_open(
                market_ticker=f"KXBTCH-MKT{i}",
                side="yes",
                quantity=1,
                entry_price=30,
                timestamp=now,
            )

        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-NEW",
            side="yes",
            quantity=1,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is True, reason


# ---------------------------------------------------------------------------
# check_before_trade — daily loss circuit breaker
# ---------------------------------------------------------------------------


class TestCheckBeforeTradeDailyLoss:
    def test_blocked_when_daily_loss_limit_hit(self, rm, cfg):
        """Trade must be blocked once daily_loss_cents >= MAX_DAILY_LOSS_CENTS."""
        rm.daily_loss_cents = cfg.MAX_DAILY_LOSS_CENTS  # pin at the limit

        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=1,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is False
        assert "Daily loss limit" in reason

    def test_blocked_when_daily_loss_exceeds_limit(self, rm, cfg):
        """Trade must be blocked when daily loss is above the limit."""
        rm.daily_loss_cents = cfg.MAX_DAILY_LOSS_CENTS + 1

        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=1,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is False
        assert "Daily loss limit" in reason

    def test_allowed_when_loss_below_limit(self, rm, cfg):
        """Trade must be allowed when daily loss is below the limit."""
        rm.daily_loss_cents = cfg.MAX_DAILY_LOSS_CENTS - 1

        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=1,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is True, reason


# ---------------------------------------------------------------------------
# check_before_trade — daily trade count
# ---------------------------------------------------------------------------


class TestCheckBeforeTradeDailyTradeCount:
    def test_blocked_when_daily_trades_limit_hit(self, rm, cfg):
        rm.daily_trades = cfg.MAX_DAILY_TRADES

        ok, reason = rm.check_before_trade(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=1,
            price_cents=30,
            balance_cents=100_000,
        )
        assert ok is False
        assert "Daily trade limit" in reason


# ---------------------------------------------------------------------------
# CSV — record_trade_open writes a correct row
# ---------------------------------------------------------------------------


class TestCsvOnOpen:
    def test_row_written_on_open(self, rm, cfg):
        """record_trade_open must append a row with correct fields."""
        ts = datetime(2026, 3, 8, 12, 0, 0, tzinfo=timezone.utc)
        rm.record_trade_open(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=5,
            entry_price=42,
            timestamp=ts,
        )

        rows = _read_csv(cfg.TRADES_CSV_PATH)
        assert len(rows) == 1

        row = rows[0]
        assert row["market_ticker"] == "KXBTCH-24MAR1500"
        assert row["side"] == "yes"
        assert row["quantity"] == "5"
        assert row["entry_price_cents"] == "42"
        assert row["timestamp_open"] == ts.isoformat()
        # Exit fields must be empty on open
        assert row["exit_price_cents"] == ""
        assert row["timestamp_close"] == ""
        assert row["pnl_cents"] == ""
        assert row["exit_reason"] == ""

    def test_csv_header_matches_schema(self, rm, cfg):
        """CSV header must exactly match the specified schema."""
        rm.record_trade_open(
            market_ticker="KXBTCH-TEST",
            side="no",
            quantity=1,
            entry_price=55,
            timestamp=_now_utc(),
        )

        with open(cfg.TRADES_CSV_PATH, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            assert list(reader.fieldnames) == _CSV_COLUMNS

    def test_open_positions_updated_in_memory(self, rm):
        """record_trade_open must add the position to open_positions."""
        rm.record_trade_open(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=3,
            entry_price=40,
            timestamp=_now_utc(),
        )
        assert "KXBTCH-24MAR1500" in rm.open_positions
        pos = rm.open_positions["KXBTCH-24MAR1500"]
        assert pos["quantity"] == 3
        assert pos["entry_price"] == 40


# ---------------------------------------------------------------------------
# CSV — record_trade_close updates the matching open row
# ---------------------------------------------------------------------------


class TestCsvOnClose:
    def test_exit_fields_written_on_close(self, rm, cfg):
        """record_trade_close must fill exit_price_cents, timestamp_close, etc."""
        open_ts = datetime(2026, 3, 8, 12, 0, 0, tzinfo=timezone.utc)
        close_ts = datetime(2026, 3, 8, 12, 45, 0, tzinfo=timezone.utc)

        rm.record_trade_open(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=2,
            entry_price=40,
            timestamp=open_ts,
        )
        rm.record_trade_close(
            market_ticker="KXBTCH-24MAR1500",
            exit_price=62,
            exit_reason="take_profit",
            timestamp=close_ts,
        )

        rows = _read_csv(cfg.TRADES_CSV_PATH)
        assert len(rows) == 1

        row = rows[0]
        assert row["exit_price_cents"] == "62"
        assert row["timestamp_close"] == close_ts.isoformat()
        assert row["exit_reason"] == "take_profit"
        # pnl must be set (non-empty)
        assert row["pnl_cents"] != ""
        # Entry fields must be preserved
        assert row["timestamp_open"] == open_ts.isoformat()
        assert row["market_ticker"] == "KXBTCH-24MAR1500"

    def test_pnl_calculation_yes_profit(self, rm, cfg):
        """YES side profit: (exit - entry) * qty minus fees."""
        import math

        entry, exit_p, qty = 40, 62, 2
        rm.record_trade_open(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=qty,
            entry_price=entry,
            timestamp=_now_utc(),
        )
        pnl = rm.record_trade_close(
            market_ticker="KXBTCH-24MAR1500",
            exit_price=exit_p,
            exit_reason="take_profit",
            timestamp=_now_utc(),
        )

        gross = (exit_p - entry) * qty
        entry_fee = max(1, math.ceil(0.07 * entry * (100 - entry) / 100))
        exit_fee = max(1, math.ceil(0.07 * exit_p * (100 - exit_p) / 100))
        expected_pnl = gross - (entry_fee + exit_fee) * qty
        assert pnl == expected_pnl

    def test_pnl_calculation_yes_loss(self, rm, cfg):
        """YES side loss: negative PnL and daily_loss_cents incremented."""
        entry, exit_p, qty = 60, 25, 1
        rm.record_trade_open(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=qty,
            entry_price=entry,
            timestamp=_now_utc(),
        )
        pnl = rm.record_trade_close(
            market_ticker="KXBTCH-24MAR1500",
            exit_price=exit_p,
            exit_reason="stop_loss",
            timestamp=_now_utc(),
        )
        assert pnl < 0
        assert rm.daily_loss_cents == abs(pnl)

    def test_position_removed_from_open_on_close(self, rm):
        """record_trade_close must remove the position from open_positions."""
        rm.record_trade_open(
            market_ticker="KXBTCH-24MAR1500",
            side="yes",
            quantity=1,
            entry_price=50,
            timestamp=_now_utc(),
        )
        assert rm.get_open_position("KXBTCH-24MAR1500") is not None

        rm.record_trade_close(
            market_ticker="KXBTCH-24MAR1500",
            exit_price=50,
            exit_reason="manual",
            timestamp=_now_utc(),
        )
        assert rm.get_open_position("KXBTCH-24MAR1500") is None

    def test_close_unknown_position_returns_zero(self, rm):
        """Closing a position that was never opened must return 0, not raise."""
        pnl = rm.record_trade_close(
            market_ticker="KXBTCH-NONEXISTENT",
            exit_price=50,
            exit_reason="manual",
            timestamp=_now_utc(),
        )
        assert pnl == 0


# ---------------------------------------------------------------------------
# get_open_positions()
# ---------------------------------------------------------------------------


class TestGetOpenPositions:
    def test_returns_empty_list_initially(self, rm):
        assert rm.get_open_positions() == []

    def test_returns_open_positions_only(self, rm):
        """Only positions that have not been closed are returned."""
        now = _now_utc()
        rm.record_trade_open("KXBTCH-A", "yes", 1, 40, now)
        rm.record_trade_open("KXBTCH-B", "no", 2, 55, now)
        rm.record_trade_close("KXBTCH-A", 62, "take_profit", now)

        positions = rm.get_open_positions()
        tickers = {p["market_ticker"] for p in positions}
        assert tickers == {"KXBTCH-B"}

    def test_returns_list_not_dict(self, rm):
        rm.record_trade_open("KXBTCH-A", "yes", 1, 40, _now_utc())
        result = rm.get_open_positions()
        assert isinstance(result, list)
        assert isinstance(result[0], dict)


# ---------------------------------------------------------------------------
# get_daily_pnl()
# ---------------------------------------------------------------------------


class TestGetDailyPnl:
    def test_returns_zero_when_no_csv(self, rm):
        assert rm.get_daily_pnl() == 0

    def test_sums_only_todays_closed_trades(self, rm, cfg):
        """get_daily_pnl() must sum pnl_cents of today's closed trades only."""
        today = datetime.now(timezone.utc)
        yesterday = today - timedelta(days=1)

        # Write two rows directly into the CSV to avoid date-drift issues
        rows = [
            {
                "timestamp_open": today.isoformat(),
                "market_ticker": "KXBTCH-TODAY1",
                "side": "yes",
                "quantity": "1",
                "entry_price_cents": "40",
                "timestamp_close": today.isoformat(),
                "exit_price_cents": "62",
                "pnl_cents": "18",
                "exit_reason": "take_profit",
            },
            {
                "timestamp_open": today.isoformat(),
                "market_ticker": "KXBTCH-TODAY2",
                "side": "yes",
                "quantity": "1",
                "entry_price_cents": "60",
                "timestamp_close": today.isoformat(),
                "exit_price_cents": "25",
                "pnl_cents": "-38",
                "exit_reason": "stop_loss",
            },
            {
                "timestamp_open": yesterday.isoformat(),
                "market_ticker": "KXBTCH-YESTERDAY",
                "side": "yes",
                "quantity": "1",
                "entry_price_cents": "40",
                "timestamp_close": yesterday.isoformat(),
                "exit_price_cents": "62",
                "pnl_cents": "100",
                "exit_reason": "take_profit",
            },
            {
                "timestamp_open": today.isoformat(),
                "market_ticker": "KXBTCH-OPEN",
                "side": "yes",
                "quantity": "1",
                "entry_price_cents": "40",
                "timestamp_close": "",
                "exit_price_cents": "",
                "pnl_cents": "",
                "exit_reason": "",
            },
        ]
        with open(cfg.TRADES_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

        daily_pnl = rm.get_daily_pnl()
        # 18 + (-38) = -20; yesterday's 100 and the open row must be excluded
        assert daily_pnl == 18 + (-38)

    def test_returns_zero_when_all_trades_open(self, rm, cfg):
        """get_daily_pnl() must return 0 when no trades are closed yet."""
        rm.record_trade_open("KXBTCH-A", "yes", 1, 40, _now_utc())
        assert rm.get_daily_pnl() == 0

    def test_accumulates_via_record_trade_close(self, rm):
        """get_daily_pnl() integrates with record_trade_open/record_trade_close."""
        now = _now_utc()
        rm.record_trade_open("KXBTCH-A", "yes", 2, 40, now)
        pnl = rm.record_trade_close("KXBTCH-A", 62, "take_profit", now)

        assert rm.get_daily_pnl() == pnl


# ---------------------------------------------------------------------------
# reset_daily_counters()
# ---------------------------------------------------------------------------


class TestResetDailyCounters:
    def test_resets_trades_and_loss(self, rm):
        rm.daily_trades = 15
        rm.daily_loss_cents = 3_000

        rm.reset_daily_counters()

        assert rm.daily_trades == 0
        assert rm.daily_loss_cents == 0

    def test_reset_unblocks_trading(self, rm, cfg):
        """After reset, trades that were blocked by the daily loss limit are allowed again."""
        rm.daily_loss_cents = cfg.MAX_DAILY_LOSS_CENTS  # hit the limit

        ok, _ = rm.check_before_trade("KXBTCH-X", "yes", 1, 30, 100_000)
        assert ok is False  # blocked

        rm.reset_daily_counters()

        ok, reason = rm.check_before_trade("KXBTCH-X", "yes", 1, 30, 100_000)
        assert ok is True, reason


# ---------------------------------------------------------------------------
# load_trades_from_csv() — restart recovery
# ---------------------------------------------------------------------------


class TestLoadTradesFromCsv:
    def test_recovers_open_positions_on_restart(self, cfg):
        """Open positions written to CSV must be restored after a bot restart."""
        today = datetime.now(timezone.utc)
        rows = [
            {
                "timestamp_open": today.isoformat(),
                "market_ticker": "KXBTCH-OPEN1",
                "side": "yes",
                "quantity": "3",
                "entry_price_cents": "45",
                "timestamp_close": "",
                "exit_price_cents": "",
                "pnl_cents": "",
                "exit_reason": "",
            },
        ]
        with open(cfg.TRADES_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

        # Fresh RiskManager simulating a restart
        rm2 = RiskManager(cfg=cfg)
        rm2.load_trades_from_csv()

        assert "KXBTCH-OPEN1" in rm2.open_positions
        pos = rm2.open_positions["KXBTCH-OPEN1"]
        assert pos["quantity"] == 3
        assert pos["entry_price"] == 45

    def test_does_not_reload_closed_positions(self, cfg):
        """Closed positions (exit_price_cents set) must not be added to open_positions."""
        today = datetime.now(timezone.utc)
        rows = [
            {
                "timestamp_open": today.isoformat(),
                "market_ticker": "KXBTCH-CLOSED",
                "side": "yes",
                "quantity": "1",
                "entry_price_cents": "40",
                "timestamp_close": today.isoformat(),
                "exit_price_cents": "62",
                "pnl_cents": "18",
                "exit_reason": "take_profit",
            },
        ]
        with open(cfg.TRADES_CSV_PATH, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

        rm2 = RiskManager(cfg=cfg)
        rm2.load_trades_from_csv()

        assert len(rm2.open_positions) == 0

    def test_no_error_when_csv_missing(self, cfg):
        """load_trades_from_csv must not raise if the CSV file does not exist."""
        assert not os.path.isfile(cfg.TRADES_CSV_PATH)
        rm2 = RiskManager(cfg=cfg)
        rm2.load_trades_from_csv()  # must not raise
        assert rm2.open_positions == {}
