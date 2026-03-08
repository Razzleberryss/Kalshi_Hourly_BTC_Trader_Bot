"""
tests/test_integration_dryrun.py — Integration tests for the main loop in DRY-RUN mode.

Checklist covered:
  - Bot starts up in DRY-RUN mode without errors (config.validate passes)
  - Config validation catches missing env vars
  - Main loop runs 3 cycles in DRY-RUN without placing real orders
  - All open position exit checks run without crashing:
      stop-loss, take-profit, expiry, signal-reversal, max-hold
  - Daily loss circuit breaker halts the loop (run_one_cycle returns False)
  - Graceful shutdown closes all open positions cleanly

All external I/O (KalshiClient, RiskManager) is mocked so no network calls are made.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import config
from bot import _close_all_positions, run_one_cycle


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_market(ticker: str, minutes_to_expiry: float = 30.0) -> dict:
    """Return a minimal market dict with a future close_time."""
    close_dt = datetime.now(timezone.utc) + timedelta(minutes=minutes_to_expiry)
    return {
        "ticker": ticker,
        "status": "active",
        "close_time": close_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _balanced_orderbook(bid_yes: int = 48, bid_no: int = 48) -> dict:
    """Return a simple balanced orderbook with equal volumes on both sides."""
    return {
        "yes": [[bid_yes, 100]],
        "no": [[bid_no, 100]],
    }


def _no_signal() -> dict:
    return {
        "signal": "none",
        "confidence": 0.0,
        "entry_price_cents": 50,
        "expected_net_edge_cents": 0.0,
    }


def _patch_config(**overrides):
    """
    Return a context manager that patches bot.config attributes for one test.

    Provides safe defaults for all attributes accessed by run_one_cycle.
    """
    defaults = dict(
        STOP_LOSS_CENTS=35,
        TAKE_PROFIT_CENTS=22,
        MAX_HOLD_MINUTES=50,
        MIN_CONFIDENCE=0.12,
        MIN_EDGE_CENTS=3,
        MAX_DAILY_LOSS_CENTS=5000,
    )
    defaults.update(overrides)
    return patch.multiple("bot.config", **defaults)


# ---------------------------------------------------------------------------
# 1. Config validation
# ---------------------------------------------------------------------------

class TestConfigValidation:
    """config.validate() must raise ValueError for every required missing var."""

    def test_missing_api_key_id_raises(self, monkeypatch):
        """Raises ValueError when KALSHI_API_KEY_ID is empty."""
        monkeypatch.setattr(config, "KALSHI_API_KEY_ID", "")
        monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", "/dev/null")
        monkeypatch.setattr(config, "KALSHI_ENV", "demo")

        with pytest.raises(ValueError, match="KALSHI_API_KEY_ID"):
            config.validate()

    def test_missing_private_key_path_raises(self, monkeypatch):
        """Raises ValueError when KALSHI_PRIVATE_KEY_PATH is empty."""
        monkeypatch.setattr(config, "KALSHI_API_KEY_ID", "test-key-id")
        monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", "")
        monkeypatch.setattr(config, "KALSHI_ENV", "demo")

        with pytest.raises(ValueError, match="KALSHI_PRIVATE_KEY_PATH"):
            config.validate()

    def test_nonexistent_key_file_raises(self, monkeypatch):
        """Raises ValueError when KALSHI_PRIVATE_KEY_PATH points to a missing file."""
        monkeypatch.setattr(config, "KALSHI_API_KEY_ID", "test-key-id")
        monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", "/nonexistent/key.pem")
        monkeypatch.setattr(config, "KALSHI_ENV", "demo")

        with pytest.raises(ValueError, match="KALSHI_PRIVATE_KEY_PATH"):
            config.validate()

    def test_invalid_kalshi_env_raises(self, monkeypatch, tmp_path):
        """Raises ValueError when KALSHI_ENV is neither 'demo' nor 'prod'."""
        key_file = tmp_path / "key.pem"
        key_file.write_text("fake-key")
        monkeypatch.setattr(config, "KALSHI_API_KEY_ID", "test-key-id")
        monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", str(key_file))
        monkeypatch.setattr(config, "KALSHI_ENV", "staging")

        with pytest.raises(ValueError, match="KALSHI_ENV"):
            config.validate()

    def test_valid_demo_config_does_not_raise(self, monkeypatch, tmp_path):
        """validate() succeeds with valid demo credentials."""
        key_file = tmp_path / "key.pem"
        key_file.write_text("fake-key")
        monkeypatch.setattr(config, "KALSHI_API_KEY_ID", "test-key-id")
        monkeypatch.setattr(config, "KALSHI_PRIVATE_KEY_PATH", str(key_file))
        monkeypatch.setattr(config, "KALSHI_ENV", "demo")
        monkeypatch.setattr(config, "MAX_DAILY_LOSS_CENTS", 5000)
        monkeypatch.setattr(config, "MAX_OPEN_POSITIONS", 3)
        monkeypatch.setattr(config, "STOP_LOSS_CENTS", 35)
        monkeypatch.setattr(config, "TAKE_PROFIT_CENTS", 22)

        # Should not raise
        config.validate()


# ---------------------------------------------------------------------------
# 2. Three dry-run cycles — no real orders placed
# ---------------------------------------------------------------------------

class TestDryRunCycles:
    """Three full DRY-RUN cycles with mocked dependencies → no real orders."""

    def _make_mocks(self):
        client = MagicMock()
        client.get_active_btc_hourly_markets.return_value = []
        client.get_balance.return_value = 100_000

        risk_manager = MagicMock()
        risk_manager.open_positions = {}
        risk_manager.daily_loss_cents = 0
        risk_manager.daily_trades = 0
        risk_manager.get_open_positions.return_value = []
        risk_manager.get_open_position.return_value = None
        risk_manager.compute_max_trade_size_dollars.return_value = 25
        risk_manager.check_before_trade.return_value = (True, "ok")

        strategy = MagicMock()
        strategy.check_time_to_expiry.return_value = True
        strategy.check_spread.return_value = True
        strategy.get_signal.return_value = _no_signal()

        return client, risk_manager, strategy

    def test_three_cycles_no_active_markets(self):
        """3 DRY-RUN cycles with no active markets → place_order never called."""
        client, risk_manager, strategy = self._make_mocks()

        with _patch_config():
            for _ in range(3):
                result = run_one_cycle(
                    client=client,
                    risk_manager=risk_manager,
                    strategy=strategy,
                    dry_run=True,
                    price_history=[],
                )
                assert result is True, "run_one_cycle should return True (continue)"

        client.place_order.assert_not_called()

    def test_three_cycles_market_exists_but_no_signal(self):
        """3 DRY-RUN cycles with 1 active market but no signal → no orders."""
        client, risk_manager, strategy = self._make_mocks()
        market = _make_market("KXBTCH-25Jan01-T99999")
        client.get_active_btc_hourly_markets.return_value = [market]
        client.get_orderbook.return_value = _balanced_orderbook()

        with _patch_config():
            for _ in range(3):
                result = run_one_cycle(
                    client=client,
                    risk_manager=risk_manager,
                    strategy=strategy,
                    dry_run=True,
                    price_history=[],
                )
                assert result is True

        client.place_order.assert_not_called()

    def test_dry_run_does_not_call_place_order_with_live_flag(self):
        """Even if a signal fires, dry_run=True means close_position receives dry_run=True."""
        client, risk_manager, strategy = self._make_mocks()
        ticker = "KXBTCH-25Jan01-T99999"
        market = _make_market(ticker)
        client.get_active_btc_hourly_markets.return_value = [market]
        client.get_orderbook.return_value = _balanced_orderbook()

        strategy.get_signal.return_value = {
            "signal": "yes",
            "confidence": 0.8,
            "entry_price_cents": 35,
            "expected_net_edge_cents": 10.0,
        }
        client.place_order.return_value = {"order_id": "dry-run-test"}

        with _patch_config():
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        # If place_order was called, it must have been called with dry_run=True
        for call in client.place_order.call_args_list:
            assert call.kwargs.get("dry_run") is True or (
                len(call.args) >= 5 and call.args[4] is True
            ), "place_order must receive dry_run=True"


# ---------------------------------------------------------------------------
# 3. Open position exit checks
# ---------------------------------------------------------------------------

class TestOpenPositionExits:
    """Each exit trigger fires the correct exit_reason via record_trade_close."""

    def _base_mocks(self, ticker: str, position: dict):
        market = _make_market(ticker, minutes_to_expiry=30)
        client = MagicMock()
        client.get_active_btc_hourly_markets.return_value = [market]
        client.get_orderbook.return_value = _balanced_orderbook()
        client.get_balance.return_value = 100_000
        client.close_position.return_value = {"order_id": "close-test"}

        risk_manager = MagicMock()
        risk_manager.open_positions = {ticker: position}
        risk_manager.daily_loss_cents = 0
        risk_manager.daily_trades = 0
        risk_manager.get_open_positions.return_value = [position]
        # Already holding this position — suppress entry evaluation
        risk_manager.get_open_position.return_value = position

        strategy = MagicMock()
        strategy.check_time_to_expiry.return_value = True
        strategy.check_spread.return_value = False  # Suppress entry
        strategy.get_signal.return_value = _no_signal()

        return client, risk_manager, strategy

    def _assert_exit_reason(self, risk_manager: MagicMock, expected_reason: str):
        """Assert that record_trade_close was called once with the expected reason."""
        risk_manager.record_trade_close.assert_called_once()
        call_args = risk_manager.record_trade_close.call_args
        # Accept both positional and keyword argument styles
        if call_args.kwargs:
            actual_reason = call_args.kwargs.get("exit_reason")
        else:
            actual_reason = call_args.args[2]
        assert actual_reason == expected_reason, (
            f"Expected exit_reason={expected_reason!r}, got {actual_reason!r}"
        )

    def test_stop_loss_triggers_close(self):
        """
        Position down > STOP_LOSS_CENTS → record_trade_close called with reason=stop_loss.

        Orderbook: bid_yes=5, bid_no=75
        mid = (5 + (100-75)) // 2 = (5+25)//2 = 15
        PnL(YES) = 15 - 50 = -35  →  -35 <= -STOP_LOSS_CENTS(35)  → triggered
        """
        ticker = "KXBTCH-stopl"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=50,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        client.get_orderbook.return_value = {"yes": [[5, 100]], "no": [[75, 100]]}

        with _patch_config(STOP_LOSS_CENTS=35):
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        self._assert_exit_reason(risk_manager, "stop_loss")

    def test_take_profit_triggers_close(self):
        """
        Position up > TAKE_PROFIT_CENTS → record_trade_close with reason=take_profit.

        Orderbook: bid_yes=60, bid_no=36
        mid = (60 + (100-36)) // 2 = (60+64)//2 = 62
        PnL(YES) = 62 - 30 = 32  →  32 >= TAKE_PROFIT_CENTS(22)  → triggered
        """
        ticker = "KXBTCH-takep"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=30,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        client.get_orderbook.return_value = {"yes": [[60, 100]], "no": [[36, 100]]}

        with _patch_config(TAKE_PROFIT_CENTS=22):
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        self._assert_exit_reason(risk_manager, "take_profit")

    def test_expiry_triggers_close_when_strategy_blocks(self):
        """
        strategy.check_time_to_expiry returns False → record_trade_close with reason=expiry.
        """
        ticker = "KXBTCH-expiry"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=48,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        # Trigger: check_time_to_expiry returns False (near expiry)
        strategy.check_time_to_expiry.return_value = False
        # Balanced orderbook → PnL ≈ 0, no stop/take trigger
        client.get_orderbook.return_value = _balanced_orderbook()

        with _patch_config():
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        self._assert_exit_reason(risk_manager, "expiry")

    def test_expiry_triggers_close_when_market_gone(self):
        """
        Position market not in active market list → record_trade_close with reason=expiry.
        """
        ticker = "KXBTCH-gone"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=48,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        # Active markets list contains a *different* ticker
        different_market = _make_market("KXBTCH-other", minutes_to_expiry=30)
        client.get_active_btc_hourly_markets.return_value = [different_market]
        # Balanced orderbook → PnL ≈ 0
        client.get_orderbook.return_value = _balanced_orderbook()

        with _patch_config():
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        self._assert_exit_reason(risk_manager, "expiry")

    def test_signal_reversal_triggers_close(self):
        """
        New signal is opposite to open position side → record_trade_close with
        reason=signal_reversal.
        """
        ticker = "KXBTCH-rev"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=48,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        # Balanced orderbook → PnL ≈ 0, no stop/take trigger
        client.get_orderbook.return_value = _balanced_orderbook()
        # Return 'no' signal — reversal from 'yes' position
        strategy.get_signal.return_value = {
            "signal": "no",
            "confidence": 0.5,
            "entry_price_cents": 48,
            "expected_net_edge_cents": 5.0,
        }

        with _patch_config():
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        self._assert_exit_reason(risk_manager, "signal_reversal")

    def test_max_hold_triggers_close(self):
        """
        Position held > MAX_HOLD_MINUTES → record_trade_close with reason=max_hold.
        """
        ticker = "KXBTCH-hold"
        old_entry = (datetime.now(timezone.utc) - timedelta(minutes=55)).isoformat()
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=48,
            entry_time=old_entry,
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        # Balanced orderbook → PnL ≈ 0, no stop/take trigger
        client.get_orderbook.return_value = _balanced_orderbook()

        with _patch_config(MAX_HOLD_MINUTES=50):
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        self._assert_exit_reason(risk_manager, "max_hold")

    def test_no_exit_trigger_does_not_close(self):
        """
        All exit conditions absent → record_trade_close never called.
        """
        ticker = "KXBTCH-hold"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=1,
            entry_price=48,
            entry_time=datetime.now(timezone.utc).isoformat(),  # Just entered
        )
        client, risk_manager, strategy = self._base_mocks(ticker, position)
        # Balanced orderbook → PnL ≈ 0, well within stop/take thresholds
        client.get_orderbook.return_value = _balanced_orderbook()

        with _patch_config():
            run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        risk_manager.record_trade_close.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Daily loss circuit breaker
# ---------------------------------------------------------------------------

class TestCircuitBreaker:
    """run_one_cycle returns False when the daily loss limit is exceeded."""

    def test_circuit_breaker_halts_loop(self):
        """
        daily_loss_cents >= MAX_DAILY_LOSS_CENTS → run_one_cycle returns False.
        """
        client = MagicMock()
        client.get_active_btc_hourly_markets.return_value = []
        client.get_balance.return_value = 100_000

        risk_manager = MagicMock()
        risk_manager.open_positions = {}
        risk_manager.daily_loss_cents = 6_000   # > MAX_DAILY_LOSS_CENTS=5000
        risk_manager.daily_trades = 5
        risk_manager.get_open_positions.return_value = []

        strategy = MagicMock()

        with _patch_config(MAX_DAILY_LOSS_CENTS=5_000):
            result = run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        assert result is False, "run_one_cycle should return False on circuit breaker"

    def test_loop_continues_below_limit(self):
        """
        daily_loss_cents < MAX_DAILY_LOSS_CENTS → run_one_cycle returns True.
        """
        client = MagicMock()
        client.get_active_btc_hourly_markets.return_value = []
        client.get_balance.return_value = 100_000

        risk_manager = MagicMock()
        risk_manager.open_positions = {}
        risk_manager.daily_loss_cents = 4_999   # < MAX_DAILY_LOSS_CENTS=5000
        risk_manager.daily_trades = 5
        risk_manager.get_open_positions.return_value = []

        strategy = MagicMock()

        with _patch_config(MAX_DAILY_LOSS_CENTS=5_000):
            result = run_one_cycle(
                client=client,
                risk_manager=risk_manager,
                strategy=strategy,
                dry_run=True,
                price_history=[],
            )

        assert result is True


# ---------------------------------------------------------------------------
# 5. Graceful shutdown
# ---------------------------------------------------------------------------

class TestGracefulShutdown:
    """_close_all_positions() closes every open position and records exits."""

    def test_closes_all_open_positions(self):
        """
        All open positions get a close_position call and a record_trade_close call.
        """
        ticker = "KXBTCH-shutdown"
        position = dict(
            market_ticker=ticker,
            side="yes",
            quantity=2,
            entry_price=45,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )

        client = MagicMock()
        client.get_orderbook.return_value = {"yes": [[48, 100]], "no": [[48, 100]]}
        client.close_position.return_value = {"order_id": "shutdown-close"}

        risk_manager = MagicMock()
        risk_manager.get_open_positions.return_value = [position]

        _close_all_positions(client, risk_manager, dry_run=True)

        client.close_position.assert_called_once_with(
            market_id=ticker,
            side="yes",
            quantity=2,
            price=48,
            dry_run=True,
        )
        risk_manager.record_trade_close.assert_called_once()
        call_args = risk_manager.record_trade_close.call_args
        if call_args.kwargs:
            assert call_args.kwargs.get("exit_reason") == "shutdown"
        else:
            assert call_args.args[2] == "shutdown"

    def test_no_positions_is_noop(self):
        """_close_all_positions() does nothing when there are no open positions."""
        client = MagicMock()
        risk_manager = MagicMock()
        risk_manager.get_open_positions.return_value = []

        _close_all_positions(client, risk_manager, dry_run=True)

        client.close_position.assert_not_called()
        risk_manager.record_trade_close.assert_not_called()

    def test_fallback_to_entry_price_when_no_orderbook(self):
        """
        When get_orderbook returns None, entry_price is used as the close price.
        """
        ticker = "KXBTCH-nob"
        position = dict(
            market_ticker=ticker,
            side="no",
            quantity=1,
            entry_price=42,
            entry_time=datetime.now(timezone.utc).isoformat(),
        )

        client = MagicMock()
        client.get_orderbook.return_value = None
        client.close_position.return_value = {"order_id": "no-book-close"}

        risk_manager = MagicMock()
        risk_manager.get_open_positions.return_value = [position]

        _close_all_positions(client, risk_manager, dry_run=True)

        client.close_position.assert_called_once_with(
            market_id=ticker,
            side="no",
            quantity=1,
            price=42,       # entry_price used as fallback
            dry_run=True,
        )
