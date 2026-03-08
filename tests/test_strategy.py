"""
tests/test_strategy.py — Unit tests for HourlyBTCStrategy.

Coverage:
  - Signal suppressed when confidence is below threshold
  - Signal suppressed when spread is too wide
  - check_time_to_expiry returns False near expiry, True with plenty of time
  - calculate_net_edge formula produces expected result
  - Momentum signal fires on a clear upward price move
  - Net edge below MIN_EDGE_CENTS suppresses an otherwise valid signal
  - check_spread returns True for a tight spread, False for a wide spread
"""

import math
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from strategy import HourlyBTCStrategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(**overrides):
    """Return a minimal config-like namespace for HourlyBTCStrategy tests."""
    defaults = dict(
        MIN_CONFIDENCE=0.12,
        MIN_EDGE_CENTS=3,
        MAX_SPREAD_CENTS=5,
        LOOKBACK_HOURS=3,
        TIME_TO_EXPIRY_MIN_MINUTES=5,
        MOMENTUM_THRESHOLD=0.5,
        TAKE_PROFIT_CENTS=22,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _balanced_orderbook(bid_yes: int = 48, bid_no: int = 48):
    """
    Build a simple orderbook with equal volume on both sides.

    spread = (100 - bid_no) - bid_yes = (100 - 48) - 48 = 4 ≤ 5
    """
    return {
        "yes": [[bid_yes, 100]],
        "no": [[bid_no, 100]],
    }


def _iso_future(minutes_from_now: float) -> str:
    """Return an ISO-8601 UTC timestamp ``minutes_from_now`` minutes in the future."""
    dt = datetime.now(timezone.utc) + timedelta(minutes=minutes_from_now)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_signal_none_below_confidence_threshold():
    """Flat price history + balanced book → confidence below threshold → no signal."""
    cfg = _make_cfg()
    strat = HourlyBTCStrategy(cfg)

    # Flat price history — zero momentum
    price_history = [50_000.0] * 5

    # Perfectly balanced orderbook — zero skew
    orderbook = _balanced_orderbook(bid_yes=48, bid_no=48)

    result = strat.get_signal("KXBTCH-25Dec31-T99999", orderbook, price_history)

    assert result["signal"] == "none"


def test_signal_none_when_spread_too_wide():
    """Spread > MAX_SPREAD_CENTS should suppress signal immediately."""
    cfg = _make_cfg(MAX_SPREAD_CENTS=5)
    strat = HourlyBTCStrategy(cfg)

    # bid_yes=45, bid_no=45 → spread = (100-45) - 45 = 10 > 5
    orderbook = _balanced_orderbook(bid_yes=45, bid_no=45)

    # Even a strong price move should not produce a signal if spread is too wide
    price_history = [50_000.0, 50_100.0, 50_200.0, 50_300.0, 50_400.0]

    result = strat.get_signal("KXBTCH-test", orderbook, price_history)

    assert result["signal"] == "none"


def test_signal_none_near_expiry():
    """check_time_to_expiry returns False when < 5 min remain, True otherwise."""
    cfg = _make_cfg(TIME_TO_EXPIRY_MIN_MINUTES=5)
    strat = HourlyBTCStrategy(cfg)

    market_near = {"close_time": _iso_future(3)}
    market_far = {"close_time": _iso_future(10)}

    assert strat.check_time_to_expiry(market_near) is False
    assert strat.check_time_to_expiry(market_far) is True


def test_calculate_net_edge_formula():
    """
    Manual verification of the fee formula:
      entry=35: fee = max(1, ceil(0.07 * 35 * 65 / 100)) = ceil(1.5925) = 2
      exit=57:  fee = max(1, ceil(0.07 * 57 * 43 / 100)) = ceil(1.7157) = 2
      gross_pnl = 57 - 35 = 22
      net_edge  = 22 - 2 - 2 = 18
    """
    cfg = _make_cfg()
    strat = HourlyBTCStrategy(cfg)

    entry_fee = max(1, math.ceil(0.07 * 35 * 65 / 100))
    exit_fee = max(1, math.ceil(0.07 * 57 * 43 / 100))
    assert entry_fee == 2
    assert exit_fee == 2

    result = strat.calculate_net_edge(35, 57, "yes")
    assert result == 18.0


def test_momentum_signal_fires_on_strong_move():
    """
    A clear upward move > MOMENTUM_THRESHOLD should produce a 'yes' signal
    with confidence >= MIN_CONFIDENCE (given a spread-valid orderbook).
    """
    cfg = _make_cfg(
        MOMENTUM_THRESHOLD=0.5,
        MIN_CONFIDENCE=0.12,
        LOOKBACK_HOURS=3,
    )
    strat = HourlyBTCStrategy(cfg)

    # The strategy uses the last LOOKBACK_HOURS=3 entries.
    # Window = [50_000, 50_250, 50_500] → pct_change = (50500-50000)/50000*100 = 1.0% > 0.5%
    price_history = [49_000.0, 49_500.0, 50_000.0, 50_250.0, 50_500.0]

    # Balanced book with tight spread
    orderbook = _balanced_orderbook(bid_yes=48, bid_no=48)

    result = strat.get_signal("KXBTCH-test", orderbook, price_history)

    assert result["signal"] == "yes"
    assert result["confidence"] >= cfg.MIN_CONFIDENCE


def test_net_edge_below_min_suppresses_signal():
    """
    When MIN_EDGE_CENTS is set very high, a valid momentum + skew signal is
    still suppressed because the net edge check fails.
    """
    cfg = _make_cfg(
        MIN_EDGE_CENTS=100,  # impossibly high
        MOMENTUM_THRESHOLD=0.5,
        MIN_CONFIDENCE=0.01,  # low threshold so confidence alone is not the blocker
        LOOKBACK_HOURS=3,
    )
    strat = HourlyBTCStrategy(cfg)

    # Strong upward momentum (last 3 entries: [50000, 50250, 50500] = 1.0% rise)
    price_history = [49_000.0, 49_500.0, 50_000.0, 50_250.0, 50_500.0]
    orderbook = _balanced_orderbook(bid_yes=48, bid_no=48)

    result = strat.get_signal("KXBTCH-test", orderbook, price_history)

    assert result["signal"] == "none"


def test_check_spread_true_when_tight():
    """
    bid_yes=48, bid_no=48 → spread = (100 - 48) - 48 = 4 ≤ 5 → True.
    """
    cfg = _make_cfg(MAX_SPREAD_CENTS=5)
    strat = HourlyBTCStrategy(cfg)

    orderbook = {
        "yes": [[48, 100]],
        "no": [[48, 100]],
    }

    assert strat.check_spread(orderbook) is True


def test_check_spread_false_when_wide():
    """
    bid_yes=45, bid_no=45 → spread = (100 - 45) - 45 = 10 > 5 → False.
    """
    cfg = _make_cfg(MAX_SPREAD_CENTS=5)
    strat = HourlyBTCStrategy(cfg)

    orderbook = {
        "yes": [[45, 100]],
        "no": [[45, 100]],
    }

    assert strat.check_spread(orderbook) is False
