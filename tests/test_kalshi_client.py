"""
tests/test_kalshi_client.py — Unit tests for KalshiClient.

All tests mock the network layer (requests.Session) and the RSA private key
so they run without real credentials or network access.

Coverage:
  - Auth header generation (_sign_request)
  - Successful order placement (place_order_yes / place_order_no / place_order)
  - Dry-run mode: no HTTP call is made
  - 409 Conflict handling: returns None, does NOT raise
  - 401 Auth error: raises KalshiAuthError
  - 400 insufficient_balance: returns None, does NOT raise
  - 5xx retry: _get / _post retry once then return None
  - get_orderbook: enriches response with best bid/ask keys
  - get_active_btc_hourly_markets: filters by status, falls back gracefully
  - get_positions: reads from a CSV, returns open positions
  - get_balance: handles both int and dict balance shapes
"""

import csv
import io
import json
import os
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from kalshi_client import KalshiClient, KalshiAuthError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def private_key_pem(tmp_path):
    """Generate a real (small) RSA private key and write it to a temp file."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_file = tmp_path / "test_key.pem"
    key_file.write_bytes(pem)
    return str(key_file)


@pytest.fixture()
def client(private_key_pem):
    """Return a KalshiClient wired to a real RSA key but with no real CSV."""
    return KalshiClient(
        api_key_id="test-key-id",
        private_key_path=private_key_pem,
        base_url="https://api.elections.kalshi.com/trade-api/v2",
        trades_csv_path="/nonexistent/trades_hourly.csv",
        btc_series_ticker="KXBTCH",
    )


def _make_response(status_code: int, body: dict | str) -> MagicMock:
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    if isinstance(body, dict):
        resp.text = json.dumps(body)
        resp.json.return_value = body
    else:
        resp.text = body
        resp.json.return_value = {}
    return resp


def _http_error(status_code: int, body: dict | str = "") -> Exception:
    """Build a requests.HTTPError with a mock response."""
    import requests
    resp = _make_response(status_code, body)
    err = requests.HTTPError(response=resp)
    return err


# ---------------------------------------------------------------------------
# Auth header tests
# ---------------------------------------------------------------------------

class TestSignRequest:
    def test_returns_required_headers(self, client):
        headers = client._sign_request("GET", "/markets")
        assert "KALSHI-ACCESS-KEY" in headers
        assert "KALSHI-ACCESS-TIMESTAMP" in headers
        assert "KALSHI-ACCESS-SIGNATURE" in headers
        assert "Content-Type" in headers

    def test_access_key_matches_api_key_id(self, client):
        headers = client._sign_request("GET", "/markets")
        assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"

    def test_timestamp_is_numeric_string(self, client):
        headers = client._sign_request("POST", "/portfolio/orders", '{"count":1}')
        ts = headers["KALSHI-ACCESS-TIMESTAMP"]
        assert ts.isdigit(), f"Expected numeric timestamp, got {ts!r}"
        # Should be milliseconds — at least 13 digits for any date after 2001
        assert len(ts) >= 13

    def test_signature_is_base64(self, client):
        import base64
        headers = client._sign_request("GET", "/markets")
        sig = headers["KALSHI-ACCESS-SIGNATURE"]
        # Should decode without error
        decoded = base64.b64decode(sig)
        assert len(decoded) > 0

    def test_different_methods_produce_different_signatures(self, client):
        h_get = client._sign_request("GET", "/markets")
        h_post = client._sign_request("POST", "/markets")
        # Signatures differ because the message differs
        assert h_get["KALSHI-ACCESS-SIGNATURE"] != h_post["KALSHI-ACCESS-SIGNATURE"]

    def test_body_included_in_signature_message(self, client):
        """Two identical requests with different bodies must yield different sigs."""
        h1 = client._sign_request("POST", "/portfolio/orders", '{"count":1}')
        h2 = client._sign_request("POST", "/portfolio/orders", '{"count":2}')
        assert h1["KALSHI-ACCESS-SIGNATURE"] != h2["KALSHI-ACCESS-SIGNATURE"]


# ---------------------------------------------------------------------------
# place_order_yes tests
# ---------------------------------------------------------------------------

class TestPlaceOrderYes:
    def test_dry_run_returns_stub_without_api_call(self, client):
        with patch.object(client._session, "post") as mock_post:
            result = client.place_order_yes(
                market_id="KXBTCH-25JUL1200",
                quantity=5,
                price=45,
                dry_run=True,
            )
        mock_post.assert_not_called()
        assert result is not None
        assert result["status"] == "dry_run"
        assert result["side"] == "yes"
        assert result["count"] == 5

    def test_live_places_order_and_returns_response(self, client):
        api_resp = _make_response(200, {"order": {"order_id": "ord-123"}})
        with patch.object(client._session, "post", return_value=api_resp):
            result = client.place_order_yes(
                market_id="KXBTCH-25JUL1200",
                quantity=2,
                price=55,
                dry_run=False,
            )
        assert result is not None
        assert result["order"]["order_id"] == "ord-123"

    def test_live_409_returns_none(self, client):
        with patch.object(
            client._session, "post", side_effect=_http_error(409, "market_closed")
        ):
            result = client.place_order_yes(
                market_id="KXBTCH-25JUL1200",
                quantity=1,
                price=50,
                dry_run=False,
            )
        assert result is None

    def test_live_401_raises_auth_error(self, client):
        with patch.object(
            client._session, "post", side_effect=_http_error(401, "unauthorized")
        ):
            with pytest.raises(KalshiAuthError):
                client.place_order_yes(
                    market_id="KXBTCH-25JUL1200",
                    quantity=1,
                    price=50,
                    dry_run=False,
                )

    def test_live_400_insufficient_balance_returns_none(self, client):
        with patch.object(
            client._session,
            "post",
            side_effect=_http_error(400, '{"error":"insufficient_balance"}'),
        ):
            result = client.place_order_yes(
                market_id="KXBTCH-25JUL1200",
                quantity=1,
                price=50,
                dry_run=False,
            )
        assert result is None


# ---------------------------------------------------------------------------
# place_order_no tests
# ---------------------------------------------------------------------------

class TestPlaceOrderNo:
    def test_dry_run_returns_stub_without_api_call(self, client):
        with patch.object(client._session, "post") as mock_post:
            result = client.place_order_no(
                market_id="KXBTCH-25JUL1200",
                quantity=3,
                price=40,
                dry_run=True,
            )
        mock_post.assert_not_called()
        assert result is not None
        assert result["status"] == "dry_run"
        assert result["side"] == "no"

    def test_yes_price_conversion(self, client):
        """NO price 40¢ should be sent as YES price 60¢."""
        captured_payload = {}

        def fake_post(url, headers=None, data=None, timeout=None):
            captured_payload.update(json.loads(data))
            return _make_response(200, {"order": {"order_id": "ord-no-1"}})

        with patch.object(client._session, "post", side_effect=fake_post):
            client.place_order_no(
                market_id="KXBTCH-25JUL1200",
                quantity=1,
                price=40,
                dry_run=False,
            )
        assert captured_payload["yes_price"] == 60

    def test_out_of_range_no_price_returns_none(self, client):
        """NO price 0 → yes_price 100 which is invalid (> 99)."""
        result = client.place_order_no(
            market_id="KXBTCH-25JUL1200",
            quantity=1,
            price=0,
            dry_run=False,
        )
        assert result is None


# ---------------------------------------------------------------------------
# Generic place_order tests
# ---------------------------------------------------------------------------

class TestPlaceOrder:
    def test_buy_yes_delegates_to_place_order_yes(self, client):
        with patch.object(client, "place_order_yes", return_value={"status": "ok"}) as m:
            result = client.place_order(
                market_ticker="KXBTCH-25JUL1200",
                side="yes",
                action="buy",
                quantity=2,
                price=45,
                dry_run=False,
            )
        m.assert_called_once_with(
            market_id="KXBTCH-25JUL1200", quantity=2, price=45, dry_run=False
        )
        assert result == {"status": "ok"}

    def test_buy_no_delegates_to_place_order_no(self, client):
        with patch.object(client, "place_order_no", return_value={"status": "ok"}) as m:
            result = client.place_order(
                market_ticker="KXBTCH-25JUL1200",
                side="no",
                action="buy",
                quantity=1,
                price=35,
                dry_run=True,
            )
        m.assert_called_once_with(
            market_id="KXBTCH-25JUL1200", quantity=1, price=35, dry_run=True
        )

    def test_sell_delegates_to_close_position(self, client):
        with patch.object(client, "close_position", return_value={"status": "ok"}) as m:
            client.place_order(
                market_ticker="KXBTCH-25JUL1200",
                side="yes",
                action="sell",
                quantity=2,
                price=60,
                dry_run=False,
            )
        m.assert_called_once_with(
            market_id="KXBTCH-25JUL1200",
            side="yes",
            quantity=2,
            price=60,
            dry_run=False,
        )

    def test_dry_run_no_api_call(self, client):
        with patch.object(client._session, "post") as mock_post:
            result = client.place_order(
                market_ticker="KXBTCH-25JUL1200",
                side="yes",
                action="buy",
                quantity=1,
                price=50,
                dry_run=True,
            )
        mock_post.assert_not_called()
        assert result is not None


# ---------------------------------------------------------------------------
# 409 handling
# ---------------------------------------------------------------------------

class TestConflictHandling:
    """409 Conflict must log a warning and return None — never raise."""

    def test_post_409_returns_none(self, client):
        with patch.object(
            client._session, "post", side_effect=_http_error(409, "market_closed")
        ):
            result = client._post("/portfolio/orders", {"ticker": "X"})
        assert result is None

    def test_get_409_returns_none(self, client):
        # GET 409 is unusual but should be handled gracefully
        with patch.object(
            client._session, "get", side_effect=_http_error(409, "conflict")
        ):
            result = client._get("/markets/X")
        assert result is None


# ---------------------------------------------------------------------------
# Auth error handling
# ---------------------------------------------------------------------------

class TestAuthError:
    def test_get_401_raises(self, client):
        with patch.object(
            client._session, "get", side_effect=_http_error(401, "unauthorized")
        ):
            with pytest.raises(KalshiAuthError, match="authentication failed"):
                client._get("/portfolio/balance")

    def test_get_403_raises(self, client):
        with patch.object(
            client._session, "get", side_effect=_http_error(403, "forbidden")
        ):
            with pytest.raises(KalshiAuthError, match="authentication failed"):
                client._get("/portfolio/balance")

    def test_post_401_raises(self, client):
        with patch.object(
            client._session, "post", side_effect=_http_error(401, "unauthorized")
        ):
            with pytest.raises(KalshiAuthError):
                client._post("/portfolio/orders", {})

    def test_post_403_raises(self, client):
        with patch.object(
            client._session, "post", side_effect=_http_error(403, "forbidden")
        ):
            with pytest.raises(KalshiAuthError):
                client._post("/portfolio/orders", {})


# ---------------------------------------------------------------------------
# 5xx retry tests
# ---------------------------------------------------------------------------

class TestRetryOn5xx:
    def test_get_retries_once_on_500(self, client):
        err_resp = _make_response(500, "internal server error")
        ok_resp = _make_response(200, {"markets": []})
        with patch.object(
            client._session, "get", side_effect=[
                __import__("requests").HTTPError(response=err_resp),
                ok_resp,
            ]
        ) as mock_get:
            result = client._get("/markets")
        assert mock_get.call_count == 2
        assert result is ok_resp

    def test_get_returns_none_after_two_failures(self, client):
        err_resp = _make_response(503, "service unavailable")
        import requests as req
        with patch.object(
            client._session, "get", side_effect=req.HTTPError(response=err_resp)
        ) as mock_get:
            result = client._get("/markets")
        assert mock_get.call_count == 2
        assert result is None

    def test_post_retries_once_on_500(self, client):
        err_resp = _make_response(500, "server error")
        ok_resp = _make_response(200, {"order": {"order_id": "x"}})
        import requests as req
        with patch.object(
            client._session, "post", side_effect=[
                req.HTTPError(response=err_resp),
                ok_resp,
            ]
        ) as mock_post:
            result = client._post("/portfolio/orders", {"ticker": "X"})
        assert mock_post.call_count == 2
        assert result is ok_resp


# ---------------------------------------------------------------------------
# get_orderbook tests
# ---------------------------------------------------------------------------

class TestGetOrderbook:
    def test_enriches_with_best_bid_ask(self, client):
        raw_ob = {
            "yes": [[55, 10], [50, 5]],
            "no": [[42, 8], [38, 3]],
        }
        api_resp = _make_response(200, {"orderbook": raw_ob})
        with patch.object(client._session, "get", return_value=api_resp):
            ob = client.get_orderbook("KXBTCH-25JUL1200")

        assert ob is not None
        assert ob["best_bid_yes"] == 55
        assert ob["best_bid_no"] == 42
        assert ob["best_ask_yes"] == 100 - 42  # 58
        assert ob["best_ask_no"] == 100 - 55   # 45

    def test_empty_levels_give_none_prices(self, client):
        raw_ob = {"yes": [], "no": []}
        api_resp = _make_response(200, {"orderbook": raw_ob})
        with patch.object(client._session, "get", return_value=api_resp):
            ob = client.get_orderbook("KXBTCH-25JUL1200")

        assert ob["best_bid_yes"] is None
        assert ob["best_ask_yes"] is None

    def test_returns_none_on_api_error(self, client):
        with patch.object(client._session, "get", side_effect=_http_error(404, "not found")):
            ob = client.get_orderbook("KXBTCH-MISSING")
        assert ob is None


# ---------------------------------------------------------------------------
# get_active_btc_hourly_markets tests
# ---------------------------------------------------------------------------

class TestGetActiveBtcHourlyMarkets:
    def test_returns_open_markets_only(self, client):
        markets_payload = {
            "markets": [
                {"ticker": "KXBTCH-A", "status": "open"},
                {"ticker": "KXBTCH-B", "status": "closed"},
                {"ticker": "KXBTCH-C", "status": "active"},
                {"ticker": "KXBTCH-D", "status": "settled"},
            ]
        }
        # /series/KXBTCH/markets endpoint
        api_resp = _make_response(200, markets_payload)
        with patch.object(client._session, "get", return_value=api_resp):
            result = client.get_active_btc_hourly_markets()

        tickers = [m["ticker"] for m in result]
        assert "KXBTCH-A" in tickers
        assert "KXBTCH-C" in tickers
        assert "KXBTCH-B" not in tickers
        assert "KXBTCH-D" not in tickers

    def test_returns_empty_list_on_api_error(self, client):
        with patch.object(client._session, "get", side_effect=_http_error(500, "error")):
            result = client.get_active_btc_hourly_markets()
        assert result == []


# ---------------------------------------------------------------------------
# get_positions tests
# ---------------------------------------------------------------------------

class TestGetPositions:
    def _write_csv(self, path: str, rows: list[dict]) -> None:
        fieldnames = [
            "timestamp", "market_id", "side", "quantity",
            "entry_price", "exit_price", "exit_timestamp", "pnl_cents", "exit_reason",
        ]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({f: row.get(f, "") for f in fieldnames})

    def test_returns_only_open_positions(self, private_key_pem, tmp_path):
        csv_path = str(tmp_path / "trades.csv")
        self._write_csv(csv_path, [
            {
                "timestamp": "2026-03-08T10:00:00+00:00",
                "market_id": "KXBTCH-A",
                "side": "yes",
                "quantity": 2,
                "entry_price": 45,
                "exit_price": "",   # still open
            },
            {
                "timestamp": "2026-03-08T09:00:00+00:00",
                "market_id": "KXBTCH-B",
                "side": "no",
                "quantity": 1,
                "entry_price": 40,
                "exit_price": "60",  # already closed
                "pnl_cents": "20",
                "exit_reason": "take_profit",
            },
        ])
        c = KalshiClient(
            api_key_id="test-key-id",
            private_key_path=private_key_pem,
            base_url="https://api.elections.kalshi.com/trade-api/v2",
            trades_csv_path=csv_path,
        )
        positions = c.get_positions()
        assert len(positions) == 1
        assert positions[0]["market_id"] == "KXBTCH-A"
        assert positions[0]["side"] == "yes"
        assert positions[0]["quantity"] == 2

    def test_returns_empty_list_when_csv_missing(self, client):
        # client fixture uses /nonexistent/trades_hourly.csv
        result = client.get_positions()
        assert result == []

    def test_all_open_when_no_exits(self, private_key_pem, tmp_path):
        csv_path = str(tmp_path / "trades.csv")
        self._write_csv(csv_path, [
            {"market_id": "KXBTCH-X", "side": "yes", "quantity": 1, "entry_price": 50, "exit_price": ""},
            {"market_id": "KXBTCH-Y", "side": "no", "quantity": 3, "entry_price": 35, "exit_price": ""},
        ])
        c = KalshiClient(
            api_key_id="k",
            private_key_path=private_key_pem,
            base_url="https://api.elections.kalshi.com/trade-api/v2",
            trades_csv_path=csv_path,
        )
        assert len(c.get_positions()) == 2


# ---------------------------------------------------------------------------
# get_balance tests
# ---------------------------------------------------------------------------

class TestGetBalance:
    def test_returns_int_balance(self, client):
        api_resp = _make_response(200, {"balance": 12345})
        with patch.object(client._session, "get", return_value=api_resp):
            bal = client.get_balance()
        assert bal == 12345

    def test_returns_nested_balance(self, client):
        api_resp = _make_response(200, {"balance": {"available_balance": 9876}})
        with patch.object(client._session, "get", return_value=api_resp):
            bal = client.get_balance()
        assert bal == 9876

    def test_returns_zero_on_error(self, client):
        with patch.object(client._session, "get", side_effect=_http_error(500, "error")):
            bal = client.get_balance()
        assert bal == 0
