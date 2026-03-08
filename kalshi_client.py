"""
kalshi_client.py — Synchronous Kalshi REST API v2 client with RSA-PSS auth.

Authentication uses RSA-PSS (SHA-256) as required by Kalshi's API.
All methods handle errors gracefully: they catch exceptions, log a warning,
and return None / [] / 0 rather than raising — so a single bad market never
crashes the bot.
"""

import base64
import json
import logging
import time
from typing import Literal

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)


class KalshiClient:
    """
    Synchronous HTTP client for the Kalshi REST API v2.

    Args:
        api_key_id:       Your Kalshi API key ID.
        private_key_path: Path to an RSA-2048 private key PEM file.
        base_url:         API base URL (default from config.KALSHI_BASE_URL).
    """

    def __init__(self, api_key_id: str, private_key_path: str, base_url: str) -> None:
        self.api_key_id = api_key_id
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()

        # Load RSA private key from PEM file once at startup
        with open(private_key_path, "rb") as key_file:
            self._private_key = serialization.load_pem_private_key(
                key_file.read(), password=None
            )
        logger.info("KalshiClient initialised (base_url=%s)", self.base_url)

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _sign_request(self, method: str, path: str, body: str = "") -> dict:
        """
        Build RSA-PSS authentication headers.

        Kalshi expects:
          - KALSHI-ACCESS-KEY:       your key ID
          - KALSHI-ACCESS-TIMESTAMP: milliseconds since epoch (string)
          - KALSHI-ACCESS-SIGNATURE: base64( RSA-PSS-SHA256( ts + METHOD + path + body ) )
        """
        ts_ms = str(int(time.time() * 1000))
        message = (ts_ms + method.upper() + path + body).encode("utf-8")

        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        sig_b64 = base64.b64encode(signature).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "Content-Type": "application/json",
        }

    def _get(self, path: str) -> requests.Response | None:
        """Perform a signed GET request; return Response or None on error."""
        headers = self._sign_request("GET", path)
        try:
            resp = self._session.get(self.base_url + path, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            logger.warning(
                "GET %s → HTTP %s: %s",
                path,
                exc.response.status_code if exc.response is not None else "?",
                exc.response.text[:300] if exc.response is not None else str(exc),
            )
        except requests.RequestException as exc:
            logger.warning("GET %s → request error: %s", path, exc)
        return None

    def _post(self, path: str, payload: dict) -> requests.Response | None:
        """Perform a signed POST request; return Response or None on error."""
        body_str = json.dumps(payload, separators=(",", ":"))
        headers = self._sign_request("POST", path, body_str)
        try:
            resp = self._session.post(
                self.base_url + path,
                headers=headers,
                data=body_str,
                timeout=10,
            )
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            text = exc.response.text[:300] if exc.response is not None else str(exc)
            logger.warning("POST %s → HTTP %s: %s", path, status, text)

            # Surface specific error flavours for callers that care
            if exc.response is not None:
                if exc.response.status_code == 409:
                    logger.warning("POST %s → 409 Conflict (duplicate order?)", path)
                elif exc.response.status_code in (401, 403):
                    logger.warning(
                        "POST %s → auth error — check KALSHI_API_KEY_ID / key file", path
                    )
        except requests.RequestException as exc:
            logger.warning("POST %s → request error: %s", path, exc)
        return None

    # ------------------------------------------------------------------
    # Market discovery
    # ------------------------------------------------------------------

    def get_markets(self, series_ticker: str) -> list[dict]:
        """
        Return open markets for a series ticker.

        GET /markets?series_ticker={series_ticker}&status=open

        Returns:
            List of market dicts, or [] on error.
        """
        path = f"/markets?series_ticker={series_ticker}&status=open"
        resp = self._get(path)
        if resp is None:
            return []
        data = resp.json()
        markets = data.get("markets", [])
        logger.debug("get_markets(%s) → %d markets", series_ticker, len(markets))
        return markets

    def get_market(self, market_ticker: str) -> dict | None:
        """
        Return a single market by ticker.

        GET /markets/{market_ticker}

        Returns:
            Market dict or None on error.
        """
        path = f"/markets/{market_ticker}"
        resp = self._get(path)
        if resp is None:
            return None
        return resp.json().get("market")

    def get_series_markets(self, series_ticker: str) -> list[dict]:
        """
        Return markets for a series.  Tries the series-specific endpoint first;
        falls back to the generic market search.

        Returns:
            List of market dicts, or [] on error.
        """
        # Try /series/{ticker}/markets first
        path = f"/series/{series_ticker}/markets"
        resp = self._get(path)
        if resp is not None:
            markets = resp.json().get("markets", [])
            logger.debug(
                "get_series_markets(%s) via /series → %d markets",
                series_ticker,
                len(markets),
            )
            return markets

        # Fallback: generic market search
        return self.get_markets(series_ticker)

    # ------------------------------------------------------------------
    # Orderbook
    # ------------------------------------------------------------------

    def get_orderbook(self, market_ticker: str) -> dict | None:
        """
        Return the orderbook for a market.

        GET /markets/{market_ticker}/orderbook

        Returns:
            Orderbook dict or None on error.
        """
        path = f"/markets/{market_ticker}/orderbook"
        resp = self._get(path)
        if resp is None:
            return None
        return resp.json().get("orderbook")

    # ------------------------------------------------------------------
    # Portfolio
    # ------------------------------------------------------------------

    def get_balance(self) -> int:
        """
        Return available balance in cents.

        GET /portfolio/balance

        Returns:
            available_balance (int, cents) or 0 on error.
        """
        resp = self._get("/portfolio/balance")
        if resp is None:
            return 0
        balance = resp.json().get("balance", {})
        # API may return either {"balance": <int>} or {"balance": {"available_balance": <int>}}
        if isinstance(balance, dict):
            return int(balance.get("available_balance", 0))
        return int(balance)

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def place_order_yes(
        self, market_id: str, quantity: int, price: int, dry_run: bool
    ) -> dict | None:
        """
        Buy YES contracts on a market.

        Args:
            market_id: The market ticker (e.g. "KXBTCH-25JUL1200").
            quantity:  Number of contracts.
            price:     Limit price in cents (1–99).
            dry_run:   If True, log the order but do not send it.

        Returns:
            Order response dict, a simulated dict for dry-run, or None on error.
        """
        if dry_run:
            logger.info(
                "[DRY-RUN] Would place YES order: market=%s qty=%d price=%d¢",
                market_id,
                quantity,
                price,
            )
            return {
                "order_id": "dry-run-yes",
                "ticker": market_id,
                "side": "yes",
                "action": "buy",
                "count": quantity,
                "yes_price": price,
                "status": "dry_run",
            }

        payload = {
            "ticker": market_id,
            "side": "yes",
            "action": "buy",
            "type": "limit",
            "count": quantity,
            "yes_price": price,
        }
        resp = self._post("/portfolio/orders", payload)
        if resp is None:
            return None
        result = resp.json()
        logger.info(
            "Placed YES order: market=%s qty=%d price=%d¢ → order_id=%s",
            market_id,
            quantity,
            price,
            result.get("order", {}).get("order_id", "?"),
        )
        return result

    def place_order_no(
        self, market_id: str, quantity: int, price: int, dry_run: bool
    ) -> dict | None:
        """
        Buy NO contracts on a market.

        The Kalshi API represents NO price as a YES price: yes_price = 100 - no_price.

        Args:
            market_id: The market ticker.
            quantity:  Number of contracts.
            price:     NO limit price in cents (1–99).
            dry_run:   If True, log the order but do not send it.

        Returns:
            Order response dict, a simulated dict for dry-run, or None on error.
        """
        yes_price = 100 - price  # Convert NO price to YES price for the API

        # Validate computed yes_price is within the Kalshi-accepted range (1–99)
        if not 1 <= yes_price <= 99:
            logger.warning(
                "place_order_no: computed yes_price=%d is out of range [1,99] "
                "(no_price=%d) — order rejected.",
                yes_price,
                price,
            )
            return None

        if dry_run:
            logger.info(
                "[DRY-RUN] Would place NO order: market=%s qty=%d no_price=%d¢ (yes_price=%d¢)",
                market_id,
                quantity,
                price,
                yes_price,
            )
            return {
                "order_id": "dry-run-no",
                "ticker": market_id,
                "side": "no",
                "action": "buy",
                "count": quantity,
                "yes_price": yes_price,
                "no_price": price,
                "status": "dry_run",
            }

        payload = {
            "ticker": market_id,
            "side": "no",
            "action": "buy",
            "type": "limit",
            "count": quantity,
            "yes_price": yes_price,
        }
        resp = self._post("/portfolio/orders", payload)
        if resp is None:
            return None
        result = resp.json()
        logger.info(
            "Placed NO order: market=%s qty=%d no_price=%d¢ (yes_price=%d¢) → order_id=%s",
            market_id,
            quantity,
            price,
            yes_price,
            result.get("order", {}).get("order_id", "?"),
        )
        return result

    def close_position(
        self,
        market_id: str,
        side: Literal["yes", "no"],
        quantity: int,
        price: int,
        dry_run: bool,
    ) -> dict | None:
        """
        Close (sell) an existing position.

        Args:
            market_id: The market ticker.
            side:      "yes" or "no" — which side to sell.
            quantity:  Number of contracts to close.
            price:     Limit price in cents.
            dry_run:   If True, log the action but do not send it.

        Returns:
            Order response dict, a simulated dict for dry-run, or None on error.
        """
        if dry_run:
            logger.info(
                "[DRY-RUN] Would close position: market=%s side=%s qty=%d price=%d¢",
                market_id,
                side,
                quantity,
                price,
            )
            return {
                "order_id": "dry-run-close",
                "ticker": market_id,
                "side": side,
                "action": "sell",
                "count": quantity,
                "yes_price": price if side == "yes" else 100 - price,
                "status": "dry_run",
            }

        yes_price = price if side == "yes" else 100 - price
        payload = {
            "ticker": market_id,
            "side": side,
            "action": "sell",
            "type": "limit",
            "count": quantity,
            "yes_price": yes_price,
        }
        resp = self._post("/portfolio/orders", payload)
        if resp is None:
            return None
        result = resp.json()
        logger.info(
            "Closed position: market=%s side=%s qty=%d price=%d¢ → order_id=%s",
            market_id,
            side,
            quantity,
            price,
            result.get("order", {}).get("order_id", "?"),
        )
        return result
