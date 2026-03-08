"""
kalshi_client.py — Synchronous Kalshi REST API v2 client with RSA-PSS auth.

Authentication uses RSA-PSS (SHA-256) as required by Kalshi's API.
All methods handle errors gracefully: they catch exceptions, log a warning,
and return None / [] / 0 rather than raising — so a single bad market never
crashes the bot.

Error-handling contract:
  - 401 / 403  → re-raised as ``KalshiAuthError`` (misconfigured credentials).
  - 409 Conflict → logged as warning, returns None (market_closed / duplicate).
  - 400 Bad Request → logged as warning with body excerpt, returns None.
                        "insufficient_balance" body triggers a distinct log line.
  - 5xx Server Error → retried once; returns None if second attempt also fails.
  - Network errors   → logged as warning, returns None.
"""

import base64
import csv
import json
import logging
import os
import time
from typing import Literal

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)


class KalshiAuthError(RuntimeError):
    """Raised when the Kalshi API returns a 401 or 403 (authentication failure)."""


class KalshiClient:
    """
    Synchronous HTTP client for the Kalshi REST API v2.

    Args:
        api_key_id:       Your Kalshi API key ID.
        private_key_path: Path to an RSA-2048 private key PEM file.
        base_url:         API base URL (default from config.KALSHI_BASE_URL).
        trades_csv_path:  Path to the trade journal CSV used to filter positions
                          opened by this bot.  Defaults to "trades_hourly.csv".
        btc_series_ticker: BTC hourly series ticker (e.g. "KXBTCH").
                           Defaults to ``config.BTC_SERIES_TICKER_HOURLY`` when
                           not provided.
    """

    def __init__(
        self,
        api_key_id: str,
        private_key_path: str,
        base_url: str,
        trades_csv_path: str = "trades_hourly.csv",
        btc_series_ticker: str = "KXBTCH",
    ) -> None:
        self.api_key_id = api_key_id
        self.base_url = base_url.rstrip("/")
        self._trades_csv_path = trades_csv_path
        self._btc_series_ticker = btc_series_ticker
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

    def _get(self, path: str, *, _retry: bool = True) -> requests.Response | None:
        """Perform a signed GET request; return Response or None on error.

        Retries once on 5xx server errors.  Re-raises ``KalshiAuthError`` on
        401 / 403 so the caller (and the bot startup) can surface the problem
        clearly instead of silently returning None.
        """
        headers = self._sign_request("GET", path)
        try:
            resp = self._session.get(self.base_url + path, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            text = exc.response.text[:300] if exc.response is not None else str(exc)

            if status in (401, 403):
                raise KalshiAuthError(
                    f"GET {path} → HTTP {status}: authentication failed. "
                    "Check KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH."
                ) from exc

            if status is not None and status >= 500 and _retry:
                logger.warning(
                    "GET %s → HTTP %s (server error) — retrying once…", path, status
                )
                return self._get(path, _retry=False)

            logger.warning("GET %s → HTTP %s: %s", path, status, text)
        except requests.RequestException as exc:
            logger.warning("GET %s → request error: %s", path, exc)
        return None

    def _post(
        self, path: str, payload: dict, *, _retry: bool = True
    ) -> requests.Response | None:
        """Perform a signed POST request; return Response or None on error.

        Error-handling contract:
          - 401 / 403 → re-raised as ``KalshiAuthError``.
          - 409 Conflict → logged as warning, returns None (market closed etc.).
          - 400 Bad Request → "insufficient_balance" gets a distinct log line;
                              other 400s are logged generically.  Returns None.
          - 5xx → retried once, then returns None.
        """
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
            status = exc.response.status_code if exc.response is not None else None
            text = exc.response.text[:300] if exc.response is not None else str(exc)

            if status in (401, 403):
                raise KalshiAuthError(
                    f"POST {path} → HTTP {status}: authentication failed. "
                    "Check KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH."
                ) from exc

            if status == 409:
                logger.warning(
                    "POST %s → 409 Conflict (market_closed or duplicate order): %s",
                    path,
                    text,
                )
                return None

            if status == 400:
                if "insufficient_balance" in text.lower():
                    logger.warning(
                        "POST %s → 400 insufficient_balance: %s", path, text
                    )
                else:
                    logger.warning("POST %s → 400 Bad Request: %s", path, text)
                return None

            if status is not None and status >= 500 and _retry:
                logger.warning(
                    "POST %s → HTTP %s (server error) — retrying once…", path, status
                )
                return self._post(path, payload, _retry=False)

            logger.warning("POST %s → HTTP %s: %s", path, status, text)
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

    def get_active_btc_hourly_markets(self) -> list[dict]:
        """
        Return currently open BTC hourly markets using the configured series ticker.

        Uses ``self._btc_series_ticker`` (set from ``config.BTC_SERIES_TICKER_HOURLY``
        at construction time).  Tries the series-specific endpoint first, then
        falls back to the generic market search.

        Returns:
            List of open market dicts, or [] on error.
        """
        markets = self.get_series_markets(self._btc_series_ticker)
        # Filter to only open/active markets.  Markets whose status field is
        # absent (empty string) are also kept — the Kalshi API occasionally
        # omits status on very-recently-opened markets; they should be traded.
        open_markets = [m for m in markets if m.get("status") in ("open", "active", "")]
        logger.debug(
            "get_active_btc_hourly_markets(%s) → %d open markets",
            self._btc_series_ticker,
            len(open_markets),
        )
        return open_markets

    # ------------------------------------------------------------------
    # Orderbook
    # ------------------------------------------------------------------

    def get_orderbook(self, market_ticker: str) -> dict | None:
        """
        Return the orderbook for a market, enriched with best bid/ask prices.

        GET /markets/{market_ticker}/orderbook

        The raw Kalshi orderbook lists ``yes`` and ``no`` bid levels as
        ``[[price, size], ...]`` sorted descending by price.  This method
        adds convenience keys so callers don't have to parse the levels
        themselves:

        * ``best_bid_yes`` — highest YES bid price (cents).
        * ``best_ask_yes`` — implied YES ask price = 100 − best_bid_no.
        * ``best_bid_no``  — highest NO bid price (cents).
        * ``best_ask_no``  — implied NO ask price = 100 − best_bid_yes.

        Returns:
            Enriched orderbook dict (with ``yes`` / ``no`` level lists and the
            four best-price keys above), or None on error.
        """
        path = f"/markets/{market_ticker}/orderbook"
        resp = self._get(path)
        if resp is None:
            return None
        ob = resp.json().get("orderbook")
        if ob is None:
            return None

        yes_levels: list[list[int]] = ob.get("yes") or []
        no_levels: list[list[int]] = ob.get("no") or []

        best_bid_yes: int | None = yes_levels[0][0] if yes_levels else None
        best_bid_no: int | None = no_levels[0][0] if no_levels else None

        ob["best_bid_yes"] = best_bid_yes
        ob["best_ask_yes"] = (100 - best_bid_no) if best_bid_no is not None else None
        ob["best_bid_no"] = best_bid_no
        ob["best_ask_no"] = (100 - best_bid_yes) if best_bid_yes is not None else None

        return ob

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

    def get_positions(self) -> list[dict]:
        """
        Return only positions opened by THIS bot.

        Instead of fetching the full account position list from the API (which
        would include manual trades and positions from other bots), this method
        reads ``trades_hourly.csv`` and returns in-memory records for rows that
        have no ``exit_price`` yet.

        Returns:
            List of open-position dicts, each containing at minimum:
            ``market_id``, ``side``, ``quantity``, ``entry_price``,
            ``entry_time``.  Returns [] if the CSV is absent or unreadable.
        """
        csv_path = self._trades_csv_path
        if not os.path.isfile(csv_path):
            logger.debug("get_positions: no trade CSV at %s — returning [].", csv_path)
            return []

        open_positions: list[dict] = []
        try:
            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    # "None" (string) can appear when Python None was written to
                    # the CSV via str(); treat it the same as an empty field.
                    # Both patterns indicate the position has not been closed yet.
                    if row.get("exit_price", "").strip() in ("", "None"):
                        open_positions.append(
                            {
                                "market_id": row.get("market_id", ""),
                                "side": row.get("side", "yes"),
                                "quantity": int(row.get("quantity", 1)),
                                "entry_price": int(row.get("entry_price", 50)),
                                "entry_time": row.get("timestamp", ""),
                            }
                        )
        except (OSError, csv.Error, ValueError) as exc:
            logger.warning("get_positions: could not read trade CSV: %s", exc)
            return []

        logger.debug("get_positions: %d open position(s) from CSV.", len(open_positions))
        return open_positions

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

    def place_order(
        self,
        market_ticker: str,
        side: Literal["yes", "no"],
        action: Literal["buy", "sell"],
        quantity: int,
        price: int,
        dry_run: bool,
    ) -> dict | None:
        """
        Place a limit order on a market (unified entry point).

        Routing:
          - ``action="buy",  side="yes"`` → calls :meth:`place_order_yes`.
          - ``action="buy",  side="no"``  → calls :meth:`place_order_no`.
          - ``action="sell"``             → calls :meth:`close_position`
                                            (side determines which contracts to sell).

        Args:
            market_ticker: The market ticker (e.g. "KXBTCH-25JUL1200").
            side:          "yes" or "no".
            action:        "buy" (enter) or "sell" (exit).
            quantity:      Number of contracts.
            price:         Limit price in cents (1–99).
            dry_run:       If True, log the order but do not call the API.

        Returns:
            Order response dict, a dry-run stub, or None on error.
        """
        if action == "sell":
            return self.close_position(
                market_id=market_ticker,
                side=side,
                quantity=quantity,
                price=price,
                dry_run=dry_run,
            )
        if side == "yes":
            return self.place_order_yes(
                market_id=market_ticker,
                quantity=quantity,
                price=price,
                dry_run=dry_run,
            )
        # side == "no"
        return self.place_order_no(
            market_id=market_ticker,
            quantity=quantity,
            price=price,
            dry_run=dry_run,
        )
