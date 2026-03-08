import httpx
import time
import hashlib
import hmac
import base64
from urllib.parse import urlparse


class KalshiClient:
    """Async HTTP client for the Kalshi REST API v2."""

    def __init__(self, api_key: str, api_key_id: str, base_url: str):
        self.api_key = api_key
        self.api_key_id = api_key_id
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)

    def _sign_request(self, method: str, path: str) -> dict:
        """Generate HMAC-based auth headers for Kalshi API."""
        ts = str(int(time.time() * 1000))
        message = ts + method.upper() + path
        private_key_bytes = base64.b64decode(self.api_key)
        signature = hmac.new(private_key_bytes, message.encode(), hashlib.sha256).digest()
        sig_b64 = base64.b64encode(signature).decode()
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "Content-Type": "application/json",
        }

    async def get_markets(self, ticker_prefix: str = "KXBTC") -> dict:
        """Fetch active Kalshi markets filtered by ticker prefix."""
        path = f"/markets?ticker={ticker_prefix}&status=open"
        headers = self._sign_request("GET", path)
        response = await self._client.get(path, headers=headers)
        response.raise_for_status()
        return response.json()

    async def get_orderbook(self, ticker: str) -> dict:
        """Fetch the orderbook for a specific market ticker."""
        path = f"/markets/{ticker}/orderbook"
        headers = self._sign_request("GET", path)
        response = await self._client.get(path, headers=headers)
        response.raise_for_status()
        return response.json()

    async def place_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        yes_price: int,
    ) -> dict:
        """Place a limit order on a Kalshi market."""
        path = "/portfolio/orders"
        headers = self._sign_request("POST", path)
        payload = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": count,
            "type": "limit",
            "yes_price": yes_price,
        }
        response = await self._client.post(path, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()

    async def get_balance(self) -> dict:
        """Return the current portfolio balance."""
        path = "/portfolio/balance"
        headers = self._sign_request("GET", path)
        response = await self._client.get(path, headers=headers)
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self._client.aclose()
