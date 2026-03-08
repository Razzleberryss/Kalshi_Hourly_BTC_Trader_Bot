import logging
from typing import Optional
from kalshi_client import KalshiClient

logger = logging.getLogger(__name__)

# Minimum edge required before placing a trade (in cents, e.g. 5 = $0.05)
MIN_EDGE_CENTS = int(5)
# Max contracts to buy per cycle
MAX_CONTRACTS = int(10)
# BTC market ticker prefix on Kalshi
BTC_TICKER_PREFIX = "KXBTC"


class HourlyBTCStrategy:
    """
    Simple hourly BTC trading strategy for Kalshi.
    Each cycle:
      1. Fetches open BTC hourly markets.
      2. Evaluates edge based on best bid/ask.
      3. Places a YES or NO order if edge exceeds MIN_EDGE_CENTS.
    """

    def __init__(self, client: KalshiClient):
        self.client = client

    async def run_cycle(self):
        """Execute one full trading cycle."""
        logger.info("Starting hourly trading cycle.")

        balance_data = await self.client.get_balance()
        balance_cents = balance_data.get("balance", 0)
        logger.info("Current balance: %d cents", balance_cents)

        markets_data = await self.client.get_markets(ticker_prefix=BTC_TICKER_PREFIX)
        markets = markets_data.get("markets", [])
        logger.info("Found %d open BTC markets.", len(markets))

        for market in markets:
            ticker = market.get("ticker")
            if not ticker:
                continue
            await self._evaluate_and_trade(ticker)

        logger.info("Hourly trading cycle complete.")

    async def _evaluate_and_trade(self, ticker: str):
        """Evaluate a single market and place an order if edge is sufficient."""
        try:
            book = await self.client.get_orderbook(ticker)
        except Exception as e:
            logger.warning("Could not fetch orderbook for %s: %s", ticker, e)
            return

        yes_bids = book.get("orderbook", {}).get("yes", [])
        no_bids = book.get("orderbook", {}).get("no", [])

        best_yes_bid = yes_bids[0][0] if yes_bids else None
        best_no_bid = no_bids[0][0] if no_bids else None

        if best_yes_bid is None or best_no_bid is None:
            logger.debug("Insufficient liquidity for %s, skipping.", ticker)
            return

        # Implied NO price from YES side
        implied_no_price = 100 - best_yes_bid
        edge = best_no_bid - implied_no_price

        logger.info(
            "Ticker: %s | YES bid: %d | NO bid: %d | Edge: %d",
            ticker, best_yes_bid, best_no_bid, edge,
        )

        if edge >= MIN_EDGE_CENTS:
            logger.info("Edge found on %s, placing NO buy order.", ticker)
            try:
                result = await self.client.place_order(
                    ticker=ticker,
                    side="no",
                    action="buy",
                    count=MAX_CONTRACTS,
                    yes_price=implied_no_price,
                )
                logger.info("Order placed: %s", result)
            except Exception as e:
                logger.error("Order failed for %s: %s", ticker, e)
        else:
            logger.debug("No edge on %s (edge=%d), skipping.", ticker, edge)
