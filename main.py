import asyncio
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from kalshi_client import KalshiClient
from hourly_strategy import HourlyBTCStrategy

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def run_hourly_bot():
    """Main loop: runs the hourly BTC trading strategy on Kalshi."""
    api_key = os.getenv("KALSHI_API_KEY")
    api_key_id = os.getenv("KALSHI_API_KEY_ID")
    base_url = os.getenv("KALSHI_BASE_URL", "https://trading-api.kalshi.com/trade-api/v2")

    if not api_key or not api_key_id:
        raise EnvironmentError("Missing KALSHI_API_KEY or KALSHI_API_KEY_ID in environment.")

    client = KalshiClient(api_key=api_key, api_key_id=api_key_id, base_url=base_url)
    strategy = HourlyBTCStrategy(client=client)

    logger.info("Kalshi Hourly BTC Trader Bot started at %s", datetime.utcnow().isoformat())

    while True:
        try:
            await strategy.run_cycle()
        except Exception as e:
            logger.error("Error during trading cycle: %s", e, exc_info=True)

        # Wait until the top of the next hour
        now = datetime.utcnow()
        seconds_until_next_hour = (60 - now.minute) * 60 - now.second
        logger.info("Sleeping %d seconds until next hourly cycle.", seconds_until_next_hour)
        await asyncio.sleep(seconds_until_next_hour)


if __name__ == "__main__":
    asyncio.run(run_hourly_bot())
