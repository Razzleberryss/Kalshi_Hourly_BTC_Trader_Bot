# Kalshi BTC Hourly Trader Bot

A production-ready, synchronous Python bot that trades Kalshi's **BTC HOURLY** prediction market series (`KXBTCH`). It uses RSA-PSS authentication, a configurable risk manager, fee-aware signal logic, and a persistent trade journal.

---

## Architecture

```
bot.py              Main entry point — startup, main loop, error handling
config.py           All tunable parameters (sourced from .env), + validate()
kalshi_client.py    Synchronous Kalshi REST API v2 client (RSA-PSS auth)
risk_manager.py     Risk controls, position tracking, trades_hourly.csv journal
strategy.py         Hourly signal logic, fee calculator, spread/edge checks
hourly_strategy.py  Legacy stub (kept for backward compatibility)
main.py             Legacy entry point (kept for backward compatibility)
```

---

## Setup

### 1. Python version

Use **Python 3.11+** to avoid LibreSSL/RSA-PSS compatibility issues present in older Python builds.

```bash
python3.11 --version
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate an RSA key pair

```bash
# Generate 2048-bit RSA private key
openssl genrsa -out private_key.pem 2048

# Extract the public key (upload this to Kalshi's API access page)
openssl rsa -in private_key.pem -pubout -out public_key.pem
```

Upload `public_key.pem` at: **https://kalshi.com/account/api-access**

### 4. Configure environment

```bash
cp .env.example .env
# Edit .env with your API key ID and private key path
```

Required values in `.env`:

| Variable | Description |
|---|---|
| `KALSHI_API_KEY_ID` | API key ID from Kalshi |
| `KALSHI_PRIVATE_KEY_PATH` | Absolute path to `private_key.pem` |

### 5. Verify the BTC Hourly series ticker

```bash
python3 -c "
import config
from kalshi_client import KalshiClient
c = KalshiClient(config.KALSHI_API_KEY_ID, config.KALSHI_PRIVATE_KEY_PATH, config.KALSHI_BASE_URL)
import json
print(json.dumps(c.get_markets('KXBTCH')[:2], indent=2))
"
```

Common tickers: `KXBTCH`, `KXBTC1H`. Update `BTC_SERIES_TICKER_HOURLY` in `.env` if needed.

---

## Running the Bot

### Dry-run mode (default, safe)

```bash
python3 bot.py
```

With `DRY_RUN=true` (the default), all orders are simulated and logged but **never sent** to Kalshi.

### Live trading mode

Only switch to live when you have:
- Verified the ticker and signal logic via dry-run
- Set reasonable risk limits in `.env`
- Funded your Kalshi account

```bash
# In .env:
DRY_RUN=false
KALSHI_ENV=prod
```

The bot will display a large warning banner and pause 5 seconds before starting.

---

## Risk Parameters

All limits are configurable in `.env`:

| Parameter | Default | Description |
|---|---|---|
| `STOP_LOSS_CENTS` | 35 | Exit if unrealised loss > 35¢ per position |
| `TAKE_PROFIT_CENTS` | 22 | Exit if unrealised gain > 22¢ per position |
| `MAX_HOLD_MINUTES` | 50 | Exit if held > 50 min without hitting stops |
| `MAX_DOLLARS_PER_TRADE` | 25 | Max $ risk per trade (also capped at 25% of balance) |
| `MAX_OPEN_POSITIONS` | 3 | Maximum simultaneous open positions |
| `MAX_TOTAL_EXPOSURE_CENTS` | 7500 | Max total exposure across all positions |
| `MAX_DAILY_LOSS_CENTS` | 5000 | Daily loss circuit breaker (halts new trades) |
| `MAX_DAILY_TRADES` | 20 | Max trades per UTC calendar day |

---

## Strategy Parameters

| Parameter | Default | Description |
|---|---|---|
| `MIN_CONFIDENCE` | 0.12 | Minimum signal confidence (12%) to enter |
| `MIN_EDGE_CENTS` | 3 | Minimum net edge after fees before entering |
| `MAX_SPREAD_CENTS` | 5 | Skip markets with bid-ask spread > 5¢ |
| `LOOKBACK_HOURS` | 3 | Lookback window for momentum signals |
| `TIME_TO_EXPIRY_MIN_MINUTES` | 5 | Do not enter new trades < 5 min before expiry |

---

## Trade Journal

Every trade is logged to `trades_hourly.csv` (path configurable via `TRADES_CSV_PATH`):

```
timestamp, market_id, side, quantity, entry_price, exit_price, pnl_cents, exit_reason
```

Exit reasons: `stop_loss`, `take_profit`, `max_hold_time`, `expiry`, `signal_reversal`, `market_gone`

---

## Logging

```bash
# Normal
LOG_LEVEL=INFO

# Verbose debug
VERBOSE_LOGGING=true
LOG_LEVEL=DEBUG
```

---

## Running Tests

```bash
# Syntax check all modules
python3 -m py_compile config.py kalshi_client.py risk_manager.py strategy.py bot.py

# Quick import smoke test (will warn about missing .env, that is expected)
python3 -c "import config; print('config ok')"
python3 -c "from risk_manager import RiskManager; print('risk_manager ok')"
python3 -c "from strategy import HourlyStrategy; print('strategy ok')"
```

---

## File Reference

| File | Purpose |
|---|---|
| `bot.py` | Main entry point |
| `config.py` | All parameters + `validate()` |
| `kalshi_client.py` | API client (RSA-PSS auth, all endpoints) |
| `risk_manager.py` | Risk limits + CSV trade journal |
| `strategy.py` | Hourly signal logic + cycle orchestration |
| `.env.example` | Template — copy to `.env` |
| `trades_hourly.csv` | Auto-created trade journal |

---

## Security Notes

- **Never commit `.env`** — it contains your API key path.
- Keep `private_key.pem` outside the repository directory.
- The bot validates config before making any API calls — misconfiguration exits cleanly.
