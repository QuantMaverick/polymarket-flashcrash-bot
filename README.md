# Polymarket Flash Crash Bot

Trades Polymarket **BTC 15-Minute Up/Down** binary markets by detecting and capitalising on artificial bid collapses caused by market-maker liquidity withdrawal.

## Strategy

Market makers on Polymarket withdraw their bids aggressively in the final minutes of a market, causing token prices to overshoot downward even when the outcome is clear. This bot:

1. Monitors real-time bid prices on both UP and DOWN tokens
2. Detects a **flash crash**: bid drops ≥$0.28 within 60 seconds, starting from ≥$0.42
3. Confirms with **BTC direction**: BTC must have moved ≥0.15% in the crashed token's direction
4. **Buys the crashed token** at the current ask (entry range: $0.15–$0.78)
5. Holds to resolution or exits via stop-loss (bid < 50% of entry)

### Why it works
MMs pull bids to avoid getting filled at stale prices — this causes the price to undershoot. If BTC has confirmed the direction, the token will resolve at $1.00, making the discounted entry very profitable.

## Markets Targeted

- **Slug pattern**: `btc-updown-15m-{boundary_timestamp}`
- **Duration**: 900 seconds (15 minutes) per market
- **Entry window**: T+180s to T+840s

## Setup

```bash
# Install dependencies (Python 3.10+)
pip install -r requirements.txt

# Set env vars
export TG_BOT_TOKEN="..."
export TG_CHAT_ID="..."
export POLYMARKET_PROXY="http://user:pass@host:port"  # if needed for geo-block

# Credentials (macOS Keychain)
security add-generic-password -a "sigmaedge" -s "private_key" -w "0xYOUR_KEY"
security add-generic-password -a "sigmaedge" -s "api_key" -w "KEY"
security add-generic-password -a "sigmaedge" -s "api_secret" -w "SECRET"
security add-generic-password -a "sigmaedge" -s "api_passphrase" -w "PASS"
```

## Run

```bash
# Dry-run (no real orders)
python flashcrash_bot.py

# Live trading
python flashcrash_bot.py --live
```

## Configuration

All parameters are in the `Config` dataclass at the top of `flashcrash_bot.py`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CRASH_DROP_ABS` | 0.28 | Bid must fall by ≥$0.28 within window |
| `CRASH_WINDOW_SEC` | 60.0 | Time window for crash detection |
| `CRASH_FROM_MIN_BID` | 0.42 | Crash must start from ≥$0.42 |
| `BTC_CONFIRM_PCT` | 0.15% | BTC move threshold for direction confirmation |
| `TRADE_SIZE_USDC` | $30 | Trade size in USDC |
| `STOP_BID_RATIO` | 0.50 | Stop-loss: exit if bid < entry × 0.50 |
| `ENTRY_WINDOW_START_SEC` | 180s | Earliest entry (T+3min) |
| `ENTRY_WINDOW_END_SEC` | 840s | Latest entry (T+14min) |

## VPS Deployment

```bash
# Copy to VPS
scp -i ~/.ssh/your_key.pem flashcrash_bot.py polymarket_feed.py requirements.txt ubuntu@VPS_IP:~/polymarket/flashcrash/

# Install dependencies on VPS
ssh -i ~/.ssh/your_key.pem ubuntu@VPS_IP "pip3 install -r ~/polymarket/flashcrash/requirements.txt"

# Run with nohup
ssh -i ~/.ssh/your_key.pem ubuntu@VPS_IP "cd ~/polymarket/flashcrash && nohup python3 -u flashcrash_bot.py --live >> flashcrash.out 2>&1 &"
```

## Architecture

```
flashcrash_bot.py          — main strategy logic
polymarket_feed.py         — Polymarket CLOB WebSocket feed (shared with momentum bot)
core/orders.py             — order placement (V2 CLOB, in parent dir)
core/credentials.py        — macOS Keychain credential loader
shared/claim.py            — winnings claim utility
shared/lockfile.py         — single-instance guard
```
