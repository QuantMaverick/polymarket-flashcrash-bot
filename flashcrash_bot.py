#!/usr/bin/env python3
"""
Flash Crash Bot for Polymarket BTC 15-Minute Binary Options

Strategy: When market-makers abruptly withdraw bids (bid drop ≥CRASH_DROP_ABS
within CRASH_WINDOW_SEC) and BTC direction confirms the crashing token's outcome,
buy that token at the artificially depressed ask price.

MMs yank bids near expiry → price overshoots downward → token resolves correctly.
We enter during the overshoot window and hold to resolution.

Entry: T+180s to T+840s  (skip first 3 min noise, exit 1 min before close)
Stop-loss: bid < entry × STOP_BID_RATIO
Market: btc-updown-15m-{boundary_ts}  (900s per market)
"""

import os
import sys

# Add parent directory for core/ and shared/ imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, "/home/ubuntu/polymarket/shared")

import asyncio
import json
import time
import csv
import argparse
import threading
import queue
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Deque
from collections import deque

import websockets
import ssl
import aiohttp
import httpx as _tg_httpx

# ═══════════════════════════════════════════════════════════════════════════════
# PROXY CONFIGURATION — before any py_clob_client imports
# ═══════════════════════════════════════════════════════════════════════════════
_PROXY_URL = os.getenv("POLYMARKET_PROXY", "")
if _PROXY_URL:
    try:
        import httpx
        CLOB_HTTP_CLIENT = httpx.Client(proxy=_PROXY_URL, verify=False, timeout=30.0)
        import py_clob_client_v2.http_helpers.helpers as _clob_helpers
        _clob_helpers._http_client = CLOB_HTTP_CLIENT
        print(f"[PROXY] Configured: {_PROXY_URL.split('@')[1] if '@' in _PROXY_URL else _PROXY_URL}")
    except Exception as e:
        print(f"[PROXY] Warning: {type(e).__name__}: {e!r}")

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════════
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "")
_TG_TIMEOUT  = _tg_httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
_tg_client_lock = threading.Lock()
_tg_client: Optional[_tg_httpx.Client] = None


def _get_tg_client() -> _tg_httpx.Client:
    global _tg_client
    if _tg_client is None or _tg_client.is_closed:
        with _tg_client_lock:
            if _tg_client is None or _tg_client.is_closed:
                _tg_client = _tg_httpx.Client(timeout=_TG_TIMEOUT)
    return _tg_client


def send_telegram_sync(message: str, bot_name: str = "FLASHCRASH") -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return False
    try:
        client = _get_tg_client()
        full_msg = f"[{bot_name}] {message}" if not message.startswith("[") else message
        resp = client.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": full_msg, "parse_mode": "HTML"},
        )
        return resp.status_code == 200
    except Exception:
        return False


def _send_telegram_async(message: str):
    def _send():
        for attempt in range(2):
            try:
                if send_telegram_sync(message, "FLASHCRASH"):
                    return
            except Exception:
                if attempt == 0:
                    time.sleep(1.0)

    threading.Thread(target=_send, daemon=True, name="tg-async").start()


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED MODULES
# ═══════════════════════════════════════════════════════════════════════════════
try:
    from lockfile import acquire_lock, release_lock
except ImportError:
    def acquire_lock(path): return True
    def release_lock(path): pass

try:
    from claim import ClaimManager, ClaimConfig, CLAIMING_AVAILABLE
except ImportError:
    CLAIMING_AVAILABLE = False
    ClaimManager = None
    ClaimConfig = None

try:
    from ctf_approvals import check_approvals_or_exit
except ImportError:
    def check_approvals_or_exit(*a, **kw): pass

# ═══════════════════════════════════════════════════════════════════════════════
# ORDER MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════
try:
    from core.orders import OrderManager, Side
    from core.credentials import load_credentials
    LIVE_TRADING_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Live trading unavailable: {type(e).__name__}: {e!r}")
    LIVE_TRADING_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════════════════════
# POLYMARKET FEED
# ═══════════════════════════════════════════════════════════════════════════════
sys.path.insert(0, str(Path(__file__).parent))
from polymarket_feed import PolymarketFeed

# Shared outcome utilities (optional — VPS only)
try:
    from outcome_utils import get_outcome_from_api
    OUTCOME_UTILS_AVAILABLE = True
except ImportError:
    OUTCOME_UTILS_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
SIGMAEDGE_FUNDER = "0x860e19aBF5a63D04E85F51D0887021f16Ab1bf25"

MARKET_INTERVAL_SEC = 900    # 15-minute markets
MARKET_SLUG_PREFIX  = "btc-updown-15m"


@dataclass
class Config:
    # Flash crash detection
    CRASH_DROP_ABS: float    = 0.28   # Bid must fall by ≥0.28 (e.g., 0.65→0.37)
    CRASH_WINDOW_SEC: float  = 60.0   # … within 60 seconds
    CRASH_FROM_MIN_BID: float = 0.42  # Crash must start from a bid ≥ 0.42 (already cheap = skip)

    # BTC direction confirmation
    BTC_CONFIRM_PCT: float   = 0.15   # BTC must have moved ≥0.15% in token's direction
    BTC_MAX_ADVERSE_PCT: float = 0.50 # Skip if BTC has moved >0.50% AGAINST the token direction

    # Entry constraints
    MIN_ENTRY_PRICE: float   = 0.15   # Don't buy below $0.15 (too speculative)
    MAX_ENTRY_PRICE: float   = 0.78   # Don't buy above $0.78 (too expensive post-crash)
    ENTRY_WINDOW_START_SEC: int = 180  # Don't enter before T+180s
    ENTRY_WINDOW_END_SEC: int   = 840  # Don't enter after T+840s (60s buffer before close)

    # Position sizing
    TRADE_SIZE_USDC: float   = 30.0

    # Stop-loss (based on bid value)
    STOP_BID_RATIO: float    = 0.50   # Exit if bid < entry × 0.50
    STOP_CHECK_INTERVAL_SEC: float = 5.0

    # Trade log
    TRADE_LOG_PATH: str      = "flashcrash_trades.csv"

    # Bot identity
    BOT_NAME: str            = "FLASHCRASH"
    LOCKFILE: str            = "/tmp/flashcrash_bot.lock"
    PROXY: str               = os.getenv("POLYMARKET_PROXY", "")


CFG = Config()


# ═══════════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class BotState:
    # Market
    market_slug: Optional[str]     = None
    market_id: Optional[str]       = None
    up_token: Optional[str]        = None
    down_token: Optional[str]      = None
    market_start_ts: Optional[float] = None
    market_end_ts: Optional[float]   = None
    condition_id: Optional[str]    = None

    # BTC
    binance_start_price: float     = 0.0
    binance_current_price: float   = 0.0

    # Polymarket order book
    up_bid:  float = 0.0
    up_ask:  float = 1.0
    down_bid: float = 0.0
    down_ask: float = 1.0

    # Rolling bid history for crash detection: {token_id: deque[(ts, bid)]}
    bid_history: Dict = field(default_factory=dict)

    # Position
    position_held: bool            = False
    trade_direction: Optional[str] = None
    trade_token_id: Optional[str]  = None
    trade_entry_price: float       = 0.0
    trade_shares: float            = 0.0
    trade_size_usdc: float         = 0.0
    trade_entry_ts: float          = 0.0
    stopped_out: bool              = False

    # Crash signal tracking (per direction)
    crash_fired: Dict[str, bool]   = field(default_factory=lambda: {"UP": False, "DOWN": False})

    # Session P&L
    session_pnl: float             = 0.0
    session_wins: int              = 0
    session_losses: int            = 0
    session_total: int             = 0


STATE = BotState()
ORDER_MGR: Optional["OrderManager"] = None


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {msg}", flush=True)


def elapsed() -> float:
    if STATE.market_start_ts:
        return time.time() - STATE.market_start_ts
    return 0.0


def format_market_time() -> str:
    if not STATE.market_start_ts:
        return "??:??-??:??"
    dt_start = datetime.fromtimestamp(STATE.market_start_ts, tz=timezone.utc)
    dt_end   = datetime.fromtimestamp(STATE.market_end_ts, tz=timezone.utc)
    return f"{dt_start.strftime('%H:%M')}-{dt_end.strftime('%H:%M')} UTC"


def btc_move_pct() -> float:
    if STATE.binance_start_price and STATE.binance_start_price > 0:
        return (STATE.binance_current_price - STATE.binance_start_price) / STATE.binance_start_price * 100
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# TRADE LOGGER
# ═══════════════════════════════════════════════════════════════════════════════
_trade_log_lock = threading.Lock()

def log_trade(direction: str, entry_price: float, shares: float, size_usdc: float,
              outcome: str, pnl: float, exit_price: float = 0.0, notes: str = ""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [ts, STATE.market_slug or "", direction, f"{entry_price:.3f}",
           f"{shares:.2f}", f"{size_usdc:.2f}",
           f"{STATE.binance_start_price:.2f}", f"{STATE.binance_current_price:.2f}",
           f"{btc_move_pct():+.3f}%",
           outcome, f"{exit_price:.3f}", f"{pnl:+.2f}", notes]
    with _trade_log_lock:
        path = Path(CFG.TRADE_LOG_PATH)
        write_header = not path.exists()
        with open(path, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(["timestamp", "market_slug", "direction", "entry_price",
                            "shares", "size_usdc", "btc_start", "btc_end", "btc_move",
                            "outcome", "exit_price", "pnl", "notes"])
            w.writerow(row)


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════
async def find_15m_market() -> Optional[Dict]:
    """Find the currently active BTC 15-min market."""
    now = time.time()
    current_boundary = (int(now) // MARKET_INTERVAL_SEC) * MARKET_INTERVAL_SEC
    next_boundary    = current_boundary + MARKET_INTERVAL_SEC

    for boundary_ts in [current_boundary, next_boundary]:
        slug = f"{MARKET_SLUG_PREFIX}-{int(boundary_ts)}"
        url  = f"https://gamma-api.polymarket.com/events?slug={slug}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        log(f"[MARKET-FIND] {slug} — HTTP {resp.status}")
                        continue
                    data = await resp.json()
                    if not data:
                        log(f"[MARKET-FIND] {slug} — empty response")
                        continue

                    event    = data[0]
                    end_ts   = boundary_ts + MARKET_INTERVAL_SEC
                    if end_ts <= now:
                        log(f"[MARKET-FIND] {slug} — already ended")
                        continue

                    markets  = event.get("markets", [])
                    up_token = down_token = condition_id = None
                    for m in markets:
                        token_ids = m.get("clobTokenIds", [])
                        outcomes  = m.get("outcomes", [])
                        if isinstance(token_ids, str):
                            token_ids = json.loads(token_ids)
                        if isinstance(outcomes, str):
                            outcomes  = json.loads(outcomes)
                        for i, tid in enumerate(token_ids):
                            outcome = outcomes[i].lower() if i < len(outcomes) else ""
                            if "up" in outcome:
                                up_token = tid
                            elif "down" in outcome:
                                down_token = tid
                        if not condition_id:
                            condition_id = m.get("conditionId")

                    if up_token and down_token:
                        log(f"[MARKET-FIND] Found: {slug}  UP={up_token[:12]}… DOWN={down_token[:12]}…")
                        return {
                            "slug":         slug,
                            "title":        event.get("title", ""),
                            "up_token":     up_token,
                            "down_token":   down_token,
                            "start_ts":     float(boundary_ts),
                            "end_ts":       float(end_ts),
                            "condition_id": condition_id,
                        }
                    else:
                        log(f"[MARKET-FIND] {slug} — tokens missing")
        except Exception as e:
            log(f"[MARKET-FIND] {slug} error: {type(e).__name__}: {e!r}")

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# BINANCE WEBSOCKET
# ═══════════════════════════════════════════════════════════════════════════════
async def binance_price_feed():
    """Stream BTC/USDT trades from Binance and update STATE.binance_current_price."""
    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    reconnect_delay = 2.0
    while True:
        try:
            ssl_ctx = ssl.create_default_context()
            async with websockets.connect(uri, ssl=ssl_ctx, ping_interval=20, ping_timeout=10) as ws:
                log("[BINANCE] Connected")
                reconnect_delay = 2.0
                async for raw in ws:
                    data = json.loads(raw)
                    price = float(data.get("p", 0))
                    if price > 0:
                        STATE.binance_current_price = price
        except Exception as e:
            log(f"[BINANCE] Disconnected: {type(e).__name__}: {e!r}. Reconnect in {reconnect_delay:.0f}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, 30.0)


# ═══════════════════════════════════════════════════════════════════════════════
# CRASH DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
def record_bid(token_id: str, bid: float):
    """Record bid price with timestamp for rolling crash detection."""
    if token_id not in STATE.bid_history:
        STATE.bid_history[token_id] = deque(maxlen=300)
    STATE.bid_history[token_id].append((time.time(), bid))


def detect_crash(token_id: str) -> Optional[float]:
    """
    Return the pre-crash bid if a crash is detected, else None.

    Crash = bid dropped ≥CRASH_DROP_ABS within CRASH_WINDOW_SEC seconds.
    Pre-crash bid must have been ≥ CRASH_FROM_MIN_BID.
    Current bid must be below (pre-crash − CRASH_DROP_ABS).
    """
    history = STATE.bid_history.get(token_id)
    if not history or len(history) < 3:
        return None

    now = time.time()
    current_bid = history[-1][1]

    # Find highest bid within the crash window
    cutoff = now - CFG.CRASH_WINDOW_SEC
    window_bids = [(ts, bid) for ts, bid in history if ts >= cutoff]
    if not window_bids:
        return None

    peak_bid = max(b for _, b in window_bids)
    drop     = peak_bid - current_bid

    if peak_bid >= CFG.CRASH_FROM_MIN_BID and drop >= CFG.CRASH_DROP_ABS:
        return peak_bid
    return None


def check_btc_confirms_direction(direction: str) -> bool:
    """True if BTC move is consistent with the given direction and within thresholds."""
    move = btc_move_pct()
    if direction == "UP":
        # BTC must be up ≥ confirm threshold, but not so far UP that the token is already expensive
        return move >= CFG.BTC_CONFIRM_PCT and move <= CFG.BTC_MAX_ADVERSE_PCT + CFG.BTC_CONFIRM_PCT
    else:  # DOWN
        return move <= -CFG.BTC_CONFIRM_PCT and move >= -(CFG.BTC_MAX_ADVERSE_PCT + CFG.BTC_CONFIRM_PCT)


# ═══════════════════════════════════════════════════════════════════════════════
# TRADE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════
async def execute_trade(direction: str, entry_price: float, crash_from: float, dry_run: bool = False):
    """Buy the crashed token at entry_price."""
    global ORDER_MGR

    token_id   = STATE.up_token if direction == "UP" else STATE.down_token
    size_usdc  = CFG.TRADE_SIZE_USDC
    shares     = round(size_usdc / entry_price, 2)
    move       = btc_move_pct()
    elapsed_s  = elapsed()

    log(f"[TRADE] Signal: {direction} @ ${entry_price:.3f} | crash from ${crash_from:.3f} | "
        f"BTC {move:+.3f}% | T+{elapsed_s:.0f}s | shares={shares:.2f}")

    if dry_run or not LIVE_TRADING_AVAILABLE or ORDER_MGR is None:
        log(f"[TRADE] DRY-RUN — would buy {direction} @ ${entry_price:.3f} | ${size_usdc:.2f} / {shares:.2f} shares")
        STATE.position_held   = True
        STATE.trade_direction = direction
        STATE.trade_token_id  = token_id
        STATE.trade_entry_price = entry_price
        STATE.trade_shares    = shares
        STATE.trade_size_usdc = size_usdc
        STATE.trade_entry_ts  = time.time()
        _send_telegram_async(
            f"@NutellaObsession\n"
            f"<b>{format_market_time()}</b>\n"
            f"<b>🟡 DRY-RUN SIGNAL</b>\n"
            f"BUY {direction} @ ${entry_price:.3f}\n"
            f"Crash: ${crash_from:.3f}→${entry_price:.3f} (−{crash_from-entry_price:.3f})\n"
            f"BTC: ${STATE.binance_current_price:,.0f} ({move:+.2f}%)\n"
            f"T+{elapsed_s:.0f}s"
        )
        return

    try:
        # Pre-approve CTF allowance
        try:
            from py_clob_client_v2 import BalanceAllowanceParams, AssetType
            ORDER_MGR._clob_client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
        except Exception as allowance_err:
            log(f"[TRADE] Allowance warning: {allowance_err}")

        from core.orders import OrderType as _OT
        result = ORDER_MGR.place_order(
            token_id=token_id,
            side=Side.BUY,
            price=entry_price,
            size=shares,
            order_type=_OT.GTC,
        )
    except Exception as e:
        log(f"[TRADE] Exception: {type(e).__name__}: {e!r}")
        _send_telegram_async(f"⚠️ Trade exception: {e}")
        return

    if not result.success:
        log(f"[TRADE] Failed: {result.error}")
        _send_telegram_async(f"⚠️ Order failed: {result.error}")
        return

    actual_price = result.average_price or entry_price
    log(f"[TRADE] Filled: {direction} @ ${actual_price:.3f} | {shares:.2f} shares | order={result.order_id[:12]}…")

    STATE.position_held    = True
    STATE.trade_direction  = direction
    STATE.trade_token_id   = token_id
    STATE.trade_entry_price = actual_price
    STATE.trade_shares     = result.filled_amount or shares
    STATE.trade_size_usdc  = size_usdc
    STATE.trade_entry_ts   = time.time()

    _send_telegram_async(
        f"@NutellaObsession\n"
        f"<b>{format_market_time()}</b>\n"
        f"<b>🟢 FLASH CRASH FILLED</b>\n"
        f"BUY {direction} @ ${actual_price:.3f}\n"
        f"Crash: ${crash_from:.3f}→${actual_price:.3f} (−{crash_from-actual_price:.3f})\n"
        f"Size: ${size_usdc:.2f} | {STATE.trade_shares:.2f} shares\n"
        f"BTC: ${STATE.binance_current_price:,.0f} ({move:+.2f}%)\n"
        f"T+{elapsed_s:.0f}s remaining: {STATE.market_end_ts - time.time():.0f}s"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# EXIT EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════
async def execute_stop_loss(reason: str, current_bid: float, dry_run: bool = False):
    """Exit position via IOC sell."""
    global ORDER_MGR

    token_id  = STATE.trade_token_id
    shares    = round(STATE.trade_shares * 0.99, 2)
    elapsed_s = elapsed()
    pnl_est   = (current_bid - STATE.trade_entry_price) * STATE.trade_shares

    log(f"[STOP] {reason} | bid=${current_bid:.3f} entry=${STATE.trade_entry_price:.3f} "
        f"pnl≈${pnl_est:+.2f} | T+{elapsed_s:.0f}s")

    STATE.stopped_out   = True
    STATE.position_held = False

    if dry_run or not LIVE_TRADING_AVAILABLE or ORDER_MGR is None:
        log(f"[STOP] DRY-RUN — would sell {shares:.2f} shares of {STATE.trade_direction}")
        log_trade(STATE.trade_direction, STATE.trade_entry_price, STATE.trade_shares,
                  STATE.trade_size_usdc, "STOPPED", pnl_est, current_bid, reason)
        STATE.session_pnl    += pnl_est
        STATE.session_total  += 1
        STATE.session_losses += 1
        _send_telegram_async(
            f"<b>{format_market_time()}</b>\n"
            f"🔴 <b>STOPPED</b> ({reason})\n"
            f"Entry: ${STATE.trade_entry_price:.3f} | Bid: ${current_bid:.3f}\n"
            f"Est. P&amp;L: ${pnl_est:+.2f} | T+{elapsed_s:.0f}s"
        )
        return

    try:
        from core.orders import OrderType as _OT
        result = ORDER_MGR.place_order(
            token_id=token_id,
            side=Side.SELL,
            price=max(0.01, current_bid - 0.03),
            size=shares,
            order_type=_OT.IOC,
        )
    except Exception as e:
        log(f"[STOP] Sell exception: {type(e).__name__}: {e!r}")
        result = None

    exit_price = result.average_price if (result and result.success) else current_bid
    actual_pnl = (exit_price - STATE.trade_entry_price) * STATE.trade_shares

    log(f"[STOP] Sold @ ${exit_price:.3f} | P&L: ${actual_pnl:+.2f}")
    log_trade(STATE.trade_direction, STATE.trade_entry_price, STATE.trade_shares,
              STATE.trade_size_usdc, "STOPPED", actual_pnl, exit_price, reason)
    STATE.session_pnl    += actual_pnl
    STATE.session_total  += 1
    STATE.session_losses += 1

    _send_telegram_async(
        f"<b>{format_market_time()}</b>\n"
        f"🔴 <b>STOPPED</b> ({reason})\n"
        f"Entry: ${STATE.trade_entry_price:.3f} → Exit: ${exit_price:.3f}\n"
        f"P&amp;L: ${actual_pnl:+.2f} | T+{elapsed_s:.0f}s"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET LOOP
# ═══════════════════════════════════════════════════════════════════════════════
async def run_market(market: Dict, dry_run: bool = False):
    """Run the flash crash strategy for one 15-minute market."""
    global ORDER_MGR

    # Initialize state for this market
    STATE.market_slug     = market["slug"]
    STATE.market_id       = market.get("condition_id", market["slug"])
    STATE.up_token        = market["up_token"]
    STATE.down_token      = market["down_token"]
    STATE.market_start_ts = market["start_ts"]
    STATE.market_end_ts   = market["end_ts"]
    STATE.condition_id    = market.get("condition_id")
    STATE.position_held   = False
    STATE.stopped_out     = False
    STATE.trade_direction = None
    STATE.bid_history     = {}
    STATE.crash_fired     = {"UP": False, "DOWN": False}
    STATE.binance_start_price = STATE.binance_current_price

    time_left = STATE.market_end_ts - time.time()
    log(f"[MARKET] Starting: {STATE.market_slug} | {time_left:.0f}s remaining")
    log(f"[MARKET]  UP   token: {STATE.up_token[:16]}…")
    log(f"[MARKET]  DOWN token: {STATE.down_token[:16]}…")

    # Setup OrderManager
    if LIVE_TRADING_AVAILABLE and not dry_run:
        if ORDER_MGR is None:
            try:
                creds = load_credentials("sigmaedge")
                globals()["ORDER_MGR"] = OrderManager(
                    creds.private_key, creds.api_key, creds.api_secret,
                    creds.api_passphrase, SIGMAEDGE_FUNDER
                )
                log("[ORDER] OrderManager initialized")
            except Exception as e:
                log(f"[ORDER] Init failed: {type(e).__name__}: {e!r}")

    # Startup Telegram
    _send_telegram_async(
        f"<b>{format_market_time()}</b>\n"
        f"<b>🔍 FLASHCRASH BOT ACTIVE</b>\n"
        f"Market: <code>{STATE.market_slug}</code>\n"
        f"<b>Crash threshold:</b> −${CFG.CRASH_DROP_ABS:.2f} in {CFG.CRASH_WINDOW_SEC:.0f}s "
        f"(from ≥${CFG.CRASH_FROM_MIN_BID:.2f})\n"
        f"<b>BTC confirm:</b> ≥{CFG.BTC_CONFIRM_PCT:.2f}%\n"
        f"<b>Entry window:</b> T+{CFG.ENTRY_WINDOW_START_SEC}s – T+{CFG.ENTRY_WINDOW_END_SEC}s\n"
        f"<b>Trade size:</b> ${CFG.TRADE_SIZE_USDC:.0f}\n"
        f"BTC: ${STATE.binance_current_price:,.0f}"
    )

    # Start Polymarket feed
    feed = PolymarketFeed(use_proxy=bool(CFG.PROXY))
    feed.subscribe(STATE.up_token, STATE.down_token)
    feed.start()

    # Spin until market ends
    try:
        while time.time() < STATE.market_end_ts:
            now     = time.time()
            elapsed_s = now - STATE.market_start_ts

            # Update book from feed
            up_book   = feed.get_book(STATE.up_token)
            down_book = feed.get_book(STATE.down_token)
            if up_book:
                STATE.up_bid = up_book.best_bid
                STATE.up_ask = up_book.best_ask
                record_bid(STATE.up_token, STATE.up_bid)
            if down_book:
                STATE.down_bid = down_book.best_bid
                STATE.down_ask = down_book.best_ask
                record_bid(STATE.down_token, STATE.down_bid)

            if not STATE.position_held and not STATE.stopped_out:
                # Enforce entry window
                if (elapsed_s >= CFG.ENTRY_WINDOW_START_SEC and
                        elapsed_s <= CFG.ENTRY_WINDOW_END_SEC):

                    for direction in ["UP", "DOWN"]:
                        if STATE.crash_fired[direction]:
                            continue
                        token_id = STATE.up_token if direction == "UP" else STATE.down_token
                        ask      = STATE.up_ask if direction == "UP" else STATE.down_ask

                        crash_from = detect_crash(token_id)
                        if crash_from is None:
                            continue

                        # Ask must be in buyable range
                        if not (CFG.MIN_ENTRY_PRICE <= ask <= CFG.MAX_ENTRY_PRICE):
                            log(f"[CRASH] {direction} crash detected but ask ${ask:.3f} out of range — skip")
                            continue

                        # BTC must confirm direction
                        if not check_btc_confirms_direction(direction):
                            move = btc_move_pct()
                            log(f"[CRASH] {direction} crash detected but BTC {move:+.3f}% doesn't confirm — skip")
                            continue

                        log(f"[CRASH] 🚨 {direction} CRASH: ${crash_from:.3f}→bid=${STATE.up_bid if direction=='UP' else STATE.down_bid:.3f} "
                            f"ask=${ask:.3f} | BTC {btc_move_pct():+.3f}%")
                        STATE.crash_fired[direction] = True
                        await execute_trade(direction, ask, crash_from, dry_run=dry_run)
                        break  # One trade per market

            elif STATE.position_held and not STATE.stopped_out:
                # Stop-loss check
                current_bid = (STATE.up_bid if STATE.trade_direction == "UP" else STATE.down_bid)
                stop_threshold = STATE.trade_entry_price * CFG.STOP_BID_RATIO
                if current_bid > 0 and current_bid < stop_threshold:
                    await execute_stop_loss(
                        f"bid ${current_bid:.3f} < ${stop_threshold:.3f} (50% entry)",
                        current_bid, dry_run=dry_run
                    )

            await asyncio.sleep(1.0)

    finally:
        feed.stop()

    # Post-market outcome
    await asyncio.sleep(5.0)  # Let market resolve
    if STATE.position_held and not STATE.stopped_out:
        outcome_str = "PENDING"
        outcome_pnl = 0.0

        if OUTCOME_UTILS_AVAILABLE and STATE.condition_id:
            try:
                outcome, is_resolved = await get_outcome_from_api(STATE.condition_id)
                if is_resolved and outcome:
                    won = (outcome.lower() == STATE.trade_direction.lower())
                    if won:
                        outcome_pnl = (1.0 - STATE.trade_entry_price) * STATE.trade_shares
                        outcome_str = "WIN"
                        STATE.session_wins += 1
                    else:
                        outcome_pnl = -STATE.trade_entry_price * STATE.trade_shares
                        outcome_str = "LOSS"
                        STATE.session_losses += 1
                    STATE.session_pnl   += outcome_pnl
                    STATE.session_total += 1
            except Exception as e:
                log(f"[OUTCOME] Error: {e}")

        log_trade(STATE.trade_direction, STATE.trade_entry_price, STATE.trade_shares,
                  STATE.trade_size_usdc, outcome_str, outcome_pnl,
                  notes=f"btc_move={btc_move_pct():+.3f}%")

        outcome_emoji = "🏆" if outcome_str == "WIN" else "❌" if outcome_str == "LOSS" else "⏳"
        _send_telegram_async(
            f"<b>{format_market_time()}</b>\n"
            f"{outcome_emoji} <b>{outcome_str}</b>\n"
            f"Direction: {STATE.trade_direction} @ ${STATE.trade_entry_price:.3f}\n"
            f"P&amp;L: ${outcome_pnl:+.2f}\n"
            f"Session: {STATE.session_wins}W / {STATE.session_losses}L | "
            f"P&amp;L: ${STATE.session_pnl:+.2f}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════
async def main_loop(dry_run: bool = False):
    """Outer loop: find market → run → repeat."""
    log(f"[MAIN] Flash Crash Bot starting | dry_run={dry_run}")

    # Start Binance price feed
    asyncio.ensure_future(binance_price_feed())

    # Wait for initial BTC price
    for _ in range(30):
        if STATE.binance_current_price > 0:
            break
        await asyncio.sleep(1.0)
    log(f"[MAIN] BTC price ready: ${STATE.binance_current_price:,.0f}")

    while True:
        market = None
        while market is None:
            market = await find_15m_market()
            if market is None:
                log("[MAIN] No active 15m market found — retry in 30s")
                await asyncio.sleep(30.0)

        # Wait until market start (in case we found next-boundary market early)
        wait_until = market["start_ts"]
        now        = time.time()
        if wait_until > now:
            wait_s = wait_until - now
            log(f"[MAIN] Next market starts in {wait_s:.0f}s — waiting")
            await asyncio.sleep(wait_s)

        await run_market(market, dry_run=dry_run)

        # Brief pause between markets
        await asyncio.sleep(5.0)


# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Polymarket Flash Crash Bot (15m BTC markets)")
    parser.add_argument("--live", action="store_true", help="Enable live trading (default: dry-run)")
    parser.add_argument("--account", default="sigmaedge", help="Keychain account name")
    args = parser.parse_args()

    dry_run = not args.live

    if dry_run:
        print("=" * 60)
        print("  DRY-RUN MODE — no real orders will be placed")
        print("  Use --live to enable live trading")
        print("=" * 60)
    else:
        print("=" * 60)
        print("  ⚠️  LIVE TRADING MODE")
        print("=" * 60)

    # Acquire lock
    if not acquire_lock(CFG.LOCKFILE):
        print("Another instance is running. Exiting.")
        sys.exit(1)

    try:
        asyncio.run(main_loop(dry_run=dry_run))
    finally:
        release_lock(CFG.LOCKFILE)
