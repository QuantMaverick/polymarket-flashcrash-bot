#!/usr/bin/env python3
"""
Real-time Polymarket CLOB WebSocket feed for BTC markets.

WORKING IMPLEMENTATION - tested and verified 2026-02-20.

Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
Protocol:
1. Connect via HTTP proxy (geo-blocked from Singapore)
2. Send init message: {"assets_ids": [], "type": "market"}
3. Subscribe: {"operation": "subscribe", "assets_ids": [...]}
4. Receive: book, price_change, last_trade_price events
"""

import json
import os
import time
import ssl
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List, Any
import websocket

# CLOB WebSocket endpoint
CLOB_WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Proxy settings (required for Singapore IPs - geo-blocked)
# Read from env var, fallback to hardcoded default
_PROXY_URL = os.getenv("POLYMARKET_PROXY", "http://ueylovyh:d9rv52w0lcqb@92.112.174.39:5623")
if _PROXY_URL:
    # Parse http://user:pass@host:port
    from urllib.parse import urlparse
    _parsed = urlparse(_PROXY_URL)
    PROXY_HOST = _parsed.hostname or ""
    PROXY_PORT = _parsed.port or 0
    PROXY_USER = _parsed.username or ""
    PROXY_PASS = _parsed.password or ""
else:
    # Fallback (should not be needed)
    PROXY_HOST = ""
    PROXY_PORT = 0
    PROXY_USER = ""
    PROXY_PASS = ""

# Connection settings
RECONNECT_INTERVAL_MS = 5000
PENDING_FLUSH_INTERVAL_MS = 100


@dataclass
class PolymarketBook:
    """Order book state for a token."""
    token_id: str
    market: str = ""
    bids: List[Dict] = field(default_factory=list)  # [{price, size}, ...]
    asks: List[Dict] = field(default_factory=list)
    last_update: float = 0
    last_trade_price: Optional[float] = None
    
    @property
    def best_bid(self) -> float:
        if not self.bids:
            return 0
        return max(float(b['price']) for b in self.bids)
    
    @property
    def best_ask(self) -> float:
        if not self.asks:
            return 1
        return min(float(a['price']) for a in self.asks)
    
    @property
    def mid_price(self) -> float:
        return (self.best_bid + self.best_ask) / 2
    
    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid
    
    @property
    def best_bid_size(self) -> float:
        if not self.bids:
            return 0
        best = self.best_bid
        return sum(float(b['size']) for b in self.bids if abs(float(b['price']) - best) < 0.001)
    
    @property
    def best_ask_size(self) -> float:
        if not self.asks:
            return 0
        best = self.best_ask
        return sum(float(a['size']) for a in self.asks if abs(float(a['price']) - best) < 0.001)


class PolymarketFeed:
    """
    Real-time Polymarket CLOB WebSocket feed.
    
    Protocol discovered from nevuamarkets/poly-websockets:
    1. Connect to wss://ws-subscriptions-clob.polymarket.com/ws/market
    2. Send init: {"assets_ids": [], "type": "market"}
    3. Subscribe: {"operation": "subscribe", "assets_ids": [...]}
    4. Unsubscribe: {"operation": "unsubscribe", "assets_ids": [...]}
    
    Events:
    - book: Full order book snapshot
    - price_change: Order book level updates
    - last_trade_price: Trade execution
    
    Usage:
        feed = PolymarketFeed(use_proxy=True)
        feed.subscribe(up_token_id, down_token_id)
        feed.start()
        
        book = feed.get_book(up_token_id)
        print(f"UP: {book.best_bid} / {book.best_ask}")
        
        feed.stop()
    """
    
    def __init__(
        self, 
        on_update: Optional[Callable] = None,
        on_book: Optional[Callable] = None,
        on_trade: Optional[Callable] = None,
        on_price_change: Optional[Callable] = None,
        use_proxy: bool = True
    ):
        self.books: Dict[str, PolymarketBook] = {}
        self.on_update = on_update
        self.on_book = on_book
        self.on_trade = on_trade
        self.on_price_change = on_price_change
        self.use_proxy = use_proxy
        
        # Subscription tracking
        self.subscribed_tokens: set = set()
        self.pending_subscribe: set = set()
        self.pending_unsubscribe: set = set()
        
        self._ws = None
        self._running = False
        self._thread = None
        self._connected = False
        self._connecting = False
        
        # Stats
        self.messages_received = 0
        self.book_updates = 0
        self.trade_updates = 0
        self.price_changes = 0
        self.last_message_time = 0
        self._reconnect_count = 0
    
    def subscribe(self, *token_ids: str):
        """Add tokens to subscribe to."""
        added_new = False
        for token_id in token_ids:
            if token_id and token_id not in self.subscribed_tokens:
                self.pending_unsubscribe.discard(token_id)
                if token_id not in self.subscribed_tokens:
                    self.pending_subscribe.add(token_id)
                    self.books[token_id] = PolymarketBook(token_id=token_id)
                    added_new = True
                    print(f"[PolymarketFeed] Added token to pending: {token_id[:20]}...")
        
        print(f"[PolymarketFeed] subscribe() called: added_new={added_new}, _connected={self._connected}")
        if added_new and self._connected and self._ws:
            # Flush subscription directly on existing connection — no disruptive reconnect
            self._flush_subscriptions(self._ws)
    
    def unsubscribe(self, *token_ids: str):
        """Remove tokens from subscription."""
        for token_id in token_ids:
            if token_id in self.pending_subscribe:
                self.pending_subscribe.discard(token_id)
                continue
            if token_id in self.subscribed_tokens:
                # Just mark for removal - reconnect will handle it
                self.subscribed_tokens.discard(token_id)
                if token_id in self.books:
                    del self.books[token_id]
    
    def start(self):
        """Start WebSocket connection."""
        if not self.pending_subscribe and not self.subscribed_tokens:
            print("[PolymarketFeed] No tokens to subscribe!")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_ws, daemon=True)
        self._thread.start()
        print(f"[PolymarketFeed] Started (proxy={self.use_proxy})")
    
    def stop(self):
        """Stop WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except:
                pass
        print("[PolymarketFeed] Stopped")
    
    def reconnect(self):
        """Force reconnection - use when changing markets."""
        print("[PolymarketFeed] Forcing reconnect...")
        # Move all subscribed tokens back to pending
        for token_id in self.subscribed_tokens:
            self.pending_subscribe.add(token_id)
        self.subscribed_tokens.clear()
        self.pending_unsubscribe.clear()
        
        # Reset connection state
        self._connected = False
        self._connecting = False
        
        # Close current WebSocket (will auto-reconnect via on_close)
        if self._ws:
            try:
                self._ws.close()
            except:
                pass
    
    def _run_ws(self):
        """WebSocket connection loop."""
        
        def on_open(ws):
            print(f"[PolymarketFeed] Connected to CLOB WebSocket")
            self._connected = True
            self._connecting = False
            
            # Send init message (required handshake)
            init_msg = {"assets_ids": [], "type": "market"}
            ws.send(json.dumps(init_msg))
            
            # Flush pending subscriptions
            time.sleep(0.1)
            self._flush_subscriptions(ws)
        
        def on_message(ws, message):
            if not self._running:
                return
            
            self.messages_received += 1
            self.last_message_time = time.time()
            
            # Handle pong
            if message.upper() == "PONG":
                return
            
            try:
                data = json.loads(message)
                events = data if isinstance(data, list) else [data]
                
                for event in events:
                    if not event:
                        continue
                    self._process_event(event)
                    
            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"[PolymarketFeed] Message error: {e}")
        
        def on_error(ws, error):
            print(f"[PolymarketFeed] Error: {error}")
            self._connected = False
            self._connecting = False
            self._clear_books()
            self._move_to_pending()
        
        def on_close(ws, code, reason):
            print(f"[PolymarketFeed] Closed (code={code})")
            self._connected = False
            self._connecting = False
            self._clear_books()
            self._move_to_pending()
            # Note: reconnection is handled by the outer while loop, not here
        
        # Use a while loop for reconnection instead of recursion to avoid stack overflow
        while self._running:
            if self._connecting:
                time.sleep(0.1)
                continue
            
            self._connecting = True
            
            self._ws = websocket.WebSocketApp(
                CLOB_WSS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            # SSL and proxy settings
            ssl_opts = {"cert_reqs": ssl.CERT_NONE}
            proxy_kwargs = {}
            
            if self.use_proxy:
                proxy_kwargs = {
                    "http_proxy_host": PROXY_HOST,
                    "http_proxy_port": PROXY_PORT,
                    "http_proxy_auth": (PROXY_USER, PROXY_PASS),
                    "proxy_type": "http"
                }
            
            self._ws.run_forever(sslopt=ssl_opts, **proxy_kwargs)
            
            # run_forever() returned, meaning connection closed
            self._connecting = False
            
            if self._running:
                self._reconnect_count += 1
                print(f"[PolymarketFeed] Reconnecting in {RECONNECT_INTERVAL_MS}ms (attempt #{self._reconnect_count})")
                time.sleep(RECONNECT_INTERVAL_MS / 1000)
    
    def _clear_books(self):
        """Clear all book data on disconnect to prevent stale reads."""
        for token_id, book in self.books.items():
            book.bids = []
            book.asks = []
            book.last_update = 0
    
    def _move_to_pending(self):
        """Move subscribed tokens back to pending on disconnect."""
        for token_id in self.subscribed_tokens:
            if token_id not in self.pending_unsubscribe:
                self.pending_subscribe.add(token_id)
        self.subscribed_tokens.clear()
        self.pending_unsubscribe.clear()
    
    def _flush_subscriptions(self, ws):
        """Flush pending subscriptions/unsubscriptions."""
        if not ws or not self._connected:
            return
        
        # Process unsubscriptions first
        if self.pending_unsubscribe:
            to_unsub = list(self.pending_unsubscribe)
            msg = {"operation": "unsubscribe", "assets_ids": to_unsub}
            try:
                ws.send(json.dumps(msg))
                for tid in to_unsub:
                    self.subscribed_tokens.discard(tid)
                    if tid in self.books:
                        del self.books[tid]
                self.pending_unsubscribe.clear()
                print(f"[PolymarketFeed] Unsubscribed from {len(to_unsub)} tokens")
            except Exception as e:
                print(f"[PolymarketFeed] Unsubscribe error: {e}")
        
        # Process subscriptions
        if self.pending_subscribe:
            to_sub = list(self.pending_subscribe)
            msg = {"operation": "subscribe", "assets_ids": to_sub}
            try:
                ws.send(json.dumps(msg))
                for tid in to_sub:
                    self.subscribed_tokens.add(tid)
                self.pending_subscribe.clear()
                print(f"[PolymarketFeed] Subscribed to {len(to_sub)} tokens")
            except Exception as e:
                print(f"[PolymarketFeed] Subscribe error: {e}")
    
    def _process_event(self, event: Dict[str, Any]):
        """Process a single WebSocket event."""
        event_type = event.get("event_type", "")
        
        if event_type == "book":
            self._process_book(event)
        elif event_type == "price_change":
            self._process_price_change(event)
        elif event_type == "last_trade_price":
            self._process_trade(event)
    
    def _process_book(self, event: Dict):
        """Process full book snapshot."""
        token_id = event.get("asset_id")
        if not token_id or token_id not in self.books:
            return
        
        book = self.books[token_id]
        book.market = event.get("market", "")
        book.bids = event.get("bids", [])
        book.asks = event.get("asks", [])
        book.last_update = time.time()
        
        self.book_updates += 1
        
        if self.on_book:
            self.on_book(book)
        if self.on_update:
            self.on_update("book", book)
    
    def _process_price_change(self, event: Dict):
        """Process order book level update."""
        changes = event.get("price_changes", [])
        timestamp = event.get("timestamp", "")
        
        for change in changes:
            token_id = change.get("asset_id")
            if not token_id or token_id not in self.books:
                continue
            
            book = self.books[token_id]
            price = change.get("price")
            size = change.get("size")
            side = change.get("side")
            
            if price is None or size is None or side is None:
                continue
            
            price = float(price)
            size = float(size)
            
            # Update the appropriate side
            if side == "BUY":
                self._update_level(book.bids, price, size)
            elif side == "SELL":
                self._update_level(book.asks, price, size)
            
            book.last_update = time.time()
        
        self.price_changes += 1
        
        if self.on_price_change:
            self.on_price_change(event)
        if self.on_update:
            self.on_update("price_change", event)
    
    def _update_level(self, levels: List[Dict], price: float, size: float):
        """Update a single price level in the order book."""
        # Remove existing entry at this price
        levels[:] = [l for l in levels if abs(float(l.get('price', 0)) - price) > 0.0001]
        
        # Add new entry if size > 0
        if size > 0:
            levels.append({"price": str(price), "size": str(size)})
    
    def _process_trade(self, event: Dict):
        """Process trade event."""
        token_id = event.get("asset_id")
        if not token_id or token_id not in self.books:
            return
        
        book = self.books[token_id]
        price = event.get("price")
        
        if price:
            book.last_trade_price = float(price)
        book.last_update = time.time()
        
        self.trade_updates += 1
        
        if self.on_trade:
            self.on_trade(event)
        if self.on_update:
            self.on_update("trade", event)
    
    def get_book(self, token_id: str) -> Optional[PolymarketBook]:
        """Get current order book for a token."""
        return self.books.get(token_id)
    
    def get_prices(self) -> Dict[str, Dict]:
        """Get current prices for all subscribed tokens."""
        prices = {}
        for token_id, book in self.books.items():
            prices[token_id] = {
                'bid': book.best_bid,
                'ask': book.best_ask,
                'mid': book.mid_price,
                'spread': book.spread,
                'bid_size': book.best_bid_size,
                'ask_size': book.best_ask_size,
                'last_trade': book.last_trade_price,
                'age_ms': (time.time() - book.last_update) * 1000 if book.last_update else float('inf'),
            }
        return prices
    
    def get_stats(self) -> Dict:
        """Get connection and message statistics."""
        return {
            'connected': self._connected,
            'subscribed_tokens': len(self.subscribed_tokens),
            'pending_subscribe': len(self.pending_subscribe),
            'messages_received': self.messages_received,
            'book_updates': self.book_updates,
            'trade_updates': self.trade_updates,
            'price_changes': self.price_changes,
            'reconnect_count': self._reconnect_count,
            'last_message_age_ms': (time.time() - self.last_message_time) * 1000 if self.last_message_time else float('inf'),
        }
    
    def is_healthy(self) -> bool:
        """Check if feed is connected and receiving data."""
        # If we've received data recently, feed is healthy (fixes race condition on startup)
        if self.last_message_time and (time.time() - self.last_message_time) < 30:
            return True
        # Fall back to connection state check
        return self._connected


# === REST fallback for initial snapshot ===
def fetch_book_rest(token_id: str, proxy: str = None) -> Optional[Dict]:
    """
    Fetch order book via REST API (fallback).
    """
    import urllib.request
    
    try:
        url = f"https://clob.polymarket.com/book?token_id={token_id}"
        
        if proxy:
            proxy_handler = urllib.request.ProxyHandler({
                'http': proxy,
                'https': proxy
            })
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            opener = urllib.request.build_opener(
                proxy_handler,
                urllib.request.HTTPSHandler(context=ctx)
            )
        else:
            opener = urllib.request.build_opener()
        
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with opener.open(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[PolymarketFeed] REST fetch error: {e}")
        return None


def get_proxy_url() -> str:
    """Get formatted proxy URL."""
    return f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"


# === Test ===
if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("Polymarket CLOB WebSocket Feed Test")
    print("=" * 60)
    
    # Get active market tokens
    test_tokens = sys.argv[1:] if len(sys.argv) > 1 else []
    
    if not test_tokens:
        print("\nUsage: python polymarket_feed.py <token_id1> [token_id2] ...")
        print("\nFinding active BTC 5-min market tokens...")
        
        import requests
        proxy = get_proxy_url()
        proxies = {'http': proxy, 'https': proxy}
        
        try:
            now = int(time.time())
            slot = (now // 300) * 300
            slug = f"btc-updown-5m-{slot}"
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            
            resp = requests.get(url, proxies=proxies, verify=False, timeout=10)
            events = resp.json()
            
            if events:
                market = events[0].get('markets', [{}])[0]
                tokens = market.get('clobTokenIds', '[]')
                if isinstance(tokens, str):
                    tokens = json.loads(tokens)
                if tokens:
                    test_tokens = tokens[:2]
                    print(f"Found tokens: {test_tokens}")
        except Exception as e:
            print(f"Error finding tokens: {e}")
    
    if not test_tokens:
        print("No tokens available. Exiting.")
        sys.exit(1)
    
    def on_update(event_type, data):
        if event_type == "book":
            print(f"[BOOK] {data.token_id[:20]}... bid={data.best_bid:.3f} ask={data.best_ask:.3f}")
        elif event_type == "trade":
            print(f"[TRADE] {data.get('asset_id', '')[:20]}... @ {data.get('price')}")
    
    feed = PolymarketFeed(on_update=on_update, use_proxy=True)
    
    for token in test_tokens:
        feed.subscribe(token)
    
    feed.start()
    
    print("\nRunning for 30 seconds (Ctrl+C to stop)...\n")
    
    try:
        for i in range(30):
            time.sleep(1)
            if i > 0 and i % 5 == 0:
                stats = feed.get_stats()
                print(f"[STATS] msgs={stats['messages_received']} books={stats['book_updates']} trades={stats['trade_updates']}")
    except KeyboardInterrupt:
        pass
    
    feed.stop()
    
    print("\n=== Final Stats ===")
    print(json.dumps(feed.get_stats(), indent=2))
