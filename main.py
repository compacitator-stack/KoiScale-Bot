#!/usr/bin/env python3
"""
KoiScale v1 — Brian Shannon VWAP Mean Reversion Strategy
Standalone | Alpaca Paper | Polygon Data | Telegram

Strategy: Buy pullbacks to intraday VWAP in stage-2 uptrending stocks.
          "Don't buy the dip. Buy strength after the dip." — Brian Shannon
          Sell in thirds at resistance levels. All-day window 9:35–15:30 ET.

Foil to GreenHebi Bot (Ross Cameron bull flag momentum).
Works best on choppy/range-bound days where GreenHebi sits idle.

Phase 1 — Infrastructure scaffold (complete)
Phase 2 — Daily MA scanner / stage-2 universe (complete)
Phase 3 — VWAP bounce detector (complete)
Phase 4 — Scaled exit manager / 1/3-out state machine (complete)
Phase 5 — Integration, paper trading, live deploy (complete)
"""

import os, sys, json, time, ssl, signal, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request, urllib.error, urllib.parse
try:
    import websocket as _websocket_lib   # pip install websocket-client
    _WS_AVAILABLE = True
except ImportError:
    _websocket_lib = None
    _WS_AVAILABLE = False
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    ET = timezone(timedelta(hours=-4))

# ── Config ────────────────────────────────────────────────────────────────────
POLYGON_KEY  = os.environ.get("POLYGON_API_KEY",    "")
ALPACA_KEY   = os.environ.get("ALPACA_API_KEY",     "")
ALPACA_SEC   = os.environ.get("ALPACA_SECRET_KEY",  "")
ALPACA_URL   = os.environ.get("ALPACA_BASE_URL",    "https://paper-api.alpaca.markets")
TG_TOKEN     = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT      = os.environ.get("TELEGRAM_CHAT_ID",   "").strip()
SHEETS_URL   = os.environ.get("SHEETS_WEBHOOK_URL", "")
DASH_TOKEN   = os.environ.get("DASHBOARD_TOKEN",    "")
DRY_RUN      = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
# Choppy mode: take profits faster (1/2 at T1 instead of 1/3), smaller positions
CHOPPY_MODE  = os.environ.get("CHOPPY_MODE", "").lower() in ("1", "true", "yes")

# ── Strategy parameters (Brian Shannon VWAP mean reversion) ───────────────────
RISK_PCT       = 0.005    # 0.5% equity at risk per trade
DAILY_TARGET   = 300.0    # daily profit goal
MAX_LOSS       = -300.0   # daily loss limit = daily goal (Shannon's symmetry rule)
MAX_TRADES     = 4        # Shannon trades more names per day than Ross
MIN_RR         = 1.5      # Shannon: mean-reversion has tighter stops, lower needed RR
MAX_ENTRY_SLIP = 0.10     # stop_limit ceiling: max cents above trigger to pay
MAX_SPREAD_PCT = 0.5      # mid/large-caps should have tight spreads

# ── Universe filter (stage-2 uptrending stocks) ───────────────────────────────
PRICE_LO         = 10.0       # $10 minimum (liquid names)
PRICE_HI         = 500.0      # $500 maximum
MIN_AVG_VOLUME   = 500_000    # need liquidity for 1/3-out exits
MAX_WATCHLIST    = 20         # top candidates by bounce-quality score

# ── MA alignment filter (Shannon stage-2) ────────────────────────────────────
# All four must hold:
#   SMA_20 > SMA_50 > SMA_200  (stage-2 uptrend structure)
#   SMA_200 flat or rising     (declining 200-day = stage 4, don't trust rallies)
#   SMA_5 flat or rising       ("if the five-day is declining I'm not going to buy")
MA_200_SLOPE_DAYS = 20          # check 200-day slope over last N days
MA_5_SLOPE_DAYS   = 5           # check 5-day slope over last N days
VWAP_PROXIMITY_PCT = 3.0        # within 3% of VWAP = actionable for watchlist

# ── VWAP bounce detection ─────────────────────────────────────────────────────
VWAP_TOUCH_TOLERANCE  = 0.002   # within 0.2% of VWAP = "touched"
VWAP_UNDERCUT_MAX     = 0.005   # max 0.5% below VWAP before it's a breakdown
BOUNCE_MAX_AGE_MIN    = 10      # bounce signal must be within last 10 minutes
MIN_BARS_AFTER_TOUCH  = 2       # need at least 2 bars of strength after the touch
CONFLUENCE_TOLERANCE  = 0.01    # MA within 1% of VWAP = confluence
LOOKBACK_BARS         = 30      # scan last 30 1-min bars for patterns

# ── Exit scaling ─────────────────────────────────────────────────────────────
TRANCHE_1_PCT    = 0.33    # sell 1/3 at Target 1
TRANCHE_2_PCT    = 0.33    # sell 1/3 at Target 2
TRANCHE_3_PCT    = 0.34    # trail final 1/3
CHOPPY_T1_PCT    = 0.50    # sell 1/2 at Target 1 in choppy mode
CHOPPY_SIZE_MULT = 0.50    # half position size in choppy mode

# ── Trading window ────────────────────────────────────────────────────────────
# VWAP mean reversion works all day, not just the morning window.
CORE_START       = (9,  35)    # 9:35 ET
CORE_END         = (15, 30)    # 3:30 ET (30 min before close)
EOD_LIQUIDATE    = (15, 55)    # forced close if any positions still open
SCAN_INTERVAL_MINS = 15        # re-evaluate watchlist every 15 min

LOG_FILE     = "koiscale.log"
STATUS_FILE  = "koiscale_status.json"
TRADES_FILE  = "koiscale_trades.json"
PID_FILE     = "koiscale.pid"

_ssl = ssl.create_default_context()
TG   = f"https://api.telegram.org/bot{TG_TOKEN}"

# ── Logging ───────────────────────────────────────────────────────────────────
def now_et():
    return datetime.now(ET)

def log(lvl, msg):
    line = f"{now_et().strftime('%Y-%m-%d %H:%M:%S ET')} | {lvl:<5} | {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ── HTTP ──────────────────────────────────────────────────────────────────────
def http(method, url, data=None, headers=None, timeout=15):
    try:
        hdrs = dict(headers) if headers else {}
        body = None
        if data is not None:
            body = json.dumps(data).encode()
            hdrs.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw.strip() else {}
    except Exception as e:
        if "/getUpdates" not in url:
            log("ERROR", f"{method} {url[:80]} → {e}")
        return None

def GET(url, h=None):       return http("GET",    url, headers=h)
def POST(url, d, h=None):   return http("POST",   url, d, h)
def PATCH(url, d, h=None):  return http("PATCH",  url, d, h)
def DELETE(url, h=None):    return http("DELETE", url, headers=h)

# ── Telegram ──────────────────────────────────────────────────────────────────
def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        return
    r = POST(f"{TG}/sendMessage",
             {"chat_id": TG_CHAT, "text": text[:4000], "parse_mode": "Markdown"})
    if not r:
        POST(f"{TG}/sendMessage", {"chat_id": TG_CHAT, "text": text[:4000]})

def tg_poll(offset=0, timeout=25):
    if not TG_TOKEN:
        time.sleep(timeout)
        return []
    url = f"{TG}/getUpdates?offset={offset}&timeout={timeout}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout + 10, context=_ssl) as r:
            return json.loads(r.read().decode()).get("result", [])
    except Exception:
        return []

# ── Google Sheets webhook ─────────────────────────────────────────────────────
def sheets_push(payload):
    """POST trade/eod/scan data to Google Apps Script webhook. Never crashes bot."""
    if not SHEETS_URL:
        return
    try:
        payload["bot_version"] = "KoiScale v1"
        payload["timestamp"]   = now_et().isoformat()
        body = json.dumps(payload, default=str).encode()
        req  = urllib.request.Request(
            SHEETS_URL, data=body,
            headers={"Content-Type": "application/json"},
            method="POST")
        urllib.request.urlopen(req, timeout=10, context=_ssl)
        log("DEBUG", "Sheets push OK: " + payload.get("type", "?"))
    except Exception as e:
        log("DEBUG", "Sheets push failed (non-fatal): " + str(e))

# ── Alpaca ────────────────────────────────────────────────────────────────────
def alp_h():
    return {"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SEC}

def alp_get(p):     return GET(f"{ALPACA_URL}{p}", h=alp_h())
def alp_post(p, d): return POST(f"{ALPACA_URL}{p}", d, h=alp_h())
def alp_patch(p, d):return PATCH(f"{ALPACA_URL}{p}", d, h=alp_h())
def alp_del(p):     return DELETE(f"{ALPACA_URL}{p}", h=alp_h())

def get_account():   return alp_get("/v2/account") or {}
def get_equity():    return float(get_account().get("equity", 0))
def get_positions(): return alp_get("/v2/positions") or []
def get_orders():    return alp_get("/v2/orders?status=open&limit=50") or []

def get_quote(sym):
    """Fetch latest bid/ask from Alpaca IEX. Returns (bid, ask) or (None, None)."""
    url = f"https://data.alpaca.markets/v2/stocks/{sym}/quotes/latest?feed=iex"
    resp = GET(url, h=alp_h())
    if not resp or "quote" not in resp:
        return None, None
    q   = resp["quote"]
    bid = float(q.get("bp", 0) or 0)
    ask = float(q.get("ap", 0) or 0)
    if bid <= 0 or ask <= 0:
        return None, None
    return bid, ask

def get_live_pnl():
    """Real-time intraday P&L from Alpaca. Returns (pnl, equity) or (None, None)."""
    try:
        acc     = get_account()
        equity  = float(acc.get("equity", 0))
        last_eq = float(acc.get("last_equity", equity))
        return round(equity - last_eq, 2), equity
    except Exception as e:
        log("DEBUG", f"Live P&L fetch failed: {e}")
        return None, None

# ── Polygon bar fetching (REST fallback for micro-cap/OTC) ─────────────────────

def _normalize_polygon_agg(r):
    """Convert Polygon REST /v2/aggs result dict to Alpaca bar format."""
    ts = datetime.fromtimestamp(
        r["t"] / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "t":  ts,
        "o":  float(r.get("o",  0)),
        "h":  float(r.get("h",  0)),
        "l":  float(r.get("l",  0)),
        "c":  float(r.get("c",  0)),
        "v":  int(  r.get("v",  0)),
        "vw": float(r.get("vw", 0)),
        "n":  int(  r.get("n",  0)),
    }

# ── Alpaca WebSocket — real-time 1-min bar streaming ──────────────────────────
# Alpaca IEX WebSocket pushes completed 1-min bars the instant they close,
# cutting bar-data latency from REST poll interval (~8s + up to 60s candle wait)
# to ~1s after bar close. Free on both paper and live accounts.
# Falls back to REST polling if WS is unavailable.

_bar_cache     = {}              # sym -> [bar_dict, ...]  (Alpaca format)
_bar_lock      = threading.Lock()
_ws_subscribed = set()           # symbols currently subscribed on WS
_ws_auth_ok    = False           # True once Alpaca WS auth handshake completes
_alpaca_ws     = None            # active WebSocketApp instance
_ws_backoff    = 5               # current reconnect delay (exponential backoff)


def _normalize_polygon_agg(r):
    """Convert Polygon REST /v2/aggs result dict to Alpaca bar format.
    Used only in the REST fallback path for micro-cap/OTC symbols."""
    ts = datetime.fromtimestamp(
        r["t"] / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "t":  ts,
        "o":  float(r.get("o",  0)),
        "h":  float(r.get("h",  0)),
        "l":  float(r.get("l",  0)),
        "c":  float(r.get("c",  0)),
        "v":  int(  r.get("v",  0)),
        "vw": float(r.get("vw", 0)),
        "n":  int(  r.get("n",  0)),
    }


def _on_ws_open(ws):
    log("INFO", "Alpaca WS: connected — authenticating")
    ws.send(json.dumps({
        "action": "auth",
        "key":    ALPACA_KEY,
        "secret": ALPACA_SEC,
    }))


def _on_ws_message(ws, message):
    global _ws_auth_ok, _ws_backoff
    try:
        events = json.loads(message)
    except Exception:
        return
    if not isinstance(events, list):
        events = [events]
    for e in events:
        msg_type = e.get("T", "")

        # ── Connection established ──
        if msg_type == "success" and e.get("msg") == "connected":
            log("DEBUG", "Alpaca WS: connection acknowledged")

        # ── Authenticated ──
        elif msg_type == "success" and e.get("msg") == "authenticated":
            _ws_auth_ok = True
            _ws_backoff = 5   # reset backoff on success
            log("INFO", "Alpaca WS: authenticated")
            # Re-subscribe symbols from prior session (handles reconnects)
            with _bar_lock:
                syms = list(_ws_subscribed)
            if syms:
                ws.send(json.dumps({"action": "subscribe", "bars": syms}))
                log("INFO", f"Alpaca WS: re-subscribed {len(syms)} symbol(s)")

        # ── Auth or other error ──
        elif msg_type == "error":
            code = e.get("code", "")
            err_msg = e.get("msg", "")
            log("ERROR", f"Alpaca WS error: [{code}] {err_msg}")

        # ── Subscription confirmation ──
        elif msg_type == "subscription":
            bars_list = e.get("bars", [])
            if bars_list:
                log("DEBUG", f"Alpaca WS: subscribed to bars for "
                             f"{len(bars_list)} symbol(s)")

        # ── Completed 1-min bar ──
        elif msg_type == "b":
            sym = e.get("S", "")
            if not sym:
                continue
            bar = {
                "t":  e.get("t", ""),
                "o":  float(e.get("o", 0)),
                "h":  float(e.get("h", 0)),
                "l":  float(e.get("l", 0)),
                "c":  float(e.get("c", 0)),
                "v":  int(e.get("v", 0)),
                "vw": float(e.get("vw", 0)),
                "n":  int(e.get("n", 0)),
            }
            with _bar_lock:
                first_bar = sym not in _bar_cache
                if sym not in _bar_cache:
                    _bar_cache[sym] = []
                _bar_cache[sym].append(bar)
                if len(_bar_cache[sym]) > 420:   # keep ~7 hours (full session for VWAP)
                    _bar_cache[sym] = _bar_cache[sym][-420:]
            if first_bar:
                log("DEBUG", f"Alpaca WS: first live bar for {sym} "
                             f"c=${bar['c']} v={bar['v']}")


def _on_ws_error(ws, error):
    log("WARN", f"Alpaca WS error: {error}")


def _on_ws_close(ws, code, msg):
    global _ws_auth_ok
    _ws_auth_ok = False
    log("INFO", f"Alpaca WS closed (code={code}) — will reconnect")


def start_alpaca_ws():
    """
    Open Alpaca data WebSocket in a daemon thread. Receives completed 1-min
    bars in real-time (IEX feed, free). Auto-reconnects with exponential
    backoff on disconnect.
    """
    global _alpaca_ws, _ws_backoff
    if not _WS_AVAILABLE:
        log("WARN", "websocket-client not installed — WS streaming disabled.")
        return
    while True:
        try:
            _alpaca_ws = _websocket_lib.WebSocketApp(
                "wss://stream.data.alpaca.markets/v2/iex",
                on_open    = _on_ws_open,
                on_message = _on_ws_message,
                on_error   = _on_ws_error,
                on_close   = _on_ws_close,
            )
            _alpaca_ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            log("WARN", f"Alpaca WS thread error: {e}")
        time.sleep(_ws_backoff)
        _ws_backoff = min(_ws_backoff * 2, 60)


def ws_subscribe(symbols):
    """
    Subscribe to real-time 1-min bar events for a list of symbols via Alpaca WS,
    and seed the cache from REST so detect_vwap_bounce() works immediately.
    Idempotent — safe to call every cycle, only acts on new symbols.
    """
    global _alpaca_ws
    new_syms = [s for s in symbols if s not in _ws_subscribed]
    if not new_syms:
        return

    # Seed bar cache from REST (WS only streams bars going forward).
    # Primary: Polygon REST (consolidated tape, full coverage for mid/large-caps).
    # Fallback: Alpaca IEX REST (sparse for KoiScale's universe).
    for sym in new_syms:
        today = now_et().date().isoformat()
        url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/"
               f"{today}/{today}?adjusted=false&sort=asc&limit=420"
               f"&apiKey={POLYGON_KEY}")
        resp = GET(url)
        if resp and resp.get("results"):
            bars = [_normalize_polygon_agg(r) for r in resp["results"][-420:]]
            with _bar_lock:
                _bar_cache[sym] = bars
            log("DEBUG", f"WS seed: {len(bars)} bars for {sym} (Polygon REST)")
        else:
            url2 = (f"https://data.alpaca.markets/v2/stocks/{sym}/bars"
                    f"?timeframe=1Min&limit=60&feed=iex&adjustment=raw")
            resp2 = GET(url2, h=alp_h())
            if resp2 and resp2.get("bars"):
                with _bar_lock:
                    _bar_cache[sym] = resp2["bars"][-60:]
                log("DEBUG", f"WS seed: {len(resp2['bars'])} bars for {sym} (IEX)")
            else:
                log("WARN", f"WS seed: no bars for {sym}")

    # Subscribe on the live WS connection (if auth is complete)
    if _alpaca_ws and _ws_auth_ok:
        try:
            _alpaca_ws.send(json.dumps({"action": "subscribe", "bars": new_syms}))
            log("INFO", f"Alpaca WS: subscribed bars for {new_syms}")
        except Exception as e:
            log("WARN", f"Alpaca WS subscribe error: {e}")

    _ws_subscribed.update(new_syms)


def get_1min_bars(sym, limit=60):
    """
    Fetch 1-min bars for KoiScale's mid/large-cap universe.
    Primary: Polygon REST (Stock Starter plan — full consolidated data).
    Fallback: Alpaca IEX REST (sparse coverage for mid/large-caps).
    WS cache checked first if ever re-enabled.
    KoiScale uses up to 420 bars (full session) for VWAP calculation.
    """
    with _bar_lock:
        cached = list(_bar_cache.get(sym, []))
    if cached:
        return cached[-limit:]
    # Primary: Polygon REST — consolidated tape, full bar coverage
    today = now_et().date().isoformat()
    url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/"
           f"{today}/{today}?adjusted=false&sort=asc&limit={limit}"
           f"&apiKey={POLYGON_KEY}")
    resp = GET(url)
    if resp and resp.get("results"):
        return [_normalize_polygon_agg(r) for r in resp["results"][-limit:]]
    # Fallback: Alpaca IEX REST
    url2 = (f"https://data.alpaca.markets/v2/stocks/{sym}/bars"
            f"?timeframe=1Min&limit={limit}&feed=iex&adjustment=raw")
    resp2 = GET(url2, h=alp_h())
    if resp2 and resp2.get("bars"):
        return resp2["bars"]
    return []


# ── Daily bar fetching (for MA computation) ───────────────────────────────────
# Cache: computed once per day per symbol. Key: sym. Value: dict of MA values.
_daily_ma_cache  = {}   # sym -> {"sma_5": float, "sma_20": float, "sma_50": float,
                         #         "sma_200": float, "sma_200_slope": float,
                         #         "sma_5_slope": float, "cached_date": str}
_daily_ma_lock   = threading.Lock()


def fetch_daily_bars(sym, n_bars=220):
    """
    Fetch last n_bars daily bars from Polygon REST for sym.
    Returns list of bar dicts in Alpaca format, or [] on failure.
    Used to compute SMAs for stage-2 filter.
    """
    from datetime import timedelta as _td
    today    = now_et().date()
    from_dt  = (today - _td(days=int(n_bars * 1.5))).isoformat()   # ~1.5× to cover weekends
    to_dt    = (today - _td(days=1)).isoformat()                    # yesterday (today incomplete)
    url = (f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/day/"
           f"{from_dt}/{to_dt}?adjusted=true&sort=asc&limit={n_bars}"
           f"&apiKey={POLYGON_KEY}")
    resp = GET(url)
    if not resp or not resp.get("results"):
        return []
    return [_normalize_polygon_agg(r) for r in resp["results"][-n_bars:]]


def compute_mas(daily_bars):
    """
    Compute SMAs from a list of daily bars (Alpaca format).
    Returns dict: {"sma_5", "sma_20", "sma_50", "sma_200",
                   "sma_200_slope", "sma_5_slope", "last_close"}
    or None if not enough bars.

    Shannon stage-2 filter requires all of:
      sma_20 > sma_50 > sma_200
      sma_200_slope >= 0 (flat or rising)
      sma_5_slope   >= 0 (flat or rising)
    """
    closes = [float(b["c"]) for b in daily_bars]
    if len(closes) < 200:
        return None

    def sma(n):
        return sum(closes[-n:]) / n

    sma_5   = sma(5)
    sma_20  = sma(20)
    sma_50  = sma(50)
    sma_200 = sma(200)

    # Slope: compare current SMA to its value N days ago
    # Positive slope = rising, negative = declining
    sma_200_old  = sum(closes[-(200 + MA_200_SLOPE_DAYS):-MA_200_SLOPE_DAYS]) / 200
    sma_200_slope = sma_200 - sma_200_old   # > 0 = rising

    sma_5_old    = sum(closes[-(5 + MA_5_SLOPE_DAYS):-MA_5_SLOPE_DAYS]) / 5
    sma_5_slope  = sma_5 - sma_5_old

    return {
        "sma_5":         round(sma_5,   4),
        "sma_20":        round(sma_20,  4),
        "sma_50":        round(sma_50,  4),
        "sma_200":       round(sma_200, 4),
        "sma_200_slope": round(sma_200_slope, 4),
        "sma_5_slope":   round(sma_5_slope,   4),
        "last_close":    closes[-1],
    }


def is_stage2(mas):
    """
    Check Shannon's stage-2 criteria against a compute_mas() result dict.
    Returns (bool, reason_str).
    """
    if not mas:
        return False, "insufficient bar history"
    if not (mas["sma_20"] > mas["sma_50"] > mas["sma_200"]):
        return False, f"MA order wrong: 20={mas['sma_20']:.2f} 50={mas['sma_50']:.2f} 200={mas['sma_200']:.2f}"
    if mas["sma_200_slope"] < 0:
        return False, f"200-SMA declining (slope={mas['sma_200_slope']:.4f})"
    if mas["sma_5_slope"] < 0:
        return False, f"5-SMA declining (slope={mas['sma_5_slope']:.4f})"
    return True, "stage-2 ✅"


# ── VWAP calculation ──────────────────────────────────────────────────────────
def calc_vwap(bars):
    """
    Compute intraday VWAP from a list of 1-min bars (Alpaca format).
    Uses (high + low + close) / 3 as typical price per bar.
    Returns float VWAP or None if no bars.

    Shannon: VWAP is the anchor. Buy strength after price dips to VWAP.
    """
    total_tpv = 0.0   # sum of (typical_price * volume)
    total_vol = 0
    for b in bars:
        h, l, c = float(b["h"]), float(b["l"]), float(b["c"])
        v       = int(b["v"])
        tp      = (h + l + c) / 3.0
        total_tpv += tp * v
        total_vol += v
    if total_vol == 0:
        return None
    return round(total_tpv / total_vol, 4)


# ── Stage-2 universe scanner (Phase 2) ────────────────────────────────────────
def scan_stage2_universe():
    """
    Scan Polygon snapshot for all tickers, filter to stage-2 uptrending stocks
    near their intraday VWAP.

    Algorithm:
    1. GET Polygon /v2/snapshot for all US equity tickers
    2. Filter: price $10-$500, prevDay volume > 500K, plain alphabetic symbol
    3. Parallel daily bar fetch → compute_mas() → is_stage2() (cached per day)
    4. For stage-2 survivors: get_1min_bars() → calc_vwap() → bounce-quality score
    5. Score by: approach from above, VWAP proximity, support holding, volume decline, momentum
    6. Rank by bounce quality (highest first), return top MAX_WATCHLIST candidates

    MA results are cached in _daily_ma_cache (keyed by symbol, per day) so
    subsequent scan cycles skip the daily bar fetch for already-checked stocks.
    """
    today_str = now_et().date().isoformat()

    # ── Step 1: Polygon snapshot — all US equity tickers ─────────────────────
    log("INFO", "scan_stage2_universe: fetching Polygon snapshot...")
    url = (f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
           f"?include_otc=false&apiKey={POLYGON_KEY}")
    resp = GET(url)
    if not resp or not resp.get("tickers"):
        log("WARN", "scan_stage2_universe: snapshot returned nothing — check POLYGON_API_KEY")
        return []

    all_tickers = resp["tickers"]
    log("INFO", f"scan_stage2_universe: snapshot has {len(all_tickers)} tickers")

    # ── Step 2: Price + volume pre-filter ────────────────────────────────────
    pre = []
    for snap in all_tickers:
        sym = snap.get("ticker", "")
        # Skip non-plain-equity symbols (options, warrants have digits or dots)
        if not sym or not sym.isalpha() or len(sym) > 5:
            continue
        day  = snap.get("day",     {}) or {}
        prev = snap.get("prevDay", {}) or {}
        price    = float(day.get("c")  or prev.get("c")  or 0)
        prev_vol = int(  prev.get("v") or 0)
        day_vol  = int(  day.get("v")  or 0)
        vol = max(prev_vol, day_vol)
        if PRICE_LO <= price <= PRICE_HI and vol >= MIN_AVG_VOLUME:
            pre.append({"symbol": sym, "price": price, "volume": vol})

    log("INFO", f"scan_stage2_universe: {len(pre)} pass price+volume filter "
                f"(${PRICE_LO:.0f}–${PRICE_HI:.0f}, vol≥{MIN_AVG_VOLUME:,})")

    if not pre:
        return []

    # ── Step 3: Stage-2 MA check (parallel, cached per day) ──────────────────
    def _check_stage2(cand):
        sym = cand["symbol"]
        # Check cache first
        with _daily_ma_lock:
            cached = _daily_ma_cache.get(sym)
        if cached and cached.get("cached_date") == today_str:
            mas = cached
        else:
            bars = fetch_daily_bars(sym, n_bars=220)
            mas  = compute_mas(bars)
            if mas:
                mas["cached_date"] = today_str
                with _daily_ma_lock:
                    _daily_ma_cache[sym] = mas
        ok, _ = is_stage2(mas)
        if not ok:
            return None
        return {**cand, "mas": mas}

    stage2 = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = {pool.submit(_check_stage2, c): c for c in pre}
        for fut in as_completed(futs):
            try:
                result = fut.result()
                if result:
                    stage2.append(result)
            except Exception as e:
                log("DEBUG", f"stage2 check error: {e}")

    log("INFO", f"scan_stage2_universe: {len(stage2)} stage-2 stocks "
                f"(of {len(pre)} candidates)")

    if not stage2:
        return []

    # ── Step 4: VWAP bounce-quality scoring (parallel) ─────────────────────────
    def _vwap_score(cand):
        sym   = cand["symbol"]
        price = cand["price"]
        bars  = get_1min_bars(sym, limit=420)
        if not bars:
            return None
        vwap = calc_vwap(bars)
        if not vwap:
            return None
        dist_pct = (price - vwap) / vwap * 100   # positive = above VWAP
        abs_dist = abs(dist_pct)
        # Gate: must be within actionable range of VWAP
        if abs_dist > VWAP_PROXIMITY_PCT * 2:
            return None

        # ── Bounce-quality score (0–100) ─────────────────────────────────
        # Shannon wants: stock that was ABOVE VWAP, pulled back TO it,
        # and is now showing strength.  Flat-at-VWAP-all-day = boring.
        recent = bars[-min(30, len(bars)):]
        score  = 0.0

        # 1. APPROACH FROM ABOVE (0–30 pts)
        #    Was price meaningfully above VWAP earlier in recent bars?
        #    Bigger intraday range above VWAP = better setup quality.
        highs_pct = [(float(b["h"]) - vwap) / vwap * 100 for b in recent]
        max_above = max(highs_pct) if highs_pct else 0
        if max_above > 0.5:
            score += min(max_above / 3.0, 1.0) * 30   # caps at 3% above

        # 2. CURRENT PROXIMITY (0–20 pts)
        #    Slightly above VWAP (bounce underway) > right at VWAP > below
        if 0.05 <= dist_pct <= 1.0:
            score += 20     # ideal: just lifted off VWAP
        elif -0.2 <= dist_pct < 0.05:
            score += 15     # touching zone
        elif 1.0 < dist_pct <= 2.0:
            score += 10     # still approaching from above
        elif -0.5 <= dist_pct < -0.2:
            score += 8      # slight undercut (could reclaim)
        else:
            score += max(0, 5 - abs_dist)

        # 3. HOLDING SUPPORT (0–20 pts)
        #    Recent bar lows staying at or above VWAP = buyers defending
        last_n = recent[-min(10, len(recent)):]
        bars_holding = sum(1 for b in last_n
                          if float(b["l"]) >= vwap * 0.998)
        score += (bars_holding / len(last_n)) * 20

        # 4. VOLUME PATTERN (0–15 pts)
        #    Declining volume on pullback = healthy (institutions accumulating)
        if len(recent) >= 10:
            half = len(recent) // 2
            v1 = sum(int(b["v"]) for b in recent[:half])
            v2 = sum(int(b["v"]) for b in recent[half:])
            if v1 > 0:
                vr = v2 / v1
                score += 15 if vr < 0.7 else (10 if vr < 1.0 else 5)
        else:
            score += 5

        # 5. DIRECTIONAL MOMENTUM (0–15 pts)
        #    Last few bars trending up from VWAP area = bounce confirmation
        if len(recent) >= 5:
            closes = [float(b["c"]) for b in recent[-5:]]
            up = sum(1 for i in range(1, len(closes))
                     if closes[i] > closes[i - 1])
            if up >= 3 and closes[-1] > vwap:
                score += 15
            elif up >= 2 and closes[-1] > vwap:
                score += 10
            elif closes[-1] > vwap:
                score += 5

        # ── Phase label ──────────────────────────────────────────────────
        if dist_pct < -0.5:
            phase = "BELOW"
        elif abs(dist_pct) <= 0.2:
            # At VWAP — distinguish bounce from flat
            if len(recent) >= 3:
                last3 = [float(b["c"]) for b in recent[-3:]]
                if (all(c > vwap for c in last3[-2:])
                        and float(recent[-3]["l"]) <= vwap * 1.002):
                    phase = "BOUNCING"
                elif max_above < 0.3:
                    phase = "FLAT"
                else:
                    phase = "TOUCHING"
            else:
                phase = "TOUCHING"
        elif dist_pct > 0.2 and max_above > dist_pct + 0.3:
            phase = "PULLING BACK"
        elif dist_pct > 0:
            if len(recent) >= 3:
                last3 = [float(b["c"]) for b in recent[-3:]]
                if all(c > vwap for c in last3) and last3[-1] > last3[0]:
                    phase = "BOUNCING"
                else:
                    phase = "ABOVE"
            else:
                phase = "ABOVE"
        else:
            phase = "UNDERCUT"

        return {
            "symbol":           sym,
            "price":            round(price, 2),
            "vwap":             vwap,
            "vwap_distance_pct": round(dist_pct, 2),
            "bounce_score":     round(score, 1),
            "phase":            phase,
            "_score":           score,
            "mas":              cand["mas"],
            "volume":           cand["volume"],
        }

    proximity = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = {pool.submit(_vwap_score, c): c for c in stage2}
        for fut in as_completed(futs):
            try:
                result = fut.result()
                if result:
                    proximity.append(result)
            except Exception as e:
                log("DEBUG", f"vwap scoring error: {e}")

    # ── Step 5: Rank by bounce quality, return top MAX_WATCHLIST ──────────────
    proximity.sort(key=lambda x: x["_score"], reverse=True)
    top = proximity[:MAX_WATCHLIST]
    for c in top:
        c.pop("_score", None)

    log("INFO", f"scan_stage2_universe: returning {len(top)} candidates "
                f"(of {len(proximity)} near VWAP)")
    for c in top[:5]:
        log("DEBUG", f"  {c['symbol']}: ${c['price']:.2f} VWAP ${c['vwap']:.2f} "
                     f"({c['vwap_distance_pct']:+.1f}%) "
                     f"score={c['bounce_score']} phase={c['phase']}")

    return top


# ── VWAP bounce detector (Phase 3) ────────────────────────────────────────────
def find_swing_lows(bars, count=3):
    """
    Identify the last N swing lows (local minima) from 1-min bars.
    Returns list of (bar_index, low_price) tuples, most recent first.

    Used by detect_vwap_bounce() to place stops under the most recent higher low.
    Shannon: "If that higher low is broken, why would I still hold this stock?"
    """
    lows = []
    for i in range(1, len(bars) - 1):
        li = float(bars[i]["l"])
        lp = float(bars[i - 1]["l"])
        ln = float(bars[i + 1]["l"])
        if li < lp and li < ln:
            lows.append((i, li))
    return list(reversed(lows))[:count]   # most recent first


def find_swing_highs(bars, count=5):
    """
    Identify the last N swing highs (local maxima) from 1-min bars.
    Returns list of (bar_index, high_price) tuples, most recent first.

    Used by detect_vwap_bounce() to identify resistance targets above entry.
    """
    highs = []
    for i in range(1, len(bars) - 1):
        hi = float(bars[i]["h"])
        hp = float(bars[i - 1]["h"])
        hn = float(bars[i + 1]["h"])
        if hi > hp and hi > hn:
            highs.append((i, hi))
    return list(reversed(highs))[:count]   # most recent first


def detect_vwap_bounce(sym, daily_mas=None):
    """
    Core pattern detection for KoiScale — Brian Shannon's VWAP bounce.

    Algorithm:
    1. Fetch full session bars; compute intraday VWAP (volume-weighted from open)
    2. Within last LOOKBACK_BARS, find the most recent VWAP touch:
         bar low in [vwap*(1-VWAP_UNDERCUT_MAX), vwap*(1+VWAP_TOUCH_TOLERANCE)]
         i.e. dipped to VWAP but didn't break down hard beneath it
    3. After the touch, confirm strength:
         - At least MIN_BARS_AFTER_TOUCH bars closing ABOVE VWAP (bounce confirmation)
         - Then a bar making a new high above the post-touch range high (breakout trigger)
         → "Don't buy the dip. Buy strength after the dip." — Shannon
    4. Freshness: breakout bar must be within BOUNCE_MAX_AGE_MIN minutes
    5. Compute levels:
         entry    = post-touch range high + $0.02  (stop-limit trigger)
         stop_1   = 2¢ under nearest swing low below entry
         stop_2   = 2¢ under second swing low (deep stop / final tranche)
         target_1 = nearest swing high above entry (first resistance)
         target_2 = next swing high / HOD
    6. Require R:R >= MIN_RR against stop_1
    7. Confluence: count daily MAs within CONFLUENCE_TOLERANCE of VWAP

    Returns signal dict or None if no valid setup found.
    """
    # Full session bars for accurate VWAP; last LOOKBACK_BARS for pattern window
    all_bars = get_1min_bars(sym, limit=420)
    if len(all_bars) < 10:
        log("DEBUG", f"  [{sym}] VWAP bounce: insufficient bars ({len(all_bars)})")
        return None

    vwap = calc_vwap(all_bars)
    if not vwap:
        return None

    bars = all_bars[-LOOKBACK_BARS:]
    n    = len(bars)

    vwap_lo = vwap * (1 - VWAP_UNDERCUT_MAX)    # floor: hard break below VWAP = skip
    vwap_hi = vwap * (1 + VWAP_TOUCH_TOLERANCE)  # ceiling: bar approached from above

    # ── Step 1: Find most recent VWAP touch ──────────────────────────────────
    # Scan backwards; stop early enough to leave room for post-touch confirmation
    touch_idx = None
    for i in range(n - MIN_BARS_AFTER_TOUCH - 1, -1, -1):
        bar_low = float(bars[i]["l"])
        if vwap_lo <= bar_low <= vwap_hi:
            touch_idx = i
            break

    if touch_idx is None:
        log("DEBUG", f"  [{sym}] no VWAP touch in last {n} bars "
                     f"(VWAP=${vwap:.2f} touch-zone ${vwap_lo:.2f}–${vwap_hi:.2f})")
        return None

    # ── Step 2: Confirm strength + find breakout bar ──────────────────────────
    # Scan forward from touch bar. Track closes above VWAP and the running range high.
    # A "breakout" is when a bar's high exceeds the established range high, AFTER
    # MIN_BARS_AFTER_TOUCH bars have already closed above VWAP.
    # If price drops back below VWAP, reset the counter (failed bounce — skip).
    post_touch        = bars[touch_idx + 1:]
    closes_above      = 0
    bounce_range_high = 0.0
    breakout_idx      = None

    for j, bar in enumerate(post_touch):
        bc = float(bar["c"])
        bh = float(bar["h"])

        # Check breakout BEFORE updating range_high (so comparison is against prior high)
        if (closes_above >= MIN_BARS_AFTER_TOUCH
                and bounce_range_high > 0
                and bh > bounce_range_high):
            breakout_idx = touch_idx + 1 + j   # keep re-assigning — want most recent

        if bc > vwap:
            closes_above      += 1
            bounce_range_high  = max(bounce_range_high, bh)
        else:
            # Price lost VWAP again — setup invalidated
            closes_above      = 0
            bounce_range_high = 0.0
            breakout_idx      = None

    if breakout_idx is None:
        log("DEBUG", f"  [{sym}] no breakout strength after touch "
                     f"(closes_above={closes_above}, range_hi={bounce_range_high:.2f})")
        return None

    # ── Step 3: Freshness check ───────────────────────────────────────────────
    breakout_bar = bars[breakout_idx]
    try:
        bar_time = datetime.fromisoformat(
            breakout_bar["t"].replace("Z", "+00:00"))
        age_min = (datetime.now(timezone.utc) - bar_time).total_seconds() / 60
    except Exception:
        age_min = 0.0

    if age_min > BOUNCE_MAX_AGE_MIN:
        log("DEBUG", f"  [{sym}] bounce signal stale ({age_min:.1f} min > {BOUNCE_MAX_AGE_MIN})")
        return None

    # ── Step 4: Trade levels ──────────────────────────────────────────────────
    entry = round(bounce_range_high + 0.02, 2)   # stop-limit trigger

    # Stops: nearest swing lows below entry (Shannon splits stop: near vs. deep)
    swing_lows = find_swing_lows(bars, count=3)
    stop_1 = stop_2 = None
    for _idx, lp in swing_lows:
        if lp < entry:
            if stop_1 is None:
                stop_1 = round(lp - 0.02, 2)
            elif stop_2 is None:
                stop_2 = round(lp - 0.02, 2)
                break

    if stop_1 is None:
        stop_1 = round(float(bars[touch_idx]["l"]) - 0.02, 2)   # touch bar low
    if stop_2 is None:
        risk_est = entry - stop_1
        stop_2   = round(stop_1 - risk_est * 0.5, 2)            # 1.5× initial risk

    risk = round(entry - stop_1, 2)
    if risk <= 0:
        log("DEBUG", f"  [{sym}] invalid risk (entry={entry} stop={stop_1})")
        return None

    # Targets: swing highs above entry, anchored by HOD
    swing_highs = find_swing_highs(bars, count=5)
    hod         = max(float(b["h"]) for b in all_bars)
    min_target  = entry + risk * 0.5   # at least half-risk above entry

    above_entry = sorted(set(
        [round(h, 2) for _, h in swing_highs if h > min_target]
        + [round(hod, 2)]
    ))

    target_1 = above_entry[0] if above_entry            else round(entry + risk * 2, 2)
    target_2 = above_entry[1] if len(above_entry) >= 2  else round(entry + risk * 3, 2)

    rr = round((target_1 - entry) / risk, 2)
    if rr < MIN_RR:
        log("DEBUG", f"  [{sym}] R:R {rr:.2f} < MIN_RR {MIN_RR} "
                     f"(entry={entry} T1={target_1} risk={risk})")
        return None

    # ── Step 5: Confluence score ──────────────────────────────────────────────
    # Count daily MAs within CONFLUENCE_TOLERANCE (1%) of current VWAP.
    # Higher score = more structural support at this price level.
    confluence_score = 0
    if daily_mas:
        for k in ("sma_5", "sma_20", "sma_50", "sma_200"):
            v = daily_mas.get(k)
            if v and abs(v - vwap) / vwap <= CONFLUENCE_TOLERANCE:
                confluence_score += 1

    log("INFO",
        f"  [{sym}] VWAP bounce ✅  "
        f"entry=${entry:.2f}  stop=${stop_1:.2f}  "
        f"T1=${target_1:.2f}  T2=${target_2:.2f}  "
        f"RR={rr:.2f}  age={age_min:.1f}m  "
        f"confluence={confluence_score}  VWAP=${vwap:.2f}")

    return {
        "symbol":           sym,
        "entry":            entry,
        "stop_1":           stop_1,
        "stop_2":           stop_2,
        "target_1":         target_1,
        "target_2":         target_2,
        "risk":             risk,
        "rr":               rr,
        "vwap":             vwap,
        "touch_bar_idx":    touch_idx,
        "breakout_bar_idx": breakout_idx,
        "age_min":          round(age_min, 1),
        "confluence_score": confluence_score,
        "total_bars":       len(all_bars),
    }


# ── Position / exit management (Phase 4) ──────────────────────────────────────
# KoiScale uses software-managed exits (not Alpaca brackets) for 1/3-out scaling.
# State machine per position:
#   PENDING  → entry stop-limit submitted, polling for fill
#   FULL     → entry filled; stop + T1 limit active; all tranches open
#   T1_SOLD  → 1/3 sold at T1; stop moved to breakeven; T2 limit active
#   T2_SOLD  → 2/3 sold; trailing final 1/3 under each new higher swing low
#   CLOSED   → final tranche exited via trailing stop
#   STOPPED  → initial or BE stop triggered
#
# Crash safety rule: ALWAYS submit new stop BEFORE canceling old stop.

def _cancel_order(oid):
    """Cancel an Alpaca order by ID. Ignores errors (already filled/cancelled)."""
    if not oid or str(oid).startswith("DRY-"):
        return
    alp_del(f"/v2/orders/{oid}")


def _place_stop_order(sym, qty, stop_price):
    """Place a market-stop sell order (stop-loss). Returns order dict or None."""
    if DRY_RUN:
        oid = f"DRY-SL-{sym}-{int(time.time())}"
        log("INFO", f"  🔵 DRY RUN — simulated stop order id={oid} stop=${stop_price:.2f}")
        return {"id": oid, "status": "simulated"}
    r = alp_post("/v2/orders", {
        "symbol":        sym,
        "qty":           str(qty),
        "side":          "sell",
        "type":          "stop",
        "stop_price":    str(stop_price),
        "time_in_force": "day",
    })
    if r and r.get("id"):
        log("INFO", f"  [{sym}] stop-loss placed id={r['id'][:8]} stop=${stop_price:.2f}")
    return r


def _place_limit_sell(sym, qty, limit_price, label="TP"):
    """Place a limit sell order (take-profit tranche). Returns order dict or None."""
    if DRY_RUN:
        oid = f"DRY-TP-{sym}-{int(time.time())}"
        log("INFO", f"  🔵 DRY RUN — simulated {label} order id={oid} lmt=${limit_price:.2f}")
        return {"id": oid, "status": "simulated"}
    r = alp_post("/v2/orders", {
        "symbol":        sym,
        "qty":           str(qty),
        "side":          "sell",
        "type":          "limit",
        "limit_price":   str(limit_price),
        "time_in_force": "day",
    })
    if r and r.get("id"):
        log("INFO", f"  [{sym}] {label} limit sell placed id={r['id'][:8]} lmt=${limit_price:.2f}")
    return r


def _place_market_sell(sym, qty):
    """Place a market sell (EOD liquidation). Returns order dict or None."""
    if DRY_RUN:
        oid = f"DRY-MKT-{sym}-{int(time.time())}"
        log("INFO", f"  🔵 DRY RUN — simulated market sell id={oid} qty={qty}")
        return {"id": oid, "status": "simulated"}
    r = alp_post("/v2/orders", {
        "symbol":        sym,
        "qty":           str(qty),
        "side":          "sell",
        "type":          "market",
        "time_in_force": "day",
    })
    if r and r.get("id"):
        log("INFO", f"  [{sym}] market sell placed id={r['id'][:8]} qty={qty}")
    return r


def _order_filled(oid):
    """
    Check if an Alpaca order has filled.
    Returns (filled: bool, avg_price: float, filled_qty: int).
    DRY_RUN: entry orders simulate fill; stop/TP orders stay open until EOD.
    """
    if not oid:
        return False, 0.0, 0
    s = str(oid)
    if s.startswith("DRY-SL-") or s.startswith("DRY-TP-") or s.startswith("DRY-MKT-"):
        return False, 0.0, 0   # open until EOD (don't auto-close in dry-run)
    if s.startswith("DRY-"):
        return True, 0.0, 0    # entry: simulate immediate fill
    o = alp_get(f"/v2/orders/{oid}")
    if not o:
        return False, 0.0, 0
    status    = o.get("status", "")
    filled_q  = int(float(o.get("filled_qty",        0) or 0))
    avg_px    = float(o.get("filled_avg_price", 0) or 0)
    if status in ("filled", "partially_filled") and filled_q > 0:
        return True, avg_px, filled_q
    return False, 0.0, 0


def _order_status(oid):
    """Return Alpaca order status string. DRY: stop/TP orders stay 'accepted'."""
    if not oid:
        return "unknown"
    s = str(oid)
    if s.startswith("DRY-SL-") or s.startswith("DRY-TP-") or s.startswith("DRY-MKT-"):
        return "accepted"
    if s.startswith("DRY-"):
        return "filled"
    o = alp_get(f"/v2/orders/{oid}")
    return (o.get("status", "unknown") if o else "unknown")


def _finalize_position(pos, outcome):
    """Record a closed position into trades_today and notify."""
    sym = pos["symbol"]
    pos["outcome"]    = outcome
    pos["close_time"] = now_et().isoformat()
    S.trades_today.append(dict(pos))
    S.trade_count += 1
    S.traded_today.add(sym)

    emoji = {"STOPPED": "🔴", "STOPPED_BE": "⚪", "CLOSED": "🟢", "EOD": "⏰"}.get(outcome, "❓")
    log("INFO", f"  [{sym}] position closed: {outcome}")
    tg_send(
        f"{emoji} *{sym}* closed — {outcome}\n"
        f"Entry: ${pos.get('avg_entry', pos['entry']):.2f}  "
        f"Stop: ${pos['stop_1']:.2f}\n"
        f"T1: ${pos['target_1']:.2f}  T2: ${pos['target_2']:.2f}  "
        f"RR: {pos['rr']:.1f}\n"
        f"Trades today: {S.trade_count}")
    sheets_push({
        "type":       "trade_close",
        "symbol":     sym,
        "outcome":    outcome,
        "entry":      pos.get("avg_entry", pos["entry"]),
        "stop_1":     pos["stop_1"],
        "target_1":   pos["target_1"],
        "target_2":   pos["target_2"],
        "rr":         pos["rr"],
        "vwap":       pos["vwap"],
        "confluence": pos.get("confluence", 0),
        "choppy":     pos.get("choppy", False),
    })


def place_entry_order(sym, qty, entry, sig):
    """
    Submit standalone stop-limit buy order (no bracket).
      stop_price  = entry  (triggers when price breaks through)
      limit_price = entry + MAX_ENTRY_SLIP  (max slippage tolerated)

    Pre-checks spread. Computes tranche sizes. Registers position in
    S.active_positions. Returns position dict or None on failure.
    """
    ceiling = round(entry + MAX_ENTRY_SLIP, 2)

    # Spread check
    bid, ask = get_quote(sym)
    if bid and ask:
        spread_pct = (ask - bid) / ask * 100
        if spread_pct > MAX_SPREAD_PCT:
            log("INFO", f"  [{sym}] SKIP: spread {spread_pct:.2f}% > {MAX_SPREAD_PCT}%")
            return None

    # Tranche sizes — clamp to prevent inflation above the sized qty
    t1_pct    = CHOPPY_T1_PCT if S.choppy_mode else TRANCHE_1_PCT
    t1_qty    = max(1, int(qty * t1_pct))
    t2_qty    = max(1, int(qty * TRANCHE_2_PCT))
    t3_qty    = qty - t1_qty - t2_qty
    if t3_qty < 1:
        # Small position: can't split into 3, give remainder to T1
        t3_qty = 0
        t2_qty = max(1, qty - t1_qty)
        if t2_qty + t1_qty > qty:
            t2_qty = 0   # single-tranche: sell all at T1
    total_qty = t1_qty + t2_qty + t3_qty

    log("INFO",
        f"ORDER: {sym} BUY {total_qty}sh "
        f"STOP-LMT trigger=${entry:.2f} ceil=${ceiling:.2f}  "
        f"T1={t1_qty} T2={t2_qty} T3={t3_qty}")

    if DRY_RUN:
        oid = f"DRY-{sym}-{int(time.time())}"
        log("INFO", f"  🔵 DRY RUN — simulated entry id={oid}")
        tg_send(
            f"🔵 *DRY RUN* — {sym}\n"
            f"BUY {total_qty}sh STOP-LMT trigger=${entry:.2f} ceil=${ceiling:.2f}\n"
            f"Stop: ${sig['stop_1']:.2f}  T1: ${sig['target_1']:.2f}  "
            f"T2: ${sig['target_2']:.2f}  RR: {sig['rr']:.1f}\n"
            f"VWAP: ${sig['vwap']:.2f}  Confluence: {sig['confluence_score']}")
        order = {"id": oid, "status": "simulated"}
    else:
        order = alp_post("/v2/orders", {
            "symbol":         sym,
            "qty":            str(total_qty),
            "side":           "buy",
            "type":           "stop_limit",
            "stop_price":     str(entry),
            "limit_price":    str(ceiling),
            "time_in_force":  "day",
            "extended_hours": False,
        })
        if not order or not order.get("id"):
            log("WARN", f"  [{sym}] entry order failed: {order}")
            return None
        tg_send(
            f"📍 *{sym}* entry order placed\n"
            f"BUY {total_qty}sh STOP-LMT trigger=${entry:.2f} ceil=${ceiling:.2f}\n"
            f"Stop: ${sig['stop_1']:.2f}  T1: ${sig['target_1']:.2f}  "
            f"T2: ${sig['target_2']:.2f}  RR: {sig['rr']:.1f}\n"
            f"VWAP: ${sig['vwap']:.2f}  Confluence: {sig['confluence_score']}")

    pos = {
        "symbol":         sym,
        "state":          "PENDING",
        "entry_order_id": order["id"],
        "stop_order_id":  None,
        "t1_order_id":    None,
        "t2_order_id":    None,
        "total_qty":      total_qty,
        "t1_qty":         t1_qty,
        "t2_qty":         t2_qty,
        "t3_qty":         t3_qty,
        "filled_qty":     0,
        "sold_qty":       0,
        "avg_entry":      entry,
        "entry":          entry,
        "stop_1":         sig["stop_1"],
        "stop_2":         sig["stop_2"],
        "target_1":       sig["target_1"],
        "target_2":       sig["target_2"],
        "risk":           sig["risk"],
        "rr":             sig["rr"],
        "vwap":           sig["vwap"],
        "trailing_stop":  sig["stop_1"],
        "time":           now_et().isoformat(),
        "confluence":     sig["confluence_score"],
        "choppy":         S.choppy_mode,
        "pending_since":  time.time(),
    }
    S.active_positions[order["id"]] = pos
    return pos


def manage_exits():
    """
    Run every cycle. Manage 1/3-out exit state machine for all active positions.

    PENDING  → poll entry order; on fill: submit stop + T1 limit → FULL
    FULL     → watch stop fill (→ STOPPED) or T1 fill (cancel stop,
               submit BE stop + T2 limit → T1_SOLD)
    T1_SOLD  → watch stop fill (→ STOPPED_BE) or T2 fill (replace stop
               with trailing stop → T2_SOLD)
    T2_SOLD  → each cycle: raise trailing stop under new higher swing lows;
               watch stop fill → CLOSED
    Any      → EOD 15:55: cancel all orders, market-sell remaining shares

    Crash safety: always submit new stop BEFORE canceling old stop.
    """
    if not S.active_positions:
        return

    t       = now_et()
    hh, mm  = t.hour, t.minute
    eod_now = (hh == EOD_LIQUIDATE[0] and mm >= EOD_LIQUIDATE[1])

    to_remove = []

    for oid, pos in list(S.active_positions.items()):
        sym   = pos["symbol"]
        state = pos["state"]

        # ── EOD forced liquidation ────────────────────────────────────────
        if eod_now and state not in ("CLOSED", "STOPPED"):
            # For PENDING positions that never filled, just cancel the entry
            if state == "PENDING":
                _cancel_order(pos.get("entry_order_id"))
                log("INFO", f"  [{sym}] EOD: PENDING order canceled (never filled)")
                to_remove.append(oid)
                S.traded_today.discard(sym)
                continue
            remaining = pos["filled_qty"] - pos.get("sold_qty", 0)
            if remaining > 0:
                log("WARN", f"  [{sym}] EOD liquidation: market sell {remaining}sh")
                _cancel_order(pos.get("stop_order_id"))
                _cancel_order(pos.get("t1_order_id"))
                _cancel_order(pos.get("t2_order_id"))
                _place_market_sell(sym, remaining)
                tg_send(f"⏰ *{sym}* EOD — market sell {remaining}sh")
            pos["state"] = "CLOSED"
            _finalize_position(pos, "EOD")
            to_remove.append(oid)
            continue

        # ── PENDING: waiting for entry fill ───────────────────────────────
        if state == "PENDING":
            # Cancel stale entries (signal expired)
            age_min = (time.time() - pos["pending_since"]) / 60
            if age_min > BOUNCE_MAX_AGE_MIN:
                log("INFO", f"  [{sym}] PENDING expired ({age_min:.1f} min) — canceling")
                _cancel_order(pos["entry_order_id"])
                to_remove.append(oid)
                S.traded_today.discard(sym)
                continue

            filled, avg_px, fq = _order_filled(pos["entry_order_id"])
            if not filled:
                continue

            pos["avg_entry"] = avg_px if avg_px > 0 else pos["entry"]
            pos["filled_qty"] = pos["total_qty"] if fq == 0 else fq

            # Submit stop-loss first (crash safety), then T1 limit
            sl_r = _place_stop_order(sym, pos["filled_qty"], pos["stop_1"])
            if sl_r:
                pos["stop_order_id"] = sl_r["id"]

            t1_r = _place_limit_sell(sym, pos["t1_qty"], pos["target_1"], label="T1")
            if t1_r:
                pos["t1_order_id"] = t1_r["id"]

            pos["state"] = "FULL"
            log("INFO",
                f"  [{sym}] PENDING→FULL  avg=${pos['avg_entry']:.2f}  "
                f"stop=${pos['stop_1']:.2f}  T1=${pos['target_1']:.2f}")
            tg_send(
                f"✅ *{sym}* FILLED @ ${pos['avg_entry']:.2f}\n"
                f"Stop: ${pos['stop_1']:.2f} | T1: ${pos['target_1']:.2f} | "
                f"T2: ${pos['target_2']:.2f}\n"
                f"{pos['filled_qty']}sh ({pos['t1_qty']}+{pos['t2_qty']}+{pos['t3_qty']})")
            continue

        # ── FULL: watch for T1 fill or stop hit ───────────────────────────
        if state == "FULL":
            # Check stop first (higher priority — protect capital)
            if _order_status(pos.get("stop_order_id")) == "filled":
                _cancel_order(pos.get("t1_order_id"))
                pos["state"] = "STOPPED"
                _finalize_position(pos, "STOPPED")
                to_remove.append(oid)
                continue

            t1_filled, _, _ = _order_filled(pos.get("t1_order_id"))
            if not t1_filled:
                continue

            pos["sold_qty"] += pos["t1_qty"]
            remaining        = pos["filled_qty"] - pos["sold_qty"]
            be_stop          = pos["avg_entry"]

            # New BE stop first, then cancel old stop (crash safety)
            new_sl = _place_stop_order(sym, remaining, be_stop)
            if new_sl:
                _cancel_order(pos.get("stop_order_id"))
                pos["stop_order_id"] = new_sl["id"]
                pos["trailing_stop"] = be_stop

            t2_r = _place_limit_sell(sym, pos["t2_qty"], pos["target_2"], label="T2")
            if t2_r:
                pos["t2_order_id"] = t2_r["id"]

            pos["state"] = "T1_SOLD"
            log("INFO",
                f"  [{sym}] T1_SOLD  stop→BE ${be_stop:.2f}  "
                f"T2=${pos['target_2']:.2f}  {remaining}sh left")
            tg_send(
                f"💰 *{sym}* T1 hit — 1/3 sold @ ${pos['target_1']:.2f}\n"
                f"Stop → breakeven ${be_stop:.2f}\n"
                f"T2: ${pos['target_2']:.2f} | {remaining}sh remaining")
            continue

        # ── T1_SOLD: watch for T2 fill or BE stop hit ─────────────────────
        if state == "T1_SOLD":
            if _order_status(pos.get("stop_order_id")) == "filled":
                _cancel_order(pos.get("t2_order_id"))
                pos["state"] = "STOPPED"
                _finalize_position(pos, "STOPPED_BE")
                to_remove.append(oid)
                continue

            t2_filled, _, _ = _order_filled(pos.get("t2_order_id"))
            if not t2_filled:
                continue

            pos["sold_qty"] += pos["t2_qty"]
            remaining        = pos["filled_qty"] - pos["sold_qty"]
            pos["t3_qty"]    = remaining

            # Set initial trailing stop under nearest swing low
            bars        = get_1min_bars(sym, limit=LOOKBACK_BARS)
            swing_lows  = find_swing_lows(bars, count=3)
            trail_cands = [round(lp - 0.02, 2) for _, lp in swing_lows
                           if lp < pos["target_2"]]
            trail_stop  = trail_cands[0] if trail_cands else pos["trailing_stop"]

            new_sl = _place_stop_order(sym, remaining, trail_stop)
            if new_sl:
                _cancel_order(pos.get("stop_order_id"))
                pos["stop_order_id"] = new_sl["id"]
                pos["trailing_stop"] = trail_stop

            pos["state"] = "T2_SOLD"
            log("INFO",
                f"  [{sym}] T2_SOLD  trail_stop=${trail_stop:.2f}  "
                f"{remaining}sh remaining")
            tg_send(
                f"💰 *{sym}* T2 hit — 2/3 sold @ ${pos['target_2']:.2f}\n"
                f"Trailing {remaining}sh | Stop: ${trail_stop:.2f}")
            continue

        # ── T2_SOLD: trail final 1/3 under each new higher swing low ──────
        if state == "T2_SOLD":
            if _order_status(pos.get("stop_order_id")) == "filled":
                pos["state"] = "CLOSED"
                _finalize_position(pos, "CLOSED")
                to_remove.append(oid)
                continue

            # Raise trailing stop if a higher swing low has formed
            bars        = get_1min_bars(sym, limit=LOOKBACK_BARS)
            swing_lows  = find_swing_lows(bars, count=3)
            new_highs   = [
                round(lp - 0.02, 2) for _, lp in swing_lows
                if lp - 0.02 > pos["trailing_stop"] + 0.02
            ]
            if new_highs:
                new_trail = max(new_highs)
                remaining = pos.get("t3_qty",
                                    pos["filled_qty"] - pos.get("sold_qty", 0))
                new_sl = _place_stop_order(sym, remaining, new_trail)
                if new_sl:
                    _cancel_order(pos.get("stop_order_id"))
                    pos["stop_order_id"] = new_sl["id"]
                    pos["trailing_stop"] = new_trail
                    log("INFO",
                        f"  [{sym}] trailing stop raised ${new_trail:.2f}")

    for oid in to_remove:
        S.active_positions.pop(oid, None)


# ── State ─────────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.watchlist          = []     # list of {symbol, price, vwap_distance, mas, ...}
        self.active_positions   = {}     # order_id -> position state dict (Phase 4)
        self.traded_today       = set()  # syms with submitted orders today
        self.trades_today       = []     # list of completed trade dicts
        self.trade_count        = 0
        self.pnl                = 0.0
        self.scanned            = False
        self.halted             = False  # loss-limit auto-halt
        self.stopped            = False  # manual /stop
        self.choppy_mode        = CHOPPY_MODE   # from env var or /choppy toggle
        self.last_trade_time    = None
        self.date               = ""
        self.equity             = 0.0
        self.offset             = 0      # Telegram update offset
        self.force_scan         = False
        self.start_time         = now_et().isoformat()
        self.last_heartbeat     = 0.0
        self.last_scan_time     = 0.0    # unix ts of last stage-2 universe scan
        self.consecutive_red_days = 0

    def new_day(self, d):
        log("INFO", f"=== NEW DAY: {d} — KoiScale state reset ===")
        if self.date:
            if self.pnl < 0:
                self.consecutive_red_days += 1
                log("INFO", f"  Consecutive red days: {self.consecutive_red_days}"
                    + (" — drawdown sizing ACTIVE (0.5×)" if self.consecutive_red_days >= 2 else ""))
            else:
                if self.consecutive_red_days:
                    log("INFO", f"  Red streak reset (was {self.consecutive_red_days} days)")
                self.consecutive_red_days = 0
        self.watchlist          = []
        self.active_positions   = {}
        self.traded_today       = set()
        self.trades_today       = []
        self.trade_count        = 0
        self.pnl                = 0.0
        self.scanned            = False
        self.halted             = False
        self.stopped            = False
        self.last_trade_time    = None
        self.date               = d
        self.force_scan         = False
        self.last_scan_time     = 0.0
        with _daily_ma_lock:
            _daily_ma_cache.clear()
        with _bar_lock:
            _bar_cache.clear()
        _ws_subscribed.clear()
        log("INFO", "  Caches cleared for new session (daily MA, bar cache, WS subscriptions)")

    def save(self):
        try:
            with open(STATUS_FILE, "w") as f:
                json.dump({
                    "time":                 now_et().isoformat(),
                    "equity":               self.equity,
                    "watchlist":            self.watchlist,
                    "trade_count":          self.trade_count,
                    "pnl":                  round(self.pnl, 2),
                    "scanned":              self.scanned,
                    "halted":               self.halted,
                    "stopped":              self.stopped,
                    "choppy_mode":          self.choppy_mode,
                    "started":              self.start_time,
                    "consecutive_red_days": self.consecutive_red_days,
                    "active_positions":     len(self.active_positions),
                }, f, indent=2)
        except Exception:
            pass
        try:
            with open(TRADES_FILE, "w") as f:
                json.dump(self.trades_today, f, indent=2)
        except Exception:
            pass


S = State()

# ── Telegram command handler ───────────────────────────────────────────────────
_unauthorized_warned = {}   # chat_id -> last_warn_time (rate-limit to 1 per 5 min)

def handle_cmd(text, cid):
    if str(cid) != str(TG_CHAT):
        now = time.time()
        last = _unauthorized_warned.get(str(cid), 0)
        if now - last > 300:   # warn at most once per 5 minutes per chat
            log("WARN", f"Ignored message from unauthorized chat {cid} "
                        f"(expected {TG_CHAT!r})")
            _unauthorized_warned[str(cid)] = now
        return
    cmd = (text.strip().split()[0] if text else "").lower()

    if cmd == "/status":
        try:
            with open(STATUS_FILE) as f:
                d = json.load(f)
        except Exception:
            d = {}
        tg_send(
            f"📊 *KoiScale v1 Status*\n"
            f"Time:      {d.get('time','?')}\n"
            f"Equity:    ${d.get('equity', 0):,.2f}\n"
            f"Watchlist: {len(d.get('watchlist', []))} symbols\n"
            f"Positions: {d.get('active_positions', 0)} active\n"
            f"Trades:    {d.get('trade_count', 0)} / {MAX_TRADES}\n"
            f"P&L:       ${d.get('pnl', 0):+.2f} (goal ${DAILY_TARGET:.0f})\n"
            f"Scanned:   {d.get('scanned', False)}\n"
            f"Halted:    {d.get('halted', False)}\n"
            f"Stopped:   {d.get('stopped', False)}\n"
            f"Choppy:    {'🟡 ON (fast exits)' if d.get('choppy_mode') else '⚪ OFF'}\n"
            f"Red streak:{d.get('consecutive_red_days', 0)}d"
            + (" — drawdown sizing ACTIVE (0.5×)" if d.get('consecutive_red_days', 0) >= 2 else "") + "\n"
            f"Dry-run:   {'🔵 ON' if DRY_RUN else '🟢 OFF'}\n"
            f"Strategy:  VWAP mean reversion (Brian Shannon)\n"
            f"Window:    9:35–15:30 ET")

    elif cmd == "/watchlist":
        if S.watchlist:
            lines = []
            for w in S.watchlist[:10]:   # cap at 10 for Telegram message length
                phase = w.get('phase', '?')
                bs    = w.get('bounce_score', 0)
                lines.append(
                    f"  *{w.get('symbol','?')}* "
                    f"${w.get('price', 0):.2f} "
                    f"VWAP ${w.get('vwap', 0):.2f} "
                    f"({w.get('vwap_distance_pct', 0):+.1f}%) "
                    f"_{phase}_ [{bs:.0f}]")
            tg_send("📋 *KoiScale Watchlist* (score / phase)\n" + "\n".join(lines))
        else:
            tg_send("📋 Watchlist empty — no stage-2 candidates near VWAP yet (scan runs every 15 min)")

    elif cmd == "/trades":
        if S.trades_today:
            lines = []
            for tr in S.trades_today:
                lines.append(
                    f"  *{tr.get('symbol','?')}* {tr.get('qty',0)}sh "
                    f"@ ${tr.get('entry', 0):.2f}\n"
                    f"    SL ${tr.get('stop_1', 0):.2f} "
                    f"T1 ${tr.get('target_1', 0):.2f} "
                    f"T2 ${tr.get('target_2', 0):.2f}\n"
                    f"    RR {tr.get('rr', 0):.1f} | "
                    f"State: {tr.get('state', '?')} | {tr.get('time', '?')}")
            tg_send("🗂 *Today's Trades*\n" + "\n".join(lines))
        else:
            tg_send("🗂 No trades yet today")

    elif cmd == "/logs":
        try:
            with open(LOG_FILE) as f:
                lines = f.readlines()[-20:]
            tg_send("📜 *Last 20 lines*\n```\n" + "".join(lines) + "```")
        except Exception:
            tg_send("Log file not found")

    elif cmd == "/scan":
        S.force_scan = True
        tg_send("🔄 Manual scan queued (~30s) — scanning for stage-2 stocks near VWAP")

    elif cmd == "/stop":
        S.stopped = True
        S.save()
        tg_send(f"⏹ *KoiScale STOPPED*\n"
                f"Trades: {S.trade_count} | P&L: ${S.pnl:+.2f}\n"
                f"Send /resume to restart")

    elif cmd == "/resume":
        S.stopped = False
        S.halted  = False
        S.save()
        tg_send("▶ *KoiScale RESUMED*")

    elif cmd == "/choppy":
        S.choppy_mode = not S.choppy_mode
        S.save()
        state = "🟡 ON (sell 1/2 at T1, half size)" if S.choppy_mode else "⚪ OFF (standard 1/3 exits)"
        tg_send(f"📉 Choppy mode: {state}")

    elif cmd == "/wsstatus":
        with _bar_lock:
            n_cached = len(_bar_cache)
            cache_info = "\n".join(
                f"  {sym}: {len(bars)} bars"
                for sym, bars in list(_bar_cache.items())[:5]
            ) or "  (none)"
        tg_send(
            f"📡 *Data Feed Status*\n"
            f"Mode: REST polling (Alpaca IEX → Polygon REST)\n"
            f"Alpaca WS: disabled (shared API key — reserved for GreenHebi)\n"
            f"Cycle interval: ~8s\n"
            f"Bar cache ({n_cached} symbol(s)):\n{cache_info}")

    elif cmd == "/help":
        tg_send(
            f"*KoiScale v1 Commands*\n"
            f"/status   — bot state, equity, P&L\n"
            f"/watchlist — current stage-2 candidates\n"
            f"/trades   — today's trade records\n"
            f"/logs     — last 20 log lines\n"
            f"/scan     — force immediate re-scan\n"
            f"/stop     — halt all trading\n"
            f"/resume   — resume after halt/stop\n"
            f"/choppy   — toggle choppy mode (fast exits)\n"
            f"/wsstatus — Alpaca WebSocket + bar cache state\n"
            f"/help     — this message\n\n"
            f"Strategy: VWAP mean reversion (Brian Shannon)\n"
            f"Window:   9:35–15:30 ET | all-day VWAP setups")

    else:
        tg_send(f"Unknown command: {cmd}\nSend /help for available commands")


# ── Dashboard HTTP server ──────────────────────────────────────────────────────
DASHBOARD_PORT = int(os.environ.get("PORT") or os.environ.get("DASHBOARD_PORT", "8080"))


class DashboardHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass   # silence default HTTP logging

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]

        if path in ("/", ""):
            self._json({"status": "ok", "bot": "KoiScale v1"})
            return

        if path == "/health":
            self._json({"status": "ok", "time": now_et().isoformat()})
            return

        if path in ("/api/dashboard", "/api/log"):
            provided = (self.path.split("token=")[-1].split("&")[0]
                        if "token=" in self.path else "")
            if DASH_TOKEN and provided != DASH_TOKEN:
                self._json({"error": "unauthorized"}, status=401)
                return

        if path == "/api/dashboard":
            self._serve_dashboard()
            return

        if path == "/api/log":
            self._serve_log()
            return

        self.send_response(404)
        self._cors()
        self.end_headers()

    def _json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", len(body))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _serve_log(self):
        try:
            with open(LOG_FILE) as f:
                lines = f.readlines()[-50:]
            self._json({"lines": lines})
        except Exception as e:
            self._json({"lines": [], "error": str(e)})

    def _serve_dashboard(self):
        try:
            acct      = alp_get("/v2/account") or {}
            positions = alp_get("/v2/positions") or []
            orders    = alp_get("/v2/orders?status=open&limit=50") or []
            fills     = alp_get("/v2/account/activities?activity_types=FILL&page_size=20") or []
            portfolio = alp_get("/v2/account/portfolio/history"
                                "?period=1D&timeframe=5Min&extended_hours=true") or {}

            equity  = float(acct.get("equity", 0))
            last_eq = float(acct.get("last_equity", equity))
            day_pnl = round(equity - last_eq, 2)

            bot_state = {}
            try:
                with open(STATUS_FILE) as f:
                    bot_state = json.load(f)
            except Exception:
                pass

            trades_today = []
            try:
                with open(TRADES_FILE) as f:
                    trades_today = json.load(f)
            except Exception:
                pass

            self._json({
                "ok":         True,
                "fetched_at": now_et().isoformat(),
                "account": {
                    "equity":       equity,
                    "last_equity":  last_eq,
                    "day_pnl":      day_pnl,
                    "buying_power": float(acct.get("buying_power", 0)),
                    "cash":         float(acct.get("cash", 0)),
                    "status":       acct.get("status", ""),
                },
                "positions":    positions,
                "orders":       orders,
                "fills":        fills,
                "portfolio":    portfolio,
                "bot": {
                    "trade_count":      bot_state.get("trade_count", 0),
                    "pnl":              bot_state.get("pnl", 0),
                    "scanned":          bot_state.get("scanned", False),
                    "halted":           bot_state.get("halted", False),
                    "stopped":          bot_state.get("stopped", False),
                    "choppy_mode":      bot_state.get("choppy_mode", False),
                    "watchlist":        bot_state.get("watchlist", []),
                    "active_positions": bot_state.get("active_positions", 0),
                    "started":          bot_state.get("started", ""),
                },
                "trades_today": trades_today,
                "config": {
                    "daily_target":  DAILY_TARGET,
                    "max_loss":      MAX_LOSS,
                    "max_trades":    MAX_TRADES,
                    "risk_pct":      RISK_PCT,
                    "price_lo":      PRICE_LO,
                    "price_hi":      PRICE_HI,
                    "min_avg_vol":   MIN_AVG_VOLUME,
                    "strategy":      "VWAP mean reversion (Brian Shannon)",
                    "window":        "9:35–15:30 ET",
                    "phase":         "5 — fully integrated (paper trading ready)",
                },
            })
        except Exception as e:
            log("ERROR", f"Dashboard API error: {e}")
            self._json({"ok": False, "error": str(e)}, status=500)


def start_dashboard_server():
    """Bind dashboard/health check port. Uses SO_REUSEADDR for clean restarts."""
    try:
        class ReusableTCPServer(HTTPServer):
            allow_reuse_address = True
        server = ReusableTCPServer(("0.0.0.0", DASHBOARD_PORT), DashboardHandler)
        log("INFO", f"Dashboard API listening on port {DASHBOARD_PORT}")
        log("INFO", f"  Endpoints: /health  /api/dashboard  /api/log")
        server.serve_forever()
    except Exception as e:
        log("ERROR", f"Dashboard server failed to start: {e}")


# ── Main cycle ─────────────────────────────────────────────────────────────────
def cycle():
    t      = now_et()
    hh, mm = t.hour, t.minute
    today  = t.strftime("%Y-%m-%d")
    is_wd  = (t.weekday() < 5)

    # ── Day rollover ──────────────────────────────────────────────────────────
    if S.date != today:
        S.new_day(today)

    # ── Heartbeat (every 30 min, outside market hours) ────────────────────────
    now_ts = time.time()
    if (hh < 9 or hh >= 16) and (now_ts - S.last_heartbeat > 1800):
        log("INFO", f"Heartbeat | {t.strftime('%H:%M ET')} | equity ${S.equity:,.2f}")
        S.last_heartbeat = now_ts

    # ── Trading window check ──────────────────────────────────────────────────
    in_window = (
        is_wd
        and ((hh == CORE_START[0] and mm >= CORE_START[1]) or hh > CORE_START[0])
        and ((hh == CORE_END[0]   and mm <= CORE_END[1])   or hh < CORE_END[0])
    )

    # ── Manage open positions every cycle ────────────────────────────────────
    manage_exits()

    # ── EOD liquidation (handled inside manage_exits at 15:55 ET) ────────────
    if is_wd and hh == EOD_LIQUIDATE[0] and mm >= EOD_LIQUIDATE[1]:
        if S.active_positions:
            log("DEBUG", f"EOD: {len(S.active_positions)} positions handled by manage_exits")

    # ── EOD scorecard 16:00–16:05 ET ─────────────────────────────────────────
    if is_wd and hh == 16 and mm < 5 and S.scanned:
        acc      = get_account()
        S.equity = float(acc.get("equity", S.equity))
        last_eq  = float(acc.get("last_equity", S.equity))
        S.pnl    = round(S.equity - last_eq, 2)

        outcome = ("🟢 Green day!" if S.pnl > 0
                   else ("🔴 Red day" if S.pnl < 0 else "⚪ Flat day"))

        trade_lines = ""
        if S.trades_today:
            trade_lines = "\n\n*Trades:*\n" + "\n".join(
                f"  {tr.get('symbol','?')} {tr.get('qty',0)}sh "
                f"@ ${tr.get('entry',0):.2f} "
                f"[{tr.get('state','?')}]"
                for tr in S.trades_today)

        tg_send(
            f"📊 *EOD KoiScale — {t.strftime('%a %b %d')}*\n"
            f"{outcome}\n"
            f"Trades: {S.trade_count} | P&L: ${S.pnl:+.2f}\n"
            f"Equity: ${S.equity:,.2f}\n"
            f"Candidates: {len(S.watchlist)}"
            + (f"\nRed streak: {S.consecutive_red_days}d — drawdown sizing ACTIVE (0.5×)"
               if S.consecutive_red_days >= 2 else
               (f"\nRed streak: {S.consecutive_red_days}d" if S.consecutive_red_days else ""))
            + trade_lines)

        sheets_push({
            "type":        "eod",
            "date":        t.strftime("%Y-%m-%d"),
            "day_name":    t.strftime("%A"),
            "trades":      S.trade_count,
            "pnl":         S.pnl,
            "equity":      S.equity,
            "last_equity": last_eq,
            "candidates":  len(S.watchlist),
            "outcome":     outcome,
            "choppy_mode": S.choppy_mode,
            "strategy":    "VWAP mean reversion",
            "trades_detail": S.trades_today,
        })
        S.scanned = False
        S.save()
        return

    if not in_window:
        return

    # ── Update live equity/P&L ────────────────────────────────────────────────
    live_pnl, live_eq = get_live_pnl()
    if live_pnl is not None:
        S.pnl    = live_pnl
        S.equity = live_eq

    # ── Daily loss limit check ────────────────────────────────────────────────
    if S.pnl <= MAX_LOSS and not S.halted:
        S.halted = True
        tg_send(f"🛑 *DAILY LOSS LIMIT HIT*\n"
                f"P&L: ${S.pnl:+.2f} | Limit: ${MAX_LOSS:.0f}\n"
                f"All trading halted. Send /resume to override.")
        log("WARN", f"Loss limit hit: ${S.pnl:.2f}")
        S.save()

    if S.halted or S.stopped:
        return

    # ── Stage-2 universe scan (Phase 2) ──────────────────────────────────────
    # Re-scan every SCAN_INTERVAL_MINS, or on force_scan, or at session open.
    should_scan = (
        S.force_scan
        or not S.scanned
        or (now_ts - S.last_scan_time) >= SCAN_INTERVAL_MINS * 60
    )

    if should_scan:
        S.force_scan    = False
        S.last_scan_time = now_ts
        log("INFO", f"Running stage-2 universe scan at {t.strftime('%H:%M ET')}")
        candidates = scan_stage2_universe()
        if candidates:
            S.watchlist = candidates
            S.scanned   = True
            log("INFO", f"Scan found {len(candidates)} stage-2 candidates")
            tg_send(f"📡 *KoiScale Scan — {len(candidates)} candidates*\n"
                    + "\n".join(f"  *{c.get('symbol','?')}* "
                                f"${c.get('price',0):.2f} "
                                f"({c.get('vwap_distance_pct',0):+.1f}%) "
                                f"_{c.get('phase','?')}_ [{c.get('bounce_score',0):.0f}]"
                                for c in candidates[:5])
                    + ("\n  ..." if len(candidates) > 5 else ""))
        else:
            S.scanned = True
            log("INFO", "Scan: no stage-2 candidates near VWAP this cycle")

    if not S.watchlist:
        return

    # Count both finalized trades and active (PENDING/FULL/etc.) positions
    total_slots_used = S.trade_count + len(S.active_positions)
    if total_slots_used >= MAX_TRADES:
        return

    # ── VWAP bounce detection (Phase 3) ──────────────────────────────────────
    positions  = get_positions()
    held_syms  = {p.get("symbol", "") for p in (positions or [])}

    for w in S.watchlist:
        sym = w.get("symbol", "")
        if not sym:
            continue
        if sym in S.traded_today or sym in held_syms:
            continue
        if S.trade_count + len(S.active_positions) >= MAX_TRADES:
            break

        daily_mas = _daily_ma_cache.get(sym)
        sig = detect_vwap_bounce(sym, daily_mas=daily_mas)
        if not sig:
            continue

        # ── Position sizing ───────────────────────────────────────────────────
        equity = S.equity
        if S.consecutive_red_days >= 2:
            equity *= 0.5          # drawdown sizing after 2+ red days
        if S.choppy_mode:
            equity *= CHOPPY_SIZE_MULT

        risk_per_trade = equity * RISK_PCT
        qty = max(1, int(risk_per_trade / sig["risk"]))
        # Hard cap: never commit more than 20% of full equity to one name
        max_qty = max(1, int(S.equity * 0.20 / sig["entry"]))
        qty = min(qty, max_qty)

        # ── Place entry and register position ─────────────────────────────────
        pos = place_entry_order(sym, qty, sig["entry"], sig)
        if pos:
            S.traded_today.add(sym)
            log("INFO", f"  [{sym}] position registered: state={pos['state']} qty={qty}")


# ── Startup ────────────────────────────────────────────────────────────────────
def startup():
    log("INFO", "=" * 60)
    log("INFO", "KoiScale v1 starting — VWAP Mean Reversion (Brian Shannon)")
    log("INFO", f"  Python:        {sys.version.split()[0]}")
    log("INFO", f"  PID:           {os.getpid()}")
    log("INFO", f"  Polygon:       {'✅' if POLYGON_KEY else '❌ MISSING'}")
    log("INFO", f"  Alpaca:        {'✅' if ALPACA_KEY  else '❌ MISSING'}")
    log("INFO", f"  Telegram:      {'✅' if TG_TOKEN    else '❌ MISSING'}")
    log("INFO", f"  TG Chat ID:    {TG_CHAT or '(empty)'}")
    log("INFO", f"  DRY RUN:       {'🔵 ON — no real orders' if DRY_RUN else '🟢 LIVE trading'}")
    log("INFO", f"  Choppy mode:   {'🟡 ON (fast exits)' if CHOPPY_MODE else '⚪ OFF'}")
    log("INFO", f"  Phase:         5 — fully integrated (paper trading ready)")
    log("INFO", f"  Phase 2:       scan_stage2_universe() ✅")
    log("INFO", f"  Phase 3:       detect_vwap_bounce() ✅")
    log("INFO", f"  Phase 4:       place_entry_order() + manage_exits() ✅")
    log("INFO", f"  Phase 5:       integrated cycle, sizing, EOD liquidation ✅")
    S.equity = get_equity()
    log("INFO", f"  Equity:        ${S.equity:,.2f}")
    log("INFO", f"  Risk/trade:    {RISK_PCT*100}% = ${S.equity * RISK_PCT:.0f}")
    log("INFO", f"  Daily goal:    ${DAILY_TARGET:.0f}")
    log("INFO", f"  Daily max loss:${abs(MAX_LOSS):.0f}")
    log("INFO", f"  Window:        9:35–15:30 ET (all-day VWAP)")
    log("INFO", f"  Time:          {now_et().strftime('%Y-%m-%d %H:%M:%S ET')}")
    log("INFO", "=" * 60)

    try:
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
    except Exception:
        pass

    # Restore persistent state
    try:
        with open(STATUS_FILE) as f:
            saved = json.load(f)
        crd = int(saved.get("consecutive_red_days", 0))
        if crd:
            S.consecutive_red_days = crd
            log("INFO", f"Restored consecutive_red_days={crd}"
                + (" — drawdown sizing ACTIVE (0.5×)" if crd >= 2 else ""))
    except Exception:
        pass

    tg_send(
        f"🟢 *KoiScale v1 Started*\n"
        f"Strategy: VWAP mean reversion (Brian Shannon)\n"
        f"Window:   9:35–15:30 ET\n"
        f"Equity:   ${S.equity:,.2f}\n"
        f"Risk/trade: ${S.equity * RISK_PCT:.0f}\n"
        f"Daily goal / max loss: ${DAILY_TARGET:.0f}\n"
        f"Time: {now_et().strftime('%Y-%m-%d %H:%M:%S ET')}\n"
        + (f"🔵 *DRY-RUN MODE* — no orders will be placed\n" if DRY_RUN else "")
        + (f"🟡 *CHOPPY MODE* — fast exits, half position size\n" if CHOPPY_MODE else "")
        + f"📡 All phases complete — paper trading ready\n"
        f"Send /help for commands")
    S.save()


# ── Cycle loop (runs on daemon thread) ────────────────────────────────────────
def _cycle_loop():
    """
    Trading cycle runs every 8 seconds on its own daemon thread,
    completely decoupled from Telegram polling latency.
    Subscribes any new watchlist symbols to Polygon WS after each cycle.
    """
    while True:
        try:
            cycle()
            S.save()
            # Subscribe WS bars for any symbols on watchlist this cycle
            if S.watchlist:
                ws_subscribe([w.get("symbol", "") for w in S.watchlist if w.get("symbol")])
        except Exception as e:
            log("ERROR", f"Cycle error: {e}")
        time.sleep(8)


# ── Shutdown ───────────────────────────────────────────────────────────────────
def _shutdown(sig, _):
    log("INFO", f"Shutdown signal {sig}")
    tg_send("🔴 KoiScale stopped")
    sys.exit(0)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    # Start dashboard API server in background thread
    t_dash = threading.Thread(target=start_dashboard_server, daemon=True)
    t_dash.start()
    time.sleep(0.5)   # give OS a moment to confirm port is listening

    startup()

    # Alpaca WS disabled — free tier allows only 1 concurrent WS connection per
    # API key, and GreenHebi has priority (bull flag breakouts are more time-sensitive).
    # KoiScale VWAP setups develop over minutes; 8s REST polling is sufficient.
    log("INFO", "Alpaca WS: disabled (shared API key — WS reserved for GreenHebi)")

    # Start trading cycle on its own thread — decoupled from Telegram
    t_cycle = threading.Thread(target=_cycle_loop, daemon=True)
    t_cycle.start()
    log("INFO", "Main loop active — cycle: 8s thread | REST bars | Telegram: long-poll 25s")

    # Main thread: Telegram commands only
    while True:
        try:
            updates = tg_poll(S.offset, 25)
            for u in updates:
                msg = u.get("message", {})
                txt = msg.get("text", "")
                cid = str(msg.get("chat", {}).get("id", ""))
                if txt:
                    handle_cmd(txt, cid)
                S.offset = u["update_id"] + 1

        except KeyboardInterrupt:
            log("INFO", "Keyboard interrupt")
            tg_send("🔴 KoiScale stopped")
            break
        except Exception as e:
            log("ERROR", f"Telegram loop error: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()
