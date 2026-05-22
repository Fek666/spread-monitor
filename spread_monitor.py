"""
MEXC Tokenized Stocks vs Pre-market Spread Monitor
====================================================
Improvements in this version:
  1. Volume display on history chart
  2. Convergence alert (spread returns to near-zero)
  3. 7/30-day average spread on each card
  4. MA line on history chart
  5. Alert markers on history chart
  6. History cache (10 min TTL)
  7. Parallel Yahoo Finance fetching
  8. Watchlist (hide/show tickers via UI)
"""

import asyncio
import json
import logging
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Optional

import aiohttp
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CONFIG = {
    "tickers": [t.strip() for t in os.getenv(
        "TICKERS",
        "MU,CSCO,NVDA,TSLA,INTC,NBIS,SNDK,CRCL,MRVL,GOOGL,MSTR,AMD,DRAM,AAPL,BABA,"
        "HIMS,PLTR,META,MSFT,RKLB,QQQ,STX,ASTS,IREN,COIN,WDC,GE,PANW,INTU,ARM,CVNA,"
        "COHR,AMZN,SPOT,SHOP,CRWV,TXN,TSM,QCOM,UBER,CRWD,VRT,ASML,IONQ,LITE,IBM,ABBV,"
        "LLY,ORCL,JPM,NKE,FIG,AMAT,ONDS,ADBE,VZ,SOXX,NOW,HOOD,SNOW,USAR,MA,JD,CRM,"
        "XOM,C,WFC,GME,BA,PYPL,SBUX,PG,LRCX,SMCI,AVGO,NFLX,V,LMT,OXY,KO,PAYP,WMT,"
        "CVX,RDDT,BAC,GEV,COST,RTX,UNH,PDD,COP,MCD,FUTU,KLAC,CBRS,FLNC"
    ).split(",")],
    "spread_threshold":      float(os.getenv("SPREAD_THRESHOLD", "0.7")),
    "check_interval":        int(os.getenv("CHECK_INTERVAL", "8")),
    "alert_cooldown":        int(os.getenv("ALERT_COOLDOWN", "60")),
    "convergence_threshold": float(os.getenv("CONVERGENCE_THRESHOLD", "0.15")),  # % — spread near zero
    "web_port":              int(os.getenv("WEB_PORT", "8765")),
    "usdt_usd_rate":         float(os.getenv("USDT_USD_RATE", "1.0")),
    "mexc_request_delay":    float(os.getenv("MEXC_REQUEST_DELAY", "1.0")),
    "force_refresh_cooldown":int(os.getenv("FORCE_REFRESH_COOLDOWN", "60")),
    "yahoo_parallel_workers":int(os.getenv("YAHOO_WORKERS", "8")),   # parallel Yahoo fetches
    "history_cache_ttl":     int(os.getenv("HISTORY_CACHE_TTL", "600")),  # seconds
    "stock_cache_ttl":       int(os.getenv("STOCK_CACHE_TTL", "45")),
    "mexc_batch_size":       int(os.getenv("MEXC_BATCH_SIZE", "4")),
    "mexc_batch_pause":      float(os.getenv("MEXC_BATCH_PAUSE", "0.35")),
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("spread_monitor")

# ─────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────
state = {
    "snapshots":   {},
    "alerts":      [],
    "last_update": None,
    "status":      "starting",
    "leverage":    {},
    "spread_history": {},
    "notes":      [],
    "trash":      [],
    "open_columns": [],
}
_alert_timestamps:       dict[str, float] = {}
_prev_spread:            dict[str, float] = {}
_history_cache:          dict[str, dict]  = {}
_watchlist_hidden:       set              = set()
_state_lock = threading.Lock()
PERSIST_PATH = Path(__file__).parent / "dashboard_state.json"

# ─────────────────────────────────────────────
# CUSTOM SYMBOL MAPPING
# ─────────────────────────────────────────────
SYMBOL_MAP: dict[str, list[str]] = {
    "NVDA": ["NVIDIA_USDT",    "NVDASTOCK_USDT"],
    "TSLA": ["TESLA_USDT",     "TSLASTOCK_USDT"],
    "COIN": ["COINBASE_USDT",  "COINSTOCK_USDT"],
    "HOOD": ["ROBINHOOD_USDT", "HOODSTOCK_USDT"],
}

# ─────────────────────────────────────────────
# SPLIT ADJUSTMENTS  (applied to MEXC price)
# ─────────────────────────────────────────────
SPLIT_DIVISOR: dict[str, float] = {
    "NFLX": 10.0,
    "NOW":   5.0,
    "CVNA":  5.0,
}


def _load_persisted_state() -> None:
    if not PERSIST_PATH.exists():
        return
    try:
        raw = json.loads(PERSIST_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("State load error: %s", e)
        return
    state["spread_history"] = raw.get("spread_history", {}) or {}
    state["notes"] = raw.get("notes", []) or []
    state["trash"] = raw.get("trash", []) or []
    hidden = raw.get("hidden", []) or []
    _watchlist_hidden.update(t.upper().strip() for t in hidden if t)


def _save_persisted_state() -> None:
    payload = {
        "spread_history": state["spread_history"],
        "notes": state["notes"],
        "trash": state["trash"],
        "hidden": sorted(_watchlist_hidden),
        "open_columns": state["open_columns"],
    }
    with _state_lock:
        PERSIST_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _add_trash_ticker(ticker: str) -> None:
    ticker = ticker.upper().strip()
    if ticker and ticker not in state["trash"]:
        state["trash"].insert(0, ticker)
        _save_persisted_state()


def _remove_trash_ticker(ticker: str) -> None:
    ticker = ticker.upper().strip()
    state["trash"] = [t for t in state["trash"] if t != ticker]
    _save_persisted_state()


def _upsert_note(note: dict) -> None:
    note_id = note.get("id") or str(int(time.time() * 1000))
    record = {"id": note_id, "title": note.get("title", ""), "body": note.get("body", "")}
    state["notes"] = [n for n in state["notes"] if n.get("id") != note_id]
    state["notes"].insert(0, record)
    _save_persisted_state()


def _delete_note(note_id: str) -> None:
    state["notes"] = [n for n in state["notes"] if n.get("id") != note_id]
    _save_persisted_state()


def _set_open_column(ticker: str, open_: bool) -> None:
    ticker = ticker.upper().strip()
    cols = [t for t in state["open_columns"] if t != ticker]
    if open_ and ticker:
        cols.insert(0, ticker)
    state["open_columns"] = cols[:4]
    _save_persisted_state()


def _record_history(ticker: str, spread: float) -> None:
    hist = state["spread_history"].setdefault(ticker, [])
    hist.append({"t": time.time(), "v": round(spread, 4)})
    cutoff = time.time() - 180 * 86400
    state["spread_history"][ticker] = [h for h in hist if h["t"] > cutoff]
    _save_persisted_state()


def _trend_summary(ticker: str) -> dict:
    hist = state["spread_history"].get(ticker, [])
    if not hist:
        return {"trend": "unknown", "avg": None, "near_zero": 0, "latest": None}
    vals = [h["v"] for h in hist]
    avg = sum(vals) / len(vals)
    latest = vals[-1]
    near_zero = sum(1 for v in vals if abs(v) < 0.1) / len(vals)
    start = vals[0]
    trend = "flat"
    if latest - start > 0.2:
        trend = "widening"
    elif start - latest > 0.2:
        trend = "converging"
    return {"trend": trend, "avg": round(avg, 4), "near_zero": round(near_zero, 3), "latest": round(latest, 4)}


def _get_news(ticker: str) -> list[dict]:
    try:
        items = []
        news = getattr(yf.Ticker(ticker), "news", []) or []
        for item in news[:3]:
            title = item.get("title") or item.get("content", {}).get("title")
            url = item.get("link") or item.get("content", {}).get("canonicalUrl", {}).get("url")
            if title and url:
                items.append({"title": title, "url": url})
        return items[:3]
    except Exception as e:
        log.warning("News %s error: %s", ticker, e)
        return []


_load_persisted_state()

# ══════════════════════════════════════════════════════════════════
# MEXC PRICE
# ══════════════════════════════════════════════════════════════════

async def get_mexc_price(session: aiohttp.ClientSession, ticker: str) -> Optional[float]:
    candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
    for symbol in candidates:
        url = f"https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                raw = await resp.json(content_type=None)
                if not raw.get("success"):
                    continue
                data = raw.get("data", {})
                if isinstance(data, list):
                    data = data[0] if data else {}
                price_raw = (data.get("lastPrice") or data.get("last")
                             or data.get("close") or data.get("price"))
                if not price_raw:
                    continue
                price = float(price_raw)
                if price > 0:
                    divisor = SPLIT_DIVISOR.get(ticker, 1.0)
                    if divisor != 1.0:
                        log.info("%-6s | MEXC raw=%.4f ÷ %.0f (split) = %.4f",
                                 ticker, price, divisor, price / divisor)
                    price = price / divisor
                    log.info("%-6s | MEXC %-20s → $%.4f", ticker, symbol, price)
                    return price * CONFIG["usdt_usd_rate"]
        except asyncio.TimeoutError:
            log.warning("MEXC %s timeout", symbol)
        except Exception as e:
            log.debug("MEXC %s error: %s", symbol, e)
    log.warning("%-6s | MEXC: no price found", ticker)
    return None


# ══════════════════════════════════════════════════════════════════
# MEXC LEVERAGE
# ══════════════════════════════════════════════════════════════════

async def get_mexc_leverage(session: aiohttp.ClientSession, ticker: str) -> Optional[int]:
    candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
    for symbol in candidates:
        url = f"https://contract.mexc.com/api/v1/contract/detail?symbol={symbol}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
                raw = await resp.json(content_type=None)
                if not raw.get("success"):
                    continue
                data = raw.get("data", {})
                if isinstance(data, list):
                    data = data[0] if data else {}
                lev = data.get("maxLeverage") or data.get("leverageLevel")
                if lev is not None:
                    return int(lev)
        except Exception as e:
            log.debug("MEXC leverage %s error: %s", symbol, e)
    return None


# ══════════════════════════════════════════════════════════════════
# MARKET SESSION
# ══════════════════════════════════════════════════════════════════

def get_market_session() -> str:
    import zoneinfo
    try:
        et = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        et = timedelta(hours=-4)
        from datetime import timezone
        et = timezone(et)
    now_et  = datetime.now(et)
    weekday = now_et.weekday()
    hm      = now_et.hour * 60 + now_et.minute
    if weekday >= 5:
        return "post-fri"
    if hm < 240:    return "overnight"
    if hm < 570:    return "premarket"
    if hm < 960:    return "regular"
    if hm < 1200:   return "postmarket"
    return "overnight"


# ══════════════════════════════════════════════════════════════════
# YAHOO FINANCE  (single ticker)
# ══════════════════════════════════════════════════════════════════

def get_stock_price(ticker: str) -> tuple[Optional[float], str]:
    session = get_market_session()
    cached = _stock_cache.get(ticker)
    now = time.time()
    if cached and cached.get("session") == session and now - float(cached.get("ts", 0)) < CONFIG["stock_cache_ttl"]:
        return cached.get("price"), session
    try:
        info = yf.Ticker(ticker).info
        if session == "post-fri":
            price = (info.get("postMarketPrice") or info.get("regularMarketPrice")
                     or info.get("previousClose"))
        elif session == "premarket":
            price = (info.get("preMarketPrice") or info.get("currentPrice")
                     or info.get("regularMarketPrice") or info.get("previousClose"))
        elif session == "regular":
            price = (info.get("currentPrice") or info.get("regularMarketPrice")
                     or info.get("previousClose"))
        elif session == "postmarket":
            price = (info.get("postMarketPrice") or info.get("currentPrice")
                     or info.get("regularMarketPrice") or info.get("previousClose"))
        else:
            price = info.get("regularMarketPrice") or info.get("previousClose")
        if price:
            price = float(price)
            _stock_cache[ticker] = {"price": price, "session": session, "ts": now}
            return price, session
        log.warning("Yahoo %s: no price field", ticker)
    except Exception as e:
        log.warning("Yahoo %s error: %s", ticker, e)
    return None, session


# ══════════════════════════════════════════════════════════════════
# SPREAD
# ══════════════════════════════════════════════════════════════════

def calculate_spread(price_mexc: float, price_stock: float) -> float:
    return ((price_mexc - price_stock) / price_stock) * 100


# ══════════════════════════════════════════════════════════════════
# ALERTING
# ══════════════════════════════════════════════════════════════════

def send_alert(ticker: str, spread: float, price_mexc: float,
               price_stock: float, alert_type: str = "threshold") -> None:
    now = time.time()
    cooldown_key = f"{ticker}:{alert_type}"
    if now - _alert_timestamps.get(cooldown_key, 0) < CONFIG["alert_cooldown"]:
        return
    _alert_timestamps[cooldown_key] = now

    if alert_type == "convergence":
        direction = "CONVERGENCE"
    else:
        direction = "PREMIUM" if spread > 0 else "DISCOUNT"

    alert = {
        "id":          int(now * 1000),
        "time":        datetime.now().strftime("%H:%M:%S"),
        "ticker":      ticker,
        "spread":      round(spread, 3),
        "price_mexc":  round(price_mexc, 4),
        "price_stock": round(price_stock, 4),
        "direction":   direction,
        "type":        alert_type,
        "threshold":   CONFIG["spread_threshold"],
    }
    state["alerts"].insert(0, alert)
    state["alerts"] = state["alerts"][:50]

    emoji = "🎯" if alert_type == "convergence" else "🚨"
    log.warning("%s ALERT %s | %.3f%% (%s) | MEXC=%.4f  Stock=%.4f",
                emoji, ticker, spread, direction, price_mexc, price_stock)


# ══════════════════════════════════════════════════════════════════
# SPREAD HISTORY  (for rolling average)
# ══════════════════════════════════════════════════════════════════

def record_spread(ticker: str, spread: float) -> None:
    _record_history(ticker, spread)


def get_spread_avg(ticker: str, days: int = 7) -> Optional[float]:
    hist = state["spread_history"].get(ticker, [])
    cutoff = time.time() - days * 86400
    vals = [h["v"] for h in hist if h["t"] > cutoff]
    return round(sum(vals) / len(vals), 3) if vals else None


# ══════════════════════════════════════════════════════════════════
# MAIN MONITOR LOOP
# ══════════════════════════════════════════════════════════════════

_force_refresh_event = asyncio.Event()
_yahoo_executor = ThreadPoolExecutor(max_workers=CONFIG["yahoo_parallel_workers"])
_stock_cache: dict[str, dict[str, float | str | None]] = {}


async def monitor_loop() -> None:
    state["status"] = "running"
    log.info("Monitor started | tickers=%d | interval=%ss | threshold=%.2f%%",
             len(CONFIG["tickers"]), CONFIG["check_interval"], CONFIG["spread_threshold"])

    connector = aiohttp.TCPConnector(limit_per_host=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        asyncio.create_task(fetch_all_leverage(session))
        while True:
            _force_refresh_event.clear()
            await run_full_cycle(session)
            try:
                await asyncio.wait_for(_force_refresh_event.wait(),
                                       timeout=CONFIG["check_interval"])
                log.info("Force refresh triggered")
            except asyncio.TimeoutError:
                pass


async def run_full_cycle(session: aiohttp.ClientSession) -> None:
    """
    Yahoo stays parallel; MEXC is fetched in small bounded batches with pauses.
    """
    loop = asyncio.get_event_loop()
    tickers = CONFIG["tickers"]

    yahoo_futures = {
        ticker: loop.run_in_executor(_yahoo_executor, get_stock_price, ticker)
        for ticker in tickers
    }

    mexc_prices: dict[str, Optional[float]] = {}
    batch_size = max(1, CONFIG["mexc_batch_size"])
    batch_pause = max(0.0, CONFIG["mexc_batch_pause"])
    request_delay = max(0.0, CONFIG["mexc_request_delay"])

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        results = await asyncio.gather(*(get_mexc_price(session, ticker) for ticker in batch))
        for ticker, price in zip(batch, results):
            mexc_prices[ticker] = price
        if i + batch_size < len(tickers):
            await asyncio.sleep(batch_pause)
        if request_delay:
            await asyncio.sleep(request_delay)

    for ticker in tickers:
        price_stock, session_label = await yahoo_futures[ticker]
        price_mexc = mexc_prices[ticker]
        _update_snapshot(ticker, price_mexc, price_stock, session_label)

    state["last_update"] = datetime.now().strftime("%H:%M:%S")
    state["status"] = "running"
    _stock_cache.clear()


































def _update_snapshot(ticker: str, price_mexc: Optional[float],
                     price_stock: Optional[float], session_label: str) -> None:
    ticker = ticker.upper().strip()
    snap = {
        "ticker":      ticker,
        "price_mexc":  price_mexc,
        "price_stock": price_stock,
        "spread":      None,
        "avg7":        None,
        "avg30":       None,
        "status":      "ok",
        "time":        datetime.now().strftime("%H:%M:%S"),
        "split_adj":   ticker in SPLIT_DIVISOR,
        "split_div":   SPLIT_DIVISOR.get(ticker, 1.0),
        "session":     session_label,
    }

    if price_mexc is None or price_stock is None:
        snap["status"] = "error"
        snap["error"]  = f"{'MEXC' if price_mexc is None else 'Yahoo'} unavailable"
        log.info("%-6s | data unavailable", ticker)
    else:
        spread = calculate_spread(price_mexc, price_stock)
        snap["spread"] = round(spread, 4)

        record_spread(ticker, spread)
        trend = _trend_summary(ticker)
        snap["trend"] = trend
        snap["avg7"]  = get_spread_avg(ticker, 7)
        snap["avg30"] = get_spread_avg(ticker, 30)

        log.info("%-6s | MEXC=%.4f  Stock=%.4f  spread=%+.3f%%  avg7=%s%%",
                 ticker, price_mexc, price_stock, spread,
                 f"{snap['avg7']:+.3f}" if snap["avg7"] is not None else "N/A")

        # Threshold alert
        if abs(spread) >= CONFIG["spread_threshold"]:
            send_alert(ticker, spread, price_mexc, price_stock, "threshold")

        # Convergence alert: was above threshold, now near zero
        prev = _prev_spread.get(ticker)
        if (prev is not None
                and abs(prev) >= CONFIG["spread_threshold"]
                and abs(spread) <= CONFIG["convergence_threshold"]):
            send_alert(ticker, spread, price_mexc, price_stock, "convergence")

        _prev_spread[ticker] = spread

    state["snapshots"][ticker] = snap


async def fetch_all_leverage(session: aiohttp.ClientSession) -> None:
    log.info("Fetching leverage data for all tickers…")
    for ticker in CONFIG["tickers"]:
        lev = await get_mexc_leverage(session, ticker)
        state["leverage"][ticker] = lev
        await asyncio.sleep(CONFIG["mexc_request_delay"])
    log.info("Leverage data fetched.")


# ══════════════════════════════════════════════════════════════════
# HISTORICAL SPREAD  (Yahoo OHLC + MEXC kline → spread candles)
# ══════════════════════════════════════════════════════════════════

def _fetch_yahoo_ohlc(ticker: str, days: int) -> dict:
    end   = datetime.utcnow()
    start = end - timedelta(days=days + 5)
    try:
        t  = yf.Ticker(ticker)
        df = t.history(start=start.strftime("%Y-%m-%d"),
                       end=end.strftime("%Y-%m-%d"),
                       interval="1d", auto_adjust=True)
        if df.empty:
            return {}
        df.columns = [str(c).strip().title() for c in df.columns]
        result = {}
        for ts, row in df.iterrows():
            date = ts.strftime("%Y-%m-%d")
            try:
                result[date] = {
                    "o": float(row["Open"]),
                    "h": float(row["High"]),
                    "l": float(row["Low"]),
                    "c": float(row["Close"]),
                }
            except Exception:
                continue
        log.info("Yahoo OHLC %s → %d candles", ticker, len(result))
        return result
    except Exception as e:
        log.warning("Yahoo OHLC %s error: %s", ticker, e)
        return {}


def _fetch_yahoo_hourly_ohlc(ticker: str, days: int) -> dict:
    end = datetime.utcnow()
    start = end - timedelta(days=days + 2)
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start.strftime("%Y-%m-%d"),
                       end=end.strftime("%Y-%m-%d"),
                       interval="1h", auto_adjust=True, prepost=True)
        if df.empty:
            return {}
        df.columns = [str(c).strip().title() for c in df.columns]
        result = {}
        for ts, row in df.iterrows():
            key = ts.strftime("%Y-%m-%d %H:00")
            try:
                result[key] = {
                    "o": float(row["Open"]),
                    "h": float(row["High"]),
                    "l": float(row["Low"]),
                    "c": float(row["Close"]),
                }
            except Exception:
                continue
        log.info("Yahoo hourly %s → %d candles", ticker, len(result))
        return result
    except Exception as e:
        log.warning("Yahoo hourly %s error: %s", ticker, e)
        return {}


async def _fetch_mexc_kline(session: aiohttp.ClientSession,
                             ticker: str, days: int) -> dict:
    candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
    divisor = SPLIT_DIVISOR.get(ticker, 1.0)
    limit   = days + 10

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.mexc.com/",
    }

    for symbol in candidates:
        url = (f"https://contract.mexc.com/api/v1/contract/kline/{symbol}"
               f"?interval=Day1&limit={limit}")
        try:
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.json(content_type=None)
                if not raw.get("success"):
                    continue
                data = raw.get("data", {})
                if not isinstance(data, dict):
                    continue
                times  = data.get("time",  [])
                opens  = data.get("open",  [])
                highs  = data.get("high",  [])
                lows   = data.get("low",   [])
                closes = data.get("close", [])
                vols   = data.get("vol",   [])
                if not times:
                    continue
                result = {}
                for i, ts in enumerate(times):
                    try:
                        ts_int = int(ts)
                        if ts_int > 1e12:
                            ts_int //= 1000
                        date = datetime.utcfromtimestamp(ts_int).strftime("%Y-%m-%d")
                        result[date] = {
                            "o": float(opens[i])  / divisor,
                            "h": float(highs[i])  / divisor,
                            "l": float(lows[i])   / divisor,
                            "c": float(closes[i]) / divisor,
                            "v": float(vols[i]) if i < len(vols) else 0,
                        }
                    except Exception:
                        continue
                if result:
                    log.info("MEXC kline %s → %d candles", symbol, len(result))
                    return result
        except Exception as e:
            log.warning("MEXC kline %s error: %s", symbol, e)
    return {}


async def _fetch_mexc_kline_sync(ticker: str, days: int) -> dict:
    connector = aiohttp.TCPConnector(limit_per_host=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        return await _fetch_mexc_kline(session, ticker, days)


async def _fetch_mexc_hourly_kline(session: aiohttp.ClientSession, ticker: str, days: int) -> dict:
    candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
    divisor = SPLIT_DIVISOR.get(ticker, 1.0)
    limit = days * 24 + 48

    for symbol in candidates:
        url = f"https://contract.mexc.com/api/v1/contract/kline/{symbol}?interval=Hour1&limit={limit}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.json(content_type=None)
                if not raw.get("success"):
                    continue
                data = raw.get("data", {})
                if not isinstance(data, dict):
                    continue
                times  = data.get("time", [])
                opens  = data.get("open", [])
                highs  = data.get("high", [])
                lows   = data.get("low", [])
                closes = data.get("close", [])
                if not times:
                    continue
                result = {}
                for i, ts in enumerate(times):
                    try:
                        ts_int = int(ts)
                        if ts_int > 1e12:
                            ts_int //= 1000
                        dt = datetime.utcfromtimestamp(ts_int)
                        key = dt.strftime("%Y-%m-%d %H:00")
                        result[key] = {
                            "o": float(opens[i]) / divisor,
                            "h": float(highs[i]) / divisor,
                            "l": float(lows[i]) / divisor,
                            "c": float(closes[i]) / divisor,
                        }
                    except Exception:
                        continue
                if result:
                    return result
        except Exception as e:
            log.warning("MEXC hourly kline %s error: %s", symbol, e)
    return {}


def _build_spread_candles(yahoo: dict, mexc: dict, days: int) -> list:
    common = sorted(set(yahoo.keys()) & set(mexc.keys()))[-days:]
    candles = []

    for date in common:
        y = yahoo[date]
        m = mexc[date]
        if y["c"] <= 0 or m["c"] <= 0:
            continue
        s_o = (m["o"] / y["o"] * 100) - 100 if y["o"] > 0 else None
        s_h = (m["h"] / y["l"] * 100) - 100 if y["l"] > 0 else None
        s_l = (m["l"] / y["h"] * 100) - 100 if y["h"] > 0 else None
        s_c = (m["c"] / y["c"] * 100) - 100
        if None in (s_o, s_h, s_l):
            continue
        candles.append({
            "date": date,
            "o":    round(s_o, 4),
            "h":    round(s_h, 4),
            "l":    round(s_l, 4),
            "c":    round(s_c, 4),
            "v":    round(m.get("v", 0), 0),
        })

    closes = [c["c"] for c in candles]
    for i, candle in enumerate(candles):
        window = closes[max(0, i - 19): i + 1]
        candle["ma20"] = round(sum(window) / len(window), 4)

    return candles


def _get_history(ticker: str, days: int) -> list:
    cache_key = f"{ticker}:{days}"
    cached = _history_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < CONFIG["history_cache_ttl"]:
        log.info("History cache HIT %s %dd", ticker, days)
        return cached["data"]

    import concurrent.futures
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            mexc_data = loop.run_until_complete(_fetch_mexc_kline_sync(ticker, days))
        finally:
            loop.close()
        yahoo_data = _fetch_yahoo_ohlc(ticker, days)
        return _build_spread_candles(yahoo_data, mexc_data, days)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        candles = ex.submit(_run).result(timeout=30)

    _history_cache[cache_key] = {"data": candles, "ts": time.time()}
    return candles


def _get_hourly_history(ticker: str) -> list:
    days = 30
    cache_key = f"{ticker}:hourly:{days}"
    cached = _history_cache.get(cache_key)
    if cached and time.time() - cached["ts"] < CONFIG["history_cache_ttl"]:
        return cached["data"]

    async def _run():
        connector = aiohttp.TCPConnector(limit_per_host=1)
        async with aiohttp.ClientSession(connector=connector) as session:
            mexc_task = _fetch_mexc_hourly_kline(session, ticker, days)
            return await asyncio.gather(asyncio.to_thread(_fetch_yahoo_hourly_ohlc, ticker, days), mexc_task)

    yahoo_data, mexc_data = asyncio.run(_run())
    candles = _build_spread_candles(yahoo_data, mexc_data, days)
    _history_cache[cache_key] = {"data": candles, "ts": time.time()}
    return candles


# ══════════════════════════════════════════════════════════════════
# HTTP SERVER
# ══════════════════════════════════════════════════════════════════

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"
_last_force_refresh: float = 0.0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        global _last_force_refresh

        if self.path == "/api/state":
            body = json.dumps({
                "snapshots":          state["snapshots"],
                "alerts":             state["alerts"],
                "last_update":        state["last_update"],
                "status":             state["status"],
                "leverage":           state["leverage"],
                "watchlist_hidden":   list(_watchlist_hidden),
        "trash":             state["trash"],
        "notes":             state["notes"],
        "open_columns":      state["open_columns"],
        "config": {
                    "tickers":                CONFIG["tickers"],
                    "threshold":              CONFIG["spread_threshold"],
                    "convergence_threshold":  CONFIG["convergence_threshold"],
                    "interval":               CONFIG["check_interval"],
                    "force_refresh_cooldown": CONFIG["force_refresh_cooldown"],
                },
                "last_force_refresh": _last_force_refresh,
            }).encode()
            self._respond(200, "application/json", body)

        elif self.path == "/api/force_refresh":
            now      = time.time()
            elapsed  = now - _last_force_refresh
            cooldown = CONFIG["force_refresh_cooldown"]
            if elapsed < cooldown:
                body = json.dumps({"ok": False, "remaining": int(cooldown - elapsed)}).encode()
            else:
                _last_force_refresh = now
                _force_refresh_event.set()
                body = json.dumps({"ok": True}).encode()
            self._respond(200, "application/json", body)

        elif self.path.startswith("/api/watchlist"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            if self.command == "GET":
                ticker = qs.get("ticker", [""])[0].upper().strip()
                action = qs.get("action", ["toggle"])[0]
                if ticker:
                    if action == "hide":
                        _watchlist_hidden.add(ticker)
                        _add_trash_ticker(ticker)
                    elif action == "show":
                        _watchlist_hidden.discard(ticker)
                        _remove_trash_ticker(ticker)
                    else:
                        if ticker in _watchlist_hidden:
                            _watchlist_hidden.discard(ticker)
                            _remove_trash_ticker(ticker)
                        else:
                            _watchlist_hidden.add(ticker)
                            _add_trash_ticker(ticker)
            elif self.command == "POST":
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                hidden = payload.get("hidden", [])
                _watchlist_hidden.clear()
                _watchlist_hidden.update(t.upper().strip() for t in hidden if t)
                state["trash"] = list(dict.fromkeys([t.upper().strip() for t in payload.get("trash", state["trash"]) if t]))
                state["open_columns"] = list(dict.fromkeys([t.upper().strip() for t in payload.get("open_columns", state["open_columns"]) if t]))
                _save_persisted_state()
            body = json.dumps({"hidden": list(_watchlist_hidden), "trash": state["trash"], "open_columns": state["open_columns"]}).encode()
            self._respond(200, "application/json", body)

        elif self.path.startswith("/api/history"):
            from urllib.parse import urlparse, parse_qs
            qs     = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0].upper().strip()
            days   = max(7, min(int(qs.get("days", ["30"])[0]), 180))
            if not ticker:
                self.send_response(400); self.end_headers(); return
            try:
                candles = _get_history(ticker, days)
                body = json.dumps({"ok": True, "ticker": ticker,
                                   "days": days, "candles": candles}).encode()
            except Exception as e:
                log.warning("history %s error: %s", ticker, e)
                body = json.dumps({"ok": False, "error": str(e)}).encode()
            self._respond(200, "application/json", body)

        elif self.path.startswith("/api/history_hourly"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0].upper().strip()
            if not ticker:
                self.send_response(400); self.end_headers(); return
            try:
                candles = _get_hourly_history(ticker)
                body = json.dumps({"ok": True, "ticker": ticker, "candles": candles}).encode()
            except Exception as e:
                log.warning("history hourly %s error: %s", ticker, e)
                body = json.dumps({"ok": False, "error": str(e)}).encode()
            self._respond(200, "application/json", body)

        elif self.path.startswith("/api/news"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0].upper().strip()
            body = json.dumps({"ok": True, "ticker": ticker, "items": _get_news(ticker)}).encode()
            self._respond(200, "application/json", body)

        elif self.path.startswith("/api/notes"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            action = qs.get("action", ["list"])[0]
            if action == "delete":
                _delete_note(qs.get("id", [""])[0])
            elif action == "save":
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                _upsert_note(payload)
            body = json.dumps({"ok": True, "notes": state["notes"]}).encode()
            self._respond(200, "application/json", body)

        elif self.path.startswith("/api/open_columns"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0].upper().strip()
            action = qs.get("action", ["open"])[0]
            _set_open_column(ticker, action != "close")
            body = json.dumps({"ok": True, "open_columns": state["open_columns"]}).encode()
            self._respond(200, "application/json", body)

        elif self.path in ("/", "/dashboard"):
            content = DASHBOARD_HTML.read_bytes()
            self._respond(200, "text/html; charset=utf-8", content)

        else:
            self.send_response(404); self.end_headers()

    def _respond(self, code: int, content_type: str, body: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def start_http_server() -> None:
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer(("127.0.0.1", CONFIG["web_port"]), Handler)
    log.info("Dashboard → http://localhost:%s", CONFIG["web_port"])
    server.serve_forever()


# ══════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════

def main():
    threading.Thread(target=start_http_server, daemon=True).start()

    def _open():
        time.sleep(1.5)
        webbrowser.open(f"http://localhost:{CONFIG['web_port']}")
    threading.Thread(target=_open, daemon=True).start()

    try:
        asyncio.run(monitor_loop())
    except KeyboardInterrupt:
        log.info("Stopped.")


async def _probe_ticker(ticker: str) -> None:
    logging.getLogger().setLevel(logging.DEBUG)
    print(f"\n{'='*60}\n  PROBE — {ticker}\n{'='*60}\n")
    candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
    async with aiohttp.ClientSession() as session:
        for symbol in candidates:
            url = f"https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}"
            print(f"GET {url}")
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    raw = await r.text()
                    print(f"  Status: {r.status}")
                    try:
                        import json as _j
                        print(f"  Body: {_j.dumps(_j.loads(raw), indent=2)[:400]}")
                    except Exception:
                        print(f"  Body: {raw[:300]}")
            except Exception as e:
                print(f"  ERROR: {e}")
            print()
    price, sess = get_stock_price(ticker)
    print(f"Yahoo [{sess}] → {ticker}: {price}")
    div = SPLIT_DIVISOR.get(ticker, 1.0)
    if div != 1.0:
        print(f"  (MEXC split ÷{div})")


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3 and sys.argv[1] == "--probe":
        asyncio.run(_probe_ticker(sys.argv[2].upper()))
    elif len(sys.argv) >= 2 and sys.argv[1] == "--debug":
        logging.getLogger().setLevel(logging.DEBUG)
        main()
    else:
        main()
