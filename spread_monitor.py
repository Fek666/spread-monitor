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
import websockets
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
        "AAL,AAOI,AAPL,ADBE,ALAB,AMAT,AMD,AMZN,APLD,APP,ARM,ASML,ASTS,AVGO,AXP,"
        "AXTI,BA,BABA,BAC,BB,BE,BLK,BRK-B,C,CBRS,CCL,CMCSA,COHR,COIN,COP,"
        "COST,CRCL,CRM,CRWD,CRWV,CSCO,CVX,DIS,DRAM,EWJ,EWT,EWY,FCX,FLNC,FOXA,"
        "FUTU,GE,GEV,GLW,GME,GOOGL,HD,HIMS,HOOD,HPE,IBM,INFQ,INTC,INTU,IONQ,"
        "IREN,ISRG,JD,JPM,KLAC,KORU,LITE,LLY,LMT,LRCX,LUNR,MA,MAR,MCD,META,"
        "MRVL,MSFT,MSTR,MU,NBIS,NEM,NKE,NOK,NVDA,NVO,OKLO,ONDS,ORCL,OXY,"
        "PANW,PAYP,PDD,PLTR,POET,PYPL,QBTS,QCOM,QNT,QQQ,RDDT,RDW,RGTI,RKLB,SHLD,"
        "SHOP,SLB,SMCI,SNDK,SNOW,SONY,SPOT,SOXX,STX,TEM,TER,TSLA,TSM,TXN,UBER,"
        "UNH,UPST,USAR,V,VRT,VST,WDC,WMT,XLE,XOM,"
      
 
    ).split(",")],
    "spread_threshold":      float(os.getenv("SPREAD_THRESHOLD", "0.7")),
    "check_interval":        int(os.getenv("CHECK_INTERVAL", "15")),
    "alert_cooldown":        int(os.getenv("ALERT_COOLDOWN", "60")),
    "convergence_threshold": float(os.getenv("CONVERGENCE_THRESHOLD", "0.15")),  # % — spread near zero
    "web_port":              int(os.getenv("WEB_PORT", "8765")),
    "usdt_usd_rate":         float(os.getenv("USDT_USD_RATE", "1.0")),
    "mexc_request_delay":    float(os.getenv("MEXC_REQUEST_DELAY", "0.0")),
    "force_refresh_cooldown":int(os.getenv("FORCE_REFRESH_COOLDOWN", "60")),
    "yahoo_parallel_workers":int(os.getenv("YAHOO_WORKERS", "1")),   # parallel Yahoo fetches
    "history_cache_ttl":     int(os.getenv("HISTORY_CACHE_TTL", "600")),  # seconds
    "stock_cache_ttl":       int(os.getenv("STOCK_CACHE_TTL", "15")),
    "mexc_batch_size":       int(os.getenv("MEXC_BATCH_SIZE", "8")),
    "mexc_batch_pause":      float(os.getenv("MEXC_BATCH_PAUSE", "0.5")),
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

# Telegram
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
# Порог изменения спреда для уведомления (в %)
SPREAD_JUMP_THRESHOLD = float(os.getenv("SPREAD_JUMP_THRESHOLD", "1.0"))

# ─────────────────────────────────────────────
# CUSTOM SYMBOL MAPPING
# ─────────────────────────────────────────────
SYMBOL_MAP: dict[str, list[str]] = {
    "SPCX":      ["SPCXSTOCK_USDT",      "SPCX_USDT"],
    "AAOI":      ["AAOISTOCK_USDT",      "AAOI_USDT"],
    "NAS100":    ["NAS100_USDT",         "NAS100STOCK_USDT"],
    "HPE":       ["HPESTOCK_USDT",       "HPE_USDT"],
    "ANTHROPIC": ["ANTHROPICSTOCK_USDT", "ANTHROPIC_USDT"],
    "OPENAI":    ["OPENAISTOCK_USDT",    "OPENAI_USDT"],
    "JP225":     ["JP225_USDT",          "JP225STOCK_USDT"],
    # QNT: крипто-токен QNT_USDT существует → ТОЛЬКО акционный символ
    "QNT":       ["QNTSTOCK_USDT"],
    "000660.KS": ["SKHYNIXSTOCK_USDT",   "SKHYNIX_USDT"],
    "BBSTOCK":   ["BBSTOCK_USDT"],
    "RDW":       ["RDWSTOCK_USDT",       "RDW_USDT"],
    # NOK: сначала акционный символ, чтобы не поймать крипто NOK
    "NOK":       ["NOKSTOCK_USDT",       "NOK_USDT"],
    "EWT":       ["EWTSTOCK_USDT",       "EWT_USDT"],
    "005930.KS": ["SAMSUNGSTOCK_USDT",   "SAMSUNG_USDT"],
    "LUNR":      ["LUNRSTOCK_USDT",      "LUNR_USDT"],
    "APP":       ["APPSTOCK_USDT",       "APP_USDT"],
    "BRK-B":     ["BRKBSTOCK_USDT",      "BRKB_USDT"],
    "APLD":      ["APLDSTOCK_USDT",      "APLD_USDT"],
    "SHLD":      ["SHLDSTOCK_USDT",      "SHLD_USDT"],
    "INFQ":      ["INFQSTOCK_USDT",      "INFQ_USDT"],
    "HK50":      ["HK50_USDT",           "HK50STOCK_USDT"],
    "EWJ":       ["EWJSTOCK_USDT",       "EWJ_USDT"],
    "EWY":       ["EWYSTOCK_USDT",       "EWY_USDT"],
    "HD":        ["HDSTOCK_USDT",        "HD_USDT"],
    "DIS":       ["DISSTOCK_USDT",       "DIS_USDT"],
    "GLW":       ["GLWSTOCK_USDT",       "GLW_USDT"],
    "BE":        ["BESTOCK_USDT",        "BE_USDT"],
    "XLE":       ["XLESTOCK_USDT",       "XLE_USDT"],
    "NVO":       ["NVOSTOCK_USDT",       "NVO_USDT"],
    "005380.KS": ["HYUNDAISTOCK_USDT",   "HYUNDAI_USDT"],
    "US30":      ["US30_USDT",           "US30STOCK_USDT"],
    # STX: STX_USDT — крипто Stacks (~$0.20), акция ~$997 → ТОЛЬКО акционный
    "STX":       ["STXSTOCK_USDT"],
    # NVDA: NVIDIA_USDT не существует на фьючерсах → правильный символ первым
    "NVDA":      ["NVDASTOCK_USDT",      "NVIDIA_USDT"],
    # TSLA: добавлен фолбэк
    "TSLA":      ["TESLA_USDT",          "TSLASTOCK_USDT"],
    # COIN: COINBASE_USDT неверное имя → правильный первым
    "COIN":      ["COINSTOCK_USDT",      "COINBASE_USDT"],
    "QQQ":       ["QQQSTOCK_USDT"],
    # HOOD: ROBINHOOD_USDT неверное имя → правильный первым
    "HOOD":      ["HOODSTOCK_USDT",      "ROBINHOOD_USDT"],
    # BB: BB_USDT — крипто-токен (~$0.02), даёт спред 99% → ТОЛЬКО акционный
    "BB":        ["BBSTOCK_USDT"],
    # CVX: CVX_USDT — Convex Finance (~$3), Chevron ~$155 → спред 99% → ТОЛЬКО акционный
    "CVX":       ["CVXSTOCK_USDT"],
    "C":         ["CSTOCK_USDT"],
    "AAPL":      ["AAPLSTOCK_USDT"],
    "BABA":      ["BABASTOCK_USDT"],
    # AXTI: AXTI_USDT не существует как крипто-токен → убираем фолбэк
    "AXTI":      ["AXTISTOCK_USDT"],
    # Тикеры без маппинга — добавляем правильные имена
    "PANW":      ["PANWSTOCK_USDT"],
    "INTU":      ["INTUSTOCK_USDT"],
    "ARM":       ["ARMSTOCK_USDT"],
    "IBM":       ["IBMSTOCK_USDT"],
    "LITE":      ["LITESTOCK_USDT"],
    "GLW":       ["GLWSTOCK_USDT"],
    "CRM":       ["CRMSTOCK_USDT"],
    "XOM":       ["XOMSTOCK_USDT"],
    "COST":      ["COSTSTOCK_USDT"],
    "INFQ":      ["INFQSTOCK_USDT"],
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


_last_state_save: float = 0.0

def _record_history(ticker: str, spread: float) -> None:
    global _last_state_save
    now = time.time()
    hist = state["spread_history"].setdefault(ticker, [])
    hist.append({"t": now, "v": round(spread, 4)})
    cutoff = now - 180 * 86400
    state["spread_history"][ticker] = [h for h in hist if h["t"] > cutoff]
    if now - _last_state_save >= 300:
        _last_state_save = now
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
# MEXC WEBSOCKET  — реалтайм цены через одно соединение
# ══════════════════════════════════════════════════════════════════

# ticker → текущая цена с MEXC (обновляется WebSocket-ом)
_mexc_prices: dict[str, float] = {}
# ticker → MEXC symbol (заполняется при старте через HTTP once)
_ticker_to_symbol: dict[str, str] = {}


async def _resolve_mexc_symbols(session: aiohttp.ClientSession) -> None:
    """Один раз при старте — определяем реальный символ на MEXC для каждого тикера."""
    log.info("Resolving MEXC symbols for %d tickers...", len(CONFIG["tickers"]))

    for ticker in CONFIG["tickers"]:
        candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
        found = False
        for symbol in candidates:
            url = f"https://contract.mexc.com/api/v1/contract/ticker?symbol={symbol}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
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
                    price = float(price_raw) / SPLIT_DIVISOR.get(ticker, 1.0)
                    if price <= 0:
                        continue
                    _ticker_to_symbol[ticker] = symbol
                    _mexc_prices[ticker] = price * CONFIG["usdt_usd_rate"]
                    log.info("%-6s → %s ($%.4f)", ticker, symbol, price)
                    found = True
                    break
            except Exception as e:
                log.debug("probe %s %s: %s", ticker, symbol, e)
            await asyncio.sleep(0.1)  # пауза между кандидатами

        if not found:
            log.warning("%-6s | no MEXC symbol found", ticker)

        await asyncio.sleep(0.15)  # пауза между тикерами

    log.info("Resolved %d/%d symbols", len(_ticker_to_symbol), len(CONFIG["tickers"]))


# symbol → ticker (обратный маппинг для WebSocket)
_symbol_to_ticker: dict[str, str] = {}


async def _mexc_websocket_loop() -> None:
    """Постоянное WebSocket соединение к MEXC — пушит обновления цен."""
    WS_URL = "wss://contract.mexc.com/edge"

    while True:
        if not _ticker_to_symbol:
            await asyncio.sleep(1)
            continue

        # Строим обратный маппинг symbol → ticker
        _symbol_to_ticker.clear()
        for ticker, symbol in _ticker_to_symbol.items():
            _symbol_to_ticker[symbol] = ticker

        symbols = list(_ticker_to_symbol.values())
        log.info("WebSocket: connecting, subscribing to %d symbols...", len(symbols))

        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                # MEXC sub.ticker принимает ОДИН символ за раз — батч-массив не поддерживается
                # и при попытке батча подписка может тихо не сработать (старая цена зависает навечно)
                for symbol in symbols:
                    sub_msg = json.dumps({
                        "method": "sub.ticker",
                        "param": {"symbol": symbol},
                    })
                    await ws.send(sub_msg)
                    await asyncio.sleep(0.03)

                log.info("WebSocket: subscribed, receiving prices...")

                last_seen: dict[str, float] = {}  # ticker → timestamp последнего обновления

                async def _staleness_watchdog():
                    """Если цена не обновлялась >60с — считаем её протухшей и убираем."""
                    while True:
                        await asyncio.sleep(30)
                        now = time.time()
                        for ticker in list(_mexc_prices.keys()):
                            ts = last_seen.get(ticker, 0)
                            if now - ts > 60:
                                log.warning("%-6s | MEXC price stale (no WS update >60s) — clearing", ticker)
                                _mexc_prices.pop(ticker, None)

                watchdog_task = asyncio.create_task(_staleness_watchdog())

                try:
                    async for raw_msg in ws:
                        try:
                            msg = json.loads(raw_msg)
                        except Exception:
                            continue

                        channel = msg.get("channel", "")
                        data    = msg.get("data", {})

                        if channel != "push.ticker" or not data:
                            continue

                        symbol    = data.get("symbol", "")
                        price_raw = (data.get("lastPrice") or data.get("last")
                                     or data.get("close") or data.get("price"))

                        if not symbol or not price_raw:
                            continue

                        ticker = _symbol_to_ticker.get(symbol)
                        if not ticker:
                            continue

                        price = float(price_raw) / SPLIT_DIVISOR.get(ticker, 1.0)
                        if price <= 0:
                            continue

                        price *= CONFIG["usdt_usd_rate"]

                        # Проверка коллизии крипто/акция
                        stock_price = _stock_cache.get(ticker, {}).get("price")
                        if stock_price and stock_price > 0:
                            if price / stock_price > 5 or stock_price / price > 5:
                                log.warning("%-6s price mismatch WS=%.4f Stock=%.4f — skipping",
                                            ticker, price, stock_price)
                                continue

                        _mexc_prices[ticker] = price
                        last_seen[ticker] = time.time()
                finally:
                    watchdog_task.cancel()

        except Exception as e:
            log.warning("WebSocket error: %s — reconnecting in 3s", e)
            await asyncio.sleep(3)


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

def fetch_all_yahoo_prices(tickers: list) -> dict:
    """Батч-запрос Yahoo батчами по 50. prepost=True для всех сессий."""
    session_label = get_market_session()
    now_ts = time.time()
    results = {}
    need = []
    for t in tickers:
        c = _stock_cache.get(t)
        if c and now_ts - float(c.get("ts", 0)) < CONFIG["stock_cache_ttl"]:
            results[t] = (c["price"], session_label)
        else:
            need.append(t)
    if not need:
        return results
    import time as _t
    t0 = _t.time()
    for i in range(0, len(need), 50):
        batch = need[i:i + 50]
        try:
            df = yf.download(
                tickers=" ".join(batch),
                period="2d", interval="1m",
                prepost=True, group_by="ticker",
                auto_adjust=True, progress=False, threads=True,
            )
            for t in batch:
                try:
                    sub = df[t] if len(batch) > 1 else df
                    valid = sub.dropna(subset=["Close"])
                    if valid.empty:
                        results[t] = (None, session_label)
                        continue
                    price = float(valid["Close"].iloc[-1])
                    if price > 0:
                        _stock_cache[t] = {"price": price, "ts": now_ts}
                        results[t] = (price, session_label)
                    else:
                        results[t] = (None, session_label)
                except Exception:
                    results[t] = (None, session_label)
        except Exception as e:
            log.warning("Yahoo batch error: %s", e)
            for t in batch:
                if t not in results:
                    results[t] = (None, session_label)
    ok = sum(1 for v in results.values() if v[0] is not None)
    log.info("Yahoo batch: %d/%d tickers in %.1fs", ok, len(need), _t.time() - t0)
    for t in need:
        if t not in results:
            results[t] = (None, session_label)
    return results


def get_stock_price(ticker: str) -> tuple:
    """Одиночный запрос — фолбэк для probe."""
    cached = _stock_cache.get(ticker)
    now = time.time()
    sess = get_market_session()
    if cached and now - float(cached.get("ts", 0)) < CONFIG["stock_cache_ttl"]:
        return cached.get("price"), sess
    res = fetch_all_yahoo_prices([ticker])
    return res.get(ticker, (None, sess))


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
# TELEGRAM
# ══════════════════════════════════════════════════════════════════

async def tg_send(text: str, chat_id: str = "") -> int:
    """Отправить сообщение. Возвращает message_id."""
    token = TG_TOKEN
    cid   = chat_id or TG_CHAT_ID
    if not token or not cid:
        return 0
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": cid, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as s:
            resp = await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
            data = await resp.json()
            if not data.get("ok"):
                log.warning("Telegram API error: %s", data.get("description", data))
                return 0
            return data.get("result", {}).get("message_id", 0)
    except Exception as e:
        log.warning("Telegram send error: %s", e)
    return 0


async def tg_edit(message_id: int, text: str, chat_id: str = "") -> None:
    """Редактировать существующее сообщение."""
    token = TG_TOKEN
    cid   = chat_id or TG_CHAT_ID
    if not token or not cid or not message_id:
        return
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    payload = {"chat_id": cid, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        log.warning("Telegram edit error: %s", e)


def _format_alert_text(ticker: str, spread: float, avg30,
                        price_mexc: float, price_stock: float,
                        closed: bool = False, avg7=None) -> str:
    circle  = "🔴" if spread > 0 else "🟢"
    action  = "ШОРТ на MEXC" if spread > 0 else "ЛОНГ на MEXC"
    avg7_str  = f"{avg7:+.3f}%" if avg7 is not None else "N/A"
    avg30_str = f"{avg30:+.3f}%" if avg30 is not None else "N/A"

    if closed:
        return (
            f"✅ <b>{ticker}</b> — спред вернулся в норму\n"
            f"Текущий: <code>{spread:+.3f}%</code>  "
            f"avg30: <code>{avg30_str}</code>"
        )
    return (
        f"{circle} <b>{ticker}</b>  —  {action}\n"
        f"├ Спред:  <code>{spread:+.3f}%</code>\n"
        f"├ MEXC:   <code>${price_mexc:.4f}</code>\n"
        f"├ Stock:  <code>${price_stock:.4f}</code>\n"
        f"├ 7d avg: <code>{avg7_str}</code>\n"
        f"└ 30d avg:<code>{avg30_str}</code>"
    )

# ticker → {msg_id, last_sent_spread}
_tg_alert_state: dict = {}
ALERT_THRESHOLD = float(os.getenv("SPREAD_JUMP_THRESHOLD", "1.0"))
ALERT_STEP      = float(os.getenv("ALERT_STEP", "0.5"))


def _log_task_exception(task: asyncio.Task) -> None:
    """Fire-and-forget таски молча проглатывают исключения — surfacing их в лог."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        log.error("Background task failed: %s: %s", type(exc).__name__, exc)


async def tg_check_spread_alert(ticker: str, spread: float, avg30,
                                  price_mexc: float, price_stock: float,
                                  avg7=None) -> None:
    if avg30 is None:
        log.debug("%-6s | TG check skipped: avg30 not ready yet", ticker)
        return
    deviation  = abs(spread - avg30)
    alert_st   = _tg_alert_state.get(ticker)

    if deviation >= ALERT_THRESHOLD:
        if alert_st is None:
            text   = _format_alert_text(ticker, spread, avg30, price_mexc, price_stock, avg7=avg7)
            msg_id = await tg_send(text)
            _tg_alert_state[ticker] = {"msg_id": msg_id, "last_sent_spread": spread}
            log.info("TG alert: %s spread=%+.3f avg30=%+.3f", ticker, spread, avg30)
        else:
            last = alert_st["last_sent_spread"]
            if abs(spread - last) >= ALERT_STEP:
                text   = _format_alert_text(ticker, spread, avg30, price_mexc, price_stock, avg7=avg7)
                msg_id = await tg_send(text)
                _tg_alert_state[ticker] = {"msg_id": msg_id, "last_sent_spread": spread}
                log.info("TG escalated: %s spread=%+.3f", ticker, spread)
    else:
        if alert_st is not None:
            if alert_st.get("msg_id"):
                text = _format_alert_text(ticker, spread, avg30, price_mexc, price_stock,
                                           closed=True, avg7=avg7)
                await tg_edit(alert_st["msg_id"], text)
                log.info("TG closed: %s spread=%+.3f", ticker, spread)
            del _tg_alert_state[ticker]


async def tg_send_top10(chat_id: str = "") -> None:
    candidates = []
    for t, s in state["snapshots"].items():
        if s.get("spread") is None or s.get("status") == "error":
            continue
        spread = s["spread"]
        avg30  = s.get("avg30")
        avg7   = s.get("avg7")
        price_mexc  = s.get("price_mexc", 0)
        price_stock = s.get("price_stock", 0)
        candidates.append((t, spread, avg30, avg7, price_mexc, price_stock))

    if not candidates:
        await tg_send("Нет данных.", chat_id)
        return

    candidates.sort(key=lambda x: abs(x[1]), reverse=True)
    top = candidates[:10]

    lines = ["📊 <b>Топ-10 спредов MEXC vs Stock</b>"]
    for i, (ticker, spread, avg30, avg7, p_mexc, p_stock) in enumerate(top, 1):
        circle    = "🔴" if spread > 0 else "🟢"
        action    = "шорт" if spread > 0 else "лонг"
        avg7_str  = f"{avg7:+.3f}%"  if avg7  is not None else "N/A"
        avg30_str = f"{avg30:+.3f}%" if avg30 is not None else "N/A"
        lines.append(
            f"\n{i}. {circle} <b>{ticker}</b>  {spread:+.3f}%  ({action})\n"
            f"   MEXC <code>${p_mexc:.4f}</code>  ·  Stock <code>${p_stock:.4f}</code>\n"
            f"   7d avg: <code>{avg7_str}</code>  |  30d avg: <code>{avg30_str}</code>\n"
            f"   ─────────────────────"
        )

    await tg_send("\n".join(lines), chat_id)


async def tg_bot_polling() -> None:
    if not TG_TOKEN:
        return
    offset = 0
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    log.info("Telegram bot polling started")
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                resp = await s.get(
                    url,
                    params={"offset": offset, "timeout": 30, "allowed_updates": '["message"]'},
                    timeout=aiohttp.ClientTimeout(total=35),
                )
                data = await resp.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip().lower()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if text in ("/chart", "/chart@" + TG_TOKEN.split(":")[0]):
                    asyncio.create_task(tg_send_top10(chat_id))
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("Telegram polling error: %s", e)
            await asyncio.sleep(5)


# ══════════════════════════════════════════════════════════════════
# SPREAD HISTORY  (for rolling average)
# ══════════════════════════════════════════════════════════════════

def record_spread(ticker: str, spread: float) -> None:
    _record_history(ticker, spread)


# Кэш исторических средних: ticker → {avg7, avg30, ts}
_hist_avg_cache: dict[str, dict] = {}
HIST_AVG_TTL = int(os.getenv("HIST_AVG_TTL", "3600"))  # обновлять раз в час


def get_spread_avg(ticker: str, days: int = 7) -> Optional[float]:
    """Берёт avg из кэша исторических данных если есть, иначе из накопленной истории."""
    cached = _hist_avg_cache.get(ticker)
    if cached:
        return cached.get(f"avg{days}")
    # Фолбэк на накопленную историю пока кэш не заполнен
    hist = state["spread_history"].get(ticker, [])
    cutoff = time.time() - days * 86400
    vals = [h["v"] for h in hist if h["t"] > cutoff]
    return round(sum(vals) / len(vals), 3) if vals else None


async def _calc_historical_avg(session: aiohttp.ClientSession, ticker: str) -> None:
    """
    Для одного тикера:
    - Берём часовые свечи MEXC за 35 дней
    - Фильтруем только 9:30–16:00 ET (когда открыт рынок)
    - Усредняем цену MEXC по каждому дню
    - Сравниваем с Yahoo close того же дня
    - Считаем avg7/avg30
    """
    try:
        import zoneinfo
        et_tz = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        from datetime import timezone
        et_tz = timezone(timedelta(hours=-4))

    days = 35

    # 1. Yahoo дневные свечи
    try:
        end_dt   = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days + 3)
        df = yf.download(
            tickers=ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
            prepost=False,
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            return
        yahoo_close: dict[str, float] = {}
        for ts, row in df.iterrows():
            date = ts.strftime("%Y-%m-%d")
            try:
                c = float(row["Close"] if hasattr(row["Close"], "__float__") else row["Close"].iloc[0])
                if c > 0:
                    yahoo_close[date] = c
            except Exception:
                pass
    except Exception as e:
        log.debug("Historical Yahoo %s: %s", ticker, e)
        return

    if not yahoo_close:
        return

    # 2. MEXC часовые свечи — только 9:30–16:00 ET
    candidates = SYMBOL_MAP.get(ticker, [f"{ticker}STOCK_USDT", f"{ticker}_USDT"])
    divisor    = SPLIT_DIVISOR.get(ticker, 1.0)
    mexc_market: dict[str, list[float]] = {}

    for symbol in candidates:
        url = (f"https://contract.mexc.com/api/v1/contract/kline/{symbol}"
               f"?interval=Hour1&limit={days * 24 + 48}")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.json(content_type=None)
                if not raw.get("success"):
                    continue
                data   = raw.get("data", {})
                times  = data.get("time", [])
                closes = data.get("close", [])
                if not times:
                    continue
                from datetime import timezone as _tz
                utc = _tz.utc
                for ts_raw, c_raw in zip(times, closes):
                    ts_int = int(ts_raw)
                    if ts_int > 1e12:
                        ts_int //= 1000
                    dt_utc = datetime.utcfromtimestamp(ts_int).replace(tzinfo=utc)
                    dt_et  = dt_utc.astimezone(et_tz)
                    hm     = dt_et.hour * 60 + dt_et.minute
                    # только 9:30–16:00 ET в будни
                    if 570 <= hm < 960 and dt_et.weekday() < 5:
                        date = dt_et.strftime("%Y-%m-%d")
                        mexc_market.setdefault(date, []).append(float(c_raw) / divisor)
                if mexc_market:
                    break
        except Exception as e:
            log.debug("Historical MEXC %s %s: %s", ticker, symbol, e)

    if not mexc_market:
        return

    # 3. Считаем дневной спред
    daily_spreads: list[float] = []
    for date in sorted(set(yahoo_close) & set(mexc_market)):
        stock_p = yahoo_close[date]
        mexc_p  = sum(mexc_market[date]) / len(mexc_market[date])
        if stock_p > 0 and mexc_p > 0:
            daily_spreads.append(((mexc_p - stock_p) / stock_p) * 100)

    if not daily_spreads:
        return

    avg7  = round(sum(daily_spreads[-7:])  / len(daily_spreads[-7:]),  3)
    avg30 = round(sum(daily_spreads[-30:]) / len(daily_spreads[-30:]), 3)

    _hist_avg_cache[ticker] = {
        "avg7":  avg7,
        "avg30": avg30,
        "ts":    time.time(),
        "days":  len(daily_spreads),
    }


async def _refresh_historical_avgs(session: aiohttp.ClientSession) -> None:
    """Раз в час обновляет исторические avg для всех тикеров."""
    while True:
        log.info("Historical avgs: refreshing %d tickers...", len(CONFIG["tickers"]))
        t0 = time.time()
        done = 0
        for ticker in CONFIG["tickers"]:
            cached = _hist_avg_cache.get(ticker)
            if cached and time.time() - cached["ts"] < HIST_AVG_TTL:
                continue
            try:
                await _calc_historical_avg(session, ticker)
            except Exception as e:
                log.warning("%-6s | historical avg failed: %s: %s", ticker, type(e).__name__, e)
            done += 1
            if done % 20 == 0:
                log.info("Historical avgs progress: %d/%d (%.0fs elapsed)",
                          done, len(CONFIG["tickers"]), time.time() - t0)
            await asyncio.sleep(0.15)
        log.info("Historical avgs done in %.0fs | cached=%d/%d",
                  time.time() - t0, len(_hist_avg_cache), len(CONFIG["tickers"]))
        await asyncio.sleep(HIST_AVG_TTL)


# ══════════════════════════════════════════════════════════════════
# MAIN MONITOR LOOP
# ══════════════════════════════════════════════════════════════════

_force_refresh_event = asyncio.Event()
_yahoo_executor = ThreadPoolExecutor(max_workers=1)
_stock_cache: dict[str, dict[str, float | str | None]] = {}


async def monitor_loop() -> None:
    state["status"] = "running"
    log.info("Monitor started | tickers=%d | interval=%ss | threshold=%.2f%%",
             len(CONFIG["tickers"]), CONFIG["check_interval"], CONFIG["spread_threshold"])

    connector = aiohttp.TCPConnector(limit_per_host=10, limit=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 1. Резолвим символы MEXC (один HTTP-запрос на тикер при старте)
        await _resolve_mexc_symbols(session)

        # 2. Запускаем WebSocket — будет держать цены актуальными в реальном времени
        ws_task = asyncio.create_task(_mexc_websocket_loop())
        ws_task.add_done_callback(_log_task_exception)

        # 3. Прочие фоновые задачи
        lev_task = asyncio.create_task(fetch_all_leverage(session))
        lev_task.add_done_callback(_log_task_exception)

        tg_task = asyncio.create_task(tg_bot_polling())
        tg_task.add_done_callback(_log_task_exception)

        hist_task = asyncio.create_task(_refresh_historical_avgs(session))
        hist_task.add_done_callback(_log_task_exception)

        # 4. Даём WebSocket секунду подключиться и получить первые цены
        await asyncio.sleep(2)

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
    MEXC цены уже актуальны из WebSocket (_mexc_prices).
    Здесь только обновляем Yahoo и пересчитываем спреды.
    """
    loop = asyncio.get_event_loop()
    tickers = CONFIG["tickers"]

    yahoo_prices = await loop.run_in_executor(
        _yahoo_executor, fetch_all_yahoo_prices, tickers
    )

    session_label = get_market_session()
    for ticker in tickers:
        price_stock, sess_lbl = yahoo_prices.get(ticker, (None, session_label))
        price_mexc = _mexc_prices.get(ticker)
        _update_snapshot(ticker, price_mexc, price_stock, sess_lbl)

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
    elif price_stock > 0 and (price_mexc / price_stock > 5 or price_stock / price_mexc > 5):
        # Цены отличаются более чем в 5 раз — скорее всего поймали крипто-токен вместо акции
        snap["status"] = "error"
        snap["error"]  = f"Price mismatch (MEXC={price_mexc:.4f} vs Stock={price_stock:.4f}) — wrong symbol?"
        snap["price_mexc"] = None
        log.warning("%-6s | PRICE MISMATCH MEXC=%.4f Stock=%.4f — likely crypto token collision!",
                    ticker, price_mexc, price_stock)
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

        # Threshold alert (для дашборда)
        if abs(spread) >= CONFIG["spread_threshold"]:
            send_alert(ticker, spread, price_mexc, price_stock, "threshold")

        # Convergence alert
        prev = _prev_spread.get(ticker)
        if (prev is not None
                and abs(prev) >= CONFIG["spread_threshold"]
                and abs(spread) <= CONFIG["convergence_threshold"]):
            send_alert(ticker, spread, price_mexc, price_stock, "convergence")

        # Telegram алерт по отклонению от avg30
        avg30 = snap.get("avg30")
        avg7  = snap.get("avg7")
        task = asyncio.create_task(
            tg_check_spread_alert(ticker, spread, avg30, price_mexc, price_stock, avg7)
        )
        task.add_done_callback(_log_task_exception)

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

    def do_POST(self):
        self.do_GET()


def start_http_server() -> None:
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer(("0.0.0.0", CONFIG["web_port"]), Handler)
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
