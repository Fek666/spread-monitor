# MEXC vs Pre-market Spread Monitor

Мониторит процентный спред между ценами токенизированных акций на MEXC Futures
и премаркетом/регулярной ценой акций через Yahoo Finance.

---

## Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка (опционально)

```bash
cp .env.example .env
# Отредактируй .env под свои нужды
```

### 3. Запуск

```bash
python spread_monitor.py
```

Браузер откроется автоматически на `http://localhost:8765`.
Остановить: `Ctrl+C`.

---

## Конфигурация (.env)

| Параметр | По умолчанию | Описание |
|---|---|---|
| `TICKERS` | `MU, CSCO, NVDA, TSLA, INTC, NBIS, SNDK, CRCL, MRVL, GOOGL, MSTR, AMD, DRAM, AAPL, BABA, HIMS, PLTR, META, MSFT, RKLB, QQQ, STX, ASTS, IREN, COIN, WDC, GE, PANW, INTU, ARM, CVNA, COHR, AMZN, SPOT, SHOP, CRWV, TXN, TSM, QCOM, UBER, CRWD, VRT, ASML, IONQ, LITE, IBM, ABBV, LLY, ORCL, JPM, NKE, FIG, AMAT, ONDS, ADBE, VZ, SOXX, NOW, HOOD, SNOW, USAR, MA, JD, CRM, XOM, C, WFC, GME, BA, PYPL, SBUX, PG, LRCX, SMCI, AVGO, NFLX, V, LMT, OXY, KO, PAYP, WMT, CVX, RDDT, BAC, GEV, COST, RTX, UNH, PDD, COP, MCD, FUTU, KLAC` | Тикеры через запятую |
| `SPREAD_THRESHOLD` | `0.7` | Порог алерта в % |
| `CHECK_INTERVAL` | `10` | Интервал проверки (секунды) |
| `ALERT_COOLDOWN` | `60` | Пауза между повторными алертами на один тикер (секунды) |
| `WEB_PORT` | `8765` | Порт веб-дашборда |
| `USDT_USD_RATE` | `1.0` | Курс USDT/USD (де-пег USDT) |

---

## Архитектура

```
spread_monitor.py
├── get_mexc_price()      → MEXC Futures API (async, aiohttp)
├── get_stock_price()     → Yahoo Finance (sync, yfinance, в executor)
├── calculate_spread()    → ((mexc - stock) / stock) × 100
├── send_alert()          → пуш в state["alerts"] + spam-guard
├── monitor_loop()        → asyncio, тикеры параллельно
└── HTTP Server           → /api/state (JSON) + /dashboard (HTML)

dashboard.html            → polling /api/state каждые 2 сек
```

---

## Источники данных

### MEXC Futures API
- Endpoint: `https://contract.mexc.com/api/v1/contract/ticker?symbol=AAPLUSDT`
- Публичный, без API-ключа
- Доступен 24/7 (MEXC торгует круглосуточно)

### Yahoo Finance (yfinance)
- Приоритет: `preMarketPrice` → `currentPrice` → `regularMarketPrice` → `previousClose`
- Премаркет доступен ~04:00–09:30 ET в торговые дни
- Если рынок закрыт — берётся `previousClose`

### USD vs USDT
USDT принимается за 1:1 к USD. Для учёта де-пега настрой `USDT_USD_RATE` в `.env`.

---

## Примечания по тикерам

MEXC использует формат `{TICKER}USDT` для фьючерсов на акции.
Проверь доступность токена на MEXC: [https://www.mexc.com/ru-RU/futures](https://www.mexc.com/ru-RU/futures)

Популярные пары: `AAPLUSDT`, `TSLAUSDT`, `NVDAUSDT`, `AMZNUSDT`, `MSFTUSDT`, `GOOGLUSDT`, `METAUSDT`

---

## Добавить Telegram-уведомления (опционально)

### 1. Создай бота
1. Напиши `/newbot` боту `@BotFather` в Telegram
2. Получи `BOT_TOKEN` вида `7123456789:AAF...`
3. Напиши боту любое сообщение, затем открой:
   `https://api.telegram.org/bot<BOT_TOKEN>/getUpdates`
4. Скопируй свой `chat_id` из ответа

### 2. Добавь в .env

```
TELEGRAM_BOT_TOKEN=7123456789:AAF...
TELEGRAM_CHAT_ID=123456789
```

### 3. Замени функцию send_alert() в spread_monitor.py

```python
import os, aiohttp   # aiohttp уже импортирован

async def send_telegram_alert(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        log.warning("Telegram error: %s", e)


def send_alert(ticker, spread, price_mexc, price_stock):
    now = time.time()
    if now - _alert_timestamps.get(ticker, 0) < CONFIG["alert_cooldown"]:
        return
    _alert_timestamps[ticker] = now
    direction = "📈 PREMIUM" if spread > 0 else "📉 DISCOUNT"
    text = (
        f"🚨 <b>Spread Alert: {ticker}</b>\n"
        f"{direction}\n"
        f"Spread: <b>{spread:+.3f}%</b> (threshold ±{CONFIG['spread_threshold']}%)\n"
        f"MEXC:  <code>${price_mexc:.4f}</code>\n"
        f"Stock: <code>${price_stock:.4f}</code>"
    )
    # push to web dashboard
    alert = { "id": int(now*1000), "time": datetime.now().strftime("%H:%M:%S"),
              "ticker": ticker, "spread": round(spread,3),
              "price_mexc": round(price_mexc,4), "price_stock": round(price_stock,4),
              "direction": "PREMIUM" if spread>0 else "DISCOUNT",
              "threshold": CONFIG["spread_threshold"] }
    state["alerts"].insert(0, alert)
    state["alerts"] = state["alerts"][:50]
    # send Telegram
    asyncio.create_task(send_telegram_alert(text))
    log.warning("🚨 ALERT %s | spread=%.3f%%", ticker, spread)
```
