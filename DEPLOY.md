# Деплой Spread Monitor

## Быстрый старт (Railway)

### 1. Подготовка

```bash
# Создай Git-репозиторий
cd "spread черновик c рефрешем тест график и авг"
git init
git add .
git commit -m "Initial commit"
```

### 2. GitHub

- Создай новый репозиторий на github.com (приватный)
- Запуш:

```bash
git remote add origin https://github.com/ТВОЙ_ЮЗЕРНЕЙМ/spread-monitor.git
git branch -M main
git push -u origin main
```

### 3. Railway

1. Зайди на [railway.app](https://railway.app) — залогинься через GitHub
2. **New Project** → **Deploy from GitHub repo** → выбери `spread-monitor`
3. Railway автоматически найдёт `Dockerfile` и задеплоит

### 4. Переменные окружения

В Railway зайди в **Settings → Variables** и добавь:

| Переменная | Значение | Обязательно |
|-----------|----------|-------------|
| `APP_PASSWORD` | твой пароль | Да |
| `SECRET_KEY` | любая длинная строка (напр. `mysecret123xyz`) | Да |
| `PORT` | `8765` | Railway ставит сам |

Остальные переменные (TICKERS, SPREAD_THRESHOLD и т.д.) — опционально, по умолчанию работает полный список тикеров.

### 5. Свой домен

1. В Railway: **Settings → Networking → Custom Domain**
2. Введи свой домен (напр. `spread.mydomain.com`)
3. Railway покажет CNAME-запись — добавь её у своего регистратора домена:
   - **Тип:** CNAME
   - **Имя:** `spread` (или что выбрал)
   - **Значение:** то что дал Railway (вида `xxx.up.railway.app`)
4. Подожди 5-10 минут — SSL-сертификат выпустится автоматически

### 6. Готово

Открой `https://spread.mydomain.com` — увидишь форму ввода пароля. После ввода — полный дашборд.

---

## Локальный запуск (для разработки)

```bash
pip install -r requirements.txt
python app.py
```

Откроется на `http://localhost:8765`. Пароль по умолчанию: `spread2024` (или что в `.env`).

---

## Структура файлов

```
app.py              — Flask-сервер с авторизацией (точка входа)
spread_monitor.py   — движок мониторинга (без изменений)
dashboard.html      — фронтенд дашборда
requirements.txt    — зависимости Python
Dockerfile          — контейнер для деплоя
Procfile            — команда запуска (Railway/Render)
railway.toml        — конфиг Railway
.env.example        — пример переменных окружения
.gitignore          — исключения из Git
```

---

## Стоимость

Railway: **~$5/мес** (план Hobby). Первые $5 бесплатно при регистрации. Приложение работает 24/7, не засыпает.

## Альтернативы

- **Render** ($7/мес за Always On) — аналогично, деплой из GitHub
- **VPS Hetzner** (€4/мес) — полный контроль, нужен Docker или systemd
