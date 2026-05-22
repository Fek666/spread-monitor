"""
MEXC Tokenized Stocks vs Pre-market Spread Monitor — Web App
=============================================================
Flask wrapper with password protection and production-ready serving.
"""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from functools import wraps

from flask import Flask, request, session, redirect, url_for, jsonify, send_file, render_template_string
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# Import the monitor engine
# ─────────────────────────────────────────────
import spread_monitor as engine

# ─────────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(32).hex())

APP_PASSWORD = os.getenv("APP_PASSWORD", "spread2024")
SESSION_LIFETIME_DAYS = int(os.getenv("SESSION_DAYS", "30"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("app")


# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Spread Monitor — Login</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap');
  :root {
    --bg:#0a0c0f; --bg2:#11141a; --bg3:#181c24;
    --border:rgba(255,255,255,0.07); --border2:rgba(255,255,255,0.14);
    --text:#e8ecf0; --muted:#5a6270; --muted2:#8a93a0;
    --green:#00e676; --red:#ff4444; --accent:#7c6af5;
    --blue:#40c4ff;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{
    background:var(--bg);
    color:var(--text);
    font-family:'IBM Plex Sans',sans-serif;
    min-height:100vh;
    display:flex;
    align-items:center;
    justify-content:center;
    overflow:hidden;
  }
  .login-container{
    position:relative;
    z-index:1;
  }
  .login-card{
    background:var(--bg2);
    border:1px solid var(--border2);
    border-radius:16px;
    padding:48px 40px;
    width:380px;
    max-width:90vw;
    box-shadow:0 24px 80px rgba(0,0,0,0.6), 0 0 0 1px rgba(124,106,245,0.05);
    animation:fadeUp 0.5s ease-out;
  }
  @keyframes fadeUp{
    from{opacity:0;transform:translateY(20px)}
    to{opacity:1;transform:translateY(0)}
  }
  .logo{
    display:flex;
    align-items:center;
    gap:10px;
    margin-bottom:32px;
    justify-content:center;
  }
  .logo-dot{
    width:10px;height:10px;border-radius:50%;
    background:var(--green);
    box-shadow:0 0 12px var(--green);
    animation:pulse 2s infinite;
  }
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .logo-text{
    font-family:'IBM Plex Mono',monospace;
    font-size:13px;font-weight:600;
    letter-spacing:.08em;text-transform:uppercase;
    color:var(--text);
  }
  .login-title{
    font-family:'IBM Plex Mono',monospace;
    font-size:18px;font-weight:600;
    text-align:center;
    margin-bottom:8px;
    color:var(--text);
  }
  .login-subtitle{
    text-align:center;
    font-size:13px;
    color:var(--muted2);
    margin-bottom:32px;
  }
  .input-group{
    position:relative;
    margin-bottom:20px;
  }
  .input-group input{
    width:100%;
    padding:14px 18px 14px 44px;
    background:var(--bg3);
    border:1px solid var(--border2);
    border-radius:10px;
    color:var(--text);
    font-family:'IBM Plex Mono',monospace;
    font-size:14px;
    outline:none;
    transition:border-color .2s, box-shadow .2s;
  }
  .input-group input:focus{
    border-color:var(--accent);
    box-shadow:0 0 0 3px rgba(124,106,245,0.15);
  }
  .input-group input::placeholder{
    color:var(--muted);
  }
  .input-icon{
    position:absolute;
    left:14px;top:50%;transform:translateY(-50%);
    color:var(--muted);font-size:16px;
    pointer-events:none;
  }
  .login-btn{
    width:100%;
    padding:14px;
    background:linear-gradient(135deg, var(--accent), #5b4fd4);
    border:none;
    border-radius:10px;
    color:#fff;
    font-family:'IBM Plex Mono',monospace;
    font-size:13px;font-weight:600;
    letter-spacing:.06em;text-transform:uppercase;
    cursor:pointer;
    transition:transform .15s, box-shadow .2s;
  }
  .login-btn:hover{
    transform:translateY(-1px);
    box-shadow:0 8px 24px rgba(124,106,245,0.3);
  }
  .login-btn:active{
    transform:translateY(0);
  }
  .error-msg{
    background:rgba(255,68,68,0.1);
    border:1px solid rgba(255,68,68,0.25);
    border-radius:8px;
    padding:10px 14px;
    margin-bottom:20px;
    font-size:12px;
    color:var(--red);
    text-align:center;
    animation:shake 0.4s ease-out;
  }
  @keyframes shake{
    0%,100%{transform:translateX(0)}
    25%{transform:translateX(-6px)}
    75%{transform:translateX(6px)}
  }
  .bg-grid{
    position:fixed;inset:0;z-index:0;
    background-image:
      linear-gradient(rgba(124,106,245,0.03) 1px, transparent 1px),
      linear-gradient(90deg, rgba(124,106,245,0.03) 1px, transparent 1px);
    background-size:60px 60px;
    pointer-events:none;
  }
  .bg-glow{
    position:fixed;
    width:500px;height:500px;
    border-radius:50%;
    background:radial-gradient(circle, rgba(124,106,245,0.08), transparent 70%);
    top:50%;left:50%;
    transform:translate(-50%,-50%);
    pointer-events:none;z-index:0;
  }
</style>
</head>
<body>
<div class="bg-grid"></div>
<div class="bg-glow"></div>
<div class="login-container">
  <div class="login-card">
    <div class="logo">
      <div class="logo-dot"></div>
      <span class="logo-text">Spread Monitor</span>
    </div>
    <div class="login-title">Welcome</div>
    <div class="login-subtitle">Enter password to access the dashboard</div>
    {% if error %}
    <div class="error-msg">{{ error }}</div>
    {% endif %}
    <form method="POST" action="/login">
      <div class="input-group">
        <span class="input-icon">&#128274;</span>
        <input type="password" name="password" placeholder="Password" autofocus autocomplete="current-password">
      </div>
      <button type="submit" class="login-btn">Enter Dashboard</button>
    </form>
  </div>
</div>
</body>
</html>"""


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return decorated


@app.route("/health")
def health():
    ensure_monitor()
    return jsonify({"status": "ok"}), 200


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if request.method == "POST":
        password = request.form.get("password", "")
        if password == APP_PASSWORD:
            session["authenticated"] = True
            session.permanent = True
            app.permanent_session_lifetime = timedelta(days=SESSION_LIFETIME_DAYS)
            return redirect("/")
        return render_template_string(LOGIN_PAGE, error="Wrong password")
    return render_template_string(LOGIN_PAGE, error=None)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"


@app.route("/")
@app.route("/dashboard")
@login_required
def dashboard():
    return send_file(DASHBOARD_HTML, mimetype="text/html")


# ─────────────────────────────────────────────
# API ROUTES
# ─────────────────────────────────────────────

_last_force_refresh: float = 0.0


@app.route("/api/state")
@login_required
def api_state():
    data = {
        "snapshots": engine.state["snapshots"],
        "alerts": engine.state["alerts"],
        "last_update": engine.state["last_update"],
        "status": engine.state["status"],
        "leverage": engine.state["leverage"],
        "watchlist_hidden": list(engine._watchlist_hidden),
        "trash": engine.state["trash"],
        "notes": engine.state["notes"],
        "open_columns": engine.state["open_columns"],
        "config": {
            "tickers": engine.CONFIG["tickers"],
            "threshold": engine.CONFIG["spread_threshold"],
            "convergence_threshold": engine.CONFIG["convergence_threshold"],
            "interval": engine.CONFIG["check_interval"],
            "force_refresh_cooldown": engine.CONFIG["force_refresh_cooldown"],
        },
        "last_force_refresh": _last_force_refresh,
    }
    return jsonify(data)


@app.route("/api/force_refresh")
@login_required
def api_force_refresh():
    global _last_force_refresh
    now = time.time()
    elapsed = now - _last_force_refresh
    cooldown = engine.CONFIG["force_refresh_cooldown"]
    if elapsed < cooldown:
        return jsonify({"ok": False, "remaining": int(cooldown - elapsed)})
    _last_force_refresh = now
    engine._force_refresh_event.set()
    return jsonify({"ok": True})


@app.route("/api/watchlist", methods=["GET", "POST"])
@login_required
def api_watchlist():
    if request.method == "POST":
        payload = request.get_json(force=True, silent=True) or {}
        hidden = payload.get("hidden", [])
        engine._watchlist_hidden.clear()
        engine._watchlist_hidden.update(t.upper().strip() for t in hidden if t)
        engine.state["trash"] = list(dict.fromkeys(
            [t.upper().strip() for t in payload.get("trash", engine.state["trash"]) if t]
        ))
        engine.state["open_columns"] = list(dict.fromkeys(
            [t.upper().strip() for t in payload.get("open_columns", engine.state["open_columns"]) if t]
        ))
        engine._save_persisted_state()
    else:
        ticker = request.args.get("ticker", "").upper().strip()
        action = request.args.get("action", "toggle")
        if ticker:
            if action == "hide":
                engine._watchlist_hidden.add(ticker)
                engine._add_trash_ticker(ticker)
            elif action == "show":
                engine._watchlist_hidden.discard(ticker)
                engine._remove_trash_ticker(ticker)
            else:
                if ticker in engine._watchlist_hidden:
                    engine._watchlist_hidden.discard(ticker)
                    engine._remove_trash_ticker(ticker)
                else:
                    engine._watchlist_hidden.add(ticker)
                    engine._add_trash_ticker(ticker)

    return jsonify({
        "hidden": list(engine._watchlist_hidden),
        "trash": engine.state["trash"],
        "open_columns": engine.state["open_columns"],
    })


@app.route("/api/history")
@login_required
def api_history():
    ticker = request.args.get("ticker", "").upper().strip()
    days = max(7, min(int(request.args.get("days", "30")), 180))
    if not ticker:
        return jsonify({"ok": False, "error": "No ticker"}), 400
    try:
        candles = engine._get_history(ticker, days)
        return jsonify({"ok": True, "ticker": ticker, "days": days, "candles": candles})
    except Exception as e:
        log.warning("history %s error: %s", ticker, e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/history_hourly")
@login_required
def api_history_hourly():
    ticker = request.args.get("ticker", "").upper().strip()
    if not ticker:
        return jsonify({"ok": False, "error": "No ticker"}), 400
    try:
        candles = engine._get_hourly_history(ticker)
        return jsonify({"ok": True, "ticker": ticker, "candles": candles})
    except Exception as e:
        log.warning("history hourly %s error: %s", ticker, e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/news")
@login_required
def api_news():
    ticker = request.args.get("ticker", "").upper().strip()
    return jsonify({"ok": True, "ticker": ticker, "items": engine._get_news(ticker)})


@app.route("/api/notes", methods=["GET", "POST"])
@login_required
def api_notes():
    action = request.args.get("action", "list")
    if action == "delete":
        engine._delete_note(request.args.get("id", ""))
    elif action == "save":
        payload = request.get_json(force=True, silent=True) or {}
        engine._upsert_note(payload)
    return jsonify({"ok": True, "notes": engine.state["notes"]})


@app.route("/api/open_columns", methods=["GET", "POST"])
@login_required
def api_open_columns():
    ticker = request.args.get("ticker", "").upper().strip()
    action = request.args.get("action", "open")
    engine._set_open_column(ticker, action != "close")
    return jsonify({"ok": True, "open_columns": engine.state["open_columns"]})


# ─────────────────────────────────────────────
# MONITOR THREAD
# ─────────────────────────────────────────────

_monitor_started = False


def run_monitor():
    import time as _t
    _t.sleep(10)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(engine.monitor_loop())


def ensure_monitor():
    global _monitor_started
    if _monitor_started:
        return
    _monitor_started = True
    monitor_thread = threading.Thread(target=run_monitor, daemon=True)
    monitor_thread.start()
    log.info("Monitor thread scheduled (starts in 10s)")


# ─────────────────────────────────────────────
# ENTRY POINT (local dev)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8765"))
    app.run(host="0.0.0.0", port=port, debug=False)
