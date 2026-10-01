# ---- Gold Scalping Signal Bot: 15M + 30M (SQLite state, Weekly summary) ----
# Environment variables (Railway):
#   TWELVEDATA_API_KEY_15M, TWELVEDATA_API_KEY_30M, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
# requirements.txt: requests, pandas

import os
import sqlite3
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import threading
import gold_exchange_bot
# ---------------- CONFIG ----------------
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

SYMBOL = "XAU/USD"
CANDLE_LOOKBACK = 30
RUNUP_WINDOW = 12

MONITOR_INTERVAL_SECONDS = 120
SUMMARY_CHECK_INTERVAL_SECONDS = 3600
DAILY_CALL_LIMIT = 790

DB_PATH = os.environ.get("DB_PATH", "gold_scalp.db")

TIMEFRAMES = [
    {
        "name": "15M",
        "interval": "15min",
        "minutes": 15,
        "api_key": os.environ["TWELVEDATA_API_KEY_15M"],
        "min_runup": 20,
        "top_lookback": 12,
        "tolerance": 0.10,
        "sl": 10,
        "tps": [12],            # single TP
        "scan_window": 4,       # minutes after candle close in which we scan
        "max_age": 5,           # max minutes after close we are still willing to enter
    },
    {
        "name": "30M",
        "interval": "30min",
        "minutes": 30,
        "api_key": os.environ["TWELVEDATA_API_KEY_30M"],
        "min_runup": 30,
        "top_lookback": 12,
        "tolerance": 0.10,
        "sl": 10,
        "tps": [10, 20],        # TP1 then TP2, SL to breakeven after TP1
        "scan_window": 5,
        "max_age": 10,
    },
]

# ---------------- API CALL COUNTER ----------------
_calls = {}

def count_call(cfg):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rec = _calls.get(cfg["name"])
    if not rec or rec["day"] != today:
        rec = {"day": today, "n": 0}
        _calls[cfg["name"]] = rec
    rec["n"] += 1
    return rec["n"]

def calls_used(cfg):
    rec = _calls.get(cfg["name"])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return rec["n"] if rec and rec["day"] == today else 0

# ---------------- DATABASE ----------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timeframe TEXT,
            entry REAL, sl REAL, tp1 REAL, tp2 REAL,
            tp1_hit INTEGER DEFAULT 0,
            status TEXT DEFAULT 'open',
            outcome TEXT,
            signal_time TEXT,
            opened_at TEXT,
            closed_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            signal_key TEXT PRIMARY KEY
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS bot_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()

def get_meta(key, default=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM bot_meta WHERE key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default

def set_meta(key, value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO bot_meta (key, value) VALUES (?, ?)", (key, value))
    conn.commit()
    conn.close()

def already_signaled(key):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT 1 FROM signal_log WHERE signal_key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row is not None

def log_signal(key):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO signal_log (signal_key) VALUES (?)", (key,))
    conn.commit()
    conn.close()

def insert_trade(tf, entry, sl, tp1, tp2, signal_time):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO trades (timeframe, entry, sl, tp1, tp2, signal_time, opened_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (tf, entry, sl, tp1, tp2, str(signal_time), datetime.now(timezone.utc).isoformat()))
    conn.commit()
    trade_id = c.lastrowid
    conn.close()
    return trade_id

def get_open_trades(tf):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM trades WHERE status = 'open' AND timeframe = ?", (tf,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

def update_trade_tp1_hit(trade_id, new_sl):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE trades SET tp1_hit = 1, sl = ? WHERE id = ?", (new_sl, trade_id))
    conn.commit()
    conn.close()

def close_trade(trade_id, outcome):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        UPDATE trades SET status = 'closed', outcome = ?, closed_at = ?
        WHERE id = ?
    """, (outcome, datetime.now(timezone.utc).isoformat(), trade_id))
    conn.commit()
    conn.close()

def get_weekly_stats(since, tf):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT outcome FROM trades WHERE timeframe = ? AND closed_at >= ?",
              (tf, since.isoformat()))
    outcomes = [r[0] for r in c.fetchall()]
    conn.close()
    return {
        "total": len(outcomes),
        "tp": outcomes.count("TP"),
        "tp2": outcomes.count("TP2"),
        "breakeven": outcomes.count("BREAKEVEN"),
        "sl": outcomes.count("SL"),
    }

# ---------------- TELEGRAM ----------------
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"[Telegram error] {e}")

def check_telegram_commands():
    """Polls for /close <id> posted in the channel and closes that trade manually."""
    offset = get_meta("telegram_update_offset")
    offset = int(offset) if offset else None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"timeout": 0}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(url, params=params, timeout=10)
        updates = r.json().get("result", [])
    except Exception as e:
        print(f"[Telegram poll error] {e}")
        return

    for update in updates:
        set_meta("telegram_update_offset", str(update["update_id"] + 1))
        post = update.get("channel_post") or update.get("message")
        if not post or "text" not in post:
            continue
        text = post["text"].strip()
        if text.startswith("/close "):
            try:
                trade_id = int(text.split()[1])
                close_trade(trade_id, "MANUAL_CLOSE")
                send_telegram(f"⚪ Trade #{trade_id} manually closed.")
            except (IndexError, ValueError):
                send_telegram("Usage: /close <trade_id>")

# ---------------- DATA (TwelveData) ----------------
def fetch_recent_candles(cfg):
    if calls_used(cfg) >= DAILY_CALL_LIMIT:
        print(f"[{cfg['name']}] daily call limit reached, skipping candle fetch")
        return None
    count_call(cfg)
    url = "https://api.twelvedata.com/time_series"
    params = {
        "timezone": "UTC",
        "symbol": SYMBOL, "interval": cfg["interval"],
        "outputsize": CANDLE_LOOKBACK, "apikey": cfg["api_key"], "format": "JSON"
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
    except Exception as e:
        print(f"[{cfg['name']}] candle fetch error: {e}")
        return None
    if "values" not in data:
        print(f"[{cfg['name']}] data fetch error: {data}")
        return None
    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    return df

def fetch_current_price(cfg):
    if calls_used(cfg) >= DAILY_CALL_LIMIT:
        print(f"[{cfg['name']}] daily call limit reached, skipping price fetch")
        return None
    count_call(cfg)
    url = "https://api.twelvedata.com/price"
    params = {"symbol": SYMBOL, "apikey": cfg["api_key"]}
    try:
        r = requests.get(url, params=params, timeout=10)
        return float(r.json()["price"])
    except Exception as e:
        print(f"[{cfg['name']}] price fetch error: {e}")
        return None

# ---------------- STRATEGY (sell-only) ----------------
def scan_for_signals(cfg):
    """Returns True when the scan completed (even with no signal),
    False if data could not be fetched so it can retry within the window."""
    name = cfg["name"]
    now = datetime.now(timezone.utc)

    if get_open_trades(name):
        print(f"[{now}] {name} scan skipped: a trade is already open")
        return True

    df = fetch_recent_candles(cfg)
    if df is None or len(df) < RUNUP_WINDOW + 4:
        print(f"[{now}] {name} scan skipped: not enough candle data")
        return False

    now_naive = now.replace(tzinfo=None)
    if df["datetime"].iloc[-1] + pd.Timedelta(minutes=cfg["minutes"]) > now_naive:
        df = df.iloc[:-1].reset_index(drop=True)   # drop the still-forming candle

    c1, c2 = len(df) - 2, len(df) - 1
    ts = df.loc[c2, "datetime"]
    age_min = (now_naive - (ts + pd.Timedelta(minutes=cfg["minutes"]))).total_seconds() / 60
    if age_min > cfg["max_age"]:
        print(f"[{now}] {name} scan skipped: candle {ts} closed {age_min:.0f} min ago, too old")
        return True
    key = f"{name}-{ts}"
    if already_signaled(key):
        print(f"[{now}] {name} scan skipped: candle {ts} already signaled")
        return True

    close1 = df.loc[c1, "close"]
    open2 = df.loc[c2, "open"]
    prior_high = df["close"].iloc[c1 - cfg["top_lookback"]:c1].max()
    runup = close1 - df["close"].iloc[c1 - RUNUP_WINDOW:c1].min()
    gap = abs(close1 - open2)

    if close1 <= prior_high:
        print(f"[{now}] {name} no signal: close1 {close1:.2f} not above prior {cfg['top_lookback']} ({prior_high:.2f})")
        return True
    if runup < cfg["min_runup"]:
        print(f"[{now}] {name} no signal: run-up {runup:.2f} below {cfg['min_runup']}")
        return True
    if gap > cfg["tolerance"]:
        print(f"[{now}] {name} no signal: gap {gap:.2f} above {cfg['tolerance']}")
        return True

    close2 = df.loc[c2, "close"]
    lowest_prev = df["close"].iloc[c2 - RUNUP_WINDOW:c2].min()
    if close2 < lowest_prev:
        print(f"[{now}] {name} no signal: candle 2 close {close2:.2f} is the lowest of the last {RUNUP_WINDOW} closes ({lowest_prev:.2f})")
        return True

    entry_price = fetch_current_price(cfg)
    if entry_price is None:
        print(f"[{now}] {name} pattern matched at {ts} but live price fetch failed")
        return False
    open_new_trade(cfg, entry_price, ts)
    log_signal(key)
    return True

def open_new_trade(cfg, entry_price, signal_time):
    name = cfg["name"]
    sl = entry_price + cfg["sl"]
    tp1 = entry_price - cfg["tps"][0]
    tp2 = entry_price - cfg["tps"][1] if len(cfg["tps"]) > 1 else None

    trade_id = insert_trade(name, entry_price, sl, tp1, tp2, signal_time)

    if tp2 is None:
        tp_line = f"TP: `{tp1:.2f}`"
    else:
        tp_line = f"TP1: `{tp1:.2f}` | TP2: `{tp2:.2f}`"

    send_telegram(
        f"🟡 *GOLD {name} SCALPING — SELL* (#{trade_id})\n"
        f"Entry: `{entry_price:.2f}`\n"
        f"SL: `{sl:.2f}`\n"
        f"{tp_line}"
    )
    print(f"[{datetime.now(timezone.utc)}] Opened {name} sell trade #{trade_id} @ {entry_price:.2f}")

# ---------------- MONITOR ----------------
def monitor_open_trades(cfg):
    name = cfg["name"]
    trades = get_open_trades(name)
    if not trades:
        return
    price = fetch_current_price(cfg)
    if price is None:
        print(f"[{datetime.now(timezone.utc)}] {name} monitor skipped, price fetch failed")
        return

    for trade in trades:
        single_tp = trade["tp2"] is None

        hit_sl = price >= trade["sl"]

        if single_tp:
            if hit_sl:
                send_telegram(
                    f"🔴 *GOLD {name} Trade #{trade['id']} Closed — Stop Loss*\n"
                    f"SELL entry {trade['entry']:.2f} → exit {price:.2f}"
                )
                close_trade(trade["id"], "SL")
            elif price <= trade["tp1"]:
                send_telegram(
                    f"🟢 *GOLD {name} TP Hit — Trade #{trade['id']} Closed*\n"
                    f"SELL entry {trade['entry']:.2f} → exit {price:.2f}"
                )
                close_trade(trade["id"], "TP")
            continue

        # two-TP logic (30M)
        hit_tp1 = (not trade["tp1_hit"]) and price <= trade["tp1"]
        hit_tp2 = trade["tp1_hit"] and price <= trade["tp2"]

        if hit_sl:
            outcome = "BREAKEVEN" if trade["tp1_hit"] else "SL"
            label = "Breakeven (SL moved after TP1)" if trade["tp1_hit"] else "Stop Loss"
            send_telegram(
                f"🔴 *GOLD {name} Trade #{trade['id']} Closed — {label}*\n"
                f"SELL entry {trade['entry']:.2f} → exit {price:.2f}"
            )
            close_trade(trade["id"], outcome)
            continue

        if hit_tp1:
            update_trade_tp1_hit(trade["id"], trade["entry"])
            send_telegram(
                f"🟢 *GOLD {name} TP1 Hit* — Trade #{trade['id']} entry {trade['entry']:.2f} → {price:.2f}\n"
                f"SL moved to breakeven. Now targeting TP2 ({trade['tp2']:.2f})"
            )
            continue

        if hit_tp2:
            send_telegram(
                f"🟢🟢 *GOLD {name} TP2 Hit — Trade #{trade['id']} Closed*\n"
                f"SELL entry {trade['entry']:.2f} → exit {price:.2f}"
            )
            close_trade(trade["id"], "TP2")

# ---------------- WEEKLY SUMMARY ----------------
def maybe_send_weekly_summary():
    last_sent_str = get_meta("last_summary_sent")
    now = datetime.now(timezone.utc)

    if last_sent_str is None:
        set_meta("last_summary_sent", now.isoformat())
        return

    last_sent = datetime.fromisoformat(last_sent_str)
    if now - last_sent < timedelta(days=7):
        return

    s15 = get_weekly_stats(last_sent, "15M")
    s30 = get_weekly_stats(last_sent, "30M")

    send_telegram(
        f"📊 *Weekly Summary*\n\n"
        f"*GOLD 15M SCALPING*\n"
        f"Total trades closed: {s15['total']}\n"
        f"TP reached: {s15['tp']}\n"
        f"SL (loss): {s15['sl']}\n\n"
        f"*GOLD 30M SCALPING*\n"
        f"Total trades closed: {s30['total']}\n"
        f"TP2 reached: {s30['tp2']}\n"
        f"SL at entry (breakeven, no loss): {s30['breakeven']}\n"
        f"SL (loss): {s30['sl']}\n"
        f"TP1 reached (before final outcome): {s30['tp2'] + s30['breakeven']}"
    )
    set_meta("last_summary_sent", now.isoformat())

# ---------------- MAIN LOOP ----------------
def main():
    init_db()
    print("Gold scalping bot (15M + 30M) started.")
    send_telegram("✅ Gold scalping bot started (15M + 30M).")

    last_scan_key = {cfg["name"]: None for cfg in TIMEFRAMES}
    last_summary_check = 0

    threading.Thread(target=gold_exchange_bot.main, daemon=True).start()

    while True:
        now_dt = datetime.now(timezone.utc)

        for cfg in TIMEFRAMES:
            minutes_into = (now_dt.hour * 60 + now_dt.minute) % cfg["minutes"]
            boundary = (now_dt - timedelta(minutes=minutes_into)).strftime("%Y-%m-%d-%H-%M")

            if boundary != last_scan_key[cfg["name"]] and minutes_into < cfg["scan_window"]:
                if scan_for_signals(cfg):
                    last_scan_key[cfg["name"]] = boundary

            monitor_open_trades(cfg)

        check_telegram_commands()

        now_ts = time.time()
        if now_ts - last_summary_check >= SUMMARY_CHECK_INTERVAL_SECONDS:
            maybe_send_weekly_summary()
            last_summary_check = now_ts

        time.sleep(MONITOR_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
