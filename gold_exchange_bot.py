# ---- Gold CFD Bot (Bitget CFD data): 1H + 30M + 15M ----
# Env vars:
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE   (READ-ONLY key)
#   CFD_SYMBOL                    (optional: XAUUSD, XAUUSD.s or XAUUSD.pro)
#   TELEGRAM_CHAT_ID_EXCHANGE     (required, the test channel)
#   TELEGRAM_BOT_TOKEN_EXCHANGE   (optional, defaults to TELEGRAM_BOT_TOKEN)
#   DB_PATH_EXCHANGE              (optional)
# requirements.txt: requests (and pandas if main.py still uses it)

import os
import time
import hmac
import hashlib
import base64
import sqlite3
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.bitget.com"
SYMBOL_CANDIDATES = ["XAUUSD", "XAUUSD.s", "XAUUSD.pro"]
CANDLE_SIDE = "sell"          # bid-based candles, because signals are sells

CANDLE_LOOKBACK = 30
RUNUP_WINDOW = 12
MONITOR_INTERVAL_SECONDS = 20
SUMMARY_CHECK_INTERVAL_SECONDS = 3600

TIMEFRAMES = [
    {"name": "15M", "label": "15M SCALPING", "tf": "15m", "minutes": 15,
     "min_runup": 20, "top_lookback": 8, "tolerance": 0.10,
     "sl": 10, "tps": [12], "scan_window": 4, "max_age": 5},
    {"name": "30M", "label": "30M SCALPING", "tf": "15m", "minutes": 30,
     "min_runup": 30, "top_lookback": 6, "tolerance": 0.20,
     "sl": 10, "tps": [10, 20], "scan_window": 5, "max_age": 10},
    {"name": "1H", "label": "1H", "tf": "1h", "minutes": 60,
     "min_runup": 40, "top_lookback": 5, "tolerance": 0.30,
     "sl": 10, "tps": [15, 20], "scan_window": 5, "max_age": 10},
]
TF_BY_NAME = {t["name"]: t for t in TIMEFRAMES}

TOKEN = CHAT = DB_PATH = None
API_KEY = API_SECRET = API_PASS = None
LAST_30M_MODE = None
SYMBOL = None
COMMANDS_ENABLED = False


def log(msg):
    print(f"[{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ---------------- DATABASE ----------------
def db(sql, params=(), fetch=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()] if fetch else None
        conn.commit()
        return rows if fetch else cur.lastrowid
    finally:
        conn.close()


def init_db():
    db("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timeframe TEXT, entry REAL, sl REAL, tp1 REAL, tp2 REAL,
            tp1_hit INTEGER DEFAULT 0, status TEXT DEFAULT 'open',
            outcome TEXT, signal_time TEXT, opened_at TEXT, closed_at TEXT)""")
    db("CREATE TABLE IF NOT EXISTS signal_log (signal_key TEXT PRIMARY KEY)")
    db("CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value TEXT)")


def get_meta(key):
    rows = db("SELECT value FROM bot_meta WHERE key = ?", (key,), fetch=True)
    return rows[0]["value"] if rows else None


def set_meta(key, value):
    db("INSERT OR REPLACE INTO bot_meta (key, value) VALUES (?, ?)", (key, value))


def signaled(key):
    return bool(db("SELECT 1 FROM signal_log WHERE signal_key = ?", (key,), fetch=True))


def log_signal(key):
    db("INSERT OR IGNORE INTO signal_log (signal_key) VALUES (?)", (key,))


def insert_trade(tf, entry, sl, tp1, tp2, signal_time):
    return db("""INSERT INTO trades (timeframe, entry, sl, tp1, tp2, signal_time, opened_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
              (tf, entry, sl, tp1, tp2, str(signal_time), datetime.now(timezone.utc).isoformat()))


def open_trades(tf=None):
    if tf:
        return db("SELECT * FROM trades WHERE status='open' AND timeframe=?", (tf,), fetch=True)
    return db("SELECT * FROM trades WHERE status='open'", fetch=True)


def close_trade(trade_id, outcome):
    db("UPDATE trades SET status='closed', outcome=?, closed_at=? WHERE id=?",
       (outcome, datetime.now(timezone.utc).isoformat(), trade_id))


def tp1_hit(trade_id, new_sl):
    db("UPDATE trades SET tp1_hit=1, sl=? WHERE id=?", (new_sl, trade_id))


def stats(since, tf):
    rows = db("SELECT outcome FROM trades WHERE timeframe=? AND closed_at>=?",
              (tf, since.isoformat()), fetch=True)
    o = [r["outcome"] for r in rows]
    return {"total": len(o), "tp": o.count("TP"), "tp2": o.count("TP2"),
            "be": o.count("BREAKEVEN"), "sl": o.count("SL")}


# ---------------- TELEGRAM ----------------
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        log(f"[Telegram error] {e}")


def check_commands():
    """/close <id> in the channel. Only active when a separate Telegram bot token is set."""
    if not COMMANDS_ENABLED:
        return
    offset = get_meta("telegram_update_offset")
    params = {"timeout": 0}
    if offset:
        params["offset"] = int(offset)
    try:
        r = requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates", params=params, timeout=10)
        updates = r.json().get("result", [])
    except Exception as e:
        log(f"[Telegram poll error] {e}")
        return

    for u in updates:
        set_meta("telegram_update_offset", str(u["update_id"] + 1))
        post = u.get("channel_post") or u.get("message")
        if not post or "text" not in post:
            continue
        if str(CHAT).lstrip("-").isdigit() and str(post["chat"]["id"]) != str(CHAT):
            continue
        parts = post["text"].strip().split()
        if not parts or parts[0].split("@")[0] != "/close":
            continue
        try:
            tid = int(parts[1])
        except (IndexError, ValueError):
            send_telegram("Usage: /close 12 (use the trade number)")
            continue
        if not db("SELECT id FROM trades WHERE id=? AND status='open'", (tid,), fetch=True):
            send_telegram(f"Trade #{tid} not found or already closed.")
            continue
        close_trade(tid, "MANUAL_CLOSE")
        send_telegram(f"⚪ Trade #{tid} manually closed.")


# ---------------- BITGET CFD DATA ----------------
def api_get(path, params):
    query = urlencode(params)
    request_path = f"{path}?{query}"
    ts = str(int(time.time() * 1000))
    msg = ts + "GET" + request_path
    sign = base64.b64encode(
        hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()
    ).decode()
    headers = {
        "ACCESS-KEY": API_KEY, "ACCESS-SIGN": sign, "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS, "Content-Type": "application/json", "locale": "en-US",
    }
    r = requests.get(BASE_URL + request_path, headers=headers, timeout=15)
    try:
        j = r.json()
    except ValueError:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
    if j.get("code") != "00000":
        raise RuntimeError(f"Bitget {j.get('code')}: {j.get('msg')}")
    return j["data"]


def fetch_quote(symbol=None):
    """Returns (bid, ask)."""
    data = api_get("/api/v3/cfd/market/tickers", {"symbol": symbol or SYMBOL})
    d = data[0] if isinstance(data, list) else data
    return float(d["bid1"]), float(d["ask1"])


def raw_candles(interval, minutes, count):
    now_ms = int(time.time() * 1000)
    params = {
        "symbol": SYMBOL, "interval": interval, "side": CANDLE_SIDE,
        "startTime": str(now_ms - (count + 2) * minutes * 60000), "limit": "100",
    }
    data = api_get("/api/v3/cfd/market/history-candlestick", params)
    rows = [[int(r[0])] + [float(x) for x in r[1:5]] for r in data]   # [ts, o, h, l, c]
    return sorted(rows, key=lambda r: r[0])

def fetch_candles(tf):
    global LAST_30M_MODE
    now_ms = time.time() * 1000
    tf_ms = tf["minutes"] * 60000

    if tf["name"] == "30M":
        out = []
        try:
            out = raw_candles("30m", 30, CANDLE_LOOKBACK)      # try native 30m first
            mode = "native 30m"
        except Exception:
            out = []
        if not out:                                            # fall back: build from 15M
            mode = "built from 15m"
            rows = raw_candles("15m", 15, 90)
            by_ts = {r[0]: r for r in rows}
            for ts in sorted(by_ts):
                if ts % 1800000 == 0 and ts + 900000 in by_ts:
                    a, b = by_ts[ts], by_ts[ts + 900000]
                    out.append([ts, a[1], max(a[2], b[2]), min(a[3], b[3]), b[4]])
        if mode != LAST_30M_MODE:
            log(f"[SOURCE] 30M candles: {mode}")
            LAST_30M_MODE = mode
    else:
        out = raw_candles(tf["tf"], tf["minutes"], CANDLE_LOOKBACK)

    closed = [c for c in out if c[0] + tf_ms <= now_ms]      # drop the still-forming candle
    return closed[-CANDLE_LOOKBACK:]

def pick_source():
    global SYMBOL
    working = []
    for sym in SYMBOL_CANDIDATES:
        try:
            bid, ask = fetch_quote(sym)
            log(f"[SOURCE] {sym}: bid {bid:.2f} ask {ask:.2f} spread {ask - bid:.2f}")
            working.append(sym)
        except Exception as e:
            log(f"[SOURCE] {sym} failed: {type(e).__name__}: {str(e)[:150]}")

    SYMBOL = os.environ.get("CFD_SYMBOL") or (working[0] if working else None)
    if SYMBOL is None:
        return False
    try:
        c = fetch_candles(TF_BY_NAME["15M"])
        if not c:
            log(f"[SOURCE] {SYMBOL}: no candles returned")
            return False
        when = datetime.fromtimestamp(c[-1][0] / 1000, timezone.utc).strftime("%m-%d %H:%M")
        log(f"[SOURCE] Using {SYMBOL}. Latest closed 15M candle {when} UTC, close {c[-1][4]:.2f}")
        return True
    except Exception as e:
        log(f"[SOURCE] candle test failed for {SYMBOL}: {type(e).__name__}: {str(e)[:150]}")
        return False


# ---------------- STRATEGY (sell-only) ----------------
def scan(tf):
    """True = scan finished (signal or not). False = data problem, retry inside the window."""
    name = tf["name"]
    if open_trades(name):
        log(f"{name} scan skipped: trade open")
        return True

    try:
        candles = fetch_candles(tf)
    except Exception as e:
        log(f"{name} candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False
    if len(candles) < RUNUP_WINDOW + 4:
        log(f"{name} scan skipped: only {len(candles)} candles returned")
        return False

    tf_ms = tf["minutes"] * 60000
    closes = [c[4] for c in candles]
    c1, c2 = len(candles) - 2, len(candles) - 1
    ts = candles[c2][0]
    when = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%m-%d %H:%M")
    age = (time.time() * 1000 - (ts + tf_ms)) / 60000
    tag = f"{name} [{when}]"

    if age > tf["max_age"]:
        log(f"{tag} skipped: candle closed {age:.0f} min ago, too old")
        return True
    key = f"{name}-{ts}"
    if signaled(key):
        log(f"{tag} skipped: already signaled")
        return True
    if candles[c2][0] - candles[c1][0] != tf_ms:
        log(f"{tag} skipped: candles 1 and 2 are not consecutive (market break)")
        return True

    close1, open2, close2 = candles[c1][4], candles[c2][1], candles[c2][4]
    prior_high = max(closes[c1 - tf["top_lookback"]:c1])
    runup = close1 - min(closes[c1 - RUNUP_WINDOW:c1])
    gap = abs(close1 - open2)
    lowest_prev = min(closes[c2 - RUNUP_WINDOW:c2])

    if close1 <= prior_high:
        log(f"{tag} no signal: close1 {close1:.2f} not above prior {tf['top_lookback']} ({prior_high:.2f})")
        return True
    if runup < tf["min_runup"]:
        log(f"{tag} no signal: run-up {runup:.2f} below {tf['min_runup']}")
        return True
    if gap > tf["tolerance"]:
        log(f"{tag} no signal: gap {gap:.2f} above {tf['tolerance']}")
        return True
    if close2 < lowest_prev:
        log(f"{tag} no signal: candle 2 close {close2:.2f} is the lowest of the last {RUNUP_WINDOW} ({lowest_prev:.2f})")
        return True

    try:
        entry, _ask = fetch_quote()          # a sell enters at the bid
    except Exception as e:
        log(f"{tag} pattern matched but quote fetch failed: {e}")
        return False
    open_new_trade(tf, entry, ts)
    log_signal(key)
    log(f"{tag} SIGNAL FIRED @ {entry:.2f} (gap {gap:.2f})")
    return True


def open_new_trade(tf, entry, signal_time):
    sl = entry + tf["sl"]
    tp1 = entry - tf["tps"][0]
    tp2 = entry - tf["tps"][1] if len(tf["tps"]) > 1 else None
    trade_id = insert_trade(tf["name"], entry, sl, tp1, tp2, signal_time)
    tp_line = f"TP: `{tp1:.2f}`" if tp2 is None else f"TP1: `{tp1:.2f}` | TP2: `{tp2:.2f}`"
    send_telegram(
        f"🟡 *GOLD {tf['label']} — SELL* (#{trade_id})\n"
        f"Entry: `{entry:.2f}`\n"
        f"SL: `{sl:.2f}`\n"
        f"{tp_line}"
    )


# ---------------- MONITOR (silent in the log) ----------------
def monitor():
    trades = open_trades()
    if not trades:
        return
    try:
        _bid, price = fetch_quote()          # a sell closes at the ask
    except Exception as e:
        log(f"monitor quote fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return

    for t in trades:
        name, tid = t["timeframe"], t["id"]
        hit_sl = price >= t["sl"]

        if t["tp2"] is None:                                   # single-TP (15M)
            if hit_sl:
                send_telegram(f"🔴 *GOLD {name} Trade #{tid} Closed — Stop Loss*\n"
                              f"SELL entry {t['entry']:.2f} → exit {price:.2f}")
                close_trade(tid, "SL")
            elif price <= t["tp1"]:
                send_telegram(f"🟢 *GOLD {name} TP Hit — Trade #{tid} Closed*\n"
                              f"SELL entry {t['entry']:.2f} → exit {price:.2f}")
                close_trade(tid, "TP")
            continue

        if hit_sl:                                             # two-TP (30M, 1H)
            outcome = "BREAKEVEN" if t["tp1_hit"] else "SL"
            label = "Breakeven (SL moved after TP1)" if t["tp1_hit"] else "Stop Loss"
            send_telegram(f"🔴 *GOLD {name} Trade #{tid} Closed — {label}*\n"
                          f"SELL entry {t['entry']:.2f} → exit {price:.2f}")
            close_trade(tid, outcome)
        elif (not t["tp1_hit"]) and price <= t["tp1"]:
            tp1_hit(tid, t["entry"])
            send_telegram(f"🟢 *GOLD {name} TP1 Hit* — Trade #{tid} entry {t['entry']:.2f} → {price:.2f}\n"
                          f"SL moved to breakeven. Now targeting TP2 ({t['tp2']:.2f})")
        elif t["tp1_hit"] and price <= t["tp2"]:
            send_telegram(f"🟢🟢 *GOLD {name} TP2 Hit — Trade #{tid} Closed*\n"
                          f"SELL entry {t['entry']:.2f} → exit {price:.2f}")
            close_trade(tid, "TP2")


# ---------------- WEEKLY SUMMARY ----------------
def weekly_summary():
    now = datetime.now(timezone.utc)
    last = get_meta("last_summary_sent")
    if last is None:
        set_meta("last_summary_sent", now.isoformat())
        return
    since = datetime.fromisoformat(last)
    if now - since < timedelta(days=7):
        return

    s15, s30, s1h = stats(since, "15M"), stats(since, "30M"), stats(since, "1H")
    send_telegram(
        f"📊 *Weekly Summary*\n\n"
        f"*GOLD 15M SCALPING*\nClosed: {s15['total']} | TP: {s15['tp']} | SL: {s15['sl']}\n\n"
        f"*GOLD 30M SCALPING*\nClosed: {s30['total']} | TP2: {s30['tp2']} | "
        f"Breakeven: {s30['be']} | SL: {s30['sl']}\n\n"
        f"*GOLD 1H*\nClosed: {s1h['total']} | TP2: {s1h['tp2']} | "
        f"Breakeven: {s1h['be']} | SL: {s1h['sl']}"
    )
    set_meta("last_summary_sent", now.isoformat())


# ---------------- MAIN LOOP ----------------
def main():
    global TOKEN, CHAT, DB_PATH, COMMANDS_ENABLED, API_KEY, API_SECRET, API_PASS
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN_EXCHANGE") or os.environ["TELEGRAM_BOT_TOKEN"]
    CHAT = os.environ["TELEGRAM_CHAT_ID_EXCHANGE"]
    DB_PATH = os.environ.get("DB_PATH_EXCHANGE", "gold_exchange.db")
    API_KEY = os.environ["BITGET_API_KEY"]
    API_SECRET = os.environ["BITGET_API_SECRET"]
    API_PASS = os.environ["BITGET_API_PASSPHRASE"]
    COMMANDS_ENABLED = bool(os.environ.get("TELEGRAM_BOT_TOKEN_EXCHANGE")) and \
        os.environ.get("TELEGRAM_BOT_TOKEN_EXCHANGE") != os.environ.get("TELEGRAM_BOT_TOKEN")
    init_db()

    while not pick_source():
        log("No data source reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ Gold CFD bot started (1H + 30M + 15M)\nData source: Bitget CFD {SYMBOL}")
    log("CFD bot running: scanning 15M, 30M, 1H")

    last_scan = {t["name"]: None for t in TIMEFRAMES}
    last_summary = 0

    while True:
        try:
            now = datetime.now(timezone.utc)
            for tf in TIMEFRAMES:
                into = (now.hour * 60 + now.minute) % tf["minutes"]
                boundary = (now - timedelta(minutes=into)).strftime("%Y-%m-%d-%H-%M")
                if boundary != last_scan[tf["name"]] and into < tf["scan_window"]:
                    if scan(tf):
                        last_scan[tf["name"]] = boundary
            monitor()
            check_commands()
            if time.time() - last_summary >= SUMMARY_CHECK_INTERVAL_SECONDS:
                weekly_summary()
                last_summary = time.time()
        except Exception as e:
            log(f"Loop error: {type(e).__name__}: {e}")
        time.sleep(MONITOR_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
