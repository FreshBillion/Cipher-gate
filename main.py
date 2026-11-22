# ---- Gold CFD Bot, SELL-ONLY (Bitget CFD data): 1H + 30M + 15M ----
# Two paper streams per timeframe:
#   BASELINE = your original rules, takes every signal. SILENT (weekly summary only).
#   MAIN     = original rules + STEP RULE + STOP-LOSS BLOCK. Only MAIN sends Telegram messages.
# Same entry / SL / TP for both streams. One trade at a time per stream per timeframe.
#
# Env vars:
#   BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE   (READ-ONLY key)
#   CFD_SYMBOL                    (optional: XAUUSD, XAUUSD.s or XAUUSD.pro)
#   TELEGRAM_CHAT_ID_EXCHANGE     (required)
#   TELEGRAM_BOT_TOKEN_EXCHANGE   (optional, defaults to TELEGRAM_BOT_TOKEN)
#   DB_PATH_EXCHANGE              (optional, e.g. /data/gold_exchange.db)
# requirements.txt: requests

import os
import time
import hmac
import hashlib
import base64
import sqlite3
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

# ================= NEW RULE SETTINGS =================
STEP_WINDOW = 5                                        # step rule: look at this many candles right before c1
BLOCK_LIFT_CANDLES = {"15M": 2, "30M": 1, "1H": 1}     # straight falling candles after a stop-loss that lift the block
RESET_GAP = 0.10                                       # reset pair: c2 open must be within this of c1 close
RESET_CLOSE_BACK = 0.30                                # reset pair: c2 must close at least this far below c1 close
EPS = 1e-9                                             # float safety for the two values above
# =====================================================

BASE_URL = "https://api.bitget.com"
SYMBOL_CANDIDATES = ["XAUUSD", "XAUUSD.s", "XAUUSD.pro"]
CANDLE_SIDE = "sell"          # bid-based candles, because signals are sells

CANDLE_LOOKBACK = 30
RUNUP_WINDOW = 12
MONITOR_INTERVAL_SECONDS = 20
SUMMARY_CHECK_INTERVAL_SECONDS = 3600
DIVIDER = "━━━━━━━━━━━━"

TIMEFRAMES = [
    {"name": "15M", "label": "15M SCALPING", "tf": "15m", "minutes": 15,
     "min_runup": 20, "top_lookback": 20, "tolerance": 0.10, "min_close_back": 0.70,
     "sl": 10, "tps": [10], "scan_window": 2, "max_age": 4},
    {"name": "30M", "label": "30M SCALPING", "tf": "30m", "minutes": 30,
     "min_runup": 30, "top_lookback": 10, "tolerance": 0.20, "min_close_back": 0.70,
     "sl": 10, "tps": [10, 20], "scan_window": 3, "max_age": 6},
    {"name": "1H", "label": "1H", "tf": "1h", "minutes": 60,
     "min_runup": 40, "top_lookback": 5, "tolerance": 0.30, "min_close_back": 0.70,
     "sl": 10, "tps": [15, 25], "scan_window": 5, "max_age": 10},
]
TF_BY_NAME = {t["name"]: t for t in TIMEFRAMES}

TOKEN = CHAT = DB_PATH = None
API_KEY = API_SECRET = API_PASS = None
SYMBOL = None
COMMANDS_ENABLED = False
LAST_30M_MODE = None


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
            outcome TEXT, signal_time TEXT, opened_at TEXT, closed_at TEXT,
            msg_id INTEGER, variant TEXT DEFAULT 'BASELINE')""")
    for col in ("msg_id INTEGER", "variant TEXT DEFAULT 'BASELINE'"):   # upgrade an older database
        try:
            db(f"ALTER TABLE trades ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass                                                         # column already exists
    db("CREATE TABLE IF NOT EXISTS signal_log (signal_key TEXT PRIMARY KEY)")
    db("CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value TEXT)")


def get_meta(key):
    rows = db("SELECT value FROM bot_meta WHERE key = ?", (key,), fetch=True)
    return rows[0]["value"] if rows else None


def set_meta(key, value):
    db("INSERT OR REPLACE INTO bot_meta (key, value) VALUES (?, ?)", (key, str(value)))


def bump(key):
    set_meta(key, int(get_meta(key) or 0) + 1)


def signaled(key):
    return bool(db("SELECT 1 FROM signal_log WHERE signal_key = ?", (key,), fetch=True))


def log_signal(key):
    db("INSERT OR IGNORE INTO signal_log (signal_key) VALUES (?)", (key,))


def insert_trade(variant, tf, entry, sl, tp1, tp2, signal_time):
    return db("""INSERT INTO trades (variant, timeframe, entry, sl, tp1, tp2, signal_time, opened_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (variant, tf, entry, sl, tp1, tp2, str(signal_time), datetime.now(timezone.utc).isoformat()))


def set_msg_id(trade_id, msg_id):
    db("UPDATE trades SET msg_id=? WHERE id=?", (msg_id, trade_id))


def open_trades(tf=None, variant=None):
    sql, params = "SELECT * FROM trades WHERE status='open'", []
    if tf:
        sql += " AND timeframe=?"
        params.append(tf)
    if variant:
        sql += " AND variant=?"
        params.append(variant)
    return db(sql, tuple(params), fetch=True)


def close_trade(trade_id, outcome):
    db("UPDATE trades SET status='closed', outcome=?, closed_at=? WHERE id=?",
       (outcome, datetime.now(timezone.utc).isoformat(), trade_id))


def tp1_hit(trade_id, new_sl):
    db("UPDATE trades SET tp1_hit=1, sl=? WHERE id=?", (new_sl, trade_id))


def stats(since, tf, variant):
    rows = db("SELECT outcome FROM trades WHERE timeframe=? AND variant=? AND closed_at>=?",
              (tf, variant, since.isoformat()), fetch=True)
    o = [r["outcome"] for r in rows]
    return {"total": len(o), "tp": o.count("TP"), "tp2": o.count("TP2"),
            "be": o.count("BREAKEVEN"), "sl": o.count("SL")}


# ---------------- STOP-LOSS BLOCK STATE (saved in the database) ----------------
def blk_key(name, field):
    return f"blk_sell_{field}_{name}"


def blk_get(name):
    """Returns (blocked, count, since_ms, last_counted_candle_open_ms)."""
    return (get_meta(blk_key(name, "on")) == "1",
            int(get_meta(blk_key(name, "count")) or 0),
            int(get_meta(blk_key(name, "since")) or 0),
            int(get_meta(blk_key(name, "last")) or 0))


def blk_set(name, on, count, since, last):
    set_meta(blk_key(name, "on"), "1" if on else "0")
    set_meta(blk_key(name, "count"), count)
    set_meta(blk_key(name, "since"), since)
    set_meta(blk_key(name, "last"), last)


# ---------------- TELEGRAM ----------------
def send_telegram(message, reply_to=None):
    """Sends a message. Returns the Telegram message id (or None)."""
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT, "text": message, "parse_mode": "Markdown"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    try:
        r = requests.post(url, data=payload, timeout=10)
        return (r.json().get("result") or {}).get("message_id")
    except Exception as e:
        log(f"[Telegram error] {e}")
        return None


def notify(t, message):
    """Only MAIN trades send Telegram messages. BASELINE trades stay silent."""
    if t.get("variant") == "MAIN":
        send_telegram(message, reply_to=t.get("msg_id"))


def money(x):
    return f"{'+' if x >= 0 else '-'}${abs(x):.2f}"


def tag(t):
    """e.g. GOLD 30M · SELL · #12"""
    return f"📊 *GOLD {t['timeframe']}* · SELL · #{t['id']}"


def result_line(t, price):
    return f"Entry `{t['entry']:.2f}` → Exit `{price:.2f}`\nResult: *{money(t['entry'] - price)}*"


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
        rows = db("SELECT * FROM trades WHERE id=? AND status='open'", (tid,), fetch=True)
        if not rows:
            send_telegram(f"Trade #{tid} not found or already closed.")
            continue
        close_trade(tid, "MANUAL_CLOSE")
        notify(rows[0], f"⚪ *MANUALLY CLOSED*\n{tag(rows[0])}")


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
        mode = "native 30m"
        try:
            out = raw_candles("30m", 30, CANDLE_LOOKBACK)      # try native 30m first
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
def step_filter(direction, candle1, candle2, min_close_back):
    """ORIGINAL rule 6 check (unchanged). Returns (ok, reason). Candles are [ts, open, high, low, close]."""
    open1, close1 = candle1[1], candle1[4]
    close2 = candle2[4]
    if direction == "sell":
        if close1 <= open1:
            return False, f"c1 not bullish (open {open1:.2f} close {close1:.2f})"
        back = close1 - close2
    else:
        if close1 >= open1:
            return False, f"c1 not bearish (open {open1:.2f} close {close1:.2f})"
        back = close2 - close1
    if back < min_close_back:
        return False, f"c2 closed only {back:.2f} back past c1 close, need {min_close_back}"
    return True, f"step ok, back {back:.2f}"


def evaluate(tf, candles):
    """Your ORIGINAL sell rules (unchanged logic). candles = closed candles, last one is c2.
    Returns (fired, reason)."""
    closes = [c[4] for c in candles]
    c1, c2 = len(candles) - 2, len(candles) - 1
    close1, open2, close2 = candles[c1][4], candles[c2][1], candles[c2][4]
    prior_high = max(closes[c1 - tf["top_lookback"]:c1])
    runup = close1 - min(closes[c1 - RUNUP_WINDOW:c1])
    gap = abs(close1 - open2)
    lowest_prev = min(closes[c2 - RUNUP_WINDOW:c2])

    if close1 <= prior_high:
        return False, f"close1 {close1:.2f} not above prior {tf['top_lookback']} ({prior_high:.2f})"
    if runup < tf["min_runup"]:
        return False, f"run-up {runup:.2f} below {tf['min_runup']}"
    if gap > tf["tolerance"]:
        return False, f"gap {gap:.2f} above {tf['tolerance']}"
    if close2 < lowest_prev:
        return False, f"candle 2 close {close2:.2f} is the lowest of the last {RUNUP_WINDOW} ({lowest_prev:.2f})"
    ok, why = step_filter("sell", candles[c1], candles[c2], tf["min_close_back"])
    if not ok:
        return False, f"step filter: {why}"
    return True, f"gap {gap:.2f}, {why}"


# ---------------- NEW RULE 1: STEP RULE ----------------
def step_rule_ok(candles):
    """Among the STEP_WINDOW candles right before c1 there must be two NEIGHBOURING bullish candles
    where the second closes higher than the first (anywhere inside the window)."""
    c1 = len(candles) - 2
    pre = candles[max(0, c1 - STEP_WINDOW):c1]
    for a, b in zip(pre, pre[1:]):
        if a[4] > a[1] and b[4] > b[1] and b[4] > a[4]:
            return True
    return False


# ---------------- NEW RULE 2: STOP-LOSS BLOCK ----------------
def reset_pair(tf, candles):
    """Last two candles look like a sell signal pair: c1 bullish, c2 opens within RESET_GAP of c1's
    close and closes at least RESET_CLOSE_BACK below it. Nothing else is checked."""
    if len(candles) < 2:
        return False
    c1, c2 = candles[-2], candles[-1]
    if c2[0] - c1[0] != tf["minutes"] * 60000:
        return False
    if abs(c1[4] - c2[1]) > RESET_GAP + EPS:
        return False
    return c1[4] > c1[1] and (c1[4] - c2[4]) >= RESET_CLOSE_BACK - EPS


def start_block(name):
    """Called when a MAIN trade hits its stop-loss (a real SL, not breakeven)."""
    blk_set(name, True, 0, int(time.time() * 1000), 0)
    log(f"{name} sell block started (needs {BLOCK_LIFT_CANDLES[name]} straight falling candles)")


def update_block(tf, candles):
    """Runs once per closed candle (candles[-1] = the candle that just closed).
    Returns True if the sell block is still on after this candle."""
    name, tf_ms = tf["name"], tf["minutes"] * 60000
    on, count, since, last = blk_get(name)
    if not on:
        return False
    open_ts = candles[-1][0]
    if last == open_ts or open_ts + tf_ms <= since:     # already counted, or closed before the stop-loss
        return True

    if (open_ts - last > tf_ms) if last else (open_ts > since):
        count, why = 0, "candle missed"                 # the candle does not count
    elif reset_pair(tf, candles):
        count, why = 0, "reset pair"                    # the triggering candle does not count
    elif candles[-1][4] < candles[-1][1]:
        count, why = count + 1, "falling"
    else:
        count, why = 0, "not falling"

    need = BLOCK_LIFT_CANDLES[name]
    if count >= need:
        blk_set(name, False, 0, 0, 0)
        log(f"{name} sell block lifted after {need} straight falling candle(s)")
        return False
    blk_set(name, True, count, since, open_ts)
    log(f"{name} sell block: {count}/{need} ({why})")
    return True


# ---------------- SCAN ----------------
def scan(tf):
    """True = scan finished (signal or not). False = data problem, retry inside the window."""
    name = tf["name"]
    try:
        candles = fetch_candles(tf)
    except Exception as e:
        log(f"{name} candle fetch failed: {type(e).__name__}: {str(e)[:150]}")
        return False
    if len(candles) < RUNUP_WINDOW + 4:
        log(f"{name} scan skipped: only {len(candles)} candles returned")
        return False

    tf_ms = tf["minutes"] * 60000
    c1, c2 = len(candles) - 2, len(candles) - 1
    ts = candles[c2][0]
    when = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%m-%d %H:%M")
    age = (time.time() * 1000 - (ts + tf_ms)) / 60000
    tag_ = f"{name} [{when}]"

    if age > tf["max_age"]:
        log(f"{tag_} skipped: candle closed {age:.0f} min ago, too old")
        return True

    blocked = update_block(tf, candles)                 # count this candle toward the block (MAIN only uses it)

    bkey, mkey = f"{name}-{ts}", f"M-{name}-{ts}"
    base_todo = not open_trades(name, "BASELINE") and not signaled(bkey)
    main_todo = not open_trades(name, "MAIN") and not signaled(mkey)
    if not (base_todo or main_todo):
        log(f"{tag_} scan skipped: trades open or already signaled")
        return True
    if candles[c2][0] - candles[c1][0] != tf_ms:
        log(f"{tag_} skipped: candles 1 and 2 are not consecutive (market break)")
        return True

    fired, why = evaluate(tf, candles)
    if not fired:
        log(f"{tag_} no signal: {why}")
        return True

    plan = []
    if base_todo:
        plan.append("BASELINE")
    if main_todo:
        reason = None
        if blocked:
            reason = "block"
        elif not step_rule_ok(candles):
            reason = "step"
        else:
            plan.append("MAIN")
        if reason:                                      # count each skipped signal once
            log_signal(mkey)
            bump(f"skip_{name}_{reason}")
            log(f"{tag_} MAIN signal skipped by the {reason} rule")
    if not plan:
        return True

    try:
        entry, _ask = fetch_quote()                     # a sell enters at the bid
    except Exception as e:
        log(f"{tag_} pattern matched but quote fetch failed: {e}")
        return False
    for variant in plan:
        open_new_trade(tf, entry, ts, variant)
        log_signal(bkey if variant == "BASELINE" else mkey)
        log(f"{tag_} {variant} SIGNAL FIRED @ {entry:.2f} ({why})")
    return True


def open_new_trade(tf, entry, signal_time, variant):
    sl = entry + tf["sl"]
    tp1 = entry - tf["tps"][0]
    tp2 = entry - tf["tps"][1] if len(tf["tps"]) > 1 else None
    trade_id = insert_trade(variant, tf["name"], entry, sl, tp1, tp2, signal_time)
    if variant != "MAIN":
        return trade_id                                 # BASELINE is silent

    lines = [
        "♦️💲 *SELL SIGNAL*",
        DIVIDER,
        f"📊 *GOLD · {tf['label']}*",
        f"🛡️ Trade #{trade_id}",
        "",
        f"🎲 Entry: `{entry:.2f}`",
        f"🚩 SL: `{sl:.2f}`",
    ]
    if tp2 is None:
        lines.append(f"🏦 TP: `{tp1:.2f}`")
    else:
        lines += [f"⏳ TP1: `{tp1:.2f}`", f"🏛️ TP2: `{tp2:.2f}`"]

    msg_id = send_telegram("\n".join(lines))
    if msg_id:
        set_msg_id(trade_id, msg_id)
    return trade_id


# ---------------- MONITOR (silent in the log) ----------------
def on_stop_loss(t):
    if t.get("variant") == "MAIN":                      # only a real SL from the MAIN stream starts a block
        start_block(t["timeframe"])


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
        tid = t["id"]
        hit_sl = price >= t["sl"]
        res = result_line(t, price)

        if t["tp2"] is None:                                   # single-TP (15M)
            if hit_sl:
                notify(t, f"❌ *STOP LOSS HIT*\n{tag(t)}\n{res}")
                close_trade(tid, "SL")
                on_stop_loss(t)
            elif price <= t["tp1"]:
                notify(t, f"🏦 *TP HIT — TRADE CLOSED*\n{tag(t)}\n{res}")
                close_trade(tid, "TP")
            continue

        if hit_sl:                                             # two-TP (30M, 1H)
            if t["tp1_hit"]:
                notify(t, f"⚖️ *BREAKEVEN — TRADE CLOSED*\n{tag(t)}\n"
                          f"SL (moved to entry after TP1) was hit\n{res}")
                close_trade(tid, "BREAKEVEN")                  # breakeven does NOT start a block
            else:
                notify(t, f"🚩 *STOP LOSS HIT*\n{tag(t)}\n{res}")
                close_trade(tid, "SL")
                on_stop_loss(t)
        elif (not t["tp1_hit"]) and price <= t["tp1"]:
            tp1_hit(tid, t["entry"])
            notify(t, f"⏳ *TP1 HIT*\n{tag(t)}\n{res}\n\n"
                      f"⚖️ SL moved to breakeven\n🏛️ Next: TP2 `{t['tp2']:.2f}`")
        elif t["tp1_hit"] and price <= t["tp2"]:
            notify(t, f"🏛️🏦 *TP2 HIT — TRADE CLOSED*\n{tag(t)}\n{res}")
            close_trade(tid, "TP2")


# ---------------- WEEKLY SUMMARY ----------------
def stat_line(name, s):
    if name == "15M":
        return f"Closed: {s['total']}  |  🏦 TP: {s['tp']}  |  🚩 SL: {s['sl']}"
    return f"Closed: {s['total']}  |  🏛️🏦 TP2: {s['tp2']}  |  ⚖️ BE: {s['be']}  |  🚩 SL: {s['sl']}"


def weekly_summary():
    now = datetime.now(timezone.utc)
    last = get_meta("last_summary_sent")
    if last is None:
        set_meta("last_summary_sent", now.isoformat())
        return
    since = datetime.fromisoformat(last)
    if now - since < timedelta(days=7):
        return

    parts = [f"📊 *WEEKLY SUMMARY*\n{DIVIDER}"]
    for tf in TIMEFRAMES:
        n = tf["name"]
        parts.append(
            f"\n*GOLD · {tf['label']}*\n"
            f"New rules: {stat_line(n, stats(since, n, 'MAIN'))}\n"
            f"Baseline: {stat_line(n, stats(since, n, 'BASELINE'))}\n"
            f"Skipped: block {get_meta(f'skip_{n}_block') or 0}  |  step rule {get_meta(f'skip_{n}_step') or 0}")
        set_meta(f"skip_{n}_block", 0)
        set_meta(f"skip_{n}_step", 0)
    send_telegram("\n".join(parts))
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
    log(f"Database file: {DB_PATH}")

    while not pick_source():
        log("No data source reachable, retrying in 60s")
        time.sleep(60)

    send_telegram(f"✅ *Bot started* {SYMBOL}")
    log("CFD sell-only bot running: scanning 15M, 30M, 1H (BASELINE + MAIN streams)")

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
