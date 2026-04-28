"""
Nifty OI Bias Monitor — Kite WebSocket Bridge (Railway / PostgreSQL edition)
=============================================================================
  • Data is stored in PostgreSQL (DATABASE_URL from Railway env)
  • Flask serves index.html at GET / and charts.html at GET /charts
  • API_KEY / ACCESS_TOKEN come from environment variables (never hard-coded)

Railway setup
─────────────
1.  Add a Postgres plugin → DATABASE_URL is injected automatically.
2.  Set env vars:
        KITE_API_KEY
        KITE_ACCESS_TOKEN
3.  Deploy from GitHub — Railway will run:  python kite_oi_bridge.py

Endpoints:
    GET  /                 → serves the OI dashboard HTML
    GET  /charts           → serves the index charts HTML
    GET  /oi               → snapshot for ALL 11 strikes + active bear/bull
    GET  /ltp              → live LTP for all 22 tokens
    GET  /oi/history       → 1-min rows for today (all 11 strikes + OHLC)
    GET  /oi/live-candle   → currently forming 1-min candle
    GET  /strikes          → strike list
    GET  /health           → status + OHLC buffer
    POST /reset-csv        → wipe today's rows, start fresh
    GET  /index-ltp        → live LTP + forming candle for BankNifty/HDFC/ICICI
    GET  /index-candles    → 1-min OHLC for ?date=YYYY-MM-DD (DB-first, Kite fallback)
    GET  /index-dates      → list of dates with stored index candle data
"""

import collections
import datetime as dt
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def ts_to_ist(ts): return datetime.fromtimestamp(ts, tz=IST)

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from kiteconnect import KiteTicker, KiteConnect


# ── CONFIG ────────────────────────────────────────────────────────────────────

API_KEY      = os.environ["KITE_API_KEY"]
ACCESS_TOKEN = os.environ["KITE_ACCESS_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

NIFTY_TOKEN       = 256265
FLASK_PORT        = int(os.environ.get("PORT", 5000))
OI_HISTORY_MAXLEN = 500

NUM_STRIKES    = 11
STRIKE_STEP    = 50
ROLL_THRESHOLD = 100

# ── INDEX INSTRUMENTS (separate lightweight ticker, LTP-only) ─────────────────

BANKNIFTY_TOKEN = 260105    # NSE:NIFTY BANK
HDFC_TOKEN      = 341249    # NSE:HDFCBANK
ICICI_TOKEN     = 1270529   # NSE:ICICIBANK

INDEX_TOKENS = {
    BANKNIFTY_TOKEN: {"symbol": "NIFTY BANK", "label": "BankNifty"},
    HDFC_TOKEN:      {"symbol": "HDFCBANK",   "label": "HDFC Bank"},
    ICICI_TOKEN:     {"symbol": "ICICIBANK",  "label": "ICICI Bank"},
}


# ── APP ───────────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static")
CORS(app)

kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)


# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_db():
    """Return a new psycopg2 connection. Call .close() when done."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """Create the oi_history table if it doesn't exist."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS oi_history (
                    id          SERIAL PRIMARY KEY,
                    ts          DOUBLE PRECISION NOT NULL,
                    time_label  TEXT,
                    session_id  INTEGER,
                    trade_date  DATE DEFAULT CURRENT_DATE,
                    data        JSONB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_oi_history_date
                    ON oi_history (trade_date);
            """)
        conn.commit()
    print("DB initialised — table oi_history ready.")


def init_index_db():
    """Create index_candles table if it doesn't exist."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS index_candles (
                    id          SERIAL PRIMARY KEY,
                    trade_date  DATE             NOT NULL DEFAULT CURRENT_DATE,
                    token       INTEGER          NOT NULL,
                    symbol      TEXT             NOT NULL,
                    time_label  TEXT             NOT NULL,
                    ts          DOUBLE PRECISION NOT NULL,
                    open        NUMERIC(12, 2),
                    high        NUMERIC(12, 2),
                    low         NUMERIC(12, 2),
                    close       NUMERIC(12, 2)
                );
                CREATE INDEX IF NOT EXISTS idx_ic_date_token
                    ON index_candles (trade_date, token);
            """)
        conn.commit()
    print("DB initialised — table index_candles ready.")


def db_write_row(row: dict):
    """Insert one 1-min completed OI candle row into PostgreSQL."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO oi_history (ts, time_label, session_id, trade_date, data)
                VALUES (%s, %s, %s, CURRENT_DATE, %s)
                """,
                (
                    row.get("ts"),
                    row.get("time_label"),
                    row.get("session_id"),
                    json.dumps(row),
                ),
            )
        conn.commit()


def db_write_index_candle(token: int, symbol: str, ts: float,
                           o: float, h: float, l: float, c: float):
    """Insert one completed 1-min index candle into PostgreSQL."""
    label = ts_to_ist(ts).strftime("%H:%M")
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO index_candles
                        (trade_date, token, symbol, time_label, ts, open, high, low, close)
                    VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (token, symbol, label, ts, o, h, l, c),
                )
            conn.commit()
    except Exception as e:
        print(f"[index_candles] DB write error: {e}")


def db_read_today() -> list:
    """Return all 1-min OI rows for today as a list of dicts."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT data FROM oi_history WHERE trade_date = CURRENT_DATE ORDER BY ts ASC"
            )
            rows = [dict(r["data"]) for r in cur.fetchall()]
    return rows


def db_read_index_candles(date_str: str) -> list:
    """Return all 1-min index candles for a given date (YYYY-MM-DD)."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT token, symbol, time_label, ts, open, high, low, close
                FROM   index_candles
                WHERE  trade_date = %s
                ORDER  BY token, ts ASC
                """,
                (date_str,),
            )
            return [dict(r) for r in cur.fetchall()]


def db_read_index_dates() -> list:
    """Return distinct trade dates that have any index candle data."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM index_candles ORDER BY trade_date DESC"
            )
            return [str(r[0]) for r in cur.fetchall()]


def db_reset_today():
    """Delete all OI rows for today."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM oi_history WHERE trade_date = CURRENT_DATE")
        conn.commit()


# ── STATE ─────────────────────────────────────────────────────────────────────

state = {
    "spot":           None,
    "last_anchored":  None,
    "atm_strike":     None,
    "strikes":        [],
    "bear_strike":    None,
    "bull_strike":    None,
    "session_id":     0,
    "tokens":         {},
    "oi":             {},
    "oi_baseline":    {},
    "oi_prev_snap":   {},
    "roll_log":       [],
}

minute_buffer = {
    "start_ts":   None,
    "start_snap": None,
    "spot_open":  None,
    "spot_high":  None,
    "spot_low":   None,
    "spot_close": None,
    "ltp_ohlc":   {},
}

state_lock = threading.Lock()
oi_history = collections.deque(maxlen=OI_HISTORY_MAXLEN)

# ── INDEX STATE ───────────────────────────────────────────────────────────────

# Live tick state — updated on every WebSocket tick from index ticker
index_tick_state = {
    token: {"ltp": None, "ts": None} for token in INDEX_TOKENS
}
index_tick_lock = threading.Lock()

# Per-minute OHLC buffer — sealed and written to DB when minute rolls over
index_minute_buf = {
    token: {"open": None, "high": None, "low": None, "close": None, "start_ts": None}
    for token in INDEX_TOKENS
}
index_buf_lock = threading.Lock()


# ── INSTRUMENT HELPERS ────────────────────────────────────────────────────────

def round_to_nearest_50(price):
    return round(price / 50) * 50


def compute_strikes_window(atm):
    half = NUM_STRIKES // 2
    return [atm + (i - half) * STRIKE_STEP for i in range(NUM_STRIKES)]


def compute_bear_bull(spot):
    base = round_to_nearest_50(spot)
    return base - 100, base + 100


def get_instruments_for_strikes(strikes):
    instruments = kite.instruments("NFO")
    nifty_opts = [
        i for i in instruments
        if i["name"] == "NIFTY" and i["instrument_type"] in ("CE", "PE")
    ]
    expiries = sorted(set(i["expiry"] for i in nifty_opts))
    front = expiries[0]
    strike_set = set(strikes)
    result = []
    for i in nifty_opts:
        if i["expiry"] != front or i["strike"] not in strike_set:
            continue
        k = str(int(i["strike"]))
        t = i["instrument_type"].lower()
        i["role_key"] = f"s{k}_{t}"
        result.append(i)
    found = {i["strike"] for i in result}
    missing = strike_set - found
    if missing:
        print(f"  Warning: strikes not found in NFO: {sorted(missing)}")
    return result


def get_live_spot():
    quote = kite.quote("NSE:NIFTY 50")
    return quote["NSE:NIFTY 50"]["last_price"]


# ── SNAPSHOT ──────────────────────────────────────────────────────────────────

def _current_snap(ts):
    snap = {"ts": ts, "spot": state["spot"]}
    for token, meta in state["tokens"].items():
        entry = state["oi"].get(token, {})
        rk = meta["role_key"]
        snap[rk + "_oi"]       = entry.get("oi", 0)
        snap[rk + "_ltp"]      = entry.get("ltp", 0)
        snap[rk + "_baseline"] = state["oi_baseline"].get(token, 0)
    return snap


# ── SPOT / LTP OHLC ───────────────────────────────────────────────────────────

def _update_spot_ohlc(spot):
    if spot is None:
        return
    if minute_buffer["spot_open"] is None:
        minute_buffer["spot_open"]  = spot
        minute_buffer["spot_high"]  = spot
        minute_buffer["spot_low"]   = spot
    else:
        if spot > minute_buffer["spot_high"]:
            minute_buffer["spot_high"] = spot
        if spot < minute_buffer["spot_low"]:
            minute_buffer["spot_low"] = spot
    minute_buffer["spot_close"] = spot


def _update_ltp_ohlc(token, ltp):
    if ltp is None or ltp == 0:
        return
    buf = minute_buffer["ltp_ohlc"]
    if token not in buf:
        buf[token] = {"open": ltp, "high": ltp, "low": ltp, "close": ltp}
    else:
        if ltp > buf[token]["high"]:
            buf[token]["high"] = ltp
        if ltp < buf[token]["low"]:
            buf[token]["low"] = ltp
        buf[token]["close"] = ltp


# ── 1-MINUTE OI AGGREGATOR ────────────────────────────────────────────────────

def _append_history():
    now          = time.time()
    current_snap = _current_snap(now)

    if minute_buffer["start_ts"] is None:
        minute_buffer["start_ts"]   = now
        minute_buffer["start_snap"] = current_snap
        return

    if now - minute_buffer["start_ts"] < 60:
        return

    start = minute_buffer["start_snap"]
    close = current_snap

    row = {
        "ts":          minute_buffer["start_ts"],
        "time_label":  ts_to_ist(minute_buffer["start_ts"]).strftime("%H:%M"),
        "session_id":  state["session_id"],
        "bear_strike": state["bear_strike"],
        "bull_strike": state["bull_strike"],
        "spot_open":   round(minute_buffer["spot_open"])  if minute_buffer["spot_open"]  else 0,
        "spot_high":   round(minute_buffer["spot_high"])  if minute_buffer["spot_high"]  else 0,
        "spot_low":    round(minute_buffer["spot_low"])   if minute_buffer["spot_low"]   else 0,
        "spot_close":  round(minute_buffer["spot_close"]) if minute_buffer["spot_close"] else 0,
    }

    for token, meta in state["tokens"].items():
        rk = meta["role_key"]
        close_ltp = close.get(rk + "_ltp", 0)
        ohlc = minute_buffer["ltp_ohlc"].get(token, {})
        row[rk + "_oi"]        = close.get(rk + "_oi", 0)
        row[rk + "_ltp"]       = close_ltp
        row[rk + "_ltp_open"]  = ohlc.get("open",  close_ltp)
        row[rk + "_ltp_high"]  = ohlc.get("high",  close_ltp)
        row[rk + "_ltp_low"]   = ohlc.get("low",   close_ltp)
        row[rk + "_ltp_close"] = ohlc.get("close", close_ltp)
        row[rk + "_baseline"]  = close.get(rk + "_baseline", 0)
        row[rk + "_delta"]     = close.get(rk + "_oi", 0) - start.get(rk + "_oi", 0)

    threading.Thread(target=db_write_row, args=(row,), daemon=True).start()
    oi_history.append(row)

    print(
        f"[{row['time_label']}] "
        f"O={row['spot_open']} H={row['spot_high']} L={row['spot_low']} C={row['spot_close']}  "
        f"bear={row['bear_strike']} bull={row['bull_strike']} session={row['session_id']}"
    )

    minute_buffer["start_ts"]   = now
    minute_buffer["start_snap"] = current_snap
    minute_buffer["spot_open"]  = minute_buffer["spot_close"]
    minute_buffer["spot_high"]  = minute_buffer["spot_close"]
    minute_buffer["spot_low"]   = minute_buffer["spot_close"]
    new_ltp_ohlc = {}
    for token in minute_buffer["ltp_ohlc"]:
        last_close = minute_buffer["ltp_ohlc"][token]["close"]
        new_ltp_ohlc[token] = {"open": last_close, "high": last_close, "low": last_close, "close": last_close}
    minute_buffer["ltp_ohlc"] = new_ltp_ohlc


# ── INDEX TICK HANDLER ────────────────────────────────────────────────────────

def handle_index_tick(tick: dict):
    """Update live LTP state and per-minute OHLC buffer for index instruments."""
    token = tick.get("instrument_token")
    if token not in INDEX_TOKENS:
        return

    ltp = tick.get("last_price")
    if ltp is None:
        return

    ts = tick.get("timestamp")
    if ts is None:
        ts = time.time()
    elif hasattr(ts, "timestamp"):
        ts = ts.timestamp()

    # Update live tick
    with index_tick_lock:
        index_tick_state[token]["ltp"] = ltp
        index_tick_state[token]["ts"]  = ts

    # Update per-minute OHLC; seal completed candle when minute rolls over
    with index_buf_lock:
        buf       = index_minute_buf[token]
        dt_ist    = ts_to_ist(ts)
        bucket_ts = dt_ist.replace(second=0, microsecond=0).timestamp()

        if buf["start_ts"] is None:
            buf.update({"open": ltp, "high": ltp, "low": ltp,
                        "close": ltp, "start_ts": bucket_ts})

        elif bucket_ts > buf["start_ts"]:
            # Seal completed candle asynchronously
            meta   = INDEX_TOKENS[token]
            sealed = dict(buf)
            threading.Thread(
                target=db_write_index_candle,
                args=(token, meta["symbol"], sealed["start_ts"],
                      sealed["open"], sealed["high"], sealed["low"], sealed["close"]),
                daemon=True,
            ).start()
            # Open new candle
            buf.update({"open": ltp, "high": ltp, "low": ltp,
                        "close": ltp, "start_ts": bucket_ts})
        else:
            buf["high"]  = max(buf["high"],  ltp)
            buf["low"]   = min(buf["low"],   ltp)
            buf["close"] = ltp


# ── NIFTY OI TICKER ───────────────────────────────────────────────────────────

ticker_instance = None


def build_ticker():
    global ticker_instance
    if ticker_instance:
        try:
            ticker_instance.close()
        except Exception:
            pass
        time.sleep(1)

    ticker_instance = KiteTicker(API_KEY, ACCESS_TOKEN)

    def on_ticks(ws, ticks):
        with state_lock:
            for tick in ticks:
                token = tick["instrument_token"]

                if token == NIFTY_TOKEN:
                    new_spot = tick.get("last_price", state["spot"])
                    state["spot"] = new_spot
                    _update_spot_ohlc(new_spot)
                    _check_roll(new_spot)
                    continue

                if token not in state["tokens"]:
                    continue

                current_oi  = tick.get("oi", 0)
                current_ltp = tick.get("last_price", 0)

                if current_oi == 0:
                    continue

                if token not in state["oi_baseline"]:
                    state["oi_baseline"][token] = current_oi
                    state["oi_prev_snap"][token] = current_oi
                    rk = state["tokens"][token]["role_key"]
                    print(f"[{now_ist().strftime('%H:%M:%S')}] Baseline — {rk}: OI={current_oi:,}")

                state["oi"][token] = {"oi": current_oi, "ltp": current_ltp}
                _update_ltp_ohlc(token, current_ltp)

            _append_history()

    def on_connect(ws, response):
        with state_lock:
            all_tokens = [NIFTY_TOKEN] + list(state["tokens"].keys())
        ws.subscribe(all_tokens)
        ws.set_mode(ws.MODE_FULL, all_tokens)
        print(f"[{now_ist().strftime('%H:%M:%S')}] Subscribed: Nifty + {len(state['tokens'])} option tokens")

    def on_error(ws, code, reason):
        print(f"Ticker error {code}: {reason}")

    def on_close(ws, code, reason):
        print(f"Ticker closed: {reason}. Reconnecting in 5s...")
        time.sleep(5)
        build_ticker()

    ticker_instance.on_ticks   = on_ticks
    ticker_instance.on_connect = on_connect
    ticker_instance.on_error   = on_error
    ticker_instance.on_close   = on_close
    ticker_instance.connect(threaded=True)


# ── INDEX TICKER (separate lightweight WebSocket, LTP-only) ───────────────────

index_ticker_instance = None


def build_index_ticker():
    global index_ticker_instance
    if index_ticker_instance:
        try:
            index_ticker_instance.close()
        except Exception:
            pass
        time.sleep(1)

    index_ticker_instance = KiteTicker(API_KEY, ACCESS_TOKEN)
    tokens = list(INDEX_TOKENS.keys())

    def on_ticks(ws, ticks):
        for tick in ticks:
            handle_index_tick(tick)

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)
        print(f"[index ticker] Subscribed: {[INDEX_TOKENS[t]['symbol'] for t in tokens]}")

    def on_error(ws, code, reason):
        print(f"[index ticker] Error {code}: {reason}")

    def on_close(ws, code, reason):
        print(f"[index ticker] Closed: {reason}. Reconnecting in 5s...")
        time.sleep(5)
        build_index_ticker()

    index_ticker_instance.on_ticks   = on_ticks
    index_ticker_instance.on_connect = on_connect
    index_ticker_instance.on_error   = on_error
    index_ticker_instance.on_close   = on_close
    index_ticker_instance.connect(threaded=True)


# ── STRIKE ROLL ───────────────────────────────────────────────────────────────

def _check_roll(new_spot):
    if state["last_anchored"] is None or new_spot is None:
        return
    if abs(new_spot - state["last_anchored"]) < ROLL_THRESHOLD:
        return

    bear, bull = compute_bear_bull(new_spot)
    min_s = min(state["strikes"])
    max_s = max(state["strikes"])
    bear  = max(min_s, min(bear, max_s))
    bull  = max(min_s, min(bull, max_s))

    if bear >= bull:
        strikes_sorted = sorted(state["strikes"])
        below = [s for s in strikes_sorted if s <= new_spot]
        above = [s for s in strikes_sorted if s > new_spot]
        bear  = below[-1] if below else strikes_sorted[0]
        bull  = above[0]  if above else strikes_sorted[-1]
        if bear == bull and len(strikes_sorted) > 1:
            idx  = strikes_sorted.index(bear)
            bear = strikes_sorted[max(0, idx - 1)]
            bull = strikes_sorted[min(len(strikes_sorted) - 1, idx + 1)]

    if bear == state["bear_strike"] and bull == state["bull_strike"]:
        state["last_anchored"] = new_spot
        return

    prev_bear = state["bear_strike"]
    prev_bull = state["bull_strike"]

    state["session_id"]   += 1
    state["bear_strike"]   = bear
    state["bull_strike"]   = bull
    state["last_anchored"] = new_spot

    state["roll_log"].append({
        "time":       now_ist().strftime("%H:%M:%S"),
        "session_id": state["session_id"],
        "from_spot":  round(new_spot),
        "from_bear":  prev_bear,
        "from_bull":  prev_bull,
        "to_bear":    bear,
        "to_bull":    bull,
    })

    print(
        f"[{now_ist().strftime('%H:%M:%S')}] "
        f"Roll → Bear {prev_bear}→{bear}  Bull {prev_bull}→{bull}  "
        f"Session {state['session_id']}"
    )


# ── STARTUP ───────────────────────────────────────────────────────────────────

def initialise(seed_spot):
    atm     = round_to_nearest_50(seed_spot)
    strikes = compute_strikes_window(atm)
    bear, bull = compute_bear_bull(seed_spot)
    bear = max(min(strikes), min(bear, max(strikes)))
    bull = max(min(strikes), min(bull, max(strikes)))

    if bear >= bull:
        strikes_sorted = sorted(strikes)
        below = [s for s in strikes_sorted if s <= seed_spot]
        above = [s for s in strikes_sorted if s > seed_spot]
        bear  = below[-1] if below else strikes_sorted[0]
        bull  = above[0]  if above else strikes_sorted[-1]

    print(f"\nATM: {atm}  |  Window: {strikes[0]} – {strikes[-1]}")
    print(f"Initial bear: {bear}  bull: {bull}")

    instruments = get_instruments_for_strikes(strikes)
    print(f"\nInstruments found: {len(instruments)} (expected {len(strikes) * 2})")
    for i in instruments:
        print(f"  {i['role_key']:15s}: {i['tradingsymbol']:25s}  token={i['instrument_token']}  expiry={i['expiry']}")

    token_map = {i["instrument_token"]: i for i in instruments}

    with state_lock:
        state["atm_strike"]    = atm
        state["strikes"]       = strikes
        state["bear_strike"]   = bear
        state["bull_strike"]   = bull
        state["last_anchored"] = seed_spot
        state["tokens"]        = token_map

    build_ticker()


# ── KITE HISTORICAL DATA FETCHER (fallback for /index-candles) ────────────────

def fetch_and_store_kite_historical(date_str: str) -> list:
    """
    Fetch 1-minute OHLC from Kite historical API for all three index instruments
    for the given date, store each candle in index_candles, and return rows.
    """
    target  = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    from_dt = dt.datetime(target.year, target.month, target.day, 9, 15, 0)
    to_dt   = dt.datetime(target.year, target.month, target.day, 15, 30, 0)

    all_rows = []

    for token, meta in INDEX_TOKENS.items():
        try:
            records = kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
                interval="minute",
                continuous=False,
                oi=False,
            )
        except Exception as e:
            print(f"[kite historical] {meta['symbol']} {date_str} error: {e}")
            continue

        for rec in records:
            candle_dt = rec["date"]
            ts = candle_dt.timestamp() if hasattr(candle_dt, "timestamp") else float(candle_dt)

            o = float(rec["open"])
            h = float(rec["high"])
            l = float(rec["low"])
            c = float(rec["close"])

            try:
                db_write_index_candle(token, meta["symbol"], ts, o, h, l, c)
            except Exception as e:
                print(f"[kite historical] DB write skip ({meta['symbol']}): {e}")

            all_rows.append({
                "token":      token,
                "symbol":     meta["symbol"],
                "time_label": ts_to_ist(ts).strftime("%H:%M"),
                "ts":         ts,
                "open":       o,
                "high":       h,
                "low":        l,
                "close":      c,
            })

    print(f"[kite historical] {date_str}: fetched {len(all_rows)} candles across {len(INDEX_TOKENS)} instruments")
    return all_rows


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────

@app.route("/")
def serve_dashboard():
    """Serve the index charts HTML."""
    return send_from_directory("static", "charts.html")


@app.route("/oi")
def get_oi():
    with state_lock:
        result = {
            "spot":        round(state["spot"]) if state["spot"] else None,
            "session_id":  state["session_id"],
            "atm_strike":  state["atm_strike"],
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "strikes":     state["strikes"],
            "roll_log":    list(state["roll_log"]),
            "as_of":       now_ist().strftime("%H:%M:%S"),
            "options":     {},
        }
        for token, meta in state["tokens"].items():
            snap       = state["oi"].get(token, {})
            current_oi = snap.get("oi", 0)
            baseline   = state["oi_baseline"].get(token, current_oi)
            prev_snap  = state["oi_prev_snap"].get(token, current_oi)
            rk         = meta["role_key"]
            result["options"][rk] = {
                "strike":                    meta["strike"],
                "type":                      meta["instrument_type"],
                "symbol":                    meta["tradingsymbol"],
                "ltp":                       snap.get("ltp", 0),
                "oi":                        current_oi,
                "oi_change_total":           current_oi - baseline,
                "oi_change_since_last_poll": current_oi - prev_snap,
                "baseline_oi":               baseline,
            }
            state["oi_prev_snap"][token] = current_oi
    return jsonify(result)


@app.route("/ltp")
def get_ltp():
    with state_lock:
        result = {
            "spot":    state["spot"],
            "as_of":   now_ist().strftime("%H:%M:%S"),
            "options": {},
        }
        for token, meta in state["tokens"].items():
            snap = state["oi"].get(token, {})
            result["options"][meta["role_key"]] = {
                "strike": meta["strike"],
                "type":   meta["instrument_type"],
                "symbol": meta["tradingsymbol"],
                "ltp":    snap.get("ltp", 0),
            }
    return jsonify(result)


@app.route("/strikes")
def get_strikes():
    with state_lock:
        return jsonify({
            "atm_strike":  state["atm_strike"],
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "strikes":     state["strikes"],
            "session_id":  state["session_id"],
        })


@app.route("/oi/history")
def get_history():
    try:
        rows = db_read_today()
        return jsonify(rows)
    except Exception as e:
        print(f"DB read error, falling back to memory: {e}")
        return jsonify(list(oi_history))


@app.route("/oi/live-candle")
def live_candle():
    with state_lock:
        if minute_buffer["start_ts"] is None or minute_buffer["start_snap"] is None:
            return jsonify({"available": False})

        now          = time.time()
        current_snap = _current_snap(now)
        start        = minute_buffer["start_snap"]
        elapsed      = round(now - minute_buffer["start_ts"])

        result = {
            "available":   True,
            "elapsed_sec": elapsed,
            "time_label":  ts_to_ist(minute_buffer["start_ts"]).strftime("%H:%M") + "*",
            "spot_open":   round(minute_buffer["spot_open"])  if minute_buffer["spot_open"]  else 0,
            "spot_high":   round(minute_buffer["spot_high"])  if minute_buffer["spot_high"]  else 0,
            "spot_low":    round(minute_buffer["spot_low"])   if minute_buffer["spot_low"]   else 0,
            "spot_close":  round(minute_buffer["spot_close"]) if minute_buffer["spot_close"] else 0,
            "bear_strike": state["bear_strike"],
            "bull_strike": state["bull_strike"],
            "deltas":      {},
        }

        for token, meta in state["tokens"].items():
            rk         = meta["role_key"]
            current_oi = state["oi"].get(token, {}).get("oi", 0)
            start_oi   = start.get(rk + "_oi", current_oi)
            result["deltas"][rk] = {
                "oi":    current_oi,
                "delta": current_oi - start_oi,
            }

    return jsonify(result)


@app.route("/health")
def health():
    with state_lock:
        return jsonify({
            "status":           "ok",
            "spot":             state["spot"],
            "session_id":       state["session_id"],
            "atm_strike":       state["atm_strike"],
            "bear_strike":      state["bear_strike"],
            "bull_strike":      state["bull_strike"],
            "strikes":          state["strikes"],
            "tokens_tracked":   len(state["tokens"]),
            "history_rows":     len(oi_history),
            "roll_count":       len(state["roll_log"]),
            "spot_ohlc_buffer": {
                "open":  minute_buffer["spot_open"],
                "high":  minute_buffer["spot_high"],
                "low":   minute_buffer["spot_low"],
                "close": minute_buffer["spot_close"],
            },
        })


@app.route("/reset-csv", methods=["POST"])
def reset_csv():
    """Wipe today's OI rows from DB + in-memory buffer. Call each morning."""
    try:
        db_reset_today()
    except Exception as e:
        print(f"DB reset error: {e}")
    oi_history.clear()
    with state_lock:
        state["session_id"]            = 0
        minute_buffer["start_ts"]      = None
        minute_buffer["start_snap"]    = None
        minute_buffer["spot_open"]     = None
        minute_buffer["spot_high"]     = None
        minute_buffer["spot_low"]      = None
        minute_buffer["spot_close"]    = None
        minute_buffer["ltp_ohlc"]      = {}
    print(f"[{now_ist().strftime('%H:%M:%S')}] DB reset. Session 0.")
    return jsonify({"status": "ok", "message": "Today's rows cleared. Session reset to 0."})


@app.route("/index-ltp")
def get_index_ltp():
    """
    Live tick LTP + forming 1-min candle for BankNifty, HDFC Bank, ICICI Bank.
    Polled by charts.html every 500 ms.
    """
    with index_tick_lock:
        ticks = {
            str(token): {
                "symbol": meta["symbol"],
                "label":  meta["label"],
                "ltp":    index_tick_state[token]["ltp"],
                "ts":     index_tick_state[token]["ts"],
            }
            for token, meta in INDEX_TOKENS.items()
        }
    with index_buf_lock:
        live_candles = {
            str(token): dict(index_minute_buf[token])
            for token in INDEX_TOKENS
        }
    return jsonify({
        "as_of":        now_ist().strftime("%H:%M:%S"),
        "ticks":        ticks,
        "live_candles": live_candles,
    })


@app.route("/index-candles")
def get_index_candles():
    """
    1-min OHLC candles for a given date. Query param: ?date=YYYY-MM-DD
    Strategy:
      1. Check DB first.
      2. If empty and date is not today, fetch from Kite historical API and save to DB.
      3. Today's data is always from the live WebSocket buffer.
    """
    date_str = request.args.get("date", now_ist().strftime("%Y-%m-%d"))
    today    = now_ist().strftime("%Y-%m-%d")

    try:
        rows = db_read_index_candles(date_str)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    source = "db"
    if not rows and date_str != today:
        print(f"[index-candles] No DB data for {date_str} — fetching from Kite historical API")
        try:
            rows = fetch_and_store_kite_historical(date_str)
            source = "kite_api"
        except Exception as e:
            return jsonify({"error": f"Kite historical fetch failed: {e}"}), 502

    grouped = {}
    for r in rows:
        key = str(r["token"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append({
            "time_label": r["time_label"],
            "ts":   float(r["ts"]),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low":  float(r["low"]),
            "close":float(r["close"]),
        })

    return jsonify({"date": date_str, "candles": grouped, "source": source})


@app.route("/index-dates")
def get_index_dates():
    """Return all dates that have stored index candle data (for date picker)."""
    try:
        dates = db_read_index_dates()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"dates": dates})


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("Nifty OI Bias Monitor — Bridge (Railway / PostgreSQL edition)")
    print("=" * 60)

    print("\nInitialising database...")
    init_db()
    init_index_db()

    print("\nFetching live Nifty spot...")
    seed_spot = get_live_spot()
    print(f"Live spot: {seed_spot:,.2f}")

    print("\nInitialising 11-strike window...")
    initialise(seed_spot)

    print("\nStarting index ticker (BankNifty / HDFC / ICICI)...")
    build_index_ticker()

    print(f"\nFlask listening on port {FLASK_PORT}")
    print(f"  GET  /                serves OI dashboard HTML")
    print(f"  GET  /charts          serves index charts HTML")
    print(f"  GET  /oi              all strikes snapshot")
    print(f"  GET  /ltp             live LTP all option tokens")
    print(f"  GET  /oi/live-candle  forming candle")
    print(f"  GET  /strikes         strike list")
    print(f"  GET  /oi/history      1-min rows + spot OHLC")
    print(f"  GET  /health          status + OHLC buffer")
    print(f"  POST /reset-csv       wipe today's OI rows")
    print(f"  GET  /index-ltp       live BankNifty/HDFC/ICICI ticks")
    print(f"  GET  /index-candles   1-min OHLC by date")
    print(f"  GET  /index-dates     available dates")
    print("\nPress Ctrl+C to stop.\n")

    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False, use_reloader=False)
