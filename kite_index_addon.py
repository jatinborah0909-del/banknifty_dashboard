"""
kite_index_addon.py — BankNifty / HDFC Bank / ICICI Bank tick + candle tracking
=================================================================================
Add this code into your existing kite_oi_bridge.py in the sections indicated.
Each section is clearly labelled with WHERE it goes.

New endpoints added:
    GET /index-ltp          → live tick LTP + forming candle for all 3 instruments
    GET /index-candles      → 1-min OHLC rows for ?date=YYYY-MM-DD
                              DB-first; falls back to Kite historical_data() API
                              for past dates not yet in DB, then saves to DB.
    GET /index-dates        → list of all dates that have stored data in DB
"""

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — ADD AFTER YOUR EXISTING TOKEN CONSTANTS (near NIFTY_TOKEN = ...)
# ══════════════════════════════════════════════════════════════════════════════

BANKNIFTY_TOKEN = 260105    # NSE:NIFTY BANK  — verify via kite.instruments("NSE")
HDFC_TOKEN      = 341249    # NSE:HDFCBANK
ICICI_TOKEN     = 1270529   # NSE:ICICIBANK

INDEX_TOKENS = {
    BANKNIFTY_TOKEN: {"symbol": "NIFTY BANK", "label": "BankNifty"},
    HDFC_TOKEN:      {"symbol": "HDFCBANK",   "label": "HDFC Bank"},
    ICICI_TOKEN:     {"symbol": "ICICIBANK",  "label": "ICICI Bank"},
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — ADD IN THE STATE SECTION (after your existing `state = {...}`)
# ══════════════════════════════════════════════════════════════════════════════

# Live tick state — updated on every WebSocket tick
index_tick_state = {
    token: {"ltp": None, "ts": None} for token in INDEX_TOKENS
}
index_tick_lock = threading.Lock()

# Per-minute OHLC buffer — one entry per token, reset each minute
index_minute_buf = {
    token: {"open": None, "high": None, "low": None, "close": None, "start_ts": None}
    for token in INDEX_TOKENS
}
index_buf_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DATABASE: ADD AFTER YOUR EXISTING init_db() FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

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


def db_read_index_candles(date_str: str) -> list:
    """Return all 1-min candles for a given date (YYYY-MM-DD)."""
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


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — TICK HANDLER
# Add this function, then call handle_index_tick(tick) as the FIRST LINE
# inside the `for tick in ticks:` loop in your existing on_ticks().
# ══════════════════════════════════════════════════════════════════════════════

MARKET_OPEN  = datetime.time(9, 15)
MARKET_CLOSE = datetime.time(15, 30)


def handle_index_tick(tick: dict):
    """Update live state and per-minute OHLC buffer for index instruments."""
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
        ts = ts.timestamp()   # Kite may return a datetime object

    # ── Market-hours guard ────────────────────────────────────────────────────
    # Ignore ticks outside 09:15–15:30 IST so pre/post-market data is never
    # stored as candles or plotted on the chart.
    tick_time = ts_to_ist(ts).time()
    in_market = MARKET_OPEN <= tick_time <= MARKET_CLOSE

    # Always update the live LTP display (so the UI shows the current price),
    # but only feed the OHLC candle buffer during market hours.
    with index_tick_lock:
        index_tick_state[token]["ltp"] = ltp
        index_tick_state[token]["ts"]  = ts

    if not in_market:
        return  # skip candle accumulation outside market hours

    # Update per-minute OHLC; seal completed candle when minute rolls over
    with index_buf_lock:
        buf = index_minute_buf[token]
        dt         = ts_to_ist(ts)
        bucket_ts  = dt.replace(second=0, microsecond=0).timestamp()

        if buf["start_ts"] is None:
            buf.update({"open": ltp, "high": ltp, "low": ltp,
                        "close": ltp, "start_ts": bucket_ts})

        elif bucket_ts > buf["start_ts"]:
            # Seal completed candle asynchronously
            meta = INDEX_TOKENS[token]
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


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — EDIT build_ticker() → on_connect() TO SUBSCRIBE INDEX TOKENS
# ══════════════════════════════════════════════════════════════════════════════
#
# Change your existing on_connect() from:
#
#   def on_connect(ws, response):
#       with state_lock:
#           all_tokens = [NIFTY_TOKEN] + list(state["tokens"].keys())
#       ws.subscribe(all_tokens)
#       ws.set_mode(ws.MODE_FULL, all_tokens)
#
# TO:
#
#   def on_connect(ws, response):
#       with state_lock:
#           option_tokens = [NIFTY_TOKEN] + list(state["tokens"].keys())
#       idx_tokens = list(INDEX_TOKENS.keys())
#       all_tokens = option_tokens + idx_tokens
#       ws.subscribe(all_tokens)
#       ws.set_mode(ws.MODE_FULL, option_tokens)   # full depth for options
#       ws.set_mode(ws.MODE_LTP,  idx_tokens)       # LTP-only for indices
#       print(f"Subscribed {len(option_tokens)} option tokens + {len(idx_tokens)} index tokens")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — STARTUP: in __main__, after init_db(), add:
#   init_index_db()
# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — NEW API ENDPOINTS (paste with your other @app.route blocks)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/index-ltp")
def get_index_ltp():
    """
    Live tick LTP + forming 1-min candle for all 3 instruments.
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


def fetch_and_store_kite_historical(date_str: str) -> list:
    """
    Fetch 1-minute OHLC from Kite historical API for all three index instruments
    for the given date, store each candle in index_candles, and return the rows
    in the same format as db_read_index_candles().

    Kite historical_data() args:
        instrument_token, from_date, to_date, interval, continuous, oi
    Market hours: 09:15–15:30 IST.

    Returns a flat list of dicts (same shape as db_read_index_candles output).
    """
    from datetime import date as date_cls
    import datetime as dt

    target = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    # Kite accepts date objects or "YYYY-MM-DD HH:MM:SS" strings
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
            # rec keys: date (datetime), open, high, low, close, volume
            candle_dt = rec["date"]
            if hasattr(candle_dt, "timestamp"):
                ts = candle_dt.timestamp()
            else:
                ts = float(candle_dt)

            o = float(rec["open"])
            h = float(rec["high"])
            l = float(rec["low"])
            c = float(rec["close"])

            # Persist to DB (ignore duplicate errors — idempotent)
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


@app.route("/index-candles")
def get_index_candles():
    """
    1-min OHLC candles for a given date.
    Query param: ?date=YYYY-MM-DD  (defaults to today)

    Strategy:
      1. Try DB first.
      2. If DB has no rows for that date AND the date is not today (today's data
         is still accumulating live), fall back to Kite historical_data() API,
         save the result to DB, then return it.
      3. Today's missing data is always expected during market hours — no fallback.
    """
    date_str = request.args.get("date", now_ist().strftime("%Y-%m-%d"))
    today    = now_ist().strftime("%Y-%m-%d")

    try:
        rows = db_read_index_candles(date_str)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # If DB is empty for a past date, backfill from Kite
    if not rows and date_str != today:
        print(f"[index-candles] No DB data for {date_str} — fetching from Kite historical API")
        try:
            rows = fetch_and_store_kite_historical(date_str)
        except Exception as e:
            return jsonify({"error": f"Kite historical fetch failed: {e}"}), 502

    # Group by token for the frontend
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

    source = "db" if rows and date_str != today else ("kite_api" if rows else "empty")
    return jsonify({"date": date_str, "candles": grouped, "source": source})


@app.route("/index-dates")
def get_index_dates():
    """Return all dates that have stored index candle data (for date picker)."""
    try:
        dates = db_read_index_dates()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"dates": dates})
