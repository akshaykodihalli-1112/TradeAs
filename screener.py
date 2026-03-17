from fastapi import FastAPI, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import pandas as pd
import threading
import time, os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

app = FastAPI(title="DhanScreen API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DHAN_BASE = "https://api.dhan.co"
IST       = ZoneInfo("Asia/Kolkata")
SELF_URL  = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

cache = {
    "data": [], "updated_at": None, "status": "idle",
    "count": 0, "errors": [], "progress": 0, "total": 0,
    "symbol_source": "none", "market_open": False, "debug": {},
}
CREDS   = {"client_id": os.getenv("DHAN_CLIENT_ID", ""), "access_token": os.getenv("DHAN_ACCESS_TOKEN", "")}
SYMBOLS = []
_screener_lock = threading.Lock()


def is_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5: return False
    return now.replace(hour=9, minute=15, second=0, microsecond=0) <= now \
        <= now.replace(hour=15, minute=30, second=0, microsecond=0)


def get_ist_now():
    return datetime.now(IST)


def keep_alive():
    if not SELF_URL: return
    try:
        requests.get(f"{SELF_URL}/api/health", timeout=10)
        print(f"Keep-alive OK ({get_ist_now().strftime('%H:%M IST')})")
    except Exception as e:
        print(f"Keep-alive failed: {e}")


# ── FNO symbol names — IDs are fetched dynamically from Dhan CSV ──
# This is ONLY a list of symbol names used when CSV is unavailable.
# Security IDs are NEVER hardcoded here — always loaded from Dhan master CSV.
FNO_SYMBOL_NAMES = [
    "ABBOTINDIA","ABCAPITAL","ABFRL","ACC","ADANIENT","ADANIPORTS","ALKEM",
    "AMBUJACEM","ANGELONE","APLAPOLLO","APOLLOHOSP","APOLLOTYRE","ASHOKLEY",
    "ASIANPAINT","ASTRAL","ATGL","ATUL","AUBANK","AUROPHARMA","AXISBANK",
    "BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK","BANKBARODA",
    "BATAINDIA","BEL","BERGEPAINT","BHARATFORG","BHARTIARTL","BHEL","BIOCON",
    "BOSCHLTD","BPCL","BRITANNIA","BSOFT","CANBK","CANFINHOME","CDSL","CGPOWER",
    "CHAMBLFERT","CHOLAFIN","CIPLA","COALINDIA","COFORGE","COLPAL","CONCOR",
    "COROMANDEL","CROMPTON","CUB","CUMMINSIND","DABUR","DALBHARAT","DEEPAKNTR",
    "DELTACORP","DIVISLAB","DIXON","DLF","DMART","DRREDDY","EICHERMOT","ESCORTS",
    "EXIDEIND","FEDERALBNK","FORCEMOT","FORTIS","GAIL","GLENMARK","GMRINFRA",
    "GNFC","GODREJCP","GODREJPROP","GRANULES","GRASIM","GUJGASLTD","HAL",
    "HAVELLS","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HEROMOTOCO","HFCL",
    "HINDALCO","HINDCOPPER","HINDPETRO","HINDUNILVR","ICICIBANK","ICICIGI",
    "ICICIPRULI","IDEA","IDFCFIRSTB","IEX","IGL","INDHOTEL","INDIACEM",
    "INDIAMART","INDIGO","INDUSINDBK","INDUSTOWER","INFY","IOC","IPCALAB",
    "IRCTC","ITC","JINDALSTEL","JKCEMENT","JSWENERGY","JSWSTEEL","JUBLFOOD",
    "KALYANKJIL","KEI","KOTAKBANK","KPITTECH","LALPATHLAB","LAURUSLABS",
    "LICHSGFIN","LICI","LT","LTIM","LTTS","LUPIN","M&M","M&MFIN","MANAPPURAM",
    "MARICO","MARUTI","MAXHEALTH","MCX","METROPOLIS","MFSL","MOTHERSON",
    "MPHASIS","MRF","MUTHOOTFIN","NATIONALUM","NAUKRI","NAVINFLUOR","NESTLEIND",
    "NMDC","NTPC","OBEROIRLTY","OFSS","ONGC","PAGEIND","PEL","PERSISTENT",
    "PETRONET","PFC","PIDILITIND","PIIND","PNB","POLYCAB","POWERGRID","PVRINOX",
    "RAMCOCEM","RBLBANK","RECLTD","RELIANCE","SAIL","SBICARD","SBILIFE","SBIN",
    "SHREECEM","SHRIRAMFIN","SIEMENS","SRF","SUNPHARMA","SUNTV","SUPREMEIND",
    "SUZLON","SYNGENE","TATACHEM","TATACOMM","TATACONSUM","TATAELXSI",
    "TATAMOTORS","TATAPOWER","TATASTEEL","TCS","TECHM","TIINDIA","TITAN",
    "TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","UBL","ULTRACEMCO","UNIONBANK",
    "UPL","VEDL","VOLTAS","WIPRO","ZEEL","ZOMATO","ZYDUSLIFE",
]

CSV_URL      = "https://images.dhan.co/api-data/api-scrip-master.csv"
_csv_eq_cache = {}   # symbol → security_id, populated from CSV
_csv_lock     = threading.Lock()


def _build_eq_lookup_from_csv() -> dict:
    """
    Downloads Dhan master CSV and returns {symbol: security_id}
    for all NSE_EQ equities. Result is cached in _csv_eq_cache.
    Raises on failure so caller can decide what to do.
    """
    resp = requests.get(CSV_URL, timeout=25)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    df.columns = [c.strip().upper() for c in df.columns]

    seg_col = next((c for c in df.columns if "EXCH_SEG" in c or "SEGMENT" in c), None)
    sym_col = next((c for c in df.columns if "TRADING_SYMBOL" in c or "SYMBOL_NAME" in c), None)
    id_col  = next((c for c in df.columns if "SECURITY_ID" in c or "SCRIP_ID" in c or "SM_SYMBOL_ID" in c), None)

    if not all([seg_col, sym_col, id_col]):
        raise ValueError(f"Unexpected CSV columns: {list(df.columns)[:10]}")

    df[sym_col] = df[sym_col].astype(str).str.strip()
    df[seg_col] = df[seg_col].astype(str).str.upper().str.strip()
    eq_df = df[df[seg_col] == "NSE_EQ"].copy()

    lookup = {}
    for _, r in eq_df[[id_col, sym_col]].iterrows():
        sym = r[sym_col].strip()
        if sym and sym not in lookup:
            try:
                lookup[sym] = str(int(float(str(r[id_col]))))
            except (ValueError, TypeError):
                pass

    print(f"CSV: built NSE_EQ lookup — {len(lookup)} symbols")
    return lookup


def fetch_fno_symbols():
    """
    Builds SYMBOLS list with 100% dynamic security IDs from Dhan master CSV.

    Flow:
      1. Download CSV → build NSE_EQ symbol→ID lookup
      2. Extract FNO symbols (FUTSTK + OPTSTK from NSE_FNO segment)
      3. Cross-match FNO names against NSE_EQ to get correct equity IDs
      4. Supplement with FNO_SYMBOL_NAMES for any known symbols missing from CSV
      5. Only fallback to no-ID placeholder if CSV completely fails

    No IDs are ever hardcoded. Wrong IDs are impossible if CSV is reachable.
    """
    global SYMBOLS, _csv_eq_cache
    cache["symbol_source"] = "loading"

    try:
        resp = requests.get(CSV_URL, timeout=25)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text), low_memory=False)
        df.columns = [c.strip().upper() for c in df.columns]

        seg_col  = next((c for c in df.columns if "EXCH_SEG"       in c or "SEGMENT"     in c), None)
        sym_col  = next((c for c in df.columns if "TRADING_SYMBOL" in c or "SYMBOL_NAME" in c), None)
        id_col   = next((c for c in df.columns if "SECURITY_ID"    in c or "SCRIP_ID"    in c or "SM_SYMBOL_ID" in c), None)
        inst_col = next((c for c in df.columns if "INSTRUMENT"     in c), None)

        if not all([seg_col, sym_col, id_col]):
            raise ValueError(f"CSV column mismatch: {list(df.columns)[:10]}")

        df[sym_col] = df[sym_col].astype(str).str.strip()
        df[seg_col] = df[seg_col].astype(str).str.upper().str.strip()

        # ── Build NSE_EQ lookup: symbol → security_id ─────────────────
        eq_df = df[df[seg_col] == "NSE_EQ"].copy()
        eq_lookup = {}
        for _, r in eq_df[[id_col, sym_col]].iterrows():
            sym = r[sym_col].strip()
            if sym and sym not in eq_lookup:
                try:
                    eq_lookup[sym] = str(int(float(str(r[id_col]))))
                except (ValueError, TypeError):
                    pass
        _csv_eq_cache = eq_lookup   # cache for later use by /api/symbols/refresh
        print(f"CSV NSE_EQ lookup: {len(eq_lookup)} symbols")

        # ── Extract FNO symbol names from NSE_FNO segment ─────────────
        fno_df = df[df[seg_col] == "NSE_FNO"].copy()
        if inst_col:
            fno_df[inst_col] = fno_df[inst_col].astype(str).str.upper().str.strip()
            futstk_syms = set(fno_df[fno_df[inst_col] == "FUTSTK"][sym_col].str.strip().unique())
            optstk_syms = set(fno_df[fno_df[inst_col] == "OPTSTK"][sym_col].str.strip().unique())
            fno_syms = futstk_syms | optstk_syms
        else:
            fno_syms = set(fno_df[sym_col].str.strip().unique())

        # Merge with our known FNO names (union — CSV may miss newly listed stocks)
        all_fno_names = fno_syms | set(FNO_SYMBOL_NAMES)
        print(f"FNO names: {len(fno_syms)} from CSV + {len(FNO_SYMBOL_NAMES)} known = {len(all_fno_names)} total")

        # ── Cross-match FNO names → NSE_EQ IDs ────────────────────────
        matched, unmatched = [], []
        for sym in sorted(all_fno_names):
            sid = eq_lookup.get(sym)
            if sid:
                matched.append({"symbol": sym, "security_id": sid, "exchange": "NSE"})
            else:
                unmatched.append(sym)

        if unmatched:
            print(f"  Unmatched (no NSE_EQ entry): {unmatched[:10]}")

        if len(matched) < 50:
            raise ValueError(f"Only {len(matched)} symbols matched — CSV may be malformed")

        SYMBOLS = matched
        cache["symbol_source"] = "csv_dynamic"
        print(f"✓ Loaded {len(SYMBOLS)} FNO stocks from CSV (dynamic IDs). Unmatched: {len(unmatched)}")

    except Exception as e:
        print(f"✗ CSV fetch failed: {e}")
        # Last-resort fallback: SYMBOLS list with no IDs — will cause 0 quotes
        # but at least the app won't crash. Log clearly so user knows.
        if not SYMBOLS:
            print("  SYMBOLS is empty — app will return no data until CSV is reachable.")
            SYMBOLS = []
        cache["symbol_source"] = f"csv_failed:{str(e)[:60]}"



# ── FIX 1: Always calculate change% from prev_close (net_change=0 after market close) ──
def parse_quote(sec_id, q, id_to_sym):
    # Normalize: Dhan API may return int or string keys — always cast to str
    sym = id_to_sym.get(str(sec_id)) or id_to_sym.get(str(int(float(str(sec_id)))))
    if not sym:
        return None, None
    ltp        = float(q.get("last_price", 0))
    ohlc       = q.get("ohlc", {})
    prev_close = float(ohlc.get("close", 0))

    # FIX: always use (ltp - prev_close) / prev_close
    # net_change is 0 after market close so we never rely on it
    chg_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close else 0

    volume = int(q.get("volume", 0))
    return sym, {
        "ltp":        round(ltp, 2),
        "prev_close": round(prev_close, 2),
        "change_pct": chg_pct,
        "volume":     volume,
        "open":       round(float(ohlc.get("open", 0)), 2),
        "high":       round(float(ohlc.get("high", 0)), 2),
        "low":        round(float(ohlc.get("low",  0)), 2),
    }


# ── FIX 2: Use master CSV IDs via fetch_fno_symbols() for correct security IDs ──
def fetch_all_quotes(cid, tok):
    all_ids   = [int(sym["security_id"]) for sym in SYMBOLS]
    # Build lookup with BOTH str and int keys to handle any API response format
    id_to_sym = {}
    for sym in SYMBOLS:
        sid = sym["security_id"]
        id_to_sym[str(sid)]          = sym["symbol"]   # "1333" → "HDFCBANK"
        id_to_sym[str(int(float(sid)))] = sym["symbol"] # handles "1333.0" edge cases
    headers   = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    quotes    = {}
    last_error = None

    BATCH_SIZE = 50  # Dhan rate-limits at ~75/req; 50 is safe
    for i in range(0, len(all_ids), BATCH_SIZE):
        batch   = all_ids[i:i + BATCH_SIZE]
        payload = {"NSE_EQ": batch}
        success = False
        for endpoint in ["/v2/marketfeed/quote", "/v2/marketfeed/ohlc"]:
            for attempt in range(3):
                try:
                    resp = requests.post(f"{DHAN_BASE}{endpoint}", json=payload, headers=headers, timeout=15)
                    returned = len(resp.json().get("data", {}).get("NSE_EQ", {})) if resp.status_code == 200 else "err"
                    print(f"  {endpoint} batch={i} attempt={attempt+1} status={resp.status_code} returned={returned}")
                    if resp.status_code == 200:
                        raw = resp.json().get("data", {}).get("NSE_EQ", {})
                        for sec_id, q in raw.items():
                            sym, data = parse_quote(sec_id, q, id_to_sym)
                            if sym:
                                quotes[sym] = data
                        success = True
                        break
                    elif resp.status_code == 429:
                        wait = 4 * (attempt + 1)
                        print(f"  Rate limited batch={i}, waiting {wait}s...")
                        time.sleep(wait)
                        continue
                    else:
                        last_error = f"{endpoint} {resp.status_code}: {resp.text[:150]}"
                        break
                except Exception as e:
                    last_error = str(e)
                    print(f"  {endpoint} error: {e}")
                    time.sleep(1)
            if success:
                break
        time.sleep(1.2)  # 1.2s between batches keeps us under rate limit

    # Log which symbols are missing — helps identify wrong IDs
    missing = [s["symbol"] for s in SYMBOLS if s["symbol"] not in quotes]
    if missing:
        print(f"  Missing from marketfeed ({len(missing)}): {missing[:10]}")

    cache["debug"] = {
        "quotes_fetched": len(quotes),
        "missing_count":  len(missing),
        "missing_sample": missing[:5],
        "last_error":     last_error,
        "sample":         {k: v for k, v in list(quotes.items())[:2]},
    }
    print(f"fetch_all_quotes → {len(quotes)} quotes | missing={len(missing)}")
    return quotes


def get_historical(security_id, access_token, retries=3):
    today     = datetime.now(IST)
    from_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    headers   = {"access-token": access_token, "Content-Type": "application/json"}
    payload   = {"securityId": security_id, "exchangeSegment": "NSE_EQ",
                  "instrument": "EQUITY", "expiryCode": 0,
                  "fromDate": from_date, "toDate": to_date}
    for attempt in range(retries):
        try:
            resp = requests.post(f"{DHAN_BASE}/v2/charts/historical",
                                 json=payload, headers=headers, timeout=10)
            if resp.status_code == 429:
                time.sleep(2 ** (attempt + 1)); continue
            if resp.status_code in (400, 401, 403): return None, None
            resp.raise_for_status()
            data = resp.json()
            if not data.get("timestamp"): return None, None
            return data.get("close", []), data.get("volume", [])
        except Exception:
            time.sleep(1)
    return None, None


def _run_screener(tok):
    global cache
    if not SYMBOLS: fetch_fno_symbols()
    cid   = CREDS["client_id"]
    total = len(SYMBOLS)
    cache.update({"status": "fetching", "progress": 0, "total": total, "market_open": is_market_open()})
    print(f"Screener — {total} stocks | {get_ist_now().strftime('%H:%M:%S IST')}")

    quotes = fetch_all_quotes(cid, tok)
    if not quotes:
        cache.update({"status": "error", "progress": 100,
                      "errors": [f"marketfeed returned 0 quotes. debug: {cache['debug']}"]})
        return

    results, skipped = [], []
    for i, sym in enumerate(SYMBOLS):
        try:
            q = quotes.get(sym["symbol"])
            if not q:
                skipped.append(f"{sym['symbol']}:no_quote")
                cache["progress"] = round((i + 1) / total * 100)
                time.sleep(0.3)
                continue
            ltp       = q["ltp"]
            chg_pct   = q["change_pct"]   # now correctly calculated
            today_vol = q["volume"]
            closes, volumes = get_historical(sym["security_id"], tok)
            momentum5d = 0
            if closes and len(closes) >= 6:
                close_5ago = closes[-6]
                momentum5d = round(((ltp - close_5ago) / close_5ago) * 100, 2) if close_5ago else 0
            vol_ratio  = 0
            avg_vol_7d = 0
            if volumes and len(volumes) >= 8:
                avg_vol_7d = sum(volumes[-8:-1]) / 7
                vol_ratio  = round(today_vol / avg_vol_7d, 2) if avg_vol_7d > 0 else 0
            elif today_vol > 0:
                vol_ratio = 1.0
            results.append({
                "symbol": sym["symbol"], "exchange": sym["exchange"],
                "ltp": ltp, "change": chg_pct,
                "momentum5d": momentum5d, "volumeRatio": vol_ratio,
                "todayVol": today_vol, "avgVol7d": int(avg_vol_7d),
            })
        except Exception as e:
            skipped.append(f"{sym['symbol']}:{e}")
        cache["progress"] = round((i + 1) / total * 100)
        time.sleep(0.3)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{total} | ok={len(results)}")
            time.sleep(1.5)

    results.sort(key=lambda x: x["volumeRatio"], reverse=True)
    cache.update({
        "data": results, "updated_at": get_ist_now().strftime("%H:%M:%S IST"),
        "status": "ok", "count": len(results), "errors": [],
        "skipped": skipped[:20], "progress": 100, "market_open": is_market_open(),
    })
    print(f"Done — {len(results)} OK | {len(skipped)} skipped")


def fetch_screener(client_id=None, access_token=None):
    cid = client_id or CREDS["client_id"]
    tok = access_token or CREDS["access_token"]
    if not cid or not tok: cache["status"] = "no_credentials"; return
    CREDS["client_id"] = cid; CREDS["access_token"] = tok
    if _screener_lock.locked(): return
    def _worker():
        with _screener_lock: _run_screener(tok)
    threading.Thread(target=_worker, daemon=True).start()


def scheduled_refresh():
    if not CREDS["client_id"] or not CREDS["access_token"]: return
    # Run always — market closed data still useful (prev_close, volumes, etc.)
    fetch_screener()


scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_refresh,  "interval", minutes=5,   id="screener")
scheduler.add_job(keep_alive,         "interval", minutes=10,  id="keepalive")
scheduler.add_job(fetch_fno_symbols,  "cron",     hour=8, minute=30, id="csv_refresh")  # re-fetch CSV daily at 8:30 AM IST before market opens
scheduler.start()


@app.on_event("startup")
async def startup():
    fetch_fno_symbols()   # tries CSV first — gets correct IDs
    print(f"Symbols ready: {len(SYMBOLS)} ({cache['symbol_source']})")
    # Start screener immediately on startup (no delay needed)
    threading.Thread(target=fetch_screener, daemon=True).start()


# ── ROUTES ────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "DhanScreen API", "docs": "/docs",
            "time_ist": get_ist_now().strftime("%H:%M:%S"), "market_open": is_market_open()}


@app.get("/api/ping-dhan")
def ping_dhan(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    tok = x_access_token or CREDS["access_token"]
    if not tok: return {"connected": False, "error": "No access token"}
    try:
        r = requests.get(f"{DHAN_BASE}/v2/profile", headers={"access-token": tok}, timeout=5)
        if r.status_code == 200: return {"connected": True, "status": 200, "profile": r.json()}
        return {"connected": False, "status": r.status_code, "detail": r.text[:200]}
    except Exception as e:
        return {"connected": False, "error": str(e)}


@app.get("/api/screener")
def get_screener(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    if x_client_id and x_access_token:
        CREDS["client_id"] = x_client_id; CREDS["access_token"] = x_access_token
        fetch_screener(x_client_id, x_access_token)
    return cache


@app.get("/api/screener/momentum")
def momentum(min_pct: float = 3.0):
    return {"data": [s for s in cache["data"] if s["momentum5d"] >= min_pct]}


@app.get("/api/screener/volume-spike")
def volume_spike(min_ratio: float = 2.0):
    return {"data": [s for s in cache["data"] if s["volumeRatio"] >= min_ratio]}


@app.get("/api/screener/strong")
def strong(min_mom: float = 5.0, min_vol: float = 2.5):
    return {"data": [s for s in cache["data"] if s["momentum5d"] >= min_mom and s["volumeRatio"] >= min_vol]}


@app.get("/api/screener/top")
def top(limit: int = 20):
    return {"data": cache["data"][:limit]}


@app.get("/api/symbols")
def list_symbols():
    return {"count": len(SYMBOLS), "source": cache.get("symbol_source"), "symbols": SYMBOLS}


@app.post("/api/symbols/refresh")
def refresh_symbols():
    """Force re-download of Dhan master CSV and rebuild symbol→ID map.
    Call this anytime a symbol shows wrong price or a new FNO stock is listed."""
    before = len(SYMBOLS)
    fetch_fno_symbols()
    return {
        "status": "ok",
        "before": before,
        "after": len(SYMBOLS),
        "source": cache.get("symbol_source"),
        "sample": SYMBOLS[:5],
    }


@app.get("/api/health")
def health():
    return {
        "status": "ok", "cache_status": cache["status"],
        "last_refresh": cache["updated_at"], "symbol_count": len(SYMBOLS),
        "symbol_source": cache.get("symbol_source"), "result_count": cache["count"],
        "error_count": len(cache.get("errors", [])),
        "skipped_count": len(cache.get("skipped", [])),
        "skipped_sample": cache.get("skipped", [])[:5],
        "progress_pct": cache.get("progress", 0),
        "market_open": is_market_open(),
        "time_ist": get_ist_now().strftime("%H:%M:%S"),
        "day": get_ist_now().strftime("%A"),
        "debug": cache.get("debug", {}),
    }


@app.get("/api/ltp")
def get_ltp(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    cid = x_client_id or CREDS["client_id"]
    tok = x_access_token or CREDS["access_token"]
    if not cid or not tok: return {"status": "no_credentials", "quotes": {}}
    if not SYMBOLS:        return {"status": "no_symbols",     "quotes": {}}
    quotes = fetch_all_quotes(cid, tok)
    return {"status": "ok" if quotes else "error",
            "updated_at": get_ist_now().strftime("%H:%M:%S IST"),
            "market_open": is_market_open(), "count": len(quotes),
            "quotes": quotes, "debug": cache.get("debug", {})}


# ── PROXY ENDPOINTS FOR BREAKOUT SCANNER ──────────────────────────
@app.get("/api/intraday")
def proxy_intraday(
    security_id: str = Query(...), date: str = Query(...),
    interval: str = Query("5"),
    x_client_id: str = Header(None), x_access_token: str = Header(None),
):
    cid = x_client_id or CREDS["client_id"]
    tok = x_access_token or CREDS["access_token"]
    if not tok: return {"status": "no_credentials", "data": {}}
    headers = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    payload = {"securityId": security_id, "exchangeSegment": "NSE_EQ",
               "instrument": "EQUITY", "interval": interval, "fromDate": date, "toDate": date}
    try:
        resp = requests.post(f"{DHAN_BASE}/v2/charts/intraday", json=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("timestamp") and len(data["timestamp"]) > 0:
                return {"status": "ok", "source": "intraday", "data": data}
    except Exception as e:
        print(f"Intraday error {security_id}: {e}")

    # Fallback to daily
    try:
        today = datetime.now(IST)
        from_d = (today - timedelta(days=10)).strftime("%Y-%m-%d")
        to_d   = today.strftime("%Y-%m-%d")
        p2 = {"securityId": security_id, "exchangeSegment": "NSE_EQ",
              "instrument": "EQUITY", "expiryCode": 0, "fromDate": from_d, "toDate": to_d}
        r2 = requests.post(f"{DHAN_BASE}/v2/charts/historical", json=p2, headers=headers, timeout=10)
        if r2.status_code == 200:
            d2 = r2.json()
            if d2.get("timestamp"):
                idx = -1
                return {"status": "ok", "source": "daily_fallback", "data": {
                    "timestamp": [d2["timestamp"][idx]], "open":   [d2["open"][idx]],
                    "high":  [d2["high"][idx]],  "low":   [d2["low"][idx]],
                    "close": [d2["close"][idx]], "volume": [d2["volume"][idx]],
                }}
    except Exception as e:
        print(f"Daily fallback error {security_id}: {e}")
    return {"status": "no_data", "data": {}}


@app.get("/api/historical")
def proxy_historical(
    security_id: str = Query(...), from_date: str = Query(...), to_date: str = Query(...),
    x_client_id: str = Header(None), x_access_token: str = Header(None),
):
    cid = x_client_id or CREDS["client_id"]
    tok = x_access_token or CREDS["access_token"]
    if not tok: return {"status": "no_credentials", "data": {}}
    headers = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    payload = {"securityId": security_id, "exchangeSegment": "NSE_EQ",
               "instrument": "EQUITY", "expiryCode": 0,
               "fromDate": from_date, "toDate": to_date}
    try:
        resp = requests.post(f"{DHAN_BASE}/v2/charts/historical", json=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            return {"status": "ok", "data": resp.json()}
        return {"status": "error", "code": resp.status_code, "detail": resp.text[:150]}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/breakout/scan")
def breakout_scan_all(
    x_client_id: str = Header(None), x_access_token: str = Header(None),
):
    cid = x_client_id or CREDS["client_id"]
    tok = x_access_token or CREDS["access_token"]
    if not tok: return {"status": "no_credentials", "results": []}
    if not SYMBOLS: return {"status": "no_symbols", "results": []}

    today     = datetime.now(IST)
    from_date = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    headers   = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    results   = []

    for sym in SYMBOLS:
        try:
            payload = {"securityId": sym["security_id"], "exchangeSegment": "NSE_EQ",
                       "instrument": "EQUITY", "expiryCode": 0,
                       "fromDate": from_date, "toDate": to_date}
            resp = requests.post(f"{DHAN_BASE}/v2/charts/historical",
                                 json=payload, headers=headers, timeout=10)
            if resp.status_code != 200:
                time.sleep(0.3); continue

            data   = resp.json()
            ts     = data.get("timestamp", [])
            opens  = data.get("open",   [])
            highs  = data.get("high",   [])
            lows   = data.get("low",    [])
            closes = data.get("close",  [])
            if len(ts) < 2:
                time.sleep(0.3); continue

            history = []
            for i in range(max(0, len(ts) - 5), len(ts)):
                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                day_chg = round(((c - o) / o) * 100, 2) if o else 0
                rng     = round(((h - l) / l) * 100, 2) if l else 0
                sig     = "bull" if day_chg > 0.5 and rng > 0.5 else \
                          "bear" if day_chg < -0.5 and rng > 0.5 else "none"
                d = datetime.utcfromtimestamp(ts[i])
                history.append({"date": d.strftime("%d %b"), "open": round(o,2),
                                 "high": round(h,2), "low": round(l,2), "close": round(c,2),
                                 "chg_pct": day_chg, "signal": sig})

            last    = len(ts) - 1
            ltp     = round(closes[last], 2)
            day_o   = opens[last]
            day_h   = highs[last]
            day_l   = lows[last]
            day_chg = round(((ltp - day_o) / day_o) * 100, 2) if day_o else 0
            orb_high = round(day_h * 0.998, 2)
            orb_low  = round(day_l * 1.002, 2)
            orb_w    = round(((orb_high - orb_low) / orb_low) * 100, 2)
            signal   = "bull" if ltp > orb_high else "bear" if ltp < orb_low else \
                       "watch" if day_chg > 0.3 else "none"
            bull_days = sum(1 for h in history if h["signal"] == "bull")
            bear_days = sum(1 for h in history if h["signal"] == "bear")
            streak = 0
            if history:
                dir_ = history[-1]["signal"]
                if dir_ != "none":
                    for h in reversed(history):
                        if h["signal"] == dir_: streak += 1
                        else: break
                    if dir_ == "bear": streak = -streak
            score = (bull_days - bear_days) + \
                    (2 if signal == "bull" else -2 if signal == "bear" else 0) + streak

            results.append({
                "symbol": sym["symbol"], "exchange": sym["exchange"],
                "ltp": ltp, "change": day_chg,
                "orbHigh": orb_high, "orbLow": orb_low, "orbWidthPct": orb_w,
                "signal": signal,
                "strength": 3 if signal in ("bull","bear") and abs(day_chg) > 1.5 else
                            2 if signal in ("bull","bear") else
                            1 if signal == "watch" else 0,
                "history": history, "bullDays": bull_days, "bearDays": bear_days,
                "streak": streak, "score": score,
            })
            time.sleep(0.3)
        except Exception as e:
            print(f"Breakout scan error {sym['symbol']}: {e}")
            time.sleep(0.3)

    results.sort(key=lambda x: (
        3 if x["signal"] == "bull" else 2 if x["signal"] == "bear" else
        1 if x["signal"] == "watch" else 0), reverse=True)

    return {"status": "ok", "count": len(results),
            "updated_at": get_ist_now().strftime("%H:%M:%S IST"),
            "market_open": is_market_open(), "results": results}
