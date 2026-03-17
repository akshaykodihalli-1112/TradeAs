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

CSV_URL         = "https://images.dhan.co/api-data/api-scrip-master.csv"
CSV_URL_DETAIL  = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
_csv_eq_cache = {}   # symbol → security_id, populated from CSV
_csv_lock     = threading.Lock()



DISK_CACHE_PATH = "/tmp/dhan_nse_eq_ids.json"


def _save_symbols_to_disk():
    """Save current SYMBOLS to disk so they survive background thread restarts."""
    try:
        import json as _json
        with open(DISK_CACHE_PATH, "w") as f:
            _json.dump({"symbols": SYMBOLS, "source": cache.get("symbol_source"), 
                        "saved_at": get_ist_now().strftime("%H:%M:%S IST")}, f)
        print(f"[symbols] Saved {len(SYMBOLS)} symbols to disk cache.")
    except Exception as e:
        print(f"[symbols] Disk save failed: {e}")


def _load_symbols_from_disk() -> bool:
    """Load symbols from disk cache. Returns True if successful."""
    global SYMBOLS
    try:
        import json as _json
        with open(DISK_CACHE_PATH) as f:
            data = _json.load(f)
        syms = data.get("symbols", [])
        if len(syms) >= 100:
            SYMBOLS = syms
            cache["symbol_source"] = "disk_cache"
            print(f"[symbols] Loaded {len(SYMBOLS)} symbols from disk cache (saved {data.get('saved_at','?')})")
            return True
    except Exception:
        pass
    return False



def _parse_eq_lookup_from_csv(text: str) -> dict:
    """Parse Dhan compact CSV → {SYMBOL_NAME: security_id} for NSE_EQ rows only.
    Uses csv module to correctly handle quoted fields with commas inside."""
    import csv as _csv
    from io import StringIO as _StringIO
    lookup = {}
    reader = _csv.reader(_StringIO(text))
    header = None
    seg_i = sym_i = id_i = None
    for row in reader:
        if header is None:
            header = [h.strip().upper() for h in row]
            try:
                seg_i = next(i for i, h in enumerate(header) if "EXCH_SEG" in h or "SEGMENT" in h)
                sym_i = next(i for i, h in enumerate(header) if "TRADING_SYMBOL" in h or "SYMBOL_NAME" in h)
                id_i  = next(i for i, h in enumerate(header) if "SECURITY_ID" in h or "SCRIP_ID" in h or "SM_SYMBOL_ID" in h)
            except StopIteration:
                raise ValueError(f"CSV missing required columns. Found: {header[:10]}")
            continue
        if len(row) <= max(seg_i, sym_i, id_i):
            continue
        if row[seg_i].strip().upper() != "NSE_EQ":
            continue
        sym = row[sym_i].strip()
        if not sym or sym in lookup:
            continue
        try:
            lookup[sym] = str(int(float(row[id_i].strip())))
        except (ValueError, TypeError):
            pass
    return lookup


def fetch_fno_symbols(tok: str = ""):
    """
    Builds SYMBOLS with correct NSE_EQ security IDs. Three strategies in order:

    1. Dhan /v2/instrument/NSE_EQ  — fastest, uses token, returns JSON directly
    2. Dhan compact CSV            — no token needed, parse without pandas
    3. Emergency fallback          — hardcoded IDs, always works

    Both live sources are tried with retries before falling back.
    """
    global SYMBOLS, _csv_eq_cache
    tok = tok or CREDS.get("access_token", "")

    # ── Strategy 1: /v2/instrument/NSE_EQ  (returns CSV text, not JSON) ──
    if tok:
        try:
            print("[symbols] Trying /v2/instrument/NSE_EQ...")
            headers = {"access-token": tok, "client-id": CREDS.get("client_id",""),
                       "Content-Type": "application/json"}
            resp = requests.get(f"{DHAN_BASE}/v2/instrument/NSE_EQ",
                                headers=headers, timeout=20)
            print(f"[symbols] /v2/instrument/NSE_EQ → {resp.status_code} len={len(resp.text)}")
            if resp.status_code == 200 and len(resp.text) > 1000:
                # Dhan returns CSV text from this endpoint, not JSON
                text = resp.text
                if text.lstrip().startswith("[") or text.lstrip().startswith("{"):
                    # It IS json on some accounts
                    import json as _json
                    instruments = _json.loads(text)
                    eq_lookup = {}
                    for inst in instruments:
                        sym = (inst.get("tradingSymbol") or inst.get("SEM_TRADING_SYMBOL") or
                               inst.get("symbolName")    or inst.get("SM_SYMBOL_NAME") or "").strip()
                        sid_raw = (inst.get("securityId") or inst.get("SEM_SMST_SECURITY_ID") or
                                   inst.get("security_id") or "")
                        if sym and sid_raw and sym not in eq_lookup:
                            try:
                                eq_lookup[sym] = str(int(float(str(sid_raw))))
                            except (ValueError, TypeError):
                                pass
                else:
                    # Parse as CSV (same format as scrip-master.csv but NSE_EQ only)
                    eq_lookup = _parse_eq_lookup_from_csv(text)

                print(f"[symbols] /v2/instrument parsed: {len(eq_lookup)} symbols")
                if len(eq_lookup) > 100:
                    _csv_eq_cache = eq_lookup
                    matched, unmatched = _match_fno_to_eq(eq_lookup)
                    if len(matched) >= 50:
                        SYMBOLS = matched
                        cache["symbol_source"] = "api_dynamic"
                        print(f"[symbols] ✓ instrument API: {len(SYMBOLS)} matched, {len(unmatched)} unmatched")
                        return
            print(f"[symbols] instrument API unusable — trying public CSV...")
        except Exception as e:
            print(f"[symbols] instrument API failed: {e} — trying CSV...")


    # ── Strategy 2: Public compact CSV (no token needed) ────────────────
    try:
        print("[symbols] Trying public compact CSV...")
        resp = None
        for url in [CSV_URL, CSV_URL_DETAIL]:
            for attempt in range(2):
                try:
                    print(f"  CSV {url.split('/')[-1]} attempt {attempt+1}...")
                    resp = requests.get(url, timeout=40)
                    print(f"  → status={resp.status_code} len={len(resp.text)}")
                    if resp.status_code == 200 and len(resp.text) > 50000:
                        break
                    resp = None
                except Exception as ce:
                    print(f"  → error: {ce}")
                    resp = None
                time.sleep(4)
            if resp and resp.status_code == 200 and len(resp.text) > 50000:
                break

        if not resp or resp.status_code != 200 or len(resp.text) < 50000:
            raise ConnectionError(
                f"Both CSV URLs failed — "
                f"status={getattr(resp,'status_code','N/A')} "
                f"len={len(getattr(resp,'text','') if resp else '')}"
            )

        eq_lookup = _parse_eq_lookup_from_csv(resp.text)
        print(f"[symbols] CSV parsed: {len(eq_lookup)} NSE_EQ symbols")
        _csv_eq_cache = eq_lookup

        matched, unmatched = _match_fno_to_eq(eq_lookup)
        print(f"[symbols] CSV match: {len(matched)} matched, {len(unmatched)} unmatched: {unmatched[:10]}")
        if len(matched) < 50:
            raise ValueError(f"Only {len(matched)} FNO symbols matched in CSV — CSV may be truncated")

        SYMBOLS = matched
        cache["symbol_source"] = "csv_dynamic"
        print(f"[symbols] ✓ CSV loaded: {len(SYMBOLS)} symbols")
        return

    except Exception as e:
        print(f"[symbols] ✗ CSV failed: {e}")


    # ── Strategy 3: Emergency fallback ───────────────────────────────────
    if not SYMBOLS:
        _load_emergency_symbols()
    else:
        print(f"[symbols] Keeping existing {len(SYMBOLS)} ({cache['symbol_source']})")


def _match_fno_to_eq(eq_lookup: dict):
    """Cross-match FNO_SYMBOL_NAMES against NSE_EQ lookup → (matched, unmatched)."""
    matched, unmatched = [], []
    for sym in sorted(set(FNO_SYMBOL_NAMES)):
        sid = eq_lookup.get(sym)
        if sid:
            matched.append({"symbol": sym, "security_id": sid, "exchange": "NSE"})
        else:
            unmatched.append(sym)
    return matched, unmatched



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
        # If we're on emergency_fallback, try to auto-fix IDs for missing symbols
        # by querying them on BSE_EQ — if found there, their IDs are BSE codes
        # and we should skip them (we only want NSE_EQ data)
        # Schedule a background symbol refresh to get correct IDs
        if cache.get("symbol_source") == "emergency_fallback" and len(missing) > 10:
            print("  ⚠ Many symbols missing on emergency_fallback — scheduling symbol refresh")
            def _bg_refresh():
                time.sleep(2)
                fetch_fno_symbols(tok)
            threading.Thread(target=_bg_refresh, daemon=True).start()

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
    """Fetch last 30 days of daily OHLCV. Returns full data dict or None."""
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
            if resp.status_code in (400, 401, 403): return None
            resp.raise_for_status()
            data = resp.json()
            if not data.get("timestamp"): return None
            return data   # full dict: timestamp, open, high, low, close, volume
        except Exception:
            time.sleep(1)
    return None


def _run_screener(tok):
    global cache

    # Load symbols if not yet loaded — synchronous, happens before any screener work
    if not SYMBOLS or cache.get("symbol_source") in ("none", "no_symbols"):
        print(f"[screener] Loading symbols (source={cache.get('symbol_source')})...")
        fetch_fno_symbols(tok)
        if cache.get("symbol_source") in ("api_dynamic", "csv_dynamic"):
            _save_symbols_to_disk()

    if not SYMBOLS:
        cache.update({"status": "error", "progress": 100,
                      "errors": ["No symbols loaded. Check Render logs and ensure CSV is reachable."]})
        return

    cid   = CREDS["client_id"]
    total = len(SYMBOLS)
    cache.update({"status": "fetching", "progress": 0, "total": total, "market_open": is_market_open()})
    print(f"Screener — {total} stocks ({cache.get('symbol_source')}) | {get_ist_now().strftime('%H:%M:%S IST')}")

    if cache.get("symbol_source") not in ("api_dynamic", "csv_dynamic", "disk_cache"):
        print(f"[screener] ⚠ Source={cache.get('symbol_source')} — IDs may be incorrect")

    quotes = fetch_all_quotes(cid, tok)
    if not quotes:
        cache.update({"status": "error", "progress": 100,
                      "errors": [f"marketfeed returned 0 quotes. debug: {cache['debug']}"]})
        return

    market_open = is_market_open()
    today_str   = get_ist_now().strftime("%Y-%m-%d")

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
            today_vol = q["volume"]

            hist = get_historical(sym["security_id"], tok)

            closes  = hist.get("close",     []) if hist else []
            volumes = hist.get("volume",    []) if hist else []
            stamps  = hist.get("timestamp", []) if hist else []

            # ── Change % logic ──────────────────────────────────────────
            # Market OPEN  → live: (ltp - prev_close) / prev_close
            #   prev_close = yesterday's closing price from historical
            # Market CLOSED → daily: (today_close - prev_close) / prev_close
            #   today_close  = last bar in historical (today's session close)
            #   prev_close   = second-to-last bar in historical
            chg_pct    = 0.0
            prev_close = 0.0
            today_close = 0.0

            if closes and len(closes) >= 2:
                # Check if latest historical bar is today's date
                last_ts   = stamps[-1] if stamps else 0
                last_date = datetime.utcfromtimestamp(last_ts).strftime("%Y-%m-%d") if last_ts else ""
                has_today = (last_date == today_str)

                if market_open:
                    # Live: use yesterday close as prev_close
                    prev_close = closes[-1] if has_today else closes[-1]
                    # Actually for live, prev_close from quote API is most accurate
                    prev_close = q.get("prev_close") or closes[-2] if has_today else closes[-1]
                    if prev_close:
                        chg_pct = round(((ltp - prev_close) / prev_close) * 100, 2)
                else:
                    # Closed: (last_close - second_last_close) / second_last_close
                    if has_today:
                        today_close = closes[-1]
                        prev_close  = closes[-2]
                    else:
                        # No today bar yet (pre-market or holiday) — use last two bars
                        today_close = closes[-1]
                        prev_close  = closes[-2]
                    if prev_close:
                        chg_pct = round(((today_close - prev_close) / prev_close) * 100, 2)
            else:
                # Fallback to quote API's change%
                chg_pct = q.get("change_pct", 0.0)

            # ── Momentum 5D ─────────────────────────────────────────────
            momentum5d = 0.0
            if closes and len(closes) >= 6:
                close_5ago = closes[-6]
                ref_price  = ltp if market_open else (closes[-1] if closes else ltp)
                momentum5d = round(((ref_price - close_5ago) / close_5ago) * 100, 2) if close_5ago else 0

            # ── Volume ratio ─────────────────────────────────────────────
            vol_ratio  = 0.0
            avg_vol_7d = 0
            if volumes and len(volumes) >= 8:
                avg_vol_7d = sum(volumes[-8:-1]) / 7
                vol_ratio  = round(today_vol / avg_vol_7d, 2) if avg_vol_7d > 0 else 0
            elif today_vol > 0:
                vol_ratio = 1.0

            results.append({
                "symbol":     sym["symbol"], "exchange": sym["exchange"],
                "ltp":        ltp,
                "prev_close": round(prev_close, 2),
                "change":     chg_pct,
                "momentum5d": momentum5d, "volumeRatio": vol_ratio,
                "todayVol":   today_vol,  "avgVol7d": int(avg_vol_7d),
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
    if not cid or not tok:
        cache["status"] = "no_credentials"
        print("[screener] No credentials — pass x-client-id and x-access-token headers")
        return
    # Always update CREDS so boot thread + scheduler can use them
    CREDS["client_id"]    = cid
    CREDS["access_token"] = tok
    if _screener_lock.locked():
        print("[screener] Already running, skipping duplicate")
        return
    def _worker():
        with _screener_lock:
            _run_screener(tok)
    threading.Thread(target=_worker, daemon=True).start()


def scheduled_refresh():
    if not CREDS["client_id"] or not CREDS["access_token"]: return
    # Refresh symbols from network if not yet on live IDs
    if cache.get("symbol_source") not in ("api_dynamic", "csv_dynamic"):
        print(f"[scheduler] Symbol source is '{cache.get('symbol_source')}' — refreshing...")
        fetch_fno_symbols(CREDS["access_token"])
        if cache.get("symbol_source") in ("api_dynamic", "csv_dynamic"):
            _save_symbols_to_disk()
    fetch_screener()


scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_refresh,  "interval", minutes=5,   id="screener")
scheduler.add_job(keep_alive,         "interval", minutes=10,  id="keepalive")
# csv_refresh runs via scheduled_refresh which has token from CREDS
scheduler.start()


@app.on_event("startup")
async def startup():
    """Boot: load disk cache → try CSV → log result. Screener starts on first API call."""
    def _boot():
        tok = CREDS.get("access_token", "")
        cid = CREDS.get("client_id", "")
        print(f"[boot] start | cid={'yes' if cid else 'NO'} tok={'yes' if tok else 'NO'}")

        # Load disk cache first (instant)
        _load_symbols_from_disk()

        # Try CSV (no token needed)
        fetch_fno_symbols(tok)
        src = cache.get("symbol_source", "none")
        print(f"[boot] done  | source={src} symbols={len(SYMBOLS)}")

        if src in ("api_dynamic", "csv_dynamic"):
            _save_symbols_to_disk()

        # Auto-start screener if credentials available
        if SYMBOLS and cid and tok:
            fetch_screener(cid, tok)

    threading.Thread(target=_boot, daemon=True).start()


# ── ROUTES ────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "DhanScreen API", "docs": "/docs",
            "time_ist": get_ist_now().strftime("%H:%M:%S"), "market_open": is_market_open()}


@app.get("/api/boot")
@app.post("/api/boot")
def manual_boot(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    """
    Call this endpoint once after deploy to force symbol load + first screener run.
    GET https://tradeas.onrender.com/api/boot
    Headers: x-client-id, x-access-token
    Returns immediately with status — screener runs in background.
    """
    cid = x_client_id or CREDS.get("client_id", "")
    tok = x_access_token or CREDS.get("access_token", "")
    if cid: CREDS["client_id"] = cid
    if tok: CREDS["access_token"] = tok

    steps = []

    # Step 1: disk cache
    if not SYMBOLS:
        if _load_symbols_from_disk():
            steps.append(f"disk_cache: loaded {len(SYMBOLS)} symbols")
        else:
            steps.append("disk_cache: empty")

    # Step 2: CSV (no token needed)
    if not SYMBOLS or cache.get("symbol_source") in ("none","no_symbols","disk_cache"):
        steps.append("csv_fetch: starting...")
        try:
            fetch_fno_symbols(tok)
            src = cache.get("symbol_source","?")
            steps.append(f"csv_fetch: done → source={src} symbols={len(SYMBOLS)}")
            if src in ("api_dynamic","csv_dynamic"):
                _save_symbols_to_disk()
                steps.append("disk_save: ok")
        except Exception as e:
            steps.append(f"csv_fetch: ERROR {e}")

    # Step 3: trigger screener
    if SYMBOLS and (cid or tok):
        fetch_screener(cid, tok)
        steps.append(f"screener: triggered ({len(SYMBOLS)} symbols)")
    elif not SYMBOLS:
        steps.append("screener: SKIPPED — no symbols loaded")
    else:
        steps.append("screener: SKIPPED — no credentials")

    return {
        "symbol_count":  len(SYMBOLS),
        "symbol_source": cache.get("symbol_source"),
        "steps":         steps,
        "has_creds":     bool(cid and tok),
        "next":          "Poll /api/health to track progress" if SYMBOLS else
                         "Set DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN env vars on Render, then redeploy"
    }


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
    cid = x_client_id or CREDS.get("client_id","")
    tok = x_access_token or CREDS.get("access_token","")
    if cid: CREDS["client_id"] = cid
    if tok: CREDS["access_token"] = tok
    # Trigger screener if cache is stale/idle and credentials available
    if cid and tok:
        if cache.get("status") in ("idle", "no_credentials", "error") or not cache.get("data"):
            fetch_screener(cid, tok)
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
def refresh_symbols(x_access_token: str = Header(None)):
    """Force re-fetch of symbol→ID map from Dhan API or CSV.
    Call this anytime a symbol shows wrong price or a new FNO stock is listed."""
    tok = x_access_token or CREDS.get("access_token", "")
    before = len(SYMBOLS)
    fetch_fno_symbols(tok)
    if cache.get("symbol_source") in ("api_dynamic", "csv_dynamic"):
        _save_symbols_to_disk()
    return {
        "status": "ok",
        "before": before,
        "after": len(SYMBOLS),
        "source": cache.get("symbol_source"),
        "unmatched_sample": [s for s in FNO_SYMBOL_NAMES if s not in {x["symbol"] for x in SYMBOLS}][:10],
        "sample": SYMBOLS[:5],
    }


@app.get("/api/debug/ids")
def debug_ids(x_access_token: str = Header(None)):
    """
    Diagnostic endpoint — shows which symbols have no quote from Dhan marketfeed.
    Use this to identify wrong security IDs.
    Call after market hours to see which 60 symbols return no data.
    Also triggers a fresh symbol refresh using your token.
    """
    tok = x_access_token or CREDS.get("access_token", "")
    
    # Try to refresh symbols first if token provided
    if tok and cache.get("symbol_source") in ("emergency_fallback", "loading", None):
        fetch_fno_symbols(tok)
    
    last_debug = cache.get("debug", {})
    missing    = last_debug.get("missing_sample", [])
    
    return {
        "symbol_count":   len(SYMBOLS),
        "symbol_source":  cache.get("symbol_source"),
        "quotes_fetched": last_debug.get("quotes_fetched", 0),
        "missing_count":  last_debug.get("missing_count", 0),
        "missing_sample": missing,
        "all_symbols": [
            {"symbol": s["symbol"], "security_id": s["security_id"]}
            for s in SYMBOLS
            if s["symbol"] in set(last_debug.get("missing_sample", []))
        ],
        "note": "If symbol_source is emergency_fallback, IDs may be wrong. Deploy new code and call POST /api/symbols/refresh with your token."
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
