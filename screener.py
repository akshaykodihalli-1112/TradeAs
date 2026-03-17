from fastapi import FastAPI, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import requests, threading, time, os, json, csv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

app = FastAPI(title="DhanScreen API", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DHAN_BASE = "https://api.dhan.co"
IST       = ZoneInfo("Asia/Kolkata")
SELF_URL  = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

# ── Credentials (set as Render env vars) ──────────────────────────────────────
CREDS = {
    "client_id":    os.getenv("DHAN_CLIENT_ID", ""),
    "access_token": os.getenv("DHAN_ACCESS_TOKEN", ""),
}

# ── In-memory cache ────────────────────────────────────────────────────────────
cache = {
    "data": [], "updated_at": None, "status": "idle",
    "count": 0, "errors": [], "progress": 0, "total": 0,
    "symbol_source": "none", "market_open": False, "debug": {},
}

SYMBOLS = []          # list of {symbol, security_id, exchange}
_lock   = threading.Lock()

# ── FNO symbol names (no IDs — IDs come from CSV) ─────────────────────────────
FNO_NAMES = [
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

# ── Helpers ────────────────────────────────────────────────────────────────────
def ist_now(): return datetime.now(IST)
def is_market_open():
    n = ist_now()
    if n.weekday() >= 5: return False
    return n.replace(hour=9,minute=15,second=0,microsecond=0) <= n <= n.replace(hour=15,minute=30,second=0,microsecond=0)


# ── Symbol loading ─────────────────────────────────────────────────────────────
DISK_PATH = "/tmp/dhan_symbols.json"

def save_symbols():
    try:
        with open(DISK_PATH, "w") as f:
            json.dump({"symbols": SYMBOLS, "source": cache["symbol_source"],
                       "at": ist_now().strftime("%H:%M IST")}, f)
        print(f"[sym] saved {len(SYMBOLS)} to disk")
    except Exception as e:
        print(f"[sym] disk save failed: {e}")

def load_symbols_disk():
    global SYMBOLS
    try:
        with open(DISK_PATH) as f:
            d = json.load(f)
        if len(d.get("symbols", [])) >= 100:
            SYMBOLS = d["symbols"]
            cache["symbol_source"] = "disk"
            print(f"[sym] loaded {len(SYMBOLS)} from disk (saved {d.get('at','?')})")
            return True
    except Exception:
        pass
    return False

def load_symbols_csv(tok=""):
    """Download Dhan scrip master CSV, extract NSE_EQ IDs for FNO symbols."""
    global SYMBOLS
    for url in [
        "https://images.dhan.co/api-data/api-scrip-master.csv",
        "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
    ]:
        try:
            print(f"[sym] fetching {url} ...")
            r = requests.get(url, timeout=45)
            print(f"[sym] {url.split('/')[-1]}: status={r.status_code} len={len(r.text)}")
            if r.status_code != 200 or len(r.text) < 10000:
                continue

            # Parse CSV with csv module (handles quoted commas correctly)
            reader = csv.reader(StringIO(r.text))
            header = None
            seg_i = sym_i = id_i = inst_i = None
            eq_lookup   = {}   # symbol → security_id for NSE_EQ rows
            fno_symbols = set()

            for row in reader:
                if header is None:
                    header = [h.strip().upper() for h in row]
                    def fi(*kws): return next((i for i,h in enumerate(header) if any(k in h for k in kws)), None)
                    seg_i  = fi("EXCH_SEG","SEGMENT","SEM_EXM_EXCH_ID")
                    sym_i  = fi("TRADING_SYMBOL","SYMBOL_NAME","SM_SYMBOL_NAME")
                    id_i   = fi("SECURITY_ID","SCRIP_ID","SM_SYMBOL_ID","SMST_SECURITY_ID")
                    inst_i = fi("INSTRUMENT","SEM_INSTRUMENT")
                    print(f"[sym] cols: seg={seg_i} sym={sym_i} id={id_i} inst={inst_i}")
                    if None in (seg_i, sym_i, id_i):
                        print(f"[sym] bad header: {header[:8]}")
                        break
                    continue

                if len(row) <= max(seg_i, sym_i, id_i):
                    continue

                seg = row[seg_i].strip().upper()
                sym = row[sym_i].strip()
                if not sym:
                    continue

                if seg == "NSE_EQ" and sym not in eq_lookup:
                    try: eq_lookup[sym] = str(int(float(row[id_i].strip())))
                    except: pass

                elif seg == "NSE_FNO" and inst_i is not None:
                    inst = row[inst_i].strip().upper()
                    if inst in ("FUTSTK", "OPTSTK"):
                        fno_symbols.add(sym)

            print(f"[sym] eq_lookup={len(eq_lookup)} fno_symbols={len(fno_symbols)}")

            # Combine: CSV FNO names + our known list
            all_names = (fno_symbols | set(FNO_NAMES)) if fno_symbols else set(FNO_NAMES)
            matched = []
            for s in sorted(all_names):
                sid = eq_lookup.get(s)
                if sid:
                    matched.append({"symbol": s, "security_id": sid, "exchange": "NSE"})

            print(f"[sym] matched {len(matched)} FNO symbols to NSE_EQ IDs")
            if len(matched) < 50:
                print(f"[sym] too few matches, skipping this URL")
                continue

            SYMBOLS = matched
            cache["symbol_source"] = "csv"
            save_symbols()
            return True

        except Exception as e:
            print(f"[sym] {url} failed: {e}")

    return False


def ensure_symbols(tok=""):
    """Make sure SYMBOLS is loaded. Returns True if ready."""
    if len(SYMBOLS) >= 50:
        return True
    print("[sym] symbols empty — loading...")
    if load_symbols_disk():
        return True
    if load_symbols_csv(tok):
        return True
    print("[sym] ✗ could not load symbols")
    return False


# ── Quote fetching ─────────────────────────────────────────────────────────────
def get_all_quotes(tok):
    """Fetch marketfeed quotes for all SYMBOLS in batches of 50."""
    cid      = CREDS["client_id"]
    headers  = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    id_map   = {sym["security_id"]: sym["symbol"] for sym in SYMBOLS}  # id → symbol
    quotes   = {}

    for i in range(0, len(SYMBOLS), 50):
        batch   = [int(s["security_id"]) for s in SYMBOLS[i:i+50]]
        payload = {"NSE_EQ": batch}

        for endpoint in ["/v2/marketfeed/quote", "/v2/marketfeed/ohlc"]:
            for attempt in range(3):
                try:
                    r = requests.post(f"{DHAN_BASE}{endpoint}", json=payload,
                                      headers=headers, timeout=15)
                    print(f"  quote batch={i} {endpoint} → {r.status_code}")
                    if r.status_code == 200:
                        for sec_id, q in r.json().get("data", {}).get("NSE_EQ", {}).items():
                            sym = id_map.get(str(int(float(sec_id))))
                            if sym:
                                ohlc = q.get("ohlc", {})
                                ltp  = float(q.get("last_price", 0))
                                quotes[sym] = {
                                    "ltp":        round(ltp, 2),
                                    "prev_close": round(float(ohlc.get("close", 0)), 2),
                                    "volume":     int(q.get("volume", 0)),
                                    "open":       round(float(ohlc.get("open",  0)), 2),
                                    "high":       round(float(ohlc.get("high",  0)), 2),
                                    "low":        round(float(ohlc.get("low",   0)), 2),
                                }
                        break  # success
                    elif r.status_code == 429:
                        time.sleep(5 * (attempt + 1))
                    else:
                        break  # non-retryable, try other endpoint
                except Exception as e:
                    print(f"  quote error: {e}")
                    time.sleep(2)
            if id_map.get(str(batch[0])) in quotes:
                break  # first symbol in batch resolved → endpoint worked
        time.sleep(1.2)

    missing = [s["symbol"] for s in SYMBOLS if s["symbol"] not in quotes]
    print(f"[quotes] got={len(quotes)} missing={len(missing)} sample={missing[:5]}")
    cache["debug"] = {"quotes_fetched": len(quotes), "missing_count": len(missing),
                      "missing_sample": missing[:10]}
    return quotes


def get_historical(security_id, tok):
    """Fetch 30 days daily OHLCV. Returns dict with lists or None."""
    today = ist_now()
    payload = {
        "securityId": security_id, "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY", "expiryCode": 0,
        "fromDate": (today - timedelta(days=30)).strftime("%Y-%m-%d"),
        "toDate":   today.strftime("%Y-%m-%d"),
    }
    headers = {"access-token": tok, "Content-Type": "application/json"}
    for attempt in range(3):
        try:
            r = requests.post(f"{DHAN_BASE}/v2/charts/historical",
                              json=payload, headers=headers, timeout=10)
            if r.status_code == 429: time.sleep(3 * (attempt+1)); continue
            if r.status_code != 200: return None
            d = r.json()
            return d if d.get("timestamp") else None
        except Exception:
            time.sleep(1)
    return None


# ── Screener run ───────────────────────────────────────────────────────────────
def run_screener(tok):
    global cache

    # 1. Ensure symbols loaded
    if not ensure_symbols(tok):
        cache.update({"status": "error", "errors": ["Cannot load symbol IDs. Check Render logs."]})
        return

    cid   = CREDS["client_id"]
    total = len(SYMBOLS)
    cache.update({"status": "fetching", "progress": 0, "total": total,
                  "market_open": is_market_open()})
    print(f"[screener] {total} symbols | {ist_now().strftime('%H:%M:%S IST')}")

    # 2. Get live quotes
    quotes = get_all_quotes(tok)
    if not quotes:
        cache.update({"status": "error", "errors": ["Marketfeed returned 0 quotes"]})
        return

    # 3. Build results
    market_open = is_market_open()
    today_str   = ist_now().strftime("%Y-%m-%d")
    results, skipped = [], []

    for i, sym in enumerate(SYMBOLS):
        try:
            q = quotes.get(sym["symbol"])
            if not q:
                skipped.append(f"{sym['symbol']}:no_quote")
                cache["progress"] = round((i+1)/total*100)
                continue

            ltp       = q["ltp"]
            today_vol = q["volume"]

            # Historical data for change%, momentum, vol ratio
            hist    = get_historical(sym["security_id"], tok)
            closes  = hist.get("close",     []) if hist else []
            volumes = hist.get("volume",    []) if hist else []
            stamps  = hist.get("timestamp", []) if hist else []

            # ── Change % ──────────────────────────────────────────────
            # Market open  → (ltp - yesterday_close) / yesterday_close
            # Market closed → (today_close - yesterday_close) / yesterday_close
            chg_pct    = 0.0
            prev_close = 0.0

            if len(closes) >= 2:
                last_date = datetime.utcfromtimestamp(stamps[-1]).strftime("%Y-%m-%d") if stamps else ""
                has_today = (last_date == today_str)

                if market_open:
                    # yesterday close = last hist bar if today bar exists, else latest
                    prev_close = closes[-2] if has_today else closes[-1]
                    if prev_close:
                        chg_pct = round(((ltp - prev_close) / prev_close) * 100, 2)
                else:
                    # closed: today close vs yesterday close
                    today_close = closes[-1] if has_today else closes[-1]
                    prev_close  = closes[-2] if has_today else (closes[-2] if len(closes) >= 2 else 0)
                    if prev_close:
                        chg_pct = round(((today_close - prev_close) / prev_close) * 100, 2)
            elif q["prev_close"]:
                # fallback to quote API prev_close
                prev_close = q["prev_close"]
                if prev_close:
                    chg_pct = round(((ltp - prev_close) / prev_close) * 100, 2)

            # ── Momentum 5D ───────────────────────────────────────────
            momentum5d = 0.0
            if len(closes) >= 6:
                ref = ltp if market_open else closes[-1]
                c5  = closes[-6]
                if c5: momentum5d = round(((ref - c5) / c5) * 100, 2)

            # ── Vol ratio ─────────────────────────────────────────────
            vol_ratio = 0.0
            avg_vol7d = 0
            if len(volumes) >= 8:
                avg_vol7d = sum(volumes[-8:-1]) / 7
                vol_ratio = round(today_vol / avg_vol7d, 2) if avg_vol7d else 0
            elif today_vol > 0:
                vol_ratio = 1.0

            results.append({
                "symbol":     sym["symbol"],
                "exchange":   "NSE",
                "ltp":        ltp,
                "prev_close": round(prev_close, 2),
                "change":     chg_pct,
                "momentum5d": momentum5d,
                "volumeRatio": vol_ratio,
                "todayVol":   today_vol,
                "avgVol7d":   int(avg_vol7d),
            })
        except Exception as e:
            skipped.append(f"{sym['symbol']}:{e}")

        cache["progress"] = round((i+1)/total*100)
        time.sleep(0.3)
        if (i+1) % 20 == 0:
            print(f"  {i+1}/{total} ok={len(results)}")
            time.sleep(1.5)

    results.sort(key=lambda x: x["volumeRatio"], reverse=True)
    cache.update({
        "data": results, "updated_at": ist_now().strftime("%H:%M:%S IST"),
        "status": "ok", "count": len(results), "errors": [],
        "skipped": skipped[:20], "progress": 100,
        "market_open": is_market_open(),
    })
    print(f"[screener] done — {len(results)} ok | {len(skipped)} skipped")


def trigger_screener(cid="", tok=""):
    cid = cid or CREDS["client_id"]
    tok = tok or CREDS["access_token"]
    if not cid or not tok:
        cache["status"] = "no_credentials"
        print("[screener] no credentials")
        return
    CREDS["client_id"]    = cid
    CREDS["access_token"] = tok
    if _lock.locked():
        print("[screener] already running")
        return
    threading.Thread(target=lambda: _lock.__enter__() or run_screener(tok) or _lock.__exit__(None,None,None), daemon=True).start()


def _locked_run(tok):
    with _lock:
        run_screener(tok)


def trigger_screener(cid="", tok=""):
    cid = cid or CREDS["client_id"]
    tok = tok or CREDS["access_token"]
    if not cid or not tok:
        cache["status"] = "no_credentials"
        return
    CREDS["client_id"]    = cid
    CREDS["access_token"] = tok
    if _lock.locked(): return
    threading.Thread(target=_locked_run, args=(tok,), daemon=True).start()


# ── Scheduler ─────────────────────────────────────────────────────────────────
def scheduled_job():
    tok = CREDS.get("access_token", "")
    cid = CREDS.get("client_id", "")
    if not tok or not cid: return
    # Refresh CSV IDs daily at 8:30 AM
    now = ist_now()
    if now.hour == 8 and now.minute < 10:
        load_symbols_csv(tok)
    trigger_screener(cid, tok)

def keep_alive():
    if not SELF_URL: return
    try: requests.get(f"{SELF_URL}/api/health", timeout=10)
    except: pass

scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_job, "interval", minutes=5,  id="screener")
scheduler.add_job(keep_alive,    "interval", minutes=10, id="keepalive")
scheduler.start()


# ── Startup ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    def _boot():
        tok = CREDS.get("access_token", "")
        cid = CREDS.get("client_id", "")
        print(f"[boot] cid={'SET' if cid else 'MISSING'} tok={'SET' if tok else 'MISSING'}")
        load_symbols_disk()          # instant — try disk first
        load_symbols_csv(tok)        # then try network
        if SYMBOLS and cid and tok:
            trigger_screener(cid, tok)
        else:
            print(f"[boot] symbols={len(SYMBOLS)} cid={bool(cid)} tok={bool(tok)} — skipping screener")
    threading.Thread(target=_boot, daemon=True).start()


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "time_ist": ist_now().strftime("%H:%M:%S"),
            "market_open": is_market_open(), "docs": "/docs"}


@app.get("/api/health")
def health():
    return {
        "status":         "ok",
        "cache_status":   cache["status"],
        "last_refresh":   cache["updated_at"],
        "symbol_count":   len(SYMBOLS),
        "symbol_source":  cache["symbol_source"],
        "result_count":   cache["count"],
        "error_count":    len(cache.get("errors", [])),
        "skipped_count":  len(cache.get("skipped", [])),
        "skipped_sample": cache.get("skipped", [])[:5],
        "progress_pct":   cache.get("progress", 0),
        "market_open":    is_market_open(),
        "time_ist":       ist_now().strftime("%H:%M:%S"),
        "day":            ist_now().strftime("%A"),
        "debug":          cache.get("debug", {}),
    }


@app.get("/api/screener")
def get_screener(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    cid = x_client_id    or CREDS.get("client_id", "")
    tok = x_access_token or CREDS.get("access_token", "")
    if cid: CREDS["client_id"]    = cid
    if tok: CREDS["access_token"] = tok
    # Trigger if idle/empty
    if cid and tok and cache.get("status") in ("idle", "no_credentials", "error"):
        trigger_screener(cid, tok)
    return cache


@app.get("/api/boot")
@app.post("/api/boot")
def boot(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    """Force symbol load + screener. Call once after deploy."""
    cid = x_client_id    or CREDS.get("client_id", "")
    tok = x_access_token or CREDS.get("access_token", "")
    if cid: CREDS["client_id"]    = cid
    if tok: CREDS["access_token"] = tok

    # Load symbols synchronously so we can report result
    ok = ensure_symbols(tok)
    if ok:
        trigger_screener(cid, tok)

    return {
        "symbols_loaded": len(SYMBOLS),
        "symbol_source":  cache["symbol_source"],
        "screener":       "triggered" if (ok and cid and tok) else "skipped",
        "has_creds":      bool(cid and tok),
        "tip": ("Screener running. Poll /api/health for progress."
                if ok else
                "Symbol load failed. Check /api/debug for details."),
    }


@app.get("/api/debug")
def debug(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    """Diagnostic — shows what's happening internally."""
    cid = x_client_id    or CREDS.get("client_id", "")
    tok = x_access_token or CREDS.get("access_token", "")
    return {
        "symbol_count":  len(SYMBOLS),
        "symbol_source": cache["symbol_source"],
        "screener_status": cache["status"],
        "has_client_id":   bool(cid),
        "has_token":       bool(tok),
        "env_client_id":   bool(os.getenv("DHAN_CLIENT_ID")),
        "env_token":       bool(os.getenv("DHAN_ACCESS_TOKEN")),
        "lock_held":       _lock.locked(),
        "sample_symbols":  SYMBOLS[:3],
        "last_debug":      cache.get("debug", {}),
    }


@app.post("/api/symbols/refresh")
def refresh_symbols(x_access_token: str = Header(None)):
    tok = x_access_token or CREDS.get("access_token", "")
    ok  = load_symbols_csv(tok)
    return {"ok": ok, "count": len(SYMBOLS), "source": cache["symbol_source"]}


@app.get("/api/symbols")
def list_symbols():
    return {"count": len(SYMBOLS), "source": cache["symbol_source"], "symbols": SYMBOLS}


@app.get("/api/screener/top")
def top(limit: int = 20):
    return {"data": cache["data"][:limit]}


@app.get("/api/screener/gainers")
def gainers():
    return {"data": sorted([x for x in cache["data"] if x["change"] > 0],
                            key=lambda x: x["change"], reverse=True)[:15]}


@app.get("/api/screener/losers")
def losers():
    return {"data": sorted([x for x in cache["data"] if x["change"] < 0],
                            key=lambda x: x["change"])[:15]}


@app.get("/api/screener/volume-spike")
def vol_spike(min_ratio: float = 2.0):
    return {"data": [x for x in cache["data"] if x["volumeRatio"] >= min_ratio]}


@app.get("/api/screener/momentum")
def momentum(min_pct: float = 3.0):
    return {"data": [x for x in cache["data"] if x["momentum5d"] >= min_pct]}


@app.get("/api/ping-dhan")
def ping_dhan(x_access_token: str = Header(None)):
    tok = x_access_token or CREDS.get("access_token", "")
    if not tok: return {"connected": False, "error": "No token"}
    try:
        r = requests.get(f"{DHAN_BASE}/v2/profile",
                         headers={"access-token": tok}, timeout=5)
        return {"connected": r.status_code == 200, "status": r.status_code}
    except Exception as e:
        return {"connected": False, "error": str(e)}


# ── Historical proxy (for breakout scanner) ───────────────────────────────────
@app.get("/api/historical")
def proxy_historical(
    security_id: str = Query(...), from_date: str = Query(...), to_date: str = Query(...),
    x_access_token: str = Header(None),
):
    tok = x_access_token or CREDS.get("access_token", "")
    if not tok: return {"status": "no_credentials"}
    headers = {"access-token": tok, "Content-Type": "application/json"}
    payload = {"securityId": security_id, "exchangeSegment": "NSE_EQ",
               "instrument": "EQUITY", "expiryCode": 0,
               "fromDate": from_date, "toDate": to_date}
    try:
        r = requests.post(f"{DHAN_BASE}/v2/charts/historical",
                          json=payload, headers=headers, timeout=15)
        return {"status": "ok", "data": r.json()} if r.status_code == 200 \
               else {"status": "error", "code": r.status_code}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/ltp")
def get_ltp(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    cid = x_client_id    or CREDS.get("client_id", "")
    tok = x_access_token or CREDS.get("access_token", "")
    if not cid or not tok: return {"status": "no_credentials", "quotes": {}}
    if not ensure_symbols(tok): return {"status": "no_symbols", "quotes": {}}
    quotes = get_all_quotes(tok)
    return {"status": "ok", "count": len(quotes), "quotes": quotes,
            "updated_at": ist_now().strftime("%H:%M:%S IST"),
            "market_open": is_market_open()}
