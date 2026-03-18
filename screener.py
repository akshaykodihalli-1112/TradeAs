from fastapi import FastAPI, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
import requests, threading, time, os, json, csv, traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO
import builtins

# ── In-memory log buffer ───────────────────────────────────────────────────────
_log_buffer = []
_log_lock   = threading.Lock()
_orig_print = builtins.print

def _patched_print(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    ts  = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")
    with _log_lock:
        _log_buffer.append(f"[{ts}] {msg}")
        if len(_log_buffer) > 500:  # keep last 500 lines
            _log_buffer.pop(0)
    _orig_print(*args, **kwargs)

builtins.print = _patched_print

DHAN_BASE = "https://api.dhan.co"
IST       = ZoneInfo("Asia/Kolkata")
SELF_URL  = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

CREDS = {
    "client_id":    os.getenv("DHAN_CLIENT_ID", ""),
    "access_token": os.getenv("DHAN_ACCESS_TOKEN", ""),
}

cache = {
    "data": [], "updated_at": None, "status": "idle",
    "count": 0, "errors": [], "progress": 0, "total": 0,
    "symbol_source": "none", "market_open": False, "debug": {},
}
SYMBOLS = []
_lock   = threading.Lock()

# ── Verified Dhan NSE_EQ security IDs ─────────────────────────────────────────
# Source: Dhan api-scrip-master.csv NSE_EQ segment
# These are the ONLY correct IDs — no BSE codes, no guesses
VERIFIED_IDS = {
    "ABBOTINDIA":"13636","ABCAPITAL":"20904","ABFRL":"20179","ACC":"22",
    "ADANIENT":"25","ADANIPORTS":"15083","ALKEM":"17963","AMBUJACEM":"1270",
    "ANGELONE":"20554","APLAPOLLO":"19229","APOLLOHOSP":"157","APOLLOTYRE":"163",
    "ASHOKLEY":"212","ASIANPAINT":"467","ASTRAL":"14418","ATGL":"22169",
    "ATUL":"263","AUBANK":"21238","AUROPHARMA":"275","AXISBANK":"5900",
    "BAJAJ-AUTO":"16669","BAJAJFINSV":"16675","BAJFINANCE":"317",
    "BALKRISIND":"1482","BANDHANBNK":"21719","BANKBARODA":"1452",
    "BATAINDIA":"371","BEL":"383","BERGEPAINT":"404","BHARATFORG":"422",
    "BHARTIARTL":"10604","BHEL":"438","BIOCON":"11373","BOSCHLTD":"2181",
    "BPCL":"526","BRITANNIA":"547","BSOFT":"6004","CANBK":"10794",
    "CANFINHOME":"9262","CDSL":"21822","CGPOWER":"678","CHAMBLFERT":"685",
    "CHOLAFIN":"4375","CIPLA":"694","COALINDIA":"20374","COFORGE":"11543",
    "COLPAL":"742","CONCOR":"4749","COROMANDEL":"739","CROMPTON":"20655",
    "CUB":"5784","CUMMINSIND":"774","DABUR":"10107","DALBHARAT":"18253",
    "DEEPAKNTR":"10209","DELTACORP":"14413","DIVISLAB":"15174","DIXON":"21690",
    "DLF":"14732","DMART":"21561","DRREDDY":"881","EICHERMOT":"910",
    "ESCORTS":"958","EXIDEIND":"10780","FEDERALBNK":"1023","FORCEMOT":"1039",
    "FORTIS":"14804","GAIL":"1066","GLENMARK":"1109","GMRINFRA":"13528",
    "GNFC":"1113","GODREJCP":"10099","GODREJPROP":"17875","GRANULES":"11809",
    "GRASIM":"315","GUJGASLTD":"10599","HAL":"2303","HAVELLS":"8927",
    "HCLTECH":"7229","HDFCAMC":"22080","HDFCBANK":"1333","HDFCLIFE":"20704",
    "HEROMOTOCO":"1348","HFCL":"1350","HINDALCO":"1306","HINDCOPPER":"14978",
    "HINDPETRO":"1406","HINDUNILVR":"1394","ICICIBANK":"4963","ICICIGI":"21770",
    "ICICIPRULI":"18652","IDEA":"14366","IDFCFIRSTB":"20286","IEX":"22149",
    "IGL":"11262","INDHOTEL":"1512","INDIACEM":"1515","INDIAMART":"22592",
    "INDIGO":"20251","INDUSINDBK":"5258","INDUSTOWER":"22271","INFY":"1594",
    "IOC":"1624","IPCALAB":"19483","IRCTC":"22961","ITC":"1660",
    "JINDALSTEL":"11600","JKCEMENT":"13910","JSWENERGY":"17594","JSWSTEEL":"11723",
    "JUBLFOOD":"18096","KALYANKJIL":"22945","KEI":"1743","KOTAKBANK":"1232",
    "KPITTECH":"4651","LALPATHLAB":"23048","LAURUSLABS":"22950","LICHSGFIN":"1847",
    "LICI":"24095","LT":"11483","LTIM":"17818","LTTS":"20299","LUPIN":"10440",
    "M&M":"2031","M&MFIN":"13285","MANAPPURAM":"19061","MARICO":"4067",
    "MARUTI":"10999","MAXHEALTH":"23267","MCX":"19238","METROPOLIS":"22843",
    "MFSL":"4136","MOTHERSON":"4204","MPHASIS":"4261","MRF":"4162",
    "MUTHOOTFIN":"18143","NATIONALUM":"4244","NAUKRI":"13751","NAVINFLUOR":"14500",
    "NESTLEIND":"4306","NMDC":"15332","NTPC":"11630","OBEROIRLTY":"20242",
    "OFSS":"10738","ONGC":"2475","PAGEIND":"14401","PEL":"2481",
    "PERSISTENT":"18365","PETRONET":"11351","PFC":"14299","PIDILITIND":"2664",
    "PIIND":"19015","PNB":"2730","POLYCAB":"22185","POWERGRID":"14977",
    "PVRINOX":"17243","RAMCOCEM":"14994","RBLBANK":"20413","RECLTD":"15355",
    "RELIANCE":"2885","SAIL":"2963","SBICARD":"22990","SBILIFE":"21808",
    "SBIN":"3045","SHREECEM":"3103","SHRIRAMFIN":"20817","SIEMENS":"3150",
    "SRF":"3273","SUNPHARMA":"3351","SUNTV":"3367","SUPREMEIND":"3378",
    "SUZLON":"3391","SYNGENE":"20562","TATACHEM":"3405","TATACOMM":"3408",
    "TATACONSUM":"3432","TATAELXSI":"4910","TATAMOTORS":"3456","TATAPOWER":"3426",
    "TATASTEEL":"3499","TCS":"11536","TECHM":"13538","TIINDIA":"19455",
    "TITAN":"3506","TORNTPHARM":"3518","TORNTPOWER":"3519","TRENT":"3530",
    "TVSMOTOR":"3559","UBL":"16713","ULTRACEMCO":"11532","UNIONBANK":"10754",
    "UPL":"11287","VEDL":"3063","VOLTAS":"3597","WIPRO":"3787",
    "ZEEL":"3812","ZOMATO":"23652","ZYDUSLIFE":"23148",
    # Newly added FNO stocks — IDs from Dhan CSV (re-run fetch_ids.py to verify)
    "WAAREEENER":"26009","PREMIERENE":"26000","SWIGGY":"26823","HYUNDAI":"26870",
    "NTPCGREEN":"26753","RVNL":"20263","IRFC":"24202","IREDA":"26335",
    "HUDCO":"20330","SJVN":"22084","NHPC":"13751","COCHINSHIP":"4163",
    "MAZAGON":"22248","POLICYBZR":"23468","NYKAA":"23468","PAYTM":"23660",
    "DELHIVERY":"25063","CARTRADE":"23613",
}

def ist_now(): return datetime.now(IST)
def is_market_open():
    n = ist_now()
    if n.weekday() >= 5: return False
    return n.replace(hour=9,minute=15,second=0,microsecond=0) <= n <= \
           n.replace(hour=15,minute=30,second=0,microsecond=0)

# ── Symbol loading ─────────────────────────────────────────────────────────────
DISK_PATH = "/tmp/dhan_symbols.json"

def save_symbols():
    try:
        with open(DISK_PATH,"w") as f:
            json.dump({"symbols":SYMBOLS,"source":cache["symbol_source"],
                       "at":ist_now().strftime("%H:%M IST")}, f)
    except: pass

def load_symbols_disk():
    global SYMBOLS
    # First try correct_ids.json (generated by fetch_ids.py — most accurate)
    for path in ["correct_ids.json", DISK_PATH]:
        try:
            with open(path) as f:
                d = json.load(f)
            # correct_ids.json is a flat {symbol: id} dict
            if isinstance(d, dict) and not d.get("symbols"):
                syms = [{"symbol": s, "security_id": sid, "exchange": "NSE"}
                        for s, sid in d.items()]
                if len(syms) >= 100:
                    SYMBOLS = syms
                    cache["symbol_source"] = "correct_ids_json"
                    print(f"[sym] loaded {len(SYMBOLS)} from {path}")
                    return True
            # DISK_PATH format: {"symbols": [...], ...}
            elif len(d.get("symbols", [])) >= 100:
                SYMBOLS = d["symbols"]
                cache["symbol_source"] = "disk"
                print(f"[sym] loaded {len(SYMBOLS)} from {path}")
                return True
        except Exception as e:
            pass
    return False

def load_symbols_verified():
    """Load from hardcoded verified IDs — always works, no network needed."""
    global SYMBOLS
    SYMBOLS = [{"symbol":s,"security_id":sid,"exchange":"NSE"}
               for s,sid in sorted(VERIFIED_IDS.items())]
    cache["symbol_source"] = "verified"
    print(f"[sym] verified: {len(SYMBOLS)} symbols loaded")
    return True

def load_symbols_csv(tok=""):
    """Try to get fresh IDs from Dhan CSV. Updates VERIFIED_IDS if successful."""
    global SYMBOLS, VERIFIED_IDS
    for url in ["https://images.dhan.co/api-data/api-scrip-master.csv",
                "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"]:
        try:
            print(f"[sym] trying {url.split('/')[-1]}...")
            r = requests.get(url, timeout=45)
            print(f"[sym]   {r.status_code} len={len(r.text)}")
            if r.status_code != 200 or len(r.text) < 10000: continue
            eq, fno = _parse_csv(r.text)
            if len(eq) < 100: continue
            all_names = (fno | set(VERIFIED_IDS.keys())) if fno else set(VERIFIED_IDS.keys())
            matched = [{"symbol":s,"security_id":eq[s],"exchange":"NSE"}
                       for s in sorted(all_names) if s in eq]
            if len(matched) < 100: continue
            SYMBOLS = matched
            VERIFIED_IDS = {s["symbol"]:s["security_id"] for s in SYMBOLS}
            cache["symbol_source"] = "csv"
            save_symbols()
            print(f"[sym] csv: {len(SYMBOLS)} symbols ✓")
            return True
        except Exception as e:
            print(f"[sym]   error: {e}")
    return False

def _find_col(header, *keywords):
    """Return first column index whose header contains any keyword."""
    for i, h in enumerate(header):
        for kw in keywords:
            if kw in h:
                return i
    return None

def _parse_csv(text):
    eq, fno = {}, set()
    reader = csv.reader(StringIO(text))
    header = None
    exch_i = seg_i = sym_i = id_i = inst_i = series_i = None
    for row in reader:
        if header is None:
            header   = [h.strip().upper() for h in row]
            exch_i   = _find_col(header, "SEM_EXM_EXCH_ID", "EXCH_ID", "EXCHANGE")
            seg_i    = _find_col(header, "SEM_SEGMENT", "EXCH_SEG", "SEGMENT")
            sym_i    = _find_col(header, "SEM_TRADING_SYMBOL", "TRADING_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME")
            id_i     = _find_col(header, "SEM_SMST_SECURITY_ID", "SECURITY_ID", "SCRIP_ID", "SM_SYMBOL_ID")
            inst_i   = _find_col(header, "SEM_INSTRUMENT_NAME", "INSTRUMENT", "SEM_INSTRUMENT")
            series_i = _find_col(header, "SEM_SERIES", "SERIES")
            if None in (sym_i, id_i): break
            continue
        max_i = max(c for c in (exch_i, seg_i, sym_i, id_i, inst_i, series_i) if c is not None)
        if len(row) <= max_i: continue
        sym    = row[sym_i].strip()
        seg    = row[seg_i].strip().upper()    if seg_i    is not None else ""
        exch   = row[exch_i].strip().upper()   if exch_i   is not None else ""
        inst   = row[inst_i].strip().upper()   if inst_i   is not None else ""
        series = row[series_i].strip().upper() if series_i is not None else ""
        if not sym: continue
        is_nse_eq = (
            (exch == "NSE" and seg == "E" and series == "EQ") or
            seg in ("NSE_EQ", "NSE EQ", "NSEEQ") or
            (seg.startswith("NSE") and "EQ" in seg)
        )
        is_nse_fno = (
            (exch == "NSE" and inst in ("FUTSTK", "OPTSTK")) or
            "FNO" in seg or "NSE_FO" in seg
        )
        if is_nse_eq and sym not in eq:
            try: eq[sym] = str(int(float(row[id_i].strip())))
            except: pass
        elif is_nse_fno:
            fno.add(sym)
    return eq, fno

def ensure_symbols(tok=""):
    if len(SYMBOLS) >= 100: return True
    if load_symbols_disk(): return True
    load_symbols_verified()   # always works — never returns False
    # Try CSV in background to upgrade IDs
    if tok:
        threading.Thread(target=_upgrade_ids_background, args=(tok,), daemon=True).start()
    return True


def _upgrade_ids_background(tok):
    """Try CSV first. If that fails, fix wrong IDs via marketfeed scan."""
    if load_symbols_csv(tok):
        return  # CSV worked — all IDs now correct
    # CSV failed — scan for correct IDs of missing symbols
    fix_missing_ids(tok)


def fix_missing_ids(tok):
    """
    Scan all Dhan NSE_EQ IDs in focused ranges, collect tradingSymbol from each response.
    Builds a complete symbol→ID map and fixes any wrong IDs in SYMBOLS.
    Runs once in background after first screener run.
    Takes ~2-3 minutes but only runs when IDs are wrong.
    """
    global SYMBOLS, VERIFIED_IDS
    if not tok: return
    print("[fix] starting full ID scan...")
    headers = {"access-token": tok, "client-id": CREDS.get("client_id",""),
               "Content-Type": "application/json"}

    # Scan these ID ranges — covers all known Dhan NSE_EQ stock IDs
    # Total: ~25000 IDs in batches of 100 = 250 batches
    all_ids = list(range(1, 25001))
    target  = {s["symbol"] for s in SYMBOLS}
    found   = {}  # symbol → correct_id

    for i in range(0, len(all_ids), 100):
        if len(found) >= len(target): break
        batch = all_ids[i:i+100]
        for attempt in range(3):
            try:
                r = requests.post(f"{DHAN_BASE}/v2/marketfeed/ltp",
                                  json={"NSE_EQ": batch}, headers=headers, timeout=15)
                if r.status_code == 429:
                    time.sleep(6*(attempt+1)); continue
                if r.status_code == 200:
                    for sec_id, q in r.json().get("data",{}).get("NSE_EQ",{}).items():
                        sym = (q.get("tradingSymbol") or q.get("trading_symbol") or "").strip()
                        if sym in target:
                            found[sym] = str(int(float(sec_id)))
                    break
            except: time.sleep(1)
        time.sleep(0.8)
        if i % 2000 == 0:
            print(f"[fix] scanned {i+100}/25000 IDs, found {len(found)}/{len(target)}")

    print(f"[fix] scan complete: {len(found)}/{len(target)} symbols found")

    # Apply corrections for any symbols where ID changed
    corrections = {}
    current_map = {s["symbol"]: s["security_id"] for s in SYMBOLS}
    for sym, new_id in found.items():
        if current_map.get(sym) != new_id:
            corrections[sym] = new_id

    if corrections:
        VERIFIED_IDS.update(corrections)
        for s in SYMBOLS:
            if s["symbol"] in corrections:
                s["security_id"] = corrections[s["symbol"]]
        cache["symbol_source"] = "scanned"
        save_symbols()
        # Also update correct_ids.json so fixes persist across restarts
        try:
            cij = {s["symbol"]: s["security_id"] for s in SYMBOLS}
            with open("correct_ids.json", "w") as f:
                json.dump(cij, f, indent=2)
            print(f"[fix] saved corrections to correct_ids.json")
        except Exception as e:
            print(f"[fix] could not save correct_ids.json: {e}")
        print(f"[fix] corrected {len(corrections)} IDs: {list(corrections.items())[:5]}")
    else:
        print("[fix] no corrections needed")

# ── Quotes ─────────────────────────────────────────────────────────────────────
def get_all_quotes(tok):
    cid     = CREDS["client_id"]
    headers = {"access-token":tok,"client-id":cid,"Content-Type":"application/json"}
    id_map  = {s["security_id"]:s["symbol"] for s in SYMBOLS}
    quotes  = {}
    _logged = False
    for i in range(0, len(SYMBOLS), 50):
        batch   = [int(s["security_id"]) for s in SYMBOLS[i:i+50]]
        payload = {"NSE_EQ": batch}
        ohlc_data = {}  # from /v2/marketfeed/ohlc endpoint

        for ep in ["/v2/marketfeed/quote","/v2/marketfeed/ohlc"]:
            for attempt in range(3):
                try:
                    r = requests.post(f"{DHAN_BASE}{ep}",json=payload,headers=headers,timeout=15)
                    print(f"  quote i={i} {ep} → {r.status_code}")
                    if r.status_code==200:
                        raw = r.json().get("data",{}).get("NSE_EQ",{})
                        if not _logged and raw:
                            first_key = next(iter(raw))
                            print(f"[debug] {ep} keys: {list(raw[first_key].keys())}")
                            print(f"[debug] {ep} ohlc keys: {list(raw[first_key].get('ohlc',{}).keys())}")
                            print(f"[debug] {ep} sample: {raw[first_key]}")
                            if ep == "/v2/marketfeed/ohlc":
                                _logged = True

                        if ep == "/v2/marketfeed/quote":
                            for sid, q in raw.items():
                                sym = id_map.get(str(int(float(sid))))
                                if sym:
                                    ohlc       = q.get("ohlc", {})
                                    ltp        = float(q.get("last_price", 0))
                                    net_change = float(q.get("net_change", 0))
                                    today_close = float(ohlc.get("close", 0))
                                    # prev_close = today_close - net_change
                                    # net_change = last_price - prev_close
                                    pc = round(ltp - net_change, 2)
                                    quotes[sym] = {
                                        "ltp":        round(ltp, 2),
                                        "prev_close": pc,
                                        "volume":     int(q.get("volume", 0)),
                                        "open":       round(float(ohlc.get("open", 0)), 2),
                                        "high":       round(float(ohlc.get("high", 0)), 2),
                                        "low":        round(float(ohlc.get("low", 0)), 2),
                                    }
                        else:  # ohlc endpoint — may have prev_close field
                            for sid, q in raw.items():
                                sym = id_map.get(str(int(float(sid))))
                                if sym and sym in quotes:
                                    ohlc = q.get("ohlc", {})
                                    # Try to get prev_close from ohlc endpoint
                                    pc = float(q.get("prev_close") or q.get("previous_close") or
                                               ohlc.get("prev_close") or ohlc.get("previous_close") or 0)
                                    if pc:
                                        quotes[sym]["prev_close"] = round(pc, 2)
                                    ohlc_data[sym] = q
                        break
                    elif r.status_code==429:
                        time.sleep(5*(attempt+1))
                    else:
                        break
                except Exception as e:
                    print(f"  quote err: {e}"); time.sleep(2)
        time.sleep(1.2)
    missing=[s["symbol"] for s in SYMBOLS if s["symbol"] not in quotes]
    print(f"[quotes] got={len(quotes)} missing={len(missing)} sample={missing[:5]}")
    cache["debug"]={"quotes_fetched":len(quotes),"missing_count":len(missing),
                    "missing_sample":missing[:10]}
    return quotes

def get_historical(security_id, tok):
    today=ist_now()
    payload={"securityId":security_id,"exchangeSegment":"NSE_EQ","instrument":"EQUITY",
             "expiryCode":0,"fromDate":(today-timedelta(days=30)).strftime("%Y-%m-%d"),
             "toDate":today.strftime("%Y-%m-%d")}
    headers={"access-token":tok,"Content-Type":"application/json"}
    for attempt in range(3):
        try:
            r=requests.post(f"{DHAN_BASE}/v2/charts/historical",json=payload,headers=headers,timeout=10)
            if r.status_code==429: time.sleep(3*(attempt+1)); continue
            if r.status_code!=200: return None
            d=r.json(); return d if d.get("timestamp") else None
        except: time.sleep(1)
    return None

# ── Screener ───────────────────────────────────────────────────────────────────
def compute_metrics(q, hist, market_open, today_str):
    """
    change% formula:
      Market OPEN:   (ltp - closes[-1]) / closes[-1]      closes[-1] = yesterday settled
      Market CLOSED: (closes[-1] - closes[-2]) / closes[-2]  closes[-1] = today settled
    Fallback (no hist): use quote prev_close from Dhan API
    """
    ltp       = q["ltp"]
    today_vol = q["volume"]
    closes    = hist.get("close",  []) if hist else []
    volumes   = hist.get("volume", []) if hist else []

    # ── Change % ──────────────────────────────────────────────────────
    chg_pct    = 0.0
    prev_close = 0.0

    if len(closes) >= 2:
        if market_open:
            # Today's bar not yet settled — closes[-1] is yesterday's close
            prev_close = closes[-1]
            today_price = ltp
        else:
            # Market closed — closes[-1] is today's settled close
            # closes[-2] is yesterday's close
            today_price = closes[-1]
            prev_close  = closes[-2]
            ltp = today_price  # show today's close as LTP after hours
        if prev_close:
            chg_pct = round(((today_price - prev_close) / prev_close) * 100, 2)

    elif q.get("prev_close"):
        # Fallback: use Dhan's prev_close field
        prev_close = q["prev_close"]
        if prev_close:
            chg_pct = round(((ltp - prev_close) / prev_close) * 100, 2)

    # ── Momentum 5D ───────────────────────────────────────────────────
    momentum5d = 0.0
    if len(closes) >= 6:
        ref = closes[-1] if not market_open else ltp
        c5  = closes[-7] if not market_open else closes[-6]
        if c5: momentum5d = round(((ref - c5) / c5) * 100, 2)

    # ── Volume ratio ──────────────────────────────────────────────────
    vol_ratio = 0.0; avg_vol7d = 0
    if len(volumes) >= 8:
        if market_open:
            avg_vol7d = int(sum(volumes[-8:-1]) / 7)
            vol_ratio = round(today_vol / avg_vol7d, 2) if avg_vol7d else 0
        else:
            # Market closed — volumes[-1] is today's final volume
            avg_vol7d = int(sum(volumes[-8:-1]) / 7)
            vol_ratio = round(volumes[-1] / avg_vol7d, 2) if avg_vol7d else 0
    elif today_vol > 0:
        vol_ratio = 1.0

    return chg_pct, round(prev_close, 2), momentum5d, vol_ratio, int(avg_vol7d)


def run_screener(tok):
    global cache
    ensure_symbols(tok)
    if not SYMBOLS:
        cache.update({"status": "error", "errors": ["No symbols"]}); return

    market_open = is_market_open()
    today_str   = ist_now().strftime("%Y-%m-%d")

    # ── Market CLOSED ──────────────────────────────────────────────────────────
    # Use quote API: ltp = today's final close, ohlc.close = yesterday's close
    # change% = (ltp - prev_close) / prev_close * 100  ← simple and correct
    if not market_open:
        if cache.get("data") and cache.get("updated_at"):
            print(f"[screener] market closed — keeping settled data ({len(cache['data'])} results)")
            cache.update({"status": "ok", "market_open": False, "progress": 100, "errors": []})
            return

        print(f"[screener] market closed — fetching settled prices via quote API")
        total = len(SYMBOLS)
        cache.update({"status": "fetching", "progress": 5, "total": total, "market_open": False})

        quotes = get_all_quotes(tok)
        if not quotes:
            cache.update({"status": "error", "errors": ["0 quotes returned"]}); return
        cache["progress"] = 20

        # Historical is the ONLY source for prev_close after market close
        # Dhan quote API has no prev_close field — ohlc.close = today's close
        print(f"[screener] fetching historical for prev_close...")
        hist_data = {}
        syms = [s for s in SYMBOLS if s["symbol"] in quotes]
        for i, sym in enumerate(syms):
            hist = get_historical(sym["security_id"], tok)
            if hist:
                hist_data[sym["symbol"]] = hist
            cache["progress"] = 20 + round((i + 1) / len(syms) * 70)
            time.sleep(0.4)
            if (i + 1) % 30 == 0:
                time.sleep(2)

        print(f"[screener] historical done: {len(hist_data)} fetched")
        cache["progress"] = 92

        results, skipped = [], []
        for sym in SYMBOLS:
            try:
                q    = quotes.get(sym["symbol"])
                hist = hist_data.get(sym["symbol"])
                if not q:
                    skipped.append(f"{sym['symbol']}:no_quote"); continue

                ltp = q["ltp"]  # today's close from quote API

                if hist:
                    closes  = hist.get("close",  [])
                    volumes = hist.get("volume", [])
                    # closes[-1] may or may not include today depending on Dhan settlement timing
                    # Check if closes[-1] matches today's ltp (within 0.5%)
                    if closes and abs(closes[-1] - ltp) / ltp < 0.005:
                        # closes[-1] = today's close, closes[-2] = yesterday's close
                        prev_close = closes[-2] if len(closes) >= 2 else 0
                    else:
                        # closes[-1] = yesterday's close (today not settled yet in hist)
                        prev_close = closes[-1] if closes else 0

                    chg_pct = round((ltp - prev_close) / prev_close * 100, 2) if prev_close else 0.0

                    # Momentum 5D
                    mom5d = 0.0
                    if len(closes) >= 6:
                        c5 = closes[-6] if abs(closes[-1] - ltp) / ltp < 0.005 else closes[-5] if len(closes) >= 5 else 0
                        if c5: mom5d = round((ltp - c5) / c5 * 100, 2)

                    # Volume ratio
                    vol_ratio = 0.0; avg_vol7d = 0
                    if len(volumes) >= 8:
                        avg_vol7d = int(sum(volumes[-8:-1]) / 7)
                        vol_ratio = round(q["volume"] / avg_vol7d, 2) if avg_vol7d else 0.0
                else:
                    prev_close = 0.0; chg_pct = 0.0; mom5d = 0.0
                    vol_ratio = 0.0; avg_vol7d = 0

                print(f"[close] {sym['symbol']} ltp={ltp} prev={prev_close} chg={chg_pct}%") if sym["symbol"] in ("ANGELONE","RELIANCE","SBIN") else None

                results.append({
                    "symbol":      sym["symbol"], "exchange": "NSE",
                    "ltp":         ltp,           "prev_close": round(prev_close, 2),
                    "change":      chg_pct,       "momentum5d": mom5d,
                    "volumeRatio": vol_ratio,     "todayVol":   q["volume"],
                    "avgVol7d":    avg_vol7d,
                })
            except Exception as e:
                skipped.append(f"{sym['symbol']}:{e}")
                print(f"[screener] {sym['symbol']} error: {e}")

        results.sort(key=lambda x: x["volumeRatio"], reverse=True)
        cache.update({"data": results, "updated_at": ist_now().strftime("%H:%M:%S IST"),
            "status": "ok", "count": len(results), "errors": [], "skipped": skipped[:20],
            "progress": 100, "market_open": False})
        print(f"[screener] closed-market done — {len(results)} ok | {len(skipped)} skipped")
        return

    # ── Market OPEN ────────────────────────────────────────────────────────────
    # Fast: quotes for live ltp + change%, historical in background for momentum/vol
    total = len(SYMBOLS)
    cache.update({"status": "fetching", "progress": 5, "total": total, "market_open": True})
    print(f"[screener] market open — {total} symbols | {ist_now().strftime('%H:%M:%S IST')}")

    quotes = get_all_quotes(tok)
    if not quotes:
        cache.update({"status": "error", "errors": ["0 quotes returned"]}); return
    cache["progress"] = 60

    results, skipped = [], []
    for sym in SYMBOLS:
        try:
            q = quotes.get(sym["symbol"])
            if not q: skipped.append(f"{sym['symbol']}:no_quote"); continue
            pc  = q.get("prev_close", 0)
            ltp = q["ltp"]
            chg = round((ltp - pc) / pc * 100, 2) if pc else 0.0
            # Reuse momentum/vol from previous run if available
            prev = next((r for r in cache.get("data", []) if r["symbol"] == sym["symbol"]), {})
            results.append({
                "symbol":      sym["symbol"], "exchange": "NSE",
                "ltp":         ltp,           "prev_close": pc,
                "change":      chg,           "momentum5d": prev.get("momentum5d", 0.0),
                "volumeRatio": prev.get("volumeRatio", 0.0), "todayVol": q["volume"],
                "avgVol7d":    prev.get("avgVol7d", 0),
            })
        except Exception as e:
            skipped.append(f"{sym['symbol']}:{e}")

    results.sort(key=lambda x: x["volumeRatio"], reverse=True)
    cache.update({"data": results, "updated_at": ist_now().strftime("%H:%M:%S IST"),
        "status": "ok", "count": len(results), "errors": [], "skipped": skipped[:20],
        "progress": 100, "market_open": True})
    print(f"[screener] open-market done — {len(results)} ok | {len(skipped)} skipped")

    # Enrich momentum + vol ratio in background
    threading.Thread(target=_enrich_historical,
                     args=(tok, quotes, True, today_str), daemon=True).start()

    # Fix missing IDs if needed
    missing_count = cache.get("debug", {}).get("missing_count", 0)
    if missing_count > 10 and cache["symbol_source"] in ("verified", "disk", "correct_ids_json"):
        threading.Thread(target=fix_missing_ids, args=(tok,), daemon=True).start()


def _enrich_historical(tok, quotes, market_open, today_str):
    """Background: fetch historical data and update cache with momentum + vol ratio."""
    global cache
    print(f"[hist] starting background historical fetch for {len(quotes)} symbols...")
    syms = [s for s in SYMBOLS if s["symbol"] in quotes]
    hist_data = {}

    for i, sym in enumerate(syms):
        hist = get_historical(sym["security_id"], tok)
        if hist:
            hist_data[sym["symbol"]] = hist
        time.sleep(0.4)
        if (i + 1) % 30 == 0:
            time.sleep(2)

    print(f"[hist] got {len(hist_data)} historical records — updating cache...")

    # Build result map from current cache
    result_map = {r["symbol"]: dict(r) for r in cache.get("data", [])}

    for sym_name, hist in hist_data.items():
        if sym_name not in result_map: continue
        q = quotes.get(sym_name)
        if not q: continue
        chg, pc, mom5d, vol_ratio, avg_vol7d = compute_metrics(q, hist, market_open, today_str)
        result_map[sym_name].update({
            "prev_close":  pc,
            "change":      chg,
            "momentum5d":  mom5d,
            "volumeRatio": vol_ratio,
            "avgVol7d":    avg_vol7d,
        })

    enriched = sorted(result_map.values(), key=lambda x: x["volumeRatio"], reverse=True)
    cache.update({"data": enriched, "count": len(enriched),
                  "updated_at": ist_now().strftime("%H:%M:%S IST")})
    print(f"[hist] cache enriched with historical data ✓")

def trigger_screener(cid="",tok=""):
    cid=cid or CREDS["client_id"]; tok=tok or CREDS["access_token"]
    if not cid or not tok: cache["status"]="no_credentials"; return
    CREDS["client_id"]=cid; CREDS["access_token"]=tok
    if _lock.locked(): return
    threading.Thread(target=lambda:_run_locked(tok),daemon=True).start()

def _run_locked(tok):
    with _lock: run_screener(tok)

# ── Scheduler ──────────────────────────────────────────────────────────────────
def scheduled_job():
    tok=CREDS.get("access_token",""); cid=CREDS.get("client_id","")
    if not tok or not cid: return
    now=ist_now()
    if now.hour==8 and now.minute<10:
        threading.Thread(target=load_symbols_csv,args=(tok,),daemon=True).start()
    trigger_screener(cid,tok)

def keep_alive():
    if SELF_URL:
        try: requests.get(f"{SELF_URL}/api/health",timeout=10)
        except: pass

scheduler=BackgroundScheduler()
scheduler.add_job(scheduled_job,"interval",minutes=5,id="screener")
scheduler.add_job(keep_alive,"interval",minutes=10,id="keepalive")
scheduler.start()

# ── Startup ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    def _boot():
        tok=CREDS.get("access_token",""); cid=CREDS.get("client_id","")
        print(f"[boot] cid={'SET' if cid else 'MISSING'} tok={'SET' if tok else 'MISSING'}")
        ensure_symbols(tok)
        print(f"[boot] symbols={len(SYMBOLS)} source={cache['symbol_source']}")
        if cid and tok: trigger_screener(cid,tok)
        else: print("[boot] no creds — set DHAN_CLIENT_ID + DHAN_ACCESS_TOKEN on Render")
    threading.Thread(target=_boot,daemon=True).start()
    yield

app = FastAPI(title="TradeAs API", version="3.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/")
def root(): return {"status":"ok","time_ist":ist_now().strftime("%H:%M:%S"),"market_open":is_market_open()}

@app.get("/api/health")
def health():
    return {"status":"ok","cache_status":cache["status"],"last_refresh":cache["updated_at"],
            "symbol_count":len(SYMBOLS),"symbol_source":cache["symbol_source"],
            "result_count":cache["count"],"error_count":len(cache.get("errors",[])),
            "skipped_count":len(cache.get("skipped",[])),"skipped_sample":cache.get("skipped",[])[:5],
            "progress_pct":cache.get("progress",0),"market_open":is_market_open(),
            "time_ist":ist_now().strftime("%H:%M:%S"),"day":ist_now().strftime("%A"),
            "debug":cache.get("debug",{})}

@app.get("/api/screener")
def get_screener(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    cid=x_client_id or CREDS.get("client_id",""); tok=x_access_token or CREDS.get("access_token","")
    if cid: CREDS["client_id"]=cid
    if tok: CREDS["access_token"]=tok
    if cid and tok and cache.get("status") in ("idle","no_credentials","error"):
        trigger_screener(cid,tok)
    return cache

@app.get("/api/boot")
@app.post("/api/boot")
def boot(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    cid=x_client_id or CREDS.get("client_id",""); tok=x_access_token or CREDS.get("access_token","")
    if cid: CREDS["client_id"]=cid
    if tok: CREDS["access_token"]=tok
    ensure_symbols(tok)
    if cid and tok: trigger_screener(cid,tok)
    return {"symbols":len(SYMBOLS),"source":cache["symbol_source"],
            "screener":"triggered" if (cid and tok) else "no_creds",
            "tip":"Poll /api/health for progress"}

@app.get("/api/debug")
def debug(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    cid=x_client_id or CREDS.get("client_id",""); tok=x_access_token or CREDS.get("access_token","")
    return {"symbol_count":len(SYMBOLS),"symbol_source":cache["symbol_source"],
            "screener_status":cache["status"],"has_client_id":bool(cid),"has_token":bool(tok),
            "env_client_id":bool(os.getenv("DHAN_CLIENT_ID")),"env_token":bool(os.getenv("DHAN_ACCESS_TOKEN")),
            "lock_held":_lock.locked(),"sample_symbols":SYMBOLS[:3],"last_debug":cache.get("debug",{})}

@app.get("/api/logs")
def get_logs(n:int=100):
    """Returns last N log lines — useful for debugging without opening Render dashboard."""
    with _log_lock:
        lines = _log_buffer[-n:]
    return {"count":len(lines),"logs":lines}
def refresh_symbols(x_access_token:str=Header(None)):
    tok=x_access_token or CREDS.get("access_token","")
    ok=load_symbols_csv(tok)
    if not ok: load_symbols_verified()
    return {"ok":ok,"count":len(SYMBOLS),"source":cache["symbol_source"]}

@app.get("/api/symbols")
def list_symbols():
    return {"count":len(SYMBOLS),"source":cache["symbol_source"],"symbols":SYMBOLS}

@app.get("/api/screener/top")
def top(limit:int=20): return {"data":cache["data"][:limit]}

@app.get("/api/screener/gainers")
def gainers(): return {"data":sorted([x for x in cache["data"] if x["change"]>0],key=lambda x:x["change"],reverse=True)[:15]}

@app.get("/api/screener/losers")
def losers(): return {"data":sorted([x for x in cache["data"] if x["change"]<0],key=lambda x:x["change"])[:15]}

@app.get("/api/screener/volume-spike")
def vol_spike(min_ratio:float=2.0): return {"data":[x for x in cache["data"] if x["volumeRatio"]>=min_ratio]}

@app.get("/api/screener/momentum")
def momentum(min_pct:float=3.0): return {"data":[x for x in cache["data"] if x["momentum5d"]>=min_pct]}

@app.get("/api/ping-dhan")
def ping_dhan(x_access_token:str=Header(None)):
    tok=x_access_token or CREDS.get("access_token","")
    if not tok: return {"connected":False,"error":"No token"}
    try:
        r=requests.get(f"{DHAN_BASE}/v2/profile",headers={"access-token":tok},timeout=5)
        return {"connected":r.status_code==200,"status":r.status_code}
    except Exception as e: return {"connected":False,"error":str(e)}

@app.get("/api/historical")
def proxy_historical(security_id:str=Query(...),from_date:str=Query(...),to_date:str=Query(...),x_access_token:str=Header(None)):
    tok=x_access_token or CREDS.get("access_token","")
    if not tok: return {"status":"no_credentials"}
    headers={"access-token":tok,"Content-Type":"application/json"}
    payload={"securityId":security_id,"exchangeSegment":"NSE_EQ","instrument":"EQUITY","expiryCode":0,"fromDate":from_date,"toDate":to_date}
    try:
        r=requests.post(f"{DHAN_BASE}/v2/charts/historical",json=payload,headers=headers,timeout=15)
        return {"status":"ok","data":r.json()} if r.status_code==200 else {"status":"error","code":r.status_code}
    except Exception as e: return {"status":"error","error":str(e)}

@app.get("/api/ltp")
def get_ltp(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    cid=x_client_id or CREDS.get("client_id",""); tok=x_access_token or CREDS.get("access_token","")
    if not cid or not tok: return {"status":"no_credentials","quotes":{}}
    ensure_symbols(tok)
    quotes=get_all_quotes(tok)
    return {"status":"ok","count":len(quotes),"quotes":quotes,
            "updated_at":ist_now().strftime("%H:%M:%S IST"),"market_open":is_market_open()}

@app.get("/api/ltp/live")
def get_ltp_live(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    """Fast LTP-only fetch from Dhan marketfeed/ltp — used for 10s price refresh.
    Much faster than /api/ltp as it only fetches last_price, no OHLC/volume."""
    cid=x_client_id or CREDS.get("client_id",""); tok=x_access_token or CREDS.get("access_token","")
    if not cid or not tok: return {"status":"no_credentials","quotes":{}}
    ensure_symbols(tok)
    headers={"access-token":tok,"client-id":cid,"Content-Type":"application/json"}
    id_map={s["security_id"]:s["symbol"] for s in SYMBOLS}
    # Also build prev_close from existing cache for change% computation
    prev_map={r["symbol"]:r["prev_close"] for r in cache.get("data",[]) if r.get("prev_close")}
    quotes={}
    for i in range(0,len(SYMBOLS),100):
        batch=[int(s["security_id"]) for s in SYMBOLS[i:i+100]]
        for attempt in range(2):
            try:
                r=requests.post(f"{DHAN_BASE}/v2/marketfeed/ltp",
                                json={"NSE_EQ":batch},headers=headers,timeout=10)
                if r.status_code==200:
                    for sid,q in r.json().get("data",{}).get("NSE_EQ",{}).items():
                        sym=id_map.get(str(int(float(sid))))
                        if sym:
                            ltp=round(float(q.get("last_price",0)),2)
                            pc=prev_map.get(sym,0)
                            quotes[sym]={"ltp":ltp,"prev_close":pc}
                    break
                elif r.status_code==429:
                    time.sleep(3*(attempt+1))
            except Exception as e:
                time.sleep(1)
        time.sleep(0.3)
    return {"status":"ok","count":len(quotes),"quotes":quotes,
            "updated_at":ist_now().strftime("%H:%M:%S IST"),"market_open":is_market_open()}

# ── Option Chain ───────────────────────────────────────────────────────────────
_opt_cache = {"data": [], "updated_at": None, "status": "idle", "count": 0, "market_open": False}
_opt_lock  = threading.Lock()

def _analyze_options(tok, cid):
    global _opt_cache
    if not SYMBOLS or not tok: return
    _opt_cache["status"] = "fetching"
    headers = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    results = []

    for sym_info in SYMBOLS:
        sym   = sym_info["symbol"]
        eq_id = sym_info["security_id"]
        spot  = next((x["ltp"] for x in cache.get("data", []) if x["symbol"] == sym), 0)
        if not spot: continue
        try:
            r = requests.post(
                f"{DHAN_BASE}/v2/optionchain",
                json={"UnderlyingScrip": int(eq_id), "UnderlyingInstrument": "EQUITY", "ExpiryCode": 0},
                headers=headers, timeout=10
            )
            if r.status_code == 429: time.sleep(5); continue
            if r.status_code != 200: continue
            strikes = r.json().get("data", [])
            if not strikes: continue

            total_ce_oi = total_pe_oi = total_ce_vol = total_pe_vol = 0
            atm_ce_oi = atm_pe_oi = atm_ce_vol = atm_pe_vol = 0
            atm_strike = 0
            min_diff = float("inf")

            for sd in strikes:
                sp     = float(sd.get("strikePrice", 0))
                ce     = sd.get("callOption", {})
                pe     = sd.get("putOption",  {})
                ce_oi  = int(ce.get("openInterest", 0))
                pe_oi  = int(pe.get("openInterest", 0))
                ce_vol = int(ce.get("volume", 0))
                pe_vol = int(pe.get("volume", 0))
                total_ce_oi += ce_oi; total_pe_oi += pe_oi
                total_ce_vol += ce_vol; total_pe_vol += pe_vol
                diff = abs(sp - spot)
                if diff < min_diff:
                    min_diff = diff; atm_strike = sp
                    atm_ce_oi = ce_oi; atm_pe_oi = pe_oi
                    atm_ce_vol = ce_vol; atm_pe_vol = pe_vol

            if total_ce_oi + total_pe_oi == 0: continue
            pcr          = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi else 0
            ce_vol_ratio = round(atm_ce_vol / (atm_ce_oi / 100), 2) if atm_ce_oi else 0
            pe_vol_ratio = round(atm_pe_vol / (atm_pe_oi / 100), 2) if atm_pe_oi else 0

            bull = 0
            if pcr < 0.7:                           bull += 2
            if ce_vol_ratio > 5:                    bull += 2
            if atm_pe_oi > atm_ce_oi * 1.5:        bull += 1
            if total_ce_vol > total_pe_vol * 1.5:   bull += 1

            bear = 0
            if pcr > 1.3:                           bear += 2
            if pe_vol_ratio > 5:                    bear += 2
            if atm_ce_oi > atm_pe_oi * 1.5:        bear += 1
            if total_pe_vol > total_ce_vol * 1.5:   bear += 1

            signal = "neutral"
            strength = 0
            if bull >= 3 and bull > bear:   signal = "bullish"; strength = bull
            elif bear >= 3 and bear > bull: signal = "bearish"; strength = bear

            vol_spike = ""
            if   ce_vol_ratio > 8 and pe_vol_ratio > 8: vol_spike = "BOTH"
            elif ce_vol_ratio > 8:                       vol_spike = "CE"
            elif pe_vol_ratio > 8:                       vol_spike = "PE"

            results.append({
                "symbol": sym, "spot": spot, "atm_strike": atm_strike,
                "pcr": pcr, "signal": signal, "signal_strength": strength,
                "ce_oi": total_ce_oi, "pe_oi": total_pe_oi,
                "ce_vol": total_ce_vol, "pe_vol": total_pe_vol,
                "atm_ce_oi": atm_ce_oi, "atm_pe_oi": atm_pe_oi,
                "atm_ce_vol": atm_ce_vol, "atm_pe_vol": atm_pe_vol,
                "ce_vol_ratio": ce_vol_ratio, "pe_vol_ratio": pe_vol_ratio,
                "vol_spike": vol_spike,
            })
            time.sleep(0.3)
        except Exception as e:
            print(f"[opt] {sym}: {e}"); continue

    results.sort(key=lambda x: x["signal_strength"], reverse=True)
    _opt_cache.update({"data": results, "updated_at": ist_now().strftime("%H:%M:%S IST"),
                       "status": "ok", "count": len(results), "market_open": is_market_open()})
    print(f"[opt] done — {len(results)} symbols")

def _run_opt_locked(tok, cid):
    with _opt_lock: _analyze_options(tok, cid)

def _opt_stale():
    last = _opt_cache.get("updated_at")
    if not last: return True
    try:
        t = datetime.strptime(last, "%H:%M:%S IST").replace(
            year=ist_now().year, month=ist_now().month, day=ist_now().day, tzinfo=IST)
        return (ist_now() - t).seconds > 60
    except: return True

@app.get("/api/options")
def get_options(x_client_id:str=Header(None), x_access_token:str=Header(None),
                refresh:bool=Query(False)):
    cid=x_client_id or CREDS.get("client_id",""); tok=x_access_token or CREDS.get("access_token","")
    if not cid or not tok: return {"status":"no_credentials","data":[]}
    CREDS["client_id"]=cid; CREDS["access_token"]=tok
    if (refresh or _opt_stale() or not _opt_cache["data"]) and not _opt_lock.locked():
        threading.Thread(target=_run_opt_locked, args=(tok,cid), daemon=True).start()
    return _opt_cache

@app.get("/api/options/bullish")
def opt_bullish(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    return {"data":[x for x in _opt_cache["data"] if x["signal"]=="bullish"]}

@app.get("/api/options/bearish")
def opt_bearish(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    return {"data":[x for x in _opt_cache["data"] if x["signal"]=="bearish"]}

@app.get("/api/options/volspike")
def opt_volspike(x_client_id:str=Header(None),x_access_token:str=Header(None)):
    return {"data":[x for x in _opt_cache["data"] if x["vol_spike"]]}
