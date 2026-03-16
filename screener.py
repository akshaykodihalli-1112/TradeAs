from fastapi import FastAPI, Header
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DHAN_BASE = "https://api.dhan.co"
IST       = ZoneInfo("Asia/Kolkata")
SELF_URL  = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

cache = {
    "data": [], "updated_at": None, "status": "idle",
    "count": 0, "errors": [], "progress": 0, "total": 0,
    "symbol_source": "none", "market_open": False, "debug": {},
}

CREDS = {
    "client_id":    os.getenv("DHAN_CLIENT_ID", ""),
    "access_token": os.getenv("DHAN_ACCESS_TOKEN", ""),
}

SYMBOLS = []
_screener_lock = threading.Lock()


# ── market hours ──────────────────────────────────────────────────
def is_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return now.replace(hour=9, minute=15, second=0, microsecond=0) <= now \
        <= now.replace(hour=15, minute=30, second=0, microsecond=0)


# ── keep-alive (prevents Render free tier spin-down) ─────────────
def keep_alive():
    if not SELF_URL:
        return
    try:
        requests.get(f"{SELF_URL}/api/health", timeout=10)
        print(f"Keep-alive OK ({datetime.now(IST).strftime('%H:%M IST')})")
    except Exception as e:
        print(f"Keep-alive failed: {e}")


# ── seed list ─────────────────────────────────────────────────────
SEED_SYMBOLS = [
    {"symbol": "ABBOTINDIA",  "security_id": "788",   "exchange": "NSE"},
    {"symbol": "ABCAPITAL",   "security_id": "20904", "exchange": "NSE"},
    {"symbol": "ABFRL",       "security_id": "20179", "exchange": "NSE"},
    {"symbol": "ACC",         "security_id": "22",    "exchange": "NSE"},
    {"symbol": "ADANIENT",    "security_id": "25",    "exchange": "NSE"},
    {"symbol": "ADANIPORTS",  "security_id": "15083", "exchange": "NSE"},
    {"symbol": "ALKEM",       "security_id": "17963", "exchange": "NSE"},
    {"symbol": "AMBUJACEM",   "security_id": "1270",  "exchange": "NSE"},
    {"symbol": "ANGELONE",    "security_id": "20554", "exchange": "NSE"},
    {"symbol": "APLAPOLLO",   "security_id": "19229", "exchange": "NSE"},
    {"symbol": "APOLLOHOSP",  "security_id": "157",   "exchange": "NSE"},
    {"symbol": "APOLLOTYRE",  "security_id": "163",   "exchange": "NSE"},
    {"symbol": "ASHOKLEY",    "security_id": "212",   "exchange": "NSE"},
    {"symbol": "ASIANPAINT",  "security_id": "467",   "exchange": "NSE"},
    {"symbol": "ASTRAL",      "security_id": "14418", "exchange": "NSE"},
    {"symbol": "ATGL",        "security_id": "22169", "exchange": "NSE"},
    {"symbol": "ATUL",        "security_id": "263",   "exchange": "NSE"},
    {"symbol": "AUBANK",      "security_id": "21238", "exchange": "NSE"},
    {"symbol": "AUROPHARMA",  "security_id": "275",   "exchange": "NSE"},
    {"symbol": "AXISBANK",    "security_id": "5900",  "exchange": "NSE"},
    {"symbol": "BAJAJ-AUTO",  "security_id": "16669", "exchange": "NSE"},
    {"symbol": "BAJAJFINSV",  "security_id": "16675", "exchange": "NSE"},
    {"symbol": "BAJFINANCE",  "security_id": "317",   "exchange": "NSE"},
    {"symbol": "BALKRISIND",  "security_id": "1482",  "exchange": "NSE"},
    {"symbol": "BANDHANBNK",  "security_id": "21719", "exchange": "NSE"},
    {"symbol": "BANKBARODA",  "security_id": "1452",  "exchange": "NSE"},
    {"symbol": "BATAINDIA",   "security_id": "371",   "exchange": "NSE"},
    {"symbol": "BEL",         "security_id": "383",   "exchange": "NSE"},
    {"symbol": "BERGEPAINT",  "security_id": "404",   "exchange": "NSE"},
    {"symbol": "BHARATFORG",  "security_id": "422",   "exchange": "NSE"},
    {"symbol": "BHARTIARTL",  "security_id": "10604", "exchange": "NSE"},
    {"symbol": "BHEL",        "security_id": "438",   "exchange": "NSE"},
    {"symbol": "BIOCON",      "security_id": "11373", "exchange": "NSE"},
    {"symbol": "BOSCHLTD",    "security_id": "2181",  "exchange": "NSE"},
    {"symbol": "BPCL",        "security_id": "526",   "exchange": "NSE"},
    {"symbol": "BRITANNIA",   "security_id": "547",   "exchange": "NSE"},
    {"symbol": "BSOFT",       "security_id": "6004",  "exchange": "NSE"},
    {"symbol": "CANBK",       "security_id": "10794", "exchange": "NSE"},
    {"symbol": "CANFINHOME",  "security_id": "9262",  "exchange": "NSE"},
    {"symbol": "CDSL",        "security_id": "21822", "exchange": "NSE"},
    {"symbol": "CGPOWER",     "security_id": "678",   "exchange": "NSE"},
    {"symbol": "CHAMBLFERT",  "security_id": "685",   "exchange": "NSE"},
    {"symbol": "CHOLAFIN",    "security_id": "4375",  "exchange": "NSE"},
    {"symbol": "CIPLA",       "security_id": "694",   "exchange": "NSE"},
    {"symbol": "COALINDIA",   "security_id": "20374", "exchange": "NSE"},
    {"symbol": "COFORGE",     "security_id": "11543", "exchange": "NSE"},
    {"symbol": "COLPAL",      "security_id": "742",   "exchange": "NSE"},
    {"symbol": "CONCOR",      "security_id": "4749",  "exchange": "NSE"},
    {"symbol": "COROMANDEL",  "security_id": "739",   "exchange": "NSE"},
    {"symbol": "CROMPTON",    "security_id": "20655", "exchange": "NSE"},
    {"symbol": "CUB",         "security_id": "5784",  "exchange": "NSE"},
    {"symbol": "CUMMINSIND",  "security_id": "774",   "exchange": "NSE"},
    {"symbol": "DABUR",       "security_id": "804",   "exchange": "NSE"},
    {"symbol": "DALBHARAT",   "security_id": "18253", "exchange": "NSE"},
    {"symbol": "DEEPAKNTR",   "security_id": "10209", "exchange": "NSE"},
    {"symbol": "DELTACORP",   "security_id": "14413", "exchange": "NSE"},
    {"symbol": "DIVISLAB",    "security_id": "15174", "exchange": "NSE"},
    {"symbol": "DIXON",       "security_id": "21690", "exchange": "NSE"},
    {"symbol": "DLF",         "security_id": "14732", "exchange": "NSE"},
    {"symbol": "DMART",       "security_id": "21561", "exchange": "NSE"},
    {"symbol": "DRREDDY",     "security_id": "881",   "exchange": "NSE"},
    {"symbol": "EICHERMOT",   "security_id": "910",   "exchange": "NSE"},
    {"symbol": "ESCORTS",     "security_id": "958",   "exchange": "NSE"},
    {"symbol": "EXIDEIND",    "security_id": "993",   "exchange": "NSE"},
    {"symbol": "FEDERALBNK",  "security_id": "1023",  "exchange": "NSE"},
    {"symbol": "FORCEMOT",    "security_id": "1039",  "exchange": "NSE"},
    {"symbol": "FORTIS",      "security_id": "14804", "exchange": "NSE"},
    {"symbol": "GAIL",        "security_id": "1066",  "exchange": "NSE"},
    {"symbol": "GLENMARK",    "security_id": "1109",  "exchange": "NSE"},
    {"symbol": "GMRINFRA",    "security_id": "13528", "exchange": "NSE"},
    {"symbol": "GNFC",        "security_id": "1113",  "exchange": "NSE"},
    {"symbol": "GODREJCP",    "security_id": "10099", "exchange": "NSE"},
    {"symbol": "GODREJPROP",  "security_id": "17875", "exchange": "NSE"},
    {"symbol": "GRANULES",    "security_id": "11809", "exchange": "NSE"},
    {"symbol": "GRASIM",      "security_id": "315",   "exchange": "NSE"},
    {"symbol": "GUJGASLTD",   "security_id": "10599", "exchange": "NSE"},
    {"symbol": "HAL",         "security_id": "2303",  "exchange": "NSE"},
    {"symbol": "HAVELLS",     "security_id": "8927",  "exchange": "NSE"},
    {"symbol": "HCLTECH",     "security_id": "7229",  "exchange": "NSE"},
    {"symbol": "HDFCAMC",     "security_id": "22080", "exchange": "NSE"},
    {"symbol": "HDFCBANK",    "security_id": "1333",  "exchange": "NSE"},
    {"symbol": "HDFCLIFE",    "security_id": "20704", "exchange": "NSE"},
    {"symbol": "HEROMOTOCO",  "security_id": "1348",  "exchange": "NSE"},
    {"symbol": "HFCL",        "security_id": "1350",  "exchange": "NSE"},
    {"symbol": "HINDALCO",    "security_id": "1306",  "exchange": "NSE"},
    {"symbol": "HINDCOPPER",  "security_id": "14978", "exchange": "NSE"},
    {"symbol": "HINDPETRO",   "security_id": "1406",  "exchange": "NSE"},
    {"symbol": "HINDUNILVR",  "security_id": "1394",  "exchange": "NSE"},
    {"symbol": "ICICIBANK",   "security_id": "4963",  "exchange": "NSE"},
    {"symbol": "ICICIGI",     "security_id": "21770", "exchange": "NSE"},
    {"symbol": "ICICIPRULI",  "security_id": "18652", "exchange": "NSE"},
    {"symbol": "IDEA",        "security_id": "14366", "exchange": "NSE"},
    {"symbol": "IDFCFIRSTB",  "security_id": "20286", "exchange": "NSE"},
    {"symbol": "IEX",         "security_id": "22149", "exchange": "NSE"},
    {"symbol": "IGL",         "security_id": "11262", "exchange": "NSE"},
    {"symbol": "INDHOTEL",    "security_id": "1512",  "exchange": "NSE"},
    {"symbol": "INDIACEM",    "security_id": "1515",  "exchange": "NSE"},
    {"symbol": "INDIAMART",   "security_id": "22592", "exchange": "NSE"},
    {"symbol": "INDIGO",      "security_id": "20251", "exchange": "NSE"},
    {"symbol": "INDUSINDBK",  "security_id": "5258",  "exchange": "NSE"},
    {"symbol": "INDUSTOWER",  "security_id": "22271", "exchange": "NSE"},
    {"symbol": "INFY",        "security_id": "1594",  "exchange": "NSE"},
    {"symbol": "IOC",         "security_id": "1624",  "exchange": "NSE"},
    {"symbol": "IPCALAB",     "security_id": "1633",  "exchange": "NSE"},
    {"symbol": "IRCTC",       "security_id": "22961", "exchange": "NSE"},
    {"symbol": "ITC",         "security_id": "1660",  "exchange": "NSE"},
    {"symbol": "JINDALSTEL",  "security_id": "11600", "exchange": "NSE"},
    {"symbol": "JKCEMENT",    "security_id": "13910", "exchange": "NSE"},
    {"symbol": "JSWENERGY",   "security_id": "17594", "exchange": "NSE"},
    {"symbol": "JSWSTEEL",    "security_id": "11723", "exchange": "NSE"},
    {"symbol": "JUBLFOOD",    "security_id": "18096", "exchange": "NSE"},
    {"symbol": "KALYANKJIL",  "security_id": "22945", "exchange": "NSE"},
    {"symbol": "KEI",         "security_id": "1743",  "exchange": "NSE"},
    {"symbol": "KOTAKBANK",   "security_id": "1232",  "exchange": "NSE"},
    {"symbol": "KPITTECH",    "security_id": "4651",  "exchange": "NSE"},
    {"symbol": "LALPATHLAB",  "security_id": "23048", "exchange": "NSE"},
    {"symbol": "LAURUSLABS",  "security_id": "22950", "exchange": "NSE"},
    {"symbol": "LICHSGFIN",   "security_id": "1847",  "exchange": "NSE"},
    {"symbol": "LICI",        "security_id": "24095", "exchange": "NSE"},
    {"symbol": "LT",          "security_id": "11483", "exchange": "NSE"},
    {"symbol": "LTIM",        "security_id": "17818", "exchange": "NSE"},
    {"symbol": "LTTS",        "security_id": "20299", "exchange": "NSE"},
    {"symbol": "LUPIN",       "security_id": "10440", "exchange": "NSE"},
    {"symbol": "M&M",         "security_id": "2031",  "exchange": "NSE"},
    {"symbol": "M&MFIN",      "security_id": "13285", "exchange": "NSE"},
    {"symbol": "MANAPPURAM",  "security_id": "19061", "exchange": "NSE"},
    {"symbol": "MARICO",      "security_id": "4067",  "exchange": "NSE"},
    {"symbol": "MARUTI",      "security_id": "10999", "exchange": "NSE"},
    {"symbol": "MAXHEALTH",   "security_id": "23267", "exchange": "NSE"},
    {"symbol": "MCX",         "security_id": "19238", "exchange": "NSE"},
    {"symbol": "METROPOLIS",  "security_id": "22843", "exchange": "NSE"},
    {"symbol": "MFSL",        "security_id": "4136",  "exchange": "NSE"},
    {"symbol": "MOTHERSON",   "security_id": "4204",  "exchange": "NSE"},
    {"symbol": "MPHASIS",     "security_id": "4261",  "exchange": "NSE"},
    {"symbol": "MRF",         "security_id": "4162",  "exchange": "NSE"},
    {"symbol": "MUTHOOTFIN",  "security_id": "18143", "exchange": "NSE"},
    {"symbol": "NATIONALUM",  "security_id": "4244",  "exchange": "NSE"},
    {"symbol": "NAUKRI",      "security_id": "13751", "exchange": "NSE"},
    {"symbol": "NAVINFLUOR",  "security_id": "14500", "exchange": "NSE"},
    {"symbol": "NESTLEIND",   "security_id": "4306",  "exchange": "NSE"},
    {"symbol": "NMDC",        "security_id": "15332", "exchange": "NSE"},
    {"symbol": "NTPC",        "security_id": "11630", "exchange": "NSE"},
    {"symbol": "OBEROIRLTY",  "security_id": "20242", "exchange": "NSE"},
    {"symbol": "OFSS",        "security_id": "10738", "exchange": "NSE"},
    {"symbol": "ONGC",        "security_id": "2475",  "exchange": "NSE"},
    {"symbol": "PAGEIND",     "security_id": "14401", "exchange": "NSE"},
    {"symbol": "PEL",         "security_id": "2481",  "exchange": "NSE"},
    {"symbol": "PERSISTENT",  "security_id": "18365", "exchange": "NSE"},
    {"symbol": "PETRONET",    "security_id": "11351", "exchange": "NSE"},
    {"symbol": "PFC",         "security_id": "14299", "exchange": "NSE"},
    {"symbol": "PIDILITIND",  "security_id": "2664",  "exchange": "NSE"},
    {"symbol": "PIIND",       "security_id": "19015", "exchange": "NSE"},
    {"symbol": "PNB",         "security_id": "2730",  "exchange": "NSE"},
    {"symbol": "POLYCAB",     "security_id": "22185", "exchange": "NSE"},
    {"symbol": "POWERGRID",   "security_id": "14977", "exchange": "NSE"},
    {"symbol": "PVRINOX",     "security_id": "17243", "exchange": "NSE"},
    {"symbol": "RAMCOCEM",    "security_id": "14994", "exchange": "NSE"},
    {"symbol": "RBLBANK",     "security_id": "20413", "exchange": "NSE"},
    {"symbol": "RECLTD",      "security_id": "15355", "exchange": "NSE"},
    {"symbol": "RELIANCE",    "security_id": "2885",  "exchange": "NSE"},
    {"symbol": "SAIL",        "security_id": "2963",  "exchange": "NSE"},
    {"symbol": "SBICARD",     "security_id": "22990", "exchange": "NSE"},
    {"symbol": "SBILIFE",     "security_id": "21808", "exchange": "NSE"},
    {"symbol": "SBIN",        "security_id": "3045",  "exchange": "NSE"},
    {"symbol": "SHREECEM",    "security_id": "3103",  "exchange": "NSE"},
    {"symbol": "SHRIRAMFIN",  "security_id": "20817", "exchange": "NSE"},
    {"symbol": "SIEMENS",     "security_id": "3150",  "exchange": "NSE"},
    {"symbol": "SRF",         "security_id": "3273",  "exchange": "NSE"},
    {"symbol": "SUNPHARMA",   "security_id": "3351",  "exchange": "NSE"},
    {"symbol": "SUNTV",       "security_id": "3367",  "exchange": "NSE"},
    {"symbol": "SUPREMEIND",  "security_id": "3378",  "exchange": "NSE"},
    {"symbol": "SUZLON",      "security_id": "3391",  "exchange": "NSE"},
    {"symbol": "SYNGENE",     "security_id": "20562", "exchange": "NSE"},
    {"symbol": "TATACHEM",    "security_id": "3405",  "exchange": "NSE"},
    {"symbol": "TATACOMM",    "security_id": "3408",  "exchange": "NSE"},
    {"symbol": "TATACONSUM",  "security_id": "3432",  "exchange": "NSE"},
    {"symbol": "TATAELXSI",   "security_id": "4910",  "exchange": "NSE"},
    {"symbol": "TATAMOTORS",  "security_id": "3456",  "exchange": "NSE"},
    {"symbol": "TATAPOWER",   "security_id": "3426",  "exchange": "NSE"},
    {"symbol": "TATASTEEL",   "security_id": "3499",  "exchange": "NSE"},
    {"symbol": "TCS",         "security_id": "11536", "exchange": "NSE"},
    {"symbol": "TECHM",       "security_id": "13538", "exchange": "NSE"},
    {"symbol": "TIINDIA",     "security_id": "19455", "exchange": "NSE"},
    {"symbol": "TITAN",       "security_id": "3506",  "exchange": "NSE"},
    {"symbol": "TORNTPHARM",  "security_id": "3518",  "exchange": "NSE"},
    {"symbol": "TORNTPOWER",  "security_id": "3519",  "exchange": "NSE"},
    {"symbol": "TRENT",       "security_id": "3530",  "exchange": "NSE"},
    {"symbol": "TVSMOTOR",    "security_id": "3559",  "exchange": "NSE"},
    {"symbol": "UBL",         "security_id": "16713", "exchange": "NSE"},
    {"symbol": "ULTRACEMCO",  "security_id": "11532", "exchange": "NSE"},
    {"symbol": "UNIONBANK",   "security_id": "10754", "exchange": "NSE"},
    {"symbol": "UPL",         "security_id": "11287", "exchange": "NSE"},
    {"symbol": "VEDL",        "security_id": "3063",  "exchange": "NSE"},
    {"symbol": "VOLTAS",      "security_id": "3597",  "exchange": "NSE"},
    {"symbol": "WIPRO",       "security_id": "3787",  "exchange": "NSE"},
    {"symbol": "ZEEL",        "security_id": "3812",  "exchange": "NSE"},
    {"symbol": "ZOMATO",      "security_id": "23652", "exchange": "NSE"},
    {"symbol": "ZYDUSLIFE",   "security_id": "23148", "exchange": "NSE"},
]


# ── fetch FNO symbols from Dhan master CSV ────────────────────────
def fetch_fno_symbols():
    global SYMBOLS
    SYMBOLS = list(SEED_SYMBOLS)
    cache["symbol_source"] = "seed"
    try:
        print("Fetching Dhan master CSV...")
        resp = requests.get(
            "https://images.dhan.co/api-data/api-scrip-master.csv", timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text), low_memory=False)
        df.columns = [c.strip().upper() for c in df.columns]
        print(f"CSV columns: {list(df.columns)}")

        seg_col  = next((c for c in df.columns if "EXCH_SEG"  in c or "SEGMENT"        in c), None)
        sym_col  = next((c for c in df.columns if "TRADING_SYMBOL" in c or "SYMBOL_NAME" in c), None)
        id_col   = next((c for c in df.columns if "SECURITY_ID"    in c or "SCRIP_ID"    in c or "SM_SYMBOL_ID" in c), None)
        inst_col = next((c for c in df.columns if "INSTRUMENT" in c), None)

        if not all([seg_col, sym_col, id_col]):
            print("CSV column mismatch — using seed.")
            return

        eq_df  = df[df[seg_col].str.upper().str.strip() == "NSE_EQ"].copy()
        fno_df = df[df[seg_col].str.upper().str.strip() == "NSE_FNO"].copy()
        if inst_col:
            fno_df = fno_df[fno_df[inst_col].str.upper().str.strip() == "FUTSTK"]

        fno_syms = set(fno_df[sym_col].str.strip().unique())
        if not fno_syms:
            print("No FUTSTK symbols — using seed.")
            return

        matched = eq_df[eq_df[sym_col].str.strip().isin(fno_syms)][
            [id_col, sym_col]].drop_duplicates(subset=[sym_col])

        if matched.empty:
            print("No matches — using seed.")
            return

        SYMBOLS = [{"symbol": r[sym_col].strip(), "security_id": str(int(r[id_col])), "exchange": "NSE"}
                   for _, r in matched.iterrows()]
        cache["symbol_source"] = "csv"
        print(f"Loaded {len(SYMBOLS)} FNO stocks from CSV.")
    except Exception as e:
        print(f"CSV fetch failed ({e}) — using seed ({len(SYMBOLS)} stocks).")


# ── historical OHLCV fetch ────────────────────────────────────────
def get_historical(security_id, access_token, retries=3, capture_debug=False):
    today     = datetime.now(IST)
    from_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    url       = f"{DHAN_BASE}/v2/charts/historical"
    headers   = {"access-token": access_token, "Content-Type": "application/json"}
    payload   = {
        "securityId": security_id, "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",    "expiryCode": 0,
        "oi": False, "fromDate": from_date, "toDate": to_date,
    }

    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=10)

            if capture_debug:
                try:    raw_keys = list(resp.json().keys())
                except: raw_keys = []
                cache["debug"] = {
                    "security_id": security_id, "status_code": resp.status_code,
                    "from_date": from_date, "to_date": to_date,
                    "raw_keys": raw_keys, "raw_sample": resp.text[:300],
                }

            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  429 — sleeping {wait}s"); time.sleep(wait); continue
            if resp.status_code in (400, 401, 403):
                print(f"  HTTP {resp.status_code} id={security_id}: {resp.text[:80]}")
                return None

            resp.raise_for_status()
            data = resp.json()
            if not data.get("timestamp"):
                return None

            df = pd.DataFrame({
                "date":   data["timestamp"],
                "open":   data.get("open",   []),
                "high":   data.get("high",   []),
                "low":    data.get("low",    []),
                "close":  data.get("close",  []),
                "volume": data.get("volume", []),
            })
            return df.sort_values("date").reset_index(drop=True)

        except requests.exceptions.Timeout:
            print(f"  Timeout id={security_id} attempt {attempt+1}"); time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"  ReqError id={security_id}: {e}"); time.sleep(1)
    return None


# ── screener worker ───────────────────────────────────────────────
def _run_screener(tok):
    global cache
    if not SYMBOLS:
        fetch_fno_symbols()

    results, errors, skipped = [], [], []
    total      = len(SYMBOLS)
    first_call = True

    cache.update({"status": "fetching", "progress": 0, "total": total,
                  "market_open": is_market_open()})
    print(f"Screener started — {total} stocks | {datetime.now(IST).strftime('%H:%M:%S IST')}")

    for i, sym in enumerate(SYMBOLS):
        try:
            df = get_historical(sym["security_id"], tok, capture_debug=first_call)
            first_call = False

            if df is None:
                skipped.append(f"{sym['symbol']}:None")
            elif len(df) < 2:
                skipped.append(f"{sym['symbol']}:{len(df)}rows")
            else:
                rows       = len(df)
                today_vol  = df["volume"].iloc[-1]
                hist_rows  = min(rows - 1, 7)
                avg_vol_7d = df["volume"].iloc[-(hist_rows+1):-1].mean()
                vol_ratio  = round(today_vol / avg_vol_7d, 2) if avg_vol_7d > 0 else 0
                close_now  = df["close"].iloc[-1]
                close_prev = df["close"].iloc[-2]
                day_chg    = round(((close_now - close_prev) / close_prev) * 100, 2) if close_prev else 0
                lookback   = min(rows - 1, 5)
                close_ago  = df["close"].iloc[-(lookback+1)]
                momentum5d = round(((close_now - close_ago) / close_ago) * 100, 2) if close_ago else 0
                results.append({
                    "symbol": sym["symbol"], "exchange": sym["exchange"],
                    "ltp": round(float(close_now), 2), "change": day_chg,
                    "momentum5d": momentum5d, "volumeRatio": vol_ratio,
                    "todayVol": int(today_vol), "avgVol7d": int(avg_vol_7d),
                    "candles": rows,
                })
        except Exception as e:
            errors.append(f"{sym['symbol']}: {e}")
            print(f"  Error {sym['symbol']}: {e}")

        cache["progress"] = round((i + 1) / total * 100)
        time.sleep(0.3)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{total} | ok={len(results)} skip={len(skipped)}")
            time.sleep(1.5)

    results.sort(key=lambda x: x["volumeRatio"], reverse=True)
    now_ist = datetime.now(IST)
    cache.update({
        "data": results, "updated_at": now_ist.strftime("%H:%M:%S IST"),
        "status": "ok", "count": len(results), "errors": errors,
        "skipped": skipped[:20], "progress": 100, "market_open": is_market_open(),
    })
    print(f"Done — {len(results)} OK | {len(skipped)} skipped | {len(errors)} errors")


def fetch_screener(client_id=None, access_token=None):
    cid = client_id or CREDS["client_id"]
    tok = access_token or CREDS["access_token"]
    if not cid or not tok:
        cache["status"] = "no_credentials"; return
    CREDS["client_id"] = cid; CREDS["access_token"] = tok
    if _screener_lock.locked():
        print("Screener already running — skipping."); return
    def _worker():
        with _screener_lock: _run_screener(tok)
    threading.Thread(target=_worker, daemon=True).start()


# ── scheduler ─────────────────────────────────────────────────────
def scheduled_refresh():
    if not CREDS["client_id"] or not CREDS["access_token"]: return
    if is_market_open(): fetch_screener()
    else: print(f"Market closed — skipping ({datetime.now(IST).strftime('%H:%M IST')})")


scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_refresh, "interval", minutes=5,  id="screener")
scheduler.add_job(keep_alive,        "interval", minutes=10, id="keepalive")
scheduler.start()


# ── startup ───────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    fetch_fno_symbols()
    print(f"Symbols ready: {len(SYMBOLS)} ({cache['symbol_source']})")
    def _delayed():
        time.sleep(3)
        fetch_screener()
    threading.Thread(target=_delayed, daemon=True).start()


# ── routes ────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "service": "DhanScreen API", "docs": "/docs",
            "time_ist": datetime.now(IST).strftime("%H:%M:%S"), "market_open": is_market_open()}


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
        "time_ist": datetime.now(IST).strftime("%H:%M:%S"),
        "day": datetime.now(IST).strftime("%A"),
        "debug": cache.get("debug", {}),
    }


# ── /api/ltp — live prices in ONE call using marketfeed/ohlc ─────
@app.get("/api/ltp")
def get_ltp(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    cid = x_client_id or CREDS["client_id"]
    tok = x_access_token or CREDS["access_token"]

    if not cid or not tok: return {"status": "no_credentials", "quotes": {}}
    if not SYMBOLS:        return {"status": "no_symbols",     "quotes": {}}

    payload = {"NSE_EQ": {sym["security_id"]: 0 for sym in SYMBOLS}}

    try:
        resp = requests.post(
            f"{DHAN_BASE}/v2/marketfeed/ohlc",
            json=payload,
            headers={"access-token": tok, "client-id": cid, "Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 401: return {"status": "token_expired", "quotes": {}}
        if resp.status_code == 429: return {"status": "rate_limited",  "quotes": {}}
        if resp.status_code == 400: return {"status": "bad_request",   "quotes": {}, "detail": resp.text[:200]}
        resp.raise_for_status()

        raw       = resp.json().get("data", {}).get("NSE_EQ", {})
        id_to_sym = {sym["security_id"]: sym["symbol"] for sym in SYMBOLS}
        quotes    = {}

        for sec_id, q in raw.items():
            sym = id_to_sym.get(str(sec_id))
            if not sym: continue
            ltp        = float(q.get("last_price", 0))
            ohlc       = q.get("ohlc", {})
            prev_close = float(ohlc.get("close", 0))
            chg_pct    = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close else 0
            quotes[sym] = {
                "ltp":        round(ltp, 2),
                "open":       round(float(ohlc.get("open", 0)), 2),
                "high":       round(float(ohlc.get("high", 0)), 2),
                "low":        round(float(ohlc.get("low",  0)), 2),
                "prev_close": round(prev_close, 2),
                "change_pct": chg_pct,
            }

        return {
            "status":      "ok",
            "updated_at":  datetime.now(IST).strftime("%H:%M:%S IST"),
            "market_open": is_market_open(),
            "count":       len(quotes),
            "quotes":      quotes,
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "quotes": {}}
