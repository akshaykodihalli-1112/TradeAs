from fastapi import FastAPI, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
import threading
import time, os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

app = FastAPI(title="DhanScreen API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DHAN_BASE = "https://api.dhan.co"
IST = ZoneInfo("Asia/Kolkata")
SELF_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

cache = {
    "data": [],
    "updated_at": None,
    "status": "idle",
    "count": 0,
    "errors": [],
    "progress": 0,
    "total": 0,
    "symbol_source": "seed",
    "market_open": False,
    "debug": {},
}

CREDS = {
    "client_id": os.getenv("DHAN_CLIENT_ID", ""),
    "access_token": os.getenv("DHAN_ACCESS_TOKEN", ""),
}

SYMBOLS = []
_screener_lock = threading.Lock()


# ---------------- UTIL ----------------
def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return now.replace(hour=9, minute=15) <= now <= now.replace(hour=15, minute=30)


def get_ist_now():
    return datetime.now(IST)


# ---------------- SYMBOLS ----------------
def fetch_fno_symbols():
    global SYMBOLS
    try:
        resp = requests.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=15)
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.strip().upper() for c in df.columns]

        eq_df = df[df["EXCH_SEG"] == "NSE_EQ"]
        fno_df = df[(df["EXCH_SEG"] == "NSE_FNO") & (df["INSTRUMENT"] == "FUTSTK")]

        fno_syms = set(fno_df["TRADING_SYMBOL"])
        matched = eq_df[eq_df["TRADING_SYMBOL"].isin(fno_syms)]

        SYMBOLS = [
            {
                "symbol": r["TRADING_SYMBOL"],
                "security_id": str(int(r["SECURITY_ID"])),
                "exchange": "NSE",
            }
            for _, r in matched.iterrows()
        ]

        cache["symbol_source"] = "csv"
        print(f"Loaded {len(SYMBOLS)} symbols")

    except Exception as e:
        print("CSV failed, fallback to empty:", e)


# ---------------- QUOTES ----------------
def parse_quote(sec_id, q, id_to_sym):
    sym = id_to_sym.get(str(sec_id))
    if not sym:
        return None, None

    ltp = float(q.get("last_price", 0))
    ohlc = q.get("ohlc", {})
    prev_close = float(ohlc.get("close", 0))

    chg_pct = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close else 0

    return sym, {
        "ltp": round(ltp, 2),
        "change_pct": chg_pct,
        "volume": int(q.get("volume", 0)),
    }


def fetch_all_quotes(cid, tok):
    headers = {
        "access-token": tok,
        "client-id": cid,
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }

    ids = [int(s["security_id"]) for s in SYMBOLS]
    id_to_sym = {s["security_id"]: s["symbol"] for s in SYMBOLS}

    quotes = {}

    for i in range(0, len(ids), 500):
        batch = ids[i:i + 500]

        try:
            resp = requests.post(
                f"{DHAN_BASE}/v2/marketfeed/quote",
                json={"NSE_EQ": batch},
                headers=headers,
                timeout=10,
            )

            if resp.status_code == 200:
                raw = resp.json().get("data", {}).get("NSE_EQ", {})
                for sec_id, q in raw.items():
                    sym, data = parse_quote(sec_id, q, id_to_sym)
                    if sym:
                        quotes[sym] = data
        except Exception as e:
            print("Quote error:", e)

        time.sleep(1.1)

    # Retry missing
    missing = [s for s in SYMBOLS if s["symbol"] not in quotes]

    if missing:
        retry_ids = [int(s["security_id"]) for s in missing]

        try:
            resp = requests.post(
                f"{DHAN_BASE}/v2/marketfeed/quote",
                json={"NSE_EQ": retry_ids},
                headers=headers,
                timeout=10,
            )

            if resp.status_code == 200:
                raw = resp.json().get("data", {}).get("NSE_EQ", {})
                for sec_id, q in raw.items():
                    sym, data = parse_quote(sec_id, q, id_to_sym)
                    if sym:
                        quotes[sym] = data
        except Exception as e:
            print("Retry error:", e)

    return quotes


# ---------------- HISTORICAL ----------------
def get_historical(security_id, token):
    try:
        today = datetime.now(IST)
        payload = {
            "securityId": security_id,
            "exchangeSegment": "NSE_EQ",
            "instrument": "EQUITY",
            "expiryCode": 0,
            "fromDate": (today - timedelta(days=30)).strftime("%Y-%m-%d"),
            "toDate": today.strftime("%Y-%m-%d"),
        }

        resp = requests.post(
            f"{DHAN_BASE}/v2/charts/historical",
            json=payload,
            headers={"access-token": token},
            timeout=10,
        )

        if resp.status_code == 200:
            data = resp.json()
            return data.get("close"), data.get("volume")

    except Exception:
        pass

    return None, None


# ---------------- PROCESS ----------------
def process_symbol(sym, quotes, tok):
    try:
        q = quotes.get(sym["symbol"])
        if not q:
            return None

        ltp = q["ltp"]
        chg = q["change_pct"]
        vol = q["volume"]

        # smart skip
        if vol < 50000 and abs(chg) < 1:
            return {
                "symbol": sym["symbol"],
                "ltp": ltp,
                "change": chg,
                "volumeRatio": 1,
            }

        closes, volumes = get_historical(sym["security_id"], tok)

        vol_ratio = 1
        if volumes and len(volumes) >= 8:
            avg = sum(volumes[-8:-1]) / 7
            if avg > 0:
                vol_ratio = round(vol / avg, 2)

        return {
            "symbol": sym["symbol"],
            "ltp": ltp,
            "change": chg,
            "volumeRatio": vol_ratio,
        }

    except Exception:
        return None


# ---------------- MAIN ----------------
def run_screener():
    cid = CREDS["client_id"]
    tok = CREDS["access_token"]

    quotes = fetch_all_quotes(cid, tok)

    results = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_symbol, s, quotes, tok) for s in SYMBOLS]

        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)

    results.sort(key=lambda x: x["volumeRatio"], reverse=True)

    cache.update({
        "data": results,
        "count": len(results),
        "status": "ok",
        "updated_at": get_ist_now().strftime("%H:%M:%S"),
    })


# ---------------- ROUTES ----------------
@app.on_event("startup")
def startup():
    fetch_fno_symbols()
    threading.Thread(target=run_screener, daemon=True).start()


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/api/screener")
def screener():
    return cache
