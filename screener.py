from fastapi import FastAPI
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

app = FastAPI(title="Dhan Momentum API", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DHAN_BASE = "https://api.dhan.co"
IST = ZoneInfo("Asia/Kolkata")

cache = {
    "data": [],
    "updated_at": None,
    "count": 0,
    "debug": {}
}

CREDS = {
    "client_id": os.getenv("DHAN_CLIENT_ID"),
    "access_token": os.getenv("DHAN_ACCESS_TOKEN"),
}

SYMBOLS = []

# ---------------- SYMBOLS ----------------
def fetch_symbols():
    global SYMBOLS
    try:
        resp = requests.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=15)
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.strip().upper() for c in df.columns]

        eq = df[df["EXCH_SEG"] == "NSE_EQ"]
        fno = df[(df["EXCH_SEG"] == "NSE_FNO") & (df["INSTRUMENT"] == "FUTSTK")]

        fno_syms = set(fno["TRADING_SYMBOL"])
        matched = eq[eq["TRADING_SYMBOL"].isin(fno_syms)]

        SYMBOLS = [
            {
                "symbol": r["TRADING_SYMBOL"],
                "security_id": str(int(r["SECURITY_ID"])),
                "exchange": "NSE",
            }
            for _, r in matched.iterrows()
        ]

        print("Loaded symbols:", len(SYMBOLS))

    except Exception as e:
        print("Symbol load failed:", e)


# ---------------- QUOTES ----------------
def parse_quote(sec_id, q, id_to_sym):
    sym = id_to_sym.get(str(sec_id))
    if not sym:
        return None, None

    ltp = float(q.get("last_price", 0))
    prev_close = float(q.get("ohlc", {}).get("close", 0))
    volume = int(q.get("volume", 0))

    change = round(((ltp - prev_close) / prev_close) * 100, 2) if prev_close else 0

    return sym, {
        "ltp": round(ltp, 2),
        "change": change,
        "volume": volume,
    }


def fetch_all_quotes():
    headers = {
        "access-token": CREDS["access_token"],
        "client-id": CREDS["client_id"],
        "Content-Type": "application/json",
    }

    ids = [int(s["security_id"]) for s in SYMBOLS]
    id_to_sym = {s["security_id"]: s["symbol"] for s in SYMBOLS}

    quotes = {}

    # 🔥 MAIN FETCH (smaller batches)
    for i in range(0, len(ids), 200):
        batch = ids[i:i + 200]

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

        time.sleep(1.3)

    # 🔥 RETRY MISSING
    missing = [s for s in SYMBOLS if s["symbol"] not in quotes]

    if missing:
        print("Retry missing:", len(missing))

        retry_ids = [int(s["security_id"]) for s in missing]

        for i in range(0, len(retry_ids), 100):
            batch = retry_ids[i:i + 100]

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
                print("Retry error:", e)

            time.sleep(1)

    return quotes


# ---------------- HISTORICAL ----------------
def get_historical(sec_id):
    try:
        today = datetime.now(IST)

        resp = requests.post(
            f"{DHAN_BASE}/v2/charts/historical",
            json={
                "securityId": sec_id,
                "exchangeSegment": "NSE_EQ",
                "instrument": "EQUITY",
                "expiryCode": 0,
                "fromDate": (today - timedelta(days=10)).strftime("%Y-%m-%d"),
                "toDate": today.strftime("%Y-%m-%d"),
            },
            headers={"access-token": CREDS["access_token"]},
            timeout=10,
        )

        if resp.status_code == 200:
            data = resp.json()
            return data.get("volume", [])

    except:
        pass

    return []


# ---------------- PROCESS ----------------
def process_symbol(sym, quotes):
    q = quotes.get(sym["symbol"])

    if not q:
        return {
            "symbol": sym["symbol"],
            "ltp": 0,
            "change": 0,
            "volumeRatio": 0,
            "momentum": 0,
            "status": "missing"
        }

    ltp = q["ltp"]
    change = q["change"]
    vol = q["volume"]

    hist_vol = get_historical(sym["security_id"])

    vol_ratio = 1
    if len(hist_vol) >= 7:
        avg = sum(hist_vol[-7:]) / 7
        if avg > 0:
            vol_ratio = round(vol / avg, 2)

    momentum = round(change * vol_ratio, 2)

    return {
        "symbol": sym["symbol"],
        "ltp": ltp,
        "change": change,
        "volumeRatio": vol_ratio,
        "momentum": momentum,
        "status": "ok"
    }


# ---------------- MAIN ----------------
def run_screener():
    quotes = fetch_all_quotes()

    results = []

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(process_symbol, s, quotes) for s in SYMBOLS]

        for f in as_completed(futures):
            results.append(f.result())

    results.sort(key=lambda x: x["momentum"], reverse=True)

    cache["data"] = results
    cache["count"] = len(results)
    cache["updated_at"] = datetime.now(IST).strftime("%H:%M:%S")

    cache["debug"] = {
        "total_symbols": len(SYMBOLS),
        "quotes_fetched": len(quotes),
        "success_rate": round((len(quotes) / len(SYMBOLS)) * 100, 2),
    }


# ---------------- ROUTES ----------------
@app.on_event("startup")
def startup():
    fetch_symbols()
    run_screener()

    scheduler = BackgroundScheduler()
    scheduler.add_job(run_screener, "interval", seconds=60)
    scheduler.start()


@app.get("/api/screener")
def screener():
    return cache


@app.get("/run")
def run_now():
    run_screener()
    return {"status": "triggered"}
