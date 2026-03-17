from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import requests, pandas as pd, threading, time, os
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO

app = FastAPI(title="FNO Screener API", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DHAN_BASE = "https://api.dhan.co"
IST = ZoneInfo("Asia/Kolkata")

cache = {"data": [], "status": "idle", "count": 0, "progress": 0, "debug": {}}
CREDS = {"client_id": os.getenv("DHAN_CLIENT_ID", ""), "access_token": os.getenv("DHAN_ACCESS_TOKEN", "")}
SYMBOLS = []
_lock = threading.Lock()


# ─────────────────────────── UTILS ───────────────────────────
def now():
    return datetime.now(IST)

def is_market_open():
    n = now()
    return n.weekday() < 5 and n.replace(hour=9, minute=15) <= n <= n.replace(hour=15, minute=30)


# ─────────────────────────── F&O SYMBOL FETCH (FIXED) ───────────────────────────
def fetch_symbols():
    global SYMBOLS
    try:
        resp = requests.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=15)
        df = pd.read_csv(StringIO(resp.text), low_memory=False)
        df.columns = [c.strip().upper() for c in df.columns]

        seg = "EXCH_SEG"
        sym = "TRADING_SYMBOL"
        sec = "SECURITY_ID"
        inst = "INSTRUMENT"

        # ✅ Step 1: Get F&O FUTSTK symbols
        fno = df[(df[seg] == "NSE_FNO") & (df[inst] == "FUTSTK")]
        fno_symbols = set(fno[sym].str.strip().unique())

        # ✅ Step 2: Map to NSE_EQ for marketfeed
        eq = df[df[seg] == "NSE_EQ"]
        matched = eq[eq[sym].str.strip().isin(fno_symbols)]

        SYMBOLS = [
            {
                "symbol": r[sym].strip(),
                "security_id": str(int(r[sec])),
                "exchange": "NSE"
            }
            for _, r in matched.iterrows()
        ]

        print(f"✅ Loaded {len(SYMBOLS)} F&O stocks")

    except Exception as e:
        print("❌ CSV fetch failed:", e)


# ─────────────────────────── QUOTES (FIXED) ───────────────────────────
def fetch_all_quotes(cid, tok):
    all_ids = [int(s["security_id"]) for s in SYMBOLS]
    id_map = {s["security_id"]: s["symbol"] for s in SYMBOLS}

    headers = {"access-token": tok, "client-id": cid, "Content-Type": "application/json"}
    quotes = {}

    for i in range(0, len(all_ids), 200):
        batch = all_ids[i:i+200]
        payload = {"NSE_EQ": batch}

        for endpoint in ["/v2/marketfeed/quote", "/v2/marketfeed/ohlc"]:
            try:
                r = requests.post(DHAN_BASE + endpoint, json=payload, headers=headers, timeout=10)

                if r.status_code == 200:
                    data = r.json().get("data", {}).get("NSE_EQ", {})

                    for sec_id, q in data.items():
                        sym = id_map.get(str(sec_id))
                        if not sym:
                            continue

                        ltp = float(q.get("last_price", 0))
                        ohlc = q.get("ohlc", {})
                        pc = float(ohlc.get("close", 0))

                        chg = ((ltp - pc) / pc * 100) if pc else 0

                        quotes[sym] = {
                            "ltp": round(ltp, 2),
                            "change": round(chg, 2),
                            "volume": int(q.get("volume", 0))
                        }

                elif r.status_code == 429:
                    print("Rate limit hit, sleeping...")
                    time.sleep(2)

            except Exception:
                pass

        time.sleep(1.2)

    # 🔥 Retry missing (important)
    missing = [s for s in SYMBOLS if s["symbol"] not in quotes]

    for sym in missing[:100]:
        try:
            payload = {"NSE_EQ": [int(sym["security_id"])]}
            r = requests.post(DHAN_BASE + "/v2/marketfeed/quote", json=payload, headers=headers, timeout=5)

            if r.status_code == 200:
                data = r.json().get("data", {}).get("NSE_EQ", {})
                for sec_id, q in data.items():
                    sname = id_map.get(str(sec_id))
                    if sname:
                        ltp = float(q.get("last_price", 0))
                        ohlc = q.get("ohlc", {})
                        pc = float(ohlc.get("close", 0))
                        chg = ((ltp - pc) / pc * 100) if pc else 0

                        quotes[sname] = {
                            "ltp": round(ltp, 2),
                            "change": round(chg, 2),
                            "volume": int(q.get("volume", 0))
                        }

            time.sleep(0.2)
        except:
            pass

    cache["debug"] = {
        "total_symbols": len(SYMBOLS),
        "quotes_fetched": len(quotes),
        "missing": len(SYMBOLS) - len(quotes),
    }

    return quotes


# ─────────────────────────── SCREENER ───────────────────────────
def run():
    if not CREDS["access_token"]:
        return

    cid = CREDS["client_id"]
    tok = CREDS["access_token"]

    cache["status"] = "running"

    quotes = fetch_all_quotes(cid, tok)

    res = []

    for s in SYMBOLS:
        q = quotes.get(s["symbol"])

        if not q:
            # ✅ keep ALL F&O stocks
            res.append({
                "symbol": s["symbol"],
                "ltp": 0,
                "change": 0,
                "volume": 0,
                "status": "no_data"
            })
        else:
            res.append({
                "symbol": s["symbol"],
                "ltp": q["ltp"],
                "change": q["change"],
                "volume": q["volume"],
                "status": "ok"
            })

    res.sort(key=lambda x: x["volume"], reverse=True)

    cache.update({
        "data": res,
        "count": len(res),
        "status": "ok",
        "progress": 100
    })


def start():
    if _lock.locked():
        return

    def worker():
        with _lock:
            run()

    threading.Thread(target=worker, daemon=True).start()


# ─────────────────────────── API ───────────────────────────
@app.on_event("startup")
def startup():
    fetch_symbols()
    start()


@app.get("/")
def root():
    return {"status": "ok", "market_open": is_market_open()}


@app.get("/api/screener")
def screener(x_client_id: str = Header(None), x_access_token: str = Header(None)):
    if x_client_id and x_access_token:
        CREDS["client_id"] = x_client_id
        CREDS["access_token"] = x_access_token
        start()
    return cache


@app.get("/api/health")
def health():
    return {
        "status": cache["status"],
        "count": cache["count"],
        "symbols": len(SYMBOLS),
        "market_open": is_market_open(),
        "time": now().strftime("%H:%M:%S"),
        "debug": cache.get("debug", {})
    }


# ─────────────────────────── SCHEDULER ───────────────────────────
scheduler = BackgroundScheduler()
scheduler.add_job(start, "interval", minutes=5)
scheduler.start()
