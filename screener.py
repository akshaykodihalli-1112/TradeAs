# ─────────────────────────────────────────────────────────────────────────────

# DhanScreen Backend — FastAPI + Dhan API

# Install: pip install -r requirements.txt

# Run:     uvicorn screener:app –reload –port 8000

# ─────────────────────────────────────────────────────────────────────────────

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import pandas as pd
import time, os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title=“DhanScreen API”, version=“1.0”)

app.add_middleware(
CORSMiddleware,
allow_origins=[”*”],   # Lock to your Netlify URL in production
allow_methods=[”*”],
allow_headers=[”*”],
)

# ─── Dhan API Base ────────────────────────────────────────────────────────────

DHAN_BASE = “https://api.dhan.co”

# ─── NSE Symbols + Security IDs ──────────────────────────────────────────────

# security_id comes from Dhan’s instrument master CSV

# Download from: https://images.dhan.co/api-data/api-scrip-master.csv

# Below are sample IDs — replace with correct ones from master CSV

SYMBOLS = [
{“symbol”: “RELIANCE”,   “security_id”: “2885”,  “exchange”: “NSE”},
{“symbol”: “TCS”,        “security_id”: “11536”, “exchange”: “NSE”},
{“symbol”: “HDFCBANK”,   “security_id”: “1333”,  “exchange”: “NSE”},
{“symbol”: “INFY”,       “security_id”: “1594”,  “exchange”: “NSE”},
{“symbol”: “ICICIBANK”,  “security_id”: “4963”,  “exchange”: “NSE”},
{“symbol”: “WIPRO”,      “security_id”: “3787”,  “exchange”: “NSE”},
{“symbol”: “AXISBANK”,   “security_id”: “5900”,  “exchange”: “NSE”},
{“symbol”: “SBIN”,       “security_id”: “3045”,  “exchange”: “NSE”},
{“symbol”: “TATAMOTORS”, “security_id”: “3456”,  “exchange”: “NSE”},
{“symbol”: “TATASTEEL”,  “security_id”: “3503”,  “exchange”: “NSE”},
{“symbol”: “ADANIENT”,   “security_id”: “25”,    “exchange”: “NSE”},
{“symbol”: “BAJFINANCE”, “security_id”: “317”,   “exchange”: “NSE”},
{“symbol”: “MARUTI”,     “security_id”: “10999”, “exchange”: “NSE”},
{“symbol”: “SUNPHARMA”,  “security_id”: “3351”,  “exchange”: “NSE”},
{“symbol”: “ONGC”,       “security_id”: “2475”,  “exchange”: “NSE”},
{“symbol”: “NTPC”,       “security_id”: “11630”, “exchange”: “NSE”},
{“symbol”: “POWERGRID”,  “security_id”: “14977”, “exchange”: “NSE”},
{“symbol”: “COALINDIA”,  “security_id”: “20374”, “exchange”: “NSE”},
{“symbol”: “HINDALCO”,   “security_id”: “1300”,  “exchange”: “NSE”},
{“symbol”: “JSWSTEEL”,   “security_id”: “11723”, “exchange”: “NSE”},
]

# ─── Cache ────────────────────────────────────────────────────────────────────

cache = {
“data”:       [],
“updated_at”: None,
“status”:     “idle”,
“count”:      0,
}

# Store credentials from env or from request header

CREDS = {
“client_id”:    os.getenv(“DHAN_CLIENT_ID”, “”),
“access_token”: os.getenv(“DHAN_ACCESS_TOKEN”, “”),
}

# ─── Dhan Historical Data ─────────────────────────────────────────────────────

def get_historical(security_id: str, exchange: str, client_id: str, access_token: str) -> pd.DataFrame:
“””
Fetch daily OHLCV data from Dhan historical data API
Docs: https://dhanhq.co/docs/v2/historical-data/
“””
today     = datetime.today()
from_date = (today - timedelta(days=20)).strftime(”%Y-%m-%d”)
to_date   = today.strftime(”%Y-%m-%d”)

```
url = f"{DHAN_BASE}/v2/charts/historical"
headers = {
    "access-token": access_token,
    "client-id":    client_id,
    "Content-Type": "application/json",
}
payload = {
    "securityId":   security_id,
    "exchangeSegment": "NSE_EQ",
    "instrument":   "EQUITY",
    "expiryCode":   0,
    "oi_flag":      "0",
    "fromDate":     from_date,
    "toDate":       to_date,
}

resp = requests.post(url, json=payload, headers=headers, timeout=10)
resp.raise_for_status()
data = resp.json()

df = pd.DataFrame({
    "date":   data.get("timestamp", []),
    "open":   data.get("open", []),
    "high":   data.get("high", []),
    "low":    data.get("low", []),
    "close":  data.get("close", []),
    "volume": data.get("volume", []),
})
return df.sort_values("date").reset_index(drop=True)
```

def fetch_screener(client_id: str = None, access_token: str = None):
“”“Main screener logic — runs every 15 seconds”””
global cache

```
cid = client_id or CREDS["client_id"]
tok = access_token or CREDS["access_token"]

if not cid or not tok:
    cache["status"] = "no_credentials"
    return

cache["status"] = "fetching"
results = []

for sym in SYMBOLS:
    try:
        df = get_historical(sym["security_id"], sym["exchange"], cid, tok)

        if len(df) < 8:
            continue

        # ── Volume Ratio: today vs 7-day avg ──────────────────────────
        today_vol  = df["volume"].iloc[-1]
        avg_vol_7d = df["volume"].iloc[-8:-1].mean()
        vol_ratio  = round(today_vol / avg_vol_7d, 2) if avg_vol_7d > 0 else 0

        # ── 5-Day Momentum % ──────────────────────────────────────────
        close_now  = df["close"].iloc[-1]
        close_5ago = df["close"].iloc[-6] if len(df) >= 6 else df["close"].iloc[0]
        momentum5d = round(((close_now - close_5ago) / close_5ago) * 100, 2)

        # ── Day Change % ──────────────────────────────────────────────
        close_prev = df["close"].iloc[-2]
        day_chg    = round(((close_now - close_prev) / close_prev) * 100, 2)

        results.append({
            "symbol":      sym["symbol"],
            "exchange":    sym["exchange"],
            "ltp":         round(close_now, 2),
            "change":      day_chg,
            "momentum5d":  momentum5d,
            "volumeRatio": vol_ratio,
            "todayVol":    int(today_vol),
            "avgVol7d":    int(avg_vol_7d),
        })

    except Exception as e:
        print(f"  ✗ {sym['symbol']}: {e}")

results.sort(key=lambda x: x["volumeRatio"], reverse=True)
cache["data"]       = results
cache["updated_at"] = time.strftime("%H:%M:%S")
cache["status"]     = "ok"
cache["count"]      = len(results)
print(f"✓ Refreshed {cache['updated_at']} — {len(results)} stocks")
```

# ─── Scheduler ───────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()
scheduler.add_job(fetch_screener, “interval”, seconds=15, id=“screener”)
scheduler.start()

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.on_event(“startup”)
async def startup():
if CREDS[“client_id”] and CREDS[“access_token”]:
fetch_screener()

@app.get(”/api/screener”)
def get_screener(
x_client_id:    str = Header(None),
x_access_token: str = Header(None),
):
# Allow per-request credentials (from frontend)
if x_client_id and x_access_token:
fetch_screener(x_client_id, x_access_token)
# Also save for scheduler
CREDS[“client_id”]    = x_client_id
CREDS[“access_token”] = x_access_token
return cache

@app.get(”/api/screener/momentum”)
def momentum(min_pct: float = 3.0):
return {“data”: [s for s in cache[“data”] if s[“momentum5d”] >= min_pct]}

@app.get(”/api/screener/volume-spike”)
def volume_spike(min_ratio: float = 2.0):
return {“data”: [s for s in cache[“data”] if s[“volumeRatio”] >= min_ratio]}

@app.get(”/api/screener/strong”)
def strong(min_mom: float = 5.0, min_vol: float = 2.5):
return {“data”: [s for s in cache[“data”]
if s[“momentum5d”] >= min_mom and s[“volumeRatio”] >= min_vol]}

@app.get(”/api/health”)
def health():
return {
“status”:       “ok”,
“last_refresh”: cache[“updated_at”],
“count”:        cache[“count”],
“scheduler”:    “running”,
}