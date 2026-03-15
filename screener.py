from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import pandas as pd
import time, os
from datetime import datetime, timedelta

app = FastAPI(title="DhanScreen API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DHAN_BASE = "https://api.dhan.co"

cache = {
    "data": [],
    "updated_at": None,
    "status": "idle",
    "count": 0,
}

CREDS = {
    "client_id": os.getenv("DHAN_CLIENT_ID", ""),
    "access_token": os.getenv("DHAN_ACCESS_TOKEN", ""),
}

# Runtime symbol list — populated at startup from Dhan instrument API
SYMBOLS = []


def fetch_fno_symbols():
    """
    Fetch all NSE FNO-eligible equity stocks from Dhan's instrument list.
    Filters NSE_EQ rows where the symbol also appears in NSE_FNO segment,
    giving us the underlying equities (not the derivatives contracts).
    Falls back to a hardcoded seed list if the API call fails.
    """
    global SYMBOLS
    try:
        # Step 1: get all NSE_EQ instruments
        eq_resp = requests.get(
            f"{DHAN_BASE}/v2/instruments/NSE_EQ",
            timeout=15,
        )
        eq_resp.raise_for_status()

        # Step 2: get all NSE_FNO instruments to extract unique underlying symbols
        fno_resp = requests.get(
            f"{DHAN_BASE}/v2/instruments/NSE_FNO",
            timeout=15,
        )
        fno_resp.raise_for_status()

        # Parse CSVs
        from io import StringIO
        eq_df = pd.read_csv(StringIO(eq_resp.text))
        fno_df = pd.read_csv(StringIO(fno_resp.text))

        # Normalise column names (Dhan uses mixed case)
        eq_df.columns  = [c.strip().upper() for c in eq_df.columns]
        fno_df.columns = [c.strip().upper() for c in fno_df.columns]

        # Identify the correct column names
        # Typical columns: SEM_SMST_SECURITY_ID, SEM_TRADING_SYMBOL, SEM_INSTRUMENT_NAME
        sym_col = next(c for c in fno_df.columns if "TRADING_SYMBOL" in c)
        inst_col = next((c for c in fno_df.columns if "INSTRUMENT" in c), None)

        # Keep only FUTSTK rows to get underlying stock names
        if inst_col:
            fut_df = fno_df[fno_df[inst_col].str.upper() == "FUTSTK"]
        else:
            fut_df = fno_df

        fno_symbols = set(fut_df[sym_col].str.strip().unique())

        # Match to NSE_EQ rows
        eq_sym_col = next(c for c in eq_df.columns if "TRADING_SYMBOL" in c)
        eq_id_col  = next(c for c in eq_df.columns if "SECURITY_ID" in c)

        matched = eq_df[eq_df[eq_sym_col].str.strip().isin(fno_symbols)][
            [eq_id_col, eq_sym_col]
        ].drop_duplicates(subset=[eq_sym_col])

        SYMBOLS = [
            {"symbol": row[eq_sym_col].strip(), "security_id": str(row[eq_id_col]), "exchange": "NSE"}
            for _, row in matched.iterrows()
        ]

        print(f"Loaded {len(SYMBOLS)} FNO stocks from Dhan instrument API.")

    except Exception as e:
        print(f"Warning: Could not fetch FNO instrument list ({e}). Using seed list.")
        # Seed list — verified security IDs for top FNO stocks
        SYMBOLS = [
            {"symbol": "RELIANCE",    "security_id": "2885",  "exchange": "NSE"},
            {"symbol": "TCS",         "security_id": "11536", "exchange": "NSE"},
            {"symbol": "HDFCBANK",    "security_id": "1333",  "exchange": "NSE"},
            {"symbol": "INFY",        "security_id": "1594",  "exchange": "NSE"},
            {"symbol": "ICICIBANK",   "security_id": "4963",  "exchange": "NSE"},
            {"symbol": "WIPRO",       "security_id": "3787",  "exchange": "NSE"},
            {"symbol": "AXISBANK",    "security_id": "5900",  "exchange": "NSE"},
            {"symbol": "SBIN",        "security_id": "3045",  "exchange": "NSE"},
            {"symbol": "TATAMOTORS",  "security_id": "3456",  "exchange": "NSE"},
            {"symbol": "TATASTEEL",   "security_id": "3499",  "exchange": "NSE"},
            {"symbol": "ADANIENT",    "security_id": "25",    "exchange": "NSE"},
            {"symbol": "BAJFINANCE",  "security_id": "317",   "exchange": "NSE"},
            {"symbol": "MARUTI",      "security_id": "10999", "exchange": "NSE"},
            {"symbol": "SUNPHARMA",   "security_id": "3351",  "exchange": "NSE"},
            {"symbol": "ONGC",        "security_id": "2475",  "exchange": "NSE"},
            {"symbol": "NTPC",        "security_id": "11630", "exchange": "NSE"},
            {"symbol": "POWERGRID",   "security_id": "14977", "exchange": "NSE"},
            {"symbol": "COALINDIA",   "security_id": "20374", "exchange": "NSE"},
            {"symbol": "HINDALCO",    "security_id": "1306",  "exchange": "NSE"},
            {"symbol": "JSWSTEEL",    "security_id": "11723", "exchange": "NSE"},
            {"symbol": "BHARTIARTL", "security_id": "10604", "exchange": "NSE"},
            {"symbol": "KOTAKBANK",  "security_id": "1232",  "exchange": "NSE"},
            {"symbol": "LT",          "security_id": "11483", "exchange": "NSE"},
            {"symbol": "TECHM",       "security_id": "13538", "exchange": "NSE"},
            {"symbol": "HCLTECH",     "security_id": "7229",  "exchange": "NSE"},
            {"symbol": "ITC",         "security_id": "1660",  "exchange": "NSE"},
            {"symbol": "BAJAJFINSV", "security_id": "16675", "exchange": "NSE"},
            {"symbol": "ASIANPAINT", "security_id": "467",   "exchange": "NSE"},
            {"symbol": "TITAN",       "security_id": "3506",  "exchange": "NSE"},
            {"symbol": "ULTRACEMCO", "security_id": "11532", "exchange": "NSE"},
            {"symbol": "NESTLEIND",  "security_id": "4306",  "exchange": "NSE"},
            {"symbol": "DRREDDY",    "security_id": "881",   "exchange": "NSE"},
            {"symbol": "CIPLA",       "security_id": "694",   "exchange": "NSE"},
            {"symbol": "DIVISLAB",   "security_id": "15174", "exchange": "NSE"},
            {"symbol": "APOLLOHOSP", "security_id": "157",   "exchange": "NSE"},
            {"symbol": "TATACONSUM", "security_id": "3432",  "exchange": "NSE"},
            {"symbol": "INDUSINDBK", "security_id": "5258",  "exchange": "NSE"},
            {"symbol": "GRASIM",     "security_id": "1232",  "exchange": "NSE"},
            {"symbol": "EICHERMOT",  "security_id": "910",   "exchange": "NSE"},
            {"symbol": "HEROMOTOCO","security_id": "1348",  "exchange": "NSE"},
            {"symbol": "BPCL",       "security_id": "526",   "exchange": "NSE"},
            {"symbol": "ADANIPORTS", "security_id": "15083", "exchange": "NSE"},
            {"symbol": "BAJAJ-AUTO", "security_id": "16669", "exchange": "NSE"},
            {"symbol": "M&M",        "security_id": "2031",  "exchange": "NSE"},
            {"symbol": "HDFCLIFE",   "security_id": "119",   "exchange": "NSE"},
            {"symbol": "SBILIFE",    "security_id": "21808", "exchange": "NSE"},
            {"symbol": "ICICIGI",    "security_id": "21770", "exchange": "NSE"},
            {"symbol": "PIDILITIND", "security_id": "2664",  "exchange": "NSE"},
            {"symbol": "SIEMENS",    "security_id": "3150",  "exchange": "NSE"},
            {"symbol": "HAVELLS",    "security_id": "8927",  "exchange": "NSE"},
        ]


def get_historical(security_id, exchange, client_id, access_token):
    today = datetime.today()
    from_date = (today - timedelta(days=20)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    url = f"{DHAN_BASE}/v2/charts/historical"
    headers = {
        "access-token": access_token,
        "client-id": client_id,
        "Content-Type": "application/json",
    }
    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "expiryCode": 0,
        "oi_flag": "0",
        "fromDate": from_date,
        "toDate": to_date,
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


def fetch_screener(client_id=None, access_token=None):
    global cache

    cid = client_id or CREDS["client_id"]
    tok = access_token or CREDS["access_token"]

    if not cid or not tok:
        cache["status"] = "no_credentials"
        return

    if not SYMBOLS:
        fetch_fno_symbols()

    cache["status"] = "fetching"
    results = []

    for sym in SYMBOLS:
        try:
            df = get_historical(sym["security_id"], sym["exchange"], cid, tok)

            if len(df) < 8:
                continue

            today_vol  = df["volume"].iloc[-1]
            avg_vol_7d = df["volume"].iloc[-8:-1].mean()
            vol_ratio  = round(today_vol / avg_vol_7d, 2) if avg_vol_7d > 0 else 0

            close_now  = df["close"].iloc[-1]
            close_5ago = df["close"].iloc[-6] if len(df) >= 6 else df["close"].iloc[0]
            momentum5d = round(((close_now - close_5ago) / close_5ago) * 100, 2)

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
            print(f"Error {sym['symbol']}: {e}")

        time.sleep(0.2)  # ~5 req/sec — stay within Dhan rate limits

    results.sort(key=lambda x: x["volumeRatio"], reverse=True)
    cache["data"]       = results
    cache["updated_at"] = time.strftime("%H:%M:%S")
    cache["status"]     = "ok"
    cache["count"]      = len(results)
    print(f"Refreshed {cache['updated_at']} - {len(results)} stocks")


scheduler = BackgroundScheduler()
scheduler.add_job(fetch_screener, "interval", seconds=300, id="screener")
scheduler.start()


@app.on_event("startup")
async def startup():
    fetch_fno_symbols()
    if CREDS["client_id"] and CREDS["access_token"]:
        fetch_screener()


@app.get("/api/screener")
def get_screener(
    x_client_id: str = Header(None),
    x_access_token: str = Header(None),
):
    if x_client_id and x_access_token:
        fetch_screener(x_client_id, x_access_token)
        CREDS["client_id"]    = x_client_id
        CREDS["access_token"] = x_access_token
    return cache


@app.get("/api/screener/momentum")
def momentum(min_pct: float = 3.0):
    return {"data": [s for s in cache["data"] if s["momentum5d"] >= min_pct]}


@app.get("/api/screener/volume-spike")
def volume_spike(min_ratio: float = 2.0):
    return {"data": [s for s in cache["data"] if s["volumeRatio"] >= min_ratio]}


@app.get("/api/screener/strong")
def strong(min_mom: float = 5.0, min_vol: float = 2.5):
    return {"data": [s for s in cache["data"]
                     if s["momentum5d"] >= min_mom and s["volumeRatio"] >= min_vol]}


@app.get("/api/symbols")
def list_symbols():
    return {"count": len(SYMBOLS), "symbols": SYMBOLS}


@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "last_refresh": cache["updated_at"],
        "symbol_count": len(SYMBOLS),
        "result_count": cache["count"],
    }
