"""
Fetches correct Dhan NSE_EQ security IDs from Dhan scrip master CSV.
Saves output to correct_ids.json.
Run via GitHub Actions or locally.
"""
import requests, csv, json, sys
from io import StringIO

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

def fetch_ids():
    urls = [
        "https://images.dhan.co/api-data/api-scrip-master.csv",
        "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
    ]
    eq_lookup = {}
    fno_symbols = set()
    success_url = None

    for url in urls:
        try:
            print(f"Trying {url.split('/')[-1]} ...", flush=True)
            r = requests.get(url, timeout=60)
            print(f"  status={r.status_code}  size={len(r.text):,} bytes", flush=True)
            if r.status_code != 200 or len(r.text) < 10000:
                print("  Too small or error — trying next", flush=True)
                continue

            reader = csv.reader(StringIO(r.text))
            header = None
            seg_i = sym_i = id_i = inst_i = None

            for row in reader:
                if header is None:
                    header = [h.strip().upper() for h in row]
                    fi = lambda *kws: next((i for i,h in enumerate(header) if any(k in h for k in kws)), None)
                    seg_i  = fi("EXCH_SEG","SEGMENT")
                    sym_i  = fi("TRADING_SYMBOL","SYMBOL_NAME","SM_SYMBOL_NAME")
                    id_i   = fi("SECURITY_ID","SCRIP_ID","SM_SYMBOL_ID","SMST_SECURITY_ID")
                    inst_i = fi("INSTRUMENT","SEM_INSTRUMENT")
                    print(f"  Columns: seg={seg_i} sym={sym_i} id={id_i} inst={inst_i}", flush=True)
                    if None in (seg_i, sym_i, id_i):
                        print(f"  Missing columns: {header[:10]}", flush=True); break
                    continue
                if len(row) <= max(seg_i, sym_i, id_i): continue
                seg = row[seg_i].strip().upper()
                sym = row[sym_i].strip()
                if not sym: continue
                if seg == "NSE_EQ" and sym not in eq_lookup:
                    try: eq_lookup[sym] = str(int(float(row[id_i].strip())))
                    except: pass
                elif seg == "NSE_FNO" and inst_i and row[inst_i].strip().upper() in ("FUTSTK","OPTSTK"):
                    fno_symbols.add(sym)

            if len(eq_lookup) > 100:
                success_url = url; break
        except Exception as e:
            print(f"  Error: {e}", flush=True)

    if not eq_lookup:
        print("FAILED: Could not load CSV", flush=True); sys.exit(1)

    print(f"NSE_EQ: {len(eq_lookup)}  FNO from CSV: {len(fno_symbols)}", flush=True)

    all_names = (fno_symbols | set(FNO_NAMES)) if fno_symbols else set(FNO_NAMES)
    matched   = {s: eq_lookup[s] for s in sorted(all_names) if s in eq_lookup}
    missing   = sorted(s for s in all_names if s not in eq_lookup)

    print(f"Matched: {len(matched)}  Missing: {missing}", flush=True)

    with open("correct_ids.json","w") as f:
        json.dump(matched, f, indent=2, sort_keys=True)
    print(f"Saved {len(matched)} IDs to correct_ids.json  (source: {success_url})", flush=True)

    # Spot check
    for sym, exp in [("HDFCBANK","1333"),("TCS","11536"),("ZOMATO","23652")]:
        got = matched.get(sym,"MISSING")
        print(f"  {sym}: {got} {'OK' if got==exp else 'WRONG expected '+exp}", flush=True)

if __name__ == "__main__":
    fetch_ids()
