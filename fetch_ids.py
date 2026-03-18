"""
GitHub Actions script — runs automatically every Sunday at 6 AM IST.
Downloads Dhan scrip master CSV, extracts NSE_EQ security IDs for all
FNO stocks, and saves correct_ids.json to the repo.

Can also be run locally:
    pip install requests
    python fetch_ids.py
"""
import requests
import csv
import json
import sys
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

CSV_URLS = [
    "https://images.dhan.co/api-data/api-scrip-master.csv",
    "https://images.dhan.co/api-data/api-scrip-master-detailed.csv",
]

def find_col(header, *keywords):
    """Return the first column index whose header contains any of the keywords."""
    for i, h in enumerate(header):
        for kw in keywords:
            if kw in h:
                return i
    return None

def parse_csv(text):
    """Parse Dhan scrip master CSV. Returns (eq_lookup, fno_symbols)."""
    eq_lookup   = {}  # symbol -> security_id  (NSE_EQ rows)
    fno_symbols = set()
    reader      = csv.reader(StringIO(text))
    header      = None
    seg_i = sym_i = id_i = inst_i = None
    sample_segs = set()   # collect unique segment values for diagnostics
    row_count   = 0

    for row in reader:
        # ── header row ────────────────────────────────────────────────
        if header is None:
            header = [h.strip().upper() for h in row]
            print(f"  Columns detected: {header[:10]}")
            seg_i  = find_col(header, "SEM_SEGMENT", "EXCH_SEG", "SEGMENT")
            sym_i  = find_col(header, "SEM_TRADING_SYMBOL", "TRADING_SYMBOL", "SYMBOL_NAME", "SM_SYMBOL_NAME")
            id_i   = find_col(header, "SEM_SMST_SECURITY_ID", "SECURITY_ID", "SCRIP_ID", "SM_SYMBOL_ID", "SMST_SECURITY_ID")
            inst_i = find_col(header, "SEM_INSTRUMENT_NAME", "INSTRUMENT", "SEM_INSTRUMENT")
            print(f"  Col indices -> seg={seg_i} sym={sym_i} id={id_i} inst={inst_i}")
            if None in (seg_i, sym_i, id_i):
                print("ERROR: could not find required columns in CSV header.")
                return {}, set()
            continue

        # ── skip short rows ───────────────────────────────────────────
        max_i = max(seg_i, sym_i, id_i)
        if len(row) <= max_i:
            continue

        seg = row[seg_i].strip().upper()
        sym = row[sym_i].strip()
        row_count += 1

        # Collect sample segment values from first 500 rows for diagnostics
        if row_count <= 500:
            sample_segs.add(seg)

        if not sym:
            continue

        # Match NSE equity segment — handle both "NSE_EQ" and "NSE EQ" variants
        is_nse_eq = seg in ("NSE_EQ", "NSE EQ", "NSEEQ") or seg.startswith("NSE") and "EQ" in seg

        if is_nse_eq and sym not in eq_lookup:
            try:
                eq_lookup[sym] = str(int(float(row[id_i].strip())))
            except (ValueError, IndexError):
                pass

        elif ("FNO" in seg or "NSE_FO" in seg) and inst_i is not None and len(row) > inst_i:
            if row[inst_i].strip().upper() in ("FUTSTK", "OPTSTK"):
                fno_symbols.add(sym)

    print(f"  Total rows parsed: {row_count}")
    print(f"  Unique segment values seen (sample): {sorted(sample_segs)}")
    return eq_lookup, fno_symbols

def fetch_ids():
    print("=" * 60)
    print("Dhan Security ID Fetcher")
    print("=" * 60)

    # ── Try each CSV URL ──────────────────────────────────────────────
    text = None
    for url in CSV_URLS:
        print(f"\nTrying: {url}")
        try:
            r = requests.get(url, timeout=60)
            print(f"  HTTP {r.status_code}  Size: {len(r.text):,} bytes")
            if r.status_code == 200 and len(r.text) > 10_000:
                text = r.text
                print("  Download OK")
                break
            else:
                print(f"  Skipping — response too small or non-200")
        except requests.RequestException as e:
            print(f"  Request failed: {e}")

    if text is None:
        print("\nFATAL: Could not download CSV from any URL.")
        print("Check your internet connection or whether Dhan changed the URL.")
        sys.exit(1)

    # ── Parse ─────────────────────────────────────────────────────────
    print("\nParsing CSV...")
    eq_lookup, fno_symbols = parse_csv(text)

    if not eq_lookup:
        print("FATAL: No NSE_EQ entries found. CSV format may have changed.")
        sys.exit(1)

    print(f"\nNSE_EQ symbols found : {len(eq_lookup)}")
    print(f"FNO symbols in CSV   : {len(fno_symbols)}")

    # ── Match FNO names to NSE_EQ IDs ────────────────────────────────
    all_names = (fno_symbols | set(FNO_NAMES)) if fno_symbols else set(FNO_NAMES)
    matched   = {s: eq_lookup[s] for s in sorted(all_names) if s in eq_lookup}
    missing   = [s for s in sorted(all_names) if s not in eq_lookup]

    print(f"Matched              : {len(matched)}")
    if missing:
        print(f"Not found in NSE_EQ  : {missing}")

    if len(matched) < 100:
        print("FATAL: Fewer than 100 symbols matched — something went wrong.")
        sys.exit(1)

    # ── Save JSON ─────────────────────────────────────────────────────
    output_path = "correct_ids.json"
    with open(output_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved {len(matched)} IDs to {output_path}")
    print("Done.")

if __name__ == "__main__":
    fetch_ids()
