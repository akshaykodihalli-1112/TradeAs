"""
GitHub Actions script — runs automatically every Sunday at 6 AM IST.

Strategy:
  1. Fetch active NSE_FNO instruments from Dhan API → get underlying symbols
  2. Fetch NSE_EQ instruments from Dhan API → get security IDs
  3. Match underlying symbols to NSE_EQ security IDs
  4. Save correct_ids.json

Requires env vars: DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID

Can also be run locally:
    pip install requests
    DHAN_ACCESS_TOKEN=xxx DHAN_CLIENT_ID=xxx python fetch_ids.py
"""
import requests
import json
import sys
import os

DHAN_BASE    = "https://api.dhan.co"
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "")

def get_headers():
    return {
        "access-token": ACCESS_TOKEN,
        "client-id":    CLIENT_ID,
        "Content-Type": "application/json",
    }

def fetch_segment(segment):
    """Fetch instrument list for a given exchange segment from Dhan API."""
    print(f"  Fetching {segment}...")
    try:
        r = requests.get(
            f"{DHAN_BASE}/v2/instrument/{segment}",
            headers=get_headers(),
            timeout=60
        )
        print(f"  {segment} → HTTP {r.status_code}, size={len(r.text):,} bytes")
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 401:
            print(f"  ERROR: Unauthorized — check DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID")
            return None
        else:
            print(f"  ERROR: {r.text[:200]}")
            return None
    except requests.RequestException as e:
        print(f"  Request failed: {e}")
        return None

def fetch_ids():
    print("=" * 60)
    print("Dhan Security ID Fetcher (Live API)")
    print("=" * 60)

    if not ACCESS_TOKEN or not CLIENT_ID:
        print("\nFATAL: DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID env vars required.")
        print("Set them in GitHub Secrets or export locally.")
        sys.exit(1)

    print(f"\nCredentials: client_id={'SET' if CLIENT_ID else 'MISSING'} token={'SET' if ACCESS_TOKEN else 'MISSING'}")

    # ── Step 1: Get active NSE_FNO instruments ────────────────────────
    print("\nStep 1: Fetching active NSE_FNO instruments...")
    fno_data = fetch_segment("NSE_FNO")
    if not fno_data:
        print("FATAL: Could not fetch NSE_FNO instruments.")
        sys.exit(1)

    # Log sample to understand structure
    if fno_data:
        print(f"  Sample FNO item keys: {list(fno_data[0].keys())[:10]}")
        print(f"  Sample FNO item: {fno_data[0]}")

    # Extract unique underlying symbols — only FUTSTK/OPTSTK
    fno_underlyings = set()
    for item in fno_data:
        inst = str(item.get("INSTRUMENT", item.get("SEM_INSTRUMENT_NAME", ""))).upper()
        if inst in ("FUTSTK", "OPTSTK"):
            undl = (item.get("SM_SYMBOL_NAME") or
                    item.get("UNDERLYING_SYMBOL") or
                    item.get("SYMBOL_NAME") or "").strip()
            if undl:
                fno_underlyings.add(undl)

    print(f"  Active FNO underlying stocks: {len(fno_underlyings)}")
    if fno_underlyings:
        print(f"  Sample: {sorted(fno_underlyings)[:10]}")

    if len(fno_underlyings) < 10:
        print("WARNING: Very few FNO underlyings. Dumping first 3 items for diagnosis:")
        for item in fno_data[:3]:
            print(f"  {item}")
        sys.exit(1)

    # ── Step 2: Get NSE_EQ instruments for security IDs ──────────────
    print("\nStep 2: Fetching NSE_EQ instruments for security IDs...")
    eq_data = fetch_segment("NSE_EQ")
    if not eq_data:
        print("FATAL: Could not fetch NSE_EQ instruments.")
        sys.exit(1)

    if eq_data:
        print(f"  Sample EQ item keys: {list(eq_data[0].keys())[:10]}")

    # Build symbol → security_id map (series=EQ only)
    eq_lookup = {}
    for item in eq_data:
        series = str(item.get("SERIES", item.get("SEM_SERIES", ""))).upper()
        if series != "EQ":
            continue
        sym = (item.get("SM_SYMBOL_NAME") or
               item.get("SYMBOL_NAME") or
               item.get("SEM_TRADING_SYMBOL") or "").strip()
        sid = (item.get("SEM_SMST_SECURITY_ID") or
               item.get("SECURITY_ID") or
               item.get("SCRIP_ID") or "")
        if sym and sid:
            try:
                eq_lookup[sym] = str(int(float(str(sid))))
            except (ValueError, TypeError):
                pass

    print(f"  NSE_EQ symbols found: {len(eq_lookup)}")

    if len(eq_lookup) < 100:
        print("FATAL: Too few NSE_EQ symbols. Dumping first 3 items for diagnosis:")
        for item in eq_data[:3]:
            print(f"  {item}")
        sys.exit(1)

    # ── Step 3: Match FNO underlyings to NSE_EQ IDs ──────────────────
    print("\nStep 3: Matching...")
    matched = {s: eq_lookup[s] for s in sorted(fno_underlyings) if s in eq_lookup}
    missing = [s for s in sorted(fno_underlyings) if s not in eq_lookup]

    print(f"  Matched : {len(matched)}")
    if missing:
        print(f"  Missing : {missing}")

    if len(matched) < 50:
        print("FATAL: Fewer than 50 symbols matched — something went wrong.")
        sys.exit(1)

    # ── Step 4: Save JSON ─────────────────────────────────────────────
    output_path = "correct_ids.json"
    with open(output_path, "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved {len(matched)} IDs to {output_path}")
    print("Done. ✓")

if __name__ == "__main__":
    fetch_ids()
