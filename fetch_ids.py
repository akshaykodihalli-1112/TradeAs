"""
GitHub Actions script — runs automatically every Sunday at 6 AM IST.
Fetches active FNO list directly from Dhan API (CSV format).
Requires: DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID env vars (GitHub Secrets)
"""
import requests
import csv
import json
import sys
import os
from io import StringIO

DHAN_BASE    = "https://api.dhan.co"
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")
CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "")

def get_headers():
    return {
        "access-token": ACCESS_TOKEN,
        "client-id":    CLIENT_ID,
        "Content-Type": "application/json",
    }

def fetch_csv_segment(segment):
    """Fetch CSV instrument list for a segment. Returns list of dicts."""
    print(f"  Fetching {segment}...")
    try:
        r = requests.get(
            f"{DHAN_BASE}/v2/instrument/{segment}",
            headers=get_headers(),
            timeout=60
        )
        print(f"  {segment} → HTTP {r.status_code}, size={len(r.text):,} bytes")
        if r.status_code != 200:
            print(f"  ERROR: {r.text[:200]}")
            return None
        # Parse CSV
        reader = csv.DictReader(StringIO(r.text))
        rows = list(reader)
        if rows:
            print(f"  Columns: {list(rows[0].keys())[:12]}")
        return rows
    except Exception as e:
        print(f"  Failed: {e}")
        return None

def fetch_ids():
    print("=" * 60)
    print("Dhan Security ID Fetcher (Live API)")
    print("=" * 60)

    if not ACCESS_TOKEN or not CLIENT_ID:
        print("\nFATAL: Set DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID in GitHub Secrets.")
        sys.exit(1)

    print(f"\nCredentials: client_id={'SET' if CLIENT_ID else 'MISSING'} token={'SET' if ACCESS_TOKEN else 'MISSING'}")

    # ── Step 1: Active FNO underlyings ────────────────────────────────
    print("\nStep 1: Fetching active NSE_FNO instruments...")
    fno_rows = fetch_csv_segment("NSE_FNO")
    if not fno_rows:
        print("FATAL: Could not fetch NSE_FNO."); sys.exit(1)

    # Print first row to understand column values
    print(f"  Sample row: {dict(list(fno_rows[0].items())[:8])}")

    # Extract unique underlyings from FUTSTK contracts only
    # UNDERLYING_SYMBOL column has the stock name e.g. "ANGELONE"
    fno_underlyings = set()
    for row in fno_rows:
        inst = row.get("INSTRUMENT", "").strip().upper()
        if inst == "FUTSTK":
            undl = row.get("UNDERLYING_SYMBOL", "").strip()
            if undl and not undl[0].isdigit():  # skip test symbols like 011NSETEST, 101NSETEST
                fno_underlyings.add(undl)

    print(f"  Active FNO stocks (FUTSTK only): {len(fno_underlyings)}")
    print(f"  Sample: {sorted(fno_underlyings)[:15]}")

    if len(fno_underlyings) < 50:
        print("FATAL: Too few underlyings found."); sys.exit(1)

    # ── Step 2: NSE_EQ security IDs ───────────────────────────────────
    print("\nStep 2: Fetching NSE_EQ instruments...")
    eq_rows = fetch_csv_segment("NSE_EQ")
    if not eq_rows:
        print("FATAL: Could not fetch NSE_EQ."); sys.exit(1)

    print(f"  Sample row: {dict(list(eq_rows[0].items())[:8])}")

    # Build UNDERLYING_SYMBOL → SECURITY_ID map for EQ series only
    # UNDERLYING_SYMBOL = trading symbol e.g. "ANGELONE", "RELIANCE"
    # SYMBOL_NAME = full company name e.g. "Angel One Limited"
    eq_lookup = {}
    for row in eq_rows:
        series = row.get("SERIES", "").strip().upper()
        if series != "EQ":
            continue
        sym = row.get("UNDERLYING_SYMBOL", "").strip()  # trading symbol
        sid = row.get("SECURITY_ID", "").strip()
        if sym and sid:
            try:
                eq_lookup[sym] = str(int(float(sid)))
            except (ValueError, TypeError):
                pass

    print(f"  NSE_EQ EQ-series symbols: {len(eq_lookup)}")
    print(f"  Sample: {list(eq_lookup.items())[:5]}")

    # ── Step 3: Match ─────────────────────────────────────────────────
    print("\nStep 3: Matching FNO underlyings to NSE_EQ IDs...")
    matched = {s: eq_lookup[s] for s in sorted(fno_underlyings) if s in eq_lookup}
    missing = [s for s in sorted(fno_underlyings) if s not in eq_lookup]

    print(f"  Matched : {len(matched)}")
    if missing:
        print(f"  Missing : {missing[:20]}")

    if len(matched) < 50:
        print("FATAL: Too few matched. Check column names.")
        sys.exit(1)

    # ── Step 4: Save ──────────────────────────────────────────────────
    with open("correct_ids.json", "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved {len(matched)} IDs to correct_ids.json")
    print("Done. ✓")

if __name__ == "__main__":
    fetch_ids()
