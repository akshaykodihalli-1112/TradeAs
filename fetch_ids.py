"""
GitHub Actions script — runs automatically every Sunday at 6 AM IST.
Fetches active FNO list directly from Dhan API (CSV format).
Requires: DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID env vars (GitHub Secrets)
"""
import requests
import json
import csv
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
    }

def fetch_segment_csv(segment):
    """Fetch instrument CSV for a segment from Dhan API."""
    print(f"  Fetching {segment}...")
    r = requests.get(f"{DHAN_BASE}/v2/instrument/{segment}",
                     headers=get_headers(), timeout=60)
    print(f"  {segment} → HTTP {r.status_code}, size={len(r.text):,} bytes")
    if r.status_code != 200:
        print(f"  ERROR: {r.text[:200]}")
        return None
    # Log first line to see format
    first_line = r.text.split('\n')[0]
    print(f"  First line: {first_line[:200]}")
    return r.text

def parse_instruments(text, want_fno=False, want_eq=False):
    """Parse Dhan instrument CSV. Returns dict based on want_fno or want_eq."""
    reader = csv.reader(StringIO(text))
    header = None
    results_fno = set()   # underlying symbols with active FNO
    results_eq  = {}      # symbol -> security_id

    for row in reader:
        if header is None:
            header = [h.strip().upper() for h in row]
            print(f"  Columns: {header[:12]}")
            continue
        if len(row) < 3: continue

        # Build row dict
        d = {header[i]: row[i].strip() for i in range(min(len(header), len(row)))}

        exch   = d.get("SEM_EXM_EXCH_ID", d.get("EXCH_ID", "")).upper()
        seg    = d.get("SEM_SEGMENT", d.get("SEGMENT", "")).upper()
        inst   = d.get("SEM_INSTRUMENT_NAME", d.get("INSTRUMENT", "")).upper()
        series = d.get("SEM_SERIES", d.get("SERIES", "")).upper()
        sym    = d.get("SEM_TRADING_SYMBOL", d.get("SYMBOL_NAME", "")).strip()
        undl   = d.get("SM_SYMBOL_NAME", d.get("UNDERLYING_SYMBOL", d.get("SYMBOL_NAME", ""))).strip()
        sid    = d.get("SEM_SMST_SECURITY_ID", d.get("SECURITY_ID", "")).strip()

        if want_fno and inst in ("FUTSTK", "OPTSTK") and undl:
            results_fno.add(undl)

        if want_eq and series == "EQ" and sym and sid:
            try:
                results_eq[sym] = str(int(float(sid)))
            except (ValueError, TypeError):
                pass

    return results_fno, results_eq

def fetch_ids():
    print("=" * 60)
    print("Dhan Security ID Fetcher (Live API)")
    print("=" * 60)

    if not ACCESS_TOKEN or not CLIENT_ID:
        print("\nFATAL: DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID env vars required.")
        sys.exit(1)

    print(f"\nCredentials: client_id={'SET' if CLIENT_ID else 'MISSING'} token={'SET' if ACCESS_TOKEN else 'MISSING'}")

    # ── Step 1: Active FNO underlyings ────────────────────────────────
    print("\nStep 1: Fetching active NSE_FNO instruments...")
    fno_text = fetch_segment_csv("NSE_FNO")
    if not fno_text:
        print("FATAL: Could not fetch NSE_FNO.")
        sys.exit(1)

    fno_underlyings, _ = parse_instruments(fno_text, want_fno=True)
    print(f"  Active FNO underlyings: {len(fno_underlyings)}")
    print(f"  Sample: {sorted(fno_underlyings)[:10]}")

    if len(fno_underlyings) < 10:
        print("FATAL: Too few FNO underlyings found.")
        sys.exit(1)

    # ── Step 2: NSE_EQ security IDs ──────────────────────────────────
    print("\nStep 2: Fetching NSE_EQ instruments...")
    eq_text = fetch_segment_csv("NSE_EQ")
    if not eq_text:
        print("FATAL: Could not fetch NSE_EQ.")
        sys.exit(1)

    _, eq_lookup = parse_instruments(eq_text, want_eq=True)
    print(f"  NSE_EQ symbols: {len(eq_lookup)}")

    if len(eq_lookup) < 100:
        print("FATAL: Too few NSE_EQ symbols.")
        sys.exit(1)

    # ── Step 3: Match ─────────────────────────────────────────────────
    print("\nStep 3: Matching FNO underlyings to NSE_EQ IDs...")
    matched = {s: eq_lookup[s] for s in sorted(fno_underlyings) if s in eq_lookup}
    missing = [s for s in sorted(fno_underlyings) if s not in eq_lookup]

    print(f"  Matched : {len(matched)}")
    if missing:
        print(f"  Missing : {missing[:20]}")

    if len(matched) < 50:
        print("FATAL: Fewer than 50 symbols matched.")
        sys.exit(1)

    # ── Step 4: Save ──────────────────────────────────────────────────
    with open("correct_ids.json", "w") as f:
        json.dump(matched, f, indent=2)
    print(f"\nSaved {len(matched)} IDs to correct_ids.json")
    print("Done. ✓")

if __name__ == "__main__":
    fetch_ids()
