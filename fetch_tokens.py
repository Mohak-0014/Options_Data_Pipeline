"""
Fetch Real Instrument Tokens from Kotak Neo Scrip Master

Authenticates, downloads the NSE CM scrip master, matches our target symbols,
and generates an updated instruments.py file with real tokens.

Usage (activate venv first):
    source .venv/Scripts/activate
    python fetch_tokens.py
"""

import sys
import os
import csv
import io

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import (
    KOTAK_CONSUMER_KEY,
    KOTAK_MOBILE,
    KOTAK_MPIN,
    KOTAK_UCC,
    TOTP_SECRET,
)

import pyotp
from neo_api_client import NeoAPI


# All symbols we want tokens for — indexes + stocks
TARGET_SYMBOLS = [
    # Indexes
    "NIFTY", "BANKNIFTY", "FINNIFTY",
    # NIFTY 50
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BHARTIARTL", "BPCL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
    "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
    "LTIM", "M&M", "MARUTI", "NESTLEIND", "NTPC",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN",
    "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "ULTRACEMCO", "UPL", "WIPRO",
    # NIFTY NEXT 50 / F&O active
    "ABBOTINDIA", "ACC", "AMBUJACEM", "AUROPHARMA", "BANDHANBNK",
    "BANKBARODA", "BEL", "BERGEPAINT", "BIOCON", "BOSCHLTD",
    "CANBK", "CHOLAFIN", "COLPAL", "CONCOR", "COROMANDEL",
    "CROMPTON", "CUB", "DABUR", "DALBHARAT", "DEEPAKNTR",
    "DLF", "ESCORTS", "EXIDEIND", "FEDERALBNK", "GAIL",
    "GODREJCP", "GODREJPROP", "GRANULES", "GUJGASLTD", "HAL",
    "HAVELLS", "HINDPETRO", "IBULHSGFIN", "IDFCFIRSTB", "IEX",
    "IGL", "INDHOTEL", "INDIGO", "IOC", "IRCTC",
    "IRFC", "JIOFIN", "JUBLFOOD", "LICI", "LUPIN",
    "MANAPPURAM", "MARICO", "MCDOWELL-N", "MCX", "METROPOLIS",
    "MFSL", "MGL", "MOTHERSON", "MPHASIS", "MUTHOOTFIN",
    "NAM-INDIA", "NATIONALUM", "NAUKRI", "NAVINFLUOR", "NMDC",
    "OBEROIRLTY", "OFSS", "PAGEIND", "PEL", "PERSISTENT",
    "PETRONET", "PFC", "PIDILITIND", "PIIND", "PNB",
    "POLYCAB", "PVRINOX", "RAMCOCEM", "RBLBANK", "RECLTD",
    "SAIL", "SHREECEM", "SHRIRAMFIN", "SIEMENS", "SRF",
    "STAR", "SUNTV", "SYNGENE", "TATACOMM", "TATAELXSI",
    "TATAPOWER", "TORNTPHARM", "TORNTPOWER", "TRENT", "TVSMOTOR",
    "UNIONBANK", "UNITDSPR", "VEDL", "VOLTAS", "WHIRLPOOL",
    "ZEEL", "ZYDUSLIFE",
    # Additional high-volume F&O stocks (to fill up to 178)
    "ABCAPITAL", "ABFRL", "ALKEM", "ATUL", "AUBANK",
    "ASTRAZEN", "BALRAMCHIN", "BATAINDIA", "BHEL", "CANFINHOME",
    "CHAMBLFERT", "COFORGE", "CUMMINSIND", "DELTACORP", "DIXON",
    "GNFC", "GSPL", "GLENMARK", "GMRINFRA", "IPCALAB",
    "INTELLECT", "INDIACEM", "INDUSTOWER", "JINDALSTEL", "LICHSGFIN",
    "L&TFH", "LAURUSLABS", "LALPATHLAB", "MRF",
]


def authenticate():
    """Login to Kotak Neo and return client."""
    print("Authenticating...")
    totp = pyotp.TOTP(TOTP_SECRET)

    client = NeoAPI(
        environment="prod",
        access_token=None,
        neo_fin_key=None,
        consumer_key=KOTAK_CONSUMER_KEY,
    )

    client.totp_login(
        mobile_number=KOTAK_MOBILE,
        ucc=KOTAK_UCC,
        totp=totp.now(),
    )

    client.totp_validate(mpin=KOTAK_MPIN)
    print("  OK — authenticated")
    return client


def fetch_scrip_master(client):
    """Download NSE CM scrip master and parse it."""
    print("Fetching scrip master (nse_cm)...")
    result = client.scrip_master(exchange_segment="nse_cm")

    # The result could be CSV text or list of dicts depending on SDK version
    if isinstance(result, str):
        print(f"  Got CSV string ({len(result)} chars)")
        return result
    elif isinstance(result, list):
        print(f"  Got list of {len(result)} records")
        return result
    elif isinstance(result, dict):
        # Some versions return {'data': [...]} or {'data': 'csv_string'}
        data = result.get("data", result)
        if isinstance(data, str):
            print(f"  Got CSV string in data ({len(data)} chars)")
            return data
        elif isinstance(data, list):
            print(f"  Got list of {len(data)} records in data")
            return data
        else:
            print(f"  Unexpected data type: {type(data)}")
            print(f"  Keys: {result.keys() if isinstance(result, dict) else 'N/A'}")
            print(f"  Sample: {str(result)[:500]}")
            return result
    else:
        print(f"  Unexpected result type: {type(result)}")
        print(f"  Sample: {str(result)[:500]}")
        return result


def match_tokens(scrip_data, target_symbols):
    """
    Match target symbols against scrip master data.
    
    Returns dict: {symbol: token}
    """
    matched = {}
    unmatched = set(target_symbols)

    if isinstance(scrip_data, str):
        # Parse CSV
        reader = csv.DictReader(io.StringIO(scrip_data))
        rows = list(reader)
        print(f"  Parsed {len(rows)} rows from CSV")
        if rows:
            print(f"  Columns: {list(rows[0].keys())}")
    elif isinstance(scrip_data, list) and scrip_data and isinstance(scrip_data[0], dict):
        rows = scrip_data
        if rows:
            print(f"  Columns: {list(rows[0].keys())}")
    else:
        print(f"  Cannot parse scrip_data of type {type(scrip_data)}")
        # Try to dump raw for debugging
        with open("data/scrip_master_raw.txt", "w") as f:
            f.write(str(scrip_data)[:50000])
        print(f"  Dumped raw data to data/scrip_master_raw.txt for inspection")
        return matched, unmatched

    # Try common column names for symbol and token
    symbol_cols = ["pSymbol", "pSymbolName", "symbol", "Symbol", "trading_symbol",
                   "TradingSymbol", "pTradingSymbol", "scrip_name", "pScripName"]
    token_cols = ["pSymbol", "token", "Token", "instrument_token", "InstrumentToken",
                  "pToken", "scrip_token", "ScripToken", "pScripToken"]

    # Find the right column names
    if not rows:
        return matched, unmatched

    sample = rows[0]
    sym_col = None
    tok_col = None

    for col in symbol_cols:
        if col in sample:
            sym_col = col
            break

    for col in token_cols:
        if col in sample and col != sym_col:
            tok_col = col
            break

    if not sym_col or not tok_col:
        print(f"  Could not identify symbol/token columns!")
        print(f"  Available columns: {list(sample.keys())}")
        print(f"  First row sample: {sample}")
        # Dump all columns for debugging
        with open("data/scrip_master_columns.txt", "w") as f:
            f.write(f"Columns: {list(sample.keys())}\n\n")
            for i, row in enumerate(rows[:5]):
                f.write(f"Row {i}: {row}\n\n")
        print(f"  Dumped sample rows to data/scrip_master_columns.txt")
        return matched, unmatched

    print(f"  Using columns: symbol='{sym_col}', token='{tok_col}'")

    # Build lookup: symbol → token (take first match)
    target_set = set(s.upper() for s in target_symbols)
    symbol_to_target = {s.upper(): s for s in target_symbols}

    for row in rows:
        sym = str(row.get(sym_col, "")).strip().upper()
        tok = str(row.get(tok_col, "")).strip()

        if sym in target_set and sym not in matched:
            original_sym = symbol_to_target.get(sym, sym)
            matched[original_sym] = tok
            unmatched.discard(original_sym)

    return matched, unmatched


def generate_instruments_file(matched, unmatched):
    """Generate the updated instruments.py file."""
    output_path = "config/instruments_live.py"

    lines = [
        '"""',
        'Instrument Definitions — Live Tokens from Kotak Neo Scrip Master',
        '',
        'Auto-generated by fetch_tokens.py.',
        'These are REAL instrument tokens fetched from the Kotak Neo scrip master.',
        '"""',
        '',
        'from dataclasses import dataclass',
        'from typing import List',
        '',
        '',
        '@dataclass(frozen=True)',
        'class Instrument:',
        '    """Immutable instrument definition."""',
        '    symbol: str',
        '    token: str',
        '    segment: str',
        '',
        '',
        'INSTRUMENTS: List[Instrument] = [',
    ]

    # Sort: indexes first, then alphabetical
    indexes = ["NIFTY", "BANKNIFTY", "FINNIFTY"]
    index_entries = [(s, matched[s]) for s in indexes if s in matched]
    stock_entries = sorted([(s, t) for s, t in matched.items() if s not in indexes])

    if index_entries:
        lines.append('    # --- Indexes ---')
        for sym, tok in index_entries:
            lines.append(f'    Instrument(symbol="{sym}", token="{tok}", segment="nse_cm"),')
        lines.append('')

    lines.append('    # --- Stocks (alphabetical) ---')
    for sym, tok in stock_entries:
        lines.append(f'    Instrument(symbol="{sym}", token="{tok}", segment="nse_cm"),')

    lines.append(']')
    lines.append('')
    lines.append('')
    lines.append('# Lookup helpers')
    lines.append('INSTRUMENT_BY_SYMBOL = {inst.symbol: inst for inst in INSTRUMENTS}')
    lines.append('INSTRUMENT_BY_TOKEN = {inst.token: inst for inst in INSTRUMENTS}')
    lines.append('')
    lines.append('')
    lines.append('def get_all_symbols() -> list[str]:')
    lines.append('    """Return list of all ticker symbols."""')
    lines.append('    return [inst.symbol for inst in INSTRUMENTS]')
    lines.append('')
    lines.append('')
    lines.append('def get_instrument_count() -> int:')
    lines.append('    """Return total number of registered instruments."""')
    lines.append('    return len(INSTRUMENTS)')
    lines.append('')

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"\nGenerated {output_path} with {len(matched)} instruments")
    return output_path


def main():
    print("=" * 55)
    print("  FETCH INSTRUMENT TOKENS")
    print("=" * 55)

    # Step 1: Auth
    client = authenticate()

    # Step 2: Fetch scrip master
    scrip_data = fetch_scrip_master(client)

    # Step 3: Match tokens
    print("\nMatching target symbols...")
    matched, unmatched = match_tokens(scrip_data, TARGET_SYMBOLS)

    print(f"\n  Matched:   {len(matched)} / {len(TARGET_SYMBOLS)}")
    print(f"  Unmatched: {len(unmatched)}")

    if unmatched:
        print(f"\n  Unmatched symbols:")
        for s in sorted(unmatched):
            print(f"    - {s}")

    if matched:
        # Step 4: Generate file
        path = generate_instruments_file(matched, unmatched)
        print(f"\n  Next step: review {path}, then copy to config/instruments.py")
    else:
        print("\n  No matches found — check data/scrip_master_raw.txt for debugging")

    # Step 5: Summary
    print(f"\n{'=' * 55}")
    if len(matched) >= 150:
        print("  PASS — sufficient instruments matched")
    else:
        print("  PARTIAL — review unmatched symbols and scrip master format")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
