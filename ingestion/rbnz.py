import os
import glob
import pandas as pd
from datetime import datetime

"""
rbnz.py
-------
Ingests residential mortgage interest rate data published by the
Reserve Bank of New Zealand (RBNZ) across three series:

    B21 — Special (discounted) rates by fixed term
    B20 — Standard (carded/advertised) rates by fixed term
    B30 — Weighted average rates on new mortgage lending

These three series together paint a complete picture of borrowing costs
in the NZ housing market and are the primary input for the affordability
index in the downstream dbt models.

Raw files are downloaded manually from the RBNZ statistics portal:
    https://www.rbnz.govt.nz/statistics/series/exchange-and-interest-rates

Once saved to data/raw/rbnz/, this script:
    1. Locates each file (with fuzzy matching in case of renamed files)
    2. Detects the correct header row (RBNZ files contain metadata rows on top)
    3. Parses and validates date values
    4. Melts from wide format (one column per term) to long format
       (one row per date + term combination) for efficient warehouse loading

Output schema: date | term | rate_pct | series
"""

RAW_DIR = "data/raw/rbnz"

SOURCES = {
    "mortgage_special_rates":  "hb21-monthly.xlsx",
    "mortgage_standard_rates": "hb20-monthly.xlsx",
    "mortgage_weighted_avg":   "hb30-monthly.xlsx",
}


def find_file(filename: str) -> str | None:
    """Find the file in RAW_DIR — handles if user renamed it slightly."""
    exact = os.path.join(RAW_DIR, filename)
    if os.path.exists(exact):
        return exact

    # fuzzy match — look for any xlsx with similar name
    stem = filename.replace(".xlsx", "").replace("-monthly", "")
    matches = glob.glob(os.path.join(RAW_DIR, f"*{stem}*.xlsx"))
    if matches:
        print(f"  Found alternative file: {matches[0]}")
        return matches[0]

    return None


def find_header_row(df: pd.DataFrame) -> int:
    """Scan rows to find where 'Date' header is — RBNZ files have metadata rows on top."""
    for i, row in df.iterrows():
        if any(str(v).strip().lower() == "date" for v in row):
            return i
    return 4  # fallback


def read_and_clean(name: str, filepath: str) -> pd.DataFrame | None:
    """Read RBNZ XLSX, find correct header row, melt to long format."""
    xl = pd.ExcelFile(filepath)
    print(f"  [{name}] Sheets: {xl.sheet_names}")

    # RBNZ puts actual data in 'Data' sheet — always use it if available
    sheet = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
    print(f"  [{name}] Reading sheet: '{sheet}'")

    # read without header first to find the right row
    raw = pd.read_excel(filepath, sheet_name=sheet, header=None)
    header_row = find_header_row(raw)
    print(f"  [{name}] Header row: {header_row}")

    # re-read with correct header
    df = pd.read_excel(filepath, sheet_name=sheet, header=header_row)
        # load series definitions to map codes → human readable names
    try:
        definitions = pd.read_excel(filepath, sheet_name="Series Definitions")
        # RBNZ definitions sheet has 'Series Id' and 'Series Description' columns
        code_map = dict(zip(
            definitions["Series Id"].astype(str).str.strip(),
            definitions["Series"].astype(str).str.strip()
        ))
        # rename rate columns using the map
        df = df.rename(columns=code_map)
        print(f"  [{name}] Mapped {len(code_map)} series codes → readable names")
    except Exception as e:
        print(f"  [{name}] Could not load series definitions: {e} — keeping raw codes")

    df = df.dropna(how="all").dropna(axis=1, how="all")

    print(f"  [{name}] Raw shape: {df.shape}")
    print(f"  [{name}] Columns: {list(df.columns)[:6]}...")

    # rename first column to date
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "date"})

    # parse dates + drop non-date rows (footnotes etc.)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # melt wide → long format
    rate_cols = [c for c in df.columns if c != "date"]
    df_long = df.melt(id_vars=["date"], value_vars=rate_cols,
                      var_name="term", value_name="rate_pct")

    df_long = df_long.dropna(subset=["rate_pct"])
    df_long = df_long.sort_values("date").reset_index(drop=True)
    df_long["series"] = name

    print(f"  [{name}] Cleaned shape (long): {df_long.shape}")
    print(f"  [{name}] Date range: {df_long['date'].min().date()} → {df_long['date'].max().date()}")
    print(f"  [{name}] Terms: {df_long['term'].unique().tolist()}\n")

    return df_long


def run():
    print("=" * 55)
    print("RBNZ Interest Rate Ingestion — Starting")
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55 + "\n")

    results = {}

    for name, filename in SOURCES.items():
        filepath = find_file(filename)

        if filepath is None:
            print(f"  [{name}] ✗ File not found: {filename}")
            print(f"  [{name}]   → Please download manually from RBNZ and save to {RAW_DIR}/\n")
            continue

        print(f"  [{name}] ✓ Found: {filepath}")
        df = read_and_clean(name, filepath)
        if df is not None:
            results[name] = df

    print("=" * 55)
    print(f"Done. {len(results)}/{len(SOURCES)} sources loaded successfully.")
    print("=" * 55)

    return results


if __name__ == "__main__":
    run()