"""
snowflake_loader.py
-------------------
Loads cleaned NZ housing data from local ingestion scripts into
Snowflake RAW schema tables.

Pulls data from:
    - stats_nz.py  → BUILDING_CONSENTS, HUD_RENTAL_INDEX
    - rbnz.py      → MORTGAGE_RATES

Then writes each DataFrame into the corresponding Snowflake table
using the Snowflake Python connector's write_pandas utility.

Prerequisites:
    - Snowflake account credentials in .env
    - Tables already created in NZ_HOUSING.RAW (run snowflake_setup.sql first)
    - Raw data files downloaded locally (run ingestion scripts first)

Output: all 3 tables populated in NZ_HOUSING.RAW
"""

import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# add project root to path so we can import ingestion scripts
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.stats_nz import run as run_stats_nz
from ingestion.rbnz import run as run_rbnz

# load credentials from .env
load_dotenv()


# ------------------------------------------------------------------
# Snowflake connection
# ------------------------------------------------------------------

def get_connection():
    """Create and return a Snowflake connection using .env credentials."""
    print("  [snowflake] Connecting...")
    conn = snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE",  "NZ_HOUSING"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA",     "RAW"),
    )
    print("  [snowflake] ✓ Connected\n")
    return conn


# ------------------------------------------------------------------
# Loaders — one per table
# ------------------------------------------------------------------

def load_table(conn, df: pd.DataFrame, table_name: str) -> bool:
    """
    Write a pandas DataFrame to a Snowflake table.
    Truncates existing data first so we don't get duplicates on re-runs.
    """
    print(f"  [snowflake] Loading {table_name}...")

    # uppercase all column names — Snowflake is case sensitive with quoted names
    df.columns = df.columns.str.upper()

    # truncate first so re-runs don't duplicate data
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

    success, num_chunks, num_rows, output = write_pandas(
        conn        = conn,
        df          = df,
        table_name  = table_name,
        database    = os.getenv("SNOWFLAKE_DATABASE", "NZ_HOUSING"),
        schema      = os.getenv("SNOWFLAKE_SCHEMA",   "RAW"),
        auto_create_table = False,
    )

    if success:
        print(f"  [snowflake] ✓ {table_name} — {num_rows} rows loaded\n")
    else:
        print(f"  [snowflake] ✗ {table_name} — load failed\n")

    return success


def prepare_building_consents(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map building consents columns to match Snowflake table schema.
    Snowflake table: period, houses, apartments, retirement_units,
                     townhouses, all_dwellings, floor_area, value
    """
    # drop junk rows — keep only rows where period looks like a year or month
    df = df[pd.to_numeric(df["period"], errors="coerce").notna() |
            df["period"].astype(str).str.contains("Month|ended", na=False)]

    # rename columns to match Snowflake table
    rename_map = {
        "period":                        "period",
        "houses":                        "houses",
        "apartments":                    "apartments",
        "retirement_village_units":      "retirement_units",
        "townhouses,_flats,_and_units":  "townhouses",
        "all_dwellings":                 "all_dwellings",
        "floor_area(2)(3)":              "floor_area",
        "value":                         "value",
    }

    # only keep columns that exist in rename map
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing)

    keep_cols = ["period", "houses", "apartments", "retirement_units",
                 "townhouses", "all_dwellings", "floor_area", "value"]
    df = df[[c for c in keep_cols if c in df.columns]]
    df = df.reset_index(drop=True)
    return df


def prepare_hud_rental(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map HUD rental index columns to match Snowflake table schema.
    Snowflake table: region, annual_change, period, rental_price_index
    """
    rename_map = {
        "region":              "region",
        "Annual Change":       "annual_change",
        "period":              "period",
        "rental_price_index":  "rental_price_index",
    }
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing)

    keep_cols = ["region", "annual_change", "period", "rental_price_index"]
    df = df[[c for c in keep_cols if c in df.columns]]

    # ensure period is date type
    df["period"] = pd.to_datetime(df["period"], errors="coerce").dt.date
    df = df.dropna(subset=["period"])
    # force rental_price_index to numeric — drops any datetime objects that slipped in
    df["rental_price_index"] = pd.to_numeric(df["rental_price_index"], errors="coerce")
    df = df.dropna(subset=["rental_price_index"])

    # reset index to avoid Snowflake non-standard index warning
    df = df.reset_index(drop=True)
    return df


def prepare_mortgage_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mortgage rates already in correct long format from rbnz.py.
    Snowflake table: date, term, rate_pct, series
    """
    keep_cols = ["date", "term", "rate_pct", "series"]
    df = df[[c for c in keep_cols if c in df.columns]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    return df


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def run():
    print("=" * 55)
    print("Snowflake Loader — Starting")
    print("=" * 55 + "\n")

    # Step 1 — run ingestion scripts to get DataFrames
    print("--- Running ingestion scripts ---\n")
    stats_nz_data = run_stats_nz()
    rbnz_data     = run_rbnz()

    # Step 2 — connect to Snowflake
    print("\n--- Connecting to Snowflake ---\n")
    conn = get_connection()

    results = {}

    # Step 3 — load each table
    print("--- Loading tables ---\n")

    # Building Consents
    if "building_consents" in stats_nz_data:
        df = prepare_building_consents(stats_nz_data["building_consents"])
        results["BUILDING_CONSENTS"] = load_table(conn, df, "BUILDING_CONSENTS")

    # HUD Rental Index
    if "hud_rental_index" in stats_nz_data:
        df = prepare_hud_rental(stats_nz_data["hud_rental_index"])
        results["HUD_RENTAL_INDEX"] = load_table(conn, df, "HUD_RENTAL_INDEX")

    # Mortgage Rates — combine all 3 RBNZ series into one table
    if rbnz_data:
        combined = pd.concat(rbnz_data.values(), ignore_index=True)
        df = prepare_mortgage_rates(combined)
        results["MORTGAGE_RATES"] = load_table(conn, df, "MORTGAGE_RATES")

    # Step 4 — close connection
    conn.close()
    print("  [snowflake] Connection closed\n")

    # Step 5 — summary
    print("=" * 55)
    success = sum(1 for v in results.values() if v)
    print(f"Done. {success}/{len(results)} tables loaded successfully.")
    for table, ok in results.items():
        status = "✓" if ok else "✗"
        print(f"  {status} {table}")
    print("=" * 55)

if __name__ == "__main__":
    run()