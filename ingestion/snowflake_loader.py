import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.stats_nz import run as run_stats_nz
from ingestion.rbnz import run as run_rbnz

load_dotenv()


def get_connection():
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


def load_table(conn, df: pd.DataFrame, table_name: str) -> bool:
    print(f"  [snowflake] Loading {table_name}...")

    df.columns = df.columns.str.upper()

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
    df = df[pd.to_numeric(df["period"], errors="coerce").notna() |
            df["period"].astype(str).str.contains("Month|ended", na=False)]

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

    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing)

    keep_cols = ["period", "houses", "apartments", "retirement_units",
                 "townhouses", "all_dwellings", "floor_area", "value"]
    df = df[[c for c in keep_cols if c in df.columns]]
    df = df.reset_index(drop=True)
    return df


def prepare_hud_rental(df: pd.DataFrame) -> pd.DataFrame:
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

    df["period"] = pd.to_datetime(df["period"], errors="coerce").dt.date
    df = df.dropna(subset=["period"])
    df["rental_price_index"] = pd.to_numeric(df["rental_price_index"], errors="coerce")
    df = df.dropna(subset=["rental_price_index"])

    df = df.reset_index(drop=True)
    return df


def prepare_mortgage_rates(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = ["date", "term", "rate_pct", "series"]
    df = df[[c for c in keep_cols if c in df.columns]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    return df


def run():
    print("=" * 55)
    print("Snowflake Loader — Starting")
    print("=" * 55 + "\n")

    stats_nz_data = run_stats_nz()
    rbnz_data     = run_rbnz()

    conn = get_connection()

    results = {}

    if "building_consents" in stats_nz_data:
        df = prepare_building_consents(stats_nz_data["building_consents"])
        results["BUILDING_CONSENTS"] = load_table(conn, df, "BUILDING_CONSENTS")

    if "hud_rental_index" in stats_nz_data:
        df = prepare_hud_rental(stats_nz_data["hud_rental_index"])
        results["HUD_RENTAL_INDEX"] = load_table(conn, df, "HUD_RENTAL_INDEX")

    if rbnz_data:
        combined = pd.concat(rbnz_data.values(), ignore_index=True)
        df = prepare_mortgage_rates(combined)
        results["MORTGAGE_RATES"] = load_table(conn, df, "MORTGAGE_RATES")

    conn.close()
    print("  [snowflake] Connection closed\n")

    print("=" * 55)
    success = sum(1 for v in results.values() if v)
    print(f"Done. {success}/{len(results)} tables loaded successfully.")
    for table, ok in results.items():
        status = "✓" if ok else "✗"
        print(f"  {status} {table}")
    print("=" * 55)

if __name__ == "__main__":
    run()
