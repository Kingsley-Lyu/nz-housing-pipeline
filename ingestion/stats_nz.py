import os
import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

"""
stats_nz.py
-----------
Ingests NZ housing supply and rental market data from two sources:

    1. Stats NZ — Building Consents Issued (monthly)
       New residential dwelling approvals broken down by type:
       houses, apartments, townhouses, and retirement village units.
       Used as the primary housing supply indicator in the pipeline.

    2. HUD — Rental Price Index (monthly)
       Published by the Ministry of Housing and Urban Development as an
       interim replacement while Stats NZ's own Rental Price Index remains
       on pause (as of 2025). Tracks annual rental price changes by region
       using bond lodgement data at record level.

Both sources are hosted as XLSX files and support automated downloads.
URLs are built dynamically based on the current date with retry logic
to account for publication lag (typically 2-4 months behind).

Once downloaded, this script:
    1. Identifies the correct header row (both files contain metadata rows on top)
    2. Reads the relevant sheet from each XLSX file
    3. Cleans and standardises column names
    4. Melts HUD data from wide to long format for warehouse loading
    5. Saves raw files locally under data/raw/stats_nz/

Output schemas:
    building_consents — period | houses | apartments | townhouses | all_dwellings | floor_area | value
    hud_rental_index  — region | annual_change | period | rental_price_index
"""

OUTPUT_DIR = "data/raw/stats_nz"


# ------------------------------------------------------------------
# URL builders
# ------------------------------------------------------------------

def get_building_consents_url(lag_months: int) -> tuple[str, str]:
    target      = date.today() - relativedelta(months=lag_months)
    month_str   = target.strftime("%B-%Y").lower()
    month_title = target.strftime("%B-%Y")
    month_label = target.strftime("%B %Y")
    url = (
        f"https://www.stats.govt.nz/assets/Uploads/Building-consents-issued/"
        f"Building-consents-issued-{month_title}/Download-data/"
        f"building-consents-issued-{month_str}.xlsx"
    )
    return url, month_label


def get_hud_rental_url(lag_months: int) -> tuple[str, str]:
    target      = date.today() - relativedelta(months=lag_months)
    month_label = target.strftime("%B %Y")
    abbrev_map  = {
        "January": "Jan", "February": "Feb", "March": "Mar",
        "April":   "Apr", "May":      "May", "June":  "Jun",
        "July":    "Jul", "August":   "Aug", "September": "Sept",
        "October": "Oct", "November": "Nov", "December":  "Dec",
    }
    month_abbrev = abbrev_map[target.strftime("%B")]
    year         = target.strftime("%Y")
    url = (
        f"https://www.hud.govt.nz/assets/Uploads/Documents/"
        f"HUD-RPI-Data-for-{month_abbrev}-{year}.xlsx"
    )
    return url, month_label


# ------------------------------------------------------------------
# Generic downloader
# ------------------------------------------------------------------

def try_download(name: str, url_fn, start_lag: int = 2, max_retries: int = 8) -> tuple:
    for lag in range(start_lag, start_lag + max_retries):
        url, label = url_fn(lag)
        print(f"  [{name}] Trying {label}...")
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                print(f"  [{name}] ✓ Found: {label}")
                return response, label, url
            else:
                print(f"  [{name}] {response.status_code} — trying earlier month...")
        except requests.RequestException as e:
            print(f"  [{name}] Request error: {e}")
    return None, None, None


def download_and_save(name: str, url_fn, sheet: str | int, header_row: int) -> pd.DataFrame | None:
    response, label, url = try_download(name, url_fn)

    if response is None:
        print(f"  [{name}] ✗ Could not fetch data.\n")
        return None

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d")
    filepath  = os.path.join(OUTPUT_DIR, f"{name}_{timestamp}.xlsx")

    with open(filepath, "wb") as f:
        f.write(response.content)

    df = pd.read_excel(filepath, sheet_name=sheet, header=header_row)
    df = df.dropna(how="all").dropna(axis=1, how="all")

    print(f"  [{name}] Shape: {df.shape}")
    print(f"  [{name}] Columns: {list(df.columns)}")
    print(f"  [{name}] Saved → {filepath}\n")

    return df


# ------------------------------------------------------------------
# Cleaners
# ------------------------------------------------------------------

def clean_building_consents(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    df = df.dropna(how="all")

    # first column is the time period
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "period"})

    # drop rows where period is NaN or is a footnote like "(1)"
    df = df[df["period"].notna()]
    df = df[~df["period"].astype(str).str.startswith("(")]

    print(f"  [building_consents] Cleaned shape: {df.shape}")
    print(f"  [building_consents] Sample:\n{df.head(3).to_string()}\n")
    return df


def clean_hud_rental(df: pd.DataFrame) -> pd.DataFrame:
    """
    HUD RPI sheet — annual rental price change by region over time.
    Columns confirmed: region, Annual Change, then date columns (2023-12 to 2025-09)
    """
    df.columns = df.columns.astype(str).str.strip()

    # first unnamed column is the region
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "region"})

    df = df[df["region"].notna()]
    df = df[~df["region"].astype(str).str.startswith("NaN")]

    # melt from wide (date columns) to long format — better for data warehouse loading
    id_cols  = ["region", "Annual Change"] if "Annual Change" in df.columns else ["region"]
    date_cols = [c for c in df.columns if c not in id_cols]

    df_long = df.melt(id_vars=id_cols, value_vars=date_cols,
                      var_name="period", value_name="rental_price_index")

    df_long["period"] = pd.to_datetime(df_long["period"], errors="coerce")
    df_long = df_long.dropna(subset=["period", "rental_price_index"])

    print(f"  [hud_rental_index] Cleaned shape (long format): {df_long.shape}")
    return df_long


# ------------------------------------------------------------------
# Source registry — name: (url_fn, sheet, header_row, cleaner)
# ------------------------------------------------------------------

SOURCES = {
    "building_consents": (get_building_consents_url, "Table 1", 6,  clean_building_consents),
    "hud_rental_index":  (get_hud_rental_url,        "HUD RPI", 9,  clean_hud_rental),
}


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def run():
    print("=" * 55)
    print("NZ Housing Ingestion — Stats NZ + HUD")
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 55 + "\n")

    results = {}

    for name, (url_fn, sheet, header_row, cleaner) in SOURCES.items():
        df = download_and_save(name, url_fn, sheet=sheet, header_row=header_row)
        if df is not None:
            df = cleaner(df)
            results[name] = df

    print("=" * 55)
    print(f"Done. {len(results)}/{len(SOURCES)} sources pulled successfully.")
    print("=" * 55)

    return results


if __name__ == "__main__":
    run()