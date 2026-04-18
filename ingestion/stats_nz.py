import os
import requests
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

OUTPUT_DIR = "data/raw/stats_nz"


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


def clean_building_consents(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    df = df.dropna(how="all")

    first_col = df.columns[0]
    df = df.rename(columns={first_col: "period"})

    df = df[df["period"].notna()]
    df = df[~df["period"].astype(str).str.startswith("(")]

    print(f"  [building_consents] Cleaned shape: {df.shape}")
    print(f"  [building_consents] Sample:\n{df.head(3).to_string()}\n")
    return df


def clean_hud_rental(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str).str.strip()

    first_col = df.columns[0]
    df = df.rename(columns={first_col: "region"})

    df = df[df["region"].notna()]
    df = df[~df["region"].astype(str).str.startswith("NaN")]

    id_cols  = ["region", "Annual Change"] if "Annual Change" in df.columns else ["region"]
    date_cols = [c for c in df.columns if c not in id_cols]

    df_long = df.melt(id_vars=id_cols, value_vars=date_cols,
                      var_name="period", value_name="rental_price_index")

    df_long["period"] = pd.to_datetime(df_long["period"], errors="coerce")
    df_long = df_long.dropna(subset=["period", "rental_price_index"])

    # values stored as decimals (0.013 = 1.3%)
    df_long["rental_price_index"] = pd.to_numeric(df_long["rental_price_index"], errors="coerce") * 100

    annual_change_col = [c for c in id_cols if c != "region"]
    if annual_change_col:
        df_long = df_long.rename(columns={annual_change_col[0]: "Annual Change"})
        df_long["Annual Change"] = pd.to_numeric(df_long["Annual Change"], errors="coerce") * 100

    print(f"  [hud_rental_index] Cleaned shape (long format): {df_long.shape}")
    return df_long


SOURCES = {
    "building_consents": (get_building_consents_url, "Table 1", 6,  clean_building_consents),
    "hud_rental_index":  (get_hud_rental_url,        "HUD RPI", 9,  clean_hud_rental),
}


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
