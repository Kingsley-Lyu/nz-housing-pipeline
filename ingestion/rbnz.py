import os
import glob
import pandas as pd
from datetime import datetime

RAW_DIR = "data/raw/rbnz"

SOURCES = {
    "mortgage_special_rates":  "hb21-monthly.xlsx",
    "mortgage_standard_rates": "hb20-monthly.xlsx",
    "mortgage_weighted_avg":   "hb30-monthly.xlsx",
}


def find_file(filename: str) -> str | None:
    exact = os.path.join(RAW_DIR, filename)
    if os.path.exists(exact):
        return exact

    stem = filename.replace(".xlsx", "").replace("-monthly", "")
    matches = glob.glob(os.path.join(RAW_DIR, f"*{stem}*.xlsx"))
    if matches:
        print(f"  Found alternative file: {matches[0]}")
        return matches[0]

    return None


def find_header_row(df: pd.DataFrame) -> int:
    for i, row in df.iterrows():
        if any(str(v).strip().lower() == "date" for v in row):
            return i
    return 4


def read_and_clean(name: str, filepath: str) -> pd.DataFrame | None:
    xl = pd.ExcelFile(filepath)
    print(f"  [{name}] Sheets: {xl.sheet_names}")

    sheet = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
    print(f"  [{name}] Reading sheet: '{sheet}'")

    raw = pd.read_excel(filepath, sheet_name=sheet, header=None)
    header_row = find_header_row(raw)
    print(f"  [{name}] Header row: {header_row}")

    df = pd.read_excel(filepath, sheet_name=sheet, header=header_row)
    try:
        definitions = pd.read_excel(filepath, sheet_name="Series Definitions")
        code_map = dict(zip(
            definitions["Series Id"].astype(str).str.strip(),
            definitions["Series"].astype(str).str.strip()
        ))
        df = df.rename(columns=code_map)
        print(f"  [{name}] Mapped {len(code_map)} series codes → readable names")
    except Exception as e:
        print(f"  [{name}] Could not load series definitions: {e} — keeping raw codes")

    df = df.dropna(how="all").dropna(axis=1, how="all")

    print(f"  [{name}] Raw shape: {df.shape}")
    print(f"  [{name}] Columns: {list(df.columns)[:6]}...")

    first_col = df.columns[0]
    df = df.rename(columns={first_col: "date"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

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
