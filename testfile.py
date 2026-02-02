import pandas as pd
import requests
from io import BytesIO
from pathlib import Path

# -----------------------------
# Inputs
# -----------------------------
REGION_LABEL_EU = "European Union"

# Reporters to include (output "Reporter" column uses the dict keys; download uses the codes)
REPORTERS = {
    "European Union": "EUN",
    "China": "CHN",
    "Chile": "CHL",
    "Canada": "CAN",
    "United States": "USA",
    "Russian Federation": "RUS",
    "Argentina": "ARG",
}

# Partners - keep as originally requested (NOT EU), plus World (will be filled as Rest of World)
BASE_PARTNERS = ["United States", "China", "Russian Federation", "Canada", "Argentina", "Chile"]
PARTNERS = BASE_PARTNERS + ["World"]

# EU member states to exclude from Rest of World calculation
EU_COUNTRIES = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark",
    "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy",
    "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal",
    "Romania", "Slovak Republic", "Slovenia", "Spain", "Sweden",
]

YEARS = [2020, 2021, 2022, 2023, 2024]
FLOWS = ["I", "E"]  # I=Import, E=Export
PRODUCTS = ["430110", "430120", "430130", "430140", "430150", "430160", "430170", "430180", "430190"]

BASE_URL = "https://wits.worldbank.org/Download.aspx"

# USD->EUR rates (year: rate)
USD_TO_EUR = {
    2024: 0.924,
    2023: 0.924,
    2022: 0.951,
    2021: 0.846,
    2020: 0.877,
}

# Optional: cache the downloaded Excel files (unique names, no overwriting)
CACHE_DIR = Path("wits_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
USE_CACHE = True

OUT_BASENAME = "Fur_Trade_Data_HS_4301"  # output .csv and .xlsx

# -----------------------------
# Helpers
# -----------------------------
def build_url(reporter_code: str, year: int, flow: str, product: str) -> str:
    params = (
        f"Reporter={reporter_code}&Year={year}&Tradeflow={flow}&Partner=ALL&product={product}"
        f"&Type=HS6Productdata&Lang=en"
    )
    return f"{BASE_URL}?{params}"

def download_excel_bytes(url: str) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    return r.content

def read_by_hs6product_sheet(excel_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(excel_bytes)
    try:
        return pd.read_excel(bio, sheet_name="By-HS6Product")
    except ValueError:
        bio.seek(0)
        return pd.read_excel(bio, sheet_name=0)

def _prep_kg_rows(df: pd.DataFrame) -> pd.DataFrame:
    required = ["Partner", "Quantity", "Quantity Unit", "Trade Value 1000USD"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}. Found: {list(df.columns)}")

    d = df.copy()
    d["Partner"] = d["Partner"].astype(str).str.strip()

    # Keep only Kg rows (quantity logic unchanged)
    d["Quantity Unit"] = d["Quantity Unit"].astype(str).str.strip()
    d = d[d["Quantity Unit"].str.lower() == "kg"]

    # Clean numeric columns
    d["Quantity"] = pd.to_numeric(d["Quantity"], errors="coerce").fillna(0.0)
    d["Trade Value 1000USD"] = pd.to_numeric(d["Trade Value 1000USD"], errors="coerce").fillna(0.0)

    return d

def get_qty_and_value_sums_for_partners(df: pd.DataFrame, partners: list[str]) -> dict[str, tuple[float, float]]:
    """
    Returns dict: partner -> (quantity_kg_sum, trade_value_1000usd_sum)
    """
    d = _prep_kg_rows(df)

    g = (
        d[d["Partner"].isin(partners)]
        .groupby("Partner", as_index=True)[["Quantity", "Trade Value 1000USD"]]
        .sum()
    )

    out = {}
    for p in partners:
        if p in g.index:
            out[p] = (float(g.loc[p, "Quantity"]), float(g.loc[p, "Trade Value 1000USD"]))
        else:
            out[p] = (0.0, 0.0)
    return out

def get_rest_of_world_sum(df: pd.DataFrame, exclude_partners: set[str]) -> tuple[float, float]:
    """
    Rest of World = sum over ALL partners except:
      - World
      - European Union
      - EU member states listed above
      - the six BASE_PARTNERS
    Aggregated on Kg rows.
    Returns (quantity_kg_sum, trade_value_1000usd_sum)
    """
    d = _prep_kg_rows(df)
    d = d[~d["Partner"].isin(exclude_partners)]
    return (float(d["Quantity"].sum()), float(d["Trade Value 1000USD"].sum()))

def inverse_flow(flow: str) -> str:
    return "E" if flow == "I" else "I"

def flow_label(flow: str) -> str:
    return "Import" if flow == "I" else "Export"

# -----------------------------
# Main aggregation (reporter x partner x year x flow)
# -----------------------------
qty_acc = {
    (reporter, partner, year, flow): 0.0
    for reporter in REPORTERS.keys()
    for partner in PARTNERS
    for year in YEARS
    for flow in FLOWS
}
val1000_acc = {
    (reporter, partner, year, flow): 0.0
    for reporter in REPORTERS.keys()
    for partner in PARTNERS
    for year in YEARS
    for flow in FLOWS
}

EXCLUDE_FOR_ROW = set(BASE_PARTNERS + [REGION_LABEL_EU, "World"] + EU_COUNTRIES)

for reporter_label, reporter_code in REPORTERS.items():
    for year in YEARS:
        for flow in FLOWS:
            for product in PRODUCTS:
                url = build_url(reporter_code, year, flow, product)

                cache_path = CACHE_DIR / f"wits_{reporter_code}_{year}_{flow}_{product}.xlsx"
                if USE_CACHE and cache_path.exists():
                    excel_bytes = cache_path.read_bytes()
                else:
                    excel_bytes = download_excel_bytes(url)
                    if USE_CACHE:
                        cache_path.write_bytes(excel_bytes)

                sheet_df = read_by_hs6product_sheet(excel_bytes)

                # Usual partners (six + World)
                sums = get_qty_and_value_sums_for_partners(sheet_df, PARTNERS)

                # CHANGE: fill "World" as Rest of World by summing all partners except excluded list
                row_qty, row_val1000 = get_rest_of_world_sum(sheet_df, EXCLUDE_FOR_ROW)
                sums["World"] = (row_qty, row_val1000)

                for partner, (qty, val1000) in sums.items():
                    qty_acc[(reporter_label, partner, year, flow)] += qty
                    val1000_acc[(reporter_label, partner, year, flow)] += val1000

# -----------------------------
# Build final table:
# 1) All reporters with partners = BASE_PARTNERS + World (World will later be renamed to Rest of World)
# 2) Extra EU<->Reporter rows: add partner="European Union" for each non-EU reporter using EU data with inverse flow
# -----------------------------
rows = []

# Part (1)
for reporter_label in REPORTERS.keys():
    for partner in PARTNERS:
        for year in YEARS:
            for flow in FLOWS:
                trade_value_usd = val1000_acc[(reporter_label, partner, year, flow)] * 1000.0
                eur_rate = USD_TO_EUR.get(year)
                trade_value_eur = trade_value_usd * eur_rate if eur_rate is not None else float("nan")

                rows.append(
                    {
                        "Reporter": reporter_label,
                        "Partner": partner,
                        "Tradeflow": flow_label(flow),
                        "Year": year,
                        "Quantity in kg": qty_acc[(reporter_label, partner, year, flow)],
                        "Trade Value USD": trade_value_usd,
                        "Trade Value EUR": trade_value_eur,
                    }
                )

# Part (2) EU<->Reporter rows derived from EU reporter (inverse flow)
for reporter_label in REPORTERS.keys():
    if reporter_label == REGION_LABEL_EU:
        continue

    eu_partner_name = reporter_label  # EU sheet partner names match these labels

    for year in YEARS:
        for flow in FLOWS:
            inv = inverse_flow(flow)

            qty_from_eu = qty_acc[(REGION_LABEL_EU, eu_partner_name, year, inv)]
            val1000_from_eu = val1000_acc[(REGION_LABEL_EU, eu_partner_name, year, inv)]

            trade_value_usd = val1000_from_eu * 1000.0
            eur_rate = USD_TO_EUR.get(year)
            trade_value_eur = trade_value_usd * eur_rate if eur_rate is not None else float("nan")

            rows.append(
                {
                    "Reporter": reporter_label,
                    "Partner": REGION_LABEL_EU,
                    "Tradeflow": flow_label(flow),
                    "Year": year,
                    "Quantity in kg": qty_from_eu,
                    "Trade Value USD": trade_value_usd,
                    "Trade Value EUR": trade_value_eur,
                }
            )

out = pd.DataFrame(rows)

# Remove rows where Reporter and Partner are the same
out = out[out["Reporter"] != out["Partner"]].copy()

# CHANGE: rename "World" rows (already computed as Rest of World)
out.loc[out["Partner"] == "World", "Partner"] = "Rest of World"

out = out.sort_values(["Reporter", "Partner", "Year", "Tradeflow"]).reset_index(drop=True)

out.to_csv(f"{OUT_BASENAME}.csv", index=False)
out.to_excel(f"{OUT_BASENAME}.xlsx", index=False)

print(out)