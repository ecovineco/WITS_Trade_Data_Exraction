# WITS Trade Data Tools

A pair of Python scripts that download and analyse international trade data from the [World Bank WITS](https://wits.worldbank.org/) platform at the HS6 product level.

## Scripts

### `wits_trade_extractor.py` — Tabular Trade Data

Downloads trade data for a predefined set of **reporters**, **partners**, **years**, and **trade flows** (import / export), then consolidates everything into a single table with:

| Column | Description |
|---|---|
| Reporter | Reporting country or bloc |
| Partner | Trading partner (including a computed *Rest of World* row) |
| Tradeflow | Import or Export |
| Year | Calendar year |
| Quantity in kg | Total weight traded |
| Trade Value USD | Value in US dollars |
| Trade Value EUR | Value converted to euros (using built-in yearly rates) |

Additional EU↔country rows are derived by reading the EU reporter sheet with the inverse flow, so that every non-EU reporter also shows trade with the European Union as a partner.

**Outputs:** CSV and Excel files in the working directory.

**What to edit:** only the `PRODUCTS` list at the top of the file — one or more 6-digit HS codes.

---

### `wits_trade_plotter.py` — Trade Trend Plots

For each combination of reporter country and HS6 product code, this script:

1. Downloads **export** data for the last 10 years.
2. Ranks the **top 10 destination countries** by total exported quantity over the most recent 5 years.
3. Produces a line chart (one curve per country) showing export quantity across all 10 years.
4. Repeats steps 1–3 for **imports** (top 10 origin countries).

All plots are saved as PNG files in the `output_plots/` directory.

**What to edit** (all at the top of the file):

- `HS_CODES` — dictionary mapping 6-digit HS codes to descriptive names.
- `REPORTERS` — dictionary mapping WITS 3-letter country codes to display names.
- `CURRENT_YEAR`, `YEARS_TOTAL`, `YEARS_RANKING`, `TOP_N_PARTNERS` — optional tuning parameters.

---

## Requirements

Both scripts need Python 3.10+ and the following packages:

```
pandas
requests
openpyxl
matplotlib
```

Install with:

```bash
pip install pandas requests openpyxl matplotlib
```

## Caching

Both scripts cache downloaded Excel files in a `wits_cache/` directory so that re-runs do not repeat HTTP requests. Set `USE_CACHE = False` in either script to disable this behaviour.

## Quick Start

```bash
# 1. Generate the consolidated trade table
python wits_trade_extractor.py

# 2. Generate trend plots
python wits_trade_plotter.py
```
