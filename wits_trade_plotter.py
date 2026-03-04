"""WITS Trade Plotter — Export & Import Trend Visualisation by HS Code.

This script downloads international trade data from the World Bank WITS
platform for a set of HS6 product codes and a set of reporter countries,
then generates time-series plots showing:

    * **Export quantity** over the last 10 years for the top-5 destination
      countries within the EU, and the top-5 partners worldwide (with all
      EU member states aggregated into a single "European Union" entity).
    * **Import quantity** — same dual-plot approach as exports.

For each HS code × reporter combination, two PNG files are produced:

    * ``_eu.png`` — side-by-side export and import panels for the top 5
      EU partner countries.
    * ``_world.png`` — side-by-side export and import panels for the top 5
      worldwide partners (EU members aggregated into one curve).

Usage:
    1. Edit ``HS_CODES`` to list the product codes you are interested in.
    2. Edit ``REPORTERS`` to list the reporter countries.
    3. Run the script:  ``python wits_trade_plotter.py``

Typical runtime depends on the number of (code × reporter × year × flow)
combinations — each one triggers one HTTP request to WITS (unless cached).
"""

from __future__ import annotations

import logging
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import requests

# ============================================================================
# Configuration — edit these dictionaries to customise the analysis
# ============================================================================

HS_CODES: dict[str, str] = {
    "020710": "Fresh or Chilled Poultry",
    "070200": "Fresh or Chilled Tomato",
    "080810": "Fresh Apples",
}
"""Mapping of HS6 product codes to human-readable names.

Each key must be a **6-digit string** corresponding to an HS6 code on WITS.
The value is the label used in plot titles and file names.

Example::

    HS_CODES = {
        "020710": "Fresh or Chilled Poultry",
        "100199": "Wheat (Other)",
    }
"""

REPORTERS: dict[str, str] = {
    "PRT": "Portugal",
}
"""Mapping of WITS 3-letter reporter codes to country display names.

Each key is the ISO-style reporter code recognised by the WITS download
endpoint (e.g. ``"PRT"``, ``"USA"``, ``"CHN"``).  The value is the
human-readable name used in plot titles and output file names.

Example::

    REPORTERS = {
        "PRT": "Portugal",
        "ESP": "Spain",
    }
"""

EU_COUNTRIES: list[str] = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark",
    "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy",
    "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal",
    "Romania", "Slovak Republic", "Slovenia", "Spain", "Sweden",
]
"""List of EU member state names as they appear in WITS partner data.

Used to filter the EU-only plot and to aggregate all member states into a
single ``"European Union"`` row for the worldwide plot.
"""

# ============================================================================
# Time-range settings
# ============================================================================

CURRENT_YEAR: int = 2024
"""Most recent year of available data on WITS (adjust if newer data exists)."""

YEARS_TOTAL: int = 10
"""Number of years to plot (counting back from ``CURRENT_YEAR``)."""

YEARS_RANKING: int = 5
"""Number of most-recent years used to rank top partner countries."""

ALL_YEARS: list[int] = list(range(CURRENT_YEAR - YEARS_TOTAL + 1, CURRENT_YEAR + 1))
"""Full list of years covered in the analysis."""

RANKING_YEARS: list[int] = list(range(CURRENT_YEAR - YEARS_RANKING + 1, CURRENT_YEAR + 1))
"""Subset of years used exclusively for ranking partners."""

TOP_N_PARTNERS: int = 5
"""Number of top partner countries to display in each plot."""

# ============================================================================
# Paths & caching
# ============================================================================

OUTPUT_DIR: Path = Path("output_plots")
"""Directory where generated PNG plots are saved."""

CACHE_DIR: Path = Path("wits_cache")
"""Directory for caching downloaded Excel files to avoid redundant requests."""

USE_CACHE: bool = True
"""If ``True``, re-use previously downloaded files found in ``CACHE_DIR``."""

# ============================================================================
# WITS download settings
# ============================================================================

BASE_URL: str = "https://wits.worldbank.org/Download.aspx"
"""Base URL for the WITS bulk-download endpoint."""

REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}
"""HTTP headers sent with every WITS request."""

REQUEST_TIMEOUT: int = 120
"""Timeout in seconds for each HTTP request."""

RETRY_ATTEMPTS: int = 3
"""Number of retry attempts for failed downloads."""

RETRY_DELAY: float = 5.0
"""Seconds to wait between retry attempts."""

# ============================================================================
# Source citation
# ============================================================================

SOURCE_TEXT: str = "Source: World Integrated Trade Solution (WITS), https://wits.worldbank.org/"
"""Attribution text displayed beneath every figure."""

# ============================================================================
# Logging
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def build_wits_url(
    reporter_code: str,
    year: int,
    trade_flow: str,
    product_code: str,
) -> str:
    """Construct the full WITS download URL for a single query.

    Args:
        reporter_code: Three-letter WITS reporter code (e.g. ``"PRT"``).
        year: Calendar year of the data.
        trade_flow: ``"E"`` for exports or ``"I"`` for imports.
        product_code: Six-digit HS6 product code.

    Returns:
        The complete URL string ready for an HTTP GET request.
    """
    params = (
        f"Reporter={reporter_code}"
        f"&Year={year}"
        f"&Tradeflow={trade_flow}"
        f"&Partner=ALL"
        f"&product={product_code}"
        f"&Type=HS6Productdata"
        f"&Lang=en"
    )
    return f"{BASE_URL}?{params}"


def download_excel_bytes(url: str) -> bytes:
    """Download binary content from a URL with retry logic.

    Args:
        url: The URL to fetch.

    Returns:
        Raw bytes of the HTTP response body.

    Raises:
        requests.HTTPError: If all retry attempts fail.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(
                url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for %s: %s",
                attempt,
                RETRY_ATTEMPTS,
                url,
                exc,
            )
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
            else:
                raise


def read_hs6_sheet(excel_bytes: bytes) -> pd.DataFrame:
    """Parse the *By-HS6Product* sheet from a WITS Excel download.

    Falls back to the first sheet if the named sheet does not exist.

    Args:
        excel_bytes: Raw bytes of the Excel file.

    Returns:
        A :class:`~pandas.DataFrame` with the sheet contents.
    """
    bio = BytesIO(excel_bytes)
    try:
        return pd.read_excel(bio, sheet_name="By-HS6Product")
    except ValueError:
        bio.seek(0)
        return pd.read_excel(bio, sheet_name=0)


def extract_partner_quantities(df: pd.DataFrame) -> dict[str, float]:
    """Sum trade quantities (in kg) per partner from a WITS data sheet.

    Only rows whose *Quantity Unit* equals ``"kg"`` (case-insensitive) are
    included.  Partners named ``"World"`` are excluded so they do not
    interfere with per-country rankings.

    Args:
        df: DataFrame read from a WITS Excel sheet via :func:`read_hs6_sheet`.

    Returns:
        Dictionary mapping partner country names to their total quantity
        in kilograms.  Partners with zero or missing quantity are omitted.
    """
    required_columns = ["Partner", "Quantity", "Quantity Unit"]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        logger.warning("Missing columns %s — returning empty dict.", missing)
        return {}

    data = df.copy()
    data["Partner"] = data["Partner"].astype(str).str.strip()
    data["Quantity Unit"] = data["Quantity Unit"].astype(str).str.strip()
    data = data[data["Quantity Unit"].str.lower() == "kg"]
    data["Quantity"] = pd.to_numeric(data["Quantity"], errors="coerce").fillna(0.0)

    # Exclude aggregate rows
    data = data[~data["Partner"].isin(["World", ""])]

    grouped = data.groupby("Partner")["Quantity"].sum()
    return grouped[grouped > 0].to_dict()


def fetch_or_cache(
    reporter_code: str,
    year: int,
    trade_flow: str,
    product_code: str,
) -> pd.DataFrame:
    """Download (or load from cache) a WITS Excel file and return parsed data.

    Args:
        reporter_code: Three-letter WITS reporter code.
        year: Calendar year.
        trade_flow: ``"E"`` for exports, ``"I"`` for imports.
        product_code: Six-digit HS6 product code.

    Returns:
        Parsed :class:`~pandas.DataFrame` from the *By-HS6Product* sheet.
    """
    cache_path = (
        CACHE_DIR / f"wits_{reporter_code}_{year}_{trade_flow}_{product_code}.xlsx"
    )

    if USE_CACHE and cache_path.exists():
        logger.info("Cache hit: %s", cache_path.name)
        excel_bytes = cache_path.read_bytes()
    else:
        url = build_wits_url(reporter_code, year, trade_flow, product_code)
        logger.info("Downloading: %s %s %s %s", reporter_code, year, trade_flow, product_code)
        excel_bytes = download_excel_bytes(url)
        if USE_CACHE:
            cache_path.write_bytes(excel_bytes)

    return read_hs6_sheet(excel_bytes)


# ============================================================================
# Core analysis functions
# ============================================================================


def collect_quantity_by_partner_and_year(
    reporter_code: str,
    product_code: str,
    trade_flow: str,
) -> pd.DataFrame:
    """Download data for all years and build a (partner × year) quantity table.

    Args:
        reporter_code: Three-letter WITS reporter code.
        product_code: Six-digit HS6 product code.
        trade_flow: ``"E"`` for exports, ``"I"`` for imports.

    Returns:
        A DataFrame indexed by partner name with one column per year
        containing the total quantity in kg.  Missing values are filled
        with ``0.0``.
    """
    yearly_dicts: dict[int, dict[str, float]] = {}

    for year in ALL_YEARS:
        sheet_df = fetch_or_cache(reporter_code, year, trade_flow, product_code)
        yearly_dicts[year] = extract_partner_quantities(sheet_df)

    # Combine into a single DataFrame (partners as rows, years as columns)
    combined = pd.DataFrame(yearly_dicts).fillna(0.0)
    combined.index.name = "Partner"
    return combined


def filter_eu_only(quantity_table: pd.DataFrame) -> pd.DataFrame:
    """Filter a quantity table to keep only EU member-state partners.

    Args:
        quantity_table: DataFrame produced by
            :func:`collect_quantity_by_partner_and_year`, with partner
            names as the index.

    Returns:
        A new DataFrame containing only rows whose index (partner name)
        is found in ``EU_COUNTRIES``.
    """
    eu_set = set(EU_COUNTRIES)
    eu_mask = quantity_table.index.isin(eu_set)
    return quantity_table.loc[eu_mask].copy()


def aggregate_eu_for_worldwide(quantity_table: pd.DataFrame) -> pd.DataFrame:
    """Aggregate all EU member states into one ``"European Union"`` row.

    Non-EU partners are kept as-is.  Any row whose index matches a name
    in ``EU_COUNTRIES`` is summed into a single ``"European Union"`` row.

    Args:
        quantity_table: DataFrame produced by
            :func:`collect_quantity_by_partner_and_year`.

    Returns:
        A new DataFrame where individual EU countries have been replaced
        by a single ``"European Union"`` aggregate row.
    """
    eu_set = set(EU_COUNTRIES)
    eu_rows = quantity_table.loc[quantity_table.index.isin(eu_set)]
    non_eu_rows = quantity_table.loc[~quantity_table.index.isin(eu_set)].copy()

    if not eu_rows.empty:
        eu_aggregate = eu_rows.sum(axis=0).to_frame("European Union").T
        eu_aggregate.index.name = "Partner"
        result = pd.concat([non_eu_rows, eu_aggregate])
    else:
        result = non_eu_rows

    result.index.name = "Partner"
    return result


def rank_top_partners(
    quantity_table: pd.DataFrame,
    n: int = TOP_N_PARTNERS,
) -> list[str]:
    """Identify the top-N partners by total quantity over the ranking window.

    Args:
        quantity_table: DataFrame with partner names as index and year
            columns, as produced by :func:`collect_quantity_by_partner_and_year`
            (or its filtered / aggregated variants).
        n: Number of top partners to return.

    Returns:
        Ordered list of partner names (highest total first).
    """
    ranking_cols = [y for y in RANKING_YEARS if y in quantity_table.columns]
    if not ranking_cols:
        logger.warning("No ranking-year columns found — returning empty list.")
        return []

    totals = quantity_table[ranking_cols].sum(axis=1).sort_values(ascending=False)
    return totals.head(n).index.tolist()


def _plot_single_panel(
    ax: plt.Axes,
    quantity_table: pd.DataFrame,
    top_partners: list[str],
    trade_flow_label: str,
) -> None:
    """Draw time-series curves for the given partners onto a single Axes.

    This is a private helper used by :func:`generate_combined_plot` to
    populate one subplot (either the export or the import panel).

    Args:
        ax: The matplotlib Axes object to draw on.
        quantity_table: DataFrame with partners as rows and years as columns.
        top_partners: Ordered list of partner names to plot.
        trade_flow_label: ``"Exports"`` or ``"Imports"`` (used as the
            panel subtitle).
    """
    years_to_plot = sorted(
        [y for y in ALL_YEARS if y in quantity_table.columns]
    )

    for partner in top_partners:
        if partner in quantity_table.index:
            values = quantity_table.loc[partner, years_to_plot]
            ax.plot(
                years_to_plot,
                values,
                marker="o",
                linewidth=4,
                markersize=5,
                label=partner,
            )

    ax.set_title(trade_flow_label, fontsize=12, fontweight="bold")
    ax.set_xlabel("Year", fontsize=10)
    ax.set_ylabel("Quantity (kg)", fontsize=10)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x:,.0f}")
    )
    ax.legend(fontsize=8, loc="best")
    ax.grid(True, alpha=0.3)


def generate_combined_plot(
    export_table: pd.DataFrame,
    export_top: list[str],
    import_table: pd.DataFrame,
    import_top: list[str],
    reporter_name: str,
    product_label: str,
    product_code: str,
    scope_label: str,
    output_path: Path,
) -> None:
    """Create a side-by-side figure with export (left) and import (right) panels.

    A source attribution line is placed beneath the figure in small, grey,
    italic Arial text, following standard data-visualisation conventions.

    Args:
        export_table: Quantity table (partners × years) for exports.
        export_top: Top partner names for the export panel.
        import_table: Quantity table (partners × years) for imports.
        import_top: Top partner names for the import panel.
        reporter_name: Human-readable reporter country name.
        product_label: Human-readable HS code description.
        product_code: Six-digit HS code string.
        scope_label: Scope description for the suptitle, e.g.
            ``"Top 5 EU Trade Partners"`` or
            ``"Top 5 Global Trade Partners"``.
        output_path: Full file path where the PNG will be saved.
    """
    fig, (ax_export, ax_import) = plt.subplots(
        nrows=1, ncols=2, figsize=(22, 8)
    )

    # --- Left panel: Exports ---
    _plot_single_panel(ax_export, export_table, export_top, "Exports")

    # --- Right panel: Imports ---
    _plot_single_panel(ax_import, import_table, import_top, "Imports")

    # --- Suptitle spanning both panels ---
    fig.suptitle(
        f"{product_label}: {reporter_name}'s {scope_label}",
        fontsize=15,
        fontweight="bold",
        y=1.02,
    )

    # --- Source citation ---
    fig.text(
        0.5, -0.02,
        SOURCE_TEXT,
        ha="center",
        va="top",
        fontsize=8,
        fontstyle="italic",
        color="grey",
        fontfamily="Arial",
    )

    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved plot: %s", output_path)


# ============================================================================
# Main entry point
# ============================================================================


def main() -> None:
    """Run the full pipeline: download data, rank partners, generate plots.

    For every combination of (reporter, HS code), the pipeline:

    1. Collects yearly quantity data from WITS for exports **and** imports.
    2. **EU figure** — filters both tables to EU member states, ranks the
       top 5 for each flow, and saves one figure with export and import
       panels side by side.
    3. **Worldwide figure** — aggregates all EU members into a single
       ``"European Union"`` row in both tables, ranks the top 5 for each
       flow, and saves a second combined figure.

    All figures are written to ``OUTPUT_DIR``.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    for reporter_code, reporter_name in REPORTERS.items():
        for product_code, product_label in HS_CODES.items():
            logger.info(
                "Processing: %s | %s (%s)",
                reporter_name,
                product_label,
                product_code,
            )

            # Step 1: collect quantity tables for both flows
            export_table = collect_quantity_by_partner_and_year(
                reporter_code, product_code, "E"
            )
            import_table = collect_quantity_by_partner_and_year(
                reporter_code, product_code, "I"
            )

            if export_table.empty and import_table.empty:
                logger.warning(
                    "No data at all for %s / %s — skipping.",
                    reporter_name,
                    product_code,
                )
                continue

            # ==============================================================
            # EU figure (export + import side by side)
            # ==============================================================
            eu_export = filter_eu_only(export_table)
            eu_import = filter_eu_only(import_table)

            eu_export_top = rank_top_partners(eu_export) if not eu_export.empty else []
            eu_import_top = rank_top_partners(eu_import) if not eu_import.empty else []

            if eu_export_top or eu_import_top:
                eu_path = OUTPUT_DIR / f"{reporter_code}_{product_code}_eu.png"
                generate_combined_plot(
                    export_table=eu_export,
                    export_top=eu_export_top,
                    import_table=eu_import,
                    import_top=eu_import_top,
                    reporter_name=reporter_name,
                    product_label=product_label,
                    product_code=product_code,
                    scope_label=f"Top {TOP_N_PARTNERS} EU Trade Partners",
                    output_path=eu_path,
                )
            else:
                logger.warning(
                    "No EU partners for %s / %s — skipping EU figure.",
                    reporter_name,
                    product_code,
                )

            # ==============================================================
            # Worldwide figure (export + import side by side, EU aggregated)
            # ==============================================================
            world_export = aggregate_eu_for_worldwide(export_table)
            world_import = aggregate_eu_for_worldwide(import_table)

            world_export_top = rank_top_partners(world_export) if not world_export.empty else []
            world_import_top = rank_top_partners(world_import) if not world_import.empty else []

            if world_export_top or world_import_top:
                world_path = OUTPUT_DIR / f"{reporter_code}_{product_code}_world.png"
                generate_combined_plot(
                    export_table=world_export,
                    export_top=world_export_top,
                    import_table=world_import,
                    import_top=world_import_top,
                    reporter_name=reporter_name,
                    product_label=product_label,
                    product_code=product_code,
                    scope_label=f"Top {TOP_N_PARTNERS} Global Trade Partners",
                    output_path=world_path,
                )
            else:
                logger.warning(
                    "No worldwide partners for %s / %s — skipping world figure.",
                    reporter_name,
                    product_code,
                )

    logger.info("All figures generated in '%s'.", OUTPUT_DIR)


if __name__ == "__main__":
    main()
