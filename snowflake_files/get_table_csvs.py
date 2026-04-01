"""
get_table_csvs.py
-----------------
Queries the source MySQL database for a single table and saves the result
to snowflake_files/table_csvs/<table_name>.csv.

Run this before executing Snowflake single-table assertions to refresh
the source CSV for that table.

Usage (standalone):
    python get_table_csvs.py vehicles
    python get_table_csvs.py parts 2024-01-01 2024-02-01
"""

import logging
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.db_connector import get_engine
from utils.extractor import (
    extract_suppliers,
    extract_vehicles,
    extract_parts,
    extract_quality_checks,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

TABLE_CSVS_DIR = Path(__file__).parent / "table_csvs"
TABLE_CSVS_DIR.mkdir(exist_ok=True)

_EXTRACTOR = {
    "suppliers":      extract_suppliers,
    "vehicles":       extract_vehicles,
    "parts":          extract_parts,
    "quality_checks": extract_quality_checks,
}

VALID_TABLES = set(_EXTRACTOR.keys())


def fetch_and_save(
    table_name: str,
    watermark_from: str = None,
    watermark_to: str = None,
) -> pd.DataFrame:
    """
    Extract one source table from MySQL and write to table_csvs/<table_name>.csv.

    Parameters
    ----------
    table_name     : one of "suppliers", "vehicles", "parts", "quality_checks"
    watermark_from : optional lower bound (exclusive) for incremental extract
    watermark_to   : optional upper bound (inclusive) for incremental extract

    Returns
    -------
    DataFrame written to CSV
    """
    if table_name not in VALID_TABLES:
        raise ValueError(
            f"Unknown table '{table_name}'. Valid options: {sorted(VALID_TABLES)}"
        )

    engine    = get_engine("source")
    extractor = _EXTRACTOR[table_name]
    df        = extractor(engine, watermark_from=watermark_from, watermark_to=watermark_to)

    dest = TABLE_CSVS_DIR / f"{table_name}.csv"
    df.to_csv(dest, index=False)
    log.info("Saved %-16s : %d rows → %s", table_name, len(df), dest)
    return df

tables=["suppliers","vehicles","parts","quality_checks"]

if __name__ == "__main__":
    # _table = sys.argv[1] if len(sys.argv) > 1 else None
    _table = tables[3]
    if not _table:
        print(f"Usage: python get_table_csvs.py <table_name> [watermark_from] [watermark_to]")
        print(f"Valid tables: {sorted(VALID_TABLES)}")
        sys.exit(1)

    _wm_from = sys.argv[2] if len(sys.argv) > 2 else None
    _wm_to   = sys.argv[3] if len(sys.argv) > 3 else None
    fetch_and_save(_table, watermark_from=_wm_from, watermark_to=_wm_to)
