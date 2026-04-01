"""
get_source_data.py
------------------
Queries the source MySQL database and saves all four source tables
to snowflake_files/source_data.xlsx, one sheet per table.

Run this once (or whenever fresh source data is needed) before
executing the Snowflake pipeline.
"""

import logging
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.db_connector import get_engine
from utils.extractor import extract_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SOURCE_EXCEL = Path(__file__).parent / "source_data.xlsx"


def fetch_and_save(
    watermark_from: str = None,
    watermark_to: str = None,
) -> dict[str, pd.DataFrame]:
    """
    Extract all four source tables from MySQL and write to source_data.xlsx.

    Parameters
    ----------
    watermark_from : optional lower bound (exclusive) for incremental extract
    watermark_to   : optional upper bound (inclusive) for incremental extract

    Returns
    -------
    dict[table_name -> DataFrame]  (same frames written to Excel)
    """
    engine = get_engine("source")
    frames = extract_all(engine, watermark_from=watermark_from, watermark_to=watermark_to)

    with pd.ExcelWriter(SOURCE_EXCEL, engine="openpyxl") as writer:
        for table, df in frames.items():
            df.to_excel(writer, sheet_name=table, index=False)
            log.info("Saved %-16s : %d rows → sheet '%s'", table, len(df), table)

    log.info("Source data written to %s", SOURCE_EXCEL)
    return frames


if __name__ == "__main__":
    watermark_from=None
    watermark_to=None
    fetch_and_save(watermark_from=watermark_from,watermark_to=watermark_to)
