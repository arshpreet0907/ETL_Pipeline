"""
extractor.py
------------
All database interaction with vehicle_manufacturing_src.
Returns raw DataFrames — no assertions, no transformations.

Watermark / window contract
----------------------------
Full load   (no watermarks):  SELECT * FROM table
            → every row in the table

Incremental (watermark_from only):
            WHERE created_at > from
            + OR updated_at > from  (for mutable tables)
            → all rows created/edited after the last run

Windowed    (both watermarks):
            WHERE created_at > from AND created_at <= to
            + OR (updated_at > from AND updated_at <= to)
            → only rows that fall inside the [from, to) window

Pass watermark_to to prevent a slow-moving window from picking up rows
that were just written to the source during the current ETL run.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# WHERE CLAUSE BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _sanitise(ts: str) -> str:
    """
    Strip anything that is not a digit, hyphen, colon, or space.
    A valid ISO datetime "2024-01-15 08:00:00" passes through unchanged.
    Malicious payloads like "2024'; DROP TABLE vehicles;--" are defused.
    """
    return re.sub(r"[^0-9\-: ]", "", ts).strip()


def _build_where(
    watermark_from: Optional[str],
    watermark_to:   Optional[str],
    has_updated_at: bool,
) -> str:
    """
    Build the WHERE clause for a source table extraction query.

    Parameters
    ----------
    watermark_from  : lower bound (exclusive).  None → no lower bound.
    watermark_to    : upper bound (inclusive).   None → no upper bound.
    has_updated_at  : True for vehicles and suppliers (mutable rows).

    Returns
    -------
    str — WHERE clause including the leading "WHERE", or "" for full load.

    Examples
    --------
    _build_where(None, None, False)
        → ""

    _build_where("2024-01-01 00:00:00", None, False)
        → "WHERE created_at > '2024-01-01 00:00:00'"

    _build_where("2024-01-01 00:00:00", "2024-02-01 00:00:00", True)
        → "WHERE (created_at > '2024-01-01 00:00:00'
                  AND created_at <= '2024-02-01 00:00:00')
               OR (updated_at > '2024-01-01 00:00:00'
                  AND updated_at <= '2024-02-01 00:00:00')"

    _build_where(None, "2024-02-01 00:00:00", False)
        → "WHERE created_at <= '2024-02-01 00:00:00'"
    """
    if not watermark_from and not watermark_to:
        return ""   # full load

    from_ts = _sanitise(watermark_from) if watermark_from else None
    to_ts   = _sanitise(watermark_to)   if watermark_to   else None

    # Build a date filter expression for one timestamp column
    def _col_filter(col: str) -> str:
        if from_ts and to_ts:
            return f"({col} > '{from_ts}' AND {col} <= '{to_ts}')"
        if from_ts:
            return f"{col} > '{from_ts}'"
        # to_ts only
        return f"{col} <= '{to_ts}'"

    if has_updated_at and from_ts:
        # Mutable tables: pick up both new rows and edited rows
        return f"WHERE {_col_filter('created_at')} OR {_col_filter('updated_at')}"
    else:
        return f"WHERE {_col_filter('created_at')}"


# ─────────────────────────────────────────────────────────────────────────────
# PER-TABLE EXTRACTORS
# ─────────────────────────────────────────────────────────────────────────────

def extract_suppliers(
    engine,
    watermark_from: Optional[str] = None,
    watermark_to:   Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract rows from suppliers.
    Has updated_at — picks up rating, tier, contract, is_active edits.
    """
    where = _build_where(watermark_from, watermark_to, has_updated_at=True)
    df = pd.read_sql(f"SELECT * FROM suppliers {where}", engine)
    log.info("  suppliers      : %d rows", len(df))
    return df


def extract_vehicles(
    engine,
    watermark_from: Optional[str] = None,
    watermark_to:   Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract rows from vehicles.
    Has updated_at — picks up status, quality_score changes.
    """
    where = _build_where(watermark_from, watermark_to, has_updated_at=True)
    df = pd.read_sql(f"SELECT * FROM vehicles {where}", engine)
    log.info("  vehicles       : %d rows", len(df))
    return df


def extract_parts(
    engine,
    watermark_from: Optional[str] = None,
    watermark_to:   Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract rows from parts.
    No updated_at — parts are immutable once a vehicle is assembled.
    """
    where = _build_where(watermark_from, watermark_to, has_updated_at=False)
    df = pd.read_sql(f"SELECT * FROM parts {where}", engine)
    log.info("  parts          : %d rows", len(df))
    return df


def extract_quality_checks(
    engine,
    watermark_from: Optional[str] = None,
    watermark_to:   Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract rows from quality_checks.
    No updated_at — QC results are immutable once recorded.
    """
    where = _build_where(watermark_from, watermark_to, has_updated_at=False)
    df = pd.read_sql(f"SELECT * FROM quality_checks {where}", engine)
    log.info("  quality_checks : %d rows", len(df))
    return df


def get_all_vehicle_ids(engine) -> set:
    """
    Return ALL vehicle_ids from the source.

    Used in incremental runs where parts/QC reference vehicles that
    were loaded in previous batches. Validating only against the current
    window's vehicles would produce false REF_INTEGRITY failures.
    """
    df = pd.read_sql("SELECT vehicle_id FROM vehicles", engine)
    log.info("  all_vehicle_ids: %d", len(df))
    return set(df["vehicle_id"])


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXTRACT FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_all(
    engine:Engine,
    watermark_from: Optional[str] = None,
    watermark_to:   Optional[str] = None,
) -> dict[str, pd.DataFrame]:
    """
    Extract all four source tables and return raw DataFrames.

    Parameters
    ----------
    engine          : SQLAlchemy engine or PyMySQL connection to source DB.
    watermark_from  : lower bound (exclusive).  None = no lower bound.
    watermark_to    : upper bound (inclusive).   None = no upper bound.

    Returns
    -------
    dict with keys "suppliers", "vehicles", "parts", "quality_checks".

    Calling patterns
    ----------------
    Full load:
        extract_all(engine)

    Resume from last watermark (open-ended):
        extract_all(engine, watermark_from="2024-01-01 00:00:00")

    Windowed batch:
        extract_all(engine,
                    watermark_from="2024-01-01 00:00:00",
                    watermark_to="2024-02-01 00:00:00")
    """
    mode = "full"
    if watermark_from or watermark_to:
        mode = f"window [{watermark_from or '—'} → {watermark_to or '—'}]"

    log.info("=" * 60)
    log.info("Extraction started  mode=%s", mode)
    log.info("=" * 60)

    frames = {
        "suppliers":      extract_suppliers(engine, watermark_from, watermark_to),
        "vehicles":       extract_vehicles(engine,  watermark_from, watermark_to),
        "parts":          extract_parts(engine,     watermark_from, watermark_to),
        "quality_checks": extract_quality_checks(engine, watermark_from, watermark_to),
    }

    total = sum(len(df) for df in frames.values())
    log.info("Extraction complete — %d rows across 4 tables", total)
    return frames


# ─────────────────────────────────────────────────────────────────────────────
# WATERMARK UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def get_run_watermark_to() -> str:
    """
    Capture the current UTC time as the watermark_to for this run.

    Call this ONCE at the very start of the pipeline run, before
    extraction begins. Storing the start time (not the end time) ensures
    any rows written to the source during the extraction window are
    picked up by the next run.

    Returns "YYYY-MM-DD HH:MM:SS" (UTC).
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def extract_rows_by_pk(
        engine:Engine,
        table_name: str,
        pk_pairs: list[tuple],
) -> pd.DataFrame:
    """
    Extract specific rows from a table identified by one or more primary key
    column/value pairs.

    Parameters
    ----------
    engine     : SQLAlchemy engine (source or target, caller decides)
    table_name : one of "suppliers", "vehicles", "parts", "quality_checks"
    pk_pairs   : list of (column, value) tuples that together identify the row(s),
                 e.g. [("vehicle_id", "42")] or [("part_id", "7"), ("supplier_id", "3")]

    Returns
    -------
    pd.DataFrame of matching rows (may be >1 if composite key is partial)

    Example
    -------
    engine = get_engine("source")
    df = extract_rows_by_pk(engine, "vehicles", [("vehicle_id", "42")])
    """
    if not pk_pairs:
        raise ValueError("pk_pairs must contain at least one (column, value) tuple.")

    # Build parameterised WHERE clause to avoid SQL injection
    conditions = " AND ".join(f"{col} = :{col}" for col, _ in pk_pairs)
    params = {col: val for col, val in pk_pairs}
    sql = text(f"SELECT * FROM {table_name} WHERE {conditions}")

    log.info("Extracting rows from %s WHERE %s", table_name, conditions)
    df = pd.read_sql(sql, engine, params=params)
    log.info("Extracted %d row(s) from %s", len(df), table_name)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE SMOKE TEST
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
    from utils.db_connector import get_engine

    wm_from = sys.argv[1] if len(sys.argv) > 1 else None
    wm_to   = sys.argv[2] if len(sys.argv) > 2 else None

    engine = get_engine("source")
    frames = extract_all(engine, watermark_from=wm_from, watermark_to=wm_to)

    print("\nExtracted:")
    for name, df in frames.items():
        print(f"  {name:<20} {len(df):>8,} rows  |  {df.shape[1]} columns")

    print("\nColumn names:")
    for name, df in frames.items():
        print(f"  {name}: {list(df.columns)}")
