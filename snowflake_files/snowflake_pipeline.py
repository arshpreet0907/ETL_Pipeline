"""
snowflake_pipeline.py
---------------------
Core pipeline functions for the Snowflake ETL.

Steps (called by run_pipeline.py)
----------------------------------
1. load_source_excel()      — Read source_data.xlsx (produced by get_source_data.py).
2. run_source_assertions()  — Apply source assertions (reuses source_assertions.py).
3. run_transform()          — Apply all column transforms (reuses transform.py).
4. push_to_snowflake()      — Write transformed frames to Snowflake tables.
5. extract_from_snowflake() — Pull the just-loaded data back out for validation.
6. run_post_assertions()    — Apply post-migration assertions (reuses target_assertions.py).
7. push_to_run_log_sf()     — Insert one ETL run log row into Snowflake etl_run_log.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text as _text

# ── project root on path so sibling packages resolve ─────────────────────────
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from assertions.source_assertions import run_all_tables as run_all_source_assertions
from assertions.target_assertions import run_all_tables as run_all_target_assertions
import transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Source Excel produced by get_source_data.py ──────────────────────────────
SOURCE_EXCEL = Path(__file__).parent / "source_data.xlsx"

# ── Snowflake INSERT order (FK-safe: parents before children) ─────────────────
_INSERT_ORDER = ["suppliers", "vehicles", "parts", "quality_checks"]

# Columns auto-filled by Snowflake DEFAULT — exclude from INSERT
_SKIP_COLS: dict[str, set] = {
    "suppliers":      {"supplier_sk", "dw_inserted_at"},
    "vehicles":       {"vehicle_sk",  "dw_inserted_at"},
    "parts":          {"dw_inserted_at"},
    "quality_checks": {"dw_inserted_at"},
}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Load source Excel
# ─────────────────────────────────────────────────────────────────────────────

def load_source_excel() -> dict[str, pd.DataFrame]:
    """
    Read all four source tables from snowflake_files/source_data.xlsx.
    Run get_source_data.py first to generate this file.

    Returns
    -------
    dict[table_name -> DataFrame]
    """
    if not SOURCE_EXCEL.exists():
        raise FileNotFoundError(
            f"source_data.xlsx not found at {SOURCE_EXCEL}. "
            "Run get_source_data.py first."
        )
    frames: dict[str, pd.DataFrame] = {}
    for table in _INSERT_ORDER:
        frames[table] = pd.read_excel(SOURCE_EXCEL, sheet_name=table)
        log.info("Loaded %-16s : %d rows from source_data.xlsx", table, len(frames[table]))
    return frames


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Source assertions
# ─────────────────────────────────────────────────────────────────────────────

def run_source_assertions(
    frames: dict[str, pd.DataFrame],
    write_report: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Run source-side assertions on the CSV-loaded frames.

    Parameters
    ----------
    frames       : output of load_csvs()
    write_report : write Excel failure report if failures exist

    Returns
    -------
    dict[table_name -> clean DataFrame]
    """
    return run_all_source_assertions(frames, write_report=write_report)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Transform
# ─────────────────────────────────────────────────────────────────────────────

def run_transform(
    clean_frames: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Apply all column transforms to the clean source frames.

    Parameters
    ----------
    clean_frames : output of run_source_assertions()

    Returns
    -------
    dict[table_name -> transformed DataFrame]
    """
    return transform.transform_all(clean_frames, fill_dw_timestamps=True)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Push to Snowflake
# ─────────────────────────────────────────────────────────────────────────────

def push_to_snowflake(
    target_frames: dict[str, pd.DataFrame],
    engine,
    *,
    if_exists: str = "replace",
    chunksize: int = 1_000,
) -> dict[str, int]:
    """
    Write all four transformed DataFrames to Snowflake tables.

    Parameters
    ----------
    target_frames : output of run_transform()
    engine        : Snowflake SQLAlchemy engine from get_snowflake_engine()
    if_exists     : "replace" — TRUNCATE then INSERT (full load)
                    "append"  — straight INSERT (incremental)
    chunksize     : rows per INSERT batch

    Returns
    -------
    dict[table_name -> rows inserted]
    """
    if if_exists not in ("append", "replace"):
        raise ValueError("if_exists must be 'append' or 'replace'")

    rows_written: dict[str, int] = {}

    with engine.begin() as conn:
        if if_exists == "replace":
            for table in reversed(_INSERT_ORDER):
                conn.execute(_text(f"TRUNCATE TABLE IF EXISTS {table.upper()}"))
                log.info("Truncated table: %s", table)

        for table in _INSERT_ORDER:
            df = target_frames[table].copy()
            skip = _SKIP_COLS.get(table, set())
            df = df.drop(columns=[c for c in skip if c in df.columns])

            df.to_sql(
                name=table,
                con=conn,
                if_exists="append",
                index=False,
                chunksize=chunksize,
            )

            rows_written[table] = len(df)
            log.info("Inserted %d rows into %s", len(df), table)

    return rows_written


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Extract from Snowflake
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_snowflake(engine) -> dict[str, pd.DataFrame]:
    """
    Pull all four tables back from Snowflake for post-migration validation.

    Parameters
    ----------
    engine : Snowflake SQLAlchemy engine

    Returns
    -------
    dict[table_name -> DataFrame]
    """
    frames: dict[str, pd.DataFrame] = {}
    for table in _INSERT_ORDER:
        frames[table] = pd.read_sql(f"SELECT * FROM {table.upper()}", engine)
        log.info("Extracted %-16s : %d rows", table, len(frames[table]))
    return frames


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Post-migration assertions
# ─────────────────────────────────────────────────────────────────────────────

def run_post_assertions(
    target_frames: dict[str, pd.DataFrame],
    source_frames: dict[str, pd.DataFrame],
    write_report: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Run post-migration assertions on data extracted from Snowflake.

    Parameters
    ----------
    target_frames : output of extract_from_snowflake()
    source_frames : clean source frames (output of run_source_assertions()),
                    needed for derived-column validation (e.g. active_status)
    write_report  : write Excel failure report if failures exist

    Returns
    -------
    dict[table_name -> clean DataFrame]
    """
    return run_all_target_assertions(
        target_frames,
        source_frames=source_frames,
        write_report=write_report,
    )


# ─────────────────────────────────────────────────────────────────────────────
# RUN LOG
# ─────────────────────────────────────────────────────────────────────────────

def push_to_run_log_sf(
    engine,
    *,
    pipeline_name: str,
    table_name: str,
    load_type: str,
    run_start,
    run_end=None,
    watermark_from=None,
    watermark_to=None,
    rows_extracted: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_failed: int = 0,
    status: str,
    error_message: str = None,
) -> None:
    """
    Insert one row into the Snowflake etl_run_log table.

    Mirrors transform.push_to_run_log() but targets Snowflake.
    The run_id column must be AUTOINCREMENT in the Snowflake DDL.
    """
    sql = _text("""
        INSERT INTO etl_run_log (
            run_start, run_end, pipeline_name, table_name, load_type,
            watermark_from, watermark_to,
            rows_extracted, rows_inserted, rows_updated, rows_failed,
            status, error_message
        ) VALUES (
            :run_start, :run_end, :pipeline_name, :table_name, :load_type,
            :watermark_from, :watermark_to,
            :rows_extracted, :rows_inserted, :rows_updated, :rows_failed,
            :status, :error_message
        )
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "run_start":      run_start,
            "run_end":        run_end,
            "pipeline_name":  pipeline_name,
            "table_name":     table_name,
            "load_type":      load_type,
            "watermark_from": watermark_from,
            "watermark_to":   watermark_to,
            "rows_extracted": rows_extracted,
            "rows_inserted":  rows_inserted,
            "rows_updated":   rows_updated,
            "rows_failed":    rows_failed,
            "status":         status,
            "error_message":  error_message,
        })
    log.info("Run log inserted: table=%s status=%s", table_name, status)
