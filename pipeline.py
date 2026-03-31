"""
pipeline.py
-----------
Orchestrates the full ETL pipeline:
    extract → assert → transform → load → log

Supports two modes
------------------
1. Full load       — migrates every row in every source table in one run.
                     Used on the very first migration and for resets.

2. Windowed batch  — migrates data in date windows (default: monthly).
                     Reads the last successful watermark from etl_run_log,
                     then walks forward window by window until caught up.
                     Safe to resume: if a window fails, the next run picks
                     up from the last successfully logged watermark.

Usage
-----
    # Full load (one shot, all tables):
    python pipeline.py --mode full

    # Windowed incremental (resumes from last successful watermark):
    python pipeline.py --mode incremental

    # Incremental with explicit start date (override etl_run_log):
    python pipeline.py --mode incremental --from "2021-01-01 00:00:00"

    # Programmatic:
    from pipeline import run_full, run_windowed
    run_full(source_engine, target_engine)
    run_windowed(source_engine, target_engine, window_days=30)

Why date windows instead of LIMIT/OFFSET
-----------------------------------------
LIMIT N OFFSET M on large tables becomes slower as M grows because MySQL
must scan and discard M rows before returning N. For 10 million rows,
OFFSET 9_000_000 is orders of magnitude slower than
WHERE created_at > '2023-01-01'.

Date windows are:
  - Reproducible: the same window always returns the same rows.
  - Safe to replay: re-running a window that already loaded does not
    double-insert (use if_exists="append" with UNIQUE constraints, or
    if_exists="replace" per window — see _run_one_window()).
  - Predictable in size: you control granularity by adjusting window_days.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# ── project imports ───────────────────────────────────────────────────────────
# Adjust the path if your project layout differs.
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

from utils.db_connector import get_engine
from utils.extractor import extract_all, get_run_watermark_to
from assertions.source_assertions import run_all_tables
from assertions.target_assertions import run_all_tables as validate_target
from transform         import transform_all, push_to_db, push_to_run_log

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PIPELINE_NAME = "vehicle_dw_migration"


# ─────────────────────────────────────────────────────────────────────────────
# WATERMARK HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def     get_last_watermark(target_engine) -> Optional[str]:
    """
    Read the most recent successful watermark_to from etl_run_log.
    Returns None if no successful run exists (triggers full load).
    """
    sql = """
        SELECT watermark_to
        FROM   etl_run_log
        WHERE  status = 'SUCCESS'
          AND  pipeline_name = :name
        ORDER  BY run_id DESC
        LIMIT  1
    """
    try:
        df = pd.read_sql(sql, target_engine, params={"name": PIPELINE_NAME})
        if df.empty or df["watermark_to"].iloc[0] is None:
            log.info("No previous successful run found — will do full load")
            return None
        wm = str(df["watermark_to"].iloc[0])
        log.info("Last successful watermark: %s", wm)
        return wm
    except Exception as exc:
        log.warning("Could not read etl_run_log (%s) — defaulting to full load", exc)
        return None


def get_source_date_range(source_engine) -> tuple[datetime, datetime]:
    """
    Find the earliest and latest created_at across all four source tables.
    Used to determine the full window span for windowed mode.
    """
    sql = """
        SELECT MIN(created_at) AS earliest, MAX(created_at) AS latest
        FROM (
            SELECT created_at FROM suppliers
            UNION ALL
            SELECT created_at FROM vehicles
            UNION ALL
            SELECT created_at FROM parts
            UNION ALL
            SELECT created_at FROM quality_checks
        ) t
    """
    row = pd.read_sql(sql, source_engine).iloc[0]
    earliest = pd.to_datetime(row["earliest"])
    latest   = pd.to_datetime(row["latest"])
    log.info("Source date range: %s  →  %s", earliest, latest)
    return earliest, latest


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE WINDOW RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def _run_one_window(
    source_engine,
    target_engine,
    watermark_from: str,
    watermark_to: str,
    load_type: str = "INCREMENTAL",
    if_exists: str = "append",
) -> dict:
    """
    Run one complete extract → assert → transform → load cycle for the
    date window [watermark_from, watermark_to).

    Parameters
    ----------
    watermark_from  : ISO datetime string, exclusive lower bound.
    watermark_to    : ISO datetime string, exclusive upper bound.
    load_type       : "FULL" or "INCREMENTAL" — recorded in etl_run_log.
    if_exists       : "append" for incremental, "replace" for full load.

    Returns
    -------
    dict mapping table_name → rows inserted.
    """
    run_start = datetime.now(timezone.utc)

    log.info("─" * 60)
    log.info("Window: %s  →  %s  (load_type=%s)", watermark_from, watermark_to, load_type)
    log.info("─" * 60)

    # ── 1. Extract ────────────────────────────────────────────────────────
    frames = extract_all(
        source_engine,
        watermark_from=watermark_from,
        watermark_to=watermark_to,
    )

    total_extracted = sum(len(df) for df in frames.values())
    if total_extracted == 0:
        log.info("No rows in this window — skipping")
        return {}

    # ── 2. Assert ─────────────────────────────────────────────────────────
    clean = run_all_tables(frames, write_report=True)

    rows_failed = sum(
        len(frames[t]) - len(clean[t]) for t in frames
    )

    # ── 3. Transform ──────────────────────────────────────────────────────
    target = transform_all(clean)

    # ── 4. Load ───────────────────────────────────────────────────────────
    counts = push_to_db(target, target_engine, if_exists=if_exists)

    run_end = datetime.now(timezone.utc)

    # ── 5. Log ────────────────────────────────────────────────────────────
    for table, rows_inserted in counts.items():
        push_to_run_log(
            target_engine,
            pipeline_name   = PIPELINE_NAME,
            table_name      = table,
            load_type       = load_type,
            run_start       = run_start,
            run_end         = run_end,
            watermark_from  = watermark_from,
            watermark_to    = watermark_to,
            rows_extracted  = len(frames[table]),
            rows_inserted   = rows_inserted,
            rows_updated    = 0,
            rows_failed     = rows_failed,
            status          = "SUCCESS",
        )

    log.info(
        "Window complete: %s rows across 4 tables  (%d failed assertions)",
        sum(counts.values()), rows_failed,
    )
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# FULL LOAD
# ─────────────────────────────────────────────────────────────────────────────

def run_full(source_engine, target_engine) -> dict:
    """
    Full load — migrate every row from all four source tables.

    Truncates all target tables before inserting (reverse FK order).
    Watermark stored as (MIN(created_at), NOW()) for the entire run.
    Use this on the first migration or when you need a clean reset.

    Returns
    -------
    dict mapping table_name → rows inserted.
    """
    log.info("=" * 60)
    log.info("FULL LOAD started")
    log.info("=" * 60)

    watermark_from = "1970-01-01 00:00:00"   # effectively no lower bound
    watermark_to   = get_run_watermark_to()   # capture NOW before extraction

    counts = _run_one_window(
        source_engine,
        target_engine,
        watermark_from = watermark_from,
        watermark_to   = watermark_to,
        load_type      = "FULL",
        if_exists      = "replace",           # TRUNCATE first
    )

    log.info("=" * 60)
    log.info("FULL LOAD complete — %s total rows", sum(counts.values()))
    log.info("=" * 60)
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# WINDOWED INCREMENTAL LOAD
# ─────────────────────────────────────────────────────────────────────────────

def run_windowed(
    source_engine,
    target_engine,
    *,
    window_days: int = 30,
    override_from: Optional[str] = None,
) -> dict:
    """
    Windowed incremental load — migrate data in date windows.

    Reads the last successful watermark from etl_run_log and walks forward
    in windows of `window_days` until reaching the current time.

    Parameters
    ----------
    window_days   : size of each extraction window in days (default 30).
                    30 days ≈ monthly batches.
                    7 days  ≈ weekly batches.
                    1 day   ≈ daily batches.
    override_from : if provided, use this as the starting watermark instead
                    of reading from etl_run_log. ISO datetime string.

    Returns
    -------
    dict mapping table_name → total rows inserted across all windows.

    Window logic
    ------------
    Each window covers (watermark_from, watermark_to] where:
        watermark_from = last successful watermark_to  (or override_from)
        watermark_to   = watermark_from + window_days

    For mutable tables (vehicles, suppliers):
        WHERE (created_at > from AND created_at <= to)
           OR (updated_at > from AND updated_at <= to)

    For immutable tables (parts, quality_checks):
        WHERE created_at > from AND created_at <= to

    This ensures edited rows (updated_at) are picked up by the window
    in which they were last changed, not the window they were created in.
    """
    log.info("=" * 60)
    log.info("WINDOWED LOAD started  (window_days=%d)", window_days)
    log.info("=" * 60)

    # Determine starting point
    if override_from:
        current_from = override_from
        log.info("Override watermark_from: %s", current_from)
    else:
        current_from = get_last_watermark(target_engine)
        if current_from is None:
            log.warning(
                "No previous watermark found — running full load instead. "
                "Use run_full() for the initial migration."
            )
            return run_full(source_engine, target_engine)

    # Walk forward in windows until we reach current time
    total_counts: dict[str, int] = {}
    window_num = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    while current_from < now:
        window_num += 1
        current_from_dt = pd.to_datetime(current_from)
        current_to_dt   = current_from_dt + timedelta(days=window_days)
        current_to      = current_to_dt.strftime("%Y-%m-%d %H:%M:%S")

        # Don't overshoot the current time
        if current_to > now:
            current_to = now

        log.info("Window %d: %s  →  %s", window_num, current_from, current_to)

        try:
            counts = _run_one_window(
                source_engine,
                target_engine,
                watermark_from = current_from,
                watermark_to   = current_to,
                load_type      = "INCREMENTAL",
                if_exists      = "append",
            )
            for table, n in counts.items():
                total_counts[table] = total_counts.get(table, 0) + n

        except Exception as exc:
            log.error(
                "Window %d FAILED: %s → %s  error: %s",
                window_num, current_from, current_to, exc,
            )
            # Log the failure and stop — do not advance the watermark.
            # The next run will retry from current_from.
            push_to_run_log(
                target_engine,
                pipeline_name  = PIPELINE_NAME,
                table_name     = "ALL",
                load_type      = "INCREMENTAL",
                run_start      = datetime.now(timezone.utc),
                watermark_from = current_from,
                watermark_to   = current_to,
                status         = "FAILED",
                error_message  = str(exc)[:500],
            )
            log.error("Stopping. Fix the error and re-run to resume from %s", current_from)
            break

        current_from = current_to

    log.info("=" * 60)
    log.info(
        "WINDOWED LOAD complete — %d windows  |  %s total rows",
        window_num, sum(total_counts.values()),
    )
    log.info("=" * 60)
    return total_counts


# ─────────────────────────────────────────────────────────────────────────────
# POST-MIGRATION VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def run_post_migration_validation(target_engine) -> dict:
    """
    Extract target tables and run all 35 post-migration assertions.

    Returns dict of clean DataFrames (rows that passed all checks).
    Failure report written to assertion_failures/ if any failures exist.

    Call this after run_full() or after all windowed batches complete.
    """
    log.info("=" * 60)
    log.info("Post-migration validation started")
    log.info("=" * 60)

    target_frames = {
        "suppliers":      pd.read_sql("SELECT * FROM suppliers",      target_engine),
        "vehicles":       pd.read_sql("SELECT * FROM vehicles",       target_engine),
        "parts":          pd.read_sql("SELECT * FROM parts",          target_engine),
        "quality_checks": pd.read_sql("SELECT * FROM quality_checks", target_engine),
    }

    for table, df in target_frames.items():
        log.info("  Loaded target %s: %d rows", table, len(df))

    clean = validate_target(target_frames, write_report=True)

    total_in     = sum(len(df) for df in target_frames.values())
    total_clean  = sum(len(df) for df in clean.values())
    total_failed = total_in - total_clean

    log.info("-" * 60)
    log.info("Post-migration result: %d rows checked  |  %d passed  |  %d failed",
             total_in, total_clean, total_failed)
    log.info("=" * 60)
    return clean


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Vehicle Manufacturing DW Migration Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --mode full
  python pipeline.py --mode incremental
  python pipeline.py --mode incremental --window 7
  python pipeline.py --mode incremental --from "2021-01-01 00:00:00"
  python pipeline.py --mode validate
        """,
    )
    p.add_argument(
        "--mode",
        choices=["full", "incremental", "validate"],
        required=True,
        help="full: migrate all rows | incremental: resume from last watermark | validate: post-migration checks only",
    )
    p.add_argument(
        "--window",
        type=int,
        default=30,
        metavar="DAYS",
        help="Window size in days for incremental mode (default: 30)",
    )
    p.add_argument(
        "--from",
        dest="from_ts",
        default=None,
        metavar="DATETIME",
        help="Override starting watermark for incremental mode (format: 'YYYY-MM-DD HH:MM:SS')",
    )
    return p


def main():
    args   = _build_parser().parse_args()
    src_e  = get_engine("source")
    tgt_e  = get_engine("target")

    if args.mode == "full":
        counts = run_full(src_e, tgt_e)
        print("\nRows inserted:")
        for t, n in counts.items():
            print(f"  {t:<20} {n:,}")

    elif args.mode == "incremental":
        counts = run_windowed(
            src_e, tgt_e,
            window_days   = args.window,
            override_from = args.from_ts,
        )
        print("\nTotal rows inserted across all windows:")
        for t, n in counts.items():
            print(f"  {t:<20} {n:,}")

    elif args.mode == "validate":
        run_post_migration_validation(tgt_e)


if __name__ == "__main__":
    main()
