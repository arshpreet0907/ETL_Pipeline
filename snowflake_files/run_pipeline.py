"""
run_pipeline.py
---------------
Orchestrates the full Snowflake ETL pipeline and records each table's
execution in the Snowflake etl_run_log table.

Run
---
    python snowflake_files/run_pipeline.py
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sf_connector import get_snowflake_engine
from snowflake_pipeline import (
    load_source_excel,
    run_source_assertions,
    run_transform,
    push_to_snowflake,
    extract_from_snowflake,
    run_post_assertions,
    push_to_run_log_sf,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def run_pipeline(if_exists: str = "replace", watermark_from: str = None, watermark_to: str = None) -> None:
    """
    Run the complete Snowflake ETL pipeline end-to-end.

    Steps
    -----
    1. Load source_data.xlsx  (produced by get_source_data.py)
    2. Source assertions
    3. Transform
    4. Push to Snowflake
    5. Extract from Snowflake
    6. Post-migration assertions
    7. Write one etl_run_log entry per table

    Parameters
    ----------
    if_exists      : "replace" (default) for full load, "append" for incremental
    watermark_from : optional lower bound passed through to the run log
    watermark_to   : optional upper bound passed through to the run log
    """
    engine = get_snowflake_engine()

    # 1. Load
    raw_frames = load_source_excel()

    # 2. Source assertions
    clean_source = run_source_assertions(raw_frames)

    # 3. Transform
    transformed = run_transform(clean_source)

    # 4. Push
    load_type = "INCREMENTAL" if watermark_from else "FULL"
    run_start = datetime.now(timezone.utc)
    error_message = None
    rows_written = {}
    try:
        rows_written = push_to_snowflake(transformed, engine, if_exists=if_exists)
        status = "SUCCESS"
    except Exception as e:
        error_message = str(e)
        status = "FAILED"
        raise
    finally:
        run_end = datetime.now(timezone.utc)
        for table in raw_frames:
            push_to_run_log_sf(
                engine,
                pipeline_name="snowflake_etl",
                table_name=table,
                load_type=load_type,
                run_start=run_start,
                run_end=run_end,
                watermark_from=watermark_from,
                watermark_to=watermark_to,
                rows_extracted=len(raw_frames[table]),
                rows_inserted=rows_written.get(table, 0),
                rows_failed=len(raw_frames[table]) - len(clean_source[table]),
                status=status,
                error_message=error_message,
            )

    # 5. Extract
    target_frames = extract_from_snowflake(engine)

    # 6. Post-migration assertions
    clean_target = run_post_assertions(target_frames, source_frames=clean_source)

    log.info("Pipeline complete in %.1fs", (run_end - run_start).total_seconds())
    log.info("Rows written   : %s", rows_written)
    log.info(
        "Post-migration : %d rows passed",
        sum(len(df) for df in clean_target.values()),
    )


if __name__ == "__main__":
    run_pipeline()
