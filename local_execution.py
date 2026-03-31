'''
sql to snowflake with validations,
queries used for tables
using python

etl testing pipeline: to directly upload data
'''


import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from utils.db_connector import get_engine
from utils.extractor import extract_all
from assertions.source_assertions import run_all_tables as run_all_source_assertions
import transform
from assertions.target_assertions import run_all_tables as run_all_target_assertions
from datetime import datetime, timezone
import argparse


def whole_pipeline(watermark_from:str=None,watermark_to:str=None):
    engine = get_engine("source")

    source_frames = extract_all(engine,watermark_from=watermark_from,watermark_to=watermark_to)

    clean_source = run_all_source_assertions(source_frames, write_report=True)
    # writeCleanResults(clean_source)

    transformed=transform.transform_all(clean_source)

    engine=get_engine("target")

    load_type = "INCREMENTAL" if watermark_from else "FULL"
    run_start = datetime.now(timezone.utc)
    error_message = None
    rows_written = {}
    try:
        rows_written=transform.push_to_db(transformed,engine,if_exists="replace") # make it append if windows are used
        status = "SUCCESS"
    except Exception as e:
        error_message = str(e)
        status = "FAILED"
        raise
    finally:
        run_end = datetime.now(timezone.utc)
        for table in source_frames:
            transform.push_to_run_log(
                engine,
                pipeline_name="etl_poc",
                table_name=table,
                load_type=load_type,
                run_start=run_start,
                run_end=run_end,
                watermark_from=watermark_from,
                watermark_to=watermark_to,
                rows_extracted=len(source_frames[table]),
                rows_inserted=rows_written.get(table, 0),
                rows_failed=len(source_frames[table]) - len(clean_source[table]),
                status=status,
                error_message=error_message,
            )

    # Extract from target DB to run post-migration assertions
    target_frames = extract_all(engine)

    # Pass both target and source frames to enable derived column validation
    clean_transformed=run_all_target_assertions(
        target_frames,
        source_frames=clean_source,
        write_report=True
    )

    print(f"Pipeline complete. {rows_written} rows written to target DB.")
    print(f"Post-migration validation: {sum(len(df) for df in clean_transformed.values())} rows passed.")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--watermark-from", default=None)
    parser.add_argument("--watermark-to", default=None)
    args = parser.parse_args()

    watermark_from=args.watermark_from
    # watermark_from=None
    watermark_to=args.watermark_to
    # watermark_to=None

    whole_pipeline(watermark_from=watermark_from, watermark_to=watermark_to)



