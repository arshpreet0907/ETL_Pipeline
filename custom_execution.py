import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

from utils.db_connector import get_engine
from utils.makeParquet import save_parquet
from utils.extractor import extract_all
from assertions.source_assertions import run_all_tables as run_all_source_assertions
from utils.test_data.frame_to_csv import writeCleanResultsCSV,writeCleanResults
from utils.watermark_generator import set_date_range,generate_windows
from local_execution import whole_pipeline

def check_source_data_working():
    watermark_from="2022-03-09 10:11:00"
    watermark_to=None
    engine = get_engine("source")
    frames = extract_all(engine,watermark_from=watermark_from,watermark_to=watermark_to)
    # frames = extract_all(engine)

    for name, df in frames.items():
        print(f"{name}: {len(df)} rows, {df.shape[1]} cols")
        print(f"  nulls: {df.isnull().sum().to_dict()}")
        print(f"  created_at range: {df['created_at'].min()} → {df['created_at'].max()}")


def source_assertions():
    engine = get_engine("source")
    frames = extract_all(engine)

    # All tables at once
    clean = run_all_source_assertions(frames, write_report=True)

    # One table (e.g. after fixing specific rows)
    # vehicles_frame = extract_vehicles(engine)
    # clean, fc = run_one_table("vehicles", vehicles_frame)
    # print(f"Passed: {len(clean)} rows")
    # print(f"Failed: {fc.failed_row_count} rows")

    # One row (spot check or streaming)
    # row = vehicles_frame.iloc[0].to_dict()
    # print(f"Running assertions on one row: {row}")
    # clean_row, failures = run_one_row("vehicles", row)
    # print(f"Row passed: {clean_row}")
    # print(f"Failures: {failures}")

    save_parquet(clean)
    writeCleanResults(clean)
    writeCleanResultsCSV(clean)
    print("Source assertions complete.")
    return clean

def get_windows():
    set_date_range(get_engine("source"))
    windows=generate_windows(1,"days")
    for window in windows:
        watermark_from=window[0]
        watermark_to=window[1]
        print(f"from: '{watermark_from}' to: '{watermark_to}'")
        # whole_pipeline(watermark_from,watermark_to)

if __name__=="__main__":
    # clean_data=source_assertions()
    # check_source_data_working()
    get_windows()

# from zoneinfo import ZoneInfo
# IST = ZoneInfo("Asia/Kolkata")
# run_start = datetime.now(IST)

# run_start.astimezone(ZoneInfo("Asia/Kolkata"))