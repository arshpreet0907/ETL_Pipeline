import pandas as pd
from pathlib import Path
from datetime import datetime

def save_parquet(frames: dict, run_ts: str = None, base_dir: str = "parquet") -> dict:
    """
    Save a dict of DataFrames to Parquet files.
    Returns a dict of {table_name: file_path} so you know where each landed.
    """
    if run_ts is None:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    out_dir = Path(base_dir) / run_ts
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = {}
    for table, df in frames.items():
        path = out_dir / f"{table}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
        paths[table] = str(path)
        print(f"  saved {table}: {len(df)} rows → {path}")

    return paths


def load_parquet(run_ts: str, base_dir: str = "parquet") -> dict:
    """
    Load all four table Parquets from a specific run timestamp directory.
    Pass the run_ts that save_parquet() used.
    """
    in_dir = Path(base_dir) / run_ts
    frames = {}
    for path in sorted(in_dir.glob("*.parquet")):
        table = path.stem
        frames[table] = pd.read_parquet(path, engine="pyarrow")
        print(f"  loaded {table}: {len(frames[table])} rows ← {path}")
    return frames