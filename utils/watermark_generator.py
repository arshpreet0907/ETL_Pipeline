"""
watermark_generator.py
----------------------
Utilities for generating time-windowed watermark pairs from source data range.

Usage
-----
    from watermark_generator import set_date_range, generate_windows
    from utils.db_connector import get_engine

    engine = get_engine("source")
    set_date_range(engine)                          # populates min_date / max_date

    windows = generate_windows(interval=1, unit="months")
    for wm_from, wm_to in windows:
        frames = extract_all(engine, watermark_from=wm_from, watermark_to=wm_to)
"""

import pandas as pd
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from typing import Literal

# ── Globals ───────────────────────────────────────────────────────────────────
# Format matches source DATETIME columns: "YYYY-MM-DD HH:MM:SS"
min_date: str = "1970-01-01 00:00:00"
max_date: str = "1970-01-01 00:00:00"

_FMT = "%Y-%m-%d %H:%M:%S"

_TABLES = ["suppliers", "vehicles", "parts", "quality_checks"]


# ── Date range setter ─────────────────────────────────────────────────────────

def set_date_range(engine) -> None:
    """
    Query all 4 source tables and set the global min_date / max_date
    to the earliest and latest created_at values found across them.
    """
    global min_date, max_date

    union_sql = " UNION ALL ".join(
        f"SELECT MIN(created_at) AS mn, MAX(created_at) AS mx FROM {t}"
        for t in _TABLES
    )
    df = pd.read_sql(f"SELECT MIN(mn) AS mn, MAX(mx) AS mx FROM ({union_sql}) AS combined", engine)

    min_date = df["mn"].iloc[0].strftime(_FMT)
    max_date = df["mx"].iloc[0].strftime(_FMT)
    print(f"Date range set: {min_date}  →  {max_date}")


# ── Window generator ──────────────────────────────────────────────────────────

def generate_windows(
    interval: int,
    unit: Literal["days", "weeks", "months"],
) -> list[tuple[str, str]]:
    """
    Split [min_date, max_date] into equal-sized windows.

    Parameters
    ----------
    interval : size of each window (e.g. 1, 2, 7)
    unit     : "days", "weeks", or "months"

    Returns
    -------
    List of (watermark_from, watermark_to) string pairs in "YYYY-MM-DD HH:MM:SS".
    The last window's upper bound is clamped to max_date.

    Example
    -------
    generate_windows(1, "months")
    → [("2020-01-01 00:00:00", "2020-02-01 00:00:00"),
       ("2020-02-01 00:00:00", "2020-03-01 00:00:00"), ...]
    """
    if unit not in ("days", "weeks", "months"):
        raise ValueError(f"unit must be 'days', 'weeks', or 'months', got {unit!r}")

    delta_kwargs = {
        "days":   {"days":   interval},
        "weeks":  {"weeks":  interval},
        "months": {"months": interval},
    }[unit]

    start = datetime.strptime(min_date, _FMT)- timedelta(seconds=1)
    end   = datetime.strptime(max_date, _FMT)
    delta = relativedelta(**delta_kwargs)

    windows: list[tuple[str, str]] = []
    cursor = start
    while cursor < end:
        next_cursor = cursor + delta
        wm_to = min(next_cursor, end)
        windows.append((cursor.strftime(_FMT), wm_to.strftime(_FMT)))
        cursor = next_cursor

    print(f"Generated {len(windows)} window(s) of {interval} {unit}")
    return windows
