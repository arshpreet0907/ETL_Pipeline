"""
transform.py
------------
Column mapping, derivation, and load layer for the vehicle manufacturing
DW migration.

What this file does
-------------------
1. Maps every source column to its target equivalent (rename / derive / drop).
2. Computes all derived columns using vectorised pandas / numpy operations.
3. Writes transformed DataFrames to the target DB via push_to_db().
4. Records each run in etl_run_log via push_to_run_log().

What this file does NOT do
---------------------------
- No database reads (that is extractor.py)
- No assertion logic (that is source_assertions.py / target_assertions.py)
- No orchestration (that is pipeline.py)

deleted_at note
---------------
Soft-delete columns have been removed from both source and target schemas.
No deleted_at references exist anywhere in this file.

cost_tier note
--------------
cost_tier is banded on total_cost_eur (the derived line-total), NOT on
unit_cost_eur. This matches what target_assertions.py verifies.
  total_cost_eur <= 500          → LOW_VALUE
  total_cost_eur >  500 and <= 2000 → MID_VALUE
  total_cost_eur >  2000         → HIGH_VALUE

dw_inserted_at note
--------------------
Never passed in INSERT — MySQL DEFAULT CURRENT_TIMESTAMP fills it
automatically. Passing NaT for a NOT NULL DEFAULT column causes
error 1048. dw_inserted_at is listed in _SKIP_COLS for every table.
fill_dw_timestamps=True pre-fills it in Python for unit tests that
run against SQLite (which has no DEFAULT CURRENT_TIMESTAMP support).
"""

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import text as _text

log = logging.getLogger(__name__)

# ── helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    """Current UTC time. datetime.utcnow() is deprecated in Python 3.12+."""
    return datetime.now(timezone.utc)


def _safe_dt(series: pd.Series) -> pd.Series:
    """Convert a series to datetime, coercing errors to NaT."""
    return pd.to_datetime(series, errors="coerce")


# ─────────────────────────────────────────────────────────────────────────────
# PER-TABLE TRANSFORMS
# ─────────────────────────────────────────────────────────────────────────────

def transform_vehicles(
    src: pd.DataFrame,
    *,
    fill_dw_timestamps: bool = False,
) -> pd.DataFrame:
    """
    Transform source vehicles DataFrame to target schema.

    Source  →  Target
    -------    ------
    vehicle_id      → src_vehicle_id        (rename; vehicle_sk auto by DB)
    vin             → vin_number            (rename)
    model_code      → model_code            (direct)
    model_code+variant → model_variant_name (derived: concat with _)
    variant         → (dropped)
    color_code      → color_code            (direct)
    engine_type     → engine_type           (direct)
    plant_code      → manufacturing_plant   (rename)
    line_number     → (dropped)
    production_date → production_date       (direct, coerced to datetime)
    production_date → production_year       (derived: dt.year)
    production_date → production_month      (derived: dt.month)
    shift           → production_shift      (rename)
    status          → production_status     (rename)
    quality_score   → quality_score         (direct)
    quality_score   → quality_tier          (derived: pd.cut bands)
    weight_kg       → gross_weight_kg       (rename)
    weight_kg       → weight_category       (derived: >2500 HEAVY else LIGHT)
    engine_type     → is_electric_vehicle   (derived: EV_MOTOR → 1)
    created_at      → created_at            (direct)
    updated_at      → dw_updated_at         (rename)
    (DB auto)       → dw_inserted_at        (excluded from INSERT)
    """
    log.debug("transform_vehicles: started with %d rows", len(src))
    df = src.copy()
    tgt = pd.DataFrame()

    # ── direct / renamed ──────────────────────────────────────────────────
    tgt["src_vehicle_id"]      = df["vehicle_id"]
    tgt["vin_number"]          = df["vin"]
    tgt["model_code"]          = df["model_code"]
    tgt["color_code"]          = df["color_code"]
    tgt["engine_type"]         = df["engine_type"]
    tgt["manufacturing_plant"] = df["plant_code"]
    tgt["production_shift"]    = df["shift"]
    tgt["production_status"]   = df["status"]
    tgt["quality_score"]       = df["quality_score"]
    tgt["gross_weight_kg"]     = df["weight_kg"]
    tgt["created_at"]          = _safe_dt(df["created_at"])

    # updated_at → dw_updated_at (NULL if source row was never edited)
    tgt["dw_updated_at"] = _safe_dt(df["updated_at"]) if "updated_at" in df.columns else pd.NaT

    # ── derived ───────────────────────────────────────────────────────────
    tgt["model_variant_name"] = df["model_code"].str.strip() + "_" + df["variant"].str.strip()

    prod_dt = _safe_dt(df["production_date"])
    tgt["production_date"]  = prod_dt
    tgt["production_year"]  = prod_dt.dt.year.astype("Int16")
    tgt["production_month"] = prod_dt.dt.month.astype("Int8")

    # quality_tier: right=False means bins are [lo, hi)
    # [-inf,60) → SUBSTANDARD, [60,75) → ECONOMY, [75,90) → STANDARD, [90,+inf) → PREMIUM
    tgt["quality_tier"] = pd.cut(
        df["quality_score"],
        bins=[-np.inf, 60, 75, 90, np.inf],
        labels=["SUBSTANDARD", "ECONOMY", "STANDARD", "PREMIUM"],
        right=False,
    ).astype(str)

    tgt["weight_category"]     = np.where(df["weight_kg"] > 2500, "HEAVY", "LIGHT")
    tgt["is_electric_vehicle"] = (df["engine_type"] == "EV_MOTOR").astype("Int8")

    # ── audit timestamp ───────────────────────────────────────────────────
    tgt["dw_inserted_at"] = _safe_dt(
        pd.Series([_utcnow()] * len(df)) if fill_dw_timestamps else pd.Series([pd.NaT] * len(df))
    )

    # ── column order matches target DDL (vehicle_sk omitted — auto by DB) ─
    log.info("transform_vehicles: completed, %d rows transformed", len(tgt))
    return tgt[[
        "src_vehicle_id", "vin_number", "model_code", "model_variant_name",
        "color_code", "engine_type", "manufacturing_plant",
        "production_date", "production_year", "production_month",
        "production_shift", "production_status",
        "quality_score", "quality_tier",
        "gross_weight_kg", "weight_category",
        "is_electric_vehicle",
        "created_at", "dw_inserted_at", "dw_updated_at",
    ]]


def transform_suppliers(
    src: pd.DataFrame,
    *,
    fill_dw_timestamps: bool = False,
) -> pd.DataFrame:
    """
    Transform source suppliers DataFrame to target schema (SCD Type 2).

    Initial load: every row gets valid_from = contract_start_date,
    valid_to = 9999-12-31, is_current = 1.

    Source  →  Target
    -------    ------
    supplier_id     → supplier_id            (direct; supplier_sk auto by DB)
    supplier_code   → supplier_code          (direct)
    supplier_name   → supplier_name          (direct)
    country         → country_of_origin      (rename)
    tier            → supplier_tier          (rename)
    tier            → tier_label             (derived: map 1→STRATEGIC etc.)
    rating          → performance_rating     (rename)
    contract_start  → contract_start_date    (rename + coerce datetime)
    contract_end    → contract_end_date      (rename + coerce datetime)
    (derived)       → contract_duration_days (derived: date diff in days)
    is_active       → active_status          (derived: 1→ACTIVE, 0→INACTIVE)
    is_active       → (dropped as integer)
    contract_start  → valid_from             (SCD2: = contract_start on load)
    (constant)      → valid_to              (SCD2: 9999-12-31)
    (constant)      → is_current            (SCD2: 1 on initial load)
    created_at      → created_at             (direct)
    updated_at      → dw_updated_at          (rename)
    (DB auto)       → dw_inserted_at         (excluded from INSERT)
    """
    log.debug("transform_suppliers: started with %d rows", len(src))
    df = src.copy()
    tgt = pd.DataFrame()

    # ── direct / renamed ──────────────────────────────────────────────────
    tgt["supplier_id"]        = df["supplier_id"]
    tgt["supplier_code"]      = df["supplier_code"]
    tgt["supplier_name"]      = df["supplier_name"]
    tgt["country_of_origin"]  = df["country"]
    tgt["supplier_tier"]      = df["tier"]
    tgt["performance_rating"] = df["rating"]
    tgt["created_at"]         = _safe_dt(df["created_at"])
    tgt["dw_updated_at"]      = _safe_dt(df["updated_at"]) if "updated_at" in df.columns else pd.NaT

    contract_start = _safe_dt(df["contract_start"])
    contract_end   = _safe_dt(df["contract_end"])
    tgt["contract_start_date"] = contract_start
    tgt["contract_end_date"]   = contract_end

    # ── derived ───────────────────────────────────────────────────────────
    _tier_map = {1: "STRATEGIC", 2: "PREFERRED", 3: "APPROVED"}
    tgt["tier_label"] = df["tier"].map(_tier_map)

    tgt["contract_duration_days"] = (
        (contract_end - contract_start).dt.days
    ).astype("Int32")

    tgt["active_status"] = np.where(df["is_active"] == 1, "ACTIVE", "INACTIVE")

    # SCD2 columns — initial load snapshot
    tgt["valid_from"] = contract_start
    tgt["valid_to"]   = "9999-12-31"
    tgt["is_current"] = pd.array([1] * len(df), dtype="Int8")

    # ── audit timestamp ───────────────────────────────────────────────────
    tgt["dw_inserted_at"] = _safe_dt(
        pd.Series([_utcnow()] * len(df)) if fill_dw_timestamps else pd.Series([pd.NaT] * len(df))
    )

    # ── column order (supplier_sk omitted — auto by DB) ───────────────────
    log.info("transform_suppliers: completed, %d rows transformed", len(tgt))
    return tgt[[
        "supplier_id", "supplier_code", "supplier_name",
        "country_of_origin", "supplier_tier", "tier_label",
        "performance_rating",
        "contract_start_date", "contract_end_date", "contract_duration_days",
        "active_status",
        "valid_from", "valid_to", "is_current",
        "created_at", "dw_inserted_at", "dw_updated_at",
    ]]


def transform_parts(
    src: pd.DataFrame,
    *,
    fill_dw_timestamps: bool = False,
) -> pd.DataFrame:
    """
    Transform source parts DataFrame to target schema.

    Source  →  Target
    -------    ------
    part_id         → part_id               (direct)
    vehicle_id      → vehicle_id            (direct — FK to vehicles.src_vehicle_id)
    part_number     → part_number           (direct)
    part_name       → component_name        (rename)
    supplier_code   → supplier_code         (direct — FK to suppliers)
    quantity        → quantity_used         (rename)
    unit_cost       → unit_cost_eur         (rename; currency baked into name)
    quantity×cost   → total_cost_eur        (derived)
    total_cost_eur  → cost_tier             (derived: banded on TOTAL cost)
    install_time_min → installation_hrs     (derived: / 60)
    currency        → (dropped)
    install_time_min → (dropped after deriving hrs)
    defect_flag     → has_defect_flag       (rename)
    batch_number    → batch_number          (direct)
    created_at      → created_at            (direct)
    (DB auto)       → dw_inserted_at        (excluded from INSERT)
    (NaT)           → dw_updated_at         (NULL on initial load)

    cost_tier bands (on total_cost_eur):
        <= 500      → LOW_VALUE
        501–2000    → MID_VALUE
        > 2000      → HIGH_VALUE
    """
    log.debug("transform_parts: started with %d rows", len(src))
    df = src.copy()
    tgt = pd.DataFrame()

    # ── direct / renamed ──────────────────────────────────────────────────
    tgt["part_id"]         = df["part_id"]
    tgt["vehicle_id"]      = df["vehicle_id"]
    tgt["part_number"]     = df["part_number"]
    tgt["component_name"]  = df["part_name"]
    tgt["supplier_code"]   = df["supplier_code"]
    tgt["quantity_used"]   = df["quantity"]
    tgt["unit_cost_eur"]   = df["unit_cost"]
    tgt["has_defect_flag"] = df["defect_flag"]
    tgt["batch_number"]    = df["batch_number"]
    tgt["created_at"]      = _safe_dt(df["created_at"])
    tgt["dw_updated_at"]   = pd.NaT   # no updated_at on parts — immutable

    # ── derived ───────────────────────────────────────────────────────────
    tgt["total_cost_eur"] = (df["quantity"] * df["unit_cost"]).round(2)

    # cost_tier banded on total_cost_eur (line total, not unit cost)
    # right=True means bins are (lo, hi] — standard accounting cut
    tgt["cost_tier"] = pd.cut(
        tgt["total_cost_eur"],
        bins=[-np.inf, 500, 2000, np.inf],
        labels=["LOW_VALUE", "MID_VALUE", "HIGH_VALUE"],
        right=True,
    ).astype(str)

    tgt["installation_hrs"] = (df["install_time_min"] / 60).round(2)

    # ── audit timestamp ───────────────────────────────────────────────────
    tgt["dw_inserted_at"] = _safe_dt(
        pd.Series([_utcnow()] * len(df)) if fill_dw_timestamps else pd.Series([pd.NaT] * len(df))
    )

    # ── column order ──────────────────────────────────────────────────────
    log.info("transform_parts: completed, %d rows transformed", len(tgt))
    return tgt[[
        "part_id", "vehicle_id", "part_number", "component_name",
        "supplier_code", "quantity_used", "unit_cost_eur", "total_cost_eur",
        "cost_tier", "installation_hrs", "has_defect_flag", "batch_number",
        "created_at", "dw_inserted_at", "dw_updated_at",
    ]]


def transform_quality_checks(
    src: pd.DataFrame,
    *,
    fill_dw_timestamps: bool = False,
) -> pd.DataFrame:
    """
    Transform source quality_checks DataFrame to target schema.

    Source  →  Target
    -------    ------
    check_id        → qc_id                 (rename)
    vehicle_id      → vehicle_id            (direct)
    check_date      → inspection_date       (rename + coerce)
    check_date      → inspection_year       (derived: dt.year)
    inspector_id    → inspector_code        (rename)
    station         → inspection_station    (rename)
    test_type       → test_category         (rename)
    result          → inspection_result     (rename)
    defect_code     → defect_code           (direct — nullable)
    defect_code     → has_defect            (derived: notna → 1)
    rework_hours    → rework_hours          (direct — nullable)
    rework_hours    → rework_cost_usd       (derived: × $85 labour rate)
    pass_fail       → is_passed             (derived: PASS → 1)
    pass_fail       → (dropped as string)
    created_at      → created_at            (direct)
    (DB auto)       → dw_inserted_at        (excluded from INSERT)
    (NaT)           → dw_updated_at         (NULL on initial load)
    """
    log.debug("transform_quality_checks: started with %d rows", len(src))
    df = src.copy()
    tgt = pd.DataFrame()

    # ── direct / renamed ──────────────────────────────────────────────────
    tgt["qc_id"]              = df["check_id"]
    tgt["vehicle_id"]         = df["vehicle_id"]
    tgt["inspector_code"]     = df["inspector_id"]
    tgt["inspection_station"] = df["station"]
    tgt["test_category"]      = df["test_type"]
    tgt["inspection_result"]  = df["result"]
    tgt["defect_code"]        = df["defect_code"]
    tgt["rework_hours"]       = df["rework_hours"]
    tgt["created_at"]         = _safe_dt(df["created_at"])
    tgt["dw_updated_at"]      = pd.NaT

    inspection_dt = _safe_dt(df["check_date"])
    tgt["inspection_date"] = inspection_dt

    # ── derived ───────────────────────────────────────────────────────────
    tgt["inspection_year"] = inspection_dt.dt.year.astype("Int16")
    tgt["has_defect"]      = df["defect_code"].notna().astype("Int8")
    tgt["rework_cost_usd"] = (df["rework_hours"].fillna(0) * 85).round(2)
    tgt["is_passed"]       = (df["pass_fail"] == "PASS").astype("Int8")

    # ── audit timestamp ───────────────────────────────────────────────────
    tgt["dw_inserted_at"] = _safe_dt(
        pd.Series([_utcnow()] * len(df)) if fill_dw_timestamps else pd.Series([pd.NaT] * len(df))
    )

    # ── column order ──────────────────────────────────────────────────────
    log.info("transform_quality_checks: completed, %d rows transformed", len(tgt))
    return tgt[[
        "qc_id", "vehicle_id",
        "inspection_date", "inspection_year",
        "inspector_code", "inspection_station",
        "test_category", "inspection_result",
        "defect_code", "has_defect",
        "rework_hours", "rework_cost_usd",
        "is_passed",
        "created_at", "dw_inserted_at", "dw_updated_at",
    ]]


# ─────────────────────────────────────────────────────────────────────────────
# BULK TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def transform_all(
    clean_frames: dict[str, pd.DataFrame],
    *,
    fill_dw_timestamps: bool = False,
) -> dict[str, pd.DataFrame]:
    """
    Apply all four table transforms in one call.

    Parameters
    ----------
    clean_frames : dict with keys "suppliers", "vehicles", "parts",
                   "quality_checks" — clean DataFrames from source_assertions.
    fill_dw_timestamps : bool
        True pre-fills dw_inserted_at in Python (for SQLite / unit tests).
        False lets MySQL DEFAULT CURRENT_TIMESTAMP fill it (production).

    Returns
    -------
    dict with the same 4 keys, transformed DataFrames ready for push_to_db().
    """
    log.debug("transform_all: starting transforms for tables: %s", list(clean_frames.keys()))
    return {
        "suppliers":      transform_suppliers(
                              clean_frames["suppliers"],
                              fill_dw_timestamps=fill_dw_timestamps,
                          ),
        "vehicles":       transform_vehicles(
                              clean_frames["vehicles"],
                              fill_dw_timestamps=fill_dw_timestamps,
                          ),
        "parts":          transform_parts(
                              clean_frames["parts"],
                              fill_dw_timestamps=fill_dw_timestamps,
                          ),
        "quality_checks": transform_quality_checks(
                              clean_frames["quality_checks"],
                              fill_dw_timestamps=fill_dw_timestamps,
                          ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE WRITE
# ─────────────────────────────────────────────────────────────────────────────

# Columns excluded from INSERT — let MySQL fill them automatically.
# vehicle_sk / supplier_sk: AUTO_INCREMENT surrogate keys.
# dw_inserted_at: DEFAULT CURRENT_TIMESTAMP. Passing NaT raises error 1048.
_SKIP_COLS: dict[str, set] = {
    "suppliers":      {"supplier_sk", "dw_inserted_at"},
    "vehicles":       {"vehicle_sk",  "dw_inserted_at"},
    "parts":          {"dw_inserted_at"},
    "quality_checks": {"dw_inserted_at"},
}

# FK-safe insert order: parent tables before child tables.
_INSERT_ORDER = ["suppliers", "vehicles", "parts", "quality_checks"]


def push_to_db(
    target_frames: dict,
    engine,
    *,
    if_exists: str = "append",
    chunksize: int = 1_000,
) -> dict[str,int]:
    """
    Write all four transformed DataFrames to the target database.

    Parameters
    ----------
    target_frames : dict from transform_all().
    engine        : SQLAlchemy engine connected to vehicle_manufacturing_dw.
    if_exists     : "replace" — TRUNCATE all tables in reverse FK order,
                               then INSERT. Safe re-run for full loads.
                    "append"  — straight INSERT. For incremental loads.
    chunksize     : rows per INSERT batch (default 1,000).

    Returns
    -------
    dict mapping table_name → rows inserted.

    Notes
    -----
    FK checks are disabled (SET FOREIGN_KEY_CHECKS = 0) for the entire
    block and re-enabled in a finally clause. Source assertions already
    guarantee referential integrity. This avoids FK violations when
    supplier rows and their referenced parts arrive in the same batch.

    pandas if_exists="replace" is explicitly NOT used — it issues
    DROP TABLE which MySQL refuses when FK constraints exist.
    """
    if if_exists not in ("append", "replace"):
        raise ValueError("if_exists must be 'append' or 'replace'")

    rows_written = {}

    with engine.begin() as conn:
        conn.execute(_text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            if if_exists == "replace":
                for table in reversed(_INSERT_ORDER):
                    conn.execute(_text(f"TRUNCATE TABLE `{table}`"))
                    log.info("Truncated table: %s", table)

            for table in _INSERT_ORDER:
                df = target_frames[table].copy()

                # Drop columns the DB fills automatically
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

        finally:
            conn.execute(_text("SET FOREIGN_KEY_CHECKS = 1"))

    return rows_written


# ─────────────────────────────────────────────────────────────────────────────
# ETL RUN LOG
# ─────────────────────────────────────────────────────────────────────────────

def push_to_run_log(
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
) -> int:
    """
    Insert one row into etl_run_log and return the generated run_id.

    Call once per table per pipeline run, after push_to_db() completes.
    watermark_from and watermark_to should be passed for incremental runs
    so the next run can resume from the correct point.
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
        result = conn.execute(sql, {
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
        run_id = result.lastrowid
        log.info("Run log row inserted: run_id=%d table=%s status=%s",
                 run_id, table_name, status)
        return run_id
