"""
source_assertions.py
--------------------
Source-side data quality assertions for the vehicle manufacturing migration.

Validates raw data extracted from vehicle_manufacturing_src BEFORE it is
transformed and loaded into the target warehouse.  Has NO knowledge of
databases — it receives pandas DataFrames and returns clean DataFrames.

Soft-delete note
----------------
Soft-delete columns (deleted_at) have been removed from the source schema
per the team decision.  This is a one-time migration of all rows; no
filtering on deletion status is performed here.

Watermark note
--------------
This module does not manage watermarks.  Watermark logic lives entirely
in extractor.py.  These functions receive whatever DataFrames the
extractor provides — whether from a full load or an incremental batch.

Three execution modes
---------------------
All three modes share the same underlying rule definitions and the same
FailureCollector / report-writer infrastructure from assertion_rules.py.

    1. run_all_tables(frames)
       Run all 23 rules across all 4 tables in one call.
       Returns: dict[table_name -> clean_DataFrame]

    2. run_one_table(table_name, df, ...)
       Run all rules for one named table against its DataFrame.
       Returns: (clean_DataFrame, FailureCollector)

    3. run_one_row(table_name, row, ...)
       Run all rules for one named table against a single row dict.
       Returns: (row_dict_or_None, list[failure_dicts])
       Returns (row, []) if clean; (None, [failures]) if any rule fails.

Usage examples
--------------
    from extractor         import extract_all, get_last_watermark
    from source_assertions import run_all_tables, run_one_table, run_one_row

    # Mode 1 — full pipeline
    wm     = get_last_watermark(target_engine)
    frames = extract_all(source_engine, watermark=wm)
    clean  = run_all_tables(frames)

    # Mode 2 — one table (e.g. for partial recovery)
    clean_df, fc = run_one_table("vehicles", df_vehicles)

    # Mode 3 — one row (e.g. for ad-hoc validation or streaming)
    row = {"vehicle_id": 42, "vin": "ABC", ...}
    clean_row, failures = run_one_row("vehicles", row)
    if clean_row:
        # row is valid, pass to transform
        ...
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from assertions.assertion_rules import (
    AssertionRule,
    FailureCollector,
    check_not_null,
    check_unique,
    make_range_check,
    make_positive_check,
    make_min_check,
    make_enum_check,
    make_str_length_check,
    make_ref_integrity_check,
    make_date_logic_check,
    run_rules_on_row,
    run_rules_on_dataframe,
    write_failure_report,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# VALID VALUE SETS
# Centralised here so they appear in one auditable place.
# ─────────────────────────────────────────────────────────────────────────────

VALID_SHIFTS     = {"MORNING", "AFTERNOON", "NIGHT"}
VALID_STATUSES   = {"COMPLETED", "IN_PROGRESS", "ON_HOLD", "REJECTED"}
VALID_CURRENCIES = {"EUR", "USD", "GBP"}
VALID_TIERS      = {1, 2, 3}
VALID_PASS_FAIL  = {"PASS", "FAIL"}
VALID_TEST_TYPES = {"PAINT", "ELECTRICAL", "STRUCTURAL", "EMISSIONS", "SAFETY"}


# ─────────────────────────────────────────────────────────────────────────────
# RULE REGISTRY
# One list of AssertionRule per table.  Adding a new check = adding one entry.
# Rules that need a ref_set at runtime declare "ref_set" in context_keys;
# the runner supplies it from the context_providers argument.
# ─────────────────────────────────────────────────────────────────────────────

SUPPLIER_RULES: list[AssertionRule] = [
    AssertionRule(
        name           = "supplier_code uniqueness",
        assertion_type = "UNIQUENESS",
        column         = "supplier_code",
        reason         = "Duplicate supplier_code causes double-counting in "
                         "parts-by-supplier reports and cost rollups",
        check_fn       = check_unique,
        context_keys   = ["series"],
    ),
    AssertionRule(
        name           = "rating range",
        assertion_type = "RANGE",
        column         = "rating",
        reason         = "Rating must be 0-5; values outside this range make "
                         "supplier performance scores incomparable",
        check_fn       = make_range_check(0, 5),
    ),
    AssertionRule(
        name           = "tier enum",
        assertion_type = "ENUM",
        column         = "tier",
        reason         = "Tier must be 1, 2, or 3; other values break the "
                         "tier_label mapping and supply-chain hierarchy reports",
        check_fn       = make_enum_check(VALID_TIERS),
    ),
    AssertionRule(
        name           = "is_active enum",
        assertion_type = "ENUM",
        column         = "is_active",
        reason         = "is_active must be 0 or 1; other values produce a "
                         "wrong active_status derivation in the target",
        check_fn       = make_enum_check({0, 1}),
    ),
    AssertionRule(
        name           = "contract date logic",
        assertion_type = "DATE_LOGIC",
        column         = "contract_start",
        reason         = "contract_start must be before contract_end; reversed "
                         "dates produce a negative contract_duration_days",
        check_fn       = make_date_logic_check("contract_end"),
        context_keys   = ["row"],
    ),
]

VEHICLE_RULES: list[AssertionRule] = [
    AssertionRule(
        name           = "vin not null",
        assertion_type = "NULL_CHECK",
        column         = "vin",
        reason         = "VIN is the legal global identity of a vehicle; a null "
                         "means the record is completely untraceable",
        check_fn       = check_not_null,
    ),
    AssertionRule(
        name           = "vin uniqueness",
        assertion_type = "UNIQUENESS",
        column         = "vin",
        reason         = "Duplicate VINs indicate a join error or source data "
                         "corruption; each physical vehicle has exactly one VIN",
        check_fn       = check_unique,
        context_keys   = ["series"],
    ),
    AssertionRule(
        name           = "vin length",
        assertion_type = "RANGE",
        column         = "vin",
        reason         = "VIN must be exactly 17 characters per ISO 3779; "
                         "shorter or longer values indicate truncation or padding",
        check_fn       = make_str_length_check(17),
    ),
    AssertionRule(
        name           = "quality_score range",
        assertion_type = "RANGE",
        column         = "quality_score",
        reason         = "quality_score must be 0-100; values outside this range "
                         "indicate an upstream scoring formula bug",
        check_fn       = make_range_check(0, 100),
    ),
    AssertionRule(
        name           = "weight_kg positive",
        assertion_type = "RANGE",
        column         = "weight_kg",
        reason         = "weight_kg must be positive; zero or negative weight is "
                         "physically impossible and corrupts weight analytics",
        check_fn       = make_positive_check(),
    ),
    AssertionRule(
        name           = "shift enum",
        assertion_type = "ENUM",
        column         = "shift",
        reason         = "shift must be MORNING, AFTERNOON, or NIGHT; unknown "
                         "values break shift-based grouping queries",
        check_fn       = make_enum_check(VALID_SHIFTS),
    ),
    AssertionRule(
        name           = "status enum",
        assertion_type = "ENUM",
        column         = "status",
        reason         = "status must be one of the 4 defined production states; "
                         "unknown values create phantom categories in reports",
        check_fn       = make_enum_check(VALID_STATUSES),
    ),
    AssertionRule(
        name           = "production_date not null",
        assertion_type = "NULL_CHECK",
        column         = "production_date",
        reason         = "production_date must be present; null dates break all "
                         "time-series partitioning and period assignment",
        check_fn       = check_not_null,
    ),
]

PARTS_RULES: list[AssertionRule] = [
    AssertionRule(
        name           = "part_number not null",
        assertion_type = "NULL_CHECK",
        column         = "part_number",
        reason         = "part_number is the procurement and recall reference; "
                         "a null means the part cannot be traced or reordered",
        check_fn       = check_not_null,
    ),
    AssertionRule(
        name           = "unit_cost positive",
        assertion_type = "RANGE",
        column         = "unit_cost",
        reason         = "unit_cost must be positive; zero or negative cost "
                         "corrupts cost-of-goods analytics and total_cost derivation",
        check_fn       = make_positive_check(),
    ),
    AssertionRule(
        name           = "quantity minimum",
        assertion_type = "RANGE",
        column         = "quantity",
        reason         = "quantity must be >= 1; zero-quantity BOM lines inflate "
                         "part counts without contributing to vehicle cost",
        check_fn       = make_min_check(1),
    ),
    AssertionRule(
        name           = "currency enum",
        assertion_type = "ENUM",
        column         = "currency",
        reason         = "currency must be a recognised ISO code; unknown values "
                         "corrupt total_cost_eur derivation and financial rollups",
        check_fn       = make_enum_check(VALID_CURRENCIES),
    ),
    AssertionRule(
        name           = "defect_flag enum",
        assertion_type = "ENUM",
        column         = "defect_flag",
        reason         = "defect_flag must be 0 or 1; other values mean the flag "
                         "is being misused and defect rates will be wrong",
        check_fn       = make_enum_check({0, 1}),
    ),
    AssertionRule(
        name           = "vehicle_id ref integrity",
        assertion_type = "REF_INTEGRITY",
        column         = "vehicle_id",
        reason         = "vehicle_id must reference a vehicle in the source table; "
                         "orphaned parts are invisible in vehicle-joined reports",
        check_fn       = make_ref_integrity_check(),
        context_keys   = ["ref_set"],
    ),
]

QUALITY_CHECK_RULES: list[AssertionRule] = [
    AssertionRule(
        name           = "pass_fail enum",
        assertion_type = "ENUM",
        column         = "pass_fail",
        reason         = "pass_fail must be PASS or FAIL; other values corrupt "
                         "the is_passed flag and all pass-rate metrics",
        check_fn       = make_enum_check(VALID_PASS_FAIL),
    ),
    AssertionRule(
        name           = "rework_hours non-negative",
        assertion_type = "RANGE",
        column         = "rework_hours",
        reason         = "rework_hours must be >= 0 when present; negative hours "
                         "produce a negative rework_cost_usd in the target",
        check_fn       = make_min_check(0),
    ),
    AssertionRule(
        name           = "vehicle_id ref integrity",
        assertion_type = "REF_INTEGRITY",
        column         = "vehicle_id",
        reason         = "vehicle_id must reference a vehicle in the source table; "
                         "orphaned QC records cannot be attributed to any vehicle",
        check_fn       = make_ref_integrity_check(),
        context_keys   = ["ref_set"],
    ),
    AssertionRule(
        name           = "test_type enum",
        assertion_type = "ENUM",
        column         = "test_type",
        reason         = "test_type must be one of the 5 defined categories; "
                         "unknown types create spurious defect-rate buckets",
        check_fn       = make_enum_check(VALID_TEST_TYPES),
    ),
]

# Registry: maps table name -> (pk_col, rules_list)
TABLE_REGISTRY: dict[str, tuple[str, list[AssertionRule]]] = {
    "suppliers":      ("supplier_id", SUPPLIER_RULES),
    "vehicles":       ("vehicle_id",  VEHICLE_RULES),
    "parts":          ("part_id",     PARTS_RULES),
    "quality_checks": ("check_id",    QUALITY_CHECK_RULES),
}


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION MODE 1 — all tables, all rows
# ─────────────────────────────────────────────────────────────────────────────

def run_all_tables(
    frames: dict[str, pd.DataFrame],
    write_report: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Run all source assertions across all four tables.

    Parameters
    ----------
    frames : dict with keys "suppliers", "vehicles", "parts", "quality_checks".
        Raw DataFrames from extractor.extract_all().
    write_report : bool, default True
        Write the Excel failure report if any failures exist.

    Returns
    -------
    dict with the same 4 keys.
    Each value is a DataFrame containing only rows that passed all rules.

    Notes
    -----
    - Referential integrity on parts and quality_checks is validated against
      the full vehicle_id set extracted in this batch.
    - For incremental runs where parts reference vehicles from previous batches,
      pass a pre-fetched all_vehicle_ids set via the context mechanism of
      run_one_table() instead.
    """
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("=" * 60)
    log.info("Source assertions started  [%s]  tables=%d", run_ts, len(frames))
    log.info("=" * 60)

    # Build the vehicle_id reference set from this batch's vehicles DataFrame.
    # For incremental runs where parts may reference vehicles loaded in prior
    # batches, callers should use run_one_table() with an explicit ref_set.
    all_vehicle_ids = set(frames["vehicles"]["vehicle_id"])

    collectors: dict[str, FailureCollector] = {}
    clean: dict[str, pd.DataFrame] = {}

    for table_name, df in frames.items():
        pk_col, rules = TABLE_REGISTRY[table_name]
        fc = FailureCollector(pk_col=pk_col, table_name=table_name)

        # Supply the vehicle ref_set to tables that need it
        ctx: dict[str, Any] = {}
        if table_name in ("parts", "quality_checks"):
            ctx["ref_set"] = all_vehicle_ids

        clean_df = run_rules_on_dataframe(df, pk_col, rules, fc, ctx)

        collectors[table_name] = fc
        clean[table_name]      = clean_df

        log.info(
            "  %-16s : %6d in  |  %6d clean  |  %6d failed  |  %d failure records",
            table_name, len(df), len(clean_df), fc.failed_row_count, fc.failure_count,
        )

    _log_summary(collectors)

    if write_report:
        path = write_failure_report(collectors, run_ts, prefix="source_failures")
        if path:
            log.info("Failure report: %s", path)

    log.info("Source assertions complete")
    log.info("=" * 60)
    return clean


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION MODE 2 — one table, all rows
# ─────────────────────────────────────────────────────────────────────────────

def run_one_table(
    table_name: str,
    df: pd.DataFrame,
    ref_set: Optional[set] = None,
    write_report: bool = True,
) -> tuple[pd.DataFrame, FailureCollector]:
    """
    Run all assertion rules for one table against its full DataFrame.

    Useful for:
    - Partial recovery runs (re-validate only the previously failed rows
      after source data has been corrected).
    - Incremental batches where you already have a pre-fetched ref_set
      from a previous full-load run.
    - Testing individual tables in isolation.

    Parameters
    ----------
    table_name   : one of "suppliers", "vehicles", "parts", "quality_checks".
    df           : raw DataFrame for that table.
    ref_set      : set of valid vehicle_ids for REF_INTEGRITY checks.
                   Required for "parts" and "quality_checks".
                   Ignored for "suppliers" and "vehicles".
    write_report : bool, default True.

    Returns
    -------
    (clean_df, FailureCollector)
        clean_df contains only rows that passed all rules.
        FailureCollector holds the failure details for this table.

    Raises
    ------
    ValueError if table_name is not in the registry.
    """
    if table_name not in TABLE_REGISTRY:
        raise ValueError(
            f"Unknown table '{table_name}'. "
            f"Valid options: {sorted(TABLE_REGISTRY.keys())}"
        )

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pk_col, rules = TABLE_REGISTRY[table_name]
    fc = FailureCollector(pk_col=pk_col, table_name=table_name)

    ctx: dict[str, Any] = {}
    if ref_set is not None:
        ctx["ref_set"] = ref_set

    clean_df = run_rules_on_dataframe(df, pk_col, rules, fc, ctx)

    log.info(
        "run_one_table('%s'): %d in | %d clean | %d failed | %d failure records",
        table_name, len(df), len(clean_df), fc.failed_row_count, fc.failure_count,
    )

    if write_report and fc.failure_count > 0:
        path = write_failure_report(
            {table_name: fc}, run_ts, prefix=f"source_failures_{table_name}"
        )
        if path:
            log.info("Failure report: %s", path)

    return clean_df, fc


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION MODE 3 — one table, one row
# ─────────────────────────────────────────────────────────────────────────────

def run_one_row(
    table_name: str,
    row: dict,
    ref_set: Optional[set] = None,
) -> tuple[Optional[dict], list[dict]]:
    """
    Run all assertion rules for one table against a single row dict.

    Designed for:
    - Ad-hoc validation of a specific record before inserting it.
    - Streaming / event-driven pipelines where rows arrive one at a time.
    - Random spot-checks requested by the team at any point.
    - Unit testing individual rows with controlled data.

    Parameters
    ----------
    table_name : one of "suppliers", "vehicles", "parts", "quality_checks".
    row        : dict mapping column_name -> value for one data row.
                 The primary key column must be present.
    ref_set    : set of valid vehicle_ids for REF_INTEGRITY checks.
                 Required when table_name is "parts" or "quality_checks".

    Returns
    -------
    (clean_row, failures)

    clean_row : dict — the original row dict if ALL rules pass, else None.
    failures  : list[dict] — one dict per failed rule, with keys:
                    primary_key, column_name, assertion_type, reason, raw_value.
                Empty list when the row is clean.

    Note on UNIQUENESS rules
    ------------------------
    UNIQUENESS rules require the full column to check for duplicates across
    the batch, which is not available for a single row.  When run_one_row()
    encounters a UNIQUENESS rule it SKIPS it and logs a warning.
    Uniqueness should always be validated at the DataFrame level
    (run_one_table or run_all_tables) before or after single-row checks.

    Examples
    --------
    row = {"vehicle_id": 42, "vin": "ABCDEFGHIJKLMNOPQ", ...}
    clean, failures = run_one_row("vehicles", row)

    if clean:
        transform_and_load(clean)
    else:
        for f in failures:
            print(f["column_name"], f["reason"])
    """
    if table_name not in TABLE_REGISTRY:
        raise ValueError(
            f"Unknown table '{table_name}'. "
            f"Valid options: {sorted(TABLE_REGISTRY.keys())}"
        )

    pk_col, rules = TABLE_REGISTRY[table_name]
    fc = FailureCollector(pk_col=pk_col, table_name=table_name)

    ctx: dict[str, Any] = {"row": row}
    if ref_set is not None:
        ctx["ref_set"] = ref_set

    # Filter out UNIQUENESS rules — they require the full batch
    applicable_rules = []
    for rule in rules:
        if rule.assertion_type == "UNIQUENESS":
            log.warning(
                "run_one_row('%s'): skipping UNIQUENESS rule '%s' — "
                "uniqueness cannot be validated on a single row; "
                "validate at the table level instead.",
                table_name, rule.name,
            )
        else:
            applicable_rules.append(rule)

    passed = run_rules_on_row(row, pk_col, applicable_rules, fc, ctx)

    if passed:
        return row, []
    return None, fc.to_dataframe().to_dict(orient="records")


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _log_summary(collectors: dict[str, FailureCollector]) -> None:
    total_in      = 0
    total_failed  = 0
    total_records = 0
    log.info("-" * 60)
    log.info("SOURCE ASSERTION SUMMARY")
    for name, fc in collectors.items():
        log.info(
            "  %-16s  failed rows: %d  |  failure records: %d",
            name, fc.failed_row_count, fc.failure_count,
        )
        total_failed  += fc.failed_row_count
        total_records += fc.failure_count
    log.info("  TOTAL  failed rows: %d  |  failure records: %d",
             total_failed, total_records)
    log.info("-" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import logging as _logging
    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    from utils.extractor import extract_all, get_last_watermark
    from utils.db_connector import get_connection

    watermark_arg = sys.argv[1] if len(sys.argv) > 1 else None
    conn          = get_connection("source")
    try:
        frames = extract_all(conn, watermark=watermark_arg)
        clean  = run_all_tables(frames)
        print("\nClean row counts ready for transformation:")
        for name, df in clean.items():
            print(f"  {name:<20} {len(df):,} rows")
    finally:
        conn.close()
