"""
target_assertions.py
--------------------
Post-migration data quality assertions for the vehicle manufacturing
data warehouse (vehicle_manufacturing_dw).

Validates that data in the target DB is correct AFTER the transform and
load stages have run.  Checks two categories of things:

1.  Structural correctness — columns that should never be null are not
    null; derived columns exist and contain values in the expected shape.

2.  Transformation correctness — derived column values match the
    expected computation from their source columns (e.g. model_variant_name
    = model_code + '_' + variant, is_electric_vehicle = 1 iff EV_MOTOR).

What this module does NOT check
--------------------------------
Row-count parity between source and target is important but belongs in a
dedicated reconciliation step that compares source and target counts
directly.  Post-migration assertions here validate the TARGET in isolation.

Three execution modes
----------------------
Identical contract to source_assertions.py:

    1.  run_all_tables(frames)           — all tables, all rows
    2.  run_one_table(table_name, df)    — one table, all rows
    3.  run_one_row(table_name, row)     — one table, one row

All three write to the same FailureCollector / Excel report infrastructure.
All three skip UNIQUENESS rules in single-row mode for the same reason as
the source module.

Usage examples
--------------
    from target_assertions import run_all_tables, run_one_table, run_one_row

    # Mode 1 — after a full load run
    frames = extract_target_tables(target_engine)   # your own extractor
    clean  = run_all_tables(frames)
    # clean["vehicles"] -> rows that passed all post-migration checks

    # Mode 2 — validate one target table
    clean_df, fc = run_one_table("vehicles", df_vehicles_target)
    print(fc.failure_count, "post-migration failures in vehicles")

    # Mode 3 — spot-check one row
    row = target_engine.execute("SELECT * FROM vehicles WHERE vehicle_sk=1")
    clean_row, failures = run_one_row("vehicles", row)
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
    make_enum_check,
    make_str_length_check,
    make_ref_integrity_check,
    make_derived_check,
    run_rules_on_row,
    run_rules_on_dataframe,
    write_failure_report,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# VALID VALUE SETS  (target-side — some differ from source)
# ─────────────────────────────────────────────────────────────────────────────

VALID_QUALITY_TIERS  = {"PREMIUM", "STANDARD", "ECONOMY", "SUBSTANDARD"}
VALID_WEIGHT_CATS    = {"HEAVY", "LIGHT"}
VALID_COST_TIERS     = {"HIGH_VALUE", "MID_VALUE", "LOW_VALUE"}
VALID_ACTIVE_STATUS  = {"ACTIVE", "INACTIVE"}
VALID_TIER_LABELS    = {"STRATEGIC", "PREFERRED", "APPROVED"}


# ─────────────────────────────────────────────────────────────────────────────
# DERIVED VALUE COMPUTERS
# Each is a pure function: row_dict -> expected_value.
# Used by make_derived_check() to build check functions.
# ─────────────────────────────────────────────────────────────────────────────

def _expected_model_variant(row: dict) -> str:
    return f"{row.get('model_code', '')}_{row.get('variant', '') if 'variant' in row else ''}"


def _expected_quality_tier(row: dict) -> str:
    qs = float(row.get("quality_score", 0) or 0)
    if qs >= 90:  return "PREMIUM"
    if qs >= 75:  return "STANDARD"
    if qs >= 60:  return "ECONOMY"
    return "SUBSTANDARD"


def _expected_weight_category(row: dict) -> str:
    wt = float(row.get("gross_weight_kg", 0) or 0)
    return "HEAVY" if wt > 2500 else "LIGHT"


def _expected_is_ev(row: dict) -> int:
    return 1 if row.get("engine_type") == "EV_MOTOR" else 0


def _expected_production_year(row: dict) -> int:
    return pd.to_datetime(row.get("production_date")).year


def _expected_production_month(row: dict) -> int:
    return pd.to_datetime(row.get("production_date")).month


def _expected_total_cost(row: dict) -> float:
    return round(float(row.get("quantity_used", 0) or 0) *
                 float(row.get("unit_cost_eur",  0) or 0), 2)


def _expected_cost_tier(row: dict) -> str:
    # Cost tier is based on TOTAL cost, not unit cost
    cost = float(row.get("total_cost_eur", 0) or 0)
    if cost > 2000:  return "HIGH_VALUE"
    if cost > 500:   return "MID_VALUE"
    return "LOW_VALUE"


def _expected_installation_hrs(row: dict) -> float:
    return round(float(row.get("install_time_min", 0) or 0) / 60, 2)


def _expected_rework_cost(row: dict) -> float:
    raw = row.get("rework_hours")
    # NULL rework_hours means no rework was done → cost is 0.0
    hours = 0.0 if (raw is None or (isinstance(raw, float) and raw != raw)) else float(raw)
    return round(hours * 85, 2)


def _expected_is_passed(row: dict) -> int:
    return 1 if row.get("pass_fail") == "PASS" else 0


def _expected_has_defect(row: dict) -> int:
    val = row.get("defect_code")
    # Treat Python None AND the string "None" AND NaN as "no defect"
    if val is None:
        return 0
    if isinstance(val, float) and val != val:   # NaN
        return 0
    if str(val).strip().lower() == "none":
        return 0
    return 1


def _expected_tier_label(row: dict) -> str:
    return {1: "STRATEGIC", 2: "PREFERRED", 3: "APPROVED"}.get(
        int(row.get("supplier_tier", 0) or 0), ""
    )


def _expected_active_status(row: dict) -> str:
    return "ACTIVE" if int(row.get("is_active", 0) or 0) == 1 else "INACTIVE"


def _expected_contract_duration(row: dict) -> int:
    try:
        start = pd.to_datetime(row.get("contract_start_date"))
        end   = pd.to_datetime(row.get("contract_end_date"))
        return (end - start).days
    except Exception:
        return -1


# ─────────────────────────────────────────────────────────────────────────────
# RULE REGISTRY — TARGET TABLES
# ─────────────────────────────────────────────────────────────────────────────

TARGET_VEHICLES_RULES: list[AssertionRule] = [

    # ── Structural: key columns must not be null ───────────────────────────
    AssertionRule(
        name="vehicle_sk not null",
        assertion_type="NOT_NULL_TARGET",
        column="vehicle_sk",
        reason="vehicle_sk is the DW surrogate key and must always be "
               "populated by the load process",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="src_vehicle_id not null",
        assertion_type="NOT_NULL_TARGET",
        column="src_vehicle_id",
        reason="src_vehicle_id must be present for source-to-target lineage "
               "tracing and partial-recovery reloads",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="vin_number not null",
        assertion_type="NOT_NULL_TARGET",
        column="vin_number",
        reason="vin_number is the legal global vehicle identifier and must "
               "be present in the DW",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="vin_number length",
        assertion_type="RANGE",
        column="vin_number",
        reason="vin_number must be exactly 17 characters per ISO 3779",
        check_fn=make_str_length_check(17),
    ),
    AssertionRule(
        name="dw_inserted_at not null",
        assertion_type="NOT_NULL_TARGET",
        column="dw_inserted_at",
        reason="dw_inserted_at must be populated by the load process to "
               "support audit and replay of any ETL run",
        check_fn=check_not_null,
    ),

    # ── Transformation correctness: derived columns ────────────────────────
    AssertionRule(
        name="quality_tier derived correctly",
        assertion_type="DERIVED_CHECK",
        column="quality_tier",
        reason="quality_tier must match the banded scoring rule: >=90 "
               "PREMIUM, >=75 STANDARD, >=60 ECONOMY, else SUBSTANDARD",
        check_fn=make_derived_check(_expected_quality_tier),
        context_keys=["row"],
    ),
    AssertionRule(
        name="weight_category derived correctly",
        assertion_type="DERIVED_CHECK",
        column="weight_category",
        reason="weight_category must be HEAVY when gross_weight_kg > 2500 "
               "and LIGHT otherwise",
        check_fn=make_derived_check(_expected_weight_category),
        context_keys=["row"],
    ),
    AssertionRule(
        name="is_electric_vehicle derived correctly",
        assertion_type="DERIVED_CHECK",
        column="is_electric_vehicle",
        reason="is_electric_vehicle must be 1 when engine_type is EV_MOTOR "
               "and 0 for all other engine types",
        check_fn=make_derived_check(_expected_is_ev),
        context_keys=["row"],
    ),
    AssertionRule(
        name="production_year derived correctly",
        assertion_type="DERIVED_CHECK",
        column="production_year",
        reason="production_year must equal YEAR(production_date)",
        check_fn=make_derived_check(_expected_production_year),
        context_keys=["row"],
    ),
    AssertionRule(
        name="production_month derived correctly",
        assertion_type="DERIVED_CHECK",
        column="production_month",
        reason="production_month must equal MONTH(production_date)",
        check_fn=make_derived_check(_expected_production_month),
        context_keys=["row"],
    ),
    AssertionRule(
        name="quality_tier enum",
        assertion_type="ENUM",
        column="quality_tier",
        reason="quality_tier must be one of the 4 defined tiers; other "
               "values indicate the derivation logic was applied incorrectly",
        check_fn=make_enum_check(VALID_QUALITY_TIERS),
    ),
    AssertionRule(
        name="weight_category enum",
        assertion_type="ENUM",
        column="weight_category",
        reason="weight_category must be HEAVY or LIGHT only",
        check_fn=make_enum_check(VALID_WEIGHT_CATS),
    ),
]

TARGET_PARTS_RULES: list[AssertionRule] = [

    # ── Structural ─────────────────────────────────────────────────────────
    AssertionRule(
        name="component_name not null",
        assertion_type="NOT_NULL_TARGET",
        column="component_name",
        reason="component_name (renamed from part_name) must always be "
               "populated; null indicates the rename mapping failed",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="dw_inserted_at not null",
        assertion_type="NOT_NULL_TARGET",
        column="dw_inserted_at",
        reason="dw_inserted_at must be populated by the load process",
        check_fn=check_not_null,
    ),

    # ── Transformation correctness ─────────────────────────────────────────
    AssertionRule(
        name="total_cost_eur derived correctly",
        assertion_type="DERIVED_CHECK",
        column="total_cost_eur",
        reason="total_cost_eur must equal quantity_used * unit_cost_eur "
               "rounded to 2 decimal places",
        check_fn=make_derived_check(_expected_total_cost, tolerance=0.01),
        context_keys=["row"],
    ),
    AssertionRule(
        name="cost_tier derived correctly",
        assertion_type="DERIVED_CHECK",
        column="cost_tier",
        reason="cost_tier must be HIGH_VALUE when unit_cost_eur > 2000, "
               "MID_VALUE when > 500, LOW_VALUE otherwise",
        check_fn=make_derived_check(_expected_cost_tier),
        context_keys=["row"],
    ),
    AssertionRule(
        name="cost_tier enum",
        assertion_type="ENUM",
        column="cost_tier",
        reason="cost_tier must be one of HIGH_VALUE, MID_VALUE, LOW_VALUE",
        check_fn=make_enum_check(VALID_COST_TIERS),
    ),
    AssertionRule(
        name="unit_cost_eur positive",
        assertion_type="RANGE",
        column="unit_cost_eur",
        reason="unit_cost_eur must be positive in the target as it was in "
               "the source; a zero or negative value indicates a load error",
        check_fn=make_positive_check(),
    ),
    AssertionRule(
        name="quantity_used minimum",
        assertion_type="RANGE",
        column="quantity_used",
        reason="quantity_used must be >= 1 in the target",
        check_fn=lambda v, _: v is None or float(v) >= 1,
    ),
]

TARGET_SUPPLIERS_RULES: list[AssertionRule] = [

    # ── Structural ─────────────────────────────────────────────────────────
    AssertionRule(
        name="supplier_sk not null",
        assertion_type="NOT_NULL_TARGET",
        column="supplier_sk",
        reason="supplier_sk is the SCD2 surrogate key and must always be "
               "populated",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="dw_inserted_at not null",
        assertion_type="NOT_NULL_TARGET",
        column="dw_inserted_at",
        reason="dw_inserted_at must be populated by the load process",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="is_current valid",
        assertion_type="ENUM",
        column="is_current",
        reason="is_current must be 0 or 1; other values indicate the SCD2 "
               "logic was applied incorrectly",
        check_fn=make_enum_check({0, 1}),
    ),
    AssertionRule(
        name="valid_to not null",
        assertion_type="NOT_NULL_TARGET",
        column="valid_to",
        reason="valid_to must always be set; current rows use 9999-12-31 "
               "and historical rows use their actual expiry date",
        check_fn=check_not_null,
    ),

    # ── Transformation correctness ─────────────────────────────────────────
    AssertionRule(
        name="tier_label derived correctly",
        assertion_type="DERIVED_CHECK",
        column="tier_label",
        reason="tier_label must be STRATEGIC for tier 1, PREFERRED for "
               "tier 2, APPROVED for tier 3",
        check_fn=make_derived_check(_expected_tier_label),
        context_keys=["row"],
    ),
    AssertionRule(
        name="active_status derived correctly",
        assertion_type="DERIVED_CHECK",
        column="active_status",
        reason="active_status must be ACTIVE when is_active=1 and "
               "INACTIVE when is_active=0",
        check_fn=make_derived_check(_expected_active_status),
        context_keys=["row"],
    ),
    AssertionRule(
        name="tier_label enum",
        assertion_type="ENUM",
        column="tier_label",
        reason="tier_label must be STRATEGIC, PREFERRED, or APPROVED",
        check_fn=make_enum_check(VALID_TIER_LABELS),
    ),
    AssertionRule(
        name="active_status enum",
        assertion_type="ENUM",
        column="active_status",
        reason="active_status must be ACTIVE or INACTIVE",
        check_fn=make_enum_check(VALID_ACTIVE_STATUS),
    ),
    AssertionRule(
        name="performance_rating range",
        assertion_type="RANGE",
        column="performance_rating",
        reason="performance_rating must be 0-5 in the target as it was in "
               "the source; out-of-range indicates a mapping error",
        check_fn=make_range_check(0, 5),
    ),
]

TARGET_QUALITY_CHECKS_RULES: list[AssertionRule] = [

    # ── Structural ─────────────────────────────────────────────────────────
    AssertionRule(
        name="qc_id not null",
        assertion_type="NOT_NULL_TARGET",
        column="qc_id",
        reason="qc_id (renamed from check_id) must be present; null "
               "indicates the rename mapping failed during load",
        check_fn=check_not_null,
    ),
    AssertionRule(
        name="dw_inserted_at not null",
        assertion_type="NOT_NULL_TARGET",
        column="dw_inserted_at",
        reason="dw_inserted_at must be populated by the load process",
        check_fn=check_not_null,
    ),

    # ── Transformation correctness ─────────────────────────────────────────
    AssertionRule(
        name="is_passed derived correctly",
        assertion_type="DERIVED_CHECK",
        column="is_passed",
        reason="is_passed must be 1 when pass_fail=PASS and 0 when "
               "pass_fail=FAIL; any mismatch indicates a transform error",
        check_fn=make_derived_check(_expected_is_passed),
        context_keys=["row"],
    ),
    AssertionRule(
        name="has_defect derived correctly",
        assertion_type="DERIVED_CHECK",
        column="has_defect",
        reason="has_defect must be 0 when defect_code is null and 1 when "
               "defect_code is populated",
        check_fn=make_derived_check(_expected_has_defect),
        context_keys=["row"],
    ),
    AssertionRule(
        name="rework_cost_usd derived correctly",
        assertion_type="DERIVED_CHECK",
        column="rework_cost_usd",
        reason="rework_cost_usd must equal rework_hours * 85 (standard "
               "labour rate); a mismatch indicates the rate was not applied",
        check_fn=make_derived_check(_expected_rework_cost, tolerance=0.01),
        context_keys=["row"],
    ),
    AssertionRule(
        name="is_passed valid flag",
        assertion_type="ENUM",
        column="is_passed",
        reason="is_passed must be 0 or 1 only",
        check_fn=make_enum_check({0, 1}),
    ),
    AssertionRule(
        name="has_defect valid flag",
        assertion_type="ENUM",
        column="has_defect",
        reason="has_defect must be 0 or 1 only",
        check_fn=make_enum_check({0, 1}),
    ),
    AssertionRule(
        name="rework_cost_usd non-negative",
        assertion_type="RANGE",
        column="rework_cost_usd",
        reason="rework_cost_usd must be >= 0; a negative value indicates "
               "a computation error in the transform stage",
        check_fn=lambda v, _: v is None or float(v) >= 0,
    ),
    AssertionRule(
        name="inspection_year not null",
        assertion_type="NOT_NULL_TARGET",
        column="inspection_year",
        reason="inspection_year must be populated; null indicates the date "
               "decomposition was not applied during transform",
        check_fn=check_not_null,
    ),
]

# Registry: maps table name -> (pk_col, rules_list)
TARGET_TABLE_REGISTRY: dict[str, tuple[str, list[AssertionRule]]] = {
    "vehicles":       ("vehicle_sk",  TARGET_VEHICLES_RULES),
    "parts":          ("part_id",     TARGET_PARTS_RULES),
    "suppliers":      ("supplier_sk", TARGET_SUPPLIERS_RULES),
    "quality_checks": ("qc_id",       TARGET_QUALITY_CHECKS_RULES),
}


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION MODE 1 — all tables, all rows
# ─────────────────────────────────────────────────────────────────────────────

def run_all_tables(
    frames: dict[str, pd.DataFrame],
    source_frames: dict[str, pd.DataFrame] = None,
    write_report: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Run all post-migration assertions across all four target tables.

    Parameters
    ----------
    frames : dict with keys "suppliers", "vehicles", "parts", "quality_checks".
        DataFrames extracted from the target database after load.
    source_frames : dict with the same keys, optional.
        DataFrames from the source database. Required for validating derived
        columns that depend on source values (e.g. active_status from is_active).
        If not provided, derived checks that need source columns will fail.
    write_report : bool, default True.

    Returns
    -------
    dict with the same 4 keys.
    Each value is a DataFrame of rows that passed all post-migration checks.

    A non-empty failure report here means the transform or load stage
    introduced errors that need to be investigated before the migration
    can be signed off.
    """
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log.info("=" * 60)
    log.info("Post-migration assertions started  [%s]", run_ts)
    log.info("=" * 60)

    # Enrich target frames with source columns needed for derived validation
    if source_frames:
        frames = _enrich_target_with_source(frames, source_frames)
        log.info("Target frames enriched with source columns for validation")

    collectors: dict[str, FailureCollector] = {}
    clean: dict[str, pd.DataFrame] = {}

    for table_name, df in frames.items():
        if table_name not in TARGET_TABLE_REGISTRY:
            log.warning("Skipping unknown table '%s'", table_name)
            continue

        pk_col, rules = TARGET_TABLE_REGISTRY[table_name]
        fc = FailureCollector(pk_col=pk_col, table_name=table_name)
        clean_df = run_rules_on_dataframe(df, pk_col, rules, fc, {})

        collectors[table_name] = fc
        clean[table_name]      = clean_df

        log.info(
            "  %-16s : %6d in  |  %6d clean  |  %6d failed  |  %d failure records",
            table_name, len(df), len(clean_df), fc.failed_row_count, fc.failure_count,
        )

    _log_summary(collectors)

    if write_report:
        path = write_failure_report(
            collectors, run_ts, prefix="post_migration_failures"
        )
        if path:
            log.info("Post-migration failure report: %s", path)

    log.info("Post-migration assertions complete")
    log.info("=" * 60)
    return clean


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTION MODE 2 — one table, all rows
# ─────────────────────────────────────────────────────────────────────────────

def run_one_table(
    table_name: str,
    df: pd.DataFrame,
    write_report: bool = True,
) -> tuple[pd.DataFrame, FailureCollector]:
    """
    Run post-migration assertions for one target table.

    Parameters
    ----------
    table_name   : one of "suppliers", "vehicles", "parts", "quality_checks".
    df           : DataFrame extracted from the target table.
    write_report : bool, default True.

    Returns
    -------
    (clean_df, FailureCollector)
    """
    if table_name not in TARGET_TABLE_REGISTRY:
        raise ValueError(
            f"Unknown target table '{table_name}'. "
            f"Valid options: {sorted(TARGET_TABLE_REGISTRY.keys())}"
        )

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    pk_col, rules = TARGET_TABLE_REGISTRY[table_name]
    fc = FailureCollector(pk_col=pk_col, table_name=table_name)
    clean_df = run_rules_on_dataframe(df, pk_col, rules, fc, {})

    log.info(
        "post_migration run_one_table('%s'): %d in | %d clean | %d failed",
        table_name, len(df), len(clean_df), fc.failed_row_count,
    )

    if write_report and fc.failure_count > 0:
        path = write_failure_report(
            {table_name: fc},
            run_ts,
            prefix=f"post_migration_failures_{table_name}",
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
) -> tuple[Optional[dict], list[dict]]:
    """
    Run post-migration assertions for one target table against a single row.

    Designed for:
    - Spot-checks on specific rows after load ("check vehicle_sk 42").
    - Validating a single row before marking it as successfully migrated.
    - Team-requested random audits at any time.

    Parameters
    ----------
    table_name : one of "suppliers", "vehicles", "parts", "quality_checks".
    row        : dict representing one row from the target table.

    Returns
    -------
    (clean_row, failures)
        clean_row : the row dict if all rules pass, else None.
        failures  : list of failure dicts (empty if row is clean).

    Note
    ----
    DERIVED_CHECK rules need the full row dict to compute expected values.
    Ensure the row dict contains ALL columns used by derived value
    computers (e.g. quality_score when checking quality_tier).
    UNIQUENESS rules are skipped for the same reason as in source mode.
    """
    if table_name not in TARGET_TABLE_REGISTRY:
        raise ValueError(
            f"Unknown target table '{table_name}'. "
            f"Valid options: {sorted(TARGET_TABLE_REGISTRY.keys())}"
        )

    pk_col, rules = TARGET_TABLE_REGISTRY[table_name]
    fc = FailureCollector(pk_col=pk_col, table_name=table_name)

    applicable_rules = []
    for rule in rules:
        if rule.assertion_type == "UNIQUENESS":
            log.warning(
                "post_migration run_one_row('%s'): skipping UNIQUENESS rule "
                "'%s' — uniqueness cannot be validated on a single row.",
                table_name, rule.name,
            )
        else:
            applicable_rules.append(rule)

    ctx = {"row": row}
    passed = run_rules_on_row(row, pk_col, applicable_rules, fc, ctx)

    if passed:
        return row, []
    return None, fc.to_dataframe().to_dict(orient="records")


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_target_with_source(
    target_frames: dict[str, pd.DataFrame],
    source_frames: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Enrich target DataFrames with source columns needed for derived validation.

    Target assertions validate that derived columns were computed correctly
    during transformation. Some derived columns are computed from source
    columns that don't exist in the target schema (e.g. active_status is
    derived from is_active, but is_active is not stored in the target).

    This function joins target data with the minimal set of source columns
    needed to validate those derivations.

    Parameters
    ----------
    target_frames : dict[table_name -> target DataFrame]
        Data extracted from the target database after load.
    source_frames : dict[table_name -> source DataFrame]
        Clean data from the source database (output of source_assertions).

    Returns
    -------
    dict[table_name -> enriched DataFrame]
        Target DataFrames with source columns added via left join on PK.

    Notes
    -----
    Enrichment strategy per table:

    - suppliers:
        Add is_active from source to validate active_status derivation.
        Join on: supplier_id

    - vehicles:
        No enrichment needed. All derived columns (quality_tier,
        weight_category, is_electric_vehicle, production_year,
        production_month) are computed from target columns only.

    - parts:
        No enrichment needed. cost_tier is derived from total_cost_eur
        which exists in the target.

    - quality_checks:
        Add pass_fail from source to validate is_passed derivation.
        Join on: qc_id (target) = check_id (source)
    """
    enriched = {}

    # Suppliers: add is_active to validate active_status
    enriched['suppliers'] = target_frames['suppliers'].merge(
        source_frames['suppliers'][['supplier_id', 'is_active']],
        on='supplier_id',
        how='left'
    )

    # Vehicles: all derived validations use target columns only
    enriched['vehicles'] = target_frames['vehicles'].copy()

    # Parts: cost_tier validation uses target total_cost_eur
    enriched['parts'] = target_frames['parts'].copy()

    # Quality checks: add pass_fail to validate is_passed
    enriched['quality_checks'] = target_frames['quality_checks'].merge(
        source_frames['quality_checks'][['check_id', 'pass_fail']],
        left_on='qc_id',
        right_on='check_id',
        how='left'
    ).drop(columns=['check_id'])  # Drop redundant join key

    return enriched


def _log_summary(collectors: dict[str, FailureCollector]) -> None:
    log.info("-" * 60)
    log.info("POST-MIGRATION ASSERTION SUMMARY")
    total_failed  = 0
    total_records = 0
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
