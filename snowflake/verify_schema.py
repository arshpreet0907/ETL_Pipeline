"""
verify_schema.py
----------------
Verifies that the Snowflake target schema matches what the pipeline
expects — columns, nullability, and data types — for all 5 tables.

Run
---
    python snowflake/verify_schema.py

Output
------
Prints a PASS / FAIL report per table. Exits with code 1 if any
mismatch is found so it can be used as a pre-flight check.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
from sf_connector import get_snowflake_engine

# ─────────────────────────────────────────────────────────────────────────────
# EXPECTED SCHEMA
# Source of truth: transform.py output columns + target_assertions.py rules
# + MySQL DDL (target_tables.sql) translated to Snowflake types.
#
# Each entry: (snowflake_data_type_prefix, nullable)
#   nullable = True  → column may be NULL
#   nullable = False → column must be NOT NULL
#
# Type prefix is matched as a startswith() against information_schema
# DATA_TYPE so e.g. "NUMBER" matches "NUMBER(5,2)" and plain "NUMBER".
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED: dict[str, dict[str, tuple[str, bool]]] = {

    "vehicles": {
        "vehicle_sk":           ("NUMBER",        False),
        "src_vehicle_id":       ("NUMBER",        False),
        "vin_number":           ("TEXT",          False),
        "model_code":           ("TEXT",          False),
        "model_variant_name":   ("TEXT",          False),
        "color_code":           ("TEXT",          False),
        "engine_type":          ("TEXT",          False),
        "manufacturing_plant":  ("TEXT",          False),
        "production_date":      ("DATE",          False),
        "production_year":      ("NUMBER",        False),
        "production_month":     ("NUMBER",        False),
        "production_shift":     ("TEXT",          False),
        "production_status":    ("TEXT",          False),
        "quality_score":        ("NUMBER",        False),
        "quality_tier":         ("TEXT",          False),
        "gross_weight_kg":      ("NUMBER",        False),
        "weight_category":      ("TEXT",          False),
        "is_electric_vehicle":  ("NUMBER",        False),
        "created_at":           ("TIMESTAMP_NTZ", False),
        "dw_inserted_at":       ("TIMESTAMP_NTZ", False),
        "dw_updated_at":        ("TIMESTAMP_NTZ", True),
    },

    "suppliers": {
        "supplier_sk":              ("NUMBER",        False),
        "supplier_id":              ("NUMBER",        False),
        "supplier_code":            ("TEXT",          False),
        "supplier_name":            ("TEXT",          False),
        "country_of_origin":        ("TEXT",          False),
        "supplier_tier":            ("NUMBER",        False),
        "tier_label":               ("TEXT",          False),
        "performance_rating":       ("NUMBER",        False),
        "contract_start_date":      ("DATE",          False),
        "contract_end_date":        ("DATE",          False),
        "contract_duration_days":   ("NUMBER",        False),
        "active_status":            ("TEXT",          False),
        "valid_from":               ("DATE",          False),
        "valid_to":                 ("DATE",          False),
        "is_current":               ("NUMBER",        False),
        "created_at":               ("TIMESTAMP_NTZ", False),
        "dw_inserted_at":           ("TIMESTAMP_NTZ", False),
        "dw_updated_at":            ("TIMESTAMP_NTZ", True),
    },

    "parts": {
        "part_id":           ("NUMBER",        False),
        "vehicle_id":        ("NUMBER",        False),
        "part_number":       ("TEXT",          False),
        "component_name":    ("TEXT",          False),
        "supplier_code":     ("TEXT",          False),
        "quantity_used":     ("NUMBER",        False),
        "unit_cost_eur":     ("NUMBER",        False),
        "total_cost_eur":    ("NUMBER",        False),
        "cost_tier":         ("TEXT",          False),
        "installation_hrs":  ("NUMBER",        False),
        "has_defect_flag":   ("NUMBER",        False),
        "batch_number":      ("TEXT",          False),
        "created_at":        ("TIMESTAMP_NTZ", False),
        "dw_inserted_at":    ("TIMESTAMP_NTZ", False),
        "dw_updated_at":     ("TIMESTAMP_NTZ", True),
    },

    "quality_checks": {
        "qc_id":               ("NUMBER",        False),
        "vehicle_id":          ("NUMBER",        False),
        "inspection_date":     ("DATE",          False),
        "inspection_year":     ("NUMBER",        False),
        "inspector_code":      ("TEXT",          False),
        "inspection_station":  ("TEXT",          False),
        "test_category":       ("TEXT",          False),
        "inspection_result":   ("TEXT",          False),
        "defect_code":         ("TEXT",          True),
        "has_defect":          ("NUMBER",        False),
        "rework_hours":        ("NUMBER",        True),
        "rework_cost_usd":     ("NUMBER",        False),
        "is_passed":           ("NUMBER",        False),
        "created_at":          ("TIMESTAMP_NTZ", False),
        "dw_inserted_at":      ("TIMESTAMP_NTZ", False),
        "dw_updated_at":       ("TIMESTAMP_NTZ", True),
    },

    "etl_run_log": {
        "run_id":         ("NUMBER",        False),
        "run_start":      ("TIMESTAMP_NTZ", False),
        "run_end":        ("TIMESTAMP_NTZ", True),
        "pipeline_name":  ("TEXT",          False),
        "table_name":     ("TEXT",          False),
        "load_type":      ("TEXT",          False),
        "watermark_from": ("TIMESTAMP_NTZ", True),
        "watermark_to":   ("TIMESTAMP_NTZ", True),
        "rows_extracted": ("NUMBER",        True),
        "rows_inserted":  ("NUMBER",        True),
        "rows_updated":   ("NUMBER",        True),
        "rows_failed":    ("NUMBER",        True),
        "status":         ("TEXT",          False),
        "error_message":  ("TEXT",          True),
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# FETCH ACTUAL SCHEMA FROM SNOWFLAKE
# ─────────────────────────────────────────────────────────────────────────────

def fetch_actual_schema(engine) -> dict[str, dict[str, tuple[str, bool]]]:
    """
    Query information_schema.columns for all expected tables.

    Returns
    -------
    dict[table_name -> dict[column_name -> (data_type, is_nullable)]]
    table and column names are lowercased for case-insensitive comparison.
    """
    table_list = ", ".join(f"'{t.upper()}'" for t in EXPECTED)
    query = f"""
        SELECT
            LOWER(table_name)   AS table_name,
            LOWER(column_name)  AS column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_name IN ({table_list})
        ORDER BY table_name, ordinal_position
    """
    df = pd.read_sql(query, engine)

    actual: dict[str, dict[str, tuple[str, bool]]] = {}
    for _, row in df.iterrows():
        tbl = row["table_name"]
        col = row["column_name"]
        dtype = row["data_type"].upper()
        nullable = row["is_nullable"].upper() == "YES"
        actual.setdefault(tbl, {})[col] = (dtype, nullable)

    return actual


# ─────────────────────────────────────────────────────────────────────────────
# COMPARE
# ─────────────────────────────────────────────────────────────────────────────

def _type_matches(actual_type: str, expected_prefix: str) -> bool:
    """
    Snowflake stores VARCHAR as TEXT in information_schema.
    Match by prefix so NUMBER(5,2) satisfies expected NUMBER.
    """
    return actual_type.startswith(expected_prefix)


def verify(actual: dict, expected: dict) -> dict[str, list[str]]:
    """
    Compare actual schema against expected.

    Returns
    -------
    dict[table_name -> list[issue strings]]
    Empty list means the table passed.
    """
    issues: dict[str, list[str]] = {}

    for table, exp_cols in expected.items():
        table_issues: list[str] = []
        act_cols = actual.get(table, {})

        if not act_cols:
            table_issues.append("TABLE NOT FOUND in Snowflake")
            issues[table] = table_issues
            continue

        # Missing columns
        for col in exp_cols:
            if col not in act_cols:
                table_issues.append(f"MISSING column: {col}")

        # Extra columns (informational only)
        for col in act_cols:
            if col not in exp_cols:
                table_issues.append(f"EXTRA column (unexpected): {col}")

        # Type and nullability mismatches
        for col, (exp_type, exp_nullable) in exp_cols.items():
            if col not in act_cols:
                continue  # already reported as missing
            act_type, act_nullable = act_cols[col]

            if not _type_matches(act_type, exp_type):
                table_issues.append(
                    f"TYPE MISMATCH  {col}: expected {exp_type}, got {act_type}"
                )
            if act_nullable != exp_nullable:
                exp_null_str = "NULL" if exp_nullable else "NOT NULL"
                act_null_str = "NULL" if act_nullable else "NOT NULL"
                table_issues.append(
                    f"NULLABILITY MISMATCH  {col}: expected {exp_null_str}, got {act_null_str}"
                )

        if table_issues:
            issues[table] = table_issues

    return issues


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_report(issues: dict[str, list[str]]) -> bool:
    """
    Print a human-readable report.

    Returns
    -------
    True if all tables passed, False if any issues found.
    """
    all_passed = True
    print()
    print("=" * 60)
    print("  SNOWFLAKE SCHEMA VERIFICATION REPORT")
    print("=" * 60)

    for table in EXPECTED:
        table_issues = issues.get(table, [])
        if not table_issues:
            print(f"  PASS  {table}")
        else:
            all_passed = False
            print(f"  FAIL  {table}")
            for issue in table_issues:
                print(f"          {issue}")

    print("=" * 60)
    if all_passed:
        print("  Result: ALL TABLES PASSED")
    else:
        print("  Result: SCHEMA MISMATCHES FOUND — fix before running pipeline")
    print("=" * 60)
    print()
    return all_passed


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = get_snowflake_engine()
    actual = fetch_actual_schema(engine)
    issues = verify(actual, EXPECTED)
    passed = print_report(issues)
    sys.exit(0 if passed else 1)
