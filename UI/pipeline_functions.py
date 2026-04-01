# UI/pipeline_functions.py
"""
All callable entry points for the Streamlit UI.
Imports directly from project modules — no subprocess involved.

Function naming convention
--------------------------
run_*      : full pipeline (extract → assert → transform → load → post-assert)
assert_*   : assertion only (no load)
*_local    : uses local MySQL source/target via get_engine()
*_snowflake: uses Snowflake via get_snowflake_engine()
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# ── path setup ────────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).parent.parent
_SNOWFLAKE       = _ROOT / "snowflake"
_SNOWFLAKE_FILES = _ROOT / "snowflake_files"

sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_SNOWFLAKE))
sys.path.insert(0, str(_SNOWFLAKE_FILES))

# ── project imports ───────────────────────────────────────────────────────────
from utils.db_connector import get_engine
from utils.extractor import (
    extract_all,
    extract_suppliers,
    extract_vehicles,
    extract_parts,
    extract_quality_checks,
    extract_rows_by_pk,
    get_all_vehicle_ids,
)

from assertions.source_assertions import (
    run_all_tables  as run_all_source_assertions,
    run_one_table   as run_one_source_table,
    run_one_row     as run_one_source_row,
)
from assertions.target_assertions import (
    run_all_tables  as run_all_target_assertions,
    run_one_table   as run_one_target_table,
    run_one_row     as run_one_target_row,
)

import transform
from local_execution import whole_pipeline

TABLES = ["suppliers", "vehicles", "parts", "quality_checks"]

# Per-table extractor map — used to call the right extractor for a single table
_TABLE_EXTRACTOR = {
    "suppliers":      extract_suppliers,
    "vehicles":       extract_vehicles,
    "parts":          extract_parts,
    "quality_checks": extract_quality_checks,
}

# Per-table transform map — used to call the right transform for a single table
_TABLE_TRANSFORMER = {
    "suppliers":      transform.transform_suppliers,
    "vehicles":       transform.transform_vehicles,
    "parts":          transform.transform_parts,
    "quality_checks": transform.transform_quality_checks,
}

# Tables that need a vehicle_id ref_set for source REF_INTEGRITY assertions
_NEEDS_REF_SET = {"parts", "quality_checks"}

# Primary key column per table — source DB
_SOURCE_PK = {
    "suppliers":      "supplier_id",
    "vehicles":       "vehicle_id",
    "parts":          "part_id",
    "quality_checks": "check_id",
}

# Primary key column per table — local target DB
_TARGET_PK = {
    "suppliers":      "supplier_sk",
    "vehicles":       "vehicle_sk",
    "parts":          "part_id",
    "quality_checks": "qc_id",
}

# For row-level target queries: suppliers/vehicles use a source-side FK
# column in the target (not the surrogate key) because the user enters
# the source PK value from the UI.
_TARGET_ROW_LOOKUP_COL = {
    "suppliers":      "supplier_id",    # source PK stored in target
    "vehicles":       "src_vehicle_id", # source PK stored in target
    "parts":          "part_id",        # same PK in both
    "quality_checks": "qc_id",          # same PK in both
}

# Required columns per table per side — used for schema validation
_REQUIRED_SOURCE_COLS = {
    "suppliers":      {"supplier_id", "supplier_code", "rating", "tier", "is_active",
                       "contract_start", "contract_end"},
    "vehicles":       {"vehicle_id", "vin", "quality_score", "weight_kg", "shift",
                       "status", "production_date"},
    "parts":          {"part_id", "part_number", "unit_cost", "quantity", "currency",
                       "defect_flag", "vehicle_id"},
    "quality_checks": {"check_id", "pass_fail", "rework_hours", "vehicle_id", "test_type"},
}

_REQUIRED_TARGET_COLS = {
    "suppliers":      {"supplier_sk", "dw_inserted_at", "is_current", "valid_to",
                       "tier_label", "active_status", "performance_rating",
                       "is_active"},          # enriched from source for DERIVED_CHECK
    "vehicles":       {"vehicle_sk", "src_vehicle_id", "vin_number", "dw_inserted_at",
                       "quality_tier", "weight_category", "is_electric_vehicle",
                       "production_year", "production_month"},
    "parts":          {"part_id", "component_name", "dw_inserted_at", "total_cost_eur",
                       "cost_tier", "unit_cost_eur", "quantity_used"},
    "quality_checks": {"qc_id", "dw_inserted_at", "is_passed", "has_defect",
                       "rework_cost_usd", "inspection_year",
                       "pass_fail"},           # enriched from source for DERIVED_CHECK
}

# Primary key column per table — Snowflake target (uppercase)
_SNOWFLAKE_PK = {
    "suppliers":      "SUPPLIER_SK",
    "vehicles":       "VEHICLE_SK",
    "parts":          "PART_ID",
    "quality_checks": "QC_ID",
}

# Snowflake row lookup: use source-side FK columns (uppercase) so the user
# can enter source PK values — same rationale as _TARGET_ROW_LOOKUP_COL.
_SNOWFLAKE_ROW_LOOKUP_COL = {
    "suppliers":      "SUPPLIER_ID",     # source PK stored in Snowflake target
    "vehicles":       "SRC_VEHICLE_ID",  # source PK stored in Snowflake target
    "parts":          "PART_ID",
    "quality_checks": "QC_ID",
}

log = logging.getLogger(__name__)


# ── Streamlit log handler ─────────────────────────────────────────────────────

class StreamlitLogHandler(logging.Handler):
    """
    Attaches to the root logger and streams every log record into a
    Streamlit st.empty() widget in real time.

    Usage:
        log_box = st.empty()
        handler = attach_handler(log_box)
        try:
            some_pipeline_function()
        finally:
            detach_handler(handler)
    """
    def __init__(self, widget_update_fn):
        super().__init__()
        self.widget_update_fn = widget_update_fn
        self.logs: list[str] = []

    def emit(self, record):
        msg = self.format(record)
        self.logs.append(msg)
        self.widget_update_fn("\n".join(self.logs))


def attach_handler(log_box) -> StreamlitLogHandler:
    handler = StreamlitLogHandler(lambda text: log_box.code(text, language="log"))
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    )
    logging.getLogger().addHandler(handler)
    return handler


def detach_handler(handler: StreamlitLogHandler):
    logging.getLogger().removeHandler(handler)


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_ref_set(table_name: str, source_engine) -> set | None:
    """
    Return all vehicle_ids from source if the table needs a ref_set,
    otherwise None. Used for source REF_INTEGRITY assertions on parts
    and quality_checks.
    """
    if table_name in _NEEDS_REF_SET:
        return get_all_vehicle_ids(source_engine)
    return None


def _enrich_for_target_one_table(
    table_name: str,
    target_df: pd.DataFrame,
    source_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Manually apply the same enrichment that run_all_target_assertions does
    internally, but for a single table. Required because run_one_table in
    target_assertions does not accept source_frames.

    Mirrors _enrich_target_with_source() from target_assertions.py:
    - suppliers : left join is_active from source on supplier_id
    - quality_checks: left join pass_fail from source on qc_id = check_id
    - vehicles, parts: no enrichment needed
    """
    if table_name == "suppliers":
        return target_df.merge(
            source_df[["supplier_id", "is_active"]],
            on="supplier_id",
            how="left",
        )
    if table_name == "quality_checks":
        return target_df.merge(
            source_df[["check_id", "pass_fail"]],
            left_on="qc_id",
            right_on="check_id",
            how="left",
        ).drop(columns=["check_id"])
    # vehicles and parts need no enrichment
    return target_df.copy()


def _get_snowflake_engine():
    """Deferred import so missing Snowflake deps don't break local-only usage."""
    from snowflake_files.sf_connector import get_snowflake_engine
    return get_snowflake_engine()


_TABLE_CSVS_DIR = _ROOT / "snowflake_files" / "table_csvs"


def fetch_table_csv(
    table_name: str,
    watermark_from: str = None,
    watermark_to: str = None,
) -> str:
    """
    Fetch source data for one table from MySQL and save to
    snowflake_files/table_csvs/<table_name>.csv.

    Returns the absolute path to the written CSV.
    """
    from snowflake_files.get_table_csvs import fetch_and_save
    fetch_and_save(table_name, watermark_from=watermark_from, watermark_to=watermark_to)
    path = str(_TABLE_CSVS_DIR / f"{table_name}.csv")
    log.info("Table CSV ready: %s", path)
    return path


def fetch_all_csvs(
    watermark_from: str = None,
    watermark_to: str = None,
) -> None:
    """
    Fetch all four source tables from MySQL and save each to
    snowflake_files/table_csvs/<table_name>.csv.
    """
    from snowflake_files.get_table_csvs import fetch_and_save
    for table in TABLES:
        fetch_and_save(table, watermark_from=watermark_from, watermark_to=watermark_to)
    log.info("All table CSVs saved to %s", _TABLE_CSVS_DIR)


_SOURCE_EXCEL = _SNOWFLAKE_FILES / "source_data.xlsx"


def fetch_source_excel(
    watermark_from: str = None,
    watermark_to: str = None,
) -> str:
    """
    Fetch all four source tables from MySQL and write to
    snowflake_files/source_data.xlsx (one sheet per table).
    Returns the absolute path to the written file.
    """
    from snowflake_files.get_source_data import fetch_and_save
    fetch_and_save(watermark_from=watermark_from, watermark_to=watermark_to)
    log.info("Source Excel ready: %s", _SOURCE_EXCEL)
    return str(_SOURCE_EXCEL)


# ─────────────────────────────────────────────────────────────────────────────
# ALL TABLES — LOCAL
# ─────────────────────────────────────────────────────────────────────────────

def run_all_tables_local(watermark_from: str = None, watermark_to: str = None):
    """
    Full pipeline across all tables using local DBs.
    Delegates to whole_pipeline() in local_execution.py.
    """
    whole_pipeline(watermark_from=watermark_from, watermark_to=watermark_to)


def assert_all_tables_local(
    mode: str,
    watermark_from: str = None,
    watermark_to: str = None,
):
    """
    Run source and/or target assertions across all tables from local DBs.
    mode: "source" | "target" | "both"
    """
    source_engine = get_engine("source")
    frames = extract_all(source_engine, watermark_from=watermark_from, watermark_to=watermark_to)

    if mode in ("source", "both"):
        log.info("Running source assertions on all tables...")
        run_all_source_assertions(frames, write_report=True)

    if mode in ("target", "both"):
        log.info("Running target assertions on all tables...")
        target_engine = get_engine("target")
        target_frames = extract_all(target_engine)
        # source_frames passed so run_all_target_assertions can enrich internally
        run_all_target_assertions(
            target_frames,
            source_frames=frames,
            write_report=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ALL TABLES — SNOWFLAKE
# ─────────────────────────────────────────────────────────────────────────────

def run_all_tables_snowflake():
    """
    Full Snowflake pipeline. Delegates to run_pipeline() in snowflake_files/run_pipeline.py.
    run_pipeline() manages its own source data — no path needed.
    """
    from snowflake_files.run_pipeline import run_pipeline
    run_pipeline()


def assert_all_tables_snowflake(mode: str):
    """
    Run source and/or target assertions for all tables using Snowflake.
    Source data is read from snowflake_files/source_data.xlsx —
    call fetch_source_excel() or upload the file first.
    mode: "source" | "target" | "both"
    """
    if not _SOURCE_EXCEL.exists():
        raise FileNotFoundError(
            f"Source Excel not found: {_SOURCE_EXCEL}\n"
            f"Fetch or upload source_data.xlsx first."
        )
    log.info("Loading source data from Excel: %s", _SOURCE_EXCEL)
    xl = pd.ExcelFile(_SOURCE_EXCEL)
    source_frames = {
        table: xl.parse(table)
        for table in TABLES
        if table in xl.sheet_names
    }
    missing = [t for t in TABLES if t not in xl.sheet_names]
    if missing:
        log.warning("Sheets missing from Excel (skipped): %s", missing)

    if mode in ("source", "both"):
        log.info("Running source assertions on all tables (from Excel)...")
        run_all_source_assertions(source_frames, write_report=True)

    if mode in ("target", "both"):
        log.info("Running target assertions on all tables (from Snowflake)...")
        sf_engine     = _get_snowflake_engine()
        target_frames = extract_all(sf_engine)
        run_all_target_assertions(
            target_frames,
            source_frames=source_frames,
            write_report=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE TABLE — LOCAL
# ─────────────────────────────────────────────────────────────────────────────

def assert_table_local(
    table_name: str,
    mode: str,
    watermark_from: str = None,
    watermark_to: str = None,
):
    """
    Run source and/or target assertions for a single table from local DBs.
    Uses extract_<table_name>() directly.
    mode: "source" | "target" | "both"
    """
    source_engine = get_engine("source")
    extractor     = _TABLE_EXTRACTOR[table_name]
    source_df     = extractor(source_engine, watermark_from=watermark_from, watermark_to=watermark_to)

    if mode in ("source", "both"):
        log.info("Running source assertions on table '%s'...", table_name)
        ref_set = _get_ref_set(table_name, source_engine)
        run_one_source_table(table_name, source_df, ref_set=ref_set, write_report=True)

    if mode in ("target", "both"):
        log.info("Running target assertions on table '%s'...", table_name)
        target_engine = get_engine("target")
        target_df     = _TABLE_EXTRACTOR[table_name](target_engine)
        enriched      = _enrich_for_target_one_table(table_name, target_df, source_df)
        run_one_target_table(table_name, enriched, write_report=True)


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE TABLE — SNOWFLAKE
# ─────────────────────────────────────────────────────────────────────────────

def assert_table_snowflake(table_name: str, mode: str):
    """
    Run source and/or target assertions for a single table using Snowflake.
    Source data is read from snowflake_files/table_csvs/<table_name>.csv —
    call fetch_table_csv() first to refresh it.
    mode: "source" | "target" | "both"
    """
    csv_path = _TABLE_CSVS_DIR / f"{table_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Source CSV not found: {csv_path}\n"
            f"Run 'Fetch Source CSV' first to pull data from MySQL."
        )
    log.info("Loading source CSV for table '%s': %s", table_name, csv_path)
    source_df = pd.read_csv(csv_path)

    if mode in ("source", "both"):
        log.info("Running source assertions on table '%s' (from CSV)...", table_name)
        ref_set = _get_ref_set(table_name, get_engine("source"))
        run_one_source_table(table_name, source_df, ref_set=ref_set, write_report=True)

    if mode in ("target", "both"):
        log.info("Running target assertions on table '%s' (from Snowflake)...", table_name)
        sf_engine = _get_snowflake_engine()
        target_df = _TABLE_EXTRACTOR[table_name](sf_engine)
        enriched  = _enrich_for_target_one_table(table_name, target_df, source_df)
        run_one_target_table(table_name, enriched, write_report=True)


# ─────────────────────────────────────────────────────────────────────────────
# ROW ASSERTION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _coerce_pk(pk_val):
    """Cast UI string PK values to int; leave non-numeric strings as-is."""
    if isinstance(pk_val, str):
        try:
            return int(pk_val.strip())
        except ValueError:
            return pk_val.strip()
    return pk_val


def _validate_row_schema(row: dict, required_cols: set, table_name: str, side: str) -> list[str]:
    """Return list of missing column names; logs a warning for each."""
    missing = required_cols - set(row.keys())
    for col in sorted(missing):
        log.warning("SCHEMA [%s/%s]: column '%s' missing from fetched row", side, table_name, col)
    return sorted(missing)


def _pretty_print_row(row: dict, table_name: str, side: str, pk_val, failures: list) -> None:
    """
    Log the row as a formatted table with column | value | constraint columns.
    Failures are annotated inline.
    """
    failed_cols = {f.get("column_name"): f for f in failures}

    col_w   = max((len(k) for k in row), default=10)
    val_w   = max((len(str(v)) for v in row.values()), default=10)
    rule_w  = 40
    sep     = f"+{'-'*(col_w+2)}+{'-'*(val_w+2)}+{'-'*(rule_w+2)}+"
    header  = f"| {'COLUMN':<{col_w}} | {'VALUE':<{val_w}} | {'CONSTRAINT / FAILURE':<{rule_w}} |"

    lines = [
        f"{'─'*len(sep)}",
        f"  {side} · {table_name} · pk={pk_val}",
        sep, header, sep,
    ]
    for col, val in row.items():
        if col in failed_cols:
            f        = failed_cols[col]
            note     = f"FAIL [{f.get('assertion_type')}] {f.get('reason', '')}"
            note     = note[:rule_w]
        else:
            note = "OK"
        lines.append(f"| {col:<{col_w}} | {str(val):<{val_w}} | {note:<{rule_w}} |")
    lines.append(sep)

    for line in lines:
        if "FAIL" in line:
            log.warning("%s", line)
        else:
            log.info("%s", line)


def _log_row_results(results: list, side: str, table_name: str) -> None:
    for clean_row, failures, pk_val, row in results:
        _pretty_print_row(row, table_name, side, pk_val, failures)
        if clean_row:
            log.info("%s row pk=%s  ✓ PASSED all assertions", side, pk_val)
        else:
            log.warning("%s row pk=%s  ✗ FAILED %d assertion(s)", side, pk_val, len(failures))
            for f in failures:
                log.warning("  column=%-20s  rule=%-18s  value=%s",
                            f.get("column_name"), f.get("assertion_type"), f.get("raw_value"))
                log.warning("  reason: %s", f.get("reason"))


# ─────────────────────────────────────────────────────────────────────────────
# ROW — LOCAL
# ─────────────────────────────────────────────────────────────────────────────

def assert_row_local(table_name: str, pk_values: list, mode: str):
    """
    Run assertions on specific rows identified by primary key value(s).

    pk_values : list of PK values entered in the UI (source PK values).
                For source assertions the source PK column is used directly.
                For target assertions the source PK value is looked up via
                the source-side FK column stored in the target table
                (supplier_id / src_vehicle_id / part_id / qc_id), because
                surrogate keys (supplier_sk, vehicle_sk) differ from source PKs.
    mode      : "source" | "target" | "both"
    """
    # Coerce all PK values from UI strings to int where possible
    pk_values = [_coerce_pk(v) for v in pk_values]

    if mode in ("source", "both"):
        pk_col        = _SOURCE_PK[table_name]
        source_engine = get_engine("source")
        ref_set       = _get_ref_set(table_name, source_engine)
        results       = []
        for pk_val in pk_values:
            df = extract_rows_by_pk(source_engine, table_name, [(pk_col, pk_val)])
            if df.empty:
                log.warning("SOURCE %s: no row found for %s=%s", table_name, pk_col, pk_val)
                continue
            for row in df.to_dict(orient="records"):
                missing = _validate_row_schema(row, _REQUIRED_SOURCE_COLS[table_name],
                                               table_name, "SOURCE")
                if missing:
                    log.warning("SOURCE %s pk=%s: schema incomplete — missing %s",
                                table_name, pk_val, missing)
                clean_row, failures = run_one_source_row(table_name, row, ref_set=ref_set)
                results.append((clean_row, failures, pk_val, row))
        _log_row_results(results, "SOURCE", table_name)

    if mode in ("target", "both"):
        # Use the source-side FK column in the target to look up by source PK value.
        # suppliers → supplier_id, vehicles → src_vehicle_id, others → same PK.
        lookup_col    = _TARGET_ROW_LOOKUP_COL[table_name]
        source_engine = get_engine("source")
        target_engine = get_engine("target")
        results       = []
        for pk_val in pk_values:
            df = extract_rows_by_pk(target_engine, table_name, [(lookup_col, pk_val)])
            if df.empty:
                log.warning(
                    "TARGET %s: no row found for %s=%s  "
                    "(note: enter the SOURCE pk value; target surrogate key is %s)",
                    table_name, lookup_col, pk_val, _TARGET_PK[table_name],
                )
                continue
            for row in df.to_dict(orient="records"):
                # Enrich with source columns needed by DERIVED_CHECK rules:
                # suppliers  → is_active  (drives active_status derivation)
                # quality_checks → pass_fail (drives is_passed derivation)
                if table_name == "suppliers":
                    src_df = extract_rows_by_pk(
                        source_engine, "suppliers", [("supplier_id", pk_val)]
                    )
                    if not src_df.empty:
                        row["is_active"] = src_df.iloc[0]["is_active"]
                    else:
                        log.warning("TARGET enrich: source suppliers row not found for supplier_id=%s", pk_val)
                elif table_name == "quality_checks":
                    src_df = extract_rows_by_pk(
                        source_engine, "quality_checks", [("check_id", pk_val)]
                    )
                    if not src_df.empty:
                        row["pass_fail"] = src_df.iloc[0]["pass_fail"]
                    else:
                        log.warning("TARGET enrich: source quality_checks row not found for check_id=%s", pk_val)

                missing = _validate_row_schema(row, _REQUIRED_TARGET_COLS[table_name],
                                               table_name, "TARGET")
                if missing:
                    log.warning("TARGET %s pk=%s: schema incomplete — missing %s",
                                table_name, pk_val, missing)
                clean_row, failures = run_one_target_row(table_name, row)
                results.append((clean_row, failures, pk_val, row))
        _log_row_results(results, "TARGET", table_name)


# ─────────────────────────────────────────────────────────────────────────────
# ROW — SNOWFLAKE
# ─────────────────────────────────────────────────────────────────────────────

def assert_row_snowflake(table_name: str, pk_values: list, mode: str):
    """
    Run assertions on specific rows in Snowflake, fetched directly by PK.

    pk_values : list of source PK values entered in the UI.
                Rows are looked up via _SNOWFLAKE_ROW_LOOKUP_COL (the source-side
                FK column stored in Snowflake) so the user always enters the
                source PK, not the Snowflake surrogate key.
    mode      : "source" | "target" | "both"
    """
    lookup_col = _SNOWFLAKE_ROW_LOOKUP_COL[table_name]
    sf_engine  = _get_snowflake_engine()
    pk_values  = [_coerce_pk(v) for v in pk_values]

    # Source engine needed only for DERIVED_CHECK enrichment on suppliers/quality_checks
    _src_engine = None
    if table_name in ("suppliers", "quality_checks") and mode in ("target", "both"):
        _src_engine = get_engine("source")

    for pk_val in pk_values:
        df = extract_rows_by_pk(sf_engine, table_name, [(lookup_col, pk_val)])
        if df.empty:
            log.warning(
                "Snowflake %s: no row found for %s=%s",
                table_name, lookup_col, pk_val,
            )
            continue

        for row in df.to_dict(orient="records"):
            # Snowflake returns uppercase column names — normalise to lowercase
            # so assertion rules (which use lowercase column names) work correctly.
            row = {k.lower(): v for k, v in row.items()}

            if mode in ("source", "both"):
                _validate_row_schema(row, _REQUIRED_SOURCE_COLS[table_name], table_name, "SNOWFLAKE/SOURCE")
                clean_row, failures = run_one_source_row(table_name, row, ref_set=None)
                _pretty_print_row(row, table_name, "SNOWFLAKE SOURCE-RULES", pk_val, failures)
                if clean_row:
                    log.info("SNOWFLAKE SOURCE-RULES row pk=%s  ✓ PASSED", pk_val)
                else:
                    log.warning("SNOWFLAKE SOURCE-RULES row pk=%s  ✗ FAILED %d assertion(s)",
                                pk_val, len(failures))
                    for f in failures:
                        log.warning("  column=%-20s  rule=%-18s  value=%s",
                                    f.get("column_name"), f.get("assertion_type"), f.get("raw_value"))
                        log.warning("  reason: %s", f.get("reason"))

            if mode in ("target", "both"):
                # Enrich with source columns needed by DERIVED_CHECK rules
                if _src_engine and table_name == "suppliers":
                    src_df = extract_rows_by_pk(_src_engine, "suppliers", [("supplier_id", pk_val)])
                    if not src_df.empty:
                        row["is_active"] = src_df.iloc[0]["is_active"]
                    else:
                        log.warning("SNOWFLAKE enrich: source suppliers row not found for supplier_id=%s", pk_val)
                elif _src_engine and table_name == "quality_checks":
                    src_df = extract_rows_by_pk(_src_engine, "quality_checks", [("check_id", pk_val)])
                    if not src_df.empty:
                        row["pass_fail"] = src_df.iloc[0]["pass_fail"]
                    else:
                        log.warning("SNOWFLAKE enrich: source quality_checks row not found for check_id=%s", pk_val)

                _validate_row_schema(row, _REQUIRED_TARGET_COLS[table_name], table_name, "SNOWFLAKE/TARGET")
                clean_row, failures = run_one_target_row(table_name, row)
                _pretty_print_row(row, table_name, "SNOWFLAKE TARGET-RULES", pk_val, failures)
                if clean_row:
                    log.info("SNOWFLAKE TARGET-RULES row pk=%s  ✓ PASSED", pk_val)
                else:
                    log.warning("SNOWFLAKE TARGET-RULES row pk=%s  ✗ FAILED %d assertion(s)",
                                pk_val, len(failures))
                    for f in failures:
                        log.warning("  column=%-20s  rule=%-18s  value=%s",
                                    f.get("column_name"), f.get("assertion_type"), f.get("raw_value"))
                        log.warning("  reason: %s", f.get("reason"))