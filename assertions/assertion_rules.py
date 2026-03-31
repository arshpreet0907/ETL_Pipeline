"""
assertion_rules.py
------------------
Shared infrastructure used by both source_assertions.py and
target_assertions.py.

Contains three things:

1.  AssertionRule — a single named, self-contained check that knows how
    to evaluate itself against a DataFrame or a single row dict.

2.  FailureCollector — accumulates failures from one or many rules into
    a flat, reportable structure.

3.  write_failure_report() — writes a timestamped Excel file.

Design rationale (for reviewers)
---------------------------------
Rules are data, not code.  Each AssertionRule is a small dataclass that
holds *what* to check and *why*, plus a callable that does the actual
check.  The execution modes (all-tables / one-table / one-row) are just
different iteration patterns over the same rules — no duplication.

This means:
  - Adding a new assertion rule = adding one entry to a list, nothing else.
  - Changing a rule's reason string = one-line edit in one place.
  - The three execution modes are trivially composable and independently
    testable without touching rule logic.

Rule callable contract
----------------------
Every rule's `check_fn` has this signature:

    check_fn(value, context: dict) -> bool

    value   — the raw cell value for the column being checked.
              For UNIQUENESS and REF_INTEGRITY rules, value is the entire
              column Series (passed as `context["series"]` and ignored
              as `value`).
    context — dict with extra data the rule needs:
                  "series"     : the full column Series (for uniqueness)
                  "ref_set"    : set of valid FK values (for RI)
                  "row"        : the full row dict (for DATE_LOGIC)

    Returns True  if the value is VALID (no failure).
    Returns False if the value is INVALID (failure should be recorded).

Assertion types used in this codebase
--------------------------------------
    NULL_CHECK      — value must not be None / NaN / NaT
    UNIQUENESS      — value must not appear more than once in the column
    RANGE           — value must fall within [lo, hi] or satisfy > 0 / >= n
    ENUM            — value must be a member of a valid set
    DATE_LOGIC      — a pair of date columns must be in the correct order
    REF_INTEGRITY   — value must exist in a reference set
    DERIVED_CHECK   — target column must match a computed expectation
    COUNT_MATCH     — row count in target must equal row count in source
    NOT_NULL_TARGET — target column that is always populated must not be null
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd

log = logging.getLogger(__name__)

FAILURE_DIR = os.path.join(os.path.dirname(__file__), "../outputs/assertion_failures")
os.makedirs(FAILURE_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# ASSERTION RULE
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AssertionRule:
    """
    A single, self-contained assertion rule.

    Attributes
    ----------
    name : str
        Short human-readable label, e.g. "vin not null".
    assertion_type : str
        Category string written to the failure report.
        One of: NULL_CHECK, UNIQUENESS, RANGE, ENUM, DATE_LOGIC,
                REF_INTEGRITY, DERIVED_CHECK, COUNT_MATCH, NOT_NULL_TARGET.
    column : str
        The primary column this rule targets.  For DATE_LOGIC rules this
        is the earlier-date column (e.g. "contract_start").
    reason : str
        Plain-English explanation of why this rule matters, written
        verbatim into the failure report so the reader understands the
        business impact without needing to read code.
    check_fn : Callable[[Any, dict], bool]
        Returns True if the value is valid, False if it is a failure.
        See module docstring for the full contract.
    context_keys : list[str]
        Names of extra items the check_fn needs from the context dict.
        Declaring them here makes the rule's dependencies explicit and
        lets the runner build only what is needed.
        Common values: ["series", "ref_set", "row"].
    """

    name:           str
    assertion_type: str
    column:         str
    reason:         str
    check_fn:       Callable[[Any, dict], bool]
    context_keys:   list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# STANDARD CHECK FUNCTIONS
# Pure functions — no side effects, no state.
# Each returns True (valid) or False (failure).
# ─────────────────────────────────────────────────────────────────────────────

def check_not_null(value, _ctx: dict) -> bool:
    """Fails if value is None, NaN, or NaT."""
    return value is not None and not (
        isinstance(value, float) and value != value  # NaN check
    )


def check_unique(value, ctx: dict) -> bool:
    """
    Fails if this value appears more than once in the full column Series.
    Must be called with context["series"] = the full column.
    """
    series = ctx["series"]
    if pd.isna(value):
        return True   # nulls are checked by check_not_null separately
    return series.dropna().tolist().count(value) == 1


def make_range_check(lo: float, hi: float) -> Callable:
    """Return a check function that passes when lo <= value <= hi."""
    def _check(value, _ctx: dict) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return True   # null — not this rule's concern
        return lo <= float(value) <= hi
    return _check


def make_positive_check() -> Callable:
    """Return a check function that passes when value > 0."""
    def _check(value, _ctx: dict) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return True
        return float(value) > 0
    return _check


def make_min_check(minimum: float) -> Callable:
    """Return a check function that passes when value >= minimum."""
    def _check(value, _ctx: dict) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return True
        return float(value) >= minimum
    return _check


def make_enum_check(valid_set: set) -> Callable:
    """Return a check function that passes when value is in valid_set."""
    def _check(value, _ctx: dict) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return True
        return value in valid_set
    return _check


def make_str_length_check(expected_len: int) -> Callable:
    """Return a check function that passes when len(value) == expected_len."""
    def _check(value, _ctx: dict) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return True
        return len(str(value)) == expected_len
    return _check


def make_ref_integrity_check() -> Callable:
    """
    Return a check function that passes when value exists in context["ref_set"].
    The ref_set is supplied at runtime by the runner.
    """
    def _check(value, ctx: dict) -> bool:
        if value is None or (isinstance(value, float) and value != value):
            return True
        return value in ctx["ref_set"]
    return _check


def make_date_logic_check(later_col: str) -> Callable:
    """
    Return a check function for DATE_LOGIC rules.
    Passes when context["row"][column] < context["row"][later_col].
    The column itself is the *earlier* date (e.g. contract_start).
    """
    def _check(value, ctx: dict) -> bool:
        row = ctx["row"]
        earlier = value
        later   = row.get(later_col)
        if earlier is None or later is None:
            return True   # null dates handled by separate null checks
        try:
            return pd.to_datetime(earlier) < pd.to_datetime(later)
        except Exception:
            return False
    return _check


def make_derived_check(compute_fn: Callable, tolerance: float = 0.0) -> Callable:
    """
    Return a check function for DERIVED_CHECK rules (target assertions).

    compute_fn(row) -> expected_value
    tolerance      -> for float comparisons (0.0 means exact match)
    """
    def _check(value, ctx: dict) -> bool:
        row = ctx["row"]
        try:
            expected = compute_fn(row)
        except Exception:
            return False   # cannot compute expected — treat as failure
        if value is None or (isinstance(value, float) and value != value):
            return expected is None
        if tolerance > 0:
            try:
                return abs(float(value) - float(expected)) <= tolerance
            except (TypeError, ValueError):
                pass
        return str(value) == str(expected)
    return _check


# ─────────────────────────────────────────────────────────────────────────────
# FAILURE COLLECTOR
# ─────────────────────────────────────────────────────────────────────────────

class FailureCollector:
    """
    Accumulates assertion failures for one table across one run.

    Maintains two structures:
        bad_pks  — set of primary key values that have at least one failure.
                   Used to filter the clean DataFrame in O(n) time.
        records  — list of dicts, one per individual failure.
                   Each row in the Excel report corresponds to one record.
                   A single PK can have multiple records if it fails
                   multiple rules.

    Usage
    -----
        fc = FailureCollector(pk_col="vehicle_id")
        fc.add(pk_value=42, rule=my_rule, raw_value="BAD_VALUE")
        clean_df = df[~df["vehicle_id"].isin(fc.bad_pks)]
        report_df = fc.to_dataframe()
    """

    COLUMNS = [
        "primary_key", "column_name", "assertion_type", "reason", "raw_value"
    ]

    def __init__(self, pk_col: str, table_name: str):
        self.pk_col     = pk_col
        self.table_name = table_name
        self.bad_pks    = set()
        self._records   = []

    def add(self, pk_value: Any, rule: AssertionRule, raw_value: Any = None):
        """Record one failure."""
        self.bad_pks.add(pk_value)
        self._records.append({
            "primary_key":    pk_value,
            "column_name":    rule.column,
            "assertion_type": rule.assertion_type,
            "reason":         rule.reason,
            "raw_value":      "" if raw_value is None else str(raw_value),
        })

    def to_dataframe(self) -> pd.DataFrame:
        """Return all recorded failures as a DataFrame."""
        if not self._records:
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.DataFrame(self._records, columns=self.COLUMNS)

    @property
    def failure_count(self) -> int:
        return len(self._records)

    @property
    def failed_row_count(self) -> int:
        return len(self.bad_pks)

    def __len__(self) -> int:
        return self.failure_count


# ─────────────────────────────────────────────────────────────────────────────
# ROW-LEVEL RUNNER
# Used by both source and target assertion modules.
# ─────────────────────────────────────────────────────────────────────────────

def run_rules_on_row(
    row: dict,
    pk_col: str,
    rules: list[AssertionRule],
    fc: FailureCollector,
    context_providers: Optional[dict[str, Any]] = None,
) -> bool:
    """
    Apply every rule in `rules` to a single row dict.

    Parameters
    ----------
    row              : dict mapping column name -> value.
    pk_col           : column name of the primary key.
    rules            : list of AssertionRule to apply.
    fc               : FailureCollector that accumulates failures.
    context_providers: optional dict supplying extra context values
                       (e.g. {"ref_set": set_of_vehicle_ids,
                               "series": full_column_series}).
                       Only values whose keys appear in a rule's
                       context_keys are included in the call.

    Returns
    -------
    True  — the row passed all rules (it is clean).
    False — the row failed at least one rule.
    """
    pk_value = row.get(pk_col)
    passed   = True

    for rule in rules:
        value = row.get(rule.column)

        # Build the context dict for this rule
        ctx = {"row": row}
        if context_providers:
            for key in rule.context_keys:
                if key in context_providers:
                    ctx[key] = context_providers[key]

        valid = rule.check_fn(value, ctx)
        if not valid:
            fc.add(pk_value, rule, raw_value=value)
            passed = False

    return passed


def run_rules_on_dataframe(
    df: pd.DataFrame,
    pk_col: str,
    rules: list[AssertionRule],
    fc: FailureCollector,
    context_providers: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Apply every rule to every row in a DataFrame.

    UNIQUENESS rules receive the full column Series via context["series"]
    so they can check for duplicates across the whole batch.

    Parameters
    ----------
    df               : DataFrame to validate.
    pk_col           : primary key column name.
    rules            : list of AssertionRule.
    fc               : FailureCollector.
    context_providers: same as run_rules_on_row.

    Returns
    -------
    DataFrame containing only the rows that passed all rules.
    """
    # Pre-build series context for uniqueness rules (computed once per call,
    # not once per row — avoids O(n²) recomputation).
    series_cache: dict[str, pd.Series] = {}
    for rule in rules:
        if rule.assertion_type == "UNIQUENESS" and rule.column not in series_cache:
            series_cache[rule.column] = df[rule.column] if rule.column in df.columns else pd.Series([], dtype=object)

    providers = dict(context_providers or {})

    for _, row_series in df.iterrows():
        row = row_series.to_dict()
        pk_value = row.get(pk_col)

        for rule in rules:
            value = row.get(rule.column)

            ctx = {"row": row}
            for key in rule.context_keys:
                if key == "series":
                    ctx["series"] = series_cache.get(rule.column, pd.Series([], dtype=object))
                elif key in providers:
                    ctx[key] = providers[key]

            valid = rule.check_fn(value, ctx)
            if not valid:
                fc.add(pk_value, rule, raw_value=value)

    if not fc.bad_pks:
        return df.copy()
    return df[~df[pk_col].isin(fc.bad_pks)].copy()


# ─────────────────────────────────────────────────────────────────────────────
# FAILURE REPORT WRITER
# ─────────────────────────────────────────────────────────────────────────────

def write_failure_report(
    collectors: dict[str, FailureCollector],
    run_ts: str,
    prefix: str = "assertion_failures",
) -> Optional[str]:
    """
    Write all assertion failures to a timestamped Excel file.

    Always writes one sheet per table even if a table has zero failures,
    so the workbook structure is consistent across every run.

    Parameters
    ----------
    collectors : dict[table_name -> FailureCollector]
    run_ts     : timestamp string used in the filename.
    prefix     : filename prefix ("assertion_failures" for source,
                 "post_migration_failures" for target).

    Returns
    -------
    str  — absolute path to the written file.
    None — if no failures exist across all collectors.
    """
    total = sum(fc.failure_count for fc in collectors.values())
    if total == 0:
        log.info("No failures recorded — report not written")
        return None

    filepath = os.path.join(FAILURE_DIR, f"{prefix}_{run_ts}.xlsx")

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        for table_name, fc in collectors.items():
            fdf = fc.to_dataframe()
            fdf.to_excel(writer, sheet_name=table_name[:31], index=False)

            # Auto-size columns for readability
            ws = writer.sheets[table_name[:31]]
            for col_cells in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col_cells if c.value is not None),
                    default=10,
                )
                ws.column_dimensions[col_cells[0].column_letter].width = min(
                    max_len + 4, 80
                )

    log.info("Report written: %s  (%d failures across %d tables)",
             filepath, total, len(collectors))
    return filepath
