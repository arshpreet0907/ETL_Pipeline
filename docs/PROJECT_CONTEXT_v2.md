# Vehicle Manufacturing DW Migration — Project Context

## What this project is

A Python ETL pipeline migrating historical vehicle manufacturing data from a
MySQL source DB (`vehicle_manufacturing_src`) to a MySQL target data warehouse
(`vehicle_manufacturing_dw`). Supports one-time full migration and incremental
windowed loads via watermarks. Both databases are MySQL 8.0. A parallel
Snowflake pipeline exists that reads from an Excel export of the same source
data. A Streamlit UI wraps both pipelines.

---

## Current status

**Fully implemented and working end-to-end:**
- Source + target schema finalised (v3) — no soft-delete columns anywhere
- MySQL DDL scripts written for both databases (`source_tables.sql`, `target_tables.sql`)
- `db_connector.py` — SQLAlchemy engine + PyMySQL connection factory
- `extractor.py` — watermark-aware extraction, sanitised WHERE builder, no assertions
- `watermark_generator.py` — queries source date range, generates windowed watermark pairs
- `makeParquet.py` — saves/loads DataFrames as Parquet files (pyarrow)
- `assertion_rules.py` — shared infrastructure: AssertionRule dataclass, FailureCollector, runners, Excel report writer
- `source_assertions.py` — 23 source rules across 4 tables, three execution modes
- `target_assertions.py` — 35 post-migration rules, same three modes, source-enrichment for derived validation
- `transform.py` — full column mapping + derivations + push_to_db + push_to_run_log. No deleted_at references.
- `pipeline.py` — primary orchestration: full load + windowed incremental + post-migration validation, CLI entry point
- `local_execution.py` — simpler orchestration script (whole_pipeline), used by the Streamlit UI for offline mode
- `custom_execution.py` — dev/debug helpers: check_source_data_working, source_assertions, get_windows
- `snowflake/` — complete parallel Snowflake pipeline (sf_connector, get_source_data, snowflake_pipeline, run_pipeline, verify_schema)
- `UI/app.py` — Streamlit front-end supporting both Snowflake and Offline (local MySQL) modes with watermark pickers
- `start_app.py` — launches the Streamlit app
- Test data: SQL (2,065 rows), medium txt (24,030 rows), large txt (1.4M rows), CSV copies, per-table SQL input files
- All test data files have `deleted_at` stripped — not in schema

**No known outstanding issues.** The transform.py deleted_at issues noted in the previous version of this document have been resolved — no deleted_at references exist anywhere in the codebase.

---

## Decisions made — do not revisit unless user raises them

### Soft deletes — RESOLVED
Soft-delete columns (`deleted_at`) are removed entirely from both source and
target schemas. This is a one-time migration; no filtering on deletion status.
`deleted_at` does not appear in any DDL, assertion rule, transform function,
or test data file.

ALTER commands to remove from existing databases (if upgrading from an older schema):
```sql
-- Source
USE vehicle_manufacturing_src;
ALTER TABLE suppliers      DROP COLUMN deleted_at;
ALTER TABLE vehicles       DROP COLUMN deleted_at;
ALTER TABLE parts          DROP COLUMN deleted_at;
ALTER TABLE quality_checks DROP COLUMN deleted_at;

-- Target
USE vehicle_manufacturing_dw;
ALTER TABLE vehicles       DROP COLUMN deleted_at, DROP COLUMN dw_updated_at;
ALTER TABLE parts          DROP COLUMN deleted_at, DROP COLUMN dw_updated_at;
ALTER TABLE suppliers      DROP COLUMN deleted_at, DROP COLUMN dw_updated_at;
ALTER TABLE quality_checks DROP COLUMN deleted_at, DROP COLUMN dw_updated_at;
```

### Schema — finalised (v3)

**Source tables — columns (from source_tables.sql):**
- `suppliers` (11): supplier_id PK AUTO, supplier_code UNIQUE, supplier_name, country, tier, rating, contract_start, contract_end, is_active, created_at, updated_at
- `vehicles` (15): vehicle_id PK AUTO, vin UNIQUE, model_code, variant, color_code, engine_type, plant_code, line_number, production_date, shift ENUM, status ENUM, quality_score, weight_kg, created_at, updated_at
- `parts` (12): part_id PK AUTO, vehicle_id FK, part_number, part_name, supplier_code FK, quantity, unit_cost, currency, install_time_min, defect_flag, batch_number, created_at
- `quality_checks` (11): check_id PK AUTO, vehicle_id FK, check_date, inspector_id, station, test_type ENUM, result ENUM, defect_code NULL, rework_hours NULL, pass_fail ENUM, created_at

**Target tables — columns (from target_tables.sql):**
- `suppliers` (18, SCD2): supplier_sk PK AUTO, supplier_id, supplier_code, supplier_name, country_of_origin, supplier_tier, tier_label, performance_rating, contract_start_date, contract_end_date, contract_duration_days, active_status, valid_from, valid_to DEFAULT '9999-12-31', is_current DEFAULT 1, created_at, dw_inserted_at DEFAULT CURRENT_TIMESTAMP, updated_at NULL
- `vehicles` (21): vehicle_sk PK AUTO, src_vehicle_id UNIQUE, vin_number UNIQUE, model_code, model_variant_name, color_code, engine_type, manufacturing_plant, production_date, production_year, production_month, production_shift, production_status, quality_score, quality_tier, gross_weight_kg, weight_category, is_electric_vehicle, created_at, dw_inserted_at DEFAULT CURRENT_TIMESTAMP, updated_at NULL
- `parts` (15): part_id PK, vehicle_id FK→vehicles.src_vehicle_id, part_number, component_name, supplier_code FK→suppliers.supplier_code, quantity_used, unit_cost_eur, total_cost_eur, cost_tier, installation_hrs, has_defect_flag, batch_number, created_at, dw_inserted_at DEFAULT CURRENT_TIMESTAMP
- `quality_checks` (15): qc_id PK, vehicle_id FK→vehicles.src_vehicle_id, inspection_date, inspection_year, inspector_code, inspection_station, test_category, inspection_result, defect_code NULL, has_defect, rework_hours NULL, rework_cost_usd, is_passed, created_at, dw_inserted_at DEFAULT CURRENT_TIMESTAMP
- `etl_run_log` (14): run_id PK AUTO, run_start, run_end NULL, pipeline_name, table_name, load_type ENUM(FULL/INCREMENTAL), watermark_from NULL, watermark_to NULL, rows_extracted, rows_inserted, rows_updated, rows_failed, status ENUM(RUNNING/SUCCESS/PARTIAL/FAILED), error_message NULL

Note: The target DDL uses `updated_at` (not `dw_updated_at`) as the column name in the actual SQL file, but transform.py writes to `dw_updated_at`. Verify column name alignment when running against a fresh schema.

### SCD Type 2 on suppliers
Another team maintains supplier changes. This pipeline snapshots what it sees.
On initial load: valid_from = contract_start, valid_to = 9999-12-31, is_current = 1.
Future runs detect changes, close old row, insert new version.

### Watermark / incremental load
- Full load: no watermarks → `SELECT *` with no date filter
- Incremental (open-ended): `watermark_from` only → `WHERE created_at > from` (+ `OR updated_at > from` for mutable tables)
- Windowed: both watermarks → `WHERE (created_at > from AND created_at <= to)` (+ OR updated_at window for mutable tables)
- Watermark stored in `etl_run_log.watermark_to` after each successful run
- `get_last_watermark(target_engine)` in pipeline.py reads last successful watermark_to
- `get_run_watermark_to()` in extractor.py captures current UTC timestamp at pipeline start
- `watermark_generator.py` queries source date range and generates window pairs (days/weeks/months)
- For incremental RI checks: `get_all_vehicle_ids(engine)` fetches all vehicle_ids from source (not just current batch)
- WHERE clause built by `_build_where()` in extractor.py; inputs sanitised via regex to prevent SQL injection

### dw_inserted_at handling
Never included in INSERT column list — MySQL DEFAULT CURRENT_TIMESTAMP fills it.
Passing NaT for a NOT NULL DEFAULT column causes MySQL error 1048.
Solution: listed in `_SKIP_COLS` in both transform.py and snowflake_pipeline.py.
`fill_dw_timestamps=True` parameter on transform functions pre-fills it in Python
for SQLite / unit tests that lack DEFAULT CURRENT_TIMESTAMP support.
The Snowflake pipeline always passes `fill_dw_timestamps=True`.

### FK checks during load
`SET FOREIGN_KEY_CHECKS = 0` wraps the entire insert block in `push_to_db`.
Re-enabled in a `finally` block. Source assertions already guarantee integrity.

### Upserts
Current `push_to_db` uses TRUNCATE + to_sql(if_exists="append") for full load,
straight append for incremental. Production incremental will use
INSERT ... ON DUPLICATE KEY UPDATE — not yet implemented.
`if_exists='replace'` on pandas is explicitly avoided (issues DROP TABLE which
MySQL blocks with FK constraints).

### Timezone convention
All datetime.now() calls use `datetime.now(timezone.utc)`. `datetime.utcnow()` deprecated in Python 3.12+.

### cost_tier banding
cost_tier is banded on `total_cost_eur` (the derived line-total), NOT on `unit_cost_eur`.
Bands: <= 500 → LOW_VALUE, (500, 2000] → MID_VALUE, > 2000 → HIGH_VALUE.
This matches what target_assertions.py verifies.

### Scale / future technology
For production (tens of millions of rows): **Polars** is recommended for single-machine
scale. **PySpark** for distributed/cluster scale. **DuckDB** for Parquet-based staging.
Current pandas `iterrows()` in assertion runners should be replaced with vectorised
column operations when moving to Polars/Spark.

---

## Pipeline modes

### pipeline.py (primary — MySQL to MySQL)
CLI entry point with three modes:

```
python pipeline.py --mode full
python pipeline.py --mode incremental
python pipeline.py --mode incremental --window 7
python pipeline.py --mode incremental --from "2021-01-01 00:00:00"
python pipeline.py --mode validate
```

- `run_full()` — TRUNCATE all targets, migrate every row, watermark_from = 1970-01-01
- `run_windowed()` — reads last watermark from etl_run_log, walks forward in windows of N days; falls back to full load if no prior watermark
- `run_post_migration_validation()` — reads all target tables, runs 35 post-migration assertions
- `_run_one_window()` — single extract→assert→transform→load→log cycle for one date window
- `get_last_watermark()` — reads most recent SUCCESS watermark_to from etl_run_log
- `get_source_date_range()` — finds MIN/MAX created_at across all 4 source tables

### local_execution.py (used by Streamlit UI offline mode)
- `whole_pipeline(watermark_from, watermark_to)` — simpler orchestration: extract → source assertions → transform → push_to_db → push_to_run_log → extract target → post-migration assertions
- Uses `pipeline_name="etl_poc"` in run log (differs from pipeline.py which uses `"vehicle_dw_migration"`)
- Always uses `if_exists="replace"` (full truncate) regardless of watermark

### Snowflake pipeline
- `get_source_data.py` — extracts from MySQL source, writes to `snowflake/source_data.xlsx`
- `snowflake_pipeline.py` — 7-step pipeline: load Excel → source assertions → transform → push to Snowflake → extract from Snowflake → post assertions → run log
- `run_pipeline.py` — orchestrates snowflake_pipeline.py, records per-table run log entries
- `sf_connector.py` — Snowflake SQLAlchemy engine via snowflake-sqlalchemy, reads SF_* env vars
- `verify_schema.py` — pre-flight schema check: queries Snowflake information_schema and compares against expected column/type/nullability definitions for all 5 tables
- Uses `fill_dw_timestamps=True` in transform (Snowflake has no DEFAULT CURRENT_TIMESTAMP equivalent in the same way)
- Uses `pipeline_name="snowflake_etl"` in run log

---

## Assertion rules summary

### Source assertions (source_assertions.py) — 23 rules

| Table | Rules |
|---|---|
| suppliers (5) | supplier_code UNIQUENESS, rating RANGE 0-5, tier ENUM {1,2,3}, is_active ENUM {0,1}, contract DATE_LOGIC (start < end) |
| vehicles (8) | vin NULL_CHECK, vin UNIQUENESS, vin length=17, quality_score RANGE 0-100, weight_kg positive, shift ENUM, status ENUM, production_date NULL_CHECK |
| parts (6) | part_number NULL_CHECK, unit_cost positive, quantity >= 1, currency ENUM {EUR/USD/GBP}, defect_flag ENUM {0,1}, vehicle_id REF_INTEGRITY |
| quality_checks (4) | pass_fail ENUM {PASS/FAIL}, rework_hours >= 0, vehicle_id REF_INTEGRITY, test_type ENUM {PAINT/ELECTRICAL/STRUCTURAL/EMISSIONS/SAFETY} |

### Target assertions (target_assertions.py) — 35 rules

| Table | Rules |
|---|---|
| vehicles (12) | vehicle_sk NOT NULL, src_vehicle_id NOT NULL, vin_number NOT NULL, vin_number length=17, dw_inserted_at NOT NULL, quality_tier DERIVED, weight_category DERIVED, is_electric_vehicle DERIVED, production_year DERIVED, production_month DERIVED, quality_tier ENUM, weight_category ENUM |
| parts (7) | component_name NOT NULL, dw_inserted_at NOT NULL, total_cost_eur DERIVED (qty×unit, tol 0.01), cost_tier DERIVED, cost_tier ENUM, unit_cost_eur positive, quantity_used >= 1 |
| suppliers (10) | supplier_sk NOT NULL, dw_inserted_at NOT NULL, is_current ENUM {0,1}, valid_to NOT NULL, tier_label DERIVED, active_status DERIVED, tier_label ENUM, active_status ENUM, performance_rating RANGE 0-5, contract_duration_days DERIVED |
| quality_checks (9) | qc_id NOT NULL, dw_inserted_at NOT NULL, is_passed DERIVED, has_defect DERIVED, rework_cost_usd DERIVED (×$85, tol 0.01), is_passed ENUM {0,1}, has_defect ENUM {0,1}, rework_cost_usd >= 0, inspection_year NOT NULL |

Derived validation for `active_status` (suppliers) and `is_passed` (quality_checks) requires source frames.
`run_all_tables()` in target_assertions.py accepts `source_frames=` and calls `_enrich_target_with_source()`
which left-joins source columns onto target frames before running rules.

---

## Transform mappings summary (transform.py)

### transform_vehicles
| Source | Target | Type |
|---|---|---|
| vehicle_id | src_vehicle_id | rename |
| vin | vin_number | rename |
| model_code | model_code | direct |
| model_code + variant | model_variant_name | derived: concat with _ |
| color_code | color_code | direct |
| engine_type | engine_type | direct |
| plant_code | manufacturing_plant | rename |
| production_date | production_date | coerce datetime |
| production_date | production_year | derived: dt.year Int16 |
| production_date | production_month | derived: dt.month Int8 |
| shift | production_shift | rename |
| status | production_status | rename |
| quality_score | quality_score | direct |
| quality_score | quality_tier | derived: pd.cut [-inf,60)→SUBSTANDARD, [60,75)→ECONOMY, [75,90)→STANDARD, [90,+inf)→PREMIUM |
| weight_kg | gross_weight_kg | rename |
| weight_kg | weight_category | derived: >2500→HEAVY else LIGHT |
| engine_type | is_electric_vehicle | derived: EV_MOTOR→1 Int8 |
| created_at | created_at | direct |
| updated_at | dw_updated_at | rename |
| (DB auto) | dw_inserted_at | excluded via _SKIP_COLS |

### transform_suppliers
| Source | Target | Type |
|---|---|---|
| supplier_id | supplier_id | direct |
| supplier_code | supplier_code | direct |
| supplier_name | supplier_name | direct |
| country | country_of_origin | rename |
| tier | supplier_tier | rename |
| tier | tier_label | derived: {1→STRATEGIC, 2→PREFERRED, 3→APPROVED} |
| rating | performance_rating | rename |
| contract_start | contract_start_date | rename + coerce |
| contract_end | contract_end_date | rename + coerce |
| (derived) | contract_duration_days | derived: (end-start).dt.days Int32 |
| is_active | active_status | derived: 1→ACTIVE, 0→INACTIVE |
| contract_start | valid_from | SCD2: = contract_start on initial load |
| (constant) | valid_to | SCD2: "9999-12-31" |
| (constant) | is_current | SCD2: 1 Int8 |
| created_at | created_at | direct |
| updated_at | dw_updated_at | rename |
| (DB auto) | dw_inserted_at | excluded via _SKIP_COLS |

### transform_parts
| Source | Target | Type |
|---|---|---|
| part_id | part_id | direct |
| vehicle_id | vehicle_id | direct |
| part_number | part_number | direct |
| part_name | component_name | rename |
| supplier_code | supplier_code | direct |
| quantity | quantity_used | rename |
| unit_cost | unit_cost_eur | rename |
| quantity × unit_cost | total_cost_eur | derived: round 2dp |
| total_cost_eur | cost_tier | derived: pd.cut (-inf,500]→LOW_VALUE, (500,2000]→MID_VALUE, (2000,+inf)→HIGH_VALUE |
| install_time_min | installation_hrs | derived: /60 round 2dp |
| defect_flag | has_defect_flag | rename |
| batch_number | batch_number | direct |
| created_at | created_at | direct |
| (NaT) | dw_updated_at | NULL — parts are immutable |
| (DB auto) | dw_inserted_at | excluded via _SKIP_COLS |

### transform_quality_checks
| Source | Target | Type |
|---|---|---|
| check_id | qc_id | rename |
| vehicle_id | vehicle_id | direct |
| check_date | inspection_date | rename + coerce |
| check_date | inspection_year | derived: dt.year Int16 |
| inspector_id | inspector_code | rename |
| station | inspection_station | rename |
| test_type | test_category | rename |
| result | inspection_result | rename |
| defect_code | defect_code | direct (nullable) |
| defect_code | has_defect | derived: notna→1 Int8 |
| rework_hours | rework_hours | direct (nullable) |
| rework_hours | rework_cost_usd | derived: fillna(0) × 85 round 2dp |
| pass_fail | is_passed | derived: PASS→1 Int8 |
| created_at | created_at | direct |
| (NaT) | dw_updated_at | NULL — QC records are immutable |
| (DB auto) | dw_inserted_at | excluded via _SKIP_COLS |

---

## File inventory

| File | Purpose | Status |
|---|---|---|
| `pipeline.py` | Primary orchestration: full load, windowed incremental, post-migration validation, CLI | Done |
| `transform.py` | Column mappings, derivations, push_to_db, push_to_run_log | Done — no deleted_at |
| `local_execution.py` | Simpler whole_pipeline() used by Streamlit UI offline mode | Done |
| `custom_execution.py` | Dev/debug helpers: source assertions, window preview, data checks | Done |
| `start_app.py` | Launches Streamlit UI via subprocess | Done |
| `utils/db_connector.py` | SQLAlchemy engine + PyMySQL connection factory, reads .env | Done |
| `utils/extractor.py` | DB extraction, _build_where, watermark utilities, no assertions | Done |
| `utils/makeParquet.py` | save_parquet / load_parquet using pyarrow | Done |
| `utils/watermark_generator.py` | set_date_range, generate_windows (days/weeks/months) | Done |
| `assertions/assertion_rules.py` | Shared: AssertionRule, FailureCollector, runners, Excel report writer | Done |
| `assertions/source_assertions.py` | 23 source rules, three modes: run_all_tables / run_one_table / run_one_row | Done |
| `assertions/target_assertions.py` | 35 post-migration rules, same three modes, source enrichment | Done |
| `UI/app.py` | Streamlit front-end: Snowflake + Offline modes, watermark date/time pickers, live log streaming | Done |
| `snowflake/sf_connector.py` | Snowflake SQLAlchemy engine factory, reads SF_* env vars | Done |
| `snowflake/get_source_data.py` | Extracts MySQL source → source_data.xlsx | Done |
| `snowflake/snowflake_pipeline.py` | 7-step Snowflake pipeline functions | Done |
| `snowflake/run_pipeline.py` | Snowflake pipeline orchestrator + run log | Done |
| `snowflake/verify_schema.py` | Pre-flight Snowflake schema verification (5 tables, types, nullability) | Done |
| `snowflake/source_data.xlsx` | Excel export of source data for Snowflake pipeline | Generated at runtime |
| `utils/test_data/sql_files/create_databases.sql` | CREATE DATABASE statements for both DBs | Done |
| `utils/test_data/sql_files/source_tables.sql` | Source schema DDL + utility queries | Done |
| `utils/test_data/sql_files/target_tables.sql` | Target schema DDL + utility queries | Done |
| `utils/test_data/sql_files/input/*.sql` | Per-table INSERT statements for test data | Done |
| `utils/test_data/txt/*.txt` | Medium (24,030 rows) and large (1.4M rows) test data, no deleted_at | Done |
| `utils/test_data/csv/*.csv` | CSV copies of test data | Done |
| `utils/test_data/frame_to_csv.py` | writeCleanResults (Excel) + writeCleanResultsCSV helpers | Done |
| `utils/test_data/csv_to_sql.py` | CSV → SQL INSERT converter | Done |
| `utils/test_data/raw_to_csv.py` | Raw txt → CSV converter | Done |
| `docs/test_data_final.sql` | INSERT statements, 2,065 rows, no deleted_at | Done |
| `docs/vehicle_dw_schema_v3.docx` | Standalone schema reference (source + target DDL, mappings, assertions, verification SQL) | Done |
| `outputs/assertion_failures/` | Timestamped Excel failure reports from source and post-migration assertions | Runtime output |
| `outputs/assertion_output/clean_data.xlsx` | Clean data Excel output from source assertions | Runtime output |
| `outputs/clean_data_csv/` | Per-table clean CSV outputs | Runtime output |
| `outputs/parquet/` | Timestamped Parquet snapshots of clean source data | Runtime output |
| `.env` | DB credentials for source, target, and Snowflake (not committed) | Required |

---

## Environment variables (.env)

```
# MySQL source
SRC_HOST=localhost
SRC_PORT=3306
SRC_USER=<user>
SRC_PASSWORD=<password>

# MySQL target
TGT_HOST=localhost
TGT_PORT=3306
TGT_USER=<user>
TGT_PASSWORD=<password>

# Snowflake
SF_ACCOUNT=<account>
SF_USER=<user>
SF_PASSWORD=<password>
SF_DATABASE=<database>
SF_SCHEMA=<schema>
SF_WAREHOUSE=<warehouse>
SF_ROLE=<role>
```

---

## Key design decisions

### Why date windows instead of LIMIT/OFFSET
LIMIT N OFFSET M on large tables becomes slower as M grows because MySQL must
scan and discard M rows. Date windows are reproducible, safe to replay, and
predictable in size. `run_windowed()` defaults to 30-day windows.

### Assertion execution modes
All three modes (run_all_tables / run_one_table / run_one_row) share the same
AssertionRule definitions and FailureCollector infrastructure. UNIQUENESS rules
are automatically skipped in run_one_row mode (require full column Series).

### Failure report output
Written to `outputs/assertion_failures/` as timestamped Excel files.
Source failures: `source_failures_<ts>.xlsx`. Post-migration: `post_migration_failures_<ts>.xlsx`.
One sheet per table, auto-sized columns. Only written if failures exist.

### Parquet staging
`makeParquet.py` saves clean source DataFrames to `outputs/parquet/<timestamp>/`.
Used in `custom_execution.py` after source assertions. Not part of the main
pipeline.py flow — available for debugging and offline analysis.

### Streamlit UI
`UI/app.py` supports two modes:
- **Snowflake**: upload source Excel → runs `snowflake/run_pipeline.py`
- **Offline (Local)**: optional watermark date/time pickers → runs `local_execution.py`
Both stream subprocess stdout/stderr live into the UI. Launch via `python start_app.py`.
