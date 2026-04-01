# UI/pages/3_Row.py
import sys
import logging
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
import importlib
import UI.pipeline_functions as pf
importlib.reload(pf)

st.set_page_config(page_title="Row — ETL Pipeline", layout="wide")
st.title("Row Assertions")

# ── Sub-mode ──────────────────────────────────────────────────────────────────
db_mode = st.radio("Database", ["Local", "Snowflake"], horizontal=True)

# ── Table selection ───────────────────────────────────────────────────────────
table_name = st.selectbox("Table", pf.TABLES, key="row_table")

# ── Auto PK display ───────────────────────────────────────────────────────────
if db_mode == "Local":
    source_pk  = pf._SOURCE_PK[table_name]
    target_pk  = pf._TARGET_PK[table_name]
    lookup_col = pf._TARGET_ROW_LOOKUP_COL[table_name]
    st.info(
        f"Source PK: **`{source_pk}`** → Target surrogate PK: **`{target_pk}`**  ·  "
        f"Always enter the **source** `{source_pk}` value — "
        f"target rows are looked up via `{lookup_col}`"
    )
else:
    sf_pk      = pf._SNOWFLAKE_PK[table_name]
    lookup_col = pf._SNOWFLAKE_ROW_LOOKUP_COL[table_name]
    st.info(
        f"Snowflake surrogate PK: **`{sf_pk}`**  ·  "
        f"Always enter the **source** PK value — "
        f"rows are looked up via `{lookup_col}`"
    )

# ── Assert mode ───────────────────────────────────────────────────────────────
# Snowflake is the target DB — running source rules against it is not meaningful.
# For Local, all three modes make sense.
if db_mode == "Snowflake":
    assert_mode = "target"
    st.info("🔹 Snowflake is the **target** — only target assertion rules are applied.")
else:
    assert_mode = st.radio(
        "Assert against", ["Source", "Target", "Both"],
        horizontal=True, key="row_assert_mode"
    ).lower()

# ── PK value input ────────────────────────────────────────────────────────────
st.markdown("### PK Values")
st.caption("Enter one or more source PK values, separated by commas or newlines.")

pk_raw = st.text_area(
    "PK values",
    placeholder="e.g.  1, 2, 3  or one per line",
    label_visibility="collapsed",
    height=80,
    key="pk_raw_input",
)

# Parse: split on commas and newlines, strip whitespace, cast to int where possible
pk_values = []
for token in pk_raw.replace(",", "\n").splitlines():
    token = token.strip()
    if not token:
        continue
    try:
        pk_values.append(int(token))
    except ValueError:
        pk_values.append(token)

# ── Ready check ───────────────────────────────────────────────────────────────
ready = len(pk_values) > 0
if not ready:
    st.warning("Enter at least one PK value.")

# ── Run ───────────────────────────────────────────────────────────────────────
if st.button("▶ Run Assertions", disabled=not ready, key="row_run"):
    st.info(f"Asserting {len(pk_values)} row(s) in `{table_name}` — streaming logs below…")
    st.caption(f"PK values: {pk_values}")
    log_box = st.empty()
    handler = pf.attach_handler(log_box)

    try:
        if db_mode == "Local":
            pf.assert_row_local(
                table_name=table_name,
                pk_values=pk_values,
                mode=assert_mode,
            )
        else:
            pf.assert_row_snowflake(
                table_name=table_name,
                pk_values=pk_values,
                mode=assert_mode,
            )
        st.success("✅ Assertions completed.")
    except Exception as e:
        st.error(f"❌ Failed: {e}")
        logging.getLogger(__name__).exception("Row assertion error")
    finally:
        pf.detach_handler(handler)
