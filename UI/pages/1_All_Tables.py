# UI/pages/1_All_Tables.py
import datetime
import os
import sys
import logging
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
import UI.pipeline_functions as pf

st.set_page_config(page_title="All Tables — ETL Pipeline", layout="wide")
st.title("All Tables")

# ── Sub-mode ─────────────────────────────────────────────────────────────────
db_mode = st.radio("Database", ["Local", "Snowflake"], horizontal=True)
action  = st.radio("Action",   ["Run Pipeline", "Assertions"], horizontal=True)

# ── Inputs ───────────────────────────────────────────────────────────────────
watermark_from = watermark_to = None

if db_mode == "Local":
    st.markdown("### Watermark Range")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Watermark From**")
        wm_from_date = st.date_input("From date", value=None, key="at_wm_from_date")
        wm_from_time = st.time_input("From time", value=datetime.time(0, 0, 0), key="at_wm_from_time")
    with col2:
        st.markdown("**Watermark To**")
        wm_to_date = st.date_input("To date", value=None, key="at_wm_to_date")
        wm_to_time = st.time_input("To time", value=datetime.time(23, 59, 59), key="at_wm_to_time")

    watermark_from = f"{wm_from_date} {wm_from_time}" if wm_from_date else None
    watermark_to   = f"{wm_to_date} {wm_to_time}"     if wm_to_date   else None

else:  # Snowflake
    st.markdown("### Source Data")

    tab_fetch, tab_upload = st.tabs(["⬇ Fetch from MySQL", "📂 Upload Excel"])

    with tab_fetch:
        st.caption(
            "Pull all four tables from MySQL and save to "
            "`snowflake_files/source_data.xlsx` (one sheet per table)."
        )
        col_wm1, col_wm2 = st.columns(2)
        with col_wm1:
            st.markdown("**Watermark From**")
            xl_wm_from_date = st.date_input("From date", value=None, key="at_xl_wm_from_date")
            xl_wm_from_time = st.time_input("From time", value=datetime.time(0, 0, 0), key="at_xl_wm_from_time")
        with col_wm2:
            st.markdown("**Watermark To**")
            xl_wm_to_date = st.date_input("To date", value=None, key="at_xl_wm_to_date")
            xl_wm_to_time = st.time_input("To time", value=datetime.time(23, 59, 59), key="at_xl_wm_to_time")

        xl_wm_from = f"{xl_wm_from_date} {xl_wm_from_time}" if xl_wm_from_date else None
        xl_wm_to   = f"{xl_wm_to_date} {xl_wm_to_time}"     if xl_wm_to_date   else None

        if st.button("⬇ Fetch Source Excel", key="at_fetch_excel"):
            fetch_box     = st.empty()
            fetch_handler = pf.attach_handler(fetch_box)
            try:
                pf.fetch_source_excel(watermark_from=xl_wm_from, watermark_to=xl_wm_to)
                st.success("✅ Saved to `snowflake_files/source_data.xlsx`")
            except Exception as e:
                st.error(f"❌ Fetch failed: {e}")
                logging.getLogger(__name__).exception("Excel fetch error")
            finally:
                pf.detach_handler(fetch_handler)

    with tab_upload:
        st.caption("Upload a `source_data.xlsx` file. It overwrites the existing file.")
        uploaded = st.file_uploader("Source Excel file", type=["xlsx"], key="at_excel_upload")
        if uploaded:
            with open(pf._SOURCE_EXCEL, "wb") as f:
                f.write(uploaded.getbuffer())
            st.success(f"✅ Uploaded and saved as `source_data.xlsx`")

    # Excel status
    if pf._SOURCE_EXCEL.exists():
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(pf._SOURCE_EXCEL))
        st.info(f"📄 `source_data.xlsx` ready — last updated: `{mtime.strftime('%Y-%m-%d %H:%M:%S')}`")
    else:
        st.warning("`source_data.xlsx` not found — fetch or upload it first.")

if action == "Assertions":
    assert_mode = st.radio(
        "Assert against", ["Source", "Target", "Both"],
        horizontal=True, key="at_assert_mode"
    )
else:
    assert_mode = None

# ── Ready check ──────────────────────────────────────────────────────────────
if db_mode == "Snowflake":
    ready = pf._SOURCE_EXCEL.exists()
else:
    ready = True

# ── Run ───────────────────────────────────────────────────────────────────────
if st.button("▶ Run", disabled=not ready, key="at_run"):
    st.info("Pipeline started — streaming logs below…")
    log_box = st.empty()
    handler = pf.attach_handler(log_box)

    try:
        if db_mode == "Local":
            if action == "Run Pipeline":
                pf.run_all_tables_local(
                    watermark_from=watermark_from,
                    watermark_to=watermark_to,
                )
            else:
                pf.assert_all_tables_local(
                    mode=assert_mode.lower(),
                    watermark_from=watermark_from,
                    watermark_to=watermark_to,
                )
        else:
            if action == "Run Pipeline":
                pf.run_all_tables_snowflake()
            else:
                pf.assert_all_tables_snowflake(mode=assert_mode.lower())
        st.success("✅ Completed successfully.")
    except NotImplementedError as e:
        st.warning(f"⚠️ Not yet wired up: {e}")
    except Exception as e:
        st.error(f"❌ Failed: {e}")
        logging.getLogger(__name__).exception("Pipeline error")
    finally:
        pf.detach_handler(handler)
