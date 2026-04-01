# UI/pages/2_Table.py
import datetime
import os
import sys
import logging
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
import UI.pipeline_functions as pf

st.set_page_config(page_title="Table — ETL Pipeline", layout="wide")
st.title("Single Table")

# ── Sub-mode ─────────────────────────────────────────────────────────────────
db_mode = st.radio("Database", ["Local", "Snowflake"], horizontal=True)

# ── Table selection ───────────────────────────────────────────────────────────
table_name = st.selectbox("Table", pf.TABLES, key="tbl_name")

# ── Inputs ────────────────────────────────────────────────────────────────────
watermark_from = watermark_to = None

if db_mode == "Local":
    st.markdown("### Watermark Range")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Watermark From**")
        wm_from_date = st.date_input("From date", value=None, key="tbl_wm_from_date")
        wm_from_time = st.time_input("From time", value=datetime.time(0, 0, 0), key="tbl_wm_from_time")
    with col2:
        st.markdown("**Watermark To**")
        wm_to_date = st.date_input("To date", value=None, key="tbl_wm_to_date")
        wm_to_time = st.time_input("To time", value=datetime.time(23, 59, 59), key="tbl_wm_to_time")

    watermark_from = f"{wm_from_date} {wm_from_time}" if wm_from_date else None
    watermark_to   = f"{wm_to_date} {wm_to_time}"     if wm_to_date   else None

else:  # Snowflake
    st.markdown("### Source Data")
    csv_path = pf._TABLE_CSVS_DIR / f"{table_name}.csv"

    tab_fetch, tab_upload = st.tabs(["⬇ Fetch from MySQL", "📂 Upload CSV"])

    with tab_fetch:
        st.caption(f"Pull `{table_name}` from MySQL and save to `snowflake_files/table_csvs/{table_name}.csv`.")
        col_wm1, col_wm2 = st.columns(2)
        with col_wm1:
            st.markdown("**Watermark From**")
            csv_wm_from_date = st.date_input("From date", value=None, key="csv_wm_from_date")
            csv_wm_from_time = st.time_input("From time", value=datetime.time(0, 0, 0), key="csv_wm_from_time")
        with col_wm2:
            st.markdown("**Watermark To**")
            csv_wm_to_date = st.date_input("To date", value=None, key="csv_wm_to_date")
            csv_wm_to_time = st.time_input("To time", value=datetime.time(23, 59, 59), key="csv_wm_to_time")

        csv_wm_from = f"{csv_wm_from_date} {csv_wm_from_time}" if csv_wm_from_date else None
        csv_wm_to   = f"{csv_wm_to_date} {csv_wm_to_time}"     if csv_wm_to_date   else None

        if st.button("⬇ Fetch Source CSV", key="tbl_fetch_csv"):
            fetch_box     = st.empty()
            fetch_handler = pf.attach_handler(fetch_box)
            try:
                pf.fetch_table_csv(table_name, watermark_from=csv_wm_from, watermark_to=csv_wm_to)
                st.success(f"✅ CSV saved: `snowflake_files/table_csvs/{table_name}.csv`")
            except Exception as e:
                st.error(f"❌ Fetch failed: {e}")
                logging.getLogger(__name__).exception("CSV fetch error")
            finally:
                pf.detach_handler(fetch_handler)

    with tab_upload:
        st.caption(f"Upload a CSV file for `{table_name}`. It will be saved to `snowflake_files/table_csvs/{table_name}.csv`.")
        uploaded = st.file_uploader(f"CSV for `{table_name}`", type=["csv"], key="tbl_csv_upload")
        if uploaded:
            pf._TABLE_CSVS_DIR.mkdir(exist_ok=True)
            with open(csv_path, "wb") as f:
                f.write(uploaded.getbuffer())
            st.success(f"✅ Uploaded and saved as `{table_name}.csv`")

    # CSV status
    if csv_path.exists():
        mtime = datetime.datetime.fromtimestamp(os.path.getmtime(csv_path))
        st.info(f"📄 CSV ready — last updated: `{mtime.strftime('%Y-%m-%d %H:%M:%S')}`")
    else:
        st.warning(f"No CSV found for `{table_name}` — fetch or upload one first.")

assert_mode = st.radio(
    "Assert against", ["Source", "Target", "Both"],
    horizontal=True, key="tbl_assert_mode"
)

# ── Ready check ───────────────────────────────────────────────────────────────
ready = csv_path.exists() if db_mode == "Snowflake" else True

# ── Run ───────────────────────────────────────────────────────────────────────
if st.button("▶ Run", disabled=not ready, key="tbl_run"):
    st.info(f"Asserting table `{table_name}` — streaming logs below…")
    log_box = st.empty()
    handler = pf.attach_handler(log_box)

    try:
        if db_mode == "Local":
            pf.assert_table_local(
                table_name=table_name,
                mode=assert_mode.lower(),
                watermark_from=watermark_from,
                watermark_to=watermark_to,
            )
        else:
            pf.assert_table_snowflake(
                table_name=table_name,
                mode=assert_mode.lower(),
            )
        st.success("✅ Completed successfully.")
    except NotImplementedError as e:
        st.warning(f"⚠️ Not yet wired up: {e}")
    except Exception as e:
        st.error(f"❌ Failed: {e}")
        logging.getLogger(__name__).exception("Pipeline error")
    finally:
        pf.detach_handler(handler)
