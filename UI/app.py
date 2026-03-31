"""
UI/app.py
---------
Streamlit front-end for the Snowflake ETL pipeline.

Run
---
    python snowflake/UI/app.py
"""
import subprocess
import sys
from pathlib import Path
import streamlit as st
import datetime

_UI_DIR        = Path(__file__).parent
_ROOT_DIR      = _UI_DIR.parent
_SNOWFLAKE_DIR = _ROOT_DIR/"snowflake"

_SOURCE_EXCEL_SF    = _SNOWFLAKE_DIR / "source_data.xlsx"
_PIPELINE_SNOWFLAKE = _SNOWFLAKE_DIR / "run_pipeline.py"
_PIPELINE_LOCAL     = _ROOT_DIR / "local_execution.py"

st.title("ETL Pipeline")

mode = st.radio("Select pipeline mode", ["Snowflake", "Offline (Local)"], horizontal=True)

if mode == "Snowflake":
    uploaded = st.file_uploader("Browse source Excel file", type=["xlsx"])
    if uploaded:
        st.success(f"File ready: **{uploaded.name}**")
    ready = uploaded is not None
    # Clear watermarks when switching to Snowflake mode
    st.session_state["watermark_from"] = None
    st.session_state["watermark_to"] = None

else:
    uploaded = None
    st.info("Offline mode uses local source data — no file upload needed.")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Watermark From**")
        wm_from_date = st.date_input("From date", value=None, key="wm_from_date")
        wm_from_time = st.time_input("From time", value=datetime.time(0, 0, 0), key="wm_from_time")
    with col2:
        st.markdown("**Watermark To**")
        wm_to_date = st.date_input("To date", value=None, key="wm_to_date")
        wm_to_time = st.time_input("To time", value=datetime.time(23, 59, 59), key="wm_to_time")

    # Build final strings only if both dates are picked
    st.session_state["watermark_from"] = f"{wm_from_date} {wm_from_time}" if wm_from_date else None
    st.session_state["watermark_to"] = f"{wm_to_date} {wm_to_time}" if wm_to_date else None
    ready = True  # dates are optional — pipeline handles None fine

if st.button("▶ Run Pipeline", disabled=not ready):
    if mode == "Snowflake":
        with open(_SOURCE_EXCEL_SF, "wb") as f:
            f.write(uploaded.getbuffer())
        pipeline = _PIPELINE_SNOWFLAKE
        cwd      = _SNOWFLAKE_DIR
    else:
        pipeline = _PIPELINE_LOCAL
        cwd      = _ROOT_DIR

    # Build the command
    cmd = [sys.executable, str(pipeline)]
    if mode == "Offline (Local)":
        if st.session_state.get("watermark_from"):
            cmd += ["--watermark-from", st.session_state.get("watermark_from")]
        if st.session_state.get("watermark_to"):
            cmd += ["--watermark-to", st.session_state.get("watermark_to")]

    st.info(f"{'Snowflake' if mode == 'Snowflake' else 'Offline'} pipeline started — streaming logs below…")
    log_box = st.empty()
    lines: list[str] = []

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        cwd=str(cwd),
    )

    for line in proc.stdout:
        lines.append(line.rstrip())
        log_box.code("\n".join(lines), language="log")

    proc.stdout.close()
    proc.wait()

    if proc.returncode == 0:
        st.success("✅ Pipeline completed successfully.")
    else:
        st.error(f"❌ Pipeline failed (exit code {proc.returncode}).")
