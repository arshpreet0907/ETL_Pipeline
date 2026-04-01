# UI/app.py
"""
ETL Pipeline — main entry point.
Navigate using the sidebar to select a pipeline mode.
"""
import streamlit as st

st.set_page_config(page_title="ETL Pipeline", layout="wide")

st.title("ETL Pipeline")
st.markdown("""
Use the **sidebar** to navigate between pipeline modes:

| Page | What it does |
|---|---|
| **All Tables** | Run or assert the full pipeline across all four tables |
| **Table** | Target a single table — run or assert |
| **Row** | Assert individual rows by primary key |

---
**Tables in scope:** `suppliers`, `vehicles`, `parts`, `quality_checks`
""")
