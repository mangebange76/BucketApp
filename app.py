# app.py – huvudfil (enbart router + grund-setup)

from __future__ import annotations

import streamlit as st

# Våra egna moduler
from core_utils import get_fx_map, get_settings_map
from sheets_io import read_data_df
from app_pages import (
    page_analysis,
    page_ranking,
    page_buy_suggestions,
    page_editor,
    page_add_ticker,
    page_portfolio,
    page_batch,
    page_settings,
    page_snapshot,
)

# =========================
# UI & Grundinställningar
# =========================
st.set_page_config(
    page_title="Aktieanalys & investeringsförslag",
    layout="wide",
)
st.markdown("<style>section.main > div {max-width: 1500px;}</style>", unsafe_allow_html=True)


def _ensure_session_data():
    """Se till att DATA, FX och SETTINGS finns i session_state."""
    if "DATA" not in st.session_state:
        st.session_state["DATA"] = read_data_df()
    if "FX" not in st.session_state:
        st.session_state["FX"] = get_fx_map()
    if "SETTINGS_MAP" not in st.session_state:
        st.session_state["SETTINGS_MAP"] = get_settings_map()


def main():
    st.title("📈 Aktieanalys & investeringsförslag")

    _ensure_session_data()

    # Sidebar-navigering
    st.sidebar.markdown("## 🧭 Navigering")
    page = st.sidebar.radio(
        "Välj vy",
        [
            "📊 Analys",
            "🏆 Ranking",
            "🛒 Köpförslag & säljförslag",
            "✏️ Editor",
            "➕ Lägg till ticker",
            "📦 Portfölj",
            "🧩 Massuppdatering",
            "⚙️ Settings",
            "🕒 Snapshot",
        ],
        index=0,
    )

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Data hämtas från Google Sheets + Yahoo Finance.\n"
        "Riktkurser beräknas i handelsvalutan (ingen FX på EPS/targets)."
    )

    # Routing
    if page == "📊 Analys":
        page_analysis()
    elif page == "🏆 Ranking":
        page_ranking()
    elif page == "🛒 Köpförslag & säljförslag":
        page_buy_suggestions()
    elif page == "✏️ Editor":
        page_editor()
    elif page == "➕ Lägg till ticker":
        page_add_ticker()
    elif page == "📦 Portfölj":
        page_portfolio()
    elif page == "🧩 Massuppdatering":
        page_batch()
    elif page == "⚙️ Settings":
        page_settings()
    elif page == "🕒 Snapshot":
        page_snapshot()
    else:
        page_analysis()


if __name__ == "__main__":
    main()
