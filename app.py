# ============================================================
# app.py — Aktieanalys & investeringsförslag (modulversion)
#
#  - Minimal toppnivå som bara:
#       • Sätter upp Streamlit-layout
#       • Laddar data/fx in i session via sheets_io._load_data_into_session()
#       • Visar sidomeny
#       • Routar till sid-funktioner i app_pages.py
#
#  Alla tunga grejer ligger i:
#    core_utils.py   – helpers, format mm.
#    sheets_io.py    – Google Sheets I/O, settings, FX
#    yahoo_fetch.py  – råhämtning från Yahoo Finance
#    valuation.py    – fair value/riktkurs-beräkningar
#    analysis_ui.py  – analys/ranking-komponenter
#    app_pages.py    – page_*-funktioner (UI-sidor)
# ============================================================

from __future__ import annotations

import streamlit as st

from sheets_io import _load_data_into_session
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
# Grundinställningar UI
# =========================
st.set_page_config(
    page_title="Aktieanalys & investeringsförslag",
    layout="wide",
)

# Lite smalare mittkolumn (som i gamla appen)
st.markdown(
    "<style>section.main > div {max-width: 1400px;}</style>",
    unsafe_allow_html=True,
)


# =========================
# MAIN
# =========================
def main() -> None:
    st.title("📈 Aktieanalys & investeringsförslag")

    # Ladda in DATA, FX, SETTINGS m.m. till session om de saknas
    _load_data_into_session()

    # -------------------------
    # Sidebar-navigering
    # -------------------------
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

    # -------------------------
    # Routing till sidorna
    # -------------------------
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
        # Fallback om något konstigt händer med sidnamnet
        page_analysis()


if __name__ == "__main__":
    main()
