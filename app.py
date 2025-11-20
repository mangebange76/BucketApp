# ============================================================
# app.py — Aktieanalys & investeringsförslag (modulversion med fel-diagnostik)
# ============================================================

from __future__ import annotations

import streamlit as st

# För att kunna visa riktiga fel istället för Streamlits maskning
IMPORT_ERROR = None

# Försök importera sheets_io
try:
    from sheets_io import _load_data_into_session
except Exception as e:
    _load_data_into_session = None
    IMPORT_ERROR = e  # spara första felet

# Försök importera sidorna
try:
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
except Exception as e:
    # Om vi redan har ett import-fel, behåll det första;
    # annars spara detta.
    if IMPORT_ERROR is None:
        IMPORT_ERROR = e

    # Skapa dummy-funktioner så att namnen finns
    def _page_stub():
        st.error("Sidorna kunde inte importeras på grund av ett import-fel i modulerna.")
    page_analysis = page_ranking = page_buy_suggestions = page_editor = \
        page_add_ticker = page_portfolio = page_batch = page_settings = page_snapshot = _page_stub


# =========================
# Grundinställningar UI
# =========================
st.set_page_config(
    page_title="Aktieanalys & investeringsförslag",
    layout="wide",
)

st.markdown(
    "<style>section.main > div {max-width: 1400px;}</style>",
    unsafe_allow_html=True,
)


# =========================
# MAIN
# =========================
def main() -> None:
    st.title("📈 Aktieanalys & investeringsförslag")

    # Om vi har ett import-fel i någon modul: visa det tydligt och stoppa
    if IMPORT_ERROR is not None:
        st.error(
            "❌ Tekniskt fel vid import av moduler.\n\n"
            "Exakt Python-fel var:\n\n"
            f"`{repr(IMPORT_ERROR)}`\n\n"
            "Kontrollera att alla filer (core_utils.py, sheets_io.py, yahoo_fetch.py, "
            "valuation.py, analysis_ui.py, app_pages.py) finns i **samma mapp** som app.py "
            "och att det inte finns några stavfel i filnamnen eller imports."
        )
        st.stop()

    # Ladda DATA via sheets_io
    if _load_data_into_session is not None:
        _load_data_into_session()
    else:
        st.error("Kunde inte ladda data eftersom _load_data_into_session saknas.")
        st.stop()

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
