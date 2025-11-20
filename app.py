# ============================================================
# app.py — Aktieanalys & investeringsförslag (modulversion)
#
#  - Använder modulerna:
#       • core_utils.py
#       • sheets_io.py
#       • yahoo_fetch.py
#       • valuation.py
#       • analysis_ui.py
#       • app_pages.py
#
#  - Robust import-logik:
#       • Försöker importera _load_data_into_session
#       • Om den saknas: testar load_data_into_session
#       • Om båda saknas: bygger en fallback-loader via read_data_df/get_fx_map
#       • Visar exakt Python-fel i UI om något import-fel kvarstår
# ============================================================

from __future__ import annotations

import streamlit as st

# -------------------------
# Globala import-fel
# -------------------------
IMPORT_ERROR = None
DATA_LOADER = None  # funktion som laddar DATA + FX i session_state


# ------------------------------------------------
# 1) Försök importera data-loader från sheets_io
# ------------------------------------------------
try:
    # Primärt namn (om vi skapat det så)
    from sheets_io import _load_data_into_session as DATA_LOADER  # type: ignore
except ImportError:
    try:
        # Alternativt namn utan underscore
        from sheets_io import load_data_into_session as DATA_LOADER  # type: ignore
    except Exception as e:
        IMPORT_ERROR = e
except Exception as e:
    IMPORT_ERROR = e

# ------------------------------------------------
# 2) Om loadern fortfarande saknas: bygg fallback
# ------------------------------------------------
if DATA_LOADER is None:
    try:
        # Vi försöker hämta byggstenar och bygga en enkel loader
        from core_utils import get_fx_map
        from sheets_io import read_data_df

        def DATA_LOADER() -> None:  # type: ignore
            df = read_data_df()
            st.session_state["DATA"] = df
            st.session_state["FX"] = get_fx_map()

    except Exception as e:
        # Om vi redan hade ett import-fel, behåll det första
        if IMPORT_ERROR is None:
            IMPORT_ERROR = e


# ------------------------------------------------
# 3) Importera sid-funktionerna (app_pages)
# ------------------------------------------------
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
    if IMPORT_ERROR is None:
        IMPORT_ERROR = e

    # Skapa stubbar så att appen inte kraschar när vi routar
    def _page_stub() -> None:
        st.error(
            "Sidorna kunde inte importeras på grund av ett import-fel i modulerna.\n\n"
            f"Teknisk detalj: `{repr(IMPORT_ERROR)}`"
        )

    page_analysis = page_ranking = page_buy_suggestions = page_editor = \
        page_add_ticker = page_portfolio = page_batch = page_settings = \
        page_snapshot = _page_stub  # type: ignore


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

    # Om vi har ett import-fel som vi inte kunnat lösa → visa det och stoppa
    if IMPORT_ERROR is not None and DATA_LOADER is None:
        st.error(
            "❌ Tekniskt fel vid import av moduler.\n\n"
            "Exakt Python-fel var:\n\n"
            f"`{repr(IMPORT_ERROR)}`\n\n"
            "Kontrollera att alla filer (core_utils.py, sheets_io.py, yahoo_fetch.py, "
            "valuation.py, analysis_ui.py, app_pages.py) finns i samma mapp som app.py "
            "och att det inte finns några stavfel i filnamnen eller exports."
        )
        st.stop()

    # Ladda DATA + FX i session med vald loader
    if DATA_LOADER is not None:
        try:
            DATA_LOADER()
        except Exception as e:
            st.error(
                "Kunde inte ladda data från Google Sheets.\n\n"
                f"Teknisk detalj: `{repr(e)}`"
            )
            st.stop()
    else:
        st.error("DATA_LOADER saknas – kunde inte ladda data.")
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
