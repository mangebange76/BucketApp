# app.py — Aktieanalys & investeringsförslag (modul-variant)
# ----------------------------------------------------------
# Den här filen är bara "router" + två tunna page-wrappers:
#  - page_analysis()  → använder render_analysis_view() i analysis_ui.py
#  - page_ranking()   → använder render_ranking_view()  i analysis_ui.py
#
# All logik för:
#  - helpers/konstanter/FX/Settings/Data I/O  → core_utils.py + sheets_io.py
#  - Yahoo-hämtning + fair value-beräkningar  → yahoo_fetch.py + valuation.py
#  - UI för Editor / Lägg till / Portfölj /
#    Massuppdatering / Köpförslag & säljförslag → app_pages.py
#  - Analys- & ranking-tabeller               → analysis_ui.py
# ----------------------------------------------------------

from __future__ import annotations

# Standard
from typing import Any, Dict
import pandas as pd
import streamlit as st

# Egna moduler
# CHANGED: importera now_stamp för tydlig “senast omladdad”-text
from core_utils import get_fx_map, _load_data_into_session, now_stamp  # wrappers mot sheets_io
from sheets_io import read_data_df, get_settings_map, refresh_fx_live
from analysis_ui import render_analysis_view, render_ranking_view
from app_pages import (
    page_buy_suggestions,
    page_editor,
    page_add_ticker,
    page_portfolio,
    page_batch,
    page_settings,
    page_snapshot,
)


# =========================
# Page-wrappers för Analys & Ranking
# =========================

def page_analysis() -> None:
    """Analys-vy: enskild ticker med fair value + metodtabell."""
    st.header("📊 Analys – enskild ticker")

    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df

    if df is None or df.empty:
        st.warning("Ingen data att analysera. Fyll på Data-bladet först.")
        return

    settings = get_settings_map()
    fx_map = st.session_state.get("FX") or get_fx_map()
    st.session_state["FX"] = fx_map

    render_analysis_view(df, settings, fx_map)


def page_ranking() -> None:
    """Ranking-vy: lista över tickers sorterat på uppsida."""
    st.header("🏆 Ranking – uppsida per ticker")

    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df

    if df is None or df.empty:
        st.warning("Ingen data att ranka. Fyll på Data-bladet först.")
        return

    settings = get_settings_map()
    fx_map = st.session_state.get("FX") or get_fx_map()
    st.session_state["FX"] = fx_map

    render_ranking_view(df, settings, fx_map)


# =========================
# MAIN
# =========================

def main() -> None:
    st.set_page_config(
        page_title="Aktieanalys & investeringsförslag",
        layout="wide",
    )
    st.markdown("<style>section.main > div {max-width: 1500px;}</style>", unsafe_allow_html=True)

    st.title("📈 Aktieanalys & investeringsförslag")

    # Se till att DATA finns i session
    _load_data_into_session()

    # Uppdatera valutakurser EN gång per session (hämtar live från Yahoo + skriver till Sheets)
    if not st.session_state.get("FX_REFRESHED", False):
        try:
            refresh_fx_live()
            st.session_state["FX_REFRESHED"] = True
        except Exception as e:
            # Visa bara lite info i sidopanelen – appen ska inte krascha p.g.a. FX
            st.sidebar.error(f"Valutakurser kunde inte uppdateras automatiskt: {e}")

    # Läs Settings + FX-karta efter ev. uppdatering
    settings: Dict[str, Any] = get_settings_map()
    fx_map = get_fx_map()
    st.session_state["FX"] = fx_map

    # Sidebar-navigering
    st.sidebar.markdown("## 🧭 Navigering")

    # CHANGED: Refresh-knapp i main (säkert ställe som alltid körs)
    if st.sidebar.button("🔄 Läs om Data (från Sheets)", key="reload_data_btn", use_container_width=True):
        try:
            # Blås ev. cachar som kan göra att ranking “fastnar”
            try:
                st.cache_data.clear()
            except Exception:
                pass

            # Läs om Data från Sheets
            st.session_state["DATA"] = read_data_df()
            st.session_state["DATA_RELOADED_TS"] = now_stamp()

            # Rensa ev. session-nycklar som kan hålla kvar gammal ranking
            for k in list(st.session_state.keys()):
                ku = str(k).upper()
                if ku.startswith("RANK") or ku.startswith("RANKING") or ku.startswith("ANALYSIS"):
                    try:
                        del st.session_state[k]
                    except Exception:
                        pass

            st.sidebar.success("Data omladdad. Kör om…")
        except Exception as e:
            st.sidebar.error(f"Kunde inte läsa om Data: {e}")
        st.rerun()

    # CHANGED: liten “bevisrad” så du ser att klicket faktiskt gjorde något
    if st.session_state.get("DATA_RELOADED_TS"):
        st.sidebar.caption(f"Data omladdad: {st.session_state['DATA_RELOADED_TS']}")

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

    fx_ts = settings.get("FX_LAST_UPDATE_TS")
    if fx_ts:
        st.sidebar.caption(f"Valutakurser uppdaterade senast: {fx_ts}")

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
