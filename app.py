# ============================================================
# app.py — Aktieanalys & investeringsförslag (modulversion)
#
#  - Streamlit-setup
#  - Init av DATA + FX i session_state
#  - Sidebar-navigering
#  - Delegerar innehåll till app_pages.py
#
# Moduler:
#   core_utils.py   → helpers (t.ex. _f, _pos, now_stamp, m.m.)
#   sheets_io.py    → I/O mot Google Sheets (+ ev. get_fx_map)
#   yahoo_fetch.py  → Yahoo-hämtning
#   valuation.py    → beräkningsmotor
#   analysis_ui.py  → analys/ranking-UI
#   app_pages.py    → page_*-funktioner för alla vyer
# ============================================================

from __future__ import annotations

# ---------- Tredjepart ----------
import streamlit as st

# ---------- Egna moduler ----------

# DATA-laddning
from sheets_io import _load_data_into_session

# Försök hämta get_fx_map från sheets_io i första hand, annars core_utils.
try:
    from sheets_io import get_fx_map  # typiskt läge
except Exception:
    try:
        from core_utils import get_fx_map  # fallback om du har den där
    except Exception:
        # Sista fallback – appen funkar, men FX blir bara SEK=1.0
        def get_fx_map() -> dict[str, float]:
            return {"SEK": 1.0}

# Sidornas UI – implementeras i app_pages.py
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
# UI & grundinställningar
# =========================
st.set_page_config(
    page_title="Aktieanalys & investeringsförslag",
    layout="wide",
)

# Samma maxbredds-CSS som tidigare
st.markdown(
    "<style>section.main > div {max-width: 1500px;}</style>",
    unsafe_allow_html=True,
)

# -------------------------
# Initiera session_state
# -------------------------
def _ensure_session_state() -> None:
    """
    Säkerställ att DATA- och FX-objekt finns i st.session_state.
    Själva laddningen av DATA görs av sheets_io._load_data_into_session()
    med samma logik som i gamla Del 1/6.
    """
    # DATA (Google Sheet → st.session_state["DATA"])
    _load_data_into_session()

    # FX (valutakurser → st.session_state["FX"])
    if "FX" not in st.session_state or not isinstance(st.session_state["FX"], dict):
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception as e:
            st.warning(f"Kunde inte ladda valutakurser (FX): {e}")
            st.session_state["FX"] = {"SEK": 1.0}

# -------------------------
# MAIN
# -------------------------
def main():
    st.title("📈 Aktieanalys & investeringsförslag")

    # Se till att DATA + FX är laddat innan vi visar någon sida
    _ensure_session_state()

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

    # Liten info om datakälla
    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Data hämtas från Google Sheets + Yahoo Finance.\n"
        "Riktkurser beräknas i handelsvalutan (ingen FX på EPS/targets)."
    )

    # Routing till respektive sida (implementerade i app_pages.py)
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
        # Fallback om något konstigt händer
        page_analysis()


if __name__ == "__main__":
    main()
