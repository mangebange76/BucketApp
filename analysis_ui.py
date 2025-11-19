# analysis_ui.py — Analys- & Ranking-UI (byggstenar)
#
#  - render_analysis_view(): enskild ticker → fair value + metodtabell
#  - render_ranking_view(): lista/sortering på uppsida per vald horisont

from __future__ import annotations

import math
import datetime as dt
from typing import Any, Dict, Optional, List

import pandas as pd
import streamlit as st

from core_utils import _f, _pos
from valuation import compute_methods_for_row


# -------------------------
# Formatteringshjälp
# -------------------------
def _fmt2(x: Any) -> str:
    v = _f(x)
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return ""
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)


def _pct_change(target: Optional[float], price: Optional[float]) -> Optional[float]:
    t, p = _pos(target), _pos(price)
    if t is None or p is None or p == 0:
        return None
    try:
        return (t - p) / p * 100.0
    except Exception:
        return None


def _pick_target(payload: Dict[str, Any], horizon: str) -> Optional[float]:
    h = str(horizon or "").lower()
    if h in ("idag", "today", "0", "now"):
        return _f(payload.get("target_today"))
    if h in ("1y", "1 år", "1 ar", "1"):
        return _f(payload.get("target_1y"))
    if h in ("2y", "2 år", "2 ar", "2"):
        return _f(payload.get("target_2y"))
    if h in ("3y", "3 år", "3 ar", "3"):
        return _f(payload.get("target_3y"))
    return _f(payload.get("target_today"))


# -------------------------
# Kompakt summeringsrad (för tabell/CSV)
# -------------------------
def _build_summary_row(row: pd.Series, payload: Dict[str, Any]) -> Dict[str, Any]:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H.%M.%S")
    return {
        "Timestamp": ts,
        "Ticker": str(row.get("Ticker") or "").upper(),
        "Valuta": (payload.get("currency") or "USD"),
        "Aktuell kurs (0)": _f(payload.get("price")),
        "Riktkurs idag": _f(payload.get("target_today")),
        "Riktkurs 1 år": _f(payload.get("target_1y")),
        "Riktkurs 2 år": _f(payload.get("target_2y")),
        "Riktkurs 3 år": _f(payload.get("target_3y")),
        "Bull 1 år": _f(payload.get("bull_1y")),
        "Bear 1 år": _f(payload.get("bear_1y")),
        "Metod": str(payload.get("method") or "fair_value_v3_auto"),
        "Input-sammanfattning": str(payload.get("Input-sammanfattning") or ""),
        "Kommentar": str(payload.get("note") or ""),
    }


# -------------------------
# Render: metodtabell (expander)
# -------------------------
def _render_methods_table(payload: Dict[str, Any]) -> None:
    dfm = payload.get("methods_df")
    if dfm is None or (hasattr(dfm, "empty") and dfm.empty):
        st.info("Inga metodvärden att visa.")
        return
    dfv = dfm.copy()
    for c in ["Idag", "1 år", "2 år", "3 år"]:
        if c in dfv.columns:
            dfv[c] = dfv[c].map(
                lambda x: None if _f(x) is None else float(f"{float(x):.6f}")
            )
    st.dataframe(dfv, use_container_width=True, hide_index=True)


# -------------------------
# Analys-vy (enskild ticker)
# -------------------------
def render_analysis_view(
    df: pd.DataFrame,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
) -> None:
    if df is None or df.empty or "Ticker" not in df.columns:
        st.warning("Ingen data att analysera.")
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    tickers = [t for t in tickers if t.strip() != ""]
    tickers.sort()

    c1, c2 = st.columns([1, 2], gap="large")
    with c1:
        sel = st.selectbox("Välj ticker", tickers, index=0 if tickers else None)
        row = df[df["Ticker"].astype(str).str.upper() == str(sel).upper()]
        if row.empty:
            st.error("Kunde inte hitta vald ticker i Data-bladet.")
            return
        row = row.iloc[0]

        payload = compute_methods_for_row(row, settings, fx_map)

        price   = _f(payload.get("price"))
        fv_t0   = _f(payload.get("target_today"))
        fv_1y   = _f(payload.get("target_1y"))
        fv_2y   = _f(payload.get("target_2y"))
        fv_3y   = _f(payload.get("target_3y"))
        curr    = payload.get("currency") or "USD"

        st.metric("Aktuell kurs", f"{_fmt2(price)} {curr}")
        st.metric("Fair Value (idag)", f"{_fmt2(fv_t0)} {curr}", delta=None)
        st.caption(payload.get("Input-sammanfattning") or "")

        up0 = _pct_change(fv_t0, price)
        up1 = _pct_change(fv_1y, price)
        up2 = _pct_change(fv_2y, price)
        up3 = _pct_change(fv_3y, price)

        st.write("**Uppsida mot aktuell kurs**")
        st.write(
            f"Idag: {(_fmt2(up0) + ' %') if up0 is not None else '-'}  •  "
            f"1 år: {(_fmt2(up1) + ' %') if up1 is not None else '-'}  •  "
            f"2 år: {(_fmt2(up2) + ' %') if up2 is not None else '-'}  •  "
            f"3 år: {(_fmt2(up3) + ' %') if up3 is not None else '-'}"
        )

    with c2:
        with st.expander("Metoder & detaljer", expanded=True):
            _render_methods_table(payload)
            st.code(payload.get("Input-sammanfattning") or "", language="text")

    summary = _build_summary_row(row, payload)
    st.subheader("Sammanfattning (denna ticker)")
    df_sum = pd.DataFrame([summary])
    st.dataframe(df_sum, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.caption("Hela databasen (enkel tabell):")
    st.dataframe(df, use_container_width=True)


# -------------------------
# Bulk: beräkna FV för flera rader (ranking)
# -------------------------
def _bulk_compute(
    df: pd.DataFrame,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
) -> pd.DataFrame:
    rows: List[tuple[pd.Series, Dict[str, Any]]] = []
    tickers = df["Ticker"].dropna().astype(str).tolist() if "Ticker" in df.columns else []
    tickers = [t for t in tickers if t.strip() != ""]
    prog = st.progress(0.0, text="Beräknar fair value…")
    n = len(tickers)
    for i, t in enumerate(tickers, start=1):
        try:
            row = df[df["Ticker"].astype(str).str.upper() == t.upper()].iloc[0]
            payload = compute_methods_for_row(row, settings, fx_map)
            rows.append((row, payload))
        except Exception as e:
            st.warning(f"{t}: beräkningsfel – {e}")
        prog.progress(i / max(1, n), text=f"Beräknar fair value… ({i}/{n})")
    prog.empty()

    out: List[Dict[str, Any]] = []
    for row, payload in rows:
        d = _build_summary_row(row, payload)
        price = _f(payload.get("price"))
        d["Uppsida % (idag)"] = _pct_change(d["Riktkurs idag"], price)
        d["Uppsida % (1 år)"] = _pct_change(d["Riktkurs 1 år"], price)
        d["Uppsida % (2 år)"] = _pct_change(d["Riktkurs 2 år"], price)
        d["Uppsida % (3 år)"] = _pct_change(d["Riktkurs 3 år"], price)
        out.append(d)

    cols = [
        "Timestamp","Ticker","Valuta","Aktuell kurs (0)",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        "Bull 1 år","Bear 1 år","Metod","Input-sammanfattning","Kommentar",
        "Uppsida % (idag)","Uppsida % (1 år)","Uppsida % (2 år)","Uppsida % (3 år)",
    ]
    df_out = pd.DataFrame(out)
    ordered = [c for c in cols if c in df_out.columns] + [c for c in df_out.columns if c not in cols]
    df_out = df_out.reindex(columns=ordered)
    return df_out


# -------------------------
# Ranking-vy
# -------------------------
def render_ranking_view(
    df: pd.DataFrame,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
) -> None:
    if df is None or df.empty or "Ticker" not in df.columns:
        st.warning("Ingen data att ranka.")
        return

    st.subheader("Ranking – uppsida mot aktuell kurs")
    colA, colB = st.columns([1, 2], gap="medium")
    with colA:
        horizon = st.selectbox("Horisont", ["Idag", "1 år", "2 år", "3 år"], index=1)
        show_n = st.slider("Visa topp N", min_value=5, max_value=200, value=50, step=5)
    with colB:
        st.caption("Värden i aktiens valuta. Framtida riktkurser använder Bucket-MoS enligt beräkningsmotorn.")

    df_rank = _bulk_compute(df, settings, fx_map)
    hmap = {
        "Idag": "Uppsida % (idag)",
        "1 år": "Uppsida % (1 år)",
        "2 år": "Uppsida % (2 år)",
        "3 år": "Uppsida % (3 år)",
    }
    up_col = hmap.get(horizon, "Uppsida % (1 år)")
    if up_col not in df_rank.columns:
        st.error("Kunde inte beräkna uppsida.")
        return

    df_show = df_rank.sort_values(by=up_col, ascending=False).head(show_n).copy()

    show_cols = [
        "Ticker","Valuta","Aktuell kurs (0)",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        up_col,
    ]
    show_cols = [c for c in show_cols if c in df_show.columns]
    st.dataframe(df_show[show_cols], use_container_width=True, hide_index=True)

    with st.expander("Visa alla kolumner (rankingresultat)", expanded=False):
        st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.caption("Hela databasen (enkel tabell):")
    st.dataframe(df, use_container_width=True)
