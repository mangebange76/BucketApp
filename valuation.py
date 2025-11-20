# ============================================================
# valuation.py – Beräkningsmotor (auto-val av metod & riktkurser)
#
#  - fetch_from_yahoo(): wrapper runt yahoo_fetch_for_ticker
#  - EPS-estimat från Yahoo (earnings_trend)
#  - AUTO-PROFIL: väljer vilka metodfamiljer som passar (per sektor/mått)
#  - Metodpriser: PE, EV/S, EV/EBITDA, P/B (+ placeholders för struktur)
#  - Multipel-decay & PE-ankare
#  - Fair Value idag = PE-growth-hybrid (EPS-bas * fair PE)
#  - Riktkurser 1–3 år = “bästa scenario” med MoS per bucket (A 5%, B 8%, C 12%)
#  - compute_methods_for_row() → DICT (targets + metadata + methods_df)
#  - compute_fair_values_for_row() → kompakt DICT för UI
# ============================================================

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

from core_utils import _f, _pos, _nz  # bara helpers här
from sheets_io import get_settings_map   # <-- FLYTTAD HIT
from yahoo_fetch import yahoo_fetch_for_ticker


# -------------------------
# Wrapper: Yahoo → beräkningsmotor
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Mappa yahoo_fetch_for_ticker() till stabila nycklar för beräkningsmotorn.
    Alla värden är i aktiens handelsvaluta.
    """
    snap = yahoo_fetch_for_ticker(ticker)
    return {
        "price":            _f(snap.get("Aktuell kurs")),
        "currency":         (snap.get("Valuta") or "USD"),
        "shares_out":       _f(snap.get("Utestående aktier")),
        "net_debt":         _f(snap.get("Net debt")),
        "rev_ttm":          _f(snap.get("Rev TTM")),
        "ebitda_ttm":       _f(snap.get("EBITDA TTM")),
        "eps_ttm":          _f(snap.get("EPS TTM")),
        "pe_ttm":           _f(snap.get("PE TTM")),
        "pe_fwd":           _f(snap.get("PE FWD")),
        "ev_rev":           _f(snap.get("EV/Revenue")),
        "ev_ebitda":        _f(snap.get("EV/EBITDA")),
        "p_b":              _f(snap.get("P/B")),
        "bvps":             _f(snap.get("BVPS")),
        "dps_annual":       _f(snap.get("Årlig utdelning")),
        # Historiska CAGRs kan saknas; beräkningsmotor hanterar None.
        "rev_cagr_hist":    _f(snap.get("Rev CAGR")),
        "eps_cagr_hist":    _f(snap.get("EPS CAGR")),
    }


# -------------------------
# Clamp-gränser (stabila)
# -------------------------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

# P/E-tak för cykliska/crypto (aktie-VALUTA, ingen FX)
PE_CAP_CRYPTO    = 8.0   # t.ex. MARA, RIOT, WULF m.fl.
PE_CAP_CYCLICAL  = 10.0  # shipping, tankers, energy, materials


# -------------------------
# Små hjälpare (beräkning)
# -------------------------
def _decay_multiple(mult0: Optional[float], years: int, decay: float,
                    floor_frac: float = 0.60) -> Optional[float]:
    """
    Exponentiell kompression av multipel:
      mult_y = mult0 * (1 - decay) ** years
    med golv på floor_frac * mult0.
    """
    m0 = _pos(mult0)
    if m0 is None:
        return None
    try:
        y = max(0, int(years))
        d = float(decay)
        factor = 1.0 - d
        if factor <= 0:
            m = m0 * floor_frac
        else:
            m = m0 * (factor ** y)
    except Exception:
        m = m0
    floor = m0 * float(floor_frac)
    return max(m, floor)


def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float) -> Optional[float]:
    pt = _pos(pe_ttm)
    pf = _pos(pe_fwd)
    if pt is None and pf is None:
        return None
    if pt is None:
        return pf
    if pf is None:
        return pt
    try:
        return float(w_ttm) * pt + (1.0 - float(w_ttm)) * pf
    except Exception:
        return None


def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float],
                          shares_fd: Optional[float]) -> Optional[float]:
    e = _pos(ev_target)
    s = _pos(shares_fd)
    if e is None or s is None:
        return None
    nd = _nz(net_debt, 0.0)
    try:
        return max(0.0, (e - nd) / s)
    except Exception:
        return None


def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e = _pos(eps)
    p = _pos(pe)
    if e is None or p is None:
        return None
    return e * p


def _ev_from_sales(rev: Optional[float], mult: Optional[float]) -> Optional[float]:
    r = _pos(rev)
    m = _pos(mult)
    if r is None or m is None:
        return None
    return r * m


def _ev_from_ebitda(ebitda: Optional[float], mult: Optional[float]) -> Optional[float]:
    m = _pos(mult)
    if ebitda is None or m is None:
        return None
    try:
        eb = float(ebitda)
        if not math.isfinite(eb) or eb <= 0:
            return None
    except Exception:
        return None
    return eb * m


def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb)
    b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b


# -------------------------
# EPS/REV paths
# -------------------------
def _eps_path_fill(
    eps_ttm: Optional[float],
    eps_1y: Optional[float],
    eps_2y: Optional[float],
    eps_cagr_hist: Optional[float],
    eps_cagr_long: Optional[float],
    rev_cagr_hist: Optional[float],
) -> Tuple[float, float, float, float]:
    e0 = _pos(eps_ttm) or 0.0
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)

    # Välj första tillgängliga tillväxtindikator
    g = None
    for cand in (eps_cagr_hist, eps_cagr_long, rev_cagr_hist, 0.0):
        if _f(cand) is not None:
            g = float(_f(cand))
            break

    if e1 is None:
        e1 = e0 * (1.0 + (g or 0.0))
    if e2 is None:
        e2 = (e1 or 0.0) * (1.0 + (g or 0.0))
    e3 = (e2 or 0.0) * (1.0 + (g or 0.0))
    return float(e0), float(e1), float(e2), float(e3)


def _ebitda_path(
    ebitda_ttm: Optional[float],
    rev0: Optional[float],
    rev1: Optional[float],
    rev2: Optional[float],
    rev3: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    b0 = _f(ebitda_ttm)
    if b0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return b0, b0, b0, b0

    def scale(r):
        try:
            return (b0 * (r / rev0)) if (r and rev0) else b0
        except Exception:
            return b0

    return b0, scale(rev1), scale(rev2), scale(rev3)


# -------------------------
# EPS-estimat från Yahoo
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    try:
        tk = yf.Ticker(ticker)
        try:
            trend = tk.get_earnings_trend()
        except Exception:
            trend = getattr(tk, "earnings_trend", None)

        if trend is None or (hasattr(trend, "empty") and trend.empty):
            return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}

        df = trend.copy()
        df.columns = [str(c).lower() for c in df.columns]

        def _avg(val):
            if isinstance(val, dict):
                for k in ("avg", "average", "mean"):
                    if k in val and _f(val[k]) is not None:
                        return _f(val[k])
            return _f(val)

        def _pick(period_aliases: List[str]):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for a in period_aliases:
                cur = m.str.contains(rf"^{a}$")
                mask = cur if mask is None else (mask | cur)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_next = _pick(["nextyear", "next fiscal year", "nextfiscalyear"])
        row_curr = _pick(["currentyear", "current fiscal year", "currentfiscalyear"])
        row_long = _pick(["longterm", "next5years", "next 5 years"])

        eps_1y = None
        if row_next is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg(row_next.get(col))
                    if eps_1y is not None:
                        break
        if eps_1y is None and row_curr is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg(row_curr.get(col))
                    if eps_1y is not None:
                        break

        eps_cagr_long = None
        if row_long is not None:
            for col in ["growth", "longtermgrowthrate"]:
                if col in df.columns and _f(row_long.get(col)) is not None:
                    eps_cagr_long = float(_f(row_long.get(col)))
                    break

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {
            "eps_1y": _f(eps_1y),
            "eps_2y": _f(eps_2y),
            "eps_cagr_long": _f(eps_cagr_long),
        }
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}


# -------------------------
# AUTO-PROFIL: välj metodfamiljer som passar
# -------------------------
def _auto_method_profile(row: pd.Series, y_snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returnerar vilka metodfamiljer som ska användas för FV-medianen.
    Familjer: 'pe', 'ev_s', 'ev_e', 'pb'

    OBS:
    - EV/S ('ev_s') och EV/EBITDA ('ev_e') är AVSTÄNGDA tills vi är säkra på enheter.
    - Vi använder bara:
        • 'pe' (P/E) där positiv EPS finns
        • 'pb' (P/B) för finans/REIT/BDC m.m. om data finns
    - För shipping/energy/materials/crypto sätts ett P/E-TAK (pe_cap) för att
      undvika absurda FV på cykliska super-earnings.
    """
    sektor = str(_nz(row.get("Sektor"), "")).lower()
    ticker = str(_nz(row.get("Ticker"), "")).upper()

    # Datatillgänglighet
    eps_ttm    = _pos(_nz(y_snap.get("eps_ttm"), row.get("EPS TTM")))
    pe_ttm     = _pos(_nz(y_snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(y_snap.get("pe_fwd"), row.get("PE FWD")))
    rev_ttm    = _pos(_nz(y_snap.get("rev_ttm"), row.get("Rev TTM")))
    ebitda_ttm = _pos(_nz(y_snap.get("ebitda_ttm"), row.get("EBITDA TTM")))
    ev_rev     = _pos(_nz(y_snap.get("ev_rev"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(y_snap.get("ev_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(y_snap.get("p_b"), row.get("P/B")))
    bvps       = _pos(_nz(y_snap.get("bvps"), row.get("BVPS")))

    # Heuristik: klassificera
    is_financial  = any(k in sektor for k in ("finans", "financial", "bank", "insurance", "forsakring", "försäkring"))
    is_reit       = any(k in sektor for k in ("reit", "fastighet", "real estate"))
    is_utility    = any(k in sektor for k in ("utility", "verk", "kraft", "forsorjn", "försörjn"))
    is_energy     = any(k in sektor for k in ("energy", "olja", "oil", "gas"))
    is_industrial = any(k in sektor for k in ("industr", "capital goods", "machinery", "transport", "marine", "shipping"))
    is_tech       = any(k in sektor for k in ("tech", "software", "internet", "semiconductor", "it"))
    is_health     = any(k in sektor for k in ("health", "biotech", "pharma", "medtech"))
    is_materials  = any(k in sektor for k in ("material", "metals", "mining", "steel"))

    # Mer precisa etiketter
    is_shipping = any(k in sektor for k in ("shipping", "tanker", "bulk", "marine")) or \
                  ticker in {"MPCC.OL", "HAUTO.OL", "2020.OL", "HAFNI.OL", "BWO.OL", "FRO.OL"}
    is_crypto   = any(k in sektor for k in ("crypto", "bitcoin", "blockchain", "digital asset")) or \
                  ticker in {"MARA", "RIOT", "WULF", "BITF", "BTBT", "HUT", "IREN", "CIFR"}

    # Tickers som ofta är BDC/mREIT (proxy → P/B)
    bdc_mreit_tickers = {"AGNC","ARR","DX","EFC","NLY","ORC","RITM","CSWC","PFLT","HRZN","ARCC","MAIN"}

    # Grund-allow baserat på data
    allow = {
        "pe":   (eps_ttm is not None) and (eps_ttm > 0) and (pe_ttm is not None or pe_fwd is not None),
        "ev_s": (rev_ttm is not None) and (ev_rev is not None),
        "ev_e": (ebitda_ttm is not None) and (ebitda_ttm > 0) and (ev_ebitda is not None),
        "pb":   (p_b is not None) and (p_b > 0) and (bvps is not None) and (bvps > 0),
    }

    # Sektor-skift (bara PE/PB är relevanta när EV är avstängt)
    if is_financial or ticker in bdc_mreit_tickers:
        allow["ev_s"] = False
        allow["ev_e"] = False
        allow["pe"] = allow["pe"] and (eps_ttm and eps_ttm > 0)
    elif is_reit:
        allow["ev_s"] = False
        allow["ev_e"] = False
    elif is_tech or is_health:
        if not (eps_ttm and eps_ttm > 0):
            allow["pe"] = False

    # Fallback innan vi slår av EV helt: försök åtminstone PE
    if not any(allow.values()):
        if (eps_ttm is not None) and (eps_ttm > 0) and (pe_ttm is not None or pe_fwd is not None):
            allow["pe"] = True

    # EV-baserade familjer AVSTÄNGDA tills enheter är lösta
    allow["ev_s"] = False
    allow["ev_e"] = False

    # P/E-tak per profil (bara för tydligt cykliska/crypto)
    pe_cap: Optional[float] = None
    if is_crypto:
        pe_cap = PE_CAP_CRYPTO
    elif is_shipping or is_energy or is_materials:
        pe_cap = PE_CAP_CYCLICAL

    # Primär (för etikett/diagnostik)
    if is_financial or is_reit or (ticker in bdc_mreit_tickers):
        prefer_order = ["pb","pe","ev_e","ev_s"]
    else:
        prefer_order = ["pe","ev_s","ev_e","pb"]
    primary = next((fam for fam in prefer_order if allow.get(fam)), None)

    # Diagnostiksträng
    allow_bits = ", ".join([f"{k}:{'yes' if v else 'no'}" for k, v in allow.items()])
    sektor_label = (sektor or "-")
    primary_label = (primary or "-")
    cap_str = f", pe_cap={pe_cap:.1f}" if pe_cap is not None else ""
    why = (
        f"auto_profile: sektor='{sektor_label}', ticker='{ticker}', allow={{"
        + allow_bits + f"}}, primary='{primary_label}'{cap_str}"
    )

    return {"allow": allow, "primary": primary, "why": why, "pe_cap": pe_cap}


# -------------------------
# Fair Value via familjemedian (v3 med filtrering)
# -------------------------
def _compute_fair_value_row_v3(
    methods_df: pd.DataFrame,
    now_price: Optional[float],
    allow_fams: Dict[str, bool],
) -> Dict[str, Any]:
    fam_map = {
        "pe_hist_vs_eps": "pe",
        "ev_sales": "ev_s",
        "ev_ebitda": "ev_e",
        "ev_dacf": "ev_e",
        "p_b": "pb",
    }
    cols = ["Idag", "1 år", "2 år", "3 år"]
    out = {"Metod": "fair_value"}

    for c in cols:
        vals: List[float] = []
        used_fams: set[str] = set()
        for _, r in methods_df.iterrows():
            m = str(r.get("Metod") or "")
            if m == "fair_value":
                continue
            fam = fam_map.get(m, m)
            if fam in used_fams:
                continue
            if not allow_fams.get(fam, False):
                continue
            v = _f(r.get(c))
            if v is None:
                continue
            # Filtrera kurs-kopior i "Idag"
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:
                    continue
            used_fams.add(fam)
            vals.append(float(v))

        if not vals:
            try:
                if allow_fams.get("pe", False):
                    row_pe = methods_df[methods_df["Metod"] == "pe_hist_vs_eps"].iloc[0]
                    out[c] = _f(row_pe.get(c))
                else:
                    out[c] = np.nan
            except Exception:
                out[c] = np.nan
        else:
            out[c] = float(np.median(vals))
    return out


# -------------------------
# Bucket → Margin of Safety
# -------------------------
def _mos_for_bucket(bucket_label: Any) -> float:
    s = str(bucket_label or "").lower()
    if "bucket a" in s:
        return 0.05
    if "bucket b" in s:
        return 0.08
    if "bucket c" in s:
        return 0.12
    return 0.08


def _best_case_row(methods_df: pd.DataFrame, allow_fams: Dict[str,bool]) -> Dict[str, Any]:
    fam_ok = {
        "pe_hist_vs_eps":"pe",
        "ev_sales":"ev_s",
        "ev_ebitda":"ev_e",
        "ev_dacf":"ev_e",
        "p_b":"pb",
    }
    cols = ["Idag", "1 år", "2 år", "3 år"]
    base = {"Metod": "best_case"}
    if methods_df is None or (hasattr(methods_df, "empty") and methods_df.empty):
        return {**base, **{c: np.nan for c in cols}}
    sub = methods_df[
        methods_df["Metod"].map(lambda m: allow_fams.get(fam_ok.get(str(m), ""), False))
    ].copy()
    for c in cols:
        try:
            vals = [float(v) for v in sub[c].tolist() if _f(v) is not None]
            base[c] = (max(vals) if vals else np.nan)
        except Exception:
            base[c] = np.nan
    return base


# -------------------------
# Huvud: compute_methods_for_row → DICT (auto-profil)
# -------------------------
def compute_methods_for_row(
    row: pd.Series,
    settings: Dict[str, str] | None = None,
    fx_map: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    settings = settings or get_settings_map()

    ticker = str(row.get("Ticker", "")).strip()
    y = fetch_from_yahoo(ticker)
    est = _fetch_eps_estimates_yahoo(ticker)

    # --- Inputs (med fallback från Data-bladet) ---
    price    = _pos(_nz(y.get("price"), row.get("Aktuell kurs")))
    currency = str(_nz(y.get("currency"), row.get("Valuta") or "USD")).upper()
    shares   = _pos(_nz(y.get("shares_out"), row.get("Utestående aktier")))
    net_debt = _nz(y.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _nz(y.get("rev_ttm"), row.get("Rev TTM"))
    ebitda_ttm = _nz(y.get("ebitda_ttm"), row.get("EBITDA TTM"))
    eps_ttm    = _nz(y.get("eps_ttm"), row.get("EPS TTM"))

    pe_ttm     = _pos(_nz(y.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(y.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(y.get("ev_rev"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(y.get("ev_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(y.get("p_b"), row.get("P/B")))
    bvps       = _pos(_nz(y.get("bvps"), row.get("BVPS")))

    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), est.get("eps_1y")))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), est.get("eps_2y")))

    # Historisk CAGR (clamp)
    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), y.get("rev_cagr_hist")))
    rev_cagr_hist     = (
        max(REV_CAGR_MIN, min(REV_CAGR_MAX, rev_cagr_hist_raw))
        if rev_cagr_hist_raw is not None
        else None
    )

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), y.get("eps_cagr_hist")))
    eps_cagr_hist     = (
        max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_hist_raw))
        if eps_cagr_hist_raw is not None
        else None
    )

    eps_cagr_long = _f(est.get("eps_cagr_long"))
    if eps_cagr_long is not None:
        eps_cagr_long = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_long))

    # AUTO-PROFIL: vilka familjer + ev. P/E-tak
    profile = _auto_method_profile(row, y)
    allow_fams = profile["allow"]
    pe_cap = profile.get("pe_cap")

    # P/E-ankare + decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.08)) or 0.08  # default 8% kompression/år
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # P/E-tak för cykliska/crypto
    if pe_anchor is not None and pe_cap is not None:
        try:
            pe_anchor = min(float(pe_anchor), float(pe_cap))
        except Exception:
            pass

    # --- Fair Value (PE-growth-hybrid) kandidat för "Idag" ---
    fv_today_pe = None
    try:
        eps_base = _pos(eps_1y_est) or _pos(eps_ttm)
        if eps_base is not None and eps_base > 0:
            g = None
            for cand in (eps_cagr_long, eps_cagr_hist, rev_cagr_hist, 0.0):
                if cand is not None:
                    g = float(cand)
                    break
            if g is None:
                g = 0.0
            if g < 0.0:
                g = 0.0
            if g > 0.30:
                g = 0.30

            pe_growth = 15.0 + 50.0 * g

            pe_mkt = None
            for cand in (pe_fwd, pe_anchor, pe_ttm):
                c = _pos(cand)
                if c is not None:
                    pe_mkt = c
                    break
            if pe_mkt is not None:
                try:
                    pe_mkt = min(float(pe_mkt), 40.0)
                except Exception:
                    pass

            pe_fair_today = None
            if pe_growth is not None and pe_mkt is not None:
                pe_fair_today = 0.5 * float(pe_growth) + 0.5 * float(pe_mkt)
            elif pe_growth is not None:
                pe_fair_today = float(pe_growth)
            elif pe_mkt is not None:
                pe_fair_today = float(pe_mkt)

            if pe_fair_today is not None and pe_fair_today > 0:
                fv_today_pe = float(eps_base) * float(pe_fair_today)
    except Exception:
        fv_today_pe = None

    # Revenue-path
    r0 = _pos(rev_ttm)
    if r0 is None:
        r1 = r2 = r3 = None
    else:
        g = float(_nz(rev_cagr_hist, 0.0))
        r1 = r0 * (1.0 + g)
        r2 = r1 * (1.0 + g)
        r3 = r2 * (1.0 + g)

    # EPS-path
    e0, e1, e2, e3 = _eps_path_fill(
        _f(eps_ttm),
        eps_1y_est,
        eps_2y_est,
        eps_cagr_hist,
        eps_cagr_long,
        rev_cagr_hist,
    )

    # EBITDA-path
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = (
        ev_sales,
        _decay_multiple(ev_sales, 1, decay),
        _decay_multiple(ev_sales, 2, decay),
        _decay_multiple(ev_sales, 3, decay),
    )
    eve0, eve1, eve2, eve3 = (
        ev_ebitda,
        _decay_multiple(ev_ebitda, 1, decay),
        _decay_multiple(ev_ebitda, 2, decay),
        _decay_multiple(ev_ebitda, 3, decay),
    )
    pb0, pb1, pb2, pb3 = (
        p_b,
        _decay_multiple(p_b, 1, decay),
        _decay_multiple(p_b, 2, decay),
        _decay_multiple(p_b, 3, decay),
    )

    # --- Priser per metod (alla i aktiens valuta) ---
    methods: List[Dict[str, Any]] = []
    methods.append({
        "Metod": "pe_hist_vs_eps",
        "Idag": _price_from_pe(e0, pe0),
        "1 år": _price_from_pe(e1, pe1m),
        "2 år": _price_from_pe(e2, pe2m),
        "3 år": _price_from_pe(e3, pe3m),
    })
    methods.append({
        "Metod": "ev_sales",
        "Idag": _equity_price_from_ev(_ev_from_sales(r0, evs0), net_debt, shares),
        "1 år": _equity_price_from_ev(_ev_from_sales(r1, evs1), net_debt, shares),
        "2 år": _equity_price_from_ev(_ev_from_sales(r2, evs2), net_debt, shares),
        "3 år": _equity_price_from_ev(_ev_from_sales(r3, evs3), net_debt, shares),
    })
    methods.append({
        "Metod": "ev_ebitda",
        "Idag": _equity_price_from_ev(_ev_from_ebitda(b0, eve0), net_debt, shares),
        "1 år": _equity_price_from_ev(_ev_from_ebitda(b1, eve1), net_debt, shares),
        "2 år": _equity_price_from_ev(_ev_from_ebitda(b2, eve2), net_debt, shares),
        "3 år": _equity_price_from_ev(_ev_from_ebitda(b3, eve3), net_debt, shares),
    })
    methods.append({
        "Metod": "p_b",
        "Idag": _price_from_pb(pb0, bvps),
        "1 år": _price_from_pb(pb1, bvps),
        "2 år": _price_from_pb(pb2, bvps),
        "3 år": _price_from_pb(pb3, bvps),
    })
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # --- Fair Value (familjemedian, filtrerad av auto-profil) = bas ---
    fv_row = _compute_fair_value_row_v3(methods_df, price, allow_fams)

    if fv_today_pe is not None:
        fv_row["Idag"] = fv_today_pe

    # --- Bästa scenario ---
    best_row = _best_case_row(methods_df, allow_fams)

    # --- Margin of Safety per bucket ---
    bucket_label = str(_nz(row.get("Bucket"), "") or "")
    mos = _mos_for_bucket(bucket_label)
    best_mos_row = {
        "Metod": "best_case_MoS",
        "Idag": _f(fv_row.get("Idag")),
        "1 år": (_f(best_row.get("1 år")) * (1.0 - mos)) if _f(best_row.get("1 år")) is not None else np.nan,
        "2 år": (_f(best_row.get("2 år")) * (1.0 - mos)) if _f(best_row.get("2 år")) is not None else np.nan,
        "3 år": (_f(best_row.get("3 år")) * (1.0 - mos)) if _f(best_row.get("3 år")) is not None else np.nan,
    }

    methods_df = pd.concat(
        [pd.DataFrame([fv_row]), pd.DataFrame([best_row]), pd.DataFrame([best_mos_row]), methods_df],
        ignore_index=True,
    )

    sanity = (
        f"price={'ok' if price else '-'}, "
        f"eps_ttm={'ok' if (eps_ttm or eps_ttm==0) else '-'}, "
        f"eps_1y={'ok' if eps_1y_est else '-'}, "
        f"eps_2y={'ok' if eps_2y_est else '-'}, "
        f"rev_ttm={'ok' if rev_ttm else '-'}, "
        f"rev_cagr_hist={'ok' if _f(rev_cagr_hist) is not None else '-'}"
        f"(clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
        f"eps_cagr_hist={'ok' if _f(eps_cagr_hist) is not None else '-'}"
        f"(clamp={EPS_CAGR_MIN*100:.0f}%..{EPS_CAGR_MAX*100:.0f}%), "
        f"ebitda_ttm={'ok' if (ebitda_ttm or ebitda_ttm==0) else '-'}, "
        f"shares={'ok' if shares else '-'}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '-'}, decay={decay}, "
        f"bucket='{bucket_label or '-'}' -> MoS={int(mos*100)}%, "
        f"{profile['why']}"
    )

    target_today = _f(fv_row.get("Idag"))
    target_1y    = _f(best_mos_row.get("1 år")) if _f(best_mos_row.get("1 år")) is not None else _f(fv_row.get("1 år"))
    target_2y    = _f(best_mos_row.get("2 år")) if _f(best_mos_row.get("2 år")) is not None else _f(fv_row.get("2 år"))
    target_3y    = _f(best_mos_row.get("3 år")) if _f(best_mos_row.get("3 år")) is not None else _f(fv_row.get("3 år"))

    payload: Dict[str, Any] = {
        "Metod": "fair_value_v3_auto",
        "method": "fair_value_v3_auto",
        "target_today": target_today,
        "target_1y":    target_1y,
        "target_2y":    target_2y,
        "target_3y":    target_3y,
        "bull_1y": None,
        "bear_1y": None,
        "Input-sammanfattning": sanity,
        "note": profile.get("primary") or "",
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "methods_df": methods_df,
    }
    return payload


# -------------------------
# Kompakt extraktor (FV) för UI
# -------------------------
def compute_fair_values_for_row(
    row: pd.Series,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
) -> Dict[str, Any]:
    payload = compute_methods_for_row(row, settings, fx_map)

    fv_today = _f(payload.get("target_today"))
    fv_1y    = _f(payload.get("target_1y"))
    fv_2y    = _f(payload.get("target_2y"))
    fv_3y    = _f(payload.get("target_3y"))

    # Nya nivåer runt FV idag
    bra_kop_niva  = fv_today * 0.80 if fv_today is not None else None
    fyndlage_niva = fv_today * 0.65 if fv_today is not None else None

    return {
        "ticker":   str(row.get("Ticker") or "").upper(),
        "price":    _f(payload.get("price")),
        "currency": (payload.get("currency") or "USD"),
        "fv_today": fv_today,
        "fv_1y":    fv_1y,
        "fv_2y":    fv_2y,
        "fv_3y":    fv_3y,
        "bra_kop_niva":  bra_kop_niva,
        "fyndlage_niva": fyndlage_niva,
        "sanity":   payload.get("Input-sammanfattning", ""),
        "methods_df": payload.get("methods_df"),
    }
