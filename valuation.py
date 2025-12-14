# valuation.py — DCF-baserad FV idag + P/E-baserade framtida riktkurser

from __future__ import annotations

import math
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import yfinance as yf

from core_utils import _f, _nz


# ============================
# Yahoo-hämtning (robust)
# ============================

def _safe_float(x: Any) -> Optional[float]:
    v = _f(x)
    if v is None:
        return None
    try:
        fv = float(v)
        if not math.isfinite(fv):
            return None
        return fv
    except Exception:
        return None


def _safe_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    try:
        return dict(obj)
    except Exception:
        return {}


def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Hämtar nyckeldata för ett bolag från Yahoo Finance.
    Returnerar en dict med fält som används av appen.

    OBS: Körs bara vid explicit uppdatering / massuppdatering,
    inte inne i själva fair value-beräkningen (compute_methods_for_row).
    """
    tkr = (ticker or "").strip()
    if not tkr:
        return {}

    try:
        t = yf.Ticker(tkr)
    except Exception:
        return {}

    info: Dict[str, Any] = {}
    try:
        info = _safe_dict(t.get_info())
    except Exception:
        # vissa versioner: .info istället
        try:
            info = _safe_dict(getattr(t, "info", {}))
        except Exception:
            info = {}

    # Pris & valuta
    price = None
    currency = None
    try:
        fi = getattr(t, "fast_info", None)
        if fi is not None:
            # fast_info kan vara ett objekt eller dict
            if isinstance(fi, dict):
                currency = fi.get("currency") or currency
                price = (
                    fi.get("last_price")
                    or fi.get("lastPrice")
                    or fi.get("last")
                    or price
                )
            else:
                currency = getattr(fi, "currency", None) or currency
                price = (
                    getattr(fi, "last_price", None)
                    or getattr(fi, "lastPrice", None)
                    or getattr(fi, "last", None)
                    or price
                )
    except Exception:
        pass

    if price is None:
        try:
            hist = t.history(period="5d")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].iloc[-1])
        except Exception:
            price = None

    if currency is None:
        currency = info.get("currency")

    # Utestående aktier & nettoskuld
    shares_out = info.get("sharesOutstanding")
    total_debt = info.get("totalDebt")
    cash = info.get("totalCash")
    net_debt = None
    try:
        if total_debt is not None and cash is not None:
            net_debt = float(total_debt) - float(cash)
    except Exception:
        net_debt = None

    rev_ttm = info.get("totalRevenue")
    ebitda_ttm = info.get("ebitda")
    eps_ttm = info.get("trailingEps")
    pe_ttm = info.get("trailingPE")
    pe_fwd = info.get("forwardPE")

    ev_rev = info.get("enterpriseToRevenue")
    ev_ebitda = info.get("enterpriseToEbitda")
    p_b = info.get("priceToBook")
    bvps = info.get("bookValue")

    # Historisk CAGR (om det finns)
    rev_cagr_hist = info.get("revenueGrowth")  # ofta QoQ eller YoY, men bättre än inget
    eps_cagr_hist = info.get("earningsQuarterlyGrowth")

    # ============================
    # Utdelning (annualiserad)
    # ============================
    dps_annual: Optional[float] = None

    # 1) Försök först med "vanliga" Yahoo-fält
    for key in ("trailingAnnualDividendRate", "dividendRate"):
        v = info.get(key)
        v_f = _safe_float(v)
        if v_f is not None and v_f > 0:
            dps_annual = v_f
            break

    # 2) Om fortfarande None/0 → räkna fram från utdelningshistorik (sista 12 mån)
    if not dps_annual:
        try:
            divs = t.dividends
            if divs is not None and not divs.empty:
                now = pd.Timestamp.utcnow()
                last_12m = divs[divs.index >= (now - pd.Timedelta(days=365))]
                if last_12m.empty:
                    last_12m = divs
                total = float(last_12m.sum())
                if total > 0:
                    dps_annual = total
        except Exception:
            dps_annual = None

    out = {
        "name": info.get("longName") or info.get("shortName") or info.get("name"),
        "longName": info.get("longName"),
        "shortName": info.get("shortName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),

        "price": _safe_float(price),
        "currency": currency,
        "shares_out": _safe_float(shares_out),
        "net_debt": _safe_float(net_debt),

        "rev_ttm": _safe_float(rev_ttm),
        "ebitda_ttm": _safe_float(ebitda_ttm),
        "eps_ttm": _safe_float(eps_ttm),
        "pe_ttm": _safe_float(pe_ttm),
        "pe_fwd": _safe_float(pe_fwd),

        "ev_rev": _safe_float(ev_rev),
        "ev_ebitda": _safe_float(ev_ebitda),
        "p_b": _safe_float(p_b),
        "bvps": _safe_float(bvps),

        "rev_cagr_hist": _safe_float(rev_cagr_hist),
        "eps_cagr_hist": _safe_float(eps_cagr_hist),
        "dps_annual": _safe_float(dps_annual),
    }
    return out


# ============================
# ✅ CHANGED: Normalisera growth-rate (procent vs decimal)
# ============================
def _normalize_growth_rate(g: Optional[float]) -> Optional[float]:
    """
    Tar en growth-rate som kan vara i decimal (0.12) eller procent (12.0)
    och normaliserar till decimalform.
    """
    if g is None:
        return None
    try:
        gv = float(g)
        if not math.isfinite(gv):
            return None
        if abs(gv) > 1.0:
            gv = gv / 100.0
        return gv
    except Exception:
        return None


# ============================
# EPS-estimat från Yahoo
# ============================

def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker plocka EPS-estimat för 1Y och 2Y framåt.
    Returnerar { 'eps_1y': float | None, 'eps_2y': float | None }.
    """
    tkr = (ticker or "").strip()
    if not tkr:
        return {"eps_1y": None, "eps_2y": None}

    try:
        t = yf.Ticker(tkr)
    except Exception:
        return {"eps_1y": None, "eps_2y": None}

    eps_1y = None
    eps_2y = None

    try:
        trend = t.get_earnings_trend()
        if isinstance(trend, pd.DataFrame) and not trend.empty:
            for _, row in trend.iterrows():
                per = str(row.get("period") or "").lower()
                eps = _safe_float(row.get("epsForward") or row.get("epsTrend"))
                if eps is None:
                    continue
                if "0y" in per or "current" in per:
                    eps_1y = eps_1y or eps
                elif "1y" in per:
                    eps_1y = eps
                elif "2y" in per:
                    eps_2y = eps
    except Exception:
        pass

    return {"eps_1y": eps_1y, "eps_2y": eps_2y}


# ============================
# DCF-metod för FV idag
# ============================

def _compute_dcf_fv_today(row: pd.Series, settings: Dict[str, Any]) -> Optional[float]:
    """
    Strikt DCF per aktie för FV idag.

    ✅ CHANGED:
    - FV idag ska i första hand baseras på EPS TTM (inte EPS 1Y/2Y),
      så att manuella estimat inte "fuckar" FV idag när du fyller i dem.
    """
    try:
        disc = float(settings.get("dcf_discount_rate", 0.10))
    except Exception:
        disc = 0.10

    try:
        years = int(float(settings.get("dcf_high_growth_years", 5)))
    except Exception:
        years = 5

    try:
        term_g = float(settings.get("dcf_terminal_growth", 0.02))
    except Exception:
        term_g = 0.02

    if years <= 0:
        return None
    if disc <= term_g:
        return None

    eps_ttm = _safe_float(row.get("EPS TTM"))
    eps1 = _safe_float(row.get("EPS 1Y"))
    eps2 = _safe_float(row.get("EPS 2Y"))

    # ✅ CHANGED: TTM först. (fallback bara om TTM saknas)
    base_eps = eps_ttm
    if base_eps is None:
        base_eps = eps1
    if base_eps is None:
        base_eps = eps2

    if base_eps is None or base_eps <= 0:
        return None

    # Tillväxt för DCF:
    # primärt historisk EPS CAGR (om finns), annars default från settings.
    g = _normalize_growth_rate(_safe_float(row.get("EPS CAGR")))
    if g is None:
        try:
            g = float(settings.get("dcf_default_growth", 0.08))
        except Exception:
            g = 0.08

    # rimliga clamps
    g = max(-0.40, min(g, 0.60))

    fv = 0.0
    for t in range(1, years + 1):
        cf_t = base_eps * ((1.0 + g) ** (t - 1))
        fv += cf_t / ((1.0 + disc) ** t)

    eps_last = base_eps * ((1.0 + g) ** years)
    terminal = eps_last * (1.0 + term_g) / (disc - term_g)
    fv += terminal / ((1.0 + disc) ** years)

    if not math.isfinite(fv):
        return None
    return float(fv)


# ============================
# P/E-baserade riktkurser 1–3 år
# ============================

def _pe_anchor_for_row(row: pd.Series, settings: Dict[str, Any]) -> Optional[float]:
    pe_ttm = _safe_float(row.get("PE TTM"))
    pe_fwd = _safe_float(row.get("PE FWD"))

    pe_list = [x for x in (pe_ttm, pe_fwd) if x is not None and x > 0]
    if pe_list:
        market_pe = sum(pe_list) / len(pe_list)
    else:
        market_pe = None

    base_pe = 20.0

    try:
        w_ttm = float(settings.get("pe_anchor_weight_ttm", 0.5))
    except Exception:
        w_ttm = 0.5
    w_ttm = max(0.0, min(1.0, w_ttm))

    if market_pe is None:
        anchor = base_pe
    else:
        anchor = w_ttm * market_pe + (1.0 - w_ttm) * base_pe

    anchor = max(5.0, min(anchor, 45.0))
    return anchor


# ============================
# ✅ CHANGED: Historisk growth för år 3 (inte eps2/eps1)
# ============================
def _hist_growth_for_year3(row: pd.Series, settings: Dict[str, Any]) -> float:
    """
    År 3 ska baseras på historisk EPS CAGR (om finns),
    annars en default. Inga "g" från EPS2/EPS1 här.
    """
    g = _normalize_growth_rate(_safe_float(row.get("EPS CAGR")))
    if g is None:
        try:
            g = float(settings.get("eps_cagr_default", 0.10))
        except Exception:
            g = 0.10
    g = max(-0.40, min(g, 0.60))
    return g


def _compute_forward_targets_pe(row: pd.Series, settings: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    ✅ CHANGED (Alternativ B):
    - target_1y använder EPS 1Y rakt av (innevarande FY)
    - target_2y använder EPS 2Y rakt av (nästa FY)
    - target_3y projiceras från EPS 2Y med HISTORISK EPS CAGR (eller default)
      (dvs inte från eps2/eps1 som kan ge konstig effekt när EPS faller ett år)
    """
    anchor_pe = _pe_anchor_for_row(row, settings)
    if anchor_pe is None or anchor_pe <= 0:
        return {"target_1y": None, "target_2y": None, "target_3y": None}

    try:
        decay = float(settings.get("multiple_decay", 0.08))
    except Exception:
        decay = 0.08
    decay = max(0.0, min(decay, 0.30))

    eps1 = _safe_float(row.get("EPS 1Y"))
    eps2 = _safe_float(row.get("EPS 2Y"))
    eps_ttm = _safe_float(row.get("EPS TTM"))

    # År 1: EPS 1Y (fallback TTM)
    eps_y1 = eps1 if eps1 is not None and eps1 > 0 else eps_ttm
    if eps_y1 is None or eps_y1 <= 0:
        return {"target_1y": None, "target_2y": None, "target_3y": None}

    # År 2: EPS 2Y (fallback: väx år1 med hist CAGR)
    g3 = _hist_growth_for_year3(row, settings)
    if eps2 is not None and eps2 > 0:
        eps_y2 = eps2
    else:
        eps_y2 = eps_y1 * (1.0 + g3)

    # År 3: basera på historisk CAGR från år2
    eps_y3 = eps_y2 * (1.0 + g3)

    mult_1 = anchor_pe * (1.0 - decay * 1)
    mult_2 = anchor_pe * (1.0 - decay * 2)
    mult_3 = anchor_pe * (1.0 - decay * 3)

    mult_1 = max(3.0, mult_1)
    mult_2 = max(3.0, mult_2)
    mult_3 = max(3.0, mult_3)

    t1 = eps_y1 * mult_1
    t2 = eps_y2 * mult_2
    t3 = eps_y3 * mult_3

    # sanity
    for name, val in (("t1", t1), ("t2", t2), ("t3", t3)):
        if val is not None and not math.isfinite(val):
            if name == "t1":
                t1 = None
            elif name == "t2":
                t2 = None
            else:
                t3 = None

    return {"target_1y": t1, "target_2y": t2, "target_3y": t3}


# ============================
# Huvudfunktion: compute_methods_for_row
# ============================

def compute_methods_for_row(
    row: pd.Series,
    settings: Dict[str, Any],
    fx_map: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    if row is None:
        return {
            "price": None,
            "currency": None,
            "target_today": None,
            "target_1y": None,
            "target_2y": None,
            "target_3y": None,
        }

    price = _safe_float(row.get("Aktuell kurs"))
    currency = str(_nz(row.get("Valuta"), "SEK")).upper()

    # FV idag ska inte påverkas av EPS 1Y / EPS 2Y (se CHANGED i DCF)
    fv_today = _compute_dcf_fv_today(row, settings)

    forwards = _compute_forward_targets_pe(row, settings)
    t1 = forwards.get("target_1y")
    t2 = forwards.get("target_2y")
    t3 = forwards.get("target_3y")

    payload: Dict[str, Any] = {
        "price": price,
        "currency": currency,
        "target_today": fv_today,
        "target_1y": t1,
        "target_2y": t2,
        "target_3y": t3,
    }

    return payload
