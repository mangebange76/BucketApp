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

    rev_cagr_hist = info.get("revenueGrowth")
    eps_cagr_hist = info.get("earningsQuarterlyGrowth")

    # ============================
    # Utdelning (annualiserad)
    # ============================
    dps_annual: Optional[float] = None
    for key in ("trailingAnnualDividendRate", "dividendRate"):
        v = info.get(key)
        v_f = _safe_float(v)
        if v_f is not None and v_f > 0:
            dps_annual = v_f
            break

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
# Normalisera growth-rate (procent vs decimal)
# ============================
def _normalize_growth_rate(g: Optional[float]) -> Optional[float]:
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
                if "1y" in per:
                    eps_1y = eps
                elif "2y" in per:
                    eps_2y = eps
                elif ("0y" in per or "current" in per) and eps_1y is None:
                    eps_1y = eps
    except Exception:
        pass

    return {"eps_1y": eps_1y, "eps_2y": eps_2y}


# ============================
# DCF-metod för FV idag
# ============================
def _compute_dcf_fv_today(row: pd.Series, settings: Dict[str, Any]) -> Optional[float]:
    """
    Strikt DCF per aktie för FV idag.

    ✅ CHANGED: FV idag baseras på "idag-bas" (EPS TTM) och påverkas INTE av EPS 1Y/2Y-estimat.
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

    # ✅ CHANGED: använd EPS TTM som bas (inte EPS 1Y/2Y)
    eps_ttm = _safe_float(row.get("EPS TTM"))
    base_eps = eps_ttm

    if base_eps is None or base_eps <= 0:
        return None

    # Tillväxt g: använd EPS CAGR om finns, annars default
    g = None
    eps_cagr_hist = _normalize_growth_rate(_safe_float(row.get("EPS CAGR")))
    if eps_cagr_hist is not None:
        g = eps_cagr_hist

    if g is None:
        try:
            g = float(settings.get("dcf_default_growth", 0.08))
        except Exception:
            g = 0.08

    # rimliga guardrails
    g = max(-0.20, min(g, 0.25))

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
    market_pe = (sum(pe_list) / len(pe_list)) if pe_list else None

    base_pe = 20.0
    try:
        base_pe = float(settings.get("pe_anchor_base", base_pe))
    except Exception:
        pass

    try:
        w_ttm = float(settings.get("pe_anchor_weight_ttm", 0.5))
    except Exception:
        w_ttm = 0.5
    w_ttm = max(0.0, min(1.0, w_ttm))

    if market_pe is None:
        anchor = base_pe
    else:
        anchor = w_ttm * market_pe + (1.0 - w_ttm) * base_pe

    anchor = max(5.0, min(anchor, 60.0))
    return anchor


def _growth_next_from_estimates_or_hist(row: pd.Series) -> float:
    """
    Tillväxt som används för att extrapolera från EPS 2Y till ett år till (år 3),
    om EPS 3Y saknas.

    ✅ CHANGED: här är det OK att använda EPS 2Y/EPS 1Y, men vi använder INTE g för att räkna om EPS 1Y igen.
    """
    eps1 = _safe_float(row.get("EPS 1Y"))
    eps2 = _safe_float(row.get("EPS 2Y"))

    g = None
    if eps1 is not None and eps2 is not None and eps1 > 0:
        try:
            g = (eps2 / eps1) - 1.0
        except Exception:
            g = None

    if g is None:
        cagr_hist = _normalize_growth_rate(_safe_float(row.get("EPS CAGR")))
        if cagr_hist is not None:
            g = cagr_hist

    if g is None:
        g = 0.10

    g = max(-0.40, min(g, 0.60))
    return float(g)


def _compute_forward_targets_pe(row: pd.Series, settings: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    ✅ CHANGED:
    - target_1y använder EPS 1Y direkt (om finns)
    - target_2y använder EPS 2Y direkt (om finns)
    - target_3y extrapolerar från EPS 2Y med g_next om EPS 3Y saknas (vi har inget EPS 3Y-fält)
    """
    anchor_pe = _pe_anchor_for_row(row, settings)
    if anchor_pe is None or anchor_pe <= 0:
        return {"target_1y": None, "target_2y": None, "target_3y": None}

    try:
        decay = float(settings.get("multiple_decay", 0.08))
    except Exception:
        decay = 0.08
    decay = max(0.0, min(decay, 0.30))

    mult_1 = max(3.0, anchor_pe * (1.0 - decay * 1))
    mult_2 = max(3.0, anchor_pe * (1.0 - decay * 2))
    mult_3 = max(3.0, anchor_pe * (1.0 - decay * 3))

    eps1 = _safe_float(row.get("EPS 1Y"))
    eps2 = _safe_float(row.get("EPS 2Y"))
    eps_ttm = _safe_float(row.get("EPS TTM"))

    # använd estimat om de finns, annars fallback
    eps_y1 = eps1 if eps1 is not None and eps1 > 0 else eps_ttm
    eps_y2 = eps2 if eps2 is not None and eps2 > 0 else (eps_y1 if eps_y1 is not None else None)

    if eps_y1 is None or eps_y1 <= 0:
        return {"target_1y": None, "target_2y": None, "target_3y": None}
    if eps_y2 is None or eps_y2 <= 0:
        eps_y2 = eps_y1

    g_next = _growth_next_from_estimates_or_hist(row)
    eps_y3 = eps_y2 * (1.0 + g_next) if eps_y2 is not None else None

    t1 = eps_y1 * mult_1 if eps_y1 is not None else None
    t2 = eps_y2 * mult_2 if eps_y2 is not None else None
    t3 = eps_y3 * mult_3 if eps_y3 is not None else None

    for val in (t1, t2, t3):
        if val is not None and not math.isfinite(val):
            return {"target_1y": None, "target_2y": None, "target_3y": None}

    return {"target_1y": float(t1) if t1 is not None else None,
            "target_2y": float(t2) if t2 is not None else None,
            "target_3y": float(t3) if t3 is not None else None}


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

    fv_today = _compute_dcf_fv_today(row, settings)

    forwards = _compute_forward_targets_pe(row, settings)
    t1 = forwards.get("target_1y")
    t2 = forwards.get("target_2y")
    t3 = forwards.get("target_3y")

    return {
        "price": price,
        "currency": currency,
        "target_today": fv_today,
        "target_1y": t1,
        "target_2y": t2,
        "target_3y": t3,
    }
