from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from core_utils import _f, _pos, _nz


# ============================================================
# Yahoo-hämtning (oförändrat interface)
# ============================================================

def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Hämtar grunddata för ett bolag från Yahoo Finance.

    Returnerar en dict med nycklar som används i appen, t.ex.:
      name, sector, industry, price, currency, shares_out, net_debt,
      rev_ttm, ebitda_ttm, eps_ttm, pe_ttm, pe_fwd, ev_rev, ev_ebitda,
      p_b, bvps, rev_cagr_hist, eps_cagr_hist, dps_annual
    """
    t = yf.Ticker(str(ticker).strip())

    out: Dict[str, Any] = {}

    # -----------------------
    # Meta / info
    # -----------------------
    try:
        info = t.fast_info or {}
    except Exception:
        info = {}

    try:
        # Nyare Yahoo-versioner har "get_info"
        long_info = t.get_info()
    except Exception:
        long_info = {}

    def _g(*keys, src=None):
        src_dict = info if src == "fast" else long_info
        for k in keys:
            if src_dict and k in src_dict and src_dict[k] not in (None, "", "None"):
                return src_dict[k]
        return None

    out["name"] = _g("longName", "shortName", "symbol")
    out["longName"] = _g("longName")
    out["shortName"] = _g("shortName")
    out["sector"] = _g("sector")
    out["industry"] = _g("industry")

    # -----------------------
    # Pris & valuta
    # -----------------------
    price = None
    try:
        if isinstance(info, dict):
            price = info.get("last_price") or info.get("lastPrice") or info.get("last")
    except Exception:
        price = None

    if price is None:
        try:
            hist = t.history(period="5d")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].iloc[-1])
        except Exception:
            price = None

    out["price"] = _f(price)

    cur = _g("currency", src="fast") or _g("currency")
    out["currency"] = cur

    # -----------------------
    # Aktier & skulder
    # -----------------------
    shares_out = _g("sharesOutstanding", "impliedSharesOutstanding")
    out["shares_out"] = _f(shares_out)

    net_debt = None
    try:
        bs = t.balance_sheet
        if bs is not None and not bs.empty:
            # Yahoo-balansräkning är kolumn per period, ta senaste
            col = bs.columns[0]
            tot_debt = _f(bs.loc.get("TotalDebt", {}).get(col)) or 0.0
            cash = _f(bs.loc.get("CashAndCashEquivalents", {}).get(col)) or 0.0
            net_debt = tot_debt - cash
    except Exception:
        net_debt = None
    out["net_debt"] = _f(net_debt)

    # -----------------------
    # Resultaträkning / multiplar
    # -----------------------
    try:
        fin = t.get_financials()
    except Exception:
        fin = None

    rev_ttm = None
    ebitda_ttm = None
    eps_ttm = None

    try:
        if fin is not None and not fin.empty:
            col = fin.columns[0]
            rev_ttm = _f(fin.loc.get("TotalRevenue", {}).get(col))
            ebitda_ttm = _f(fin.loc.get("Ebitda", {}).get(col))
    except Exception:
        pass

    try:
        eps_hist = t.get_earnings_history()
        if eps_hist is not None and len(eps_hist) > 0:
            eps_vals = [x.get("epsactual") for x in eps_hist if x.get("epsactual") is not None]
            if eps_vals:
                eps_ttm = float(np.mean(eps_vals[-4:]))
    except Exception:
        eps_ttm = None

    out["rev_ttm"] = rev_ttm
    out["ebitda_ttm"] = ebitda_ttm
    out["eps_ttm"] = eps_ttm

    if _pos(price) and _pos(eps_ttm):
        out["pe_ttm"] = float(price) / float(eps_ttm)
    else:
        out["pe_ttm"] = None

    try:
        pe_fwd = _g("forwardPE")
    except Exception:
        pe_fwd = None
    out["pe_fwd"] = _f(pe_fwd)

    # EV / Sales & EV / EBITDA från info om de finns
    out["ev_rev"] = _f(_g("enterpriseToRevenue", src="fast") or _g("enterpriseToRevenue"))
    out["ev_ebitda"] = _f(_g("enterpriseToEbitda", src="fast") or _g("enterpriseToEbitda"))

    # P/B & BVPS
    out["p_b"] = _f(_g("priceToBook", src="fast") or _g("priceToBook"))
    out["bvps"] = _f(_g("bookValue", src="fast") or _g("bookValue"))

    # Utdelning
    out["dps_annual"] = _f(
        _g("dividendsPerShare", src="fast")
        or _g("dividendsPerShare", "trailingAnnualDividendRate")
    )

    # Historiska CAGR (lämnas None så länge)
    out["rev_cagr_hist"] = None
    out["eps_cagr_hist"] = None

    return out


def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker hämta EPS-estimat 1Y och 2Y från Yahoo.
    Returnerar {"eps_1y": float|None, "eps_2y": float|None}
    """
    t = yf.Ticker(str(ticker).strip())
    eps_1y = None
    eps_2y = None
    try:
        est = t.get_earnings_trend()
    except Exception:
        est = None

    if est is not None and not est.empty:
        try:
            # Yahoo brukar ha rader "0y", "1y", "2y" etc
            for _, r in est.iterrows():
                p = str(r.get("period") or "").lower()
                eps_val = _f(
                    r.get("epsTrend")
                    or r.get("epsHigh")
                    or r.get("epsLow")
                    or r.get("epsMean")
                )
                if eps_val is None:
                    continue
                if "1y" in p and eps_1y is None:
                    eps_1y = eps_val
                elif "2y" in p and eps_2y is None:
                    eps_2y = eps_val
        except Exception:
            pass

    return {"eps_1y": eps_1y, "eps_2y": eps_2y}


# ============================================================
# Fair value-beräkning – fokus FV IDAG
# ============================================================

def _pick_price_and_currency(row: pd.Series) -> Tuple[Optional[float], str]:
    """Plocka aktuell kurs + valuta från raden."""
    price = None
    for c in ("Aktuell kurs", "Price", "Senaste kurs", "Kurs"):
        if c in row and _f(row.get(c)) is not None:
            price = _f(row.get(c))
            break

    cur = None
    for c in ("Valuta", "Currency", "CUR"):
        if c in row and row.get(c) not in (None, "", "nan"):
            cur = str(row.get(c)).upper().strip()
            break
    if not cur:
        cur = "SEK"
    return price, cur


def _eps_inputs_from_row(row: pd.Series) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returnerar (eps_ttm, eps_1y, eps_2y) från raden med rimliga fallbacks.
    """
    eps_ttm = _f(row.get("EPS TTM"))
    eps_1y = _f(row.get("EPS 1Y"))
    eps_2y = _f(row.get("EPS 2Y"))

    # Fallbacks
    if eps_1y is None and eps_ttm is not None:
        eps_1y = eps_ttm
    if eps_2y is None and eps_1y is not None:
        # default ~10% tillväxt om inget bättre
        eps_2y = eps_1y * 1.10

    return eps_ttm, eps_1y, eps_2y


def _derive_growth(eps_1y: Optional[float], eps_2y: Optional[float]) -> float:
    """
    Grovt EPS-tillväxtantagande från 1Y → 2Y.
    Clampas till [-40 %, +60 %] för att undvika extremfall.
    """
    if not _pos(eps_1y) or not _pos(eps_2y):
        return 0.0
    try:
        g = eps_2y / eps_1y - 1.0
    except Exception:
        g = 0.0
    if not math.isfinite(g):
        return 0.0
    g = max(-0.40, min(0.60, g))
    return float(g)


def _pe_anchor_from_row(row: pd.Series, settings: Dict[str, str]) -> float:
    """
    Bygger ett P/E-ankare för FV **idag**.
    Blandar observerat P/E (ttm/fwd) med en "normal" multipel ~20x.

    Nycklar i Settings:
      - pe_anchor_weight_ttm (0–1, default 0.50)
    """
    pe_ttm = _f(row.get("PE TTM"))
    pe_fwd = _f(row.get("PE FWD"))

    base_normal = 20.0
    obs = None
    if _pos(pe_fwd):
        obs = pe_fwd
    elif _pos(pe_ttm):
        obs = pe_ttm

    if obs is None:
        return base_normal

    try:
        w = float(settings.get("pe_anchor_weight_ttm", "0.50"))
    except Exception:
        w = 0.5
    w = max(0.0, min(1.0, w))

    pe_anchor = w * obs + (1.0 - w) * base_normal
    # Rimliga gränser
    pe_anchor = max(8.0, min(40.0, pe_anchor))
    return float(pe_anchor)


def _multiple_for_year(pe_anchor: float, year: int, settings: Dict[str, str]) -> float:
    """
    Justerar multipeln längre ut i tiden via 'multiple_decay'.

    multiple_decay i Settings är per år, t.ex. 0.08 → -8 %-enheter per år.
    """
    try:
        decay = float(settings.get("multiple_decay", "0.08"))
    except Exception:
        decay = 0.08
    decay = max(0.0, min(0.25, decay))

    m = pe_anchor * (1.0 - decay * year)
    m = max(5.0, min(35.0, m))
    return float(m)


def compute_methods_for_row(
    row: pd.Series,
    settings: Dict[str, str],
    fx_map: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    """
    Huvudfunktion som appen anropar.

    Returnerar en dict med minst:
      - price
      - currency
      - method_name
      - target_today
      - target_1y
      - target_2y
      - target_3y

    Fokus är en robust, konservativ FV **idag**, där framåtblickande
    riktkurser bygger vidare på EPS-tillväxt + multipel-decay.
    """
    if row is None or not isinstance(row, (pd.Series, dict)):
        return {
            "price": None,
            "currency": "SEK",
            "method_name": "n/a",
            "target_today": None,
            "target_1y": None,
            "target_2y": None,
            "target_3y": None,
        }
    if isinstance(row, dict):
        row = pd.Series(row)

    price, currency = _pick_price_and_currency(row)
    if not _pos(price):
        # Utan aktuell kurs kan vi inte säga så mycket
        return {
            "price": None,
            "currency": currency,
            "method_name": "no_price",
            "target_today": None,
            "target_1y": None,
            "target_2y": None,
            "target_3y": None,
        }

    eps_ttm, eps_1y, eps_2y = _eps_inputs_from_row(row)
    if not _pos(eps_1y):
        # Vi saknar någon form av EPS-bas → returnera bara pris
        return {
            "price": price,
            "currency": currency,
            "method_name": "price_only",
            "target_today": None,
            "target_1y": None,
            "target_2y": None,
            "target_3y": None,
        }

    pe_anchor = _pe_anchor_from_row(row, settings)
    g = _derive_growth(eps_1y, eps_2y)

    # EPS-banor
    eps_y0 = eps_1y  # "nära kommande år" som används för FV idag
    eps_y1 = eps_y0 * (1.0 + g)
    eps_y2 = eps_y1 * (1.0 + g)
    eps_y3 = eps_y2 * (1.0 + g)

    m0 = _multiple_for_year(pe_anchor, 0, settings)
    m1 = _multiple_for_year(pe_anchor, 1, settings)
    m2 = _multiple_for_year(pe_anchor, 2, settings)
    m3 = _multiple_for_year(pe_anchor, 3, settings)

    target_today = eps_y0 * m0
    target_1y = eps_y1 * m1
    target_2y = eps_y2 * m2
    target_3y = eps_y3 * m3

    # Säkerhetsbälte: om nåt blivit orimligt (t.ex. <0) → None
    def _safe(v: float | None) -> Optional[float]:
        if v is None:
            return None
        try:
            v = float(v)
        except Exception:
            return None
        if not math.isfinite(v) or v <= 0:
            return None
        return v

    out = {
        "price": _safe(price),
        "currency": currency,
        "method_name": "pe_anchor_eps",
        "eps_ttm": eps_ttm,
        "eps_1y": eps_1y,
        "eps_2y": eps_2y,
        "growth_eps_1y_2y": g,
        "pe_anchor": pe_anchor,
        "target_today": _safe(target_today),
        "target_1y": _safe(target_1y),
        "target_2y": _safe(target_2y),
        "target_3y": _safe(target_3y),
    }
    return out
