# yahoo_fetch.py — Datainhämtning från Yahoo & uppdateringshjälpare

from __future__ import annotations

import math
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

from core_utils import _f, now_stamp


# ------------------------------
# yfinance-hjälpare (robusta)
# ------------------------------
def _yf_ticker(sym: str):
    try:
        return yf.Ticker(sym)
    except Exception:
        return None


def _yf_last_price(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # 1) fast_info
    try:
        fi = tkr.fast_info
        px = getattr(fi, "last_price", None)
        if px:
            return float(px)
    except Exception:
        pass
    # 2) info
    try:
        info = tkr.info
        px = info.get("currentPrice") or info.get("regularMarketPrice")
        if px:
            return float(px)
    except Exception:
        pass
    # 3) history fallback
    try:
        h = tkr.history(period="5d")
        if not h.empty:
            return float(h["Close"].dropna().iloc[-1])
    except Exception:
        pass
    return None


def _yf_currency(tkr) -> Optional[str]:
    if tkr is None:
        return None
    # fast_info
    try:
        fi = tkr.fast_info
        c = getattr(fi, "currency", None)
        if c:
            return str(c).upper()
    except Exception:
        pass
    # info
    try:
        info = tkr.info
        c = info.get("currency")
        if c:
            return str(c).upper()
    except Exception:
        pass
    return None


def _yf_shares_out(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # fast_info
    try:
        fi = tkr.fast_info
        s = getattr(fi, "shares", None)
        if s and s > 0:
            return float(s)
    except Exception:
        pass
    # info
    try:
        info = tkr.info
        s = info.get("sharesOutstanding")
        if s and s > 0:
            return float(s)
    except Exception:
        pass
    # get_shares_full (senaste kända)
    try:
        df = tkr.get_shares_full()
        if df is not None and not df.empty:
            val = float(df["SharesOutstanding"].dropna().iloc[-1])
            if val > 0:
                return val
    except Exception:
        pass
    return None


def _yf_eps_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        fi = tkr.fast_info
        v = getattr(fi, "trailingEps", None)
        if v and v == v:
            return float(v)
    except Exception:
        pass
    try:
        info = tkr.info
        v = info.get("trailingEps")
        if v and v == v:
            return float(v)
    except Exception:
        pass
    return None


def _yf_pe_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        fi = tkr.fast_info
        v = getattr(fi, "trailingPe", None)
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    try:
        info = tkr.info
        v = info.get("trailingPE")
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    return None


def _yf_pe_fwd(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("forwardPE")
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    return None


def _yf_rev_ttm(tkr) -> Optional[float]:
    # Intäkter TTM – plocka från financials/trailingTotalRevenue om möjligt.
    if tkr is None:
        return None
    # info
    try:
        info = tkr.info
        v = info.get("totalRevenue") or info.get("trailingTotalRevenue")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    # income stmt
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            if "Total Revenue" in fin.index:
                vals = fin.loc["Total Revenue"].dropna()
                if not vals.empty:
                    return float(vals.iloc[0])
    except Exception:
        pass
    return None


def _yf_ebitda_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("ebitda")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            if "Ebitda" in fin.index:
                vals = fin.loc["Ebitda"].dropna()
                if not vals.empty:
                    return float(vals.iloc[0])
    except Exception:
        pass
    return None


def _yf_p_b(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("priceToBook")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None


def _yf_bvps(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("bookValue")
        if v and v == v:
            return float(v)
    except Exception:
        pass
    return None


def _yf_ev_rev(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("enterpriseToRevenue")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None


def _yf_ev_ebitda(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("enterpriseToEbitda")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None


def _yf_dividend_annual(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # info → trailingAnnualDividendRate
    try:
        info = tkr.info
        v = info.get("trailingAnnualDividendRate")
        if v and v >= 0:
            return float(v)
    except Exception:
        pass
    # dividends-serien → summera senaste 12m
    try:
        divs = tkr.dividends
        if divs is not None and not divs.empty:
            last_12m = divs[divs.index >= (dt.datetime.utcnow() - dt.timedelta(days=365))]
            s = float(last_12m.sum())
            if s >= 0:
                return s
    except Exception:
        pass
    return None


def _yf_rev_cagr_hist(tkr) -> Optional[float]:
    """
    Hämtar revenueGrowth från Yahoo info (≈ senaste årets intäkts-tillväxt).
    Används som proxy för Rev CAGR i beräkningsmotorn.
    """
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("revenueGrowth")
        if v is None:
            return None
        fv = _f(v)
        if fv is None:
            return None
        return float(fv)
    except Exception:
        return None


def _yf_eps_cagr_hist(tkr) -> Optional[float]:
    """
    Hämtar earningsGrowth från Yahoo info (≈ senaste årets vinst-tillväxt).
    Används som proxy för EPS CAGR i beräkningsmotorn.
    """
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("earningsGrowth")
        if v is None:
            return None
        fv = _f(v)
        if fv is None:
            return None
        return float(fv)
    except Exception:
        return None


# ------------------------------
# Hämta ett paket för en ticker
# ------------------------------
def yahoo_fetch_for_ticker(sym: str) -> Dict[str, Any]:
    tkr = _yf_ticker(sym)

    info = None
    if tkr is not None:
        try:
            info = tkr.info
        except Exception:
            info = None

    short_name = None
    long_name = None
    sector = None
    industry = None

    if isinstance(info, dict):
        short_name = info.get("shortName")
        long_name = info.get("longName")
        sector = info.get("sector")
        industry = info.get("industry")

    company_name = short_name or long_name or sym
    company_sector = sector or industry

    out: Dict[str, Any] = {
        # Namn/sektor till Data-bladet
        "Bolagsnamn": company_name,
        "Sektor": company_sector,
        # Samma info även i "råa" fält (för valuation.fetch_from_yahoo m.m.)
        "name": company_name,
        "shortName": short_name,
        "longName": long_name,
        "sector": sector,
        "industry": industry,

        # Nyckeltal
        "Aktuell kurs": _yf_last_price(tkr),
        "Valuta": _yf_currency(tkr),
        "Utestående aktier": _yf_shares_out(tkr),
        "EPS TTM": _yf_eps_ttm(tkr),
        "PE TTM": _yf_pe_ttm(tkr),
        "PE FWD": _yf_pe_fwd(tkr),
        "Rev TTM": _yf_rev_ttm(tkr),
        "EBITDA TTM": _yf_ebitda_ttm(tkr),
        "EV/Revenue": _yf_ev_rev(tkr),
        "EV/EBITDA": _yf_ev_ebitda(tkr),
        "P/B": _yf_p_b(tkr),
        "BVPS": _yf_bvps(tkr),
        "Årlig utdelning": _yf_dividend_annual(tkr),
        # Tillväxtproxys
        "Rev CAGR": _yf_rev_cagr_hist(tkr),
        "EPS CAGR": _yf_eps_cagr_hist(tkr),
        # Resterande kan hämtas från andra källor / manuellt:
        "Net debt": None,
        "EPS 1Y": None,
        "EPS 2Y": None,
        "Rev 1Y": None,
        "Rev 2Y": None,
        "Utdelning CAGR": None,
    }
    return out


# --------------------------------------------
# Försiktig skrivning till Data-blad per rad
# --------------------------------------------
def _apply_fetch_to_row(row: pd.Series, fetched: Dict[str, Any]) -> pd.Series:
    """
    Endast skriva över de fält som har icke-None och meningsfulla värden.
    Respekterar principen: skriv över endast det som kunde hämtas.
    """
    if not isinstance(row, pd.Series):
        row = pd.Series(row)

    for key, val in fetched.items():
        if key not in row.index:
            continue
        if val is None:
            continue
        # Om numeriskt: NaN/None-skydd
        if isinstance(val, (int, float)) and not math.isfinite(float(val)):
            continue
        row[key] = val

    # Stämpla auto-källa/tid
    row["Senast auto uppdaterad"] = now_stamp()
    row["Auto källa"] = "Yahoo Finance"
    return row


def update_one_row_from_yahoo(df: pd.DataFrame, idx: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Uppdaterar en (1) rad i Data-bladet från Yahoo (om möjligt).
    Returnerar (df, status_dict).
    """
    if df is None or df.empty or idx < 0 or idx >= len(df):
        return df, {"ok": False, "msg": "Ogiltig radindex eller tom Data."}

    sym = str(df.at[idx, "Ticker"]).strip() if "Ticker" in df.columns else ""
    if not sym:
        return df, {"ok": False, "msg": "Saknar Ticker i vald rad."}

    try:
        fetched = yahoo_fetch_for_ticker(sym)
        row = df.iloc[idx].copy()
        row = _apply_fetch_to_row(row, fetched)
        df.iloc[idx] = row
        return df, {"ok": True, "msg": f"Uppdaterade {sym} från Yahoo."}
    except Exception as e:
        return df, {"ok": False, "msg": f"Fel vid uppdatering av {sym}: {e}"}


def mass_update_from_yahoo(
    df: pd.DataFrame,
    idx_list: List[int],
    sleep_sec: float = 1.0,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Massuppdaterar valda rader (en i taget) med fördröjning.
    Skriver inte till Sheets här (UI-delen sköter sparning).
    """
    if df is None or df.empty:
        return df, [{"ok": False, "msg": "Tom Data."}]

    logs: List[Dict[str, Any]] = []
    for i, idx in enumerate(idx_list, start=1):
        df, status = update_one_row_from_yahoo(df, idx)
        status["seq"] = f"{i}/{len(idx_list)}"
        logs.append(status)
        time.sleep(max(0.0, float(sleep_sec)))

    return df, logs
