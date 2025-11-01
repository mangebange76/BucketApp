# ============================================================
# BucketApp — värdering & riktkurser med estimat→CAGR-fallback
# Visning i bolagets valuta, portföljvärde i SEK
# ============================================================

from __future__ import annotations

import os
import time
import math
import json
import textwrap
from typing import Any, Dict, Optional, Tuple, List

import requests
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

# -------------------------
# Grundinställningar
# -------------------------
st.set_page_config(page_title="BucketApp – Riktkurser", page_icon="📈", layout="wide")

# Hastighet/kvoter
RATE_LIMIT_SLEEP = float(st.secrets.get("RATE_LIMIT_SLEEP", 0.35))

# ------------
# Hjälpfunktioner (säkra cast/NaN)
# ------------
def nz(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)) and not np.isnan(x):
            return float(x)
        if isinstance(x, str) and x.strip() != "":
            return float(x)
    except Exception:
        pass
    return default

def to_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x))):
            return default
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            xs = x.replace(" ", "").replace(",", ".")
            if xs == "":
                return default
            return float(xs)
    except Exception:
        return default

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b and abs(b) > 1e-12:
            return float(a) / float(b)
    except Exception:
        pass
    return default

def _df_pick_first(df: Optional[pd.DataFrame], keys: List[str]) -> Optional[float]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    vals: List[float] = []
    for k in keys:
        try:
            if k in df.index:
                ser = df.loc[k]
            elif k in df.columns:
                ser = df[k]
            else:
                continue
            if hasattr(ser, "iloc"):
                v = ser.dropna()
                if not v.empty:
                    vals.append(float(v.iloc[0]))
        except Exception:
            continue
    return vals[0] if vals else None

def _fi_get(fi, key: str) -> Optional[float]:
    try:
        return getattr(fi, key, None)
    except Exception:
        return None

def _resolve_price(*candidates, mcap=None, shares=None) -> Optional[float]:
    for c in candidates:
        try:
            if c is not None and float(c) > 0:
                return float(c)
        except Exception:
            continue
    try:
        if mcap and shares and float(shares) > 0:
            return float(mcap) / float(shares)
    except Exception:
        pass
    return None

# ---------------------------------------------------
# Google Sheets: auth + öppna/garantiera arbetsblad
# ---------------------------------------------------
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

@st.cache_resource(show_spinner=False)
def _gs_client():
    creds_obj = st.secrets.get("GOOGLE_CREDENTIALS", None)
    if creds_obj is None:
        st.error("Saknar GOOGLE_CREDENTIALS i secrets.")
        st.stop()
    if isinstance(creds_obj, str):
        creds_obj = json.loads(creds_obj)
    creds_obj = _normalize_private_key(dict(creds_obj))
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_obj, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet():
    gc = _gs_client()
    sheet_url = st.secrets.get("SHEET_URL", "").strip()
    sheet_id  = st.secrets.get("SHEET_ID", "").strip()
    try:
        if sheet_url:
            return gc.open_by_url(sheet_url)
        if sheet_id:
            return gc.open_by_key(sheet_id)
    except Exception as e:
        st.error("Kunde inte öppna Google Sheet. Kontrollera SHEET_URL/SHEET_ID och delning.")
        st.stop()

def _get_ws(spread, title: str) -> gspread.Worksheet:
    try:
        return spread.worksheet(title)
    except WorksheetNotFound:
        return spread.add_worksheet(title=title, rows=2000, cols=50)

SPREAD = _open_spreadsheet()
WS_DATA       = _get_ws(SPREAD, "Data")
WS_RESULT     = _get_ws(SPREAD, "Resultat")
WS_VALUTA     = _get_ws(SPREAD, "Valutakurser")
WS_HIST       = _get_ws(SPREAD, "Historik")

# ---------------------------
# Läs/skriv Data som DataFrame
# ---------------------------
DATA_COLUMNS = [
    "Ticker", "Bolagsnamn", "Bucket", "Valuta", "Antal aktier",
    "Last Price", "Market Cap", "EV", "Shares Out",
    "PE TTM", "PE FWD", "EV/Revenue", "EV/EBITDA",
    "EPS_CAGR_5Y",
    # manuella multiplar/parametrar
    "ev_s_mult", "ev_eb_mult", "ev_fcf_mult", "dacf_mult",
    "p_fcf_mult", "p_nav_mult", "p_affo_mult", "p_b_mult", "p_tbv_mult", "p_nii_mult",
    # proxies per aktie (om du vill fylla manuellt)
    "FCF/ps", "NAV/ps", "AFFO/ps", "BV/ps", "TBV/ps", "NII/ps",
    # TTM baser
    "Revenue TTM", "EBITDA TTM", "FCF TTM",
    # Manuell g om inget estimat/CAGR
    "G1", "G2", "G3",
]

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    return df[DATA_COLUMNS]

@st.cache_data(show_spinner=False, ttl=30)
def read_data_df() -> pd.DataFrame:
    records = WS_DATA.get_all_records(numericise_ignore=['all'])
    df = pd.DataFrame(records)
    if df.empty:
        df = pd.DataFrame(columns=DATA_COLUMNS)
    df = _ensure_columns(df)
    return df

def write_data_df(df: pd.DataFrame):
    df = _ensure_columns(df.copy())
    WS_DATA.clear()
    WS_DATA.update([df.columns.tolist()] + df.fillna("").values.tolist())

# ---------------------------
# Valutakurser (→ SEK)
# ---------------------------
YF_FX_MAP = {
    "USD": "USDSEK=X",
    "EUR": "EURSEK=X",
    "NOK": "NOKSEK=X",
    "CAD": "CADSEK=X",
    "GBP": "GBPSEK=X",
    "DKK": "DKKSEK=X",
    "SEK": None,
}

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_fx_to_sek() -> Dict[str, float]:
    rates = {"SEK": 1.0}
    for ccy, yfsym in YF_FX_MAP.items():
        if yfsym is None:
            rates[ccy] = 1.0
            continue
        try:
            t = yf.Ticker(yfsym)
            h = t.history(period="5d", interval="1d")
            if h is not None and not h.empty:
                rates[ccy] = float(h["Close"].dropna().iloc[-1])
        except Exception:
            continue
        time.sleep(0.05)
    return rates

def push_fx_sheet(rates: Dict[str, float]):
    WS_VALUTA.clear()
    rows = [["Valuta", "SEK"]]
    for k, v in rates.items():
        rows.append([k, v])
    WS_VALUTA.update(rows)

@st.cache_data(show_spinner=False, ttl=1800)
def load_fx_from_sheet() -> Dict[str, float]:
    try:
        vals = WS_VALUTA.get_all_values()
        out = {}
        for r in vals[1:]:
            if len(r) >= 2:
                k = (r[0] or "").strip()
                v = to_float(r[1], None)
                if k and v is not None:
                    out[k] = float(v)
        if not out:
            out = fetch_fx_to_sek()
            push_fx_sheet(out)
        return out
    except Exception:
        out = fetch_fx_to_sek()
        push_fx_sheet(out)
        return out

def fx_to_sek(ccy: str, rates: Dict[str, float]) -> float:
    return float(rates.get(ccy, 1.0))

# ============================================================
# Del 2/4 — Datakällor (Yahoo/Finnhub), CAGR & hjälpfunktioner
# ============================================================

# --------- Tidsstämplar ---------
def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def yq_of(ts: Optional[str] = None) -> Tuple[int, int, str]:
    """
    Returnerar (år, kvartal, "YYYY-QN") för nu eller given ts "YYYY-mm-dd HH:MM:SS".
    """
    if ts:
        y = int(ts[:4]); m = int(ts[5:7])
    else:
        y = int(time.strftime("%Y")); m = int(time.strftime("%m"))
    q = 1 + (m - 1) // 3
    return y, q, f"{y}-Q{q}"

# --------- Liten formatterare ---------
def fmt2(x: Any) -> str:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "–"
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

# ============================================================
# YAHOO — ögonblicksdata + multiplar
# ============================================================
@st.cache_data(show_spinner=False, ttl=900)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    t = yf.Ticker(ticker)
    snap: Dict[str, Any] = {}

    # fast_info + info
    try:
        fi = t.fast_info
    except Exception:
        fi = None

    def _fi(key):
        try:
            return getattr(fi, key)
        except Exception:
            return None

    try:
        info = t.info or {}
    except Exception:
        info = {}

    last_price_cands = [
        info.get("regularMarketPrice"),
        info.get("currentPrice"),
        _fi("last_price"),
    ]

    # history som fallback
    hist_px = None
    try:
        hist = t.history(period="5d", interval="1d")
        if hist is not None and not hist.empty:
            hist_px = float(hist["Close"].dropna().iloc[-1])
            last_price_cands.append(hist_px)
    except Exception:
        pass

    currency   = _fi("currency") or info.get("currency") or "SEK"
    market_cap = _fi("market_cap") or info.get("marketCap")
    shares_out = info.get("sharesOutstanding")

    # härled missing MC/Shares
    try:
        px_tmp = _resolve_price(*last_price_cands, mcap=market_cap, shares=shares_out)
        if shares_out is None and market_cap and px_tmp:
            shares_out = float(market_cap) / float(px_tmp)
        if market_cap is None and shares_out and px_tmp:
            market_cap = float(shares_out) * float(px_tmp)
    except Exception:
        pass

    # statements
    try:
        income = getattr(t, "income_stmt", None)
        if income is None or income.empty:
            income = getattr(t, "financials", pd.DataFrame())
    except Exception:
        income = pd.DataFrame()
    try:
        cashf = getattr(t, "cash_flow", None)
        if cashf is None or cashf.empty:
            cashf = getattr(t, "cashflow", pd.DataFrame())
    except Exception:
        cashf = pd.DataFrame()
    try:
        bal = getattr(t, "balance_sheet", pd.DataFrame())
    except Exception:
        bal = pd.DataFrame()

    rev_ttm   = _df_pick_first(income, ["Total Revenue","TotalRevenue","Revenue"])
    ebitda_tt = _df_pick_first(income, ["EBITDA"])
    ocf       = _df_pick_first(cashf, ["Total Cash From Operating Activities","Operating Cash Flow","OperatingCashFlow"])
    capex     = _df_pick_first(cashf, ["Capital Expenditures","CapitalExpenditures"])
    fcf_ttm   = (ocf - abs(capex)) if (ocf is not None and capex is not None) else None

    total_debt = _df_pick_first(bal, ["Total Debt","TotalDebt"])
    cash       = _df_pick_first(bal, ["Cash And Cash Equivalents","Cash","Cash And Short Term Investments","CashAndShortTermInvestments"])

    ev = info.get("enterpriseValue")
    if ev is None and market_cap is not None:
        ev = float(market_cap) + float(nz(total_debt, 0.0)) - float(nz(cash, 0.0))

    px = _resolve_price(*last_price_cands, mcap=market_cap, shares=shares_out)

    div_ps    = info.get("dividendRate") or info.get("trailingAnnualDividendRate")
    div_yield = info.get("dividendYield") or info.get("trailingAnnualDividendYield")

    # Multiplar (ankare)
    forward_pe  = info.get("forwardPE")
    trailing_pe = info.get("trailingPE")
    ev_to_rev   = info.get("enterpriseToRevenue")
    ev_to_ebit  = info.get("enterpriseToEbitda")

    snap.update({
        "currency": currency,
        "last_price": px,
        "market_cap": market_cap,
        "enterprise_value": ev,
        "shares_out": shares_out,
        "short_name": info.get("shortName"),
        "long_name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "revenue_ttm": rev_ttm,
        "ebitda_ttm": ebitda_tt,
        "fcf_ttm": fcf_ttm,
        "total_debt": total_debt,
        "cash": cash,
        "dividend_ps": div_ps,
        "dividend_yield": div_yield,
        "pe_forward": forward_pe,
        "pe_ttm": trailing_pe,
        "ev_to_revenue": ev_to_rev,
        "ev_to_ebitda": ev_to_ebit,
    })
    return snap

# ============================================================
# FINNHUB — metrics (P/E-band, EPS/BV), estimates & EPS-CAGR
# ============================================================
def _sec_headers():
    ua = st.secrets.get("SEC_USER_AGENT", "BucketApp/1.0 (contact: you@example.com)")
    return {"User-Agent": ua}

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_finnhub_metrics(symbol: str) -> Dict[str, Any]:
    api = st.secrets.get("FINNHUB_API_KEY", "")
    if not api:
        return {}
    url = f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={api}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return {}
        data = r.json() or {}
    except Exception:
        return {}
    metric = data.get("metric", {}) or {}
    series = data.get("series", {}) or {}

    eps_ttm = metric.get("epsBasicExclExtraTTM") or metric.get("epsInclExtraTTM") or metric.get("epsTTM")
    book_ps = metric.get("bookValuePerShareAnnual") or metric.get("bookValuePerShareTTM")

    pe_ttm = metric.get("peExclExtraTTM") or metric.get("peTTM")
    pb_ttm = metric.get("pbAnnual") or metric.get("pbTTM") or metric.get("priceToBookAnnual") or metric.get("priceToBookTTM")

    # P/E-band ~ percentiler av historiska P/E
    vals: List[float] = []
    def _collect(arr):
        for item in arr or []:
            v = item.get("v")
            if isinstance(v, (int, float)) and not math.isnan(v) and v > 0:
                vals.append(float(v))
    try:
        q = series.get("quarterly", {}) or {}
        for key in ("peBasicExclExtraTTM","peExclExtraTTM","peTTM"):
            _collect(q.get(key, []))
        a = series.get("annual", {}) or {}
        for key in ("peBasicExclExtraAnnual","peExclExtraAnnual","peAnnual"):
            _collect(a.get(key, []))
    except Exception:
        pass

    vals = [v for v in vals if v > 0]
    if len(vals) > 20:
        vals = vals[-20:]
    pe_p25 = np.percentile(vals, 25) if vals else None
    pe_p50 = np.percentile(vals, 50) if vals else None
    pe_p75 = np.percentile(vals, 75) if vals else None
    if pe_p50 is None:
        pe_p50 = pe_ttm

    return {
        "eps_ttm": eps_ttm, "book_ps": book_ps,
        "pe_ttm": pe_ttm, "pb_ttm": pb_ttm,
        "pe_band": (pe_p25, pe_p50, pe_p75)
    }

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_finnhub_estimates(symbol: str) -> dict:
    api = st.secrets.get("FINNHUB_API_KEY", "")
    if not api:
        return {}
    def _get(url):
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 200:
                return r.json() or {}
        except Exception:
            pass
        return {}

    epsj = _get(f"https://finnhub.io/api/v1/stock/eps-estimates?symbol={symbol}&freq=annual&token={api}")
    revj = _get(f"https://finnhub.io/api/v1/stock/revenue-estimates?symbol={symbol}&freq=annual&token={api}")

    def _series(obj, key):
        arr = obj.get("data") or obj.get("series") or []
        out = []
        for it in arr:
            v = it.get(key)
            if isinstance(v, (int, float)):
                try:
                    y = int(str(it.get("fiscalYear") or it.get("period") or "")[:4])
                except Exception:
                    y = None
                out.append((y, float(v)))
        out = [(y, v) for (y, v) in out if v is not None]
        out.sort(key=lambda x: (x[0] is None, x[0]))
        return out

    eps_ser = _series(epsj, "epsAvg")
    rev_ser = _series(revj, "revenueAvg")

    def _growth_from_series(ser):
        gs = []
        for i in range(1, min(4, len(ser))):
            prev, cur = ser[i-1][1], ser[i][1]
            if prev:
                gs.append((cur/prev) - 1.0)
        while len(gs) < 3:
            gs.append(gs[-1] if gs else 0.0)
        return tuple(gs[:3])

    out = {}
    if eps_ser:
        out["eps_series"] = eps_ser[:]
        if len(eps_ser) >= 1: out["eps_next1"] = eps_ser[-1][1]
        if len(eps_ser) >= 2: out["eps_next2"] = eps_ser[-2][1]
        out["g_eps"] = _growth_from_series(eps_ser)
    if rev_ser:
        out["rev_series"] = rev_ser[:]
        if len(rev_ser) >= 1: out["rev_next1"] = rev_ser[-1][1]
        if len(rev_ser) >= 2: out["rev_next2"] = rev_ser[-2][1]
        out["g_rev"] = _growth_from_series(rev_ser)
    return out

@st.cache_data(show_spinner=False, ttl=1800)
def fetch_finnhub_eps_quarterly(symbol: str) -> List[Tuple[int,int,float]]:
    """ [(år, kvartal, epsActual), ...] """
    api = st.secrets.get("FINNHUB_API_KEY", "")
    if not api:
        return []
    url = f"https://finnhub.io/api/v1/stock/earnings?symbol={symbol}&token={api}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return []
        js = r.json() or []
    except Exception:
        return []
    out: List[Tuple[int,int,float]] = []
    for it in js:
        try:
            y = int(it.get("year")); q = int(it.get("quarter"))
            ea = it.get("epsActual")
            if isinstance(ea, (int, float)):
                out.append((y, q, float(ea)))
        except Exception:
            continue
    out.sort(key=lambda x: (x[0], x[1]))
    return out

def eps_cagr_from_quarters(qeps: List[Tuple[int,int,float]], years: int = 5) -> Optional[float]:
    """CAGR på års-EPS (summa kvartal per år) över 5y→3y fallback."""
    if not qeps:
        return None
    per_year: Dict[int, float] = {}
    for y, q, v in qeps:
        per_year[y] = per_year.get(y, 0.0) + float(v)
    ys = sorted(per_year.keys())
    if len(ys) < 2:
        return None
    for span in (years, 3):
        if len(ys) >= span + 1:
            y0, yN = ys[-(span+1)], ys[-1]
            v0, vN = per_year.get(y0, 0.0), per_year.get(yN, 0.0)
            if v0 > 0 and vN > 0:
                return (vN / v0) ** (1.0 / span) - 1.0
    return None

@st.cache_data(show_spinner=False, ttl=1800)
def yahoo_revenue_cagr(ticker: str, years: int = 5) -> Optional[float]:
    """CAGR på omsättning från Yahoo (annual income_stmt)."""
    try:
        t = yf.Ticker(ticker)
        inc = getattr(t, "income_stmt", None)
        if inc is None or inc.empty:
            inc = getattr(t, "financials", pd.DataFrame())
        if inc is None or inc.empty:
            return None
        row = None
        if "Total Revenue" in inc.index:
            row = inc.loc["Total Revenue"]
        elif "TotalRevenue" in inc.index:
            row = inc.loc["TotalRevenue"]
        if row is None:
            return None
        vals = [nz(row.get(col), 0.0) for col in inc.columns]
        vals = [v for v in vals if v]
        if len(vals) < 2:
            return None
        vals = vals[:years][::-1]  # äldst → nyast
        start, end = vals[0], vals[-1]
        n = max(1, len(vals)-1)
        if start > 0 and end > 0:
            return (end/start)**(1.0/n) - 1.0
    except Exception:
        return None
    return None

# ============================================================
# Serie- & multipel-hjälpare (estimat→CAGR, kompression)
# ============================================================
def series_from_est_or_cagr(base: float,
                            est1: Optional[float], est2: Optional[float],
                            cagr: Optional[float],
                            g1: float, g2: float, g3: float) -> Tuple[float,float,float,float]:
    """
    Returnerar (v0, v1, v2, v3).
    Prioritet: explicita estimat för år1/år2 → annars CAGR → annars manuella g1..g3.
    """
    v0 = float(nz(base, 0.0))

    if est1 is not None:
        v1 = float(est1)
    else:
        r1 = (cagr if cagr is not None else g1)
        v1 = v0 * (1.0 + r1)

    if est2 is not None:
        v2 = float(est2)
    else:
        r2 = (cagr if cagr is not None else g2)
        v2 = v1 * (1.0 + r2)

    r3 = (cagr if cagr is not None else g3)
    v3 = v2 * (1.0 + r3)
    return v0, v1, v2, v3

def ev_multiple_from_series(v0: float, v1: float, v2: float, v3: float,
                            mult: float, comp_rate: float,
                            net_debt: float, shares_out: float) -> Tuple[float,float,float,float]:
    """
    EV-baserade målpriser med multipelkompression.
    År n får multipeln: mult * (1-comp_rate)**n
    """
    comp1 = (1.0 - comp_rate) ** 1
    comp2 = (1.0 - comp_rate) ** 2
    comp3 = (1.0 - comp_rate) ** 3

    def _px(x, m):
        ev = float(x) * float(m)
        return safe_div(ev + float(net_debt), float(shares_out), 0.0)

    px0 = _px(v0, mult)
    px1 = _px(v1, mult * comp1)
    px2 = _px(v2, mult * comp2)
    px3 = _px(v3, mult * comp3)
    return px0, px1, px2, px3

def price_multiple_from_series(ps0: float, ps1: float, ps2: float, ps3: float,
                               mult: float, comp_rate: float) -> Tuple[float,float,float,float]:
    comp1 = (1.0 - comp_rate) ** 1
    comp2 = (1.0 - comp_rate) ** 2
    comp3 = (1.0 - comp_rate) ** 3
    return (
        float(mult)*float(ps0),
        float(mult)*comp1*float(ps1),
        float(mult)*comp2*float(ps2),
        float(mult)*comp3*float(ps3),
    )

def project_tbv_per_share(tbv0_ps: float, rotce: float, payout_ratio: float) -> Tuple[float, float, float]:
    g = float(rotce) * (1.0 - float(payout_ratio))
    tbv1 = float(tbv0_ps) * (1.0 + g)
    tbv2 = tbv1 * (1.0 + g)
    tbv3 = tbv2 * (1.0 + g)
    return tbv1, tbv2, tbv3

def bull_bear(base_1y: float, bull_mult: float, bear_mult: float) -> Tuple[float, float]:
    return float(nz(base_1y, 0.0)) * float(nz(bull_mult, 1.0)), float(nz(base_1y, 0.0)) * float(nz(bear_mult, 1.0))

# ============================================================
# P/E – riktkurser från estimat + EPS-CAGR + multipel-kompression
# ============================================================
def pe_targets_from_estimates(price: float,
                              pe_ttm: Optional[float], pe_fwd: Optional[float],
                              pe_band: Optional[Tuple[Optional[float],Optional[float],Optional[float]]],
                              eps_ttm: Optional[float],
                              eps_next1: Optional[float],
                              eps_next2: Optional[float],
                              eps_cagr: Optional[float],
                              pe_comp_rate: float,
                              g3: float) -> Tuple[float,float,float,float,str]:
    """
    Idag:   eps_ttm × PE_TTM_ankare
    1 år:   eps1   × PE_FWD_ankare*(1 - pe_comp_rate)^1
    2 år:   eps2   × PE_FWD_ankare*(1 - pe_comp_rate)^2
    3 år:   eps3   × PE_FWD_ankare*(1 - pe_comp_rate)^3  (eps3 = eps2*(1+g3))
    Ankare: blandning av p50 och faktiska PE (ttm/fwd) om de finns.
    """
    p25,p50,p75 = (pe_band or (None,None,None))
    def _mix(a,b):
        if a and b: return (float(a)+float(b))/2.0
        return a or b or None

    pe_ttm_anchor = _mix(pe_ttm, p50)
    pe_fwd_anchor = _mix(pe_fwd, p50)

    # EPS TTM – om inte given, härled via price/PE TTM
    if not eps_ttm and pe_ttm and pe_ttm > 0 and price and price > 0:
        eps_ttm = price / pe_ttm

    # Fallback EPS via EPS-CAGR om estimat saknas
    def _fallback_eps(base_eps: Optional[float], prev: Optional[float]) -> float:
        if prev is not None:
            return prev
        if base_eps is not None:
            g = eps_cagr if (eps_cagr is not None) else g3
            return base_eps * (1.0 + g)
        return 0.0

    eps1 = eps_next1 if eps_next1 is not None else _fallback_eps(eps_ttm, None)
    eps2 = eps_next2 if eps_next2 is not None else _fallback_eps(eps1 if eps1>0 else eps_ttm, None)
    eps3 = (eps2 if eps2>0 else eps1) * (1.0 + g3)

    c1 = (1.0 - pe_comp_rate) ** 1
    c2 = (1.0 - pe_comp_rate) ** 2
    c3 = (1.0 - pe_comp_rate) ** 3

    t0 = (eps_ttm or 0.0) * float(pe_ttm_anchor or pe_fwd_anchor or p50 or 0.0)
    anchor = float(pe_fwd_anchor or pe_ttm_anchor or p50 or 0.0)
    t1 = eps1 * (anchor * c1)
    t2 = eps2 * (anchor * c2)
    t3 = eps3 * (anchor * c3)

    comment = f"pe_ttm={pe_ttm_anchor}, pe_fwd={pe_fwd_anchor}, cagr={None if eps_cagr is None else round(eps_cagr,4)}, comp={pe_comp_rate}"
    return t0,t1,t2,t3,comment

# ============================================================
# Heuristik för val av primär metod
# ============================================================
BDC_TICKERS = {"CSWC","PFLT","HRZN","ARCC","MAIN","FSK","OCSL","ORCC"}
REIT_HINTS = {"reit"}
BANK_HINTS = {"bank","banks","thrifts","credit","financial services"}
INSURANCE_HINTS = {"insurance"}
ENERGY_HINTS = {"oil","gas","energy","midstream","mlp"}
SHIPPING_HINTS = {"marine","shipping","tanker","bulk"}
SAAS_HINTS = {"software","application","it services","cloud"}

def choose_primary_method(bucket: str, sector: str, industry: str, ticker: str,
                          has_fcf: bool, has_ebitda: bool) -> str:
    tk = (ticker or "").upper()
    s = (sector or "").lower()
    i = (industry or "").lower()

    if tk in BDC_TICKERS or "bdc" in i:
        return "p_nii"
    if any(k in i for k in REIT_HINTS):
        return "p_affo"
    if any(k in s for k in BANK_HINTS) or any(k in i for k in BANK_HINTS) or any(k in i for k in INSURANCE_HINTS):
        return "p_tbv"
    if any(k in s for k in ENERGY_HINTS) or any(k in i for k in ENERGY_HINTS) or any(k in i for k in SHIPPING_HINTS):
        return "ev_dacf" if has_ebitda else "ev_ebitda"
    if any(k in s for k in SAAS_HINTS) or any(k in i for k in SAAS_HINTS):
        return "ev_fcf" if has_fcf else "ev_sales"

    b = (bucket or "").lower()
    if "tillväxt" in b:
        if has_fcf:
            return "ev_fcf"
        if has_ebitda:
            return "ev_ebitda"
        return "ev_sales"
    else:
        if has_fcf:
            return "p_fcf"
        return "p_b"

# ============================================================
# Del 3/4 — UI, formulär, spara/uppdatera, FX & massuppdatering
# ============================================================

# -------------------------
# Små util-helpers för Data
# -------------------------
def upsert_row_df(df: pd.DataFrame, key_col: str, row: Dict[str, Any]) -> pd.DataFrame:
    """Upsert på key_col, returnerar ny df."""
    df = df.copy()
    key = str(row.get(key_col, "")).strip()
    if not key:
        return df
    mask = (df[key_col].astype(str).str.strip() == key)
    if mask.any():
        idx = df.index[mask][0]
        for k, v in row.items():
            if k in df.columns:
                df.at[idx, k] = v
    else:
        # se till att alla kolumner finns
        for c in DATA_COLUMNS:
            row.setdefault(c, np.nan)
        df = pd.concat([df, pd.DataFrame([row])[DATA_COLUMNS]], ignore_index=True)
    return df

def persist_fx_latest():
    rates = fetch_fx_to_sek()
    push_fx_sheet(rates)
    return rates

# -------------------------
# Sidopanel — inställningar
# -------------------------
with st.sidebar:
    st.header("Inställningar")

    # Källor
    use_finnhub = st.checkbox("Använd Finnhub (estimat, P/E-band, EPS-CAGR)", value=True)

    st.markdown("---")
    st.subheader("Paus/kvoter")
    rate_limit_sleep = st.number_input("Paus mellan nätanrop (sek)", min_value=0.0, value=RATE_LIMIT_SLEEP, step=0.05, format="%.2f")

    st.markdown("---")
    st.subheader("Bull/Bear (på 1 år)")
    bull_mult = st.number_input("Bull ×", value=1.15, step=0.05, format="%.2f")
    bear_mult = st.number_input("Bear ×", value=0.85, step=0.05, format="%.2f")

    st.markdown("---")
    st.subheader("Multipelkompression (per år)")
    pe_comp_pct = st.slider("P/E-kompression (%)", 0.0, 20.0, 4.0, 0.5)
    ev_comp_pct = st.slider("EV-multiplar (EV/S, EV/EBITDA, EV/FCF, EV/DACF) (%)", 0.0, 20.0, 3.0, 0.5)
    p_comp_pct  = st.slider("P-multiplar (P/FCF, P/B, P/NAV, P/TBV, P/NII, P/AFFO) (%)", 0.0, 20.0, 2.0, 0.5)

    pe_comp_rate = pe_comp_pct/100.0
    ev_comp_rate = ev_comp_pct/100.0
    p_comp_rate  = p_comp_pct/100.0

    st.markdown("---")
    st.subheader("Tillväxtkälla (Auto)")
    growth_source = st.selectbox(
        "Välj tillväxtkälla för projektion (fallbackordning inbyggd i Del 4):",
        ["Auto (Analytiker → CAGR → Manuell)"], index=0
    )
    cagr_blend_w = st.slider("Vikt vid blandning (CAGR vs Manuell) – används vid brist på estimat", 0.0, 1.0, 0.50, 0.05)

    st.markdown("---")
    st.subheader("Filter")
    bucket_opts = [
        "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
        "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning",
    ]
    pick_buckets = st.multiselect("Buckets att visa", bucket_opts, default=bucket_opts)
    only_owned   = st.checkbox("Visa endast innehav (>0 aktier)", value=False)
    only_watch   = st.checkbox("Visa endast bevakning (=0 aktier)", value=False)

    st.markdown("---")
    if st.button("🔄 Uppdatera alla (Yahoo + Finnhub + FX)"):
        st.session_state.__dict__["__do_mass_refresh__"] = True

    st.caption("Priser & riktkurser visas i bolagets egen valuta. Portföljsummor räknas i SEK.")

    st.markdown("---")
    st.subheader("FX (för SEK-summering)")
    try:
        fx_now = load_fx_from_sheet()
        st.write({k: round(v, 4) for k, v in fx_now.items() if k in ("USD","EUR","NOK","CAD","GBP","DKK","SEK")})
    except Exception:
        st.write("—")

# -------------------------
# Formulär: Lägg till / uppdatera
# -------------------------
st.markdown("## ➕ Lägg till / uppdatera bolag")

c_top = st.columns(5)
ticker_in = c_top[0].text_input("Ticker (t.ex. NVDA)", "")
bucket_in = c_top[1].selectbox("Bucket", bucket_opts, index=0)
antal_in  = c_top[2].number_input("Antal aktier", min_value=0, value=0, step=1)
pref_method_in = c_top[3].selectbox(
    "Preferred metod (om lämnas = AUTO)",
    ["AUTO","pe_hist_vs_eps","ev_fcf","p_fcf","ev_sales","ev_ebitda","p_nav","ev_dacf","p_affo","p_b","p_tbv","p_nii"],
    index=0
)
g1_in = c_top[4].number_input("G1 (år 1, 0.15=15%)", value=0.15, step=0.01, format="%.2f")
g2_in = st.number_input("G2 (år 2)", value=0.12, step=0.01, format="%.2f")
g3_in = st.number_input("G3 (år 3)", value=0.10, step=0.01, format="%.2f")

with st.expander("Avancerat (multiplar & per-aktie-proxy)"):
    c1,c2,c3,c4 = st.columns(4)
    ev_s_mult      = c1.number_input("EV/S-multiple", value=5.0, step=0.5, format="%.2f")
    ev_eb_mult     = c2.number_input("EV/EBITDA-multiple", value=12.0, step=0.5, format="%.2f")
    ev_fcf_mult    = c3.number_input("EV/FCF-multiple", value=18.0, step=0.5, format="%.2f")
    dacf_mult      = c4.number_input("EV/DACF-multiple", value=12.0, step=0.5, format="%.2f")

    c5,c6,c7,c8 = st.columns(4)
    p_fcf_mult    = c5.number_input("P/FCF-multiple", value=20.0, step=0.5, format="%.2f")
    p_nav_mult    = c6.number_input("P/NAV-multiple", value=1.00, step=0.05, format="%.2f")
    p_affo_mult   = c7.number_input("P/AFFO-multiple (REIT)", value=13.0, step=0.5, format="%.2f")
    p_b_mult      = c8.number_input("P/B-multiple", value=1.50, step=0.05, format="%.2f")

    c9,c10,c11,c12 = st.columns(4)
    p_tbv_mult     = c9.number_input("P/TBV-multiple (bank)", value=1.20, step=0.05, format="%.2f")
    p_nii_mult     = c10.number_input("P/NII-multiple (BDC)", value=10.0, step=0.5, format="%.2f")
    bv_ps0         = c11.number_input("BV/ps (om källa saknas)", value=0.00, step=0.01, format="%.2f")
    tbv_ps0        = c12.number_input("TBV/ps (om källa saknas)", value=0.00, step=0.01, format="%.2f")

    c13,c14,c15,c16 = st.columns(4)
    nav_ps0 = c13.number_input("NAV/ps (om källa saknas)", value=0.00, step=0.01, format="%.2f")
    affo_ps0= c14.number_input("AFFO/ps (om källa saknas)", value=0.00, step=0.01, format="%.2f")
    fcf_ps0 = c15.number_input("FCF/ps (om källa saknas)", value=0.00, step=0.01, format="%.2f")
    nii_ps0 = c16.number_input("NII/ps (om källa saknas)", value=0.00, step=0.01, format="%.2f")

save_clicked = st.button("💾 Spara till Google Sheets (hämtar Yahoo + Finnhub + FX)")

# -------------------------
# Spara/uppdatera en ticker
# -------------------------
def handle_one_ticker_save(ticker: str,
                           bucket: str,
                           antal: int,
                           pref_method: str,
                           g1: float, g2: float, g3: float,
                           adv: Dict[str, Any],
                           use_finn: bool,
                           rate_sleep: float) -> Dict[str, Any]:
    tk = (ticker or "").strip().upper()
    if not tk:
        return {}

    # 1) Yahoo
    snap = fetch_yahoo_snapshot(tk)
    time.sleep(rate_sleep)

    # 2) Finnhub – nivådata + EPS-CAGR
    eps_cagr = None
    if use_finn:
        try:
            qeps = fetch_finnhub_eps_quarterly(tk)
            time.sleep(rate_sleep)
            eps_cagr = eps_cagr_from_quarters(qeps, years=5)
            if eps_cagr is None:
                eps_cagr = eps_cagr_from_quarters(qeps, years=3)
        except Exception:
            eps_cagr = None

    # 3) FX → uppdatera valutaflik
    try:
        fx = fetch_fx_to_sek()
        push_fx_sheet(fx)
    except Exception:
        pass

    # 4) Bygg rad
    row = {
        "Ticker": tk,
        "Bolagsnamn": snap.get("long_name") or snap.get("short_name") or "",
        "Bucket": bucket,
        "Valuta": snap.get("currency") or "SEK",
        "Antal aktier": int(antal),

        # Snapshot
        "Last Price":  snap.get("last_price") or np.nan,
        "Market Cap":  snap.get("market_cap") or np.nan,
        "EV":          snap.get("enterprise_value") or np.nan,
        "Shares Out":  snap.get("shares_out") or np.nan,
        "Revenue TTM": snap.get("revenue_ttm") or np.nan,
        "EBITDA TTM":  snap.get("ebitda_ttm") or np.nan,
        "FCF TTM":     snap.get("fcf_ttm") or np.nan,
        "PE TTM":      snap.get("pe_ttm") or np.nan,
        "PE FWD":      snap.get("pe_forward") or np.nan,
        "EV/Revenue":  snap.get("ev_to_revenue") or np.nan,
        "EV/EBITDA":   snap.get("ev_to_ebitda") or np.nan,

        # Manuell styrning & proxies
        "G1": g1, "G2": g2, "G3": g3,
        "ev_s_mult":   adv.get("ev_s_mult"),
        "ev_eb_mult":  adv.get("ev_eb_mult"),
        "ev_fcf_mult": adv.get("ev_fcf_mult"),
        "dacf_mult":   adv.get("dacf_mult"),
        "p_fcf_mult":  adv.get("p_fcf_mult"),
        "p_nav_mult":  adv.get("p_nav_mult"),
        "p_affo_mult": adv.get("p_affo_mult"),
        "p_b_mult":    adv.get("p_b_mult"),
        "p_tbv_mult":  adv.get("p_tbv_mult"),
        "p_nii_mult":  adv.get("p_nii_mult"),

        "BV/ps":   adv.get("bv_ps0"),
        "TBV/ps":  adv.get("tbv_ps0"),
        "NAV/ps":  adv.get("nav_ps0"),
        "AFFO/ps": adv.get("affo_ps0"),
        "FCF/ps":  adv.get("fcf_ps0"),
        "NII/ps":  adv.get("nii_ps0"),

        "EPS_CAGR_5Y": eps_cagr if eps_cagr is not None else np.nan,
    }

    # 5) Upsert i Data
    df = read_data_df()
    df2 = upsert_row_df(df, "Ticker", row)
    write_data_df(df2)

    return row

# -------------------------
# Klick: Spara en
# -------------------------
if save_clicked and ticker_in:
    adv = dict(
        ev_s_mult=ev_s_mult, ev_eb_mult=ev_eb_mult, ev_fcf_mult=ev_fcf_mult, dacf_mult=dacf_mult,
        p_fcf_mult=p_fcf_mult, p_nav_mult=p_nav_mult, p_affo_mult=p_affo_mult,
        p_b_mult=p_b_mult, p_tbv_mult=p_tbv_mult, p_nii_mult=p_nii_mult,
        bv_ps0=bv_ps0, tbv_ps0=tbv_ps0, nav_ps0=nav_ps0, affo_ps0=affo_ps0, fcf_ps0=fcf_ps0, nii_ps0=nii_ps0
    )
    saved = handle_one_ticker_save(
        ticker_in, bucket_in, int(antal_in), pref_method_in,
        float(g1_in), float(g2_in), float(g3_in),
        adv, use_finnhub, rate_limit_sleep
    )
    if saved.get("Last Price"):
        st.success(f"{ticker_in} sparad/uppdaterad i fliken **Data**.")
    else:
        st.warning(f"{ticker_in}: kunde inte läsa pris från Yahoo just nu.")

# -------------------------
# Massuppdatering (om klickad i sidopanel)
# -------------------------
if st.session_state.get("__do_mass_refresh__"):
    st.session_state.__dict__.pop("__do_mass_refresh__", None)
    df = read_data_df()
    if df.empty:
        st.info("Inga rader i Data ännu.")
    else:
        progress = st.progress(0.0, text="Startar massuppdatering …")
        n = len(df)
        for i, r in df.iterrows():
            tk = str(r.get("Ticker","")).strip().upper()
            if not tk:
                continue
            adv = dict(
                ev_s_mult=r.get("ev_s_mult"), ev_eb_mult=r.get("ev_eb_mult"), ev_fcf_mult=r.get("ev_fcf_mult"), dacf_mult=r.get("dacf_mult"),
                p_fcf_mult=r.get("p_fcf_mult"), p_nav_mult=r.get("p_nav_mult"), p_affo_mult=r.get("p_affo_mult"),
                p_b_mult=r.get("p_b_mult"), p_tbv_mult=r.get("p_tbv_mult"), p_nii_mult=r.get("p_nii_mult"),
                bv_ps0=r.get("BV/ps"), tbv_ps0=r.get("TBV/ps"), nav_ps0=r.get("NAV/ps"),
                affo_ps0=r.get("AFFO/ps"), fcf_ps0=r.get("FCF/ps"), nii_ps0=r.get("NII/ps")
            )
            try:
                handle_one_ticker_save(
                    tk,
                    r.get("Bucket","Bucket A tillväxt"),
                    int(nz(r.get("Antal aktier"), 0)),
                    r.get("Preferred metod","AUTO"),
                    float(nz(r.get("G1"),0.15)), float(nz(r.get("G2"),0.12)), float(nz(r.get("G3"),0.10)),
                    adv, use_finnhub, rate_limit_sleep
                )
            except Exception as e:
                st.warning(f"Misslyckades uppdatera {tk}: {e}")
            progress.progress((i+1)/n, text=f"Uppdaterar {i+1}/{n} …")
            time.sleep(rate_limit_sleep)
        st.success("Massuppdatering klar ✅")

# ============================================================
# Del 4/4 — Beräkningar, vyer, export (Resultat)
# ============================================================

# ------------ Hjälp för Resultat-upsert ------------
def ws_upsert_row(ws: gspread.Worksheet, key_col: str, row: Dict[str, Any]):
    # Läs befintligt
    records = ws.get_all_records(numericise_ignore=['all'])
    df = pd.DataFrame(records)
    # Säkerställ kolumn
    cols = set(df.columns) if not df.empty else set()
    for k in row.keys():
        if k not in cols:
            cols.add(k)
    cols = list(cols) if cols else list(row.keys())
    if df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan
        df = df[cols]

    # Upsert
    key = str(row.get(key_col, "")).strip()
    if not key:
        return
    mask = (df[key_col].astype(str).str.strip() == key)
    if mask.any():
        idx = df.index[mask][0]
        for k, v in row.items():
            if k in df.columns:
                df.at[idx, k] = v
    else:
        add = {c: "" for c in cols}
        add.update(row)
        df = pd.concat([df, pd.DataFrame([add])[cols]], ignore_index=True)

    # Skriv tillbaka
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())

def stringify_inputs(d: Dict[str, Any]) -> str:
    parts = []
    for k, v in d.items():
        try:
            if isinstance(v, float):
                parts.append(f"{k}={v:.4g}")
            else:
                parts.append(f"{k}={v}")
        except Exception:
            parts.append(f"{k}={v}")
    s = ";".join(parts)
    # hålla kort
    return (s[:990] + "…") if len(s) > 1000 else s

# ------------ Beräkning per rad ------------
def compute_methods_row(r: pd.Series,
                        use_finnhub: bool,
                        pe_comp_rate: float,
                        ev_comp_rate: float,
                        p_comp_rate: float,
                        rate_sleep: float) -> Dict[str, Any]:

    tkr = (r.get("Ticker") or "").upper()
    cur = (r.get("Valuta") or "SEK")
    px  = float(nz(r.get("Last Price"), 0.0))
    mc  = float(nz(r.get("Market Cap"), 0.0))
    shs = float(nz(r.get("Shares Out"), 0.0))
    if px <= 0 and mc > 0 and shs > 0:
        px = mc / shs

    # Bas
    rev0    = float(nz(r.get("Revenue TTM"), 0.0))
    ebitda0 = float(nz(r.get("EBITDA TTM"), 0.0))
    fcf0    = float(nz(r.get("FCF TTM"), 0.0))
    total_debt = float(nz(r.get("Total Debt"), nz(r.get("EV") - r.get("Market Cap"), 0.0)))
    cash       = float(nz(r.get("Cash"), 0.0))
    net_debt = total_debt - cash
    shares_out = shs if shs > 0 else safe_div(mc, px, 0.0)

    # Multiplar (manuella med ankare från Yahoo om saknas)
    ev_s_mult   = float(nz(r.get("ev_s_mult"), nz(r.get("EV/Revenue"), 5.0)))
    ev_eb_mult  = float(nz(r.get("ev_eb_mult"), nz(r.get("EV/EBITDA"), 12.0)))
    ev_fcf_mult = float(nz(r.get("ev_fcf_mult"), 18.0))
    dacf_mult   = float(nz(r.get("dacf_mult"), ev_eb_mult))

    p_fcf_mult  = float(nz(r.get("p_fcf_mult"), 20.0))
    p_nav_mult  = float(nz(r.get("p_nav_mult"), 1.0))
    p_affo_mult = float(nz(r.get("p_affo_mult"), 13.0))
    p_b_mult    = float(nz(r.get("p_b_mult"), 1.5))
    p_tbv_mult  = float(nz(r.get("p_tbv_mult"), 1.2))
    p_nii_mult  = float(nz(r.get("p_nii_mult"), 10.0))

    # Per-aktie proxies (kan vara 0 → då blir serie = 0)
    fcf_ps0  = float(nz(r.get("FCF/ps"), 0.0))
    nav_ps0  = float(nz(r.get("NAV/ps"), 0.0))
    affo_ps0 = float(nz(r.get("AFFO/ps"), 0.0))
    bv_ps0   = float(nz(r.get("BV/ps"), 0.0))
    tbv_ps0  = float(nz(r.get("TBV/ps"), 0.0))
    nii_ps0  = float(nz(r.get("NII/ps"), 0.0))

    # Tillväxt manuellt om inget estimat/CAGR
    g1 = float(nz(r.get("G1"), 0.15))
    g2 = float(nz(r.get("G2"), 0.12))
    g3 = float(nz(r.get("G3"), 0.10))

    # Estimat (Finnhub)
    est = {}
    pe_band = None
    pe_ttm_data = to_float(r.get("PE TTM"), None)
    pe_fwd_data = to_float(r.get("PE FWD"), None)
    try:
        if use_finnhub:
            est = fetch_finnhub_estimates(tkr) or {}
            time.sleep(rate_sleep)
            met = fetch_finnhub_metrics(tkr) or {}
            pe_band = met.get("pe_band")
            # om Finhubb har TTM-PE/PB, använd som sanity-ankare om våra saknas
            if pe_ttm_data is None:
                pe_ttm_data = to_float(met.get("pe_ttm"), None)
            time.sleep(rate_sleep)
    except Exception:
        pass

    eps_next1 = est.get("eps_next1")
    eps_next2 = est.get("eps_next2")
    rev_next1 = est.get("rev_next1")
    rev_next2 = est.get("rev_next2")

    # CAGR
    eps_cagr = to_float(r.get("EPS_CAGR_5Y"), None)
    if use_finnhub and eps_cagr is None:
        try:
            qeps = fetch_finnhub_eps_quarterly(tkr)
            time.sleep(rate_sleep)
            eps_cagr = eps_cagr_from_quarters(qeps, years=5) or eps_cagr_from_quarters(qeps, years=3)
        except Exception:
            pass

    rev_cagr = yahoo_revenue_cagr(tkr, years=5) or yahoo_revenue_cagr(tkr, years=3)

    # Konservativa proxy-CAGR för komponenter
    cagr_for_ebitda = rev_cagr if rev_cagr is not None else (eps_cagr if eps_cagr is not None else g1)
    if eps_cagr is not None and rev_cagr is not None:
        cagr_fcf = min(eps_cagr, rev_cagr)
    else:
        cagr_fcf = eps_cagr if eps_cagr is not None else (rev_cagr if rev_cagr is not None else g1)
    cagr_nav  = eps_cagr if eps_cagr is not None else (rev_cagr if rev_cagr is not None else g1)
    cagr_bv   = cagr_nav
    cagr_affo = cagr_nav
    cagr_nii  = cagr_nav

    # ---- P/E metod (estimat → EPS-CAGR fallback) med multipel-kompression ----
    eps_ttm_inferred = None
    if pe_ttm_data and pe_ttm_data > 0 and px and px > 0:
        eps_ttm_inferred = px / pe_ttm_data

    t0,t1,t2,t3,pe_note = pe_targets_from_estimates(
        price=px,
        pe_ttm=pe_ttm_data, pe_fwd=pe_fwd_data,
        pe_band=pe_band,
        eps_ttm=eps_ttm_inferred,
        eps_next1=eps_next1, eps_next2=eps_next2,
        eps_cagr=eps_cagr,
        pe_comp_rate=pe_comp_rate,
        g3=g3
    )

    vals: Dict[str, Tuple[float,float,float,float]] = {}
    vals["pe_hist_vs_eps"] = (t0,t1,t2,t3)

    # ---- EV/SALES (estimat → CAGR) + kompression ----
    rv0, rv1, rv2, rv3 = series_from_est_or_cagr(rev0, rev_next1, rev_next2, rev_cagr, g1, g2, g3)
    vals["ev_sales"]  = ev_multiple_from_series(rv0, rv1, rv2, rv3, ev_s_mult,  ev_comp_rate, net_debt, shares_out)

    # ---- EV/EBITDA ----
    eb0, eb1, eb2, eb3 = series_from_est_or_cagr(ebitda0, None, None, cagr_for_ebitda, g1, g2, g3)
    vals["ev_ebitda"] = ev_multiple_from_series(eb0, eb1, eb2, eb3, ev_eb_mult, ev_comp_rate, net_debt, shares_out)

    # ---- EV/DACF (proxas av EBITDA) ----
    dc0, dc1, dc2, dc3 = eb0, eb1, eb2, eb3
    vals["ev_dacf"]   = ev_multiple_from_series(dc0, dc1, dc2, dc3, dacf_mult,  ev_comp_rate, net_debt, shares_out)

    # ---- EV/FCF ----
    fc0, fc1, fc2, fc3 = series_from_est_or_cagr(fcf0, None, None, cagr_fcf, g1, g2, g3)
    vals["ev_fcf"]    = ev_multiple_from_series(fc0, fc1, fc2, fc3, ev_fcf_mult, ev_comp_rate, net_debt, shares_out)

    # ---- P/FCF per aktie ----
    pf0, pf1, pf2, pf3 = series_from_est_or_cagr(fcf_ps0, None, None, cagr_fcf, g1, g2, g3)
    vals["p_fcf"]     = price_multiple_from_series(pf0, pf1, pf2, pf3, p_fcf_mult, p_comp_rate)

    # ---- P/NAV per aktie ----
    nv0, nv1, nv2, nv3 = series_from_est_or_cagr(nav_ps0, None, None, cagr_nav, g1, g2, g3)
    vals["p_nav"]     = price_multiple_from_series(nv0, nv1, nv2, nv3, p_nav_mult, p_comp_rate)

    # ---- P/AFFO per aktie ----
    af0, af1, af2, af3 = series_from_est_or_cagr(affo_ps0, None, None, cagr_affo, g1, g2, g3)
    vals["p_affo"]    = price_multiple_from_series(af0, af1, af2, af3, p_affo_mult, p_comp_rate)

    # ---- P/B ----
    bv0, bv1, bv2, bv3 = series_from_est_or_cagr(bv_ps0, None, None, cagr_bv, g1, g2, g3)
    vals["p_b"]       = price_multiple_from_series(bv0, bv1, bv2, bv3, p_b_mult,   p_comp_rate)

    # ---- P/TBV (via ROTCE/payout om du senare lägger in) → här enkel CAGR-proxy via cagr_bv ----
    tb0, tb1, tb2, tb3 = series_from_est_or_cagr(tbv_ps0, None, None, cagr_bv, g1, g2, g3)
    vals["p_tbv"]     = price_multiple_from_series(tb0, tb1, tb2, tb3, p_tbv_mult, p_comp_rate)

    # ---- P/NII ----
    ni0, ni1, ni2, ni3 = series_from_est_or_cagr(nii_ps0, None, None, cagr_nii, g1, g2, g3)
    vals["p_nii"]     = price_multiple_from_series(ni0, ni1, ni2, ni3, p_nii_mult, p_comp_rate)

    # Primär metod
    has_fcf    = fcf0 > 0.0 or fcf_ps0 > 0.0
    has_ebitda = ebitda0 > 0.0
    primary = (r.get("Preferred metod") or "AUTO").strip().lower()
    if primary == "auto":
        primary = choose_primary_method(r.get("Bucket",""), "", "", tkr, has_fcf, has_ebitda)
    if primary not in vals:
        primary = "pe_hist_vs_eps"

    t_today, t_1y, t_2y, t_3y = vals[primary]
    b1, br1 = bull_bear(t_1y, bull_mult, bear_mult)

    # Dividendinfo (om Yahoo saknar → 0)
    div_ps = 0.0  # vi lagrar inte här; kan byggas ut
    da = 0.0
    if px > 0 and div_ps > 0:
        da = (div_ps/px)*100.0

    inputs = {
        "pe_ttm": pe_ttm_data, "pe_fwd": pe_fwd_data, "pe_band": pe_band,
        "eps_cagr": eps_cagr, "rev_cagr": rev_cagr,
        "g1": g1, "g2": g2, "g3": g3,
        "ev_s_mult": ev_s_mult, "ev_eb_mult": ev_eb_mult, "ev_fcf_mult": ev_fcf_mult, "dacf_mult": dacf_mult,
        "p_fcf_mult": p_fcf_mult, "p_nav_mult": p_nav_mult, "p_affo_mult": p_affo_mult,
        "p_b_mult": p_b_mult, "p_tbv_mult": p_tbv_mult, "p_nii_mult": p_nii_mult,
        "fcf_ps0": fcf_ps0, "nav_ps0": nav_ps0, "affo_ps0": affo_ps0, "bv_ps0": bv_ps0, "tbv_ps0": tbv_ps0, "nii_ps0": nii_ps0,
        "net_debt": net_debt, "shares_out": shares_out
    }

    return {
        "Ticker": tkr,
        "Namn": r.get("Bolagsnamn"),
        "Valuta": cur,
        "Pris": px,
        "Primär metod": primary,
        "Fair idag": t_today,
        "Fair 1y": t_1y,
        "Fair 2y": t_2y,
        "Fair 3y": t_3y,
        "Bull 1y": b1,
        "Bear 1y": br1,
        "DA_%": da,
        "Alla metoder": vals,
        "Inputs": inputs,
        "Note": f"pe_anchor({pe_note})"
    }

# ------------ Läs Data & filtrera ------------
data_df = read_data_df()
if data_df.empty:
    st.info("Lägg till minst ett bolag i fliken **Data** via formuläret ovan.")
    st.stop()

# Valutakurser för SEK-summering
fx_map = load_fx_from_sheet()
def _rate(ccy: str) -> float:
    return fx_to_sek(ccy or "SEK", fx_map)

# Filtrera
view_df = data_df.copy()
view_df = view_df[view_df["Bucket"].isin(pick_buckets)]
if only_owned:
    view_df = view_df[nz(view_df["Antal aktier"], 0) > 0]
if only_watch:
    view_df = view_df[nz(view_df["Antal aktier"], 0) == 0]

if view_df.empty:
    st.info("Inget att visa för valda filter.")
    st.stop()

# ------------ Beräkna ------------
calc_rows: List[Dict[str, Any]] = []
for _, rr in view_df.iterrows():
    try:
        calc_rows.append(
            compute_methods_row(rr, use_finnhub, pe_comp_rate, ev_comp_rate, p_comp_rate, rate_limit_sleep)
        )
        time.sleep(rate_limit_sleep)
    except Exception as e:
        st.warning(f"{rr.get('Ticker')}: beräkning misslyckades ({e}).")

calc_df = pd.DataFrame(calc_rows)

# SEK-summering och uppsida
def _upside(row) -> float:
    try:
        p = float(row["Pris"]); t = float(row["Fair idag"])
        return (t/p - 1.0)*100.0 if p > 0 else 0.0
    except Exception:
        return 0.0

calc_df["Upside_%"] = calc_df.apply(_upside, axis=1)

def _sek(x: float) -> str:
    try:
        return f"{x:,.0f} SEK".replace(",", " ")
    except Exception:
        return f"{x} SEK"

# SEK-portföljvärden
innehav_sek = []
utd_år_sek  = []
for _, r in view_df.iterrows():
    t = (r.get("Ticker") or "").upper()
    cur = (r.get("Valuta") or "SEK")
    pris = float(nz(r.get("Last Price"), 0.0))
    antal = int(nz(r.get("Antal aktier"), 0))
    rate = _rate(cur)
    innehav_sek.append(antal * pris * rate)
    # utd per år okänd här → 0 (kan utökas senare)
    utd_år_sek.append(0.0)

total_value_sek      = float(np.nansum(innehav_sek))
total_div_year_sek   = float(np.nansum(utd_år_sek))
total_div_month_sek  = total_div_year_sek / 12.0

# ------------ Vyer ------------
st.markdown("## 💼 Portföljsammanställning (SEK)")
c1,c2,c3 = st.columns(3)
c1.metric("Totalt portföljvärde", _sek(total_value_sek))
c2.metric("Total utdelning (12m)", _sek(total_div_year_sek))
c3.metric("Utdelning / månad", _sek(total_div_month_sek))

st.markdown("## 🧮 Rangordning (störst uppsida →)")
rank_cols = ["Ticker","Namn","Valuta","Pris","Primär metod","Fair idag","Fair 1y","Fair 2y","Fair 3y","Bull 1y","Bear 1y","Upside_%"]
st.dataframe(calc_df.sort_values("Upside_%", ascending=False)[rank_cols].reset_index(drop=True), use_container_width=True)

st.markdown("## 🔎 Detaljer per bolag (alla metoder)")
for _, r in calc_df.sort_values("Upside_%", ascending=False).iterrows():
    tk = r["Ticker"]
    with st.expander(f"{tk} • {r['Namn']} • {r['Valuta']} • Pris {fmt2(r['Pris'])}"):
        st.caption(r.get("Note",""))
        rows = []
        for m, (t0,t1,t2,t3) in r["Alla metoder"].items():
            rows.append([m, t0, t1, t2, t3])
        dfm = pd.DataFrame(rows, columns=["Metod","Idag","1 år","2 år","3 år"])
        st.dataframe(dfm, use_container_width=True)

# ------------ Export till fliken Resultat ------------
st.markdown("---")
if st.button("💾 Spara *primär metod* till fliken Resultat"):
    for _, r in calc_df.iterrows():
        tkr = r["Ticker"]
        vals = r["Alla metoder"].get(r["Primär metod"], (0.0,0.0,0.0,0.0))
        t0,t1,t2,t3 = vals
        b1, br1 = r["Bull 1y"], r["Bear 1y"]
        inp = stringify_inputs(r.get("Inputs", {}))
        row = {
            "Timestamp": now_ts(),
            "Ticker": tkr,
            "Valuta": r["Valuta"],
            "Aktuell kurs (0)": r["Pris"],
            "Riktkurs idag": t0,
            "Riktkurs 1 år": t1,
            "Riktkurs 2 år": t2,
            "Riktkurs 3 år": t3,
            "Bull 1 år": b1,
            "Bear 1 år": br1,
            "Metod": r["Primär metod"],
            "Input-sammanfattning": inp,
            "Kommentar": r.get("Note",""),
        }
        ws_upsert_row(WS_RESULT, "Ticker", row)
        time.sleep(rate_limit_sleep)
    st.success("Resultat uppdaterat ✅")
