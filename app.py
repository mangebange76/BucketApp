# app.py — Del 1/4
from __future__ import annotations

import os
import time
import math
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import numpy as np
import streamlit as st
import gspread
import yfinance as yf
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# =========================
# Grundinställningar
# =========================
st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")
APP_TITLE = "Aktieanalys & investeringsförslag"

DATA_TITLE = "Data"
FX_TITLE = "Valutakurser"        # valfritt, men reserverat namn
DECAY_DEFAULT = 0.10             # multipel-decay per år
PE_ANCHOR_WEIGHT = 0.50          # vikt av P/E-ankare (TTM-ankare-vikt)
CURRENCY_DEFAULT = "USD"

# Kolumner i Data-arket (minimalt men robust)
DATA_COLUMNS: List[str] = [
    "Ticker",
    "Bolagsnamn",
    "Valuta",
    # --- Körningsparametrar (kan vara tomma) ---
    "Primär metod",
    "EPS CAGR (%)",
    "Rev CAGR (%)",
    # --- Senast hämtade basnycklar (kan vara tomma) ---
    "Pris",
    "P/E TTM",
    "P/E FWD",
    "EV/Sales",
    "EV/EBITDA",
    "P/B",
    "BVPS",
    "EPS 1Y (estimat)",
]

# =========================
# Värderingsmetoder
# =========================
PRIMARY_METHOD_CHOICES = [
    "pe_hist_vs_eps", "ev_ebitda", "ev_sales", "p_b",
    "p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"
]
PRIMARY_METHOD_LABELS = {
    "pe_hist_vs_eps": "P/E × EPS (ankare/decay)",
    "ev_ebitda": "EV/EBITDA",
    "ev_sales": "EV/Sales",
    "p_b": "P/B",
    "p_nav": "P/NAV",
    "p_tbv": "P/TBV",
    "p_affo": "P/AFFO",
    "p_fcf": "P/FCF",
    "ev_fcf": "EV/FCF",
    "p_nii": "P/NII",
}

# =========================
# Hjälpare: Google Sheets
# =========================
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _get_spreadsheet_id() -> str:
    """
    Hämtar Spreadsheet ID från st.secrets eller miljö.
    Stödjer både ren ID och full URL.
    """
    sid = st.secrets.get("SPREADSHEET_ID", os.getenv("SPREADSHEET_ID", "")).strip()
    if not sid:
        url = st.secrets.get("SPREADSHEET_URL", os.getenv("SPREADSHEET_URL", "")).strip()
        if url:
            # plocka ID från URL
            try:
                sid = url.split("/d/")[1].split("/")[0]
            except Exception:
                sid = ""
    if not sid:
        raise RuntimeError("SPREADSHEET_ID saknas. Lägg till i secrets eller env.")
    return sid

def _open_spreadsheet() -> Spreadsheet:
    raw = st.secrets.get("GOOGLE_CREDENTIALS", None)
    if raw is None:
        raise RuntimeError("GOOGLE_CREDENTIALS saknas i secrets.")
    # st.secrets returnerar redan ett dict i Streamlit Cloud
    if isinstance(raw, str):
        import json
        creds_dict = json.loads(raw)
    else:
        creds_dict = dict(raw)
    creds_dict = _normalize_private_key(creds_dict)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    client = gspread.authorize(creds)
    sid = _get_spreadsheet_id()
    return client.open_by_key(sid)

def _with_backoff(callable_fn, *args, retries: int = 5, **kwargs):
    """
    Enkel exponential backoff för Google-API:er (429 m.m.).
    """
    delay = 1.0
    for i in range(retries):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            # 429 eller liknande: backoff
            if i == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2.0

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    try:
        return _with_backoff(spread.worksheet, title)
    except APIError:
        # Skapa om den inte finns
        _with_backoff(spread.add_worksheet, title=title, rows=1000, cols=50)
        return _with_backoff(spread.worksheet, title)

@st.cache_data(ttl=60)
def _read_df(title: str) -> pd.DataFrame:
    sh = _open_spreadsheet()
    ws = _get_ws(sh, title)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return pd.DataFrame(columns=DATA_COLUMNS)
    df = pd.DataFrame(values[1:], columns=values[0])
    # säkerställ alla kolumner finns
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    return df[DATA_COLUMNS].copy()

def _write_df(title: str, df: pd.DataFrame):
    sh = _open_spreadsheet()
    ws = _get_ws(sh, title)

    # säkerställ kolumner
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    df = df[DATA_COLUMNS].copy()

    data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    _with_backoff(ws.clear)
    _with_backoff(ws.update, "A1", data)
    try:
        st.cache_data.clear()
    except Exception:
        pass

def _ensure_sheet_schema():
    """
    Skapar Data-arket om det saknas och säkrar kolumnuppsättningen.
    """
    df = _read_df(DATA_TITLE)
    changed = False
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
            changed = True
    # sortera kolumner enligt specifikation
    df = df[DATA_COLUMNS]
    if changed or df.empty:
        _write_df(DATA_TITLE, df)

# =========================
# Hjälpare: Primär metod
# =========================
def _default_primary_method(
    selected_ticker: str,
    methods_df: pd.DataFrame,
    df_row: Optional[pd.Series],
) -> Optional[str]:
    """
    Välj förval från Data['Primär metod'] om giltig, annars smart fallback.
    """
    available = set(methods_df["Metod"].dropna().astype(str))
    m = None
    if df_row is not None:
        m = str(df_row.get("Primär metod", "")).strip()
    if m in available:
        return m
    for cand in PRIMARY_METHOD_CHOICES:
        if cand in available:
            return cand
    return next(iter(available), None)

def _save_primary_method_to_sheet(ticker: str, method: str):
    """
    Uppdaterar kolumnen 'Primär metod' för den rad som matchar Ticker.
    """
    sh = _open_spreadsheet()
    ws = _get_ws(sh, DATA_TITLE)

    header = _with_backoff(ws.row_values, 1)
    col_map = {h: i+1 for i, h in enumerate(header)}
    if "Ticker" not in col_map or "Primär metod" not in col_map:
        raise RuntimeError("Saknar kolumn 'Ticker' eller 'Primär metod' i Data.")

    col_t, col_m = col_map["Ticker"], col_map["Primär metod"]
    tickers = _with_backoff(ws.col_values, col_t)

    row_idx = None
    for i, val in enumerate(tickers, start=1):
        if i == 1:
            continue
        if str(val).strip().upper() == str(ticker).strip().upper():
            row_idx = i
            break
    if row_idx is None:
        raise RuntimeError(f"Hittar inte '{ticker}' i Data.")

    _with_backoff(ws.update_cell, row_idx, col_m, method)
    try:
        st.cache_data.clear()
    except Exception:
        pass

# app.py — Del 2/4
import json
import requests

# =========================
# Numeriska hjälpare
# =========================
def _to_float(x) -> Optional[float]:
    try:
        if x is None: 
            return None
        if isinstance(x, (int, float)) and math.isfinite(float(x)):
            return float(x)
        s = str(x).strip().replace(" ", "").replace(",", ".")
        if s == "" or s.lower() in {"nan", "none"}:
            return None
        v = float(s)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _pos(x) -> Optional[float]:
    v = _to_float(x)
    return v if (v is not None and v > 0) else None

def _nz(x, fallback=None):
    return x if (x is not None and x == x) else fallback

# =========================
# Multipel-ankare & decay
# =========================
def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float = PE_ANCHOR_WEIGHT) -> Optional[float]:
    pt = _pos(pe_ttm)
    pf = _pos(pe_fwd)
    if pt is None and pf is None:
        return None
    if pt is None:
        return pf
    if pf is None:
        return pt
    return w_ttm * pt + (1.0 - w_ttm) * pf

def _decay_linear(mult0: Optional[float], years: int, decay: float = DECAY_DEFAULT, floor_ratio: float = 0.60) -> Optional[float]:
    m0 = _pos(mult0)
    if m0 is None:
        return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_ratio
    return max(m, floor)

# =========================
# Bygg pris från olika ankare
# =========================
def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e = _pos(eps)
    p = _pos(pe)
    if e is None or p is None:
        return None
    return e * p

def _ev_from_sales(rev: Optional[float], mult: Optional[float]) -> Optional[float]:
    r = _pos(rev); m = _pos(mult)
    if r is None or m is None: return None
    return r * m

def _ev_from_ebitda(ebitda: Optional[float], mult: Optional[float]) -> Optional[float]:
    e = _pos(ebitda); m = _pos(mult)
    if e is None or m is None: return None
    return e * m

def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float], shares_fd: Optional[float]) -> Optional[float]:
    e = _pos(ev_target); s = _pos(shares_fd)
    if e is None or s is None: return None
    nd = _to_float(net_debt) or 0.0
    return max(0.0, (e - nd) / s)

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb); b = _pos(bvps)
    if p is None or b is None: return None
    return p * b

# =========================
# Datakällor
# =========================
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar pris, valuta och centrala nyckeltal via yfinance.
    Returnerar:
      {
        'values': {... nycklar -> float/str ...},
        'source': {... nycklar -> 'yahoo'/'derived' ...}
      }
    """
    out_v: Dict[str, Any] = {}
    out_s: Dict[str, str] = {}
    try:
        tk = yf.Ticker(ticker)
    except Exception:
        return {"values": out_v, "source": out_s}

    # Snabbkanal
    try:
        fi = tk.fast_info
        val = _to_float(getattr(fi, "last_price", None))
        if val is not None:
            out_v["price"] = val; out_s["price"] = "yahoo"
        ccy = getattr(fi, "currency", None)
        if ccy:
            out_v["currency"] = str(ccy).upper(); out_s["currency"] = "yahoo"
        mc = _to_float(getattr(fi, "market_cap", None))
        if mc is not None:
            out_v["market_cap"] = mc; out_s["market_cap"] = "yahoo"
        sh = _to_float(getattr(fi, "shares", None))
        if sh is not None:
            out_v["shares"] = sh; out_s["shares"] = "yahoo"
    except Exception:
        pass

    # Långkanal (info)
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k):
        try:
            return info.get(k)
        except Exception:
            return None

    # Pris/valuta fallback
    if "price" not in out_v:
        cp = _to_float(gi("currentPrice"))
        if cp is not None:
            out_v["price"] = cp; out_s["price"] = "yahoo"
    if "currency" not in out_v:
        cu = gi("currency")
        if cu:
            out_v["currency"] = str(cu).upper(); out_s["currency"] = "yahoo"

    # Basnycklar
    mcap = _to_float(gi("marketCap"))
    if "market_cap" not in out_v and mcap is not None:
        out_v["market_cap"] = mcap; out_s["market_cap"] = "yahoo"

    ev = _to_float(gi("enterpriseValue"))
    td = _to_float(gi("totalDebt"))
    tc = _to_float(gi("totalCash"))

    eps_ttm = _to_float(gi("trailingEps"))
    pe_ttm  = _to_float(gi("trailingPE"))
    pe_fwd  = _to_float(gi("forwardPE"))

    rev_ttm   = _to_float(gi("totalRevenue"))
    ebitda_t  = _to_float(gi("ebitda"))
    ev_sales  = _to_float(gi("enterpriseToRevenue"))
    ev_ebitda = _to_float(gi("enterpriseToEbitda"))
    p_to_book = _to_float(gi("priceToBook"))
    bvps      = _to_float(gi("bookValue"))

    # Spara och markera källor
    for k, v in [
        ("eps_ttm", eps_ttm),
        ("pe_ttm",  pe_ttm),
        ("pe_fwd",  pe_fwd),
        ("revenue_ttm", rev_ttm),
        ("ebitda_ttm", ebitda_t),
        ("ev_to_sales", ev_sales),
        ("ev_to_ebitda", ev_ebitda),
        ("p_to_book", p_to_book),
        ("bvps", bvps),
    ]:
        if v is not None:
            out_v[k] = v; out_s[k] = "yahoo"

    # EV & nettoskuld
    if ev is not None:
        out_v["ev"] = ev; out_s["ev"] = "yahoo"
    elif _pos(out_v.get("market_cap")) and _pos(td) is not None and _pos(tc) is not None:
        try:
            out_v["ev"] = out_v["market_cap"] + td - tc; out_s["ev"] = "derived"
        except Exception:
            pass

    if out_v.get("market_cap") is not None and out_v.get("ev") is not None:
        out_v["net_debt"] = out_v["ev"] - out_v["market_cap"]; out_s["net_debt"] = "derived"

    # Shares fallback (MCAP/price)
    if "shares" not in out_v and _pos(out_v.get("market_cap")) and _pos(out_v.get("price")):
        try:
            out_v["shares"] = out_v["market_cap"] / out_v["price"]; out_s["shares"] = "derived"
        except Exception:
            pass

    # Normalisera valuta
    out_v["currency"] = str(out_v.get("currency", CURRENCY_DEFAULT)).upper()
    return {"values": out_v, "source": out_s}

def _get_finnhub_key() -> Optional[str]:
    return (
        st.secrets.get("FINNHUB_API_KEY", None)
        or os.getenv("FINNHUB_API_KEY")
        or st.secrets.get("FINNHUB_TOKEN", None)
        or os.getenv("FINNHUB_TOKEN")
    )

def fetch_finnhub_estimates(ticker: str) -> Dict[str, Any]:
    """
    Försöker hämta EPS-estimat (1–2 år) från Finnhub.
    Returnerar: {'eps_1y': float|None, 'eps_2y': float|None, 'source': 'finnhub'|'none'}
    """
    key = _get_finnhub_key()
    if not key:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

    try:
        url = f"https://finnhub.io/api/v1/stock/estimate?symbol={ticker}&token={key}"
        r = requests.get(url, timeout=10)
        if not r.ok:
            return {"eps_1y": None, "eps_2y": None, "source": "none"}
        js = r.json()
        rows = js if isinstance(js, list) else js.get("data", [])
        rows = rows or []
        # sortera på period
        rows = sorted(rows, key=lambda x: str(x.get("period", "")))
        vals = [_to_float(x.get("epsAvg")) for x in rows if _to_float(x.get("epsAvg")) is not None]
        eps_1y = vals[-1] if vals else None
        eps_2y = vals[-2] if len(vals) > 1 else None
        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "source": "finnhub"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

# =========================
# EPS/REV/EBITDA-banor (prognos)
# =========================
def _derive_eps_if_missing(price: Optional[float], pe_ttm: Optional[float], pe_fwd: Optional[float],
                           eps_ttm: Optional[float], eps_1y: Optional[float],
                           sources: Dict[str, str]) -> Tuple[Optional[float], Optional[float]]:
    # EPS TTM från price / PE TTM
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        sources["eps_ttm"] = "derived"
    # EPS 1Y från price / PE FWD (om ingen Finnhub)
    if eps_1y is None and _pos(price) and _pos(pe_fwd):
        eps_1y = price / pe_fwd
        sources["eps_1y"] = "derived"
    return eps_ttm, eps_1y

def _eps_path(eps_ttm: Optional[float], eps_1y: Optional[float], eps_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(eps_ttm)
    e1 = _pos(eps_1y)
    g  = _to_float(eps_cagr)
    if g is not None and g > 2.0:  # skydda mot felmatade % (t.ex. 25 istället för 0.25)
        g = g / 100.0
    if e1 is None and e0 is not None and g is not None:
        e1 = e0 * (1.0 + g)
    e2 = e1 * (1.0 + (g or 0.0)) if e1 is not None else None
    e3 = e2 * (1.0 + (g or 0.0)) if e2 is not None else None
    return e0, e1, e2, e3

def _rev_path(rev_ttm: Optional[float], rev_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    r0 = _pos(rev_ttm)
    g  = _to_float(rev_cagr)
    if g is not None and g > 2.0:
        g = g / 100.0
    if r0 is None or g is None:
        return r0, None, None, None
    r1 = r0 * (1.0 + g)
    r2 = r1 * (1.0 + g)
    r3 = r2 * (1.0 + g)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(ebitda_ttm)
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return e0, e0, e0, e0
    # proxy: skala med omsättningens förändring
    def scale(r): 
        try:
            return (e0 * (r / rev0)) if (r and rev0) else e0
        except Exception:
            return e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# =========================
# Beräkningsmotor per rad
# =========================
def compute_methods_for_row(row: pd.Series) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Tar en Data-rad och producerar:
      - methods_df: tabell med Metod | Idag | 1 år | 2 år | 3 år
      - meta: { 'ticker', 'currency', 'price', 'pe_anchor', 'decay',
                'shares_out', 'net_debt', 'sources': {key -> 'yahoo'/'finnhub'/'derived'/'manual'} }
    """
    ticker = str(row.get("Ticker", "")).strip().upper()
    if not ticker:
        return pd.DataFrame(columns=["Metod","Idag","1 år","2 år","3 år"]), {}

    # 1) Livehämtning
    ya = fetch_yahoo_snapshot(ticker)
    yv, ys = ya.get("values", {}), ya.get("source", {})
    time.sleep(0.35)  # mild throttling
    fh = fetch_finnhub_estimates(ticker)

    sources: Dict[str, str] = {}

    # 2) Inputs (fallback: Data-bladet)
    price    = _pos(_nz(yv.get("price"), row.get("Pris")));              sources["price"] = ys.get("price","manual" if row.get("Pris") else "—")
    currency = str(_nz(yv.get("currency"), row.get("Valuta") or CURRENCY_DEFAULT)).upper(); sources["currency"] = ys.get("currency","manual" if row.get("Valuta") else "—")

    shares   = _pos(_nz(yv.get("shares"), row.get("Utestående aktier"))); sources["shares"] = ys.get("shares","manual" if row.get("Utestående aktier") else "—")
    net_debt = _nz(yv.get("net_debt"), row.get("Net debt"));              sources["net_debt"] = ys.get("net_debt","manual" if row.get("Net debt") else "—")

    eps_ttm  = _pos(_nz(yv.get("eps_ttm"), row.get("EPS TTM")));          sources["eps_ttm"] = ys.get("eps_ttm","manual" if row.get("EPS TTM") else "—")
    pe_ttm   = _pos(_nz(yv.get("pe_ttm"), row.get("P/E TTM")));           sources["pe_ttm"]  = ys.get("pe_ttm","manual" if row.get("P/E TTM") else "—")
    pe_fwd   = _pos(_nz(yv.get("pe_fwd"), row.get("P/E FWD")));           sources["pe_fwd"]  = ys.get("pe_fwd","manual" if row.get("P/E FWD") else "—")

    rev_ttm  = _pos(_nz(yv.get("revenue_ttm"), row.get("Rev TTM")));      sources["rev_ttm"] = ys.get("revenue_ttm","manual" if row.get("Rev TTM") else "—")
    ebitda_t = _pos(_nz(yv.get("ebitda_ttm"), row.get("EBITDA TTM")));    sources["ebitda_ttm"] = ys.get("ebitda_ttm","manual" if row.get("EBITDA TTM") else "—")

    ev_sales_val  = _pos(_nz(yv.get("ev_to_sales"), row.get("EV/Sales")));     sources["ev_to_sales"]  = ys.get("ev_to_sales","manual" if row.get("EV/Sales") else "—")
    ev_ebitda_val = _pos(_nz(yv.get("ev_to_ebitda"), row.get("EV/EBITDA")));   sources["ev_to_ebitda"] = ys.get("ev_to_ebitda","manual" if row.get("EV/EBITDA") else "—")
    pb_val        = _pos(_nz(yv.get("p_to_book"), row.get("P/B")));            sources["p_to_book"]    = ys.get("p_to_book","manual" if row.get("P/B") else "—")
    bvps          = _pos(_nz(yv.get("bvps"), row.get("BVPS")));                sources["bvps"]         = ys.get("bvps","manual" if row.get("BVPS") else "—")

    # Estimat & CAGR
    eps1y_data = _pos(_nz(row.get("EPS 1Y (estimat)"), None))
    if _pos(fh.get("eps_1y")):
        eps_1y = _pos(fh.get("eps_1y")); sources["eps_1y"] = "finnhub"
    else:
        eps_1y = eps1y_data; 
        if eps1y_data is not None: sources["eps_1y"] = "manual"

    eps_2y = _pos(fh.get("eps_2y")) if _pos(fh.get("eps_2y")) else None
    if eps_2y is not None:
        sources["eps_2y"] = "finnhub"

    eps_cagr = _to_float(row.get("EPS CAGR (%)"))
    rev_cagr = _to_float(row.get("Rev CAGR (%)"))
    if eps_cagr is not None: sources["eps_cagr"] = "manual"
    if rev_cagr is not None: sources["rev_cagr"] = "manual"

    # 3) Härled EPS om saknas
    eps_ttm, eps_1y = _derive_eps_if_missing(price, pe_ttm, pe_fwd, eps_ttm, eps_1y, sources)

    # Om EPS CAGR saknas men vi har EPS TTM & 1Y → bakåthärled enkel CAGR
    if eps_cagr is None and _pos(eps_ttm) and _pos(eps_1y):
        try:
            eps_cagr = (eps_1y / eps_ttm) - 1.0
            sources["eps_cagr"] = "derived"
        except Exception:
            pass

    # 4) PE-ankare & decay
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, PE_ANCHOR_WEIGHT)
    decay = DECAY_DEFAULT

    # 5) Paths
    e0, e1, e2e, e3e = _eps_path(eps_ttm, eps_1y, eps_cagr)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(ebitda_t, r0, r1, r2, r3)

    # Om EPS_2Y kom från Finnhub, skriv över e2e
    if eps_2y is not None:
        e2e = eps_2y

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_linear(pe_anchor, 1, decay)
    pe2m = _decay_linear(pe_anchor, 2, decay)
    pe3m = _decay_linear(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = (
        ev_sales_val, _decay_linear(ev_sales_val, 1, decay),
        _decay_linear(ev_sales_val, 2, decay), _decay_linear(ev_sales_val, 3, decay)
    )
    eve0, eve1, eve2, eve3 = (
        ev_ebitda_val, _decay_linear(ev_ebitda_val, 1, decay),
        _decay_linear(ev_ebitda_val, 2, decay), _decay_linear(ev_ebitda_val, 3, decay)
    )
    pb0, pb1, pb2, pb3 = (
        pb_val, _decay_linear(pb_val, 1, decay),
        _decay_linear(pb_val, 2, decay), _decay_linear(pb_val, 3, decay)
    )

    # 6) Riktkurser per metod
    methods = []

    # P/E × EPS
    methods.append({
        "Metod": "pe_hist_vs_eps",
        "Idag": _price_from_pe(e0, pe0),
        "1 år": _price_from_pe(e1, pe1m),
        "2 år": _price_from_pe(e2e, pe2m),
        "3 år": _price_from_pe(e3e, pe3m),
    })

    # EV/Sales → EV → Equity value / aktie
    methods.append({
        "Metod": "ev_sales",
        "Idag": _equity_price_from_ev(_ev_from_sales(r0, evs0), net_debt, shares),
        "1 år": _equity_price_from_ev(_ev_from_sales(r1, evs1), net_debt, shares),
        "2 år": _equity_price_from_ev(_ev_from_sales(r2, evs2), net_debt, shares),
        "3 år": _equity_price_from_ev(_ev_from_sales(r3, evs3), net_debt, shares),
    })

    # EV/EBITDA
    methods.append({
        "Metod": "ev_ebitda",
        "Idag": _equity_price_from_ev(_ev_from_ebitda(b0, eve0), net_debt, shares),
        "1 år": _equity_price_from_ev(_ev_from_ebitda(b1, eve1), net_debt, shares),
        "2 år": _equity_price_from_ev(_ev_from_ebitda(b2, eve2), net_debt, shares),
        "3 år": _equity_price_from_ev(_ev_from_ebitda(b3, eve3), net_debt, shares),
    })

    # P/B
    methods.append({
        "Metod": "p_b",
        "Idag": _price_from_pb(pb0, bvps),
        "1 år": _price_from_pb(pb1, bvps),
        "2 år": _price_from_pb(pb2, bvps),
        "3 år": _price_from_pb(pb3, bvps),
    })

    # Platshållare för metoder som kräver per-aktie-tal som ofta saknas automatiskt
    for m in ("p_nav","p_tbv","p_affo","p_fcf","ev_fcf","p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    meta = {
        "ticker": ticker,
        "currency": currency,
        "price": price,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "shares_out": shares,
        "net_debt": net_debt,
        "sources": sources,  # <- visar vilka fält kom från yahoo/finnhub/derived/manual
    }
    return methods_df, meta

# app.py — Del 3/4

# =========================
# Hjälpare för metodval
# =========================
_PREFER_ORDER = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]

def _first_nonempty_method(methods_df: pd.DataFrame) -> Optional[str]:
    if methods_df is None or methods_df.empty:
        return None
    counts = methods_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None
    maxc = counts.max()
    candidates = [m for m in _PREFER_ORDER if m in counts.index and counts[m] == maxc]
    return candidates[0] if candidates else counts.index[counts == maxc].tolist()[0]

def _get_method_row(methods_df: pd.DataFrame, method: str) -> Optional[pd.Series]:
    if methods_df is None or methods_df.empty:
        return None
    m = methods_df[methods_df["Metod"] == method]
    return m.iloc[0] if not m.empty else None

def _calc_upside(price_now: Optional[float], target: Optional[float]) -> Optional[float]:
    pn = _pos(price_now); tg = _pos(target)
    if pn is None or tg is None:
        return None
    return (tg/pn - 1.0) * 100.0

# =========================
# Bygg “värden + källor” för presentation
# (vi använder samma fetchers som motorn)
# =========================
def _build_values_and_sources_for_display(row: pd.Series) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Returnerar (values, sources) för tydlig presentation."""
    ticker = str(row.get("Ticker","")).strip().upper()
    ya = fetch_yahoo_snapshot(ticker)
    yv, ys = ya.get("values", {}), ya.get("source", {})
    fh = fetch_finnhub_estimates(ticker)

    values: Dict[str, Any] = {}
    src: Dict[str, str]    = {}

    # Bas
    values["price"]    = _pos(_nz(yv.get("price"), row.get("Aktuell kurs")))
    src["price"]       = ys.get("price","manual" if row.get("Aktuell kurs") else "—")
    values["currency"] = str(_nz(yv.get("currency"), row.get("Valuta") or CURRENCY_DEFAULT)).upper()
    src["currency"]    = ys.get("currency","manual" if row.get("Valuta") else "—")
    values["shares"]   = _pos(_nz(yv.get("shares"), row.get("Utestående aktier")))
    src["shares"]      = ys.get("shares","manual" if row.get("Utestående aktier") else "—")
    values["net_debt"] = _nz(yv.get("net_debt"), row.get("Net debt"))
    src["net_debt"]    = ys.get("net_debt","manual" if row.get("Net debt") else "—")

    # Resultaträkning/nycklar
    mapping = [
        ("eps_ttm",      "EPS TTM",      "eps_ttm"),
        ("pe_ttm",       "PE TTM",       "pe_ttm"),
        ("pe_fwd",       "PE FWD",       "pe_fwd"),
        ("revenue_ttm",  "Rev TTM",      "revenue_ttm"),
        ("ebitda_ttm",   "EBITDA TTM",   "ebitda_ttm"),
        ("ev_to_sales",  "EV/Revenue",   "ev_to_sales"),
        ("ev_to_ebitda", "EV/EBITDA",    "ev_to_ebitda"),
        ("p_to_book",    "P/B",          "p_to_book"),
        ("bvps",         "BVPS",         "bvps"),
    ]
    for key_sheet, col_sheet, key_yv in mapping:
        values[key_sheet] = _pos(_nz(yv.get(key_yv), row.get(col_sheet)))
        src[key_sheet]    = ys.get(key_yv,"manual" if row.get(col_sheet) is not None else "—")

    # Estimat & CAGR
    eps1y_manual = _pos(row.get("EPS 1Y"))
    eps1y_api = _pos(fh.get("eps_1y"))
    values["eps_1y"] = eps1y_api if eps1y_api is not None else eps1y_manual
    src["eps_1y"]    = "finnhub" if eps1y_api is not None else ("manual" if eps1y_manual is not None else "—")

    eps2y_api = _pos(fh.get("eps_2y"))
    values["eps_2y"] = eps2y_api
    src["eps_2y"]    = "finnhub" if eps2y_api is not None else "—"

    # Härled EPS om saknas
    if values.get("eps_ttm") is None and _pos(values.get("price")) and _pos(values.get("pe_ttm")):
        try:
            values["eps_ttm"] = values["price"] / values["pe_ttm"]
            src["eps_ttm"] = "derived"
        except Exception:
            pass
    if values.get("eps_1y") is None and _pos(values.get("price")) and _pos(values.get("pe_fwd")):
        try:
            values["eps_1y"] = values["price"] / values["pe_fwd"]
            src["eps_1y"] = "derived"
        except Exception:
            pass

    # CAGR (procent → andel)
    def _to_rate(x):
        r = _to_float(x)
        if r is None:
            return None
        return r/100.0 if r > 2.0 else r

    values["eps_cagr"] = _to_rate(row.get("EPS CAGR"))
    values["rev_cagr"] = _to_rate(row.get("Rev CAGR"))
    src["eps_cagr"]    = "manual" if row.get("EPS CAGR") not in [None, ""] else "—"
    src["rev_cagr"]    = "manual" if row.get("Rev CAGR") not in [None, ""] else "—"

    # Om eps_cagr saknas men vi har eps_ttm + eps_1y, härled
    if values.get("eps_cagr") is None and _pos(values.get("eps_ttm")) and _pos(values.get("eps_1y")):
        try:
            values["eps_cagr"] = values["eps_1y"] / values["eps_ttm"] - 1.0
            src["eps_cagr"] = "derived"
        except Exception:
            pass

    return values, src

def _fmt_val(v) -> str:
    if v is None: 
        return "–"
    try:
        if isinstance(v, float) and (abs(v) >= 1000.0):
            return f"{v:,.0f}".replace(",", " ").replace(".", ",")
        if isinstance(v, float):
            return f"{v:,.2f}".replace(",", " ").replace(".", ",")
        return str(v)
    except Exception:
        return str(v)

# =========================
# Spara primär metod till Data
# =========================
def _save_primary_method_to_data(ticker: str, method: str) -> None:
    df = read_data_df()
    if df.empty or "Ticker" not in df.columns:
        st.warning("Kunde inte hitta Data-bladet.")
        return
    mask = df["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if not mask.any():
        st.warning(f"Hittade inte {ticker} i Data.")
        return
    if "Primär metod" not in df.columns:
        df["Primär metod"] = np.nan
    df.loc[mask, "Primär metod"] = method
    write_data_df(df)
    st.success(f"Primär metod sparad för {ticker}: **{method}**")

# =========================
# Bolagspresentation (Analys)
# =========================
def _render_company_card(row: pd.Series, idx: int, total: int) -> Tuple[str, Optional[float]]:
    """Renderar ett bolag, returnerar (vald_metod, fair_today)."""
    tkr = str(row.get("Ticker","")).strip().upper()
    name = str(_nz(row.get("Bolagsnamn"), tkr))
    st.subheader(f"{tkr} • {name}  \n_{idx+1} / {total}_")

    # Kör motorn
    methods_df, meta = compute_methods_for_row(row)
    price_now   = meta.get("price")
    currency    = meta.get("currency", CURRENCY_DEFAULT)

    # Standardmetod = radens Primär metod (om giltig), annars heuristik
    row_primary = str(_nz(row.get("Primär metod"), "")).strip()
    if row_primary and row_primary in methods_df["Metod"].values:
        default_method = row_primary
    else:
        default_method = _first_nonempty_method(methods_df)

    # Valbar primär metod i Analys
    method_choice = st.selectbox(
        "Primär värderingsmetod",
        _PREFER_ORDER,
        index=_PREFER_ORDER.index(default_method) if default_method in _PREFER_ORDER else 0,
        key=f"method_{tkr}"
    )
    mr = _get_method_row(methods_df, method_choice)
    p0 = _f(mr["Idag"]) if mr is not None else None
    p1 = _f(mr["1 år"]) if mr is not None else None
    p2 = _f(mr["2 år"]) if mr is not None else None
    p3 = _f(mr["3 år"]) if mr is not None else None

    # Metrikrutor
    c = st.columns(5)
    c[0].metric("Pris", _fmt_money(price_now, currency))
    c[1].metric("Idag (FV)", _fmt_money(p0, currency))
    c[2].metric("1 år", _fmt_money(p1, currency))
    c[3].metric("2 år", _fmt_money(p2, currency))
    c[4].metric("3 år", _fmt_money(p3, currency))

    # Uppsidor
    cu = st.columns(4)
    for i, (lbl, target) in enumerate([("Idag", p0), ("1 år", p1), ("2 år", p2), ("3 år", p3)]):
        up = _calc_upside(price_now, target)
        cu[i].metric(f"Uppsida {lbl}", f"{up:,.1f} %".replace(",", " ")) if up is not None else cu[i].metric(f"Uppsida {lbl}", "–")

    # Visa metodtabellen
    st.dataframe(methods_df, use_container_width=True)

    # KÄLLOR & VÄRDEN
    with st.expander("🔎 Värden som används i beräkningarna (med källor)", expanded=False):
        vals, srcs = _build_values_and_sources_for_display(row)
        disp_rows = []
        pretty = [
            ("price","Pris"),("currency","Valuta"),("shares","Utestående aktier"),("net_debt","Net debt"),
            ("eps_ttm","EPS TTM"),("eps_1y","EPS 1Y"),("eps_2y","EPS 2Y"),
            ("pe_ttm","P/E TTM"),("pe_fwd","P/E FWD"),
            ("revenue_ttm","Rev TTM"),("ebitda_ttm","EBITDA TTM"),
            ("ev_to_sales","EV/Sales"),("ev_to_ebitda","EV/EBITDA"),
            ("p_to_book","P/B"),("bvps","BVPS"),
            ("eps_cagr","EPS CAGR"),("rev_cagr","Rev CAGR")
        ]
        for k, title in pretty:
            disp_rows.append({"Nyckel": title, "Värde": _fmt_val(vals.get(k)), "Källa": srcs.get(k, "—")})
        st.dataframe(pd.DataFrame(disp_rows), use_container_width=True)

    # Åtgärder
    ac = st.columns(3)
    if ac[0].button("💾 Spara primär metod → Data", key=f"save_pm_{tkr}"):
        _save_primary_method_to_data(tkr, method_choice)

    if ac[1].button("📝 Spara riktkurser → Resultat", key=f"save_targets_{tkr}"):
        _append_or_update_result(
            ticker=tkr, currency=currency, method=method_choice,
            today=p0, y1=p1, y2=p2, y3=p3
        )
        st.success("Riktkurser sparade till fliken Resultat.")

    # Liten sammanfattning
    st.caption(f"Ankare P/E: {round(_f(meta.get('pe_anchor')) or 0, 2)} • Decay/år: {DECAY_DEFAULT} • Metod: **{method_choice}**")
    return method_choice, p0

# =========================
# Analys – bläddringsvy (störst uppsida först)
# =========================
def page_analysis():
    st.header("🔬 Analys – ett bolag i taget")

    settings = get_settings_map()
    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    # Filter & sortering
    c1, c2 = st.columns(2)
    buckets = c1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned   = c2.selectbox("Urval", ["Alla", "Endast innehav (>0)"], index=0)

    q = df.copy()
    if buckets: q = q[q["Bucket"].isin(buckets)]
    if owned.startswith("Endast"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]

    if q.empty:
        st.warning("Inget matchar filtret.")
        return

    # Bygg ranking på uppsida (FV 'Idag' mot pris) via vald/heuristisk metod
    rank_rows = []
    prog = st.progress(0.0)
    for i, (_, row) in enumerate(q.iterrows()):
        try:
            methods_df, meta = compute_methods_for_row(row)
            # välj "standard" metod
            row_primary = str(_nz(row.get("Primär metod"), "")).strip()
            if row_primary and row_primary in methods_df["Metod"].values:
                m = row_primary
            else:
                m = _first_nonempty_method(methods_df)
            mr = _get_method_row(methods_df, m)
            fair_today = _f(mr["Idag"]) if mr is not None else None
            up = _calc_upside(meta.get("price"), fair_today)
            rank_rows.append((up if up is not None else -1e9, row))
        except Exception:
            rank_rows.append((-1e9, row))
        prog.progress((i+1)/len(q))
        time.sleep(0.15)
    prog.empty()

    # Sortera störst uppsida först
    rank_rows.sort(key=lambda x: (x[0] is None, -x[0] if x[0] is not None else -1e9))
    ranked = [r for _, r in rank_rows]

    # Bläddringstillstånd
    if "analysis_idx" not in st.session_state:
        st.session_state.analysis_idx = 0
    total = len(ranked)

    # Bläddringsknappar
    bc = st.columns(3)
    if bc[0].button("⬅️ Föregående", use_container_width=True):
        st.session_state.analysis_idx = (st.session_state.analysis_idx - 1) % total
    bc[1].markdown(f"<div style='text-align:center'>**{st.session_state.analysis_idx+1} / {total}**</div>", unsafe_allow_html=True)
    if bc[2].button("➡️ Nästa", use_container_width=True):
        st.session_state.analysis_idx = (st.session_state.analysis_idx + 1) % total

    # Val att hoppa direkt till ticker
    jump_ticker = st.selectbox(
        "Hoppa till bolag",
        [str(r.get("Ticker")) for r in ranked],
        index=st.session_state.analysis_idx
    )
    # Sync index om användaren hoppar
    for i, r in enumerate(ranked):
        if str(r.get("Ticker")) == jump_ticker:
            st.session_state.analysis_idx = i
            break

    # Rendera aktivt bolag
    active_row = ranked[st.session_state.analysis_idx]
    _render_company_card(active_row, st.session_state.analysis_idx, total)

# app.py — Del 4/4
# ============================================================
# Sidor: Editor / Analys (redan i Del 3) / Ranking / Inställningar / Batch
# Main()
# ============================================================

# -------------------------
# Editor – Lägg till/Uppdatera bolag
# -------------------------
def page_editor():
    st.header("📝 Lägg till / uppdatera bolag")

    df = read_data_df()
    is_empty = df.empty

    # Välj ticker (ny eller befintlig)
    tickers = ["— nytt —"] + (sorted(df["Ticker"].dropna().astype(str).unique().tolist()) if not is_empty else [])
    pick = st.selectbox("Välj ticker", tickers, index=0)
    is_new = pick == "— nytt —"

    # Förifyllning
    init = {}
    if not is_new and not is_empty:
        init = df[df["Ticker"].astype(str).str.upper() == pick.upper()].iloc[0].to_dict()

    with st.form("edit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        ticker = c1.text_input("Ticker", value="" if is_new else pick).strip().upper()
        name   = c2.text_input("Bolagsnamn", value=str(_nz(init.get("Bolagsnamn"), "")))
        sector = c3.text_input("Sektor", value=str(_nz(init.get("Sektor"), "")))

        bucket = st.selectbox("Bucket", DEFAULT_BUCKETS,
                              index=DEFAULT_BUCKETS.index(_nz(init.get("Bucket"), DEFAULT_BUCKETS[0]))
                              if _nz(init.get("Bucket"), DEFAULT_BUCKETS[0]) in DEFAULT_BUCKETS else 0)

        valuta = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"],
                              index=["USD","EUR","NOK","CAD","SEK"].index(str(_nz(init.get("Valuta"), "USD")).upper()))

        d1, d2, d3, d4 = st.columns(4)
        antal   = d1.number_input("Antal aktier", min_value=0, step=1, value=int(_nz(_f(init.get("Antal aktier")), 0)))
        gav     = d2.number_input("GAV (SEK)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("GAV (SEK)")), 0.0)))
        kurs    = d3.number_input("Aktuell kurs", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("Aktuell kurs")), 0.0)))
        shrs    = d4.number_input("Utestående aktier", min_value=0.0, step=1.0, value=float(_nz(_f(init.get("Utestående aktier")), 0.0)))

        e1, e2, e3, e4 = st.columns(4)
        rev_ttm  = e1.number_input("Rev TTM", min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("Rev TTM")), 0.0)))
        ebitda_t = e2.number_input("EBITDA TTM", min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("EBITDA TTM")), 0.0)))
        eps_ttm  = e3.number_input("EPS TTM", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EPS TTM")), 0.0)))
        net_debt = e4.number_input("Net debt", min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("Net debt")), 0.0)))

        f1, f2, f3, f4 = st.columns(4)
        pe_ttm  = f1.number_input("PE TTM", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("PE TTM")), 0.0)))
        pe_fwd  = f2.number_input("PE FWD", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("PE FWD")), 0.0)))
        ev_rev  = f3.number_input("EV/Revenue", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EV/Revenue")), 0.0)))
        ev_ebit = f4.number_input("EV/EBITDA", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EV/EBITDA")), 0.0)))

        g1, g2, g3, g4 = st.columns(4)
        p_b   = g1.number_input("P/B", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("P/B")), 0.0)))
        bvps  = g2.number_input("BVPS", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("BVPS")), 0.0)))
        eps1y = g3.number_input("EPS 1Y (estimat)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EPS 1Y")), 0.0)))
        epscg = g4.number_input("EPS CAGR", min_value=0.0, step=0.001, value=float(_nz(_f(init.get("EPS CAGR")), 0.0)))

        h1, h2, h3, h4 = st.columns(4)
        revcg = h1.number_input("Rev CAGR", min_value=0.0, step=0.001, value=float(_nz(_f(init.get("Rev CAGR")), 0.0)))
        dps   = h2.number_input("Årlig utdelning (DPS)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("Årlig utdelning")), 0.0)))
        dpscg = h3.number_input("Utdelning CAGR", min_value=0.0, step=0.001, value=float(_nz(_f(init.get("Utdelning CAGR")), 0.0)))
        prim  = h4.selectbox("Primär metod", _PREFER_ORDER,
                             index=_PREFER_ORDER.index(str(_nz(init.get("Primär metod"), "ev_ebitda"))) if str(_nz(init.get("Primär metod"), "ev_ebitda")) in _PREFER_ORDER else 0)

        i1, i2 = st.columns(2)
        fetch_btn = i1.form_submit_button("🔎 Hämta från Yahoo nu")
        save_btn  = i2.form_submit_button("💾 Spara till Data")

    if fetch_btn and ticker:
        snap = fetch_yahoo_snapshot(ticker)
        st.info(
            f"Hämtat: pris={snap.get('price')} {snap.get('currency')}, "
            f"Rev TTM={snap.get('revenue_ttm')}, EBITDA TTM={snap.get('ebitda_ttm')}, "
            f"EPS TTM={snap.get('eps_ttm')}, PE TTM={snap.get('pe_ttm')}, PE FWD={snap.get('pe_fwd')}, "
            f"EV/Rev={snap.get('ev_to_sales')}, EV/EBITDA={snap.get('ev_to_ebitda')}, "
            f"P/B={snap.get('p_to_book')}, BVPS={snap.get('bvps')}, NetDebt={snap.get('net_debt')}"
        )
        st.caption("Fyll i de fält du vill spara och klicka **Spara till Data**.")

    if save_btn and ticker:
        # Upsert
        df_new = read_data_df()
        ts = now_stamp()
        new_row = {
            "Timestamp": ts,
            "Ticker": ticker,
            "Bolagsnamn": name,
            "Sektor": sector,
            "Bucket": bucket,
            "Valuta": valuta,
            "Antal aktier": antal,
            "GAV (SEK)": gav,
            "Aktuell kurs": kurs,
            "Utestående aktier": shrs,
            "Net debt": net_debt,
            "Rev TTM": rev_ttm,
            "EBITDA TTM": ebitda_t,
            "EPS TTM": eps_ttm,
            "PE TTM": pe_ttm,
            "PE FWD": pe_fwd,
            "EV/Revenue": ev_rev,
            "EV/EBITDA": ev_ebit,
            "P/B": p_b,
            "BVPS": bvps,
            "EPS 1Y": eps1y,
            "Rev CAGR": revcg,
            "EPS CAGR": epscg,
            "Årlig utdelning": dps,
            "Utdelning CAGR": dpscg,
            "Primär metod": prim,
            "Senast auto uppdaterad": "",
            "Auto källa": "Manuell",
        }
        # säkerställ alla kolumner
        for c in DATA_COLUMNS:
            if c not in df_new.columns:
                df_new[c] = np.nan

        mask = df_new["Ticker"].astype(str).str.upper() == ticker.upper()
        if mask.any():
            for k, v in new_row.items():
                df_new.loc[mask, k] = v
        else:
            df_new = pd.concat([df_new, pd.DataFrame([new_row])[df_new.columns]], ignore_index=True)

        write_data_df(df_new)
        st.success("Sparat till Data.")

# -------------------------
# Ranking – uppsida mot primär FV (Idag)
# -------------------------
def page_ranking():
    st.header("🏁 Ranking – uppsida mot primär fair value (Idag)")

    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    b1, b2 = st.columns(2)
    buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned   = b2.selectbox("Urval", ["Alla", "Innehav (>0)", "Watchlist (=0)"], index=0)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned == "Innehav (>0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    elif owned == "Watchlist (=0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    if q.empty:
        st.warning("Inget matchar filtret.")
        return

    rows = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            methods_df, meta = compute_methods_for_row(r)
            row_primary = str(_nz(r.get("Primär metod"), "")).strip()
            if row_primary and row_primary in methods_df["Metod"].values:
                method = row_primary
            else:
                method = _first_nonempty_method(methods_df)
            mr = _get_method_row(methods_df, method)
            fair_today = _f(mr["Idag"]) if mr is not None else None
            price = meta.get("price")
            up = _calc_upside(price, fair_today)
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": meta.get("currency"),
                "Pris": price,
                "Primär metod": method,
                "Fair value (Idag)": fair_today,
                "Uppsida %": up
            })
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None, "Primär metod": None, "Fair value (Idag)": None, "Uppsida %": None
            })
        prog.progress((i+1)/len(q))
        time.sleep(0.1)
    prog.empty()

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(by=["Uppsida %"], ascending=False, na_position="last")
    st.dataframe(out, use_container_width=True)

# -------------------------
# Inställningar – källskatt, parametrar, FX
# -------------------------
def page_settings():
    st.header("⚙️ Inställningar")
    settings = get_settings_map()

    st.subheader("Källskatt per valuta (andel, 0–1)")
    currencies = ["USD","EUR","NOK","CAD","SEK"]
    with st.form("wh_form"):
        cols = st.columns(len(currencies))
        vals = {}
        for i, ccy in enumerate(currencies):
            key = f"withholding_{ccy}"
            cur = float(settings.get(key, "0.15" if ccy != "SEK" else "0.0"))
            vals[ccy] = cols[i].number_input(ccy, min_value=0.0, max_value=1.0, step=0.01, value=cur, format="%.2f")
        submit = st.form_submit_button("💾 Spara källskatt")
    if submit:
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=["Key","Value"])
        for ccy, v in vals.items():
            if (s["Key"] == f"withholding_{ccy}").any():
                s.loc[s["Key"] == f"withholding_{ccy}", "Value"] = str(v)
            else:
                s.loc[len(s)] = [f"withholding_{ccy}", str(v)]
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    pe_w  = st.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05,
                            value=float(settings.get("pe_anchor_weight_ttm","0.50")))
    decay = st.number_input("Multipel-decay per år (0..1)", min_value=0.0, max_value=1.0, step=0.01,
                            value=float(settings.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=["Key","Value"])
        def upsert(k, v):
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s.loc[len(s)] = [k, str(v)]
        upsert("pe_anchor_weight_ttm", pe_w)
        upsert("multiple_decay", decay)
        _write_df(SETTINGS_TITLE, s)
        st.success("Parametrar uppdaterade.")

    st.subheader("Valutakurser (SEK per 1)")
    if st.button("🔄 Uppdatera FX från Yahoo"):
        mp = _load_fx_and_update_sheet()
        st.json(mp)
        st.success("FX uppdaterad.")

# -------------------------
# Batch – uppdatera alla, snapshots
# -------------------------
def page_batch():
    st.header("🧰 Batch-uppdatering")
    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    throttle = st.slider("Fördröjning per bolag (sek)", 0.1, 2.0, 0.6, 0.1)

    if st.button("🔎 Uppdatera nycklar från Yahoo (alla)"):
        prog = st.progress(0.0)
        df2 = df.copy()
        for i, (idx, r) in enumerate(df2.iterrows()):
            tkr = str(r["Ticker"]).strip().upper()
            snap = fetch_yahoo_snapshot(tkr)
            def set_if(k_sheet, v):
                if v is not None: df2.at[idx, k_sheet] = v
            set_if("Aktuell kurs", snap.get("price"))
            if snap.get("currency"): df2.at[idx, "Valuta"] = snap.get("currency")
            set_if("Rev TTM",    snap.get("revenue_ttm"))
            set_if("EBITDA TTM", snap.get("ebitda_ttm"))
            set_if("EPS TTM",    snap.get("eps_ttm"))
            set_if("PE TTM",     snap.get("pe_ttm"))
            set_if("PE FWD",     snap.get("pe_fwd"))
            set_if("EV/Revenue", snap.get("ev_to_sales"))
            set_if("EV/EBITDA",  snap.get("ev_to_ebitda"))
            set_if("P/B",        snap.get("p_to_book"))
            set_if("BVPS",       snap.get("bvps"))
            set_if("Net debt",   snap.get("net_debt"))
            df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
            df2.at[idx, "Auto källa"] = "Yahoo"
            time.sleep(throttle)
            prog.progress((i+1)/len(df2))
        write_data_df(df2)
        prog.empty()
        st.success("Alla tickers uppdaterade från Yahoo.")

    if st.button("📷 Spara snapshots (alla)"):
        settings = get_settings_map()
        prog = st.progress(0.0)
        for i, (_, r) in enumerate(df.iterrows()):
            methods_df, meta = compute_methods_for_row(r)
            save_quarter_snapshot(str(r["Ticker"]).strip().upper(), methods_df, meta)
            time.sleep(throttle)
            prog.progress((i+1)/len(df))
        prog.empty()
        st.success("Snapshots sparade.")

# -------------------------
# MAIN
# -------------------------
def run_main_ui():
    st.title(APP_TITLE)

    # Liten statusrad
    with st.expander("📊 Status (FX & Settings)", expanded=False):
        st.write("FX:", get_fx_map())
        st.write("Settings:", get_settings_map())

    page = st.sidebar.radio("Sidor", ["Editor", "Analys", "Ranking", "Inställningar", "Batch"], index=1)

    if page == "Editor":
        page_editor()
    elif page == "Analys":
        page_analysis()      # från Del 3
    elif page == "Ranking":
        page_ranking()
    elif page == "Inställningar":
        page_settings()
    elif page == "Batch":
        page_batch()

def main():
    run_main_ui()

if __name__ == "__main__":
    main()
