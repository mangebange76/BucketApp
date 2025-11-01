# ============================================================
# app.py — Del 1/4
# Bas, robust Sheets-anslutning, utils, FX & Settings (WHT)
# ============================================================
from __future__ import annotations

import re
import math
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st
import gspread
import yfinance as yf
import requests
from google.oauth2.service_account import Credentials
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import WorksheetNotFound

# -----------------------------
# Streamlit & globala defaults
# -----------------------------
st.set_page_config(page_title="Aktieanalys & Riktkurser", layout="wide")
RATE_LIMIT_SLEEP: float = 0.35  # paus mellan nätanrop (kan ändras i sidopanelen)

# FX-par (Yahoo Finance symboler för SEK-kors)
FX_PAIRS = {
    "USD": "USDSEK=X",
    "EUR": "EURSEK=X",
    "NOK": "NOKSEK=X",
    "CAD": "CADSEK=X",
    "GBP": "GBPSEK=X",
    "DKK": "DKKSEK=X",
    "SEK": None,  # bas
}

# ============================================================
# Små util-funktioner
# ============================================================
def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def nz(x: Any, default: Any = 0.0) -> Any:
    """Coalesce: None/NaN/'' → default."""
    if x is None:
        return default
    if isinstance(x, float) and math.isnan(x):
        return default
    if isinstance(x, str) and x.strip() == "":
        return default
    return x

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b is None or float(b) == 0.0:
            return default
        return float(a) / float(b)
    except Exception:
        return default

def to_float(val: Any, default: Optional[float] = None) -> Optional[float]:
    """Robust talparser: stöd för '1 234,56', '1,234.56', '', None."""
    if val is None:
        return default
    if isinstance(val, (int, float)) and not (isinstance(val, float) and math.isnan(val)):
        return float(val)
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return default
        # Ta bort mellanslag
        s = s.replace(" ", "")
        # Om både komma & punkt, anta komma som decimal (EU)
        if "," in s and "." in s:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return default
    return default

def _resolve_price(*candidates, mcap=None, shares=None) -> Optional[float]:
    """Välj första vettiga priset; härled via MC/Shares vid behov."""
    for c in candidates:
        v = to_float(c, None)
        if v is not None and v > 0:
            return v
    m = to_float(mcap, None)
    s = to_float(shares, None)
    if m and s and s > 0:
        return m / s
    return None

def _df_pick_first(df: pd.DataFrame, keys: List[str]) -> Optional[float]:
    """Hämta första icke-NaN i valfri rad (nyaste kolumn oftast först i yfinance)."""
    try:
        for k in keys:
            if k in df.index:
                row = df.loc[k]
                if hasattr(row, "dropna"):
                    ser = row.dropna()
                    if not ser.empty:
                        return float(ser.iloc[0])
                v = to_float(row, None)
                if v is not None:
                    return v
    except Exception:
        pass
    return None

# ============================================================
# Google Sheets — auth & öppning (robust ID/URL-hantering)
# ============================================================
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _load_google_credentials_dict() -> Dict[str, Any]:
    """
    Hämtar servicekonto från st.secrets["GOOGLE_CREDENTIALS"] (dict eller JSON-sträng).
    """
    raw = st.secrets.get("GOOGLE_CREDENTIALS")
    if raw is None:
        raise RuntimeError("GOOGLE_CREDENTIALS saknas i secrets.")
    if isinstance(raw, dict):
        creds = raw
    elif isinstance(raw, str):
        try:
            creds = json.loads(raw)
        except Exception:
            raise RuntimeError("GOOGLE_CREDENTIALS är en ogiltig JSON-sträng.")
    else:
        raise RuntimeError("GOOGLE_CREDENTIALS måste vara dict eller JSON-sträng.")
    return _normalize_private_key(creds)

def _extract_sheet_id(s: str) -> Optional[str]:
    """Ta ID från ren ID eller från full URL (/d/<ID>/...). Hanterar 'drivesdk' och whitespace."""
    if not s:
        return None
    s = str(s).strip().strip('"').strip("'")
    # Redan en ID?
    if "/" not in s and len(s) >= 30:
        return s
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)", s)
    return m.group(1) if m else None

def _open_spreadsheet() -> Spreadsheet:
    creds_dict = _load_google_credentials_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",  # krävs för open_by_url-resolver
    ]
    client = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(client)

    raw_id  = (st.secrets.get("SHEET_ID") or "").strip()
    raw_url = (st.secrets.get("SHEET_URL") or "").strip()

    # Först försök med ID (mest robust). Om tomt, extrahera ur URL.
    sid = _extract_sheet_id(raw_id) or _extract_sheet_id(raw_url)
    try:
        if sid:
            return gc.open_by_key(sid)
        if raw_url:
            return gc.open_by_url(raw_url)
        raise RuntimeError("SHEET_ID/SHEET_URL saknas i secrets.")
    except Exception as e:
        st.error(
            f"Kunde inte öppna Google Sheet. "
            f"(servicekonto: {creds_dict.get('client_email')}) "
            f"Kontrollera delning som **Editor** och att **SHEET_ID/SHEET_URL** är korrekt. "
            f"Fel: {e}"
        )
        raise

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    try:
        return spread.worksheet(title)
    except WorksheetNotFound:
        return spread.add_worksheet(title=title, rows=2000, cols=40)

# Initiera Spreadsheet & blad
try:
    SPREAD: Spreadsheet = _open_spreadsheet()
except Exception:
    st.stop()

WS_DATA       = _get_ws(SPREAD, "Data")
WS_RESULT     = _get_ws(SPREAD, "Resultat")
WS_HIST       = _get_ws(SPREAD, "Historik")
WS_FX         = _get_ws(SPREAD, "Valutakurser")
WS_SETTINGS   = _get_ws(SPREAD, "Inställningar")  # för källskatt m.m.

# ============================================================
# Data-schemats kolumner (Data-fliken)
# ============================================================
DATA_COLUMNS: List[str] = [
    "Ticker","Bolagsnamn","Bucket","Valuta","Antal aktier","Preferred metod",
    # Snapshot/nyckeltal
    "Last Price","Market Cap","EV","Shares Out",
    "Revenue TTM","EBITDA TTM","FCF TTM",
    "PE TTM","PE FWD","EV/Revenue","EV/EBITDA",
    # Manuell styrning & proxies
    "G1","G2","G3",
    "ev_s_mult","ev_eb_mult","ev_fcf_mult","dacf_mult",
    "p_fcf_mult","p_nav_mult","p_affo_mult","p_b_mult","p_tbv_mult","p_nii_mult",
    "BV/ps","TBV/ps","NAV/ps","AFFO/ps","FCF/ps","NII/ps",
    "EPS_CAGR_5Y",
]

def _ensure_data_header():
    vals = WS_DATA.get_all_values()
    if not vals:
        WS_DATA.update([DATA_COLUMNS])
        return
    header = vals[0]
    changed = False
    for c in DATA_COLUMNS:
        if c not in header:
            header.append(c)
            changed = True
    if changed:
        df = pd.DataFrame(vals[1:], columns=vals[0]) if len(vals) > 1 else pd.DataFrame(columns=vals[0])
        for c in header:
            if c not in df.columns:
                df[c] = ""
        WS_DATA.clear()
        WS_DATA.update([header] + df[header].values.tolist())

_ensure_data_header()

# ============================================================
# FX — hämtning & lagring
# ============================================================
def fetch_fx_to_sek() -> Dict[str, float]:
    """Hämtar SEK-kurser via Yahoo FX-par. Returnerar {CCY: SEK_per_1}."""
    out = {"SEK": 1.0}
    for ccy, ysym in FX_PAIRS.items():
        if ccy == "SEK":
            continue
        try:
            t = yf.Ticker(ysym)
            hist = t.history(period="5d", interval="1d")
            if hist is not None and not hist.empty:
                rate = float(hist["Close"].dropna().iloc[-1])
                if rate > 0:
                    out[ccy] = rate
                    continue
        except Exception:
            pass
        # fallback: lämna gammalt värde om finns i WS_FX
        try:
            cur = load_fx_from_sheet()
            if ccy in cur:
                out[ccy] = float(cur[ccy])
        except Exception:
            pass
    return out

def push_fx_sheet(rates: Dict[str, float]):
    rows = [["Valuta","SEK_per_1","Uppdaterad"]]
    ts = now_ts()
    for k in sorted(rates.keys()):
        rows.append([k, f"{float(rates[k]):.6f}", ts])
    WS_FX.clear()
    WS_FX.update(rows)

def load_fx_from_sheet() -> Dict[str, float]:
    vals = WS_FX.get_all_values()
    if not vals:
        rates = fetch_fx_to_sek()
        push_fx_sheet(rates)
        return rates
    header = vals[0]
    m = {}
    for r in vals[1:]:
        if len(r) >= 2:
            ccy = (r[0] or "").strip().upper()
            rate = to_float(r[1], None)
            if ccy and rate:
                m[ccy] = float(rate)
    m["SEK"] = 1.0
    return m

def fx_to_sek(ccy: str, rates: Optional[Dict[str, float]] = None) -> float:
    if rates is None:
        rates = load_fx_from_sheet()
    if not ccy or ccy.upper() == "SEK":
        return 1.0
    return float(rates.get(ccy.upper(), 1.0))

# ============================================================
# Inställningar — källskatt (WHT) med persistens
# ============================================================
DEFAULT_WHT = {"USD": 0.15, "NOK": 0.25, "CAD": 0.15, "SEK": 0.00}

def _init_settings_wht_if_empty():
    vals = WS_SETTINGS.get_all_values()
    if not vals or not vals[0] or vals[0][0] != "Valuta":
        WS_SETTINGS.clear()
        rows = [["Valuta", "WHT (decimal)", "Senast uppdaterad"]]
        ts = now_ts()
        for k, v in DEFAULT_WHT.items():
            rows.append([k, f"{v:.4f}", ts])
        WS_SETTINGS.update(rows)

@st.cache_data(show_spinner=False, ttl=600)
def read_settings_wht() -> Dict[str, float]:
    _init_settings_wht_if_empty()
    vals = WS_SETTINGS.get_all_values()
    out = dict(DEFAULT_WHT)
    for r in vals[1:]:
        if len(r) >= 2:
            ccy = (r[0] or "").strip().upper()
            v = to_float(r[1], None)
            if ccy and v is not None:
                out[ccy] = float(v)
    return out

def _sorted_wht_keys(mapping: Dict[str, float]) -> List[str]:
    known = ["SEK","USD","EUR","NOK","CAD","GBP","DKK"]
    rest = [k for k in mapping.keys() if k not in known]
    return [k for k in known if k in mapping] + sorted(rest)

def write_settings_wht(mapping: Dict[str, float]):
    rows = [["Valuta", "WHT (decimal)", "Senast uppdaterad"]]
    ts = now_ts()
    for k in _sorted_wht_keys(mapping):
        v = float(mapping.get(k, DEFAULT_WHT.get(k, 0.15)))
        rows.append([k, f"{v:.4f}", ts])
    WS_SETTINGS.clear()
    WS_SETTINGS.update(rows)
    try:
        read_settings_wht.clear()
    except Exception:
        pass

def _norm_ccy(code: str) -> Optional[str]:
    c = str(code or "").strip().upper()
    return c if (2 <= len(c) <= 5 and c.isalpha()) else None

# ============================================================
# Data — läs/skriv mot fliken Data
# ============================================================
def read_data_df() -> pd.DataFrame:
    vals = WS_DATA.get_all_records(numericise_ignore=['all'])
    df = pd.DataFrame(vals)
    if df.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)
    # säkerställ alla kolumner
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
    return df[DATA_COLUMNS]

def write_data_df(df: pd.DataFrame):
    # säkerställ ordning
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[DATA_COLUMNS]
    WS_DATA.clear()
    WS_DATA.update([DATA_COLUMNS] + df.fillna("").values.tolist())

# ============================================================
# Header
# ============================================================
st.title("📊 Aktieanalys & investeringsförslag – riktkurser & utdelning")
st.caption("Alla priser & riktkurser visas i **bolagets valuta**. Portföljsummor summeras i **SEK** med automatiska valutakurser.")

# ============================================================
# app.py — Del 2/4
# Yahoo/Finnhub-hämtare, utdelning, CAGR & värderings-helpers
# ============================================================

# -----------------------------
# Små format-helpers
# -----------------------------
def fmt2(x: Any) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def yq_of(ts_str: str) -> Tuple[int, int, str]:
    """Returnera (år, kvartal, 'YYYY-QX') för en timestamp 'YYYY-mm-dd ...'."""
    try:
        y = int(ts_str[:4])
        m = int(ts_str[5:7])
    except Exception:
        y, m = time.gmtime().tm_year, time.gmtime().tm_mon
    q = 1 + (m - 1) // 3
    return y, q, f"{y}-Q{q}"

# ============================================================
# Yahoo Finance — snapshot & historiska serier
# ============================================================
def _yf_fast_info(tk: str) -> Dict[str, Any]:
    out = {}
    try:
        t = yf.Ticker(tk)
        fi = getattr(t, "fast_info", None)
        if fi:
            out = dict(fi)
    except Exception:
        pass
    return out

def _yf_info(tk: str) -> Dict[str, Any]:
    # yfinance 'info' kan vara långsam/instabil – använd sparsamt.
    out = {}
    try:
        t = yf.Ticker(tk)
        inf = getattr(t, "info", None)
        if isinstance(inf, dict):
            out = inf
    except Exception:
        pass
    return out

@st.cache_data(show_spinner=False, ttl=900)
def _yf_quarterly_income_stmt(tk: str) -> Optional[pd.DataFrame]:
    try:
        t = yf.Ticker(tk)
        df = t.quarterly_income_stmt  # index=post, kolumner=perioder (senaste först)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def _yf_annual_income_stmt(tk: str) -> Optional[pd.DataFrame]:
    try:
        t = yf.Ticker(tk)
        df = t.income_stmt
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def _yf_quarterly_cashflow(tk: str) -> Optional[pd.DataFrame]:
    try:
        t = yf.Ticker(tk)
        df = t.quarterly_cashflow
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=1200)
def yahoo_dividend_ttm(ticker: str) -> Optional[float]:
    """Summa utdelningar per aktie de senaste 365 dagarna (TTM)."""
    try:
        t = yf.Ticker(ticker)
        s = getattr(t, "dividends", None)
        if s is None or s.empty:
            return None
        s = s.dropna()
        if s.empty:
            return None
        last_year_sum = float(s[s.index >= (s.index.max() - pd.Timedelta(days=365))].sum())
        return last_year_sum if last_year_sum > 0 else None
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=1200)
def yahoo_dividend_cagr(ticker: str, years: int = 5) -> Optional[float]:
    """CAGR på utdelning per år (kalenderårssummor)."""
    try:
        t = yf.Ticker(ticker)
        s = getattr(t, "dividends", None)
        if s is None or s.empty:
            return None
        s = s.dropna()
        if s.empty:
            return None
        per_year = s.groupby(s.index.year).sum()
        ys = per_year.index.tolist()
        if len(ys) < 2:
            return None
        for span in (years, 3):
            if len(ys) >= span + 1:
                y0, yN = ys[-(span+1)], ys[-1]
                v0, vN = float(per_year.loc[y0]), float(per_year.loc[yN])
                if v0 > 0 and vN > 0:
                    return (vN / v0) ** (1.0 / span) - 1.0
    except Exception:
        return None
    return None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """Hämta snapshot: pris, MC, EV, TTM/Forward-PE, EV/Revenue/EBITDA, FCF, DPS m.m."""
    tk = ticker.upper()
    snap: Dict[str, Any] = {}
    fi = _yf_fast_info(tk)
    info = _yf_info(tk)

    currency = fi.get("currency") or info.get("currency") or "SEK"
    last_price = _resolve_price(
        fi.get("last_price"), fi.get("lastPrice"), fi.get("last"), info.get("currentPrice"),
        mcap=fi.get("market_cap") or info.get("marketCap"),
        shares=info.get("sharesOutstanding") or fi.get("shares")
    )
    mcap = to_float(fi.get("market_cap") or info.get("marketCap"), None)
    shares_out = to_float(info.get("sharesOutstanding") or fi.get("shares"), None)
    if (shares_out is None or shares_out <= 0) and mcap and last_price and last_price > 0:
        shares_out = mcap / last_price

    # EV: ta direkt om finns, annars MC + Debt - Cash
    ev = to_float(fi.get("enterprise_value") or info.get("enterpriseValue"), None)
    if ev is None:
        total_debt = to_float(info.get("totalDebt"), 0.0)
        cash = to_float(info.get("totalCash"), 0.0)
        ev = (mcap or 0.0) + total_debt - cash

    # P/E TTM & Forward
    pe_ttm = to_float(fi.get("trailing_pe") or info.get("trailingPE"), None)
    pe_fwd = to_float(fi.get("forward_pe") or info.get("forwardPE"), None)

    # EV/Revenue & EV/EBITDA
    ev_rev = to_float(info.get("enterpriseToRevenue"), None)
    ev_eb  = to_float(info.get("enterpriseToEbitda"), None)

    # TTM via statements (summa 4 senaste kvartal)
    rev_ttm = None
    ebitda_ttm = None
    inc_q = _yf_quarterly_income_stmt(tk)
    if inc_q is not None and not inc_q.empty:
        if "Total Revenue" in inc_q.index:
            rv = inc_q.loc["Total Revenue"].dropna()
            if rv.size >= 1:
                rev_ttm = float(rv.iloc[:4].sum())
        for cand in ["Ebitda", "EBITDA", "Operating Income"]:
            if cand in inc_q.index:
                eb = inc_q.loc[cand].dropna()
                if eb.size >= 1:
                    ebitda_ttm = float(eb.iloc[:4].sum())
                    break

    # FCF TTM (via cashflow)
    fcf_ttm = None
    cf_q = _yf_quarterly_cashflow(tk)
    if cf_q is not None and not cf_q.empty:
        if "Free Cash Flow" in cf_q.index:
            ser = cf_q.loc["Free Cash Flow"].dropna()
            if ser.size >= 1:
                fcf_ttm = float(ser.iloc[:4].sum())
        else:
            ocf = cf_q.loc["Total Cash From Operating Activities"].dropna() if "Total Cash From Operating Activities" in cf_q.index else None
            capex = cf_q.loc["Capital Expenditures"].dropna() if "Capital Expenditures" in cf_q.index else None
            if ocf is not None and capex is not None and ocf.size >= 1 and capex.size >= 1:
                fcf_ttm = float(ocf.iloc[:4].sum() - capex.iloc[:4].sum())

    # Dividend per share (TTM)
    dps_ttm = yahoo_dividend_ttm(tk)
    div_yield = (dps_ttm / last_price) if (dps_ttm and last_price and last_price > 0) else None

    snap.update({
        "currency": currency,
        "last_price": last_price,
        "market_cap": mcap,
        "enterprise_value": ev,
        "shares_out": shares_out,
        "revenue_ttm": rev_ttm,
        "ebitda_ttm": ebitda_ttm,
        "fcf_ttm": fcf_ttm,
        "pe_ttm": pe_ttm,
        "pe_forward": pe_fwd,
        "ev_to_revenue": ev_rev,
        "ev_to_ebitda": ev_eb,
        "dividend_ps": dps_ttm,
        "dividend_yield": div_yield,
        "long_name": info.get("longName"),
        "short_name": info.get("shortName"),
    })
    return snap

# ============================================================
# Finnhub — estimat & metrics (valfritt)
# ============================================================
def _fh_key() -> Optional[str]:
    k = st.secrets.get("FINNHUB_API_KEY")
    if k:
        return str(k).strip()
    return None

def _fh_get(path: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    key = _fh_key()
    if not key:
        return None
    try:
        url = f"https://finnhub.io/api/v1/{path}"
        p = dict(params or {})
        p["token"] = key
        r = requests.get(url, params=p, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_finnhub_estimates(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Försök hämta EPS/Revenue-estimat ~nästa 1–2 år.
    Vi använder kvartalslistan 'stock/earnings' och summerar 4/8 kvartal framåt.
    """
    tk = ticker.upper()
    out: Dict[str, Any] = {}
    j = _fh_get("stock/earnings", {"symbol": tk})
    eps_next1 = eps_next2 = rev_next1 = rev_next2 = None
    try:
        if isinstance(j, list) and j:
            df = pd.DataFrame(j)
            df = df.sort_values("period", ascending=True)
            est_eps = df.get("epsEstimated")
            est_rev = df.get("revenueEstimated")
            if est_eps is not None and est_eps.notna().any():
                eps_next1 = float(est_eps.tail(4).sum())
                eps_next2 = float(est_eps.tail(8).sum() - est_eps.tail(4).sum()) if est_eps.size >= 8 else None
            if est_rev is not None and est_rev.notna().any():
                rev_next1 = float(est_rev.tail(4).sum())
                rev_next2 = float(est_rev.tail(8).sum() - est_rev.tail(4).sum()) if est_rev.size >= 8 else None
    except Exception:
        pass

    res = {}
    if eps_next1 is not None: res["eps_next1"] = eps_next1
    if eps_next2 is not None: res["eps_next2"] = eps_next2
    if rev_next1 is not None: res["rev_next1"] = rev_next1
    if rev_next2 is not None: res["rev_next2"] = rev_next2
    return res or None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_finnhub_eps_quarterly(ticker: str) -> Optional[pd.Series]:
    """Kvartalsvis EPS (faktiskt) – kan användas för att härleda EPS-CAGR."""
    tk = ticker.upper()
    j = _fh_get("stock/earnings", {"symbol": tk})
    if not isinstance(j, list) or not j:
        return None
    try:
        df = pd.DataFrame(j)
        if "epsActual" not in df.columns:
            return None
        s = df.set_index("period")["epsActual"].dropna()
        if s.empty:
            return None
        s.index = pd.to_datetime(s.index)
        s = s.sort_index()  # äldst→nyast
        return s
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_finnhub_metrics(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Hämtar 'stock/metric?metric=all' och försöker extrahera P/E-band (p50).
    Ofta saknas band – då används peTTM som ankare.
    """
    tk = ticker.upper()
    j = _fh_get("stock/metric", {"symbol": tk, "metric": "all"})
    if not isinstance(j, dict):
        return None
    metric = j.get("metric") or {}
    pe_ttm = to_float(metric.get("peTTM"), None)
    out = {"pe_band": {"p25": None, "p50": pe_ttm, "p75": None}, "pe_ttm": pe_ttm}
    return out

# ============================================================
# CAGR från kvartalsserier
# ============================================================
def eps_cagr_from_quarters(q_eps: Optional[pd.Series], years: int = 5) -> Optional[float]:
    """Omvandla kvartals-EPS till årssummor och beräkna CAGR."""
    if q_eps is None or q_eps.empty:
        return None
    s = q_eps.copy().dropna()
    if s.empty:
        return None
    by_year = s.resample("Y").sum()
    if len(by_year) < 2:
        return None
    for span in (years, 3):
        if len(by_year) >= span + 1:
            v0 = float(by_year.iloc[-(span+1)])
            vN = float(by_year.iloc[-1])
            if v0 > 0 and vN > 0:
                return (vN / v0) ** (1.0 / span) - 1.0
    return None

# ============================================================
# Värderings-helpers
# ============================================================
def series_from_est_or_cagr(base0: float,
                            est1: Optional[float],
                            est2: Optional[float],
                            cagr: Optional[float],
                            g1: float, g2: float, g3: float) -> Tuple[float, float, float, float]:
    """Returnera (t0,t1,t2,t3) för en bas (TTM) + antingen estimat eller CAGR-fallback."""
    b0 = float(nz(base0, 0.0))
    if est1 is not None:
        t1 = float(est1)
    else:
        t1 = b0 * (1.0 + float(cagr)) if cagr is not None else b0 * (1.0 + float(g1))
    if est2 is not None:
        t2 = float(est2)
    else:
        t2 = t1 * (1.0 + float(cagr)) if cagr is not None else t1 * (1.0 + float(g2))
    t3 = t2 * (1.0 + float(cagr)) if cagr is not None else t2 * (1.0 + float(g3))
    return b0, t1, t2, t3

def ev_multiple_from_series(s0: float, s1: float, s2: float, s3: float,
                            mult: float, comp_rate: float,
                            net_debt: float, shares_out: float) -> Tuple[float, float, float, float]:
    """Pris/aktie från EV-multipel-serie med multipelkompression."""
    m0 = float(mult)
    m1 = float(mult) * (1.0 - float(comp_rate)) ** 1
    m2 = float(mult) * (1.0 - float(comp_rate)) ** 2
    m3 = float(mult) * (1.0 - float(comp_rate)) ** 3

    def _px(val, mm):
        ev_val = float(val) * float(mm)
        eq = ev_val - float(net_debt)
        return safe_div(eq, float(shares_out), 0.0)

    return (_px(s0, m0), _px(s1, m1), _px(s2, m2), _px(s3, m3))

def price_multiple_from_series(ps0: float, ps1: float, ps2: float, ps3: float,
                               mult: float, comp_rate: float) -> Tuple[float, float, float, float]:
    """Pris/aktie från P-multipel (per-aktie-serie) med kompression."""
    m0 = float(mult)
    m1 = float(mult) * (1.0 - float(comp_rate)) ** 1
    m2 = float(mult) * (1.0 - float(comp_rate)) ** 2
    m3 = float(mult) * (1.0 - float(comp_rate)) ** 3
    return (ps0 * m0, ps1 * m1, ps2 * m2, ps3 * m3)

def bull_bear(t1: float, bull_mult: float, bear_mult: float) -> Tuple[float, float]:
    try:
        return float(t1) * float(bull_mult), float(t1) * float(bear_mult)
    except Exception:
        return 0.0, 0.0

def choose_primary_method(bucket: str, sector: str, industry: str,
                          ticker: str, has_fcf: bool, has_ebitda: bool) -> str:
    b = (bucket or "").lower()
    # Utdelningshinkar: luta mot utdelningsankare
    if "utdelning" in b:
        return "p_affo" if has_fcf else ("p_nii" if not has_ebitda else "p_affo")
    # Tillväxthinkar
    if has_fcf:
        return "ev_fcf"
    if has_ebitda:
        return "ev_ebitda"
    return "pe_hist_vs_eps"

def pe_targets_from_estimates(price: float,
                              pe_ttm: Optional[float],
                              pe_fwd: Optional[float],
                              pe_band: Optional[Dict[str, Optional[float]]],
                              eps_ttm: Optional[float],
                              eps_next1: Optional[float],
                              eps_next2: Optional[float],
                              eps_cagr: Optional[float],
                              pe_comp_rate: float,
                              g3: float) -> Tuple[float, float, float, float, str]:
    """
    Returnera (t0,t1,t2,t3, note) för P/E-metoden:
    - t0: TTM-ankare (EPS_ttm × PE_anchor)
    - t1..t3: framåt (estimat→CAGR→G3) med årlig kompression på multipeln
    """
    px = float(nz(price, 0.0))
    p50 = pe_band.get("p50") if pe_band else None
    anchors = [x for x in [p50, pe_fwd, pe_ttm] if x is not None and x > 0]
    pe_anchor = float(np.median(anchors)) if anchors else float(nz(pe_ttm, 20.0))

    # EPS_ttm via pris/PE om saknas
    if (eps_ttm is None or eps_ttm <= 0.0) and pe_ttm and pe_ttm > 0 and px > 0:
        eps_ttm = px / pe_ttm
    if eps_ttm is None:
        eps_ttm = 0.0

    # EPS framåt
    if eps_next1 is None:
        eps_next1 = eps_ttm * (1.0 + float(eps_cagr)) if eps_cagr is not None else eps_ttm * (1.0 + float(g3))
    if eps_next2 is None:
        eps_next2 = float(eps_next1) * (1.0 + float(eps_cagr)) if eps_cagr is not None else float(eps_next1) * (1.0 + float(g3))

    # Komprimerade multiplar
    pe1 = pe_anchor * (1.0 - float(pe_comp_rate)) ** 1
    pe2 = pe_anchor * (1.0 - float(pe_comp_rate)) ** 2
    pe3 = pe_anchor * (1.0 - float(pe_comp_rate)) ** 3

    t0 = float(eps_ttm)   * float(pe_anchor)
    t1 = float(eps_next1) * float(pe1)
    t2 = float(eps_next2) * float(pe2)
    t3 = float(eps_next2) * (1.0 + float(eps_cagr or g3)) * float(pe3)

    note = f"anchor={pe_anchor:.2f}; pe_ttm={pe_ttm}; pe_fwd={pe_fwd}; p50={p50}"
    return t0, t1, t2, t3, note

# ============================================================
# app.py — Del 3/4
# UI: Sidopanel (reglage), Data-form, dynamisk källskatt
# ============================================================

# -----------------------------
# Sidopanel — globala reglage
# -----------------------------
with st.sidebar:
    st.header("⚙️ Inställningar")

    # Hastighet på nätanrop
    rate_limit_sleep = st.slider(
        "Paus mellan nätanrop (sek)", 0.10, 2.00, RATE_LIMIT_SLEEP, 0.05,
        help="Öka om du får rate limit från Yahoo/Finnhub/Sheets."
    )

    # Finnhub toggles
    finnhub_key_present = bool(st.secrets.get("FINNHUB_API_KEY"))
    use_finnhub = st.checkbox(
        "Använd Finnhub-estimat (om möjligt)",
        value=finnhub_key_present,
        help="Kräver FINNHUB_API_KEY i secrets.",
        disabled=not finnhub_key_present
    )

    st.markdown("---")
    st.subheader("Multipel-kompression (per år)")
    pe_comp_pct = st.slider("P/E-kompression (%)", 0.0, 15.0, 6.0, 0.5)
    ev_comp_pct = st.slider("EV-multiplar (%): EV/S, EV/EBITDA, EV/FCF", 0.0, 15.0, 5.0, 0.5)
    p_comp_pct  = st.slider("P-multiplar (%): P/FCF, P/B, P/NAV...", 0.0, 15.0, 4.0, 0.5)
    pe_comp_rate = pe_comp_pct/100.0
    ev_comp_rate = ev_comp_pct/100.0
    p_comp_rate  = p_comp_pct/100.0

    st.markdown("---")
    st.subheader("Fair idag (TTM vs Nästa 12m)")
    today_blend_w = st.slider(
        "Vikt mot framtid i Fair idag",
        min_value=0.0, max_value=1.0, value=0.50, step=0.05,
        help="0.00 = bara TTM • 1.00 = bara nästa 12m (estimat/CAGR)."
    )

    st.markdown("---")
    st.subheader("Bull/Bear (1 år)")
    bull_mult = st.number_input("Bull ×", min_value=1.00, max_value=3.00, value=1.15, step=0.01)
    bear_mult = st.number_input("Bear ×", min_value=0.10, max_value=0.99, value=0.85, step=0.01)

    st.markdown("---")
    st.subheader("Utdelningsvärdering")
    yield_anchor = st.number_input("Målyield idag (dec)", min_value=0.0, value=0.08, step=0.005, format="%.3f")
    yield_comp_pct = st.slider("Yield-kompression (per år, %)", 0.0, 20.0, 3.0, 0.5)
    yield_comp_rate = yield_comp_pct/100.0

    st.subheader("DDM (Gordon)")
    ddm_r     = st.number_input("Avkastningskrav r", min_value=0.01, value=0.10, step=0.005, format="%.3f")
    ddm_g_cap = st.number_input("Tak för långsiktig g", min_value=0.00, value=0.06, step=0.005, format="%.3f")

    st.markdown("---")
    st.subheader("Valutakurser")
    if st.button("🔄 Uppdatera valutakurser"):
        try:
            fx = fetch_fx_to_sek()
            push_fx_sheet(fx)
            st.success("Valutakurser uppdaterade.")
        except Exception as e:
            st.error(f"Kunde inte uppdatera valutakurser: {e}")

    st.markdown("---")
    st.subheader("Historik")
    autosave_hist = st.checkbox("Autospara kvartalssnapshot vid hämtning", value=True)

    st.markdown("---")
    st.subheader("Källskatt (withholding tax) – dynamisk lista")

    # Läs sparade värden
    try:
        _wht_map_current = read_settings_wht()
    except Exception:
        _wht_map_current = dict(DEFAULT_WHT)

    def _sorted_wht_keys(mapping: Dict[str, float]) -> List[str]:
        known = ["SEK","USD","EUR","NOK","CAD","GBP","DKK"]
        rest = [k for k in mapping.keys() if k not in known]
        return [k for k in known if k in mapping] + sorted(rest)

    # Redigera befintliga
    st.caption("Redigera (0.00–0.50) och markera för borttagning.")
    new_map: Dict[str, float] = {}
    to_delete: List[str] = []

    for ccy in _sorted_wht_keys(_wht_map_current):
        c1, c2, c3 = st.columns([2, 2, 1])
        c1.write(f"**{ccy}**")
        rate_val = float(_wht_map_current.get(ccy, 0.15))
        new_rate = c2.number_input(
            f"{ccy} WHT", min_value=0.0, max_value=0.50, value=rate_val, step=0.01,
            format="%.2f", key=f"wht_{ccy}"
        )
        new_map[ccy] = float(new_rate)
        if ccy != "SEK":
            if c3.checkbox("Ta bort", key=f"del_{ccy}"):
                to_delete.append(ccy)
        else:
            c3.caption("—")

    # Lägg till ny valuta
    with st.expander("➕ Lägg till valuta"):
        add_ccy = st.text_input("Valutakod (t.ex. EUR)", key="add_wht_ccy")
        add_rate = st.number_input("Källskatt (0.00–0.50)", 0.0, 0.50, 0.15, 0.01, format="%.2f", key="add_wht_rate")
        if st.button("Lägg till"):
            nc = _norm_ccy(add_ccy)
            if not nc:
                st.error("Ogiltig valutakod. Använd 2–5 bokstäver (t.ex. USD, EUR).")
            elif nc in _wht_map_current:
                st.info(f"{nc} finns redan.")
            else:
                m2 = dict(_wht_map_current)
                m2[nc] = float(add_rate)
                write_settings_wht(m2)
                st.session_state["WHT_MAP"] = m2
                st.success(f"Lade till {nc}.")
                st.experimental_rerun()

    c_save, c_reset = st.columns(2)
    if c_save.button("💾 Spara källskatt"):
        for d in to_delete:
            new_map.pop(d, None)
        write_settings_wht(new_map)
        st.session_state["WHT_MAP"] = dict(new_map)
        st.success("Källskatt uppdaterad.")
        st.experimental_rerun()

    if c_reset.button("↩︎ Återställ standard"):
        write_settings_wht(DEFAULT_WHT)
        st.session_state["WHT_MAP"] = dict(DEFAULT_WHT)
        st.info("Återställt till standard.")
        st.experimental_rerun()

# -----------------------------
# Helpers: spara kvartalssnapshot
# -----------------------------
def save_quarter_snapshot(ticker: str, snap: Dict[str, Any]):
    """Append:a en rad i fliken Historik med kvartalsstämplade nyckeltal."""
    try:
        # Initiera header om tomt
        vals = WS_HIST.get_all_values()
        header = [
            "Timestamp","YQ","Ticker","Currency","Last Price","Market Cap","EV","Shares Out",
            "Revenue TTM","EBITDA TTM","FCF TTM","Dividend/ps",
            "PE TTM","PE FWD","EV/Revenue","EV/EBITDA"
        ]
        if not vals:
            WS_HIST.update([header])

        ts = now_ts()
        y, q, yq = yq_of(ts)

        row = [
            ts, yq, ticker.upper(), snap.get("currency"),
            snap.get("last_price"), snap.get("market_cap"), snap.get("enterprise_value"), snap.get("shares_out"),
            snap.get("revenue_ttm"), snap.get("ebitda_ttm"), snap.get("fcf_ttm"), snap.get("dividend_ps"),
            snap.get("pe_ttm"), snap.get("pe_forward"), snap.get("ev_to_revenue"), snap.get("ev_to_ebitda"),
        ]
        WS_HIST.append_row(row, value_input_option="RAW")
    except Exception as e:
        st.warning(f"Kunde inte spara historik: {e}")

# -----------------------------
# Data — formulär för lägg till/uppdatera
# -----------------------------
st.markdown("## 📝 Lägg till / Uppdatera bolag (Data)")

data_df = read_data_df()
existing_tickers = sorted([t for t in data_df["Ticker"].dropna().astype(str).str.upper().unique() if t])

cA, cB = st.columns([1, 2])
with cA:
    mode = st.radio("Läge", ["Lägg till nytt", "Uppdatera befintligt"], horizontal=True)
with cB:
    sel_ticker = ""
    if mode == "Uppdatera befintligt" and existing_tickers:
        sel_ticker = st.selectbox("Välj ticker", existing_tickers, index=0)
    else:
        sel_ticker = st.text_input("Ticker (t.ex. NVDA, 2020.OL)").strip().upper()

# Hämta befintlig rad (om uppdatering)
row_data: Dict[str, Any] = {c: "" for c in DATA_COLUMNS}
if sel_ticker:
    if mode == "Uppdatera befintligt" and sel_ticker in data_df["Ticker"].astype(str).str.upper().values:
        row_data.update(data_df[data_df["Ticker"].astype(str).str.upper() == sel_ticker].iloc[0].to_dict())
    else:
        row_data["Ticker"] = sel_ticker

# Grundfält
c1, c2, c3, c4 = st.columns(4)
bucket_opts = [
    "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
    "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning"
]
row_data["Bolagsnamn"] = c1.text_input("Bolagsnamn", value=str(nz(row_data.get("Bolagsnamn"), "")))
row_data["Bucket"]     = c2.selectbox("Bucket", bucket_opts, index= bucket_opts.index(row_data.get("Bucket")) if row_data.get("Bucket") in bucket_opts else 0)
row_data["Valuta"]     = c3.text_input("Valuta (t.ex. USD, NOK, SEK)", value=str(nz(row_data.get("Valuta"), "USD"))).upper()
row_data["Antal aktier"] = c4.number_input("Antal aktier", min_value=0, step=1, value=int(nz(row_data.get("Antal aktier"), 0)))

# Preferred metod
method_opts = [
    "AUTO","pe_hist_vs_eps","ev_sales","ev_ebitda","ev_dacf","ev_fcf",
    "p_fcf","p_nav","p_affo","p_b","p_tbv","p_nii","div_yield","ddm"
]
row_data["Preferred metod"] = st.selectbox("Primär metod (valfri)", method_opts, index=method_opts.index(str(nz(row_data.get("Preferred metod"), "AUTO"))))

# G1–G3
gc1, gc2, gc3 = st.columns(3)
row_data["G1"] = gc1.number_input("G1 (år 1 tillväxt)", min_value=-1.0, max_value=3.0, value=float(nz(row_data.get("G1"), 0.15)), step=0.01, format="%.2f")
row_data["G2"] = gc2.number_input("G2 (år 2 tillväxt)", min_value=-1.0, max_value=3.0, value=float(nz(row_data.get("G2"), 0.12)), step=0.01, format="%.2f")
row_data["G3"] = gc3.number_input("G3 (år 3 tillväxt)", min_value=-1.0, max_value=3.0, value=float(nz(row_data.get("G3"), 0.10)), step=0.01, format="%.2f")

st.markdown("#### Avancerat (manuella multiplar & per-aktie-proxy)")
am1, am2, am3, am4 = st.columns(4)
row_data["ev_s_mult"]   = am1.number_input("EV/S multipel", 0.0, 200.0, float(nz(row_data.get("ev_s_mult"), 6.0)), 0.1)
row_data["ev_eb_mult"]  = am2.number_input("EV/EBITDA multipel", 0.0, 200.0, float(nz(row_data.get("ev_eb_mult"), 12.0)), 0.1)
row_data["ev_fcf_mult"] = am3.number_input("EV/FCF multipel", 0.0, 200.0, float(nz(row_data.get("ev_fcf_mult"), 18.0)), 0.1)
row_data["dacf_mult"]   = am4.number_input("EV/DACF multipel", 0.0, 200.0, float(nz(row_data.get("dacf_mult"), 10.0)), 0.1)

bm1, bm2, bm3, bm4 = st.columns(4)
row_data["p_fcf_mult"]  = bm1.number_input("P/FCF multipel", 0.0, 200.0, float(nz(row_data.get("p_fcf_mult"), 20.0)), 0.1)
row_data["p_nav_mult"]  = bm2.number_input("P/NAV multipel", 0.0, 200.0, float(nz(row_data.get("p_nav_mult"), 1.0)), 0.1)
row_data["p_affo_mult"] = bm3.number_input("P/AFFO multipel", 0.0, 200.0, float(nz(row_data.get("p_affo_mult"), 13.0)), 0.1)
row_data["p_b_mult"]    = bm4.number_input("P/B multipel", 0.0, 200.0, float(nz(row_data.get("p_b_mult"), 1.5)), 0.1)

bm5, bm6 = st.columns(2)
row_data["p_tbv_mult"]  = bm5.number_input("P/TBV multipel", 0.0, 200.0, float(nz(row_data.get("p_tbv_mult"), 1.2)), 0.1)
row_data["p_nii_mult"]  = bm6.number_input("P/NII multipel", 0.0, 200.0, float(nz(row_data.get("p_nii_mult"), 10.0)), 0.1)

ps1, ps2, ps3 = st.columns(3)
row_data["BV/ps"]   = ps1.number_input("BV/ps (valfritt)", 0.0, 1e9, float(nz(row_data.get("BV/ps"), 0.0)))
row_data["TBV/ps"]  = ps2.number_input("TBV/ps (valfritt)", 0.0, 1e9, float(nz(row_data.get("TBV/ps"), 0.0)))
row_data["NAV/ps"]  = ps3.number_input("NAV/ps (valfritt)", 0.0, 1e9, float(nz(row_data.get("NAV/ps"), 0.0)))

ps4, ps5, ps6 = st.columns(3)
row_data["AFFO/ps"] = ps4.number_input("AFFO/ps (valfritt)", 0.0, 1e9, float(nz(row_data.get("AFFO/ps"), 0.0)))
row_data["FCF/ps"]  = ps5.number_input("FCF/ps (valfritt)", 0.0, 1e9, float(nz(row_data.get("FCF/ps"), 0.0)))
row_data["NII/ps"]  = ps6.number_input("NII/ps (valfritt)", 0.0, 1e9, float(nz(row_data.get("NII/ps"), 0.0)))

row_data["EPS_CAGR_5Y"] = st.number_input("EPS CAGR 5Y (dec, valfritt)", -1.0, 3.0, float(nz(row_data.get("EPS_CAGR_5Y"), 0.0)), 0.01, format="%.2f")

# Snapshotfält (visas read-only – fylls vid Yahoo-hämtning)
st.markdown("#### Snapshot (fylls från Yahoo)")
sn1, sn2, sn3, sn4 = st.columns(4)
sn1.write(f"Last Price: **{fmt2(nz(row_data.get('Last Price'), ''))}**")
sn2.write(f"Market Cap: **{fmt2(nz(row_data.get('Market Cap'), ''))}**")
sn3.write(f"EV: **{fmt2(nz(row_data.get('EV'), ''))}**")
sn4.write(f"Shares Out: **{fmt2(nz(row_data.get('Shares Out'), ''))}**")

sn5, sn6, sn7, sn8 = st.columns(4)
sn5.write(f"Revenue TTM: **{fmt2(nz(row_data.get('Revenue TTM'), ''))}**")
sn6.write(f"EBITDA TTM: **{fmt2(nz(row_data.get('EBITDA TTM'), ''))}**")
sn7.write(f"FCF TTM: **{fmt2(nz(row_data.get('FCF TTM'), ''))}**")
sn8.write(f"Dividend/ps (TTM): **{fmt2(nz(row_data.get('Dividend/ps'), ''))}**")

sn9, sn10, sn11, sn12 = st.columns(4)
sn9.write(f"PE TTM: **{fmt2(nz(row_data.get('PE TTM'), ''))}**")
sn10.write(f"PE FWD: **{fmt2(nz(row_data.get('PE FWD'), ''))}**")
sn11.write(f"EV/Revenue: **{fmt2(nz(row_data.get('EV/Revenue'), ''))}**")
sn12.write(f"EV/EBITDA: **{fmt2(nz(row_data.get('EV/EBITDA'), ''))}**")

# Actions
ac1, ac2, ac3 = st.columns([1,1,2])
if ac1.button("📥 Hämta från Yahoo", disabled=(not sel_ticker)):
    try:
        snap = fetch_yahoo_snapshot(sel_ticker)
        time.sleep(rate_limit_sleep)
        # Mappa till Data-kolumner
        row_data["Valuta"]        = snap.get("currency") or row_data.get("Valuta") or "USD"
        row_data["Last Price"]    = snap.get("last_price") or row_data.get("Last Price")
        row_data["Market Cap"]    = snap.get("market_cap") or row_data.get("Market Cap")
        row_data["EV"]            = snap.get("enterprise_value") or row_data.get("EV")
        row_data["Shares Out"]    = snap.get("shares_out") or row_data.get("Shares Out")
        row_data["Revenue TTM"]   = snap.get("revenue_ttm") or row_data.get("Revenue TTM")
        row_data["EBITDA TTM"]    = snap.get("ebitda_ttm") or row_data.get("EBITDA TTM")
        row_data["FCF TTM"]       = snap.get("fcf_ttm") or row_data.get("FCF TTM")
        row_data["PE TTM"]        = snap.get("pe_ttm") or row_data.get("PE TTM")
        row_data["PE FWD"]        = snap.get("pe_forward") or row_data.get("PE FWD")
        row_data["EV/Revenue"]    = snap.get("ev_to_revenue") or row_data.get("EV/Revenue")
        row_data["EV/EBITDA"]     = snap.get("ev_to_ebitda") or row_data.get("EV/EBITDA")
        row_data["Dividend/ps"]   = snap.get("dividend_ps") or row_data.get("Dividend/ps")
        if not row_data.get("Bolagsnamn"):
            row_data["Bolagsnamn"] = snap.get("long_name") or snap.get("short_name") or row_data.get("Bolagsnamn")

        # Visa direkt
        st.success("Hämtat från Yahoo. Granska fälten ovan — spara för att skriva till Data.")
        # Spara historik vid behov
        if autosave_hist:
            save_quarter_snapshot(sel_ticker, snap)
    except Exception as e:
        st.error(f"Yahoo-hämtning misslyckades: {e}")

if ac2.button("💾 Spara till Data", disabled=(not sel_ticker)):
    try:
        # Uppdatera/Infoga rad i WS_DATA
        df = read_data_df()
        exists_mask = df["Ticker"].astype(str).str.upper() == sel_ticker
        if exists_mask.any():
            idx = df.index[exists_mask][0]
            for k in DATA_COLUMNS:
                if k in row_data:
                    df.at[idx, k] = row_data[k]
        else:
            new_row = {c: "" for c in DATA_COLUMNS}
            for k in DATA_COLUMNS:
                new_row[k] = row_data.get(k, "")
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        write_data_df(df)
        st.success("Sparat till fliken **Data**.")
    except Exception as e:
        st.error(f"Kan inte spara: {e}")

# -----------------------------
# Filtrering för analysvyn
# -----------------------------
st.markdown("---")
st.markdown("## 🔍 Filter för analys")
left, mid, right = st.columns([2,1,1])

with left:
    all_buckets = bucket_opts
    pick_buckets = st.multiselect("Visa buckets", all_buckets, default=all_buckets)

with mid:
    only_owned = st.checkbox("Visa endast innehav (antal > 0)", value=False)
with right:
    only_watch = st.checkbox("Visa endast watchlist (antal = 0)", value=False)
    if only_owned and only_watch:
        st.warning("Kan inte välja båda: avmarkerar 'watchlist'.")
        only_watch = False

# ============================================================
# app.py — Del 4/4
# Beräkningar, ranking, tabeller & kortvy (inkl. utdelning)
# ============================================================

# -----------------------------
# Små helpers för WHT/visning
# -----------------------------
def wht_rate(ccy: str) -> float:
    """Hämta källskatt (decimal) för en valuta från Inställningar, fallback till DEFAULT_WHT."""
    try:
        m = read_settings_wht()
        return float(m.get((ccy or "SEK").upper(), DEFAULT_WHT.get((ccy or "SEK").upper(), 0.15)))
    except Exception:
        return float(DEFAULT_WHT.get((ccy or "SEK").upper(), 0.15))

def _rate(ccy: str) -> float:
    try:
        return fx_to_sek(ccy, load_fx_from_sheet())
    except Exception:
        return 1.0

def _cur(x: Any, ccy: str) -> str:
    try:
        return f"{float(x):.2f} {ccy}"
    except Exception:
        return f"{x} {ccy}"

def _pct(x: Any) -> str:
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "–"

# -----------------------------
# Metodetiketter & “varför”
# -----------------------------
METHOD_LABEL = {
    "pe_hist_vs_eps": "P/E (hist vs EPS)",
    "ev_fcf": "EV/FCF",
    "p_fcf": "P/FCF",
    "ev_sales": "EV/Sales",
    "ev_ebitda": "EV/EBITDA",
    "p_nav": "P/NAV",
    "ev_dacf": "EV/DACF",
    "p_affo": "P/AFFO",
    "p_b": "P/B",
    "p_tbv": "P/TBV",
    "p_nii": "P/NII",
    "div_yield": "Yield-ankare (DPS/Yield)",
    "ddm": "DDM (Gordon)",
}

METHOD_REASON = {
    "p_nii": "BDC – värdera mot NII.",
    "p_affo": "REIT – kassaflöde via AFFO/ps.",
    "p_tbv": "Bank/finans – kapitalankare via TBV.",
    "ev_dacf": "Energi/Shipping – DACF/EBITDA-proxy.",
    "ev_ebitda": "Konjunktur/cykliskt – EV/EBITDA.",
    "ev_fcf": "Mogen tillväxt – EV/FCF.",
    "ev_sales": "Tidigt skede – EV/Sales (riskviktad).",
    "p_fcf": "Stabilt positiv FCF – P/FCF per aktie.",
    "p_b": "Kapitaltunga – P/B som golv.",
    "p_nav": "NAV-drivna (förvaltning/holding).",
    "pe_hist_vs_eps": "Vinstdrivet – P/E med historiskt ankare.",
    "div_yield": "Utdelningscase – målyield och DPS.",
    "ddm": "Stabil DPS-tillväxt – Gordon-formeln.",
}

def explain_method(primary: str, base_row: pd.Series) -> str:
    txt = METHOD_REASON.get(primary, "")
    has_fcf = nz(base_row.get("FCF TTM"), 0) > 0 or nz(base_row.get("FCF/ps"), 0) > 0
    has_eb  = nz(base_row.get("EBITDA TTM"), 0) > 0
    hints = []
    if has_fcf: hints.append("FCF>0")
    if has_eb:  hints.append("EBITDA>0")
    if hints: txt += (" • " if txt else "") + ", ".join(hints)
    return txt or "Automatisk heuristik."

# -----------------------------
# Huvudberäkning per rad
# -----------------------------
def compute_methods_row(r: pd.Series,
                        use_finnhub: bool,
                        pe_comp_rate: float,
                        ev_comp_rate: float,
                        p_comp_rate: float,
                        rate_sleep: float,
                        today_blend_w: float,
                        yield_anchor: float,
                        yield_comp_rate: float,
                        ddm_r: float,
                        ddm_g_cap: float,
                        bull_mult: float,
                        bear_mult: float) -> Dict[str, Any]:
    tkr = str(r.get("Ticker") or "").upper()
    if not tkr:
        raise ValueError("Saknar ticker")
    ccy = (r.get("Valuta") or "USD").upper()
    bucket = str(r.get("Bucket") or "")
    name = str(r.get("Bolagsnamn") or "")

    # Snapshot/bas
    px      = to_float(r.get("Last Price"), None)
    mcap    = to_float(r.get("Market Cap"), None)
    ev      = to_float(r.get("EV"), None)
    shares  = to_float(r.get("Shares Out"), None)
    rev0    = to_float(r.get("Revenue TTM"), None)
    ebit0   = to_float(r.get("EBITDA TTM"), None)
    fcf0    = to_float(r.get("FCF TTM"), None)
    pe_ttm  = to_float(r.get("PE TTM"), None)
    pe_fwd  = to_float(r.get("PE FWD"), None)
    ev_s0   = to_float(r.get("EV/Revenue"), None)  # info-only
    ev_eb0  = to_float(r.get("EV/EBITDA"), None)   # info-only
    dps0    = to_float(r.get("Dividend/ps"), None)

    # Fyll in saknade via Yahoo vid behov
    if px is None or shares is None or ev is None or mcap is None:
        snap = fetch_yahoo_snapshot(tkr)
        time.sleep(rate_sleep)
        px   = _resolve_price(px, snap.get("last_price"), mcap, shares)
        mcap = mcap or to_float(snap.get("market_cap"), None)
        ev   = ev or to_float(snap.get("enterprise_value"), None)
        shares = shares or to_float(snap.get("shares_out"), None)
        if dps0 in (None, 0.0):
            dps0 = snap.get("dividend_ps")
        if pe_ttm is None: pe_ttm = snap.get("pe_ttm")
        if pe_fwd is None: pe_fwd = snap.get("pe_forward")
        if rev0 is None:   rev0   = snap.get("revenue_ttm")
        if ebit0 is None:  ebit0  = snap.get("ebitda_ttm")
        if fcf0 is None:   fcf0   = snap.get("fcf_ttm")
        if not name:       name   = snap.get("long_name") or snap.get("short_name") or ""

    # Robust pris vid behov
    px = _resolve_price(px, mcap=mcap, shares=shares) or 0.0
    shares = float(nz(shares, 0.0))
    mcap   = float(nz(mcap, 0.0))
    ev     = float(nz(ev, mcap))
    net_debt = float(ev - mcap)

    # Multiplar (manuellt styrbara)
    ev_s_mult   = float(nz(r.get("ev_s_mult"), 6.0))
    ev_eb_mult  = float(nz(r.get("ev_eb_mult"), 12.0))
    ev_fcf_mult = float(nz(r.get("ev_fcf_mult"), 18.0))
    dacf_mult   = float(nz(r.get("dacf_mult"), 10.0))

    p_fcf_mult  = float(nz(r.get("p_fcf_mult"), 20.0))
    p_nav_mult  = float(nz(r.get("p_nav_mult"), 1.0))
    p_affo_mult = float(nz(r.get("p_affo_mult"), 13.0))
    p_b_mult    = float(nz(r.get("p_b_mult"), 1.5))
    p_tbv_mult  = float(nz(r.get("p_tbv_mult"), 1.2))
    p_nii_mult  = float(nz(r.get("p_nii_mult"), 10.0))

    # Per-aktie proxies
    bv_ps   = to_float(r.get("BV/ps"), None)
    tbv_ps  = to_float(r.get("TBV/ps"), None)
    nav_ps  = to_float(r.get("NAV/ps"), None)
    affo_ps = to_float(r.get("AFFO/ps"), None)
    fcf_ps0 = to_float(r.get("FCF/ps"), None)
    nii_ps0 = to_float(r.get("NII/ps"), None)

    # Tillväxtparametrar
    g1 = float(nz(r.get("G1"), 0.15))
    g2 = float(nz(r.get("G2"), 0.12))
    g3 = float(nz(r.get("G3"), 0.10))
    eps_cagr_manual = to_float(r.get("EPS_CAGR_5Y"), None)

    # Estimat (Finnhub) + härledning av EPS/REV-CAGR
    eps1 = eps2 = rev1 = rev2 = None
    pe_band = {"p50": None}
    eps_cagr = eps_cagr_manual
    rev_cagr = None

    if use_finnhub:
        try:
            e = fetch_finnhub_estimates(tkr) or {}
            eps1 = to_float(e.get("eps_next1"), None)
            eps2 = to_float(e.get("eps_next2"), None)
            rev1 = to_float(e.get("rev_next1"), None)
            rev2 = to_float(e.get("rev_next2"), None)
            time.sleep(rate_limit_sleep)
            m = fetch_finnhub_metrics(tkr) or {}
            pe_band = m.get("pe_band") or pe_band
            time.sleep(rate_limit_sleep)
            if eps_cagr is None:
                q = fetch_finnhub_eps_quarterly(tkr)
                time.sleep(rate_limit_sleep)
                eps_cagr = eps_cagr_from_quarters(q) or eps_cagr
        except Exception:
            pass

    if rev_cagr is None:
        try:
            rev_cagr = yahoo_revenue_cagr(tkr)  # definieras strax nedan
        except Exception:
            rev_cagr = None

    # EPS_ttm härledning vid behov för P/E
    eps_ttm = None
    if pe_ttm and pe_ttm > 0 and px > 0:
        eps_ttm = px / pe_ttm

    # Serier: Revenue / EBITDA / FCF
    r0, r1, r2, r3 = series_from_est_or_cagr(nz(rev0, 0.0), rev1, rev2, rev_cagr, g1, g2, g3)
    e0, e1, e2, e3 = series_from_est_or_cagr(nz(ebit0, 0.0), None, None, rev_cagr, g1, g2, g3)
    f0, f1, f2, f3 = series_from_est_or_cagr(nz(fcf0, 0.0), None, None, (eps_cagr or rev_cagr), g1, g2, g3)

    # Per-aktie FCF fallback
    if fcf_ps0 in (None, 0.0) and shares > 0:
        fcf_ps0 = f0 / shares
    # DPS-serie (utdelning) + DDM/ Yield-ankare
    if dps0 in (None, 0.0):
        dps0 = yahoo_dividend_ttm(tkr) or 0.0
    try:
        div_cagr = yahoo_dividend_cagr(tkr)
    except Exception:
        div_cagr = None
    if div_cagr is None:
        cands = [x for x in [eps_cagr, rev_cagr, g1] if x is not None]
        div_cagr = (min(cands) if cands else g1)
    d0, d1, d2, d3 = series_from_est_or_cagr(dps0, None, None, div_cagr, g1, g2, g3)

    # EV-multipelmetoder (pris/aktie)
    vals: Dict[str, Tuple[float,float,float,float]] = {}
    if r0 and shares > 0:
        vals["ev_sales"] = ev_multiple_from_series(r0, r1, r2, r3, ev_s_mult, ev_comp_rate, net_debt, shares)
    if e0 and shares > 0:
        vals["ev_ebitda"] = ev_multiple_from_series(e0, e1, e2, e3, ev_eb_mult, ev_comp_rate, net_debt, shares)
    if f0 and shares > 0:
        vals["ev_fcf"] = ev_multiple_from_series(f0, f1, f2, f3, ev_fcf_mult, ev_comp_rate, net_debt, shares)
    # DACF-approx (fallback EBITDA)
    if e0 and shares > 0:
        vals["ev_dacf"] = ev_multiple_from_series(e0, e1, e2, e3, dacf_mult, ev_comp_rate, net_debt, shares)

    # P-multiplar (per-aktie-serier)
    if fcf_ps0 and fcf_ps0 != 0.0:
        pf0, pf1, pf2, pf3 = series_from_est_or_cagr(fcf_ps0, None, None, (eps_cagr or rev_cagr), g1, g2, g3)
        vals["p_fcf"] = price_multiple_from_series(pf0, pf1, pf2, pf3, p_fcf_mult, p_comp_rate)

    def _add_p_series(key: str, base_ps: Optional[float], mult: float):
        if base_ps and base_ps > 0.0:
            s0, s1, s2, s3 = series_from_est_or_cagr(base_ps, None, None, g1, g1, g2, g3)
            vals[key] = price_multiple_from_series(s0, s1, s2, s3, mult, p_comp_rate)

    _add_p_series("p_b",   bv_ps,   p_b_mult)
    _add_p_series("p_tbv", tbv_ps,  p_tbv_mult)
    _add_p_series("p_nav", nav_ps,  p_nav_mult)
    _add_p_series("p_affo",affo_ps, p_affo_mult)
    _add_p_series("p_nii", nii_ps0, p_nii_mult)

    # Dividendmetoder
    # Yield-ankare (pris = DPS / yield), yield komprimeras (yield ner ⇒ pris upp)
    y1 = max(1e-6, yield_anchor * (1.0 - yield_comp_rate) ** 1)
    y2 = max(1e-6, yield_anchor * (1.0 - yield_comp_rate) ** 2)
    y3 = max(1e-6, yield_anchor * (1.0 - yield_comp_rate) ** 3)
    px_y0 = safe_div(d0, max(1e-6, yield_anchor), 0.0)
    px_y1 = safe_div(d1, y1, 0.0)
    px_y2 = safe_div(d2, y2, 0.0)
    px_y3 = safe_div(d3, y3, 0.0)
    vals["div_yield"] = (px_y0, px_y1, px_y2, px_y3)

    # DDM (Gordon) – använd N12M som "idag"-bas
    gL = min(max(div_cagr or 0.0, 0.0), float(ddm_g_cap))
    r_req = max(0.01, float(ddm_r))

    def _ddm(dps1, g, r):
        if r <= g:
            return 0.0
        return dps1 * (1.0 + g) / (r - g)

    d4 = d3 * (1.0 + (g1 if isinstance(g1, float) else float(g1)))
    vals["ddm"] = (_ddm(d1, gL, r_req), _ddm(d2, gL, r_req), _ddm(d3, gL, r_req), _ddm(d4, gL, r_req))

    # P/E-metoden
    t0, t1, t2, t3, note = pe_targets_from_estimates(
        price=px, pe_ttm=pe_ttm, pe_fwd=pe_fwd, pe_band=pe_band,
        eps_ttm=eps_ttm, eps_next1=eps1, eps_next2=eps2,
        eps_cagr=eps_cagr, pe_comp_rate=pe_comp_rate, g3=g3
    )
    vals["pe_hist_vs_eps"] = (t0, t1, t2, t3)

    # --- Fair-idag-blend  (TTM vs Nästa 12m) ---
    for k, (a0, a1, a2, a3) in list(vals.items()):
        a0b = (1.0 - today_blend_w) * float(a0) + today_blend_w * float(a1)
        vals[k] = (a0b, a1, a2, a3)

    # Primär metod
    has_fcf = (nz(f0, 0.0) != 0.0) or (nz(fcf_ps0, 0.0) != 0.0)
    has_eb  = nz(e0, 0.0) != 0.0
    pref = str(r.get("Preferred metod") or "AUTO")
    if pref != "AUTO" and pref in vals:
        primary = pref
    else:
        primary = choose_primary_method(bucket, "", "", tkr, has_fcf, has_eb)
        if primary not in vals or vals[primary] == (0.0,0.0,0.0,0.0):
            # fallback: ta bästa icke-noll metod
            nonzero = [(k, v) for k, v in vals.items() if any(float(nz(x,0.0)) != 0.0 for x in v)]
            primary = (nonzero[0][0] if nonzero else "pe_hist_vs_eps")

    # Plocka ut 1y bull/bear på primärens 1y-mål
    p0, p1, p2, p3 = vals[primary]
    b_up, b_dn = bull_bear(p1, bull_mult, bear_mult)
    upside = (safe_div(p0, px, 1.0) - 1.0) * 100.0 if px > 0 else 0.0

    return {
        "Ticker": tkr,
        "Namn": name,
        "Valuta": ccy,
        "Pris": float(px),
        "Primär metod": primary,
        "Fair idag": float(p0),
        "Fair 1y": float(p1),
        "Fair 2y": float(p2),
        "Fair 3y": float(p3),
        "Bull 1y": float(b_up),
        "Bear 1y": float(b_dn),
        "Upside_%": float(upside),
        "Alla metoder": vals,
        "DivSeries": (float(d0), float(d1), float(d2), float(d3)),
        "DivCAGR": float(div_cagr or 0.0),
        "PE_note": note,
    }

# -----------------------------------------------------------
# Hjälpare: Revenue-CAGR via Yahoo (enkel approx)
# -----------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=900)
def yahoo_revenue_cagr(ticker: str, years: int = 5) -> Optional[float]:
    """Approx: använd årliga income_stmt och beräkna CAGR på 'Total Revenue'."""
    try:
        df = _yf_annual_income_stmt(ticker)
        if df is None or df.empty or "Total Revenue" not in df.index:
            return None
        ser = df.loc["Total Revenue"].dropna()
        if ser.size < 2:
            return None
        # Nyast sist
        v = ser.values.astype(float)
        # Använd upp till 'years' intervall, annars kortare
        n = min(years, len(v) - 1)
        if n <= 0:
            return None
        v0 = float(v[-(n+1)])
        vN = float(v[-1])
        if v0 <= 0 or vN <= 0:
            return None
        return (vN / v0) ** (1.0 / n) - 1.0
    except Exception:
        return None

# -----------------------------
# Bygg analysvy (filtrerat Data)
# -----------------------------
rates_map = load_fx_from_sheet()
df_all = read_data_df()
mask = df_all["Bucket"].isin(pick_buckets)
if only_owned:
    mask &= (df_all["Antal aktier"].fillna(0).astype(float) > 0)
if only_watch:
    mask &= (df_all["Antal aktier"].fillna(0).astype(float) == 0)

view_df = df_all[mask].copy()
if view_df.empty:
    st.info("Inget att visa för valet av filter.")
    st.stop()

# -----------------------------
# Beräkna alla rader (med paus)
# -----------------------------
calc_rows: List[Dict[str, Any]] = []
for _, rr in view_df.iterrows():
    try:
        calc_rows.append(
            compute_methods_row(
                rr, use_finnhub,
                pe_comp_rate, ev_comp_rate, p_comp_rate,
                rate_limit_sleep, today_blend_w,
                yield_anchor, yield_comp_rate,
                ddm_r, ddm_g_cap,
                bull_mult, bear_mult
            )
        )
        time.sleep(rate_limit_sleep)
    except Exception as e:
        st.warning(f"{rr.get('Ticker')}: beräkning misslyckades ({e}).")

calc_df = pd.DataFrame(calc_rows)
if calc_df.empty:
    st.warning("Inga beräkningar kunde göras.")
    st.stop()

# -----------------------------
# Portföljhuvud — SEK & utdelning 12m
# -----------------------------
innehav_sek = []
utd_12m_net_sek = []

# DPS 1y per ticker (för summering)
_div1_map = {}
for _, r in calc_df.iterrows():
    ds = r.get("DivSeries")
    if ds and len(ds) >= 2:
        _div1_map[str(r["Ticker"]).upper()] = float(nz(ds[1], 0.0))

for _, r in view_df.iterrows():
    t   = (r.get("Ticker") or "").upper()
    cur = (r.get("Valuta") or "SEK")
    pris = float(nz(r.get("Last Price"), 0.0))
    antal = int(nz(r.get("Antal aktier"), 0))
    rate = _rate(cur)
    wht  = wht_rate(cur)

    innehav_sek.append(antal * pris * rate)

    d1_ps = float(nz(_div1_map.get(t, 0.0), 0.0))
    utd_net = antal * d1_ps * rate * (1.0 - wht)
    utd_12m_net_sek.append(utd_net)

tot_sek = float(sum(innehav_sek))
utd12   = float(sum(utd_12m_net_sek))
mth_nt  = utd12/12.0

ph1, ph2, ph3 = st.columns(3)
ph1.metric("Portföljvärde (SEK)", f"{tot_sek:,.0f} SEK".replace(",", " "))
ph2.metric("Förväntad utdelning 12m (SEK, netto)", f"{utd12:,.0f} SEK".replace(",", " "))
ph3.metric("Utdelning per månad (SEK, netto)", f"{mth_nt:,.0f} SEK".replace(",", " "))

st.markdown("---")
st.markdown("## 📈 Rangordning (störst uppsida →)")

# Bästa & näst bästa metod per rad
def rank_methods_by_upside(row: pd.Series) -> List[Dict[str, Any]]:
    """Sortera metoder på uppsida (Fair idag t0 vs pris)."""
    price = float(nz(row.get("Pris"), 0.0))
    out: List[Dict[str, Any]] = []
    methods = row.get("Alla metoder", {}) or {}
    for key, vals in methods.items():
        try:
            t0, t1, t2, t3 = vals
            t0f = float(nz(t0, 0.0))
            up  = (t0f/price - 1.0)*100.0 if price > 0 else float("-inf")
            out.append({
                "key": key,
                "label": METHOD_LABEL.get(key, key),
                "t0": t0f, "t1": float(nz(t1,0.0)), "t2": float(nz(t2,0.0)), "t3": float(nz(t3,0.0)),
                "up": up,
            })
        except Exception:
            continue
    out.sort(key=lambda d: d["up"], reverse=True)
    return out

def _best_methods(row: pd.Series) -> pd.Series:
    ranked = rank_methods_by_upside(row)
    best = ranked[0] if ranked else None
    second = ranked[1] if ranked and len(ranked) > 1 else None
    return pd.Series({
        "Bästa metod": (best["label"] if best else ""),
        "Bästa uppsida (%)": (round(best["up"], 1) if best else np.nan),
        "Näst bästa": (second["label"] if second else ""),
        "Näst bästa uppsida (%)": (round(second["up"], 1) if second else np.nan),
    })

extra_cols = calc_df.apply(_best_methods, axis=1)
calc_df = pd.concat([calc_df, extra_cols], axis=1)

rank_cols = [
    "Ticker","Namn","Valuta","Pris",
    "Primär metod",
    "Bästa metod","Bästa uppsida (%)",
    "Näst bästa","Näst bästa uppsida (%)",
    "Fair idag","Fair 1y","Fair 2y","Fair 3y",
    "Bull 1y","Bear 1y",
    "Upside_%"
]
st.dataframe(
    calc_df.sort_values("Upside_%", ascending=False)[rank_cols].reset_index(drop=True),
    use_container_width=True
)

# -----------------------------
# Kortvy per bolag
# -----------------------------
st.markdown("## 🎴 Snygg kortvy (rekommenderad metod per bolag)")

# Snabb lookup på originaldata (antal, valuta m.m.)
_view_by_ticker = {str(r.get("Ticker")).upper(): r for _, r in view_df.iterrows()}

def render_company_card(calc_row: pd.Series, base_row: pd.Series, fxrate: float):
    tk   = calc_row["Ticker"]
    name = calc_row.get("Namn") or ""
    ccy  = calc_row["Valuta"]
    px   = float(nz(calc_row["Pris"], 0.0))
    prim = calc_row["Primär metod"]
    label= METHOD_LABEL.get(prim, prim)

    vals = calc_row["Alla metoder"].get(prim, (0.0,0.0,0.0,0.0))
    t0, t1, t2, t3 = [float(nz(v,0.0)) for v in vals]
    bull, bear = float(nz(calc_row["Bull 1y"],0.0)), float(nz(calc_row["Bear 1y"],0.0))
    upside = (t0/px - 1.0)*100.0 if px>0 else 0.0

    antal = int(nz(base_row.get("Antal aktier"), 0))
    value_sek = antal * px * fxrate

    # Rankning av metoder efter uppsida
    ranked = rank_methods_by_upside(calc_row)
    top_m = ranked[0] if ranked else None
    sec_m = ranked[1] if ranked and len(ranked) > 1 else None

    # DPS-serie
    dps = calc_row.get("DivSeries") or (0.0, 0.0, 0.0, 0.0)
    d0, d1, d2, d3 = [float(nz(x,0.0)) for x in dps]
    wht = wht_rate(ccy)

    with st.container():
        st.markdown(
            f"### **{tk}** — {name}  \n"
            f"**Rekommenderad metod:** `{label}`  \n"
            f"<span style='color:gray'>{explain_method(prim, base_row)}</span>",
            unsafe_allow_html=True
        )

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Aktuell kurs", _cur(px, ccy))
        c2.metric("Fair idag",   _cur(t0, ccy), delta=_pct(upside))
        c3.metric("1 år",        _cur(t1, ccy))
        c4.metric("2 år",        _cur(t2, ccy))
        c5.metric("3 år",        _cur(t3, ccy))

        c6, c7, c8, c9 = st.columns(4)
        c6.metric("Bull 1 år", _cur(bull, ccy))
        c7.metric("Bear 1 år", _cur(bear, ccy))
        c8.metric("Antal aktier", f"{antal}")
        c9.metric("Värde (SEK)", f"{value_sek:,.0f} SEK".replace(",", " "))

        if top_m:
            s = f"**Bästa uppsida (Fair idag):** `{top_m['label']}` — {_cur(top_m['t0'], ccy)} ({_pct(top_m['up'])})"
            if sec_m:
                s += f"  \n**Näst bästa:** `{sec_m['label']}` — {_cur(sec_m['t0'], ccy)} ({_pct(sec_m['up'])})"
            st.markdown(s)
            if top_m["key"] != prim:
                st.caption(
                    f"Notis: heuristisk *primär metod* är `{label}`, "
                    f"men störst uppsida just nu ges av `{top_m['label']}`."
                )

        # Utdelningsöversikt
        with st.expander("📬 Utdelningsöversikt (prognos)"):
            dcol = st.columns(4)
            dcol[0].metric("DPS TTM", _cur(d0, ccy))
            dcol[1].metric("DPS 1 år", _cur(d1, ccy))
            dcol[2].metric("DPS 2 år", _cur(d2, ccy))
            dcol[3].metric("DPS 3 år", _cur(d3, ccy))

            y_br = (d0/px*100.0) if px>0 and d0>0 else 0.0
            y_nt = y_br * (1.0 - wht)
            ycol = st.columns(2)
            ycol[0].metric("Yield idag (brutto)", _pct(y_br))
            ycol[1].metric("Yield idag (netto)",  _pct(y_nt))

            gross = [antal*d1*fxrate, antal*d2*fxrate, antal*d3*fxrate]
            net   = [g*(1.0 - wht) for g in gross]
            dfu = pd.DataFrame({
                "År": ["1 år","2 år","3 år"],
                "Brutto (SEK)": [round(x, 0) for x in gross],
                "Netto (SEK)":  [round(x, 0) for x in net],
            })
            st.dataframe(dfu, use_container_width=True, hide_index=True)

        st.markdown("---")

# Rendera korten (sorterat på uppsida)
for _, row in calc_df.sort_values("Upside_%", ascending=False).iterrows():
    tk  = row["Ticker"]
    br  = _view_by_ticker.get(tk)
    rate= _rate(row["Valuta"])
    if br is None:
        continue
    render_company_card(row, br, rate)

# -----------------------------
# Detaljer per bolag (valfritt)
# -----------------------------
st.markdown("## 🔎 Detaljer per bolag")
with st.expander("Visa rådata (Alla metoder & inputs)"):
    st.dataframe(calc_df.drop(columns=["Alla metoder"]), use_container_width=True)
    st.caption("Kolumnen 'Alla metoder' innehåller kompletta prisserier per metod och visas i kortvyn.")
