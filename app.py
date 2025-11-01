# ============================================================
# BucketApp — värdering & riktkurser (Streamlit)
# app.py — Del 1/4: Imports, boot, hjälpfunktioner
# ============================================================

# ---- Standard ----
from __future__ import annotations
import os, sys, time, math, json, traceback, statistics
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

# ---- 3rd party ----
import pandas as pd
import numpy as np
import streamlit as st

# Nätanrop (Finnhub)
import requests

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# Yahoo Finance
try:
    import yfinance as yf
except Exception:
    yf = None  # hanteras i kod

# ---------------------------
# Fail-safe boot: alltid rubrik
# ---------------------------
st.set_page_config(page_title="BucketApp", layout="wide")
st.title("📊 BucketApp – värdering & riktkurser")

# Visa stacktrace på sidan om något smäller
def guard(fn, *, label: str = ""):
    try:
        return fn()
    except Exception:
        st.error(f"💥 Fel under uppstart {label}".strip())
        st.code(traceback.format_exc())
        st.stop()

# ---------------------------
# Säkra defaults (för NameError)
# ---------------------------
if "RATE_LIMIT_SLEEP" not in globals():
    RATE_LIMIT_SLEEP = 0.35

if "DEFAULT_WHT" not in globals():
    DEFAULT_WHT = {
        "SEK": 0.00,
        "USD": 0.15,
        "EUR": 0.15,
        "NOK": 0.25,
        "CAD": 0.15,
        "GBP": 0.10,
        "DKK": 0.27,
    }

# ---------------------------
# Små utils
# ---------------------------
def nz(x: Any, default: float) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none"):
            return default
        # ersätt svenskt kommatecken
        s = s.replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        return default

def to_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        s = str(x).strip().replace(" ", "").replace(",", ".")
        if s == "" or s.lower() in ("nan", "none"):
            return default
        return float(s)
    except Exception:
        return default

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        b = float(b)
        if b == 0:
            return default
        return float(a) / float(b)
    except Exception:
        return default

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def year_quarter(dt: Optional[date] = None) -> str:
    dt = dt or date.today()
    q = (dt.month - 1) // 3 + 1
    return f"{dt.year}Q{q}"

def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Valutaformat
def fmt_cur(x: float, ccy: str) -> str:
    try:
        return f"{float(x):.2f} {ccy}"
    except Exception:
        return f"{x} {ccy}"

def fmt_pct(x: float) -> str:
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "–"

# ============================================================
# app.py — Del 2/4
# Google Sheets, FX, dataladdning & sidopanel
# ============================================================

# ---------------------------
# Secrets & Google-auth
# ---------------------------
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _load_google_credentials_dict() -> Dict[str, Any]:
    # Tillåt både TOML-tabell (dict) och JSON-sträng
    if "GOOGLE_CREDENTIALS" in st.secrets:
        obj = st.secrets["GOOGLE_CREDENTIALS"]
        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except Exception:
                st.error("Fel: GOOGLE_CREDENTIALS i secrets är en sträng men inte giltig JSON.")
                st.stop()
        return _normalize_private_key(dict(obj))
    st.error("Saknar GOOGLE_CREDENTIALS i secrets.")
    st.stop()

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    creds_dict = _load_google_credentials_dict()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

def _sheet_id_from_url_or_id(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("http"):
        try:
            # URL-format: /d/<ID>/
            parts = s.split("/d/")[1].split("/")[0]
            return parts
        except Exception:
            return s
    return s

@st.cache_resource(show_spinner=False)
def open_spreadsheet(gc) -> gspread.Spreadsheet:
    sheet_url = st.secrets.get("SHEET_URL", "").strip()
    sheet_id = st.secrets.get("SHEET_ID", "").strip()
    if not sheet_id and sheet_url:
        sheet_id = _sheet_id_from_url_or_id(sheet_url)
    if not sheet_id:
        st.error("Saknar SHEET_URL eller SHEET_ID i secrets.")
        st.stop()
    try:
        return gc.open_by_key(sheet_id)
    except Exception as e:
        st.error("Kunde inte öppna Google Sheet. Kontrollera SHEET_URL/SHEET_ID och delning till service-kontot.")
        st.code(str(e))
        st.stop()

GC = guard(lambda: get_gspread_client(), label="(Google-auth)")
SPREADSHEET_HANDLE = guard(lambda: open_spreadsheet(GC), label="(Öppna Google Sheet)")

# ---------------------------
# Worksheet-hjälpare
# ---------------------------
WS_DATA_NAME   = "Data"
WS_RATES_NAME  = "Valutakurser"
WS_SETTINGS    = "Inställningar"
WS_HIST_NAME   = "Historik"

def _open_ws(name: str) -> gspread.Worksheet:
    try:
        return SPREADSHEET_HANDLE.worksheet(name)
    except Exception:
        # Skapa om saknas
        ws = SPREADSHEET_HANDLE.add_worksheet(title=name, rows=1000, cols=50)
        return ws

def ensure_hist_sheet() -> gspread.Worksheet:
    try:
        ws = SPREADSHEET_HANDLE.worksheet(WS_HIST_NAME)
    except Exception:
        ws = SPREADSHEET_HANDLE.add_worksheet(title=WS_HIST_NAME, rows=2000, cols=20)
        ws.update([[
            "Timestamp","YQ","Ticker","Currency","Last Price","Market Cap","EV","Shares Out",
            "Revenue TTM","EBITDA TTM","FCF TTM","Dividend/ps",
            "PE TTM","PE FWD","EV/Revenue","EV/EBITDA"
        ]])
    return ws

WS_HIST = guard(lambda: ensure_hist_sheet(), label="(Säkerställ Historik-blad)")

# ---------------------------
# Valutakurser
# ---------------------------
@st.cache_data(show_spinner=False, ttl=600)
def load_fx_from_sheet() -> Dict[str, float]:
    try:
        ws = _open_ws(WS_RATES_NAME)
        rows = ws.get_all_values()
        if not rows:
            return {"SEK": 1.0}
        # Förväntar kolumner: Valuta | SEK
        header = [c.strip() for c in rows[0]]
        data = rows[1:]
        m: Dict[str, float] = {"SEK": 1.0}
        if len(header) >= 2 and header[0].lower().startswith("valuta"):
            for r in data:
                if not r or len(r) < 2: 
                    continue
                ccy = (r[0] or "").strip().upper()
                rate = to_float(r[1], None)
                if ccy and rate:
                    m[ccy] = float(rate)
        else:
            # fallback: två kolumner ändå
            for r in data:
                if len(r) >= 2:
                    ccy = (r[0] or "").strip().upper()
                    rate = to_float(r[1], None)
                    if ccy and rate:
                        m[ccy] = float(rate)
        if "SEK" not in m: 
            m["SEK"] = 1.0
        return m
    except Exception:
        return {"SEK": 1.0}

def fx_to_sek(ccy: str, rates: Optional[Dict[str, float]] = None) -> float:
    rates = rates or load_fx_from_sheet()
    c = (ccy or "SEK").upper()
    return float(rates.get(c, 1.0 if c == "SEK" else 1.0))

@st.cache_data(show_spinner=False, ttl=600)
def read_settings_wht() -> Dict[str, float]:
    try:
        ws = _open_ws(WS_SETTINGS)
        rows = ws.get_all_values()
        if not rows:
            return DEFAULT_WHT
        header = [h.strip().lower() for h in rows[0]]
        data = rows[1:]
        m: Dict[str, float] = dict(DEFAULT_WHT)
        # Förväntar kolumner: Valuta | WHT
        col_val = None
        col_wht = None
        for i, h in enumerate(header):
            if "valuta" in h: col_val = i
            if "wht" in h or "källskatt" in h: col_wht = i
        for r in data:
            try:
                ccy = (r[col_val] if col_val is not None else "").strip().upper()
                w = to_float(r[col_wht], None) if col_wht is not None else None
                if ccy and w is not None:
                    m[ccy] = float(w)
            except Exception:
                continue
        return m
    except Exception:
        return dict(DEFAULT_WHT)

# ---------------------------
# Data-bladet som DataFrame
# ---------------------------
EXPECTED_COLS = [
    "Ticker","Bolagsnamn","Valuta","Bucket","Antal aktier",
    "Last Price","Market Cap","EV","Shares Out",
    "Revenue TTM","EBITDA TTM","FCF TTM","Dividend/ps",
    "PE TTM","PE FWD","EV/Revenue","EV/EBITDA",
    "BV/ps","TBV/ps","NAV/ps","AFFO/ps","FCF/ps","NII/ps",
    "Preferred metod",
    "G1","G2","G3","EPS_CAGR_5Y",
    "ev_s_mult","ev_eb_mult","ev_fcf_mult","dacf_mult",
    "p_fcf_mult","p_nav_mult","p_affo_mult","p_b_mult","p_tbv_mult","p_nii_mult",
]

@st.cache_data(show_spinner=False, ttl=120)
def read_data_df() -> pd.DataFrame:
    ws = _open_ws(WS_DATA_NAME)
    rows = ws.get_all_values()
    if not rows:
        # skapa tomt blad med header
        ws.update([EXPECTED_COLS])
        return pd.DataFrame(columns=EXPECTED_COLS)
    header = [h.strip() for h in rows[0]]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header)
    # säkerställ alla förväntade kolumner finns
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = ""
    return df[EXPECTED_COLS].copy()

# ---------------------------
# Sidopanel (alla reglage)
# ---------------------------
def _sidebar_block():
    with st.sidebar:
        st.header("⚙️ Inställningar")
        rate_limit_sleep = st.slider(
            "Paus mellan nätanrop (sek)", 0.10, 2.00, float(RATE_LIMIT_SLEEP), 0.05,
            help="Öka om du får rate limit från Yahoo/Finnhub/Sheets."
        )

        st.subheader("Datakällor")
        finnhub_key = st.secrets.get("FINNHUB_API_KEY", "")
        use_finnhub = st.toggle("Använd Finnhub-estimat", value=bool(finnhub_key))

        st.subheader("Kompression (multiplar)")
        pe_comp_rate = st.slider("P/E-komp/år", 0.00, 0.30, 0.08, 0.01, help="Sänker P/E varje år.")
        ev_comp_rate = st.slider("EV-multipel-komp/år", 0.00, 0.30, 0.10, 0.01)
        p_comp_rate  = st.slider("P-multipel-komp/år", 0.00, 0.30, 0.12, 0.01)

        today_blend_w = st.slider("Idag = blend(TTM vs N12M)", 0.00, 1.00, 0.50, 0.05,
                                  help="0=TTM, 1=Next12m, 0.5=rekommenderat")

        st.subheader("Utdelning")
        yield_anchor   = st.number_input("Målyield (brutto)", 0.01, 0.25, 0.08, 0.01)
        yield_comp_rate= st.slider("Yield-komp/år (lägre yield över tid)", 0.00, 0.40, 0.10, 0.01)
        ddm_r          = st.number_input("DDM: avkastningskrav r", 0.05, 0.25, 0.12, 0.01)
        ddm_g_cap      = st.number_input("DDM: max evig g", 0.00, 0.12, 0.06, 0.01)

        st.subheader("Bull/Bear (1 år)")
        bull_mult = st.number_input("Bull ×", 1.00, 2.50, 1.20, 0.05)
        bear_mult = st.number_input("Bear ×", 0.40, 1.00, 0.80, 0.05)

        st.subheader("Filter")
        df_tmp = read_data_df()
        buckets = sorted([b for b in df_tmp["Bucket"].fillna("").unique() if b])
        pick_buckets = st.multiselect("Visa buckets", buckets, default=buckets)
        only_owned = st.checkbox("Visa endast innehav (Antal aktier > 0)", value=False)
        only_watch = st.checkbox("Visa endast bevakningslista (Antal aktier = 0)", value=False)

        return {
            "rate_limit_sleep": float(rate_limit_sleep),
            "use_finnhub": bool(use_finnhub),
            "pe_comp_rate": float(pe_comp_rate),
            "ev_comp_rate": float(ev_comp_rate),
            "p_comp_rate": float(p_comp_rate),
            "today_blend_w": float(today_blend_w),
            "yield_anchor": float(yield_anchor),
            "yield_comp_rate": float(yield_comp_rate),
            "ddm_r": float(ddm_r),
            "ddm_g_cap": float(ddm_g_cap),
            "bull_mult": float(bull_mult),
            "bear_mult": float(bear_mult),
            "pick_buckets": pick_buckets,
            "only_owned": bool(only_owned),
            "only_watch": bool(only_watch),
        }

cfg = guard(lambda: _sidebar_block(), label="(Sidopanel)")

# Exponera parametrar globalt till Del 3/4–4/4
rate_limit_sleep = cfg["rate_limit_sleep"]
use_finnhub      = cfg["use_finnhub"]
pe_comp_rate     = cfg["pe_comp_rate"]
ev_comp_rate     = cfg["ev_comp_rate"]
p_comp_rate      = cfg["p_comp_rate"]
today_blend_w    = cfg["today_blend_w"]
yield_anchor     = cfg["yield_anchor"]
yield_comp_rate  = cfg["yield_comp_rate"]
ddm_r            = cfg["ddm_r"]
ddm_g_cap        = cfg["ddm_g_cap"]
bull_mult        = cfg["bull_mult"]
bear_mult        = cfg["bear_mult"]
pick_buckets     = cfg["pick_buckets"]
only_owned       = cfg["only_owned"]
only_watch       = cfg["only_watch"]

# ============================================================
# app.py — Del 3/4
# Datainsamling (Yahoo/Finnhub) & värderingsmotor
# ============================================================

# ---------------------------
# Yahoo Finance helpers
# ---------------------------
def _yf_ticker(ticker: str):
    if yf is None:
        return None
    try:
        return yf.Ticker(ticker)
    except Exception:
        return None

def _resolve_price(px: Optional[float], y_last: Optional[float] = None,
                   mcap: Optional[float] = None, shares: Optional[float] = None) -> Optional[float]:
    """Välj bästa pris: explicit px → y_last → mcap/shares → None."""
    for cand in (px, y_last, safe_div(mcap, shares, None)):
        try:
            if cand is None:
                continue
            v = float(cand)
            if v > 0:
                return v
        except Exception:
            continue
    return None

@st.cache_data(show_spinner=False, ttl=600)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    t = _yf_ticker(ticker)
    if t is None:
        return out
    try:
        # fast_info (snabbt och robust)
        fi = getattr(t, "fast_info", None)
        if fi:
            out["last_price"] = to_float(getattr(fi, "last_price", None), None)
            out["market_cap"] = to_float(getattr(fi, "market_cap", None), None)
            out["shares_out"] = to_float(getattr(fi, "shares", None), None)
            out["pe_forward"] = to_float(getattr(fi, "forward_pe", None), None)
        # info (kan vara långsammare)
        inf = getattr(t, "info", {}) or {}
        out["long_name"] = inf.get("longName") or ""
        out["short_name"] = inf.get("shortName") or ""
        out["enterprise_value"] = to_float(inf.get("enterpriseValue"), None)
        if out.get("pe_forward") is None:
            out["pe_forward"] = to_float(inf.get("forwardPE"), None)
        out["pe_ttm"] = to_float(inf.get("trailingPE"), None)
        # Multiplar
        out["ev_to_revenue"] = to_float(inf.get("enterpriseToRevenue"), None)
        out["ev_to_ebitda"]  = to_float(inf.get("enterpriseToEbitda"), None)
    except Exception:
        pass

    # TTM-remsor
    try:
        fin = t.financials or pd.DataFrame()
        is_ = t.income_stmt if hasattr(t, "income_stmt") else None
        # yfinance 0.2: t.income_stmt / t.cashflow / t.balance_sheet (annual & quarterly)
    except Exception:
        fin = pd.DataFrame()

    # Fång upp TTM via yfinance attribut
    try:
        out["revenue_ttm"] = to_float(getattr(t, "income_stmt_trailing_total_revenue", None), None)
    except Exception:
        pass
    try:
        out["ebitda_ttm"] = to_float(getattr(t, "income_stmt_trailing_ebitda", None), None)
    except Exception:
        pass
    try:
        # FCF = operatingCF - capex; yfinance saknar ofta direkt TTM → försök via cashflow history
        cf = t.cashflow or pd.DataFrame()
        if not cf.empty:
            ocf = to_float(cf.get("Total Cash From Operating Activities", pd.Series([None])).iloc[0], None)
            capex = to_float(cf.get("Capital Expenditures", pd.Series([None])).iloc[0], None)
            if ocf is not None and capex is not None:
                out["fcf_ttm"] = float(ocf) - float(capex)
    except Exception:
        pass

    # Utdelning per aktie TTM
    try:
        d = t.dividends
        if isinstance(d, pd.Series) and not d.empty:
            last_12m = d[d.index >= (pd.Timestamp.today() - pd.DateOffset(years=1))].sum()
            out["dividend_ps"] = float(last_12m) if pd.notnull(last_12m) else None
    except Exception:
        pass

    return out

# Annual income statement (för CAGR på omsättning)
@st.cache_data(show_spinner=False, ttl=900)
def _yf_annual_income_stmt(ticker: str) -> Optional[pd.DataFrame]:
    t = _yf_ticker(ticker)
    if t is None:
        return None
    try:
        df = t.income_stmt  # annual
        if df is None or df.empty:
            return None
        # rader som index
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        return df
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def yahoo_dividend_ttm(ticker: str) -> Optional[float]:
    t = _yf_ticker(ticker)
    if t is None:
        return None
    try:
        d = t.dividends
        if isinstance(d, pd.Series) and not d.empty:
            val = d[d.index >= (pd.Timestamp.today() - pd.DateOffset(years=1))].sum()
            return float(val) if pd.notnull(val) else None
    except Exception:
        return None
    return None

@st.cache_data(show_spinner=False, ttl=3600)
def yahoo_dividend_cagr(ticker: str, years: int = 5) -> Optional[float]:
    """Enkel utdelnings-CAGR på års-summor (senaste N helår)."""
    t = _yf_ticker(ticker)
    if t is None:
        return None
    try:
        d = t.dividends
        if not isinstance(d, pd.Series) or d.empty:
            return None
        # grupp per år
        ann = d.groupby(d.index.year).sum()
        if len(ann) < 2:
            return None
        # ta sista (nyast) och en som ligger N år bak om finns
        years = min(years, len(ann) - 1)
        v0 = float(ann.iloc[-(years+1)])
        vN = float(ann.iloc[-1])
        if v0 <= 0 or vN <= 0:
            return None
        return (vN / v0) ** (1.0 / years) - 1.0
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def yahoo_revenue_cagr(ticker: str, years: int = 5) -> Optional[float]:
    """CAGR på 'Total Revenue' från annual income statement."""
    try:
        df = _yf_annual_income_stmt(ticker)
        if df is None or df.empty or "Total Revenue" not in df.index:
            return None
        ser = df.loc["Total Revenue"].dropna()
        if ser.size < 2:
            return None
        v = ser.values.astype(float)
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

# ---------------------------
# Finnhub helpers (estimat)
# ---------------------------
def _fh_get(path: str, params: Dict[str, Any]) -> Optional[dict]:
    key = st.secrets.get("FINNHUB_API_KEY", "")
    if not key:
        return None
    url = f"https://finnhub.io/api/v1/{path}"
    p = dict(params)
    p["token"] = key
    try:
        r = requests.get(url, params=p, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_finnhub_estimates(ticker: str) -> Optional[Dict[str, Any]]:
    """Hämta enkla EPS/Revenue-estimat för nästa två år via Finnhub."""
    # Endpoint: /forecast?symbol=...
    j = _fh_get("forecast", {"symbol": ticker})
    if not j:
        return None
    # Finnhub svar varierar; plocka EPS/Revenue för nextY/nextY+1 om det finns
    out = {}
    try:
        # vissa svar: {"epsNextY":..., "epsNext2Y":..., "revenueNextY":..., "revenueNext2Y":...}
        out["eps_next1"] = to_float(j.get("epsNextY"), None)
        out["eps_next2"] = to_float(j.get("epsNext2Y"), None)
        out["rev_next1"] = to_float(j.get("revenueNextY"), None)
        out["rev_next2"] = to_float(j.get("revenueNext2Y"), None)
    except Exception:
        pass
    return out or None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_finnhub_metrics(ticker: str) -> Optional[Dict[str, Any]]:
    """Hämta historisk PE-fördelning (percentiler) om tillgängligt."""
    j = _fh_get("stock/metric", {"symbol": ticker, "metric": "valuation"})
    if not j or "metric" not in j:
        return None
    met = j["metric"] or {}
    # fabricera band p25/p50/p75 om de finns
    band = {}
    for k in ("peTTMPercentile25", "peTTMPercentile50", "peTTMPercentile75"):
        v = to_float(met.get(k), None)
        if v is None:
            continue
        if k.endswith("25"): band["p25"] = v
        if k.endswith("50"): band["p50"] = v
        if k.endswith("75"): band["p75"] = v
    return {"pe_band": band} if band else None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_finnhub_eps_quarterly(ticker: str) -> Optional[List[float]]:
    """Hämta (enkel) quarterly EPS-serie för CAGR-approx."""
    j = _fh_get("stock/earnings", {"symbol": ticker})
    if not j:
        return None
    # Försök plocka EPS (actual) i kronologisk ordning
    try:
        arr = []
        for it in sorted(j, key=lambda x: (x.get("year", 0), x.get("quarter", 0))):
            v = to_float(it.get("actual"), None)
            if v is None:
                continue
            arr.append(float(v))
        return arr if arr else None
    except Exception:
        return None

def eps_cagr_from_quarters(q_eps: Optional[List[float]]) -> Optional[float]:
    """Rough EPS-CAGR: jämför senaste 4q vs föregående 4q."""
    if not q_eps or len(q_eps) < 8:
        return None
    last4 = sum(q_eps[-4:])
    prev4 = sum(q_eps[-8:-4])
    if prev4 <= 0 or last4 <= 0:
        return None
    # ett år mellan blocken
    return (last4 / prev4) - 1.0

# ---------------------------
# Serier & multipel-beräkningar
# ---------------------------
def series_from_est_or_cagr(base0: float,
                            est1: Optional[float],
                            est2: Optional[float],
                            cagr: Optional[float],
                            g1: float, g2: float, g3: float) -> Tuple[float, float, float, float]:
    """Returnera (år0, år1, år2, år3). Använd estimat om finns, annars CAGR/g*."""
    a0 = float(nz(base0, 0.0))
    # år1
    if est1 is not None and est1 > 0:
        a1 = float(est1)
    else:
        g = float(cagr) if cagr is not None else float(g1)
        a1 = a0 * (1.0 + g)
    # år2
    if est2 is not None and est2 > 0:
        a2 = float(est2)
    else:
        g = float(cagr) if cagr is not None else float(g2)
        a2 = a1 * (1.0 + g)
    # år3
    g = float(cagr) if cagr is not None else float(g3)
    a3 = a2 * (1.0 + g)
    return a0, a1, a2, a3

def _compress(mult: float, comp_rate: float, years: int) -> float:
    """Sänk multipeln gejmässigt per år."""
    return float(mult) * ((1.0 - float(comp_rate)) ** years)

def ev_multiple_from_series(s0: float, s1: float, s2: float, s3: float,
                            mult0: float, comp_rate: float,
                            net_debt: float, shares_out: float) -> Tuple[float, float, float, float]:
    """EV-baserade mål → pris/aktie."""
    def _to_px(s, y):  # y = år (0..3)
        mult = _compress(mult0, comp_rate, y)
        ev_target = float(s) * float(mult)
        eq_val = ev_target - float(net_debt)
        return safe_div(eq_val, shares_out, 0.0)
    return _to_px(s0,0), _to_px(s1,1), _to_px(s2,2), _to_px(s3,3)

def price_multiple_from_series(ps0: float, ps1: float, ps2: float, ps3: float,
                               mult0: float, comp_rate: float) -> Tuple[float, float, float, float]:
    def _to_px(ps, y):
        mult = _compress(mult0, comp_rate, y)
        return float(ps) * float(mult)
    return _to_px(ps0,0), _to_px(ps1,1), _to_px(ps2,2), _to_px(ps3,3)

# ---------------------------
# P/E-ankare & målkurs
# ---------------------------
def _anchor_pe(pe_ttm: Optional[float], pe_fwd: Optional[float], pe_band: Dict[str, Any]) -> float:
    cands = []
    for v in (pe_ttm, pe_fwd, (pe_band or {}).get("p50")):
        v = to_float(v, None)
        if v and v > 0:
            cands.append(v)
    if not cands:
        return 20.0
    # median ger robust ankare
    try:
        return float(statistics.median(cands))
    except Exception:
        return float(sum(cands)/len(cands))

def pe_targets_from_estimates(*,
                              price: float,
                              pe_ttm: Optional[float],
                              pe_fwd: Optional[float],
                              pe_band: Optional[Dict[str, Any]],
                              eps_ttm: Optional[float],
                              eps_next1: Optional[float],
                              eps_next2: Optional[float],
                              eps_cagr: Optional[float],
                              pe_comp_rate: float,
                              g3: float) -> Tuple[float,float,float,float,str]:
    """
    Returnerar (idag, 1y, 2y, 3y, note).
    Idag = blend(TTM_eps vs N12M_eps) men den slutliga blenden görs utanför i Del 4.
    Här ger vi riktkurser baserat på EPS-serien * komprimerad ankarmultipel.
    """
    anchor0 = _anchor_pe(pe_ttm, pe_fwd, pe_band or {})
    # EPS-serie
    if eps_ttm is None or eps_ttm <= 0:
        # härled från price / pe_ttm om möjligt
        if pe_ttm and pe_ttm > 0 and price > 0:
            eps_ttm = price / pe_ttm
    if eps_ttm is None or eps_ttm <= 0:
        # utan EPS_ttm kan vi inte räkna P/E idag – sätt rimligt litet värde
        eps_ttm = 0.0001

    # EPS år1/år2 – använd estimat, annars CAGR från ttm
    if eps_next1 is None or eps_next1 <= 0:
        g = eps_cagr if eps_cagr is not None else 0.15
        eps_next1 = eps_ttm * (1.0 + g)
    if eps_next2 is None or eps_next2 <= 0:
        g = eps_cagr if eps_cagr is not None else 0.12
        eps_next2 = eps_next1 * (1.0 + g)
    eps3 = eps_next2 * (1.0 + (eps_cagr if eps_cagr is not None else g3))

    def _px(eps, y):
        mult = _compress(anchor0, pe_comp_rate, y)
        return float(eps) * float(mult)

    t0 = _px(eps_ttm, 0)
    t1 = _px(eps_next1, 1)
    t2 = _px(eps_next2, 2)
    t3 = _px(eps3, 3)
    note = f"pe_anchor(pe_ttm={pe_ttm}, pe_fwd={pe_fwd}, band={pe_band}, anchor0~{round(anchor0,2)})"
    return t0, t1, t2, t3, note

# ---------------------------
# Heuristik: välj primär metod
# ---------------------------
def choose_primary_method(bucket: str,
                          sector: str,
                          industry: str,
                          ticker: str,
                          has_fcf: bool,
                          has_ebitda: bool) -> str:
    b = (bucket or "").lower()
    if "utdel" in b or "income" in b:
        # REIT/BDC/Fond-liknande prior
        return "p_affo"  # ofta fallbackar vi om värden saknas
    if "bank" in industry.lower():
        return "p_tbv"
    if not has_fcf and not has_ebitda:
        return "ev_sales"
    if has_fcf:
        return "ev_fcf"
    return "ev_ebitda"

def bull_bear(base_1y: float, bull_mult: float, bear_mult: float) -> Tuple[float, float]:
    return float(base_1y)*float(bull_mult), float(base_1y)*float(bear_mult)

# ============================================================
# app.py — Del 4/4
# UI-rendering, beräkningar per rad, ranking & snapshot
# ============================================================

# ---------------------------
# Snapshot till fliken "Historik"
# ---------------------------
def save_quarter_snapshot(tkr: str, snap: Dict[str, Any]):
    try:
        row = [
            now_iso(), year_quarter(),
            tkr, snap.get("ccy", ""),
            nz(snap.get("price"), 0.0),
            nz(snap.get("mcap"), 0.0),
            nz(snap.get("ev"), 0.0),
            nz(snap.get("shares_out"), 0.0),
            nz(snap.get("revenue_ttm"), 0.0),
            nz(snap.get("ebitda_ttm"), 0.0),
            nz(snap.get("fcf_ttm"), 0.0),
            nz(snap.get("dividend_ps"), 0.0),
            nz(snap.get("pe_ttm"), 0.0),
            nz(snap.get("pe_fwd"), 0.0),
            nz(snap.get("ev_to_revenue"), 0.0),
            nz(snap.get("ev_to_ebitda"), 0.0),
        ]
        WS_HIST.append_row(row, value_input_option="USER_ENTERED")
        time.sleep(rate_limit_sleep)
        return True
    except Exception:
        st.warning("Kunde inte spara kvartalssnapshot (Historik).")
        return False

# ---------------------------
# Beräkning per bolag
# ---------------------------
def compute_company_row(row: pd.Series) -> Dict[str, Any]:
    tkr   = (row.get("Ticker") or "").strip().upper()
    name  = (row.get("Bolagsnamn") or "").strip() or tkr
    ccy   = (row.get("Valuta") or "USD").strip().upper()
    buck  = (row.get("Bucket") or "").strip()
    shares_owned = nz(row.get("Antal aktier"), 0.0)

    # Multiplar (kan vara tomma → defaultas nedan)
    ev_s_mult   = to_float(row.get("ev_s_mult"), None)
    ev_eb_mult  = to_float(row.get("ev_eb_mult"), None)
    ev_fcf_mult = to_float(row.get("ev_fcf_mult"), None)
    dacf_mult   = to_float(row.get("dacf_mult"), None)
    p_fcf_mult  = to_float(row.get("p_fcf_mult"), None)
    p_nav_mult  = to_float(row.get("p_nav_mult"), None)
    p_affo_mult = to_float(row.get("p_affo_mult"), None)
    p_b_mult    = to_float(row.get("p_b_mult"), None)
    p_tbv_mult  = to_float(row.get("p_tbv_mult"), None)
    p_nii_mult  = to_float(row.get("p_nii_mult"), None)

    # Tillväxtinput
    g1 = nz(row.get("G1"), 0.20)
    g2 = nz(row.get("G2"), 0.16)
    g3 = nz(row.get("G3"), 0.12)

    # Läs ev. siffror från Sheet
    price_sheet   = to_float(row.get("Last Price"), None)
    mcap_sheet    = to_float(row.get("Market Cap"), None)
    ev_sheet      = to_float(row.get("EV"), None)
    shares_out    = to_float(row.get("Shares Out"), None)
    rev_ttm_sheet = to_float(row.get("Revenue TTM"), None)
    ebitda_ttm_sh = to_float(row.get("EBITDA TTM"), None)
    fcf_ttm_sheet = to_float(row.get("FCF TTM"), None)
    div_ps_sheet  = to_float(row.get("Dividend/ps"), None)
    pe_ttm_sheet  = to_float(row.get("PE TTM"), None)
    pe_fwd_sheet  = to_float(row.get("PE FWD"), None)
    ev_rev_sheet  = to_float(row.get("EV/Revenue"), None)
    ev_ebit_sheet = to_float(row.get("EV/EBITDA"), None)

    # Per-aktie data (för P/x-metoder)
    bv_ps   = to_float(row.get("BV/ps"), None)
    tbv_ps  = to_float(row.get("TBV/ps"), None)
    nav_ps  = to_float(row.get("NAV/ps"), None)
    affo_ps = to_float(row.get("AFFO/ps"), None)
    fcf_ps  = to_float(row.get("FCF/ps"), None)
    nii_ps  = to_float(row.get("NII/ps"), None)

    # ---- Yahoo snapshot (fyll luckor) ----
    y = fetch_yahoo_snapshot(tkr)
    price = _resolve_price(price_sheet, y.get("last_price"), y.get("market_cap"), y.get("shares_out"))
    mcap  = mcap_sheet or y.get("market_cap")
    ev    = ev_sheet or y.get("enterprise_value") or None
    if ev is None and mcap is not None and price is not None and y.get("shares_out"):
        # fallback: om we missar EV men har nettoskuld via diff (saknas) → lämna None
        ev = None
    shares_out = shares_out or y.get("shares_out")
    pe_ttm = pe_ttm_sheet or y.get("pe_ttm")
    pe_fwd = pe_fwd_sheet or y.get("pe_forward")
    ev_to_rev = ev_rev_sheet or y.get("ev_to_revenue")
    ev_to_eb  = ev_ebit_sheet or y.get("ev_to_ebitda")
    rev_ttm = rev_ttm_sheet or y.get("revenue_ttm")
    ebitda_ttm = ebitda_ttm_sh or y.get("ebitda_ttm")
    fcf_ttm = fcf_ttm_sheet or y.get("fcf_ttm")
    div_ps = div_ps_sheet or y.get("dividend_ps")

    # Defaults på multiplar om saknas
    if ev_s_mult is None:   ev_s_mult = ev_to_rev or 6.0
    if ev_eb_mult is None:  ev_eb_mult = ev_to_eb or 12.0
    if ev_fcf_mult is None: ev_fcf_mult = 20.0
    if dacf_mult is None:   dacf_mult = ev_eb_mult  # DACF≈EBITDA om vi saknar annat
    if p_fcf_mult is None:  p_fcf_mult = 20.0
    if p_nav_mult is None:  p_nav_mult = 1.10
    if p_affo_mult is None: p_affo_mult = 12.0
    if p_b_mult is None:    p_b_mult = 2.0
    if p_tbv_mult is None:  p_tbv_mult = 1.6
    if p_nii_mult is None:  p_nii_mult = 10.0

    # EV-komponenter
    net_debt = None
    if ev is not None and mcap is not None:
        net_debt = float(ev) - float(mcap)
    else:
        net_debt = 0.0
    shares_out = nz(shares_out, 0.0)

    # ---- Estimat & band via Finnhub ----
    pe_band = {}
    eps_next1 = eps_next2 = None
    rev_next1 = rev_next2 = None
    eps_cagr = None
    if use_finnhub:
        est = fetch_finnhub_estimates(tkr) or {}
        eps_next1 = est.get("eps_next1")
        eps_next2 = est.get("eps_next2")
        rev_next1 = est.get("rev_next1")
        rev_next2 = est.get("rev_next2")
        met = fetch_finnhub_metrics(tkr) or {}
        pe_band = met.get("pe_band", {})
        qeps = fetch_finnhub_eps_quarterly(tkr)
        eps_cagr = eps_cagr_from_quarters(qeps)

    # CAGR fallback (omsättning)
    rev_cagr = yahoo_revenue_cagr(tkr, years=5)
    if rev_cagr is None:
        rev_cagr = g1  # nödfall: använd G1 som uppskattning

    # EPS TTM (om vi saknar – härled via price/PE_TTM)
    eps_ttm = None
    if pe_ttm and pe_ttm > 0 and price:
        eps_ttm = float(price) / float(pe_ttm)

    # ---- Serier (Revenue / EBITDA / FCF) ----
    rev0, rev1, rev2, rev3 = series_from_est_or_cagr(
        rev_ttm or 0.0, rev_next1, rev_next2, rev_cagr, g1, g2, g3
    )
    # EBITDA antag ≈ växer med rev-cagr om vi saknar estimat
    e0, e1, e2, e3 = series_from_est_or_cagr(
        ebitda_ttm or 0.0, None, None, rev_cagr, g1, g2, g3
    )
    f0, f1, f2, f3 = series_from_est_or_cagr(
        fcf_ttm or 0.0, None, None, rev_cagr, g1, g2, g3
    )

    # ---- P/E-baserad riktkurs ----
    t0_pe, t1_pe, t2_pe, t3_pe, pe_note = pe_targets_from_estimates(
        price=nz(price, 0.0),
        pe_ttm=pe_ttm,
        pe_fwd=pe_fwd,
        pe_band=pe_band,
        eps_ttm=eps_ttm,
        eps_next1=eps_next1,
        eps_next2=eps_next2,
        eps_cagr=eps_cagr if eps_cagr is not None else rev_cagr,
        pe_comp_rate=pe_comp_rate,
        g3=g3,
    )

    # Idag = blend(TTM vs N12M) för P/E: blenda t0,t1
    today_from_pe = (1.0 - today_blend_w) * t0_pe + today_blend_w * t1_pe

    # ---- EV-baserade mål (→ pris per aktie) ----
    # Om shares_out saknas kan vi inte omvandla EV → pris: ge 0:or
    if shares_out <= 0:
        ev_sales_today = ev_sales_1 = ev_sales_2 = ev_sales_3 = 0.0
        ev_eb_today = ev_eb_1 = ev_eb_2 = ev_eb_3 = 0.0
        ev_fcf_today = ev_fcf_1 = ev_fcf_2 = ev_fcf_3 = 0.0
        ev_dacf_today = ev_dacf_1 = ev_dacf_2 = ev_dacf_3 = 0.0
    else:
        ev_sales_today, ev_sales_1, ev_sales_2, ev_sales_3 = ev_multiple_from_series(
            rev0, rev1, rev2, rev3, ev_s_mult, ev_comp_rate, net_debt, shares_out
        )
        ev_eb_today, ev_eb_1, ev_eb_2, ev_eb_3 = ev_multiple_from_series(
            e0, e1, e2, e3, ev_eb_mult, ev_comp_rate, net_debt, shares_out
        )
        ev_fcf_today, ev_fcf_1, ev_fcf_2, ev_fcf_3 = ev_multiple_from_series(
            f0, f1, f2, f3, ev_fcf_mult, ev_comp_rate, net_debt, shares_out
        )
        # DACF ≈ EBITDA här
        ev_dacf_today, ev_dacf_1, ev_dacf_2, ev_dacf_3 = ev_multiple_from_series(
            e0, e1, e2, e3, dacf_mult, ev_comp_rate, net_debt, shares_out
        )

    # ---- Price × per-share ----
    def _px_per_share(base_ps, mult0):
        if base_ps is None or base_ps <= 0:
            return (0.0, 0.0, 0.0, 0.0)
        a0, a1, a2, a3 = series_from_est_or_cagr(base_ps, None, None, rev_cagr, g1, g2, g3)
        return price_multiple_from_series(a0, a1, a2, a3, mult0, p_comp_rate)

    p_fcf_today, p_fcf_1, p_fcf_2, p_fcf_3 = _px_per_share(fcf_ps, p_fcf_mult)
    p_nav_today, p_nav_1, p_nav_2, p_nav_3 = _px_per_share(nav_ps, p_nav_mult)
    p_affo_today, p_affo_1, p_affo_2, p_affo_3 = _px_per_share(affo_ps, p_affo_mult)
    p_b_today, p_b_1, p_b_2, p_b_3       = _px_per_share(bv_ps, p_b_mult)
    p_tbv_today, p_tbv_1, p_tbv_2, p_tbv_3= _px_per_share(tbv_ps, p_tbv_mult)
    p_nii_today, p_nii_1, p_nii_2, p_nii_3= _px_per_share(nii_ps, p_nii_mult)

    # ---- Sammanställ metodtabell ----
    tbl = pd.DataFrame([
        ["pe_hist_vs_eps", round(today_from_pe, 4), round(t1_pe,4), round(t2_pe,4), round(t3_pe,4)],
        ["ev_sales",       round(ev_sales_today,4),  round(ev_sales_1,4), round(ev_sales_2,4), round(ev_sales_3,4)],
        ["ev_ebitda",      round(ev_eb_today,4),     round(ev_eb_1,4),    round(ev_eb_2,4),    round(ev_eb_3,4)],
        ["ev_dacf",        round(ev_dacf_today,4),   round(ev_dacf_1,4),  round(ev_dacf_2,4),  round(ev_dacf_3,4)],
        ["ev_fcf",         round(ev_fcf_today,4),    round(ev_fcf_1,4),   round(ev_fcf_2,4),   round(ev_fcf_3,4)],
        ["p_fcf",          round(p_fcf_today,4),     round(p_fcf_1,4),    round(p_fcf_2,4),    round(p_fcf_3,4)],
        ["p_nav",          round(p_nav_today,4),     round(p_nav_1,4),    round(p_nav_2,4),    round(p_nav_3,4)],
        ["p_affo",         round(p_affo_today,4),    round(p_affo_1,4),   round(p_affo_2,4),   round(p_affo_3,4)],
        ["p_b",            round(p_b_today,4),       round(p_b_1,4),      round(p_b_2,4),      round(p_b_3,4)],
        ["p_tbv",          round(p_tbv_today,4),     round(p_tbv_1,4),    round(p_tbv_2,4),    round(p_tbv_3,4)],
        ["p_nii",          round(p_nii_today,4),     round(p_nii_1,4),    round(p_nii_2,4),    round(p_nii_3,4)],
    ], columns=["Metod","Idag","1 år","2 år","3 år"])

    # primär metod
    has_fcf = (fcf_ttm or fcf_ps)
    has_eb  = (ebitda_ttm is not None and ebitda_ttm > 0)
    prim = choose_primary_method(buck, "", "", tkr, bool(has_fcf), bool(has_eb))

    # primära mål
    prim_row = tbl[tbl["Metod"] == prim].iloc[0] if prim in set(tbl["Metod"]) else tbl.iloc[0]
    prim_today, prim_1y, prim_2y, prim_3y = float(prim_row["Idag"]), float(prim_row["1 år"]), float(prim_row["2 år"]), float(prim_row["3 år"])

    bull1, bear1 = bull_bear(prim_1y, bull_mult, bear_mult)

    # ---- Utdelning (brutto → netto, i SEK) ----
    wht_map = read_settings_wht()
    wht = float(wht_map.get(ccy, DEFAULT_WHT.get(ccy, 0.15)))
    div_ttm = div_ps or 0.0
    div1 = div_ttm * (1.0 + (yahoo_dividend_cagr(tkr, years=5) or 0.05))
    div2 = div1 * (1.0 + (yahoo_dividend_cagr(tkr, years=5) or 0.05))
    div3 = div2 * (1.0 + (yahoo_dividend_cagr(tkr, years=5) or 0.05))

    fx = fx_to_sek(ccy)
    holding_value_sek = nz(price,0.0) * shares_owned * fx
    next_div1_net_sek = div1 * (1.0 - wht) * shares_owned * fx
    next_div2_net_sek = div2 * (1.0 - wht) * shares_owned * fx
    next_div3_net_sek = div3 * (1.0 - wht) * shares_owned * fx

    # Uppsida %
    up_today = safe_div(prim_today - nz(price,0.0), nz(price,1.0), 0.0) * 100.0
    up_1y    = safe_div(prim_1y    - nz(price,0.0), nz(price,1.0), 0.0) * 100.0

    # Sanity-rad
    sanity_tags = []
    if price and price > 0: sanity_tags.append("price ok")
    if eps_ttm and eps_ttm > 0: sanity_tags.append("eps_ttm ok")
    if rev_ttm and rev_ttm > 0: sanity_tags.append("rev_ttm ok")
    if ebitda_ttm and ebitda_ttm > 0: sanity_tags.append("ebitda_ttm ok")
    if shares_out and shares_out > 0: sanity_tags.append("shares ok")

    return {
        "ticker": tkr,
        "name": name,
        "bucket": buck,
        "ccy": ccy,
        "price": price or 0.0,
        "mcap": mcap,
        "ev": ev,
        "shares_out": shares_out,
        "revenue_ttm": rev_ttm,
        "ebitda_ttm": ebitda_ttm,
        "fcf_ttm": fcf_ttm,
        "dividend_ps": div_ps,
        "pe_ttm": pe_ttm,
        "pe_fwd": pe_fwd,
        "ev_to_revenue": ev_to_rev,
        "ev_to_ebitda": ev_to_eb,
        "pe_note": pe_note,
        "table": tbl,
        "primary": prim,
        "prim_today": prim_today,
        "prim_1y": prim_1y,
        "prim_2y": prim_2y,
        "prim_3y": prim_3y,
        "bull_1y": bull1,
        "bear_1y": bear1,
        "holding_value_sek": holding_value_sek,
        "next_div": {
            "gross_ps_ttm": div_ttm,
            "y1_ps": div1, "y2_ps": div2, "y3_ps": div3,
            "net_y1_sek": next_div1_net_sek,
            "net_y2_sek": next_div2_net_sek,
            "net_y3_sek": next_div3_net_sek,
            "wht": wht,
        },
        "up_today_pct": up_today,
        "up_1y_pct": up_1y,
        "sanity": ", ".join(sanity_tags),
    }

# ---------------------------
# UI: Ladda data, filtrera, räkna
# ---------------------------
st.subheader("🔎 Rangordning (störst uppsida →)")
df_all = guard(lambda: read_data_df(), label="(Läs Data)")

# Filtrera
mask = df_all["Bucket"].isin(pick_buckets) if pick_buckets else np.ones(len(df_all), dtype=bool)
if only_owned:
    mask &= df_all["Antal aktier"].apply(lambda x: nz(x,0)>0)
if only_watch:
    mask &= df_all["Antal aktier"].apply(lambda x: nz(x,0)==0)

df_f = df_all[mask].copy()
if df_f.empty:
    st.info("Inga rader matchade filtret.")
    st.stop()

results: List[Dict[str, Any]] = []
portfolio_value_sek = 0.0

progress = st.progress(0.0, text="Beräknar...")
for i, (_, r) in enumerate(df_f.iterrows(), start=1):
    res = compute_company_row(r)
    results.append(res)
    portfolio_value_sek += res["holding_value_sek"]
    progress.progress(i/len(df_f), text=f"Beräknar… ({i}/{len(df_f)})")
    time.sleep(rate_limit_sleep)

# Rankingtabell
rank_rows = []
for x in results:
    rank_rows.append({
        "Ticker": x["ticker"],
        "Namn": x["name"],
        "Bucket": x["bucket"],
        "Valuta": x["ccy"],
        "Pris": round(x["price"], 2),
        "Primär metod": x["primary"],
        "FV idag": round(x["prim_today"], 2),
        "FV 1 år": round(x["prim_1y"], 2),
        "Uppsida idag (%)": round(x["up_today_pct"], 1),
        "Uppsida 1 år (%)": round(x["up_1y_pct"], 1),
    })
rank_df = pd.DataFrame(rank_rows).sort_values(by="Uppsida idag (%)", ascending=False).reset_index(drop=True)
st.dataframe(rank_df, use_container_width=True, height=300)

# Portföljsummering (SEK)
st.caption(f"💼 Portföljvärde (SEK, alla visade rader): **{portfolio_value_sek:,.0f}**")

# Knapp: spara snapshot för alla visade
if st.button("💾 Spara kvartalssnapshot för visade bolag", use_container_width=True):
    ok = 0
    for x in results:
        if save_quarter_snapshot(x["ticker"], x):
            ok += 1
    st.success(f"Sparade {ok} rader till fliken {WS_HIST_NAME}.")

st.markdown("---")

# ---------------------------
# Kort-vy per bolag + detaljer
# ---------------------------
st.header("🧭 Detaljer per bolag (alla värderingsmetoder)")

for x in results:
    with st.expander(f"{x['ticker']} • {x['name']} • {x['bucket']}"):
        subc1, subc2 = st.columns([2,1])
        with subc1:
            st.markdown(
                f"**Valuta:** {x['ccy']} • **Pris:** {x['price']:.2f} • "
                f"**Primär metod:** `{x['primary']}`"
            )
            st.caption(f"Sanity: {x['sanity']}")
            st.dataframe(x["table"], use_container_width=True, height=260)

            # P/E-band-anteckning om vi lyckades få ett band
            st.caption(f"P/E-ankare: {x['pe_note']}")

        with subc2:
            st.subheader("🎯 Primär riktkurs")
            st.metric("Idag",   fmt_cur(x["prim_today"], x["ccy"]), delta=f"{x['up_today_pct']:.1f}% uppsida")
            st.metric("1 år",   fmt_cur(x["prim_1y"], x["ccy"]))
            st.metric("2 år",   fmt_cur(x["prim_2y"], x["ccy"]))
            st.metric("3 år",   fmt_cur(x["prim_3y"], x["ccy"]))
            b1, b2 = bull_bear(x["prim_1y"], bull_mult, bear_mult)
            st.caption(f"Bull 1 år: {b1:.2f} • Bear 1 år: {b2:.2f} ({x['ccy']})")

            st.subheader("💰 Utdelning (netto, SEK)")
            nd = x["next_div"]
            st.write(f"• Nästa år: **{nd['net_y1_sek']:.0f} SEK**")
            st.write(f"• 2 år: **{nd['net_y2_sek']:.0f} SEK**")
            st.write(f"• 3 år: **{nd['net_y3_sek']:.0f} SEK**")
            st.caption(f"Källskatt: {int(nd['wht']*100)}% • Antal aktier: {int(nz(df_all.loc[df_all['Ticker']==x['ticker'],'Antal aktier'].iloc[0],0))}")

            st.subheader("🧾 Innehavsvärde")
            st.write(f"Totalt värde nu: **{x['holding_value_sek']:.0f} SEK**")

        st.markdown("---")
