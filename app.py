# app.py — Del 1/5
# ============================================================
# Bas: Streamlit-app för fair value / riktkurser / portfölj
# Lagring: Google Sheets (Data, Resultat, Valutakurser, Settings, Snapshot)
# Hämtning: Yahoo (yfinance) + (valfritt) Finnhub för EPS 1–2y
# Robust: backoff mot Sheets, pris-fallbacks, rimliga cappar på tillväxt
# ============================================================

from __future__ import annotations
import os, json, math, time
from typing import Any, Dict, List, Optional, Tuple
from collections.abc import Mapping
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

import yfinance as yf
import gspread
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import WorksheetNotFound, APIError
from google.oauth2.service_account import Credentials

# =========================
# UI & Grundinställningar
# =========================
st.set_page_config(page_title="Aktieanalys & riktkurser", layout="wide")
st.markdown("<style>section.main > div {max-width: 1400px;}</style>", unsafe_allow_html=True)

APP_TITLE       = "Aktieanalys och investeringsförslag"
DATA_TITLE      = "Data"
FX_TITLE        = "Valutakurser"
SETTINGS_TITLE  = "Settings"
RESULT_TITLE    = "Resultat"
SNAPSHOT_TITLE  = "Snapshot"

DEFAULT_BUCKETS = [
    "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
    "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning"
]

# =========================
# Små hjälpare
# =========================
def now_stamp() -> str:
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _env_or_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(key)
    if v: return v
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def guard(fn, label: str = ""):
    try:
        return fn()
    except Exception as e:
        st.error(f"💥 Fel {label}\n\n{e}")
        raise

def _with_backoff(callable_fn, *args, **kwargs):
    """Backoff för gspread 429/5xx."""
    delay = 0.6
    for i in range(6):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            code = getattr(e, "response", None).status_code if hasattr(e, "response") else None
            if code in (429, 500, 502, 503, 504):
                time.sleep(delay)
                delay *= 1.6
                continue
            raise
        except Exception:
            if i == 5: raise
            time.sleep(delay)
            delay *= 1.6

def _f(x) -> Optional[float]:
    """Robust float – tolkar '1 234,56' och returnerar None för tomt/NaN."""
    try:
        if x is None: return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            if s == "": return None
            v = float(s)
        else:
            v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None

def _pos(x) -> Optional[float]:
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(*vals):
    """Första icke-None/icke-NaN i listan."""
    for v in vals:
        if v is None: 
            continue
        try:
            if isinstance(v, float) and v != v:
                continue
        except Exception:
            pass
        return v
    return None

# =========================
# Google Sheets Auth
# =========================
@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    """
    Skapa gspread Client från GOOGLE_CREDENTIALS.
    Stöd: Mapping/AttrDict, str (JSON), bytes/bytearray.
    """
    raw = _env_or_secret("GOOGLE_CREDENTIALS")
    if raw is None:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i secrets eller env.")

    if isinstance(raw, Mapping):
        try:
            creds_dict = dict(raw)
        except Exception:
            creds_dict = json.loads(json.dumps(raw))
    elif isinstance(raw, (bytes, bytearray)):
        creds_dict = json.loads(raw.decode("utf-8"))
    elif isinstance(raw, str):
        creds_dict = json.loads(raw)
    else:
        try:
            creds_dict = raw.to_dict()
        except Exception as e:
            raise TypeError(f"GOOGLE_CREDENTIALS oväntad typ: {type(raw)}") from e

    creds_dict = _normalize_private_key(creds_dict)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppnar spreadsheet via SHEET_URL eller SHEET_ID (trimmar whitespace)."""
    sheet_url = _env_or_secret("SHEET_URL")
    sheet_id  = _env_or_secret("SHEET_ID")
    if sheet_url and sheet_url.strip():
        return _with_backoff(_gc.open_by_url, sheet_url.strip())
    if sheet_id and sheet_id.strip():
        return _with_backoff(_gc.open_by_key, sheet_id.strip())
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID i secrets.")

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    try:
        return _with_backoff(spread.worksheet, title)
    except WorksheetNotFound:
        return _with_backoff(spread.add_worksheet, title=title, rows=4000, cols=200)

# =========================
# I/O – läs/skriv/append
# =========================
@st.cache_data(ttl=120, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows   = values[1:]
    df = pd.DataFrame(rows, columns=header).replace("", np.nan)
    return df

def _write_df(title: str, df: pd.DataFrame):
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    out = df.copy()
    out.columns = [str(c) for c in out.columns]
    out = out.fillna("")
    _with_backoff(ws.clear)
    if out.shape[0] == 0:
        _with_backoff(ws.update, [list(out.columns)])
    else:
        _with_backoff(ws.update, [list(out.columns)] + out.astype(str).values.tolist())

def _append_rows(title: str, rows: List[List[Any]]):
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    _with_backoff(ws.append_rows, rows, value_input_option="RAW")

# =========================
# Schema – kolumner
# =========================
DATA_COLUMNS = [
    "Timestamp","Ticker","Bolagsnamn","Sektor","Bucket","Valuta",
    "Antal aktier","GAV (SEK)","Aktuell kurs",
    "Utestående aktier","Net debt",
    "Rev TTM","EBITDA TTM","EPS TTM",
    "PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
    "EPS 1Y","EPS 2Y","Rev CAGR","EPS CAGR",
    "Årlig utdelning","Utdelning CAGR",
    "Primär metod",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    "Senast auto uppdaterad","Auto källa"
]

SETTINGS_COLUMNS = ["Key","Value"]
FX_COLUMNS       = ["Valuta","SEK_per_1"]

def _ensure_sheet_schema():
    # Data
    df = _read_df(DATA_TITLE)
    if df.empty:
        _write_df(DATA_TITLE, pd.DataFrame(columns=DATA_COLUMNS))
    else:
        changed = False
        for c in DATA_COLUMNS:
            if c not in df.columns:
                df[c] = np.nan
                changed = True
        if changed:
            df = df[[c for c in DATA_COLUMNS if c in df.columns] + [c for c in df.columns if c not in DATA_COLUMNS]]
            _write_df(DATA_TITLE, df)

    # Settings (med rimlig default för cappar & parametrar)
    s = _read_df(SETTINGS_TITLE)
    if s.empty:
        base = pd.DataFrame([
            ["withholding_USD","0.15"],
            ["withholding_NOK","0.25"],
            ["withholding_CAD","0.15"],
            ["withholding_EUR","0.15"],
            ["withholding_SEK","0.00"],
            ["primary_currency","SEK"],
            ["multiple_decay","0.10"],
            ["pe_anchor_weight_ttm","0.50"],
            ["eps_cagr_cap_pos","0.40"],   # max +40%/år
            ["eps_cagr_cap_neg","0.30"],   # max -30%/år
            ["rev_cagr_cap_pos","0.30"],   # max +30%/år
            ["rev_cagr_cap_neg","0.20"],   # max -20%/år
        ], columns=SETTINGS_COLUMNS)
        _write_df(SETTINGS_TITLE, base)
    else:
        changed = False
        for c in SETTINGS_COLUMNS:
            if c not in s.columns:
                s[c] = np.nan
                changed = True
        if changed:
            _write_df(SETTINGS_TITLE, s[SETTINGS_COLUMNS])

    # FX
    fx = _read_df(FX_TITLE)
    if fx.empty:
        base_fx = pd.DataFrame([
            ["SEK",1.0],
            ["USD",np.nan],
            ["EUR",np.nan],
            ["NOK",np.nan],
            ["CAD",np.nan],
        ], columns=FX_COLUMNS)
        _write_df(FX_TITLE, base_fx)
    else:
        changed = False
        for c in FX_COLUMNS:
            if c not in fx.columns:
                fx[c] = np.nan
                changed = True
        if changed:
            _write_df(FX_TITLE, fx[FX_COLUMNS])

    # Snapshot
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        _write_df(SNAPSHOT_TITLE, pd.DataFrame(columns=[
            "Timestamp","Ticker","Valuta","Metod","Idag","1 år","2 år","3 år","Ankare PE","Decay"
        ]))

guard(_ensure_sheet_schema, label="(säkra ark/kolumner)")

# =========================
# FX – hämta via yfinance
# =========================
FX_PAIRS = {"USD":"USDSEK=X","EUR":"EURSEK=X","NOK":"NOKSEK=X","CAD":"CADSEK=X","SEK":None}

@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_fx_from_yahoo() -> Dict[str, float]:
    out = {"SEK":1.0}
    for code, pair in FX_PAIRS.items():
        if pair is None: continue
        try:
            t = yf.Ticker(pair)
            px = None
            try:
                fi = t.fast_info
                px = fi.last_price
            except Exception:
                px = None
            if not px:
                info = getattr(t, "info", {}) or {}
                px = info.get("regularMarketPrice") or info.get("currentPrice") or info.get("previousClose")
            if not px:
                hist = t.history(period="5d")
                if not hist.empty:
                    px = float(hist["Close"].dropna().iloc[-1])
            if px:
                out[code] = float(px)
        except Exception:
            pass
    return out

def _load_fx_and_update_sheet() -> Dict[str, float]:
    fx_df = _read_df(FX_TITLE)
    current = {"SEK":1.0}
    if not fx_df.empty:
        for _, r in fx_df.iterrows():
            try:
                current[str(r["Valuta"]).upper()] = float(r["SEK_per_1"])
            except Exception:
                pass
    fresh = _fetch_fx_from_yahoo()
    current.update({k:v for k,v in fresh.items() if v})
    rows = [(k, current.get(k, "")) for k in ["SEK","USD","EUR","NOK","CAD"]]
    _write_df(FX_TITLE, pd.DataFrame(rows, columns=FX_COLUMNS))
    return current

@st.cache_data(ttl=1800, show_spinner=False)
def get_fx_map() -> Dict[str, float]:
    mp = _load_fx_and_update_sheet()
    for c in ["SEK","USD","EUR","NOK","CAD"]:
        mp.setdefault(c, 1.0 if c=="SEK" else np.nan)
    return mp

# =========================
# Settings – kartor & källskatt
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    s = _read_df(SETTINGS_TITLE)
    out: Dict[str,str] = {}
    if not s.empty:
        for _, r in s.iterrows():
            k = str(r.get("Key"))
            v = "" if pd.isna(r.get("Value")) else str(r.get("Value"))
            out[k] = v
    return out

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
    code = (currency or "USD").upper()
    key  = f"withholding_{code}"
    try:
        return float(settings.get(key, "0.15"))
    except Exception:
        return 0.15

# =========================
# Publika IO – Data/Resultat
# =========================
def read_data_df() -> pd.DataFrame:
    df = _read_df(DATA_TITLE)
    if df.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)
    # typning för nycklar
    num_cols = [
        "Antal aktier","GAV (SEK)","Aktuell kurs",
        "Utestående aktier","Net debt",
        "Rev TTM","EBITDA TTM","EPS TTM","PE TTM","PE FWD",
        "EV/Revenue","EV/EBITDA","P/B","BVPS","EPS 1Y","EPS 2Y",
        "Rev CAGR","EPS CAGR","Årlig utdelning","Utdelning CAGR",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def write_data_df(df: pd.DataFrame):
    cols = [c for c in DATA_COLUMNS if c in df.columns] + [c for c in df.columns if c not in DATA_COLUMNS]
    _write_df(DATA_TITLE, df[cols])

def append_result_row(row: Dict[str, Any]):
    res = _read_df(RESULT_TITLE)
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame([row]))
    else:
        cols = list(res.columns)
        for k in row.keys():
            if k not in cols:
                res[k] = np.nan
                cols.append(k)
        res = pd.concat([res, pd.DataFrame([row])[cols]], ignore_index=True)
        _write_df(RESULT_TITLE, res[cols])

# ===== Hotfix-guard: säkerställ kritiska symboler finns =====
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b",
        "ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST
# ===== slut hotfix =====

# app.py — Del 2/5
# ============================================================
# Datainsamling (Yahoo primärt, Finnhub fallback) + growth-caps
# Beräkningsmotor (EPS/REV/EBITDA-paths, metoder, sanity & meta)
# ============================================================

import requests

# -------------------------
# Små hjälpare (parse)
# -------------------------
def _safe_float(x) -> Optional[float]:
    return _f(x)

def _parse_pct_like(v) -> Optional[float]:
    """
    '15.2%' -> 0.152, '0.152' -> 0.152, 0.152 -> 0.152
    """
    if v is None or (isinstance(v, float) and v != v):
        return None
    try:
        if isinstance(v, str):
            s = v.strip().replace(" ", "")
            if s.endswith("%"):
                return float(s[:-1].replace(",", ".")) / 100.0
            return float(s.replace(",", "."))
        return float(v)
    except Exception:
        return None

def _cap_growth(g: Optional[float], cap_pos: float, cap_neg: float) -> Optional[float]:
    if g is None:
        return None
    try:
        g = float(g)
        g = min(g, float(cap_pos))
        g = max(g, -float(cap_neg))
        return g
    except Exception:
        return g

# -------------------------
# Yahoo (yfinance) – robust snapshot med källmarkering
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar pris, valuta, namn och centrala nyckeltal från yfinance.
    Returnerar dict med nycklar:
      price, currency, company_name, market_cap, ev, shares,
      eps_ttm, pe_ttm, pe_fwd,
      revenue_ttm, ebitda_ttm,
      ev_to_sales, ev_to_ebitda, p_to_book, bvps,
      net_debt, sources={}
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {"sources": {}}

    # Snabbkanal (fast_info)
    try:
        fi = tk.fast_info
        if getattr(fi, "last_price", None):
            out["price"] = _safe_float(fi.last_price); out["sources"]["price"] = "yahoo_fast"
        if getattr(fi, "currency", None):
            out["currency"] = str(fi.currency).upper(); out["sources"]["currency"] = "yahoo_fast"
        if getattr(fi, "market_cap", None):
            out["market_cap"] = _safe_float(fi.market_cap); out["sources"]["market_cap"] = "yahoo_fast"
        if getattr(fi, "shares", None):
            out["shares"] = _safe_float(fi.shares); out["sources"]["shares"] = "yahoo_fast"
    except Exception:
        pass

    # Info (fallback + namn)
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k, default=None):
        try:
            return info.get(k, default)
        except Exception:
            return default

    def set_if_missing(key, val, src):
        if out.get(key) is None and val is not None:
            out[key] = _safe_float(val) if isinstance(val, (int, float, str)) else val
            out["sources"][key] = src

    # Namn – prioritera longName → shortName → symbol
    cname = gi("longName") or gi("shortName")
    if cname:
        out["company_name"] = str(cname)
        out["sources"]["company_name"] = "yahoo_info"

    # Pris/nycklar
    set_if_missing("price",        gi("regularMarketPrice") or gi("currentPrice") or gi("previousClose"), "yahoo_info")
    set_if_missing("currency",     gi("currency"),            "yahoo_info")
    set_if_missing("market_cap",   gi("marketCap"),           "yahoo_info")
    set_if_missing("eps_ttm",      gi("trailingEps"),         "yahoo_info")
    set_if_missing("pe_ttm",       gi("trailingPE"),          "yahoo_info")
    set_if_missing("pe_fwd",       gi("forwardPE"),           "yahoo_info")
    set_if_missing("revenue_ttm",  gi("totalRevenue"),        "yahoo_info")
    set_if_missing("ebitda_ttm",   gi("ebitda"),              "yahoo_info")
    set_if_missing("ev_to_sales",  gi("enterpriseToRevenue"), "yahoo_info")
    set_if_missing("ev_to_ebitda", gi("enterpriseToEbitda"),  "yahoo_info")
    set_if_missing("p_to_book",    gi("priceToBook"),         "yahoo_info")
    set_if_missing("bvps",         gi("bookValue"),           "yahoo_info")

    ev_info    = _safe_float(gi("enterpriseValue"))
    total_debt = _safe_float(gi("totalDebt"))
    total_cash = _safe_float(gi("totalCash"))

    if ev_info is not None:
        set_if_missing("ev", ev_info, "yahoo_info")
    elif out.get("market_cap") is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
        out["sources"]["ev"] = "calc_mc+debt-cash"

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]
        out["sources"]["net_debt"] = "calc_ev-mcap"

    # Shares fallback via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            out["sources"]["shares"] = "derived_mcap/price"
        except Exception:
            pass

    # Historik fallback för pris
    if not _pos(out.get("price")):
        try:
            hist = tk.history(period="5d")
            if not hist.empty:
                out["price"] = float(hist["Close"].dropna().iloc[-1])
                out["sources"]["price"] = "yahoo_hist_close"
        except Exception:
            pass

    # Normalisera valuta
    out["currency"] = str(out.get("currency") or "USD").upper()

    return out

# -------------------------
# Yahoo – EPS-estimat (earnings trend)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Plockar EPS currentYear/nextYear från Yahoo earnings trend (earningsEstimate.avg).
    Härleder 2Y via long-term growth ('next5Years') om möjligt.
    """
    try:
        tk = yf.Ticker(ticker)
        try:
            trend = tk.get_earnings_trend()
        except Exception:
            trend = getattr(tk, "earnings_trend", None)
        if trend is None or (hasattr(trend, "empty") and trend.empty):
            return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

        df = trend.copy()
        df.columns = [str(c).lower() for c in df.columns]

        def pick_eps(rowname: str) -> Optional[float]:
            sub = df[df["period"].astype(str).str.lower() == rowname]
            if sub.empty: return None
            row = sub.iloc[0]
            for k in ["earningsestimate.avg","earningsestimate_average","epsestimate.avg","epsestimate_average","epstrend.current","epstrend.mean"]:
                if k in df.columns:
                    return _safe_float(row.get(k))
            return None

        def pick_growth(rowname: str) -> Optional[float]:
            sub = df[df["period"].astype(str).str.lower() == rowname]
            if sub.empty: return None
            row = sub.iloc[0]
            for k in ["growth","growthrate","longtermgrowthrate","epsgrowth"]:
                if k in df.columns:
                    g = _parse_pct_like(row.get(k))
                    if g is not None:
                        return g
            return None

        eps_1y = pick_eps("nextyear")
        eps_cy = pick_eps("currentyear")  # ej direkt använd men bra för sanity
        long_cagr = pick_growth("next5years")

        eps_2y = None
        if _pos(eps_1y) and long_cagr is not None:
            eps_2y = float(eps_1y) * (1.0 + float(long_cagr))

        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": long_cagr, "source": "yahoo_trend"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

# -------------------------
# Yahoo – Revenue CAGR via income statement (3–5 år)
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str, min_years: int = 3, max_years: int = 5) -> Dict[str, Optional[float]]:
    try:
        tk = yf.Ticker(ticker)
        try:
            inc = tk.get_income_stmt(freq="annual")
        except Exception:
            inc = getattr(tk, "income_stmt", None)

        if inc is None or (hasattr(inc, "empty") and inc.empty):
            return {"rev_cagr": None, "years": None, "source": "none"}

        df = inc.copy()
        total_rev = None
        if "TotalRevenue" in df.index:
            total_rev = df.loc["TotalRevenue"]
        elif "Total Revenue" in df.index:
            total_rev = df.loc["Total Revenue"]
        else:
            for idx in df.index:
                s = str(idx).replace(" ", "").lower()
                if "totalrevenue" in s or s == "revenue":
                    total_rev = df.loc[idx]
                    break
        if total_rev is None:
            return {"rev_cagr": None, "years": None, "source": "none"}

        ser = pd.to_numeric(pd.Series(total_rev).dropna(), errors="coerce").dropna()
        if ser.empty:
            return {"rev_cagr": None, "years": None, "source": "none"}

        try:
            tmp = ser.copy()
            tmp.index = pd.to_datetime(tmp.index, errors="coerce")
            tmp = tmp.sort_index()
        except Exception:
            tmp = ser

        vals = tmp.dropna().values.tolist()
        if len(vals) < 2:
            return {"rev_cagr": None, "years": None, "source": "none"}

        n = min(max_years, len(vals))
        vals = vals[-n:]
        n_years = len(vals) - 1
        if n_years < 1 or n_years < min_years-1:
            return {"rev_cagr": None, "years": len(vals), "source": "yahoo_financials"}

        try:
            cagr = (vals[-1] / vals[0]) ** (1.0 / n_years) - 1.0
        except Exception:
            cagr = None

        return {"rev_cagr": cagr, "years": n_years, "source": "yahoo_financials"}
    except Exception:
        return {"rev_cagr": None, "years": None, "source": "none"}

# -------------------------
# Finnhub (valfritt) – EPS-estimat 1–2 år (fallback)
# -------------------------
def _get_finnhub_key() -> Optional[str]:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

@st.cache_data(ttl=900, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> Dict[str, Optional[float]]:
    key = _get_finnhub_key()
    if not key:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

    try:
        url = f"https://finnhub.io/api/v1/stock/estimate?symbol={ticker}&token={key}"
        r = requests.get(url, timeout=10)
        eps_1y, eps_2y = None, None
        if r.ok:
            js = r.json()
            rows = js if isinstance(js, list) else js.get("data", [])
            try:
                rows = sorted(rows or [], key=lambda x: str(x.get("period", "")))
            except Exception:
                rows = rows or []
            vals = [_safe_float(x.get("epsAvg")) for x in rows if _safe_float(x.get("epsAvg")) is not None]
            if len(vals) >= 1: eps_1y = vals[-1]
            if len(vals) >= 2: eps_2y = vals[-2]
        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "source": "finnhub"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

# -------------------------
# Multipel-decay & ankar-P/E
# -------------------------
def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    m0 = _pos(mult0)
    if m0 is None:
        return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_frac
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
    return w_ttm * pt + (1.0 - w_ttm) * pf

# -------------------------
# Price/EV helpers
# -------------------------
def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float], shares_fd: Optional[float]) -> Optional[float]:
    e = _pos(ev_target)
    s = _pos(shares_fd)
    if e is None or s is None:
        return None
    nd = _nz(net_debt, 0.0) or 0.0
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
    e = _pos(ebitda)
    m = _pos(mult)
    if e is None or m is None:
        return None
    return e * m

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb)
    b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b

# -------------------------
# EPS/REV/EBITDA paths med growth-caps
# -------------------------
def _eps_path_capped(eps_ttm: Optional[float],
                     eps_1y: Optional[float],
                     eps_2y: Optional[float],
                     eps_cagr_fallback: Optional[float],
                     cap_pos: float,
                     cap_neg: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(eps_ttm)
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    cg = _f(eps_cagr_fallback)

    # Om e0 & e1 finns → kapa tillåtet steg
    if e0 is not None and e1 is not None:
        g1 = e1 / e0 - 1.0
        g1 = _cap_growth(g1, cap_pos, cap_neg)
        e1 = e0 * (1.0 + g1)

    # Om e1 saknas → härled från e0 + (cappad) cagr
    if e1 is None and e0 is not None and cg is not None:
        cg = _cap_growth(cg, cap_pos, cap_neg)
        e1 = e0 * (1.0 + cg)

    # e2: om inte satt, extrapolera från e1 med cagr (cappad)
    if e2 is None and e1 is not None:
        cg2 = _cap_growth(cg, cap_pos, cap_neg)
        if cg2 is not None:
            e2 = e1 * (1.0 + cg2)

    # e3: extrapolera vidare om möjligt
    e3 = None
    if e2 is not None:
        cg3 = _cap_growth(cg, cap_pos, cap_neg)
        if cg3 is not None:
            e3 = e2 * (1.0 + cg3)
    return e0, e1, e2, e3

def _rev_path_capped(rev_ttm: Optional[float],
                     rev_cagr: Optional[float],
                     cap_pos: float,
                     cap_neg: float) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    r0 = _pos(rev_ttm)
    g  = _cap_growth(_f(rev_cagr), cap_pos, cap_neg)
    if r0 is None or g is None:
        return r0, None, None, None
    r1 = r0 * (1.0 + g)
    r2 = r1 * (1.0 + g)
    r3 = r2 * (1.0 + g)
    return r0, r1, r2, r3

def _ebitda_path_scale_with_rev(ebitda_ttm: Optional[float],
                                rev0: Optional[float], rev1: Optional[float],
                                rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(ebitda_ttm)
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return e0, e0, e0, e0
    def scale(r): return (e0 * (r / rev0)) if (r and rev0) else e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# -------------------------
# Utdelningsprognos (netto i SEK)
# -------------------------
def forecast_dividends_net_sek(
    currency: str,
    shares: Optional[float],
    current_dps: Optional[float],
    dps_cagr: Optional[float],
    fx_map: Dict[str, float],
    settings: Dict[str, str],
) -> Dict[str, Optional[float]]:
    if not _pos(shares) or current_dps is None:
        return {"y1": 0.0, "y2": 0.0, "y3": 0.0}
    g = _f(dps_cagr) or 0.0
    wh = get_withholding_for(currency, settings)
    fx = fx_map.get((currency or "USD").upper(), 1.0) or 1.0
    def net(years: int) -> float:
        gross = float(current_dps) * ((1.0 + g) ** years) * float(shares)
        return gross * (1.0 - wh) * float(fx)
    return {"y1": net(1), "y2": net(2), "y3": net(3)}

# -------------------------
# Huvudmotor per rad
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Beräknar metodtabell (Idag, 1,2,3 år) för raden.
    Returnerar (methods_df, sanity_text, meta)
    meta innehåller: currency, price, shares_out, net_debt, pe_anchor, decay, company_name, sources{}, paths{}
    """
    ticker = str(row.get("Ticker", "")).strip().upper()

    # 1) Live-data
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.20)  # lite throttling mot 429
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.10)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)
    fh = fetch_finnhub_estimates(ticker)

    # 2) Inputs (med fallback från Data-bladet)
    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs")))
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper()
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier")))
    net_debt = _nz(snap.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _pos(_nz(snap.get("revenue_ttm"), row.get("Rev TTM")))
    ebitda_ttm = _f(_nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM")))  # EBITDA kan vara negativ
    eps_ttm    = _f(_nz(snap.get("eps_ttm"), row.get("EPS TTM")))        # EPS kan vara negativ
    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B")))
    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS")))

    company_name = _nz(snap.get("company_name"), row.get("Bolagsnamn"))

    # Estimat / tillväxt – PRIORITET: Yahoo → Finnhub → Data → derivat
    eps_1y_est = _f(_nz(yh_eps.get("eps_1y"), _nz(fh.get("eps_1y"), row.get("EPS 1Y"))))
    eps_2y_est = _f(_nz(yh_eps.get("eps_2y"), _nz(fh.get("eps_2y"), row.get("EPS 2Y"))))

    # EPS CAGR – Data → Yahoo long-term → härledd från TTM→1Y → None
    eps_cagr   = _f(row.get("EPS CAGR"))
    if eps_cagr is None and yh_eps.get("eps_cagr_long") is not None:
        eps_cagr = _f(yh_eps.get("eps_cagr_long"))
    if eps_cagr is None and _pos(eps_ttm) and _pos(eps_1y_est):
        try:
            eps_cagr = (float(eps_1y_est)/float(eps_ttm)) - 1.0
        except Exception:
            eps_cagr = None

    # Rev CAGR – Data → Yahoo financials → None
    rev_cagr   = _f(row.get("Rev CAGR"))
    if rev_cagr is None and revcg_yh.get("rev_cagr") is not None:
        rev_cagr = _f(revcg_yh.get("rev_cagr"))

    # 3) Anchors, decay, growth-caps
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    eps_cap_pos = _f(settings.get("eps_cagr_cap_pos")) or 0.40
    eps_cap_neg = _f(settings.get("eps_cagr_cap_neg")) or 0.30
    rev_cap_pos = _f(settings.get("rev_cagr_cap_pos")) or 0.30
    rev_cap_neg = _f(settings.get("rev_cagr_cap_neg")) or 0.20

    # 4) Paths (med caps)
    e0, e1, e2, e3 = _eps_path_capped(eps_ttm, eps_1y_est, eps_2y_est, eps_cagr, eps_cap_pos, eps_cap_neg)
    r0, r1, r2, r3 = _rev_path_capped(rev_ttm, rev_cagr, rev_cap_pos, rev_cap_neg)
    b0, b1, b2, b3 = _ebitda_path_scale_with_rev(ebitda_ttm, r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,     _decay_multiple(p_b,     1, decay), _decay_multiple(p_b,     2, decay), _decay_multiple(p_b,     3, decay)

    # 5) Priser per metod (alla i bolagets handelsvaluta)
    methods = []

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
        "Metod": "ev_dacf",
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

    # 6) Sanity + META
    src = snap.get("sources", {})
    eps1_src = "yahoo_trend" if _pos(yh_eps.get("eps_1y")) else ("finnhub" if _pos(fh.get("eps_1y")) else ("sheet/derived" if _pos(row.get("EPS 1Y")) else "none"))
    eps2_src = "yahoo_trend" if _pos(yh_eps.get("eps_2y")) else ("finnhub" if _pos(fh.get("eps_2y")) else ("sheet/derived" if _pos(row.get("EPS 2Y")) or _pos(eps_2y_est) else "none"))
    revc_src = "yahoo_financials" if revcg_yh.get("rev_cagr") is not None else ("sheet" if _f(row.get("Rev CAGR")) is not None else "none")

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if e0 is not None else '—'}, "
        f"eps_1y={'ok' if e1 is not None else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 is not None else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 is not None else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_cagr={'ok' if _f(rev_cagr) is not None else '—'}({revc_src}), "
        f"ebitda_ttm={'ok' if b0 is not None else '—'}({src.get('ebitda_ttm','?')}), "
        f"shares={'ok' if shares else '—'}({src.get('shares','?')}), "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "company_name": company_name,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "sources": {
            **src,
            "eps_1y_source": eps1_src,
            "eps_2y_source": eps2_src,
            "rev_cagr_source": revc_src,
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# app.py — Del 3/5
# ============================================================
# Analys-vy: fair value-sammanställning, ranking & bläddring
# ============================================================

import math

# -------------------------
# Fair value = robust median över metoder
# -------------------------
def _median_pos(vals: list[Optional[float]]) -> Optional[float]:
    xs = []
    for v in vals:
        try:
            if v is None: 
                continue
            f = float(v)
            if not math.isfinite(f) or f <= 0:
                continue
            xs.append(f)
        except Exception:
            continue
    if not xs:
        return None
    xs.sort()
    mid = len(xs) // 2
    if len(xs) % 2 == 1:
        return float(xs[mid])
    return float((xs[mid - 1] + xs[mid]) / 2.0)

def aggregate_fair_values(methods_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    Tar metoderna (kolumner: Idag, 1 år, 2 år, 3 år) och räknar fair value
    som robust median över alla metoder som ger >0 pris.
    """
    cols = ["Idag", "1 år", "2 år", "3 år"]
    out = {}
    for c in cols:
        out_key = {"Idag": "today", "1 år": "y1", "2 år": "y2", "3 år": "y3"}[c]
        out[out_key] = _median_pos(methods_df[c].tolist())
    return out  # {"today": .., "y1": .., "y2": .., "y3": ..}

# -------------------------
# Per-rad beräkning → fair values & meta
# -------------------------
def compute_row_outputs(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Dict[str, Any]:
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    fv = aggregate_fair_values(methods_df)

    price = _pos(meta.get("price"))
    curr  = (meta.get("currency") or "USD").upper()
    fx    = fx_map.get(curr, 1.0) or 1.0
    price_sek = price * fx if price else None

    return {
        "ticker": str(row.get("Ticker")),
        "company": meta.get("company_name") or row.get("Bolagsnamn"),
        "currency": curr,
        "price": price,
        "price_sek": price_sek,
        "fair_today": fv.get("today"),
        "fair_1y": fv.get("y1"),
        "fair_2y": fv.get("y2"),
        "fair_3y": fv.get("y3"),
        "methods_df": methods_df,
        "sanity": sanity,
        "meta": meta,
    }

# -------------------------
# Bygg Analys-dataframe (portföljvärde, undervärdering, ranking)
# -------------------------
def build_analysis_df(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> pd.DataFrame:
    rows = []
    for _, row in df_data.iterrows():
        out = compute_row_outputs(row, settings, fx_map)

        shares_own = _pos(row.get("Antal aktier"))
        fx = fx_map.get(out["currency"], 1.0) or 1.0
        pos_value_sek = None
        if _pos(out["price"]) and _pos(shares_own):
            pos_value_sek = float(out["price"]) * float(shares_own) * float(fx)

        underv = None
        if _pos(out["fair_today"]) and _pos(out["price"]):
            underv = (float(out["fair_today"]) / float(out["price"])) - 1.0

        rows.append({
            "Ticker": out["ticker"],
            "Bolagsnamn": out["company"],
            "Bucket": row.get("Bucket"),
            "Valuta": out["currency"],
            "Aktuell kurs (live)": out["price"],
            "Fair value (idag)": out["fair_today"],
            "Fair value (1y)": out["fair_1y"],
            "Fair value (2y)": out["fair_2y"],
            "Fair value (3y)": out["fair_3y"],
            "Antal aktier": shares_own,
            "Positionvärde (SEK)": pos_value_sek,
            "Undervärdering (%)": (underv * 100.0) if underv is not None else None,
        })

    adf = pd.DataFrame(rows)

    # Sorteringsnyckel enligt: 1) minst position i SEK inom bucket  2) störst undervärdering
    def sort_key(r):
        v = r.get("Positionvärde (SEK)")
        u = r.get("Undervärdering (%)")
        v_key = v if (isinstance(v, (int, float)) and math.isfinite(v)) else float("inf")
        u_key = -(u if (isinstance(u, (int, float)) and math.isfinite(u)) else -1e9)
        return (v_key, u_key, str(r.get("Ticker")))

    if not adf.empty:
        adf = adf.sort_values(by=list(range(len(adf.columns))), key=lambda col: col, kind='stable')  # no-op to keep columns
        adf = adf.reindex(sorted(adf.index, key=lambda i: sort_key(adf.loc[i])))

    return adf

# -------------------------
# UI: ett bolagskort
# -------------------------
def render_company_card(calc: Dict[str, Any], fx_map: Dict[str, float]):
    price     = calc["price"]
    currency  = calc["currency"]
    fx        = fx_map.get(currency, 1.0) or 1.0
    price_sek = price * fx if price else None

    fv_today  = calc["fair_today"]
    fv_1y     = calc["fair_1y"]
    fv_2y     = calc["fair_2y"]
    fv_3y     = calc["fair_3y"]

    underv = None
    if _pos(fv_today) and _pos(price):
        underv = fv_today/price - 1.0

    st.subheader(f"**{calc['ticker']} — {calc['company'] or ''}**")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Pris (valuta)", f"{price:,.2f} {currency}" if price else "—")
    with c2:
        st.metric("Pris i SEK", f"{price_sek:,.2f} SEK" if price_sek else "—")
    with c3:
        st.metric("Fair value (idag)", f"{fv_today:,.2f} {currency}" if fv_today else "—")
    with c4:
        st.metric("Undervärdering", f"{underv*100:,.1f} %" if underv is not None else "—")

    st.caption(calc["sanity"])
    with st.expander("Beräkningsmetoder (priser i bolagets valuta)"):
        st.dataframe(calc["methods_df"], use_container_width=True)

# -------------------------
# Analys-vy (huvud)
# -------------------------
def page_analysis(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]):
    st.header("🔎 Analys & värdering")

    # Bucket-filter
    buckets = ["Alla"] + sorted([b for b in df_data["Bucket"].dropna().unique().tolist() if str(b).strip() != ""])
    sel_bucket = st.selectbox("Bucket-filter", buckets, index=0)

    df_filt = df_data.copy()
    if sel_bucket != "Alla":
        df_filt = df_filt[df_filt["Bucket"] == sel_bucket]

    # Bygg rankingtabell
    with st.spinner("Beräknar fair value & ranking…"):
        rank_df = build_analysis_df(df_filt, settings, fx_map)

    # Visa rankingtabell (minst position i bucket först, sedan undervärdering)
    st.subheader("📋 Ranking (minst position i SEK först, därefter störst undervärdering)")
    if rank_df.empty:
        st.info("Inga bolag i urvalet.")
        return

    st.dataframe(rank_df, use_container_width=True)

    # Bläddra ett-och-ett
    st.subheader("📑 Bolagskort (bläddra)")
    if "analysis_idx" not in st.session_state:
        st.session_state.analysis_idx = 0

    # Synka index om filtret ändrats så vi håller oss inom bounds
    n = len(rank_df)
    st.session_state.analysis_idx = max(0, min(st.session_state.analysis_idx, n - 1))

    c_prev, c_pos, c_next = st.columns([1, 4, 1])
    with c_prev:
        if st.button("◀︎ Föregående", use_container_width=True, disabled=(st.session_state.analysis_idx <= 0)):
            st.session_state.analysis_idx = max(0, st.session_state.analysis_idx - 1)
    with c_next:
        if st.button("Nästa ▶︎", use_container_width=True, disabled=(st.session_state.analysis_idx >= n - 1)):
            st.session_state.analysis_idx = min(n - 1, st.session_state.analysis_idx + 1)

    cur_row = rank_df.iloc[st.session_state.analysis_idx]
    # Hitta originalrad (för att köra full metodtabell igen)
    src_row = df_filt[df_filt["Ticker"] == cur_row["Ticker"]].iloc[0]
    calc = compute_row_outputs(src_row, settings, fx_map)
    render_company_card(calc, fx_map)

    # Visa hela databasen längst ned (som användaren vill)
    st.subheader("🗂️ Hela databasen (rådata)")
    st.dataframe(df_data, use_container_width=True)

# app.py — Del 4/5
# ============================================================
# Portfölj-vy: priser (live+fallback), GAV i SEK, P/L, utdelning 12m,
# summering per bucket
# ============================================================

# ---- Hjälpare för namn/pris med robusta fallbacks ----
@st.cache_data(ttl=300, show_spinner=False)
def _fetch_company_name(ticker: str) -> Optional[str]:
    try:
        tk = yf.Ticker(ticker)
        try:
            info = tk.info or {}
        except Exception:
            info = {}
        name = info.get("longName") or info.get("shortName") or info.get("symbol")
        if name and str(name).strip():
            return str(name)
    except Exception:
        pass
    return None

def _live_price_or_fallback(ticker: str, row_price: Optional[float]) -> Tuple[Optional[float], str]:
    """
    Försök: fast_info.last_price → history Close → Data-bladets 'Aktuell kurs'.
    Returnerar (pris, källa)
    """
    snap = fetch_yahoo_snapshot(ticker)
    price = _pos(snap.get("price"))
    if price:
        return price, snap.get("sources", {}).get("price", "yahoo")
    # Fallback till Data-blad
    rp = _pos(row_price)
    if rp:
        return rp, "sheet"
    return None, "none"

# ---- Kärnberäkning: rad → portföljmått ----
def _portfolio_row_metrics(row: pd.Series, fx_map: Dict[str, float], settings: Dict[str, str]) -> Dict[str, Any]:
    tkr   = str(row.get("Ticker")).strip().upper()
    name  = str(row.get("Bolagsnamn") or "") or _fetch_company_name(tkr) or tkr
    ccy   = str(row.get("Valuta") or "USD").upper()
    fx    = fx_map.get(ccy, 1.0) or 1.0

    shares = _pos(row.get("Antal aktier")) or 0.0
    gav_sek = _pos(row.get("GAV (SEK)")) or 0.0

    price, price_src = _live_price_or_fallback(tkr, row.get("Aktuell kurs"))
    price_ccy = price  # i bolagets valuta
    price_sek = price_ccy * fx if price_ccy else None

    # Position- & anskaffningsvärde
    pos_value_sek = (price_ccy * shares * fx) if (price_ccy and shares) else 0.0
    cost_sek      = shares * gav_sek

    pl_sek = pos_value_sek - cost_sek
    pl_pct = (pl_sek / cost_sek * 100.0) if cost_sek > 0 else (None)

    # Estimerad utdelning 12m, netto i SEK (använder DPS + CAGR om finns)
    dps_now  = _pos(row.get("Årlig utdelning"))
    dps_cagr = _f(row.get("Utdelning CAGR"))
    div12m   = 0.0
    if dps_now is not None and shares > 0:
        f = forecast_dividends_net_sek(ccy, shares, dps_now, dps_cagr, fx_map, settings)
        # Vi visar "y1" som uppskattad 12m-utdelning
        div12m = float(f.get("y1") or 0.0)

    return {
        "Ticker": tkr,
        "Bolagsnamn": name,
        "Bucket": row.get("Bucket"),
        "Valuta": ccy,
        "Pris (valuta)": price_ccy,
        "Källa pris": price_src,
        "Antal": shares,
        "GAV (SEK)": gav_sek,
        "Innehavsvärde (SEK)": pos_value_sek,
        "Anskaffningsvärde (SEK)": cost_sek,
        "P/L (SEK)": pl_sek,
        "P/L (%)": pl_pct,
        "Est. utdelning 12m (SEK)": div12m,
    }

def _fmt_money0(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    try:
        return f"{float(v):,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

def _fmt_money2_ccy(v: Optional[float], ccy: str) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    try:
        return f"{float(v):,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} {ccy}"

def _fmt_pct1(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    try:
        return f"{float(v):,.1f} %".replace(",", " ").replace(".", ",")
    except Exception:
        return str(v)

# ---- Bygg portföljtabell från Data-bladet ----
def build_portfolio_table(df_data: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, str]) -> pd.DataFrame:
    hold = df_data.copy()
    if "Antal aktier" in hold.columns:
        hold = hold[pd.to_numeric(hold["Antal aktier"], errors="coerce").fillna(0) > 0]
    if hold.empty:
        return pd.DataFrame(columns=[
            "Ticker","Bolagsnamn","Bucket","Valuta","Pris (valuta)","Antal",
            "GAV (SEK)","Innehavsvärde (SEK)","Anskaffningsvärde (SEK)","P/L (SEK)","P/L (%)","Est. utdelning 12m (SEK)","Källa pris"
        ])

    rows = []
    for _, r in hold.iterrows():
        rows.append(_portfolio_row_metrics(r, fx_map, settings))
    pf = pd.DataFrame(rows)

    # Sortera: minst positionvärde först (bra för “build from the back”)
    pf = pf.sort_values(by=["Innehavsvärde (SEK)","P/L (%)"], ascending=[True, True], na_position="last").reset_index(drop=True)
    return pf

# ---- UI: Portfölj-sida ----
def page_portfolio(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]):
    st.header("💼 Portfölj")

    pf = build_portfolio_table(df_data, fx_map, settings)

    total_value = float(pf["Innehavsvärde (SEK)"].sum()) if not pf.empty else 0.0
    total_cost  = float(pf["Anskaffningsvärde (SEK)"].sum()) if not pf.empty else 0.0
    total_pl    = total_value - total_cost
    total_plpct = (total_pl / total_cost * 100.0) if total_cost > 0 else None
    total_div12 = float(pf["Est. utdelning 12m (SEK)"].sum()) if not pf.empty else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Portföljvärde", _fmt_money0(total_value))
    c2.metric("P/L (SEK)", _fmt_money0(total_pl), delta=_fmt_pct1(total_plpct))
    c3.metric("Est. utdelning 12m", _fmt_money0(total_div12))

    # Visa tabell
    if pf.empty:
        st.info("Inga innehav hittades (Antal aktier = 0). Gå till **Editor** och lägg in antal/GAV.")
        return

    # Välj vilka kolumner att visa tydligt
    vis_cols = [
        "Ticker","Bolagsnamn","Bucket","Valuta","Pris (valuta)","Antal",
        "GAV (SEK)","Innehavsvärde (SEK)","Anskaffningsvärde (SEK)",
        "P/L (SEK)","P/L (%)","Est. utdelning 12m (SEK)","Källa pris"
    ]
    st.dataframe(pf[vis_cols], use_container_width=True)

    # Summering per bucket
    with st.expander("📦 Summering per bucket"):
        grp = pf.groupby("Bucket", dropna=False).agg({
            "Innehavsvärde (SEK)": "sum",
            "Anskaffningsvärde (SEK)": "sum",
            "Est. utdelning 12m (SEK)": "sum"
        }).reset_index()
        grp["P/L (SEK)"] = grp["Innehavsvärde (SEK)"] - grp["Anskaffningsvärde (SEK)"]
        grp["P/L (%)"] = np.where(grp["Anskaffningsvärde (SEK)"] > 0,
                                  grp["P/L (SEK)"] / grp["Anskaffningsvärde (SEK)"] * 100.0,
                                  np.nan)
        st.dataframe(grp, use_container_width=True)

    st.caption("Tips: pris hämtas live från Yahoo (med historisk close som fallback). Om pris saknas och 'Aktuell kurs' i Data är tomt kan källan visas som 'none'.")

# app.py — Del 5/5
# ============================================================
# Sidor + navigation + main
#  • Ny Analys-sortering: minsta bucketvärde (SEK) först,
#    därefter störst undervärdering (FV Idag > pris).
#  • Portfölj-sidan integrerad.
# ============================================================

# --- Liten kompatibilitets-hjälpare för rerun (olika Streamlit-versioner) ---
def _safe_rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass

# --- Beräkna bucket-värden (SEK) baserat på dina faktiska innehav ---
@st.cache_data(ttl=180, show_spinner=False)
def _bucket_value_map(df_data: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, str]) -> Dict[str, float]:
    """
    Summerar innehavsvärde (SEK) per Bucket, men endast för rader där Antal > 0.
    Använder samma logik som Portfölj-vyn (livepris + fallback).
    """
    pf = build_portfolio_table(df_data, fx_map, settings)
    if pf.empty:
        return {}
    grp = pf.groupby("Bucket", dropna=False)["Innehavsvärde (SEK)"].sum()
    return { (k if k == k else "Okänd"): float(v) for k, v in grp.items() }

# --- Hjälpare: räkna FV “Idag” och uppsida för en rad enligt preset/heuristik ---
def _fair_today_and_price(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Returnerar (fair_today, live_price, used_method)
    """
    met_df, _, meta = compute_methods_for_row(row, settings, fx_map)
    preset = str(_nz(row.get("Primär metod"), "")).strip() or None
    method, fair_today, _, _, _ = _pick_primary_from_table(met_df, preset)
    price = meta.get("price")
    return _pos(fair_today), _pos(price), method

# --- Uppdaterad Analys-sida med nytt sorteringsläge ---
def page_analysis_new():
    st.header("🔬 Analys – minsta bucket först → störst undervärdering")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()

    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till bolag.")
        return

    # Filter & val
    f1, f2, f3 = st.columns(3)
    buckets = f1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_only = f2.checkbox("Visa endast innehav (antal > 0)", value=False)
    underv_only = f3.checkbox("Visa endast undervärderade (FV Idag > pris)", value=True)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_only:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]

    if q.empty:
        st.warning("Inget att visa efter filter.")
        return

    # Hämta bucket-värdekarta (SEK) baserat på verkliga innehav
    bucket_map = _bucket_value_map(df, fx_map, settings)  # använder alla innehav oavsett filter
    # Buckets utan innehav -> 0 SEK så att de prioriteras (minst först)
    def bucket_val(b):
        return float(bucket_map.get(b, 0.0))

    # Bygg scorer: först bucketvärde-nyckel, sedan undervärdering (uppsida Idag)
    rows_scored = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            fair_today, price, used_method = _fair_today_and_price(r, settings, fx_map)
            if underv_only and not (_pos(fair_today) and _pos(price) and fair_today > price):
                # hoppa över om vi bara vill se undervärderade
                pass
            else:
                up = None
                if _pos(fair_today) and _pos(price):
                    up = fair_today/price - 1.0
                rows_scored.append({
                    "row": r,
                    "bucket": r.get("Bucket"),
                    "bucket_value_sek": bucket_val(r.get("Bucket")),
                    "upside_today": up,
                    "fair_today": fair_today,
                    "price": price,
                    "method": used_method
                })
        except Exception:
            rows_scored.append({
                "row": r,
                "bucket": r.get("Bucket"),
                "bucket_value_sek": bucket_val(r.get("Bucket")),
                "upside_today": None,
                "fair_today": None,
                "price": None,
                "method": None
            })
        prog.progress((i+1)/max(1, len(q)))
    prog.empty()

    if not rows_scored:
        st.info("Inget att visa. Inga bolag uppfyllde villkoren.")
        return

    # Sortering: 1) minsta bucketvärde  2) undervärdering (störst upp först)  3) namn
    def sort_key(x):
        bval = x["bucket_value_sek"]
        up   = x["upside_today"]
        # undervärderade före icke-undervärderade, sedan största uppsida
        underv_rank = 0 if (up is not None and up > 0) else 1
        return (bval if bval is not None else 0.0,
                underv_rank,
                -(up if (up is not None) else -9e9),
                str(x["row"].get("Ticker")))

    rows_scored.sort(key=sort_key)

    # Bläddringsvy (1/X) i denna ordning
    key_idx = "analysis_idx_new"
    if key_idx not in st.session_state:
        st.session_state[key_idx] = 0

    # Lista för snabbhoppa
    tkr_options = [str(x["row"].get("Ticker")) for x in rows_scored]
    jump = st.selectbox("Gå direkt till bolag", tkr_options, index=min(st.session_state[key_idx], len(tkr_options)-1))
    if jump in tkr_options:
        st.session_state[key_idx] = tkr_options.index(jump)

    # Visning av rankingtoppen (mini-tabell)
    with st.expander("📋 Ordning (förhandsvisning)", expanded=False):
        preview = []
        for x in rows_scored:
            preview.append({
                "Bucket": x["bucket"],
                "Bucketvärde (SEK)": x["bucket_value_sek"],
                "Ticker": x["row"].get("Ticker"),
                "Uppsida Idag (%)": (x["upside_today"]*100.0 if x["upside_today"] is not None else None),
                "Metod": x["method"],
            })
        st.dataframe(pd.DataFrame(preview), use_container_width=True)

    # Navigering
    cprev, cpos, cnext = st.columns([1,2,1])
    with cprev:
        st.button("⬅️ Föregående", use_container_width=True, on_click=lambda: st.session_state.update({key_idx: max(0, st.session_state[key_idx]-1)}), disabled=(st.session_state[key_idx] <= 0))
    with cpos:
        st.write(f"**{st.session_state[key_idx]+1} / {len(rows_scored)}** — minsta bucket först → störst undervärdering")
    with cnext:
        st.button("Nästa ➡️", use_container_width=True, on_click=lambda: st.session_state.update({key_idx: min(len(rows_scored)-1, st.session_state[key_idx]+1)}), disabled=(st.session_state[key_idx] >= len(rows_scored)-1))

    # Rendera valt bolag med full company_card (metoder + källor + knappar)
    sel = rows_scored[st.session_state[key_idx]]["row"]
    with st.container(border=True):
        _company_card(sel, settings, fx_map)
        st.markdown("---")

# --- Routing (sidor) ---
def run_main_ui():
    st.title(APP_TITLE)

    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        st.write("FX:", get_fx_map())
        st.write("Settings:", get_settings_map())

    page = st.sidebar.radio("Sidor", ["Editor", "Analys", "Portfölj", "Ranking (gammal)", "Inställningar", "Batch"], index=2)

    if page == "Editor":
        page_editor()
    elif page == "Analys":
        page_analysis_new()   # <-- nya analys-rankingen
    elif page == "Portfölj":
        df = read_data_df()
        page_portfolio(df, get_settings_map(), get_fx_map())
    elif page == "Ranking (gammal)":
        page_ranking()        # kvar för jämförelse/test
    elif page == "Inställningar":
        page_settings()
    elif page == "Batch":
        page_batch()

def main():
    run_main_ui()

if __name__ == "__main__":
    main()
