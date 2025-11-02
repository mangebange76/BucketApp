# app.py — Del 1/4
# ============================================================
# Bas: Streamlit-app för fair value / riktkurser / portfölj
# Lagring: Google Sheets (Data, Resultat, Valutakurser, Settings, Snapshot)
# Hämtning: Yahoo (yfinance) + (valfritt) Finnhub för EPS 1–2y
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
    if v: 
        return v
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
            if i == 5: 
                raise
            time.sleep(delay)
            delay *= 1.6

def _f(x) -> Optional[float]:
    try:
        if x is None: 
            return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            if s == "": 
                return None
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

def _nz(x, fallback=None):
    return x if (x is not None and x == x) else fallback

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
        return _with_backoff(spread.add_worksheet, title=title, rows=2000, cols=200)

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

    # Settings
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
            ["auto_refresh_on_start","0"],  # 0 = av, 1 = på
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
        if pair is None: 
            continue
        try:
            t = yf.Ticker(pair)
            px = None
            try:
                fi = t.fast_info
                px = fi.last_price
            except Exception:
                px = None
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
# Settings – läs/källskatt
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

# =========================
# Session bootstrap-funktion (kallas i Del 4)
# =========================
def _startup_refresh():
    """
    Körs en gång per session (kallas i main/Del 4):
      1) Uppdatera FX till fliken Valutakurser
      2) Om Settings.auto_refresh_on_start = 1 → uppdatera Data-fält från Yahoo
    OBS: Använder fetch_yahoo_snapshot som definieras i Del 2.
    """
    # 1) FX
    get_fx_map()

    # 2) Auto-refresh (styrt från Settings)
    s = get_settings_map()
    auto = str(s.get("auto_refresh_on_start", "0")).strip().lower() in ("1","true","yes")
    if not auto:
        return

    df = read_data_df()
    if df.empty:
        return

    df2 = df.copy()
    for idx, r in df2.iterrows():
        tkr = str(r.get("Ticker","")).strip().upper()
        if not tkr:
            continue
        # fetch_yahoo_snapshot kommer från Del 2
        try:
            snap = fetch_yahoo_snapshot(tkr)  # type: ignore[name-defined]
        except Exception:
            snap = {}
        if snap.get("price") is not None:       df2.at[idx, "Aktuell kurs"]     = snap["price"]
        if snap.get("currency"):                df2.at[idx, "Valuta"]           = snap["currency"]
        if snap.get("revenue_ttm") is not None: df2.at[idx, "Rev TTM"]          = snap["revenue_ttm"]
        if snap.get("ebitda_ttm") is not None:  df2.at[idx, "EBITDA TTM"]       = snap["ebitda_ttm"]
        if snap.get("eps_ttm") is not None:     df2.at[idx, "EPS TTM"]          = snap["eps_ttm"]
        if snap.get("pe_ttm") is not None:      df2.at[idx, "PE TTM"]           = snap["pe_ttm"]
        if snap.get("pe_fwd") is not None:      df2.at[idx, "PE FWD"]           = snap["pe_fwd"]
        if snap.get("ev_to_sales") is not None: df2.at[idx, "EV/Revenue"]       = snap["ev_to_sales"]
        if snap.get("ev_to_ebitda") is not None:df2.at[idx, "EV/EBITDA"]        = snap["ev_to_ebitda"]
        if snap.get("p_to_book") is not None:   df2.at[idx, "P/B"]              = snap["p_to_book"]
        if snap.get("bvps") is not None:        df2.at[idx, "BVPS"]             = snap["bvps"]
        if snap.get("net_debt") is not None:    df2.at[idx, "Net debt"]         = snap["net_debt"]
        df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
        df2.at[idx, "Auto källa"] = "Yahoo"

    write_data_df(df2)

# app.py — Del 2/4
# ============================================================
# Datainsamling (Yahoo primärt, Finnhub fallback), rimliga spärrar
# för tillväxt (CAGR-clamps), beräkningsmotor & utdelning
# ============================================================

import requests

# -------------------------
# Små hjälpare (parse/guard)
# -------------------------
def _safe_float(x) -> Optional[float]:
    return _f(x)

def _parse_growth_pct(v) -> Optional[float]:
    """
    Tar '15.2%' eller '0.152' eller 0.152 -> returnerar 0.152
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

def _clamp(val: Optional[float], lo: float, hi: float) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        if not math.isfinite(v):
            return None
        return max(lo, min(hi, v))
    except Exception:
        return None

# Rimlighets-spärrar (kan flyttas till Settings senare om du vill styra från UI)
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.40   # +40 %

# -------------------------
# Yahoo (yfinance) – robust snapshot med källmarkering
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar pris, valuta och centrala nyckeltal från yfinance.
    Returnerar dict med nycklar:
      price, currency, market_cap, ev, shares,
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
        out["price"]      = _safe_float(getattr(fi, "last_price", None));      out["sources"]["price"] = "yahoo_fast"
        out["currency"]   = getattr(fi, "currency", None);                     out["sources"]["currency"] = "yahoo_fast"
        out["market_cap"] = _safe_float(getattr(fi, "market_cap", None));      out["sources"]["market_cap"] = "yahoo_fast"
        out["shares"]     = _safe_float(getattr(fi, "shares", None));          out["sources"]["shares"] = "yahoo_fast"
    except Exception:
        pass

    # Info (fallback)
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k):
        try:
            return info.get(k)
        except Exception:
            return None

    def set_if_missing(key, val, src):
        if out.get(key) is None and val is not None:
            out[key] = _safe_float(val) if isinstance(val, (int, float, str)) else val
            out["sources"][key] = src

    set_if_missing("price",        gi("currentPrice"),        "yahoo_info")
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
# Yahoo – EPS-estimat & långsiktig tillväxt (earnings trend)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Plockar EPS currentYear/nextYear från Yahoo earnings trend (earningsEstimate.avg).
    Försöker härleda 2Y via långsiktig EPS-growth ('next5Years') om den finns.
    Returnerar: {"eps_1y": float|None, "eps_2y": float|None, "eps_cagr_long": float|None, "source": "yahoo_trend"|"none"}
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

        if "period" not in df.columns:
            return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

        def get_eps_avg(rowname: str) -> Optional[float]:
            sub = df[df["period"].astype(str).str.lower() == rowname]
            if sub.empty:
                return None
            row = sub.iloc[0]
            # Vanliga fält-namn (yfinance varierar mellan versioner)
            for k in ["earningsestimate.avg","earningsestimate_average","earningsestimate.avg.0","earningsestimate_avg","epsestimate.avg","epsestimate_average","epstrend.current","epstrend.mean"]:
                if k in df.columns:
                    return _safe_float(row.get(k))
            return None

        def get_growth(rowname: str) -> Optional[float]:
            sub = df[df["period"].astype(str).str.lower() == rowname]
            if sub.empty:
                return None
            row = sub.iloc[0]
            for k in ["growth","growthrate","longtermgrowthrate","epsgrowth"]:
                if k in df.columns:
                    g = _parse_growth_pct(row.get(k))
                    if g is not None:
                        return g
            return None

        eps_1y = get_eps_avg("nextyear")
        eps_cy = get_eps_avg("currentyear")  # ibland användbart för derivat
        eps_cagr_long = get_growth("next5years")

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": eps_cagr_long, "source": "yahoo_trend"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

# -------------------------
# Yahoo – Revenue CAGR från årsredovisade intäkter
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str, min_years: int = 3, max_years: int = 5) -> Dict[str, Optional[float]]:
    """
    Försöker hämta annual income statement och beräkna Rev CAGR över 3–5 år (så långt det går).
    Returnerar {"rev_cagr": float|None, "years": int|None, "source": "yahoo_financials"|"none"}.
    """
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
    """
    Fallback: EPS-estimat 1–2 år framåt från Finnhub om nyckel finns.
    Returnerar {"eps_1y": float|None, "eps_2y": float|None, "source": "finnhub"|"none"}
    """
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
# Price/EV builders
# -------------------------
def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float], shares_fd: Optional[float]) -> Optional[float]:
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
# EPS/REV/EBITDA paths + härledning
# -------------------------
def _derive_eps_from_pe_if_missing(price: Optional[float], pe_ttm: Optional[float], pe_fwd: Optional[float],
                                   eps_ttm: Optional[float], eps_1y: Optional[float]) -> Tuple[Optional[float], str, Optional[float], str]:
    src_ttm = "source" if eps_ttm is not None else ""
    src_1y  = "source" if eps_1y  is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe"
    if eps_1y is None and _pos(price) and _pos(pe_fwd):
        eps_1y = price / pe_fwd
        src_1y = "derived_from_forward_pe"
    return eps_ttm, src_ttm, eps_1y, src_1y

def _eps_path(eps_ttm: Optional[float], eps_1y: Optional[float], eps_2y: Optional[float], eps_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(eps_ttm)
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    cg = _f(eps_cagr)

    if e1 is None and e0 is not None and cg is not None:
        e1 = e0 * (1.0 + cg)
    if e2 is None and e1 is not None and cg is not None:
        e2 = e1 * (1.0 + cg)
    e3 = e2 * (1.0 + cg) if (e2 is not None and cg is not None) else None
    return e0, e1, e2, e3

def _rev_path(rev_ttm: Optional[float], rev_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    r0 = _pos(rev_ttm)
    cg = _f(rev_cagr)
    if r0 is None or cg is None:
        return r0, None, None, None
    r1 = r0 * (1.0 + cg)
    r2 = r1 * (1.0 + cg)
    r3 = r2 * (1.0 + cg)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(ebitda_ttm)
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        # ok – lås EBITDA oförändrat om vi saknar intäkts-bana
        return e0, e0, e0, e0
    def scale(r): 
        try:
            return (e0 * (r / rev0)) if (r and rev0) else e0
        except Exception:
            return e0
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
# Huvudmotor per rad (inkl. rimlighets-spärrar på CAGR)
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Beräknar metodtabell (Idag, 1,2,3 år) för raden.
    Returnerar (methods_df, sanity_text, meta)
    meta innehåller: currency, price, shares_out, net_debt, pe_anchor, decay, sources{}, cagr_clamped{}
    """
    ticker = str(row.get("Ticker", "")).strip()
    # 1) Live-data
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.25)  # mild throttling för att undvika 429
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.15)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)
    # Finnhub (fallback)
    fh = fetch_finnhub_estimates(ticker)

    # 2) Inputs (med fallback från Data-bladet)
    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs")))
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper()
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier")))
    net_debt = _nz(snap.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _pos(_nz(snap.get("revenue_ttm"), row.get("Rev TTM")))
    ebitda_ttm = _pos(_nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM")))
    eps_ttm    = _pos(_nz(snap.get("eps_ttm"), row.get("EPS TTM")))
    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B")))
    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS")))

    # Estimat / tillväxt – PRIORITET: Yahoo → Finnhub → Data → derivat
    eps_1y_est = _pos(_nz(yh_eps.get("eps_1y"), _nz(fh.get("eps_1y"), row.get("EPS 1Y"))))
    eps_2y_est = _pos(_nz(yh_eps.get("eps_2y"), _nz(fh.get("eps_2y"), row.get("EPS 2Y"))))

    # EPS CAGR – Data → Yahoo long-term → härledd från TTM→1Y → None
    eps_cagr_raw   = _f(row.get("EPS CAGR"))
    if eps_cagr_raw is None and yh_eps.get("eps_cagr_long") is not None:
        eps_cagr_raw = _f(yh_eps.get("eps_cagr_long"))
    if eps_cagr_raw is None and _pos(eps_ttm) and _pos(eps_1y_est):
        try:
            eps_cagr_raw = (float(eps_1y_est)/float(eps_ttm)) - 1.0
        except Exception:
            eps_cagr_raw = None
    eps_cagr = _clamp(eps_cagr_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    # Om eps_2y fortfarande saknas men vi har en (clampad) CAGR → extrapolera
    if eps_2y_est is None and _pos(eps_1y_est) and eps_cagr is not None:
        eps_2y_est = float(eps_1y_est) * (1.0 + float(eps_cagr))

    # Rev CAGR – Data → Yahoo financials → None → clamp
    rev_cagr_raw = _f(row.get("Rev CAGR"))
    if rev_cagr_raw is None and revcg_yh.get("rev_cagr") is not None:
        rev_cagr_raw = _f(revcg_yh.get("rev_cagr"))
    rev_cagr = _clamp(rev_cagr_raw, REV_CAGR_MIN, REV_CAGR_MAX)

    # 3) Härled EPS om saknas men PE+price finns
    eps_ttm, src_eps_ttm, eps_1y_est, src_eps_1y = _derive_eps_from_pe_if_missing(
        price, pe_ttm, pe_fwd, eps_ttm, eps_1y_est
    )

    # 4) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 5) Paths (använder clampade CAGR)
    e0, e1, e2, e3 = _eps_path(eps_ttm, eps_1y_est, eps_2y_est, eps_cagr)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(ebitda_ttm, r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,     _decay_multiple(p_b,     1, decay), _decay_multiple(p_b,     2, decay), _decay_multiple(p_b,     3, decay)

    # 6) Priser per metod (alla i bolagets handelsvaluta)
    methods = []

    # P/E vs EPS
    methods.append({
        "Metod": "pe_hist_vs_eps",
        "Idag": _price_from_pe(e0, pe0),
        "1 år": _price_from_pe(e1, pe1m),
        "2 år": _price_from_pe(e2, pe2m),
        "3 år": _price_from_pe(e3, pe3m),
    })

    # EV/Sales
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

    # EV/DACF (proxy = EV/EBITDA tills DACF finns)
    methods.append({
        "Metod": "ev_dacf",
        "Idag": _equity_price_from_ev(_ev_from_ebitda(b0, eve0), net_debt, shares),
        "1 år": _equity_price_from_ev(_ev_from_ebitda(b1, eve1), net_debt, shares),
        "2 år": _equity_price_from_ev(_ev_from_ebitda(b2, eve2), net_debt, shares),
        "3 år": _equity_price_from_ev(_ev_from_ebitda(b3, eve3), net_debt, shares),
    })

    # P/B (kräver BVPS – annars None)
    methods.append({
        "Metod": "p_b",
        "Idag": _price_from_pb(pb0, bvps),
        "1 år": _price_from_pb(pb1, bvps),
        "2 år": _price_from_pb(pb2, bvps),
        "3 år": _price_from_pb(pb3, bvps),
    })

    # Platshållare för metoder som kräver per-aktie-tal vi ofta inte kan hämta automatiskt
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # 7) Sanity + META
    src = snap.get("sources", {}) or {}
    # märk upp vilka EPS/Rev-källor som faktiskt användes
    eps1_src = "yahoo_trend" if _pos(yh_eps.get("eps_1y")) else ("finnhub" if _pos(fh.get("eps_1y")) else (src.get("eps_1y","") or "sheet/derived" if _pos(row.get("EPS 1Y")) else src_eps_1y or "sheet/derived"))
    eps2_src = "yahoo_trend" if _pos(yh_eps.get("eps_2y")) else ("finnhub" if _pos(fh.get("eps_2y")) else ("sheet/derived" if _pos(row.get("EPS 2Y")) or _pos(eps_2y_est) else "none"))
    revc_src = "yahoo_financials" if revcg_yh.get("rev_cagr") is not None else ("sheet" if _f(row.get("Rev CAGR")) is not None else "none")

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if e0 else '—'}({src.get('eps_ttm','?') or src_eps_ttm}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_cagr={'ok' if _f(rev_cagr) is not None else '—'}({revc_src} ; clamp={_fmt_pct(REV_CAGR_MIN)}..{_fmt_pct(REV_CAGR_MAX)}), "
        f"eps_cagr={'ok' if _f(eps_cagr) is not None else '—'}(clamp={_fmt_pct(EPS_CAGR_MIN)}..{_fmt_pct(EPS_CAGR_MAX)}), "
        f"ebitda_ttm={'ok' if b0 else '—'}({src.get('ebitda_ttm','?')}), "
        f"shares={'ok' if shares else '—'}({src.get('shares','?')}), "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "sources": {
            **src,
            "eps_1y_source": eps1_src,
            "eps_2y_source": eps2_src,
            "rev_cagr_source": revc_src,
        },
        "cagr_clamped": {
            "rev_cagr_raw": _f(rev_cagr_raw),
            "rev_cagr_used": _f(rev_cagr),
            "eps_cagr_raw": _f(eps_cagr_raw),
            "eps_cagr_used": _f(eps_cagr),
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# app.py — Del 3/4
# ============================================================
# Analys-UI: bolagspresentation, käll-taggar, bläddringsvy,
# val av primär metod (med spar till Data), och "spara riktkurser"
# ============================================================

# ---------- Små UI-hjälpare ----------
def _fmt_money(v: Optional[float], ccy: str) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{float(v):,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} {ccy}"

def _fmt_num(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{float(v):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(v)

def _fmt_pct(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{100*float(v):.1f}%".replace(".", ",")
    except Exception:
        return str(v)

def _fmt_sek(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "0 SEK"
    try:
        return f"{float(v):,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

# ---------- Heuristik: välj primär metod ----------
if "_PREFER_ORDER" not in globals():
    _PREFER_ORDER = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]

def _pick_primary_from_table(met_df: pd.DataFrame, preset: Optional[str] = None) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if met_df is None or met_df.empty:
        return None, None, None, None, None
    available = set(met_df["Metod"].astype(str))
    chosen = None
    # 1) Använd radens "Primär metod" om den finns och är tillgänglig
    if preset and preset in available:
        chosen = preset
    # 2) Annars: flest datapunkter; tie-break med _PREFER_ORDER
    if chosen is None:
        counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
        if counts.empty:
            return None, None, None, None, None
        maxc = counts.max()
        candidates = [m for m in counts.index if counts[m] == maxc]
        for p in _PREFER_ORDER:
            if p in candidates:
                chosen = p
                break
        if chosen is None:
            chosen = candidates[0]
    row = met_df[met_df["Metod"] == chosen].iloc[0]
    return chosen, _f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"])

# ---------- Skriv "Primär metod" till Data-bladet ----------
def _save_primary_method_to_data(ticker: str, method: str):
    df = read_data_df()
    if df.empty or "Ticker" not in df.columns:
        st.warning("Kunde inte uppdatera primär metod (saknar Data-blad?).")
        return
    if "Primär metod" not in df.columns:
        df["Primär metod"] = np.nan
    mask = df["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if not mask.any():
        st.warning(f"{ticker}: fanns inte i Data-bladet.")
        return
    df.loc[mask, "Primär metod"] = method
    write_data_df(df)

# ---------- Spara riktkurser till Resultat ----------
def _save_targets_to_result(ticker: str, currency: str, method: Optional[str],
                            t0: Optional[float], t1: Optional[float], t2: Optional[float], t3: Optional[float]):
    res = _read_df(RESULT_TITLE)
    row = {
        "Timestamp": now_stamp(),
        "Ticker": ticker,
        "Valuta": currency,
        "Metod": method or "",
        "Riktkurs idag": t0,
        "Riktkurs 1 år": t1,
        "Riktkurs 2 år": t2,
        "Riktkurs 3 år": t3,
    }
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame([row]))
        return
    cols = list(res.columns)
    for k in row.keys():
        if k not in cols:
            cols.append(k)
            res[k] = np.nan
    # skriv över senaste rad för ticker om den finns, annars append
    mask = res["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if mask.any():
        idx = res.index[mask][-1]
        for k, v in row.items():
            res.at[idx, k] = v
    else:
        res = pd.concat([res, pd.DataFrame([row])[cols]], ignore_index=True)
    _write_df(RESULT_TITLE, res[cols])

# ---------- Hjälp: innehav/anskaffning/PNL i SEK ----------
def _holding_metrics(currency: str, price_now: Optional[float], shares_own: Optional[float], gav_sek: Optional[float], fx_rate: float) -> Dict[str, Optional[float]]:
    if not _pos(shares_own):
        return {"value_sek": None, "cost_sek": None, "pnl_sek": None, "pnl_pct": None}
    v_sek = None
    if _pos(price_now):
        try:
            v_sek = float(price_now) * float(shares_own) * float(fx_rate)
        except Exception:
            v_sek = None
    c_sek = None
    if _pos(gav_sek):
        try:
            c_sek = float(gav_sek) * float(shares_own)
        except Exception:
            c_sek = None
    pnl_sek = None
    pnl_pct = None
    if v_sek is not None and c_sek is not None and c_sek != 0:
        pnl_sek = v_sek - c_sek
        pnl_pct = pnl_sek / c_sek
    return {"value_sek": v_sek, "cost_sek": c_sek, "pnl_sek": pnl_sek, "pnl_pct": pnl_pct}

# ---------- Bolagskort (presentation + källor + metodval) ----------
def _company_card(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    tkr = str(row.get("Ticker","")).upper().strip()
    name = str((row.get("Bolagsnamn") if row.get("Bolagsnamn") not in [None, np.nan, ""] else tkr))
    bucket = str(row.get("Bucket") or "")
    preset_primary = str(row.get("Primär metod") or "").strip() or None

    st.markdown(f"### {tkr} • {name}" + (f" • {bucket}" if bucket else ""))

    # Kör beräkningsmotorn
    met_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    currency = meta.get("currency") or str(row.get("Valuta") or "USD").upper()
    price_now = meta.get("price")
    fx_rate = fx_map.get(currency, 1.0) or 1.0

    # Val av primär metod (default = preset/heuristik)
    default_method, t0_d, t1_d, t2_d, t3_d = _pick_primary_from_table(met_df, preset_primary)

    # UI: välj metod
    st.caption("Välj värderingssätt (primär metod). Tabellen visar alla metoder under.")
    method_choices = list(met_df["Metod"].astype(str))
    method_sel = st.selectbox("Primär metod", method_choices, index=method_choices.index(default_method) if default_method in method_choices else 0, key=f"method_{tkr}")

    # Targets för vald metod
    row_sel = met_df[met_df["Metod"] == method_sel].iloc[0]
    t0, t1, t2, t3 = _f(row_sel["Idag"]), _f(row_sel["1 år"]), _f(row_sel["2 år"]), _f(row_sel["3 år"])

    cols = st.columns(4)
    cols[0].metric("Idag", _fmt_money(t0, currency))
    cols[1].metric("1 år", _fmt_money(t1, currency))
    cols[2].metric("2 år", _fmt_money(t2, currency))
    cols[3].metric("3 år", _fmt_money(t3, currency))

    # Uppsida vs aktuell kurs
    if _pos(price_now):
        up_cols = st.columns(4)
        for i, (lbl, tgt) in enumerate([("Idag", t0), ("1 år", t1), ("2 år", t2), ("3 år", t3)]):
            if _pos(tgt):
                delta_pct = (tgt/price_now - 1.0)
                up_cols[i].metric(f"Uppsida {lbl}", _fmt_pct(delta_pct))

    # Innehav/anskaffning/PNL i SEK
    try:
        shares_own = _f(row.get("Antal aktier")) or 0.0
        gav_sek    = _f(row.get("GAV (SEK)"))
        hm = _holding_metrics(currency, price_now, shares_own, gav_sek, fx_rate)
        with st.expander("📦 Innehav & P/L (SEK)", expanded=False):
            st.write(f"• Innehavsvärde: {_fmt_sek(hm['value_sek'])}")
            st.write(f"• Anskaffningsvärde: {_fmt_sek(hm['cost_sek'])}")
            if hm["pnl_sek"] is not None:
                st.write(f"• P/L: {_fmt_sek(hm['pnl_sek'])} ({_fmt_pct(hm['pnl_pct'])})")
    except Exception:
        pass

    # Metodtabell (kompakt)
    with st.expander("📊 Metoder & målpriser (alla)", expanded=False):
        st.dataframe(met_df, use_container_width=True)

    # Källor & beräkningsväg
    with st.expander("🔎 Källor & beräkningsväg", expanded=True):
        sources = meta.get("sources", {}) or {}
        paths = {
            "EPS-path": meta.get("eps_path"),
            "REV-path": meta.get("rev_path"),
            "EBITDA-path": meta.get("ebitda_path"),
        }
        left, right = st.columns(2)
        with left:
            st.markdown("**Ankare & parametrar**")
            st.write(f"• **PE-ankare:** { _fmt_num(meta.get('pe_anchor')) }")
            st.write(f"• **Multipel-decay/år:** { settings.get('multiple_decay','0.10') }")
            st.write(f"• **Vikt TTM i PE-ankare:** { settings.get('pe_anchor_weight_ttm','0.50') }")
            st.write(f"• **Valuta:** {currency}  • **FX:** {fx_rate:.3f}")
            st.write(f"• **Aktuell kurs:** {_fmt_money(price_now, currency)}")
        with right:
            st.markdown("**Källor (hämtade/deriverade)**")
            if sources:
                src_rows = sorted([(k, sources[k]) for k in sources.keys()])
                st.dataframe(pd.DataFrame(src_rows, columns=["Fält","Källa"]), use_container_width=True, height=260)
            else:
                st.caption("Inga käll-taggar tillgängliga.")
        st.markdown("**Beräkningsvägar**")
        st.json(paths)

    # Utdelningsprognos (om fält finns i Data)
    try:
        shares_own = _f(row.get("Antal aktier")) or 0.0
        dps_now = _f(row.get("Årlig utdelning"))
        dps_cagr = _f(row.get("Utdelning CAGR"))
        divs = forecast_dividends_net_sek(currency, shares_own, dps_now, dps_cagr, fx_map, settings)
        with st.expander("💰 Utdelning (netto SEK, prognos 1–3 år)", expanded=False):
            st.write(f"• 1 år: {_fmt_sek(divs['y1'])}  • 2 år: {_fmt_sek(divs['y2'])}  • 3 år: {_fmt_sek(divs['y3'])}")
    except Exception:
        pass

    # Åtgärdsknappar
    c1, c2, c3 = st.columns(3)
    if c1.button("💾 Spara primär metod", key=f"saveprim_{tkr}"):
        _save_primary_method_to_data(tkr, method_sel)
        st.success(f"Primär metod '{method_sel}' sparad för {tkr}.")

    if c2.button("🧮 Spara riktkurser → Resultat", key=f"saveres_{tkr}"):
        _save_targets_to_result(tkr, currency, method_sel, t0, t1, t2, t3)
        st.success("Riktkurser sparade till fliken Resultat.")

    if c3.button("♻️ Uppdatera EPS 1Y/CAGR i Data", key=f"upd_est_{tkr}"):
        # Enkel uppdatering: räkna om EPS CAGR om ttm + 1y finns (och skriv in eps 1y)
        df = read_data_df()
        mask = df["Ticker"].astype(str).str.upper() == tkr
        if mask.any():
            e0 = meta.get("eps_path", {}).get("ttm")
            e1 = meta.get("eps_path", {}).get("y1")
            new_cagr = None
            if _pos(e0) and _pos(e1):
                try:
                    new_cagr = (float(e1)/float(e0)) - 1.0
                except Exception:
                    new_cagr = None
            if "EPS 1Y" not in df.columns: df["EPS 1Y"] = np.nan
            if "EPS CAGR" not in df.columns: df["EPS CAGR"] = np.nan
            if _pos(e1): df.loc[mask, "EPS 1Y"] = float(e1)
            if new_cagr is not None: df.loc[mask, "EPS CAGR"] = float(new_cagr)
            write_data_df(df)
            st.success("Estimat/CAGR uppdaterade i Data.")
        else:
            st.warning("Kunde inte hitta raden i Data för uppdatering.")

    st.caption(f"Sanity: {sanity}")

    return method_sel, t0, t1, t2, t3, meta

# ---------- Analys-sida (bläddringsvy, sorterad på uppsida mot fair value 'Idag') ----------
def page_analysis():
    st.header("🔬 Analys")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()

    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    # Filter
    f1, f2, f3, f4 = st.columns(4)
    buckets = f1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_only = f2.checkbox("Visa endast innehav (antal > 0)", value=False)
    hide_zero_price = f3.checkbox("Dölj bolag utan aktuell kurs", value=True)
    undervalued_only = f4.checkbox("Visa endast undervärderade (fair idag > pris)", value=False)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_only:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    if hide_zero_price:
        q = q[(pd.to_numeric(q["Aktuell kurs"], errors="coerce") > 0)]

    if q.empty:
        st.warning("Inget att visa efter filter.")
        return

    # Beräkna fair value (Idag) för sortering – använd aktuell "Primär metod" som preset
    progress = st.progress(0.0)
    scored: List[Tuple[str, float, Dict[str, Any], pd.Series]] = []
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            preset = str(r.get("Primär metod") or "").strip() or None
            method, t0, _, _, _ = _pick_primary_from_table(met_df, preset)
            price = meta.get("price")
            up = None
            if _pos(price) and _pos(t0):
                up = float(t0)/float(price) - 1.0
            scored.append((r.get("Ticker"), up if up is not None else -9e9, {"method": method, "t0": t0, "price": price}, r))
        except Exception:
            scored.append((r.get("Ticker"), -9e9, {"method": None, "t0": None, "price": None}, r))
        progress.progress((i+1)/len(q))
    progress.empty()

    # Filtrera undervärderade om valt
    if undervalued_only:
        scored = [t for t in scored if (t[2].get("t0") is not None and t[2].get("price") is not None and t[2]["t0"] > t[2]["price"])]

    # Sortera: störst uppsida först
    scored.sort(key=lambda x: (x[1] is None, -x[1] if x[1] is not None else -9e9))
    ordered_rows = [t[3] for t in scored]

    if not ordered_rows:
        st.info("Inga poster uppfyllde kriterierna.")
        return

    # Bläddringsindex i session_state
    key_idx = "analysis_idx"
    if key_idx not in st.session_state:
        st.session_state[key_idx] = 0

    # Valbar starttiker (hoppa direkt)
    tkr_options = [str(r.get("Ticker")) for r in ordered_rows]
    jump = st.selectbox("Gå direkt till bolag", tkr_options, index=st.session_state[key_idx] if 0 <= st.session_state[key_idx] < len(tkr_options) else 0)
    if jump in tkr_options:
        st.session_state[key_idx] = tkr_options.index(jump)

    # Navigering
    cprev, cpos, cnext = st.columns([1,2,1])
    with cprev:
        if st.button("⬅️ Föregående", use_container_width=True, disabled=(st.session_state[key_idx] <= 0)):
            st.session_state[key_idx] = max(0, st.session_state[key_idx]-1)
    with cpos:
        st.write(f"**{st.session_state[key_idx]+1} / {len(ordered_rows)}** — sorterat efter störst uppsida")
    with cnext:
        if st.button("Nästa ➡️", use_container_width=True, disabled=(st.session_state[key_idx] >= len(ordered_rows)-1)):
            st.session_state[key_idx] = min(len(ordered_rows)-1, st.session_state[key_idx]+1)

    # Rendera just den valda posten
    row = ordered_rows[st.session_state[key_idx]]
    with st.container(border=True):
        _company_card(row, settings, fx_map)
        st.markdown("---")

# app.py — Del 4/4
# ============================================================
# Sidor: Editor / Ranking / Inställningar / Batch + Snapshot & main()
# ============================================================

# ---------- Snapshot → fliken "Snapshot" ----------
def save_quarter_snapshot(ticker: str, methods_df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    snap = _read_df(SNAPSHOT_TITLE)
    ts = now_stamp()
    rows = []
    for _, r in methods_df.iterrows():
        rows.append({
            "Timestamp": ts,
            "Ticker": ticker,
            "Valuta": meta.get("currency"),
            "Metod": r.get("Metod"),
            "Idag": _f(r.get("Idag")),
            "1 år": _f(r.get("1 år")),
            "2 år": _f(r.get("2 år")),
            "3 år": _f(r.get("3 år")),
            "Ankare PE": _f(meta.get("pe_anchor")),
            "Decay": _f(meta.get("decay")),
        })
    out = pd.DataFrame(rows)
    if snap.empty:
        _write_df(SNAPSHOT_TITLE, out)
    else:
        # Säkerställ unions-kolumner
        for c in out.columns:
            if c not in snap.columns:
                snap[c] = np.nan
        for c in snap.columns:
            if c not in out.columns:
                out[c] = np.nan
        snap = pd.concat([snap[snap.columns], out[snap.columns]], ignore_index=True)
        _write_df(SNAPSHOT_TITLE, snap)

# ============================================================
#                       SIDA: Editor
# ============================================================
def page_editor():
    st.header("📝 Lägg till / Uppdatera bolag")

    df = read_data_df()

    # Välj befintlig eller nytt
    tickers = ["— nytt —"] + (sorted(df["Ticker"].dropna().astype(str).unique().tolist()) if not df.empty else [])
    tkr_sel = st.selectbox("Välj ticker", tickers, index=0, key="editor_tkr_sel")
    is_new  = (tkr_sel == "— nytt —")

    # Session-state för prefill (påverkas av "Hämta & fyll från Yahoo")
    if "editor_prefill" not in st.session_state:
        st.session_state["editor_prefill"] = {}

    # Grund-init från Data-bladet om befintlig
    init = {c: None for c in DATA_COLUMNS}
    if not is_new and not df.empty:
        row = df[df["Ticker"].astype(str) == tkr_sel].iloc[0].to_dict()
        for k in DATA_COLUMNS:
            init[k] = row.get(k, None)

    # Slå ihop med ev. prefill
    merged = dict(init)
    merged.update({k: v for k, v in st.session_state["editor_prefill"].items() if v is not None})

    st.caption("Tips: Använd **Hämta & fyll från Yahoo** för att auto-populera formuläret. Spara sedan.")

    with st.form("edit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        ticker  = c1.text_input("Ticker", value="" if is_new else tkr_sel).strip().upper()
        name    = c2.text_input("Bolagsnamn", value=str(_nz(merged.get("Bolagsnamn"), "")))
        sector  = c3.text_input("Sektor", value=str(_nz(merged.get("Sektor"), "")))

        bucket_choices = DEFAULT_BUCKETS
        bucket_idx = bucket_choices.index(_nz(merged.get("Bucket"), bucket_choices[0])) if _nz(merged.get("Bucket"), bucket_choices[0]) in bucket_choices else 0
        bucket  = st.selectbox("Bucket/Kategori", bucket_choices, index=bucket_idx)
        valuta  = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"], index=["USD","EUR","NOK","CAD","SEK"].index(str(_nz(merged.get("Valuta"), "USD")).upper()))

        d1, d2, d3, d4 = st.columns(4)
        antal   = d1.number_input("Antal aktier", step=1, value=int(_nz(_f(merged.get("Antal aktier")), 0)))
        gav_sek = d2.number_input("GAV (SEK)", step=0.01, value=float(_nz(_f(merged.get("GAV (SEK)")), 0.0)))
        kurs    = d3.number_input("Aktuell kurs", step=0.01, value=float(_nz(_f(merged.get("Aktuell kurs")), 0.0)))
        shares  = d4.number_input("Utestående aktier", step=1.0, value=float(_nz(_f(merged.get("Utestående aktier")), 0.0)))

        e1, e2, e3, e4 = st.columns(4)
        rev_ttm   = e1.number_input("Rev TTM", step=1000.0, value=float(_nz(_f(merged.get("Rev TTM")), 0.0)))
        ebitda_t  = e2.number_input("EBITDA TTM (kan vara negativ)", value=float(_nz(_f(merged.get("EBITDA TTM")), 0.0)))
        eps_ttm   = e3.number_input("EPS TTM (kan vara negativ)", value=float(_nz(_f(merged.get("EPS TTM")), 0.0)))
        net_debt  = e4.number_input("Net debt (kan vara negativ)", value=float(_nz(_f(merged.get("Net debt")), 0.0)))

        f1, f2, f3, f4 = st.columns(4)
        pe_ttm   = f1.number_input("PE TTM", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("PE TTM")), 0.0)))
        pe_fwd   = f2.number_input("PE FWD", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("PE FWD")), 0.0)))
        ev_rev   = f3.number_input("EV/Revenue", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("EV/Revenue")), 0.0)))
        ev_ebit  = f4.number_input("EV/EBITDA", step=0.01, value=float(_nz(_f(merged.get("EV/EBITDA")), 0.0)))

        g1, g2, g3, g4 = st.columns(4)
        pb      = g1.number_input("P/B", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("P/B")), 0.0)))
        bvps    = g2.number_input("BVPS", step=0.01, value=float(_nz(_f(merged.get("BVPS")), 0.0)))
        eps1y   = g3.number_input("EPS 1Y (estimat)", value=float(_nz(_f(merged.get("EPS 1Y")), 0.0)))
        eps2y   = g4.number_input("EPS 2Y (estimat)", value=float(_nz(_f(merged.get("EPS 2Y")), 0.0)))

        h1, h2, h3, h4 = st.columns(4)
        revcg   = h1.number_input("Rev CAGR", step=0.001, value=float(_nz(_f(merged.get("Rev CAGR")), 0.0)))
        dps     = h2.number_input("Årlig utdelning (DPS)", step=0.01, value=float(_nz(_f(merged.get("Årlig utdelning")), 0.0)))
        dpscg   = h3.number_input("Utdelning CAGR", step=0.001, value=float(_nz(_f(merged.get("Utdelning CAGR")), 0.0)))
        prim_choices = _PREFER_ORDER
        prim_default = str(_nz(merged.get("Primär metod"), prim_choices[0]))
        prim_idx = prim_choices.index(prim_default) if prim_default in prim_choices else 0
        prim    = h4.selectbox("Primär metod", prim_choices, index=prim_idx)

        c_left, c_right = st.columns(2)
        fetch_btn = c_left.form_submit_button("🔎 Hämta & fyll från Yahoo")
        save_btn  = c_right.form_submit_button("💾 Spara till Data")

    # Hantera "Hämta & fyll"
    if fetch_btn:
        if not ticker:
            st.warning("Ange en ticker först.")
            st.stop()

        # 1) Snapshot + säkra pris/valuta/nycklar
        snap = fetch_yahoo_snapshot(ticker)

        # 2) Hämta bolagsnamn separat (säker fallback)
        comp_name = None
        try:
            tk = yf.Ticker(ticker)
            try:
                inf = tk.info or {}
            except Exception:
                inf = {}
            comp_name = inf.get("longName") or inf.get("shortName") or None
            if not comp_name:
                # Fallback via calendar/events? Om inget: lämna None
                pass
        except Exception:
            comp_name = None

        # 3) Fallback för pris om saknas
        px = snap.get("price")
        if not _pos(px):
            try:
                hist = yf.Ticker(ticker).history(period="5d")
                if not hist.empty:
                    px = float(hist["Close"].dropna().iloc[-1])
            except Exception:
                px = None

        # 4) Lägg in vettiga fält i prefill och kör om
        st.session_state["editor_prefill"] = {
            "Ticker": ticker,
            "Bolagsnamn": comp_name or _nz(name, ""),
            "Valuta": snap.get("currency"),
            "Aktuell kurs": px,
            "Rev TTM": snap.get("revenue_ttm"),
            "EBITDA TTM": snap.get("ebitda_ttm"),
            "EPS TTM": snap.get("eps_ttm"),
            "PE TTM": snap.get("pe_ttm"),
            "PE FWD": snap.get("pe_fwd"),
            "EV/Revenue": snap.get("ev_to_sales"),
            "EV/EBITDA": snap.get("ev_to_ebitda"),
            "P/B": snap.get("p_to_book"),
            "BVPS": snap.get("bvps"),
            "Net debt": snap.get("net_debt"),
            "Utestående aktier": snap.get("shares"),
        }
        st.success("Fält förifyllda från Yahoo – granska och klicka **Spara**.")
        st.rerun()

    # Hantera "Spara"
    if save_btn:
        if not ticker:
            st.warning("Ticker saknas.")
            st.stop()
        new_row = {
            "Timestamp": now_stamp(),
            "Ticker": ticker,
            "Bolagsnamn": name,
            "Sektor": sector,
            "Bucket": bucket,
            "Valuta": valuta,
            "Antal aktier": antal,
            "GAV (SEK)": gav_sek,
            "Aktuell kurs": kurs,
            "Utestående aktier": shares,
            "Net debt": net_debt,
            "Rev TTM": rev_ttm,
            "EBITDA TTM": ebitda_t,
            "EPS TTM": eps_ttm,
            "PE TTM": pe_ttm,
            "PE FWD": pe_fwd,
            "EV/Revenue": ev_rev,
            "EV/EBITDA": ev_ebit,
            "P/B": pb,
            "BVPS": bvps,
            "EPS 1Y": eps1y,
            "EPS 2Y": eps2y,
            "Rev CAGR": revcg,
            "EPS CAGR": np.nan,   # uppdateras via Analys-knappen "Uppdatera EPS 1Y/CAGR"
            "Årlig utdelning": dps,
            "Utdelning CAGR": dpscg,
            "Primär metod": prim,
            "Senast auto uppdaterad": "",
            "Auto källa": "Manuell",
        }
        df_new = read_data_df()
        # säkerställ alla kolumner finns
        for c in DATA_COLUMNS:
            if c not in df_new.columns:
                df_new[c] = np.nan
        if (df_new["Ticker"].astype(str).str.upper() == ticker).any():
            mask = df_new["Ticker"].astype(str).str.upper() == ticker
            for k, v in new_row.items():
                df_new.loc[mask, k] = v
        else:
            # append med exakt kolumnordning
            df_new = pd.concat([df_new, pd.DataFrame([new_row])[df_new.columns]], ignore_index=True)
        write_data_df(df_new)
        st.session_state["editor_prefill"] = {}  # töm prefill när vi sparat
        st.success("Sparat till Data.")
        st.rerun()

# ============================================================
#                      SIDA: Ranking
# ============================================================
def page_ranking():
    st.header("🏁 Ranking – Prioritera lägsta portföljandel i bucket & uppsida")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    b1, b2, b3 = st.columns(3)
    buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_tab = b2.selectbox("Urval", ["Innehav (antal > 0)","Watchlist (antal = 0)"], index=0)
    only_underval = b3.checkbox("Visa endast undervärderade (fair idag > pris)", value=True)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_tab.startswith("Innehav"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    else:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    if q.empty:
        st.info("Inget att visa efter filter.")
        return

    rows = []
    prog = st.progress(0.0)
    # För bucket-andel krävs total värde per bucket
    tmp_vals: List[Dict[str, Any]] = []
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            preset = str(_nz(r.get("Primär metod"), "")).strip() or None
            method, fair_today, _, _, _ = _pick_primary_from_table(met_df, preset)
            price = meta.get("price")
            currency = meta.get("currency") or str(_nz(r.get("Valuta"), "USD")).upper()
            fx_rate = fx_map.get(currency, 1.0) or 1.0
            shares_own = _f(r.get("Antal aktier")) or 0.0
            value_sek = (float(price)*shares_own*fx_rate) if (_pos(price) and shares_own>0) else 0.0
            upside = None
            if _pos(price) and _pos(fair_today):
                upside = (fair_today/price - 1.0) * 100.0
            tmp_vals.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": currency,
                "Pris": price,
                "Primär metod": method,
                "Fair value (Idag)": fair_today,
                "Uppsida %": upside,
                "Value SEK": value_sek,
            })
            time.sleep(0.10)
        except Exception:
            tmp_vals.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None, "Primär metod": None, "Fair value (Idag)": None, "Uppsida %": None,
                "Value SEK": 0.0
            })
        prog.progress((i+1)/max(1,len(q)))
    prog.empty()

    out = pd.DataFrame(tmp_vals)

    # Beräkna bucket-totals & andel
    if not out.empty:
        totals = out.groupby("Bucket")["Value SEK"].sum().rename("Bucket Total SEK")
        out = out.merge(totals, on="Bucket", how="left")
        out["Bucket Total SEK"] = out["Bucket Total SEK"].replace({0.0: np.nan})
        out["Andel i bucket"] = out["Value SEK"] / out["Bucket Total SEK"]
        # Andel NaN (bucket utan innehav) → 1.0 för att hamna sist
        out["Andel i bucket"] = out["Andel i bucket"].fillna(1.0)

        # Undervärderad-flagga
        out["Undervärderad"] = (_pos(out["Pris"]) & _pos(out["Fair value (Idag)"]) & (out["Fair value (Idag)"] > out["Pris"])).astype(int)

        if only_underval:
            out = out[out["Undervärderad"] == 1]

        # Sortera: undervärderad (1->0), lägsta andel i bucket, störst uppsida
        out = out.sort_values(by=["Undervärderad","Andel i bucket","Uppsida %"],
                              ascending=[False, True, False], na_position="last")

    st.dataframe(out[[
        "Ticker","Bolagsnamn","Bucket","Valuta","Pris","Primär metod",
        "Fair value (Idag)","Uppsida %","Value SEK","Andel i bucket"
    ]], use_container_width=True)

# ============================================================
#                     SIDA: Inställningar
# ============================================================
def page_settings():
    st.header("⚙️ Inställningar")
    settings = get_settings_map()

    st.subheader("Källskatt per valuta")
    currencies = ["USD","EUR","NOK","CAD","SEK"]
    with st.form("wh_form"):
        cols = st.columns(len(currencies))
        vals = {}
        for i, ccy in enumerate(currencies):
            key = f"withholding_{ccy}"
            cur = float(settings.get(key, "0.15" if ccy!="SEK" else "0.0"))
            vals[ccy] = cols[i].number_input(f"{ccy}", min_value=0.0, max_value=1.0, step=0.01, value=cur, format="%.2f")
        w_submit = st.form_submit_button("💾 Spara källskatt")
    if w_submit:
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def upsert(sdf, k, v):
            if (sdf["Key"] == k).any():
                sdf.loc[sdf["Key"] == k, "Value"] = str(v)
            else:
                sdf = pd.concat([sdf, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)
            return sdf
        for ccy, v in vals.items():
            s = upsert(s, f"withholding_{ccy}", v)
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    p1, p2 = st.columns(2)
    pe_w  = p1.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05, value=float(settings.get("pe_anchor_weight_ttm","0.5")))
    decay = p2.number_input("Multipel-decay per år (0..1)", min_value=0.0, max_value=1.0, step=0.01, value=float(settings.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty: s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def setv(sdf, k, v):
            if (sdf["Key"] == k).any():
                sdf.loc[sdf["Key"] == k, "Value"] = str(v)
            else:
                sdf.loc[len(sdf)] = [k, str(v)]
            return sdf
        s = setv(s, "pe_anchor_weight_ttm", pe_w)
        s = setv(s, "multiple_decay", decay)
        _write_df(SETTINGS_TITLE, s)
        st.success("Parametrar uppdaterade.")

    st.subheader("Valutakurser")
    if st.button("🔄 Hämta & uppdatera FX från Yahoo"):
        mp = _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")
        st.json(mp)

# ============================================================
#                     SIDA: Batch
# ============================================================
def page_batch():
    st.header("🧰 Batch-uppdatering")
    df       = read_data_df()
    if df.empty:
        st.info("Inga bolag i Data.")
        return

    throttle = st.slider("Fördröjning per bolag (sek)", min_value=0.1, max_value=2.0, value=0.6, step=0.1)

    if st.button("🔄 Uppdatera pris & nycklar från Yahoo (alla)"):
        prog = st.progress(0.0)
        df2 = df.copy()
        for i, (idx, r) in enumerate(df2.iterrows()):
            tkr = str(r["Ticker"]).strip().upper()
            snap = fetch_yahoo_snapshot(tkr)
            # Skriv över fält vi kan
            if snap.get("price") is not None:       df2.at[idx, "Aktuell kurs"] = snap["price"]
            if snap.get("currency"):                df2.at[idx, "Valuta"] = snap["currency"]
            if snap.get("revenue_ttm") is not None: df2.at[idx, "Rev TTM"] = snap["revenue_ttm"]
            if snap.get("ebitda_ttm") is not None:  df2.at[idx, "EBITDA TTM"] = snap["ebitda_ttm"]
            if snap.get("eps_ttm") is not None:     df2.at[idx, "EPS TTM"] = snap["eps_ttm"]
            if snap.get("pe_ttm") is not None:      df2.at[idx, "PE TTM"] = snap["pe_ttm"]
            if snap.get("pe_fwd") is not None:      df2.at[idx, "PE FWD"] = snap["pe_fwd"]
            if snap.get("ev_to_sales") is not None: df2.at[idx, "EV/Revenue"] = snap["ev_to_sales"]
            if snap.get("ev_to_ebitda") is not None:df2.at[idx, "EV/EBITDA"] = snap["ev_to_ebitda"]
            if snap.get("p_to_book") is not None:   df2.at[idx, "P/B"] = snap["p_to_book"]
            if snap.get("bvps") is not None:        df2.at[idx, "BVPS"] = snap["bvps"]
            if snap.get("net_debt") is not None:    df2.at[idx, "Net debt"] = snap["net_debt"]
            df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
            df2.at[idx, "Auto källa"] = "Yahoo"
            time.sleep(throttle)
            prog.progress((i+1)/len(df2))
        write_data_df(df2)
        prog.empty()
        st.success("Uppdaterat alla tickers från Yahoo.")

    if st.button("📷 Spara snapshots (alla)"):
        settings = get_settings_map()
        fx_map   = get_fx_map()
        prog = st.progress(0.0)
        count = 0
        for i, (_, r) in enumerate(df.iterrows()):
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            save_quarter_snapshot(str(r["Ticker"]).strip().upper(), met_df, meta)
            count += 1
            time.sleep(throttle)
            prog.progress((i+1)/len(df))
        prog.empty()
        st.success(f"Snapshot sparade för {count} bolag.")

# ============================================================
#                          MAIN
# ============================================================
def run_main_ui():
    st.title(APP_TITLE)

    # Auto-init: uppdatera FX en gång per session
    if "fx_inited" not in st.session_state:
        try:
            _load_fx_and_update_sheet()
        except Exception:
            pass
        st.session_state["fx_inited"] = True

    # Snabbstatus (valfritt)
    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        st.write("FX:", get_fx_map())
        st.write("Settings:", get_settings_map())

    page = st.sidebar.radio("Sidor", ["Editor", "Analys", "Ranking", "Inställningar", "Batch"], index=1)

    if page == "Editor":
        page_editor()
    elif page == "Analys":
        page_analysis()   # från Del 3/4
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
