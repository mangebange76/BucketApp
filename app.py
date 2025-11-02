# app.py — Del 1/4
# ============================================================
# Bas: Streamlit-app för fair value / riktkurser / portfölj
# Lagring: Google Sheets (Data, Resultat, Valutakurser, Settings, Snapshot)
# Hämtning: Yahoo (yfinance) + (Del 2: Finnhub/Yahoo-trend fallbacks)
# ============================================================

from __future__ import annotations
import os, json, math, time, random
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
    # normalisera
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

# app.py — Del 2/4
# ============================================================
# Datainsamling (Yahoo först, Finnhub som fallback) + beräkningshjälpare
# ============================================================

import requests

# -------------------------
# Hjälpare
# -------------------------
def _nz(x, fallback=None):
    """Returnera x om det är ett giltigt tal/objekt, annars fallback."""
    return x if (x is not None and x == x) else fallback

def _safe_float(x) -> Optional[float]:
    """Som _f men snällare när strängar innehåller tusentals- eller decimaltecken."""
    return _f(x)

def _as_pct_float(v) -> Optional[float]:
    """'12.3%' -> 0.123; 0.123 -> 0.123."""
    if v is None: 
        return None
    if isinstance(v, str):
        s = v.strip().replace(",", ".").replace(" ", "")
        if s.endswith("%"):
            s = s[:-1]
        try:
            return float(s)/100.0
        except Exception:
            return None
    return _f(v)

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

    set_if_missing("price",        gi("currentPrice"),      "yahoo_info")
    set_if_missing("currency",     gi("currency"),          "yahoo_info")
    set_if_missing("market_cap",   gi("marketCap"),         "yahoo_info")
    set_if_missing("eps_ttm",      gi("trailingEps"),       "yahoo_info")
    set_if_missing("pe_ttm",       gi("trailingPE"),        "yahoo_info")
    set_if_missing("pe_fwd",       gi("forwardPE"),         "yahoo_info")
    set_if_missing("revenue_ttm",  gi("totalRevenue"),      "yahoo_info")
    set_if_missing("ebitda_ttm",   gi("ebitda"),            "yahoo_info")
    set_if_missing("ev_to_sales",  gi("enterpriseToRevenue"), "yahoo_info")
    set_if_missing("ev_to_ebitda", gi("enterpriseToEbitda"),  "yahoo_info")
    set_if_missing("p_to_book",    gi("priceToBook"),       "yahoo_info")
    set_if_missing("bvps",         gi("bookValue"),         "yahoo_info")

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
# Yahoo – EPS-estimat (1–2 år) via analyst/earnings trend
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker hämta EPS-estimat 1–2 år från Yahoo-källor via yfinance.
      1) get_earnings_trend() → epsTrend.nextYear för 1Y (om finns)
      2) get_analysis() → Growth Estimates 'Next 5 Years (per annum)' som CAGR
         för att modellera 2Y = 1Y * (1+g)
    Fallback: returnerar None om inget hittas.
    """
    tk = yf.Ticker(ticker)
    eps_1y, eps_2y = None, None
    used = []

    # 1) Earnings Trend
    try:
        df = None
        try:
            df = tk.get_earnings_trend()
        except Exception:
            # vissa versioner exponerar .earnings_trend
            df = getattr(tk, "earnings_trend", None)
        if df is not None and hasattr(df, "empty") and not df.empty:
            # kolumnnamn varierar - försök hitta 'epsTrend.nextYear' eller 'epsTrend_nextYear'
            cand_cols = [c for c in df.columns if "epsTrend" in str(c)]
            # ofta är det kolumner på formen 'epsTrend.nextYear'
            next_year_cols = [c for c in cand_cols if "nextYear" in str(c)]
            if next_year_cols:
                # ta sista icke-NaN
                ser = df[next_year_cols[0]].dropna()
                if not ser.empty:
                    eps_1y = _safe_float(ser.iloc[-1])
                    if _pos(eps_1y): 
                        used.append("yahoo_trend_1y")
    except Exception:
        pass

    # 2) Analysis – Growth Estimates (Next 5 Years per annum) → CAGR
    g5 = None
    try:
        an = tk.get_analysis()
        if an is not None and hasattr(an, "empty") and not an.empty:
            # Robust plock: hitta rad som innehåller 'Next 5 Years (per annum)'
            idx = [i for i in an.index if isinstance(i, str) and "Next 5 Years" in i]
            if idx:
                row = an.loc[idx[0]]
                # kolumn 'Avg' finns ofta
                for key in ["Avg", "avg", "Estimate", 0]:
                    if key in row.index:
                        g5 = _as_pct_float(row[key])
                        break
                if g5 is None:
                    # ibland ligger värdena som strängar i första kolumnen
                    try:
                        g5 = _as_pct_float(row.iloc[0])
                    except Exception:
                        g5 = None
                if g5 is not None:
                    used.append("yahoo_analysis_g5")
            # Analysis kan också ge EPS för 'Next Year' under 'Earnings Estimate'
            # hitta tvärsnitt där index innehåller 'Earnings Estimate' och kolumn 'Next Year'/'Avg'
            try:
                # multiindex-skydd
                if isinstance(an.columns, pd.MultiIndex):
                    # leta efter ('Earnings Estimate','Avg')
                    if ("Earnings Estimate","Avg") in an.columns and "Next Year" in an.index:
                        eps_1y = _safe_float(an.loc["Next Year", ("Earnings Estimate","Avg")]) or eps_1y
                        if _pos(eps_1y): used.append("yahoo_analysis_1y")
                else:
                    if "Next Year" in an.index and "Avg" in an.columns:
                        eps_1y = _safe_float(an.loc["Next Year","Avg"]) or eps_1y
                        if _pos(eps_1y): used.append("yahoo_analysis_1y")
            except Exception:
                pass
    except Exception:
        pass

    # Härled 2Y från 1Y + g5 om möjligt
    if _pos(eps_1y) and g5 is not None:
        try:
            eps_2y = float(eps_1y) * (1.0 + float(g5))
            used.append("derived_2y_from_g5")
        except Exception:
            eps_2y = None

    return {"eps_1y": _pos(eps_1y), "eps_2y": _pos(eps_2y), "source": "+".join(used) if used else "none"}

# -------------------------
# Finnhub (fallback) – EPS-estimat 1–2 år
# -------------------------
def _get_finnhub_key() -> Optional[str]:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

@st.cache_data(ttl=900, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Fallback om Yahoo inte ger något: hämta EPS-estimat 1–2 år från Finnhub (om API-nyckel finns).
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
            if len(vals) >= 1:
                eps_1y = vals[-1]
            if len(vals) >= 2:
                eps_2y = vals[-2]
        return {"eps_1y": _pos(eps_1y), "eps_2y": _pos(eps_2y), "source": "finnhub"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

# -------------------------
# Multipel-decay & ankar-P/E
# -------------------------
def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    """Minska multipeln med decay per år (linjär mot ett golv)."""
    m0 = _pos(mult0)
    if m0 is None:
        return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_frac
    return max(m, floor)

def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float) -> Optional[float]:
    """Viktad ankare mellan TTM och FWD (t.ex. 50/50)."""
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
# Price builders för EV- och P/x-metoder
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
    """
    Om EPS saknas men vi har price+PE, härled EPS. Returnerar (eps_ttm, src_ttm, eps_1y, src_1y)
    """
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
    """
    Returnerar (eps_0, eps_1, eps_2, eps_3).
    • Om eps_1y saknas men eps_cagr finns: extrapolera från ttm.
    • Om eps_2y saknas men eps_1y+eps_cagr finns: extrapolera ett år till.
    • eps_3y extrapoleras vidare om eps_cagr finns.
    """
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
    """
    Proxy: EBITDA växer ungefär i takt med omsättning (om vi saknar riktiga prognoser).
    Om rev-path saknas -> håll ebitda konstant.
    """
    e0 = _pos(ebitda_ttm)
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return e0, e0, e0, e0
    def scale(r): return (e0 * (r / rev0)) if (r and rev0) else e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# app.py — Del 3/4
# ============================================================
# Huvudmotor per rad + Analys-UI (bolagskort, bläddring, spar)
# ============================================================

# -------------------------
# Huvudmotor per rad
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Beräknar metodtabell (Idag, 1,2,3 år) för raden.
    Returnerar (methods_df, sanity_text, meta)
    meta innehåller: currency, price, shares_out, net_debt, pe_anchor, decay, sources{}
    """
    ticker = str(row.get("Ticker", "")).strip().upper()
    if not ticker:
        return pd.DataFrame(columns=["Metod","Idag","1 år","2 år","3 år"]), "saknar ticker", {}

    # 1) Live-data
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.25)  # mild throttling
    yest = fetch_yahoo_eps_estimates(ticker)

    # Fallback till Finnhub om Yahoo inte gav något
    if not _pos(yest.get("eps_1y")) or not _pos(yest.get("eps_2y")):
        fest = fetch_finnhub_estimates(ticker)
        if not _pos(yest.get("eps_1y")) and _pos(fest.get("eps_1y")):
            yest["eps_1y"] = fest["eps_1y"]
            yest["source"] = (yest.get("source") + "+finnhub_1y") if yest.get("source") else "finnhub_1y"
        if not _pos(yest.get("eps_2y")) and _pos(fest.get("eps_2y")):
            yest["eps_2y"] = fest["eps_2y"]
            yest["source"] = (yest.get("source") + "+finnhub_2y") if yest.get("source") else "finnhub_2y"

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

    # Estimat / tillväxt
    eps_1y_est = _pos(_nz(yest.get("eps_1y"), row.get("EPS 1Y")))
    eps_2y_est = _pos(_nz(yest.get("eps_2y"), row.get("EPS 2Y")))
    eps_cagr   = _f(row.get("EPS CAGR"))
    rev_cagr   = _f(row.get("Rev CAGR"))

    # 3) Härled EPS om saknas men PE+price finns
    eps_ttm, src_eps_ttm, eps_1y_est, src_eps_1y = _derive_eps_from_pe_if_missing(
        price, pe_ttm, pe_fwd, eps_ttm, eps_1y_est
    )

    # 4) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 5) Paths
    e0, e1, e2, e3 = _eps_path(eps_ttm, eps_1y_est, eps_2y_est, eps_cagr)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(ebitda_ttm, r0, r1, r2, r3)

    # Multiplar med decay
    pe0 = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)

    pb0, pb1, pb2, pb3 = p_b, _decay_multiple(p_b, 1, decay), _decay_multiple(p_b, 2, decay), _decay_multiple(p_b, 3, decay)

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
    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if e0 else '—'}({src.get('eps_ttm','?') or src_eps_ttm}), "
        f"eps_1y={'ok' if e1 else '—'}({yest.get('source') or src_eps_1y}), "
        f"eps_2y={'ok' if e2 else '—'}({yest.get('source')}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
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
            "eps_1y_source": yest.get("source") or src_eps_1y or "sheet/derived",
            "eps_2y_source": yest.get("source") or "sheet/derived",
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta


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
_PREFER_ORDER = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]

def _pick_primary_from_table(met_df: pd.DataFrame, preset: Optional[str] = None) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if met_df is None or met_df.empty:
        return None, None, None, None, None
    available = set(met_df["Metod"].astype(str))
    chosen = None
    # 1) Om användaren/row redan valt primär metod & den finns: använd den
    if preset and preset in available:
        chosen = preset
    # 2) Annars: välj metoden med flest icke-NaN punkter, tie-break via _PREFER_ORDER
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
    mask = res["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if mask.any():
        idx = res.index[mask][-1]
        for k, v in row.items():
            res.at[idx, k] = v
    else:
        res = pd.concat([res, pd.DataFrame([row])[cols]], ignore_index=True)
    _write_df(RESULT_TITLE, res[cols])

# ---------- Bolagskort (presentation + källor + metodval) ----------
def _company_card(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
    tkr = str(row.get("Ticker","")).upper().strip()
    name = str(_nz(row.get("Bolagsnamn"), tkr))
    bucket = str(_nz(row.get("Bucket"), ""))
    preset_primary = str(_nz(row.get("Primär metod"), "")).strip() or None

    st.markdown(f"### {tkr} • {name}" + (f" • {bucket}" if bucket else ""))

    # Kör beräkningsmotorn
    met_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    currency = meta.get("currency") or str(_nz(row.get("Valuta"), "USD")).upper()
    price_now = meta.get("price")

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
            st.write(f"• **Valuta:** {currency}")
            st.write(f"• **Aktuell kurs:** {_fmt_money(price_now, currency)}")
        with right:
            st.markdown("**Källor (hämtade/deriverade)**")
            if sources:
                src_rows = sorted([(k, sources[k]) for k in sources.keys()])
                st.dataframe(pd.DataFrame(src_rows, columns=["Fält","Källa"]), use_container_width=True)
            else:
                st.caption("Inga käll-taggar tillgängliga.")
        st.markdown("**Beräkningsvägar**")
        st.json(paths)

    # Utdelningsprognos (om fält finns i Data)
    try:
        shares_owned = _f(row.get("Antal aktier")) or 0.0
        dps_now = _f(row.get("Årlig utdelning"))
        dps_cagr = _f(row.get("Utdelning CAGR"))
        divs = forecast_dividends_net_sek(currency, shares_owned, dps_now, dps_cagr, fx_map, settings)
        with st.expander("💰 Utdelning (netto SEK, prognos 1–3 år)", expanded=False):
            st.write(f"• 1 år: {_fmt_sek(divs['y1'])}  • 2 år: {_fmt_sek(divs['y2'])}  • 3 år: {_fmt_sek(divs['y3'])}")
    except Exception:
        pass

    # Åtgärdsknappar
    b1, b2, b3 = st.columns(3)
    if b1.button("💾 Spara primär metod", key=f"saveprim_{tkr}"):
        _save_primary_method_to_data(tkr, method_sel)
        st.success(f"Primär metod '{method_sel}' sparad för {tkr}.")

    if b2.button("🧮 Spara riktkurser → Resultat", key=f"saveres_{tkr}"):
        _save_targets_to_result(tkr, currency, method_sel, t0, t1, t2, t3)
        st.success("Riktkurser sparade till fliken Resultat.")

    if b3.button("♻️ Uppdatera estimat/CAGR i Data", key=f"upd_est_{tkr}"):
        # Enkel uppdatering: räkna om EPS CAGR om ttm + 1y finns
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
    f1, f2, f3 = st.columns(3)
    buckets = f1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_only = f2.checkbox("Visa endast innehav (antal > 0)", value=False)
    hide_zero_price = f3.checkbox("Dölj bolag utan aktuell kurs", value=True)

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

    # Beräkna fair value (Idag) för varje rad utifrån aktuell primär metod (preset) för sortering
    progress = st.progress(0.0)
    scored: List[Tuple[str, float, Dict[str, Any], pd.Series]] = []
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            preset = str(_nz(r.get("Primär metod"), "")).strip() or None
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

    # Sortera: störst uppsida först
    scored.sort(key=lambda x: (x[1] is None, -x[1] if x[1] is not None else -9e9))
    ordered_rows = [t[3] for t in scored]

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
# Editor (ticker in -> auto-fyll + spara), Portfölj, Router, main()
# ============================================================

# ------------ Små helpers för UI-state ------------
def _set_state_if_absent(k, v):
    if k not in st.session_state:
        st.session_state[k] = v

def _apply_prefill_to_state(prefill: Dict[str, Any]):
    # Mappar prefills -> widget keys
    mapping = {
        "Valuta": "edit_currency",
        "Aktuell kurs": "edit_price",
        "Utestående aktier": "edit_shares",
        "Net debt": "edit_net_debt",
        "Rev TTM": "edit_rev_ttm",
        "EBITDA TTM": "edit_ebitda_ttm",
        "EPS TTM": "edit_eps_ttm",
        "PE TTM": "edit_pe_ttm",
        "PE FWD": "edit_pe_fwd",
        "EV/Revenue": "edit_evs",
        "EV/EBITDA": "edit_eve",
        "P/B": "edit_pb",
        "BVPS": "edit_bvps",
        "EPS 1Y": "edit_eps_1y",
        "EPS 2Y": "edit_eps_2y",
    }
    for col, key in mapping.items():
        if col in prefill and prefill[col] is not None:
            st.session_state[key] = prefill[col]


# ------------ Editor-sida ------------
def page_editor():
    st.header("✏️ Editor — lägg till/uppdatera bolag")

    df = read_data_df()
    settings = get_settings_map()

    st.markdown("Ange **Ticker** och klicka **Hämta & fyll** för att auto-populera fält från Yahoo (med fallback). Justera sedan manuellt vid behov och **Spara**.")

    # Tickerfält (utanför form så vi kan trigga prefill)
    t1, t2 = st.columns([2,1])
    ticker = t1.text_input("Ticker", value=st.session_state.get("edit_ticker", ""), placeholder="t.ex. TTD, NVDA, 2020.OL", key="edit_ticker")
    do_fetch = t2.button("Hämta & fyll")

    # Om "Hämta & fyll" – läs live och skriv till session_state, sedan rerun
    if do_fetch and ticker.strip():
        snap = fetch_yahoo_snapshot(ticker.strip().upper())
        yest = fetch_yahoo_eps_estimates(ticker.strip().upper())
        # Blanda in EPS1Y/EPS2Y från est-källor
        prefill = {
            "Valuta": snap.get("currency"),
            "Aktuell kurs": snap.get("price"),
            "Utestående aktier": snap.get("shares"),
            "Net debt": snap.get("net_debt"),
            "Rev TTM": snap.get("revenue_ttm"),
            "EBITDA TTM": snap.get("ebitda_ttm"),
            "EPS TTM": snap.get("eps_ttm"),
            "PE TTM": snap.get("pe_ttm"),
            "PE FWD": snap.get("pe_fwd"),
            "EV/Revenue": snap.get("ev_to_sales"),
            "EV/EBITDA": snap.get("ev_to_ebitda"),
            "P/B": snap.get("p_to_book"),
            "BVPS": snap.get("bvps"),
            "EPS 1Y": yest.get("eps_1y"),
            "EPS 2Y": yest.get("eps_2y"),
        }
        _apply_prefill_to_state(prefill)
        st.toast("Data hämtad. Fälten är förifyllda – kontrollera och spara.")
        st.experimental_rerun()

    # Hämta ev. befintlig rad för defaultvärden
    existing: Optional[pd.Series] = None
    if df is not None and not df.empty and ticker.strip():
        mask = df["Ticker"].astype(str).str.upper() == ticker.strip().upper()
        if mask.any():
            existing = df[mask].iloc[-1]

    # Initiera states från befintlig rad om de saknas
    _set_state_if_absent("edit_currency", str(_nz(existing.get("Valuta"), "USD")).upper() if existing is not None else "USD")
    _set_state_if_absent("edit_price", _f(existing.get("Aktuell kurs")) if existing is not None else 0.0)
    _set_state_if_absent("edit_shares", _f(existing.get("Utestående aktier")) if existing is not None else 0.0)
    _set_state_if_absent("edit_net_debt", _f(existing.get("Net debt")) if existing is not None else 0.0)
    _set_state_if_absent("edit_rev_ttm", _f(existing.get("Rev TTM")) if existing is not None else 0.0)
    _set_state_if_absent("edit_ebitda_ttm", _f(existing.get("EBITDA TTM")) if existing is not None else 0.0)
    _set_state_if_absent("edit_eps_ttm", _f(existing.get("EPS TTM")) if existing is not None else 0.0)
    _set_state_if_absent("edit_pe_ttm", _f(existing.get("PE TTM")) if existing is not None else 0.0)
    _set_state_if_absent("edit_pe_fwd", _f(existing.get("PE FWD")) if existing is not None else 0.0)
    _set_state_if_absent("edit_evs", _f(existing.get("EV/Revenue")) if existing is not None else 0.0)
    _set_state_if_absent("edit_eve", _f(existing.get("EV/EBITDA")) if existing is not None else 0.0)
    _set_state_if_absent("edit_pb", _f(existing.get("P/B")) if existing is not None else 0.0)
    _set_state_if_absent("edit_bvps", _f(existing.get("BVPS")) if existing is not None else 0.0)
    _set_state_if_absent("edit_eps_1y", _f(existing.get("EPS 1Y")) if existing is not None else 0.0)
    _set_state_if_absent("edit_eps_2y", _f(existing.get("EPS 2Y")) if existing is not None else 0.0)
    _set_state_if_absent("edit_eps_cagr", _f(existing.get("EPS CAGR")) if existing is not None else 0.0)
    _set_state_if_absent("edit_rev_cagr", _f(existing.get("Rev CAGR")) if existing is not None else 0.0)
    _set_state_if_absent("edit_bucket", str(_nz(existing.get("Bucket"), "A")) if existing is not None else "A")
    _set_state_if_absent("edit_primary", str(_nz(existing.get("Primär metod"), "ev_ebitda")) if existing is not None else "ev_ebitda")
    _set_state_if_absent("edit_name", str(_nz(existing.get("Bolagsnamn"), "")) if existing is not None else "")
    _set_state_if_absent("edit_qty", _f(existing.get("Antal aktier")) if existing is not None else 0.0)
    _set_state_if_absent("edit_divpa", _f(existing.get("Årlig utdelning")) if existing is not None else 0.0)
    _set_state_if_absent("edit_div_cagr", _f(existing.get("Utdelning CAGR")) if existing is not None else 0.0)

    # Själva formuläret (med submit-knapp!)
    with st.form("edit_form", clear_on_submit=False):
        c0, c1 = st.columns([2, 1])
        name = c0.text_input("Bolagsnamn (valfritt)", value=st.session_state["edit_name"], key="edit_name")
        bucket = c1.selectbox("Bucket", DEFAULT_BUCKETS, index=DEFAULT_BUCKETS.index(st.session_state["edit_bucket"]) if st.session_state["edit_bucket"] in DEFAULT_BUCKETS else 0, key="edit_bucket")

        ccur, cprice, cshares = st.columns([1,1,1])
        currency = ccur.text_input("Valuta", value=st.session_state["edit_currency"], key="edit_currency")
        price = cprice.number_input("Aktuell kurs", value=float(st.session_state["edit_price"]), step=0.01, format="%.4f", key="edit_price")
        shares_out = cshares.number_input("Utestående aktier", value=float(st.session_state["edit_shares"]), step=1e3, format="%.0f", key="edit_shares")

        # OBS: tillåt negativa (ingen min_value angiven) -> undviker StreamlitValueBelowMinError
        cnd, crev, ceb = st.columns([1,1,1])
        net_debt = cnd.number_input("Net debt (kan vara negativ)", value=float(st.session_state["edit_net_debt"]), step=1e5, format="%.0f", key="edit_net_debt")
        rev_ttm = crev.number_input("Rev TTM", value=float(st.session_state["edit_rev_ttm"]), step=1e6, format="%.0f", key="edit_rev_ttm")
        ebitda_ttm = ceb.number_input("EBITDA TTM", value=float(st.session_state["edit_ebitda_ttm"]), step=1e6, format="%.0f", key="edit_ebitda_ttm")

        ceps, cpe, cpf = st.columns([1,1,1])
        eps_ttm = ceps.number_input("EPS TTM", value=float(st.session_state["edit_eps_ttm"]), step=0.01, format="%.4f", key="edit_eps_ttm")
        pe_ttm = cpe.number_input("PE TTM", value=float(st.session_state["edit_pe_ttm"]), step=0.1, format="%.2f", key="edit_pe_ttm")
        pe_fwd = cpf.number_input("PE FWD", value=float(st.session_state["edit_pe_fwd"]), step=0.1, format="%.2f", key="edit_pe_fwd")

        cev, ceve, cpb = st.columns([1,1,1])
        evs = cev.number_input("EV/Revenue", value=float(st.session_state["edit_evs"]), step=0.1, format="%.2f", key="edit_evs")
        eve = ceve.number_input("EV/EBITDA", value=float(st.session_state["edit_eve"]), step=0.1, format="%.2f", key="edit_eve")
        pb = cpb.number_input("P/B", value=float(st.session_state["edit_pb"]), step=0.1, format="%.2f", key="edit_pb")

        cbv, ce1, ce2 = st.columns([1,1,1])
        bvps = cbv.number_input("BVPS", value=float(st.session_state["edit_bvps"]), step=0.01, format="%.4f", key="edit_bvps")
        eps_1y = ce1.number_input("EPS 1Y (est.)", value=float(st.session_state["edit_eps_1y"]), step=0.01, format="%.4f", key="edit_eps_1y")
        eps_2y = ce2.number_input("EPS 2Y (est.)", value=float(st.session_state["edit_eps_2y"]), step=0.01, format="%.4f", key="edit_eps_2y")

        cc1, cc2 = st.columns([1,1])
        eps_cagr = cc1.number_input("EPS CAGR (frivillig, %/år)", value=float(st.session_state["edit_eps_cagr"]), step=0.1, format="%.2f", key="edit_eps_cagr")
        rev_cagr = cc2.number_input("Revenue CAGR (frivillig, %/år)", value=float(st.session_state["edit_rev_cagr"]), step=0.1, format="%.2f", key="edit_rev_cagr")

        cqty, cdps, cdpsg = st.columns([1,1,1])
        qty = cqty.number_input("Antal aktier (ägda)", value=float(st.session_state["edit_qty"]), step=1.0, format="%.0f", key="edit_qty")
        dps = cdps.number_input("Årlig utdelning per aktie", value=float(st.session_state["edit_divpa"]), step=0.01, format="%.4f", key="edit_divpa")
        dps_cagr = cdpsg.number_input("Utdelning CAGR (%, frivillig)", value=float(st.session_state["edit_div_cagr"]), step=0.1, format="%.2f", key="edit_div_cagr")

        primary = st.selectbox("Primär metod (för ranking/uppsida)", _PREFER_ORDER, index=_PREFER_ORDER.index(st.session_state["edit_primary"]) if st.session_state["edit_primary"] in _PREFER_ORDER else 0, key="edit_primary")

        submitted = st.form_submit_button("💾 Spara till Data")
        if submitted:
            if not ticker.strip():
                st.error("Ange en Ticker först.")
            else:
                # Se till att kolumnerna finns
                needed_cols = [
                    "Ticker","Bolagsnamn","Bucket","Valuta","Aktuell kurs","Utestående aktier","Net debt",
                    "Rev TTM","EBITDA TTM","EPS TTM","PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
                    "EPS 1Y","EPS 2Y","EPS CAGR","Rev CAGR","Antal aktier","Årlig utdelning","Utdelning CAGR","Primär metod",
                    "Senast manuellt uppdaterad"
                ]
                if df.empty:
                    df = pd.DataFrame(columns=needed_cols)
                for c in needed_cols:
                    if c not in df.columns:
                        df[c] = np.nan

                row = {
                    "Ticker": ticker.strip().upper(),
                    "Bolagsnamn": name,
                    "Bucket": bucket,
                    "Valuta": currency.strip().upper(),
                    "Aktuell kurs": price,
                    "Utestående aktier": shares_out,
                    "Net debt": net_debt,
                    "Rev TTM": rev_ttm,
                    "EBITDA TTM": ebitda_ttm,
                    "EPS TTM": eps_ttm,
                    "PE TTM": pe_ttm,
                    "PE FWD": pe_fwd,
                    "EV/Revenue": evs,
                    "EV/EBITDA": eve,
                    "P/B": pb,
                    "BVPS": bvps,
                    "EPS 1Y": eps_1y,
                    "EPS 2Y": eps_2y,
                    "EPS CAGR": eps_cagr,
                    "Rev CAGR": rev_cagr,
                    "Antal aktier": qty,
                    "Årlig utdelning": dps,
                    "Utdelning CAGR": dps_cagr,
                    "Primär metod": primary,
                    "Senast manuellt uppdaterad": now_stamp(),
                }

                # Skriv in/uppdatera
                mask = df["Ticker"].astype(str).str.upper() == row["Ticker"]
                if mask.any():
                    idx = df.index[mask][-1]
                    for k, v in row.items():
                        df.at[idx, k] = v
                else:
                    df = pd.concat([df, pd.DataFrame([row])[df.columns]], ignore_index=True)

                write_data_df(df)
                st.success(f"{row['Ticker']} sparad/uppdaterad i Data.")

    st.divider()
    with st.expander("Visa Data (förhandsgranskning)"):
        st.dataframe(read_data_df(), use_container_width=True)


# ------------ Portfölj-sida ------------
def page_portfolio():
    st.header("📦 Portfölj")

    fx_map = get_fx_map()
    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    # Visa bara innehav (>0)
    q = df.copy()
    q["Antal aktier"] = pd.to_numeric(q["Antal aktier"], errors="coerce")
    q = q[q["Antal aktier"] > 0]

    if q.empty:
        st.info("Inga rader med 'Antal aktier' > 0.")
        return

    # Beräkna portföljvärde i SEK
    def _sek(row):
        ccy = str(_nz(row.get("Valuta"), "USD")).upper()
        fx = fx_map.get(ccy, 1.0)
        return _f(row.get("Aktuell kurs")) * _f(row.get("Antal aktier")) * fx

    q["Värde (SEK)"] = q.apply(_sek, axis=1)
    total_value = q["Värde (SEK)"].sum()

    st.metric("Totalt portföljvärde (SEK)", _fmt_sek(total_value))

    # Visa tabell
    view_cols = ["Ticker","Bolagsnamn","Valuta","Antal aktier","Aktuell kurs","Värde (SEK)","Årlig utdelning"]
    for c in view_cols:
        if c not in q.columns:
            q[c] = np.nan
    st.dataframe(q[view_cols].sort_values("Värde (SEK)", ascending=False), use_container_width=True)

    # Enkel utdelningssumma i SEK (netto med default källskatt via settings)
    settings = get_settings_map()
    tot_net = 0.0
    for _, r in q.iterrows():
        ccy = str(_nz(r.get("Valuta"), "USD")).upper()
        divs = forecast_dividends_net_sek(
            currency=ccy,
            shares_owned=_f(r.get("Antal aktier")),
            dps_now=_f(r.get("Årlig utdelning")),
            dps_cagr=_f(r.get("Utdelning CAGR")),
            fx_map=fx_map,
            settings=settings
        )
        tot_net += _f(divs.get("y1"))
    st.caption(f"Prognos utdelning (netto, 12 mån): {_fmt_sek(tot_net)}")


# ------------ Router + main() ------------
def run_main_ui():
    st.sidebar.title("📚 Navigering")
    page = st.sidebar.radio("Välj sida", ["Editor", "Analys", "Portfölj"], index=1)

    # Snabbknappar
    with st.sidebar.expander("⚙️ Verktyg"):
        if st.button("Uppdatera valutakurser (om tillgängligt)"):
            # Dummy – i denna version antar vi att FX redan finns i bladet
            st.toast("Valutakurser uppdateras externt i denna basversion.")
        st.caption("Valutakurser läses från fliken **Valutakurser**.")

    if page == "Editor":
        page_editor()
    elif page == "Analys":
        page_analysis()
    else:
        page_portfolio()


def main():
    st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")
    st.title("Aktieanalys & investeringsförslag — Basversion")
    run_main_ui()


if __name__ == "__main__":
    main()
