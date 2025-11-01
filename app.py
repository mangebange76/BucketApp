# app.py — Del 1/4
# ============================================================
# Bas: Streamlit-app för fair value / riktkurser / portfölj
# Lagring: Google Sheets (Data, Resultat, Valutakurser, Settings, Snapshot)
# Hämtning: Yahoo (yfinance) + (valfritt) Finnhub
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

def _with_backoff(callable_fn, *args, **kwargs):
    """Exponential backoff för Google Sheets-API (429/5xx)."""
    max_tries = kwargs.pop("_max_tries", 6)
    base = kwargs.pop("_base_sleep", 0.7)
    for i in range(max_tries):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            code = getattr(e, "response", None)
            code = getattr(code, "status_code", None) or getattr(e, "status", None)
            if code in (429, 500, 502, 503, 504):
                sleep_s = base * (2 ** i) + random.random() * 0.2
                time.sleep(min(sleep_s, 8.0))
                continue
            raise
        except Exception:
            # små transienta fel – försök igen kort
            time.sleep(0.4 + 0.1 * i)
    # sista försöket utan fångst
    return callable_fn(*args, **kwargs)

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
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppnar spreadsheet via SHEET_URL eller SHEET_ID (trimmar whitespace)."""
    sheet_url = _env_or_secret("SHEET_URL")
    sheet_id  = _env_or_secret("SHEET_ID")
    if sheet_url:
        return _with_backoff(_gc.open_by_url, sheet_url.strip())
    if sheet_id:
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
    # skriv atomärt: clear + update (med backoff)
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
    "EPS 1Y","Rev CAGR","EPS CAGR",
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
        for c in DATA_COLUMNS:
            if c not in df.columns:
                df[c] = np.nan
        _write_df(DATA_TITLE, df[[c for c in DATA_COLUMNS]])

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
        for c in SETTINGS_COLUMNS:
            if c not in s.columns: s[c] = np.nan
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
        for c in FX_COLUMNS:
            if c not in fx.columns: fx[c] = np.nan
        _write_df(FX_TITLE, fx[FX_COLUMNS])

    # Snapshot
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        _write_df(SNAPSHOT_TITLE, pd.DataFrame(columns=[
            "Timestamp","Ticker","Valuta","Metod","Idag","1 år","2 år","3 år","Ankare PE","Decay",
            "Källa:EPS_TTM","Källa:EPS_1Y","Källa:Rev_TTM","Källa:EBITDA_TTM","Källa:PE_anchor"
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
            px = getattr(t.fast_info, "last_price", None)
            if not px:
                hist = t.history(period="1d")
                if not hist.empty: px = float(hist["Close"].iloc[-1])
            if px: out[code] = float(px)
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
    # defaults
    out.setdefault("pe_anchor_weight_ttm","0.50")
    out.setdefault("multiple_decay","0.10")
    out.setdefault("withholding_USD","0.15")
    out.setdefault("withholding_EUR","0.15")
    out.setdefault("withholding_NOK","0.25")
    out.setdefault("withholding_CAD","0.15")
    out.setdefault("withholding_SEK","0.00")
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
        "EV/Revenue","EV/EBITDA","P/B","BVPS","EPS 1Y",
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

# app.py — Del 2/4
# ============================================================
# Datainsamling (Yahoo, Finnhub) + beräkningsmotor & utdelning
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

def _mark(src_map: Dict[str,str], key: str, src: str):
    """Skriv källmarkering för ett fält."""
    if key and src:
        src_map[key] = src

# -------------------------
# Yahoo (yfinance) – robust snapshot + källor
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar pris, valuta och centrala nyckeltal från yfinance.
    Returnerar dict med nycklar:
      price, currency, market_cap, ev, shares,
      eps_ttm, pe_ttm, pe_fwd,
      eps_fwd (om tillgängligt),  revenue_ttm, ebitda_ttm,
      ev_to_sales, ev_to_ebitda, p_to_book, bvps,
      revenue_growth (YoY), ebitda_margins,
      net_debt,
      __sources (källor för fält)
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {}
    src: Dict[str, str]  = {}

    # Snabbkanal
    try:
        fi = tk.fast_info
        val = _safe_float(getattr(fi, "last_price", None))
        if val is not None:
            out["price"] = val; _mark(src, "price", "yahoo.fast_info")
        cur = getattr(fi, "currency", None)
        if cur:
            out["currency"] = cur; _mark(src, "currency", "yahoo.fast_info")
        mc = _safe_float(getattr(fi, "market_cap", None))
        if mc is not None:
            out["market_cap"] = mc; _mark(src, "market_cap", "yahoo.fast_info")
        sh = _safe_float(getattr(fi, "shares", None))
        if sh is not None:
            out["shares"] = sh; _mark(src, "shares", "yahoo.fast_info")
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

    def set_if_none(name: str, val, source: str):
        if out.get(name) is None and val is not None:
            out[name] = val; _mark(src, name, source)

    set_if_none("price",       _safe_float(gi("currentPrice")),       "yahoo.info")
    set_if_none("currency",    gi("currency"),                         "yahoo.info")
    set_if_none("market_cap",  _safe_float(gi("marketCap")),           "yahoo.info")

    set_if_none("eps_ttm",     _safe_float(gi("trailingEps")),         "yahoo.info")
    set_if_none("pe_ttm",      _safe_float(gi("trailingPE")),          "yahoo.info")
    set_if_none("pe_fwd",      _safe_float(gi("forwardPE")),           "yahoo.info")
    set_if_none("eps_fwd",     _safe_float(gi("forwardEps")),          "yahoo.info")

    set_if_none("revenue_ttm", _safe_float(gi("totalRevenue")),        "yahoo.info")
    set_if_none("ebitda_ttm",  _safe_float(gi("ebitda")),              "yahoo.info")
    set_if_none("ev_to_sales", _safe_float(gi("enterpriseToRevenue")), "yahoo.info")
    set_if_none("ev_to_ebitda",_safe_float(gi("enterpriseToEbitda")),  "yahoo.info")
    set_if_none("p_to_book",   _safe_float(gi("priceToBook")),         "yahoo.info")
    set_if_none("bvps",        _safe_float(gi("bookValue")),           "yahoo.info")

    set_if_none("revenue_growth", _safe_float(gi("revenueGrowth")),    "yahoo.info")  # ~YoY (decimaltal)
    set_if_none("ebitda_margins", _safe_float(gi("ebitdaMargins")),    "yahoo.info")  # andel

    ev_info   = _safe_float(gi("enterpriseValue"))
    total_debt= _safe_float(gi("totalDebt"))
    total_cash= _safe_float(gi("totalCash"))

    if ev_info is not None:
        set_if_none("ev", ev_info, "yahoo.info")
    elif out.get("market_cap") is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
        _mark(src, "ev", "derived: mcap + debt - cash")

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]
        _mark(src, "net_debt", "derived: ev - mcap")

    # Shares fallback via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            _mark(src, "shares", "derived: mcap/price")
        except Exception:
            pass

    # Normalisera valuta
    out["currency"] = str(out.get("currency") or "USD").upper()

    out["__sources"] = src
    return out

# -------------------------
# Finnhub (valfritt) – EPS-estimat
# -------------------------
def _get_finnhub_key() -> Optional[str]:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

@st.cache_data(ttl=600, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker hämta EPS-estimat 1–2 år framåt från Finnhub (om nyckel finns).
    Returnerar {"eps_1y": float|None, "eps_2y": float|None, "__source": "..."}
    """
    key = _get_finnhub_key()
    if not key:
        return {"eps_1y": None, "eps_2y": None, "__source": "none"}

    # Primärt försök: /stock/estimate
    try:
        url = f"https://finnhub.io/api/v1/stock/estimate?symbol={ticker}&token={key}"
        r = requests.get(url, timeout=10)
        eps_1y, eps_2y = None, None
        if r.ok:
            js = r.json()
            rows = js if isinstance(js, list) else js.get("data", [])
            rows = rows or []
            rows = sorted(rows, key=lambda x: str(x.get("period","")))
            vals = [_safe_float(x.get("epsAvg")) for x in rows if _safe_float(x.get("epsAvg")) is not None]
            if vals:
                eps_1y = vals[-1]
                eps_2y = vals[-2] if len(vals) > 1 else None
        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "__source": "finnhub.estimate"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "__source": "finnhub.error"}

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
# EPS/REV/EBITDA paths (inkl. fallback + källor)
# -------------------------
def _derive_eps_from_pe_if_missing(price: Optional[float], pe_ttm: Optional[float], pe_fwd: Optional[float],
                                   eps_ttm: Optional[float], eps_1y: Optional[float],
                                   src_out: Dict[str,str]) -> Tuple[Optional[float], Optional[float]]:
    """
    Om EPS saknas men vi har price+PE, härled EPS. Uppdaterar src_out.
    Returnerar (eps_ttm, eps_1y)
    """
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        _mark(src_out, "eps_ttm", "derived_from_pe_ttm")
    if eps_1y is None and _pos(price) and _pos(pe_fwd):
        eps_1y = price / pe_fwd
        _mark(src_out, "eps_1y", "derived_from_pe_fwd")
    return eps_ttm, eps_1y

def _eps_path(eps_ttm: Optional[float], eps_1y: Optional[float], eps_2y: Optional[float],
              eps_cagr: Optional[float], src_out: Dict[str,str]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returnerar (eps_0, eps_1, eps_2, eps_3).
    Prioritet: estimat (1–2y). Saknas → extrapolera med CAGR från senaste kända punkt.
    eps_cagr förväntas som decimaltal (0.18 = 18%).
    """
    e0 = _pos(eps_ttm)
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    cg = _f(eps_cagr)

    # Om vi har e0 och e1 men saknar CAGR – härled enkel tillväxt
    if cg is None and e0 is not None and e1 is not None and e0 > 0:
        try:
            cg = (e1 / e0) - 1.0
            _mark(src_out, "eps_cagr", "derived_simple_from_eps_ttm_to_1y")
        except Exception:
            pass

    # Fyll luckor med CAGR
    if e1 is None and e0 is not None and cg is not None:
        e1 = e0 * (1.0 + cg); _mark(src_out, "eps_1y", "derived_from_cagr")
    if e2 is None and e1 is not None and cg is not None:
        e2 = e1 * (1.0 + cg); _mark(src_out, "eps_2y", "derived_from_cagr")
    e3 = e2 * (1.0 + cg) if (e2 is not None and cg is not None) else None
    if e3 is not None: _mark(src_out, "eps_3y", "derived_from_cagr")

    return e0, e1, e2, e3

def _rev_path(rev_ttm: Optional[float], rev_cagr: Optional[float], revenue_growth_hint: Optional[float],
              src_out: Dict[str,str]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returnerar (rev_0..rev_3). rev_cagr i decimaltal. Om rev_cagr saknas, använd revenue_growth_hint (YoY från Yahoo) som fallback.
    """
    r0 = _pos(rev_ttm)
    cg = _f(rev_cagr)
    if cg is None:
        cg = _f(revenue_growth_hint)
        if cg is not None:
            _mark(src_out, "rev_cagr", "yahoo.revenueGrowth_hint")

    if r0 is None or cg is None:
        return r0, None, None, None
    r1 = r0 * (1.0 + cg)
    r2 = r1 * (1.0 + cg)
    r3 = r2 * (1.0 + cg)
    _mark(src_out, "rev_path", "derived_from_cagr")
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float],
                 src_out: Dict[str,str]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Proxy: EBITDA växer ungefär i takt med omsättning (om vi saknar riktiga prognoser).
    Om rev-path saknas -> håll ebitda konstant.
    """
    e0 = _pos(ebitda_ttm)
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        _mark(src_out, "ebitda_path", "flat_due_to_missing_rev_path")
        return e0, e0, e0, e0
    def scale(r): return (e0 * (r / rev0)) if (r and rev0) else e0
    _mark(src_out, "ebitda_path", "scaled_with_revenue_path")
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
# Huvudmotor per rad (inkl. källkartläggning)
# -------------------------
def compute_methods_for_row(
    row: pd.Series,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
    primary_override: Optional[str] = None,
) -> Tuple[pd.DataFrame, str, Dict[str, Any], Dict[str,str]]:
    """
    Beräknar metodtabell (Idag, 1,2,3 år) för raden.
    Returnerar (methods_df, sanity_text, meta, sources_map)
    """
    ticker = str(row.get("Ticker", "")).strip()
    # 1) Live-data
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.35)  # mild throttling
    est  = fetch_finnhub_estimates(ticker)

    src_map: Dict[str,str] = {}
    src_map.update(snap.get("__sources", {}))

    # 2) Inputs (med fallback från Data-bladet)
    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs")));           _mark(src_map, "price",    src_map.get("price","Data") if price else "")
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper()
    mcap     = _pos(snap.get("market_cap"))
    ev_now   = _pos(_nz(snap.get("ev"), None))
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier")))
    net_debt = _nz(snap.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _pos(_nz(snap.get("revenue_ttm"), row.get("Rev TTM")));        _mark(src_map, "revenue_ttm", "yahoo.info" if snap.get("revenue_ttm") else ("Data" if row.get("Rev TTM") else ""))
    ebitda_ttm = _pos(_nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM")));      _mark(src_map, "ebitda_ttm",  "yahoo.info" if snap.get("ebitda_ttm") else ("Data" if row.get("EBITDA TTM") else ""))
    eps_ttm    = _pos(_nz(snap.get("eps_ttm"), row.get("EPS TTM")));            _mark(src_map, "eps_ttm",     "yahoo.info" if snap.get("eps_ttm") else ("Data" if row.get("EPS TTM") else ""))
    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B")))
    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS")))
    rev_hint   = _pos(snap.get("revenue_growth"))

    # Estimat / tillväxt
    eps_1y_est = _pos(_nz(est.get("eps_1y"), _nz(snap.get("eps_fwd"), row.get("EPS 1Y"))))
    if est.get("__source","").startswith("finnhub") and eps_1y_est is not None:
        _mark(src_map, "eps_1y", "finnhub")
    elif snap.get("eps_fwd") is not None and eps_1y_est is not None:
        _mark(src_map, "eps_1y", "yahoo.forwardEps")
    elif row.get("EPS 1Y") is not None and _pos(row.get("EPS 1Y")):
        _mark(src_map, "eps_1y", "Data")

    eps_2y_est = _pos(est.get("eps_2y"))
    if eps_2y_est is not None:
        _mark(src_map, "eps_2y", "finnhub")

    eps_cagr   = _f(row.get("EPS CAGR"))
    if eps_cagr is not None:
        _mark(src_map, "eps_cagr", "Data")

    rev_cagr   = _f(row.get("Rev CAGR"))
    if rev_cagr is not None:
        _mark(src_map, "rev_cagr", "Data")

    # 3) Härled EPS om saknas men PE+price finns
    eps_ttm, eps_1y_est = _derive_eps_from_pe_if_missing(
        price, pe_ttm, pe_fwd, eps_ttm, eps_1y_est, src_map
    )

    # 4) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)
    if pe_anchor is not None:
        _mark(src_map, "pe_anchor", f"weighted(TTM={w_ttm:.2f},FWD={1.0-w_ttm:.2f})")

    # 5) Paths
    e0, e1, e2, e3 = _eps_path(eps_ttm, eps_1y_est, eps_2y_est, eps_cagr, src_map)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr, rev_hint, src_map)
    b0, b1, b2, b3 = _ebitda_path(ebitda_ttm, r0, r1, r2, r3, src_map)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,      _decay_multiple(p_b,      1, decay), _decay_multiple(p_b,      2, decay), _decay_multiple(p_b,      3, decay)

    # 6) Priser per metod
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

    # 7) Sanity
    sanity = (
        f"price={'ok' if price else '—'}, eps_ttm={'ok' if e0 else '—'}, "
        f"eps_1y={'ok' if e1 else '—'}, eps_2y={'ok' if e2 else '—'}, "
        f"rev_ttm={'ok' if r0 else '—'}, ebitda_ttm={'ok' if b0 else '—'}, shares={'ok' if shares else '—'}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "preferred": primary_override
    }
    return methods_df, sanity, meta, src_map

# app.py — Del 3/4
# ============================================================
# UI per bolag: presentation + metodval + spar-funktioner
# ============================================================

# ---------- Formatering ----------
def _fmt_money(v: Optional[float], ccy: str) -> str:
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return f"– {ccy}"
    try:
        return f"{float(v):,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} {ccy}"

def _fmt_pct(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "–"
    try:
        return f"{float(v)*100:,.1f}%".replace(",", " ")
    except Exception:
        return f"{v}%"

def _fmt_sek(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "0 SEK"
    try:
        return f"{float(v):,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

# ---------- Spara/uppdatera hjälp ----------
def _save_primary_method_to_data(ticker: str, method: str) -> None:
    df = read_data_df()
    if df.empty:
        st.warning("Data-bladet är tomt.")
        return
    if "Primär metod" not in df.columns:
        df["Primär metod"] = np.nan
    mask = df["Ticker"].astype(str) == str(ticker)
    if not mask.any():
        st.warning(f"Hittade inte {ticker} i Data-bladet.")
        return
    df.loc[mask, "Primär metod"] = method
    write_data_df(df)

def _append_or_update_result_row(ticker: str, currency: str, method: str,
                                 today: Optional[float], y1: Optional[float],
                                 y2: Optional[float], y3: Optional[float]) -> None:
    res = _read_df(RESULT_TITLE)
    now = now_stamp()
    new_row = {
        "Timestamp": now, "Ticker": ticker, "Valuta": currency, "Metod": method,
        "Riktkurs idag": _f(today), "Riktkurs 1 år": _f(y1),
        "Riktkurs 2 år": _f(y2), "Riktkurs 3 år": _f(y3),
    }
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame([new_row]))
        return
    # upsert per Ticker
    if "Ticker" in res.columns and (res["Ticker"].astype(str) == ticker).any():
        idx = res.index[res["Ticker"].astype(str) == ticker][-1]
        for k, v in new_row.items():
            if k in res.columns:
                res.at[idx, k] = v
            else:
                res[k] = np.nan
                res.at[idx, k] = v
        _write_df(RESULT_TITLE, res)
    else:
        cols = list(res.columns)
        for k in new_row.keys():
            if k not in cols:
                cols.append(k)
        res = pd.concat([res, pd.DataFrame([new_row])[cols]], ignore_index=True)
        _write_df(RESULT_TITLE, res[cols])

def _update_estimates_in_data(ticker: str, eps1y: Optional[float], eps2y: Optional[float],
                              eps_cagr: Optional[float], rev_cagr: Optional[float]) -> None:
    df = read_data_df()
    if df.empty:
        st.warning("Data-bladet är tomt.")
        return
    for c in ["EPS 1Y", "EPS CAGR", "Rev CAGR"]:
        if c not in df.columns:
            df[c] = np.nan
    mask = df["Ticker"].astype(str) == str(ticker)
    if not mask.any():
        st.warning(f"Hittade inte {ticker} i Data-bladet.")
        return
    if eps1y is not None:  df.loc[mask, "EPS 1Y"]   = float(eps1y)
    if eps_cagr is not None: df.loc[mask, "EPS CAGR"] = float(eps_cagr)
    if rev_cagr is not None: df.loc[mask, "Rev CAGR"] = float(rev_cagr)
    write_data_df(df)

# ---------- Primärmetod-heuristik ----------
PREFER_ORDER = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]

def _pick_primary_from_table(met_df: pd.DataFrame) -> Optional[str]:
    if met_df is None or met_df.empty:
        return None
    counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None
    maxc = counts.max()
    candidates = list(counts[counts == maxc].index)
    for m in PREFER_ORDER:
        if m in candidates:
            return m
    return candidates[0] if candidates else None

# ---------- Källtabel ----------
def _render_sources_card(src_map: Dict[str,str], meta: Dict[str, Any]) -> None:
    with st.expander("🔎 Datakällor & härledningar", expanded=False):
        # Visa nyckelvärden + källa
        rows = []
        show_keys = [
            ("price", "Aktuell kurs"),
            ("currency", "Valuta"),
            ("shares_out", "Utestående aktier"),
            ("net_debt", "Net debt"),
            ("pe_anchor", "PE-ankare (TTM/FWD mix)"),
            ("revenue_ttm", "Omsättning TTM"),
            ("ebitda_ttm", "EBITDA TTM"),
            ("eps_ttm", "EPS TTM"),
            ("eps_1y", "EPS 1 år"),
            ("eps_2y", "EPS 2 år"),
            ("rev_cagr", "Rev CAGR"),
            ("eps_cagr", "EPS CAGR"),
        ]
        for key, nice in show_keys:
            src = src_map.get(key, "")
            val = meta.get(key) if key in meta else None
            rows.append({"Fält": nice, "Källa": src if src else "—", "Värde": val})
        df_src = pd.DataFrame(rows)
        st.dataframe(df_src, use_container_width=True)

# ---------- Bolagspresentation ----------
def render_company_view(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float], method_override: Optional[str] = None) -> None:
    tkr = str(row.get("Ticker", "")).strip().upper()
    name = str(_nz(row.get("Bolagsnamn"), tkr))
    bucket = str(_nz(row.get("Bucket"), "")).strip() or "—"

    st.markdown(f"### {tkr} • {name} • {bucket}")

    # Kör motor
    met_df, sanity, meta, src_map = compute_methods_for_row(row, settings, fx_map, primary_override=method_override)

    currency = meta.get("currency") or str(_nz(row.get("Valuta"), "USD")).upper()
    price_now = meta.get("price")
    fx = fx_map.get(currency, 1.0) or 1.0
    shares_owned = _f(row.get("Antal aktier")) or 0.0

    # Metodval (primär)
    saved_primary = str(_nz(row.get("Primär metod"), ""))
    heuristic = _pick_primary_from_table(met_df) or "pe_hist_vs_eps"
    default_method = method_override or (saved_primary if saved_primary in met_df["Metod"].tolist() else heuristic)

    # UI-rad
    top1, top2, top3 = st.columns([1.2, 1, 1])
    with top1:
        st.caption(f"Sanity: {sanity}")
        st.dataframe(met_df, use_container_width=True)
    with top2:
        sel = st.selectbox("Värderingssätt (primärt)", met_df["Metod"].tolist(),
                           index=met_df.index[met_df["Metod"] == default_method].tolist()[0] if default_method in met_df["Metod"].tolist() else 0,
                           key=f"method_{tkr}")
        st.caption("Välj temporärt här — spara som primär i Data om du vill behålla valet.")
        if st.button("💾 Spara som primär i Data", key=f"save_primary_{tkr}"):
            _save_primary_method_to_data(tkr, sel)
            st.success(f"Sparade primär metod '{sel}' till Data.")
    with top3:
        _render_sources_card(src_map, {
            "price": price_now,
            "currency": currency,
            "shares_out": meta.get("shares_out"),
            "net_debt": meta.get("net_debt"),
            "pe_anchor": meta.get("pe_anchor"),
            "revenue_ttm": _f(row.get("Rev TTM")) or None,
            "ebitda_ttm": _f(row.get("EBITDA TTM")) or None,
            "eps_ttm": _f(row.get("EPS TTM")) or None,
            "eps_1y": None,   # visas i tabellen nedan (hämtas/deriveras i engine)
            "eps_2y": None,
            "rev_cagr": _f(row.get("Rev CAGR")),
            "eps_cagr": _f(row.get("EPS CAGR")),
        })

    # Utvalda mål (vald metod)
    choose_row = met_df[met_df["Metod"] == sel].iloc[0]
    p0 = _f(choose_row["Idag"])
    p1 = _f(choose_row["1 år"])
    p2 = _f(choose_row["2 år"])
    p3 = _f(choose_row["3 år"])

    st.markdown("#### 🎯 Fair value (vald metod)")
    mcols = st.columns(4)
    mcols[0].metric("Idag", _fmt_money(p0, currency))
    mcols[1].metric("1 år", _fmt_money(p1, currency))
    mcols[2].metric("2 år", _fmt_money(p2, currency))
    mcols[3].metric("3 år", _fmt_money(p3, currency))
    st.caption(f"Metod: **{sel}** • Valuta: **{currency}** • PE-ankare (TTM/FWD vikt): {settings.get('pe_anchor_weight_ttm','0.5')} • Decay/år: {settings.get('multiple_decay','0.10')}")

    # Uppsida vs pris
    if _pos(price_now):
        upcols = st.columns(4)
        for i, (label, target) in enumerate([("Idag", p0),("1 år", p1),("2 år", p2),("3 år", p3)]):
            if _pos(target):
                delta = (target/price_now - 1.0)
                upcols[i].metric(f"Uppsida {label}", _fmt_pct(delta))
            else:
                upcols[i].metric(f"Uppsida {label}", "–")

    # Utdelning (netto SEK) 1–3 år
    dps_now  = _f(row.get("Årlig utdelning"))
    dps_cagr = _f(row.get("Utdelning CAGR"))
    divs = forecast_dividends_net_sek(currency, shares_owned, dps_now, dps_cagr, fx_map, settings)
    with st.expander("💰 Utdelning (netto, SEK) – prognos", expanded=True):
        st.write(f"• **1 år:** {_fmt_sek(divs['y1'])} • **2 år:** {_fmt_sek(divs['y2'])} • **3 år:** {_fmt_sek(divs['y3'])}")
        st.caption(f"Källskatt {currency}: {int(get_withholding_for(currency, settings)*100)}% • Antal aktier: {int(shares_owned)}")

    # Innehavsvärde nu (SEK)
    port_sek = (price_now or 0.0) * shares_owned * fx
    with st.expander("🧾 Innehavsvärde (nu)", expanded=True):
        st.write(f"Totalt värde: **{_fmt_sek(port_sek)}**  • Pris: {price_now if price_now else '—'} {currency} • FX: {fx:.3f}")

    st.divider()
    a, b, c = st.columns(3)

    if a.button("🔄 Uppdatera Estimat/CAGR", key=f"upd_est_{tkr}"):
        try:
            # hämta igen
            est = fetch_finnhub_estimates(tkr)
            snap = fetch_yahoo_snapshot(tkr)
            eps_ttm = _pos(_nz(snap.get("eps_ttm"), row.get("EPS TTM")))
            eps1 = _pos(_nz(est.get("eps_1y"), row.get("EPS 1Y")))
            eps2 = _pos(est.get("eps_2y"))
            eps_cagr = _f(row.get("EPS CAGR"))
            if eps_cagr is None and _pos(eps_ttm) and _pos(eps1) and eps_ttm > 0:
                eps_cagr = (eps1/eps_ttm) - 1.0
            _update_estimates_in_data(tkr, eps1, eps2, eps_cagr, _f(row.get("Rev CAGR")))
            st.success("Estimat/CAGR uppdaterade i Data.")
        except Exception as e:
            st.error(f"Kunde inte uppdatera estimat: {e}")

    if b.button("💾 Spara vald metods riktkurser → Resultat", key=f"save_res_{tkr}"):
        try:
            _append_or_update_result_row(tkr, currency, sel, p0, p1, p2, p3)
            st.success("Primär riktkurs sparad till fliken Resultat.")
        except Exception as e:
            st.error(f"Kunde inte spara Resultat: {e}")

    if c.button("📷 Snapshot (alla metoder) → Snapshot-flik", key=f"snap_{tkr}"):
        try:
            save_quarter_snapshot(tkr, met_df, {"currency": currency, "pe_anchor": meta.get("pe_anchor"), "decay": meta.get("decay")})
            st.success("Snapshot sparad.")
        except Exception as e:
            st.error(f"Kunde inte spara snapshot: {e}")

# app.py — Del 4/4
# ============================================================
# Sidor: Editor / Analys (med bläddring) / Ranking / Inställningar / Batch
# Snapshot och main()
# ============================================================

# -------------------------
# Snapshot till "Snapshot"-fliken
# -------------------------
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
    snap = pd.concat([snap, pd.DataFrame(rows)], ignore_index=True) if not snap.empty else pd.DataFrame(rows)
    _write_df(SNAPSHOT_TITLE, snap)

# ============================================================
#                   SIDA: Editor (Lägg till/Uppdatera)
# ============================================================
def page_editor():
    st.header("📝 Lägg till / Uppdatera bolag")

    df = read_data_df()
    tickers = ["— nytt —"] + (sorted(df["Ticker"].dropna().astype(str).unique().tolist()) if not df.empty else [])
    pick = st.selectbox("Välj ticker", tickers, index=0)
    is_new = (pick == "— nytt —")

    init = {}
    if not is_new and not df.empty:
        init = df[df["Ticker"].astype(str) == pick].iloc[0].to_dict()

    with st.form("edit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        tkr   = c1.text_input("Ticker", value="" if is_new else pick).strip().upper()
        name  = c2.text_input("Bolagsnamn", value=str(_nz(init.get("Bolagsnamn"), "")))
        sect  = c3.text_input("Sektor", value=str(_nz(init.get("Sektor"), "")))

        bucket = st.selectbox("Bucket/Kategori", DEFAULT_BUCKETS,
                              index=DEFAULT_BUCKETS.index(_nz(init.get("Bucket"), DEFAULT_BUCKETS[0])) if init.get("Bucket") in DEFAULT_BUCKETS else 0)
        valuta = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"],
                              index=["USD","EUR","NOK","CAD","SEK"].index(str(_nz(init.get("Valuta"), "USD")).upper()))

        d1, d2, d3, d4 = st.columns(4)
        antal   = d1.number_input("Antal aktier", min_value=0, step=1, value=int(_nz(_f(init.get("Antal aktier")), 0)))
        gav_sek = d2.number_input("GAV (SEK)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("GAV (SEK)")), 0.0)))
        kurs    = d3.number_input("Aktuell kurs", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("Aktuell kurs")), 0.0)))
        shares  = d4.number_input("Utestående aktier", min_value=0.0, step=1.0, value=float(_nz(_f(init.get("Utestående aktier")), 0.0)))

        e1, e2, e3, e4 = st.columns(4)
        rev_ttm   = e1.number_input("Rev TTM", min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("Rev TTM")), 0.0)))
        ebitda_t  = e2.number_input("EBITDA TTM", min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("EBITDA TTM")), 0.0)))
        eps_ttm   = e3.number_input("EPS TTM", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EPS TTM")), 0.0)))
        net_debt  = e4.number_input("Net debt", min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("Net debt")), 0.0)))

        f1, f2, f3, f4 = st.columns(4)
        pe_ttm   = f1.number_input("PE TTM", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("PE TTM")), 0.0)))
        pe_fwd   = f2.number_input("PE FWD", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("PE FWD")), 0.0)))
        ev_rev   = f3.number_input("EV/Revenue", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EV/Revenue")), 0.0)))
        ev_ebit  = f4.number_input("EV/EBITDA", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EV/EBITDA")), 0.0)))

        g1, g2, g3, g4 = st.columns(4)
        pb      = g1.number_input("P/B", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("P/B")), 0.0)))
        bvps    = g2.number_input("BVPS", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("BVPS")), 0.0)))
        eps1y   = g3.number_input("EPS 1Y (estimat)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EPS 1Y")), 0.0)))
        epscg   = g4.number_input("EPS CAGR", min_value=0.0, step=0.001, value=float(_nz(_f(init.get("EPS CAGR")), 0.0)))

        h1, h2, h3, h4 = st.columns(4)
        revcg   = h1.number_input("Rev CAGR", min_value=0.0, step=0.001, value=float(_nz(_f(init.get("Rev CAGR")), 0.0)))
        dps     = h2.number_input("Årlig utdelning (DPS)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("Årlig utdelning")), 0.0)))
        dpscg   = h3.number_input("Utdelning CAGR", min_value=0.0, step=0.001, value=float(_nz(_f(init.get("Utdelning CAGR")), 0.0)))
        prim    = h4.selectbox("Primär metod", PREFER_ORDER,
                               index=PREFER_ORDER.index(str(_nz(init.get("Primär metod"), "ev_ebitda"))) if _nz(init.get("Primär metod"), "ev_ebitda") in PREFER_ORDER else 0)

        i1, i2, i3 = st.columns(3)
        btn_fetch = i1.form_submit_button("🔎 Hämta från Yahoo (visa)")
        btn_fill  = i2.form_submit_button("↙️ Fyll 'Aktuell kurs' från Yahoo")
        btn_save  = i3.form_submit_button("💾 Spara till Data")

    if btn_fetch and tkr:
        snap = fetch_yahoo_snapshot(tkr)
        st.info(
            f"Hämtat ({tkr}): pris={snap.get('price')} {snap.get('currency')}, "
            f"MCAP={snap.get('market_cap')}, EV/Rev={snap.get('ev_to_sales')}, "
            f"EV/EBITDA={snap.get('ev_to_ebitda')}, P/B={snap.get('p_to_book')}, "
            f"BVPS={snap.get('bvps')}, EPS TTM={snap.get('eps_ttm')}"
        )

    if btn_fill and tkr:
        snap = fetch_yahoo_snapshot(tkr)
        if snap.get("price") is not None:
            st.session_state["Aktuell_kurs_prefill"] = float(snap["price"])
            st.success("Fyllde 'Aktuell kurs' i sessionen. Ändra vid behov och klicka Spara.")
        else:
            st.warning("Hittade inget pris i Yahoo för att fylla i.")

    if btn_save and tkr:
        # ev. session-fyllning
        if "Aktuell_kurs_prefill" in st.session_state:
            kurs = float(st.session_state["Aktuell_kurs_prefill"])
        df = read_data_df()
        ts = now_stamp()
        new_row = {
            "Timestamp": ts,
            "Ticker": tkr,
            "Bolagsnamn": name,
            "Sektor": sect,
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
            "Rev CAGR": revcg,
            "EPS CAGR": epscg,
            "Årlig utdelning": dps,
            "Utdelning CAGR": dpscg,
            "Primär metod": prim,
            "Senast auto uppdaterad": "",
            "Auto källa": "Manuell",
        }
        if df.empty:
            df = pd.DataFrame([new_row], columns=DATA_COLUMNS)
        else:
            if (df["Ticker"].astype(str) == tkr).any():
                mask = df["Ticker"].astype(str) == tkr
                for k, v in new_row.items():
                    if k in df.columns:
                        df.loc[mask, k] = v
                    else:
                        df[k] = np.nan
                        df.loc[mask, k] = v
            else:
                for k in new_row.keys():
                    if k not in df.columns:
                        df[k] = np.nan
                df = pd.concat([df, pd.DataFrame([new_row])[df.columns]], ignore_index=True)
        write_data_df(df)
        st.success("Sparat till Data.")

# ============================================================
#                   SIDA: Analys (bläddringsvy)
# ============================================================
def _calc_upside_row(r: pd.Series, settings: Dict[str,str], fx_map: Dict[str,float], method_override: Optional[str]) -> Dict[str, Any]:
    met_df, sanity, meta, _src = compute_methods_for_row(r, settings, fx_map, primary_override=method_override)
    # välj metod: override > sparad > heuristik
    saved = str(_nz(r.get("Primär metod"), ""))
    chosen = (method_override or (saved if saved in met_df["Metod"].tolist() else _pick_primary_from_table(met_df) or "pe_hist_vs_eps"))
    row = met_df[met_df["Metod"] == chosen].iloc[0]
    fair_today = _f(row["Idag"])
    price = meta.get("price")
    up = None
    if _pos(price) and _pos(fair_today):
        up = (fair_today/price - 1.0) * 100.0
    return {
        "met_df": met_df, "meta": meta, "sanity": sanity,
        "chosen": chosen, "fair_today": fair_today, "upside": up
    }

def page_analysis():
    st.header("🔬 Analys — bläddra ett bolag i taget")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt. Gå till Editor och lägg till ett bolag.")
        return

    # Filter
    f1, f2, f3 = st.columns(3)
    buckets = f1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    mode = f2.selectbox("Urval", ["Alla", "Innehav (antal > 0)", "Watchlist (antal = 0)"], index=0)
    method_pref = f3.selectbox(
        "Värderingssätt för vy",
        ["Auto (primär/heuristik)"] + PREFER_ORDER,
        index=0
    )
    method_override = None if method_pref.startswith("Auto") else method_pref

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if mode == "Innehav (antal > 0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    elif mode == "Watchlist (antal = 0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    if q.empty:
        st.warning("Inget matchade filtret.")
        return

    # Beräkna uppsida & sortera
    rows_with_scores = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            res = _calc_upside_row(r, settings, fx_map, method_override)
            rows_with_scores.append((r, res))
        except Exception:
            rows_with_scores.append((r, {"met_df": pd.DataFrame(), "meta": {}, "sanity": "", "chosen":"", "fair_today":None, "upside":None}))
        prog.progress((i+1)/len(q))
    prog.empty()

    rows_with_scores.sort(key=lambda x: (x[1].get("upside") is None, -1 if x[1].get("upside") is None else x[1]["upside"]), reverse=False)

    # Bläddringsindex i session
    key_ns = f"browse_idx_{hash(tuple(q['Ticker'].astype(str)))}_{method_override or 'auto'}"
    if key_ns not in st.session_state:
        st.session_state[key_ns] = 0

    # Navigering
    n = len(rows_with_scores)
    col_nav1, col_nav2, col_nav3 = st.columns([1,1,6])
    if col_nav1.button("⬅️ Föregående"):
        st.session_state[key_ns] = (st.session_state[key_ns] - 1) % n
    if col_nav2.button("Nästa ➡️"):
        st.session_state[key_ns] = (st.session_state[key_ns] + 1) % n
    col_nav3.caption(f"Visar {st.session_state[key_ns]+1} / {n} • Sorterat på störst uppsida")

    # Rendera ett bolag (störst uppsida först)
    idx = st.session_state[key_ns]
    row, res = rows_with_scores[idx]
    # render_company_view använder sitt eget metod-val UI, men vi skickar override om användaren valt något annat än Auto.
    render_company_view(row, settings, fx_map, method_override=method_override)

# ============================================================
#                   SIDA: Ranking (tabell)
# ============================================================
def page_ranking():
    st.header("🏁 Ranking – Uppsida mot vald (primär/override) fair value (Idag)")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    b1, b2 = st.columns(2)
    buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    view = b2.selectbox("Urval", ["Innehav (antal > 0)","Watchlist (antal = 0)","Alla"], index=0)
    msel = st.selectbox("Värderingssätt för ranking", ["Auto (primär/heuristik)"] + PREFER_ORDER, index=0)
    method_override = None if msel.startswith("Auto") else msel

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if view == "Innehav (antal > 0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    elif view == "Watchlist (antal = 0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    if q.empty:
        st.warning("Inget matchade filtret.")
        return

    rows = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            res = _calc_upside_row(r, settings, fx_map, method_override)
            price = res["meta"].get("price")
            fair_today = res["fair_today"]
            up = res["upside"]
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": res["meta"].get("currency"),
                "Pris": price,
                "Metod": res["chosen"],
                "Fair value (Idag)": fair_today,
                "Uppsida %": up,
            })
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None, "Metod": None, "Fair value (Idag)": None, "Uppsida %": None
            })
        prog.progress((i+1)/len(q))
    prog.empty()

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(by=["Uppsida %"], ascending=False, na_position="last")
    st.dataframe(out, use_container_width=True)

# ============================================================
#               SIDA: Inställningar (källskatt, modell, FX)
# ============================================================
def page_settings():
    st.header("⚙️ Inställningar")

    # Källskatt per valuta (dynamisk lista baserat på FX-arket)
    fx_df = _read_df(FX_TITLE)
    currencies = ["USD","EUR","NOK","CAD","SEK"]
    s_map = get_settings_map()

    st.subheader("Källskatt per valuta")
    with st.form("wh_form"):
        cols = st.columns(len(currencies))
        vals = {}
        for i, ccy in enumerate(currencies):
            default = 0.0 if ccy == "SEK" else 0.15
            cur = float(s_map.get(f"withholding_{ccy}", default))
            vals[ccy] = cols[i].number_input(f"{ccy}", min_value=0.0, max_value=1.0, step=0.01, value=cur, format="%.2f")
        submit = st.form_submit_button("💾 Spara källskatt")
    if submit:
        s = _read_df(SETTINGS_TITLE)
        if s.empty: s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def upsert(k, v):
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s = pd.concat([s, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)
            return s
        for ccy, v in vals.items():
            s = upsert(f"withholding_{ccy}", v)
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    p1, p2 = st.columns(2)
    cur = get_settings_map()
    pe_w  = p1.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05, value=float(cur.get("pe_anchor_weight_ttm","0.5")))
    decay = p2.number_input("Multipel-decay per år (0..1)", min_value=0.0, max_value=1.0, step=0.01, value=float(cur.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty: s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def setv(k, v):
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s.loc[len(s)] = [k, str(v)]
        setv("pe_anchor_weight_ttm", pe_w)
        setv("multiple_decay", decay)
        _write_df(SETTINGS_TITLE, s)
        st.success("Parametrar uppdaterade.")

    st.subheader("Valutakurser (SEK per 1)")
    if st.button("🔄 Hämta & uppdatera FX från Yahoo"):
        mp = _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")
        st.json(mp)

# ============================================================
#                   SIDA: Batch-uppdatering
# ============================================================
def page_batch():
    st.header("🧰 Batch-uppdatering")

    df = read_data_df()
    if df.empty:
        st.info("Inga bolag i Data.")
        return
    settings = get_settings_map()
    fx_map   = get_fx_map()

    throttle = st.slider("Fördröjning per bolag (sek)", min_value=0.1, max_value=2.0, value=0.6, step=0.1)

    if st.button("🔄 Uppdatera pris & nycklar från Yahoo (alla)"):
        prog = st.progress(0.0)
        df2 = df.copy()
        for i, (idx, r) in enumerate(df2.iterrows()):
            t = str(r["Ticker"]).strip().upper()
            snap = fetch_yahoo_snapshot(t)
            # skriv över fält vi kan
            mapping = {
                "Aktuell kurs": "price",
                "Valuta": "currency",
                "Rev TTM": "revenue_ttm",
                "EBITDA TTM": "ebitda_ttm",
                "EPS TTM": "eps_ttm",
                "PE TTM": "pe_ttm",
                "PE FWD": "pe_fwd",
                "EV/Revenue": "ev_to_sales",
                "EV/EBITDA": "ev_to_ebitda",
                "P/B": "p_to_book",
                "BVPS": "bvps",
                "Net debt": "net_debt",
            }
            for col, key in mapping.items():
                val = snap.get(key)
                if val is not None:
                    df2.at[idx, col] = val
            df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
            df2.at[idx, "Auto källa"] = "Yahoo"
            time.sleep(throttle)
            prog.progress((i+1)/len(df2))
        write_data_df(df2)
        prog.empty()
        st.success("Uppdaterat alla tickers från Yahoo.")

    if st.button("📷 Spara snapshots (alla)"):
        prog = st.progress(0.0)
        for i, (_, r) in enumerate(df.iterrows()):
            met_df, _, meta, _ = compute_methods_for_row(r, settings, fx_map, primary_override=None)
            save_quarter_snapshot(str(r["Ticker"]).strip().upper(), met_df, meta)
            time.sleep(throttle)
            prog.progress((i+1)/len(df))
        prog.empty()
        st.success("Snapshot sparade.")

# ============================================================
#                           MAIN
# ============================================================
def run_main_ui():
    st.title(APP_TITLE)

    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        st.write("FX:", get_fx_map())
        st.write("Settings:", get_settings_map())

    page = st.sidebar.radio("Sidor", ["Editor", "Analys", "Ranking", "Inställningar", "Batch"], index=1)

    if page == "Editor":
        page_editor()
    elif page == "Analys":
        page_analysis()
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
