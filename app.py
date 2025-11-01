# app.py — Del 1/4
# ============================================================
# Bas: Streamlit-app för fair value / riktkurser / portfölj
# Lagring: Google Sheets (Data, Resultat, Valutakurser, Settings, Snapshot)
# Hämtning: Yahoo (yfinance) + valfri Finnhub (estimat)
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

PREFER_ORDER = [
    "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b",
    "ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
]

# mild throttling för att undvika 429
SHEETS_MIN_SLEEP = 0.25
SHEETS_MAX_SLEEP = 0.6

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
    """Hämta värde från env eller st.secrets."""
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
    """Exponential backoff runt gspread-anrop (429/5xx)."""
    delay = SHEETS_MIN_SLEEP
    for attempt in range(6):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            code = getattr(e, "response", None)
            # alltid sova lite för att slippa per-minute-quota
            time.sleep(delay + random.uniform(0, 0.2))
            delay = min(SHEETS_MAX_SLEEP + attempt * 0.25, 2.0)
            if attempt >= 5:
                raise
        except Exception:
            # okända fel – gör ett försök till med liten sömn
            time.sleep(delay)
            delay = min(SHEETS_MAX_SLEEP + attempt * 0.25, 2.0)
            if attempt >= 5:
                raise

# =========================
# Google Sheets Auth
# =========================
@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    """
    Skapa gspread Client från GOOGLE_CREDENTIALS.
    Stödjer: Mapping/AttrDict, JSON-sträng, bytes/bytearray.
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
    """
    Öppnar spreadsheet via SHEET_URL eller SHEET_ID (trim av whitespace).
    Notera underscore på _gc för att undvika Streamlit hashing-problem.
    """
    sheet_url = (_env_or_secret("SHEET_URL") or "").strip()
    sheet_id  = (_env_or_secret("SHEET_ID")  or "").strip()
    if sheet_url:
        return _with_backoff(_gc.open_by_url, sheet_url)
    if sheet_id:
        return _with_backoff(_gc.open_by_key, sheet_id)
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
    # skriv head + body
    _with_backoff(ws.clear)
    if out.shape[0] == 0:
        _with_backoff(ws.update, [list(out.columns)])
    else:
        body = [list(out.columns)] + out.astype(str).values.tolist()
        _with_backoff(ws.update, body)
    # litet andrum för kvoter
    time.sleep(SHEETS_MIN_SLEEP)

def _append_rows(title: str, rows: List[List[Any]]):
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    _with_backoff(ws.append_rows, rows, value_input_option="RAW")
    time.sleep(SHEETS_MIN_SLEEP)

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
        changed = False
        for c in DATA_COLUMNS:
            if c not in df.columns:
                df[c] = np.nan
                changed = True
        if changed:
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
                hist = t.history(period="1d")
                if not hist.empty: 
                    px = float(hist["Close"].iloc[-1])
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

# -------------------------
# Yahoo (yfinance) – robust snapshot + källspårning
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
      net_debt,
      source_map (per-fälts källa: 'yahoo_fast' | 'yahoo_info' | 'derived')
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {}
    src: Dict[str, str] = {}

    # Snabbkanal
    try:
        fi = tk.fast_info
        v = _safe_float(getattr(fi, "last_price", None))
        if v is not None: out["price"] = v; src["price"] = "yahoo_fast"
        v = getattr(fi, "currency", None)
        if v: out["currency"] = v; src["currency"] = "yahoo_fast"
        v = _safe_float(getattr(fi, "market_cap", None))
        if v is not None: out["market_cap"] = v; src["market_cap"] = "yahoo_fast"
        v = _safe_float(getattr(fi, "shares", None))
        if v is not None: out["shares"] = v; src["shares"] = "yahoo_fast"
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

    def setv(key, val, tag="yahoo_info"):
        if val is not None and val == val:
            out[key] = val
            if key not in src:
                src[key] = tag

    setv("price",       _safe_float(gi("currentPrice")))
    setv("currency",    gi("currency"))
    setv("market_cap",  _safe_float(gi("marketCap")))
    setv("eps_ttm",     _safe_float(gi("trailingEps")))
    setv("pe_ttm",      _safe_float(gi("trailingPE")))
    setv("pe_fwd",      _safe_float(gi("forwardPE")))
    setv("revenue_ttm", _safe_float(gi("totalRevenue")))
    setv("ebitda_ttm",  _safe_float(gi("ebitda")))
    setv("ev_to_sales", _safe_float(gi("enterpriseToRevenue")))
    setv("ev_to_ebitda",_safe_float(gi("enterpriseToEbitda")))
    setv("p_to_book",   _safe_float(gi("priceToBook")))
    setv("bvps",        _safe_float(gi("bookValue")))

    # EV och nettoskuld
    ev_info   = _safe_float(gi("enterpriseValue"))
    total_debt= _safe_float(gi("totalDebt"))
    total_cash= _safe_float(gi("totalCash"))

    if ev_info is not None:
        out["ev"] = ev_info; src["ev"] = "yahoo_info"
    elif _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
        src["ev"] = "derived"

    if _pos(out.get("market_cap")) is not None and _pos(out.get("ev")) is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]; src["net_debt"] = "derived"

    # Shares fallback via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            src["shares"] = "derived"
        except Exception:
            pass

    # Normalisera valuta
    out["currency"] = str(out.get("currency") or "USD").upper()

    out["source_map"] = src
    return out

# -------------------------
# Finnhub (valfritt) – EPS-estimat 1–2 år framåt
# -------------------------
def _get_finnhub_key() -> Optional[str]:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

@st.cache_data(ttl=900, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker hämta EPS-estimat 1–2 år framåt från Finnhub (om nyckel finns).
    Returnerar {"eps_1y": float|None, "eps_2y": float|None, "source": "finnhub"|"none"}
    """
    key = _get_finnhub_key()
    if not key:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

    # Primärt försök: /stock/estimate (periodsorterad lista med epsAvg)
    try:
        url = f"https://finnhub.io/api/v1/stock/estimate?symbol={ticker}&token={key}"
        r = requests.get(url, timeout=12)
        if not r.ok:
            return {"eps_1y": None, "eps_2y": None, "source": "none"}
        js = r.json()
        rows = js if isinstance(js, list) else js.get("data", [])
        rows = rows or []
        # sortera på period (YYYY-MM-DD eller YYYY-MM)
        def per_key(x):
            p = str(x.get("period") or "")
            return p
        rows = sorted(rows, key=per_key)
        vals = [_safe_float(x.get("epsAvg")) for x in rows if _safe_float(x.get("epsAvg")) is not None]
        if not vals:
            return {"eps_1y": None, "eps_2y": None, "source": "none"}
        # anta senaste = 1Y, näst senaste = 2Y om finns
        eps_1y = vals[-1]
        eps_2y = vals[-2] if len(vals) > 1 else None
        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "source": "finnhub"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

# -------------------------
# Multipel-decay & ankar-P/E
# -------------------------
def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    """Minska multipeln med decay per år (linjärt mot ett golv)."""
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
    Prioritet: given eps_1y/2y → annars extrapolera med EPS CAGR från ttm.
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
    if r0 is None:
        return None, None, None, None
    if cg is None:
        return r0, r0, r0, r0
    r1 = r0 * (1.0 + cg)
    r2 = r1 * (1.0 + cg)
    r3 = r2 * (1.0 + cg)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Proxy: EBITDA växer ungefär i takt med omsättning (om vi saknar riktiga prognoser).
    Om rev-path saknas → håll ebitda konstant.
    """
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
# Metodval (heuristik)
# -------------------------
def choose_primary_method(met_df: pd.DataFrame) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Välj primär metod: flest icke-NaN → bryt-tie med PREFER_ORDER.
    Returnerar (method, idag, y1, y2, y3)
    """
    if met_df is None or met_df.empty:
        return None, None, None, None, None
    counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None, None, None, None, None
    maxc = counts.max()
    candidates = list(counts[counts == maxc].index)
    chosen = None
    for p in PREFER_ORDER:
        if p in candidates:
            chosen = p
            break
    if chosen is None:
        chosen = candidates[0]
    row = met_df[met_df["Metod"] == chosen].iloc[0]
    return chosen, _f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"])

# -------------------------
# Huvudmotor per rad
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Beräknar metodtabell (Idag, 1,2,3 år) för raden.
    Returnerar (methods_df, sanity_text, meta)
    meta innehåller: currency, price, shares_out, net_debt, pe_anchor, decay, source_map
    """
    ticker = str(row.get("Ticker", "")).strip().upper()
    # 1) Live-data
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.35)  # mild throttling
    est  = fetch_finnhub_estimates(ticker)

    src_map = dict(snap.get("source_map") or {})  # kopia som vi fyller på

    # 2) Inputs (med fallback från Data-bladet + källmarkering)
    def take(name_snap, name_row, manual_tag="manual"):
        """Ta värde från snapshot, annars rad; märk källa."""
        val = snap.get(name_snap)
        if val is None and (row.get(name_row) is not None and row.get(name_row) == row.get(name_row)):
            val = _f(row.get(name_row))
            if val is not None:
                src_map[name_snap] = manual_tag
        return val

    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs")))
    if price is None and _pos(row.get("Aktuell kurs")):
        price = _pos(row.get("Aktuell kurs")); src_map["price"] = "manual"
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper()
    mcap     = _pos(snap.get("market_cap"))
    ev_now   = _pos(_nz(snap.get("ev"), None))
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier")))
    if shares is None and _pos(row.get("Utestående aktier")):
        shares = _pos(row.get("Utestående aktier")); src_map["shares"] = "manual"

    net_debt   = _nz(snap.get("net_debt"), row.get("Net debt"))
    if net_debt is None and _pos(row.get("Net debt")):
        net_debt = _pos(row.get("Net debt")); src_map["net_debt"] = "manual"

    rev_ttm    = _pos(_nz(snap.get("revenue_ttm"), row.get("Rev TTM")))
    if rev_ttm is None and _pos(row.get("Rev TTM")):
        rev_ttm = _pos(row.get("Rev TTM")); src_map["revenue_ttm"] = "manual"

    ebitda_ttm = _pos(_nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM")))
    if ebitda_ttm is None and _pos(row.get("EBITDA TTM")):
        ebitda_ttm = _pos(row.get("EBITDA TTM")); src_map["ebitda_ttm"] = "manual"

    eps_ttm    = _pos(_nz(snap.get("eps_ttm"), row.get("EPS TTM")))
    if eps_ttm is None and _pos(row.get("EPS TTM")):
        eps_ttm = _pos(row.get("EPS TTM")); src_map["eps_ttm"] = "manual"

    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM")))
    if pe_ttm is None and _pos(row.get("PE TTM")):
        pe_ttm = _pos(row.get("PE TTM")); src_map["pe_ttm"] = "manual"

    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD")))
    if pe_fwd is None and _pos(row.get("PE FWD")):
        pe_fwd = _pos(row.get("PE FWD")); src_map["pe_fwd"] = "manual"

    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue")))
    if ev_sales is None and _pos(row.get("EV/Revenue")):
        ev_sales = _pos(row.get("EV/Revenue")); src_map["ev_to_sales"] = "manual"

    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA")))
    if ev_ebitda is None and _pos(row.get("EV/EBITDA")):
        ev_ebitda = _pos(row.get("EV/EBITDA")); src_map["ev_to_ebitda"] = "manual"

    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B")))
    if p_b is None and _pos(row.get("P/B")):
        p_b = _pos(row.get("P/B")); src_map["p_to_book"] = "manual"

    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS")))
    if bvps is None and _pos(row.get("BVPS")):
        bvps = _pos(row.get("BVPS")); src_map["bvps"] = "manual"

    # Estimat / tillväxt
    eps_1y_est = _pos(est.get("eps_1y")) if _pos(est.get("eps_1y")) else _pos(row.get("EPS 1Y"))
    if _pos(est.get("eps_1y")): src_map["eps_1y"] = "finnhub"
    elif _pos(row.get("EPS 1Y")): src_map["eps_1y"] = "manual"
    else: src_map["eps_1y"] = ""

    eps_2y_est = _pos(est.get("eps_2y")) if _pos(est.get("eps_2y")) else None
    if _pos(est.get("eps_2y")): src_map["eps_2y"] = "finnhub"
    else: src_map["eps_2y"] = ""

    eps_cagr   = _f(row.get("EPS CAGR"))
    rev_cagr   = _f(row.get("Rev CAGR"))

    # 3) Härled EPS om saknas men PE+price finns
    eps_ttm, src_eps_ttm, eps_1y_est2, src_eps_1y = _derive_eps_from_pe_if_missing(
        price, pe_ttm, pe_fwd, eps_ttm, eps_1y_est
    )
    if src_eps_ttm.startswith("derived"): src_map["eps_ttm"] = src_eps_ttm
    if src_eps_1y.startswith("derived"):  src_map["eps_1y"]  = src_eps_1y
    # Om finnub gav 1y, behåll den framför derivatan:
    if _pos(est.get("eps_1y")):
        eps_1y_est = _pos(est.get("eps_1y")); src_map["eps_1y"] = "finnhub"
    else:
        eps_1y_est = eps_1y_est2

    # 4) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 5) Paths (EPS prioriterar: ttm → 1y/2y (finnhub) → cagr; REV via cagr; EBITDA skalar med REV)
    e0, e1, e2, e3 = _eps_path(eps_ttm, eps_1y_est, eps_2y_est, eps_cagr)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(ebitda_ttm, r0, r1, r2, r3)

    # Multiplar med decay (antag re-rating nedåt)
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = (
        ev_sales,
        _decay_multiple(ev_sales, 1, decay),
        _decay_multiple(ev_sales, 2, decay),
        _decay_multiple(ev_sales, 3, decay),
    )
    eve0, eve1, eve2, eve3 = (
        ev_ebitda,
        _decay_multiple(ev_ebitda, 1, decay),
        _decay_multiple(ev_ebitda, 2, decay),
        _decay_multiple(ev_ebitda, 3, decay),
    )
    pb0, pb1, pb2, pb3 = (
        p_b,
        _decay_multiple(p_b, 1, decay),
        _decay_multiple(p_b, 2, decay),
        _decay_multiple(p_b, 3, decay),
    )

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
    # P/B (kräver BVPS)
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

    # 7) Sanity + meta
    def ok(v): return "ok" if _pos(v) else "—"
    sanity = (
        f"price={ok(price)}, eps_ttm={ok(e0)} ({src_map.get('eps_ttm','') or '—'}), "
        f"eps_1y={ok(e1)} ({src_map.get('eps_1y','') or '—'}), eps_2y={ok(e2)} ({src_map.get('eps_2y','') or '—'}), "
        f"rev_ttm={ok(r0)}, ebitda_ttm={ok(b0)}, shares={ok(shares)}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "source_map": src_map,
        "paths": {"eps": (e0, e1, e2, e3), "rev": (r0, r1, r2, r3), "ebitda": (b0, b1, b2, b3)},
        "multiples": {"pe": (pe0, pe1m, pe2m, pe3m), "ev_sales": (evs0, evs1, evs2, evs3), "ev_ebitda": (eve0, eve1, eve2, eve3), "p_b": (pb0, pb1, pb2, pb3)},
    }
    return methods_df, sanity, meta

# app.py — Del 3/4
# ============================================================
# UI för bolagspresentation (en i taget), metodval i Analys,
# källspårning & sparning till Sheets (Resultat/Primär metod)
# ============================================================

# Ordning för metodpreferens (används vid auto-val)
METHOD_LIST  = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]
PREFER_ORDER = METHOD_LIST[:]  # samma ordning

# ---------- Hjälpformat ----------
def _fmt_money(v: Optional[float], ccy: str) -> str:
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "–"
    try:
        return f"{v:,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} {ccy}"

def _fmt_sek(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "0 SEK"
    try:
        return f"{v:,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

# ---------- Spara till fliken Resultat (append/overwrite senaste för ticker) ----------
def _append_or_update_result(
    ticker: str,
    currency: str,
    method: Optional[str],
    today: Optional[float],
    y1: Optional[float],
    y2: Optional[float],
    y3: Optional[float],
) -> None:
    res_df = _read_df(RESULT_TITLE)
    ts = now_stamp()
    new_row = {
        "Timestamp": ts,
        "Ticker": ticker,
        "Valuta": currency,
        "Metod": method or "",
        "Riktkurs idag": today,
        "Riktkurs 1 år": y1,
        "Riktkurs 2 år": y2,
        "Riktkurs 3 år": y3,
    }
    if not res_df.empty and "Ticker" in res_df.columns:
        mask = res_df["Ticker"].astype(str) == ticker
        if mask.any():
            idx = res_df.index[mask][-1]
            for k, v in new_row.items():
                if k in res_df.columns:
                    res_df.at[idx, k] = v
                else:
                    res_df[k] = np.nan
                    res_df.at[idx, k] = v
            _write_df(RESULT_TITLE, res_df)
            return
    # append
    cols = list(res_df.columns) if not res_df.empty else list(new_row.keys())
    for k in new_row.keys():
        if k not in cols:
            cols.append(k)
    res_df = pd.concat([res_df, pd.DataFrame([new_row])[cols]], ignore_index=True)
    _write_df(RESULT_TITLE, res_df[cols])

# ---------- Uppdatera primär metod i Data ----------
def _save_primary_method_in_data(ticker: str, method: str) -> None:
    df = read_data_df()
    if df.empty:
        st.warning("Data-bladet är tomt.")
        return
    if "Primär metod" not in df.columns:
        df["Primär metod"] = np.nan
    mask = (df["Ticker"].astype(str).str.upper() == str(ticker).upper())
    if not mask.any():
        st.warning(f"Hittade inte {ticker} i Data.")
        return
    df.loc[mask, "Primär metod"] = method
    write_data_df(df)

# ---------- Rendera en bolagsvy ----------
def render_company_view(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float], method_override: Optional[str] = None) -> Dict[str, Any]:
    tkr = str(row.get("Ticker", "")).strip().upper()
    name = str(_nz(row.get("Bolagsnamn"), tkr))
    bucket = str(_nz(row.get("Bucket"), "")).strip()

    st.markdown(f"### {tkr} • {name} {'• ' + bucket if bucket else ''}")

    # Kör motor
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    currency = meta.get("currency") or str(_nz(row.get("Valuta"), "USD")).upper()
    price_now = meta.get("price")
    fx = fx_map.get(currency, 1.0) or 1.0

    # Tillgängliga metoder (icke-NaN)
    available_methods = methods_df.loc[methods_df[["Idag","1 år","2 år","3 år"]].notna().any(axis=1), "Metod"].tolist()

    # Välj primär metod: override → Data-blad → auto
    saved_primary = str(_nz(row.get("Primär metod"), "")) if "Primär metod" in row.index else ""
    chosen_method = None
    if method_override and method_override in available_methods:
        chosen_method = method_override
    elif saved_primary and saved_primary in available_methods:
        chosen_method = saved_primary
    else:
        cm, p0c, _, _, _ = choose_primary_method(methods_df)
        if cm in available_methods:
            chosen_method = cm
        else:
            chosen_method = available_methods[0] if available_methods else None

    # Visa toppkort
    st.caption(f"Sanity: {sanity}")
    with st.expander("🔍 Metodtabell (alla)", expanded=False):
        st.dataframe(methods_df, use_container_width=True)

    # Primär riktkurs (från valt method)
    p_row = methods_df[methods_df["Metod"] == chosen_method].iloc[0] if chosen_method else None
    p0 = _f(p_row["Idag"]) if p_row is not None else None
    p1 = _f(p_row["1 år"]) if p_row is not None else None
    p2 = _f(p_row["2 år"]) if p_row is not None else None
    p3 = _f(p_row["3 år"]) if p_row is not None else None

    st.markdown("#### 🎯 Primär riktkurs")
    cols = st.columns(5)
    cols[0].metric("Aktuell kurs", _fmt_money(price_now, currency))
    cols[1].metric("Idag", _fmt_money(p0, currency))
    cols[2].metric("1 år", _fmt_money(p1, currency))
    cols[3].metric("2 år", _fmt_money(p2, currency))
    cols[4].metric("3 år", _fmt_money(p3, currency))
    st.caption(f"Metod: **{chosen_method or '—'}** • Valuta: **{currency}** • PE-ankare vikt (TTM): {int(float(settings.get('pe_anchor_weight_ttm','0.5'))*100)}% • Decay: {settings.get('multiple_decay','0.10')}")

    # Uppsida/ner-sida vs aktuell kurs
    if _pos(price_now) and _pos(p0):
        delta_pct = (p0/price_now - 1.0) * 100.0
        st.metric("Uppsida (Idag mot aktuell kurs)", f"{delta_pct:,.1f}%".replace(",", " "))

    # Källor & inputs
    with st.expander("📎 Källor & inputs som beräkningen bygger på", expanded=True):
        sm = meta.get("source_map", {}) or {}
        paths = meta.get("paths", {}) or {}
        multiples = meta.get("multiples", {}) or {}
        # Visa viktiga inputs med källa
        show_fields = [
            ("price","Pris"), ("currency","Valuta"), ("shares","Utestående aktier"),
            ("net_debt","Net debt"), ("revenue_ttm","Rev TTM"), ("ebitda_ttm","EBITDA TTM"),
            ("eps_ttm","EPS TTM"), ("eps_1y","EPS 1Y (estimat)"), ("eps_2y","EPS 2Y (estimat)"),
            ("pe_ttm","PE TTM"), ("pe_fwd","PE FWD"),
            ("ev_to_sales","EV/Revenue"), ("ev_to_ebitda","EV/EBITDA"),
            ("p_to_book","P/B"), ("bvps","BVPS")
        ]
        vals = []
        snap = fetch_yahoo_snapshot(tkr)  # endast för att hämta raw-fält som saknas i row/meta
        for key, label in show_fields:
            # Hämta värde från meta/snap/row
            v = None
            if key in ("price","currency","shares","net_debt","revenue_ttm","ebitda_ttm","eps_ttm","pe_ttm","pe_fwd","ev_to_sales","ev_to_ebitda","p_to_book","bvps"):
                v = snap.get(key, None)
                # fallback från rad
                if v is None:
                    # mappa key -> datakolumn
                    map_row = {
                        "price":"Aktuell kurs", "currency":"Valuta", "shares":"Utestående aktier",
                        "net_debt":"Net debt", "revenue_ttm":"Rev TTM", "ebitda_ttm":"EBITDA TTM",
                        "eps_ttm":"EPS TTM", "pe_ttm":"PE TTM", "pe_fwd":"PE FWD",
                        "ev_to_sales":"EV/Revenue", "ev_to_ebitda":"EV/EBITDA",
                        "p_to_book":"P/B", "bvps":"BVPS"
                    }
                    v = _f(row.get(map_row.get(key,"")))
            elif key in ("eps_1y","eps_2y"):
                # tas från Finnhub i compute_methods_for_row -> meta.paths['eps']
                eps_tuple = paths.get("eps") or (None,None,None,None)
                idx = 1 if key == "eps_1y" else 2
                v = eps_tuple[idx] if len(eps_tuple) > idx else None
            src = sm.get(key, "")
            vals.append([label, v, src or ""])

        df_inputs = pd.DataFrame(vals, columns=["Fält","Värde","Källa"])
        st.dataframe(df_inputs, use_container_width=True)

        with st.expander("🧪 Använda paths & multiplar", expanded=False):
            e0,e1,e2,e3 = (paths.get("eps") or (None,)*4)
            r0,r1,r2,r3 = (paths.get("rev") or (None,)*4)
            b0,b1,b2,b3 = (paths.get("ebitda") or (None,)*4)
            pe0,pe1m,pe2m,pe3m = (multiples.get("pe") or (None,)*4)
            evs0,evs1,evs2,evs3 = (multiples.get("ev_sales") or (None,)*4)
            eve0,eve1,eve2,eve3 = (multiples.get("ev_ebitda") or (None,)*4)
            pb0,pb1,pb2,pb3     = (multiples.get("p_b") or (None,)*4)
            st.write("**EPS-path**:", e0, e1, e2, e3)
            st.write("**REV-path**:", r0, r1, r2, r3)
            st.write("**EBITDA-path**:", b0, b1, b2, b3)
            st.write("**PE (med decay)**:", pe0, pe1m, pe2m, pe3m)
            st.write("**EV/S (med decay)**:", evs0, evs1, evs2, evs3)
            st.write("**EV/EBITDA (med decay)**:", eve0, eve1, eve2, eve3)
            st.write("**P/B (med decay)**:", pb0, pb1, pb2, pb3)

    # Utdelning (netto SEK) kommande 1–3 år
    shares_owned = _f(row.get("Antal aktier")) or 0.0
    dps_now = _f(row.get("Årlig utdelning"))
    dps_cagr = _f(row.get("Utdelning CAGR"))
    divs = forecast_dividends_net_sek(currency, shares_owned, dps_now, dps_cagr, fx_map, settings)
    with st.expander("💰 Utdelning (netto, SEK)", expanded=True):
        st.write(f"• **1 år:** {_fmt_sek(divs['y1'])} • **2 år:** {_fmt_sek(divs['y2'])} • **3 år:** {_fmt_sek(divs['y3'])}")
        st.caption(f"Källskatt {currency}: {int(get_withholding_for(currency, settings)*100)}% • Antal aktier: {int(shares_owned)}")

    # Åtgärder (primär metod, spara riktkurs)
    st.divider()
    c1, c2, c3 = st.columns([2,2,3])
    # Välj metod i analysvyn (override)
    method_choice = c1.selectbox(
        "Primär metod (välj här för att testa; spara för att lagra i Data)",
        options=available_methods,
        index=available_methods.index(chosen_method) if chosen_method in available_methods else 0,
        key=f"method_sel_{tkr}"
    )
    if c2.button("💾 Spara primär metod till Data", key=f"save_pm_{tkr}"):
        _save_primary_method_in_data(tkr, method_choice)
        st.success(f"Primär metod sparad: {method_choice}")

    if c3.button("🧷 Spara primär riktkurs till Resultat", key=f"save_res_{tkr}"):
        prow = methods_df[methods_df["Metod"] == method_choice].iloc[0]
        _append_or_update_result(
            tkr, currency, method_choice,
            _f(prow["Idag"]), _f(prow["1 år"]), _f(prow["2 år"]), _f(prow["3 år"])
        )
        st.success("Primär riktkurs sparad till fliken Resultat.")

    return {
        "ticker": tkr,
        "currency": currency,
        "price": price_now,
        "method": method_choice,
        "p0": p0, "p1": p1, "p2": p2, "p3": p3,
        "upside_today_pct": ((p0/price_now - 1.0)*100.0) if (_pos(p0) and _pos(price_now)) else None,
        "methods_df": methods_df,
        "meta": meta,
    }

# ---------- Analys-sidan: en-i-taget, sorterat på störst uppsida ----------
def page_analysis():
    st.header("🔬 Analys – en i taget (störst uppsida först)")
    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    # Filter
    f1, f2, f3 = st.columns([2,2,2])
    buckets = f1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_only = f2.selectbox("Urval", ["Alla", "Endast innehav (antal > 0)", "Endast watchlist (antal = 0)"], index=0)
    throttle = f3.slider("Fördröjning per bolag (sek)", 0.0, 1.5, 0.30, 0.05)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_only == "Endast innehav (antal > 0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    elif owned_only == "Endast watchlist (antal = 0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]
    q = q.reset_index(drop=True)

    # Knapp för att beräkna ranking (för att spara API-kvot)
    if st.button("⚡ Beräkna & sortera på uppsida (Idag vs vald primär metod)"):
        results = []
        prog = st.progress(0.0)
        for i in range(len(q)):
            r = q.iloc[i]
            # hämtar ev sparad primär metod
            saved_primary = str(_nz(r.get("Primär metod"), ""))
            # beräkna
            met_df, sanity, meta = compute_methods_for_row(r, settings, fx_map)
            # välj metod: sparad → auto
            available = met_df.loc[met_df[["Idag","1 år","2 år","3 år"]].notna().any(axis=1), "Metod"].tolist()
            if saved_primary in available:
                chosen = saved_primary
            else:
                chosen, _, _, _, _ = choose_primary_method(met_df)
                if chosen not in available and available:
                    chosen = available[0]
            # ta fair idag & pris
            prow = met_df[met_df["Metod"] == chosen].iloc[0] if chosen else None
            fair_today = _f(prow["Idag"]) if prow is not None else None
            price_now = meta.get("price")
            up = (fair_today/price_now - 1.0)*100.0 if (_pos(fair_today) and _pos(price_now)) else None
            results.append({"idx": i, "ticker": r.get("Ticker"), "up": up, "met": chosen})
            time.sleep(throttle)
            prog.progress((i+1)/max(1,len(q)))
        prog.empty()
        # sortera och spara i session
        results.sort(key=lambda x: (x["up"] is None, -(x["up"] or -1e9)))
        st.session_state["anal_sorted"] = results
        st.session_state["anal_idx"] = 0

    # Init tom sortering om saknas
    if "anal_sorted" not in st.session_state or not st.session_state["anal_sorted"]:
        st.info("Tryck på **Beräkna & sortera** för att skapa listan.")
        return

    # Bläddra
    total = len(st.session_state["anal_sorted"])
    if "anal_idx" not in st.session_state:
        st.session_state["anal_idx"] = 0
    cprev, cinfo, cnext = st.columns([1,2,1])
    if cprev.button("⬅️ Föregående") and st.session_state["anal_idx"] > 0:
        st.session_state["anal_idx"] -= 1
    if cnext.button("Nästa ➡️") and st.session_state["anal_idx"] < total-1:
        st.session_state["anal_idx"] += 1
    cur = st.session_state["anal_idx"]
    cinfo.write(f"Post **{cur+1} / {total}** • Störst uppsida först")

    # Visa vald post
    cur_idx = st.session_state["anal_sorted"][cur]["idx"]
    cur_row = q.iloc[cur_idx]

    # Metodoverride-val för just denna vy
    # (startvärde = sparad metod, annars auto)
    saved_primary = str(_nz(cur_row.get("Primär metod"), ""))
    method_override = st.selectbox(
        "Temporärt metodval för denna vy (kan sparas nedan)",
        options=METHOD_LIST,
        index=METHOD_LIST.index(saved_primary) if saved_primary in METHOD_LIST else 0,
        key=f"override_sel_{cur_row.get('Ticker')}"
    )

    # Rendera bolagskortet
    render_company_view(cur_row, settings, fx_map, method_override=method_override)

# app.py — Del 4/4
# ============================================================
# Sidor: Editor / Ranking / Inställningar / Batch
# Snapshot + choose_primary_method + main()
# ============================================================

# ---------- Snapshot till "Snapshot"-fliken ----------
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

# ---------- Val av primär metod (auto) ----------
def choose_primary_method(met_df: pd.DataFrame) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if met_df is None or met_df.empty:
        return None, None, None, None, None
    counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None, None, None, None, None
    maxc = counts.max()
    candidates = list(counts[counts == maxc].index)
    chosen = None
    for p in PREFER_ORDER:
        if p in candidates:
            chosen = p
            break
    if chosen is None:
        chosen = candidates[0]
    row = met_df[met_df["Metod"] == chosen].iloc[0]
    return chosen, _f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"])

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
        row = df[df["Ticker"].astype(str).str.upper() == str(pick).upper()].head(1)
        if not row.empty:
            init = row.iloc[0].to_dict()

    with st.form("edit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        ticker  = c1.text_input("Ticker", value="" if is_new else str(pick)).strip().upper()
        name    = c2.text_input("Bolagsnamn", value=str(_nz(init.get("Bolagsnamn"), "")))
        sector  = c3.text_input("Sektor", value=str(_nz(init.get("Sektor"), "")))

        bucket  = st.selectbox("Bucket/Kategori", DEFAULT_BUCKETS,
                               index=DEFAULT_BUCKETS.index(_nz(init.get("Bucket"), DEFAULT_BUCKETS[0]))
                               if init.get("Bucket") in DEFAULT_BUCKETS else 0)
        valuta  = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"],
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
        prim    = h4.selectbox("Primär metod", METHOD_LIST,
                               index=METHOD_LIST.index(str(_nz(init.get("Primär metod"), "ev_ebitda"))) if str(_nz(init.get("Primär metod"), "ev_ebitda")) in METHOD_LIST else 0)

        i1, i2, i3 = st.columns(3)
        fetch_btn  = i1.form_submit_button("🔎 Hämta från Yahoo")
        fill_btn   = i2.form_submit_button("↩️ Fyll fält ovan från Yahoo")
        save_btn   = i3.form_submit_button("💾 Spara till Data")

    if (fetch_btn or fill_btn) and ticker:
        snap = fetch_yahoo_snapshot(ticker)
        st.info(
            f"Hämtat från Yahoo: pris={snap.get('price')} {snap.get('currency')}, "
            f"MCAP={snap.get('market_cap')}, EV/Rev={snap.get('ev_to_sales')}, "
            f"EV/EBITDA={snap.get('ev_to_ebitda')}, P/B={snap.get('p_to_book')}, "
            f"BVPS={snap.get('bvps')}, EPS_TTM={snap.get('eps_ttm')}"
        )
        if fill_btn:
            # Tips: Fyll i manuellt i UI-fälten enligt rutan ovan och klicka Spara.
            st.warning("Fyll i de fält du vill spara enligt info-rutan ovan och tryck **Spara**.")

    if save_btn and ticker:
        # Säkerställ full kolumnuppsättning
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
            "Rev CAGR": revcg,
            "EPS CAGR": epscg,
            "Årlig utdelning": dps,
            "Utdelning CAGR": dpscg,
            "Primär metod": prim,
            "Riktkurs idag": np.nan,
            "Riktkurs 1 år": np.nan,
            "Riktkurs 2 år": np.nan,
            "Riktkurs 3 år": np.nan,
            "Senast auto uppdaterad": "",
            "Auto källa": "Manuell",
        }
        if df.empty:
            df_new = pd.DataFrame([new_row], columns=DATA_COLUMNS)
        else:
            df_new = df.copy()
            # lägg ev saknade kolumner
            for c in DATA_COLUMNS:
                if c not in df_new.columns:
                    df_new[c] = np.nan
            if (df_new["Ticker"].astype(str).str.upper() == ticker.upper()).any():
                mask = (df_new["Ticker"].astype(str).str.upper() == ticker.upper())
                for k, v in new_row.items():
                    df_new.loc[mask, k] = v
            else:
                # union av kolumner
                union_cols = list(dict.fromkeys(list(df_new.columns) + list(new_row.keys())))
                row_df = pd.DataFrame([new_row])
                for c in union_cols:
                    if c not in row_df.columns:
                        row_df[c] = np.nan
                df_new = pd.concat([df_new[union_cols], row_df[union_cols]], ignore_index=True)
        write_data_df(df_new)
        st.success("Sparat till Data.")

# ============================================================
#                   SIDA: Ranking (tabell)
# ============================================================
def page_ranking():
    st.header("🏁 Ranking – Uppsida mot primär fair value (Idag)")
    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    b1, b2 = st.columns(2)
    buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_tab = b2.selectbox("Urval", ["Innehav (antal > 0)","Watchlist (antal = 0)"], index=0)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_tab.startswith("Innehav"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    else:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    rows = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            saved_primary = str(_nz(r.get("Primär metod"), ""))
            avail = met_df.loc[met_df[["Idag","1 år","2 år","3 år"]].notna().any(axis=1), "Metod"].tolist()
            if saved_primary in avail:
                chosen = saved_primary
            else:
                chosen, _, _, _, _ = choose_primary_method(met_df)
                if chosen not in avail and avail:
                    chosen = avail[0]
            prow = met_df[met_df["Metod"] == chosen].iloc[0] if chosen else None
            fair_today = _f(prow["Idag"]) if prow is not None else None
            price = meta.get("price")
            up = (fair_today/price - 1.0) * 100.0 if (_pos(fair_today) and _pos(price)) else None
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": meta.get("currency"),
                "Pris": price,
                "Primär metod": chosen,
                "Fair value (Idag)": fair_today,
                "Uppsida %": up,
            })
            time.sleep(0.15)
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None, "Primär metod": None, "Fair value (Idag)": None, "Uppsida %": None
            })
    prog.empty()

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(by=["Uppsida %"], ascending=False, na_position="last")
    st.dataframe(out, use_container_width=True)

# ============================================================
#               SIDA: Inställningar (källskatt, parametrar)
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
        def upsert(df_s, k, v):
            if (df_s["Key"] == k).any():
                df_s.loc[df_s["Key"] == k, "Value"] = str(v)
            else:
                df_s = pd.concat([df_s, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)
            return df_s
        for ccy, v in vals.items():
            s = upsert(s, f"withholding_{ccy}", v)
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    p1, p2 = st.columns(2)
    pe_w  = p1.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05, value=float(settings.get("pe_anchor_weight_ttm","0.5")))
    decay = p2.number_input("Multipel-decay per år (0..1)",  min_value=0.0, max_value=1.0, step=0.01, value=float(settings.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def setv(df_s, k, v):
            if (df_s["Key"] == k).any():
                df_s.loc[df_s["Key"] == k, "Value"] = str(v)
            else:
                df_s.loc[len(df_s)] = [k, str(v)]
        setv(s, "pe_anchor_weight_ttm", pe_w)
        setv(s, "multiple_decay", decay)
        _write_df(SETTINGS_TITLE, s)
        st.success("Parametrar uppdaterade.")

    st.subheader("Valutakurser")
    if st.button("🔄 Hämta & uppdatera FX från Yahoo"):
        mp = _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")
        st.json(mp)

# ============================================================
#                   SIDA: Batch-uppdatering
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
            # skriv över fält vi kan
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
        prog = st.progress(0.0)
        count = 0
        settings = get_settings_map()
        fx_map = get_fx_map()
        for i, (_, r) in enumerate(df.iterrows()):
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            save_quarter_snapshot(str(r["Ticker"]).strip().upper(), met_df, meta)
            count += 1
            time.sleep(throttle)
            prog.progress((i+1)/len(df))
        prog.empty()
        st.success(f"Snapshot sparade för {count} bolag.")

# ============================================================
#                           MAIN
# ============================================================
def run_main_ui():
    st.title(APP_TITLE)

    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        st.write("FX (SEK per 1):", get_fx_map())
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
