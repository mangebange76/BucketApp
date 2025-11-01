# ============================================================
# app.py — Del 1/4
# Bas & infrastruktur: imports, konstanter, hjälpare,
# Google Sheets I/O (+backoff), schema, FX & Settings
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
    """Läs från os.environ eller st.secrets (om finns)."""
    v = os.environ.get(key)
    if v is not None and str(v).strip() != "":
        return v
    try:
        val = st.secrets.get(key, None)
        if val is None:
            return default
        return val
    except Exception:
        return default

def guard(fn, label: str = ""):
    try:
        return fn()
    except Exception as e:
        st.error(f"💥 Fel {label}\n\n{e}")
        raise

def _with_backoff(callable_fn, *args, max_retries: int = 5, base_sleep: float = 0.6, **kwargs):
    """Exponential backoff för gspread-429 m.m."""
    for i in range(max_retries):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            # 429 eller temporärt fel -> backoff
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code in (429, 500, 503) or "Quota exceeded" in str(e):
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(base_sleep * (2 ** i))

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _f(x) -> Optional[float]:
    """Robust float-parser (svenska/engelska format)."""
    try:
        if x is None: return None
        if isinstance(x, str):
            s = x.strip()
            if s == "": return None
            s = s.replace(" ", "").replace("%","").replace("\u00a0","")
            # svensk decimal -> punkt
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            return float(s)
        v = float(x)
        if math.isfinite(v): return v
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

    # normalisera till dict
    if isinstance(raw, Mapping):
        try:
            creds_dict = dict(raw)  # t.ex. AttrDict -> dict
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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppnar spreadsheet via SHEET_URL eller SHEET_ID (trimmar whitespace)."""
    url = _env_or_secret("SHEET_URL")
    key = _env_or_secret("SHEET_ID")
    if url and str(url).strip():
        return _with_backoff(_gc.open_by_url, str(url).strip())
    if key and str(key).strip():
        return _with_backoff(_gc.open_by_key, str(key).strip())
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID i secrets.")

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    """Hämta eller skapa worksheet med backoff."""
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
    header = values[0] if values else []
    rows   = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=header).replace("", np.nan)
    return df

def _write_df(title: str, df: pd.DataFrame):
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    out = df.copy()
    out.columns = [str(c) for c in out.columns]
    out = out.fillna("")
    # skriv säkert
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
        # lägg till saknade kolumner, behåll befintliga värden
        for c in DATA_COLUMNS:
            if c not in df.columns:
                df[c] = np.nan
        df = df[[c for c in DATA_COLUMNS]]  # ordning
        _write_df(DATA_TITLE, df)

    # Settings – fyll defaultar (källskatt, decay, pe-ankare)
    s = _read_df(SETTINGS_TITLE)
    if s.empty:
        base = pd.DataFrame([
            ["withholding_USD","0.15"],
            ["withholding_EUR","0.15"],
            ["withholding_NOK","0.25"],
            ["withholding_CAD","0.15"],
            ["withholding_SEK","0.00"],
            ["primary_currency","SEK"],
            ["multiple_decay","0.10"],       # 10% årlig multipel-kompression
            ["pe_anchor_weight_ttm","0.50"], # 50/50 TTM vs FWD
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
                px = t.fast_info.last_price
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

def save_data_df(df: pd.DataFrame):
    # unionera kolumner med DATA_COLUMNS (bevara extra kolumner om de finns)
    col_union = list(pd.Index(DATA_COLUMNS).union(df.columns))
    df2 = df.reindex(columns=col_union)
    _write_df(DATA_TITLE, df2)

# alias för bakåtkomp
write_data_df = save_data_df

# ============================================================
# app.py — Del 2/4
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

def _mark(used: Dict[str, Dict[str, Any]], key: str, val: Any, src: str):
    used[key] = {"value": val, "source": src}

# -------------------------
# Yahoo (yfinance) – robust snapshot
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
      net_debt
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {}

    # Snabbkanal
    try:
        fi = tk.fast_info
        out["price"]      = _safe_float(getattr(fi, "last_price", None))
        out["currency"]   = getattr(fi, "currency", None)
        out["market_cap"] = _safe_float(getattr(fi, "market_cap", None))
        out["shares"]     = _safe_float(getattr(fi, "shares", None))
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

    out["price"]        = _nz(out.get("price"), _safe_float(gi("currentPrice")))
    out["currency"]     = _nz(out.get("currency"), gi("currency"))
    out["market_cap"]   = _nz(out.get("market_cap"), _safe_float(gi("marketCap")))
    out["eps_ttm"]      = _safe_float(gi("trailingEps"))
    out["pe_ttm"]       = _safe_float(gi("trailingPE"))
    out["pe_fwd"]       = _safe_float(gi("forwardPE"))
    out["revenue_ttm"]  = _safe_float(gi("totalRevenue"))
    out["ebitda_ttm"]   = _safe_float(gi("ebitda"))
    out["ev_to_sales"]  = _safe_float(gi("enterpriseToRevenue"))
    out["ev_to_ebitda"] = _safe_float(gi("enterpriseToEbitda"))
    out["p_to_book"]    = _safe_float(gi("priceToBook"))
    out["bvps"]         = _safe_float(gi("bookValue"))

    # EV och nettoskuld
    ev_info   = _safe_float(gi("enterpriseValue"))
    total_debt = _safe_float(gi("totalDebt"))
    total_cash = _safe_float(gi("totalCash"))

    if ev_info is not None:
        out["ev"] = ev_info
    elif out.get("market_cap") is not None and \
         total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
    else:
        out["ev"] = None

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]
    else:
        out["net_debt"] = None

    # Shares fallback via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
        except Exception:
            pass

    # Normalisera valuta
    out["currency"] = str(out.get("currency") or "USD").upper()
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
    Returnerar {"eps_1y": float|None, "eps_2y": float|None}
    """
    key = _get_finnhub_key()
    if not key:
        return {"eps_1y": None, "eps_2y": None}

    try:
        url = f"https://finnhub.io/api/v1/stock/estimate?symbol={ticker}&token={key}"
        r = requests.get(url, timeout=10)
        eps_1y, eps_2y = None, None
        if r.ok:
            js = r.json()
            rows = js if isinstance(js, list) else js.get("data", [])
            rows = rows or []
            rows = sorted(rows, key=lambda x: x.get("period", ""))
            if rows:
                vals = [_safe_float(x.get("epsAvg")) for x in rows if _safe_float(x.get("epsAvg")) is not None]
                if vals:
                    eps_1y = vals[-1]
                    eps_2y = vals[-2] if len(vals) > 1 else None
        return {"eps_1y": eps_1y, "eps_2y": eps_2y}
    except Exception:
        return {"eps_1y": None, "eps_2y": None}

# -------------------------
# Multipel-decay & ankare
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
# EPS/REV/EBITDA paths
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

def _eps_path(eps_ttm: Optional[float], eps_1y: Optional[float], eps_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returnerar (eps_0, eps_1, eps_2, eps_3). Om eps_1y saknas men cagr finns: extrapolera från ttm.
    """
    e0 = _pos(eps_ttm)
    e1 = _pos(eps_1y)
    cg = _f(eps_cagr)
    if e1 is None and e0 is not None and cg is not None:
        e1 = e0 * (1.0 + cg)
    e2 = e1 * (1.0 + cg) if (e1 is not None and cg is not None) else None
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
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """
    Beräknar metodtabell (Idag, 1,2,3 år) för raden.
    Returnerar (methods_df, sanity_text, meta, used_fields)
    """
    used: Dict[str, Dict[str, Any]] = {}
    ticker = str(row.get("Ticker", "")).strip()

    # 1) Live-data
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.35)  # mild throttling
    est  = fetch_finnhub_estimates(ticker)

    # 2) Inputs (med fallback från Data-bladet) + markera källa
    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs"))); _mark(used, "price", price, "yahoo" if snap.get("price") else "sheet")
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper(); _mark(used, "currency", currency, "yahoo" if snap.get("currency") else "sheet")
    mcap     = _pos(snap.get("market_cap")); _mark(used, "market_cap", mcap, "yahoo")
    ev_now   = _pos(_nz(snap.get("ev"), None)); _mark(used, "ev", ev_now, "yahoo" if snap.get("ev") is not None else "derived/sheet")
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier"))); _mark(used, "shares", shares, "yahoo" if snap.get("shares") else "sheet")
    net_debt = _nz(snap.get("net_debt"), row.get("Net debt")); _mark(used, "net_debt", net_debt, "yahoo" if snap.get("net_debt") is not None else "sheet")

    rev_ttm    = _pos(_nz(snap.get("revenue_ttm"), row.get("Rev TTM"))); _mark(used, "rev_ttm", rev_ttm, "yahoo" if snap.get("revenue_ttm") else "sheet")
    ebitda_ttm = _pos(_nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM"))); _mark(used, "ebitda_ttm", ebitda_ttm, "yahoo" if snap.get("ebitda_ttm") else "sheet")
    eps_ttm    = _pos(_nz(snap.get("eps_ttm"), row.get("EPS TTM"))); _mark(used, "eps_ttm", eps_ttm, "yahoo" if snap.get("eps_ttm") else "sheet")
    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM"))); _mark(used, "pe_ttm", pe_ttm, "yahoo" if snap.get("pe_ttm") else "sheet")
    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD"))); _mark(used, "pe_fwd", pe_fwd, "yahoo" if snap.get("pe_fwd") else "sheet")
    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue"))); _mark(used, "ev_sales", ev_sales, "yahoo" if snap.get("ev_to_sales") else "sheet")
    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA"))); _mark(used, "ev_ebitda", ev_ebitda, "yahoo" if snap.get("ev_to_ebitda") else "sheet")
    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B"))); _mark(used, "p_b", p_b, "yahoo" if snap.get("p_to_book") else "sheet")
    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS"))); _mark(used, "bvps", bvps, "yahoo" if snap.get("bvps") else "sheet")

    # Estimat / tillväxt (estimat prioriteras, annars fall-back till CAGR)
    eps_1y_est = _pos(_nz(est.get("eps_1y"), row.get("EPS 1Y"))); _mark(used, "eps_1y", eps_1y_est, "finnhub" if est.get("eps_1y") is not None else ("sheet" if _nz(row.get("EPS 1Y")) is not None else "—"))
    eps_cagr   = _f(row.get("EPS CAGR")); _mark(used, "eps_cagr", eps_cagr, "sheet")
    rev_cagr   = _f(row.get("Rev CAGR")); _mark(used, "rev_cagr", rev_cagr, "sheet")

    # 3) Härled EPS om saknas men PE+price finns
    eps_ttm, src_eps_ttm, eps_1y_est, src_eps_1y = _derive_eps_from_pe_if_missing(
        price, pe_ttm, pe_fwd, eps_ttm, eps_1y_est
    )
    if src_eps_ttm.startswith("derived"): _mark(used, "eps_ttm", eps_ttm, src_eps_ttm)
    if src_eps_1y.startswith("derived"):  _mark(used, "eps_1y",  eps_1y_est, src_eps_1y)

    # 4) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 5) Paths
    e0, e1, e2, e3 = _eps_path(eps_ttm, eps_1y_est, eps_cagr)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(ebitda_ttm, r0, r1, r2, r3)

    # Multiplar med decay
    pe0 = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b, _decay_multiple(p_b, 1, decay), _decay_multiple(p_b, 2, decay), _decay_multiple(p_b, 3, decay)

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

    # 7) Sanity + meta
    sanity = (
        f"price={'ok' if price else '—'}, eps_ttm={'ok' if e0 else '—'} ({src_eps_ttm}), "
        f"eps_1y={'ok' if e1 else '—'} ({src_eps_1y}), rev_ttm={'ok' if r0 else '—'}, "
        f"ebitda_ttm={'ok' if b0 else '—'}, shares={'ok' if shares else '—'}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
    }
    return methods_df, sanity, meta, used

# ============================================================
# app.py — Del 3/4
# UI per bolag: presentation, val av värderingssätt, spar-funktioner
# ============================================================

# ---------- Små formatterare ----------
def _fmt_num(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{float(v):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(v)

def _fmt_money(v: Optional[float], ccy: str) -> str:
    s = _fmt_num(v)
    return f"{s} {ccy}"

def _fmt_sek0(v: Optional[float]) -> str:
    if v is None:
        return "0 SEK"
    try:
        return f"{float(v):,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

# ---------- Resultat-skrivning ----------
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
            # säkerställ kolumner
            for k in new_row.keys():
                if k not in res_df.columns:
                    res_df[k] = np.nan
            for k, v in new_row.items():
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

# ---------- Uppdatera estimat/CAGR i Data ----------
def _update_estimates_for_ticker(
    ticker: str,
    eps1y: Optional[float],
    eps_cagr: Optional[float],
    rev_cagr: Optional[float],
) -> None:
    df = read_data_df()
    if df.empty or "Ticker" not in df.columns:
        st.warning("Hittade inte Data-bladet eller kolumnen 'Ticker'.")
        return
    mask = df["Ticker"].astype(str) == str(ticker)
    if not mask.any():
        st.warning(f"Hittade inte {ticker} i Data-bladet.")
        return
    for col in ["EPS 1Y", "EPS CAGR", "Rev CAGR"]:
        if col not in df.columns:
            df[col] = np.nan
    if eps1y is not None:     df.loc[mask, "EPS 1Y"]   = float(eps1y)
    if eps_cagr is not None:  df.loc[mask, "EPS CAGR"] = float(eps_cagr)
    if rev_cagr is not None:  df.loc[mask, "Rev CAGR"] = float(rev_cagr)
    write_data_df(df)

# ---------- Heuristik/val av primär metod ----------
_METHOD_ORDER = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]
_METHOD_LABEL = {
    "pe_hist_vs_eps": "P/E × EPS (ankare/decay)",
    "ev_sales":       "EV/Sales → aktiepris",
    "ev_ebitda":      "EV/EBITDA → aktiepris",
    "ev_dacf":        "EV/DACF (proxy EBITDA)",
    "p_b":            "P/B × BVPS",
    "p_nav":          "P/NAV",
    "p_tbv":          "P/TBV",
    "p_affo":         "P/AFFO",
    "p_fcf":          "P/FCF",
    "ev_fcf":         "EV/FCF",
    "p_nii":          "P/NII",
}

def _pick_primary_targets(met_df: pd.DataFrame, override: Optional[str] = None) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if met_df is None or met_df.empty:
        return None, None, None, None, None
    if override and (met_df["Metod"] == override).any():
        row = met_df[met_df["Metod"] == override].iloc[0]
        return override, _f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"])
    # auto: flest icke-NaN → metodprioritet
    counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None, None, None, None, None
    maxc = counts.max()
    candidates = list(counts[counts == maxc].index)
    chosen = None
    for p in _METHOD_ORDER:
        if p in candidates:
            chosen = p
            break
    if chosen is None:
        chosen = candidates[0]
    row = met_df[met_df["Metod"] == chosen].iloc[0]
    return chosen, _f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"])

# ---------- Presentation: “använt fält & källa” ----------
def _used_fields_table(used: Dict[str, Dict[str, Any]], currency: str) -> pd.DataFrame:
    pretty = []
    label_map = {
        "price": "Pris",
        "currency": "Valuta",
        "market_cap": "Market Cap",
        "ev": "Enterprise Value",
        "shares": "Utest. aktier",
        "net_debt": "Net debt",
        "rev_ttm": "Omsättning TTM",
        "ebitda_ttm": "EBITDA TTM",
        "eps_ttm": "EPS TTM",
        "pe_ttm": "P/E TTM",
        "pe_fwd": "P/E FWD",
        "ev_sales": "EV/Sales",
        "ev_ebitda": "EV/EBITDA",
        "p_b": "P/B",
        "bvps": "BVPS",
        "eps_1y": "EPS 1Y (estimat)",
        "eps_cagr": "EPS CAGR",
        "rev_cagr": "Rev CAGR",
    }
    money_keys = {"price","rev_ttm","ebitda_ttm","net_debt"}
    for k, d in used.items():
        val = d.get("value")
        src = d.get("source")
        if k in money_keys:
            v_str = _fmt_money(val, currency) if k == "price" else _fmt_num(val)
        elif k == "currency":
            v_str = str(val or "")
        else:
            v_str = _fmt_num(val)
        pretty.append({"Fält": label_map.get(k, k), "Värde": v_str, "Källa": src})
    df = pd.DataFrame(pretty)
    if not df.empty:
        order = ["Fält","Värde","Källa"]
        df = df[order]
    return df

# ---------- UI för ett bolag ----------
def render_company_view(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    tkr = str(row.get("Ticker", "")).strip().upper()
    name = str(_nz(row.get("Bolagsnamn"), tkr))
    bucket = str(_nz(row.get("Bucket"), "")).strip()

    st.markdown(f"### {tkr} • {name} {'• ' + bucket if bucket else ''}")

    # Kör motor
    methods_df, sanity, meta, used = compute_methods_for_row(row, settings, fx_map)
    currency = meta.get("currency") or str(_nz(row.get("Valuta"), "USD")).upper()
    price_now = meta.get("price")
    fx = fx_map.get(currency, 1.0) or 1.0
    shares_owned = _f(row.get("Antal aktier")) or 0.0

    # Val av värderingssätt (Auto eller specifik)
    available_methods = methods_df["Metod"].tolist() if not methods_df.empty else []
    pretty_options = ["Auto"] + [f"{_METHOD_LABEL.get(m, m)} · [{m}]" for m in available_methods]
    default_idx = 0
    # Om användaren har valt primär metod i Data -> förslå den
    prim_pref = str(_nz(row.get("Primär metod"), "")).strip()
    if prim_pref and prim_pref in available_methods:
        try:
            default_idx = 1 + available_methods.index(prim_pref)
        except Exception:
            default_idx = 0
    sel = st.selectbox("Värderingssätt i vyn", options=pretty_options, index=default_idx, key=f"method_sel_{tkr}")
    override = None
    if sel != "Auto":
        # plocka ut metod-koden ur texten "Label · [metod]"
        try:
            override = sel.split("[")[-1].split("]")[0]
        except Exception:
            override = None

    # Primär riktkurs (enligt vald metod eller auto)
    primary, p0, p1, p2, p3 = _pick_primary_targets(methods_df, override=override)

    # Header – sanity + snabbvy
    cA, cB = st.columns([0.55, 0.45])
    with cA:
        st.caption(f"Sanity: {sanity}")
        st.dataframe(methods_df, use_container_width=True)
    with cB:
        st.markdown("**Körningens inputs (värde & källa)**")
        used_df = _used_fields_table(used, currency)
        st.dataframe(used_df, use_container_width=True, height=360)

    # Primär riktkurs – kort
    st.markdown("#### 🎯 Riktkurser (valda)")
    cols = st.columns(4)
    cols[0].metric("Idag",  _fmt_money(p0, currency))
    cols[1].metric("1 år",  _fmt_money(p1, currency))
    cols[2].metric("2 år",  _fmt_money(p2, currency))
    cols[3].metric("3 år",  _fmt_money(p3, currency))
    st.caption(f"Metod: **{_METHOD_LABEL.get(primary, primary) if primary else '—'}** • Valuta: **{currency}** • PE-ankare vikt (TTM): {int(float(settings.get('pe_anchor_weight_ttm','0.5'))*100)}% • Decay: {settings.get('multiple_decay','0.10')}")

    # Uppsida vs aktuell kurs
    if _pos(price_now):
        up_cols = st.columns(4)
        for i, (lbl, tgt) in enumerate([("Idag", p0), ("1 år", p1), ("2 år", p2), ("3 år", p3)]):
            if _pos(tgt):
                delta_pct = (tgt/price_now - 1.0) * 100.0
                up_cols[i].metric(f"Uppsida {lbl}", f"{delta_pct:,.1f}%".replace(",", " "), delta=None)

    # Utdelning (netto SEK) kommande 1–3 år
    dps_now  = _f(row.get("Årlig utdelning"))
    dps_cagr = _f(row.get("Utdelning CAGR"))
    divs = forecast_dividends_net_sek(currency, shares_owned, dps_now, dps_cagr, fx_map, settings)
    with st.expander("💰 Utdelningsprognos (netto i SEK)", expanded=False):
        st.write(f"• **1 år:** {_fmt_sek0(divs['y1'])} • **2 år:** {_fmt_sek0(divs['y2'])} • **3 år:** {_fmt_sek0(divs['y3'])}")
        st.caption(f"Källskatt {currency}: {int(get_withholding_for(currency, settings)*100)}% • Antal aktier: {int(shares_owned)}")

    # Innehavsvärde nu (SEK)
    port_sek = (price_now or 0.0) * shares_owned * fx
    with st.expander("🧾 Innehavsvärde", expanded=False):
        st.write(f"Totalt värde nu: **{_fmt_sek0(port_sek)}** (pris {price_now if price_now else '—'} {currency}, FX {fx:.3f})")

    # Åtgärder
    st.divider()
    c1, c2, c3 = st.columns(3)
    if c1.button("🔄 Uppdatera Estimat/CAGR", key=f"upd_est_{tkr}"):
        snap = fetch_yahoo_snapshot(tkr)
        est  = fetch_finnhub_estimates(tkr)
        eps_ttm = _pos(_nz(snap.get("eps_ttm"), row.get("EPS TTM")))
        eps1    = _pos(est.get("eps_1y"))
        eps_cagr = _f(row.get("EPS CAGR"))
        if eps_cagr is None and _pos(eps_ttm) and _pos(eps1):
            try:
                eps_cagr = (eps1/eps_ttm) - 1.0
            except Exception:
                eps_cagr = None
        _update_estimates_for_ticker(tkr, eps1, eps_cagr, _f(row.get("Rev CAGR")))
        st.success("Estimat/CAGR uppdaterade i Data-bladet.")

    if c2.button("💾 Spara valda riktkurser → Resultat", key=f"save_res_{tkr}"):
        _append_or_update_result(tkr, currency, primary, p0, p1, p2, p3)
        st.success("Primär riktkurs sparad till fliken Resultat.")

    if c3.button("📷 Spara kvartalssnapshot", key=f"snap_{tkr}"):
        try:
            save_quarter_snapshot(tkr, methods_df, {"currency": currency, "pe_anchor": meta.get("pe_anchor"), "decay": meta.get("decay")})
            st.success("Snapshot sparad till fliken Snapshot.")
        except NameError:
            st.warning("Snapshot-funktionen definieras i Del 4/4. Spara igen när Del 4 är inklistrad.")

# ============================================================
# app.py — Del 4/4
# Sidor + snapshot + main()
# ============================================================

# -------------------------
# Snapshot till fliken "Snapshot"
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

    df  = read_data_df()
    fxm = get_fx_map()

    tickers = ["— nytt —"] + (sorted(df["Ticker"].dropna().astype(str).unique().tolist()) if not df.empty else [])
    tkr_sel = st.selectbox("Välj ticker", tickers, index=0)
    is_new  = (tkr_sel == "— nytt —")

    # Förifyll om befintlig
    init = {}
    if not is_new and not df.empty:
        row = df[df["Ticker"].astype(str) == tkr_sel].iloc[0].to_dict()
        init = {k: row.get(k) for k in DATA_COLUMNS}

    with st.form("edit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        ticker  = c1.text_input("Ticker", value="" if is_new else tkr_sel).strip().upper()
        name    = c2.text_input("Bolagsnamn", value=str(_nz(init.get("Bolagsnamn"), "")))
        sector  = c3.text_input("Sektor", value=str(_nz(init.get("Sektor"), "")))

        bucket  = st.selectbox("Bucket/Kategori", DEFAULT_BUCKETS, index=DEFAULT_BUCKETS.index(_nz(init.get("Bucket"), DEFAULT_BUCKETS[0])) if init.get("Bucket") in DEFAULT_BUCKETS else 0)
        valuta  = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"], index=["USD","EUR","NOK","CAD","SEK"].index(str(_nz(init.get("Valuta"), "USD")).upper()))

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
        prim    = h4.selectbox(
            "Primär metod (förslag/standard)",
            _METHOD_ORDER,
            index=_METHOD_ORDER.index(str(_nz(init.get("Primär metod"), "ev_ebitda"))) if str(_nz(init.get("Primär metod"), "")) in _METHOD_ORDER else 0
        )

        i1, i2, i3 = st.columns(3)
        fetch_btn  = i1.form_submit_button("🔎 Hämta från Yahoo nu")
        fill_btn   = i2.form_submit_button("⬇️ Autofylla fält från senaste Yahoo")
        save_btn   = i3.form_submit_button("💾 Spara till Data")

    # Hämta från Yahoo – visa i logg
    if fetch_btn and ticker:
        snap = fetch_yahoo_snapshot(ticker)
        st.info(
            f"Hämtat {ticker}: pris={snap.get('price')} {snap.get('currency')}, "
            f"MCAP={snap.get('market_cap')}, EV/Rev={snap.get('ev_to_sales')}, "
            f"EV/EBITDA={snap.get('ev_to_ebitda')}, P/B={snap.get('p_to_book')}, "
            f"BVPS={snap.get('bvps')}, EPS TTM={snap.get('eps_ttm')}"
        )
        st.caption("Använd **Autofylla** för att lägga in dessa i formuläret (eller fyll manuellt).")

        # Spara i session för autofyll
        st.session_state["last_snap"] = {ticker: snap}

    # Autofylla formulärfält (så gott det går – man kan behöva mata om i UI efteråt)
    if fill_btn and ticker and "last_snap" in st.session_state:
        snap = st.session_state["last_snap"].get(ticker, {})
        if snap:
            st.success("Autofylla: använd värdena ovan som referens och fyll in i fälten, tryck sedan Spara.")
        else:
            st.warning("Ingen cachead hämtning hittades. Kör 'Hämta från Yahoo nu' först.")

    # Spara till Google Sheets
    if save_btn and ticker:
        df_old = read_data_df()
        ts = now_stamp()
        new_row = {
            "Timestamp": ts,
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
            "Senast auto uppdaterad": "",
            "Auto källa": "Manuell",
        }

        # Union av kolumner (för att undvika KeyError vid concat)
        cols = list(DATA_COLUMNS)
        for k in new_row.keys():
            if k not in cols:
                cols.append(k)

        if df_old.empty:
            df_new = pd.DataFrame([new_row], columns=cols)
        else:
            df_new = df_old.copy()
            for k in cols:
                if k not in df_new.columns:
                    df_new[k] = np.nan

            mask = (df_new["Ticker"].astype(str) == ticker)
            if mask.any():
                for k, v in new_row.items():
                    df_new.loc[mask, k] = v
            else:
                # append ny rad men behåll kolumnordning
                add = pd.DataFrame([new_row])
                for k in add.columns:
                    if k not in df_new.columns:
                        df_new[k] = np.nan
                add = add[df_new.columns]
                df_new = pd.concat([df_new, add], ignore_index=True)

        write_data_df(df_new)
        st.success("Sparat till Data.")

# ============================================================
#                   SIDA: Analys (per bolag)
# ============================================================
def page_analysis():
    st.header("🔬 Analys")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()

    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    c1, c2, c3 = st.columns(3)
    bucket = c1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_only = c2.selectbox("Urval", ["Alla", "Endast innehav (>0)", "Endast watchlist (=0)"], index=0)
    tkr_pick = c3.selectbox("Välj ett specifikt bolag (valfritt)", ["—"] + df["Ticker"].astype(str).tolist())

    q = df.copy()
    if bucket:
        q = q[q["Bucket"].isin(bucket)]
    if owned_only == "Endast innehav (>0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    elif owned_only == "Endast watchlist (=0)":
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]
    if tkr_pick != "—":
        q = q[q["Ticker"].astype(str) == tkr_pick]

    # Portföljsumma (SEK) för urval
    total_sek = 0.0
    for _, r in q.iterrows():
        try:
            currency = str(_nz(r.get("Valuta"), "USD")).upper()
            fx = fx_map.get(currency, 1.0) or 1.0
            total_sek += float(_nz(_f(r.get("Aktuell kurs")), 0.0)) * float(_nz(_f(r.get("Antal aktier")),0.0)) * float(fx)
        except Exception:
            pass
    st.caption(f"Totalt värde för urvalet: **{total_sek:,.0f} SEK**".replace(",", " ").replace(".", ","))

    for _, row in q.iterrows():
        with st.container(border=True):
            render_company_view(row, settings, fx_map)
            st.markdown("---")

# ============================================================
#                   SIDA: Ranking (Uppsida)
# ============================================================
def _pick_primary_today(met_df: pd.DataFrame, preferred: Optional[str]) -> Tuple[Optional[str], Optional[float]]:
    """Returnera (metod, target_today) för vald/heuristisk primärmetod."""
    if met_df is None or met_df.empty:
        return None, None
    if preferred and (met_df["Metod"] == preferred).any():
        row = met_df[met_df["Metod"] == preferred].iloc[0]
        return preferred, _f(row["Idag"])
    counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None, None
    maxc = counts.max()
    candidates = list(counts[counts == maxc].index)
    chosen = None
    for p in _METHOD_ORDER:
        if p in candidates:
            chosen = p
            break
    if chosen is None:
        chosen = candidates[0]
    row = met_df[met_df["Metod"] == chosen].iloc[0]
    return chosen, _f(row["Idag"])

def page_ranking():
    st.header("🏁 Ranking – Uppsida mot vald/auto fair value (Idag)")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    b1, b2, b3 = st.columns(3)
    buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_tab = b2.selectbox("Urval", ["Innehav (antal > 0)","Watchlist (antal = 0)","Alla"], index=0)
    method_override = b3.selectbox("Tvinga metod för ranking (valfritt)", ["Auto"] + _METHOD_ORDER, index=0)

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_tab.startswith("Innehav"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    elif owned_tab.startswith("Watchlist"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    rows = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, sanity, meta, _used = compute_methods_for_row(r, settings, fx_map)
            preferred = None
            if method_override != "Auto":
                preferred = method_override
            elif str(_nz(r.get("Primär metod"), "")) in _METHOD_ORDER:
                preferred = str(r.get("Primär metod"))
            method, fair_today = _pick_primary_today(met_df, preferred)
            price = meta.get("price")
            currency = meta.get("currency") or str(_nz(r.get("Valuta"), "USD")).upper()
            upside = None
            if _pos(price) and _pos(fair_today):
                upside = (fair_today/price - 1.0) * 100.0
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": currency,
                "Pris": price,
                "Primär metod": method,
                "Fair value (Idag)": fair_today,
                "Uppsida %": upside,
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
        prog.progress((i+1)/max(1,len(q)))
    prog.empty()

    out = pd.DataFrame(rows)
    if not out.empty and "Uppsida %" in out.columns:
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
        def upsert(k, v):
            nonlocal s
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s = pd.concat([s, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)
        for ccy, v in vals.items():
            upsert(f"withholding_{ccy}", v)
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    p1, p2 = st.columns(2)
    pe_w  = p1.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05, value=float(settings.get("pe_anchor_weight_ttm","0.5")))
    decay = p2.number_input("Multipel-decay per år (0..1)", min_value=0.0, max_value=1.0, step=0.01, value=float(settings.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def setv(k, v):
            nonlocal s
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s.loc[len(s)] = [k, str(v)]
        setv("pe_anchor_weight_ttm", pe_w)
        setv("multiple_decay", decay)
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
    settings = get_settings_map()
    fx_map   = get_fx_map()
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
        for i, (_, r) in enumerate(df.iterrows()):
            met_df, _, meta, _used = compute_methods_for_row(r, settings, fx_map)
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

    # Snabbstatus – FX och inställningar
    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        fx_map = get_fx_map()
        st.write("FX (SEK per 1):", fx_map)
        settings = get_settings_map()
        st.write("Settings:", settings)

    # Navigering
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
