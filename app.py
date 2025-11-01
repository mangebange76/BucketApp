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

# ---------------- UI ----------------
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
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _pos(x) -> Optional[float]:
    v = _f(x)
    return v if (v is not None and v > 0) else None

# -------- Backoff för gspread (429, quota) --------
def _with_backoff(callable_fn, *args, **kwargs):
    delay = 0.7
    for attempt in range(8):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            msg = (str(e) or "").lower()
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (429, 500, 503) or "quota" in msg or "rate limit" in msg:
                time.sleep(delay + random.random()*0.4)
                delay = min(delay * 1.8, 10.0)
                continue
            raise
        except Exception:
            raise
    return callable_fn(*args, **kwargs)

@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    raw = _env_or_secret("GOOGLE_CREDENTIALS")
    if raw is None:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i secrets/env.")
    if isinstance(raw, Mapping):
        creds_dict = dict(raw)
    elif isinstance(raw, (bytes, bytearray)):
        creds_dict = json.loads(raw.decode("utf-8"))
    elif isinstance(raw, str):
        creds_dict = json.loads(raw)
    else:
        creds_dict = json.loads(json.dumps(raw))
    creds_dict = _normalize_private_key(creds_dict)
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    sheet_url = _env_or_secret("SHEET_URL")
    sheet_id  = _env_or_secret("SHEET_ID")
    if sheet_url:
        return _with_backoff(_gc.open_by_url, sheet_url.strip())
    if sheet_id:
        return _with_backoff(_gc.open_by_key, sheet_id.strip())
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID i secrets.")

# Håll Worksheet-objekt i session för färre metadata-anrop (minskar 429-risk)
if "_ws_cache" not in st.session_state:
    st.session_state["_ws_cache"] = {}

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    cache = st.session_state["_ws_cache"]
    if title in cache:
        try:
            return cache[title]
        except Exception:
            cache.pop(title, None)
    try:
        ws = _with_backoff(spread.worksheet, title)
    except WorksheetNotFound:
        ws = _with_backoff(spread.add_worksheet, title=title, rows=2000, cols=200)
    cache[title] = ws
    return ws

@st.cache_data(ttl=180, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
    gc = _build_gspread_client(); sh = _open_spreadsheet(gc); ws = _get_ws(sh, title)
    values = _with_backoff(ws.get_all_values)
    if not values: return pd.DataFrame()
    header, rows = values[0], values[1:]
    return pd.DataFrame(rows, columns=header).replace("", np.nan)

def _invalidate_caches():
    try: _read_df.clear()
    except Exception: pass

def _write_df(title: str, df: pd.DataFrame):
    gc = _build_gspread_client(); sh = _open_spreadsheet(gc); ws = _get_ws(sh, title)
    out = df.copy(); out.columns = [str(c) for c in out.columns]; out = out.fillna("")
    _with_backoff(ws.clear)
    if out.shape[0] == 0:
        _with_backoff(ws.update, [list(out.columns)])
    else:
        _with_backoff(ws.update, [list(out.columns)] + out.astype(str).values.tolist())
    _invalidate_caches()

def _append_rows(title: str, rows: List[List[Any]]):
    gc = _build_gspread_client(); sh = _open_spreadsheet(gc); ws = _get_ws(sh, title)
    _with_backoff(ws.append_rows, rows, value_input_option="RAW")
    _invalidate_caches()

# ---------- Schema ----------
DATA_COLUMNS = [
    "Timestamp","Ticker","Bolagsnamn","Sektor","Bucket","Valuta",
    "Antal aktier","GAV (SEK)","Aktuell kurs",
    "Utestående aktier","Net debt",
    "Rev TTM","EBITDA TTM","EPS TTM",
    "PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
    "EPS 1Y","Rev CAGR","EPS CAGR",
    "Årlig utdelning","Utdelning CAGR",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    "Primär metod","Senast auto uppdaterad","Auto källa"
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
            if c not in df.columns: df[c] = np.nan
        _write_df(DATA_TITLE, df[[c for c in DATA_COLUMNS]])

    # Settings
    s = _read_df(SETTINGS_TITLE)
    if s.empty:
        base = pd.DataFrame([
            ["withholding_USD","0.15"],["withholding_NOK","0.25"],
            ["withholding_CAD","0.15"],["withholding_EUR","0.15"],["withholding_SEK","0.00"],
            ["primary_currency","SEK"],["multiple_decay","0.10"],["pe_anchor_weight_ttm","0.50"],
        ], columns=SETTINGS_COLUMNS)
        _write_df(SETTINGS_TITLE, base)
    else:
        for c in SETTINGS_COLUMNS:
            if c not in s.columns: s[c] = np.nan
        _write_df(SETTINGS_TITLE, s[SETTINGS_COLUMNS])

    # FX
    fx = _read_df(FX_TITLE)
    if fx.empty:
        base_fx = pd.DataFrame([["SEK",1.0],["USD",np.nan],["EUR",np.nan],["NOK",np.nan],["CAD",np.nan]], columns=FX_COLUMNS)
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
# FX — hämta via yfinance
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
            px = getattr(t.fast_info, "last_price", None)
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
# Settings – karta & källskatt
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

# =========================
# Yahoo (yfinance) – robust snapshot
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Returnerar dict:
      price, currency, market_cap, ev, shares,
      eps_ttm, pe_ttm, pe_fwd,
      revenue_ttm, ebitda_ttm,
      ev_to_sales, ev_to_ebitda, p_to_book, bvps,
      net_debt
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {}

    # Fast info
    try:
        fi = tk.fast_info
        out["price"]      = _f(getattr(fi, "last_price", None))
        out["currency"]   = getattr(fi, "currency", None)
        out["market_cap"] = _f(getattr(fi, "market_cap", None))
        out["shares"]     = _f(getattr(fi, "shares", None))
    except Exception:
        pass

    # Info fallback
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k): 
        try: return info.get(k)
        except Exception: return None

    out["price"]        = out.get("price") or _f(gi("currentPrice"))
    out["currency"]     = out.get("currency") or gi("currency")
    out["market_cap"]   = out.get("market_cap") or _f(gi("marketCap"))
    out["eps_ttm"]      = _f(gi("trailingEps"))
    out["pe_ttm"]       = _f(gi("trailingPE"))
    out["pe_fwd"]       = _f(gi("forwardPE"))
    out["revenue_ttm"]  = _f(gi("totalRevenue"))
    out["ebitda_ttm"]   = _f(gi("ebitda"))
    out["ev_to_sales"]  = _f(gi("enterpriseToRevenue"))
    out["ev_to_ebitda"] = _f(gi("enterpriseToEbitda"))
    out["p_to_book"]    = _f(gi("priceToBook"))
    out["bvps"]         = _f(gi("bookValue"))

    ev_info   = _f(gi("enterpriseValue"))
    total_debt= _f(gi("totalDebt"))
    total_cash= _f(gi("totalCash"))

    if ev_info is not None:
        out["ev"] = ev_info
    elif out.get("market_cap") is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
    else:
        out["ev"] = None

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]
    else:
        out["net_debt"] = None

    # Shares via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
        except Exception:
            pass

    out["currency"] = str(out.get("currency") or "USD").upper()
    return out

# =========================
# Finnhub (valfritt) – EPS-estimat
# =========================
def _get_finnhub_key() -> Optional[str]:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

@st.cache_data(ttl=600, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> Dict[str, Optional[float]]:
    key = _get_finnhub_key()
    if not key:
        return {"eps_1y": None, "eps_2y": None}
    import requests
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
                vals = [_f(x.get("epsAvg")) for x in rows if _f(x.get("epsAvg")) is not None]
                if vals:
                    eps_1y = vals[-1]
                    eps_2y = vals[-2] if len(vals) > 1 else None
        return {"eps_1y": eps_1y, "eps_2y": eps_2y}
    except Exception:
        return {"eps_1y": None, "eps_2y": None}

# =========================
# Multiplar, vägar & prisformler
# =========================
def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    m0 = _pos(mult0)
    if m0 is None: return None
    m = m0 * (1.0 - decay * years)
    return max(m, m0 * floor_frac)

def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float) -> Optional[float]:
    pt = _pos(pe_ttm); pf = _pos(pe_fwd)
    if pt is None and pf is None: return None
    if pt is None: return pf
    if pf is None: return pt
    return w_ttm * pt + (1.0 - w_ttm) * pf

def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float], shares_fd: Optional[float]) -> Optional[float]:
    e = _pos(ev_target); s = _pos(shares_fd)
    if e is None or s is None: return None
    nd = _f(net_debt) or 0.0
    return max(0.0, (e - nd) / s)

def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e = _pos(eps); p = _pos(pe)
    if e is None or p is None: return None
    return e * p

def _ev_from_sales(rev: Optional[float], mult: Optional[float]) -> Optional[float]:
    r = _pos(rev); m = _pos(mult)
    if r is None or m is None: return None
    return r * m

def _ev_from_ebitda(ebitda: Optional[float], mult: Optional[float]) -> Optional[float]:
    e = _pos(ebitda); m = _pos(mult)
    if e is None or m is None: return None
    return e * m

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb); b = _pos(bvps)
    if p is None or b is None: return None
    return p * b

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

def _eps_path(eps_ttm: Optional[float], eps_1y: Optional[float], eps_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(eps_ttm); e1 = _pos(eps_1y); cg = _f(eps_cagr)
    if e1 is None and e0 is not None and cg is not None:
        e1 = e0 * (1.0 + cg)
    e2 = e1 * (1.0 + cg) if (e1 is not None and cg is not None) else None
    e3 = e2 * (1.0 + cg) if (e2 is not None and cg is not None) else None
    return e0, e1, e2, e3

def _rev_path(rev_ttm: Optional[float], rev_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    r0 = _pos(rev_ttm); cg = _f(rev_cagr)
    if r0 is None or cg is None:
        return r0, None, None, None
    r1 = r0 * (1.0 + cg); r2 = r1 * (1.0 + cg); r3 = r2 * (1.0 + cg)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _pos(ebitda_ttm)
    if e0 is None: return None, None, None, None
    if rev0 is None or rev1 is None: 
        return e0, e0, e0, e0
    def scale(r): return (e0 * (r / rev0)) if (r and rev0) else e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# =========================
# Utdelning (netto SEK)
# =========================
def forecast_dividends_net_sek(currency: str, shares: Optional[float], current_dps: Optional[float],
                               dps_cagr: Optional[float], fx_map: Dict[str, float], settings: Dict[str, str]) -> Dict[str, Optional[float]]:
    if not _pos(shares) or current_dps is None:
        return {"y1": 0.0, "y2": 0.0, "y3": 0.0}
    g  = _f(dps_cagr) or 0.0
    wh = get_withholding_for(currency, settings)
    fx = fx_map.get((currency or "USD").upper(), 1.0) or 1.0
    def net(years: int) -> float:
        gross = float(current_dps) * ((1.0 + g) ** years) * float(shares)
        return gross * (1.0 - wh) * float(fx)
    return {"y1": net(1), "y2": net(2), "y3": net(3)}

# =========================
# Motor: metodtabell per rad
# =========================
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    ticker = str(row.get("Ticker", "")).strip().upper()

    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.25)  # mild throttling
    est  = fetch_finnhub_estimates(ticker)

    price    = _pos(snap.get("price") or row.get("Aktuell kurs"))
    currency = str((snap.get("currency") or row.get("Valuta") or "USD")).upper()
    shares   = _pos(snap.get("shares") or row.get("Utestående aktier"))
    net_debt = _f(snap.get("net_debt") if snap.get("net_debt") is not None else row.get("Net debt"))

    rev_ttm    = _pos(snap.get("revenue_ttm") or row.get("Rev TTM"))
    ebitda_ttm = _pos(snap.get("ebitda_ttm") or row.get("EBITDA TTM"))
    eps_ttm    = _pos(snap.get("eps_ttm") or row.get("EPS TTM"))
    pe_ttm     = _pos(snap.get("pe_ttm") or row.get("PE TTM"))
    pe_fwd     = _pos(snap.get("pe_fwd") or row.get("PE FWD"))
    ev_sales   = _pos(snap.get("ev_to_sales") or row.get("EV/Revenue"))
    ev_ebitda  = _pos(snap.get("ev_to_ebitda") or row.get("EV/EBITDA"))
    p_b        = _pos(snap.get("p_to_book") or row.get("P/B"))
    bvps       = _pos(snap.get("bvps") or row.get("BVPS"))

    eps_1y_est = _pos(est.get("eps_1y") or row.get("EPS 1Y"))
    eps_cagr   = _f(row.get("EPS CAGR"))
    rev_cagr   = _f(row.get("Rev CAGR"))

    eps_ttm, src_eps_ttm, eps_1y_est, src_eps_1y = _derive_eps_from_pe_if_missing(price, pe_ttm, pe_fwd, eps_ttm, eps_1y_est)

    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    e0, e1, e2, e3 = _eps_path(eps_ttm, eps_1y_est, eps_cagr)
    r0, r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(ebitda_ttm, r0, r1, r2, r3)

    pe0 = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0, pb1, pb2, pb3     = p_b, _decay_multiple(p_b, 1, decay), _decay_multiple(p_b, 2, decay), _decay_multiple(p_b, 3, decay)

    methods = []
    methods.append({"Metod":"pe_hist_vs_eps","Idag":_price_from_pe(e0,pe0),"1 år":_price_from_pe(e1,pe1m),"2 år":_price_from_pe(e2,pe2m),"3 år":_price_from_pe(e3,pe3m)})
    methods.append({"Metod":"ev_sales","Idag":_equity_price_from_ev(_ev_from_sales(r0,evs0),net_debt,shares),"1 år":_equity_price_from_ev(_ev_from_sales(r1,evs1),net_debt,shares),"2 år":_equity_price_from_ev(_ev_from_sales(r2,evs2),net_debt,shares),"3 år":_equity_price_from_ev(_ev_from_sales(r3,evs3),net_debt,shares)})
    methods.append({"Metod":"ev_ebitda","Idag":_equity_price_from_ev(_ev_from_ebitda(b0,eve0),net_debt,shares),"1 år":_equity_price_from_ev(_ev_from_ebitda(b1,eve1),net_debt,shares),"2 år":_equity_price_from_ev(_ev_from_ebitda(b2,eve2),net_debt,shares),"3 år":_equity_price_from_ev(_ev_from_ebitda(b3,eve3),net_debt,shares)})
    methods.append({"Metod":"ev_dacf","Idag":_equity_price_from_ev(_ev_from_ebitda(b0,eve0),net_debt,shares),"1 år":_equity_price_from_ev(_ev_from_ebitda(b1,eve1),net_debt,shares),"2 år":_equity_price_from_ev(_ev_from_ebitda(b2,eve2),net_debt,shares),"3 år":_equity_price_from_ev(_ev_from_ebitda(b3,eve3),net_debt,shares)})
    methods.append({"Metod":"p_b","Idag":_price_from_pb(pb0,bvps),"1 år":_price_from_pb(pb1,bvps),"2 år":_price_from_pb(pb2,bvps),"3 år":_price_from_pb(pb3,bvps)})

    for m in ("p_nav","p_tbv","p_affo","p_fcf","ev_fcf","p_nii"):
        methods.append({"Metod":m,"Idag":None,"1 år":None,"2 år":None,"3 år":None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    sanity = (
        f"price={'ok' if price else '—'}, eps_ttm={'ok' if e0 else '—'} ({src_eps_ttm}), "
        f"eps_1y={'ok' if e1 else '—'} ({src_eps_1y}), rev_ttm={'ok' if r0 else '—'}, "
        f"ebitda_ttm={'ok' if b0 else '—'}, shares={'ok' if shares else '—'}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {"currency": currency, "price": price, "shares_out": shares, "net_debt": net_debt, "pe_anchor": pe_anchor, "decay": decay}
    return methods_df, sanity, meta

# =========================
# Hjälpare för primär metod
# =========================
PRIMARY_FALLBACK = ["pe_hist_vs_eps", "ev_ebitda", "ev_sales", "p_b"]

def _auto_primary(methods_df: pd.DataFrame) -> str:
    """Välj första metod med minst 'Idag' ifylld enligt vår fallback-ordning."""
    avail = {m: methods_df.loc[methods_df["Metod"] == m] for m in PRIMARY_FALLBACK}
    for m, rowdf in avail.items():
        if not rowdf.empty and pd.notna(rowdf.iloc[0]["Idag"]):
            return m
    # sista utväg: första raden med något värde
    for _, r in methods_df.iterrows():
        if pd.notna(r["Idag"]):
            return str(r["Metod"])
    return methods_df["Metod"].iloc[0]

def _extract_primary_series(methods_df: pd.DataFrame, method: str) -> Dict[str, Optional[float]]:
    r = methods_df.loc[methods_df["Metod"] == method]
    if r.empty:
        return {"today": None, "y1": None, "y2": None, "y3": None}
    r = r.iloc[0]
    return {
        "today": _f(r.get("Idag")),
        "y1":    _f(r.get("1 år")),
        "y2":    _f(r.get("2 år")),
        "y3":    _f(r.get("3 år")),
    }

def _fmt_money(x: Optional[float], currency: str) -> str:
    return f"{x:,.2f} {currency}" if x is not None else "—"

def _calc_upside(target: Optional[float], price: Optional[float]) -> Optional[float]:
    if target is None or not _pos(price):
        return None
    try:
        return (float(target) / float(price) - 1.0) * 100.0
    except Exception:
        return None

# =========================
# Analys-vy
# =========================
def page_analysis():
    st.header("🔎 Analys")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    w_ttm    = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50

    df = read_data_df()
    if df.empty or "Ticker" not in df.columns or df["Ticker"].dropna().empty:
        st.info("Ingen data ännu. Gå till **Editor** och lägg till en ticker.")
        return

    tickers = df["Ticker"].astype(str).dropna().unique().tolist()
    tick = st.selectbox("Välj ticker", options=sorted(tickers))

    row = df.loc[df["Ticker"].astype(str) == str(tick)].iloc[0]
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    st.caption(f"Sanity: {sanity}")
    st.dataframe(methods_df, use_container_width=True)

    # Primär metod: respektera Data['Primär metod'] om den finns och är giltig
    preset = str(row.get("Primär metod") or "").strip()
    preset = preset if preset in methods_df["Metod"].tolist() else _auto_primary(methods_df)
    sel = st.selectbox("Primär metod", options=methods_df["Metod"].tolist(),
                       index=methods_df["Metod"].tolist().index(preset))

    prim = _extract_primary_series(methods_df, sel)

    st.markdown("### 🎯 Primär riktkurs")
    st.write(
        f"**Idag:** { _fmt_money(prim['today'], meta['currency']) }  \n"
        f"**1 år:** { _fmt_money(prim['y1'],    meta['currency']) }  \n"
        f"**2 år:** { _fmt_money(prim['y2'],    meta['currency']) }  \n"
        f"**3 år:** { _fmt_money(prim['y3'],    meta['currency']) }"
    )
    st.caption(f"Metod: **{sel}** • Valuta: **{meta['currency']}** • PE-ankare vikt (TTM): **{w_ttm:.0%}** • Multipel-decay/år: **{meta.get('decay', 0.10):.2f}**")

    # Uppsida
    up0 = _calc_upside(prim["today"], meta.get("price"))
    up1 = _calc_upside(prim["y1"],   meta.get("price"))
    colu = st.columns(2)
    colu[0].metric("Uppsida Idag", f"{up0:.1f}%" if up0 is not None else "—")
    colu[1].metric("Uppsida 1 år", f"{up1:.1f}%" if up1 is not None else "—")

    # Utdelning (netto i SEK)
    st.subheader("💸 Utdelningsprognos (netto SEK)")
    div_net = forecast_dividends_net_sek(
        currency=row.get("Valuta") or meta["currency"],
        shares=_pos(row.get("Antal aktier")),
        current_dps=_f(row.get("Årlig utdelning")),
        dps_cagr=_f(row.get("Utdelning CAGR")),
        fx_map=fx_map,
        settings=settings,
    )
    st.write(
        f"**Nästa 12m:** {div_net['y1']:,.0f} SEK  \n"
        f"**År 2:** {div_net['y2']:,.0f} SEK  \n"
        f"**År 3:** {div_net['y3']:,.0f} SEK"
    )

    st.divider()
    st.subheader("Spara")

    c1, c2 = st.columns(2)
    if c1.button("💾 Spara **primär metod + snapshot** till Data", use_container_width=True):
        try:
            # uppdatera raden i Data
            idx = df.index[df["Ticker"].astype(str) == str(tick)][0]
            df.loc[idx, "Primär metod"]   = sel
            df.loc[idx, "Aktuell kurs"]   = _f(meta.get("price"))
            df.loc[idx, "Valuta"]         = meta.get("currency")

            # Snapshotfält om de finns i Data – skriv endast om vi HAR värden
            snap_fields = {
                "Utestående aktier": _f(meta.get("shares_out")),
                "Net debt":          _f(meta.get("net_debt")),
            }

            # hämta även senaste snap direkt från yfinance (för tydligare sparning)
            snap = fetch_yahoo_snapshot(str(tick))
            snap_map = {
                "Rev TTM":      _f(snap.get("revenue_ttm")),
                "EBITDA TTM":   _f(snap.get("ebitda_ttm")),
                "EPS TTM":      _f(snap.get("eps_ttm")),
                "PE TTM":       _f(snap.get("pe_ttm")),
                "PE FWD":       _f(snap.get("pe_fwd")),
                "EV/Revenue":   _f(snap.get("ev_to_sales")),
                "EV/EBITDA":    _f(snap.get("ev_to_ebitda")),
                "P/B":          _f(snap.get("p_to_book")),
                "BVPS":         _f(snap.get("bvps")),
            }
            snap_fields.update(snap_map)

            for colname, val in snap_fields.items():
                if colname in df.columns and val is not None:
                    df.loc[idx, colname] = val

            write_data_df(df)
            st.success("Sparat till **Data**.")
        except Exception as e:
            st.error(f"Fel vid sparning till Data: {e}")

    if c2.button("🧷 Spara **resultatrad** (Primär riktkurs) till Resultat", use_container_width=True):
        try:
            append_result_row({
                "Timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "Ticker": str(tick),
                "Valuta": meta.get("currency"),
                "Aktuell kurs (0)": _f(meta.get("price")),
                "Riktkurs idag": prim["today"],
                "Riktkurs 1 år":  prim["y1"],
                "Riktkurs 2 år":  prim["y2"],
                "Riktkurs 3 år":  prim["y3"],
                "Metod": sel,
                "Input-sammanfattning": f"pe_anchor={_f(meta.get('pe_anchor'))}, decay={_f(meta.get('decay'))}, shares_fd={_f(meta.get('shares_out'))}",
                "Kommentar": "",
            })
            st.success("Resultatrad sparad.")
        except Exception as e:
            st.error(f"Fel vid sparning till Resultat: {e}")

# =========================
# Editor (lägg till / uppdatera)
# =========================
def _ensure_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in DATA_COLUMNS:
        if c not in out.columns:
            out[c] = np.nan
    # behåll ev. extra kolumner men se till att standardsatsen ligger först
    return out[[c for c in DATA_COLUMNS if c in out.columns] + [c for c in out.columns if c not in DATA_COLUMNS]]

def _val(x, fallback):
    """Om x är ett giltigt tal/sträng -> x, annars fallback."""
    if isinstance(x, str):
        s = x.strip()
        return s if s != "" else fallback
    v = _f(x)
    return x if (v is not None or x not in (None, np.nan, "")) else fallback

def page_editor():
    st.header("📝 Lägg till / uppdatera bolag")

    df = read_data_df()
    df = _ensure_all_columns(df)

    existing = ["— nytt —"] + sorted(df["Ticker"].astype(str).dropna().unique().tolist()) if not df.empty else ["— nytt —"]
    pick = st.selectbox("Välj ticker", existing, index=0)
    is_new = pick == "— nytt —"

    init = {} if is_new or df.empty else df.loc[df["Ticker"].astype(str) == pick].iloc[0].to_dict()

    # ---- Form ----
    with st.form("edit_form"):
        c1, c2, c3 = st.columns(3)
        ticker  = c1.text_input("Ticker", value=(init.get("Ticker") or "")).upper().strip()
        namn    = c2.text_input("Bolagsnamn", value=str(init.get("Bolagsnamn") or ""))
        sektor  = c3.text_input("Sektor", value=str(init.get("Sektor") or ""))

        bucket  = st.selectbox("Bucket", DEFAULT_BUCKETS, index=DEFAULT_BUCKETS.index(init.get("Bucket")) if init.get("Bucket") in DEFAULT_BUCKETS else 0)
        valuta  = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"], index=["USD","EUR","NOK","CAD","SEK"].index(str(init.get("Valuta") or "USD").upper()))

        n1, n2, n3, n4 = st.columns(4)
        antal      = n1.number_input("Antal aktier", min_value=0, step=1, value=int(_f(init.get("Antal aktier")) or 0))
        gav_sek    = n2.number_input("GAV (SEK)", min_value=0.0, step=0.01, value=float(_f(init.get("GAV (SEK)")) or 0.0))
        pris       = n3.number_input("Aktuell kurs", min_value=0.0, step=0.01, value=float(_f(init.get("Aktuell kurs")) or 0.0))
        shares_out = n4.number_input("Utestående aktier", min_value=0.0, step=1.0, value=float(_f(init.get("Utestående aktier")) or 0.0))

        m1, m2, m3, m4 = st.columns(4)
        rev_ttm    = m1.number_input("Rev TTM", min_value=0.0, step=1000.0, value=float(_f(init.get("Rev TTM")) or 0.0))
        ebitda_ttm = m2.number_input("EBITDA TTM", min_value=0.0, step=1000.0, value=float(_f(init.get("EBITDA TTM")) or 0.0))
        eps_ttm    = m3.number_input("EPS TTM", min_value=0.0, step=0.01, value=float(_f(init.get("EPS TTM")) or 0.0))
        net_debt   = m4.number_input("Net debt", min_value=0.0, step=1000.0, value=float(_f(init.get("Net debt")) or 0.0))

        k1, k2, k3, k4 = st.columns(4)
        pe_ttm = k1.number_input("PE TTM", min_value=0.0, step=0.01, value=float(_f(init.get("PE TTM")) or 0.0))
        pe_fwd = k2.number_input("PE FWD", min_value=0.0, step=0.01, value=float(_f(init.get("PE FWD")) or 0.0))
        ev_rev = k3.number_input("EV/Revenue", min_value=0.0, step=0.01, value=float(_f(init.get("EV/Revenue")) or 0.0))
        ev_eb  = k4.number_input("EV/EBITDA", min_value=0.0, step=0.01, value=float(_f(init.get("EV/EBITDA")) or 0.0))

        b1, b2, b3, b4 = st.columns(4)
        pb    = b1.number_input("P/B", min_value=0.0, step=0.01, value=float(_f(init.get("P/B")) or 0.0))
        bvps  = b2.number_input("BVPS", min_value=0.0, step=0.01, value=float(_f(init.get("BVPS")) or 0.0))
        eps1y = b3.number_input("EPS 1Y (estimat)", min_value=0.0, step=0.01, value=float(_f(init.get("EPS 1Y")) or 0.0))
        epscg = b4.number_input("EPS CAGR", min_value=0.0, step=0.001, value=float(_f(init.get("EPS CAGR")) or 0.0))

        g1, g2, g3 = st.columns(3)
        revcg = g1.number_input("Rev CAGR", min_value=0.0, step=0.001, value=float(_f(init.get("Rev CAGR")) or 0.0))
        dps   = g2.number_input("Årlig utdelning (DPS)", min_value=0.0, step=0.01, value=float(_f(init.get("Årlig utdelning")) or 0.0))
        dpscg = g3.number_input("Utdelning CAGR", min_value=0.0, step=0.001, value=float(_f(init.get("Utdelning CAGR")) or 0.0))

        prim_fallback = init.get("Primär metod") if init.get("Primär metod") in [
            "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
        ] else "ev_ebitda"
        prim  = st.selectbox("Primär metod", [
            "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
        ], index=["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"].index(prim_fallback))

        cbt1, cbt2 = st.columns(2)
        fetch_btn  = cbt1.form_submit_button("🔎 Hämta från Yahoo nu")
        save_btn   = cbt2.form_submit_button("💾 Spara till Data")

    # ---- Hämta från Yahoo (visar snapshot) ----
    if fetch_btn and ticker:
        snap = fetch_yahoo_snapshot(ticker)
        st.info(
            f"Hämtat: pris={snap.get('price')} {snap.get('currency')}, "
            f"EV/Rev={snap.get('ev_to_sales')}, EV/EBITDA={snap.get('enterpriseToEbitda') or snap.get('ev_to_ebitda')}, "
            f"P/B={snap.get('p_to_book')}, BVPS={snap.get('bvps')}, EPS_TTM={snap.get('eps_ttm')}, "
            f"PE_TTM={snap.get('pe_ttm')}, PE_FWD={snap.get('pe_fwd')}"
        )
        st.caption("Värdena fylls in vid spar om dina fält ovan är tomma/0.")

    # ---- Spara (upsert) ----
    if save_btn:
        if not ticker:
            st.error("Ange en ticker.")
            return
        # hämta snapshot nu för att fylla luckor
        snap = fetch_yahoo_snapshot(ticker)

        def take(val, key_in_snap=None):
            """Använd formulärvärde om >0/sträng, annars snapshot."""
            if isinstance(val, (int, float)):
                vv = _f(val)
                if vv is not None and vv > 0:
                    return vv
            elif isinstance(val, str) and val.strip() != "":
                return val.strip()
            if key_in_snap:
                sv = snap.get(key_in_snap)
                fv = _f(sv)
                return fv if fv is not None else sv
            return val

        new_row = {
            "Timestamp": now_stamp(),
            "Ticker": ticker,
            "Bolagsnamn": _val(namn, ""),
            "Sektor": _val(sektor, ""),
            "Bucket": bucket,
            "Valuta": (snap.get("currency") or valuta or "USD"),
            "Antal aktier": antal,
            "GAV (SEK)": gav_sek,
            "Aktuell kurs": take(pris, "price"),
            "Utestående aktier": take(shares_out, "shares"),
            "Net debt": take(net_debt, "net_debt"),
            "Rev TTM": take(rev_ttm, "revenue_ttm"),
            "EBITDA TTM": take(ebitda_ttm, "ebitda_ttm"),
            "EPS TTM": take(eps_ttm, "eps_ttm"),
            "PE TTM": take(pe_ttm, "pe_ttm"),
            "PE FWD": take(pe_fwd, "pe_fwd"),
            "EV/Revenue": take(ev_rev, "ev_to_sales"),
            "EV/EBITDA": take(ev_eb, "ev_to_ebitda"),
            "P/B": take(pb, "p_to_book"),
            "BVPS": take(bvps, "bvps"),
            "EPS 1Y": _f(eps1y),
            "Rev CAGR": _f(revcg),
            "EPS CAGR": _f(epscg),
            "Årlig utdelning": _f(dps),
            "Utdelning CAGR": _f(dpscg),
            "Primär metod": prim,
            "Senast auto uppdaterad": now_stamp(),
            "Auto källa": "Yahoo" if snap.get("price") else "Manuell",
        }

        dfw = _ensure_all_columns(df)
        if (dfw["Ticker"].astype(str) == ticker).any():
            mask = (dfw["Ticker"].astype(str) == ticker)
            for k, v in new_row.items():
                if k in dfw.columns:
                    dfw.loc[mask, k] = v
                else:
                    dfw[k] = np.nan
                    dfw.loc[mask, k] = v
        else:
            # append tryggt: aligna mot dfw.columns
            append_row = {c: new_row.get(c, np.nan) for c in dfw.columns}
            dfw = pd.concat([dfw, pd.DataFrame([append_row])], ignore_index=True)

        try:
            write_data_df(dfw)
            st.success("Sparat till Google Sheets ✅")
            # uppdatera cache så Analys/Ranking ser nya värden direkt
            try:
                _read_df.clear()  # type: ignore[attr-defined]
                read_data_df.clear()  # type: ignore[attr-defined]
            except Exception:
                pass
        except Exception as e:
            st.error(f"Fel vid skrivning till Google Sheets: {e}")

# =========================
# Settings
# =========================
def page_settings():
    st.header("⚙️ Inställningar")
    s = get_settings_map()
    c1, c2 = st.columns(2)
    pe_w  = c1.number_input("PE-ankare vikt (TTM)", min_value=0.0, max_value=1.0, step=0.05, value=float(s.get("pe_anchor_weight_ttm", 0.50)))
    decay = c2.number_input("Multipel-decay per år", min_value=0.0, max_value=1.0, step=0.01, value=float(s.get("multiple_decay", 0.10)))
    if st.button("💾 Spara parametrar"):
        sdf = _read_df(SETTINGS_TITLE)
        if sdf.empty:
            sdf = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def upsert(key, val):
            if (sdf["Key"] == key).any():
                sdf.loc[sdf["Key"] == key, "Value"] = str(val)
            else:
                sdf.loc[len(sdf)] = [key, str(val)]
        upsert("pe_anchor_weight_ttm", pe_w)
        upsert("multiple_decay", decay)
        _write_df(SETTINGS_TITLE, sdf)
        try:
            get_settings_map.clear()  # type: ignore[attr-defined]
        except Exception:
            pass
        st.success("Sparat.")

# =========================
# Batch (uppdatera alla från Yahoo)
# =========================
def page_batch():
    st.header("🧰 Batch-uppdatering från Yahoo")
    df = read_data_df()
    if df.empty:
        st.info("Inga bolag i Data.")
        return
    delay = st.slider("Fördröjning per bolag (sek)", 0.2, 2.0, 0.8, 0.1)
    if st.button("🚀 Kör uppdatering"):
        df2 = _ensure_all_columns(df)
        prog = st.progress(0.0)
        for i, (idx, r) in enumerate(df2.iterrows()):
            tkr = str(r["Ticker"]).strip().upper()
            snap = fetch_yahoo_snapshot(tkr)
            # skriv endast om värden finns
            for k_map, y_key in [
                ("Aktuell kurs", "price"), ("Valuta", "currency"), ("Rev TTM","revenue_ttm"),
                ("EBITDA TTM","ebitda_ttm"), ("EPS TTM","eps_ttm"), ("PE TTM","pe_ttm"),
                ("PE FWD","pe_fwd"), ("EV/Revenue","ev_to_sales"), ("EV/EBITDA","ev_to_ebitda"),
                ("P/B","p_to_book"), ("BVPS","bvps"), ("Net debt","net_debt"), ("Utestående aktier","shares")
            ]:
                val = snap.get(y_key)
                if val is not None and k_map in df2.columns:
                    df2.at[idx, k_map] = val
            df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
            df2.at[idx, "Auto källa"] = "Yahoo" if snap.get("price") else "Manuell"
            prog.progress((i+1)/len(df2))
            time.sleep(delay)
        write_data_df(df2)
        st.success("Alla rader uppdaterade.")

# =========================
# Enkel Ranking (uppsida idag)
# =========================
def page_ranking():
    st.header("🏁 Ranking (uppsida mot primär riktkurs idag)")
    df = read_data_df()
    if df.empty:
        st.info("Tomt Data-blad.")
        return
    settings = get_settings_map()
    fx_map   = get_fx_map()

    rows = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(df.iterrows()):
        try:
            methods_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            chosen = r.get("Primär metod")
            if chosen not in methods_df["Metod"].tolist():
                chosen = _auto_primary(methods_df)
            prim = _extract_primary_series(methods_df, str(chosen))
            price = _f(meta.get("price"))
            up = _calc_upside(prim["today"], price)
            rows.append({
                "Ticker": r.get("Ticker"),
                "Metod": chosen,
                "Pris": price,
                "Fair (Idag)": prim["today"],
                "Uppsida %": up
            })
        except Exception:
            rows.append({"Ticker": r.get("Ticker"), "Metod": None, "Pris": None, "Fair (Idag)": None, "Uppsida %": None})
        prog.progress((i+1)/len(df))
        time.sleep(0.15)
    prog.empty()
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(by=["Uppsida %"], ascending=False, na_position="last")
    st.dataframe(out, use_container_width=True)

# =========================
# MAIN / Routing
# =========================
def run_main_ui():
    st.title(APP_TITLE)
    page = st.sidebar.radio("Sidor", ["Editor","Analys","Ranking","Settings","Batch"], index=1)
    if page == "Editor":
        page_editor()
    elif page == "Analys":
        page_analysis()
    elif page == "Ranking":
        page_ranking()
    elif page == "Settings":
        page_settings()
    elif page == "Batch":
        page_batch()

def main():
    run_main_ui()

if __name__ == "__main__":
    main()
