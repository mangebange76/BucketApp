# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 1/4: Bas & infrastruktur (UI, helpers, Sheets I/O, schema, FX/Settings)
# ============================================================

from __future__ import annotations

# ---------- Standardbibliotek ----------
import os, json, math, time
from typing import Any, Dict, List, Optional, Tuple
from collections.abc import Mapping
import datetime as dt

# ---------- Tredjepart ----------
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
        return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def today_date() -> dt.date:
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return dt.datetime.now(tz).date()
    except Exception:
        return dt.datetime.now().date()

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

# ---------- Extra säkra hjälpare (för Editor/inputs) ----------
def _f0(x, default: float = 0.0) -> float:  # CHANGED: ny — float med säker default
    v = _f(x)
    return float(default) if v is None else float(v)

def _i0(x, default: int = 0) -> int:       # CHANGED: ny — int med säker default
    v = _f(x)
    try:
        return int(round(v)) if v is not None and math.isfinite(v) else int(default)
    except Exception:
        return int(default)

def _r2(x):                                # CHANGED: ny — rundar till 2 decimaler om möjligt
    try:
        v = float(x)
        return round(v, 2)
    except Exception:
        return x

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

# (Alias-stöd) — acceptera även GOOGLE_SHEET_URL / GOOGLE_SHEET_ID
@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppnar spreadsheet via SHEET_URL/SHEET_ID (stöder även GOOGLE_SHEET_URL/GOOGLE_SHEET_ID)."""
    sheet_url = (_env_or_secret("SHEET_URL") or _env_or_secret("GOOGLE_SHEET_URL"))
    sheet_id  = (_env_or_secret("SHEET_ID")  or _env_or_secret("GOOGLE_SHEET_ID"))

    if sheet_url and sheet_url.strip():
        return _with_backoff(_gc.open_by_url, sheet_url.strip())
    if sheet_id and sheet_id.strip():
        return _with_backoff(_gc.open_by_key, sheet_id.strip())
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID (eller GOOGLE_SHEET_URL / GOOGLE_SHEET_ID) i secrets.")

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    try:
        return _with_backoff(spread.worksheet, title)
    except WorksheetNotFound:
        return _with_backoff(spread.add_worksheet, title=title, rows=2000, cols=200)

# =========================
# I/O – läs/skriv/append
# =========================
def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Garanti: alla kolumner finns i df (annars läggs de till som NaN)."""
    if df.empty:
        return pd.DataFrame(columns=cols)
    changed = False
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
            changed = True
    # behåll befintlig ordning + lägg nya sist
    if changed:
        df = df[[*(k for k in cols if k in df.columns), *[c for c in df.columns if c not in cols]]]
    return df

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
# Utökad schema för Editor (manuella fält & tidsstämplar)
DATA_COLUMNS = [
    "Timestamp","Ticker","Bolagsnamn","Sektor","Bucket","Valuta",
    "Antal aktier","GAV (SEK)","Aktuell kurs",
    "Utestående aktier","Net debt",
    "Rev TTM","EBITDA TTM","EPS TTM",
    "PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
    "EPS 1Y","EPS 2Y",
    "Rev 1Y","Rev 2Y",
    "Rev CAGR","EPS CAGR",
    "Årlig utdelning","Utdelning CAGR",
    # Utdelningslista
    "Utdelningsfrekvens",
    "Nästa utdelningsdatum",
    "Nästa utdelning (per aktie)",
    "Primär metod",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    # Fältvisa tidsstämplar
    "TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y",
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
            ["auto_refresh_on_start","0"],
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
    # 🔒 Global garanti mot KeyError – säkerställ alla kolumner finns
    df = _ensure_columns(df, DATA_COLUMNS)

    if df.empty:
        return df

    # Typning för nycklar
    num_cols = [
        "Antal aktier","GAV (SEK)","Aktuell kurs",
        "Utestående aktier","Net debt",
        "Rev TTM","EBITDA TTM","EPS TTM","PE TTM","PE FWD",
        "EV/Revenue","EV/EBITDA","P/B","BVPS","EPS 1Y","EPS 2Y",
        "Rev 1Y","Rev 2Y",
        "Rev CAGR","EPS CAGR","Årlig utdelning","Utdelning CAGR",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        "Nästa utdelning (per aktie)"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Datumkolumn
    if "Nästa utdelningsdatum" in df.columns:
        df["Nästa utdelningsdatum"] = pd.to_datetime(df["Nästa utdelningsdatum"], errors="coerce").dt.date

    # TS-fält
    for tcol in ["TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y","Senast auto uppdaterad"]:
        if tcol in df.columns:
            df[tcol] = df[tcol].astype(str)

    # Ignorera nollor i auto-fält
    IGNORE_ZERO_COLS = [
        "Aktuell kurs","Utestående aktier","Net debt",
        "Rev TTM","EBITDA TTM","EPS TTM",
        "PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
        "EPS 1Y","EPS 2Y","Rev CAGR","EPS CAGR",
        "Årlig utdelning","Utdelning CAGR",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        "Nästa utdelning (per aktie)"
    ]
    for c in IGNORE_ZERO_COLS:
        if c in df.columns:
            df.loc[(df[c].notna()) & (df[c] == 0), c] = np.nan

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

# ============================================================
# Del 1/4 slut — fortsätt i Del 2/4 (datainsamling & beräkningsmotor)
# ============================================================

# ============================================================
# app.py — Del 2/4
# Datainsamling (Yahoo/Finnhub) & beräkningsmotor
#  • Robust snapshot från Yahoo (pris, valuta, EV, TTM osv)
#  • EPS/REV-estimat + 5-års CAGR (hist)
#  • Multipel-decay + builders
#  • compute_methods_for_row (returnerar metodtabell)
#  • _build_updates_from_yahoo + _apply_updates_to_df_row  ← NYTT
# ============================================================

import requests
import pandas as pd
import numpy as np
import streamlit as st
import math
import time
import yfinance as yf

# -------------------------
# Hjälpare
# -------------------------
def _ix_pick(df: pd.DataFrame, candidates: list[str]):
    if df is None or getattr(df, "empty", True):
        return None
    norm = {str(i).replace(" ", "").lower(): i for i in df.index}
    for cand in candidates:
        key = cand.replace(" ", "").lower()
        if key in norm:
            try:
                return df.loc[norm[key]]
            except Exception:
                pass
    for k, orig in norm.items():
        for cand in candidates:
            if cand.replace(" ", "").lower() in k:
                try:
                    return df.loc[orig]
                except Exception:
                    pass
    return None

def _sum_last4(ser_like):
    try:
        s = pd.to_numeric(pd.Series(ser_like), errors="coerce").dropna()
        if s.empty:
            return None
        try:
            s.index = pd.to_datetime(s.index, errors="coerce")
            s = s.sort_index()
        except Exception:
            pass
        vals = s.dropna().values.tolist()
        if len(vals) == 0:
            return None
        return float(np.nansum(vals[-4:]))
    except Exception:
        return None

def _sum_eps_last4(ser_like):
    return _sum_last4(ser_like)

# -------------------------
# Yahoo (yfinance) – robust snapshot
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> dict[str, any]:
    tk = yf.Ticker(ticker)
    out: dict[str, any] = {"sources": {}}

    # Snabbkanal
    try:
        fi = tk.fast_info
        out["price"]      = _f(getattr(fi, "last_price", None));      out["sources"]["price"] = "yahoo_fast"
        out["currency"]   = getattr(fi, "currency", None);            out["sources"]["currency"] = "yahoo_fast"
        out["market_cap"] = _f(getattr(fi, "market_cap", None));      out["sources"]["market_cap"] = "yahoo_fast"
        out["shares"]     = _f(getattr(fi, "shares", None));          out["sources"]["shares"] = "yahoo_fast"
    except Exception:
        pass

    # info()-fallbacks
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k): 
        try: return info.get(k)
        except Exception: return None

    def set_if_missing(k, val, src):
        if out.get(k) is None and val is not None:
            out[k] = _f(val) if isinstance(val, (int, float, str)) else val
            out["sources"][k] = src

    set_if_missing("price",        gi("currentPrice"),        "yahoo_info")
    set_if_missing("currency",     gi("currency"),            "yahoo_info")
    set_if_missing("market_cap",   gi("marketCap"),           "yahoo_info")
    set_if_missing("pe_ttm",       gi("trailingPE"),          "yahoo_info")
    set_if_missing("pe_fwd",       gi("forwardPE"),           "yahoo_info")
    set_if_missing("p_to_book",    gi("priceToBook"),         "yahoo_info")
    set_if_missing("bvps",         gi("bookValue"),           "yahoo_info")
    set_if_missing("eps_ttm",      gi("trailingEps"),         "yahoo_info")

    if out.get("shares") is None:
        so = _f(gi("sharesOutstanding"))
        if so is not None:
            out["shares"] = so
            out["sources"]["shares"] = "yahoo_info_sharesOutstanding"

    if out.get("pe_ttm") is None and _pos(out.get("price")) and _pos(out.get("eps_ttm")):
        try:
            out["pe_ttm"] = float(out["price"]) / float(out["eps_ttm"])
            out["sources"]["pe_ttm"] = "calc_price/eps_ttm"
        except Exception:
            pass

    if out.get("pe_fwd") is None and _pos(out.get("price")):
        fwd_eps = _f(gi("forwardEps") or gi("forwardEPS"))
        if _pos(fwd_eps):
            try:
                out["pe_fwd"] = float(out["price"]) / float(fwd_eps)
                out["sources"]["pe_fwd"] = "calc_price/forwardEPS"
            except Exception:
                pass

    try:
        cname = gi("longName") or gi("shortName")
        if cname:
            out["company_name"] = str(cname); out["sources"]["company_name"] = "yahoo_info"
        sector = gi("sector")
        if sector:
            out["sector"] = str(sector); out["sources"]["sector"] = "yahoo_info"
        industry = gi("industry")
        if industry:
            out["industry"] = str(industry); out["sources"]["industry"] = "yahoo_info"
    except Exception:
        pass

    total_debt = _f(gi("totalDebt"))
    total_cash = _f(gi("totalCash"))
    ev_info    = _f(gi("enterpriseValue"))

    if ev_info is not None:
        out["ev"] = ev_info; out["sources"]["ev"] = "yahoo_info"
    elif _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
        out["sources"]["ev"] = "calc_mc+debt-cash"

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]; out["sources"]["net_debt"] = "calc_ev-mcap"

    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            out["sources"]["shares"] = "derived_mcap/price"
        except Exception:
            pass

    if not _pos(out.get("price")):
        try:
            hist = tk.history(period="5d")
            if not hist.empty:
                out["price"] = float(hist["Close"].dropna().iloc[-1])
                out["sources"]["price"] = "yahoo_hist_close"
        except Exception:
            pass

    # Balance sheet-fallbacks + BVPS/PB
    bs_q = None
    try:
        bs_q = tk.get_balance_sheet(freq="quarterly")
    except Exception:
        bs_q = getattr(tk, "quarterly_balance_sheet", None) or getattr(tk, "balance_sheet", None)

    if bs_q is not None and not getattr(bs_q, "empty", True):
        debt_row = _ix_pick(bs_q, ["TotalDebt", "Total Debt", "ShortLongTermDebtTotal"])
        cash_row = _ix_pick(bs_q, ["CashAndCashEquivalents", "Cash And Cash Equivalents", "CashAndShortTermInvestments"])
        if total_debt is None and debt_row is not None:
            try:
                total_debt = float(pd.to_numeric(pd.Series(debt_row), errors="coerce").dropna().iloc[-1])
                out["sources"]["_total_debt_fallback"] = "balance_sheet_q"
            except Exception:
                pass
        if total_cash is None and cash_row is not None:
            try:
                total_cash = float(pd.to_numeric(pd.Series(cash_row), errors="coerce").dropna().iloc[-1])
                out["sources"]["_total_cash_fallback"] = "balance_sheet_q"
            except Exception:
                pass
        if out.get("ev") is None and _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
            out["ev"] = out["market_cap"] + total_debt - total_cash
            out["sources"]["ev"] = "calc_mc+debt-cash(bs)"
        if out.get("net_debt") is None and out.get("ev") is not None and out.get("market_cap") is not None:
            out["net_debt"] = out["ev"] - out["market_cap"]
            out["sources"]["net_debt"] = "calc_ev-mcap(bs)"
        eq_row = _ix_pick(bs_q, ["StockholdersEquity", "TotalStockholderEquity", "Total Stockholder Equity"])
        if out.get("bvps") is None and eq_row is not None and _pos(out.get("shares")):
            try:
                eq_last = float(pd.to_numeric(pd.Series(eq_row), errors="coerce").dropna().iloc[-1])
                out["bvps"] = eq_last / float(out["shares"])
                out["sources"]["bvps"] = "calc_equity/shares(balance_sheet_q)"
            except Exception:
                pass
        if out.get("p_to_book") is None and _pos(out.get("price")) and _pos(out.get("bvps")):
            try:
                out["p_to_book"] = float(out["price"]) / float(out["bvps"])
                out["sources"]["p_to_book"] = "calc_price/bvps"
            except Exception:
                pass

    # TTM via kvartal
    try:
        try:
            inc_q = tk.get_income_stmt(freq="quarterly")
        except Exception:
            inc_q = getattr(tk, "quarterly_income_stmt", None) or getattr(tk, "income_stmt", None)

        if inc_q is not None and not getattr(inc_q, "empty", True):
            dfq = inc_q.copy()

            eps_row = _ix_pick(dfq, ["DilutedEPS", "BasicEPS", "EPS"])
            eps_ttm_q = _sum_eps_last4(eps_row) if eps_row is not None else None

            rev_row = _ix_pick(dfq, ["TotalRevenue", "Total Revenue", "Revenue"])
            rev_ttm_q = _sum_last4(rev_row) if rev_row is not None else None

            ebitda_row = _ix_pick(dfq, ["Ebitda", "EBITDA", "EarningsBeforeInterestTaxesDepreciationAmortization"])
            ebitda_ttm_q = _sum_last4(ebitda_row) if ebitda_row is not None else None

            if _pos(eps_ttm_q) is not None:
                out["eps_ttm"] = float(eps_ttm_q); out["sources"]["eps_ttm"] = "yahoo_quarterly_TTM"

            if _pos(rev_ttm_q) is not None:
                out["revenue_ttm"] = float(rev_ttm_q); out["sources"]["revenue_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")

            if ebitda_ttm_q is not None:
                out["ebitda_ttm"] = float(ebitda_ttm_q); out["sources"]["ebitda_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("ebitda_ttm", gi("ebitda"), "yahoo_info")
    except Exception:
        set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")
        set_if_missing("ebitda_ttm",  gi("ebitda"),        "yahoo_info")

    if _pos(out.get("ev")) and _pos(out.get("revenue_ttm")):
        try:
            out["ev_to_sales"] = float(out["ev"]) / float(out["revenue_ttm"])
            out["sources"]["ev_to_sales"] = out["sources"].get("revenue_ttm", "calc_ev/sales")
        except Exception:
            pass

    if _pos(out.get("ev")) and out.get("ebitda_ttm") is not None:
        try:
            e = float(out["ebitda_ttm"])
            out["ev_to_ebitda"] = (float(out["ev"]) / e) if e != 0 else None
            out["sources"]["ev_to_ebitda"] = out["sources"].get("ebitda_ttm", "calc_ev/ebitda")
        except Exception:
            pass

    # Utdelning & frekvens
    fwd_div = _f(gi("dividendRate") or gi("forwardAnnualDividendRate"))
    trl_div = _f(gi("trailingAnnualDividendRate"))
    if out.get("annual_dividend") is None and (fwd_div is not None or trl_div is not None):
        out["annual_dividend"] = float(_nz(fwd_div, trl_div)); out["sources"]["annual_dividend"] = "yahoo_info"

    try:
        divs = None
        try:
            divs = tk.get_dividends()
        except Exception:
            divs = getattr(tk, "dividends", None)
        if divs is not None and hasattr(divs, "index") and len(divs) > 0:
            last12 = divs[divs.index >= (pd.Timestamp.today() - pd.Timedelta(days=370))]
            n = int(len(last12))
            freq = None
            if n >= 10:   freq = "M"
            elif n >= 3:  freq = "Q"
            elif n == 2:  freq = "S"
            elif n == 1:  freq = "A"
            if freq:
                out["dividend_frequency"] = freq
                out["sources"]["dividend_frequency"] = "yahoo_dividends_infer"
    except Exception:
        pass

    out["currency"] = str(out.get("currency") or "USD").upper()
    return out

# -------------------------
# EPS-estimat (Yahoo) + fallback
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> dict[str, float | None]:
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

        def _avg_from_cell(val) -> float | None:
            if isinstance(val, dict):
                for k in ("avg", "average", "mean"):
                    if k in val and _f(val[k]) is not None:
                        return _f(val[k])
            return _f(val)

        def _pick_row(period_aliases: list[str]):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for alias in period_aliases:
                a = m.str.contains(rf"^{alias}$")
                mask = a if mask is None else (mask | a)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_nextyear    = _pick_row(["nextyear", "next fiscal year", "nextfiscalyear"])
        row_longterm    = _pick_row(["longterm", "next5years", "next 5 years"])
        row_currentyear = _pick_row(["currentyear", "current fiscal year", "currentfiscalyear"])

        eps_1y = None
        if row_nextyear is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg_from_cell(row_nextyear.get(col))
                    if eps_1y is not None:
                        break
        if eps_1y is None and row_currentyear is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg_from_cell(row_currentyear.get(col))
                    if eps_1y is not None:
                        break

        eps_cagr_long = None
        if row_longterm is not None:
            for col in ["growth", "longtermgrowthrate"]:
                if col in df.columns:
                    eps_cagr_long = None if _f(row_longterm.get(col)) is None else float(_f(row_longterm.get(col)))
                    if eps_cagr_long is not None:
                        break

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": eps_cagr_long, "source": "yahoo_trend"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

# -------------------------
# 5-års CAGR (Revenue & EPS)
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    try:
        tk = yf.Ticker(ticker)
        try:
            inc = tk.get_income_stmt(freq="annual")
        except Exception:
            inc = getattr(tk, "income_stmt", None)

        if inc is None or (hasattr(inc, "empty") and inc.empty):
            return {"rev_cagr": None, "years": None, "source": "none"}

        df = inc.copy()
        total_rev = _ix_pick(df, ["TotalRevenue", "Total Revenue", "Revenue"])
        if total_rev is None:
            return {"rev_cagr": None, "years": None, "source": "none"}

        ser = pd.to_numeric(pd.Series(total_rev).dropna(), errors="coerce").dropna()
        if ser.empty:
            return {"rev_cagr": None, "years": None, "source": "none"}

        try:
            ser.index = pd.to_datetime(ser.index, errors="coerce"); ser = ser.sort_index()
        except Exception:
            pass

        vals = ser.dropna().values.tolist()
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

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_eps_cagr_hist(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    try:
        tk = yf.Ticker(ticker)
        try:
            inc = tk.get_income_stmt(freq="annual")
        except Exception:
            inc = getattr(tk, "income_stmt", None)

        if inc is None or (hasattr(inc, "empty") and inc.empty):
            return {"eps_cagr": None, "years": None, "source": "none"}

        df = inc.copy()
        eps_row = _ix_pick(df, ["DilutedEPS", "BasicEPS", "EPS"])

        if eps_row is None:
            ni_row = _ix_pick(df, ["NetIncome", "Net Income", "NetIncomeApplicableToCommonShares", "NetIncomeCommonStockholders"])
            sh_row = _ix_pick(df, ["DilutedAverageShares", "Diluted Shares", "AverageDilutedSharesOutstanding", "WeightedAverageDilutedSharesOutstanding"])
            if ni_row is None or sh_row is None:
                return {"eps_cagr": None, "years": None, "source": "none"}
            ni = pd.to_numeric(pd.Series(ni_row), errors="coerce")
            sh = pd.to_numeric(pd.Series(sh_row), errors="coerce")
            eps_series = (ni / sh).replace([np.inf, -np.inf], np.nan).dropna()
        else:
            eps_series = pd.to_numeric(pd.Series(eps_row), errors="coerce").dropna()

        if eps_series.empty:
            return {"eps_cagr": None, "years": None, "source": "none"}

        try:
            eps_series.index = pd.to_datetime(eps_series.index, errors="coerce"); eps_series = eps_series.sort_index()
        except Exception:
            pass

        vals = eps_series.dropna().values.tolist()
        if len(vals) < 2:
            return {"eps_cagr": None, "years": None, "source": "yahoo_financials"}

        n = min(max_years, len(vals))
        vals = vals[-n:]
        n_years = len(vals) - 1
        if n_years < 1 or n_years < min_years-1:
            return {"eps_cagr": None, "years": len(vals), "source": "yahoo_financials"}

        try:
            cagr = (vals[-1] / max(1e-12, vals[0])) ** (1.0 / n_years) - 1.0
        except Exception:
            cagr = None

        return {"eps_cagr": cagr, "years": n_years, "source": "yahoo_financials"}
    except Exception:
        return {"eps_cagr": None, "years": None, "source": "none"}

# -------------------------
# Finnhub – EPS fallback
# -------------------------
def _get_finnhub_key() -> str | None:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

@st.cache_data(ttl=900, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> dict[str, float | None]:
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
            vals = [_f(x.get("epsAvg")) for x in rows if _f(x.get("epsAvg")) is not None]
            if len(vals) >= 1: eps_1y = vals[-1]
            if len(vals) >= 2: eps_2y = vals[-2]
        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "source": "finnhub"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

# -------------------------
# Multiplar & builders
# -------------------------
REV_CAGR_MIN = -0.10
REV_CAGR_MAX =  0.35
EPS_CAGR_MIN = -0.20
EPS_CAGR_MAX =  0.35

def _clamp(val: float | None, lo: float, hi: float) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        if not math.isfinite(v):
            return None
        return max(lo, min(hi, v))
    except Exception:
        return None

def _decay_multiple(mult0: float | None, years: int, decay: float, floor_frac: float = 0.60) -> float | None:
    m0 = _pos(mult0)
    if m0 is None:
        return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_frac
    return max(m, floor)

def _pe_anchor(pe_ttm: float | None, pe_fwd: float | None, w_ttm: float) -> float | None:
    pt = _pos(pe_ttm)
    pf = _pos(pe_fwd)
    if pt is None and pf is None:
        return None
    if pt is None:
        return pf
    if pf is None:
        return pt
    return w_ttm * pt + (1.0 - w_ttm) * pf

def _equity_price_from_ev(ev_target: float | None, net_debt: float | None, shares_fd: float | None) -> float | None:
    e = _pos(ev_target); s = _pos(shares_fd)
    if e is None or s is None:
        return None
    nd = _nz(net_debt, 0.0)
    try:
        return max(0.0, (e - nd) / s)
    except Exception:
        return None

def _price_from_pe(eps: float | None, pe: float | None) -> float | None:
    e = _pos(eps); p = _pos(pe)
    if e is None or p is None:
        return None
    return e * p

def _ev_from_sales(rev: float | None, mult: float | None) -> float | None:
    r = _pos(rev); m = _pos(mult)
    if r is None or m is None:
        return None
    return r * m

def _ev_from_ebitda(ebitda: float | None, mult: float | None) -> float | None:
    e = _f(ebitda); m = _pos(mult)
    if e is None or m is None:
        return None
    return e * m

def _price_from_pb(pb: float | None, bvps: float | None) -> float | None:
    p = _pos(pb); b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b

def _derive_eps_ttm_from_pe_only(price: float | None, pe_ttm: float | None,
                                 eps_ttm: float | None) -> tuple[float | None, str]:
    src_ttm = "source" if eps_ttm is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe_ttm"
    return eps_ttm, src_ttm

def _rev_million_to_units(v: float | None) -> float | None:
    x = _f(v)
    if x is None:
        return None
    try:
        return float(x) * 1_000_000.0
    except Exception:
        return None

def _eps_path_fill(eps_ttm: float | None, eps_1y: float | None, eps_2y: float | None,
                   eps_cagr_hist: float | None, eps_cagr_long: float | None,
                   rev_cagr_hist: float | None) -> tuple[float, float, float, float]:
    e0 = _pos(eps_ttm) or 0.0
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    g = None
    for cand in (eps_cagr_hist, eps_cagr_long, rev_cagr_hist, 0.0):
        if _f(cand) is not None:
            g = float(_f(cand))
            break
    if e1 is None:
        e1 = e0 * (1.0 + (g or 0.0))
    if e2 is None:
        e2 = (e1 or 0.0) * (1.0 + (g or 0.0))
    e3 = (e2 or 0.0) * (1.0 + (g or 0.0))
    return float(e0), float(e1), float(e2), float(e3)

def _rev_path(rev_ttm: float | None, rev_cagr_hist: float | None,
              rev1_manual_units: float | None, rev2_manual_units: float | None) -> tuple[float | None, float | None, float | None, float | None]:
    r0 = _pos(rev_ttm)
    if _pos(rev1_manual_units) and _pos(rev2_manual_units):
        return r0, float(rev1_manual_units), float(rev2_manual_units), float(rev2_manual_units) * (1.0 + float(_f(rev_cagr_hist) or 0.0))
    if _pos(rev1_manual_units) and (not _pos(rev2_manual_units)):
        g = float(_f(rev_cagr_hist) or 0.0)
        r1 = float(rev1_manual_units)
        r2 = r1 * (1.0 + g)
        r3 = r2 * (1.0 + g)
        return r0, r1, r2, r3
    g = float(_f(rev_cagr_hist) or 0.0)
    if r0 is None:
        return None, None, None, None
    r1 = r0 * (1.0 + g)
    r2 = r1 * (1.0 + g)
    r3 = r2 * (1.0 + g)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: float | None, rev0: float | None, rev1: float | None, rev2: float | None, rev3: float | None) -> tuple[float | None, float | None, float | None, float | None]:
    e0 = _f(ebitda_ttm)
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return e0, e0, e0, e0
    def scale(r):
        try:
            return (e0 * (r / rev0)) if (r and rev0) else e0
        except Exception:
            return e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# -------------------------
# Huvudmotor per rad
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: dict[str, str], fx_map: dict[str, float]) -> tuple[pd.DataFrame, str, dict[str, any]]:
    ticker = str(row.get("Ticker", "")).strip()

    snap   = fetch_yahoo_snapshot(ticker)
    time.sleep(0.12)
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.05)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)
    epscg_yh = fetch_yahoo_eps_cagr_hist(ticker)
    fh = fetch_finnhub_estimates(ticker)

    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs")))
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper()
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier")))
    net_debt = _nz(snap.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _nz(snap.get("revenue_ttm"), row.get("Rev TTM"))
    ebitda_ttm = _nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM"))
    eps_ttm    = _nz(snap.get("eps_ttm"), row.get("EPS TTM"))
    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B")))
    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS")))

    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), _nz(yh_eps.get("eps_1y"), fh.get("eps_1y"))))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), _nz(yh_eps.get("eps_2y"), fh.get("eps_2y"))))

    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), revcg_yh.get("rev_cagr")))
    rev_cagr_hist     = _clamp(rev_cagr_hist_raw, REV_CAGR_MIN, REV_CAGR_MAX)

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), epscg_yh.get("eps_cagr")))
    eps_cagr_hist     = _clamp(eps_cagr_hist_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    eps_ttm, src_eps_ttm = _derive_eps_ttm_from_pe_only(price, pe_ttm, _f(eps_ttm))

    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    rev1_manual_units = _rev_million_to_units(_f(row.get("Rev 1Y")))
    rev2_manual_units = _rev_million_to_units(_f(row.get("Rev 2Y")))
    r0, r1, r2, r3 = _rev_path(_f(rev_ttm), rev_cagr_hist, rev1_manual_units, rev2_manual_units)

    eps_cagr_long = _clamp(_f(yh_eps.get("eps_cagr_long")), EPS_CAGR_MIN, EPS_CAGR_MAX)
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr_hist, eps_cagr_long, rev_cagr_hist)

    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,     _decay_multiple(p_b,     1, decay), _decay_multiple(p_b,     2, decay), _decay_multiple(p_b,     3, decay)

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

    src = snap.get("sources", {}) or {}
    eps1_src = ("sheet" if _pos(row.get("EPS 1Y")) else
                ("yahoo_trend" if _pos(yh_eps.get("eps_1y")) else
                 ("finnhub" if _pos(fh.get("eps_1y")) else "filled_by_rule")))
    eps2_src = ("sheet" if _pos(row.get("EPS 2Y")) else
                ("yahoo_trend" if _pos(yh_eps.get("eps_2y")) else
                 ("finnhub" if _pos(fh.get("eps_2y")) else "filled_by_rule")))
    revc_src = ("sheet" if _f(row.get("Rev CAGR")) is not None else
                ("yahoo_financials" if revcg_yh.get("rev_cagr") is not None else "none"))
    epsc_src = ("sheet" if _f(row.get("EPS CAGR")) is not None else
                ("yahoo_financials" if epscg_yh.get("eps_cagr") is not None else "none"))

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if (e0 or e0==0) else '—'}({src.get('eps_ttm','?')}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_cagr_hist={'ok' if _f(rev_cagr_hist) is not None else '—'}({revc_src}), "
        f"eps_cagr_hist={'ok' if _f(eps_cagr_hist) is not None else '—'}({epsc_src}), "
        f"ebitda_ttm={'ok' if (b0 or b0==0) else '—'}({src.get('ebitda_ttm','?')}), "
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
        "company_name": snap.get("company_name"),
        "sector": snap.get("sector"),
        "industry": snap.get("industry"),
        "annual_dividend": snap.get("annual_dividend"),
        "dividend_frequency": snap.get("dividend_frequency"),
        "sources": {
            **src,
            "eps_1y_source": eps1_src,
            "eps_2y_source": eps2_src,
            "rev_cagr_source": revc_src,
            "eps_cagr_source": epsc_src,
        },
        "cagr_clamped": {
            "rev_cagr_used": _f(_clamp(rev_cagr_hist_raw, REV_CAGR_MIN, REV_CAGR_MAX)),
            "eps_cagr_used": _f(_clamp(eps_cagr_hist_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)),
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# -------------------------
# NYTT: Bygg uppdateringar från Yahoo till Data-bladet
# -------------------------
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series | dict | None = None):
    """
    Returnerar (updates_dict, meta, methods_df) för en ticker.
    Skrivs senare in med _apply_updates_to_df_row.
    """
    ex = pd.Series(existing_row) if existing_row is not None else pd.Series({"Ticker": ticker})
    settings = get_settings_map()
    fx_map   = get_fx_map()

    # Beräkna metoder & meta
    try:
        methods_df, sanity, meta = compute_methods_for_row(ex, settings, fx_map)
    except Exception as e:
        # Faller tillbaka till endast snapshot
        meta = {}
        methods_df = pd.DataFrame(columns=["Metod","Idag","1 år","2 år","3 år"])

    snap = fetch_yahoo_snapshot(ticker)
    eps_est = fetch_yahoo_eps_estimates(ticker)
    revcg   = fetch_yahoo_rev_cagr(ticker)
    epscg   = fetch_yahoo_eps_cagr_hist(ticker)

    # Välj metod för riktkurser (default: pe_hist_vs_eps)
    md = None
    try:
        mdf = methods_df.set_index("Metod")
        md = mdf.loc["pe_hist_vs_eps"] if "pe_hist_vs_eps" in mdf.index else mdf.iloc[0]
    except Exception:
        md = None

    updates: dict[str, any] = {}

    # Grunddata
    if snap.get("company_name"): updates["Bolagsnamn"] = snap["company_name"]
    if snap.get("sector"):       updates["Sektor"] = snap["sector"]
    if snap.get("currency"):     updates["Valuta"] = str(snap["currency"]).upper()
    if _pos(snap.get("price")):  updates["Aktuell kurs"] = float(snap["price"])
    if _pos(snap.get("shares")): updates["Utestående aktier"] = float(snap["shares"])
    if snap.get("net_debt") is not None: updates["Net debt"] = float(snap["net_debt"])

    # Nyckeltal TTM/multiplar
    mapping = {
        "Rev TTM": "revenue_ttm",
        "EBITDA TTM": "ebitda_ttm",
        "EPS TTM": "eps_ttm",
        "PE TTM": "pe_ttm",
        "PE FWD": "pe_fwd",
        "EV/Revenue": "ev_to_sales",
        "EV/EBITDA": "ev_to_ebitda",
        "P/B": "p_to_book",
        "BVPS": "bvps",
        "Årlig utdelning": "annual_dividend",
        "Utdelningsfrekvens": "dividend_frequency",
    }
    for col, key in mapping.items():
        val = snap.get(key)
        if val is not None and (not isinstance(val, float) or val == val):
            updates[col] = float(val) if isinstance(val, (int, float)) else val

    # Estimat/CAGR
    if _pos(eps_est.get("eps_1y")): updates["EPS 1Y"] = float(eps_est["eps_1y"])
    if _pos(eps_est.get("eps_2y")): updates["EPS 2Y"] = float(eps_est["eps_2y"])
    if _f(revcg.get("rev_cagr")) is not None: updates["Rev CAGR"] = float(revcg["rev_cagr"])
    if _f(epscg.get("eps_cagr")) is not None: updates["EPS CAGR"] = float(epscg["eps_cagr"])

    # Riktkurser (avrundning sker i UI; här sparar vi råa värden)
    if md is not None:
        for k_sheet, k_md in [("Riktkurs idag","Idag"),("Riktkurs 1 år","1 år"),("Riktkurs 2 år","2 år"),("Riktkurs 3 år","3 år")]:
            v = _f(md.get(k_md))
            if v is not None:
                updates[k_sheet] = float(v)

    # Stämplar
    updates["Auto källa"] = "Yahoo Finance"
    updates["Senast auto uppdaterad"] = now_stamp()

    # Snapshot-logg (valfritt – spara riktkurser)
    try:
        if md is not None:
            _append_rows(SNAPSHOT_TITLE, [[
                now_stamp(), ticker, updates.get("Valuta","USD"), "pe_hist_vs_eps",
                _f(md.get("Idag")), _f(md.get("1 år")), _f(md.get("2 år")), _f(md.get("3 år")),
                _f(meta.get("pe_anchor")), _f(meta.get("decay"))
            ]])
    except Exception:
        pass

    return updates, meta, methods_df

# -------------------------
# NYTT: Applicera updates i en DF-rad
# -------------------------
def _apply_updates_to_df_row(df: pd.DataFrame, idx, updates: dict) -> int:
    """
    Skriv endast fält som har värde (ej None/NaN/""), returnera antal ändrade fält.
    """
    if not updates:
        return 0
    changed = 0
    for k, v in updates.items():
        if v is None:
            continue
        if isinstance(v, float) and (v != v):  # NaN
            continue
        old = df.at[idx, k] if (k in df.columns) else np.nan
        # skapa kolumn vid behov
        if k not in df.columns:
            df[k] = np.nan
        if (pd.isna(old) and not pd.isna(v)) or (not pd.isna(old) and not pd.isna(v) and str(old) != str(v)):
            df.at[idx, k] = v
            changed += 1
    return changed

# ============================================================
# app.py — Del 3/4
# Editor-vy: manuell inmatning + radvis Yahoo-uppdatering
#  • Robust sparning av EPS 1Y / EPS 2Y
#  • Revenue-inmatning i MILJARDER (konverteras till "Rev 1Y/2Y" i MILJONER)
#  • Rekalkylering av riktkurser från manuella fält (utan Yahoo)
#  • Radvis "Hämta från Yahoo" (använder _build_updates_from_yahoo + _apply_updates_to_df_row)
#  • Skydd mot None/NaN -> inga fler float(NoneType)-fel
# ============================================================

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------
# Hjälpare (lokala för Editor-vyn)
# ---------------------------------------
def _swe_to_float(txt: str | float | int | None) -> float | None:
    """
    Robust svensk talparser:
      - tillåter komma som decimaltecken
      - tar bort mellanslag
      - hanterar tomma strängar -> None
      - låter redan-floats passera
    """
    if txt is None:
        return None
    if isinstance(txt, (int, float)):
        try:
            v = float(txt)
            if v == v and np.isfinite(v):
                return v
            return None
        except Exception:
            return None
    s = str(txt).strip()
    if s == "":
        return None
    s = s.replace(" ", "").replace("\u00a0", "")  # vanliga/icke-brytande mellanslag
    # stöd för tusentalspunkt + decimalkomma
    if "," in s and "." in s:
        # om både . och , finns – anta . = tusen, , = decimal
        s = s.replace(".", "").replace(",", ".")
    else:
        # bara komma – tolkas som decimal
        s = s.replace(",", ".")
    try:
        v = float(s)
        return v if (v == v and np.isfinite(v)) else None
    except Exception:
        return None

def _fmt_or_empty(v) -> str:
    if v is None or (isinstance(v, float) and (not np.isfinite(v))):
        return ""
    try:
        return str(v)
    except Exception:
        return ""

def _rev_mdr_to_miljoner(x: float | None) -> float | None:
    """
    Convert från MILJARDER till MILJONER.
    Editor tar emot miljarder (användarvänligt),
    men databladets "Rev 1Y"/"Rev 2Y" lagras i MILJONER.
    """
    if x is None:
        return None
    try:
        return float(x) * 1_000.0
    except Exception:
        return None

def _rev_miljoner_to_mdr(x: float | None) -> float | None:
    """Visa tillbaka som MILJARDER i editorfältet om vi läser från bladet."""
    if x is None:
        return None
    try:
        return float(x) / 1_000.0
    except Exception:
        return None

def _recalc_targets_for_row(df: pd.DataFrame, idx) -> dict[str, float | None]:
    """
    Kör beräkningsmotorn utifrån aktuell DF-rad (utan ny Yahoo-hämtning)
    och returnerar en dict med nya riktkurser (Idag/1/2/3 år) – tar den
    första metoden ('pe_hist_vs_eps') om tillgänglig.
    """
    try:
        row = df.loc[idx]
        settings = get_settings_map()
        fx_map   = get_fx_map()
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
        if methods_df is None or methods_df.empty:
            return {}

        mdf = methods_df.set_index("Metod")
        if "pe_hist_vs_eps" in mdf.index:
            chosen = mdf.loc["pe_hist_vs_eps"]
        else:
            chosen = mdf.iloc[0]

        return {
            "Riktkurs idag": _f(chosen.get("Idag")),
            "Riktkurs 1 år": _f(chosen.get("1 år")),
            "Riktkurs 2 år": _f(chosen.get("2 år")),
            "Riktkurs 3 år": _f(chosen.get("3 år")),
        }
    except Exception as e:
        st.warning(f"Rekalkylering misslyckades: {e}")
        return {}

# ---------------------------------------
# Editor-vy
# ---------------------------------------
def render_editor_view():
    st.header("✍️ Editor – manuella fält & radvis uppdatering")

    if "DATA" not in st.session_state or st.session_state["DATA"] is None or st.session_state["DATA"].empty:
        st.info("Ingen data laddad ännu. Kontrollera **Del 1/4** att Google Sheets lästes in till `st.session_state['DATA']`.")
        return

    df: pd.DataFrame = st.session_state["DATA"]
    # Säkerställ obligatoriska kolumner finns i DF (om saknas skapas de som NaN)
    for col in ["Ticker","EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y",
                "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
                "Aktuell kurs","Valuta","Bolagsnamn","Sektor","Utestående aktier"]:
        if col not in df.columns:
            df[col] = np.nan

    tickers = df["Ticker"].astype(str).fillna("").tolist()
    preselect = 0
    try:
        if "EDITOR_SELECTED_TICKER" in st.session_state and st.session_state["EDITOR_SELECTED_TICKER"] in tickers:
            preselect = tickers.index(st.session_state["EDITOR_SELECTED_TICKER"])
    except Exception:
        pass

    sel = st.selectbox("Välj bolag (Ticker)", tickers, index=preselect if tickers else 0, key="EDITOR_SELECTBOX")

    if not sel:
        st.stop()

    # Index för vald ticker
    try:
        idx = df.index[df["Ticker"].astype(str) == str(sel)].tolist()[0]
    except Exception:
        st.error("Kunde inte hitta vald rad i DataFrame.")
        st.stop()

    st.session_state["EDITOR_SELECTED_TICKER"] = sel

    row = df.loc[idx]

    # Visa nyckelfält
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Ticker", str(row.get("Ticker","")))
        st.caption(f"Namn: {row.get('Bolagsnamn') or '—'}")
    with c2:
        st.metric("Valuta", str(row.get("Valuta","") or "—"))
        st.caption(f"Sektor: {row.get('Sektor') or '—'}")
    with c3:
        st.metric("Kurs", f"{_f(row.get('Aktuell kurs')) or '—'}")
        st.caption(f"Utest. aktier: {_f(row.get('Utestående aktier')) or '—'}")
    with c4:
        st.caption("Senaste riktkurser:")
        st.write(
            f"Idag: {_f(row.get('Riktkurs idag')) or '—'}\n\n"
            f"1 år: {_f(row.get('Riktkurs 1 år')) or '—'}\n\n"
            f"2 år: {_f(row.get('Riktkurs 2 år')) or '—'}\n\n"
            f"3 år: {_f(row.get('Riktkurs 3 år')) or '—'}"
        )

    st.divider()

    st.subheader("Manuell inmatning")
    st.caption("• **EPS** anges per aktie.\n• **Omsättning** anges i **MILJARDER** (vi konverterar automatiskt till miljoner i databladet).")

    # Förifyll från DF (konvertera Rev-miljoner -> miljarder i editorn)
    eps1_prefill = _fmt_or_empty(_f(row.get("EPS 1Y")))
    eps2_prefill = _fmt_or_empty(_f(row.get("EPS 2Y")))
    rev1_mdr_prefill = _fmt_or_empty(_rev_miljoner_to_mdr(_f(row.get("Rev 1Y"))))
    rev2_mdr_prefill = _fmt_or_empty(_rev_miljoner_to_mdr(_f(row.get("Rev 2Y"))))

    with st.form("editor_manual_form", clear_on_submit=False):
        cc1, cc2 = st.columns(2)
        with cc1:
            eps1_txt = st.text_input("EPS 1Y (per aktie)", value=eps1_prefill, key="EPS1Y_INPUT")
            rev1_txt = st.text_input("Omsättning nästa år (MILJARDER)", value=rev1_mdr_prefill, key="REV1Y_INPUT")
        with cc2:
            eps2_txt = st.text_input("EPS 2Y (per aktie)", value=eps2_prefill, key="EPS2Y_INPUT")
            rev2_txt = st.text_input("Omsättning om 2 år (MILJARDER)", value=rev2_mdr_prefill, key="REV2Y_INPUT")

        recalc_targets = st.checkbox("Rekalkylera riktkurser direkt efter spar", value=True)
        do_save = st.form_submit_button("💾 Spara manuella fält")

    if do_save:
        try:
            # Parse input
            eps1 = _swe_to_float(eps1_txt)
            eps2 = _swe_to_float(eps2_txt)
            rev1_mdr = _swe_to_float(rev1_txt)
            rev2_mdr = _swe_to_float(rev2_txt)

            # Konvertera revenue till MILJONER innan vi sparar till DF (viktigt för Del 2/4-beräkningar)
            rev1_milj = _rev_mdr_to_miljoner(rev1_mdr)
            rev2_milj = _rev_mdr_to_miljoner(rev2_mdr)

            updates = {}
            # OBS: Vi skriver bara fält som användaren faktiskt fyllt (None => rör inte befintligt)
            if eps1 is not None:     updates["EPS 1Y"] = float(eps1)
            if eps2 is not None:     updates["EPS 2Y"] = float(eps2)
            if rev1_milj is not None: updates["Rev 1Y"] = float(rev1_milj)
            if rev2_milj is not None: updates["Rev 2Y"] = float(rev2_milj)

            changed = _apply_updates_to_df_row(df, idx, updates)

            # Rekalkylera riktkurser direkt om valt
            if recalc_targets:
                targets = _recalc_targets_for_row(df, idx)
                if targets:
                    changed += _apply_updates_to_df_row(df, idx, targets)

            st.session_state["DATA"] = df
            st.success(f"Sparat! Uppdaterade {changed} fält för {sel}.")
        except Exception as e:
            st.error(f"Kunde inte spara: {e}")

    st.divider()

    cL, cR = st.columns([1,1])
    with cL:
        if st.button("⬇️ Hämta från Yahoo (endast vald rad)"):
            try:
                updates, meta, methods_df = _build_updates_from_yahoo(sel, existing_row=df.loc[idx])
                changed = _apply_updates_to_df_row(df, idx, updates)
                st.session_state["DATA"] = df
                st.success(f"Yahoo klart. Uppdaterade {changed} fält för {sel}.")
                if methods_df is not None and not methods_df.empty:
                    st.write("Beräkningsmetoder (preview):")
                    st.dataframe(methods_df, use_container_width=True)
            except Exception as e:
                st.error(f"Hämtning från Yahoo misslyckades: {e}")

    with cR:
        if st.button("🧮 Rekalkylera riktkurser från manuella fält"):
            try:
                targets = _recalc_targets_for_row(df, idx)
                if not targets:
                    st.warning("Inga riktkurser kunde räknas fram (saknar underlag).")
                else:
                    changed = _apply_updates_to_df_row(df, idx, targets)
                    st.session_state["DATA"] = df
                    st.success(f"Rekalkylerat. Uppdaterade {changed} fält.")
            except Exception as e:
                st.error(f"Rekalkylering misslyckades: {e}")

    st.divider()
    with st.expander("Visa DF-rad (debug)"):
        st.write(df.loc[idx:idx])

# Slut på Del 3/4

# ============================================================
# app.py — Del 4/4
# Helpers för Yahoo-uppdatering + säker DataFrame-skrivning
#  • _build_updates_from_yahoo(ticker, existing_row)
#  • _apply_updates_to_df_row(df, idx, updates)  → antal fält som ändrades
#  • Liten wrapper för Editor: page_editor(...) → render_editor_view()
# ============================================================

from __future__ import annotations
import math
import numpy as np
import pandas as pd
import streamlit as st

# -----------------------------
# Säker uppdatering av DF-rad
# -----------------------------
def _apply_updates_to_df_row(df: pd.DataFrame, idx, updates: dict) -> int:
    """
    Skriver endast värden som inte är None/NaN. Lägger till kolumn om den saknas.
    Returnerar antal fält som faktiskt ändrades (värdet skilde sig).
    """
    if updates is None:
        return 0
    changed = 0
    for k, v in updates.items():
        # hoppa över None/NaN
        if v is None or (isinstance(v, float) and (not math.isfinite(v))):
            continue
        # säkerställ kolumn
        if k not in df.columns:
            df[k] = np.nan
        old = df.at[idx, k] if idx in df.index else np.nan
        # jämför med tolerans om numeriskt
        try:
            old_f = float(old)
            new_f = float(v)
            equal = (math.isfinite(old_f) and math.isfinite(new_f) and abs(old_f - new_f) < 1e-12)
        except Exception:
            equal = (str(old) == str(v))
        if not equal:
            df.at[idx, k] = v
            changed += 1
    return changed

# -----------------------------
# Bygg uppdateringar från Yahoo
# -----------------------------
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series | None = None):
    """
    Hämtar snapshot + estimat + CAGR från Yahoo och bygger ett 'updates'-dict
    som kan skrivas in i Data-bladet. Returnerar (updates, meta, methods_df).
    Sätter även stämplar för auto-uppdatering.
    """
    # 1) Hämta live
    snap    = fetch_yahoo_snapshot(ticker)
    eps_tr  = fetch_yahoo_eps_estimates(ticker)
    rev_cg  = fetch_yahoo_rev_cagr(ticker)
    eps_cg  = fetch_yahoo_eps_cagr_hist(ticker)

    # 2) Mappa till Data-kolumner
    updates = {
        "Valuta":                 (snap.get("currency") or (existing_row.get("Valuta") if existing_row is not None else None)),
        "Aktuell kurs":           _f(snap.get("price")),
        "Bolagsnamn":             snap.get("company_name"),
        "Sektor":                 snap.get("sector"),
        "Utestående aktier":      _f(snap.get("shares")),
        "Net debt":               _f(snap.get("net_debt")),
        "Rev TTM":                _f(snap.get("revenue_ttm")),
        "EBITDA TTM":             _f(snap.get("ebitda_ttm")),
        "EPS TTM":                _f(snap.get("eps_ttm")),
        "PE TTM":                 _f(snap.get("pe_ttm")),
        "PE FWD":                 _f(snap.get("pe_fwd")),
        "EV/Revenue":             _f(snap.get("ev_to_sales")),
        "EV/EBITDA":              _f(snap.get("ev_to_ebitda")),
        "P/B":                    _f(snap.get("p_to_book")),
        "BVPS":                   _f(snap.get("bvps")),
        "Årlig utdelning":        _f(snap.get("annual_dividend")),
        "Utdelningsfrekvens":     snap.get("dividend_frequency"),
        "EPS 1Y":                 _f(eps_tr.get("eps_1y")),
        "EPS 2Y":                 _f(eps_tr.get("eps_2y")),
        "Rev CAGR":               _f(rev_cg.get("rev_cagr")),
        "EPS CAGR":               _f(eps_cg.get("eps_cagr")),
        "Senast auto uppdaterad": now_stamp(),
        "Auto källa":             "Yahoo Finance",
    }

    # 3) Beräkningsmetoder (för UI/snapshot); bygg en temporär rad = existing + updates
    if existing_row is None:
        existing_row = pd.Series({"Ticker": ticker})
    temp_row = existing_row.copy()
    temp_row["Ticker"] = ticker
    for k, v in updates.items():
        if v is not None:
            temp_row[k] = v

    settings = get_settings_map()
    fx_map   = get_fx_map()
    methods_df, sanity, meta = compute_methods_for_row(temp_row, settings, fx_map)

    return updates, meta, methods_df

# -----------------------------
# Editor-wrapper (om main anropar page_editor)
# -----------------------------
def page_editor(df_data: pd.DataFrame, settings: dict):
    # Anropa den editor som definierades i Del 3/4
    return render_editor_view()

# =======================
# (Slut Del 4/4)
# =======================
