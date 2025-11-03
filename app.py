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
DATA_COLUMNS = [
    "Timestamp","Ticker","Bolagsnamn","Sektor","Bucket","Valuta",
    "Antal aktier","GAV (SEK)","Aktuell kurs",
    "Utestående aktier","Net debt",
    "Rev TTM","EBITDA TTM","EPS TTM",
    "PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
    "EPS 1Y","EPS 2Y","Rev CAGR","EPS CAGR",
    "Årlig utdelning","Utdelning CAGR",
    # Utdelningslista
    "Utdelningsfrekvens",                # "M","Q","S","A"
    "Nästa utdelningsdatum",             # YYYY-MM-DD
    "Nästa utdelning (per aktie)",       # DPS nästa
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

    # --- Ignorera nollor (tolka 0 som NaN) för auto-hämtade fält ---
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

    for c in IGNORE_ZERO_COLS:
        if c in df.columns:
            df.loc[(df[c].notna()) & (df[c] == 0), c] = np.nan
    # OBS: Vi låter 'Antal aktier' och 'GAV (SEK)' vara 0 om du har watchlist/ej äger.

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
# app.py — Del 2/4 — 2A
# Hjälpfunktioner + Förbättrad Yahoo-hämtning (snapshot)
#  • Robust TTM via kvartalssummor (EPS/Revenue/EBITDA)
#  • EV/net debt från info + balance sheet fallback
#  • BVPS/PB via kalkyl om saknas
#  • Utdelning & frekvens (infer) som metadata
# ============================================================

import requests
import pandas as pd
import numpy as np
import streamlit as st
# OBS: yfinance (yf), math, os och hjälpfunktionerna _f, _pos, _nz, _env_or_secret
# antas vara importerade/definierade i Del 1/4 enligt basversionen.

# -------------------------
# Små hjälpare (index-pick, TTM-summerare)
# -------------------------
def _ix_pick(df: pd.DataFrame, candidates: list[str]):
    """Hitta rad i df (index) via kandidater — case/space-insensitivt."""
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
    # mjuk sökning "contains"
    for k, orig in norm.items():
        for cand in candidates:
            if cand.replace(" ", "").lower() in k:
                try:
                    return df.loc[orig]
                except Exception:
                    pass
    return None

def _sum_last4(ser_like):
    """Summera de 4 senaste datapunkterna (för kvartalsserier)."""
    try:
        s = pd.to_numeric(pd.Series(ser_like), errors="coerce").dropna()
        if s.empty:
            return None
        # säkerställ kronologisk ordning (datumkolumner)
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
    """Summera senaste 4 kvartalens EPS (Diluted/Basic)."""
    return _sum_last4(ser_like)

# -------------------------
# Yahoo (yfinance) – robust snapshot
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> dict[str, any]:
    """
    Hämtar pris/valuta + nyckeltal från Yahoo.
    TTM byggs primärt från kvartalssummor.
    Keys (urval):
      price, currency, market_cap, ev, shares,
      revenue_ttm, ebitda_ttm, eps_ttm,
      ev_to_sales, ev_to_ebitda, pe_ttm, pe_fwd,
      p_to_book, bvps, net_debt, company_name, sector, industry, sources={}
    """
    tk = yf.Ticker(ticker)
    out: dict[str, any] = {"sources": {}}

    # --- Snabbkanal för pris/valuta/MCAP/shares
    try:
        fi = tk.fast_info
        out["price"]      = _f(getattr(fi, "last_price", None));      out["sources"]["price"] = "yahoo_fast"
        out["currency"]   = getattr(fi, "currency", None);            out["sources"]["currency"] = "yahoo_fast"
        out["market_cap"] = _f(getattr(fi, "market_cap", None));      out["sources"]["market_cap"] = "yahoo_fast"
        out["shares"]     = _f(getattr(fi, "shares", None));          out["sources"]["shares"] = "yahoo_fast"
    except Exception:
        pass

    # --- info()-fallbacks
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k):
        try:
            return info.get(k)
        except Exception:
            return None

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
    set_if_missing("eps_ttm",      gi("trailingEps"),         "yahoo_info")   # kan bli överskriven

    # >>> EXTRA: shares-fallback via info (sharesOutstanding)
    if out.get("shares") is None:
        so = _f(gi("sharesOutstanding"))
        if so is not None:
            out["shares"] = so
            out["sources"]["shares"] = "yahoo_info_sharesOutstanding"

    # >>> EXTRA: derivat av PE-talet om saknas
    if out.get("pe_ttm") is None and _pos(out.get("price")) and _pos(out.get("eps_ttm")):
        try:
            out["pe_ttm"] = float(out["price"]) / float(out["eps_ttm"])
            out["sources"]["pe_ttm"] = "calc_price/eps_ttm"
        except Exception:
            pass

    # >>> EXTRA: framåtblickande PE via forwardEPS om forwardPE saknas
    if out.get("pe_fwd") is None and _pos(out.get("price")):
        fwd_eps = _f(gi("forwardEps") or gi("forwardEPS"))
        if _pos(fwd_eps):
            try:
                out["pe_fwd"] = float(out["price"]) / float(fwd_eps)
                out["sources"]["pe_fwd"] = "calc_price/forwardEPS"
            except Exception:
                pass

    # Namn/sector/industry
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

    # --- EV / net debt (info + derivat)
    total_debt = _f(gi("totalDebt"))
    total_cash = _f(gi("totalCash"))
    ev_info    = _f(gi("enterpriseValue"))

    if ev_info is not None:
        out["ev"] = ev_info; out["sources"]["ev"] = "yahoo_info"
    elif _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
        out["sources"]["ev"] = "calc_mc+debt-cash"

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]
        out["sources"]["net_debt"] = "calc_ev-mcap"

    # --- Shares fallback via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            out["sources"]["shares"] = "derived_mcap/price"
        except Exception:
            pass

    # --- Pris-historik fallback
    if not _pos(out.get("price")):
        try:
            hist = tk.history(period="5d")
            if not hist.empty:
                out["price"] = float(hist["Close"].dropna().iloc[-1])
                out["sources"]["price"] = "yahoo_hist_close"
        except Exception:
            pass

    # >>> Balance Sheet-fallbacks för Debt/Cash samt BVPS/P/B
    bs_q = None
    try:
        bs_q = tk.get_balance_sheet(freq="quarterly")
    except Exception:
        bs_q = getattr(tk, "quarterly_balance_sheet", None) or getattr(tk, "balance_sheet", None)

    if bs_q is not None and not getattr(bs_q, "empty", True):
        # debt/cash om info() saknade
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
        # Net debt/EV igen om saknas
        if out.get("ev") is None and _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
            out["ev"] = out["market_cap"] + total_debt - total_cash
            out["sources"]["ev"] = "calc_mc+debt-cash(bs)"
        if out.get("net_debt") is None and out.get("ev") is not None and out.get("market_cap") is not None:
            out["net_debt"] = out["ev"] - out["market_cap"]
            out["sources"]["net_debt"] = "calc_ev-mcap(bs)"
        # BVPS (book value per share)
        eq_row = _ix_pick(bs_q, ["StockholdersEquity", "TotalStockholderEquity", "Total Stockholder Equity"])
        if out.get("bvps") is None and eq_row is not None and _pos(out.get("shares")):
            try:
                eq_last = float(pd.to_numeric(pd.Series(eq_row), errors="coerce").dropna().iloc[-1])
                out["bvps"] = eq_last / float(out["shares"])
                out["sources"]["bvps"] = "calc_equity/shares(balance_sheet_q)"
            except Exception:
                pass
        # P/B från pris och BVPS
        if out.get("p_to_book") is None and _pos(out.get("price")) and _pos(out.get("bvps")):
            try:
                out["p_to_book"] = float(out["price"]) / float(out["bvps"])
                out["sources"]["p_to_book"] = "calc_price/bvps"
            except Exception:
                pass

    # --- TTM via kvartal (income statement quarterly)
    EPS_KEYS_Q     = ["DilutedEPS", "BasicEPS", "EPS"]
    REV_KEYS_Q     = ["TotalRevenue", "Total Revenue", "Revenue"]
    EBITDA_KEYS_Q  = ["Ebitda", "EBITDA", "EarningsBeforeInterestTaxesDepreciationAmortization"]

    try:
        try:
            inc_q = tk.get_income_stmt(freq="quarterly")
        except Exception:
            inc_q = getattr(tk, "quarterly_income_stmt", None) or getattr(tk, "income_stmt", None)

        if inc_q is not None and not getattr(inc_q, "empty", True):
            dfq = inc_q.copy()

            # EPS TTM
            eps_row = _ix_pick(dfq, EPS_KEYS_Q)
            eps_ttm_q = _sum_eps_last4(eps_row) if eps_row is not None else None

            # Revenue TTM
            rev_row = _ix_pick(dfq, REV_KEYS_Q)
            rev_ttm_q = _sum_last4(rev_row) if rev_row is not None else None

            # EBITDA TTM
            ebitda_row = _ix_pick(dfq, EBITDA_KEYS_Q)
            ebitda_ttm_q = _sum_last4(ebitda_row) if ebitda_row is not None else None

            # Om EPS saknas som rad, försök NetIncome / Shares
            if eps_ttm_q is None:
                net_row = _ix_pick(dfq, ["NetIncome", "Net Income", "NetIncomeApplicableToCommonShares", "NetIncomeCommonStockholders"])
                shd_row = _ix_pick(dfq, ["DilutedAverageShares", "Diluted Shares", "AverageDilutedSharesOutstanding", "WeightedAverageDilutedSharesOutstanding"])
                if net_row is not None and shd_row is not None:
                    ni_ttm = _sum_last4(net_row)
                    sh_ttm = _sum_last4(shd_row)
                    if _pos(ni_ttm) is not None and _pos(sh_ttm) is not None and sh_ttm != 0:
                        eps_ttm_q = float(ni_ttm) / float(sh_ttm)

            # Skriv in TTM från kvartal om de finns (överskriv info()-värden vid diff)
            if _pos(eps_ttm_q) is not None:
                out["eps_ttm"] = float(eps_ttm_q)
                out["sources"]["eps_ttm"] = "yahoo_quarterly_TTM"

            if _pos(rev_ttm_q) is not None:
                out["revenue_ttm"] = float(rev_ttm_q)
                out["sources"]["revenue_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")

            if ebitda_ttm_q is not None:  # EBITDA kan vara <=0
                out["ebitda_ttm"] = float(ebitda_ttm_q)
                out["sources"]["ebitda_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("ebitda_ttm", gi("ebitda"), "yahoo_info")
    except Exception:
        # Fallback direkt från info om kvartalssidan bråkade
        set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")
        set_if_missing("ebitda_ttm",  gi("ebitda"),        "yahoo_info")

    # --- Härled multiplar om möjligt
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

    # >>> Årlig utdelning (per aktie) från Yahoo info
    fwd_div = _f(gi("dividendRate") or gi("forwardAnnualDividendRate"))
    trl_div = _f(gi("trailingAnnualDividendRate"))
    if out.get("annual_dividend") is None and (fwd_div is not None or trl_div is not None):
        out["annual_dividend"] = float(_nz(fwd_div, trl_div))
        out["sources"]["annual_dividend"] = "yahoo_info"

    # >>> Gissa utdelningsfrekvens utifrån historik (om finns)
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

    # Normalisera valuta
    out["currency"] = str(out.get("currency") or "USD").upper()
    return out

# ===== Slut på Del 2/4 — 2A. Fortsätt med 2B i nästa del. =====

# ============================================================
# app.py — Del 2/4 — 2B
# EPS/REV-estimat (Yahoo) + valfri Finnhub fallback,
# multipel-decay, builders och compute_methods_for_row
# ============================================================

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> dict[str, float | None]:
    """
    Plockar EPS currentYear/nextYear från Yahoo earnings trend.
    Returnerar: {"eps_1y": float|None, "eps_2y": float|None,
                 "eps_cagr_long": float|None, "source": "..."}
    *Härleder INTE EPS 1Y från PE FWD.*
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

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    """CAGR på intäkter från årliga statements (Yahoo)."""
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
            ser.index = pd.to_datetime(ser.index, errors="coerce")
            ser = ser.sort_index()
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

        cagr = None
        try:
            cagr = (vals[-1] / vals[0]) ** (1.0 / n_years) - 1.0
        except Exception:
            cagr = None

        return {"rev_cagr": cagr, "years": n_years, "source": "yahoo_financials"}
    except Exception:
        return {"rev_cagr": None, "years": None, "source": "none"}

# -------------------------
# Finnhub (valfritt) – EPS-estimat fallback (oförändrat om redan fanns)
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
# Multipel-decay & P/E-ankare
# -------------------------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.40   # +40 %

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

# -------------------------
# Builders (pris/EV)
# -------------------------
def _equity_price_from_ev(ev_target: float | None, net_debt: float | None, shares_fd: float | None) -> float | None:
    e = _pos(ev_target)
    s = _pos(shares_fd)
    if e is None or s is None:
        return None
    nd = _nz(net_debt, 0.0)
    try:
        return max(0.0, (e - nd) / s)
    except Exception:
        return None

def _price_from_pe(eps: float | None, pe: float | None) -> float | None:
    e = _pos(eps)
    p = _pos(pe)
    if e is None or p is None:
        return None
    return e * p

def _ev_from_sales(rev: float | None, mult: float | None) -> float | None:
    r = _pos(rev)
    m = _pos(mult)
    if r is None or m is None:
        return None
    return r * m

def _ev_from_ebitda(ebitda: float | None, mult: float | None) -> float | None:
    e = _f(ebitda)  # får vara negativ/0
    m = _pos(mult)
    if e is None or m is None:
        return None
    return e * m

def _price_from_pb(pb: float | None, bvps: float | None) -> float | None:
    p = _pos(pb)
    b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b

# -------------------------
# EPS/REV/EBITDA paths + härledning
# -------------------------
def _derive_eps_ttm_from_pe_only(price: float | None, pe_ttm: float | None,
                                 eps_ttm: float | None) -> tuple[float | None, str]:
    src_ttm = "source" if eps_ttm is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe_ttm"
    return eps_ttm, src_ttm

def _eps_path_fill(eps_ttm: float | None, eps_1y: float | None, eps_2y: float | None,
                   eps_cagr: float | None, rev_cagr_fallback: float | None) -> tuple[float, float, float, float]:
    e0 = _pos(eps_ttm) or 0.0
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    g  = _f(eps_cagr)

    if g is None:
        g = _f(rev_cagr_fallback)
    if g is None:
        g = 0.0

    if e1 is None:
        e1 = e0 * (1.0 + g)
    if e2 is None:
        e2 = e1 * (1.0 + g)
    e3 = e2 * (1.0 + g)

    return float(e0), float(e1), float(e2), float(e3)

def _rev_path(rev_ttm: float | None, rev_cagr: float | None) -> tuple[float | None, float | None, float | None, float | None]:
    r0 = _pos(rev_ttm)
    cg = _f(rev_cagr)
    if r0 is None or cg is None:
        return r0, None, None, None
    r1 = r0 * (1.0 + cg)
    r2 = r1 * (1.0 + cg)
    r3 = r2 * (1.0 + cg)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: float | None, rev0: float | None, rev1: float | None, rev2: float | None, rev3: float | None) -> tuple[float | None, float | None, float | None, float | None]:
    e0 = _f(ebitda_ttm)  # kan vara negativt
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
# Huvudmotor per rad (värderingsmetoder)
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: dict[str, str], fx_map: dict[str, float]) -> tuple[pd.DataFrame, str, dict[str, any]]:
    ticker = str(row.get("Ticker", "")).strip()

    # 1) Live-data
    snap   = fetch_yahoo_snapshot(ticker)  # >>> förbättrad insida av funktionen
    time.sleep(0.15)  # mild throttling
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.06)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)
    fh = fetch_finnhub_estimates(ticker)  # fallback (om aktiverad)

    # 2) Inputs (med fallback från Data-bladet)
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

    # Estimat / tillväxt
    eps_1y_est = _pos(_nz(yh_eps.get("eps_1y"), _nz(fh.get("eps_1y"), row.get("EPS 1Y"))))
    eps_2y_est = _pos(_nz(yh_eps.get("eps_2y"), _nz(fh.get("eps_2y"), row.get("EPS 2Y"))))

    # EPS CAGR
    eps_cagr_raw = _f(row.get("EPS CAGR"))
    if eps_cagr_raw is None and yh_eps.get("eps_cagr_long") is not None:
        eps_cagr_raw = _f(yh_eps.get("eps_cagr_long"))
    if eps_cagr_raw is None and _pos(eps_ttm) is not None and _pos(eps_1y_est) is not None:
        try:
            eps_cagr_raw = (float(eps_1y_est)/float(eps_ttm)) - 1.0
        except Exception:
            eps_cagr_raw = None
    eps_cagr = _clamp(eps_cagr_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    # Rev CAGR
    rev_cagr_raw = _f(row.get("Rev CAGR"))
    if rev_cagr_raw is None and revcg_yh.get("rev_cagr") is not None:
        rev_cagr_raw = _f(revcg_yh.get("rev_cagr"))
    rev_cagr = _clamp(rev_cagr_raw, REV_CAGR_MIN, REV_CAGR_MAX)

    # 3) Härled ENDAST EPS TTM om saknas (inte EPS 1Y)
    eps_ttm, src_eps_ttm = _derive_eps_ttm_from_pe_only(price, pe_ttm, _f(eps_ttm))

    # 4) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 5) Paths
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr, rev_cagr)
    r0, r1, r2, r3 = _rev_path(_f(rev_ttm), rev_cagr)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,     _decay_multiple(p_b,     1, decay), _decay_multiple(p_b,     2, decay), _decay_multiple(p_b,     3, decay)

    # 6) Priser per metod
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

    # 7) Sanity + META
    src = snap.get("sources", {}) or {}
    eps1_src = "yahoo_trend" if _pos(yh_eps.get("eps_1y")) else ("finnhub" if _pos(fh.get("eps_1y")) else ("sheet" if _pos(row.get("EPS 1Y")) else "filled_by_rule"))
    eps2_src = "yahoo_trend" if _pos(yh_eps.get("eps_2y")) else ("finnhub" if _pos(fh.get("eps_2y")) else ("sheet/derived" if _pos(row.get("EPS 2Y")) else "filled_by_rule"))
    revc_src = "yahoo_financials" if revcg_yh.get("rev_cagr") is not None else ("sheet" if _f(row.get("Rev CAGR")) is not None else "none")

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if e0 or e0==0 else '—'}({src.get('eps_ttm','?') or ('derived' if (isinstance(src_eps_ttm, str) and src_eps_ttm.startswith('derived')) else src_eps_ttm)}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_cagr={'ok' if _f(rev_cagr) is not None else '—'}({revc_src} ; clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
        f"eps_cagr={'ok' if _f(eps_cagr) is not None else '—'}(clamp={EPS_CAGR_MIN*100:.0f}%..{EPS_CAGR_MAX*100:.0f}%), "
        f"ebitda_ttm={'ok' if b0 or b0==0 else '—'}({src.get('ebitda_ttm','?')}), "
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

# ===== Slut på Del 2/4 — 2B. Fortsätt med Del 3/4 (Analys/Portfölj/Ranking UI). =====

# ============================================================
# app.py — Del 3a/4
# Analys / Hjälpfunktioner / Utdelningslogik / Bolagskort / Analys-sida
# ============================================================

from typing import Optional, Dict, Any, List, Tuple
import datetime as dt
import numpy as np
import pandas as pd
import streamlit as st
import time

# ---------- Format-hjälpare ----------
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

# ---------- Utdelningshjälpare ----------
def _add_months(d: dt.date, n: int) -> dt.date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    day = min(d.day, [31,
        29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
        31,30,31,30,31,31,30,31,30,31][m-1])
    return dt.date(y, m, day)

def _roll_forward(d: Optional[dt.date], freq: str) -> Optional[dt.date]:
    if not isinstance(d, dt.date):
        return None
    freq = (freq or "").upper()
    if freq == "M":  return _add_months(d, 1)
    if freq == "Q":  return _add_months(d, 3)
    if freq == "S":  return _add_months(d, 6)
    if freq == "A":  return _add_months(d, 12)
    return None

def _dps_from_annual_and_freq(annual: Optional[float], freq: str) -> Optional[float]:
    a = _f(annual)
    if a is None:
        return None
    freq = (freq or "").upper()
    if freq == "M": return a / 12.0
    if freq == "Q": return a / 4.0
    if freq == "S": return a / 2.0
    if freq == "A": return a
    return None

def today_date() -> dt.date:
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return dt.datetime.now(tz).date()
    except Exception:
        return dt.date.today()

def forecast_dividends_net_sek(currency: str, shares_own: float,
                               dps_now: Optional[float], dps_cagr: Optional[float],
                               fx_map: Dict[str, float], settings: Dict[str, str]) -> Dict[str, float]:
    """Prognos 1–3 år, netto i SEK (källskatt enligt settings)."""
    ccy = (currency or "USD").upper()
    wh  = get_withholding_for(ccy, settings)
    fx  = fx_map.get(ccy, 1.0) or 1.0
    dps0 = _f(dps_now) or 0.0
    g    = _f(dps_cagr) or 0.0
    def yr(n):
        gross = (dps0 * ((1.0 + g) ** n)) * (shares_own or 0.0)
        return gross * (1.0 - wh) * fx
    return {"y1": yr(1), "y2": yr(2), "y3": yr(3)}

def update_next_dividends_in_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """Rulla fram 'Nästa utdelningsdatum' som passerat och fyll 'Nästa utdelning (per aktie)' om möjligt."""
    if df.empty:
        return df, 0
    out = df.copy()
    for c in ["Utdelningsfrekvens","Nästa utdelningsdatum","Nästa utdelning (per aktie)","Årlig utdelning"]:
        if c not in out.columns:
            out[c] = np.nan

    out["Utdelningsfrekvens"] = out["Utdelningsfrekvens"].fillna("").astype(str).str.upper()
    out["Nästa utdelningsdatum"] = pd.to_datetime(out["Nästa utdelningsdatum"], errors="coerce").dt.date

    n_changed = 0
    today = today_date()
    for idx, r in out.iterrows():
        freq = str(r.get("Utdelningsfrekvens") or "").upper()
        d    = r.get("Nästa utdelningsdatum")
        if isinstance(d, dt.date):
            safe = 0
            while d is not None and d <= today and freq in ("M","Q","S","A") and safe < 36:
                d = _roll_forward(d, freq)
                safe += 1
            if d != r.get("Nästa utdelningsdatum"):
                out.at[idx, "Nästa utdelningsdatum"] = d
                n_changed += 1

        if pd.isna(r.get("Nästa utdelning (per aktie)")) or float(_f(r.get("Nästa utdelning (per aktie)")) or 0) == 0.0:
            dps = _dps_from_annual_and_freq(_f(r.get("Årlig utdelning")), freq)
            if dps is not None:
                out.at[idx, "Nästa utdelning (per aktie)"] = dps
                n_changed += 1

    return out, n_changed

# ---------- Heuristik: välj primär metod ----------
if "_PREFER_ORDER" not in globals():
    _PREFER_ORDER = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]

def _pick_primary_from_table(met_df: pd.DataFrame, preset: Optional[str] = None) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if met_df is None or met_df.empty:
        return None, None, None, None, None
    available = set(met_df["Metod"].astype(str))
    chosen = None
    if preset and preset in available:
        chosen = preset
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

# ---------- Spara primär metod & målpriser ----------
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

def _save_targets_to_result(tkr: str, currency: str, method: Optional[str],
                            t0: Optional[float], t1: Optional[float], t2: Optional[float], t3: Optional[float]):
    res = _read_df(RESULT_TITLE)
    row = {
        "Timestamp": now_stamp(),
        "Ticker": tkr,
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
    mask = res["Ticker"].astype(str).str.upper() == str(tkr).upper()
    if mask.any():
        idx = res.index[mask][-1]
        for k, v in row.items():
            res.at[idx, k] = v
    else:
        res = pd.concat([res, pd.DataFrame([row])[cols]], ignore_index=True)
    _write_df(RESULT_TITLE, res[cols])

# ---------- Innehav/anskaffning/PNL i SEK ----------
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

# ---------- Bolagskort (Analys) ----------
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

    # Val av primär metod
    default_method, t0_d, t1_d, t2_d, t3_d = _pick_primary_from_table(met_df, preset_primary)

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

    # Metodtabell
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
            st.write(f"• **Multipel-decay/år:** { get_settings_map().get('multiple_decay','0.10') }")
            st.write(f"• **Vikt TTM i PE-ankare:** { get_settings_map().get('pe_anchor_weight_ttm','0.50') }")
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

    # Utdelningsprognos (netto SEK, 1–3 år)
    try:
        shares_own = _f(row.get("Antal aktier")) or 0.0
        dps_now = _f(row.get("Årlig utdelning"))
        dps_cagr = _f(row.get("Utdelning CAGR"))
        divs = forecast_dividends_net_sek(currency, shares_own, dps_now, dps_cagr, fx_map, get_settings_map())
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

# ---------- Analys-sida ----------
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

    # Beräkna fair value (Idag) för sortering
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

    if undervalued_only:
        scored = [t for t in scored if (t[2].get("t0") is not None and t[2].get("price") is not None and t[2]["t0"] > t[2]["price"])]

    scored.sort(key=lambda x: (x[1] is None, -x[1] if x[1] is not None else -9e9))
    ordered_rows = [t[3] for t in scored]

    if not ordered_rows:
        st.info("Inga poster uppfyllde kriterierna.")
        return

    key_idx = "analysis_idx"
    if key_idx not in st.session_state:
        st.session_state[key_idx] = 0

    tkr_options = [str(r.get("Ticker")) for r in ordered_rows]
    jump = st.selectbox("Gå direkt till bolag", tkr_options, index=st.session_state[key_idx] if 0 <= st.session_state[key_idx] < len(tkr_options) else 0)
    if jump in tkr_options:
        st.session_state[key_idx] = tkr_options.index(jump)

    cprev, cpos, cnext = st.columns([1,2,1])
    with cprev:
        st.button("⬅️ Föregående", use_container_width=True, disabled=(st.session_state[key_idx] <= 0), on_click=lambda: st.session_state.update({key_idx: max(0, st.session_state[key_idx]-1)}))
    with cpos:
        st.write(f"**{st.session_state[key_idx]+1} / {len(ordered_rows)}** — sorterat efter störst uppsida")
    with cnext:
        st.button("Nästa ➡️", use_container_width=True, disabled=(st.session_state[key_idx] >= len(ordered_rows)-1), on_click=lambda: st.session_state.update({key_idx: min(len(ordered_rows)-1, st.session_state[key_idx]+1)}))

    row = ordered_rows[st.session_state[key_idx]]
    with st.container(border=True):
        _company_card(row, settings, fx_map)
        st.markdown("---")

# ======= Fortsätt med Del 3b/4 direkt efter denna del =======

# ============================================================
# app.py — Del 3b/4
# Portfölj / Utdelningslista / Ranking
# ============================================================

# ---------- Portfölj-sida ----------
def page_portfolio():
    st.header("📦 Portfölj")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()

    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    # Visa endast verkliga innehav
    q = df.copy()
    q["Antal aktier"] = pd.to_numeric(q["Antal aktier"], errors="coerce")
    q["GAV (SEK)"]    = pd.to_numeric(q["GAV (SEK)"], errors="coerce")
    q = q[(q["Antal aktier"] > 0)]
    if q.empty:
        st.info("Inga innehav (Antal aktier > 0).")
        return

    # ------- Sammanfattning värde/PNL -------
    rows = []
    prog = st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            preset = str(r.get("Primär metod") or "").strip() or None
            method, fair_today, *_ = _pick_primary_from_table(met_df, preset)
            price = meta.get("price")
            currency = meta.get("currency") or str(_nz(r.get("Valuta"), "USD")).upper()
            fx_rate = fx_map.get(currency, 1.0) or 1.0

            shares_own = float(_nz(_f(r.get("Antal aktier")), 0.0))
            gav_sek    = _f(r.get("GAV (SEK)"))

            hm = _holding_metrics(currency, price, shares_own, gav_sek, fx_rate)
            up_pct = None
            if _pos(price) and _pos(fair_today):
                up_pct = (fair_today/price - 1.0) * 100.0

            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": currency,
                "Antal aktier": shares_own,
                "GAV (SEK)": gav_sek,
                "Pris": price,
                "Fair value (Idag)": fair_today,
                "Uppsida %": up_pct,
                "Värde (SEK)": hm["value_sek"],
                "Anskaffning (SEK)": hm["cost_sek"],
                "P/L (SEK)": hm["pnl_sek"],
                "P/L %": (hm["pnl_pct"]*100.0 if hm["pnl_pct"] is not None else None),
            })
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Antal aktier": _f(r.get("Antal aktier")),
                "GAV (SEK)": _f(r.get("GAV (SEK)")),
                "Pris": None,
                "Fair value (Idag)": None,
                "Uppsida %": None,
                "Värde (SEK)": None,
                "Anskaffning (SEK)": None,
                "P/L (SEK)": None,
                "P/L %": None,
            })
        prog.progress((i+1)/max(1,len(q)))
    prog.empty()

    out = pd.DataFrame(rows)

    # Summera
    tot_value = pd.to_numeric(out["Värde (SEK)"], errors="coerce").sum()
    tot_cost  = pd.to_numeric(out["Anskaffning (SEK)"], errors="coerce").sum()
    tot_pnl   = tot_value - tot_cost if (pd.notna(tot_value) and pd.notna(tot_cost)) else np.nan
    tot_pnl_pct = (tot_pnl / tot_cost) if (tot_cost and not pd.isna(tot_cost) and tot_cost != 0) else np.nan

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Portföljvärde", _fmt_sek(tot_value))
    c2.metric("Anskaffning", _fmt_sek(tot_cost))
    c3.metric("P/L (SEK)", _fmt_sek(tot_pnl))
    c4.metric("P/L (%)", _fmt_pct(tot_pnl_pct))

    show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Antal aktier","GAV (SEK)","Pris","Fair value (Idag)","Uppsida %","Värde (SEK)","Anskaffning (SEK)","P/L (SEK)","P/L %"]
    st.dataframe(out[show_cols], use_container_width=True)

    # ------- Kommande utdelningsutbetalningar (netto i SEK) -------
    st.subheader("🗓️ Kommande utdelningsutbetalningar (netto i SEK)")

    dd = read_data_df().copy()
    needed_cols = ["Årlig utdelning","Nästa utdelningsdatum","Utdelningsfrekvens","Nästa utdelning (per aktie)","Antal aktier","Valuta","Ticker","Bolagsnamn"]
    for c in needed_cols:
        if c not in dd.columns:
            dd[c] = np.nan

    # Endast innehav
    dd["Antal aktier"] = pd.to_numeric(dd["Antal aktier"], errors="coerce")
    dd = dd[(dd["Antal aktier"] > 0)]

    left, right = st.columns([1,1])
    if left.button("🔁 Uppdatera nästa utdelningsdatum & belopp (spara)"):
        df_cur = read_data_df()
        df_upd, n = update_next_dividends_in_df(df_cur)
        write_data_df(df_upd)
        st.success(f"Uppdaterade {n} rader i Data.")
        st.rerun()

    # Filtrera bolag som sannolikt betalar
    pays_mask = (pd.to_numeric(dd["Årlig utdelning"], errors="coerce").fillna(0) > 0) | dd["Nästa utdelningsdatum"].notna()
    dd = dd[pays_mask]

    # Datum-typ och rulla fram passerade datum (i minnet)
    dd["Nästa utdelningsdatum"] = pd.to_datetime(dd["Nästa utdelningsdatum"], errors="coerce").dt.date

    def _rolled_future(d, freq):
        if not isinstance(d, dt.date):
            return np.nan
        x = d
        today = today_date()
        safe = 0
        freq = (freq or "").upper()
        while x <= today and freq in ("M","Q","S","A") and safe < 24:
            x = _roll_forward(x, freq)
            safe += 1
        return x

    dd["Utdelningsfrekvens"] = dd["Utdelningsfrekvens"].fillna("").astype(str).str.upper()
    dd["Datum"] = dd.apply(lambda r: _rolled_future(r.get("Nästa utdelningsdatum"), r.get("Utdelningsfrekvens")), axis=1)

    # DPS per aktie nästa gång
    def _next_dps_row(r):
        v = _f(r.get("Nästa utdelning (per aktie)"))
        if v is not None:
            return v
        annual = _f(r.get("Årlig utdelning"))
        freq   = str(r.get("Utdelningsfrekvens") or "").upper()
        return _dps_from_annual_and_freq(annual, freq)

    dd["DPS_nästa"] = dd.apply(_next_dps_row, axis=1)

    # Rensa bort utan datum eller dps
    dd = dd[(dd["Datum"].notna()) & (pd.to_numeric(dd["DPS_nästa"], errors="coerce").fillna(0) > 0)]

    # Beräkna brutto & netto SEK
    def _net_sek_row(r):
        ccy = str(r.get("Valuta") or "USD").upper()
        wh = get_withholding_for(ccy, get_settings_map())
        fx = get_fx_map().get(ccy, 1.0) or 1.0
        shares = _f(r.get("Antal aktier")) or 0.0
        dps = _f(r.get("DPS_nästa")) or 0.0
        gross_ccy = dps * shares
        net_sek = gross_ccy * (1.0 - wh) * fx
        return gross_ccy, net_sek

    gross_list, net_list = [], []
    for _, r in dd.iterrows():
        g, n = _net_sek_row(r)
        gross_list.append(g)
        net_list.append(n)
    dd["Brutto (valuta)"] = gross_list
    dd["Netto (SEK)"] = net_list

    # Sortera och visa
    dd = dd.sort_values(by=["Datum","Ticker"])
    cols_pay = ["Datum","Ticker","Bolagsnamn","Valuta","Antal aktier","DPS_nästa","Brutto (valuta)","Netto (SEK)","Utdelningsfrekvens"]
    st.dataframe(dd[cols_pay], use_container_width=True)

    # Summering nästa 60 dagar
    horizon = today_date() + dt.timedelta(days=60)
    mask60 = (dd["Datum"] <= horizon)
    tot60 = pd.to_numeric(dd.loc[mask60, "Netto (SEK)"], errors="coerce").sum()
    right.metric("Netto utdelning kommande 60 dagar", _fmt_sek(tot60))

# ---------- Ranking-sida ----------
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
            rows.append({
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
            time.sleep(0.06)
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None, "Primär metod": None, "Fair value (Idag)": None, "Uppsida %": None,
                "Value SEK": 0.0
            })
        prog.progress((i+1)/max(1,len(q)))
    prog.empty()

    out = pd.DataFrame(rows)

    out["Pris_num"]    = pd.to_numeric(out["Pris"], errors="coerce")
    out["FV_idag_num"] = pd.to_numeric(out["Fair value (Idag)"], errors="coerce")

    totals = out.groupby("Bucket")["Value SEK"].sum().rename("Bucket Total SEK")
    out = out.merge(totals, on="Bucket", how="left")
    out["Bucket Total SEK"] = out["Bucket Total SEK"].replace({0.0: np.nan})
    out["Andel i bucket"] = out["Value SEK"] / out["Bucket Total SEK"]
    out["Andel i bucket"] = out["Andel i bucket"].fillna(1.0)

    out["Undervärderad"] = ((out["FV_idag_num"].notna()) &
                            (out["Pris_num"].notna()) &
                            (out["FV_idag_num"] > out["Pris_num"])).astype(int)

    if only_underval:
        out = out[out["Undervärderad"] == 1]

    out = out.sort_values(by=["Undervärderad","Andel i bucket","Uppsida %"],
                          ascending=[False, True, False], na_position="last")

    show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Pris","Primär metod","Fair value (Idag)","Uppsida %","Value SEK","Andel i bucket"]
    st.dataframe(out[show_cols], use_container_width=True)

# ============================================================
# app.py — Del 4/4
# Inställningar, Editor (Hämta & fyll från Yahoo), Batch & Main
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
        for c in out.columns:
            if c not in snap.columns:
                snap[c] = np.nan
        for c in snap.columns:
            if c not in out.columns:
                out[c] = np.nan
        snap = pd.concat([snap[snap.columns], out[snap.columns]], ignore_index=True)
        _write_df(SNAPSHOT_TITLE, snap)

# ---------- Startup-refresh (valfritt via Settings) ----------
def _startup_refresh():
    """
    Lätt auto-uppdatering av pris/valuta på uppstart om Settings->auto_refresh_on_start == '1'.
    Kör INTE massuppdatering; endast pris & valuta.
    """
    s = get_settings_map()
    if str(s.get("auto_refresh_on_start", "0")).strip() != "1":
        return

    df = read_data_df()
    if df.empty:
        return

    changed = 0
    for i, idx in enumerate(df.index):
        tkr = str(df.at[idx, "Ticker"]).strip()
        if not tkr:
            continue
        try:
            tk = yf.Ticker(tkr)
            fi = getattr(tk, "fast_info", None)
            px = None
            ccy = None
            if fi:
                px = _f(getattr(fi, "last_price", None))
                ccy = str(getattr(fi, "currency", None) or "").upper() or None
            if px is None:
                hist = tk.history(period="5d")
                if not hist.empty:
                    px = float(hist["Close"].dropna().iloc[-1])
            if px is not None:
                df.at[idx, "Aktuell kurs"] = px
                changed += 1
            if ccy:
                df.at[idx, "Valuta"] = ccy
        except Exception:
            pass
    if changed > 0:
        df["Senast auto uppdaterad"] = df["Senast auto uppdaterad"].where(df["Senast auto uppdaterad"].notna(), "")
        write_data_df(df)

# ---------- Gemensam: mappa Yahoo-snapshot + estimat till Data-kolumner ----------
def _build_updates_from_yahoo(ticker: str, existing_row: Optional[pd.Series] = None) -> Tuple[Dict[str, Any], Dict[str, Any], pd.DataFrame]:
    """
    Hämtar Yahoo-snapshot + EPS/REV-estimat och returnerar:
      updates_dict (endast fält med värde),
      meta (inkl. källor),
      methods_df (för ev. snapshot).
    Skriv INTE själv till arket här; returnera bara data.
    """
    # Kör motor för att få både snapshot & metoder konsekvent
    fake_row = pd.Series({"Ticker": ticker}) if existing_row is None else existing_row
    settings = get_settings_map()
    fx_map   = get_fx_map()

    methods_df, sanity, meta = compute_methods_for_row(fake_row, settings, fx_map)

    # Plocka upp snapshot-värden från meta/sources där compute_methods_for_row redan hämtat
    snap_fields = {
        "Aktuell kurs": meta.get("price"),
        "Valuta": meta.get("currency"),
        "Utestående aktier": meta.get("shares_out"),
        "Net debt": meta.get("net_debt"),
    }

    # Hämta även rena snapshotfält direkt från fetch_yahoo_snapshot för säkerhets skull
    snap = fetch_yahoo_snapshot(ticker)

    # Nyckeltal
    derived_fields = {
        "Rev TTM": snap.get("revenue_ttm"),
        "EBITDA TTM": snap.get("ebitda_ttm"),
        "EPS TTM": snap.get("eps_ttm"),
        "PE TTM": snap.get("pe_ttm"),
        "PE FWD": snap.get("pe_fwd"),
        "EV/Revenue": snap.get("ev_to_sales"),
        "EV/EBITDA": snap.get("ev_to_ebitda"),
        "P/B": snap.get("p_to_book"),
        "BVPS": snap.get("bvps"),
        "Bolagsnamn": snap.get("company_name"),
        "Sektor": snap.get("sector"),
    }

    # Estimat & tillväxt
    yh_eps  = fetch_yahoo_eps_estimates(ticker)
    rev_cg  = fetch_yahoo_rev_cagr(ticker)
    # EPS 1Y, EPS 2Y, Rev CAGR, EPS CAGR (om härledd)
    eps_1y = _pos(yh_eps.get("eps_1y"))
    eps_2y = _pos(yh_eps.get("eps_2y"))
    rev_cg_v = _f(rev_cg.get("rev_cagr"))
    eps_cg_v = None
    try:
        e0 = _pos(snap.get("eps_ttm"))
        if e0 and eps_1y:
            eps_cg_v = (float(eps_1y) / float(e0)) - 1.0
    except Exception:
        eps_cg_v = None

    est_fields = {
        "EPS 1Y": eps_1y,
        "EPS 2Y": eps_2y,
        "Rev CAGR": rev_cg_v,
        "EPS CAGR": _clamp(eps_cg_v, EPS_CAGR_MIN, EPS_CAGR_MAX) if eps_cg_v is not None else None,
    }

    updates = {}
    for k, v in {**snap_fields, **derived_fields, **est_fields}.items():
        if v is None or (isinstance(v, float) and (v != v)):
            continue
        updates[k] = v

    # Meta-taggar för "Senast auto uppdaterad" och "Auto källa"
    updates["Senast auto uppdaterad"] = now_stamp()
    updates["Auto källa"] = "Yahoo"

    return updates, meta, methods_df

# ---------- Editor-sida ----------
def page_editor():
    st.header("✍️ Lägg till / uppdatera bolag")

    df = read_data_df()
    all_tickers = sorted([t for t in df["Ticker"].dropna().astype(str).unique().tolist() if t])

    c1, c2 = st.columns([2,1])
    with c1:
        ticker = st.text_input("Ticker (t.ex. NVDA, 2020.OL)", value=st.session_state.get("editor_ticker","")).strip().upper()
    with c2:
        bucket = st.selectbox("Bucket", DEFAULT_BUCKETS, index=0)

    if ticker:
        st.session_state["editor_ticker"] = ticker

    # Förhandsvisa ev. befintlig rad
    existing_row = None
    if ticker and not df.empty:
        mask = df["Ticker"].astype(str).str.upper() == ticker
        if mask.any():
            existing_row = df[mask].iloc[0]

    # Hämta & förhandsvisa uppdateringar
    upd_col, save_col = st.columns([1,1])
    if upd_col.button("🔎 Hämta & fyll från Yahoo (inkl. EPS/REV-estimat)"):
        try:
            updates, meta, methods_df = _build_updates_from_yahoo(ticker, existing_row)
            st.session_state["editor_updates"] = updates
            st.session_state["editor_meta"] = meta
            st.session_state["editor_methods"] = methods_df
            st.success("Hämtning klar.")
        except Exception as e:
            st.error(f"Misslyckades att hämta: {e}")

    updates = st.session_state.get("editor_updates", {})
    meta    = st.session_state.get("editor_meta", {})
    methods = st.session_state.get("editor_methods", pd.DataFrame())

    if updates:
        st.subheader("Föreslagna uppdateringar")
        # Visa skillnader gammalt → nytt
        def _old_val(k):
            if existing_row is None or k not in existing_row.index:
                return None
            return existing_row.get(k)
        preview = []
        for k in sorted(updates.keys()):
            preview.append({"Fält": k, "Gammalt": _old_val(k), "Nytt": updates[k]})
        st.dataframe(pd.DataFrame(preview), use_container_width=True)

    # Spara
    if save_col.button("💾 Spara till Data"):
        if not ticker:
            st.warning("Ange ticker först.")
        else:
            # Säkerställ baskolumner
            df = read_data_df()
            df = _ensure_columns(df, DATA_COLUMNS)

            mask = df["Ticker"].astype(str).str.upper() == ticker
            if not mask.any():
                # Skapa ny rad
                base = {c: np.nan for c in DATA_COLUMNS}
                base.update({
                    "Timestamp": now_stamp(),
                    "Ticker": ticker,
                    "Bucket": bucket,
                })
                if updates:
                    base.update(updates)
                df = pd.concat([df, pd.DataFrame([base])], ignore_index=True)
            else:
                # Uppdatera befintliga fält — endast de vi har värden för
                idx = df.index[mask][0]
                df.at[idx, "Bucket"] = bucket
                for k, v in (updates or {}).items():
                    if v is not None and not (isinstance(v, float) and (v != v)):
                        df.at[idx, k] = v

            write_data_df(df)
            st.success("Sparat till Data.")
            # Spara även snapshot om vi har methods_df
            if isinstance(methods, pd.DataFrame) and not methods.empty:
                try:
                    save_quarter_snapshot(ticker, methods, meta or {})
                except Exception:
                    pass

    # Visa methods från senaste hämtning (om finns)
    if isinstance(methods, pd.DataFrame) and not methods.empty:
        with st.expander("📊 Metoder & målpriser (senaste hämtning)"):
            st.dataframe(methods, use_container_width=True)

# ---------- Batch-sida (massuppdatera) ----------
def _apply_updates_to_df_row(df: pd.DataFrame, idx, updates: Dict[str, Any]) -> int:
    n = 0
    for k, v in (updates or {}).items():
        if v is None or (isinstance(v, float) and (v != v)):
            continue
        df.at[idx, k] = v
        n += 1
    return n

def page_batch():
    st.header("🧩 Massuppdatering (Yahoo) — 1s fördröjning per bolag")

    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    tickers = sorted(df["Ticker"].dropna().astype(str).unique().tolist())
    sel = st.multiselect("Välj tickers att uppdatera (tom = alla)", options=tickers, default=[])

    do_all = (len(sel) == 0)
    target = tickers if do_all else sel

    c1, c2 = st.columns([1,1])
    delay_sec = c1.number_input("Fördröjning per bolag (sek)", min_value=0.5, max_value=5.0, value=1.0, step=0.5)
    go = c2.button("🚀 Starta massuppdatering")

    if go:
        df_cur = read_data_df()
        df_cur = _ensure_columns(df_cur, DATA_COLUMNS)
        progress = st.progress(0.0)
        status = st.empty()
        updated_total = 0

        for i, tkr in enumerate(target, start=1):
            try:
                status.write(f"Uppdaterar bolag {i} av {len(target)} — {tkr}")
                mask = df_cur["Ticker"].astype(str).str.upper() == str(tkr).upper()
                existing_row = df_cur[mask].iloc[0] if mask.any() else pd.Series({"Ticker": tkr})
                updates, meta, methods_df = _build_updates_from_yahoo(tkr, existing_row)
                if mask.any():
                    idx = df_cur.index[mask][0]
                    updated_total += _apply_updates_to_df_row(df_cur, idx, updates)
                else:
                    base = {c: np.nan for c in DATA_COLUMNS}
                    base.update({"Timestamp": now_stamp(), "Ticker": tkr})
                    base.update(updates)
                    df_cur = pd.concat([df_cur, pd.DataFrame([base])], ignore_index=True)
                    updated_total += len(updates)
                time.sleep(float(delay_sec))
            except Exception as e:
                st.error(f"{tkr}: {e}")
            progress.progress(i/len(target))

        write_data_df(df_cur)
        progress.empty()
        status.empty()
        st.success(f"Klar. Uppdaterade {len(target)} bolag, {updated_total} fält ändrades.")
        st.balloons()

# ---------- Settings-sida ----------
def page_settings():
    st.header("⚙️ Inställningar")

    s = get_settings_map()
    fx = get_fx_map()

    c1, c2, c3 = st.columns(3)
    with c1:
        primary_ccy = st.selectbox("Primär visningsvaluta", ["SEK","USD","EUR","NOK","CAD"], index=["SEK","USD","EUR","NOK","CAD"].index(s.get("primary_currency","SEK")))
        pe_w = st.number_input("Vikt TTM i PE-ankare (0–1)", min_value=0.0, max_value=1.0, value=float(_f(s.get("pe_anchor_weight_ttm")) or 0.50), step=0.05)
    with c2:
        decay = st.number_input("Multipel-decay per år", min_value=0.00, max_value=0.50, value=float(_f(s.get("multiple_decay")) or 0.10), step=0.01)
        auto = st.checkbox("Auto-refresh pris/valuta vid start", value=str(s.get("auto_refresh_on_start","0"))=="1")
    with c3:
        st.caption("Källskatt per valuta")
        wh_usd = st.number_input("USD", min_value=0.0, max_value=0.50, value=float(_f(s.get("withholding_USD")) or 0.15), step=0.01)
        wh_nok = st.number_input("NOK", min_value=0.0, max_value=0.50, value=float(_f(s.get("withholding_NOK")) or 0.25), step=0.01)
        wh_cad = st.number_input("CAD", min_value=0.0, max_value=0.50, value=float(_f(s.get("withholding_CAD")) or 0.15), step=0.01)
        wh_eur = st.number_input("EUR", min_value=0.0, max_value=0.50, value=float(_f(s.get("withholding_EUR")) or 0.15), step=0.01)
        wh_sek = st.number_input("SEK", min_value=0.0, max_value=0.50, value=float(_f(s.get("withholding_SEK")) or 0.00), step=0.01)

    if st.button("💾 Spara inställningar"):
        s_df = _read_df(SETTINGS_TITLE)
        if s_df.empty:
            s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)

        def set_kv(k, v):
            nonlocal s_df
            if "Key" not in s_df.columns or "Value" not in s_df.columns:
                s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)
            mask = s_df["Key"].astype(str) == k
            if mask.any():
                s_df.loc[mask, "Value"] = str(v)
            else:
                s_df = pd.concat([s_df, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)

        set_kv("primary_currency", primary_ccy)
        set_kv("pe_anchor_weight_ttm", pe_w)
        set_kv("multiple_decay", decay)
        set_kv("auto_refresh_on_start", "1" if auto else "0")
        set_kv("withholding_USD", wh_usd)
        set_kv("withholding_NOK", wh_nok)
        set_kv("withholding_CAD", wh_cad)
        set_kv("withholding_EUR", wh_eur)
        set_kv("withholding_SEK", wh_sek)

        _write_df(SETTINGS_TITLE, s_df[SETTINGS_COLUMNS])
        st.success("Inställningar sparade.")

    st.markdown("---")
    st.subheader("Valutakurser (SEK per 1 enhet)")
    fx_df = _read_df(FX_TITLE)
    st.dataframe(fx_df, use_container_width=True)
    if st.button("🔁 Hämta & uppdatera valutakurser (Yahoo)"):
        _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")

# ---------- Snapshot-visning ----------
def page_snapshot():
    st.header("🕒 Snapshot-logg")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True)

# ---------- Main ----------
def main():
    _startup_refresh()

    st.sidebar.title("Navigering")
    page = st.sidebar.radio("Gå till:", ["Analys","Portfölj","Ranking","Editor","Batch","Settings","Snapshot"], index=0)

    if page == "Analys":
        page_analysis()
    elif page == "Portfölj":
        page_portfolio()
    elif page == "Ranking":
        page_ranking()
    elif page == "Editor":
        page_editor()
    elif page == "Batch":
        page_batch()
    elif page == "Settings":
        page_settings()
    elif page == "Snapshot":
        page_snapshot()

# Entrypoint
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")
