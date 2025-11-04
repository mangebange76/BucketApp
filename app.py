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
    "Utdelningsfrekvens",                # "M","Q","S","A"
    "Nästa utdelningsdatum",             # YYYY-MM-DD
    "Nästa utdelning (per aktie)",       # DPS nästa
    "Primär metod",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    # Fältvisa tidsstämplar (för "10 äldsta"-listan i Editor)
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

    # TS-fält (behåll som str; tolkas vid behov)
    for tcol in ["TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y","Senast auto uppdaterad"]:
        if tcol in df.columns:
            df[tcol] = df[tcol].astype(str)

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

    # Shares-fallback via info
    if out.get("shares") is None:
        so = _f(gi("sharesOutstanding"))
        if so is not None:
            out["shares"] = so
            out["sources"]["shares"] = "yahoo_info_sharesOutstanding"

    # Derivera PE om möjligt
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

    # --- EV / net debt
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

    # Shares via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            out["sources"]["shares"] = "derived_mcap/price"
        except Exception:
            pass

    # Pris-historik fallback
    if not _pos(out.get("price")):
        try:
            hist = tk.history(period="5d")
            if not hist.empty:
                out["price"] = float(hist["Close"].dropna().iloc[-1])
                out["sources"]["price"] = "yahoo_hist_close"
        except Exception:
            pass

    # Balance Sheet-fallbacks (debt/cash, BVPS, P/B)
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

    # Årlig utdelning (per aktie)
    fwd_div = _f(gi("dividendRate") or gi("forwardAnnualDividendRate"))
    trl_div = _f(gi("trailingAnnualDividendRate"))
    if out.get("annual_dividend") is None and (fwd_div is not None or trl_div is not None):
        out["annual_dividend"] = float(_nz(fwd_div, trl_div))
        out["sources"]["annual_dividend"] = "yahoo_info"

    # Gissa utdelningsfrekvens
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

# ============================================================
# app.py — Del 2/4 — 2B
# EPS/REV-estimat (Yahoo/Fallback), 5-års historisk CAGR,
# multipel-decay, builders och compute_methods_for_row
# ============================================================

# -------------------------
# Yahoo – EPS-estimat (trend)
# -------------------------
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

# -------------------------
# Yahoo – 5-års historisk CAGR för Revenue
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    """CAGR på intäkter från årliga statements (Yahoo), sista 3–5 år."""
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
# Yahoo – 5-års historisk CAGR för EPS (årliga rapporter)
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_eps_cagr_hist(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    """
    Beräknar EPS (diluted/basic) årligen och CAGR över 3–5 år (senaste tillgängliga),
    fallback: NetIncome / DilutedAverageShares om EPS-rad saknas.
    """
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
            # bygg EPS = NetIncome / DilutedAverageShares
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
            eps_series.index = pd.to_datetime(eps_series.index, errors="coerce")
            eps_series = eps_series.sort_index()
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
# Multipel-decay & P/E-ankare
# -------------------------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 % (krav)
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 % (krav)

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
# EPS/REV-paths + manuell Rev1Y/Rev2Y (i miljoner) & härledning
# -------------------------
def _derive_eps_ttm_from_pe_only(price: float | None, pe_ttm: float | None,
                                 eps_ttm: float | None) -> tuple[float | None, str]:
    src_ttm = "source" if eps_ttm is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe_ttm"
    return eps_ttm, src_ttm

def _rev_million_to_units(v: float | None) -> float | None:
    """Används för manuella Rev 1Y/2Y som anges i miljoner (8,81B skrivs 8810)."""
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
    """
    Fyll EPS-path (TTM, 1y, 2y, 3y). Prioritet:
      1) Direktestimat (eps_1y/eps_2y) om finns
      2) Vektor via historisk EPS CAGR (5y) om finns
      3) Vektor via long-term eps trend (Yahoo) om finns
      4) Fallback via Revenue CAGR (hist) om inget annat finns
    """
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
    """
    Revenue-path. Prioritet:
      1) Manuell Rev 1Y/2Y (i miljoner → *1e6)
      2) Härledd från historisk Rev CAGR (5y) + TTM
    """
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
    snap   = fetch_yahoo_snapshot(ticker)
    time.sleep(0.12)  # mild throttling
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.05)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)         # 5y hist Revenue CAGR
    epscg_yh = fetch_yahoo_eps_cagr_hist(ticker)    # 5y hist EPS CAGR

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
    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), _nz(yh_eps.get("eps_1y"), None)))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), _nz(yh_eps.get("eps_2y"), None)))

    # Historisk CAGR (5y) — tak 35%
    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), revcg_yh.get("rev_cagr")))
    rev_cagr_hist     = _clamp(rev_cagr_hist_raw, REV_CAGR_MIN, REV_CAGR_MAX)

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), epscg_yh.get("eps_cagr")))
    eps_cagr_hist     = _clamp(eps_cagr_hist_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    # EPS TTM härledning endast om saknas
    eps_ttm, src_eps_ttm = _derive_eps_ttm_from_pe_only(price, pe_ttm, _f(eps_ttm))

    # 3) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 4) Revenue: prioritera manuella 1Y/2Y (anges i miljoner → *1e6)
    rev1_manual_units = _rev_million_to_units(_f(row.get("Rev 1Y")))
    rev2_manual_units = _rev_million_to_units(_f(row.get("Rev 2Y")))
    r0, r1, r2, r3 = _rev_path(_f(rev_ttm), rev_cagr_hist, rev1_manual_units, rev2_manual_units)

    # 5) EPS-path
    eps_cagr_long = _clamp(_f(yh_eps.get("eps_cagr_long")), EPS_CAGR_MIN, EPS_CAGR_MAX)
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr_hist, eps_cagr_long, rev_cagr_hist)

    # 6) EBITDA-path (skalar mot intäktsbana)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,     _decay_multiple(p_b,     1, decay), _decay_multiple(p_b,     2, decay), _decay_multiple(p_b,     3, decay)

    # 7) Priser per metod
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

    # 8) Sanity + META
    src = snap.get("sources", {}) or {}

    eps1_src = ("sheet" if _pos(row.get("EPS 1Y")) else
                ("yahoo_trend" if _pos(yh_eps.get("eps_1y")) else "filled_by_rule"))

    eps2_src = ("sheet" if _pos(row.get("EPS 2Y")) else
                ("yahoo_trend" if _pos(yh_eps.get("eps_2y")) else "filled_by_rule"))

    revc_src = ("sheet" if _f(row.get("Rev CAGR")) is not None else
                ("yahoo_financials" if revcg_yh.get("rev_cagr") is not None else "none"))

    epsc_src = ("sheet" if _f(row.get("EPS CAGR")) is not None else
                ("yahoo_financials" if epscg_yh.get("eps_cagr") is not None else "none"))

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if (e0 or e0==0) else '—'}({src.get('eps_ttm','?') or ('derived' if (isinstance(src_eps_ttm, str) and src_eps_ttm.startswith('derived')) else src_eps_ttm)}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_cagr_hist={'ok' if _f(rev_cagr_hist) is not None else '—'}({revc_src} ; clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
        f"eps_cagr_hist={'ok' if _f(eps_cagr_hist) is not None else '—'}({epsc_src} ; clamp={EPS_CAGR_MIN*100:.0f}%..{EPS_CAGR_MAX*100:.0f}%), "
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
            "rev_cagr_raw": _f(rev_cagr_hist_raw),
            "rev_cagr_used": _f(rev_cagr_hist),
            "eps_cagr_raw": _f(eps_cagr_hist_raw),
            "eps_cagr_used": _f(eps_cagr_hist),
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# ============================================================
# app.py — Del 3/4 — Sidvyer (Analys/Portfölj/Ranking/Editor)
#  • Visar beräkningar per ticker via compute_methods_for_row
#  • Två-decimalsvisning utan att krascha på None
#  • Editor: säkra textfält för EPS/REV/CAGR (tillåter tomt)
#  • Alltid hela databasen längst ned i Analys-vyn
# ============================================================

import math
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime

# -------- Hjälpare för visning/inputs (förutsätter _f, _nz m.fl. från Del 1) --------
def _fmt2(v):
    """Snygg två-decimalsvisning; None→ tom sträng."""
    try:
        x = float(v)
        if math.isfinite(x):
            return f"{x:.2f}"
        return ""
    except Exception:
        return ""

def _fmt_pct(v):
    try:
        x = float(v)
        if math.isfinite(x):
            return f"{x*100:.1f}%"
        return ""
    except Exception:
        return ""

def _ensure_df() -> pd.DataFrame:
    df = st.session_state.get("DATA", None)
    if df is None:
        st.warning("Ingen data laddad ännu. Lägg in en DataFrame i `st.session_state['DATA']` i Del 1.")
        return pd.DataFrame()
    return df

def _get_settings() -> dict:
    return st.session_state.get("SETTINGS", {}) or {}

def _get_fx_map() -> dict:
    fx = st.session_state.get("FX", {}) or {}
    # Basfallback
    if not fx:
        fx = {"USD": 1.0, "SEK": 1.0, "EUR": 1.0, "NOK": 1.0, "CAD": 1.0}
    return fx

def _df_format_2dec(df: pd.DataFrame, numeric_cols: list[str] | None = None) -> pd.DataFrame:
    """Returnerar en DataFrame med alla numeriska kolumner rundade till 2 dec (utan att spränga None)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if numeric_cols is None:
        numeric_cols = [c for c in out.columns if pd.api.types.is_numeric_dtype(out[c])]
    for c in numeric_cols:
        try:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(2)
        except Exception:
            pass
    return out

def _now_stamp():
    try:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(datetime.now())

# --------- Analysvy ---------
def view_analys():
    st.header("Analys")

    df = _ensure_df()
    if df.empty:
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    tickers.sort(key=lambda x: x.lower())
    col_sel, col_btn = st.columns([3,1])
    ticker = col_sel.selectbox("Välj ticker", tickers, index=0 if tickers else None)
    run = col_btn.button("Beräkna", use_container_width=True)

    # Auto-beräkna vid valbyte också för smidighet
    if run or ticker:
        row = df.loc[df["Ticker"].astype(str) == str(ticker)]
        if row.empty:
            st.error("Hittade inte vald ticker i databasen.")
            return
        row = row.iloc[0]

        settings = _get_settings()
        fx_map = _get_fx_map()

        try:
            methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
        except Exception as e:
            st.error(f"💥 Fel i huvudloopen: {e}")
            with st.expander("Visa traceback"):
                import traceback, sys
                st.code("".join(traceback.format_exception(*sys.exc_info())))
            return

        # Visa rubriker & prisinfo
        top_cols = st.columns(4)
        top_cols[0].metric("Ticker", ticker)
        top_cols[1].metric("Valuta", meta.get("currency", ""))
        top_cols[2].metric("Aktuell kurs", _fmt2(meta.get("price")))
        top_cols[3].metric("Utestående aktier", _fmt2(meta.get("shares_out")))

        # Metodtabell — rundad
        st.subheader("Riktkurser per metod")
        st.dataframe(_df_format_2dec(methods_df), use_container_width=True)

        # Sammanfattning
        with st.expander("Meta & sanity", expanded=False):
            st.write(sanity)
            st.json({
                "pe_anchor": meta.get("pe_anchor"),
                "decay": meta.get("decay"),
                "net_debt": meta.get("net_debt"),
                "annual_dividend": meta.get("annual_dividend"),
                "dividend_frequency": meta.get("dividend_frequency"),
                "eps_path": meta.get("eps_path"),
                "rev_path": meta.get("rev_path"),
                "ebitda_path": meta.get("ebitda_path"),
                "cagr_clamped": meta.get("cagr_clamped"),
                "sources": meta.get("sources"),
            })

    # Alltid hela databasen längst ner
    st.subheader("Hela databasen (visning)")
    st.caption("Enkel tabell (utan sort/redigering här).")
    st.dataframe(_df_format_2dec(df), use_container_width=True, hide_index=True)

# --------- Portföljvy ---------
def view_portfolio():
    st.header("Portfölj")

    df = _ensure_df()
    if df.empty:
        return
    fx = _get_fx_map()

    # Antag kolumner (för basversionen):
    #  - Antal aktier (float/int)
    #  - Valuta (str)
    #  - Aktuell kurs (pris i respektive valuta)
    #  - GAV (SEK) — om saknas, behandla som NaN
    cols_needed = ["Ticker", "Antal aktier", "Aktuell kurs", "Valuta", "GAV (SEK)"]
    for c in cols_needed:
        if c not in df.columns:
            df[c] = np.nan if c != "Ticker" and c != "Valuta" else ""

    # Räknar värde i SEK (om FX finns), vinst/förlust
    def _fx_of(cur: str) -> float:
        try:
            return float(fx.get(str(cur).upper(), 1.0))
        except Exception:
            return 1.0

    work = df.copy()
    work["Antal aktier"] = pd.to_numeric(work["Antal aktier"], errors="coerce")
    work["Aktuell kurs"] = pd.to_numeric(work["Aktuell kurs"], errors="coerce")
    work["GAV (SEK)"] = pd.to_numeric(work["GAV (SEK)"], errors="coerce")

    work["FX"] = work["Valuta"].astype(str).str.upper().map(lambda c: _fx_of(c))
    work["Värde (SEK)"] = (work["Antal aktier"] * work["Aktuell kurs"] * work["FX"]).fillna(0.0)
    work["Anskaffning (SEK)"] = work["GAV (SEK)"].fillna(0.0)
    work["P/L (SEK)"] = work["Värde (SEK)"] - work["Anskaffning (SEK)"]
    work["P/L (%)"] = np.where(work["Anskaffning (SEK)"] > 0,
                               work["P/L (SEK)"] / work["Anskaffning (SEK)"],
                               np.nan)

    totals = {
        "Totalt värde (SEK)": work["Värde (SEK)"].sum(),
        "Totalt anskaffning (SEK)": work["Anskaffning (SEK)"].sum(),
    }
    totals["Totalt P/L (SEK)"] = totals["Totalt värde (SEK)"] - totals["Totalt anskaffning (SEK)"]
    totals["Totalt P/L (%)"] = (totals["Totalt P/L (SEK)"] / totals["Totalt anskaffning (SEK)"]
                                if totals["Totalt anskaffning (SEK)"] > 0 else np.nan)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Totalt värde (SEK)", _fmt2(totals["Totalt värde (SEK)"]))
    m2.metric("Anskaffning (SEK)", _fmt2(totals["Totalt anskaffning (SEK)"]))
    m3.metric("P/L (SEK)", _fmt2(totals["Totalt P/L (SEK)"]))
    m4.metric("P/L (%)", _fmt_pct(totals["Totalt P/L (%)"]))

    show_cols = ["Ticker", "Antal aktier", "Valuta", "Aktuell kurs", "FX",
                 "Värde (SEK)", "GAV (SEK)", "Anskaffning (SEK)", "P/L (SEK)", "P/L (%)"]
    st.dataframe(_df_format_2dec(work[show_cols]), use_container_width=True, hide_index=True)

# --------- Rankingvy (lättvikt, baserar på befintliga kolumner om de finns) ---------
def view_ranking():
    st.header("Ranking")

    df = _ensure_df()
    if df.empty:
        return

    # Anta att databasen kan ha färdiga kolumner för riktkurser/uppsida
    # Om ej, visa en enkel ranking på P/S eller EV/S om de finns.
    ranking_candidates = [
        "Uppsida 1 år (%)",
        "Uppsida idag (%)",
        "Uppsida 2 år (%)",
        "Uppsida 3 år (%)",
        "EV/Revenue", "EV/EBITDA", "P/B", "PE FWD", "PE TTM",
    ]
    metric_col = st.selectbox("Välj rankingfält", ranking_candidates, index=0)
    ascending = st.checkbox("Stigande (lägst först)?", value=False)

    if metric_col not in df.columns:
        st.warning(f"Kolumnen **{metric_col}** saknas i datan. Visar endast råtabell.")
        st.dataframe(_df_format_2dec(df), use_container_width=True, hide_index=True)
        return

    work = df.copy()
    work[metric_col] = pd.to_numeric(work[metric_col], errors="coerce")
    work = work.sort_values(by=metric_col, ascending=ascending)
    st.dataframe(_df_format_2dec(work), use_container_width=True, hide_index=True)

# --------- Editor-vy (säker sparning av EPS/REV/CAGR) ---------
def view_editor():
    st.header("Editor")

    df = _ensure_df()
    if df.empty:
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    tickers.sort(key=lambda x: x.lower())
    sel = st.selectbox("Välj ticker att redigera", tickers, index=0 if tickers else None)
    if not sel:
        return

    idx = df.index[df["Ticker"].astype(str) == str(sel)]
    if len(idx) == 0:
        st.error("Kunde inte hitta vald rad.")
        return
    i = idx[0]
    row = df.loc[i]

    st.caption("Tomma fält är tillåtna (sparas som tomt/None).")
    c1, c2, c3 = st.columns(3)
    with c1:
        eps_ttm = st.text_input("EPS TTM", value="" if pd.isna(row.get("EPS TTM")) else str(row.get("EPS TTM")))
        eps_1y  = st.text_input("EPS 1Y",  value="" if pd.isna(row.get("EPS 1Y")) else str(row.get("EPS 1Y")))
        eps_2y  = st.text_input("EPS 2Y",  value="" if pd.isna(row.get("EPS 2Y")) else str(row.get("EPS 2Y")))
        eps_cagr = st.text_input("EPS CAGR", value="" if pd.isna(row.get("EPS CAGR")) else str(row.get("EPS CAGR")))
    with c2:
        rev_ttm = st.text_input("Rev TTM", value="" if pd.isna(row.get("Rev TTM")) else str(row.get("Rev TTM")))
        rev_1y  = st.text_input("Rev 1Y (miljoner)", value="" if pd.isna(row.get("Rev 1Y")) else str(row.get("Rev 1Y")))
        rev_2y  = st.text_input("Rev 2Y (miljoner)", value="" if pd.isna(row.get("Rev 2Y")) else str(row.get("Rev 2Y")))
        rev_cagr = st.text_input("Rev CAGR", value="" if pd.isna(row.get("Rev CAGR")) else str(row.get("Rev CAGR")))
    with c3:
        pe_ttm  = st.text_input("PE TTM", value="" if pd.isna(row.get("PE TTM")) else str(row.get("PE TTM")))
        pe_fwd  = st.text_input("PE FWD", value="" if pd.isna(row.get("PE FWD")) else str(row.get("PE FWD")))
        ev_sales = st.text_input("EV/Revenue", value="" if pd.isna(row.get("EV/Revenue")) else str(row.get("EV/Revenue")))
        ev_ebitda = st.text_input("EV/EBITDA", value="" if pd.isna(row.get("EV/EBITDA")) else str(row.get("EV/EBITDA")))

    # Spara-knapp
    if st.button("Spara ändringar (lokalt)", type="primary", use_container_width=True):
        # Robust parsing: tom str -> None; annars _f (från Del 1) som tål komma/space
        def P(x): 
            x = (x or "").strip()
            return None if x == "" else _f(x)

        updates = {
            "EPS TTM": P(eps_ttm),
            "EPS 1Y": P(eps_1y),
            "EPS 2Y": P(eps_2y),
            "EPS CAGR": P(eps_cagr),
            "Rev TTM": P(rev_ttm),
            "Rev 1Y": P(rev_1y),
            "Rev 2Y": P(rev_2y),
            "Rev CAGR": P(rev_cagr),
            "PE TTM": P(pe_ttm),
            "PE FWD": P(pe_fwd),
            "EV/Revenue": P(ev_sales),
            "EV/EBITDA": P(ev_ebitda),
            "Senast manuellt uppdaterad": _now_stamp(),
        }

        # Skriv tillbaka utan att kasta på None
        for k, v in updates.items():
            try:
                st.session_state["DATA"].at[i, k] = v
            except Exception:
                pass

        st.success(f"✅ Sparat lokalt för {sel}. (Ingen Google Sheets-skrivning i denna del.)")

    # Snabb beräkning från editor
    st.divider()
    st.caption("Snabbkontroll av beräkningar på vald rad")
    settings = _get_settings()
    fx_map = _get_fx_map()
    try:
        methods_df, sanity, meta = compute_methods_for_row(st.session_state["DATA"].loc[i], settings, fx_map)
        st.dataframe(_df_format_2dec(methods_df), use_container_width=True)
        with st.expander("Sanity"):
            st.write(sanity)
    except Exception as e:
        st.error(f"💥 Beräkning misslyckades: {e}")

# --------- Huvudnavigering (Denna körs i Del 4/4 main()) ---------
def render_views():
    menu = st.sidebar.radio("Meny", ["Analys", "Portfölj", "Ranking", "Editor"], index=0, horizontal=False)
    if menu == "Analys":
        view_analys()
    elif menu == "Portfölj":
        view_portfolio()
    elif menu == "Ranking":
        view_ranking()
    elif menu == "Editor":
        view_editor()

# ============================================================
# app.py — Del 4/4
# Main, Sheets I/O, Editor-stämplar, Settings, Batch, Snapshot
# + CHANGED: _build_updates_from_yahoo, _apply_updates_to_df_row, main()
# ============================================================

import pandas as pd
import numpy as np
import streamlit as st
import time

# ---------- Sheets I/O för session ----------
def _load_data_into_session():
    if "DATA" not in st.session_state or st.session_state["DATA"] is None or st.session_state["DATA"].empty:
        df = read_data_df()
        # Säkerställ editor-stämpelkolumner
        df = _ensure_editor_stamp_cols(df)
        st.session_state["DATA"] = df

def _write_session_to_sheets():
    if "DATA" not in st.session_state or st.session_state["DATA"] is None:
        st.warning("Ingen DATA i sessionen att spara.")
        return
    df = st.session_state["DATA"].copy()
    # Garanti kolumnordning + schema
    df = _ensure_columns(df, DATA_COLUMNS + EDITOR_STAMP_COLS)
    write_data_df(df)
    st.success("Sessionens DATA sparad till Google Sheets.")

# ---------- Editor-stämpelkolumner ----------
EDITOR_STAMP_COLS = [
    "EPS 1Y uppdaterad", "EPS 2Y uppdaterad",
    "Rev 1Y uppdaterad", "Rev 2Y uppdaterad",
    "Senast manuellt uppdaterad"
]

def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *EDITOR_STAMP_COLS])
    for c in EDITOR_STAMP_COLS:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _update_editor_stamps_on_change(df: pd.DataFrame, idx, old_row: pd.Series,
                                    new_vals: dict):
    """Sätt tidsstämplar när EPS/REV 1Y/2Y ändras manuellt."""
    changed = False
    mapping = {
        "EPS 1Y": "EPS 1Y uppdaterad",
        "EPS 2Y": "EPS 2Y uppdaterad",
        "Rev 1Y": "Rev 1Y uppdaterad",
        "Rev 2Y": "Rev 2Y uppdaterad",
    }
    for src, stamp_col in mapping.items():
        if src in new_vals:
            old_v = old_row.get(src)
            new_v = new_vals[src]
            eq = (pd.isna(old_v) and pd.isna(new_v)) or (not pd.isna(old_v) and not pd.isna(new_v) and float(old_v) == float(new_v))
            if not eq:
                df.at[idx, stamp_col] = now_stamp()
                changed = True
    if changed:
        df.at[idx, "Senast manuellt uppdaterad"] = now_stamp()

# ---------- “Äldst uppdaterad” lista ----------
def _oldest_eps_rev_table(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Ticker","Bolagsnamn","Äldst fält","Senast uppdaterad","Dagar"])
    work = df.copy()
    work = _ensure_editor_stamp_cols(work)
    for c in ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad","Rev 2Y uppdaterad"]:
        if c in work.columns:
            work[c] = pd.to_datetime(work[c], errors="coerce")
    now_ts = pd.Timestamp.now(tz="Europe/Stockholm")
    rows = []
    for _, r in work.iterrows():
        tkr = str(r.get("Ticker") or "")
        name = r.get("Bolagsnamn")
        stamps = {
            "EPS 1Y": r.get("EPS 1Y uppdaterad"),
            "EPS 2Y": r.get("EPS 2Y uppdaterad"),
            "Rev 1Y": r.get("Rev 1Y uppdaterad"),
            "Rev 2Y": r.get("Rev 2Y uppdaterad"),
        }
        avail = {k: v for k, v in stamps.items() if pd.notna(v)}
        if not avail:
            rows.append({"Ticker": tkr,"Bolagsnamn": name,"Äldst fält":"Inget datum","Senast uppdaterad": None,"Dagar": 9_999})
            continue
        oldest_field = min(avail, key=lambda k: avail[k])
        oldest_ts = avail[oldest_field]
        days = (now_ts - pd.Timestamp(oldest_ts, tz="Europe/Stockholm")).days if pd.notna(oldest_ts) else 9_999
        rows.append({
            "Ticker": tkr, "Bolagsnamn": name, "Äldst fält": oldest_field,
            "Senast uppdaterad": oldest_ts.date().isoformat() if pd.notna(oldest_ts) else None,
            "Dagar": days
        })
    out = pd.DataFrame(rows)
    out = out.sort_values("Dagar", ascending=False).head(top_n)
    return out

# ---------- CHANGED: Builder för Yahoo-uppdateringar ----------
def _clean_non_empty(d: dict) -> dict:
    """Behåll endast keys med riktiga värden (ej None/NaN/''), för 'skriv bara över fält vi har hämtat'."""
    out = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, float) and (pd.isna(v)):
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out

def _round2_or_none(x):
    v = _f(x)
    return None if v is None else round(float(v), 2)

def _maybe(x):  # enkel helper för att inte skriva sönder med NaN
    return x if (x is not None and not (isinstance(x, float) and pd.isna(x))) else None

# ---------- CHANGED: saknad funktion som orsakar 'name ... not defined' ----------
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    """
    Hämtar snapshot/estimat från Yahoo och returnerar:
      updates(dict), meta(dict), methods_df(DataFrame|empty)
    Endast fält som faktiskt har värden följer med i 'updates'.
    """
    snap = fetch_yahoo_snapshot(ticker)                 # pris/valuta/TTM/mm
    est  = fetch_yahoo_eps_estimates(ticker)            # EPS 1Y/2Y + longterm trend
    rc   = fetch_yahoo_rev_cagr(ticker)                 # Revenue CAGR (hist)
    ec   = fetch_yahoo_eps_cagr_hist(ticker)            # EPS CAGR (hist)

    updates = {
        "Timestamp": now_stamp(),
        "Bolagsnamn": _maybe(snap.get("company_name")),
        "Sektor": _maybe(snap.get("sector")),
        "Aktuell kurs": _round2_or_none(snap.get("price")),
        "Valuta": (snap.get("currency") or _maybe(existing_row.get("Valuta"))),
        "Utestående aktier": _maybe(snap.get("shares")),
        "Net debt": _maybe(snap.get("net_debt")),
        "Rev TTM": _maybe(snap.get("revenue_ttm")),
        "EBITDA TTM": _maybe(snap.get("ebitda_ttm")),
        "EPS TTM": _maybe(snap.get("eps_ttm")),
        "PE TTM": _maybe(snap.get("pe_ttm")),
        "PE FWD": _maybe(snap.get("pe_fwd")),
        "EV/Revenue": _maybe(snap.get("ev_to_sales")),
        "EV/EBITDA": _maybe(snap.get("ev_to_ebitda")),
        "P/B": _maybe(snap.get("p_to_book")),
        "BVPS": _maybe(snap.get("bvps")),
        "EPS 1Y": _maybe(est.get("eps_1y")),
        "EPS 2Y": _maybe(est.get("eps_2y")),
        "Rev CAGR": _maybe(rc.get("rev_cagr")),
        "EPS CAGR": _maybe(ec.get("eps_cagr")),
        "Årlig utdelning": _maybe(snap.get("annual_dividend")),
        "Utdelningsfrekvens": _maybe(snap.get("dividend_frequency")),
        "Senast auto uppdaterad": now_stamp(),
        "Auto källa": "Yahoo",
    }
    updates = _clean_non_empty(updates)

    # (Valfritt) – bygg metoder för logg/snapshot (behöver inte för massuppd.)
    try:
        methods_df, sanity, meta = compute_methods_for_row(existing_row, get_settings_map(), get_fx_map())
    except Exception:
        methods_df, sanity, meta = pd.DataFrame(), "", {"sources": snap.get("sources", {})}

    # Spara snapshot-rad om vi faktiskt har något pris
    try:
        if _f(snap.get("price")):
            _append_rows(SNAPSHOT_TITLE, [[
                now_stamp(),
                ticker,
                str(snap.get("currency") or existing_row.get("Valuta") or ""),
                "snapshot",
                _round2_or_none(snap.get("price")),
                None, None, None,
                _f(get_settings_map().get("pe_anchor_weight_ttm", 0.50)) or 0.50,
                _f(get_settings_map().get("multiple_decay", 0.10)) or 0.10,
            ]])
    except Exception:
        pass

    return updates, meta, methods_df

# ---------- CHANGED: applicera uppdateringar på en DF-rad ----------
def _apply_updates_to_df_row(df: pd.DataFrame, idx, updates: dict) -> int:
    changed = 0
    for k, v in (updates or {}).items():
        # skapa kolumn om den saknas
        if k not in df.columns:
            df[k] = np.nan
        old = df.at[idx, k]
        # jämför robust (NaN-safe)
        same = (pd.isna(old) and pd.isna(v)) or (not pd.isna(old) and not pd.isna(v) and str(old) == str(v))
        if same:
            continue
        df.at[idx, k] = v
        changed += 1
    return changed

# ---------- Yahoo enrich för vald rad (används i Editor) ----------
def _enrich_selected_from_yahoo_in_session(ticker: str):
    df = st.session_state["DATA"]
    mask = df["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if not mask.any():
        st.warning(f"{ticker}: finns inte i DATA.")
        return
    idx = df.index[mask][0]
    existing = df.loc[idx]
    updates, meta, methods_df = _build_updates_from_yahoo(ticker, existing)
    # skriv endast fält som faktiskt finns i updates
    _apply_updates_to_df_row(df, idx, updates)
    st.session_state["DATA"] = _ensure_editor_stamp_cols(df)

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

# ---------- Batch-sida (använder _build_updates_from_yahoo) ----------
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
        df_cur = _ensure_columns(df_cur, DATA_COLUMNS + EDITOR_STAMP_COLS)
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
                    # Stämpla om EPS/REV uppdaterats
                    _row_old = existing_row
                    changed_vals = {}
                    for f in ["EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y"]:
                        if f in updates:
                            changed_vals[f] = updates[f]
                    if changed_vals:
                        df_cur = _ensure_editor_stamp_cols(df_cur)
                        _update_editor_stamps_on_change(df_cur, idx, _row_old, changed_vals)
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

# ---------- Snapshot-visning ----------
def page_snapshot():
    st.header("🕒 Snapshot-logg")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True)

# ---------- CHANGED: Main ----------
def main():
    # Ladda DATA i sessionen
    _load_data_into_session()

    st.sidebar.title("Navigering")
    if st.sidebar.button("↻ Läs om från Google Sheets"):
        st.session_state["DATA"] = _ensure_editor_stamp_cols(read_data_df())
        st.success("Läste om DATA från Google Sheets.")
        st.rerun()
    if st.sidebar.button("⬆️ Spara session → Google Sheets"):
        _write_session_to_sheets()

    page = st.sidebar.radio("Gå till:", ["Analys","Portfölj","Ranking","Editor","Batch","Settings","Snapshot"], index=0)

    # Hämta basmaps
    settings = get_settings_map()
    fx_map   = get_fx_map()
    df_data  = st.session_state.get("DATA", pd.DataFrame())

    try:
        if page == "Analys":
            page_analysis(df_data, settings, fx_map)
        elif page == "Portfölj":
            page_portfolio(df_data, settings, fx_map)
        elif page == "Ranking":
            page_ranking(df_data, settings, fx_map)
        elif page == "Editor":
            page_editor(df_data, settings)
        elif page == "Batch":
            page_batch()
        elif page == "Settings":
            page_settings()
        elif page == "Snapshot":
            page_snapshot()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# Entrypoint
if __name__ == "__main__":
    main()
