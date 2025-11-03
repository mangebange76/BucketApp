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
    # ▼▼ Nya kolumner (nödvändiga för att spara Yahoo Revenue Estimates) ▼▼
    "Rev FY1 (est)","Rev FY2 (est)",
    # ▲▲
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
        "Nästa utdelning (per aktie)",
        "Rev FY1 (est)","Rev FY2 (est)"
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
        "Nästa utdelning (per aktie)",
        "Rev FY1 (est)","Rev FY2 (est)"
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
# app.py — Del 2/4
# Datainsamling (Yahoo/Finnhub) & beräkningsmotor
#  • TTM via kvartalssummor (EPS/Rev/EBITDA) från Yahoo
#  • EPS-estimat (current/next FY) via Yahoo trend
#  • Revenue-estimat (current/next FY) via Yahoo Analysis
#  • REV-path prioriterar FY1/FY2-estimat (hanterar olika räkenskapsår)
# ============================================================

import requests
import pandas as pd
import numpy as np
import math
import time
import streamlit as st
import yfinance as yf
from typing import Dict, Any, Optional, Tuple, List

# -------------------------
# Små hjälpare (index-pick, TTM-summerare)
# -------------------------
def _ix_pick(df: pd.DataFrame, candidates: List[str]):
    """Hitta rad i df (index) via kandidater — case/space-insensitivt, tolererar variationer."""
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
    # "contains"-sök
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
    return _sum_last4(ser_like)

def _f(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and (x != x)):
            return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            if s == "":
                return None
            v = float(s)
        else:
            v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _pos(x) -> Optional[float]:
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(x, fb=None):
    return x if (x is not None and x == x) else fb

# -------------------------
# Yahoo (yfinance) – robust snapshot
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Pris/valuta + TTM-nycklar (byggt från kvartal om möjligt) och EV/net debt.
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {"sources": {}}

    # Fast-info
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

    # Namn/sector
    try:
        cname = gi("longName") or gi("shortName")
        if cname:   out["company_name"] = str(cname); out["sources"]["company_name"] = "yahoo_info"
        sector = gi("sector")
        if sector:  out["sector"] = str(sector); out["sources"]["sector"] = "yahoo_info"
        industry = gi("industry")
        if industry:out["industry"] = str(industry); out["sources"]["industry"] = "yahoo_info"
    except Exception:
        pass

    # EV / net debt
    total_debt = _f(gi("totalDebt"))
    total_cash = _f(gi("totalCash"))
    ev_info    = _f(gi("enterpriseValue"))
    if ev_info is not None:
        out["ev"] = ev_info; out["sources"]["ev"] = "yahoo_info"
    elif _pos(out.get("market_cap")) and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash; out["sources"]["ev"] = "calc_mc+debt-cash"
    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]; out["sources"]["net_debt"] = "calc_ev-mcap"

    # Shares via MCAP/price
    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        out["shares"] = out["market_cap"] / out["price"]; out["sources"]["shares"] = "derived_mcap/price"

    # Pris-historik fallback
    if not _pos(out.get("price")):
        try:
            hist = tk.history(period="5d")
            if not hist.empty:
                out["price"] = float(hist["Close"].dropna().iloc[-1]); out["sources"]["price"] = "yahoo_hist_close"
        except Exception:
            pass

    # TTM via kvartal (income statement quarterly)
    EPS_KEYS_Q     = ["DilutedEPS", "BasicEPS", "EPS"]
    REV_KEYS_Q     = ["TotalRevenue", "Total Revenue", "Revenue"]
    EBITDA_KEYS_Q  = ["Ebitda", "EBITDA"]

    try:
        try:
            inc_q = tk.get_income_stmt(freq="quarterly")
        except Exception:
            inc_q = getattr(tk, "quarterly_income_stmt", None) or getattr(tk, "income_stmt", None)

        if inc_q is not None and not getattr(inc_q, "empty", True):
            dfq = inc_q.copy()

            eps_row = _ix_pick(dfq, EPS_KEYS_Q)
            rev_row = _ix_pick(dfq, REV_KEYS_Q)
            ebitda_row = _ix_pick(dfq, EBITDA_KEYS_Q)

            eps_ttm_q    = _sum_eps_last4(eps_row) if eps_row is not None else None
            rev_ttm_q    = _sum_last4(rev_row) if rev_row is not None else None
            ebitda_ttm_q = _sum_last4(ebitda_row) if ebitda_row is not None else None

            # EPS fallback via NetIncome / DilutedShares om saknas
            if eps_ttm_q is None:
                net_row = _ix_pick(dfq, ["NetIncome","Net Income","NetIncomeCommonStockholders"])
                shd_row = _ix_pick(dfq, ["DilutedAverageShares","AverageDilutedSharesOutstanding","WeightedAverageDilutedSharesOutstanding"])
                if net_row is not None and shd_row is not None:
                    ni_ttm = _sum_last4(net_row); sh_ttm = _sum_last4(shd_row)
                    if _pos(ni_ttm) and _pos(sh_ttm) and sh_ttm != 0:
                        eps_ttm_q = float(ni_ttm) / float(sh_ttm)

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

    # Härled multiplar
    if _pos(out.get("ev")) and _pos(out.get("revenue_ttm")):
        out["ev_to_sales"] = float(out["ev"]) / float(out["revenue_ttm"]); out["sources"]["ev_to_sales"] = out["sources"].get("revenue_ttm","calc_ev/sales")
    if _pos(out.get("ev")) and out.get("ebitda_ttm") is not None:
        e = float(out["ebitda_ttm"]); out["ev_to_ebitda"] = (float(out["ev"]) / e) if e != 0 else None; out["sources"]["ev_to_ebitda"] = out["sources"].get("ebitda_ttm","calc_ev/ebitda")

    out["currency"] = str(out.get("currency") or "USD").upper()
    return out

# -------------------------
# Yahoo – EPS-estimat (trend)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    EPS current FY och next FY + long-term growth (Yahoo earnings trend).
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

        def _avg_from_cell(val) -> Optional[float]:
            if isinstance(val, dict):
                for k in ("avg","average","mean"):
                    if k in val and _f(val[k]) is not None:
                        return _f(val[k])
            return _f(val)

        def _pick_row(period_aliases: List[str]):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for alias in period_aliases:
                a = m.str.contains(rf"^{alias}$")
                mask = a if mask is None else (mask | a)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_nextyear    = _pick_row(["nextyear","next fiscal year","nextfiscalyear"])
        row_longterm    = _pick_row(["longterm","next5years","next 5 years"])
        row_currentyear = _pick_row(["currentyear","current fiscal year","currentfiscalyear"])

        eps_1y = None
        if row_currentyear is not None:
            for col in ["earningsestimate","epsestimate","epstrend"]:
                if col in df.columns:
                    eps_1y = _avg_from_cell(row_currentyear.get(col))
                    if eps_1y is not None: break
        if eps_1y is None and row_nextyear is not None:
            for col in ["earningsestimate","epsestimate","epstrend"]:
                if col in df.columns:
                    eps_1y = _avg_from_cell(row_nextyear.get(col))
                    if eps_1y is not None: break

        eps_cagr_long = None
        if row_longterm is not None:
            for col in ["growth","longtermgrowthrate"]:
                if col in df.columns:
                    eps_cagr_long = _f(row_longterm.get(col))
                    if eps_cagr_long is not None: break

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": eps_cagr_long, "source": "yahoo_trend"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

# -------------------------
# Yahoo – Revenue-estimat (Analysis: Current FY / Next FY)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_revenue_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker plocka Revenue 'Current Year' och 'Next Year' (Avg Estimate) från Yahoo Analysis.
    Hanterar olika DataFrame-strukturer (MultiIndex/flat) samt transponerade varianter.
    Returnerar: {"rev_fy1": float|None, "rev_fy2": float|None, "source": "..."}.
    """
    tk = yf.Ticker(ticker)
    rev_fy1, rev_fy2, src = None, None, "none"

    # 1) Nyare yfinance: get_analysis()
    ana = None
    try:
        ana = tk.get_analysis()
    except Exception:
        try:
            ana = getattr(tk, "analysis", None)
        except Exception:
            ana = None

    def _pick_col(row_like, names: List[str]):
        if row_like is None: return None
        cols = [str(c).lower() for c in row_like.index] if isinstance(row_like, pd.Series) else []
        # prova i ordning
        for alias in names:
            for c in row_like.index:
                if alias in str(c).lower():
                    v = _f(row_like[c])
                    if v is not None: 
                        return v
        return None

    if isinstance(ana, pd.DataFrame) and not ana.empty:
        df = ana.copy()
        # Case A: MultiIndex i index (('Revenue Estimate','Avg Estimate'), ...)
        if isinstance(df.index, pd.MultiIndex):
            df.index = pd.MultiIndex.from_tuples(tuple((str(a).lower(), str(b).lower()) for a,b in df.index))
            df.columns = [str(c).lower() for c in df.columns]
            # hitta rad ('revenue estimate','avg estimate'|varianter)
            row = None
            for cand in [
                ("revenue estimate","avg estimate"),
                ("revenue estimate","average estimate"),
                ("revenue estimate","avg. estimate"),
                ("revenue estimate","avg"),
            ]:
                if cand in df.index:
                    row = df.loc[cand]; break
            if row is None:
                # fallback: första rad där level0 innehåller 'revenue' och level1 'avg'
                for idx in df.index:
                    if "revenue" in idx[0] and "avg" in idx[1]:
                        row = df.loc[idx]; break
            if row is not None:
                rev_fy1 = _pick_col(row, ["current year","curr year","this year","fiscal year"])
                rev_fy2 = _pick_col(row, ["next year","nex year"])
                if rev_fy1 is not None or rev_fy2 is not None:
                    src = "yahoo_analysis_multiindex"
        else:
            # Case B: platt DF – försök med rad 'Avg Estimate'
            df.index = [str(i).lower() for i in df.index]
            df.columns = [str(c).lower() for c in df.columns]
            row = None
            for rname in ["avg estimate","average estimate","avg. estimate","avg"]:
                if rname in df.index:
                    row = df.loc[rname]; break
            if isinstance(row, pd.Series):
                rev_fy1 = _pick_col(row, ["current year","curr year","this year","fiscal year"])
                rev_fy2 = _pick_col(row, ["next year","nex year"])
                if rev_fy1 is not None or rev_fy2 is not None:
                    src = "yahoo_analysis_flat"
            else:
                # Case C: transponerad tabell – leta kolumn 'avg estimate'
                if "avg estimate" in df.columns or "average estimate" in df.columns:
                    col = "avg estimate" if "avg estimate" in df.columns else "average estimate"
                    # leta rader 'revenue estimate' + ('current year'/'next year') i andra dimensionen
                    # vanligen är rader typ 'current year'/'next year'
                    def _pick_row(names: List[str]):
                        for idx in df.index:
                            sidx = str(idx).lower()
                            if any(n in sidx for n in names):
                                v = _f(df.loc[idx, col])
                                if v is not None: return v
                        return None
                    rev_fy1 = _pick_row(["current year","curr year","this year","fiscal year"])
                    rev_fy2 = _pick_row(["next year","nex year"])
                    if rev_fy1 is not None or rev_fy2 is not None:
                        src = "yahoo_analysis_transposed"

    return {"rev_fy1": rev_fy1, "rev_fy2": rev_fy2, "source": src}

# -------------------------
# Yahoo – Revenue CAGR från årsdata (fallback)
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
        total_rev = _ix_pick(df, ["TotalRevenue","Total Revenue","Revenue"])
        if total_rev is None:
            return {"rev_cagr": None, "years": None, "source": "none"}
        ser = pd.to_numeric(pd.Series(total_rev), errors="coerce").dropna()
        if ser.empty:
            return {"rev_cagr": None, "years": None, "source": "none"}
        try:
            ser.index = pd.to_datetime(ser.index, errors="coerce"); ser = ser.sort_index()
        except Exception:
            pass
        vals = ser.dropna().values.tolist()
        if len(vals) < 2:
            return {"rev_cagr": None, "years": None, "source": "none"}
        n = min(max_years, len(vals)); vals = vals[-n:]; n_years = len(vals) - 1
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
# Finnhub fallback (EPS)
# -------------------------
def _get_finnhub_key() -> Optional[str]:
    from os import environ
    return (environ.get("FINNHUB_API_KEY") or environ.get("FINNHUB_TOKEN") or
            _env_or_secret("FINNHUB_API_KEY") or _env_or_secret("FINNHUB_TOKEN"))

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
            vals = [_f(x.get("epsAvg")) for x in rows if _f(x.get("epsAvg")) is not None]
            if len(vals) >= 1: eps_1y = vals[-1]
            if len(vals) >= 2: eps_2y = vals[-2]
        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "source": "finnhub"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "source": "none"}

# -------------------------
# Multipel-decay & P/E-ankare
# -------------------------
def _clamp(val: Optional[float], lo: float, hi: float) -> Optional[float]:
    if val is None: return None
    try:
        v = float(val)
        if not math.isfinite(v): return None
        return max(lo, min(hi, v))
    except Exception:
        return None

REV_CAGR_MIN = -0.10
REV_CAGR_MAX =  0.35
EPS_CAGR_MIN = -0.20
EPS_CAGR_MAX =  0.40

def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    m0 = _pos(mult0)
    if m0 is None: return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_frac
    return max(m, floor)

def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float) -> Optional[float]:
    pt = _pos(pe_ttm); pf = _pos(pe_fwd)
    if pt is None and pf is None: return None
    if pt is None: return pf
    if pf is None: return pt
    return w_ttm * pt + (1.0 - w_ttm) * pf

# -------------------------
# Builders (pris/EV)
# -------------------------
def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float], shares_fd: Optional[float]) -> Optional[float]:
    e = _pos(ev_target); s = _pos(shares_fd)
    if e is None or s is None: return None
    nd = _nz(net_debt, 0.0)
    try: return max(0.0, (e - nd) / s)
    except Exception: return None

def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e = _pos(eps); p = _pos(pe)
    if e is None or p is None: return None
    return e * p

def _ev_from_sales(rev: Optional[float], mult: Optional[float]) -> Optional[float]:
    r = _pos(rev); m = _pos(mult)
    if r is None or m is None: return None
    return r * m

def _ev_from_ebitda(ebitda: Optional[float], mult: Optional[float]) -> Optional[float]:
    e = _f(ebitda); m = _pos(mult)
    if e is None or m is None: return None
    return e * m

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb); b = _pos(bvps)
    if p is None or b is None: return None
    return p * b

# -------------------------
# EPS/REV/EBITDA paths (REV-path använder FY-estimat)
# -------------------------
def _derive_eps_ttm_from_pe_only(price: Optional[float], pe_ttm: Optional[float],
                                 eps_ttm: Optional[float]) -> Tuple[Optional[float], str]:
    src_ttm = "source" if eps_ttm is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe_ttm"
    return eps_ttm, src_ttm

def _eps_path_fill(eps_ttm: Optional[float], eps_1y: Optional[float], eps_2y: Optional[float],
                   eps_cagr: Optional[float], rev_cagr_fallback: Optional[float]) -> Tuple[float, float, float, float]:
    e0 = _pos(eps_ttm) or 0.0
    e1 = _pos(eps_1y); e2 = _pos(eps_2y)
    g  = _f(eps_cagr)
    if g is None: g = _f(rev_cagr_fallback)
    if g is None: g = 0.0
    if e1 is None: e1 = e0 * (1.0 + g)
    if e2 is None: e2 = e1 * (1.0 + g)
    e3 = e2 * (1.0 + g)
    return float(e0), float(e1), float(e2), float(e3)

def _rev_path_with_estimates(rev_ttm: Optional[float],
                             rev_fy1: Optional[float], rev_fy2: Optional[float],
                             rev_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    r0 = TTM, r1 = FY1-estimat om finns (annars CAGR på r0), r2 = FY2-estimat om finns (annars CAGR),
    r3 = extrapoleras från r1->r2 tillväxt (om båda finns) annars CAGR från r2.
    """
    r0 = _pos(rev_ttm)
    g  = _f(rev_cagr)
    r1 = _pos(rev_fy1)
    r2 = _pos(rev_fy2)

    if r0 is None and r1 is not None and g is not None:
        # saknar TTM – backa en period med CAGR som approximation
        try: r0 = r1 / (1.0 + g)
        except Exception: r0 = None

    if r1 is None and r0 is not None and g is not None:
        r1 = r0 * (1.0 + g)
    if r2 is None and r1 is not None and g is not None:
        r2 = r1 * (1.0 + g)

    # y3: om både r1 och r2 finns, använd deras tillväxt; annars CAGR
    r3 = None
    try:
        if _pos(r1) and _pos(r2):
            growth12 = (float(r2) / float(r1)) - 1.0
            r3 = float(r2) * (1.0 + growth12)
        elif _pos(r2) and g is not None:
            r3 = float(r2) * (1.0 + g)
        elif _pos(r1) and g is not None:
            r2 = float(r1) * (1.0 + g)
            r3 = float(r2) * (1.0 + g)
    except Exception:
        r3 = None
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    e0 = _f(ebitda_ttm)  # kan vara <= 0
    if e0 is None: return None, None, None, None
    if rev0 is None or rev1 is None: return e0, e0, e0, e0
    def scale(r):
        try: return (e0 * (r / rev0)) if (r and rev0) else e0
        except Exception: return e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# -------------------------
# Huvudmotor per rad (värderingsmetoder)
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    ticker = str(row.get("Ticker", "")).strip().upper()

    # 1) Live-data
    snap    = fetch_yahoo_snapshot(ticker)
    time.sleep(0.15)  # mild throttling
    yh_eps  = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.06)
    yh_rev_est = fetch_yahoo_revenue_estimates(ticker)     # <<– NEW
    revcg_yh   = fetch_yahoo_rev_cagr(ticker)

    fh = fetch_finnhub_estimates(ticker)  # EPS fallback

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

    # Revenue FY1/FY2 — använd Excel/Sheets-värden om redan ifyllda, annars Yahoo Analysis
    rev_fy1_est = _pos(_nz(row.get("Rev FY1 (est)"), yh_rev_est.get("rev_fy1")))
    rev_fy2_est = _pos(_nz(row.get("Rev FY2 (est)"), yh_rev_est.get("rev_fy2")))

    # EPS CAGR
    eps_cagr_raw = _f(row.get("EPS CAGR"))
    if eps_cagr_raw is None and yh_eps.get("eps_cagr_long") is not None:
        eps_cagr_raw = _f(yh_eps.get("eps_cagr_long"))
    if eps_cagr_raw is None and _pos(eps_ttm) and _pos(eps_1y_est):
        try: eps_cagr_raw = (float(eps_1y_est)/float(eps_ttm)) - 1.0
        except Exception: eps_cagr_raw = None
    eps_cagr = _clamp(eps_cagr_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    # Rev CAGR (fallback när FY-estimat saknas)
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

    # 5) Paths — EPS & REV (REV använder FY1/FY2 om finns)
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr, rev_cagr)
    r0, r1, r2, r3 = _rev_path_with_estimates(_f(rev_ttm), rev_fy1_est, rev_fy2_est, rev_cagr)
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
    for m in ("p_nav","p_tbv","p_affo","p_fcf","ev_fcf","p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # 7) Sanity + META
    src = snap.get("sources", {}) or {}
    eps1_src = "yahoo_trend" if _pos(yh_eps.get("eps_1y")) else ("finnhub" if _pos(fh.get("eps_1y")) else ("sheet" if _pos(row.get("EPS 1Y")) else "filled_by_rule"))
    eps2_src = "yahoo_trend" if _pos(yh_eps.get("eps_2y")) else ("finnhub" if _pos(fh.get("eps_2y")) else ("sheet/derived" if _pos(row.get("EPS 2Y")) else "filled_by_rule"))

    rev_est_src = yh_rev_est.get("source") if (rev_fy1_est or rev_fy2_est) else ("sheet" if (_pos(row.get("Rev FY1 (est)")) or _pos(row.get("Rev FY2 (est)"))) else "none")
    revc_src = "yahoo_financials" if revcg_yh.get("rev_cagr") is not None else ("sheet" if _f(row.get("Rev CAGR")) is not None else "none")

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if e0 or e0==0 else '—'}({src.get('eps_ttm','?') or ('derived' if str(src_eps_ttm).startswith('derived') else src_eps_ttm)}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_FY1/FY2={'ok' if (r1 or r2) else '—'}({rev_est_src}), "
        f"rev_cagr={'ok' if _f(rev_cagr) is not None else '—'}({revc_src}; clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
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
        "sources": {
            **src,
            "eps_1y_source": eps1_src,
            "eps_2y_source": eps2_src,
            "rev_est_source": rev_est_src,
            "rev_cagr_source": revc_src,
        },
        "cagr_clamped": {
            "rev_cagr_used": _f(rev_cagr),
            "eps_cagr_used": _f(eps_cagr),
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        # Visa också FY-estimat separat för transparens
        "rev_estimates": {"fy1": rev_fy1_est, "fy2": rev_fy2_est},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# ============================================================
# Del 2/4 slut — fortsätt i Del 3/4 (Analys/Portfölj/Ranking UI)
# ============================================================

# ============================================================
# app.py — Del 3/4
# UI: Analys / Portfölj / Ranking
#  • Analys: kör motor för vald ticker, visar metoder + FY-estimat
#  • Spara riktkurser (Idag, 1, 2, 3 år) tillbaka till DATA
#  • Portfölj: enkel summering (SEK) + tabell
#  • Ranking: kör beräkning för alla och sorterar på uppsida
# Kräver:
#   st.session_state['DATA']        -> DataFrame med minst kolumnen 'Ticker'
#   st.session_state['SETTINGS']    -> dict (pe_anchor_weight_ttm, multiple_decay) – valfritt
#   st.session_state['FX']          -> dict valutakurser till SEK, t.ex. {'USD':10.5,'NOK':1.0,...} – valfritt
# ============================================================

import time
import math
import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime

# ---------- Hjälp ----------
def _get_data_df() -> pd.DataFrame:
    df = st.session_state.get("DATA")
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame()

def _get_settings() -> dict:
    d = st.session_state.get("SETTINGS") or {}
    # rimliga defaults
    return {
        "pe_anchor_weight_ttm": d.get("pe_anchor_weight_ttm", 0.50),
        "multiple_decay": d.get("multiple_decay", 0.10),
    }

def _get_fx() -> dict:
    # Bas SEK=1.0 om saknas
    fx = st.session_state.get("FX") or {}
    fx.setdefault("SEK", 1.0)
    return fx

def _fmt(x, nd=2):
    try:
        if x is None or (isinstance(x, float) and (x != x)):
            return "—"
        return f"{float(x):,.{nd}f}".replace(",", " ").replace(".", ",")
    except Exception:
        return "—"

def _to_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            return float(s) if s != "" else None
        return float(x)
    except Exception:
        return None

def _median_ignore_nan(vals):
    arr = [float(v) for v in vals if v is not None and v == v]
    return float(np.median(arr)) if arr else None

def _aggregate_targets(methods_df: pd.DataFrame, mode: str, chosen_method: str | None = None):
    """
    Returnerar (t0, t1, t2, t3) enligt:
      - mode == 'Median över alla metoder'  -> median kolumnvis
      - mode == 'Välj metod' + chosen_method -> hämtar rad för vald metod
    """
    if methods_df is None or methods_df.empty:
        return None, None, None, None

    cols = ["Idag", "1 år", "2 år", "3 år"]

    if mode == "Välj metod" and chosen_method:
        row = methods_df[methods_df["Metod"] == chosen_method]
        if not row.empty:
            r0 = _to_float(row.iloc[0].get("Idag"))
            r1 = _to_float(row.iloc[0].get("1 år"))
            r2 = _to_float(row.iloc[0].get("2 år"))
            r3 = _to_float(row.iloc[0].get("3 år"))
            return r0, r1, r2, r3
        # fall back till median om metoden inte fanns
    # Median över alla
    t = []
    for c in cols:
        t.append(_median_ignore_nan([_to_float(v) for v in methods_df[c].tolist()]))
    return tuple(t)

def _jump_to(tab_name: str):
    st.session_state["__active_tab__"] = tab_name

# ---------------- UI-start ----------------
st.header("Analys & Portfölj")

df = _get_data_df()
if df.empty:
    st.info("Ingen data laddad ännu. Lägg in en DataFrame i `st.session_state['DATA']` i Del 1/4.")
    st.stop()

settings = _get_settings()
fx_map = _get_fx()

# Välj aktiv flik (behåller senast vald)
tabs = st.tabs(["🔎 Analys", "🧺 Portfölj", "📈 Ranking"])

# ============================================================
# 🔎 Analys
# ============================================================
with tabs[0]:
    left, right = st.columns([2, 3])

    # Välj ticker
    with left:
        tickers = sorted(df["Ticker"].astype(str).unique().tolist())
        default_ix = 0
        if "ANALYS_TICKER" in st.session_state and st.session_state["ANALYS_TICKER"] in tickers:
            default_ix = tickers.index(st.session_state["ANALYS_TICKER"])
        ticker = st.selectbox("Välj ticker", tickers, index=default_ix, key="ANALYS_TICKER")
        rad = df[df["Ticker"] == ticker]
        if rad.empty:
            st.warning("Kunde inte hitta rad för vald ticker.")
            st.stop()
        row = rad.iloc[0]

        # Kör motor (Del 2/4)
        from math import isnan  # bara för typer
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

        st.caption("Beräknade metoder")
        st.dataframe(
            methods_df.style.format(
                {"Idag": lambda v: _fmt(v), "1 år": lambda v: _fmt(v),
                 "2 år": lambda v: _fmt(v), "3 år": lambda v: _fmt(v)}
            ),
            use_container_width=True,
            hide_index=True
        )

        # Välj sammanställningssätt
        st.subheader("Riktkurs – sammanställning")
        agg_mode = st.radio(
            "Hur vill du sammanställa riktkurserna?",
            ["Median över alla metoder", "Välj metod"],
            horizontal=True
        )
        chosen_method = None
        if agg_mode == "Välj metod":
            chosen_method = st.selectbox("Välj metod", methods_df["Metod"].tolist())

        t0, t1, t2, t3 = _aggregate_targets(methods_df, agg_mode, chosen_method)

        # Visa riktkurser
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Riktkurs idag", _fmt(t0))
        c2.metric("Riktkurs 1 år", _fmt(t1))
        c3.metric("Riktkurs 2 år", _fmt(t2))
        c4.metric("Riktkurs 3 år", _fmt(t3))

        # Spara-knappar
        st.markdown("---")
        colA, colB = st.columns([1, 1])
        with colA:
            if st.button("💾 Spara riktkurser till DATA", use_container_width=True):
                try:
                    idx = df.index[df["Ticker"] == ticker][0]
                    # skriv tillbaka
                    for k, v in [
                        ("Riktkurs idag", t0),
                        ("Riktkurs om 1 år", t1),
                        ("Riktkurs om 2 år", t2),
                        ("Riktkurs om 3 år", t3),
                        ("Senast beräknad", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    ]:
                        df.loc[idx, k] = v
                    st.session_state["DATA"] = df
                    st.success("Riktkurser sparade till DATA.")
                except Exception as e:
                    st.error(f"Kunde inte spara: {e}")

        with colB:
            # Om du har en spar-funktion till Sheets i Del 1/4 kan du lägga den i session_state
            # t.ex. st.session_state['SAVE_ROWS_TO_SHEETS'](rows=[...])
            if st.button("🧾 (Valfritt) Spara till Google Sheets", use_container_width=True):
                saver = st.session_state.get("SAVE_ROWS_TO_SHEETS")
                if callable(saver):
                    try:
                        idx = df.index[df["Ticker"] == ticker][0]
                        saver(rows=[df.loc[idx:idx]])
                        st.success("Riktkurser skickade till Google Sheets.")
                    except Exception as e:
                        st.error(f"Sparning till Sheets misslyckades: {e}")
                else:
                    st.info("Ingen Sheets-sparfunktion registrerad i denna session.")

    # Meta / sanity & företagssammanfattning
    with right:
        st.subheader("Företagsinfo")
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Valuta", meta.get("currency") or "—")
        cc2.metric("Aktuell kurs", _fmt(meta.get("price")))
        cc3.metric("Utest. aktier", _fmt(meta.get("shares_out"), 0))

        cc4, cc5, cc6 = st.columns(3)
        cc4.metric("Net debt", _fmt(meta.get("net_debt"), 0))
        cc5.metric("PE-ankare", _fmt(meta.get("pe_anchor")))
        cc6.metric("Decay", _fmt(meta.get("decay")))

        st.caption(f"**Sanity**: {sanity}")

        with st.expander("🔬 Källor & beräkningsväg (inkl. FY-estimat)"):
            # Visa tydligt FY-estimaten och paths
            st.markdown("**EPS-path** (TTM → Y1 → Y2 → Y3)")
            st.write(meta.get("eps_path"))
            st.markdown("**Revenue FY-estimat (Yahoo Analysis)**")
            st.write(meta.get("rev_estimates"))  # {'fy1': ..., 'fy2': ...}
            st.markdown("**Revenue-path** (TTM → Y1 → Y2 → Y3)")
            st.write(meta.get("rev_path"))
            st.markdown("**EBITDA-path** (skalad av revenue-path)")
            st.write(meta.get("ebitda_path"))
            st.markdown("**Källa per fält**")
            st.json(meta.get("sources"))

# ============================================================
# 🧺 Portfölj
# ============================================================
with tabs[1]:
    st.subheader("Portföljsammanställning (SEK)")
    dfp = df.copy()

    # Försök räkna värde i SEK om möjligt
    def _fx_to_sek(curr: str) -> float:
        if not curr:
            return 1.0
        return float(_to_float(fx_map.get(str(curr).upper(), 1.0)) or 1.0)

    # Skapa kolumner om saknas
    for col in ["Antal aktier", "Aktuell kurs", "Valuta"]:
        if col not in dfp.columns:
            dfp[col] = np.nan

    dfp["FX→SEK"] = dfp["Valuta"].astype(str).map(lambda c: _fx_to_sek(c))
    dfp["Värde (SEK)"] = (
        dfp["Antal aktier"].map(_to_float).fillna(0.0)
        * dfp["Aktuell kurs"].map(_to_float).fillna(0.0)
        * dfp["FX→SEK"].fillna(1.0)
    )

    total_value = float(dfp["Värde (SEK)"].sum()) if not dfp.empty else 0.0
    st.metric("Totalt portföljvärde (SEK)", _fmt(total_value, 0))

    st.dataframe(
        dfp[["Ticker", "Antal aktier", "Aktuell kurs", "Valuta", "FX→SEK", "Värde (SEK)"]]
        .sort_values("Värde (SEK)", ascending=False),
        use_container_width=True
    )

# ============================================================
# 📈 Ranking
# ============================================================
with tabs[2]:
    st.subheader("Ranking efter uppsida")
    st.caption("Beräknar riktkurser (median över metoder) för alla tickers och räknar uppsida mot aktuell kurs.")

    run = st.button("Kör ranking nu")
    if run:
        rows = []
        t0 = time.time()
        for i, (_, r) in enumerate(df.iterrows(), 1):
            try:
                methods_df, sanity, meta = compute_methods_for_row(r, settings, fx_map)
                p = _to_float(meta.get("price"))
                # median-aggregat
                tgt0, tgt1, tgt2, tgt3 = _aggregate_targets(methods_df, "Median över alla metoder")
                up0 = (tgt0 / p - 1.0) * 100 if (tgt0 and p and p != 0) else None
                up1 = (tgt1 / p - 1.0) * 100 if (tgt1 and p and p != 0) else None
                up2 = (tgt2 / p - 1.0) * 100 if (tgt2 and p and p != 0) else None
                up3 = (tgt3 / p - 1.0) * 100 if (tgt3 and p and p != 0) else None
                rows.append({
                    "Ticker": r.get("Ticker"),
                    "Kurs": p,
                    "Riktkurs idag": tgt0, "Uppsida idag (%)": up0,
                    "Riktkurs 1 år": tgt1, "Uppsida 1 år (%)": up1,
                    "Riktkurs 2 år": tgt2, "Uppsida 2 år (%)": up2,
                    "Riktkurs 3 år": tgt3, "Uppsida 3 år (%)": up3,
                })
                # liten paus för att vara snäll mot externa källor
                time.sleep(0.05)
            except Exception as e:
                rows.append({"Ticker": r.get("Ticker"), "Fel": str(e)})

        rank_df = pd.DataFrame(rows)
        # sortera på 1-års uppsida om finns, annars idag
        if "Uppsida 1 år (%)" in rank_df.columns:
            rank_df = rank_df.sort_values(by=["Uppsida 1 år (%)", "Uppsida idag (%)"], ascending=False, na_position="last")
        st.dataframe(
            rank_df.style.format({
                "Kurs": _fmt,
                "Riktkurs idag": _fmt, "Uppsida idag (%)": lambda v: _fmt(v, 2),
                "Riktkurs 1 år": _fmt, "Uppsida 1 år (%)": lambda v: _fmt(v, 2),
                "Riktkurs 2 år": _fmt, "Uppsida 2 år (%)": lambda v: _fmt(v, 2),
                "Riktkurs 3 år": _fmt, "Uppsida 3 år (%)": lambda v: _fmt(v, 2),
            }),
            use_container_width=True
        )
        st.caption(f"Klar på {_fmt(time.time()-t0, 2)} s")

# ============================================================
# Del 3/4 slut — fortsätt i Del 4/4 (Spar & glue)
# ============================================================

# ============================================================
# app.py — Del 4/4
# Spar & Glue: Google Sheets + Yahoo FY-estimat (EPS/Revenue)
# • Optional Sheets-I/O via st.secrets["GOOGLE_CREDENTIALS"]
# • Sidebar: Ladda/Spara DATA
# • Knapp: Uppdatera EPS & Revenue (FY1/FY2) från Yahoo och spara
# • Patch: meta["rev_estimates"] + meta["rev_path"] justeras till FY-estimat
# ============================================================

from __future__ import annotations
import json, time, math
from typing import Any, Dict, Optional, Tuple
import pandas as pd
import numpy as np
import streamlit as st
import yfinance as yf

# ---------- (A) Google Sheets – optional ----------
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _has_sheets_cfg() -> bool:
    try:
        s = st.secrets
    except Exception:
        return False
    return ("GOOGLE_CREDENTIALS" in s) and ("SHEET_URL" in s or "SHEET_ID" in s)

def _sheets_client():
    import gspread
    from google.oauth2.service_account import Credentials
    raw = st.secrets["GOOGLE_CREDENTIALS"]
    creds = json.loads(raw) if isinstance(raw, str) else dict(raw)
    creds = _normalize_private_key(creds)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    gc = gspread.authorize(Credentials.from_service_account_info(creds, scopes=scopes))
    return gc

def _open_spreadsheet():
    gc = _sheets_client()
    if "SHEET_URL" in st.secrets:
        return gc.open_by_url(st.secrets["SHEET_URL"].strip())
    return gc.open_by_key(st.secrets["SHEET_ID"].strip())

def _read_sheet_df(title: str) -> pd.DataFrame:
    sh = _open_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=2000, cols=200)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header, rows = values[0], values[1:]
    return pd.DataFrame(rows, columns=header).replace("", np.nan)

def _write_sheet_df(title: str, df: pd.DataFrame):
    sh = _open_spreadsheet()
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=2000, cols=200)
    out = df.copy()
    out = out.fillna("")
    ws.clear()
    if out.shape[0] == 0:
        ws.update([list(out.columns)])
    else:
        ws.update([list(out.columns)] + out.astype(str).values.tolist())

# ---------- (B) Yahoo – Revenue FY-estimat ----------
def _f(x):
    try:
        if x is None: return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            return float(s) if s != "" else None
        return float(x)
    except Exception:
        return None

def _avg_from_cell(val) -> Optional[float]:
    # Yahoo trend kolumnvärden är ofta dicts: {"avg": ..., "low": ..., "high": ...}
    if isinstance(val, dict):
        for k in ("avg", "average", "mean"):
            if k in val and _f(val[k]) is not None:
                return _f(val[k])
    return _f(val)

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_revenue_estimates(ticker: str) -> Dict[str, Any]:
    """
    Plockar Revenue 'currentYear' och 'nextYear' från Yahoo earnings_trend.
    Returnerar: {"fy1": float|None, "fy2": float|None,
                 "fy1_label": "currentYear", "fy2_label": "nextYear",
                 "source": "yahoo_trend"}
    """
    try:
        tk = yf.Ticker(ticker)
        try:
            trend = tk.get_earnings_trend()
        except Exception:
            trend = getattr(tk, "earnings_trend", None)

        if trend is None or (hasattr(trend, "empty") and trend.empty):
            return {"fy1": None, "fy2": None, "fy1_label": None, "fy2_label": None, "source": "none"}

        df = trend.copy()
        cols_lower = {c: str(c).lower() for c in df.columns}
        df.columns = [cols_lower.get(c, str(c).lower()) for c in df.columns]

        def pick_row(period_names):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for p in period_names:
                a = m.str.contains(rf"^{p}$")
                mask = a if mask is None else (mask | a)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_cy = pick_row(["currentyear", "current fiscal year", "currentfiscalyear"])
        row_ny = pick_row(["nextyear", "next fiscal year", "nextfiscalyear"])

        def revenue_avg(row):
            if row is None:
                return None
            for c in ("revenueestimate", "revenue", "revenueest"):
                if c in df.columns:
                    v = row.get(c)
                    a = _avg_from_cell(v)
                    if a is not None:
                        return a
            return None

        fy1 = revenue_avg(row_cy)
        fy2 = revenue_avg(row_ny)
        return {"fy1": fy1, "fy2": fy2, "fy1_label": "currentYear", "fy2_label": "nextYear", "source": "yahoo_trend"}
    except Exception:
        return {"fy1": None, "fy2": None, "fy1_label": None, "fy2_label": None, "source": "none"}

# ---------- (C) Patch: komplettera compute_methods_for_row med FY-rev ----------
# Spara originalet
__compute_methods_for_row_orig = compute_methods_for_row

def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]):
    """
    Wrapper som:
      1) Kör originalmotorn
      2) Hämtar Yahoo Revenue FY1/FY2-estimat
      3) Sätter meta["rev_estimates"] och uppdaterar meta["rev_path"] (y1/y2/y3)
         så att Beräkningsvägar speglar FY-estimaten (ttm lämnas oförändrat).
    """
    methods_df, sanity, meta = __compute_methods_for_row_orig(row, settings, fx_map)

    tkr = str(row.get("Ticker","")).strip().upper()
    re = fetch_yahoo_revenue_estimates(tkr)

    # Lägg till tydlig sektion i meta
    meta.setdefault("rev_estimates", {"fy1": None, "fy2": None, "source": "none"})
    if re.get("fy1") or re.get("fy2"):
        meta["rev_estimates"] = {"fy1": re.get("fy1"), "fy2": re.get("fy2"),
                                 "fy1_label": re.get("fy1_label"), "fy2_label": re.get("fy2_label"),
                                 "source": re.get("source")}

        # Uppdatera REV-path att följa FY-estimaten (ger bättre överensstämmelse med Yahoo Analysis)
        rp = meta.get("rev_path") or {}
        r0 = _f(rp.get("ttm"))
        g_used = None
        try:
            g_used = float(meta.get("cagr_clamped", {}).get("rev_cagr_used"))
        except Exception:
            g_used = None

        if r0 is not None:
            r1 = _f(re.get("fy1")) if re.get("fy1") is not None else (r0 * (1.0 + (g_used or 0.0)))
            r2 = _f(re.get("fy2")) if re.get("fy2") is not None else (r1 * (1.0 + (g_used or 0.0)))
            r3 = (r2 * (1.0 + (g_used or 0.0))) if (r2 is not None) else None
            meta["rev_path"] = {"ttm": r0, "y1": r1, "y2": r2, "y3": r3}

    return methods_df, sanity, meta

# ---------- (D) Sidebar – Sheets & FY-estimat till DATA ----------
def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _to_num(x):
    try:
        if x is None: return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            return float(s) if s else None
        return float(x)
    except Exception:
        return None

def _load_DATA_from_sheets():
    df = _read_sheet_df("Data")
    st.session_state["DATA"] = df
    st.success(f"Läst {len(df)} rader från fliken 'Data'.")

def _save_DATA_to_sheets():
    df = st.session_state.get("DATA")
    if not isinstance(df, pd.DataFrame):
        st.warning("Ingen DATA i sessionen.")
        return
    _write_sheet_df("Data", df)
    st.success("DATA sparad till fliken 'Data'.")

def _update_eps_rev_for_ticker_and_save(ticker: str):
    if not ticker:
        st.warning("Välj en ticker först.")
        return
    df = st.session_state.get("DATA")
    if not isinstance(df, pd.DataFrame) or df.empty:
        st.warning("DATA saknas eller är tom.")
        return

    # Hämta estimat
    from math import isnan
    eps = fetch_yahoo_eps_estimates(ticker)   # Del 2/4
    rev = fetch_yahoo_revenue_estimates(ticker)

    # Säkerställ kolumner
    df = _ensure_cols(df, ["Ticker", "EPS 1Y", "EPS 2Y", "Rev FY1", "Rev FY2"])

    mask = df["Ticker"].astype(str).str.upper() == ticker.upper()
    if not mask.any():
        st.warning(f"{ticker}: hittades inte i DATA.")
        return

    # Uppdatera
    if eps.get("eps_1y") is not None: df.loc[mask, "EPS 1Y"] = float(eps["eps_1y"])
    if eps.get("eps_2y") is not None: df.loc[mask, "EPS 2Y"] = float(eps["eps_2y"])
    if rev.get("fy1") is not None:    df.loc[mask, "Rev FY1"] = float(rev["fy1"])
    if rev.get("fy2") is not None:    df.loc[mask, "Rev FY2"] = float(rev["fy2"])

    # Skriv tillbaka både lokalt och till Sheets
    st.session_state["DATA"] = df
    try:
        _write_sheet_df("Data", df)
        st.success(f"{ticker}: EPS/Revenue-estimat uppdaterade och sparade.")
    except Exception as e:
        st.warning(f"Sparning till Sheets misslyckades: {e}")

# ---- Sidebar UI ----
with st.sidebar.expander("💾 Google Sheets & FY-estimat", expanded=True):
    if _has_sheets_cfg():
        c1, c2 = st.columns(2)
        if c1.button("⬇️ Ladda DATA"):
            _load_DATA_from_sheets()
        if c2.button("⬆️ Spara DATA"):
            _save_DATA_to_sheets()
    else:
        st.caption("Tips: Lägg in `GOOGLE_CREDENTIALS` + `SHEET_URL/SHEET_ID` i `st.secrets` för att aktivera Sheets.")

    # Uppdatera EPS/Revenue FY för den ticker du valt i Analys-fliken
    curr_tkr = st.session_state.get("ANALYS_TICKER", "")
    st.text_input("Ticker (från Analys)", value=curr_tkr, key="__fy_upd_tkr__")
    if st.button("🔄 Uppdatera EPS & Revenue (FY1/FY2) från Yahoo → Spara"):
        _update_eps_rev_for_ticker_and_save(st.session_state.get("__fy_upd_tkr__", "").strip().upper())

# ============================================================
# Del 4/4 — SLUT
# ============================================================
