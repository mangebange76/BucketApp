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
# app.py — Del 2/4 — Datainsamling & beräkningsmotor
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor som förstahandsval
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Multipel-decay, P/E-ankare, pris-/EV-byggare
#  • compute_methods_for_row: returnerar metodtabell + meta
# ============================================================

import time
import pandas as pd
import numpy as np
import streamlit as st
import yfinance as yf

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
            return {"rev_cagr": None, "years": None, "source": "yahoo_financials"}

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
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

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
# EPS/REV-paths + manuell Rev1Y/Rev2Y & härledning
# -------------------------

# CHANGED: Ny robust parser för Rev-inmatning (miljoner ELLER hela enheter)
def _rev_input_to_units(v: float | None) -> float | None:
    """
    Tolkar Rev 1Y/2Y från Editor:
      - Om värdet är < 1e6 antar vi 'miljoner' och multiplicerar med 1e6.
      - Om värdet är >= 1e6 antar vi att det redan är i valutaenheter och låter det vara.
    Exempel:
      8810   -> 8.81e9
      223044300000 -> 223044300000 (oförändrat)
    """
    x = _f(v)
    if x is None:
        return None
    try:
        return float(x) if x >= 1_000_000 else float(x) * 1_000_000.0
    except Exception:
        return None

def _derive_eps_ttm_from_pe_only(price: float | None, pe_ttm: float | None,
                                 eps_ttm: float | None) -> tuple[float | None, str]:
    src_ttm = "source" if eps_ttm is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe_ttm"
    return eps_ttm, src_ttm

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
      1) Manuell Rev 1Y/2Y (Editor) — tolkad via _rev_input_to_units()
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

    # Estimat / tillväxt (MANUELLT FÅR FÖRETRÄDE)
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

    # 4) Revenue: prioritera manuella 1Y/2Y (CHANGED: smart tolkning av enheter)
    rev1_manual_units = _rev_input_to_units(_f(row.get("Rev 1Y")))
    rev2_manual_units = _rev_input_to_units(_f(row.get("Rev 2Y")))
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
# app.py — Del 3/4 — Editor & Portfölj
#  • Editor: Ticker, EPS 1Y/2Y, Rev 1Y/2Y, Rev CAGR, EPS CAGR, Antal, GAV (SEK)
#  • Portfölj: Marknadsvärde per innehav, anskaffning, P&L i SEK
#  • Valutaräknare via st.session_state["FX"] (SEK-bas)
# ============================================================

import time
import pandas as pd
import numpy as np
import streamlit as st

# ====== Interna hjälpare (från Del 1/4 finns: _f, _nz, _pos) ======

def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Säkerställ minimikolumner så editor/portfölj alltid fungerar."""
    needed = [
        "Ticker", "Bolagsnamn", "Valuta",
        "Antal aktier", "GAV (SEK)", "Aktuell kurs",
        "Utestående aktier",
        "EPS 1Y", "EPS 2Y", "EPS CAGR",
        "Rev 1Y", "Rev 2Y", "Rev CAGR",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = None
    # typer
    num_cols = ["Antal aktier", "GAV (SEK)", "Aktuell kurs", "Utestående aktier",
                "EPS 1Y", "EPS 2Y", "EPS CAGR", "Rev 1Y", "Rev 2Y", "Rev CAGR"]
    for c in num_cols:
        try:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        except Exception:
            pass
    # text
    for c in ["Ticker", "Bolagsnamn", "Valuta"]:
        try:
            df[c] = df[c].astype(str)
        except Exception:
            pass
    return df

def _fx_rate(curr: str) -> float | None:
    """Hämta FX→SEK från state (SEK=1.0 default)."""
    fx = st.session_state.get("FX", {}) or {}
    if not curr or str(curr).upper() == "SEK":
        return 1.0
    r = fx.get(str(curr).upper())
    return float(r) if r is not None else None

def _format_money(v):
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "—"

# ====== EDITOR ======

def render_editor_view():
    st.header("Editor")
    df = st.session_state.get("DATA")
    if df is None or (hasattr(df, "empty") and df.empty):
        st.info("Ingen data laddad ännu. Lägg in en DataFrame i `st.session_state['DATA']` i Del 1.")
        return

    df = _ensure_columns(df.copy())
    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    tick = st.selectbox("Välj bolag", tickers, key="ed_sel_ticker")

    row_idx = df.index[df["Ticker"].astype(str) == str(tick)]
    if len(row_idx) == 0:
        st.warning("Valt bolag hittades inte i tabellen.")
        return
    i = row_idx[0]
    r = df.loc[i]

    col1, col2 = st.columns(2)
    with col1:
        antal = st.number_input("Antal aktier", min_value=0, step=1,
                                value=int(_nz(r.get("Antal aktier"), 0)))
        gav_sek = st.number_input("GAV (SEK) per aktie",
                                  min_value=0.0, step=0.01,
                                  value=float(_nz(r.get("GAV (SEK)"), 0.0)))
        eps1 = st.text_input("EPS 1Y (estimat)", value=str(_nz(r.get("EPS 1Y"), "")))
        eps2 = st.text_input("EPS 2Y (estimat)", value=str(_nz(r.get("EPS 2Y"), "")))
    with col2:
        rev_cagr = st.text_input("Rev CAGR (decimal, t.ex. 0.15 = 15%)",
                                 value=str(_nz(r.get("Rev CAGR"), "")))
        # OBS: du kan skriva in MILJONER (8810) eller HELA belopp (223044300000)
        rev1 = st.text_input("Rev 1Y (miljoner ELLER hela belopp)",
                             value=str(_nz(r.get("Rev 1Y"), "")))
        rev2 = st.text_input("Rev 2Y (miljoner ELLER hela belopp)",
                             value=str(_nz(r.get("Rev 2Y"), "")))
        eps_cagr = st.text_input("EPS CAGR (decimal)",
                                 value=str(_nz(r.get("EPS CAGR"), "")))

    if st.button("Spara till data", use_container_width=True):
        # robust parsing (float eller tomt)
        def _p(x):
            x = _f(x)
            return None if x is None else float(x)

        df.at[i, "Antal aktier"] = int(_nz(antal, 0))
        df.at[i, "GAV (SEK)"] = _p(gav_sek)
        df.at[i, "EPS 1Y"] = _p(eps1)
        df.at[i, "EPS 2Y"] = _p(eps2)
        df.at[i, "Rev CAGR"] = _p(rev_cagr)
        df.at[i, "Rev 1Y"] = _p(rev1)
        df.at[i, "Rev 2Y"] = _p(rev2)
        df.at[i, "EPS CAGR"] = _p(eps_cagr)

        st.session_state["DATA"] = df
        st.success("Sparat ✅ Dina manuella värden har företräde i beräkningarna (Del 2/4).")

    with st.expander("Förhandsgranskning av rad", expanded=False):
        st.dataframe(df.loc[[i]])

# ====== PORTFÖLJ ======

def _row_price_and_currency(row):
    """Hämta pris/valuta från data eller Yahoo-snapshot om pris saknas."""
    price = _f(row.get("Aktuell kurs"))
    curr = (row.get("Valuta") or "USD")
    if price is None:
        try:
            snap = fetch_yahoo_snapshot(str(row.get("Ticker")))
            price = _f(_nz(snap.get("price"), None))
            curr = (snap.get("currency") or curr)
        except Exception:
            pass
    return price, (curr or "USD")

def render_portfolio_view():
    st.header("Portfölj")

    df = st.session_state.get("DATA")
    if df is None or (hasattr(df, "empty") and df.empty):
        st.info("Ingen data i portföljen ännu.")
        return
    df = _ensure_columns(df.copy())

    # Visa endast innehav > 0
    pos = df.copy()
    pos["Antal aktier"] = pd.to_numeric(pos["Antal aktier"], errors="coerce").fillna(0).astype(int)
    pos = pos[pos["Antal aktier"] > 0]
    if pos.empty:
        st.info("Inga innehav ännu. Lägg in antal i Editor.")
        return

    rows = []
    for _, r in pos.iterrows():
        ticker = str(r.get("Ticker"))
        name = r.get("Bolagsnamn")
        price, curr = _row_price_and_currency(r)
        shares = int(_nz(r.get("Antal aktier"), 0))
        gav = float(_nz(r.get("GAV (SEK)"), 0.0))

        mv_ccy = None if (price is None) else float(price) * shares
        fx = _fx_rate(curr)
        mv_sek = None if (mv_ccy is None or fx is None) else mv_ccy * fx
        cost_sek = shares * gav if shares and gav is not None else None
        pnl_sek = None if (mv_sek is None or cost_sek is None) else (mv_sek - cost_sek)
        pnl_pct = None
        if cost_sek and cost_sek != 0 and pnl_sek is not None:
            pnl_pct = pnl_sek / cost_sek

        rows.append({
            "Ticker": ticker,
            "Bolagsnamn": name,
            "Valuta": str(curr).upper(),
            "Antal": shares,
            "Kurs (valuta)": price,
            "Marknadsvärde (valuta)": mv_ccy,
            "FX→SEK": fx,
            "Marknadsvärde (SEK)": mv_sek,
            "GAV (SEK)": gav,
            "Anskaffning (SEK)": cost_sek,
            "P&L (SEK)": pnl_sek,
            "P&L (%)": None if pnl_pct is None else float(pnl_pct) * 100.0,
        })

    port = pd.DataFrame(rows)
    st.dataframe(port[[
        "Ticker", "Bolagsnamn", "Valuta", "Antal",
        "Kurs (valuta)", "Marknadsvärde (valuta)",
        "FX→SEK", "Marknadsvärde (SEK)",
        "GAV (SEK)", "Anskaffning (SEK)",
        "P&L (SEK)", "P&L (%)"
    ]])

    # Summering i SEK
    tot_mv_sek = pd.to_numeric(port["Marknadsvärde (SEK)"], errors="coerce").sum()
    tot_cost = pd.to_numeric(port["Anskaffning (SEK)"], errors="coerce").sum()
    tot_pnl = tot_mv_sek - tot_cost
    tot_pnl_pct = (tot_pnl / tot_cost) * 100.0 if tot_cost else None

    st.subheader("Summering (SEK)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Summa marknadsvärde", _format_money(tot_mv_sek))
    c2.metric("Summa anskaffning", _format_money(tot_cost))
    c3.metric("P&L (SEK)", _format_money(tot_pnl))
    c4.metric("P&L (%)", "-" if tot_pnl_pct is None else f"{tot_pnl_pct:,.2f}%")

# ============================================================
# app.py — Del 4/4 — Analys + Main (med Watchlist/Bucket-filter)
# ============================================================

import pandas as pd
import numpy as np
import streamlit as st

# ===== Hjälpare =====

def _fmt(x, nd=2):
    try:
        v = float(x)
        if not np.isfinite(v):
            return "—"
        return f"{v:.{nd}f}"
    except Exception:
        return "—"

def _fmt_pct(x, nd=2):
    try:
        v = float(x)
        if not np.isfinite(v):
            return "—"
        return f"{v*100:.{nd}f}%"
    except Exception:
        return "—"

def _ensure_min_cols(df: pd.DataFrame) -> pd.DataFrame:
    need = ["Ticker","Valuta","Aktuell kurs","Antal aktier","Bucket",
            "EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y","Rev CAGR","EPS CAGR"]
    for c in need:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _looks_full_units(x: float | int | str | None) -> bool:
    """Heuristik: om Rev 1Y/2Y ser ut att vara hela beloppet (>= 1e8) i stället för miljoner."""
    try:
        v = float(str(x).replace(" ", "").replace(",", "."))
        return v >= 1e8
    except Exception:
        return False

def _prepare_row_for_compute(row: pd.Series) -> pd.Series:
    """
    Gör en kopia av raden där manuella Rev 1Y/2Y normaliseras till 'miljoner'
    om användaren råkat ange hela beloppet. (Del 2/4 multiplicerar sedan med 1e6.)
    """
    r = row.copy()
    for key in ("Rev 1Y", "Rev 2Y"):
        if key in r and pd.notna(r[key]) and _looks_full_units(r[key]):
            try:
                r[key] = float(r[key]) / 1_000_000.0
            except Exception:
                pass
    return r

# ===== Analys-vy med filter =====

def render_analysis_view():
    st.header("Analys")

    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad ännu. Läs in Data i sidomenyn eller lägg till bolag.")
        return

    df = _ensure_min_cols(df.copy())

    # --- Filterrad ---
    col_f1, col_f2, col_f3 = st.columns([1, 1.2, 1])
    with col_f1:
        only_watch = st.checkbox("Visa endast Watchlist (ej ägda)", value=False)
    with col_f2:
        buckets = sorted([b for b in df["Bucket"].dropna().astype(str).unique().tolist() if b.strip() != ""])
        pick_buckets = st.multiselect("Bucket-filter", options=buckets, default=buckets)
    with col_f3:
        method_pick = st.selectbox("Metod", ["ev_sales","pe_hist_vs_eps","ev_ebitda","p_b"], index=0)

    # Tillämpa filter
    filt = df.copy()
    if only_watch:
        a = pd.to_numeric(filt["Antal aktier"], errors="coerce").fillna(0).astype(float)
        filt = filt[a <= 0.0]
    if pick_buckets:
        filt = filt[filt["Bucket"].astype(str).isin(pick_buckets)]

    tickers = filt["Ticker"].dropna().astype(str).unique().tolist()
    if not tickers:
        st.info("Inga bolag matchar dina filter.")
        return

    selected = st.selectbox("Välj bolag", tickers, index=0, key="analysis_sel_ticker")

    row = df[df["Ticker"].astype(str) == selected].iloc[0]
    row_for_compute = _prepare_row_for_compute(row)

    # Settings/FX från session (från Del 1/4)
    settings = st.session_state.get("SETTINGS", {}) or {}
    fx_map   = st.session_state.get("FX", {}) or {}

    with st.spinner(f"Beräknar metoder för {selected}…"):
        methods_df, sanity, meta = compute_methods_for_row(row_for_compute, settings, fx_map)

    price    = meta.get("price")
    currency = str(meta.get("currency") or row.get("Valuta") or "USD").upper()

    # Lägg till uppsida-kolumner
    def _ups(x):
        try:
            px = float(price)
            tgt = float(x)
            if not np.isfinite(px) or px == 0 or not np.isfinite(tgt):
                return np.nan
            return (tgt / px) - 1.0
        except Exception:
            return np.nan

    for h in ["Idag","1 år","2 år","3 år"]:
        methods_df[f"Uppsida {h}"] = methods_df[h].apply(_ups)

    # Toppmetrik
    st.subheader(f"{selected} • {meta.get('company_name') or ''}")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Kurs", f"{_fmt(price)} {currency}")
    c2.metric("Valuta", currency)
    c3.metric("Utest. aktier", _fmt(meta.get("shares_out"), 0))
    c4.metric("Net debt", _fmt(meta.get("net_debt"), 0))

    # Tabell med metoder
    st.markdown("**Värderingsmetoder** (pris i aktiens valuta)")
    show_cols = ["Metod","Idag","1 år","2 år","3 år","Uppsida Idag","Uppsida 1 år","Uppsida 2 år","Uppsida 3 år"]
    st.dataframe(methods_df[show_cols], hide_index=True, use_container_width=True)

    # Lyft vald metod
    sel_row = methods_df[methods_df["Metod"] == method_pick].iloc[0] if (methods_df["Metod"] == method_pick).any() else None
    st.markdown("---")
    st.markdown(f"**Vald metod:** `{method_pick}`")
    if sel_row is not None:
        d1, d2, d3, d4, d5 = st.columns(5)
        d1.metric("Target idag",  f"{_fmt(sel_row['Idag'])} {currency}",  _fmt_pct(_ups(sel_row['Idag'])))
        d2.metric("Target 1 år",  f"{_fmt(sel_row['1 år'])} {currency}", _fmt_pct(_ups(sel_row['1 år'])))
        d3.metric("Target 2 år",  f"{_fmt(sel_row['2 år'])} {currency}", _fmt_pct(_ups(sel_row['2 år'])))
        d4.metric("Target 3 år",  f"{_fmt(sel_row['3 år'])} {currency}", _fmt_pct(_ups(sel_row['3 år'])))
        d5.metric("PE-ankare", _fmt(meta.get("pe_anchor")))
    else:
        st.info("Vald metod saknar värden för detta bolag.")

    with st.expander("Källor & sanity-check", expanded=False):
        st.code(sanity)
        st.json(meta.get("sources", {}))

    st.markdown("---")
    st.subheader("Databasen (ofilterad)")
    st.dataframe(df, use_container_width=True)

# ===== Main / routing =====

def _boot_session_if_needed():
    # Del 1/4 lägger normalt in DATA/SETTINGS/FX — här en defensiv fallback.
    if "DATA" not in st.session_state:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception:
            st.session_state["DATA"] = pd.DataFrame()
    if "SETTINGS" not in st.session_state:
        try:
            st.session_state["SETTINGS"] = get_settings_map()
        except Exception:
            st.session_state["SETTINGS"] = {}
    if "FX" not in st.session_state:
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception:
            st.session_state["FX"] = {"SEK":1.0,"USD":1.0,"EUR":1.0,"NOK":1.0,"CAD":1.0}

def main():
    _boot_session_if_needed()

    st.sidebar.title("Navigering")
    if st.sidebar.button("↻ Läs om från Google Sheets"):
        st.session_state["DATA"] = read_data_df()
        st.success("DATA omläst.")
        st.rerun()
    if st.sidebar.button("⬆️ Spara session → Google Sheets"):
        write_data_df(st.session_state["DATA"])
        st.success("DATA sparad.")

    page = st.sidebar.radio("Gå till:", ["Analys","Portfölj","Editor"], index=0)

    try:
        if page == "Analys":
            render_analysis_view()
        elif page == "Portfölj":
            render_portfolio_view()   # Del 3/4
        elif page == "Editor":
            render_editor_view()      # Del 3/4
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# Entrypoint
if __name__ == "__main__":
    main()
