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
#  • NEW: EPS- & Revenue-estimat från Yahoo Analysis
#  • Fallbacks + clamp
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

    # --- TTM via kvartal (income statement quarterly)
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

    # Normalisera valuta
    out["currency"] = str(out.get("currency") or "USD").upper()
    return out

# -------------------------
# Yahoo – EPS/REV-estimat från Analysis (earnings_trend)
# -------------------------

def _pick_period_row(df: pd.DataFrame, aliases: list[str]):
    if df is None or df.empty or "period" not in df.columns:
        return None
    m = df["period"].astype(str).str.strip().str.lower()
    mask = None
    for a in aliases:
        pat = a.strip().lower()
        hit = (m == pat) | m.str.contains(pat, na=False)
        mask = hit if mask is None else (mask | hit)
    sub = df[mask] if mask is not None else pd.DataFrame()
    return sub.iloc[0] if not sub.empty else None

def _avg_from_nested(cell):
    """
    Yahoo trend använder dict med 'avg'/'average'/'mean' (ibland 'raw'/'fmt').
    Returnera float om möjligt.
    """
    if isinstance(cell, dict):
        for k in ("avg","average","mean","raw","fmt","median"):
            if k in cell and _f(cell[k]) is not None:
                return _f(cell[k])
        # vissa versioner {'low':..,'high':..,'numberOfAnalysts':..}
        for k in cell.values():
            if _f(k) is not None:
                return _f(k)
    return _f(cell)

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> dict[str, float | None]:
    """
    CHANGED: Hämtar EPS currentYear & nextYear från Yahoo *Analysis* (earnings_trend).
    Returnerar: {"eps_1y": currentYear, "eps_2y": nextYear,
                 "eps_cagr_long": long-term growth, "source": "..."}
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
        df.columns = [str(c).strip().lower() for c in df.columns]

        r_curr = _pick_period_row(df, ["currentyear","current year","current fiscal year","currentfiscalyear","thisyear"])
        r_next = _pick_period_row(df, ["nextyear","next year","next fiscal year","nextfiscalyear"])

        eps_1y = None
        eps_2y = None
        if r_curr is not None:
            for col in ("earningsestimate","epsestimate","epstrend"):
                if col in df.columns:
                    eps_1y = _avg_from_nested(r_curr.get(col))
                    if eps_1y is not None:
                        break
        if r_next is not None:
            for col in ("earningsestimate","epsestimate","epstrend"):
                if col in df.columns:
                    eps_2y = _avg_from_nested(r_next.get(col))
                    if eps_2y is not None:
                        break

        # Long-term growth (5y)
        r_long = _pick_period_row(df, ["longterm","long term","next5years","next 5 years"])
        eps_cagr_long = None
        if r_long is not None:
            for col in ("growth","longtermgrowthrate"):
                if col in df.columns:
                    eps_cagr_long = _f(r_long.get(col))
                    if eps_cagr_long is not None:
                        break

        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": eps_cagr_long, "source": "yahoo_analysis"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

# === NEW: Revenue-estimat (Current/Next Year) från Analysis ===
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_rev_estimates(ticker: str) -> dict[str, float | None]:
    """
    Läser 'revenueEstimate' från earnings_trend (Analysis).
    Returnerar {"rev_1y": currentYear, "rev_2y": nextYear, "source": "..."} i REPORT CURRENCY.
    """
    try:
        tk = yf.Ticker(ticker)
        try:
            trend = tk.get_earnings_trend()
        except Exception:
            trend = getattr(tk, "earnings_trend", None)

        if trend is None or (hasattr(trend, "empty") and trend.empty):
            return {"rev_1y": None, "rev_2y": None, "source": "none"}

        df = trend.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]

        r_curr = _pick_period_row(df, ["currentyear","current year","current fiscal year","currentfiscalyear","thisyear"])
        r_next = _pick_period_row(df, ["nextyear","next year","next fiscal year","nextfiscalyear"])

        rev_1y = None
        rev_2y = None
        if r_curr is not None and "revenueestimate" in df.columns:
            rev_1y = _avg_from_nested(r_curr.get("revenueestimate"))
        if r_next is not None and "revenueestimate" in df.columns:
            rev_2y = _avg_from_nested(r_next.get("revenueestimate"))

        return {"rev_1y": rev_1y, "rev_2y": rev_2y, "source": "yahoo_analysis"}
    except Exception:
        return {"rev_1y": None, "rev_2y": None, "source": "none"}

# -------------------------
# Yahoo – Revenue CAGR från årsintäkter (historik)
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
# Finnhub (valfritt) – EPS-estimat fallback
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
# EPS/REV/EBITDA paths + härledning (EPS y2/y3 aldrig NULL)
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
        e2 = (e1 or e0) * (1.0 + g)
    e3 = (e2 or e1 or e0) * (1.0 + g)

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
    snap   = fetch_yahoo_snapshot(ticker)
    time.sleep(0.15)  # mild throttling

    # === CHANGED: hämta Analysis-estimat (EPS + Revenue) ===
    yh_eps   = fetch_yahoo_eps_estimates(ticker)     # EPS current & next year
    time.sleep(0.05)
    yh_revs  = fetch_yahoo_rev_estimates(ticker)     # Revenue current & next year
    time.sleep(0.05)

    # Historiska fallbacks
    revcg_yh = fetch_yahoo_rev_cagr(ticker)
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

    # Estimat / tillväxt (EPS via Analysis först, annars Finnhub/sheet)
    eps_1y_est = _pos(_nz(yh_eps.get("eps_1y"), _nz(fh.get("eps_1y"), row.get("EPS 1Y"))))
    eps_2y_est = _pos(_nz(yh_eps.get("eps_2y"), _nz(fh.get("eps_2y"), row.get("EPS 2Y"))))

    # Rev-estimat (Analysis)
    rev_1y_est = _pos(yh_revs.get("rev_1y"))
    rev_2y_est = _pos(yh_revs.get("rev_2y"))

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

    # Rev CAGR historik (fallback)
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

    # 5) Paths (init)
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr, rev_cagr)
    r0, r1, r2, r3 = _rev_path(_f(rev_ttm), rev_cagr)

    # === CHANGED: Override paths med Analysis-estimat när de finns ===
    # EPS: om Analysis gav värden, använd dem och extrapolera år 3 på samma tillväxttakt
    if _pos(eps_1y_est):
        e1 = float(eps_1y_est)
    if _pos(eps_2y_est):
        e2 = float(eps_2y_est)
    # extrapolera e3 på tillväxt mellan e2 och e1 (eller e1/e0)
    if _pos(e2) and _pos(e1):
        g_eps = (float(e2)/float(e1) - 1.0)
        e3 = float(e2) * (1.0 + g_eps)
    elif _pos(e1) and _pos(e0):
        g_eps = (float(e1)/float(e0) - 1.0)
        e2 = float(e1) * (1.0 + g_eps)
        e3 = float(e2) * (1.0 + g_eps)

    # Revenue: använd Analysis current/next year om tillgängligt
    if _pos(rev_1y_est):
        r1 = float(rev_1y_est)
    if _pos(rev_2y_est):
        r2 = float(rev_2y_est)
    # extrapolera r3 från r2->r1 eller r1->r0
    if _pos(r2) and _pos(r1):
        g_rev = (float(r2)/float(r1) - 1.0)
        r3 = float(r2) * (1.0 + g_rev)
    elif _pos(r1) and _pos(r0):
        g_rev = (float(r1)/float(r0) - 1.0)
        r2 = float(r1) * (1.0 + g_rev)
        r3 = float(r2) * (1.0 + g_rev)

    # EBITDA skalar mot Revenue-path
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
    eps1_src = "yahoo_analysis" if _pos(yh_eps.get("eps_1y")) else ("finnhub" if _pos(fh.get("eps_1y")) else ("sheet" if _pos(row.get("EPS 1Y")) else "filled_by_rule"))
    eps2_src = "yahoo_analysis" if _pos(yh_eps.get("eps_2y")) else ("finnhub" if _pos(fh.get("eps_2y")) else ("sheet/derived" if _pos(row.get("EPS 2Y")) else "filled_by_rule"))
    rev_src  = "yahoo_analysis" if (_pos(rev_1y_est) or _pos(rev_2y_est)) else ("yahoo_financials" if revcg_yh.get("rev_cagr") is not None else "none")

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if e0 or e0==0 else '—'}({src.get('eps_ttm','?') or ('derived' if (isinstance(src_eps_ttm, str) and src_eps_ttm.startswith('derived')) else src_eps_ttm)}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_est={'ok' if (_pos(rev_1y_est) or _pos(rev_2y_est)) else '—'}({rev_src}), "
        f"rev_cagr_fallback={'ok' if _f(rev_cagr) is not None else '—'}(clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
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
        "sources": {
            **src,
            "eps_1y_source": eps1_src,
            "eps_2y_source": eps2_src,
            "rev_est_source": rev_src,
            "rev_cagr_source": "yahoo_financials" if revcg_yh.get("rev_cagr") is not None else "none",
        },
        "cagr_clamped": {
            "rev_cagr_used": _f((float(r1)/float(r0)-1.0) if (_pos(r1) and _pos(r0)) else rev_cagr),
            "eps_cagr_used": _f((float(e1)/float(e0)-1.0) if (_pos(e1) and _pos(e0)) else eps_cagr),
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# ============================================================
# app.py — Del 3/4
# UI: Analysvy & Resultatpanel (använder compute_methods_for_row)
# ============================================================

import pandas as pd
import streamlit as st

# ------------------------------------------------------------
# Hjälp: hämta DATA/FX/SETTINGS ur session_state
# ------------------------------------------------------------
def _get_data_fx_settings():
    data = st.session_state.get("DATA")
    fx_map = st.session_state.get("FX", {}) or {}
    settings = st.session_state.get("SETTINGS", {}) or {}
    return data, fx_map, settings

# ------------------------------------------------------------
# UI-komponent: välj ticker från DATA
# ------------------------------------------------------------
def _select_row_from_data(df: pd.DataFrame):
    tickers = []
    try:
        tickers = [str(t).strip() for t in df["Ticker"].dropna().astype(str).unique().tolist()]
    except Exception:
        pass
    if not tickers:
        st.warning("Ingen kolumn 'Ticker' eller inga tickers funna i DATA.")
        return None
    sel = st.selectbox("Välj bolag (Ticker)", sorted(tickers))
    try:
        row = df.loc[df["Ticker"].astype(str).str.strip() == sel].iloc[0]
        return row
    except Exception:
        st.error("Kunde inte läsa vald rad.")
        return None

# ------------------------------------------------------------
# UI: visa resultattabeller + meta
# ------------------------------------------------------------
def _render_methods_and_meta(methods_df: pd.DataFrame, sanity: str, meta: dict):
    st.subheader("Resultat per metod")
    st.dataframe(methods_df, use_container_width=True)

    st.caption(sanity)

    st.subheader("Beräkningsvägar")
    st.json({
        "EPS-path": meta.get("eps_path"),
        "REV-path": meta.get("rev_path"),
        "EBITDA-path": meta.get("ebitda_path"),
    })

    st.subheader("Källor & Meta")
    st.json({
        "currency": meta.get("currency"),
        "price": meta.get("price"),
        "shares_out": meta.get("shares_out"),
        "net_debt": meta.get("net_debt"),
        "pe_anchor": meta.get("pe_anchor"),
        "decay": meta.get("decay"),
        "company_name": meta.get("company_name"),
        "sector": meta.get("sector"),
        "industry": meta.get("industry"),
        "sources": meta.get("sources"),
        "cagr_clamped": meta.get("cagr_clamped"),
    })

# ------------------------------------------------------------
# Analysvy (huvudfunktion för sidan)
# ------------------------------------------------------------
def render_analysis_view():
    data, fx_map, settings = _get_data_fx_settings()

    if data is None or getattr(data, "empty", True):
        st.info("Ingen data laddad ännu. Lägg in en DataFrame i `st.session_state['DATA']` i Del 1.")
        return

    st.header("Analys")

    row = _select_row_from_data(data)
    if row is None:
        return

    with st.spinner("Beräknar..."):
        # Den här funktionen definieras i Del 2 och innehåller förbättrad Yahoo-hämtning
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    _render_methods_and_meta(methods_df, sanity, meta)

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
