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
    """
    Robust floatparser:
    • Tar bort mellanslag
    • Byter komma→punkt
    • Stöd för parentes-negativt: (123,45) -> -123.45
    • Returnerar None för NaN/inf/empty
    """
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip()
            if s == "":
                return None
            neg = False
            if s.startswith("(") and s.endswith(")"):
                neg = True
                s = s[1:-1]
            s = s.replace(" ", "").replace(",", ".")
            v = float(s)
            if neg:
                v = -v
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
#  • Estimat (EPS 1–2 år) från Yahoo trend (+ Finnhub fallback)
#  • CAGR-härledningar, multipel-decay, metodpriser
# ============================================================

import requests
import pandas as pd
import numpy as np

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
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
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
    out: Dict[str, Any] = {"sources": {}}

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
# Yahoo – EPS-estimat & långsiktig tillväxt (earnings trend)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
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

        def _avg_from_cell(val) -> Optional[float]:
            if isinstance(val, dict):
                for k in ("avg", "average", "mean"):
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
# Yahoo – Revenue CAGR från årsintäkter
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
def _get_finnhub_key() -> Optional[str]:
    return (_env_or_secret("FINNHUB_API_KEY")
            or _env_or_secret("FINNHUB_TOKEN")
            or os.environ.get("FINNHUB_API_KEY")
            or os.environ.get("FINNHUB_TOKEN"))

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
    if val is None:
        return None
    try:
        v = float(val)
        if not math.isfinite(v):
            return None
        return max(lo, min(hi, v))
    except Exception:
        return None

REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.40   # +40 %

def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    m0 = _pos(mult0)
    if m0 is None:
        return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_frac
    return max(m, floor)

def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float) -> Optional[float]:
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
    e = _f(ebitda)  # får vara negativ/0
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
# EPS/REV/EBITDA paths + härledning (EPS y2/y3 aldrig NULL)
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
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    ticker = str(row.get("Ticker", "")).strip()

    # 1) Live-data
    snap   = fetch_yahoo_snapshot(ticker)
    time.sleep(0.15)  # mild throttling
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.06)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)
    fh = fetch_finnhub_estimates(ticker)  # fallback

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
        f"eps_ttm={'ok' if e0 or e0==0 else '—'}({src.get('eps_ttm','?') or ('derived' if src_eps_ttm.startswith('derived') else src_eps_ttm)}), "
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

# ============================================================
# Del 2/4 slut — fortsätt i Del 3/4 (Analys/Portfölj/Ranking UI)
# ============================================================

# ============================================================
# app.py — Del 3/4
# UI: Analys / Portfölj / Ranking
#  • Välj ticker + bläddra (1/X)
#  • Kör beräkningar (compute_methods_for_row) och visa metodpriser
#  • Visa uppsida vs aktuell kurs (Idag, 1–3 år)
#  • Enkel portföljsammanställning (värde, andelar)
#  • Ranking – körs on-demand (knapp) för alla tickers
# ============================================================

import streamlit as st
import pandas as pd
import numpy as np
import time

# -------------------------------------------
# Hjälpare: hämta DataFrame ur session state
# -------------------------------------------
def _get_df_from_state() -> pd.DataFrame | None:
    for k in ("DATA", "df", "data", "df_data"):
        if k in st.session_state and isinstance(st.session_state[k], pd.DataFrame):
            return st.session_state[k]
    return None

def _put_df_to_state(df: pd.DataFrame):
    st.session_state["DATA"] = df

# -------------------------------------------
# Hjälpare: settings & fx_map ur state
# (definieras i Del 1; här säkra fallbacks)
# -------------------------------------------
def _get_settings() -> dict:
    return st.session_state.get("SETTINGS", {
        "pe_anchor_weight_ttm": 0.50,
        "multiple_decay": 0.10,
    })

def _get_fx_map() -> dict:
    # t.ex. {"USD": 10.5, "NOK": 1.02, ...} om ni vill använda
    return st.session_state.get("FX_MAP", {})

# -------------------------------------------
# Hjälpare: formattering
# -------------------------------------------
def _fmt_price(x):
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
            return "—"
        return f"{float(x):,.2f}"
    except Exception:
        return "—"

def _med_non_null(values: list[float | None]) -> float | None:
    cleaned = [float(v) for v in values if v is not None and np.isfinite(v)]
    if not cleaned:
        return None
    return float(np.median(cleaned))

# -------------------------------------------
# UI: Analys (en ticker i taget)
# -------------------------------------------
def page_analys():
    st.subheader("🔎 Analys (1/X)")
    df = _get_df_from_state()
    if df is None or df.empty:
        st.info("Ingen data laddad ännu. Lägg in en DataFrame i `st.session_state['DATA']` i Del 1.")
        return

    # Säkerställ grundkolumner
    if "Ticker" not in df.columns:
        st.warning("Kolumnen ‘Ticker’ saknas i data.")
        return

    tickers = [str(t) for t in df["Ticker"].fillna("").astype(str).tolist() if str(t).strip()]
    if not tickers:
        st.warning("Inga tickers i tabellen.")
        return

    # Bläddringsindex i session
    if "IDX_ANALYS" not in st.session_state:
        st.session_state["IDX_ANALYS"] = 0

    # Topprad: välj ticker eller bläddra
    cols = st.columns([3, 1, 1, 1.2])
    with cols[0]:
        sel = st.selectbox("Välj ticker", options=tickers, index=st.session_state["IDX_ANALYS"])
        cur_idx = tickers.index(sel)
        st.session_state["IDX_ANALYS"] = cur_idx
    with cols[1]:
        if st.button("⬅️ Föregående", use_container_width=True):
            st.session_state["IDX_ANALYS"] = (cur_idx - 1) % len(tickers)
            st.experimental_rerun()
    with cols[2]:
        if st.button("Nästa ➡️", use_container_width=True):
            st.session_state["IDX_ANALYS"] = (cur_idx + 1) % len(tickers)
            st.experimental_rerun()
    with cols[3]:
        st.markdown(f"**Rad:** {cur_idx+1} / {len(tickers)}")

    # Plocka rad för vald ticker
    row = df.loc[df["Ticker"] == sel].iloc[0]

    # Kör motor (Del 2)
    settings = _get_settings()
    fx_map   = _get_fx_map()
    with st.spinner("Hämtar och beräknar…"):
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    # Visa grunddata
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pris", f"{_fmt_price(meta.get('price'))} {meta.get('currency','')}")
    k2.metric("Utestående aktier", _fmt_price(meta.get("shares_out")))
    k3.metric("Netto skuld", _fmt_price(meta.get("net_debt")))
    k4.metric("PE-ankare", _fmt_price(meta.get("pe_anchor")))

    # Metodtabell
    st.write("#### Metodpriser (per aktie)")
    st.dataframe(methods_df, use_container_width=True, height=260)

    # “Blend”/median av metoder (exkl. None)
    blend_today = _med_non_null(methods_df["Idag"].tolist())
    blend_1y    = _med_non_null(methods_df["1 år"].tolist())
    blend_2y    = _med_non_null(methods_df["2 år"].tolist())
    blend_3y    = _med_non_null(methods_df["3 år"].tolist())

    st.write("#### Sammanfattning (median över icke-null metoder)")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Riktkurs idag",  _fmt_price(blend_today))
    c2.metric("Riktkurs 1 år",  _fmt_price(blend_1y))
    c3.metric("Riktkurs 2 år",  _fmt_price(blend_2y))
    c4.metric("Riktkurs 3 år",  _fmt_price(blend_3y))

    # Uppsida vs aktuell kurs
    price = meta.get("price")
    def _uppside(target):
        try:
            if target is None or price in (None, 0) or not np.isfinite(float(price)):
                return None
            return (float(target) / float(price) - 1.0) * 100.0
        except Exception:
            return None

    up0 = _uppside(blend_today)
    up1 = _uppside(blend_1y)
    up2 = _uppside(blend_2y)
    up3 = _uppside(blend_3y)
    c5.metric("Uppsida 1 år (%)", f"{up1:.1f}%" if up1 is not None else "—")

    # Visa uppsidor i en liten tabell
    ups_tab = pd.DataFrame({
        "Horisont": ["Idag", "1 år", "2 år", "3 år"],
        "Riktkurs": [blend_today, blend_1y, blend_2y, blend_3y],
        "Uppsida %": [up0, up1, up2, up3],
    })
    st.dataframe(ups_tab, use_container_width=True, height=160)

    # Debug/sanity (kan döljas i expander)
    with st.expander("🔧 Datakälla & sanity"):
        st.caption(sanity)
        st.json(meta)

# -------------------------------------------
# UI: Portfölj (enkel sammanställning)
#  • Förväntar kolumner: 'Antal aktier', 'Aktuell kurs'
# -------------------------------------------
def page_portfolio():
    st.subheader("💼 Portfölj")
    df = _get_df_from_state()
    if df is None or df.empty:
        st.info("Ingen data laddad ännu.")
        return

    cols_needed = ["Ticker", "Antal aktier", "Aktuell kurs"]
    for c in cols_needed:
        if c not in df.columns:
            st.warning(f"Kolumnen ‘{c}’ saknas — portföljberäkningar kan bli felaktiga.")
    work = df.copy()

    # NaN → 0 på innehav & kurs
    work["Antal aktier"] = pd.to_numeric(work.get("Antal aktier", 0), errors="coerce").fillna(0.0)
    work["Aktuell kurs"] = pd.to_numeric(work.get("Aktuell kurs", 0), errors="coerce").fillna(0.0)
    work["Positionvärde"] = work["Antal aktier"] * work["Aktuell kurs"]

    tot_value = float(work["Positionvärde"].sum()) if not work.empty else 0.0
    st.metric("Totalt portföljvärde (i respektive aktievaluta)", _fmt_price(tot_value))

    # Andelar
    if tot_value > 0:
        work["Andel %"] = (work["Positionvärde"] / tot_value) * 100.0
    else:
        work["Andel %"] = 0.0

    st.write("#### Innehav")
    show_cols = [c for c in ["Ticker", "Antal aktier", "Aktuell kurs", "Positionvärde", "Andel %"] if c in work.columns]
    st.dataframe(work[show_cols].sort_values("Positionvärde", ascending=False), use_container_width=True, height=360)

# -------------------------------------------
# UI: Ranking (on-demand beräkning)
#  • Kör compute_methods_for_row för alla tickers
#  • Rankar på uppsida 1 år (median-blend) som standard
# -------------------------------------------
def page_ranking():
    st.subheader("🏁 Ranking (on-demand)")

    df = _get_df_from_state()
    if df is None or df.empty or "Ticker" not in df.columns:
        st.info("Ingen data att ranka. Lägg in data i `st.session_state['DATA']`.")
        return

    tickers = [str(t) for t in df["Ticker"].fillna("").astype(str).tolist() if str(t).strip()]
    if not tickers:
        st.info("Inga tickers att ranka.")
        return

    settings = _get_settings()
    fx_map   = _get_fx_map()

    run = st.button("⚙️ Beräkna ranking nu")
    if not run:
        st.caption("Klicka på knappen ovan för att hämta färska datapunkter och räkna ranking.")
        return

    rows = []
    prog = st.progress(0.0, text="Startar…")
    N = len(tickers)

    for i, t in enumerate(tickers, start=1):
        try:
            row = df.loc[df["Ticker"] == t].iloc[0]
        except Exception:
            continue

        try:
            methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
            blend_today = _med_non_null(methods_df["Idag"].tolist())
            blend_1y    = _med_non_null(methods_df["1 år"].tolist())
            price       = meta.get("price")

            up_today = None if blend_today is None or price in (None, 0) else (blend_today/price - 1.0)*100.0
            up_1y    = None if blend_1y    is None or price in (None, 0) else (blend_1y/price - 1.0)*100.0

            rows.append({
                "Ticker": t,
                "Pris": price,
                "Riktkurs (blend) idag": blend_today,
                "Riktkurs (blend) 1 år": blend_1y,
                "Uppsida idag %": up_today,
                "Uppsida 1 år %": up_1y,
            })
        except Exception:
            rows.append({
                "Ticker": t, "Pris": None,
                "Riktkurs (blend) idag": None, "Riktkurs (blend) 1 år": None,
                "Uppsida idag %": None, "Uppsida 1 år %": None,
            })

        prog.progress(i / N, text=f"Beräknar… {i}/{N}")
        time.sleep(0.02)  # mild throttling av UI

    prog.empty()

    if not rows:
        st.info("Inga resultat.")
        return

    rank_df = pd.DataFrame(rows)
    # Standard: sortera på uppsida 1 år
    rank_df = rank_df.sort_values("Uppsida 1 år %", ascending=False, na_position="last").reset_index(drop=True)

    st.write("#### Ranking (störst uppsida 1 år överst)")
    st.dataframe(rank_df, use_container_width=True, height=420)

# -------------------------------------------
# Router (anropas från main i Del 4)
# -------------------------------------------
def render_main_ui():
    st.title("Aktieanalys och investeringsförslag — UI")
    tabs = st.tabs(["Analys", "Portfölj", "Ranking"])

    with tabs[0]:
        page_analys()
    with tabs[1]:
        page_portfolio()
    with tabs[2]:
        page_ranking()

# ============================================================
# Del 3/4 slut — Del 4/4 innehåller main() och integration
# ============================================================

# ============================================================
# app.py — Del 4/4
# Inställningar, Editor, Batch & Main
#  • Editor: "Hämta & fyll från Yahoo" (inkl. EPS/REV-estimat)
#  • Inställningar: källskatt per valuta + modellparametrar + manuellt FX-refresh
#  • Batch: massuppdatera alla tickers + snapshots
#  • Main: init (FX + auto-refresh), sidval & robust felhantering
#  • OBS: Denna Del definierar en mer avancerad page_portfolio() som
#         ersätter en enklare version om den definierades i Del 3.
# ============================================================

import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import time
from typing import Any, Dict, Optional, Tuple, List

# ---------- Små hjälpare ----------
def _nan_if_zero(v):
    try:
        f = float(v)
        return np.nan if f == 0.0 else f
    except Exception:
        return v

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
    """Lätt auto-uppdatering av pris/valuta/antal aktier på uppstart om Settings->auto_refresh_on_start='1'."""
    s = get_settings_map()
    try:
        flag = str(s.get("auto_refresh_on_start","0")).strip()
    except Exception:
        flag = "0"
    if flag != "1":
        return
    df = read_data_df()
    if df.empty:
        return
    df2 = df.copy()
    for idx, r in df2.iterrows():
        tkr = str(r.get("Ticker","")).strip().upper()
        if not tkr:
            continue
        try:
            snap = fetch_yahoo_snapshot(tkr)
            if snap.get("price") is not None:       df2.at[idx, "Aktuell kurs"] = snap["price"]
            if snap.get("currency"):                df2.at[idx, "Valuta"] = snap["currency"]
            if snap.get("shares") is not None:      df2.at[idx, "Utestående aktier"] = snap["shares"]
            df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
            df2.at[idx, "Auto källa"] = "Yahoo"
            time.sleep(0.20)
        except Exception:
            continue
    write_data_df(df2)

# ============================================================
#                      SIDA: Editor
# ============================================================
def page_editor():
    st.header("📝 Lägg till / Uppdatera bolag")

    df = read_data_df()

    # Välj befintlig eller nytt
    tickers = ["— nytt —"] + (sorted(df["Ticker"].dropna().astype(str).unique().tolist()) if not df.empty else [])
    tkr_sel = st.selectbox("Välj ticker", tickers, index=0, key="editor_tkr_sel")
    is_new  = (tkr_sel == "— nytt —")

    # Session-state för prefill
    if "editor_prefill" not in st.session_state:
        st.session_state["editor_prefill"] = {}

    # Grund-init från Data
    init = {c: None for c in DATA_COLUMNS}
    if not is_new and not df.empty:
        row = df[df["Ticker"].astype(str) == tkr_sel].iloc[0].to_dict()
        for k in DATA_COLUMNS:
            init[k] = row.get(k, None)

    # Slå ihop med ev. prefill
    merged = dict(init)
    merged.update({k: v for k, v in st.session_state["editor_prefill"].items() if v is not None})

    st.caption("Använd **Hämta & fyll från Yahoo** för att auto-populera formuläret (inkl. EPS/REV-estimat). Spara sedan.")

    with st.form("edit_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        ticker  = c1.text_input("Ticker", value="" if is_new else tkr_sel).strip().upper()
        name    = c2.text_input("Bolagsnamn", value=str(_nz(merged.get("Bolagsnamn"), "")))
        sector  = c3.text_input("Sektor", value=str(_nz(merged.get("Sektor"), "")))

        bucket_choices = DEFAULT_BUCKETS
        bucket_idx = bucket_choices.index(_nz(merged.get("Bucket"), bucket_choices[0])) if _nz(merged.get("Bucket"), bucket_choices[0]) in bucket_choices else 0
        bucket  = st.selectbox("Bucket/Kategori", bucket_choices, index=bucket_idx)
        valuta  = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"], index=["USD","EUR","NOK","CAD","SEK"].index(str(_nz(merged.get("Valuta"), "USD")).upper()))

        d1, d2, d3, d4 = st.columns(4)
        antal   = d1.number_input("Antal aktier", step=1, value=int(_nz(_f(merged.get("Antal aktier")), 0)))
        gav_sek = d2.number_input("GAV (SEK)", step=0.01, value=float(_nz(_f(merged.get("GAV (SEK)")), 0.0)))
        kurs    = d3.number_input("Aktuell kurs", step=0.01, value=float(_nz(_f(merged.get("Aktuell kurs")), 0.0)))
        shares  = d4.number_input("Utestående aktier", step=1.0, value=float(_nz(_f(merged.get("Utestående aktier")), 0.0)))

        e1, e2, e3, e4 = st.columns(4)
        rev_ttm   = e1.number_input("Rev TTM", step=1000.0, value=float(_nz(_f(merged.get("Rev TTM")), 0.0)))
        ebitda_t  = e2.number_input("EBITDA TTM (kan vara negativ)", value=float(_nz(_f(merged.get("EBITDA TTM")), 0.0)))
        eps_ttm   = e3.number_input("EPS TTM (kan vara negativ)", value=float(_nz(_f(merged.get("EPS TTM")), 0.0)))
        net_debt  = e4.number_input("Net debt (kan vara negativ)", value=float(_nz(_f(merged.get("Net debt")), 0.0)))

        f1, f2, f3, f4 = st.columns(4)
        pe_ttm   = f1.number_input("PE TTM", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("PE TTM")), 0.0)))
        pe_fwd   = f2.number_input("PE FWD", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("PE FWD")), 0.0)))
        ev_rev   = f3.number_input("EV/Revenue", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("EV/Revenue")), 0.0)))
        ev_ebit  = f4.number_input("EV/EBITDA", step=0.01, value=float(_nz(_f(merged.get("EV/EBITDA")), 0.0)))

        g1, g2, g3, g4 = st.columns(4)
        pb      = g1.number_input("P/B", min_value=0.0, step=0.01, value=float(_nz(_f(merged.get("P/B")), 0.0)))
        bvps    = g2.number_input("BVPS", step=0.01, value=float(_nz(_f(merged.get("BVPS")), 0.0)))
        eps1y   = g3.number_input("EPS 1Y (estimat)", value=float(_nz(_f(merged.get("EPS 1Y")), 0.0)))
        eps2y   = g4.number_input("EPS 2Y (estimat)", value=float(_nz(_f(merged.get("EPS 2Y")), 0.0)))

        h1, h2, h3, h4 = st.columns(4)
        revcg   = h1.number_input("Rev CAGR", step=0.001, value=float(_nz(_f(merged.get("Rev CAGR")), 0.0)))
        dps     = h2.number_input("Årlig utdelning (DPS)", step=0.01, value=float(_nz(_f(merged.get("Årlig utdelning")), 0.0)))
        dpscg   = h3.number_input("Utdelning CAGR", step=0.001, value=float(_nz(_f(merged.get("Utdelning CAGR")), 0.0)))
        prim_choices = _PREFER_ORDER
        prim_default = str(_nz(merged.get("Primär metod"), prim_choices[0]))
        prim_idx = prim_choices.index(prim_default) if prim_default in prim_choices else 0
        prim    = h4.selectbox("Primär metod", prim_choices, index=prim_idx)

        # Manuell möjlighet att sätta frekvens/DPS nästa/datum
        j1, j2, j3 = st.columns(3)
        freq_in = j1.selectbox("Utdelningsfrekvens (M/Q/S/A)", ["", "M","Q","S","A"], index=["","M","Q","S","A"].index(str(_nz(merged.get("Utdelningsfrekvens"), "")).upper()))
        next_dps = j2.number_input("Nästa utdelning (per aktie)", step=0.0001, value=float(_nz(_f(merged.get("Nästa utdelning (per aktie)")), 0.0)))

        next_dt_default = merged.get("Nästa utdelningsdatum")
        if isinstance(next_dt_default, str):
            try:
                next_dt_default = pd.to_datetime(next_dt_default, errors="coerce").date()
            except Exception:
                next_dt_default = None
        if isinstance(next_dt_default, dt.date):
            next_dt = j3.date_input("Nästa utdelningsdatum", value=next_dt_default)
        else:
            next_dt = j3.date_input("Nästa utdelningsdatum")

        c_left, c_right = st.columns(2)
        fetch_btn = c_left.form_submit_button("🔎 Hämta & fyll från Yahoo")
        save_btn  = c_right.form_submit_button("💾 Spara till Data")

    # Hantera "Hämta & fyll"
    if fetch_btn:
        if not ticker:
            st.warning("Ange en ticker först.")
            st.stop()

        # 1) Snapshot (pris, valuta, TTM-nycklar, EV, shares, P/B, BVPS …)
        snap = fetch_yahoo_snapshot(ticker)

        # 2) EPS-estimat (current/next year) + long-term EPS CAGR (trend)
        yh_eps = fetch_yahoo_eps_estimates(ticker)

        # 3) Revenue CAGR från Yahoo financials
        revcg_yh = fetch_yahoo_rev_cagr(ticker)

        # 4) Namn/Sektor (fallback)
        comp_name, comp_sector = None, None
        try:
            tk = yf.Ticker(ticker)
            try:
                inf = tk.info or {}
            except Exception:
                inf = {}
            comp_name   = inf.get("longName") or inf.get("shortName") or None
            comp_sector = inf.get("sector") or inf.get("industry") or None
        except Exception:
            comp_name, comp_sector = None, None

        # 5) Prisfallback via historik
        px = snap.get("price")
        if not _pos(px):
            try:
                hist = yf.Ticker(ticker).history(period="5d")
                if not hist.empty:
                    px = float(hist["Close"].dropna().iloc[-1])
            except Exception:
                px = None

        # 6) Prefill
        st.session_state["editor_prefill"] = {
            "Ticker": ticker,
            "Bolagsnamn": comp_name or _nz(name, ""),
            "Sektor": comp_sector or _nz(sector, ""),
            "Valuta": snap.get("currency"),
            "Aktuell kurs": px,
            "Rev TTM": snap.get("revenue_ttm"),
            "EBITDA TTM": snap.get("ebitda_ttm"),
            "EPS TTM": snap.get("eps_ttm"),
            "PE TTM": snap.get("pe_ttm"),
            "PE FWD": snap.get("pe_fwd"),
            "EV/Revenue": snap.get("ev_to_sales"),
            "EV/EBITDA": snap.get("ev_to_ebitda"),
            "P/B": snap.get("p_to_book"),
            "BVPS": snap.get("bvps"),
            "Net debt": snap.get("net_debt"),
            "Utestående aktier": snap.get("shares"),
            # Estimat/tillväxt:
            "EPS 1Y": yh_eps.get("eps_1y"),
            "EPS 2Y": yh_eps.get("eps_2y"),
            "Rev CAGR": revcg_yh.get("rev_cagr"),
        }
        st.success("Fält förifyllda från Yahoo – granska och klicka **Spara**.")
        st.rerun()

    # Hantera "Spara"
    if save_btn:
        if not ticker:
            st.warning("Ticker saknas.")
            st.stop()

        def _n(v): return _nan_if_zero(v)

        new_row = {
            "Timestamp": now_stamp(),
            "Ticker": ticker,
            "Bolagsnamn": name,
            "Sektor": sector,
            "Bucket": bucket,
            "Valuta": valuta,
            "Antal aktier": antal,
            "GAV (SEK)": gav_sek,
            "Aktuell kurs": _n(kurs),
            "Utestående aktier": _n(shares),
            "Net debt": _n(net_debt),
            "Rev TTM": _n(rev_ttm),
            "EBITDA TTM": _n(ebitda_t),
            "EPS TTM": _n(eps_ttm),
            "PE TTM": _n(pe_ttm),
            "PE FWD": _n(pe_fwd),
            "EV/Revenue": _n(ev_rev),
            "EV/EBITDA": _n(ev_ebit),
            "P/B": _n(pb),
            "BVPS": _n(bvps),
            "EPS 1Y": _n(eps1y),
            "EPS 2Y": _n(eps2y),
            "Rev CAGR": _n(revcg),
            "EPS CAGR": np.nan,  # beräknas i analysen vid behov
            "Årlig utdelning": _n(dps),
            "Utdelning CAGR": _n(dpscg),
            "Utdelningsfrekvens": (freq_in if freq_in else np.nan),
            "Nästa utdelningsdatum": next_dt if isinstance(next_dt, dt.date) else np.nan,
            "Nästa utdelning (per aktie)": _n(next_dps),
            "Primär metod": prim,
            "Senast auto uppdaterad": "",
            "Auto källa": "Manuell",
        }

        df_new = read_data_df()
        for c in DATA_COLUMNS:
            if c not in df_new.columns:
                df_new[c] = np.nan

        mask = (df_new["Ticker"].astype(str).str.upper() == ticker)
        if mask.any():
            for k, v in new_row.items():
                if k in df_new.columns:
                    df_new.loc[mask, k] = v
        else:
            aligned = {c: new_row.get(c, np.nan) for c in df_new.columns}
            df_new = pd.concat([df_new, pd.DataFrame([aligned])], ignore_index=True)

        write_data_df(df_new)
        st.session_state["editor_prefill"] = {}
        st.success("Sparat till Data.")
        st.rerun()

# ============================================================
#                      SIDA: Inställningar
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
        def upsert(sdf, k, v):
            if (sdf["Key"] == k).any():
                sdf.loc[sdf["Key"] == k, "Value"] = str(v)
            else:
                sdf = pd.concat([sdf, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)
            return sdf
        for ccy, v in vals.items():
            s = upsert(s, f"withholding_{ccy}", v)
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    p1, p2 = st.columns(2)
    pe_w  = p1.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05, value=float(settings.get("pe_anchor_weight_ttm","0.5")))
    decay = p2.number_input("Multipel-decay per år (0..1)", min_value=0.0, max_value=1.0, step=0.01, value=float(settings.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty: s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def setv(sdf, k, v):
            if (sdf["Key"] == k).any():
                sdf.loc[sdf["Key"] == k, "Value"] = str(v)
            else:
                sdf.loc[len(sdf)] = [k, str(v)]
            return sdf
        s = setv(s, "pe_anchor_weight_ttm", pe_w)
        s = setv(s, "multiple_decay", decay)
        _write_df(SETTINGS_TITLE, s)
        st.success("Parametrar uppdaterade.")

    st.subheader("Valutakurser")
    if st.button("🔄 Hämta & uppdatera FX från Yahoo"):
        mp = _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")
        st.json(mp)

# ============================================================
#                        SIDA: Portfölj (avancerad)
#  • Ersätter ev. enklare variant definierad i Del 3
#  • P/L i SEK, uppsida, samt lista över kommande utbetalningar
# ============================================================
def today_date() -> dt.date:
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return dt.datetime.now(tz).date()
    except Exception:
        return dt.date.today()

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

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
    code = (currency or "USD").upper()
    key  = f"withholding_{code}"
    try:
        return float(settings.get(key, "0.15"))
    except Exception:
        return 0.15

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

# ============================================================
#                        SIDA: Batch
# ============================================================
def page_batch():
    st.header("🧰 Batch-uppdatering")
    df = read_data_df()
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
            if snap.get("shares") is not None:      df2.at[idx, "Utestående aktier"] = snap["shares"]
            df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
            df2.at[idx, "Auto källa"] = "Yahoo"
            time.sleep(throttle)
            prog.progress((i+1)/len(df2))
        write_data_df(df2)
        prog.empty()
        st.success("Uppdaterat alla tickers från Yahoo.")

    if st.button("📷 Spara snapshots (alla)"):
        settings = get_settings_map()
        fx_map   = get_fx_map()
        prog = st.progress(0.0)
        count = 0
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

    # Init: uppdatera FX + ev. auto-refresh på start (en gång per session)
    if "fx_inited" not in st.session_state:
        try:
            _load_fx_and_update_sheet()
        except Exception as e:
            st.warning(f"⚠️ FX kunde inte uppdateras just nu: {e}")
        st.session_state["fx_inited"] = True

    if "boot_refreshed" not in st.session_state:
        try:
            _startup_refresh()
        except Exception as e:
            st.warning(f"⚠️ Auto-refresh vid start misslyckades: {e}")
        st.session_state["boot_refreshed"] = True

    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        try:
            st.write("FX:", get_fx_map())
        except Exception as e:
            st.write("FX: (fel)", e)
        try:
            st.write("Settings:", get_settings_map())
        except Exception as e:
            st.write("Settings: (fel)", e)

    page = st.sidebar.radio("Sidor", ["Editor", "Portfölj", "Analys", "Ranking", "Inställningar", "Batch"], index=1)

    # Kör vald sida med robust felhantering så att Streamlit inte maskerar felmeddelandet
    try:
        if page == "Editor":
            page_editor()
        elif page == "Portfölj":
            page_portfolio()  # använder den avancerade versionen i denna Del
        elif page == "Analys":
            # Om Del 3 definierade page_analys(), kör den — annars visa info
            if "page_analys" in globals():
                page_analys()
            else:
                st.info("Analys-sidan är inte definierad i denna version.")
        elif page == "Ranking":
            if "page_ranking" in globals():
                page_ranking()
            else:
                st.info("Ranking-sidan är inte definierad i denna version.")
        elif page == "Inställningar":
            page_settings()
        elif page == "Batch":
            page_batch()
    except KeyError as e:
        st.error(f"💥 Nyckelfel (saknad kolumn eller fält): {e}. Kontrollera att bladet **Data** har alla kolumner enligt schema.")
    except Exception as e:
        st.error(f"💥 Ov\u00e4ntat fel i sidan '{page}': {e}")

def main():
    run_main_ui()

if __name__ == "__main__":
    main()
# ============================================================
# Del 4/4 — SLUT
# ============================================================
