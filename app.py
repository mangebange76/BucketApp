# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 1/6: Bas & infrastruktur
#
#  - Streamlit setup
#  - Hjälpfunktioner (_f, _pos, etc)
#  - Google Sheets in/ut
#  - Kolumnschema (inkl tidsstämplar)
#  - FX-hantering (LÄS separat från UPPDATERA)
#  - Settings-hantering (Settings-bladet)
#
# ============================================================

from __future__ import annotations

# ---------- Standardbibliotek ----------
import os, json, math, time
from typing import Any, Dict, List, Optional
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
    """Tidsstämpel i Stockholmstid om möjligt."""
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
    """Hämta värde från env eller st.secrets."""
    v = os.environ.get(key)
    if v:
        return v
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    """Fixar '\\n' i private_key som lagts in via secrets."""
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def guard(fn, label: str = ""):
    """Visa fel i UI men raisa vidare (så vi ser var det small)."""
    try:
        return fn()
    except Exception as e:
        st.error(f"💥 Fel {label}\n\n{e}")
        raise

def _with_backoff(callable_fn, *args, **kwargs):
    """Backoff för gspread vid 429/5xx."""
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
    """Robust float-parser. Returnerar float eller None."""
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
    """Positiv float (>0) eller None."""
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(x, fallback=None):
    """Null coalesce."""
    return x if (x is not None and x == x) else fallback

# =========================
# Google Sheets Auth
# =========================
@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    """Skapa gspread Client från GOOGLE_CREDENTIALS."""
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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppna spreadsheet via SHEET_URL/SHEET_ID (även GOOGLE_* alias)."""
    sheet_url = (_env_or_secret("SHEET_URL") or _env_or_secret("GOOGLE_SHEET_URL"))
    sheet_id  = (_env_or_secret("SHEET_ID")  or _env_or_secret("GOOGLE_SHEET_ID"))

    if sheet_url and sheet_url.strip():
        return _with_backoff(_gc.open_by_url, sheet_url.strip())
    if sheet_id and sheet_id.strip():
        return _with_backoff(_gc.open_by_key, sheet_id.strip())
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID (eller GOOGLE_SHEET_URL / GOOGLE_SHEET_ID) i secrets.")

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    """Hämta/eller skapa worksheet med rätt titel."""
    try:
        return _with_backoff(spread.worksheet, title)
    except WorksheetNotFound:
        return _with_backoff(spread.add_worksheet, title=title, rows=2000, cols=200)

# =========================
# I/O – läs/skriv/append
# =========================
def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Garanti: df innehåller alla kolumner i 'cols'."""
    if df.empty:
        return pd.DataFrame(columns=cols)
    changed = False
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
            changed = True
    if changed:
        df = df[[*(k for k in cols if k in df.columns),
                 *[c for c in df.columns if c not in cols]]]
    return df

@st.cache_data(ttl=120, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
    """Läs ett helt ark (worksheet) till DataFrame."""
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
    """Skriv HELA df till ett ark, rensar först."""
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
    """Appendar rader längst ned i arket."""
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
    "EPS 1Y","EPS 2Y",
    "Rev 1Y","Rev 2Y",
    "Rev CAGR","EPS CAGR",
    "Årlig utdelning","Utdelning CAGR",
    # Utdelningsinfo
    "Utdelningsfrekvens",
    "Nästa utdelningsdatum",
    "Nästa utdelning (per aktie)",
    # Riktkurser/ranking
    "Primär metod",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    # Tidsstämplar/fältspårning
    "TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y",
    "Senast auto uppdaterad","Auto källa",
    # Manuell tillsyn-stämpel
    "Senast manuellt uppdaterad",
    # Editor-hjälp
    "EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad","Rev 2Y uppdaterad"
]

SETTINGS_COLUMNS = ["Key","Value"]
FX_COLUMNS       = ["Valuta","SEK_per_1"]

def _ensure_sheet_schema():
    """Säkerställ att alla ark existerar och har rätt kolumner."""
    # --- Data ---
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
            df = df[[c for c in DATA_COLUMNS if c in df.columns] +
                    [c for c in df.columns if c not in DATA_COLUMNS]]
            _write_df(DATA_TITLE, df)

    # --- Settings ---
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

    # --- Valutakurser (läs: skapa baseline-rader, ingen extern hämtning här) ---
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

    # --- Snapshot ---
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        _write_df(SNAPSHOT_TITLE, pd.DataFrame(columns=[
            "Timestamp","Ticker","Valuta","Metod",
            "Idag","1 år","2 år","3 år",
            "Ankare PE","Decay"
        ]))

# Kör schema-guard direkt vid import
guard(_ensure_sheet_schema, label="(säkra ark/kolumner)")

# =========================
# FX – hämta via yfinance
# =========================
FX_PAIRS = {
    "USD": "USDSEK=X",
    "EUR": "EURSEK=X",
    "NOK": "NOKSEK=X",
    "CAD": "CADSEK=X",
    "SEK": None,
}

@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_fx_from_yahoo() -> Dict[str, float]:
    """Hämtar SEK per 1 valutaenhet via yfinance. (LÄS – ingen skrivning)"""
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

def refresh_fx_map() -> Dict[str, float]:
    """
    Hämta färska FX från Yahoo och **skriv** till Valutakurser-bladet.
    Anropas endast manuellt (knapp) eller vid auto-refresh on start.
    """
    # Läs nuvarande
    fx_df = _read_df(FX_TITLE)
    current = {"SEK":1.0}
    if not fx_df.empty:
        for _, r in fx_df.iterrows():
            try:
                current[str(r["Valuta"]).upper()] = float(r["SEK_per_1"])
            except Exception:
                pass

    # Hämta färskt
    fresh = _fetch_fx_from_yahoo()
    current.update({k:v for k,v in fresh.items() if v})

    # Skriv tillbaka i fix ordning
    rows = [(k, current.get(k, "")) for k in ["SEK","USD","EUR","NOK","CAD"]]
    _write_df(FX_TITLE, pd.DataFrame(rows, columns=FX_COLUMNS))
    return current

@st.cache_data(ttl=600, show_spinner=False)
def get_fx_map() -> Dict[str, float]:
    """
    LÄS-ENDA: returnerar valutakartan från bladet.
    **Inga skrivningar** här (för att undvika recursiva reruns).
    """
    fx_df = _read_df(FX_TITLE)
    mp = {"SEK":1.0, "USD":np.nan, "EUR":np.nan, "NOK":np.nan, "CAD":np.nan}
    if fx_df.empty:
        return mp
    try:
        for _, r in fx_df.iterrows():
            c = str(r.get("Valuta", "")).upper().strip()
            v = _f(r.get("SEK_per_1"))
            if c:
                mp[c] = v if v is not None else mp.get(c, np.nan)
    except Exception:
        pass
    return mp

# =========================
# Settings – läs/källskatt
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    """Läser Settings-bladet som dict {Key: Value}."""
    s = _read_df(SETTINGS_TITLE)
    out: Dict[str,str] = {}
    if not s.empty:
        for _, r in s.iterrows():
            k = str(r.get("Key"))
            v = "" if pd.isna(r.get("Value")) else str(r.get("Value"))
            out[k] = v
    return out

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
    """Källskatt (%) per valuta från Settings (fallback 0.15)."""
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
    """Läs Data-bladet + typkonvertera numeriska kolumner."""
    df = _read_df(DATA_TITLE)
    df = _ensure_columns(df, DATA_COLUMNS)

    if df.empty:
        return df

    num_cols = [
        "Antal aktier","GAV (SEK)","Aktuell kurs",
        "Utestående aktier","Net debt",
        "Rev TTM","EBITDA TTM","EPS TTM",
        "PE TTM","PE FWD",
        "EV/Revenue","EV/EBITDA","P/B","BVPS",
        "EPS 1Y","EPS 2Y",
        "Rev 1Y","Rev 2Y",
        "Rev CAGR","EPS CAGR",
        "Årlig utdelning","Utdelning CAGR",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        "Nästa utdelning (per aktie)"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "Nästa utdelningsdatum" in df.columns:
        df["Nästa utdelningsdatum"] = pd.to_datetime(
            df["Nästa utdelningsdatum"],
            errors="coerce"
        ).dt.date

    for tcol in ["TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y","Senast auto uppdaterad","Senast manuellt uppdaterad"]:
        if tcol in df.columns:
            df[tcol] = df[tcol].astype(str)

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
    """Skriv tillbaka Data-bladet (bevarar schemaordning)."""
    cols = [c for c in DATA_COLUMNS if c in df.columns] + \
           [c for c in df.columns if c not in DATA_COLUMNS]
    _write_df(DATA_TITLE, df[cols])

def append_result_row(row: Dict[str, Any]):
    """Loggrad till Resultat-bladet (skapar vid behov)."""
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

# ===== Fallback-listor (om inte satta senare) =====
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b",
        "ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST

# ============================================================
# app.py — Del 2/6 — Datainsamling & beräkningsmotor (1/2)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor som förstahandsval
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Multipel-decay, P/E-ankare, pris-/EV-byggare
#  • Auto-detekt för manuella Rev 1Y/2Y (miljoner vs enheter)
#  • Ingen valutakonvertering av EPS (manuella värden antas redan i bolagets valuta)
# ============================================================

import time
import math
import numpy as np
import pandas as pd
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
    Keys (urval):
      price, currency, market_cap, ev, shares,
      revenue_ttm, ebitda_ttm, eps_ttm,
      ev_to_sales, ev_to_ebitda, pe_ttm, pe_fwd,
      p_to_book, bvps, net_debt, company_name, sector, industry, annual_dividend, dividend_frequency, sources={}
    """
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

    # EV / net debt
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

    # Balance Sheet-fallbacks
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

    # TTM via kvartal (income statement quarterly)
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

            eps_row = _ix_pick(dfq, EPS_KEYS_Q)
            eps_ttm_q = _sum_eps_last4(eps_row) if eps_row is not None else None

            rev_row = _ix_pick(dfq, REV_KEYS_Q)
            rev_ttm_q = _sum_last4(rev_row) if rev_row is not None else None

            ebitda_row = _ix_pick(dfq, EBITDA_KEYS_Q)
            ebitda_ttm_q = _sum_last4(ebitda_row) if ebitda_row is not None else None

            if eps_ttm_q is None:
                net_row = _ix_pick(dfq, ["NetIncome", "Net Income", "NetIncomeApplicableToCommonShares", "NetIncomeCommonStockholders"])
                shd_row = _ix_pick(dfq, ["DilutedAverageShares", "Diluted Shares", "AverageDilutedSharesOutstanding", "WeightedAverageDilutedSharesOutstanding"])
                if net_row is not None and shd_row is not None:
                    ni_ttm = _sum_last4(net_row)
                    sh_ttm = _sum_last4(shd_row)
                    if _pos(ni_ttm) is not None and _pos(sh_ttm) is not None and sh_ttm != 0:
                        eps_ttm_q = float(ni_ttm) / float(sh_ttm)

            if _pos(eps_ttm_q) is not None:
                out["eps_ttm"] = float(eps_ttm_q)
                out["sources"]["eps_ttm"] = "yahoo_quarterly_TTM"

            if _pos(rev_ttm_q) is not None:
                out["revenue_ttm"] = float(rev_ttm_q)
                out["sources"]["revenue_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")

            if ebitda_ttm_q is not None:
                out["ebitda_ttm"] = float(ebitda_ttm_q)
                out["sources"]["ebitda_ttm"] = "yahoo_quarterly_TTM"
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

    fwd_div = _f(gi("dividendRate") or gi("forwardAnnualDividendRate"))
    trl_div = _f(gi("trailingAnnualDividendRate"))
    if out.get("annual_dividend") is None and (fwd_div is not None or trl_div is not None):
        out["annual_dividend"] = float(_nz(fwd_div, trl_div))
        out["sources"]["annual_dividend"] = "yahoo_info"

    # Försök infer frekvens från historik
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
# EPS/REV-paths + manuell Rev1Y/Rev2Y (auto-detekt enheter)
# -------------------------
def _derive_eps_ttm_from_pe_only(price: float | None, pe_ttm: float | None,
                                 eps_ttm: float | None) -> tuple[float | None, str]:
    src_ttm = "source" if eps_ttm is not None else ""
    if eps_ttm is None and _pos(price) and _pos(pe_ttm):
        eps_ttm = price / pe_ttm
        src_ttm = "derived_from_pe_ttm"
    return eps_ttm, src_ttm

def _rev_manual_to_units_autosense(v: float | None, rev_ttm_hint: float | None) -> float | None:
    """
    Tolkar manuellt värde som 'miljoner' ELLER 'redan i enheter'.
    Strategi:
      • Om rev_ttm_hint finns: välj det tolkade värde (x eller x*1e6) som ligger närmast rev_ttm_hint.
      • Annars: heuristik — om x >= 1e8: tolka som redan i enheter; om x <= 1e7: tolka som 'miljoner' → x*1e6.
    """
    x = _f(v)
    if x is None:
        return None
    if _pos(rev_ttm_hint):
        as_is = x
        as_mn = x * 1_000_000.0
        try:
            r1 = abs(math.log(max(1e-12, as_is / rev_ttm_hint)))
            r2 = abs(math.log(max(1e-12, as_mn / rev_ttm_hint)))
            return as_is if r1 <= r2 else as_mn
        except Exception:
            pass
    # Fallback-heuristik
    if x >= 1e8:
        return x
    if x <= 1e7:
        return x * 1_000_000.0
    # Ambiguöst → anta miljoner (säkrare för 8 810 → 8.81B)
    return x * 1_000_000.0

# (legacy-namn för bakåtkompatibilitet)
def _rev_million_to_units(v: float | None) -> float | None:
    return _rev_manual_to_units_autosense(v, None)

# ============================================================
# app.py — Del 3/6 — Beräkningsmotor (2/2) & utdelningar
#  • compute_methods_for_row: bygger riktkurser (idag/1/2/3 år, bull/bear 1 år)
#  • Stöd för metoder: EV/Sales, EV/EBITDA, P/E, P/B
#  • Auto-projektion Rev/EPS 1–3 år (utan valutakonvertering av EPS — manuella tolkas i bolagets valuta)
#  • Utdelningshjälp: frekvensinferens → estimerad nästa utbetalningsdag + nettobelopp efter källskatt
# ============================================================

from __future__ import annotations
import math
import pandas as pd
import numpy as np
import streamlit as st

# ------------------------------------------------------------
# Lokala hjälpare (förväntas även finnas i Del 1)
# ------------------------------------------------------------
def _f(x):
    """Snäll float-konvertering."""
    try:
        if x is None: return None
        if isinstance(x, str) and x.strip() == "": return None
        v = float(x)
        if not math.isfinite(v): return None
        return v
    except Exception:
        return None

def _pos(x):
    """Positivt tal (>=0) → float, annars None."""
    v = _f(x)
    if v is None: return None
    return v if v >= 0 else None

def _nz(x, z):
    """x eller z om x är None."""
    return z if x is None else x

# ------------------------------------------------------------
# Parametrar & justeringsregler
# ------------------------------------------------------------
# Multipel-decay vid horisont (lätt kontraherande multiplar)
MULT_DECAY_PER_YEAR = {
    "ev_sales": 0.06,     # 6% per år, golv 60%
    "ev_ebitda": 0.05,
    "pe": 0.05,
    "pb": 0.00,           # ingen decay för PB default
}
MULT_FLOOR_FRAC = 0.60

# EPS/REV-projektion – clamp på CAGR
REV_CAGR_CLAMP = (-0.10, 0.35)
EPS_CAGR_CLAMP = (-0.20, 0.35)

# Bull/Bear 1Y-spread runt bas 1Y
BULL_UP = 0.20
BEAR_DN = 0.20

# Källskatter per valuta (brutto → netto)
WITHHOLDING = {
    "NOK": 0.25,
    "USD": 0.15,
    "CAD": 0.15,
    "SEK": 0.00,
    "EUR": 0.00,  # sätt 0 här; verkligt avdrag beror på depå/land
}

# ------------------------------------------------------------
# Multipel-verktyg
# ------------------------------------------------------------
def _decay_multiple(m0: float | None, years: int, method: str) -> float | None:
    m = _pos(m0)
    if m is None: return None
    d = MULT_DECAY_PER_YEAR.get(method, 0.05)
    m_y = m * (1.0 - d * years)
    floor = m * MULT_FLOOR_FRAC
    return max(m_y, floor)

def _pe_anchor(pe_ttm: float | None, pe_fwd: float | None, w_ttm: float = 0.5) -> float | None:
    pt = _pos(pe_ttm); pf = _pos(pe_fwd)
    if pt is None and pf is None: return None
    if pt is None: return pf
    if pf is None: return pt
    return w_ttm * pt + (1.0 - w_ttm) * pf

def _price_from_pe(eps: float | None, pe: float | None) -> float | None:
    e = _f(eps); p = _pos(pe)
    if e is None or p is None: return None
    return e * p

def _ev_from_sales(rev: float | None, mult: float | None) -> float | None:
    r = _pos(rev); m = _pos(mult)
    if r is None or m is None: return None
    return r * m

def _ev_from_ebitda(ebitda: float | None, mult: float | None) -> float | None:
    e = _f(ebitda)  # får vara <=0
    m = _pos(mult)
    if e is None or m is None: return None
    return e * m

def _equity_price_from_ev(ev_target: float | None, net_debt: float | None, shares_fd: float | None) -> float | None:
    e = _f(ev_target); nd = _f(_nz(net_debt, 0.0)); s = _pos(shares_fd)
    if e is None or s is None: return None
    return max(0.0, (e - nd) / s)

# ------------------------------------------------------------
# Projektioner (utan valutakonvertering av EPS)
#   • Manuella EPS/REV antas redan vara i bolagets rapportvaluta
# ------------------------------------------------------------
def _clamp(v: float | None, lo: float, hi: float) -> float | None:
    if v is None: return None
    return max(lo, min(hi, v))

def _project_series(ttm: float | None, cagr: float | None) -> tuple[float|None, float|None, float|None]:
    """Returnerar (t0, t1, t2) där t0 = ttm, t1 = ttm*(1+cagr), t2 = t1*(1+cagr)."""
    t0 = _f(ttm)
    cg = _f(cagr)
    if t0 is None: return (None, None, None)
    if cg is None: return (t0, None, None)
    t1 = t0 * (1.0 + cg)
    t2 = t1 * (1.0 + cg)
    return (t0, t1, t2)

# ------------------------------------------------------------
# Huvud: compute_methods_for_row
#  Returnerar EXAKT 3 värden: (targets_dict, meta_dict, debug_dict)
# ------------------------------------------------------------
def compute_methods_for_row(
    ticker: str,
    row: dict | pd.Series,
    snapshot: dict,
    rev_cagr_hint: float | None = None,
    eps_cagr_hint: float | None = None,
) -> tuple[dict, dict, dict]:
    """
    Bygger riktkurser enligt vald metod:
      • 'ev_sales'   → EV = Rev * mult; Price = (EV - NetDebt)/Shares
      • 'ev_ebitda'  → EV = EBITDA * mult
      • 'pe'         → Price = EPS * PE_anchor
      • 'pb'         → Price = PB * BVPS
    FÖRUTSÄTTNINGAR:
      • Ingen valutakonvertering av EPS i denna funktion.
      • Manuella EPS/REV antas redan i bolagets valuta (du sa att du själv konverterar).
    """
    # ----- Läs manuella inputs (om finns)
    metod = (str(row.get("Metod") or row.get("method") or "ev_sales")).strip().lower()

    # Multiplar / ankare (manuellt överstyr → annars snapshot/fallback)
    mult_ev_sales = _f(row.get("EV/S-multipel") or row.get("Multipel EV/S") or row.get("evs_mult"))
    mult_ev_ebitda = _f(row.get("EV/EBITDA-multipel") or row.get("Multipel EV/EBITDA") or row.get("eve_mult"))
    pe_manual = _f(row.get("P/E-ankare") or row.get("pe_anchor"))
    pb_manual = _f(row.get("P/B-ankare") or row.get("pb_anchor"))

    # EPS manuellt/snapshot
    eps_ttm_m = _f(row.get("EPS TTM (manuell)") or row.get("eps_ttm_manual"))
    eps1y_m   = _f(row.get("EPS 1Y (manuell)") or row.get("eps_1y_manual"))
    eps2y_m   = _f(row.get("EPS 2Y (manuell)") or row.get("eps_2y_manual"))
    eps_ttm   = _nz(eps_ttm_m, snapshot.get("eps_ttm"))
    eps1y     = _nz(eps1y_m, None)
    eps2y     = _nz(eps2y_m, None)

    # Revenue manuellt/snapshot (manuellt fält kan vara i enheter eller "miljoner" – hanteras i Del 2)
    rev_ttm_m = _f(row.get("Revenue TTM (manuell)") or row.get("rev_ttm_manual"))
    rev1y_m   = _f(row.get("Revenue 1Y (manuell)") or row.get("rev_1y_manual"))
    rev2y_m   = _f(row.get("Revenue 2Y (manuell)") or row.get("rev_2y_manual"))

    rev_ttm   = _nz(rev_ttm_m, snapshot.get("revenue_ttm"))
    rev_cagr_hist = rev_cagr_hint
    eps_cagr_hist = eps_cagr_hint

    # Om inga manuella EPS 1Y/2Y: använd CAGR-projektion
    if eps1y is None or eps2y is None:
        c_eps = _clamp(eps_cagr_hist, *EPS_CAGR_CLAMP) if eps_cagr_hist is not None else snapshot.get("eps_cagr_long")
        e0, e1, e2 = _project_series(eps_ttm, c_eps)
        eps1y = _nz(eps1y, e1)
        eps2y = _nz(eps2y, e2)

    # Revenue 1Y/2Y från CAGR om manuellt saknas
    if rev1y_m is None or rev2y_m is None:
        c_rev = _clamp(rev_cagr_hist, *REV_CAGR_CLAMP)
        r0, r1, r2 = _project_series(rev_ttm, c_rev)
        rev1y = _nz(rev1y_m, r1)
        rev2y = _nz(rev2y_m, r2)
    else:
        rev1y, rev2y = rev1y_m, rev2y_m

    # Övriga snapshotvärden
    shares = _nz(_f(row.get("Utestående aktier")) , snapshot.get("shares"))
    net_debt = snapshot.get("net_debt")
    pe_ttm = snapshot.get("pe_ttm")
    pe_fwd = snapshot.get("pe_fwd")
    pb_snap = snapshot.get("p_to_book")
    bvps = snapshot.get("bvps")
    price_now = snapshot.get("price")

    # Bygg ankare/multiplar om manuellt saknas
    if pe_manual is None:
        pe_manual = _pe_anchor(pe_ttm, pe_fwd, w_ttm=0.6)
    if pb_manual is None:
        pb_manual = pb_snap
    if mult_ev_sales is None:
        mult_ev_sales = snapshot.get("ev_to_sales")
    # EV/EBITDA – använd snapshot EV/EBITDA som utgång om finns
    if mult_ev_ebitda is None:
        mult_ev_ebitda = snapshot.get("ev_to_ebitda")

    # Projektion 0/1/2/3 år (utifrån vald metod)
    targets: dict[str, float | None] = {
        "Riktkurs idag": None,
        "Riktkurs 1 år": None,
        "Riktkurs 2 år": None,
        "Riktkurs 3 år": None,
        "Bull 1 år": None,
        "Bear 1 år": None,
        "Aktuell kurs (0)": _f(price_now),
    }
    meta: dict[str, any] = {"Metod använd": metod}
    dbg: dict[str, any] = {}

    # ----- EV/Sales
    if metod == "ev_sales":
        m0 = mult_ev_sales
        m1 = _decay_multiple(m0, 1, metod)
        m2 = _decay_multiple(m0, 2, metod)
        m3 = _decay_multiple(m0, 3, metod)

        # Rev path
        r0 = _f(rev_ttm)
        r1 = _f(rev1y)
        r2 = _f(rev2y)
        # approx för år 3
        c_rev_eff = None
        if r0 and r1:
            try: c_rev_eff = (r1 / r0) - 1.0
            except Exception: c_rev_eff = None
        r3 = (r2 * (1.0 + c_rev_eff)) if (r2 is not None and c_rev_eff is not None) else None

        ev0 = _ev_from_sales(r0, m0)
        ev1 = _ev_from_sales(r1, m1)
        ev2 = _ev_from_sales(r2, m2)
        ev3 = _ev_from_sales(r3, m3)

        p0 = _equity_price_from_ev(ev0, net_debt, shares)
        p1 = _equity_price_from_ev(ev1, net_debt, shares)
        p2 = _equity_price_from_ev(ev2, net_debt, shares)
        p3 = _equity_price_from_ev(ev3, net_debt, shares)

        targets.update({"Riktkurs idag": p0, "Riktkurs 1 år": p1, "Riktkurs 2 år": p2, "Riktkurs 3 år": p3})
        if p1 is not None:
            targets["Bull 1 år"] = p1 * (1.0 + BULL_UP)
            targets["Bear 1 år"] = p1 * (1.0 - BEAR_DN)

        meta.update({
            "EV/S-multipel (0/1/2/3y)": (m0, m1, m2, m3),
            "Rev (0/1/2/3y)": (r0, r1, r2, r3),
            "Shares FD": shares,
            "Net debt": net_debt,
        })
        dbg.update({"ev(0..3)": (ev0, ev1, ev2, ev3)})

    # ----- EV/EBITDA
    elif metod == "ev_ebitda":
        m0 = mult_ev_ebitda
        m1 = _decay_multiple(m0, 1, metod)
        m2 = _decay_multiple(m0, 2, metod)
        m3 = _decay_multiple(m0, 3, metod)

        ebitda_ttm = snapshot.get("ebitda_ttm")
        # antar EBITDA växer i takt med rev-cagr om inget bättre finns
        c_rev_eff = _clamp(rev_cagr_hint, *REV_CAGR_CLAMP)
        e0, e1, e2 = _project_series(ebitda_ttm, c_rev_eff)
        e3 = (e2 * (1.0 + _nz(c_rev_eff, 0.0))) if e2 is not None else None

        ev0 = _ev_from_ebitda(e0, m0)
        ev1 = _ev_from_ebitda(e1, m1)
        ev2 = _ev_from_ebitda(e2, m2)
        ev3 = _ev_from_ebitda(e3, m3)

        p0 = _equity_price_from_ev(ev0, net_debt, shares)
        p1 = _equity_price_from_ev(ev1, net_debt, shares)
        p2 = _equity_price_from_ev(ev2, net_debt, shares)
        p3 = _equity_price_from_ev(ev3, net_debt, shares)

        targets.update({"Riktkurs idag": p0, "Riktkurs 1 år": p1, "Riktkurs 2 år": p2, "Riktkurs 3 år": p3})
        if p1 is not None:
            targets["Bull 1 år"] = p1 * (1.0 + BULL_UP)
            targets["Bear 1 år"] = p1 * (1.0 - BEAR_DN)

        meta.update({
            "EV/EBITDA-multipel (0/1/2/3y)": (m0, m1, m2, m3),
            "EBITDA (0/1/2/3y)": (e0, e1, e2, e3),
            "Shares FD": shares,
            "Net debt": net_debt,
        })
        dbg.update({"ev(0..3)": (ev0, ev1, ev2, ev3)})

    # ----- P/E
    elif metod == "pe":
        pe0 = pe_manual
        pe1 = _decay_multiple(pe0, 1, metod)
        pe2 = _decay_multiple(pe0, 2, metod)
        pe3 = _decay_multiple(pe0, 3, metod)

        # EPS path (respektera manuella 1Y/2Y om angivna)
        c_eps_eff = _clamp(eps_cagr_hint, *EPS_CAGR_CLAMP)
        e0, e1, e2 = _project_series(eps_ttm, c_eps_eff)
        if eps1y is not None: e1 = eps1y
        if eps2y is not None: e2 = eps2y
        e3 = (e2 * (1.0 + _nz(c_eps_eff, 0.0))) if e2 is not None else None

        p0 = _price_from_pe(e0, pe0)
        p1 = _price_from_pe(e1, pe1)
        p2 = _price_from_pe(e2, pe2)
        p3 = _price_from_pe(e3, pe3)

        targets.update({"Riktkurs idag": p0, "Riktkurs 1 år": p1, "Riktkurs 2 år": p2, "Riktkurs 3 år": p3})
        if p1 is not None:
            targets["Bull 1 år"] = p1 * (1.0 + BULL_UP)
            targets["Bear 1 år"] = p1 * (1.0 - BEAR_DN)

        meta.update({
            "P/E-ankare (0/1/2/3y)": (pe0, pe1, pe2, pe3),
            "EPS (0/1/2/3y)": (e0, e1, e2, e3),
        })

    # ----- P/B
    elif metod == "pb":
        pb0 = pb_manual
        pb1 = _decay_multiple(pb0, 1, metod)
        pb2 = _decay_multiple(pb0, 2, metod)
        pb3 = _decay_multiple(pb0, 3, metod)

        # BVPS path – antar oförändrat om inget angivet
        b0 = _f(bvps)
        b1 = b0
        b2 = b0
        b3 = b0

        p0 = _price_from_pe(b0, pb0)  # återanvänder multiplikation (b*pb)
        p1 = _price_from_pe(b1, pb1)
        p2 = _price_from_pe(b2, pb2)
        p3 = _price_from_pe(b3, pb3)

        targets.update({"Riktkurs idag": p0, "Riktkurs 1 år": p1, "Riktkurs 2 år": p2, "Riktkurs 3 år": p3})
        if p1 is not None:
            targets["Bull 1 år"] = p1 * (1.0 + BULL_UP)
            targets["Bear 1 år"] = p1 * (1.0 - BEAR_DN)

        meta.update({
            "P/B-ankare (0/1/2/3y)": (pb0, pb1, pb2, pb3),
            "BVPS (0/1/2/3y)": (b0, b1, b2, b3),
        })

    else:
        meta["Fel"] = f"Okänd metod: {metod}"

    # Säkerställ float med två decimaler på targets (om värde finns)
    for k in list(targets.keys()):
        if targets[k] is not None:
            try:
                targets[k] = float(targets[k])
            except Exception:
                pass

    return targets, meta, dbg

# ------------------------------------------------------------
# Utdelningsberäkningar (SEK-total, nästa utbetalning — estimerad)
# ------------------------------------------------------------
def infer_frequency_to_payments(freq: str | None) -> int:
    if freq is None: return 1
    f = str(freq).upper()
    if f == "M": return 12
    if f == "Q": return 4
    if f == "S": return 2   # semiannual
    return 1                # annual

def estimate_next_pay_date(last_div_date: pd.Timestamp | None, freq: str | None) -> pd.Timestamp | None:
    """Grov estimering av nästa *betalningsdag* från senaste utdelningsdatum (kan vara ex-datum i Yahoo)."""
    if last_div_date is None:
        return None
    n = infer_frequency_to_payments(freq)
    # Antag betalning 30 dagar efter ex-datum, och periodisering enligt frekvens
    months = {12: 1, 4: 3, 2: 6, 1: 12}.get(n, 12)
    try:
        approx_ex_next = last_div_date + pd.DateOffset(months=months)
        pay_next = approx_ex_next + pd.Timedelta(days=30)
        return pay_next.normalize()
    except Exception:
        return None

def compute_dividend_next_row(
    ticker: str,
    shares_owned: float,
    snapshot: dict,
    fx_rate_func=None,
) -> dict:
    """
    Returnerar info om 'nästa utdelning' (estimerad), både brutto & netto i SEK.
    Kräver: snapshot["annual_dividend"], snapshot["dividend_frequency"], snapshot["currency"].
    """
    res = {
        "ticker": ticker,
        "currency": snapshot.get("currency"),
        "shares": shares_owned,
        "annual_div": snapshot.get("annual_dividend"),
        "freq": snapshot.get("dividend_frequency"),
        "next_pay_date": None,
        "gross_per_share": None,
        "gross_total": None,
        "net_total": None,
        "net_total_sek": None,
        "withholding": None,
    }
    if _pos(shares_owned) is None:
        return res

    # försök läsa senaste utd.datum ur Yahoo-dividends
    last_div_date = None
    try:
        tk = yf.Ticker(ticker)
        divs = None
        try:
            divs = tk.get_dividends()
        except Exception:
            divs = getattr(tk, "dividends", None)
        if divs is not None and hasattr(divs, "index") and len(divs) > 0:
            last_div_date = pd.to_datetime(divs.index[-1])
    except Exception:
        pass

    freq = snapshot.get("dividend_frequency")
    pay_date = estimate_next_pay_date(last_div_date, freq)
    res["next_pay_date"] = pay_date

    n_pay = infer_frequency_to_payments(freq)
    annual_div = _f(snapshot.get("annual_dividend"))
    if annual_div is None or n_pay <= 0:
        return res

    per_pay = annual_div / float(n_pay)
    gross_total = per_pay * float(shares_owned)

    cur = str(snapshot.get("currency") or "USD").upper()
    wht = WITHHOLDING.get(cur, 0.0)
    net_total = gross_total * (1.0 - wht)

    fx = 1.0
    if callable(fx_rate_func):
        try:
            fx = _f(fx_rate_func(cur)) or 1.0
        except Exception:
            fx = 1.0

    res.update({
        "gross_per_share": per_pay,
        "gross_total": gross_total,
        "net_total": net_total,
        "net_total_sek": net_total * fx,
        "withholding": wht,
    })
    return res

def compute_upcoming_dividends_table(
    positions_df: pd.DataFrame,
    snapshots: dict[str, dict],
    fx_rate_func=None,
) -> pd.DataFrame:
    """
    Bygger en tabell över estimerad nästa utbetalning per innehav.
    positions_df förväntar kolumner: ['Ticker', 'Antal aktier'].
    """
    rows = []
    for _, r in positions_df.iterrows():
        t = str(r.get("Ticker") or r.get("ticker") or "").strip()
        if not t:
            continue
        shares = _f(r.get("Antal aktier") or r.get("antal") or r.get("shares")) or 0.0
        snap = snapshots.get(t, {})
        rows.append(compute_dividend_next_row(t, shares, snap, fx_rate_func))

    df = pd.DataFrame(rows)
    if not df.empty and "next_pay_date" in df.columns:
        try:
            df = df.sort_values(["next_pay_date", "ticker"])
        except Exception:
            pass
    return df

# ============================================================
# app.py — Del 4/6 — UI (Analys, Ranking, Portfölj m. kommande utdelningar)
#  • Analys: sök/val av bolag, val av metod (ev_sales/ev_ebitda/pe/pb),
#            riktkurser (0/1/2/3 år) + Bull/Bear 1 år, spara till DATA/Snapshot
#  • Ranking: uppsida per horisont (idag/1/2/3 år), valfritt filter "endast innehav"
#  • Portfölj: P/L i SEK, årlig utdelning i SEK, samt tabell "Nästa utdelningar (est)"  
# ============================================================

from __future__ import annotations
import time
import numpy as np
import pandas as pd
import streamlit as st

# ---- Hjälpare (synkar med tidigare delar) ------------------
def _f(x):
    try:
        if x is None: return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            if s == "": return None
            v = float(s)
        else:
            v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None

def _pos(x):
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(x, z):
    return z if (x is None or (isinstance(x, float) and pd.isna(x))) else x

def _format_num(x, nd=2):
    v = _f(x)
    return "—" if v is None else f"{v:.{nd}f}"

def _fx_rate_from_state(ccy: str) -> float:
    """SEK per 1 enhet ccy, hämtar från st.session_state['FX'] (fallback 1.0 för SEK)."""
    try:
        mp = st.session_state.get("FX", {}) or {}
        c = (ccy or "SEK").upper()
        if c == "SEK": return 1.0
        v = _f(mp.get(c))
        return float(v) if v else 0.0
    except Exception:
        return 0.0

def _names_map_from_df(df: pd.DataFrame) -> dict[str, str]:
    mp = {}
    if df is None or df.empty: return mp
    for _, r in df.iterrows():
        t = str(r.get("Ticker") or "").strip()
        if not t: continue
        mp[t] = str(r.get("Bolagsnamn") or "").strip()
    return mp

def _select_with_search_nav_fallback(label: str, options: list[str], names_map: dict[str,str], idx_key: str, query_key: str) -> str | None:
    """
    Minimal fallback om din avancerade _select_with_search_nav inte finns.
    """
    ss = st.session_state
    q = st.text_input("Sök (ticker/bolagsnamn)", value=ss.get(query_key, ""))
    ss[query_key] = q
    ql = (q or "").lower().strip()
    filtered = [t for t in options if (ql in t.lower() or ql in (names_map.get(t,"").lower()))] or options
    if not filtered:
        st.info("Inget matchande resultat."); return None
    idx = ss.get(idx_key, 0) % len(filtered)
    col_prev, col_mid, col_next = st.columns([1,6,1])
    with col_prev:
        if st.button("◀︎", key=f"{idx_key}_prev"):
            ss[idx_key] = (idx - 1) % len(filtered); st.rerun()
    with col_next:
        if st.button("▶︎", key=f"{idx_key}_next"):
            ss[idx_key] = (idx + 1) % len(filtered); st.rerun()
    with col_mid:
        cur = filtered[idx]
        nm  = names_map.get(cur, "")
        st.caption(f"Aktuell: **{cur}**" + (f" — {nm}" if nm else ""))
    labels = [f"{t} — {names_map.get(t, '')}" if names_map.get(t) else t for t in filtered]
    sel_label = st.selectbox(label, labels, index=idx)
    sel_idx = labels.index(sel_label) if sel_label in labels else idx
    ss[idx_key] = sel_idx
    return filtered[sel_idx]

# ---- Yttre beroenden från Del 1/2/3 (redan definierade där) ----
# • read_data_df, write_data_df, _append_rows
# • get_fx_map, fetch_yahoo_snapshot, fetch_yahoo_rev_cagr, fetch_yahoo_eps_cagr_hist
# • compute_methods_for_row (från Del 3/6)
# • append_result_row (om du använder Resultat-logg)

# ============================================================
# Analys – sök/beräkna/spara
# ============================================================
def page_analysis():
    st.header("🔬 Analys")

    df: pd.DataFrame = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad."); return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)

    # Använd din avancerade sök/bläddrare om den finns, annars fallback
    if " _select_with_search_nav" in globals() or "_select_with_search_nav" in globals():
        sel = globals().get("_select_with_search_nav", _select_with_search_nav_fallback)(
            "Välj bolag", tickers, names_map, "analysis_idx", "analysis_q"
        )
    else:
        sel = _select_with_search_nav_fallback("Välj bolag", tickers, names_map, "analysis_idx", "analysis_q")

    if not sel:
        st.info("Välj ett bolag."); return

    row = df.loc[df["Ticker"].astype(str) == sel]
    if row.empty:
        st.error("Kunde inte hitta vald rad."); return
    row = row.iloc[0].to_dict()

    # Val av metod + ev. manuella multiplar/ankare
    st.markdown("#### Metod & parametrar")
    c1,c2,c3,c4 = st.columns(4)
    with c1:
        met = st.selectbox("Metod", ["ev_sales","ev_ebitda","pe","pb"], index=0)
    with c2:
        evs = st.text_input("EV/S multipel (tom = Yahoo/fallback)", value="")
    with c3:
        eve = st.text_input("EV/EBITDA multipel (tom = Yahoo/fallback)", value="")
    with c4:
        pea = st.text_input("P/E-ankare (tom = TTM/FWD-ankare)", value="")
    c5,c6 = st.columns(2)
    with c5:
        pba = st.text_input("P/B-ankare (tom = Yahoo/fallback)", value="")
    with c6:
        st.caption("EPS manuella antas redan i bolagets valuta (ingen auto-konvertering).")

    # bygg ett "row override" med metod + manuella multiplar
    row_override = dict(row)
    row_override["Metod"] = met
    if evs.strip(): row_override["EV/S-multipel"] = _f(evs)
    if eve.strip(): row_override["EV/EBITDA-multipel"] = _f(eve)
    if pea.strip(): row_override["P/E-ankare"] = _f(pea)
    if pba.strip(): row_override["P/B-ankare"] = _f(pba)

    # Hämtningar för beräkningsmotor
    with st.spinner("Hämtar snapshot och tillväxt…"):
        snap = fetch_yahoo_snapshot(sel)
        time.sleep(0.08)
        rc   = fetch_yahoo_rev_cagr(sel)            # {"rev_cagr": …}
        ec   = fetch_yahoo_eps_cagr_hist(sel)       # {"eps_cagr": …}

    rev_cagr_hint = rc.get("rev_cagr")
    eps_cagr_hint = ec.get("eps_cagr")

    # Beräkna riktkurser
    with st.spinner("Beräknar riktkurser…"):
        targets, meta, dbg = compute_methods_for_row(
            sel, row_override, snap, rev_cagr_hint=rev_cagr_hint, eps_cagr_hint=eps_cagr_hint
        )

    price0 = _f(snap.get("price"))
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Kurs", _format_num(price0))
    c2.metric("Riktkurs idag", _format_num(targets.get("Riktkurs idag")))
    c3.metric("Riktkurs 1 år", _format_num(targets.get("Riktkurs 1 år")))
    c4.metric("Riktkurs 2 år", _format_num(targets.get("Riktkurs 2 år")))
    c5.metric("Riktkurs 3 år", _format_num(targets.get("Riktkurs 3 år")))

    c6,c7 = st.columns(2)
    c6.metric("Bull 1 år", _format_num(targets.get("Bull 1 år")))
    c7.metric("Bear 1 år", _format_num(targets.get("Bear 1 år")))

    st.caption(f"Metod: **{meta.get('Metod använd','?')}**  ·  Valuta: **{(snap.get('currency') or row.get('Valuta') or 'USD')}**")

    st.markdown("---")
    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara riktkurser till DATA"):
            try:
                idx = df.index[df["Ticker"].astype(str) == sel][0]
                df.at[idx, "Primär metod"] = met
                df.at[idx, "Riktkurs idag"] = _f(targets.get("Riktkurs idag"))
                df.at[idx, "Riktkurs 1 år"] = _f(targets.get("Riktkurs 1 år"))
                df.at[idx, "Riktkurs 2 år"] = _f(targets.get("Riktkurs 2 år"))
                df.at[idx, "Riktkurs 3 år"] = _f(targets.get("Riktkurs 3 år"))
                st.session_state["DATA"] = df
                write_data_df(df)
                st.success("Riktkurser sparade till DATA.")
            except Exception as e:
                st.error(f"Kunde inte spara: {e}")
    with colB:
        if st.button("📸 Lägg snapshot (ark)"):
            try:
                _append_rows("Snapshot", [[
                    pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                    sel,
                    (snap.get("currency") or row.get("Valuta") or "USD"),
                    met,
                    _f(targets.get("Riktkurs idag")),
                    _f(targets.get("Riktkurs 1 år")),
                    _f(targets.get("Riktkurs 2 år")),
                    _f(targets.get("Riktkurs 3 år")),
                    _f(snap.get("pe_ttm")),
                    0.10  # decay loggas som exempel; styrs i Del 3/Settings i din bas
                ]])
                st.success("Snapshot tillagd.")
            except Exception as e:
                st.error(f"Kunde inte spara snapshot: {e}")

    st.markdown("---")
    st.subheader("Hela databasen (ofiltererad vy)")
    st.dataframe(st.session_state["DATA"], use_container_width=True)

# ============================================================
# Ranking – uppsida per horisont
# ============================================================
def page_ranking():
    st.header("🏆 Ranking – Uppsida")
    df: pd.DataFrame = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad."); return

    only_owned = st.checkbox("Visa endast innehav (Antal aktier > 0)", value=False)
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)

    base = df.copy()
    if only_owned:
        base = base[(pd.to_numeric(base["Antal aktier"], errors="coerce") > 0)]

    rows = []
    prog = st.progress(0.0)
    total = len(base)

    for i, (_, r) in enumerate(base.iterrows(), start=1):
        try:
            tkr = str(r.get("Ticker"))
            # välj metod: dataraden kan ha "Primär metod", annars 'ev_sales'
            m = str(_nz(r.get("Primär metod"), "ev_sales")).strip().lower()
            row_override = r.to_dict()
            row_override["Metod"] = m

            snap = fetch_yahoo_snapshot(tkr)
            rc   = fetch_yahoo_rev_cagr(tkr)
            ec   = fetch_yahoo_eps_cagr_hist(tkr)

            targets, meta, _dbg = compute_methods_for_row(
                tkr, row_override, snap,
                rev_cagr_hint=rc.get("rev_cagr"),
                eps_cagr_hint=ec.get("eps_cagr"),
            )
            price = _f(snap.get("price"))
            tgt   = _f(targets.get(f"Riktkurs {horizon}"))
            up    = ((tgt - price) / price * 100.0) if (_pos(price) and _pos(tgt)) else None
            rows.append({
                "Ticker": tkr,
                "Valuta": str(_nz(snap.get("currency"), r.get("Valuta") or "USD")).upper(),
                "Kurs": price,
                f"Riktkurs {horizon}": tgt,
                "Uppsida (%)": up,
                "Metod": m,
            })
        except Exception:
            pass
        prog.progress(i/total if total else 1.0)

    prog.empty()
    if not rows:
        st.info("Inget att visa."); return

    rank = pd.DataFrame(rows)
    rank = rank.sort_values("Uppsida (%)", ascending=False, na_position="last").reset_index(drop=True)
    st.caption(f"{len(rank)} bolag")
    st.dataframe(rank, use_container_width=True)

    st.markdown("---")
    if st.checkbox("Visa ett bolag i taget"):
        idx = st.number_input("Index", min_value=1, max_value=len(rank), value=1, step=1)
        item = rank.iloc[int(idx)-1]
        st.metric("Ticker", item["Ticker"])
        c1,c2,c3 = st.columns(3)
        c1.metric("Kurs", _format_num(item["Kurs"]))
        c2.metric(f"Riktkurs {horizon}", _format_num(item[f"Riktkurs {horizon}"]))
        upv = item["Uppsida (%)"]
        c3.metric("Uppsida (%)", "—" if pd.isna(upv) else f"{upv:.1f}%")
        st.caption(f"Metod: {item['Metod']}  ·  Valuta: {item['Valuta']}")

# ============================================================
# Portfölj – P/L och nästa utdelningar (est)
#  • Respekterar GAV (SEK) och visar P/L i SEK
#  • Kommande utdelningar hämtas via compute_upcoming_dividends_table (Del 3)
# ============================================================
def page_portfolio():
    st.header("📊 Portfölj (SEK) + kommande utdelningar")

    df: pd.DataFrame = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad."); return

    fx_map = st.session_state.get("FX", {}) or {}

    # Portföljtabell
    rows = []
    tot_mv = tot_cost = tot_div_y = 0.0

    for _, r in df.iterrows():
        try:
            tkr = str(r.get("Ticker") or "").strip()
            if not tkr: continue
            qty = _pos(r.get("Antal aktier")) or 0.0
            if qty <= 0: continue

            ccy = str(_nz(r.get("Valuta","USD"))).upper()
            fx  = _fx_rate_from_state(ccy)
            # Kurs: först DATA, annars Yahoo
            price = _pos(r.get("Aktuell kurs"))
            if price is None:
                snap = fetch_yahoo_snapshot(tkr)
                price = _pos(snap.get("price"))

            gav_sek = _pos(r.get("GAV (SEK)")) or 0.0
            mv_sek   = float(price or 0.0) * float(qty) * float(fx)
            cost_sek = float(gav_sek) * float(qty)
            pl_sek   = mv_sek - cost_sek
            pl_pct   = (pl_sek / cost_sek * 100.0) if cost_sek > 0 else None

            # Årlig utd (netto, SEK)
            # Försök: DATA "Årlig utdelning" annars Yahoo "annual_dividend"
            annual_ps = _pos(r.get("Årlig utdelning"))
            if annual_ps is None:
                snap = fetch_yahoo_snapshot(tkr)
                annual_ps = _pos(snap.get("annual_dividend"))
            # Källskatt från Settings (Del 1) om funktionen finns, annars fallback-tablå från Del 3
            try:
                wht = get_withholding_for(ccy, get_settings_map())
            except Exception:
                # fallback: samma default som Del 3
                wht = {"USD":0.15,"CAD":0.15,"NOK":0.25}.get(ccy, 0.0)

            div_y_net_sek = (float(annual_ps) * float(qty) * (1.0 - float(wht)) * float(fx)) if (annual_ps and fx) else 0.0

            rows.append({
                "Ticker": tkr,
                "Valuta": ccy,
                "Antal": qty,
                "FX (→SEK)": fx,
                "Kurs": price,
                "MV (SEK)": mv_sek,
                "GAV (SEK)": gav_sek,
                "AV (SEK)": cost_sek,
                "P/L (SEK)": pl_sek,
                "P/L (%)": pl_pct,
                "Årlig utd (SEK)": div_y_net_sek,
                "Utd/mån (SEK)": div_y_net_sek/12.0,
            })

            tot_mv   += mv_sek
            tot_cost += cost_sek
            tot_div_y += div_y_net_sek
        except Exception:
            continue

    tbl = pd.DataFrame(rows, columns=[
        "Ticker","Valuta","Antal","FX (→SEK)","Kurs","MV (SEK)","GAV (SEK)","AV (SEK)",
        "P/L (SEK)","P/L (%)","Årlig utd (SEK)","Utd/mån (SEK)"
    ])

    tot_pl = tot_mv - tot_cost
    tot_pl_pct = (tot_pl / tot_cost * 100.0) if tot_cost > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Portföljvärde (SEK)", f"{tot_mv:,.0f}".replace(',', ' '))
    col2.metric("Anskaffningsvärde (SEK)", f"{tot_cost:,.0f}".replace(',', ' '))
    col3.metric("Orealiserad vinst (SEK)", f"{tot_pl:,.0f}".replace(',', ' '))
    col4.metric("Orealiserad vinst (%)", f"{tot_pl_pct:.2f}%")

    col5, col6 = st.columns(2)
    col5.metric("Årlig utdelning (SEK, netto)", f"{tot_div_y:,.0f}".replace(',', ' '))
    col6.metric("Utdelning per månad (SEK, netto)", f"{(tot_div_y/12.0):,.0f}".replace(',', ' '))

    if tbl.empty:
        st.info("Inga innehav med antal > 0.")
    else:
        st.dataframe(tbl, use_container_width=True)

    # ----------------------------------------------
    st.markdown("---")
    st.subheader("📅 Nästa utdelningar (estimerade betalningsdatum)")
    # Bygg snapshots för innehav
    snaps = {}
    pos_df = df[(pd.to_numeric(df["Antal aktier"], errors="coerce") > 0)].copy()
    for t in pos_df["Ticker"].dropna().astype(str).unique().tolist():
        try:
            snaps[t] = fetch_yahoo_snapshot(t)
            time.sleep(0.03)
        except Exception:
            snaps[t] = {}

    nd = compute_upcoming_dividends_table(
        positions_df=pos_df[["Ticker","Antal aktier"]],
        snapshots=snaps,
        fx_rate_func=_fx_rate_from_state,
    )
    if nd.empty:
        st.info("Ingen prognos att visa. Saknas utdelningshistorik eller innehav.")
    else:
        # Städa & formatera lite
        view = nd.copy()
        if "next_pay_date" in view.columns:
            view.rename(columns={"next_pay_date":"Datum","ticker":"Ticker","currency":"Valuta"}, inplace=True)
            try:
                view["Datum"] = pd.to_datetime(view["Datum"]).dt.date.astype(str)
            except Exception:
                pass
        # Visning – endast relevanta kolumner
        keep = ["Datum","Ticker","Valuta","shares","gross_per_share","net_total","net_total_sek","withholding"]
        view = view[[c for c in keep if c in view.columns]].rename(columns={
            "shares":"Antal",
            "gross_per_share":"Per aktie (brutto)",
            "net_total":"Netto (valuta)",
            "net_total_sek":"Netto (SEK)",
            "withholding":"Källskatt"
        })
        st.dataframe(view, use_container_width=True)

# ============================================================
# app.py — Del 5/6 — Settings, Snapshot, Editor, Lägg till, Batch, Boot & Main
# ============================================================

import time
import numpy as np
import pandas as pd
import streamlit as st

# ---------- Småhjälpare (samma semantik som tidigare delar) ----------
def _f(x):
    try:
        if x is None: return None
        if isinstance(x, str):
            s = x.strip().replace(" ", "").replace(",", ".")
            if s == "": return None
            v = float(s)
        else:
            v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None

def _pos(x):
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(x, z):
    return z if (x is None or (isinstance(x, float) and pd.isna(x))) else x

def _round2_or_none(x):
    v = _f(x)
    return None if v is None else round(float(v), 2)

def _parse_float(s: str | None) -> float | None:
    if s is None:
        return None
    s = str(s).strip().replace(" ", "").replace(",", ".")
    if s == "" or s == "—":
        return None
    try:
        return float(s)
    except Exception:
        return None

def _maybe(v):
    return v if (v is not None and not (isinstance(v, float) and pd.isna(v))) else None

def _names_map_from_df(df: pd.DataFrame) -> dict[str, str]:
    mp = {}
    if df is None or df.empty: return mp
    for _, r in df.iterrows():
        t = str(r.get("Ticker") or "").strip()
        if not t: continue
        mp[t] = str(r.get("Bolagsnamn") or "").strip()
    return mp

def _select_with_search_nav_fallback(label: str, options: list[str], names_map: dict[str,str], idx_key: str, query_key: str) -> str | None:
    ss = st.session_state
    q = st.text_input("Sök (ticker/bolagsnamn)", value=ss.get(query_key, ""))
    ss[query_key] = q
    ql = (q or "").lower().strip()
    filtered = [t for t in options if (ql in t.lower() or ql in (names_map.get(t,"").lower()))] or options
    if not filtered:
        st.info("Inget matchande resultat."); return None
    idx = ss.get(idx_key, 0) % len(filtered)
    col_prev, col_mid, col_next = st.columns([1,6,1])
    with col_prev:
        if st.button("◀︎", key=f"{idx_key}_prev"):
            ss[idx_key] = (idx - 1) % len(filtered); st.rerun()
    with col_next:
        if st.button("▶︎", key=f"{idx_key}_next"):
            ss[idx_key] = (idx + 1) % len(filtered); st.rerun()
    with col_mid:
        cur = filtered[idx]
        nm  = names_map.get(cur, "")
        st.caption(f"Aktuell: **{cur}**" + (f" — {nm}" if nm else ""))
    labels = [f"{t} — {names_map.get(t, '')}" if names_map.get(t) else t for t in filtered]
    sel_label = st.selectbox(label, labels, index=idx)
    sel_idx = labels.index(sel_label) if sel_label in labels else idx
    ss[idx_key] = sel_idx
    return filtered[sel_idx]

def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad",
            "Rev 2Y uppdaterad","Senast manuellt uppdaterad"]
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *cols]) if 'DATA_COLUMNS' in globals() else df
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _fx_rate_from_state(ccy: str) -> float:
    try:
        mp = st.session_state.get("FX", {}) or {}
        c = (ccy or "SEK").upper()
        if c == "SEK": return 1.0
        v = _f(mp.get(c))
        return float(v) if v else 0.0
    except Exception:
        return 0.0

# ---------- Funktioner som förväntas finnas från Del 1/3 ----------
# read_data_df, write_data_df, get_settings_map, get_fx_map, get_withholding_for
# fetch_yahoo_snapshot, fetch_yahoo_rev_cagr, fetch_yahoo_eps_cagr_hist
# compute_methods_for_row, compute_upcoming_dividends_table, _append_rows
# DATA_COLUMNS, SETTINGS_TITLE, FX_TITLE, SNAPSHOT_TITLE, RESULT_TITLE, DEFAULT_BUCKETS

# ============================================================
# Settings
# ============================================================
def page_settings():
    st.header("⚙️ Settings")
    s = get_settings_map()
    fx = get_fx_map()

    c1,c2,c3 = st.columns(3)
    with c1:
        primary = st.selectbox("Primär valuta", ["SEK","USD","EUR","NOK","CAD"],
                               index=["SEK","USD","EUR","NOK","CAD"].index(s.get("primary_currency","SEK")))
        pe_w = float(_f(s.get("pe_anchor_weight_ttm")) or 0.50)
        pe_w = st.number_input("Vikt TTM i PE-ankare", 0.0, 1.0, pe_w, 0.05)
    with c2:
        decay = float(_f(s.get("multiple_decay")) or 0.10)
        decay = st.number_input("Multipel-decay/år", 0.0, 0.5, decay, 0.01)
        auto  = st.checkbox("Auto-uppdatera FX vid start", value=str(s.get("auto_refresh_on_start","0"))=="1")
    with c3:
        st.caption("Källskatt per valuta")
        wh_usd = st.number_input("USD", 0.0, 0.5, float(_f(s.get("withholding_USD")) or 0.15), 0.01)
        wh_nok = st.number_input("NOK", 0.0, 0.5, float(_f(s.get("withholding_NOK")) or 0.25), 0.01)
        wh_cad = st.number_input("CAD", 0.0, 0.5, float(_f(s.get("withholding_CAD")) or 0.15), 0.01)
        wh_eur = st.number_input("EUR", 0.0, 0.5, float(_f(s.get("withholding_EUR")) or 0.15), 0.01)
        wh_sek = st.number_input("SEK", 0.0, 0.5, float(_f(s.get("withholding_SEK")) or 0.00), 0.01)

    if st.button("💾 Spara inställningar"):
        s_df = _read_df(SETTINGS_TITLE)
        if s_df.empty:
            s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)

        def set_kv(k, v):
            nonlocal s_df
            if "Key" not in s_df or "Value" not in s_df:
                s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)
            mask = s_df["Key"].astype(str) == k
            if mask.any():
                s_df.loc[mask, "Value"] = str(v)
            else:
                s_df = pd.concat([s_df, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)

        set_kv("primary_currency", primary)
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
    st.subheader("Valutakurser")
    st.dataframe(_read_df(FX_TITLE), use_container_width=True)
    if st.button("🔁 Hämta/uppdatera valutakurser"):
        # get_fx_map() uppdaterar redan Valutakurser-bladet i basimplementationen
        _ = get_fx_map()
        st.success("Valutakurser uppdaterade.")

# ============================================================
# Snapshot
# ============================================================
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True)

# ============================================================
# Editor (manuella fält, ingen EPS-valutakonvertering)
# ============================================================
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series | dict):
    snap = fetch_yahoo_snapshot(ticker)
    rc   = fetch_yahoo_rev_cagr(ticker)
    ec   = fetch_yahoo_eps_cagr_hist(ticker)
    updates = {
        "Timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Bolagsnamn": _maybe(snap.get("company_name")),
        "Sektor": _maybe(snap.get("sector")),
        "Aktuell kurs": _round2_or_none(snap.get("price")),
        "Valuta": (snap.get("currency") or (existing_row.get("Valuta") if isinstance(existing_row, (pd.Series, dict)) else None)),
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
        "Rev CAGR": _maybe(rc.get("rev_cagr")),
        "EPS CAGR": _maybe(ec.get("eps_cagr")),
        "Årlig utdelning": _maybe(snap.get("annual_dividend")),
        "Utdelningsfrekvens": _maybe(snap.get("dividend_frequency")),
        "Senast auto uppdaterad": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Auto källa": "Yahoo",
    }
    # Rensa None/NaN/""
    clean = {}
    for k, v in updates.items():
        if v is None: continue
        if isinstance(v, float) and pd.isna(v): continue
        if isinstance(v, str) and v.strip() == "": continue
        clean[k] = v
    return clean

def page_editor():
    st.header("✏️ Editor (manuella fält)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad."); return

    df = _ensure_editor_stamp_cols(df)

    need = ["Ticker","Bucket","Antal aktier","GAV (SEK)","EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y"]
    for c in need:
        if c not in df.columns: df[c] = np.nan

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)

    # använd avancerad sök/bläddrare om den finns, annars fallback
    select_fn = globals().get("_select_with_search_nav", _select_with_search_nav_fallback)
    sel = select_fn("Välj rad (Ticker)", tickers, names_map, "editor_idx", "editor_q")
    if not sel:
        st.info("Välj ett bolag."); return

    ridx = df.index[df["Ticker"].astype(str) == sel]
    if len(ridx) == 0:
        st.error("Kunde inte hitta vald rad."); return
    idx = ridx[0]
    row = df.loc[idx].copy()

    c1, c2 = st.columns(2)
    with c1:
        new_ticker = st.text_input("Ticker", value=str(row.get("Ticker") or "").upper())
        antal_in   = st.text_input("Antal aktier", value=str(_f(row.get("Antal aktier")) or ""))
        gav_in     = st.text_input("GAV (SEK)", value=str(_f(row.get("GAV (SEK)")) or ""))
        current_bucket = str(_nz(row.get("Bucket"), DEFAULT_BUCKETS[0]))
        bucket_sel = st.selectbox("Bucket", DEFAULT_BUCKETS,
                                  index=DEFAULT_BUCKETS.index(current_bucket) if current_bucket in DEFAULT_BUCKETS else 0)
    with c2:
        eps1_in = st.text_input("EPS 1Y (estimat)", value=str(_f(row.get("EPS 1Y")) or ""))
        eps2_in = st.text_input("EPS 2Y (estimat)", value=str(_f(row.get("EPS 2Y")) or ""))
        rev1_in = st.text_input("Rev 1Y (miljoner, 8.81B skrivs 8810)", value=str(_f(row.get("Rev 1Y")) or ""))
        rev2_in = st.text_input("Rev 2Y (miljoner)", value=str(_f(row.get("Rev 2Y")) or ""))

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara rad (session)"):
            try:
                antal_v = _parse_float(antal_in) or 0.0
                gav_v   = _parse_float(gav_in)
                eps1_v  = _parse_float(eps1_in)  # ingen konvertering — antas i bolagsvaluta
                eps2_v  = _parse_float(eps2_in)
                rev1_vm = (_parse_float(rev1_in) or 0.0) * 1_000_000.0 if rev1_in not in (None,"") else None
                rev2_vm = (_parse_float(rev2_in) or 0.0) * 1_000_000.0 if rev2_in not in (None,"") else None

                df.loc[idx, "Ticker"] = str(new_ticker).upper().strip() if new_ticker else sel
                df.loc[idx, "Bucket"] = bucket_sel
                df.loc[idx, "Antal aktier"] = antal_v
                if gav_v is not None:
                    df.loc[idx, "GAV (SEK)"] = gav_v
                df.loc[idx, "EPS 1Y"] = eps1_v
                df.loc[idx, "EPS 2Y"] = eps2_v
                df.loc[idx, "Rev 1Y"] = rev1_vm
                df.loc[idx, "Rev 2Y"] = rev2_vm
                df.loc[idx, "Senast manuellt uppdaterad"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

                st.session_state["DATA"] = df
                st.success("Sparat i minnet. Använd knappen till höger för att skriva rad → Google Sheets + auto-hämtning.")
            except Exception as e:
                st.error(f"Fel vid sparning: {e}")

    with colB:
        if st.button("⬆️ Spara rad till Google Sheets + hämta från Yahoo"):
            try:
                st.session_state["DATA"] = df
                tkr = str(_nz(df.loc[idx, "Ticker"], new_ticker or sel)).upper()
                updates = _build_updates_from_yahoo(tkr, df.loc[idx])

                df_cur = df.copy()
                for k, v in updates.items():
                    if k not in df_cur.columns:
                        df_cur[k] = np.nan
                    df_cur.at[idx, k] = v

                write_data_df(df_cur)
                st.session_state["DATA"] = df_cur
                st.success(f"{tkr}: Rad sparad till Google Sheets och uppdaterad från Yahoo.")
            except Exception as e:
                st.error(f"Fel vid sparning till Sheets: {e}")

    st.markdown("---")
    st.subheader("Förhandsgranskning")
    st.dataframe(df.loc[[idx]], use_container_width=True)

# ============================================================
# ➕ Lägg till ticker
# ============================================================
def page_add_ticker():
    st.header("➕ Lägg till ticker")

    tkr = st.text_input("Ticker", key="add_ticker").upper().strip()
    c1, c2, c3 = st.columns(3)
    with c1:
        bolagsnamn = st.text_input("Bolagsnamn", key="add_name")
        sektor     = st.text_input("Sektor", key="add_sector")
    with c2:
        bucket = st.selectbox("Bucket", DEFAULT_BUCKETS, index=0, key="add_bucket")
        valuta = st.text_input("Valuta (t.ex. USD)", value="USD", key="add_ccy").upper()
    with c3:
        antal = st.text_input("Antal aktier", value="", key="add_qty")
        gav   = st.text_input("GAV (SEK)", value="", key="add_gav")

    st.markdown("**Prognos-/manuella fält (frivilliga):**")
    c4, c5 = st.columns(2)
    with c4:
        eps1_in = st.text_input("EPS 1Y (estimat)", key="add_eps1")  # ingen auto-konvertering
        rev1_in = st.text_input("Rev 1Y (miljoner, 8.81B skrivs 8810)", key="add_rev1")
    with c5:
        eps2_in = st.text_input("EPS 2Y (estimat)", key="add_eps2")
        rev2_in = st.text_input("Rev 2Y (miljoner)", key="add_rev2")

    colA, colB = st.columns(2)
    with colA:
        do_prefill = st.checkbox("Hämta & fyll på fält från Yahoo", value=True, key="add_prefill")
        if st.button("🔍 Hämta från Yahoo nu"):
            if not tkr:
                st.warning("Ange en ticker först.")
            else:
                try:
                    snap = fetch_yahoo_snapshot(tkr)
                    st.session_state["add_name"]   = snap.get("company_name") or st.session_state.get("add_name","")
                    st.session_state["add_sector"] = snap.get("sector") or st.session_state.get("add_sector","")
                    st.session_state["add_ccy"]    = (snap.get("currency") or st.session_state.get("add_ccy","USD")).upper()
                    st.success("Fält uppdaterade från Yahoo.")
                except Exception as e:
                    st.error(f"Kunde inte hämta från Yahoo: {e}")

    with colB:
        st.caption("Tips: lämna Antal/GAV tomt om du ännu inte äger aktien.")

    st.markdown("---")
    if st.button("💾 Lägg till i DATA (spara till Google Sheets)"):
        if not tkr:
            st.warning("Ticker krävs.")
            return
        try:
            base_df = read_data_df()
            if not base_df.empty and (base_df["Ticker"].astype(str).str.upper() == tkr.upper()).any():
                st.error("Ticker finns redan i DATA. Använd Editor för att uppdatera befintlig rad.")
                return

            new_row = {c: np.nan for c in DATA_COLUMNS}
            new_row.update({
                "Timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Ticker": tkr,
                "Bolagsnamn": bolagsnamn if bolagsnamn else np.nan,
                "Sektor": sektor if sektor else np.nan,
                "Bucket": bucket,
                "Valuta": valuta or "USD",
            })

            qty_v = _parse_float(antal) or 0.0
            gav_v = _parse_float(gav)
            new_row["Antal aktier"] = qty_v
            if gav_v is not None:
                new_row["GAV (SEK)"] = gav_v

            eps1_v  = _parse_float(eps1_in)
            eps2_v  = _parse_float(eps2_in)
            rev1_vm = (_parse_float(rev1_in) or 0.0) * 1_000_000.0 if rev1_in not in (None,"") else None
            rev2_vm = (_parse_float(rev2_in) or 0.0) * 1_000_000.0 if rev2_in not in (None,"") else None
            if eps1_v is not None: new_row["EPS 1Y"] = eps1_v
            if eps2_v is not None: new_row["EPS 2Y"] = eps2_v
            if rev1_vm is not None: new_row["Rev 1Y"] = rev1_vm
            if rev2_vm is not None: new_row["Rev 2Y"] = rev2_vm
            new_row["Senast manuellt uppdaterad"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

            if do_prefill:
                updates = _build_updates_from_yahoo(tkr, pd.Series(new_row))
                new_row.update(updates)
                time.sleep(0.10)

            out_df = pd.concat([base_df, pd.DataFrame([new_row])], ignore_index=True)
            write_data_df(out_df)
            st.session_state["DATA"] = out_df
            st.success(f"{tkr} tillagd i DATA och sparad till Google Sheets.")
        except Exception as e:
            st.error(f"Kunde inte lägga till: {e}")

# ============================================================
# Batch (massuppdatering Yahoo)
# ============================================================
def _apply_updates_to_df_row(df: pd.DataFrame, idx, updates: dict) -> int:
    changed = 0
    for k, v in (updates or {}).items():
        if k not in df.columns:
            df[k] = np.nan
        old = df.at[idx, k]
        same = (pd.isna(old) and pd.isna(v)) or (not pd.isna(old) and not pd.isna(v) and str(old) == str(v))
        if same:
            continue
        df.at[idx, k] = v
        changed += 1
    return changed

def page_batch():
    st.header("🧩 Massuppdatering (Yahoo) — 1s per bolag")

    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    df = _ensure_editor_stamp_cols(df)
    tickers = sorted(df["Ticker"].dropna().astype(str).unique().tolist())
    names_map = _names_map_from_df(df)

    q = st.text_input("Sök (ticker/bolagsnamn) för urval", value=st.session_state.get("batch_q", ""), key="batch_q")

    def _match(t: str) -> bool:
        if not q:
            return True
        nm = names_map.get(t, "")
        ql = q.lower()
        return (ql in t.lower()) or (ql in nm.lower())

    filtered = [t for t in tickers if _match(t)]
    if not filtered:
        st.info("Inget matchande resultat – visar alla bolag.")
        filtered = tickers[:]

    ss = st.session_state
    if "batch_idx" not in ss:
        ss["batch_idx"] = 0
    if "batch_selected" not in ss:
        ss["batch_selected"] = []

    col_prev, col_mid, col_next, col_toggle = st.columns([1, 6, 1, 2])
    with col_prev:
        if st.button("◀︎", key="batch_prev") and filtered:
            ss["batch_idx"] = (ss["batch_idx"] - 1) % len(filtered)
            st.rerun()
    with col_next:
        if st.button("▶︎", key="batch_next") and filtered:
            ss["batch_idx"] = (ss["batch_idx"] + 1) % len(filtered)
            st.rerun()
    with col_mid:
        cur = filtered[ss["batch_idx"] % len(filtered)]
        nm = names_map.get(cur, "")
        st.caption(f"Aktuell: **{cur}**" + (f" — {nm}" if nm else ""))
    with col_toggle:
        cur = filtered[ss["batch_idx"] % len(filtered)]
        label = "➕ Lägg till i urval" if cur not in ss["batch_selected"] else "➖ Ta bort från urval"
        if st.button(label, key="batch_toggle"):
            if cur in ss["batch_selected"]:
                ss["batch_selected"] = [x for x in ss["batch_selected"] if x != cur]
            else:
                ss["batch_selected"] = ss["batch_selected"] + [cur]
            st.rerun()

    default_sel = [x for x in ss["batch_selected"] if x in filtered]
    sel = st.multiselect("Välj tickers att uppdatera (tom = alla)", options=filtered, default=default_sel)
    ss["batch_selected"] = sel

    target = tickers if len(sel) == 0 else sel

    delay = st.slider("Fördröjning per bolag (sek)", 0.5, 5.0, 1.0, 0.5)
    go = st.button("🚀 Starta")

    if not go:
        return

    progress = st.progress(0.0)
    status = st.empty()
    df_cur = df.copy()
    changed_total = 0

    for i, tkr in enumerate(target, start=1):
        try:
            status.write(f"Uppdaterar {i}/{len(target)} – {tkr}")
            mask = df_cur["Ticker"].astype(str).str.upper() == str(tkr).upper()
            existing = df_cur[mask].iloc[0] if mask.any() else pd.Series({"Ticker": tkr})
            updates = _build_updates_from_yahoo(tkr, existing)
            if mask.any():
                idx = df_cur.index[mask][0]
                changed_total += _apply_updates_to_df_row(df_cur, idx, updates)
            else:
                base = {c: np.nan for c in DATA_COLUMNS}
                base.update({"Timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"), "Ticker": tkr})
                base.update(updates)
                df_cur = pd.concat([df_cur, pd.DataFrame([base])], ignore_index=True)
                changed_total += len(updates)
        except Exception as e:
            st.error(f"{tkr}: {e}")
        progress.progress(i/len(target))
        time.sleep(float(delay))

    write_data_df(df_cur)
    progress.empty()
    status.empty()
    st.success(f"Klar. {len(target)} bolag uppdaterade. {changed_total} fält ändrades.")

# ============================================================
# Boot & Main
# ============================================================
def _boot_session():
    # Data
    if "DATA" not in st.session_state or st.session_state["DATA"] is None or st.session_state["DATA"].empty:
        try:
            df = read_data_df()
            st.session_state["DATA"] = _ensure_editor_stamp_cols(df)
        except Exception as e:
            st.error(f"Kunde inte läsa Data-bladet: {e}")
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)

    # Settings
    try:
        st.session_state["SETTINGS"] = get_settings_map()
    except Exception:
        st.session_state["SETTINGS"] = {}

    # FX (ev. auto-refresh vid start)
    try:
        if str(st.session_state["SETTINGS"].get("auto_refresh_on_start","0")) == "1":
            st.session_state["FX"] = get_fx_map()  # basversionen uppdaterar också arket
        else:
            st.session_state["FX"] = get_fx_map()
    except Exception:
        st.session_state["FX"] = {"SEK":1.0,"USD":1.0,"EUR":1.0,"NOK":1.0,"CAD":1.0}

def main():
    _boot_session()

    st.sidebar.title("Navigering")
    if st.sidebar.button("↻ Läs om från Google Sheets"):
        st.session_state["DATA"] = _ensure_editor_stamp_cols(read_data_df())
        st.success("DATA omläst.")
        st.rerun()
    if st.sidebar.button("⬆️ Spara session → Google Sheets"):
        write_data_df(st.session_state["DATA"])
        st.success("DATA sparad.")

    page = st.sidebar.radio(
        "Gå till:",
        ["Analys","Portfölj","Ranking","Editor","Lägg till ticker","Batch","Settings","Snapshot"],
        index=0
    )

    try:
        if page == "Analys":
            page_analysis()
        elif page == "Portfölj":
            page_portfolio()
        elif page == "Ranking":
            page_ranking()
        elif page == "Editor":
            page_editor()
        elif page == "Lägg till ticker":
            page_add_ticker()
        elif page == "Batch":
            page_batch()
        elif page == "Settings":
            page_settings()
        elif page == "Snapshot":
            page_snapshot()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# ============================================================
# app.py — Del 6/6 — Entrypoint (kör appen)
# ============================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import streamlit as st
        st.error(f"💥 Fel i huvudloopen: {e}")
