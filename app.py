# app.py — Aktieanalys & investeringsförslag
# Del 1/4: Importer, konstanter, Google Sheets, cache/backoff, schema & settings

from __future__ import annotations

import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# Externa fetchers (Yahoo). Finnhub-estimat implementeras defensivt (kan vara tomt).
import yfinance as yf

# Google Sheets
import gspread
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import APIError, WorksheetNotFound

# ──────────────────────────────────────────────────────────────────────────────
# Grundinställningar/tema
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Aktieanalys och investeringsförslag", layout="wide")

# ──────────────────────────────────────────────────────────────────────────────
# Konstanter & kolumnschema
# ──────────────────────────────────────────────────────────────────────────────
DATA_TITLE     = "Data"
RESULT_TITLE   = "Resultat"
SNAPSHOT_TITLE = "Snapshot"
SETTINGS_TITLE = "Settings"
RATES_TITLE    = "Valutakurser"   # används om du har en FX-flik (annars default=1.0)

# Minimalt, robust schema (lägg gärna till egna kolumner – koden tål extra)
DATA_COLUMNS: List[str] = [
    "Ticker", "Bolagsnamn", "Bucket", "Valuta",
    "Antal aktier", "Årlig utdelning", "Utdelning CAGR",
    "Primär metod",

    # Inputfält (kan fyllas från Yahoo eller manuellt)
    "Aktuell kurs", "Utestående aktier",
    "PE TTM", "PE FWD",
    "Rev TTM", "EBITDA TTM",
    "EV/Revenue", "EV/EBITDA",
    "P/B", "BVPS",

    # Estimat/antaganden för motor
    "EPS TTM", "EPS 1Y", "EPS CAGR", "Rev CAGR",

    # Bokföring
    "Senast auto uppdaterad", "Auto källa", "Senast manuellt uppdaterad",
]

RESULT_COLUMNS: List[str] = [
    "Timestamp", "Ticker", "Valuta", "Metod",
    "Riktkurs idag", "Riktkurs 1 år", "Riktkurs 2 år", "Riktkurs 3 år",
]

# ──────────────────────────────────────────────────────────────────────────────
# Småhjälpare
# ──────────────────────────────────────────────────────────────────────────────
def _now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _f(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return None
        if isinstance(x, str) and not x.strip():
            return None
        return float(x)
    except Exception:
        return None

def _nz(val, alt):
    return val if (val is not None and (not isinstance(val, float) or not np.isnan(val)) and val != "") else alt

def _pos(x) -> Optional[float]:
    v = _f(x)
    return v if (v is not None and np.isfinite(v)) else None

def _fmt_money(v: Optional[float], ccy: str) -> str:
    if v is None: return "—"
    s = f"{v:,.2f}".replace(",", " ")
    return f"{s} {ccy}"

def _fmt_sek(v: Optional[float]) -> str:
    if v is None: return "—"
    return f"{v:,.0f} SEK".replace(",", " ")

# ──────────────────────────────────────────────────────────────────────────────
# Google-autentisering
# ──────────────────────────────────────────────────────────────────────────────
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _get_spreadsheet_id() -> str:
    # Försök i ordning: st.secrets["SPREADSHEET_ID"] → URL → miljövariabel
    sid = st.secrets.get("SPREADSHEET_ID")
    if sid:
        return sid
    url = st.secrets.get("SPREADSHEET_URL") or os.environ.get("SPREADSHEET_URL")
    if url:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
        if m:
            return m.group(1)
    sid = os.environ.get("SPREADSHEET_ID")
    if not sid:
        raise RuntimeError("Saknar Spreadsheet-ID. Lägg in SPREADSHEET_ID i secrets.")
    return sid

@st.cache_resource(show_spinner=False)
def _get_gspread_client() -> gspread.Client:
    creds_any = st.secrets.get("GOOGLE_CREDENTIALS")
    if not creds_any:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i secrets.")
    # Tillåt både JSON-sträng och toml-/dict-objekt
    if isinstance(creds_any, str):
        import json
        creds = json.loads(creds_any)
    else:
        creds = dict(creds_any)
    creds = _normalize_private_key(creds)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    from google.oauth2.service_account import Credentials
    gc = gspread.Client(auth=Credentials.from_service_account_info(creds, scopes=scope))
    gc.session = gc.session  # no-op, men håller referensen
    return gc

@st.cache_resource(show_spinner=False)
def _get_spreadsheet() -> Spreadsheet:
    gc = _get_gspread_client()
    sid = _get_spreadsheet_id()
    return gc.open_by_key(sid)

# Exponentiell backoff för API-anrop (429, 5xx)
def _with_backoff(callable_fn, *args, **kwargs):
    delay = 0.8
    for attempt in range(6):
        try:
            return callable_fn(*args, **kwargs)
        except APIError as e:
            if attempt == 5:
                raise
            time.sleep(delay)
            delay *= 1.8

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    # Hämta eller skapa flik med backoff
    try:
        return _with_backoff(spread.worksheet, title)
    except WorksheetNotFound:
        return _with_backoff(spread.add_worksheet, title=title, rows=1000, cols=50)

# ──────────────────────────────────────────────────────────────────────────────
# Läs/skriv DataFrame till Google Sheets
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=60)
def _read_df(title: str) -> pd.DataFrame:
    sh = _get_spreadsheet()
    ws = _get_ws(sh, title)
    values = _with_backoff(ws.get_all_values)
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    # Försök konvertera numeriska kolumner försiktigt
    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c].str.replace(" ", "").str.replace(",", "."), errors="ignore")
        except Exception:
            pass
    return df

def _write_df(title: str, df: pd.DataFrame) -> None:
    sh = _get_spreadsheet()
    ws = _get_ws(sh, title)
    df = df.copy()
    df = df.fillna("")
    payload = [list(df.columns)] + df.astype(str).values.tolist()
    # Clear + batch-update i ett svep
    _with_backoff(ws.clear)
    _with_backoff(ws.update, payload)

# Publika wrappers
def read_data_df() -> pd.DataFrame:
    return _read_df(DATA_TITLE)

def write_data_df(df: pd.DataFrame) -> None:
    _write_df(DATA_TITLE, df)
    _read_df.clear()  # töm cache

def read_result_df() -> pd.DataFrame:
    return _read_df(RESULT_TITLE)

def write_result_df(df: pd.DataFrame) -> None:
    _write_df(RESULT_TITLE, df)
    _read_df.clear()

# ──────────────────────────────────────────────────────────────────────────────
# Säkerställ schema (skapa flikar, lägg till saknade kolumner)
# ──────────────────────────────────────────────────────────────────────────────
def ensure_sheet_schema() -> None:
    sh = _get_spreadsheet()
    for title in (DATA_TITLE, RESULT_TITLE, SNAPSHOT_TITLE, SETTINGS_TITLE, RATES_TITLE):
        _get_ws(sh, title)

    # Data
    try:
        df = _read_df(DATA_TITLE)
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        df = pd.DataFrame(columns=DATA_COLUMNS)
    else:
        for c in DATA_COLUMNS:
            if c not in df.columns:
                df[c] = ""

        # ordna om enligt schema (ägna inte överdrivet – behåll ev. extra kolumner på slutet)
        ordered = [c for c in DATA_COLUMNS if c in df.columns]
        extras  = [c for c in df.columns if c not in DATA_COLUMNS]
        df = df[ordered + extras]

    _write_df(DATA_TITLE, df)

    # Resultat
    try:
        rf = _read_df(RESULT_TITLE)
    except Exception:
        rf = pd.DataFrame()

    if rf.empty:
        rf = pd.DataFrame(columns=RESULT_COLUMNS)
    else:
        for c in RESULT_COLUMNS:
            if c not in rf.columns:
                rf[c] = ""
        rf = rf[RESULT_COLUMNS + [c for c in rf.columns if c not in RESULT_COLUMNS]]

    _write_df(RESULT_TITLE, rf)

# ──────────────────────────────────────────────────────────────────────────────
# Settings (defaultar om Settings-flik saknar värden)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_SETTINGS: Dict[str, Any] = {
    "pe_anchor_weight_ttm": 0.50,   # vikt mot PE TTM i ankare (resten mot PE FWD)
    "multiple_decay": 0.10,         # årlig multipelkompression
    # Källskatt per valuta
    "withholding_USD": 0.15,
    "withholding_NOK": 0.25,
    "withholding_CAD": 0.15,
    "withholding_SEK": 0.00,
    "withholding_EUR": 0.00,
}

@st.cache_data(show_spinner=False, ttl=120)
def get_settings_map() -> Dict[str, Any]:
    try:
        s = _read_df(SETTINGS_TITLE)
        m = dict(DEFAULT_SETTINGS)
        if not s.empty and {"Nyckel", "Värde"}.issubset(set(s.columns)):
            for _, r in s.iterrows():
                key = str(r.get("Nyckel", "")).strip()
                val = r.get("Värde")
                if key:
                    fv = _f(val)
                    m[key] = fv if fv is not None else (val if val != "" else m.get(key))
        return m
    except Exception:
        return dict(DEFAULT_SETTINGS)

def get_withholding_for(ccy: str, settings: Dict[str, Any]) -> float:
    return float(settings.get(f"withholding_{ccy.upper()}", 0.00))

# ──────────────────────────────────────────────────────────────────────────────
# FX-karta (enkel). Om du har en Valutakurser-flik: kolumner ["Valuta","Kurs_SEK"]
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=900)
def get_fx_map() -> Dict[str, float]:
    fx = {"SEK": 1.0, "USD": 11.0, "EUR": 11.8, "NOK": 1.0, "CAD": 8.5}  # fallback grovt
    try:
        df = _read_df(RATES_TITLE)
        if not df.empty:
            vcol = None
            for c in df.columns:
                if str(c).lower().strip() in ("valuta", "currency"):
                    vcol = c
            rcol = None
            for c in df.columns:
                if "sek" in str(c).lower() and "kurs" in str(c).lower():
                    rcol = c
            if vcol and rcol:
                fx = {}
                for _, r in df.iterrows():
                    k = str(r[vcol]).upper().strip()
                    v = _f(r[rcol])
                    if k and v:
                        fx[k] = v
                if "SEK" not in fx:
                    fx["SEK"] = 1.0
    except Exception:
        pass
    return fx

# ──────────────────────────────────────────────────────────────────────────────
# UI-hjälpare (delade mellan sidor)
# ──────────────────────────────────────────────────────────────────────────────
def info_badge(text: str):
    st.markdown(f"<div style='padding:8px;border-radius:6px;background:#eef2ff;border:1px solid #dbe3ff'>{text}</div>", unsafe_allow_html=True)

def success_badge(text: str):
    st.markdown(f"<div style='padding:8px;border-radius:6px;background:#e6ffed;border:1px solid #b6f2c6'>{text}</div>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# Datainsamling (Yahoo) och värderingsmotor
# ──────────────────────────────────────────────────────────────────────────────

VALID_METHODS = [
    "pe_hist_vs_eps",
    "ev_sales",
    "ev_ebitda",
    "ev_dacf",     # prox: samma som ebitda om DACF saknas
    "p_b",
    "p_nav",       # saknas oftast – lämnas tomt
    "p_tbv",       # saknas oftast – lämnas tomt
    "p_affo",      # REIT/BDC – lämnas tomt om saknas
    "p_fcf",
    "ev_fcf",
]

def safe_get(info: dict, *keys, default=None):
    cur = info
    for k in keys:
        if cur is None:
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Hämtar robusta, vanliga nyckeltal från Yahoo (yfinance).
    Allt är 'best effort' – None om saknas.
    """
    try:
        yf_t = yf.Ticker(ticker)
        info = yf_t.fast_info or {}
    except Exception:
        info = {}

    out: Dict[str, Any] = {}

    # Pris/valuta
    out["price"]  = _f(safe_get(info, "last_price")) or _f(safe_get(info, "last_price", default=None))
    out["ccy"]    = safe_get(info, "currency") or ""

    # Börsvärde/aktier
    out["mcap"]   = _f(safe_get(info, "market_cap"))
    out["shares_out"] = None
    if out["mcap"] and out["price"] and out["price"] > 0:
        out["shares_out"] = out["mcap"] / out["price"]

    # Multiplar
    try:
        finfo = yf.Ticker(ticker).info
    except Exception:
        finfo = {}

    # EPS/PE
    out["eps_ttm"]   = _f(finfo.get("trailingEps"))
    out["pe_ttm"]    = _f(finfo.get("trailingPE"))
    out["pe_fwd"]    = _f(finfo.get("forwardPE"))
    out["eps_1y"]    = None  # här kan man fylla från estimatkälla – lämnas None om saknas

    # Intäkter/EBITDA + multiplar
    out["rev_ttm"]   = _f(finfo.get("totalRevenue"))
    out["ebitda_ttm"]= _f(finfo.get("ebitda"))
    out["ev_sales"]  = _f(finfo.get("enterpriseToRevenue"))
    out["ev_ebitda"] = _f(finfo.get("enterpriseToEbitda"))

    # P/B & BVPS
    out["pb"]        = _f(finfo.get("priceToBook"))
    out["bvps"]      = _f(finfo.get("bookValue"))

    # Namn
    out["longName"]  = finfo.get("longName") or finfo.get("shortName") or ticker

    # Bucket heuristik (kan skrivas över i UI)
    out["bucket"]    = "Bucket A tillväxt"

    # Källflagga
    out["source"]    = "Yahoo Finance"

    return out

def merge_fetch_into_row(row: pd.Series, fetched: Dict[str, Any]) -> pd.Series:
    """
    Matar in hämtade värden i vår datastruktur – skriver bara in om värdet finns.
    Pekar inte ut 'manuellt vs auto' – men vi sparar 'Auto källa' + timestamp.
    """
    r = row.copy()

    r["Bolagsnamn"]      = _nz(r.get("Bolagsnamn"), fetched.get("longName"))
    r["Bucket"]          = _nz(r.get("Bucket"), fetched.get("bucket"))
    r["Valuta"]          = _nz(r.get("Valuta"), fetched.get("ccy"))

    r["Aktuell kurs"]    = _nz(r.get("Aktuell kurs"), fetched.get("price"))
    r["Utestående aktier"]= _nz(r.get("Utestående aktier"), fetched.get("shares_out"))

    r["PE TTM"]          = _nz(r.get("PE TTM"), fetched.get("pe_ttm"))
    r["PE FWD"]          = _nz(r.get("PE FWD"), fetched.get("pe_fwd"))

    r["Rev TTM"]         = _nz(r.get("Rev TTM"), fetched.get("rev_ttm"))
    r["EBITDA TTM"]      = _nz(r.get("EBITDA TTM"), fetched.get("ebitda_ttm"))

    r["EV/Revenue"]      = _nz(r.get("EV/Revenue"), fetched.get("ev_sales"))
    r["EV/EBITDA"]       = _nz(r.get("EV/EBITDA"), fetched.get("ev_ebitda"))

    r["P/B"]             = _nz(r.get("P/B"), fetched.get("pb"))
    r["BVPS"]            = _nz(r.get("BVPS"), fetched.get("bvps"))

    r["EPS TTM"]         = _nz(r.get("EPS TTM"), fetched.get("eps_ttm"))
    r["EPS 1Y"]          = _nz(r.get("EPS 1Y"), fetched.get("eps_1y"))

    r["Senast auto uppdaterad"] = _now_str()
    r["Auto källa"]      = fetched.get("source", "Yahoo Finance")

    return r

# ──────────────────────────────────────────────────────────────────────────────
# Sanity text (visas i Analys-tabellen)
# ──────────────────────────────────────────────────────────────────────────────
def build_sanity_note(row: pd.Series, settings: Dict[str, Any]) -> str:
    bits = []
    bits.append("price=" + ("ok" if _pos(row.get("Aktuell kurs")) else "—"))
    bits.append("eps_ttm=" + ("ok (source)" if _pos(row.get("EPS TTM")) else "—"))
    bits.append("eps_1y=" + ("ok" if _pos(row.get("EPS 1Y")) else "—"))
    bits.append("rev_ttm=" + ("ok" if _pos(row.get("Rev TTM")) else "—"))
    bits.append("ebitda_ttm=" + ("ok" if _pos(row.get("EBITDA TTM")) else "—"))
    bits.append("shares=" + ("ok" if _pos(row.get("Utestående aktier")) else "—"))
    pe_anchor = compute_pe_anchor(row, settings)
    bits.append(f"pe_anchor={_f(pe_anchor) if pe_anchor else '—'}")
    bits.append(f"decay={settings.get('multiple_decay', 0.1)}")
    return ", ".join(bits)

# ──────────────────────────────────────────────────────────────────────────────
# Ankare & multipel-decay
# ──────────────────────────────────────────────────────────────────────────────
def compute_pe_anchor(row: pd.Series, settings: Dict[str, Any]) -> Optional[float]:
    w = float(settings.get("pe_anchor_weight_ttm", 0.5))
    pe_ttm = _f(row.get("PE TTM"))
    pe_fwd = _f(row.get("PE FWD"))
    if pe_ttm is None and pe_fwd is None:
        return None
    if pe_ttm is None:
        return pe_fwd
    if pe_fwd is None:
        return pe_ttm
    return w * pe_ttm + (1.0 - w) * pe_fwd

def apply_decay(value: Optional[float], years: int, settings: Dict[str, Any]) -> Optional[float]:
    if value is None:
        return None
    d = float(settings.get("multiple_decay", 0.10))
    return value * ((1.0 - d) ** years)

# ──────────────────────────────────────────────────────────────────────────────
# Värderingsmetoder (returnerar pris per aktie, sedan decay för 1–3 år)
# ──────────────────────────────────────────────────────────────────────────────
def method_pe_hist_vs_eps(row: pd.Series, settings: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    pe_anchor = compute_pe_anchor(row, settings)
    eps_ttm = _f(row.get("EPS TTM"))
    eps_1y  = _f(row.get("EPS 1Y"))
    if pe_anchor is None or eps_ttm is None:
        return None, None, None, None
    today = pe_anchor * eps_ttm
    y1 = apply_decay(pe_anchor, 1, settings) * (eps_1y if eps_1y is not None else eps_ttm)
    y2 = apply_decay(pe_anchor, 2, settings) * (eps_1y if eps_1y is not None else eps_ttm)
    y3 = apply_decay(pe_anchor, 3, settings) * (eps_1y if eps_1y is not None else eps_ttm)
    return today, y1, y2, y3

def method_ev_sales(row: pd.Series, settings: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Approx: pris ≈ (EV/Sales) * (Revenue per aktie).
    EV→equity justeras EJ pga saknad nettoskuld – metod blir lik P/S-ankare.
    """
    mult = _f(row.get("EV/Revenue"))
    rev  = _f(row.get("Rev TTM"))
    sh   = _f(row.get("Utestående aktier"))
    if mult is None or rev is None or sh is None or sh <= 0:
        return None, None, None, None
    base = mult * (rev / sh)
    return base, None, None, None  # lämna framtid tom då vi saknar säkra rev-estimat

def method_ev_ebitda_like(row: pd.Series, settings: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Pris ≈ (EV/EBITDA) * (EBITDA per aktie). EV→equity ignoreras (saknar nettoskuld).
    """
    mult = _f(row.get("EV/EBITDA"))
    ebitda = _f(row.get("EBITDA TTM"))
    sh   = _f(row.get("Utestående aktier"))
    if mult is None or ebitda is None or sh is None or sh <= 0:
        return None, None, None, None
    today = mult * (ebitda / sh)
    y1 = apply_decay(today, 1, settings)
    y2 = apply_decay(today, 2, settings)
    y3 = apply_decay(today, 3, settings)
    return today, y1, y2, y3

def method_p_b(row: pd.Series, settings: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    pb = _f(row.get("P/B"))
    bvps = _f(row.get("BVPS"))
    if pb is None or bvps is None:
        return None, None, None, None
    today = pb * bvps
    y1 = apply_decay(today, 1, settings)
    y2 = apply_decay(today, 2, settings)
    y3 = apply_decay(today, 3, settings)
    return today, y1, y2, y3

def method_none(*args, **kwargs):
    return None, None, None, None

METHOD_IMPL = {
    "pe_hist_vs_eps": method_pe_hist_vs_eps,
    "ev_sales":       method_ev_sales,
    "ev_ebitda":      method_ev_ebitda_like,
    "ev_dacf":        method_ev_ebitda_like,  # proxy
    "p_b":            method_p_b,
    "p_nav":          method_none,
    "p_tbv":          method_none,
    "p_affo":         method_none,
    "p_fcf":          method_none,
    "ev_fcf":         method_none,
}

def pick_primary_method(row: pd.Series) -> str:
    # 1) Respektera manuellt vald metod om giltig
    m = str(row.get("Primär metod", "")).strip()
    if m in VALID_METHODS:
        return m
    # 2) Heuristik
    if _pos(row.get("EBITDA TTM")) and _pos(row.get("EV/EBITDA")):
        return "ev_ebitda"
    if _pos(row.get("EPS TTM")) and (compute_pe_anchor(row, DEFAULT_SETTINGS) is not None):
        return "pe_hist_vs_eps"
    if _pos(row.get("Rev TTM")) and _pos(row.get("EV/Revenue")):
        return "ev_sales"
    if _pos(row.get("P/B")) and _pos(row.get("BVPS")):
        return "p_b"
    return "pe_hist_vs_eps"  # fall-back

def compute_methods_table(row: pd.Series, settings: Dict[str, Any]) -> Tuple[pd.DataFrame, str]:
    """
    Returnerar en DF med kolumner: ['Metod','Idag','1 år','2 år','3 år']
    samt vald primär metod.
    """
    data = []
    for m in VALID_METHODS:
        fn = METHOD_IMPL.get(m, method_none)
        t0, t1, t2, t3 = fn(row, settings)
        data.append({
            "Metod": m,
            "Idag": t0,
            "1 år": t1,
            "2 år": t2,
            "3 år": t3,
        })
    df = pd.DataFrame(data)
    # sortera upp "vettiga" först
    order = ["pe_hist_vs_eps", "ev_sales", "ev_ebitda", "ev_dacf", "p_b", "p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf"]
    df["__ord"] = df["Metod"].apply(lambda x: order.index(x) if x in order else 999)
    df = df.sort_values("__ord").drop(columns="__ord").reset_index(drop=True)
    chosen = pick_primary_method(row)
    return df, chosen

def extract_primary_targets(df_methods: pd.DataFrame, primary_method: str) -> Dict[str, Optional[float]]:
    r = df_methods[df_methods["Metod"] == primary_method]
    if r.empty:
        return {"today": None, "y1": None, "y2": None, "y3": None}
    rr = r.iloc[0]
    return {
        "today": _f(rr["Idag"]),
        "y1": _f(rr["1 år"]),
        "y2": _f(rr["2 år"]),
        "y3": _f(rr["3 år"]),
    }

# ───────────────────────────────────────────────────────────
# Del 3/4 — Editor & Analys + presentation & spar-hjälpare
# ───────────────────────────────────────────────────────────

# ========== Hjälpformat ==========
def _fmt_num(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    try:
        return f"{float(x):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(x)

def _fmt_pct(x) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.1f}%"
    except Exception:
        return "—"

def _fmt_money(x, ccy: Optional[str]) -> str:
    if x is None:
        return "—"
    try:
        val = f"{float(x):,.2f}".replace(",", " ").replace(".", ",")
        return f"{val} {ccy or ''}".strip()
    except Exception:
        return "—"

def _now_str() -> str:
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ========== Data I/O (byggt ovanpå Del 1) ==========
@st.cache_data(ttl=60, show_spinner=False)
def read_data_df() -> pd.DataFrame:
    df = _read_df(DATA_TITLE)
    if df.empty:
        return pd.DataFrame(columns=list(DATA_COLUMNS))
    # säkerställ alla kolumner finns
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[DATA_COLUMNS].copy()

def save_data_df(df: pd.DataFrame) -> None:
    # skriv endast definierat schema, fyll resten till tomt
    out = df.copy()
    for c in DATA_COLUMNS:
        if c not in out.columns:
            out[c] = None
    _write_df(DATA_TITLE, out[DATA_COLUMNS])

def upsert_row(df: pd.DataFrame, new_row: Dict[str, Any], key="Ticker") -> pd.DataFrame:
    """Idempotent upsert: lägger till/updaterar exakt våra DATA_COLUMNS."""
    df = df.copy()
    # säkerställ schema
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = None
    tkr = str(new_row.get(key, "")).strip().upper()
    if not tkr:
        return df
    # skapa en komplett rad enligt schema
    full = {c: None for c in DATA_COLUMNS}
    for k, v in new_row.items():
        if k in full:
            full[k] = v
    mask = df[key].astype(str).str.upper() == tkr
    if mask.any():
        i = df[mask].index[0]
        base = df.loc[i].to_dict()
        base.update({k: v for k, v in full.items() if v is not None})
        df.loc[i] = base
    else:
        df = pd.concat([df, pd.DataFrame([full])], ignore_index=True)
    return df

# ========== Resultat-skrivning ==========
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
    ts = _now_str()
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
    if res_df.empty:
        _write_df(RESULT_TITLE, pd.DataFrame([new_row]))
        return
    # säkerställ kolumner
    for k in new_row.keys():
        if k not in res_df.columns:
            res_df[k] = None
    mask = res_df["Ticker"].astype(str) == ticker
    if mask.any():
        idx = res_df.index[mask][-1]
        for k, v in new_row.items():
            res_df.at[idx, k] = v
        _write_df(RESULT_TITLE, res_df)
    else:
        res_df = pd.concat([res_df, pd.DataFrame([new_row])[res_df.columns]], ignore_index=True)
        _write_df(RESULT_TITLE, res_df)

# ========== Metodhjälpare för vy ==========
def _pick_primary_targets(methods_df: pd.DataFrame) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Samma heuristik som tidigare: välj metod med flest icke-NaN; tie-break med prior-lista."""
    if methods_df is None or methods_df.empty:
        return None, None, None, None, None
    counts = methods_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if counts.empty:
        return None, None, None, None, None
    maxc = counts.max()
    candidates = list(counts[counts == maxc].index)
    prefer = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]
    chosen = next((p for p in prefer if p in candidates), candidates[0])
    row = methods_df[methods_df["Metod"] == chosen].iloc[0]
    return chosen, _f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"])

def _extract_targets_for(methods_df: pd.DataFrame, method: str) -> Dict[str, Optional[float]]:
    if methods_df is None or methods_df.empty:
        return {"today": None, "y1": None, "y2": None, "y3": None}
    r = methods_df[methods_df["Metod"] == method]
    if r.empty:
        return {"today": None, "y1": None, "y2": None, "y3": None}
    r = r.iloc[0]
    return {
        "today": _f(r["Idag"]),
        "y1": _f(r["1 år"]),
        "y2": _f(r["2 år"]),
        "y3": _f(r["3 år"]),
    }

def _compute_upsides(current_price: Optional[float], tgts: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    def pct(t):
        if not _pos(current_price) or t is None:
            return None
        return 100.0 * (t - current_price) / current_price
    return {"today": pct(tgts["today"]), "y1": pct(tgts["y1"]), "y2": pct(tgts["y2"]), "y3": pct(tgts["y3"])}

def _method_inputs_blurb(method: str) -> str:
    mapping = {
        "pe_hist_vs_eps": "PE-ankare (TTM/Fwd) × EPS (TTM/1Y) med multipel-decay.",
        "ev_sales":       "EV/Sales × intäkter, sedan till equity via nettoskuld/antal aktier.",
        "ev_ebitda":      "EV/EBITDA × EBITDA-stig + nettoskuld/aktier.",
        "ev_dacf":        "EV/DACF (proxy = EV/EBITDA) tills DACF finns.",
        "p_b":            "P/B × BVPS.",
        "p_nav":          "P/NAV × NAVPS (kräver NAV).",
        "p_tbv":          "P/TBV × TBVPS (kräver TBV).",
        "p_affo":         "P/AFFO × AFFO/aktie (REIT/BDC, kräver AFFO).",
        "p_fcf":          "P/FCF × FCF/aktie.",
        "ev_fcf":         "EV/FCF × FCF + nettoskuld/aktier.",
        "p_nii":          "P/NII × NII/aktie (BDC), kräver NII.",
    }
    return mapping.get(method, "")

# ========== Bolagspresentation ==========
def _presentation_panels(row: pd.Series, sanity_text: str, meta: Dict[str, Any], methods_df: pd.DataFrame, chosen_method: str):
    tkr = str(row.get("Ticker", "")).strip().upper()
    currency = meta.get("currency") or str(row.get("Valuta") or "USD").upper()
    price_now = meta.get("price")
    shares    = _f(row.get("Antal aktier")) or 0.0

    # hämta "rå" snapshot/estimat för att visa källor i presentation (kan vara samma som Compute använde)
    snap = fetch_yahoo_snapshot(tkr)
    est  = fetch_finnhub_estimates(tkr)

    # paneler: Hämtat vs Manuell
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📥 Hämtade fält (live)")
        st.write(
            pd.DataFrame([{
                "Valuta": snap.get("currency"),
                "Pris": snap.get("price"),
                "MCAP": snap.get("market_cap"),
                "EV": snap.get("ev"),
                "Nettoskuld": snap.get("net_debt"),
                "Antal aktier": snap.get("shares"),
                "Rev TTM": snap.get("revenue_ttm"),
                "EBITDA TTM": snap.get("ebitda_ttm"),
                "EV/Sales": snap.get("ev_to_sales"),
                "EV/EBITDA": snap.get("ev_to_ebitda"),
                "P/B": snap.get("p_to_book"),
                "BVPS": snap.get("bvps"),
                "EPS TTM": snap.get("eps_ttm"),
                "PE TTM": snap.get("pe_ttm"),
                "PE FWD": snap.get("pe_fwd"),
                "EPS 1Y (Finnhub)": est.get("eps_1y"),
                "EPS 2Y (Finnhub)": est.get("eps_2y"),
            }]).T.rename(columns={0: "Värde"})
        )
    with c2:
        st.subheader("✍️ Manuella/Data-fält")
        st.write(
            pd.DataFrame([{
                "Valuta": row.get("Valuta"),
                "Aktuell kurs": row.get("Aktuell kurs"),
                "Utestående aktier": row.get("Utestående aktier"),
                "Net debt": row.get("Net debt"),
                "Rev TTM": row.get("Rev TTM"),
                "EBITDA TTM": row.get("EBITDA TTM"),
                "EV/Revenue": row.get("EV/Revenue"),
                "EV/EBITDA": row.get("EV/EBITDA"),
                "P/B": row.get("P/B"),
                "BVPS": row.get("BVPS"),
                "EPS TTM": row.get("EPS TTM"),
                "PE TTM": row.get("PE TTM"),
                "PE FWD": row.get("PE FWD"),
                "EPS 1Y (Data)": row.get("EPS 1Y"),
                "Rev CAGR": row.get("Rev CAGR"),
                "EPS CAGR": row.get("EPS CAGR"),
                "Årsutdelning (DPS)": row.get("Årlig utdelning"),
                "Utdelning CAGR": row.get("Utdelning CAGR"),
                "Primär metod": row.get("Primär metod"),
            }]).T.rename(columns={0: "Värde"})
        )

    # beräknat: PE-ankare/decay + riktkurser (vald metod)
    st.subheader("🧮 Beräknat (ankare, decay & riktkurser)")
    tgt = _extract_targets_for(methods_df, chosen_method)
    ups = _compute_upsides(price_now, tgt)
    cA, cB, cC, cD, cE = st.columns(5)
    cA.metric("Idag",  _fmt_money(tgt["today"], currency))
    cB.metric("1 år",   _fmt_money(tgt["y1"],    currency))
    cC.metric("2 år",   _fmt_money(tgt["y2"],    currency))
    cD.metric("3 år",   _fmt_money(tgt["y3"],    currency))
    cE.metric("Metod",  chosen_method)

    u1, u2, u3, u4 = st.columns(4)
    u1.metric("Uppsida Idag", _fmt_pct(ups["today"]))
    u2.metric("Uppsida 1 år", _fmt_pct(ups["y1"]))
    u3.metric("Uppsida 2 år", _fmt_pct(ups["y2"]))
    u4.metric("Uppsida 3 år", _fmt_pct(ups["y3"]))
    st.caption(f"Sanity: {sanity_text} • PE-ankare={_fmt_num(meta.get('pe_anchor'))} • Decay/år={_fmt_num(meta.get('decay'))} • Pris={_fmt_money(price_now, currency)}")

# ========== Editor ==========
def page_editor():
    st.header("📝 Lägg till / uppdatera bolag")

    df = read_data_df()
    tickers = ["— nytt —"] + (sorted(df["Ticker"].dropna().astype(str).unique()) if not df.empty else [])
    pick = st.selectbox("Välj ticker", tickers, index=0)
    is_new = (pick == "— nytt —")

    # Initvärden om befintlig
    init = {c: None for c in DATA_COLUMNS}
    if not is_new:
        row = df[df["Ticker"].astype(str) == pick].iloc[0].to_dict()
        init.update(row)

    # Inmatning (utan form → så att fetch kan fylla direkt)
    c1, c2, c3 = st.columns(3)
    ticker  = c1.text_input("Ticker", value="" if is_new else str(init.get("Ticker") or "")).strip().upper()
    name    = c2.text_input("Bolagsnamn", value=str(init.get("Bolagsnamn") or ""))
    sector  = c3.text_input("Sektor", value=str(init.get("Sektor") or ""))

    bucket  = st.selectbox("Bucket", DEFAULT_BUCKETS, index=(DEFAULT_BUCKETS.index(init.get("Bucket")) if init.get("Bucket") in DEFAULT_BUCKETS else 0))
    valuta  = st.selectbox("Valuta", ["USD","EUR","NOK","CAD","SEK"], index=["USD","EUR","NOK","CAD","SEK"].index(str(init.get("Valuta") or "USD").upper()))

    d1, d2, d3, d4 = st.columns(4)
    antal   = d1.number_input("Antal aktier",      min_value=0,   step=1,    value=int(_nz(_f(init.get("Antal aktier")), 0)))
    gav_sek = d2.number_input("GAV (SEK)",         min_value=0.0, step=0.01, value=float(_nz(_f(init.get("GAV (SEK)")), 0.0)))
    kurs    = d3.number_input("Aktuell kurs",      min_value=0.0, step=0.01, value=float(_nz(_f(init.get("Aktuell kurs")), 0.0)))
    shares  = d4.number_input("Utestående aktier", min_value=0.0, step=1.0,  value=float(_nz(_f(init.get("Utestående aktier")), 0.0)))

    e1, e2, e3, e4 = st.columns(4)
    rev_ttm   = e1.number_input("Rev TTM",     min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("Rev TTM")), 0.0)))
    ebitda_t  = e2.number_input("EBITDA TTM",  min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("EBITDA TTM")), 0.0)))
    eps_ttm   = e3.number_input("EPS TTM",     min_value=0.0, step=0.01,   value=float(_nz(_f(init.get("EPS TTM")), 0.0)))
    net_debt  = e4.number_input("Net debt",    min_value=0.0, step=1000.0, value=float(_nz(_f(init.get("Net debt")), 0.0)))

    f1, f2, f3, f4 = st.columns(4)
    pe_ttm   = f1.number_input("PE TTM",      min_value=0.0, step=0.01, value=float(_nz(_f(init.get("PE TTM")), 0.0)))
    pe_fwd   = f2.number_input("PE FWD",      min_value=0.0, step=0.01, value=float(_nz(_f(init.get("PE FWD")), 0.0)))
    ev_rev   = f3.number_input("EV/Revenue",  min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EV/Revenue")), 0.0)))
    ev_ebit  = f4.number_input("EV/EBITDA",   min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EV/EBITDA")), 0.0)))

    g1, g2, g3, g4 = st.columns(4)
    pb      = g1.number_input("P/B",                min_value=0.0, step=0.01, value=float(_nz(_f(init.get("P/B")), 0.0)))
    bvps    = g2.number_input("BVPS",               min_value=0.0, step=0.01, value=float(_nz(_f(init.get("BVPS")), 0.0)))
    eps1y   = g3.number_input("EPS 1Y (est)",       min_value=0.0, step=0.01, value=float(_nz(_f(init.get("EPS 1Y")), 0.0)))
    epscg   = g4.number_input("EPS CAGR",           min_value=0.0, step=0.001, value=float(_nz(_f(init.get("EPS CAGR")), 0.0)))

    h1, h2, h3, h4 = st.columns(4)
    revcg   = h1.number_input("Rev CAGR",           min_value=0.0, step=0.001, value=float(_nz(_f(init.get("Rev CAGR")), 0.0)))
    dps     = h2.number_input("Årlig utdelning (DPS)", min_value=0.0, step=0.01, value=float(_nz(_f(init.get("Årlig utdelning")), 0.0)))
    dpscg   = h3.number_input("Utdelning CAGR",     min_value=0.0, step=0.001, value=float(_nz(_f(init.get("Utdelning CAGR")), 0.0)))
    prim    = h4.selectbox("Primär metod", ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"],
                           index=["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"].index(str(_nz(init.get("Primär metod"), "ev_ebitda"))))

    i1, i2 = st.columns(2)
    fetch_btn = i1.button("🔎 Hämta från Yahoo (förhandsvisa)")
    save_btn  = i2.button("💾 Spara till Data", type="primary")

    if fetch_btn and ticker:
        snap = fetch_yahoo_snapshot(ticker)
        st.success(
            f"Hämtat {ticker}: pris={_fmt_money(snap.get('price'), snap.get('currency'))}, "
            f"EV/Rev={_fmt_num(snap.get('ev_to_sales'))}, EV/EBITDA={_fmt_num(snap.get('ev_to_ebitda'))}, "
            f"P/B={_fmt_num(snap.get('p_to_book'))}, BVPS={_fmt_num(snap.get('bvps'))}, "
            f"EPS TTM={_fmt_num(snap.get('eps_ttm'))}, PE TTM/FWD={_fmt_num(snap.get('pe_ttm'))}/{_fmt_num(snap.get('pe_fwd'))}"
        )
        with st.expander("Förhandsvisa hämtat (fyll manuellt i fälten ovan de värden du vill spara)", expanded=True):
            st.write(pd.DataFrame([snap]))

    if save_btn and ticker:
        new_row = {
            "Timestamp": _now_str(),
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
        try:
            df2 = upsert_row(df, new_row, key="Ticker")
            save_data_df(df2)
            st.success(f"✅ Sparat '{ticker}' till {DATA_TITLE}.")
        except Exception as e:
            st.error(f"❌ Fel vid skrivning till '{DATA_TITLE}': {e}")

# ========== Analys ==========
def page_analysis():
    st.header("🔬 Analys")

    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()

    if df.empty:
        st.info("Data-bladet är tomt. Gå till **Lägg till / uppdatera** först.")
        return

    # Filter/val
    c1, c2 = st.columns(2)
    bucket_sel = c1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    tickers = sorted(df[df["Bucket"].isin(bucket_sel)]["Ticker"].dropna().astype(str).unique())
    if not tickers:
        st.info("Inga tickers i urvalet.")
        return
    tkr = c2.selectbox("Välj ticker", tickers)

    row = df[df["Ticker"].astype(str) == tkr].iloc[0]
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    # visa metodtabell
    st.caption(f"Sanity: {sanity}")
    st.dataframe(methods_df, use_container_width=True)

    # välj metod för visning (default = lagrad Primär metod eller heuristik)
    default_method, p0, p1, p2, p3 = _pick_primary_targets(methods_df)
    saved_primary = row.get("Primär metod") if row.get("Primär metod") in methods_df["Metod"].tolist() else None
    initial = saved_primary or default_method or methods_df["Metod"].iloc[0]
    sel = st.selectbox("Värderingssätt (vy)", methods_df["Metod"].tolist(), index=methods_df["Metod"].tolist().index(initial))
    tgts = _extract_targets_for(methods_df, sel)

    # knappar: spara primär metod + spara primär riktkurs
    b1, b2 = st.columns(2)
    if b1.button("💾 Spara vald metod som Primär", use_container_width=True):
        try:
            df2 = df.copy()
            idx = df2[df2["Ticker"].astype(str) == tkr].index[0]
            df2.loc[idx, "Primär metod"] = sel
            df2.loc[idx, "Senast beräknad"] = _now_str()
            save_data_df(df2)
            st.success(f"✅ Primär metod satt till '{sel}' för {tkr}.")
        except Exception as e:
            st.error(f"Kunde inte spara primär metod: {e}")

    if b2.button("🧷 Spara primär riktkurs till Resultat", use_container_width=True):
        try:
            _append_or_update_result(tkr, meta.get("currency") or row.get("Valuta") or "USD", sel, tgts["today"], tgts["y1"], tgts["y2"], tgts["y3"])
            st.success("✅ Sparat till fliken Resultat.")
        except Exception as e:
            st.error(f"Kunde inte spara riktkurs: {e}")

    # huvudkort – primär riktkurs & uppsidor
    st.markdown("### 🎯 Primär riktkurs")
    px = meta.get("price")
    ups = _compute_upsides(px, tgts)
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Idag", _fmt_money(tgts["today"], meta.get("currency")))
    cB.metric("1 år", _fmt_money(tgts["y1"], meta.get("currency")))
    cC.metric("2 år", _fmt_money(tgts["y2"], meta.get("currency")))
    cD.metric("3 år", _fmt_money(tgts["y3"], meta.get("currency")))
    u1, u2, u3, u4 = st.columns(4)
    u1.metric("Uppsida Idag", _fmt_pct(ups["today"]))
    u2.metric("Uppsida 1 år", _fmt_pct(ups["y1"]))
    u3.metric("Uppsida 2 år", _fmt_pct(ups["y2"]))
    u4.metric("Uppsida 3 år", _fmt_pct(ups["y3"]))
    st.caption(f"Metod: `{sel}` • {_method_inputs_blurb(sel)} • Valuta: **{meta.get('currency') or row.get('Valuta') or 'USD'}** • PE-ankare={_fmt_num(meta.get('pe_anchor'))} • Decay/år={_fmt_num(meta.get('decay'))}")

    st.divider()
    st.markdown("### 🧾 Bolagspresentation (hämtat vs manuellt vs beräknat)")
    _presentation_panels(row, sanity, meta, methods_df, sel)

# ───────────────────────────────────────────────────────────
# Del 4/4 — Snapshot, Ranking, Inställningar, Batch & main
# ───────────────────────────────────────────────────────────

# ========== Snapshot till fliken "Snapshot" ==========
def save_quarter_snapshot(ticker: str, methods_df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    """Appendar en rad per metod till fliken Snapshot."""
    try:
        snap = _read_df(SNAPSHOT_TITLE)
    except Exception:
        snap = pd.DataFrame()
    ts = now_stamp() if "now_stamp" in globals() else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    if snap.empty:
        out = pd.DataFrame(rows)
    else:
        # säkerställ kolumnuppsättning
        cols = list(set(snap.columns).union(set(pd.DataFrame(rows).columns)))
        snap = snap.reindex(columns=cols)
        out = pd.concat([snap, pd.DataFrame(rows)[cols]], ignore_index=True)
    _write_df(SNAPSHOT_TITLE, out)

# ========== Ranking ==========
def _pick_primary_for_row(methods_df: pd.DataFrame, saved_primary: Optional[str]) -> Tuple[str, Optional[float]]:
    """Välj metod att ranka på: lagrad 'Primär metod' om giltig, annars heuristik."""
    if methods_df is None or methods_df.empty:
        return "", None
    methods = methods_df["Metod"].tolist()
    method = None
    if saved_primary and saved_primary in methods:
        method = saved_primary
    else:
        # heuristik (samma som i Del 3)
        counts = methods_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
        prefer = ["ev_ebitda","ev_sales","pe_hist_vs_eps","p_b","ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"]
        cand = counts[counts == counts.max()].index.tolist()
        method = next((p for p in prefer if p in cand), cand[0])
    row = methods_df[methods_df["Metod"] == method].iloc[0]
    return method, _f(row["Idag"])

def page_ranking():
    st.header("🏁 Ranking – Uppsida mot primär fair value (Idag)")
    settings = get_settings_map()
    fx_map   = get_fx_map()
    df       = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    c1, c2, c3 = st.columns(3)
    buckets = c1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned   = c2.selectbox("Urval", ["Innehav (>0)", "Watchlist (=0)"], index=0)
    topn    = int(c3.number_input("Visa topp N", min_value=5, max_value=500, value=100, step=5))

    q = df.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned.startswith("Innehav"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    else:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    rows = []
    prog = st.progress(0.0)
    total = max(1, len(q))
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            method, fair_today = _pick_primary_for_row(met_df, r.get("Primär metod"))
            price = meta.get("price")
            currency = meta.get("currency") or str(r.get("Valuta") or "USD").upper()
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
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None,
                "Primär metod": None,
                "Fair value (Idag)": None,
                "Uppsida %": None,
            })
        time.sleep(0.15)
        prog.progress((i+1)/total)
    prog.empty()

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(by=["Uppsida %"], ascending=False, na_position="last").head(topn)
    st.dataframe(out, use_container_width=True)

# ========== Inställningar ==========
def page_settings():
    st.header("⚙️ Inställningar")
    settings = get_settings_map()
    fx_map   = get_fx_map()

    st.subheader("Källskatt per valuta")
    currencies = ["USD","EUR","NOK","CAD","SEK"]
    with st.form("wh_form"):
        cols = st.columns(len(currencies))
        vals = {}
        for i, ccy in enumerate(currencies):
            key = f"withholding_{ccy}"
            default = 0.0 if ccy == "SEK" else 0.15
            cur = float(settings.get(key, default))
            vals[ccy] = cols[i].number_input(
                f"{ccy}", min_value=0.0, max_value=1.0, step=0.01, value=cur, format="%.2f"
            )
        w_submit = st.form_submit_button("💾 Spara källskatt")
    if w_submit:
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        # upsert
        for ccy, v in vals.items():
            k = f"withholding_{ccy}"
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s.loc[len(s)] = [k, str(v)]
        _write_df(SETTINGS_TITLE, s)
        st.success("Källskatt uppdaterad.")

    st.subheader("Modellparametrar")
    p1, p2 = st.columns(2)
    pe_w  = p1.number_input("PE-ankare, vikt TTM (0..1)", min_value=0.0, max_value=1.0, step=0.05,
                            value=float(settings.get("pe_anchor_weight_ttm","0.5")))
    decay = p2.number_input("Multipel-decay per år (0..1)", min_value=0.0, max_value=1.0, step=0.01,
                            value=float(settings.get("multiple_decay","0.10")))
    if st.button("💾 Spara modellparametrar"):
        s = _read_df(SETTINGS_TITLE)
        if s.empty:
            s = pd.DataFrame(columns=SETTINGS_COLUMNS)
        def setv(k, v):
            if (s["Key"] == k).any():
                s.loc[s["Key"] == k, "Value"] = str(v)
            else:
                s.loc[len(s)] = [k, str(v)]
        setv("pe_anchor_weight_ttm", pe_w)
        setv("multiple_decay", decay)
        _write_df(SETTINGS_TITLE, s)
        st.success("Parametrar uppdaterade.")

    st.subheader("Valutakurser (SEK per 1)")
    if st.button("🔄 Hämta från Yahoo & skriv till 'Valutakurser'"):
        mp = _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")
        st.json(mp)

# ========== Batch ==========
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
            try:
                snap = fetch_yahoo_snapshot(tkr)
                # skriv över fält vi kan
                if snap.get("price") is not None:       df2.at[idx, "Aktuell kurs"]  = snap["price"]
                if snap.get("currency"):                df2.at[idx, "Valuta"]        = snap["currency"]
                if snap.get("revenue_ttm") is not None: df2.at[idx, "Rev TTM"]       = snap["revenue_ttm"]
                if snap.get("ebitda_ttm") is not None:  df2.at[idx, "EBITDA TTM"]    = snap["ebitda_ttm"]
                if snap.get("eps_ttm") is not None:     df2.at[idx, "EPS TTM"]       = snap["eps_ttm"]
                if snap.get("pe_ttm") is not None:      df2.at[idx, "PE TTM"]        = snap["pe_ttm"]
                if snap.get("pe_fwd") is not None:      df2.at[idx, "PE FWD"]        = snap["pe_fwd"]
                if snap.get("ev_to_sales") is not None: df2.at[idx, "EV/Revenue"]    = snap["ev_to_sales"]
                if snap.get("ev_to_ebitda") is not None:df2.at[idx, "EV/EBITDA"]     = snap["ev_to_ebitda"]
                if snap.get("p_to_book") is not None:   df2.at[idx, "P/B"]           = snap["p_to_book"]
                if snap.get("bvps") is not None:        df2.at[idx, "BVPS"]          = snap["bvps"]
                if snap.get("net_debt") is not None:    df2.at[idx, "Net debt"]      = snap["net_debt"]
                df2.at[idx, "Senast auto uppdaterad"] = now_stamp()
                df2.at[idx, "Auto källa"] = "Yahoo"
            except Exception:
                pass
            time.sleep(throttle)
            prog.progress((i+1)/len(df2))
        save_data_df(df2)
        prog.empty()
        st.success("Uppdaterat alla tickers från Yahoo.")

    if st.button("📷 Spara snapshots (alla)"):
        prog = st.progress(0.0)
        count = 0
        for i, (_, r) in enumerate(df.iterrows()):
            try:
                met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
                save_quarter_snapshot(str(r["Ticker"]).strip().upper(), met_df, meta)
                count += 1
            except Exception:
                pass
            time.sleep(throttle)
            prog.progress((i+1)/len(df))
        prog.empty()
        st.success(f"Snapshot sparade för {count} bolag.")

# ========== Router & MAIN ==========
def run_main_ui():
    st.title(APP_TITLE)

    with st.expander("📊 Status (FX & inställningar)", expanded=False):
        try:
            st.write("FX (SEK per 1):", get_fx_map())
        except Exception as e:
            st.warning(f"FX misslyckades: {e}")
        try:
            st.write("Settings:", get_settings_map())
        except Exception as e:
            st.warning(f"Settings misslyckades: {e}")

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
