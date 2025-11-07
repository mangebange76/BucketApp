# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 1/6: Bas & infrastruktur
#
#  - Streamlit setup
#  - Hjälpfunktioner (_f, _pos, etc)
#  - Google Sheets in/ut
#  - Kolumnschema (inkl tidsstämplar)
#  - FX-hantering (Valutakurser-bladet)
#  - Settings-hantering (Settings-bladet)
#
# All funktionalitet från tidigare version är kvar.
# Senare delar bygger vidare på detta utan att ta bort något.
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
    """
    För service account keys som lagts in i secrets med '\n' istället för riktiga radbrytningar.
    """
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def guard(fn, label: str = ""):
    """Wrapper för att visa fel i UI men ändå raisa (t.ex. schema-säkring i start)."""
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
    """
    Robust float-parser som accepterar "1 234,56" och returnerar None vid tomt/icke-siffra.
    Alltid float eller None.
    """
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
    """Returnera positiv float (>0) eller None."""
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(x, fallback=None):
    """Null coalesce: returnera x om det inte är NaN/None, annars fallback."""
    return x if (x is not None and x == x) else fallback

# =========================
# Google Sheets Auth
# =========================
@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    """
    Skapa gspread Client från GOOGLE_CREDENTIALS.
    Stödjer:
      - Mapping/dict
      - str (JSON)
      - bytes/bytearray
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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """
    Öppnar spreadsheet via SHEET_URL/SHEET_ID
    (stöder även GOOGLE_SHEET_URL / GOOGLE_SHEET_ID).
    """
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
    """
    Garanti: df innehåller alla kolumner i 'cols'.
    Saknade kolumner läggs till som NaN.
    Kolumnordning: först huvudschema, sen ev. extra kolumner.
    """
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
    """Appendar rader längst ned i arket utan att påverka befintliga celler."""
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    _with_backoff(ws.append_rows, rows, value_input_option="RAW")

# =========================
# Schema – kolumner
# =========================
# Viktigt: här definierar vi hela "Data"-bladets struktur.
# Dessa kolumner är vad resten av appen förväntar sig finnas.

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
    "Utdelningsfrekvens",                # "M","Q","S","A"
    "Nästa utdelningsdatum",             # YYYY-MM-DD (betalningsdatum)
    "Nästa utdelning (per aktie)",       # kommande DPS
    # Riktkurser/ranking
    "Primär metod",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    # Tidsstämplar/fältspårning
    "TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y",
    "Senast auto uppdaterad","Auto källa",
    # Manuell tillsyn-stämpel
    "Senast manuellt uppdaterad",
    # Extra hjälp-fält för editorns gamla vyer
    "EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad","Rev 2Y uppdaterad"
]

SETTINGS_COLUMNS = ["Key","Value"]
FX_COLUMNS       = ["Valuta","SEK_per_1"]

def _ensure_sheet_schema():
    """
    Säkerställ att alla ark (Data, Settings, Valutakurser, Snapshot)
    existerar och har rätt kolumner.
    """
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

    # --- Valutakurser ---
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
    """
    Hämtar SEK per 1 valutaenhet via yfinance.
    T.ex. USDSEK=X => hur många SEK kostar 1 USD.
    """
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
    """
    Kombinerar existerande kurser i arket med färska kurser,
    sparar tillbaka till Valutakurser-bladet,
    och returnerar den slutliga mappen { "USD": sek_per_usd, ... }.
    """
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
    """
    Returnerar uppdaterad valutakarta, tex {"USD": 10.9, "NOK": 1.02 ...}.
    SEK blir alltid 1.0.
    """
    mp = _load_fx_and_update_sheet()
    for c in ["SEK","USD","EUR","NOK","CAD"]:
        mp.setdefault(c, 1.0 if c=="SEK" else np.nan)
    return mp

# =========================
# Settings – läs/källskatt
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    """
    Läser in Settings-bladet och returnerar som dict {Key: Value}.
    """
    s = _read_df(SETTINGS_TITLE)
    out: Dict[str,str] = {}
    if not s.empty:
        for _, r in s.iterrows():
            k = str(r.get("Key"))
            v = "" if pd.isna(r.get("Value")) else str(r.get("Value"))
            out[k] = v
    return out

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
    """
    Hämtar källskatt (%) per valuta från Settings-bladet.
    default = 0.15 (15%) om inget hittas, enligt tidigare beteende.
    """
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
    """
    Läser Data-bladet, ser till att alla DATA_COLUMNS finns,
    typkonverterar numeriska kolumner, sätter 0→NaN på auto-fält osv.
    """
    df = _read_df(DATA_TITLE)
    df = _ensure_columns(df, DATA_COLUMNS)

    if df.empty:
        return df

    # Numeriska kolumner som ska bli float
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

    # Datumkolumn till date
    if "Nästa utdelningsdatum" in df.columns:
        df["Nästa utdelningsdatum"] = pd.to_datetime(
            df["Nästa utdelningsdatum"],
            errors="coerce"
        ).dt.date

    # Tidsstämplar som sträng
    for tcol in ["TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y","Senast auto uppdaterad","Senast manuellt uppdaterad"]:
        if tcol in df.columns:
            df[tcol] = df[tcol].astype(str)

    # Sätt 0→NaN på auto-hämtade fält (kurs, PE, multiplar, utdelning osv)
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

    # Antal aktier och GAV (SEK) kan vara 0, det är okej för bevakningslistor osv.
    return df

def write_data_df(df: pd.DataFrame):
    """
    Skriver tillbaka huvud-Data till Google Sheets.
    Bevarar DATA_COLUMNS först, sen ev. extra kolumner.
    """
    cols = [c for c in DATA_COLUMNS if c in df.columns] + \
           [c for c in df.columns if c not in DATA_COLUMNS]
    _write_df(DATA_TITLE, df[cols])

def append_result_row(row: Dict[str, Any]):
    """
    Lägger till en rad i Resultat-bladet (logg, historik etc).
    Skapar bladet om det inte finns.
    """
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

# ===== Säkerställa att metoderna finns (fallback om inte satt senare) =====
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b",
        "ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST

# ============================================================
# Del 1/6 slut — fortsätt i Del 2/6 (datainsamling & beräkningsmotor 1/2)
# ============================================================

# ============================================================
# app.py — Del 2/6 — Datainsamling & beräkningsmotor (1/2)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor som förstahandsval
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Multipel-decay, P/E-ankare, pris-/EV-byggare
#  • Auto-detekt för manuella Rev 1Y/2Y (miljoner vs enheter)
# ============================================================

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
def fetch_yahoo_snapshot(ticker: str) -> dict[str, Any]:
    """
    Hämtar pris/valuta + nyckeltal från Yahoo.
    Keys (urval):
      price, currency, market_cap, ev, shares,
      revenue_ttm, ebitda_ttm, eps_ttm,
      ev_to_sales, ev_to_ebitda, pe_ttm, pe_fwd,
      p_to_book, bvps, net_debt, company_name, sector, industry, sources={}
    """
    tk = yf.Ticker(ticker)
    out: dict[str, Any] = {"sources": {}}

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
# EPS/REV-paths – hjälpare (utan FX-konvertering av EPS)
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
# Del 2/6 slut — fortsätt i Del 3/6 (compute_methods_for_row m.m.)
# ============================================================

# ============================================================
# app.py — Del 3/6 — Beräkningsmotor (2/2)
#  • Framtidsbanor för EPS/Revenue
#  • Val av metod (P/E, EV/Sales, EV/EBITDA, P/B)
#  • Riktkurser: idag, 1y, 2y, 3y + bull/bear 1y
#  • Ingen FX-konvertering av EPS (respekterar manuellt valuta-val)
# ============================================================

def _fmt_pct(x: float | None) -> str:
    if x is None or not isinstance(x, (int, float)) or not math.isfinite(float(x)):
        return "–"
    return f"{float(x)*100:.1f}%"

def _brief(*parts: str) -> str:
    return " | ".join([p for p in parts if p])

def _clip_mult_from_current(curr_mult: float | None, lo: float, hi: float) -> float | None:
    m = _pos(curr_mult)
    if m is None:
        return None
    return max(lo, min(hi, float(m)))

def _safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    try:
        if float(b) == 0:
            return None
        return float(a) / float(b)
    except Exception:
        return None

def _yearly_decay_series(m0: float | None, years: list[int], decay: float, floor_frac: float) -> dict[int, float | None]:
    out = {}
    for y in years:
        out[y] = _decay_multiple(m0, y, decay, floor_frac)
    return out

# ---------------------------------------------
# Framtidsbanor (utan valutakonvertering av EPS)
# ---------------------------------------------
def _build_growth_paths(
    eps_ttm: float | None,
    eps_1y: float | None,
    eps_2y: float | None,
    eps_cagr_long: float | None,
    rev_ttm: float | None,
    rev_manual_ttm: float | None,
    rev_manual_1y: float | None,
    rev_manual_2y: float | None,
    rev_cagr_hist: float | None,
) -> dict[str, Any]:
    """
    Returnerar:
      eps_now, eps_y1, eps_y2, eps_y3
      rev_now, rev_y1, rev_y2, rev_y3
    Logik:
      • EPS: använd manuella EPS om finns (via eps_ttm), annars Yahoo.
             eps_y1/eps_y2: om finns från Yahoo-trend → använd; annars extrapolera via eps_cagr_long.
      • REV: prioritet: rev_manual_X om ifyllda (autosense miljoner/enheter mot hint),
             i övrigt rev_ttm från Yahoo och extrapolation via rev_cagr_hist.
    """
    # EPS-bana
    eps_now = _f(eps_ttm)
    y1 = _f(eps_1y)
    y2 = _f(eps_2y)
    eps_cagr = _f(eps_cagr_long)

    if y1 is None and eps_now is not None and eps_cagr is not None:
        y1 = eps_now * (1.0 + eps_cagr)
    if y2 is None and y1 is not None and eps_cagr is not None:
        y2 = y1 * (1.0 + eps_cagr)
    eps_y3 = None
    if y2 is not None and eps_cagr is not None:
        eps_y3 = y2 * (1.0 + eps_cagr)

    # Revenue-bana
    rev_hint = _f(rev_ttm)
    rev_now = _rev_manual_to_units_autosense(rev_manual_ttm, rev_hint) if rev_manual_ttm is not None else _f(rev_ttm)

    # Manuella framtidsår tolkas via autosense mot hint (rev_now om finns, annars rev_ttm)
    hint = rev_now if _pos(rev_now) else rev_hint
    rev_y1 = _rev_manual_to_units_autosense(rev_manual_1y, hint) if rev_manual_1y is not None else None
    rev_y2 = _rev_manual_to_units_autosense(rev_manual_2y, hint) if rev_manual_2y is not None else None

    # Om manuella saknas, extrapolera via historisk CAGR
    rc = _f(rev_cagr_hist)
    if rev_y1 is None and _pos(rev_now) and rc is not None:
        rev_y1 = rev_now * (1.0 + rc)
    if rev_y2 is None and _pos(rev_y1) and rc is not None:
        rev_y2 = rev_y1 * (1.0 + rc)
    rev_y3 = None
    if _pos(rev_y2) and rc is not None:
        rev_y3 = rev_y2 * (1.0 + rc)

    return {
        "eps_now": eps_now, "eps_y1": y1, "eps_y2": y2, "eps_y3": eps_y3,
        "rev_now": rev_now, "rev_y1": rev_y1, "rev_y2": rev_y2, "rev_y3": rev_y3,
        "eps_cagr": eps_cagr, "rev_cagr": rc
    }

# ---------------------------------------------
# Val av metod + riktkurser
# ---------------------------------------------
def compute_methods_for_row(
    ticker: str,
    snap: dict[str, Any],
    manual: dict[str, Any]
) -> dict[str, Any]:
    """
    Bestämmer värderingsmetod och beräknar riktkurser.
    Inputs:
      snap  — resultat från fetch_yahoo_snapshot(ticker)
      manual — ev. manuella overrides:
        {
          "eps_ttm_manual": float|None,
          "rev_ttm_manual": float|None,   # kan vara i miljoner → autosense
          "rev_1y_manual": float|None,
          "rev_2y_manual": float|None,
          "pe_override": float|None,
          "evs_override": float|None,
          "eve_override": float|None,     # EV/EBITDA
          "pb_override": float|None
        }
    Returnerar:
      {
        "currency": "...",
        "method": "pe|ev_sales|ev_ebitda|p_b",
        "target_today": float|None,
        "target_1y": float|None,
        "target_2y": float|None,
        "target_3y": float|None,
        "bull_1y": float|None,
        "bear_1y": float|None,
        "input_summary": str,
        "details": dict
      }
    Not: Ingen valutakonvertering av EPS — allt i instrumentets handelsvaluta.
    """

    cur = str(snap.get("currency") or "USD").upper()

    # ------- Plocka basdata -------
    price      = _f(snap.get("price"))
    shares     = _f(snap.get("shares"))
    net_debt   = _f(snap.get("net_debt"))
    ev_curr    = _f(snap.get("ev"))

    eps_ttm    = _f(manual.get("eps_ttm_manual")) if manual.get("eps_ttm_manual") not in (None, "") else _f(snap.get("eps_ttm"))
    pe_ttm     = _f(snap.get("pe_ttm"))
    pe_fwd     = _f(snap.get("pe_fwd"))

    rev_ttm    = _f(snap.get("revenue_ttm"))
    rev_m_ttm  = _f(manual.get("rev_ttm_manual"))
    rev_m_1y   = _f(manual.get("rev_1y_manual"))
    rev_m_2y   = _f(manual.get("rev_2y_manual"))

    ebitda_ttm = _f(snap.get("ebitda_ttm"))

    evs_now    = _f(snap.get("ev_to_sales"))
    eve_now    = _f(snap.get("ev_to_ebitda"))
    pb_now     = _f(snap.get("p_to_book"))
    bvps       = _f(snap.get("bvps"))

    # Overrides (om användaren anger egna multiplar)
    pe_override  = _f(manual.get("pe_override"))
    evs_override = _f(manual.get("evs_override"))
    eve_override = _f(manual.get("eve_override"))
    pb_override  = _f(manual.get("pb_override"))

    # CAGR-hjälp
    rev_cagr_hist = _clamp(snap.get("rev_cagr_hist"), REV_CAGR_MIN, REV_CAGR_MAX) if "rev_cagr_hist" in snap else None
    if rev_cagr_hist is None:
        try:
            rc = fetch_yahoo_rev_cagr(ticker)
            rev_cagr_hist = _clamp(rc.get("rev_cagr"), REV_CAGR_MIN, REV_CAGR_MAX)
        except Exception:
            rev_cagr_hist = None

    eps_trend = fetch_yahoo_eps_estimates(ticker)
    eps_1y = _f(eps_trend.get("eps_1y"))
    eps_2y = _f(eps_trend.get("eps_2y"))
    eps_cagr_long = _clamp(eps_trend.get("eps_cagr_long"), EPS_CAGR_MIN, EPS_CAGR_MAX)

    # Bygg tillväxtbanor
    paths = _build_growth_paths(
        eps_ttm=eps_ttm,
        eps_1y=eps_1y,
        eps_2y=eps_2y,
        eps_cagr_long=eps_cagr_long,
        rev_ttm=rev_ttm,
        rev_manual_ttm=rev_m_ttm,
        rev_manual_1y=rev_m_1y,
        rev_manual_2y=rev_m_2y,
        rev_cagr_hist=rev_cagr_hist
    )

    eps_now, eps_y1, eps_y2, eps_y3 = paths["eps_now"], paths["eps_y1"], paths["eps_y2"], paths["eps_y3"]
    rev_now, rev_y1, rev_y2, rev_y3 = paths["rev_now"], paths["rev_y1"], paths["rev_y2"], paths["rev_y3"]

    # P/E-ankare (om inte override)
    pe_anchor_now = _pos(pe_override) or _pe_anchor(pe_ttm, pe_fwd, w_ttm=0.60)
    # EV/S-ankare
    evs_anchor_now = _pos(evs_override) or _clip_mult_from_current(evs_now, lo=0.5, hi=25.0)
    # EV/EBITDA-ankare
    eve_anchor_now = _pos(eve_override) or _clip_mult_from_current(eve_now, lo=4.0, hi=35.0)
    # P/B-ankare
    pb_anchor_now  = _pos(pb_override)  or _clip_mult_from_current(pb_now,  lo=0.3, hi=8.0)

    # Decay-serier (multiplar tenderar mot historiskt band)
    pe_decay  = _yearly_decay_series(pe_anchor_now,  [1,2,3], decay=0.08, floor_frac=0.60)
    evs_decay = _yearly_decay_series(evs_anchor_now, [1,2,3], decay=0.10, floor_frac=0.55)
    eve_decay = _yearly_decay_series(eve_anchor_now, [1,2,3], decay=0.10, floor_frac=0.60)
    pb_decay  = _yearly_decay_series(pb_anchor_now,  [1,2,3], decay=0.06, floor_frac=0.65)

    # ---------------------------
    # Metodval (prioritetsordning)
    # ---------------------------
    method = None
    targets = {"today": None, 1: None, 2: None, 3: None}
    bull_1y = None
    bear_1y = None

    # 1) P/E om EPS>0 och vettig multipel
    if _pos(eps_now) and _pos(pe_anchor_now):
        method = "pe"
        targets["today"] = _price_from_pe(eps_now, pe_anchor_now)
        targets[1] = _price_from_pe(_nz(eps_y1, eps_now), pe_decay[1])
        targets[2] = _price_from_pe(_nz(eps_y2, _nz(eps_y1, eps_now)), pe_decay[2])
        targets[3] = _price_from_pe(_nz(eps_y3, _nz(eps_y2, _nz(eps_y1, eps_now))), pe_decay[3])

        # Bull/Bear via multipel± och EPS±10%
        if targets[1] is not None:
            bull_pe = (pe_decay[1] or pe_anchor_now) * 1.20 if (pe_decay[1] or pe_anchor_now) else None
            bear_pe = (pe_decay[1] or pe_anchor_now) * 0.80 if (pe_decay[1] or pe_anchor_now) else None
            bull_1y = _price_from_pe(_nz(eps_y1, eps_now) * 1.10 if _pos(_nz(eps_y1, eps_now)) else None, bull_pe)
            bear_1y = _price_from_pe(_nz(eps_y1, eps_now) * 0.90 if _pos(_nz(eps_y1, eps_now)) else None, bear_pe)

    # 2) EV/S om intäkter finns
    if method is None and _pos(rev_now) and (_pos(evs_anchor_now) or _pos(evs_now)):
        method = "ev_sales"
        m0 = _nz(evs_anchor_now, evs_now)
        targets["today"] = _equity_price_from_ev(_ev_from_sales(rev_now, m0), net_debt, shares)
        targets[1]       = _equity_price_from_ev(_ev_from_sales(_nz(rev_y1, rev_now), evs_decay[1]), net_debt, shares)
        targets[2]       = _equity_price_from_ev(_ev_from_sales(_nz(rev_y2, _nz(rev_y1, rev_now)), evs_decay[2]), net_debt, shares)
        targets[3]       = _equity_price_from_ev(_ev_from_sales(_nz(rev_y3, _nz(rev_y2, _nz(rev_y1, rev_now))), evs_decay[3]), net_debt, shares)

        if targets[1] is not None:
            bull_1y = _equity_price_from_ev(_ev_from_sales(_nz(rev_y1, rev_now) * 1.05 if _pos(_nz(rev_y1, rev_now)) else None,
                                                          (evs_decay[1] or m0) * 1.15 if (evs_decay[1] or m0) else None),
                                            net_debt, shares)
            bear_1y = _equity_price_from_ev(_ev_from_sales(_nz(rev_y1, rev_now) * 0.95 if _pos(_nz(rev_y1, rev_now)) else None,
                                                          (evs_decay[1] or m0) * 0.85 if (evs_decay[1] or m0) else None),
                                            net_debt, shares)

    # 3) EV/EBITDA om EBITDA finns
    if method is None and ebitda_ttm is not None and (_pos(eve_anchor_now) or _pos(eve_now)):
        method = "ev_ebitda"
        m0 = _nz(eve_anchor_now, eve_now)
        targets["today"] = _equity_price_from_ev(_ev_from_ebitda(ebitda_ttm, m0), net_debt, shares)
        # En enkel antagande: EBITDA växer i takt med revenue-cagr
        e_growth = paths["rev_cagr"] if paths["rev_cagr"] is not None else 0.0
        e1 = None if ebitda_ttm is None else ebitda_ttm * (1.0 + e_growth)
        e2 = None if e1 is None else e1 * (1.0 + e_growth)
        e3 = None if e2 is None else e2 * (1.0 + e_growth)
        targets[1] = _equity_price_from_ev(_ev_from_ebitda(e1, eve_decay[1]), net_debt, shares)
        targets[2] = _equity_price_from_ev(_ev_from_ebitda(e2, eve_decay[2]), net_debt, shares)
        targets[3] = _equity_price_from_ev(_ev_from_ebitda(e3, eve_decay[3]), net_debt, shares)

        if targets[1] is not None:
            bull_1y = _equity_price_from_ev(_ev_from_ebitda(None if e1 is None else e1 * 1.05,
                                                            (eve_decay[1] or m0) * 1.10 if (eve_decay[1] or m0) else None),
                                            net_debt, shares)
            bear_1y = _equity_price_from_ev(_ev_from_ebitda(None if e1 is None else e1 * 0.95,
                                                            (eve_decay[1] or m0) * 0.90 if (eve_decay[1] or m0) else None),
                                            net_debt, shares)

    # 4) P/B som sista fallback
    if method is None and _pos(pb_anchor_now) and _pos(bvps):
        method = "p_b"
        targets["today"] = _price_from_pb(pb_anchor_now, bvps)
        targets[1] = _price_from_pb(pb_decay[1], bvps)   # antar BVPS ~ konstant på 1y
        targets[2] = _price_from_pb(pb_decay[2], bvps)
        targets[3] = _price_from_pb(pb_decay[3], bvps)
        if targets[1] is not None:
            bull_1y = _price_from_pb(_nz(pb_decay[1], pb_anchor_now) * 1.15 if _pos(_nz(pb_decay[1], pb_anchor_now)) else None, bvps)
            bear_1y = _price_from_pb(_nz(pb_decay[1], pb_anchor_now) * 0.85 if _pos(_nz(pb_decay[1], pb_anchor_now)) else None, bvps)

    # Om ingen metod fungerar — lämna None
    if method is None:
        method = "none"

    # ---------------------------
    # Summering / detaljer
    # ---------------------------
    det = {
        "ticker": ticker,
        "currency": cur,
        "price_now": price,
        "eps_now": eps_now, "eps_1y": eps_y1, "eps_2y": eps_y2, "eps_3y": eps_y3,
        "rev_now": rev_now, "rev_1y": rev_y1, "rev_2y": rev_y2, "rev_3y": rev_y3,
        "rev_cagr_hist": paths["rev_cagr"], "eps_cagr_long": paths["eps_cagr"],
        "pe_anchor_now": pe_anchor_now, "evs_anchor_now": evs_anchor_now,
        "eve_anchor_now": eve_anchor_now, "pb_anchor_now": pb_anchor_now,
        "shares": shares, "net_debt": net_debt, "ev_curr": ev_curr,
    }

    inp = _brief(
        f"Metod: {method.upper()}",
        f"EPS TTM: {eps_now:.2f}" if _pos(eps_now) else "EPS TTM: –",
        f"Rev TTM: {rev_now/1e6:.1f} M" if _pos(rev_now) else "Rev TTM: –",
        f"Rev CAGR: {_fmt_pct(paths['rev_cagr'])}",
        f"EPS CAGR: {_fmt_pct(paths['eps_cagr'])}",
    )

    return {
        "currency": cur,
        "method": method,
        "target_today": _f(targets["today"]),
        "target_1y": _f(targets[1]),
        "target_2y": _f(targets[2]),
        "target_3y": _f(targets[3]),
        "bull_1y": _f(bull_1y),
        "bear_1y": _f(bear_1y),
        "input_summary": inp,
        "details": det,
    }

# ============================================================
# Del 3/6 slut — fortsätt i Del 4/6 (Sheets I/O + Editor-vy)
# ============================================================

# ============================================================
# app.py — Del 4/6 — Portfölj, P/L & utdelningar
#  • Portföljtabell (GAV i SEK, MV i SEK, P/L kr & %, Årlig utd. (SEK), /månad)
#  • Källskatt (grundtabell) — kan överskuggas i Settings (Del 5)
#  • Nästa utdelningsdatum (prognos, betalningsdatum – ej X-dag)
# ============================================================

# -------------------------
# Valuta & källskatt (grund)
# -------------------------
WITHHOLDING_BY_CCY = {
    "USD": 0.15,
    "CAD": 0.15,
    "NOK": 0.25,
    "EUR": 0.15,
    "SEK": 0.00,
}

def _fx_rate(fx_map: dict[str, float] | None, ccy: str, base: str = "SEK") -> float:
    """
    Växelkurs från fx_map (pris i 'base' per 1 enhet 'ccy').
    Om ej hittas: 1.0 för SEK, annars 0.0 markerar att kurs saknas.
    """
    if not ccy:
        return 0.0
    c = str(ccy).upper().strip()
    if c == base.upper():
        return 1.0
    if isinstance(fx_map, dict) and c in fx_map and _pos(fx_map[c]):
        return float(fx_map[c])
    return 0.0

def _withholding_for(ccy: str) -> float:
    """Grundtabell (kan ersättas/överskuggas av Settings i Del 5)."""
    return WITHHOLDING_BY_CCY.get(str(ccy).upper(), 0.0)

# -------------------------
# Utdelningshistorik & nästa datum (prognos)
# -------------------------
@st.cache_data(ttl=1200, show_spinner=False)
def _yf_dividends(ticker: str) -> pd.Series | None:
    try:
        tk = yf.Ticker(str(ticker))
        try:
            divs = tk.get_dividends()
        except Exception:
            divs = getattr(tk, "dividends", None)
        if divs is None or len(divs) == 0:
            return None
        s = pd.Series(divs).dropna()
        if s.empty:
            return None
        s.index = pd.to_datetime(s.index, errors="coerce")
        s = s.dropna()
        return s.sort_index()
    except Exception:
        return None

def _infer_next_dividend(ticker: str) -> tuple[pd.Timestamp | None, float | None, str]:
    """
    Returnerar (next_pay_date, per_share_amount, cadence_hint)
    • cadence_hint är 'M','Q','S','A' eller '?'.
    • Om ingen historik → (None, None, '?')
    """
    s = _yf_dividends(ticker)
    if s is None or s.empty:
        return None, None, "?"
    recent = s.copy()
    recent = recent[recent.index >= (pd.Timestamp.today() - pd.Timedelta(days=5*365))]
    if recent.empty:
        return None, None, "?"
    last_amt = float(recent.iloc[-1])
    last_dt  = pd.Timestamp(recent.index[-1])

    # Försök uppskatta betalningsintervall med median av senaste intervallen
    cadence_hint = "?"
    if len(recent) >= 4:
        diffs = np.diff(recent.index.values).astype("timedelta64[D]").astype(int)
        if len(diffs) > 0:
            med_days = int(np.median(diffs[-8:]))
            med_days = int(max(25, min(380, med_days)))
            if med_days <= 40:
                cadence_hint = "M"
            elif med_days <= 120:
                cadence_hint = "Q"
            elif med_days <= 220:
                cadence_hint = "S"
            else:
                cadence_hint = "A"
            nxt = last_dt + pd.Timedelta(days=med_days)
            today = pd.Timestamp.today().normalize()
            while nxt.normalize() <= today:
                nxt += pd.Timedelta(days=med_days)
            return nxt, last_amt, cadence_hint

    # Fallback-heuristik om median ej gick
    n = len(recent[recent.index >= (pd.Timestamp.today() - pd.Timedelta(days=370))])
    if n >= 10:
        cadence_hint = "M"; step = 30
    elif n >= 3:
        cadence_hint = "Q"; step = 90
    elif n == 2:
        cadence_hint = "S"; step = 182
    else:
        cadence_hint = "A"; step = 365
    nxt = last_dt + pd.Timedelta(days=step)
    today = pd.Timestamp.today().normalize()
    while nxt.normalize() <= today:
        nxt += pd.Timedelta(days=step)
    return nxt, last_amt, cadence_hint

# -------------------------
# Portföljtabell & summeringar
# -------------------------
def _ensure_price(row: pd.Series) -> float | None:
    """Pris-fallback: Data-bladets 'Aktuell kurs' annars Yahoo-snapshot."""
    p = _pos(row.get("Aktuell kurs"))
    if _pos(p):
        return float(p)
    tick = str(row.get("Ticker", "")).strip()
    snap = fetch_yahoo_snapshot(tick)
    return _pos(snap.get("price"))

def compute_portfolio_table(data_df: pd.DataFrame, fx_map: dict[str, float]) -> tuple[pd.DataFrame, dict[str, float]]:
    """
    Returnerar (tabell, totals) där tabell har:
      Ticker | Valuta | Antal | FX(→SEK) | Kurs | MV (SEK) | GAV (SEK) | AV (SEK) | P/L (SEK) | P/L (%) | Årlig utd (SEK) | /månad (SEK)
    Totals: {"tot_mv":..., "tot_cost":..., "tot_pl":..., "tot_pl_pct":..., "tot_div_y":..., "tot_div_m":...}
    """
    if data_df is None or data_df.empty:
        return pd.DataFrame(), {"tot_mv": 0.0, "tot_cost": 0.0, "tot_pl": 0.0, "tot_pl_pct": 0.0, "tot_div_y": 0.0, "tot_div_m": 0.0}

    rows = []
    tot_mv = tot_cost = tot_div_y = 0.0

    for _, r in data_df.iterrows():
        try:
            ticker = str(r.get("Ticker", "")).strip()
            if not ticker:
                continue
            qty = _pos(r.get("Antal aktier")) or 0.0
            if qty <= 0:
                continue

            ccy = str(r.get("Valuta", "USD")).upper()
            fx  = _fx_rate(fx_map, ccy, base="SEK")

            price = _ensure_price(r) or 0.0
            gav_sek = _pos(r.get("GAV (SEK)")) or 0.0  # alltid SEK

            mv_sek   = float(price) * float(qty) * float(fx)
            cost_sek = float(gav_sek) * float(qty)
            pl_sek   = mv_sek - cost_sek
            pl_pct   = (pl_sek / cost_sek * 100.0) if cost_sek > 0 else None

            # Årlig utdelning (netto, SEK)
            annual_ps = _pos(r.get("Årlig utdelning"))
            if not _pos(annual_ps):
                snap = fetch_yahoo_snapshot(ticker)
                annual_ps = _pos(snap.get("annual_dividend"))
            tax = _withholding_for(ccy)
            div_y_net_sek = 0.0
            if _pos(annual_ps) and fx > 0:
                div_y_net_sek = float(annual_ps) * float(qty) * (1.0 - float(tax)) * float(fx)
            div_m_net_sek = div_y_net_sek / 12.0

            rows.append({
                "Ticker": ticker,
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
                "Utd/mån (SEK)": div_m_net_sek,
            })

            tot_mv   += mv_sek
            tot_cost += cost_sek
            tot_div_y += div_y_net_sek
        except Exception:
            continue

    df = pd.DataFrame(rows, columns=[
        "Ticker","Valuta","Antal","FX (→SEK)","Kurs","MV (SEK)","GAV (SEK)","AV (SEK)",
        "P/L (SEK)","P/L (%)","Årlig utd (SEK)","Utd/mån (SEK)"
    ])

    tot_pl = tot_mv - tot_cost
    tot_pl_pct = (tot_pl / tot_cost * 100.0) if tot_cost > 0 else 0.0
    totals = {
        "tot_mv": tot_mv,
        "tot_cost": tot_cost,
        "tot_pl": tot_pl,
        "tot_pl_pct": tot_pl_pct,
        "tot_div_y": tot_div_y,
        "tot_div_m": tot_div_y / 12.0,
    }
    return df, totals

# -------------------------
# Nästa utbetalning — lista
# -------------------------
def build_next_dividends_list(data_df: pd.DataFrame, fx_map: dict[str, float]) -> pd.DataFrame:
    """
    Bygger en tabell över nästa utdelningsdatum (prognos), exkluderar passerade datum.
    Kolumner: Datum | Ticker | Valuta | Antal | Per aktie (valuta) | Källskatt | Netto (SEK)
    Sorterad på Datum stigande.
    """
    cols = ["Datum","Ticker","Valuta","Antal","Per aktie","Källskatt","Netto (SEK)"]
    if data_df is None or data_df.empty:
        return pd.DataFrame(columns=cols)

    out = []
    today = pd.Timestamp.today().normalize()

    for _, r in data_df.iterrows():
        try:
            ticker = str(r.get("Ticker", "")).strip()
            qty = _pos(r.get("Antal aktier")) or 0.0
            if not ticker or qty <= 0:
                continue
            ccy = str(r.get("Valuta", "USD")).upper()
            fx  = _fx_rate(fx_map, ccy, base="SEK")
            if fx <= 0:
                continue

            nxt_dt, last_amt, hint = _infer_next_dividend(ticker)
            if nxt_dt is None or last_amt is None:
                continue
            if nxt_dt.normalize() <= today:
                continue

            tax = _withholding_for(ccy)
            net_sek = float(last_amt) * float(qty) * (1.0 - float(tax)) * float(fx)
            out.append({
                "Datum": nxt_dt.date().isoformat(),
                "Ticker": ticker,
                "Valuta": ccy,
                "Antal": qty,
                "Per aktie": float(last_amt),
                "Källskatt": f"{int(tax*100)}%",
                "Netto (SEK)": net_sek,
            })
        except Exception:
            continue

    if not out:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(out, columns=cols)
    try:
        df["Datum"] = pd.to_datetime(df["Datum"], errors="coerce")
        df = df.dropna(subset=["Datum"]).sort_values("Datum", ascending=True)
        df["Datum"] = df["Datum"].dt.date.astype(str)
    except Exception:
        pass
    return df

# -------------------------
# Render: Portfölj-sektion
# -------------------------
def render_portfolio_view(data_df: pd.DataFrame, fx_map: dict[str, float]):
    st.subheader("📊 Portfölj (SEK-baserad vy)")

    tbl, totals = compute_portfolio_table(data_df, fx_map)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Totalt portföljvärde (SEK)", f"{totals['tot_mv']:,.0f}".replace(',', ' '))
    col2.metric("Anskaffningsvärde (SEK)", f"{totals['tot_cost']:,.0f}".replace(',', ' '))
    col3.metric("Orealiserad vinst (SEK)", f"{totals['tot_pl']:,.0f}".replace(',', ' '))
    col4.metric("Orealiserad vinst (%)", f"{totals['tot_pl_pct']:.2f}%")

    col5, col6 = st.columns(2)
    col5.metric("Årlig utdelning (SEK, netto)", f"{totals['tot_div_y']:,.0f}".replace(',', ' '))
    col6.metric("Utdelning per månad (SEK, netto)", f"{totals['tot_div_m']:,.0f}".replace(',', ' '))

    st.caption("Obs: GAV anges och behandlas i SEK. FX-kolumn visar växelkurs (SEK per 1 enhet bolagsvaluta).")

    if tbl.empty:
        st.info("Inga innehav med antal > 0 hittades.")
    else:
        st.dataframe(tbl, use_container_width=True)

    st.markdown("---")
    st.subheader("📅 Nästa utdelningar (prognos, **betalningsdatum**)")
    nd = build_next_dividends_list(data_df, fx_map)
    if nd.empty:
        st.info("Ingen prognos att visa. Antingen saknas utdelningshistorik eller innehav.")
    else:
        st.dataframe(nd, use_container_width=True)

# ============================================================
# app.py — Del 5/6 — Main & vyer
#  • Settings & Snapshot
#  • Editor (sök + bläddra) och Lägg till ticker
#  • Portfölj, Analys, Ranking, Batch
#  • Boot & main (entrypoint i Del 6/6)
# ============================================================

# ---------- Små nyttiga hjälpare för vyerna ----------
def _now():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

def _format_num(x, nd=2):
    v = _f(x)
    if v is None:
        return "—"
    return f"{v:.{nd}f}"

def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad",
            "Rev 2Y uppdaterad","Senast manuellt uppdaterad"]
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *cols])
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

# -------------------------------
# Gemensam sök + bläddrare
# -------------------------------
def _names_map_from_df(df: pd.DataFrame) -> dict[str, str]:
    mp: dict[str, str] = {}
    if df is None or df.empty:
        return mp
    if "Ticker" not in df.columns:
        return mp
    for _, r in df.iterrows():
        t = str(r.get("Ticker") or "").strip()
        if not t:
            continue
        n = str(r.get("Bolagsnamn") or "").strip()
        mp[t] = n
    return mp

def _select_with_search_nav(label: str,
                            options: list[str],
                            names_map: dict[str,str],
                            idx_key: str,
                            query_key: str) -> str | None:
    """
    Renderar:
      • Sökfält (matchar ticker eller bolagsnamn)
      • Föregående / Nästa-knappar (uppdaterar index i session state)
      • Selectbox utan egen key (index styrs av vår idx-state)
    Returnerar vald ticker (str) eller None.
    """
    ss = st.session_state
    if idx_key not in ss:
        ss[idx_key] = 0
    if query_key not in ss:
        ss[query_key] = ""

    q = st.text_input("Sök (ticker/bolagsnamn)", value=ss[query_key], key=query_key)

    def _match(t: str) -> bool:
        if not q:
            return True
        nm = names_map.get(t, "")
        ql = q.lower()
        return (ql in t.lower()) or (ql in nm.lower())

    filtered = [t for t in options if _match(t)]
    if not filtered:
        st.info("Inget matchande resultat – visar alla bolag.")
        filtered = options[:]

    col_prev, col_mid, col_next = st.columns([1, 6, 1])
    with col_prev:
        if st.button("◀︎", key=f"{idx_key}_prev") and filtered:
            ss[idx_key] = (ss[idx_key] - 1) % len(filtered)
            st.rerun()
    with col_next:
        if st.button("▶︎", key=f"{idx_key}_next") and filtered:
            ss[idx_key] = (ss[idx_key] + 1) % len(filtered)
            st.rerun()
    with col_mid:
        if filtered:
            cur_t = filtered[ss[idx_key] % len(filtered)]
            disp = names_map.get(cur_t, "")
            st.caption(f"Aktuell: **{cur_t}**" + (f" — {disp}" if disp else ""))

    idx = ss[idx_key] % len(filtered) if filtered else 0
    labels = [f"{t} — {names_map.get(t,'')}" if names_map.get(t) else t for t in filtered]
    sel_label = st.selectbox(label, labels, index=idx)
    sel_idx = labels.index(sel_label) if sel_label in labels else idx
    ss[idx_key] = sel_idx
    return filtered[sel_idx] if filtered else None

# ============================================================
# Settings-vy + valutakurser (överskugga källskatt)
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
        _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")

# Överskugga _withholding_for så portföljen använder Settings-bladet
def _withholding_for(ccy: str) -> float:
    s = get_settings_map()
    code = (ccy or "USD").upper()
    key  = f"withholding_{code}"
    try:
        return float(s.get(key, "0.0"))
    except Exception:
        return 0.0

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
# Editor (manuella fält) — sök + bläddra
#  (EPS lämnas i bolagets valuta – ingen automatiskt konvertering)
# ============================================================
def page_editor():
    st.header("Editor (manuella fält)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    need_cols = ["Ticker","Bucket","Antal aktier","GAV (SEK)","EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y","Senast manuellt uppdaterad"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = np.nan

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)

    sel = _select_with_search_nav("Välj rad (Ticker)", tickers, names_map, "editor_idx", "editor_q")
    if not sel:
        st.info("Välj ett bolag.")
        return

    ridx = df.index[df["Ticker"].astype(str) == sel]
    if len(ridx) == 0:
        st.error("Kunde inte hitta vald rad.")
        return
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
        # EPS: ingen konvertering – användare anger i bolagets valuta
        eps1_in = st.text_input("EPS 1Y (estimat)", value=str(_f(row.get("EPS 1Y")) or ""))
        eps2_in = st.text_input("EPS 2Y (estimat)", value=str(_f(row.get("EPS 2Y")) or ""))
        # Revenue: användarinput i MILJONER – vi skalar till enheter
        rev1_in = st.text_input("Rev 1Y (miljoner, 8.81B skrivs 8810)", value=str(_f(row.get("Rev 1Y")) or ""))
        rev2_in = st.text_input("Rev 2Y (miljoner)", value=str(_f(row.get("Rev 2Y")) or ""))

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara rad (session)"):
            try:
                antal_v = _parse_float(antal_in) or 0.0
                gav_v   = _parse_float(gav_in)
                eps1_v  = _parse_float(eps1_in)   # ingen valutakonvertering
                eps2_v  = _parse_float(eps2_in)   # ingen valutakonvertering
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
                df.loc[idx, "Senast manuellt uppdaterad"] = _now()

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
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    """Bygger uppdateringsdict från Yahoo (återanvänds i Editor/Batch)."""
    snap = fetch_yahoo_snapshot(ticker)
    est  = fetch_yahoo_eps_estimates(ticker)
    rc   = fetch_yahoo_rev_cagr(ticker)
    ec   = fetch_yahoo_eps_cagr_hist(ticker)

    updates = {
        "Timestamp": _now(),
        "Bolagsnamn": _maybe(snap.get("company_name")),
        "Sektor": _maybe(snap.get("sector")),
        "Aktuell kurs": _round2_or_none(snap.get("price")),
        "Valuta": (snap.get("currency") or existing_row.get("Valuta")),
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
        # EPS-estimat lämnas som Yahoo ger – ingen valutakonvertering här heller
        "EPS 1Y": _maybe(est.get("eps_1y")) if pd.isna(existing_row.get("EPS 1Y")) else existing_row.get("EPS 1Y"),
        "EPS 2Y": _maybe(est.get("eps_2y")) if pd.isna(existing_row.get("EPS 2Y")) else existing_row.get("EPS 2Y"),
        "Rev CAGR": _maybe(rc.get("rev_cagr")),
        "EPS CAGR": _maybe(ec.get("eps_cagr")),
        "Årlig utdelning": _maybe(snap.get("annual_dividend")),
        "Utdelningsfrekvens": _maybe(snap.get("dividend_frequency")),
        "Senast auto uppdaterad": _now(),
        "Auto källa": "Yahoo",
    }
    # Rensa tomma
    out = {}
    for k, v in updates.items():
        if v is None: 
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out

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
        eps1_in = st.text_input("EPS 1Y (estimat)", key="add_eps1")   # ingen konvertering
        rev1_in = st.text_input("Rev 1Y (miljoner, 8.81B skrivs 8810)", key="add_rev1")
    with c5:
        eps2_in = st.text_input("EPS 2Y (estimat)", key="add_eps2")   # ingen konvertering
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
                "Timestamp": _now(),
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

            eps1_v  = _parse_float(eps1_in)  # ingen konvertering
            eps2_v  = _parse_float(eps2_in)  # ingen konvertering
            rev1_vm = (_parse_float(rev1_in) or 0.0) * 1_000_000.0 if rev1_in not in (None,"") else None
            rev2_vm = (_parse_float(rev2_in) or 0.0) * 1_000_000.0 if rev2_in not in (None,"") else None
            if eps1_v is not None: new_row["EPS 1Y"] = eps1_v
            if eps2_v is not None: new_row["EPS 2Y"] = eps2_v
            if rev1_vm is not None: new_row["Rev 1Y"] = rev1_vm
            if rev2_vm is not None: new_row["Rev 2Y"] = rev2_vm
            new_row["Senast manuellt uppdaterad"] = _now()

            if do_prefill:
                updates = _build_updates_from_yahoo(tkr, pd.Series(new_row))
                new_row.update(updates)
                time.sleep(0.15)

            out_df = pd.concat([base_df, pd.DataFrame([new_row])], ignore_index=True)
            write_data_df(out_df)
            st.session_state["DATA"] = out_df
            st.success(f"{tkr} tillagd i DATA och sparad till Google Sheets.")
        except Exception as e:
            st.error(f"Kunde inte lägga till: {e}")

# ============================================================
# Portfölj (kopplar till Del 4)
# ============================================================
def page_portfolio():
    st.header("Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx = st.session_state.get("FX", {}) or {}
    try:
        render_portfolio_view(df, fx)
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")

# ============================================================
# Analys – huvudvy (sök + bläddra)
# ============================================================
def _pick_primary_method(row: pd.Series, methods_df: pd.DataFrame) -> str:
    existing = str(row.get("Primär metod") or "").strip()
    available = methods_df["Metod"].tolist()
    if existing and existing in available:
        r = methods_df[methods_df["Metod"] == existing]
        if not r.empty and r[["Idag","1 år","2 år","3 år"]].notna().any(axis=None):
            return existing
    for m in PREFER_ORDER:
        if m in available:
            r = methods_df[methods_df["Metod"] == m]
            if not r.empty and r[["Idag","1 år","2 år","3 år"]].notna().any(axis=None):
                return m
    return available[0] if available else "ev_sales"

def _targets_from_methods(methods_df: pd.DataFrame, method_name: str) -> dict[str, float | None]:
    r = methods_df[methods_df["Metod"] == method_name]
    if r.empty:
        return {"Idag": None, "1 år": None, "2 år": None, "3 år": None}
    r = r.iloc[0]
    return {"Idag": _f(r["Idag"]), "1 år": _f(r["1 år"]), "2 år": _f(r["2 år"]), "3 år": _f(r["3 år"])}

def page_analysis():
    st.header("🔬 Analys")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)

    tkr = _select_with_search_nav("Välj bolag", tickers, names_map, "analysis_idx", "analysis_q")
    if not tkr:
        st.info("Välj ett bolag.")
        return

    row = df.loc[df["Ticker"].astype(str) == tkr]
    if row.empty:
        st.error("Kunde inte hitta vald rad.")
        return
    row = row.iloc[0]

    settings = get_settings_map()
    fx_map   = get_fx_map()

    with st.spinner("Hämtar/beräknar…"):
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    st.caption(f"Sanity: {sanity}")
    st.dataframe(methods_df, use_container_width=True)

    default_m = _pick_primary_method(row, methods_df)
    method = st.selectbox("Primär metod för riktkurser", methods_df["Metod"].tolist(),
                          index=methods_df["Metod"].tolist().index(default_m) if default_m in methods_df["Metod"].tolist() else 0)

    targets = _targets_from_methods(methods_df, method)
    price   = _pos(_nz(meta.get("price"), row.get("Aktuell kurs")))

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Kurs", _format_num(price))
    c2.metric("Riktkurs idag", _format_num(targets["Idag"]))
    c3.metric("Riktkurs 1 år", _format_num(targets["1 år"]))
    c4.metric("Riktkurs 2 år", _format_num(targets["2 år"]))
    c5.metric("Riktkurs 3 år", _format_num(targets["3 år"]))

    horizon = st.selectbox("Uppsida vs", ["Idag","1 år","2 år","3 år"], index=1)
    tgt = _f(targets[horizon])
    up_pct = ((tgt - price) / price * 100.0) if (_pos(tgt) and _pos(price)) else None
    st.metric("Uppsida (%)", "—" if up_pct is None else f"{up_pct:.1f}%")

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara riktkurser till DATA"):
            try:
                idx = df.index[df["Ticker"].astype(str) == tkr][0]
                df.at[idx, "Primär metod"] = method
                df.at[idx, "Riktkurs idag"] = _f(targets["Idag"])
                df.at[idx, "Riktkurs 1 år"] = _f(targets["1 år"])
                df.at[idx, "Riktkurs 2 år"] = _f(targets["2 år"])
                df.at[idx, "Riktkurs 3 år"] = _f(targets["3 år"])
                if meta.get("currency"):
                    df.at[idx, "Valuta"] = str(meta["currency"]).upper()
                st.session_state["DATA"] = df
                st.success("Riktkurser uppdaterade i sessionens DATA.")
            except Exception as e:
                st.error(f"Kunde inte spara: {e}")
    with colB:
        if st.button("📸 Lägg snapshot (ark)"):
            try:
                _append_rows(SNAPSHOT_TITLE, [[
                    now_stamp(), tkr, meta.get("currency") or row.get("Valuta") or "USD",
                    method,
                    _f(targets["Idag"]), _f(targets["1 år"]), _f(targets["2 år"]), _f(targets["3 år"]),
                    _f(meta.get("pe_anchor")), _f(meta.get("decay"))
                ]])
                st.success("Snapshot tillagd.")
            except Exception as e:
                st.error(f"Kunde inte spara snapshot: {e}")

    st.markdown("---")
    st.subheader("Hela databasen (ofiltererad vy)")
    st.dataframe(st.session_state["DATA"], use_container_width=True)

# ============================================================
# Ranking – uppsida
# ============================================================
def _ensure_price_for_row(row: pd.Series) -> float | None:
    p = _pos(row.get("Aktuell kurs"))
    if _pos(p):
        return float(p)
    snap = fetch_yahoo_snapshot(str(row.get("Ticker")))
    return _pos(snap.get("price"))

def page_ranking():
    st.header("🏆 Ranking – Uppsida per horisont")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    only_owned = st.checkbox("Visa endast innehav (Antal aktier > 0)", value=False)
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)

    base = df.copy()
    if only_owned:
        base = base[(pd.to_numeric(base["Antal aktier"], errors="coerce") > 0)]

    rows = []
    settings = get_settings_map()
    fx_map   = get_fx_map()

    prog = st.progress(0.0)
    total = len(base)
    for i, (_, r) in enumerate(base.iterrows(), start=1):
        try:
            methods_df, sanity, meta = compute_methods_for_row(r, settings, fx_map)
            meth = _pick_primary_method(r, methods_df)
            tgts = _targets_from_methods(methods_df, meth)
            price = _ensure_price_for_row(r) or np.nan
            target = _f(tgts[horizon])
            up = ((target - price) / price * 100.0) if (_pos(target) and _pos(price)) else np.nan

            rows.append({
                "Ticker": str(r.get("Ticker")),
                "Valuta": str(_nz(meta.get("currency"), r.get("Valuta") or "USD")).upper(),
                "Kurs": price,
                f"Riktkurs {horizon}": target,
                "Uppsida (%)": up,
                "Metod": meth,
            })
        except Exception:
            pass
        prog.progress(i/total if total else 1.0)

    prog.empty()
    if not rows:
        st.info("Inget att visa.")
        return

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
        c3.metric("Uppsida (%)", "—" if pd.isna(item["Uppsida (%)"]) else f"{item['Uppsida (%)']:.1f}%")
        st.caption(f"Metod: {item['Metod']}  ·  Valuta: {item['Valuta']}")

# ============================================================
# Batch (Massuppdatering Yahoo) — sök + bläddra + toggle
# ============================================================
def _clean_non_empty(d: dict) -> dict:
    out = {}
    for k, v in (d or {}).items():
        if v is None: continue
        if isinstance(v, float) and pd.isna(v): continue
        if isinstance(v, str) and v.strip() == "":
            continue
        out[k] = v
    return out

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
                base.update({"Timestamp": _now(), "Ticker": tkr})
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
# Session-boot & Main
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
            st.session_state["FX"] = _load_fx_and_update_sheet()
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
# app.py — Del 6/6 — Entrypoint
# ============================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import streamlit as st, traceback
        st.error(f"💥 Fel i huvudloopen: {e}")
        st.code(traceback.format_exc())
