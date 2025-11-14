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
# Viktigt:
# • Denna fil skickas i 6 delar. Importen "from __future__ ..." får bara ligga här i Del 1.
# • Ingen valutakonvertering av EPS – manuella EPS-värden behandlas som redan i bolagets valuta.
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
    För service account keys som lagts in i secrets med '\\n' istället för riktiga radbrytningar.
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
# Schema – kolumner (Data/Settings/FX/Snapshot)
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
    # Extra hjälp-fält för editorns äldre vyer
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
            # === ADDED: Bucket-tak per innehav (SEK) ===
            ["bucket_cap_A_tillvaxt","20000"],
            ["bucket_cap_B_tillvaxt","10000"],
            ["bucket_cap_C_tillvaxt","6000"],
            ["bucket_cap_A_utdelning","10000"],
            ["bucket_cap_B_utdelning","7000"],
            ["bucket_cap_C_utdelning","4000"],
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
    default = 0.15 (15%) om inget hittas.
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

# ===== Metodlistor =====
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "pe_hist_vs_eps","ev_sales","ev_ebitda","ev_dacf","p_b",
        "p_nav","p_tbv","p_affo","p_fcf","ev_fcf","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS) med clamp
#  • Wrapper-funktioner som Editor förväntar sig
# ============================================================

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue/EBITDA TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Wrapper-funktioner som Editor/Batch förväntar sig
# ============================================================

# ---------- Importer från Del 1 ----------
# - yf (yfinance), np, pd, st
# - now_stamp(), _f(), _pos(), _nz()
# - read_data_df(), get_fx_map()

# ==============================
# Små hjälpfunktioner (Del 2)
# ==============================
def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    try:
        if a is None or b is None:
            return None
        if b == 0:
            return None
        v = float(a) / float(b)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None

def _cagr(first: Optional[float], last: Optional[float], years: int) -> Optional[float]:
    try:
        if first is None or last is None or years <= 0:
            return None
        if first <= 0 or last <= 0:
            return None
        g = (last / first) ** (1.0 / years) - 1.0
        if math.isfinite(g):
            return float(g)
        return None
    except Exception:
        return None

# ============================================================
# Yahoo – råhämtning
# ============================================================
@st.cache_data(ttl=900, show_spinner=False)
def yahoo_fast_info(ticker: str) -> Dict[str, Any]:
    """Hämtar snabbinfo (pris, valuta, mcap, shares) via yfinance.fast_info."""
    out: Dict[str, Any] = {}
    try:
        t = yf.Ticker(ticker)
        fi = getattr(t, "fast_info", None)
        if fi:
            # yfinance>=0.2.x: fast_info är ett objekt med attributes
            try:
                out["last_price"]   = _pos(getattr(fi, "last_price", None))
            except Exception:
                out["last_price"]   = None
            try:
                out["currency"]     = str(getattr(fi, "currency", "") or "")
            except Exception:
                out["currency"]     = ""
            try:
                out["market_cap"]   = _pos(getattr(fi, "market_cap", None))
            except Exception:
                out["market_cap"]   = None
            try:
                out["shares"]       = _pos(getattr(fi, "shares", None))
            except Exception:
                out["shares"]       = None
        # Fallback om fast_info saknar pris
        if not out.get("last_price"):
            hist = t.history(period="5d", auto_adjust=False)
            if isinstance(hist, pd.DataFrame) and not hist.empty:
                out["last_price"] = _pos(float(hist["Close"].dropna().iloc[-1]))
        return out
    except Exception:
        return out

@st.cache_data(ttl=1800, show_spinner=False)
def yahoo_quarterly_income(ticker: str) -> pd.DataFrame:
    """Kvartalsvis resultaträkning (income statement)."""
    try:
        t = yf.Ticker(ticker)
        # yfinance: .quarterly_financials eller .quarterly_income_stmt beroende på version
        df = getattr(t, "quarterly_financials", None)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            df = getattr(t, "quarterly_income_stmt", None)
        if isinstance(df, pd.DataFrame):
            return df.copy()
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=1800, show_spinner=False)
def yahoo_annual_income(ticker: str) -> pd.DataFrame:
    """Årlig resultaträkning för CAGR-beräkning."""
    try:
        t = yf.Ticker(ticker)
        df = getattr(t, "financials", None)
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            df = getattr(t, "income_stmt", None)
        if isinstance(df, pd.DataFrame):
            return df.copy()
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=1800, show_spinner=False)
def yahoo_dividends(ticker: str) -> pd.Series:
    """Utdelningsserie (betalningar)."""
    try:
        t = yf.Ticker(ticker)
        d = getattr(t, "dividends", None)
        if isinstance(d, pd.Series):
            return d.copy()
        return pd.Series(dtype=float)
    except Exception:
        return pd.Series(dtype=float)

# ============================================================
# TTM-beräkningar (Revenue, EBITDA, EPS) + utdelning
# ============================================================
def _ttm_from_quarters(qdf: pd.DataFrame, key_candidates: List[str]) -> Optional[float]:
    """Summera senaste 4 kvartal för angiven nyckel (t.ex. 'Total Revenue', 'Ebitda')."""
    if qdf is None or qdf.empty:
        return None
    # Säkerställ radindex som str (yfinance använder rader som poster)
    q = qdf.copy()
    q.index = [str(i) for i in q.index]
    # Hitta första matchande rad
    row_name = None
    for k in key_candidates:
        if k in q.index:
            row_name = k
            break
        # Prova case-insensitive
        for idx in q.index:
            if idx.lower() == k.lower():
                row_name = idx
                break
        if row_name:
            break
    if not row_name:
        return None
    # Ta de 4 senaste kolumnerna (senaste till vänster i yfinance)
    vals = q.loc[row_name]
    if not isinstance(vals, pd.Series):
        return None
    try:
        series = pd.to_numeric(vals, errors="coerce").dropna()
        if series.empty:
            return None
        # yfinance har nyaste längst till vänster → sortera efter indexordning om datumlike
        # enklast: ta de första fyra
        ttm = float(series.iloc[:4].sum())
        return ttm if math.isfinite(ttm) else None
    except Exception:
        return None

def compute_ttm_metrics(ticker: str) -> Dict[str, Optional[float]]:
    """Returnerar {'Rev TTM','EBITDA TTM','EPS TTM'}."""
    qdf = yahoo_quarterly_income(ticker)
    out = {"Rev TTM": None, "EBITDA TTM": None, "EPS TTM": None}
    if qdf is None or qdf.empty:
        return out
    # Revenue
    rev = _ttm_from_quarters(qdf, ["Total Revenue", "TotalRevenue", "Revenue", "Sales"])
    # EBITDA
    ebitda = _ttm_from_quarters(qdf, ["Ebitda", "EBITDA"])
    # EPS (Diluted först, annars Basic)
    eps_ttm = None
    eps_ttm = _ttm_from_quarters(qdf, ["Diluted EPS", "EPS Diluted", "EPS (Diluted)"])
    if eps_ttm is None:
        eps_ttm = _ttm_from_quarters(qdf, ["Basic EPS", "EPS", "EPS (Basic)"])
    # EPS TTM ska vara snitt per kvartal * 4, inte summa, om källan är per-aktie per kvartal.
    # I många YF-tabeller är EPS rad redan per kvartal; summan över 4 kvartal ≈ EPS TTM,
    # så behåll summan (standardpraxis).
    out["Rev TTM"] = _pos(rev)
    out["EBITDA TTM"] = _pos(ebitda)
    out["EPS TTM"] = _pos(eps_ttm)
    return out

def compute_dividend_info(ticker: str) -> Dict[str, Any]:
    """
    Returnerar:
      • 'Årlig utdelning' (summa 12m)
      • 'Utdelningsfrekvens' ['M','Q','S','A'] när det går att gissa
      • 'Nästa utdelningsdatum' (betalningsdatum) – ofta ej tillgängligt via YF → None
      • 'Nästa utdelning (per aktie)' – senaste betalningen som proxy om serie finns
    """
    s = yahoo_dividends(ticker)
    if s is None or s.empty:
        return {
            "Årlig utdelning": None,
            "Utdelningsfrekvens": None,
            "Nästa utdelningsdatum": None,
            "Nästa utdelning (per aktie)": None,
        }
    s = s.sort_index()
    # Summa 12 månader:
    try:
        last_date = s.index.max()
        window_start = last_date - pd.Timedelta(days=365)
        annual = float(s[s.index > window_start].sum())
        annual = annual if math.isfinite(annual) else None
    except Exception:
        annual = None

    # Gissa frekvens efter antal per år
    freq = None
    try:
        last_year = s[s.index > s.index.max() - pd.Timedelta(days=365)]
        n = len(last_year)
        if n >= 11:
            freq = "M"
        elif n >= 3:
            freq = "Q"
        elif n == 2:
            freq = "S"
        elif n == 1:
            freq = "A"
    except Exception:
        freq = None

    # Nästa betalningsdatum saknas ofta i YF; lämna None
    next_pay = None
    next_amt = None
    try:
        if not s.empty:
            next_amt = float(s.iloc[-1])
            if not math.isfinite(next_amt):
                next_amt = None
    except Exception:
        next_amt = None

    return {
        "Årlig utdelning": _pos(annual),
        "Utdelningsfrekvens": freq,
        "Nästa utdelningsdatum": None,
        "Nästa utdelning (per aktie)": _pos(next_amt),
    }

# ============================================================
# 5-års CAGR (Revenue & EPS) från årliga siffror
# ============================================================
def compute_cagr_5y(ticker: str) -> Dict[str, Optional[float]]:
    """
    Beräknar 5-års historisk CAGR för Revenue & EPS (om data finns).
    Använder årlig resultaträkning (YF financials/income_stmt).
    """
    df = yahoo_annual_income(ticker)
    out = {"Rev CAGR": None, "EPS CAGR": None}
    if df is None or df.empty:
        return out

    df = df.copy()
    df.index = [str(i) for i in df.index]

    # Revenue
    rev_row = None
    for k in ["Total Revenue", "TotalRevenue", "Revenue", "Sales"]:
        if k in df.index:
            rev_row = df.loc[k]
            break
        for idx in df.index:
            if idx.lower() == k.lower():
                rev_row = df.loc[idx]
                break
        if rev_row is not None:
            break

    # EPS (Diluted/Basic)
    eps_row = None
    for k in ["Diluted EPS", "EPS Diluted", "EPS (Diluted)", "Basic EPS", "EPS", "EPS (Basic)"]:
        if k in df.index:
            eps_row = df.loc[k]
            break
        for idx in df.index:
            if idx.lower() == k.lower():
                eps_row = df.loc[idx]
                break
        if eps_row is not None:
            break

    def _series_cagr(sr: Optional[pd.Series]) -> Optional[float]:
        if sr is None or not isinstance(sr, pd.Series):
            return None
        ser = pd.to_numeric(sr, errors="coerce").dropna()
        if ser.shape[0] < 2:
            return None
        # Ta senaste och ca 5 år bakåt (så gott det går)
        last = float(ser.iloc[0])
        # Hitta punkt ~5 entries bort om finns, annars sista
        if ser.shape[0] >= 6:
            first = float(ser.iloc[5])
            years = 5
        else:
            first = float(ser.iloc[-1])
            years = max(1, ser.shape[0]-1)
        return _cagr(first, last, years)

    out["Rev CAGR"] = _series_cagr(rev_row)
    out["EPS CAGR"] = _series_cagr(eps_row)
    return out

# ============================================================
# Samlad Yahoo-hämtning + härledda multiplar
# ============================================================
def yahoo_collect_for_ticker(ticker: str, net_debt_hint: Optional[float] = None) -> Dict[str, Any]:
    """
    Returnerar en dict med fält som matchar DATA_COLUMNS där det är rimligt att fylla från Yahoo:
      • Aktuell kurs, Valuta, Utestående aktier
      • Rev TTM, EBITDA TTM, EPS TTM
      • PE TTM (pris / EPS TTM om > 0)
      • Årlig utdelning, Utdelningsfrekvens, Nästa utdelning (per aktie)
      • Rev CAGR, EPS CAGR (5-år)
      • EV/Revenue, EV/EBITDA (om market_cap/net_debt finns)
    """
    tkr = str(ticker).strip()
    fast = yahoo_fast_info(tkr)
    ttm  = compute_ttm_metrics(tkr)
    cagr = compute_cagr_5y(tkr)
    divs = compute_dividend_info(tkr)

    price   = _pos(fast.get("last_price"))
    curr    = (fast.get("currency") or "").upper()
    shares  = _pos(fast.get("shares"))
    mcap    = _pos(fast.get("market_cap"))

    eps_ttm   = _pos(ttm.get("EPS TTM"))
    pe_ttm    = _safe_div(price, eps_ttm) if (price and eps_ttm and eps_ttm > 0) else None

    # EV (om möjligt)
    net_debt = _pos(net_debt_hint)
    ev = None
    if mcap is not None and net_debt is not None:
        ev = mcap + net_debt
    elif mcap is not None:
        ev = mcap  # fallback om net_debt saknas

    ev_rev   = _safe_div(ev, _pos(ttm.get("Rev TTM"))) if ev is not None else None
    ev_ebitda= _safe_div(ev, _pos(ttm.get("EBITDA TTM"))) if ev is not None else None

    row = {
        "Aktuell kurs": price,
        "Valuta": curr or None,
        "Utestående aktier": shares,
        "Rev TTM": _pos(ttm.get("Rev TTM")),
        "EBITDA TTM": _pos(ttm.get("EBITDA TTM")),
        "EPS TTM": eps_ttm,
        "PE TTM": pe_ttm,
        "Årlig utdelning": _pos(divs.get("Årlig utdelning")),
        "Utdelningsfrekvens": divs.get("Utdelningsfrekvens"),
        "Nästa utdelningsdatum": divs.get("Nästa utdelningsdatum"),
        "Nästa utdelning (per aktie)": _pos(divs.get("Nästa utdelning (per aktie)")),
        "Rev CAGR": _pct(cagr.get("Rev CAGR")),
        "EPS CAGR": _pct(cagr.get("EPS CAGR")),
        "EV/Revenue": _pos(ev_rev),
        "EV/EBITDA": _pos(ev_ebitda),
        # Markera källa/tid först när skriv sker i senare delar
    }
    return row

# ============================================================
# Wrapper för Editor/Batch: uppdatera specifika fält från Yahoo
# ============================================================
def fetch_from_yahoo_for_row(row: pd.Series) -> Dict[str, Any]:
    """
    Tar en rad (pd.Series) från Data-bladet och returnerar en dict med fält att uppdatera.
    Skrivning till Sheets sker i senare delar, inte här.
    """
    ticker = str(row.get("Ticker", "") or "").strip()
    if not ticker:
        return {}

    net_debt_hint = None
    try:
        nd = row.get("Net debt")
        net_debt_hint = _pos(nd)
    except Exception:
        net_debt_hint = None

    values = yahoo_collect_for_ticker(ticker, net_debt_hint=net_debt_hint)
    return {k: v for k, v in values.items() if v is not None}

# ============================================================
# Slut Del 2/6
# Nästa: Del 3/6 — Fair value & riktkursmetoder (beräkningsmotor, utan UI)
# ============================================================

# ============================================================
# Del 3/6 — Beräkningsmotor
#  • Kompatibel wrapper: fetch_from_yahoo() (bygger på Del 2)
#  • EPS-estimat från Yahoo (earnings_trend)
#  • Metodpriser: PE, EV/S, EV/EBITDA, DACF, P/B (+ placeholders)
#  • Multipel-decay & PE-ankare
#  • ✅ Fair Value (median över metodfamiljer, filtrerar kurs-kopia)
#  • compute_methods_for_row() → används av Analys/Ranking
# ============================================================

# -------------------------
# Kompatibel wrapper (Del 2 → Del 3)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Kompatibilitetslager som samlar ihop nycklar som resten av appen förväntar sig.
    Bygger på Del 2-funktionerna (yahoo_fast_info, compute_ttm_metrics, compute_cagr_5y, compute_dividend_info).
    """
    tkr = str(ticker).strip()
    fast = yahoo_fast_info(tkr)
    ttm  = compute_ttm_metrics(tkr)
    cagr = compute_cagr_5y(tkr)
    divs = compute_dividend_info(tkr)

    price   = _pos(fast.get("last_price"))
    currency= (fast.get("currency") or "USD")
    shares  = _pos(fast.get("shares"))
    # Forward PE & P/B & BVPS (försök via info – kan saknas)
    pe_fwd = None
    p_b    = None
    bvps   = None
    try:
        info = yf.Ticker(tkr).get_info() if hasattr(yf.Ticker(tkr), "get_info") else yf.Ticker(tkr).info
        pe_fwd = _f(info.get("forwardPE"))
        # yfinance brukar exponera priceToBook och bookValue (per aktie)
        p_b  = _f(info.get("priceToBook"))
        bvps = _f(info.get("bookValue"))  # per share
    except Exception:
        pass

    eps_ttm   = _pos(ttm.get("EPS TTM"))
    pe_ttm    = (price / eps_ttm) if (_pos(price) and _pos(eps_ttm) and eps_ttm > 0) else None

    # EV-multiplar från nuvarande EV (om möjligt); annars None
    ev_rev = None
    ev_ebitda = None
    try:
        mcap = _pos(fast.get("market_cap"))
        # Net debt saknas i Del 2 → antag 0 om ej ges (konservativt här)
        net_debt = 0.0
        ev = mcap + net_debt if mcap is not None else None
        if ev is not None:
            if _pos(ttm.get("Rev TTM")):
                ev_rev = ev / float(ttm["Rev TTM"])
            if _pos(ttm.get("EBITDA TTM")):
                e = float(ttm["EBITDA TTM"])
                ev_ebitda = (ev / e) if e != 0 else None
    except Exception:
        pass

    return {
        "price":            price,
        "currency":         currency,
        "shares_out":       shares,
        "net_debt":         None,  # ok att vara None (dras som 0 i equity-omräkningen)
        "rev_ttm":          _f(ttm.get("Rev TTM")),
        "ebitda_ttm":       _f(ttm.get("EBITDA TTM")),
        "eps_ttm":          _f(ttm.get("EPS TTM")),
        "pe_ttm":           _f(pe_ttm),
        "pe_fwd":           _f(pe_fwd),
        "ev_rev":           _f(ev_rev),
        "ev_ebitda":        _f(ev_ebitda),
        "p_b":              _f(p_b),
        "bvps":             _f(bvps),
        "dps_annual":       _f(divs.get("Årlig utdelning")),
        "rev_cagr_hist":    _f(cagr.get("Rev CAGR")),
        "eps_cagr_hist":    _f(cagr.get("EPS CAGR")),
    }

# ----- Clamp-gränser (i linje med din praxis) -----
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

# -------------------------
# Små helpers beräkning
# -------------------------
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
    try:
        return float(w_ttm) * pt + (1.0 - float(w_ttm)) * pf
    except Exception:
        return None

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
    m = _pos(mult)
    if ebitda is None or m is None:
        return None
    return float(ebitda) * m

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb)
    b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b

# -------------------------
# EPS/REV paths
# -------------------------
def _eps_path_fill(eps_ttm: Optional[float], eps_1y: Optional[float], eps_2y: Optional[float],
                   eps_cagr_hist: Optional[float], eps_cagr_long: Optional[float],
                   rev_cagr_hist: Optional[float]) -> Tuple[float, float, float, float]:
    e0 = _pos(eps_ttm) or 0.0
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)

    # välj första tillgängliga tillväxtindikator
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

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    b0 = _f(ebitda_ttm)  # kan vara negativ/0
    if b0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return b0, b0, b0, b0
    def scale(r):
        try:
            return (b0 * (r / rev0)) if (r and rev0) else b0
        except Exception:
            return b0
    return b0, scale(rev1), scale(rev2), scale(rev3)

# -------------------------
# EPS-estimat från Yahoo (earnings_trend)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    try:
        tk = yf.Ticker(ticker)
        try:
            trend = tk.get_earnings_trend()
        except Exception:
            trend = getattr(tk, "earnings_trend", None)
        if trend is None or (hasattr(trend, "empty") and trend.empty):
            return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}

        df = trend.copy()
        df.columns = [str(c).lower() for c in df.columns]

        def _avg(val):
            if isinstance(val, dict):
                for k in ("avg", "average", "mean"):
                    if k in val and _f(val[k]) is not None:
                        return _f(val[k])
            return _f(val)

        def _pick(period_aliases: List[str]):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for a in period_aliases:
                cur = m.str.contains(rf"^{a}$")
                mask = cur if mask is None else (mask | cur)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_next = _pick(["nextyear", "next fiscal year", "nextfiscalyear"])
        row_curr = _pick(["currentyear", "current fiscal year", "currentfiscalyear"])
        row_long = _pick(["longterm", "next5years", "next 5 years"])

        eps_1y = None
        if row_next is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg(row_next.get(col))
                    if eps_1y is not None:
                        break
        if eps_1y is None and row_curr is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg(row_curr.get(col))
                    if eps_1y is not None:
                        break

        eps_cagr_long = None
        if row_long is not None:
            for col in ["growth", "longtermgrowthrate"]:
                if col in df.columns and _f(row_long.get(col)) is not None:
                    eps_cagr_long = float(_f(row_long.get(col)))
                    break

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {"eps_1y": _f(eps_1y), "eps_2y": _f(eps_2y), "eps_cagr_long": _f(eps_cagr_long)}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}

# -------------------------
# Fair Value (familjemedian + kurs-kopiafilter)
# -------------------------
def _compute_fair_value_row_v2(methods_df: pd.DataFrame, now_price: Optional[float]) -> Dict[str, Any]:
    """
    Median över *oberoende metodfamiljer*:
      • 'pe_hist_vs_eps'  → fam 'pe'
      • 'ev_sales'        → fam 'ev_s'
      • 'ev_ebitda','ev_dacf' → fam 'ev_e' (räknas EN gång)
      • 'p_b'             → fam 'pb'
    Regler:
      • Dubbletter inom samma familj ignoreras.
      • 'Idag': filtrera bort värden som ≈ aktuell kurs (±0.5%) för att undvika tautologier.
        Fall-back till 'pe_hist_vs_eps' om allt försvinner.
    """
    fam_map = {
        "pe_hist_vs_eps": "pe",
        "ev_sales": "ev_s",
        "ev_ebitda": "ev_e",
        "ev_dacf": "ev_e",
        "p_b": "pb",
    }
    cols = ["Idag", "1 år", "2 år", "3 år"]
    out = {"Metod": "fair_value"}

    for c in cols:
        vals = []
        used_fams: set[str] = set()
        for _, r in methods_df.iterrows():
            m = str(r.get("Metod") or "")
            if m == "fair_value":
                continue
            v = _f(r.get(c))
            if v is None:
                continue
            fam = fam_map.get(m, m)
            if fam in used_fams:
                continue
            # Filtrera kurs-kopior i "Idag"
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:  # ±0.5 %
                    continue
            used_fams.add(fam)
            vals.append(float(v))

        if not vals:
            # Fall-back: ta PE-raden om den finns, annars NaN
            try:
                row_pe = methods_df[methods_df["Metod"] == "pe_hist_vs_eps"].iloc[0]
                out[c] = _f(row_pe.get(c))
            except Exception:
                out[c] = np.nan
        else:
            out[c] = float(np.median(vals))
    return out

# -------------------------
# Huvud: compute_methods_for_row
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Returnerar:
      • methods_df: DataFrame [Metod, Idag, 1 år, 2 år, 3 år]
      • sanity    : text
      • meta      : hjälpfält + fair_value (v2)
    Alla target i bolagets egen handelsvaluta.
    """
    ticker = str(row.get("Ticker", "")).strip()
    y = fetch_from_yahoo(ticker)  # wrapper ovan
    est = _fetch_eps_estimates_yahoo(ticker)

    # --- Inputs (med fallback från Data-bladet) ---
    price    = _pos(_nz(y.get("price"), row.get("Aktuell kurs")))
    currency = str(_nz(y.get("currency"), row.get("Valuta") or "USD")).upper()
    shares   = _pos(_nz(y.get("shares_out"), row.get("Utestående aktier")))
    net_debt = _nz(y.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _nz(y.get("rev_ttm"), row.get("Rev TTM"))
    ebitda_ttm = _nz(y.get("ebitda_ttm"), row.get("EBITDA TTM"))
    eps_ttm    = _nz(y.get("eps_ttm"), row.get("EPS TTM"))

    pe_ttm     = _pos(_nz(y.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(y.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(y.get("ev_rev"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(y.get("ev_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(y.get("p_b"), row.get("P/B")))
    bvps       = _pos(_nz(y.get("bvps"), row.get("BVPS")))

    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), est.get("eps_1y")))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), est.get("eps_2y")))

    # Historisk CAGR (clamp)
    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), y.get("rev_cagr_hist")))
    rev_cagr_hist     = max(REV_CAGR_MIN, min(REV_CAGR_MAX, rev_cagr_hist_raw)) if rev_cagr_hist_raw is not None else None

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), y.get("eps_cagr_hist")))
    eps_cagr_hist     = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_hist_raw)) if eps_cagr_hist_raw is not None else None

    eps_cagr_long = _f(est.get("eps_cagr_long"))
    if eps_cagr_long is not None:
        eps_cagr_long = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_long))

    # P/E-ankare + decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # Revenue-path (om bara TTM → väx med rev_cagr_hist)
    r0 = _pos(rev_ttm)
    if r0 is None:
        r1 = r2 = r3 = None
    else:
        g = float(_nz(rev_cagr_hist, 0.0))
        r1 = r0 * (1.0 + g)
        r2 = r1 * (1.0 + g)
        r3 = r2 * (1.0 + g)

    # EPS-path
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr_hist, eps_cagr_long, rev_cagr_hist)

    # EBITDA-path (skala med intäkter)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales,  _decay_multiple(ev_sales,  1, decay), _decay_multiple(ev_sales,  2, decay), _decay_multiple(ev_sales,  3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,       _decay_multiple(p_b,       1, decay), _decay_multiple(p_b,       2, decay), _decay_multiple(p_b,       3, decay)

    # --- Priser per metod (alla i bolagets valuta) ---
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
    # DACF-proxy (identisk med EV/EBITDA tills separat cash flow-källa kopplas på)
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
    # Platshållare (struktur bevaras)
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # --- ✅ Fair Value (ny korrigerad) ---
    fv_row = _compute_fair_value_row_v2(methods_df, price)
    methods_df = pd.concat([pd.DataFrame([fv_row]), methods_df], ignore_index=True)

    # --- Sanity-text ---
    sanity = (
        f"price={'ok' if price else '—'}, "
        f"eps_ttm={'ok' if (eps_ttm or eps_ttm==0) else '—'}, "
        f"eps_1y={'ok' if eps_1y_est else '—'}, "
        f"eps_2y={'ok' if eps_2y_est else '—'}, "
        f"rev_ttm={'ok' if rev_ttm else '—'}, "
        f"rev_cagr_hist={'ok' if _f(rev_cagr_hist) is not None else '—'}(clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
        f"eps_cagr_hist={'ok' if _f(eps_cagr_hist) is not None else '—'}(clamp={EPS_CAGR_MIN*100:.0f}%..{EPS_CAGR_MAX*100:.0f}%), "
        f"ebitda_ttm={'ok' if (ebitda_ttm or ebitda_ttm==0) else '—'}, "
        f"shares={'ok' if shares else '—'}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "fair_value": {
            "today": _f(fv_row.get("Idag")),
            "y1": _f(fv_row.get("1 år")),
            "y2": _f(fv_row.get("2 år")),
            "y3": _f(fv_row.get("3 år")),
        },
        "eps_path": {"ttm": _f(eps_ttm), "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": _f(rev_ttm), "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
        "cagr_clamped": {
            "rev_cagr_raw": _f(rev_cagr_hist_raw),
            "rev_cagr_used": _f(rev_cagr_hist),
            "eps_cagr_raw": _f(eps_cagr_hist_raw),
            "eps_cagr_used": _f(eps_cagr_hist),
        },
    }
    return methods_df, sanity, meta

# ============================================================
# (Slut Del 3/6)
# Nästa del (Del 4/6) — Portfölj & utdelningar + Bucket-tak
# ============================================================

# ============================================================
# Del 4/6 — Portfölj & utdelningar + Bucket-tak
#  • FX → SEK-omräkning
#  • Bucket-tak (A/B/C, tillväxt/utdelning) och utrymme per innehav
#  • Portföljsammanställning (värde i SEK)
#  • Utdelningsplan: nästa utbetalningsdatum (estimering), källskatt per valuta,
#    nettobelopp per innehav och summeringar i SEK
#  • Render-funktion för Portfölj-vyn (anropas i Del 6)
# ============================================================

# -------------------------
# Hjälpare (FX & parsing)
# -------------------------
def _fx_rate_to_sek(fx_map: Dict[str, float], ccy: str) -> float:
    """
    Returnerar SEK per 1 enhet 'ccy'. Antas vara lagrat i Valutakurser-bladet.
    Fall-back: 1.0 för SEK, annars 0 om okänd (hanteras uppströms).
    """
    if not ccy:
        return 0.0
    c = str(ccy).upper().strip()
    if c == "SEK":
        return 1.0
    r = _f((fx_map or {}).get(c))
    return float(r) if r is not None else 0.0

def _amount_sek(amount: Optional[float], ccy: str, fx_map: Dict[str, float]) -> Optional[float]:
    a = _f(amount)
    if a is None:
        return None
    rate = _fx_rate_to_sek(fx_map, ccy)
    if rate <= 0:
        return None
    return float(a) * rate

# -------------------------
# Bucket-tak (SEK)
# -------------------------
DEFAULT_BUCKET_CAPS_SEK = {
    "A TILLVÄXT":   20000.0,
    "A UTDELNING":  10000.0,
    "B TILLVÄXT":   10000.0,
    "B UTDELNING":   7000.0,
    "C TILLVÄXT":    6000.0,
    "C UTDELNING":   4000.0,
}

def _normalize_bucket_name(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    # Vanliga varianter: "A", "A tillväxt", "A - tillväxt", "A (tillväxt)" etc.
    s = s.replace("(", " ").replace(")", " ").replace("-", " ").replace("_", " ")
    s = " ".join(s.split())
    return s

def load_bucket_caps_from_settings(settings: Dict[str, Any]) -> Dict[str, float]:
    """
    Hämtar tak från Settings-bladet om de finns, annars default enligt Bucket-metoden.
    Nycklar som stöds (valfria):
      bucket_cap_A_tillväxt, bucket_cap_A_utdelning, bucket_cap_B_tillväxt, ...
    """
    caps = DEFAULT_BUCKET_CAPS_SEK.copy()
    key_map = {
        "bucket_cap_A_tillväxt": "A TILLVÄXT",
        "bucket_cap_A_utdelning": "A UTDELNING",
        "bucket_cap_B_tillväxt": "B TILLVÄXT",
        "bucket_cap_B_utdelning": "B UTDELNING",
        "bucket_cap_C_tillväxt": "C TILLVÄXT",
        "bucket_cap_C_utdelning": "C UTDELNING",
    }
    for k, canon in key_map.items():
        v = _f(settings.get(k))
        if v is not None and v >= 0:
            caps[canon] = float(v)
    return caps

# -------------------------
# Källskatt per valuta (KF-antagande)
# -------------------------
WITHHOLDING_BY_CCY = {
    "USD": 0.15,
    "CAD": 0.15,
    "NOK": 0.25,
    # Vanliga övriga – om okänd → 0.15 som konservativ default
    "EUR": 0.15,
    "GBP": 0.15,
    "DKK": 0.27,  # Danmark ofta 27% (kan variera – behandlas konservativt)
    "CHF": 0.35,  # Schweiz 35% (KF hanterar åter)
    "SEK": 0.00,
}

def withholding_rate_for_currency(ccy: str) -> float:
    c = (ccy or "").upper()
    return WITHHOLDING_BY_CCY.get(c, 0.15)

# -------------------------
# Portföljvärden i SEK + Bucket-utrymme
# -------------------------
def compute_portfolio_snapshot(df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Tar Data-bladets DataFrame (minst kolumner: Ticker, Valuta, Aktuell kurs, Antal aktier, Bucket)
    och returnerar:
      • holdings_df med kolumner:
        [Ticker, Valuta, Kurs, Antal, Värde SEK, Bucket, Tak SEK, Utrymme SEK, Över tak?]
      • totals: {"värde_sek": ..., "antal_innehav": ...}
    Hämtar aktuell kurs från Yahoo om inte ifyllt.
    """
    caps = load_bucket_caps_from_settings(settings)

    rows = []
    total_value_sek = 0.0

    for _, row in (df or pd.DataFrame()).iterrows():
        tkr = str(row.get("Ticker", "")).strip()
        if not tkr:
            continue

        # Kurs
        y = None
        price = _f(row.get("Aktuell kurs"))
        currency = (row.get("Valuta") or "USD")
        if price is None:
            try:
                y = fetch_from_yahoo(tkr)
                price = _f(y.get("price"))
                currency = (y.get("currency") or currency)
            except Exception:
                pass

        shares = _f(row.get("Antal aktier")) or 0.0
        if shares <= 0:
            continue

        # Värde i SEK
        val_ccy = (price or 0.0) * shares
        val_sek = _amount_sek(val_ccy, currency, fx_map) or 0.0
        total_value_sek += val_sek

        # Bucket
        bucket_raw = row.get("Bucket") or row.get("Hink") or ""
        bucket = _normalize_bucket_name(bucket_raw)
        # Försök detektera "A", "B", "C" + kategori
        # Om användaren bara skrivit "A" → antag tillväxt (kan ändras i Settings)
        if bucket in ("A", "B", "C"):
            bucket = f"{bucket} TILLVÄXT"
        # Om inget "TILLVÄXT/UTDELNING" finns men raden är utdelningscase (har DPS) → märk utdelning
        if "TILLVÄXT" not in bucket and "UTDELNING" not in bucket:
            dps = None
            try:
                if y is None:
                    y = fetch_from_yahoo(tkr)
                dps = _f(y.get("dps_annual"))
            except Exception:
                pass
            bucket = f"{bucket} UTDELNING" if (dps and dps > 0) else f"{bucket} TILLVÄXT"
            bucket = bucket.strip()

        cap = caps.get(bucket, None)
        over = None
        room = None
        if cap is not None:
            over = val_sek > cap
            room = cap - val_sek

        rows.append({
            "Ticker": tkr,
            "Valuta": str(currency).upper(),
            "Kurs": price,
            "Antal": shares,
            "Värde SEK": val_sek,
            "Bucket": bucket,
            "Tak SEK": cap,
            "Utrymme SEK": room,
            "Över tak?": bool(over) if over is not None else None,
        })

    holdings_df = pd.DataFrame(rows, columns=["Ticker","Valuta","Kurs","Antal","Värde SEK","Bucket","Tak SEK","Utrymme SEK","Över tak?"])
    holdings_df = holdings_df.sort_values("Värde SEK", ascending=False, kind="mergesort", na_position="last").reset_index(drop=True)

    totals = {
        "värde_sek": float(total_value_sek),
        "antal_innehav": int((holdings_df["Ticker"].notna()).sum()) if not holdings_df.empty else 0,
    }
    return holdings_df, totals

# -------------------------
# Nästa utdelning (estimering) per ticker
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def _infer_next_dividend_paydate_and_amount(ticker: str, currency_hint: Optional[str]) -> Dict[str, Any]:
    """
    Försöker härleda nästa *utbetalningsdatum* (PAY date) och *per-betalning DPS*.
    Källor:
      1) yfinance.dividends (historik) → frekvens (månad/kvartal/halv/år)
         *estimerar* nästa EX-datum = senaste + frekvens; PAY ≈ EX + 30 dagar (approx).
      2) Om ingen historik → returnera None.
    Returnerar: {"ccy": "USD", "dps_pay": float|None, "freq": "quarterly|monthly|semi|annual|unknown", "paydate": pd.Timestamp|None, "estimated": bool}
    Obs: Det är en *estimering* när exakta pay-datum ej kan hämtas via API.
    """
    try:
        tk = yf.Ticker(ticker)
        div = tk.dividends
        ccy = (currency_hint or "USD")
        if div is None or len(div) == 0:
            return {"ccy": ccy, "dps_pay": None, "freq": "unknown", "paydate": None, "estimated": True}

        # Frekvens via median av senaste intervall
        dts = pd.Series(div.index).sort_values()
        if len(dts) >= 3:
            diffs = dts.diff().dropna().dt.days
            med = float(np.median(diffs))
        elif len(dts) >= 2:
            med = float((dts.iloc[-1] - dts.iloc[-2]).days)
        else:
            med = 90.0  # gissa kvartal

        if med <= 40:
            freq = "monthly"
            parts = 12
            gap_days = 30
        elif med <= 120:
            freq = "quarterly"
            parts = 4
            gap_days = 30
        elif med <= 240:
            freq = "semi"
            parts = 2
            gap_days = 45
        else:
            freq = "annual"
            parts = 1
            gap_days = 60

        # Per-betalning DPS ~ senaste betalning (robustare än årlig/parts när ändringar sker)
        last_dps = float(div.iloc[-1]) if len(div) else None

        # Nästa *ex* ≈ senaste index + med
        last_ex = pd.Timestamp(dts.iloc[-1]).normalize()
        next_ex = last_ex + pd.Timedelta(days=int(round(med)))
        # Nästa *pay* ≈ next_ex + gap_days (estimat)
        next_pay = next_ex + pd.Timedelta(days=gap_days)

        # Om kommande pay redan passerat (tidszonsskillnader) → skjut fram ett intervall till
        today = pd.Timestamp(pd.Timestamp.utcnow().date())
        if next_pay.date() <= today.date():
            next_ex = next_ex + pd.Timedelta(days=int(round(med)))
            next_pay = next_pay + pd.Timedelta(days=int(round(med)))

        # dps_pay: använd senaste *utbetalda* per-betalning om den verkar rimlig
        dps_pay = None
        try:
            dps_pay = float(last_dps) if last_dps is not None and last_dps >= 0 else None
        except Exception:
            dps_pay = None

        return {"ccy": ccy, "dps_pay": dps_pay, "freq": freq, "paydate": next_pay, "estimated": True}
    except Exception:
        return {"ccy": (currency_hint or "USD"), "dps_pay": None, "freq": "unknown", "paydate": None, "estimated": True}

def build_next_dividends_table(df: pd.DataFrame, fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Skapar tabell med nästa *utbetalningsdatum* per innehav (endast framtida),
    inklusive källskatt och nettobelopp i SEK.
    Kräver minst: Ticker, Valuta, Antal aktier. Använder Yahoo för DPS/frekvens om möjligt.
    """
    records = []
    total_net_sek = 0.0

    for _, row in (df or pd.DataFrame()).iterrows():
        tkr = str(row.get("Ticker", "")).strip()
        if not tkr:
            continue
        shares = _f(row.get("Antal aktier")) or 0.0
        if shares <= 0:
            continue

        # Hämta valuta/DPS via Yahoo wrapper
        y = None
        currency = (row.get("Valuta") or "USD")
        dps_annual = None
        try:
            y = fetch_from_yahoo(tkr)
            currency = y.get("currency") or currency
            dps_annual = _f(y.get("dps_annual"))
        except Exception:
            pass

        # Försök estimera nästa pay-datum och per-betalning DPS
        est = _infer_next_dividend_paydate_and_amount(tkr, currency)
        paydate = est.get("paydate")
        dps_pay = _f(est.get("dps_pay"))

        # Om dps_pay saknas men årlig finns → approximera per betalning via frekvens
        if dps_pay is None and dps_annual is not None:
            freq = est.get("freq")
            parts = {"monthly":12, "quarterly":4, "semi":2, "annual":1}.get(freq, 4)
            dps_pay = float(dps_annual) / float(parts)

        # Skippa om vi saknar både datum och dps
        if paydate is None or dps_pay is None:
            continue

        # Skippa passerade datum
        today = pd.Timestamp(pd.Timestamp.utcnow().date())
        if pd.Timestamp(paydate).date() <= today.date():
            continue

        gross = dps_pay * shares                            # i aktiens valuta
        w = withholding_rate_for_currency(currency)
        tax = gross * w
        net = gross - tax

        net_sek = _amount_sek(net, currency, fx_map) or 0.0
        total_net_sek += net_sek

        records.append({
            "Datum": pd.Timestamp(paydate).date(),
            "Ticker": tkr,
            "Valuta": str(currency).upper(),
            "Per-betalning DPS": dps_pay,
            "Antal": shares,
            "Brutto": gross,
            "Källskatt %": w,
            "Skatt": tax,
            "Netto": net,
            "Netto SEK": net_sek,
            "Estimerad?": bool(est.get("estimated", True)),
        })

    dd = pd.DataFrame(records, columns=["Datum","Ticker","Valuta","Per-betalning DPS","Antal","Brutto","Källskatt %","Skatt","Netto","Netto SEK","Estimerad?"])
    if not dd.empty:
        dd = dd.sort_values(["Datum","Ticker"], ascending=[True, True], kind="mergesort").reset_index(drop=True)

    summary = {
        "next_dividends_net_sek": float(total_net_sek),
        "next_rows": int(len(dd)),
    }
    return dd, summary

def estimate_annual_dividends_net_sek(df: pd.DataFrame, fx_map: Dict[str, float]) -> float:
    """
    Grov årlig *netto* utdelning i SEK = SUM(antal * dps_annual * (1-wtax) * FX).
    Använder Yahoo dps_annual där möjligt. Om saknas → 0.
    """
    total = 0.0
    for _, row in (df or pd.DataFrame()).iterrows():
        tkr = str(row.get("Ticker", "")).strip()
        if not tkr:
            continue
        shares = _f(row.get("Antal aktier")) or 0.0
        if shares <= 0:
            continue
        ccy = (row.get("Valuta") or "USD")
        dps_annual = None
        try:
            y = fetch_from_yahoo(tkr)
            ccy = y.get("currency") or ccy
            dps_annual = _f(y.get("dps_annual"))
        except Exception:
            pass
        if dps_annual is None or dps_annual <= 0:
            continue
        w = withholding_rate_for_currency(ccy)
        net_ccy = dps_annual * (1.0 - w) * shares
        net_sek = _amount_sek(net_ccy, ccy, fx_map) or 0.0
        total += net_sek
    return float(total)

# -------------------------
# Render: Portfölj-vy (UI)
# -------------------------
def render_portfolio_view(data_df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, Any]) -> None:
    """
    Visar:
      • Nyckeltal överst (Portföljvärde SEK, Årlig netto-utdelning SEK (est.), Utdelning/mån)
      • Tabell: Nästa utdelningsutbetalningar (framtida datum, netto i SEK)
      • Tabell: Innehav (värde i SEK, bucket-tak, utrymme)
    """
    st.header("📦 Portfölj")

    holdings_df, totals = compute_portfolio_snapshot(data_df, fx_map, settings)
    annual_net_sek = estimate_annual_dividends_net_sek(data_df, fx_map)
    monthly_net_sek = annual_net_sek / 12.0 if annual_net_sek else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Portföljvärde (SEK)", f"{totals.get('värde_sek',0):,.0f}".replace(",", " ").replace(".", ","))
    col2.metric("Årlig utdelning netto (SEK, est.)", f"{annual_net_sek:,.0f}".replace(",", " ").replace(".", ","))
    col3.metric("Utdelning/månad (SEK, est.)", f"{monthly_net_sek:,.0f}".replace(",", " ").replace(".", ","))

    # Nästa utbetalningar
    with st.expander("📅 Nästa utdelningsutbetalningar (netto, SEK)"):
        next_dd, sm = build_next_dividends_table(data_df, fx_map)
        if next_dd.empty:
            st.info("Inga kommande utbetalningar kunde estimeras just nu.")
        else:
            st.dataframe(
                next_dd.style.format({
                    "Per-betalning DPS": "{:.4f}",
                    "Antal": "{:.0f}",
                    "Brutto": "{:.2f}",
                    "Källskatt %": "{:.0%}",
                    "Skatt": "{:.2f}",
                    "Netto": "{:.2f}",
                    "Netto SEK": "{:.0f}",
                }),
                use_container_width=True,
                hide_index=True,
            )
            st.caption(f"Rader: {sm.get('next_rows',0)} • Summa netto SEK (dessa betalningar): {sm.get('next_dividends_net_sek',0):,.0f}".replace(",", " ").replace(".", ","))

    # Innehav & Bucket-tak
    with st.expander("📊 Innehav & Bucket-tak (SEK)"):
        caps = load_bucket_caps_from_settings(settings)
        cap_str = ", ".join([f"{k}: {v:,.0f} SEK".replace(",", " ").replace(".", ",") for k, v in caps.items()])
        st.caption(f"Tak per hink (kan ändras i Settings): {cap_str}")
        if holdings_df.empty:
            st.info("Inga innehav att visa.")
        else:
            st.dataframe(
                holdings_df.style.format({
                    "Kurs": "{:.2f}",
                    "Antal": "{:.0f}",
                    "Värde SEK": "{:,.0f}",
                    "Tak SEK": "{:,.0f}",
                    "Utrymme SEK": "{:,.0f}",
                }).apply(
                    lambda s: ["background-color: #ffecec" if (isinstance(v, bool) and v) else "" for v in s] if s.name == "Över tak?" else ["" for _ in s],
                    axis=0
                ),
                use_container_width=True,
                hide_index=True,
            )

# ============================================================
# (Slut Del 4/6)
# Nästa del (Del 5/6) — Analysvy & Investeringsförslag (ranking, bläddrare, fair value-visning)
# ============================================================

# ============================================================
# Del 5/6 — Vyer (UI)
#  • Settings
#  • Snapshot
#  • Editor (manuellt + Yahoo-prefill)
#  • Lägg till ticker
#  • Portfölj (inkl. per-Bucket & andel av Bucket)
#  • Analys (fair value, metoder)
#  • Ranking (uppsida)
#  • Batch (massuppdatering Yahoo)
# ============================================================

# -----------------------------
# Små helpers (formatteringar & val)
# -----------------------------
if "_now" not in globals():
    def _now():
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if "_round2_or_none" not in globals():
    def _round2_or_none(x):
        v = _f(x)
        return None if v is None else round(float(v), 2)

if "_parse_float" not in globals():
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

if "_maybe" not in globals():
    def _maybe(v):
        return v if (v is not None and not (isinstance(v, float) and pd.isna(v))) else None

if "_format_num" not in globals():
    def _format_num(x, nd=2):
        v = _f(x)
        if v is None:
            return "—"
        return f"{v:.{nd}f}"

def _names_map_from_df(df: pd.DataFrame) -> dict[str, str]:
    mp: dict[str, str] = {}
    if df is None or df.empty or "Ticker" not in df.columns:
        return mp
    for _, r in df.iterrows():
        t = str(r.get("Ticker") or "").strip()
        n = str(r.get("Bolagsnamn") or "").strip()
        if t:
            mp[t] = n
    return mp

def _select_with_search_nav(label: str,
                            options: list[str],
                            names_map: dict[str,str],
                            idx_key: str,
                            query_key: str) -> str | None:
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

# Kompatibelt yahoo_snapshot för vyer (hämtar kurs/valuta via Del 3/6)
if "yahoo_snapshot" not in globals():
    def yahoo_snapshot(ticker: str) -> dict:
        y = fetch_from_yahoo(ticker)
        return {"Aktuell kurs": _f(y.get("price")), "Valuta": y.get("currency")}

# -----------------------------
# Settings-vy (sparar till Google Sheets)
# -----------------------------
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
        st.caption("Källskatt per valuta (brutto → netto)")
        wh_usd = st.number_input("USD", 0.0, 0.5, float(_f(s.get("withholding_USD", s.get("tax_usd"))) or 0.15), 0.01)
        wh_nok = st.number_input("NOK", 0.0, 0.5, float(_f(s.get("withholding_NOK", s.get("tax_nok"))) or 0.25), 0.01)
        wh_cad = st.number_input("CAD", 0.0, 0.5, float(_f(s.get("withholding_CAD", s.get("tax_cad"))) or 0.15), 0.01)
        wh_eur = st.number_input("EUR", 0.0, 0.5, float(_f(s.get("withholding_EUR", s.get("tax_eur"))) or 0.15), 0.01)
        wh_sek = st.number_input("SEK", 0.0, 0.5, float(_f(s.get("withholding_SEK", s.get("tax_sek"))) or 0.00), 0.01)

    st.markdown("#### Bucket-tak per innehav (SEK)")
    cA, cB = st.columns(2)
    with cA:
        cap_A_t = st.number_input("Bucket A tillväxt (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_A_tillvaxt")) or 20000.0), step=100.0)
        cap_B_t = st.number_input("Bucket B tillväxt (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_B_tillvaxt")) or 10000.0), step=100.0)
        cap_C_t = st.number_input("Bucket C tillväxt (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_C_tillvaxt")) or 6000.0), step=100.0)
    with cB:
        cap_A_u = st.number_input("Bucket A utdelning (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_A_utdelning")) or 10000.0), step=100.0)
        cap_B_u = st.number_input("Bucket B utdelning (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_B_utdelning")) or 7000.0), step=100.0)
        cap_C_u = st.number_input("Bucket C utdelning (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_C_utdelning")) or 4000.0), step=100.0)

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

        # Bas
        set_kv("primary_currency", primary)
        set_kv("pe_anchor_weight_ttm", pe_w)
        set_kv("multiple_decay", decay)
        set_kv("auto_refresh_on_start", "1" if auto else "0")
        # Källskatt (legacy + override)
        set_kv("withholding_USD", wh_usd); set_kv("tax_usd", wh_usd)
        set_kv("withholding_NOK", wh_nok); set_kv("tax_nok", wh_nok)
        set_kv("withholding_CAD", wh_cad); set_kv("tax_cad", wh_cad)
        set_kv("withholding_EUR", wh_eur); set_kv("tax_eur", wh_eur)
        set_kv("withholding_SEK", wh_sek); set_kv("tax_sek", wh_sek)
        # Bucket-tak (SEK)
        set_kv("bucket_cap_A_tillvaxt", cap_A_t)
        set_kv("bucket_cap_B_tillvaxt", cap_B_t)
        set_kv("bucket_cap_C_tillvaxt", cap_C_t)
        set_kv("bucket_cap_A_utdelning", cap_A_u)
        set_kv("bucket_cap_B_utdelning", cap_B_u)
        set_kv("bucket_cap_C_utdelning", cap_C_u)

        _write_df(SETTINGS_TITLE, s_df[SETTINGS_COLUMNS])
        st.success("Inställningar sparade till Google Sheets.")

    st.markdown("---")
    st.subheader("Valutakurser")
    st.dataframe(_read_df(FX_TITLE), use_container_width=True, hide_index=True)
    if st.button("🔁 Hämta/uppdatera valutakurser"):
        _load_fx_and_update_sheet()
        st.success("Valutakurser uppdaterade.")

# -----------------------------
# Snapshot-vy
# -----------------------------
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True, hide_index=True)

# -----------------------------
# Editor-hjälpare
# -----------------------------
def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad",
            "Rev 2Y uppdaterad","Senast manuellt uppdaterad"]
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *cols])
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _build_updates_from_yahoo(tkr: str, existing_row: pd.Series):
    y   = fetch_from_yahoo(tkr)
    est = _fetch_eps_estimates_yahoo(tkr)
    updates = {
        "Timestamp": _now(),
        "Aktuell kurs": _round2_or_none(y.get("price")),
        "Valuta": (y.get("currency") or existing_row.get("Valuta")),
        "Utestående aktier": _maybe(y.get("shares_out")),
        "Net debt": _maybe(y.get("net_debt")),
        "Rev TTM": _maybe(y.get("rev_ttm")),
        "EBITDA TTM": _maybe(y.get("ebitda_ttm")),
        "EPS TTM": _maybe(y.get("eps_ttm")),
        "PE TTM": _maybe(y.get("pe_ttm")),
        "PE FWD": _maybe(y.get("pe_fwd")),
        "EV/Revenue": _maybe(y.get("ev_rev")),
        "EV/EBITDA": _maybe(y.get("ev_ebitda")),
        "P/B": _maybe(y.get("p_b")),
        "BVPS": _maybe(y.get("bvps")),
        "Rev CAGR": _maybe(y.get("rev_cagr_hist")),
        "EPS CAGR": _maybe(y.get("eps_cagr_hist")),
        "Årlig utdelning": _maybe(y.get("dps_annual")),
        "EPS 1Y": existing_row.get("EPS 1Y") if pd.notna(existing_row.get("EPS 1Y")) else _maybe(est.get("eps_1y")),
        "EPS 2Y": existing_row.get("EPS 2Y") if pd.notna(existing_row.get("EPS 2Y")) else _maybe(est.get("eps_2y")),
        "Senast auto uppdaterad": _now(),
        "Auto källa": "Yahoo",
    }
    out = {}
    for k, v in updates.items():
        if v is None: continue
        if isinstance(v, float) and pd.isna(v): continue
        if isinstance(v, str) and v.strip() == "": continue
        out[k] = v
    return out

# -----------------------------
# Editor
# -----------------------------
def page_editor():
    st.header("✏️ Editor (manuella fält)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    df = _ensure_editor_stamp_cols(df)
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
                eps1_v  = _parse_float(eps1_in)
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
    st.dataframe(df.loc[[idx]], use_container_width=True, hide_index=True)

# -----------------------------
# Lägg till ticker
# -----------------------------
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
        eps1_in = st.text_input("EPS 1Y (estimat)", key="add_eps1")
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
                    y = fetch_from_yahoo(tkr)
                    st.session_state["add_ccy"] = (y.get("currency") or st.session_state.get("add_ccy","USD")).upper()
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

            eps1_v  = _parse_float(eps1_in)
            eps2_v  = _parse_float(eps2_in)
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

# -----------------------------
# Portfölj (UI)
#  – Använder render_portfolio_view (Del 4/6) + extra expander:
#    🪣 Bucket-tillhörighet per Bucket inkl. “Andel av Bucket”
# -----------------------------
def page_portfolio():
    st.header("📦 Portfölj")

    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    fx = st.session_state.get("FX", {}) or {}
    settings = get_settings_map()

    # Basrender från Del 4 (värden, kommande utdelningar, bucket-tak-tabell)
    try:
        render_portfolio_view(df, fx, settings)
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")
        return

    # === Extra expander: per Bucket + andel av Bucket ===
    try:
        holdings_df, _ = compute_portfolio_snapshot(df, fx, settings)
        if holdings_df.empty or "Bucket" not in holdings_df.columns:
            st.info("Ingen bucket-klassning hittades (lägg till kolumnen **Bucket** i DATA).")
            return

        st.markdown("---")
        with st.expander("🪣 Bucket-tillhörighet (per Bucket & andel av Bucket)", expanded=False):
            # Normalisera bucket-ordning
            order = pd.CategoricalDtype(["A TILLVÄXT","A UTDELNING","B TILLVÄXT","B UTDELNING","C TILLVÄXT","C UTDELNING"], ordered=True)
            try:
                holdings_df["Bucket"] = holdings_df["Bucket"].astype(order)
            except Exception:
                pass

            for bkt, grp in holdings_df.sort_values(["Bucket","Värde SEK"], ascending=[True, False]).groupby("Bucket"):
                if pd.isna(bkt):
                    continue
                bucket_total = float(np.nansum(grp["Värde SEK"].values)) if not grp.empty else 0.0
                if bucket_total <= 0:
                    continue
                # Andel av Bucket (%)
                g2 = grp.copy()
                g2["Andel av Bucket (%)"] = g2["Värde SEK"] / bucket_total * 100.0
                show_cols = ["Ticker","Värde SEK","Andel av Bucket (%)"]
                st.markdown(f"**{bkt}** — Summa: `{bucket_total:,.0f} SEK`".replace(",", " ").replace(".", ","))
                st.dataframe(
                    g2[show_cols].style.format({"Värde SEK": "{:,.0f}", "Andel av Bucket (%)": "{:.1f}"}),
                    use_container_width=True, hide_index=True
                )
    except Exception as e:
        st.warning(f"Bucket-expander kunde inte beräknas: {e}")

# -----------------------------
# Analys
# -----------------------------
def _pick_primary_method(row: pd.Series, methods_df: pd.DataFrame) -> str:
    existing = str(row.get("Primär metod") or "").strip()
    available = methods_df["Metod"].tolist()
    if existing and existing in available:
        r = methods_df[methods_df["Metod"] == existing]
        if not r.empty and r[["Idag","1 år","2 år","3 år"]].notna().any(axis=None):
            return existing
    for m in (PREFER_ORDER if "PREFER_ORDER" in globals() else methods_df["Metod"].tolist()):
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

def _ensure_price_for_row(row: pd.Series) -> float | None:
    p = _pos(row.get("Aktuell kurs"))
    if _pos(p):
        return float(p)
    y = fetch_from_yahoo(str(row.get("Ticker")))
    return _pos(y.get("price"))

def page_analysis():
    st.header("🔬 Analys")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    # Filter: Bucket + Ägande
    all_buckets = [b for b in df["Bucket"].dropna().astype(str).unique().tolist() if b.strip()] or DEFAULT_BUCKETS
    sel_buckets = st.multiselect("Filtrera på Bucket", options=all_buckets, default=all_buckets)
    ownership = st.radio("Ägande", ["Alla","Äger (>0)","Äger inte (=0)"], index=0, horizontal=True)

    base = df.copy()
    if sel_buckets:
        base = base[base["Bucket"].astype(str).isin(sel_buckets)]
    if ownership == "Äger (>0)":
        base = base[(pd.to_numeric(base["Antal aktier"], errors="coerce") > 0)]
    elif ownership == "Äger inte (=0)":
        base = base[(pd.to_numeric(base["Antal aktier"], errors="coerce").fillna(0) == 0)]

    if base.empty:
        st.info("Inga bolag matchar urvalet.")
        return

    tickers = base["Ticker"].dropna().astype(str).unique().tolist()
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
    st.dataframe(methods_df, use_container_width=True, hide_index=True)

    fv = meta.get("fair_value", {}) or {}
    st.markdown("#### 🧭 Fair Value (median över metoder)")
    cfa, cfb, cfc, cfd = st.columns(4)
    cfa.metric("FV idag", _format_num(fv.get("today")))
    cfb.metric("FV 1 år", _format_num(fv.get("y1")))
    cfc.metric("FV 2 år", _format_num(fv.get("y2")))
    cfd.metric("FV 3 år", _format_num(fv.get("y3")))

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
    st.dataframe(st.session_state["DATA"], use_container_width=True, hide_index=True)

# -----------------------------
# Ranking – Uppsida
# -----------------------------
def page_ranking():
    st.header("🏆 Ranking – Uppsida per horisont")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    all_buckets = [b for b in df["Bucket"].dropna().astype(str).unique().tolist() if b.strip()] or DEFAULT_BUCKETS
    sel_buckets = st.multiselect("Filtrera på Bucket", options=all_buckets, default=all_buckets)

    only_owned = st.checkbox("Visa endast innehav (Antal aktier > 0)", value=False)
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)

    base = df.copy()
    if sel_buckets:
        base = base[base["Bucket"].astype(str).isin(sel_buckets)]
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
                "Bucket": str(r.get("Bucket") or ""),
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
    st.dataframe(rank, use_container_width=True, hide_index=True)

    st.markdown("---")
    if st.checkbox("Visa ett bolag i taget"):
        idx = st.number_input("Index", min_value=1, max_value=len(rank), value=1, step=1)
        item = rank.iloc[int(idx)-1]
        st.metric("Ticker", item["Ticker"])
        c1,c2,c3 = st.columns(3)
        c1.metric("Kurs", _format_num(item["Kurs"]))
        c2.metric(f"Riktkurs {horizon}", _format_num(item[f"Riktkurs {horizon}"]))
        c3.metric("Uppsida (%)", "—" if pd.isna(item["Uppsida (%)"]) else f"{item['Uppsida (%)']:.1f}%")
        st.caption(f"Metod: {item['Metod']}  ·  Valuta: {item['Valuta']}  ·  Bucket: {item['Bucket']}")

# -----------------------------
# Batch (Massuppdatering Yahoo)
# -----------------------------
def _clean_non_empty(d: dict) -> dict:
    out = {}
    for k, v in (d or {}).items():
        if v is None: continue
        if isinstance(v, float) and pd.isna(v): continue
        if isinstance(v, str) and v.strip() == "": continue
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
# (Slut Del 5/6)
# Nästa del (Del 6/6) — Main & routing
# ============================================================

# ============================================================
# Del 6/6 — Main & routing
#  • Init (Settings, FX, DATA)
#  • Sidopanel & navigation
#  • Spara/Ladda-knappar
#  • Felhantering runt sidvyer
# ============================================================

# -----------------------------
# Init helpers
# -----------------------------
def _init_state_once():
    ss = st.session_state
    ss.setdefault("DATA", None)
    ss.setdefault("FX", {})
    ss.setdefault("SETTINGS", {})
    ss.setdefault("boot_fx_refreshed", False)

def _load_all_into_state(force_reload: bool = False):
    """Läs Settings, FX och DATA → session_state."""
    try:
        st.session_state["SETTINGS"] = get_settings_map(force_reload=force_reload)
    except Exception as e:
        st.warning(f"Kunde inte läsa Settings: {e}")

    try:
        # Läs in befintliga FX från bladet
        st.session_state["FX"] = get_fx_map(force_reload=force_reload)
    except Exception as e:
        st.warning(f"Kunde inte läsa Valutakurser: {e}")

    try:
        st.session_state["DATA"] = read_data_df(force_reload=force_reload)
    except Exception as e:
        st.error(f"Kunde inte läsa DATA-bladet: {e}")
        st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)

def _maybe_auto_refresh_fx_on_boot():
    """Respektera inställningen 'auto_refresh_on_start' (en gång per körning)."""
    s = get_settings_map()
    if str(s.get("auto_refresh_on_start", "0")) == "1" and not st.session_state.get("boot_fx_refreshed", False):
        try:
            _load_fx_and_update_sheet()
            st.session_state["FX"] = get_fx_map(force_reload=True)
            st.session_state["boot_fx_refreshed"] = True
            st.toast("Valutakurser auto-uppdaterade.", icon="🔁")
        except Exception as e:
            st.warning(f"Auto-uppdatering av FX misslyckades: {e}")

# -----------------------------
# Routing-tabell
# -----------------------------
PAGES: dict[str, tuple[str, callable]] = {
    "settings": ("⚙️ Settings", page_settings),
    "snapshot": ("🕒 Snapshot", page_snapshot),
    "editor":   ("✏️ Editor", page_editor),
    "add":      ("➕ Lägg till ticker", page_add_ticker),
    "portfolio":("📦 Portfölj", page_portfolio),
    "analysis": ("🔬 Analys", page_analysis),
    "ranking":  ("🏆 Ranking", page_ranking),
    "batch":    ("🧩 Massuppdatering", page_batch),
}

# Ordning i sidomenyn
PAGE_ORDER = ["portfolio", "analysis", "ranking", "editor", "add", "batch", "snapshot", "settings"]

# -----------------------------
# Main
# -----------------------------
def main():
    _init_state_once()
    _load_all_into_state(force_reload=False)
    _maybe_auto_refresh_fx_on_boot()

    # -------- Sidopanel --------
    with st.sidebar:
        st.markdown("### Aktieanalys & investeringsförslag")
        st.caption("Basvaluta visas per bolagets handelsvaluta. Ingen konvertering av EPS.")

        # Snabbåtgärder
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🔄 Ladda om DATA"):
                _load_all_into_state(force_reload=True)
                st.success("DATA/Settings/FX omladdat från Google Sheets.")
                st.rerun()
        with col_b:
            if st.button("💾 Spara DATA"):
                try:
                    df = st.session_state.get("DATA")
                    if df is None:
                        st.warning("Inget att spara.")
                    else:
                        write_data_df(df)
                        st.success("DATA sparad till Google Sheets.")
                except Exception as e:
                    st.error(f"Kunde inte spara: {e}")

        st.markdown("---")

        # Navigering
        labels = [PAGES[key][0] for key in PAGE_ORDER]
        keys   = PAGE_ORDER[:]
        default_idx = st.session_state.get("nav_idx", 0)
        choice = st.radio("Navigation", labels, index=default_idx, key="nav_radio")
        page_key = keys[labels.index(choice)]
        st.session_state["nav_idx"] = keys.index(page_key)

        st.markdown("---")
        # Liten status
        df = st.session_state.get("DATA")
        rows = 0 if df is None else len(df)
        st.caption(f"📄 Rader i DATA: **{rows}**")
        s = get_settings_map()
        st.caption(f"🌐 Primär valuta: **{s.get('primary_currency','SEK')}**")

    # -------- Innehåll --------
    try:
        # Kör vald sida
        PAGES[page_key][1]()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

    # Footer
    st.markdown("---")
    st.caption(f"Senast uppdaterad: {now_stamp()}")

# Kör appen (Streamlit kör filen topp-till-botten)
main()

# ============================================================
# (Slut Del 6/6)
# ============================================================
