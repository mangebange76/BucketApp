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

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown("<style>section.main > div {max-width: 1400px;}</style>", unsafe_allow_html=True)

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
    """
    Hämta värde från env eller st.secrets.
    Stöd även för gemener och några vanliga alias (GOOGLE_/SHEET_).
    """
    # 1) Exakt miljövariabel
    v = os.environ.get(key)
    if v:
        return v

    # 2) streamlit secrets (exakt + gemener)
    try:
        if key in st.secrets:
            return st.secrets.get(key, default)
        # försök med gemener och alias
        low_key = key.lower()
        aliases = {
            "sheet_url": ["google_sheet_url", "spreadsheet_url"],
            "sheet_id":  ["google_sheet_id", "spreadsheet_id"],
            "google_credentials": ["gcp_service_account", "service_account_json"],
        }
        # direkt gemener
        if low_key in st.secrets:
            return st.secrets.get(low_key, default)
        # alias
        for k, alist in aliases.items():
            if low_key == k and any(a in st.secrets for a in alist):
                for a in alist:
                    if a in st.secrets:
                        return st.secrets[a]
    except Exception:
        pass

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
    header = values[0] if values and len(values) > 0 else []
    rows   = values[1:] if values and len(values) > 1 else []
    if not header:
        return pd.DataFrame()
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
    # Extra hjälp-fält för editorns äldre vyer
    "EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad","Rev 2Y uppdaterad"
]

SETTINGS_COLUMNS = ["Key","Value"]
FX_COLUMNS       = ["Valuta","SEK_per_1"]

def _ensure_sheet_schema():
    """
    Säkerställ att alla ark (Data, Settings, Valutakurser, Snapshot, Resultat)
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
            ["auto_refresh_on_start","0"],
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

    # --- Resultat ---
    res = _read_df(RESULT_TITLE)
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame(columns=[
            "Timestamp","Ticker","Valuta","Metod",
            "Idag","1 år","2 år","3 år","Kommentar"
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

    # Datumkolumn → ren Python date eller None (aldrig NaT)
    if "Nästa utdelningsdatum" in df.columns:
        dcol = pd.to_datetime(df["Nästa utdelningsdatum"], errors="coerce", utc=False)
        df["Nästa utdelningsdatum"] = dcol.apply(lambda x: x.date() if pd.notna(x) else None)

    # Tidsstämplar som sträng
    for tcol in ["TS EPS 1Y","TS EPS 2Y","TS Rev 1Y","TS Rev 2Y","Senast auto uppdaterad","Senast manuellt uppdaterad"]:
        if tcol in df.columns:
            df[tcol] = df[tcol].astype(str)

    # Sätt 0→NaN på auto-hämtade fält
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

# ------------------------------------------------------------
# Hjälpfunktion för säker Data-inläsning från session
# ------------------------------------------------------------
def df_or_reload_from_session() -> pd.DataFrame:
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df
    return df

# (Slut Del 1/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 2/6: Datainhämtning (Yahoo) & uppdateringshjälpare
#
#  - Säkra wrappers för yfinance (pris, valuta, shares, PE, EPS, TTM)
#  - Fältmappning → Data-bladets kolumner
#  - Uppdatera en rad / massuppdatering (utan UI)
#
# Viktigt:
# • Ingen ändring av riktkurslogik här.
# • Endast säkra hämtningar och försiktig skrivning.
# ============================================================

# ------------------------------
# yfinance-hjälpare (robusta)
# ------------------------------
def _yf_ticker(sym: str):
    try:
        return yf.Ticker(sym)
    except Exception:
        return None

def _yf_last_price(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # 1) fast_info
    try:
        fi = tkr.fast_info
        px = getattr(fi, "last_price", None)
        if px:
            return float(px)
    except Exception:
        pass
    # 2) info
    try:
        info = tkr.info
        px = info.get("currentPrice") or info.get("regularMarketPrice")
        if px:
            return float(px)
    except Exception:
        pass
    # 3) history fallback
    try:
        h = tkr.history(period="5d")
        if not h.empty:
            return float(h["Close"].dropna().iloc[-1])
    except Exception:
        pass
    return None

def _yf_currency(tkr) -> Optional[str]:
    if tkr is None:
        return None
    # fast_info
    try:
        fi = tkr.fast_info
        c = getattr(fi, "currency", None)
        if c:
            return str(c).upper()
    except Exception:
        pass
    # info
    try:
        info = tkr.info
        c = info.get("currency")
        if c:
            return str(c).upper()
    except Exception:
        pass
    return None

def _yf_shares_out(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # fast_info
    try:
        fi = tkr.fast_info
        s = getattr(fi, "shares", None)
        if s and s > 0:
            return float(s)
    except Exception:
        pass
    # info
    try:
        info = tkr.info
        s = info.get("sharesOutstanding")
        if s and s > 0:
            return float(s)
    except Exception:
        pass
    # get_shares_full (senaste kända)
    try:
        df = tkr.get_shares_full()
        if df is not None and not df.empty:
            val = float(df["SharesOutstanding"].dropna().iloc[-1])
            if val > 0:
                return val
    except Exception:
        pass
    return None

def _yf_eps_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        fi = tkr.fast_info
        v = getattr(fi, "trailingEps", None)
        if v and v == v:
            return float(v)
    except Exception:
        pass
    try:
        info = tkr.info
        v = info.get("trailingEps")
        if v and v == v:
            return float(v)
    except Exception:
        pass
    return None

def _yf_pe_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        fi = tkr.fast_info
        v = getattr(fi, "trailingPe", None)
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    try:
        info = tkr.info
        v = info.get("trailingPE")
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_pe_fwd(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("forwardPE")
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_rev_ttm(tkr) -> Optional[float]:
    # Intäkter TTM – plocka från financials/trailingTotalRevenue om möjligt.
    if tkr is None:
        return None
    # info
    try:
        info = tkr.info
        v = info.get("totalRevenue") or info.get("trailingTotalRevenue")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    # income stmt
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            if "Total Revenue" in fin.index:
                vals = fin.loc["Total Revenue"].dropna()
                if not vals.empty:
                    return float(vals.iloc[0])
    except Exception:
        pass
    return None

def _yf_ebitda_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("ebitda")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            if "Ebitda" in fin.index:
                vals = fin.loc["Ebitda"].dropna()
                if not vals.empty:
                    return float(vals.iloc[0])
    except Exception:
        pass
    return None

def _yf_p_b(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("priceToBook")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_bvps(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("bookValue")
        if v and v == v:
            return float(v)
    except Exception:
        pass
    return None

def _yf_ev_rev(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("enterpriseToRevenue")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_ev_ebitda(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("enterpriseToEbitda")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_dividend_annual(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # info → trailingAnnualDividendRate
    try:
        info = tkr.info
        v = info.get("trailingAnnualDividendRate")
        if v and v >= 0:
            return float(v)
    except Exception:
        pass
    # dividends-serien → summera senaste 12m
    try:
        divs = tkr.dividends
        if divs is not None and not divs.empty:
            last_12m = divs[divs.index >= (dt.datetime.utcnow() - dt.timedelta(days=365))]
            s = float(last_12m.sum())
            if s >= 0:
                return s
    except Exception:
        pass
    return None

# ------------------------------
# Hämta ett paket för en ticker
# ------------------------------
def yahoo_fetch_for_ticker(sym: str) -> Dict[str, Any]:
    tkr = _yf_ticker(sym)
    out: Dict[str, Any] = {
        "Aktuell kurs": _yf_last_price(tkr),
        "Valuta": _yf_currency(tkr),
        "Utestående aktier": _yf_shares_out(tkr),
        "EPS TTM": _yf_eps_ttm(tkr),
        "PE TTM": _yf_pe_ttm(tkr),
        "PE FWD": _yf_pe_fwd(tkr),
        "Rev TTM": _yf_rev_ttm(tkr),
        "EBITDA TTM": _yf_ebitda_ttm(tkr),
        "EV/Revenue": _yf_ev_rev(tkr),
        "EV/EBITDA": _yf_ev_ebitda(tkr),
        "P/B": _yf_p_b(tkr),
        "BVPS": _yf_bvps(tkr),
        "Årlig utdelning": _yf_dividend_annual(tkr),
        # Dessa lämnas orörda här (kan hämtas från andra källor / manuellt):
        "Net debt": None,
        "EPS 1Y": None, "EPS 2Y": None,
        "Rev 1Y": None, "Rev 2Y": None,
        "Rev CAGR": None, "EPS CAGR": None,
        "Utdelning CAGR": None,
    }
    return out

# --------------------------------------------
# Försiktig skrivning till Data-blad per rad
# --------------------------------------------
def _apply_fetch_to_row(row: pd.Series, fetched: Dict[str, Any]) -> pd.Series:
    """
    Endast skriva över de fält som har icke-None och meningsfulla värden.
    Respekterar principen: skriv över endast det som kunde hämtas.
    """
    if not isinstance(row, pd.Series):
        row = pd.Series(row)

    for key, val in fetched.items():
        if key not in row.index:
            continue
        if val is None:
            continue
        # Om numeriskt: NaN/None skydd
        if isinstance(val, (int, float)) and not math.isfinite(float(val)):
            continue
        row[key] = val
    # Stämpla auto-källa/tid
    row["Senast auto uppdaterad"] = now_stamp()
    row["Auto källa"] = "Yahoo Finance"
    return row

def update_one_row_from_yahoo(df: pd.DataFrame, idx: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Uppdaterar en (1) rad i Data-bladet från Yahoo (om möjligt).
    Returnerar (df, status_dict).
    """
    if df is None or df.empty or idx < 0 or idx >= len(df):
        return df, {"ok": False, "msg": "Ogiltig radindex eller tom Data."}

    sym = str(df.at[idx, "Ticker"]).strip() if "Ticker" in df.columns else ""
    if not sym:
        return df, {"ok": False, "msg": "Saknar Ticker i vald rad."}

    try:
        fetched = yahoo_fetch_for_ticker(sym)
        row = df.iloc[idx].copy()
        row = _apply_fetch_to_row(row, fetched)
        df.iloc[idx] = row
        return df, {"ok": True, "msg": f"Uppdaterade {sym} från Yahoo."}
    except Exception as e:
        return df, {"ok": False, "msg": f"Fel vid uppdatering av {sym}: {e}"}

def mass_update_from_yahoo(df: pd.DataFrame, idx_list: List[int], sleep_sec: float = 1.0) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Massuppdaterar valda rader (en i taget) med fördröjning.
    Skriver inte till Sheets här (UI-delen sköter sparning).
    """
    if df is None or df.empty:
        return df, [{"ok": False, "msg": "Tom Data."}]
    logs: List[Dict[str, Any]] = []
    for i, idx in enumerate(idx_list, start=1):
        df, status = update_one_row_from_yahoo(df, idx)
        status["seq"] = f"{i}/{len(idx_list)}"
        logs.append(status)
        time.sleep(max(0.0, float(sleep_sec)))
    return df, logs

# (Slut Del 2/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 3/6: Beräkningsmotor (auto-val av metod & riktkurser)
#
#  - fetch_from_yahoo(): wrapper runt yahoo_fetch_for_ticker (Del 2)
#  - EPS-estimat från Yahoo (earnings_trend)
#  - AUTO-PROFIL: väljer vilka metodfamiljer som passar (per sektor/mått)
#  - Metodpriser: PE, EV/S, EV/EBITDA, P/B (+ placeholders för struktur)
#  - Multipel-decay & PE-ankare
#  - Fair Value = median över valda metodfamiljer (v3)
#  - Riktkurser 1–3 år = “bästa scenario” med MoS per bucket (A 5%, B 8%, C 12%)
#  - compute_methods_for_row() → DICT (targets + metadata + methods_df)
#  - compute_fair_values_for_row() → kompakt DICT för UI
# ============================================================

# -------------------------
# Wrapper: Del 2 → Del 3
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Mappa Del 2:s yahoo_fetch_for_ticker() till stabila nycklar för beräkningsmotorn.
    Alla värden är i aktiens handelsvaluta.
    """
    snap = yahoo_fetch_for_ticker(ticker)
    return {
        "price":            _f(snap.get("Aktuell kurs")),
        "currency":         (snap.get("Valuta") or "USD"),
        "shares_out":       _f(snap.get("Utestående aktier")),
        "net_debt":         _f(snap.get("Net debt")),
        "rev_ttm":          _f(snap.get("Rev TTM")),
        "ebitda_ttm":       _f(snap.get("EBITDA TTM")),
        "eps_ttm":          _f(snap.get("EPS TTM")),
        "pe_ttm":           _f(snap.get("PE TTM")),
        "pe_fwd":           _f(snap.get("PE FWD")),
        "ev_rev":           _f(snap.get("EV/Revenue")),
        "ev_ebitda":        _f(snap.get("EV/EBITDA")),
        "p_b":              _f(snap.get("P/B")),
        "bvps":             _f(snap.get("BVPS")),
        "dps_annual":       _f(snap.get("Årlig utdelning")),
        # Historiska CAGRs kan saknas i Del 2; beräkningsmotor hanterar None.
        "rev_cagr_hist":    _f(snap.get("Rev CAGR")),
        "eps_cagr_hist":    _f(snap.get("EPS CAGR")),
    }

# -------------------------
# Clamp-gränser (stabila)
# -------------------------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

# -------------------------
# Små hjälpare (beräkning)
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
    try:
        eb = float(ebitda)
        if not math.isfinite(eb) or eb <= 0:
            return None
    except Exception:
        return None
    return eb * m

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

    # Välj första tillgängliga tillväxtindikator
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
    b0 = _f(ebitda_ttm)
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
# EPS-estimat från Yahoo
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
# AUTO-PROFIL: välj metodfamiljer som passar
# -------------------------
def _auto_method_profile(row: pd.Series, y_snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returnerar vilka metodfamiljer som ska användas för FV-medianen.
    Familjer: 'pe', 'ev_s', 'ev_e', 'pb'
    Beslut baseras på Sektor + måtttillgänglighet + tecken på tidigt skede.
    """
    sektor = str(_nz(row.get("Sektor"), "")).lower()
    ticker = str(_nz(row.get("Ticker"), "")).upper()

    # Datatillgänglighet
    eps_ttm    = _pos(_nz(y_snap.get("eps_ttm"), row.get("EPS TTM")))
    pe_ttm     = _pos(_nz(y_snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(y_snap.get("pe_fwd"), row.get("PE FWD")))
    rev_ttm    = _pos(_nz(y_snap.get("rev_ttm"), row.get("Rev TTM")))
    ebitda_ttm = _pos(_nz(y_snap.get("ebitda_ttm"), row.get("EBITDA TTM")))
    ev_rev     = _pos(_nz(y_snap.get("ev_rev"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(y_snap.get("ev_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(y_snap.get("p_b"), row.get("P/B")))
    bvps       = _pos(_nz(y_snap.get("bvps"), row.get("BVPS")))

    # Heuristik: klassificera
    is_financial  = any(k in sektor for k in ("finans", "financial", "bank", "insurance", "försäkring"))
    is_reit       = any(k in sektor for k in ("reit", "fastighet", "real estate"))
    is_utility    = any(k in sektor for k in ("utility", "verk", "kraft", "försörjn"))
    is_energy     = any(k in sektor for k in ("energy", "olja", "gas", "oil", "gas"))
    is_industrial = any(k in sektor for k in ("industr", "capital goods", "machinery", "transport", "marine", "shipping"))
    is_tech       = any(k in sektor for k in ("tech", "software", "internet", "semiconductor", "it"))
    is_health     = any(k in sektor for k in ("health", "biotech", "pharma", "medtech"))
    # Tickers som ofta är BDC/mREIT (proxy → P/B)
    bdc_mreit_tickers = {"AGNC","ARR","DX","EFC","NLY","ORC","RITM","CSWC","PFLT","HRZN","ARCC","MAIN"}

    # Grund-allow baserat på data
    allow = {
        "pe":   (eps_ttm is not None) and (pe_ttm is not None or pe_fwd is not None) and (eps_ttm > 0),
        "ev_s": (rev_ttm is not None) and (ev_rev is not None),
        "ev_e": (ebitda_ttm is not None) and (ebitda_ttm > 0) and (ev_ebitda is not None),
        "pb":   (p_b is not None) and (p_b > 0) and (bvps is not None) and (bvps > 0),
    }

    # Sektor-skift
    if is_financial or ticker in bdc_mreit_tickers:
        # Finans/BDC/mREIT → P/B primärt, PE sekundärt (om lönsam), undvik EV-mått
        allow["ev_s"] = False
        allow["ev_e"] = False
        # PE bara om positiv EPS
        allow["pe"] = allow["pe"] and (eps_ttm and eps_ttm > 0)
    elif is_reit:
        # REIT/fastigheter → P/B + EV/EBITDA om möjligt, undvik EV/S
        allow["ev_s"] = False
        # behåll pb & ev_e enligt data
    elif is_utility or is_energy or is_industrial:
        # Tillgångstunga/cykliska → EV/EBITDA + PE; EV/S ok men inte primär
        # behåll datadrivna allow, men kräver inte EV/S
        pass
    elif is_tech or is_health:
        # Tidigt skede/loss-making → EV/S prioriteras; PE om positiv EPS
        # behåll allow enligt data; om EPS ≤ 0 → slå av PE
        if not (eps_ttm and eps_ttm > 0):
            allow["pe"] = False
        # EV/EBITDA kräver positiv EBITDA — redan hanterat via data
    # Övriga sektorer → data-drivet som default

    # Fallback: om allt råkar bli avstängt, försök välja ett rimligt spår
    if not any(allow.values()):
        if (rev_ttm is not None) and (ev_rev is not None):
            allow["ev_s"] = True
        elif (eps_ttm is not None) and (eps_ttm > 0) and (pe_ttm is not None or pe_fwd is not None):
            allow["pe"] = True
        elif (p_b is not None) and (p_b > 0) and (bvps is not None) and (bvps > 0):
            allow["pb"] = True

    # Primär (för etikett/diagnostik)
    prefer_order = ["pe","ev_e","ev_s","pb"] if (is_utility or is_energy or is_industrial) else ["pe","ev_s","ev_e","pb"]
    if is_financial or is_reit or (ticker in bdc_mreit_tickers):
        prefer_order = ["pb","pe","ev_e","ev_s"]
    primary = next((fam for fam in prefer_order if allow.get(fam)), None)

    why = f"auto_profile: sektor='{sektor or '—'}', ticker='{ticker}', allow={{{', '.join([f'{k}:{'✓' if v else '×'}' for k,v in allow.items()])}}}, primary='{primary or '—'}'"
    return {"allow": allow, "primary": primary, "why": why}

# -------------------------
# Fair Value via familjemedian (v3 med filtrering)
# -------------------------
def _compute_fair_value_row_v3(methods_df: pd.DataFrame, now_price: Optional[float], allow_fams: Dict[str, bool]) -> Dict[str, Any]:
    """
    Median över *tillåtna* metodfamiljer:
      • 'pe_hist_vs_eps'          → fam 'pe'
      • 'ev_sales'                → fam 'ev_s'
      • 'ev_ebitda','ev_dacf'     → fam 'ev_e' (en gång)
      • 'p_b'                     → fam 'pb'
    Regler:
      • Dubbletter inom familj ignoreras.
      • Endast familjer där allow_fams[fam] == True räknas.
      • 'Idag': filtrera bort värden ≈ aktuell kurs (±0,5 %).
        Fall-back till 'pe_hist_vs_eps' om allt filtreras bort och 'pe' är tillåten.
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
            fam = fam_map.get(m, m)
            if fam in used_fams:
                continue
            if not allow_fams.get(fam, False):
                continue
            v = _f(r.get(c))
            if v is None:
                continue
            # Filtrera kurs-kopior i "Idag"
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:
                    continue
            used_fams.add(fam)
            vals.append(float(v))

        if not vals:
            # Fall-back: PE-raden om den finns och 'pe' är tillåten
            try:
                if allow_fams.get("pe", False):
                    row_pe = methods_df[methods_df["Metod"] == "pe_hist_vs_eps"].iloc[0]
                    out[c] = _f(row_pe.get(c))
                else:
                    out[c] = np.nan
            except Exception:
                out[c] = np.nan
        else:
            out[c] = float(np.median(vals))
    return out

# -------------------------
# Bucket → Margin of Safety
# -------------------------
def _mos_for_bucket(bucket_label: Any) -> float:
    """
    Returnerar MoS (0.05/0.08/0.12) enligt:
      Bucket A → 5%, Bucket B → 8%, Bucket C → 12%.
    Okänt → 8% (mitten).
    """
    s = str(bucket_label or "").lower()
    if "bucket a" in s:
        return 0.05
    if "bucket b" in s:
        return 0.08
    if "bucket c" in s:
        return 0.12
    return 0.08

def _best_case_row(methods_df: pd.DataFrame, allow_fams: Dict[str,bool]) -> Dict[str, Any]:
    """
    'Bästa scenario' = max-pris över tillåtna familjer per horisont.
    """
    fam_ok = {"pe_hist_vs_eps":"pe", "ev_sales":"ev_s", "ev_ebitda":"ev_e", "ev_dacf":"ev_e", "p_b":"pb"}
    cols = ["Idag", "1 år", "2 år", "3 år"]
    base = {"Metod": "best_case"}
    if methods_df is None or (hasattr(methods_df, "empty") and methods_df.empty):
        return {**base, **{c: np.nan for c in cols}}
    sub = methods_df[methods_df["Metod"].map(lambda m: allow_fams.get(fam_ok.get(str(m), ""), False))].copy()
    for c in cols:
        try:
            vals = [float(v) for v in sub[c].tolist() if _f(v) is not None]
            base[c] = (max(vals) if vals else np.nan)
        except Exception:
            base[c] = np.nan
    return base

# -------------------------
# Huvud: compute_methods_for_row → DICT (auto-profil)
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str] | None = None, fx_map: Dict[str, float] | None = None) -> Dict[str, Any]:
    """
    Returnerar en DICT som funkar både för Ranking-sidan och analysvyer:
      {
        "Metod": "fair_value_v3_auto",
        "target_today": float|None,  # = Fair Value idag (ingen MoS)
        "target_1y":    float|None,  # = Best case 1y * (1 - MoS bucket)
        "target_2y":    float|None,  # = Best case 2y * (1 - MoS bucket)
        "target_3y":    float|None,  # = Best case 3y * (1 - MoS bucket)
        "bull_1y": None, "bear_1y": None,
        "method": "fair_value_v3_auto",
        "Input-sammanfattning": "...",
        "note": "",
        "currency": "USD",
        "price": 123.45,
        "shares_out": ...,
        "net_debt": ...,
        "pe_anchor": ...,
        "decay": ...,
        "methods_df": <DataFrame>
      }
    Alla target i aktiens handelsvaluta.
    """
    settings = settings or get_settings_map()

    ticker = str(row.get("Ticker", "")).strip()
    y = fetch_from_yahoo(ticker)
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

    # --- AUTO-PROFIL: vilka familjer ska räknas in? ---
    profile = _auto_method_profile(row, y)
    allow_fams = profile["allow"]

    # --- Priser per metod (alla i aktiens valuta) ---
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
        "Metod": "p_b",
        "Idag": _price_from_pb(pb0, bvps),
        "1 år": _price_from_pb(pb1, bvps),
        "2 år": _price_from_pb(pb2, bvps),
        "3 år": _price_from_pb(pb3, bvps),
    })
    # Platshållare för struktur
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # --- Fair Value (familjemedian, filtrerad av auto-profil) = IDAG ---
    fv_row = _compute_fair_value_row_v3(methods_df, price, allow_fams)
    # --- Bästa scenario (max per horisont över tillåtna familjer) ---
    best_row = _best_case_row(methods_df, allow_fams)

    # --- Margin of Safety per bucket för framtiden ---
    bucket_label = str(_nz(row.get("Bucket"), "") or "")
    mos = _mos_for_bucket(bucket_label)
    best_mos_row = {
        "Metod": "best_case_MoS",
        "Idag": _f(fv_row.get("Idag")),  # ingen MoS på dagens fair value
        "1 år": (_f(best_row.get("1 år")) * (1.0 - mos)) if _f(best_row.get("1 år")) is not None else np.nan,
        "2 år": (_f(best_row.get("2 år")) * (1.0 - mos)) if _f(best_row.get("2 år")) is not None else np.nan,
        "3 år": (_f(best_row.get("3 år")) * (1.0 - mos)) if _f(best_row.get("3 år")) is not None else np.nan,
    }

    # Sätt ihop metodtabellen i tydlig ordning
    methods_df = pd.concat([pd.DataFrame([fv_row]), pd.DataFrame([best_row]), pd.DataFrame([best_mos_row]), methods_df], ignore_index=True)

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
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}, "
        f"bucket='{bucket_label or '—'}' → MoS={int(mos*100)}%, "
        f"{profile['why']}"
    )

    # --- Targets att skriva till Data-bladet ---
    target_today = _f(fv_row.get("Idag"))
    target_1y    = _f(best_mos_row.get("1 år")) if _f(best_mos_row.get("1 år")) is not None else _f(fv_row.get("1 år"))
    target_2y    = _f(best_mos_row.get("2 år")) if _f(best_mos_row.get("2 år")) is not None else _f(fv_row.get("2 år"))
    target_3y    = _f(best_mos_row.get("3 år")) if _f(best_mos_row.get("3 år")) is not None else _f(fv_row.get("3 år"))

    payload: Dict[str, Any] = {
        "Metod": "fair_value_v3_auto",
        "method": "fair_value_v3_auto",
        "target_today": target_today,  # Fair value idag (ingen MoS)
        "target_1y":    target_1y,     # Best case – MoS
        "target_2y":    target_2y,     # Best case – MoS
        "target_3y":    target_3y,     # Best case – MoS
        "bull_1y": None,
        "bear_1y": None,
        "Input-sammanfattning": sanity,
        "note": profile.get("primary") or "",
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "methods_df": methods_df,
    }
    return payload

# -------------------------
# Kompakt extraktor (FV) för UI
# -------------------------
def compute_fair_values_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Dict[str, Any]:
    """
    Beräknar metoder för en rad och returnerar en kompakt dict för UI:
      {
        'ticker': 'AAPL',
        'price':  195.12,
        'currency': 'USD',
        'fv_today':  Fair Value idag (utan MoS),
        'fv_1y':     Best case 1y – MoS(bucket),
        'fv_2y':     Best case 2y – MoS(bucket),
        'fv_3y':     Best case 3y – MoS(bucket),
        'sanity': '...',
        'methods_df': <DataFrame>
      }
    """
    payload = compute_methods_for_row(row, settings, fx_map)
    return {
        "ticker": str(row.get("Ticker") or "").upper(),
        "price": _f(payload.get("price")),
        "currency": (payload.get("currency") or "USD"),
        "fv_today": _f(payload.get("target_today")),
        "fv_1y": _f(payload.get("target_1y")),
        "fv_2y": _f(payload.get("target_2y")),
        "fv_3y": _f(payload.get("target_3y")),
        "sanity": payload.get("Input-sammanfattning", ""),
        "methods_df": payload.get("methods_df"),
    }
# (Slut Del 3/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 4/6: UI — Analysvy & Ranking (Fair Value-kort)
#
#  - view_analys(): välj bolag, räkna ut FV och visa metodtabell
#  - view_ranking(): kompakt rankingtabell baserad på FV vs pris
#  - Allt i aktiens handelsvaluta; ingen FX på EPS/riktkurser
#  - Enkel tabell med hela databasen längst ner (ofiltrerad)
# ============================================================

# -----------------------------------
# Små UI-hjälpare (format & metrik)
# -----------------------------------
def _fmt_num(x: Any, nd: int = 2) -> str:
    v = _f(x)
    if v is None or not math.isfinite(v):
        return "—"
    try:
        return f"{v:.{nd}f}"
    except Exception:
        return str(v)

def _fmt_pct(x: Any, nd: int = 1) -> str:
    v = _f(x)
    if v is None or not math.isfinite(v):
        return "—"
    return f"{v*100:.{nd}f}%"

def _upside_ratio(price: Optional[float], target: Optional[float]) -> Optional[float]:
    p = _pos(price); t = _pos(target)
    if p is None or t is None or p <= 0:
        return None
    return (t - p) / p

# -----------------------------------
# FV-kort (huvudpanel för ett bolag)
# -----------------------------------
def _render_fv_card(ticker: str, payload: Dict[str, Any]) -> None:
    currency = (payload.get("currency") or "USD")
    price    = _f(payload.get("price"))
    t0 = _f(payload.get("target_today"))
    t1 = _f(payload.get("target_1y"))
    t2 = _f(payload.get("target_2y"))
    t3 = _f(payload.get("target_3y"))

    u0 = _upside_ratio(price, t0)
    u1 = _upside_ratio(price, t1)
    u2 = _upside_ratio(price, t2)
    u3 = _upside_ratio(price, t3)

    st.markdown(f"### {ticker} · {currency}")
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1.2, 1.2, 1.2])
    with c1:
        st.metric("Aktuell kurs", f"{_fmt_num(price)} {currency}")
    with c2:
        st.metric("Fair Value (idag)", f"{_fmt_num(t0)} {currency}", _fmt_pct(u0) if u0 is not None else None)
    with c3:
        st.metric("Riktkurs 1 år (MoS)", f"{_fmt_num(t1)} {currency}", _fmt_pct(u1) if u1 is not None else None)
    with c4:
        st.metric("Riktkurs 2 år (MoS)", f"{_fmt_num(t2)} {currency}", _fmt_pct(u2) if u2 is not None else None)
    with c5:
        st.metric("Riktkurs 3 år (MoS)", f"{_fmt_num(t3)} {currency}", _fmt_pct(u3) if u3 is not None else None)

    # Diagnostikrad
    sanity = str(payload.get("Input-sammanfattning") or "")
    with st.expander("Diagnostik & indata", expanded=False):
        st.write(sanity)

    # Metodtabell (2 decimaler)
    methods_df = payload.get("methods_df")
    if methods_df is not None and not (hasattr(methods_df, "empty") and methods_df.empty):
        df_show = methods_df.copy()
        for col in ["Idag", "1 år", "2 år", "3 år"]:
            if col in df_show.columns:
                df_show[col] = df_show[col].map(lambda v: None if _f(v) is None else float(f"{_f(v):.2f}"))
        st.markdown("#### Metoder (alla i aktiens valuta)")
        st.dataframe(df_show, use_container_width=True, hide_index=True)
    else:
        st.info("Inga metoder att visa för detta bolag (saknar nödvändig data).")

# -----------------------------------
# Analys-vy (per bolag)
# -----------------------------------
def view_analys(data_df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.header("Analys")
    if data_df is None or (hasattr(data_df, "empty") and data_df.empty):
        st.info("Ingen data ännu. Lägg till bolag i vyn ”Lägg till / uppdatera”.")
        return

    # Val av bolag
    tickers = sorted([str(t) for t in data_df["Ticker"].dropna().unique().tolist()])
    if not tickers:
        st.info("Hittade inga tickers i databasen.")
        return

    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        ticker = st.selectbox("Välj bolag", tickers, index=0)
    with col_btn:
        st.write("")  # spacing

    # Hämta rad och beräkna
    try:
        row = data_df.loc[data_df["Ticker"].astype(str) == str(ticker)].iloc[0]
    except Exception:
        st.error("Kunde inte läsa vald rad från databasen.")
        return

    payload = compute_methods_for_row(row, settings, fx_map)
    _render_fv_card(ticker, payload)

    st.divider()
    st.subheader("Hela databasen (enkel tabell)")
    st.caption("Ofiltrerad översikt — alla kolumner. (Scrolla i sidled vid behov.)")
    st.dataframe(data_df, use_container_width=True)

# -----------------------------------
# Ranking-vy (kompakt)
# -----------------------------------
def _build_ranking_table(data_df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> pd.DataFrame:
    rows = []
    for _, r in data_df.iterrows():
        try:
            fv = compute_fair_values_for_row(r, settings, fx_map)
            price = _f(fv.get("price"))
            fv0   = _f(fv.get("fv_today"))
            fv1   = _f(fv.get("fv_1y"))
            cur = {
                "Ticker": fv.get("ticker"),
                "Valuta": fv.get("currency"),
                "Aktuell kurs": price,
                "Fair Value (idag)": fv0,
                "Riktkurs 1 år (MoS)": fv1,
                "Uppsida % (FV idag)": _upside_ratio(price, fv0),
                "Uppsida % (1 år)": _upside_ratio(price, fv1),
            }
            rows.append(cur)
        except Exception:
            # Fortsätt även om en rad fallerar
            pass
    df = pd.DataFrame(rows)
    if not df.empty:
        # Sortera på störst uppsida mot FV idag
        df = df.sort_values(by="Uppsida % (FV idag)", ascending=False, na_position="last").reset_index(drop=True)
        # Formatera två decimaler (utom procentkolumner som vi visar som tal; UI kan visa procent via st.dataframe)
        for c in ["Aktuell kurs", "Fair Value (idag)", "Riktkurs 1 år (MoS)"]:
            if c in df.columns:
                df[c] = df[c].map(lambda v: None if _f(v) is None else float(f"{_f(v):.2f}"))
        # Procent som två decimaler för tydlighet (siffror, inte strängar)
        for c in ["Uppsida % (FV idag)", "Uppsida % (1 år)"]:
            if c in df.columns:
                df[c] = df[c].map(lambda v: None if _f(v) is None else float(f"{_f(v)*100:.2f}"))
    return df

def view_ranking(data_df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.header("Ranking (Fair Value)")
    if data_df is None or (hasattr(data_df, "empty") and data_df.empty):
        st.info("Ingen data i databasen ännu.")
        return

    with st.spinner("Beräknar ranking…"):
        rank_df = _build_ranking_table(data_df, settings, fx_map)

    if rank_df is None or (hasattr(rank_df, "empty") and rank_df.empty):
        st.info("Kunde inte beräkna ranking (saknar nödvändig data).")
        return

    st.dataframe(rank_df, use_container_width=True, hide_index=True)
    st.caption("Uppsida visas i procent. Alla belopp i respektive akties handelsvaluta.")
# (Slut Del 4/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 5/6: Settings, Snapshot, Editor, Lägg till, Portfölj,
#          Massuppdatering & Köpförslag (UI)
#
#  - Lagrar/läser allt via schema från Del 1
#  - Använder Yahoo-hämtning (Del 3) och beräkningsmotor (Del 3)
#  - Visar hela databasen som enkel tabell i relevanta vyer
# ============================================================

# -------------------------
# Små UI-hjälpare (sök + nav)
# -------------------------
if "_names_map_from_df" not in globals():
    def _names_map_from_df(df: pd.DataFrame) -> Dict[str, str]:
        out = {}
        if df is None or df.empty:
            return out
        for _, r in df.iterrows():
            t = str(r.get("Ticker") or "").upper().strip()
            n = str(r.get("Bolagsnamn") or "").strip()
            if t:
                out[t] = f"{t} — {n}" if n else t
        return out

if "_select_with_search_nav" not in globals():
    def _select_with_search_nav(label: str, options: List[str], names_map: Dict[str, str],
                                session_idx_key: str, query_key: str) -> Optional[str]:
        if not options:
            st.info("Inga alternativ.")
            return None
        options = sorted(list({o.upper().strip() for o in options if o}))
        if session_idx_key not in st.session_state:
            st.session_state[session_idx_key] = 0
        st.session_state[session_idx_key] = max(0, min(st.session_state[session_idx_key], len(options)-1))

        q = st.text_input("Sök (ticker/namn)", key=query_key)
        if q:
            ql = q.lower().strip()
            shown = [o for o in options if (ql in o.lower()) or (ql in names_map.get(o, o).lower())]
            if not shown:
                shown = options
        else:
            shown = options

        pretty = [names_map.get(o, o) for o in shown]
        idx = min(st.session_state[session_idx_key], len(shown)-1)
        picked_pretty = st.selectbox(label, pretty, index=idx)
        picked = shown[pretty.index(picked_pretty)] if picked_pretty in pretty else shown[idx]

        c1, c2, c3 = st.columns([1, 1, 6])
        with c1:
            if st.button("◀︎", use_container_width=True, disabled=len(shown) <= 1):
                st.session_state[session_idx_key] = (shown.index(picked) - 1) % len(shown)
        with c2:
            if st.button("▶︎", use_container_width=True, disabled=len(shown) <= 1):
                st.session_state[session_idx_key] = (shown.index(picked) + 1) % len(shown)
        with c3:
            st.caption(f"{shown.index(picked)+1}/{len(shown)}")
        return picked

# Om Del 4 inte definierade en generell tabellvisare
if "_show_df" not in globals():
    def _show_df(df: pd.DataFrame, height: int = 360, use_container_width: bool = True) -> None:
        try:
            st.dataframe(df, use_container_width=use_container_width, height=height)
        except Exception:
            st.table(df.head(200))


# ============================================================
# ⚙️ Settings (redigerbar)
# ============================================================
def page_settings():
    st.header("⚙️ Settings")
    s_df = _read_df(SETTINGS_TITLE)
    if s_df.empty:
        s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)

    st.caption("Redigera nedan och klicka **Spara**.")
    edited = st.data_editor(
        s_df,
        num_rows="dynamic",
        use_container_width=True,
        key="settings_editor",
    )

    if st.button("💾 Spara Settings"):
        try:
            _write_df(SETTINGS_TITLE, edited[SETTINGS_COLUMNS])
            st.cache_data.clear()
            st.session_state["SETTINGS_MAP"] = get_settings_map()
            st.success("Settings sparade.")
        except Exception as e:
            st.error(f"Kunde inte spara: {e}")


# ============================================================
# 🕒 Snapshot (read-only)
# ============================================================
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    _show_df(snap, height=420, use_container_width=True)


# ============================================================
# ✏️ Editor (manuellt + Yahoo-prefill)
# ============================================================
def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad",
            "Rev 2Y uppdaterad","Senast manuellt uppdaterad"]
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *cols])
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _build_updates_from_yahoo(tkr: str, existing_row: pd.Series) -> Dict[str, Any]:
    y   = fetch_from_yahoo(tkr)
    try:
        est = _fetch_eps_estimates_yahoo(tkr)
    except Exception:
        est = {"eps_1y": None, "eps_2y": None}
    updates = {
        "Timestamp": now_stamp(),
        "Aktuell kurs": _f(y.get("price")),
        "Valuta": (y.get("currency") or existing_row.get("Valuta")),
        "Utestående aktier": _f(y.get("shares_out")),
        "Net debt": _f(y.get("net_debt")),
        "Rev TTM": _f(y.get("rev_ttm")),
        "EBITDA TTM": _f(y.get("ebitda_ttm")),
        "EPS TTM": _f(y.get("eps_ttm")),
        "PE TTM": _f(y.get("pe_ttm")),
        "PE FWD": _f(y.get("pe_fwd")),
        "EV/Revenue": _f(y.get("ev_rev")),
        "EV/EBITDA": _f(y.get("ev_ebitda")),
        "P/B": _f(y.get("p_b")),
        "BVPS": _f(y.get("bvps")),
        "Rev CAGR": _f(y.get("rev_cagr_hist")),
        "EPS CAGR": _f(y.get("eps_cagr_hist")),
        "Årlig utdelning": _f(y.get("dps_annual")),
        "EPS 1Y": existing_row.get("EPS 1Y") if pd.notna(existing_row.get("EPS 1Y")) else _f(est.get("eps_1y")),
        "EPS 2Y": existing_row.get("EPS 2Y") if pd.notna(existing_row.get("EPS 2Y")) else _f(est.get("eps_2y")),
        "Senast auto uppdaterad": now_stamp(),
        "Auto källa": "Yahoo",
    }
    return {k:v for k,v in updates.items() if v is not None and not (isinstance(v, float) and pd.isna(v))}

def page_editor():
    st.header("✏️ Editor (manuellt + Yahoo)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    df = _ensure_editor_stamp_cols(df)
    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)

    sel = _select_with_search_nav("Välj rad", tickers, names_map, "editor_idx", "editor_q")
    if not sel:
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
        bucket_opts = [""] + DEFAULT_BUCKETS
        current_bucket = str(row.get("Bucket") or "")
        try:
            bucket_idx = bucket_opts.index(current_bucket) if current_bucket in bucket_opts else 0
        except Exception:
            bucket_idx = 0
        bucket_sel = st.selectbox("Bucket", bucket_opts, index=bucket_idx)
    with c2:
        eps1_in = st.text_input("EPS 1Y (estimat)", value=str(_f(row.get("EPS 1Y")) or ""))
        eps2_in = st.text_input("EPS 2Y (estimat)", value=str(_f(row.get("EPS 2Y")) or ""))
        rev1_in = st.text_input("Rev 1Y (miljoner)", value=str(_f(row.get("Rev 1Y")) or ""))
        rev2_in = st.text_input("Rev 2Y (miljoner)", value=str(_f(row.get("Rev 2Y")) or ""))

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara (session)"):
            try:
                df.loc[idx, "Ticker"] = str(new_ticker).upper().strip() or sel
                df.loc[idx, "Antal aktier"] = _f(antal_in) or 0.0
                if _f(gav_in) is not None:
                    df.loc[idx, "GAV (SEK)"] = _f(gav_in)
                if bucket_sel is not None:
                    df.loc[idx, "Bucket"] = bucket_sel if bucket_sel != "" else np.nan
                if _f(eps1_in) is not None:
                    df.loc[idx, "EPS 1Y"] = _f(eps1_in)
                if _f(eps2_in) is not None:
                    df.loc[idx, "EPS 2Y"] = _f(eps2_in)
                if _f(rev1_in) is not None:
                    df.loc[idx, "Rev 1Y"] = _f(rev1_in) * 1_000_000.0
                if _f(rev2_in) is not None:
                    df.loc[idx, "Rev 2Y"] = _f(rev2_in) * 1_000_000.0
                df.loc[idx, "Senast manuellt uppdaterad"] = now_stamp()
                st.session_state["DATA"] = df
                st.success("Sparat i session.")
            except Exception as e:
                st.error(f"Fel: {e}")

    with colB:
        if st.button("⬆️ Spara till Google Sheets + Yahoo-prefill"):
            try:
                tkr = str(_nz(df.loc[idx, "Ticker"], new_ticker or sel)).upper()
                updates = _build_updates_from_yahoo(tkr, df.loc[idx])

                df_cur = df.copy()
                for k, v in updates.items():
                    if k not in df_cur.columns:
                        df_cur[k] = np.nan
                    df_cur.at[idx, k] = v

                write_data_df(df_cur)
                st.session_state["DATA"] = df_cur
                st.success(f"{tkr}: Rad sparad och uppdaterad från Yahoo.")
            except Exception as e:
                st.error(f"Fel vid sparning: {e}")

    st.markdown("---")
    st.subheader("Förhandsgranskning")
    _show_df(df.loc[[idx]], height=240, use_container_width=True)


# ============================================================
# ➕ Lägg till ticker (med valfri Yahoo-prefill)
# ============================================================
def page_add_ticker():
    st.header("➕ Lägg till ticker")

    tkr = st.text_input("Ticker").upper().strip()
    c1, c2, c3 = st.columns(3)
    with c1:
        bolagsnamn = st.text_input("Bolagsnamn")
        sektor     = st.text_input("Sektor")
    with c2:
        bucket_sel = st.selectbox("Bucket", [""] + DEFAULT_BUCKETS, index=0)
        valuta     = st.text_input("Valuta (t.ex. USD)", value="USD").upper()
    with c3:
        antal = st.text_input("Antal aktier", value="")
        gav   = st.text_input("GAV (SEK)", value="")

    st.markdown("**Prognosfält (frivilliga)**")
    c4, c5 = st.columns(2)
    with c4:
        eps1_in = st.text_input("EPS 1Y (estimat)")
        rev1_in = st.text_input("Rev 1Y (miljoner)")
    with c5:
        eps2_in = st.text_input("EPS 2Y (estimat)")
        rev2_in = st.text_input("Rev 2Y (miljoner)")

    do_prefill = st.checkbox("Hämta & fyll från Yahoo", value=True)

    if st.button("💾 Lägg till"):
        if not tkr:
            st.warning("Ticker krävs.")
            return
        try:
            base_df = read_data_df()
            if not base_df.empty and (base_df["Ticker"].astype(str).str.upper() == tkr.upper()).any():
                st.error("Ticker finns redan i DATA.")
                return

            new_row = {c: np.nan for c in DATA_COLUMNS}
            new_row.update({
                "Timestamp": now_stamp(),
                "Ticker": tkr,
                "Bolagsnamn": bolagsnamn if bolagsnamn else np.nan,
                "Sektor": sektor if sektor else np.nan,
                "Bucket": bucket_sel if bucket_sel != "" else np.nan,
                "Valuta": valuta or "USD",
            })

            qty_v = _f(antal) or 0.0
            gav_v = _f(gav)
            new_row["Antal aktier"] = qty_v
            if gav_v is not None:
                new_row["GAV (SEK)"] = gav_v

            eps1_v  = _f(eps1_in)
            eps2_v  = _f(eps2_in)
            rev1_vm = (_f(rev1_in) or 0.0) * 1_000_000.0 if rev1_in not in (None,"") else None
            rev2_vm = (_f(rev2_in) or 0.0) * 1_000_000.0 if rev2_in not in (None,"") else None
            if eps1_v is not None: new_row["EPS 1Y"] = eps1_v
            if eps2_v is not None: new_row["EPS 2Y"] = eps2_v
            if rev1_vm is not None: new_row["Rev 1Y"] = rev1_vm
            if rev2_vm is not None: new_row["Rev 2Y"] = rev2_vm
            new_row["Senast manuellt uppdaterad"] = now_stamp()

            if do_prefill:
                try:
                    y = fetch_from_yahoo(tkr)
                    pre = {
                        "Aktuell kurs": _f(y.get("price")),
                        "Valuta": y.get("currency") or valuta,
                        "Utestående aktier": _f(y.get("shares_out")),
                        "Net debt": _f(y.get("net_debt")),
                        "Rev TTM": _f(y.get("rev_ttm")),
                        "EBITDA TTM": _f(y.get("ebitda_ttm")),
                        "EPS TTM": _f(y.get("eps_ttm")),
                        "PE TTM": _f(y.get("pe_ttm")),
                        "PE FWD": _f(y.get("pe_fwd")),
                        "EV/Revenue": _f(y.get("ev_rev")),
                        "EV/EBITDA": _f(y.get("ev_ebitda")),
                        "P/B": _f(y.get("p_b")),
                        "BVPS": _f(y.get("bvps")),
                        "Rev CAGR": _f(y.get("rev_cagr_hist")),
                        "EPS CAGR": _f(y.get("eps_cagr_hist")),
                        "Årlig utdelning": _f(y.get("dps_annual")),
                        "Senast auto uppdaterad": now_stamp(),
                        "Auto källa": "Yahoo",
                    }
                    new_row.update({k:v for k,v in pre.items() if v is not None})
                except Exception:
                    pass

            out_df = pd.concat([base_df, pd.DataFrame([new_row])], ignore_index=True)
            write_data_df(out_df)
            st.session_state["DATA"] = out_df
            st.success(f"{tkr} tillagd.")
        except Exception as e:
            st.error(f"Kunde inte lägga till: {e}")


# ============================================================
# 📦 Portfölj (innehav + kommande utdelningar)
# ============================================================
def _fx_rate_to_sek(currency: str, fx_map: Dict[str, float]) -> float:
    cur = (currency or "SEK").upper().strip()
    if cur == "SEK":
        return 1.0
    r = fx_map.get(cur)
    try:
        return float(r) if r is not None and math.isfinite(float(r)) and float(r) > 0 else 1.0
    except Exception:
        return 1.0

def _position_value_tables(df_data: pd.DataFrame, fx_map: Dict[str, float]) -> pd.DataFrame:
    cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Antal","Aktuell kurs","Värde (valuta)","Värde (SEK)"]
    rows = []
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols)

    base = df_data.copy()
    if "Antal aktier" in base.columns:
        base["Antal aktier"] = pd.to_numeric(base["Antal aktier"], errors="coerce")
    owned = base[(base.get("Antal aktier") > 0) if "Antal aktier" in base.columns else []].copy()

    for _, r in owned.iterrows():
        tkr = str(r.get("Ticker") or "").strip()
        if not tkr:
            continue
        name = str(_nz(r.get("Bolagsnamn"), ""))
        bucket = str(_nz(r.get("Bucket"), "") or "")
        ccy = str(_nz(r.get("Valuta"), "SEK")).upper()

        price = _f(r.get("Aktuell kurs"))
        qty = _pos(r.get("Antal aktier")) or 0.0
        fx  = _fx_rate_to_sek(ccy, fx_map)
        val_ccy = (price or 0.0) * qty
        val_sek = val_ccy * fx

        rows.append({
            "Ticker": tkr,
            "Bolagsnamn": name,
            "Bucket": bucket,
            "Valuta": ccy,
            "Antal": float(qty),
            "Aktuell kurs": _f(price),
            "Värde (valuta)": float(val_ccy),
            "Värde (SEK)": float(val_sek),
        })
    out = pd.DataFrame(rows, columns=cols)
    return out

def _guess_frequency(freq_raw: Any) -> Optional[int]:
    if freq_raw is None:
        return None
    try:
        n = int(freq_raw)
        return n if n in (1, 2, 4, 12) else None
    except Exception:
        pass
    s = str(freq_raw).strip().lower()
    if s in ("m", "monthly", "månad", "månatlig"): return 12
    if s in ("q", "quarterly", "kvartal", "kvartalsvis"): return 4
    if s in ("s", "semi", "semi-annual", "halvår", "halvårsvis"): return 2
    if s in ("a", "annual", "år", "årligen"): return 1
    return None

def _parse_date_any(x) -> Optional[dt.date]:
    if x is None or (isinstance(x, float) and (pd.isna(x) or math.isnan(x))):
        return None
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    if isinstance(x, dt.datetime):
        return x.date()
    try:
        d = pd.to_datetime(x, errors="coerce", utc=False)
        if pd.isna(d):
            return None
        if isinstance(d, pd.Timestamp):
            return d.date()
        return dt.datetime.fromtimestamp(d.astype("datetime64[s]").astype(int)).date()
    except Exception:
        pass
    try:
        s = str(x).strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return dt.datetime.strptime(s, fmt).date()
            except Exception:
                continue
    except Exception:
        return None
    return None

def _pick_next_pay_date(row: pd.Series) -> Optional[dt.date]:
    candidates = [
        "Nästa utdelningsdatum", "Utdelningsdatum nästa", "Next dividend date",
        "Next Pay Date", "Dividend Pay Date", "Pay Date", "Payment Date"
    ]
    for c in candidates:
        if c in row and (row[c] is not None) and (not (isinstance(row[c], float) and pd.isna(row[c]))):
            d = _parse_date_any(row[c])
            if d is not None:
                return d
    return None

def _next_dps_per_share(row: pd.Series) -> Optional[float]:
    for c in ("Nästa utdelning (per aktie)", "Utdelning nästa", "Next Dividend", "Next DPS", "Dividend Next"):
        if c in row and _f(row[c]) is not None:
            return float(_f(row[c]))

    annual = None
    for c in ("Årlig utdelning", "Dividend (Annual)", "DPS Annual", "Årsutdelning"):
        if c in row and _f(row[c]) is not None:
            annual = float(_f(row[c]))
            break

    if annual is None:
        return None

    freq = None
    for c in ("Utdelningsfrekvens", "Frekvens", "Frequency", "Dividend Frequency"):
        if c in row and row[c] is not None:
            freq = _guess_frequency(row[c])
            if freq:
                break
    if not freq:
        freq = 4
    try:
        return annual / float(freq) if float(freq) > 0 else None
    except Exception:
        return None

def build_next_dividends_table(data_df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, Any]) -> pd.DataFrame:
    rows = []
    today = dt.date.today()
    if data_df is None or data_df.empty:
        return pd.DataFrame(columns=["Datum","Ticker","Valuta","Antal","DPS nästa","Brutto","Källskatt","Netto","Netto SEK"])

    for _, r in data_df.iterrows():
        ticker = str(r.get("Ticker") or "").strip()
        if not ticker:
            continue

        shares = _pos(_nz(r.get("Antal aktier"), r.get("Shares")))
        if shares is None or shares <= 0:
            continue

        currency = str(_nz(r.get("Valuta"), "SEK")).upper()
        pay_date = _pick_next_pay_date(r)
        if pay_date is None or pay_date < today:
            continue

        dps_next = _next_dps_per_share(r)
        if dps_next is None or dps_next <= 0:
            continue

        code = (currency or "USD").upper()
        key  = f"withholding_{code}"
        try:
            wht = float(get_settings_map().get(key, "0.15"))
        except Exception:
            wht = 0.15

        fx  = _fx_rate_to_sek(currency, fx_map)

        brutto = dps_next * shares
        kalls  = brutto * wht
        netto  = brutto - kalls
        netto_sek = netto * fx

        rows.append({
            "Datum": pay_date,
            "Ticker": ticker,
            "Valuta": currency,
            "Antal": float(shares),
            "DPS nästa": float(dps_next),
            "Brutto": float(brutto),
            "Källskatt": float(kalls),
            "Netto": float(netto),
            "Netto SEK": float(netto_sek),
        })

    df = pd.DataFrame(rows, columns=["Datum","Ticker","Valuta","Antal","DPS nästa","Brutto","Källskatt","Netto","Netto SEK"])
    if df.empty:
        return df
    df = df.sort_values(["Datum", "Ticker"]).reset_index(drop=True)
    return df

def render_portfolio_dividends_section(data_df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, Any]) -> None:
    st.subheader("📅 Kommande utdelningar (nästa utbetalningsdatum)")
    nxt = build_next_dividends_table(data_df, fx_map, settings)

    if nxt.empty:
        st.info("Inga kommande utdelningsdatum hittades i databasen (eller alla har passerat).")
        st.caption("Tips: fyll i 'Nästa utdelningsdatum' och 'Nästa utdelning (per aktie)' i Data-bladet, "
                   "eller säkerställ 'Årlig utdelning' + frekvens.")
        return

    tot_netto_sek = float(nxt["Netto SEK"].sum())
    st.metric("Summa netto kommande (SEK)", f"{tot_netto_sek:,.2f}".replace(",", " ").replace(".", ","))

    df_show = nxt.copy()
    df_show["Datum"] = df_show["Datum"].astype(str)
    _show_df(df_show, height=300, use_container_width=True)

    with st.expander("Visa summering per månad (SEK, netto)"):
        try:
            g = nxt.copy()
            g["YYYY-MM"] = g["Datum"].astype(str).str.slice(0, 7)
            agg = g.groupby("YYYY-MM", as_index=False)["Netto SEK"].sum().sort_values("YYYY-MM")
            agg["Netto SEK"] = agg["Netto SEK"].map(lambda x: f"{x:,.2f}".replace(",", " ").replace(".", ","))
            _show_df(agg, height=240, use_container_width=True)
        except Exception:
            st.caption("Kunde inte göra månadssummering (saknade datum eller värden).")

def render_bucket_expandables(pos_df: pd.DataFrame, settings: Dict[str, str]) -> None:
    if pos_df is None or pos_df.empty:
        return
    buckets = [b for b in sorted(pos_df["Bucket"].dropna().unique().tolist()) if b]
    for b in buckets:
        sub = pos_df[pos_df["Bucket"] == b].copy().sort_values("Värde (SEK)", ascending=True)
        total = float(sub["Värde (SEK)"].sum()) if not sub.empty else 0.0
        with st.expander(f"{b} — värde {total:,.0f} SEK".replace(",", " "), expanded=False):
            show = sub[["Ticker","Bolagsnamn","Valuta","Antal","Aktuell kurs","Värde (valuta)","Värde (SEK)"]].copy()
            show["Andel i bucket (%)"] = show["Värde (SEK)"].map(lambda x: (x/total*100.0) if total>0 else np.nan)
            _show_df(show, height=260, use_container_width=True)

def page_portfolio():
    st.header("📦 Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx_map = st.session_state.get("FX", {}) or get_fx_map()
    settings = get_settings_map()

    pos = _position_value_tables(df, fx_map)
    if pos.empty:
        st.info("Inga innehav (Antal aktier <= 0).")
    else:
        tot_sek = float(pos["Värde (SEK)"].sum())
        st.metric("Totalt portföljvärde (SEK)", f"{tot_sek:,.0f}".replace(",", " "))
        _show_df(pos.sort_values(["Bucket","Värde (SEK)"]), height=320, use_container_width=True)
        st.markdown("#### Hinkar (Bucket) – innehåll")
        render_bucket_expandables(pos, settings)

    st.markdown("---")
    render_portfolio_dividends_section(df, fx_map, settings)


# ============================================================
# 🧩 Massuppdatering (Yahoo) — 1s per bolag
# ============================================================
def page_batch():
    st.header("🧩 Massuppdatering (Yahoo) — 1s per bolag")
    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    tickers = sorted(df["Ticker"].dropna().astype(str).unique().tolist())
    sel = st.multiselect("Välj tickers (tom = alla)", options=tickers, default=[])
    target = tickers if len(sel) == 0 else sel

    delay = st.slider("Fördröjning per bolag (sek)", 0.5, 5.0, 1.0, 0.5)
    go = st.button("🚀 Starta")

    if not go:
        return

    df_cur = df.copy()
    progress = st.progress(0.0)
    status = st.empty()
    changed_total = 0

    for i, tkr in enumerate(target, start=1):
        try:
            status.write(f"Uppdaterar {i}/{len(target)} – {tkr}")
            mask = df_cur["Ticker"].astype(str).str.upper() == str(tkr).upper()
            existing = df_cur[mask].iloc[0] if mask.any() else pd.Series({"Ticker": tkr})
            updates = _build_updates_from_yahoo(tkr, existing)

            if mask.any():
                idx = df_cur.index[mask][0]
                for k, v in updates.items():
                    if k not in df_cur.columns:
                        df_cur[k] = np.nan
                    old = df_cur.at[idx, k]
                    same = (pd.isna(old) and pd.isna(v)) or (not pd.isna(old) and not pd.isna(v) and str(old) == str(v))
                    if same:
                        continue
                    df_cur.at[idx, k] = v
                    changed_total += 1
            else:
                base = {c: np.nan for c in DATA_COLUMNS}
                base.update({"Timestamp": now_stamp(), "Ticker": tkr})
                base.update(updates)
                df_cur = pd.concat([df_cur, pd.DataFrame([base])], ignore_index=True)
                changed_total += len(updates)
        except Exception as e:
            st.error(f"{tkr}: {e}")
        progress.progress(i/len(target))
        time.sleep(float(delay))

    write_data_df(df_cur)
    st.session_state["DATA"] = df_cur
    progress.empty()
    status.empty()
    st.success(f"Klar. {len(target)} bolag uppdaterade. {changed_total} fält ändrades.")


# ============================================================
# 🛒 Köpförslag (läser Data-bladet; ingen skrivning)
# ============================================================
def _cap_for_bucket(bucket_label: str, settings: Dict[str, str]) -> Optional[float]:
    s = (bucket_label or "").lower().replace("tillväxt","tillvaxt").strip()
    mapping = {
        "bucket a tillväxt":"bucket_cap_A_tillvaxt",
        "bucket b tillväxt":"bucket_cap_B_tillvaxt",
        "bucket c tillväxt":"bucket_cap_C_tillvaxt",
        "bucket a utdelning":"bucket_cap_A_utdelning",
        "bucket b utdelning":"bucket_cap_B_utdelning",
        "bucket c utdelning":"bucket_cap_C_utdelning",
    }
    k = mapping.get(s)
    if not k:
        return None
    v = _f(settings.get(k))
    return float(v) if v is not None else None

def _quick_pos_lookup(df: pd.DataFrame, fx_map: Dict[str, float]) -> dict[str, dict]:
    out = {}
    pos = _position_value_tables(df, fx_map)
    for _, r in pos.iterrows():
        out[str(r["Ticker"]).upper()] = {
            "value_sek": _f(r["Värde (SEK)"]) or 0.0,
            "qty": _f(r["Antal"]) or 0.0,
            "currency": str(r.get("Valuta") or "SEK").upper(),
            "price": _f(r.get("Aktuell kurs"))
        }
    return out

def build_buy_suggestions(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float],
                          own_filter: str = "Alla", fv_horizon: str = "Idag") -> pd.DataFrame:
    fv_col_map = {
        "Idag": "Riktkurs idag",
        "1 år": "Riktkurs 1 år",
        "2 år": "Riktkurs 2 år",
        "3 år": "Riktkurs 3 år",
    }
    fv_col = fv_col_map.get(fv_horizon, "Riktkurs idag")

    cols_out = [
        "Ticker","Bolagsnamn","Bucket","Valuta","Kurs",f"FV {fv_horizon}",
        "Uppsida (%)","Äger (antal)","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"
    ]
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols_out)

    base = df_data.copy()
    for c in ("Antal aktier","Aktuell kurs","Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"):
        if c in base.columns:
            base[c] = pd.to_numeric(base[c], errors="coerce")

    lu = _quick_pos_lookup(base, fx_map)
    rows = []
    for _, r in base.iterrows():
        try:
            tkr = str(r.get("Ticker") or "").upper().strip()
            if not tkr:
                continue
            bucket = str(_nz(r.get("Bucket"), "") or "")
            if not bucket:
                continue
            cap = _cap_for_bucket(bucket, settings)
            if cap is None or cap <= 0:
                continue

            price    = _f(r.get("Aktuell kurs"))
            fv_target = _f(r.get(fv_col))
            ccy      = (str(_nz(r.get("Valuta"), "SEK"))).upper()
            name     = str(_nz(r.get("Bolagsnamn"), ""))

            if not _pos(price) or not _pos(fv_target):
                continue
            if price >= fv_target:
                continue

            entry = lu.get(tkr, {"value_sek": 0.0, "qty": _f(r.get("Antal aktier")) or 0.0, "currency": ccy, "price": price})
            qty = entry["qty"] if entry["qty"] is not None else (_f(r.get("Antal aktier")) or 0.0)

            own_status = "own" if (qty and qty > 0) else "no_own"
            if own_filter == "Endast innehav" and own_status != "own":
                continue
            if own_filter == "Endast ej ägda" and own_status != "no_own":
                continue

            fx = _fx_rate_to_sek(ccy, fx_map)
            value_sek = float((price or 0.0) * (qty or 0.0) * fx)
            if _pos(value_sek) and value_sek >= cap:
                continue

            up_pct = (fv_target - price) / price * 100.0 if _pos(price) else None
            rows.append({
                "Ticker": tkr,
                "Bolagsnamn": name,
                "Bucket": bucket,
                "Valuta": ccy,
                "Kurs": price,
                f"FV {fv_horizon}": fv_target,
                "Uppsida (%)": up_pct,
                "Äger (antal)": qty or 0.0,
                "Värde (SEK)": value_sek or 0.0,
                "Cap per innehav (SEK)": cap,
                "Slack till cap (SEK)": (cap - (value_sek or 0.0)),
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=cols_out)

    out = pd.DataFrame(rows, columns=cols_out)
    out = out.sort_values(["Värde (SEK)", "Uppsida (%)"], ascending=[True, False]).reset_index(drop=True)
    return out

def page_buy_suggestions():
    st.header("🛒 Köpförslag (läser Data-bladet)")
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
    if df is None or df.empty:
        st.info("Ingen data.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    col_top1, col_top2 = st.columns([2,2])
    with col_top1:
        fv_horizon = st.selectbox("Riktkurs-horisont", ["Idag","1 år","2 år","3 år"], index=0)
    with col_top2:
        filt = st.radio("Visa", ["Alla","Endast innehav","Endast ej ägda"], index=0, horizontal=True)

    st.caption(f"Kriterier: **Aktuell kurs < Riktkurs {fv_horizon}** och **(innehavsvärde i SEK) < cap per innehav i bucket**.")

    with st.spinner("Hämtar förslag från Data-bladet…"):
        sug = build_buy_suggestions(df, settings, fx_map, own_filter=filt, fv_horizon=fv_horizon)

    if sug.empty:
        st.info("Inga kandidater uppfyller kriterierna just nu.")
        st.caption("Tips: kör **🏆 Ranking** först för att uppdatera riktkurserna i Data-bladet.")
        return

    st.caption(f"{len(sug)} förslag — sorterat minsta innehavet först.")
    show = sug.copy()
    if "Kurs" in show.columns:
        show["Kurs"] = show["Kurs"].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
    fv_col_label = f"FV {fv_horizon}"
    if fv_col_label in show.columns:
        show[fv_col_label] = show[fv_col_label].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
    for c in ("Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"):
        if c in show.columns:
            show[c] = show[c].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
    if "Uppsida (%)" in show.columns:
        show["Uppsida (%)"] = show["Uppsida (%)"].map(lambda v: f"{v:.1f}%" if v is not None else "—")

    _show_df(show, height=420, use_container_width=True)

    with st.expander("Summering per Bucket (antal förslag)"):
        agg = sug.groupby("Bucket", as_index=False).size().rename(columns={"size":"Antal förslag"})
        _show_df(agg, height=240, use_container_width=True)
# (Slut Del 5/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 6/6: 🏆 Ranking (riktkurser) + Main()
#
#  - Beräknar fair value Idag/1/2/3 år samt Bull/Bear 1 år
#  - Skriver riktkurserna till Data-bladet
#  - Huvudnavigering för alla vyer
# ============================================================

# --------------------------------------------
# Skydd: om compute_methods_for_row saknas (Del 3)
# --------------------------------------------
if "compute_methods_for_row" not in globals():
    def compute_methods_for_row(*args, **kwargs):
        raise RuntimeError(
            "compute_methods_for_row saknas. Säkerställ att Del 3/6 är korrekt inladdad."
        )

# --------------------------------------------
# Hjälpare för ranking
# --------------------------------------------
def _try_compute_methods(row: pd.Series) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    Anropar compute_methods_for_row robust, oavsett signatur (Series eller dict, med/utan settings).
    Returnerar (methods_df, meta). Vid fel: (None, {"error": str}).
    """
    try:
        # Vanligast: rad som Series
        try:
            return compute_methods_for_row(row)  # type: ignore
        except TypeError:
            pass
        # Alternativ 1: Series + settings
        try:
            return compute_methods_for_row(row, settings=get_settings_map())  # type: ignore
        except TypeError:
            pass
        # Alternativ 2: dict
        rdict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        try:
            return compute_methods_for_row(rdict)  # type: ignore
        except TypeError:
            # Alternativ 3: dict + settings
            return compute_methods_for_row(rdict, settings=get_settings_map())  # type: ignore
    except Exception as e:
        return None, {"error": str(e)}

def _extract_fair_values(methods_df: Optional[pd.DataFrame], meta: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """
    Försöker plocka ut fair value (idag/1y/2y/3y + bull/bear 1y) från meta i första hand.
    Om saknas, gör ett försök via methods_df (t.ex. rad 'fair_value' eller kolumner 'target_*').
    """
    out = {"today": None, "1y": None, "2y": None, "3y": None, "bull_1y": None, "bear_1y": None}

    fv = (meta or {}).get("fair_value", {})
    if isinstance(fv, Mapping):
        out["today"]   = _f(fv.get("today"))
        out["1y"]      = _f(fv.get("1y"))
        out["2y"]      = _f(fv.get("2y"))
        out["3y"]      = _f(fv.get("3y"))
        out["bull_1y"] = _f(fv.get("bull_1y"))
        out["bear_1y"] = _f(fv.get("bear_1y"))

    # Om något saknas, försök hämta från methods_df
    def _try_df(colnames: List[str]) -> Optional[float]:
        if methods_df is None or methods_df is np.nan:
            return None
        try:
            df = methods_df.copy()
            # Leta på en ev. 'fair_value'-rad
            method_col = None
            for cand in ("method", "Method", "modell", "Modell", "name", "Name"):
                if cand in df.columns:
                    method_col = cand
                    break
            if method_col:
                mask = df[method_col].astype(str).str.contains("fair", case=False, na=False)
                if mask.any():
                    r = df[mask].iloc[0]
                    for c in colnames:
                        if c in df.columns and _f(r.get(c)) is not None:
                            return _f(r.get(c))
            # Annars, ta median över metodkolumner om de finns
            for c in colnames:
                if c in df.columns:
                    vals = pd.to_numeric(df[c], errors="coerce").dropna()
                    if not vals.empty:
                        return float(vals.median())
        except Exception:
            return None
        return None

    # Fallbacks om saknas
    if out["today"]   is None: out["today"]   = _try_df(["target_today", "FV_today", "Fair Today"])
    if out["1y"]      is None: out["1y"]      = _try_df(["target_1y", "FV_1y", "Fair 1Y"])
    if out["2y"]      is None: out["2y"]      = _try_df(["target_2y", "FV_2y", "Fair 2Y"])
    if out["3y"]      is None: out["3y"]      = _try_df(["target_3y", "FV_3y", "Fair 3Y"])
    if out["bull_1y"] is None: out["bull_1y"] = _try_df(["bull_1y", "Bull 1Y"])
    if out["bear_1y"] is None: out["bear_1y"] = _try_df(["bear_1y", "Bear 1Y"])

    return out

def _r2(x: Optional[float]) -> Optional[float]:
    v = _f(x)
    if v is None: 
        return None
    try:
        return float(f"{float(v):.2f}")
    except Exception:
        return None

def _update_row_with_fair_values(df: pd.DataFrame, idx, fv: Dict[str, Optional[float]]) -> None:
    """
    Sätter riktkurskolumner i df på index idx om värden finns.
    Två decimaler, inget annat.
    """
    mapping = [
        ("Riktkurs idag", fv.get("today")),
        ("Riktkurs 1 år", fv.get("1y")),
        ("Riktkurs 2 år", fv.get("2y")),
        ("Riktkurs 3 år", fv.get("3y")),
        ("Bull 1 år",    fv.get("bull_1y")),
        ("Bear 1 år",    fv.get("bear_1y")),
    ]
    for col, val in mapping:
        if col not in df.columns:
            df[col] = np.nan
        v2 = _r2(val)
        if v2 is not None:
            df.at[idx, col] = v2

    # Stämpel
    col_stamp = "Senast rankad"
    if col_stamp not in df.columns:
        df[col_stamp] = np.nan
    df.at[idx, col_stamp] = now_stamp()

def _rank_one(row: pd.Series) -> Dict[str, Any]:
    """
    Rankar en rad → returnerar sammanfattning (för visning i tabellen).
    """
    tkr = str(row.get("Ticker") or "").upper().strip()
    ccy = str(_nz(row.get("Valuta"), "SEK")).upper()
    price = _f(row.get("Aktuell kurs"))

    methods_df, meta = _try_compute_methods(row)
    if isinstance(meta, dict) and meta.get("error"):
        return {"Ticker": tkr, "Valuta": ccy, "Kurs": price, "FV idag": None, "FV 1 år": None, "FV 2 år": None, "FV 3 år": None,
                "Bull 1 år": None, "Bear 1 år": None, "Uppsida (%)": None, "Status": f"Fel: {meta.get('error')}"}

    fv = _extract_fair_values(methods_df, meta)
    # Uppsida vs idag
    upct = None
    if _pos(price) and _pos(fv.get("today")):
        upct = (float(fv["today"]) - float(price)) / float(price) * 100.0

    return {
        "Ticker": tkr,
        "Valuta": ccy,
        "Kurs": _r2(price),
        "FV idag": _r2(fv.get("today")),
        "FV 1 år": _r2(fv.get("1y")),
        "FV 2 år": _r2(fv.get("2y")),
        "FV 3 år": _r2(fv.get("3y")),
        "Bull 1 år": _r2(fv.get("bull_1y")),
        "Bear 1 år": _r2(fv.get("bear_1y")),
        "Uppsida (%)": None if upct is None else float(f"{upct:.1f}"),
        "Status": "OK",
        "_fv_raw": fv
    }

# --------------------------------------------
# 🏆 Ranking-sida
# --------------------------------------------
def page_ranking():
    st.header("🏆 Ranking (Riktkurser)")

    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df

    if df is None or df.empty:
        st.info("Inga rader i Data-bladet.")
        return

    tickers = sorted(df["Ticker"].dropna().astype(str).unique().tolist())
    defaults = []  # tom = alla
    sel = st.multiselect("Välj tickers (tom = alla)", options=tickers, default=defaults)
    target = tickers if len(sel) == 0 else sel

    c1, c2, c3 = st.columns([1,1,2])
    with c1:
        do_save = st.checkbox("Spara till Data", value=True)
    with c2:
        show_only_errors = st.checkbox("Visa endast fel", value=False)
    with c3:
        st.caption("Beräkning enligt appens beräkningsmotor (Del 3) och FV-logik från basversionen.")

    if not st.button("🚀 Kör ranking"):
        return

    work = df.copy()
    rows_out = []
    progress = st.progress(0.0)
    status = st.empty()

    for i, t in enumerate(target, start=1):
        status.write(f"Beräknar {i}/{len(target)} – {t}")
        try:
            mask = work["Ticker"].astype(str).str.upper() == str(t).upper()
            if not mask.any():
                rows_out.append({"Ticker": t, "Status": "Saknas i Data"})
                progress.progress(i/len(target))
                continue

            idx = work.index[mask][0]
            row = work.loc[idx]

            res = _rank_one(row)
            rows_out.append(res)

            if res.get("Status") == "OK" and do_save:
                _update_row_with_fair_values(work, idx, res.get("_fv_raw", {}))
        except Exception as e:
            rows_out.append({"Ticker": t, "Status": f"Fel: {e}"})
        progress.progress(i/len(target))

    progress.empty()
    status.empty()

    # Spara till Sheets om valt
    if do_save:
        try:
            write_data_df(work)
            st.session_state["DATA"] = work
            st.success("Riktkurser uppdaterade i Data-bladet.")
        except Exception as e:
            st.error(f"Kunde inte spara till Data: {e}")

    # Visa resultat-tabell
    out = pd.DataFrame(rows_out)
    if not out.empty:
        if show_only_errors:
            mask_err = out["Status"].astype(str).str.startswith("Fel")
            out = out[mask_err | (out["Status"].astype(str) == "Saknas i Data")]
        # Städa visningskolumner
        show_cols = ["Ticker","Valuta","Kurs","FV idag","FV 1 år","FV 2 år","FV 3 år","Bull 1 år","Bear 1 år","Uppsida (%)","Status"]
        for c in show_cols:
            if c not in out.columns:
                out[c] = np.nan
        _show_df(out[show_cols].reset_index(drop=True), height=420, use_container_width=True)

# --------------------------------------------
# Main: navigering mellan alla sidor
# --------------------------------------------
def _ensure_session_boot():
    # Ladda in grunddata till session vid behov
    if "DATA" not in st.session_state:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception:
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
    if "FX" not in st.session_state:
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception:
            st.session_state["FX"] = {}

def main():
    _ensure_session_boot()

    st.sidebar.title("📈 Aktieanalys & investeringsförslag")
    menu = st.sidebar.radio(
        "Meny",
        [
            "🏆 Ranking",
            "📦 Portfölj",
            "🛒 Köpförslag",
            "✏️ Editor",
            "➕ Lägg till",
            "🧩 Massuppdatering",
            "🕒 Snapshot",
            "⚙️ Settings",
        ],
        index=0
    )

    try:
        if menu == "🏆 Ranking":
            page_ranking()
        elif menu == "📦 Portfölj":
            page_portfolio()
        elif menu == "🛒 Köpförslag":
            page_buy_suggestions()
        elif menu == "✏️ Editor":
            page_editor()
        elif menu == "➕ Lägg till":
            page_add_ticker()
        elif menu == "🧩 Massuppdatering":
            page_batch()
        elif menu == "🕒 Snapshot":
            page_snapshot()
        elif menu == "⚙️ Settings":
            page_settings()
        else:
            page_ranking()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# Streamlit entry
if __name__ == "__main__":
    main()
# (Slut Del 6/6)
