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
        # CHANGED: säkra korrekt tom DataFrame om arket är helt tomt
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

    # Datumkolumn → ren Python date eller None (aldrig NaT)
    if "Nästa utdelningsdatum" in df.columns:
        # CHANGED: ersätt NaT med None och håll rena date-objekt
        dcol = pd.to_datetime(df["Nästa utdelningsdatum"], errors="coerce", utc=False)
        df["Nästa utdelningsdatum"] = dcol.apply(lambda x: x.date() if pd.notna(x) else None)

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
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Uppdateringsfunktioner (enskild & massa) som endast skriver över fält vi lyckas hämta
# ============================================================

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Yahoo-snapshot (pris, valuta, aktier, utdelning m.m.)
#  • Säkra uppdaterare som endast skriver över lyckade fält
#  • Enskild uppdatering + batch (med 1s fördröjning och progress-callback)
# ============================================================

# ---------- Yahoo helpers ----------

def _yf_ticker(tick: str):
    try:
        return yf.Ticker(str(tick).strip())
    except Exception:
        return None

def _as_float(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _try_fast_info(t):
    """
    Försök hämta snabba fält via fast_info.
    Returnerar dict med ev. None-värden om ej tillgängligt.
    """
    out = {
        "price": None,
        "currency": None,
        "shares_out": None,
        "pe_ttm": None,
        "pe_fwd": None,
        "pb": None,
        "dividend_fwd": None,
    }
    try:
        fi = getattr(t, "fast_info", None)
        if fi:
            # Pris & valuta
            out["price"]    = _as_float(getattr(fi, "last_price", None))
            out["currency"] = getattr(fi, "currency", None)

            # Multiplar (om tillgängligt i fast_info – inte alltid)
            out["pe_ttm"] = _as_float(getattr(fi, "trailing_pe", None))
            out["pe_fwd"] = _as_float(getattr(fi, "forward_pe", None))
            out["pb"]     = _as_float(getattr(fi, "price_to_book", None))
    except Exception:
        pass
    return out

def _try_info(t):
    """
    Komplettera via .info (kan vara långsammare/instabilt beroende på yfinance-version).
    Hämtar t.ex. sharesOutstanding och forwardDividendRate om möjligt.
    """
    out = {
        "shares_out": None,
        "dividend_fwd": None,
        "currency": None,
        "pe_ttm": None,
        "pe_fwd": None,
        "pb": None,
        "price": None,
    }
    try:
        info = t.info  # kan kasta eller vara tomt
        if isinstance(info, dict) and info:
            out["shares_out"]   = _as_float(info.get("sharesOutstanding"))
            out["dividend_fwd"] = _as_float(info.get("forwardDividendRate"))
            out["currency"]     = info.get("currency", out["currency"])
            out["pe_ttm"]       = _as_float(info.get("trailingPE"))
            out["pe_fwd"]       = _as_float(info.get("forwardPE"))
            out["pb"]           = _as_float(info.get("priceToBook"))
            # Pris: ta regularMarketPrice om saknas sedan tidigare
            out["price"]        = _as_float(info.get("regularMarketPrice"))
    except Exception:
        pass
    return out

def _try_dividends(t):
    """
    Summera utdelningar senaste 12 månader (fallback om forward saknas).
    Returnerar årlig utdelning (per aktie) om datat finns.
    """
    try:
        div = t.dividends
        if div is not None and not div.empty:
            # Senaste 365 dagarna
            cutoff = pd.Timestamp.today(tz=None) - pd.Timedelta(days=365)
            recent = div[div.index >= cutoff]
            if not recent.empty:
                return _as_float(float(recent.sum()))
    except Exception:
        pass
    return None

def yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar en enkel snapshot för 'ticker'.
    Fyller bara de fält vi med säkerhet kan fånga via yfinance.
    """
    t = _yf_ticker(ticker)
    if t is None:
        return {}

    out = {
        "Aktuell kurs": None,
        "Valuta": None,
        "Utestående aktier": None,
        "PE TTM": None,
        "PE FWD": None,
        "P/B": None,
        "Årlig utdelning": None,
        # Övriga fält lämnas för vyer/beräkningssteg att sätta om/när sådant stöd läggs på
        # "Rev TTM","EBITDA TTM","EPS TTM","EV/Revenue","EV/EBITDA","BVPS","Net debt"
    }

    # 1) fast_info
    fi = _try_fast_info(t)
    # 2) info – komplettera
    ii = _try_info(t)

    # Pris
    price = fi.get("price") if fi.get("price") is not None else ii.get("price")
    out["Aktuell kurs"] = price

    # Valuta
    cur = fi.get("currency") or ii.get("currency")
    out["Valuta"] = cur

    # Aktier
    shares = fi.get("shares_out") if fi.get("shares_out") is not None else ii.get("shares_out")
    out["Utestående aktier"] = shares

    # Multiplar
    out["PE TTM"] = fi.get("pe_ttm") if fi.get("pe_ttm") is not None else ii.get("pe_ttm")
    out["PE FWD"] = fi.get("pe_fwd") if fi.get("pe_fwd") is not None else ii.get("pe_fwd")
    out["P/B"]    = fi.get("pb")     if fi.get("pb")     is not None else ii.get("pb")

    # Utdelning – forward i första hand, annars trailing 12m
    div_fwd = ii.get("dividend_fwd") if ii.get("dividend_fwd") is not None else fi.get("dividend_fwd")
    if div_fwd is None:
        div_fwd = _try_dividends(t)
    out["Årlig utdelning"] = div_fwd

    # Städa bort uppenbart skräp (<=0 → None)
    for k in list(out.keys()):
        if isinstance(out[k], (int, float)):
            if out[k] is not None and out[k] <= 0:
                out[k] = None

    return out

# ---------- Skriv-säkrare uppdateringar (Data-bladet) ----------

_YAHOO_WRITABLE_KEYS = [
    "Aktuell kurs","Valuta","Utestående aktier",
    "PE TTM","PE FWD","P/B","Årlig utdelning",
    # Dessa finns i schemat men fylls ej här om vi inte är säkra:
    # "Rev TTM","EBITDA TTM","EPS TTM","EV/Revenue","EV/EBITDA","BVPS","Net debt"
]

def _apply_snapshot_to_dataframe(df: pd.DataFrame, ticker: str, snap: Dict[str, Any]) -> pd.DataFrame:
    """
    Skriver endast över kolumner i _YAHOO_WRITABLE_KEYS där 'snap' har icke-None.
    Sätter även 'Senast auto uppdaterad' + 'Auto källa' på rader som uppdaterats.
    """
    if df.empty or not snap:
        return df

    mask = df["Ticker"].astype(str).str.strip().str.upper() == str(ticker).strip().upper()
    if not mask.any():
        # Om ticker inte finns, lägg INTE till ny rad här – det görs via editor/tickervy.
        return df

    upd_cols = [k for k in _YAHOO_WRITABLE_KEYS if k in df.columns and (snap.get(k) is not None)]
    if not upd_cols:
        # Inget att skriva
        return df

    df.loc[mask, upd_cols] = df.loc[mask, upd_cols].apply(
        lambda s: s  # kolumnvis apply, vi sätter celler nedan
    )
    for k in upd_cols:
        df.loc[mask, k] = snap.get(k)

    # Stämplar
    if "Senast auto uppdaterad" in df.columns:
        df.loc[mask, "Senast auto uppdaterad"] = now_stamp()
    if "Auto källa" in df.columns:
        df.loc[mask, "Auto källa"] = "Yahoo Finance"
    return df

def update_single_from_yahoo(ticker: str) -> pd.DataFrame:
    """
    Läser Data, hämtar snapshot för 'ticker', skriver endast lyckade fält och returnerar uppdaterad df.
    """
    df = read_data_df()
    try:
        snap = yahoo_snapshot(ticker)
        df2  = _apply_snapshot_to_dataframe(df.copy(), ticker, snap)
        if not df2.equals(df):
            write_data_df(df2)
            return df2
        return df
    except Exception as e:
        st.error(f"💥 Fel vid uppdatering av {ticker} från Yahoo: {e}")
        return df

def batch_update_from_yahoo(tickers: List[str], progress_cb=None, delay_sec: float = 1.0) -> pd.DataFrame:
    """
    Batch-uppdaterar en lista av tickers.
    • Kör 1 sekunds fördröjning mellan anrop (API-vänligt).
    • Visar progress via valfri progress_cb(i, n, current_ticker).
    • Returnerar slutlig uppdaterad Data-DataFrame.
    """
    df = read_data_df()
    n  = len(tickers)
    for i, tk in enumerate(tickers, start=1):
        if callable(progress_cb):
            try:
                progress_cb(i, n, tk)
            except Exception:
                pass
        try:
            snap = yahoo_snapshot(tk)
            df   = _apply_snapshot_to_dataframe(df, tk, snap)
        except Exception as e:
            st.warning(f"⚠️ Misslyckades för {tk}: {e}")
        # Fördröjning mellan anrop
        try:
            time.sleep(max(0.0, float(delay_sec)))
        except Exception:
            time.sleep(1.0)
    # Skriv tillbaka om något ändrats: (enkelt jämförelsetest via shape/kolumner + fallback alltid skriv)
    write_data_df(df)
    return df

# ---------- Hjälpare för Bucket-cap (till köpförslag m.fl.) ----------

def _bucket_cap_lookup(bucket_name: str, settings: Dict[str, str]) -> Optional[float]:
    """
    Returnerar maxvärde i SEK för ett innehav baserat på bucket-namn och Settings.
    Exempel:
      "Bucket A tillväxt"   -> settings["bucket_cap_A_tillvaxt"]
      "Bucket B utdelning"  -> settings["bucket_cap_B_utdelning"]
    """
    if not bucket_name:
        return None
    b = str(bucket_name).strip().lower()
    key = None
    if "tillväxt" in b or "tillvaxt" in b:
        if b.startswith("bucket a"):
            key = "bucket_cap_A_tillvaxt"
        elif b.startswith("bucket b"):
            key = "bucket_cap_B_tillvaxt"
        elif b.startswith("bucket c"):
            key = "bucket_cap_C_tillvaxt"
    elif "utdelning" in b:
        if b.startswith("bucket a"):
            key = "bucket_cap_A_utdelning"
        elif b.startswith("bucket b"):
            key = "bucket_cap_B_utdelning"
        elif b.startswith("bucket c"):
            key = "bucket_cap_C_utdelning"
    if not key:
        return None
    try:
        return float(settings.get(key, "0"))
    except Exception:
        return None

# ============================================================
# Del 3/6 — Värderingskärna (fair value-metoder, preferensordning)
#  • Innehåller rena funktionsanrop för att beräkna riktkurser/fair value
#  • INGA auto-körningar – vyerna (Ranking) triggar detta via knapp + progressbar
# ============================================================

# ============================================================
# Del 3/6 — Värderingskärna
#  • fetch_from_yahoo() wrapper (mappar Del 2/6 → beräkningsnycklar)
#  • EPS-estimat (yahoo) – försök hämta next year + long-term growth
#  • Metoder: pe_hist_vs_eps, ev_sales, ev_ebitda, p_b (+ placeholders)
#  • Multipel-decay & PE-ankare
#  • Fair Value = median över oberoende metodfamiljer
#  • compute_methods_for_row(row, settings, fx_map) → (methods_df, sanity, meta)
# ============================================================

# ---------- Wrapper: Del 2/6 → beräkningsnycklar ----------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Hämtar snapshot (Del 2/6) och mappar till nycklar som beräkningskärnan använder.
    Vissa fält kan saknas (None) – beräkningarna är defensiva.
    """
    snap = yahoo_snapshot(ticker)
    return {
        "price":      _f(snap.get("Aktuell kurs")),
        "currency":   (snap.get("Valuta") or "USD"),
        "shares_out": _f(snap.get("Utestående aktier")),
        "pe_ttm":     _f(snap.get("PE TTM")),
        "pe_fwd":     _f(snap.get("PE FWD")),
        "p_b":        _f(snap.get("P/B")),
        "dps_annual": _f(snap.get("Årlig utdelning")),
        # Följande kan komma från Data-bladet som fallback i compute_methods_for_row:
        "rev_ttm":    None,
        "ebitda_ttm": None,
        "eps_ttm":    None,
        "ev_rev":     None,
        "ev_ebitda":  None,
        "bvps":       None,
        "net_debt":   None,
        "rev_cagr_hist": None,
        "eps_cagr_hist": None,
    }

# ---------- EPS-estimat från Yahoo (earnings_trend) ----------
@st.cache_data(ttl=900, show_spinner=False)
def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker läsa EPS-estimat för 'nästa år' + långsiktig tillväxt (5y).
    Returnerar {"eps_1y": float|None, "eps_2y": float|None, "eps_cagr_long": float|None}
    """
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

        def _to_float(v):
            if isinstance(v, dict):
                for k in ("avg", "average", "mean"):
                    if k in v and _f(v[k]) is not None:
                        return _f(v[k])
            return _f(v)

        # Hitta rader
        if "period" not in df.columns:
            return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}
        per = df["period"].astype(str).str.lower()

        row_next = df[per.isin(["nextfiscalyear", "next fiscal year", "nextyear"])]
        row_curr = df[per.isin(["currentfiscalyear", "current fiscal year", "currentyear"])]
        row_long = df[per.isin(["next5years", "longterm"])]

        eps_1y = None
        for src in (row_next, row_curr):
            if src is not None and not src.empty:
                r0 = src.iloc[0].to_dict()
                for key in ("earningsestimate", "epsestimate", "epstrend"):
                    if key in r0:
                        val = _to_float(r0.get(key))
                        if val is not None:
                            eps_1y = val
                            break
            if eps_1y is not None:
                break

        eps_cagr_long = None
        if row_long is not None and not row_long.empty:
            rL = row_long.iloc[0].to_dict()
            for key in ("growth", "longtermgrowthrate"):
                g = _f(rL.get(key))
                if g is not None:
                    eps_cagr_long = g
                    break

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {
            "eps_1y": _f(eps_1y),
            "eps_2y": _f(eps_2y),
            "eps_cagr_long": _f(eps_cagr_long)
        }
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}

# ---------- Clamp-gränser (i linje med din praxis) ----------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

# ---------- Små helpers för beräkning ----------
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

def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    try:
        if a is None or b is None or b == 0:
            return None
        return float(a) / float(b)
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

# ---------- EPS & EBITDA-paths ----------
def _eps_path_fill(eps_ttm: Optional[float], eps_1y: Optional[float], eps_2y: Optional[float],
                   eps_cagr_hist: Optional[float], eps_cagr_long: Optional[float],
                   rev_cagr_hist: Optional[float]) -> Tuple[float, float, float, float]:
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

# ---------- Fair Value (familjemedian) ----------
def _compute_fair_value_row_v2(methods_df: pd.DataFrame, now_price: Optional[float]) -> Dict[str, Any]:
    """
    Median över oberoende familjer:
      • 'pe_hist_vs_eps'           → 'pe'
      • 'ev_sales'                 → 'ev_s'
      • 'ev_ebitda' / 'ev_dacf'    → 'ev_e' (en gång)
      • 'p_b'                      → 'pb'
    Filtrerar bort target som ≈ dagens kurs (±0,5 %) i kolumn 'Idag' för att undvika tautologi.
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
        used = set()
        for _, r in methods_df.iterrows():
            m = str(r.get("Metod") or "")
            if m == "fair_value":
                continue
            v = _f(r.get(c))
            if v is None:
                continue
            fam = fam_map.get(m, m)
            if fam in used:
                continue
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:
                    continue
            used.add(fam)
            vals.append(float(v))

        if not vals:
            try:
                row_pe = methods_df[methods_df["Metod"] == "pe_hist_vs_eps"].iloc[0]
                out[c] = _f(row_pe.get(c))
            except Exception:
                out[c] = np.nan
        else:
            out[c] = float(np.median(vals))
    return out

# ---------- Huvud: compute_methods_for_row ----------
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "pe_hist_vs_eps","ev_sales","ev_ebitda","ev_dacf","p_b",
        "p_nav","p_tbv","p_affo","p_fcf","ev_fcf","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST

def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Returnerar:
      • methods_df: DataFrame [Metod, Idag, 1 år, 2 år, 3 år]
      • sanity    : kort text
      • meta      : hjälpfält + 'fair_value' med {today, y1, y2, y3}
    Alla värden i bolagets egen handelsvaluta.
    """
    ticker = str(row.get("Ticker", "")).strip()
    y  = fetch_from_yahoo(ticker)
    es = _fetch_eps_estimates_yahoo(ticker)

    # Inputs med fallback från Data-bladet
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

    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), es.get("eps_1y")))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), es.get("eps_2y")))

    # Historisk CAGR (med clamp)
    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), y.get("rev_cagr_hist")))
    rev_cagr_hist     = max(REV_CAGR_MIN, min(REV_CAGR_MAX, rev_cagr_hist_raw)) if rev_cagr_hist_raw is not None else None

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), y.get("eps_cagr_hist")))
    eps_cagr_hist     = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_hist_raw)) if eps_cagr_hist_raw is not None else None

    eps_cagr_long = _f(es.get("eps_cagr_long"))
    if eps_cagr_long is not None:
        eps_cagr_long = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_long))

    # Settings
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

    # Multiplar (med decay)
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales,  _decay_multiple(ev_sales,  1, decay), _decay_multiple(ev_sales,  2, decay), _decay_multiple(ev_sales,  3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,       _decay_multiple(p_b,       1, decay), _decay_multiple(p_b,       2, decay), _decay_multiple(p_b,       3, decay)

    # Metoder (alla i bolagets valuta)
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
    # Platshållare för strukturkompatibilitet
    for m in ("p_nav","p_tbv","p_affo","p_fcf","ev_fcf","p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # Fair Value (familjemedian)
    fv_row = _compute_fair_value_row_v2(methods_df, price)
    methods_df = pd.concat([pd.DataFrame([fv_row]), methods_df], ignore_index=True)

    # Sanity-text (utan inbäddade f-strängskrockar)
    pe_anchor_disp = (f"{pe_anchor:.2f}" if _pos(pe_anchor) else "—")
    rev_clamp_span = f"{int(REV_CAGR_MIN*100)}%..{int(REV_CAGR_MAX*100)}%"
    eps_clamp_span = f"{int(EPS_CAGR_MIN*100)}%..{int(EPS_CAGR_MAX*100)}%"
    sanity = (
        "price=" + ("ok" if price else "—") + ", "
        "eps_ttm=" + ("ok" if (eps_ttm or eps_ttm == 0) else "—") + ", "
        "eps_1y=" + ("ok" if eps_1y_est else "—") + ", "
        "eps_2y=" + ("ok" if eps_2y_est else "—") + ", "
        "rev_ttm=" + ("ok" if rev_ttm else "—") + ", "
        f"rev_cagr_hist=" + ("ok" if _f(rev_cagr_hist) is not None else "—") + f"(clamp={rev_clamp_span}), "
        f"eps_cagr_hist=" + ("ok" if _f(eps_cagr_hist) is not None else "—") + f"(clamp={eps_clamp_span}), "
        "ebitda_ttm=" + ("ok" if (ebitda_ttm or ebitda_ttm == 0) else "—") + ", "
        "shares=" + ("ok" if shares else "—") + ", "
        f"pe_anchor={pe_anchor_disp}, decay={decay}"
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
# Del 4/6 — Portfölj, utdelningar & köpförslag-helpers
#  • Säkerhetshelpers för DataFrames (fix för "truth value of a DataFrame")
#  • Fältalias/kolumnkonstanter
#  • Källskatt per valuta + FX-hjälpare
#  • Utdelningsberäkning (nästa utbetalning → SEK netto per innehav)
#  • Bucket-regler (tak i SEK, defaults enligt din Bucket-metod)
#  • Köpförslag: läser sparade riktkurser (Ranking) och föreslår köp om
#    (Aktuell kurs < vald riktkurs) och (innehavsvärde < bucket-tak)
# ============================================================

# ---------- DF-säkerhetshelpers ----------
def _is_df(x: Any) -> bool:
    return isinstance(x, pd.DataFrame)

def _df_nonempty(x: Any) -> bool:
    return _is_df(x) and not x.empty

def _series_get(row: pd.Series, *names: str, default=None):
    for n in names:
        if n in row and row[n] is not None and row[n] != "":
            return row[n]
    return default

# ---------- Kolumnalias ----------
COL_TICKER    = "Ticker"
COL_CCY       = "Valuta"
COL_PRICE     = "Aktuell kurs"
COL_SHARES    = "Antal aktier"
COL_OWNS      = "Äger"  # kan vara bool eller härledas från Antal aktier > 0
COL_BUCKET    = "Bucket"
COL_NEXT_DATE = "Nästa utdelningsdatum"           # YYYY-MM-DD (helst)
COL_DPS_ANNU  = "Årlig utdelning"                 # DPS per år i handelsvaluta
COL_PAY_FREQ  = "Utdelningstillfällen/år"         # 1/2/4/12...
COL_NEXT_DPS  = "Nästa utdelning per aktie"       # Om explicit nästa DPS finns
COL_WHT_RATE  = "Källskatt (%)"

# Sparade riktkurser (Ranking-vyn skriver hit)
COL_TGT_0     = "Riktkurs idag"
COL_TGT_1     = "Riktkurs 1 år"
COL_TGT_2     = "Riktkurs 2 år"
COL_TGT_3     = "Riktkurs 3 år"

# ---------- Valuta & källskatt ----------
def _fx_convert(amount: Optional[float], ccy_from: str, ccy_to: str, fx_map: Dict[str, float]) -> Optional[float]:
    """
    Konverterar via fx_map som antas ha 'SEK', 'USD', 'NOK', 'CAD', 'EUR' etc med
    NYCKEL = VALUTA, VÄRDE = SEK per enhet valuta (dvs 1 USD → X SEK).
    """
    a = _f(amount)
    if a is None:
        return None
    if ccy_from == ccy_to:
        return float(a)
    # till SEK: a * sek_per_ccy
    if ccy_to.upper() == "SEK":
        rate = _f(fx_map.get(ccy_from.upper()))
        return float(a) * rate if rate else None
    # annan kors: from → SEK → to
    sek_per_from = _f(fx_map.get(ccy_from.upper()))
    sek_per_to   = _f(fx_map.get(ccy_to.upper()))
    if not sek_per_from or not sek_per_to:
        return None
    return float(a) * float(sek_per_from) / float(sek_per_to)

def _withholding_rate_for_currency(ccy: str, settings: Optional[Mapping] = None) -> float:
    """
    Standard: NOK 25%, USD 15%, CAD 15%, annars 0%.
    Tillåter override via Settings:
      - wht_usd_percent, wht_nok_percent, wht_cad_percent, wht_default_percent
    """
    c = (ccy or "").upper()
    def _pick(key: str, fallback: float) -> float:
        if settings is None:
            return fallback
        v = settings.get(key)
        x = _f(v)
        return float(x) if x is not None else fallback

    if c == "USD":
        return _pick("wht_usd_percent", 15.0)
    if c == "NOK":
        return _pick("wht_nok_percent", 25.0)
    if c == "CAD":
        return _pick("wht_cad_percent", 15.0)
    return _pick("wht_default_percent", 0.0)

# ---------- Utdelningslogik ----------
def _parse_date_ymd(s: Any) -> Optional[pd.Timestamp]:
    if s is None or s == "":
        return None
    try:
        return pd.to_datetime(str(s), errors="coerce").normalize()
    except Exception:
        return None

def make_dividend_row(row: pd.Series, fx_map: Dict[str, float], settings: Mapping[str, Any], today: Optional[pd.Timestamp]=None) -> Optional[Dict[str, Any]]:
    """
    Beräknar NETTO SEK för nästa utdelning för ett innehav.
    Regler:
      • Antal aktier > 0 eller Äger==True
      • Datum i framtiden (>= idag)
      • DPS hämtas prioriterat: COL_NEXT_DPS → (COL_DPS_ANNU / COL_PAY_FREQ)
        Om COL_PAY_FREQ saknas → rimligt default: USD=4, CAD=4, NOK=2, annars 4.
    Returnerar en dict för tabellvisning eller None om ej tillämpligt.
    """
    ticker = _series_get(row, COL_TICKER)
    if not ticker:
        return None

    shares = _f(_series_get(row, COL_SHARES))
    owns   = _series_get(row, COL_OWNS)
    if shares is None:
        shares = 0.0
    owns_bool = False
    try:
        owns_bool = bool(owns)
    except Exception:
        owns_bool = False
    if shares <= 0 and not owns_bool:
        return None

    next_date = _parse_date_ymd(_series_get(row, COL_NEXT_DATE))
    t0 = today or pd.Timestamp(pd.Timestamp.now().date())
    if next_date is None or next_date < t0:
        return None

    ccy   = (_series_get(row, COL_CCY) or "USD").upper()
    price = _f(_series_get(row, COL_PRICE))  # används inte här men bra meta
    dps_next = _f(_series_get(row, COL_NEXT_DPS))

    if dps_next is None:
        dps_annual = _f(_series_get(row, COL_DPS_ANNU))
        freq = _f(_series_get(row, COL_PAY_FREQ))
        if freq is None:
            # rimliga defaults
            freq = 4.0 if ccy in ("USD", "CAD") else (2.0 if ccy == "NOK" else 4.0)
        if dps_annual is not None and freq and freq > 0:
            dps_next = dps_annual / float(freq)

    if dps_next is None or dps_next <= 0:
        return None

    gross_ccy = float(dps_next) * float(shares)
    gross_sek = _fx_convert(gross_ccy, ccy_from=ccy, ccy_to="SEK", fx_map=fx_map) or 0.0

    wht_rate = _withholding_rate_for_currency(ccy, settings)
    wht_sek  = gross_sek * (wht_rate / 100.0)
    net_sek  = gross_sek - wht_sek

    return {
        "Ticker": ticker,
        "Valuta": ccy,
        "Antal aktier": float(shares),
        "Datum": next_date.date().isoformat(),
        "DPS nästa": round(float(dps_next), 4),
        "Brutto (ccy)": round(float(gross_ccy), 2),
        "Brutto (SEK)": round(float(gross_sek), 2),
        "Källskatt %": float(wht_rate),
        "Källskatt (SEK)": round(float(wht_sek), 2),
        "Netto (SEK)": round(float(net_sek), 2),
    }

def build_dividends_table(df: pd.DataFrame, fx_map: Dict[str, float], settings: Mapping[str, Any]) -> pd.DataFrame:
    """
    Skapar tabell med kommande *nästa* utdelning per innehav (en rad per ticker).
    Filtrerar bort rader utan datum i framtiden eller utan DPS.
    Sorterar på datum stigande.
    """
    if not _df_nonempty(df):
        return pd.DataFrame(columns=["Ticker","Valuta","Antal aktier","Datum","DPS nästa","Brutto (ccy)","Brutto (SEK)","Källskatt %","Källskatt (SEK)","Netto (SEK)"])

    rows = []
    today = pd.Timestamp(pd.Timestamp.now().date())
    for _, r in df.iterrows():
        d = make_dividend_row(r, fx_map=fx_map, settings=settings, today=today)
        if d:
            rows.append(d)
    out = pd.DataFrame(rows)
    if _df_nonempty(out):
        out = out.sort_values(by="Datum").reset_index(drop=True)
    return out

# ---------- Bucket-regler ----------
BUCKET_TYPES = [
    "A tillväxt", "A utdelning",
    "B tillväxt", "B utdelning",
    "C tillväxt", "C utdelning",
]

# Standardtak i SEK enligt din Bucket-metod (kan överskridas via Settings)
BUCKET_DEFAULTS_SEK = {
    "A tillväxt": 20000.0,
    "A utdelning": 10000.0,
    "B tillväxt": 10000.0,
    "B utdelning": 7000.0,
    "C tillväxt": 6000.0,
    "C utdelning": 4000.0,
}

def bucket_cap_sek(bucket: Optional[str], settings: Optional[Mapping[str, Any]] = None) -> float:
    """
    Returnerar tak i SEK för givet bucket-namn.
    Settings-override (om satt):
      bucket_cap_sek_A_tillvaxt, bucket_cap_sek_A_utdelning, ...
      (mellanslag ersätts med underscore i nyckeln)
    """
    b = (bucket or "").strip()
    base = BUCKET_DEFAULTS_SEK.get(b, 0.0)
    if not settings:
        return float(base)
    key = "bucket_cap_sek_" + b.replace(" ", "_").replace("å","a").replace("ä","a").replace("ö","o")
    v = _f(settings.get(key))
    return float(v) if v is not None else float(base)

def position_value_sek(row: pd.Series, fx_map: Dict[str, float]) -> float:
    ccy   = (_series_get(row, COL_CCY) or "USD").upper()
    price = _f(_series_get(row, COL_PRICE)) or 0.0
    shares = _f(_series_get(row, COL_SHARES)) or 0.0
    gross_ccy = float(price) * float(shares)
    gross_sek = _fx_convert(gross_ccy, ccy_from=ccy, ccy_to="SEK", fx_map=fx_map) or 0.0
    return float(gross_sek)

# ---------- Köpförslag ----------
def _pick_target_for_horizon(row: pd.Series, horizon: str) -> Optional[float]:
    """
    horizon ∈ {"Idag","1 år","2 år","3 år"} → hämtar sparad riktkurs från respektive kolumn.
    """
    h = (horizon or "Idag").strip().lower()
    if h.startswith("idag"):
        return _f(row.get(COL_TGT_0))
    if h.startswith("1"):
        return _f(row.get(COL_TGT_1))
    if h.startswith("2"):
        return _f(row.get(COL_TGT_2))
    if h.startswith("3"):
        return _f(row.get(COL_TGT_3))
    return _f(row.get(COL_TGT_0))

def build_buy_suggestions(
    df_data: pd.DataFrame,
    fx_map: Dict[str, float],
    settings: Mapping[str, Any],
    horizon_label: str = "Idag",
    filter_owned: Optional[str] = None,     # None/"Äger"/"Äger ej"
    filter_bucket: Optional[str] = None,    # None eller en i BUCKET_TYPES
) -> pd.DataFrame:
    """
    Genererar köpförslag enligt:
      • Aktuell kurs < Riktkurs(horizon)
      • Innehavsvärde (SEK) < Bucket-tak (SEK)
    Filtrering:
      • filter_owned: "Äger" (kräv antal >0), "Äger ej" (antal==0), None (alla)
      • filter_bucket: Begränsa till vald bucket
    Returnerar DataFrame med förslag sorterade på uppsida % störst → minst.
    """
    if not _df_nonempty(df_data):
        return pd.DataFrame(columns=["Ticker","Bucket","Valuta","Aktuell kurs","Riktkurs","Uppsida %","Antal aktier","Innehavsvärde (SEK)","Bucket-tak (SEK)","Motivering"])

    rows = []
    for _, r in df_data.iterrows():
        ticker = _series_get(r, COL_TICKER)
        if not ticker:
            continue

        # Filtrering: bucket
        btype = _series_get(r, COL_BUCKET)
        if filter_bucket and btype != filter_bucket:
            continue

        # Filtrering: äger/ej
        shares = _f(_series_get(r, COL_SHARES)) or 0.0
        if filter_owned == "Äger" and shares <= 0:
            continue
        if filter_owned == "Äger ej" and shares > 0:
            continue

        price  = _f(_series_get(r, COL_PRICE))
        ccy    = (_series_get(r, COL_CCY) or "USD").upper()
        target = _pick_target_for_horizon(r, horizon_label)

        if price is None or target is None:
            # saknar pris eller riktkurs – inget förslag
            continue

        if float(price) >= float(target):
            # ej under fair value
            continue

        # Bucket-tak och innehavsvärde
        cap_sek = bucket_cap_sek(btype, settings=settings)
        pos_sek = position_value_sek(r, fx_map=fx_map)

        if pos_sek >= cap_sek and cap_sek > 0:
            # redan på eller över tak
            continue

        upside = (float(target) - float(price)) / float(price) * 100.0

        rows.append({
            "Ticker": ticker,
            "Bucket": btype or "",
            "Valuta": ccy,
            "Aktuell kurs": float(price),
            "Riktkurs": float(target),
            "Uppsida %": round(float(upside), 2),
            "Antal aktier": float(shares),
            "Innehavsvärde (SEK)": round(pos_sek, 2),
            "Bucket-tak (SEK)": round(cap_sek, 2),
            "Motivering": "Under fair value och under bucket-tak",
        })

    out = pd.DataFrame(rows)
    if _df_nonempty(out):
        out = out.sort_values(by="Uppsida %", ascending=False).reset_index(drop=True)
    else:
        out = pd.DataFrame(columns=["Ticker","Bucket","Valuta","Aktuell kurs","Riktkurs","Uppsida %","Antal aktier","Innehavsvärde (SEK)","Bucket-tak (SEK)","Motivering"])
    return out

# ---------- Hjälp: summeringar för UI ----------
def summarize_dividends(df_div: pd.DataFrame) -> Dict[str, float]:
    """
    Summerar brutto/källskatt/netto SEK över tabellen build_dividends_table().
    """
    if not _df_nonempty(df_div):
        return {"Brutto (SEK)": 0.0, "Källskatt (SEK)": 0.0, "Netto (SEK)": 0.0}
    return {
        "Brutto (SEK)": float(_f(df_div["Brutto (SEK)"].sum()) or 0.0),
        "Källskatt (SEK)": float(_f(df_div["Källskatt (SEK)"].sum()) or 0.0),
        "Netto (SEK)": float(_f(df_div["Netto (SEK)"].sum()) or 0.0),
    }

def summarize_suggestions(df_sugg: pd.DataFrame) -> Dict[str, float]:
    """
    T.ex. medel-uppsida etc (för UI).
    """
    if not _df_nonempty(df_sugg):
        return {"Antal förslag": 0, "Medel uppsida %": 0.0}
    return {
        "Antal förslag": int(len(df_sugg)),
        "Medel uppsida %": float(round(_f(df_sugg["Uppsida %"].mean()) or 0.0, 2)),
    }

# ============================================================
# Del 5/6 — Vyer
#  • Settings (redigerbar tabell)
#  • Snapshot
#  • Editor (rullista för Bucket)
#  • Lägg till (rullista för Bucket)
#  • Portfölj (oförändrad här – finns redan i Del 4/6/Del 6/6)
#  • Analys (behåller singel-analys + lägger till full tabell m. filtrering per horisont)
#  • Ranking (ingen autoberäkning – knapp som beräknar & sparar alla med progress 1/X)
#  • Köpförslag (läser SPARADE riktkurser; filter Äger/Äger ej + Bucket; inga nya beräkningar)
# ============================================================

# ---- Små formaterare/hjälpare (endast om saknas) ----
if "_format_num" not in globals():
    def _format_num(x, nd=2):
        v = _f(x)
        if v is None:
            return "—"
        return f"{v:.{nd}f}"

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

if "_now" not in globals():
    def _now():
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ============================================================
# Settings
# ============================================================
def page_settings():
    st.header("⚙️ Settings")
    s_df = _read_df(SETTINGS_TITLE)
    if s_df.empty:
        s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)

    st.caption("Ändra värden direkt i tabellen nedan och klicka **Spara**.")
    # CHANGED: gör tabellen redigerbar
    edited = st.data_editor(
        s_df,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "Key": st.column_config.TextColumn("Key"),
            "Value": st.column_config.TextColumn("Value"),
        }
    )

    if st.button("💾 Spara Settings"):
        try:
            # säkerställ korrekta kolumner & ordning
            if "Key" not in edited.columns or "Value" not in edited.columns:
                st.error("Tabellen måste ha kolumnerna 'Key' och 'Value'.")
                return
            _write_df(SETTINGS_TITLE, edited[SETTINGS_COLUMNS])
            st.success("Settings sparade.")
        except Exception as e:
            st.error(f"Kunde inte spara: {e}")

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
# Editor (manuell + Yahoo) — med rullista för Bucket
# ============================================================
def page_editor():
    st.header("✏️ Editor (manuellt + Yahoo)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    # se till att tidsstämplar och "uppdaterad"-fält finns (samma som tidigare)
    def _ensure_editor_stamp_cols(df_in: pd.DataFrame) -> pd.DataFrame:
        cols = ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad",
                "Rev 2Y uppdaterad","Senast manuellt uppdaterad"]
        if df_in is None or df_in.empty:
            return pd.DataFrame(columns=[*DATA_COLUMNS, *cols])
        for c in cols:
            if c not in df_in.columns:
                df_in[c] = np.nan
        return df_in

    df = _ensure_editor_stamp_cols(df)
    tickers = df["Ticker"].dropna().astype(str).unique().tolist()

    # namn-karta
    def _names_map_from_df(df_in: pd.DataFrame) -> dict[str, str]:
        mp: dict[str, str] = {}
        if df_in is None or df_in.empty or "Ticker" not in df_in.columns:
            return mp
        for _, r in df_in.iterrows():
            t = str(r.get("Ticker") or "").strip()
            n = str(r.get("Bolagsnamn") or "").strip()
            if t:
                mp[t] = n
        return mp

    names_map = _names_map_from_df(df)

    # återanvänd sök + bläddra-komponenten
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
        # CHANGED: rullista för Bucket (istället för fri text)
        bucket_opts = [""] + DEFAULT_BUCKETS
        cur_bucket = str(row.get("Bucket") or "")
        bucket_in  = st.selectbox("Bucket", options=bucket_opts,
                                  index=bucket_opts.index(cur_bucket) if cur_bucket in bucket_opts else 0)
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
                df.loc[idx, "Antal aktier"] = _parse_float(antal_in) or 0.0
                if _parse_float(gav_in) is not None:
                    df.loc[idx, "GAV (SEK)"] = _parse_float(gav_in)
                # CHANGED: spara vald bucket från rullistan
                df.loc[idx, "Bucket"] = bucket_in if bucket_in else np.nan
                if _parse_float(eps1_in) is not None:
                    df.loc[idx, "EPS 1Y"] = _parse_float(eps1_in)
                if _parse_float(eps2_in) is not None:
                    df.loc[idx, "EPS 2Y"] = _parse_float(eps2_in)
                if _parse_float(rev1_in) is not None:
                    df.loc[idx, "Rev 1Y"] = _parse_float(rev1_in) * 1_000_000.0
                if _parse_float(rev2_in) is not None:
                    df.loc[idx, "Rev 2Y"] = _parse_float(rev2_in) * 1_000_000.0
                df.loc[idx, "Senast manuellt uppdaterad"] = _now()
                st.session_state["DATA"] = df
                st.success("Sparat i session.")
            except Exception as e:
                st.error(f"Fel: {e}")

    with colB:
        if st.button("⬆️ Spara till Google Sheets + Yahoo-prefill"):
            try:
                tkr = str(_nz(df.loc[idx, "Ticker"], new_ticker or sel)).upper()

                # samma Yahoo-prefill som tidigare
                y   = fetch_from_yahoo(tkr)
                est = _fetch_eps_estimates_yahoo(tkr)
                updates = {
                    "Timestamp": _now(),
                    "Aktuell kurs": _f(y.get("price")),
                    "Valuta": (y.get("currency") or df.loc[idx].get("Valuta")),
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
                    "EPS 1Y": df.loc[idx].get("EPS 1Y") if pd.notna(df.loc[idx].get("EPS 1Y")) else _f(est.get("eps_1y")),
                    "EPS 2Y": df.loc[idx].get("EPS 2Y") if pd.notna(df.loc[idx].get("EPS 2Y")) else _f(est.get("eps_2y")),
                    "Senast auto uppdaterad": _now(),
                    "Auto källa": "Yahoo",
                }

                df_cur = df.copy()
                for k, v in updates.items():
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        continue
                    if k not in df_cur.columns:
                        df_cur[k] = np.nan
                    df_cur.at[idx, k] = v

                # spara Bucket från rullistan innan skriv
                df_cur.at[idx, "Bucket"] = bucket_in if bucket_in else np.nan

                write_data_df(df_cur)
                st.session_state["DATA"] = df_cur
                st.success(f"{tkr}: Rad sparad och uppdaterad från Yahoo.")
            except Exception as e:
                st.error(f"Fel vid sparning: {e}")

    st.markdown("---")
    st.subheader("Förhandsgranskning")
    st.dataframe(df.loc[[idx]], use_container_width=True)

# ============================================================
# Lägg till ticker — med rullista för Bucket
# ============================================================
def page_add_ticker():
    st.header("➕ Lägg till ticker")

    tkr = st.text_input("Ticker").upper().strip()
    c1, c2, c3 = st.columns(3)
    with c1:
        bolagsnamn = st.text_input("Bolagsnamn")
        sektor     = st.text_input("Sektor")
    with c2:
        # CHANGED: rullista för Bucket
        bucket_opts = [""] + DEFAULT_BUCKETS
        bucket      = st.selectbox("Bucket", options=bucket_opts, index=0)
        valuta      = st.text_input("Valuta (t.ex. USD)", value="USD").upper()
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
                "Timestamp": _now(),
                "Ticker": tkr,
                "Bolagsnamn": bolagsnamn if bolagsnamn else np.nan,
                "Sektor": sektor if sektor else np.nan,
                "Bucket": bucket if bucket else np.nan,     # CHANGED: spara vald bucket
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
                        "Senast auto uppdaterad": _now(),
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
# Analys
#  • Behåller singel-analysen
#  • CHANGED: lägg till full tabell med ALLA tickers, filtrerad på vald horisont
#    (tabellen läser *sparade* riktkurser, ej live-beräkning)
# ============================================================
def page_analysis():
    st.header("🔬 Analys")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = {str(r["Ticker"]): str(_nz(r.get("Bolagsnamn"), "")) for _, r in df.iterrows() if pd.notna(r.get("Ticker"))}

    # ====== Original: singel-analys (oförändrat) ======
    tkr = _select_with_search_nav("Välj bolag", tickers, names_map, "analysis_idx", "analysis_q")
    if tkr:
        row = df.loc[df["Ticker"].astype(str) == tkr]
        if not row.empty:
            row = row.iloc[0]
            settings = get_settings_map()
            fx_map   = get_fx_map()
            with st.spinner("Beräknar…"):
                methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

            st.caption(f"Sanity: {sanity}")
            st.dataframe(methods_df, use_container_width=True)

            fv = meta.get("fair_value", {}) or {}
            st.markdown("#### 🧭 Fair Value (median över metoder)")
            cfa, cfb, cfc, cfd = st.columns(4)
            cfa.metric("FV idag", _format_num(fv.get("today")))
            cfb.metric("FV 1 år", _format_num(fv.get("y1")))
            cfc.metric("FV 2 år", _format_num(fv.get("y2")))
            cfd.metric("FV 3 år", _format_num(fv.get("y3")))

    st.markdown("---")

    # ====== CHANGED: Full bordvy över alla tickers (sparade riktkurser) ======
    st.subheader("📋 Alla bolag – sparade riktkurser")
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1, key="analysis_table_hor")
    search  = st.text_input("Filtrera (ticker/namn innehåller)", value="", key="analysis_table_q").strip().lower()

    tgt_col = {
        "Idag": "Riktkurs idag",
        "1 år": "Riktkurs 1 år",
        "2 år": "Riktkurs 2 år",
        "3 år": "Riktkurs 3 år",
    }[horizon]

    show_cols = ["Ticker","Bolagsnamn","Valuta","Aktuell kurs", tgt_col, "Primär metod", "Bucket"]
    base = df.copy()
    for c in show_cols:
        if c not in base.columns:
            base[c] = np.nan

    # filtrera på sök
    if search:
        base = base[
            base["Ticker"].astype(str).str.lower().str.contains(search) |
            base["Bolagsnamn"].astype(str).str.lower().str.contains(search)
        ].copy()

    # beräkna uppsida utifrån sparade targets
    base["Uppsida (%)"] = np.where(
        (pd.to_numeric(base["Aktuell kurs"], errors="coerce") > 0) &
        (pd.to_numeric(base[tgt_col], errors="coerce") > 0),
        (pd.to_numeric(base[tgt_col], errors="coerce") - pd.to_numeric(base["Aktuell kurs"], errors="coerce"))
        / pd.to_numeric(base["Aktuell kurs"], errors="coerce") * 100.0,
        np.nan
    )

    display = base[show_cols + ["Uppsida (%)"]].copy()
    st.dataframe(display.sort_values("Uppsida (%)", ascending=False, na_position="last"),
                 use_container_width=True, hide_index=True)

# ============================================================
# Ranking
#  • CHANGED: gör INGA beräkningar automatiskt
#  • Visar ranking baserat på SPARADE riktkurser
#  • Knapp: "Beräkna & spara riktkurser (alla)" → progress 1/X och skriv till Google Sheets
# ============================================================
def page_ranking():
    st.header("🏆 Ranking – Uppsida (sparade riktkurser)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1, key="ranking_hor")
    tgt_col = {
        "Idag": "Riktkurs idag",
        "1 år": "Riktkurs 1 år",
        "2 år": "Riktkurs 2 år",
        "3 år": "Riktkurs 3 år",
    }[horizon]

    # Visa ranking utifrån redan sparade värden
    base = df.copy()
    for c in ["Ticker","Valuta","Aktuell kurs", tgt_col, "Bucket"]:
        if c not in base.columns:
            base[c] = np.nan

    base["Uppsida (%)"] = np.where(
        (pd.to_numeric(base["Aktuell kurs"], errors="coerce") > 0) &
        (pd.to_numeric(base[tgt_col], errors="coerce") > 0),
        (pd.to_numeric(base[tgt_col], errors="coerce") - pd.to_numeric(base["Aktuell kurs"], errors="coerce"))
        / pd.to_numeric(base["Aktuell kurs"], errors="coerce") * 100.0,
        np.nan
    )
    out = base[["Ticker","Valuta","Aktuell kurs", tgt_col, "Uppsida (%)","Bucket"]].copy()
    out = out.sort_values("Uppsida (%)", ascending=False, na_position="last").reset_index(drop=True)

    st.caption(f"{len(out)} bolag")
    st.dataframe(out, use_container_width=True, hide_index=True)

    st.markdown("---")
    # CHANGED: aktiv knapp för att *beräkna & spara* riktkurser för alla rader
    st.subheader("Uppdatera riktkurser (aktiv handling)")
    if st.button("🔢 Beräkna & spara riktkurser (alla rader)"):
        settings = get_settings_map()
        fx_map   = get_fx_map()

        df_cur = df.copy()
        tickers = df_cur["Ticker"].dropna().astype(str).tolist()
        total = len(tickers)
        prog  = st.progress(0.0, text="Startar …")
        changed_rows = 0

        for i, tkr in enumerate(tickers, start=1):
            try:
                row_idx = df_cur.index[df_cur["Ticker"].astype(str) == tkr][0]
                row     = df_cur.loc[row_idx]
                methods_df, _, meta = compute_methods_for_row(row, settings, fx_map)

                # primär metod (som tidigare logik)
                meth = _pick_primary_method(row, methods_df)
                tgts = methods_df[methods_df["Metod"] == meth].iloc[0] if not methods_df.empty and (methods_df["Metod"] == meth).any() else None

                df_cur.at[row_idx, "Primär metod"] = meth if meth else df_cur.at[row_idx, "Primär metod"]
                if tgts is not None:
                    df_cur.at[row_idx, "Riktkurs idag"] = _f(tgts.get("Idag"))
                    df_cur.at[row_idx, "Riktkurs 1 år"] = _f(tgts.get("1 år"))
                    df_cur.at[row_idx, "Riktkurs 2 år"] = _f(tgts.get("2 år"))
                    df_cur.at[row_idx, "Riktkurs 3 år"] = _f(tgts.get("3 år"))
                    changed_rows += 1
            except Exception as e:
                st.warning(f"{tkr}: {e}")

            prog.progress(i/total if total else 1.0, text=f"Beräknar & sparar {i}/{total} – {tkr}")

        # skriv till Google Sheets EN gång i slutet
        write_data_df(df_cur)
        st.session_state["DATA"] = df_cur
        prog.empty()
        st.success(f"Klart. Uppdaterade riktkurser för {changed_rows} rader.")

# ============================================================
# Köpförslag (sparade riktkurser)
#  • CHANGED: använder redan SPARADE riktkurser (inga nya beräkningar)
#  • Villkor: Kurs < Riktkurs(horisont) OCH innehavsvärde(SEK) < bucket-cap
#  • Filter: Äger/Äger ej + Bucket
# ============================================================
def page_buy_suggestions():
    st.header("🛒 Köpförslag (sparade riktkurser)")
    df = st.session_state.get("DATA") or read_data_df()
    if df is None or df.empty:
        st.info("Ingen data.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    # UI-filter
    c1, c2, c3 = st.columns(3)
    with c1:
        horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1, key="buy_hor")
    with c2:
        own_filter = st.radio("Innehavsfilter", ["Alla","Endast innehav","Endast ej ägda"], index=0, horizontal=True, key="buy_owned")
    with c3:
        # bucket-filter
        buckets_available = ["Alla"] + DEFAULT_BUCKETS
        bucket_choice = st.selectbox("Bucket-filter", buckets_available, index=0, key="buy_bucket")

    tgt_col = {
        "Idag": "Riktkurs idag",
        "1 år": "Riktkurs 1 år",
        "2 år": "Riktkurs 2 år",
        "3 år": "Riktkurs 3 år",
    }[horizon]

    # bygg positions-lookup (pris, värde i SEK, kvantitet)
    pos_lu = _position_value_lookup(df, fx_map)

    rows = []
    for _, r in df.iterrows():
        tkr = str(_nz(r.get("Ticker"), "")).upper().strip()
        if not tkr:
            continue

        bucket = str(_nz(r.get("Bucket"), "") or "")
        if bucket_choice != "Alla" and bucket != bucket_choice:
            continue

        qty = _f(r.get("Antal aktier")) or 0.0
        if own_filter == "Endast innehav" and qty <= 0:
            continue
        if own_filter == "Endast ej ägda" and qty > 0:
            continue

        price = _f(_nz(r.get("Aktuell kurs"), (pos_lu.get(tkr, {}) or {}).get("price")))
        target = _f(r.get(tgt_col))
        if price is None or target is None:
            continue
        if price >= target:
            continue

        cap = _bucket_cap_per_holding(bucket, settings)  # använder befintliga Settings-nycklar
        val_sek = (pos_lu.get(tkr, {}) or {}).get("value_sek", 0.0)
        if cap and val_sek >= cap:
            continue

        up_pct = (target - price) / price * 100.0 if price else None

        rows.append({
            "Ticker": tkr,
            "Bucket": bucket,
            "Valuta": str(_nz(r.get("Valuta"), (pos_lu.get(tkr, {}) or {}).get("currency",""))).upper(),
            "Aktuell kurs": price,
            "Riktkurs": target,
            "Uppsida (%)": round(up_pct, 2) if up_pct is not None else np.nan,
            "Antal aktier": qty,
            "Värde (SEK)": val_sek or 0.0,
            "Cap per innehav (SEK)": cap,
            "Slack till cap (SEK)": (cap - (val_sek or 0.0)) if cap else np.nan,
            "Motivering": "Under fair value och under bucket-tak",
        })

    out = pd.DataFrame(rows, columns=[
        "Ticker","Bucket","Valuta","Aktuell kurs","Riktkurs","Uppsida (%)",
        "Antal aktier","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)","Motivering"
    ])
    if out.empty:
        st.info("Inga kandidater uppfyller kriterierna just nu.")
        st.caption("Kriterier: Kurs < vald riktkurs (sparad) och innehavsvärde < cap per innehav i sin bucket.")
        return

    out = out.sort_values(["Värde (SEK)", "Uppsida (%)"], ascending=[True, False]).reset_index(drop=True)
    st.caption(f"{len(out)} förslag — sorterat minsta innehavet först.")
    st.dataframe(out, use_container_width=True, hide_index=True)

# ============================================================
# Del 6/6 — Main & Routing
#  • Sidopanel, dataladdning, och vy-routing
#  • Felfångning per vy ⇒ tydligt “💥 Fel i huvudloopen: …”
#  • Ingen autoberäkning här – Ranking görs enbart via sin knapp
# ============================================================

def _safe_call(page_fn, name: str):
    try:
        page_fn()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen ({name}): {e}")

def _ensure_session_data_loaded():
    # Ladda DATA en gång (eller vid “Uppdatera data”)
    if "DATA" not in st.session_state or st.session_state.get("DATA") is None:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception as e:
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
            st.error(f"Kunde inte läsa DATA: {e}")

def _sidebar_menu_items():
    items = [
        "Analys",
        "Ranking",
        "Köpförslag",
        "Editor",
        "Lägg till ticker",
        "Snapshot",
        "Settings",
    ]
    # Lägg till Portfölj om funktionen finns i basen
    if "page_portfolio" in globals():
        items.insert(3, "Portfölj")
    return items

def main():
    st.sidebar.title("📊 Aktieanalys & investeringsförslag")
    st.sidebar.caption("Basversion 2025-11-16")

    # Datakontroll & snabbåtgärder
    _ensure_session_data_loaded()
    if st.sidebar.button("🔄 Uppdatera data från Google Sheets"):
        try:
            st.session_state["DATA"] = read_data_df()
            st.sidebar.success("Data uppdaterad.")
        except Exception as e:
            st.sidebar.error(f"Kunde inte läsa DATA: {e}")

    menu = st.sidebar.radio("Meny", _sidebar_menu_items(), index=0, key="menu_radio")

    # Routing
    if menu == "Analys":
        _safe_call(page_analysis, "Analys")
    elif menu == "Ranking":
        _safe_call(page_ranking, "Ranking")
    elif menu == "Köpförslag":
        _safe_call(page_buy_suggestions, "Köpförslag")
    elif menu == "Portfölj" and "page_portfolio" in globals():
        _safe_call(page_portfolio, "Portfölj")
    elif menu == "Editor":
        _safe_call(page_editor, "Editor")
    elif menu == "Lägg till ticker":
        _safe_call(page_add_ticker, "Lägg till ticker")
    elif menu == "Snapshot":
        _safe_call(page_snapshot, "Snapshot")
    elif menu == "Settings":
        _safe_call(page_settings, "Settings")
    else:
        # Fallback – om något oförutsett händer
        st.info("Välj en vy i sidomenyn.")

# Streamlit entrypoint
if __name__ == "__main__":
    main()
