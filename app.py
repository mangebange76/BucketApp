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
#  • EPS/Revenue TTM från kvartalssummor (fallbacks till fast_info/info)
#  • Net debt = Total debt – Cash & equivalents (kvartals-BS med fallback)
#  • Utdelningsfrekvens infererad från 12–24 mån historik
#  • Wrapper-funktioner som Editor/uppdaterare kan anropa
# ============================================================

# ---- Hjälpare för yfinance-ramar ----
def _yf_pick_attr(t: yf.Ticker, names: List[str]):
    """Returnera första existerande attributet på Ticker i 'names', annars None."""
    for n in names:
        if hasattr(t, n):
            try:
                v = getattr(t, n)
                if v is not None:
                    return v
            except Exception:
                pass
    return None

def _df_row_any(df: pd.DataFrame, keys: List[str]) -> Optional[pd.Series]:
    """Hitta första rad där index-namnet matchar någon av keys (case-insensitivt)."""
    if df is None or isinstance(df, (float, int)) or len(getattr(df, "index", [])) == 0:
        return None
    idx_l = [str(i).strip().lower() for i in df.index]
    for k in keys:
        k_l = k.strip().lower()
        for i, nm in enumerate(idx_l):
            if nm == k_l:
                return df.iloc[i]
    # ibland innehåller raderna extra mellanslag/format; försök "in" match
    for k in keys:
        k_l = k.strip().lower()
        for i, nm in enumerate(idx_l):
            if k_l in nm:
                return df.iloc[i]
    return None

def _sum_last_n(series: pd.Series, n: int = 4) -> Optional[float]:
    """Summera de n senaste icke-NaN."""
    try:
        vals = pd.to_numeric(series.dropna(), errors="coerce").dropna()
        if vals.empty:
            return None
        return float(vals.iloc[:n].sum())
    except Exception:
        return None

def _last_non_nan(series: pd.Series) -> Optional[float]:
    try:
        vals = pd.to_numeric(series.dropna(), errors="coerce").dropna()
        if vals.empty:
            return None
        return float(vals.iloc[0])
    except Exception:
        return None

def _infer_div_freq(div_series: pd.Series) -> Optional[str]:
    """
    Gissa utdelningsfrekvens: M/Q/S/A baserat på utdelningar senaste 12–24 månader.
    """
    try:
        if div_series is None or div_series.empty:
            return None
        s = div_series.sort_index(ascending=False)
        # Ta 365 dagar bakåt
        cutoff = (s.index.max() - pd.Timedelta(days=365)) if len(s.index) else None
        if cutoff is not None:
            s = s[s.index >= cutoff]
        cnt = s.shape[0]
        if cnt >= 10:
            return "M"
        if 3 <= cnt <= 5:
            return "Q"
        if 2 <= cnt <= 3:
            return "S"
        if cnt == 1:
            return "A"
        # Om tomt efter filter, titta 24 månader
        s2 = div_series.sort_index(ascending=False)
        cutoff2 = (s2.index.max() - pd.Timedelta(days=720)) if len(s2.index) else None
        if cutoff2 is not None:
            s2 = s2[s2.index >= cutoff2]
        cnt2 = s2.shape[0]
        if cnt2 >= 18:
            return "M"
        if 6 <= cnt2 <= 8:
            return "Q"
        if 3 <= cnt2 <= 5:
            return "S"
        if cnt2 == 1 or cnt2 == 2:
            return "A"
        return None
    except Exception:
        return None

# ---- Huvud: hämta snapshot från Yahoo ----
def yahoo_fetch_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar ett robust snapshot från Yahoo Finance och mappar till vårt schema.
    Fyller endast fält där data hittas – övriga lämnas som None.
    """
    out: Dict[str, Any] = {
        "Ticker": ticker,
        "Valuta": None,
        "Aktuell kurs": None,
        "Utestående aktier": None,
        "Net debt": None,
        "Rev TTM": None,
        "EBITDA TTM": None,
        "EPS TTM": None,
        "Årlig utdelning": None,
        "Utdelningsfrekvens": None,
        "Nästa utdelningsdatum": None,          # Betalningsdatum – lämnas None om okänt
        "Nästa utdelning (per aktie)": None,
        "Senast auto uppdaterad": None,
        "Auto källa": None,
    }

    try:
        t = yf.Ticker(ticker)
    except Exception:
        return out

    # --- Valuta & pris ---
    currency = None
    last_px = None
    try:
        fi = getattr(t, "fast_info", None)
        if fi:
            currency = _nz(getattr(fi, "currency", None))
            last_px  = _pos(getattr(fi, "last_price", None))
    except Exception:
        pass
    if currency is None:
        try:
            inf = getattr(t, "info", None) or {}
            currency = _nz(inf.get("currency"))
            if last_px is None:
                last_px = _pos(inf.get("currentPrice") or inf.get("regularMarketPrice"))
        except Exception:
            pass
    out["Valuta"] = currency or out["Valuta"]
    out["Aktuell kurs"] = last_px or out["Aktuell kurs"]

    # --- Utestående aktier ---
    shares = None
    try:
        if fi and getattr(fi, "shares_outstanding", None):
            shares = _pos(fi.shares_outstanding)
    except Exception:
        pass
    if shares is None:
        try:
            inf = getattr(t, "info", None) or {}
            shares = _pos(inf.get("sharesOutstanding") or inf.get("impliedSharesOutstanding"))
        except Exception:
            pass
    out["Utestående aktier"] = shares or out["Utestående aktier"]

    # --- TTM: Revenue / EBITDA / EPS ---
    q_is = _yf_pick_attr(t, ["quarterly_financials", "quarterly_income_stmt", "quarterly_income_statement"])
    a_is = _yf_pick_attr(t, ["financials", "income_stmt", "income_statement"])
    # Säkerställ att vi har DataFrame
    if isinstance(q_is, pd.DataFrame) and not q_is.empty:
        q_is = q_is.copy()
    else:
        q_is = None
    if isinstance(a_is, pd.DataFrame) and not a_is.empty:
        a_is = a_is.copy()
    else:
        a_is = None

    # Revenue TTM
    rev_row = _df_row_any(q_is, ["Total Revenue", "Revenue"])
    if rev_row is not None:
        out["Rev TTM"] = _sum_last_n(rev_row, 4)
    if out["Rev TTM"] is None and a_is is not None:
        rev_row_a = _df_row_any(a_is, ["Total Revenue", "Revenue"])
        if rev_row_a is not None:
            out["Rev TTM"] = _last_non_nan(rev_row_a)

    # EBITDA TTM (finns inte alltid per kvartal -> ta senaste årsrad om TTM saknas)
    ebitda_row = _df_row_any(q_is, ["EBITDA"])
    if ebitda_row is not None:
        out["EBITDA TTM"] = _sum_last_n(ebitda_row, 4)
    if out["EBITDA TTM"] is None and a_is is not None:
        ebitda_row_a = _df_row_any(a_is, ["EBITDA"])
        if ebitda_row_a is not None:
            out["EBITDA TTM"] = _last_non_nan(ebitda_row_a)

    # EPS TTM (kvartals-‘Diluted EPS’ eller ‘Basic EPS’, annars trailingEps)
    eps_row = _df_row_any(q_is, ["Diluted EPS", "Basic EPS", "EPS"])
    if eps_row is not None:
        eps_ttm = _sum_last_n(eps_row, 4)
        out["EPS TTM"] = eps_ttm if eps_ttm is not None else out["EPS TTM"]
    if out["EPS TTM"] is None:
        try:
            if fi and getattr(fi, "trailing_eps", None):
                out["EPS TTM"] = _pos(fi.trailing_eps)
        except Exception:
            pass
    if out["EPS TTM"] is None:
        try:
            inf = getattr(t, "info", None) or {}
            out["EPS TTM"] = _pos(inf.get("trailingEps"))
        except Exception:
            pass

    # --- Net debt = Total debt – Cash & equivalents (kvartals-BS om möjligt) ---
    q_bs = _yf_pick_attr(t, ["quarterly_balance_sheet", "balance_sheet"])
    if isinstance(q_bs, pd.DataFrame) and not q_bs.empty:
        q_bs = q_bs.copy()
        debt_row = _df_row_any(q_bs, ["Total Debt", "Total Liabilities Net Minority Interest", "Total Liabilities"])
        cash_row = _df_row_any(q_bs, ["Cash And Cash Equivalents", "Cash", "Cash And Short Term Investments"])
        total_debt = _last_non_nan(debt_row) if debt_row is not None else None
        cash_eq    = _last_non_nan(cash_row) if cash_row is not None else None
        if total_debt is not None and cash_eq is not None:
            out["Net debt"] = float(total_debt - cash_eq)
        elif total_debt is not None:
            out["Net debt"] = float(total_debt)
        # om inget hittas i kvartal, försök års-BS
        if out["Net debt"] is None:
            a_bs = _yf_pick_attr(t, ["balance_sheet"])
            if isinstance(a_bs, pd.DataFrame) and not a_bs.empty:
                a_bs = a_bs.copy()
                debt_row_a = _df_row_any(a_bs, ["Total Debt", "Total Liabilities Net Minority Interest", "Total Liabilities"])
                cash_row_a = _df_row_any(a_bs, ["Cash And Cash Equivalents", "Cash", "Cash And Short Term Investments"])
                total_debt_a = _last_non_nan(debt_row_a) if debt_row_a is not None else None
                cash_eq_a    = _last_non_nan(cash_row_a) if cash_row_a is not None else None
                if total_debt_a is not None and cash_eq_a is not None:
                    out["Net debt"] = float(total_debt_a - cash_eq_a)
                elif total_debt_a is not None:
                    out["Net debt"] = float(total_debt_a)

    # --- Utdelning (historik ⇒ årlig takt + frekvens; nästa datum lämnas None om okänt) ---
    try:
        divs = t.dividends
        if isinstance(divs, pd.Series) and not divs.empty:
            divs = divs.sort_index(ascending=False)
            freq = _infer_div_freq(divs)
            out["Utdelningsfrekvens"] = freq

            # Årlig utdelning = summera 12 senaste månaderna
            cutoff = divs.index.max() - pd.Timedelta(days=365)
            last12 = divs[divs.index >= cutoff]
            if not last12.empty:
                out["Årlig utdelning"] = float(last12.sum())

            # Nästa utdelning (per aktie) saknas ofta i Yahoo → lämna None
            out["Nästa utdelning (per aktie)"] = None
            out["Nästa utdelningsdatum"] = None
    except Exception:
        pass

    out["Senast auto uppdaterad"] = now_stamp()
    out["Auto källa"] = "Yahoo Finance"
    return out

# ---- Merge mot Data-DF ----
def yahoo_merge_into_df(df: pd.DataFrame, row_idx: int, snap: Dict[str, Any]) -> pd.DataFrame:
    """
    Skriver endast fält som finns i snapshot (ej None) till df-raden.
    Övriga fält lämnas orörda. Returnerar nytt df.
    """
    if df is None or df.empty or row_idx < 0 or row_idx >= len(df):
        return df
    row = df.iloc[row_idx].copy()
    for k, v in snap.items():
        if k in df.columns and v is not None:
            row[k] = v
    df.iloc[row_idx] = row
    return df

# ---- Publika wrappers för Editor/knappar ----
def yahoo_update_one(df: pd.DataFrame, ticker: str, row_idx: Optional[int] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Hämta Yahoo-snapshot för 'ticker' och skriv in i df på vald rad.
    Om row_idx är None försöker vi hitta rad via Ticker-kolumnen (första match).
    """
    if df is None or df.empty:
        return df, {}
    if row_idx is None:
        hit = df.index[df["Ticker"].astype(str).str.upper() == str(ticker).upper()]
        if len(hit) == 0:
            return df, {}
        row_idx = int(hit[0])

    snap = yahoo_fetch_snapshot(ticker)
    if not snap:
        return df, {}

    df2 = yahoo_merge_into_df(df.copy(), row_idx, snap)
    return df2, snap

def yahoo_update_many(df: pd.DataFrame, tickers: List[str]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Massuppdatera flera tickers i ordning. Returnerar nytt df samt lista med snapshots.
    (Fördröjning/Progress hanteras i UI-del senare.)
    """
    out_df = df.copy()
    snaps: List[Dict[str, Any]] = []
    for tk in tickers:
        try:
            out_df, snap = yahoo_update_one(out_df, tk)
            if snap:
                snaps.append(snap)
        except Exception:
            # fortsätt trots fel i enskilt bolag
            pass
    return out_df, snaps

# ============================================================
# Del 3/6 — UI: Editor & massuppdatering
#  • Editor-vy som använder yahoo_update_one/many
#  • Manuell komplettering där Yahoo saknar fält
#  • Statusfält & bekräftelser
# ============================================================

# ============================================================
# Del 3/6 — Beräkningsmotor & Fair Value
#  • fetch_from_yahoo() → wrapper runt yahoo_fetch_snapshot (Del 2)
#  • EPS-estimat (Yahoo earnings_trend) + clampade CAGR
#  • Metoder: pe_hist_vs_eps, ev_sales, ev_ebitda, ev_dacf, p_b*
#  • Multipel-decay & PE-ankare
#  • ✅ Fair Value = median över oberoende metodfamiljer (kurs-kopia filtreras)
# ============================================================

# ---------- Wrapper mot Del 2 ----------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Mappar Del 2/6:s yahoo_fetch_snapshot() till beräkningsnycklar.
    Beräknar EV/S och EV/EBITDA on-the-fly när price/shares/net_debt finns.
    """
    snap = yahoo_fetch_snapshot(ticker)  # från Del 2
    price   = _f(snap.get("Aktuell kurs"))
    ccy     = (snap.get("Valuta") or "USD")
    shares  = _f(snap.get("Utestående aktier"))
    net_debt= _f(snap.get("Net debt"))
    rev_ttm = _f(snap.get("Rev TTM"))
    ebitda  = _f(snap.get("EBITDA TTM"))
    eps_ttm = _f(snap.get("EPS TTM"))

    mcap = (price * shares) if (_pos(price) and _pos(shares)) else None
    ev   = (mcap + net_debt) if (mcap is not None and net_debt is not None) else mcap

    ev_rev    = (ev / rev_ttm) if (_pos(ev) and _pos(rev_ttm)) else None
    ev_ebitda = (ev / ebitda)  if (_pos(ev) and _pos(ebitda) and ebitda != 0) else None

    pe_ttm = (price / eps_ttm) if (_pos(price) and _pos(eps_ttm) and eps_ttm != 0) else None

    return {
        "price":          _f(price),
        "currency":       ccy,
        "shares_out":     _f(shares),
        "net_debt":       _f(net_debt),
        "rev_ttm":        _f(rev_ttm),
        "ebitda_ttm":     _f(ebitda),
        "eps_ttm":        _f(eps_ttm),
        "pe_ttm":         _f(pe_ttm),
        "pe_fwd":         None,          # fylls om vi senare har källa
        "ev_rev":         _f(ev_rev),
        "ev_ebitda":      _f(ev_ebitda),
        "p_b":            None,          # kräver BVPS/Equity – lämnas None om saknas
        "bvps":           None,
        "dps_annual":     _f(snap.get("Årlig utdelning")),
        "rev_cagr_hist":  None,          # historiska CAGR kan saknas → clamp hanteras nedan
        "eps_cagr_hist":  None,
    }

# ---------- EPS-estimat (Yahoo) ----------
@st.cache_data(ttl=900, show_spinner=False)
def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker läsa EPS-estimat (nästa år) + långsiktig EPS-CAGR från Yahoo.
    Robust mot saknade tabeller/kolumner.
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

        def _avg(val):
            if isinstance(val, dict):
                for k in ("avg", "average", "mean"):
                    if k in val and _f(val[k]) is not None:
                        return _f(val[k])
            return _f(val)

        def _pick(period_aliases: list[str]):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for a in period_aliases:
                cur = m.str.contains(rf"^{a}$")
                mask = cur if mask is None else (mask | cur)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_curr = _pick(["currentyear", "current fiscal year", "currentfiscalyear"])
        row_next = _pick(["nextyear", "next fiscal year", "nextfiscalyear"])
        row_long = _pick(["longterm", "next5years", "next 5 years"])

        eps_1y = None
        for r in (row_next, row_curr):
            if r is None: 
                continue
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg(r.get(col))
                    if eps_1y is not None:
                        break
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

# ---------- Clamp-parametrar ----------
REV_CAGR_MIN = -0.10
REV_CAGR_MAX =  0.35
EPS_CAGR_MIN = -0.20
EPS_CAGR_MAX =  0.35

# ---------- Små hjälpare ----------
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
    if pt is None and pf is None: return None
    if pt is None: return pf
    if pf is None: return pt
    return float(w_ttm) * pt + (1.0 - float(w_ttm)) * pf

def _equity_price_from_ev(ev_target: Optional[float], net_debt: Optional[float], shares_fd: Optional[float]) -> Optional[float]:
    e = _pos(ev_target); s = _pos(shares_fd)
    if e is None or s is None: return None
    nd = _nz(net_debt, 0.0)
    return max(0.0, (e - nd) / s)

def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e = _pos(eps); p = _pos(pe)
    return (e * p) if (e is not None and p is not None) else None

def _ev_from_sales(rev: Optional[float], mult: Optional[float]) -> Optional[float]:
    r = _pos(rev); m = _pos(mult)
    return (r * m) if (r is not None and m is not None) else None

def _ev_from_ebitda(ebitda: Optional[float], mult: Optional[float]) -> Optional[float]:
    m = _pos(mult)
    return (float(ebitda) * m) if (ebitda is not None and m is not None) else None

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    p = _pos(pb); b = _pos(bvps)
    return (p * b) if (p is not None and b is not None) else None

def _eps_path_fill(eps_ttm: Optional[float], eps_1y: Optional[float], eps_2y: Optional[float],
                   eps_cagr_hist: Optional[float], eps_cagr_long: Optional[float],
                   rev_cagr_hist: Optional[float]) -> tuple[float, float, float, float]:
    e0 = _pos(eps_ttm) or 0.0
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    g = None
    for cand in (eps_cagr_hist, eps_cagr_long, rev_cagr_hist, 0.0):
        if _f(cand) is not None:
            g = float(_f(cand)); break
    if e1 is None: e1 = e0 * (1.0 + (g or 0.0))
    if e2 is None: e2 = (e1 or 0.0) * (1.0 + (g or 0.0))
    e3 = (e2 or 0.0) * (1.0 + (g or 0.0))
    return float(e0), float(e1), float(e2), float(e3)

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float],
                 rev2: Optional[float], rev3: Optional[float]) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    b0 = _f(ebitda_ttm)
    if b0 is None: return None, None, None, None
    if rev0 is None or rev1 is None: return b0, b0, b0, b0
    def scale(r):
        try: return (b0 * (r / rev0)) if (r and rev0) else b0
        except Exception: return b0
    return b0, scale(rev1), scale(rev2), scale(rev3)

# ---------- Fair Value (familjemedian + kursfilter) ----------
def _compute_fair_value_row_v2(methods_df: pd.DataFrame, now_price: Optional[float]) -> dict:
    fam_map = {
        "pe_hist_vs_eps": "pe",
        "ev_sales":       "ev_s",
        "ev_ebitda":      "ev_e",
        "ev_dacf":        "ev_e",
        "p_b":            "pb",
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
            # Filtrera bort "kurs-kopia" i "Idag"
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:  # ±0.5 %
                    continue
            used_fams.add(fam)
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
def compute_methods_for_row(row: pd.Series, settings: dict[str, str], fx_map: dict[str, float]) -> tuple[pd.DataFrame, str, dict]:
    """
    Returnerar:
      • methods_df: DataFrame [Metod, Idag, 1 år, 2 år, 3 år]
      • sanity    : str
      • meta      : dict (inputs, paths, fair_value)
    Alla target i bolagets egen handelsvaluta.
    """
    ticker = str(row.get("Ticker", "")).strip()
    y   = fetch_from_yahoo(ticker)
    est = _fetch_eps_estimates_yahoo(ticker)

    # Inputs (fallback mot Data-bladet)
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

    # EPS-estimat (fallback till tomt)
    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), est.get("eps_1y")))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), est.get("eps_2y")))

    # Historisk CAGR (clamp om/ när vi har källor i Data-bladet)
    rev_cagr_hist_raw = _f(row.get("Rev CAGR"))
    rev_cagr_hist = max(REV_CAGR_MIN, min(REV_CAGR_MAX, rev_cagr_hist_raw)) if rev_cagr_hist_raw is not None else None
    eps_cagr_hist_raw = _f(row.get("EPS CAGR"))
    eps_cagr_hist = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_hist_raw)) if eps_cagr_hist_raw is not None else None

    eps_cagr_long = _f(est.get("eps_cagr_long"))
    if eps_cagr_long is not None:
        eps_cagr_long = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_long))

    # P/E-ankare & decay
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

    # EBITDA-path (skala mot intäkt)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multipel-decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales,  _decay_multiple(ev_sales,  1, decay), _decay_multiple(ev_sales,  2, decay), _decay_multiple(ev_sales,  3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,       _decay_multiple(p_b,       1, decay), _decay_multiple(p_b,       2, decay), _decay_multiple(p_b,       3, decay)

    # Metoder (priser i bolagets valuta)
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
    # DACF-proxy = EV/EBITDA tills vi har explicit op. cash flow
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
    # Platshållare för framtida metoder – behåller struktur
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # ✅ Fair Value-rad
    fv_row = _compute_fair_value_row_v2(methods_df, price)
    methods_df = pd.concat([pd.DataFrame([fv_row]), methods_df], ignore_index=True)

    # Sanity-text
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
# Del 4/6 — Analysvy & Fair Value-UI
#  • build_analysis_panel(df_data, settings, fx_map)
#  • render_methods_table(), render_meta_debug()
#  • Per-ticker analys: Fair Value idag + 1/2/3 år, uppsida vs kurs
# ============================================================

# ---------- Små UI-hjälpare ----------
def _fmt_num(x: Optional[float], nd: int = 2) -> str:
    v = _f(x)
    if v is None:
        return "—"
    try:
        return f"{float(v):.{nd}f}"
    except Exception:
        return "—"

def _fmt_ccy(x: Optional[float], ccy: str, nd: int = 2) -> str:
    return f"{_fmt_num(x, nd)} {ccy}"

def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    aa, bb = _pos(a), _pos(b)
    if aa is None or bb is None or bb == 0:
        return None
    return (aa / bb - 1.0) * 100.0

def _round_df_prices(df: pd.DataFrame, cols: list[str], nd: int = 2) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].apply(lambda v: None if _f(v) is None else round(float(v), nd))
    return out

# ---------- Rendering ----------
def render_methods_table(methods_df: pd.DataFrame, currency: str) -> None:
    cols_price = ["Idag", "1 år", "2 år", "3 år"]
    df_view = _round_df_prices(methods_df, cols_price, 2)
    # Visa valuta i kolumnrubriker
    renamed = {c: f"{c} ({currency})" for c in cols_price}
    df_view = df_view.rename(columns=renamed)
    st.dataframe(df_view, use_container_width=True)

def render_meta_debug(sanity: str, meta: dict) -> None:
    with st.expander("Diagnostik & indata", expanded=False):
        st.write("**Sanity**:", sanity)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Allmänt**")
            st.write({
                "Valuta": meta.get("currency"),
                "Kurs (nu)": meta.get("price"),
                "Utest. aktier": meta.get("shares_out"),
                "Net debt": meta.get("net_debt"),
            })
        with c2:
            st.markdown("**Multipelparametrar**")
            st.write({
                "PE-ankare": meta.get("pe_anchor"),
                "Decay": meta.get("decay"),
            })
            st.markdown("**CAGR clamp**")
            st.write(meta.get("cagr_clamped", {}))
        with c3:
            st.markdown("**Fair Value (sammanfattning)**")
            st.write(meta.get("fair_value", {}))

        st.markdown("**EPS-path**")
        st.write(meta.get("eps_path", {}))
        st.markdown("**Revenue-path**")
        st.write(meta.get("rev_path", {}))
        st.markdown("**EBITDA-path**")
        st.write(meta.get("ebitda_path", {}))

# ---------- Analyspanel ----------
def build_analysis_panel(df_data: pd.DataFrame, settings: dict[str, str], fx_map: dict[str, float]) -> None:
    """
    Enkel analysvy per ticker.
    Förutsätter att df_data har kolumner som 'Ticker', 'Valuta', 'Aktuell kurs' m.fl.
    """
    st.subheader("Analys")
    if df_data is None or df_data.empty:
        st.info("Ingen data att visa.")
        return

    tickers = df_data["Ticker"].astype(str).dropna().unique().tolist()
    tickers = sorted([t for t in tickers if t.strip()])

    csel, cbtn = st.columns([3, 1])
    with csel:
        ticker = st.selectbox("Välj ticker", tickers, index=0)
    with cbtn:
        run_now = st.button("Beräkna", use_container_width=True)

    # Autoberäkna första gången för vald ticker
    if run_now or True:
        row = df_data[df_data["Ticker"].astype(str) == str(ticker)].iloc[0].copy()
        with st.spinner(f"Beräknar metoder & Fair Value för {ticker} …"):
            methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

        currency = meta.get("currency", str(row.get("Valuta") or "USD"))
        now_price = meta.get("price") or _f(row.get("Aktuell kurs"))

        # Plocka Fair Value-raden
        fv_row = methods_df[methods_df["Metod"] == "fair_value"].iloc[0] if not methods_df.empty else None
        fv_today = _f(fv_row.get("Idag")) if fv_row is not None else None
        fv_y1    = _f(fv_row.get("1 år")) if fv_row is not None else None
        fv_y2    = _f(fv_row.get("2 år")) if fv_row is not None else None
        fv_y3    = _f(fv_row.get("3 år")) if fv_row is not None else None

        up_pct = _pct(fv_today, now_price)

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("Aktuell kurs", _fmt_ccy(now_price, currency))
        with k2:
            st.metric("Fair Value (idag)", _fmt_ccy(fv_today, currency))
        with k3:
            st.metric("Uppsida % vs kurs", _fmt_num(up_pct, 2) + " %")
        with k4:
            st.metric("Fair Value (1 år)", _fmt_ccy(fv_y1, currency))

        k5, k6 = st.columns(2)
        with k5:
            st.metric("Fair Value (2 år)", _fmt_ccy(fv_y2, currency))
        with k6:
            st.metric("Fair Value (3 år)", _fmt_ccy(fv_y3, currency))

        st.markdown("#### Metodtabell")
        render_methods_table(methods_df, currency)

        render_meta_debug(sanity, meta)

# ============================================================
# Del 5/6 — Vyer
#  • Settings
#  • Snapshot
#  • Editor (manuellt + Yahoo-prefill)
#  • Lägg till ticker
#  • Portfölj (värde i SEK, netto-utdelningar & kommande utbetalningar)
# ============================================================

# ---------- Små helpers (UI/parse) ----------
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

# Liten wrapper om Del 2 inte exporterat exakt denna util i global scope
if "yahoo_snapshot" not in globals():
    @st.cache_data(ttl=900, show_spinner=False)
    def yahoo_snapshot(ticker: str) -> dict:
        y = fetch_from_yahoo(ticker)  # Del 3 wrapper → Del 2
        return {"Aktuell kurs": _f(y.get("price")), "Valuta": y.get("currency")}

# ============================================================
# Settings
# ============================================================
def page_settings():
    st.header("⚙️ Settings")

    s = get_settings_map()
    fx_df = _read_df(FX_TITLE)

    c1, c2, c3 = st.columns(3)
    with c1:
        primary = st.selectbox("Primär valuta", ["SEK","USD","EUR","NOK","CAD"],
                               index=["SEK","USD","EUR","NOK","CAD"].index(s.get("primary_currency","SEK")))
        pe_w = float(_f(s.get("pe_anchor_weight_ttm")) or 0.50)
        pe_w = st.number_input("Vikt TTM i PE-ankare", 0.0, 1.0, pe_w, 0.05)
    with c2:
        decay = float(_f(s.get("multiple_decay")) or 0.10)
        decay = st.number_input("Multipel-decay/år", 0.0, 0.5, decay, 0.01)
        auto_fx = st.checkbox("Auto-uppdatera FX vid start", value=str(s.get("auto_refresh_on_start","0"))=="1")
    with c3:
        st.caption("Källskatt per valuta (brutto → netto)")
        wh_usd = st.number_input("USD", 0.0, 0.5, float(_f(s.get("withholding_USD", s.get("tax_usd"))) or 0.15), 0.01)
        wh_nok = st.number_input("NOK", 0.0, 0.5, float(_f(s.get("withholding_NOK", s.get("tax_nok"))) or 0.25), 0.01)
        wh_cad = st.number_input("CAD", 0.0, 0.5, float(_f(s.get("withholding_CAD", s.get("tax_cad"))) or 0.15), 0.01)
        wh_eur = st.number_input("EUR", 0.0, 0.5, float(_f(s.get("withholding_EUR", s.get("tax_eur"))) or 0.15), 0.01)
        wh_sek = st.number_input("SEK", 0.0, 0.5, float(_f(s.get("withholding_SEK", s.get("tax_sek"))) or 0.00), 0.01)

    if st.button("💾 Spara inställningar"):
        s_df = _read_df(SETTINGS_TITLE)
        if s_df.empty:
            s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)

        def set_kv(k, v):
            nonlocal s_df
            mask = (s_df["Key"].astype(str) == k) if not s_df.empty else pd.Series([], dtype=bool)
            if not s_df.empty and mask.any():
                s_df.loc[mask, "Value"] = str(v)
            else:
                s_df = pd.concat([s_df, pd.DataFrame([[k, str(v)]], columns=SETTINGS_COLUMNS)], ignore_index=True)

        set_kv("primary_currency", primary)
        set_kv("pe_anchor_weight_ttm", pe_w)
        set_kv("multiple_decay", decay)
        set_kv("auto_refresh_on_start", "1" if auto_fx else "0")
        # Källskatt (legacy + override)
        set_kv("withholding_USD", wh_usd); set_kv("tax_usd", wh_usd)
        set_kv("withholding_NOK", wh_nok); set_kv("tax_nok", wh_nok)
        set_kv("withholding_CAD", wh_cad); set_kv("tax_cad", wh_cad)
        set_kv("withholding_EUR", wh_eur); set_kv("tax_eur", wh_eur)
        set_kv("withholding_SEK", wh_sek); set_kv("tax_sek", wh_sek)

        _write_df(SETTINGS_TITLE, s_df[SETTINGS_COLUMNS])
        st.success("Inställningar sparade.")

    st.markdown("---")
    st.subheader("Valutakurser")
    st.dataframe(fx_df, use_container_width=True)
    if st.button("🔁 Uppdatera valutakurser (Yahoo)"):
        _load_fx_and_update_sheet()
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
# Editor-hjälpare
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

# Yahoo-uppdateringar för Editor/Lägg till
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series) -> dict:
    y   = fetch_from_yahoo(ticker)
    est = _fetch_eps_estimates_yahoo(ticker)
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

# ============================================================
# Editor
# ============================================================
def page_editor():
    st.header("✏️ Editor (manuella fält)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    df = _ensure_editor_stamp_cols(df)
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
    st.dataframe(df.loc[[idx]], use_container_width=True)

# ============================================================
# Lägg till ticker
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

# ============================================================
# Portfölj (SEK-värden & kommande utdelningar)
# ============================================================
def _withholding_rate(currency: str, settings: dict[str, str]) -> float:
    c = (currency or "").upper().strip()
    overrides = {
        "USD": _f(settings.get("tax_usd")),
        "CAD": _f(settings.get("tax_cad")),
        "NOK": _f(settings.get("tax_nok")),
        "SEK": _f(settings.get("tax_sek")),
        "EUR": _f(settings.get("tax_eur")),
        "DKK": _f(settings.get("tax_dkk")),
        "GBP": _f(settings.get("tax_gbp")),
    }
    base = {"USD": 0.15, "CAD": 0.15, "NOK": 0.25, "SEK": 0.00}
    if overrides.get(c) is not None:
        return float(overrides[c])
    return float(base.get(c, 0.0))

def _to_sek(amount_in_ccy: Optional[float], currency: str, fx_map: dict[str, float]) -> Optional[float]:
    a = _f(amount_in_ccy)
    if a is None:
        return None
    rate = _f(fx_map.get((currency or "").upper(), 1.0)) or 1.0
    try:
        return float(a) * float(rate)
    except Exception:
        return None

def _compute_next_div_row(row: pd.Series, fx_map: dict[str, float], settings: dict[str, str]) -> Optional[dict]:
    ticker = str(row.get("Ticker", "")).strip()
    ccy    = str(row.get("Valuta", "SEK")).upper().strip()
    shares = _pos(row.get("Antal aktier")) or 0.0

    pay_raw = row.get("Nästa utdelningsdatum")
    pay_dt: Optional[dt.date] = None
    try:
        if isinstance(pay_raw, (dt.date, dt.datetime, pd.Timestamp)):
            pay_dt = pd.to_datetime(pay_raw).date()
        elif isinstance(pay_raw, str) and pay_raw.strip():
            pay_dt = pd.to_datetime(pay_raw, dayfirst=True, errors="coerce").date()
    except Exception:
        pay_dt = None

    today = dt.date.today()
    if pay_dt is None or pay_dt < today:
        return None

    dps_gross = _f(row.get("Nästa utdelning (per aktie)")) or _f(row.get("Nästa utdelning per aktie"))
    if dps_gross is None or shares <= 0:
        return None

    tax = _withholding_rate(ccy, settings)
    dps_net = float(dps_gross) * (1.0 - float(tax))
    gross_amt = dps_gross * shares
    net_amt   = dps_net * shares

    gross_sek = _to_sek(gross_amt, ccy, fx_map)
    net_sek   = _to_sek(net_amt,   ccy, fx_map)

    return {
        "Datum": pay_dt,
        "Ticker": ticker,
        "Valuta": ccy,
        "Antal": float(shares),
        "DPS (brutto)": float(dps_gross),
        "Skatt (%)": float(tax * 100.0),
        "DPS (netto)": float(dps_net),
        "Belopp (brutto)": float(gross_amt),
        "Belopp (netto)": float(net_amt),
        "Belopp SEK (netto)": _f(net_sek),
    }

def _annual_dividend_net_sek(row: pd.Series, fx_map: dict[str, float], settings: dict[str, str]) -> float:
    ccy    = str(row.get("Valuta", "SEK")).upper().strip()
    shares = _pos(row.get("Antal aktier")) or 0.0
    dps_y  = _f(row.get("Årlig utdelning"))
    if dps_y is None or shares <= 0:
        return 0.0
    tax = _withholding_rate(ccy, settings)
    net = float(dps_y) * (1.0 - float(tax)) * float(shares)
    sek = _to_sek(net, ccy, fx_map) or 0.0
    return float(sek)

def _position_value_sek(row: pd.Series, fx_map: dict[str, float]) -> float:
    ccy    = str(row.get("Valuta", "SEK")).upper().strip()
    price  = _f(row.get("Aktuell kurs"))
    if price is None:
        tk = str(row.get("Ticker", "")).strip()
        try:
            snap = yahoo_snapshot(tk)
            price = _f(snap.get("Aktuell kurs"))
            if not ccy:
                ccy = str(snap.get("Valuta") or "SEK").upper()
        except Exception:
            price = None
    shares = _pos(row.get("Antal aktier")) or 0.0
    if price is None or shares <= 0:
        return 0.0
    return float(_to_sek(price * shares, ccy, fx_map) or 0.0)

def _build_portfolio_tables(df_data: pd.DataFrame, fx_map: dict[str, float], settings: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    base = df_data.copy()
    if "Antal aktier" in base.columns:
        base["Antal aktier"] = base["Antal aktier"].apply(_f)

    own = base[(base.get("Antal aktier") > 0) if "Antal aktier" in base.columns else []].copy()

    # Positionsvärden
    pos_vals = []
    for _, r in own.iterrows():
        ccy = str(r.get("Valuta", "SEK")).upper().strip()
        price = _f(r.get("Aktuell kurs"))
        if price is None:
            try:
                snap = yahoo_snapshot(str(r.get("Ticker", "")))
                price = _f(snap.get("Aktuell kurs"))
                if not r.get("Valuta"):
                    ccy = str(snap.get("Valuta") or "SEK").upper()
            except Exception:
                price = None
        value_ccy = (price or 0.0) * (_pos(r.get("Antal aktier")) or 0.0)
        value_sek = _to_sek(value_ccy, ccy, fx_map) or 0.0
        pos_vals.append({
            "Ticker": str(r.get("Ticker", "")).strip(),
            "Valuta": ccy,
            "Antal": float(_pos(r.get("Antal aktier")) or 0.0),
            "Aktuell kurs": _f(price),
            "Värde (valuta)": float(value_ccy),
            "Värde (SEK)": float(value_sek),
        })
    positions_df = pd.DataFrame(pos_vals) if pos_vals else pd.DataFrame(columns=["Ticker","Valuta","Antal","Aktuell kurs","Värde (valuta)","Värde (SEK)"])

    # Kommande utdelningar
    up_rows = []
    for _, r in own.iterrows():
        row = _compute_next_div_row(r, fx_map, settings)
        if row:
            up_rows.append(row)
    upcoming_df = pd.DataFrame(up_rows) if up_rows else pd.DataFrame(columns=["Datum","Ticker","Valuta","Antal","DPS (brutto)","Skatt (%)","DPS (netto)","Belopp (netto)","Belopp SEK (netto)"])
    if not upcoming_df.empty:
        upcoming_df = upcoming_df.sort_values(["Datum","Ticker"]).reset_index(drop=True)

    # Summering
    total_value_sek = float(positions_df["Värde (SEK)"].sum()) if not positions_df.empty else 0.0
    total_div_year  = float(sum(_annual_dividend_net_sek(r, fx_map, settings) for _, r in own.iterrows()))
    per_month       = total_div_year / 12.0

    summary = {
        "total_value_sek": total_value_sek,
        "total_div_year_sek": total_div_year,
        "div_per_month_sek": per_month,
        "count_positions": int(len(positions_df)),
        "count_upcoming": int(len(upcoming_df)),
    }
    return positions_df, upcoming_df, summary

def render_portfolio_view(df_data: pd.DataFrame, fx_map: dict[str, float], settings: dict[str, str]):
    positions_df, upcoming_df, summary = _build_portfolio_tables(df_data, fx_map, settings)

    col1, col2, col3, col4 = st.columns([1.2,1.2,1.2,1.0])
    col1.metric("Totalt portföljvärde (SEK)", f"{summary['total_value_sek']:,.0f}".replace(",", " "))
    col2.metric("Årlig utdelning (SEK, netto)", f"{summary['total_div_year_sek']:,.0f}".replace(",", " "))
    col3.metric("Utdelning/månad (SEK, netto)", f"{summary['div_per_month_sek']:,.0f}".replace(",", " "))
    col4.metric("Innehav", f"{summary['count_positions']} st")

    st.markdown("### Kommande utdelningsutbetalningar (netto, SEK)")
    if upcoming_df.empty:
        st.info("Inga kommande betalningsdatum hittades (kontrollera kolumnerna ”Nästa utdelningsdatum” och ”Nästa utdelning (per aktie)”).")
    else:
        show_cols = ["Datum","Ticker","Valuta","Antal","DPS (brutto)","Skatt (%)","DPS (netto)","Belopp (netto)","Belopp SEK (netto)"]
        st.dataframe(upcoming_df[show_cols], use_container_width=True, hide_index=True)

    st.markdown("### Innehavsvärden (SEK)")
    if positions_df.empty:
        st.warning("Inga positioner med Antal > 0.")
    else:
        st.dataframe(positions_df, use_container_width=True, hide_index=True)

# Wrapper-sidor för portfölj och analyspanel (Del 4 visade analys-panel)
def page_portfolio():
    st.header("📦 Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx = st.session_state.get("FX", {}) or get_fx_map()
    settings = get_settings_map()
    try:
        render_portfolio_view(df, fx, settings)
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")

def page_analysis_panel():
    """Koppla Del 4:s analyspanel."""
    st.header("🔬 Analys (per bolag)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    settings = get_settings_map()
    fx_map   = get_fx_map()
    # Del 4 definierade build_analysis_panel()
    build_analysis_panel(df, settings, fx_map)

# ============================================================
# Del 6/6 — Main, routing & massuppdatering
#  • Bootstrap (DATA, FX, Settings)
#  • Sidomeny & navigering
#  • Ladda/Spara/Refresh-knappar
#  • Massuppdatera alla (Yahoo) med 1s delay + status
#  • Kallar respektive sida: Analys, Portfölj, Editor, Lägg till, Snapshot, Settings
# ============================================================

# ---------- Bootstrapping ----------
def _bootstrap_once():
    """Läs in DATA, FX och Settings till session_state. Körs en gång per körning."""
    if "bootstrapped" in st.session_state:
        return

    # Läs DATA
    try:
        df0 = read_data_df()
    except Exception as e:
        st.error(f"Kunde inte läsa DATA från Google Sheets: {e}")
        df0 = pd.DataFrame(columns=DATA_COLUMNS)

    # Säkerställ kolumner
    for c in DATA_COLUMNS:
        if c not in df0.columns:
            df0[c] = np.nan

    st.session_state["DATA"] = df0

    # Läs Settings & FX
    settings = get_settings_map()
    st.session_state["SETTINGS_MAP"] = settings

    # Auto FX om flaggat
    try:
        if str(settings.get("auto_refresh_on_start", "0")) == "1":
            _load_fx_and_update_sheet()
    except Exception as e:
        st.warning(f"FX auto-uppdatering misslyckades: {e}")

    st.session_state["FX"] = get_fx_map()

    st.session_state["bootstrapped"] = True


# ---------- Massuppdatering (Yahoo) ----------
YAHOO_UPDATE_FIELDS = [
    "Aktuell kurs", "Valuta", "Utestående aktier", "Net debt", "Rev TTM", "EBITDA TTM",
    "EPS TTM", "PE TTM", "PE FWD", "EV/Revenue", "EV/EBITDA", "P/B", "BVPS",
    "Rev CAGR", "EPS CAGR", "Årlig utdelning", "EPS 1Y", "EPS 2Y",
    "Senast auto uppdaterad", "Auto källa"
]

def _apply_yahoo_updates_to_row(df: pd.DataFrame, idx: int, ticker: str) -> dict:
    """
    Hämtar fält från Yahoo och fyller i endast de fält som finns.
    Återanvänder samma logik som Editor (_build_updates_from_yahoo).
    """
    base = df.loc[idx]
    updates = _build_updates_from_yahoo(ticker, base)
    if not updates:
        return {"updated": 0, "ticker": ticker}

    for k, v in updates.items():
        if k not in df.columns:
            df[k] = np.nan
        df.at[idx, k] = v
    return {"updated": len(updates), "ticker": ticker}

def page_mass_update():
    st.header("🛠️ Massuppdatera alla (Yahoo)")
    df = st.session_state.get("DATA")
    if df is None or df.empty:
        st.info("Ingen DATA att uppdatera.")
        return

    tickers = df["Ticker"].fillna("").astype(str).str.strip()
    tickers = [t for t in tickers if t]

    st.write(f"Hittade **{len(tickers)}** tickers i DATA.")
    only_owned = st.checkbox("Uppdatera endast innehav (Antal aktier > 0)", value=False)
    if only_owned and "Antal aktier" in df.columns:
        idxs = df.index[(pd.to_numeric(df["Antal aktier"], errors="coerce") > 0)]
    else:
        idxs = df.index

    # Välj ordning
    order = st.selectbox("Ordning", ["A→Ö (ticker)", "Ö→A (ticker)", "Som i bladet"])
    if order == "A→Ö (ticker)":
        idxs = df.loc[idxs].sort_values("Ticker").index
    elif order == "Ö→A (ticker)":
        idxs = df.loc[idxs].sort_values("Ticker", ascending=False).index
    # annars: som i bladet

    delay = st.number_input("Fördröjning per bolag (sek)", min_value=0.0, max_value=5.0, value=1.0, step=0.5)
    do_recompute = st.checkbox("Räkna om analys (fair value) efter varje uppdatering", value=False,
                               help="Kallar compute_methods_for_row(row) om funktionen finns.")

    if st.button("🚀 Starta massuppdatering"):
        prog = st.progress(0.0)
        status = st.empty()
        total = len(idxs)
        updated_rows = 0
        errors = []

        df_work = df.copy()

        for i, ix in enumerate(idxs, start=1):
            tkr = str(df_work.at[ix, "Ticker"]).strip()
            status.write(f"Uppdaterar **{tkr}** ({i}/{total}) …")
            try:
                res = _apply_yahoo_updates_to_row(df_work, ix, tkr)
                if do_recompute and "compute_methods_for_row" in globals():
                    try:
                        # Recompute fair value/targets om funktionen finns i Del 3/4
                        row_new = df_work.loc[ix]
                        comp = compute_methods_for_row(row_new)  # förväntas returnera dict med beräknade fält
                        if isinstance(comp, dict):
                            for k, v in comp.items():
                                if k not in df_work.columns:
                                    df_work[k] = np.nan
                                df_work.at[ix, k] = v
                    except Exception as e:
                        # Fortsätt även om analys-steget fallerar för enskild rad
                        errors.append(f"{tkr}: analysfel {e}")

                updated_rows += 1 if res.get("updated", 0) > 0 else 0

                # Skriv efter varje rad (tydlig status och robusthet)
                write_data_df(df_work)

                # Delay
                if delay and delay > 0:
                    time.sleep(float(delay))
            except Exception as e:
                errors.append(f"{tkr}: {e}")

            prog.progress(i / total)

        # Uppdatera session
        st.session_state["DATA"] = df_work

        # Slutrapport
        if errors:
            st.warning("Klar med vissa fel. Se lista nedan.")
            for err in errors[:100]:
                st.caption("• " + err)
            if len(errors) > 100:
                st.caption(f"… och {len(errors)-100} till.")
        st.success(f"Massuppdatering klar. Uppdaterade {updated_rows} av {total} rader.")

        # Liten hint
        st.info("Tips: Gå till **Analys** eller **Portfölj** för att se uppdaterade siffror.")


# ---------- Sidomeny & router ----------
def _sidebar_header():
    st.sidebar.markdown("### Aktieanalys & investeringsförslag")
    st.sidebar.caption("Bas: Google Sheets + Yahoo. Fair value enligt dina inställningar.")
    # Snabbkommandon
    if st.sidebar.button("🔄 Ladda om DATA från Google Sheets"):
        try:
            df = read_data_df()
            st.session_state["DATA"] = df
            st.sidebar.success("DATA laddad.")
        except Exception as e:
            st.sidebar.error(f"Kunde inte läsa DATA: {e}")

    if st.sidebar.button("💾 Spara DATA → Google Sheets"):
        try:
            df = st.session_state.get("DATA", pd.DataFrame(columns=DATA_COLUMNS))
            # Säkerställ schema vid skriv
            for c in DATA_COLUMNS:
                if c not in df.columns:
                    df[c] = np.nan
            write_data_df(df)
            st.sidebar.success("Sparat.")
        except Exception as e:
            st.sidebar.error(f"Fel vid sparning: {e}")

    if st.sidebar.button("💱 Uppdatera FX nu (Yahoo)"):
        try:
            _load_fx_and_update_sheet()
            st.session_state["FX"] = get_fx_map()
            st.sidebar.success("Valutakurser uppdaterade.")
        except Exception as e:
            st.sidebar.error(f"FX-uppdatering misslyckades: {e}")

def _route_menu():
    menu = [
        "Analys",          # Del 4 → build_analysis_panel(...)
        "Portfölj",        # Del 5 → page_portfolio()
        "Editor",          # Del 5
        "Lägg till",       # Del 5
        "Massuppdatera",   # Del 6
        "Snapshot",        # Del 5
        "Settings",        # Del 5
    ]
    choice = st.sidebar.radio("Meny", menu, index=0)
    return choice

# ---------- Main ----------
def main():
    _bootstrap_once()
    _sidebar_header()

    choice = _route_menu()
    if choice == "Analys":
        page_analysis_panel()
    elif choice == "Portfölj":
        page_portfolio()
    elif choice == "Editor":
        page_editor()
    elif choice == "Lägg till":
        page_add_ticker()
    elif choice == "Massuppdatera":
        page_mass_update()
    elif choice == "Snapshot":
        page_snapshot()
    elif choice == "Settings":
        page_settings()
    else:
        st.error("Okänd sida.")

    st.markdown("---")
    st.caption("© Din app – bygger på din basversion. Denna koddel är Del 6/6.")

# Kör appen
main()
