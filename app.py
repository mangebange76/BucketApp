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

# CHANGED: använd APP_TITLE för konsekvent titel
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
    CHANGED: Stöd även för gemener och några vanliga alias (GOOGLE_/SHEET_).
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
            ["auto_refresh_on_start","0"],  # 0 = av, 1 = på
            # === ADDED: Bucket-tak per innehav (SEK)
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

    # --- Resultat (CHANGED: säkra existens för loggar/historik) ---
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

# ------------------------------------------------------------
# Hjälpfunktion för säker Data-inläsning från session
# ------------------------------------------------------------
def df_or_reload_from_session() -> pd.DataFrame:
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df
    return df

# ============================================================
# (Forts. i Del 2/6 — Datainsamling & beräkningshjälp via Yahoo)
# ============================================================

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Uppdateringsfunktioner (enskild & massa) som endast skriver över fält vi lyckas hämta
# ============================================================

# -------------------------
# Hjälpare för TTM & CAGR
# -------------------------
def _sum_last_n(series: pd.Series, n: int) -> Optional[float]:
    try:
        s = pd.to_numeric(series.dropna(), errors="coerce").dropna()
        if s.empty:
            return None
        return float(s.iloc[:n].sum())
    except Exception:
        return None

def _cagr(first: Optional[float], last: Optional[float], years: int) -> Optional[float]:
    try:
        if first is None or last is None:
            return None
        if years <= 0 or first <= 0:
            return None
        return (last / first) ** (1.0 / years) - 1.0
    except Exception:
        return None

def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    try:
        if a is None or b is None or b == 0:
            return None
        return float(a) / float(b)
    except Exception:
        return None

# -------------------------
# Utdelningshjälp (frekvens)
# -------------------------
def _infer_div_freq_from_series(div_ser: pd.Series) -> Optional[str]:
    """
    Försök gissa utdelningsfrekvens (M/Q/S/A) från historik i yfinance .dividends (ex-datum).
    """
    try:
        if div_ser is None or len(div_ser) < 3:
            return None
        idx = pd.to_datetime(div_ser.index)
        diffs = idx.to_series().sort_index().diff().dropna()
        if diffs.empty:
            return None
        median_days = diffs.dt.days.median()
        if median_days < 40:
            return "M"
        if median_days < 130:
            return "Q"
        if median_days < 220:
            return "S"
        return "A"
    except Exception:
        return None

# -------------------------
# Yahoo snapshot
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar ett robust "snapshot" för ett ticker via yfinance.
    Returnerar en dict med fält som mappar mot våra DATA_COLUMNS där det är rimligt.
    Vi försöker vara defensiva: None vid saknad data.
    """
    out: Dict[str, Any] = {}
    try:
        t = yf.Ticker(ticker)

        # --- Snabbinfo/pris/valuta/PE ---
        last_price = None
        currency   = None
        trailing_pe = None
        forward_pe  = None
        market_cap  = None
        shares_os   = None

        try:
            fi = t.fast_info  # kan kasta om saknas
            last_price = _f(getattr(fi, "last_price", None))
            currency   = getattr(fi, "currency", None)
            trailing_pe = _f(getattr(fi, "trailing_pe", None))
            forward_pe  = _f(getattr(fi, "forward_pe", None))
            market_cap  = _f(getattr(fi, "market_cap", None))
            shares_os   = _f(getattr(fi, "shares", None))  # kan saknas
        except Exception:
            pass

        # Fallback: pris via history
        if last_price is None:
            try:
                h = t.history(period="5d")
                if not h.empty:
                    last_price = float(h["Close"].dropna().iloc[-1])
            except Exception:
                pass

        # --- Bolagsnamn + ev. fler fält ---
        company_name = None
        try:
            info = t.get_info() if hasattr(t, "get_info") else getattr(t, "info", {})
            if info:
                company_name = info.get("longName") or info.get("shortName")
                currency = currency or info.get("currency")
                trailing_pe = trailing_pe or _f(info.get("trailingPE"))
                forward_pe  = forward_pe  or _f(info.get("forwardPE"))
                market_cap  = market_cap  or _f(info.get("marketCap"))
                shares_os   = shares_os   or _f(info.get("sharesOutstanding"))
        except Exception:
            pass

        # Fallback-beräkning av antal aktier
        if (shares_os is None) and (market_cap is not None) and (last_price is not None) and last_price != 0:
            shares_os = market_cap / last_price

        # --- Kvartalsdata (TTM för Rev/EBITDA/EPS) ---
        rev_ttm = None
        ebitda_ttm = None
        eps_ttm = None
        bvps = None
        pb_ratio = None
        ev_rev = None
        ev_ebitda = None
        net_debt = None

        try:
            q_is = t.quarterly_income_stmt or t.quarterly_financials
        except Exception:
            q_is = None

        try:
            q_bs = t.quarterly_balance_sheet
        except Exception:
            q_bs = None

        try:
            q_cf = t.quarterly_cashflow
        except Exception:
            q_cf = None

        # TTM Revenue/EBITDA/EPS
        try:
            if q_is is not None and not q_is.empty:
                # Revenue
                for key in ["Total Revenue", "TotalRevenue", "Revenue", "TotalSales"]:
                    if key in q_is.index:
                        rev_ttm = _sum_last_n(q_is.loc[key], 4)
                        break
                # EBITDA
                for key in ["EBITDA", "Ebitda"]:
                    if key in q_is.index:
                        ebitda_ttm = _sum_last_n(q_is.loc[key], 4)
                        break
                # EPS via Net Income / shares
                net_income_ttm = None
                for key in ["Net Income", "NetIncome", "Net Income Common Stockholders"]:
                    if key in q_is.index:
                        net_income_ttm = _sum_last_n(q_is.loc[key], 4)
                        break
                if net_income_ttm is not None and shares_os:
                    eps_ttm = _safe_div(net_income_ttm, shares_os)
        except Exception:
            pass

        # Fallback EPS från info
        if eps_ttm is None:
            try:
                info = t.get_info() if hasattr(t, "get_info") else getattr(t, "info", {})
                eps_ttm = _f(info.get("trailingEps"))
            except Exception:
                pass

        # Net debt & BVPS & P/B
        try:
            if q_bs is not None and not q_bs.empty:
                total_debt = None
                for key in ["Total Debt", "TotalDebt"]:
                    if key in q_bs.index:
                        total_debt = pd.to_numeric(q_bs.loc[key], errors="coerce").dropna()
                        total_debt = float(total_debt.iloc[0]) if not total_debt.empty else None
                        break
                cash = None
                for key in ["Cash And Cash Equivalents", "CashAndCashEquivalents", "Cash And Short Term Investments"]:
                    if key in q_bs.index:
                        cash = pd.to_numeric(q_bs.loc[key], errors="coerce").dropna()
                        cash = float(cash.iloc[0]) if not cash.empty else None
                        break
                if (total_debt is not None) and (cash is not None):
                    net_debt = float(total_debt - cash)

                total_equity = None
                for key in ["Total Stockholder Equity", "TotalStockholderEquity", "Stockholders Equity"]:
                    if key in q_bs.index:
                        total_equity = pd.to_numeric(q_bs.loc[key], errors="coerce").dropna()
                        total_equity = float(total_equity.iloc[0]) if not total_equity.empty else None
                        break
                if total_equity is not None and shares_os:
                    bvps = _safe_div(total_equity, shares_os)
                if bvps and last_price:
                    pb_ratio = _safe_div(last_price, bvps)
        except Exception:
            pass

        # EV multiplar
        ev = None
        if market_cap is not None and net_debt is not None:
            ev = float(market_cap + net_debt)
        elif market_cap is not None:
            ev = float(market_cap)

        if ev is not None and rev_ttm:
            ev_rev = _safe_div(ev, rev_ttm)
        if ev is not None and ebitda_ttm and ebitda_ttm > 0:
            ev_ebitda = _safe_div(ev, ebitda_ttm)

        # Utdelning (annualiserad) och frekvens (heuristik)
        annual_div = None
        div_freq = None
        try:
            divs = t.dividends
            if divs is not None and not divs.empty:
                divs_sorted = divs.sort_index(ascending=False)
                annual_div = float(divs_sorted.iloc[:4].sum())
                div_freq = _infer_div_freq_from_series(divs)
        except Exception:
            pass

        # Historisk CAGR (5 år) för Revenue & EPS
        rev_cagr_5y = None
        eps_cagr_5y = None
        try:
            annual_is = None
            try:
                annual_is = t.income_stmt or t.financials
            except Exception:
                pass

            if annual_is is not None and not annual_is.empty:
                # Revenue
                for key in ["Total Revenue", "TotalRevenue", "Revenue", "TotalSales"]:
                    if key in annual_is.index:
                        series = pd.to_numeric(annual_is.loc[key], errors="coerce").dropna()
                        if len(series) >= 2:
                            first = float(series.iloc[-1])  # äldst
                            last  = float(series.iloc[0])   # senaste
                            years = min(5, max(1, len(series)-1))
                            rev_cagr_5y = _cagr(first, last, years)
                        break

                # EPS: direkt om tillgänglig
                eps_series = None
                for key in ["Diluted EPS", "Basic EPS", "DilutedEPS", "BasicEPS"]:
                    if key in annual_is.index:
                        eps_series = pd.to_numeric(annual_is.loc[key], errors="coerce").dropna()
                        break
                if eps_series is not None and len(eps_series) >= 2:
                    first = float(eps_series.iloc[-1])
                    last  = float(eps_series.iloc[0])
                    years = min(5, max(1, len(eps_series)-1))
                    eps_cagr_5y = _cagr(first, last, years)
                else:
                    # Fallback via Net Income / shares
                    ni_key = None
                    for key in ["Net Income", "NetIncome", "Net Income Common Stockholders"]:
                        if key in annual_is.index:
                            ni_key = key
                            break
                    if ni_key and shares_os:
                        ni_series = pd.to_numeric(annual_is.loc[ni_key], errors="coerce").dropna()
                        if len(ni_series) >= 2:
                            first = float(ni_series.iloc[-1]) / shares_os
                            last  = float(ni_series.iloc[0]) / shares_os
                            years = min(5, max(1, len(ni_series)-1))
                            eps_cagr_5y = _cagr(first, last, years)
        except Exception:
            pass

        # ----------------------------
        # Output till vår Data-modell
        # ----------------------------
        out.update({
            "Bolagsnamn": company_name,
            "Aktuell kurs": last_price,
            "Valuta": currency,
            "PE TTM": trailing_pe,
            "PE FWD": forward_pe,
            "Utestående aktier": shares_os,
            "Rev TTM": rev_ttm,
            "EBITDA TTM": ebitda_ttm,
            "EPS TTM": eps_ttm,
            "EV/Revenue": ev_rev,
            "EV/EBITDA": ev_ebitda,
            "P/B": pb_ratio,
            "BVPS": bvps,
            "Årlig utdelning": annual_div,
            "Utdelningsfrekvens": div_freq,
            # CAGR-fält
            "Rev CAGR": rev_cagr_5y,
            "EPS CAGR": eps_cagr_5y,
            # Net debt
            "Net debt": net_debt,
        })
    except Exception as e:
        out["error"] = str(e)
    return out

# ------------------------------------------------
# Mappa snapshot till Data-bladets kolumner
# ------------------------------------------------
_YAHOO_TO_DATA_MAP = [
    ("Bolagsnamn", "Bolagsnamn"),
    ("Valuta", "Valuta"),
    ("Aktuell kurs", "Aktuell kurs"),
    ("Utestående aktier", "Utestående aktier"),
    ("Rev TTM", "Rev TTM"),
    ("EBITDA TTM", "EBITDA TTM"),
    ("EPS TTM", "EPS TTM"),
    ("PE TTM", "PE TTM"),
    ("PE FWD", "PE FWD"),
    ("EV/Revenue", "EV/Revenue"),
    ("EV/EBITDA", "EV/EBITDA"),
    ("P/B", "P/B"),
    ("BVPS", "BVPS"),
    ("Årlig utdelning", "Årlig utdelning"),
    ("Utdelningsfrekvens", "Utdelningsfrekvens"),
    ("Rev CAGR", "Rev CAGR"),
    ("EPS CAGR", "EPS CAGR"),
    ("Net debt", "Net debt"),
]

def _apply_snapshot_to_row(row: pd.Series, snap: Dict[str, Any]) -> pd.Series:
    """
    Applicera Yahoo-snapshot på en rad.
    Skriver endast över fält där snapshot har ett giltigt (icke-None och icke-NaN) värde.
    """
    for src, dst in _YAHOO_TO_DATA_MAP:
        val = snap.get(src, None)
        if val is None or (isinstance(val, float) and (not math.isfinite(val))):
            continue
        # numeriska kolumner ska vara float
        if dst in [
            "Aktuell kurs","Utestående aktier","Rev TTM","EBITDA TTM","EPS TTM",
            "PE TTM","PE FWD","EV/Revenue","EV/EBITDA","P/B","BVPS",
            "Årlig utdelning","Rev CAGR","EPS CAGR","Net debt"
        ]:
            row[dst] = _f(val)
        else:
            row[dst] = val
    row["Auto källa"] = "Yahoo Finance"
    row["Senast auto uppdaterad"] = now_stamp()
    return row

# ------------------------------------------------
# Publika uppdateringsfunktioner (Yahoo)
# ------------------------------------------------
def update_one_ticker_from_yahoo(df: pd.DataFrame, ticker: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Uppdatera EN ticker i Data-df från Yahoo.
    - Om raden saknas skapas en ny minimal rad med Ticker och Timestamp.
    - Endast fält som kunde hämtas skrivs över.
    Returnerar (df, summary).
    """
    if df is None or df.empty:
        df = pd.DataFrame(columns=DATA_COLUMNS)

    mask = df["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if not mask.any():
        # skapa ny rad
        base = {c: np.nan for c in DATA_COLUMNS}
        base["Timestamp"] = now_stamp()
        base["Ticker"] = ticker
        df = pd.concat([df, pd.DataFrame([base])], ignore_index=True)
        mask = df["Ticker"].astype(str).str.upper() == str(ticker).upper()

    idxs = df.index[mask].tolist()
    updated_fields = 0
    snap = yahoo_snapshot(ticker)
    for idx in idxs:
        before = df.loc[idx].copy()
        df.loc[idx] = _apply_snapshot_to_row(df.loc[idx], snap)
        changed = (before != df.loc[idx]).sum()
        updated_fields += int(changed)

    summary = {
        "ticker": ticker,
        "rows_updated": len(idxs),
        "fields_touched_est": updated_fields,
        "source": "Yahoo Finance",
        "ok": ("error" not in snap),
        "error": snap.get("error")
    }
    return df, summary

def mass_update_all_from_yahoo(df: pd.DataFrame, sleep_seconds: float = 1.0, show_progress: bool = True) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Massuppdatera alla unika tickers i df via Yahoo.
    - 1 sekunds fördröjning per bolag (default).
    - Returnerar (df, summaries).
    """
    if df is None or df.empty:
        return df, []

    tickers = (
        df["Ticker"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )
    summaries: List[Dict[str, Any]] = []
    prog = st.progress(0) if show_progress else None
    total = len(tickers)

    for i, tk in enumerate(tickers, start=1):
        try:
            df, sm = update_one_ticker_from_yahoo(df, tk)
            summaries.append(sm)
        except Exception as e:
            summaries.append({"ticker": tk, "ok": False, "error": str(e), "source": "Yahoo Finance"})
        if show_progress:
            prog.progress(min(i/total, 1.0), text=f"Uppdaterar bolag {i} av {total} – {tk}")
        time.sleep(max(0.0, float(sleep_seconds)))

    if show_progress:
        prog.empty()
    return df, summaries

# ------------------------------------------------
# Småverktyg för UI att visa resultat (Delas av senare vyer)
# ------------------------------------------------
def _summaries_to_dataframe(summaries: List[Dict[str, Any]]) -> pd.DataFrame:
    if not summaries:
        return pd.DataFrame(columns=["ticker","ok","fields_touched_est","source","error"])
    cols = sorted({k for sm in summaries for k in sm.keys()})
    out = pd.DataFrame(summaries)[cols]
    return out

# ============================================================
# Del 3/6 — Beräkningsmotor
#  • Kompatibel wrapper: fetch_from_yahoo() (bygger på yahoo_snapshot)
#  • EPS-estimat från Yahoo (earnings_trend)
#  • Metodpriser: PE, EV/S, EV/EBITDA, DACF, P/B (+ placeholders)
#  • Multipel-decay & PE-ankare
#  • ✅ Fair Value (familjemedian + kurs-kopiafilter)
#  • compute_methods_for_row() → används av Analys/Ranking
#  • compute_fair_values_for_row() → smidig extraktor till Ranking/Sheets
# ============================================================

# -------------------------
# Kompatibel wrapper (Del 2 → Del 3)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Kompatibilitetslager som mappar Del 2/6:s yahoo_snapshot() till
    samma nycklar som resten av appen förväntar sig.
    """
    snap = yahoo_snapshot(ticker)  # från Del 2/6
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
        "rev_cagr_hist":    _f(snap.get("Rev CAGR")),
        "eps_cagr_hist":    _f(snap.get("EPS CAGR")),
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
    # tillåt inte <=0 EBITDA i EV/EBITDA
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
      • 'Idag': filtrera bort värden som ≈ aktuell kurs (±0.5%) för att
        undvika tautologier; fall-back till 'pe_hist_vs_eps' om allt försvinner.
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
        "2 år": _equity_price_from_ev(_equity_price_from_ev(_ev_from_ebitda(b2, eve2), net_debt, shares), None, None),  # skydd om b2/eve2 saknas
        "3 år": _equity_price_from_ev(_equity_price_from_ev(_ev_from_ebitda(b3, eve3), net_debt, shares), None, None),
    })
    # Rätta EV/EBITDA för 2/3 år (utan extra _equity_price_from_ev)
    methods[-2]["2 år"] = _equity_price_from_ev(_ev_from_ebitda(b2, eve2), net_debt, shares)
    methods[-2]["3 år"] = _equity_price_from_ev(_ev_from_ebitda(b3, eve3), net_debt, shares)

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

# ------------------------------------------------------------
# Smidig extraktor för Ranking/Sheets
# ------------------------------------------------------------
def compute_fair_values_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Dict[str, Any]:
    """
    Beräknar metoder för en rad och returnerar en kompakt dict:
      {
        'ticker': 'AAPL',
        'price':  195.12,
        'currency': 'USD',
        'fv_today': 210.34,
        'fv_1y': 222.11,
        'fv_2y': 235.45,
        'fv_3y': 248.76,
        'sanity': '...'
      }
    """
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    fv = meta.get("fair_value") or {}
    return {
        "ticker": str(row.get("Ticker") or "").upper(),
        "price": _f(meta.get("price")),
        "currency": (meta.get("currency") or "USD"),
        "fv_today": _f(fv.get("today")),
        "fv_1y": _f(fv.get("y1")),
        "fv_2y": _f(fv.get("y2")),
        "fv_3y": _f(fv.get("y3")),
        "sanity": sanity,
        "methods_df": methods_df,  # kan användas för debug/UI
    }

# ============================================================
# Del 4/6 — Vyer: Analys & Ranking (fair value)
#  • Bygger fair value-tabell per bolag (today, 1y, 2y, 3y)
#  • Uppsida (%) vs aktuell kurs
#  • Enkel bläddringsvy 1/X för valt bolag
#  • Metoddjup för valt bolag (methods_df)
#  • Hela databasen visas längst ner
# ============================================================

# -------------------------
# Hjälpformattering
# -------------------------
def _fmt2(x: Optional[float]) -> str:
    v = _f(x)
    if v is None or (isinstance(v, float) and (math.isnan(v) or not math.isfinite(v))):
        return "—"
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "—"

def _fmtpct(x: Optional[float]) -> str:
    v = _f(x)
    if v is None or (isinstance(v, float) and (math.isnan(v) or not math.isfinite(v))):
        return "—"
    try:
        return f"{float(v):+.1f}%"
    except Exception:
        return "—"

# -------------------------
# Fair value-bygge för hela tabellen
# -------------------------
@st.cache_data(ttl=600, show_spinner=True)
def build_fair_value_table(df_input: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> pd.DataFrame:
    rows = []
    for _, r in df_input.iterrows():
        try:
            out = compute_fair_values_for_row(r, settings, fx_map)
        except Exception as e:
            out = {
                "ticker": str(r.get("Ticker") or "").upper(),
                "price": None, "currency": r.get("Valuta") or "USD",
                "fv_today": None, "fv_1y": None, "fv_2y": None, "fv_3y": None,
                "sanity": f"error: {e}",
            }
        rows.append(out)

    res = pd.DataFrame(rows)
    if res.empty:
        return res

    # Uppsida i %
    for col_src, col_out in [
        ("fv_today", "Uppsida idag %"),
        ("fv_1y",    "Uppsida 1 år %"),
        ("fv_2y",    "Uppsida 2 år %"),
        ("fv_3y",    "Uppsida 3 år %"),
    ]:
        try:
            res[col_out] = ((res[col_src] / res["price"]) - 1.0) * 100.0
        except Exception:
            res[col_out] = np.nan

    # Visningskolumner
    res = res.rename(columns={
        "ticker": "Ticker",
        "currency": "Valuta",
        "price": "Aktuell kurs (0)",
        "fv_today": "Fair value idag",
        "fv_1y": "Fair value 1 år",
        "fv_2y": "Fair value 2 år",
        "fv_3y": "Fair value 3 år",
    })
    # Ordning
    cols = [
        "Ticker", "Valuta", "Aktuell kurs (0)",
        "Fair value idag", "Fair value 1 år", "Fair value 2 år", "Fair value 3 år",
        "Uppsida idag %", "Uppsida 1 år %", "Uppsida 2 år %", "Uppsida 3 år %",
        "sanity",
    ]
    for c in cols:
        if c not in res.columns:
            res[c] = np.nan
    res = res[cols]
    return res

# -------------------------
# Render: Analys & Ranking
# -------------------------
def render_view_analys_ranking(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.header("Analys & fair value")

    if df_data is None or df_data.empty:
        st.info("Ingen data i 'Data'-bladet ännu.")
        return

    # Bygg fair value-tabell
    with st.spinner("Beräknar fair value för alla tickers..."):
        fv_df = build_fair_value_table(df_data, settings, fx_map)

    if fv_df.empty:
        st.warning("Kunde inte beräkna fair value (tomt resultat).")
        return

    # ------- Ranking (sortera på uppsida idag %) -------
    sort_col = st.selectbox("Sortera efter", ["Uppsida idag %", "Uppsida 1 år %", "Uppsida 2 år %", "Uppsida 3 år %"], index=0)
    asc = st.toggle("Stigande (default: fallande)", value=False)
    fv_rank = fv_df.sort_values(by=sort_col, ascending=asc).reset_index(drop=True)

    # Visning med två decimaler
    show_df = fv_rank.copy()
    for c in ["Aktuell kurs (0)", "Fair value idag", "Fair value 1 år", "Fair value 2 år", "Fair value 3 år"]:
        show_df[c] = show_df[c].map(_fmt2)
    for c in ["Uppsida idag %", "Uppsida 1 år %", "Uppsida 2 år %", "Uppsida 3 år %"]:
        show_df[c] = show_df[c].map(_fmtpct)

    st.subheader("Ranking (fair value & uppsida)")
    st.dataframe(show_df.drop(columns=["sanity"]), use_container_width=True, height=min(600, 60 + 28 * len(show_df)))

    # ------- Bläddringsvy 1/X -------
    st.subheader("Detaljvy (bläddra)")
    tickers = fv_rank["Ticker"].tolist()
    if "rank_idx" not in st.session_state:
        st.session_state["rank_idx"] = 0

    cols_nav = st.columns(3)
    with cols_nav[0]:
        if st.button("◀️ Föregående", use_container_width=True):
            st.session_state["rank_idx"] = (st.session_state["rank_idx"] - 1) % len(tickers)
    with cols_nav[1]:
        st.markdown(
            f"<div style='text-align:center;padding-top:6px;'>"
            f"{st.session_state['rank_idx']+1} / {len(tickers)}</div>",
            unsafe_allow_html=True
        )
    with cols_nav[2]:
        if st.button("Nästa ▶️", use_container_width=True):
            st.session_state["rank_idx"] = (st.session_state["rank_idx"] + 1) % len(tickers)

    idx = st.session_state["rank_idx"]
    cur_ticker = tickers[idx]
    st.markdown(f"**Valt bolag:** {cur_ticker}")

    # Hämta rad från ursprungsdata (för compute_methods_for_row)
    try:
        base_row = df_data[df_data["Ticker"].astype(str).str.upper() == cur_ticker].iloc[0]
    except Exception:
        # fallback: skapa tom rad med ticker
        base_row = pd.Series({"Ticker": cur_ticker})

    # Metoder & fair value för valt bolag
    with st.spinner(f"Beräknar metoder för {cur_ticker}…"):
        methods_df, sanity, meta = compute_methods_for_row(base_row, settings, fx_map)

    # Kort summering
    colA, colB, colC, colD, colE = st.columns(5)
    with colA:
        st.metric("Valuta", meta.get("currency", "—"))
    with colB:
        st.metric("Aktuell kurs", _fmt2(meta.get("price")))
    with colC:
        st.metric("Fair value idag", _fmt2(meta.get("fair_value", {}).get("today")))
    with colD:
        st.metric("Fair value 1 år", _fmt2(meta.get("fair_value", {}).get("y1")))
    with colE:
        st.metric("PE-ankare", _fmt2(meta.get("pe_anchor")))

    # Uppsidor
    p0 = _f(meta.get("price"))
    fv_today = _f(meta.get("fair_value", {}).get("today"))
    fv_1y = _f(meta.get("fair_value", {}).get("y1"))
    fv_2y = _f(meta.get("fair_value", {}).get("y2"))
    fv_3y = _f(meta.get("fair_value", {}).get("y3"))
    up_today = ((fv_today / p0) - 1) * 100 if (p0 and fv_today) else None
    up_1y    = ((fv_1y    / p0) - 1) * 100 if (p0 and fv_1y)    else None
    up_2y    = ((fv_2y    / p0) - 1) * 100 if (p0 and fv_2y)    else None
    up_3y    = ((fv_3y    / p0) - 1) * 100 if (p0 and fv_3y)    else None

    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("Uppsida idag", _fmtpct(up_today))
    with col2: st.metric("Uppsida 1 år", _fmtpct(up_1y))
    with col3: st.metric("Uppsida 2 år", _fmtpct(up_2y))
    with col4: st.metric("Uppsida 3 år", _fmtpct(up_3y))

    # Metodtabell
    st.markdown("**Metodpriser (i bolagets valuta)**")
    mt = methods_df.copy()
    for c in ["Idag", "1 år", "2 år", "3 år"]:
        mt[c] = mt[c].map(_fmt2)
    st.dataframe(mt, use_container_width=True)

    # Sanity-detaljer
    with st.expander("Tekniska detaljer (sanity)"):
        st.code(sanity)

    # ------- Hela databasen längst ner -------
    st.markdown("---")
    st.subheader("Hela databasen (ofiltrerad, enkel tabell)")
    try:
        st.dataframe(df_data, use_container_width=True, height=400)
    except Exception:
        st.write("Kunde inte visa hela databasen (kontrollera Data-bladet).")

# ============================================================
# Del 5/6 — Vyer
#  • ⚙️ Settings (redigerbar)
#  • 🕒 Snapshot (read-only)
#  • ✏️ Editor (manuellt + Yahoo)
#  • ➕ Lägg till ticker (med valfri Yahoo-prefill)
#  • 📦 Portfölj (värden + hinkar + kommande utdelningar)
#  • 🛒 Köpförslag (läser riktkurser ur Data-bladet)
#  • 🧩 Massuppdatering från Yahoo (1s/b bolag)
# ============================================================

# -------------------------
# Settings
# -------------------------
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

# -------------------------
# Snapshot
# -------------------------
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True)

# -------------------------
# Editor (manuellt + Yahoo)
# -------------------------
def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["EPS 1Y uppdaterad","EPS 2Y uppdaterad","Rev 1Y uppdaterad",
            "Rev 2Y uppdaterad","Senast manuellt uppdaterad"]
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *cols])
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    y   = fetch_from_yahoo(ticker)
    try:
        est = _fetch_eps_estimates_yahoo(ticker)
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
        "Auto källa": "Yahoo Finance",
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
    tickers = sorted([t.upper() for t in tickers])

    if "editor_idx" not in st.session_state:
        st.session_state["editor_idx"] = 0
    if tickers:
        st.session_state["editor_idx"] = min(st.session_state["editor_idx"], len(tickers)-1)

    col_sel, col_nav = st.columns([3, 2])
    with col_sel:
        current = tickers[st.session_state["editor_idx"]] if tickers else ""
        picked = st.selectbox("Välj rad", options=tickers, index=st.session_state["editor_idx"] if tickers else 0)
        if picked != current:
            st.session_state["editor_idx"] = tickers.index(picked)
    with col_nav:
        st.write("")
        st.write(f"Post {st.session_state['editor_idx']+1}/{len(tickers)}")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("◀︎ Föregående", use_container_width=True) and tickers:
                st.session_state["editor_idx"] = (st.session_state["editor_idx"] - 1) % len(tickers)
        with c2:
            if st.button("Nästa ▶︎", use_container_width=True) and tickers:
                st.session_state["editor_idx"] = (st.session_state["editor_idx"] + 1) % len(tickers)

    if not tickers:
        st.info("Lägg till en ticker först.")
        return

    sel = tickers[st.session_state["editor_idx"]]
    ridx = df.index[df["Ticker"].astype(str).str.upper() == sel]
    if len(ridx) == 0:
        st.error("Kunde inte hitta vald rad.")
        return
    idx = ridx[0]
    row = df.loc[idx].copy()

    # --- UI ---
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
    st.dataframe(df.loc[[idx]], use_container_width=True)

# -------------------------
# Lägg till ticker
# -------------------------
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
                        "Auto källa": "Yahoo Finance",
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

# -------------------------
# Portfölj + kommande utdelningar
# -------------------------
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
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    with st.expander("Visa summering per månad (SEK, netto)"):
        try:
            g = nxt.copy()
            g["YYYY-MM"] = g["Datum"].astype(str).str.slice(0, 7)
            agg = g.groupby("YYYY-MM", as_index=False)["Netto SEK"].sum().sort_values("YYYY-MM")
            agg["Netto SEK"] = agg["Netto SEK"].map(lambda x: f"{x:,.2f}".replace(",", " ").replace(".", ","))
            st.dataframe(agg, use_container_width=True, hide_index=True)
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
            st.dataframe(show, use_container_width=True, hide_index=True)

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
        st.dataframe(pos.sort_values(["Bucket","Värde (SEK)"]), use_container_width=True, hide_index=True)
        st.markdown("#### Hinkar (Bucket) – innehåll")
        render_bucket_expandables(pos, settings)

    st.markdown("---")
    render_portfolio_dividends_section(df, fx_map, settings)

# -------------------------
# Köpförslag
# -------------------------
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
            ccy      = str(_nz(r.get("Valuta"), "SEK")).upper()
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
    st.header("🛒 Köpförslag (läser från Data-bladet)")
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
        st.caption("Tips: Kör **Analys & Ranking** först för att uppdatera riktkurserna i Data-bladet.")
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

    st.dataframe(show, use_container_width=True, hide_index=True)

    with st.expander("Summering per Bucket (antal förslag)"):
        agg = sug.groupby("Bucket", as_index=False).size().rename(columns={"size":"Antal förslag"})
        st.dataframe(agg, use_container_width=True, hide_index=True)

# -------------------------
# Massuppdatering
# -------------------------
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
    progress.empty()
    status.empty()
    st.success(f"Klar. {len(target)} bolag uppdaterade. {changed_total} fält ändrades.")

# ============================================================
# Del 6/6 — Analys & navigation (main)
#  • Strikt fair value-beräkning (EV/S eller P/E beroende på data)
#  • Analys & Ranking-vy som skriver riktkurser till Data-bladet
#  • Sidomeny & main() som kopplar ihop alla vyer
# ============================================================

# -------------------------
# Fair value – hjälpare
# -------------------------
def _strict_targets_from_settings(settings: Dict[str, Any]) -> Dict[str, float]:
    """
    Plockar konservativa multiplar från Settings om de finns,
    annars används vettiga standarder (strikt läge).
    """
    def _pick(k: str, default: float) -> float:
        v = _f(settings.get(k))
        try:
            return float(v) if v is not None and float(v) > 0 else float(default)
        except Exception:
            return float(default)

    return {
        # EV/S – growth vs mature
        "evs_growth": _pick("evs_growth_target", 3.0),
        "evs_mature": _pick("evs_mature_target", 1.5),

        # P/E – growth vs mature (framåtblickande, strikt)
        "pe_growth": _pick("pe_growth_target", 22.0),
        "pe_mature": _pick("pe_mature_target", 15.0),

        # Default antagen tillväxt om prognoser saknas
        "rev_g_default": _pick("rev_growth_default", 0.08),   # 8%
        "eps_g_default": _pick("eps_growth_default", 0.08),   # 8%
    }

def _is_growth(row: pd.Series) -> bool:
    """Heuristik för att avgöra om caset är 'growth'."""
    for k in ("Rev CAGR","EPS CAGR"):
        g = _f(row.get(k))
        if g is not None and g >= 0.15:  # >=15% anses growth
            return True
    # fallback: om PE FWD väldigt högt kan det vara growth
    pef = _f(row.get("PE FWD"))
    return bool(pef is not None and pef >= 25)

def _fv_from_evs(row: pd.Series, tgt_evs: float, targets: Dict[str, float]) -> Dict[str, Optional[float]]:
    """Riktkurs via EV/S (Equity = EV - NetDebt; /\ aktier)."""
    rev_ttm = _f(row.get("Rev TTM"))
    rev1    = _f(row.get("Rev 1Y"))
    rev2    = _f(row.get("Rev 2Y"))
    shares  = _pos(row.get("Utestående aktier"))
    net_debt = _f(row.get("Net debt")) or 0.0
    if not (_pos(rev_ttm) and _pos(shares)):
        return {"Riktkurs idag": None, "Riktkurs 1 år": None, "Riktkurs 2 år": None, "Riktkurs 3 år": None}

    # Estimera framtida omsättning om saknas
    if not _pos(rev1):
        g = targets["rev_g_default"]
        rev1 = rev_ttm * (1.0 + g)
    if not _pos(rev2):
        g = targets["rev_g_default"]
        rev2 = (rev1 if _pos(rev1) else rev_ttm) * (1.0 + g)

    # 3Y extrapolation
    try:
        g12 = (rev2/rev1 - 1.0) if (_pos(rev2) and _pos(rev1)) else targets["rev_g_default"]
    except Exception:
        g12 = targets["rev_g_default"]
    rev3 = (rev2 if _pos(rev2) else rev1) * (1.0 + g12)

    def per_share(rev):
        EV = tgt_evs * (rev or 0.0)
        eq = EV - (net_debt or 0.0)
        if not _pos(shares):
            return None
        return (eq / shares) if math.isfinite(eq) else None

    return {
        "Riktkurs idag": _pos(rev_ttm) and per_share(rev_ttm) or None,
        "Riktkurs 1 år": _pos(rev1)    and per_share(rev1)    or None,
        "Riktkurs 2 år": _pos(rev2)    and per_share(rev2)    or None,
        "Riktkurs 3 år": _pos(rev3)    and per_share(rev3)    or None,
    }

def _fv_from_pe(row: pd.Series, tgt_pe: float, targets: Dict[str, float]) -> Dict[str, Optional[float]]:
    """Riktkurs via P/E (pris = EPS * P/E)."""
    eps_ttm = _f(row.get("EPS TTM"))
    eps1    = _f(row.get("EPS 1Y"))
    eps2    = _f(row.get("EPS 2Y"))

    if not _pos(eps_ttm) and not _pos(eps1) and not _pos(eps2):
        return {"Riktkurs idag": None, "Riktkurs 1 år": None, "Riktkurs 2 år": None, "Riktkurs 3 år": None}

    if not _pos(eps1):
        g = targets["eps_g_default"]
        eps1 = (eps_ttm or 0.0) * (1.0 + g)
    if not _pos(eps2):
        g = targets["eps_g_default"]
        eps2 = (eps1 or eps_ttm or 0.0) * (1.0 + g)

    # 3Y extrapolation
    try:
        g12 = (eps2/eps1 - 1.0) if (_pos(eps2) and _pos(eps1)) else targets["eps_g_default"]
    except Exception:
        g12 = targets["eps_g_default"]
    eps3 = (eps2 if _pos(eps2) else (eps1 or eps_ttm or 0.0)) * (1.0 + g12)

    def px(eps):
        return (eps or 0.0) * (tgt_pe or 0.0)

    return {
        "Riktkurs idag": _pos(eps_ttm) and px(eps_ttm) or None,
        "Riktkurs 1 år": _pos(eps1)    and px(eps1)    or None,
        "Riktkurs 2 år": _pos(eps2)    and px(eps2)    or None,
        "Riktkurs 3 år": _pos(eps3)    and px(eps3)    or None,
    }

def compute_methods_for_row(row: pd.Series, settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Väljer metod per rad (EV/S om vi har oms & balans, annars P/E om vi har EPS).
    Återger 'Metod' + riktkurser (Idag/1/2/3 år).
    Strikt, ej pris-ankrat. Allt i aktiens handelsvaluta.
    """
    targets = _strict_targets_from_settings(settings)
    growth  = _is_growth(row)

    # Försök EV/S först om datan räcker (omsättning + aktier)
    rev_ttm  = _f(row.get("Rev TTM"))
    shares   = _pos(row.get("Utestående aktier"))
    use_evs  = _pos(rev_ttm) and _pos(shares)

    if use_evs:
        tgt_evs = targets["evs_growth"] if growth else targets["evs_mature"]
        out = _fv_from_evs(row, tgt_evs, targets)
        out["Metod"] = f"EV/S strict ({'growth' if growth else 'mature'})"
        return out

    # Annars P/E om vi har EPS
    eps_any = any(_pos(_f(row.get(k))) for k in ("EPS TTM","EPS 1Y","EPS 2Y"))
    if eps_any:
        tgt_pe = targets["pe_growth"] if growth else targets["pe_mature"]
        out = _fv_from_pe(row, tgt_pe, targets)
        out["Metod"] = f"P/E strict ({'growth' if growth else 'mature'})"
        return out

    # Knapphändig data → inga riktkurser
    return {
        "Metod": "—",
        "Riktkurs idag": None,
        "Riktkurs 1 år": None,
        "Riktkurs 2 år": None,
        "Riktkurs 3 år": None,
    }

def compute_fair_values_df(df_in: pd.DataFrame, settings: Dict[str, Any]) -> pd.DataFrame:
    """Kör compute_methods_for_row radvis och fyller riktkurs-kolumnerna."""
    if df_in is None or df_in.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)

    df = df_in.copy()
    for col in ("Metod","Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"):
        if col not in df.columns:
            df[col] = np.nan

    rows_out = []
    for _, r in df.iterrows():
        res = compute_methods_for_row(r, settings)
        for k, v in res.items():
            r[k] = None if (isinstance(v, float) and not math.isfinite(v)) else v
        rows_out.append(r)
    out = pd.DataFrame(rows_out)
    # avrunda till 2 decimaler
    for c in ("Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").map(lambda x: None if pd.isna(x) else float(f"{x:.2f}"))
    return out

# -------------------------
# Analys & Ranking-vy
# -------------------------
def page_analysis():
    st.header("📊 Analys & Ranking (strikt fair value)")
    base = st.session_state.get("DATA")
    if base is None or (isinstance(base, pd.DataFrame) and base.empty):
        base = read_data_df()

    if base is None or base.empty:
        st.info("Data-bladet är tomt.")
        return

    st.caption("Metod väljs radvis: **EV/S** om omsättning + aktier finns, annars **P/E** om EPS finns. "
               "Tillväxt/mognad avgör multipel (strikt). Inga prisankare används.")
    settings = get_settings_map()
    targets  = _strict_targets_from_settings(settings)
    with st.expander("Visa använda mål (targets)"):
        tdf = pd.DataFrame([targets])
        st.dataframe(tdf, use_container_width=True, hide_index=True)

    if st.button("🔎 Kör analys nu"):
        with st.spinner("Beräknar riktkurser…"):
            analyzed = compute_fair_values_df(base, settings)
            st.session_state["ANALYZED"] = analyzed
            st.success("Klar.")

    analyzed = st.session_state.get("ANALYZED")
    if isinstance(analyzed, pd.DataFrame) and not analyzed.empty:
        show = analyzed.copy()
        # Beräkna uppsida (%) mot kurs, om kurs finns
        if "Aktuell kurs" in show.columns:
            show["Uppsida idag (%)"] = np.where(
                pd.to_numeric(show["Aktuell kurs"], errors="coerce") > 0,
                (pd.to_numeric(show["Riktkurs idag"], errors="coerce") - pd.to_numeric(show["Aktuell kurs"], errors="coerce"))
                / pd.to_numeric(show["Aktuell kurs"], errors="coerce") * 100.0,
                np.nan
            )
        order_cols = [c for c in [
            "Ticker","Bolagsnamn","Valuta","Metod","Aktuell kurs",
            "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år","Uppsida idag (%)",
            "Rev TTM","EPS TTM","Utestående aktier","Net debt"
        ] if c in show.columns]
        st.dataframe(show[order_cols], use_container_width=True, hide_index=True)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Spara riktkurser till Data"):
                try:
                    # Skriv tillbaka endast relevanta kolumner
                    base2 = base.copy()
                    for c in ("Metod","Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"):
                        if c not in base2.columns:
                            base2[c] = np.nan
                    # align på Ticker
                    join_cols = ["Ticker"]
                    merged = base2.merge(
                        analyzed[["Ticker","Metod","Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"]],
                        on="Ticker", how="left", suffixes=("","")
                    )
                    for c in ("Metod","Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"):
                        if c in merged.columns:
                            base2[c] = merged[c]
                    write_data_df(base2)
                    st.session_state["DATA"] = base2
                    st.success("Riktkurser sparade till Data-bladet.")
                except Exception as e:
                    st.error(f"Kunde inte spara: {e}")
        with col2:
            if st.button("🔄 Läs om Data"):
                st.session_state["DATA"] = read_data_df()
                st.experimental_rerun()

# -------------------------
# Navigation & main()
# -------------------------
def _load_bootstrap_into_session():
    """Initial laddning av Data/FX/Settings till session."""
    if "DATA" not in st.session_state or st.session_state.get("DATA") is None:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception:
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
    if "FX" not in st.session_state or not st.session_state.get("FX"):
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception:
            st.session_state["FX"] = {}
    if "SETTINGS_MAP" not in st.session_state or not st.session_state.get("SETTINGS_MAP"):
        try:
            st.session_state["SETTINGS_MAP"] = get_settings_map()
        except Exception:
            st.session_state["SETTINGS_MAP"] = {}

def main():
    _load_bootstrap_into_session()

    st.sidebar.title("📚 Navigering")
    menu = st.sidebar.radio(
        "Välj vy:",
        [
            "📊 Analys & Ranking",
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

    if menu == "📊 Analys & Ranking":
        page_analysis()
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
        page_analysis()

# Streamlit-körning
if __name__ == "__main__":
    main()
