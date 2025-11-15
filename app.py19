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
#  • CHANGED: compute_fair_values_for_row() → smidig extraktor till Ranking/Sheets
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

# ------------------------------------------------------------
# CHANGED: Smidig extraktor för Ranking/Sheets
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
# Del 4/6 — Analys & Ranking (UI-helpers som anropas från main)
#  • render_analysvy(df, settings, fx_map)
#  • render_rankingpanel(df, settings, fx_map)  ← progressbar + spar till Sheets
#  • Hjälpmetoder för uppsida, tabellformatering och bläddring
#  • Allt bygger på compute_methods_for_row() / compute_fair_values_for_row()
# ============================================================

# Fallback-formatters om de inte redan finns från Del 1/6
try:
    _fmt2  # type: ignore
except NameError:
    def _fmt2(x):
        try:
            if x is None or (isinstance(x, float) and (not math.isfinite(x))):
                return "—"
            return f"{float(x):.2f}"
        except Exception:
            return "—"

def _fmt_pct(p):
    try:
        if p is None or (isinstance(p, float) and (not math.isfinite(p))):
            return "—"
        return f"{float(p)*100:.1f}%"
    except Exception:
        return "—"

def _safe_currency(c):
    return (str(c) if c else "USD").upper()

def _calc_upside(target: Optional[float], price: Optional[float]) -> Optional[float]:
    t = _f(target)
    p = _f(price)
    if t is None or p is None or p <= 0:
        return None
    return (t / p) - 1.0

def _build_methods_display(methods_df: pd.DataFrame) -> pd.DataFrame:
    df = methods_df.copy()
    for col in ["Idag", "1 år", "2 år", "3 år"]:
        if col in df.columns:
            df[col] = df[col].map(_fmt2)
    return df.set_index("Metod")

def _render_fairvalue_summary(meta: Dict[str, Any]) -> None:
    currency = _safe_currency(meta.get("currency"))
    price    = meta.get("price")
    fv       = meta.get("fair_value", {}) or {}
    fv0, fv1, fv2, fv3 = fv.get("today"), fv.get("y1"), fv.get("y2"), fv.get("y3")

    up0 = _calc_upside(fv0, price)
    up1 = _calc_upside(fv1, price)
    up2 = _calc_upside(fv2, price)
    up3 = _calc_upside(fv3, price)

    c1, c2, c3, c4, c5 = st.columns([1.2,1.2,1.2,1.2,1.2])
    with c1:
        st.metric("Kurs (nu)", f"{_fmt2(price)} {currency}")
    with c2:
        st.metric("Fair value — Idag", f"{_fmt2(fv0)} {currency}", _fmt_pct(up0))
    with c3:
        st.metric("Fair value — 1 år", f"{_fmt2(fv1)} {currency}", _fmt_pct(up1))
    with c4:
        st.metric("Fair value — 2 år", f"{_fmt2(fv2)} {currency}", _fmt_pct(up2))
    with c5:
        st.metric("Fair value — 3 år", f"{_fmt2(fv3)} {currency}", _fmt_pct(up3))

def _compute_one(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    return methods_df, sanity, meta

def _pick_row_by_ticker(df: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    m = df["Ticker"].astype(str).str.upper() == str(ticker).upper()
    sub = df[m]
    if sub.empty:
        return None
    return sub.iloc[0]

# -----------------------------
# Publik Analys-vy (enskilt bolag)
# -----------------------------
def render_analysvy(df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    """
    Enkel analysvy för ett enskilt bolag:
      • Välj ticker
      • Visar Fair Value (Idag/1/2/3 år) och uppsida
      • Visar metodrader (PE, EV/S, EV/EBITDA, P/B, m.fl.)
    """
    if df is None or df.empty:
        st.info("Ingen data att visa ännu.")
        return

    tickers = sorted(df["Ticker"].dropna().astype(str).unique().tolist())
    col_a, col_b = st.columns([2, 1])
    with col_a:
        picked = st.selectbox("Välj bolag (Ticker)", tickers, index=0 if tickers else None)
    with col_b:
        st.caption("Tips: skriv för att filtrera listan.")

    row = _pick_row_by_ticker(df, picked)
    if row is None:
        st.warning("Kunde inte hitta valt bolag i datan.")
        return

    with st.spinner(f"Beräknar {picked} …"):
        methods_df, sanity, meta = _compute_one(row, settings, fx_map)

    _render_fairvalue_summary(meta)
    st.caption(f"Sanity: {sanity}")

    st.subheader("Metoder & riktkurser")
    st.dataframe(_build_methods_display(methods_df), use_container_width=True)

# -----------------------------
# Hjälp: Spara FV + kurs till Data-DF
# -----------------------------
def _persist_fair_values_to_df(df: pd.DataFrame, fv_rec: Dict[str, Any]) -> None:
    """
    Uppdaterar (in-place) Data-df för givna 'fv_rec' från compute_fair_values_for_row():
      • 'Riktkurs idag', 'Riktkurs 1 år', 'Riktkurs 2 år', 'Riktkurs 3 år'
      • 'Aktuell kurs'
    Skriver EJ till Sheets här (görs av anropare efter batch).
    """
    tkr = str(fv_rec.get("ticker") or "").upper()
    if not tkr or df is None or df.empty:
        return
    mask = df["Ticker"].astype(str).str.upper() == tkr
    if not mask.any():
        return
    idx = df.index[mask][0]
    # Säkerställ kolumner finns
    for c in ["Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år","Aktuell kurs"]:
        if c not in df.columns:
            df[c] = np.nan
    df.at[idx, "Riktkurs idag"] = _f(fv_rec.get("fv_today"))
    df.at[idx, "Riktkurs 1 år"]  = _f(fv_rec.get("fv_1y"))
    df.at[idx, "Riktkurs 2 år"]  = _f(fv_rec.get("fv_2y"))
    df.at[idx, "Riktkurs 3 år"]  = _f(fv_rec.get("fv_3y"))
    # Uppdatera kurs om vi har ett färskt pris från beräkningen
    if _f(fv_rec.get("price")) is not None:
        df.at[idx, "Aktuell kurs"] = _f(fv_rec.get("price"))

# -----------------------------
# Publik Ranking-panel
# -----------------------------
def _init_pager(key: str, n: int) -> None:
    if key not in st.session_state:
        st.session_state[key] = 0
    st.session_state[key] = max(0, min(st.session_state[key], max(0, n-1)))

def render_rankingpanel(df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    """
    Rankingpanel:
      • Välj horisont (Idag/1/2/3 år)
      • Klicka 'Beräkna ranking' för att räkna
      • Visar progressbar 'i/total – TICKER'
      • Sparar FV (Idag/1/2/3) + Kurs tillbaka till Data-bladet
      • Lista med uppsida i %, sorterad fallande
      • Bläddringsfunktion (1/N) för detaljvy
    """
    st.subheader("Ranking efter uppsida")
    left, right = st.columns([1.2, 1])
    with left:
        horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=0)
    with right:
        st.caption("Körs vid knapptryck och sparar FV till Google Sheets.")

    go = st.button("🔁 Beräkna ranking", type="primary", use_container_width=False)
    if not go:
        st.info("Klicka **Beräkna ranking** för att räkna ut listan.")
        return

    # Körning
    rows_out = []
    df_cur = df.copy()
    tickers = df_cur["Ticker"].dropna().astype(str).str.upper().tolist()
    total = len(tickers)
    prog = st.progress(0.0)
    status = st.empty()

    for i, tkr in enumerate(tickers, start=1):
        status.write(f"Beräknar {i}/{total} – {tkr}")
        try:
            base_row = _pick_row_by_ticker(df_cur, tkr)
            if base_row is None:
                continue
            # Kompakt beräkning
            rec = compute_fair_values_for_row(base_row, settings, fx_map)
            # Persist till vår lokala df (sparas till Sheets efter loopen)
            _persist_fair_values_to_df(df_cur, rec)

            # Välj FV-fält beroende på horisont
            if horizon == "Idag":   tgt = rec.get("fv_today")
            elif horizon == "1 år": tgt = rec.get("fv_1y")
            elif horizon == "2 år": tgt = rec.get("fv_2y")
            else:                   tgt = rec.get("fv_3y")

            up = _calc_upside(tgt, rec.get("price"))
            rows_out.append({
                "Ticker": tkr,
                "Valuta": _safe_currency(rec.get("currency")),
                "Kurs": _f(rec.get("price")),
                "Fair value": _f(tgt),
                "Uppsida %": up*100 if up is not None else None
            })
        except Exception:
            # Fortsätt även om en rad failar
            pass
        prog.progress(i/total if total else 1.0)

    prog.empty()
    status.empty()

    # Skriv uppdaterad Data till Sheets + session
    try:
        write_data_df(df_cur)
        st.session_state["DATA"] = df_cur
        st.success("Fair value & kurs sparat till Google Sheets.")
    except Exception as e:
        st.error(f"Kunde inte spara till Google Sheets: {e}")

    # Visa ranking
    rank_df = pd.DataFrame(rows_out)
    if rank_df.empty:
        st.info("Inga rankingdata att visa.")
        return

    rank_df = rank_df.sort_values(by="Uppsida %", ascending=False, na_position="last").reset_index(drop=True)

    # Visningstabell
    disp = rank_df.copy()
    disp["Kurs"] = disp["Kurs"].map(_fmt2)
    disp["Fair value"] = disp["Fair value"].map(_fmt2)
    disp["Uppsida %"] = disp["Uppsida %"].map(lambda v: f"{v:.1f}%" if v is not None else "—")
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.caption(f"{len(rank_df)} bolag")

    # Bläddring
    n = len(rank_df)
    _init_pager("rank_idx", n)

    c1, c2, c3, _ = st.columns([0.8,0.8,1.2,6])
    with c1:
        if st.button("◀︎ Föregående", use_container_width=True, disabled=(st.session_state["rank_idx"]<=0)):
            st.session_state["rank_idx"] = max(0, st.session_state["rank_idx"]-1)
    with c2:
        if st.button("Nästa ▶︎", use_container_width=True, disabled=(st.session_state["rank_idx"]>=n-1)):
            st.session_state["rank_idx"] = min(n-1, st.session_state["rank_idx"]+1)
    with c3:
        st.write(f"{st.session_state['rank_idx']+1} / {n}")

    # Detalj för vald rad
    sel = rank_df.iloc[st.session_state["rank_idx"]]
    picked_ticker = sel["Ticker"]
    st.markdown(f"### Detalj: **{picked_ticker}**")

    base_row = _pick_row_by_ticker(df_cur, picked_ticker)
    if base_row is not None:
        with st.spinner(f"Beräknar {picked_ticker} …"):
            methods_df, sanity, meta = _compute_one(base_row, settings, fx_map)
        _render_fairvalue_summary(meta)
        st.caption(f"Sanity: {sanity}")
        st.dataframe(_build_methods_display(methods_df), use_container_width=True)
    else:
        st.warning("Kunde inte hitta den valda tickern i datan.")

# ============================================================
# Del 5/6 — Vyer
#  • Settings, Snapshot, Editor, Lägg till
#  • Portfölj (innehav + Bucket + kommande utdelningar)
#  • Analys & Ranking (använder Del 4/6 helpers)
#  • Batch (massuppdatering från Yahoo)
#  • Köpförslag — läser FV/Kurs/Bucket/Antal från Google Sheets (ingen egen beräkning)
# ============================================================

# ------------------------------------------------------------
# Hjälp: compute_fair_values_for_row (om inte redan definierad)
#  Returnerar en liten dict som Del 4/6 använder i rankingloopen.
# ------------------------------------------------------------
if 'compute_fair_values_for_row' not in globals():
    def compute_fair_values_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Dict[str, Any]:
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
        fv = meta.get("fair_value", {}) or {}
        return {
            "ticker": str(row.get("Ticker") or "").upper(),
            "currency": (meta.get("currency") or row.get("Valuta") or "USD"),
            "price": _f(meta.get("price")),
            "fv_today": _f(fv.get("today")),
            "fv_1y": _f(fv.get("y1")),
            "fv_2y": _f(fv.get("y2")),
            "fv_3y": _f(fv.get("y3")),
        }

# -----------------------------
# Små hjälpare (format/parsers)
# -----------------------------
def _vv(x):
    return "" if x is None or (isinstance(x, float) and (not math.isfinite(x))) else x

def _fmt2(x):
    try:
        if x is None or (isinstance(x, float) and (not math.isfinite(x))):
            return "—"
        return f"{float(x):.2f}"
    except Exception:
        return "—"

def _fmt_pct(p):
    try:
        if p is None or (isinstance(p, float) and (not math.isfinite(p))):
            return "—"
        return f"{float(p)*100:.1f}%"
    except Exception:
        return "—"

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

def _now():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _safe_currency(c):
    return (str(c) if c else "USD").upper()

def _calc_upside(target: Optional[float], price: Optional[float]) -> Optional[float]:
    t = _f(target)
    p = _f(price)
    if t is None or p is None or p <= 0:
        return None
    return (t / p) - 1.0

# -----------------------------
# Namn-mappning & sök/bläddring (återanvänds i flera vyer)
# -----------------------------
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

# ============================================================
# Settings  ✅ redigerbar
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
            _write_df(SETTINGS_TITLE, edited[SETTINGS_COLUMNS])  # skriv endast schema-kolumner
            st.cache_data.clear()
            st.session_state["SETTINGS_MAP"] = get_settings_map()
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
# Editor (manuellt + Yahoo)  ✅ Bucket via rullista
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

def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    y   = fetch_from_yahoo(ticker)
    try:
        est = _fetch_eps_estimates_yahoo(ticker)
    except Exception:
        est = {"eps_1y": None, "eps_2y": None}
    updates = {
        "Timestamp": _now(),
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
        "Senast auto uppdaterad": _now(),
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
                df.loc[idx, "Antal aktier"] = _parse_float(antal_in) or 0.0
                if _parse_float(gav_in) is not None:
                    df.loc[idx, "GAV (SEK)"] = _parse_float(gav_in)
                if bucket_sel is not None:
                    df.loc[idx, "Bucket"] = bucket_sel if bucket_sel != "" else np.nan
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

# ============================================================
# Lägg till ticker  ✅ Bucket via rullista
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
                "Timestamp": _now(),
                "Ticker": tkr,
                "Bolagsnamn": bolagsnamn if bolagsnamn else np.nan,
                "Sektor": sektor if sektor else np.nan,
                "Bucket": bucket_sel if bucket_sel != "" else np.nan,
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
# Portfölj (innehav + Bucket + kommande utdelningar)
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

# --- Utdelningshjälp (återanvänd från tidigare del) ---
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

# ============================================================
# Analys & Ranking — vyn (Del 4/6 har helpersna)
# ============================================================
def page_analysis():
    st.header("🔬 Analys")
    df = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    settings = get_settings_map()
    fx_map   = get_fx_map()

    if "render_analysvy" in globals():
        render_analysvy(df, settings, fx_map)
        return

    # Fallback (kompakt)
    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)
    tkr = _select_with_search_nav("Välj bolag", tickers, names_map, "analysis_idx", "analysis_q")
    if not tkr:
        return

    row = df.loc[df["Ticker"].astype(str) == tkr]
    if row.empty:
        st.error("Kunde inte hitta vald rad.")
        return
    row = row.iloc[0]

    with st.spinner("Beräknar…"):
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    fv = meta.get("fair_value", {}) or {}
    currency = _safe_currency(meta.get("currency"))
    price    = meta.get("price")
    cfa, cfb, cfc, cfd, cfe = st.columns(5)
    cfa.metric("Kurs", f"{_fmt2(price)} {currency}")
    cfb.metric("FV idag", f"{_fmt2(fv.get('today'))} {currency}", _fmt_pct(_calc_upside(fv.get("today"), price)))
    cfc.metric("FV 1 år", f"{_fmt2(fv.get('y1'))} {currency}", _fmt_pct(_calc_upside(fv.get('y1'), price)))
    cfd.metric("FV 2 år", f"{_fmt2(fv.get('y2'))} {currency}", _fmt_pct(_calc_upside(fv.get('y2'), price)))
    cfe.metric("FV 3 år", f"{_fmt2(fv.get('y3'))} {currency}", _fmt_pct(_calc_upside(fv.get('y3'), price)))

    st.caption(f"Sanity: {sanity}")
    st.dataframe(methods_df, use_container_width=True)

def page_ranking():
    st.header("🏆 Ranking – Uppsida")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    if "render_rankingpanel" in globals():
        render_rankingpanel(df, settings, fx_map)
        return

    # Fallback (kompakt) — kräver knapptryck; visar progress
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)
    if not st.button("🔁 Beräkna ranking"):
        st.info("Klicka **Beräkna ranking** för att räkna ut listan.")
        return

    rows = []
    prog = st.progress(0.0)
    total = len(df)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        try:
            methods_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            fvmap = meta.get("fair_value", {}) or {}
            price = meta.get("price")
            if horizon == "Idag": tgt = fvmap.get("today")
            elif horizon == "1 år": tgt = fvmap.get("y1")
            elif horizon == "2 år": tgt = fvmap.get("y2")
            else: tgt = fvmap.get("y3")
            up = _calc_upside(tgt, price)
            rows.append({
                "Ticker": str(r.get("Ticker")),
                "Valuta": _safe_currency(meta.get("currency")),
                "Kurs": _f(price),
                f"Riktkurs {horizon}": _f(tgt),
                "Uppsida (%)": up*100 if up is not None else None,
            })
        except Exception:
            pass
        prog.progress(i/total if total else 1.0)
    prog.empty()

    if not rows:
        st.info("Inget att visa.")
        return
    rank = pd.DataFrame(rows).sort_values("Uppsida (%)", ascending=False, na_position="last").reset_index(drop=True)
    st.caption(f"{len(rank)} bolag")
    st.dataframe(rank, use_container_width=True)

# ============================================================
# Batch (massuppdatering)
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
# 🛒 Köpförslag – LÄSER från Sheets (FV/Kurs/Antal/Bucket)
#   • Kriterier: Kurs < Fair Value (idag) och värde(SEK) < cap
#   • Ingen egen FV-beräkning här
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
                          own_filter: str = "Alla") -> pd.DataFrame:
    """
    Läser KUNSKAP från Data-bladet:
      - Riktkurs idag (FV)  → 'Riktkurs idag'
      - Kurs                  'Aktuell kurs'
      - Antal aktier          'Antal aktier'
      - Valuta, Bucket
      - Cap per innehav (Settings)
    Returnerar förslag sorterat: minsta innehavet först (SEK), sedan högst uppsida %.
    """
    cols_out = [
        "Ticker","Bolagsnamn","Bucket","Valuta","Kurs","FV idag",
        "Uppsida (%)","Äger (antal)","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"
    ]
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols_out)

    # Säkerställ numerik
    base = df_data.copy()
    for c in ("Antal aktier","Aktuell kurs","Riktkurs idag"):
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

            price   = _f(r.get("Aktuell kurs"))
            fv_today = _f(r.get("Riktkurs idag"))
            ccy     = _safe_currency(_nz(r.get("Valuta"), "SEK"))
            name    = str(_nz(r.get("Bolagsnamn"), ""))

            if not _pos(price) or not _pos(fv_today):
                continue
            if price >= fv_today:
                continue

            entry = lu.get(tkr, {"value_sek": 0.0, "qty": _f(r.get("Antal aktier")) or 0.0, "currency": ccy, "price": price})
            qty = entry["qty"] if entry["qty"] is not None else (_f(r.get("Antal aktier")) or 0.0)

            own_status = "own" if (qty and qty > 0) else "no_own"
            if own_filter == "Endast innehav" and own_status != "own":
                continue
            if own_filter == "Endast ej ägda" and own_status != "no_own":
                continue

            # Innehavsvärde i SEK
            fx = _fx_rate_to_sek(ccy, fx_map)
            value_sek = float((price or 0.0) * (qty or 0.0) * fx)
            if _pos(value_sek) and value_sek >= cap:
                continue

            up_pct = (fv_today - price) / price * 100.0 if _pos(price) else None
            rows.append({
                "Ticker": tkr,
                "Bolagsnamn": name,
                "Bucket": bucket,
                "Valuta": ccy,
                "Kurs": price,
                "FV idag": fv_today,
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
    # Läs från session eller Sheets
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
    if df is None or df.empty:
        st.info("Ingen data.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    st.caption("Kriterier: **Aktuell kurs < Riktkurs idag** och **(innehavsvärde i SEK) < cap per innehav i bucket**.")
    filt = st.radio("Visa", ["Alla","Endast innehav","Endast ej ägda"], index=0, horizontal=True)

    with st.spinner("Hämtar förslag från Data-bladet…"):
        sug = build_buy_suggestions(df, settings, fx_map, own_filter=filt)

    if sug.empty:
        st.info("Inga kandidater uppfyller kriterierna just nu.")
        st.caption("Tips: Kör **🏆 Ranking** först för att uppdatera 'Riktkurs idag' i Data-bladet.")
        return

    st.caption(f"{len(sug)} förslag — sorterat minsta innehavet först.")
    show = sug.copy()
    for c in ("Kurs","FV idag","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"):
        if c in show.columns:
            show[c] = show[c].map(_fmt2)
    if "Uppsida (%)" in show.columns:
        show["Uppsida (%)"] = show["Uppsida (%)"].map(lambda v: f"{v:.1f}%" if v is not None else "—")

    st.dataframe(show, use_container_width=True, hide_index=True)

    with st.expander("Summering per Bucket (antal förslag)"):
        agg = sug.groupby("Bucket", as_index=False).size().rename(columns={"size":"Antal förslag"})
        st.dataframe(agg, use_container_width=True, hide_index=True)

# ============================================================
# Del 6/6 — Huvud & routing
#  • Laddning av DATA/FX/Settings till session
#  • Sidomeny & navigering
#  • Startvy (översikt)
#  • Safe main() med felfångare
# ============================================================

# -------------------------
# Start / översiktssida
# -------------------------
def page_home():
    st.header("🏠 Aktieanalys & investeringsförslag — översikt")

    df = st.session_state.get("DATA")
    fx = st.session_state.get("FX") or {}
    settings = get_settings_map()

    # Statusrunda
    n_rows = 0 if df is None or df.empty else len(df)
    n_owned = 0
    if df is not None and not df.empty and "Antal aktier" in df.columns:
        try:
            n_owned = int((pd.to_numeric(df["Antal aktier"], errors="coerce").fillna(0) > 0).sum())
        except Exception:
            n_owned = 0

    colA, colB, colC = st.columns(3)
    colA.metric("Antal rader i Data", f"{n_rows}")
    colB.metric("Antal innehav (>0 st)", f"{n_owned}")
    colC.metric("Valutor inlästa", f"{len(fx)}")

    # Senaste timestamp i DATA (om finns)
    last_ts = None
    if df is not None and not df.empty and "Timestamp" in df.columns:
        try:
            ts_col = pd.to_datetime(df["Timestamp"], errors="coerce")
            if not ts_col.isna().all():
                last_ts = str(ts_col.max())
        except Exception:
            last_ts = None

    st.caption("Basvaluta/visning: **SEK** (värderingsmetoder sker i bolagets handelsvaluta).")
    if last_ts:
        st.caption(f"Senast uppdaterat (Data): {last_ts}")

    st.markdown("---")
    st.subheader("Snabbnavigering")
    st.write(
        "• **🔬 Analys** — djupdyk i ett bolag och se FV idag/1/2/3 år.\n"
        "• **🏆 Ranking** — räkna uppsida och skriv FV till Data.\n"
        "• **📦 Portfölj** — innehav, buckets och kommande utdelningar.\n"
        "• **🛒 Köpförslag** — läser kurs/FV/antal från Data (inga egna beräkningar).\n"
        "• **✏️ Editor** — manuell redigering + Yahoo-prefill.\n"
        "• **➕ Lägg till** — skapa ny rad (valfri Yahoo-prefill).\n"
        "• **🧩 Massuppdatering** — uppdatera många tickers från Yahoo (1s/bolag).\n"
        "• **⚙️ Settings** — caps per bucket, källskatt, m.m.\n"
        "• **🕒 Snapshot** — visa snapshots om de finns."
    )

# -------------------------
# Session-laddning & helpers
# -------------------------
def _ensure_session_loaded(force_reload: bool = False) -> None:
    """Läs DATA/FX/Settings till session_state."""
    if force_reload or "DATA" not in st.session_state:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception as e:
            st.warning(f"Kunde inte läsa Data-bladet: {e}")
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
    if force_reload or "FX" not in st.session_state:
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception as e:
            st.warning(f"Kunde inte läsa Valutakurser: {e}")
            st.session_state["FX"] = {}
    if force_reload or "SETTINGS_MAP" not in st.session_state:
        try:
            st.session_state["SETTINGS_MAP"] = get_settings_map()
        except Exception as e:
            st.warning(f"Kunde inte läsa Settings: {e}")
            st.session_state["SETTINGS_MAP"] = {}

def _sidebar_nav() -> str:
    st.sidebar.markdown("## 📂 Navigering")

    pages = [
        "🏠 Start",
        "🔬 Analys",
        "🏆 Ranking",
        "📦 Portfölj",
        "🛒 Köpförslag",
        "✏️ Editor",
        "➕ Lägg till",
        "🧩 Massuppdatering",
        "⚙️ Settings",
        "🕒 Snapshot",
    ]
    cur = st.sidebar.radio("Välj vy", pages, index=0)

    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Ladda om från Google Sheets"):
        _ensure_session_loaded(force_reload=True)
        st.sidebar.success("Omladdat.")

    if st.sidebar.button("🧹 Rensa cache (st.cache_data)"):
        try:
            st.cache_data.clear()
            st.sidebar.success("Cache rensad.")
        except Exception as e:
            st.sidebar.error(f"Kunde inte rensa cache: {e}")

    return cur

def _route(page_label: str) -> None:
    if page_label == "🏠 Start":
        page_home()
    elif page_label == "🔬 Analys":
        page_analysis()
    elif page_label == "🏆 Ranking":
        page_ranking()
    elif page_label == "📦 Portfölj":
        page_portfolio()
    elif page_label == "🛒 Köpförslag":
        page_buy_suggestions()
    elif page_label == "✏️ Editor":
        page_editor()
    elif page_label == "➕ Lägg till":
        page_add_ticker()
    elif page_label == "🧩 Massuppdatering":
        page_batch()
    elif page_label == "⚙️ Settings":
        page_settings()
    elif page_label == "🕒 Snapshot":
        page_snapshot()
    else:
        page_home()

# -------------------------
# main()
# -------------------------
def main():
    # Säkerställ basladdning
    _ensure_session_loaded(force_reload=False)

    # Sidomeny
    cur = _sidebar_nav()

    # Kör vald vy
    _route(cur)

# Kör appen
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")
