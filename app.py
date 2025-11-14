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
            # vissa versioner har 'shares'
            shares_os   = _f(getattr(fi, "shares", None))
        except Exception:
            pass

        # Fallback: försök via history om pris saknas
        if last_price is None:
            try:
                h = t.history(period="5d")
                if not h.empty:
                    last_price = float(h["Close"].dropna().iloc[-1])
            except Exception:
                pass

        # --- Bolagsnamn ---
        company_name = None
        try:
            # yfinance.info är långsammare/ibland tomt, men använd som fallback
            info = t.get_info() if hasattr(t, "get_info") else getattr(t, "info", {})  # nyare yfinance har get_info
            if info:
                company_name = info.get("longName") or info.get("shortName")
                currency = currency or info.get("currency")
                trailing_pe = trailing_pe or _f(info.get("trailingPE"))
                forward_pe  = forward_pe or _f(info.get("forwardPE"))
                market_cap  = market_cap or _f(info.get("marketCap"))
                # ibland finns 'sharesOutstanding'
                shares_os   = shares_os or _f(info.get("sharesOutstanding"))
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
            q_is = t.quarterly_income_stmt or t.quarterly_financials  # kompatibilitet
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

        # TTM Revenue
        try:
            if q_is is not None and not q_is.empty:
                # yfinance form: rows as items, columns as periods
                # Total Revenue kan heta 'Total Revenue' eller 'TotalRevenue'
                for key in ["Total Revenue", "TotalRevenue", "Revenue", "TotalSales"]:
                    if key in q_is.index:
                        rev_ttm = _sum_last_n(q_is.loc[key], 4)
                        break
                # EBITDA-kan nyckel variera
                for key in ["EBITDA", "Ebitda"]:
                    if key in q_is.index:
                        ebitda_ttm = _sum_last_n(q_is.loc[key], 4)
                        break
                # EPS TTM: försök via Net Income / genomsnittliga aktier
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
                        total_debt = float(q_bs.loc[key].dropna().iloc[0])
                        break
                cash = None
                for key in ["Cash And Cash Equivalents", "CashAndCashEquivalents", "Cash And Short Term Investments"]:
                    if key in q_bs.index:
                        cash = float(q_bs.loc[key].dropna().iloc[0])
                        break
                if total_debt is not None and cash is not None:
                    net_debt = float(total_debt - cash)

                total_equity = None
                for key in ["Total Stockholder Equity", "TotalStockholderEquity", "Stockholders Equity"]:
                    if key in q_bs.index:
                        total_equity = float(q_bs.loc[key].dropna().iloc[0])
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
                # summera sista 12m (eller 4 senaste) som enkel approximation
                divs_sorted = divs.sort_index(ascending=False)
                annual_div = float(divs_sorted.iloc[:4].sum())
                div_freq = _infer_div_freq_from_series(divs)
        except Exception:
            pass

        # Historisk CAGR (5 år) för Revenue & EPS
        rev_cagr_5y = None
        eps_cagr_5y = None
        try:
            # yfinance.earnings (deprecated i vissa versioner); försök även med annual_* statements
            annual_is = None
            try:
                annual_is = t.income_stmt or t.financials
            except Exception:
                pass

            # Revenue:
            if annual_is is not None and not annual_is.empty:
                # använd 'Total Revenue' rader över kolumner (år)
                for key in ["Total Revenue", "TotalRevenue", "Revenue", "TotalSales"]:
                    if key in annual_is.index:
                        series = pd.to_numeric(annual_is.loc[key], errors="coerce").dropna()
                        if len(series) >= 2:
                            # första & sista över upp till ~5 år
                            first = float(series.iloc[-1])  # äldsta
                            last  = float(series.iloc[0])   # senaste
                            years = min(5, max(1, len(series)-1))
                            rev_cagr_5y = _cagr(first, last, years)
                        break

                # EPS: försök via 'Diluted EPS' -> annars Net Income / shares senaste år (osäkrare)
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
                    # fallback via Net Income / shares_os
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
        })
    except Exception as e:
        # Vid helt haveri: returnera tomt så att updaters kan hoppa över
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
            "Årlig utdelning","Rev CAGR","EPS CAGR"
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
        # räkna hur många fält som faktiskt ändrades (grovt)
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
    - 1 sekunds fördröjning per bolag (default) enligt dina önskemål.
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
# (Slut Del 2/6)
# Nästa del (Del 3/6) innehåller: Värderingsmetoder & fair value
#  • compute_methods_for_row (P/E-band, EV/S, EV/EBITDA, P/B, AFFO/FCF mm)
#  • multipel-decay, metodval (Primär metod), riktkurser (idag/1/2/3 år)
#  • Snapshot-logg (Snapshot-bladet)
# ============================================================

# ============================================================
# Del 3/6 — Beräkningsmotor
#  • Kompatibel wrapper: fetch_from_yahoo() (bygger på yahoo_snapshot)
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
    Kompatibilitetslager som mappar Del 2/6:s yahoo_snapshot() till
    samma nycklar som resten av appen förväntar sig.
    """
    snap = yahoo_snapshot(ticker)  # från Del 2/6
    return {
        "price":            _f(snap.get("Aktuell kurs")),
        "currency":         (snap.get("Valuta") or "USD"),
        "shares_out":       _f(snap.get("Utestående aktier")),
        "net_debt":         _f(snap.get("Net debt")) if "Net debt" in snap else _f(snap.get("Net debt", None)),
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
                   rev_cagr_hist: Optional[float]) -> tuple[float, float, float, float]:
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

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float], rev2: Optional[float], rev3: Optional[float]) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
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
def _fetch_eps_estimates_yahoo(ticker: str) -> dict[str, Optional[float]]:
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
def _compute_fair_value_row_v2(methods_df: pd.DataFrame, now_price: Optional[float]) -> dict:
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
def compute_methods_for_row(row: pd.Series, settings: dict[str, str], fx_map: dict[str, float]) -> tuple[pd.DataFrame, str, dict[str, Any]]:
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
# Nästa del (Del 4/6) — Portfölj & utdelningar + Bucket-tak-förslag
# ============================================================

# ============================================================
# Del 4/6 — Portfölj & utdelningar (SEK) + Bucket-tak
#  • Skattar på utdelning per valuta (default: USD/CAD 15%, NOK 25%, SEK 0%)
#  • Portföljsammanställning (värde i SEK, årlig utdelning, snitt/månad)
#  • Lista: kommande utbetalningsdatum (ignorerar passerade)
#  • Bucket-tak: visar vilka innehav som över/under mål (A/B/C; tillväxt/utdelning)
#  • Alla belopp i SEK i denna vy (värde & utdelningar)
# ============================================================

# -------------------------
# Skattesats per valuta (kan överskridas via Settings)
# -------------------------
def _withholding_rate(currency: str, settings: dict[str, str]) -> float:
    c = (currency or "").upper().strip()
    # Settings override (t.ex. 'tax_usd' = 0.15)
    overrides = {
        "USD": _f(settings.get("tax_usd")),
        "CAD": _f(settings.get("tax_cad")),
        "NOK": _f(settings.get("tax_nok")),
        "SEK": _f(settings.get("tax_sek")),
        "EUR": _f(settings.get("tax_eur")),
        "DKK": _f(settings.get("tax_dkk")),
        "GBP": _f(settings.get("tax_gbp")),
    }
    base = {
        "USD": 0.15,
        "CAD": 0.15,
        "NOK": 0.25,
        "SEK": 0.00,
        # Övriga valutor → 0% om ej satt (undvik att anta fel skattesats)
    }
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

# -------------------------
# Utdelningar — beräkningar
# -------------------------
def _compute_next_div_row(row: pd.Series, fx_map: dict[str, float], settings: dict[str, str]) -> Optional[dict]:
    """
    Räknar *nästa* kända utbetalning:
      • Kräver kolumner: 'Nästa utdelningsdatum', 'Nästa utdelning per aktie'
      • Ignorerar passerade datum
      • Nettar källskatt enligt valuta
      • Returnerar rad för tabell "Kommande utdelningar"
    """
    ticker = str(row.get("Ticker", "")).strip()
    ccy    = str(row.get("Valuta", "SEK")).upper().strip()
    shares = _pos(row.get("Antal aktier")) or 0.0

    # Datum (betalningsdatum – inte X-dag)
    pay_raw = row.get("Nästa utdelningsdatum")
    pay_dt: Optional[dt.date] = None
    try:
        if isinstance(pay_raw, (dt.date, dt.datetime, pd.Timestamp)):
            pay_dt = pd.to_datetime(pay_raw).date()
        elif isinstance(pay_raw, str) and pay_raw.strip():
            pay_dt = pd.to_datetime(pay_raw, dayfirst=True, errors="coerce").date()
    except Exception:
        pay_dt = None

    # Filtrera bort om passerat eller saknas
    today = dt.date.today()
    if pay_dt is None or pay_dt < today:
        return None

    dps_gross = _f(row.get("Nästa utdelning per aktie"))
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
    """
    Årlig utdelning (netto, SEK) ≈ 'Årlig utdelning' * Antal * (1 - skatt) * FX
    Om 'Årlig utdelning' saknas → 0.
    """
    ccy    = str(row.get("Valuta", "SEK")).upper().strip()
    shares = _pos(row.get("Antal aktier")) or 0.0
    dps_y  = _f(row.get("Årlig utdelning"))
    if dps_y is None or shares <= 0:
        return 0.0
    tax = _withholding_rate(ccy, settings)
    net = float(dps_y) * (1.0 - float(tax)) * float(shares)
    sek = _to_sek(net, ccy, fx_map) or 0.0
    return float(sek)

# -------------------------
# Portföljvärde (SEK)
# -------------------------
def _position_value_sek(row: pd.Series, fx_map: dict[str, float]) -> float:
    """
    Värde i SEK = (Aktuell kurs i bolagets valuta) * antal * FX
    Om 'Aktuell kurs' saknas använder vi snapshot via yahoo (Del 2/6).
    """
    ccy    = str(row.get("Valuta", "SEK")).upper().strip()
    price  = _f(row.get("Aktuell kurs"))
    if price is None:
        # Försök hämta från Yahoo för enstaka rad
        tk = str(row.get("Ticker", "")).strip()
        try:
            snap = yahoo_snapshot(tk)
            price = _f(snap.get("Aktuell kurs"))
            # (valuta kan korrigeras om saknas)
            if not ccy:
                ccy = str(snap.get("Valuta") or "SEK").upper()
        except Exception:
            price = None
    shares = _pos(row.get("Antal aktier")) or 0.0
    if price is None or shares <= 0:
        return 0.0
    return float(_to_sek(price * shares, ccy, fx_map) or 0.0)

def _build_portfolio_tables(df_data: pd.DataFrame, fx_map: dict[str, float], settings: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Returnerar:
      • positions_df: tabell över innehavsvärden per rad (SEK)
      • upcoming_df : tabell över *nästa* kända utbetalningar (netto, SEK)
      • summary     : totals (värde, årlig utd, per månad)
    """
    base = df_data.copy()
    if "Antal aktier" in base.columns:
        base["Antal aktier"] = base["Antal aktier"].apply(_f)

    # Filtrera faktiskt ägda (Antal > 0)
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
    upcoming_df = pd.DataFrame(up_rows) if up_rows else pd.DataFrame(columns=["Datum","Ticker","Valuta","Antal","DPS (brutto)","Skatt (%)","DPS (netto)","Belopp (brutto)","Belopp (netto)","Belopp SEK (netto)"])
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

# -------------------------
# Bucket-tak (A/B/C tillväxt/utdelning)
# -------------------------
def _bucket_targets_from_settings(settings: dict[str, str]) -> dict:
    """
    Hämtar hink-mål från Settings om de finns, annars default enligt sparad metodik.
    Nycklar (förslag):
      - bucket_a_tillvaxt_sek ≈ 20000
      - bucket_a_utdelning_sek ≈ 10000
      - bucket_b_tillvaxt_sek ≈ 10000
      - bucket_b_utdelning_sek ≈ 7000
      - bucket_c_tillvaxt_sek ≈ 6000
      - bucket_c_utdelning_sek ≈ 4000
    """
    def _pick(k: str, d: float) -> float:
        v = _f(settings.get(k))
        return float(v) if v is not None else d
    return {
        ("A","tillväxt"):   _pick("bucket_a_tillvaxt_sek",   20000.0),
        ("A","utdelning"):  _pick("bucket_a_utdelning_sek",  10000.0),
        ("B","tillväxt"):   _pick("bucket_b_tillvaxt_sek",   10000.0),
        ("B","utdelning"):  _pick("bucket_b_utdelning_sek",   7000.0),
        ("C","tillväxt"):   _pick("bucket_c_tillvaxt_sek",    6000.0),
        ("C","utdelning"):  _pick("bucket_c_utdelning_sek",   4000.0),
    }

def _classify_bucket_row(row: pd.Series) -> tuple[str, str]:
    """
    Förväntar kolumner: 'Hink' (A/B/C) och 'Typ' ('tillväxt'/'utdelning').
    Om saknas: försöker tolka från 'Bucket' eller lämnar ('','').
    """
    def _norm(s):
        return str(s or "").strip().lower()
    # Primära kolumner
    hink = str(row.get("Hink", "")).strip().upper()
    typ  = _norm(row.get("Typ"))
    # Alternativ heuristik
    if not hink and row.get("Bucket"):
        b = str(row.get("Bucket")).strip().upper()
        if b in ("A","B","C"):
            hink = b
    if not typ and row.get("Kategori"):
        k = _norm(row.get("Kategori"))
        if "utdel" in k:
            typ = "utdelning"
        elif "tillv" in k or "growth" in k:
            typ = "tillväxt"
    return hink, ("utdelning" if typ=="utdelning" else "tillväxt" if typ=="tillväxt" else "")

def _bucket_table(positions_df: pd.DataFrame, df_data: pd.DataFrame, fx_map: dict[str, float], settings: dict[str, str]) -> pd.DataFrame:
    """
    Returnerar per rad: Ticker, Hink, Typ, Värde (SEK), Mål (SEK), Avvikelse, Status
    """
    if positions_df.empty:
        return pd.DataFrame(columns=["Ticker","Hink","Typ","Värde (SEK)","Mål (SEK)","Avvikelse (SEK)","Status"])

    tgt = _bucket_targets_from_settings(settings)
    rows = []
    # Skapa snabb lookup för positioners värde
    val_map = {str(r["Ticker"]).strip(): float(r["Värde (SEK)"]) for _, r in positions_df.iterrows()}

    for _, r in df_data.iterrows():
        t = str(r.get("Ticker", "")).strip()
        if t not in val_map:
            continue
        hink, typ = _classify_bucket_row(r)
        if not hink or not typ:
            continue
        goal = float(tgt.get((hink, typ), 0.0))
        val  = float(val_map.get(t, 0.0))
        diff = val - goal
        status = "OK"
        if goal > 0:
            if val > goal * 1.10:
                status = "Över mål"
            elif val < goal * 0.90:
                status = "Under mål"
        rows.append({
            "Ticker": t,
            "Hink": hink,
            "Typ": typ,
            "Värde (SEK)": val,
            "Mål (SEK)": goal,
            "Avvikelse (SEK)": diff,
            "Status": status,
        })
    out = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["Ticker","Hink","Typ","Värde (SEK)","Mål (SEK)","Avvikelse (SEK)","Status"])
    if not out.empty:
        order = pd.CategoricalDtype(["A","B","C"], ordered=True)
        out["Hink"] = out["Hink"].astype(order)
        out = out.sort_values(["Hink","Typ","Avvikelse (SEK)"], ascending=[True, True, True]).reset_index(drop=True)
    return out

# -------------------------
# Render: Portfölj-vy (anropas i Del 6/6)
# -------------------------
def render_portfolio_view(df_data: pd.DataFrame, fx_map: dict[str, float], settings: dict[str, str]):
    """
    Visar tre delar:
      1) Nyckeltal (Totalvärde SEK, Årlig utdelning SEK, Per månad SEK)
      2) Kommande utbetalningar (betalningsdatum-lista, netto i SEK)
      3) Bucket-tak per innehav (mål vs värde)
    """
    positions_df, upcoming_df, summary = _build_portfolio_tables(df_data, fx_map, settings)
    bucket_df = _bucket_table(positions_df, df_data, fx_map, settings)

    col1, col2, col3, col4 = st.columns([1.2,1.2,1.2,1.0])
    col1.metric("Totalt portföljvärde (SEK)", f"{summary['total_value_sek']:,.0f}".replace(",", " "))
    col2.metric("Årlig utdelning (SEK, netto)", f"{summary['total_div_year_sek']:,.0f}".replace(",", " "))
    col3.metric("Utdelning/månad (SEK, netto)", f"{summary['div_per_month_sek']:,.0f}".replace(",", " "))
    col4.metric("Innehav", f"{summary['count_positions']} st")

    st.markdown("### Kommande utdelningsutbetalningar (netto, SEK)")
    if upcoming_df.empty:
        st.info("Inga kommande betalningsdatum hittades (kontrollera kolumnerna ”Nästa utdelningsdatum” och ”Nästa utdelning per aktie”).")
    else:
        show_cols = ["Datum","Ticker","Valuta","Antal","DPS (brutto)","Skatt (%)","DPS (netto)","Belopp (netto)","Belopp SEK (netto)"]
        st.dataframe(upcoming_df[show_cols], use_container_width=True, hide_index=True)

    st.markdown("### Innehavsvärden (SEK)")
    if positions_df.empty:
        st.warning("Inga positioner med Antal > 0.")
    else:
        st.dataframe(positions_df, use_container_width=True, hide_index=True)

    st.markdown("### Bucket-tak (mål vs värde)")
    if bucket_df.empty:
        st.caption("Ingen bucket-klassning hittades (lägg till kolumnerna **Hink** och **Typ**).")
    else:
        st.dataframe(bucket_df, use_container_width=True, hide_index=True)

# ============================================================
# (Slut Del 4/6)
# Nästa del (Del 5/6) — Analys/Ranking-vy (använder compute_methods_for_row + fair value)
# ============================================================

# ============================================================
# Del 5/6 — Vyer
#  • Settings (sparar till Google Sheets)
#  • Snapshot
#  • Editor (manuellt + Yahoo-prefill)
#  • Lägg till ticker
#  • Portfölj (använder render_portfolio_view från Del 4)
#  • Analys (metodtabell + Fair Value-rad)
#  • Ranking (uppsida)
#  • Batch (massuppdatering från Yahoo)
# ============================================================

# -----------------------------
# Små helpers (skydda mot dubbletter)
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

# En liten wrapper som min Del 4/6 använder (för att hämta pris/valuta)
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
        # Stöd både withholding_* (legacy) och tax_* (override-nycklar)
        wh_usd = st.number_input("USD", 0.0, 0.5, float(_f(s.get("withholding_USD", s.get("tax_usd"))) or 0.15), 0.01)
        wh_nok = st.number_input("NOK", 0.0, 0.5, float(_f(s.get("withholding_NOK", s.get("tax_nok"))) or 0.25), 0.01)
        wh_cad = st.number_input("CAD", 0.0, 0.5, float(_f(s.get("withholding_CAD", s.get("tax_cad"))) or 0.15), 0.01)
        wh_eur = st.number_input("EUR", 0.0, 0.5, float(_f(s.get("withholding_EUR", s.get("tax_eur"))) or 0.15), 0.01)
        wh_sek = st.number_input("SEK", 0.0, 0.5, float(_f(s.get("withholding_SEK", s.get("tax_sek"))) or 0.00), 0.01)

    st.markdown("#### Bucket-tak per innehav (SEK)")
    cA, cB = st.columns(2)
    with cA:
        cap_A_t = st.number_input("Bucket A tillväxt (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_A_tillvaxt")) or 0.0), step=100.0)
        cap_B_t = st.number_input("Bucket B tillväxt (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_B_tillvaxt")) or 0.0), step=100.0)
        cap_C_t = st.number_input("Bucket C tillväxt (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_C_tillvaxt")) or 0.0), step=100.0)
    with cB:
        cap_A_u = st.number_input("Bucket A utdelning (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_A_utdelning")) or 0.0), step=100.0)
        cap_B_u = st.number_input("Bucket B utdelning (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_B_utdelning")) or 0.0), step=100.0)
        cap_C_u = st.number_input("Bucket C utdelning (SEK)", min_value=0.0, value=float(_f(s.get("bucket_cap_C_utdelning")) or 0.0), step=100.0)

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
    st.dataframe(_read_df(FX_TITLE), use_container_width=True)
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
    st.dataframe(snap, use_container_width=True)

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

# -----------------------------
# Yahoo-updates för Editor/Batch
# -----------------------------
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
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

# -----------------------------
# Editor
# -----------------------------
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
# Portfölj (kopplar till Del 4/6)
# -----------------------------
def page_portfolio():
    st.header("📦 Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx = st.session_state.get("FX", {}) or {}
    settings = get_settings_map()
    try:
        render_portfolio_view(df, fx, settings)  # definierad i Del 4/6
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")

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
    st.dataframe(methods_df, use_container_width=True)

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
    st.dataframe(st.session_state["DATA"], use_container_width=True)

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
#  • Init (DATA, FX, Settings)
#  • Auto-FX enligt inställning
#  • Sidebar-navigering
#  • Page-routing
# ============================================================

# Fallbacks om tidigare delar saknar dessa små helpers
if "now_stamp" not in globals():
    def now_stamp():
        try:
            return _now()
        except Exception:
            from datetime import datetime
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if "ensure_data_columns" not in globals():
    def ensure_data_columns(df: pd.DataFrame | None) -> pd.DataFrame:
        base = pd.DataFrame(columns=DATA_COLUMNS)
        if df is None or df.empty:
            return base.copy()
        for c in DATA_COLUMNS:
            if c not in df.columns:
                df[c] = np.nan
        return df

# -----------------------------
# Init loaders
# -----------------------------
def _load_all_state(force_reload: bool = False):
    ss = st.session_state

    # Settings
    try:
        ss["SETTINGS_MAP"] = get_settings_map()
    except Exception as e:
        st.warning(f"Kunde inte läsa Settings: {e}")
        ss["SETTINGS_MAP"] = {}

    # FX
    try:
        if force_reload or ("FX" not in ss):
            ss["FX"] = get_fx_map()
    except Exception as e:
        st.warning(f"Kunde inte läsa Valutakurser: {e}")
        ss["FX"] = {}

    # DATA
    try:
        if force_reload or ("DATA" not in ss):
            df = read_data_df()
            df = ensure_data_columns(df)
            ss["DATA"] = df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        if "DATA" not in ss:
            ss["DATA"] = ensure_data_columns(pd.DataFrame())

# -----------------------------
# Sidebar
# -----------------------------
def _sidebar():
    st.sidebar.markdown("## Navigering")

    # Snabbåtgärder
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔄 Läs in", key="btn_reload_all"):
            _load_all_state(force_reload=True)
            st.rerun()
    with col2:
        if st.button("💾 Spara DATA", key="btn_save_all"):
            try:
                write_data_df(st.session_state.get("DATA", pd.DataFrame(columns=DATA_COLUMNS)))
                st.sidebar.success("DATA sparad till Sheets.")
            except Exception as e:
                st.sidebar.error(f"Kunde inte spara: {e}")

    st.sidebar.markdown("---")

    pages = {
        "Analys": page_analysis,
        "Portfölj": page_portfolio,
        "Ranking": page_ranking,
        "Editor": page_editor,
        "Lägg till": page_add_ticker,
        "Batch": page_batch,
        "Snapshot": page_snapshot,
        "Settings": page_settings,
    }
    choice = st.sidebar.radio("Välj vy", list(pages.keys()), index=0)
    return pages[choice]

# -----------------------------
# Auto-FX vid start (en gång)
# -----------------------------
def _auto_fx_refresh_once():
    ss = st.session_state
    try:
        s = get_settings_map()
        auto = str(s.get("auto_refresh_on_start", "0")) == "1"
        if not auto:
            return
        if ss.get("_fx_autorefreshed_once"):
            return
        _load_fx_and_update_sheet()
        ss["_fx_autorefreshed_once"] = True
        ss["FX"] = get_fx_map()
        st.toast("Valutakurser uppdaterade automatiskt vid start.", icon="🔁")
    except Exception as e:
        st.warning(f"Auto-FX misslyckades: {e}")

# -----------------------------
# Hem (liten översikt)
# -----------------------------
def _header_overview():
    df: pd.DataFrame = st.session_state.get("DATA", pd.DataFrame(columns=DATA_COLUMNS))
    n = len(df)
    owned = pd.to_numeric(df.get("Antal aktier", pd.Series(dtype=float)), errors="coerce").fillna(0) > 0
    num_owned = int(owned.sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Bolag i databasen", f"{n}")
    c2.metric("Antal innehav (>0)", f"{num_owned}")
    try:
        last_snap = _read_df(SNAPSHOT_TITLE)
        c3.metric("Snapshots", f"{0 if last_snap.empty else len(last_snap)}")
    except Exception:
        c3.metric("Snapshots", "—")

# -----------------------------
# main()
# -----------------------------
def main():
    # Grundladdning
    _load_all_state(force_reload=False)
    _auto_fx_refresh_once()

    st.title("📈 Aktieanalys & investeringsförslag")
    _header_overview()

    # Routing
    page_fn = _sidebar()
    st.markdown("---")
    try:
        page_fn()
    except Exception as e:
        st.error(f"Fel i vy: {e}")

    # Footer
    st.markdown("---")
    st.caption("Bas: DATA/Settings/Valutakurser i Google Sheets · Fair Value = median över metoder · ©")

if __name__ == "__main__":
    main()
# ============================ SLUT PÅ app.py ============================
