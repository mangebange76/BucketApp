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
# app.py — Aktieanalys & investeringsförslag
# Del 2/6: Datainsamling & beräkningshjälp (Yahoo)
#
#  • Robust Yahoo-snapshot (pris, valuta, MCAP/EV, TTM, utdelning)
#  • EPS/Revenue/EBITDA TTM (kvartalssummor/fallback årligt)
#  • 5-års historisk CAGR (Revenue & EPS) om data finns
#  • Uppdateringshjälpare: endast skriv över fält vi lyckas hämta
# ============================================================

# =============== Interna hjälpare för Yahoo ===============
def _yf_last_price_and_ccy(tkr: str) -> Tuple[Optional[float], Optional[str]]:
    """Försök hämta senaste pris + valuta via yfinance (fast_info → history fallback)."""
    try:
        y = yf.Ticker(tkr)
        px = None
        ccy = None
        # fast_info
        try:
            fi = y.fast_info
            px = _f(fi.get("last_price"))
            ccy = fi.get("currency")
        except Exception:
            pass
        # fallback pris via history
        if px is None:
            h = y.history(period="5d")
            if isinstance(h, pd.DataFrame) and not h.empty and "Close" in h:
                px = _f(h["Close"].dropna().iloc[-1])
        # valuta via info
        if not ccy:
            try:
                inf = y.info or {}
                ccy = inf.get("currency")
            except Exception:
                ccy = None
        return px, (ccy.upper() if isinstance(ccy, str) else None)
    except Exception:
        return None, None

def _yf_quarter_sum_last4(dfq: Optional[pd.DataFrame], col: str) -> Optional[float]:
    """Summera senaste 4 kvartal för kolumn 'col' (om finns), returnera float eller None."""
    try:
        if dfq is None or not isinstance(dfq, pd.DataFrame) or dfq.empty:
            return None
        ser = dfq.get(col)
        if ser is None or ser.dropna().empty:
            return None
        vals = pd.to_numeric(ser.dropna(), errors="coerce").dropna()
        if vals.shape[0] == 0:
            return None
        # yfinance quarterly_* brukar ha index som datum (senaste först/eller inte). Ta 4 senaste.
        return _f(vals.iloc[:4].sum())
    except Exception:
        return None

def _yf_annual_last(dfy: Optional[pd.DataFrame], col: str) -> Optional[float]:
    """Senaste årsrad (annual) för 'col' som fallback om kvartal saknas."""
    try:
        if dfy is None or not isinstance(dfy, pd.DataFrame) or dfy.empty:
            return None
        ser = dfy.get(col)
        if ser is None or ser.dropna().empty:
            return None
        vals = pd.to_numeric(ser.dropna(), errors="coerce").dropna()
        if vals.shape[0] == 0:
            return None
        return _f(vals.iloc[0])
    except Exception:
        return None

def _yf_next_dividend_info(y: "yf.Ticker") -> Tuple[Optional[dt.date], Optional[float], Optional[str]]:
    """
    Försök härleda nästa utdelningsbetalning (betalningsdatum & belopp per aktie).
    yfinance exponerar oftast 'dividends' historik; vi kan inte alltid säkert få nästa,
    men vi kan åtminstone ge senaste betalningens belopp och frekvens.
    Returnerar (nästa_datum, nästa_belopp, frekvenskod 'M'/'Q'/'S'/'A' eller None).
    """
    try:
        div = y.dividends
        if isinstance(div, pd.Series) and not div.empty:
            div = div.dropna()
            if not div.empty:
                # frekvens: mät intervallen mellan betalningar (≈ mån/kvartal/halv/år)
                dates = div.index.sort_values()
                # beräkna median intervall i dagar
                if len(dates) >= 3:
                    deltas = (dates[1:] - dates[:-1]).days
                    md = np.median(deltas)
                    # grov mappning
                    if md <= 40:   freq = "M"
                    elif md <= 120: freq = "Q"
                    elif md <= 220: freq = "S"
                    else:           freq = "A"
                else:
                    freq = "Q"  # vanligast

                last_dt = dates[-1].date()
                last_amt = _f(div.iloc[-1])

                # gissa nästa betalningsdatum utifrån frekvens
                def _add_months(d: dt.date, months: int) -> dt.date:
                    y_, m_ = d.year, d.month + months
                    y_ += (m_ - 1) // 12
                    m_ = ((m_ - 1) % 12) + 1
                    day = min(d.day, 28)  # säkra
                    return dt.date(y_, m_, day)

                if freq == "M":
                    nxt = _add_months(last_dt, 1)
                elif freq == "Q":
                    nxt = _add_months(last_dt, 3)
                elif freq == "S":
                    nxt = _add_months(last_dt, 6)
                else:
                    nxt = dt.date(last_dt.year + 1, last_dt.month, min(last_dt.day, 28))

                return nxt, last_amt, freq
    except Exception:
        pass
    return None, None, None

def _yf_shares_and_netdebt(y: "yf.Ticker") -> Tuple[Optional[float], Optional[float]]:
    """
    Hämtar utestående aktier (senaste) och Net Debt ≈ TotalDebt - Cash.
    Faller tillbaka till info.get('sharesOutstanding') om statements saknas.
    """
    shares = None
    netdebt = None
    try:
        # Försök via info först (ibland mest robust)
        try:
            inf = y.info or {}
            shares = _f(inf.get("sharesOutstanding"))
            total_debt_info = _f(inf.get("totalDebt"))
            cash_info = _f(inf.get("totalCash"))
            if total_debt_info is not None or cash_info is not None:
                netdebt = (total_debt_info or 0.0) - (cash_info or 0.0)
        except Exception:
            pass

        # Bokslut fallback
        try:
            bs_q = y.quarterly_balance_sheet
            bs_a = y.balance_sheet
            td = None; cs = None
            if (netdebt is None) or (shares is None):
                # shares kan ev. hämtas från shares if available i cashflow/earnings, men info räcker oftast
                pass
            if netdebt is None:
                td = None
                cs = None
                # försök Total Debt
                for key in ["Total Debt","TotalDebt","Total debt","totalDebt"]:
                    if td is None:
                        td = _yf_annual_last(bs_q, key) or _yf_annual_last(bs_a, key)
                # försök Cash
                for key in ["Cash","Cash And Cash Equivalents","CashAndCashEquivalents","Total Cash","totalCash"]:
                    if cs is None:
                        cs = _yf_annual_last(bs_q, key) or _yf_annual_last(bs_a, key)
                if td is not None or cs is not None:
                    netdebt = (td or 0.0) - (cs or 0.0)
        except Exception:
            pass
    except Exception:
        pass
    return shares, netdebt

def _yf_ttm_blocks(y: "yf.Ticker") -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Returnerar (Rev TTM, EBITDA TTM, EPS TTM) via kvartal (fallback årligt)."""
    rev, ebitda, eps = None, None, None
    try:
        # Revenue
        rev = _yf_quarter_sum_last4(y.quarterly_financials, "Total Revenue")
        if rev is None:
            rev = _yf_annual_last(y.financials, "Total Revenue")
        # EBITDA
        ebitda = _yf_quarter_sum_last4(y.quarterly_financials, "Ebitda")
        if ebitda is None:
            ebitda = _yf_annual_last(y.financials, "Ebitda")
        # EPS (TTM, approximera via quarterly_earnings)
        try:
            qe = y.quarterly_earnings
            if isinstance(qe, pd.DataFrame) and not qe.empty and "Earnings" in qe:
                # quarterly_earnings i yfinance har ofta kolumner ["Revenue","Earnings"]
                # EPS ≈ Earnings / Shares (svårt här) → använd annual_earnings fallback
                eps = None
        except Exception:
            pass
        if eps is None:
            ae = y.earnings
            # 'earnings' (annual) har ibland kolumner ["Revenue","Earnings"], då kan vi inte få EPS direkt.
            eps = None
        # Om yfinance saknar EPS TTM – lämna None (manuell/annan källa kan fylla)
    except Exception:
        pass
    return rev, ebitda, eps

def _cagr(start: float, end: float, years: float) -> Optional[float]:
    """CAGR helper."""
    try:
        if start is None or end is None or years is None:
            return None
        if start <= 0 or years <= 0:
            return None
        return (end / start) ** (1.0 / years) - 1.0
    except Exception:
        return None

def _yf_cagr_5y(y: "yf.Ticker") -> Tuple[Optional[float], Optional[float]]:
    """
    Försök härleda 5-års CAGR för Revenue & EPS från årliga rapporter.
    Returnerar (Rev CAGR, EPS CAGR) i decimal (0.12 = 12%).
    """
    rev_cagr, eps_cagr = None, None
    try:
        ae = y.earnings  # annual earnings: kolumner ofta ["Revenue","Earnings"]
        if isinstance(ae, pd.DataFrame) and not ae.empty:
            # Revenue CAGR
            if "Revenue" in ae and ae["Revenue"].dropna().shape[0] >= 2:
                vals = pd.to_numeric(ae["Revenue"].dropna(), errors="coerce").dropna()
                if vals.shape[0] >= 2:
                    start = _f(vals.iloc[min(4, len(vals)-1)]) if len(vals) >= 5 else _f(vals.iloc[-1])
                    end   = _f(vals.iloc[0])
                    yrs = 5 if len(vals) >= 5 else (len(vals)-1)
                    if start and end and yrs and yrs > 0:
                        rev_cagr = _cagr(start, end, yrs)

        # EPS CAGR kräver per-aktie data – ofta saknas i yfinance.
        # Lämnas None för att manuell kolumn/extern källa kan fylla.
        eps_cagr = None
    except Exception:
        pass
    return rev_cagr, eps_cagr

# =============== Publik snapshot-funktion ===============
def yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Returnerar dict med fält som matchar våra DATA_COLUMNS där det är möjligt.
    Sätter endast nycklar där vi har värde; andra uteblir (så att vi inte skriver över manuellt).
    """
    out: Dict[str, Any] = {}
    try:
        if not isinstance(ticker, str) or ticker.strip() == "":
            return out
        tkr = ticker.strip().upper()
        y = yf.Ticker(tkr)

        # Pris + valuta
        last_px, ccy = _yf_last_price_and_ccy(tkr)
        if last_px is not None:
            out["Aktuell kurs"] = last_px
        if ccy:
            out["Valuta"] = ccy

        # Utestående aktier & Net debt
        shares, netdebt = _yf_shares_and_netdebt(y)
        if shares is not None:
            out["Utestående aktier"] = shares
        if netdebt is not None:
            out["Net debt"] = netdebt

        # TTM-block
        rev_ttm, ebitda_ttm, eps_ttm = _yf_ttm_blocks(y)
        if rev_ttm is not None:
            out["Rev TTM"] = rev_ttm
        if ebitda_ttm is not None:
            out["EBITDA TTM"] = ebitda_ttm
        if eps_ttm is not None:
            out["EPS TTM"] = eps_ttm

        # 5-års CAGR (Revenue/EPS)
        rev_cagr, eps_cagr = _yf_cagr_5y(y)
        if rev_cagr is not None:
            out["Rev CAGR"] = float(rev_cagr)
        if eps_cagr is not None:
            out["EPS CAGR"] = float(eps_cagr)

        # Utdelningsinfo (gissning utifrån historik)
        nxt_dt, next_amt, freq = _yf_next_dividend_info(y)
        if nxt_dt:
            out["Nästa utdelningsdatum"] = nxt_dt
        if next_amt is not None:
            out["Nästa utdelning (per aktie)"] = next_amt
        if freq:
            out["Utdelningsfrekvens"] = freq

        # Stämpla källa/tid
        out["Senast auto uppdaterad"] = now_stamp()
        out["Auto källa"] = "Yahoo Finance"
    except Exception as e:
        # ytlig logg i UI – låt anroparen välja att visa/ignorera
        st.info(f"Notis (Yahoo): {ticker} – {e}")
    return out

# =============== Merge/skriv-helpers ===============
def _merge_fields_into_row(df: pd.DataFrame, idx: int, fields: Dict[str, Any]) -> pd.DataFrame:
    """
    Skriv endast fält med icke-None värden in i df-raden idx.
    Returnerar uppdaterad DataFrame (kopierad i slice, in-place assignment).
    """
    if df is None or df.empty or not isinstance(fields, dict) or idx < 0 or idx >= len(df):
        return df
    for k, v in fields.items():
        if v is None:
            continue
        if k not in df.columns:
            # skapa kolumn om saknas (borde inte hända då schema säkrat i Del 1)
            df[k] = np.nan
        # Datumfält hanteras som date/str beroende på kolumn
        if k == "Nästa utdelningsdatum":
            if isinstance(v, dt.date):
                df.at[idx, k] = v
            else:
                # försök parsa
                try:
                    vv = pd.to_datetime(v, errors="coerce")
                    df.at[idx, k] = vv.date() if pd.notna(vv) else None
                except Exception:
                    df.at[idx, k] = None
        else:
            df.at[idx, k] = v
    return df

def update_single_row_from_yahoo(df: pd.DataFrame, idx: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Hämtar snapshot för radens ticker och skriver endast över fält vi fick värde för.
    Returnerar (df, fields) där 'fields' är vad som uppdaterades.
    """
    if df is None or df.empty or idx < 0 or idx >= len(df):
        return df, {}
    tkr = str(df.at[idx, "Ticker"]).strip()
    if not tkr:
        return df, {}
    snap = yahoo_snapshot(tkr)
    if not snap:
        return df, {}
    df = _merge_fields_into_row(df.copy(), idx, snap)
    return df, snap

def update_many_from_yahoo(df: pd.DataFrame, indices: List[int], delay_sec: float = 1.0) -> pd.DataFrame:
    """
    Massuppdatering: loopar igenom index, uppdaterar en i taget, valfri fördröjning mellan (1s default).
    Skriver endast över fält som kunde hämtas.
    (UI/knapp finns i senare delar – här är bara logik.)
    """
    if df is None or df.empty or not indices:
        return df
    prog = st.progress(0, text="Startar Yahoo-uppdatering…")
    total = len(indices)
    out_df = df.copy()
    for i, idx in enumerate(indices, start=1):
        try:
            out_df, _ = update_single_row_from_yahoo(out_df, idx)
        except Exception as e:
            st.warning(f"Misslyckades för rad {idx}: {e}")
        prog.progress(min(i/total, 1.0), text=f"Uppdaterar bolag {i} av {total}")
        if delay_sec and delay_sec > 0:
            time.sleep(delay_sec)
    prog.empty()
    return out_df

# ============================================================
# Del 3/6 — Beräkningsmotor
#  • Kompatibel wrapper: fetch_from_yahoo() (bygger på yahoo_snapshot)
#  • EPS-estimat från Yahoo (earnings_trend)
#  • Metodpriser: PE, EV/S, EV/EBITDA, DACF, P/B (+ placeholders)
#  • Multipel-decay & PE-ankare
#  • ✅ Fair Value (median över metodfamiljer, filtrerar kurs-kopia)
#  • compute_methods_for_row() → används av Analys/Ranking
# ============================================================

@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """Wrapper som mappar yahoo_snapshot() till nycklar som resten av appen förväntar sig."""
    snap = yahoo_snapshot(ticker)
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
REV_CAGR_MIN = -0.10
REV_CAGR_MAX =  0.35
EPS_CAGR_MIN = -0.20
EPS_CAGR_MAX =  0.35

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
    e = _pos(ev_target); s = _pos(shares_fd)
    if e is None or s is None:
        return None
    nd = _nz(net_debt, 0.0)
    try:
        return max(0.0, (e - nd) / s)
    except Exception:
        return None

def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e = _pos(eps); p = _pos(pe)
    if e is None or p is None:
        return None
    return e * p

def _ev_from_sales(rev: Optional[float], mult: Optional[float]) -> Optional[float]:
    r = _pos(rev); m = _pos(mult)
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
    p = _pos(pb); b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b

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

def _compute_fair_value_row_v2(methods_df: pd.DataFrame, now_price: Optional[float]) -> Dict[str, Any]:
    """
    Median över *oberoende metodfamiljer*:
      • 'pe_hist_vs_eps'  → fam 'pe'
      • 'ev_sales'        → fam 'ev_s'
      • 'ev_ebitda','ev_dacf' → fam 'ev_e' (räknas EN gång)
      • 'p_b'             → fam 'pb'
    I 'Idag' filtreras värden ≈ kurs (±0.5%) för att undvika tautologi.
    """
    fam_map = {"pe_hist_vs_eps": "pe", "ev_sales": "ev_s", "ev_ebitda": "ev_e", "ev_dacf": "ev_e", "p_b": "pb"}
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
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:
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

def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Returnerar:
      • methods_df: DataFrame [Metod, Idag, 1 år, 2 år, 3 år]
      • sanity    : text
      • meta      : hjälpfält + fair_value (v2)
    Target alltid i bolagets handelsvaluta.
    """
    ticker = str(row.get("Ticker", "")).strip()
    y = fetch_from_yahoo(ticker)
    est = _fetch_eps_estimates_yahoo(ticker)

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

    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), y.get("rev_cagr_hist")))
    rev_cagr_hist     = max(REV_CAGR_MIN, min(REV_CAGR_MAX, rev_cagr_hist_raw)) if rev_cagr_hist_raw is not None else None

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), y.get("eps_cagr_hist")))
    eps_cagr_hist     = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_hist_raw)) if eps_cagr_hist_raw is not None else None

    eps_cagr_long = _f(est.get("eps_cagr_long"))
    if eps_cagr_long is not None:
        eps_cagr_long = max(EPS_CAGR_MIN, min(EPS_CAGR_MAX, eps_cagr_long))

    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    r0 = _pos(rev_ttm)
    if r0 is None:
        r1 = r2 = r3 = None
    else:
        g = float(_nz(rev_cagr_hist, 0.0))
        r1 = r0 * (1.0 + g)
        r2 = r1 * (1.0 + g)
        r3 = r2 * (1.0 + g)

    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr_hist, eps_cagr_long, rev_cagr_hist)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales,  _decay_multiple(ev_sales,  1, decay), _decay_multiple(ev_sales,  2, decay), _decay_multiple(ev_sales,  3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,       _decay_multiple(p_b,       1, decay), _decay_multiple(p_b,       2, decay), _decay_multiple(p_b,       3, decay)

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
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    fv_row = _compute_fair_value_row_v2(methods_df, price)
    methods_df = pd.concat([pd.DataFrame([fv_row]), methods_df], ignore_index=True)

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
# Del 4/6 — Analys & Köpförslag (buggfix)
#  • Hjälpfunktioner för säkra mask-filter (boolserier)
#  • _position_value_tables(df, ...)  ← CHANGED: robust mask-hantering
#  • (Övriga anropspunkter och struktur bibehållna)
# ============================================================

import pandas as pd
import numpy as np
import streamlit as st

# ------------------------------------------------------------
# Säkra mask-hjälpare (för att undvika "truth value is ambiguous")
# ------------------------------------------------------------

def _as_bool_series(df: pd.DataFrame, s, default: bool = False) -> pd.Series:
    """
    Normaliserar till en boolsk Series, index-justerad mot df.
    All NaN → default (vanligen False).
    """
    if isinstance(s, pd.Series):
        out = s.reindex(df.index)
    else:
        # broadcast av skalar/list till Series
        out = pd.Series(s, index=df.index)
    # Pandas 'boolean' dtypen tillåter NA; fyll bort dem
    if out.dtype != "boolean":
        try:
            out = out.astype("boolean")
        except Exception:
            out = out.astype("object")
            out = out.apply(lambda x: bool(x) if pd.notna(x) else default).astype("boolean")
    return out.fillna(default).astype(bool)


def _mask_and(df: pd.DataFrame, *parts) -> pd.Series:
    """
    Vektoriserad OCH-mask. Alla delar normaliseras till bool-Serier.
    """
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)
    m = pd.Series(True, index=df.index, dtype=bool)
    for p in parts:
        if p is None:
            continue
        m = m & _as_bool_series(df, p, default=False)
    return m


def _mask_or(df: pd.DataFrame, *parts) -> pd.Series:
    """
    Vektoriserad ELLER-mask. Alla delar normaliseras till bool-Serier.
    """
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)
    m = pd.Series(False, index=df.index, dtype=bool)
    for p in parts:
        if p is None:
            continue
        m = m | _as_bool_series(df, p, default=False)
    return m


# ------------------------------------------------------------
# Tabellurval till Analys/Köpförslag
# ------------------------------------------------------------

def _owned_mask(df: pd.DataFrame) -> pd.Series:
    """
    Bedömer om en rad 'ägs':
      • primärt: 'Antal aktier' > 0
      • alternativt: kolumnen 'Äger' == True (om finns)
    """
    got_shares = df["Antal aktier"].fillna(0) > 0 if "Antal aktier" in df.columns else None
    got_flag   = df["Äger"].fillna(False) if "Äger" in df.columns else None
    if got_shares is None and got_flag is None:
        return pd.Series(False, index=df.index, dtype=bool)
    if got_shares is None:
        return _as_bool_series(df, got_flag, default=False)
    if got_flag is None:
        return _as_bool_series(df, got_shares, default=False)
    return _mask_or(df, got_shares, got_flag)


def _bucket_mask(df: pd.DataFrame, bucket: str | None) -> pd.Series:
    if not bucket:
        return pd.Series(True, index=df.index, dtype=bool)
    if "Bucket" not in df.columns:
        return pd.Series(True, index=df.index, dtype=bool)
    return (df["Bucket"].astype(str).str.strip().str.lower()
            == str(bucket).strip().lower()).fillna(False)


def _nonempty_ticker_mask(df: pd.DataFrame) -> pd.Series:
    col = "Ticker" if "Ticker" in df.columns else None
    if col is None:
        return pd.Series(True, index=df.index, dtype=bool)
    return df[col].astype(str).str.len().gt(0).fillna(False)


# CHANGED: robust mask-hantering (paranteser, &/|, fillna, helper-funktioner)
def _position_value_tables(
    df: pd.DataFrame,
    *,
    bucket: str | None = None,
    only_owned: bool = False,
    sort_by: str | None = None,
    ascending: bool = False,
    limit: int | None = None
) -> pd.DataFrame:
    """
    Returnerar en vy av positioner för Analys/Köpförslag.
    • Buggfix: all mask-hantering är vektoriserad och fri från 'and/or'-fel.
    • Rör inte indata; returnerar en ny DataFrame.

    Parametrar:
      bucket      – filtrera på viss bucket (ex 'A tillväxt'), None = alla
      only_owned  – visa endast innehav som ägs
      sort_by     – kolumn att sortera på (om den finns)
      ascending   – sorteringsordning
      limit       – max rader att returnera (None = alla)
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    # Grundmasker
    m_valid   = _nonempty_ticker_mask(df)
    m_bucket  = _bucket_mask(df, bucket)
    m_owned   = _owned_mask(df) if only_owned else pd.Series(True, index=df.index, dtype=bool)

    # Slutlig mask — vektoriserat OCH mellan alla filter
    mask = _mask_and(df, m_valid, m_bucket, m_owned)

    view = df.loc[mask].copy()

    # Sortering (om kolumnen finns och är sorteringsbar)
    if sort_by and sort_by in view.columns:
        try:
            # Pröva numerisk sort först
            view["_sort_key_"] = pd.to_numeric(view[sort_by], errors="coerce")
            if view["_sort_key_"].notna().any():
                view = view.sort_values(by=["_sort_key_"], ascending=ascending, kind="mergesort")
                view = view.drop(columns=["_sort_key_"])
            else:
                # fallback till strängsort
                view = view.sort_values(by=[sort_by], ascending=ascending, kind="mergesort")
        except Exception:
            # ingen sort om något går snett
            pass

    if isinstance(limit, int) and limit > 0:
        view = view.head(limit)

    # Liten etikett för antal matchningar (kan visas i UI)
    view.attrs["match_count"] = int(mask.sum())
    view.attrs["total_count"] = int(len(df))
    return view


# ------------------------------------------------------------
# (Hookar för vyer – bibehållna namn/anrop; innehållet här ändras ej)
# ------------------------------------------------------------

def render_analys_view(df: pd.DataFrame, settings: dict, fx_map: dict) -> None:
    """
    Analysvy (oförändrad i sak). Använder _position_value_tables för tabellurval.
    """
    st.header("Analys")
    col1, col2, col3, col4 = st.columns([2,2,2,2])
    with col1:
        bucket = st.selectbox("Bucket", options=["(alla)"] + sorted(df.get("Bucket", pd.Series(dtype=str)).dropna().unique().tolist()))
        bucket = None if bucket == "(alla)" else bucket
    with col2:
        only_owned = st.checkbox("Visa endast innehav jag äger", value=False)
    with col3:
        sort_by = st.selectbox("Sortera på", options=["(ingen)"] + list(df.columns))
        sort_by = None if sort_by == "(ingen)" else sort_by
    with col4:
        ascending = st.checkbox("Stigande?", value=False)

    view = _position_value_tables(
        df,
        bucket=bucket,
        only_owned=only_owned,
        sort_by=sort_by,
        ascending=ascending,
        limit=None
    )

    cnt = view.attrs.get("match_count", len(view))
    tot = view.attrs.get("total_count", len(df))
    st.caption(f"Visar {cnt} av {tot} rader.")

    st.dataframe(view, use_container_width=True, hide_index=True)


def render_kopforslag_view(df: pd.DataFrame, settings: dict, fx_map: dict) -> None:
    """
    Köpförslag (oförändrat i logik här – förutom att det nu hämtar rader via
    _position_value_tables med robust mask).
    """
    st.header("Investeringsförslag")
    col1, col2 = st.columns([2,2])
    with col1:
        bucket = st.selectbox("Bucket", options=["(alla)"] + sorted(df.get("Bucket", pd.Series(dtype=str)).dropna().unique().tolist()))
        bucket = None if bucket == "(alla)" else bucket
    with col2:
        only_owned = st.checkbox("Visa endast ägda", value=False)

    # Sortera efter högst uppsida om kolumnen finns, annars ingen sort
    sort_col = "Uppsida (%)" if "Uppsida (%)" in df.columns else None

    view = _position_value_tables(
        df,
        bucket=bucket,
        only_owned=only_owned,
        sort_by=sort_col,
        ascending=False,
        limit=None
    )

    cnt = view.attrs.get("match_count", len(view))
    tot = view.attrs.get("total_count", len(df))
    st.caption(f"Visar {cnt} av {tot} rader.")

    # Presentationslista
    if view.empty:
        st.info("Inga bolag matchade filtret.")
        return

    for _, r in view.iterrows():
        with st.expander(f"{r.get('Ticker','?')} — {r.get('Bolagsnamn','')}"):
            # Här förlitar vi oss på att Del 3/6 finns: compute_methods_for_row
            try:
                methods_df, sanity, meta = compute_methods_for_row(r, settings, fx_map)
                cols = st.columns([3,2])
                with cols[0]:
                    st.markdown("**Riktkurser (lokal valuta)**")
                    st.dataframe(methods_df, use_container_width=True, hide_index=True)
                with cols[1]:
                    st.markdown("**Sammanfattning**")
                    st.write(f"Valuta: {meta.get('currency')}")
                    st.write(f"Aktuell kurs: {meta.get('price')}")
                    fv = meta.get("fair_value", {})
                    st.write(f"Fair value idag: {fv.get('today')}")
                    st.write(f"Fair value 1 år: {fv.get('y1')}")
                    st.write(f"Fair value 2 år: {fv.get('y2')}")
                    st.write(f"Fair value 3 år: {fv.get('y3')}")
                    st.caption(sanity)
            except Exception as ex:
                st.warning(f"Kunde inte beräkna riktkurser för {r.get('Ticker')}: {ex}")

# ============================================================
# Del 5/6 — Vyer
#  • Settings, Snapshot, Editor, Lägg till
#  • Portfölj (Bucket-kolumn + expanders per Bucket + utdelningar)
#  • Analys (metoder + Fair Value)
#  • Ranking (uppsida)
#  • Batch (massuppdatering)
#  • 🛒 Köpförslag
#  • CHANGED (minimalt):
#      – Editor: Bucket = selectbox (rullista)
#      – Lägg till: Bucket = selectbox (rullista)
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
    st.dataframe(s_df, use_container_width=True)

    if st.button("💾 Spara Settings"):
        try:
            _write_df(SETTINGS_TITLE, s_df[SETTINGS_COLUMNS])
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
# Editor (manuellt + Yahoo)
# ============================================================
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
    y   = fetch_from_yahoo(ticker)  # Del 3 wrapper
    est = _fetch_eps_estimates_yahoo(ticker)
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

    # --- CHANGED: Bucket → selectbox (rullista) ---
    bucket_options = [""] + DEFAULT_BUCKETS
    current_bucket = str(row.get("Bucket") or "")
    try:
        bucket_index = bucket_options.index(current_bucket) if current_bucket in bucket_options else 0
    except Exception:
        bucket_index = 0

    c1, c2 = st.columns(2)
    with c1:
        new_ticker = st.text_input("Ticker", value=str(row.get("Ticker") or "").upper())
        antal_in   = st.text_input("Antal aktier", value=str(_f(row.get("Antal aktier")) or ""))
        gav_in     = st.text_input("GAV (SEK)", value=str(_f(row.get("GAV (SEK)")) or ""))
        bucket_in  = st.selectbox("Bucket", options=bucket_options, index=bucket_index)  # CHANGED
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
                # CHANGED: skriv tillbaka vald bucket (kan vara tom sträng)
                df.loc[idx, "Bucket"] = bucket_in if bucket_in is not None else current_bucket
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
                # CHANGED: se till att aktuell bucket skrivs innan write
                df_cur.at[idx, "Bucket"] = bucket_in if bucket_in is not None else current_bucket

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
# Lägg till ticker
# ============================================================
def page_add_ticker():
    st.header("➕ Lägg till ticker")

    tkr = st.text_input("Ticker").upper().strip()
    c1, c2, c3 = st.columns(3)
    with c1:
        bolagsnamn = st.text_input("Bolagsnamn")
        sektor     = st.text_input("Sektor")
    with c2:
        # CHANGED: Bucket → selectbox (rullista)
        bucket = st.selectbox("Bucket", options=[""] + DEFAULT_BUCKETS, index=0)
        valuta = st.text_input("Valuta (t.ex. USD)", value="USD").upper()
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
                "Bucket": bucket if bucket else np.nan,   # CHANGED: rullist-val
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
# Portfölj (innehav + Bucket + utdelningar)
# ============================================================
def page_portfolio():
    st.header("📦 Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx_map = st.session_state.get("FX", {}) or get_fx_map()
    settings = get_settings_map()

    # Innehavsvärden med Bucket-kolumn
    pos = _position_value_tables(df, fx_map)
    if pos.empty:
        st.info("Inga innehav (Antal aktier <= 0).")
    else:
        tot_sek = float(pos["Värde (SEK)"].sum())
        st.metric("Totalt portföljvärde (SEK)", f"{tot_sek:,.0f}".replace(",", " "))
        # Visa bucket-kolumn direkt
        st.dataframe(pos.sort_values(["Bucket","Värde (SEK)"]), use_container_width=True, hide_index=True)

        st.markdown("#### Hinkar (Bucket) – innehåll")
        # Expanders per bucket (innehåll + cap-info)
        render_bucket_expandables(pos, settings)

    st.markdown("---")
    # Kommande utdelningar
    render_portfolio_dividends_section(df, fx_map, settings)

# ============================================================
# Analys (metoder + Fair Value)
# ============================================================
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
        return

    row = df.loc[df["Ticker"].astype(str) == tkr]
    if row.empty:
        st.error("Kunde inte hitta vald rad.")
        return
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

    default_m = _pick_primary_method(row, methods_df)
    method = st.selectbox("Primär metod", methods_df["Metod"].tolist(),
                          index=methods_df["Metod"].tolist().index(default_m) if default_m in methods_df["Metod"].tolist() else 0)

    targets = _targets_from_methods(methods_df, method)
    price   = _f(_nz(meta.get("price"), row.get("Aktuell kurs")))

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

# ============================================================
# Ranking
# ============================================================
def page_ranking():
    st.header("🏆 Ranking – Uppsida")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)
    rows = []
    settings = get_settings_map()
    fx_map   = get_fx_map()

    prog = st.progress(0.0)
    total = len(df)
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        try:
            methods_df, _, meta = compute_methods_for_row(r, settings, fx_map)
            meth = _pick_primary_method(r, methods_df)
            tgts = _targets_from_methods(methods_df, meth)
            price = _f(_nz(meta.get("price"), r.get("Aktuell kurs")))
            target = _f(tgts[horizon])
            up = ((target - price) / price * 100.0) if (_pos(target) and _pos(price)) else None

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
# 🛒 Köpförslag – under Fair Value + under cap per innehav
# ============================================================
def _bucket_cap_per_holding(bucket_label: str, settings: Dict[str, str]) -> Optional[float]:
    key = _bucket_cap_key(bucket_label)
    if not key:
        return None
    v = _f(settings.get(key))
    return float(v) if v is not None else None

def _position_value_lookup(df_data: pd.DataFrame, fx_map: Dict[str, float]) -> dict[str, dict]:
    out = {}
    pos = _position_value_tables(df_data, fx_map)
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
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=[
            "Ticker","Bolagsnamn","Bucket","Valuta","Kurs","FV idag",
            "Uppsida (%)","Äger (antal)","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"
        ])

    fx_map = fx_map or get_fx_map()
    pos_lu = _position_value_lookup(df_data, fx_map)
    rows = []

    prog = st.progress(0.0)
    total = len(df_data)
    for i, (_, r) in enumerate(df_data.iterrows(), start=1):
        tkr = str(r.get("Ticker") or "").upper().strip()
        if not tkr:
            continue
        bucket = str(_nz(r.get("Bucket"), "") or "")
        if not bucket:
            prog.progress(i/total if total else 1.0)
            continue

        cap = _bucket_cap_per_holding(bucket, settings)
        if cap is None or cap <= 0:
            prog.progress(i/total if total else 1.0)
            continue

        try:
            methods_df, _, meta = compute_methods_for_row(r, settings, fx_map)
        except Exception:
            prog.progress(i/total if total else 1.0)
            continue

        price = _f(_nz(meta.get("price"), r.get("Aktuell kurs")))
        fv_today = _f((meta.get("fair_value") or {}).get("today"))
        if not _pos(price) or not _pos(fv_today):
            prog.progress(i/total if total else 1.0)
            continue

        if price >= fv_today:
            prog.progress(i/total if total else 1.0)
            continue

        lu = pos_lu.get(tkr, {"value_sek": 0.0, "qty": 0.0, "currency": str(_nz(r.get("Valuta"), "SEK")).upper(), "price": price})
        qty = lu["qty"]
        own_status = "own" if (qty and qty > 0) else "no_own"

        if own_filter == "Endast innehav" and own_status != "own":
            prog.progress(i/total if total else 1.0)
            continue
        if own_filter == "Endast ej ägda" and own_status != "no_own":
            prog.progress(i/total if total else 1.0)
            continue

        value_sek = lu["value_sek"]
        if _pos(value_sek) and value_sek >= cap:
            prog.progress(i/total if total else 1.0)
            continue

        up_pct = (fv_today - price) / price * 100.0 if _pos(price) else None
        rows.append({
            "Ticker": tkr,
            "Bolagsnamn": str(_nz(r.get("Bolagsnamn"), "")),
            "Bucket": bucket,
            "Valuta": str(_nz(meta.get("currency"), r.get("Valuta") or "USD")).upper(),
            "Kurs": price,
            "FV idag": fv_today,
            "Uppsida (%)": up_pct,
            "Äger (antal)": qty,
            "Värde (SEK)": value_sek or 0.0,
            "Cap per innehav (SEK)": cap,
            "Slack till cap (SEK)": (cap - (value_sek or 0.0)),
        })
        prog.progress(i/total if total else 1.0)

    prog.empty()
    if not rows:
        return pd.DataFrame(columns=[
            "Ticker","Bolagsnamn","Bucket","Valuta","Kurs","FV idag",
            "Uppsida (%)","Äger (antal)","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"
        ])

    out = pd.DataFrame(rows)
    out = out.sort_values(["Värde (SEK)", "Uppsida (%)"], ascending=[True, False]).reset_index(drop=True)
    return out

def page_buy_suggestions():
    st.header("🛒 Köpförslag (under FV + under cap per innehav)")
    df = st.session_state.get("DATA") or read_data_df()
    if df is None or df.empty:
        st.info("Ingen data.")
        return
    settings = get_settings_map()
    fx_map   = get_fx_map()

    filt = st.radio("Visa", ["Alla","Endast innehav","Endast ej ägda"], index=0, horizontal=True)
    with st.spinner("Beräknar köpförslag…"):
        sug = build_buy_suggestions(df, settings, fx_map, own_filter=filt)

    if sug.empty:
        st.info("Inga kandidater uppfyller kriterierna just nu.")
        st.caption("Kriterier: Kurs < Fair Value (idag) och innehavsvärde < cap per innehav i sin bucket.")
        return

    st.caption(f"{len(sug)} förslag — sorterat minsta innehavet först.")
    st.dataframe(sug, use_container_width=True, hide_index=True)

    with st.expander("Summering per Bucket (antal förslag)"):
        agg = sug.groupby("Bucket", as_index=False).size().rename(columns={"size":"Antal förslag"})
        st.dataframe(agg, use_container_width=True, hide_index=True)

# ============================================================
# Del 6/6 — Main & routing
#  • Sidebar-navigering
#  • Init av session (DATA, FX, Settings-cache)
#  • Minimal hantering för omladdning
#  • Ingen annan befintlig logik ändrad
# ============================================================

# Fallback endast om saknas (ofarligt för basen)
if "now_stamp" not in globals():
    def now_stamp():
        return _now()

def _ensure_session_loaded():
    """Ladda DATA/FX/Settings till sessionens cache om de saknas."""
    try:
        if "DATA" not in st.session_state or st.session_state.get("DATA") is None:
            st.session_state["DATA"] = read_data_df()
    except Exception as e:
        st.error(f"Kunde inte läsa DATA: {e}")
        st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)

    try:
        if "FX" not in st.session_state or not st.session_state.get("FX"):
            st.session_state["FX"] = get_fx_map()
    except Exception as e:
        st.warning(f"Kunde inte läsa Valutakurser: {e}")
        st.session_state["FX"] = {}

    # Settings map kan hämtas on-demand via get_settings_map()

def _sidebar_menu():
    st.sidebar.markdown("### Meny")
    items = [
        ("📦 Portfölj", page_portfolio),
        ("🔬 Analys", page_analysis),
        ("🏆 Ranking", page_ranking),
        ("🛒 Köpförslag", page_buy_suggestions),
        ("✏️ Editor", page_editor),
        ("➕ Lägg till ticker", page_add_ticker),
        ("🧩 Massuppdatering", page_batch),
        ("⚙️ Settings", page_settings),
        ("🕒 Snapshot", page_snapshot),
    ]
    labels = [lbl for (lbl, _) in items]
    default_idx = 0
    key = "nav_choice"

    # Behåll senaste val
    if key not in st.session_state:
        st.session_state[key] = labels[default_idx]

    choice = st.sidebar.radio("",
                              labels,
                              index=labels.index(st.session_state[key]) if st.session_state[key] in labels else default_idx)
    st.session_state[key] = choice

    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Läs om DATA från Google Sheets"):
        try:
            st.session_state["DATA"] = read_data_df()
            st.success("DATA omläst.")
        except Exception as e:
            st.error(f"Misslyckades läsa DATA: {e}")

    if st.sidebar.button("🔁 Läs om Valutakurser"):
        try:
            st.session_state["FX"] = get_fx_map()
            st.success("Valutakurser omlästa.")
        except Exception as e:
            st.warning(f"Kunde inte läsa FX: {e}")

    return dict(items)[choice]

def main():
    _ensure_session_loaded()
    render = _sidebar_menu()
    # Kör vald sida
    render()

if __name__ == "__main__":
    main()
