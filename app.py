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
# ============================================================

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor (+ fallback)
#  • 5-års historisk CAGR (Revenue & EPS) med clamp
#  • Utdelning: årlig DPS (forward/senaste 12m), frekvens, nästa datum/belopp (est.)
#  • Wrapper-funktioner som Editor förväntar sig
# ============================================================

# -----------------------------
# Hjälpare för yfinance
# -----------------------------
def _yf_ticker(ticker: str) -> yf.Ticker:
    return yf.Ticker(str(ticker).strip())

def _safe_last(series: pd.Series) -> Optional[float]:
    if series is None or len(series) == 0:
        return None
    try:
        v = float(series.dropna().iloc[-1])
        return v if math.isfinite(v) else None
    except Exception:
        return None

def _infer_currency_from_fastinfo(fi) -> Optional[str]:
    try:
        c = getattr(fi, "currency", None)
        if c:
            return str(c).upper()
    except Exception:
        pass
    return None

def _infer_currency_from_price_df(df: pd.DataFrame) -> Optional[str]:
    # yfinance exponerar sällan valutakod i history-metadatan – lämna None
    return None

# -----------------------------
# TTM från kvartal (+ fallback)
# -----------------------------
def _sum_last_four(q: pd.Series) -> Optional[float]:
    try:
        q = pd.to_numeric(q, errors="coerce").dropna()
        if q.shape[0] == 0:
            return None
        return float(q.iloc[-4:].sum())
    except Exception:
        return None

def _ttm_from_quarterly(fin: yf.Ticker) -> Dict[str, Optional[float]]:
    """
    Hämtar quarterly_financials och quarterly_earnings (per-share) för att approximera TTM:
      - Rev TTM
      - EBITDA TTM
      - EPS TTM (via quarterly_earnings["Earnings"])
    Fallback: använder årsdata (senaste året) om kvartal saknas.
    """
    rev_ttm, ebitda_ttm, eps_ttm = None, None, None

    # Revenue & EBITDA via quarterly_financials
    try:
        qfin = fin.quarterly_financials
        if isinstance(qfin, pd.DataFrame) and not qfin.empty:
            qfin = qfin.copy()
            qfin.index = [str(x) for x in qfin.index]
            if "Total Revenue" in qfin.index:
                rev_ttm = _sum_last_four(qfin.loc["Total Revenue"])
            if "EBITDA" in qfin.index:
                ebitda_ttm = _sum_last_four(qfin.loc["EBITDA"])
    except Exception:
        pass

    # EPS via quarterly_earnings (per-share)
    try:
        qearn = fin.quarterly_earnings
        if isinstance(qearn, pd.DataFrame) and not qearn.empty and "Earnings" in qearn.columns:
            eps_ttm = _sum_last_four(qearn["Earnings"])
    except Exception:
        pass

    # Fallback från årsdata om något saknas
    try:
        if rev_ttm is None:
            yfin = fin.financials
            if isinstance(yfin, pd.DataFrame) and not yfin.empty and "Total Revenue" in yfin.index:
                rev = pd.to_numeric(yfin.loc["Total Revenue"], errors="coerce").dropna()
                if not rev.empty:
                    rev_ttm = float(rev.iloc[-1])
        if ebitda_ttm is None:
            yfin = fin.financials
            if isinstance(yfin, pd.DataFrame) and not yfin.empty and "EBITDA" in yfin.index:
                e = pd.to_numeric(yfin.loc["EBITDA"], errors="coerce").dropna()
                if not e.empty:
                    ebitda_ttm = float(e.iloc[-1])
        if eps_ttm is None:
            yearn = fin.earnings
            if isinstance(yearn, pd.DataFrame) and not yearn.empty and "Earnings" in yearn.columns:
                e = pd.to_numeric(yearn["Earnings"], errors="coerce").dropna()
                if not e.empty:
                    eps_ttm = float(e.iloc[-1])
    except Exception:
        pass

    return {
        "rev_ttm": rev_ttm,
        "ebitda_ttm": ebitda_ttm,
        "eps_ttm": eps_ttm,
    }

# -----------------------------
# 5-års historisk CAGR (Revenue & EPS)
# -----------------------------
def _cagr(start: float, end: float, years: float) -> Optional[float]:
    try:
        if start is None or end is None or start <= 0 or years <= 0:
            return None
        return (end / start) ** (1.0 / years) - 1.0
    except Exception:
        return None

def _hist_cagr_5y(fin: yf.Ticker) -> Dict[str, Optional[float]]:
    """
    Beräkna 5Y CAGR från årsdata:
      - Revenue från .financials (år)
      - EPS från .earnings (per-share, kolumn 'Earnings')
    """
    rev_cagr, eps_cagr = None, None

    # Revenue (årsdata)
    try:
        yfin = fin.financials
        if isinstance(yfin, pd.DataFrame) and not yfin.empty and "Total Revenue" in yfin.index:
            rev = pd.to_numeric(yfin.loc["Total Revenue"], errors="coerce").dropna()
            if rev.shape[0] >= 5:
                start = float(rev.iloc[-5])
                end   = float(rev.iloc[-1])
                rev_cagr = _cagr(start, end, 4)  # 5 år ≈ 4 steg
    except Exception:
        pass

    # EPS (årsdata, per-share)
    try:
        yearn = fin.earnings
        if isinstance(yearn, pd.DataFrame) and not yearn.empty and "Earnings" in yearn.columns:
            eps = pd.to_numeric(yearn["Earnings"], errors="coerce").dropna()
            if eps.shape[0] >= 5:
                start = float(eps.iloc[-5])
                end   = float(eps.iloc[-1])
                if start > 0 and end > 0:
                    eps_cagr = _cagr(start, end, 4)
    except Exception:
        pass

    return {"rev_cagr_5y": rev_cagr, "eps_cagr_5y": eps_cagr}

def _clamp(x: Optional[float], lo: float, hi: float) -> Optional[float]:
    if x is None or not math.isfinite(x):
        return None
    return max(lo, min(hi, x))

# -----------------------------
# Utdelningshjälp
# -----------------------------
def _infer_dividend_metrics(t: yf.Ticker) -> Dict[str, Any]:
    """
    Försöker härleda:
      • dps_annual  – årlig utdelning per aktie (helår, forward om tillgängligt)
      • div_freq    – 'M','Q','S','A' (månad/kvartal/halvår/år) utifrån 12–15 mån historik
      • next_date   – nästa beräknade utbetalningsdatum (estimerat från historiken)
      • next_amount – nästa beräknade belopp (≈ senaste beloppet)
    """
    dps_annual, div_freq, next_date, next_amount = None, None, None, None

    # 1) Försök med forward rate från info (bäst för totalsumman)
    info = {}
    try:
        info = t.info or {}
    except Exception:
        info = {}

    # forward (dividendRate) eller trailingAnnualDividendRate
    for key in ("dividendRate", "trailingAnnualDividendRate"):
        val = _f(info.get(key))
        if val is not None and val > 0:
            dps_annual = float(val)
            break

    # 2) Historik – summera sista 12 månaderna om forward saknas
    div = None
    try:
        # yfinance: .dividends är en Series (datumindex) — oftast ex-datum
        div = t.dividends
    except Exception:
        div = None

    if div is not None and hasattr(div, "index") and div.shape[0] > 0:
        try:
            s = pd.to_numeric(div, errors="coerce").dropna()
            if not s.empty:
                # frekvens via antal utbetalningar senaste 400 dagarna
                cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=400)
                last_400 = s[s.index >= cutoff]
                cnt = int(last_400.shape[0])

                if cnt >= 11:
                    div_freq = "M"
                elif 3 <= cnt <= 5:
                    div_freq = "Q"
                elif cnt == 2:
                    div_freq = "S"
                elif cnt == 1:
                    div_freq = "A"

                # årlig DPS – preferera *sista 12 mån* (robust för höjningar/sänkningar)
                cutoff12 = pd.Timestamp.utcnow() - pd.Timedelta(days=365)
                last_12m = s[s.index >= cutoff12]
                if last_12m.shape[0] > 0 and dps_annual is None:
                    dps_annual = float(last_12m.sum())

                # Om inget 12m-summa hittades, härled via senaste belopp * frekvens
                last_amt = float(s.iloc[-1]) if s.shape[0] else None
                if dps_annual is None and last_amt is not None and div_freq:
                    mult = {"M":12, "Q":4, "S":2, "A":1}.get(div_freq, None)
                    if mult:
                        dps_annual = last_amt * mult

                # Nästa ”betalningsdatum” (estimat) – addera period till senaste datum
                if last_amt is not None and div_freq:
                    last_dt = pd.Timestamp(s.index[-1]).tz_localize(None)
                    if div_freq == "M":
                        next_date = (last_dt + pd.DateOffset(months=1)).date()
                    elif div_freq == "Q":
                        next_date = (last_dt + pd.DateOffset(months=3)).date()
                    elif div_freq == "S":
                        next_date = (last_dt + pd.DateOffset(months=6)).date()
                    else:
                        next_date = (last_dt + pd.DateOffset(years=1)).date()
                    next_amount = last_amt
        except Exception:
            pass

    # 3) Sista fallback – använd lastDividendValue som proxy för ”senaste belopp”
    if next_amount is None:
        try:
            last_val = _f(info.get("lastDividendValue"))
            if last_val is not None:
                next_amount = float(last_val)
        except Exception:
            pass

    return {
        "dps_annual": _f(dps_annual),
        "div_freq": div_freq,
        "next_date": next_date,            # date-objekt eller None
        "next_amount": _f(next_amount),    # per aktie (brutto)
    }

# -----------------------------
# Snabb snapshot (pris, valuta, shares, net debt, multiplar)
# -----------------------------
def _fast_snapshot(ticker: str) -> Dict[str, Optional[float]]:
    t = _yf_ticker(ticker)

    price, currency = None, None
    shares_out, market_cap, enterprise_value, net_debt = None, None, None, None
    pe_ttm, pe_fwd = None, None
    ev_rev, ev_ebitda, pb, bvps = None, None, None, None

    # price + currency
    try:
        fi = t.fast_info
        price = _f(getattr(fi, "last_price", None))
        currency = _infer_currency_from_fastinfo(fi) or getattr(fi, "currency", None)
        if currency:
            currency = str(currency).upper()
    except Exception:
        pass
    if price is None:
        try:
            hist = t.history(period="5d")
            if not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            pass

    # info-baserade fält
    try:
        inf = t.info
    except Exception:
        inf = {}

    # shares / mcap / EV / debt (med fallback)
    try:
        shares_out = _f(inf.get("sharesOutstanding")) or _f(inf.get("floatShares"))
        if shares_out is None and _f(inf.get("marketCap")) and _pos(price):
            shares_out = float(inf["marketCap"]) / float(price)
    except Exception:
        pass
    try:
        market_cap = _f(inf.get("marketCap"))
        if market_cap is None and _pos(price) and _pos(shares_out):
            market_cap = float(price) * float(shares_out)
    except Exception:
        pass
    try:
        enterprise_value = _f(inf.get("enterpriseValue"))
    except Exception:
        enterprise_value = None
    try:
        total_debt = _f(inf.get("totalDebt"))
        cash = _f(inf.get("totalCash"))
        if total_debt is not None and cash is not None:
            net_debt = total_debt - cash
        elif enterprise_value is not None and market_cap is not None:
            net_debt = enterprise_value - market_cap
    except Exception:
        pass

    # Multiplar (om Yahoo exponerar dem)
    pe_ttm = _f(inf.get("trailingPE"))
    pe_fwd = _f(inf.get("forwardPE"))
    ev_rev = _f(inf.get("enterpriseToRevenue"))
    ev_ebitda = _f(inf.get("enterpriseToEbitda"))
    pb = _f(inf.get("priceToBook"))
    bvps = _f(inf.get("bookValue"))

    # Fallback-beräkna multiplar om saknas
    try:
        if ev_rev is None and enterprise_value and _pos(enterprise_value):
            # fylls senare med rev_ttm i fetch_from_yahoo
            ev_rev = None
    except Exception:
        pass
    try:
        if ev_ebitda is None and enterprise_value and _pos(enterprise_value):
            ev_ebitda = None
    except Exception:
        pass
    try:
        if pb is None and _pos(price) and _pos(bvps):
            pb = float(price) / float(bvps)
    except Exception:
        pass

    return {
        "price": price,
        "currency": (str(currency).upper() if currency else None),
        "shares_out": shares_out,
        "market_cap": market_cap,
        "enterprise_value": enterprise_value,
        "net_debt": net_debt,
        "pe_ttm": pe_ttm,
        "pe_fwd": pe_fwd,
        "ev_rev": ev_rev,
        "ev_ebitda": ev_ebitda,
        "p_b": pb,
        "bvps": bvps,
    }

# -----------------------------
# Publik Yahoo-hämtning (används av beräkning)
# -----------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Hämtar ett robust snapshot från Yahoo:
      • Pris + Valuta
      • Shares out, net debt, MCAP/EV
      • TTM (rev/ebitda/eps)
      • Multiplar (PE TTM/FWD, EV/S, EV/EBITDA, P/B, BVPS)
      • 5Y hist. CAGR för rev & eps (clampade intervall)
      • Utdelning (årlig DPS, frekvens, nästa datum/belopp – estimerat)
    """
    t = _yf_ticker(ticker)

    snap = _fast_snapshot(ticker)
    ttm  = _ttm_from_quarterly(t)
    cagr = _hist_cagr_5y(t)
    divm = _infer_dividend_metrics(t)

    # Härled PE TTM om saknas men EPS TTM finns
    pe_ttm = snap.get("pe_ttm")
    if (pe_ttm is None or not math.isfinite(pe_ttm)) and ttm.get("eps_ttm") and snap.get("price"):
        try:
            if ttm["eps_ttm"] > 0:
                pe_ttm = float(snap["price"]) / float(ttm["eps_ttm"])
        except Exception:
            pass

    # Fyll EV/Revenue & EV/EBITDA om saknas och EV/TTM finns
    ev_rev = snap.get("ev_rev")
    ev_ebitda = snap.get("ev_ebitda")
    try:
        if ev_rev is None and _pos(snap.get("enterprise_value")) and _pos(ttm.get("rev_ttm")):
            ev_rev = float(snap["enterprise_value"]) / float(ttm["rev_ttm"])
    except Exception:
        pass
    try:
        if ev_ebitda is None and _pos(snap.get("enterprise_value")) and _pos(ttm.get("ebitda_ttm")):
            ev_ebitda = float(snap["enterprise_value"]) / float(ttm["ebitda_ttm"])
    except Exception:
        pass

    # Clamp enligt praxis
    rev_cagr_hist = _clamp(cagr.get("rev_cagr_5y"), -0.10, 0.35)
    eps_cagr_hist = _clamp(cagr.get("eps_cagr_5y"), -0.20, 0.35)

    out = {
        # Pris & valuta
        "price": snap.get("price"),
        "currency": snap.get("currency") or "USD",

        # Kapitalstruktur
        "shares_out": snap.get("shares_out"),
        "market_cap": snap.get("market_cap"),
        "enterprise_value": snap.get("enterprise_value"),
        "net_debt": snap.get("net_debt"),

        # TTM
        "rev_ttm": ttm.get("rev_ttm"),
        "ebitda_ttm": ttm.get("ebitda_ttm"),
        "eps_ttm": ttm.get("eps_ttm"),

        # Multiplar
        "pe_ttm": pe_ttm,
        "pe_fwd": snap.get("pe_fwd"),
        "ev_rev": ev_rev,
        "ev_ebitda": ev_ebitda,
        "p_b": snap.get("p_b"),
        "bvps": snap.get("bvps"),

        # Utdelning
        "dps_annual": divm.get("dps_annual"),
        "div_freq": divm.get("div_freq"),
        "next_div_date": divm.get("next_date"),
        "next_div_amount": divm.get("next_amount"),

        # Historik (clampad)
        "rev_cagr_hist": rev_cagr_hist,
        "eps_cagr_hist": eps_cagr_hist,
    }
    return out

# -----------------------------
# Hjälpfunktion: merge in i Data-DF
# -----------------------------
def merge_yahoo_into_row(row: pd.Series, y: Dict[str, Any]) -> pd.Series:
    """
    Skriver endast fält som Yahoo faktiskt levererat (None ignoreras).
    Bevarar övriga manuella värden.
    """
    m = row.copy()

    mapping = {
        "Aktuell kurs": "price",
        "Valuta": "currency",
        "Utestående aktier": "shares_out",
        "Net debt": "net_debt",
        "Rev TTM": "rev_ttm",
        "EBITDA TTM": "ebitda_ttm",
        "EPS TTM": "eps_ttm",
        "PE TTM": "pe_ttm",
        "PE FWD": "pe_fwd",
        "EV/Revenue": "ev_rev",
        "EV/EBITDA": "ev_ebitda",
        "P/B": "p_b",
        "BVPS": "bvps",
        "Årlig utdelning": "dps_annual",
        "Utdelningsfrekvens": "div_freq",
        "Nästa utdelningsdatum": "next_div_date",
        "Nästa utdelning (per aktie)": "next_div_amount",
        "Rev CAGR": "rev_cagr_hist",
        "EPS CAGR": "eps_cagr_hist",
    }
    for col, key in mapping.items():
        val = y.get(key, None)
        if val is None:
            continue
        # Datum/sträng/float tillåts — skriv om faktiskt värde finns
        if isinstance(val, (int, float)) and math.isfinite(float(val)):
            m[col] = float(val)
        elif isinstance(val, (dt.date,)):
            m[col] = val
        elif isinstance(val, str) and val.strip() != "":
            m[col] = val

    m["Senast auto uppdaterad"] = now_stamp()
    m["Auto källa"] = "Yahoo"
    return m

# -----------------------------
# WRAPPERS för Editor/Add Ticker (förväntade namn)
# -----------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Returnerar ett "editor-vänligt" snapshot-dict med nycklar:
      company_name, sector, price, currency, shares, net_debt,
      revenue_ttm, ebitda_ttm, eps_ttm, pe_ttm, pe_fwd,
      ev_to_sales, ev_to_ebitda, p_to_book, bvps,
      annual_dividend, dividend_frequency
    """
    t = _yf_ticker(ticker)
    base = fetch_from_yahoo(ticker)

    # Namn & sektor
    try:
        info = t.info
    except Exception:
        info = {}

    return {
        "company_name": info.get("longName") or info.get("shortName"),
        "sector": info.get("sector"),
        "price": base.get("price"),
        "currency": base.get("currency"),
        "shares": base.get("shares_out"),
        "net_debt": base.get("net_debt"),
        "revenue_ttm": base.get("rev_ttm"),
        "ebitda_ttm": base.get("ebitda_ttm"),
        "eps_ttm": base.get("eps_ttm"),
        "pe_ttm": base.get("pe_ttm"),
        "pe_fwd": base.get("pe_fwd"),
        "ev_to_sales": base.get("ev_rev"),
        "ev_to_ebitda": base.get("ev_ebitda"),
        "p_to_book": base.get("p_b"),
        "bvps": base.get("bvps"),
        "annual_dividend": base.get("dps_annual"),
        # Frekvens (M/Q/S/A) om vi kunnat härleda den
        "dividend_frequency": base.get("div_freq"),
    }

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Wrapper som returnerar {'eps_1y','eps_2y','eps_cagr_long'}
    (nyttjar _fetch_eps_estimates_yahoo som definieras i Del 3/6).
    """
    try:
        est = _fetch_eps_estimates_yahoo(ticker)  # definieras i Del 3/6
        return {
            "eps_1y": est.get("eps_1y"),
            "eps_2y": est.get("eps_2y"),
            "eps_cagr_long": est.get("eps_cagr_long"),
        }
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None}

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str) -> Dict[str, Optional[float]]:
    """Wrapper för historisk revenue-CAGR (5Y) → {'rev_cagr': ...}"""
    try:
        fin = _yf_ticker(ticker)
        c = _hist_cagr_5y(fin)
        return {"rev_cagr": c.get("rev_cagr_5y")}
    except Exception:
        return {"rev_cagr": None}

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_cagr_hist(ticker: str) -> Dict[str, Optional[float]]:
    """Wrapper för historisk EPS-CAGR (5Y) → {'eps_cagr': ...}"""
    try:
        fin = _yf_ticker(ticker)
        c = _hist_cagr_5y(fin)
        return {"eps_cagr": c.get("eps_cagr_5y")}
    except Exception:
        return {"eps_cagr": None}

# ============================================================
# Del 3/6 — Prognoser & Fair Value (strikt)
#  • EPS-estimat (Yahoo, robusta fallbacks)
#  • Tillväxtinferens (hist/estimat, clamp)
#  • Fair value-motor (P/E-band för lönsamma, EV/S-band för olönsamma)
#  • Bull/Bear 1 år
#  • Hjälp: compute_methods_for_row() + update_row_with_fair_values()
# ============================================================

# -----------------------------
# EPS-estimat från Yahoo (robust)
# -----------------------------
def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker hämta EPS-estimat 1–2 år fram samt långsiktig EPS-tillväxt.
    Vi provar flera källor i yfinance och faller tillbaka till .info-fält.
    Returnerar dict: {'eps_1y','eps_2y','eps_cagr_long'}
    """
    t = _yf_ticker(ticker)
    eps_1y, eps_2y, eps_cagr_long = None, None, None

    # 1) Försök via "analysis" (vanligt i yfinance 0.2.x)
    try:
        a = t.analysis
        if isinstance(a, pd.DataFrame) and not a.empty:
            # För vissa tickers ligger raden "Earnings Estimate" med kolumner:
            # ['Avg','Low','High','Year Ago EPS','Next Qtr','Current Year','Next Year']
            # Vi försöker hitta 'Earnings Estimate' → 'Avg' för 'Current Year'/'Next Year'
            if "Earnings Estimate" in a.index:
                row = a.loc["Earnings Estimate"].copy()
                # current & next year via kolumner, om de finns
                # yfinance skiftar ibland namn – vi söker pragmatiskt
                def _col_like(cols, key_frag):
                    for c in cols:
                        if key_frag.lower() in str(c).lower():
                            return c
                    return None
                cols = list(a.columns)
                col_cy = _col_like(cols, "Current Year")
                col_ny = _col_like(cols, "Next Year")
                # 'Avg' kan vara radkolumn i annan layout – hantera båda
                try:
                    if isinstance(row, pd.Series) and "Avg" in row.index:
                        # Ibland är 'Avg' en hel rad → då är 'Current Year'/'Next Year' kolumner
                        if col_cy and pd.notna(row[col_cy]):
                            eps_1y = _f(row[col_cy])
                        if col_ny and pd.notna(row[col_ny]):
                            eps_2y = _f(row[col_ny])
                except Exception:
                    pass
    except Exception:
        pass

    # 2) Fallback via "earnings_forecasts" (nyare yfinance API)
    if eps_1y is None or eps_2y is None:
        try:
            ef = getattr(t, "earnings_forecasts", None)
            if ef is not None and isinstance(ef, dict):
                # Typiskt innehåller 'year', 'epsAvg','epsHigh','epsLow'
                # Vi försöker plocka år N och N+1 om tillgängligt
                df = None
                try:
                    df = ef.get("eps", None)
                except Exception:
                    df = None
                if isinstance(df, pd.DataFrame) and not df.empty:
                    df = df.sort_values(by="year")
                    # Ta de två sista raderna som proxy för 1y och 2y fram
                    if df.shape[0] >= 1:
                        eps_1y = _f(df["epsAvg"].iloc[-1]) if "epsAvg" in df.columns else eps_1y
                    if df.shape[0] >= 2:
                        eps_2y = _f(df["epsAvg"].iloc[-2]) if "epsAvg" in df.columns else eps_2y
        except Exception:
            pass

    # 3) .info fallback: forwardEps (≈ nästa år), trailingEps (≈ TTM)
    try:
        inf = t.info or {}
    except Exception:
        inf = {}
    if eps_1y is None:
        eps_1y = _f(inf.get("forwardEps"))
    # långsiktig tillväxt – yfinance saknar ibland bra fält, vi försöker med några alternativ
    for k in ("earningsGrowth", "earningsQuarterlyGrowth", "longTermEarningsGrowthRate"):
        if eps_cagr_long is None:
            eps_cagr_long = _f(inf.get(k))

    return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": eps_cagr_long}


# -----------------------------
# Tillväxtinferens (hist + estimat)
# -----------------------------
def _infer_eps_growth(row_like: Mapping[str, Any], est: Mapping[str, Any]) -> Optional[float]:
    """
    Välj lämplig EPS-tillväxt att använda i FV:
      1) Om EPS_1Y och EPS_TTM>0 → (EPS_1Y/EPS_TTM - 1)
      2) Annars långsiktig estimerad EPS-CAGR
      3) Annars historisk EPS-CAGR 5Y
    Clamp: [-20%, +35%] (konservativt)
    """
    eps_ttm = _f(row_like.get("EPS TTM") or row_like.get("eps_ttm"))
    eps_1y  = _f(est.get("eps_1y"))
    g_long  = _f(est.get("eps_cagr_long"))
    g_hist  = _f(row_like.get("EPS CAGR") or row_like.get("eps_cagr_hist"))

    g = None
    if _pos(eps_ttm) and _pos(eps_1y):
        try:
            g = float(eps_1y) / float(eps_ttm) - 1.0
        except Exception:
            g = None
    if g is None and g_long is not None:
        g = g_long
    if g is None and g_hist is not None:
        g = g_hist
    if g is None:
        return None
    return _clamp(g, -0.20, 0.35)


def _infer_rev_growth(row_like: Mapping[str, Any]) -> Optional[float]:
    """
    Revenue-tillväxt för EV/S-metod:
      1) Historisk Rev CAGR (5Y) om finns
      2) Annars enkel konservativ default 5% (defensivt)
    Clamp: [-10%, +35%]
    """
    g = _f(row_like.get("Rev CAGR") or row_like.get("rev_cagr_hist"))
    if g is None:
        g = 0.05
    return _clamp(g, -0.10, 0.35)


# -----------------------------
# Målmultiplar (strikt band)
# -----------------------------
def _target_pe_strict(eps_g: Optional[float]) -> float:
    """
    Konservativt P/E-ankare:
      • Bas 14x
      • Justera svagt efter tillväxt: +/− 4p över hela spannet
      • Clamp 10–18x
    """
    base = 14.0
    if eps_g is None:
        return 14.0
    # eps_g i [-0.20, 0.35] → skala till [-2, +2] ungefär
    adj = 4.0 * float(eps_g) / 0.35
    pe = base + adj
    return float(max(10.0, min(18.0, pe)))


def _target_evs_strict(rev_g: Optional[float]) -> float:
    """
    Konservativt EV/S-ankare beroende på revenue-tillväxt:
      • ≤0% → 1.0x
      • 0–10% → 1.5x
      • 10–20% → 2.5x
      • 20–30% → 3.5x
      • >30% → 4.0x
    """
    if rev_g is None:
        return 1.5
    g = float(rev_g)
    if g <= 0.00:   return 1.0
    if g <= 0.10:   return 1.5
    if g <= 0.20:   return 2.5
    if g <= 0.30:   return 3.5
    return 4.0


# -----------------------------
# Fair value-beräkningar
# -----------------------------
def _fv_pe_per_share(eps_today: float, pe: float) -> Optional[float]:
    if not (_pos(eps_today) and _pos(pe)):
        return None
    try:
        return float(eps_today) * float(pe)
    except Exception:
        return None


def _fv_pe_forward_series(eps_today: Optional[float], g_eps: Optional[float], pe_anchor: float) -> Dict[str, Optional[float]]:
    """
    Riktkurser via P/E: idag, +1y, +2y, +3y.
    EPS(t) = EPS0 * (1+g)^t. Multipeln hålls på ankaret (strikt).
    """
    out = {"today": None, "y1": None, "y2": None, "y3": None}
    if not _pos(eps_today) or not _pos(pe_anchor):
        return out
    e0 = float(eps_today)
    g  = float(g_eps) if g_eps is not None else 0.0

    out["today"] = _fv_pe_per_share(e0, pe_anchor)
    out["y1"]    = _fv_pe_per_share(e0*(1+g), pe_anchor)
    out["y2"]    = _fv_pe_per_share(e0*(1+g)**2, pe_anchor)
    out["y3"]    = _fv_pe_per_share(e0*(1+g)**3, pe_anchor)
    return out


def _fv_evs_per_share(rev_today: float, evs: float, net_debt: float, shares_out: float) -> Optional[float]:
    """
    Equity = EV − NetDebt. EV = EV/S * Revenue.
    Fair value per aktie = Equity / Shares.
    """
    if not (_pos(rev_today) and _pos(evs) and _pos(shares_out)):
        return None
    try:
        ev = float(evs) * float(rev_today)
        eq = ev - float(net_debt or 0.0)
        return eq / float(shares_out)
    except Exception:
        return None


def _fv_evs_forward_series(rev_today: Optional[float], g_rev: Optional[float],
                           evs_anchor: float, net_debt: Optional[float],
                           shares_out: Optional[float]) -> Dict[str, Optional[float]]:
    out = {"today": None, "y1": None, "y2": None, "y3": None}
    if not (_pos(rev_today) and _pos(evs_anchor) and _pos(shares_out)):
        return out
    r0 = float(rev_today)
    g  = float(g_rev) if g_rev is not None else 0.0
    nd = float(net_debt or 0.0)
    sh = float(shares_out)

    out["today"] = _fv_evs_per_share(r0, evs_anchor, nd, sh)
    out["y1"]    = _fv_evs_per_share(r0*(1+g), evs_anchor, nd, sh)
    out["y2"]    = _fv_evs_per_share(r0*(1+g)**2, evs_anchor, nd, sh)
    out["y3"]    = _fv_evs_per_share(r0*(1+g)**3, evs_anchor, nd, sh)
    return out


def _bull_bear_1y(base_1y: Optional[float], mult_anchor: float, kind: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Bull/Bear 1 år: ±20% multipel-ändring och ±50% av tillväxten.
    För enkelhet använder vi bara multipel-justering på 1Y-värdet om det finns.
    kind: 'pe' eller 'evs' (endast för text/ev. framtida logik)
    """
    if base_1y is None or not _pos(mult_anchor):
        return (None, None)
    try:
        bull = float(base_1y) * 1.20
        bear = float(base_1y) * 0.80
        return (bull, bear)
    except Exception:
        return (None, None)


# -----------------------------
# Valmetod & sammanställning per rad
# -----------------------------
def compute_methods_for_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Väljer värderingsmetod per rad och beräknar riktkurser:
      • Om EPS TTM > 0 → P/E-band (strikt)
      • Annars → EV/S-band (strikt)
    Returnerar ett dict med:
      {
        'method': 'PE' | 'EVS',
        'fv_today','fv_1y','fv_2y','fv_3y','bull_1y','bear_1y',
        'anchor_multiple','growth_used','input_summary'
      }
    """
    ticker = str(row.get("Ticker") or row.get("ticker") or "").strip()

    # Hämta estimerad EPS-tillväxt
    est = {}
    try:
        if ticker:
            est = fetch_yahoo_eps_estimates(ticker)
    except Exception:
        est = {}

    eps_ttm   = _f(row.get("EPS TTM") or row.get("eps_ttm"))
    rev_ttm   = _f(row.get("Rev TTM") or row.get("rev_ttm"))
    shares    = _f(row.get("Utestående aktier") or row.get("shares_out"))
    net_debt  = _f(row.get("Net debt") or row.get("net_debt"))
    currency  = row.get("Valuta") or row.get("currency") or "USD"

    # Välj metod
    use_pe = _pos(eps_ttm)

    if use_pe:
        g_eps   = _infer_eps_growth(row, est)
        pe_tgt  = _target_pe_strict(g_eps)
        series  = _fv_pe_forward_series(eps_ttm, g_eps, pe_tgt)
        bull, bear = _bull_bear_1y(series.get("y1"), pe_tgt, "pe")

        summary = f"P/E-band strikt; EPS_TTM={_f(eps_ttm)}, g_eps={_f(g_eps)}, PE*={_f(pe_tgt)}, Valuta={currency}"
        return {
            "method": "PE",
            "fv_today": series.get("today"),
            "fv_1y":    series.get("y1"),
            "fv_2y":    series.get("y2"),
            "fv_3y":    series.get("y3"),
            "bull_1y":  bull,
            "bear_1y":  bear,
            "anchor_multiple": pe_tgt,
            "growth_used": g_eps,
            "input_summary": summary,
            "currency": currency,
        }

    # EV/S-gren (kräver rev + shares; net_debt antas 0 om saknas)
    g_rev   = _infer_rev_growth(row)
    evs_tgt = _target_evs_strict(g_rev)
    series  = _fv_evs_forward_series(rev_ttm, g_rev, evs_tgt, net_debt, shares)
    bull, bear = _bull_bear_1y(series.get("y1"), evs_tgt, "evs")

    summary = f"EV/S-band strikt; Rev_TTM={_f(rev_ttm)}, g_rev={_f(g_rev)}, EV/S*={_f(evs_tgt)}, NetDebt={_f(net_debt)}, Shares={_f(shares)}, Valuta={currency}"
    return {
        "method": "EVS",
        "fv_today": series.get("today"),
        "fv_1y":    series.get("y1"),
        "fv_2y":    series.get("y2"),
        "fv_3y":    series.get("y3"),
        "bull_1y":  bull,
        "bear_1y":  bear,
        "anchor_multiple": evs_tgt,
        "growth_used": g_rev,
        "input_summary": summary,
        "currency": currency,
    }


# -----------------------------
# Skriv tillbaka till Data-DF
# -----------------------------
def update_row_with_fair_values(row: pd.Series) -> pd.Series:
    """
    Beräknar fair value och skriver in följande kolumner:
      • 'Riktkurs idag', 'Riktkurs 1 år', 'Riktkurs 2 år', 'Riktkurs 3 år'
      • 'Bull 1 år', 'Bear 1 år'
      • 'Metod', 'Input-sammanfattning'
    Modifierar inte andra fält.
    """
    try:
        res = compute_methods_for_row(row)
    except Exception as e:
        # Skriv felspår i sammanfattningen för felsökning
        row["Metod"] = "ERROR"
        row["Input-sammanfattning"] = f"FV error: {e}"
        return row

    row["Metod"] = "P/E (strikt)" if res.get("method") == "PE" else "EV/S (strikt)"
    row["Riktkurs idag"] = _f(res.get("fv_today"))
    row["Riktkurs 1 år"] = _f(res.get("fv_1y"))
    row["Riktkurs 2 år"] = _f(res.get("fv_2y"))
    row["Riktkurs 3 år"] = _f(res.get("fv_3y"))
    row["Bull 1 år"] = _f(res.get("bull_1y"))
    row["Bear 1 år"] = _f(res.get("bear_1y"))
    row["Input-sammanfattning"] = res.get("input_summary")
    row["Senast FV uppdaterad"] = now_stamp()
    return row


def apply_fair_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Anropa denna på hela Data-tabellen för att uppdatera riktkurser.
    Kolumner som inte berörs lämnas oförändrade.
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return df
    cols_needed = [
        "Riktkurs idag", "Riktkurs 1 år", "Riktkurs 2 år", "Riktkurs 3 år",
        "Bull 1 år", "Bear 1 år", "Metod", "Input-sammanfattning", "Senast FV uppdaterad"
    ]
    for c in cols_needed:
        if c not in df.columns:
            df[c] = None
    return df.apply(update_row_with_fair_values, axis=1)

# ============================================================
# Del 4/6 — UI: Analys & Investeringsförslag
#  • Alias till EPS-estimat-funktionen
#  • Uppsida-beräkningar och hjälpare
#  • Analys-vy (bläddra 1/X, visa ett bolag + hela tabellen längst ned)
#  • Investeringsförslag-vy (välj riktkurshorisont, sortera på uppsida, bläddra)
#  • Knappar: Uppdatera Fair Value nu (beräknar om lokalt i DF)
# ============================================================

# -----------------------------
# Alias (säkerställa symbol som används i övrig kod)
# -----------------------------
try:
    fetch_yahoo_eps_estimates  # type: ignore[name-defined]
except NameError:
    # Gör publik alias till den interna funktionen om den inte redan finns
    fetch_yahoo_eps_estimates = _fetch_eps_estimates_yahoo  # type: ignore


# -----------------------------
# Hjälpfunktioner för uppsida & visning
# -----------------------------
_UPP_COLS = {
    "Idag": "Riktkurs idag",
    "1 år": "Riktkurs 1 år",
    "2 år": "Riktkurs 2 år",
    "3 år": "Riktkurs 3 år",
}

def _current_price_from_row(row: Mapping[str, Any]) -> Optional[float]:
    """
    Hämtar nuvarande kurs ur kända kolumnnamn.
    Stödjer flera varianter från tidigare basversioner.
    """
    for cand in ("Aktuell kurs", "Aktuell kurs (0)", "Kurs", "Price"):
        v = row.get(cand)
        fv = _f(v)
        if fv is not None:
            return fv
    return None

def _ensure_uppsida(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """
    Lägger till kolumnen 'Uppsida %' baserat på target_col och aktuell kurs.
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return df
    if "Uppsida %" not in df.columns:
        df["Uppsida %"] = None

    def _calc_row(r: pd.Series) -> Optional[float]:
        p0   = _current_price_from_row(r)
        tgt  = _f(r.get(target_col))
        if _pos(p0) and _pos(tgt):
            try:
                return (float(tgt) / float(p0) - 1.0) * 100.0
            except Exception:
                return None
        return None

    df["Uppsida %"] = df.apply(_calc_row, axis=1)
    return df

def _fmt_money(x: Optional[float]) -> str:
    return "-" if x is None else f"{float(x):.2f}"

def _fmt_pct(x: Optional[float]) -> str:
    return "-" if x is None else f"{float(x):.1f}%"

def _browse_index(key: str, n: int) -> int:
    if key not in st.session_state:
        st.session_state[key] = 0
    if n <= 0:
        st.session_state[key] = 0
        return 0
    col_a, col_b, col_c = st.columns([1,1,3])
    with col_a:
        if st.button("◀︎ Föregående", key=f"{key}_prev"):
            st.session_state[key] = (st.session_state[key] - 1) % n
    with col_b:
        if st.button("Nästa ▶︎", key=f"{key}_next"):
            st.session_state[key] = (st.session_state[key] + 1) % n
    with col_c:
        st.caption(f"Visar {st.session_state[key]+1} / {n}")
    return st.session_state[key]


# -----------------------------
# Knappar/åtgärder
# -----------------------------
def action_update_fair_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uppdaterar fair value-kolumner i befintligt DataFrame lokalt (ingen Sheets-skrivning här).
    """
    with st.spinner("Beräknar fair value (strikt)…"):
        df2 = apply_fair_values(df.copy())
    st.success("Fair value uppdaterat (lokalt i appen).")
    return df2


# -----------------------------
# Analys-vy
# -----------------------------
def render_analysis_view(df: pd.DataFrame):
    st.subheader("🔎 Analys")

    # Åtgärdsknappar
    col1, col2 = st.columns([1,3])
    with col1:
        if st.button("Uppdatera Fair Value nu", key="btn_update_fv_now"):
            st.session_state["DATA_DF"] = action_update_fair_values(df)
            df = st.session_state["DATA_DF"]

    # Välj ett bolag och visa nycklar
    tickers = [t for t in (df.get("Ticker") or pd.Series(dtype=str)).astype(str).tolist() if t]
    sel = st.selectbox("Välj bolag", options=["—"] + tickers, index=0, key="analysis_pick")
    if sel != "—":
        row = df.loc[df["Ticker"].astype(str) == sel].head(1)
        if not row.empty:
            r = row.iloc[0]
            cols_left = [
                "Ticker", "Valuta", "EPS TTM", "Rev TTM", "Utestående aktier",
                "Net debt", "Metod", "Input-sammanfattning"
            ]
            show = {c: r.get(c) for c in cols_left if c in df.columns}
            # Lägg till kurs + riktkurser om finns
            price_now = _current_price_from_row(r)
            show["Aktuell kurs"] = _f(price_now)
            for c in ("Riktkurs idag", "Riktkurs 1 år", "Riktkurs 2 år", "Riktkurs 3 år", "Bull 1 år", "Bear 1 år"):
                if c in df.columns:
                    show[c] = _f(r.get(c))

            # Presentera
            grid = []
            for k, v in show.items():
                if isinstance(v, (int, float)) or _f(v) is not None:
                    grid.append((k, _fmt_money(_f(v))))
                else:
                    grid.append((k, str(v) if v is not None else "-"))

            st.write("**Nycklar**")
            for k, v in grid:
                st.write(f"- {k}: {v}")
        else:
            st.info("Hittade inte valt bolag i tabellen.")

    st.divider()
    st.write("### Hela databasen (ofiltrerad)")
    st.caption("Visar alla kolumner. (Enkel tabell utan sortering/redigering.)")
    st.dataframe(df, use_container_width=True)


# -----------------------------
# Investeringsförslag-vy
# -----------------------------
def render_invest_view(df: pd.DataFrame):
    st.subheader("💡 Investeringsförslag")

    col_a, col_b, col_c = st.columns([1,1,2])
    with col_a:
        horizon = st.selectbox("Riktkurs-horisont", options=list(_UPP_COLS.keys()), index=1, key="horizon_pick")
    with col_b:
        # Valbart belopp för att räkna antal aktier
        cash = st.number_input("Tillgängligt belopp", min_value=0.0, step=100.0, value=0.0, key="invest_cash")

    target_col = _UPP_COLS[horizon]

    # Säkerställ Fair Value-kolumner och uppsida
    if any(c not in df.columns for c in _UPP_COLS.values()):
        st.warning("Riktkurskolumner saknas – beräkna Fair Value först.")
    else:
        df = _ensure_uppsida(df.copy(), target_col)
        # Sortera på Uppsida % (störst först)
        df_sorted = df.sort_values(by=["Uppsida %"], ascending=[False], na_position="last")

        # Bläddra 1/X
        idx = _browse_index("invest_idx", len(df_sorted))
        if len(df_sorted) > 0:
            row = df_sorted.iloc[idx]
            tkr = str(row.get("Ticker") or "")
            price_now = _current_price_from_row(row)
            tgt = _f(row.get(target_col))
            upp = _f(row.get("Uppsida %"))

            st.write(f"#### {tkr}")
            met = row.get("Metod")
            st.caption(f"Metod: {met} | Mål: {target_col}")

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Aktuell kurs", _fmt_money(price_now))
            with col2:
                st.metric(f"Riktkurs ({horizon})", _fmt_money(tgt))
            with col3:
                st.metric("Uppsida", _fmt_pct(upp))

            # Köp-simulering
            if cash and _pos(price_now):
                try:
                    qty = int(max(0, math.floor(float(cash) / float(price_now))))
                except Exception:
                    qty = 0
                st.write(f"**Köp-simulering:** för {cash:,.0f} kan du köpa ≈ **{qty} st** aktier.")
            else:
                st.caption("Ange 'Tillgängligt belopp' för att se hur många aktier som kan köpas.")

            with st.expander("Visa Input-sammanfattning"):
                st.code(str(row.get("Input-sammanfattning") or "-"))

        st.divider()
        st.write("### Ranking (störst uppsida först)")
        cols_show = ["Ticker", "Valuta", "Uppsida %", target_col, "Aktuell kurs", "Metod"]
        # Sätt "Aktuell kurs"-kolumn dynamiskt om saknas
        if "Aktuell kurs" not in df_sorted.columns:
            df_sorted["Aktuell kurs"] = df_sorted.apply(_current_price_from_row, axis=1)
        st.dataframe(df_sorted[ [c for c in cols_show if c in df_sorted.columns] ], use_container_width=True)

# ============================================================
# Del 5/6 — Vyer: Editor • Lägg till • Portfölj • Batch • Settings • Snapshot
#  • Robust utdelningshämtning (fallback) används i Editor/Portfölj/Batch
#  • Ingen valutakonvertering av EPS – samma praxis som basen
# ============================================================

# -----------------------------
# Små helpers (definiera bara om de saknas)
# -----------------------------
try:
    _round2_or_none  # type: ignore[name-defined]
except NameError:
    def _round2_or_none(x):
        v = _f(x)
        return None if v is None else round(float(v), 2)

try:
    _parse_float  # type: ignore[name-defined]
except NameError:
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

try:
    _maybe  # type: ignore[name-defined]
except NameError:
    def _maybe(v):
        return v if (v is not None and not (isinstance(v, float) and pd.isna(v))) else None

# -----------------------------
# Utdelnings-snapshot (fallback)
# -----------------------------
@st.cache_data(ttl=6*3600, show_spinner=False)
def _fetch_dividend_snapshot(ticker: str) -> dict:
    """
    Returnerar:
      {
        'dps_annual': float | None,       # års-takt (brutto per aktie)
        'freq': 'M'|'Q'|'S'|'A'|None,     # gissad frekvens
        'next_pay_date': date|None,       # enkel prognos från historik
        'next_per_payment': float|None    # dps_annual / n_per_year
      }
    Strategi:
      1) yfinance.info['dividendRate'] (forward) om finns
      2) annars 'trailingAnnualDividendRate'
      3) annars summera TTM från .dividends (senaste 365–400 dagar)
    Frekvens gissas via antal kuponger senaste ~400 dagar.
    """
    try:
        tk = yf.Ticker(str(ticker).strip())
        dps = None
        freq = None
        next_pay = None
        per_payment = None

        # 1/2) Försök med forward / trailing från info
        info = {}
        try:
            info = tk.info or {}
        except Exception:
            info = {}

        for key in ("dividendRate", "trailingAnnualDividendRate"):
            if _f(info.get(key)) is not None:
                dps = float(_f(info.get(key)))
                break

        # 3) TTM från utdelningshistorik
        try:
            divs = tk.dividends  # pandas Series (date index)
        except Exception:
            divs = None

        if (dps is None) and (divs is not None) and (hasattr(divs, "empty") and not divs.empty):
            s = divs.copy().dropna()
            cutoff = pd.Timestamp.today(tz=s.index.tz) - pd.Timedelta(days=400)
            s_ttm = s[s.index >= cutoff]
            if not s_ttm.empty:
                dps = float(pd.to_numeric(s_ttm, errors="coerce").dropna().sum())

        # Gissa frekvens + nästa betalningsdag
        if (divs is not None) and (hasattr(divs, "empty") and not divs.empty):
            s = divs.dropna()
            cutoff = pd.Timestamp.today(tz=s.index.tz) - pd.Timedelta(days=400)
            s_recent = s[s.index >= cutoff]
            n = int(s_recent.shape[0])
            if n >= 11:
                freq = "M"
            elif n >= 4:
                freq = "Q"
            elif n >= 2:
                freq = "S"
            elif n >= 1:
                freq = "A"
            try:
                last_dt = pd.to_datetime(s.index[-1]).date()
                step = {"M": 30, "Q": 90, "S": 180, "A": 365}.get(freq, None)
                if step:
                    next_pay = (pd.Timestamp(last_dt) + pd.Timedelta(days=step)).date()
            except Exception:
                next_pay = None

        if dps is not None and freq in ("M","Q","S","A"):
            denom = {"M": 12, "Q": 4, "S": 2, "A": 1}[freq]
            per_payment = dps / denom

        return {
            "dps_annual": (None if dps is None else float(dps)),
            "freq": freq,
            "next_pay_date": next_pay,
            "next_per_payment": (None if per_payment is None else float(per_payment)),
        }
    except Exception:
        return {"dps_annual": None, "freq": None, "next_pay_date": None, "next_per_payment": None}

# -----------------------------
# Settings & FX-hjälpare
# -----------------------------
def _withholding_tax_from_settings(currency: str, settings: dict[str, str]) -> float:
    """
    Källskatt utifrån handelsvaluta.
    Hämtas från Settings-bladet om satt, annars default enligt dina regler.
    """
    code = (currency or "USD").upper().strip()
    key = f"withholding_{code}"
    try:
        v = settings.get(key, None)
        if v is not None and str(v).strip() != "":
            val = float(_f(v))
            if val is not None:
                return max(0.0, min(0.5, val))
    except Exception:
        pass
    # defaults
    if code == "USD": return 0.15
    if code == "CAD": return 0.15
    if code == "NOK": return 0.25
    return 0.0

def _fx_to_sek(amount: Optional[float], currency: str, fx_map: dict[str, float]) -> Optional[float]:
    a = _f(amount)
    if a is None:
        return None
    cur = (currency or "").upper().strip()
    if cur == "SEK":
        return float(a)
    rate = _f(fx_map.get(cur))
    if rate is None:
        return None
    return float(a) * float(rate)

@st.cache_data(ttl=600, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    s = _read_df(SETTINGS_TITLE)
    out: Dict[str,str] = {}
    if not s.empty:
        for _, r in s.iterrows():
            k = str(r.get("Key"))
            v = "" if pd.isna(r.get("Value")) else str(r.get("Value"))
            out[k] = v
    return out

# -----------------------------
# Portföljtabell + vy
# -----------------------------
def _build_portfolio_table(data_df: pd.DataFrame, fx_map: dict[str, float], settings: dict[str, str]) -> pd.DataFrame:
    """
    Returnerar en per-innehav-tabell med:
      Ticker, Bucket, Valuta, Antal, Kurs, Värde (SEK), GAV (SEK), Kostnad (SEK),
      P/L (SEK, %), Årlig utd/aktie (brutto/netto), Årlig utd (valuta/SEK), Yield (netto).
    ✅ Om 'Årlig utdelning' saknas → estimeras via _fetch_dividend_snapshot().
       Saknas 'Aktuell kurs'/'Valuta' → hämtas via fetch_from_yahoo().
    """
    cols_out = [
        "Ticker","Bolagsnamn","Bucket","Valuta",
        "Antal aktier","Aktuell kurs","Värde (SEK)",
        "GAV (SEK)","Kostnad (SEK)","P/L (SEK)","P/L (%)",
        "Årlig utd/aktie (brutto)","Årlig utd/aktie (netto)",
        "Årlig utd (valuta, netto)","Årlig utd (SEK, netto)",
        "Yield (netto)"
    ]
    rows: list[dict] = []

    for _, r in data_df.iterrows():
        shares = _pos(r.get("Antal aktier")) or 0.0
        if shares <= 0:
            continue

        ticker = str(r.get("Ticker") or "").strip()
        name   = str(r.get("Bolagsnamn") or "").strip()
        bucket = str(r.get("Bucket") or "").strip()
        ccy    = str(r.get("Valuta") or "").upper().strip()

        # Pris/valuta – fallback till Yahoo om saknas
        price  = _pos(r.get("Aktuell kurs"))
        if price is None or not ccy:
            y = fetch_from_yahoo(ticker)
            if price is None:
                price = _pos(y.get("price"))
            if not ccy:
                ccy = str(y.get("currency") or "USD").upper()

        gav    = _f(r.get("GAV (SEK)"))  # per aktie i SEK

        # Utdelning per aktie (brutto) – bladets värde eller uppskattning
        dps_br = _pos(r.get("Årlig utdelning"))
        if dps_br is None:
            snap = _fetch_dividend_snapshot(ticker)
            dps_br = _pos(snap.get("dps_annual"))

        value_ccy = (price or 0.0) * shares if price is not None else None
        value_sek = _fx_to_sek(value_ccy, ccy, fx_map) if value_ccy is not None else None

        cost_sek  = (gav * shares) if (gav is not None) else None
        pl_sek    = (None if (value_sek is None or cost_sek is None) else (value_sek - cost_sek))
        pl_pct    = (None if (pl_sek is None or not cost_sek or cost_sek == 0) else (pl_sek / cost_sek) * 100.0)

        tax = _withholding_tax_from_settings(ccy, settings)
        dps_net = (None if dps_br is None else dps_br * (1.0 - tax))

        div_annual_ccy = (None if (dps_net is None) else dps_net * shares)
        div_annual_sek = None if div_annual_ccy is None else _fx_to_sek(div_annual_ccy, ccy, fx_map)

        net_yield = None
        if _pos(price) and _pos(dps_net):
            net_yield = float(dps_net) / float(price)

        rows.append({
            "Ticker": ticker,
            "Bolagsnamn": name,
            "Bucket": bucket,
            "Valuta": ccy or "USD",
            "Antal aktier": int(shares) if float(shares).is_integer() else float(shares),
            "Aktuell kurs": None if price is None else round(float(price), 4),
            "Värde (SEK)": None if value_sek is None else round(float(value_sek), 2),
            "GAV (SEK)": None if gav is None else round(float(gav), 4),
            "Kostnad (SEK)": None if cost_sek is None else round(float(cost_sek), 2),
            "P/L (SEK)": None if pl_sek is None else round(float(pl_sek), 2),
            "P/L (%)": None if pl_pct is None else round(float(pl_pct), 2),
            "Årlig utd/aktie (brutto)": None if dps_br is None else round(float(dps_br), 6),
            "Årlig utd/aktie (netto)": None if dps_net is None else round(float(dps_net), 6),
            "Årlig utd (valuta, netto)": None if div_annual_ccy is None else round(float(div_annual_ccy), 4),
            "Årlig utd (SEK, netto)": None if div_annual_sek is None else round(float(div_annual_sek), 2),
            "Yield (netto)": None if net_yield is None else round(float(net_yield)*100.0, 2),
        })

    if not rows:
        return pd.DataFrame(columns=cols_out)

    df = pd.DataFrame(rows)
    if "Värde (SEK)" in df.columns:
        df = df.sort_values("Värde (SEK)", ascending=False, na_position="last").reset_index(drop=True)
    return df[cols_out]

def render_portfolio_view(data_df: pd.DataFrame, fx_map: dict[str, float]) -> None:
    st.subheader("📦 Portfölj")
    settings = get_settings_map()
    pf = _build_portfolio_table(data_df, fx_map, settings)

    total_value = float(pf["Värde (SEK)"].sum()) if not pf.empty else 0.0
    total_cost  = float(pf["Kostnad (SEK)"].sum()) if not pf.empty else 0.0
    total_pl    = total_value - total_cost
    total_plpct = (total_pl / total_cost * 100.0) if total_cost > 0 else 0.0

    total_div_sek = float(pf["Årlig utd (SEK, netto)"].sum()) if ("Årlig utd (SEK, netto)" in pf.columns and not pf.empty) else 0.0
    avg_per_month = total_div_sek / 12.0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Totalt portföljvärde (SEK)", f"{total_value:,.0f}".replace(",", " "))
    with c2:
        st.metric("Anskaffning (SEK)", f"{total_cost:,.0f}".replace(",", " "))
    with c3:
        st.metric("P/L (SEK)", f"{total_pl:,.0f}".replace(",", " "), delta=f"{total_plpct:.2f}%")
    with c4:
        st.metric("Årlig utd. (SEK, netto)", f"{total_div_sek:,.0f}".replace(",", " "))

    st.metric("Snitt per månad (SEK, netto)", f"{avg_per_month:,.0f}".replace(",", " "))

    st.markdown("### Innehav (detaljer)")
    if pf.empty:
        st.info("Inga innehav med Antal aktier > 0.")
    else:
        st.dataframe(pf, use_container_width=True, hide_index=True)

# -----------------------------
# Editor-stöd
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
# Yahoo→rad-uppdateringar (inkl utdelning)
# -----------------------------
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    y   = fetch_from_yahoo(ticker)
    # Namn & sektor
    long_name, sector = None, None
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}
        long_name = info.get("longName") or info.get("shortName")
        sector    = info.get("sector")
    except Exception:
        pass

    # Utdelning (fallback)
    d = _fetch_dividend_snapshot(ticker)

    updates = {
        "Timestamp": now_stamp(),
        # Basdata
        "Bolagsnamn": long_name if long_name else existing_row.get("Bolagsnamn"),
        "Sektor": sector if sector else existing_row.get("Sektor"),
        "Aktuell kurs": _round2_or_none(y.get("price")),
        "Valuta": (y.get("currency") or existing_row.get("Valuta")),
        "Utestående aktier": _maybe(y.get("shares_out")),
        "Net debt": _maybe(y.get("net_debt")),
        # TTM
        "Rev TTM": _maybe(y.get("rev_ttm")),
        "EBITDA TTM": _maybe(y.get("ebitda_ttm")),
        "EPS TTM": _maybe(y.get("eps_ttm")),
        # Multiplar
        "PE TTM": _maybe(y.get("pe_ttm")),
        "PE FWD": _maybe(y.get("pe_fwd")),
        "EV/Revenue": _maybe(y.get("ev_rev")),
        "EV/EBITDA": _maybe(y.get("ev_ebitda")),
        "P/B": _maybe(y.get("p_b")),
        "BVPS": _maybe(y.get("bvps")),
        # CAGR
        "Rev CAGR": _maybe(y.get("rev_cagr_hist")),
        "EPS CAGR": _maybe(y.get("eps_cagr_hist")),
        # Utdelning (skriv bara om saknas i raden)
        "Årlig utdelning": (_maybe(d.get("dps_annual"))
                            if pd.isna(existing_row.get("Årlig utdelning")) else existing_row.get("Årlig utdelning")),
        "Utdelningsfrekvens": (d.get("freq")
                               if (existing_row.get("Utdelningsfrekvens") in (None, np.nan, "", "nan"))
                               else existing_row.get("Utdelningsfrekvens")),
        "Nästa utdelningsdatum": (d.get("next_pay_date")
                                  if pd.isna(existing_row.get("Nästa utdelningsdatum")) else existing_row.get("Nästa utdelningsdatum")),
        "Nästa utdelning (per aktie)": (_maybe(d.get("next_per_payment"))
                                        if pd.isna(existing_row.get("Nästa utdelning (per aktie)")) else existing_row.get("Nästa utdelning (per aktie)")),
        # Spårning
        "Senast auto uppdaterad": now_stamp(),
        "Auto källa": "Yahoo",
    }

    # Rensa None/NaN/tomma
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

# -----------------------------
# Editor-vy
# -----------------------------
def page_editor():
    st.header("✏️ Editor (manuella fält + Yahoo-fyll)")

    df = st.session_state.get("DATA")
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
        # Ingen valutakonvertering av EPS – användaren matar redan i bolagets valuta
        eps1_in = st.text_input("EPS 1Y (estimat)", value=str(_f(row.get("EPS 1Y")) or ""))
        eps2_in = st.text_input("EPS 2Y (estimat)", value=str(_f(row.get("EPS 2Y")) or ""))
        # Rev 1Y/2Y i MILJONER – vi multiplicerar 1e6 när vi sparar i raden
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
                df.loc[idx, "Senast manuellt uppdaterad"] = now_stamp()

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
                st.success(f"{tkr}: Rad sparad till Google Sheets och uppdaterad från Yahoo (inkl utdelning om tillgänglig).")
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
                    # Namn/sektor + valuta
                    try:
                        t = yf.Ticker(tkr)
                        info = t.info or {}
                        st.session_state["add_name"] = info.get("longName") or info.get("shortName") or st.session_state.get("add_name", "")
                        st.session_state["add_sector"] = info.get("sector") or st.session_state.get("add_sector", "")
                    except Exception:
                        pass
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
                "Timestamp": now_stamp(),
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

            # Ingen EPS-konvertering – användarens värden är i bolagets valuta
            eps1_v  = _parse_float(eps1_in)
            eps2_v  = _parse_float(eps2_in)
            # Rev i MILJONER → 1e6
            rev1_vm = (_parse_float(rev1_in) or 0.0) * 1_000_000.0 if rev1_in not in (None,"") else None
            rev2_vm = (_parse_float(rev2_in) or 0.0) * 1_000_000.0 if rev2_in not in (None,"") else None
            if eps1_v is not None: new_row["EPS 1Y"] = eps1_v
            if eps2_v is not None: new_row["EPS 2Y"] = eps2_v
            if rev1_vm is not None: new_row["Rev 1Y"] = rev1_vm
            if rev2_vm is not None: new_row["Rev 2Y"] = rev2_vm
            new_row["Senast manuellt uppdaterad"] = now_stamp()

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
# Portfölj-vy (kopplar mot render_portfolio_view)
# -----------------------------
def page_portfolio():
    st.header("📊 Portfölj")
    df = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx = st.session_state.get("FX", {}) or {}
    try:
        render_portfolio_view(df, fx)
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")

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

# -----------------------------
# Settings
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

# -----------------------------
# Snapshot
# -----------------------------
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True)

# ============================================================
# Del 6/6 — Main & Navigation
#  • Startsekvens (DATA/FX/Settings in i session_state)
#  • Sidomeny och routning
#  • Analys-vy (inkl. full datatabell längst ner)
#  • Fallbacks om vissa vy-funktioner saknas i denna bas
# ============================================================

# -----------------------------
# Bootstrapping
# -----------------------------
def _bootstrap_session():
    """Säkerställ att DATA, FX och Settings finns i session_state."""
    if "DATA" not in st.session_state or st.session_state.get("DATA") is None:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception as e:
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
            st.warning(f"Kunde inte läsa DATA-bladet: {e}")

    if "FX" not in st.session_state or not st.session_state.get("FX"):
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception as e:
            st.session_state["FX"] = {}
            st.warning(f"Kunde inte läsa Valutakurser: {e}")

    # Auto-refresh av FX vid start (om satt i Settings)
    try:
        s = get_settings_map()
        if str(s.get("auto_refresh_on_start", "0")) == "1":
            try:
                _load_fx_and_update_sheet()
                st.session_state["FX"] = get_fx_map()
                st.caption("Valutakurser uppdaterades automatiskt vid start.")
            except Exception as fx_e:
                st.warning(f"Kunde inte auto-uppdatera FX: {fx_e}")
    except Exception:
        pass

# -----------------------------
# Analys-vy
#  - Lättviktsvy som alltid finns
#  - Visar antal bolag och hela databasen längst ner (krav)
# -----------------------------
def page_analysis():
    st.header("🔎 Analys")

    df = st.session_state.get("DATA")
    if df is None or df.empty:
        st.info("Ingen data att visa ännu. Lägg till bolag eller uppdatera från Yahoo.")
        return

    # En enkel översikt på antal bolag och senaste timestamp
    n = int(df.shape[0])
    latest_ts = None
    if "Timestamp" in df.columns and not df["Timestamp"].isna().all():
        try:
            latest_ts = str(df["Timestamp"].dropna().astype(str).max())
        except Exception:
            latest_ts = None

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Antal rader i DATA", f"{n}")
    with c2:
        st.metric("Senast uppdaterad (max Timestamp)", latest_ts or "—")

    st.markdown("---")
    st.subheader("Hela databasen (ofiltrerad)")
    st.caption("Krav: hela tabellen visas längst ner i analysvyn.")
    st.dataframe(df, use_container_width=True)

# -----------------------------
# Hjälpsidor (fallback)
# -----------------------------
def _call_or_fallback(fn, title: str):
    """Anropa fn() om den finns, annars visa tydlig info."""
    try:
        return fn()
    except NameError:
        st.info(f"'{title}' är inte inkluderad i den här delen av koden.")
    except Exception as e:
        st.error(f"Fel i '{title}': {e}")

# -----------------------------
# Main
# -----------------------------
def main():
    _bootstrap_session()

    # Sidomeny
    st.sidebar.title("📈 Aktieanalys & investeringsförslag")
    menu = st.sidebar.radio(
        "Meny",
        [
            "Analys",
            "Investeringsförslag",
            "Portfölj",
            "Editor",
            "Lägg till",
            "Massuppdatera",
            "Settings",
            "Snapshot",
        ],
        index=0,
    )

    # Visa aktuell primär valuta (från Settings)
    try:
        s = get_settings_map()
        prim = s.get("primary_currency", "SEK")
        st.sidebar.caption(f"Primär valuta: **{prim}**")
    except Exception:
        pass

    # Routing
    if menu == "Analys":
        page_analysis()
    elif menu == "Investeringsförslag":
        _call_or_fallback(page_invest, "Investeringsförslag")
    elif menu == "Portfölj":
        _call_or_fallback(lambda: page_portfolio(), "Portfölj")
    elif menu == "Editor":
        _call_or_fallback(lambda: page_editor(), "Editor")
    elif menu == "Lägg till":
        _call_or_fallback(lambda: page_add_ticker(), "Lägg till")
    elif menu == "Massuppdatera":
        _call_or_fallback(lambda: page_batch(), "Massuppdatera")
    elif menu == "Settings":
        _call_or_fallback(lambda: page_settings(), "Settings")
    elif menu == "Snapshot":
        _call_or_fallback(lambda: page_snapshot(), "Snapshot")
    else:
        page_analysis()

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    main()
