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
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS) med clamp
#  • Wrapper-funktioner som Editor förväntar sig
# ============================================================

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS) med clamp
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
# TTM från kvartal
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
# Snabb snapshot (pris, valuta, shares, net debt, multiplar)
# -----------------------------
def _fast_snapshot(ticker: str) -> Dict[str, Optional[float]]:
    t = _yf_ticker(ticker)

    price, currency = None, None
    shares_out, market_cap, enterprise_value, net_debt = None, None, None, None
    pe_ttm, pe_fwd = None, None
    ev_rev, ev_ebitda, pb, bvps, dps_annual = None, None, None, None, None

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

    # shares / mcap / EV / debt
    try:
        shares_out = _f(inf.get("sharesOutstanding"))
    except Exception:
        pass
    try:
        market_cap = _f(inf.get("marketCap"))
    except Exception:
        pass
    try:
        enterprise_value = _f(inf.get("enterpriseValue"))
    except Exception:
        pass
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

    # Utdelning (annual DPS, inte yield)
    try:
        dps_annual = _f(inf.get("lastDividendValue"))
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
        "dps_annual": dps_annual,
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
    """
    t = _yf_ticker(ticker)

    snap = _fast_snapshot(ticker)
    ttm  = _ttm_from_quarterly(t)
    cagr = _hist_cagr_5y(t)

    # Härled PE TTM om saknas men EPS TTM finns
    pe_ttm = snap.get("pe_ttm")
    if (pe_ttm is None or not math.isfinite(pe_ttm)) and ttm.get("eps_ttm") and snap.get("price"):
        try:
            if ttm["eps_ttm"] > 0:
                pe_ttm = float(snap["price"]) / float(ttm["eps_ttm"])
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
        "ev_rev": snap.get("ev_rev"),
        "ev_ebitda": snap.get("ev_ebitda"),
        "p_b": snap.get("p_b"),
        "bvps": snap.get("bvps"),

        # Utdelning
        "dps_annual": snap.get("dps_annual"),

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
        "Rev CAGR": "rev_cagr_hist",
        "EPS CAGR": "eps_cagr_hist",
    }
    for col, key in mapping.items():
        val = y.get(key, None)
        if val is not None and (isinstance(val, (int, float)) and math.isfinite(val) or isinstance(val, str)):
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
        # Yahoo exponerar ofta inte frekvens – lämna None så Editor approxar via FWD/4
        "dividend_frequency": None,
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
    """
    Wrapper för historisk revenue-CAGR (5Y) → {'rev_cagr': ...}
    """
    try:
        fin = _yf_ticker(ticker)
        c = _hist_cagr_5y(fin)
        return {"rev_cagr": c.get("rev_cagr_5y")}
    except Exception:
        return {"rev_cagr": None}

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_cagr_hist(ticker: str) -> Dict[str, Optional[float]]:
    """
    Wrapper för historisk EPS-CAGR (5Y) → {'eps_cagr': ...}
    """
    try:
        fin = _yf_ticker(ticker)
        c = _hist_cagr_5y(fin)
        return {"eps_cagr": c.get("eps_cagr_5y")}
    except Exception:
        return {"eps_cagr": None}

# ============================================================
# Del 3/6 — Beräkningsmotor
#  • Metodpriser: PE, EV/S, EV/EBITDA, DACF, P/B (+ placeholders)
#  • Multipel-decay & ankare
#  • ✅ Fair Value (korrigerad): median över metodfamiljer,
#    filtrerar "kurs-kopia"-värden och slår ihop dubbletter
#  • Inkl. _fetch_eps_estimates_yahoo (används av Editor/Add & beräkning)
# ============================================================

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
# Fair Value (NY korrigerad)
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
    y = fetch_from_yahoo(ticker)  # Del 2/6
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
    rev_cagr_hist     = _clamp(rev_cagr_hist_raw, REV_CAGR_MIN, REV_CAGR_MAX)

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), y.get("eps_cagr_hist")))
    eps_cagr_hist     = _clamp(eps_cagr_hist_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    eps_cagr_long = _clamp(est.get("eps_cagr_long"), EPS_CAGR_MIN, EPS_CAGR_MAX)

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
# Del 4/6 — Portfölj & utdelningar
#  • Utdelningsskatt per valuta (USD/CAD/NOK)
#  • Robust datumparser & filtrering av passerade datum
#  • Beräkning av "nästa utbetalning" per innehav:
#      next_pay_date, dps_brutto, dps_netto, summa netto (valuta/SEK)
#  • Sammanfattning + månadstabell
# ============================================================

# -------------------------
# Hjälpare: källskatt & FX
# -------------------------
def _withholding_tax(currency: str) -> float:
    """
    Källskatt utifrån handelsvaluta (enligt dina regler):
    • USD: 15%
    • CAD: 15%
    • NOK: 25%
    • Övrigt: 0% (default)
    """
    cur = (currency or "").upper().strip()
    if cur == "USD":
        return 0.15
    if cur == "CAD":
        return 0.15
    if cur == "NOK":
        return 0.25
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
        # Om kurs saknas: visa None hellre än att chansa
        return None
    return float(a) * float(rate)

# -------------------------
# Hjälpare: datumhantering
# -------------------------
def _parse_date_any(x: Any) -> Optional[dt.date]:
    """
    Försök tolka str/number/datetime till *datum* (utan tid).
    Tomt/ogiltigt → None.
    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if isinstance(x, (dt.date, )):
        return x
    if isinstance(x, dt.datetime):
        return x.date()
    s = str(x).strip()
    if not s:
        return None
    # Vanliga format
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # Sista utväg: pandas to_datetime
    try:
        ts = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(ts) else ts.date()  # type: ignore
    except Exception:
        return None

def _is_future_or_today(d: Optional[dt.date]) -> bool:
    if d is None:
        return False
    today = dt.date.today()
    return d >= today

# -------------------------
# Datakällor i bladet (kolumnalias)
# -------------------------
_NEXT_PAY_DATE_KEYS = [
    "Nästa utdelningsdatum", "Next Dividend Pay Date", "Dividend Pay Date Next",
    "Utdelningsdatum nästa", "Next pay date"
]
_NEXT_DPS_KEYS = [
    "Nästa utd/aktie", "Next DPS", "Dividend Next per Share",
    "Next Dividend/Share", "Utdelning/aktie nästa"
]
_LAST_DPS_KEYS = [
    "Senaste utd/aktie", "Last DPS", "Dividend Last per Share",
    "Senaste utdelning/aktie"
]
_FWD_ANNUAL_DPS_KEYS = [
    "FWD DPS", "Dividend Rate", "Utdelning/aktie (år)", "Dividend per Share (Annual)"
]

def _first_present(row: pd.Series, keys: list[str]) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, "", np.nan):
            return row[k]
    return None

# -------------------------
# Beräkning: nästa utbetalning
# -------------------------
def _build_next_payouts_table(data_df: pd.DataFrame, fx_map: dict[str, float]) -> pd.DataFrame:
    """
    Skapar en tabell med nästa *betalningsdatum* per innehav.
    Kräver minst: Ticker, Valuta, Antal aktier.
    Hämtar per-aktie-belopp från kolumner i bladet i denna ordning:
      1) 'Nästa utd/aktie' (om finns)
      2) 'Senaste utd/aktie' (fallback)
      3) 'FWD DPS' (approx för nästa — delas med sannolik frekvens om 'Frequency' finns)
    Betalningsdatum tas från någon av _NEXT_PAY_DATE_KEYS.
    Poster utan framtida datum filtreras bort.
    """
    cols_out = [
        "Ticker", "Namn", "Valuta", "Antal aktier", "Nästa utb.-datum",
        "Utd/aktie (brutto)", "Källskatt", "Utd/aktie (netto)",
        "Utdelning netto (valuta)", "Utdelning netto (SEK)"
    ]
    out_rows: list[dict] = []

    for _, r in data_df.iterrows():
        ticker = str(r.get("Ticker") or "").strip()
        if not ticker:
            continue

        currency = str(r.get("Valuta") or "USD").upper().strip()
        shares = _pos(r.get("Antal aktier")) or 0.0
        if shares <= 0:
            continue

        # Datum (till python date → filtrera bort dåtid)
        next_pay_raw = _first_present(r, _NEXT_PAY_DATE_KEYS)
        next_pay = _parse_date_any(next_pay_raw)
        if not _is_future_or_today(next_pay):
            continue

        # DPS-kandidater
        dps_next = _f(_first_present(r, _NEXT_DPS_KEYS))
        dps_last = _f(_first_present(r, _LAST_DPS_KEYS))
        dps_fwd  = _f(_first_present(r, _FWD_ANNUAL_DPS_KEYS))

        # Välj belopp för *nästa* utbetalning
        dps_brutto = None
        if dps_next is not None and dps_next > 0:
            dps_brutto = dps_next
        elif dps_last is not None and dps_last > 0:
            dps_brutto = dps_last
        elif dps_fwd is not None and dps_fwd > 0:
            # Om endast FWD (årstakt) finns → approx per kvartal (vanligast).
            # Har bladet en 'Frequency' kan vi använda den, annars anta 4.
            freq = _pos(r.get("Dividend Frequency")) or 4.0
            if freq <= 0:
                freq = 4.0
            dps_brutto = dps_fwd / freq

        if _f(dps_brutto) is None or dps_brutto <= 0:
            continue

        tax = _withholding_tax(currency)
        dps_net = float(dps_brutto) * (1.0 - tax)
        sum_net_ccy = dps_net * float(shares)
        sum_net_sek = _fx_to_sek(sum_net_ccy, currency, fx_map)

        out_rows.append({
            "Ticker": ticker,
            "Namn": r.get("Namn") or r.get("Company") or "",
            "Valuta": currency,
            "Antal aktier": int(shares) if float(shares).is_integer() else float(shares),
            "Nästa utb.-datum": next_pay,  # python date (normaliseras till ts i render)
            "Utd/aktie (brutto)": round(float(dps_brutto), 6),
            "Källskatt": f"{int(tax*100)}%",
            "Utd/aktie (netto)": round(float(dps_net), 6),
            "Utdelning netto (valuta)": round(float(sum_net_ccy), 6),
            "Utdelning netto (SEK)": None if sum_net_sek is None else round(float(sum_net_sek), 2),
        })

    if not out_rows:
        return pd.DataFrame(columns=cols_out)

    df = pd.DataFrame(out_rows)

    # 🔧 Viktigt: normalisera till datetime64[ns] för att undvika NaT/date-jämförelser
    df["Nästa utb.-datum"] = pd.to_datetime(df["Nästa utb.-datum"], errors="coerce")

    df = df.sort_values(by=["Nästa utb.-datum", "Ticker"]).reset_index(drop=True)
    return df[cols_out]

# -------------------------
# UI: Portföljvy
# -------------------------
def render_portfolio_view(data_df: pd.DataFrame, fx_map: dict[str, float]) -> None:
    """
    Visar portfölj + lista över nästa *betalningsdatum* för utdelningar.
    • Filtrerar bort passerade datum.
    • Belopp räknas netto efter källskatt och omräknas till SEK.
    """
    st.subheader("📦 Portfölj")

    # Portföljsammanfattning (enkel): portföljvärde i SEK om 'Aktuell kurs' finns
    if {"Aktuell kurs", "Valuta", "Antal aktier"}.issubset(set(data_df.columns)):
        try:
            values_sek = []
            for _, r in data_df.iterrows():
                px = _f(r.get("Aktuell kurs"))
                sh = _pos(r.get("Antal aktier")) or 0.0
                cur = str(r.get("Valuta") or "USD").upper().strip()
                if px is None or sh <= 0:
                    continue
                v_ccy = float(px) * float(sh)
                v_sek = _fx_to_sek(v_ccy, cur, fx_map)
                if v_sek is not None:
                    values_sek.append(v_sek)
            port_sum_sek = sum(values_sek) if values_sek else 0.0
        except Exception:
            port_sum_sek = 0.0
    else:
        port_sum_sek = 0.0

    # --- Nästa utbetalningar ---
    st.markdown("### 💸 Nästa utdelningsutbetalningar (endast framtida datum)")
    df_next = _build_next_payouts_table(data_df, fx_map)

    if not df_next.empty:
        # 🔧 Säkerställ datetime64[ns] innan .dt används
        df_next["Nästa utb.-datum"] = pd.to_datetime(df_next["Nästa utb.-datum"], errors="coerce")

    total_next_sek = float(df_next["Utdelning netto (SEK)"].sum()) if not df_next.empty else 0.0

    # Sammanfattningsrad
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Totalt portföljvärde (SEK)", f"{port_sum_sek:,.0f}".replace(",", " "))
    with c2:
        st.metric("Summa nästa utdelningar (SEK)", f"{total_next_sek:,.0f}".replace(",", " "))
    with c3:
        # Genomsnitt per aktiv månad baserat på nästabord
        if not df_next.empty:
            active_months = df_next["Nästa utb.-datum"].dt.to_period("M").nunique()
        else:
            active_months = 0
        per_m = (total_next_sek / active_months) if active_months else 0.0
        st.metric("Snitt per aktiv månad (SEK)", f"{per_m:,.0f}".replace(",", " "))

    # Visa tabellen
    if df_next.empty:
        st.info("Inga framtida utdelningsdatum hittades i databasen. Fyll kolumnen **'Nästa utdelningsdatum'** per bolag för att se listan här.")
    else:
        st.dataframe(
            df_next,
            use_container_width=True,
            hide_index=True
        )

        # Månadssummering
        st.markdown("#### Månadssummering (SEK, netto)")
        dfm = (
            df_next.assign(Månad=df_next["Nästa utb.-datum"].dt.to_period("M").astype(str))
                  .groupby("Månad", as_index=False)["Utdelning netto (SEK)"].sum()
                  .sort_values("Månad")
        )
        st.dataframe(dfm, use_container_width=True, hide_index=True)

# ============================================================
# Del 5/6 — Vyer
#  • Settings & Valutakurser
#  • Snapshot
#  • Editor (manuellt + Yahoo-prefill)
#  • Lägg till ticker
#  • Portfölj (använder render_portfolio_view från Del 4)
#  • Analys (metodtabell + Fair Value som egen metodrad)
#  • Ranking (uppsida)  ← ✅ Bucket-filter tillagt
#  • Batch (massuppdatering Yahoo)
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
      • Selectbox UTAN key (index styrs av vår egen idx-state)
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
# Settings-vy + valutakurser
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

# Överskugga _withholding_for så andra vyer kan använda Settings-bladet vid behov
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
# ============================================================
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    """Bygger uppdateringsdict från Yahoo (återanvänds i Editor/Batch)."""
    y   = fetch_from_yahoo(ticker)
    est = _fetch_eps_estimates_yahoo(ticker)

    updates = {
        "Timestamp": _now(),
        # Pris/valuta/struktur
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
        # CAGR & utdelning
        "Rev CAGR": _maybe(y.get("rev_cagr_hist")),
        "EPS CAGR": _maybe(y.get("eps_cagr_hist")),
        "Årlig utdelning": _maybe(y.get("dps_annual")),
        # EPS-estimat (bevara manuellt ifyllda)
        "EPS 1Y": _maybe(est.get("eps_1y")) if pd.isna(existing_row.get("EPS 1Y")) else existing_row.get("EPS 1Y"),
        "EPS 2Y": _maybe(est.get("eps_2y")) if pd.isna(existing_row.get("EPS 2Y")) else existing_row.get("EPS 2Y"),
        # Spårning
        "Senast auto uppdaterad": _now(),
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
        # OBS: ingen valutakonvertering av EPS – användaren matar redan i bolagets valuta
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
                eps1_v  = _parse_float(eps1_in)  # ingen konvertering
                eps2_v  = _parse_float(eps2_in)  # ingen konvertering
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

            # Ingen EPS-konvertering – användarens värden är i bolagets valuta
            eps1_v  = _parse_float(eps1_in)
            eps2_v  = _parse_float(eps2_in)
            # Rev i MILJONER → multiplicera 1e6 till enheter
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
        render_portfolio_view(df, fx)  # definierad i Del 4/6
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")

# ============================================================
# Analys – huvudvy (sök + bläddra)  ➕ Bucket & Ägande-filter + Fair Value
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
    names_map = _names_map_from_df(df)  # namnmap från hela databasen

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

    # Fair Value (från meta → median över metoder)
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

# ============================================================
# 🏆 Ranking – Uppsida per horisont  ✅ Bucket-filter tillagt
# ============================================================
def page_ranking():
    st.header("🏆 Ranking – Uppsida per horisont")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    # ✅ Nytt: Bucket-filter (multiselect) – default alla
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

# ============================================================
# Batch (Massuppdatering Yahoo)
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
# Del 6/6 — Main & routing
#  • Init (DATA, FX, Settings)
#  • Sidopanel / navigering
#  • Routing till vyer (Analys, Ranking, Portfölj, Editor, Lägg till, Batch, Snapshot, Settings)
#  • Robust felhantering
# ============================================================

def _init_app_state():
    """Initiera nödvändiga objekt i session_state en gång."""
    ss = st.session_state
    if "INIT_DONE" in ss and ss["INIT_DONE"]:
        return

    # Läs in Settings / FX / DATA
    try:
        ss["SETTINGS"] = get_settings_map()
    except Exception:
        ss["SETTINGS"] = {}

    try:
        ss["FX"] = get_fx_map()
    except Exception:
        ss["FX"] = {}

    try:
        ss["DATA"] = read_data_df()
    except Exception:
        ss["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)

    # Auto-uppdatera FX vid start om aktiverat i Settings
    try:
        auto = str(ss["SETTINGS"].get("auto_refresh_on_start", "0")) == "1"
        if auto and not ss.get("FX_AUTO_RAN", False):
            _load_fx_and_update_sheet()
            ss["FX"] = get_fx_map()
            ss["FX_AUTO_RAN"] = True
    except Exception as e:
        st.sidebar.warning(f"FX auto-uppd misslyckades: {e}")

    ss["INIT_DONE"] = True


def _sidebar():
    st.sidebar.markdown("### Aktieanalys & investeringsförslag")
    st.sidebar.caption("Basvaluta/visning: **SEK** (ingen valutakonvertering av EPS)")

    # Snabbval av primär vy
    pages = [
        "Analys",
        "Ranking",
        "Portfölj",
        "Editor",
        "Lägg till",
        "Batch",
        "Snapshot",
        "Settings",
    ]
    default_page = st.session_state.get("LAST_PAGE", "Analys")
    if default_page not in pages:
        default_page = "Analys"
    choice = st.sidebar.radio("Välj vy", pages, index=pages.index(default_page))
    st.session_state["LAST_PAGE"] = choice

    st.sidebar.markdown("---")

    # Mini-info: antal rader i DATA
    try:
        df = st.session_state.get("DATA")
        n = 0 if df is None else len(df)
        st.sidebar.caption(f"DATA-rader: **{n}**")
    except Exception:
        pass

    # Snabb-KPI (om senast visade ticker i Analys finns buffrad i state)
    fv = st.session_state.get("LAST_FAIR_VALUE", None)
    if isinstance(fv, dict) and fv:
        st.sidebar.markdown("**Senaste Fair Value** (median):")
        c1, c2 = st.sidebar.columns(2)
        c1.metric("Idag", f"{_f(fv.get('today')):.2f}" if _f(fv.get('today')) is not None else "—")
        c2.metric("1 år", f"{_f(fv.get('y1')):.2f}" if _f(fv.get('y1')) is not None else "—")
        c1, c2 = st.sidebar.columns(2)
        c1.metric("2 år", f"{_f(fv.get('y2')):.2f}" if _f(fv.get('y2')) is not None else "—")
        c2.metric("3 år", f"{_f(fv.get('y3')):.2f}" if _f(fv.get('y3')) is not None else "—")

    st.sidebar.markdown("---")
    st.sidebar.caption("© 2025 — Basversion 2025-11-03 · Fair Value = median av metoder (inkl. 'Idag').")

    return choice


def _route(choice: str):
    """Skicka vidare till rätt sida."""
    if choice == "Settings":
        page_settings()
    elif choice == "Snapshot":
        page_snapshot()
    elif choice == "Editor":
        page_editor()
    elif choice == "Lägg till":
        page_add_ticker()
    elif choice == "Portfölj":
        page_portfolio()
    elif choice == "Analys":
        # Fånga FV till sidopanelens snabb-KPI
        _capture_fv = st.empty()
        before = st.session_state.get("LAST_FAIR_VALUE", None)
        page_analysis()
        after = st.session_state.get("LAST_FAIR_VALUE", None)
        # Om page_analysis satte meta fair_value i state, behåll; annars försök fiska från lokal variabel
        if after is None and before is not None:
            st.session_state["LAST_FAIR_VALUE"] = before
    elif choice == "Ranking":
        page_ranking()
    elif choice == "Batch":
        page_batch()
    else:
        page_analysis()  # fallback


def main():
    try:
        _init_app_state()
        choice = _sidebar()
        _route(choice)
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")


if __name__ == "__main__":
    main()
