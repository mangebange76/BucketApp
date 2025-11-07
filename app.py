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
# Viktigt: det finns bara EN 'from __future__ import annotations' i Del 1.
# ============================================================

from __future__ import annotations

# ---------- Standardbibliotek ----------
import os, json, math, time
from typing import Any, Dict, List, Optional
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
    För service account keys som lagts in i secrets med '\n' istället för riktiga radbrytningar.
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
# Schema – kolumner
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
    # Extra hjälp-fält för editorns gamla vyer
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
    default = 0.15 (15%) om inget hittas, enligt tidigare beteende.
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

# ===== Säkerställa att metoderna finns (fallback om inte satt senare) =====
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b",
        "ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST

# ============================================================
# Del 1/6 slut — fortsätt i Del 2/6 (datainsamling & beräkningsmotor 1/2)
# ============================================================

# ============================================================
# app.py — Del 2/6 — Datainsamling & beräkningsmotor (1/2)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS-estimat (current/next year) + långsiktig EPS-trend
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Multipel-decay, P/E-ankare, pris-/EV-byggare
#  • Hjälpare för manuella Rev 1Y/2Y (miljoner → enheter) vid behov
#
#  OBS: Ingen valutakonvertering av EPS här. Manuella EPS antas redan i
#       bolagets valuta (t.ex. NOK om bolagets valuta är NOK).
# ============================================================

# (upprepad import är OK i en enda fil; Python hanterar det idempotent)
import time, math
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

# -------------------------
# Hjälpare (index-pick, TTM-summerare)
# -------------------------
def _ix_pick(df: pd.DataFrame, candidates: list[str]):
    """Returnerar raden (Series) vars index matchar någon av kandidaterna (case/space-insensitivt)."""
    if df is None or getattr(df, "empty", True):
        return None
    norm = {str(i).replace(" ", "").lower(): i for i in df.index}
    for cand in candidates:
        key = cand.replace(" ", "").lower()
        if key in norm:
            try:
                return df.loc[norm[key]]
            except Exception:
                pass
    # Delvis match om exakt uteblev
    for k, orig in norm.items():
        for cand in candidates:
            if cand.replace(" ", "").lower() in k:
                try:
                    return df.loc[orig]
                except Exception:
                    pass
    return None

def _sum_last4(ser_like):
    """Summera de 4 senaste datapunkterna (för kvartalsserier)."""
    try:
        s = pd.to_numeric(pd.Series(ser_like), errors="coerce").dropna()
        if s.empty:
            return None
        try:
            s.index = pd.to_datetime(s.index, errors="coerce")
            s = s.sort_index()
        except Exception:
            pass
        vals = s.dropna().values.tolist()
        if len(vals) == 0:
            return None
        return float(np.nansum(vals[-4:]))
    except Exception:
        return None

def _sum_eps_last4(ser_like):
    """Summera senaste 4 kvartalens EPS (Diluted/Basic)."""
    return _sum_last4(ser_like)

# -------------------------
# Yahoo (yfinance) – robust snapshot
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> dict[str, any]:
    """
    Hämtar pris/valuta + nyckeltal från Yahoo.
    Keys (urval):
      price, currency, market_cap, ev, shares,
      revenue_ttm, ebitda_ttm, eps_ttm,
      ev_to_sales, ev_to_ebitda, pe_ttm, pe_fwd,
      p_to_book, bvps, net_debt, company_name, sector, industry,
      annual_dividend, dividend_frequency, sources={}
    """
    tk = yf.Ticker(ticker)
    out: dict[str, any] = {"sources": {}}

    # Snabbkanal
    try:
        fi = tk.fast_info
        out["price"]      = _f(getattr(fi, "last_price", None));      out["sources"]["price"] = "yahoo_fast"
        out["currency"]   = getattr(fi, "currency", None);            out["sources"]["currency"] = "yahoo_fast"
        out["market_cap"] = _f(getattr(fi, "market_cap", None));      out["sources"]["market_cap"] = "yahoo_fast"
        out["shares"]     = _f(getattr(fi, "shares", None));          out["sources"]["shares"] = "yahoo_fast"
    except Exception:
        pass

    # info()-fallbacks
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    def gi(k):
        try:
            return info.get(k)
        except Exception:
            return None

    def set_if_missing(k, val, src):
        if out.get(k) is None and val is not None:
            out[k] = _f(val) if isinstance(val, (int, float, str)) else val
            out["sources"][k] = src

    set_if_missing("price",        gi("currentPrice"),        "yahoo_info")
    set_if_missing("currency",     gi("currency"),            "yahoo_info")
    set_if_missing("market_cap",   gi("marketCap"),           "yahoo_info")
    set_if_missing("pe_ttm",       gi("trailingPE"),          "yahoo_info")
    set_if_missing("pe_fwd",       gi("forwardPE"),           "yahoo_info")
    set_if_missing("p_to_book",    gi("priceToBook"),         "yahoo_info")
    set_if_missing("bvps",         gi("bookValue"),           "yahoo_info")
    set_if_missing("eps_ttm",      gi("trailingEps"),         "yahoo_info")

    if out.get("shares") is None:
        so = _f(gi("sharesOutstanding"))
        if so is not None:
            out["shares"] = so
            out["sources"]["shares"] = "yahoo_info_sharesOutstanding"

    if out.get("pe_ttm") is None and _pos(out.get("price")) and _pos(out.get("eps_ttm")):
        try:
            out["pe_ttm"] = float(out["price"]) / float(out["eps_ttm"])
            out["sources"]["pe_ttm"] = "calc_price/eps_ttm"
        except Exception:
            pass

    if out.get("pe_fwd") is None and _pos(out.get("price")):
        fwd_eps = _f(gi("forwardEps") or gi("forwardEPS"))
        if _pos(fwd_eps):
            try:
                out["pe_fwd"] = float(out["price"]) / float(fwd_eps)
                out["sources"]["pe_fwd"] = "calc_price/forwardEPS"
            except Exception:
                pass

    try:
        cname = gi("longName") or gi("shortName")
        if cname:
            out["company_name"] = str(cname); out["sources"]["company_name"] = "yahoo_info"
        sector = gi("sector")
        if sector:
            out["sector"] = str(sector); out["sources"]["sector"] = "yahoo_info"
        industry = gi("industry")
        if industry:
            out["industry"] = str(industry); out["sources"]["industry"] = "yahoo_info"
    except Exception:
        pass

    # EV / net debt
    total_debt = _f(gi("totalDebt"))
    total_cash = _f(gi("totalCash"))
    ev_info    = _f(gi("enterpriseValue"))

    if ev_info is not None:
        out["ev"] = ev_info; out["sources"]["ev"] = "yahoo_info"
    elif _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
        out["ev"] = out["market_cap"] + total_debt - total_cash
        out["sources"]["ev"] = "calc_mc+debt-cash"

    if out.get("market_cap") is not None and out.get("ev") is not None:
        out["net_debt"] = out["ev"] - out["market_cap"]
        out["sources"]["net_debt"] = "calc_ev-mcap"

    if not _pos(out.get("shares")) and _pos(out.get("market_cap")) and _pos(out.get("price")):
        try:
            out["shares"] = out["market_cap"] / out["price"]
            out["sources"]["shares"] = "derived_mcap/price"
        except Exception:
            pass

    if not _pos(out.get("price")):
        try:
            hist = tk.history(period="5d")
            if not hist.empty:
                out["price"] = float(hist["Close"].dropna().iloc[-1])
                out["sources"]["price"] = "yahoo_hist_close"
        except Exception:
            pass

    # Balance Sheet-fallbacks
    bs_q = None
    try:
        bs_q = tk.get_balance_sheet(freq="quarterly")
    except Exception:
        bs_q = getattr(tk, "quarterly_balance_sheet", None) or getattr(tk, "balance_sheet", None)

    if bs_q is not None and not getattr(bs_q, "empty", True):
        debt_row = _ix_pick(bs_q, ["TotalDebt", "Total Debt", "ShortLongTermDebtTotal"])
        cash_row = _ix_pick(bs_q, ["CashAndCashEquivalents", "Cash And Cash Equivalents", "CashAndShortTermInvestments"])
        if total_debt is None and debt_row is not None:
            try:
                total_debt = float(pd.to_numeric(pd.Series(debt_row), errors="coerce").dropna().iloc[-1])
                out["sources"]["_total_debt_fallback"] = "balance_sheet_q"
            except Exception:
                pass
        if total_cash is None and cash_row is not None:
            try:
                total_cash = float(pd.to_numeric(pd.Series(cash_row), errors="coerce").dropna().iloc[-1])
                out["sources"]["_total_cash_fallback"] = "balance_sheet_q"
            except Exception:
                pass
        if out.get("ev") is None and _pos(out.get("market_cap")) is not None and total_debt is not None and total_cash is not None:
            out["ev"] = out["market_cap"] + total_debt - total_cash
            out["sources"]["ev"] = "calc_mc+debt-cash(bs)"
        if out.get("net_debt") is None and out.get("ev") is not None and out.get("market_cap") is not None:
            out["net_debt"] = out["ev"] - out["market_cap"]
            out["sources"]["net_debt"] = "calc_ev-mcap(bs)"
        eq_row = _ix_pick(bs_q, ["StockholdersEquity", "TotalStockholderEquity", "Total Stockholder Equity"])
        if out.get("bvps") is None and eq_row is not None and _pos(out.get("shares")):
            try:
                eq_last = float(pd.to_numeric(pd.Series(eq_row), errors="coerce").dropna().iloc[-1])
                out["bvps"] = eq_last / float(out["shares"])
                out["sources"]["bvps"] = "calc_equity/shares(balance_sheet_q)"
            except Exception:
                pass
        if out.get("p_to_book") is None and _pos(out.get("price")) and _pos(out.get("bvps")):
            try:
                out["p_to_book"] = float(out["price"]) / float(out["bvps"])
                out["sources"]["p_to_book"] = "calc_price/bvps"
            except Exception:
                pass

    # TTM via kvartal (income statement quarterly)
    EPS_KEYS_Q     = ["DilutedEPS", "BasicEPS", "EPS"]
    REV_KEYS_Q     = ["TotalRevenue", "Total Revenue", "Revenue"]
    EBITDA_KEYS_Q  = ["Ebitda", "EBITDA", "EarningsBeforeInterestTaxesDepreciationAmortization"]

    try:
        try:
            inc_q = tk.get_income_stmt(freq="quarterly")
        except Exception:
            inc_q = getattr(tk, "quarterly_income_stmt", None) or getattr(tk, "income_stmt", None)

        if inc_q is not None and not getattr(inc_q, "empty", True):
            dfq = inc_q.copy()

            eps_row = _ix_pick(dfq, EPS_KEYS_Q)
            eps_ttm_q = _sum_eps_last4(eps_row) if eps_row is not None else None

            rev_row = _ix_pick(dfq, REV_KEYS_Q)
            rev_ttm_q = _sum_last4(rev_row) if rev_row is not None else None

            ebitda_row = _ix_pick(dfq, EBITDA_KEYS_Q)
            ebitda_ttm_q = _sum_last4(ebitda_row) if ebitda_row is not None else None

            if eps_ttm_q is None:
                # Fallback EPS via NetIncome / DilutedShares, TTM
                net_row = _ix_pick(dfq, ["NetIncome", "Net Income", "NetIncomeApplicableToCommonShares", "NetIncomeCommonStockholders"])
                shd_row = _ix_pick(dfq, ["DilutedAverageShares", "Diluted Shares", "AverageDilutedSharesOutstanding", "WeightedAverageDilutedSharesOutstanding"])
                if net_row is not None and shd_row is not None:
                    ni_ttm = _sum_last4(net_row)
                    sh_ttm = _sum_last4(shd_row)
                    if _pos(ni_ttm) is not None and _pos(sh_ttm) is not None and sh_ttm != 0:
                        eps_ttm_q = float(ni_ttm) / float(sh_ttm)

            if _pos(eps_ttm_q) is not None:
                out["eps_ttm"] = float(eps_ttm_q)
                out["sources"]["eps_ttm"] = "yahoo_quarterly_TTM"

            if _pos(rev_ttm_q) is not None:
                out["revenue_ttm"] = float(rev_ttm_q)
                out["sources"]["revenue_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")

            if ebitda_ttm_q is not None:
                out["ebitda_ttm"] = float(ebitda_ttm_q)
                out["sources"]["ebitda_ttm"] = "yahoo_quarterly_TTM"
            else:
                set_if_missing("ebitda_ttm", gi("ebitda"), "yahoo_info")
    except Exception:
        set_if_missing("revenue_ttm", gi("totalRevenue"), "yahoo_info")
        set_if_missing("ebitda_ttm",  gi("ebitda"),        "yahoo_info")

    if _pos(out.get("ev")) and _pos(out.get("revenue_ttm")):
        try:
            out["ev_to_sales"] = float(out["ev"]) / float(out["revenue_ttm"])
            out["sources"]["ev_to_sales"] = out["sources"].get("revenue_ttm", "calc_ev/sales")
        except Exception:
            pass

    if _pos(out.get("ev")) and out.get("ebitda_ttm") is not None:
        try:
            e = float(out["ebitda_ttm"])
            out["ev_to_ebitda"] = (float(out["ev"]) / e) if e != 0 else None
            out["sources"]["ev_to_ebitda"] = out["sources"].get("ebitda_ttm", "calc_ev/ebitda")
        except Exception:
            pass

    # Utdelning (annual rate + enkel frekvensgissning)
    fwd_div = _f(gi("dividendRate") or gi("forwardAnnualDividendRate"))
    trl_div = _f(gi("trailingAnnualDividendRate"))
    if out.get("annual_dividend") is None and (fwd_div is not None or trl_div is not None):
        out["annual_dividend"] = float(_nz(fwd_div, trl_div))
        out["sources"]["annual_dividend"] = "yahoo_info"

    try:
        divs = None
        try:
            divs = tk.get_dividends()
        except Exception:
            divs = getattr(tk, "dividends", None)
        if divs is not None and hasattr(divs, "index") and len(divs) > 0:
            last12 = divs[divs.index >= (pd.Timestamp.today() - pd.Timedelta(days=370))]
            n = int(len(last12))
            freq = None
            if n >= 10:   freq = "M"
            elif n >= 3:  freq = "Q"
            elif n == 2:  freq = "S"
            elif n == 1:  freq = "A"
            if freq:
                out["dividend_frequency"] = freq
                out["sources"]["dividend_frequency"] = "yahoo_dividends_infer"
    except Exception:
        pass

    out["currency"] = str(out.get("currency") or "USD").upper()
    return out

# -------------------------
# Yahoo – EPS-estimat (trend)
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> dict[str, float | None]:
    """
    Plockar EPS currentYear/nextYear från Yahoo earnings trend.
    Returnerar: {"eps_1y": float|None, "eps_2y": float|None,
                 "eps_cagr_long": float|None, "source": "..."}
    """
    try:
        tk = yf.Ticker(ticker)
        try:
            trend = tk.get_earnings_trend()
        except Exception:
            trend = getattr(tk, "earnings_trend", None)

        if trend is None or (hasattr(trend, "empty") and trend.empty):
            return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

        df = trend.copy()
        df.columns = [str(c).lower() for c in df.columns]

        def _avg_from_cell(val) -> float | None:
            if isinstance(val, dict):
                for k in ("avg", "average", "mean"):
                    if k in val and _f(val[k]) is not None:
                        return _f(val[k])
            return _f(val)

        def _pick_row(period_aliases: list[str]):
            if "period" not in df.columns:
                return None
            m = df["period"].astype(str).str.lower()
            mask = None
            for alias in period_aliases:
                a = m.str.contains(rf"^{alias}$")
                mask = a if mask is None else (mask | a)
            sub = df[mask] if mask is not None else pd.DataFrame()
            return sub.iloc[0] if not sub.empty else None

        row_nextyear    = _pick_row(["nextyear", "next fiscal year", "nextfiscalyear"])
        row_longterm    = _pick_row(["longterm", "next5years", "next 5 years"])
        row_currentyear = _pick_row(["currentyear", "current fiscal year", "currentfiscalyear"])

        eps_1y = None
        if row_nextyear is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg_from_cell(row_nextyear.get(col))
                    if eps_1y is not None:
                        break
        if eps_1y is None and row_currentyear is not None:
            for col in ["earningsestimate", "epsestimate", "epstrend"]:
                if col in df.columns:
                    eps_1y = _avg_from_cell(row_currentyear.get(col))
                    if eps_1y is not None:
                        break

        eps_cagr_long = None
        if row_longterm is not None:
            for col in ["growth", "longtermgrowthrate"]:
                if col in df.columns:
                    eps_cagr_long = None if _f(row_longterm.get(col)) is None else float(_f(row_longterm.get(col)))
                    if eps_cagr_long is not None:
                        break

        eps_2y = None
        if _pos(eps_1y) and eps_cagr_long is not None:
            eps_2y = float(eps_1y) * (1.0 + float(eps_cagr_long))

        return {"eps_1y": eps_1y, "eps_2y": eps_2y, "eps_cagr_long": eps_cagr_long, "source": "yahoo_trend"}
    except Exception:
        return {"eps_1y": None, "eps_2y": None, "eps_cagr_long": None, "source": "none"}

# -------------------------
# Yahoo – 5-års historisk CAGR för Revenue
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    """CAGR på intäkter från årliga statements (Yahoo), sista 3–5 år."""
    try:
        tk = yf.Ticker(ticker)
        try:
            inc = tk.get_income_stmt(freq="annual")
        except Exception:
            inc = getattr(tk, "income_stmt", None)

        if inc is None or (hasattr(inc, "empty") and inc.empty):
            return {"rev_cagr": None, "years": None, "source": "none"}

        df = inc.copy()
        total_rev = _ix_pick(df, ["TotalRevenue", "Total Revenue", "Revenue"])
        if total_rev is None:
            return {"rev_cagr": None, "years": None, "source": "none"}

        ser = pd.to_numeric(pd.Series(total_rev).dropna(), errors="coerce").dropna()
        if ser.empty:
            return {"rev_cagr": None, "years": None, "source": "none"}

        try:
            ser.index = pd.to_datetime(ser.index, errors="coerce")
            ser = ser.sort_index()
        except Exception:
            pass

        vals = ser.dropna().values.tolist()
        if len(vals) < 2:
            return {"rev_cagr": None, "years": None, "source": "yahoo_financials"}

        n = min(max_years, len(vals))
        vals = vals[-n:]
        n_years = len(vals) - 1
        if n_years < 1 or n_years < min_years-1:
            return {"rev_cagr": None, "years": len(vals), "source": "yahoo_financials"}

        try:
            cagr = (vals[-1] / vals[0]) ** (1.0 / n_years) - 1.0
        except Exception:
            cagr = None

        return {"rev_cagr": cagr, "years": n_years, "source": "yahoo_financials"}
    except Exception:
        return {"rev_cagr": None, "years": None, "source": "none"}

# -------------------------
# Yahoo – 5-års historisk CAGR för EPS (årliga rapporter)
# -------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_eps_cagr_hist(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    """
    Beräknar EPS (diluted/basic) årligen och CAGR över 3–5 år (senaste tillgängliga),
    fallback: NetIncome / DilutedAverageShares om EPS-rad saknas.
    """
    try:
        tk = yf.Ticker(ticker)
        try:
            inc = tk.get_income_stmt(freq="annual")
        except Exception:
            inc = getattr(tk, "income_stmt", None)

        if inc is None or (hasattr(inc, "empty") and inc.empty):
            return {"eps_cagr": None, "years": None, "source": "none"}

        df = inc.copy()
        eps_row = _ix_pick(df, ["DilutedEPS", "BasicEPS", "EPS"])

        if eps_row is None:
            ni_row = _ix_pick(df, ["NetIncome", "Net Income", "NetIncomeApplicableToCommonShares", "NetIncomeCommonStockholders"])
            sh_row = _ix_pick(df, ["DilutedAverageShares", "Diluted Shares", "AverageDilutedSharesOutstanding", "WeightedAverageDilutedSharesOutstanding"])
            if ni_row is None or sh_row is None:
                return {"eps_cagr": None, "years": None, "source": "none"}
            ni = pd.to_numeric(pd.Series(ni_row), errors="coerce")
            sh = pd.to_numeric(pd.Series(sh_row), errors="coerce")
            eps_series = (ni / sh).replace([np.inf, -np.inf], np.nan).dropna()
        else:
            eps_series = pd.to_numeric(pd.Series(eps_row), errors="coerce").dropna()

        if eps_series.empty:
            return {"eps_cagr": None, "years": None, "source": "none"}

        try:
            eps_series.index = pd.to_datetime(eps_series.index, errors="coerce")
            eps_series = eps_series.sort_index()
        except Exception:
            pass

        vals = eps_series.dropna().values.tolist()
        if len(vals) < 2:
            return {"eps_cagr": None, "years": None, "source": "yahoo_financials"}

        n = min(max_years, len(vals))
        vals = vals[-n:]
        n_years = len(vals) - 1
        if n_years < 1 or n_years < min_years-1:
            return {"eps_cagr": None, "years": len(vals), "source": "yahoo_financials"}

        try:
            cagr = (vals[-1] / max(1e-12, vals[0])) ** (1.0 / n_years) - 1.0
        except Exception:
            cagr = None

        return {"eps_cagr": cagr, "years": n_years, "source": "yahoo_financials"}
    except Exception:
        return {"eps_cagr": None, "years": None, "source": "none"}

# -------------------------
# Multipel-decay & P/E-ankare
# -------------------------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

def _clamp(val: float | None, lo: float, hi: float) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        if not math.isfinite(v):
            return None
        return max(lo, min(hi, v))
    except Exception:
        return None

def _decay_multiple(mult0: float | None, years: int, decay: float, floor_frac: float = 0.60) -> float | None:
    m0 = _pos(mult0)
    if m0 is None:
        return None
    m = m0 * (1.0 - decay * years)
    floor = m0 * floor_frac
    return max(m, floor)

def _pe_anchor(pe_ttm: float | None, pe_fwd: float | None, w_ttm: float) -> float | None:
    pt = _pos(pe_ttm)
    pf = _pos(pe_fwd)
    if pt is None and pf is None:
        return None
    if pt is None:
        return pf
    if pf is None:
        return pt
    return w_ttm * pt + (1.0 - w_ttm) * pf

# -------------------------
# Builders (pris/EV)
# -------------------------
def _equity_price_from_ev(ev_target: float | None, net_debt: float | None, shares_fd: float | None) -> float | None:
    e = _pos(ev_target)
    s = _pos(shares_fd)
    if e is None or s is None:
        return None
    nd = _nz(net_debt, 0.0)
    try:
        return max(0.0, (e - nd) / s)
    except Exception:
        return None

def _price_from_pe(eps: float | None, pe: float | None) -> float | None:
    e = _pos(eps)
    p = _pos(pe)
    if e is None or p is None:
        return None
    return e * p

def _ev_from_sales(rev: float | None, mult: float | None) -> float | None:
    r = _pos(rev)
    m = _pos(mult)
    if r is None or m is None:
        return None
    return r * m

def _ev_from_ebitda(ebitda: float | None, mult: float | None) -> float | None:
    e = _f(ebitda)  # får vara negativ/0
    m = _pos(mult)
    if e is None or m is None:
        return None
    return e * m

def _price_from_pb(pb: float | None, bvps: float | None) -> float | None:
    p = _pos(pb)
    b = _pos(bvps)
    if p is None or b is None:
        return None
    return p * b

# -------------------------
# EPS/REV-manualhjälp (miljoner → enheter)
# -------------------------
def _rev_manual_to_units_autosense(v: float | None, rev_ttm_hint: float | None) -> float | None:
    """
    Tolkar manuellt värde som 'miljoner' ELLER 'redan i enheter'.
    Strategi:
      • Om rev_ttm_hint finns: välj det tolkade värde (x eller x*1e6) som ligger närmast rev_ttm_hint.
      • Annars: heuristik — om x >= 1e8: tolka som redan i enheter; om x <= 1e7: tolka som 'miljoner' → x*1e6.
    """
    x = _f(v)
    if x is None:
        return None
    if _pos(rev_ttm_hint):
        as_is = x
        as_mn = x * 1_000_000.0
        try:
            r1 = abs(math.log(max(1e-12, as_is / rev_ttm_hint)))
            r2 = abs(math.log(max(1e-12, as_mn / rev_ttm_hint)))
            return as_is if r1 <= r2 else as_mn
        except Exception:
            pass
    # Fallback-heuristik
    if x >= 1e8:
        return x
    if x <= 1e7:
        return x * 1_000_000.0
    # Ambiguöst → anta miljoner (säkrare för 8 810 → 8.81B)
    return x * 1_000_000.0

# (legacy-namn för bakåtkompatibilitet)
def _rev_million_to_units(v: float | None) -> float | None:
    return _rev_manual_to_units_autosense(v, None)

# ============================================================
# Del 2/6 slut — fortsätt i Del 3/6 (compute_methods_for_row)
# ============================================================

# ============================================================
# app.py — Del 3/6 — Datainsamling & beräkningsmotor (2/2)
#  • Säkra hjälpfunktioner (_f, _nz, _pos) om de saknas
#  • compute_methods_for_row(): beräknar riktkurser (idag/1/2/3 år)
#    och bull/bear 1 år, väljer metod (PE-ankare vs EV/S fallback)
#  • Returnerar exakt 3 objekt: (targets_flat, meta, debug_rows)
# ============================================================

import math, time
import numpy as np
import pandas as pd
import streamlit as st

# -------- Säkra hjälpfunktioner (definieras om de inte redan finns) --------
if "_f" not in globals():
    def _f(x):
        """Försök float(x); None vid fel. Bevarar 0.0."""
        try:
            if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
                return None
            return float(x)
        except Exception:
            return None

if "_nz" not in globals():
    def _nz(x, default=0.0):
        """x om definierat annars default."""
        v = _f(x)
        return default if v is None else v

if "_pos" not in globals():
    def _pos(x):
        """
        Returnera float(x) om x är definierat och ändligt.
        Tillåter 0.0 (ej 'positivt' i strikt mening men användbart i kalkyler).
        """
        v = _f(x)
        if v is None:
            return None
        if not math.isfinite(v):
            return None
        return v

# ---- Import från Del 2/6 (finns redan i filen) ----
# fetch_yahoo_snapshot, fetch_yahoo_eps_estimates, fetch_yahoo_rev_cagr,
# fetch_yahoo_eps_cagr_hist, _pe_anchor, _decay_multiple, _equity_price_from_ev,
# _price_from_pe, _ev_from_sales, _price_from_pb, REV_CAGR_MIN/MAX, EPS_CAGR_MIN/MAX
# (Alla definierade tidigare i filen.)

# ------------------------------
# Metodval & multipelparametrar
# ------------------------------
PE_DECAY_PER_YEAR      = 0.10  # 10% multipelkontraktion årligen (golv = 60% av start)
MULT_FLOOR_FRACTION    = 0.60
EVS_DECAY_PER_YEAR     = 0.08
PB_DECAY_PER_YEAR      = 0.06

BULL_MULT_UP   = 0.20  # +20% mot basmultipel
BEAR_MULT_DOWN = 0.20  # -20% mot basmultipel
BEAR_EPS_DOWN  = 0.20  # -20% EPS i bear 1y

def _cap_growth(val: float | None, lo: float, hi: float) -> float | None:
    if val is None:
        return None
    try:
        return max(lo, min(hi, float(val)))
    except Exception:
        return None

def _extend_eps(eps_1y, eps_2y, eps_cagr_long):
    """Skatta EPS väg framåt (2–3 år). eps_2y kan komma från trend; år 3 byggs via long-CAGR."""
    e1 = _pos(eps_1y)
    e2 = _pos(eps_2y)
    cg = _cap_growth(eps_cagr_long, EPS_CAGR_MIN, EPS_CAGR_MAX)
    if e2 is None and e1 is not None and cg is not None:
        e2 = e1 * (1.0 + cg)
    e3 = None
    if e2 is not None:
        e3 = e2 * (1.0 + _nz(cg, 0.0))
    return e1, e2, e3

def _extend_rev(rev_ttm, rev_cagr_hist):
    """Skatta Revenue väg framåt (1–3 år) från TTM och historisk CAGR."""
    r0 = _pos(rev_ttm)
    cg = _cap_growth(rev_cagr_hist, REV_CAGR_MIN, REV_CAGR_MAX)
    if r0 is None:
        return None, None, None, None
    r1 = r0 * (1.0 + _nz(cg, 0.0))
    r2 = r1 * (1.0 + _nz(cg, 0.0))
    r3 = r2 * (1.0 + _nz(cg, 0.0))
    return r0, r1, r2, r3

def _decay_series(mult0, years, per_year, floor_frac):
    """Returnerar multipel efter 'years' med linjär decay och golv."""
    arr = []
    for y in range(years + 1):
        arr.append(_decay_multiple(mult0, y, per_year, floor_frac))
    return arr  # [m0, m1, m2, m3]

def _choose_method(snapshot: dict) -> str:
    """Heuristik för metodval utan prisankring."""
    sector = str(snapshot.get("sector") or "").lower()
    industry = str(snapshot.get("industry") or "").lower()
    evs = _pos(snapshot.get("ev_to_sales"))
    pef = _pos(snapshot.get("pe_fwd"))
    pt  = _pos(snapshot.get("pe_ttm"))
    pb  = _pos(snapshot.get("p_to_book"))

    # Banker/finans → P/B
    if any(k in sector for k in ["financial", "bank"]):
        if pb:
            return "p_b"
    # Software/plattform/SaaS → EV/S om EV/S finns, annars P/E
    if any(k in industry for k in ["software", "saas", "application", "cloud"]):
        if evs:
            return "ev_sales"
        if pef or pt:
            return "pe_anchor"
    # Om P/E-data bra → pe_anchor
    if pef or pt:
        return "pe_anchor"
    # Om EV/S finns → ev_sales
    if evs:
        return "ev_sales"
    # Fallback → p_b om det finns
    if pb:
        return "p_b"
    # Sista utväg
    return "pe_anchor"

def _targets_from_pe(snapshot, eps1, eps2, eps3):
    pe0 = _pe_anchor(snapshot.get("pe_ttm"), snapshot.get("pe_fwd"), w_ttm=0.7)
    if pe0 is None:
        return None
    m0, m1, m2, m3 = _decay_series(pe0, 3, PE_DECAY_PER_YEAR, MULT_FLOOR_FRACTION)

    # Baslinje: använd närmaste EPS för respektive horisont
    p0 = _price_from_pe(_pos(snapshot.get("eps_ttm")) or eps1, m0)
    p1 = _price_from_pe(eps1, m1)
    p2 = _price_from_pe(eps2, m2)
    p3 = _price_from_pe(eps3, m3)

    # Bull/Bear 1y via multipel +/- och EPS stress
    bull1 = _price_from_pe(eps1, None if m1 is None else (m1 * (1.0 + BULL_MULT_UP)))
    bear1 = _price_from_pe(None if eps1 is None else (eps1 * (1.0 - BEAR_EPS_DOWN)),
                           None if m1 is None else (m1 * (1.0 - BEAR_MULT_DOWN)))

    return {
        "target_today": p0, "target_1y": p1, "target_2y": p2, "target_3y": p3,
        "bull_1y": bull1, "bear_1y": bear1, "method": "pe_anchor"
    }

def _targets_from_ev_sales(snapshot, r0, r1, r2, r3):
    evs0 = _pos(snapshot.get("ev_to_sales"))
    if evs0 is None:
        return None
    m0, m1, m2, m3 = _decay_series(evs0, 3, EVS_DECAY_PER_YEAR, MULT_FLOOR_FRACTION)
    ev0 = _ev_from_sales(r0, m0)
    ev1 = _ev_from_sales(r1, m1)
    ev2 = _ev_from_sales(r2, m2)
    ev3 = _ev_from_sales(r3, m3)

    p0 = _equity_price_from_ev(ev0, snapshot.get("net_debt"), snapshot.get("shares"))
    p1 = _equity_price_from_ev(ev1, snapshot.get("net_debt"), snapshot.get("shares"))
    p2 = _equity_price_from_ev(ev2, snapshot.get("net_debt"), snapshot.get("shares"))
    p3 = _equity_price_from_ev(ev3, snapshot.get("net_debt"), snapshot.get("shares"))

    bull1 = None
    bear1 = None
    if _pos(r1) and m1 is not None:
        bull1 = _equity_price_from_ev(_ev_from_sales(r1, m1 * (1.0 + BULL_MULT_UP)),
                                      snapshot.get("net_debt"), snapshot.get("shares"))
        bear1 = _equity_price_from_ev(_ev_from_sales(r1, m1 * (1.0 - BEAR_MULT_DOWN)),
                                      snapshot.get("net_debt"), snapshot.get("shares"))

    return {
        "target_today": p0, "target_1y": p1, "target_2y": p2, "target_3y": p3,
        "bull_1y": bull1, "bear_1y": bear1, "method": "ev_sales"
    }

def _targets_from_p_b(snapshot):
    pb0 = _pos(snapshot.get("p_to_book"))
    bv  = _pos(snapshot.get("bvps"))
    if pb0 is None or bv is None:
        return None
    m0, m1, m2, m3 = _decay_series(pb0, 3, PB_DECAY_PER_YEAR, MULT_FLOOR_FRACTION)
    p0 = _price_from_pb(m0, bv)
    p1 = _price_from_pb(m1, bv)
    p2 = _price_from_pb(m2, bv)
    p3 = _price_from_pb(m3, bv)

    bull1 = None if m1 is None else _price_from_pb(m1 * (1.0 + BULL_MULT_UP), bv)
    bear1 = None if m1 is None else _price_from_pb(m1 * (1.0 - BEAR_MULT_DOWN), bv)
    return {
        "target_today": p0, "target_1y": p1, "target_2y": p2, "target_3y": p3,
        "bull_1y": bull1, "bear_1y": bear1, "method": "p_b"
    }

def _flatten_targets(d: dict | None) -> dict:
    """Säker flattning + avrundning."""
    if not d:
        return {}
    out = {}
    for k in ["target_today", "target_1y", "target_2y", "target_3y", "bull_1y", "bear_1y"]:
        v = d.get(k)
        out[k] = None if v is None else float(v)
    out["method"] = d.get("method")
    return out

# ------------------------------
# Huvud: compute_methods_for_row
# ------------------------------
def compute_methods_for_row(
    ticker: str,
    *,
    manual: dict | None = None,
    prefer_method: str | None = None
):
    """
    Beräknar riktkurser för en ticker (idag/1/2/3 år) och bull/bear 1 år.
    Väljer metod heuristiskt (eller tvingad via prefer_method).
    Returnerar exakt en 3-tuple:
        (targets_flat: dict, meta: dict, debug_rows: list[tuple[str, str, float|str|None]])

    targets_flat keys:
        target_today, target_1y, target_2y, target_3y, bull_1y, bear_1y, method

    meta innehåller snapshot, antagna EPS/REV, multiplar m.m. för spårbarhet.
    debug_rows är en enkel lista (nyckel, källa, värde) för UI-tabell.
    """
    t0 = time.time()
    manual = manual or {}

    # --- 1) Snapshot + uppskattningar ---
    snap = fetch_yahoo_snapshot(ticker)
    eps_tr = fetch_yahoo_eps_estimates(ticker)
    rev_h  = fetch_yahoo_rev_cagr(ticker)
    eps_h  = fetch_yahoo_eps_cagr_hist(ticker)

    # --- 2) Antaganden (manuella overrides respekteras) ---
    # EPS TTM/1y/2y/3y
    eps_ttm = _pos(manual.get("eps_ttm")) or _pos(snap.get("eps_ttm"))
    eps_1y  = _pos(manual.get("eps_1y"))  or _pos(eps_tr.get("eps_1y"))
    eps_2y  = _pos(manual.get("eps_2y"))  or _pos(eps_tr.get("eps_2y"))
    eps_cagr_long = manual.get("eps_cagr_long")
    if eps_cagr_long is None:
        eps_cagr_long = eps_tr.get("eps_cagr_long")
    eps_cagr_long = _cap_growth(_f(eps_cagr_long), EPS_CAGR_MIN, EPS_CAGR_MAX)
    eps_1y, eps_2y, eps_3y = _extend_eps(eps_1y, eps_2y, eps_cagr_long)
    if eps_ttm is not None and eps_1y is None:
        eps_1y = eps_ttm  # fallback

    # Revenue TTM & tillväxt
    rev_ttm = _pos(manual.get("revenue_ttm")) or _pos(snap.get("revenue_ttm"))
    rev_cagr_hist = manual.get("rev_cagr") if manual.get("rev_cagr") is not None else rev_h.get("rev_cagr")
    rev_cagr_hist = _cap_growth(_f(rev_cagr_hist), REV_CAGR_MIN, REV_CAGR_MAX)
    r0, r1, r2, r3 = _extend_rev(rev_ttm, rev_cagr_hist)

    # --- 3) Metodval ---
    method = prefer_method or _choose_method(snap)

    # --- 4) Targets enligt metod + robust fallbackkedja ---
    targets = None
    tried = []
    if method == "pe_anchor":
        tried.append("pe_anchor")
        targets = _targets_from_pe({"pe_ttm": snap.get("pe_ttm"),
                                    "pe_fwd": snap.get("pe_fwd"),
                                    "eps_ttm": eps_ttm},
                                   eps_1y, eps_2y, eps_3y)
        if targets is None:
            tried.append("ev_sales")
            targets = _targets_from_ev_sales(snap, r0, r1, r2, r3)
        if targets is None:
            tried.append("p_b")
            targets = _targets_from_p_b(snap)
    elif method == "ev_sales":
        tried.append("ev_sales")
        targets = _targets_from_ev_sales(snap, r0, r1, r2, r3)
        if targets is None:
            tried.append("pe_anchor")
            targets = _targets_from_pe({"pe_ttm": snap.get("pe_ttm"),
                                        "pe_fwd": snap.get("pe_fwd"),
                                        "eps_ttm": eps_ttm},
                                       eps_1y, eps_2y, eps_3y)
        if targets is None:
            tried.append("p_b")
            targets = _targets_from_p_b(snap)
    else:  # p_b eller okänt
        tried.append("p_b")
        targets = _targets_from_p_b(snap)
        if targets is None:
            tried.append("pe_anchor")
            targets = _targets_from_pe({"pe_ttm": snap.get("pe_ttm"),
                                        "pe_fwd": snap.get("pe_fwd"),
                                        "eps_ttm": eps_ttm},
                                       eps_1y, eps_2y, eps_3y)
        if targets is None:
            tried.append("ev_sales")
            targets = _targets_from_ev_sales(snap, r0, r1, r2, r3)

    # Om allt misslyckas, ge tomt paket men korrekt struktur
    if targets is None:
        targets = {
            "target_today": None, "target_1y": None, "target_2y": None, "target_3y": None,
            "bull_1y": None, "bear_1y": None, "method": method or "unknown"
        }

    # --- 5) Meta + debug ---
    targets_flat = _flatten_targets(targets)
    targets_flat["currency"]   = snap.get("currency")
    targets_flat["price_now"]  = _pos(snap.get("price"))
    targets_flat["shares_fd"]  = _pos(snap.get("shares"))
    targets_flat["net_debt"]   = _f(snap.get("net_debt"))

    meta = {
        "ticker": ticker,
        "method_requested": method,
        "method_tried_order": tried,
        "snapshot_used": {
            "price": targets_flat["price_now"],
            "currency": targets_flat["currency"],
            "mcap": _pos(snap.get("market_cap")),
            "ev": _pos(snap.get("ev")),
            "ev_to_sales": _pos(snap.get("ev_to_sales")),
            "pe_ttm": _pos(snap.get("pe_ttm")),
            "pe_fwd": _pos(snap.get("pe_fwd")),
            "p_to_book": _pos(snap.get("p_to_book")),
            "bvps": _pos(snap.get("bvps")),
            "revenue_ttm": _pos(snap.get("revenue_ttm")),
            "ebitda_ttm": _f(snap.get("ebitda_ttm")),
            "shares": targets_flat["shares_fd"],
            "net_debt": targets_flat["net_debt"],
            "sector": snap.get("sector"),
            "industry": snap.get("industry"),
        },
        "assumptions": {
            "eps_ttm": eps_ttm,
            "eps_1y": eps_1y,
            "eps_2y": eps_2y,
            "eps_3y": eps_3y,
            "eps_cagr_long": eps_cagr_long,
            "rev_ttm": rev_ttm,
            "rev_cagr_hist": rev_cagr_hist,
            "rev_1y": r1, "rev_2y": r2, "rev_3y": r3,
            "decay": {
                "pe_per_year": PE_DECAY_PER_YEAR,
                "evs_per_year": EVS_DECAY_PER_YEAR,
                "pb_per_year": PB_DECAY_PER_YEAR,
                "floor": MULT_FLOOR_FRACTION
            },
            "bull_bear": {
                "bull_mult_up": BULL_MULT_UP,
                "bear_mult_down": BEAR_MULT_DOWN,
                "bear_eps_down": BEAR_EPS_DOWN
            }
        }
    }

    debug_rows = []
    def dbg(k, src, v):
        debug_rows.append((k, src, None if v is None else float(v) if isinstance(v, (int, float, np.floating)) else v))

    # Debug snapshot
    for k in ["price", "currency", "market_cap", "ev", "ev_to_sales", "pe_ttm", "pe_fwd", "p_to_book",
              "bvps", "revenue_ttm", "ebitda_ttm", "shares", "net_debt"]:
        dbg(f"snap.{k}", "yahoo/derived", snap.get(k))
    # Debug EPS/REV
    for k, v in [("eps_ttm", eps_ttm), ("eps_1y", eps_1y), ("eps_2y", eps_2y),
                 ("eps_3y", eps_3y), ("eps_cagr_long", eps_cagr_long),
                 ("rev_ttm", rev_ttm), ("rev_cagr_hist", rev_cagr_hist),
                 ("rev_1y", r1), ("rev_2y", r2), ("rev_3y", r3)]:
        dbg(f"assumption.{k}", "model", v)
    # Debug targets
    for k in ["target_today", "target_1y", "target_2y", "target_3y", "bull_1y", "bear_1y"]:
        dbg(f"target.{k}", targets_flat.get("method") or "n/a", targets_flat.get(k))

    targets_flat["calc_ms"] = round((time.time() - t0) * 1000.0, 1)
    return targets_flat, meta, debug_rows

# ============================================================
# Del 3/6 slut — UI/Editor/Sheets följer i Del 4/6
# ============================================================

# ============================================================
# app.py — Del 4/6 — UI: Analys + Editor-koppling + Snapshot
#  • Sök + bläddra mellan bolag
#  • Kör compute_methods_for_row(ticker, manual=…) och visar riktkurser
#  • Sparar riktkurser till DATA och snapshot till SNAPSHOT
#  • Lätt editor-stöd (manuella fält läses från raden)
# ============================================================

import math
import numpy as np
import pandas as pd
import streamlit as st

# ---- Små hjälpare (definieras om de inte redan finns) ----
if "_f" not in globals():
    def _f(x):
        try:
            if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
                return None
            return float(x)
        except Exception:
            return None

if "_pos" not in globals():
    def _pos(x):
        v = _f(x)
        if v is None:
            return None
        return v

if "_nz" not in globals():
    def _nz(x, default=None):
        return x if (x is not None and not (isinstance(x, float) and math.isnan(x))) else default

def _now():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
    • Sökfält (ticker/namn), Föregående/Nästa, selectbox
    • Håller index i st.session_state[idx_key]
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
    sel_label = st.selectbox(label, labels, index=idx, key=f"{idx_key}_sel")
    sel_idx = labels.index(sel_label) if sel_label in labels else idx
    ss[idx_key] = sel_idx
    return filtered[sel_idx] if filtered else None

# ---- Extern funktioner från Del 1/6 & Del 2/6 (antagna redan definierade) ----
# read_data_df(), write_data_df(), _append_rows()
# get_settings_map(), get_fx_map()  (ej direkt använda här, men finns globalt)
# compute_methods_for_row()  (från Del 3/6)

def _row_to_manual_overrides(row: pd.Series) -> dict:
    """
    Bygger manual-override dicten till compute_methods_for_row() från Data-raden.
    Viktigt: inga valutakonverteringar görs här. Dina manuella EPS/Rev antas redan vara i bolagets valuta.
    """
    d: dict = {}
    # EPS – manuella estimat
    if "EPS 1Y" in row and _f(row["EPS 1Y"]) is not None:
        d["eps_1y"] = _f(row["EPS 1Y"])
    if "EPS 2Y" in row and _f(row["EPS 2Y"]) is not None:
        d["eps_2y"] = _f(row["EPS 2Y"])
    # EPS TTM om du lagt in manuellt (annars hämtas från Yahoo)
    if "EPS TTM" in row and _f(row["EPS TTM"]) is not None:
        d["eps_ttm"] = _f(row["EPS TTM"])
    # Revenue: vi skickar med TTM om du har manuellt värde
    if "Rev TTM" in row and _f(row["Rev TTM"]) is not None:
        d["revenue_ttm"] = _f(row["Rev TTM"])
    # CAGR (om du matat manuellt i Data-bladet)
    if "Rev CAGR" in row and _f(row["Rev CAGR"]) is not None:
        d["rev_cagr"] = _f(row["Rev CAGR"])
    if "EPS CAGR" in row and _f(row["EPS CAGR"]) is not None:
        d["eps_cagr_long"] = _f(row["EPS CAGR"])
    return d

def _save_targets_to_data(df: pd.DataFrame, ticker: str, targets: dict, method_name: str | None):
    """Sparar riktkurser + metod till DATA (i minnes-df:en; du kan skriva till Sheets med separat knapp)."""
    mask = df["Ticker"].astype(str).str.upper() == str(ticker).upper()
    if not mask.any():
        return
    idx = df.index[mask][0]
    df.at[idx, "Primär metod"]   = method_name or df.at[idx, "Primär metod"] if "Primär metod" in df.columns else method_name
    df.at[idx, "Riktkurs idag"]  = _f(targets.get("target_today"))
    df.at[idx, "Riktkurs 1 år"]  = _f(targets.get("target_1y"))
    df.at[idx, "Riktkurs 2 år"]  = _f(targets.get("target_2y"))
    df.at[idx, "Riktkurs 3 år"]  = _f(targets.get("target_3y"))

def page_analysis():
    st.header("🔬 Analys")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        try:
            df = read_data_df()
        except Exception as e:
            st.error(f"Kunde inte läsa Data-bladet: {e}")
            return
        st.session_state["DATA"] = df

    if df.empty or "Ticker" not in df.columns:
        st.info("Lägg till tickers i DATA först.")
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
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

    manual = _row_to_manual_overrides(row)

    with st.spinner("Beräknar riktkurser…"):
        targets, meta, debug_rows = compute_methods_for_row(tkr, manual=manual, prefer_method=None)

    # --- KPI-rad ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Kurs", _format_num(_pos(meta["snapshot_used"]["price"])))
    c2.metric("Riktkurs idag", _format_num(targets.get("target_today")))
    c3.metric("Riktkurs 1 år", _format_num(targets.get("target_1y")))
    c4.metric("Riktkurs 2 år", _format_num(targets.get("target_2y")))
    c5.metric("Riktkurs 3 år", _format_num(targets.get("target_3y")))

    # Uppsida
    horizon = st.selectbox("Uppsida vs", ["Idag","1 år","2 år","3 år"], index=1)
    price = _pos(meta["snapshot_used"]["price"])
    tgt = {
        "Idag": targets.get("target_today"),
        "1 år": targets.get("target_1y"),
        "2 år": targets.get("target_2y"),
        "3 år": targets.get("target_3y"),
    }.get(horizon)
    up = None
    if _pos(tgt) and _pos(price):
        up = (float(tgt) - float(price)) / float(price) * 100.0
    st.metric("Uppsida (%)", "—" if up is None else f"{up:.1f}%")

    # Bull/Bear 1y
    b1, b2 = st.columns(2)
    b1.metric("Bull 1 år", _format_num(targets.get("bull_1y")))
    b2.metric("Bear 1 år", _format_num(targets.get("bear_1y")))

    # Metodval (informativt)
    st.caption(f"Metod: **{targets.get('method') or '—'}**  ·  Valuta: **{targets.get('currency') or row.get('Valuta') or 'USD'}**  ·  Ber.tid: {targets.get('calc_ms','—')} ms")

    # Debugtabell
    if st.checkbox("Visa debugdetaljer"):
        dbg_df = pd.DataFrame(debug_rows, columns=["Nyckel","Källa/Metod","Värde"])
        st.dataframe(dbg_df, use_container_width=True)

    st.markdown("---")
    colA, colB, colC = st.columns(3)
    with colA:
        if st.button("💾 Spara riktkurser till DATA (i minnet)"):
            _save_targets_to_data(df, tkr, targets, targets.get("method"))
            st.session_state["DATA"] = df
            st.success("Riktkurser uppdaterade i sessionens DATA.")
    with colB:
        if st.button("📸 Lägg snapshot (ark)"):
            try:
                _append_rows("Snapshot", [[
                    _now(), tkr, (targets.get("currency") or row.get("Valuta") or "USD"),
                    (targets.get("method") or ""),
                    _f(targets.get("target_today")),
                    _f(targets.get("target_1y")),
                    _f(targets.get("target_2y")),
                    _f(targets.get("target_3y")),
                    None,  # Ankare PE – ej exponerad här
                    None   # Decay      – ej exponerad här
                ]])
                st.success("Snapshot tillagd.")
            except Exception as e:
                st.error(f"Kunde inte spara snapshot: {e}")
    with colC:
        if st.button("⬆️ Skriv DATA → Google Sheets"):
            try:
                write_data_df(df)
                st.success("DATA sparad till Google Sheets.")
            except Exception as e:
                st.error(f"Kunde inte skriva till Google Sheets: {e}")

    st.markdown("---")
    st.subheader("Hela databasen (ofiltererad vy)")
    st.dataframe(st.session_state["DATA"], use_container_width=True)

# ============================================================
# app.py — Del 5/6 — Navigering, Portfölj, Ranking, Editor, Batch, Settings, Snapshot + main()
#  • Kompletta vyer och main()-koppling
#  • Utnyttjar compute_methods_for_row(ticker, manual=..., prefer_method=None)
#  • Ingen konvertering av manuella EPS/Rev (antas redan i bolagets valuta)
# ============================================================

import math
import time
import numpy as np
import pandas as pd
import streamlit as st

# ---- Hjälpare (idempotenta) --------------------------------
if "_f" not in globals():
    def _f(x):
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

if "_pos" not in globals():
    def _pos(x):
        v = _f(x)
        return v if (v is not None and v > 0) else None

if "_nz" not in globals():
    def _nz(x, default=None):
        return x if (x is not None and not (isinstance(x, float) and pd.isna(x))) else default

def _now():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
    • Sökfält (ticker/namn), Föregående/Nästa, selectbox
    • Håller index i st.session_state[idx_key]
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
    sel_label = st.selectbox(label, labels, index=idx, key=f"{idx_key}_sel")
    sel_idx = labels.index(sel_label) if sel_label in labels else idx
    ss[idx_key] = sel_idx
    return filtered[sel_idx] if filtered else None

# ---- Säker last av settings/fx (om inte del 1 finns blir det default) ----
def _settings_safe() -> dict:
    try:
        return get_settings_map()
    except Exception:
        return {}

def _fx_safe() -> dict:
    try:
        return get_fx_map()
    except Exception:
        # SEK=1, övriga okända -> 1 (visar SEK rakt av)
        return {"SEK":1.0, "USD":1.0, "EUR":1.0, "NOK":1.0, "CAD":1.0}

def _fx_rate(fx_map: dict, ccy: str, base: str = "SEK") -> float:
    if not ccy:
        return 1.0
    c = str(ccy).upper()
    if c == base.upper():
        return 1.0
    v = fx_map.get(c)
    return float(v) if _pos(v) else 1.0

# ---- Manuell-override-byggare (ingen valuta-konvertering) ----
def _row_to_manual_overrides(row: pd.Series) -> dict:
    d: dict = {}
    if "EPS 1Y" in row and _f(row["EPS 1Y"]) is not None:
        d["eps_1y"] = _f(row["EPS 1Y"])
    if "EPS 2Y" in row and _f(row["EPS 2Y"]) is not None:
        d["eps_2y"] = _f(row["EPS 2Y"])
    if "EPS TTM" in row and _f(row["EPS TTM"]) is not None:
        d["eps_ttm"] = _f(row["EPS TTM"])
    if "Rev TTM" in row and _f(row["Rev TTM"]) is not None:
        d["revenue_ttm"] = _f(row["Rev TTM"])
    if "Rev 1Y" in row and _f(row["Rev 1Y"]) is not None:
        d["rev_1y_units"] = _f(row["Rev 1Y"])
    if "Rev 2Y" in row and _f(row["Rev 2Y"]) is not None:
        d["rev_2y_units"] = _f(row["Rev 2Y"])
    if "Rev CAGR" in row and _f(row["Rev CAGR"]) is not None:
        d["rev_cagr"] = _f(row["Rev CAGR"])
    if "EPS CAGR" in row and _f(row["EPS CAGR"]) is not None:
        d["eps_cagr_long"] = _f(row["EPS CAGR"])
    return d

# ============================================================
# Settings-vy
# ============================================================
def page_settings():
    st.header("⚙️ Settings")
    s = _settings_safe()
    st.json(s, expanded=False)

    st.markdown("---")
    st.subheader("Valutakurser (SEK per 1)")
    try:
        st.dataframe(_read_df("Valutakurser"), use_container_width=True)
    except Exception:
        fx = _fx_safe()
        st.write(pd.DataFrame(list(fx.items()), columns=["Valuta","SEK_per_1"]))

# ============================================================
# Snapshot-vy
# ============================================================
def page_snapshot():
    st.header("🕒 Snapshot")
    try:
        snap = _read_df("Snapshot")
        if snap.empty:
            st.info("Inga snapshots ännu.")
        else:
            st.dataframe(snap, use_container_width=True)
    except Exception as e:
        st.error(f"Kunde inte läsa Snapshot: {e}")

# ============================================================
# Portfölj-vy (SEK) + Nästa utdelningslista (enkel inferrad via Yahoo)
# ============================================================
def _ensure_price_via_compute(ticker: str) -> float | None:
    try:
        targets, meta, _dbg = compute_methods_for_row(ticker, manual={}, prefer_method=None)
        return _pos(_nz(meta.get("snapshot_used", {}).get("price"), None))
    except Exception:
        return None

@st.cache_data(ttl=1200, show_spinner=False)
def _yf_dividends_simple(ticker: str):
    import yfinance as yf
    try:
        s = yf.Ticker(ticker).dividends
        if s is None or len(s) == 0:
            return None
        s = pd.Series(s).dropna()
        s.index = pd.to_datetime(s.index, errors="coerce")
        return s.dropna().sort_index()
    except Exception:
        return None

def _infer_next_div_simple(ticker: str):
    s = _yf_dividends_simple(ticker)
    if s is None or s.empty:
        return None, None, "?"
    last_amt = float(s.iloc[-1])
    last_dt  = pd.Timestamp(s.index[-1])
    # grov frekvens
    recent = s[s.index >= (pd.Timestamp.today() - pd.Timedelta(days=370))]
    n = len(recent)
    if n >= 10:
        step, hint = 30, "M"
    elif n >= 3:
        step, hint = 90, "Q"
    elif n == 2:
        step, hint = 182, "S"
    else:
        step, hint = 365, "A"
    nxt = last_dt + pd.Timedelta(days=step)
    today = pd.Timestamp.today().normalize()
    while nxt.normalize() <= today:
        nxt += pd.Timedelta(days=step)
    return nxt.date(), last_amt, hint

def page_portfolio():
    st.header("📊 Portfölj (SEK-baserad)")
    try:
        df = st.session_state.get("DATA") or read_data_df()
        st.session_state["DATA"] = df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        return

    if df.empty:
        st.info("DATA-bladet är tomt.")
        return

    fx_map = _fx_safe()
    rows = []
    tot_mv = tot_cost = tot_div_y = 0.0

    for _, r in df.iterrows():
        try:
            qty = _pos(r.get("Antal aktier")) or 0.0
            if qty <= 0: 
                continue
            tkr = str(r.get("Ticker") or "").strip()
            ccy = str(_nz(r.get("Valuta"), "USD")).upper()
            fx  = _fx_rate(fx_map, ccy, base="SEK")
            price = _pos(r.get("Aktuell kurs"))
            if price is None:
                price = _ensure_price_via_compute(tkr)  # hämtar via Yahoo under huven
            if price is None:
                continue
            gav_sek = _f(r.get("GAV (SEK)")) or 0.0

            mv_sek   = float(price) * float(qty) * float(fx)
            cost_sek = float(gav_sek) * float(qty)
            pl_sek   = mv_sek - cost_sek
            pl_pct   = (pl_sek / cost_sek * 100.0) if cost_sek > 0 else None

            # Årlig utdelning (brutto – enkel; källskatt hanteras ev. i Settings i Del 1)
            annual_ps = _pos(r.get("Årlig utdelning"))
            div_y_sek = (annual_ps or 0.0) * float(qty) * float(fx)

            rows.append({
                "Ticker": tkr,
                "Valuta": ccy,
                "Antal": qty,
                "FX(→SEK)": fx,
                "Kurs": price,
                "MV (SEK)": mv_sek,
                "GAV (SEK)": gav_sek,
                "AV (SEK)": cost_sek,
                "P/L (SEK)": pl_sek,
                "P/L (%)": pl_pct,
                "Årlig utd (SEK) ~brutto": div_y_sek,
            })

            tot_mv   += mv_sek
            tot_cost += cost_sek
            tot_div_y += div_y_sek
        except Exception:
            continue

    if not rows:
        st.info("Inga innehav med pris hittades.")
        return

    tbl = pd.DataFrame(rows)
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Värde (SEK)", f"{tot_mv:,.0f}".replace(",", " "))
    c2.metric("Anskaffn. (SEK)", f"{tot_cost:,.0f}".replace(",", " "))
    c3.metric("P/L (SEK)", f"{(tot_mv-tot_cost):,.0f}".replace(",", " "))
    c4.metric("P/L (%)", f"{((tot_mv-tot_cost)/tot_cost*100.0 if tot_cost>0 else 0):.2f}%")
    st.dataframe(tbl, use_container_width=True)

    # Nästa utdelningar (enkel prognos)
    st.markdown("---")
    st.subheader("📅 Nästa utdelningar (prognos – betalningsdatum, enkel)")
    out = []
    for _, r in df.iterrows():
        qty = _pos(r.get("Antal aktier")) or 0.0
        if qty <= 0: 
            continue
        tkr = str(r.get("Ticker") or "").strip()
        ccy = str(_nz(r.get("Valuta"), "USD")).upper()
        fx  = _fx_rate(fx_map, ccy, base="SEK")
        nxt_dt, last_amt, hint = _infer_next_div_simple(tkr)
        if nxt_dt and _pos(last_amt):
            out.append({
                "Datum": str(nxt_dt),
                "Ticker": tkr,
                "Valuta": ccy,
                "Antal": qty,
                "Per aktie": float(last_amt),
                "Netto (SEK) ~brutto": float(last_amt) * float(qty) * float(fx),
                "Frekvens": hint
            })
    if out:
        nd = pd.DataFrame(out).sort_values("Datum")
        st.dataframe(nd, use_container_width=True)
    else:
        st.info("Ingen utdelningsprognos att visa.")

# ============================================================
# Analys-vy (kopplar mot Del 4 logiken om den redan finns)
# ============================================================
def page_analysis():
    # Om Del 4 redan definierade page_analysis() – undvik dubbeldefinition
    if "__analysis_defined_guard__" in st.session_state:
        # Anropa den version som redan finns i globala namnrymden
        globals()["page_analysis"] = st.session_state["__analysis_defined_guard__"]
        return globals()["page_analysis"]()
    # Minimal fallback (om Del 4 saknas): lista tickers
    st.header("🔬 Analys")
    try:
        df = st.session_state.get("DATA") or read_data_df()
        st.session_state["DATA"] = df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        return
    if df.empty:
        st.info("DATA är tomt.")
        return
    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)
    tkr = _select_with_search_nav("Välj bolag", tickers, names_map, "analysis_idx_fallback", "analysis_q_fallback")
    if not tkr:
        return
    row = df.loc[df["Ticker"].astype(str) == tkr].iloc[0]
    manual = _row_to_manual_overrides(row)
    with st.spinner("Beräknar riktkurser…"):
        targets, meta, dbg = compute_methods_for_row(tkr, manual=manual, prefer_method=None)
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Kurs", _format_num(_pos(_nz(meta.get("snapshot_used",{}).get("price"), None))))
    c2.metric("Riktkurs idag", _format_num(targets.get("target_today")))
    c3.metric("Riktkurs 1 år", _format_num(targets.get("target_1y")))
    c4.metric("Riktkurs 2 år", _format_num(targets.get("target_2y")))
    c5.metric("Riktkurs 3 år", _format_num(targets.get("target_3y")))
    if st.checkbox("Visa debugdetaljer"):
        st.dataframe(pd.DataFrame(dbg, columns=["Nyckel","Källa/Metod","Värde"]), use_container_width=True)

# Lagra ev. redan-definierad page_analysis från Del 4 i session guard,
# så Del 5 inte kliver över den.
if "page_analysis" in globals() and "__analysis_defined_guard__" not in st.session_state:
    st.session_state["__analysis_defined_guard__"] = globals()["page_analysis"]

# ============================================================
# Ranking – beräknar uppsida per bolag
# ============================================================
def page_ranking():
    st.header("🏆 Ranking – Uppsida")
    try:
        df = st.session_state.get("DATA") or read_data_df()
        st.session_state["DATA"] = df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        return
    if df.empty:
        st.info("DATA är tomt.")
        return

    only_owned = st.checkbox("Visa endast innehav (Antal > 0)", value=False, key="rank_owned")
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1, key="rank_horizon")

    base = df.copy()
    if only_owned:
        base = base[(pd.to_numeric(base["Antal aktier"], errors="coerce") > 0)]

    rows = []
    prog = st.progress(0.0)
    total = len(base)

    for i, (_, r) in enumerate(base.iterrows(), start=1):
        try:
            tkr = str(r.get("Ticker") or "").strip()
            manual = _row_to_manual_overrides(r)
            targets, meta, _dbg = compute_methods_for_row(tkr, manual=manual, prefer_method=None)
            price = _pos(_nz(meta.get("snapshot_used",{}).get("price"), None))
            tgt = {
                "Idag": targets.get("target_today"),
                "1 år": targets.get("target_1y"),
                "2 år": targets.get("target_2y"),
                "3 år": targets.get("target_3y")
            }.get(horizon)
            up = None
            if _pos(tgt) and _pos(price):
                up = (float(tgt) - float(price)) / float(price) * 100.0
            rows.append({
                "Ticker": tkr,
                "Valuta": (targets.get("currency") or r.get("Valuta") or "USD"),
                "Kurs": price,
                f"Riktkurs {horizon}": _f(tgt),
                "Uppsida (%)": up
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
# Editor – enkel radredigering (manuella fält)
# ============================================================
def page_editor():
    st.header("✏️ Editor (manuella fält)")
    try:
        df = st.session_state.get("DATA") or read_data_df()
        st.session_state["DATA"] = df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        return
    if df.empty:
        st.info("DATA är tomt.")
        return

    need_cols = ["Ticker","Bucket","Antal aktier","GAV (SEK)","EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y","Senast manuellt uppdaterad"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = np.nan

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)
    sel = _select_with_search_nav("Välj rad (Ticker)", tickers, names_map, "editor_idx_v2", "editor_q_v2")
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
        new_ticker = st.text_input("Ticker", value=str(row.get("Ticker") or "").upper(), key="ed_tkr")
        antal_in   = st.text_input("Antal aktier", value=str(_f(row.get("Antal aktier")) or ""), key="ed_qty")
        gav_in     = st.text_input("GAV (SEK)", value=str(_f(row.get("GAV (SEK)")) or ""), key="ed_gav")
        bucket_sel = st.selectbox("Bucket",
                                  [
                                      "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
                                      "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning"
                                  ],
                                  index=0, key="ed_bucket")
    with c2:
        eps1_in = st.text_input("EPS 1Y (estimat)", value=str(_f(row.get("EPS 1Y")) or ""), key="ed_eps1")
        eps2_in = st.text_input("EPS 2Y (estimat)", value=str(_f(row.get("EPS 2Y")) or ""), key="ed_eps2")
        rev1_in = st.text_input("Rev 1Y (ENHETER, ingen auto-konvertering)", value=str(_f(row.get("Rev 1Y")) or ""), key="ed_rev1")
        rev2_in = st.text_input("Rev 2Y (ENHETER)", value=str(_f(row.get("Rev 2Y")) or ""), key="ed_rev2")

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara rad (session)"):
            try:
                def _pf(v): 
                    v = str(v).strip()
                    return None if v=="" else _f(v)
                df.loc[idx, "Ticker"] = (new_ticker or sel).upper().strip()
                df.loc[idx, "Bucket"] = bucket_sel
                df.loc[idx, "Antal aktier"] = _pf(antal_in) or 0.0
                if _pf(gav_in) is not None:
                    df.loc[idx, "GAV (SEK)"] = _pf(gav_in)
                if _pf(eps1_in) is not None:
                    df.loc[idx, "EPS 1Y"] = _pf(eps1_in)
                if _pf(eps2_in) is not None:
                    df.loc[idx, "EPS 2Y"] = _pf(eps2_in)
                if _pf(rev1_in) is not None:
                    df.loc[idx, "Rev 1Y"] = _pf(rev1_in)
                if _pf(rev2_in) is not None:
                    df.loc[idx, "Rev 2Y"] = _pf(rev2_in)
                df.loc[idx, "Senast manuellt uppdaterad"] = _now()
                st.session_state["DATA"] = df
                st.success("Sparat i session. Klicka '⬆️ Skriv DATA → Google Sheets' för att spara.")
            except Exception as e:
                st.error(f"Fel: {e}")
    with colB:
        if st.button("⬆️ Skriv DATA → Google Sheets"):
            try:
                write_data_df(df)
                st.success("DATA sparad till Google Sheets.")
            except Exception as e:
                st.error(f"Kunde inte skriva: {e}")

    st.markdown("---")
    st.subheader("Förhandsgranskning")
    st.dataframe(df.loc[[idx]], use_container_width=True)

# ============================================================
# Lägg till ticker
# ============================================================
def page_add_ticker():
    st.header("➕ Lägg till ticker")
    try:
        base_df = st.session_state.get("DATA") or read_data_df()
        st.session_state["DATA"] = base_df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        return

    tkr = st.text_input("Ticker", key="add_tkr").upper().strip()
    c1,c2,c3 = st.columns(3)
    with c1:
        bolagsnamn = st.text_input("Bolagsnamn", key="add_name")
        sektor     = st.text_input("Sektor", key="add_sector")
    with c2:
        bucket = st.selectbox("Bucket",
                              [
                                  "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
                                  "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning"
                              ], index=0, key="add_bucket")
        valuta = st.text_input("Valuta (t.ex. USD)", value="USD", key="add_ccy").upper()
    with c3:
        antal = st.text_input("Antal aktier", value="", key="add_qty")
        gav   = st.text_input("GAV (SEK)", value="", key="add_gav")

    st.markdown("**Prognos-/manuella fält (frivilliga, ingen auto-konvertering):**")
    c4,c5 = st.columns(2)
    with c4:
        eps1_in = st.text_input("EPS 1Y (estimat)", key="add_eps1")
        rev1_in = st.text_input("Rev 1Y (ENHETER)", key="add_rev1")
    with c5:
        eps2_in = st.text_input("EPS 2Y (estimat)", key="add_eps2")
        rev2_in = st.text_input("Rev 2Y (ENHETER)", key="add_rev2")

    if st.button("💾 Lägg till i DATA (skriv till Google Sheets)"):
        if not tkr:
            st.warning("Ticker krävs.")
            return
        try:
            if not base_df.empty and (base_df["Ticker"].astype(str).str.upper() == tkr.upper()).any():
                st.error("Ticker finns redan. Använd Editor för att uppdatera.")
                return
            new_row = {c: np.nan for c in (base_df.columns if not base_df.empty else [
                "Timestamp","Ticker","Bolagsnamn","Sektor","Bucket","Valuta",
                "Antal aktier","GAV (SEK)","EPS 1Y","EPS 2Y","Rev 1Y","Rev 2Y","Senast manuellt uppdaterad"
            ])}
            new_row.update({
                "Timestamp": _now(),
                "Ticker": tkr,
                "Bolagsnamn": bolagsnamn if bolagsnamn else np.nan,
                "Sektor": sektor if sektor else np.nan,
                "Bucket": bucket,
                "Valuta": valuta or "USD",
                "Antal aktier": _f(antal) or 0.0,
                "GAV (SEK)": _f(gav) if _f(gav) is not None else np.nan,
                "EPS 1Y": _f(eps1_in) if _f(eps1_in) is not None else np.nan,
                "EPS 2Y": _f(eps2_in) if _f(eps2_in) is not None else np.nan,
                "Rev 1Y": _f(rev1_in) if _f(rev1_in) is not None else np.nan,
                "Rev 2Y": _f(rev2_in) if _f(rev2_in) is not None else np.nan,
                "Senast manuellt uppdaterad": _now()
            })
            out_df = pd.concat([base_df, pd.DataFrame([new_row])], ignore_index=True)
            write_data_df(out_df)
            st.session_state["DATA"] = out_df
            st.success(f"{tkr} tillagd i DATA.")
        except Exception as e:
            st.error(f"Kunde inte lägga till: {e}")

# ============================================================
# Batch – massuppdatering mål & snapshot (beräknar via compute_methods_for_row)
# ============================================================
def page_batch():
    st.header("🧩 Massuppdatering (riktkurser via compute)")
    try:
        df = st.session_state.get("DATA") or read_data_df()
        st.session_state["DATA"] = df
    except Exception as e:
        st.error(f"Kunde inte läsa Data-bladet: {e}")
        return
    if df.empty:
        st.info("DATA är tomt.")
        return

    tickers = sorted(df["Ticker"].dropna().astype(str).unique().tolist())
    names_map = _names_map_from_df(df)
    q = st.text_input("Sök (ticker/namn) för urval", value=st.session_state.get("batch_q2",""), key="batch_q2")

    def _match(t: str) -> bool:
        if not q:
            return True
        nm = names_map.get(t, "")
        ql = q.lower()
        return (ql in t.lower()) or (ql in nm.lower())

    filtered = [t for t in tickers if _match(t)]
    sel = st.multiselect("Välj tickers (tom = alla)", options=filtered, default=filtered[:20], key="batch_sel2")
    target = tickers if len(sel) == 0 else sel

    delay = st.slider("Fördröjning per bolag (sek)", 0.5, 5.0, 1.0, 0.5, key="batch_delay2")
    go = st.button("🚀 Kör uppdatering")

    if not go:
        return

    progress = st.progress(0.0)
    status = st.empty()
    df_cur = df.copy()

    for i, tkr in enumerate(target, start=1):
        try:
            status.write(f"Uppdaterar {i}/{len(target)} – {tkr}")
            r = df_cur.loc[df_cur["Ticker"].astype(str) == tkr]
            row = r.iloc[0] if not r.empty else pd.Series({})
            manual = _row_to_manual_overrides(row)
            targets, meta, _dbg = compute_methods_for_row(tkr, manual=manual, prefer_method=None)

            # skriv riktkurser i df_cur
            mask = df_cur["Ticker"].astype(str) == tkr
            if mask.any():
                idx = df_cur.index[mask][0]
                df_cur.at[idx, "Primär metod"] = targets.get("method")
                df_cur.at[idx, "Riktkurs idag"] = _f(targets.get("target_today"))
                df_cur.at[idx, "Riktkurs 1 år"] = _f(targets.get("target_1y"))
                df_cur.at[idx, "Riktkurs 2 år"] = _f(targets.get("target_2y"))
                df_cur.at[idx, "Riktkurs 3 år"] = _f(targets.get("target_3y"))

            # snapshot-logg
            try:
                _append_rows("Snapshot", [[
                    _now(), tkr, (targets.get("currency") or row.get("Valuta") or "USD"),
                    (targets.get("method") or ""),
                    _f(targets.get("target_today")),
                    _f(targets.get("target_1y")),
                    _f(targets.get("target_2y")),
                    _f(targets.get("target_3y")),
                    None, None
                ]])
            except Exception:
                pass

        except Exception as e:
            st.error(f"{tkr}: {e}")
        progress.progress(i/len(target))
        time.sleep(float(delay))

    write_data_df(df_cur)
    st.session_state["DATA"] = df_cur
    progress.empty()
    status.empty()
    st.success(f"Klar. {len(target)} bolag uppdaterade.")

# ============================================================
# Main & router
# ============================================================
def _boot_session():
    # Läs DATA en gång om inte finns
    if "DATA" not in st.session_state or st.session_state["DATA"] is None:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception:
            st.session_state["DATA"] = pd.DataFrame()

def main():
    _boot_session()

    st.sidebar.title("Navigering")
    if st.sidebar.button("↻ Läs om från Google Sheets"):
        try:
            st.session_state["DATA"] = read_data_df()
            st.success("DATA omläst.")
            st.rerun()
        except Exception as e:
            st.error(f"Kunde inte läsa: {e}")

    if st.sidebar.button("⬆️ Spara session → Google Sheets"):
        try:
            write_data_df(st.session_state["DATA"])
            st.success("DATA sparad.")
        except Exception as e:
            st.error(f"Kunde inte skriva: {e}")

    page = st.sidebar.radio(
        "Gå till:",
        ["Analys","Portfölj","Ranking","Editor","Lägg till ticker","Batch","Settings","Snapshot"],
        index=0
    )

    try:
        if page == "Analys":
            page_analysis()
        elif page == "Portfölj":
            page_portfolio()
        elif page == "Ranking":
            page_ranking()
        elif page == "Editor":
            page_editor()
        elif page == "Lägg till ticker":
            page_add_ticker()
        elif page == "Batch":
            page_batch()
        elif page == "Settings":
            page_settings()
        elif page == "Snapshot":
            page_snapshot()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# ============================================================
# app.py — Del 6/6 — Entrypoint
# ============================================================

if __name__ == "__main__":
    main()
