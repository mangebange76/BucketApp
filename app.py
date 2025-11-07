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
# All funktionalitet från tidigare version är kvar.
# Senare delar bygger vidare på detta utan att ta bort något.
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
#
#  • Yahoo-fetchers (snapshot, EPS-estimat, Rev/EPS-CAGR)
#  • Multipel-ankare, decay & prisberäknare
#  • Här återfinns alla helpers som Del 3 använder
#    (t.ex. _clamp, _pe_anchor, _decay_multiple, _rev_manual_to_units_autosense)
# ============================================================

from __future__ import annotations
import math, time
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st

# ------------------------
# Gränsvärden & helpers
# ------------------------
REV_CAGR_MIN = -0.30
REV_CAGR_MAX =  0.60
EPS_CAGR_MIN = -0.30
EPS_CAGR_MAX =  0.60

def _clamp(x: Optional[float], lo: float, hi: float) -> Optional[float]:
    v = _f(x)
    if v is None:
        return None
    return max(lo, min(hi, float(v)))

def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    x, y = _f(a), _f(b)
    if x is None or y is None or y == 0:
        return None
    return float(x) / float(y)

def _nan_none(x):
    if x is None:
        return None
    try:
        if isinstance(x, float) and (np.isnan(x) or not math.isfinite(x)):
            return None
    except Exception:
        pass
    return x

def _compute_ev(market_cap: Optional[float], total_debt: Optional[float], cash_and_equiv: Optional[float]) -> Optional[float]:
    mc = _f(market_cap) or 0.0
    td = _f(total_debt) or 0.0
    ca = _f(cash_and_equiv) or 0.0
    ev = mc + td - ca
    return ev if math.isfinite(ev) and ev > 0 else None

def _eps_from_price_pe(price: Optional[float], pe_ttm: Optional[float]) -> Optional[float]:
    p, pe = _f(price), _f(pe_ttm)
    if p is None or pe is None or pe <= 0:
        return None
    return float(p) / float(pe)

# ------------------------
# Multipel-ankare & decay
# ------------------------
def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float = 0.50) -> Optional[float]:
    a = _f(pe_ttm)
    b = _f(pe_fwd)
    w = _f(w_ttm) or 0.50
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return float(a)*float(w) + float(b)*(1.0-float(w))

def _decay_multiple(mult: Optional[float], years: int, decay: float) -> Optional[float]:
    m = _f(mult)
    d = _f(decay) or 0.0
    if m is None:
        return None
    try:
        return float(m) * ((1.0 - float(d)) ** int(years))
    except Exception:
        return m

# ------------------------
# Prisberäknare (metodspecifika)
# ------------------------
def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    e, p = _f(eps), _f(pe)
    if e is None or p is None or p <= 0:
        return None
    return float(e) * float(p)

def _equity_price_from_ev(ev: Optional[float], net_debt: Optional[float], shares: Optional[float]) -> Optional[float]:
    if ev is None:
        return None
    nd = _f(net_debt) or 0.0
    sh = _f(shares)
    if sh is None or sh <= 0:
        return None
    eq_val = float(ev) - float(nd)  # EV = Eq + NetDebt  => Eq = EV - NetDebt
    return eq_val / float(sh) if math.isfinite(eq_val) else None

def _ev_from_sales(revenue: Optional[float], ev_sales_mult: Optional[float]) -> Optional[float]:
    r = _f(revenue)
    m = _f(ev_sales_mult)
    if r is None or r <= 0 or m is None or m <= 0:
        return None
    return float(r) * float(m)

def _ev_from_ebitda(ebitda: Optional[float], ev_ebitda_mult: Optional[float]) -> Optional[float]:
    e = _f(ebitda)
    m = _f(ev_ebitda_mult)
    if e is None or e <= 0 or m is None or m <= 0:
        return None
    return float(e) * float(m)

def _price_from_pb(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    pbv = _f(pb)
    bv  = _f(bvps)
    if pbv is None or bv is None or pbv <= 0 or bv <= 0:
        return None
    return float(pbv) * float(bv)

def _derive_eps_ttm_from_pe_only(price_in: Any, pe_ttm_in: Any, eps_ttm_in: Any) -> Tuple[Optional[float], str]:
    """
    Om EPS TTM saknas men pris + PE TTM finns: härled EPS=Pris/PE.
    Returnerar (eps_ttm, källa)
    """
    eps = _f(eps_ttm_in)
    if eps is not None:
        return float(eps), "input"
    derived = _eps_from_price_pe(price_in, pe_ttm_in)
    if derived is not None:
        return float(derived), "derived_from_price_pe"
    return None, "none"

# ------------------------
# Rev-manual autosense
# ------------------------
def _rev_manual_to_units_autosense(manual_value: Optional[float], rev_ttm: Optional[float]) -> Optional[float]:
    """
    Vi lagrar redan Rev 1Y/2Y i **absoluta enheter** (miljoner * 1_000_000 i Del 5).
    Denna funktion finns för bakåtkompatibilitet:
      - Om användaren råkat skriva i miljoner (t.ex. 8810) men TTM tydligt är i hela kronor (>> 1e6),
        multiplicerar vi med 1e6.
      - Annars returnerar vi värdet som det är.
    """
    v = _f(manual_value)
    if v is None:
        return None
    r = _f(rev_ttm)
    if r is not None and r > 10_000_000 and v < 50_000:  # heuristik: sannolikt miljoner
        return float(v) * 1_000_000.0
    return float(v)

# ------------------------
# Yahoo: generella extraktörer
# ------------------------
def _try_fast_info(t: yf.Ticker) -> dict:
    try:
        fi = t.fast_info  # yfinance 2.x
        return dict(fi) if fi else {}
    except Exception:
        return {}

def _try_info(t: yf.Ticker) -> dict:
    # get_info() är snabbare/stabilare i nyare yfinance än gamla t.info
    for attr in ("get_info", "info"):
        try:
            data = getattr(t, attr)()
            if data:
                return dict(data)
        except Exception:
            continue
    return {}

def _try_shares_out(t: yf.Ticker) -> Optional[float]:
    # Först: get_shares_full() (senaste datapunkt). Fallback: info['sharesOutstanding'].
    try:
        df = t.get_shares_full()
        if df is not None and not df.empty:
            x = float(df["SharesOutstanding"].dropna().iloc[-1])
            return x if math.isfinite(x) and x > 0 else None
    except Exception:
        pass
    info = _try_info(t)
    so = info.get("sharesOutstanding")
    return float(so) if _pos(so) else None

def _try_prices_currency(t: yf.Ticker) -> Tuple[Optional[float], Optional[str]]:
    px = None
    ccy = None
    fi = _try_fast_info(t)
    if fi:
        px = fi.get("last_price") or fi.get("lastPrice") or fi.get("last_close") or fi.get("lastClose") or fi.get("regularMarketPrice")
        ccy = fi.get("currency")
    if not _pos(px):
        try:
            hist = t.history(period="5d")
            if not hist.empty:
                px = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            pass
    if not ccy:
        info = _try_info(t)
        ccy = info.get("currency")
    return (_f(px), (str(ccy).upper() if ccy else None))

def _try_market_cap(t: yf.Ticker) -> Optional[float]:
    fi = _try_fast_info(t)
    mc = fi.get("market_cap") or fi.get("marketCap")
    if _pos(mc):
        return float(mc)
    info = _try_info(t)
    mc = info.get("marketCap")
    return float(mc) if _pos(mc) else None

def _try_total_debt_cash(t: yf.Ticker) -> Tuple[Optional[float], Optional[float]]:
    info = _try_info(t)
    td = info.get("totalDebt") or info.get("total_debt")
    cash = info.get("cash") or info.get("cashAndCashEquivalents") or info.get("totalCash")
    # Fallback via balance sheet
    if not _pos(td) or not _pos(cash):
        try:
            bs_q = t.balance_sheet  # annual i vissa versioner
            if bs_q is not None and not bs_q.empty:
                # sök kolumn (senaste)
                col = bs_q.columns[0]
                if not _pos(td):
                    for k in ["TotalDebt", "LongTermDebt", "ShortLongTermDebtTotal"]:
                        if k in bs_q.index and _pos(bs_q.loc[k, col]):
                            td = float(bs_q.loc[k, col]); break
                if not _pos(cash):
                    for k in ["CashAndCashEquivalents", "CashAndShortTermInvestments", "Cash"]:
                        if k in bs_q.index and _pos(bs_q.loc[k, col]):
                            cash = float(bs_q.loc[k, col]); break
        except Exception:
            pass
    return (_f(td), _f(cash))

def _ttm_from_quarters(df: pd.DataFrame, key: str) -> Optional[float]:
    """
    Summera de 4 senaste kvartalen för 'key' i income_stmt (quarterly_financials),
    fallback: annual financials senaste värde.
    """
    try:
        q = getattr(df, "quarterly_financials", None)
        if isinstance(q, pd.DataFrame) and not q.empty and key in q.index:
            vals = q.loc[key].dropna().astype(float).sort_index(ascending=False).iloc[:4]
            if not vals.empty:
                s = float(vals.sum())
                return s if math.isfinite(s) else None
    except Exception:
        pass
    try:
        a = getattr(df, "financials", None)
        if isinstance(a, pd.DataFrame) and not a.empty and key in a.index:
            col = a.columns[0]
            v = float(a.loc[key, col])
            return v if math.isfinite(v) else None
    except Exception:
        pass
    return None

def _try_revenue_ttm(t: yf.Ticker) -> Optional[float]:
    try:
        return _ttm_from_quarters(t, "TotalRevenue")
    except Exception:
        return None

def _try_ebitda_ttm(t: yf.Ticker) -> Optional[float]:
    # EBITDA kan heta "Ebitda" i info; i statements kan det saknas. Vi faller tillbaka mot info.
    try:
        val = _ttm_from_quarters(t, "Ebitda")
        if _pos(val):
            return float(val)
    except Exception:
        pass
    info = _try_info(t)
    e = info.get("ebitda")
    return float(e) if _pos(e) else None

def _try_eps_ttm(t: yf.Ticker) -> Optional[float]:
    # trailingEps i info/fast_info
    info = _try_info(t)
    eps = info.get("trailingEps") or info.get("trailingEPS")
    return float(eps) if _nan_none(_pos(eps)) else None

def _try_pe_ratios(t: yf.Ticker) -> Tuple[Optional[float], Optional[float]]:
    info = _try_info(t)
    pe_ttm = info.get("trailingPE") or info.get("trailingPe")
    pe_fwd = info.get("forwardPE") or info.get("forwardPe")
    return (_f(pe_ttm), _f(pe_fwd))

def _try_ev_multiples(t: yf.Ticker, ev_val: Optional[float], revenue_ttm: Optional[float], ebitda_ttm: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """Returnera (EV/S, EV/EBITDA) — med säkra fallbacks, aldrig KeyError på 'ev'."""
    info = _try_info(t)
    ev_sales = info.get("enterpriseToRevenue") or info.get("enterpriseToSales")
    ev_ebitda = info.get("enterpriseToEbitda") or info.get("enterpriseToEBITDA")
    evs = _f(ev_sales)
    eve = _f(ev_ebitda)
    if evs is None and _pos(ev_val) and _pos(revenue_ttm):
        evs = _safe_div(ev_val, revenue_ttm)
    if eve is None and _pos(ev_val) and _pos(ebitda_ttm):
        eve = _safe_div(ev_val, ebitda_ttm)
    return (evs, eve)

def _try_pb_bvps(t: yf.Ticker, price: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    info = _try_info(t)
    pb  = info.get("priceToBook") or info.get("priceToBookRatio")
    bv  = info.get("bookValue") or info.get("bookValuePerShare")
    pbv = _f(pb)
    bvps = _f(bv)
    if (bvps is None or bvps <= 0) and _pos(pbv) and _pos(price):
        try:
            bvps = float(price) / float(pbv)
        except Exception:
            pass
    return (pbv, _f(bvps))

def _infer_dividend_12m_and_freq(t: yf.Ticker) -> Tuple[Optional[float], Optional[str]]:
    try:
        try:
            s = t.get_dividends()
        except Exception:
            s = getattr(t, "dividends", None)
        if s is None or len(s) == 0:
            return (None, None)
        ser = pd.Series(s).dropna()
        if ser.empty:
            return (None, None)
        ser.index = pd.to_datetime(ser.index, errors="coerce")
        ser = ser.dropna().sort_index()
        last_12m = ser[ser.index >= (pd.Timestamp.today() - pd.Timedelta(days=365))]
        total = float(last_12m.sum()) if not last_12m.empty else None
        # Frekvens
        freq = None
        if len(ser) >= 4:
            diffs = np.diff(ser.index.values).astype("timedelta64[D]").astype(int)
            if len(diffs) > 0:
                md = int(np.median(diffs[-8:]))
                if md <= 40:   freq = "M"
                elif md <= 120: freq = "Q"
                elif md <= 220: freq = "S"
                else:           freq = "A"
        return (_f(total), freq)
    except Exception:
        return (None, None)

# ------------------------
# Publika fetchers
# ------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar snabb-snapshot för beräkningar. Alla fält är frivilliga (kan vara None).
    Returnerar även 'sources' som anger var viktiga fält kom ifrån.
    """
    tkr = str(ticker).strip()
    t = yf.Ticker(tkr)
    out: Dict[str, Any] = {"sources": {}}

    # Pris & valuta
    price, currency = _try_prices_currency(t)
    out["price"] = _f(price);      out["sources"]["price"] = "fast_info/history"
    out["currency"] = currency;    out["sources"]["currency"] = "fast_info/info"

    # Shares & MCAP, debt/cash → EV
    shares = _try_shares_out(t)
    out["shares"] = _f(shares);    out["sources"]["shares"] = "shares_full/info"
    mcap = _try_market_cap(t)
    td, cash = _try_total_debt_cash(t)
    ev_val = _compute_ev(mcap, td, cash)

    out["net_debt"] = _f(_f(td) - _f(cash) if (_f(td) is not None and _f(cash) is not None) else None)
    out["sources"]["net_debt"] = "info/balance_sheet"

    # TTM-intäkter/EBITDA/EPS
    rev_ttm = _try_revenue_ttm(t);   out["revenue_ttm"] = _f(rev_ttm);   out["sources"]["revenue_ttm"] = "financials"
    ebitda  = _try_ebitda_ttm(t);    out["ebitda_ttm"]  = _f(ebitda);    out["sources"]["ebitda_ttm"]  = "financials/info"
    eps_ttm = _try_eps_ttm(t);       out["eps_ttm"]     = _f(eps_ttm);   out["sources"]["eps_ttm"]     = "info"
    pe_ttm, pe_fwd = _try_pe_ratios(t)
    out["pe_ttm"] = _f(pe_ttm);      out["sources"]["pe_ttm"] = "info"
    out["pe_fwd"] = _f(pe_fwd);      out["sources"]["pe_fwd"] = "info"

    # EV-multiplar – säkra guards, INGA 'ev' nycklar används
    evs, eve = _try_ev_multiples(t, ev_val, rev_ttm, ebitda)
    out["ev_to_sales"]  = _f(evs);   out["sources"]["ev_to_sales"]  = "info/derived"
    out["ev_to_ebitda"] = _f(eve);   out["sources"]["ev_to_ebitda"] = "info/derived"

    # P/B & BVPS
    pb, bvps = _try_pb_bvps(t, price)
    out["p_to_book"] = _f(pb);       out["sources"]["p_to_book"] = "info/derived"
    out["bvps"]      = _f(bvps);     out["sources"]["bvps"]      = "info/derived"

    # Dividend (12m) & frekvens
    ann_div, div_freq = _infer_dividend_12m_and_freq(t)
    out["annual_dividend"]     = _f(ann_div)
    out["dividend_frequency"]  = div_freq

    # Namn/sector/industry
    info = _try_info(t)
    out["company_name"] = info.get("longName") or info.get("shortName") or info.get("symbol")
    out["sector"]       = info.get("sector")
    out["industry"]     = info.get("industry")

    return out

@st.cache_data(ttl=900, show_spinner=False)
def fetch_yahoo_eps_estimates(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker hämta EPS-estimat för 1Y & 2Y framåt.
    Faller tillbaka till None om inget hittas.
    """
    t = yf.Ticker(str(ticker).strip())
    eps_1y = None
    eps_2y = None
    # earnings_trend / earnings_forecasts varierar mellan versioner
    for attr in ("get_earnings_trend", "earnings_trend", "get_earnings_forecasts", "earnings_forecasts"):
        try:
            data = getattr(t, attr)()
            if data is None:
                continue
            # Vanliga format i yfinance: DataFrame med kolumn 'epsTrend' eller nycklar med 'epsLow/epsHigh/epsAvg'
            if isinstance(data, pd.DataFrame):
                df = data.copy()
                # Leta efter rader som antyder kommande år (nextYear) och nästnästa (yearAfterNext)
                cols = [c.lower() for c in df.columns]
                if "epsforward" in cols:
                    # ibland ligger framtida EPS i 'epsForward'/'epsCurrentYear' etc
                    try:
                        if "epscurrentyear" in cols:
                            eps_1y = _f(df["epsCurrentYear"].dropna().iloc[-1])
                        if "epsnextyear" in cols:
                            eps_2y = _f(df["epsNextYear"].dropna().iloc[-1])
                    except Exception:
                        pass
                # fallback: försök hitta någon kolumn med "eps" i namnet
                for c in df.columns:
                    if str(c).lower() in ("epsnextyear","eps_next_year") and eps_2y is None:
                        try: eps_2y = _f(df[c].dropna().iloc[-1])
                        except Exception: pass
                    if str(c).lower() in ("epscurrentyear","eps_current_year") and eps_1y is None:
                        try: eps_1y = _f(df[c].dropna().iloc[-1])
                        except Exception: pass
            elif isinstance(data, dict):
                # Nyare dict-format
                tr = data.get("trend") or data.get("earningsTrend") or []
                if isinstance(tr, list):
                    for item in tr:
                        period = str(item.get("period","")).lower()
                        if "currentyear" in period and eps_1y is None:
                            eps_1y = _f((item.get("epsTrend") or {}).get("avg"))
                        if "nextyear" in period and eps_2y is None:
                            eps_2y = _f((item.get("epsTrend") or {}).get("avg"))
        except Exception:
            continue
    return {"eps_1y": _f(eps_1y), "eps_2y": _f(eps_2y)}

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_rev_cagr(ticker: str) -> Dict[str, Optional[float]]:
    """
    Beräkna historisk Rev-CAGR (≈5 år om möjligt) från årsredovisade intäkter.
    Returnerar {"rev_cagr": float | None}
    """
    t = yf.Ticker(str(ticker).strip())
    try:
        df = t.financials  # Annual
        if df is None or df.empty or "TotalRevenue" not in df.index:
            return {"rev_cagr": None}
        # sortera på år (kolumner är år)
        cols = list(df.columns)
        if len(cols) < 3:
            return {"rev_cagr": None}
        # ta tidigaste & senaste (minst 3 år, helst 5)
        last = df.loc["TotalRevenue"].dropna()
        if last.empty:
            return {"rev_cagr": None}
        vals = last.astype(float).sort_index()
        if len(vals) < 3:
            return {"rev_cagr": None}
        # Välj 5 senast om finns annars allt
        sel = vals.iloc[-5:] if len(vals) >= 5 else vals
        v0 = float(sel.iloc[0]); vN = float(sel.iloc[-1])
        n_years = max(1, len(sel)-1)
        if v0 <= 0 or not math.isfinite(v0) or not math.isfinite(vN):
            return {"rev_cagr": None}
        cagr = (vN / v0) ** (1.0 / n_years) - 1.0
        return {"rev_cagr": float(cagr)}
    except Exception:
        return {"rev_cagr": None}

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_yahoo_eps_cagr_hist(ticker: str) -> Dict[str, Optional[float]]:
    """
    Grovt EPS-CAGR (≈5 år) – försöker använda 'earnings' (annual EPS).
    """
    t = yf.Ticker(str(ticker).strip())
    try:
        earn = t.earnings  # historisk EPS/Revenue (äldre API)
        if earn is None or earn.empty:
            return {"eps_cagr": None}
        if "Earnings" not in earn.columns:
            return {"eps_cagr": None}
        vals = earn["Earnings"].dropna().astype(float).sort_index()
        if len(vals) < 3:
            return {"eps_cagr": None}
        sel = vals.iloc[-5:] if len(vals) >= 5 else vals
        v0 = float(sel.iloc[0]); vN = float(sel.iloc[-1])
        n_years = max(1, len(sel)-1)
        if v0 <= 0 or not math.isfinite(v0) or not math.isfinite(vN):
            return {"eps_cagr": None}
        cagr = (vN / v0) ** (1.0 / n_years) - 1.0
        return {"eps_cagr": float(cagr)}
    except Exception:
        return {"eps_cagr": None}

# ============================================================
# app.py — Del 3/6 — Datainsamling & beräkningsmotor (2/2)
#  • compute_methods_for_row: returnerar metodtabell + meta/sanity
#  • Rev 1Y/2Y auto-detektas mot Rev TTM
#  • Inga valutakonverteringar av dina manuella EPS — lämnas orörda.
# ============================================================

import math, time
import pandas as pd
import numpy as np

# ---- EPS/REV-banor ----
def _eps_path_fill(eps_ttm: float | None, eps_1y: float | None, eps_2y: float | None,
                   eps_cagr_hist: float | None, eps_cagr_long: float | None,
                   rev_cagr_hist: float | None) -> tuple[float, float, float, float]:
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

def _rev_path(rev_ttm: float | None, rev_cagr_hist: float | None,
              rev1_manual_units: float | None, rev2_manual_units: float | None) -> tuple[float | None, float | None, float | None, float | None]:
    r0 = _pos(rev_ttm)
    if _pos(rev1_manual_units) and _pos(rev2_manual_units):
        return r0, float(rev1_manual_units), float(rev2_manual_units), float(rev2_manual_units) * (1.0 + float(_f(rev_cagr_hist) or 0.0))
    if _pos(rev1_manual_units) and (not _pos(rev2_manual_units)):
        g = float(_f(rev_cagr_hist) or 0.0)
        r1 = float(rev1_manual_units)
        r2 = r1 * (1.0 + g)
        r3 = r2 * (1.0 + g)
        return r0, r1, r2, r3
    g = float(_f(rev_cagr_hist) or 0.0)
    if r0 is None:
        return None, None, None, None
    r1 = r0 * (1.0 + g)
    r2 = r1 * (1.0 + g)
    r3 = r2 * (1.0 + g)
    return r0, r1, r2, r3

def _ebitda_path(ebitda_ttm: float | None, rev0: float | None, rev1: float | None, rev2: float | None, rev3: float | None) -> tuple[float | None, float | None, float | None, float | None]:
    e0 = _f(ebitda_ttm)  # kan vara negativt
    if e0 is None:
        return None, None, None, None
    if rev0 is None or rev1 is None:
        return e0, e0, e0, e0
    def scale(r):
        try:
            return (e0 * (r / rev0)) if (r and rev0) else e0
        except Exception:
            return e0
    return e0, scale(rev1), scale(rev2), scale(rev3)

# ---- Huvud: beräkna metoder för en rad ----
def compute_methods_for_row(row: pd.Series, settings: dict[str, str], fx_map: dict[str, float]) -> tuple[pd.DataFrame, str, dict[str, any]]:
    ticker = str(row.get("Ticker", "")).strip()

    # 1) Live-data (Del 2/6 funktioner)
    snap   = fetch_yahoo_snapshot(ticker)
    time.sleep(0.12)  # mild throttling
    yh_eps = fetch_yahoo_eps_estimates(ticker)
    time.sleep(0.05)
    revcg_yh = fetch_yahoo_rev_cagr(ticker)         # 5y hist Revenue CAGR
    epscg_yh = fetch_yahoo_eps_cagr_hist(ticker)    # 5y hist EPS CAGR

    # 2) Inputs (med fallback från Data-bladet)
    price    = _pos(_nz(snap.get("price"), row.get("Aktuell kurs")))
    currency = str(_nz(snap.get("currency"), row.get("Valuta") or "USD")).upper()
    shares   = _pos(_nz(snap.get("shares"), row.get("Utestående aktier")))
    net_debt = _nz(snap.get("net_debt"), row.get("Net debt"))

    rev_ttm    = _nz(snap.get("revenue_ttm"), row.get("Rev TTM"))
    ebitda_ttm = _nz(snap.get("ebitda_ttm"), row.get("EBITDA TTM"))
    eps_ttm    = _nz(snap.get("eps_ttm"), row.get("EPS TTM"))
    pe_ttm     = _pos(_nz(snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(snap.get("pe_fwd"), row.get("PE FWD")))
    ev_sales   = _pos(_nz(snap.get("ev_to_sales"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(snap.get("ev_to_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(snap.get("p_to_book"), row.get("P/B")))
    bvps       = _pos(_nz(snap.get("bvps"), row.get("BVPS")))

    # Estimat / tillväxt — **EPS manuella lämnas orörda (ingen FX-konvertering här)**
    eps_1y_est = _pos(_nz(row.get("EPS 1Y"), _nz(yh_eps.get("eps_1y"), None)))
    eps_2y_est = _pos(_nz(row.get("EPS 2Y"), _nz(yh_eps.get("eps_2y"), None)))

    # Historisk CAGR (5y) — clamp
    rev_cagr_hist_raw = _f(_nz(row.get("Rev CAGR"), revcg_yh.get("rev_cagr")))
    rev_cagr_hist     = _clamp(rev_cagr_hist_raw, REV_CAGR_MIN, REV_CAGR_MAX)

    eps_cagr_hist_raw = _f(_nz(row.get("EPS CAGR"), epscg_yh.get("eps_cagr")))
    eps_cagr_hist     = _clamp(eps_cagr_hist_raw, EPS_CAGR_MIN, EPS_CAGR_MAX)

    # EPS TTM härledning endast om saknas
    eps_ttm, src_eps_ttm = _derive_eps_ttm_from_pe_only(price, pe_ttm, _f(eps_ttm))

    # 3) Anchors & decay
    w_ttm = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # 4) Revenue: **auto-detekt** manuella 1Y/2Y mot TTM (milj→enheter om användaren råkat skriva i miljoner)
    rev1_manual_units = _rev_manual_to_units_autosense(_f(row.get("Rev 1Y")), _f(rev_ttm))
    rev2_manual_units = _rev_manual_to_units_autosense(_f(row.get("Rev 2Y")), _f(rev_ttm))
    r0, r1, r2, r3 = _rev_path(_f(rev_ttm), rev_cagr_hist, rev1_manual_units, rev2_manual_units)

    # 5) EPS-path
    eps_cagr_long = None  # vi använder i första hand hist-CAGR; (kan pluggas in om du vill)
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est, eps_cagr_hist, eps_cagr_long, rev_cagr_hist)

    # 6) EBITDA-path (skalar mot intäktsbana)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multiplar med decay
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales, _decay_multiple(ev_sales, 1, decay), _decay_multiple(ev_sales, 2, decay), _decay_multiple(ev_sales, 3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,     _decay_multiple(p_b,     1, decay), _decay_multiple(p_b,     2, decay), _decay_multiple(p_b,     3, decay)

    # 7) Priser per metod
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

    # 8) Sanity + META
    src = snap.get("sources", {}) or {}

    eps1_src = ("sheet" if _pos(row.get("EPS 1Y")) else
                ("yahoo_trend" if _pos(yh_eps.get("eps_1y")) else "filled_by_rule"))

    eps2_src = ("sheet" if _pos(row.get("EPS 2Y")) else
                ("yahoo_trend" if _pos(yh_eps.get("eps_2y")) else "filled_by_rule"))

    revc_src = ("sheet" if _f(row.get("Rev CAGR")) is not None else
                ("yahoo_financials" if revcg_yh.get("rev_cagr") is not None else "none"))

    epsc_src = ("sheet" if _f(row.get("EPS CAGR")) is not None else
                ("yahoo_financials" if epscg_yh.get("eps_cagr") is not None else "none"))

    sanity = (
        f"price={'ok' if price else '—'}({src.get('price','?')}), "
        f"eps_ttm={'ok' if (e0 or e0==0) else '—'}({src.get('eps_ttm','?') or ('derived' if (isinstance(src_eps_ttm, str) and src_eps_ttm.startswith('derived')) else src_eps_ttm)}), "
        f"eps_1y={'ok' if e1 else '—'}({eps1_src}), "
        f"eps_2y={'ok' if e2 else '—'}({eps2_src}), "
        f"rev_ttm={'ok' if r0 else '—'}({src.get('revenue_ttm','?')}), "
        f"rev_cagr_hist={'ok' if _f(rev_cagr_hist) is not None else '—'}({revc_src} ; clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
        f"eps_cagr_hist={'ok' if _f(eps_cagr_hist) is not None else '—'}({epsc_src} ; clamp={EPS_CAGR_MIN*100:.0f}%..{EPS_CAGR_MAX*100:.0f}%), "
        f"ebitda_ttm={'ok' if (b0 or b0==0) else '—'}({src.get('ebitda_ttm','?')}), "
        f"shares={'ok' if shares else '—'}({src.get('shares','?')}), "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '—'}, decay={decay}"
    )

    meta = {
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "company_name": snap.get("company_name"),
        "sector": snap.get("sector"),
        "industry": snap.get("industry"),
        "annual_dividend": snap.get("annual_dividend"),
        "dividend_frequency": snap.get("dividend_frequency"),
        "sources": {
            **src,
            "eps_1y_source": eps1_src,
            "eps_2y_source": eps2_src,
            "rev_cagr_source": revc_src,
            "eps_cagr_source": epsc_src,
        },
        "cagr_clamped": {
            "rev_cagr_raw": _f(rev_cagr_hist_raw),
            "rev_cagr_used": _f(rev_cagr_hist),
            "eps_cagr_raw": _f(eps_cagr_hist_raw),
            "eps_cagr_used": _f(eps_cagr_hist),
        },
        "eps_path": {"ttm": e0, "y1": e1, "y2": e2, "y3": e3},
        "rev_path": {"ttm": r0, "y1": r1, "y2": r2, "y3": r3},
        "ebitda_path": {"ttm": b0, "y1": b1, "y2": b2, "y3": b3},
    }
    return methods_df, sanity, meta

# ============================================================
# (fortsätt i Del 4/6 — Portfölj, P/L & utdelningar)
# ============================================================

# ============================================================
# app.py — Del 4/6 — Portfölj, P/L & utdelningar
#  • Portföljtabell (GAV i SEK, MV i SEK, P/L kr & %, Årlig utd. (SEK), /månad)
#  • Källskatt: USD 15%, CAD 15%, NOK 25% (överskuggas av Settings i Del 5)
#  • Nästa utdelningsdatum (prognos, ej X-dag) + nettobelopp i SEK
# ============================================================

import pandas as pd
import numpy as np
import yfinance as yf
import streamlit as st

WITHHOLDING_BY_CCY = {
    "USD": 0.15,
    "CAD": 0.15,
    "NOK": 0.25,
}

def _fx_rate(fx_map: dict[str, float] | None, ccy: str, base: str = "SEK") -> float:
    if not ccy:
        return 0.0
    c = str(ccy).upper().strip()
    if c == base.upper():
        return 1.0
    if isinstance(fx_map, dict) and c in fx_map and _pos(fx_map[c]):
        return float(fx_map[c])
    return 0.0

def _withholding_for(ccy: str) -> float:
    return WITHHOLDING_BY_CCY.get(str(ccy).upper(), 0.0)

@st.cache_data(ttl=1200, show_spinner=False)
def _yf_dividends(ticker: str) -> pd.Series | None:
    try:
        tk = yf.Ticker(str(ticker))
        try:
            divs = tk.get_dividends()
        except Exception:
            divs = getattr(tk, "dividends", None)
        if divs is None or len(divs) == 0:
            return None
        s = pd.Series(divs).dropna()
        if s.empty:
            return None
        s.index = pd.to_datetime(s.index, errors="coerce")
        s = s.dropna()
        return s.sort_index()
    except Exception:
        return None

def _infer_next_dividend(ticker: str) -> tuple[pd.Timestamp | None, float | None, str]:
    s = _yf_dividends(ticker)
    if s is None or s.empty:
        return None, None, "?"
    recent = s.copy()
    recent = recent[recent.index >= (pd.Timestamp.today() - pd.Timedelta(days=5*365))]
    if recent.empty:
        return None, None, "?"
    last_amt = float(recent.iloc[-1])
    last_dt  = pd.Timestamp(recent.index[-1])

    cadence_hint = "?"
    if len(recent) >= 4:
        diffs = np.diff(recent.index.values).astype("timedelta64[D]").astype(int)
        if len(diffs) > 0:
            med_days = int(np.median(diffs[-8:]))
            med_days = int(max(25, min(380, med_days)))
            if med_days <= 40:
                cadence_hint = "M"
            elif med_days <= 120:
                cadence_hint = "Q"
            elif med_days <= 220:
                cadence_hint = "S"
            else:
                cadence_hint = "A"
            nxt = last_dt + pd.Timedelta(days=med_days)
            today = pd.Timestamp.today().normalize()
            while nxt.normalize() <= today:
                nxt += pd.Timedelta(days=med_days)
            return nxt, last_amt, cadence_hint

    n = len(recent[recent.index >= (pd.Timestamp.today() - pd.Timedelta(days=370))])
    if n >= 10:
        cadence_hint = "M"; step = 30
    elif n >= 3:
        cadence_hint = "Q"; step = 90
    elif n == 2:
        cadence_hint = "S"; step = 182
    else:
        cadence_hint = "A"; step = 365
    nxt = last_dt + pd.Timedelta(days=step)
    today = pd.Timestamp.today().normalize()
    while nxt.normalize() <= today:
        nxt += pd.Timedelta(days=step)
    return nxt, last_amt, cadence_hint

def _ensure_price(row: pd.Series) -> float | None:
    p = _pos(row.get("Aktuell kurs"))
    if _pos(p):
        return float(p)
    tick = str(row.get("Ticker", "")).strip()
    snap = fetch_yahoo_snapshot(tick)
    return _pos(snap.get("price"))

def compute_portfolio_table(data_df: pd.DataFrame, fx_map: dict[str, float]) -> tuple[pd.DataFrame, dict[str, float]]:
    if data_df is None or data_df.empty:
        return pd.DataFrame(), {"tot_mv": 0.0, "tot_cost": 0.0, "tot_pl": 0.0, "tot_pl_pct": 0.0, "tot_div_y": 0.0, "tot_div_m": 0.0}

    rows = []
    tot_mv = tot_cost = tot_div_y = 0.0

    for _, r in data_df.iterrows():
        try:
            ticker = str(r.get("Ticker", "")).strip()
            if not ticker:
                continue
            qty = _pos(r.get("Antal aktier")) or 0.0
            if qty <= 0:
                continue

            ccy = str(r.get("Valuta", "USD")).upper()
            fx  = _fx_rate(fx_map, ccy, base="SEK")

            price = _ensure_price(r) or 0.0
            gav_sek = _pos(r.get("GAV (SEK)")) or 0.0  # alltid SEK

            mv_sek   = float(price) * float(qty) * float(fx)
            cost_sek = float(gav_sek) * float(qty)
            pl_sek   = mv_sek - cost_sek
            pl_pct   = (pl_sek / cost_sek * 100.0) if cost_sek > 0 else None

            annual_ps = _pos(r.get("Årlig utdelning"))
            if not _pos(annual_ps):
                snap = fetch_yahoo_snapshot(ticker)
                annual_ps = _pos(snap.get("annual_dividend"))
            tax = _withholding_for(ccy)
            div_y_net_sek = 0.0
            if _pos(annual_ps) and fx > 0:
                div_y_net_sek = float(annual_ps) * float(qty) * (1.0 - float(tax)) * float(fx)
            div_m_net_sek = div_y_net_sek / 12.0

            rows.append({
                "Ticker": ticker,
                "Valuta": ccy,
                "Antal": qty,
                "FX (→SEK)": fx,
                "Kurs": price,
                "MV (SEK)": mv_sek,
                "GAV (SEK)": gav_sek,
                "AV (SEK)": cost_sek,
                "P/L (SEK)": pl_sek,
                "P/L (%)": pl_pct,
                "Årlig utd (SEK)": div_y_net_sek,
                "Utd/mån (SEK)": div_m_net_sek,
            })

            tot_mv   += mv_sek
            tot_cost += cost_sek
            tot_div_y += div_y_net_sek
        except Exception:
            continue

    df = pd.DataFrame(rows, columns=[
        "Ticker","Valuta","Antal","FX (→SEK)","Kurs","MV (SEK)","GAV (SEK)","AV (SEK)",
        "P/L (SEK)","P/L (%)","Årlig utd (SEK)","Utd/mån (SEK)"
    ])

    tot_pl = tot_mv - tot_cost
    tot_pl_pct = (tot_pl / tot_cost * 100.0) if tot_cost > 0 else 0.0
    totals = {
        "tot_mv": tot_mv,
        "tot_cost": tot_cost,
        "tot_pl": tot_pl,
        "tot_pl_pct": tot_pl_pct,
        "tot_div_y": tot_div_y,
        "tot_div_m": tot_div_y / 12.0,
    }
    return df, totals

def build_next_dividends_list(data_df: pd.DataFrame, fx_map: dict[str, float]) -> pd.DataFrame:
    if data_df is None or data_df.empty:
        return pd.DataFrame(columns=["Datum","Ticker","Valuta","Antal","Per aktie","Källskatt","Netto (SEK)"])

    out = []
    today = pd.Timestamp.today().normalize()

    for _, r in data_df.iterrows():
        try:
            ticker = str(r.get("Ticker", "")).strip()
            qty = _pos(r.get("Antal aktier")) or 0.0
            if not ticker or qty <= 0:
                continue
            ccy = str(r.get("Valuta", "USD")).upper()
            fx  = _fx_rate(fx_map, ccy, base="SEK")
            if fx <= 0:
                continue

            nxt_dt, last_amt, hint = _infer_next_dividend(ticker)
            if nxt_dt is None or last_amt is None:
                continue
            if nxt_dt.normalize() <= today:
                continue

            tax = _withholding_for(ccy)
            net_sek = float(last_amt) * float(qty) * (1.0 - float(tax)) * float(fx)
            out.append({
                "Datum": nxt_dt.date().isoformat(),
                "Ticker": ticker,
                "Valuta": ccy,
                "Antal": qty,
                "Per aktie": float(last_amt),
                "Källskatt": f"{int(tax*100)}%",
                "Netto (SEK)": net_sek,
            })
        except Exception:
            continue

    if not out:
        return pd.DataFrame(columns=["Datum","Ticker","Valuta","Antal","Per aktie","Källskatt","Netto (SEK)"])

    df = pd.DataFrame(out, columns=["Datum","Ticker","Valuta","Antal","Per aktie","Källskatt","Netto (SEK)"])
    try:
        df["Datum"] = pd.to_datetime(df["Datum"], errors="coerce")
        df = df.dropna(subset=["Datum"]).sort_values("Datum", ascending=True)
        df["Datum"] = df["Datum"].dt.date.astype(str)
    except Exception:
        pass
    return df

def render_portfolio_view(data_df: pd.DataFrame, fx_map: dict[str, float]):
    st.subheader("📊 Portfölj (SEK-baserad vy)")

    tbl, totals = compute_portfolio_table(data_df, fx_map)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Totalt portföljvärde (SEK)", f"{totals['tot_mv']:,.0f}".replace(',', ' '))
    col2.metric("Anskaffningsvärde (SEK)", f"{totals['tot_cost']:,.0f}".replace(',', ' '))
    col3.metric("Orealiserad vinst (SEK)", f"{totals['tot_pl']:,.0f}".replace(',', ' '))
    col4.metric("Orealiserad vinst (%)", f"{totals['tot_pl_pct']:.2f}%")

    col5, col6 = st.columns(2)
    col5.metric("Årlig utdelning (SEK, netto)", f"{totals['tot_div_y']:,.0f}".replace(',', ' '))
    col6.metric("Utdelning per månad (SEK, netto)", f"{totals['tot_div_m']:,.0f}".replace(',', ' '))

    st.caption("Obs: GAV anges och behandlas i SEK. FX-kolumn visar växelkurs (SEK per 1 enhet bolagsvaluta).")

    if tbl.empty:
        st.info("Inga innehav med antal > 0 hittades.")
    else:
        st.dataframe(tbl, use_container_width=True)

    st.markdown("---")
    st.subheader("📅 Nästa utdelningar (prognos, **betalningsdatum**)")
    nd = build_next_dividends_list(data_df, fx_map)
    if nd.empty:
        st.info("Ingen prognos att visa. Antingen saknas utdelningshistorik eller innehav.")
    else:
        st.dataframe(nd, use_container_width=True)

# ============================================================
# app.py — Del 5/6 — Main & vyer
#  • Settings, Snapshot
#  • Editor (skriv bolagsnamn/ticker + Föregående/Nästa)
#  • Lägg till ticker
#  • Portfölj, Analys, Ranking, Batch
#  • Boot & main
# ============================================================

import time
import pandas as pd
import numpy as np
import streamlit as st

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

# ----- Namn-karta & sök/bläddra -----
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

# ----- Settings -----
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

# Överskugga _withholding_for så portföljen använder Settings-bladet
def _withholding_for(ccy: str) -> float:
    s = get_settings_map()
    code = (ccy or "USD").upper()
    key  = f"withholding_{code}"
    try:
        return float(s.get(key, "0.0"))
    except Exception:
        return 0.0

# ----- Snapshot -----
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    st.dataframe(snap, use_container_width=True)

# ----- Editor -----
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

# ----- Lägg till ticker -----
def _build_updates_from_yahoo(ticker: str, existing_row: pd.Series):
    snap = fetch_yahoo_snapshot(ticker)
    est  = fetch_yahoo_eps_estimates(ticker)
    rc   = fetch_yahoo_rev_cagr(ticker)
    ec   = fetch_yahoo_eps_cagr_hist(ticker)

    updates = {
        "Timestamp": _now(),
        "Bolagsnamn": _maybe(snap.get("company_name")),
        "Sektor": _maybe(snap.get("sector")),
        "Aktuell kurs": _round2_or_none(snap.get("price")),
        "Valuta": (snap.get("currency") or existing_row.get("Valuta")),
        "Utestående aktier": _maybe(snap.get("shares")),
        "Net debt": _maybe(snap.get("net_debt")),
        "Rev TTM": _maybe(snap.get("revenue_ttm")),
        "EBITDA TTM": _maybe(snap.get("ebitda_ttm")),
        "EPS TTM": _maybe(snap.get("eps_ttm")),
        "PE TTM": _maybe(snap.get("pe_ttm")),
        "PE FWD": _maybe(snap.get("pe_fwd")),
        "EV/Revenue": _maybe(snap.get("ev_to_sales")),
        "EV/EBITDA": _maybe(snap.get("ev_to_ebitda")),
        "P/B": _maybe(snap.get("p_to_book")),
        "BVPS": _maybe(snap.get("bvps")),
        "EPS 1Y": _maybe(est.get("eps_1y")) if pd.isna(existing_row.get("EPS 1Y")) else existing_row.get("EPS 1Y"),
        "EPS 2Y": _maybe(est.get("eps_2y")) if pd.isna(existing_row.get("EPS 2Y")) else existing_row.get("EPS 2Y"),
        "Rev CAGR": _maybe(rc.get("rev_cagr")),
        "EPS CAGR": _maybe(ec.get("eps_cagr")),
        "Årlig utdelning": _maybe(snap.get("annual_dividend")),
        "Utdelningsfrekvens": _maybe(snap.get("dividend_frequency")),
        "Senast auto uppdaterad": _now(),
        "Auto källa": "Yahoo",
    }
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
                    snap = fetch_yahoo_snapshot(tkr)
                    st.session_state["add_name"]   = snap.get("company_name") or st.session_state.get("add_name","")
                    st.session_state["add_sector"] = snap.get("sector") or st.session_state.get("add_sector","")
                    st.session_state["add_ccy"]    = (snap.get("currency") or st.session_state.get("add_ccy","USD")).upper()
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
            # Rev i miljoner → vi sparar i enheter (din vy anger “miljoner” i input)
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

# ----- Portfölj -----
def page_portfolio():
    st.header("Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return
    fx = st.session_state.get("FX", {}) or {}
    try:
        render_portfolio_view(df, fx)
    except Exception as e:
        st.error(f"Kunde inte rendera portföljen: {e}")

# ----- Analys -----
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

# ----- Ranking -----
def _ensure_price_for_row(row: pd.Series) -> float | None:
    p = _pos(row.get("Aktuell kurs"))
    if _pos(p):
        return float(p)
    snap = fetch_yahoo_snapshot(str(row.get("Ticker")))
    return _pos(snap.get("price"))

def page_ranking():
    st.header("🏆 Ranking – Uppsida per horisont")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    only_owned = st.checkbox("Visa endast innehav (Antal aktier > 0)", value=False)
    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)

    base = df.copy()
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
        st.caption(f"Metod: {item['Metod']}  ·  Valuta: {item['Valuta']}")

# ----- Batch -----
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

# ----- Boot & Main -----
def _boot_session():
    if "DATA" not in st.session_state or st.session_state["DATA"] is None or st.session_state["DATA"].empty:
        try:
            df = read_data_df()
            st.session_state["DATA"] = _ensure_editor_stamp_cols(df)
        except Exception as e:
            st.error(f"Kunde inte läsa Data-bladet: {e}")
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)

    try:
        st.session_state["SETTINGS"] = get_settings_map()
    except Exception:
        st.session_state["SETTINGS"] = {}

    try:
        if str(st.session_state["SETTINGS"].get("auto_refresh_on_start","0")) == "1":
            st.session_state["FX"] = _load_fx_and_update_sheet()
        else:
            st.session_state["FX"] = get_fx_map()
    except Exception:
        st.session_state["FX"] = {"SEK":1.0,"USD":1.0,"EUR":1.0,"NOK":1.0,"CAD":1.0}

def main():
    _boot_session()

    st.sidebar.title("Navigering")
    if st.sidebar.button("↻ Läs om från Google Sheets"):
        st.session_state["DATA"] = _ensure_editor_stamp_cols(read_data_df())
        st.success("DATA omläst.")
        st.rerun()
    if st.sidebar.button("⬆️ Spara session → Google Sheets"):
        write_data_df(st.session_state["DATA"])
        st.success("DATA sparad.")

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
    try:
        main()
    except Exception as e:
        import streamlit as st
        st.error(f"💥 Fel i huvudloopen: {e}")
