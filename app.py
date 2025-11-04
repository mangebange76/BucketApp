# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 1/4: Bas & infrastruktur (UI, helpers, Sheets I/O, schema, FX/Settings)
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
    v = os.environ.get(key)
    if v:
        return v
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def guard(fn, label: str = ""):
    try:
        return fn()
    except Exception as e:
        st.error(f"💥 Fel {label}\n\n{e}")
        raise

def _with_backoff(callable_fn, *args, **kwargs):
    """Backoff för gspread 429/5xx."""
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
    v = _f(x)
    return v if (v is not None and v > 0) else None

def _nz(x, fallback=None):
    return x if (x is not None and x == x) else fallback

# =========================
# Google Sheets Auth
# =========================
@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    """
    Skapa gspread Client från GOOGLE_CREDENTIALS.
    Stöd: Mapping/AttrDict, str (JSON), bytes/bytearray.
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
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppnar spreadsheet via SHEET_URL eller SHEET_ID (trimmar whitespace)."""
    sheet_url = _env_or_secret("SHEET_URL")
    sheet_id  = _env_or_secret("SHEET_ID")
    if sheet_url and sheet_url.strip():
        return _with_backoff(_gc.open_by_url, sheet_url.strip())
    if sheet_id and sheet_id.strip():
        return _with_backoff(_gc.open_by_key, sheet_id.strip())
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID i secrets.")

def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    try:
        return _with_backoff(spread.worksheet, title)
    except WorksheetNotFound:
        return _with_backoff(spread.add_worksheet, title=title, rows=2000, cols=200)

# =========================
# I/O – läs/skriv/append
# =========================
def _ensure_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Garanti: alla kolumner finns i df (annars läggs de till som NaN)."""
    if df.empty:
        return pd.DataFrame(columns=cols)
    changed = False
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
            changed = True
    # behåll befintlig ordning + lägg nya sist
    if changed:
        df = df[[*(k for k in cols if k in df.columns), *[c for c in df.columns if c not in cols]]]
    return df

@st.cache_data(ttl=120, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
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
    "EPS 1Y","EPS 2Y","Rev CAGR","EPS CAGR",
    # <<< CHANGED: manuella framtidsantaganden för intäkter (miljoner i bolagets valuta)
    "Revenue 1Y (M)","Revenue 2Y (M)",  # <<< CHANGED
    "Årlig utdelning","Utdelning CAGR",
    # Utdelningslista
    "Utdelningsfrekvens",
    "Nästa utdelningsdatum",
    "Nästa utdelning (per aktie)",
    "Primär metod",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    # <<< CHANGED: tidsstämplar för manuell/auto
    "Senast manuellt uppdaterad",        # <<< CHANGED
    "Uppd EPS/REV TS",                   # <<< CHANGED (för tabell “äldst uppdaterade”)
    "Senast auto uppdaterad","Auto källa"
]

SETTINGS_COLUMNS = ["Key","Value"]
FX_COLUMNS       = ["Valuta","SEK_per_1"]

def _ensure_sheet_schema():
    # Data
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
            df = df[[c for c in DATA_COLUMNS if c in df.columns] + [c for c in df.columns if c not in DATA_COLUMNS]]
            _write_df(DATA_TITLE, df)

    # Settings
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

    # FX
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

    # Snapshot
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        _write_df(SNAPSHOT_TITLE, pd.DataFrame(columns=[
            "Timestamp","Ticker","Valuta","Metod","Idag","1 år","2 år","3 år","Ankare PE","Decay"
        ]))

guard(_ensure_sheet_schema, label="(säkra ark/kolumner)")

# =========================
# FX – hämta via yfinance
# =========================
FX_PAIRS = {"USD":"USDSEK=X","EUR":"EURSEK=X","NOK":"NOKSEK=X","CAD":"CADSEK=X","SEK":None}

@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_fx_from_yahoo() -> Dict[str, float]:
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
    mp = _load_fx_and_update_sheet()
    for c in ["SEK","USD","EUR","NOK","CAD"]:
        mp.setdefault(c, 1.0 if c=="SEK" else np.nan)
    return mp

# =========================
# Settings – läs/källskatt
# =========================
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

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
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
    df = _read_df(DATA_TITLE)
    # 🔒 Global garanti mot KeyError – säkerställ alla kolumner finns
    df = _ensure_columns(df, DATA_COLUMNS)

    if df.empty:
        return df

    # Numeriska kolumner
    num_cols = [
        "Antal aktier","GAV (SEK)","Aktuell kurs",
        "Utestående aktier","Net debt",
        "Rev TTM","EBITDA TTM","EPS TTM","PE TTM","PE FWD",
        "EV/Revenue","EV/EBITDA","P/B","BVPS","EPS 1Y","EPS 2Y",
        "Rev CAGR","EPS CAGR","Årlig utdelning","Utdelning CAGR",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        "Nästa utdelning (per aktie)",
        # <<< CHANGED: säkerställ att manuella revenue-fält läses in
        "Revenue 1Y (M)","Revenue 2Y (M)"  # <<< CHANGED
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Datum/tidsstämplar
    if "Nästa utdelningsdatum" in df.columns:
        df["Nästa utdelningsdatum"] = pd.to_datetime(df["Nästa utdelningsdatum"], errors="coerce").dt.date

    # <<< CHANGED: tolkning av manuella tidsstämplar
    if "Senast manuellt uppdaterad" in df.columns:
        df["Senast manuellt uppdaterad"] = pd.to_datetime(df["Senast manuellt uppdaterad"], errors="coerce")
    if "Uppd EPS/REV TS" in df.columns:
        df["Uppd EPS/REV TS"] = pd.to_datetime(df["Uppd EPS/REV TS"], errors="coerce")

    # --- Ignorera nollor (tolka 0 som NaN) för auto-hämtade fält ---
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
    cols = [c for c in DATA_COLUMNS if c in df.columns] + [c for c in df.columns if c not in DATA_COLUMNS]
    _write_df(DATA_TITLE, df[cols])

def append_result_row(row: Dict[str, Any]):
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

# ===== Hotfix-guard: säkerställ kritiska symboler finns =====
if 'METHOD_LIST' not in globals():
    METHOD_LIST = [
        "ev_ebitda","ev_sales","pe_hist_vs_eps","p_b",
        "ev_dacf","p_fcf","ev_fcf","p_nav","p_affo","p_tbv","p_nii"
    ]
if 'PREFER_ORDER' not in globals():
    PREFER_ORDER = METHOD_LIST

# ============================================================
# Del 1/4 slut — fortsätt i Del 2/4 (datainsamling & beräkningsmotor)
# ============================================================

# ============================================================
# app.py — Del 2/4: Datainsamling (Yahoo) & Beräkningsmotor
# ============================================================

# ---------- Importer som används här ----------
from typing import Callable

# ================
# Generella helpers
# ================
def clamp(x: Optional[float], lo: Optional[float] = None, hi: Optional[float] = None) -> Optional[float]:
    if x is None or (isinstance(x, float) and (x != x)):
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v

def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b

def _growth(a: Optional[float], b: Optional[float], years: float) -> Optional[float]:
    """CAGR från a -> b över 'years' (kan bli negativ)."""
    if a is None or b is None or a <= 0 or years <= 0:
        return None
    try:
        return (b / a) ** (1.0 / years) - 1.0
    except Exception:
        return None

# =========================
# Yahoo – kurs & financials
# =========================
@st.cache_data(ttl=900, show_spinner=False)
def yahoo_last_price(ticker: str) -> Optional[float]:
    try:
        t = yf.Ticker(ticker)
        px = None
        # fast_info är snabbast när den finns
        try:
            fi = t.fast_info
            px = float(fi.last_price)
        except Exception:
            px = None
        if not px or not math.isfinite(px):
            hist = t.history(period="5d")
            if not hist.empty:
                px = float(hist["Close"].dropna().iloc[-1])
        return float(px) if px and math.isfinite(px) else None
    except Exception:
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def yahoo_financials_for_growth(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försök plocka årlig 'Total Revenue' och 'Diluted EPS' (eller nära proxy)
    de senaste ~5 åren och beräkna CAGR. Robust mot tomma svar från yfinance.
    Returnerar { 'rev_cagr_5y': float|None, 'eps_cagr_5y': float|None }.
    """
    rev_cagr = None
    eps_cagr = None

    try:
        t = yf.Ticker(ticker)

        # --- Revenue från income statement (årsdata) ---
        # Nyare yfinance: t.income_stmt (columns = period, index = items)
        # Äldre: t.financials/quarterly_financials
        df_rev = None
        for attr in ("income_stmt", "financials"):
            try:
                df = getattr(t, attr)
                if df is not None and not df.empty:
                    df_rev = df
                    break
            except Exception:
                pass

        def _extract_series(df: pd.DataFrame, candidates: List[str]) -> Optional[pd.Series]:
            if df is None or df.empty:
                return None
            # index är items, kolumner är perioder (DatetimeIndex/str)
            idx = [i.lower().replace(" ", "") for i in df.index.astype(str)]
            for name in candidates:
                key = name.lower().replace(" ", "")
                matches = [i for i in range(len(idx)) if idx[i] == key]
                if matches:
                    row = df.iloc[matches[0]]
                    # sortera kolumner i tidsordning om möjligt
                    try:
                        row = row.rename(pd.to_datetime, axis=0, errors="ignore")
                    except Exception:
                        pass
                    return row.dropna()
            # fallback: heuristik
            for i, nm in enumerate(idx):
                if "totalrevenue" in nm or nm.endswith("revenue"):
                    row = df.iloc[i].dropna()
                    try:
                        row = row.rename(pd.to_datetime, axis=0, errors="ignore")
                    except Exception:
                        pass
                    return row
            return None

        rev_series = _extract_series(df_rev, ["Total Revenue", "TotalRevenue"])
        if rev_series is not None and len(rev_series) >= 3:
            # ta senaste & äldsta upp till ~5 år tillbaka
            # kolumner kan vara datums/år – sortera stigande
            try:
                rev_series = rev_series.sort_index()
            except Exception:
                pass
            last = float(rev_series.iloc[-1])
            # välj punkt ~5 år bakåt om finns, annars första
            first = float(rev_series.iloc[0]) if len(rev_series) < 5 else float(rev_series.iloc[-5])
            years = max(1.0, min(5.0, len(rev_series)-1))
            rev_cagr = _growth(first, last, years)

        # --- EPS (Diluted) från income statement eller earnings ---
        eps_series = None
        if df_rev is not None and not df_rev.empty:
            eps_series = _extract_series(df_rev, ["Diluted EPS", "DilutedEPS"])
        if eps_series is None:
            try:
                earn = t.earnings
                if earn is not None and not earn.empty:
                    # earn index = Year
                    if "Earnings" in earn.columns and "Revenue" in earn.columns:
                        # saknar EPS – hoppa
                        pass
            except Exception:
                pass

        if eps_series is not None and len(eps_series) >= 3:
            try:
                eps_series = eps_series.sort_index()
            except Exception:
                pass
            last = float(eps_series.iloc[-1])
            first = float(eps_series.iloc[0]) if len(eps_series) < 5 else float(eps_series.iloc[-5])
            years = max(1.0, min(5.0, len(eps_series)-1))
            # EPS kan vara negativ – CAGR på EPS är knepigt; använd absolutvärde som proxy
            if first != 0 and last != 0 and first > 0 and last > 0:
                eps_cagr = _growth(first, last, years)
            else:
                eps_cagr = None
    except Exception:
        pass

    # <<< CHANGED: klampa max 35%
    if rev_cagr is not None:
        rev_cagr = clamp(rev_cagr, hi=0.35)
    if eps_cagr is not None:
        eps_cagr = clamp(eps_cagr, hi=0.35)

    return {"rev_cagr_5y": rev_cagr, "eps_cagr_5y": eps_cagr}

# =========================
# Utdelning & portföljhjälp
# =========================
def compute_dividend_net_sek(row: pd.Series, fx_map: Dict[str, float], settings: Dict[str, str]) -> Tuple[float, float, float]:
    """
    Returnerar (brutto_valuta, källskatt_valuta, netto_SEK) för *nästa* utdelning.
    """
    try:
        per_share = _pos(row.get("Nästa utdelning (per aktie)"))
        shares    = _pos(row.get("Antal aktier"))
        if not per_share or not shares:
            return (0.0, 0.0, 0.0)
        currency  = str(row.get("Valuta") or "USD").upper()
        fx        = _pos(fx_map.get(currency, None)) or 1.0
        withh     = get_withholding_for(currency, settings)
        brutto_val = per_share * shares
        skatt_val  = brutto_val * withh
        netto_sek  = (brutto_val - skatt_val) * fx
        return (float(brutto_val), float(skatt_val), float(netto_sek))
    except Exception:
        return (0.0, 0.0, 0.0)

def position_value_sek(row: pd.Series, fx_map: Dict[str, float]) -> float:
    try:
        shares = _pos(row.get("Antal aktier"))
        px     = _pos(row.get("Aktuell kurs"))
        fx     = _pos(fx_map.get(str(row.get("Valuta") or "USD").upper(), None)) or 1.0
        if not shares or not px:
            return 0.0
        return float(shares * px * fx)
    except Exception:
        return 0.0

# =========================
# Beräkningsmotor: EPS-bana & riktkurser
# =========================
def _pick_pe_anchor(row: pd.Series) -> float:
    """Välj PE-ankare: PE FWD > PE TTM > 20."""
    pe = _pos(row.get("PE FWD")) or _pos(row.get("PE TTM")) or 20.0
    # mild klamp mot absurda ankare
    return float(clamp(pe, lo=5.0, hi=60.0))

def _derive_eps_growth(row: pd.Series) -> Optional[float]:
    """
    EPS-tillväxt för prognos:
      1) Om både EPS 1Y och EPS 2Y (manuella) finns: använd (2Y/1Y - 1)
      2) Annars: använd 'EPS CAGR' (5y) från databasen
      3) Klampa max 35 %
    """
    eps1 = _pos(row.get("EPS 1Y"))
    eps2 = _pos(row.get("EPS 2Y"))
    if eps1 and eps2 and eps1 > 0:
        g = (eps2 / eps1) - 1.0
    else:
        g = _f(row.get("EPS CAGR"))
    return clamp(g, hi=0.35)

def _eps_today(row: pd.Series) -> Optional[float]:
    # preferera EPS TTM, annars EPS 1Y
    return _pos(row.get("EPS TTM")) or _pos(row.get("EPS 1Y"))

def _project_eps_path(row: pd.Series) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returnerar (eps0, eps1, eps2, eps3) där:
      eps0 = TTM (eller fallback 1Y)
      eps1 = manuellt 'EPS 1Y' om finns, annars eps0*(1+g)
      eps2 = manuellt 'EPS 2Y' om finns, annars eps1*(1+g)
      eps3 = eps2*(1+g)
    """
    g = _derive_eps_growth(row)
    eps0 = _eps_today(row)
    m1   = _pos(row.get("EPS 1Y"))
    m2   = _pos(row.get("EPS 2Y"))

    if eps0 is None and m1:
        eps0 = m1 / (1.0 + (g or 0.0))
    if eps0 is None:
        return (None, m1, m2, None)

    eps1 = m1 if m1 else (eps0 * (1.0 + (g or 0.0)) if g is not None else None)
    eps2 = m2 if m2 else (eps1 * (1.0 + (g or 0.0)) if (eps1 is not None and g is not None) else None)
    eps3 = eps2 * (1.0 + (g or 0.0)) if (eps2 is not None and g is not None) else None
    return (eps0, eps1, eps2, eps3)

def _targets_from_eps(row: pd.Series) -> Dict[str, Optional[float]]:
    """
    Enkelt, transparent prisankare: P = PE_anchor × EPS.
    - Idag:   EPS TTM (eller 1Y)
    - 1 år:   EPS 1Y
    - 2 år:   EPS 2Y
    - 3 år:   extrapolerad EPS 3Y
    """
    pe = _pick_pe_anchor(row)
    eps0, eps1, eps2, eps3 = _project_eps_path(row)
    return {
        "Riktkurs idag":  (pe * eps0) if eps0 is not None else None,
        "Riktkurs 1 år":  (pe * eps1) if eps1 is not None else None,
        "Riktkurs 2 år":  (pe * eps2) if eps2 is not None else None,
        "Riktkurs 3 år":  (pe * eps3) if eps3 is not None else None,
        "Primär metod":   "pe_vs_eps"
    }

def compute_targets_for_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Beräknar riktkurser för alla rader och skriver in i df (utan att spara).
    EPS-CAGR klampas på 35 % (om används). Manuella EPS 1Y/2Y prioriteras.
    """
    if df.empty:
        return df
    out = df.copy()
    for idx, row in out.iterrows():
        tvals = _targets_from_eps(row)
        for k, v in tvals.items():
            if k in out.columns:
                out.at[idx, k] = v
    return out

# =========================
# Uppdatering – Yahoo + CAGR
# =========================
def update_rows_from_yahoo(df: pd.DataFrame, tickers: List[str], delay_s: float = 0.0,
                           status: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    """
    Uppdatera 'Aktuell kurs' och (om möjligt) 'Rev CAGR' / 'EPS CAGR' från Yahoo.
    Skriv INTE över manuella fält (EPS/Revenue 1Y/2Y).
    """
    if df.empty or not tickers:
        return df

    out = df.copy()
    tset = set([t.strip().upper() for t in tickers if t and str(t).strip()])

    for i, idx in enumerate(out.index):
        t = str(out.at[idx, "Ticker"]).strip().upper() if "Ticker" in out.columns else None
        if not t or t not in tset:
            continue

        if status:
            status(f"Uppdaterar {t} ({i+1}/{len(tickers)})")

        # Kurs
        try:
            px = yahoo_last_price(t)
            if px is not None:
                out.at[idx, "Aktuell kurs"] = float(px)
        except Exception:
            pass

        # 5y CAGR från financials
        try:
            g = yahoo_financials_for_growth(t)
            if g.get("rev_cagr_5y") is not None:
                out.at[idx, "Rev CAGR"] = float(g["rev_cagr_5y"])   # <<< CHANGED (max 35 % inbyggt)
            if g.get("eps_cagr_5y") is not None:
                out.at[idx, "EPS CAGR"] = float(g["eps_cagr_5y"])   # <<< CHANGED (max 35 % inbyggt)
        except Exception:
            pass

        # metadata
        out.at[idx, "Senast auto uppdaterad"] = now_stamp()
        out.at[idx, "Auto källa"] = "Yahoo Finance"

        if delay_s and delay_s > 0:
            time.sleep(delay_s)

    # räkna riktkurser efter uppdatering
    out = compute_targets_for_df(out)
    return out

def update_all_from_yahoo(df: pd.DataFrame, delay_s: float = 1.0, status: Optional[Callable[[str], None]] = None) -> pd.DataFrame:
    uniq = []
    if "Ticker" in df.columns and not df.empty:
        uniq = [str(x).strip().upper() for x in df["Ticker"].dropna().unique().tolist()]
    return update_rows_from_yahoo(df, uniq, delay_s=delay_s, status=status)

def update_single_from_yahoo(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    return update_rows_from_yahoo(df, [ticker], delay_s=0.0)

# =========================
# Manuellt spar (Editor-vyn)
# =========================
def save_manual_fields(df: pd.DataFrame, ticker: str,
                       antal_aktier: Optional[float] = None,
                       gav_sek: Optional[float] = None,
                       eps1y: Optional[float] = None,
                       eps2y: Optional[float] = None,
                       rev1_m: Optional[float] = None,
                       rev2_m: Optional[float] = None) -> pd.DataFrame:
    """
    Uppdaterar endast de manuella fälten för 'ticker'.
    - EPS/Revenue skrivs till kolumnerna 'EPS 1Y', 'EPS 2Y', 'Revenue 1Y (M)', 'Revenue 2Y (M)'
    - Sätter tidsstämplar 'Senast manuellt uppdaterad' samt 'Uppd EPS/REV TS' om EPS/REV ändrats.
    """
    if df.empty:
        return df
    out = df.copy()
    t = ticker.strip().upper()

    mask = out["Ticker"].astype(str).str.upper().eq(t)
    if not mask.any():
        # lägg till ny rad om ticker saknas
        new_row = {c: np.nan for c in DATA_COLUMNS}
        new_row["Ticker"] = t
        out = pd.concat([out, pd.DataFrame([new_row])], ignore_index=True)
        mask = out["Ticker"].astype(str).str.upper().eq(t)

    idx = out[mask].index[0]
    changed_epsrev = False

    def _set(col: str, val: Optional[float]):
        if col in out.columns and val is not None:
            out.at[idx, col] = float(val)

    _set("Antal aktier", antal_aktier)
    _set("GAV (SEK)",    gav_sek)

    before_eps1 = out.at[idx, "EPS 1Y"] if "EPS 1Y" in out.columns else np.nan
    before_eps2 = out.at[idx, "EPS 2Y"] if "EPS 2Y" in out.columns else np.nan
    before_r1   = out.at[idx, "Revenue 1Y (M)"] if "Revenue 1Y (M)" in out.columns else np.nan
    before_r2   = out.at[idx, "Revenue 2Y (M)"] if "Revenue 2Y (M)" in out.columns else np.nan

    _set("EPS 1Y", eps1y)
    _set("EPS 2Y", eps2y)
    _set("Revenue 1Y (M)", rev1_m)   # <<< CHANGED (miljoner i bolagets valuta)
    _set("Revenue 2Y (M)", rev2_m)   # <<< CHANGED

    # markera om EPS/REV verkligen ändrats
    def _neq(a, b) -> bool:
        try:
            return not (pd.isna(a) and pd.isna(b)) and (float(a) != float(b))
        except Exception:
            return str(a) != str(b)

    if _neq(before_eps1, out.at[idx, "EPS 1Y"]) or _neq(before_eps2, out.at[idx, "EPS 2Y"]) \
       or _neq(before_r1, out.at[idx, "Revenue 1Y (M)"]) or _neq(before_r2, out.at[idx, "Revenue 2Y (M)"]):
        changed_epsrev = True

    out.at[idx, "Senast manuellt uppdaterad"] = now_stamp()
    if changed_epsrev:
        out.at[idx, "Uppd EPS/REV TS"] = now_stamp()

    # efter manuell uppdatering – räkna riktkurser på nytt för just raden
    tvals = _targets_from_eps(out.loc[idx])
    for k, v in tvals.items():
        if k in out.columns:
            out.at[idx, k] = v

    return out

# =========================
# Tabeller – portfölj & utdelningar
# =========================
def build_portfolio_table(df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Ticker","Bolagsnamn","Valuta","Antal aktier","GAV (SEK)","Aktuell kurs","Positionsvärde (SEK)"])
    cols = ["Ticker","Bolagsnamn","Valuta","Antal aktier","GAV (SEK)","Aktuell kurs"]
    base = df[cols].copy()
    base["Positionsvärde (SEK)"] = df.apply(lambda r: position_value_sek(r, fx_map), axis=1)
    return base

def build_upcoming_dividends(df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, str]) -> pd.DataFrame:
    """
    Lista över *betalningsdatum* som finns i kolumnen 'Nästa utdelningsdatum'.
    Filtrerar bort datum i det förflutna. Tar bara rader där per-aktie > 0 och antal > 0.
    """
    if df.empty or "Nästa utdelningsdatum" not in df.columns:
        return pd.DataFrame(columns=["Datum","Ticker","Bolag","Valuta","Per aktie","Antal","Brutto (val)", "Källskatt (val)","Netto (SEK)"])

    today = today_date()
    mask_ok = (df["Nästa utdelningsdatum"].notna()) & (df["Nästa utdelningsdatum"] >= today) \
              & (pd.to_numeric(df["Nästa utdelning (per aktie)"], errors="coerce").fillna(0) > 0) \
              & (pd.to_numeric(df["Antal aktier"], errors="coerce").fillna(0) > 0)

    sub = df.loc[mask_ok].copy()
    if sub.empty:
        return pd.DataFrame(columns=["Datum","Ticker","Bolag","Valuta","Per aktie","Antal","Brutto (val)", "Källskatt (val)","Netto (SEK)"])

    rows = []
    for _, r in sub.iterrows():
        brutto, skatt, netto = compute_dividend_net_sek(r, fx_map, settings)
        rows.append({
            "Datum": r.get("Nästa utdelningsdatum"),
            "Ticker": r.get("Ticker"),
            "Bolag": r.get("Bolagsnamn"),
            "Valuta": r.get("Valuta"),
            "Per aktie": _pos(r.get("Nästa utdelning (per aktie)")) or 0.0,
            "Antal": _pos(r.get("Antal aktier")) or 0.0,
            "Brutto (val)": brutto,
            "Källskatt (val)": skatt,
            "Netto (SEK)": netto
        })
    out = pd.DataFrame(rows)
    try:
        out = out.sort_values(["Datum","Ticker"]).reset_index(drop=True)
    except Exception:
        pass
    return out

# ============================================================
# Del 2/4 slut — Del 3/4 innehåller Editor/Portfölj/Ranking-vyer + navigation
# ============================================================

# ============================================================
# app.py — Del 3/4: Vyer + UI-helpers
# ============================================================

# ----------- UI-rerun helper (utan experimental_) -----------
def ui_rerun():
    try:
        st.rerun()
    except Exception:
        pass

# ----------- Patch: Bygg portföljtabell (med P/L) -----------
def build_portfolio_table(df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "Ticker","Bolagsnamn","Valuta","Antal aktier","GAV (SEK)",
            "Aktuell kurs","Positionsvärde (SEK)","Anskaffningsvärde (SEK)",
            "Vinst/förlust (SEK)","Vinst/förlust (%)"
        ])
    cols = ["Ticker","Bolagsnamn","Valuta","Antal aktier","GAV (SEK)","Aktuell kurs"]
    base = df.reindex(columns=[c for c in cols if c in df.columns]).copy()

    def _fx(row):
        cur = str(row.get("Valuta") or "USD").upper()
        return _pos(fx_map.get(cur)) or 1.0

    base["Positionsvärde (SEK)"] = df.apply(lambda r: (_pos(r.get("Antal aktier")) or 0.0) *
                                                     (_pos(r.get("Aktuell kurs")) or 0.0) *
                                                     (_pos(fx_map.get(str(r.get("Valuta") or "USD").upper())) or 1.0), axis=1)

    base["Anskaffningsvärde (SEK)"] = df.apply(lambda r: (_pos(r.get("Antal aktier")) or 0.0) *
                                                         (_pos(r.get("GAV (SEK)")) or 0.0), axis=1)

    base["Vinst/förlust (SEK)"] = base["Positionsvärde (SEK)"] - base["Anskaffningsvärde (SEK)"]
    base["Vinst/förlust (%)"]   = base.apply(
        lambda r: (_safe_div(r["Vinst/förlust (SEK)"], r["Anskaffningsvärde (SEK)"]) or 0.0) * 100.0
        if r["Anskaffningsvärde (SEK)"] else 0.0,
        axis=1
    )
    return base

# ----------- Patch: Utdelningslista (fixar Timestamp vs date) -----------
def build_upcoming_dividends(df: pd.DataFrame, fx_map: Dict[str, float], settings: Dict[str, str]) -> pd.DataFrame:
    cols_needed = {"Nästa utdelningsdatum","Nästa utdelning (per aktie)","Antal aktier","Ticker","Bolagsnamn","Valuta"}
    if df.empty or not cols_needed.issubset(set(df.columns)):
        return pd.DataFrame(columns=["Datum","Ticker","Bolag","Valuta","Per aktie","Antal","Brutto (val)","Källskatt (val)","Netto (SEK)"])

    # Konvertera datumkolumnen till date
    dates = pd.to_datetime(df["Nästa utdelningsdatum"], errors="coerce").dt.date
    today = today_date()

    mask_ok = (dates.notna()) & (dates >= today) \
              & (pd.to_numeric(df["Nästa utdelning (per aktie)"], errors="coerce").fillna(0) > 0) \
              & (pd.to_numeric(df["Antal aktier"], errors="coerce").fillna(0) > 0)

    sub = df.loc[mask_ok].copy()
    if sub.empty:
        return pd.DataFrame(columns=["Datum","Ticker","Bolag","Valuta","Per aktie","Antal","Brutto (val)","Källskatt (val)","Netto (SEK)"])

    rows = []
    for i, r in sub.iterrows():
        brutto, skatt, netto = compute_dividend_net_sek(r, fx_map, settings)
        rows.append({
            "Datum": dates.iloc[i],
            "Ticker": r.get("Ticker"),
            "Bolag": r.get("Bolagsnamn"),
            "Valuta": r.get("Valuta"),
            "Per aktie": _pos(r.get("Nästa utdelning (per aktie)")) or 0.0,
            "Antal": _pos(r.get("Antal aktier")) or 0.0,
            "Brutto (val)": brutto,
            "Källskatt (val)": skatt,
            "Netto (SEK)": netto
        })
    out = pd.DataFrame(rows)
    try:
        out = out.sort_values(["Datum","Ticker"]).reset_index(drop=True)
    except Exception:
        pass
    return out

# ---------------- Editor-vy ----------------
def page_editor(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.title("✏️ Editor")

    if df_data is None or df_data.empty:
        st.warning("Ingen data laddad ännu. Lägg in en DataFrame i `st.session_state['DATA']` i Del 1.")
        return

    tickers = df_data["Ticker"].astype(str).tolist() if "Ticker" in df_data.columns else []
    if not tickers:
        st.info("Lägg till minst en ticker i databasen först.")
        return

    t = st.selectbox("Välj ticker", tickers, index=0, key="editor_ticker")
    row = df_data[df_data["Ticker"].astype(str) == t].iloc[0]

    c1, c2 = st.columns(2)
    with c1:
        antal = st.number_input("Antal aktier", min_value=0.0, step=1.0, value=float(_pos(row.get("Antal aktier")) or 0.0))
        gav   = st.number_input("GAV (SEK)", min_value=0.0, step=0.01, value=float(_pos(row.get("GAV (SEK)")) or 0.0))
        eps1  = st.number_input("EPS 1Y", step=0.01, value=float(_pos(row.get("EPS 1Y")) or 0.0))
    with c2:
        eps2  = st.number_input("EPS 2Y", step=0.01, value=float(_pos(row.get("EPS 2Y")) or 0.0))
        rev1m = st.number_input("Revenue 1Y (M) • *8,81B → 8810*", step=1.0, value=float(_pos(row.get("Revenue 1Y (M)")) or 0.0))
        rev2m = st.number_input("Revenue 2Y (M) • *10,7B → 10700*", step=1.0, value=float(_pos(row.get("Revenue 2Y (M)")) or 0.0))

    if st.button("💾 Spara manuella fält", use_container_width=True):
        df_new = save_manual_fields(
            df_data, t,
            antal_aktier=antal, gav_sek=gav,
            eps1y=eps1, eps2y=eps2, rev1_m=rev1m, rev2_m=rev2m
        )
        # Lägg tillbaka i session och räkna riktkurser
        df_new = compute_targets_for_df(df_new)
        st.session_state["DATA"] = df_new
        st.success(f"Sparat manuella fält för {t}.")
        ui_rerun()

    st.subheader("3) Äldst uppdaterade EPS/Revenue (topp 10)")
    if "Uppd EPS/REV TS" in df_data.columns:
        tmp = df_data.sort_values("Uppd EPS/REV TS", na_position="first").head(10)
    else:
        tmp = df_data.copy().head(10)
    st.dataframe(tmp[["Ticker","Bolagsnamn","EPS 1Y","EPS 2Y","Revenue 1Y (M)","Revenue 2Y (M)"]], use_container_width=True)

# ---------------- Portfölj-vy ----------------
def page_portfolio(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.title("📊 Portfölj")

    if df_data is None or df_data.empty:
        st.warning("Ingen data laddad ännu.")
        return

    table = build_portfolio_table(df_data, fx_map, settings)
    st.dataframe(table, use_container_width=True)

    # Totalsummer
    try:
        tot_val = float(pd.to_numeric(table["Positionsvärde (SEK)"], errors="coerce").fillna(0).sum())
        tot_cost = float(pd.to_numeric(table["Anskaffningsvärde (SEK)"], errors="coerce").fillna(0).sum())
        pl = tot_val - tot_cost
        pl_pct = (pl / tot_cost * 100.0) if tot_cost else 0.0
        st.metric("Portföljvärde (SEK)", f"{tot_val:,.2f}".replace(",", " "), delta=f"{pl:,.2f} SEK ({pl_pct:.2f} %)")
    except Exception:
        pass

    st.markdown("---")
    st.subheader("🔔 Kommande utdelningar (betalningsdatum)")
    pay = build_upcoming_dividends(df_data, fx_map, settings)
    if pay.empty:
        st.info("Inga kommande utdelningar registrerade.")
    else:
        st.dataframe(pay, use_container_width=True)
        try:
            st.caption(f"Summa netto (SEK): {pay['Netto (SEK)'].sum():,.2f}".replace(",", " "))
        except Exception:
            pass

# ---------------- Analys-vy ----------------
def page_analysis(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.title("🧮 Analys")

    if df_data is None or df_data.empty:
        st.warning("Ingen data laddad ännu.")
        return

    # Välj bolag
    tickers = df_data["Ticker"].astype(str).tolist() if "Ticker" in df_data.columns else []
    t = st.selectbox("Välj bolag", tickers, index=0, key="analysis_ticker")
    row_idx = df_data[df_data["Ticker"].astype(str) == t].index[0]

    # Valbar enskild Yahoo-uppdatering
    if st.button("🔄 Uppdatera från Yahoo (enskild)", use_container_width=True):
        df_new = update_single_from_yahoo(df_data, t)
        st.session_state["DATA"] = df_new
        st.success(f"Uppdaterade {t} från Yahoo.")
        ui_rerun()

    # Räkna riktkurser on-the-fly och visa metodtabell
    df_view = compute_targets_for_df(df_data.copy())
    view_row = df_view.loc[row_idx]

    st.markdown("#### Tekniskt: pris & utdelning")
    c1, c2, c3 = st.columns(3)
    c1.metric("Aktuell kurs", f"{_pos(view_row.get('Aktuell kurs')) or 0:.2f} {view_row.get('Valuta') or ''}")
    c2.metric("Årsutdelning", f"{_pos(view_row.get('Årlig utdelning')) or 0:.2f} {view_row.get('Valuta') or ''}")
    c3.metric("Utdelningsfrekvens", str(view_row.get("Utdelningsfrekvens") or "-"))

    st.markdown("#### Metoder & riktkurser")
    pe_anchor = _pick_pe_anchor(view_row)
    eps0, eps1, eps2, eps3 = _project_eps_path(view_row)
    metod_df = pd.DataFrame([{
        "Metod": "pe_vs_eps",
        "PE-ankare": pe_anchor,
        "EPS (TTM/0)": eps0, "EPS 1Y": eps1, "EPS 2Y": eps2, "EPS 3Y": eps3,
        "Idag": view_row.get("Riktkurs idag"),
        "1 år": view_row.get("Riktkurs 1 år"),
        "2 år": view_row.get("Riktkurs 2 år"),
        "3 år": view_row.get("Riktkurs 3 år"),
    }])
    st.dataframe(metod_df, use_container_width=True)

# ---------------- Ranking-vy ----------------
def _ranking_table(df: pd.DataFrame, horizon: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Ticker","Bolagsnamn","Aktuell kurs",f"Riktkurs {horizon}","Uppsida (%)"])

    df_calc = compute_targets_for_df(df.copy())

    tgt_col = {
        "Idag": "Riktkurs idag",
        "1 år": "Riktkurs 1 år",
        "2 år": "Riktkurs 2 år",
        "3 år": "Riktkurs 3 år"
    }[horizon]

    out = pd.DataFrame({
        "Ticker": df_calc["Ticker"],
        "Bolagsnamn": df_calc.get("Bolagsnamn", pd.Series([""]*len(df_calc))),
        "Aktuell kurs": pd.to_numeric(df_calc.get("Aktuell kurs"), errors="coerce"),
        f"Riktkurs {horizon}": pd.to_numeric(df_calc.get(tgt_col), errors="coerce")
    })
    out["Uppsida (%)"] = ((out[f"Riktkurs {horizon}"] - out["Aktuell kurs"]) / out["Aktuell kurs"]) * 100.0
    try:
        out = out.sort_values("Uppsida (%)", ascending=False)
    except Exception:
        pass
    return out

def page_ranking(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    st.title("🏆 Ranking")
    if df_data is None or df_data.empty:
        st.warning("Ingen data laddad ännu.")
        return

    horizon = st.selectbox("Horisont", ["Idag","1 år","2 år","3 år"], index=1)
    table = _ranking_table(df_data, horizon)
    st.dataframe(table, use_container_width=True)

# ============================================================
# Del 3/4 slut — Del 4/4: Huvudloop, navigering & Sheets-IO
# ============================================================

# ============================================================
# app.py — Del 4/4: Huvudloop, Sheets-IO, Yahoo, targets, utils
# ============================================================

# ----------------------- Utils ------------------------------
def _pos(x):
    """Robust talparser -> float eller None. Stöder '8,81B' och '10 700'."""
    if x is None:
        return None
    if isinstance(x, (int, float, np.number)):
        try:
            if np.isnan(x):  # type: ignore
                return None
        except Exception:
            pass
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"none", "nan"}:
        return None
    # Ta bort mellanslag, tusentalsavskiljare, ersätt komma
    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    mult = 1.0
    if s.endswith(("b","B")):
        mult = 1_000.0  # vi tolkar "B" som miljarder -> inmatning i M
        s = s[:-1]
    try:
        return float(s) * mult
    except Exception:
        try:
            return float(s)
        except Exception:
            return None

def _safe_div(a, b):
    a = _pos(a) or 0.0
    b = _pos(b)
    if not b:
        return None
    try:
        return float(a) / float(b)
    except Exception:
        return None

def today_date():
    # Lokal dag som date-objekt (används i utdelningslistan)
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return datetime.now(tz).date()
    except Exception:
        return datetime.now().date()

# ----------------------- Skatt & utdelningar ----------------
def _withholding_tax_rate(currency: str) -> float:
    cur = (currency or "").upper()
    if cur == "NOK":
        return 0.25
    if cur in {"USD", "CAD"}:
        return 0.15
    # SEK eller övrigt – default 0
    return 0.0

def compute_dividend_net_sek(row: pd.Series, fx_map: Dict[str, float], settings: Dict[str, str]):
    per_share = _pos(row.get("Nästa utdelning (per aktie)")) or 0.0
    shares    = _pos(row.get("Antal aktier")) or 0.0
    currency  = str(row.get("Valuta") or "SEK").upper()
    fx        = _pos(fx_map.get(currency)) or 1.0

    brutto_local = per_share * shares
    tax = brutto_local * _withholding_tax_rate(currency)
    netto_local = brutto_local - tax
    netto_sek = netto_local * fx
    return brutto_local, tax, netto_local * 0 + tax, netto_sek  # behåller signatur-kompabilitet

# ----------------------- Targets / riktkurser ----------------
def _cap35(pct):
    """Max 35% uppåt enligt kravet. Negativa lämnas som de är."""
    if pct is None:
        return None
    try:
        return min(float(pct), 35.0)
    except Exception:
        return None

def _get_eps_growth_pct(row: pd.Series):
    # 1) prioritet: EPS CAGR 5 år (%)
    for key in ["EPS CAGR 5 år (%)", "EPS CAGR 5Y (%)", "EPS CAGR 5y (%)", "EPS CAGR 5y"]:
        if key in row and _pos(row[key]) is not None:
            return _cap35(_pos(row[key]))
    # 2) fallback: beräkna av EPS 1Y -> EPS 2Y
    g = None
    e1 = _pos(row.get("EPS 1Y"))
    e2 = _pos(row.get("EPS 2Y"))
    if e1 and e2 and e1 > 0:
        g = ((e2 / e1) - 1.0) * 100.0
    return _cap35(g if g is not None else 0.0)

def _project_eps_path(row: pd.Series):
    # Starta från "senaste kända" EPS; välj EPS 2Y om den finns, annars 1Y
    eps0 = _pos(row.get("EPS 2Y"))
    if eps0 is None:
        eps0 = _pos(row.get("EPS 1Y")) or 0.0
    g_pct = _get_eps_growth_pct(row) or 0.0
    g = 1.0 + (g_pct / 100.0)
    eps1 = eps0 * g
    eps2 = eps1 * g
    eps3 = eps2 * g
    return round(eps0, 6), round(eps1, 6), round(eps2, 6), round(eps3, 6)

def _pick_pe_anchor(row: pd.Series):
    # Enkel, stabil default – kan förfinas vid behov
    # Om direktavkastning > 4% → lägre ankare, annars lite högre
    try:
        dy = _pos(row.get("DA (%)"))
        if dy and dy >= 4.0:
            return 15.0
    except Exception:
        pass
    return 20.0

def compute_targets_for_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # Säkerställ kolumner
    for col in ["Riktkurs idag", "Riktkurs 1 år", "Riktkurs 2 år", "Riktkurs 3 år"]:
        if col not in df.columns:
            df[col] = None

    out_rows = []
    for _, r in df.iterrows():
        try:
            pe = _pick_pe_anchor(r)
            eps0, eps1, eps2, eps3 = _project_eps_path(r)
            df.loc[r.name, "Riktkurs idag"] = eps0 * pe if pe and eps0 else None
            df.loc[r.name, "Riktkurs 1 år"]  = eps1 * pe if pe and eps1 else None
            df.loc[r.name, "Riktkurs 2 år"]  = eps2 * pe if pe and eps2 else None
            df.loc[r.name, "Riktkurs 3 år"]  = eps3 * pe if pe and eps3 else None
        except Exception:
            # Låt None stå kvar om något saknas
            pass
        out_rows.append(r)
    return df

# ----------------------- Spara manuella fält ----------------
def _stamp_now():
    try:
        import pytz
        tz = pytz.timezone("Europe/Stockholm")
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def save_manual_fields(df: pd.DataFrame, ticker: str,
                       antal_aktier=None, gav_sek=None,
                       eps1y=None, eps2y=None, rev1_m=None, rev2_m=None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    mask = df["Ticker"].astype(str) == str(ticker)
    if not mask.any():
        return df
    idx = df[mask].index[0]

    # Skriv värdena
    if antal_aktier is not None:
        df.at[idx, "Antal aktier"] = float(antal_aktier)
    if gav_sek is not None:
        df.at[idx, "GAV (SEK)"] = float(gav_sek)
    if eps1y is not None:
        df.at[idx, "EPS 1Y"] = float(eps1y)
    if eps2y is not None:
        df.at[idx, "EPS 2Y"] = float(eps2y)
    if rev1_m is not None:
        df.at[idx, "Revenue 1Y (M)"] = float(rev1_m)
    if rev2_m is not None:
        df.at[idx, "Revenue 2Y (M)"] = float(rev2_m)

    # Stämplar
    df.at[idx, "Uppd EPS/REV TS"] = _stamp_now()
    df.at[idx, "Senast manuellt uppdaterad"] = _stamp_now()

    return df

# ----------------------- Yahoo-hämtning (enskild) -----------
def update_single_from_yahoo(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    try:
        yt = yf.Ticker(str(ticker))
    except Exception:
        return df

    info = {}
    fast = {}
    try:
        info = yt.info or {}
    except Exception:
        info = {}
    try:
        fast = yt.fast_info or {}
    except Exception:
        fast = {}

    name = info.get("longName") or info.get("shortName")
    currency = (fast.get("currency") or info.get("currency") or "USD")
    last = fast.get("last_price") or fast.get("lastPrice") or info.get("currentPrice") or info.get("regularMarketPrice")
    dividend = info.get("dividendRate") or info.get("trailingAnnualDividendRate")

    mask = df["Ticker"].astype(str) == str(ticker)
    if not mask.any():
        return df
    idx = df[mask].index[0]

    if name:
        df.at[idx, "Bolagsnamn"] = name
    if currency:
        df.at[idx, "Valuta"] = currency
    if last:
        df.at[idx, "Aktuell kurs"] = float(last)
    if dividend is not None:
        try:
            df.at[idx, "Årlig utdelning"] = float(dividend)
        except Exception:
            pass

    df.at[idx, "Senast auto uppdaterad"] = _stamp_now()
    df.at[idx, "Auto källa"] = "Yahoo Finance"
    return df

# ----------------------- Google Sheets I/O -------------------
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _load_google_credentials_dict() -> Dict[str, Any]:
    if "GOOGLE_CREDENTIALS" not in st.secrets:
        return {}
    raw = st.secrets["GOOGLE_CREDENTIALS"]
    if isinstance(raw, dict):
        return _normalize_private_key(dict(raw))
    if isinstance(raw, str):
        try:
            import json
            data = json.loads(raw)
            return _normalize_private_key(data)
        except Exception:
            return {}
    return {}

def _get_gspread_client():
    creds_dict = _load_google_credentials_dict()
    if not creds_dict:
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)

def _open_spreadsheet():
    gc = _get_gspread_client()
    if not gc:
        return None
    key = st.secrets.get("GOOGLE_SHEET_KEY")
    if not key:
        return None
    try:
        return gc.open_by_key(key)
    except Exception:
        return None

def gs_read_data() -> pd.DataFrame:
    ss = _open_spreadsheet()
    if not ss:
        return pd.DataFrame()
    try:
        ws = ss.worksheet("Data")
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        return df
    except Exception:
        return pd.DataFrame()

def gs_write_data(df: pd.DataFrame) -> bool:
    ss = _open_spreadsheet()
    if not ss:
        return False
    try:
        ws = ss.worksheet("Data")
    except Exception:
        try:
            ws = ss.add_worksheet(title="Data", rows="2000", cols="50")
        except Exception:
            return False
    try:
        # skriv header + data
        values = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
        ws.clear()
        ws.update(values)
        return True
    except Exception:
        return False

def gs_read_fx() -> Dict[str, float]:
    ss = _open_spreadsheet()
    default = {"SEK": 1.0}
    if not ss:
        return default
    try:
        ws = ss.worksheet("Valutakurser")
        rows = ws.get_all_values()
        fx = {}
        for r in rows[1:]:
            if len(r) < 2:
                continue
            cur = (r[0] or "").upper()
            rate = _pos(r[1])
            if cur and rate:
                fx[cur] = float(rate)
        if "SEK" not in fx:
            fx["SEK"] = 1.0
        return fx or default
    except Exception:
        return default

# ----------------------- Huvudloop ---------------------------
def _startup_refresh():
    # 1) Försök läsa DATA från Sheets om den saknas
    if "DATA" not in st.session_state or st.session_state["DATA"] is None or st.session_state["DATA"].empty:
        df = gs_read_data()
        # Säkerställ bas-kolumner
        needed = [
            "Ticker","Bolagsnamn","Valuta","Aktuell kurs",
            "Antal aktier","GAV (SEK)","Årlig utdelning",
            "EPS 1Y","EPS 2Y","Revenue 1Y (M)","Revenue 2Y (M)",
            "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
            "Utdelningsfrekvens","Nästa utdelningsdatum","Nästa utdelning (per aktie)",
            "Senast manuellt uppdaterad","Senast auto uppdaterad","Auto källa","Uppd EPS/REV TS","DA (%)"
        ]
        if df.empty:
            df = pd.DataFrame(columns=needed)
        else:
            for c in needed:
                if c not in df.columns:
                    df[c] = None
        st.session_state["DATA"] = df

    # 2) Läs valutakurser (SEK-bas)
    st.session_state["FX_MAP"] = gs_read_fx()

    # 3) Settings – enkel standard
    st.session_state["SETTINGS"] = {
        "base_ccy": "SEK"
    }

def _nav_sidebar() -> str:
    st.sidebar.title("📚 Navigering")
    page = st.sidebar.radio("Välj vy", ["Analys", "Portfölj", "Ranking", "Editor"], index=0)
    with st.sidebar.expander("Google Sheets", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("↩️ Ladda från Sheets"):
                st.session_state["DATA"] = gs_read_data()
                st.success("Läste om data från Google Sheets.")
                st.rerun()
        with col2:
            if st.button("💾 Spara till Sheets"):
                ok = gs_write_data(st.session_state.get("DATA", pd.DataFrame()))
                if ok:
                    st.success("Sparat till Google Sheets.")
                else:
                    st.error("Kunde inte spara till Google Sheets.")
    return page

def main():
    st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")
    _startup_refresh()

    df_data = st.session_state.get("DATA", pd.DataFrame())
    fx_map = st.session_state.get("FX_MAP", {"SEK": 1.0})
    settings = st.session_state.get("SETTINGS", {"base_ccy": "SEK"})

    page = _nav_sidebar()

    try:
        if page == "Analys":
            page_analysis(df_data, settings, fx_map)
        elif page == "Portfölj":
            page_portfolio(df_data, settings, fx_map)
        elif page == "Ranking":
            page_ranking(df_data, settings, fx_map)
        else:
            page_editor(df_data, settings, fx_map)
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e!s}")

if __name__ == "__main__":
    main()

# ============================ Slut ===========================
