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

# ===== Extra UI-hjälpare (används i Editor/Analys) =====
def _names_map_from_df(df: pd.DataFrame) -> Dict[str, str]:
    """
    Bygger en display-sträng per ticker för söklistor: 'TICKER — Bolagsnamn'.
    """
    out: Dict[str, str] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        t = str(_nz(r.get("Ticker"), "")).upper().strip()
        if not t:
            continue
        name = str(_nz(r.get("Bolagsnamn"), "")).strip()
        out[t] = f"{t} — {name}" if name else t
    return out

def _select_with_search_nav(label: str, options: List[str], names_map: Dict[str, str],
                            idx_key: str, query_key: str) -> Optional[str]:
    """
    Sök + välj ticker med enkel föregående/nästa navigering.
    Returnerar vald ticker eller None.
    """
    if not options:
        st.info("Inga alternativ.")
        return None
    options = [str(o).upper().strip() for o in options]
    options = sorted(list(dict.fromkeys(options)))  # unika + sort

    if idx_key not in st.session_state:
        st.session_state[idx_key] = 0
    if query_key not in st.session_state:
        st.session_state[query_key] = ""

    q = st.text_input(f"{label} — sök", value=st.session_state[query_key], key=query_key).strip().lower()
    display = [(o, names_map.get(o, o)) for o in options]
    if q:
        display = [(o, d) for (o, d) in display if (q in o.lower()) or (q in str(d).lower())]

    if not display:
        st.warning("Inget matchade sökningen.")
        return None

    tickers_filtered = [o for (o, _) in display]
    labels = [d for (_, d) in display]

    # Justera index om out-of-range
    if st.session_state[idx_key] >= len(tickers_filtered):
        st.session_state[idx_key] = 0

    col_sel, col_nav = st.columns([3, 1])
    with col_sel:
        idx = st.session_state[idx_key]
        picked_label = st.selectbox(label, labels, index=idx if idx < len(labels) else 0)
        # mappa tillbaka label -> ticker
        picked_idx = labels.index(picked_label)
        st.session_state[idx_key] = picked_idx
        picked = tickers_filtered[picked_idx]
    with col_nav:
        st.write("")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("◀︎", use_container_width=True):
                st.session_state[idx_key] = (st.session_state[idx_key] - 1) % len(tickers_filtered)
                picked = tickers_filtered[st.session_state[idx_key]]
        with c2:
            if st.button("▶︎", use_container_width=True):
                st.session_state[idx_key] = (st.session_state[idx_key] + 1) % len(tickers_filtered)
                picked = tickers_filtered[st.session_state[idx_key]]

    return picked

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
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Uppdateringsfunktioner (enskild & massa) som endast skriver över fält vi lyckas hämta
# ============================================================

# ============================================================
# Del 2/6 — Datainsamling & beräkningshjälp (Yahoo)
#  • Robust Yahoo-snapshot (pris, valuta, MCAP, EV, TTM, utdelning)
#  • EPS/Revenue TTM från kvartalssummor
#  • 5-års historisk CAGR (Revenue & EPS)
#  • Uppdateringsfunktioner (enskild & massa) – skriver endast över fält vi lyckas hämta
# ============================================================

# ---------- Hjälpare för säkra tal ----------
def _num(x):
    try:
        if x is None or (isinstance(x, float) and (not math.isfinite(x))):
            return None
        return float(x)
    except Exception:
        return None

def _set_if(df: pd.DataFrame, idx: int, col: str, val):
    """Skriv endast om vi har ett värde (ej None/NaN)."""
    if val is None:
        return
    if isinstance(val, float) and not math.isfinite(val):
        return
    df.at[idx, col] = val

def _sum_last_n(series_like, n: int) -> Optional[float]:
    try:
        s = pd.Series(series_like).dropna().astype(float)
        if s.empty:
            return None
        return float(s.iloc[-n:].sum())
    except Exception:
        return None

def _cagr(first: Optional[float], last: Optional[float], years: float) -> Optional[float]:
    try:
        f = _num(first); l = _num(last)
        if f is None or l is None or f <= 0 or l <= 0 or years <= 0:
            return None
        return (l / f) ** (1.0 / years) - 1.0
    except Exception:
        return None

# ---------- Yahoo-hämtning ----------
def yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar ett paket värden från yfinance för 'ticker'.
    OBS: Vi konverterar inte EPS till SEK. Alla siffror stannar i bolagets handelsvaluta.
    Returnerar en dict som redan mappar mot Data-kolumner där det är möjligt.
    """
    out: Dict[str, Any] = {}
    if not ticker:
        return out

    t = yf.Ticker(ticker)

    # ----- Snabb pris/valuta/mcap -----
    price = None
    currency = None
    mcap = None
    ev = None
    pe_ttm = None
    pe_fwd = None
    pb = None
    bvps = None
    ev_rev = None
    ev_ebitda = None
    shares_out = None
    net_debt = None
    dps_annual = None
    next_div_pay_date = None  # betalningsdatum är sällsynt i Yahoo
    next_div_ps = None
    sector = None
    short_name = None

    # Nya yfinance använder .fast_info och .get_info()
    try:
        fi = t.fast_info
        try: price = _num(getattr(fi, "last_price", None))
        except Exception: price = None
        try: currency = getattr(fi, "currency", None)
        except Exception: currency = None
        try: mcap = _num(getattr(fi, "market_cap", None))
        except Exception: mcap = None
        try: shares_out = _num(getattr(fi, "shares", None))
        except Exception: shares_out = None
    except Exception:
        pass

    # Fler fält via .get_info() (ersätter gamla .info)
    info = {}
    try:
        info = t.get_info() or {}
    except Exception:
        try:
            # fallback till äldre .info om tillgängligt
            info = getattr(t, "info", {}) or {}
        except Exception:
            info = {}

    # plocka extra fält
    currency = info.get("currency", currency)
    short_name = info.get("shortName") or info.get("longName")
    sector = info.get("sector")
    pe_ttm = _num(info.get("trailingPE"))
    pe_fwd = _num(info.get("forwardPE"))
    pb = _num(info.get("priceToBook"))
    bvps = _num(info.get("bookValue"))
    ev = _num(info.get("enterpriseValue"))
    ev_rev = _num(info.get("enterpriseToRevenue"))
    ev_ebitda = _num(info.get("enterpriseToEbitda"))
    shares_out = _num(info.get("sharesOutstanding")) or shares_out

    # Utdelning (obs: Yahoo saknar ofta betalningsdatum)
    dps_annual = _num(info.get("dividendRate"))  # årlig DPS
    next_div_ps = _num(info.get("lastDividendValue"))
    # paymentDate saknas ofta; ex-datum finns som 'exDividendDate'
    # Vi lämnar betalningsdatum tomt om vi inte är säkra
    try:
        pay_ts = info.get("lastDividendDate")
        if isinstance(pay_ts, (int, float)) and pay_ts > 0:
            next_div_pay_date = dt.datetime.utcfromtimestamp(int(pay_ts)).date()
    except Exception:
        pass

    # ----- TTM: Revenue & EBITDA & EPS -----
    # Revenue TTM: summera "Total Revenue" sista 4 kvartal
    rev_ttm = None
    ebitda_ttm = None
    eps_ttm = None

    try:
        q_is = t.quarterly_income_stmt
        if q_is is not None and not q_is.empty:
            # DataFrame index: konton (rader), kolumner: datums
            # Vi vill ha 'Total Revenue' och 'EBITDA'
            if "Total Revenue" in q_is.index:
                rev_ttm = _sum_last_n(q_is.loc["Total Revenue"].values, 4)
            if "EBITDA" in q_is.index:
                ebitda_ttm = _sum_last_n(q_is.loc["EBITDA"].values, 4)

            # EPS TTM: använd "Basic EPS" eller "Diluted EPS" kvartalssummor
            eps_series = None
            if "Basic EPS" in q_is.index:
                eps_series = pd.Series(q_is.loc["Basic EPS"].values).dropna().astype(float)
            elif "Diluted EPS" in q_is.index:
                eps_series = pd.Series(q_is.loc["Diluted EPS"].values).dropna().astype(float)
            if eps_series is not None and not eps_series.empty:
                eps_ttm = float(eps_series.iloc[-4:].sum())
    except Exception:
        pass

    # Alternativ EPS TTM från info om ovan saknas
    if eps_ttm is None:
        eps_ttm = _num(info.get("trailingEps"))

    # ----- 5-års CAGR (Revenue & EPS) -----
    rev_cagr_5 = None
    eps_cagr_5 = None
    try:
        y_is = t.income_stmt  # årliga
        if y_is is not None and not y_is.empty:
            # Kolumner: årsslut (nyast -> äldst)
            # Revenue
            if "Total Revenue" in y_is.index:
                rev_series = pd.Series(y_is.loc["Total Revenue"].dropna().astype(float).values)
                if rev_series.size >= 5:
                    first = rev_series.iloc[-5]
                    last = rev_series.iloc[-1]
                    rev_cagr_5 = _cagr(first, last, years=4)  # 5 punkter = 4 intervall
            # EPS (Basic/Diluted – vi tar Diluted i första hand)
            eps_annual = None
            if "Diluted EPS" in y_is.index:
                eps_annual = pd.Series(y_is.loc["Diluted EPS"].dropna().astype(float).values)
            elif "Basic EPS" in y_is.index:
                eps_annual = pd.Series(y_is.loc["Basic EPS"].dropna().astype(float).values)
            if eps_annual is not None and eps_annual.size >= 5:
                # undvik icke-positiva för CAGR
                eps_pos = eps_annual.replace(0, np.nan).dropna()
                if eps_pos.size >= 5:
                    first = eps_pos.iloc[-5]
                    last = eps_pos.iloc[-1]
                    rev_years = 4
                    # EPS kan vara negativt under resan → CAGR saknas i så fall
                    if first > 0 and last > 0:
                        eps_cagr_5 = _cagr(first, last, years=rev_years)
    except Exception:
        pass

    # ----- Net debt (TotalDebt - Cash) via balance sheet -----
    try:
        bs = t.quarterly_balance_sheet
        total_debt = None
        cash = None
        if bs is not None and not bs.empty:
            if "Total Debt" in bs.index:
                total_debt = _num(pd.Series(bs.loc["Total Debt"].values).dropna().iloc[-1])
            if "Cash And Cash Equivalents" in bs.index:
                cash = _num(pd.Series(bs.loc["Cash And Cash Equivalents"].values).dropna().iloc[-1])
            elif "Cash And Cash Equivalents Including Restricted Cash" in bs.index:
                cash = _num(pd.Series(bs.loc["Cash And Cash Equivalents Including Restricted Cash"].values).dropna().iloc[-1])
        if total_debt is not None and cash is not None:
            net_debt = total_debt - cash
    except Exception:
        pass

    # ----- Fallback för EV/Revenue/EBITDA om multiplar saknas -----
    if ev_rev is None:
        # EV/Revenue = EV / rev_ttm
        if ev is not None and rev_ttm not in (None, 0):
            ev_rev = ev / rev_ttm
    if ev_ebitda is None:
        if ev is not None and ebitda_ttm not in (None, 0):
            ev_ebitda = ev / ebitda_ttm

    # ----- Fyll ut-ut dikt (matchar Data-kolumner) -----
    out.update({
        "Bolagsnamn": short_name,
        "Sektor": sector,
        "Valuta": currency,
        "Aktuell kurs": price,
        "Utestående aktier": shares_out,
        "Net debt": net_debt,
        "Rev TTM": rev_ttm,
        "EBITDA TTM": ebitda_ttm,
        "EPS TTM": eps_ttm,
        "PE TTM": pe_ttm,
        "PE FWD": pe_fwd,
        "EV/Revenue": ev_rev,
        "EV/EBITDA": ev_ebitda,
        "P/B": pb,
        "BVPS": bvps,
        "Årlig utdelning": dps_annual,
        "Nästa utdelningsdatum": next_div_pay_date,     # om inte känd → None (lämnas orörd i DF)
        "Nästa utdelning (per aktie)": next_div_ps,
        "Auto källa": "Yahoo Finance",
        "Senast auto uppdaterad": now_stamp(),
    })
    # CAGR-fält (som andel, ej i %)
    if rev_cagr_5 is not None:
        out["Rev CAGR"] = rev_cagr_5
    if eps_cagr_5 is not None:
        out["EPS CAGR"] = eps_cagr_5

    return out

# ---------- Uppdatera en rad ----------
def update_row_with_yahoo(df: pd.DataFrame, idx: int) -> Tuple[pd.DataFrame, bool, str]:
    """
    Uppdaterar rad 'idx' i df med Yahoo-data baserat på df.at[idx, 'Ticker'].
    Skriver ENDAST över fält som kunde hämtas. Returnerar (df, success, msg).
    """
    try:
        ticker = str(df.at[idx, "Ticker"]).strip()
    except Exception:
        return df, False, "Saknar Ticker för vald rad."

    if not ticker:
        return df, False, "Tom Ticker."

    shot = {}
    try:
        shot = yahoo_snapshot(ticker)
    except Exception as e:
        return df, False, f"Yahoo-fel för {ticker}: {e}"

    if not shot:
        return df, False, f"Inget data hämtades för {ticker}."

    # Skriv endast de fält som finns i df & shot och är 'sanna' värden
    for k, v in shot.items():
        if k in df.columns:
            if k == "Nästa utdelningsdatum":
                # Lämna befintligt om Yahoo saknar säkert betalningsdatum
                if v is not None:
                    _set_if(df, idx, k, v)
            else:
                _set_if(df, idx, k, v)

    # Om Valuta saknas i DF och vi fick en, sätt den
    if "Valuta" in df.columns and (pd.isna(df.at[idx, "Valuta"]) or str(df.at[idx, "Valuta"]).strip() == ""):
        if shot.get("Valuta"):
            df.at[idx, "Valuta"] = shot["Valuta"]

    return df, True, f"Uppdaterade {ticker}."

# ---------- Uppdatera många ----------
def mass_update_yahoo(df: pd.DataFrame, tickers: Optional[List[str]] = None, delay_sec: float = 1.0) -> Tuple[pd.DataFrame, List[str]]:
    """
    Massuppdatera valda tickers (eller alla med Ticker ifall tickers=None).
    1 sekunds fördröjning per bolag (default).
    Returnerar (df, loglist).
    """
    logs: List[str] = []
    if df.empty:
        return df, ["Data-bladet är tomt."]

    if tickers is None:
        tickers = [str(t).strip().upper() for t in df["Ticker"].dropna().tolist() if str(t).strip()]

    # index per ticker
    idx_map: Dict[str, List[int]] = {}
    for i, r in df.reset_index().iterrows():
        t = str(_nz(r.get("Ticker"), "")).strip().upper()
        if not t:
            continue
        idx_map.setdefault(t, []).append(r["index"])

    progress = st.progress(0.0)
    total = len(tickers)
    for n, t in enumerate(tickers, start=1):
        rows = idx_map.get(t.upper(), [])
        if not rows:
            logs.append(f"{t}: hittade ingen rad.")
            progress.progress(min(n/total, 1.0))
            time.sleep(delay_sec)
            continue
        for ridx in rows:
            df, ok, msg = update_row_with_yahoo(df, ridx)
            logs.append(msg if ok else f"{t}: {msg}")
        progress.progress(min(n/total, 1.0))
        time.sleep(delay_sec)

    return df, logs

# ---------- Publika hjälp-funktioner för UI-delar ----------
def yahoo_update_single_ticker(ticker: str) -> Tuple[pd.DataFrame, str]:
    """
    Hämtar DF från session, uppdaterar första förekomsten av 'ticker', sparar och returnerar (df, msg).
    """
    df = df_or_reload_from_session()
    if df.empty:
        return df, "Data-bladet är tomt."
    # hitta index
    locs = df.index[df["Ticker"].astype(str).str.upper().eq(ticker.upper())].tolist()
    if not locs:
        return df, f"Hittar inte {ticker} i Data-bladet."
    df, ok, msg = update_row_with_yahoo(df, locs[0])
    if ok:
        write_data_df(df)
        st.session_state["DATA"] = df
    return df, msg

def yahoo_update_many(selected_tickers: Optional[List[str]] = None, delay_sec: float = 1.0) -> Tuple[pd.DataFrame, List[str]]:
    """
    Massuppdatering med valfria tickers (None => alla). Sparar DF vid slutet.
    """
    df = df_or_reload_from_session()
    if df.empty:
        return df, ["Data-bladet är tomt."]
    df2, logs = mass_update_yahoo(df.copy(), tickers=selected_tickers, delay_sec=delay_sec)
    write_data_df(df2)
    st.session_state["DATA"] = df2
    return df2, logs

# ============================================================
# (Slut Del 2/6)
# Nästa del innehåller UI för Editor/Analys som anropar dessa funktioner och
# sätter upp riktkurs-beräkningar senare i Del 4/6.
# ============================================================

# ============================================================
# Del 3/6 — Beräkningsmotor
#  • Kompatibel wrapper: fetch_from_yahoo() (bygger på yahoo_snapshot)
#  • EPS-estimat från Yahoo (earnings_trend)
#  • Metodpriser: PE, EV/S, EV/EBITDA, DACF, P/B (+ placeholders)
#  • Multipel-decay & PE-ankare
#  • ✅ Fair Value (familjemedian + kurs-kopiafilter)
#  • compute_methods_for_row() → används av Analys/Ranking
#  • compute_fair_values_for_row() → kompakt extraktor
# ============================================================

# ---------- Wrapper mot Del 2:s snapshot ----------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """Mappar yahoo_snapshot() till neutrala nycklar för beräkningsmotorn."""
    s = yahoo_snapshot(ticker)
    return {
        "price":      _f(s.get("Aktuell kurs")),
        "currency":   (s.get("Valuta") or "USD"),
        "shares_out": _f(s.get("Utestående aktier")),
        "net_debt":   _f(s.get("Net debt")),
        "rev_ttm":    _f(s.get("Rev TTM")),
        "ebitda_ttm": _f(s.get("EBITDA TTM")),
        "eps_ttm":    _f(s.get("EPS TTM")),
        "pe_ttm":     _f(s.get("PE TTM")),
        "pe_fwd":     _f(s.get("PE FWD")),
        "ev_rev":     _f(s.get("EV/Revenue")),
        "ev_ebitda":  _f(s.get("EV/EBITDA")),
        "p_b":        _f(s.get("P/B")),
        "bvps":       _f(s.get("BVPS")),
        "dps_annual": _f(s.get("Årlig utdelning")),
        "rev_cagr_hist": _f(s.get("Rev CAGR")),
        "eps_cagr_hist": _f(s.get("EPS CAGR")),
    }

# ---------- EPS-estimat (Yahoo earnings_trend) ----------
@st.cache_data(ttl=900, show_spinner=False)
def _fetch_eps_estimates_yahoo(ticker: str) -> Dict[str, Optional[float]]:
    """
    Försöker läsa nästa års EPS (eps_1y), eps_2y (via longterm growth) och långsiktig EPS-CAGR.
    Returnerar None om inget hittas.
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

# ---------- Clamp & hjälpare ----------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

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
    # tillåt inte <=0 EBITDA i EV/EBITDA
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

# ---------- Paths (EPS/REV/EBITDA) ----------
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

# ---------- Fair Value = median av metodfamiljer ----------
def _compute_fair_value_row_v2(methods_df: pd.DataFrame, now_price: Optional[float]) -> Dict[str, Any]:
    """
    Median över oberoende metod-familjer:
      • pe_hist_vs_eps      → 'pe'
      • ev_sales            → 'ev_s'
      • ev_ebitda, ev_dacf  → 'ev_e' (räknas EN gång)
      • p_b                 → 'pb'
    Regler:
      • Dubbletter inom samma familj ignoreras.
      • 'Idag': filtrera bort värden ≈ aktuell kurs (±0.5 %) för att undvika tautologi.
        Fall-back till 'pe_hist_vs_eps' om allt filtreras bort.
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
            # Filtrera "kurs-kopia" i 'Idag'
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:  # ±0.5 %
                    continue
            used_fams.add(fam)
            vals.append(float(v))

        if not vals:
            # fall-back: ta PE-raden om den finns, annars NaN
            try:
                row_pe = methods_df[methods_df["Metod"] == "pe_hist_vs_eps"].iloc[0]
                out[c] = _f(row_pe.get(c))
            except Exception:
                out[c] = np.nan
        else:
            out[c] = float(np.median(vals))
    return out

# ---------- Huvudberäkning ----------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Returnerar:
      • methods_df: DataFrame [Metod, Idag, 1 år, 2 år, 3 år]
      • sanity    : text-rad med insikter
      • meta      : hjälpfält (valuta, pris, paths, clamped CAGRs, fair_value)
    Alla priser i aktiens handelsvaluta (ingen FX).
    """
    ticker = str(row.get("Ticker", "")).strip()
    y = fetch_from_yahoo(ticker)
    est = _fetch_eps_estimates_yahoo(ticker)

    # Inputs (fallback till Data-bladet)
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

    # --- Priser per metod (bolagets valuta) ---
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
    # DACF-proxy (samma som EV/EBITDA tills riktig FCF kopplas på)
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
    # Platshållare för struktur
    for m in ("p_nav", "p_tbv", "p_affo", "p_fcf", "ev_fcf", "p_nii"):
        methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år"])

    # --- ✅ Fair Value (familjemedian med kursfilter) ---
    fv_row = _compute_fair_value_row_v2(methods_df, price)
    methods_df = pd.concat([pd.DataFrame([fv_row]), methods_df], ignore_index=True)

    # --- Sanity-text ---
    sanity = (
        f"price={'ok' if price else '—'}, "
        f"eps_ttm={'ok' if (eps_ttm or eps_ttm==0) else '—'}, "
        f"eps_1y={'ok' if eps_1y_est else '—'}, eps_2y={'ok' if eps_2y_est else '—'}, "
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

# ---------- Kompakt extraktor ----------
def compute_fair_values_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Dict[str, Any]:
    """
    Kör compute_methods_for_row() men returnerar en kompakt dict för ranking/listor.
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
        "methods_df": methods_df,  # för debug/UI
    }

# ============================================================
# (Slut Del 3/6)
# Nästa del: UI för Analys/Ranking som använder compute_* ovan.
# ============================================================

# ============================================================
# Del 4/6 — Analys & Ranking (UI)
#  • render_analysis_view(): interaktiv analys av valt bolag
#  • Bläddringsknappar (Föregående/Nästa)
#  • Visar Fair Value (Idag/1/2/3 år), uppsida %, och metodtabell
#  • Ranking (manuell körning) baserat på fair_value vs kurs
#  • Hela databastabellen visas längst ned
#  • Alla priser i aktiens handelsvaluta (ingen FX-konvertering)
# ============================================================

# ---------- Hjälpfunktioner för UI-format ----------
def _r2(x: Any) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
            return None
        return float(f"{float(x):.2f}")
    except Exception:
        return None

def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    # (a/b - 1) i %
    try:
        if a is None or b is None or b == 0:
            return None
        return (float(a) / float(b) - 1.0) * 100.0
    except Exception:
        return None

def _fmt_money(v: Optional[float], ccy: str) -> str:
    v2 = _r2(v)
    if v2 is None:
        return "—"
    return f"{v2:,.2f} {ccy}".replace(",", " ").replace(".", ",")

def _fmt_pct(v: Optional[float]) -> str:
    p = _r2(v)
    if p is None:
        return "—"
    return f"{p:,.2f} %".replace(",", " ").replace(".", ",")

def _format_methods_for_display(df: pd.DataFrame, ccy: str) -> pd.DataFrame:
    disp = df.copy()
    for col in ["Idag","1 år","2 år","3 år"]:
        disp[col] = disp[col].apply(lambda x: _fmt_money(_f(x), ccy))
    return disp

# ---------- UI: Analys av enskilt bolag ----------
def _get_or_init_idx(key: str, n: int) -> int:
    if key not in st.session_state:
        st.session_state[key] = 0
    idx = st.session_state[key]
    if not isinstance(idx, int) or n <= 0:
        st.session_state[key] = 0
        return 0
    if idx < 0:
        idx = 0
    if idx >= n:
        idx = n - 1
    st.session_state[key] = idx
    return idx

def _bump_idx(key: str, delta: int, n: int) -> None:
    cur = _get_or_init_idx(key, n)
    cur = (cur + delta) % max(n, 1)
    st.session_state[key] = cur

def render_analysis_view(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    """
    Huvud-UI för analys & ranking. Anropas från main() i Del 6.
    Kräver compute_methods_for_row() och compute_fair_values_for_row() från Del 3.
    """
    st.header("Analys & riktkurser")

    if df_data is None or df_data.empty:
        st.info("Ingen data hittades i databasen.")
        return

    # --- Välj ticker + bläddring ---
    tickers = df_data["Ticker"].astype(str).fillna("").str.upper().tolist()
    tickers_unique = sorted(list(dict.fromkeys([t for t in tickers if t])))

    if not tickers_unique:
        st.warning("Inga tickers i databasen.")
        return

    cols_top = st.columns([3,1,1,1])
    with cols_top[0]:
        sel = st.selectbox("Välj bolag", tickers_unique, index=0, key="analysis_ticker_select")
    n_all = len(tickers_unique)
    cur_index_key = "analysis_idx"

    # Håll index i synk med selectbox
    if sel in tickers_unique:
        st.session_state[cur_index_key] = tickers_unique.index(sel)

    with cols_top[1]:
        if st.button("⟵ Föregående", use_container_width=True):
            _bump_idx(cur_index_key, -1, n_all)
    with cols_top[2]:
        if st.button("Nästa ⟶", use_container_width=True):
            _bump_idx(cur_index_key, +1, n_all)
    with cols_top[3]:
        st.caption(f"{_get_or_init_idx(cur_index_key, n_all)+1} / {n_all}")

    # Uppdatera vald ticker efter bläddring
    cur_idx = _get_or_init_idx(cur_index_key, n_all)
    sel_ticker = tickers_unique[cur_idx]

    # Hämta raden för vald ticker (första träff)
    row = df_data[df_data["Ticker"].astype(str).str.upper() == sel_ticker].iloc[0]

    # --- Kör beräkning för valt bolag ---
    with st.spinner(f"Beräknar riktkurser för {sel_ticker} …"):
        methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)

    currency = (meta.get("currency") or "USD")
    now_price = _f(meta.get("price"))
    fv = meta.get("fair_value") or {}
    fv_today = _f(fv.get("today"))
    fv_1y    = _f(fv.get("y1"))
    fv_2y    = _f(fv.get("y2"))
    fv_3y    = _f(fv.get("y3"))

    up_today = _pct(fv_today, now_price)
    up_1y    = _pct(fv_1y, now_price)
    up_2y    = _pct(fv_2y, now_price)
    up_3y    = _pct(fv_3y, now_price)

    # --- KPI-rad ---
    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
    kpi1.metric("Aktuell kurs", _fmt_money(now_price, currency))
    kpi2.metric("Fair Value — Idag", _fmt_money(fv_today, currency), _fmt_pct(up_today))
    kpi3.metric("Fair Value — 1 år", _fmt_money(fv_1y,    currency), _fmt_pct(up_1y))
    kpi4.metric("Fair Value — 2 år", _fmt_money(fv_2y,    currency), _fmt_pct(up_2y))
    kpi5.metric("Fair Value — 3 år", _fmt_money(fv_3y,    currency), _fmt_pct(up_3y))
    kpi6.metric("PE-ankare", f"{_r2(meta.get('pe_anchor')) or '—'}×")

    # --- Metodtabell (formaterad) ---
    st.subheader("Metoder (alla priser i bolagets valuta)")
    disp = _format_methods_for_display(methods_df, currency)
    st.dataframe(disp, use_container_width=True, hide_index=True)

    with st.expander("Diagnostik"):
        st.text(sanity)
        st.write("Inputs & paths", meta)

    st.markdown("---")

    # ---------- Ranking (manuell körning) ----------
    st.subheader("Ranking — Fair Value vs kurs (manuell körning)")
    colR1, colR2 = st.columns([1,3])
    with colR1:
        run_rank = st.button("Beräkna ranking nu")
    with colR2:
        st.caption("Tips: Kör vid behov. Hämtar data per bolag och kan ta en stund på stora listor.")

    if run_rank:
        results = []
        prog = st.progress(0)
        N = len(tickers_unique)
        for i, t in enumerate(tickers_unique, 1):
            try:
                r = df_data[df_data["Ticker"].astype(str).str.upper() == t].iloc[0]
                pack = compute_fair_values_for_row(r, settings, fx_map)
                # sammanställ för tabell
                price   = _f(pack.get("price"))
                fv0     = _f(pack.get("fv_today"))
                fv1     = _f(pack.get("fv_1y"))
                fv2     = _f(pack.get("fv_2y"))
                fv3     = _f(pack.get("fv_3y"))
                up0     = _pct(fv0, price)
                up1     = _pct(fv1, price)
                up2     = _pct(fv2, price)
                up3     = _pct(fv3, price)
                results.append({
                    "Ticker": pack.get("ticker"),
                    "Valuta": pack.get("currency"),
                    "Kurs": _r2(price),
                    "FV Idag": _r2(fv0),
                    "FV 1 år": _r2(fv1),
                    "FV 2 år": _r2(fv2),
                    "FV 3 år": _r2(fv3),
                    "Uppsida Idag %": _r2(up0),
                    "Uppsida 1 år %": _r2(up1),
                    "Uppsida 2 år %": _r2(up2),
                    "Uppsida 3 år %": _r2(up3),
                })
            except Exception:
                # fortsätt trots fel på enskilda rader
                pass
            prog.progress(min(i / max(N,1), 1.0))
        prog.empty()

        if results:
            rank_df = pd.DataFrame(results)
            # sortera på störst uppsida i % (Idag)
            rank_df = rank_df.sort_values(by=["Uppsida Idag %","Uppsida 1 år %"], ascending=False, na_position="last")
            # Formatera visning (pengar + %)
            df_show = rank_df.copy()
            def _fmt_row_money(v, ccy):
                return "—" if v is None or (isinstance(v,float) and math.isnan(v)) else f"{v:,.2f} {ccy}".replace(",", " ").replace(".", ",")
            def _fmt_row_pct(v):
                return "—" if v is None or (isinstance(v,float) and math.isnan(v)) else f"{v:,.2f} %".replace(",", " ").replace(".", ",")
            # Bygg visning rad för rad m.h.t. valuta
            rows = []
            for _, rr in df_show.iterrows():
                ccy = rr["Valuta"] or "USD"
                rows.append({
                    "Ticker": rr["Ticker"],
                    "Kurs": _fmt_row_money(rr["Kurs"], ccy),
                    "FV Idag": _fmt_row_money(rr["FV Idag"], ccy),
                    "FV 1 år": _fmt_row_money(rr["FV 1 år"], ccy),
                    "FV 2 år": _fmt_row_money(rr["FV 2 år"], ccy),
                    "FV 3 år": _fmt_row_money(rr["FV 3 år"], ccy),
                    "Uppsida Idag %": _fmt_row_pct(rr["Uppsida Idag %"]),
                    "Uppsida 1 år %": _fmt_row_pct(rr["Uppsida 1 år %"]),
                    "Uppsida 2 år %": _fmt_row_pct(rr["Uppsida 2 år %"]),
                    "Uppsida 3 år %": _fmt_row_pct(rr["Uppsida 3 år %"]),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            st.session_state["ranking_last"] = rank_df  # spara rådata om det behövs i andra vyer
        else:
            st.info("Inga rankingresultat kunde beräknas.")

    st.markdown("---")

    # ---------- Hela databasen, enkel tabell ----------
    st.subheader("Hela databasen (ofiltererad)")
    # visa ett urval av kolumner först om tabellen är massiv; annars hela
    try:
        preferred_cols = [
            "Ticker","Valuta","Aktuell kurs","Utestående aktier","Net debt",
            "Rev TTM","EBITDA TTM","EPS TTM","PE TTM","PE FWD","EV/Revenue","EV/EBITDA",
            "P/B","BVPS","Rev CAGR","EPS CAGR"
        ]
        show_cols = [c for c in preferred_cols if c in df_data.columns]
        base = df_data[show_cols] if show_cols else df_data
        st.dataframe(base, use_container_width=True)
    except Exception:
        st.dataframe(df_data, use_container_width=True)

# ============================================================
# (Slut Del 4/6)
# Nästa del: Portfölj/Editor eller övriga vyer beroende på din basstruktur.
# ============================================================

# ============================================================
# Del 5/6 — Vyer
#  • Settings  (redigerbar)
#  • Snapshot  (read-only)
#  • Editor    (manuellt + Yahoo-prefill)
#  • Lägg till (ny ticker)
#  • Portfölj  (värden + kommande utdelningar)
#  • Köpförslag (läser från Data-bladet)
#  • Massuppdatering (Yahoo) — 1s/bolag
#  • Små UI-hjälpare för sök & navigation
# ============================================================

# ---------- Små UI-hjälpare (sök + nav) ----------
def _names_map_from_df(df: pd.DataFrame) -> Dict[str, str]:
    out = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        t = str(r.get("Ticker") or "").upper()
        if not t:
            continue
        n = str(r.get("Bolagsnamn") or "").strip()
        out[t] = f"{t} — {n}" if n else t
    return out

def _select_with_search_nav(label: str, tickers: List[str], names_map: Dict[str, str],
                            idx_key: str, query_key: str) -> Optional[str]:
    tickers = sorted([t for t in set([str(t).upper().strip() for t in tickers]) if t])
    if not tickers:
        st.warning("Inga tickers att välja på.")
        return None

    if idx_key not in st.session_state:
        st.session_state[idx_key] = 0
    if query_key not in st.session_state:
        st.session_state[query_key] = ""

    q = st.text_input("Sök (ticker/namn)", value=st.session_state[query_key], key=query_key)
    if q:
        ql = q.strip().lower()
        filtered = [t for t in tickers if (ql in t.lower()) or (ql in names_map.get(t, "").lower())]
        if filtered:
            tickers = filtered

    # säkra index inom spann
    if st.session_state[idx_key] >= len(tickers):
        st.session_state[idx_key] = 0

    col_sel, col_nav = st.columns([3, 1])
    with col_sel:
        options = [names_map.get(t, t) for t in tickers]
        sel_disp = st.selectbox(label, options=options, index=st.session_state[idx_key] if options else 0)
        # översätt tillbaka till ticker
        rev = {v: k for k, v in names_map.items()}
        sel = rev.get(sel_disp, sel_disp.split(" — ")[0])
    with col_nav:
        st.write(f"{st.session_state[idx_key]+1}/{len(tickers)}")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("◀︎", use_container_width=True):
                st.session_state[idx_key] = (st.session_state[idx_key] - 1) % len(tickers)
        with c2:
            if st.button("▶︎", use_container_width=True):
                st.session_state[idx_key] = (st.session_state[idx_key] + 1) % len(tickers)

    # håll selectbox & index i synk
    try:
        st.session_state[idx_key] = tickers.index(sel)
    except Exception:
        pass
    return sel

# ============================================================
# Settings (redigerbar)
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
# Snapshot (read-only)
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

# ============================================================
# Portfölj (värden + kommande utdelningar)
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
# Köpförslag – läser från Data-bladet
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

# ============================================================
# Massuppdatering (Yahoo) — 1s per bolag
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
# Del 6/6 — Analys & Ranking + main()  ✅ FIX
#  • CHANGED: Riktkurser hämtas från Del 3: compute_methods_for_row()
#  • Skriver FV (Idag/1/2/3 år) till Data-bladet (oförändrade siffror)
#  • Ranking på vald horisont
#  • Main + enkel sidomeny
# ============================================================

# ---------- Hjälpare ----------
def _safe_currency(c):
    return (str(c) if c else "USD").upper()

def _calc_upside(target: Optional[float], price: Optional[float]) -> Optional[float]:
    t = _f(target)
    p = _f(price)
    if t is None or p is None or p <= 0:
        return None
    return (t / p) - 1.0

# ---------- CHANGED: använd Del 3:s fair value rakt av ----------
def _fv_from_row(row: pd.Series, settings: Dict[str, Any], fx_map: Dict[str, float]) -> Dict[str, Any]:
    """
    Returnerar fair value från compute_methods_for_row():
      { 'today': x, 'y1': y, 'y2': z, 'y3': w, 'price': p, 'currency': c, 'sanity': str }
    """
    methods_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    fv = meta.get("fair_value") or {}
    return {
        "today": _f(fv.get("today")),
        "y1": _f(fv.get("y1")),
        "y2": _f(fv.get("y2")),
        "y3": _f(fv.get("y3")),
        "price": _f(meta.get("price")),
        "currency": meta.get("currency") or row.get("Valuta") or "USD",
        "sanity": sanity,
        "methods_df": methods_df,
    }

def _write_fv_into_df(df: pd.DataFrame, settings: Dict[str, Any], fx_map: Dict[str, float]) -> pd.DataFrame:
    """
    CHANGED: Beräknar FV via Del 3 och skriver till:
      Riktkurs idag / 1 år / 2 år / 3 år
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    for need in ["Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år"]:
        if need not in out.columns:
            out[need] = np.nan

    for i, r in out.iterrows():
        try:
            res = _fv_from_row(r, settings, fx_map)
            out.at[i, "Riktkurs idag"] = res["today"]
            out.at[i, "Riktkurs 1 år"]  = res["y1"]
            out.at[i, "Riktkurs 2 år"]  = res["y2"]
            out.at[i, "Riktkurs 3 år"]  = res["y3"]
        except Exception:
            # fortsätt även om en rad fallerar
            continue
    return out

# ---------- Analys & Ranking (FV från Del 3) ----------
def page_analysis_ranking():
    st.header("📊 Analys & Ranking (FV från metodfamilj)")

    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    col1, col2 = st.columns([1,1])
    with col1:
        horizon = st.selectbox("Ranking-horisont", ["Idag","1 år","2 år","3 år"], index=0)
    with col2:
        run = st.button("🔁 Beräkna & skriv FV till Data")

    if run:
        with st.spinner("Beräknar fair value för alla rader…"):
            new_df = _write_fv_into_df(df, settings, fx_map)
            write_data_df(new_df)
            st.session_state["DATA"] = new_df
            df = new_df
            st.success("Klart – FV sparat till Data-bladet.")

    # Rankingvy (läser FV-kolumnerna i Data)
    hcol = {"Idag":"Riktkurs idag", "1 år":"Riktkurs 1 år", "2 år":"Riktkurs 2 år", "3 år":"Riktkurs 3 år"}[horizon]
    base = df.copy()
    for c in ("Aktuell kurs", hcol):
        if c in base.columns:
            base[c] = pd.to_numeric(base[c], errors="coerce")

    rows = []
    for _, r in base.iterrows():
        try:
            px = _f(r.get("Aktuell kurs"))
            tgt = _f(r.get(hcol))
            if px is None or tgt is None or px <= 0:
                continue
            up = (tgt - px)/px*100.0
            rows.append({
                "Ticker": str(r.get("Ticker") or "").upper(),
                "Bolagsnamn": str(_nz(r.get("Bolagsnamn"), "")),
                "Bucket": str(_nz(r.get("Bucket"), "")),
                "Valuta": _safe_currency(r.get("Valuta")),
                "Aktuell kurs": float(px),
                hcol: float(tgt),
                "Uppsida (%)": float(up),
            })
        except Exception:
            continue

    if not rows:
        st.info("Inget att visa ännu. Kör beräkningen ovan först eller kontrollera att 'Aktuell kurs' och FV finns.")
        return

    tbl = pd.DataFrame(rows, columns=["Ticker","Bolagsnamn","Bucket","Valuta","Aktuell kurs",hcol,"Uppsida (%)"])
    tbl = tbl.sort_values("Uppsida (%)", ascending=False).reset_index(drop=True)

    # Visning med 2 decimaler (endast UI)
    vis = tbl.copy()
    vis["Aktuell kurs"] = vis["Aktuell kurs"].map(lambda v: f"{v:.2f}")
    vis[hcol] = vis[hcol].map(lambda v: f"{v:.2f}")
    vis["Uppsida (%)"] = vis["Uppsida (%)"].map(lambda v: f"{v:.1f}%")
    st.dataframe(vis, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("🔎 Enskild analys (samma FV-motor)")
    opts = df["Ticker"].dropna().astype(str).unique().tolist()
    tkr = st.selectbox("Välj bolag", sorted(opts))
    if tkr:
        row = df[df["Ticker"].astype(str) == tkr].iloc[0]
        res = _fv_from_row(row, settings, fx_map)
        price = res["price"]; curr = _safe_currency(res["currency"])
        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Kurs", f"{(price is not None and f'{price:.2f}') or '—'} {curr}")
        c2.metric("FV idag", f"{(res['today'] is not None and f'{res['today']:.2f}') or '—'} {curr}",
                  delta=f"{((_calc_upside(res['today'], price) or 0)*100):.1f}%")
        c3.metric("FV 1 år", f"{(res['y1'] is not None and f'{res['y1']:.2f}') or '—'} {curr}",
                  delta=f"{((_calc_upside(res['y1'], price) or 0)*100):.1f}%")
        c4.metric("FV 2 år", f"{(res['y2'] is not None and f'{res['y2']:.2f}') or '—'} {curr}",
                  delta=f"{((_calc_upside(res['y2'], price) or 0)*100):.1f}%")
        c5.metric("FV 3 år", f"{(res['y3'] is not None and f'{res['y3']:.2f}') or '—'} {curr}",
                  delta=f"{((_calc_upside(res['y3'], price) or 0)*100):.1f}%")
        st.caption(f"Sanity: {res['sanity']}")
        st.subheader("Metodtabell")
        st.dataframe(_fmt_methods_for_display(res["methods_df"]), use_container_width=True, hide_index=True)

# ---------- Main & Routing ----------
def _load_bootstrap_into_session():
    if "DATA" not in st.session_state or st.session_state.get("DATA") is None:
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception:
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
    if "SETTINGS_MAP" not in st.session_state:
        try:
            st.session_state["SETTINGS_MAP"] = get_settings_map()
        except Exception:
            st.session_state["SETTINGS_MAP"] = {}
    if "FX" not in st.session_state:
        try:
            st.session_state["FX"] = get_fx_map()
        except Exception:
            st.session_state["FX"] = {}

def main():
    _load_bootstrap_into_session()

    PAGES = {
        "Analys & Ranking": page_analysis_ranking,
        "Portfölj": page_portfolio,
        "Köpförslag": page_buy_suggestions,
        "Editor": page_editor,
        "Lägg till": page_add_ticker,
        "Snapshot": page_snapshot,
        "Settings": page_settings,
        "Massuppdatering": page_batch,
    }

    with st.sidebar:
        st.header("📚 Navigering")
        choice = st.radio("Vyer", list(PAGES.keys()), index=0)
        st.caption("Denna vy använder FV från Del 3 (familjemedian + decay).")

    try:
        PAGES[choice]()
    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# Kör appen
if __name__ == "__main__":
    main()
