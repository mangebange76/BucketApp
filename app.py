# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 1/6: Bas & infrastruktur
#
#  - Streamlit setup
#  - Hjälpfunktioner (_f, _pos, _nz, now_stamp, etc)
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
st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")
st.markdown("<style>section.main > div {max-width: 1500px;}</style>", unsafe_allow_html=True)

# =========================
# Globala konstanter
# =========================

# Google Sheet: titlar på flikar
DATA_TITLE      = "Data"
FX_TITLE        = "Valutakurser"
SETTINGS_TITLE  = "Settings"
SNAPSHOT_TITLE  = "Snapshot"

# Settings-bladets kolumner (nyckel → värde)
SETTINGS_COLUMNS = ["Nyckel", "Värde"]

# Data-bladets grundschema (ordningen kan vara längre i verkligheten,
# men dessa kolumner förväntas finnas / kan fyllas ut).
DATA_COLUMNS: List[str] = [
    "Timestamp",
    "Ticker",
    "Bolagsnamn",
    "Sektor",
    "Bucket",
    "Valuta",
    "Antal aktier",
    "GAV (SEK)",
    "Aktuell kurs",
    "Utestående aktier",
    "Net debt",
    "Rev TTM",
    "EBITDA TTM",
    "EPS TTM",
    "PE TTM",
    "PE FWD",
    "EV/Revenue",
    "EV/EBITDA",
    "P/B",
    "BVPS",
    "Rev 1Y",
    "Rev 2Y",
    "Rev CAGR",
    "EPS 1Y",
    "EPS 2Y",
    "EPS CAGR",
    "Årlig utdelning",
    "Utdelning CAGR",
    "Riktkurs idag",
    "Riktkurs 1 år",
    "Riktkurs 2 år",
    "Riktkurs 3 år",
    "Bull 1 år",
    "Bear 1 år",
    "Senast auto uppdaterad",
    "Auto källa",
    "Senast manuellt uppdaterad",
]

# Standard-Buckets (används i editor/add-ticker)
DEFAULT_BUCKETS: List[str] = [
    "Bucket A tillväxt",
    "Bucket B tillväxt",
    "Bucket C tillväxt",
    "Bucket A utdelning",
    "Bucket B utdelning",
    "Bucket C utdelning",
]

# =========================
# Hjälpfunktioner (tal, tid)
# =========================
def now_stamp() -> str:
    """Returnerar en enkel tidsstämpel i format YYYY-MM-DD HH.MM.SS."""
    return dt.datetime.now().strftime("%Y-%m-%d %H.%M.%S")

def _nz(x: Any, default: Any = None) -> Any:
    """Returnera x om x inte är None/NaN/tom sträng, annars default."""
    if x is None:
        return default
    if isinstance(x, float) and (math.isnan(x) or x != x):
        return default
    if isinstance(x, str) and x.strip() == "":
        return default
    return x

def _f(x: Any) -> Optional[float]:
    """
    Robust float-parser:
      - Accepterar svenska format (komma som decimal, mellanslag tusentalsavskiljare)
      - Returnerar None om det inte går att tolka
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            if isinstance(x, float) and (math.isnan(x) or x != x):
                return None
            return float(x)
        except Exception:
            return None
    s = str(x).strip()
    if s == "":
        return None
    # Ta bort mellanslag (tusentalsavskiljare) och ersätt komma med punkt
    s = s.replace(" ", "").replace("\u00a0", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _pos(x: Any) -> Optional[float]:
    """Som _f men returnerar endast positiva (>= 0) tal, annars None."""
    v = _f(x)
    if v is None:
        return None
    try:
        if not math.isfinite(v):
            return None
    except Exception:
        return None
    return v

# =============================
# Google auth & Spreadsheet
# =============================

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hanterar privata nycklar där radbrytningar är ersatta med '\\n' i secrets.
    """
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _env_or_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Letar efter en nyckel i både os.environ och st.secrets, med flera alias.

    Exempel:
      - "SHEET_URL" → letar även efter GOOGLE_SHEET_URL, spreadsheet_url osv.
      - "SHEET_ID"  → letar även efter GOOGLE_SHEET_ID, SPREADSHEET_ID osv.
    """
    key_upper = key.upper()

    # Grundkandidater
    candidates = {key, key.upper(), key.lower()}

    # Alias för URL
    if key_upper in {"SHEET_URL", "GOOGLE_SHEET_URL"}:
        candidates.update(
            {
                "SHEET_URL",
                "sheet_url",
                "GOOGLE_SHEET_URL",
                "google_sheet_url",
                "SPREADSHEET_URL",
                "spreadsheet_url",
            }
        )

    # Alias för ID
    if key_upper in {"SHEET_ID", "GOOGLE_SHEET_ID", "SPREADSHEET_ID"}:
        candidates.update(
            {
                "SHEET_ID",
                "sheet_id",
                "GOOGLE_SHEET_ID",
                "google_sheet_id",
                "SPREADSHEET_ID",
                "spreadsheet_id",
            }
        )

    # Sök i miljövariabler först
    for name in candidates:
        val = os.environ.get(name)
        if val:
            return str(val)

    # Sedan i Streamlit secrets (om det finns)
    try:
        secrets_obj = getattr(st, "secrets", None)
    except Exception:
        secrets_obj = None

    if secrets_obj is not None:
        for name in candidates:
            try:
                val = secrets_obj.get(name)
            except Exception:
                val = None
            if val:
                return str(val)

    return default

@st.cache_resource(show_spinner=False)
def _get_gspread_client() -> gspread.Client:
    """
    Bygger en gspread-klient från st.secrets["GOOGLE_CREDENTIALS"].
    Stöder:
      - dict
      - JSON-sträng
    """
    raw = st.secrets.get("GOOGLE_CREDENTIALS", None)
    if raw is None:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i Streamlit secrets.")

    if isinstance(raw, Mapping):
        creds_dict = dict(raw)
    else:
        # Förväntar oss JSON-sträng
        try:
            creds_dict = json.loads(str(raw))
        except Exception as e:
            raise RuntimeError(f"Kunde inte tolka GOOGLE_CREDENTIALS som JSON: {e}") from e

    creds_dict = _normalize_private_key(creds_dict)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet() -> Spreadsheet:
    """
    Öppnar kalkylarket med stöd för flera olika nycklar:
      - SHEET_URL / GOOGLE_SHEET_URL / spreadsheet_url
      - SHEET_ID  / GOOGLE_SHEET_ID  / SPREADSHEET_ID
    Funkar därmed med samma secrets som din basversion.
    """
    # Först försök med URL
    sheet_url = _env_or_secret("SHEET_URL")
    # Sedan ID som fallback
    sheet_id = _env_or_secret("SHEET_ID")

    client = _get_gspread_client()

    if sheet_url and str(sheet_url).strip():
        return client.open_by_url(sheet_url)

    if sheet_id and str(sheet_id).strip():
        return client.open_by_key(sheet_id)

    raise RuntimeError(
        "Ange SHEET_URL eller SHEET_ID (eller GOOGLE_SHEET_URL / GOOGLE_SHEET_ID) "
        "i Streamlit secrets eller som miljövariabler."
    )

def _open_worksheet(title: str) -> Worksheet:
    """
    Öppna (eller skapa) en flik i Spreadsheet med angivet title.
    """
    ss = _open_spreadsheet()
    try:
        return ss.worksheet(title)
    except WorksheetNotFound:
        # Skapa ny med tomma kolumner
        ws = ss.add_worksheet(title=title, rows=200, cols=50)
        return ws

# =============================
# Läs/skriv DataFrame <-> Sheet
# =============================
def _read_df(title: str) -> pd.DataFrame:
    """
    Läs en flik som DataFrame.
    Första raden antas vara header. Tomt → tom DataFrame.
    """
    try:
        ws = _open_worksheet(title)
    except Exception as e:
        st.error(f"Kunde inte öppna blad '{title}': {e}")
        return pd.DataFrame()

    try:
        values = ws.get_all_values()
    except APIError as e:
        st.error(f"API-fel vid läsning av blad '{title}': {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Fel vid läsning av blad '{title}': {e}")
        return pd.DataFrame()

    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    if not header:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=header)

    # Trimma tomma rader
    df = df.dropna(how="all").reset_index(drop=True)
    return df

def _write_df(title: str, df: pd.DataFrame) -> None:
    """
    Skriv en DataFrame till en flik. Första raden = header.
    Överskriver hela bladet.
    """
    if df is None:
        df = pd.DataFrame()
    df = df.copy()

    # Konvertera alla kolumner till str för Sheets-kompabilitet
    df = df.fillna("")
    df = df.astype(str)

    ws = _open_worksheet(title)
    # Rensa och skriv om
    try:
        ws.clear()
        if df.empty:
            return
        values = [list(df.columns)] + df.values.tolist()
        ws.update(values)
    except APIError as e:
        st.error(f"API-fel vid skrivning till blad '{title}': {e}")
        raise
    except Exception as e:
        st.error(f"Fel vid skrivning till blad '{title}': {e}")
        raise

# =============================
# Hjälpare för DATA-bladet
# =============================
def _ensure_data_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Säkerställ att alla DATA_COLUMNS finns. Lägg till saknade som NaN.
    Behåller även ev. extra kolumner (t.ex. manuella).
    """
    if df is None or df.empty:
        df = pd.DataFrame(columns=DATA_COLUMNS)
    for col in DATA_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    # Behåll kolumnordning: DATA_COLUMNS först, sedan övriga
    extra = [c for c in df.columns if c not in DATA_COLUMNS]
    df = df[DATA_COLUMNS + extra]
    return df

def read_data_df() -> pd.DataFrame:
    """
    Läs Data-bladet från Sheets och säkerställ kolumnschema.
    """
    df = _read_df(DATA_TITLE)
    df = _ensure_data_columns(df)
    return df

def write_data_df(df: pd.DataFrame) -> None:
    """
    Skriv Data-bladet till Sheets, med DATA_COLUMNS först.
    """
    if df is None:
        df = pd.DataFrame(columns=DATA_COLUMNS)
    df = _ensure_data_columns(df)
    _write_df(DATA_TITLE, df)

# =============================
# Settings-hantering
# =============================
@st.cache_data(ttl=300, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    """
    Läser Settings-bladet och returnerar en dict:
      { 'nyckel': 'värde', ... }
    Stöder både 'Nyckel'/'Värde' och 'Key'/'Value'.
    """
    df = _read_df(SETTINGS_TITLE)
    if df is None or df.empty:
        return {}

    key_col = None
    val_col = None
    for cand in ("Nyckel", "Key", "Setting", "Inställning"):
        if cand in df.columns:
            key_col = cand
            break
    for cand in ("Värde", "Varde", "Value"):
        if cand in df.columns:
            val_col = cand
            break

    if key_col is None or val_col is None:
        # Försök tolka första två kolumner
        if len(df.columns) >= 2:
            key_col = df.columns[0]
            val_col = df.columns[1]
        else:
            return {}

    settings: Dict[str, str] = {}
    for _, r in df.iterrows():
        k_raw = r.get(key_col)
        if k_raw is None:
            continue
        k = str(k_raw).strip()
        if not k:
            continue
        v = r.get(val_col)
        settings[k] = "" if v is None else str(v).strip()
    return settings

# =============================
# FX-hantering (Valutakurser)
# =============================
@st.cache_data(ttl=300, show_spinner=False)
def get_fx_map() -> Dict[str, float]:
    """
    Läser 'Valutakurser'-bladet och returnerar:
      { 'USD': 10.50, 'NOK': 1.02, ... }  (valuta → SEK-kurs)
    Förväntar kolumner typ ['Valuta','SEK'] eller liknande.
    """
    df = _read_df(FX_TITLE)
    if df is None or df.empty:
        return {}

    # Hitta valuta-kolumn
    cur_col = None
    for cand in ("Valuta", "Currency", "CUR", "Fx", "FX"):
        if cand in df.columns:
            cur_col = cand
            break
    # Hitta SEK-kolumn
    rate_col = None
    for cand in ("SEK", "Kurs", "Rate", "Fx-rate"):
        if cand in df.columns:
            rate_col = cand
            break

    if cur_col is None or rate_col is None:
        return {}

    out: Dict[str, float] = {}
    for _, r in df.iterrows():
        c = r.get(cur_col)
        if c is None:
            continue
        code = str(c).strip().upper()
        if not code:
            continue
        val = _f(r.get(rate_col))
        if val is None or not math.isfinite(val) or val <= 0:
            continue
        out[code] = float(val)
    # Bas: SEK = 1.0
    if "SEK" not in out:
        out["SEK"] = 1.0
    return out

# =============================
# Laddning av DATA i session
# =============================
def _load_data_into_session() -> None:
    """
    Hjälpare som ser till att st.session_state["DATA"] är laddad.
    Anropas från main() (Del 6).
    """
    if "DATA" not in st.session_state or not isinstance(st.session_state["DATA"], pd.DataFrame):
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception as e:
            st.error(f"Kunde inte ladda Data-bladet: {e}")
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)

# (Slut Del 1/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 2/6: Datainhämtning (Yahoo) & uppdateringshjälpare
#
#  - Säkra wrappers för yfinance (pris, valuta, shares, PE, EPS, TTM)
#  - Fältmappning → Data-bladets kolumner
#  - Uppdatera en rad / massuppdatering (utan UI)
#
# Viktigt:
# • Ingen ändring av riktkurslogik här.
# • Endast säkra hämtningar och försiktig skrivning.
# ============================================================

# ------------------------------
# yfinance-hjälpare (robusta)
# ------------------------------
def _yf_ticker(sym: str):
    try:
        return yf.Ticker(sym)
    except Exception:
        return None

def _yf_last_price(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # 1) fast_info
    try:
        fi = tkr.fast_info
        px = getattr(fi, "last_price", None)
        if px:
            return float(px)
    except Exception:
        pass
    # 2) info
    try:
        info = tkr.info
        px = info.get("currentPrice") or info.get("regularMarketPrice")
        if px:
            return float(px)
    except Exception:
        pass
    # 3) history fallback
    try:
        h = tkr.history(period="5d")
        if not h.empty:
            return float(h["Close"].dropna().iloc[-1])
    except Exception:
        pass
    return None

def _yf_currency(tkr) -> Optional[str]:
    if tkr is None:
        return None
    # fast_info
    try:
        fi = tkr.fast_info
        c = getattr(fi, "currency", None)
        if c:
            return str(c).upper()
    except Exception:
        pass
    # info
    try:
        info = tkr.info
        c = info.get("currency")
        if c:
            return str(c).upper()
    except Exception:
        pass
    return None

def _yf_shares_out(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # fast_info
    try:
        fi = tkr.fast_info
        s = getattr(fi, "shares", None)
        if s and s > 0:
            return float(s)
    except Exception:
        pass
    # info
    try:
        info = tkr.info
        s = info.get("sharesOutstanding")
        if s and s > 0:
            return float(s)
    except Exception:
        pass
    # get_shares_full (senaste kända)
    try:
        df = tkr.get_shares_full()
        if df is not None and not df.empty:
            val = float(df["SharesOutstanding"].dropna().iloc[-1])
            if val > 0:
                return val
    except Exception:
        pass
    return None

def _yf_eps_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        fi = tkr.fast_info
        v = getattr(fi, "trailingEps", None)
        if v and v == v:
            return float(v)
    except Exception:
        pass
    try:
        info = tkr.info
        v = info.get("trailingEps")
        if v and v == v:
            return float(v)
    except Exception:
        pass
    return None

def _yf_pe_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        fi = tkr.fast_info
        v = getattr(fi, "trailingPe", None)
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    try:
        info = tkr.info
        v = info.get("trailingPE")
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_pe_fwd(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("forwardPE")
        if v and v == v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_rev_ttm(tkr) -> Optional[float]:
    # Intäkter TTM – plocka från financials/trailingTotalRevenue om möjligt.
    if tkr is None:
        return None
    # info
    try:
        info = tkr.info
        v = info.get("totalRevenue") or info.get("trailingTotalRevenue")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    # income stmt
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            if "Total Revenue" in fin.index:
                vals = fin.loc["Total Revenue"].dropna()
                if not vals.empty:
                    return float(vals.iloc[0])
    except Exception:
        pass
    return None

def _yf_ebitda_ttm(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("ebitda")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    try:
        fin = tkr.financials
        if fin is not None and not fin.empty:
            if "Ebitda" in fin.index:
                vals = fin.loc["Ebitda"].dropna()
                if not vals.empty:
                    return float(vals.iloc[0])
    except Exception:
        pass
    return None

def _yf_p_b(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("priceToBook")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_bvps(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("bookValue")
        if v and v == v:
            return float(v)
    except Exception:
        pass
    return None

def _yf_ev_rev(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("enterpriseToRevenue")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_ev_ebitda(tkr) -> Optional[float]:
    if tkr is None:
        return None
    try:
        info = tkr.info
        v = info.get("enterpriseToEbitda")
        if v and v > 0:
            return float(v)
    except Exception:
        pass
    return None

def _yf_dividend_annual(tkr) -> Optional[float]:
    if tkr is None:
        return None
    # info → trailingAnnualDividendRate
    try:
        info = tkr.info
        v = info.get("trailingAnnualDividendRate")
        if v and v >= 0:
            return float(v)
    except Exception:
        pass
    # dividends-serien → summera senaste 12m
    try:
        divs = tkr.dividends
        if divs is not None and not divs.empty:
            last_12m = divs[divs.index >= (dt.datetime.utcnow() - dt.timedelta(days=365))]
            s = float(last_12m.sum())
            if s >= 0:
                return s
    except Exception:
        pass
    return None

# ------------------------------
# Hämta ett paket för en ticker
# ------------------------------
def yahoo_fetch_for_ticker(sym: str) -> Dict[str, Any]:
    tkr = _yf_ticker(sym)
    out: Dict[str, Any] = {
        "Aktuell kurs": _yf_last_price(tkr),
        "Valuta": _yf_currency(tkr),
        "Utestående aktier": _yf_shares_out(tkr),
        "EPS TTM": _yf_eps_ttm(tkr),
        "PE TTM": _yf_pe_ttm(tkr),
        "PE FWD": _yf_pe_fwd(tkr),
        "Rev TTM": _yf_rev_ttm(tkr),
        "EBITDA TTM": _yf_ebitda_ttm(tkr),
        "EV/Revenue": _yf_ev_rev(tkr),
        "EV/EBITDA": _yf_ev_ebitda(tkr),
        "P/B": _yf_p_b(tkr),
        "BVPS": _yf_bvps(tkr),
        "Årlig utdelning": _yf_dividend_annual(tkr),
        # Dessa lämnas orörda här (kan hämtas från andra källor / manuellt):
        "Net debt": None,
        "EPS 1Y": None, "EPS 2Y": None,
        "Rev 1Y": None, "Rev 2Y": None,
        "Rev CAGR": None, "EPS CAGR": None,
        "Utdelning CAGR": None,
    }
    return out

# --------------------------------------------
# Försiktig skrivning till Data-blad per rad
# --------------------------------------------
def _apply_fetch_to_row(row: pd.Series, fetched: Dict[str, Any]) -> pd.Series:
    """
    Endast skriva över de fält som har icke-None och meningsfulla värden.
    Respekterar principen: skriv över endast det som kunde hämtas.
    """
    if not isinstance(row, pd.Series):
        row = pd.Series(row)

    for key, val in fetched.items():
        if key not in row.index:
            continue
        if val is None:
            continue
        # Om numeriskt: NaN/None skydd
        if isinstance(val, (int, float)) and not math.isfinite(float(val)):
            continue
        row[key] = val
    # Stämpla auto-källa/tid
    row["Senast auto uppdaterad"] = now_stamp()
    row["Auto källa"] = "Yahoo Finance"
    return row

def update_one_row_from_yahoo(df: pd.DataFrame, idx: int) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Uppdaterar en (1) rad i Data-bladet från Yahoo (om möjligt).
    Returnerar (df, status_dict).
    """
    if df is None or df.empty or idx < 0 or idx >= len(df):
        return df, {"ok": False, "msg": "Ogiltig radindex eller tom Data."}

    sym = str(df.at[idx, "Ticker"]).strip() if "Ticker" in df.columns else ""
    if not sym:
        return df, {"ok": False, "msg": "Saknar Ticker i vald rad."}

    try:
        fetched = yahoo_fetch_for_ticker(sym)
        row = df.iloc[idx].copy()
        row = _apply_fetch_to_row(row, fetched)
        df.iloc[idx] = row
        return df, {"ok": True, "msg": f"Uppdaterade {sym} från Yahoo."}
    except Exception as e:
        return df, {"ok": False, "msg": f"Fel vid uppdatering av {sym}: {e}"}

def mass_update_from_yahoo(df: pd.DataFrame, idx_list: List[int], sleep_sec: float = 1.0) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Massuppdaterar valda rader (en i taget) med fördröjning.
    Skriver inte till Sheets här (UI-delen sköter sparning).
    """
    if df is None or df.empty:
        return df, [{"ok": False, "msg": "Tom Data."}]
    logs: List[Dict[str, Any]] = []
    for i, idx in enumerate(idx_list, start=1):
        df, status = update_one_row_from_yahoo(df, idx)
        status["seq"] = f"{i}/{len(idx_list)}"
        logs.append(status)
        time.sleep(max(0.0, float(sleep_sec)))
    return df, logs

# (Slut Del 2/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 3/6: Beräkningsmotor (auto-val av metod & riktkurser)
#
#  - fetch_from_yahoo(): wrapper runt yahoo_fetch_for_ticker (Del 2)
#  - EPS-estimat från Yahoo (earnings_trend)
#  - AUTO-PROFIL: väljer vilka metodfamiljer som passar (per sektor/mått)
#  - Metodpriser: PE, EV/S, EV/EBITDA, P/B (+ placeholders för struktur)
#  - Multipel-decay & PE-ankare
#  - Fair Value = median över valda metodfamiljer (v3)
#  - Riktkurser 1–3 år = “bästa scenario” med MoS per bucket (A 5%, B 8%, C 12%)
#  - compute_methods_for_row() → DICT (targets + metadata + methods_df)
#  - compute_fair_values_for_row() → kompakt DICT för UI
# ============================================================

# -------------------------
# Wrapper: Del 2 → Del 3
# -------------------------
@st.cache_data(ttl=900, show_spinner=False)
def fetch_from_yahoo(ticker: str) -> Dict[str, Any]:
    """
    Mappa Del 2:s yahoo_fetch_for_ticker() till stabila nycklar för beräkningsmotorn.
    Alla värden är i aktiens handelsvaluta.
    """
    snap = yahoo_fetch_for_ticker(ticker)
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
        # Historiska CAGRs kan saknas i Del 2; beräkningsmotor hanterar None.
        "rev_cagr_hist":    _f(snap.get("Rev CAGR")),
        "eps_cagr_hist":    _f(snap.get("EPS CAGR")),
    }

# -------------------------
# Clamp-gränser (stabila)
# -------------------------
REV_CAGR_MIN = -0.10   # -10 %
REV_CAGR_MAX =  0.35   # +35 %
EPS_CAGR_MIN = -0.20   # -20 %
EPS_CAGR_MAX =  0.35   # +35 %

# -------------------------
# Små hjälpare (beräkning)
# -------------------------
def _decay_multiple(mult0: Optional[float], years: int, decay: float, floor_frac: float = 0.60) -> Optional[float]:
    """
    Exponentiell kompression av multipel:
      mult_y = mult0 * (1 - decay) ** years
    med golv på floor_frac * mult0.
    """
    m0 = _pos(mult0)
    if m0 is None:
        return None
    try:  # CHANGED: exponentiell decay i stället för linjär
        y = max(0, int(years))
        d = float(decay)
        factor = 1.0 - d
        if factor <= 0:
            m = m0 * floor_frac
        else:
            m = m0 * (factor ** y)
    except Exception:
        m = m0
    floor = m0 * float(floor_frac)
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

    # Välj första tillgängliga tillväxtindikator
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

def _ebitda_path(ebitda_ttm: Optional[float], rev0: Optional[float], rev1: Optional[float],
                 rev2: Optional[float], rev3: Optional[float]
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
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

# -------------------------
# EPS-estimat från Yahoo
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
# AUTO-PROFIL: välj metodfamiljer som passar
# -------------------------
def _auto_method_profile(row: pd.Series, y_snap: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returnerar vilka metodfamiljer som ska användas för FV-medianen.
    Familjer: 'pe', 'ev_s', 'ev_e', 'pb'
    Beslut baseras på Sektor + måtttillgänglighet + tecken på tidigt skede.
    """
    sektor = str(_nz(row.get("Sektor"), "")).lower()
    ticker = str(_nz(row.get("Ticker"), "")).upper()

    # Datatillgänglighet
    eps_ttm    = _pos(_nz(y_snap.get("eps_ttm"), row.get("EPS TTM")))
    pe_ttm     = _pos(_nz(y_snap.get("pe_ttm"), row.get("PE TTM")))
    pe_fwd     = _pos(_nz(y_snap.get("pe_fwd"), row.get("PE FWD")))
    rev_ttm    = _pos(_nz(y_snap.get("rev_ttm"), row.get("Rev TTM")))
    ebitda_ttm = _pos(_nz(y_snap.get("ebitda_ttm"), row.get("EBITDA TTM")))
    ev_rev     = _pos(_nz(y_snap.get("ev_rev"), row.get("EV/Revenue")))
    ev_ebitda  = _pos(_nz(y_snap.get("ev_ebitda"), row.get("EV/EBITDA")))
    p_b        = _pos(_nz(y_snap.get("p_b"), row.get("P/B")))
    bvps       = _pos(_nz(y_snap.get("bvps"), row.get("BVPS")))

    # Heuristik: klassificera
    is_financial  = any(k in sektor for k in ("finans", "financial", "bank", "insurance", "forsakring", "försäkring"))
    is_reit       = any(k in sektor for k in ("reit", "fastighet", "real estate"))
    is_utility    = any(k in sektor for k in ("utility", "verk", "kraft", "forsorjn", "försörjn"))
    is_energy     = any(k in sektor for k in ("energy", "olja", "gas", "oil", "gas"))
    is_industrial = any(k in sektor for k in ("industr", "capital goods", "machinery", "transport", "marine", "shipping"))
    is_tech       = any(k in sektor for k in ("tech", "software", "internet", "semiconductor", "it"))
    is_health     = any(k in sektor for k in ("health", "biotech", "pharma", "medtech"))
    # Tickers som ofta är BDC/mREIT (proxy → P/B)
    bdc_mreit_tickers = {"AGNC","ARR","DX","EFC","NLY","ORC","RITM","CSWC","PFLT","HRZN","ARCC","MAIN"}

    # Grund-allow baserat på data
    allow = {
        "pe":   (eps_ttm is not None) and (pe_ttm is not None or pe_fwd is not None) and (eps_ttm > 0),
        "ev_s": (rev_ttm is not None) and (ev_rev is not None),
        "ev_e": (ebitda_ttm is not None) and (ebitda_ttm > 0) and (ev_ebitda is not None),
        "pb":   (p_b is not None) and (p_b > 0) and (bvps is not None) and (bvps > 0),
    }

    # Sektor-skift
    if is_financial or ticker in bdc_mreit_tickers:
        # Finans/BDC/mREIT → P/B primärt, PE sekundärt (om lönsam), undvik EV-mått
        allow["ev_s"] = False
        allow["ev_e"] = False
        # PE bara om positiv EPS
        allow["pe"] = allow["pe"] and (eps_ttm and eps_ttm > 0)
    elif is_reit:
        # REIT/fastigheter → P/B + EV/EBITDA om möjligt, undvik EV/S
        allow["ev_s"] = False
        # behåll pb & ev_e enligt data
    elif is_utility or is_energy or is_industrial:
        # Tillgångstunga/cykliska → EV/EBITDA + PE; EV/S ok men inte primär
        pass
    elif is_tech or is_health:
        # Tidigt skede/loss-making → EV/S prioriteras; PE om positiv EPS
        if not (eps_ttm and eps_ttm > 0):
            allow["pe"] = False
        # EV/EBITDA kräver positiv EBITDA — redan hanterat via data
    # Övriga sektorer → data-drivet som default

    # Fallback: om allt råkar bli avstängt, försök välja ett rimligt spår
    if not any(allow.values()):
        if (rev_ttm is not None) and (ev_rev is not None):
            allow["ev_s"] = True
        elif (eps_ttm is not None) and (eps_ttm > 0) and (pe_ttm is not None or pe_fwd is not None):
            allow["pe"] = True
        elif (p_b is not None) and (p_b > 0) and (bvps is not None) and (bvps > 0):
            allow["pb"] = True

    # Primär (för etikett/diagnostik)
    prefer_order = ["pe","ev_e","ev_s","pb"] if (is_utility or is_energy or is_industrial) else ["pe","ev_s","ev_e","pb"]
    if is_financial or is_reit or (ticker in bdc_mreit_tickers):
        prefer_order = ["pb","pe","ev_e","ev_s"]
    primary = next((fam for fam in prefer_order if allow.get(fam)), None)

    # Bygg en ren ASCII-diagnostiksträng
    allow_bits = ", ".join([f"{k}:{'yes' if v else 'no'}" for k, v in allow.items()])
    sektor_label = (sektor or "-")
    primary_label = (primary or "-")
    why = f"auto_profile: sektor='{sektor_label}', ticker='{ticker}', allow={{" + allow_bits + f"}}, primary='{primary_label}'"

    return {"allow": allow, "primary": primary, "why": why}

# -------------------------
# Fair Value via familjemedian (v3 med filtrering)
# -------------------------
def _compute_fair_value_row_v3(methods_df: pd.DataFrame, now_price: Optional[float], allow_fams: Dict[str, bool]) -> Dict[str, Any]:
    """
    Median över *tillåtna* metodfamiljer:
      • 'pe_hist_vs_eps'          → fam 'pe'
      • 'ev_sales'                → fam 'ev_s'
      • 'ev_ebitda','ev_dacf'     → fam 'ev_e' (en gång)
      • 'p_b'                     → fam 'pb'
    Regler:
      • Dubbletter inom familj ignoreras.
      • Endast familjer där allow_fams[fam] == True räknas.
      • 'Idag': filtrera bort värden ≈ aktuell kurs (±0,5 %).
        Fall-back till 'pe_hist_vs_eps' om allt filtreras bort och 'pe' är tillåten.
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
            fam = fam_map.get(m, m)
            if fam in used_fams:
                continue
            if not allow_fams.get(fam, False):
                continue
            v = _f(r.get(c))
            if v is None:
                continue
            # Filtrera kurs-kopior i "Idag"
            if c == "Idag" and _pos(now_price) and _pos(v):
                if abs(v - float(now_price)) / float(now_price) <= 0.005:
                    continue
            used_fams.add(fam)
            vals.append(float(v))

        if not vals:
            # Fall-back: PE-raden om den finns och 'pe' är tillåten
            try:
                if allow_fams.get("pe", False):
                    row_pe = methods_df[methods_df["Metod"] == "pe_hist_vs_eps"].iloc[0]
                    out[c] = _f(row_pe.get(c))
                else:
                    out[c] = np.nan
            except Exception:
                out[c] = np.nan
        else:
            out[c] = float(np.median(vals))
    return out

# -------------------------
# Bucket → Margin of Safety
# -------------------------
def _mos_for_bucket(bucket_label: Any) -> float:
    """
    Returnerar MoS (0.05/0.08/0.12) enligt:
      Bucket A → 5%, Bucket B → 8%, Bucket C → 12%.
    Okänt → 8% (mitten).
    """
    s = str(bucket_label or "").lower()
    if "bucket a" in s:
        return 0.05
    if "bucket b" in s:
        return 0.08
    if "bucket c" in s:
        return 0.12
    return 0.08

def _best_case_row(methods_df: pd.DataFrame, allow_fams: Dict[str,bool]) -> Dict[str, Any]:
    """
    'Bästa scenario' = max-pris över tillåtna familjer per horisont.
    """
    fam_ok = {"pe_hist_vs_eps":"pe", "ev_sales":"ev_s", "ev_ebitda":"ev_e", "ev_dacf":"ev_e", "p_b":"pb"}
    cols = ["Idag", "1 år", "2 år", "3 år"]
    base = {"Metod": "best_case"}
    if methods_df is None or (hasattr(methods_df, "empty") and methods_df.empty):
        return {**base, **{c: np.nan for c in cols}}
    sub = methods_df[methods_df["Metod"].map(lambda m: allow_fams.get(fam_ok.get(str(m), ""), False))].copy()
    for c in cols:
        try:
            vals = [float(v) for v in sub[c].tolist() if _f(v) is not None]
            base[c] = (max(vals) if vals else np.nan)
        except Exception:
            base[c] = np.nan
    return base

# -------------------------
# Huvud: compute_methods_for_row → DICT (auto-profil)
# -------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str] | None = None,
                            fx_map: Dict[str, float] | None = None) -> Dict[str, Any]:
    """
    Returnerar en DICT som funkar både för Ranking-sidan och analysvyer:
      {
        "Metod": "fair_value_v3_auto",
        "target_today": float|None,  # = Fair Value idag (ingen MoS)
        "target_1y":    float|None,  # = Best case 1y * (1 - MoS bucket)
        "target_2y":    float|None,  # = Best case 2y * (1 - MoS bucket)
        "target_3y":    float|None,  # = Best case 3y * (1 - MoS bucket)
        "bull_1y": None, "bear_1y": None,
        "method": "fair_value_v3_auto",
        "Input-sammanfattning": "...",
        "note": "",
        "currency": "USD",
        "price": 123.45,
        "shares_out": ...,
        "net_debt": ...,
        "pe_anchor": ...,
        "decay": ...,
        "methods_df": <DataFrame>
      }
    Alla target i aktiens handelsvaluta.
    """
    settings = settings or get_settings_map()

    ticker = str(row.get("Ticker", "")).strip()
    y = fetch_from_yahoo(ticker)
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

    # CHANGED: manuella revenue-estimat om de finns på raden
    rev_1y_manual = _pos(_nz(row.get("Rev 1Y"), None))
    rev_2y_manual = _pos(_nz(row.get("Rev 2Y"), None))
    rev_3y_manual = _pos(_nz(row.get("Rev 3Y"), None))

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
    decay = _f(settings.get("multiple_decay", 0.08)) or 0.08  # 8% kompression/år
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, w_ttm)

    # Revenue-path (TTM + ev. manuella estimat + CAGR)
    r0 = _pos(rev_ttm)
    if r0 is None:
        g = float(_nz(rev_cagr_hist, 0.0))
        r1 = rev_1y_manual
        r2 = rev_2y_manual if rev_2y_manual is not None else (r1 * (1.0 + g) if r1 is not None else None)
        r3 = rev_3y_manual if rev_3y_manual is not None else (r2 * (1.0 + g) if r2 is not None else None)
    else:
        g = float(_nz(rev_cagr_hist, 0.0))
        r1 = rev_1y_manual if rev_1y_manual is not None else (r0 * (1.0 + g))
        r2 = rev_2y_manual if rev_2y_manual is not None else ((r1 or r0) * (1.0 + g))
        r3 = rev_3y_manual if rev_3y_manual is not None else ((r2 or r1 or r0) * (1.0 + g))

    # EPS-path
    e0, e1, e2, e3 = _eps_path_fill(_f(eps_ttm), eps_1y_est, eps_2y_est,
                                    eps_cagr_hist, eps_cagr_long, rev_cagr_hist)

    # EBITDA-path (skala med intäkter)
    b0, b1, b2, b3 = _ebitda_path(_f(ebitda_ttm), r0, r1, r2, r3)

    # Multiplar med decay (exponentiell)
    pe0  = pe_anchor
    pe1m = _decay_multiple(pe_anchor, 1, decay)
    pe2m = _decay_multiple(pe_anchor, 2, decay)
    pe3m = _decay_multiple(pe_anchor, 3, decay)

    evs0, evs1, evs2, evs3 = ev_sales,  _decay_multiple(ev_sales,  1, decay), _decay_multiple(ev_sales,  2, decay), _decay_multiple(ev_sales,  3, decay)
    eve0, eve1, eve2, eve3 = ev_ebitda, _decay_multiple(ev_ebitda, 1, decay), _decay_multiple(ev_ebitda, 2, decay), _decay_multiple(ev_ebitda, 3, decay)
    pb0,  pb1,  pb2,  pb3  = p_b,       _decay_multiple(p_b,       1, decay), _decay_multiple(p_b,       2, decay), _decay_multiple(p_b,       3, decay)

    # --- AUTO-PROFIL: vilka familjer ska räknas in? ---
    profile = _auto_method_profile(row, y)
    allow_fams = profile["allow"]

    # ---- EV/S-pris via relativ väg mot dagens kurs (ingen enhetsrisk) ----
    def _evs_price(rel_rev, rel_mult) -> Optional[float]:
        if not _pos(price):
            return None
        if not (_pos(rel_rev) and _pos(rel_mult)):
            return None
        try:
            factor = float(rel_rev) * float(rel_mult)
            if not math.isfinite(factor):
                return None
            # CHANGED: clamp totalt EV/S-scenario per horisont
            factor = max(0.10, min(5.0, factor))  # 0.1x–5x av dagens kurs
            return float(price) * factor
        except Exception:
            return None

    # Relativa faktorer mot r0/evs0
    def _rel(r, base):
        if _pos(r) and _pos(base):
            try:
                return float(r) / float(base)
            except Exception:
                return None
        return None

    rev_rel_1 = _rel(r1, r0) if _pos(r0) else None
    rev_rel_2 = _rel(r2, r0) if _pos(r0) else None
    rev_rel_3 = _rel(r3, r0) if _pos(r0) else None
    mult_rel_0 = 1.0
    mult_rel_1 = _rel(evs1, evs0) if _pos(evs0) else None
    mult_rel_2 = _rel(evs2, evs0) if _pos(evs0) else None
    mult_rel_3 = _rel(evs3, evs0) if _pos(evs0) else None

    evs_price_0 = price if _pos(price) and _pos(evs0) and _pos(r0) else None
    evs_price_1 = _evs_price(rev_rel_1, mult_rel_1) if (rev_rel_1 is not None and mult_rel_1 is not None) else None
    evs_price_2 = _evs_price(rev_rel_2, mult_rel_2) if (rev_rel_2 is not None and mult_rel_2 is not None) else None
    evs_price_3 = _evs_price(rev_rel_3, mult_rel_3) if (rev_rel_3 is not None and mult_rel_3 is not None) else None

    # --- Priser per metod (alla i aktiens valuta) ---
    methods = []
    methods.append({
        "Metod": "pe_hist_vs_eps",
        "Idag": _price_from_pe(e0, pe0),
        "1 år": _price_from_pe(e1, pe1m),
        "2 år": _price_from_pe(e2, pe2m),
        "3 år": _price_from_pe(e3, pe3m),
    })
    methods.append({
        "Metod": "ev_sales",  # CHANGED: baserad på relativ prisfaktor mot dagens kurs
        "Idag": evs_price_0,
        "1 år": evs_price_1,
        "2 år": evs_price_2,
        "3 år": evs_price_3,
    })
    methods.append({
        "Metod": "ev_ebitda",
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

    # --- Fair Value (familjemedian, filtrerad av auto-profil) = IDAG ---
    fv_row = _compute_fair_value_row_v3(methods_df, price, allow_fams)
    # --- Bästa scenario (max per horisont över tillåtna familjer) ---
    best_row = _best_case_row(methods_df, allow_fams)

    # --- Margin of Safety per bucket för framtiden ---
    bucket_label = str(_nz(row.get("Bucket"), "") or "")
    mos = _mos_for_bucket(bucket_label)
    best_mos_row = {
        "Metod": "best_case_MoS",
        "Idag": _f(fv_row.get("Idag")),  # ingen MoS på dagens fair value
        "1 år": (_f(best_row.get("1 år")) * (1.0 - mos)) if _f(best_row.get("1 år")) is not None else np.nan,
        "2 år": (_f(best_row.get("2 år")) * (1.0 - mos)) if _f(best_row.get("2 år")) is not None else np.nan,
        "3 år": (_f(best_row.get("3 år")) * (1.0 - mos)) if _f(best_row.get("3 år")) is not None else np.nan,
    }

    # Sätt ihop metodtabellen i tydlig ordning
    methods_df = pd.concat(
        [pd.DataFrame([fv_row]), pd.DataFrame([best_row]), pd.DataFrame([best_mos_row]), methods_df],
        ignore_index=True
    )

    # --- Sanity-text (ASCII) ---
    sanity = (
        f"price={'ok' if price else '-'}, "
        f"eps_ttm={'ok' if (eps_ttm or eps_ttm==0) else '-'}, "
        f"eps_1y={'ok' if eps_1y_est else '-'}, "
        f"eps_2y={'ok' if eps_2y_est else '-'}, "
        f"rev_ttm={'ok' if rev_ttm else '-'}, "
        f"rev_cagr_hist={'ok' if _f(rev_cagr_hist) is not None else '-'}(clamp={REV_CAGR_MIN*100:.0f}%..{REV_CAGR_MAX*100:.0f}%), "
        f"eps_cagr_hist={'ok' if _f(eps_cagr_hist) is not None else '-'}(clamp={EPS_CAGR_MIN*100:.0f}%..{EPS_CAGR_MAX*100:.0f}%), "
        f"ebitda_ttm={'ok' if (ebitda_ttm or ebitda_ttm==0) else '-'}, "
        f"shares={'ok' if shares else '-'}, "
        f"pe_anchor={round(pe_anchor,2) if pe_anchor else '-'}, decay={decay}, "
        f"bucket='{bucket_label or '-'}' -> MoS={int(mos*100)}%, "
        f"{profile['why']}"
    )

    # --- Targets att skriva till Data-bladet ---
    target_today = _f(fv_row.get("Idag"))
    target_1y    = _f(best_mos_row.get("1 år")) if _f(best_mos_row.get("1 år")) is not None else _f(fv_row.get("1 år"))
    target_2y    = _f(best_mos_row.get("2 år")) if _f(best_mos_row.get("2 år")) is not None else _f(fv_row.get("2 år"))
    target_3y    = _f(best_mos_row.get("3 år")) if _f(best_mos_row.get("3 år")) is not None else _f(fv_row.get("3 år"))

    payload: Dict[str, Any] = {
        "Metod": "fair_value_v3_auto",
        "method": "fair_value_v3_auto",
        "target_today": target_today,  # Fair value idag (ingen MoS)
        "target_1y":    target_1y,     # Best case – MoS
        "target_2y":    target_2y,     # Best case – MoS
        "target_3y":    target_3y,     # Best case – MoS
        "bull_1y": None,
        "bear_1y": None,
        "Input-sammanfattning": sanity,
        "note": profile.get("primary") or "",
        "currency": currency,
        "price": price,
        "shares_out": shares,
        "net_debt": net_debt,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "methods_df": methods_df,
    }
    return payload

# -------------------------
# Kompakt extraktor (FV) för UI
# -------------------------
def compute_fair_values_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Dict[str, Any]:
    """
    Beräknar metoder för en rad och returnerar en kompakt dict för UI:
      {
        'ticker': 'AAPL',
        'price':  195.12,
        'currency': 'USD',
        'fv_today':  Fair Value idag (utan MoS),
        'fv_1y':     Best case 1y – MoS(bucket),
        'fv_2y':     Best case 2y – MoS(bucket),
        'fv_3y':     Best case 3y – MoS(bucket),
        'sanity': '...',
        'methods_df': <DataFrame>
      }
    """
    payload = compute_methods_for_row(row, settings, fx_map)
    return {
        "ticker": str(row.get("Ticker") or "").upper(),
        "price": _f(payload.get("price")),
        "currency": (payload.get("currency") or "USD"),
        "fv_today": _f(payload.get("target_today")),
        "fv_1y": _f(payload.get("target_1y")),
        "fv_2y": _f(payload.get("target_2y")),
        "fv_3y": _f(payload.get("target_3y")),
        "sanity": payload.get("Input-sammanfattning", ""),
        "methods_df": payload.get("methods_df"),
    }
# (Slut Del 3/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 4/6: Analys- & Ranking-UI (byggstenar)
#
#  - render_analysis_view(): enskild ticker → fair value + metodtabell
#  - render_ranking_view(): lista/sortering på uppsida per vald horisont
#  - Hjälpfunktioner för formattering & bulk-beräkning
#
# Not:
#  • Alla priser/targets i aktiens handelsvaluta (ingen FX på EPS/targets).
#  • compute_methods_for_row()/compute_fair_values_for_row() definieras i Del 3.
#  • Datakällor (Sheets/Valuta/Settings) finns i Del 1–2; main() lägger ihop.
# ============================================================

# -------------------------
# Formatteringshjälp
# -------------------------
def _fmt2(x: Any) -> str:
    v = _f(x)
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return ""
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)

def _pct_change(target: Optional[float], price: Optional[float]) -> Optional[float]:
    t, p = _pos(target), _pos(price)
    if t is None or p is None or p == 0:
        return None
    try:
        return (t - p) / p * 100.0
    except Exception:
        return None

def _pick_target(payload: Dict[str, Any], horizon: str) -> Optional[float]:
    h = str(horizon or "").lower()
    if h in ("idag", "today", "0", "now"):
        return _f(payload.get("target_today"))
    if h in ("1y", "1 år", "1 ar", "1"):
        return _f(payload.get("target_1y"))
    if h in ("2y", "2 år", "2 ar", "2"):
        return _f(payload.get("target_2y"))
    if h in ("3y", "3 år", "3 ar", "3"):
        return _f(payload.get("target_3y"))
    return _f(payload.get("target_today"))

# -------------------------
# Kompakt summeringsrad (för tabell/CSV)
# -------------------------
def _build_summary_row(row: pd.Series, payload: Dict[str, Any]) -> Dict[str, Any]:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H.%M.%S")
    return {
        "Timestamp": ts,
        "Ticker": str(row.get("Ticker") or "").upper(),
        "Valuta": (payload.get("currency") or "USD"),
        "Aktuell kurs (0)": _f(payload.get("price")),
        "Riktkurs idag": _f(payload.get("target_today")),
        "Riktkurs 1 år": _f(payload.get("target_1y")),
        "Riktkurs 2 år": _f(payload.get("target_2y")),
        "Riktkurs 3 år": _f(payload.get("target_3y")),
        "Bull 1 år": _f(payload.get("bull_1y")),
        "Bear 1 år": _f(payload.get("bear_1y")),
        "Metod": str(payload.get("method") or "fair_value_v3_auto"),
        "Input-sammanfattning": str(payload.get("Input-sammanfattning") or ""),
        "Kommentar": str(payload.get("note") or ""),
    }

# -------------------------
# Render: metodtabell (expander)
# -------------------------
def _render_methods_table(payload: Dict[str, Any]) -> None:
    dfm = payload.get("methods_df")
    if dfm is None or (hasattr(dfm, "empty") and dfm.empty):
        st.info("Inga metodvärden att visa.")
        return
    # Runda siffror snyggt
    dfv = dfm.copy()
    for c in ["Idag", "1 år", "2 år", "3 år"]:
        if c in dfv.columns:
            dfv[c] = dfv[c].map(lambda x: None if _f(x) is None else float(f"{float(x):.6f}"))
    st.dataframe(dfv, use_container_width=True, hide_index=True)

# -------------------------
# Analys-vy (enskild ticker)
# -------------------------
def render_analysis_view(df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    if df is None or df.empty or "Ticker" not in df.columns:
        st.warning("Ingen data att analysera.")
        return

    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    tickers = [t for t in tickers if t.strip() != ""]
    tickers.sort()

    c1, c2 = st.columns([1, 2], gap="large")
    with c1:
        sel = st.selectbox("Välj ticker", tickers, index=0 if tickers else None)
        row = df[df["Ticker"].astype(str).str.upper() == str(sel).upper()]
        if row.empty:
            st.error("Kunde inte hitta vald ticker i Data-bladet.")
            return
        row = row.iloc[0]

        payload = compute_methods_for_row(row, settings, fx_map)

        price   = _f(payload.get("price"))
        fv_t0   = _f(payload.get("target_today"))
        fv_1y   = _f(payload.get("target_1y"))
        fv_2y   = _f(payload.get("target_2y"))
        fv_3y   = _f(payload.get("target_3y"))
        curr    = payload.get("currency") or "USD"

        st.metric("Aktuell kurs", f"{_fmt2(price)} {curr}")
        st.metric("Fair Value (idag)", f"{_fmt2(fv_t0)} {curr}", delta=None)
        st.caption(payload.get("Input-sammanfattning") or "")

        up0 = _pct_change(fv_t0, price)
        up1 = _pct_change(fv_1y, price)
        up2 = _pct_change(fv_2y, price)
        up3 = _pct_change(fv_3y, price)

        st.write("**Uppsida mot aktuell kurs**")
        st.write(
            f"Idag: {(_fmt2(up0) + ' %') if up0 is not None else '-'}  •  "
            f"1 år: {(_fmt2(up1) + ' %') if up1 is not None else '-'}  •  "
            f"2 år: {(_fmt2(up2) + ' %') if up2 is not None else '-'}  •  "
            f"3 år: {(_fmt2(up3) + ' %') if up3 is not None else '-'}"
        )

    with c2:
        with st.expander("Metoder & detaljer", expanded=True):
            _render_methods_table(payload)
            st.code(payload.get("Input-sammanfattning") or "", language="text")

    # Summeringsrad (enligt användarens tabellstruktur)
    summary = _build_summary_row(row, payload)
    st.subheader("Sammanfattning (denna ticker)")
    df_sum = pd.DataFrame([summary])
    st.dataframe(df_sum, use_container_width=True, hide_index=True)

    # Alltid visa hela databasen längst ner (enkel tabell, ofiltrerad)
    st.markdown("---")
    st.caption("Hela databasen (enkel tabell):")
    st.dataframe(df, use_container_width=True)

# -------------------------
# Bulk: beräkna FV för flera rader (ranking)
# -------------------------
def _bulk_compute(df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> pd.DataFrame:
    rows = []
    tickers = df["Ticker"].dropna().astype(str).tolist() if "Ticker" in df.columns else []
    tickers = [t for t in tickers if t.strip() != ""]
    prog = st.progress(0.0, text="Beräknar fair value…")
    n = len(tickers)
    for i, t in enumerate(tickers, start=1):
        try:
            row = df[df["Ticker"].astype(str).str.upper() == t.upper()].iloc[0]
            payload = compute_methods_for_row(row, settings, fx_map)
            rows.append((row, payload))
        except Exception as e:
            st.warning(f"{t}: beräkningsfel – {e}")
        prog.progress(i / max(1, n), text=f"Beräknar fair value… ({i}/{n})")
    prog.empty()

    out = []
    for row, payload in rows:
        d = _build_summary_row(row, payload)
        # Lägg uppsidor för snabb sortering i UI
        price = _f(payload.get("price"))
        d["Uppsida % (idag)"] = _pct_change(d["Riktkurs idag"], price)
        d["Uppsida % (1 år)"] = _pct_change(d["Riktkurs 1 år"], price)
        d["Uppsida % (2 år)"] = _pct_change(d["Riktkurs 2 år"], price)
        d["Uppsida % (3 år)"] = _pct_change(d["Riktkurs 3 år"], price)
        out.append(d)

    cols = [
        "Timestamp","Ticker","Valuta","Aktuell kurs (0)",
        "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
        "Bull 1 år","Bear 1 år","Metod","Input-sammanfattning","Kommentar",
        "Uppsida % (idag)","Uppsida % (1 år)","Uppsida % (2 år)","Uppsida % (3 år)"
    ]
    df_out = pd.DataFrame(out)
    # Säkerställ kolumnordning där det går
    ordered = [c for c in cols if c in df_out.columns] + [c for c in df_out.columns if c not in cols]
    df_out = df_out.reindex(columns=ordered)
    return df_out

# -------------------------
# Ranking-vy
# -------------------------
def render_ranking_view(df: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]) -> None:
    if df is None or df.empty or "Ticker" not in df.columns:
        st.warning("Ingen data att ranka.")
        return

    st.subheader("Ranking – uppsida mot aktuell kurs")
    colA, colB = st.columns([1, 2], gap="medium")
    with colA:
        horizon = st.selectbox("Horisont", ["Idag", "1 år", "2 år", "3 år"], index=1)
        show_n = st.slider("Visa topp N", min_value=5, max_value=200, value=50, step=5)
    with colB:
        st.caption("Värden i aktiens valuta. Framtida riktkurser använder Bucket-MoS enligt Del 3.")

    df_rank = _bulk_compute(df, settings, fx_map)
    # Välj uppsida-kolumn
    hmap = {"Idag":"Uppsida % (idag)", "1 år":"Uppsida % (1 år)", "2 år":"Uppsida % (2 år)", "3 år":"Uppsida % (3 år)"}
    up_col = hmap.get(horizon, "Uppsida % (1 år)")
    if up_col not in df_rank.columns:
        st.error("Kunde inte beräkna uppsida.")
        return

    df_show = df_rank.sort_values(by=up_col, ascending=False).head(show_n).copy()

    # Visa kärnkolumner först
    show_cols = [
        "Ticker","Valuta","Aktuell kurs (0)","Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år", up_col
    ]
    show_cols = [c for c in show_cols if c in df_show.columns]
    st.dataframe(df_show[show_cols], use_container_width=True, hide_index=True)

    with st.expander("Visa alla kolumner (rankingresultat)", expanded=False):
        st.dataframe(df_show, use_container_width=True, hide_index=True)

    # Alltid visa hela databasen längst ner
    st.markdown("---")
    st.caption("Hela databasen (enkel tabell):")
    st.dataframe(df, use_container_width=True)

# (Slut Del 4/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 5/6: Settings, Snapshot, Editor, Lägg till, Portfölj,
#          Massuppdatering & Köpförslag (UI)
#
#  - Lagrar/läser allt via schema från Del 1
#  - Använder Yahoo-hämtning (Del 3) och beräkningsmotor (Del 3)
#  - Visar hela databasen som enkel tabell i relevanta vyer
# ============================================================

# -------------------------
# Små UI-hjälpare (sök + nav)
# -------------------------
if "_names_map_from_df" not in globals():
    def _names_map_from_df(df: pd.DataFrame) -> Dict[str, str]:
        out = {}
        if df is None or df.empty:
            return out
        for _, r in df.iterrows():
            t = str(r.get("Ticker") or "").upper().strip()
            n = str(r.get("Bolagsnamn") or "").strip()
            if t:
                out[t] = f"{t} — {n}" if n else t
        return out

if "_select_with_search_nav" not in globals():
    def _select_with_search_nav(label: str, options: List[str], names_map: Dict[str, str],
                                session_idx_key: str, query_key: str) -> Optional[str]:
        if not options:
            st.info("Inga alternativ.")
            return None
        options = sorted(list({o.upper().strip() for o in options if o}))
        if session_idx_key not in st.session_state:
            st.session_state[session_idx_key] = 0
        st.session_state[session_idx_key] = max(0, min(st.session_state[session_idx_key], len(options)-1))

        q = st.text_input("Sök (ticker/namn)", key=query_key)
        if q:
            ql = q.lower().strip()
            shown = [o for o in options if (ql in o.lower()) or (ql in names_map.get(o, o).lower())]
            if not shown:
                shown = options
        else:
            shown = options

        pretty = [names_map.get(o, o) for o in shown]
        idx = min(st.session_state[session_idx_key], len(shown)-1)
        picked_pretty = st.selectbox(label, pretty, index=idx)
        picked = shown[pretty.index(picked_pretty)] if picked_pretty in pretty else shown[idx]

        c1, c2, c3 = st.columns([1, 1, 6])
        with c1:
            if st.button("◀︎", use_container_width=True, disabled=len(shown) <= 1):
                st.session_state[session_idx_key] = (shown.index(picked) - 1) % len(shown)
        with c2:
            if st.button("▶︎", use_container_width=True, disabled=len(shown) <= 1):
                st.session_state[session_idx_key] = (shown.index(picked) + 1) % len(shown)
        with c3:
            st.caption(f"{shown.index(picked)+1}/{len(shown)}")
        return picked

# Om Del 4 inte definierade en generell tabellvisare
if "_show_df" not in globals():
    def _show_df(df: pd.DataFrame, height: int = 360, use_container_width: bool = True) -> None:
        try:
            st.dataframe(df, use_container_width=use_container_width, height=height)
        except Exception:
            st.table(df.head(200))


# ============================================================
# ⚙️ Settings (redigerbar)
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
            _write_df(SETTINGS_TITLE, edited[SETTINGS_COLUMNS])
            st.cache_data.clear()
            st.session_state["SETTINGS_MAP"] = get_settings_map()
            st.success("Settings sparade.")
        except Exception as e:
            st.error(f"Kunde inte spara: {e}")


# ============================================================
# 🕒 Snapshot (read-only)
# ============================================================
def page_snapshot():
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    _show_df(snap, height=420, use_container_width=True)


# ============================================================
# ✏️ Editor (manuellt + Yahoo-prefill)
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

def _build_updates_from_yahoo(tkr: str, existing_row: pd.Series) -> Dict[str, Any]:
    y   = fetch_from_yahoo(tkr)
    try:
        est = _fetch_eps_estimates_yahoo(tkr)
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
    _show_df(df.loc[[idx]], height=240, use_container_width=True)


# ============================================================
# ➕ Lägg till ticker (med valfri Yahoo-prefill)
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
# 📦 Portfölj (innehav + kommande utdelningar)
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
    _show_df(df_show, height=300, use_container_width=True)

    with st.expander("Visa summering per månad (SEK, netto)"):
        try:
            g = nxt.copy()
            g["YYYY-MM"] = g["Datum"].astype(str).str.slice(0, 7)
            agg = g.groupby("YYYY-MM", as_index=False)["Netto SEK"].sum().sort_values("YYYY-MM")
            agg["Netto SEK"] = agg["Netto SEK"].map(lambda x: f"{x:,.2f}".replace(",", " ").replace(".", ","))
            _show_df(agg, height=240, use_container_width=True)
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
            _show_df(show, height=260, use_container_width=True)

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
        _show_df(pos.sort_values(["Bucket","Värde (SEK)"]), height=320, use_container_width=True)
        st.markdown("#### Hinkar (Bucket) – innehåll")
        render_bucket_expandables(pos, settings)

    st.markdown("---")
    render_portfolio_dividends_section(df, fx_map, settings)


# ============================================================
# 🧩 Massuppdatering (Yahoo) — 1s per bolag
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
    st.session_state["DATA"] = df_cur
    progress.empty()
    status.empty()
    st.success(f"Klar. {len(target)} bolag uppdaterade. {changed_total} fält ändrades.")


# ============================================================
# 🛒 Köpförslag + Säljförslag
#   - Köpförslag: pris < FV för **vald horisont** OCH värde < bucket-cap
#   - Filtrering: horisont (för uppsida/sortering), Bucket, innehavsfilter
#   - Säljförslag: värde > bucket-cap, filtrerbart på Bucket
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
                          own_filter: str = "Alla", fv_horizon: str = "Idag",
                          bucket_filter: str = "Alla") -> pd.DataFrame:
    """
    Köpförslag:
      • Aktuell kurs < Riktkurs för **vald horisont** (Idag / 1 år / 2 år / 3 år)
      • Innehavets värde (SEK) < cap för dess Bucket
      • Filtrering på Bucket + innehav (Alla / Endast innehav / Endast ej ägda)
      • fv_horizon styr både:
          – vilken riktkurs som används för 'Uppsida (%)' och sortering
          – köpsignalen (kurs < vald FV)
    Övriga riktkurser används endast som visningsfält.
    """
    cols_out = [
        "Ticker","Bolagsnamn","Bucket","Valuta",
        "Kurs","FV idag","FV 1 år","FV 2 år","FV 3 år",
        "Uppsida (%)",
        "Äger (antal)","Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"
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

            # Bucket-filter (köpförslag)
            if bucket_filter and bucket_filter != "Alla" and bucket != bucket_filter:
                continue

            cap = _cap_for_bucket(bucket, settings)
            if cap is None or cap <= 0:
                continue

            price = _f(r.get("Aktuell kurs"))
            if not _pos(price):
                continue

            ccy  = (str(_nz(r.get("Valuta"), "SEK"))).upper()
            name = str(_nz(r.get("Bolagsnamn"), ""))

            fv_today = _f(r.get("Riktkurs idag"))
            fv_1y    = _f(r.get("Riktkurs 1 år"))
            fv_2y    = _f(r.get("Riktkurs 2 år"))
            fv_3y    = _f(r.get("Riktkurs 3 år"))

            # Aktiv FV för uppsida/sortering + köpsignal
            fv_map = {
                "Idag": fv_today,
                "1 år": fv_1y,
                "2 år": fv_2y,
                "3 år": fv_3y,
            }
            fv_active = fv_map.get(fv_horizon, fv_today)

            # Måste ha en användbar FV för vald horisont
            if not _pos(fv_active):
                continue

            # Köpsignal: pris < FV (vald horisont)
            if not (price < fv_active):
                continue

            entry = lu.get(
                tkr,
                {
                    "value_sek": 0.0,
                    "qty": _f(r.get("Antal aktier")) or 0.0,
                    "currency": ccy,
                    "price": price,
                },
            )
            qty = entry["qty"] if entry["qty"] is not None else (_f(r.get("Antal aktier")) or 0.0)

            own_status = "own" if (qty and qty > 0) else "no_own"
            if own_filter == "Endast innehav" and own_status != "own":
                continue
            if own_filter == "Endast ej ägda" and own_status != "no_own":
                continue

            fx = _fx_rate_to_sek(ccy, fx_map)
            value_sek = float((price or 0.0) * (qty or 0.0) * fx)

            # Endast innehav som inte är större än maxvärdet för respektive Bucket
            if _pos(value_sek) and value_sek >= cap:
                continue

            up_pct = None
            if _pos(price) and _pos(fv_active):
                up_pct = (fv_active - price) / price * 100.0

            rows.append({
                "Ticker": tkr,
                "Bolagsnamn": name,
                "Bucket": bucket,
                "Valuta": ccy,
                "Kurs": price,
                "FV idag": fv_today,
                "FV 1 år": fv_1y,
                "FV 2 år": fv_2y,
                "FV 3 år": fv_3y,
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
    # Sortera: först störst slack till cap (minst innehav) och därefter störst uppsida
    out = out.sort_values(
        ["Slack till cap (SEK)", "Uppsida (%)"],
        ascending=[False, False]
    ).reset_index(drop=True)
    return out

def build_sell_suggestions(df_data: pd.DataFrame, settings: Dict[str, str],
                           fx_map: Dict[str, float], bucket_filter: str = "Alla") -> pd.DataFrame:
    """
    Säljförslag:
      • Endast innehav (Antal > 0)
      • Värde (SEK) > cap för respektive Bucket
      • Filtrering på Bucket
    """
    cols_out = [
        "Ticker","Bolagsnamn","Bucket","Valuta",
        "Antal","Aktuell kurs",
        "Värde (SEK)","Cap per innehav (SEK)","Över cap (SEK)"
    ]
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols_out)

    fx_map = fx_map or get_fx_map()
    pos = _position_value_tables(df_data, fx_map)
    if pos.empty:
        return pd.DataFrame(columns=cols_out)

    rows = []
    for _, r in pos.iterrows():
        try:
            bucket = str(_nz(r.get("Bucket"), "") or "")
            if not bucket:
                continue
            if bucket_filter and bucket_filter != "Alla" and bucket != bucket_filter:
                continue

            cap = _cap_for_bucket(bucket, settings)
            if cap is None or cap <= 0:
                continue

            value_sek = _f(r.get("Värde (SEK)")) or 0.0
            if value_sek <= cap:
                continue  # endast de som är större än maxvärdet

            over_cap = value_sek - cap
            rows.append({
                "Ticker": str(r.get("Ticker") or ""),
                "Bolagsnamn": str(_nz(r.get("Bolagsnamn"), "")),
                "Bucket": bucket,
                "Valuta": str(_nz(r.get("Valuta"), "SEK")).upper(),
                "Antal": _f(r.get("Antal")) or 0.0,
                "Aktuell kurs": _f(r.get("Aktuell kurs")),
                "Värde (SEK)": value_sek,
                "Cap per innehav (SEK)": cap,
                "Över cap (SEK)": over_cap,
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=cols_out)

    out = pd.DataFrame(rows, columns=cols_out)
    out = out.sort_values("Över cap (SEK)", ascending=False).reset_index(drop=True)
    return out

def page_buy_suggestions():
    st.header("🛒 Köpförslag & säljförslag (läser Data-bladet)")
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
    if df is None or df.empty:
        st.info("Ingen data.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    # Bucket-lista för filter
    all_buckets = sorted({str(b) for b in df.get("Bucket", pd.Series([], dtype=object)).dropna().tolist() if str(b).strip()})
    bucket_opts = ["Alla"] + [b for b in DEFAULT_BUCKETS if b in all_buckets] + [b for b in all_buckets if b not in DEFAULT_BUCKETS]

    col_top1, col_top2, col_top3 = st.columns([2,2,2])
    with col_top1:
        fv_horizon = st.selectbox("Riktkurs-horisont (för uppsida/sortering)", ["Idag","1 år","2 år","3 år"], index=0)
    with col_top2:
        own_filter = st.radio("Innehavsfilter", ["Alla","Endast innehav","Endast ej ägda"], index=0, horizontal=True)
    with col_top3:
        bucket_filter_buy = st.selectbox("Bucket-filter (köpförslag)", bucket_opts, index=0)

    st.caption(
        f"Köpförslag visar bolag där aktuell kurs är lägre än riktkurs för **vald horisont** "
        f"(**{fv_horizon}**) och där innehavet inte är större än maxvärdet (cap) för respektive Bucket."
    )

    with st.spinner("Bygger köpförslag…"):
        sug = build_buy_suggestions(
            df, settings, fx_map,
            own_filter=own_filter,
            fv_horizon=fv_horizon,
            bucket_filter=bucket_filter_buy,
        )

    if sug.empty:
        st.info("Inga köpkandidater uppfyller kriterierna just nu.")
        st.caption("Tips: kör **🏆 Ranking** först för att uppdatera riktkurserna i Data-bladet.")
    else:
        st.caption(f"{len(sug)} köpförslag — sorterat på störst slack till cap och därefter uppsida mot vald riktkurs ({fv_horizon}).")
        show = sug.copy()

        # Formatera siffror snyggt
        if "Kurs" in show.columns:
            show["Kurs"] = show["Kurs"].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
        for c in ("FV idag","FV 1 år","FV 2 år","FV 3 år"):
            if c in show.columns:
                show[c] = show[c].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
        for c in ("Värde (SEK)","Cap per innehav (SEK)","Slack till cap (SEK)"):
            if c in show.columns:
                show[c] = show[c].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
        if "Uppsida (%)" in show.columns:
            show["Uppsida (%)"] = show["Uppsida (%)"].map(lambda v: f"{v:.1f}%" if v is not None else "—")

        _show_df(show, height=420, use_container_width=True)

        with st.expander("Summering per Bucket (antal köpförslag)"):
            agg = sug.groupby("Bucket", as_index=False).size().rename(columns={"size":"Antal förslag"})
            _show_df(agg, height=240, use_container_width=True)

    st.markdown("---")
    st.subheader("💼 Säljförslag (över Bucket-max)")

    bucket_filter_sell = st.selectbox("Bucket-filter (säljförslag)", bucket_opts, index=0, key="sell_bucket_filter")

    with st.spinner("Bygger säljförslag…"):
        sell_df = build_sell_suggestions(df, settings, fx_map, bucket_filter=bucket_filter_sell)

    if sell_df.empty:
        st.info("Inga innehav ligger över maxvärdet (cap) för vald Bucket just nu.")
    else:
        st.caption(f"{len(sell_df)} säljförslag — innehav där värdet överstiger Bucket-cap.")
        show_s = sell_df.copy()
        for c in ("Aktuell kurs","Värde (SEK)","Cap per innehav (SEK)","Över cap (SEK)"):
            if c in show_s.columns:
                show_s[c] = show_s[c].map(lambda v: "" if _f(v) is None else f"{float(v):.2f}")
        _show_df(show_s, height=360, use_container_width=True)

# (Slut Del 5/6)

# ============================================================
# app.py — Aktieanalys & investeringsförslag
# Del 6/6: Navigering & main()
#
#  - Sidebar-menyer
#  - Kopplar ihop alla vyer:
#       • Analys (enskild ticker)
#       • Ranking (lista/sortering)
#       • Köpförslag & Säljförslag
#       • Editor
#       • Lägg till ticker
#       • Portfölj (inkl. kommande utdelningar)
#       • Massuppdatering (Yahoo)
#       • Settings
#       • Snapshot
# ============================================================

# -------------------------
# Page-wrappers för Analys & Ranking
# -------------------------
def page_analysis():
    st.header("📊 Analys – enskild ticker")
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df
    if df is None or df.empty:
        st.warning("Ingen data att analysera. Fyll på Data-bladet först.")
        return

    settings = get_settings_map()
    fx_map   = st.session_state.get("FX") or get_fx_map()
    st.session_state["FX"] = fx_map

    render_analysis_view(df, settings, fx_map)


def page_ranking():
    st.header("🏆 Ranking – uppsida per ticker")
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
        st.session_state["DATA"] = df
    if df is None or df.empty:
        st.warning("Ingen data att ranka. Fyll på Data-bladet först.")
        return

    settings = get_settings_map()
    fx_map   = st.session_state.get("FX") or get_fx_map()
    st.session_state["FX"] = fx_map

    render_ranking_view(df, settings, fx_map)


# -------------------------
# MAIN
# -------------------------
def main():
    st.title("📈 Aktieanalys & investeringsförslag")

    # Se till att vi har Data + FX i session
    if "DATA" not in st.session_state:
        st.session_state["DATA"] = read_data_df()
    if "FX" not in st.session_state:
        st.session_state["FX"] = get_fx_map()

    # Sidebar-navigering
    st.sidebar.markdown("## 🧭 Navigering")

    page = st.sidebar.radio(
        "Välj vy",
        [
            "📊 Analys",
            "🏆 Ranking",
            "🛒 Köpförslag & säljförslag",
            "✏️ Editor",
            "➕ Lägg till ticker",
            "📦 Portfölj",
            "🧩 Massuppdatering",
            "⚙️ Settings",
            "🕒 Snapshot",
        ],
        index=0,
    )

    # Liten info om datakälla
    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Data hämtas från Google Sheets + Yahoo Finance.\n"
        "Riktkurser beräknas i handelsvalutan (ingen FX på EPS/targets)."
    )

    # Routing
    if page == "📊 Analys":
        page_analysis()
    elif page == "🏆 Ranking":
        page_ranking()
    elif page == "🛒 Köpförslag & säljförslag":
        page_buy_suggestions()
    elif page == "✏️ Editor":
        page_editor()
    elif page == "➕ Lägg till ticker":
        page_add_ticker()
    elif page == "📦 Portfölj":
        page_portfolio()
    elif page == "🧩 Massuppdatering":
        page_batch()
    elif page == "⚙️ Settings":
        page_settings()
    elif page == "🕒 Snapshot":
        page_snapshot()
    else:
        page_analysis()
if __name__ == "__main__":
    main()
# (Slut Del 6/6 – hela app.py klar)
