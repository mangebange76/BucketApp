# app.py — Del 1/4
# ============================================================
# Bas: Streamlit-app för fair value / riktkurser / portfölj
# Hämtning: Yahoo (yfinance) + valfri Finnhub (om API-nyckel finns)
# Lagring: Google Sheets (Data, Resultat, Valutakurser, Settings)
# Denna del innehåller: imports, config, Sheets-utils, FX, kolumnsäkring
# ============================================================

from __future__ import annotations
import os
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# --- Externa beroenden för data/Sheets ---
import yfinance as yf
import gspread
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

# =========================
# Grundinställningar
# =========================
st.set_page_config(page_title="Aktieanalys & riktkurser", layout="wide")
st.markdown(
    "<style>section.main > div {max-width: 1400px;}</style>", unsafe_allow_html=True
)

APP_TITLE = "Aktieanalys och investeringsförslag"
DATA_TITLE = "Data"
FX_TITLE = "Valutakurser"
SETTINGS_TITLE = "Settings"
RESULT_TITLE = "Resultat"

# =========================
# Helpers / Guard
# =========================
def guard(fn, label: str = ""):
    """Kör fn() och fångar fel med tydligt meddelande i UI."""
    try:
        return fn()
    except Exception as e:
        st.error(f"💥 Fel {label}\n\n{e}")
        raise

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

# =========================
# Google Sheets Auth
# =========================
@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    """Skapar gspread Client från st.secrets['GOOGLE_CREDENTIALS'] eller env."""
    raw = _env_or_secret("GOOGLE_CREDENTIALS")
    if not raw:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i secrets eller env.")
    if isinstance(raw, dict):
        creds_dict = dict(raw)
    else:
        # kan vara JSON-sträng
        creds_dict = json.loads(raw)
    creds_dict = _normalize_private_key(creds_dict)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client) -> Spreadsheet:
    """Öppnar spreadsheet via SHEET_URL eller SHEET_ID (prefixed underscore för cache)."""
    sheet_url = _env_or_secret("SHEET_URL")
    sheet_id = _env_or_secret("SHEET_ID")
    if sheet_url:
        return _gc.open_by_url(sheet_url)
    if sheet_id:
        return _gc.open_by_key(sheet_id)
    raise RuntimeError("Ange SHEET_URL eller SHEET_ID i secrets.")

# =========================
# Worksheet access + robust in/ut
# =========================
def _get_ws(spread: Spreadsheet, title: str) -> Worksheet:
    try:
        return spread.worksheet(title)
    except WorksheetNotFound:
        ws = spread.add_worksheet(title=title, rows=2000, cols=200)
        return ws

# Cachea läsning på worksheet-ID (inte objektet)
@st.cache_data(ttl=120, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
    """Läser ett helt ark till DataFrame. Tomt ark -> tom DF med 0 rader."""
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    # get_all_records är långsam + typning. Kombinera med row_values för header.
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    # Normalisera tomma -> NaN
    df = df.replace("", np.nan)
    return df

def _write_df(title: str, df: pd.DataFrame):
    """Skriver en DF till arket (ersätter innehåll)."""
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    # Säkerställ strängheader
    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    # Konvertera NaN -> ""
    df_out = df.fillna("")
    # Skriv (clear + update)
    ws.clear()
    if df_out.shape[0] == 0:
        ws.update([list(df_out.columns)])
    else:
        ws.update([list(df_out.columns)] + df_out.astype(str).values.tolist())

def _append_rows(title: str, rows: List[List[Any]]):
    """Append rader (lista av listor) till slutet av arket."""
    gc = _build_gspread_client()
    sh = _open_spreadsheet(gc)
    ws = _get_ws(sh, title)
    ws.append_rows(rows, value_input_option="RAW")

# =========================
# Säkerställ kolumner i Data/Settings/FX
# =========================
DATA_COLUMNS = [
    "Timestamp", "Ticker", "Bolagsnamn", "Sektor", "Valuta",
    "Antal aktier", "GAV (SEK)", "Aktuell kurs",
    "Utestående aktier", "Net debt",
    "Rev TTM", "EBITDA TTM", "EPS TTM",
    "PE TTM", "PE FWD", "EV/Revenue", "EV/EBITDA", "P/B",
    "EPS 1Y", "Rev CAGR", "EPS CAGR",
    # outputfält (kan fyllas av appen)
    "Riktkurs idag", "Riktkurs 1 år", "Riktkurs 2 år", "Riktkurs 3 år",
    "Primär metod", "Senast auto uppdaterad"
]

SETTINGS_COLUMNS = [
    "Key", "Value"
]

FX_COLUMNS = [
    "Valuta", "SEK_per_1"
]

def _ensure_sheet_schema():
    """Ser till att arken existerar och har rimlig header."""
    # Data
    df = _read_df(DATA_TITLE)
    if df.empty:
        _write_df(DATA_TITLE, pd.DataFrame(columns=DATA_COLUMNS))
    else:
        # Lägg till saknade kolumner
        missing = [c for c in DATA_COLUMNS if c not in df.columns]
        if missing:
            for c in missing:
                df[c] = np.nan
            _write_df(DATA_TITLE, df[DATA_COLUMNS])

    # Settings
    s = _read_df(SETTINGS_TITLE)
    if s.empty:
        base = pd.DataFrame(
            [
                ["withholding_USD", "0.15"],
                ["withholding_NOK", "0.25"],
                ["withholding_CAD", "0.15"],
                ["withholding_EUR", "0.15"],
                ["withholding_SEK", "0.00"],
                ["primary_currency", "SEK"],
                ["multiple_decay", "0.10"],
                ["pe_anchor_weight_ttm", "0.50"],
            ],
            columns=SETTINGS_COLUMNS,
        )
        _write_df(SETTINGS_TITLE, base)
    else:
        for c in SETTINGS_COLUMNS:
            if c not in s.columns:
                s[c] = np.nan
        _write_df(SETTINGS_TITLE, s[SETTINGS_COLUMNS])

    # Valutakurser
    fx = _read_df(FX_TITLE)
    if fx.empty:
        base_fx = pd.DataFrame(
            [
                ["SEK", 1.0],
                ["USD", np.nan],
                ["NOK", np.nan],
                ["CAD", np.nan],
                ["EUR", np.nan],
            ],
            columns=FX_COLUMNS,
        )
        _write_df(FX_TITLE, base_fx)
    else:
        for c in FX_COLUMNS:
            if c not in fx.columns:
                fx[c] = np.nan
        _write_df(FX_TITLE, fx[FX_COLUMNS])

# Kör schema bootstrap vid import
guard(_ensure_sheet_schema, label="(säkra ark/kolumner)")

# =========================
# Hämta & cachea valutakurser (yfinance)
# =========================
FX_PAIRS = {
    "USD": "USDSEK=X",
    "EUR": "EURSEK=X",
    "NOK": "NOKSEK=X",
    "CAD": "CADSEK=X",
    "SEK": None,  # bas
}

@st.cache_data(ttl=60 * 60, show_spinner=False)
def _fetch_fx_from_yahoo() -> Dict[str, float]:
    out = {"SEK": 1.0}
    for code, pair in FX_PAIRS.items():
        if pair is None:
            continue
        try:
            t = yf.Ticker(pair)
            px = t.fast_info.last_price
            if not px:  # fallback history
                hist = t.history(period="1d")
                if not hist.empty:
                    px = float(hist["Close"].iloc[-1])
            if px:
                out[code] = float(px)
        except Exception:
            pass
    return out

def _load_fx_and_update_sheet() -> Dict[str, float]:
    # Läs nuvarande
    fx_df = _read_df(FX_TITLE)
    cur_map = {}
    if not fx_df.empty:
        for _, r in fx_df.iterrows():
            try:
                cur_map[str(r["Valuta"]).upper()] = float(r["SEK_per_1"])
            except Exception:
                pass
    # Hämta nya
    fresh = _fetch_fx_from_yahoo()
    # Mixa in
    cur_map.update({k: v for k, v in fresh.items() if v})
    # Skriv tillbaka
    rows = [(k, cur_map.get(k, "")) for k in ["SEK", "USD", "EUR", "NOK", "CAD"]]
    _write_df(FX_TITLE, pd.DataFrame(rows, columns=FX_COLUMNS))
    return cur_map

@st.cache_data(ttl=30 * 60, show_spinner=False)
def get_fx_map() -> Dict[str, float]:
    """Publik funktion som alltid returnerar en komplett map med SEK=1."""
    mp = _load_fx_and_update_sheet()
    if "SEK" not in mp:
        mp["SEK"] = 1.0
    # se till att alla nycklar finns
    for c in ["USD", "EUR", "NOK", "CAD"]:
        mp.setdefault(c, np.nan)
    return mp

def to_sek(amount: float, cur: str, fx_map: Dict[str, float]) -> Optional[float]:
    if amount is None:
        return None
    if cur is None:
        return None
    code = str(cur).upper()
    rate = fx_map.get(code)
    if not rate or rate <= 0:
        return None
    return float(amount) * float(rate)

# =========================
# Läs Settings som dict
# =========================
@st.cache_data(ttl=600, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    s = _read_df(SETTINGS_TITLE)
    out = {}
    if s.empty:
        return out
    for _, r in s.iterrows():
        k = str(r.get("Key"))
        v = "" if pd.isna(r.get("Value")) else str(r.get("Value"))
        out[k] = v
    return out

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
    code = (currency or "USD").upper()
    key = f"withholding_{code}"
    try:
        return float(settings.get(key, "0.15"))
    except Exception:
        return 0.15

# =========================
# Publika I/O-primitiver
# =========================
def read_data_df() -> pd.DataFrame:
    df = _read_df(DATA_TITLE)
    if df.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)
    # normalisera
    for c in ["Antal aktier", "GAV (SEK)", "Aktuell kurs",
              "Utestående aktier", "Net debt",
              "Rev TTM", "EBITDA TTM", "EPS TTM", "PE TTM", "PE FWD",
              "EV/Revenue", "EV/EBITDA", "P/B", "EPS 1Y",
              "Rev CAGR", "EPS CAGR"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def write_data_df(df: pd.DataFrame):
    # skriv i korrekt ordning
    cols = [c for c in DATA_COLUMNS if c in df.columns] + [c for c in df.columns if c not in DATA_COLUMNS]
    _write_df(DATA_TITLE, df[cols])

def append_result_row(row: Dict[str, Any]):
    # skriv en rad till Resultat
    # säkerställ rubriker
    res = _read_df(RESULT_TITLE)
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame([row]))
    else:
        cols = list(res.columns)
        # lägg till ev. nya kolumner
        for k in row.keys():
            if k not in cols:
                res[k] = np.nan
                cols.append(k)
        res = pd.concat([res, pd.DataFrame([row])[cols]], ignore_index=True)
        _write_df(RESULT_TITLE, res[cols])

# app.py — Del 2/4
# ============================================================
# Datainsamling (Yahoo, ev. Finnhub) + beräkningsmotor
# ============================================================

import math
import requests

# -------------------------
# Små hjälpare
# -------------------------
def _f(x) -> Optional[float]:
    """Tvinga till float eller None."""
    try:
        if x is None or (isinstance(x, str) and x.strip() == ""):
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None

def _nz(x: Optional[float], alt: Optional[float] = None) -> Optional[float]:
    return x if x is not None else alt

def _pos(x: Optional[float]) -> Optional[float]:
    return x if (x is not None and x > 0) else None

# -------------------------
# Yahoo (yfinance) – robust hämtning
# -------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar pris, valuta + centrala nycklar ur yfinance.
    Returnerar alltid dict; saknade fält är None.
    """
    tk = yf.Ticker(ticker)
    out: Dict[str, Any] = {k: None for k in [
        "price", "currency", "market_cap", "enterprise_value",
        "trailing_pe", "forward_pe", "price_to_book",
        "ev_to_revenue", "ev_to_ebitda",
        "shares_out", "net_debt",
        "rev_ttm", "ebitda_ttm", "eps_ttm",
    ]}

    # Pris + valuta (fast_info är snabbast)
    try:
        fi = tk.fast_info
        out["price"] = _f(getattr(fi, "last_price", None))
        out["currency"] = getattr(fi, "currency", None)
        out["market_cap"] = _f(getattr(fi, "market_cap", None))
        out["shares_out"] = _f(getattr(fi, "shares", None))
    except Exception:
        pass

    # .info (kan vara långsam men bra fallback)
    try:
        info = tk.info
    except Exception:
        info = {}

    def gi(k):  # get info
        try:
            return info.get(k)
        except Exception:
            return None

    out["price"] = _nz(out["price"], _f(gi("currentPrice")))
    out["currency"] = _nz(out["currency"], gi("currency"))
    out["market_cap"] = _nz(out["market_cap"], _f(gi("marketCap")))
    out["enterprise_value"] = _f(gi("enterpriseValue"))
    out["trailing_pe"] = _f(gi("trailingPE"))
    out["forward_pe"] = _f(gi("forwardPE"))
    out["price_to_book"] = _f(gi("priceToBook"))
    out["ev_to_revenue"] = _f(gi("enterpriseToRevenue"))
    out["ev_to_ebitda"] = _f(gi("enterpriseToEbitda"))
    out["rev_ttm"] = _f(gi("totalRevenue"))
    out["eps_ttm"] = _f(gi("trailingEps"))
    out["ebitda_ttm"] = _f(gi("ebitda"))

    # Net debt ≈ Enterprise Value − Market Cap
    if _pos(out["enterprise_value"]) and _pos(out["market_cap"]):
        out["net_debt"] = out["enterprise_value"] - out["market_cap"]
    else:
        out["net_debt"] = None

    # Shares fallback via market_cap / price
    if not _pos(out["shares_out"]) and _pos(out["market_cap"]) and _pos(out["price"]):
        try:
            out["shares_out"] = out["market_cap"] / out["price"]
        except Exception:
            pass

    return out

# -------------------------
# Finnhub (valfritt) – EPS/Revenue estimates
# -------------------------
@st.cache_data(ttl=600, show_spinner=False)
def fetch_finnhub_estimates(ticker: str) -> Dict[str, Any]:
    """
    Hämtar EPS nästa 1–2 år (consensus) om FINNHUB_TOKEN finns.
    Returnerar {"eps_1y": float|None, "eps_2y": float|None, "rev_cagr": None}
    (rev_cagr beräknas inte här – vi använder EPS/REV TTM + fallback CAGR i motorn)
    """
    token = _env_or_secret("FINNHUB_TOKEN")
    if not token:
        return {"eps_1y": None, "eps_2y": None}
    base = "https://finnhub.io/api/v1/stock/price-target"
    # Notera: Finnhub har flera endpoints – här använder vi en enkel fallback
    # och försöker även /forecast (earnings estimates).
    try:
        # EPS forecast
        url = f"https://finnhub.io/api/v1/stock/estimate?symbol={ticker}&token={token}"
        r = requests.get(url, timeout=10)
        eps_1y, eps_2y = None, None
        if r.ok:
            js = r.json()
            # Förväntad struktur: list med {"period":"2024-12-31","epsAvg":...}
            rows = js if isinstance(js, list) else js.get("data", [])
            rows = rows or []
            rows = sorted(rows, key=lambda x: x.get("period", ""))
            if rows:
                # Ta närmast 1Y & 2Y
                eps_vals = [ _f(x.get("epsAvg")) for x in rows if _f(x.get("epsAvg")) is not None ]
                if eps_vals:
                    eps_1y = eps_vals[-1]   # närmast framåt
                    eps_2y = eps_vals[-2] if len(eps_vals) > 1 else None
        return {"eps_1y": eps_1y, "eps_2y": eps_2y}
    except Exception:
        return {"eps_1y": None, "eps_2y": None}

# -------------------------
# Multipel-ankare & decay
# -------------------------
def _decay_multiple(anchor: Optional[float], years: int, decay: float, floor_frac: float = 0.6) -> Optional[float]:
    """Minska multipeln linjärt per år (ex. 10%/år). Nedre golv=anchor*floor_frac."""
    if anchor is None:
        return None
    if anchor <= 0:
        return None
    m = anchor * (1.0 - decay * years)
    mmin = anchor * floor_frac
    return max(m, mmin)

def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], settings: Dict[str, str]) -> Optional[float]:
    w = _f(settings.get("pe_anchor_weight_ttm", 0.50)) or 0.50
    if pe_ttm and pe_fwd:
        return w*pe_ttm + (1.0 - w)*pe_fwd
    return pe_ttm or pe_fwd

# -------------------------
# Kärnberäkningar per metod
# -------------------------
def _equity_price_from_ev(ev: float, net_debt: float, shares_out: float) -> Optional[float]:
    try:
        return (ev - (net_debt or 0.0)) / shares_out
    except Exception:
        return None

def _price_from_pe(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    if not _pos(eps) or not _pos(pe):
        return None
    return eps * pe

def _ev_from_sales(sales: Optional[float], multiple: Optional[float]) -> Optional[float]:
    if not _pos(sales) or not _pos(multiple):
        return None
    return sales * multiple

def _ev_from_ebitda(ebitda: Optional[float], multiple: Optional[float]) -> Optional[float]:
    if not _pos(ebitda) or not _pos(multiple):
        return None
    return ebitda * multiple

# -------------------------
# Utdelningsprognos (valfri, använder Settings källskatt)
# -------------------------
def forecast_dividends_net_sek(
    currency: str,
    shares: Optional[float],
    current_dps: Optional[float],
    dps_cagr: Optional[float],
    fx_map: Dict[str, float],
    settings: Dict[str, str],
) -> Dict[str, Optional[float]]:
    """
    Returnerar netto i SEK kommande 1–3 år (en enkel CAGR-modell).
    """
    if not _pos(shares) or current_dps is None:
        return {"y1": 0.0, "y2": 0.0, "y3": 0.0}
    try:
        g = float(dps_cagr) if dps_cagr is not None else 0.0
    except Exception:
        g = 0.0
    withh = get_withholding_for(currency, settings)  # t.ex. 0.15
    fx = fx_map.get((currency or "USD").upper(), 1.0) or 1.0

    def net_sek(years: int) -> float:
        gross = current_dps * ((1.0 + g) ** years) * shares
        net = gross * (1.0 - withh)
        return float(net) * float(fx)

    return {"y1": net_sek(1), "y2": net_sek(2), "y3": net_sek(3)}

# -------------------------
# Huvudmotor – beräkna alla metoder för 0/1/2/3 år
# -------------------------
def compute_methods_for_row(
    row: pd.Series,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
) -> Tuple[pd.DataFrame, str, Dict[str, Any]]:
    """
    Returnerar:
      - df_metoder (kolumner: ['Metod','Idag','1 år','2 år','3 år'])
      - sanity_text
      - meta (bl.a. 'primary_method', 'pe_anchor', 'decay')
    """
    tkr = str(row.get("Ticker", "")).strip()
    snap = fetch_yahoo_snapshot(tkr)

    # Inparametrar
    price = _pos(_nz(snap["price"], row.get("Aktuell kurs")))
    currency = str(_nz(snap["currency"], row.get("Valuta") or "USD")).upper()
    shares_out = _pos(_nz(snap["shares_out"], row.get("Utestående aktier")))
    market_cap = _pos(_nz(snap["market_cap"], None))
    net_debt = _nz(snap["net_debt"], row.get("Net debt"))
    rev_ttm = _pos(_nz(snap["rev_ttm"], row.get("Rev TTM")))
    ebitda_ttm = _pos(_nz(snap["ebitda_ttm"], row.get("EBITDA TTM")))
    eps_ttm = _pos(_nz(snap["eps_ttm"], row.get("EPS TTM")))
    pe_ttm = _pos(_nz(snap["trailing_pe"], row.get("PE TTM")))
    pe_fwd = _pos(_nz(snap["forward_pe"], row.get("PE FWD")))
    ev_rev = _pos(_nz(snap["ev_to_revenue"], row.get("EV/Revenue")))
    ev_ebitda = _pos(_nz(snap["ev_to_ebitda"], row.get("EV/EBITDA")))
    p_b = _pos(_nz(snap["price_to_book"], row.get("P/B")))

    # Estimat / fallback via Finnhub och/eller CAGR
    est = fetch_finnhub_estimates(tkr)
    eps_1y_est = _pos(_nz(est.get("eps_1y"), row.get("EPS 1Y")))
    eps_cagr = _f(row.get("EPS CAGR"))
    rev_cagr = _f(row.get("Rev CAGR"))

    # Om saknas 1Y-EPS men har EPS TTM + CAGR => räkna fram
    if not _pos(eps_1y_est) and _pos(eps_ttm) and eps_cagr is not None:
        eps_1y_est = eps_ttm * (1.0 + float(eps_cagr))

    # Anchor-P/E (50/50 TTM/FWD som default i Settings)
    pe_anchor = _pe_anchor(pe_ttm, pe_fwd, settings)
    decay = _f(settings.get("multiple_decay", 0.10)) or 0.10

    # --- Metoder att beräkna ---
    methods = {
        "pe_hist_vs_eps": {"today": None, "y1": None, "y2": None, "y3": None},
        "ev_sales": {"today": None, "y1": None, "y2": None, "y3": None},
        "ev_ebitda": {"today": None, "y1": None, "y2": None, "y3": None},
        "ev_dacf": {"today": None, "y1": None, "y2": None, "y3": None},  # placeholder
        "ev_fcf": {"today": None, "y1": None, "y2": None, "y3": None},    # placeholder
        "p_fcf": {"today": None, "y1": None, "y2": None, "y3": None},      # placeholder
        "p_nav": {"today": None, "y1": None, "y2": None, "y3": None},      # placeholder
        "p_affo": {"today": None, "y1": None, "y2": None, "y3": None},     # placeholder
        "p_b": {"today": None, "y1": None, "y2": None, "y3": None},
        "p_tbv": {"today": None, "y1": None, "y2": None, "y3": None},      # placeholder
        "p_nii": {"today": None, "y1": None, "y2": None, "y3": None},      # placeholder
    }

    # Säkerställ EV (om saknas): EV ≈ MCAP + NetDebt
    EV_now = None
    if _pos(market_cap) and net_debt is not None:
        EV_now = float(market_cap) + float(net_debt)

    # ---------------- P/E mot EPS ----------------
    # Idag: EPS TTM * pe_anchor
    if _pos(eps_ttm) and _pos(pe_anchor):
        methods["pe_hist_vs_eps"]["today"] = _price_from_pe(eps_ttm, pe_anchor)
        # 1–3 år: använd eps_1y_est och CAGR (fallback), multipel-decay
        # eps_1y
        if _pos(eps_1y_est):
            pe1 = _decay_multiple(pe_anchor, 1, decay)
            methods["pe_hist_vs_eps"]["y1"] = _price_from_pe(eps_1y_est, pe1)
            # eps_2y via CAGR
            if eps_cagr is not None:
                eps_2y = float(eps_1y_est) * (1.0 + float(eps_cagr))
                pe2 = _decay_multiple(pe_anchor, 2, decay)
                methods["pe_hist_vs_eps"]["y2"] = _price_from_pe(eps_2y, pe2)
                # eps_3y
                eps_3y = eps_2y * (1.0 + float(eps_cagr))
                pe3 = _decay_multiple(pe_anchor, 3, decay)
                methods["pe_hist_vs_eps"]["y3"] = _price_from_pe(eps_3y, pe3)

    # ---------------- EV/Sales ----------------
    # Anchor multiple = EV/Revenue
    if _pos(ev_rev) and _pos(rev_ttm) and _pos(shares_out):
        m0 = ev_rev
        ev0 = _ev_from_sales(rev_ttm, m0)
        px0 = _equity_price_from_ev(ev0, net_debt or 0.0, shares_out)
        methods["ev_sales"]["today"] = px0

        if rev_cagr is not None:
            r1 = rev_ttm * (1.0 + float(rev_cagr))
            r2 = r1 * (1.0 + float(rev_cagr))
            r3 = r2 * (1.0 + float(rev_cagr))
            m1 = _decay_multiple(m0, 1, decay)
            m2 = _decay_multiple(m0, 2, decay)
            m3 = _decay_multiple(m0, 3, decay)
            for yrs, r, m in [("y1", r1, m1), ("y2", r2, m2), ("y3", r3, m3)]:
                ev = _ev_from_sales(r, m)
                methods["ev_sales"][yrs] = _equity_price_from_ev(ev, net_debt or 0.0, shares_out)

    # ---------------- EV/EBITDA ----------------
    if _pos(ev_ebitda) and _pos(ebitda_ttm) and _pos(shares_out):
        m0 = ev_ebitda
        ev0 = _ev_from_ebitda(ebitda_ttm, m0)
        px0 = _equity_price_from_ev(ev0, net_debt or 0.0, shares_out)
        methods["ev_ebitda"]["today"] = px0

        # Använd EPS CAGR som proxy för EBITDA-tillväxt om Rev CAGR saknas
        g = _nz(rev_cagr, eps_cagr)
        if g is not None:
            e1 = ebitda_ttm * (1.0 + float(g))
            e2 = e1 * (1.0 + float(g))
            e3 = e2 * (1.0 + float(g))
            m1 = _decay_multiple(m0, 1, decay)
            m2 = _decay_multiple(m0, 2, decay)
            m3 = _decay_multiple(m0, 3, decay)
            for yrs, e, m in [("y1", e1, m1), ("y2", e2, m2), ("y3", e3, m3)]:
                ev = _ev_from_ebitda(e, m)
                methods["ev_ebitda"][yrs] = _equity_price_from_ev(ev, net_debt or 0.0, shares_out)

    # ---------------- P/B (om PB finns) ----------------
    if _pos(p_b) and _pos(price):
        # Anta oförändrat BVPS men decaya multipeln
        m0 = p_b
        methods["p_b"]["today"] = price  # price ≈ PB * BVPS -> dagens price är baseline
        m1 = _decay_multiple(m0, 1, decay)
        m2 = _decay_multiple(m0, 2, decay)
        m3 = _decay_multiple(m0, 3, decay)
        # Utan explicit BVPS får vi bara skala dagens pris med multipel-förändringen
        if _pos(m0):
            scale = lambda mk: (mk / m0) * price if _pos(mk) else None
            methods["p_b"]["y1"] = scale(m1)
            methods["p_b"]["y2"] = scale(m2)
            methods["p_b"]["y3"] = scale(m3)

    # ---------------- Sanity text ----------------
    sanity_bits = []
    sanity_bits.append("price ok" if _pos(price) else "price —")
    sanity_bits.append("eps_ttm ok" if _pos(eps_ttm) else "eps_ttm —")
    sanity_bits.append("rev_ttm ok" if _pos(rev_ttm) else "rev_ttm —")
    sanity_bits.append("ebitda_ttm ok" if _pos(ebitda_ttm) else "ebitda_ttm —")
    sanity_bits.append("shares ok" if _pos(shares_out) else "shares —")
    sanity = ", ".join(sanity_bits)

    # ---------------- Konvertera till DataFrame i konsistent ordning ----------------
    order = ["pe_hist_vs_eps", "ev_sales", "ev_ebitda", "ev_dacf", "ev_fcf",
             "p_fcf", "p_nav", "p_affo", "p_b", "p_tbv", "p_nii"]
    rows = []
    for m in order:
        d = methods[m]
        rows.append([
            m,
            _f(d["today"]),
            _f(d["y1"]),
            _f(d["y2"]),
            _f(d["y3"]),
        ])
    met_df = pd.DataFrame(rows, columns=["Metod", "Idag", "1 år", "2 år", "3 år"])

    # ---------------- Välj primär metod ----------------
    # Regler: 1) flest icke-NaN, 2) preferensordning ev_ebitda > ev_sales > pe_hist_vs_eps > p_b
    prefer = ["ev_ebitda", "ev_sales", "pe_hist_vs_eps", "p_b"]
    counts = met_df.set_index("Metod")[["Idag", "1 år", "2 år", "3 år"]].notna().sum(axis=1)
    best = counts.sort_values(ascending=False)
    primary = None
    if not best.empty:
        top_k = list(best[best == best.max()].index)
        for cand in prefer:
            if cand in top_k:
                primary = cand
                break
        if primary is None:
            primary = top_k[0]

    meta = {
        "primary_method": primary,
        "currency": currency,
        "pe_anchor": pe_anchor,
        "decay": decay,
        "price": price,
        "shares_out": shares_out,
        "net_debt": net_debt,
    }
    return met_df, sanity, meta

# app.py — Del 3/4
# ============================================================
# Primär fair value, UI-rendering och skrivning till Sheets
# ============================================================

# --------- Settings & FX helpers (förkortade wrappers mot Del 1) ----------
def get_settings_dict() -> Dict[str, str]:
    df = _read_df(SETTINGS_TITLE)
    out = {}
    for k, v in zip(df["Nyckel"].astype(str), df["Värde"]):
        out[str(k)] = str(v)
    # standardvärden
    out.setdefault("pe_anchor_weight_ttm", "0.50")
    out.setdefault("multiple_decay", "0.10")
    out.setdefault("withholding_USD", "0.15")
    out.setdefault("withholding_NOK", "0.25")
    out.setdefault("withholding_CAD", "0.15")
    return out

def build_fx_map() -> Dict[str, float]:
    """Returnerar { 'USD': SEK_per_USD, ... } från fliken Valutakurser."""
    fx_df = _read_df(FX_TITLE)
    # förväntar kolumner: Currency, SEK_per_unit
    out = {}
    for _, r in fx_df.iterrows():
        c = str(r.get("Currency", "")).upper()
        v = _f(r.get("SEK_per_unit"))
        if c and v:
            out[c] = v
    out.setdefault("USD", 11.0)  # defensiva fallback
    out.setdefault("EUR", 11.5)
    out.setdefault("NOK", 1.0)
    out.setdefault("CAD", 8.0)
    out.setdefault("SEK", 1.0)
    return out

def get_withholding_for(currency: str, settings: Dict[str, str]) -> float:
    key = f"withholding_{(currency or 'USD').upper()}"
    try:
        return float(settings.get(key, "0.15"))
    except Exception:
        return 0.15

# ----------------- Resultat-skrivning -----------------
def _append_or_update_result(
    ticker: str,
    currency: str,
    method: str,
    today: Optional[float],
    y1: Optional[float],
    y2: Optional[float],
    y3: Optional[float],
):
    """Spara primära riktkurser till fliken Resultat (append eller update per ticker)."""
    res_df = _read_df(RESULT_TITLE)
    ts = now_stamp()
    row_new = {
        "Timestamp": ts,
        "Ticker": ticker,
        "Valuta": currency,
        "Metod": method or "",
        "Riktkurs idag": today,
        "Riktkurs 1 år": y1,
        "Riktkurs 2 år": y2,
        "Riktkurs 3 år": y3,
    }
    if "Ticker" in res_df.columns and not res_df.empty and ticker in set(res_df["Ticker"].astype(str)):
        # uppdatera sista förekomsten
        idx = res_df.index[res_df["Ticker"].astype(str) == ticker][-1]
        for k, v in row_new.items():
            res_df.at[idx, k] = v
    else:
        res_df = pd.concat([res_df, pd.DataFrame([row_new])], ignore_index=True)

    _write_df(RESULT_TITLE, res_df)

# ----------------- Uppdatera estimat/CAGR till Data -----------------
def _update_estimates_for_ticker(ticker: str, eps1y: Optional[float], eps_cagr: Optional[float], rev_cagr: Optional[float]):
    df = _read_df(DATA_TITLE)
    if "Ticker" not in df.columns:
        st.warning("Data-bladet saknar kolumnen 'Ticker'.")
        return
    mask = df["Ticker"].astype(str) == str(ticker)
    if not mask.any():
        st.warning(f"Hittade inte ticker {ticker} i Data-bladet.")
        return
    # säkerställ kolumner
    for col in ["EPS 1Y", "EPS CAGR", "Rev CAGR"]:
        if col not in df.columns:
            df[col] = None
    if eps1y is not None:   df.loc[mask, "EPS 1Y"] = float(eps1y)
    if eps_cagr is not None: df.loc[mask, "EPS CAGR"] = float(eps_cagr)
    if rev_cagr is not None: df.loc[mask, "Rev CAGR"] = float(rev_cagr)
    _write_df(DATA_TITLE, df)

# ----------------- Primärt mål från metodtabell -----------------
def _primary_targets(met_df: pd.DataFrame, primary: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if primary and primary in set(met_df["Metod"]):
        row = met_df[met_df["Metod"] == primary].iloc[0]
        return (_f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"]))
    # fallback: ta rad med flest värden
    counts = met_df.set_index("Metod")[["Idag","1 år","2 år","3 år"]].notna().sum(axis=1)
    if not counts.empty:
        best = counts.idxmax()
        row = met_df[met_df["Metod"] == best].iloc[0]
        return (_f(row["Idag"]), _f(row["1 år"]), _f(row["2 år"]), _f(row["3 år"]))
    return (None, None, None, None)

# ----------------- Presentations-widgets -----------------
def _fmt_money(v: Optional[float], currency: str) -> str:
    if v is None:
        return "–"
    try:
        return f"{v:,.2f} {currency}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} {currency}"

def _fmt_sek(v: Optional[float]) -> str:
    if v is None:
        return "0 SEK"
    try:
        return f"{v:,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

def render_company_view(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]):
    tkr = str(row.get("Ticker", "")).strip()
    name = str(row.get("Bolagsnamn", tkr)).strip()
    bucket = str(row.get("Bucket", "")).strip() or str(row.get("Bucket/Kategori", "")).strip()
    st.markdown(f"#### {tkr} • {name} • *{bucket or '—'}*")

    met_df, sanity, meta = compute_methods_for_row(row, settings, fx_map)
    currency = meta.get("currency") or str(row.get("Valuta") or "USD").upper()

    # Visa metodtabellen
    st.caption(f"Sanity: {sanity}")
    grid = met_df.rename(columns={"Metod":"Metod","Idag":"Idag","1 år":"1 år","2 år":"2 år","3 år":"3 år"})
    st.dataframe(grid, use_container_width=True)

    # Primära riktkurser
    p0, p1, p2, p3 = _primary_targets(met_df, meta.get("primary_method"))
    st.markdown("### 🎯 Primär riktkurs")
    cols = st.columns(4)
    cols[0].metric("Idag", _fmt_money(p0, currency), delta=None)
    cols[1].metric("1 år", _fmt_money(p1, currency), delta=None)
    cols[2].metric("2 år", _fmt_money(p2, currency), delta=None)
    cols[3].metric("3 år", _fmt_money(p3, currency), delta=None)
    st.caption(f"Metod: **{meta.get('primary_method') or '—'}** • Valuta: **{currency}** • Ankare P/E vikt: **{float(settings.get('pe_anchor_weight_ttm','0.5'))*100:.0f}% TTM**")

    # Utdelning netto (SEK)
    shares = _f(row.get("Antal aktier")) or 0.0
    dps = _f(row.get("Årlig utdelning"))
    dps_cagr = _f(row.get("Utdelnings CAGR"))
    divs = forecast_dividends_net_sek(currency, shares, dps, dps_cagr, fx_map, settings)
    with st.expander("💰 Utdelning (netto, SEK)", expanded=True):
        st.write(f"• **1 år:** {_fmt_sek(divs['y1'])} • **2 år:** {_fmt_sek(divs['y2'])} • **3 år:** {_fmt_sek(divs['y3'])}")
        st.caption(f"Källskatt: {int(get_withholding_for(currency, settings)*100)}% • Antal aktier: {int(shares)}")

    # Innehavsvärde nu (SEK)
    price_now = meta.get("price")
    fx = fx_map.get(currency, 1.0)
    port_val = (price_now or 0.0) * shares * fx
    with st.expander("🧾 Innehavsvärde", expanded=True):
        st.write(f"Totalt värde nu: **{_fmt_sek(port_val)}**")

    # Åtgärdsrad: uppdatera estimat/CAGR och spara primär riktkurs
    st.divider()
    c1, c2, c3 = st.columns(3)
    if c1.button("🔄 Uppdatera Estimat/CAGR (och spara)"):
        # Hämta estimat igen och räkna fallback CAGR om möjligt
        snap = fetch_yahoo_snapshot(tkr)
        est = fetch_finnhub_estimates(tkr)
        eps_ttm = _pos(_nz(snap["eps_ttm"], row.get("EPS TTM")))
        eps1 = _pos(est.get("eps_1y"))
        eps_cagr = _f(row.get("EPS CAGR"))
        if eps_cagr is None and _pos(eps_ttm) and _pos(eps1):
            try:
                eps_cagr = (eps1/eps_ttm) - 1.0
            except Exception:
                eps_cagr = None
        # revenue CAGR: enkel proxy om vi har EV/Sales + 1y sales implicit? saknas => lämna
        rev_cagr = _f(row.get("Rev CAGR"))
        _update_estimates_for_ticker(tkr, eps1, eps_cagr, rev_cagr)
        st.success("Estimat/CAGR uppdaterade och sparade.")

    if c2.button("💾 Spara primär riktkurs till Resultat"):
        _append_or_update_result(tkr, currency, meta.get("primary_method"), p0, p1, p2, p3)
        st.success("Sparat till fliken Resultat.")

    if c3.button("📷 Spara kvartalssnapshot"):
        try:
            save_quarter_snapshot(tkr, met_df, meta)  # definieras i Del 4
            st.success("Kvartalssnapshot sparad.")
        except NameError:
            st.warning("Snapshot-funktionen laddas i Del 4. Spara igen efter full uppladdning.")

# app.py — Del 4/4
# ============================================================
# Datainsamling (Yahoo + Finnhub), beräkningsmotor, snapshots
# ============================================================

import json, time
from typing import Any, Dict, Optional, Tuple, List

# ---- försäkringar att hjälp-funktioner finns (om Del 1 inte redan definierat dem) ----
try:
    _ = _f  # typ: ignore
except NameError:
    def _f(x) -> Optional[float]:
        try:
            if x is None or (isinstance(x, str) and x.strip() == ""):
                return None
            return float(str(x).replace(" ", "").replace(",", "."))
        except Exception:
            return None

try:
    _ = _nz  # typ: ignore
except NameError:
    def _nz(v, dflt=0.0):
        return v if (v is not None and v == v) else dflt

try:
    _ = _pos  # typ: ignore
except NameError:
    def _pos(v) -> Optional[float]:
        v = _f(v)
        return v if (v is not None and v > 0) else None

try:
    _ = now_stamp  # typ: ignore
except NameError:
    from datetime import datetime
    def now_stamp() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ============================================================
#                 Hämtare (Yahoo + Finnhub)
# ============================================================

def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """
    Hämtar snabb-snapshot från Yahoo Finance via yfinance.
    Returnerar: price, currency, market_cap, ev, shares, eps_ttm, pe_ttm, pe_fwd,
                revenue_ttm, ebitda_ttm, ev_to_sales, ev_to_ebitda, p_to_book
    """
    import yfinance as yf
    snap: Dict[str, Any] = {}
    try:
        tk = yf.Ticker(ticker)
        fi = {}
        try:
            fi = tk.fast_info or {}
        except Exception:
            fi = {}
        inf = {}
        try:
            inf = tk.info or {}
        except Exception:
            inf = {}

        # Pris & valuta
        price = fi.get("last_price") or inf.get("currentPrice") or inf.get("regularMarketPrice")
        currency = fi.get("currency") or inf.get("currency") or "USD"

        # Kap/EV
        mcap = inf.get("marketCap")
        cash = inf.get("totalCash")
        debt = inf.get("totalDebt")
        ev = inf.get("enterpriseValue") or (
            (mcap or 0) + (debt or 0) - (cash or 0)
            if (mcap is not None and debt is not None and cash is not None) else None
        )

        # Bas-nycklar
        shares = inf.get("sharesOutstanding")
        eps_ttm = inf.get("trailingEps")
        pe_ttm = inf.get("trailingPE")
        pe_fwd = inf.get("forwardPE")
        rev_ttm = inf.get("totalRevenue")
        ebitda_ttm = inf.get("ebitda")
        ev_rev = inf.get("enterpriseToRevenue")
        ev_ebitda = inf.get("enterpriseToEbitda")
        p_b = inf.get("priceToBook")
        bvps = inf.get("bookValue")  # per-aktie

        # packa
        snap.update(
            price=_f(price),
            currency=str(currency or "USD").upper(),
            market_cap=_f(mcap),
            ev=_f(ev),
            shares=_f(shares),
            eps_ttm=_f(eps_ttm),
            pe_ttm=_f(pe_ttm),
            pe_fwd=_f(pe_fwd),
            revenue_ttm=_f(rev_ttm),
            ebitda_ttm=_f(ebitda_ttm),
            ev_to_sales=_f(ev_rev),
            ev_to_ebitda=_f(ev_ebitda),
            p_to_book=_f(p_b),
            bvps=_f(bvps),
        )
    except Exception:
        pass
    return snap

def fetch_finnhub_estimates(ticker: str) -> Dict[str, Any]:
    """
    Hämtar EPS-estimat ~1 år framåt om FINNHUB_API_KEY finns i secrets.
    Returnerar: eps_1y (forward 12m), pe_band (p25/p50/p75) om tillgängligt (ofta ej).
    Robust fallback: tom dict.
    """
    est: Dict[str, Any] = {}
    import os, requests
    key = os.environ.get("FINNHUB_API_KEY") or st.secrets.get("FINNHUB_API_KEY", None)
    if not key:
        return est
    try:
        # EPS forward (enkelt prox: senaste års-estimat)
        url = f"https://finnhub.io/api/v1/stock/eps?symbol={ticker}&token={key}"
        time.sleep(0.6)
        r = requests.get(url, timeout=10)
        if r.ok:
            data = r.json() or []
            # plocka närmaste kommande årsestimat
            eps_vals = [ _f(x.get("epsEstimate")) for x in data if _f(x.get("epsEstimate")) ]
            if eps_vals:
                est["eps_1y"] = sorted(eps_vals)[-1]
        # (valfritt) multiband – lämnas tomt om ej finns
    except Exception:
        pass
    return est

# ============================================================
#                      Beräkningsmotor
# ============================================================

def _decay_mult(base: Optional[float], years: int, decay: float) -> Optional[float]:
    if base is None:
        return None
    try:
        return base * ((1.0 - decay) ** years)
    except Exception:
        return base

def _pe_anchor(pe_ttm: Optional[float], pe_fwd: Optional[float], w_ttm: float) -> Optional[float]:
    if pe_ttm is None and pe_fwd is None:
        return None
    if pe_ttm is None:
        return pe_fwd
    if pe_fwd is None:
        return pe_ttm
    return pe_ttm * w_ttm + pe_fwd * (1.0 - w_ttm)

def _eps_path(eps_ttm: Optional[float], eps_1y: Optional[float], eps_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Returnera EPS om 1,2,3 år. Om eps_1y saknas: använd cagr från ttm."""
    if eps_1y is None and eps_ttm is not None and eps_cagr is not None:
        eps_1y = eps_ttm * (1.0 + eps_cagr)
    if eps_ttm is None and eps_1y is not None and eps_cagr is not None:
        # gissa bakåt ett år (mild fallback)
        try:
            eps_ttm = eps_1y / (1.0 + eps_cagr)
        except Exception:
            pass
    if eps_1y is None:
        return (None, None, None)
    # extrapolera vidare 2y, 3y
    e2 = None if eps_cagr is None else eps_1y * (1.0 + eps_cagr)
    e3 = None if eps_cagr is None or e2 is None else e2 * (1.0 + eps_cagr)
    return (eps_1y, e2, e3)

def _rev_path(rev_ttm: Optional[float], rev_cagr: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if rev_ttm is None or rev_cagr is None:
        return (None, None, None)
    r1 = rev_ttm * (1.0 + rev_cagr)
    r2 = r1 * (1.0 + rev_cagr)
    r3 = r2 * (1.0 + rev_cagr)
    return (r1, r2, r3)

def _ebitda_path(ebitda_ttm: Optional[float], rev_path: Tuple[Optional[float],Optional[float],Optional[float]], margin_keep: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Skala EBITDA ungefär med omsättningsbanan om marginal_keep finns (0..1)."""
    if ebitda_ttm is None:
        return (None, None, None)
    if margin_keep is None:
        margin_keep = 1.0
    r1, r2, r3 = rev_path
    out = []
    for r in (r1, r2, r3):
        if r is None or r == 0:
            out.append(None)
        else:
            out.append(ebitda_ttm * margin_keep * (r / _nz(r1, r or 1.0)) if r1 else None)
    # baseline: om vi saknade omsättningsbana -> håll EBITDA konstant
    if not any(out):
        out = [ebitda_ttm, ebitda_ttm, ebitda_ttm]
    return tuple(out)  # type: ignore

def _price_from_PE(eps: Optional[float], pe: Optional[float]) -> Optional[float]:
    if eps is None or pe is None:
        return None
    return eps * pe

def _price_from_EV_sales(ev_rev: Optional[float], rev: Optional[float], shares: Optional[float], debt_cash_ev: Optional[float], price_now: Optional[float], mcap_now: Optional[float]) -> Optional[float]:
    if ev_rev is None or rev is None:
        return None
    try:
        ev = ev_rev * rev
        # pris = (EV - nettoskuld)/antal aktier. Om saknas: fallback mot mcap/price relation
        if shares and debt_cash_ev is not None:
            price = (ev - debt_cash_ev) / shares
        elif mcap_now and price_now:
            # håll relation mcap/price konstant
            k = mcap_now / price_now
            price = (ev / k)
        else:
            return None
        return price
    except Exception:
        return None

def _price_from_EV_ebitda(ev_ebitda: Optional[float], ebitda: Optional[float], shares: Optional[float], debt_cash_ev: Optional[float], price_now: Optional[float], mcap_now: Optional[float]) -> Optional[float]:
    if ev_ebitda is None or ebitda is None:
        return None
    try:
        ev = ev_ebitda * ebitda
        if shares and debt_cash_ev is not None:
            price = (ev - debt_cash_ev) / shares
        elif mcap_now and price_now:
            k = mcap_now / price_now
            price = (ev / k)
        else:
            return None
        return price
    except Exception:
        return None

def _price_from_PB(pb: Optional[float], bvps: Optional[float]) -> Optional[float]:
    if pb is None or bvps is None:
        return None
    return pb * bvps

def _debt_minus_cash_from_snapshot(snap: Dict[str, Any]) -> Optional[float]:
    # Vi har inte alltid raw debt/cash här – utnyttja ev - mcap om båda finns
    ev = snap.get("ev")
    mcap = snap.get("market_cap")
    if ev is not None and mcap is not None:
        try:
            return float(ev) - float(mcap)
        except Exception:
            return None
    return None

# ------------------------------------------------------------
# Huvud: beräkna alla metoder för en rad
# ------------------------------------------------------------
def compute_methods_for_row(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]):
    ticker = str(row.get("Ticker", "")).strip()
    primary_method = str(row.get("Primär metod") or "ev_ebitda")
    w_ttm = _f(settings.get("pe_anchor_weight_ttm")) or 0.50
    decay = _f(settings.get("multiple_decay")) or 0.10

    # 1) Hämta live-snapshot + estimat
    snap = fetch_yahoo_snapshot(ticker)
    time.sleep(0.5)  # mild throttling
    est = fetch_finnhub_estimates(ticker)

    # 2) Basdata (fallback till Data-bladet om saknas)
    price_now = _pos(snap.get("price")) or _pos(row.get("Aktuell kurs"))
    currency = (snap.get("currency") or row.get("Valuta") or "USD")
    currency = str(currency).upper()
    mcap_now = _pos(snap.get("market_cap"))
    ev_now = _pos(snap.get("ev"))
    shares = _pos(snap.get("shares")) or _pos(row.get("Utestående aktier"))
    debt_minus_cash = _debt_minus_cash_from_snapshot(snap)

    eps_ttm = _pos(snap.get("eps_ttm")) or _pos(row.get("EPS TTM"))
    pe_ttm = _pos(snap.get("pe_ttm"))
    pe_fwd = _pos(snap.get("pe_fwd"))

    eps_1y = _pos(est.get("eps_1y")) or _pos(row.get("EPS 1Y"))
    eps_cagr = _f(row.get("EPS CAGR"))

    rev_ttm = _pos(snap.get("revenue_ttm")) or _pos(row.get("Omsättning idag"))
    rev_cagr = _f(row.get("Rev CAGR"))

    ebitda_ttm = _pos(snap.get("ebitda_ttm"))
    ev_to_sales = _pos(snap.get("ev_to_sales"))
    ev_to_ebitda = _pos(snap.get("ev_to_ebitda"))
    p_to_book = _pos(snap.get("p_to_book")) or _pos(row.get("P/B"))
    bvps = _pos(snap.get("BVPS")) or _pos(snap.get("bvps")) or _pos(row.get("BVPS"))

    # 3) Ankare & paths
    pe_anchor0 = _pe_anchor(pe_ttm, pe_fwd, w_ttm)
    pe1 = _decay_mult(pe_anchor0, 1, decay)
    pe2 = _decay_mult(pe_anchor0, 2, decay)
    pe3 = _decay_mult(pe_anchor0, 3, decay)

    eps1, eps2, eps3 = _eps_path(eps_ttm, eps_1y, eps_cagr)
    r1, r2, r3 = _rev_path(rev_ttm, rev_cagr)
    # anta margin_keep = 1 (behåll EBITDA som följer omsättning proportionellt), fallback konstant
    e1, e2, e3 = _ebitda_path(ebitda_ttm, (r1, r2, r3), margin_keep=1.0)

    # EV multiplar med decay
    evs0, evs1, evs2, evs3 = ev_to_sales, _decay_mult(ev_to_sales,1,decay), _decay_mult(ev_to_sales,2,decay), _decay_mult(ev_to_sales,3,decay)
    eve0, eve1, eve2, eve3 = ev_to_ebitda, _decay_mult(ev_to_ebitda,1,decay), _decay_mult(ev_to_ebitda,2,decay), _decay_mult(ev_to_ebitda,3,decay)
    pb0, pb1, pb2, pb3 = p_to_book, _decay_mult(p_to_book,1,decay), _decay_mult(p_to_book,2,decay), _decay_mult(p_to_book,3,decay)

    # 4) Priser per metod
    row_methods: List[Dict[str, Any]] = []

    # pe_hist_vs_eps
    p0 = _price_from_PE(eps_ttm, pe_anchor0)
    p1 = _price_from_PE(eps1, pe1)
    p2 = _price_from_PE(eps2, pe2)
    p3 = _price_from_PE(eps3, pe3)
    row_methods.append({"Metod": "pe_hist_vs_eps", "Idag": p0, "1 år": p1, "2 år": p2, "3 år": p3})

    # ev_sales
    ps0 = _price_from_EV_sales(evs0, rev_ttm, shares, debt_minus_cash, price_now, mcap_now)
    ps1 = _price_from_EV_sales(evs1, r1,      shares, debt_minus_cash, price_now, mcap_now)
    ps2 = _price_from_EV_sales(evs2, r2,      shares, debt_minus_cash, price_now, mcap_now)
    ps3 = _price_from_EV_sales(evs3, r3,      shares, debt_minus_cash, price_now, mcap_now)
    row_methods.append({"Metod": "ev_sales", "Idag": ps0, "1 år": ps1, "2 år": ps2, "3 år": ps3})

    # ev_ebitda
    pe0 = _price_from_EV_ebitda(eve0, ebitda_ttm, shares, debt_minus_cash, price_now, mcap_now)
    pe1p = _price_from_EV_ebitda(eve1, e1,        shares, debt_minus_cash, price_now, mcap_now)
    pe2p = _price_from_EV_ebitda(eve2, e2,        shares, debt_minus_cash, price_now, mcap_now)
    pe3p = _price_from_EV_ebitda(eve3, e3,        shares, debt_minus_cash, price_now, mcap_now)
    row_methods.append({"Metod": "ev_ebitda", "Idag": pe0, "1 år": pe1p, "2 år": pe2p, "3 år": pe3p})

    # ev_dacf (prox = ev_ebitda)
    row_methods.append({"Metod": "ev_dacf", "Idag": pe0, "1 år": pe1p, "2 år": pe2p, "3 år": pe3p})

    # p_b (kräver BVPS och P/B)
    pbp0 = _price_from_PB(pb0, bvps)
    pbp1 = _price_from_PB(pb1, bvps)
    pbp2 = _price_from_PB(pb2, bvps)
    pbp3 = _price_from_PB(pb3, bvps)
    row_methods.append({"Metod": "p_b", "Idag": pbp0, "1 år": pbp1, "2 år": pbp2, "3 år": pbp3})

    # tomma ställ före REIT/BDC etc (kräver per-aktie tal som inte alltid finns)
    for m in ("p_nav","p_tbv","p_affo","p_fcf","ev_fcf"):
        row_methods.append({"Metod": m, "Idag": None, "1 år": None, "2 år": None, "3 år": None})

    methods_df = pd.DataFrame(row_methods)

    # 5) Meta + sanity
    sanity_bits = []
    sanity_bits.append("price ok" if price_now else "price —")
    sanity_bits.append("eps_ttm ok" if eps_ttm else "eps_ttm —")
    sanity_bits.append("rev_ttm ok" if rev_ttm else "rev_ttm —")
    sanity_bits.append("ev/ebitda ok" if ebitda_ttm and ev_to_ebitda else "ev/ebitda —")
    sanity_bits.append("shares ok" if shares else "shares —")
    sanity = ", ".join(sanity_bits)

    meta = {
        "currency": currency,
        "price": price_now,
        "primary_method": primary_method,
        "pe_anchor": pe_anchor0,
        "pe_w_ttm": w_ttm,
        "decay": decay,
        "eps_ttm": eps_ttm,
        "eps1": eps1, "eps2": eps2, "eps3": eps3,
    }
    return methods_df, sanity, meta

# ============================================================
#                 Utdelningsprognos (netto, SEK)
# ============================================================
def forecast_dividends_net_sek(currency: str, shares: float, dps_now: Optional[float], dps_cagr: Optional[float], fx_map: Dict[str, float], settings: Dict[str, str]) -> Dict[str, float]:
    fx = fx_map.get(currency.upper(), 1.0)
    wh = get_withholding_for(currency, settings)
    if not shares or dps_now is None:
        return {"y1": 0.0, "y2": 0.0, "y3": 0.0}
    g = dps_cagr if (dps_cagr is not None) else 0.0
    d1 = dps_now * (1.0 + g)
    d2 = d1 * (1.0 + g)
    d3 = d2 * (1.0 + g)
    def net(v): return max(0.0, v) * shares * (1.0 - wh) * fx
    return {"y1": net(d1), "y2": net(d2), "y3": net(d3)}

# ============================================================
#                 Snapshot till flik (kvartalsvis)
# ============================================================
def save_quarter_snapshot(ticker: str, methods_df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    df_prev = _read_df(SNAPSHOT_TITLE)
    ts = now_stamp()
    rows = []
    for _, r in methods_df.iterrows():
        rows.append({
            "Timestamp": ts,
            "Ticker": ticker,
            "Valuta": meta.get("currency"),
            "Metod": r.get("Metod"),
            "Idag": _f(r.get("Idag")),
            "1 år": _f(r.get("1 år")),
            "2 år": _f(r.get("2 år")),
            "3 år": _f(r.get("3 år")),
            "Ankare PE": _f(meta.get("pe_anchor")),
            "Decay": _f(meta.get("decay")),
        })
    snap_df = pd.concat([df_prev, pd.DataFrame(rows)], ignore_index=True)
    _write_df(SNAPSHOT_TITLE, snap_df)

# ============================================================
#               (Slut på Del 4/4 — main() finns i Del 2)
# ============================================================
