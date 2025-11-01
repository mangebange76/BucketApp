# app.py — Del 1/4: Grund, Google Sheets & hjälpfunktioner

from __future__ import annotations

# ==============
# Importer
# ==============
import os
import time
from datetime import datetime
from typing import Any, Dict, Tuple, List, Optional

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials

# För DataFrame <-> Sheets (med fallback om libben saknas)
try:
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
except Exception:
    set_with_dataframe = None
    get_as_dataframe = None

# =========================
# Sidan
# =========================
st.set_page_config(page_title="BucketApp – Fair Value & Riktkurser", layout="wide")

# =========================
# UI-tema & små helpers
# =========================
def _fmt_num(x, nd=2):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "–"
        return f"{float(x):,.{nd}f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(x)

def _fmt_pct(x, nd=1):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "–"
        return f"{float(x)*100:.{nd}f}%".replace(".", ",")
    except Exception:
        return str(x)

def _fmt_ccy(x, ccy="USD", nd=2):
    return f"{_fmt_num(x, nd)} {ccy}"

def _coerce_float(x) -> float:
    try:
        if x is None:
            return np.nan
        if isinstance(x, (int, float, np.number)):
            return float(x)
        s = str(x).strip().replace(" ", "").replace(",", ".")
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return np.nan
        return float(s)
    except Exception:
        return np.nan

def _safe_div(a, b):
    a = _coerce_float(a)
    b = _coerce_float(b)
    if b is None or np.isnan(b) or b == 0:
        return np.nan
    return a / b

def now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ======================================
# Läs secrets & initiera Google-klient
# ======================================
def _normalize_private_key(creds_dict: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds_dict.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds_dict["private_key"] = pk.replace("\\n", "\n")
    return creds_dict

@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    # Secrets: SHEET_URL eller SHEET_ID måste finnas
    if "GOOGLE_CREDENTIALS" not in st.secrets:
        st.stop()
    creds_dict = dict(st.secrets["GOOGLE_CREDENTIALS"])
    creds_dict = _normalize_private_key(creds_dict)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def _open_spreadsheet(_gc: gspread.Client):
    """
    OBS: _gc med underscore => undviker Streamlits hash/ pickle-fel.
    Öppnar via URL om satt, annars via ID.
    """
    sheet_url = st.secrets.get("SHEET_URL", "").strip()
    sheet_id = st.secrets.get("SHEET_ID", "").strip()
    if sheet_url:
        return _gc.open_by_url(sheet_url)
    if sheet_id:
        return _gc.open_by_key(sheet_id)
    st.error("SHEET_URL eller SHEET_ID saknas i secrets.")
    st.stop()

# Titlar vi använder i arket
DATA_TITLE = "Data"          # Tickers, bucket, antal aktier, GAV, mm
RATES_TITLE = "Valutakurser" # FX cache
RESULT_TITLE = "Resultat"    # Sparade riktkurser

def _ensure_worksheet(ss: gspread.Spreadsheet, title: str, cols: Optional[List[str]] = None) -> gspread.Worksheet:
    try:
        ws = ss.worksheet(title)
        return ws
    except Exception:
        ws = ss.add_worksheet(title=title, rows=1000, cols=50)
        if cols:
            ws.update("A1", [cols])
        return ws

# ======================================
# DataFrame <-> Sheets helpers
# ======================================
@st.cache_data(ttl=60, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
    """Cachead läsning baserat på titel (inte Worksheet-objektet => hashbar)."""
    gc = _build_gspread_client()
    ss = _open_spreadsheet(gc)
    ws = _ensure_worksheet(ss, title)
    if get_as_dataframe is None:
        # Minimal fallback om gspread_dataframe saknas
        rows = ws.get_all_records(empty2nan=True)
        df = pd.DataFrame(rows)
        return df
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    # Rensa helt tomma rader/kolumner
    if df is None:
        return pd.DataFrame()
    df = df.dropna(how="all")
    # Standardisera kolumnnamn
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _write_df(title: str, df: pd.DataFrame):
    gc = _build_gspread_client()
    ss = _open_spreadsheet(gc)
    ws = _ensure_worksheet(ss, title)
    if df is None:
        return
    # Fyll NaN med tom str för att undvika problem vid skrivning
    df_out = df.copy()
    if set_with_dataframe is None:
        # Fallback: skriv rad för rad (långsamt – men funkar)
        ws.clear()
        ws.update("A1", [list(df_out.columns)])
        if not df_out.empty:
            ws.update(f"A2", df_out.astype(str).values.tolist())
        return
    ws.clear()
    set_with_dataframe(ws, df_out, include_index=False, include_column_header=True, resize=True)

# ======================================
# Standardkolumner & init av flikar
# ======================================
DEFAULT_DATA_COLS = [
    "Ticker", "Bolagsnamn", "Bucket", "Valuta",
    "Antal aktier", "GAV (SEK)",
    "Utestående aktier",
    # Basdata (hämtas auto)
    "Aktuell kurs", "Market Cap", "EV",
    "P/E TTM", "P/E FWD", "P/S TTM", "P/B", "EV/EBITDA", "EV/Sales",
    "EPS TTM", "Omsättning TTM",
    # Estimat/CAGR som vi beräknar & sparar
    "EPS 1y", "EPS 2y", "EPS 3y",
    "REV 1y", "REV 2y", "REV 3y",
    "EPS-källa", "REV-källa", "g_eps", "g_rev",
    # Utdelning
    "Utdelning TTM", "Utdelning CAGR 5y (%)",
    # Stämplar
    "Senast auto uppdaterad", "Auto källa"
]

DEFAULT_RATES_COLS = [
    "Valuta", "Mot SEK", "Senast uppdaterad"
]

DEFAULT_RESULT_COLS = [
    "Timestamp", "Ticker", "Valuta",
    "Primär metod", "Pris nu",
    "Fair value idag", "Fair value 1y", "Fair value 2y", "Fair value 3y",
    "Uppsida idag (%)", "Uppsida 1y (%)", "Uppsida 2y (%)", "Uppsida 3y (%)"
]

def _bootstrap_sheets():
    gc = _build_gspread_client()
    ss = _open_spreadsheet(gc)
    # Data
    ws = _ensure_worksheet(ss, DATA_TITLE, DEFAULT_DATA_COLS)
    df = _read_df(DATA_TITLE)
    if df.empty:
        _write_df(DATA_TITLE, pd.DataFrame(columns=DEFAULT_DATA_COLS))
    else:
        # Se till att minst alla kolumner finns
        cols = list(df.columns)
        for c in DEFAULT_DATA_COLS:
            if c not in cols:
                cols.append(c)
        df = df.reindex(columns=cols)
        _write_df(DATA_TITLE, df)
    # Valutakurser
    ws2 = _ensure_worksheet(ss, RATES_TITLE, DEFAULT_RATES_COLS)
    rates = _read_df(RATES_TITLE)
    if rates.empty:
        base = pd.DataFrame([["USD", 0.0, now_str()],
                             ["EUR", 0.0, now_str()],
                             ["NOK", 0.0, now_str()],
                             ["CAD", 0.0, now_str()]],
                            columns=DEFAULT_RATES_COLS)
        _write_df(RATES_TITLE, base)
    # Resultat
    ws3 = _ensure_worksheet(ss, RESULT_TITLE, DEFAULT_RESULT_COLS)
    res = _read_df(RESULT_TITLE)
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame(columns=DEFAULT_RESULT_COLS))

_bootstrap_sheets()

# ======================================
# Valutakurser (auto via yfinance)
# ======================================
@st.cache_data(ttl=60*60, show_spinner=False)
def fetch_fx_to_sek() -> Dict[str, float]:
    """
    Hämtar USDSEK=X, EURSEK=X, NOKSEK=X, CADSEK=X via yfinance.
    Uppdaterar även fliken Valutakurser.
    """
    wanted = ["USD", "EUR", "NOK", "CAD", "SEK"]
    pairs = { "USD": "USDSEK=X", "EUR": "EURSEK=X", "NOK": "NOKSEK=X", "CAD": "CADSEK=X" }
    out = {"SEK": 1.0}
    for c, sym in pairs.items():
        try:
            px = yf.Ticker(sym).fast_info.last_price
        except Exception:
            try:
                px = yf.Ticker(sym).history(period="1d")["Close"].iloc[-1]
            except Exception:
                px = np.nan
        out[c] = _coerce_float(px)
    # Skriv tillbaka till Sheets
    df = _read_df(RATES_TITLE)
    if df.empty:
        df = pd.DataFrame(columns=DEFAULT_RATES_COLS)
    for c in wanted:
        if c == "SEK":
            continue
        rate = _coerce_float(out.get(c))
        ts = now_str()
        if c in df["Valuta"].astype(str).tolist():
            df.loc[df["Valuta"] == c, "Mot SEK"] = rate
            df.loc[df["Valuta"] == c, "Senast uppdaterad"] = ts
        else:
            df = pd.concat([df, pd.DataFrame([[c, rate, ts]], columns=DEFAULT_RATES_COLS)], ignore_index=True)
    _write_df(RATES_TITLE, df)
    return out

def fx_to_sek(ccy: str, amount: float) -> float:
    if ccy is None or str(ccy).strip() == "":
        return np.nan
    rates = fetch_fx_to_sek()
    r = rates.get(ccy.upper(), np.nan)
    if r is None or np.isnan(r):
        return np.nan
    return _coerce_float(amount) * _coerce_float(r)

# ======================================
# Buckets & källskatt (dynamisk)
# ======================================
DEFAULT_BUCKETS = [
    "Bucket A tillväxt", "Bucket B tillväxt", "Bucket C tillväxt",
    "Bucket A utdelning", "Bucket B utdelning", "Bucket C utdelning"
]

# Källskatt per valuta (kan ändras i UI och sparas till Rates-fliken)
@st.cache_data(ttl=0, show_spinner=False)
def get_withholding_map() -> Dict[str, float]:
    # standard: USD 15%, NOK 25%, CAD 15%, SEK 0
    return {"USD": 0.15, "NOK": 0.25, "CAD": 0.15, "SEK": 0.00, "EUR": 0.15}

def set_withholding_map(new_map: Dict[str, float]):
    # Lägg in i session_state (räcker för nu; kan senare skrivas till Settings-flik)
    st.session_state["_withholding"] = dict(new_map)

def current_withholding():
    return st.session_state.get("_withholding", get_withholding_map())

# app.py — Del 2/4: Hämtare (Yahoo) + estimat/CAGR-fallback + värderingslogik

# ===========
# Yahoo fetch
# ===========
def _yf_info(tkr: str) -> Dict[str, Any]:
    t = yf.Ticker(tkr)
    info = {}
    # fast_info
    try:
        fi = t.fast_info
        info["last_price"] = _coerce_float(getattr(fi, "last_price", np.nan))
        info["currency"]   = getattr(fi, "currency", None)
        info["shares_out"] = _coerce_float(getattr(fi, "shares", np.nan))
        info["market_cap"] = _coerce_float(getattr(fi, "market_cap", np.nan))
    except Exception:
        pass
    # get_info (robust mot versionsskillnader)
    try:
        gi = t.get_info()
    except Exception:
        try:
            gi = t.info  # legacy
        except Exception:
            gi = {}
    gi = gi or {}
    # Plocka ut vanligt förekommande nycklar
    for k_app, keys in {
        "shortName": ["shortName", "longName", "displayName"],
        "market_cap": ["marketCap"],
        "enterprise_value": ["enterpriseValue"],
        "pe_ttm": ["trailingPE"],
        "pe_fwd": ["forwardPE"],
        "ps_ttm": ["priceToSalesTrailing12Months", "trailingPS"],
        "pb": ["priceToBook"],
        "ev_ebitda": ["enterpriseToEbitda"],
        "ev_sales": ["enterpriseToRevenue"],
        "eps_ttm": ["trailingEps", "trailingEPS"],
        "revenue_ttm": ["totalRevenue"],
        "fcf_ttm": ["freeCashflow"],
        "dividend_rate": ["dividendRate"],
        "dividend_yield": ["dividendYield"],
        "currency": ["currency"],
        "shares_out": ["sharesOutstanding"],
    }.items():
        for key in keys:
            if key in gi and gi[key] is not None:
                info[k_app] = gi[key]
                break

    # Dividends TTM via serie (om finns)
    try:
        div = yf.Ticker(tkr).dividends
        if div is not None and len(div) > 0:
            last_4 = div.iloc[-4:].sum()
            info["dividend_ttm_series"] = float(last_4)
    except Exception:
        pass

    # Sista fallback på namn/valuta/pris
    info["shortName"] = info.get("shortName") or tkr
    info["currency"]  = info.get("currency") or "USD"
    return info


@st.cache_data(ttl=60*15, show_spinner=True)
def fetch_yahoo_basics(tkr: str) -> Dict[str, Any]:
    """Hämtar basdata från Yahoo. Returnerar alltid ett dict med nycklar (kan vara NaN)."""
    info = _yf_info(tkr)
    out = {
        "ticker": tkr,
        "name": info.get("shortName", tkr),
        "currency": info.get("currency", "USD"),
        "price": _coerce_float(info.get("last_price")),
        "market_cap": _coerce_float(info.get("market_cap")),
        "enterprise_value": _coerce_float(info.get("enterprise_value")),
        "pe_ttm": _coerce_float(info.get("pe_ttm")),
        "pe_fwd": _coerce_float(info.get("pe_fwd")),
        "ps_ttm": _coerce_float(info.get("ps_ttm")),
        "pb": _coerce_float(info.get("pb")),
        "ev_ebitda": _coerce_float(info.get("ev_ebitda")),
        "ev_sales": _coerce_float(info.get("ev_sales")),
        "eps_ttm": _coerce_float(info.get("eps_ttm")),
        "revenue_ttm": _coerce_float(info.get("revenue_ttm")),
        "fcf_ttm": _coerce_float(info.get("fcf_ttm")),
        "dividend_ttm": _coerce_float(info.get("dividend_ttm_series") or info.get("dividend_rate")),
        "shares_out": _coerce_float(info.get("shares_out")),
        "source": "Yahoo",
        "fetched_at": now_str(),
    }
    # Härledd nettoskuld (NetDebt) för EV-baserade metoder
    # EV = Mcap + NetDebt  =>  NetDebt = EV - Mcap
    if not np.isnan(out["enterprise_value"]) and not np.isnan(out["market_cap"]):
        out["net_debt"] = out["enterprise_value"] - out["market_cap"]
    else:
        out["net_debt"] = np.nan
    return out


# =======================
# Estimat & historik-CAGR
# =======================
def _calc_cagr_from_series(vals: List[float], years: int) -> float:
    """Standard CAGR på en lista (första->sista) över 'years' år."""
    arr = [v for v in vals if v is not None and not np.isnan(_coerce_float(v))]
    if len(arr) < 2 or years <= 0:
        return np.nan
    a0 = _coerce_float(arr[0])
    a1 = _coerce_float(arr[-1])
    if a0 <= 0 or a1 <= 0:
        return np.nan
    try:
        return (a1 / a0) ** (1.0 / years) - 1.0
    except Exception:
        return np.nan


def _yahoo_earnings_trend(tkr: str) -> Dict[str, Any]:
    """Hämtar EPS- och Revenue-trend om möjligt."""
    out = {"eps_1y": np.nan, "eps_2y": np.nan, "eps_3y": np.nan,
           "rev_1y": np.nan, "rev_2y": np.nan, "rev_3y": np.nan,
           "eps_source": None, "rev_source": None}
    try:
        tt = yf.Ticker(tkr)
        # earnings_trend (DataFrame) – inte alltid tillgänglig
        et = tt.get_earnings_trend()
        if isinstance(et, pd.DataFrame) and not et.empty:
            # Perioder brukar vara t.ex. '0y', '+1y', '+2y'
            # Kolumner: 'epsTrend'->'current', 'growth', etc (kan variera)
            # Vi försöker tolka 'epsTrend'->'current' (per period)
            if "period" in et.columns:
                et = et.copy()
                et["period"] = et["period"].astype(str)
                # Försök hämta EPS current för +1y, +2y, +3y
                def _get_eps_for(per_tag):
                    try:
                        row = et[et["period"].str.contains(per_tag)].iloc[0]
                        # epsTrend kan vara dict-liknande kolumn
                        if "epsTrend" in et.columns and isinstance(row["epsTrend"], dict):
                            return _coerce_float(row["epsTrend"].get("current"))
                    except Exception:
                        return np.nan
                    return np.nan

                out["eps_1y"] = _get_eps_for("+1y")
                out["eps_2y"] = _get_eps_for("+2y")
                out["eps_3y"] = _get_eps_for("+3y")
                if not np.isnan(out["eps_1y"]) or not np.isnan(out["eps_2y"]):
                    out["eps_source"] = "Yahoo (earnings_trend)"

        # Revenue forecast finns sällan direkt; vi lämnar NaN här – täcks av CAGR-fallback
    except Exception:
        pass
    return out


def _yahoo_history_growth(tkr: str) -> Dict[str, Any]:
    """
    Försök räkna historisk CAGR på omsättning och (om möjligt) EPS från Yahoo statements.
    """
    out = {"g_rev": np.nan, "g_eps": np.nan}
    try:
        tt = yf.Ticker(tkr)

        # Omsättning – årlig income_stmt
        rev_hist = []
        try:
            inc = tt.income_stmt  # nyare yfinance
        except Exception:
            try:
                inc = tt.financials  # äldre alias
            except Exception:
                inc = None
        if isinstance(inc, pd.DataFrame) and not inc.empty:
            # För vissa versioner ligger 'Total Revenue' eller 'TotalRevenue'
            for key in ["Total Revenue", "TotalRevenue", "totalRevenue"]:
                if key in inc.index:
                    vals = inc.loc[key].dropna().values.tolist()
                    rev_hist = [float(v) for v in vals if _coerce_float(v) > 0]
                    break
        if len(rev_hist) >= 3:
            years = max(1, len(rev_hist) - 1)
            out["g_rev"] = _calc_cagr_from_series(rev_hist[::-1], years)  # äldst->nyast

        # EPS – ibland "Diluted EPS" i income_stmt eller i 'earnings'
        eps_hist = []
        for key in ["Diluted EPS", "Basic EPS", "EPS Basic", "EPS"]:
            if isinstance(inc, pd.DataFrame) and key in inc.index:
                vals = inc.loc[key].dropna().values.tolist()
                eps_hist = [float(v) for v in vals if _coerce_float(v) > 0]
                break
        if len(eps_hist) >= 3:
            years = max(1, len(eps_hist) - 1)
            out["g_eps"] = _calc_cagr_from_series(eps_hist[::-1], years)

    except Exception:
        pass
    return out


@st.cache_data(ttl=60*30, show_spinner=True)
def fetch_estimates_or_cagr(tkr: str, eps_ttm: float, rev_ttm: float) -> Dict[str, Any]:
    """
    Försöker först hämta EPS-estimat via Yahoo. Faller tillbaka på historisk CAGR
    (om estimat saknas) för både EPS och Revenue.
    """
    res = {
        "eps_1y": np.nan, "eps_2y": np.nan, "eps_3y": np.nan,
        "rev_1y": np.nan, "rev_2y": np.nan, "rev_3y": np.nan,
        "eps_source": None, "rev_source": None,
        "g_eps": np.nan, "g_rev": np.nan
    }

    # 1) Försök estimat
    et = _yahoo_earnings_trend(tkr)
    res.update({k: et[k] for k in ["eps_1y", "eps_2y", "eps_3y", "eps_source"]})

    # 2) CAGR-fallback för EPS/Revenue
    hg = _yahoo_history_growth(tkr)
    res["g_rev"] = hg.get("g_rev")
    res["g_eps"] = hg.get("g_eps")

    # Revenue-prognoser från CAGR om saknas
    if not np.isnan(rev_ttm) and rev_ttm > 0:
        g_rev = res["g_rev"] if not np.isnan(res["g_rev"]) else 0.10  # default 10% om allt saknas
        res["rev_1y"] = rev_ttm * (1 + g_rev)
        res["rev_2y"] = rev_ttm * (1 + g_rev) ** 2
        res["rev_3y"] = rev_ttm * (1 + g_rev) ** 3
        res["rev_source"] = "CAGR" if not np.isnan(res["g_rev"]) else "CAGR(default 10%)"

    # EPS-prognoser från estimat eller CAGR
    if np.isnan(res["eps_1y"]) or np.isnan(res["eps_2y"]):
        if not np.isnan(eps_ttm) and eps_ttm > 0:
            g_eps = res["g_eps"] if not np.isnan(res["g_eps"]) else 0.12  # default 12%
            res["eps_1y"] = eps_ttm * (1 + g_eps)
            res["eps_2y"] = eps_ttm * (1 + g_eps) ** 2
            res["eps_3y"] = eps_ttm * (1 + g_eps) ** 3
            res["eps_source"] = "CAGR" if not np.isnan(res["g_eps"]) else "CAGR(default 12%)"

    return res


# ======================
# Multipel-ankare & drift
# ======================
def anchor_pe(pe_ttm: float, pe_fwd: float, w_hist_fwd: float = 0.50) -> float:
    """
    50/50 mellan TTM och FWD som default. Används som basmultipel "idag".
    """
    a = _coerce_float(pe_ttm)
    b = _coerce_float(pe_fwd)
    if np.isnan(a) and np.isnan(b):
        return np.nan
    if np.isnan(a):
        return b
    if np.isnan(b):
        return a
    w = min(max(w_hist_fwd, 0.0), 1.0)
    return a * w + b * (1.0 - w)


def drift_multiple(m: float, years: int, annual_drift: float = -0.06) -> float:
    """
    Låt multipeln drifta (t.ex. –6%/år). Negativt värde => multipel komprimeras.
    """
    m = _coerce_float(m)
    if np.isnan(m):
        return np.nan
    return m * ((1.0 + annual_drift) ** years)


# ======================
# Riktkurser per metod
# ======================
def _price_from_ev_multiple(ev_mult: float, driver_future: float, net_debt: float, shares: float) -> float:
    """
    Vanlig formel: EV_future = ev_mult * driver_future
    Equity_future = EV_future - NetDebt   (NetDebt antas konstant)
    Price = Equity_future / shares
    """
    ev_mult = _coerce_float(ev_mult)
    driver_future = _coerce_float(driver_future)
    net_debt = _coerce_float(net_debt)
    shares = _coerce_float(shares)
    if any(np.isnan(v) for v in [ev_mult, driver_future, shares]):
        return np.nan
    ev_fut = ev_mult * driver_future
    eq_fut = ev_fut - (0.0 if np.isnan(net_debt) else net_debt)
    return eq_fut / shares if shares > 0 else np.nan


def _eps_path(eps_ttm: float, eps1: float, eps2: float, eps3: float) -> Dict[str, float]:
    return {
        "0y": _coerce_float(eps_ttm),
        "1y": _coerce_float(eps1),
        "2y": _coerce_float(eps2),
        "3y": _coerce_float(eps3),
    }


def _rev_path(rev_ttm: float, r1: float, r2: float, r3: float) -> Dict[str, float]:
    return {
        "0y": _coerce_float(rev_ttm),
        "1y": _coerce_float(r1),
        "2y": _coerce_float(r2),
        "3y": _coerce_float(r3),
    }


def compute_methods_matrix(bas: Dict[str, Any],
                           est: Dict[str, Any],
                           mult_drift: float = -0.06,
                           ebitda_margin_fallback: float = 0.35) -> pd.DataFrame:
    """
    Beräknar riktkurser för flera metoder (Idag, 1y, 2y, 3y).
    • pe_hist_vs_eps (50/50 TTM/FWD + multipel-drift)
    • ev_sales
    • ev_ebitda  (EBITDA ≈ margin * Revenue om EBITDA saknas)
    • ev_dacf    (proxy = ev_ebitda)
    • ev_fcf/p_fcf   (om FCF-data finns)
    • p_b / p_nav / p_tbv (lämnas NaN om data saknas)
    """
    price_now = _coerce_float(bas.get("price"))
    shares = _coerce_float(bas.get("shares_out"))
    net_debt = _coerce_float(bas.get("net_debt"))
    currency = bas.get("currency", "USD")

    epsp = _eps_path(bas.get("eps_ttm"), est.get("eps_1y"), est.get("eps_2y"), est.get("eps_3y"))
    revp = _rev_path(bas.get("revenue_ttm"), est.get("rev_1y"), est.get("rev_2y"), est.get("rev_3y"))

    rows = []

    # ---- 1) P/E-ankare ≈ historik/fwd mix
    pe0 = anchor_pe(bas.get("pe_ttm"), bas.get("pe_fwd"), w_hist_fwd=0.50)
    pe1 = drift_multiple(pe0, 1, mult_drift)
    pe2 = drift_multiple(pe0, 2, mult_drift)
    pe3 = drift_multiple(pe0, 3, mult_drift)

    def _pe_target(pe, eps):
        if np.isnan(pe) or np.isnan(eps):
            return np.nan
        return pe * eps

    rows.append({
        "Metod": "pe_hist_vs_eps",
        "Idag": _pe_target(pe0, epsp["0y"]),
        "1 år": _pe_target(pe1, epsp["1y"]),
        "2 år": _pe_target(pe2, epsp["2y"]),
        "3 år": _pe_target(pe3, epsp["3y"]),
    })

    # ---- 2) EV/Sales
    evs = _coerce_float(bas.get("ev_sales"))
    rows.append({
        "Metod": "ev_sales",
        "Idag": _price_from_ev_multiple(evs, revp["0y"], net_debt, shares),
        "1 år": _price_from_ev_multiple(evs * (1 + mult_drift), revp["1y"], net_debt, shares),
        "2 år": _price_from_ev_multiple(evs * (1 + mult_drift) ** 2, revp["2y"], net_debt, shares),
        "3 år": _price_from_ev_multiple(evs * (1 + mult_drift) ** 3, revp["3y"], net_debt, shares),
    })

    # ---- 3) EV/EBITDA (EBITDA ≈ margin * revenue om ebitda saknas)
    ev_ebitda = _coerce_float(bas.get("ev_ebitda"))
    ebitda_ttm = np.nan
    if not np.isnan(bas.get("revenue_ttm", np.nan)):
        # härled margin om möjligt via EV/EBITDA & EV/Sales -> EBITDA/Revenue ≈ (Revenue * EV/Revenue)/(EV/EBITDA)? Nej.
        # Enklare: anta fallback-marginal; eller, om pe/ps antyder hög lönsamhet – håll fallback.
        pass
    def _ebitda_from_rev(r):  # proxy
        if np.isnan(r):
            return np.nan
        return r * ebitda_margin_fallback

    rows.append({
        "Metod": "ev_ebitda",
        "Idag": _price_from_ev_multiple(ev_ebitda, _ebitda_from_rev(revp["0y"]), net_debt, shares),
        "1 år": _price_from_ev_multiple(ev_ebitda * (1 + mult_drift), _ebitda_from_rev(revp["1y"]), net_debt, shares),
        "2 år": _price_from_ev_multiple(ev_ebitda * (1 + mult_drift) ** 2, _ebitda_from_rev(revp["2y"]), net_debt, shares),
        "3 år": _price_from_ev_multiple(ev_ebitda * (1 + mult_drift) ** 3, _ebitda_from_rev(revp["3y"]), net_debt, shares),
    })

    # ---- 4) EV/DACF (proxy samma som EV/EBITDA tills vidare)
    rows.append({
        "Metod": "ev_dacf",
        "Idag": _price_from_ev_multiple(ev_ebitda, _ebitda_from_rev(revp["0y"]), net_debt, shares),
        "1 år": _price_from_ev_multiple(ev_ebitda * (1 + mult_drift), _ebitda_from_rev(revp["1y"]), net_debt, shares),
        "2 år": _price_from_ev_multiple(ev_ebitda * (1 + mult_drift) ** 2, _ebitda_from_rev(revp["2y"]), net_debt, shares),
        "3 år": _price_from_ev_multiple(ev_ebitda * (1 + mult_drift) ** 3, _ebitda_from_rev(revp["3y"]), net_debt, shares),
    })

    # ---- 5) EV/FCF och P/FCF om data finns
    fcf_ttm = _coerce_float(bas.get("fcf_ttm"))
    if not np.isnan(fcf_ttm) and fcf_ttm > 0 and shares and shares > 0:
        fcf_ps = fcf_ttm / shares
        # P/FCF (idag) ≈ Price / FCF/aktie
        p_fcf_now = price_now / fcf_ps if price_now and fcf_ps and fcf_ps > 0 else np.nan
        # prognoser: anta att FCF växer med g_rev
        g_rev = _coerce_float(est.get("g_rev"))
        if np.isnan(g_rev):
            g_rev = 0.10
        f1 = fcf_ps * (1 + g_rev)
        f2 = fcf_ps * (1 + g_rev) ** 2
        f3 = fcf_ps * (1 + g_rev) ** 3
        rows.append({
            "Metod": "p_fcf",
            "Idag": p_fcf_now * fcf_ps if not np.isnan(p_fcf_now) else np.nan,  # = Price (sanity)
            "1 år": p_fcf_now * (1 + mult_drift) * f1,
            "2 år": p_fcf_now * (1 + mult_drift) ** 2 * f2,
            "3 år": p_fcf_now * (1 + mult_drift) ** 3 * f3,
        })
        # EV/FCF (via EV multiple)
        ev_fcf_now = _coerce_float(bas.get("enterprise_value")) / fcf_ttm if not np.isnan(bas.get("enterprise_value")) and fcf_ttm > 0 else np.nan
        rows.append({
            "Metod": "ev_fcf",
            "Idag": _price_from_ev_multiple(ev_fcf_now, fcf_ttm, net_debt, shares),
            "1 år": _price_from_ev_multiple(ev_fcf_now * (1 + mult_drift), fcf_ttm * (1 + g_rev), net_debt, shares),
            "2 år": _price_from_ev_multiple(ev_fcf_now * (1 + mult_drift) ** 2, fcf_ttm * (1 + g_rev) ** 2, net_debt, shares),
            "3 år": _price_from_ev_multiple(ev_fcf_now * (1 + mult_drift) ** 3, fcf_ttm * (1 + g_rev) ** 3, net_debt, shares),
        })
    else:
        rows.append({"Metod": "p_fcf", "Idag": np.nan, "1 år": np.nan, "2 år": np.nan, "3 år": np.nan})
        rows.append({"Metod": "ev_fcf", "Idag": np.nan, "1 år": np.nan, "2 år": np.nan, "3 år": np.nan})

    # ---- 6) P/B, P/NAV, P/TBV (placeholder – kräver per-aktie-tal vi ej alltid har)
    for m in ["p_b", "p_nav", "p_tbv", "p_nii", "p_affo"]:
        rows.append({"Metod": m, "Idag": np.nan, "1 år": np.nan, "2 år": np.nan, "3 år": np.nan})

    df = pd.DataFrame(rows, columns=["Metod", "Idag", "1 år", "2 år", "3 år"])
    # Snygg avrundning
    for c in ["Idag", "1 år", "2 år", "3 år"]:
        df[c] = df[c].astype(float)
    return df


# ==========================
# Utdelning – prognoser (SEK)
# ==========================
def forecast_dividends_net_sek(bas: Dict[str, Any],
                               years: int,
                               withholding_map: Dict[str, float],
                               user_shares: float,
                               div_cagr: Optional[float] = None) -> List[float]:
    """
    Returnerar lista med framtida netto-utdelningar (SEK) för 1..years
    baserat på TTM dividend och (om finns) utdelnings-CAGR.
    """
    d_ttm = _coerce_float(bas.get("dividend_ttm"))
    ccy = bas.get("currency", "USD")
    w_map = withholding_map or get_withholding_map()
    tax = w_map.get(ccy.upper(), 0.15)

    if np.isnan(d_ttm) or d_ttm <= 0 or user_shares is None or user_shares <= 0:
        return [0.0 for _ in range(years)]

    g = 0.0 if div_cagr is None or np.isnan(div_cagr) else float(div_cagr)
    out = []
    for i in range(1, years + 1):
        gross = d_ttm * ((1 + g) ** i) * user_shares
        net_ccy = gross * (1.0 - tax)
        out.append(fx_to_sek(ccy, net_ccy))
    return out


# ==========================
# Hjälp: välj primär metod
# ==========================
def pick_primary_method(bucket: str, methods_df: pd.DataFrame) -> Tuple[str, float, float, float, float]:
    """
    Enkel heuristik:
      • Tillväxt-bucket: ev_ebitda om finns, annars ev_sales
      • Utdelnings-bucket: pe_hist_vs_eps (brukar vara mest stabilt) – annars ev_ebitda
    Returnerar (metod, idag, 1y, 2y, 3y)
    """
    def _get_row(name):
        rr = methods_df[methods_df["Metod"] == name]
        return rr.iloc[0] if not rr.empty else None

    b = (bucket or "").lower()
    order = []
    if "utdelning" in b:
        order = ["pe_hist_vs_eps", "ev_ebitda", "ev_sales"]
    else:
        order = ["ev_ebitda", "ev_sales", "pe_hist_vs_eps"]

    for m in order:
        r = _get_row(m)
        if r is not None and (not np.isnan(r["Idag"]) or not np.isnan(r["1 år"])):
            return m, r["Idag"], r["1 år"], r["2 år"], r["3 år"]

    # Fallback: första raden
    r0 = methods_df.iloc[0]
    return r0["Metod"], r0["Idag"], r0["1 år"], r0["2 år"], r0["3 år"]


# ==========================
# Skriv tillbaka till Data
# ==========================
def upsert_data_row_from_fetch(tkr: str, bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    Hämtar Yahoo-basics + estimat/CAGR och skriver in/uppdaterar i fliken Data.
    Returnerar den uppdaterade raden (dict).
    """
    bas = fetch_yahoo_basics(tkr)
    est = fetch_estimates_or_cagr(tkr, bas.get("eps_ttm"), bas.get("revenue_ttm"))

    df = _read_df(DATA_TITLE)
    if df.empty:
        df = pd.DataFrame(columns=DEFAULT_DATA_COLS)

    # Finns raden?
    mask = (df["Ticker"].astype(str).str.upper() == tkr.upper())
    exists = mask.any()
    row = {
        "Ticker": tkr,
        "Bolagsnamn": bas["name"],
        "Bucket": bucket or (df.loc[mask, "Bucket"].iloc[0] if exists else ""),
        "Valuta": bas["currency"],
        "Aktuell kurs": bas["price"],
        "Market Cap": bas["market_cap"],
        "EV": bas["enterprise_value"],
        "P/E TTM": bas["pe_ttm"],
        "P/E FWD": bas["pe_fwd"],
        "P/S TTM": bas["ps_ttm"],
        "P/B": bas["pb"],
        "EV/EBITDA": bas["ev_ebitda"],
        "EV/Sales": bas["ev_sales"],
        "EPS TTM": bas["eps_ttm"],
        "Omsättning TTM": bas["revenue_ttm"],
        "EPS 1y": est["eps_1y"],
        "EPS 2y": est["eps_2y"],
        "EPS 3y": est["eps_3y"],
        "REV 1y": est["rev_1y"],
        "REV 2y": est["rev_2y"],
        "REV 3y": est["rev_3y"],
        "EPS-källa": est["eps_source"],
        "REV-källa": est["rev_source"],
        "g_eps": est["g_eps"],
        "g_rev": est["g_rev"],
        "Utdelning TTM": bas["dividend_ttm"],
        "Senast auto uppdaterad": now_str(),
        "Auto källa": "Yahoo",
    }

    if exists:
        for k, v in row.items():
            df.loc[mask, k] = v
    else:
        # säkerställ kolumner
        for k in DEFAULT_DATA_COLS:
            if k not in df.columns:
                df[k] = np.nan
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    _write_df(DATA_TITLE, df)
    return row

# app.py — Del 3/4: UI (analys + riktkurser + utdelning + spara till Resultat)

# =========================
# Små UI-hjälpare (format)
# =========================
def _fmt_money(x: Any, ccy: str) -> str:
    v = _coerce_float(x)
    if np.isnan(v):
        return "–"
    try:
        return f"{v:,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v:.2f} {ccy}"

def _fmt_pct(x: Any) -> str:
    v = _coerce_float(x)
    if np.isnan(v):
        return "–"
    return f"{v*100:+.1f}%"

def _upside(now_price: float, target: float) -> float:
    now_price = _coerce_float(now_price)
    target = _coerce_float(target)
    if np.isnan(now_price) or np.isnan(target) or now_price <= 0:
        return np.nan
    return (target/now_price) - 1.0


# =================================
# Skriv “primär riktkurs” till blad
# =================================
def _ensure_result_cols(df: pd.DataFrame) -> pd.DataFrame:
    need = [
        "Timestamp","Ticker","Namn","Bucket","Valuta","Metod",
        "Target idag","Target 1 år","Target 2 år","Target 3 år",
        "Upp/Ned idag (%)","Källa"
    ]
    for c in need:
        if c not in df.columns:
            df[c] = np.nan
    return df[need]

def save_primary_targets_to_result(row_in_data: Dict[str, Any],
                                   method_name: str,
                                   t0: float, t1: float, t2: float, t3: float,
                                   price_now: float,
                                   source_note: str = "") -> None:
    res_df = _read_df(RESULT_TITLE)
    if res_df is None or res_df.empty:
        res_df = pd.DataFrame()
    res_df = _ensure_result_cols(res_df)

    up = _upside(price_now, t0)
    newr = {
        "Timestamp": now_str(),
        "Ticker": row_in_data.get("Ticker"),
        "Namn": row_in_data.get("Bolagsnamn") or row_in_data.get("name") or row_in_data.get("shortName"),
        "Bucket": row_in_data.get("Bucket", ""),
        "Valuta": row_in_data.get("Valuta", row_in_data.get("currency","USD")),
        "Metod": method_name,
        "Target idag": t0,
        "Target 1 år": t1,
        "Target 2 år": t2,
        "Target 3 år": t3,
        "Upp/Ned idag (%)": up*100 if not np.isnan(up) else np.nan,
        "Källa": source_note or "Yahoo/Model"
    }
    res_df = pd.concat([res_df, pd.DataFrame([newr])], ignore_index=True)
    _write_df(RESULT_TITLE, res_df)


# ======================
# Huvud-analysflödet (UI)
# ======================
def _load_current_row(df: pd.DataFrame, ticker: str) -> Dict[str, Any]:
    if df.empty:
        return {}
    mask = (df["Ticker"].astype(str).str.upper() == (ticker or "").upper())
    if not mask.any():
        return {}
    d = df.loc[mask].iloc[0].to_dict()
    return d

def run_analysis_ui():
    st.header("🔍 Analys & riktkurser")

    # -------- Dataframe från blad
    data_df = _read_df(DATA_TITLE)
    if data_df is None or data_df.empty:
        st.info("Inga bolag i fliken **Data** än. Lägg till ett ticker-rad först.")
        return

    # Val av ticker
    tickers = data_df["Ticker"].dropna().astype(str).unique().tolist()
    ticker = st.selectbox("Välj ticker", options=sorted(tickers))
    row = _load_current_row(data_df, ticker)

    # Visa snabbinfo från Data
    colA, colB, colC, colD = st.columns(4)
    colA.metric("Namn", row.get("Bolagsnamn", "—"))
    colB.metric("Bucket", row.get("Bucket", "—"))
    ccy = row.get("Valuta", "USD") or "USD"
    colC.metric("Valuta", ccy)
    colD.metric("Pris", _fmt_money(row.get("Aktuell kurs"), ccy))

    # -------- Parametrar för modell
    with st.expander("⚙️ Modellinställningar", expanded=False):
        w_hist_fwd = st.slider("P/E-ankare: vikt historisk (TTM) vs framåtblickande (FWD)",
                               min_value=0.0, max_value=1.0, value=0.50, step=0.05)
        drift = st.slider("Multipel-drift/år (negativ = multipel sjunker)",
                          min_value=-0.20, max_value=0.00, value=-0.06, step=0.01)
        ebitda_margin = st.slider("Fallback EBITDA-marginal (om saknas)", 0.05, 0.60, 0.35, 0.01)

    # -------- Uppdateringsknappar
    c1, c2 = st.columns([1,1])
    if c1.button("🔄 Uppdatera vald ticker (Yahoo)"):
        upsert = upsert_data_row_from_fetch(ticker, bucket=row.get("Bucket"))
        st.success(f"Uppdaterade {ticker} från Yahoo.")
        data_df = _read_df(DATA_TITLE)  # läs om
        row = _load_current_row(data_df, ticker)

    # Bygg bas/est för beräkningar (från Data + komplettera via Yahoo-helpers om saknas)
    bas = {
        "price": _coerce_float(row.get("Aktuell kurs")),
        "currency": ccy,
        "market_cap": _coerce_float(row.get("Market Cap")),
        "enterprise_value": _coerce_float(row.get("EV")),
        "pe_ttm": _coerce_float(row.get("P/E TTM")),
        "pe_fwd": _coerce_float(row.get("P/E FWD")),
        "ps_ttm": _coerce_float(row.get("P/S TTM")),
        "pb": _coerce_float(row.get("P/B")),
        "ev_ebitda": _coerce_float(row.get("EV/EBITDA")),
        "ev_sales": _coerce_float(row.get("EV/Sales")),
        "eps_ttm": _coerce_float(row.get("EPS TTM")),
        "revenue_ttm": _coerce_float(row.get("Omsättning TTM")),
        "dividend_ttm": _coerce_float(row.get("Utdelning TTM")),
        "shares_out": _coerce_float(row.get("Utestående aktier", row.get("shares_out"))),
    }
    # härledd nettoskuld
    if not np.isnan(bas["enterprise_value"]) and not np.isnan(bas["market_cap"]):
        bas["net_debt"] = bas["enterprise_value"] - bas["market_cap"]
    else:
        bas["net_debt"] = np.nan

    # Estimat/CAGR (från Data om finns, annars hämta)
    est = {
        "eps_1y": _coerce_float(row.get("EPS 1y")),
        "eps_2y": _coerce_float(row.get("EPS 2y")),
        "eps_3y": _coerce_float(row.get("EPS 3y")),
        "rev_1y": _coerce_float(row.get("REV 1y")),
        "rev_2y": _coerce_float(row.get("REV 2y")),
        "rev_3y": _coerce_float(row.get("REV 3y")),
        "eps_source": row.get("EPS-källa"),
        "rev_source": row.get("REV-källa"),
        "g_eps": _coerce_float(row.get("g_eps")),
        "g_rev": _coerce_float(row.get("g_rev")),
    }

    # Om viktiga fält saknas – fyll via fetchers (cacheade)
    if any(np.isnan(v) for v in [bas["price"], bas["pe_ttm"], bas["pe_fwd"], bas["revenue_ttm"]]):
        y = fetch_yahoo_basics(ticker)
        for k in ["price","pe_ttm","pe_fwd","revenue_ttm","ev_ebitda","ev_sales","pb","ps_ttm",
                  "market_cap","enterprise_value","dividend_ttm","shares_out","currency"]:
            if np.isnan(_coerce_float(bas.get(k))) and (y.get(k) is not None):
                bas[k] = y.get(k)
        if np.isnan(bas["net_debt"]) and not np.isnan(bas["enterprise_value"]) and not np.isnan(bas["market_cap"]):
            bas["net_debt"] = bas["enterprise_value"] - bas["market_cap"]

    if any(np.isnan(v) for v in [est["eps_1y"], est["eps_2y"]]):
        est2 = fetch_estimates_or_cagr(ticker, bas.get("eps_ttm"), bas.get("revenue_ttm"))
        for k,v in est2.items():
            if k in est and (est[k] is None or np.isnan(_coerce_float(est[k]))):
                est[k] = v

    # ========== Beräkna metod-matris ==========
    # Använd användarens vikt för PE-ankare via liten wrapper
    def _local_anchor(pe_ttm, pe_fwd):
        return anchor_pe(pe_ttm, pe_fwd, w_hist_fwd=w_hist_fwd)

    # (compute_methods_matrix använder global anchor_pe; vi “ställer in” vikt genom att
    # beräkna pe0 innan och ersätta i bas)
    pe0 = _local_anchor(bas.get("pe_ttm"), bas.get("pe_fwd"))
    bas_for_calc = dict(bas)
    # ersätt TTM/FWD med vårt bland-ankare för tydlighet i logik nedan
    bas_for_calc["pe_ttm"] = pe0
    bas_for_calc["pe_fwd"] = pe0

    methods_df = compute_methods_matrix(bas_for_calc, est,
                                        mult_drift=drift,
                                        ebitda_margin_fallback=ebitda_margin)

    # Sanity-rad
    sanity = []
    sanity.append("price ok" if not np.isnan(bas["price"]) else "price —")
    sanity.append("eps_ttm ok" if not np.isnan(bas["eps_ttm"]) else "eps_ttm —")
    sanity.append("rev_ttm ok" if not np.isnan(bas["revenue_ttm"]) else "rev_ttm —")
    sanity.append("ebitda mult ok" if not np.isnan(bas["ev_ebitda"]) else "ev_ebitda —")
    sanity.append("shares ok" if not np.isnan(bas["shares_out"]) else "shares —")
    st.caption("Sanity: " + ", ".join(sanity))

    st.subheader("🧭 Detaljer per bolag (alla metoder)")
    st.dataframe(methods_df, use_container_width=True)

    # Välj primär metod enligt heuristik/bucket
    prim_method, t0, t1, t2, t3 = pick_primary_method(row.get("Bucket",""), methods_df)

    # ========== Primär riktkurs ==========
    st.subheader("🎯 Primär riktkurs")
    price_now = bas["price"]
    cols = st.columns(4)
    cols[0].metric("Idag", _fmt_money(t0, ccy), _fmt_pct(_upside(price_now, t0)))
    cols[1].metric("1 år", _fmt_money(t1, ccy))
    cols[2].metric("2 år", _fmt_money(t2, ccy))
    cols[3].metric("3 år", _fmt_money(t3, ccy))
    st.caption(f"Metod: **{prim_method}** • Valuta: **{ccy}**")

    # ========== Utdelning & innehavsvärde ==========
    st.subheader("💰 Utdelning (netto, SEK)")
    wmap = get_withholding_map()
    shares_user = _coerce_float(row.get("Antal aktier", 0))
    div_cagr = _coerce_float(row.get("Utdelning CAGR", np.nan))
    divs = forecast_dividends_net_sek(bas, years=3, withholding_map=wmap,
                                      user_shares=shares_user, div_cagr=div_cagr)
    d1, d2, d3 = divs if len(divs) == 3 else (0,0,0)
    st.write(f"• Nästa år: **{_fmt_money(d1, 'SEK')}** • 2 år: **{_fmt_money(d2, 'SEK')}** • 3 år: **{_fmt_money(d3, 'SEK')}**")
    st.caption(f"Källskatt: {int((wmap.get(ccy.upper(),0.15))*100)}% • Antal aktier: {int(shares_user or 0)}")

    st.subheader("🧾 Innehavsvärde")
    position_value_sek = 0.0
    if shares_user and shares_user > 0 and not np.isnan(price_now):
        position_value_sek = fx_to_sek(ccy, price_now * shares_user)
    st.write(f"Totalt värde nu: **{_fmt_money(position_value_sek, 'SEK')}**")

    # ========== Spara ==========
    if st.button("💾 Spara primära riktkurser till Resultat"):
        try:
            save_primary_targets_to_result(
                row_in_data=row,
                method_name=prim_method,
                t0=t0, t1=t1, t2=t2, t3=t3,
                price_now=price_now,
                source_note=f"pe_w={w_hist_fwd:.2f}, drift={drift:.2%}, ebitda_fallback={ebitda_margin:.0%}"
            )
            st.success("Sparat till fliken **Resultat**.")
        except Exception as e:
            st.error(f"Kunde inte spara: {e}")

# app.py — Del 4/4: Navigering, Ranking, Settings, Batch, main()

import time

# ==========================
# UI: Ranking & portföljsammanställning
# ==========================
def run_rank_ui():
    st.header("🏁 Rangordning (uppsida →)")

    data_df = _read_df(DATA_TITLE)
    if data_df is None or data_df.empty:
        st.info("Inga bolag i fliken **Data** än.")
        return

    buckets = ["(alla)"] + sorted(data_df["Bucket"].dropna().astype(str).unique().tolist())
    bucket_pick = st.selectbox("Filter: Bucket", options=buckets, index=0)
    only_owned = st.checkbox("Visa endast bolag jag äger (Antal aktier > 0)", value=False)
    max_rows = st.number_input("Max antal beräknade rader", 1, 5000, min(200, len(data_df)))

    with st.expander("⚙️ Modellinställningar (ranking)", expanded=False):
        w_hist_fwd = st.slider("P/E-ankare vikt (hist vs fwd)", 0.0, 1.0, 0.50, 0.05, key="rk_pew")
        drift = st.slider("Multipel-drift per år", -0.20, 0.00, -0.06, 0.01, key="rk_drift")
        ebitda_margin = st.slider("Fallback EBITDA-marginal", 0.05, 0.60, 0.35, 0.01, key="rk_ebm")
        throttle = st.slider("Fördröjning mellan hämtningar (sek)", 0.0, 2.0, 0.4, 0.1, key="rk_thr")

    # Filtrera
    df = data_df.copy()
    if bucket_pick != "(alla)":
        df = df[df["Bucket"].astype(str) == bucket_pick]
    if only_owned:
        df = df[_coerce_float_series(df.get("Antal aktier")) > 0]

    df = df.head(int(max_rows)).reset_index(drop=True)
    if df.empty:
        st.info("Inget matchade filtret.")
        return

    btn = st.button("▶️ Beräkna ranking nu")
    if not btn:
        st.caption("Tryck på **Beräkna** för att räkna uppsida och rangordna.")
        return

    progress = st.progress(0, text="Startar…")
    out_rows = []
    for i, (_, r) in enumerate(df.iterrows(), start=1):
        ticker = str(r.get("Ticker"))
        ccy = r.get("Valuta", "USD") or "USD"

        # Bas/est
        bas = {
            "price": _coerce_float(r.get("Aktuell kurs")),
            "currency": ccy,
            "market_cap": _coerce_float(r.get("Market Cap")),
            "enterprise_value": _coerce_float(r.get("EV")),
            "pe_ttm": _coerce_float(r.get("P/E TTM")),
            "pe_fwd": _coerce_float(r.get("P/E FWD")),
            "ps_ttm": _coerce_float(r.get("P/S TTM")),
            "pb": _coerce_float(r.get("P/B")),
            "ev_ebitda": _coerce_float(r.get("EV/EBITDA")),
            "ev_sales": _coerce_float(r.get("EV/Sales")),
            "eps_ttm": _coerce_float(r.get("EPS TTM")),
            "revenue_ttm": _coerce_float(r.get("Omsättning TTM")),
            "dividend_ttm": _coerce_float(r.get("Utdelning TTM")),
            "shares_out": _coerce_float(r.get("Utestående aktier", r.get("shares_out"))),
        }
        if not np.isnan(bas["enterprise_value"]) and not np.isnan(bas["market_cap"]):
            bas["net_debt"] = bas["enterprise_value"] - bas["market_cap"]
        else:
            bas["net_debt"] = np.nan

        est = {
            "eps_1y": _coerce_float(r.get("EPS 1y")),
            "eps_2y": _coerce_float(r.get("EPS 2y")),
            "eps_3y": _coerce_float(r.get("EPS 3y")),
            "rev_1y": _coerce_float(r.get("REV 1y")),
            "rev_2y": _coerce_float(r.get("REV 2y")),
            "rev_3y": _coerce_float(r.get("REV 3y")),
            "g_eps": _coerce_float(r.get("g_eps")),
            "g_rev": _coerce_float(r.get("g_rev")),
        }

        # Komplettera via fetchers vid behov
        if any(np.isnan(v) for v in [bas["price"], bas["pe_ttm"], bas["pe_fwd"], bas["revenue_ttm"]]):
            y = fetch_yahoo_basics(ticker)
            for k in ["price","pe_ttm","pe_fwd","revenue_ttm","ev_ebitda","ev_sales","pb","ps_ttm",
                      "market_cap","enterprise_value","dividend_ttm","shares_out","currency"]:
                if np.isnan(_coerce_float(bas.get(k))) and (y.get(k) is not None):
                    bas[k] = y.get(k)
            if np.isnan(bas["net_debt"]) and not np.isnan(bas["enterprise_value"]) and not np.isnan(bas["market_cap"]):
                bas["net_debt"] = bas["enterprise_value"] - bas["market_cap"]

        if any(np.isnan(v) for v in [est["eps_1y"], est["eps_2y"]]):
            est2 = fetch_estimates_or_cagr(ticker, bas.get("eps_ttm"), bas.get("revenue_ttm"))
            for k, v in est2.items():
                if k in est and (est[k] is None or np.isnan(_coerce_float(est[k]))):
                    est[k] = v

        # Ankar-PE
        pe0 = anchor_pe(bas.get("pe_ttm"), bas.get("pe_fwd"), w_hist_fwd=w_hist_fwd)
        bas_calc = dict(bas)
        bas_calc["pe_ttm"] = pe0
        bas_calc["pe_fwd"] = pe0

        mdf = compute_methods_matrix(bas_calc, est,
                                     mult_drift=drift,
                                     ebitda_margin_fallback=ebitda_margin)
        prim, t0, t1, t2, t3 = pick_primary_method(r.get("Bucket",""), mdf)
        up = _upside(bas["price"], t0)

        out_rows.append({
            "Ticker": ticker,
            "Namn": r.get("Bolagsnamn"),
            "Bucket": r.get("Bucket"),
            "Valuta": ccy,
            "Pris": bas["price"],
            "Primär metod": prim,
            "Idag": t0, "1 år": t1, "2 år": t2, "3 år": t3,
            "Upp/Ned (%)": up*100 if not np.isnan(up) else np.nan,
        })

        progress.progress(i/len(df), text=f"Beräknar {ticker} ({i}/{len(df)})")
        if throttle and throttle > 0:
            time.sleep(throttle)

    progress.empty()
    out = pd.DataFrame(out_rows)
    out = out.sort_values(by="Upp/Ned (%)", ascending=False, na_position="last").reset_index(drop=True)
    st.dataframe(out, use_container_width=True)


def run_settings_ui():
    st.header("⚙️ Inställningar")

    # Källskatt per valuta
    st.subheader("Källskatt per valuta (dynamisk lista)")
    curr_map = get_withholding_map()
    edit_df = pd.DataFrame(
        [{"Valuta": k, "Källskatt (%)": int(v*100)} for k, v in sorted(curr_map.items())]
    )
    edit_df = st.data_editor(edit_df, num_rows="dynamic",
                             use_container_width=True,
                             key="withholding_editor")
    if st.button("💾 Spara källskatter"):
        # validera & skriv
        new_map = {}
        for _, r in edit_df.iterrows():
            code = str(r.get("Valuta","")).strip().upper()
            if not code:
                continue
            try:
                pct = float(r.get("Källskatt (%)"))
            except Exception:
                pct = 15.0
            new_map[code] = max(0.0, min(100.0, pct)) / 100.0
        write_withholding_map(new_map)
        st.success("Källskatter sparade.")

    st.subheader("Valutakurser")
    fx_df = _read_df(RATES_TITLE)
    if fx_df is not None and not fx_df.empty:
        st.dataframe(fx_df, use_container_width=True)
    if st.button("🔄 Uppdatera valutakurser"):
        try:
            refreshed = refresh_fx_sheet()
            if refreshed:
                st.success("Valutakurser uppdaterade.")
            else:
                st.warning("Kunde inte uppdatera valutakurser.")
        except Exception as e:
            st.error(f"Fel vid FX-uppdatering: {e}")


def run_batch_update_ui():
    st.header("🧰 Batch-uppdatera alla tickers (Yahoo)")

    df = _read_df(DATA_TITLE)
    if df is None or df.empty:
        st.info("Inga bolag i **Data**.")
        return

    throttle = st.slider("Fördröjning mellan bolag (sek)", 0.0, 3.0, 1.0, 0.1)
    run_btn = st.button("🚀 Kör batch-uppdatering")

    if not run_btn:
        return

    tickers = df["Ticker"].dropna().astype(str).tolist()
    p = st.progress(0, text="Startar…")
    log = st.empty()

    ok, fail = 0, 0
    for i, t in enumerate(tickers, start=1):
        try:
            upsert_data_row_from_fetch(t, bucket=None)
            ok += 1
            log.info(f"✅ {t} uppdaterad ({i}/{len(tickers)})")
        except Exception as e:
            fail += 1
            log.warning(f"⚠️ {t}: {e}")
        p.progress(i/len(tickers), text=f"Uppdaterar {t} ({i}/{len(tickers)})")
        if throttle and throttle > 0:
            time.sleep(throttle)

    p.empty()
    st.success(f"Klar. ✅ {ok} lyckades • ⚠️ {fail} misslyckades.")


# ================
# Huvudrouter (UI)
# ================
def run_main_ui():
    st.sidebar.title("📌 Meny")
    view = st.sidebar.radio(
        "Välj vy",
        ["Analys", "Ranking", "Inställningar", "Batch-uppdatera"],
        index=0
    )

    if view == "Analys":
        run_analysis_ui()
    elif view == "Ranking":
        run_rank_ui()
    elif view == "Inställningar":
        run_settings_ui()
    elif view == "Batch-uppdatera":
        run_batch_update_ui()


# =====
# main
# =====
def main():
    st.set_page_config(page_title="Aktieanalys & riktkurser", layout="wide")
    st.title("📈 Aktieanalys & riktkurser")

    # Säkra Google-anslutning och blad tidigt, men kapsla med guard (vänlig felvisning)
    global GC, SPREADSHEET_HANDLE, DATA_WS, RATES_WS, SETTINGS_WS, RESULT_WS
    GC = guard(lambda: build_gspread_client_from_secrets(), label="(Google-auth)")
    SPREADSHEET_HANDLE = guard(lambda: open_spreadsheet(GC), label="(Öppna Google Sheet)")
    DATA_WS = guard(lambda: open_or_create_ws(SPREADSHEET_HANDLE, DATA_TITLE), label="(Öppna Data)")
    RATES_WS = guard(lambda: open_or_create_ws(SPREADSHEET_HANDLE, RATES_TITLE), label="(Öppna Valutakurser)")
    SETTINGS_WS = guard(lambda: open_or_create_ws(SPREADSHEET_HANDLE, SETTINGS_TITLE), label="(Öppna Settings)")
    RESULT_WS = guard(lambda: open_or_create_ws(SPREADSHEET_HANDLE, RESULT_TITLE), label="(Öppna Resultat)")

    # Kör huvudvyn
    run_main_ui()


if __name__ == "__main__":
    main()
