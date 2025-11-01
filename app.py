# app.py — Del 1/5
# Grund, Google Sheets-IO, format-helpers, valutakurser, källskatt, bootstrap

from __future__ import annotations

# =========
# Importer
# =========
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials

# För DataFrame <-> Sheets (med fallback om lib saknas)
try:
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
except Exception:
    set_with_dataframe = None
    get_as_dataframe = None

# =========
# Sidan
# =========
st.set_page_config(page_title="Aktieanalys & riktkurser", layout="wide")

# ==================
# Format-hjälpare
# ==================
def _fmt_num(x, nd=2):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "–"
        return f"{float(x):,.{nd}f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(x)

def _fmt_money(x: Any, ccy: str = "USD", nd: int = 2) -> str:
    v = _coerce_float(x)
    if np.isnan(v):
        return "–"
    try:
        return f"{v:,.{nd}f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v:.{nd}f} {ccy}"

def _fmt_pct(x: Any, nd: int = 1) -> str:
    v = _coerce_float(x)
    if np.isnan(v):
        return "–"
    return f"{v*100:.{nd}f}%".replace(".", ",")

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

def _coerce_float_series(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(
        s.astype(str).str.replace(" ", "").str.replace(",", ".", regex=False),
        errors="coerce"
    )

def now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# ==============================
# Google-auth & Spreadsheet
# ==============================
def _normalize_private_key(creds_dict: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds_dict.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds_dict["private_key"] = pk.replace("\\n", "\n")
    return creds_dict

@st.cache_resource(show_spinner=False)
def _build_gspread_client() -> gspread.Client:
    if "GOOGLE_CREDENTIALS" not in st.secrets:
        st.error("Saknar GOOGLE_CREDENTIALS i secrets.")
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
    OBS: _gc med underscore => undviker Streamlits hash/pickle-fel.
    Öppnar via URL om satt, annars via ID.
    """
    sheet_url = str(st.secrets.get("SHEET_URL", "")).strip()
    sheet_id = str(st.secrets.get("SHEET_ID", "")).strip()
    if sheet_url:
        return _gc.open_by_url(sheet_url)
    if sheet_id:
        return _gc.open_by_key(sheet_id)
    st.error("SHEET_URL eller SHEET_ID saknas i secrets.")
    st.stop()

def _ensure_worksheet(ss: gspread.Spreadsheet, title: str, cols: Optional[List[str]] = None) -> gspread.Worksheet:
    try:
        ws = ss.worksheet(title)
        return ws
    except Exception:
        ws = ss.add_worksheet(title=title, rows=1000, cols=60)
        if cols:
            ws.update("A1", [cols])
        return ws

# ==================================
# DataFrame <-> Sheets helpers
# ==================================
@st.cache_data(ttl=60, show_spinner=False)
def _read_df(title: str) -> pd.DataFrame:
    """Cachead läsning baserat på titel (inte Worksheet-objekt => hashbar)."""
    gc = _build_gspread_client()
    ss = _open_spreadsheet(gc)
    ws = _ensure_worksheet(ss, title)
    if get_as_dataframe is None:
        # Fallback utan gspread_dataframe
        rows = ws.get_all_records()  # OBS: har EJ empty2nan
        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame()
        df = df.replace({"": np.nan})
        df.columns = [str(c).strip() for c in df.columns]
        df = df.dropna(how="all")
        return df

    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    if df is None:
        return pd.DataFrame()
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _write_df(title: str, df: pd.DataFrame):
    gc = _build_gspread_client()
    ss = _open_spreadsheet(gc)
    ws = _ensure_worksheet(ss, title)
    if df is None:
        return
    df_out = df.copy()
    if set_with_dataframe is None:
        ws.clear()
        ws.update("A1", [list(df_out.columns)])
        if not df_out.empty:
            ws.update("A2", df_out.astype(str).values.tolist())
        return
    ws.clear()
    set_with_dataframe(ws, df_out, include_index=False, include_column_header=True, resize=True)

# =========================
# Flikar & standardkolumner
# =========================
DATA_TITLE    = "Data"          # Tickers mm
RATES_TITLE   = "Valutakurser"  # FX-cache
RESULT_TITLE  = "Resultat"      # Sparade riktkurser
SETTINGS_TITLE = "Settings"     # (för framtida config om vi vill)

DEFAULT_DATA_COLS = [
    "Ticker", "Bolagsnamn", "Bucket", "Valuta",
    "Antal aktier", "GAV (SEK)",
    "Utestående aktier",
    # Basdata (auto)
    "Aktuell kurs", "Market Cap", "EV",
    "P/E TTM", "P/E FWD", "P/S TTM", "P/B", "EV/EBITDA", "EV/Sales",
    "EPS TTM", "Omsättning TTM",
    # Estimat/CAGR
    "EPS 1y", "EPS 2y", "EPS 3y",
    "REV 1y", "REV 2y", "REV 3y",
    "EPS-källa", "REV-källa", "g_eps", "g_rev",
    # Utdelning
    "Utdelning TTM", "Utdelning CAGR 5y (%)",
    # Stämplar
    "Senast auto uppdaterad", "Auto källa"
]

DEFAULT_RATES_COLS = ["Valuta", "Mot SEK", "Senast uppdaterad"]

# OBS: Anpassad så den matchar save-funktionen
DEFAULT_RESULT_COLS = [
    "Timestamp","Ticker","Namn","Bucket","Valuta","Metod",
    "Pris nu","Target idag","Target 1 år","Target 2 år","Target 3 år",
    "Upp/Ned idag (%)","Källa"
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
        cols = list(df.columns)
        for c in DEFAULT_DATA_COLS:
            if c not in cols:
                cols.append(c)
        df = df.reindex(columns=cols)
        _write_df(DATA_TITLE, df)
    # FX
    _ensure_worksheet(ss, RATES_TITLE, DEFAULT_RATES_COLS)
    rates = _read_df(RATES_TITLE)
    if rates.empty:
        base = pd.DataFrame(
            [["USD", 0.0, now_str()],
             ["EUR", 0.0, now_str()],
             ["NOK", 0.0, now_str()],
             ["CAD", 0.0, now_str()],
             ["SEK", 1.0, now_str()]],
            columns=DEFAULT_RATES_COLS
        )
        _write_df(RATES_TITLE, base)
    # Resultat
    _ensure_worksheet(ss, RESULT_TITLE, DEFAULT_RESULT_COLS)
    res = _read_df(RESULT_TITLE)
    if res.empty:
        _write_df(RESULT_TITLE, pd.DataFrame(columns=DEFAULT_RESULT_COLS))
    # Settings (tom)
    _ensure_worksheet(ss, SETTINGS_TITLE, ["Nyckel","Värde"])

_bootstrap_sheets()

# ===================
# Valutakurser (FX)
# ===================
@st.cache_data(ttl=60*60, show_spinner=False)
def fetch_fx_to_sek() -> Dict[str, float]:
    """
    Hämtar USD/EUR/NOK/CAD → SEK via yfinance och uppdaterar fliken Valutakurser.
    """
    pairs = {
        "USD": "USDSEK=X",
        "EUR": "EURSEK=X",
        "NOK": "NOKSEK=X",
        "CAD": "CADSEK=X",
    }
    out = {"SEK": 1.0}
    for code, sym in pairs.items():
        px = np.nan
        try:
            px = yf.Ticker(sym).fast_info.last_price
        except Exception:
            try:
                px = yf.Ticker(sym).history(period="5d")["Close"].dropna().iloc[-1]
            except Exception:
                px = np.nan
        out[code] = _coerce_float(px)

    df = _read_df(RATES_TITLE)
    if df.empty:
        df = pd.DataFrame(columns=DEFAULT_RATES_COLS)
    for code, rate in out.items():
        if code == "SEK":
            # säkerställ rad finns
            if "SEK" not in df["Valuta"].astype(str).tolist():
                df = pd.concat([df, pd.DataFrame([["SEK", 1.0, now_str()]], columns=DEFAULT_RATES_COLS)], ignore_index=True)
            continue
        ts = now_str()
        if code in df["Valuta"].astype(str).tolist():
            df.loc[df["Valuta"] == code, "Mot SEK"] = rate
            df.loc[df["Valuta"] == code, "Senast uppdaterad"] = ts
        else:
            df = pd.concat([df, pd.DataFrame([[code, rate, ts]], columns=DEFAULT_RATES_COLS)], ignore_index=True)
    _write_df(RATES_TITLE, df)
    return out

def fx_to_sek(ccy: str, amount: float) -> float:
    if not ccy:
        return np.nan
    rates = fetch_fx_to_sek()
    r = rates.get(str(ccy).upper(), np.nan)
    if r is None or np.isnan(r):
        return np.nan
    return _coerce_float(amount) * _coerce_float(r)

# ============================
# Buckets & källskatt (WHT)
# ============================
DEFAULT_BUCKETS = [
    "Bucket A tillväxt", "Bucket B tillväxt", "Bucket C tillväxt",
    "Bucket A utdelning", "Bucket B utdelning", "Bucket C utdelning"
]

@st.cache_data(ttl=0, show_spinner=False)
def get_withholding_map() -> Dict[str, float]:
    # standard: USD 15%, NOK 25%, CAD 15%, SEK 0, EUR 15 (kan ändras i UI)
    return {"USD": 0.15, "NOK": 0.25, "CAD": 0.15, "SEK": 0.00, "EUR": 0.15}

def set_withholding_map(new_map: Dict[str, float]):
    st.session_state["_withholding"] = dict(new_map)

def current_withholding():
    return st.session_state.get("_withholding", get_withholding_map())

# ============
# Alias/helpers
# ============
def guard(cb, label: str = ""):
    try:
        return cb()
    except Exception as e:
        st.error(f"💥 Fel {label}: {e}")
        raise

def build_gspread_client_from_secrets():
    return _build_gspread_client()

def open_spreadsheet(gc):
    return _open_spreadsheet(gc)

def open_or_create_ws(ss, title):
    return _ensure_worksheet(ss, title)

def write_withholding_map(new_map: Dict[str, float]) -> bool:
    set_withholding_map(new_map)
    return True

def refresh_fx_sheet() -> bool:
    fetch_fx_to_sek()
    return True

# app.py — Del 2/5
# Yahoo-hämtare, estimat/CAGR, multipel-drift & metodmatris

# ===========
# Yahoo fetch
# ===========
def _yf_info(tkr: str) -> Dict[str, Any]:
    t = yf.Ticker(tkr)
    info = {}

    # fast_info (snabbt & stabilt)
    try:
        fi = t.fast_info
        info["last_price"] = _coerce_float(getattr(fi, "last_price", np.nan))
        info["currency"]   = getattr(fi, "currency", None)
        info["shares_out"] = _coerce_float(getattr(fi, "shares", np.nan))
        info["market_cap"] = _coerce_float(getattr(fi, "market_cap", np.nan))
    except Exception:
        pass

    # get_info / info (beroende på yfinance-version)
    gi = {}
    try:
        gi = t.get_info()
    except Exception:
        try:
            gi = t.info
        except Exception:
            gi = {}

    gi = gi or {}
    # Plocka ut vanliga nycklar med fallback-listor
    map_keys = {
        "shortName":       ["shortName", "longName", "displayName"],
        "market_cap":      ["marketCap"],
        "enterprise_value":["enterpriseValue"],
        "pe_ttm":          ["trailingPE"],
        "pe_fwd":          ["forwardPE"],
        "ps_ttm":          ["priceToSalesTrailing12Months", "trailingPS"],
        "pb":              ["priceToBook"],
        "ev_ebitda":       ["enterpriseToEbitda"],
        "ev_sales":        ["enterpriseToRevenue"],
        "eps_ttm":         ["trailingEps", "trailingEPS"],
        "revenue_ttm":     ["totalRevenue"],
        "fcf_ttm":         ["freeCashflow"],
        "dividend_rate":   ["dividendRate"],
        "dividend_yield":  ["dividendYield"],
        "currency":        ["currency"],
        "shares_out":      ["sharesOutstanding"],
    }
    for out_key, cand in map_keys.items():
        for k in cand:
            if k in gi and gi[k] is not None:
                info[out_key] = gi[k]
                break

    # Dividend TTM via serie (robustare än info['dividendRate'])
    try:
        div = t.dividends
        if div is not None and len(div) > 0:
            info["dividend_ttm_series"] = float(div.iloc[-4:].sum())
    except Exception:
        pass

    # Sista fallback
    info["shortName"] = info.get("shortName") or tkr
    info["currency"]  = info.get("currency") or "USD"
    return info


@st.cache_data(ttl=60*15, show_spinner=True)
def fetch_yahoo_basics(tkr: str) -> Dict[str, Any]:
    """Hämtar basdata från Yahoo. Returnerar alltid dict med nycklar (kan vara NaN)."""
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
    # Nettoskuld ≈ EV - Market Cap
    if not np.isnan(out["enterprise_value"]) and not np.isnan(out["market_cap"]):
        out["net_debt"] = out["enterprise_value"] - out["market_cap"]
    else:
        out["net_debt"] = np.nan
    return out


# =======================
# Estimat (analytiker) + CAGR-fallback
# =======================
def _try_get_analyst_estimates(tk: yf.Ticker) -> Tuple[float, float, float, float, str]:
    """
    Försök hämta EPS & Revenue-estimat:
      • earnings_trend (current year/next year)
      • analysis-tabellen (om tillgänglig)
      • forwardEps (som sista EPS1 fallback)
    Returnerar: (eps1, eps2, rev1, rev2, source_tag)
    """
    # 1) earnings_trend (nyare yfinance)
    try:
        et = None
        try:
            et = tk.get_earnings_trend()
        except Exception:
            et = getattr(tk, "earnings_trend", None)

        def _extract_from_row(obj, label):
            eps = rev = np.nan
            if obj is None:
                return eps, rev
            if isinstance(obj, pd.DataFrame):
                df = obj.copy()
                if "period" not in df.columns:
                    return eps, rev
                m = df[df["period"].astype(str).str.lower() == label]
                if m.empty:
                    return eps, rev
                r = m.iloc[0]
                ee = r.get("earningsEstimate", {})
                re = r.get("revenueEstimate", {})
                if isinstance(ee, dict):
                    eps = _coerce_float(ee.get("avg"))
                if isinstance(re, dict):
                    rev = _coerce_float(re.get("avg"))
                return eps, rev
            # vissa versioner exponerar lista av dicts
            if isinstance(obj, (list, tuple)):
                row = None
                for d in obj:
                    if isinstance(d, dict) and str(d.get("period","")).lower() == label:
                        row = d; break
                if row:
                    ee = row.get("earningsEstimate", {})
                    re = row.get("revenueEstimate", {})
                    if isinstance(ee, dict):
                        eps = _coerce_float(ee.get("avg"))
                    if isinstance(re, dict):
                        rev = _coerce_float(re.get("avg"))
            return eps, rev

        eps1, rev1 = _extract_from_row(et, "currentyear")
        eps2, rev2 = _extract_from_row(et, "nextyear")
        if not all(np.isnan(x) for x in [eps1, eps2, rev1, rev2]):
            return eps1, eps2, rev1, rev2, "analyst_trend"
    except Exception:
        pass

    # 2) analysis-tabell
    try:
        an = None
        try:
            an = tk.get_analysis()
        except Exception:
            an = getattr(tk, "analysis", None)
        if isinstance(an, pd.DataFrame) and not an.empty:
            df = an.copy()
            def _grab(row_name, col):
                for c in df.columns:
                    if str(c).strip().lower() == col:
                        return _coerce_float(df.loc[row_name, c])
                return np.nan
            if "Earnings Estimate" in df.index and "Revenue Estimate" in df.index:
                eps1 = _grab("Earnings Estimate", "current year")
                eps2 = _grab("Earnings Estimate", "next year")
                rev1 = _grab("Revenue Estimate", "current year")
                rev2 = _grab("Revenue Estimate", "next year")
                if not all(np.isnan(x) for x in [eps1, eps2, rev1, rev2]):
                    return eps1, eps2, rev1, rev2, "analyst_analysis"
    except Exception:
        pass

    # 3) forwardEps fallback
    try:
        info = {}
        try:
            info = tk.get_info()
        except Exception:
            info = {}
        fwd_eps = _coerce_float(info.get("forwardEps"))
        if fwd_eps and not np.isnan(fwd_eps):
            return fwd_eps, np.nan, np.nan, np.nan, "forward_eps"
    except Exception:
        pass

    return np.nan, np.nan, np.nan, np.nan, "none"


@st.cache_data(ttl=60*20, show_spinner=False)
def fetch_eps_rev_estimates(ticker: str, rev_cagr_default=0.10, eps_cagr_default=0.12) -> dict:
    """
    Hämtar eps1/eps2 & rev1/rev2 från analytiker om möjligt,
    annars fall-back till CAGR (uträknad från historik; default om ej går).
    eps3/rev3 extrapoleras från eps2/rev2 med vald CAGR.
    """
    out = {"eps1": np.nan, "eps2": np.nan, "eps3": np.nan,
           "rev1": np.nan, "rev2": np.nan, "rev3": np.nan,
           "g_rev": np.nan, "g_eps": np.nan, "source": "cagr"}

    base = fetch_yahoo_basics(ticker)
    eps0 = _coerce_float(base.get("eps_ttm"))
    rev0 = _coerce_float(base.get("revenue_ttm"))
    tk = yf.Ticker(ticker)

    # 1) Försök med analytiker
    eps1_a, eps2_a, rev1_a, rev2_a, tag = _try_get_analyst_estimates(tk)

    # 2) Historisk CAGR (fallback + för extrapolering till år 3)
    g_rev = np.nan
    g_eps = np.nan
    try:
        q = None
        try:
            q = tk.quarterly_income_stmt
        except Exception:
            try:
                q = tk.get_income_stmt(trailing=True)
            except Exception:
                q = None
        shares = _coerce_float(base.get("shares_out"))

        if isinstance(q, pd.DataFrame) and not q.empty:
            rnR = "TotalRevenue" if "TotalRevenue" in q.index else ("Total Revenue" if "Total Revenue" in q.index else None)
            if rnR:
                vR = pd.Series(q.loc[rnR].dropna().astype(float))
                if len(vR) >= 8:
                    # 8 kvartal → CAGR per år
                    g_rev = (float(vR.iloc[-4:].sum())/float(vR.iloc[-8:-4].sum())) - 1.0

            rnN = "NetIncome" if "NetIncome" in q.index else ("Net Income" if "Net Income" in q.index else None)
            if rnN and shares and shares > 0:
                vE = (pd.Series(q.loc[rnN].dropna().astype(float)) / float(shares))
                if len(vE) >= 8:
                    g_eps = (float(vE.iloc[-4:].sum())/float(vE.iloc[-8:-4].sum())) - 1.0
    except Exception:
        pass
    if np.isnan(g_rev): g_rev = rev_cagr_default
    if np.isnan(g_eps): g_eps = eps_cagr_default

    # 3) Sätt värden, preferera analytiker där det finns
    out["source"] = tag if tag != "none" else "cagr"

    # EPS
    if not np.isnan(eps1_a):
        out["eps1"] = eps1_a
        out["eps2"] = eps2_a if not np.isnan(eps2_a) else (eps1_a * (1 + g_eps))
    else:
        if not np.isnan(eps0) and eps0 > 0:
            out["eps1"] = eps0 * (1 + g_eps)
            out["eps2"] = out["eps1"] * (1 + g_eps)
    out["eps3"] = out["eps2"] * (1 + g_eps) if not np.isnan(out["eps2"]) else np.nan

    # Revenue
    if not np.isnan(rev1_a):
        out["rev1"] = rev1_a
        out["rev2"] = rev2_a if not np.isnan(rev2_a) else (rev1_a * (1 + g_rev))
    else:
        if not np.isnan(rev0) and rev0 > 0:
            out["rev1"] = rev0 * (1 + g_rev)
            out["rev2"] = out["rev1"] * (1 + g_rev)
    out["rev3"] = out["rev2"] * (1 + g_rev) if not np.isnan(out["rev2"]) else np.nan

    out["g_rev"] = g_rev
    out["g_eps"] = g_eps
    return out

# Bakåtkompatibelt alias (om senare kod råkar kalla det “fetch_estimates_or_cagr”)
def fetch_estimates_or_cagr(tkr: str, eps_ttm: float = np.nan, rev_ttm: float = np.nan) -> Dict[str, Any]:
    d = fetch_eps_rev_estimates(tkr)
    return {
        "eps_1y": d["eps1"], "eps_2y": d["eps2"], "eps_3y": d["eps3"],
        "rev_1y": d["rev1"], "rev_2y": d["rev2"], "rev_3y": d["rev3"],
        "eps_source": d["source"], "rev_source": d["source"],
        "g_eps": d["g_eps"], "g_rev": d["g_rev"]
    }


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
    EV_future = ev_mult * driver_future
    Equity_future = EV_future - NetDebt
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
    return {"0y": _coerce_float(eps_ttm), "1y": _coerce_float(eps1),
            "2y": _coerce_float(eps2), "3y": _coerce_float(eps3)}

def _rev_path(rev_ttm: float, r1: float, r2: float, r3: float) -> Dict[str, float]:
    return {"0y": _coerce_float(rev_ttm), "1y": _coerce_float(r1),
            "2y": _coerce_float(r2), "3y": _coerce_float(r3)}

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
    • ev_fcf/p_fcf (om FCF-data finns)
    • p_b / p_nav / p_tbv / p_nii / p_affo (placeholder om saknas per-aktie-tal)
    """
    price_now = _coerce_float(bas.get("price"))
    shares = _coerce_float(bas.get("shares_out"))
    net_debt = _coerce_float(bas.get("net_debt"))
    currency = bas.get("currency", "USD")

    epsp = _eps_path(bas.get("eps_ttm"), est.get("eps_1y"), est.get("eps_2y"), est.get("eps_3y"))
    revp = _rev_path(bas.get("revenue_ttm"), est.get("rev_1y"), est.get("rev_2y"), est.get("rev_3y"))

    rows = []

    # ---- 1) P/E-ankare ≈ historik/fwd mix
    pe0 = _coerce_float(bas.get("pe_ttm"))  # vi sätter in blandankaret före anrop (se UI)
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

    # ---- 3) EV/EBITDA (EBITDA ≈ margin * revenue om saknas)
    ev_ebitda = _coerce_float(bas.get("ev_ebitda"))
    def _ebitda_from_rev(r):
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

    # ---- 5) EV/FCF & P/FCF (om data finns)
    fcf_ttm = _coerce_float(bas.get("fcf_ttm"))
    if not np.isnan(fcf_ttm) and fcf_ttm > 0 and shares and shares > 0:
        fcf_ps = fcf_ttm / shares
        p_fcf_now = price_now / fcf_ps if price_now and fcf_ps and fcf_ps > 0 else np.nan
        g_rev = _coerce_float(est.get("g_rev"))
        if np.isnan(g_rev):
            g_rev = 0.10
        f1 = fcf_ps * (1 + g_rev)
        f2 = fcf_ps * (1 + g_rev) ** 2
        f3 = fcf_ps * (1 + g_rev) ** 3
        rows.append({
            "Metod": "p_fcf",
            "Idag": p_fcf_now * fcf_ps if not np.isnan(p_fcf_now) else np.nan,
            "1 år": p_fcf_now * (1 + mult_drift) * f1,
            "2 år": p_fcf_now * (1 + mult_drift) ** 2 * f2,
            "3 år": p_fcf_now * (1 + mult_drift) ** 3 * f3,
        })
        ev_fcf_now = _coerce_float(bas.get("enterprise_value")) / fcf_ttm \
            if not np.isnan(bas.get("enterprise_value")) and fcf_ttm > 0 else np.nan
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

    # ---- 6) P/B, P/NAV, P/TBV, P/NII, P/AFFO (kräver per-aktie-tal; placeholder)
    for m in ["p_b", "p_nav", "p_tbv", "p_nii", "p_affo"]:
        rows.append({"Metod": m, "Idag": np.nan, "1 år": np.nan, "2 år": np.nan, "3 år": np.nan})

    df = pd.DataFrame(rows, columns=["Metod", "Idag", "1 år", "2 år", "3 år"])
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
      • Utdelnings-bucket: pe_hist_vs_eps – annars ev_ebitda
    Returnerar (metod, idag, 1y, 2y, 3y)
    """
    def _get_row(name):
        rr = methods_df[methods_df["Metod"] == name]
        return rr.iloc[0] if not rr.empty else None

    b = (bucket or "").lower()
    order = ["ev_ebitda", "ev_sales", "pe_hist_vs_eps"] if "utdelning" not in b else \
            ["pe_hist_vs_eps", "ev_ebitda", "ev_sales"]

    for m in order:
        r = _get_row(m)
        if r is not None and (not np.isnan(r["Idag"]) or not np.isnan(r["1 år"])):
            return m, r["Idag"], r["1 år"], r["2 år"], r["3 år"]

    r0 = methods_df.iloc[0]
    return r0["Metod"], r0["Idag"], r0["1 år"], r0["2 år"], r0["3 år"]


# ==========================
# Skriv tillbaka till Data
# ==========================
def upsert_data_row_from_fetch(tkr: str, bucket: Optional[str] = None) -> Dict[str, Any]:
    """
    Hämtar Yahoo-basics + analytikerestimat/CAGR och skriver in/uppdaterar i fliken Data.
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
    cur_bucket = bucket or (df.loc[mask, "Bucket"].iloc[0] if exists else "")

    row = {
        "Ticker": tkr,
        "Bolagsnamn": bas["name"],
        "Bucket": cur_bucket,
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
        for k in DEFAULT_DATA_COLS:
            if k not in df.columns:
                df[k] = np.nan
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    _write_df(DATA_TITLE, df)
    return row

# app.py — Del 3/5
# Analys-UI (per ticker), metodmatris, utdelning, spara till Resultat

# ============
# Små UI-hjälpare
# ============
def _upside(now_price: float, target: float) -> float:
    now_price = _coerce_float(now_price)
    target = _coerce_float(target)
    if np.isnan(now_price) or np.isnan(target) or now_price <= 0:
        return np.nan
    return (target / now_price) - 1.0

def _ensure_result_cols(df: pd.DataFrame) -> pd.DataFrame:
    need = DEFAULT_RESULT_COLS  # från Del 1
    for c in need:
        if c not in df.columns:
            df[c] = np.nan
    return df[need]

def save_primary_targets_to_result(row_in_data: Dict[str, Any],
                                   method_name: str,
                                   t0: float, t1: float, t2: float, t3: float,
                                   price_now: float,
                                   source_note: str = "") -> None:
    """Append primär riktkurs till fliken RESULT_TITLE."""
    res_df = _read_df(RESULT_TITLE)
    if res_df is None or res_df.empty:
        res_df = pd.DataFrame(columns=DEFAULT_RESULT_COLS)
    res_df = _ensure_result_cols(res_df)

    up = _upside(price_now, t0)
    newr = {
        "Timestamp": now_str(),
        "Ticker": row_in_data.get("Ticker"),
        "Namn": row_in_data.get("Bolagsnamn") or row_in_data.get("name") or row_in_data.get("shortName"),
        "Bucket": row_in_data.get("Bucket", ""),
        "Valuta": row_in_data.get("Valuta", row_in_data.get("currency", "USD")),
        "Metod": method_name,
        "Pris nu": price_now,
        "Target idag": t0,
        "Target 1 år": t1,
        "Target 2 år": t2,
        "Target 3 år": t3,
        "Upp/Ned idag (%)": up * 100 if not np.isnan(up) else np.nan,
        "Källa": source_note or "Yahoo/Model",
    }
    res_df = pd.concat([res_df, pd.DataFrame([newr])], ignore_index=True)
    _write_df(RESULT_TITLE, res_df)


# ======================
# Hjälp: hämta vald rad
# ======================
def _load_current_row(df: pd.DataFrame, ticker: str) -> Dict[str, Any]:
    if df is None or df.empty:
        return {}
    mask = (df["Ticker"].astype(str).str.upper() == (ticker or "").upper())
    if not mask.any():
        return {}
    return df.loc[mask].iloc[0].to_dict()


# ======================
# Analys-vy (huvud)
# ======================
def run_analysis_ui():
    st.header("🔍 Analys & riktkurser")

    # Läs Data-bladet
    data_df = _read_df(DATA_TITLE)
    if data_df is None or data_df.empty:
        st.info("Inga bolag i fliken **Data** ännu. Lägg till minst en rad.")
        return

    # Val av ticker
    tickers = data_df["Ticker"].dropna().astype(str).unique().tolist()
    ticker = st.selectbox("Välj ticker", options=sorted(tickers))
    row = _load_current_row(data_df, ticker)

    # Snabbinfo
    ccy = row.get("Valuta", "USD") or "USD"
    colA, colB, colC, colD = st.columns(4)
    colA.metric("Namn", row.get("Bolagsnamn", "—"))
    colB.metric("Bucket", row.get("Bucket", "—"))
    colC.metric("Valuta", ccy)
    colD.metric("Aktuell kurs", _fmt_money(row.get("Aktuell kurs"), ccy))

    # Modellinställningar
    with st.expander("⚙️ Modellinställningar", expanded=False):
        w_hist_fwd = st.slider("P/E-ankare: vikt historisk (TTM) vs framåtblickande (FWD)",
                               min_value=0.0, max_value=1.0, value=0.50, step=0.05, help="0.50 = 50/50 (rekommenderat)")
        drift = st.slider("Multipel-drift/år (negativ = multipel sjunker)",
                          min_value=-0.25, max_value=0.00, value=-0.06, step=0.01,
                          help="Antag multipelkompression över tid, t.ex. −6%/år")
        ebitda_margin = st.slider("Fallback EBITDA-marginal (om saknas)", 0.05, 0.60, 0.35, 0.01)

    # Uppdateringsknappar
    c1, c2 = st.columns([1,1])
    if c1.button("🔄 Uppdatera vald ticker (Yahoo)"):
        upsert_data_row_from_fetch(ticker, bucket=row.get("Bucket"))
        st.success(f"Uppdaterade {ticker} från Yahoo.")
        data_df = _read_df(DATA_TITLE)  # läs om
        row = _load_current_row(data_df, ticker)
    if c2.button("📥 Hämta estimat/CAGR igen"):
        # tvinga cachestart om
        _ = fetch_estimates_or_cagr(ticker)
        upsert_data_row_from_fetch(ticker, bucket=row.get("Bucket"))
        st.success("Estimat/CAGR uppdaterade och sparade.")

    # Bygg 'bas' och 'est'
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
    if not np.isnan(bas["enterprise_value"]) and not np.isnan(bas["market_cap"]):
        bas["net_debt"] = bas["enterprise_value"] - bas["market_cap"]
    else:
        bas["net_debt"] = np.nan

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

    # Komplettera om kritiska saknas
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

    # Beräkna metod-matris
    # Användaren styr blandningen via w_hist_fwd, så vi ersätter pe_ttm/pe_fwd med ankaret
    pe0 = anchor_pe(bas.get("pe_ttm"), bas.get("pe_fwd"), w_hist_fwd=w_hist_fwd)
    bas_for_calc = dict(bas)
    bas_for_calc["pe_ttm"] = pe0
    bas_for_calc["pe_fwd"] = pe0

    methods_df = compute_methods_matrix(bas_for_calc, est,
                                        mult_drift=drift,
                                        ebitda_margin_fallback=ebitda_margin)

    # Sanity
    sanity = []
    sanity.append("price ok" if not np.isnan(bas["price"]) else "price —")
    sanity.append("eps_ttm ok" if not np.isnan(bas["eps_ttm"]) else "eps_ttm —")
    sanity.append("rev_ttm ok" if not np.isnan(bas["revenue_ttm"]) else "rev_ttm —")
    sanity.append("ev/ebitda ok" if not np.isnan(bas["ev_ebitda"]) else "ev/ebitda —")
    sanity.append("shares ok" if not np.isnan(bas["shares_out"]) else "shares —")
    st.caption("Sanity: " + ", ".join(sanity))

    # Visa hela matrisen
    st.subheader("🧭 Alla metoder (Idag, 1, 2, 3 år)")
    st.dataframe(methods_df, use_container_width=True)

    # Välj primär metod (heuristik från bucket)
    prim_method, t0, t1, t2, t3 = pick_primary_method(row.get("Bucket",""), methods_df)

    # Primär riktkurs
    st.subheader("🎯 Primär riktkurs")
    price_now = bas["price"]
    cols = st.columns(4)
    cols[0].metric("Idag", _fmt_money(t0, ccy), f"{_fmt_pct(_upside(price_now, t0))}")
    cols[1].metric("1 år", _fmt_money(t1, ccy))
    cols[2].metric("2 år", _fmt_money(t2, ccy))
    cols[3].metric("3 år", _fmt_money(t3, ccy))
    st.caption(f"Metod: **{prim_method}** • Valuta: **{ccy}** • Ankare P/E vikt: {int(w_hist_fwd*100)}% TTM")

    # Utdelning & innehavsvärde
    st.subheader("💰 Utdelning (netto, SEK)")
    wmap = get_withholding_map()
    shares_user = _coerce_float(row.get("Antal aktier", 0))
    div_cagr = _coerce_float(row.get("Utdelning CAGR 5y (%)", np.nan)) / 100.0 if not np.isnan(_coerce_float(row.get("Utdelning CAGR 5y (%)", np.nan))) else np.nan
    divs = forecast_dividends_net_sek(bas, years=3, withholding_map=wmap,
                                      user_shares=shares_user, div_cagr=div_cagr)
    d1, d2, d3 = divs if len(divs) == 3 else (0,0,0)
    st.write(f"• 1 år: **{_fmt_money(d1, 'SEK')}** • 2 år: **{_fmt_money(d2, 'SEK')}** • 3 år: **{_fmt_money(d3, 'SEK')}**")
    st.caption(f"Källskatt: {int((wmap.get(ccy.upper(),0.15))*100)}% • Antal aktier: {int(shares_user or 0)}")

    st.subheader("🧾 Innehavsvärde")
    position_value_sek = 0.0
    if shares_user and shares_user > 0 and not np.isnan(price_now):
        position_value_sek = fx_to_sek(ccy, price_now * shares_user)
    st.write(f"Totalt värde nu: **{_fmt_money(position_value_sek, 'SEK')}**")

    # Spara primär riktkurs till Resultat
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

# app.py — Del 4/5
# Ranking, Inställningar (källskatt + FX), Batch-uppdatering, Navigation & main()

import time

# ==========================
# UI: Ranking & portföljsammanställning
# ==========================
def run_rank_ui():
    st.header("🏁 Rangordning (störst uppsida först)")

    data_df = _read_df(DATA_TITLE)
    if data_df is None or data_df.empty:
        st.info("Inga bolag i fliken **Data** än.")
        return

    # Filter: buckets + ägande
    buckets = ["(alla)"] + sorted(data_df["Bucket"].dropna().astype(str).unique().tolist())
    c1, c2, c3 = st.columns([1,1,1])
    bucket_pick = c1.selectbox("Filter: Bucket", options=buckets, index=0)

    own_filter = c2.selectbox(
        "Ägande",
        options=["Alla", "Endast jag äger", "Endast jag äger inte"],
        index=0
    )
    max_rows = int(c3.number_input("Max rader att beräkna", 1, 5000, min(400, len(data_df))))

    with st.expander("⚙️ Modellinställningar (ranking)", expanded=False):
        w_hist_fwd = st.slider("P/E-ankare vikt (TTM vs FWD)", 0.0, 1.0, 0.50, 0.05, key="rk_pew")
        drift = st.slider("Multipel-drift per år", -0.25, 0.00, -0.06, 0.01, key="rk_drift")
        ebitda_margin = st.slider("Fallback EBITDA-marginal", 0.05, 0.60, 0.35, 0.01, key="rk_ebm")
        throttle = st.slider("Fördröjning mellan hämtningar (sek)", 0.0, 2.0, 0.4, 0.1, key="rk_thr")

    # Filtrera
    df = data_df.copy()
    if bucket_pick != "(alla)":
        df = df[df["Bucket"].astype(str) == bucket_pick]

    shares_series = _coerce_float_series(df.get("Antal aktier"))
    if own_filter == "Endast jag äger":
        df = df[shares_series > 0]
    elif own_filter == "Endast jag äger inte":
        df = df[(shares_series.isna()) | (shares_series <= 0)]

    df = df.head(max_rows).reset_index(drop=True)
    if df.empty:
        st.info("Inget matchade filtret.")
        return

    if not st.button("▶️ Beräkna ranking"):
        st.caption("Tryck på **Beräkna ranking** för att räkna uppsida per bolag.")
        return

    progress = st.progress(0, text="Startar…")
    out_rows = []

    for i, (_, r) in enumerate(df.iterrows(), start=1):
        ticker = str(r.get("Ticker"))
        ccy = r.get("Valuta", "USD") or "USD"

        # Bas/est från Data
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

        # Komplettera via Yahoo om något kritiskt saknas
        if any(np.isnan(v) for v in [bas["price"], bas["pe_ttm"], bas["pe_fwd"], bas["revenue_ttm"]]):
            y = fetch_yahoo_basics(ticker)
            for k in ["price","pe_ttm","pe_fwd","revenue_ttm","ev_ebitda","ev_sales","pb","ps_ttm",
                      "market_cap","enterprise_value","dividend_ttm","shares_out","currency"]:
                if np.isnan(_coerce_float(bas.get(k))) and (y.get(k) is not None):
                    bas[k] = y.get(k)
            if np.isnan(bas["net_debt"]) and not np.isnan(bas["enterprise_value"]) and not np.isnan(bas["market_cap"]):
                bas["net_debt"] = bas["enterprise_value"] - bas["market_cap"]

        # Estimat/CAGR om tomt
        if any(np.isnan(v) for v in [est["eps_1y"], est["eps_2y"]]):
            est2 = fetch_estimates_or_cagr(ticker, bas.get("eps_ttm"), bas.get("revenue_ttm"))
            for k, v in est2.items():
                if k in est and (est[k] is None or np.isnan(_coerce_float(est[k]))):
                    est[k] = v

        # P/E-ankare (mix TTM/FWD)
        pe0 = anchor_pe(bas.get("pe_ttm"), bas.get("pe_fwd"), w_hist_fwd=w_hist_fwd)
        bas_calc = dict(bas)
        bas_calc["pe_ttm"] = pe0
        bas_calc["pe_fwd"] = pe0

        # Matris + primär metod
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


# ==========================
# UI: Inställningar
# ==========================
def run_settings_ui():
    st.header("⚙️ Inställningar")

    # Källskatt per valuta (dynamisk lista)
    st.subheader("Källskatt per valuta (dynamisk)")
    curr_map = get_withholding_map()
    edit_df = pd.DataFrame(
        [{"Valuta": k, "Källskatt (%)": int(v*100)} for k, v in sorted(curr_map.items())]
    )
    edit_df = st.data_editor(edit_df, num_rows="dynamic",
                             use_container_width=True,
                             key="withholding_editor")
    if st.button("💾 Spara källskatter"):
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
        st.success("Källskatter sparade (session).")

    # Valutakurser
    st.subheader("Valutakurser (→ SEK)")
    fx_df = _read_df(RATES_TITLE)
    if fx_df is not None and not fx_df.empty:
        st.dataframe(fx_df, use_container_width=True)
    if st.button("🔄 Uppdatera valutakurser nu"):
        try:
            refreshed = refresh_fx_sheet()
            if refreshed:
                st.success("Valutakurser uppdaterade.")
            else:
                st.warning("Kunde inte uppdatera valutakurser.")
        except Exception as e:
            st.error(f"Fel vid FX-uppdatering: {e}")


# ==========================
# UI: Batch-uppdatera Data
# ==========================
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
    st.title("📈 Aktieanalys & riktkurser")
    # Säkerställ att flikar finns (kördes även vid import i Del 1, men harmless att köra igen)
    _bootstrap_sheets()
    # Kör UI
    run_main_ui()

if __name__ == "__main__":
    main()

# app.py — Del 5/5
# Editor (lägg till/uppdatera), Portföljvy och små fixar

# ==============================
# Källskatt – gör UI-ändringen global
# ==============================
def get_withholding_map() -> Dict[str, float]:
    """
    Överskuggar tidigare definition: hämta sessionens karta om satt,
    annars default. (Ingen caching → alltid aktuell i session.)
    """
    return st.session_state.get("_withholding", {
        "USD": 0.15, "NOK": 0.25, "CAD": 0.15, "SEK": 0.00, "EUR": 0.15
    })


# ==================
# Editor – lägg till / uppdatera
# ==================
def _ensure_data_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=DEFAULT_DATA_COLS)
    cols = list(df.columns)
    for c in DEFAULT_DATA_COLS:
        if c not in cols:
            cols.append(c)
    return df.reindex(columns=cols)

def run_editor_ui():
    st.header("✍️ Editor – lägg till / uppdatera bolag")

    df = _read_df(DATA_TITLE)
    df = _ensure_data_cols(df)

    # Välj befintlig ticker eller skriv ny
    left, right = st.columns([2, 1])
    existing = sorted([t for t in df["Ticker"].dropna().astype(str).unique().tolist() if t])
    pick = left.selectbox("Välj befintlig ticker (eller skriv ny nedan)", options=["(ny)"] + existing, index=0)
    tkr = left.text_input("Ticker", value="" if pick == "(ny)" else pick).strip().upper()

    # Förifyll från befintlig rad
    row = {}
    if tkr and (tkr in df["Ticker"].astype(str).str.upper().values):
        row = df.loc[df["Ticker"].astype(str).str.upper() == tkr].iloc[0].to_dict()

    bucket = right.selectbox(
        "Bucket",
        options=DEFAULT_BUCKETS,
        index=(DEFAULT_BUCKETS.index(row.get("Bucket")) if row.get("Bucket") in DEFAULT_BUCKETS else 0)
    )
    col1, col2, col3 = st.columns(3)
    shares = col1.number_input("Antal aktier", min_value=0.0, value=float(_coerce_float(row.get("Antal aktier") or 0.0)), step=1.0)
    gav_sek = col2.number_input("GAV (SEK)", min_value=0.0, value=float(_coerce_float(row.get("GAV (SEK)") or 0.0)), step=0.01, format="%.2f")
    div_cagr_pct = col3.number_input("Utdelning CAGR 5y (%)", min_value=0.0, max_value=100.0,
                                     value=float(_coerce_float(row.get("Utdelning CAGR 5y (%)") or 0.0)), step=0.1)

    st.caption("Tips: Spara först, därefter kan du köra **Hämta från Yahoo** för att autofylla nycklar.")

    c1, c2, c3 = st.columns([1, 1, 2])

    def _save_manual_fields(ticker: str):
        nonlocal df
        df = _ensure_data_cols(df)
        mask = (df["Ticker"].astype(str).str.upper() == ticker.upper())
        exists = mask.any()
        if not exists:
            base = {c: np.nan for c in DEFAULT_DATA_COLS}
            base.update({
                "Ticker": ticker,
                "Bucket": bucket,
                "Antal aktier": shares,
                "GAV (SEK)": gav_sek,
                "Utdelning CAGR 5y (%)": div_cagr_pct,
                "Senast auto uppdaterad": "",
                "Auto källa": ""
            })
            df = pd.concat([df, pd.DataFrame([base])], ignore_index=True)
        else:
            df.loc[mask, "Bucket"] = bucket
            df.loc[mask, "Antal aktier"] = shares
            df.loc[mask, "GAV (SEK)"] = gav_sek
            df.loc[mask, "Utdelning CAGR 5y (%)"] = div_cagr_pct
        _write_df(DATA_TITLE, df)

    if c1.button("💾 Spara (manuella fält)"):
        if not tkr:
            st.error("Ange en ticker.")
        else:
            _save_manual_fields(tkr)
            st.success(f"Sparat manuella fält för {tkr}.")

    if c2.button("🔄 Hämta från Yahoo & spara"):
        if not tkr:
            st.error("Ange en ticker.")
        else:
            _save_manual_fields(tkr)
            upsert_data_row_from_fetch(tkr, bucket=bucket)
            st.success(f"Hämtat och sparat auto-fält för {tkr}.")

    # Visa rad efter spar
    if tkr:
        df2 = _read_df(DATA_TITLE)
        mask = (df2["Ticker"].astype(str).str.upper() == tkr.upper())
        if mask.any():
            st.subheader("📄 Nuvarande rad i Data")
            st.dataframe(df2.loc[mask], use_container_width=True)


# ==================
# Portfölj – värde & utdelning
# ==================
def _position_value_sek(row: Dict[str, Any], rates: Dict[str, float]) -> float:
    ccy = row.get("Valuta", "USD") or "USD"
    price = _coerce_float(row.get("Aktuell kurs"))
    shares = _coerce_float(row.get("Antal aktier"))
    if np.isnan(price) or np.isnan(shares) or shares <= 0:
        return 0.0
    fx = rates.get(str(ccy).upper(), np.nan)
    if np.isnan(fx):
        return 0.0
    return price * shares * fx

def _net_dividend_12m_sek(row: Dict[str, Any], rates: Dict[str, float], wmap: Dict[str, float]) -> float:
    ccy = row.get("Valuta", "USD") or "USD"
    div = _coerce_float(row.get("Utdelning TTM"))
    shares = _coerce_float(row.get("Antal aktier"))
    if np.isnan(div) or np.isnan(shares) or shares <= 0:
        return 0.0
    fx = rates.get(str(ccy).upper(), np.nan)
    w = wmap.get(str(ccy).upper(), 0.15)
    if np.isnan(fx):
        return 0.0
    return div * shares * (1.0 - w) * fx

def run_portfolio_ui():
    st.header("📦 Portfölj")

    df = _read_df(DATA_TITLE)
    if df is None or df.empty:
        st.info("Inga bolag i **Data**.")
        return

    rates = fetch_fx_to_sek()
    wmap = get_withholding_map()

    work = df.copy()
    work["Antal aktier"] = _coerce_float_series(work.get("Antal aktier"))
    work["Aktuell kurs"] = _coerce_float_series(work.get("Aktuell kurs"))
    work["Utdelning TTM"] = _coerce_float_series(work.get("Utdelning TTM"))
    work["Valuta"] = work.get("Valuta").fillna("USD")

    # Beräkna position & utdelning
    pos_values = []
    net_divs = []
    net_yields = []
    for _, r in work.iterrows():
        rv = r.to_dict()
        vsek = _position_value_sek(rv, rates)
        dsek = _net_dividend_12m_sek(rv, rates, wmap)
        pos_values.append(vsek)
        net_divs.append(dsek)
        # Net yield på aktuell kurs (i bolagsvaluta men netto efter källskatt)
        price = _coerce_float(rv.get("Aktuell kurs"))
        div = _coerce_float(rv.get("Utdelning TTM"))
        w = wmap.get(str(rv.get("Valuta","USD")).upper(), 0.15)
        ny = np.nan
        if not np.isnan(price) and not np.isnan(div) and price > 0:
            ny = (div * (1.0 - w)) / price
        net_yields.append(ny)

    work["Värde (SEK)"] = pos_values
    work["Netto utdelning 12m (SEK)"] = net_divs
    work["DA netto (%)"] = [v*100 if v == v else np.nan for v in net_yields]  # v==v → not NaN

    # Filtrering
    c1, c2 = st.columns([1,1])
    bucket_pick = c1.selectbox("Filter: Bucket", options=["(alla)"] + sorted(work["Bucket"].dropna().unique().tolist()), index=0)
    own_filter = c2.selectbox("Ägande", options=["Alla", "Endast jag äger"], index=1)

    f = work.copy()
    if bucket_pick != "(alla)":
        f = f[f["Bucket"].astype(str) == bucket_pick]
    if own_filter == "Endast jag äger":
        f = f[_coerce_float_series(f["Antal aktier"]) > 0]

    # Visa tabell
    show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Aktuell kurs","Antal aktier","Värde (SEK)","Utdelning TTM","DA netto (%)","Netto utdelning 12m (SEK)"]
    for c in show_cols:
        if c not in f.columns:
            f[c] = np.nan
    st.dataframe(f[show_cols].sort_values(by="Värde (SEK)", ascending=False), use_container_width=True)

    # Summer
    total_value = float(np.nansum(f["Värde (SEK)"].values))
    total_div = float(np.nansum(f["Netto utdelning 12m (SEK)"].values))

    st.subheader("📊 Summering")
    cA, cB = st.columns(2)
    cA.metric("Totalt portföljvärde (SEK)", _fmt_money(total_value, "SEK"))
    cB.metric("Total netto-utdelning 12m (SEK)", _fmt_money(total_div, "SEK"))

    st.subheader("🧺 Per bucket")
    grp = f.groupby("Bucket", dropna=False).agg({
        "Värde (SEK)": "sum",
        "Netto utdelning 12m (SEK)": "sum"
    }).reset_index().rename(columns={"Värde (SEK)":"Värde SEK", "Netto utdelning 12m (SEK)":"Netto utd 12m SEK"})
    st.dataframe(grp.sort_values(by="Värde SEK", ascending=False), use_container_width=True)


# ==================
# Uppdatera menyn – inkludera Editor & Portfölj
# ==================
def run_main_ui():
    st.sidebar.title("📌 Meny")
    view = st.sidebar.radio(
        "Välj vy",
        ["Analys", "Ranking", "Portfölj", "Editor", "Inställningar", "Batch-uppdatera"],
        index=0
    )

    if view == "Analys":
        run_analysis_ui()
    elif view == "Ranking":
        run_rank_ui()
    elif view == "Portfölj":
        run_portfolio_ui()
    elif view == "Editor":
        run_editor_ui()
    elif view == "Inställningar":
        run_settings_ui()
    elif view == "Batch-uppdatera":
        run_batch_update_ui()
