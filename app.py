# app.py — BucketApp (full)
from __future__ import annotations

import os, time, math, json, re, io
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

import yfinance as yf

# ============== UI & sida ==============
st.set_page_config(page_title="BucketApp – fair value & riktkurser", layout="wide", page_icon="🎯")

# ============== Hjälp: datum/tid ==============
def now_iso():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

# ============== Hjälp: tal ==============
def _coerce_float(x, default=np.nan):
    try:
        if x is None: return default
        if isinstance(x, (int, float, np.number)): return float(x)
        s = str(x).strip().replace(" ", "").replace(",", ".")
        if s == "" or s.lower() == "nan": return default
        # ta bort T, B, M suffix om de kommer från Yahoo info
        m = re.match(r"^([\-]?\d+(\.\d+)?)([TtBbMmKk])$", s)
        if m:
            val = float(m.group(1))
            suf = m.group(3).lower()
            mult = {"k":1e3,"m":1e6,"b":1e9,"t":1e12}[suf]
            return val*mult
        return float(s)
    except Exception:
        return default

def _fmt_money(x, ccy):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "-"
    try:
        return f"{x:,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{x} {ccy}"

def _fmt_num(x):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return ""
    try:
        return f"{x:,.4f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(x)

def _safe_div(a, b):
    a = _coerce_float(a); b = _coerce_float(b)
    if b in [0, None] or (isinstance(b, float) and b == 0.0): return np.nan
    if np.isnan(a) or np.isnan(b): return np.nan
    return a / b

# ============== Backoff ==============
def _sleep_backoff(i):
    time.sleep(min(0.25*(i+1), 2.0))

# ============== Secrets & konfig ==============
SHEET_URL = st.secrets.get("SHEET_URL", "").strip()
FINNHUB_API_KEY = st.secrets.get("FINNHUB_API_KEY", "").strip()
FMP_API_KEY = st.secrets.get("FMP_API_KEY", "").strip()
SEC_USER_AGENT = st.secrets.get("SEC_USER_AGENT", "BucketApp/1.0 (contact: user@example.com)").strip()

# Standard-buckets → primär metod (kan överstyras per rad i Data via kolumn "Primär metod")
BUCKET_PRIMARY = {
    "Bucket A tillväxt": "ev_ebitda",
    "Bucket B tillväxt": "ev_sales",
    "Bucket C tillväxt": "ev_sales",
    "Bucket A utdelning": "p_affo",
    "Bucket B utdelning": "p_affo",
    "Bucket C utdelning": "p_b",
}

# Multipel-drift per år (kan ändras i Settings)
DEFAULT_MULT_DRIFT = -0.10  # -10% per år

# ============== Google Sheets klient (fix för UnhashableParamError) ==============
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    if "GOOGLE_CREDENTIALS" not in st.secrets:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i secrets.")
    creds = dict(st.secrets["GOOGLE_CREDENTIALS"])
    creds = _normalize_private_key(creds)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def open_spreadsheet(_gc: gspread.Client):
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL saknas i secrets.")
    for i in range(3):
        try:
            return _gc.open_by_url(SHEET_URL)
        except APIError:
            _sleep_backoff(i)
    raise RuntimeError("Kunde inte öppna Google Sheet. Kontrollera delning till servicekontot.")

def _get_ws(spreadsheet: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=1000, cols=50)

# ============== Läs/skriv DF till Google Sheets ==============
def _df_from_ws(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = ws.get_all_values()
    if not vals:
        return pd.DataFrame()
    df = pd.DataFrame(vals)
    if df.empty:
        return df
    df.columns = df.iloc[0].fillna("").tolist()
    df = df.iloc[1:].reset_index(drop=True)
    return df

def _write_df_to_ws(ws: gspread.Worksheet, df: pd.DataFrame):
    if df is None:
        return
    df = df.copy()
    df = df.fillna("")
    # skriv rubriker + data
    data = [list(df.columns)] + df.values.tolist()
    ws.clear()
    ws.update("A1", data, value_input_option="USER_ENTERED")

# ============== Initiera Sheets & standardflikar ==============
@st.cache_resource(show_spinner=False)
def ensure_sheets():
    gc = get_gspread_client()
    ss = open_spreadsheet(gc)
    data_ws = _get_ws(ss, "Data")
    fx_ws = _get_ws(ss, "Valutakurser")
    adv_ws = _get_ws(ss, "Avancerat")       # manuell per ticker: BVPS, NAVPS, AFFOPS, NIIps, TBVPS, FCF margin etc
    res_ws = _get_ws(ss, "Resultat")        # historik av sparade primära riktkurser
    set_ws = _get_ws(ss, "Settings")        # multipel-drift, källskatt-tabell m.m.
    snap_ws = _get_ws(ss, "Snapshots")      # kvartalssnapshots (långa loggar)
    return dict(ss=ss, data_ws=data_ws, fx_ws=fx_ws, adv_ws=adv_ws, res_ws=res_ws, set_ws=set_ws, snap_ws=snap_ws)

# ============== Default/seed för Settings ==============
DEFAULT_SETTINGS = {
    "mult_drift_per_year": DEFAULT_MULT_DRIFT,   # ex. -0.10
    "withholding": [                             
        {"currency":"USD","pct":15},
        {"currency":"NOK","pct":25},
        {"currency":"CAD","pct":15},
        {"currency":"EUR","pct":0},
        {"currency":"SEK","pct":0},
        {"currency":"GBP","pct":0},
    ],
    "dividend_cagr_default": 0.03,
    "rev_cagr_default": 0.10,
    "eps_cagr_default": 0.10,
}

def _load_settings(set_ws: gspread.Worksheet) -> Dict[str, Any]:
    df = _df_from_ws(set_ws)
    if df.empty:
        # skriv seed
        payload = {
            "Key": ["mult_drift_per_year","withholding","dividend_cagr_default","rev_cagr_default","eps_cagr_default"],
            "Value": [
                str(DEFAULT_SETTINGS["mult_drift_per_year"]),
                json.dumps(DEFAULT_SETTINGS["withholding"]),
                str(DEFAULT_SETTINGS["dividend_cagr_default"]),
                str(DEFAULT_SETTINGS["rev_cagr_default"]),
                str(DEFAULT_SETTINGS["eps_cagr_default"]),
            ]
        }
        _write_df_to_ws(set_ws, pd.DataFrame(payload))
        return DEFAULT_SETTINGS.copy()
    out = DEFAULT_SETTINGS.copy()
    try:
        for _, row in df.iterrows():
            k = str(row.get("Key","")).strip()
            v = str(row.get("Value","")).strip()
            if k == "withholding":
                out["withholding"] = json.loads(v) if v else DEFAULT_SETTINGS["withholding"]
            elif k in out:
                out[k] = _coerce_float(v, out[k]) if "cagr" in k or "drift" in k else v
    except Exception:
        pass
    return out

# ============== Valutakurser (SEK-baserade) ==============
@st.cache_data(ttl=60*60, show_spinner=False)
def _yahoo_fx_to_sek(ccy: str) -> float:
    """Hämtar SEK per 1 CCY, ex USD→SEK: 'SEK=X' / 'USDSEK=X' """
    if ccy.upper() == "SEK": return 1.0
    pair = f"{ccy.upper()}SEK=X"
    try:
        t = yf.Ticker(pair)
        p = t.fast_info.get("last_price")
        if not p:
            hist = t.history(period="5d")["Close"]
            if len(hist)>0:
                p = float(hist.dropna().iloc[-1])
        return _coerce_float(p, np.nan)
    except Exception:
        return np.nan

def refresh_fx_table(fx_ws: gspread.Worksheet, currencies: List[str]) -> pd.DataFrame:
    currencies = sorted(list(set([c or "" for c in currencies if c])))
    rows = []
    for c in currencies:
        rate = _yahoo_fx_to_sek(c)
        rows.append({"Currency": c, "SEK_per_CCY": rate, "UpdatedAt": now_iso()})
        time.sleep(0.2)
    df = pd.DataFrame(rows)
    if not df.empty:
        _write_df_to_ws(fx_ws, df)
    return df

def load_fx_map(fx_ws: gspread.Worksheet) -> Dict[str, float]:
    df = _df_from_ws(fx_ws)
    if df.empty:
        return {"SEK":1.0, "USD":_yahoo_fx_to_sek("USD")}
    out = {}
    for _, r in df.iterrows():
        out[str(r.get("Currency","")).upper()] = _coerce_float(r.get("SEK_per_CCY"), np.nan)
    if "SEK" not in out: out["SEK"] = 1.0
    return out

# ============== Yahoo-hämtning (robust) ==============

def _sum_last(series_like, n=4):
    try:
        s = pd.Series(series_like).dropna().astype(float)
        if s.empty: return np.nan
        return float(s.tail(n).sum())
    except Exception:
        return np.nan

def _ttm_from_quarterly(df: pd.DataFrame, row_names: List[str]) -> float:
    if not isinstance(df, pd.DataFrame) or df.empty: return np.nan
    tgt = None
    for r in row_names:
        if r in df.index:
            tgt = r; break
    if not tgt: return np.nan
    try:
        row = df.loc[tgt]
        return _sum_last(row.values, 4)
    except Exception:
        return np.nan

def _fix_eps_units(price, eps, pe_ttm):
    """Om pris/eps ≈ 100*pe_ttm → eps var i cent, skala ner 100x."""
    try:
        price = float(price); eps = float(eps); pe_ttm = float(pe_ttm)
        if eps == 0: return eps
        calc = price/eps
        if pe_ttm>0 and abs(calc/pe_ttm) > 50 and abs(calc/pe_ttm) < 150:
            return eps/100.0
        return eps
    except Exception:
        return eps

@st.cache_data(ttl=60*10, show_spinner=False)
def fetch_yahoo_basics(ticker: str) -> dict:
    out = {
        "ticker": ticker, "name": ticker, "currency": None, "price": np.nan,
        "market_cap": np.nan, "ev": np.nan, "shares_out": np.nan,
        "rev_ttm": np.nan, "ebitda_ttm": np.nan, "eps_ttm": np.nan,
        "pe_ttm": np.nan, "pe_fwd": np.nan, "ev_rev": np.nan, "ev_ebitda": np.nan,
        "pb": np.nan, "dividend_ttm": np.nan,
    }
    try:
        tk = yf.Ticker(ticker)
        fi = tk.fast_info or {}

        # Pris
        price = _coerce_float(fi.get("last_price"))
        if not price or np.isnan(price):
            try:
                hist = tk.history(period="5d")["Close"]
                if len(hist)>0:
                    price = float(hist.dropna().iloc[-1])
            except Exception:
                pass
        if not price or np.isnan(price):
            info_try = {}
            try: info_try = tk.get_info()
            except Exception: pass
            price = _coerce_float(info_try.get("currentPrice"))
        out["price"] = price

        # Valuta, namn
        info = {}
        try: info = tk.get_info()
        except Exception: pass
        out["currency"] = fi.get("currency") or info.get("currency") or "USD"
        out["name"] = info.get("shortName") or info.get("longName") or out["name"]

        # Aktier
        shares = _coerce_float(fi.get("shares"))
        if not shares or np.isnan(shares):
            shares = _coerce_float(info.get("sharesOutstanding"))
        out["shares_out"] = shares

        # Market cap
        mcap = _coerce_float(info.get("marketCap"))
        if (not mcap or np.isnan(mcap)) and (price and shares):
            mcap = float(price)*float(shares)
        out["market_cap"] = mcap

        # EV
        ev = _coerce_float(info.get("enterpriseValue"))
        if not ev or np.isnan(ev):
            total_debt = _coerce_float(info.get("totalDebt"), 0.0)
            cash = _coerce_float(info.get("totalCash"), 0.0)
            if mcap and not np.isnan(mcap):
                ev = float(mcap) + float(total_debt) - float(cash)
        out["ev"] = ev

        # P/B
        pb = _coerce_float(info.get("priceToBook"))
        if (not pb or np.isnan(pb)) and price:
            bvps = _coerce_float(info.get("bookValue"), np.nan)
            if bvps and not np.isnan(bvps):
                pb = float(price)/float(bvps)
        out["pb"] = pb

        # TTM från quarterly
        q_inc = None
        try: q_inc = tk.quarterly_income_stmt
        except Exception:
            try: q_inc = tk.get_income_stmt(trailing=True)
            except Exception: q_inc = None

        rev_ttm = _ttm_from_quarterly(q_inc, ["TotalRevenue","Total Revenue"])
        ebitda_ttm = _ttm_from_quarterly(q_inc, ["EBITDA"])
        ni_ttm = _ttm_from_quarterly(q_inc, ["NetIncome","Net Income"])

        if (not rev_ttm or np.isnan(rev_ttm)):
            try:
                inc = tk.income_stmt
                if isinstance(inc, pd.DataFrame) and not inc.empty:
                    rn = "TotalRevenue" if "TotalRevenue" in inc.index else ("Total Revenue" if "Total Revenue" in inc.index else None)
                    if rn:
                        rev_ttm = _coerce_float(inc.loc[rn].dropna().astype(float).iloc[0])
            except Exception:
                pass

        if (not ebitda_ttm or np.isnan(ebitda_ttm)):
            try:
                inc = tk.income_stmt
                if isinstance(inc, pd.DataFrame) and not inc.empty and "EBITDA" in inc.index:
                    ebitda_ttm = _coerce_float(inc.loc["EBITDA"].dropna().astype(float).iloc[0])
            except Exception:
                pass

        if (not ni_ttm or np.isnan(ni_ttm)):
            try:
                inc = tk.income_stmt
                if isinstance(inc, pd.DataFrame) and not inc.empty:
                    rn = "NetIncome" if "NetIncome" in inc.index else ("Net Income" if "Net Income" in inc.index else None)
                    if rn:
                        ni_ttm = _coerce_float(inc.loc[rn].dropna().astype(float).iloc[0])
            except Exception:
                pass

        out["rev_ttm"] = rev_ttm
        out["ebitda_ttm"] = ebitda_ttm

        # EPS TTM
        eps_ttm = np.nan
        if ni_ttm and shares and shares>0:
            eps_ttm = float(ni_ttm)/float(shares)
        out["eps_ttm"] = eps_ttm

        # PE TTM & FWD
        pe_ttm = _coerce_float(info.get("trailingPE"))
        pe_fwd = _coerce_float(info.get("forwardPE"))
        if (not pe_ttm or np.isnan(pe_ttm)) and (price and eps_ttm and eps_ttm!=0):
            pe_ttm = float(price)/float(eps_ttm)
        # Sanity: EPS skalning
        if price and pe_ttm and eps_ttm:
            eps_ttm = _fix_eps_units(price, eps_ttm, pe_ttm)
            out["eps_ttm"] = eps_ttm
            # recalibrate if needed
            if (not info.get("trailingPE")):
                pe_ttm = float(price)/float(eps_ttm)
        out["pe_ttm"] = pe_ttm
        out["pe_fwd"] = pe_fwd

        # EV multiplar
        if ev and rev_ttm and rev_ttm>0:
            out["ev_rev"] = float(ev)/float(rev_ttm)
        if ev and ebitda_ttm and ebitda_ttm>0:
            out["ev_ebitda"] = float(ev)/float(ebitda_ttm)

        # Dividend TTM
        try:
            divs = yf.Ticker(ticker).dividends
            if divs is not None and len(divs)>0:
                last12 = divs[divs.index >= (divs.index.max() - pd.Timedelta(days=365))]
                out["dividend_ttm"] = float(last12.sum())
        except Exception:
            pass

    except Exception:
        pass

    time.sleep(0.3)
    return out

# ============== Estimat/CAGR ==============
def _cagr(start, end, years):
    try:
        start = float(start); end=float(end); years=float(years)
        if start<=0 or years<=0: return np.nan
        return (end/start)**(1/years) - 1.0
    except Exception:
        return np.nan

@st.cache_data(ttl=60*20, show_spinner=False)
def fetch_eps_rev_estimates(ticker: str, rev_cagr_default=0.10, eps_cagr_default=0.10) -> dict:
    out = {"eps1": np.nan, "eps2": np.nan, "eps3": np.nan,
           "rev1": np.nan, "rev2": np.nan, "rev3": np.nan,
           "g_rev": np.nan, "g_eps": np.nan}
    base = fetch_yahoo_basics(ticker)
    eps0 = _coerce_float(base.get("eps_ttm"))
    rev0 = _coerce_float(base.get("rev_ttm"))
    tk = yf.Ticker(ticker)

    # försök härleda g_rev & g_eps från kvartal
    g_rev = np.nan; g_eps = np.nan
    try:
        q = tk.quarterly_income_stmt
    except Exception:
        try: q = tk.get_income_stmt(trailing=True)
        except Exception: q = None
    shares = _coerce_float(base.get("shares_out"))

    if isinstance(q, pd.DataFrame) and not q.empty:
        # Revenue
        rn = "TotalRevenue" if "TotalRevenue" in q.index else ("Total Revenue" if "Total Revenue" in q.index else None)
        if rn:
            vals = pd.Series(q.loc[rn].dropna().astype(float))
            if len(vals)>=8:
                a = float(vals.iloc[-8:-4].sum())
                b = float(vals.iloc[-4:].sum())
                g_rev = _cagr(a, b, 1.0)
        # EPS
        rn2 = "NetIncome" if "NetIncome" in q.index else ("Net Income" if "Net Income" in q.index else None)
        if rn2 and shares and shares>0:
            vals = pd.Series(q.loc[rn2].dropna().astype(float)) / float(shares)
            if len(vals)>=8:
                a = float(vals.iloc[-8:-4].sum())
                b = float(vals.iloc[-4:].sum())
                g_eps = _cagr(a, b, 1.0)

    if np.isnan(g_rev): g_rev = rev_cagr_default
    if np.isnan(g_eps): g_eps = eps_cagr_default

    out["g_rev"] = g_rev; out["g_eps"] = g_eps

    # framåtblick (1,2,3 år)
    if rev0 and rev0>0:
        out["rev1"] = rev0*(1+g_rev)
        out["rev2"] = out["rev1"]*(1+g_rev)
        out["rev3"] = out["rev2"]*(1+g_rev)
    if eps0 and not np.isnan(eps0):
        out["eps1"] = eps0*(1+g_eps)
        out["eps2"] = out["eps1"]*(1+g_eps)
        out["eps3"] = out["eps2"]*(1+g_eps)
    return out

# ============== Avancerade manuella fält ==============
def load_advanced_map(adv_ws: gspread.Worksheet) -> Dict[str, Dict[str, float]]:
    df = _df_from_ws(adv_ws)
    if df.empty: return {}
    # förväntade kolumner: Ticker, BVPS, NAVPS, TBVPS, AFFOPS, NIIps, DACFps, FCFps, FCF_margin
    out = {}
    for _, r in df.iterrows():
        t = str(r.get("Ticker","")).strip().upper()
        out[t] = {
            "BVPS": _coerce_float(r.get("BVPS")),
            "NAVPS": _coerce_float(r.get("NAVPS")),
            "TBVPS": _coerce_float(r.get("TBVPS")),
            "AFFOPS": _coerce_float(r.get("AFFOPS")),
            "NIIps": _coerce_float(r.get("NIIps")),
            "DACFps": _coerce_float(r.get("DACFps")),
            "FCFps": _coerce_float(r.get("FCFps")),
            "FCF_margin": _coerce_float(r.get("FCF_margin")),
        }
    return out

# ============== Värderingsmetoder ==============
def _apply_multiple_drift(mult0, years, drift_per_year):
    if mult0 is None or np.isnan(mult0): return np.nan
    try:
        return float(mult0) * (1.0 + float(drift_per_year))**years
    except Exception:
        return np.nan

def build_valuations(ticker: str,
                     base: dict,
                     est: dict,
                     adv: Dict[str, float],
                     settings: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Returnerar:
      - DataFrame med kolumner: Metod, Idag, 1 år, 2 år, 3 år
      - meta: dict (ankare, sanity, etc)
    """
    price = _coerce_float(base.get("price"))
    ccy = base.get("currency") or "USD"
    eps0 = _coerce_float(base.get("eps_ttm"))
    pe_ttm = _coerce_float(base.get("pe_ttm"))
    pe_fwd = _coerce_float(base.get("pe_fwd"))
    ev_rev = _coerce_float(base.get("ev_rev"))
    ev_ebitda = _coerce_float(base.get("ev_ebitda"))
    pb = _coerce_float(base.get("pb"))
    ev = _coerce_float(base.get("ev"))
    mcap = _coerce_float(base.get("market_cap"))
    shares = _coerce_float(base.get("shares_out"))

    # framåtblickande nivåer
    eps1, eps2, eps3 = est.get("eps1"), est.get("eps2"), est.get("eps3")
    rev1, rev2, rev3 = est.get("rev1"), est.get("rev2"), est.get("rev3")

    # multipel-ankare för PE (p50 ~ mitten mellan pe_ttm och pe_fwd om båda finns)
    anchor_pe = None
    if pe_ttm and pe_fwd and not np.isnan(pe_ttm) and not np.isnan(pe_fwd):
        anchor_pe = 0.5*(pe_ttm + pe_fwd)
    elif pe_ttm:
        anchor_pe = pe_ttm
    elif pe_fwd:
        anchor_pe = pe_fwd

    drift = _coerce_float(settings.get("mult_drift_per_year"), DEFAULT_MULT_DRIFT)

    rows = []
    meta = {"pe_anchor": anchor_pe, "ccy": ccy}

    # --- pe_hist_vs_eps (fair value pris = eps * multipel)
    def _pe_price(eps, mult):
        if eps is None or np.isnan(eps) or mult is None or np.isnan(mult):
            return np.nan
        return float(eps)*float(mult)

    pe0 = anchor_pe
    pe1 = _apply_multiple_drift(pe0, 1, drift)
    pe2 = _apply_multiple_drift(pe0, 2, drift)
    pe3 = _apply_multiple_drift(pe0, 3, drift)

    rows.append(dict(Metod="pe_hist_vs_eps",
                     Idag=_pe_price(eps0, pe0),
                     **{"1 år": _pe_price(eps1, pe1),
                        "2 år": _pe_price(eps2, pe2),
                        "3 år": _pe_price(eps3, pe3)}))

    # --- ev_sales: target EV = mult * Revenue; target Price = (EV + cash - debt)/shares approx EV→MCAP
    def _ev_from_mult_on_rev(mult, rev):
        if not mult or np.isnan(mult) or not rev or np.isnan(rev): return np.nan
        return float(mult)*float(rev)
    def _price_from_ev(target_ev):
        if target_ev is None or np.isnan(target_ev): return np.nan
        # MCAP ≈ EV om (Debt - Cash) liten, annars använd baseline för att justera
        if ev and mcap and not np.isnan(ev) and not np.isnan(mcap) and ev>0:
            mc_ratio = mcap/ev
        else:
            mc_ratio = 1.0
        target_mcap = target_ev * mc_ratio
        if shares and shares>0:
            return target_mcap/float(shares)
        return np.nan

    evs0 = ev_rev
    evs1 = _apply_multiple_drift(evs0, 1, drift)
    evs2 = _apply_multiple_drift(evs0, 2, drift)
    evs3 = _apply_multiple_drift(evs0, 3, drift)
    rows.append(dict(Metod="ev_sales",
                     Idag=_price_from_ev(_ev_from_mult_on_rev(evs0, base.get("rev_ttm"))),
                     **{"1 år": _price_from_ev(_ev_from_mult_on_rev(evs1, rev1)),
                        "2 år": _price_from_ev(_ev_from_mult_on_rev(evs2, rev2)),
                        "3 år": _price_from_ev(_ev_from_mult_on_rev(evs3, rev3))}))

    # --- ev_ebitda
    e0 = ev_ebitda
    e1 = _apply_multiple_drift(e0, 1, drift)
    e2 = _apply_multiple_drift(e0, 2, drift)
    e3 = _apply_multiple_drift(e0, 3, drift)

    # Approx EBITDAs från rev via marginal om ej tillgängligt: använd Avancerat["EBITDA_margin"] via FCF_margin som proxy
    ebitda_ttm = _coerce_float(base.get("ebitda_ttm"))
    if (not ebitda_ttm or np.isnan(ebitda_ttm)) and base.get("rev_ttm") and adv.get("FCF_margin"):
        ebitda_ttm = float(base["rev_ttm"])*float(adv["FCF_margin"])

    def _price_from_e_mult(mult, e_level):
        if not mult or np.isnan(mult) or not e_level or np.isnan(e_level): return np.nan
        target_ev = float(mult)*float(e_level)
        return _price_from_ev(target_ev)

    # uppskatta framtida EBITDA proportionellt mot revenue om saknas
    def _ebitda_from_rev(rev):
        if not rev or np.isnan(rev): return np.nan
        if ebitda_ttm and base.get("rev_ttm") and base["rev_ttm"]>0:
            margin = ebitda_ttm/base["rev_ttm"]
            return rev*margin
        return np.nan

    rows.append(dict(Metod="ev_ebitda",
                     Idag=_price_from_e_mult(e0, ebitda_ttm),
                     **{"1 år": _price_from_e_mult(e1, _ebitda_from_rev(rev1)),
                        "2 år": _price_from_e_mult(e2, _ebitda_from_rev(rev2)),
                        "3 år": _price_from_e_mult(e3, _ebitda_from_rev(rev3))}))

    # --- p_b (pris = BVPS * P/B)
    bvps = adv.get("BVPS", np.nan)
    rows.append(dict(Metod="p_b",
                     Idag=(bvps*pb if bvps and pb else np.nan),
                     **{"1 år": bvps*_apply_multiple_drift(pb,1,drift) if bvps and pb else np.nan,
                        "2 år": bvps*_apply_multiple_drift(pb,2,drift) if bvps and pb else np.nan,
                        "3 år": bvps*_apply_multiple_drift(pb,3,drift) if bvps and pb else np.nan}))

    # --- p_tbv (för banker/finans)
    tbvps = adv.get("TBVPS", np.nan)
    rows.append(dict(Metod="p_tbv",
                     Idag=(tbvps*pb if tbvps and pb else np.nan),
                     **{"1 år": tbvps*_apply_multiple_drift(pb,1,drift) if tbvps and pb else np.nan,
                        "2 år": tbvps*_apply_multiple_drift(pb,2,drift) if tbvps and pb else np.nan,
                        "3 år": tbvps*_apply_multiple_drift(pb,3,drift) if tbvps and pb else np.nan}))

    # --- p_nav (REIT/CEF/Shipping etc – kräver NAVPS)
    navps = adv.get("NAVPS", np.nan)
    # approximera P/NAV via P/B om NAV saknas och sektor inte REIT: vi lämnar NaN (manuell inmatning rekommenderas)
    p_nav_mult = None
    if navps and price:
        p_nav_mult = price/navps
    rows.append(dict(Metod="p_nav",
                     Idag=(navps*p_nav_mult if navps and p_nav_mult else np.nan),
                     **{"1 år": navps*_apply_multiple_drift(p_nav_mult,1,drift) if navps and p_nav_mult else np.nan,
                        "2 år": navps*_apply_multiple_drift(p_nav_mult,2,drift) if navps and p_nav_mult else np.nan,
                        "3 år": navps*_apply_multiple_drift(p_nav_mult,3,drift) if navps and p_nav_mult else np.nan}))

    # --- p_affo (REIT): pris = AFFO/share * P/AFFO (här approx med PE om inget finns)
    affops = adv.get("AFFOPS", np.nan)
    p_affo_mult0 = pe_ttm if pe_ttm and not np.isnan(pe_ttm) else None
    rows.append(dict(Metod="p_affo",
                     Idag=(affops*p_affo_mult0 if affops and p_affo_mult0 else np.nan),
                     **{"1 år": affops*_apply_multiple_drift(p_affo_mult0,1,drift) if affops and p_affo_mult0 else np.nan,
                        "2 år": affops*_apply_multiple_drift(p_affo_mult0,2,drift) if affops and p_affo_mult0 else np.nan,
                        "3 år": affops*_apply_multiple_drift(p_affo_mult0,3,drift) if affops and p_affo_mult0 else np.nan}))

    # --- p_nii (BDC/finans): pris = NII/share * P/NII (ankare ≈ PE)
    niips = adv.get("NIIps", np.nan)
    p_nii_mult0 = pe_ttm if pe_ttm and not np.isnan(pe_ttm) else None
    rows.append(dict(Metod="p_nii",
                     Idag=(niips*p_nii_mult0 if niips and p_nii_mult0 else np.nan),
                     **{"1 år": niips*_apply_multiple_drift(p_nii_mult0,1,drift) if niips and p_nii_mult0 else np.nan,
                        "2 år": niips*_apply_multiple_drift(p_nii_mult0,2,drift) if niips and p_nii_mult0 else np.nan,
                        "3 år": niips*_apply_multiple_drift(p_nii_mult0,3,drift) if niips and p_nii_mult0 else np.nan}))

    # --- ev_dacf (oil&gas/shipping): pris ≈ DACFps * EV/DACF → prox med EV/EBITDA mult och DACFps från Avancerat
    dacfps = adv.get("DACFps", np.nan)
    rows.append(dict(Metod="ev_dacf",
                     Idag=(dacfps*ev_ebitda if dacfps and ev_ebitda else np.nan),
                     **{"1 år": dacfps*_apply_multiple_drift(ev_ebitda,1,drift) if dacfps and ev_ebitda else np.nan,
                        "2 år": dacfps*_apply_multiple_drift(ev_ebitda,2,drift) if dacfps and ev_ebitda else np.nan,
                        "3 år": dacfps*_apply_multiple_drift(ev_ebitda,3,drift) if dacfps and ev_ebitda else np.nan}))

    # --- p_fcf / ev_fcf (kräver FCFps eller FCF_margin)
    fcfps = adv.get("FCFps", np.nan)
    if (not fcfps or np.isnan(fcfps)) and base.get("rev_ttm") and shares and adv.get("FCF_margin"):
        fcf_total = base["rev_ttm"]*adv["FCF_margin"]
        fcfps = fcf_total/shares

    # proxy: p_fcf multiple ≈ PE
    rows.append(dict(Metod="p_fcf",
                     Idag=(fcfps*pe_ttm if fcfps and pe_ttm else np.nan),
                     **{"1 år": fcfps*_apply_multiple_drift(pe_ttm,1,drift) if fcfps and pe_ttm else np.nan,
                        "2 år": fcfps*_apply_multiple_drift(pe_ttm,2,drift) if fcfps and pe_ttm else np.nan,
                        "3 år": fcfps*_apply_multiple_drift(pe_ttm,3,drift) if fcfps and pe_ttm else np.nan}))

    # ev_fcf = FCF * EV/FCF, approximera EV/FCF ≈ EV/EBITDA multipel
    rows.append(dict(Metod="ev_fcf",
                     Idag=(fcfps*ev_ebitda if fcfps and ev_ebitda else np.nan),
                     **{"1 år": fcfps*_apply_multiple_drift(ev_ebitda,1,drift) if fcfps and ev_ebitda else np.nan,
                        "2 år": fcfps*_apply_multiple_drift(ev_ebitda,2,drift) if fcfps and ev_ebitda else np.nan,
                        "3 år": fcfps*_apply_multiple_drift(ev_ebitda,3,drift) if fcfps and ev_ebitda else np.nan}))

    df = pd.DataFrame(rows)
    return df, meta

# ============== Läs Data-fliken och normalisera ==============
def load_data_df(data_ws: gspread.Worksheet) -> pd.DataFrame:
    df = _df_from_ws(data_ws)
    # förväntade kolumner minst: Ticker, Bolagsnamn, Sektor, Valuta, Antal aktier, GAV (SEK), Bucket, Primär metod (valfritt)
    if df.empty:
        seed = pd.DataFrame({
            "Ticker": ["NVDA"],
            "Bolagsnamn": ["NVIDIA Corporation"],
            "Sektor": ["Tech"],
            "Valuta": ["USD"],
            "Antal aktier": [0],
            "GAV (SEK)": [0],
            "Bucket": ["Bucket A tillväxt"],
            "Primär metod": [""],
        })
        _write_df_to_ws(data_ws, seed)
        df = seed.copy()

    # coerces
    num_cols = ["Antal aktier", "GAV (SEK)"]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].apply(_coerce_float)
    if "Bucket" not in df.columns:
        df["Bucket"] = ""
    if "Primär metod" not in df.columns:
        df["Primär metod"] = ""
    df["Ticker"] = df["Ticker"].astype(str).str.upper().str.strip()
    if "Valuta" in df.columns:
        df["Valuta"] = df["Valuta"].astype(str).str.upper().str.strip()
    return df

# ============== Källskatt-karta ==============
def withholding_map(settings: Dict[str, Any]) -> Dict[str, float]:
    lst = settings.get("withholding", [])
    out = {}
    for it in lst:
        c = (it.get("currency") or "").upper()
        p = _coerce_float(it.get("pct"), 0.0)
        if c: out[c] = float(p)
    if "SEK" not in out: out["SEK"] = 0.0
    return out

# ============== Dividend-prognos ==============
def forecast_dividends(base: dict, ccy: str, fx_map: Dict[str,float], settings: Dict[str,Any],
                       shares_owned: float) -> Dict[str, float]:
    """
    Årlig utdelning i SEK (netto), nästa år, 2, 3 år.
    CAGR från historiska utdelningar om möjligt, annars default i settings.
    """
    dps0 = _coerce_float(base.get("dividend_ttm"), 0.0)  # per aktie i bolagets valuta
    # CAGR: försök på 3 år historik
    div_cagr_default = _coerce_float(settings.get("dividend_cagr_default"), 0.03)
    cagr = div_cagr_default
    try:
        divs = yf.Ticker(base.get("ticker")).dividends
        if divs is not None and len(divs)>0:
            by_year = divs.groupby(divs.index.year).sum()
            if len(by_year)>=3:
                start = float(by_year.iloc[-3])
                end = float(by_year.iloc[-1])
                c = _cagr(start, end, 2.0)
                if not np.isnan(c): cagr = c
    except Exception:
        pass
    d1 = dps0*(1+cagr); d2 = d1*(1+cagr); d3 = d2*(1+cagr)
    fx = fx_map.get(ccy.upper(), np.nan)
    wmap = withholding_map(settings)
    tax = wmap.get(ccy.upper(), 0.0)/100.0
    def _net_sek(x):
        if np.isnan(x) or np.isnan(fx): return 0.0
        brutto = x*shares_owned*fx
        return brutto*(1.0 - tax)
    return {"y1": _net_sek(d1), "y2": _net_sek(d2), "y3": _net_sek(d3), "tax_pct": tax*100}

# ============== Primär metod ==============
def pick_primary_method(bucket: str, explicit: str, methods_df: pd.DataFrame) -> str:
    if explicit:
        return explicit
    return BUCKET_PRIMARY.get(bucket, "ev_ebitda")

def extract_primary_prices(methods_df: pd.DataFrame, method_name: str) -> Dict[str, float]:
    row = methods_df[methods_df["Metod"]==method_name]
    if row.empty: return {"today": np.nan, "y1": np.nan, "y2": np.nan, "y3": np.nan}
    r = row.iloc[0]
    return {"today": _coerce_float(r["Idag"]), "y1": _coerce_float(r["1 år"]),
            "y2": _coerce_float(r["2 år"]), "y3": _coerce_float(r["3 år"])}

# ============== Ranking ==============
def build_ranking(df_data: pd.DataFrame, settings: Dict[str,Any],
                  fx_map: Dict[str,float]) -> pd.DataFrame:
    rows = []
    for _, row in df_data.iterrows():
        ticker = str(row["Ticker"]).upper().strip()
        bucket = str(row.get("Bucket",""))
        explicit = str(row.get("Primär metod","")).strip()
        base = fetch_yahoo_basics(ticker)
        adv_map = load_advanced_map(ensure_sheets()["adv_ws"])
        adv = adv_map.get(ticker, {})
        est = fetch_eps_rev_estimates(ticker,
                                      settings.get("rev_cagr_default",0.10),
                                      settings.get("eps_cagr_default",0.10))
        methods, meta = build_valuations(ticker, base, est, adv, settings)
        prim = pick_primary_method(bucket, explicit, methods)
        prices = extract_primary_prices(methods, prim)
        price_now = _coerce_float(base.get("price"))
        up = np.nan
        if prices["today"] and price_now:
            up = (prices["today"]/price_now - 1.0)*100.0
        rows.append({
            "Ticker": ticker,
            "Namn": base.get("name"),
            "Bucket": bucket,
            "Valuta": base.get("currency",""),
            "Pris": price_now,
            "Primär metod": prim,
            "FV idag": prices["today"],
            "Uppsida idag (%)": up
        })
        time.sleep(0.15)
    r = pd.DataFrame(rows)
    r = r.sort_values(by="Uppsida idag (%)", ascending=False, na_position="last").reset_index(drop=True)
    return r

# ============== Lagring: Resultat + Snapshots ==============
def save_primary_targets(res_ws: gspread.Worksheet, row_ctx: Dict[str, Any]):
    df_old = _df_from_ws(res_ws)
    new_row = pd.DataFrame([row_ctx])
    df = pd.concat([df_old, new_row], ignore_index=True) if not df_old.empty else new_row
    _write_df_to_ws(res_ws, df)

def save_quarter_snapshot(snap_ws: gspread.Worksheet, ticker: str, methods_df: pd.DataFrame, meta: Dict[str,Any]):
    methods_df = methods_df.copy()
    methods_df.insert(0, "Ticker", ticker)
    methods_df.insert(1, "Timestamp", now_iso())
    methods_df.insert(2, "Meta", json.dumps(meta))
    df_old = _df_from_ws(snap_ws)
    df = pd.concat([df_old, methods_df], ignore_index=True) if not df_old.empty else methods_df
    _write_df_to_ws(snap_ws, df)

# ============== UI: detaljvy för valt bolag ==============
def show_company_panel(row: pd.Series, fx_map: Dict[str,float], settings: Dict[str,Any], sheets: Dict[str,Any]):
    ticker = str(row["Ticker"]).upper().strip()
    st.subheader(f"{ticker} • {row.get('Bolagsnamn','')} • {row.get('Bucket','')}")
    base = fetch_yahoo_basics(ticker)
    adv_map = load_advanced_map(sheets["adv_ws"])
    adv = adv_map.get(ticker, {})
    est = fetch_eps_rev_estimates(ticker,
                                  settings.get("rev_cagr_default",0.10),
                                  settings.get("eps_cagr_default",0.10))
    methods, meta = build_valuations(ticker, base, est, adv, settings)
    ccy = base.get("currency","")
    price_now = _coerce_float(base.get("price"))

    # Primär metod
    explicit = str(row.get("Primär metod",""))
    prim = pick_primary_method(row.get("Bucket",""), explicit, methods)
    prices = extract_primary_prices(methods, prim)

    # Visa “Sanity”
    st.caption(f"Sanity: pris={_fmt_money(price_now, ccy)}, pe_ttm={_fmt_num(base.get('pe_ttm'))}, pe_fwd={_fmt_num(base.get('pe_fwd'))}, ev/s={_fmt_num(base.get('ev_rev'))}, ev/ebitda={_fmt_num(base.get('ev_ebitda'))}")

    # Tabell alla metoder (snyggt)
    mdisp = methods.copy()
    mdisp = mdisp.replace({None: np.nan}).fillna("")
    st.dataframe(mdisp, use_container_width=True)

    # 🎯 Primär riktkurs – idag/1/2/3 år
    st.markdown("### 🎯 Primär riktkurs")
    cols = st.columns(4)
    cols[0].metric("Idag", _fmt_money(prices["today"], ccy), delta=f"{((prices['today']/price_now - 1)*100):.1f}%" if prices["today"] and price_now else None)
    cols[1].metric("1 år", _fmt_money(prices["y1"], ccy))
    cols[2].metric("2 år", _fmt_money(prices["y2"], ccy))
    cols[3].metric("3 år", _fmt_money(prices["y3"], ccy))

    # 💰 Utdelning (netto, SEK)
    st.markdown("### 💰 Utdelning (netto, SEK)")
    owns = _coerce_float(row.get("Antal aktier"), 0.0)
    divf = forecast_dividends(base, ccy, fx_map, settings, owns)
    st.write(f"• Nästa år: **{_fmt_money(divf['y1'], 'SEK')}**")
    st.write(f"• 2 år: **{_fmt_money(divf['y2'], 'SEK')}**")
    st.write(f"• 3 år: **{_fmt_money(divf['y3'], 'SEK')}**")
    st.caption(f"Källskatt: {divf['tax_pct']:.0f}% • Antal aktier: {int(owns)}")

    # 🧾 Innehavsvärde (SEK)
    st.markdown("### 🧾 Innehavsvärde")
    fx = fx_map.get(ccy.upper(), np.nan)
    port_val = 0.0
    if price_now and owns and fx and not np.isnan(fx):
        port_val = price_now*owns*fx
    st.write(f"Totalt värde nu: **{_fmt_money(port_val,'SEK')}**")

    # Spara-knappar
    c1, c2 = st.columns(2)
    if c1.button("💾 Spara primära riktkurser till Resultat"):
        row_ctx = {
            "Timestamp": now_iso(),
            "Ticker": ticker,
            "Namn": base.get("name"),
            "Valuta": ccy,
            "Pris nu": price_now,
            "Primär metod": prim,
            "FV idag": prices["today"],
            "FV 1 år": prices["y1"],
            "FV 2 år": prices["y2"],
            "FV 3 år": prices["y3"],
        }
        save_primary_targets(sheets["res_ws"], row_ctx)
        st.success("Sparat till fliken Resultat.")

    if c2.button("🗂️ Spara kvartalssnapshot"):
        save_quarter_snapshot(sheets["snap_ws"], ticker, methods, meta)
        st.success("Snapshot sparad.")

# ============== UI: Ranking & filter ==============
def show_ranking(df_data: pd.DataFrame, settings: Dict[str,Any], fx_map: Dict[str,float]):
    st.markdown("## 🏁 Rangordning (störst uppsida →)")
    bucket_filter = st.selectbox("Filter: Bucket", options=["(alla)"] + sorted(df_data["Bucket"].dropna().unique().tolist()))
    df = df_data.copy()
    if bucket_filter != "(alla)":
        df = df[df["Bucket"]==bucket_filter]
    if df.empty:
        st.info("Inga rader matchar.")
        return
    with st.spinner("Beräknar uppsida…"):
        rank = build_ranking(df, settings, fx_map)
    st.dataframe(rank, use_container_width=True)

# ============== Run Main UI ==============
def run_main_ui():
    sheets = ensure_sheets()
    data_df = load_data_df(sheets["data_ws"])
    settings = _load_settings(sheets["set_ws"])

    # uppdatera FX om vi saknar rate för någon av valutor i Data
    currencies = data_df["Valuta"].dropna().astype(str).str.upper().unique().tolist()
    fx_df = _df_from_ws(sheets["fx_ws"])
    known = set(fx_df["Currency"].str.upper().tolist()) if not fx_df.empty else set()
    missing = [c for c in currencies if c not in known]
    if missing:
        with st.spinner("Hämtar valutakurser…"):
            refresh_fx_table(sheets["fx_ws"], currencies)
    fx_map = load_fx_map(sheets["fx_ws"])

    # Välj ticker
    st.sidebar.header("⚙️ Val")
    tickers = data_df["Ticker"].astype(str).tolist()
    ix = st.sidebar.selectbox("Välj rad", options=list(range(len(tickers))), format_func=lambda i: f"{tickers[i]} • {data_df.loc[i,'Bolagsnamn']}")
    row = data_df.loc[ix]

    # Visa ranking
    show_ranking(data_df, settings, fx_map)
    st.markdown("---")

    # Detaljpanel
    show_company_panel(row, fx_map, settings, sheets)

# ============== main() ==============
def main():
    try:
        run_main_ui()
    except Exception as e:
        st.error(f"💥 Fel: {e}")

if __name__ == "__main__":
    main()
