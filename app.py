# app.py — Del 1/4
from __future__ import annotations
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st
import gspread
import yfinance as yf
from google.oauth2.service_account import Credentials

# ============== Grundinställning ==============
st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")

# ============== Hjälpare ==============
def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _coerce_float(x, default=np.nan):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float, np.number)):
            return float(x)
        xs = str(x).strip().replace(" ", "").replace("%", "").replace(",", ".")
        if xs in ("", "-", "nan", "NaN", "None"):
            return default
        return float(xs)
    except Exception:
        return default

def _coerce_float_series(s: pd.Series) -> pd.Series:
    return s.apply(_coerce_float)

# ============== Secrets & klienter ==============
@st.cache_resource(show_spinner=False)
def _normalize_private_key(creds: Dict) -> Dict:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    creds = st.secrets.get("GOOGLE_CREDENTIALS", {})
    if not creds:
        st.stop()
    creds = _normalize_private_key(dict(creds))
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return gspread.authorize(Credentials.from_service_account_info(creds, scopes=scope))

@st.cache_resource(show_spinner=False)
def open_spreadsheet(_gc: gspread.Client, _sheet_url_or_id: str):
    s = (_sheet_url_or_id or "").strip()
    if not s:
        raise RuntimeError("SHEET_URL saknas i secrets.")
    if "/d/" in s:
        return _gc.open_by_url(s)
    return _gc.open_by_key(s)

SHEET_URL = st.secrets.get("SHEET_URL", "").strip()
GC = get_gspread_client()
SS = open_spreadsheet(GC, SHEET_URL)

def _get_or_create_ws(ss: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=4000, cols=100)

DATA_WS = _get_or_create_ws(SS, "Data")
FX_WS   = _get_or_create_ws(SS, "Valutakurser")
RES_WS  = _get_or_create_ws(SS, "Resultat")
SET_WS  = _get_or_create_ws(SS, "Settings")
SNAP_WS = _get_or_create_ws(SS, "Snapshots")

# ============== Kolumner som vi stödjer ==============
DATA_COLS = [
    "Ticker","Namn","Bucket","Valuta","Antal aktier","GAV (SEK)",
    "Price","MarketCap","EV","Revenue_TTM","EBITDA_TTM","EPS_TTM",
    "PE_TTM","PE_FWD","EV_Sales","EV_EBITDA","P_B","Shares_Out",
    "BVPS","NAVPS","AFFOPS","NIIps","TBVPS",
    "Dividend_TTM","Div_Currency","Senast auto uppdaterad"
]

RESULT_COLS = [
    "Timestamp","Ticker","Valuta","Aktuell kurs (0)",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    "Bull 1 år","Bear 1 år","Metod","Input-sammanfattning","Kommentar"
]

SET_COLS = [
    "Valuta","Källskatt (%)",
    "Kompression år1 (%)","Kompression år2 (%)","Kompression år3 (%)",
    "PE_vikt_hist (%)"
]

SNAP_COLS = [
    "Timestamp","Ticker","Namn","Bucket","Valuta","Pris","PE_TTM","PE_FWD",
    "EV_Sales","EV_EBITDA","P_B","Revenue_TTM","EBITDA_TTM","EPS_TTM","EV","MarketCap"
]

def _ensure_cols(ws: gspread.Worksheet, cols: List[str]) -> pd.DataFrame:
    df = pd.DataFrame(ws.get_all_records())
    changed = False
    if df.empty:
        df = pd.DataFrame(columns=cols); changed = True
    else:
        for c in cols:
            if c not in df.columns:
                df[c] = ""
                changed = True
        # håll ordningen: kända först
        known = [c for c in cols]
        unknown = [c for c in df.columns if c not in cols]
        df = df[known + unknown]
    if changed:
        ws.clear()
        ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
    return df

DATA_DF = _ensure_cols(DATA_WS, DATA_COLS)
_ensure_cols(FX_WS,   ["Valuta","Mot","Kurs","Timestamp"])
_ensure_cols(RES_WS,  RESULT_COLS)
SET_DF  = _ensure_cols(SET_WS,  SET_COLS)
_ensure_cols(SNAP_WS, SNAP_COLS)

def _seed_settings_if_empty():
    df = pd.DataFrame(SET_WS.get_all_records())
    if df.empty or df["Valuta"].isna().all():
        base = pd.DataFrame({
            "Valuta":["USD","NOK","CAD","EUR","SEK","GBP"],
            "Källskatt (%)":[15,25,15,15,0,0],
            "Kompression år1 (%)":[6,6,6,6,6,6],
            "Kompression år2 (%)":[10,10,10,10,10,10],
            "Kompression år3 (%)":[15,15,15,15,15,15],
            "PE_vikt_hist (%)":[50,50,50,50,50,50],
        })
        SET_WS.clear()
        SET_WS.update([base.columns.tolist()] + base.astype(str).values.tolist())

_seed_settings_if_empty()

# ============== FX → SEK ==============
@st.cache_data(ttl=60*30, show_spinner=False)
def fetch_fx_pairs() -> pd.DataFrame:
    pairs = [("USD","SEK"),("NOK","SEK"),("CAD","SEK"),("EUR","SEK"),("GBP","SEK")]
    rows = []
    for a,b in pairs:
        try:
            yft = f"{a}{b}=X"
            fi = yf.Ticker(yft).fast_info
            rate = _coerce_float(fi.get("last_price"))
            if rate and not np.isnan(rate):
                rows.append({"Valuta":a,"Mot":b,"Kurs":rate,"Timestamp":_now_iso()})
        except Exception:
            pass
        time.sleep(0.2)
    return pd.DataFrame(rows)

def sync_fx_sheet():
    df = fetch_fx_pairs()
    if not df.empty:
        FX_WS.clear()
        FX_WS.update([df.columns.tolist()] + df.astype(str).values.tolist())

def _fx_rate_to_sek(ccy: str) -> float:
    try:
        df = pd.DataFrame(FX_WS.get_all_records())
        if df.empty:
            return 1.0 if str(ccy).upper()=="SEK" else np.nan
        row = df.loc[df["Valuta"].astype(str).str.upper()==str(ccy).upper()]
        if row.empty:
            return 1.0 if str(ccy).upper()=="SEK" else np.nan
        r = _coerce_float(row.iloc[0].get("Kurs"), np.nan)
        if r and not np.isnan(r):
            return float(r)
    except Exception:
        pass
    return 1.0 if str(ccy).upper()=="SEK" else np.nan

def fx_to_sek(amount: float, ccy: str) -> float:
    r = _fx_rate_to_sek(ccy)
    if np.isnan(r):
        return 0.0
    return float(amount or 0.0) * r

# app.py — Del 2/4
# ====================== Yahoo & estimat ======================

def _fix_eps_units(price, eps, pe_ttm):
    """Rätta vanliga 10×/100×-fel i EPS mot PE/price."""
    if not price or not eps or not pe_ttm or pe_ttm <= 0:
        return eps
    try:
        implied = abs(price / eps)
        if implied > pe_ttm * 4:
            eps10 = eps * 10.0
            if abs(price / eps10) <= pe_ttm * 2:
                return eps10
            eps100 = eps * 100.0
            if abs(price / eps100) <= pe_ttm * 2:
                return eps100
        if implied < pe_ttm / 4:
            eps10 = eps / 10.0
            if abs(price / eps10) >= pe_ttm / 2:
                return eps10
            eps100 = eps / 100.0
            if abs(price / eps100) >= pe_ttm / 2:
                return eps100
    except Exception:
        pass
    return eps

@st.cache_data(ttl=60*10, show_spinner=False)
def fetch_yahoo_basics(ticker: str) -> dict:
    out = {
        "ticker": ticker, "name": ticker, "currency": None, "price": np.nan,
        "market_cap": np.nan, "ev": np.nan, "shares_out": np.nan,
        "rev_ttm": np.nan, "ebitda_ttm": np.nan, "eps_ttm": np.nan,
        "pe_ttm": np.nan, "pe_fwd": np.nan, "ev_rev": np.nan, "ev_ebitda": np.nan,
        "pb": np.nan, "dividend_ttm": np.nan, "div_ccy": None
    }
    try:
        tk = yf.Ticker(ticker)
        fi = tk.fast_info or {}
        out["price"] = _coerce_float(fi.get("last_price"))
        out["currency"] = fi.get("currency")
        out["shares_out"] = _coerce_float(fi.get("shares"))

        try:
            info = tk.get_info()
        except Exception:
            info = {}
        out["name"] = info.get("shortName") or info.get("longName") or out["name"]
        out["market_cap"] = _coerce_float(info.get("marketCap"))
        out["pb"] = _coerce_float(info.get("priceToBook") or info.get("priceToBookMRQ"))
        out["pe_ttm"] = _coerce_float(info.get("trailingPE"))
        out["pe_fwd"] = _coerce_float(info.get("forwardPE"))
        out["ev"] = _coerce_float(info.get("enterpriseValue"))
        out["dividend_ttm"] = _coerce_float(info.get("trailingAnnualDividendRate"))
        out["div_ccy"] = out["currency"]

        # TTM via statements
        try:
            inc = tk.get_income_stmt(trailing=True)
            if isinstance(inc, pd.DataFrame) and not inc.empty:
                rev = inc.get("TotalRevenue", pd.Series([np.nan])).iloc[0] if "TotalRevenue" in inc else np.nan
                ebt = inc.get("EBITDA", pd.Series([np.nan])).iloc[0] if "EBITDA" in inc else np.nan
                ni  = inc.get("NetIncome", pd.Series([np.nan])).iloc[0] if "NetIncome" in inc else np.nan
                out["rev_ttm"]    = _coerce_float(rev)
                out["ebitda_ttm"] = _coerce_float(ebt)
                sh = out["shares_out"]
                if sh and not np.isnan(sh) and ni is not None:
                    out["eps_ttm"] = _coerce_float(ni) / float(sh)
        except Exception:
            pass

        if (not out["ev"] or np.isnan(out["ev"])) and out["market_cap"]:
            debt = _coerce_float(info.get("totalDebt"), 0.0)
            cash = _coerce_float(info.get("totalCash"), 0.0)
            out["ev"] = float(out["market_cap"]) + float(debt) - float(cash)

        if out["ev"] and out["rev_ttm"]:
            out["ev_rev"] = out["ev"] / out["rev_ttm"]
        if out["ev"] and out["ebitda_ttm"]:
            out["ev_ebitda"] = out["ev"] / out["ebitda_ttm"]

        if out["price"] and out["pe_ttm"] and out["eps_ttm"]:
            out["eps_ttm"] = _fix_eps_units(out["price"], out["eps_ttm"], out["pe_ttm"])
    except Exception:
        pass

    time.sleep(0.4)
    return out

def _cagr(a: float, b: float, years: float) -> float:
    try:
        if a is None or b is None or a <= 0 or years <= 0:
            return np.nan
        return (b / a) ** (1.0 / years) - 1.0
    except Exception:
        return np.nan

@st.cache_data(ttl=60*30, show_spinner=False)
def fetch_eps_rev_estimates(ticker: str) -> dict:
    """
    Försök hämta EPS/revenue-estimat. Fallback till 2Y-CAGR från historik.
    Returnerar: eps1, eps2, rev1, rev2, g_rev, g_eps
    """
    out = {"eps1": np.nan, "eps2": np.nan, "rev1": np.nan, "rev2": np.nan,
           "g_rev": np.nan, "g_eps": np.nan}
    tk = yf.Ticker(ticker)
    # Revenue CAGR (2 år) från get_earnings (historik)
    try:
        hist = tk.get_earnings()
        if isinstance(hist, pd.DataFrame) and not hist.empty and "Revenue" in hist.columns and len(hist["Revenue"]) >= 3:
            rev_now = _coerce_float(hist["Revenue"].iloc[-1])
            rev_2y  = _coerce_float(hist["Revenue"].iloc[-3])
            out["g_rev"] = _cagr(rev_2y, rev_now, 2.0)
    except Exception:
        pass
    # EPS CAGR (approx från NetIncome/aktier)
    try:
        inc_hist = tk.get_income_stmt(trailing=False)
        if isinstance(inc_hist, pd.DataFrame) and not inc_hist.empty:
            ni = inc_hist.get("NetIncome")
            sh = _coerce_float(tk.fast_info.get("shares"))
            if ni is not None and sh and len(ni) >= 3:
                eps_now = _coerce_float(ni.iloc[0]) / sh
                eps_2y  = _coerce_float(ni.iloc[2]) / sh
                out["g_eps"] = _cagr(eps_2y, eps_now, 2.0)
    except Exception:
        pass
    time.sleep(0.2)
    return out

# ============== Settings & kompression ==============
def get_currency_settings(ccy: str) -> dict:
    df = pd.DataFrame(SET_WS.get_all_records())
    row = df.loc[df["Valuta"].astype(str).str.upper() == str(ccy).upper()]
    if row.empty:
        return {"withholding": 15.0, "k1": 0.94, "k2": 0.90, "k3": 0.85, "pe_hist_w": 0.50}
    r = row.iloc[0]
    return {
        "withholding": _coerce_float(r.get("Källskatt (%)"), 15.0),
        "k1": 1.0 - _coerce_float(r.get("Kompression år1 (%)"), 6.0)/100.0,
        "k2": 1.0 - _coerce_float(r.get("Kompression år2 (%)"), 10.0)/100.0,
        "k3": 1.0 - _coerce_float(r.get("Kompression år3 (%)"), 15.0)/100.0,
        "pe_hist_w": _coerce_float(r.get("PE_vikt_hist (%)"), 50.0)/100.0
    }

def gentle_compression(mult: float, k: float) -> float:
    if not mult or np.isnan(mult): return np.nan
    return float(mult) * float(k)

# ============== Värderingsmetoder ==============
def _price_from_ev(ev_target: float, net_debt: float, shares_out: float) -> float:
    if not ev_target or np.isnan(ev_target) or not shares_out or shares_out <= 0:
        return np.nan
    mcap = ev_target - (net_debt or 0.0)
    if mcap <= 0: return np.nan
    return float(mcap) / float(shares_out)

def _derive_net_debt(ev: float, mcap: float) -> float:
    if ev is None or mcap is None or np.isnan(ev) or np.isnan(mcap): return 0.0
    return float(ev) - float(mcap)

def _derive_bvps(price: float, pb: float, existing_bvps: float) -> float:
    if existing_bvps and not np.isnan(existing_bvps): return existing_bvps
    if price and pb and pb > 0: return float(price) / float(pb)
    return np.nan

def compute_pe_targets(bas: dict, est: dict, settings: dict):
    price = _coerce_float(bas.get("price"))
    eps0  = _coerce_float(bas.get("eps_ttm"))
    pe_t  = _coerce_float(bas.get("pe_ttm"))
    pe_f  = _coerce_float(bas.get("pe_fwd"))
    w = settings.get("pe_hist_w", 0.5)
    if np.isnan(pe_f) or pe_f <= 0: pe_f = pe_t
    if np.isnan(pe_t) or pe_t <= 0: pe_t = pe_f
    anchor = np.nan
    if pe_t and pe_f and not np.isnan(pe_t) and not np.isnan(pe_f): anchor = w*pe_t + (1-w)*pe_f
    eps1 = est.get("eps1"); eps2 = est.get("eps2"); g_eps = est.get("g_eps")
    if (eps1 is None or np.isnan(eps1)) and not np.isnan(eps0) and g_eps and not np.isnan(g_eps):
        eps1 = eps0*(1.0+g_eps)
    if (eps2 is None or np.isnan(eps2)) and eps1 and not np.isnan(eps1) and g_eps and not np.isnan(g_eps):
        eps2 = eps1*(1.0+g_eps)
    if eps1 is None or np.isnan(eps1): eps1 = eps0
    if eps2 is None or np.isnan(eps2): eps2 = eps1
    k1,k2,k3 = settings["k1"], settings["k2"], settings["k3"]
    def _p(eps,m): 
        if eps is None or np.isnan(eps) or m is None or np.isnan(m) or m<=0: return np.nan
        return float(eps)*float(m)
    p0 = _p(eps0, anchor)
    p1 = _p(eps1, gentle_compression(anchor, k1))
    p2 = _p(eps2, gentle_compression(anchor, k2))
    eps3 = eps2 if (g_eps is None or np.isnan(g_eps)) else eps2*(1.0+g_eps)
    p3 = _p(eps3, gentle_compression(anchor, k3))
    meta = {"anchor":anchor,"eps0":eps0,"eps1":eps1,"eps2":eps2,"eps3":eps3}
    return p0,p1,p2,p3,meta

def compute_evsales_targets(bas: dict, est: dict, settings: dict):
    ev   = _coerce_float(bas.get("ev")); mcap=_coerce_float(bas.get("market_cap"))
    rev0 = _coerce_float(bas.get("rev_ttm")); evsales=_coerce_float(bas.get("ev_rev"))
    shares = _coerce_float(bas.get("shares_out"))
    if not evsales or evsales<=0 or not rev0 or rev0<=0 or not shares or shares<=0:
        return np.nan, np.nan, np.nan, np.nan, {}
    nd = _derive_net_debt(ev, mcap)
    g  = est.get("g_rev")
    rev1 = rev0*(1.0+g) if (g and not np.isnan(g)) else rev0
    rev2 = rev1*(1.0+g) if (g and not np.isnan(g)) else rev1
    rev3 = rev2*(1.0+g) if (g and not np.isnan(g)) else rev2
    k1,k2,k3 = settings["k1"], settings["k2"], settings["k3"]
    EV0 = evsales*rev0; EV1 = gentle_compression(evsales,k1)*rev1
    EV2 = gentle_compression(evsales,k2)*rev2; EV3 = gentle_compression(evsales,k3)*rev3
    p0 = _price_from_ev(EV0, nd, shares)
    p1 = _price_from_ev(EV1, nd, shares)
    p2 = _price_from_ev(EV2, nd, shares)
    p3 = _price_from_ev(EV3, nd, shares)
    return p0,p1,p2,p3,{"anchor":evsales,"rev0":rev0,"rev1":rev1,"rev2":rev2,"rev3":rev3,"net_debt":nd}

def compute_evebitda_targets(bas: dict, est: dict, settings: dict):
    ev=_coerce_float(bas.get("ev")); mcap=_coerce_float(bas.get("market_cap"))
    e0=_coerce_float(bas.get("ebitda_ttm")); multiple=_coerce_float(bas.get("ev_ebitda"))
    shares=_coerce_float(bas.get("shares_out"))
    if not multiple or multiple<=0 or not e0 or e0<=0 or not shares or shares<=0:
        return np.nan, np.nan, np.nan, np.nan, {}
    nd=_derive_net_debt(ev,mcap); g=est.get("g_rev")
    e1=e0*(1.0+g) if (g and not np.isnan(g)) else e0
    e2=e1*(1.0+g) if (g and not np.isnan(g)) else e1
    e3=e2*(1.0+g) if (g and not np.isnan(g)) else e2
    k1,k2,k3 = settings["k1"], settings["k2"], settings["k3"]
    EV0=multiple*e0; EV1=gentle_compression(multiple,k1)*e1
    EV2=gentle_compression(multiple,k2)*e2; EV3=gentle_compression(multiple,k3)*e3
    p0=_price_from_ev(EV0,nd,shares); p1=_price_from_ev(EV1,nd,shares)
    p2=_price_from_ev(EV2,nd,shares); p3=_price_from_ev(EV3,nd,shares)
    return p0,p1,p2,p3,{"anchor":multiple,"ebitda0":e0,"ebitda1":e1,"ebitda2":e2,"ebitda3":e3,"net_debt":nd}

def compute_pb_targets(bas: dict, settings: dict, bvps_manual: float):
    price=_coerce_float(bas.get("price")); pb=_coerce_float(bas.get("pb"))
    bvps0=_derive_bvps(price,pb,_coerce_float(bvps_manual))
    if np.isnan(bvps0) or not pb or pb<=0: return np.nan, np.nan, np.nan, np.nan, {}
    g_rev_guess=0.07
    bvps1=bvps0*(1.0+g_rev_guess); bvps2=bvps1*(1.0+g_rev_guess); bvps3=bvps2*(1.0+g_rev_guess)
    k1,k2,k3 = settings["k1"], settings["k2"], settings["k3"]
    p0=bvps0*pb; p1=bvps1*gentle_compression(pb,k1); p2=bvps2*gentle_compression(pb,k2); p3=bvps3*gentle_compression(pb,k3)
    return p0,p1,p2,p3,{"anchor":pb,"bvps0":bvps0,"bvps1":bvps1,"bvps2":bvps2,"bvps3":bvps3}

def compute_nav_like_targets(mult: float, ps0: float, settings: dict):
    if not mult or mult<=0 or not ps0 or ps0<=0: return np.nan, np.nan, np.nan, np.nan
    k1,k2,k3 = settings["k1"], settings["k2"], settings["k3"]
    return ps0*mult, ps0*gentle_compression(mult,k1), ps0*gentle_compression(mult,k2), ps0*gentle_compression(mult,k3)

def pick_primary_method(bucket: str) -> str:
    if not bucket: return "ev_ebitda"
    b=str(bucket).lower()
    if "tillväxt" in b: return "ev_ebitda"
    if "utdelning" in b: return "pe"
    return "ev_ebitda"

def compute_all_methods_for_row(row: pd.Series, basics: dict, est: dict, settings: dict) -> pd.DataFrame:
    methods=[]
    p0,p1,p2,p3,meta_pe = compute_pe_targets(basics, est, settings)
    methods.append(("pe_hist_vs_eps", p0,p1,p2,p3, meta_pe))
    s0,s1,s2,s3,meta_s = compute_evsales_targets(basics, est, settings)
    methods.append(("ev_sales", s0,s1,s2,s3, meta_s))
    e0,e1,e2,e3,meta_e = compute_evebitda_targets(basics, est, settings)
    methods.append(("ev_ebitda", e0,e1,e2,e3, meta_e))
    # proxy DACF = EBITDA
    methods.append(("ev_dacf", e0,e1,e2,e3, meta_e))
    # P/B
    b0,b1,b2,b3,meta_b = compute_pb_targets(basics, settings, _coerce_float(row.get("BVPS")))
    methods.append(("p_b", b0,b1,b2,b3, meta_b))
    # NAV/AFFO/TBV/NII (om manuell per-aktie finns)
    for label, col, mult in [("p_nav","NAVPS", _coerce_float(basics.get("pb"))),
                             ("p_affo","AFFOPS",  _coerce_float(basics.get("pb"))),
                             ("p_tbv","TBVPS",    _coerce_float(row.get("P_B") or basics.get("pb"))),
                             ("p_nii","NIIps",    _coerce_float(basics.get("pb")))]:
        ps0 = _coerce_float(row.get(col))
        if ps0 and not np.isnan(ps0) and mult and mult>0:
            m0,m1,m2,m3 = compute_nav_like_targets(mult, ps0, settings)
        else:
            m0=m1=m2=m3=np.nan
        methods.append((label, m0,m1,m2,m3, {}))
    return pd.DataFrame(methods, columns=["Metod","Idag","1 år","2 år","3 år","Meta"])

# ============== Skrivningar mot ark ==============
def update_data_row_with_basics(df: pd.DataFrame, ws: gspread.Worksheet, ticker: str, basics: dict) -> pd.DataFrame:
    if df.empty: df = pd.DataFrame(columns=DATA_COLS)
    mask = df["Ticker"].astype(str).str.upper() == ticker.upper()
    if not mask.any():
        new = pd.Series({c:"" for c in DATA_COLS})
        new["Ticker"]=ticker.upper()
        df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
        mask = df["Ticker"].astype(str).str.upper() == ticker.upper()
    idx = df.index[mask][0]
    # uppdatera fält
    df.loc[idx, "Namn"]         = basics.get("name")
    df.loc[idx, "Valuta"]       = basics.get("currency")
    df.loc[idx, "Price"]        = basics.get("price")
    df.loc[idx, "MarketCap"]    = basics.get("market_cap")
    df.loc[idx, "EV"]           = basics.get("ev")
    df.loc[idx, "Revenue_TTM"]  = basics.get("rev_ttm")
    df.loc[idx, "EBITDA_TTM"]   = basics.get("ebitda_ttm")
    df.loc[idx, "EPS_TTM"]      = basics.get("eps_ttm")
    df.loc[idx, "PE_TTM"]       = basics.get("pe_ttm")
    df.loc[idx, "PE_FWD"]       = basics.get("pe_fwd")
    df.loc[idx, "EV_Sales"]     = basics.get("ev_rev")
    df.loc[idx, "EV_EBITDA"]    = basics.get("ev_ebitda")
    df.loc[idx, "P_B"]          = basics.get("pb")
    df.loc[idx, "Shares_Out"]   = basics.get("shares_out")
    df.loc[idx, "Dividend_TTM"] = basics.get("dividend_ttm")
    df.loc[idx, "Div_Currency"] = basics.get("div_ccy")
    df.loc[idx, "Senast auto uppdaterad"] = _now_iso()
    # skriv tillbaka
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
    return df

def save_quarter_snapshot(ticker: str, basics: dict):
    row = {
        "Timestamp": _now_iso(),
        "Ticker": ticker.upper(),
        "Namn": basics.get("name"),
        "Bucket": "",
        "Valuta": basics.get("currency"),
        "Pris": basics.get("price"),
        "PE_TTM": basics.get("pe_ttm"),
        "PE_FWD": basics.get("pe_fwd"),
        "EV_Sales": basics.get("ev_rev"),
        "EV_EBITDA": basics.get("ev_ebitda"),
        "P_B": basics.get("pb"),
        "Revenue_TTM": basics.get("rev_ttm"),
        "EBITDA_TTM": basics.get("ebitda_ttm"),
        "EPS_TTM": basics.get("eps_ttm"),
        "EV": basics.get("ev"),
        "MarketCap": basics.get("market_cap"),
    }
    df = pd.DataFrame(SNAP_WS.get_all_records())
    if df.empty: df = pd.DataFrame(columns=SNAP_COLS)
    for c in SNAP_COLS:
        if c not in row: row[c] = ""
    df = pd.concat([df, pd.DataFrame([row])[SNAP_COLS]], ignore_index=True)
    SNAP_WS.clear()
    SNAP_WS.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())

# app.py — Del 3/4
# ====================== Läsning med cache (Worksheet som _ws) ======================
@st.cache_data(ttl=60, show_spinner=False)
def _read_df_from_ws(_ws: gspread.Worksheet) -> pd.DataFrame:
    """
    Ledande underscore på parametern gör att Streamlit inte försöker hasha Worksheet-objektet.
    """
    try:
        df = pd.DataFrame(_ws.get_all_records())
        # säkerställ kolumner
        for c in DATA_COLS:
            if c not in df.columns:
                df[c] = ""
        return df
    except Exception:
        return pd.DataFrame(columns=DATA_COLS)

# ====================== UI-hjälpare ======================
def _fmt_money(x: float, ccy: str) -> str:
    return "–" if (x is None or np.isnan(x)) else f"{x:,.2f} {ccy}".replace(",", " ")

def _fmt_pct(x: float) -> str:
    return "–" if (x is None or np.isnan(x)) else f"{x*100:.1f}%"

def _ui_sidebar_filters(df: pd.DataFrame) -> dict:
    st.sidebar.header("⚙️ Filter")
    buckets = sorted([b for b in df["Bucket"].dropna().unique().tolist() if str(b).strip()!=""])
    bucket_sel = st.sidebar.multiselect("Bucket", buckets, default=buckets)
    inv_choice = st.sidebar.radio("Innehav/Watchlist", ["Alla","Endast innehav","Endast ej ägda"], index=0)
    only_owned = (inv_choice=="Endast innehav")
    only_empty = (inv_choice=="Endast ej ägda")
    return {"bucket_sel":bucket_sel,"only_owned":only_owned,"only_empty":only_empty}

def _apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    out = df.copy()
    if f["bucket_sel"]:
        out = out[out["Bucket"].isin(f["bucket_sel"])]
    if f["only_owned"]:
        out = out[_coerce_float_series(out["Antal aktier"]).fillna(0) > 0]
    if f["only_empty"]:
        out = out[_coerce_float_series(out["Antal aktier"]).fillna(0) <= 0]
    return out.reset_index(drop=True)

def _present_primary_cards(ccy: str, price: float, primary_method: str,
                           targets: Tuple[float,float,float,float], band_hint: str=""):
    st.subheader("🎯 Primär riktkurs")
    p0,p1,p2,p3 = targets
    st.metric(label="Idag", value=_fmt_money(p0, ccy),
              delta=(f"{_fmt_pct((p0/price - 1.0) if (price and p0) else np.nan)} uppsida"))
    c1,c2,c3 = st.columns(3)
    c1.metric("1 år", _fmt_money(p1, ccy),
              delta=_fmt_pct((p1/price - 1.0) if (price and p1) else np.nan))
    c2.metric("2 år", _fmt_money(p2, ccy),
              delta=_fmt_pct((p2/price - 1.0) if (price and p2) else np.nan))
    c3.metric("3 år", _fmt_money(p3, ccy),
              delta=_fmt_pct((p3/price - 1.0) if (price and p3) else np.nan))
    if band_hint:
        st.caption(band_hint)

# ====================== Analys-vy ======================
def run_main_ui():
    st.title("📊 Analys & rangordning")

    data_df = _read_df_from_ws(DATA_WS)
    if data_df.empty:
        st.warning("Fliken **Data** är tom.")
        return

    F = _ui_sidebar_filters(data_df)
    view_df = _apply_filters(data_df, F)

    # Välj bolag
    tickers = view_df["Ticker"].astype(str).tolist()
    sel = st.selectbox("Välj bolag", options=tickers, index=0 if tickers else None)

    # Enskild uppdatering
    with st.expander("🔄 Uppdatera valt bolag från Yahoo"):
        if st.button("Uppdatera nu"):
            if sel:
                bas = fetch_yahoo_basics(sel)
                cur = _read_df_from_ws(DATA_WS)
                _ = update_data_row_with_basics(cur, DATA_WS, sel, bas)
                try: save_quarter_snapshot(sel, bas)
                except Exception: pass
                st.success(f"Uppdaterade {sel}.")
            else:
                st.info("Välj ett bolag först.")

    # Rangordning
    st.header("🏁 Rangordning (störst uppsida →)")
    rows=[]
    for _,row in view_df.iterrows():
        t = str(row.get("Ticker") or "").strip()
        if not t: continue
        bas = fetch_yahoo_basics(t)
        settings = get_currency_settings(bas.get("currency") or "SEK")
        est = fetch_eps_rev_estimates(t)
        methods = compute_all_methods_for_row(row, bas, est, settings)
        prim_key = pick_primary_method(row.get("Bucket",""))
        want = "pe_hist_vs_eps" if prim_key=="pe" else prim_key
        try:
            mrow = methods.loc[methods["Metod"]==want].iloc[0]
        except Exception:
            mrow = methods.iloc[0]
        fv = _coerce_float(mrow["Idag"])
        px = _coerce_float(bas.get("price"))
        up = (fv/px - 1.0) if (fv and px and px>0) else np.nan
        rows.append({
            "Ticker":t, "Namn": row.get("Namn") or bas.get("name"),
            "Bucket":row.get("Bucket"), "Valuta": bas.get("currency") or row.get("Valuta") or "",
            "Pris": px, "Primär metod": want, "FV idag": fv, "Uppsida": up
        })
    rank_df = pd.DataFrame(rows)
    if not rank_df.empty:
        rank_df = rank_df.sort_values("Uppsida", ascending=False).reset_index(drop=True)
        st.dataframe(rank_df[["Ticker","Namn","Bucket","Valuta","Pris","Primär metod","FV idag","Uppsida"]],
                     use_container_width=True)

        if st.button("💾 Spara primära riktkurser till Resultat"):
            res = pd.DataFrame(RES_WS.get_all_records())
            if res.empty: res = pd.DataFrame(columns=RESULT_COLS)
            ts = _now_iso()
            add = []
            for _,r in rank_df.iterrows():
                add.append({
                    "Timestamp": ts, "Ticker": r["Ticker"], "Valuta": r["Valuta"],
                    "Aktuell kurs (0)": f"{_coerce_float(r['Pris']):.4f}" if r["Pris"] else "",
                    "Riktkurs idag": f"{_coerce_float(r['FV idag']):.4f}" if r["FV idag"] else "",
                    "Riktkurs 1 år":"", "Riktkurs 2 år":"", "Riktkurs 3 år":"",
                    "Bull 1 år":"", "Bear 1 år":"",
                    "Metod": r["Primär metod"], "Input-sammanfattning":"", "Kommentar":""
                })
            res = pd.concat([res, pd.DataFrame(add)[RESULT_COLS]], ignore_index=True)
            RES_WS.clear()
            RES_WS.update([res.columns.tolist()] + res.fillna("").astype(str).values.tolist())
            st.success("Sparat till fliken **Resultat**.")

    # Detaljer för valt bolag
    st.header("🔍 Detaljer per bolag (alla metoder)")
    if sel:
        row = view_df.loc[view_df["Ticker"].astype(str)==str(sel)].iloc[0]
        bas = fetch_yahoo_basics(sel)
        settings = get_currency_settings(bas.get("currency") or "SEK")
        est = fetch_eps_rev_estimates(sel)
        methods = compute_all_methods_for_row(row, bas, est, settings)

        ccy = bas.get("currency") or row.get("Valuta") or "USD"
        price = _coerce_float(bas.get("price"))
        st.caption(f"Valuta: **{ccy}** • Pris: **{_fmt_money(price, ccy)}** • Bucket: **{row.get('Bucket','')}**")
        st.dataframe(methods.drop(columns=["Meta"]), use_container_width=True)

        prim_key = pick_primary_method(row.get("Bucket",""))
        want = "pe_hist_vs_eps" if prim_key=="pe" else prim_key
        try:
            mrow = methods.loc[methods["Metod"]==want].iloc[0]
        except Exception:
            mrow = methods.iloc[0]
        p0,p1,p2,p3 = [_coerce_float(mrow[k]) for k in ["Idag","1 år","2 år","3 år"]]
        _present_primary_cards(ccy, price, want, (p0,p1,p2,p3))

        # Utdelning & innehavsvärde i SEK
        st.subheader("💰 Utdelning (netto, SEK)")
        div_ccy = bas.get("div_ccy") or ccy
        div_ttm = _coerce_float(bas.get("dividend_ttm"), 0.0)
        shares  = _coerce_float(row.get("Antal aktier"), 0.0)
        wh = get_currency_settings(div_ccy)["withholding"]
        # enkel prognos: väx med g_rev
        g = est.get("g_rev")
        g = g if (g and not np.isnan(g) and g>-0.9) else 0.0
        d1 = div_ttm*(1.0+g); d2 = d1*(1.0+g); d3 = d2*(1.0+g)
        def _net_div_sek(dps): 
            return fx_to_sek((dps or 0.0) * (shares or 0.0) * (1.0 - wh/100.0), div_ccy)
        st.write(f"• Nästa år: **{_net_div_sek(d1):,.0f} SEK**")
        st.write(f"• 2 år: **{_net_div_sek(d2):,.0f} SEK**")
        st.write(f"• 3 år: **{_net_div_sek(d3):,.0f} SEK**")
        st.caption(f"Källskatt: {wh:.0f}% • Antal aktier: {int(shares) if shares>0 else 0}")

        st.subheader("🧾 Innehavsvärde")
        tot_sek = fx_to_sek((price or 0.0)*(shares or 0.0), ccy)
        st.write(f"Totalt värde nu: **{tot_sek:,.0f} SEK**")

# app.py — Del 4/4
# ====================== Inställningar (UI) ======================
def settings_ui():
    st.title("⚙️ Inställningar")
    st.caption("Redigera källskatt per valuta, multipelkompression (år 1–3) och PE-vikt (hist vs fwd).")
    df = pd.DataFrame(SET_WS.get_all_records())
    if df.empty:
        _seed_settings_if_empty()
        df = pd.DataFrame(SET_WS.get_all_records())

    edits=[]
    for i,r in df.iterrows():
        cols = st.columns([1,1,1,1,1,1.2])
        with cols[0]:
            v = st.text_input("Valuta", value=str(r["Valuta"]), key=f"set_val_{i}").upper().strip()
        with cols[1]:
            k = st.number_input("Källskatt (%)", 0, 60, int(_coerce_float(r["Källskatt (%)"],15)), key=f"set_wh_{i}")
        with cols[2]:
            c1 = st.number_input("Kompr år1 (%)", 0, 40, int(_coerce_float(r["Kompression år1 (%)"],6)), key=f"set_k1_{i}")
        with cols[3]:
            c2 = st.number_input("Kompr år2 (%)", 0, 60, int(_coerce_float(r["Kompression år2 (%)"],10)), key=f"set_k2_{i}")
        with cols[4]:
            c3 = st.number_input("Kompr år3 (%)", 0, 80, int(_coerce_float(r["Kompression år3 (%)"],15)), key=f"set_k3_{i}")
        with cols[5]:
            pe = st.number_input("PE_vikt_hist (%)", 0, 100, int(_coerce_float(r.get("PE_vikt_hist (%)",50))), key=f"set_pew_{i}")
        edits.append({"Valuta":v,"Källskatt (%)":k,"Kompression år1 (%)":c1,"Kompression år2 (%)":c2,"Kompression år3 (%)":c3,"PE_vikt_hist (%)":pe})
        st.divider()

    with st.expander("➕ Lägg till valuta"):
        nv = st.text_input("Ny valuta (t.ex. DKK)").upper().strip()
        if st.button("Lägg till"):
            if nv:
                edits.append({"Valuta":nv,"Källskatt (%)":15,"Kompression år1 (%)":6,"Kompression år2 (%)":10,"Kompression år3 (%)":15,"PE_vikt_hist (%)":50})
                st.success(f"Lagt till {nv} – glöm inte Spara.")

    if st.button("💾 Spara inställningar"):
        out = pd.DataFrame(edits)
        out = out.dropna(subset=["Valuta"]).drop_duplicates(subset=["Valuta"], keep="last").reset_index(drop=True)
        SET_WS.clear()
        SET_WS.update([out.columns.tolist()] + out.astype(str).values.tolist())
        st.success("Inställningar sparade.")

# ====================== FX-vy ======================
def fx_ui():
    st.title("💱 Valutakurser")
    st.caption("Kurser lagras i fliken **Valutakurser** (SEK-kors).")
    if st.button("🔄 Hämta & skriv valuta→SEK"):
        sync_fx_sheet()
        st.success("Valutakurser uppdaterade.")
    df = pd.DataFrame(FX_WS.get_all_records())
    if df.empty:
        st.info("Inga kurser sparade än.")
    else:
        st.dataframe(df, use_container_width=True)

# ====================== Massuppdatering ======================
def mass_update_ui():
    st.title("🔁 Massuppdatera från Yahoo")
    df = pd.DataFrame(DATA_WS.get_all_records())
    if df.empty:
        st.warning("Data-fliken är tom.")
        return
    tickers = df["Ticker"].astype(str).tolist()
    delay = st.slider("Fördröjning mellan bolag (sek)", 0.2, 3.0, 1.0, 0.1)
    do_snap = st.checkbox("Spara kvartalssnapshot för varje bolag", value=True)
    if st.button("Starta massuppdatering"):
        prog = st.progress(0.0, text="Startar…"); log = st.empty()
        n = len(tickers)
        for i,t in enumerate(tickers,1):
            try:
                log.write(f"Uppdaterar {t} ({i}/{n}) …")
                basics = fetch_yahoo_basics(t)
                cur = pd.DataFrame(DATA_WS.get_all_records())
                _ = update_data_row_with_basics(cur, DATA_WS, t, basics)
                if do_snap:
                    try: save_quarter_snapshot(t, basics)
                    except Exception: pass
                time.sleep(float(delay))
            except Exception as e:
                st.warning(f"{t}: {e}")
            prog.progress(i/n, text=f"Klart {i}/{n}")
        st.success("Massuppdatering klar.")

# ====================== Lägg till / uppdatera enskilt ======================
def editor_ui():
    st.title("📝 Lägg till/uppdatera bolag")
    df = pd.DataFrame(DATA_WS.get_all_records())
    buckets = [
        "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
        "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning"
    ]
    c1,c2,c3 = st.columns([1,1,1])
    with c1:
        t = st.text_input("Ticker (t.ex. NVDA)").upper().strip()
        b = st.selectbox("Bucket", buckets)
    with c2:
        sh  = st.number_input("Antal aktier", min_value=0, step=1, value=0)
        gav = st.number_input("GAV (SEK)", min_value=0.0, step=0.01, format="%.2f")
    with c3:
        bvps  = st.text_input("BVPS (valfritt)")
        navps = st.text_input("NAVPS (valfritt)")
        affops= st.text_input("AFFOPS (valfritt)")
        tbvps = st.text_input("TBVPS (valfritt)")
        niips = st.text_input("NIIps (valfritt)")

    if st.button("💾 Spara rad"):
        if not t:
            st.error("Ange ticker.")
            return
        if df.empty: df = pd.DataFrame(columns=DATA_COLS)
        if "Ticker" not in df.columns: df["Ticker"] = ""
        mask = df["Ticker"].astype(str).str.upper() == t
        if not mask.any():
            new = {c:"" for c in DATA_COLS}; new["Ticker"]=t; df = pd.concat([df, pd.DataFrame([new])], ignore_index=True)
            mask = df["Ticker"].astype(str).str.upper() == t
        idx = df.index[mask][0]
        df.loc[idx,"Bucket"]=b; df.loc[idx,"Antal aktier"]=sh; df.loc[idx,"GAV (SEK)"]=gav
        if bvps:  df.loc[idx,"BVPS"]=bvps
        if navps: df.loc[idx,"NAVPS"]=navps
        if affops:df.loc[idx,"AFFOPS"]=affops
        if tbvps: df.loc[idx,"TBVPS"]=tbvps
        if niips: df.loc[idx,"NIIps"]=niips

        bas = fetch_yahoo_basics(t)
        df.loc[idx,"Valuta"]=bas.get("currency")
        df.loc[idx,"Namn"]=bas.get("name")
        df.loc[idx,"Price"]=bas.get("price")
        df.loc[idx,"MarketCap"]=bas.get("market_cap")
        df.loc[idx,"EV"]=bas.get("ev")
        df.loc[idx,"Revenue_TTM"]=bas.get("rev_ttm")
        df.loc[idx,"EBITDA_TTM"]=bas.get("ebitda_ttm")
        df.loc[idx,"EPS_TTM"]=bas.get("eps_ttm")
        df.loc[idx,"PE_TTM"]=bas.get("pe_ttm")
        df.loc[idx,"PE_FWD"]=bas.get("pe_fwd")
        df.loc[idx,"EV_Sales"]=bas.get("ev_rev")
        df.loc[idx,"EV_EBITDA"]=bas.get("ev_ebitda")
        df.loc[idx,"P_B"]=bas.get("pb")
        df.loc[idx,"Shares_Out"]=bas.get("shares_out")
        df.loc[idx,"Dividend_TTM"]=bas.get("dividend_ttm")
        df.loc[idx,"Div_Currency"]=bas.get("div_ccy")
        df.loc[idx,"Senast auto uppdaterad"]=_now_iso()

        DATA_WS.clear()
        DATA_WS.update([df.columns.tolist()] + df.fillna("").astype(str).values.tolist())
        try: save_quarter_snapshot(t, bas)
        except Exception: pass
        st.success(f"Sparade/uppdaterade {t}.")

# ====================== Main ======================
def main():
    st.sidebar.title("📚 Meny")
    page = st.sidebar.radio("Gå till", ["Analys","Lägg till/uppdatera","Massuppdatering","Inställningar","FX"], index=0)
    st.sidebar.divider()
    if st.sidebar.button("🔄 Uppdatera valutakurser"):
        sync_fx_sheet()
        st.sidebar.success("Valutakurser uppdaterade.")

    if page=="Analys": run_main_ui()
    elif page=="Lägg till/uppdatera": editor_ui()
    elif page=="Massuppdatering": mass_update_ui()
    elif page=="Inställningar": settings_ui()
    elif page=="FX": fx_ui()

if __name__ == "__main__":
    main()
