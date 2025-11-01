# app.py — BucketApp (full, stabil)
from __future__ import annotations

import os, time, math, json, re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

import yfinance as yf

# ---------- Sidhuvud ----------
st.set_page_config(page_title="BucketApp – fair value & portfölj", layout="wide", page_icon="🎯")
def now_iso(): return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

# ---------- Talhjälp ----------
def _coerce_float(x, default=np.nan):
    try:
        if x is None: return default
        if isinstance(x, (int,float,np.number)): return float(x)
        s = str(x).strip().replace(" ", "").replace(",", ".")
        if s == "" or s.lower()=="nan": return default
        m = re.match(r"^([\-]?\d+(\.\d+)?)([TtBbMmKk])$", s)
        if m:
            base = float(m.group(1)); mult = {"k":1e3,"m":1e6,"b":1e9,"t":1e12}[m.group(3).lower()]
            return base*mult
        return float(s)
    except Exception:
        return default

def _fmt_money(x, ccy):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))): return "-"
    try: return f"{x:,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except: return f"{x} {ccy}"

def _fmt_num(x):
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))): return ""
    try: return f"{x:,.4f}".replace(",", " ").replace(".", ",")
    except: return str(x)

def _safe_div(a,b):
    a=_coerce_float(a); b=_coerce_float(b)
    if np.isnan(a) or np.isnan(b) or b==0: return np.nan
    return a/b

def _sleep_backoff(i): time.sleep(min(0.25*(i+1), 2.0))

# ---------- Secrets ----------
SHEET_URL = st.secrets.get("SHEET_URL","").strip()
DEFAULT_MULT_DRIFT = -0.10   # −10 %/år

BUCKET_PRIMARY = {
    "Bucket A tillväxt":"ev_ebitda",
    "Bucket B tillväxt":"ev_sales",
    "Bucket C tillväxt":"ev_sales",
    "Bucket A utdelning":"p_affo",
    "Bucket B utdelning":"p_affo",
    "Bucket C utdelning":"p_b",
}

# ---------- Sheets-klient ----------
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n","\n")
    return creds

@st.cache_resource(show_spinner=False)
def get_gspread_client() -> gspread.Client:
    if "GOOGLE_CREDENTIALS" not in st.secrets:
        raise RuntimeError("Saknar GOOGLE_CREDENTIALS i secrets.")
    creds = _normalize_private_key(dict(st.secrets["GOOGLE_CREDENTIALS"]))
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds, scopes=scopes)
    return gspread.authorize(credentials)

@st.cache_resource(show_spinner=False)
def open_spreadsheet(_gc: gspread.Client):
    if not SHEET_URL: raise RuntimeError("SHEET_URL saknas i secrets.")
    for i in range(3):
        try: return _gc.open_by_url(SHEET_URL)
        except APIError: _sleep_backoff(i)
    raise RuntimeError("Kunde inte öppna Google Sheet. Kontrollera delning till servicekontot.")

def _get_ws(ss: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try: return ss.worksheet(title)
    except WorksheetNotFound: return ss.add_worksheet(title=title, rows=1000, cols=50)

def _df_from_ws(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = ws.get_all_values()
    if not vals: return pd.DataFrame()
    df = pd.DataFrame(vals)
    if df.empty: return df
    df.columns = df.iloc[0].fillna("").tolist()
    df = df.iloc[1:].reset_index(drop=True)
    return df

def _write_df_to_ws(ws: gspread.Worksheet, df: pd.DataFrame):
    if df is None: return
    df = df.copy().fillna("")
    ws.clear()
    ws.update("A1", [list(df.columns)] + df.values.tolist(), value_input_option="USER_ENTERED")

@st.cache_resource(show_spinner=False)
def ensure_sheets():
    gc = get_gspread_client()
    ss = open_spreadsheet(gc)
    data_ws = _get_ws(ss, "Data")
    fx_ws = _get_ws(ss, "Valutakurser")
    adv_ws = _get_ws(ss, "Avancerat")
    res_ws = _get_ws(ss, "Resultat")
    set_ws = _get_ws(ss, "Settings")
    snap_ws = _get_ws(ss, "Snapshots")
    port_ws = _get_ws(ss, "Portfölj")  # summeringar
    # seed FX-header om saknas → fixar KeyError 'Currency'
    fx_df = _df_from_ws(fx_ws)
    if fx_df.empty or "Currency" not in fx_df.columns:
        _write_df_to_ws(fx_ws, pd.DataFrame({"Currency":["SEK","USD"],"SEK_per_CCY":[1.0,np.nan],"UpdatedAt":[now_iso(),now_iso()]}))
    return dict(ss=ss, data_ws=data_ws, fx_ws=fx_ws, adv_ws=adv_ws, res_ws=res_ws, set_ws=set_ws, snap_ws=snap_ws, port_ws=port_ws)

# ---------- Settings ----------
DEFAULT_SETTINGS = {
    "mult_drift_per_year": DEFAULT_MULT_DRIFT,
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
        _write_df_to_ws(set_ws, pd.DataFrame({
            "Key":["mult_drift_per_year","withholding","dividend_cagr_default","rev_cagr_default","eps_cagr_default"],
            "Value":[str(DEFAULT_SETTINGS["mult_drift_per_year"]),
                     json.dumps(DEFAULT_SETTINGS["withholding"]),
                     str(DEFAULT_SETTINGS["dividend_cagr_default"]),
                     str(DEFAULT_SETTINGS["rev_cagr_default"]),
                     str(DEFAULT_SETTINGS["eps_cagr_default"])]
        }))
        return DEFAULT_SETTINGS.copy()
    out = DEFAULT_SETTINGS.copy()
    for _, r in df.iterrows():
        k = str(r.get("Key","")).strip(); v = str(r.get("Value","")).strip()
        if k=="withholding":
            try: out["withholding"]=json.loads(v)
            except: pass
        elif k in out:
            out[k]=_coerce_float(v,out[k]) if "cagr" in k or "drift" in k else v
    return out

# ---------- FX ----------
@st.cache_data(ttl=60*60, show_spinner=False)
def _yahoo_fx_to_sek(ccy: str) -> float:
    if ccy.upper()=="SEK": return 1.0
    pair = f"{ccy.upper()}SEK=X"
    try:
        t=yf.Ticker(pair)
        p=t.fast_info.get("last_price")
        if not p:
            hist=t.history(period="5d")["Close"]
            if len(hist)>0: p=float(hist.dropna().iloc[-1])
        return _coerce_float(p, np.nan)
    except: return np.nan

def refresh_fx_table(fx_ws: gspread.Worksheet, currencies: List[str]) -> pd.DataFrame:
    currencies=sorted(list(set([c for c in (currencies or []) if c])))
    rows=[]
    for c in currencies:
        rows.append({"Currency":c.upper(),"SEK_per_CCY":_yahoo_fx_to_sek(c),"UpdatedAt":now_iso()})
        time.sleep(0.2)
    df=pd.DataFrame(rows)
    if not df.empty: _write_df_to_ws(fx_ws, df)
    return df

def load_fx_map(fx_ws: gspread.Worksheet) -> Dict[str,float]:
    df=_df_from_ws(fx_ws)
    if df.empty or "Currency" not in df.columns or "SEK_per_CCY" not in df.columns:
        return {"SEK":1.0,"USD":_yahoo_fx_to_sek("USD")}
    out={}
    for _,r in df.iterrows():
        out[str(r.get("Currency","")).upper()] = _coerce_float(r.get("SEK_per_CCY"), np.nan)
    if "SEK" not in out: out["SEK"]=1.0
    return out

# ---------- Yahoo bas ----------
def _sum_last(series_like,n=4):
    try: return float(pd.Series(series_like).dropna().astype(float).tail(n).sum())
    except: return np.nan

def _ttm_from_quarterly(df: pd.DataFrame, row_names: List[str]) -> float:
    if not isinstance(df,pd.DataFrame) or df.empty: return np.nan
    tgt = next((r for r in row_names if r in df.index), None)
    if not tgt: return np.nan
    try: return _sum_last(df.loc[tgt].values,4)
    except: return np.nan

def _fix_eps_units(price, eps, pe_ttm):
    try:
        price=float(price); eps=float(eps); pe_ttm=float(pe_ttm)
        if eps==0: return eps
        calc=price/eps
        if pe_ttm>0 and 50 < abs(calc/pe_ttm) < 150: return eps/100.0
        return eps
    except: return eps

@st.cache_data(ttl=60*10, show_spinner=False)
def fetch_yahoo_basics(ticker:str)->dict:
    out={"ticker":ticker,"name":ticker,"currency":None,"price":np.nan,
         "market_cap":np.nan,"ev":np.nan,"shares_out":np.nan,
         "rev_ttm":np.nan,"ebitda_ttm":np.nan,"eps_ttm":np.nan,
         "pe_ttm":np.nan,"pe_fwd":np.nan,"ev_rev":np.nan,"ev_ebitda":np.nan,
         "pb":np.nan,"dividend_ttm":np.nan}
    try:
        tk=yf.Ticker(ticker)
        fi=tk.fast_info or {}
        info={}
        try: info=tk.get_info()
        except: pass

        price=_coerce_float(fi.get("last_price"))
        if not price or np.isnan(price):
            try:
                hist=tk.history(period="5d")["Close"]
                if len(hist)>0: price=float(hist.dropna().iloc[-1])
            except: pass
        if not price or np.isnan(price): price=_coerce_float(info.get("currentPrice"))
        out["price"]=price

        out["currency"]=fi.get("currency") or info.get("currency") or "USD"
        out["name"]=info.get("shortName") or info.get("longName") or ticker

        shares=_coerce_float(fi.get("shares")) or _coerce_float(info.get("sharesOutstanding"))
        out["shares_out"]=shares

        mcap=_coerce_float(info.get("marketCap"))
        if (not mcap or np.isnan(mcap)) and price and shares: mcap=float(price)*float(shares)
        out["market_cap"]=mcap

        ev=_coerce_float(info.get("enterpriseValue"))
        if not ev or np.isnan(ev):
            total_debt=_coerce_float(info.get("totalDebt"),0.0)
            cash=_coerce_float(info.get("totalCash"),0.0)
            if mcap and not np.isnan(mcap): ev=float(mcap)+float(total_debt)-float(cash)
        out["ev"]=ev

        pb=_coerce_float(info.get("priceToBook"))
        if (not pb or np.isnan(pb)) and price:
            bvps=_coerce_float(info.get("bookValue"), np.nan)
            if bvps and not np.isnan(bvps): pb=float(price)/float(bvps)
        out["pb"]=pb

        # TTM från quarterly
        q_inc=None
        try: q_inc=tk.quarterly_income_stmt
        except:
            try: q_inc=tk.get_income_stmt(trailing=True)
            except: q_inc=None

        rev_ttm=_ttm_from_quarterly(q_inc,["TotalRevenue","Total Revenue"])
        ebitda_ttm=_ttm_from_quarterly(q_inc,["EBITDA"])
        ni_ttm=_ttm_from_quarterly(q_inc,["NetIncome","Net Income"])

        if not rev_ttm or np.isnan(rev_ttm):
            try:
                inc=tk.income_stmt
                if isinstance(inc,pd.DataFrame) and not inc.empty:
                    rn="TotalRevenue" if "TotalRevenue" in inc.index else ("Total Revenue" if "Total Revenue" in inc.index else None)
                    if rn: rev_ttm=_coerce_float(inc.loc[rn].dropna().astype(float).iloc[0])
            except: pass

        if not ebitda_ttm or np.isnan(ebitda_ttm):
            try:
                inc=tk.income_stmt
                if isinstance(inc,pd.DataFrame) and not inc.empty and "EBITDA" in inc.index:
                    ebitda_ttm=_coerce_float(inc.loc["EBITDA"].dropna().astype(float).iloc[0])
            except: pass

        if not ni_ttm or np.isnan(ni_ttm):
            try:
                inc=tk.income_stmt
                if isinstance(inc,pd.DataFrame) and not inc.empty:
                    rn="NetIncome" if "NetIncome" in inc.index else ("Net Income" if "Net Income" in inc.index else None)
                    if rn: ni_ttm=_coerce_float(inc.loc[rn].dropna().astype(float).iloc[0])
            except: pass

        out["rev_ttm"]=rev_ttm
        out["ebitda_ttm"]=ebitda_ttm

        eps_ttm=np.nan
        if ni_ttm and shares and shares>0: eps_ttm=float(ni_ttm)/float(shares)
        out["eps_ttm"]=eps_ttm

        pe_ttm=_coerce_float(info.get("trailingPE"))
        pe_fwd=_coerce_float(info.get("forwardPE"))
        if (not pe_ttm or np.isnan(pe_ttm)) and (price and eps_ttm and eps_ttm!=0):
            pe_ttm=float(price)/float(eps_ttm)
        if price and pe_ttm and eps_ttm:
            eps_ttm=_fix_eps_units(price, eps_ttm, pe_ttm); out["eps_ttm"]=eps_ttm
            if not info.get("trailingPE"): pe_ttm=float(price)/float(eps_ttm)
        out["pe_ttm"]=pe_ttm; out["pe_fwd"]=pe_fwd

        if ev and rev_ttm and rev_ttm>0: out["ev_rev"]=float(ev)/float(rev_ttm)
        if ev and ebitda_ttm and ebitda_ttm>0: out["ev_ebitda"]=float(ev)/float(ebitda_ttm)

        # Dividend TTM
        try:
            divs=yf.Ticker(ticker).dividends
            if divs is not None and len(divs)>0:
                last12=divs[divs.index >= (divs.index.max()-pd.Timedelta(days=365))]
                out["dividend_ttm"]=float(last12.sum())
        except: pass
    except: pass
    time.sleep(0.3)
    return out

# ---------- Estimat & CAGR ----------
def _cagr(start,end,years):
    try:
        start=float(start); end=float(end); years=float(years)
        if start<=0 or years<=0: return np.nan
        return (end/start)**(1/years)-1.0
    except: return np.nan

@st.cache_data(ttl=60*20, show_spinner=False)
def fetch_eps_rev_estimates(ticker:str, rev_cagr_default=0.10, eps_cagr_default=0.10)->dict:
    out={"eps1":np.nan,"eps2":np.nan,"eps3":np.nan,"rev1":np.nan,"rev2":np.nan,"rev3":np.nan,"g_rev":np.nan,"g_eps":np.nan}
    base=fetch_yahoo_basics(ticker)
    eps0=_coerce_float(base.get("eps_ttm")); rev0=_coerce_float(base.get("rev_ttm"))
    tk=yf.Ticker(ticker)
    g_rev=np.nan; g_eps=np.nan
    try: q=tk.quarterly_income_stmt
    except:
        try: q=tk.get_income_stmt(trailing=True)
        except: q=None
    shares=_coerce_float(base.get("shares_out"))

    if isinstance(q,pd.DataFrame) and not q.empty:
        rn = "TotalRevenue" if "TotalRevenue" in q.index else ("Total Revenue" if "Total Revenue" in q.index else None)
        if rn:
            vals=pd.Series(q.loc[rn].dropna().astype(float))
            if len(vals)>=8:
                g_rev=_cagr(float(vals.iloc[-8:-4].sum()), float(vals.iloc[-4:].sum()), 1.0)
        rn2="NetIncome" if "NetIncome" in q.index else ("Net Income" if "Net Income" in q.index else None)
        if rn2 and shares and shares>0:
            v=(pd.Series(q.loc[rn2].dropna().astype(float))/float(shares))
            if len(v)>=8:
                g_eps=_cagr(float(v.iloc[-8:-4].sum()), float(v.iloc[-4:].sum()), 1.0)

    if np.isnan(g_rev): g_rev=rev_cagr_default
    if np.isnan(g_eps): g_eps=eps_cagr_default
    out["g_rev"]=g_rev; out["g_eps"]=g_eps

    if rev0 and rev0>0:
        out["rev1"]=rev0*(1+g_rev); out["rev2"]=out["rev1"]*(1+g_rev); out["rev3"]=out["rev2"]*(1+g_rev)
    if eps0 and not np.isnan(eps0):
        out["eps1"]=eps0*(1+g_eps); out["eps2"]=out["eps1"]*(1+g_eps); out["eps3"]=out["eps2"]*(1+g_eps)
    return out

# ---------- Avancerade manuella fält ----------
def load_advanced_map(adv_ws: gspread.Worksheet)->Dict[str,Dict[str,float]]:
    df=_df_from_ws(adv_ws)
    if df.empty: return {}
    out={}
    for _,r in df.iterrows():
        t=str(r.get("Ticker","")).upper().strip()
        out[t]={
            "BVPS":_coerce_float(r.get("BVPS")),
            "NAVPS":_coerce_float(r.get("NAVPS")),
            "TBVPS":_coerce_float(r.get("TBVPS")),
            "AFFOPS":_coerce_float(r.get("AFFOPS")),
            "NIIps":_coerce_float(r.get("NIIps")),
            "DACFps":_coerce_float(r.get("DACFps")),
            "FCFps":_coerce_float(r.get("FCFps")),
            "FCF_margin":_coerce_float(r.get("FCF_margin")),
        }
    return out

# ---------- Värderingsmetoder ----------
def _apply_multiple_drift(mult0, years, drift_per_year):
    if mult0 is None or np.isnan(mult0): return np.nan
    try: return float(mult0)*(1.0+float(drift_per_year))**years
    except: return np.nan

def build_valuations(ticker:str, base:dict, est:dict, adv:Dict[str,float], settings:Dict[str,Any]) -> Tuple[pd.DataFrame, Dict[str,Any]]:
    price=_coerce_float(base.get("price")); ccy=base.get("currency") or "USD"
    eps0=_coerce_float(base.get("eps_ttm")); pe_ttm=_coerce_float(base.get("pe_ttm")); pe_fwd=_coerce_float(base.get("pe_fwd"))
    ev_rev=_coerce_float(base.get("ev_rev")); ev_ebitda=_coerce_float(base.get("ev_ebitda")); pb=_coerce_float(base.get("pb"))
    ev=_coerce_float(base.get("ev")); mcap=_coerce_float(base.get("market_cap")); shares=_coerce_float(base.get("shares_out"))
    eps1,eps2,eps3 = est.get("eps1"),est.get("eps2"),est.get("eps3")
    rev1,rev2,rev3 = est.get("rev1"),est.get("rev2"),est.get("rev3")

    # PE-ankare = mitten av TTM & FWD om båda finns
    anchor_pe = 0.5*(pe_ttm+pe_fwd) if pe_ttm and pe_fwd and not np.isnan(pe_ttm) and not np.isnan(pe_fwd) else (pe_ttm or pe_fwd)
    drift=_coerce_float(settings.get("mult_drift_per_year"), DEFAULT_MULT_DRIFT)

    rows=[]; meta={"pe_anchor":anchor_pe,"ccy":ccy}

    def _pe_price(eps,mult):
        if eps is None or np.isnan(eps) or mult is None or np.isnan(mult): return np.nan
        return float(eps)*float(mult)
    pe0=anchor_pe; pe1=_apply_multiple_drift(pe0,1,drift); pe2=_apply_multiple_drift(pe0,2,drift); pe3=_apply_multiple_drift(pe0,3,drift)
    rows.append(dict(Metod="pe_hist_vs_eps", Idag=_pe_price(eps0,pe0), **{"1 år":_pe_price(eps1,pe1),"2 år":_pe_price(eps2,pe2),"3 år":_pe_price(eps3,pe3)}))

    def _price_from_ev(target_ev):
        if target_ev is None or np.isnan(target_ev): return np.nan
        mc_ratio = (mcap/ev) if ev and mcap and not np.isnan(ev) and not np.isnan(mcap) and ev>0 else 1.0
        target_mcap = target_ev*mc_ratio
        return target_mcap/float(shares) if shares and shares>0 else np.nan

    # EV/S
    evs0=ev_rev; evs1=_apply_multiple_drift(evs0,1,drift); evs2=_apply_multiple_drift(evs0,2,drift); evs3=_apply_multiple_drift(evs0,3,drift)
    def _ev_from_rev(mult, rev): return float(mult)*float(rev) if mult and rev and not np.isnan(mult) and not np.isnan(rev) else np.nan
    rows.append(dict(Metod="ev_sales",
                     Idag=_price_from_ev(_ev_from_rev(evs0, base.get("rev_ttm"))),
                     **{"1 år":_price_from_ev(_ev_from_rev(evs1,rev1)),
                        "2 år":_price_from_ev(_ev_from_rev(evs2,rev2)),
                        "3 år":_price_from_ev(_ev_from_rev(evs3,rev3))}))

    # EV/EBITDA
    e0=ev_ebitda; e1=_apply_multiple_drift(e0,1,drift); e2=_apply_multiple_drift(e0,2,drift); e3=_apply_multiple_drift(e0,3,drift)
    ebitda_ttm=_coerce_float(base.get("ebitda_ttm"))
    if (not ebitda_ttm or np.isnan(ebitda_ttm)) and base.get("rev_ttm") and adv.get("FCF_margin"):
        ebitda_ttm=float(base["rev_ttm"])*float(adv["FCF_margin"])
    def _ebitda_from_rev(rev):
        if not rev or np.isnan(rev): return np.nan
        if ebitda_ttm and base.get("rev_ttm") and base["rev_ttm"]>0: return rev*(ebitda_ttm/base["rev_ttm"])
        return np.nan
    def _price_from_e_mult(mult, e_level):
        if not mult or np.isnan(mult) or not e_level or np.isnan(e_level): return np.nan
        return _price_from_ev(float(mult)*float(e_level))
    rows.append(dict(Metod="ev_ebitda",
                     Idag=_price_from_e_mult(e0,ebitda_ttm),
                     **{"1 år":_price_from_e_mult(e1,_ebitda_from_rev(rev1)),
                        "2 år":_price_from_e_mult(e2,_ebitda_from_rev(rev2)),
                        "3 år":_price_from_e_mult(e3,_ebitda_from_rev(rev3))}))

    # P/B, P/TBV, P/NAV
    pb=_coerce_float(pb)
    bvps=adv.get("BVPS",np.nan); tbvps=adv.get("TBVPS",np.nan); navps=adv.get("NAVPS",np.nan)
    rows.append(dict(Metod="p_b",
                     Idag=(bvps*pb if bvps and pb else np.nan),
                     **{"1 år":bvps*_apply_multiple_drift(pb,1,drift) if bvps and pb else np.nan,
                        "2 år":bvps*_apply_multiple_drift(pb,2,drift) if bvps and pb else np.nan,
                        "3 år":bvps*_apply_multiple_drift(pb,3,drift) if bvps and pb else np.nan}))
    p_nav_mult = (price/navps) if navps and price else np.nan
    rows.append(dict(Metod="p_nav",
                     Idag=(navps*p_nav_mult if navps and p_nav_mult else np.nan),
                     **{"1 år":navps*_apply_multiple_drift(p_nav_mult,1,drift) if navps and p_nav_mult else np.nan,
                        "2 år":navps*_apply_multiple_drift(p_nav_mult,2,drift) if navps and p_nav_mult else np.nan,
                        "3 år":navps*_apply_multiple_drift(p_nav_mult,3,drift) if navps and p_nav_mult else np.nan}))
    rows.append(dict(Metod="p_tbv",
                     Idag=(tbvps*pb if tbvps and pb else np.nan),
                     **{"1 år":tbvps*_apply_multiple_drift(pb,1,drift) if tbvps and pb else np.nan,
                        "2 år":tbvps*_apply_multiple_drift(pb,2,drift) if tbvps and pb else np.nan,
                        "3 år":tbvps*_apply_multiple_drift(pb,3,drift) if tbvps and pb else np.nan}))

    # REIT/BDC proxys (P/AFFO, P/NII)
    pe_ttm=_coerce_float(pe_ttm)
    affops=adv.get("AFFOPS",np.nan); niips=adv.get("NIIps",np.nan)
    rows.append(dict(Metod="p_affo",
                     Idag=(affops*pe_ttm if affops and pe_ttm else np.nan),
                     **{"1 år":affops*_apply_multiple_drift(pe_ttm,1,drift) if affops and pe_ttm else np.nan,
                        "2 år":affops*_apply_multiple_drift(pe_ttm,2,drift) if affops and pe_ttm else np.nan,
                        "3 år":affops*_apply_multiple_drift(pe_ttm,3,drift) if affops and pe_ttm else np.nan}))
    rows.append(dict(Metod="p_nii",
                     Idag=(niips*pe_ttm if niips and pe_ttm else np.nan),
                     **{"1 år":niips*_apply_multiple_drift(pe_ttm,1,drift) if niips and pe_ttm else np.nan,
                        "2 år":niips*_apply_multiple_drift(pe_ttm,2,drift) if niips and pe_ttm else np.nan,
                        "3 år":niips*_apply_multiple_drift(pe_ttm,3,drift) if niips and pe_ttm else np.nan}))

    # FCF (proxys)
    fcfps=adv.get("FCFps",np.nan)
    if (not fcfps or np.isnan(fcfps)) and base.get("rev_ttm") and shares and adv.get("FCF_margin"):
        fcf_total=base["rev_ttm"]*adv["FCF_margin"]; fcfps=fcf_total/shares
    rows.append(dict(Metod="p_fcf",
                     Idag=(fcfps*pe_ttm if fcfps and pe_ttm else np.nan),
                     **{"1 år":fcfps*_apply_multiple_drift(pe_ttm,1,drift) if fcfps and pe_ttm else np.nan,
                        "2 år":fcfps*_apply_multiple_drift(pe_ttm,2,drift) if fcfps and pe_ttm else np.nan,
                        "3 år":fcfps*_apply_multiple_drift(pe_ttm,3,drift) if fcfps and pe_ttm else np.nan}))
    ev_ebitda=_coerce_float(base.get("ev_ebitda"))
    rows.append(dict(Metod="ev_fcf",
                     Idag=(fcfps*ev_ebitda if fcfps and ev_ebitda else np.nan),
                     **{"1 år":fcfps*_apply_multiple_drift(ev_ebitda,1,drift) if fcfps and ev_ebitda else np.nan,
                        "2 år":fcfps*_apply_multiple_drift(ev_ebitda,2,drift) if fcfps and ev_ebitda else np.nan,
                        "3 år":fcfps*_apply_multiple_drift(ev_ebitda,3,drift) if fcfps and ev_ebitda else np.nan}))

    return pd.DataFrame(rows), meta

# ---------- Data-fliken ----------
def load_data_df(data_ws: gspread.Worksheet) -> pd.DataFrame:
    df=_df_from_ws(data_ws)
    if df.empty:
        seed=pd.DataFrame({"Ticker":["NVDA"],"Bolagsnamn":["NVIDIA Corporation"],"Sektor":["Tech"],"Valuta":["USD"],
                           "Antal aktier":[0],"GAV (SEK)":[0],"Bucket":["Bucket A tillväxt"],"Primär metod":[""]})
        _write_df_to_ws(data_ws, seed); df=seed.copy()
    for c in ["Antal aktier","GAV (SEK)"]:
        if c in df.columns: df[c]=df[c].apply(_coerce_float)
    if "Bucket" not in df.columns: df["Bucket"]=""
    if "Primär metod" not in df.columns: df["Primär metod"]=""
    df["Ticker"]=df["Ticker"].astype(str).str.upper().str.strip()
    if "Valuta" in df.columns: df["Valuta"]=df["Valuta"].astype(str).str.upper().str.strip()
    return df

def withholding_map(settings: Dict[str,Any]) -> Dict[str,float]:
    out={}
    for it in settings.get("withholding",[]):
        out[(it.get("currency") or "").upper()] = _coerce_float(it.get("pct"),0.0)
    if "SEK" not in out: out["SEK"]=0.0
    return out

def forecast_dividends(base:dict, ccy:str, fx_map:Dict[str,float], settings:Dict[str,Any], shares_owned:float)->Dict[str,float]:
    dps0=_coerce_float(base.get("dividend_ttm"),0.0)
    div_cagr_default=_coerce_float(settings.get("dividend_cagr_default"),0.03)
    cagr=div_cagr_default
    try:
        divs=yf.Ticker(base.get("ticker")).dividends
        if divs is not None and len(divs)>0:
            byy=divs.groupby(divs.index.year).sum()
            if len(byy)>=3:
                c=_cagr(float(byy.iloc[-3]), float(byy.iloc[-1]), 2.0)
                if not np.isnan(c): cagr=c
    except: pass
    d1=dps0*(1+cagr); d2=d1*(1+cagr); d3=d2*(1+cagr)
    fx=fx_map.get(ccy.upper(), np.nan)
    tax=withholding_map(settings).get(ccy.upper(),0.0)/100.0
    def _net_sek(x):
        if np.isnan(x) or np.isnan(fx): return 0.0
        return x*shares_owned*fx*(1.0-tax)
    return {"y1":_net_sek(d1),"y2":_net_sek(d2),"y3":_net_sek(d3),"tax_pct":tax*100}

def pick_primary_method(bucket:str, explicit:str, methods_df:pd.DataFrame)->str:
    return explicit if explicit else BUCKET_PRIMARY.get(bucket,"ev_ebitda")

def extract_primary_prices(methods_df:pd.DataFrame, method_name:str)->Dict[str,float]:
    row=methods_df[methods_df["Metod"]==method_name]
    if row.empty: return {"today":np.nan,"y1":np.nan,"y2":np.nan,"y3":np.nan}
    r=row.iloc[0]
    return {"today":_coerce_float(r["Idag"]), "y1":_coerce_float(r["1 år"]),
            "y2":_coerce_float(r["2 år"]), "y3":_coerce_float(r["3 år"])}

def build_ranking(df_data:pd.DataFrame, settings:Dict[str,Any], fx_map:Dict[str,float]) -> pd.DataFrame:
    rows=[]
    adv_map=load_advanced_map(ensure_sheets()["adv_ws"])
    for _, row in df_data.iterrows():
        ticker=str(row["Ticker"]).upper().strip()
        bucket=str(row.get("Bucket","")); explicit=str(row.get("Primär metod","")).strip()
        base=fetch_yahoo_basics(ticker)
        est=fetch_eps_rev_estimates(ticker, settings.get("rev_cagr_default",0.10), settings.get("eps_cagr_default",0.10))
        methods, meta=build_valuations(ticker, base, est, adv_map.get(ticker,{}), settings)
        prim=pick_primary_method(bucket, explicit, methods)
        prices=extract_primary_prices(methods, prim)
        price_now=_coerce_float(base.get("price"))
        up = ((prices["today"]/price_now - 1.0)*100.0) if prices["today"] and price_now else np.nan
        rows.append({"Ticker":ticker,"Namn":base.get("name"),"Bucket":bucket,"Valuta":base.get("currency",""),
                     "Pris":price_now,"Primär metod":prim,"FV idag":prices["today"],"Uppsida idag (%)":up,
                     "Under FV": bool(prices["today"] and price_now and prices["today"]>price_now)})
        time.sleep(0.12)
    r=pd.DataFrame(rows)
    return r.sort_values(by="Uppsida idag (%)", ascending=False, na_position="last").reset_index(drop=True)

# ---------- Portföljsummering (SEK), andelar & spar ----------
def compute_portfolio_summary(df_data:pd.DataFrame, fx_map:Dict[str,float]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows=[]; by_bucket={}
    for _, r in df_data.iterrows():
        t=str(r["Ticker"]).upper().strip()
        owns=_coerce_float(r.get("Antal aktier"),0.0); bucket=str(r.get("Bucket",""))
        base=fetch_yahoo_basics(t); ccy=base.get("currency","USD"); price=_coerce_float(base.get("price"))
        fx=fx_map.get(ccy.upper(), np.nan)
        sek = price*owns*fx if price and owns and fx and not np.isnan(fx) else 0.0
        rows.append({"Ticker":t,"Bucket":bucket,"Valuta":ccy,"Antal aktier":owns,"Pris":price,"Värde (SEK)":sek})
        by_bucket[bucket]=by_bucket.get(bucket,0.0)+sek
        time.sleep(0.05)
    pos=pd.DataFrame(rows)
    total=float(pos["Värde (SEK)"].sum()) if not pos.empty else 0.0
    if not pos.empty and total>0:
        pos["Andel av total (%)"]=pos["Värde (SEK)"]/total*100.0
    bdf=pd.DataFrame([{"Bucket":b,"Värde (SEK)":v,"Andel av total (%)":(v/total*100.0 if total>0 else 0.0)} for b,v in by_bucket.items()])
    bdf=bdf.sort_values("Värde (SEK)", ascending=False)
    return pos.sort_values("Värde (SEK)", ascending=False), bdf

def write_portfolio_summary(port_ws:gspread.Worksheet, positions:pd.DataFrame, buckets:pd.DataFrame):
    top = positions.copy(); top.insert(0,"Timestamp",now_iso())
    sep = pd.DataFrame([{"Timestamp":now_iso(),"Ticker":"—","Bucket":"—","Valuta":"—","Antal aktier":"—","Pris":"—","Värde (SEK)":"—","Andel av total (%)":"—"}])
    bottom = buckets.copy(); bottom.insert(0,"Timestamp",now_iso())
    df = pd.concat([top, sep, bottom], ignore_index=True)
    _write_df_to_ws(port_ws, df)

def save_primary_targets(res_ws: gspread.Worksheet, row_ctx: Dict[str, Any]):
    df_old=_df_from_ws(res_ws); new=pd.DataFrame([row_ctx])
    df=pd.concat([df_old,new], ignore_index=True) if not df_old.empty else new
    _write_df_to_ws(res_ws, df)

def save_quarter_snapshot(snap_ws:gspread.Worksheet, ticker:str, methods_df:pd.DataFrame, meta:Dict[str,Any]):
    md=methods_df.copy(); md.insert(0,"Ticker",ticker); md.insert(1,"Timestamp",now_iso()); md.insert(2,"Meta",json.dumps(meta))
    old=_df_from_ws(snap_ws)
    df=pd.concat([old,md], ignore_index=True) if not old.empty else md
    _write_df_to_ws(snap_ws, df)

# ---------- UI: Ranking ----------
def show_ranking(df_data:pd.DataFrame, settings:Dict[str,Any], fx_map:Dict[str,float]):
    st.markdown("## 🏁 Rangordning")
    c1,c2,c3=st.columns(3)
    bucket_filter=c1.selectbox("Bucket", options=["(alla)"]+sorted(df_data["Bucket"].dropna().unique().tolist()))
    only_under=c2.checkbox("Visa endast under fair value (FV idag > pris)")
    only_not_owned=c3.checkbox("Visa endast tickers jag **inte** äger")

    df=df_data.copy()
    if bucket_filter!="(alla)": df=df[df["Bucket"]==bucket_filter]
    if only_not_owned and "Antal aktier" in df.columns:
        df=df[(_coerce_float(df["Antal aktier"]))==0]

    with st.spinner("Beräknar…"):
        rank=build_ranking(df, settings, fx_map)
    if only_under:
        rank=rank[rank["Under FV"]==True]
    st.dataframe(rank, use_container_width=True)

# ---------- UI: Detalj per bolag ----------
def show_company_panel(row:pd.Series, fx_map:Dict[str,float], settings:Dict[str,Any], sheets:Dict[str,Any]):
    ticker=str(row["Ticker"]).upper().strip()
    st.markdown(f"### 🔎 Detaljer: **{ticker}** • {row.get('Bolagsnamn','')} • {row.get('Bucket','')}")
    base=fetch_yahoo_basics(ticker)
    adv_map=load_advanced_map(sheets["adv_ws"]); adv=adv_map.get(ticker,{})
    est=fetch_eps_rev_estimates(ticker, settings.get("rev_cagr_default",0.10), settings.get("eps_cagr_default",0.10))
    methods, meta=build_valuations(ticker, base, est, adv, settings)
    ccy=base.get("currency",""); price_now=_coerce_float(base.get("price"))

    explicit=str(row.get("Primär metod","")); prim=pick_primary_method(row.get("Bucket",""), explicit, methods)
    prices=extract_primary_prices(methods, prim)

    st.caption(f"Sanity • pris={_fmt_money(price_now, ccy)} • pe_ttm={_fmt_num(base.get('pe_ttm'))} • pe_fwd={_fmt_num(base.get('pe_fwd'))} • ev/s={_fmt_num(base.get('ev_rev'))} • ev/ebitda={_fmt_num(base.get('ev_ebitda'))}")
    st.dataframe(methods.replace({None:np.nan}).fillna(""), use_container_width=True)

    st.markdown("#### 🎯 Primär riktkurs")
    c = st.columns(4)
    delta_today = f"{((prices['today']/price_now-1)*100):.1f}%" if prices['today'] and price_now else None
    c[0].metric("Idag", _fmt_money(prices["today"], ccy), delta=delta_today)
    c[1].metric("1 år", _fmt_money(prices["y1"], ccy))
    c[2].metric("2 år", _fmt_money(prices["y2"], ccy))
    c[3].metric("3 år", _fmt_money(prices["y3"], ccy))

    st.markdown("#### 💰 Utdelning (netto, SEK)")
    owns=_coerce_float(row.get("Antal aktier"),0.0)
    divf=forecast_dividends(base, ccy, fx_map, settings, owns)
    st.write(f"• Nästa år: **{_fmt_money(divf['y1'],'SEK')}**  •  2 år: **{_fmt_money(divf['y2'],'SEK')}**  •  3 år: **{_fmt_money(divf['y3'],'SEK')}**")
    st.caption(f"Källskatt: {divf['tax_pct']:.0f}% • Antal aktier: {int(owns)}")

    st.markdown("#### 🧾 Innehavsvärde (SEK)")
    fx=fx_map.get(ccy.upper(), np.nan)
    port_val = price_now*owns*fx if price_now and owns and fx and not np.isnan(fx) else 0.0
    st.write(f"Totalt värde nu: **{_fmt_money(port_val,'SEK')}**")

    b1,b2=st.columns(2)
    if b1.button("💾 Spara primära riktkurser till Resultat"):
        save_primary_targets(sheets["res_ws"], {
            "Timestamp":now_iso(),"Ticker":ticker,"Namn":base.get("name"),"Valuta":ccy,"Pris nu":price_now,
            "Primär metod":prim,"FV idag":prices["today"],"FV 1 år":prices["y1"],"FV 2 år":prices["y2"],"FV 3 år":prices["y3"],
        })
        st.success("Sparat till *Resultat*.")
    if b2.button("🗂️ Spara kvartalssnapshot"):
        save_quarter_snapshot(sheets["snap_ws"], ticker, methods, meta)
        st.success("Snapshot sparad i *Snapshots*.")

# ---------- UI: Portföljsektion ----------
def show_portfolio_panel(df_data:pd.DataFrame, fx_map:Dict[str,float], sheets:Dict[str,Any]):
    st.markdown("## 💼 Portfölj (SEK)")
    pos, bdf = compute_portfolio_summary(df_data, fx_map)
    total = float(pos["Värde (SEK)"].sum()) if not pos.empty else 0.0
    c1,c2 = st.columns(2)
    c1.metric("Totalt portföljvärde", _fmt_money(total, "SEK"))
    if not bdf.empty:
        for _, r in bdf.iterrows():
            c2.metric(f"{r['Bucket']}", _fmt_money(r["Värde (SEK)"],"SEK"), f"{r['Andel av total (%)']:.1f}%")
    st.dataframe(pos, use_container_width=True)
    st.dataframe(bdf, use_container_width=True)
    if st.button("💾 Spara summering till fliken Portfölj"):
        write_portfolio_summary(sheets["port_ws"], pos, bdf)
        st.success("Portföljsummering sparad.")

# ---------- Huvud-UI ----------
def run_main_ui():
    sheets = ensure_sheets()
    data_df = load_data_df(sheets["data_ws"])
    settings = _load_settings(sheets["set_ws"])

    # FX: säkerställ kurser för alla valutor i Data
    currencies = sorted(list(set(data_df["Valuta"].dropna().astype(str).str.upper().tolist())))
    fx_df = _df_from_ws(sheets["fx_ws"])
    known = set(fx_df["Currency"].str.upper().tolist()) if (not fx_df.empty and "Currency" in fx_df.columns) else set()
    missing = [c for c in currencies if c not in known]
    if missing:
        with st.spinner("Hämtar valutakurser…"): refresh_fx_table(sheets["fx_ws"], currencies)
    fx_map = load_fx_map(sheets["fx_ws"])

    # Välj rad/ticker
    st.sidebar.header("⚙️ Val")
    tickers = data_df["Ticker"].astype(str).tolist()
    ix = st.sidebar.selectbox("Välj rad", options=list(range(len(tickers))), format_func=lambda i: f"{tickers[i]} • {data_df.loc[i,'Bolagsnamn']}")
    row = data_df.loc[ix]

    # Ranking + filter
    show_ranking(data_df, settings, fx_map)
    st.markdown("---")

    # Portfölj
    show_portfolio_panel(data_df, fx_map, sheets)
    st.markdown("---")

    # Detaljvy
    show_company_panel(row, fx_map, settings, sheets)

def main():
    try: run_main_ui()
    except Exception as e: st.error(f"💥 Fel: {e}")

if __name__ == "__main__":
    main()
