# app.py — Riktkurser + Google Sheets (v4.3, split 1/4)
# Nytt/ändrat:
# - Robust pris-normalisering (hanteras i Del 2) för Yahoo (fast_info i cent → ÷100).
# - FX hämtas & loggas (Del 2), portföljsummering i SEK (Del 4).
# - Kvartalssnapshot till fliken "Historik".
# - Rangordning, export till "Resultat".
# - Buckets, filter äger/ej, utd-data.

from __future__ import annotations
import math, json, re, time
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
import gspread
import requests
from google.oauth2.service_account import Credentials

# ============== Grundlayout ==============
st.set_page_config(page_title="Riktkurser + Sheets", page_icon="📈", layout="wide")
st.title("📈 Riktkurser + Google Sheets (Buckets, auto-FX, multi-metod)")

# Visa service-kontot i sidofältet (om finns)
try:
    st.sidebar.caption("Service-konto: " + st.secrets["GOOGLE_CREDENTIALS"].get("client_email", "<saknas>"))
except Exception:
    pass

# ============== Hjälpfunktioner ==============
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def yq_of(dt: Optional[datetime] = None) -> Tuple[int, int, str]:
    d = dt or datetime.now()
    q = (d.month - 1)//3 + 1
    return d.year, q, f"{d.year}-Q{q}"

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b in (None, 0): return default
        return float(a) / float(b)
    except Exception:
        return default

def nz(x, default=0.0):
    try:
        if x is None: return default
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)): return default
        return x
    except Exception:
        return default

def fmt2(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))): return ""
    return f"{x:.2f}"

def grow(v: float, g: float) -> float:
    return v * (1.0 + g)

def multi_year(v0: float, g1: float, g2: float, g3: float) -> Tuple[float, float, float]:
    y1 = grow(v0, g1); y2 = grow(y1, g2); y3 = grow(y2, g3)
    return y1, y2, y3

def bull_bear(value_1y: float, bull_mult: float = 1.15, bear_mult: float = 0.85) -> Tuple[float, float]:
    return value_1y * bull_mult, value_1y * bear_mult

# ============== Kolumnuppsättningar för Sheets ==============
REQUIRED_DATA_COLS = [
    "Timestamp","Ticker","Bolagsnamn","Sektor","Industri","Valuta",
    "Bucket","Antal aktier",
    "Preferred metod","G1","G2","G3",
    "PE_hist","EPS0",
    "EV_FCF_mult","P_FCF_mult","EV_S_mult","EV_EBITDA_mult",
    "P_NAV_mult","P_AFFO_mult","P_B_mult","P_TBV_mult","P_NII_mult",
    "TBV_ps0","ROTCE","Payout",
    "AFFO_ps0","NAV_ps0","NII_ps0","BV_ps0","FCF_ps0",
    "Last Price","Market Cap","EV","Shares Out",
    "Revenue TTM","EBITDA TTM","FCF TTM","Total Debt","Cash",
    "Dividend/ps","Dividend Yield",
]

RESULT_COLS = [
    "Timestamp","Ticker","Valuta","Aktuell kurs (0)",
    "Riktkurs idag","Riktkurs 1 år","Riktkurs 2 år","Riktkurs 3 år",
    "Bull 1 år","Bear 1 år","Metod","Input-sammanfattning","Kommentar"
]

HIST_COLS = [
    "Key","Timestamp","YQ","Ticker","Namn","Valuta","Pris",
    "PE_p25","PE_p50","PE_p75","PE_hist","G1","G2","G3","Primär metod",
    "Fair_today","Fair_1y","Fair_2y","Fair_3y","Bull_1y","Bear_1y","Upside_%",
    "EPS_TTM","BV_ps0","FCF_ps0","NII_ps0","AFFO_ps0","NAV_ps0","TBV_ps0",
    "Shares_Out","Sektor","Industri"
]

# FX-symboler för Yahoo
FX_SYMBOLS = {
    "USD":"USDSEK=X","EUR":"EURSEK=X","NOK":"NOKSEK=X",
    "CAD":"CADSEK=X","GBP":"GBPSEK=X","DKK":"DKKSEK=X",
    "SEK":None
}

# ============== Google Sheets I/O ==============
def col_idx_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    creds_raw = st.secrets.get("GOOGLE_CREDENTIALS")
    creds_dict = json.loads(creds_raw) if isinstance(creds_raw, str) else dict(creds_raw)
    creds_dict = _normalize_private_key(creds_dict)
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scope))

def _sheet_id_from_url_or_id(val: str) -> str:
    val = (val or "").strip()
    if "/" not in val and " " not in val and val:
        return val
    m = re.search(r"/d/([a-zA-Z0-9-_]+)", val)
    if not m:
        raise ValueError("Kunde inte hitta Sheet-ID i SHEET_URL/SHEET_ID")
    return m.group(1)

def _ensure_ws(sh: gspread.Spreadsheet, title: str, cols: List[str]) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2, cols=max(50, len(cols)))
        ws.append_row(cols)
        return ws
    header = ws.row_values(1)
    if header != cols:
        ws.clear()
        ws.append_row(cols)
    return ws

@st.cache_resource(show_spinner=False)
def open_sheets():
    sheet_id = st.secrets.get("SHEET_ID","") or _sheet_id_from_url_or_id(st.secrets.get("SHEET_URL",""))
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws_data = _ensure_ws(sh, "Data", REQUIRED_DATA_COLS)
    ws_fx   = _ensure_ws(sh, "Valutakurser", ["Timestamp","Valuta","SEK_per_unit"])
    ws_res  = _ensure_ws(sh, "Resultat", RESULT_COLS)
    ws_hist = _ensure_ws(sh, "Historik", HIST_COLS)
    return sh, ws_data, ws_fx, ws_res, ws_hist

def read_df(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = ws.get_all_records()
    if not vals:
        return pd.DataFrame(columns=ws.row_values(1))
    df = pd.DataFrame(vals)
    for c in ws.row_values(1):
        if c not in df.columns:
            df[c] = np.nan
    return df

def upsert_row(ws: gspread.Worksheet, key_col: str, key_val: str, row_dict: Dict[str, Any]):
    df = read_df(ws)
    header = ws.row_values(1)
    if df.empty:
        ws.append_row([row_dict.get(c, "") for c in header])
        return
    idx = df.index[df.get(key_col, pd.Series(dtype=object)) == key_val].tolist()
    if idx:
        rnum = idx[0] + 2  # 1-baserad + rubrikrad
        existing = ws.row_values(rnum)
        new_row = [row_dict.get(col, existing[i] if i < len(existing) else "") for i, col in enumerate(header)]
        ws.update(f"A{rnum}:{col_idx_to_a1(len(header))}{rnum}", [new_row])
    else:
        ws.append_row([row_dict.get(c, "") for c in header])

# ========== Del 2/4 — Datakällor (Yahoo/FX/Finnhub/SEC), motor & heuristik ==========

# ---------- Små hjälpare ----------
def _first(*vals):
    for v in vals:
        if v is None: 
            continue
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            continue
        return v
    return None

def _df_pick_first(df: pd.DataFrame, keys: List[str]) -> Optional[float]:
    """För Yahoo statements: försök hitta första icke-NaN-värdet för 'keys' i index."""
    try:
        if df is None or df.empty:
            return None
        idxs = {str(i).strip().lower(): i for i in df.index}
        for k in keys:
            lk = k.strip().lower()
            if lk in idxs:
                ser = df.loc[idxs[lk]]
                if isinstance(ser, pd.Series):
                    ser = ser.dropna()
                    if not ser.empty:
                        return float(ser.iloc[0])
    except Exception:
        pass
    return None

# ---------- Yahoo pris-normalisering & FX ----------
def _fi_get(fi_obj, key):
    """yfinance.fast_info kan vara ett objekt med attribut eller dict-liknande."""
    try:
        if hasattr(fi_obj, key):
            return getattr(fi_obj, key)
        return fi_obj.get(key)
    except Exception:
        return None

def _norm_price(px, mcap=None, shares=None):
    """Normalisera pris. Om px ser ut att vara i cent (mycket stort tal), dividera med 100.
       Använder även mcap/shares som referens där det går."""
    try:
        if px is None:
            return None
        px = float(px)

        ref = None
        if mcap and shares and float(shares) > 0:
            ref = float(mcap) / float(shares)

        if ref and ref > 0:
            # px i cent?
            if px > 10000 and abs(px/100.0 - ref) / ref < 0.03:
                return px / 100.0
            # px redan rätt skala?
            if abs(px - ref) / ref < 0.03:
                return px
            # ingen bra match: antag cent om px väldigt stort
            if px > 10000:
                return px / 100.0
            return px

        # Utan referens
        if px > 10000:   # typiskt "cent"
            return px / 100.0
        return px
    except Exception:
        return px

def _resolve_price(fi_last, info_rmp, info_cp, hist_px, mcap=None, shares=None) -> Optional[float]:
    """Prioritet: regularMarketPrice → history close → currentPrice → fast_info.last_price.
       Varje kandidat normaliseras via _norm_price."""
    candidates = [info_rmp, hist_px, info_cp, fi_last]
    for c in candidates:
        if c is None:
            continue
        try:
            c = float(c)
            if c <= 0:
                continue
            return _norm_price(c, mcap, shares)
        except Exception:
            continue
    return None

@st.cache_data(show_spinner=False, ttl=900)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    """Hämtar pris, valuta, MC, EV, shares, TTM rev/EBITDA/FCF, skuld/kassa, utdelning samt meta från Yahoo."""
    t = yf.Ticker(ticker)
    snap: Dict[str, Any] = {}

    # fast_info (robust)
    try:
        fi = t.fast_info
    except Exception:
        fi = None
    fi_last = _fi_get(fi, "last_price") if fi is not None else None
    fi_curr = _fi_get(fi, "currency")   if fi is not None else None
    fi_mcap = _fi_get(fi, "market_cap") if fi is not None else None

    # info
    try:
        info = t.info or {}
    except Exception:
        info = {}

    # history-close som extra säker pris-källa
    hist_px = None
    try:
        hist = t.history(period="5d", interval="1d")
        if hist is not None and not hist.empty:
            hist_px = float(hist["Close"].dropna().iloc[-1])
    except Exception:
        pass

    # grunddata
    currency   = fi_curr or info.get("currency")
    market_cap = fi_mcap or info.get("marketCap")
    shares_out = info.get("sharesOutstanding")

    # statements
    try:
        income = getattr(t, "income_stmt", None)
        if income is None or income.empty:
            income = getattr(t, "financials", pd.DataFrame())
    except Exception:
        income = pd.DataFrame()
    try:
        cashf = getattr(t, "cash_flow", None)
        if cashf is None or cashf.empty:
            cashf = getattr(t, "cashflow", pd.DataFrame())
    except Exception:
        cashf = pd.DataFrame()
    try:
        bal = getattr(t, "balance_sheet", pd.DataFrame())
    except Exception:
        bal = pd.DataFrame()

    # plocka TTM-nycklar (bästa ansträngning)
    rev_ttm   = _df_pick_first(income, ["Total Revenue","TotalRevenue","Revenue"])
    ebitda_tt = _df_pick_first(income, ["EBITDA"])
    ocf       = _df_pick_first(cashf, ["Total Cash From Operating Activities","Operating Cash Flow","OperatingCashFlow"])
    capex     = _df_pick_first(cashf, ["Capital Expenditures","CapitalExpenditures"])
    fcf_ttm   = (ocf - abs(capex)) if (ocf is not None and capex is not None) else None

    total_debt = _df_pick_first(bal, ["Total Debt","TotalDebt"])
    cash       = _df_pick_first(bal, ["Cash And Cash Equivalents","Cash","Cash And Short Term Investments","CashAndShortTermInvestments"])

    # härled shares/mcap om något saknas
    try:
        if shares_out is None and market_cap and hist_px:
            shares_out = float(market_cap) / float(hist_px)
        if market_cap is None and shares_out and hist_px:
            market_cap = float(shares_out) * float(hist_px)
    except Exception:
        pass

    # EV
    ev = info.get("enterpriseValue")
    if ev is None and market_cap is not None:
        ev = float(market_cap) + float(nz(total_debt, 0.0)) - float(nz(cash, 0.0))

    # pris (robust normalisering – fixar cent → ÷100)
    px = _resolve_price(
        fi_last,
        info.get("regularMarketPrice"),
        info.get("currentPrice"),
        hist_px,
        mcap=market_cap,
        shares=shares_out
    )

    # utdelning
    div_ps    = info.get("dividendRate") or info.get("trailingAnnualDividendRate")
    div_yield = info.get("dividendYield") or info.get("trailingAnnualDividendYield")

    snap.update({
        "currency": currency or "SEK",
        "last_price": px,
        "market_cap": market_cap,
        "enterprise_value": ev,
        "shares_out": shares_out,
        "short_name": info.get("shortName"),
        "long_name": info.get("longName"),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "revenue_ttm": rev_ttm,
        "ebitda_ttm": ebitda_tt,
        "fcf_ttm": fcf_ttm,
        "total_debt": total_debt,
        "cash": cash,
        "dividend_ps": div_ps,
        "dividend_yield": div_yield,
    })
    return snap

@st.cache_data(show_spinner=False, ttl=900)
def fetch_fx_to_sek(codes: List[str]) -> Dict[str, float]:
    """Hämta FX→SEK via Yahoo (USDSEK=X etc). Historik-close prioriteras, annars fast_info."""
    rates: Dict[str, float] = {}
    for c in codes:
        if not c:
            continue
        if c == "SEK":
            rates[c] = 1.0
            continue
        sym = FX_SYMBOLS.get(c)
        if not sym:
            continue
        try:
            t = yf.Ticker(sym)
            val = None
            hist = t.history(period="3d", interval="1d")
            if hist is not None and not hist.empty:
                val = float(hist["Close"].dropna().iloc[-1])
            if not val:
                fi = t.fast_info
                val = _fi_get(fi, "last_price")
            if val and float(val) > 0:
                rates[c] = float(val)
        except Exception:
            continue
    return rates

def persist_fx(ws_fx: gspread.Worksheet, rates: Dict[str, float]):
    ts = now_ts()
    rows = [[ts, k, round(v, 6)] for k, v in rates.items()]
    if rows:
        ws_fx.append_rows(rows, value_input_option="USER_ENTERED")

# ---------- Finnhub (EPS, BV/ps, P/E-band) ----------
@st.cache_data(show_spinner=False, ttl=1800)
def fetch_finnhub_metrics(symbol: str) -> Dict[str, Any]:
    api = st.secrets.get("FINNHUB_API_KEY", "")
    if not api:
        return {}
    url = f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={api}"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return {}
        data = r.json() or {}
    except Exception:
        return {}
    metric = data.get("metric", {}) or {}
    series = data.get("series", {}) or {}
    eps_ttm = metric.get("epsBasicExclExtraTTM") or metric.get("epsInclExtraTTM") or metric.get("epsTTM")
    book_ps = metric.get("bookValuePerShareAnnual") or metric.get("bookValuePerShareTTM")

    vals: List[float] = []
    def _collect(arr):
        for item in arr or []:
            v = item.get("v")
            if isinstance(v, (int, float)) and not math.isnan(v) and v > 0:
                vals.append(float(v))
    try:
        q = series.get("quarterly", {}) or {}
        for key in ("peBasicExclExtraTTM","peExclExtraTTM","peTTM"):
            _collect(q.get(key, []))
        a = series.get("annual", {}) or {}
        for key in ("peBasicExclExtraAnnual","peExclExtraAnnual","peAnnual"):
            _collect(a.get(key, []))
    except Exception:
        pass

    vals = [v for v in vals if v > 0]
    if len(vals) > 20:
        vals = vals[-20:]

    pe_p25 = np.percentile(vals, 25) if vals else None
    pe_p50 = np.percentile(vals, 50) if vals else None
    pe_p75 = np.percentile(vals, 75) if vals else None
    if pe_p50 is None:
        pe_p50 = metric.get("peExclExtraTTM") or metric.get("peTTM")

    return {"eps_ttm": eps_ttm, "book_ps": book_ps, "pe_band": (pe_p25, pe_p50, pe_p75)}

# ---------- SEC XBRL (beta: NII/FFO/AFFO per aktie) ----------
def _sec_headers():
    ua = st.secrets.get("SEC_USER_AGENT", "BucketApp/1.0 (contact: you@example.com)")
    return {"User-Agent": ua}

@st.cache_data(show_spinner=False, ttl=86400)
def sec_cik_map() -> Dict[str, str]:
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=_sec_headers(), timeout=12)
        if r.status_code != 200:
            return {}
        j = r.json()
        out = {}
        if isinstance(j, dict):
            for _, v in j.items():
                t = (v.get("ticker") or "").upper()
                cik = str(v.get("cik_str") or "").zfill(10)
                if t:
                    out[t] = cik
        elif isinstance(j, list):
            for v in j:
                t = (v.get("ticker") or "").upper()
                cik = str(v.get("cik_str") or "").zfill(10)
                if t:
                    out[t] = cik
        return out
    except Exception:
        return {}

@st.cache_data(show_spinner=False, ttl=3600)
def sec_companyfacts(cik: str) -> Dict[str, Any]:
    if not cik:
        return {}
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        r = requests.get(url, headers=_sec_headers(), timeout=15)
        if r.status_code != 200:
            return {}
        return r.json() or {}
    except Exception:
        return {}

def _sec_latest_per_share(facts: Dict[str, Any], names: List[str]) -> Optional[float]:
    for ns in ("us-gaap", "nareit", "ifrs-full", "dei", "srt"):
        fns = facts.get("facts", {}).get(ns, {})
        for name in names:
            f = fns.get(name)
            if not f:
                continue
            units = f.get("units", {})
            for unit, arr in units.items():
                if "/shares" in unit.lower() or "perShare" in unit:
                    try:
                        arr = sorted(arr, key=lambda x: x.get("end", ""))
                        return float(arr[-1]["val"])
                    except Exception:
                        pass
    return None

def _sec_latest_value(facts: Dict[str, Any], names: List[str], unit_pref: List[str]) -> Optional[float]:
    for ns in ("us-gaap", "nareit", "ifrs-full", "dei", "srt"):
        fns = facts.get("facts", {}).get(ns, {})
        for name in names:
            f = fns.get(name)
            if not f:
                continue
            units = f.get("units", {})
            for u in unit_pref:
                if u in units:
                    try:
                        arr = sorted(units[u], key=lambda x: x.get("end", ""))
                        return float(arr[-1]["val"])
                    except Exception:
                        pass
    return None

def sec_try_nii_affo_ps(symbol: str) -> Dict[str, Optional[float]]:
    """Försök få NII/FFO/AFFO per aktie (BDC/REIT m.fl.). Returnerar dict med ev. nycklar: nii_ps, ffo_ps, affo_ps."""
    cik = sec_cik_map().get(symbol.upper(), "")
    if not cik:
        return {}
    facts = sec_companyfacts(cik)
    if not facts:
        return {}

    # NII per aktie (primärt), annars NII / diluted shares
    nii_ps = _sec_latest_per_share(facts, ["NetInvestmentIncomeLoss","InvestmentIncomeNet","NetInvestmentIncome"])
    if nii_ps is None:
        nii = _sec_latest_value(facts, ["NetInvestmentIncomeLoss","InvestmentIncomeNet","NetInvestmentIncome"], ["USD"])
        diluted_sh = _sec_latest_value(facts, ["WeightedAverageNumberOfDilutedSharesOutstanding"], ["shares"])
        if nii is not None and diluted_sh:
            nii_ps = safe_div(nii, diluted_sh, 0.0)

    # AFFO/FFO per aktie (REIT)
    ffo_ps  = _sec_latest_per_share(facts, ["FundsFromOperations","FundsFromOperationsAndGainsLossesOnDisposalOfProperties"])
    affo_ps = _sec_latest_per_share(facts, ["AdjustedFundsFromOperations","AdjustedFundsFromOperationsBasic"])

    return {"nii_ps": nii_ps, "ffo_ps": ffo_ps, "affo_ps": affo_ps}

# ---------- Värderingsmotor ----------
def target_from_PE(eps0: float, pe_hist: float, g1: float, g2: float, g3: float) -> Tuple[float, float, float, float]:
    today = eps0 * pe_hist
    y1, y2, y3 = multi_year(eps0, g1, g2, g3)
    return today, y1 * pe_hist, y2 * pe_hist, y3 * pe_hist

def per_share_from_EV(multiple: float, metric: float, net_debt: float, shares: float) -> float:
    if not shares or shares <= 0:
        return 0.0
    return (multiple * metric - net_debt) / shares

def targets_from_ev_multiple(metric0: float, multiple: float, net_debt: float, shares: float,
                             g1: float, g2: float, g3: float) -> Tuple[float, float, float, float]:
    today = per_share_from_EV(multiple, metric0, net_debt, shares)
    y1, y2, y3 = multi_year(metric0, g1, g2, g3)
    return (
        today,
        per_share_from_EV(multiple, y1, net_debt, shares),
        per_share_from_EV(multiple, y2, net_debt, shares),
        per_share_from_EV(multiple, y3, net_debt, shares),
    )

def targets_from_price_multiple(metric_ps0: float, multiple: float,
                                g1: float, g2: float, g3: float) -> Tuple[float, float, float, float]:
    today = multiple * metric_ps0
    y1, y2, y3 = multi_year(metric_ps0, g1, g2, g3)
    return today, multiple * y1, multiple * y2, multiple * y3

def project_tbv_per_share(tbv0_ps: float, rotce: float, payout_ratio: float) -> Tuple[float, float, float]:
    g = rotce * (1.0 - payout_ratio)
    return multi_year(tbv0_ps, g, g, g)

def stringify_inputs(d: Dict[str, Any]) -> str:
    parts = []
    for k, v in d.items():
        if isinstance(v, float):
            parts.append(f"{k}:{v:.4f}")
        else:
            parts.append(f"{k}:{str(v)}")
    return "|".join(parts).replace(" ", "_")

# ---------- Heuristik för primär metod ----------
BDC_TICKERS = {"CSWC","PFLT","HRZN","ARCC","MAIN","FSK","OCSL","ORCC"}
REIT_HINTS = {"reit"}
BANK_HINTS = {"bank","banks","thrifts","credit","financial services"}
INSURANCE_HINTS = {"insurance"}
ENERGY_HINTS = {"oil","gas","energy","midstream","mlp"}
SHIPPING_HINTS = {"marine","shipping","tanker","bulk"}
SAAS_HINTS = {"software","application","it services","cloud"}

def choose_primary_method(bucket: str, sector: str, industry: str, ticker: str,
                          has_fcf: bool, has_ebitda: bool) -> str:
    tk = (ticker or "").upper()
    s = (sector or "").lower()
    i = (industry or "").lower()

    if tk in BDC_TICKERS or "bdc" in i:
        return "p_nii"
    if any(k in i for k in REIT_HINTS):
        return "p_affo"
    if any(k in s for k in BANK_HINTS) or any(k in i for k in BANK_HINTS) or any(k in i for k in INSURANCE_HINTS):
        return "p_tbv"
    if any(k in s for k in ENERGY_HINTS) or any(k in i for k in ENERGY_HINTS) or any(k in i for k in SHIPPING_HINTS):
        return "ev_dacf" if has_ebitda else "ev_ebitda"
    if any(k in s for k in SAAS_HINTS) or any(k in i for k in SAAS_HINTS):
        return "ev_fcf" if has_fcf else "ev_sales"

    b = (bucket or "").lower()
    if "tillväxt" in b:
        if has_fcf:
            return "ev_fcf"
        if has_ebitda:
            return "ev_ebitda"
        return "ev_sales"
    else:
        if has_fcf:
            return "p_fcf"
        return "p_b"

# ========== Del 3/4 — UI, öppna Sheets, autofyll & spara en ticker ==========

# ---------- Sidopanel ----------
with st.sidebar:
    st.header("Inställningar & datakällor")
    use_finnhub = st.checkbox("Använd Finnhub (EPS/BV/PE-band)", value=True)
    try_sec = st.checkbox("Försök SEC beta (NII/FFO/AFFO)", value=False)
    autosave_hist = st.checkbox("Autospara kvartalssnapshot efter massuppdatering", value=True)
    bull_mult = st.number_input("Bull × (på 1-års riktkurs)", value=1.15, step=0.05, format="%.2f")
    bear_mult = st.number_input("Bear × (på 1-års riktkurs)", value=0.85, step=0.05, format="%.2f")
    st.caption("Basvaluta/visning: SEK (FX hämtas automatiskt).")

    st.markdown("---")
    st.subheader("Filter")
    bucket_opts = [
        "Bucket A tillväxt","Bucket B tillväxt","Bucket C tillväxt",
        "Bucket A utdelning","Bucket B utdelning","Bucket C utdelning",
    ]
    pick_buckets = st.multiselect("Välj buckets att visa:", bucket_opts, default=bucket_opts)
    only_owned = st.checkbox("Visa endast innehav (>0 aktier)", value=False)
    only_watch = st.checkbox("Visa endast bevakningslista (0 aktier)", value=False)

    st.markdown("---")
    st.subheader("Uppdatering")
    do_mass_refresh = st.button("🔄 Uppdatera alla (Yahoo+Finnhub+SEC beta+FX)")

    st.markdown("---")
    st.caption("Senaste FX→SEK (Yahoo)")
    fx_preview = fetch_fx_to_sek(["USD","EUR","NOK","CAD","GBP","DKK"])
    if fx_preview:
        st.write({k: round(v, 4) for k, v in fx_preview.items()})

# ---------- Lägg till / uppdatera bolag ----------
st.markdown("## ➕ Lägg till/uppdatera bolag")
col = st.columns(5)
ticker_in = col[0].text_input("Ticker (t.ex. NVDA)", "")
bucket_in = col[1].selectbox("Bucket", bucket_opts, index=0)
antal_in = col[2].number_input("Antal aktier", min_value=0, value=0, step=1)
pref_method_in = col[3].selectbox(
    "Preferred metod (valfritt – annars AUTO)",
    ["AUTO","pe_hist_vs_eps","ev_fcf","p_fcf","ev_sales","ev_ebitda","p_nav","ev_dacf","p_affo","p_b","p_tbv","p_nii"],
    index=0
)
g1_in = col[4].number_input("G1 (år 1)", value=0.15, step=0.01, format="%.2f")
g2_in = st.number_input("G2 (år 2)", value=0.12, step=0.01, format="%.2f")
g3_in = st.number_input("G3 (år 3)", value=0.10, step=0.01, format="%.2f")

with st.expander("Avancerat (frivilligt) – multiplar/inputs per metod"):
    c1,c2,c3,c4 = st.columns(4)
    pe_hist = c1.number_input("P/E hist snitt", value=15.0, step=0.5, format="%.2f")
    eps0   = c2.number_input("EPS0 (idag)", value=0.00, step=0.01, format="%.2f")
    ev_fcf_mult = c3.number_input("EV/FCF-multiple", value=18.0, step=0.5, format="%.2f")
    p_fcf_mult  = c4.number_input("P/FCF-multiple", value=20.0, step=0.5, format="%.2f")

    c5,c6,c7,c8 = st.columns(4)
    ev_s_mult = c5.number_input("EV/S-multiple", value=5.0, step=0.5, format="%.2f")
    ev_ebitda_mult = c6.number_input("EV/EBITDA-multiple", value=12.0, step=0.5, format="%.2f")
    p_nav_mult = c7.number_input("P/NAV-multiple", value=1.00, step=0.05, format="%.2f")
    p_affo_mult = c8.number_input("P/AFFO-multiple (REIT)", value=13.0, step=0.5, format="%.2f")

    c9,c10,c11,c12 = st.columns(4)
    p_b_mult   = c9.number_input("P/B-multiple", value=1.50, step=0.05, format="%.2f")
    p_tbv_mult = c10.number_input("P/TBV-multiple (bank)", value=1.20, step=0.05, format="%.2f")
    p_nii_mult = c11.number_input("P/NII-multiple (BDC)", value=10.0, step=0.5, format="%.2f")
    tbv_ps0    = c12.number_input("TBV/aktie (idag)", value=0.00, step=0.01, format="%.2f")

    c13,c14,c15,c16 = st.columns(4)
    rotce  = c13.number_input("ROTCE (t.ex. 0.12=12%)", value=0.12, step=0.01, format="%.2f")
    payout = c14.number_input("Payout-ratio", value=0.30, step=0.05, format="%.2f")
    affo_ps0 = c15.number_input("AFFO/aktie (idag)", value=0.00, step=0.01, format="%.2f")
    nav_ps0  = c16.number_input("NAV/aktie (idag)", value=0.00, step=0.01, format="%.2f")

    c17,c18,c19 = st.columns(3)
    nii_ps0 = c17.number_input("NII/aktie (idag)", value=0.00, step=0.01, format="%.2f")
    bv_ps0  = c18.number_input("BV/aktie (idag)", value=0.00, step=0.01, format="%.2f")
    fcf_ps0 = c19.number_input("FCF/aktie (idag)", value=0.00, step=0.01, format="%.2f")

save_clicked = st.button("💾 Spara till Google Sheets (hämtar Yahoo+Finnhub+SEC beta+FX)")

# ---------- Öppna Sheets ----------
try:
    sh, ws_data, ws_fx, ws_res, ws_hist = open_sheets()
except Exception:
    st.error("Kunde inte öppna Google Sheet. Kontrollera SHEET_ID/SHEET_URL och delning med service-kontot.")
    st.stop()

# ---------- Autofyll från källor ----------
def auto_fill_from_sources(tk: str, row: Dict[str,Any], use_finn: bool, use_sec_beta: bool) -> Dict[str,Any]:
    # Finnhub: EPS TTM / Book value per share + PE-band
    if use_finn:
        fm = fetch_finnhub_metrics(tk) or {}
        if fm.get("eps_ttm") and not row.get("EPS0"):
            row["EPS0"] = fm["eps_ttm"]
        if fm.get("book_ps") and not row.get("BV_ps0"):
            row["BV_ps0"] = fm["book_ps"]
        band = fm.get("pe_band")
        if band and not row.get("PE_hist"):
            row["PE_hist"] = band[1] if band[1] else row.get("PE_hist")
        row["_pe_band"] = band  # bär vidare för ev. snapshot

    # Härled FCF/aktie om vi har FCF TTM och Shares Out
    if not row.get("FCF_ps0"):
        fcf_ttm = nz(row.get("FCF TTM"), 0.0)
        shs = nz(row.get("Shares Out"), 0.0)
        if fcf_ttm and shs:
            try:
                row["FCF_ps0"] = float(fcf_ttm) / float(shs)
            except Exception:
                pass

    # Anta NAV≈BV om REIT/BDC-industri (bästa ansträngning)
    ind = (row.get("Industri") or "").lower()
    if not row.get("NAV_ps0") and ("reit" in ind or "real estate" in ind or tk in BDC_TICKERS):
        if row.get("BV_ps0"):
            row["NAV_ps0"] = row["BV_ps0"]

    # SEC beta: NII/AFFO per aktie om möjligt
    if use_sec_beta:
        try:
            secv = sec_try_nii_affo_ps(tk) or {}
            if secv.get("nii_ps") and not row.get("NII_ps0"):
                row["NII_ps0"] = secv["nii_ps"]
            if not row.get("AFFO_ps0"):
                if secv.get("affo_ps"):
                    row["AFFO_ps0"] = secv["affo_ps"]
                elif secv.get("ffo_ps"):
                    row["AFFO_ps0"] = secv["ffo_ps"]
        except Exception:
            pass
    return row

# ---------- Spara/uppdatera en ticker ----------
def handle_one_ticker_save(ticker: str, bucket: str, antal: int, pref_method: str,
                           g1: float, g2: float, g3: float,
                           adv: Dict[str, Any], use_finn: bool, use_sec_beta: bool):
    tk = ticker.strip().upper()
    snap = fetch_yahoo_snapshot(tk)

    cur = snap.get("currency") or "SEK"
    rates = fetch_fx_to_sek([cur])
    persist_fx(ws_fx, rates)

    row = {
        "Timestamp": now_ts(), "Ticker": tk,
        "Bolagsnamn": snap.get("long_name") or snap.get("short_name") or "",
        "Sektor": snap.get("sector") or "", "Industri": snap.get("industry") or "",
        "Valuta": cur, "Bucket": bucket, "Antal aktier": int(antal),
        "Preferred metod": pref_method, "G1": g1, "G2": g2, "G3": g3,
        "PE_hist": adv.get("pe_hist"), "EPS0": adv.get("eps0"),
        "EV_FCF_mult": adv.get("ev_fcf_mult"), "P_FCF_mult": adv.get("p_fcf_mult"),
        "EV_S_mult": adv.get("ev_s_mult"), "EV_EBITDA_mult": adv.get("ev_ebitda_mult"),
        "P_NAV_mult": adv.get("p_nav_mult"), "P_AFFO_mult": adv.get("p_affo_mult"),
        "P_B_mult": adv.get("p_b_mult"), "P_TBV_mult": adv.get("p_tbv_mult"),
        "P_NII_mult": adv.get("p_nii_mult"), "TBV_ps0": adv.get("tbv_ps0"),
        "ROTCE": adv.get("rotce"), "Payout": adv.get("payout"),
        "AFFO_ps0": adv.get("affo_ps0"), "NAV_ps0": adv.get("nav_ps0"),
        "NII_ps0": adv.get("nii_ps0"), "BV_ps0": adv.get("bv_ps0"),
        "FCF_ps0": adv.get("fcf_ps0"),
        "Last Price": snap.get("last_price") or 0.0,
        "Market Cap": snap.get("market_cap") or 0.0,
        "EV": snap.get("enterprise_value") or 0.0,
        "Shares Out": snap.get("shares_out") or 0.0,
        "Revenue TTM": snap.get("revenue_ttm") or 0.0,
        "EBITDA TTM": snap.get("ebitda_ttm") or 0.0,
        "FCF TTM": snap.get("fcf_ttm") or 0.0,
        "Total Debt": snap.get("total_debt") or 0.0,
        "Cash": snap.get("cash") or 0.0,
        "Dividend/ps": snap.get("dividend_ps") or 0.0,
        "Dividend Yield": snap.get("dividend_yield") or 0.0,
    }

    row = auto_fill_from_sources(tk, row, use_finn, use_sec_beta)
    upsert_row(ws_data, "Ticker", tk, row)
    return row

# ---------- UI-knapp: spara en ----------
if save_clicked and ticker_in:
    adv = dict(
        pe_hist=pe_hist, eps0=eps0, ev_fcf_mult=ev_fcf_mult, p_fcf_mult=p_fcf_mult,
        ev_s_mult=ev_s_mult, ev_ebitda_mult=ev_ebitda_mult, p_nav_mult=p_nav_mult, p_affo_mult=p_affo_mult,
        p_b_mult=p_b_mult, p_tbv_mult=p_tbv_mult, p_nii_mult=p_nii_mult, tbv_ps0=tbv_ps0,
        rotce=rotce, payout=payout, affo_ps0=affo_ps0, nav_ps0=nav_ps0, nii_ps0=nii_ps0,
        bv_ps0=bv_ps0, fcf_ps0=fcf_ps0
    )
    row = handle_one_ticker_save(
        ticker_in, bucket_in, antal_in, pref_method_in, g1_in, g2_in, g3_in,
        adv, use_finnhub, try_sec
    )
    if not row.get("Last Price"):
        st.warning(f"{ticker_in}: pris saknas från Yahoo – försök igen senare.")
    else:
        st.success(f"{ticker_in} sparad/uppdaterad i Google Sheets.")

# ========== Del 4/4 — Beräkna/visa, portföljsummering, massuppdatering, export & snapshots ==========

# --- Läs Data (efter ev. spar) ---
data_df = read_df(ws_data)
if data_df.empty:
    st.info("Lägg till minst ett bolag ovan så visas vyer här nedanför.")
    st.stop()

# --- FX för värdering i SEK (pris visas i bolagsvaluta) ---
fx_map = fetch_fx_to_sek(sorted({(c or "SEK") for c in data_df["Valuta"].tolist()}))
sek_rate_for = lambda c: fx_map.get(c or "SEK", 1.0)

# ---------- Värdering för en rad ----------
def compute_methods_row(r: pd.Series) -> Dict[str, Any]:
    cur = r.get("Valuta") or "SEK"
    px = float(nz(r.get("Last Price"), 0.0))
    mc = float(nz(r.get("Market Cap"), 0.0))
    shares_out = float(nz(r.get("Shares Out"), safe_div(mc, px, 0.0)))

    rev0    = float(nz(r.get("Revenue TTM"), 0.0))
    ebitda0 = float(nz(r.get("EBITDA TTM"), 0.0))
    fcf0    = float(nz(r.get("FCF TTM"), 0.0))
    td      = float(nz(r.get("Total Debt"), 0.0))
    ca      = float(nz(r.get("Cash"), 0.0))
    net_debt = td - ca

    g1 = float(nz(r.get("G1"), 0.15))
    g2 = float(nz(r.get("G2"), 0.12))
    g3 = float(nz(r.get("G3"), 0.10))

    pe_hist      = float(nz(r.get("PE_hist"), 15.0))
    eps0         = float(nz(r.get("EPS0"), 0.0))
    ev_fcf_mult  = float(nz(r.get("EV_FCF_mult"), 18.0))
    p_fcf_mult   = float(nz(r.get("P_FCF_mult"), 20.0))
    ev_s_mult    = float(nz(r.get("EV_S_mult"), 5.0))
    ev_eb_mult   = float(nz(r.get("EV_EBITDA_mult"), 12.0))
    p_nav_mult   = float(nz(r.get("P_NAV_mult"), 1.0))
    p_affo_mult  = float(nz(r.get("P_AFFO_mult"), 13.0))
    p_b_mult     = float(nz(r.get("P_B_mult"), 1.5))
    p_tbv_mult   = float(nz(r.get("P_TBV_mult"), 1.2))
    p_nii_mult   = float(nz(r.get("P_NII_mult"), 10.0))

    tbv_ps0 = float(nz(r.get("TBV_ps0"), 0.0))
    rotce   = float(nz(r.get("ROTCE"), 0.12))
    payout  = float(nz(r.get("Payout"), 0.30))
    affo_ps0 = float(nz(r.get("AFFO_ps0"), 0.0))
    nav_ps0  = float(nz(r.get("NAV_ps0"), 0.0))
    nii_ps0  = float(nz(r.get("NII_ps0"), 0.0))
    bv_ps0   = float(nz(r.get("BV_ps0"), 0.0))
    fcf_ps0  = float(nz(r.get("FCF_ps0"), 0.0))

    has_fcf    = fcf0 > 0.0
    has_ebitda = ebitda0 > 0.0

    vals = {}
    vals["pe_hist_vs_eps"] = target_from_PE(eps0, pe_hist, g1, g2, g3)
    vals["ev_fcf"]   = targets_from_ev_multiple(fcf0,    ev_fcf_mult, net_debt, shares_out, g1, g2, g3)
    vals["p_fcf"]    = targets_from_price_multiple(fcf_ps0, p_fcf_mult, g1, g2, g3)
    vals["ev_sales"] = targets_from_ev_multiple(rev0,    ev_s_mult,   net_debt, shares_out, g1, g2, g3)
    vals["ev_ebitda"]= targets_from_ev_multiple(ebitda0, ev_eb_mult,  net_debt, shares_out, g1, g2, g3)
    vals["p_nav"]    = targets_from_price_multiple(nav_ps0,  p_nav_mult,  g1, g2, g3)
    # ev_dacf: fallback 6x om ev_eb_mult==0
    dacf_mult = 6.0 if math.isclose(ev_eb_mult, 0.0) else ev_eb_mult
    vals["ev_dacf"]  = targets_from_ev_multiple(ebitda0, dacf_mult,   net_debt, shares_out, g1, g2, g3)
    vals["p_affo"]   = targets_from_price_multiple(affo_ps0, p_affo_mult, g1, g2, g3)
    vals["p_b"]      = targets_from_price_multiple(bv_ps0,   p_b_mult,    g1, g2, g3)
    tbv1,tbv2,tbv3   = project_tbv_per_share(tbv_ps0, rotce, payout)
    vals["p_tbv"]    = (p_tbv_mult*tbv_ps0, p_tbv_mult*tbv1, p_tbv_mult*tbv2, p_tbv_mult*tbv3)
    vals["p_nii"]    = targets_from_price_multiple(nii_ps0,  p_nii_mult,  g1, g2, g3)

    # Primär metod
    pref = (r.get("Preferred metod") or "AUTO").strip().lower()
    if pref != "auto" and pref in vals:
        primary = pref
    else:
        primary = choose_primary_method(
            r.get("Bucket",""), r.get("Sektor",""), r.get("Industri",""), r.get("Ticker",""),
            has_fcf=has_fcf, has_ebitda=has_ebitda
        )

    today,y1,y2,y3 = vals.get(primary, (0.0,0.0,0.0,0.0))
    b1,br1 = bull_bear(y1, bull_mult, bear_mult)

    div_ps = float(nz(r.get("Dividend/ps"), 0.0))
    da = float(nz(r.get("Dividend Yield"), 0.0))*100.0 if nz(r.get("Dividend Yield"),0.0) \
         else (safe_div(div_ps, px, 0.0)*100.0 if px>0 else 0.0)

    rate = sek_rate_for(cur)
    innehav_sek = int(nz(r.get("Antal aktier"),0)) * px * rate
    utd_år_sek  = int(nz(r.get("Antal aktier"),0)) * div_ps * rate
    upside = (safe_div(today, px, 0.0) - 1.0)*100.0 if px>0 else 0.0

    inputs = {
        "g1": g1, "g2": g2, "g3": g3, "pe_hist": pe_hist, "eps0": eps0,
        "ev_fcf": ev_fcf_mult, "p_fcf": p_fcf_mult, "ev_s": ev_s_mult, "ev_ebitda": ev_eb_mult,
        "p_nav": p_nav_mult, "p_affo": p_affo_mult, "p_b": p_b_mult, "p_tbv": p_tbv_mult, "p_nii": p_nii_mult,
        "tbv_ps0": tbv_ps0, "rotce": rotce, "payout": payout,
        "affo_ps0": affo_ps0, "nav_ps0": nav_ps0, "nii_ps0": nii_ps0, "bv_ps0": bv_ps0, "fcf_ps0": fcf_ps0,
        "shares_fd": shares_out, "net_debt": net_debt
    }

    return {
        "Ticker": r.get("Ticker"), "Namn": r.get("Bolagsnamn"),
        "Valuta": cur, "Pris": px, "Rate_SEK": rate,
        "Antal": int(nz(r.get("Antal aktier"),0)),
        "Innehav_SEK": innehav_sek, "Utdelning/år_SEK": utd_år_sek, "DA_%": da,
        "Bucket": r.get("Bucket"), "Primär metod": primary,
        "Fair idag": today, "Fair 1y": y1, "Fair 2y": y2, "Fair 3y": y3,
        "Bull 1y": b1, "Bear 1y": br1, "Upside_%": upside,
        "Alla metoder": vals, "Inputs": inputs
    }

# ---------- Mass-snapshot-hjälp ----------
def compute_primary_for_snapshot(row: Dict[str,Any]) -> Tuple[str, Tuple[float,float,float,float], float, float, float]:
    px = float(nz(row.get("Last Price"), 0.0))
    mc = float(nz(row.get("Market Cap"), 0.0))
    shares_out = float(nz(row.get("Shares Out"), safe_div(mc, px, 0.0)))
    rev0    = float(nz(row.get("Revenue TTM"), 0.0))
    ebitda0 = float(nz(row.get("EBITDA TTM"), 0.0))
    fcf0    = float(nz(row.get("FCF TTM"), 0.0))
    td      = float(nz(row.get("Total Debt"), 0.0))
    ca      = float(nz(row.get("Cash"), 0.0))
    net_debt = td - ca

    g1 = float(nz(row.get("G1"), 0.15))
    g2 = float(nz(row.get("G2"), 0.12))
    g3 = float(nz(row.get("G3"), 0.10))

    pe_hist      = float(nz(row.get("PE_hist"), 15.0))
    eps0         = float(nz(row.get("EPS0"), 0.0))
    ev_fcf_mult  = float(nz(row.get("EV_FCF_mult"), 18.0))
    p_fcf_mult   = float(nz(row.get("P_FCF_mult"), 20.0))
    ev_s_mult    = float(nz(row.get("EV_S_mult"), 5.0))
    ev_eb_mult   = float(nz(row.get("EV_EBITDA_mult"), 12.0))
    p_nav_mult   = float(nz(row.get("P_NAV_mult"), 1.0))
    p_affo_mult  = float(nz(row.get("P_AFFO_mult"), 13.0))
    p_b_mult     = float(nz(row.get("P_B_mult"), 1.5))
    p_tbv_mult   = float(nz(row.get("P_TBV_mult"), 1.2))
    p_nii_mult   = float(nz(row.get("P_NII_mult"), 10.0))

    tbv_ps0 = float(nz(row.get("TBV_ps0"), 0.0))
    rotce   = float(nz(row.get("ROTCE"), 0.12))
    payout  = float(nz(row.get("Payout"), 0.30))
    affo_ps0 = float(nz(row.get("AFFO_ps0"), 0.0))
    nav_ps0  = float(nz(row.get("NAV_ps0"), 0.0))
    nii_ps0  = float(nz(row.get("NII_ps0"), 0.0))
    bv_ps0   = float(nz(row.get("BV_ps0"), 0.0))
    fcf_ps0  = float(nz(row.get("FCF_ps0"), 0.0))

    tbv1,tbv2,tbv3 = project_tbv_per_share(tbv_ps0, rotce, payout)

    vals = {
        "pe_hist_vs_eps": target_from_PE(eps0, pe_hist, g1, g2, g3),
        "ev_fcf":   targets_from_ev_multiple(fcf0,    ev_fcf_mult, net_debt, shares_out, g1, g2, g3),
        "p_fcf":    targets_from_price_multiple(fcf_ps0, p_fcf_mult, g1, g2, g3),
        "ev_sales": targets_from_ev_multiple(rev0,    ev_s_mult,   net_debt, shares_out, g1, g2, g3),
        "ev_ebitda":targets_from_ev_multiple(ebitda0, ev_eb_mult,  net_debt, shares_out, g1, g2, g3),
        "p_nav":    targets_from_price_multiple(nav_ps0,  p_nav_mult,  g1, g2, g3),
        "ev_dacf":  targets_from_ev_multiple(ebitda0, 6.0 if math.isclose(ev_eb_mult,0.0) else ev_eb_mult, net_debt, shares_out, g1, g2, g3),
        "p_affo":   targets_from_price_multiple(affo_ps0, p_affo_mult, g1, g2, g3),
        "p_b":      targets_from_price_multiple(bv_ps0,   p_b_mult,    g1, g2, g3),
        "p_tbv":    (p_tbv_mult*tbv_ps0, p_tbv_mult*tbv1, p_tbv_mult*tbv2, p_tbv_mult*tbv3),
        "p_nii":    targets_from_price_multiple(nii_ps0,  p_nii_mult,  g1, g2, g3),
    }

    bucket = row.get("Bucket",""); sector = row.get("Sektor",""); industry = row.get("Industri",""); ticker = row.get("Ticker","")
    pref = (row.get("Preferred metod") or "AUTO").strip().lower()
    if pref != "auto" and pref in vals:
        primary = pref
    else:
        primary = choose_primary_method(bucket, sector, industry, ticker, fcf0>0, ebitda0>0)

    today,y1,y2,y3 = vals.get(primary, (0.0,0.0,0.0,0.0))
    b1,br1 = bull_bear(y1, bull_mult, bear_mult)
    upside = (safe_div(today, px, 0.0)-1.0)*100.0 if px>0 else 0.0
    return primary, (today,y1,y2,y3), b1, br1, upside

# ---------- Massuppdatering ----------
if do_mass_refresh:
    df = read_df(ws_data)
    if df.empty:
        st.warning("Inga bolag i Data ännu.")
    else:
        cur_list = sorted({(c or "SEK") for c in df["Valuta"].tolist()})
        persist_fx(ws_fx, fetch_fx_to_sek(cur_list))
        done = 0
        for _, r in df.iterrows():
            tk = r.get("Ticker","")
            if not tk: 
                continue
            try:
                adv = dict(
                    pe_hist=r.get("PE_hist", np.nan), eps0=r.get("EPS0", np.nan),
                    ev_fcf_mult=r.get("EV_FCF_mult", np.nan), p_fcf_mult=r.get("P_FCF_mult", np.nan),
                    ev_s_mult=r.get("EV_S_mult", np.nan), ev_ebitda_mult=r.get("EV_EBITDA_mult", np.nan),
                    p_nav_mult=r.get("P_NAV_mult", np.nan), p_affo_mult=r.get("P_AFFO_mult", np.nan),
                    p_b_mult=r.get("P_B_mult", np.nan), p_tbv_mult=r.get("P_TBV_mult", np.nan),
                    p_nii_mult=r.get("P_NII_mult", np.nan), tbv_ps0=r.get("TBV_ps0", np.nan),
                    rotce=r.get("ROTCE", np.nan), payout=r.get("Payout", np.nan),
                    affo_ps0=r.get("AFFO_ps0", np.nan), nav_ps0=r.get("NAV_ps0", np.nan),
                    nii_ps0=r.get("NII_ps0", np.nan), bv_ps0=r.get("BV_ps0", np.nan), fcf_ps0=r.get("FCF_ps0", np.nan)
                )
                newrow = handle_one_ticker_save(
                    tk, r.get("Bucket",""), int(nz(r.get("Antal aktier"),0)),
                    r.get("Preferred metod","AUTO"),
                    float(nz(r.get("G1"),0.15)), float(nz(r.get("G2"),0.12)), float(nz(r.get("G3"),0.10)),
                    adv, use_finnhub, try_sec
                )
                if autosave_hist and use_finnhub:
                    band = fetch_finnhub_metrics(tk).get("pe_band")
                    primary, fairt, b1, br1, upc = compute_primary_for_snapshot(newrow)
                    save_quarter_snapshot(newrow, band, primary, fairt, b1, br1, upc)
                time.sleep(0.6)
                done += 1
            except Exception as e:
                st.warning(f"Misslyckades uppdatera {tk}: {e}")
        st.success(f"Alla bolag uppdaterade ({done} st).")

# ---------- Beräkna alla (filter) ----------
calc_rows=[]
for _, rr in data_df.iterrows():
    if rr.get("Bucket") not in pick_buckets:
        continue
    if only_owned and int(nz(rr.get("Antal aktier"),0)) <= 0:
        continue
    if only_watch and int(nz(rr.get("Antal aktier"),0)) > 0:
        continue
    try:
        row = compute_methods_row(rr)
        if row["Pris"] == 0.0:
            st.warning(f"{row['Ticker']}: pris=0 från Yahoo (tillfälligt).")
        calc_rows.append(row)
    except Exception as e:
        st.warning(f"Kunde inte beräkna {rr.get('Ticker')}: {e}")

if not calc_rows:
    st.info("Inget att visa med aktuella filter.")
    st.stop()

calc_df = pd.DataFrame(calc_rows).sort_values(by="Upside_%", ascending=False)

# ---------- Portföljsummering i SEK ----------
def _sek(x: float) -> str:
    try:
        return f"{x:,.0f} SEK".replace(",", " ")
    except:
        return f"{x} SEK"

total_value_sek      = float(np.nansum(calc_df["Innehav_SEK"]))
total_div_year_sek   = float(np.nansum(calc_df["Utdelning/år_SEK"]))
total_div_month_sek  = total_div_year_sek / 12.0

st.markdown("## 💼 Portföljsammanställning (filtrerad vy)")
c1, c2, c3 = st.columns(3)
c1.metric("Totalt portföljvärde (SEK)", _sek(total_value_sek))
c2.metric("Total utdelning kommande 12m (SEK)", _sek(total_div_year_sek))
c3.metric("Utdelning per månad (SEK)", _sek(total_div_month_sek))
st.caption("Summorna gäller bolagen i tabellen nedan (enligt dina filter).")

# ---------- Rankingtabell ----------
st.markdown("## 📊 Rangordning (störst uppsida →)")
show_cols = [
    "Ticker","Namn","Bucket","Valuta","Pris","Primär metod",
    "Fair idag","Fair 1y","Upside_%","Antal","Innehav_SEK","Utdelning/år_SEK","DA_%"
]
st.dataframe(calc_df[show_cols].reset_index(drop=True), use_container_width=True)

# ---------- Export till fliken "Resultat" ----------
def persist_result_row(tkr: str, cur: str, pris: float, vals: Dict[str, Any], inputs: Dict[str, Any], method: str):
    today,y1,y2,y3 = vals.get(method,(0.0,0.0,0.0,0.0))
    b1,br1 = bull_bear(y1, bull_mult, bear_mult)
    row = {
        "Timestamp": now_ts(), "Ticker": tkr, "Valuta": cur, "Aktuell kurs (0)": pris,
        "Riktkurs idag": today, "Riktkurs 1 år": y1, "Riktkurs 2 år": y2, "Riktkurs 3 år": y3,
        "Bull 1 år": b1, "Bear 1 år": br1, "Metod": method,
        "Input-sammanfattning": stringify_inputs(inputs), "Kommentar": "",
    }
    upsert_row(ws_res, "Ticker", tkr, row)

if st.button("💾 Spara primära riktkurser till fliken Resultat"):
    for _, r in calc_df.iterrows():
        persist_result_row(r["Ticker"], r["Valuta"], r["Pris"], r["Alla metoder"], r["Inputs"], r["Primär metod"])
    st.success("Primära riktkurser sparade till 'Resultat'.")

# ---------- Detaljer per bolag & snapshots ----------
st.markdown("## 🔎 Detaljer per bolag (alla värderingsmetoder)")

def set_pe_hist_in_data(ticker: str, pe_value: float):
    upsert_row(ws_data, "Ticker", ticker, {"Ticker": ticker, "PE_hist": pe_value, "Timestamp": now_ts()})

for _, r in calc_df.iterrows():
    tk = r["Ticker"]
    with st.expander(f"{tk} • {r['Namn']} • {r['Bucket']}"):
        st.write(f"**Valuta:** {r['Valuta']} • **Pris:** {fmt2(r['Pris'])} • **Primär metod:** `{r['Primär metod']}`")
        rows = []
        for m, (t0,t1,t2,t3) in r["Alla metoder"].items():
            b1, br1 = bull_bear(t1, bull_mult, bear_mult)
            rows.append([m,t0,t1,t2,t3,b1,br1])
        dfm = pd.DataFrame(rows, columns=["Metod","Idag","1 år","2 år","3 år","Bull 1 år","Bear 1 år"])
        st.dataframe(dfm, use_container_width=True)

        if use_finnhub:
            band = fetch_finnhub_metrics(tk).get("pe_band")
            p25,p50,p75 = (band or (None,None,None))
            st.caption(f"P/E-band (≈ senaste 5–10 år): p25={fmt2(p25)} • p50={fmt2(p50)} • p75={fmt2(p75)}")

            c1,c2,c3 = st.columns(3)
            if c1.button(f"Sätt PE_hist = p50 ({fmt2(p50)})", key=f"set_pe_{tk}") and p50:
                set_pe_hist_in_data(tk, float(p50))
                st.success(f"{tk}: PE_hist satt till {fmt2(p50)} i fliken Data.")

            if c2.button("Spara kvartalssnapshot", key=f"snap_{tk}"):
                data_row = read_df(ws_data)
                rr = data_row.loc[data_row["Ticker"] == tk]
                if not rr.empty:
                    row_dict = rr.iloc[0].to_dict()
                    t0,t1,t2,t3 = r["Alla metoder"][r["Primär metod"]]
                    bb1,bb2 = bull_bear(t1, bull_mult, bear_mult)
                    save_quarter_snapshot(row_dict, band, r["Primär metod"], (t0,t1,t2,t3), bb1, bb2, r["Upside_%"])
                    st.success(f"{tk}: snapshot sparad för {yq_of()[2]}.")

            if c3.button("Spara snapshot för alla i listan", key=f"snap_all_{tk}"):
                done = 0
                data_df_full = read_df(ws_data).set_index("Ticker", drop=False)
                for _, cr in calc_df.iterrows():
                    tk2 = cr["Ticker"]
                    if tk2 in data_df_full.index:
                        row_dict = data_df_full.loc[tk2].to_dict()
                        b2 = fetch_finnhub_metrics(tk2).get("pe_band")
                        t0,t1,t2,t3 = cr["Alla metoder"][cr["Primär metod"]]
                        b1x,b2x = bull_bear(t1, bull_mult, bear_mult)
                        save_quarter_snapshot(row_dict, b2, cr["Primär metod"], (t0,t1,t2,t3), b1x, b2x, cr["Upside_%"])
                        done += 1
                st.success(f"Snapshots sparade för {done} tickers ({yq_of()[2]}).")
