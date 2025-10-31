# app.py — Portfölj + Riktkurser till Google Sheets, multi-metod, auto-FX
# Flikar i Google Sheets: "Data", "Valutakurser", "Resultat"
# Författare: GPT-5 Thinking — 2025-10-31

from __future__ import annotations
import math
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# =========================
# Grund
# =========================

st.set_page_config(page_title="Riktkurser + Sheets", page_icon="📈", layout="wide")
st.title("📈 Riktkurser + Google Sheets (Buckets, auto-FX, multi-metod)")

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b is None or b == 0:
            return default
        return float(a) / float(b)
    except Exception:
        return default

def nz(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return default
        return x
    except Exception:
        return default

def fmt2(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return ""
    return f"{x:.2f}"

def grow(v: float, g: float) -> float:
    return v * (1.0 + g)

def multi_year(v0: float, g1: float, g2: float, g3: float) -> Tuple[float, float, float]:
    y1 = grow(v0, g1)
    y2 = grow(y1, g2)
    y3 = grow(y2, g3)
    return y1, y2, y3

def bull_bear(value_1y: float, bull_mult: float = 1.15, bear_mult: float = 0.85) -> Tuple[float, float]:
    return value_1y * bull_mult, value_1y * bear_mult

def stringify_inputs(d: Dict[str, Any]) -> str:
    parts = []
    for k, v in d.items():
        if isinstance(v, float):
            parts.append(f"{k}:{v:.4f}")
        else:
            parts.append(f"{k}:{str(v)}")
    return "|".join(parts).replace(" ", "_")

def build_gas_url(ticker: str, method: str, inputs: Dict[str, Any], shares_fd: Optional[float], note: str = "") -> str:
    base = "https://script.google.com/macros/s/DEPLOYMENT_ID/exec"  # byt till din deployment
    q = {
        "ticker": ticker,
        "method": method,
        "input": stringify_inputs(inputs),
        "shares_fd": f"{shares_fd:.2f}" if shares_fd else "",
        "note": note.replace(" ", "_")
    }
    qs = "&".join([f"{k}={str(v)}" for k, v in q.items()])
    return f"{base}?{qs}"

# =========================
# Google Sheets I/O
# =========================

REQUIRED_DATA_COLS = [
    "Timestamp", "Ticker", "Bolagsnamn", "Sektor", "Industri", "Valuta",
    "Bucket", "Antal aktier",
    "Preferred metod", "G1", "G2", "G3",
    # valfria multiplar/inputs (sparas om du fyller i manuellt)
    "PE_hist", "EPS0",
    "EV_FCF_mult", "P_FCF_mult", "EV_S_mult", "EV_EBITDA_mult",
    "P_NAV_mult", "P_AFFO_mult", "P_B_mult", "P_TBV_mult", "P_NII_mult",
    "TBV_ps0", "ROTCE", "Payout",
    "AFFO_ps0", "NAV_ps0", "NII_ps0", "BV_ps0", "FCF_ps0",
    # cache från Yahoo (kan uppdateras)
    "Last Price", "Market Cap", "EV", "Shares Out", "Revenue TTM", "EBITDA TTM", "FCF TTM",
    "Total Debt", "Cash", "Dividend/ps", "Dividend Yield",
]

RESULT_COLS = [
    "Timestamp", "Ticker", "Valuta", "Aktuell kurs (0)",
    "Riktkurs idag", "Riktkurs 1 år", "Riktkurs 2 år", "Riktkurs 3 år",
    "Bull 1 år", "Bear 1 år", "Metod", "Input-sammanfattning", "Kommentar"
]

FX_SYMBOLS = {
    "USD": "USDSEK=X",
    "EUR": "EURSEK=X",
    "NOK": "NOKSEK=X",
    "CAD": "CADSEK=X",
    "GBP": "GBPSEK=X",
    "DKK": "DKKSEK=X",
    "SEK": None,  # bas
}

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    creds_raw = st.secrets.get("GOOGLE_CREDENTIALS")
    if isinstance(creds_raw, str):
        creds_dict = json.loads(creds_raw)
    else:
        creds_dict = dict(creds_raw)
    creds_dict = _normalize_private_key(creds_dict)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    gc = gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scope))
    return gc

def open_or_create_ws(sh: gspread.Spreadsheet, title: str, cols: List[str]) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2, cols=max(50, len(cols)))
        ws.append_row(cols)
    # se till att headern finns
    header = ws.row_values(1)
    if header != cols:
        # skriv exakt header (rensa först vid behov)
        ws.clear()
        ws.append_row(cols)
    return ws

def read_df(ws: gspread.Worksheet) -> pd.DataFrame:
    vals = ws.get_all_records()
    if not vals:
        return pd.DataFrame(columns=ws.row_values(1))
    df = pd.DataFrame(vals)
    # säkerställ alla kolumnnamn
    for c in ws.row_values(1):
        if c not in df.columns:
            df[c] = np.nan
    return df

def upsert_row(ws: gspread.Worksheet, key_col: str, key_val: str, row_dict: Dict[str, Any]):
    df = read_df(ws)
    if df.empty:
        # lägg första raden
        ordered = [row_dict.get(c, "") for c in ws.row_values(1)]
        ws.append_row(ordered)
        return
    idx = df.index[df[key_col] == key_val].tolist()
    header = ws.row_values(1)
    if idx:
        r = idx[0] + 2  # 1-bas + header
        values = ws.row_values(r)
        # bygg ny rad i rätt ordning
        new_row = []
        for col in header:
            new_row.append(row_dict.get(col, values[header.index(col)] if header.index(col) < len(values) else ""))
        ws.update(f"A{r}:{chr(64+len(header))}{r}", [new_row])
    else:
        ordered = [row_dict.get(c, "") for c in header]
        ws.append_row(ordered)

@st.cache_resource(show_spinner=False)
def open_sheets():
    sheet_id = st.secrets.get("SHEET_ID", "")
    if not sheet_id:
        st.stop()
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws_data = open_or_create_ws(sh, "Data", REQUIRED_DATA_COLS)
    ws_fx = open_or_create_ws(sh, "Valutakurser", ["Timestamp", "Valuta", "SEK_per_unit"])
    ws_res = open_or_create_ws(sh, "Resultat", RESULT_COLS)
    return sh, ws_data, ws_fx, ws_res

# =========================
# Yahoo & FX
# =========================

@st.cache_data(show_spinner=False, ttl=900)
def fetch_yahoo_snapshot(ticker: str) -> Dict[str, Any]:
    t = yf.Ticker(ticker)
    snap: Dict[str, Any] = {}

    try:
        fi = t.fast_info
        snap["currency"] = fi.get("currency")
        snap["last_price"] = fi.get("last_price")
        snap["market_cap"] = fi.get("market_cap")
    except Exception:
        pass

    try:
        info = t.info or {}
    except Exception:
        info = {}

    snap["short_name"] = info.get("shortName")
    snap["long_name"] = info.get("longName")
    snap["sector"] = info.get("sector")
    snap["industry"] = info.get("industry")
    snap["enterprise_value"] = info.get("enterpriseValue")
    snap["shares_out"] = info.get("sharesOutstanding")
    snap["dividend_ps"] = info.get("dividendRate")  # per år
    snap["dividend_yield"] = info.get("dividendYield")  # i decimal

    # statements (årliga)
    try:
        income = t.financials or pd.DataFrame()
        cashf = t.cashflow or pd.DataFrame()
        bal = t.balance_sheet or pd.DataFrame()
    except Exception:
        income = pd.DataFrame(); cashf = pd.DataFrame(); bal = pd.DataFrame()

    rev_ttm = None
    if not income.empty and "Total Revenue" in income.index and len(income.columns) > 0:
        rev_ttm = float(income.loc["Total Revenue"].iloc[0])

    ebitda_ttm = None
    if not income.empty and "EBITDA" in income.index and len(income.columns) > 0:
        ebitda_ttm = float(income.loc["EBITDA"].iloc[0])

    fcf_ttm = None
    if not cashf.empty:
        ocf = None; capex = None
        if "Total Cash From Operating Activities" in cashf.index:
            ocf = float(cashf.loc["Total Cash From Operating Activities"].iloc[0])
        if "Capital Expenditures" in cashf.index:
            capex = float(cashf.loc["Capital Expenditures"].iloc[0])
        if ocf is not None and capex is not None:
            fcf_ttm = ocf - abs(capex)

    total_debt = None; cash = None
    if not bal.empty:
        if "Total Debt" in bal.index:
            total_debt = float(bal.loc["Total Debt"].iloc[0])
        for key in ["Cash And Cash Equivalents", "Cash", "Cash And Short Term Investments"]:
            if key in bal.index:
                cash = float(bal.loc[key].iloc[0]); break

    snap["revenue_ttm"] = rev_ttm
    snap["ebitda_ttm"] = ebitda_ttm
    snap["fcf_ttm"] = fcf_ttm
    snap["total_debt"] = total_debt
    snap["cash"] = cash

    if snap.get("enterprise_value") is None:
        mc = snap.get("market_cap")
        td = nz(total_debt, 0.0); ca = nz(cash, 0.0)
        if mc is not None:
            snap["enterprise_value"] = float(mc) + float(td) - float(ca)

    return snap

@st.cache_data(show_spinner=False, ttl=900)
def fetch_fx_to_sek(codes: List[str]) -> Dict[str, float]:
    rates: Dict[str, float] = {}
    for c in codes:
        if c == "SEK":
            rates[c] = 1.0
            continue
        sym = FX_SYMBOLS.get(c)
        if not sym:
            continue
        t = yf.Ticker(sym)
        try:
            last = t.fast_info.get("last_price")
        except Exception:
            last = None
        if last:
            rates[c] = float(last)
    return rates

def persist_fx(ws_fx: gspread.Worksheet, rates: Dict[str, float]):
    ts = now_ts()
    rows = [[ts, k, v] for k, v in rates.items()]
    if rows:
        ws_fx.append_rows(rows, value_input_option="USER_ENTERED")

# =========================
# Värderingsmotor
# =========================

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
    return today, per_share_from_EV(multiple, y1, net_debt, shares), \
           per_share_from_EV(multiple, y2, net_debt, shares), \
           per_share_from_EV(multiple, y3, net_debt, shares)

def targets_from_price_multiple(metric_ps0: float, multiple: float,
                                g1: float, g2: float, g3: float) -> Tuple[float, float, float, float]:
    today = multiple * metric_ps0
    y1, y2, y3 = multi_year(metric_ps0, g1, g2, g3)
    return today, multiple * y1, multiple * y2, multiple * y3

def project_tbv_per_share(tbv0_ps: float, rotce: float, payout_ratio: float) -> Tuple[float, float, float]:
    g = rotce * (1.0 - payout_ratio)
    return multi_year(tbv0_ps, g, g, g)

# =========================
# Heuristik: välj primär metod per bolag
# =========================

BDC_TICKERS = {"CSWC", "PFLT", "HRZN", "ARCC", "MAIN", "FSK", "OCSL", "ORCC"}
REIT_HINTS = {"reit"}
BANK_HINTS = {"bank", "banks", "thrifts", "credit", "financial services"}
INSURANCE_HINTS = {"insurance"}
ENERGY_HINTS = {"oil", "gas", "energy", "midstream", "mlp"}
SHIPPING_HINTS = {"marine", "shipping", "tanker", "bulk"}
SAAS_HINTS = {"software", "application", "it services", "cloud"}

def choose_primary_method(bucket: str, sector: str, industry: str, ticker: str,
                          has_fcf: bool, has_ebitda: bool) -> str:
    tk = ticker.upper()
    s = (sector or "").lower()
    i = (industry or "").lower()
    # typdetektering
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

    # bucket-bias
    b = (bucket or "").lower()
    if "tillväxt" in b:
        if has_fcf:
            return "ev_fcf"
        if has_ebitda:
            return "ev_ebitda"
        return "ev_sales"
    else:  # utdelning
        if has_fcf:
            return "p_fcf"
        return "p_b"

# =========================
# UI – sidopanel
# =========================

with st.sidebar:
    st.header("Inställningar & filter")
    bull_mult = st.number_input("Bull × (på 1-års riktkurs)", value=1.15, step=0.05, format="%.2f")
    bear_mult = st.number_input("Bear × (på 1-års riktkurs)", value=0.85, step=0.05, format="%.2f")
    st.caption("Basvaluta/visning: SEK (FX hämtas automatiskt).")

    st.markdown("---")
    st.subheader("Filter")
    bucket_opts = [
        "Bucket A tillväxt", "Bucket B tillväxt", "Bucket C tillväxt",
        "Bucket A utdelning", "Bucket B utdelning", "Bucket C utdelning",
    ]
    pick_buckets = st.multiselect("Välj buckets att visa:", bucket_opts, default=bucket_opts)
    only_owned = st.checkbox("Visa endast innehav (>0 aktier)", value=False)
    only_watch = st.checkbox("Visa endast bevakningslista (0 aktier)", value=False)

    st.markdown("---")
    st.subheader("Uppdatering")
    do_mass_refresh = st.button("🔄 Uppdatera alla från Yahoo + FX + beräkna & spara")

# =========================
# Lägg till / uppdatera bolag
# =========================

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
    c1, c2, c3, c4 = st.columns(4)
    pe_hist = c1.number_input("P/E hist snitt", value=15.0, step=0.5, format="%.2f")
    eps0 = c2.number_input("EPS0 (idag)", value=0.00, step=0.01, format="%.2f")
    ev_fcf_mult = c3.number_input("EV/FCF-multiple", value=18.0, step=0.5, format="%.2f")
    p_fcf_mult = c4.number_input("P/FCF-multiple", value=20.0, step=0.5, format="%.2f")

    c5, c6, c7, c8 = st.columns(4)
    ev_s_mult = c5.number_input("EV/S-multiple", value=5.0, step=0.5, format="%.2f")
    ev_ebitda_mult = c6.number_input("EV/EBITDA-multiple", value=12.0, step=0.5, format="%.2f")
    p_nav_mult = c7.number_input("P/NAV-multiple", value=1.00, step=0.05, format="%.2f")
    p_affo_mult = c8.number_input("P/AFFO-multiple (REIT)", value=13.0, step=0.5, format="%.2f")

    c9, c10, c11, c12 = st.columns(4)
    p_b_mult = c9.number_input("P/B-multiple", value=1.50, step=0.05, format="%.2f")
    p_tbv_mult = c10.number_input("P/TBV-multiple (bank)", value=1.20, step=0.05, format="%.2f")
    p_nii_mult = c11.number_input("P/NII-multiple (BDC)", value=10.0, step=0.5, format="%.2f")
    tbv_ps0 = c12.number_input("TBV/aktie (idag)", value=0.00, step=0.01, format="%.2f")

    c13, c14, c15, c16 = st.columns(4)
    rotce = c13.number_input("ROTCE (t.ex. 0.12=12%)", value=0.12, step=0.01, format="%.2f")
    payout = c14.number_input("Payout-ratio", value=0.30, step=0.05, format="%.2f")
    affo_ps0 = c15.number_input("AFFO/aktie (idag)", value=0.00, step=0.01, format="%.2f")
    nav_ps0 = c16.number_input("NAV/aktie (idag)", value=0.00, step=0.01, format="%.2f")

    c17, c18, c19 = st.columns(3)
    nii_ps0 = c17.number_input("NII/aktie (idag)", value=0.00, step=0.01, format="%.2f")
    bv_ps0 = c18.number_input("BV/aktie (idag)", value=0.00, step=0.01, format="%.2f")
    fcf_ps0 = c19.number_input("FCF/aktie (idag)", value=0.00, step=0.01, format="%.2f")

save_clicked = st.button("💾 Spara till Google Sheets (hämtar Yahoo + FX + beräknar)")

# =========================
# Sheets öppnas
# =========================

try:
    sh, ws_data, ws_fx, ws_res = open_sheets()
except Exception as e:
    st.error("Kunde inte öppna Google Sheet. Kontrollera SHEET_ID och GOOGLE_CREDENTIALS i secrets.")
    st.stop()

# =========================
# Händelser: Spara/uppdatera
# =========================

def handle_one_ticker_save(ticker: str, bucket: str, antal: int, pref_method: str,
                           g1: float, g2: float, g3: float,
                           adv: Dict[str, Any]):
    tk = ticker.strip().upper()
    snap = fetch_yahoo_snapshot(tk)

    # FX
    cur = snap.get("currency") or "SEK"
    rates = fetch_fx_to_sek([cur])
    persist_fx(ws_fx, rates)
    sek_rate = rates.get(cur, 1.0)

    # bygg rad
    row = {
        "Timestamp": now_ts(),
        "Ticker": tk,
        "Bolagsnamn": snap.get("long_name") or snap.get("short_name") or "",
        "Sektor": snap.get("sector") or "",
        "Industri": snap.get("industry") or "",
        "Valuta": cur,
        "Bucket": bucket,
        "Antal aktier": int(antal),
        "Preferred metod": pref_method,
        "G1": g1, "G2": g2, "G3": g3,
        "PE_hist": adv.get("pe_hist"), "EPS0": adv.get("eps0"),
        "EV_FCF_mult": adv.get("ev_fcf_mult"),
        "P_FCF_mult": adv.get("p_fcf_mult"),
        "EV_S_mult": adv.get("ev_s_mult"),
        "EV_EBITDA_mult": adv.get("ev_ebitda_mult"),
        "P_NAV_mult": adv.get("p_nav_mult"),
        "P_AFFO_mult": adv.get("p_affo_mult"),
        "P_B_mult": adv.get("p_b_mult"),
        "P_TBV_mult": adv.get("p_tbv_mult"),
        "P_NII_mult": adv.get("p_nii_mult"),
        "TBV_ps0": adv.get("tbv_ps0"),
        "ROTCE": adv.get("rotce"),
        "Payout": adv.get("payout"),
        "AFFO_ps0": adv.get("affo_ps0"),
        "NAV_ps0": adv.get("nav_ps0"),
        "NII_ps0": adv.get("nii_ps0"),
        "BV_ps0": adv.get("bv_ps0"),
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
    upsert_row(ws_data, "Ticker", tk, row)
    return row, sek_rate, snap

if save_clicked and ticker_in:
    adv = dict(
        pe_hist=pe_hist, eps0=eps0, ev_fcf_mult=ev_fcf_mult, p_fcf_mult=p_fcf_mult,
        ev_s_mult=ev_s_mult, ev_ebitda_mult=ev_ebitda_mult, p_nav_mult=p_nav_mult, p_affo_mult=p_affo_mult,
        p_b_mult=p_b_mult, p_tbv_mult=p_tbv_mult, p_nii_mult=p_nii_mult, tbv_ps0=tbv_ps0,
        rotce=rotce, payout=payout, affo_ps0=affo_ps0, nav_ps0=nav_ps0, nii_ps0=nii_ps0,
        bv_ps0=bv_ps0, fcf_ps0=fcf_ps0
    )
    row, rate, snap = handle_one_ticker_save(ticker_in, bucket_in, antal_in, pref_method_in, g1_in, g2_in, g3_in, adv)
    st.success(f"{ticker_in} sparad/uppdaterad i Google Sheets.")

if do_mass_refresh:
    data_df = read_df(ws_data)
    if data_df.empty:
        st.warning("Inga bolag i Data ännu.")
    else:
        # samla valutor, hämta FX i ett svep
        cur_list = sorted({c for c in data_df["Valuta"].fillna("SEK").tolist()})
        rates = fetch_fx_to_sek(cur_list)
        persist_fx(ws_fx, rates)
        for _, r in data_df.iterrows():
            tk = r["Ticker"]
            try:
                adv = dict(
                    pe_hist=r.get("PE_hist", np.nan), eps0=r.get("EPS0", np.nan),
                    ev_fcf_mult=r.get("EV_FCF_mult", np.nan), p_fcf_mult=r.get("P_FCF_mult", np.nan),
                    ev_s_mult=r.get("EV_S_mult", np.nan), ev_ebitda_mult=r.get("EV_EBITDA_mult", np.nan),
                    p_nav_mult=r.get("P_NAV_mult", np.nan), p_affo_mult=r.get("P_AFFO_mult", np.nan),
                    p_b_mult=r.get("P_B_mult", np.nan), p_tbv_mult=r.get("P_TBV_mult", np.nan),
                    p_nii_mult=r.get("P_NII_mult", np.nan),
                    tbv_ps0=r.get("TBV_ps0", np.nan), rotce=r.get("ROTCE", np.nan), payout=r.get("Payout", np.nan),
                    affo_ps0=r.get("AFFO_ps0", np.nan), nav_ps0=r.get("NAV_ps0", np.nan),
                    nii_ps0=r.get("NII_ps0", np.nan), bv_ps0=r.get("BV_ps0", np.nan), fcf_ps0=r.get("FCF_ps0", np.nan)
                )
                handle_one_ticker_save(
                    tk, r.get("Bucket",""), int(nz(r.get("Antal aktier"),0)),
                    r.get("Preferred metod","AUTO"),
                    float(nz(r.get("G1"),0.15)), float(nz(r.get("G2"),0.12)), float(nz(r.get("G3"),0.10)),
                    adv
                )
            except Exception as e:
                st.warning(f"Misslyckades uppdatera {tk}: {e}")
        st.success("Alla bolag uppdaterade från Yahoo + FX.")

# =========================
# Läs Data och beräkna vy
# =========================

data_df = read_df(ws_data)
if data_df.empty:
    st.info("Lägg till minst ett bolag ovan så visas vyer här nedanför.")
    st.stop()

# FX cache
cur_list = sorted({c for c in data_df["Valuta"].fillna("SEK").tolist()})
fx = fetch_fx_to_sek(cur_list)
sek_rate_for = lambda c: fx.get(c or "SEK", 1.0)

# Beräkna alla metoder per rad och välj primär metod
def compute_methods_row(r: pd.Series) -> Dict[str, Any]:
    cur = r.get("Valuta") or "SEK"
    px = float(nz(r.get("Last Price"), 0.0))
    mc = float(nz(r.get("Market Cap"), 0.0))
    ev = float(nz(r.get("EV"), 0.0))
    shares_out = float(nz(r.get("Shares Out"), safe_div(mc, px, 0.0)))
    rev0 = float(nz(r.get("Revenue TTM"), 0.0))
    ebitda0 = float(nz(r.get("EBITDA TTM"), 0.0))
    fcf0 = float(nz(r.get("FCF TTM"), 0.0))
    td = float(nz(r.get("Total Debt"), 0.0))
    ca = float(nz(r.get("Cash"), 0.0))
    net_debt = td - ca

    # tillväxt
    g1 = float(nz(r.get("G1"), 0.15)); g2 = float(nz(r.get("G2"), 0.12)); g3 = float(nz(r.get("G3"), 0.10))

    # multiplar/inputs
    pe_hist = float(nz(r.get("PE_hist"), 15.0))
    eps0 = float(nz(r.get("EPS0"), 0.0))
    ev_fcf_mult = float(nz(r.get("EV_FCF_mult"), 18.0))
    p_fcf_mult = float(nz(r.get("P_FCF_mult"), 20.0))
    ev_s_mult = float(nz(r.get("EV_S_mult"), 5.0))
    ev_eb_mult = float(nz(r.get("EV_EBITDA_mult"), 12.0))
    p_nav_mult = float(nz(r.get("P_NAV_mult"), 1.0))
    p_affo_mult = float(nz(r.get("P_AFFO_mult"), 13.0))
    p_b_mult = float(nz(r.get("P_B_mult"), 1.5))
    p_tbv_mult = float(nz(r.get("P_TBV_mult"), 1.2))
    p_nii_mult = float(nz(r.get("P_NII_mult"), 10.0))

    tbv_ps0 = float(nz(r.get("TBV_ps0"), 0.0))
    rotce = float(nz(r.get("ROTCE"), 0.12))
    payout = float(nz(r.get("Payout"), 0.30))
    affo_ps0 = float(nz(r.get("AFFO_ps0"), 0.0))
    nav_ps0 = float(nz(r.get("NAV_ps0"), 0.0))
    nii_ps0 = float(nz(r.get("NII_ps0"), 0.0))
    bv_ps0 = float(nz(r.get("BV_ps0"), 0.0))
    fcf_ps0 = float(nz(r.get("FCF_ps0"), 0.0))

    # flaggor
    has_fcf = fcf0 > 0.0
    has_ebitda = ebitda0 > 0.0

    vals = {}

    # P/E
    vals["pe_hist_vs_eps"] = target_from_PE(eps0, pe_hist, g1, g2, g3)

    # EV/FCF
    vals["ev_fcf"] = targets_from_ev_multiple(fcf0, ev_fcf_mult, net_debt, shares_out, g1, g2, g3)

    # P/FCF
    vals["p_fcf"] = targets_from_price_multiple(fcf_ps0, p_fcf_mult, g1, g2, g3)

    # EV/S
    vals["ev_sales"] = targets_from_ev_multiple(rev0, ev_s_mult, net_debt, shares_out, g1, g2, g3)

    # EV/EBITDA
    vals["ev_ebitda"] = targets_from_ev_multiple(ebitda0, ev_eb_mult, net_debt, shares_out, g1, g2, g3)

    # P/NAV
    vals["p_nav"] = targets_from_price_multiple(nav_ps0, p_nav_mult, g1, g2, g3)

    # EV/DACF (använder EBITDA om DACF ej ifyllt – proxy)
    vals["ev_dacf"] = targets_from_ev_multiple(ebitda0, 6.0 if math.isclose(ev_eb_mult,0.0) else ev_eb_mult, net_debt, shares_out, g1, g2, g3)

    # P/AFFO
    vals["p_affo"] = targets_from_price_multiple(affo_ps0, p_affo_mult, g1, g2, g3)

    # P/B
    vals["p_b"] = targets_from_price_multiple(bv_ps0, p_b_mult, g1, g2, g3)

    # P/TBV med TBV-projektion
    tbv1, tbv2, tbv3 = project_tbv_per_share(tbv_ps0, rotce, payout)
    vals["p_tbv"] = (p_tbv_mult * tbv_ps0, p_tbv_mult * tbv1, p_tbv_mult * tbv2, p_tbv_mult * tbv3)

    # P/NII
    vals["p_nii"] = targets_from_price_multiple(nii_ps0, p_nii_mult, g1, g2, g3)

    # Välj primär metod
    pref = (r.get("Preferred metod") or "AUTO").strip().lower()
    if pref != "auto" and pref in vals:
        primary = pref
    else:
        primary = choose_primary_method(
            r.get("Bucket",""),
            r.get("Sektor",""),
            r.get("Industri",""),
            r.get("Ticker",""),
            has_fcf=has_fcf, has_ebitda=has_ebitda
        )

    today, y1, y2, y3 = vals.get(primary, (0.0,0.0,0.0,0.0))
    b1, br1 = bull_bear(y1, bull_mult, bear_mult)

    # Utdelning/DA
    div_ps = float(nz(r.get("Dividend/ps"), 0.0))
    da = float(nz(r.get("Dividend Yield"), 0.0)) * 100.0 if nz(r.get("Dividend Yield"), 0.0) else (safe_div(div_ps, px, 0.0) * 100.0 if px>0 else 0.0)

    # SEK
    rate = sek_rate_for(cur)
    innehav_sek = int(nz(r.get("Antal aktier"),0)) * px * rate
    utd_år_sek = int(nz(r.get("Antal aktier"),0)) * div_ps * rate
    upside = safe_div(today, px, 0.0) - 1.0 if px>0 else 0.0

    return {
        "Ticker": r.get("Ticker"),
        "Namn": r.get("Bolagsnamn"),
        "Valuta": cur,
        "Pris": px,
        "Rate_SEK": rate,
        "Antal": int(nz(r.get("Antal aktier"),0)),
        "Innehav_SEK": innehav_sek,
        "Utdelning/år_SEK": utd_år_sek,
        "DA_%": da,
        "Bucket": r.get("Bucket"),
        "Primär metod": primary,
        "Fair idag": today,
        "Fair 1y": y1,
        "Fair 2y": y2,
        "Fair 3y": y3,
        "Bull 1y": b1,
        "Bear 1y": br1,
        "Upside_%": upside*100.0,
        "Alla metoder": vals,
        "Inputs": {
            "g1": g1, "g2": g2, "g3": g3,
            "pe_hist": pe_hist, "eps0": eps0,
            "ev_fcf": ev_fcf_mult, "p_fcf": p_fcf_mult, "ev_s": ev_s_mult, "ev_ebitda": ev_eb_mult,
            "p_nav": p_nav_mult, "p_affo": p_affo_mult, "p_b": p_b_mult, "p_tbv": p_tbv_mult, "p_nii": p_nii_mult,
            "tbv_ps0": tbv_ps0, "rotce": rotce, "payout": payout, "affo_ps0": affo_ps0, "nav_ps0": nav_ps0,
            "nii_ps0": nii_ps0, "bv_ps0": bv_ps0, "fcf_ps0": fcf_ps0,
            "shares_fd": shares_out, "net_debt": net_debt
        }
    }

calc_rows = []
for _, rr in data_df.iterrows():
    if rr.get("Bucket") not in pick_buckets:
        continue
    if only_owned and int(nz(rr.get("Antal aktier"),0)) <= 0:
        continue
    if only_watch and int(nz(rr.get("Antal aktier"),0)) > 0:
        continue
    try:
        calc_rows.append(compute_methods_row(rr))
    except Exception as e:
        st.warning(f"Kunde inte beräkna {rr.get('Ticker')}: {e}")

if not calc_rows:
    st.info("Inget att visa med aktuella filter.")
    st.stop()

# Sortera på störst uppsida
calc_df = pd.DataFrame(calc_rows)
calc_df = calc_df.sort_values(by="Upside_%", ascending=False)

st.markdown("## 📊 Rangordning (störst uppsida →)")
show_cols = ["Ticker","Namn","Bucket","Valuta","Pris","Primär metod","Fair idag","Fair 1y","Upside_%","Antal","Innehav_SEK","Utdelning/år_SEK","DA_%"]
st.dataframe(calc_df[show_cols].reset_index(drop=True), use_container_width=True)

# Spara ”Resultat” (primär metod) till Sheets
def persist_result_row(tkr: str, cur: str, pris: float, vals: Dict[str, Any], inputs: Dict[str, Any], method: str):
    today, y1, y2, y3 = vals.get(method, (0.0,0.0,0.0,0.0))
    b1, br1 = bull_bear(y1, bull_mult, bear_mult)
    url = build_gas_url(tkr, method, inputs, inputs.get("shares_fd"), note="fair_value_targets")
    row = {
        "Timestamp": now_ts(),
        "Ticker": tkr,
        "Valuta": cur,
        "Aktuell kurs (0)": pris,
        "Riktkurs idag": today,
        "Riktkurs 1 år": y1,
        "Riktkurs 2 år": y2,
        "Riktkurs 3 år": y3,
        "Bull 1 år": b1,
        "Bear 1 år": br1,
        "Metod": method,
        "Input-sammanfattning": stringify_inputs(inputs),
        "Kommentar": url,  # lägger URL i kommentarsfält
    }
    upsert_row(ws_res, "Ticker", tkr, row)

if st.button("💾 Spara primära riktkurser till fliken Resultat"):
    for _, r in calc_df.iterrows():
        vals = r["Alla metoder"]
        inputs = r["Inputs"]
        persist_result_row(r["Ticker"], r["Valuta"], r["Pris"], vals, inputs, r["Primär metod"])
    st.success("Primära riktkurser sparade till 'Resultat'.")

# Detaljer per bolag (alla metoder)
st.markdown("## 🔎 Detaljer per bolag (alla värderingsmetoder)")
for _, r in calc_df.iterrows():
    with st.expander(f"{r['Ticker']} • {r['Namn']} • {r['Bucket']}"):
        st.write(f"**Valuta:** {r['Valuta']} • **Pris:** {fmt2(r['Pris'])} • **Primär metod:** `{r['Primär metod']}`")
        # tabell över metoder
        rows = []
        for m, (t0, t1, t2, t3) in r["Alla metoder"].items():
            b1, br1 = bull_bear(t1, bull_mult, bear_mult)
            rows.append([m, t0, t1, t2, t3, b1, br1])
        dfm = pd.DataFrame(rows, columns=["Metod","Idag","1 år","2 år","3 år","Bull 1 år","Bear 1 år"])
        st.dataframe(dfm, use_container_width=True)

        # kopieringsbar URL för primär metod
        url = build_gas_url(r["Ticker"], r["Primär metod"], r["Inputs"], r["Inputs"].get("shares_fd"), note="fair_value_targets")
        st.code(url, language="text")
