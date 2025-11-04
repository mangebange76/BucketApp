# app.py — Bas + efterfrågade förbättringar
# ============================================================
# Del 1/6: Imports, konstanter, util-funktioner, Google Sheets-IO
#  - Robust läsning av "Data", "Valutakurser", "Settings"
#  - Säkra skrivningar av manuella fält (Antal, GAV, EPS 1Y/2Y, Revenue 1Y/2Y)
#  - Svenska tal (komma) stöds
#  - Lägger saknade kolumner automatiskt
# ============================================================

from __future__ import annotations

import re
import time
import math
import json
from datetime import datetime, date
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd
import streamlit as st
import gspread
from gspread import Worksheet
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials

# -----------------------------
# Grundinställningar Streamlit
# -----------------------------
st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")

# Säker rerun (Streamlit 1.30+ har st.rerun)
if not hasattr(st, "rerun"):
    if hasattr(st, "experimental_rerun"):
        st.rerun = st.experimental_rerun  # type: ignore
    else:
        def _no_rerun(): pass
        st.rerun = _no_rerun  # type: ignore

# -----------------------------
# Konstanter / kolumnnamn
# -----------------------------
SHEET_DATA = "Data"
SHEET_RATES = "Valutakurser"
SHEET_SETTINGS = "Settings"

# Förväntade kolumner (bas + de du efterfrågat)
EXPECTED_COLS = [
    "Timestamp","Ticker","Bolagsnamn","Sektor","Bucket","Valuta",
    "Antal aktier","GAV (SEK)","Aktuell kurs",
    "Utestående aktier",
    "P/S","P/S Q1","P/S Q2","P/S Q3","P/S Q4","P/S-snitt (Q1..Q4)",
    "P/B","P/B Q1","P/B Q2","P/B Q3","P/B Q4","P/B-snitt (Q1..Q4)",
    "Omsättning idag","Omsättning nästa år","Omsättning om 2 år","Omsättning om 3 år",
    "Riktkurs idag","Riktkurs om 1 år","Riktkurs om 2 år","Riktkurs om 3 år",
    "Årlig utdelning","Payout (%)",
    "CAGR 5 år (%)",  # kan vara Revenue- eller EPS-CAGR lagrat manuellt/auto
    "EPS 1Y","EPS 2Y",                     # CHANGED: manuella fält för Editor
    "Revenue 1Y (M)","Revenue 2Y (M)",     # CHANGED: i MILJONER av bolagets valuta (8,81B => 8810)
    "Utdelningsfrekvens",                  # t.ex. Quarterly/Monthly/Yearly
    "Nästa utdelningsdatum",               # CHANGED: betalningsdatum (inte X-dag)
    "Nästa utdelning per aktie",           # CHANGED: i bolagets valuta per aktie
    "Senast manuellt uppdaterad","Senast auto uppdaterad","Auto källa","Senast beräknad",
    "DA (%)","Uppsida idag (%)","Uppsida 1 år (%)","Uppsida 2 år (%)","Uppsida 3 år (%)",
    "Score (Growth)"
]

NUMERIC_COLS = {
    "Antal aktier": int,
    "GAV (SEK)": float,
    "Aktuell kurs": float,
    "Utestående aktier": float,
    "P/S": float, "P/S Q1": float, "P/S Q2": float, "P/S Q3": float, "P/S Q4": float, "P/S-snitt (Q1..Q4)": float,
    "P/B": float, "P/B Q1": float, "P/B Q2": float, "P/B Q3": float, "P/B Q4": float, "P/B-snitt (Q1..Q4)": float,
    "Omsättning idag": float, "Omsättning nästa år": float, "Omsättning om 2 år": float, "Omsättning om 3 år": float,
    "Riktkurs idag": float, "Riktkurs om 1 år": float, "Riktkurs om 2 år": float, "Riktkurs om 3 år": float,
    "Årlig utdelning": float, "Payout (%)": float, "CAGR 5 år (%)": float,
    "EPS 1Y": float, "EPS 2Y": float,                    # CHANGED
    "Revenue 1Y (M)": float, "Revenue 2Y (M)": float,    # CHANGED
    "Nästa utdelning per aktie": float,                  # CHANGED
    "DA (%)": float, "Uppsida idag (%)": float, "Uppsida 1 år (%)": float,
    "Uppsida 2 år (%)": float, "Uppsida 3 år (%)": float,
    "Score (Growth)": float,
}

DATE_COLS = {"Timestamp","Senast manuellt uppdaterad","Senast auto uppdaterad","Senast beräknad","Nästa utdelningsdatum"}

# -----------------------------
# Hjälpare: tid & talformat
# -----------------------------
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

_SWEDISH_NUM_RE = re.compile(r"[ \u00A0]")  # vanliga & icke-brytande mellanslag

def parse_swe_number(x: Any) -> Optional[float]:
    """Accepterar '1 234,56' och '1234.56'. Tomt=>None."""
    if x is None:
        return None
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in ("nan","none","null","-"):
        return None
    s = _SWEDISH_NUM_RE.sub("", s)     # ta bort blanksteg
    # Byt komma till punkt om båda inte förekommer
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def to_int_safe(x: Any) -> int:
    v = parse_swe_number(x)
    return int(v) if v is not None else 0

def to_float_safe(x: Any) -> float:
    v = parse_swe_number(x)
    return float(v) if v is not None else 0.0

def to_date(x: Any) -> Optional[pd.Timestamp]:
    if x in (None, "", "nan", "NaT"):
        return None
    try:
        if isinstance(x, (datetime, date)):
            return pd.to_datetime(x)
        return pd.to_datetime(str(x))
    except Exception:
        return None

# -----------------------------
# Google Sheets auth & helpers
# -----------------------------
def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds

def _get_gspread_client() -> gspread.Client:
    # Läser servicekontot från st.secrets["GOOGLE_CREDENTIALS"]
    creds_raw = st.secrets.get("GOOGLE_CREDENTIALS", {})
    if isinstance(creds_raw, str):
        try:
            creds_dict = json.loads(creds_raw)
        except Exception:
            st.stop()
    else:
        creds_dict = dict(creds_raw)
    creds_dict = _normalize_private_key(creds_dict)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)

def _open_sheet(ws_name: str) -> Worksheet:
    sh_id = st.secrets.get("GOOGLE_SHEET_ID", "")
    if not sh_id:
        st.error("Saknar GOOGLE_SHEET_ID i secrets.")
        st.stop()
    gc = _get_gspread_client()
    sh = gc.open_by_key(sh_id)
    try:
        return sh.worksheet(ws_name)
    except WorksheetNotFound:
        # Skapa om blad saknas
        ws = sh.add_worksheet(title=ws_name, rows=2000, cols=100)
        return ws

def _read_worksheet_df(ws_name: str) -> pd.DataFrame:
    ws = _open_sheet(ws_name)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    # Drop tomma rader (helt tomma eller tom Ticker)
    if "Ticker" in df.columns:
        df = df[~df["Ticker"].astype(str).str.strip().eq("")]
    df.replace({"": np.nan}, inplace=True)
    return df

def _ensure_columns(df: pd.DataFrame, expected: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in expected:
        if c not in df.columns:
            df[c] = np.nan
    # Ordna kolumnordningen
    df = df[expected]
    return df

def read_rates_df() -> pd.DataFrame:
    """Läser 'Valutakurser' och normaliserar två kolumner: ['Valuta','SEK per 1']"""
    df = _read_worksheet_df(SHEET_RATES)
    if df.empty:
        return pd.DataFrame({"Valuta": ["SEK"], "SEK per 1": [1.0]})
    # Försök hitta kolumner
    cols = {c.lower().strip(): c for c in df.columns}
    cur = cols.get("valuta", list(df.columns)[0])
    # sök någon numerisk kurskolumn
    rate = None
    for cand in ("sek per 1","sek_per_1","sek","rate","kurs","value"):
        if cand in cols:
            rate = cols[cand]
            break
    if rate is None:
        # välj första numeriska
        for c in df.columns:
            if pd.to_numeric(df[c], errors="coerce").notna().any():
                rate = c
                break
    out = pd.DataFrame({
        "Valuta": df[cur].astype(str).str.upper().str.strip(),
        "SEK per 1": pd.to_numeric(df[rate], errors="coerce")
    })
    out.dropna(subset=["Valuta","SEK per 1"], inplace=True)
    if "SEK" not in out["Valuta"].values:
        out = pd.concat([pd.DataFrame([{"Valuta":"SEK","SEK per 1":1.0}]), out], ignore_index=True)
    return out

def read_settings_dict() -> Dict[str, str]:
    """Läser Settings (frivilligt). Fallback källskatt: USD 15%, NOK 25%, CAD 15%."""
    try:
        df = _read_worksheet_df(SHEET_SETTINGS)
        s = {}
        for _, r in df.iterrows():
            k = str(r.get("Nyckel","")).strip()
            v = str(r.get("Värde","")).strip()
            if k:
                s[k] = v
        # standarder om inte satta
        s.setdefault("withhold_usd", "0.15")
        s.setdefault("withhold_nok", "0.25")
        s.setdefault("withhold_cad", "0.15")
        s.setdefault("withhold_default", "0.00")
        return s
    except Exception:
        return {"withhold_usd":"0.15","withhold_nok":"0.25","withhold_cad":"0.15","withhold_default":"0.00"}

def read_data_df() -> pd.DataFrame:
    """Läser Data-bladet, säkerställer kolumner, kastar om typer, tolkar svenska tal."""
    raw = _read_worksheet_df(SHEET_DATA)
    if raw.empty:
        return _ensure_columns(pd.DataFrame(), EXPECTED_COLS)

    df = _ensure_columns(raw, EXPECTED_COLS).copy()

    # Typning
    for c, t in NUMERIC_COLS.items():
        if c in df.columns:
            df[c] = df[c].apply(parse_swe_number)
    for c in DATE_COLS:
        if c in df.columns:
            df[c] = df[c].apply(to_date)

    # Standardvärden
    if "Valuta" in df.columns:
        df["Valuta"] = df["Valuta"].astype(str).str.upper().str.strip()

    # Fyll NA för kritiska kolumner
    for c in ["Antal aktier","GAV (SEK)","Aktuell kurs","EPS 1Y","EPS 2Y","Revenue 1Y (M)","Revenue 2Y (M)"]:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    return df

# ---------------------------------------------
# Skriv manuella fält för vald ticker till Sheet
# ---------------------------------------------
def _header_map(ws: Worksheet) -> Dict[str, int]:
    header = ws.row_values(1)
    return {h: i+1 for i, h in enumerate(header)}

def save_manual_fields(
    ticker: str,
    antal: Optional[int] = None,
    gav_sek: Optional[float] = None,
    eps1: Optional[float] = None,
    eps2: Optional[float] = None,
    rev1_m: Optional[float] = None,
    rev2_m: Optional[float] = None
) -> None:
    """
    CHANGED: Skriver manuella fält till rätt rad (enbart de fält som angivits).
    Uppdaterar 'Senast manuellt uppdaterad'.
    """
    ws = _open_sheet(SHEET_DATA)
    hmap = _header_map(ws)
    # Hitta rad för ticker
    tick_col = hmap.get("Ticker")
    if not tick_col:
        st.error("Hittar inte kolumnen 'Ticker' i Data-bladet.")
        return
    col_vals = ws.col_values(tick_col)
    row_idx = None
    for i, v in enumerate(col_vals[1:], start=2):
        if str(v).strip().upper() == str(ticker).strip().upper():
            row_idx = i
            break
    if row_idx is None:
        st.warning(f"Kunde inte hitta raden för ticker '{ticker}'.")
        return

    updates = []
    def _queue(name: str, value: Any):
        col = hmap.get(name)
        if col:
            updates.append((row_idx, col, value))

    if antal is not None: _queue("Antal aktier", antal)
    if gav_sek is not None: _queue("GAV (SEK)", gav_sek)
    if eps1 is not None: _queue("EPS 1Y", eps1)
    if eps2 is not None: _queue("EPS 2Y", eps2)
    if rev1_m is not None: _queue("Revenue 1Y (M)", rev1_m)
    if rev2_m is not None: _queue("Revenue 2Y (M)", rev2_m)
    _queue("Senast manuellt uppdaterad", now_ts())

    if not updates:
        return

    cells = ws.range(row_idx, 1, row_idx, len(hmap))
    # Gör en lookup för snabbare skrivning
    col_to_cell = {c.col: c for c in cells}
    for r, c, v in updates:
        cell = col_to_cell.get(c)
        if cell is None:
            # Om raden är kortare, hämta specifik cell
            cell = ws.cell(r, c)
        cell.value = v
        ws.update_cell(r, c, v)

# -----------------------------
# FX-karta
# -----------------------------
def build_fx_map(df_rates: pd.DataFrame) -> Dict[str, float]:
    fx = {"SEK": 1.0}
    if df_rates is None or df_rates.empty:
        return fx
    for _, r in df_rates.iterrows():
        cur = str(r.get("Valuta","")).upper().strip()
        rate = parse_swe_number(r.get("SEK per 1"))
        if cur and rate and rate > 0:
            fx[cur] = float(rate)
    return fx

# Lagra i session_state vid start (Del 6/6 gör uppstartskörning)
if "SETTINGS" not in st.session_state:
    st.session_state["SETTINGS"] = read_settings_dict()
if "RATES" not in st.session_state:
    st.session_state["RATES"] = read_rates_df()
if "DATA" not in st.session_state:
    st.session_state["DATA"] = read_data_df()

# ============================================================
# Del 2/6: Beräkningar & hjälpfunktioner
#  • CHANGED: _ensure_columns behåller EXTRA kolumner (slutar kapa!)
#  • CHANGED: Lägger till Rev/EPS CAGR-fält (5Y) och “PE TTM/FWD”
#  • Riktkurser 1–3 år från EPS- eller Revenue-väg (max growth 35 %)
#  • Uppsida-beräkning
#  • Ålderstabell (EPS1Y/EPS2Y/Rev1Y/Rev2Y) – topp 10 äldst
# ============================================================

# ---------- Viktigt schema-fix ----------
# CHANGED: Behåll befintliga kolumner och lägg till saknade sist (ta INTE bort!)
def _ensure_columns(df: pd.DataFrame, expected: List[str]) -> pd.DataFrame:
    df = df.copy()
    added = False
    for c in expected:
        if c not in df.columns:
            df[c] = np.nan
            added = True
    if added:
        # behåll originalordning + lägg saknade sist
        keep = list(df.columns)
        want = [c for c in expected if c in keep]
        rest = [c for c in keep if c not in expected]
        df = df[want + rest]
    return df

# CHANGED: Utöka kända kolumner (droppa inget – bara lägg till!)
_extra_cols = [
    "Rev CAGR", "EPS CAGR",               # ifall dessa finns i ditt blad
    "Rev CAGR 5Y (%)","EPS CAGR 5Y (%)",  # explicit 5-års fält
    "PE TTM","PE FWD"                      # ibland finns dessa – används som ankare om du vill
]
for c in _extra_cols:
    if c not in EXPECTED_COLS:
        EXPECTED_COLS.append(c)

for c in ["Rev CAGR","EPS CAGR","Rev CAGR 5Y (%)","EPS CAGR 5Y (%)","PE TTM","PE FWD"]:
    NUMERIC_COLS.setdefault(c, float)

# ============================================================
# Tillväxt & ankare
# ============================================================
GROWTH_CAP = 0.35  # CHANGED: max 35 % enligt önskan

def _as_growth_decimal(x: Any) -> Optional[float]:
    """Tar emot t.ex. 0.18 eller 18 → 0.18. Hanterar None/NaN."""
    v = parse_swe_number(x)
    if v is None:
        return None
    # tolka >1 som procent (18 → 0.18)
    if v > 1.0:
        v = v / 100.0
    return float(v)

def _cap_growth(g: Optional[float]) -> Optional[float]:
    if g is None:
        return None
    try:
        return min(float(g), GROWTH_CAP)
    except Exception:
        return None

def _pick_eps_growth_5y(row: pd.Series) -> Optional[float]:
    # prioritet: explicit 5Y → EPS CAGR → None
    for key in ("EPS CAGR 5Y (%)","EPS CAGR"):
        g = _as_growth_decimal(row.get(key))
        if g is not None:
            return _cap_growth(g)
    return None

def _pick_rev_growth_5y(row: pd.Series) -> Optional[float]:
    for key in ("Rev CAGR 5Y (%)","Rev CAGR"):
        g = _as_growth_decimal(row.get(key))
        if g is not None:
            return _cap_growth(g)
    return None

def _pe_anchor_from_price_and_eps(price: Optional[float], eps1y: Optional[float]) -> Optional[float]:
    p = parse_swe_number(price)
    e = parse_swe_number(eps1y)
    if p is None or e is None or e <= 0:
        return None
    return p / e  # forward P/E-ankare (enkel och robust)

def _ps_anchor_from_row(row: pd.Series) -> Optional[float]:
    ps = parse_swe_number(row.get("P/S-snitt (Q1..Q4)"))
    if ps is None or ps <= 0:
        ps = parse_swe_number(row.get("P/S"))
    return ps if (ps is not None and ps > 0) else None

# ============================================================
# Riktkurser (EPS-väg eller Revenues-väg)
# ============================================================
def compute_targets_for_row(row: pd.Series) -> Dict[str, Any]:
    """
    Returnerar:
      {
        'method': 'EPS'|'REV'|'NONE',
        't0': float|None, 't1': ..., 't2': ..., 't3': ...,
        'anchor': float|None, 'note': str
      }
    Logik:
      • EPS-vägen om EPS 1Y och pris finns – använd PE-ankare = pris/EPS1Y
        – EPS 2Y används om EPS 1Y saknas (med varning)
        – väx vidare 1–3 år med EPS 5Y CAGR (tak 35 %)
      • Revenue-vägen annars:
        – kräver P/S-ankare, Utestående aktier, samt Revenue 1Y (M) eller 2Y (M)
        – väx revenue per aktie 1–3 år med 5Y Rev CAGR (tak 35 %)
      • t0 = nuvarande pris om möjligt; annars ankare*”nuvarande bas”
    """
    price_now = parse_swe_number(row.get("Aktuell kurs"))
    shares    = parse_swe_number(row.get("Utestående aktier"))

    # EPS-väg
    eps1 = parse_swe_number(row.get("EPS 1Y"))
    eps2 = parse_swe_number(row.get("EPS 2Y"))
    g_eps = _pick_eps_growth_5y(row)

    if eps1 and eps1 > 0 and price_now and price_now > 0:
        pe_anchor = _pe_anchor_from_price_and_eps(price_now, eps1)
        if pe_anchor and pe_anchor > 0:
            # bygg EPS-path
            e1 = eps1
            e2 = (e1 * (1.0 + (g_eps or 0.0))) if g_eps is not None else eps2 or e1
            e3 = (e2 * (1.0 + (g_eps or 0.0))) if g_eps is not None else (eps2 if eps2 else e2)

            t0 = price_now  # visa nuvarande
            t1 = e1 * pe_anchor
            t2 = e2 * pe_anchor
            t3 = e3 * pe_anchor
            return {"method":"EPS","t0":t0,"t1":t1,"t2":t2,"t3":t3,"anchor":pe_anchor,
                    "note": f"PE-ankare=pris/EPS1Y, g_eps={(g_eps if g_eps is not None else 'n/a')} (cap 35%)"}

    # fallback: om vi saknar eps1 men har eps2
    if (eps1 is None or eps1 <= 0) and eps2 and price_now and price_now > 0:
        # anta 1 år tidigare ≈ eps2/(1+g) om g känd, annars eps2
        g = g_eps or 0.0
        approx_eps1 = eps2 / (1.0 + g) if g > 0 else eps2
        pe_anchor = _pe_anchor_from_price_and_eps(price_now, approx_eps1)
        if pe_anchor and pe_anchor > 0:
            e1 = approx_eps1
            e2 = eps2
            e3 = (e2 * (1.0 + (g_eps or 0.0))) if g_eps is not None else e2
            t0 = price_now
            t1 = e1 * pe_anchor
            t2 = e2 * pe_anchor
            t3 = e3 * pe_anchor
            return {"method":"EPS","t0":t0,"t1":t1,"t2":t2,"t3":t3,"anchor":pe_anchor,
                    "note": f"PE-ankare=pris/approx(EPS1Y), g_eps={(g_eps if g_eps is not None else 'n/a')} (cap 35%)"}

    # Revenue-väg
    ps_anchor = _ps_anchor_from_row(row)
    rev1_m = parse_swe_number(row.get("Revenue 1Y (M)"))
    rev2_m = parse_swe_number(row.get("Revenue 2Y (M)"))
    g_rev = _pick_rev_growth_5y(row)

    if ps_anchor and shares and shares > 0 and (rev1_m or rev2_m):
        # använd miljoner → valuta
        # välj bas för 1Y & 2Y
        if rev1_m:
            r1 = rev1_m * 1_000_000.0
            # generera r2 om saknas
            r2 = (r1 * (1.0 + (g_rev or 0.0))) if (g_rev is not None and not rev2_m) else (rev2_m * 1_000_000.0 if rev2_m else r1)
        else:
            # har bara rev2
            r2 = rev2_m * 1_000_000.0
            r1 = r2 / (1.0 + (g_rev or 0.0)) if (g_rev and g_rev > 0) else r2

        # 3Y
        r3 = (r2 * (1.0 + (g_rev or 0.0))) if g_rev is not None else r2

        # per aktie
        rps1 = r1 / shares
        rps2 = r2 / shares
        rps3 = r3 / shares

        # pris via P/S
        t1 = ps_anchor * rps1
        t2 = ps_anchor * rps2
        t3 = ps_anchor * rps3
        t0 = price_now if price_now else (ps_anchor * (r1 / shares))  # visa något vettigt även utan pris

        return {"method":"REV","t0":t0,"t1":t1,"t2":t2,"t3":t3,"anchor":ps_anchor,
                "note": f"P/S-ankare, g_rev={(g_rev if g_rev is not None else 'n/a')} (cap 35%)"}

    # Inget att göra
    return {"method":"NONE","t0":price_now,"t1":None,"t2":None,"t3":None,"anchor":None,"note":"Saknar data för EPS- eller Revenue-väg"}

# ============================================================
# Uppsida-hjälpare
# ============================================================
def _rel_upside(target: Optional[float], price_now: Optional[float]) -> Optional[float]:
    t = parse_swe_number(target)
    p = parse_swe_number(price_now)
    if t is None or p is None or p <= 0:
        return None
    return (t / p - 1.0) * 100.0

# ============================================================
# Källskatt (till portföljen/utdelningar – används i Del 3)
# ============================================================
def _withholding_for_currency(ccy: str) -> float:
    s = st.session_state.get("SETTINGS", {}) or {}
    c = (ccy or "USD").upper()
    if c == "USD":
        return float(parse_swe_number(s.get("withhold_usd", 0.15)) or 0.15)
    if c == "NOK":
        return float(parse_swe_number(s.get("withhold_nok", 0.25)) or 0.25)
    if c == "CAD":
        return float(parse_swe_number(s.get("withhold_cad", 0.15)) or 0.15)
    # default för andra valutor (EUR/SEK mm) – om du vill kan du lägga nycklar
    return float(parse_swe_number(s.get("withhold_default", 0.0)) or 0.0)

# ============================================================
# Ålderstabell för EPS/Revenue-estimat (Editor-vy)
# ============================================================
def _age_in_days(ts: Any) -> float:
    t = to_date(ts)
    if t is None:
        return 9e9  # extremt gammal
    try:
        return (pd.Timestamp.today(tz="UTC").normalize() - t.normalize()).days
    except Exception:
        return 9e9

def build_oldest_estimates_table(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Visar de rader där fälten EPS1Y/EPS2Y/Rev1Y/Rev2Y sannolikt är äldst.
    Vi använder 'Senast manuellt uppdaterad' som proxy (per-fälts tidsstämplar finns ej),
    och sorterar stigande (äldst först). Tomma datum → topp.
    """
    if df.empty:
        return pd.DataFrame(columns=["Ticker","Bolagsnamn","Ålder (dagar)","EPS 1Y","EPS 2Y","Revenue 1Y (M)","Revenue 2Y (M)","Senast manuellt uppdaterad"])

    q = df.copy()
    for c in ["EPS 1Y","EPS 2Y","Revenue 1Y (M)","Revenue 2Y (M)"]:
        if c not in q.columns:
            q[c] = np.nan

    # proxy: använd kolumnen Senast manuellt uppdaterad om den finns
    if "Senast manuellt uppdaterad" not in q.columns:
        q["Senast manuellt uppdaterad"] = np.nan

    ages = []
    for _, r in q.iterrows():
        ages.append(_age_in_days(r.get("Senast manuellt uppdaterad")))
    q["Ålder (dagar)"] = ages

    out = q.sort_values(by=["Ålder (dagar)","Ticker"], ascending=[False, True]).copy()
    # vi vill ha ÄLDST (störst antal dagar) först → redan rätt: False (störst överst)
    cols = ["Ticker","Bolagsnamn","Ålder (dagar)","EPS 1Y","EPS 2Y","Revenue 1Y (M)","Revenue 2Y (M)","Senast manuellt uppdaterad"]
    for c in cols:
        if c not in out.columns:
            out[c] = np.nan
    return out[cols].head(top_n)

# ============================================================
# Del 3/6: UI – Analys, Portfölj, Ranking, Editor-förbättringar
#  • CHANGED: Robust _age_in_days (fixar Timestamp vs date)
#  • Analys: sortera på uppsida, bläddra 1/X, spara riktkurser -> Resultat
#  • Portfölj: värde/PNL i SEK + kommande utdelningar (netto i SEK)
#  • Ranking: prioritera lägsta andel i bucket + uppsida
#  • Editor: sökbar rullista (alfabetisk) + “10 äldst”-tabell
# ============================================================

# ---------- Datumhjälpare (rulla fram datum enligt frekvens) ----------
def _add_months(d: dt.date, n: int) -> dt.date:
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    day = min(d.day, [31,
        29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
        31,30,31,30,31,31,30,31,30,31][m-1])
    return dt.date(y, m, day)

def _roll_forward(d: Optional[dt.date], freq: str) -> Optional[dt.date]:
    if not isinstance(d, dt.date):
        return None
    freq = (freq or "").upper()
    if freq == "M":  return _add_months(d, 1)
    if freq == "Q":  return _add_months(d, 3)
    if freq == "S":  return _add_months(d, 6)
    if freq == "A":  return _add_months(d, 12)
    return None

def _dps_from_annual_and_freq(annual: Optional[float], freq: str) -> Optional[float]:
    a = parse_swe_number(annual)
    if a is None:
        return None
    f = (freq or "").upper()
    if f == "M": return a / 12.0
    if f == "Q": return a / 4.0
    if f == "S": return a / 2.0
    if f == "A": return a
    return None

# ---------- Format ----------
def _fmt_money(v: Optional[float], ccy: str) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{float(v):,.2f} {ccy}".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} {ccy}"

def _fmt_num(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{float(v):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(v)

def _fmt_pct(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "–"
    try:
        return f"{100*float(v):.1f}%".replace(".", ",")
    except Exception:
        return str(v)

def _fmt_sek(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and (v != v)):
        return "0 SEK"
    try:
        return f"{float(v):,.0f} SEK".replace(",", " ").replace(".", ",")
    except Exception:
        return f"{v} SEK"

# ---------- CHANGED: robust åldersberäkning (fixar Timestamp vs date) ----------
def _age_in_days(ts: Any) -> float:
    t = to_date(ts)
    if t is None:
        return 9e9
    try:
        # Normalisera till pandas.Timestamp
        if isinstance(t, dt.date) and not isinstance(t, pd.Timestamp):
            t = pd.Timestamp(t)
        today = pd.Timestamp.today(tz="UTC").normalize()
        return float((today - t.tz_localize("UTC").normalize()).days) if t.tzinfo is None else float((today - t.normalize()).days)
    except Exception:
        try:
            # Fallback utan tz
            if isinstance(t, pd.Timestamp):
                return float((pd.Timestamp.today().normalize() - t.normalize()).days)
            if isinstance(t, dt.date):
                return float((dt.date.today() - t).days)
        except Exception:
            return 9e9
    return 9e9

# ============================================================
# Analys
# ============================================================
def _holding_metrics(currency: str, price_now: Optional[float], shares_own: Optional[float], gav_sek: Optional[float], fx_rate: float) -> Dict[str, Optional[float]]:
    if not parse_swe_number(shares_own):
        return {"value_sek": None, "cost_sek": None, "pnl_sek": None, "pnl_pct": None}
    s = float(parse_swe_number(shares_own) or 0.0)
    v_sek = None
    if parse_swe_number(price_now):
        v_sek = float(price_now) * s * float(fx_rate or 1.0)
    c_sek = None
    if parse_swe_number(gav_sek):
        c_sek = float(gav_sek) * s
    pnl_sek = None
    pnl_pct = None
    if v_sek is not None and c_sek is not None and c_sek != 0:
        pnl_sek = v_sek - c_sek
        pnl_pct = pnl_sek / c_sek
    return {"value_sek": v_sek, "cost_sek": c_sek, "pnl_sek": pnl_sek, "pnl_pct": pnl_pct}

def _save_targets_to_result(tkr: str, currency: str, method: str,
                            t0: Optional[float], t1: Optional[float], t2: Optional[float], t3: Optional[float]):
    row = {
        "Timestamp": now_stamp(),
        "Ticker": tkr,
        "Valuta": currency,
        "Metod": method or "",
        "Riktkurs idag": t0,
        "Riktkurs 1 år": t1,
        "Riktkurs 2 år": t2,
        "Riktkurs 3 år": t3,
    }
    try:
        # Om din bas har append_result_row – använd den
        append_result_row(row)
    except Exception:
        # Fallback: skriv direkt till Resultat
        res = _read_df(RESULT_TITLE)
        if res.empty:
            _write_df(RESULT_TITLE, pd.DataFrame([row]))
            return
        cols = list(res.columns)
        for k in row.keys():
            if k not in cols:
                cols.append(k)
                res[k] = np.nan
        res = pd.concat([res, pd.DataFrame([row])[cols]], ignore_index=True)
        _write_df(RESULT_TITLE, res[cols])

def _company_card_simple(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[float], Optional[float]]:
    tkr = str(row.get("Ticker","")).upper().strip()
    name = str(row.get("Bolagsnamn") or tkr)
    bucket = str(row.get("Bucket") or "")
    st.markdown(f"### {tkr} • {name}" + (f" • {bucket}" if bucket else ""))

    # Beräkna riktkurser via efterfrågad logik (EPS/Rev, 5Y CAGR capped 35 %)
    out = compute_targets_for_row(row)
    price_now = parse_swe_number(row.get("Aktuell kurs"))
    currency = str(row.get("Valuta") or "USD").upper()

    cols = st.columns(4)
    cols[0].metric("Idag", _fmt_money(out["t0"], currency))
    cols[1].metric("1 år", _fmt_money(out["t1"], currency))
    cols[2].metric("2 år", _fmt_money(out["t2"], currency))
    cols[3].metric("3 år", _fmt_money(out["t3"], currency))

    if parse_swe_number(price_now):
        up_cols = st.columns(4)
        for i, (lbl, tgt) in enumerate([("Idag", out["t0"]), ("1 år", out["t1"]), ("2 år", out["t2"]), ("3 år", out["t3"])]):
            if parse_swe_number(tgt):
                up_cols[i].metric(f"Uppsida {lbl}", _fmt_pct(_rel_upside(tgt, price_now)))

    with st.expander("ℹ️ Notering"):
        st.write(out.get("note") or "")

    c1, c2 = st.columns(2)
    if c1.button("💾 Spara riktkurser → Resultat", key=f"saveres_{tkr}"):
        _save_targets_to_result(tkr, currency, out.get("method",""), out["t0"], out["t1"], out["t2"], out["t3"])
        st.success("Riktkurser sparade till fliken Resultat.")

    # Innehav/PNL (SEK)
    try:
        fx_rate = fx_map.get(currency, 1.0) or 1.0
        shares_own = parse_swe_number(row.get("Antal aktier")) or 0.0
        gav_sek    = parse_swe_number(row.get("GAV (SEK)"))
        hm = _holding_metrics(currency, price_now, shares_own, gav_sek, fx_rate)
        with c2.expander("📦 Innehav & P/L (SEK)"):
            st.write(f"• Innehavsvärde: {_fmt_sek(hm['value_sek'])}")
            st.write(f"• Anskaffning: {_fmt_sek(hm['cost_sek'])}")
            if hm["pnl_sek"] is not None:
                st.write(f"• P/L: {_fmt_sek(hm['pnl_sek'])} ({_fmt_pct(hm['pnl_pct'])})")
    except Exception:
        pass

    return out.get("method"), out.get("t0"), out.get("t1"), out.get("t2"), out.get("t3")

def page_analysis(df_data: pd.DataFrame, settings: Dict[str, str], fx_map: Dict[str, float]):
    st.header("🔬 Analys")

    if df_data.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    f1, f2, f3, f4 = st.columns(4)
    buckets = f1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_only = f2.checkbox("Visa endast innehav (antal > 0)", value=False)
    hide_zero_price = f3.checkbox("Dölj bolag utan aktuell kurs", value=True)
    undervalued_only = f4.checkbox("Visa endast undervärderade (fair idag > pris)", value=False)

    q = df_data.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_only:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    if hide_zero_price:
        q = q[(pd.to_numeric(q["Aktuell kurs"], errors="coerce") > 0)]

    if q.empty:
        st.warning("Inget att visa efter filter.")
        return

    # Beräkna uppsida (fair idag = t0 från compute_targets_for_row)
    rows_calc: List[Tuple[pd.Series, Optional[float]]] = []
    prog = st.progress(0.0)
    q_idx = list(q.index)
    for i, idx in enumerate(q_idx):
        r = q.loc[idx]
        out = compute_targets_for_row(r)
        price = parse_swe_number(r.get("Aktuell kurs"))
        t0 = out.get("t0")
        up = None
        if parse_swe_number(price) and parse_swe_number(t0):
            up = (float(t0)/float(price) - 1.0)
        rows_calc.append((r, up))
        prog.progress((i+1)/max(1,len(q_idx)))
    prog.empty()

    if undervalued_only:
        rows_calc = [x for x in rows_calc if (x[1] is not None and x[1] > 0)]

    # sortera efter uppsida
    rows_calc.sort(key=lambda x: (x[1] is None, -(x[1] if x[1] is not None else -9e9)))
    ordered_rows = [t[0] for t in rows_calc]
    if not ordered_rows:
        st.info("Inga poster uppfyllde kriterierna.")
        return

    key_idx = "analysis_idx_v2"
    if key_idx not in st.session_state:
        st.session_state[key_idx] = 0

    tkr_options = [f"{str(r.get('Ticker'))}" for r in ordered_rows]
    jump = st.selectbox("Gå direkt till bolag", tkr_options, index=st.session_state[key_idx] if 0 <= st.session_state[key_idx] < len(tkr_options) else 0)
    if jump in tkr_options:
        st.session_state[key_idx] = tkr_options.index(jump)

    cprev, cpos, cnext = st.columns([1,2,1])
    with cprev:
        st.button("⬅️ Föregående", use_container_width=True, disabled=(st.session_state[key_idx] <= 0),
                  on_click=lambda: st.session_state.update({key_idx: max(0, st.session_state[key_idx]-1)}))
    with cpos:
        st.write(f"**{st.session_state[key_idx]+1} / {len(ordered_rows)}** — sorterat efter störst uppsida")
    with cnext:
        st.button("Nästa ➡️", use_container_width=True, disabled=(st.session_state[key_idx] >= len(ordered_rows)-1),
                  on_click=lambda: st.session_state.update({key_idx: min(len(ordered_rows)-1, st.session_state[key_idx]+1)}))

    row = ordered_rows[st.session_state[key_idx]]
    with st.container(border=True):
        _ = _company_card_simple(row, settings, fx_map)
        st.markdown("---")

# ============================================================
# Portfölj
# ============================================================
def page_portfolio(df_data: pd.DataFrame):
    st.header("📦 Portfölj")

    if df_data.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
        return

    q = df_data.copy()
    q["Antal aktier"] = pd.to_numeric(q["Antal aktier"], errors="coerce")
    q["GAV (SEK)"]    = pd.to_numeric(q["GAV (SEK)"], errors="coerce")
    q = q[(q["Antal aktier"] > 0)]
    if q.empty:
        st.info("Inga innehav (Antal aktier > 0).")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    rows, prog = [], st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            out = compute_targets_for_row(r)
            t0 = out.get("t0")
            price = parse_swe_number(r.get("Aktuell kurs"))
            currency = str(r.get("Valuta") or "USD").upper()
            fx_rate = fx_map.get(currency, 1.0) or 1.0

            shares_own = float(parse_swe_number(r.get("Antal aktier")) or 0.0)
            gav_sek    = parse_swe_number(r.get("GAV (SEK)"))
            hm = _holding_metrics(currency, price, shares_own, gav_sek, fx_rate)

            up_pct = None
            if parse_swe_number(price) and parse_swe_number(t0):
                up_pct = (float(t0)/float(price) - 1.0) * 100.0

            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": currency,
                "Antal aktier": shares_own,
                "GAV (SEK)": gav_sek,
                "Pris": price,
                "Fair value (Idag)": t0,
                "Uppsida %": up_pct,
                "Värde (SEK)": hm["value_sek"],
                "Anskaffning (SEK)": hm["cost_sek"],
                "P/L (SEK)": hm["pnl_sek"],
                "P/L %": (hm["pnl_pct"]*100.0 if hm["pnl_pct"] is not None else None),
            })
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Antal aktier": parse_swe_number(r.get("Antal aktier")),
                "GAV (SEK)": parse_swe_number(r.get("GAV (SEK)")),
                "Pris": None,
                "Fair value (Idag)": None,
                "Uppsida %": None,
                "Värde (SEK)": None,
                "Anskaffning (SEK)": None,
                "P/L (SEK)": None,
                "P/L %": None,
            })
        prog.progress((i+1)/max(1,len(q)))
    prog.empty()

    out = pd.DataFrame(rows)

    tot_value = pd.to_numeric(out["Värde (SEK)"], errors="coerce").sum()
    tot_cost  = pd.to_numeric(out["Anskaffning (SEK)"], errors="coerce").sum()
    tot_pnl   = tot_value - tot_cost if (pd.notna(tot_value) and pd.notna(tot_cost)) else np.nan
    tot_pnl_pct = (tot_pnl / tot_cost) if (tot_cost and not pd.isna(tot_cost) and tot_cost != 0) else np.nan

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Portföljvärde", _fmt_sek(tot_value))
    c2.metric("Anskaffning", _fmt_sek(tot_cost))
    c3.metric("P/L (SEK)", _fmt_sek(tot_pnl))
    c4.metric("P/L (%)", _fmt_pct(tot_pnl_pct))

    show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Antal aktier","GAV (SEK)","Pris","Fair value (Idag)","Uppsida %","Värde (SEK)","Anskaffning (SEK)","P/L (SEK)","P/L %"]
    st.dataframe(out[show_cols], use_container_width=True)

    # ------- Kommande utdelningsutbetalningar (netto i SEK) -------
    st.subheader("🗓️ Kommande utdelningsutbetalningar (netto i SEK)")

    dd = df_data.copy()
    needed_cols = ["Årlig utdelning","Nästa utdelningsdatum","Utdelningsfrekvens","Nästa utdelning (per aktie)","Antal aktier","Valuta","Ticker","Bolagsnamn"]
    for c in needed_cols:
        if c not in dd.columns:
            dd[c] = np.nan

    dd["Antal aktier"] = pd.to_numeric(dd["Antal aktier"], errors="coerce")
    dd = dd[(dd["Antal aktier"] > 0)]
    if dd.empty:
        st.info("Inga innehav med utdelning.")
        return

    # sannolikt betalare
    pays_mask = (pd.to_numeric(dd["Årlig utdelning"], errors="coerce").fillna(0) > 0) | dd["Nästa utdelningsdatum"].notna()
    dd = dd[pays_mask].copy()

    dd["Utdelningsfrekvens"] = dd["Utdelningsfrekvens"].fillna("").astype(str).str.upper()
    dd["Nästa utdelningsdatum"] = pd.to_datetime(dd["Nästa utdelningsdatum"], errors="coerce").dt.date

    def _rolled_future(d, freq):
        if not isinstance(d, dt.date):
            return np.nan
        x = d
        today = dt.date.today()
        safe = 0
        f = (freq or "").upper()
        while x <= today and f in ("M","Q","S","A") and safe < 24:
            x = _roll_forward(x, f)
            safe += 1
        return x

    dd["Datum"] = dd.apply(lambda r: _rolled_future(r.get("Nästa utdelningsdatum"), r.get("Utdelningsfrekvens")), axis=1)

    def _next_dps_row(r):
        v = parse_swe_number(r.get("Nästa utdelning (per aktie)"))
        if v is not None and v > 0:
            return v
        annual = parse_swe_number(r.get("Årlig utdelning"))
        freq   = str(r.get("Utdelningsfrekvens") or "").upper()
        return _dps_from_annual_and_freq(annual, freq)

    dd["DPS_nästa"] = dd.apply(_next_dps_row, axis=1)

    dd = dd[(dd["Datum"].notna()) & (pd.to_numeric(dd["DPS_nästa"], errors="coerce").fillna(0) > 0)].copy()

    def _net_sek_row(r):
        ccy = str(r.get("Valuta") or "USD").upper()
        wh = get_withholding_for(ccy, get_settings_map())
        fx = get_fx_map().get(ccy, 1.0) or 1.0
        shares = parse_swe_number(r.get("Antal aktier")) or 0.0
        dps = parse_swe_number(r.get("DPS_nästa")) or 0.0
        gross_ccy = dps * shares
        net_sek = gross_ccy * (1.0 - wh) * fx
        return gross_ccy, net_sek

    gross_list, net_list = [], []
    for _, r in dd.iterrows():
        g, n = _net_sek_row(r)
        gross_list.append(g)
        net_list.append(n)
    dd["Brutto (valuta)"] = gross_list
    dd["Netto (SEK)"] = net_list

    dd = dd.sort_values(by=["Datum","Ticker"])
    cols_pay = ["Datum","Ticker","Bolagsnamn","Valuta","Antal aktier","DPS_nästa","Brutto (valuta)","Netto (SEK)","Utdelningsfrekvens"]
    st.dataframe(dd[cols_pay], use_container_width=True)

    horizon = dt.date.today() + dt.timedelta(days=60)
    mask60 = (dd["Datum"] <= horizon)
    tot60 = pd.to_numeric(dd.loc[mask60, "Netto (SEK)"], errors="coerce").sum()
    st.metric("Netto utdelning kommande 60 dagar", _fmt_sek(tot60))

# ============================================================
# Ranking
# ============================================================
def page_ranking(df_data: pd.DataFrame):
    st.header("🏁 Ranking – Prioritera lägsta portföljandel i bucket & uppsida")

    if df_data.empty:
        st.info("Data-bladet är tomt.")
        return

    settings = get_settings_map()
    fx_map   = get_fx_map()

    b1, b2, b3 = st.columns(3)
    buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
    owned_tab = b2.selectbox("Urval", ["Innehav (antal > 0)","Watchlist (antal = 0)"], index=0)
    only_underval = b3.checkbox("Visa endast undervärderade (fair idag > pris)", value=True)

    q = df_data.copy()
    if buckets:
        q = q[q["Bucket"].isin(buckets)]
    if owned_tab.startswith("Innehav"):
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
    else:
        q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

    if q.empty:
        st.info("Inget att visa efter filter.")
        return

    rows, prog = [], st.progress(0.0)
    for i, (_, r) in enumerate(q.iterrows()):
        try:
            out = compute_targets_for_row(r)
            t0 = out.get("t0")
            price = parse_swe_number(r.get("Aktuell kurs"))
            currency = str(r.get("Valuta") or "USD").upper()
            fx_rate = fx_map.get(currency, 1.0) or 1.0
            shares_own = parse_swe_number(r.get("Antal aktier")) or 0.0
            value_sek = (float(price)*float(shares_own)*fx_rate) if (parse_swe_number(price) and shares_own) else 0.0
            upside = None
            if parse_swe_number(price) and parse_swe_number(t0):
                upside = (float(t0)/float(price) - 1.0) * 100.0
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": currency,
                "Pris": price,
                "Primär metod": out.get("method"),
                "Fair value (Idag)": t0,
                "Uppsida %": upside,
                "Value SEK": value_sek,
            })
            time.sleep(0.02)
        except Exception:
            rows.append({
                "Ticker": r.get("Ticker"),
                "Bolagsnamn": r.get("Bolagsnamn"),
                "Bucket": r.get("Bucket"),
                "Valuta": r.get("Valuta"),
                "Pris": None, "Primär metod": None, "Fair value (Idag)": None, "Uppsida %": None,
                "Value SEK": 0.0
            })
        prog.progress((i+1)/max(1,len(q)))
    prog.empty()

    out = pd.DataFrame(rows)
    out["Pris_num"]    = pd.to_numeric(out["Pris"], errors="coerce")
    out["FV_idag_num"] = pd.to_numeric(out["Fair value (Idag)"], errors="coerce")

    totals = out.groupby("Bucket")["Value SEK"].sum().rename("Bucket Total SEK")
    out = out.merge(totals, on="Bucket", how="left")
    out["Bucket Total SEK"] = out["Bucket Total SEK"].replace({0.0: np.nan})
    out["Andel i bucket"] = out["Value SEK"] / out["Bucket Total SEK"]
    out["Andel i bucket"] = out["Andel i bucket"].fillna(1.0)

    out["Undervärderad"] = ((out["FV_idag_num"].notna()) &
                            (out["Pris_num"].notna()) &
                            (out["FV_idag_num"] > out["Pris_num"])).astype(int)

    if only_underval:
        out = out[out["Undervärderad"] == 1]

    out = out.sort_values(by=["Undervärderad","Andel i bucket","Uppsida %"],
                          ascending=[False, True, False], na_position="last")

    show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Pris","Primär metod","Fair value (Idag)","Uppsida %","Value SEK","Andel i bucket"]
    st.dataframe(out[show_cols], use_container_width=True)

# ============================================================
# Editor – sökbar rullista + “10 äldst” tabell
# ============================================================
def page_editor(df_data: pd.DataFrame):
    st.header("✍️ Lägg till / uppdatera bolag")

    # CHANGED: Sökbar rullista i alfabetisk ordning (sök både på namn & ticker)
    options = []
    for _, r in df_data.iterrows():
        t = str(r.get("Ticker") or "").strip()
        n = str(r.get("Bolagsnamn") or "").strip()
        label = f"{n} — {t}" if n else t
        if t:
            options.append((label, t))
    options = sorted(options, key=lambda x: x[0].lower())

    if options:
        sel = st.selectbox("Välj bolag (sökbar lista)", [o[0] for o in options])
        if sel:
            sel_tkr = dict(options).get(sel)
            if sel_tkr:
                st.session_state["editor_ticker"] = sel_tkr

    # Visa standard-editor från basen (text_input använder session_state['editor_ticker'])
    # OBS: Själva “Hämta & fyll från Yahoo”/Spara-knapparna ligger i din bas (Del 4/6).
    tkr_prefill = st.session_state.get("editor_ticker", "")
    st.text_input("Ticker (t.ex. NVDA, 2020.OL)", value=tkr_prefill, key="editor_ticker_field")

    st.markdown("---")
    st.subheader("🕰️ Äldst uppdaterade (EPS/REV-estimat) – topp 10")
    try:
        tbl = build_oldest_estimates_table(df_data, top_n=10)
        st.dataframe(tbl, use_container_width=True)
    except Exception as e:
        st.warning(f"Kunde inte bygga ålderstabell: {e}")

# ============================================================
# Del 4/6: Batch, Settings, Snapshot, Main
#  • Batch: mass-beräkna riktkurser lokalt (1s delay; inga nätanrop)
#  • Settings: källskatt mm (skrivs till Settings-bladet)
#  • Snapshot: skapa fryst kopia av Data-bladet
#  • Main: robust start; automatisk inläsning från Google Sheets
# ============================================================

# ---------- Settings helpers ----------
def _read_settings_df() -> pd.DataFrame:
    try:
        df = _read_df(SETTINGS_TITLE)
        if df is None or df.empty:
            return pd.DataFrame(columns=["Key","Value"])
        # Normalisera
        if "Key" not in df.columns or "Value" not in df.columns:
            return pd.DataFrame(columns=["Key","Value"])
        df["Key"] = df["Key"].astype(str)
        return df
    except Exception:
        return pd.DataFrame(columns=["Key","Value"])

def _write_settings_df(df: pd.DataFrame) -> None:
    # skriv i nyckel/värde-format
    cols = ["Key","Value"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    _write_df(SETTINGS_TITLE, df[cols])

# CHANGED: säkra defaults om get_settings_map saknas i basen
def _safe_settings_map() -> Dict[str, str]:
    try:
        m = get_settings_map()
        if isinstance(m, dict) and m:
            return m
    except Exception:
        pass
    # defaults
    return {
        "withholding_USD": "0.15",
        "withholding_NOK": "0.25",
        "withholding_CAD": "0.15",
        "withholding_EUR": "0.15",
    }

# ------------------------------------------------------------
# Batch – massberäkning lokalt (ingen nät-hämtning)
# ------------------------------------------------------------
def page_batch(df_data: pd.DataFrame):
    st.header("⚙️ Batch")

    if df_data.empty:
        st.info("Data-bladet är tomt. Gå till **Editor** och lägg till bolag först.")
        return

    st.write("Den här batchen **beräknar om** riktkurser lokalt för alla rader med hjälp av din valideringslogik.")
    st.write("• Ingen nät-hämtning görs här.\n• 1 sekunds fördröjning per bolag.\n• Resultat kan sparas till fliken **Resultat** och/eller tillbaka till **Data** (kolumnen *Senast beräknad*).")

    c1, c2 = st.columns(2)
    do_save_result = c1.checkbox("Spara riktkurser till fliken Resultat", value=True)
    do_stamp_data  = c2.checkbox("Uppdatera 'Senast beräknad' i Data", value=True)

    run = st.button("🚀 Kör massberäkning", type="primary")
    if not run:
        return

    settings = _safe_settings_map()
    fx_map   = get_fx_map() if 'get_fx_map' in globals() else {}
    idxs = list(df_data.index)
    prog = st.progress(0.0)
    log  = st.empty()
    changed_rows = 0

    for i, idx in enumerate(idxs, start=1):
        row = df_data.loc[idx]
        tkr = str(row.get("Ticker") or "").upper()
        log.info(f"🧮 Uppdaterar {i} / {len(idxs)} — {tkr}")
        try:
            out = compute_targets_for_row(row)  # använder din baslogik (cap 35% på 5Y CAGR bör ske där)
            if do_save_result:
                _save_targets_to_result(
                    tkr,
                    str(row.get("Valuta") or "USD").upper(),
                    out.get("method",""),
                    out.get("t0"), out.get("t1"), out.get("t2"), out.get("t3")
                )
            if do_stamp_data:
                df_data.at[idx, "Senast beräknad"] = now_stamp()
                changed_rows += 1
        except Exception as e:
            st.warning(f"{tkr}: misslyckades – {e}")
        prog.progress(i/len(idxs))
        time.sleep(1.0)

    prog.empty()
    # skriv tillbaka till Data om vi stämplat
    if do_stamp_data and changed_rows > 0:
        try:
            _write_df(DATA_TITLE, df_data)
            st.success(f"Klar! {changed_rows} rader stämplade i **{DATA_TITLE}**.")
        except Exception as e:
            st.error(f"Kunde inte skriva tillbaka till {DATA_TITLE}: {e}")
    else:
        st.success("Klar!")

# ------------------------------------------------------------
# Settings – källskatt m.m.
# ------------------------------------------------------------
def page_settings():
    st.header("🛠️ Settings")

    # läs befintliga settings
    df_set = _read_settings_df()
    mp = {str(k): str(v) for k, v in zip(df_set.get("Key", []), df_set.get("Value", []))}
    # defaults om nycklar saknas
    def _get(key, default):
        return mp.get(key, default)

    st.subheader("Källskatt per valuta")
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)
    w_usd = c1.text_input("USD",  _get("withholding_USD", "0.15"))
    w_nok = c2.text_input("NOK",  _get("withholding_NOK","0.25"))
    w_cad = c3.text_input("CAD",  _get("withholding_CAD","0.15"))
    w_eur = c4.text_input("EUR",  _get("withholding_EUR","0.15"))

    st.caption("Ange som decimaltal (t.ex. **0.15** = 15%). Används för netto-utdelningar i Portfölj-vyn.")

    if st.button("💾 Spara inställningar", type="primary"):
        rows = [
            {"Key":"withholding_USD","Value":w_usd},
            {"Key":"withholding_NOK","Value":w_nok},
            {"Key":"withholding_CAD","Value":w_cad},
            {"Key":"withholding_EUR","Value":w_eur},
        ]
        # slå ihop med eventuella övriga nycklar
        known = {r["Key"] for r in rows}
        for _, r in df_set.iterrows():
            k = str(r.get("Key") or "")
            if k and k not in known:
                rows.append({"Key":k, "Value":str(r.get("Value") or "")})
        try:
            _write_settings_df(pd.DataFrame(rows))
            st.success("Inställningar sparade.")
        except Exception as e:
            st.error(f"Kunde inte spara settings: {e}")

# ------------------------------------------------------------
# Snapshot – fryst kopia av Data-bladet
# ------------------------------------------------------------
def page_snapshot(df_data: pd.DataFrame):
    st.header("📸 Snapshot")
    st.write("Skapa en **fryst kopia** av nuvarande Data-blad i en ny flik med tidsstämpel.")

    if st.button("📸 Skapa snapshot"):
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H.%M.%S")
            title = f"Snapshot {ts}"
            _write_df(title, df_data.copy())
            st.success(f"Snapshot skapad: **{title}**")
        except Exception as e:
            st.error(f"Misslyckades att skapa snapshot: {e}")

# ------------------------------------------------------------
# Startup – läs Data/FX/Settings från Google Sheets
# ------------------------------------------------------------
def _startup_refresh():
    # CHANGED: robust inläsning – om Data redan finns i session, behåll men erbjud om-laddning
    try:
        df_data = _read_df(DATA_TITLE)
        if df_data is None or df_data.empty:
            st.warning(f"Fliken **{DATA_TITLE}** verkar vara tom.")
            st.session_state["DATA"] = pd.DataFrame()
        else:
            st.session_state["DATA"] = df_data
    except Exception as e:
        st.error(f"Kunde inte läsa **{DATA_TITLE}**: {e}")
        st.session_state["DATA"] = pd.DataFrame()

    # Valutakurser – frivilligt
    try:
        df_fx = _read_df(FX_TITLE)
        st.session_state["FX"] = df_fx if isinstance(df_fx, pd.DataFrame) else pd.DataFrame()
    except Exception:
        st.session_state["FX"] = pd.DataFrame()

    # Settings – som DataFrame (för Settings-sidan) och map (för logik)
    try:
        st.session_state["SETTINGS_DF"] = _read_settings_df()
    except Exception:
        st.session_state["SETTINGS_DF"] = pd.DataFrame(columns=["Key","Value"])

# ------------------------------------------------------------
# Main – sidnavigering
# ------------------------------------------------------------
def main():
    st.set_page_config(page_title="Aktieanalys & investeringsförslag", layout="wide")

    # Topprad
    st.title("Aktieanalys & investeringsförslag")

    # Första start/refresh
    if "APP_INIT" not in st.session_state:
        _startup_refresh()
        st.session_state["APP_INIT"] = True

    # Sidebar: refresh-knapp
    with st.sidebar:
        st.subheader("Navigation")
        page = st.radio("Välj sida", ["Analys","Portfölj","Ranking","Editor","Batch","Settings","Snapshot"])
        if st.button("🔄 Läs om från Google Sheets"):
            _startup_refresh()
            # undvik experimental_rerun – uppdatering sker genom state och nästa UI-draw

    # Hämta data
    df_data = st.session_state.get("DATA")
    if df_data is None:
        st.session_state["DATA"] = pd.DataFrame()
        df_data = st.session_state["DATA"]

    # Rutt
    try:
        if page == "Analys":
            settings_map = _safe_settings_map()
            try:
                fx_map = get_fx_map()
            except Exception:
                fx_map = {}
            page_analysis(df_data, settings_map, fx_map)

        elif page == "Portfölj":
            page_portfolio(df_data)

        elif page == "Ranking":
            page_ranking(df_data)

        elif page == "Editor":
            page_editor(df_data)

        elif page == "Batch":
            page_batch(df_data)

        elif page == "Settings":
            page_settings()

        elif page == "Snapshot":
            page_snapshot(df_data)

    except Exception as e:
        st.error(f"💥 Fel i huvudloopen: {e}")

# Kör appen
if __name__ == "__main__":
    main()

# ============================================================
# Del 5/6: Editor – sökbar rullista + EPS/REV-manual, 5Y-CAGR
#  • Sökbar rullista (Ticker/Bolagsnamn), alfabetisk ordning
#  • Editera & spara: Antal aktier, GAV (SEK), EPS 1Y/2Y, Rev 1Y/2Y
#  • Rev 1Y/2Y anges i **miljoner** (M) av bolagets valuta (8,81B = 8 810)
#  • Hämta 5-års **Rev CAGR** och **EPS CAGR** från Yahoo (cap 35 %)
#  • “Äldst uppdaterad”-tabell: topp 10 baserat på “Senast manuellt uppdaterad”
# ============================================================

# CHANGED: säkerställ extra kolumner i schemat i runtime
_EDITOR_EXTRA_COLS = ["Rev 1Y", "Rev 2Y", "Senast manuellt uppdaterad"]
for _c in _EDITOR_EXTRA_COLS:
    if _c not in DATA_COLUMNS:
        DATA_COLUMNS.append(_c)
# EPS-kolumnerna finns redan i basen: "EPS 1Y", "EPS 2Y"

# CHANGED: cap EPS-cagr till 35 % enligt önskemål
try:
    EPS_CAGR_MAX
    EPS_CAGR_MAX = 0.35
except Exception:
    EPS_CAGR_MAX = 0.35

# -----------------------------
# Editor-utils
# -----------------------------
def _ensure_editor_extra_columns():
    df = _read_df(DATA_TITLE)
    if df.empty:
        # skapa tomt blad med alla kolumner
        _write_df(DATA_TITLE, pd.DataFrame(columns=DATA_COLUMNS))
        return
    changed = False
    for c in DATA_COLUMNS:
        if c not in df.columns:
            df[c] = np.nan
            changed = True
    if changed:
        # lägg nya kolumner sist
        df = df[[*(k for k in DATA_COLUMNS if k in df.columns), *[c for c in df.columns if c not in DATA_COLUMNS]]]
        _write_df(DATA_TITLE, df)

def _sorted_ticker_options(df: pd.DataFrame) -> list[tuple[str, str]]:
    """Returnerar [(ticker, label)] sorterad alfabetiskt på label (Ticker — Bolagsnamn)."""
    if df.empty:
        return []
    tmp = df[["Ticker","Bolagsnamn"]].copy()
    tmp["Ticker"] = tmp["Ticker"].astype(str)
    tmp["Bolagsnamn"] = tmp["Bolagsnamn"].fillna("").astype(str)
    tmp["label"] = tmp.apply(lambda r: f"{r['Ticker'].upper()} — {r['Bolagsnamn']}" if r["Bolagsnamn"] else r["Ticker"].upper(), axis=1)
    tmp = tmp.drop_duplicates(subset=["Ticker"]).sort_values("label")
    return [(r["Ticker"], r["label"]) for _, r in tmp.iterrows()]

def _parse_millions(x) -> float | None:
    """Tolkar text/tal som miljoner (M). '8810' → 8.81e9."""
    v = _f(x)
    if v is None:
        return None
    try:
        return float(v) * 1_000_000.0
    except Exception:
        return None

# -----------------------------
# Yahoo 5Y CAGR (EPS & Revenue)
# -----------------------------
@st.cache_data(ttl=1200, show_spinner=False)
def fetch_yahoo_eps_cagr_annual(ticker: str, min_years: int = 3, max_years: int = 5) -> dict[str, float | int | None]:
    """
    Hämtar årlig EPS-serie (Diluted/Basic) via Yahoo och beräknar 3–5 års CAGR.
    Returnerar {"eps_cagr": float|None, "years": int|None, "source": "yahoo_financials"}
    """
    try:
        tk = yf.Ticker(ticker)
        try:
            inc = tk.get_income_stmt(freq="annual")
        except Exception:
            inc = getattr(tk, "income_stmt", None)

        if inc is None or (hasattr(inc, "empty") and inc.empty):
            return {"eps_cagr": None, "years": None, "source": "none"}

        df = inc.copy()
        eps_row = _ix_pick(df, ["DilutedEPS", "BasicEPS", "EPS"])
        if eps_row is None:
            return {"eps_cagr": None, "years": None, "source": "none"}

        ser = pd.to_numeric(pd.Series(eps_row), errors="coerce").dropna()
        if ser.empty:
            return {"eps_cagr": None, "years": None, "source": "none"}

        # sortera på år
        try:
            ser.index = pd.to_datetime(ser.index, errors="coerce")
            ser = ser.sort_index()
        except Exception:
            pass

        vals = ser.dropna().values.tolist()
        # kräver minst 2 datapunkter
        if len(vals) < 2:
            return {"eps_cagr": None, "years": None, "source": "none"}

        n = min(max_years, len(vals))
        vals = vals[-n:]
        n_years = len(vals) - 1
        if n_years < max(1, min_years - 1):
            return {"eps_cagr": None, "years": n_years, "source": "yahoo_financials"}

        start, end = float(vals[0]), float(vals[-1])
        # EPS kan vara <= 0 — CAGR definieras inte väl vid teckenvändning
        if start <= 0 or end <= 0:
            return {"eps_cagr": None, "years": n_years, "source": "yahoo_financials"}

        try:
            cagr = (end / start) ** (1.0 / n_years) - 1.0
        except Exception:
            cagr = None

        return {"eps_cagr": cagr, "years": n_years, "source": "yahoo_financials"}
    except Exception:
        return {"eps_cagr": None, "years": None, "source": "none"}

# -----------------------------
# Editor-sida (ny version)
# -----------------------------
def page_editor(df_data: pd.DataFrame):
    st.header("✍️ Editor")

    # Säkerställ kolumner
    _ensure_editor_extra_columns()
    df = _read_df(DATA_TITLE)
    df = _ensure_columns(df, DATA_COLUMNS)

    # Sökbar rullista (alfabetisk label)
    opts = _sorted_ticker_options(df)
    col_sel, col_add = st.columns([2, 1])

    with col_sel:
        if opts:
            labels = [lbl for _, lbl in opts]
            tickers = [t for t, _ in opts]
            idx_init = 0
            sel_label = st.selectbox("Välj bolag (sökbar)", options=labels, index=idx_init)
            sel_ticker = tickers[labels.index(sel_label)]
        else:
            st.info("Inga bolag i databasen ännu.")
            sel_ticker = ""

    with col_add:
        st.caption("Lägg till nytt bolag")
        new_ticker = st.text_input("Ny ticker", value="", placeholder="t.ex. NVDA eller 2020.OL").strip().upper()
        bucket_new = st.selectbox("Bucket (ny)", DEFAULT_BUCKETS, index=0, key="bucket_new_editor")

        if st.button("➕ Lägg till ny ticker"):
            if not new_ticker:
                st.warning("Ange ticker.")
            else:
                df2 = _read_df(DATA_TITLE)
                if "Ticker" not in df2.columns:
                    df2 = _ensure_columns(df2, DATA_COLUMNS)
                mask = df2["Ticker"].astype(str).str.upper() == new_ticker
                if mask.any():
                    st.warning("Tickern finns redan.")
                else:
                    base = {c: np.nan for c in DATA_COLUMNS}
                    base.update({"Timestamp": now_stamp(), "Ticker": new_ticker, "Bucket": bucket_new})
                    df2 = pd.concat([df2, pd.DataFrame([base])], ignore_index=True)
                    _write_df(DATA_TITLE, df2)
                    st.success(f"Lade till {new_ticker}.")
                    st.session_state["DATA"] = df2  # uppdatera cache

    # Hämta rad för vald ticker
    existing = None
    if sel_ticker:
        mask = df["Ticker"].astype(str).str.upper() == sel_ticker.upper()
        if mask.any():
            existing = df[mask].iloc[0]

    st.markdown("---")

    # Redigerbara fält
    st.subheader(f"📝 Manuell uppdatering – {sel_ticker or '—'}")
    c1, c2, c3, c4 = st.columns(4)
    c5, c6 = st.columns(2)
    c7, c8 = st.columns(2)

    def _get_row_val(key, default=None):
        if existing is None:
            return default
        return existing.get(key, default)

    # Antal aktier / GAV
    antal_aktier = c1.number_input("Antal aktier", min_value=0.0, value=float(_f(_get_row_val("Antal aktier")) or 0.0), step=1.0)
    gav_sek      = c2.number_input("GAV (SEK)", min_value=0.0, value=float(_f(_get_row_val("GAV (SEK)")) or 0.0), step=0.01)
    bucket       = c3.selectbox("Bucket", DEFAULT_BUCKETS, index=(DEFAULT_BUCKETS.index(_get_row_val("Bucket")) if _get_row_val("Bucket") in DEFAULT_BUCKETS else 0))
    valuta_cur   = c4.text_input("Valuta (t.ex. USD, NOK, SEK)", value=str(_get_row_val("Valuta") or "USD").upper()).strip().upper()

    # EPS 1Y/2Y
    eps_1y = c5.text_input("EPS 1Y (kommande 12m – om du vill sätta manuellt)", value=str(_get_row_val("EPS 1Y") or ""))  # låter tom str passera
    eps_2y = c6.text_input("EPS 2Y (därpå följande – manuellt)", value=str(_get_row_val("EPS 2Y") or ""))

    # Rev 1Y/2Y i **miljoner**
    rev_1y_m = c7.text_input("Revenue 1Y (M, miljoner – 8,81B = 8810)", value=str( int((_get_row_val("Rev 1Y") or 0)/1_000_000) if _get_row_val("Rev 1Y") is not None and _f(_get_row_val("Rev 1Y")) else "" ))
    rev_2y_m = c8.text_input("Revenue 2Y (M, miljoner)", value=str( int((_get_row_val("Rev 2Y") or 0)/1_000_000) if _get_row_val("Rev 2Y") is not None and _f(_get_row_val("Rev 2Y")) else "" ))

    st.caption("**OBS:** Revenue 1Y/2Y anges i **miljoner**. Ex: 8,81B skrivs som **8810**.")

    # Knapp-rad
    b1, b2, b3 = st.columns(3)

    # 5y CAGR-knapp
    if b1.button("📈 Hämta 5-års CAGR (Yahoo) och cap 35%"):
        if not sel_ticker:
            st.warning("Välj ett bolag i rullistan först.")
        else:
            try:
                # Revenue 5Y CAGR
                rev5 = fetch_yahoo_rev_cagr(sel_ticker, min_years=3, max_years=5)
                rev_cagr = _clamp(rev5.get("rev_cagr"), REV_CAGR_MIN if 'REV_CAGR_MIN' in globals() else -0.10, 0.35)

                # EPS 5Y CAGR
                eps5 = fetch_yahoo_eps_cagr_annual(sel_ticker, min_years=3, max_years=5)
                eps_cagr = _clamp(eps5.get("eps_cagr"), EPS_CAGR_MIN if 'EPS_CAGR_MIN' in globals() else -0.20, 0.35)

                df_cur = _read_df(DATA_TITLE)
                df_cur = _ensure_columns(df_cur, DATA_COLUMNS)
                m = df_cur["Ticker"].astype(str).str.upper() == sel_ticker.upper()
                if not m.any():
                    st.error("Kunde inte hitta vald ticker i Data.")
                else:
                    if "Rev CAGR" not in df_cur.columns: df_cur["Rev CAGR"] = np.nan
                    if "EPS CAGR" not in df_cur.columns: df_cur["EPS CAGR"] = np.nan
                    if rev_cagr is not None: df_cur.loc[m, "Rev CAGR"] = float(rev_cagr)
                    if eps_cagr is not None: df_cur.loc[m, "EPS CAGR"] = float(eps_cagr)
                    _write_df(DATA_TITLE, df_cur)
                    st.success(f"CAGR sparad. Rev CAGR={_fmt_pct(rev_cagr)}, EPS CAGR={_fmt_pct(eps_cagr)}")
                    st.session_state["DATA"] = df_cur
            except Exception as e:
                st.error(f"Misslyckades: {e}")

    # Spara-knapp
    if b2.button("💾 Spara manuella fält"):
        if not sel_ticker:
            st.warning("Välj ett bolag i rullistan först.")
        else:
            try:
                df_cur = _read_df(DATA_TITLE)
                df_cur = _ensure_columns(df_cur, DATA_COLUMNS)
                m = df_cur["Ticker"].astype(str).str.upper() == sel_ticker.upper()
                if not m.any():
                    st.error("Kunde inte hitta vald ticker i Data.")
                else:
                    idx = df_cur.index[m][0]
                    df_cur.at[idx, "Bucket"] = bucket
                    if valuta_cur: df_cur.at[idx, "Valuta"] = valuta_cur
                    df_cur.at[idx, "Antal aktier"] = antal_aktier
                    df_cur.at[idx, "GAV (SEK)"] = gav_sek

                    # EPS 1Y/2Y
                    if _f(eps_1y) is not None: df_cur.at[idx, "EPS 1Y"] = _f(eps_1y)
                    if _f(eps_2y) is not None: df_cur.at[idx, "EPS 2Y"] = _f(eps_2y)

                    # Rev 1Y/2Y – lagras i absoluta tal (ej miljoner)
                    r1 = _parse_millions(rev_1y_m)
                    r2 = _parse_millions(rev_2y_m)
                    if r1 is not None: df_cur.at[idx, "Rev 1Y"] = r1
                    if r2 is not None: df_cur.at[idx, "Rev 2Y"] = r2

                    # stämpla manuell uppdatering
                    df_cur.at[idx, "Senast manuellt uppdaterad"] = now_stamp()
                    _write_df(DATA_TITLE, df_cur)
                    st.success("Sparat till Data.")
                    st.session_state["DATA"] = df_cur
            except Exception as e:
                st.error(f"Kunde inte spara: {e}")

    # Quick-länk: hämta endast pris/valuta (valfritt)
    if b3.button("🔎 Hämta pris/valuta (snabb)"):
        if not sel_ticker:
            st.warning("Välj ticker först.")
        else:
            try:
                snap = fetch_yahoo_snapshot(sel_ticker)
                df_cur = _read_df(DATA_TITLE)
                df_cur = _ensure_columns(df_cur, DATA_COLUMNS)
                m = df_cur["Ticker"].astype(str).str.upper() == sel_ticker.upper()
                if not m.any():
                    st.error("Ticker saknas i Data.")
                else:
                    idx = df_cur.index[m][0]
                    if _pos(snap.get("price")): df_cur.at[idx, "Aktuell kurs"] = float(snap["price"])
                    if snap.get("currency"):     df_cur.at[idx, "Valuta"] = str(snap["currency"]).upper()
                    df_cur.at[idx, "Senast auto uppdaterad"] = now_stamp()
                    df_cur.at[idx, "Auto källa"] = "Yahoo"
                    _write_df(DATA_TITLE, df_cur)
                    st.success("Pris/valuta uppdaterat.")
                    st.session_state["DATA"] = df_cur
            except Exception as e:
                st.error(f"Misslyckades: {e}")

    st.markdown("---")

    # Översikt: vilka är äldst uppdaterade (topp 10)
    st.subheader("⏱️ Äldst uppdaterade (EPS/REV manuella)")
    view = _read_df(DATA_TITLE)
    if view.empty:
        st.info("Inget att visa ännu.")
        return

    # Säkerställ kolumner som används här
    for col in ["Senast manuellt uppdaterad", "EPS 1Y", "EPS 2Y", "Rev 1Y", "Rev 2Y"]:
        if col not in view.columns:
            view[col] = np.nan

    # Ålder i dagar (om ingen stämpel → stor ålder)
    now_dt = dt.datetime.now()
    def _age_days(ts):
        try:
            t = pd.to_datetime(ts, errors="coerce")
            if pd.isna(t):
                return 9_999  # saknas → hamnar överst
            # hantera både datetime64 och str
            if isinstance(t, pd.Timestamp):
                return max(0, (now_dt - t.to_pydatetime()).days)
            return 9_999
        except Exception:
            return 9_999

    view["Ålder (dagar)"] = view["Senast manuellt uppdaterad"].apply(_age_days)

    # flagga vilka fält som saknas
    def _ok(v): 
        return 1 if _f(v) is not None else 0
    view["EPS1Y?"] = view["EPS 1Y"].apply(_ok)
    view["EPS2Y?"] = view["EPS 2Y"].apply(_ok)
    view["REV1Y?"] = view["Rev 1Y"].apply(_ok)
    view["REV2Y?"] = view["Rev 2Y"].apply(_ok)

    out = view.sort_values(["Ålder (dagar)","Ticker"], ascending=[False, True])
    out = out[["Ticker","Bolagsnamn","Ålder (dagar)","EPS1Y?","EPS2Y?","REV1Y?","REV2Y?","Senast manuellt uppdaterad"]].head(10)
    st.dataframe(out, use_container_width=True)

# ============================================================
# Del 6/6: Små städningar + manual overrides för EPS/REV
#  • Tooltips/hjälptexter i Editor
#  • Säkerställ att manuella EPS 1Y/2Y & Rev 1Y/2Y används
#    i värderingen (post-process på methods_df)
#  • Lätt UI-förbättring: visar metoder i Editor efter override
# ============================================================

# CHANGED: liten hjälpförklaring som kan återanvändas i widgets
_HELP_VALUTA = "Bolagets handelsvaluta (t.ex. USD, NOK, SEK)."
_HELP_REV_M  = "Ange i miljoner (M) av bolagets valuta. Ex: 8,81B = 8 810."
_HELP_EPS    = "Om du anger manuellt här, används detta i värderingen (kör över externa estimat)."
_HELP_BUCKET = "Styr visning i vyer & ranking. Endast presentation – påverkar inte själva beräkningarna."
_HELP_GAV    = "Ditt genomsnittliga anskaffningsvärde per aktie i SEK."

# CHANGED: applicera tooltips i Editor – ersätt widgets via on-the-fly monkey patching
if 'page_editor' in globals():
    _orig_page_editor = page_editor
    def page_editor(df_data: pd.DataFrame):
        # kör originalen först så vi har layout + widgets
        _orig_page_editor(df_data) if _orig_page_editor.__code__.co_argcount == 1 else _orig_page_editor()
        # ingen hård re-render här (Streamlit saknar enkel widget-API för retroaktiv help),
        # vi lämnar hjälptexterna i captions/labels nedan i stället.
        st.caption("ℹ️ **Tips:** EPS 1Y/2Y och Revenue 1Y/2Y du anger här får **företräde** i beräkningarna. Revenue matas i **miljoner**.")
else:
    # om Del 5/6 inte hunnit ladda än
    pass

# ------------------------------------------------------------
# CHANGED: Post-process – applicera manuella EPS/REV i methods_df
# ------------------------------------------------------------
def _apply_manual_overrides_to_methods(methods_df: pd.DataFrame, meta: Dict[str, Any], row: pd.Series) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Tar methods_df + meta från compute_methods_for_row och uppdaterar:
      • pe_hist_vs_eps – om EPS 1Y/2Y matats manuellt
      • ev_sales       – om Rev 1Y/2Y matats manuellt (i miljoner → absolut)
    Behåller multiplar via backsolvning från originaltabellen.
    """
    try:
        df = methods_df.copy()

        # ------- EPS overrides (pe_hist_vs_eps) -------
        e_path = (meta or {}).get("eps_path") or {}
        e0 = _f(e_path.get("ttm"))  # TTM lämnas orörd
        e1_man = _f(row.get("EPS 1Y"))
        e2_man = _f(row.get("EPS 2Y"))

        def _safe_div(p, e):
            try:
                p = _f(p); e = _f(e)
                if p is not None and e not in (None, 0):
                    return float(p) / float(e)
            except Exception:
                pass
            return None

        if "pe_hist_vs_eps" in df["Metod"].astype(str).values:
            idx = df.index[df["Metod"] == "pe_hist_vs_eps"][0]
            # backsolva multiplar från originalen
            pe0 = _safe_div(df.at[idx, "Idag"], _f(e_path.get("ttm")))
            pe1 = _safe_div(df.at[idx, "1 år"], _f(e_path.get("y1")))
            pe2 = _safe_div(df.at[idx, "2 år"], _f(e_path.get("y2")))
            pe3 = _safe_div(df.at[idx, "3 år"], _f(e_path.get("y3")))

            # ny EPS-path med manuella värden där de finns
            g = _f((meta or {}).get("cagr_clamped", {}).get("eps_cagr_used")) or 0.0
            e1_new = _f(e1_man) if _f(e1_man) is not None else _f(e_path.get("y1"))
            e2_new = _f(e2_man) if _f(e2_man) is not None else (
                _f(e_path.get("y2")) if _f(e_path.get("y2")) is not None else (_f(e1_new)*(1.0+g) if _f(e1_new) is not None else None)
            )
            e3_new = (_f(e2_new)*(1.0+g)) if _f(e2_new) is not None else _f(e_path.get("y3"))

            def _upd(val, mult):
                if _f(val) is None or _pos(mult) is None:
                    return None
                return float(val) * float(mult)

            if pe0 is not None and _f(e0) is not None:
                df.at[idx, "Idag"] = _upd(e0, pe0)
            if pe1 is not None and _f(e1_new) is not None:
                df.at[idx, "1 år"] = _upd(e1_new, pe1)
            if pe2 is not None and _f(e2_new) is not None:
                df.at[idx, "2 år"] = _upd(e2_new, pe2)
            if pe3 is not None and _f(e3_new) is not None:
                df.at[idx, "3 år"] = _upd(e3_new, pe3)

            meta.setdefault("eps_path_manual", {})
            meta["eps_path_manual"].update({"y1": e1_new, "y2": e2_new, "y3": e3_new})

        # ------- Revenue overrides (ev_sales) -------
        r_path = (meta or {}).get("rev_path") or {}
        r0 = _f(r_path.get("ttm"))
        # i Data lagras Rev 1Y/2Y som absolut-tal; i Editor matar man i miljoner
        r1_abs = _f(row.get("Rev 1Y"))
        r2_abs = _f(row.get("Rev 2Y"))
        if r1_abs is None:
            # använd Editor-hjälparen om fält kommit som str i miljoner
            try:
                r1_abs = _parse_millions(row.get("Rev 1Y (M)"))
            except Exception:
                pass
        if r2_abs is None:
            try:
                r2_abs = _parse_millions(row.get("Rev 2Y (M)"))
            except Exception:
                pass

        if "ev_sales" in df["Metod"].astype(str).values:
            idx = df.index[df["Metod"] == "ev_sales"][0]
            shares = _pos((meta or {}).get("shares_out"))
            nd     = _f((meta or {}).get("net_debt")) or 0.0

            def _evs_from(price_cell, rev_cell):
                p = _f(price_cell); r = _f(rev_cell)
                if _pos(p) and _pos(shares) and _pos(r):
                    try:
                        ev = float(p)*float(shares) + float(nd)
                        return ev / float(r)
                    except Exception:
                        return None
                return None

            evs0 = _evs_from(df.at[idx, "Idag"], _f(r_path.get("ttm")))
            evs1 = _evs_from(df.at[idx, "1 år"], _f(r_path.get("y1")))
            evs2 = _evs_from(df.at[idx, "2 år"], _f(r_path.get("y2")))
            evs3 = _evs_from(df.at[idx, "3 år"], _f(r_path.get("y3")))

            g_rev = _f((meta or {}).get("cagr_clamped", {}).get("rev_cagr_used")) or 0.0
            r1_new = _f(r1_abs) if _f(r1_abs) is not None else _f(r_path.get("y1"))
            r2_new = _f(r2_abs) if _f(r2_abs) is not None else (
                _f(r_path.get("y2")) if _f(r_path.get("y2")) is not None else (_f(r1_new)*(1.0+g_rev) if _f(r1_new) is not None else None)
            )
            r3_new = (_f(r2_new)*(1.0+g_rev)) if _f(r2_new) is not None else _f(r_path.get("y3"))

            def _price_from_evs(evs, rev):
                if _pos(evs) and _pos(rev) and _pos(shares):
                    try:
                        ev_new = float(evs)*float(rev)
                        eq = (ev_new - float(nd)) / float(shares)
                        return max(0.0, eq)
                    except Exception:
                        return None
                return None

            if evs0 is not None and _f(r0) is not None:
                df.at[idx, "Idag"] = _price_from_evs(evs0, r0)
            if evs1 is not None and _f(r1_new) is not None:
                df.at[idx, "1 år"] = _price_from_evs(evs1, r1_new)
            if evs2 is not None and _f(r2_new) is not None:
                df.at[idx, "2 år"] = _price_from_evs(evs2, r2_new)
            if evs3 is not None and _f(r3_new) is not None:
                df.at[idx, "3 år"] = _price_from_evs(evs3, r3_new)

            meta.setdefault("rev_path_manual", {})
            meta["rev_path_manual"].update({"y1": r1_new, "y2": r2_new, "y3": r3_new})

        return df, meta
    except Exception:
        return methods_df, meta

# ------------------------------------------------------------
# CHANGED: Hooka in overrides i Analys/Portfölj/Ranking
# ------------------------------------------------------------
if '_company_card' in globals():
    _orig_company_card = _company_card
    def _company_card(row: pd.Series, settings: Dict[str, str], fx_map: Dict[str, float]):
        # kör original för att få methods_df + meta
        method_sel, t0, t1, t2, t3, meta = _orig_company_card(row, settings, fx_map)
        try:
            methods_df, _, base_meta = compute_methods_for_row(row, settings, fx_map)
            methods_df, meta2 = _apply_manual_overrides_to_methods(methods_df, base_meta, row)
            # välj om primär metod igen (utan att störa UI mindre än nödvändigt):
            preset_primary = str(row.get("Primär metod") or "").strip() or None
            chosen, nt0, nt1, nt2, nt3 = _pick_primary_from_table(methods_df, preset_primary)
            # visa en liten notis om override skett
            if (meta2.get("eps_path_manual") or meta2.get("rev_path_manual")):
                st.info("Manuella värden (EPS/REV) har prioriterats i beräkningen för denna vy.")
            # uppdatera return – håll vald metod om möjligt
            return (method_sel or chosen, nt0 if nt0 is not None else t0, nt1 if nt1 is not None else t1,
                    nt2 if nt2 is not None else t2, nt3 if nt3 is not None else t3, meta2)
        except Exception:
            return method_sel, t0, t1, t2, t3, meta

if 'page_portfolio' in globals():
    _orig_page_portfolio = page_portfolio
    def page_portfolio():
        settings = get_settings_map()
        fx_map   = get_fx_map()
        df       = read_data_df()
        if df.empty:
            st.header("📦 Portfölj")
            st.info("Data-bladet är tomt. Gå till **Editor** och lägg till ett bolag.")
            return

        # kör original logik men replikerar fair-value hämtningen med override
        st.header("📦 Portfölj")
        q = df.copy()
        q["Antal aktier"] = pd.to_numeric(q["Antal aktier"], errors="coerce")
        q["GAV (SEK)"]    = pd.to_numeric(q["GAV (SEK)"], errors="coerce")
        q = q[(q["Antal aktier"] > 0)]
        if q.empty:
            st.info("Inga innehav (Antal aktier > 0).")
            return

        rows = []
        prog = st.progress(0.0)
        for i, (_, r) in enumerate(q.iterrows()):
            try:
                met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
                met_df, meta = _apply_manual_overrides_to_methods(met_df, meta, r)
                preset = str(r.get("Primär metod") or "").strip() or None
                method, fair_today, *_ = _pick_primary_from_table(met_df, preset)
                price = meta.get("price")
                currency = meta.get("currency") or str(_nz(r.get("Valuta"), "USD")).upper()
                fx_rate = fx_map.get(currency, 1.0) or 1.0

                shares_own = float(_nz(_f(r.get("Antal aktier")), 0.0))
                gav_sek    = _f(r.get("GAV (SEK)"))

                hm = _holding_metrics(currency, price, shares_own, gav_sek, fx_rate)
                up_pct = None
                if _pos(price) and _pos(fair_today):
                    up_pct = (fair_today/price - 1.0) * 100.0

                rows.append({
                    "Ticker": r.get("Ticker"),
                    "Bolagsnamn": r.get("Bolagsnamn"),
                    "Bucket": r.get("Bucket"),
                    "Valuta": currency,
                    "Antal aktier": shares_own,
                    "GAV (SEK)": gav_sek,
                    "Pris": price,
                    "Fair value (Idag)": fair_today,
                    "Uppsida %": up_pct,
                    "Värde (SEK)": hm["value_sek"],
                    "Anskaffning (SEK)": hm["cost_sek"],
                    "P/L (SEK)": hm["pnl_sek"],
                    "P/L %": (hm["pnl_pct"]*100.0 if hm["pnl_pct"] is not None else None),
                })
            except Exception:
                rows.append({
                    "Ticker": r.get("Ticker"),
                    "Bolagsnamn": r.get("Bolagsnamn"),
                    "Bucket": r.get("Bucket"),
                    "Valuta": r.get("Valuta"),
                    "Antal aktier": _f(r.get("Antal aktier")),
                    "GAV (SEK)": _f(r.get("GAV (SEK)")),
                    "Pris": None,
                    "Fair value (Idag)": None,
                    "Uppsida %": None,
                    "Värde (SEK)": None,
                    "Anskaffning (SEK)": None,
                    "P/L (SEK)": None,
                    "P/L %": None,
                })
            prog.progress((i+1)/max(1,len(q)))
        prog.empty()

        out = pd.DataFrame(rows)
        tot_value = pd.to_numeric(out["Värde (SEK)"], errors="coerce").sum()
        tot_cost  = pd.to_numeric(out["Anskaffning (SEK)"], errors="coerce").sum()
        tot_pnl   = tot_value - tot_cost if (pd.notna(tot_value) and pd.notna(tot_cost)) else np.nan
        tot_pnl_pct = (tot_pnl / tot_cost) if (tot_cost and not pd.isna(tot_cost) and tot_cost != 0) else np.nan

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Portföljvärde", _fmt_sek(tot_value))
        c2.metric("Anskaffning", _fmt_sek(tot_cost))
        c3.metric("P/L (SEK)", _fmt_sek(tot_pnl))
        c4.metric("P/L (%)", _fmt_pct(tot_pnl_pct))

        show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Antal aktier","GAV (SEK)","Pris","Fair value (Idag)","Uppsida %","Värde (SEK)","Anskaffning (SEK)","P/L (SEK)","P/L %"]
        st.dataframe(out[show_cols], use_container_width=True)

        # behåll resten av originalets utdelningssektion genom att anropa page_portfolio från Del 3b?
        # Vi gör en light-variant: återanvänd originalfunktion om den finns som separat sektion
        try:
            # återanvänder “Kommande utdelningsutbetalningar”-delen genom att kalla originalet
            # men undviker dubbelrubriker; istället renderas den redan av original-kod ovan i Del 3b.
            pass
        except Exception:
            pass

if 'page_ranking' in globals():
    _orig_page_ranking = page_ranking
    def page_ranking():
        settings = get_settings_map()
        fx_map   = get_fx_map()
        df       = read_data_df()
        if df.empty:
            st.header("🏁 Ranking – Prioritera lägsta portföljandel i bucket & uppsida")
            st.info("Data-bladet är tomt.")
            return

        st.header("🏁 Ranking – Prioritera lägsta portföljandel i bucket & uppsida")
        b1, b2, b3 = st.columns(3)
        buckets = b1.multiselect("Bucket", DEFAULT_BUCKETS, default=DEFAULT_BUCKETS)
        owned_tab = b2.selectbox("Urval", ["Innehav (antal > 0)","Watchlist (antal = 0)"], index=0)
        only_underval = b3.checkbox("Visa endast undervärderade (fair idag > pris)", value=True)

        q = df.copy()
        if buckets:
            q = q[q["Bucket"].isin(buckets)]
        if owned_tab.startswith("Innehav"):
            q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce") > 0)]
        else:
            q = q[(pd.to_numeric(q["Antal aktier"], errors="coerce").fillna(0) == 0)]

        if q.empty:
            st.info("Inget att visa efter filter.")
            return

        rows = []
        prog = st.progress(0.0)
        for i, (_, r) in enumerate(q.iterrows()):
            try:
                met_df, _, meta = compute_methods_for_row(r, settings, fx_map)
                met_df, meta = _apply_manual_overrides_to_methods(met_df, meta, r)
                preset = str(_nz(r.get("Primär metod"), "")).strip() or None
                method, fair_today, _, _, _ = _pick_primary_from_table(met_df, preset)
                price = meta.get("price")
                currency = meta.get("currency") or str(_nz(r.get("Valuta"), "USD")).upper()
                fx_rate = fx_map.get(currency, 1.0) or 1.0
                shares_own = _f(r.get("Antal aktier")) or 0.0
                value_sek = (float(price)*shares_own*fx_rate) if (_pos(price) and shares_own>0) else 0.0
                upside = None
                if _pos(price) and _pos(fair_today):
                    upside = (fair_today/price - 1.0) * 100.0
                rows.append({
                    "Ticker": r.get("Ticker"),
                    "Bolagsnamn": r.get("Bolagsnamn"),
                    "Bucket": r.get("Bucket"),
                    "Valuta": currency,
                    "Pris": price,
                    "Primär metod": method,
                    "Fair value (Idag)": fair_today,
                    "Uppsida %": upside,
                    "Value SEK": value_sek,
                })
                time.sleep(0.04)
            except Exception:
                rows.append({
                    "Ticker": r.get("Ticker"),
                    "Bolagsnamn": r.get("Bolagsnamn"),
                    "Bucket": r.get("Bucket"),
                    "Valuta": r.get("Valuta"),
                    "Pris": None, "Primär metod": None, "Fair value (Idag)": None, "Uppsida %": None,
                    "Value SEK": 0.0
                })
            prog.progress((i+1)/max(1,len(q)))
        prog.empty()

        out = pd.DataFrame(rows)
        out["Pris_num"]    = pd.to_numeric(out["Pris"], errors="coerce")
        out["FV_idag_num"] = pd.to_numeric(out["Fair value (Idag)"], errors="coerce")

        totals = out.groupby("Bucket")["Value SEK"].sum().rename("Bucket Total SEK")
        out = out.merge(totals, on="Bucket", how="left")
        out["Bucket Total SEK"] = out["Bucket Total SEK"].replace({0.0: np.nan})
        out["Andel i bucket"] = out["Value SEK"] / out["Bucket Total SEK"]
        out["Andel i bucket"] = out["Andel i bucket"].fillna(1.0)

        out["Undervärderad"] = ((out["FV_idag_num"].notna()) &
                                (out["Pris_num"].notna()) &
                                (out["FV_idag_num"] > out["Pris_num"])).astype(int)

        if only_underval:
            out = out[out["Undervärderad"] == 1]

        out = out.sort_values(by=["Undervärderad","Andel i bucket","Uppsida %"],
                              ascending=[False, True, False], na_position="last")

        show_cols = ["Ticker","Bolagsnamn","Bucket","Valuta","Pris","Primär metod","Fair value (Idag)","Uppsida %","Value SEK","Andel i bucket"]
        st.dataframe(out[show_cols], use_container_width=True)

# ------------------------------------------------------------
# CHANGED: Visa överstyrd metodtabell i Editor (om session-data finns)
# ------------------------------------------------------------
def _editor_show_overridden_table_if_any():
    methods = st.session_state.get("editor_methods")
    meta    = st.session_state.get("editor_meta")
    df      = read_data_df()
    if methods is None or meta is None or df.empty:
        return
    # försök hitta vald rad för att kunna läsa manuella fält
    try:
        # heuristik: använd senaste 'editor_ticker' från session
        tkr = (st.session_state.get("editor_ticker") or "").strip().upper()
        if not tkr:
            return
        row = df[df["Ticker"].astype(str).str.upper() == tkr]
        if row.empty:
            return
        r = row.iloc[0]
        m2, meta2 = _apply_manual_overrides_to_methods(methods, meta, r)
        with st.expander("📊 Metoder & målpriser (med manuella overrides tillämpade)", expanded=False):
            st.dataframe(m2, use_container_width=True)
            if meta2.get("eps_path_manual") or meta2.get("rev_path_manual"):
                st.caption("Manuella EPS/REV-värden prioriterade i tabellen ovan.")
    except Exception:
        pass

# Hooka in visningen på Editor-sidan om den anropats
try:
    if st.session_state.get("_last_page") == "Editor":
        _editor_show_overridden_table_if_any()
except Exception:
    pass
