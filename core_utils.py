# core_utils.py – helpers, konstanter, wrappers

from __future__ import annotations

import math
import datetime as dt
from typing import Any, Dict, List, Optional

import numpy as np  # kan behövas i annan kod
import pandas as pd
import streamlit as st


# =========================
# Globala konstanter
# =========================

# Google Sheet: titlar på flikar
DATA_TITLE      = "Data"
FX_TITLE        = "Valutakurser"
SETTINGS_TITLE  = "Settings"
SNAPSHOT_TITLE  = "Snapshot"

# Settings-bladets kolumner (nyckel → värde)
SETTINGS_COLUMNS: List[str] = ["Nyckel", "Värde"]

# Data-bladets grundschema
DATA_COLUMNS: List[str] = [
    "Timestamp",
    "Ticker",
    "Bolagsnamn",
    "Sektor",
    "Sektor-detalj",          # NY: t.ex. 'Semiconductors', 'REIT—Residential'
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
# Hjälpfunktioner (tid & tal)
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
      - Returnerar None om det inte går att tolka.
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
    """Som _f men returnerar endast tal där värdet är finite; annars None."""
    v = _f(x)
    if v is None:
        return None
    try:
        if not math.isfinite(v):
            return None
    except Exception:
        return None
    return v


# =========================
# Wrappers mot sheets_io
# =========================
# (så att gammal kod som importerar från core_utils fortsatt fungerar)

def read_data_df() -> pd.DataFrame:
    from sheets_io import read_data_df as _read
    return _read()


def write_data_df(df: pd.DataFrame) -> None:
    from sheets_io import write_data_df as _write
    _write(df)


def get_settings_map() -> Dict[str, str]:
    from sheets_io import get_settings_map as _gsm
    return _gsm()


def get_fx_map() -> Dict[str, float]:
    from sheets_io import get_fx_map as _gfm
    return _gfm()


def _load_data_into_session() -> None:
    """Wrapper för kompatibilitet – anropar sheets_io._load_data_into_session()."""
    from sheets_io import _load_data_into_session as _lds
    _lds()
