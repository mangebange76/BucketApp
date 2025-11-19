# core_utils.py — generella hjälpfunktioner (tid, talformat)

from __future__ import annotations

import math
import datetime as dt
from typing import Any, Optional


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
      - Returnerar None om det inte går att tolka
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
    """Som _f men returnerar endast icke-negativa tal (>= 0), annars None."""
    v = _f(x)
    if v is None:
        return None
    try:
        if not math.isfinite(v):
            return None
    except Exception:
        return None
    return v
