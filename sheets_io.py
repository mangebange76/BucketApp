# sheets_io.py – Google Sheets I/O + Settings + FX

from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, Optional
from collections.abc import Mapping

import pandas as pd
import numpy as np
import streamlit as st
import gspread
import yfinance as yf
from gspread import Spreadsheet, Worksheet
from gspread.exceptions import WorksheetNotFound, APIError
from google.oauth2.service_account import Credentials

from core_utils import (
    _f,
    _nz,
    now_stamp,
    DATA_TITLE,
    FX_TITLE,
    SETTINGS_TITLE,
    SNAPSHOT_TITLE,
    SETTINGS_COLUMNS,
    DATA_COLUMNS,
)


# =============================
# Google auth & Spreadsheet
# =============================

def _normalize_private_key(creds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hanterar privata nycklar där radbrytningar är ersatta med '\\n' i secrets.
    """
    pk = creds.get("private_key")
    if isinstance(pk, str) and "\\n" in pk:
        creds["private_key"] = pk.replace("\\n", "\n")
    return creds


def _env_or_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Letar efter en nyckel i både os.environ och st.secrets, med flera alias.

    Exempel:
      - "SHEET_URL" → letar även efter GOOGLE_SHEET_URL, SPREADSHEET_URL osv.
      - "SHEET_ID"  → letar även efter GOOGLE_SHEET_ID, SPREADSHEET_ID osv.
    """
    key_upper = key.upper()

    # Grundkandidater
    candidates = {key, key.upper(), key.lower()}

    # Alias för URL
    if key_upper in {"SHEET_URL", "GOOGLE_SHEET_URL"}:
        candidates.update(
            {
                "SHEET_URL",
                "sheet_url",
                "GOOGLE_SHEET_URL",
                "google_sheet_url",
                "SPREADSHEET_URL",
                "spreadsheet_url",
            }
        )

    # Alias för ID
    if key_upper in {"SHEET_ID", "GOOGLE_SHEET_ID", "SPREADSHEET_ID"}:
        candidates.update(
            {
                "SHEET_ID",
                "sheet_id",
                "GOOGLE_SHEET_ID",
                "google_sheet_id",
                "SPREADSHEET_ID",
                "spreadsheet_id",
            }
        )

    # Sök i miljövariabler först
    for name in candidates:
        val = os.environ.get(name)
        if val:
            return str(val)

    # Sedan i Streamlit secrets (om det finns)
    try:
        secrets_obj = getattr(st, "secrets", None)
    except Exception:
        secrets_obj = None

    if secrets_obj is not None:
        for name in candidates:
            try:
                val = secrets_obj.get(name)
            except Exception:
                val = None
            if val:
                return str(val)

    return default


def _load_google_credentials_dict() -> Dict[str, Any]:
    """
    Försöker tolka GOOGLE_CREDENTIALS på flera sätt:

    1) [GOOGLE_CREDENTIALS] som tabell i secrets.toml  → Mapping
    2) GOOGLE_CREDENTIALS = "{...json...}"             → JSON-sträng
    3) Service account-fälten på toppnivå i secrets    → type, project_id, private_key, client_email, ...
    """
    secrets_obj = getattr(st, "secrets", None)
    if secrets_obj is None:
        raise RuntimeError("Saknar Streamlit secrets – kunde inte hitta GOOGLE_CREDENTIALS.")

    raw = None
    try:
        raw = secrets_obj.get("GOOGLE_CREDENTIALS", None)
    except Exception:
        raw = None

    # 1) Tabell / dict: [GOOGLE_CREDENTIALS] i secrets.toml
    if isinstance(raw, Mapping):
        creds_dict = dict(raw)
        return _normalize_private_key(creds_dict)

    # 2) JSON-sträng: GOOGLE_CREDENTIALS = "{...}"
    if isinstance(raw, str):
        try:
            creds_dict = json.loads(raw)
            return _normalize_private_key(creds_dict)
        except Exception:
            # Fortsätt till fallback i stället för att kasta fel
            pass

    # 3) Service account-fält direkt på toppnivå i secrets.toml
    if isinstance(secrets_obj, Mapping):
        candidate_keys = [
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "auth_uri",
            "token_uri",
            "auth_provider_x509_cert_url",
            "client_x509_cert_url",
        ]
        if any(k in secrets_obj for k in candidate_keys):
            creds_dict: Dict[str, Any] = {}
            for k in candidate_keys:
                if k in secrets_obj:
                    creds_dict[k] = secrets_obj.get(k)
            if "private_key" in creds_dict:
                return _normalize_private_key(creds_dict)

    raise RuntimeError(
        "Kunde inte tolka GOOGLE_CREDENTIALS. Antingen:\n"
        "  • lägg hela service account JSON i [GOOGLE_CREDENTIALS] som tabell i secrets.toml\n"
        "  • eller lägg JSON-strängen som GOOGLE_CREDENTIALS = \"{...}\"\n"
        "  • eller lägg service account-fälten (type, project_id, private_key, client_email, ...) på toppnivå."
    )


@st.cache_resource(show_spinner=False)
def _get_gspread_client() -> gspread.Client:
    """
    Bygger en gspread-klient från Streamlit secrets, med stöd för:
      - [GOOGLE_CREDENTIALS] (toml-tabell)
      - GOOGLE_CREDENTIALS = "{...json...}"
      - service account-fält direkt på toppnivå i secrets.toml
    """
    creds_dict = _load_google_credentials_dict()

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


@st.cache_resource(show_spinner=False)
def _open_spreadsheet() -> Spreadsheet:
    """
    Öppnar kalkylarket med stöd för flera olika nycklar:
      - SHEET_URL / GOOGLE_SHEET_URL / spreadsheet_url
      - SHEET_ID  / GOOGLE_SHEET_ID  / SPREADSHEET_ID
    """
    # Först försök med URL
    sheet_url = _env_or_secret("SHEET_URL")
    # Sedan ID som fallback
    sheet_id = _env_or_secret("SHEET_ID")

    client = _get_gspread_client()

    if sheet_url and str(sheet_url).strip():
        return client.open_by_url(sheet_url)

    if sheet_id and str(sheet_id).strip():
        return client.open_by_key(sheet_id)

    raise RuntimeError(
        "Ange SHEET_URL eller SHEET_ID (eller GOOGLE_SHEET_URL / GOOGLE_SHEET_ID) "
        "i Streamlit secrets eller som miljövariabler."
    )


def _open_worksheet(title: str) -> Worksheet:
    """Öppna (eller skapa) en flik i Spreadsheet med angivet title."""
    ss = _open_spreadsheet()
    try:
        return ss.worksheet(title)
    except WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=200, cols=50)
        return ws


# =============================
# Läs/skriv DataFrame <-> Sheet
# =============================

def _read_df(title: str) -> pd.DataFrame:
    """
    Läs en flik som DataFrame.
    Första raden antas vara header. Tomt → tom DataFrame.
    """
    try:
        ws = _open_worksheet(title)
    except Exception as e:
        st.error(f"Kunde inte öppna blad '{title}': {e}")
        return pd.DataFrame()

    try:
        values = ws.get_all_values()
    except APIError as e:
        st.error(f"API-fel vid läsning av blad '{title}': {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Fel vid läsning av blad '{title}': {e}")
        return pd.DataFrame()

    if not values:
        return pd.DataFrame()

    header = [str(c) for c in values[0]]
    rows = values[1:]
    if not header:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=header)
    df = df.dropna(how="all").reset_index(drop=True)
    return df


def _write_df(title: str, df: pd.DataFrame) -> None:
    """
    Skriv en DataFrame till en flik. Första raden = header.
    Överskriver hela bladet.
    """
    if df is None:
        df = pd.DataFrame()
    df = df.copy()

    df = df.fillna("")
    df = df.astype(str)

    ws = _open_worksheet(title)
    try:
        ws.clear()
        if df.empty:
            return
        values = [list(df.columns)] + df.values.tolist()
        ws.update(values)
    except APIError as e:
        st.error(f"API-fel vid skrivning till blad '{title}': {e}")
        raise
    except Exception as e:
        st.error(f"Fel vid skrivning till blad '{title}': {e}")
        raise


# =============================
# Hjälpare för DATA-bladet
# =============================

def _ensure_data_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Säkerställ att alla DATA_COLUMNS finns. Lägg till saknade som NaN.
    Behåller även ev. extra kolumner (t.ex. manuella).
    """
    if df is None or df.empty:
        df = pd.DataFrame(columns=DATA_COLUMNS)
    for col in DATA_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    extra = [c for c in df.columns if c not in DATA_COLUMNS]
    df = df[DATA_COLUMNS + extra]
    return df


def read_data_df() -> pd.DataFrame:
    """Läs Data-bladet från Sheets och säkerställ kolumnschema."""
    df = _read_df(DATA_TITLE)
    df = _ensure_data_columns(df)
    return df


def write_data_df(df: pd.DataFrame) -> None:
    """Skriv Data-bladet till Sheets, med DATA_COLUMNS först."""
    if df is None:
        df = pd.DataFrame(columns=DATA_COLUMNS)
    df = _ensure_data_columns(df)
    _write_df(DATA_TITLE, df)


# =============================
# Settings-hantering
# =============================

def _ensure_settings_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Säkerställ att Settings-bladet har åtminstone de kolumner som
    SETTINGS_COLUMNS anger (oftast t.ex. ['Nyckel', 'Värde']).
    """
    if df is None or df.empty:
        if SETTINGS_COLUMNS and len(SETTINGS_COLUMNS) >= 2:
            df = pd.DataFrame(columns=SETTINGS_COLUMNS)
        else:
            df = pd.DataFrame(columns=["Nyckel", "Värde"])
        return df

    target_cols: list[str]
    if SETTINGS_COLUMNS and len(SETTINGS_COLUMNS) >= 2:
        target_cols = list(SETTINGS_COLUMNS)
    else:
        # Fallback: använd första två kolumner + behåll resten
        if len(df.columns) < 2:
            df = pd.DataFrame(columns=["Nyckel", "Värde"])
            return df
        target_cols = [df.columns[0], df.columns[1]]

    for c in target_cols:
        if c not in df.columns:
            df[c] = np.nan
    extra = [c for c in df.columns if c not in target_cols]
    df = df[target_cols + extra]
    return df


def _upsert_setting(key: str, value: Any) -> None:
    """
    Skapar eller uppdaterar en rad i Settings-bladet med given nyckel.
    Använder första kolumnen som 'nyckel', andra som 'värde'.
    """
    df = _read_df(SETTINGS_TITLE)
    df = _ensure_settings_columns(df)

    if len(df.columns) < 2:
        # Kan inte göra så mycket utan minst två kolumner
        return

    key_col = df.columns[0]
    val_col = df.columns[1]

    if key_col not in df.columns or val_col not in df.columns:
        return

    mask = df[key_col].astype(str) == str(key)
    if mask.any():
        df.loc[mask, val_col] = str(value)
    else:
        new_row = {col: "" for col in df.columns}
        new_row[key_col] = str(key)
        new_row[val_col] = str(value)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    _write_df(SETTINGS_TITLE, df)


@st.cache_data(ttl=300, show_spinner=False)
def get_settings_map() -> Dict[str, str]:
    """
    Läser Settings-bladet och returnerar en dict:
      { 'nyckel': 'värde', ... }
    Stöder både 'Nyckel'/'Värde' och 'Key'/'Value'.
    """
    df = _read_df(SETTINGS_TITLE)
    if df is None or df.empty:
        return {}

    key_col = None
    val_col = None
    for cand in ("Nyckel", "Key", "Setting", "Inställning"):
        if cand in df.columns:
            key_col = cand
            break
    for cand in ("Värde", "Varde", "Value"):
        if cand in df.columns:
            val_col = cand
            break

    if key_col is None or val_col is None:
        if len(df.columns) >= 2:
            key_col = df.columns[0]
            val_col = df.columns[1]
        else:
            return {}

    settings: Dict[str, str] = {}
    for _, r in df.iterrows():
        k_raw = r.get(key_col)
        if k_raw is None:
            continue
        k = str(k_raw).strip()
        if not k:
            continue
        v = r.get(val_col)
        settings[k] = "" if v is None else str(v).strip()
    return settings


# =============================
# FX-hantering (Valutakurser)
# =============================

def _fx_df_to_map(df: pd.DataFrame) -> Dict[str, float]:
    """
    Hjälpare: omvandlar en DataFrame från FX-bladet till dict {valuta → kurs}.
    """
    if df is None or df.empty:
        return {}

    # Säkerställ str-kolumnnamn
    df = df.copy()
    df.columns = [str(c) for c in df.columns]

    cur_col = None
    for cand in ("Valuta", "Currency", "CUR", "Fx", "FX"):
        if cand in df.columns:
            cur_col = cand
            break

    rate_col = None
    for cand in ("SEK", "Kurs", "Rate", "Fx-rate"):
        if cand in df.columns:
            rate_col = cand
            break

    # Fallback: använd första två kolumner
    if cur_col is None or rate_col is None:
        if len(df.columns) >= 2:
            cur_col = df.columns[0]
            rate_col = df.columns[1]
        else:
            return {}

    out: Dict[str, float] = {}
    for _, r in df.iterrows():
        c = r.get(cur_col)
        if c is None:
            continue
        code = str(c).strip().upper()
        if not code:
            continue
        val = _f(r.get(rate_col))
        if val is None or not math.isfinite(val) or val <= 0:
            continue

        # Direkt valuta-kod
        out[code] = float(val)
        # Stöd för t.ex. "USDSEK" → "USD"
        if len(code) == 6 and code.endswith("SEK"):
            base = code[:3]
            out[base] = float(val)

    if "SEK" not in out:
        out["SEK"] = 1.0
    return out


@st.cache_data(ttl=300, show_spinner=False)
def get_fx_map() -> Dict[str, float]:
    """
    Läser 'Valutakurser'-bladet och returnerar:
      { 'USD': 10.50, 'NOK': 1.02, ... }  (valuta → SEK-kurs)
    """
    df = _read_df(FX_TITLE)
    return _fx_df_to_map(df)


def _fetch_fx_to_sek(code: str) -> Optional[float]:
    """
    Hämtar live-kurs för <code>/SEK via Yahoo Finance, t.ex. USD → ticker 'USDSEK=X'.
    Returnerar kurs (float) eller None vid fel.
    """
    cur = (code or "").strip().upper()
    if not cur:
        return None
    if cur == "SEK":
        return 1.0

    symbol = f"{cur}SEK=X"
    try:
        t = yf.Ticker(symbol)
    except Exception:
        return None

    price = None

    # Försök först med fast_info
    try:
        fi = getattr(t, "fast_info", None)
        if isinstance(fi, dict):
            price = fi.get("last_price") or fi.get("lastPrice") or fi.get("last")
    except Exception:
        price = None

    # Fallback: history
    if price is None:
        try:
            hist = t.history(period="5d")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].iloc[-1])
        except Exception:
            price = None

    try:
        if price is None:
            return None
        price_f = float(price)
        if not math.isfinite(price_f) or price_f <= 0:
            return None
        return price_f
    except Exception:
        return None


def refresh_fx_live() -> None:
    """
    Hämtar aktuella valutakurser via Yahoo Finance och skriver dem till FX-bladet.
    - Valutor hämtas från:
        • 'Valuta'-kolumnen i Data-bladet
        • några standard: USD, EUR, NOK, CAD, GBP, DKK
    - Skriver fliken 'Valutakurser' med kolumner:
        ['Valuta', 'SEK', 'Senast uppdaterad']
    - Skriver även 'FX_LAST_UPDATE_TS' + 'FX_LAST_SOURCE' i Settings-bladet.
    """
    # 1) Bestäm vilka valutor vi bryr oss om
    try:
        data_df = read_data_df()
    except Exception:
        data_df = pd.DataFrame()

    codes: set[str] = set()
    if data_df is not None and not data_df.empty and "Valuta" in data_df.columns:
        for v in data_df["Valuta"].dropna().tolist():
            s = str(v).strip().upper()
            if len(s) == 3:
                codes.add(s)

    # Grundvalutor vi nästan alltid vill ha
    base_defaults = {"USD", "EUR", "NOK", "CAD", "GBP", "DKK"}
    codes.update(base_defaults)
    codes.add("SEK")

    # 2) Existerande kurser (ifall någon ny hämtning misslyckas)
    try:
        df_old_fx = _read_df(FX_TITLE)
        existing_map = _fx_df_to_map(df_old_fx)
    except Exception:
        existing_map = {}

    # 3) Hämta livekurser
    ts = now_stamp()
    new_rates: Dict[str, float] = {}
    failed: list[str] = []

    for code in sorted(codes):
        if code == "SEK":
            new_rates["SEK"] = 1.0
            continue

        rate = _fetch_fx_to_sek(code)
        if rate is None:
            # fallback: gammal kurs om den finns
            old = existing_map.get(code)
            if old is not None and math.isfinite(old) and old > 0:
                new_rates[code] = float(old)
            else:
                failed.append(code)
        else:
            new_rates[code] = float(rate)

    # Om vi inte lyckades få nåt alls (förutom ev. SEK) → gör inget
    if not new_rates:
        return

    # 4) Skriv tillbaka till FX-bladet
    rows = []
    for c, r in sorted(new_rates.items()):
        rows.append(
            {
                "Valuta": c,
                "SEK": r,
                "Senast uppdaterad": ts,
            }
        )
    df_fx = pd.DataFrame(rows, columns=["Valuta", "SEK", "Senast uppdaterad"])
    _write_df(FX_TITLE, df_fx)

    # 5) Sätt timestamp + källa i Settings
    _upsert_setting("FX_LAST_UPDATE_TS", ts)
    _upsert_setting("FX_LAST_SOURCE", "Yahoo Finance FX (XXXSEK=X)")

    # 6) Invalidera cache så get_fx_map() läser in nya värden
    try:
        get_fx_map.clear()
    except Exception:
        try:
            st.cache_data.clear()
        except Exception:
            pass

    # Ev. debug/info i logg (inte i UI)
    if failed:
        # Tyst logg, inte UI
        print(f"[refresh_fx_live] Kunde inte uppdatera vissa valutor: {', '.join(failed)}")


# =============================
# Laddning av DATA i session
# =============================

def _load_data_into_session() -> None:
    """
    Hjälpare som ser till att st.session_state["DATA"] är laddad.
    Kan anropas från main() eller andra moduler.
    """
    if "DATA" not in st.session_state or not isinstance(st.session_state["DATA"], pd.DataFrame):
        try:
            st.session_state["DATA"] = read_data_df()
        except Exception as e:
            st.error(f"Kunde inte ladda Data-bladet: {e}")
            st.session_state["DATA"] = pd.DataFrame(columns=DATA_COLUMNS)
