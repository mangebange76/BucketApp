# app_pages.py — Settings, Snapshot, Editor, Lägg till, Portfölj,
#                Massuppdatering & Köpförslag (UI)

from __future__ import annotations

import math
import time
import datetime as dt
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import streamlit as st

from core_utils import _f, _pos, _nz, now_stamp, DEFAULT_BUCKETS
from sheets_io import (
    _read_df,
    _write_df,
    read_data_df,
    write_data_df,
    get_settings_map,
    get_fx_map,
    DATA_COLUMNS,
    SETTINGS_COLUMNS,
    SETTINGS_TITLE,
    SNAPSHOT_TITLE,
)
from valuation import fetch_from_yahoo, _fetch_eps_estimates_yahoo, compute_methods_for_row


# -------------------------
# Autosnapshot vid appstart (max 10 snapshots)
# -------------------------
def _auto_snapshot_on_import() -> None:
    """
    Skapar en snapshot av Data-bladet vid första import av denna modul.
    - Läser 'Data' via read_data_df()
    - Lägger in kolumn 'Snapshot TS' med tidsstämpel
    - Append:ar till SNAPSHOT_TITLE-bladet
    - Behåller max 10 unika snapshot-tidsstämplar (äldsta tas bort)
    Körs EN gång per Python-process (modul-import), vilket i Streamlit
    motsvarar "appstart" snarare än varje rerun.
    """
    try:
        df = read_data_df()
        if df is None or df.empty:
            return

        ts = now_stamp()
        df_add = df.copy()
        df_add.insert(0, "Snapshot TS", ts)

        try:
            snap_old = _read_df(SNAPSHOT_TITLE)
        except Exception:
            snap_old = None

        if snap_old is None or snap_old.empty:
            snap_new = df_add
        else:
            snap_new = pd.concat([snap_old, df_add], ignore_index=True)

            # Säkerställ kolumnen finns
            if "Snapshot TS" not in snap_new.columns:
                # Om äldre schema saknade kolumnen – sätt allt till första värdet
                snap_new.insert(0, "Snapshot TS", ts)

            # Behåll max 10 snapshots baserat på unika "Snapshot TS"
            ts_list = snap_new["Snapshot TS"].astype(str).tolist()
            # Bevara ordning: första förekommer = äldst
            seen = []
            for v in ts_list:
                if v not in seen:
                    seen.append(v)

            if len(seen) > 10:
                keep_ts = set(seen[-10:])  # behåll de 10 senaste
                snap_new = snap_new[snap_new["Snapshot TS"].astype(str).isin(keep_ts)].copy()

        _write_df(SNAPSHOT_TITLE, snap_new)
    except Exception:
        # Snapshot får aldrig krascha appen – svälj ev. fel tyst.
        pass


_auto_snapshot_on_import()


# -------------------------
# Små helpers
# -------------------------
def _safe_str_val(x: Any) -> str:
    """
    Returnerar '' om värdet är None/NaN/'nan', annars strippad sträng.
    Hindrar att vi får 'nan' som bolagsnamn/sektor och gör det lättare
    att avgöra om fältet verkligen är tomt.
    """
    if x is None:
        return ""
    if isinstance(x, float) and (pd.isna(x) or math.isnan(x)):
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none"):
        return ""
    return s


def _fmt_sek(v: Any) -> str:
    """
    Robust formattering till svenskt talformat '1 234,56'.
    Om v inte går att tolkas som tal → ''.
    """
    try:
        if v is None:
            return ""
        if isinstance(v, (float, int)):
            if isinstance(v, float) and (pd.isna(v) or math.isnan(v)):
                return ""
            val = float(v)
        else:
            val = float(v)
        if not math.isfinite(val):
            return ""
        return f"{val:,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return ""


# -------------------------
# Små UI-hjälpare (sök + nav)
# -------------------------
def _names_map_from_df(df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if df is None or df.empty:
        return out
    for _, r in df.iterrows():
        t = str(r.get("Ticker") or "").upper().strip()
        n = _safe_str_val(r.get("Bolagsnamn"))
        if t:
            out[t] = f"{t} — {n}" if n else t
    return out


def _select_with_search_nav(
    label: str,
    options: List[str],
    names_map: Dict[str, str],
    session_idx_key: str,
    query_key: str,
) -> Optional[str]:
    if not options:
        st.info("Inga alternativ.")
        return None
    options = sorted(list({o.upper().strip() for o in options if o}))
    if session_idx_key not in st.session_state:
        st.session_state[session_idx_key] = 0
    st.session_state[session_idx_key] = max(
        0, min(st.session_state[session_idx_key], len(options) - 1)
    )

    q = st.text_input("Sök (ticker/namn)", key=query_key)
    if q:
        ql = q.lower().strip()
        shown = [o for o in options if (ql in o.lower()) or (ql in names_map.get(o, o).lower())]
        if not shown:
            shown = options
    else:
        shown = options

    pretty = [names_map.get(o, o) for o in shown]
    idx = min(st.session_state[session_idx_key], len(shown) - 1)
    picked_pretty = st.selectbox(label, pretty, index=idx)
    picked = shown[pretty.index(picked_pretty)] if picked_pretty in pretty else shown[idx]

    c1, c2, c3 = st.columns([1, 1, 6])
    with c1:
        if st.button("◀︎", use_container_width=True, disabled=len(shown) <= 1):
            st.session_state[session_idx_key] = (shown.index(picked) - 1) % len(shown)
    with c2:
        if st.button("▶︎", use_container_width=True, disabled=len(shown) <= 1):
            st.session_state[session_idx_key] = (shown.index(picked) + 1) % len(shown)
    with c3:
        st.caption(f"{shown.index(picked)+1}/{len(shown)}")
    return picked


def _show_df(df: pd.DataFrame, height: int = 360, use_container_width: bool = True) -> None:
    try:
        st.dataframe(df, use_container_width=use_container_width, height=height)
    except Exception:
        st.table(df.head(200))


# ============================================================
# ⚙️ Settings (redigerbar)
# ============================================================
def page_settings() -> None:
    st.header("⚙️ Settings")
    s_df = _read_df(SETTINGS_TITLE)
    if s_df is None or s_df.empty:
        s_df = pd.DataFrame(columns=SETTINGS_COLUMNS)

    st.caption("Redigera nedan och klicka **Spara**.")
    edited = st.data_editor(
        s_df,
        num_rows="dynamic",
        use_container_width=True,
        key="settings_editor",
    )

    if st.button("💾 Spara Settings"):
        try:
            df_to_write = edited.copy()
            if df_to_write is None:
                df_to_write = pd.DataFrame(columns=SETTINGS_COLUMNS)
            if df_to_write.empty and len(df_to_write.columns) == 0:
                df_to_write = pd.DataFrame(columns=SETTINGS_COLUMNS)

            _write_df(SETTINGS_TITLE, df_to_write)

            # Töm cache så nya värden används direkt
            st.cache_data.clear()
            st.session_state["SETTINGS_MAP"] = get_settings_map()
            st.success("Settings sparade. Laddar om sidan…")
        except Exception as e:
            st.error(f"Kunde inte spara: {e}")


# ============================================================
# 🕒 Snapshot (read-only)
# ============================================================
def page_snapshot() -> None:
    st.header("🕒 Snapshot")
    snap = _read_df(SNAPSHOT_TITLE)
    if snap.empty:
        st.info("Inga snapshots ännu.")
        return
    _show_df(snap, height=420, use_container_width=True)


# ============================================================
# ✏️ Editor (manuellt + Yahoo)
# ============================================================
def _ensure_editor_stamp_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "EPS 1Y uppdaterad",
        "EPS 2Y uppdaterad",
        "Rev 1Y uppdaterad",
        "Rev 2Y uppdaterad",
        "Senast manuellt uppdaterad",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=[*DATA_COLUMNS, *cols])
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def _build_updates_from_yahoo(tkr: str, existing_row: pd.Series) -> Dict[str, Any]:
    y = fetch_from_yahoo(tkr)

    # Fyll Bolagsnamn & Sektor – men bara om de inte redan har manuell text
    existing_name = _safe_str_val(existing_row.get("Bolagsnamn"))
    if existing_name:
        name = existing_name
    else:
        name = (
            y.get("name")
            or y.get("longName")
            or y.get("shortName")
        )

    existing_sector = _safe_str_val(existing_row.get("Sektor"))
    if existing_sector:
        sector = existing_sector
    else:
        sector = (
            y.get("sector")
            or y.get("industry")
        )

    try:
        est = _fetch_eps_estimates_yahoo(tkr)
    except Exception:
        est = {"eps_1y": None, "eps_2y": None}
    updates = {
        "Timestamp": now_stamp(),
        "Bolagsnamn": name,
        "Sektor": sector,
        "Aktuell kurs": _f(y.get("price")),
        "Valuta": (y.get("currency") or existing_row.get("Valuta")),
        "Utestående aktier": _f(y.get("shares_out")),
        "Net debt": _f(y.get("net_debt")),
        "Rev TTM": _f(y.get("rev_ttm")),
        "EBITDA TTM": _f(y.get("ebitda_ttm")),
        "EPS TTM": _f(y.get("eps_ttm")),
        "PE TTM": _f(y.get("pe_ttm")),
        "PE FWD": _f(y.get("pe_fwd")),
        "EV/Revenue": _f(y.get("ev_rev")),
        "EV/EBITDA": _f(y.get("ev_ebitda")),
        "P/B": _f(y.get("p_b")),
        "BVPS": _f(y.get("bvps")),
        "Rev CAGR": _f(y.get("rev_cagr_hist")),
        "EPS CAGR": _f(y.get("eps_cagr_hist")),
        "Årlig utdelning": _f(y.get("dps_annual")),
        "EPS 1Y": existing_row.get("EPS 1Y") if pd.notna(existing_row.get("EPS 1Y")) else _f(est.get("eps_1y")),
        "EPS 2Y": existing_row.get("EPS 2Y") if pd.notna(existing_row.get("EPS 2Y")) else _f(est.get("eps_2y")),
        "Senast auto uppdaterad": now_stamp(),
        "Auto källa": "Yahoo",
    }
    return {
        k: v
        for k, v in updates.items()
        if v is not None and not (isinstance(v, float) and pd.isna(v))
    }


def page_editor() -> None:
    st.header("✏️ Editor (manuellt + Yahoo)")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    df = _ensure_editor_stamp_cols(df)
    tickers = df["Ticker"].dropna().astype(str).unique().tolist()
    names_map = _names_map_from_df(df)

    sel = _select_with_search_nav("Välj rad", tickers, names_map, "editor_idx", "editor_q")
    if not sel:
        return

    ridx = df.index[df["Ticker"].astype(str) == sel]
    if len(ridx) == 0:
        st.error("Kunde inte hitta vald rad.")
        return
    idx = ridx[0]
    row = df.loc[idx].copy()

    c1, c2 = st.columns(2)
    with c1:
        new_ticker = st.text_input("Ticker", value=str(row.get("Ticker") or "").upper())
        antal_in   = st.text_input("Antal aktier", value=str(_f(row.get("Antal aktier")) or ""))
        gav_in     = st.text_input("GAV (SEK)", value=str(_f(row.get("GAV (SEK)")) or ""))
        bucket_opts = [""] + DEFAULT_BUCKETS
        current_bucket = str(row.get("Bucket") or "")
        try:
            bucket_idx = bucket_opts.index(current_bucket) if current_bucket in bucket_opts else 0
        except Exception:
            bucket_idx = 0
        bucket_sel = st.selectbox("Bucket", bucket_opts, index=bucket_idx)
    with c2:
        eps1_in = st.text_input("EPS 1Y (estimat)", value=str(_f(row.get("EPS 1Y")) or ""))
        eps2_in = st.text_input("EPS 2Y (estimat)", value=str(_f(row.get("EPS 2Y")) or ""))
        rev1_in = st.text_input("Rev 1Y (miljoner)", value=str(_f(row.get("Rev 1Y")) or ""))
        rev2_in = st.text_input("Rev 2Y (miljoner)", value=str(_f(row.get("Rev 2Y")) or ""))

    colA, colB = st.columns(2)
    with colA:
        if st.button("💾 Spara (session)"):
            try:
                df.loc[idx, "Ticker"] = str(new_ticker).upper().strip() or sel
                df.loc[idx, "Antal aktier"] = _f(antal_in) or 0.0
                if _f(gav_in) is not None:
                    df.loc[idx, "GAV (SEK)"] = _f(gav_in)
                if bucket_sel is not None:
                    df.loc[idx, "Bucket"] = bucket_sel if bucket_sel != "" else np.nan
                if _f(eps1_in) is not None:
                    df.loc[idx, "EPS 1Y"] = _f(eps1_in)
                if _f(eps2_in) is not None:
                    df.loc[idx, "EPS 2Y"] = _f(eps2_in)
                if _f(rev1_in) is not None:
                    df.loc[idx, "Rev 1Y"] = _f(rev1_in) * 1_000_000.0
                if _f(rev2_in) is not None:
                    df.loc[idx, "Rev 2Y"] = _f(rev2_in) * 1_000_000.0
                df.loc[idx, "Senast manuellt uppdaterad"] = now_stamp()
                st.session_state["DATA"] = df
                st.success("Sparat i session.")
            except Exception as e:
                st.error(f"Fel: {e}")

    with colB:
        if st.button("⬆️ Spara till Google Sheets + Yahoo-prefill"):
            try:
                tkr = str(_nz(df.loc[idx, "Ticker"], new_ticker or sel)).upper()
                updates = _build_updates_from_yahoo(tkr, df.loc[idx])

                df_cur = df.copy()
                for k, v in updates.items():
                    if k not in df_cur.columns:
                        df_cur[k] = np.nan
                    df_cur.at[idx, k] = v

                write_data_df(df_cur)
                st.session_state["DATA"] = df_cur
                st.success(f"{tkr}: Rad sparad och uppdaterad från Yahoo.")
            except Exception as e:
                st.error(f"Fel vid sparning: {e}")

    st.markdown("---")
    st.subheader("Förhandsgranskning")
    _show_df(df.loc[[idx]], height=240, use_container_width=True)

# ============================================================
# ➕ Lägg till ticker (med valfri Yahoo-prefill)
# ============================================================
def page_add_ticker() -> None:
    st.header("➕ Lägg till ticker")

    tkr = st.text_input("Ticker").upper().strip()
    c1, c2, c3 = st.columns(3)
    with c1:
        bolagsnamn = st.text_input("Bolagsnamn")
        sektor     = st.text_input("Sektor")
    with c2:
        bucket_sel = st.selectbox("Bucket", [""] + DEFAULT_BUCKETS, index=0)
        valuta     = st.text_input("Valuta (t.ex. USD)", value="USD").upper()
    with c3:
        antal = st.text_input("Antal aktier", value="")
        gav   = st.text_input("GAV (SEK)", value="")

    st.markdown("**Prognosfält (frivilliga)**")
    c4, c5 = st.columns(2)
    with c4:
        eps1_in = st.text_input("EPS 1Y (estimat)")
        rev1_in = st.text_input("Rev 1Y (miljoner)")
    with c5:
        eps2_in = st.text_input("EPS 2Y (estimat)")
        rev2_in = st.text_input("Rev 2Y (miljoner)")

    do_prefill = st.checkbox("Hämta & fyll från Yahoo", value=True)

    if st.button("💾 Lägg till"):
        if not tkr:
            st.warning("Ticker krävs.")
            return
        try:
            base_df = read_data_df()
            if not base_df.empty and (base_df["Ticker"].astype(str).str.upper() == tkr.upper()).any():
                st.error("Ticker finns redan i DATA.")
                return

            new_row: Dict[str, Any] = {c: np.nan for c in DATA_COLUMNS}
            new_row.update({
                "Timestamp": now_stamp(),
                "Ticker": tkr,
                "Bolagsnamn": bolagsnamn if bolagsnamn else np.nan,
                "Sektor": sektor if sektor else np.nan,
                "Bucket": bucket_sel if bucket_sel != "" else np.nan,
                "Valuta": valuta or "USD",
            })

            qty_v = _f(antal) or 0.0
            gav_v = _f(gav)
            new_row["Antal aktier"] = qty_v
            if gav_v is not None:
                new_row["GAV (SEK)"] = gav_v

            eps1_v  = _f(eps1_in)
            eps2_v  = _f(eps2_in)
            rev1_vm = (_f(rev1_in) or 0.0) * 1_000_000.0 if rev1_in not in (None, "") else None
            rev2_vm = (_f(rev2_in) or 0.0) * 1_000_000.0 if rev2_in not in (None, "") else None
            if eps1_v is not None:
                new_row["EPS 1Y"] = eps1_v
            if eps2_v is not None:
                new_row["EPS 2Y"] = eps2_v
            if rev1_vm is not None:
                new_row["Rev 1Y"] = rev1_vm
            if rev2_vm is not None:
                new_row["Rev 2Y"] = rev2_vm
            new_row["Senast manuellt uppdaterad"] = now_stamp()

            if do_prefill:
                try:
                    y = fetch_from_yahoo(tkr)

                    existing_name = _safe_str_val(new_row.get("Bolagsnamn"))
                    if existing_name:
                        name = existing_name
                    else:
                        name = (
                            y.get("name")
                            or y.get("longName")
                            or y.get("shortName")
                        )

                    existing_sector = _safe_str_val(new_row.get("Sektor"))
                    if existing_sector:
                        sector_y = existing_sector
                    else:
                        sector_y = (
                            y.get("sector")
                            or y.get("industry")
                        )

                    pre = {
                        "Bolagsnamn": name,
                        "Sektor": sector_y,
                        "Aktuell kurs": _f(y.get("price")),
                        "Valuta": y.get("currency") or valuta,
                        "Utestående aktier": _f(y.get("shares_out")),
                        "Net debt": _f(y.get("net_debt")),
                        "Rev TTM": _f(y.get("rev_ttm")),
                        "EBITDA TTM": _f(y.get("ebitda_ttm")),
                        "EPS TTM": _f(y.get("eps_ttm")),
                        "PE TTM": _f(y.get("pe_ttm")),
                        "PE FWD": _f(y.get("pe_fwd")),
                        "EV/Revenue": _f(y.get("ev_rev")),
                        "EV/EBITDA": _f(y.get("ev_ebitda")),
                        "P/B": _f(y.get("p_b")),
                        "BVPS": _f(y.get("bvps")),
                        "Rev CAGR": _f(y.get("rev_cagr_hist")),
                        "EPS CAGR": _f(y.get("eps_cagr_hist")),
                        "Årlig utdelning": _f(y.get("dps_annual")),
                        "Senast auto uppdaterad": now_stamp(),
                        "Auto källa": "Yahoo",
                    }
                    new_row.update({k: v for k, v in pre.items() if v is not None})
                except Exception:
                    pass

            out_df = pd.concat([base_df, pd.DataFrame([new_row])], ignore_index=True)
            write_data_df(out_df)
            st.session_state["DATA"] = out_df
            st.success(f"{tkr} tillagd.")
        except Exception as e:
            st.error(f"Kunde inte lägga till: {e}")


# ============================================================
# 📦 Portfölj (innehav + kommande utdelningar)
# ============================================================
def _fx_rate_to_sek(currency: str, fx_map: Dict[str, float]) -> float:
    cur = (currency or "SEK").upper().strip()
    if cur == "SEK":
        return 1.0
    r = fx_map.get(cur)
    try:
        return float(r) if r is not None and math.isfinite(float(r)) and float(r) > 0 else 1.0
    except Exception:
        return 1.0


def _position_value_tables(df_data: pd.DataFrame, fx_map: Dict[str, float]) -> pd.DataFrame:
    cols = [
        "Ticker",
        "Bolagsnamn",
        "Bucket",
        "Valuta",
        "Antal",
        "Aktuell kurs",
        "Värde (valuta)",
        "Värde (SEK)",
    ]
    rows: List[Dict[str, Any]] = []
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols)

    base = df_data.copy()

    # Hitta rätt kvantitetskolumn (backwards-kompatibelt)
    qty_col = None
    for cand in ("Antal aktier", "Antal", "Shares"):
        if cand in base.columns:
            qty_col = cand
            break

    if qty_col is None:
        return pd.DataFrame(columns=cols)

    base[qty_col] = pd.to_numeric(base[qty_col], errors="coerce")
    owned = base[base[qty_col] > 0].copy()

    for _, r in owned.iterrows():
        tkr = str(r.get("Ticker") or "").strip()
        if not tkr:
            continue
        name = str(_nz(r.get("Bolagsnamn"), ""))
        bucket = str(_nz(r.get("Bucket"), "") or "")
        ccy = str(_nz(r.get("Valuta"), "SEK")).upper()

        price = _f(r.get("Aktuell kurs"))
        qty = _pos(r.get(qty_col)) or 0.0
        fx = _fx_rate_to_sek(ccy, fx_map)
        val_ccy = (price or 0.0) * qty
        val_sek = val_ccy * fx

        rows.append({
            "Ticker": tkr,
            "Bolagsnamn": name,
            "Bucket": bucket,
            "Valuta": ccy,
            "Antal": float(qty),
            "Aktuell kurs": _f(price),
            "Värde (valuta)": float(val_ccy),
            "Värde (SEK)": float(val_sek),
        })
    out = pd.DataFrame(rows, columns=cols)
    return out


def _guess_frequency(freq_raw: Any) -> Optional[int]:
    if freq_raw is None:
        return None
    try:
        n = int(freq_raw)
        return n if n in (1, 2, 4, 12) else None
    except Exception:
        pass
    s = str(freq_raw).strip().lower()
    if s in ("m", "monthly", "månad", "månatlig"):
        return 12
    if s in ("q", "quarterly", "kvartal", "kvartalsvis"):
        return 4
    if s in ("s", "semi", "semi-annual", "halvår", "halvårsvis"):
        return 2
    if s in ("a", "annual", "år", "årligen"):
        return 1
    return None


def _parse_date_any(x) -> Optional[dt.date]:
    if x is None or (isinstance(x, float) and (pd.isna(x) or math.isnan(x))):
        return None
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    if isinstance(x, dt.datetime):
        return x.date()
    try:
        d = pd.to_datetime(x, errors="coerce", utc=False)
        if pd.isna(d):
            return None
        if isinstance(d, pd.Timestamp):
            return d.date()
        return dt.datetime.fromtimestamp(d.astype("datetime64[s]").astype(int)).date()
    except Exception:
        pass
    try:
        s = str(x).strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return dt.datetime.strptime(s, fmt).date()
            except Exception:
                continue
    except Exception:
        return None
    return None


def _pick_next_pay_date(row: pd.Series) -> Optional[dt.date]:
    candidates = [
        "Nästa utdelningsdatum",
        "Utdelningsdatum nästa",
        "Next dividend date",
        "Next Pay Date",
        "Dividend Pay Date",
        "Pay Date",
        "Payment Date",
    ]
    for c in candidates:
        if c in row and (row[c] is not None) and not (isinstance(row[c], float) and pd.isna(row[c])):
            d = _parse_date_any(row[c])
            if d is not None:
                return d
    return None


def _next_dps_per_share(row: pd.Series) -> Optional[float]:
    for c in (
        "Nästa utdelning (per aktie)",
        "Utdelning nästa",
        "Next Dividend",
        "Next DPS",
        "Dividend Next",
    ):
        if c in row and _f(row[c]) is not None:
            return float(_f(row[c]))

    annual = None
    for c in ("Årlig utdelning", "Dividend (Annual)", "DPS Annual", "Årsutdelning"):
        if c in row and _f(row[c]) is not None:
            annual = float(_f(row[c]))
            break

    if annual is None:
        return None

    freq = None
    for c in ("Utdelningsfrekvens", "Frekvens", "Frequency", "Dividend Frequency"):
        if c in row and row[c] is not None:
            freq = _guess_frequency(row[c])
            if freq:
                break
    if not freq:
        freq = 4
    try:
        return annual / float(freq) if float(freq) > 0 else None
    except Exception:
        return None


def build_next_dividends_table(
    data_df: pd.DataFrame,
    fx_map: Dict[str, float],
    settings: Dict[str, Any],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    today = dt.date.today()
    if data_df is None or data_df.empty:
        return pd.DataFrame(
            columns=[
                "Datum",
                "Ticker",
                "Valuta",
                "Antal",
                "DPS nästa",
                "Brutto",
                "Källskatt",
                "Netto",
                "Netto SEK",
            ]
        )

    for _, r in data_df.iterrows():
        ticker = str(r.get("Ticker") or "").strip()
        if not ticker:
            continue

        shares = _pos(_nz(r.get("Antal aktier"), r.get("Shares")))
        if shares is None or shares <= 0:
            continue

        currency = str(_nz(r.get("Valuta"), "SEK")).upper()
        pay_date = _pick_next_pay_date(r)
        if pay_date is None or pay_date < today:
            continue

        dps_next = _next_dps_per_share(r)
        if dps_next is None or dps_next <= 0:
            continue

        code = (currency or "USD").upper()
        key = f"withholding_{code}"
        try:
            wht = float(settings.get(key, "0.15"))
        except Exception:
            wht = 0.15

        fx = _fx_rate_to_sek(currency, fx_map)

        brutto = dps_next * shares
        kalls = brutto * wht
        netto = brutto - kalls
        netto_sek = netto * fx

        rows.append({
            "Datum": pay_date,
            "Ticker": ticker,
            "Valuta": currency,
            "Antal": float(shares),
            "DPS nästa": float(dps_next),
            "Brutto": float(brutto),
            "Källskatt": float(kalls),
            "Netto": float(netto),
            "Netto SEK": float(netto_sek),
        })

    df = pd.DataFrame(
        rows,
        columns=[
            "Datum",
            "Ticker",
            "Valuta",
            "Antal",
            "DPS nästa",
            "Brutto",
            "Källskatt",
            "Netto",
            "Netto SEK",
        ],
    )
    if df.empty:
        return df
    df = df.sort_values(["Datum", "Ticker"]).reset_index(drop=True)
    return df


def render_portfolio_dividends_section(
    data_df: pd.DataFrame,
    fx_map: Dict[str, float],
    settings: Dict[str, Any],
) -> None:
    st.subheader("📅 Kommande utdelningar (nästa utbetalningsdatum)")
    nxt = build_next_dividends_table(data_df, fx_map, settings)

    if nxt.empty:
        st.info("Inga kommande utdelningsdatum hittades i databasen (eller alla har passerat).")
        st.caption(
            "Tips: fyll i 'Nästa utdelningsdatum' och 'Nästa utdelning (per aktie)' i Data-bladet, "
            "eller säkerställ 'Årlig utdelning' + frekvens."
        )
        return

    tot_netto_sek = float(nxt["Netto SEK"].sum())
    st.metric(
        "Summa netto kommande (SEK)",
        _fmt_sek(tot_netto_sek),
    )

    df_show = nxt.copy()
    df_show["Datum"] = df_show["Datum"].astype(str)
    _show_df(df_show, height=300, use_container_width=True)

    with st.expander("Visa summering per månad (SEK, netto)"):
        try:
            g = nxt.copy()
            g["YYYY-MM"] = g["Datum"].astype(str).str.slice(0, 7)
            agg = g.groupby("YYYY-MM", as_index=False)["Netto SEK"].sum().sort_values("YYYY-MM")
            agg["Netto SEK"] = agg["Netto SEK"].map(_fmt_sek)
            _show_df(agg, height=240, use_container_width=True)
        except Exception:
            st.caption("Kunde inte göra månadssummering (saknade datum eller värden).")


# ============================================================
# 📆 Rullande 12 mån utdelningskalender (estimat + bekräftade)
# ============================================================
def _annual_dps(row: pd.Series) -> Optional[float]:
    """
    Försöker läsa årlig utdelning (per aktie) från vanliga kolumnnamn.
    """
    for c in ("Årlig utdelning", "Dividend (Annual)", "DPS Annual", "Årsutdelning"):
        if c in row and _f(row.get(c)) is not None:
            v = _f(row.get(c))
            if v is not None and v > 0:
                return float(v)
    return None


def build_dividend_rolling_12m(
    data_df: pd.DataFrame,
    fx_map: Dict[str, float],
    settings: Dict[str, Any],
) -> pd.DataFrame:
    """
    Bygger en lista av utdelningshändelser (bekräftade + estimerade)
    för de kommande ~12 månaderna.
    """
    events: List[Dict[str, Any]] = []
    if data_df is None or data_df.empty:
        return pd.DataFrame(
            columns=[
                "Datum",
                "Ticker",
                "Bolag",
                "Valuta",
                "Antal",
                "DPS",
                "Brutto",
                "Källskatt",
                "Netto",
                "Netto SEK",
                "Status",
            ]
        )

    today = dt.date.today()
    horizon = today + dt.timedelta(days=365)

    # Första passet: samla bekräftade kommande utdelningar
    confirmed_by_ticker: Dict[str, List[Dict[str, Any]]] = {}

    for _, r in data_df.iterrows():
        ticker = str(r.get("Ticker") or "").strip()
        if not ticker:
            continue

        shares = _pos(_nz(r.get("Antal aktier"), r.get("Shares")))
        if shares is None or shares <= 0:
            continue

        bolag = str(_nz(r.get("Bolagsnamn"), ""))
        currency = str(_nz(r.get("Valuta"), "SEK")).upper()

        pay_date = _pick_next_pay_date(r)
        if pay_date is None or pay_date < today or pay_date > horizon:
            continue

        dps_next = _next_dps_per_share(r)
        if dps_next is None or dps_next <= 0:
            continue

        code = (currency or "USD").upper()
        key = f"withholding_{code}"
        try:
            wht = float(settings.get(key, "0.15"))
        except Exception:
            wht = 0.15

        fx = _fx_rate_to_sek(currency, fx_map)

        brutto = float(dps_next * shares)
        kalls = float(brutto * wht)
        netto = float(brutto - kalls)
        netto_sek = float(netto * fx)

        ev = {
            "Datum": pay_date,
            "Ticker": ticker,
            "Bolag": bolag,
            "Valuta": currency,
            "Antal": float(shares),
            "DPS": float(dps_next),
            "Brutto": brutto,
            "Källskatt": kalls,
            "Netto": netto,
            "Netto SEK": netto_sek,
            "Status": "Bekräftad",
        }
        events.append(ev)
        confirmed_by_ticker.setdefault(ticker, []).append(ev)

    # Andra passet: estimera resterande utdelningar utifrån årlig utdelning + frekvens
    for _, r in data_df.iterrows():
        ticker = str(r.get("Ticker") or "").strip()
        if not ticker:
            continue

        shares = _pos(_nz(r.get("Antal aktier"), r.get("Shares")))
        if shares is None or shares <= 0:
            continue

        bolag = str(_nz(r.get("Bolagsnamn"), ""))
        currency = str(_nz(r.get("Valuta"), "SEK")).upper()

        annual_dps = _annual_dps(r)
        if annual_dps is None or annual_dps <= 0:
            # Ingen årlig utdelning – behåll ev. bekräftade events men estimera inget extra
            continue

        # Frekvens
        freq = None
        for c in ("Utdelningsfrekvens", "Frekvens", "Frequency", "Dividend Frequency"):
            if c in r and r[c] is not None:
                freq = _guess_frequency(r[c])
                if freq:
                    break
        if not freq:
            # Simple default: anta kvartalsvis om inget annat anges
            freq = 4

        if freq <= 0:
            continue

        code = (currency or "USD").upper()
        key = f"withholding_{code}"
        try:
            wht = float(settings.get(key, "0.15"))
        except Exception:
            wht = 0.15

        fx = _fx_rate_to_sek(currency, fx_map)

        annual_brutto = float(annual_dps * (shares or 0.0))
        annual_netto = float(annual_brutto * (1.0 - wht))
        annual_netto_sek = float(annual_netto * fx)

        # Summera redan bekräftade brutto inom perioden
        confirmed_list = confirmed_by_ticker.get(ticker, [])
        confirmed_brutto = sum(ev["Brutto"] for ev in confirmed_list)

        remaining_brutto = max(annual_brutto - confirmed_brutto, 0.0)

        # Om allt redan "förbrukat" via bekräftade events → ingen extra estimerad
        if remaining_brutto <= 0.0:
            continue

        # Antal kvarvarande "slots" detta år
        remaining_slots = max(freq - len(confirmed_list), 0)
        if remaining_slots == 0:
            # Inga "platser" kvar – vi låter bekräftade event representera hela årets utdelning
            continue

        # Generera ungefärliga datum jämnt fördelade under året
        if confirmed_list:
            # Starta runt tidigaste bekräftade datum om det finns
            base_date = min(ev["Datum"] for ev in confirmed_list)
            if base_date < today:
                base_date = today
        else:
            base_date = today

        existing_dates = [ev["Datum"] for ev in confirmed_list]
        interval_days = int(round(365.0 / float(freq)))
        est_dates: List[dt.date] = []

        # Bygg datum framåt inom horisonten, undvik krock nära bekräftade datum
        d0 = base_date
        while d0 < today:
            d0 = d0 + dt.timedelta(days=interval_days)

        for k in range(freq * 2):  # lite slack
            d = d0 + dt.timedelta(days=interval_days * k)
            if d > horizon:
                break
            # hoppa om vi ligger väldigt nära en bekräftad utdelning
            if any(abs((d - ed).days) <= 10 for ed in existing_dates):
                continue
            est_dates.append(d)
            if len(est_dates) >= remaining_slots:
                break

        if not est_dates:
            continue

        brutto_per_event = remaining_brutto / float(len(est_dates))
        dps_est_per_event = brutto_per_event / float(shares or 1.0)
        netto_per_event = brutto_per_event * (1.0 - wht)
        netto_sek_per_event = netto_per_event * fx

        for d in est_dates:
            ev = {
                "Datum": d,
                "Ticker": ticker,
                "Bolag": bolag,
                "Valuta": currency,
                "Antal": float(shares),
                "DPS": float(dps_est_per_event),
                "Brutto": float(brutto_per_event),
                "Källskatt": float(brutto_per_event * wht),
                "Netto": float(netto_per_event),
                "Netto SEK": float(netto_sek_per_event),
                "Status": "Estimerad",
            }
            events.append(ev)

    if not events:
        return pd.DataFrame(
            columns=[
                "Datum",
                "Ticker",
                "Bolag",
                "Valuta",
                "Antal",
                "DPS",
                "Brutto",
                "Källskatt",
                "Netto",
                "Netto SEK",
                "Status",
            ]
        )

    df = pd.DataFrame(events)
    # Filtrera bara kommande 12 månader (säkerhetsbälte)
    df = df[df["Datum"].between(today, horizon)]
    df = df.sort_values(["Datum", "Ticker"]).reset_index(drop=True)
    return df


def render_dividend_rolling_12m_section(
    data_df: pd.DataFrame,
    fx_map: Dict[str, float],
    settings: Dict[str, Any],
) -> None:
    st.subheader("📆 Utdelningskalender – rullande 12 månader (netto, SEK)")
    df_events = build_dividend_rolling_12m(data_df, fx_map, settings)

    if df_events.empty:
        st.info("Ingen utdelningsdata att estimera kommande 12 månader från.")
        st.caption(
            "Tips: säkerställ att du har 'Årlig utdelning' och ev. 'Utdelningsfrekvens' ifyllda, "
            "samt antal aktier för respektive innehav."
        )
        return

    df_events = df_events.copy()
    df_events["Månad"] = df_events["Datum"].astype(str).str.slice(0, 7)

    agg = (
        df_events.groupby("Månad", as_index=False)["Netto SEK"]
        .sum()
        .sort_values("Månad")
        .reset_index(drop=True)
    )
    agg["Netto SEK"] = agg["Netto SEK"].map(_fmt_sek)

    st.markdown("**Summering per månad (netto, SEK)**")
    _show_df(agg, height=260, use_container_width=True)

    with st.expander("Visa detaljer per månad (bolag + status)"):
        months = agg["Månad"].tolist()
        for m in months:
            sub = df_events[df_events["Månad"] == m].copy()
            if sub.empty:
                continue
            st.markdown(f"**{m}**")
            sub = sub.sort_values(["Datum", "Ticker"])
            sub["Datum"] = sub["Datum"].astype(str)
            for c in ("Brutto", "Källskatt", "Netto", "Netto SEK"):
                if c in sub.columns:
                    sub[c] = sub[c].map(_fmt_sek)
            _show_df(
                sub[
                    [
                        "Datum",
                        "Ticker",
                        "Bolag",
                        "Valuta",
                        "Antal",
                        "DPS",
                        "Brutto",
                        "Källskatt",
                        "Netto",
                        "Netto SEK",
                        "Status",
                    ]
                ],
                height=260,
                use_container_width=True,
            )


def render_bucket_expandables(pos_df: pd.DataFrame, settings: Dict[str, str]) -> None:
    if pos_df is None or pos_df.empty:
        return
    buckets = [b for b in sorted(pos_df["Bucket"].dropna().unique().tolist()) if b]
    for b in buckets:
        sub = pos_df[pos_df["Bucket"] == b].copy().sort_values("Värde (SEK)", ascending=True)
        total = float(sub["Värde (SEK)"].sum()) if not sub.empty else 0.0
        with st.expander(f"{b} — värde {total:,.0f} SEK".replace(",", " "), expanded=False):
            show = sub[
                [
                    "Ticker",
                    "Bolagsnamn",
                    "Valuta",
                    "Antal",
                    "Aktuell kurs",
                    "Värde (valuta)",
                    "Värde (SEK)",
                ]
            ].copy()
            show["Andel i bucket (%)"] = show["Värde (SEK)"].map(
                lambda x: (x / total * 100.0) if total > 0 else np.nan
            )
            _show_df(show, height=260, use_container_width=True)


def page_portfolio() -> None:
    st.header("📦 Portfölj")
    df: pd.DataFrame | None = st.session_state.get("DATA")
    if df is None or df.empty:
        st.warning("Ingen data laddad.")
        return

    # Använd live-FX från session om satt, annars läs från Valutakurser-bladet
    fx_map = st.session_state.get("FX", {}) or get_fx_map()
    settings = get_settings_map()

    pos = _position_value_tables(df, fx_map)
    if pos.empty:
        st.info("Inga innehav (Antal aktier <= 0).")
    else:
        tot_sek = float(pos["Värde (SEK)"].sum())
        st.metric(
            "Totalt portföljvärde (SEK)",
            f"{tot_sek:,.0f}".replace(",", " "),
        )
        st.caption(f"{len(fx_map)} valutakurser i FX-kartan i denna session.")
        _show_df(pos.sort_values(["Bucket", "Värde (SEK)"]), height=320, use_container_width=True)
        st.markdown("#### Hinkar (Bucket) – innehåll")
        render_bucket_expandables(pos, settings)

    st.markdown("---")
    render_portfolio_dividends_section(df, fx_map, settings)

    st.markdown("---")
    render_dividend_rolling_12m_section(df, fx_map, settings)


# ============================================================
# 🧩 Massuppdatering (Yahoo) — 1s per bolag
#    med urval: alla / endast innehav / valda Buckets
# ============================================================
def page_batch() -> None:
    st.header("🧩 Massuppdatering (Yahoo) — 1s per bolag")
    df = read_data_df()
    if df.empty:
        st.info("Data-bladet är tomt.")
        return

    base = df.copy()
    if "Antal aktier" in base.columns:
        base["Antal aktier"] = pd.to_numeric(base["Antal aktier"], errors="coerce")

    # Urval: alla / endast innehav / valda buckets
    st.markdown("### Urval att uppdatera")
    mode = st.radio(
        "Välj vilka rader som ska ingå i massuppdateringen:",
        ["Alla bolag", "Endast innehav", "Välj Buckets"],
        index=0,
        horizontal=True,
    )

    subset = base
    selected_buckets: List[str] = []

    if mode == "Endast innehav":
        if "Antal aktier" not in base.columns:
            st.warning("Kolumnen 'Antal aktier' saknas – kan inte filtrera på innehav.")
        else:
            subset = base[base["Antal aktier"] > 0].copy()
    elif mode == "Välj Buckets":
        all_buckets = (
            base.get("Bucket", pd.Series([], dtype=object))
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        all_buckets = sorted([b for b in all_buckets if b])
        selected_buckets = st.multiselect(
            "Välj en eller flera Buckets",
            options=all_buckets,
            default=all_buckets,
        )
        if not selected_buckets:
            st.info("Välj minst en Bucket för att fortsätta.")
            return
        subset = base[base["Bucket"].astype(str).str.strip().isin(selected_buckets)].copy()

    if subset.empty:
        st.info("Inga rader matchar urvalet.")
        return

    # Nuvarande universum av tickers inom urvalet
    tickers_universe = (
        subset["Ticker"]
        .dropna()
        .astype(str)
        .str.upper()
        .str.strip()
        .unique()
        .tolist()
    )
    tickers_universe = sorted(tickers_universe)

    st.markdown("### Finjustera (valfritt)")
    sel_tickers = st.multiselect(
        "Begränsa till specifika tickers (tom = alla i urvalet)",
        options=tickers_universe,
        default=tickers_universe,
    )
    target = sel_tickers if sel_tickers else tickers_universe

    if not target:
        st.info("Inga tickers valda.")
        return

    delay = st.slider("Fördröjning per bolag (sek)", 0.5, 5.0, 1.0, 0.5)
    go = st.button("🚀 Starta massuppdatering")

    if not go:
        return

    df_cur = df.copy()
    progress = st.progress(0.0)
    status = st.empty()
    changed_total = 0

    for i, tkr in enumerate(target, start=1):
        try:
            status.write(f"Uppdaterar {i}/{len(target)} – {tkr}")
            mask = df_cur["Ticker"].astype(str).str.upper() == str(tkr).upper()
            existing = df_cur[mask].iloc[0] if mask.any() else pd.Series({"Ticker": tkr})
            updates = _build_updates_from_yahoo(tkr, existing)

            if mask.any():
                idx = df_cur.index[mask][0]
                for k, v in updates.items():
                    if k not in df_cur.columns:
                        df_cur[k] = np.nan
                    old = df_cur.at[idx, k]
                    same = (pd.isna(old) and pd.isna(v)) or (
                        not pd.isna(old) and not pd.isna(v) and str(old) == str(v)
                    )
                    if same:
                        continue
                    df_cur.at[idx, k] = v
                    changed_total += 1
            else:
                base_row = {c: np.nan for c in DATA_COLUMNS}
                base_row.update({"Timestamp": now_stamp(), "Ticker": tkr})
                base_row.update(updates)
                df_cur = pd.concat([df_cur, pd.DataFrame([base_row])], ignore_index=True)
                changed_total += len(updates)
        except Exception as e:
            st.error(f"{tkr}: {e}")
        progress.progress(i / len(target))
        time.sleep(float(delay))

    write_data_df(df_cur)
    st.session_state["DATA"] = df_cur
    progress.empty()
    status.empty()
    st.success(
        f"Klar. {len(target)} tickers uppdaterade inom urvalet "
        f"({mode.lower()}{' / buckets: ' + ', '.join(selected_buckets) if selected_buckets else ''}). "
        f"{changed_total} fält ändrades."
    )

# ============================================================
# 🛒 Köpförslag + Säljförslag
# ============================================================
def _normalize_key(s: str) -> str:
    return (
        (s or "")
        .lower()
        .replace("ä", "a")
        .replace("ö", "o")
        .replace("å", "a")
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )


def _cap_for_bucket(bucket_label: str, settings: Dict[str, str]) -> Optional[float]:
    """
    Försöker hitta cap per Bucket med flera olika namnvarianter.
    Om ingen nyckel hittas → behandlas som oändlig cap (ingen begränsning).
    """
    if not bucket_label:
        return float("inf")

    s = (bucket_label or "").lower()

    # Lista ut bokstav + typ (tillväxt/utdelning)
    letter = None
    if "bucket a" in s:
        letter = "a"
    elif "bucket b" in s:
        letter = "b"
    elif "bucket c" in s:
        letter = "c"

    kind = None
    if "tillv" in s or "growth" in s:
        kind = "tillvaxt"
    elif "utdel" in s or "div" in s:
        kind = "utdelning"

    if letter is None or kind is None:
        return float("inf")

    canonical = f"bucket_cap_{letter}_{kind}"

    for k, v in settings.items():
        if _normalize_key(k) == _normalize_key(canonical):
            vv = _f(v)
            if vv is not None:
                return float(vv)

    target1 = f"bucket{letter}"
    target2 = kind
    for k, v in settings.items():
        nk = _normalize_key(k)
        if target1 in nk and target2 in nk:
            vv = _f(v)
            if vv is not None:
                return float(vv)

    return float("inf")


# ============================================================
# 🧮 Helpers: Bucket-sida (Tillväxt/Utdelning) + B/C-mål
# ============================================================
def _side_for_bucket(bucket_label: str) -> Optional[str]:
    """
    Returnerar 'Tillväxt' eller 'Utdelning' baserat på Bucket-namnet.
    Används för att gruppera hinkar per sida.
    """
    if not bucket_label:
        return None
    s = str(bucket_label).lower()
    if "tillv" in s or "growth" in s:
        return "Tillväxt"
    if "utdel" in s or "div" in s:
        return "Utdelning"
    return None


def _bucket_letter(bucket_label: str) -> Optional[str]:
    """
    Returnerar 'A', 'B' eller 'C' baserat på Bucket-namnet.
    Förväntar sig t.ex. 'Bucket A tillväxt', 'Bucket B utdelning', etc.
    """
    if not bucket_label:
        return None
    s = str(bucket_label).lower()
    if "bucket a" in s:
        return "A"
    if "bucket b" in s:
        return "B"
    if "bucket c" in s:
        return "C"
    return None


def _lookup_pct_setting(settings: Dict[str, str], canonical_key: str, fallback: float) -> float:
    """
    Hämtar en procentsats (t.ex. 40.0) från Settings med ett "kanoniskt" namn,
    men matchar mot olika stavningar/format via _normalize_key.
    """
    target_norm = _normalize_key(canonical_key)
    for k, v in settings.items():
        if _normalize_key(k) == target_norm:
            vv = _f(v)
            if vv is not None and vv >= 0:
                return float(vv)
    return float(fallback)


def _bucket_ratio_targets_for_side(side: str, settings: Dict[str, str]) -> tuple[float, float]:
    """
    Läser önskade B/C-nivåer (i % av Bucket A) från Settings.

    Nycklar som stöds:
      Tillväxt:
        - bucket_b_tillvaxt_pct_of_a
        - bucket_c_tillvaxt_pct_of_a

      Utdelning:
        - bucket_b_utdelning_pct_of_a
        - bucket_c_utdelning_pct_of_a

    Om nycklar saknas → default 40 % (B) och 20 % (C).
    """
    if side == "Tillväxt":
        b_key = "bucket_b_tillvaxt_pct_of_a"
        c_key = "bucket_c_tillvaxt_pct_of_a"
    else:
        b_key = "bucket_b_utdelning_pct_of_a"
        c_key = "bucket_c_utdelning_pct_of_a"

    b_target = _lookup_pct_setting(settings, b_key, 40.0)
    c_target = _lookup_pct_setting(settings, c_key, 20.0)
    return b_target, c_target


def _compute_bucket_side_summary(
    pos_df: pd.DataFrame,
    settings: Dict[str, str],
    side: str,
) -> Dict[str, Any]:
    """
    Summerar A/B/C-värden inom vald sida (Tillväxt/Utdelning) och
    beräknar nuvarande B/A- och C/A-nivåer i procent.
    """
    summary: Dict[str, Any] = {
        "side": side,
        "total_side_value": 0.0,
        "A_value": 0.0,
        "B_value": 0.0,
        "C_value": 0.0,
        "A_count": 0,
        "B_count": 0,
        "C_count": 0,
        "B_target_pct": None,
        "C_target_pct": None,
        "B_now_pct": None,
        "C_now_pct": None,
    }

    b_target, c_target = _bucket_ratio_targets_for_side(side, settings)
    summary["B_target_pct"] = b_target
    summary["C_target_pct"] = c_target

    if pos_df is None or pos_df.empty:
        return summary

    letter_totals = {"A": 0.0, "B": 0.0, "C": 0.0}
    letter_counts = {"A": 0, "B": 0, "C": 0}

    for _, r in pos_df.iterrows():
        bucket = str(_nz(r.get("Bucket"), "") or "")
        if not bucket:
            continue
        if _side_for_bucket(bucket) != side:
            continue
        letter = _bucket_letter(bucket)
        if letter not in letter_totals:
            continue

        val = _f(r.get("Värde (SEK)")) or 0.0
        qty = _f(r.get("Antal")) or 0.0
        if val <= 0 and qty <= 0:
            continue

        letter_totals[letter] += float(val)
        letter_counts[letter] += 1

    summary["A_value"] = letter_totals["A"]
    summary["B_value"] = letter_totals["B"]
    summary["C_value"] = letter_totals["C"]
    summary["A_count"] = letter_counts[letter]  # noqa: F821 (ignored in this snippet)
    summary["B_count"] = letter_counts["B"]
    summary["C_count"] = letter_counts["C"]
    summary["total_side_value"] = sum(letter_totals.values())

    if letter_totals["A"] > 0:
        summary["B_now_pct"] = letter_totals["B"] / letter_totals["A"] * 100.0
        summary["C_now_pct"] = letter_totals["C"] / letter_totals["A"] * 100.0

    return summary


def _compute_recommended_buy_for_side(
    df_data: pd.DataFrame,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
    side: str,
    new_capital_sek: float,
) -> Optional[Dict[str, Any]]:
    """
    Bucket-köpalgoritm:
    (oförändrad, bara anropar build_buy_suggestions utan progressbar)
    """
    if df_data is None or df_data.empty:
        return None
    if new_capital_sek is None or new_capital_sek <= 0:
        return None

    pos_df = _position_value_tables(df_data, fx_map)
    if pos_df is None or pos_df.empty:
        return None

    side_summary = _compute_bucket_side_summary(pos_df, settings, side)
    A_val = side_summary["A_value"]
    B_val = side_summary["B_value"]
    C_val = side_summary["C_value"]
    B_target_pct = side_summary["B_target_pct"]
    C_target_pct = side_summary["C_target_pct"]
    B_now_pct = side_summary["B_now_pct"] or 0.0
    C_now_pct = side_summary["C_now_pct"] or 0.0

    # Bygg mapping: bokstav -> bucket-namn (inom vald sida)
    letter_to_buckets: Dict[str, List[str]] = {"A": [], "B": [], "C": []}
    for _, r in pos_df.iterrows():
        bucket = str(_nz(r.get("Bucket"), "") or "")
        if not bucket:
            continue
        if _side_for_bucket(bucket) != side:
            continue
        letter = _bucket_letter(bucket)
        if letter in letter_to_buckets:
            if bucket not in letter_to_buckets[letter]:
                letter_to_buckets[letter].append(bucket)

    # Steg 1–3: välj vilken bucket (A/B/C) kapitalet ska till
    if A_val <= 0:
        # Inget A ännu → börja bygga A
        target_letter = "A"
    else:
        best_letter: Optional[str] = None
        best_gap = 0.0

        # Jämför B
        if B_target_pct is not None:
            gap_b = B_target_pct - B_now_pct
            if gap_b > best_gap:
                best_gap = gap_b
                best_letter = "B"

        # Jämför C
        if C_target_pct is not None:
            gap_c = C_target_pct - C_now_pct
            if gap_c > best_gap:
                best_gap = gap_c
                best_letter = "C"

        if best_letter is None or not letter_to_buckets.get(best_letter):
            # Inget underläge i B/C eller inga buckets för dem → fyll A
            target_letter = "A"
        else:
            target_letter = best_letter

    buckets_for_letter = letter_to_buckets.get(target_letter, [])
    if not buckets_for_letter:
        return None

    # Ta första bucket-namnet för denna bokstav/sida (normalt bara ett)
    target_bucket_label = sorted(buckets_for_letter)[0]

    # Begränsa till innehav i vald bucket (bara aktier vi faktiskt äger)
    pos_bucket = pos_df[
        (pos_df["Bucket"] == target_bucket_label) & (pos_df["Antal"] > 0)
    ].copy()
    if pos_bucket.empty:
        return None

    bucket_total = float(pos_bucket["Värde (SEK)"].sum())
    n_holdings = len(pos_bucket)
    if n_holdings <= 0 or bucket_total <= 0:
        return None

    target_each = bucket_total / float(n_holdings)

    # Använd buy-suggestions för att säkerställa att vi bara plockar från "köpbara" case
    sug_all = build_buy_suggestions(
        df_data,
        settings,
        fx_map,
        own_filter="Alla",
        fv_horizon="1 år",
        bucket_filter="Alla",
        zone_filter="Alla",
    )
    if sug_all is None or sug_all.empty:
        return None

    sug_bucket = sug_all[sug_all["Bucket"] == target_bucket_label].copy()
    if sug_bucket.empty:
        # Inga bolag i den bucketen är under FV / har slack till cap
        return None

    # Slå ihop pos_bucket (värde i SEK) med buy-suggestions (pris, slack, m.m.)
    # Nyckel: Ticker
    cand_rows: List[Dict[str, Any]] = []
    for _, r_pos in pos_bucket.iterrows():
        tkr = str(r_pos.get("Ticker") or "").upper().strip()
        if not tkr:
            continue

        s_match = sug_bucket[sug_bucket["Ticker"].astype(str).str.upper() == tkr]
        if s_match.empty:
            # Den här aktien är antingen över FV eller vid cap → hoppa
            continue

        s = s_match.iloc[0]
        current_value_sek = _f(r_pos.get("Värde (SEK)")) or 0.0
        need_to_equal = max(target_each - current_value_sek, 0.0)
        if need_to_equal <= 0:
            # Redan minst på snittnivå – vi gillar hellre de som släpar efter
            continue

        cand_rows.append({
            "ticker": tkr,
            "bucket": target_bucket_label,
            "current_value_sek": current_value_sek,
            "need_to_equal_sek": need_to_equal,
            "price": _f(s.get("Kurs")),
            "currency": str(s.get("Valuta") or r_pos.get("Valuta") or "SEK").upper(),
            "slack_cap_sek": _f(s.get("Slack till cap (SEK)")),
        })

    if not cand_rows:
        return None

    # Välj den aktie som ligger mest under "lika stort"-nivån
    cand_df = pd.DataFrame(cand_rows)
    cand_df = cand_df.sort_values("need_to_equal_sek", ascending=False).reset_index(drop=True)
    best = cand_df.iloc[0]

    price = _f(best.get("price"))
    ccy = str(best.get("currency") or "SEK").upper()
    if price is None or price <= 0:
        return None

    fx = _fx_rate_to_sek(ccy, fx_map)
    price_sek = price * fx
    if price_sek <= 0:
        return None

    slack_cap_sek = best.get("slack_cap_sek")
    if slack_cap_sek is None or (isinstance(slack_cap_sek, float) and math.isnan(slack_cap_sek)):
        slack_cap_sek = float("inf")

    # Hur många aktier kan vi köpa utan att:
    #  - gå över cap
    #  - gå över "lika stort"-nivån
    #  - överskrida tillgängligt kapital
    max_by_cash = math.floor(new_capital_sek / price_sek) if new_capital_sek > 0 else 0
    max_by_equal = math.floor(best["need_to_equal_sek"] / price_sek) if best["need_to_equal_sek"] > 0 else 0
    max_by_cap = math.floor(slack_cap_sek / price_sek) if slack_cap_sek > 0 and math.isfinite(slack_cap_sek) else max_by_cash

    candidates = [x for x in (max_by_cash, max_by_equal, max_by_cap) if x is not None and x > 0]
    if not candidates:
        return None

    shares_to_buy = int(min(candidates))
    if shares_to_buy <= 0:
        return None

    invest_sek = shares_to_buy * price_sek

    return {
        "side": side,
        "bucket": best["bucket"],
        "ticker": best["ticker"],
        "price": price,
        "ccy": ccy,
        "fx": fx,
        "current_value_sek": float(best["current_value_sek"]),
        "target_value_each_sek": float(target_each),
        "need_to_equal_sek": float(best["need_to_equal_sek"]),
        "shares_to_buy": shares_to_buy,
        "invest_sek": float(invest_sek),
    }


def _quick_pos_lookup(df: pd.DataFrame, fx_map: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    pos = _position_value_tables(df, fx_map)
    for _, r in pos.iterrows():
        out[str(r["Ticker"]).upper()] = {
            "value_sek": _f(r["Värde (SEK)"]) or 0.0,
            "qty": _f(r["Antal"]) or 0.0,
            "currency": str(r.get("Valuta") or "SEK").upper(),
            "price": _f(r.get("Aktuell kurs")),
        }
    return out


# CHANGED: lagt till progress_obj och progress_status
def build_buy_suggestions(
    df_data: pd.DataFrame,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
    own_filter: str = "Alla",
    fv_horizon: str = "Idag",
    bucket_filter: str = "Alla",
    zone_filter: str = "Alla",
    progress_obj=None,
    progress_status=None,
) -> pd.DataFrame:
    cols_out = [
        "Ticker",
        "Bolagsnamn",
        "Bucket",
        "Valuta",
        "Kurs",
        "FV idag",
        "FV 1 år",
        "FV 2 år",
        "FV 3 år",
        "Uppsida (%)",
        "Äger (antal)",
        "Värde (SEK)",
        "Cap per innehav (SEK)",
        "Slack till cap (SEK)",
        "Bra köp-nivå",
        "Fyndläge-nivå",
        "Köpzon",
    ]
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols_out)

    base = df_data.copy()
    if "Antal aktier" in base.columns:
        base["Antal aktier"] = pd.to_numeric(base["Antal aktier"], errors="coerce")

    total_rows = len(base.index)
    if total_rows <= 0:
        return pd.DataFrame(columns=cols_out)

    # Init progress
    if progress_obj is not None:
        try:
            progress_obj.progress(0.0)
        except Exception:
            progress_obj = None
    if progress_status is not None:
        try:
            progress_status.write("Bygger köpförslag… 0 %")
        except Exception:
            progress_status = None

    lu = _quick_pos_lookup(base, fx_map)
    rows: List[Dict[str, Any]] = []

    for i, (_, r) in enumerate(base.iterrows(), start=1):
        # Uppdatera progressbar
        if progress_obj is not None:
            try:
                frac = i / total_rows
                progress_obj.progress(frac)
                if progress_status is not None:
                    pct = int(frac * 100)
                    progress_status.write(f"Bygger köpförslag… {pct} %")
            except Exception:
                pass

        try:
            tkr = str(r.get("Ticker") or "").upper().strip()
            if not tkr:
                continue
            bucket = str(_nz(r.get("Bucket"), "") or "")
            if not bucket:
                continue

            # Bucket-filter (om vi inte kör ren zon-filtrering)
            if bucket_filter and bucket_filter != "Alla" and bucket != bucket_filter:
                continue

            cap = _cap_for_bucket(bucket, settings)
            if cap is None or (math.isfinite(cap) and cap <= 0):
                continue

            payload = compute_methods_for_row(r, settings, fx_map)

            price = _f(payload.get("price"))
            if not _pos(price):
                continue

            ccy = str(_nz(payload.get("currency"), _nz(r.get("Valuta"), "SEK"))).upper()
            name = str(_nz(r.get("Bolagsnamn"), ""))

            fv_today = _f(payload.get("target_today"))
            fv_1y    = _f(payload.get("target_1y"))
            fv_2y    = _f(payload.get("target_2y"))
            fv_3y    = _f(payload.get("target_3y"))

            fv_map = {
                "Idag": fv_today,
                "1 år": fv_1y,
                "2 år": fv_2y,
                "3 år": fv_3y,
            }
            fv_active = fv_map.get(fv_horizon, fv_today)

            if not _pos(fv_active):
                continue

            if not (price < fv_active):
                continue

            entry = lu.get(
                tkr,
                {
                    "value_sek": 0.0,
                    "qty": _f(r.get("Antal aktier")) or 0.0,
                    "currency": ccy,
                    "price": price,
                },
            )
            qty = entry["qty"] if entry["qty"] is not None else (_f(r.get("Antal aktier")) or 0.0)

            own_status = "own" if (qty and qty > 0) else "no_own"
            if own_filter == "Endast innehav" and own_status != "own":
                continue
            if own_filter == "Endast ej ägda" and own_status != "no_own":
                continue

            fx = _fx_rate_to_sek(ccy, fx_map)
            value_sek = float((price or 0.0) * (qty or 0.0) * fx)

            if math.isfinite(cap) and _pos(value_sek) and value_sek >= cap:
                continue

            up_pct: Optional[float] = None
            if _pos(price) and _pos(fv_active):
                up_pct = (fv_active - price) / price * 100.0

            # Bra köp / fyndläge-nivåer + zon
            bra_level: Optional[float] = None
            fynd_level: Optional[float] = None
            zone: str = ""

            if _pos(fv_active):
                fa = float(fv_active)
                bra_level = fa * 0.80
                fynd_level = fa * 0.65

                if _pos(price):
                    pval = float(price)
                    if fynd_level is not None and pval <= fynd_level:
                        zone = "Fyndläge"
                    elif bra_level is not None and pval <= bra_level:
                        zone = "Bra köp"
                    else:
                        zone = "Under FV"

            # Zon-filter (Fyndläge / Bra köp) om valt
            if zone_filter == "Fyndläge" and zone != "Fyndläge":
                continue
            if zone_filter == "Bra köp" and zone != "Bra köp":
                continue

            rows.append({
                "Ticker": tkr,
                "Bolagsnamn": name,
                "Bucket": bucket,
                "Valuta": ccy,
                "Kurs": price,
                "FV idag": fv_today,
                "FV 1 år": fv_1y,
                "FV 2 år": fv_2y,
                "FV 3 år": fv_3y,
                "Uppsida (%)": up_pct,
                "Äger (antal)": qty or 0.0,
                "Värde (SEK)": value_sek or 0.0,
                "Cap per innehav (SEK)": cap,
                "Slack till cap (SEK)": (cap - (value_sek or 0.0)) if math.isfinite(cap) else None,
                "Bra köp-nivå": bra_level,
                "Fyndläge-nivå": fynd_level,
                "Köpzon": zone,
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=cols_out)

    out = pd.DataFrame(rows, columns=cols_out)

    for col in ("Slack till cap (SEK)", "Uppsida (%)"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    sort_cols: List[str] = []
    sort_asc: List[bool] = []
    if "Slack till cap (SEK)" in out.columns:
        sort_cols.append("Slack till cap (SEK)")
        sort_asc.append(False)
    if "Uppsida (%)" in out.columns:
        sort_cols.append("Uppsida (%)")
        sort_asc.append(False)

    if sort_cols:
        out = out.sort_values(sort_cols, ascending=sort_asc, na_position="last")

    out = out.reset_index(drop=True)
    return out


def build_sell_suggestions(
    df_data: pd.DataFrame,
    settings: Dict[str, str],
    fx_map: Dict[str, float],
    bucket_filter: str = "Alla",
) -> pd.DataFrame:
    cols_out = [
        "Ticker",
        "Bolagsnamn",
        "Bucket",
        "Valuta",
        "Antal",
        "Aktuell kurs",
        "Värde (SEK)",
        "Cap per innehav (SEK)",
        "Över cap (SEK)",
    ]
    if df_data is None or df_data.empty:
        return pd.DataFrame(columns=cols_out)

    fx_map = fx_map or get_fx_map()
    pos = _position_value_tables(df_data, fx_map)
    if pos.empty:
        return pd.DataFrame(columns=cols_out)

    rows: List[Dict[str, Any]] = []
    for _, r in pos.iterrows():
        try:
            bucket = str(_nz(r.get("Bucket"), "") or "")
            if not bucket:
                continue
            if bucket_filter and bucket_filter != "Alla" and bucket != bucket_filter:
                continue

            cap = _cap_for_bucket(bucket, settings)
            if cap is None or (math.isfinite(cap) and cap <= 0):
                continue

            value_sek = _f(r.get("Värde (SEK)")) or 0.0

            if not math.isfinite(cap):
                continue

            if value_sek <= cap:
                continue

            over_cap = value_sek - cap
            rows.append({
                "Ticker": str(r.get("Ticker") or ""),
                "Bolagsnamn": str(_nz(r.get("Bolagsnamn"), "")),
                "Bucket": bucket,
                "Valuta": str(_nz(r.get("Valuta"), "SEK")).upper(),
                "Antal": _f(r.get("Antal")) or 0.0,
                "Aktuell kurs": _f(r.get("Aktuell kurs")),
                "Värde (SEK)": value_sek,
                "Cap per innehav (SEK)": cap,
                "Över cap (SEK)": over_cap,
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=cols_out)

    out = pd.DataFrame(rows, columns=cols_out)
    out = out.sort_values("Över cap (SEK)", ascending=False).reset_index(drop=True)
    return out


def page_buy_suggestions() -> None:
    st.header("🛒 Köp-/säljförslag (läser Data-bladet)")
    df = st.session_state.get("DATA")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        df = read_data_df()
    if df is None or df.empty:
        st.info("Ingen data.")
        return

    settings = get_settings_map()
    fx_map = get_fx_map()

    # Visuell "bevis"-rad för FX-uppdatering om vi har någon timestamp lagrad
    fx_ts = (
        settings.get("FX_LAST_UPDATE_TS")
        or settings.get("FX last update")
        or settings.get("FX senast uppdaterad")
        or st.session_state.get("FX_TS")
    )
    if fx_ts:
        st.caption(f"Senaste valutauppdatering (enligt Sheets/session): {fx_ts}")

    all_buckets = sorted(
        {
            str(b)
            for b in df.get("Bucket", pd.Series([], dtype=object)).dropna().tolist()
            if str(b).strip()
        }
    )
    bucket_opts = (
        ["Alla"]
        + [b for b in DEFAULT_BUCKETS if b in all_buckets]
        + [b for b in all_buckets if b not in DEFAULT_BUCKETS]
    )

    # Rullista: Alla / Fyndläge / Bra köp + buckets
    bucket_zone_opts = ["Alla", "Fyndläge", "Bra köp"] + [b for b in bucket_opts if b != "Alla"]

    col_top1, col_top2, col_top3 = st.columns([2, 2, 2])
    with col_top1:
        fv_horizon = st.selectbox(
            "Riktkurs-horisont (för uppsida/sortering)",
            ["Idag", "1 år", "2 år", "3 år"],
            index=0,
        )
    with col_top2:
        own_filter = st.radio(
            "Innehavsfilter",
            ["Alla", "Endast innehav", "Endast ej ägda"],
            index=0,
            horizontal=True,
        )
    with col_top3:
        bucket_or_zone = st.selectbox(
            "Bucket-/zon-filter (köpförslag)",
            bucket_zone_opts,
            index=0,
        )

    # Tolka valet: antingen ren zon (Fyndläge/Bra köp) eller bucket-filter
    if bucket_or_zone in ("Fyndläge", "Bra köp"):
        zone_filter = bucket_or_zone
        bucket_filter_buy = "Alla"
    else:
        zone_filter = "Alla"
        bucket_filter_buy = bucket_or_zone

    st.caption(
        f"Köpförslag visar bolag där aktuell kurs är lägre än riktkurs för **vald horisont** "
        f"(**{fv_horizon}**) och där innehavet inte är större än maxvärdet (cap) för respektive Bucket.\n\n"
        f"Om ingen cap hittas i Settings behandlas den bucketen som **obegränsad**.\n\n"
        f"**Bucket-/zon-filter** kan användas så här:\n"
        f"- Välj en **Bucket** för att se bara den hinken\n"
        f"- Välj **Fyndläge** eller **Bra köp** för att filtrera på Köpzon över alla buckets"
    )

    # CHANGED: progressbar + % under bygger köpförslag
    progress_placeholder = st.empty()
    status_placeholder = st.empty()

    with st.spinner("Bygger köpförslag…"):
        sug = build_buy_suggestions(
            df,
            settings,
            fx_map,
            own_filter=own_filter,
            fv_horizon=fv_horizon,
            bucket_filter=bucket_filter_buy,
            zone_filter=zone_filter,
            progress_obj=progress_placeholder,
            progress_status=status_placeholder,
        )

    # Städa bort progressbar + status efteråt
    progress_placeholder.empty()
    status_placeholder.empty()

    if sug.empty:
        st.info("Inga köpkandidater uppfyller kriterierna just nu.")
        st.caption(
            "Tips: kontrollera Bucket-cap i Settings, samt att EPS/Revenue-fälten och Yahoo-data "
            "är rimligt ifyllda för bolagen."
        )
    else:
        st.caption(
            f"{len(sug)} köpförslag — sorterat på störst slack till cap och därefter uppsida "
            f"mot vald riktkurs ({fv_horizon})."
        )
        show = sug.copy()

        if "Kurs" in show.columns:
            show["Kurs"] = show["Kurs"].map(
                lambda v: "" if _f(v) is None else f"{float(v):.2f}"
            )
        for c in ("FV idag", "FV 1 år", "FV 2 år", "FV 3 år", "Bra köp-nivå", "Fyndläge-nivå"):
            if c in show.columns:
                show[c] = show[c].map(
                    lambda v: "" if _f(v) is None else f"{float(v):.2f}"
                )
        for c in ("Värde (SEK)", "Cap per innehav (SEK)", "Slack till cap (SEK)"):
            if c in show.columns:
                show[c] = show[c].map(
                    lambda v: "" if _f(v) is None else f"{float(v):.2f}"
                )
        if "Uppsida (%)" in show.columns:
            show["Uppsida (%)"] = show["Uppsida (%)"].map(
                lambda v: f"{v:.1f}%" if v is not None else "—"
            )

        _show_df(show, height=420, use_container_width=True)

        with st.expander("Summering per Bucket (antal köpförslag)"):
            agg = sug.groupby("Bucket", as_index=False).size().rename(
                columns={"size": "Antal förslag"}
            )
            _show_df(agg, height=240, use_container_width=True)

    st.markdown("---")
    st.subheader("💼 Säljförslag (över Bucket-max)")

    bucket_filter_sell = st.selectbox(
        "Bucket-filter (säljförslag)",
        bucket_opts,
        index=0,
        key="sell_bucket_filter",
    )

    with st.spinner("Bygger säljförslag…"):
        sell_df = build_sell_suggestions(
            df,
            settings,
            fx_map,
            bucket_filter=bucket_filter_sell,
        )

    if sell_df.empty:
        st.info("Inga innehav ligger över maxvärdet (cap) för vald Bucket just nu.")
    else:
        st.caption(
            f"{len(sell_df)} säljförslag — innehav där värdet överstiger Bucket-cap."
        )
        show_s = sell_df.copy()
        for c in ("Aktuell kurs", "Värde (SEK)", "Cap per innehav (SEK)", "Över cap (SEK)"):
            if c in show_s.columns:
                show_s[c] = show_s[c].map(
                    lambda v: "" if _f(v) is None else f"{float(v):.2f}"
                )
        _show_df(show_s, height=360, use_container_width=True)

    # ========================================================
    # 🤖 Bucket-köpalgoritm – nästa rekommenderade köp
    # ========================================================
    st.markdown("---")
    st.subheader("🤖 Bucket-köpalgoritm – nästa rekommenderade köp")

    side_opt = st.radio(
        "Välj sida som detta kapital gäller",
        ["Tillväxt", "Utdelning"],
        index=0,
        horizontal=True,
        key="bucket_algo_side",
    )

    pos_df = _position_value_tables(df, fx_map)
    side_summary = _compute_bucket_side_summary(pos_df, settings, side_opt)

    if side_summary["total_side_value"] <= 0:
        st.info(
            "Inga innehav hittades för vald sida. "
            "Kontrollera att Bucket-namnen innehåller t.ex. 'tillväxt' eller 'utdelning'."
        )
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Totalt värde (sida)", _fmt_sek(side_summary["total_side_value"]))
        with c2:
            st.metric("Bucket A värde", _fmt_sek(side_summary["A_value"]))

        # Bucket B / A
        with c3:
            b_now = side_summary["B_now_pct"]
            b_tgt = side_summary["B_target_pct"]
            if b_now is not None and b_tgt is not None and side_summary["A_value"] > 0:
                delta_b = b_now - b_tgt
                st.metric(
                    "Bucket B / A",
                    f"{b_now:.1f} %",
                    f"{delta_b:+.1f} %-enheter (mål {b_tgt:.1f} %)",
                )
            else:
                st.metric("Bucket B / A", "—", "")

        # Bucket C / A
        with c4:
            c_now = side_summary["C_now_pct"]
            c_tgt = side_summary["C_target_pct"]
            if c_now is not None and c_tgt is not None and side_summary["A_value"] > 0:
                delta_c = c_now - c_tgt
                st.metric(
                    "Bucket C / A",
                    f"{c_now:.1f} %",
                    f"{delta_c:+.1f} %-enheter (mål {c_tgt:.1f} %)",
                )
            else:
                st.metric("Bucket C / A", "—", "")

        st.caption(
            "Målen för Bucket B/C läses från Settings om nycklarna finns:\n"
            "- bucket_b_tillvaxt_pct_of_a / bucket_c_tillvaxt_pct_of_a\n"
            "- bucket_b_utdelning_pct_of_a / bucket_c_utdelning_pct_of_a\n"
            "Annars används defaultvärden (40 % för B, 20 % för C)."
        )

        new_cap = st.number_input(
            "Tillgängligt kapital (SEK) att placera i denna sida",
            min_value=0.0,
            step=100.0,
            value=0.0,
            key="bucket_algo_cap_sek",
        )

        if new_cap > 0 and st.button(
            "💡 Beräkna nästa köp utifrån bucket-reglerna",
            key="btn_bucket_algo",
        ):
            suggestion = _compute_recommended_buy_for_side(
                df,
                settings,
                fx_map,
                side_opt,
                new_cap,
            )
            if suggestion is None:
                st.info(
                    "Kunde inte ta fram ett konkret förslag. "
                    "Orsaker kan vara: alla kandidater över FV, fulla mot cap eller "
                    "att kapitalet inte räcker till en aktie."
                )
            else:
                tkr = suggestion["ticker"]
                buk = suggestion["bucket"]
                qty = int(suggestion["shares_to_buy"])
                invest_sek = suggestion["invest_sek"]
                cur_sek = suggestion["current_value_sek"]
                tgt_each = suggestion["target_value_each_sek"]
                price = suggestion["price"]
                ccy = suggestion["ccy"]
                fx = suggestion["fx"]

                st.markdown(
                    f"**Förslag:** köp **{qty} st {tkr}** i *{buk}*.\n\n"
                    f"- Nuvarande position: ≈ {_fmt_sek(cur_sek)} SEK\n"
                    f"- Målnivå (lika stort som övriga i bucket): ≈ {_fmt_sek(tgt_each)} SEK\n"
                    f"- Denna affär investerar ≈ {_fmt_sek(invest_sek)} SEK i {tkr}\n"
                    f"- Ny uppskattad positionsstorlek: ≈ {_fmt_sek(cur_sek + invest_sek)} SEK"
                )
                st.caption(
                    f"Beräkningen använder pris ≈ {price:.2f} {ccy} "
                    f"(FX ≈ {fx:.2f} SEK/{ccy}). "
                    f"Övrigt kapital kan du fördela manuellt eller via nästa körning av algoritmen."
                )
