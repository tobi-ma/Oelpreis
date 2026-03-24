"""
Heizöl-Dashboard — Intelligenter Preistracker
==============================================
Streamlit-App zur Unterstützung des cleveren Heizöl-Kaufs.

Kaskaden-Logik:
  Ebene 1: Brent Crude (globaler Frühindikator, ~2–4 Wochen Vorlauf)
  Ebene 2: Nationaler Heizölpreis Deutschland (Bundesdurchschnitt)
  Ebene 3: Lokale Händlerpreise nach Postleitzahl

Deployment: Streamlit Cloud (streamlit.io)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import json
import os
import csv

from dotenv import load_dotenv
load_dotenv(override=True)

from collectors.brent import get_brent_history, get_brent_current, get_brent_trend
from collectors.national import get_national_history, get_national_current, get_moving_average
from collectors.local import get_local_quotes, get_best_local_price, get_comparison_links
from collectors.llm_analysis import (
    run_llm_analysis, save_analysis, load_all_analyses,
    has_analysis_today, get_latest_analysis,
)

# ── Seitenkonfiguration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Heizöl-Dashboard Freudenberg",
    page_icon="🛢️",
    layout="wide",
)

# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_brent_history(days: int = 180) -> pd.DataFrame:
    return get_brent_history(days=days)

@st.cache_data(ttl=3600)
def load_brent_current() -> dict:
    return get_brent_current()

@st.cache_data(ttl=3600)
def load_brent_trend() -> str:
    return get_brent_trend(days=5)

@st.cache_data(ttl=3600)
def load_national_history(days: int = 365) -> pd.DataFrame:
    return get_national_history(days=days)

@st.cache_data(ttl=3600)
def load_national_current() -> dict:
    return get_national_current()

@st.cache_data(ttl=3600)
def load_local_quotes(plz: str, liters: int) -> pd.DataFrame:
    return get_local_quotes(plz=plz, liters=liters)


def fmt_price(val, unit="", decimals=2) -> str:
    if val is None:
        return "–"
    return f"{val:.{decimals}f}{unit}"


def delta_color(val) -> str:
    if val is None:
        return "off"
    return "normal" if val <= 0 else "inverse"


def buy_recommendation(brent_trend: str, national_df: pd.DataFrame, best_local: float) -> tuple[str, str, str]:
    """
    Ampel-Empfehlung.
    Returns: (signal, label, erklärung)
    signal: 'green' | 'yellow' | 'red'
    """
    if national_df.empty:
        return "yellow", "Keine Daten", "Nationaler Preis nicht verfügbar – bitte später erneut versuchen."

    ma30 = get_moving_average(national_df, window=30)
    national_current = national_df["price"].iloc[-1]
    below_ma = national_current < ma30.iloc[-1]

    if brent_trend == "steigend":
        return (
            "red",
            "Abwarten",
            "Brent Crude steigt gerade stark – die Heizölpreise werden "
            "voraussichtlich in 2–4 Wochen folgen. Noch etwas Geduld, dann kaufen.",
        )
    elif below_ma and brent_trend in ("fallend", "stabil"):
        return (
            "green",
            "Jetzt kaufen",
            "Der aktuelle Preis liegt unter dem 30-Tage-Schnitt und Brent ist "
            "stabil oder fallend. Guter Zeitpunkt zum Kaufen.",
        )
    else:
        return (
            "yellow",
            "Beobachten",
            "Der Preis liegt nahe am Durchschnitt und Brent ist stabil. "
            "Kein dringender Handlungsbedarf – täglich im Blick behalten.",
        )


def initialize_plz_history(plz: str, liters: int) -> None:
    """
    Stelle sicher, dass die PLZ-spezifische History-Datei existiert.
    Falls nicht: Erstelle sie mit leerem Header.
    (Tägliche Updates kommen von collect_daily.py)
    """
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)

    plz_history_file = os.path.join(data_dir, f"history_plz_{plz}.csv")

    # Wenn Datei existiert: Fertig
    if os.path.exists(plz_history_file):
        return

    # Sonst: Erstelle leere Datei mit Header
    try:
        fieldnames = ["date", "best_local_ct_per_liter", "top_3_dealers"]
        with open(plz_history_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    except Exception as e:
        st.warning(f"Konnte Datei für PLZ {plz} nicht erstellen: {e}")


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Einstellungen")
    plz = st.text_input(
        "Postleitzahl",
        value="57258",
        max_chars=5,
        help="PLZ für den Händlervergleich (z.B. 57258 für Freudenberg)"
    )
    liters = st.selectbox("Bestellmenge (Liter)", [1000, 1500, 2000, 3000, 4000, 5000], index=3)
    alarm_threshold = st.number_input(
        "Preisalarm bei lokalem Preis ≤ (ct/Liter)",
        min_value=50.0, max_value=200.0, value=85.0, step=0.5,
        format="%.1f",
        help="Benachrichtigung, wenn der günstigste lokale Händlerpreis unter diesem Wert liegt"
    )
    st.divider()
    st.caption("Daten werden stündlich aktualisiert.")
    st.caption(f"Stand: {datetime.now().strftime('%d.%m.%Y %H:%M')} Uhr")
    if st.button("Daten neu laden", width='stretch'):
        st.cache_data.clear()
        st.rerun()

# ── Daten laden ────────────────────────────────────────────────────────────────
PLZ = plz if plz and plz.isdigit() and len(plz) == 5 else "57258"

# Stelle sicher, dass die PLZ-Datei existiert
initialize_plz_history(PLZ, liters)

with st.spinner("Preisdaten werden geladen…"):
    brent_hist = load_brent_history(days=180)
    brent_current = load_brent_current()
    brent_trend = load_brent_trend()
    national_hist = load_national_history(days=365)
    national_current = load_national_current()
    local_quotes = load_local_quotes(plz=PLZ, liters=liters)

best_local = local_quotes["price"].min() if not local_quotes.empty else None

# ── Preisalarm ────────────────────────────────────────────────────────────────
if best_local is not None and best_local <= alarm_threshold:
    st.success(
        f"**Preisalarm!** Der günstigste lokale Preis beträgt aktuell "
        f"**{fmt_price(best_local, ' ct/L')}** – unter deiner Schwelle von "
        f"{alarm_threshold:.1f} ct/L. Jetzt ist ein guter Moment zum Kaufen!"
    )

# ── Titel ──────────────────────────────────────────────────────────────────────
st.title(f"🛢️ Heizöl-Dashboard — PLZ {PLZ}")
st.caption(
    "Drei-Ebenen-Analyse: Globaler Frühindikator → Nationaler Marktpreis → "
    "Lokale Händler. Alle Preise für Standard-Heizöl, Abnahmemenge 3.000 Liter."
)

# ── KPI-Kacheln ───────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Brent Crude (Frühindikator)",
        value=fmt_price(brent_current.get("price"), " $/Barrel"),
        delta=fmt_price(brent_current.get("change_pct"), "%") if brent_current.get("change_pct") else None,
        delta_color=delta_color(brent_current.get("change_pct")),
        help="Rohölpreis – steigt Brent, steigen Heizölpreise ca. 2–4 Wochen später.",
    )

with col2:
    nat = national_current.get("price")
    st.metric(
        label="DE Marktpreis (Bundesdurchschnitt)",
        value=fmt_price(nat, " ct/L"),
        delta=fmt_price(national_current.get("change_pct"), "%") if national_current.get("change_pct") else None,
        delta_color=delta_color(national_current.get("change_pct")),
        help="Täglicher Bundesdurchschnitt von heizoel24.de",
    )

with col3:
    st.metric(
        label=f"Bestes lokales Angebot ({PLZ})",
        value=fmt_price(best_local, " ct/L") if best_local else "–",
        help=f"Günstigster Händlerpreis für {liters:,} L in PLZ {PLZ} (heizoel24.de)",
    )

st.divider()

# ── Kaufempfehlung ─────────────────────────────────────────────────────────────
signal, label, explanation = buy_recommendation(brent_trend, national_hist, best_local)

signal_emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(signal, "⚪")
signal_fn = {"green": st.success, "yellow": st.warning, "red": st.error}.get(signal, st.info)

signal_fn(f"{signal_emoji} **{label}** — {explanation}")

# ── Statistik & Zielpreise ────────────────────────────────────────────────────
if not national_hist.empty:
    ma30 = get_moving_average(national_hist, window=30)
    nat_current = national_hist["price"].iloc[-1]
    nat_ma30 = ma30.iloc[-1]
    nat_min_90d = national_hist.tail(90)["price"].min() if len(national_hist) >= 90 else national_hist["price"].min()
    nat_max_90d = national_hist.tail(90)["price"].max() if len(national_hist) >= 90 else national_hist["price"].max()
    nat_avg_90d = national_hist.tail(90)["price"].mean() if len(national_hist) >= 90 else national_hist["price"].mean()

    # Brent-Veränderung der letzten 30 Tage → geschätzte Heizöl-Auswirkung in 2–4 Wochen
    brent_price = brent_current.get("price")
    brent_forecast_note = ""
    if not brent_hist.empty and len(brent_hist) >= 30 and brent_price:
        brent_30d_ago = brent_hist.iloc[-30]["price"]
        brent_change_pct = (brent_price - brent_30d_ago) / brent_30d_ago * 100
        # Heizöl korreliert ~0.6–0.8 mit Brent, Faustformel: 1% Brent ≈ 0.5–0.7% Heizöl
        estimated_impact = brent_change_pct * 0.6
        forecast_price = nat_current * (1 + estimated_impact / 100)
        if abs(brent_change_pct) > 1:
            direction = "steigen" if brent_change_pct > 0 else "sinken"
            brent_forecast_note = (
                f"Brent hat sich in 30 Tagen um **{brent_change_pct:+.1f}%** bewegt. "
                f"Heizölpreise könnten in 2–4 Wochen auf ca. **{forecast_price:.1f} ct/L** {direction}."
            )

    with st.container(border=True):
        st.markdown("##### Marktanalyse & Zielpreise")

        stat_cols = st.columns(4)
        with stat_cols[0]:
            diff_ma = nat_current - nat_ma30
            arrow = "▼" if diff_ma < 0 else "▲"
            color = "green" if diff_ma < 0 else "red"
            st.markdown(f"**Ø 30 Tage**")
            st.markdown(f":{color}[{arrow} {abs(diff_ma):.1f} ct/L {'drunter' if diff_ma < 0 else 'drüber'}]")
            st.caption(f"Schnitt: {nat_ma30:.1f} ct/L")

        with stat_cols[1]:
            st.markdown(f"**90-Tage-Korridor**")
            st.markdown(f"{nat_min_90d:.1f} – {nat_max_90d:.1f} ct/L")
            percentile = (nat_current - nat_min_90d) / (nat_max_90d - nat_min_90d) * 100 if nat_max_90d != nat_min_90d else 50
            st.caption(f"Aktuell bei Perzentil {percentile:.0f}%")

        with stat_cols[2]:
            st.markdown(f"**5-Tage-Trend Brent**")
            trend_icons = {"steigend": "📈 Steigend", "fallend": "📉 Fallend", "stabil": "➡️ Stabil", "unbekannt": "❓ Unbekannt"}
            trend_colors = {"steigend": "red", "fallend": "green", "stabil": "orange"}
            t_color = trend_colors.get(brent_trend, "gray")
            st.markdown(f":{t_color}[{trend_icons.get(brent_trend, brent_trend)}]")
            if brent_price:
                st.caption(f"Brent: {brent_price:.2f} $/Barrel")

        with stat_cols[3]:
            st.markdown(f"**Lokaler Vorteil**")
            if best_local and nat_current:
                local_diff = best_local - nat_current
                if local_diff < 0:
                    st.markdown(f":green[{local_diff:.1f} ct/L unter Markt]")
                else:
                    st.markdown(f":red[+{local_diff:.1f} ct/L über Markt]")
                savings = (nat_current - best_local) * liters / 100 if best_local < nat_current else 0
                if savings > 0:
                    st.caption(f"Ersparnis: {savings:.0f} € bei {liters:,} L")
                else:
                    st.caption(f"Aufpreis: {abs(local_diff) * liters / 100:.0f} € bei {liters:,} L")
            else:
                st.markdown("–")

        # Zielpreise
        st.markdown("---")
        target_cols = st.columns(3)
        with target_cols[0]:
            st.markdown("**Kurzfristig** (1–2 Wochen)")
            if brent_trend == "fallend":
                short_target = nat_current * 0.98
                st.markdown(f":green[~ {short_target:.1f} ct/L] ↓")
                st.caption("Brent fällt → Preissenkung erwartet")
            elif brent_trend == "steigend":
                short_target = nat_current * 1.02
                st.markdown(f":red[~ {short_target:.1f} ct/L] ↑")
                st.caption("Brent steigt → Preisanstieg erwartet")
            else:
                st.markdown(f"~ {nat_current:.1f} ct/L ➡️")
                st.caption("Brent stabil → Seitwärtsbewegung")

        with target_cols[1]:
            st.markdown("**Mittelfristig** (2–6 Wochen)")
            if brent_forecast_note:
                st.markdown(f"~ {forecast_price:.1f} ct/L")
                st.caption(f"Brent-Trend: {brent_change_pct:+.1f}% in 30d")
            else:
                st.markdown(f"~ {nat_avg_90d:.1f} ct/L")
                st.caption("Rückkehr zum 90-Tage-Schnitt erwartet")

        with target_cols[2]:
            st.markdown("**Saisonziel** (Sommer)")
            # Heizöl ist typischerweise im Sommer günstiger
            seasonal_target = nat_min_90d * 0.95
            st.markdown(f":green[~ {seasonal_target:.1f} ct/L]")
            st.caption("Sommerloch: Beste Kaufzeit Juni–August")

        if brent_forecast_note:
            st.info(f"📊 {brent_forecast_note}")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# KI-FUNDAMENTALANALYSE
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("KI-Fundamentalanalyse")

def _render_analysis(analysis: dict) -> None:
    """Rendert eine einzelne LLM-Analyse im Dashboard."""
    meta = analysis.get("_meta", {})
    ts = meta.get("timestamp", "")
    if ts:
        try:
            dt = datetime.fromisoformat(ts)
            st.caption(f"Analyse vom {dt.strftime('%d.%m.%Y um %H:%M')} Uhr — "
                       f"Brent: {meta.get('brent_price', '?')} $ · "
                       f"National: {meta.get('national_price', '?')} ct/L · "
                       f"Tokens: {meta.get('tokens_used', '?')}")
        except (ValueError, TypeError):
            pass

    # Kennzahlen-Reihe
    k_cols = st.columns(4)
    with k_cols[0]:
        geo = analysis.get("geopolitik_score", "?")
        geo_color = "green" if isinstance(geo, int) and geo <= 3 else ("red" if isinstance(geo, int) and geo >= 7 else "orange")
        geo_label = {1: "sehr entspannt", 2: "entspannt", 3: "eher ruhig", 4: "leicht angespannt", 5: "moderat", 6: "angespannt", 7: "sehr angespannt", 8: "kritisch", 9: "hochkritisch", 10: "extrem angespannt"}.get(geo, "")
        st.markdown(f"**Geopolitik**")
        st.markdown(f":{geo_color}[{geo}/10]")
        if geo_label:
            st.caption(geo_label)
    with k_cols[1]:
        an = analysis.get("angebot_nachfrage_score", "?")
        an_color = "green" if isinstance(an, int) and an <= 3 else ("red" if isinstance(an, int) and an >= 7 else "orange")
        an_label = {1: "starkes Überangebot", 2: "Überangebot", 3: "leichtes Überangebot", 4: "eher ausgeglichen", 5: "ausgeglichen", 6: "leicht knapp", 7: "knapp", 8: "sehr knapp", 9: "hochgradig knapp", 10: "extreme Knappheit"}.get(an, "")
        st.markdown(f"**Angebot/Nachfrage**")
        st.markdown(f":{an_color}[{an}/10]")
        if an_label:
            st.caption(an_label)
    with k_cols[2]:
        richtung = analysis.get("preisdruck_richtung", "?")
        r_icon = {"aufwärts": "📈 Aufwärts", "abwärts": "📉 Abwärts", "seitwärts": "➡️ Seitwärts"}.get(richtung, richtung)
        r_color = {"aufwärts": "red", "abwärts": "green", "seitwärts": "orange"}.get(richtung, "gray")
        st.markdown(f"**Preisdruck**")
        st.markdown(f":{r_color}[{r_icon}]")
    with k_cols[3]:
        empf = analysis.get("empfehlung", "?")
        e_colors = {"jetzt kaufen": "green", "bald kaufen": "green", "abwarten": "orange", "dringend abwarten": "red"}
        e_icons = {"jetzt kaufen": "🟢", "bald kaufen": "🟡", "abwarten": "🟠", "dringend abwarten": "🔴"}
        st.markdown(f"**KI-Empfehlung**")
        st.markdown(f":{e_colors.get(empf, 'gray')}[{e_icons.get(empf, '⚪')} {empf.title()}]")

    # Lageanalyse
    st.markdown(f"**Lageeinschätzung:** {analysis.get('lageanalyse', '–')}")

    # Ereignisse als kompakte Tabelle
    ereignisse = analysis.get("ereignisse", [])
    if ereignisse:
        with st.expander(f"Relevante Ereignisse ({len(ereignisse)})", expanded=False):
            for e in ereignisse:
                auswirkung = e.get("auswirkung", "")
                a_icon = {"preistreibend": "🔴", "preissenkend": "🟢", "neutral": "⚪"}.get(auswirkung, "")
                hz = e.get("zeithorizont", "")
                st.markdown(f"{a_icon} **{e.get('titel', '')}** ({hz}) — {e.get('beschreibung', '')}")

    # Prognose + Risiken
    st.info(f"📊 **Prognose:** {analysis.get('prognose', '–')}")
    risiken = analysis.get("risiken", "")
    if risiken:
        st.warning(f"⚠️ **Risiken:** {risiken}")

    # Kaufrisiko nach Zeithorizont
    kaufrisiko = analysis.get("kaufrisiko", {})
    if kaufrisiko:
        st.markdown("**Kaufrisiko-Einschätzung:**")
        kr_cols = st.columns(3)
        kr_labels = [
            ("kurzfristig", "📅 Kurzfristig", "1–2 Wochen"),
            ("mittelfristig", "📆 Mittelfristig", "Wochen bis Monate"),
            ("langfristig", "🗓️ Langfristig", "mehrere Monate"),
        ]
        kr_colors = {"niedrig": "green", "mittel": "orange", "hoch": "red"}
        kr_icons = {"niedrig": "🟢", "mittel": "🟡", "hoch": "🔴"}
        for col, (key, label, zeitraum) in zip(kr_cols, kr_labels):
            entry = kaufrisiko.get(key, {})
            risiko = entry.get("risiko", "?")
            beschreibung = entry.get("beschreibung", "")
            color = kr_colors.get(risiko, "gray")
            icon = kr_icons.get(risiko, "⚪")
            with col:
                st.markdown(f"**{label}**")
                st.markdown(f":{color}[{icon} {risiko.title()}]")
                st.caption(f"{zeitraum}")
                if beschreibung:
                    st.markdown(f"{beschreibung}")

# Prüfe ob Azure-Credentials vorhanden sind
azure_configured = all(
    os.environ.get(k) for k in ("AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_OPENAI_DEPLOYMENT")
)

# Button + History-Selektor nebeneinander
llm_running = st.session_state.get("llm_running", False)
btn_col, hist_col = st.columns([1, 2])

analyze_clicked = False
with btn_col:
    if llm_running:
        st.button("⏳ Analyse läuft…", width="stretch", disabled=True)
    else:
        analyze_clicked = st.button(
            "KI-Analyse starten",
            width="stretch",
            disabled=not azure_configured,
            help="Ruft Azure OpenAI auf, um aktuelle Fundamentaldaten zu analysieren" if azure_configured
                 else "Azure OpenAI nicht konfiguriert — siehe .env.example",
        )

# Passwortabfrage nach Klick
if analyze_clicked and azure_configured:
    st.session_state.llm_ask_password = True

if st.session_state.get("llm_ask_password") and azure_configured:
    st.info("🔐 Bitte Passwort für die KI-Analyse eingeben:")
    pw = st.text_input("Passwort", type="password", key="llm_password")
    pw_cols = st.columns([1, 1, 4])
    with pw_cols[0]:
        if st.button("Bestätigen", type="primary"):
            if pw == os.environ.get("LLM_ANALYSIS_PASSWORD", ""):
                st.session_state.llm_ask_password = False
                # Weiter zur Überschreib-Prüfung
                if has_analysis_today():
                    st.session_state.llm_confirm_overwrite = True
                else:
                    st.session_state.llm_run_now = True
                st.rerun()
            else:
                st.error("❌ Falsches Passwort.")
    with pw_cols[1]:
        if st.button("Abbrechen"):
            st.session_state.llm_ask_password = False
            st.rerun()

if st.session_state.get("llm_confirm_overwrite") and azure_configured:
    st.warning("Heute wurde bereits eine KI-Analyse durchgeführt. Erneut ausführen und überschreiben?")
    confirm_cols = st.columns([1, 1, 4])
    with confirm_cols[0]:
        if st.button("Ja, überschreiben", type="primary"):
            st.session_state.llm_confirm_overwrite = False
            st.session_state.llm_run_now = True
            st.rerun()
    with confirm_cols[1]:
        if st.button("Nein, abbrechen"):
            st.session_state.llm_confirm_overwrite = False
            st.rerun()

# LLM-Aufruf ausführen
if st.session_state.get("llm_run_now") and azure_configured:
    st.session_state.llm_run_now = False
    st.session_state.llm_running = True
    brent_p = brent_current.get("price")
    nat_p = national_current.get("price")
    if brent_p and nat_p:
        status = st.status("KI-Fundamentalanalyse wird erstellt…", expanded=True)
        with status:
            st.write("📡 Aktuelle Nachrichten werden abgerufen…")
            try:
                from collectors.llm_analysis import _fetch_oil_news
                news = _fetch_oil_news()
                news_count = news.count("\n- ")
                st.write(f"✅ {news_count} Nachrichten geladen")
                st.write(f"🤖 Kimi-K2.5 analysiert Marktlage… (kann 30–60 Sek. dauern)")
                analysis = run_llm_analysis(brent_p, nat_p, brent_trend)
                save_analysis(analysis)
                tokens = analysis.get("_meta", {}).get("tokens_used", "?")
                st.write(f"✅ Analyse fertig ({tokens} Tokens)")
                status.update(label="KI-Analyse abgeschlossen!", state="complete", expanded=False)
                st.session_state.llm_running = False
                st.rerun()
            except Exception as e:
                st.session_state.llm_running = False
                status.update(label="KI-Analyse fehlgeschlagen", state="error")
                st.error(f"Fehler: {e}")
    else:
        st.session_state.llm_running = False
        st.error("Preis-Daten fehlen — KI-Analyse nicht möglich.")

# History laden und anzeigen
all_analyses = load_all_analyses()

if all_analyses:
    with hist_col:
        date_options = [a.get("_meta", {}).get("date", "?") for a in all_analyses]
        date_labels = []
        for d in date_options:
            try:
                dt = datetime.fromisoformat(d)
                date_labels.append(dt.strftime("%d.%m.%Y"))
            except (ValueError, TypeError):
                date_labels.append(d)
        selected_idx = st.selectbox(
            "Vergangene Analysen",
            range(len(date_labels)),
            format_func=lambda i: date_labels[i],
            help="Wähle eine vergangene Analyse zum Anzeigen",
        )

    with st.container(border=True):
        _render_analysis(all_analyses[selected_idx])
elif not azure_configured:
    st.info(
        "KI-Analyse nicht konfiguriert. Erstelle eine `.env`-Datei mit:\n\n"
        "`AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_DEPLOYMENT`"
    )
else:
    st.caption("Noch keine KI-Analysen vorhanden. Klicke den Button, um die erste zu erstellen.")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# EBENE 1: Brent Crude
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("Ebene 1: Brent Crude – Globaler Frühindikator")

with st.expander("Was zeigt dieser Chart?", expanded=False):
    st.markdown(
        """
        **Brent Crude** ist die internationale Referenz für Rohölpreise.
        Da Heizöl ein Erdölprodukt ist, folgen die deutschen Heizölpreise dem
        Rohölpreis – typischerweise mit einem **Verzug von 2 bis 4 Wochen**.

        - Steigt Brent stark → Heizölpreise werden in Kürze teurer → **jetzt kaufen**
        - Fällt Brent → Heizölpreise werden günstiger → **noch warten**
        - Der Kurs wird in USD pro Barrel angegeben
        """
    )

if not brent_hist.empty:
    fig_brent = go.Figure()
    fig_brent.add_trace(go.Scatter(
        x=brent_hist["date"], y=brent_hist["price"],
        mode="lines", name="Brent Crude (USD/Barrel)",
        line=dict(color="#e67e22", width=2),
        hovertemplate="%{x|%d.%m.%Y}: $%{y:.2f}<extra></extra>",
    ))
    # 20-Tage gleitender Schnitt als Orientierung
    brent_ma = brent_hist["price"].rolling(20, min_periods=1).mean()
    fig_brent.add_trace(go.Scatter(
        x=brent_hist["date"], y=brent_ma,
        mode="lines", name="20-Tage-Schnitt",
        line=dict(color="#f0a500", width=1, dash="dot"),
        hovertemplate="%{x|%d.%m.%Y}: $%{y:.2f}<extra></extra>",
    ))
    fig_brent.update_layout(
        height=350, margin=dict(l=0, r=0, t=10, b=50),
        yaxis_title="USD/Barrel",
        xaxis=dict(type="date", tickmode="auto", nticks=6, tickformat="%d.%m.%Y"),
        legend=dict(orientation="h", yanchor="top", y=0.99, xanchor="left", x=0.01),
        hovermode="x unified",
    )
    st.plotly_chart(fig_brent, width='stretch')
    trend_text = {"steigend": "📈 Steigend", "fallend": "📉 Fallend", "stabil": "➡️ Stabil"}.get(brent_trend, "–")
    st.caption(f"Aktueller 5-Tage-Trend: **{trend_text}**")
else:
    st.warning("Brent-Daten konnten nicht geladen werden. Bitte später erneut versuchen.")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# EBENE 2: Nationaler Heizölpreis
# ══════════════════════════════════════════════════════════════════════════════
st.subheader("Ebene 2: Nationaler Heizölpreis Deutschland")

with st.expander("Was zeigt dieser Chart?", expanded=False):
    st.markdown(
        """
        Der **bundesweite Tagesdurchschnitt** zeigt, wie sich der Heizölpreis
        in Deutschland insgesamt entwickelt. Quelle: heizoel24.de.

        - Der **30-Tage-Schnitt** (gestrichelte Linie) glättet kurzfristige Schwankungen
        - Liegt der aktuelle Preis **unter** dem 30-Tage-Schnitt → günstiger Einstieg
        - Liegt er **darüber** → eher abwarten

        Preise in ct/Liter für eine Abnahme von 3.000 Litern.
        """
    )

if not national_hist.empty:
    ma30 = get_moving_average(national_hist, window=30)

    fig_nat = go.Figure()
    fig_nat.add_trace(go.Scatter(
        x=national_hist["date"], y=national_hist["price"],
        mode="lines", name="Bundesdurchschnitt (ct/L)",
        line=dict(color="#2980b9", width=2),
        hovertemplate="%{x|%d.%m.%Y}: %{y:.2f} ct/L<extra></extra>",
    ))
    fig_nat.add_trace(go.Scatter(
        x=national_hist["date"], y=ma30,
        mode="lines", name="30-Tage-Schnitt",
        line=dict(color="#85c1e9", width=1.5, dash="dot"),
        hovertemplate="%{x|%d.%m.%Y}: %{y:.2f} ct/L<extra></extra>",
    ))
    fig_nat.update_layout(
        height=350, margin=dict(l=0, r=0, t=10, b=50),
        yaxis_title="ct/Liter",
        xaxis=dict(type="date", tickmode="auto", nticks=6, tickformat="%d.%m.%Y"),
        legend=dict(orientation="h", yanchor="top", y=0.99, xanchor="left", x=0.01),
        hovermode="x unified",
    )
    st.plotly_chart(fig_nat, width='stretch')

    current_price = national_hist["price"].iloc[-1]
    current_ma = ma30.iloc[-1]
    diff = current_price - current_ma
    if diff < 0:
        st.caption(f"Aktueller Preis liegt **{abs(diff):.2f} ct/L unter** dem 30-Tage-Schnitt ({current_ma:.2f} ct/L) — günstig.")
    else:
        st.caption(f"Aktueller Preis liegt **{diff:.2f} ct/L über** dem 30-Tage-Schnitt ({current_ma:.2f} ct/L).")
else:
    st.warning("Nationale Preisdaten konnten nicht geladen werden. Bitte später erneut versuchen.")

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# EBENE 3: Lokale Händlerpreise
# ══════════════════════════════════════════════════════════════════════════════
st.subheader(f"Ebene 3: Lokale Händlerpreise (PLZ {PLZ})")

with st.expander("Was zeigt diese Tabelle?", expanded=False):
    st.markdown(
        f"""
        Hier siehst du die **aktuellen Angebote von Heizöl-Händlern**, die nach
        PLZ **{PLZ}** liefern, für eine Bestellmenge von **{liters:,} Litern**.

        - Sortiert nach Preis (günstigster oben)
        - Klicke auf einen Link, um direkt beim Händler zu bestellen
        - Quelle: heizoel24.de
        """
    )

if not local_quotes.empty:
    def highlight_best(row):
        if row.name == 0:
            return ["background-color: #d4efdf; font-weight: bold"] * len(row)
        return [""] * len(row)

    display_df = local_quotes.copy()
    display_df["price"] = display_df["price"].apply(lambda x: f"{x:.2f} ct/L")
    display_df["total"] = display_df["total"].apply(
        lambda x: f"{x:,.2f} €" if pd.notna(x) and x else "–"
    )
    display_df["rating"] = display_df.apply(
        lambda r: f"{r['rating']}% ({r['rating_count']} Bew.)" if pd.notna(r.get("rating")) else "–", axis=1
    )
    display_df["url"] = display_df["url"].apply(
        lambda u: f'<a href="{u}" target="_blank">Profil ↗</a>' if u else "–"
    )
    display_df = display_df[["dealer", "price", "total", "rating", "url"]]
    display_df.columns = ["Händler", "Preis (ct/L)", f"Gesamt ({liters:,} L)", "Bewertung", "Link"]
    display_df.index = display_df.index + 1

    st.write(display_df.style.apply(highlight_best, axis=1).to_html(escape=False), unsafe_allow_html=True)
    best_total = local_quotes["total"].iloc[0]
    st.caption(
        f"Bestes Angebot: **{fmt_price(best_local, ' ct/L')}** — "
        f"Gesamtpreis für {liters:,} L: **{best_total:,.2f} €**"
    )
    comparison_links = get_comparison_links(plz=PLZ, liters=liters)
    st.caption(
        "Direkt bestellen: "
        + " · ".join(f"[{l['name']}]({l['url']})" for l in comparison_links)
    )
else:
    st.info(
        f"Lokale Händlerpreise konnten nicht abgerufen werden. "
        f"Direkt vergleichen:"
    )
    comparison_links = get_comparison_links(plz=PLZ, liters=liters)
    cols = st.columns(len(comparison_links))
    for col, link in zip(cols, comparison_links):
        col.link_button(f"↗ {link['name']}", url=link["url"], width='stretch')

st.divider()

# ── Historische Händlerdaten ───────────────────────────────────────────────────
with st.expander("Preisverlauf: Beste lokale Angebote (Zeitreihe)", expanded=False):
    st.markdown(
        f"""
        Zeigt den Preis des **günstigsten Händlers** in **PLZ {PLZ}** über die Zeit.
        Wird täglich um 10:00 Uhr aktualisiert.
        """
    )

    # Lade globale Daten (Brent + National)
    global_hist_file = os.path.join(os.path.dirname(__file__), "data", "history_global.csv")
    # Lade PLZ-spezifische Daten
    plz_hist_file = os.path.join(os.path.dirname(__file__), "data", f"history_plz_{PLZ}.csv")

    if os.path.exists(plz_hist_file):
        df_plz = pd.read_csv(plz_hist_file)
        df_plz = df_plz[df_plz["best_local_ct_per_liter"] != ""].copy()

        if not df_plz.empty:
            df_plz["date"] = pd.to_datetime(df_plz["date"])
            df_plz["best_local_ct_per_liter"] = pd.to_numeric(df_plz["best_local_ct_per_liter"], errors="coerce")
            df_plz = df_plz.dropna(subset=["best_local_ct_per_liter"]).sort_values("date")

            # Lade nationale Daten zum Vergleich
            df_national = None
            if os.path.exists(global_hist_file):
                df_national = pd.read_csv(global_hist_file)
                df_national["date"] = pd.to_datetime(df_national["date"])
                df_national["national_ct_per_liter"] = pd.to_numeric(df_national["national_ct_per_liter"], errors="coerce")
                df_national = df_national.dropna(subset=["national_ct_per_liter"]).sort_values("date")

            if not df_plz.empty:
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Scatter(
                    x=df_plz["date"], y=df_plz["best_local_ct_per_liter"],
                    mode="lines+markers", name=f"Beste lokal (PLZ {PLZ})",
                    line=dict(color="#27ae60", width=2),
                    hovertemplate="%{x|%d.%m.%Y}: %{y:.2f} ct/L<extra></extra>",
                ))
                if df_national is not None:
                    fig_hist.add_trace(go.Scatter(
                        x=df_national["date"], y=df_national["national_ct_per_liter"],
                        mode="lines", name="Bundesdurchschnitt",
                        line=dict(color="#3498db", width=1, dash="dot"),
                        hovertemplate="%{x|%d.%m.%Y}: %{y:.2f} ct/L<extra></extra>",
                    ))
                fig_hist.update_layout(
                    height=350, margin=dict(l=0, r=0, t=10, b=50),
                    yaxis_title="ct/Liter",
                    xaxis=dict(type="date", dtick="1d", title="Datum"),
                    legend=dict(orientation="h", yanchor="top", y=0.99, xanchor="left", x=0.01),
                    hovermode="x unified",
                )
                st.plotly_chart(fig_hist, width='stretch')

                # Statistik
                if len(df_plz) > 1:
                    min_price = df_plz["best_local_ct_per_liter"].min()
                    max_price = df_plz["best_local_ct_per_liter"].max()
                    avg_price = df_plz["best_local_ct_per_liter"].mean()
                    current_price = df_plz["best_local_ct_per_liter"].iloc[-1]
                    st.caption(
                        f"📊 Min: **{min_price:.2f}** ct/L | Ø: **{avg_price:.2f}** ct/L | "
                        f"Max: **{max_price:.2f}** ct/L | Jetzt: **{current_price:.2f}** ct/L"
                    )
            else:
                st.info(f"Noch keine Preisverlauf-Daten für PLZ {PLZ} vorhanden. Kommt täglich hinzu!")
        else:
            st.info(f"Noch keine Preisverlauf-Daten für PLZ {PLZ} vorhanden. Kommt täglich hinzu!")
    else:
        st.info(f"Noch keine Preisverlauf-Daten für PLZ {PLZ} vorhanden. Kommt täglich hinzu!")

st.divider()

# ── Kombinierter Korrelations-Chart ───────────────────────────────────────────
with st.expander("Korrelation: Brent Crude vs. nationaler Heizölpreis", expanded=False):
    st.markdown(
        """
        Dieser Chart zeigt beide Kurven übereinander (mit zwei Y-Achsen), um die
        zeitliche Korrelation zwischen Rohöl und Heizöl sichtbar zu machen.
        Typischerweise folgt der Heizölpreis dem Rohölpreis mit 2–4 Wochen Verzug.
        """
    )
    if not brent_hist.empty and not national_hist.empty:
        fig_combined = make_subplots(specs=[[{"secondary_y": True}]])
        fig_combined.add_trace(
            go.Scatter(
                x=brent_hist["date"], y=brent_hist["price"],
                name="Brent (USD/Barrel)", line=dict(color="#e67e22", width=1.5),
                hovertemplate="%{x|%d.%m.%Y}: $%{y:.2f}<extra></extra>",
            ),
            secondary_y=False,
        )
        fig_combined.add_trace(
            go.Scatter(
                x=national_hist["date"], y=national_hist["price"],
                name="Heizöl DE (ct/L)", line=dict(color="#2980b9", width=1.5),
                hovertemplate="%{x|%d.%m.%Y}: %{y:.2f} ct/L<extra></extra>",
            ),
            secondary_y=True,
        )
        fig_combined.update_yaxes(title_text="Brent (USD/Barrel)", secondary_y=False)
        fig_combined.update_yaxes(title_text="Heizöl DE (ct/L)", secondary_y=True)
        fig_combined.update_layout(
            height=350, margin=dict(l=0, r=0, t=10, b=50),
            xaxis=dict(type="date", tickmode="auto", nticks=6, tickformat="%d.%m.%Y"),
            legend=dict(orientation="h", yanchor="top", y=0.99, xanchor="left", x=0.01),
            hovermode="x unified",
        )
        st.plotly_chart(fig_combined, width='stretch')
    else:
        st.info("Nicht genug Daten für Korrelations-Chart.")

st.caption(
    "Datenquellen: [heizoel24.de](https://www.heizoel24.de) · "
    "[Yahoo Finance (Brent)](https://finance.yahoo.com) · "
    "Kein Anspruch auf Vollständigkeit oder Richtigkeit."
)
