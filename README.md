# 🛢️ Heizöl-Dashboard Freudenberg

Intelligenter Preistracker für den optimalen Heizöl-Kauf — mit Live-Daten, historischen Trends und KI-gestützter Marktanalyse.

![Python](https://img.shields.io/badge/Python-3.11+-3776ab?logo=python&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.55-ff4b4b?logo=streamlit&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Features

### 📊 Drei-Ebenen-Preismodell

| Ebene | Datenquelle | Beschreibung |
|-------|-------------|--------------|
| **Brent Crude** | Yahoo Finance | Globaler Frühindikator — Änderungen spiegeln sich ~2–4 Wochen später im Heizölpreis |
| **Bundesdurchschnitt** | heizoel24.de | Täglicher Durchschnittspreis für Deutschland (ct/Liter, 3.000 L) |
| **Lokale Händler** | heizoel24.de | Aktuelle Angebote nach PLZ mit Händler-Ranking |

### 🤖 KI-Fundamentalanalyse

Per Klick analysiert ein LLM (via Azure OpenAI) die aktuelle Marktlage:

- **Geopolitik-Score** (1–10) — von „sehr entspannt" bis „extrem angespannt"
- **Angebot/Nachfrage-Score** (1–10) — von „starkes Überangebot" bis „extreme Knappheit"
- **Preisdruck-Richtung** — aufwärts / abwärts / seitwärts
- **Kaufempfehlung** — jetzt kaufen, bald kaufen, abwarten, dringend abwarten
- **Kaufrisiko-Einschätzung** — kurzfristig, mittelfristig, langfristig
- **Relevante Ereignisse** mit Auswirkung und Zeithorizont
- **Prognose & Risiken** für die nächsten 2–8 Wochen

Analysen werden in `data/llm_analyses.json` gespeichert und sind im Dashboard als History abrufbar.

### ⏱️ Automatische Datensammlung

Ein **GitHub Actions Workflow** sammelt täglich um 10:00 Uhr die aktuellen Preise und committet sie automatisch ins Repository.

---

## Projektstruktur

```
├── app.py                  # Streamlit-Dashboard (Hauptanwendung)
├── collect_daily.py        # Tägliches Sammel-Skript (GitHub Actions)
├── collectors/
│   ├── brent.py            # Brent Crude via yfinance
│   ├── national.py         # Bundesdurchschnitt via heizoel24.de
│   ├── local.py            # Lokale Händlerpreise via heizoel24.de
│   └── llm_analysis.py     # KI-Analyse via Azure OpenAI
├── data/
│   ├── history_global.csv  # Historische Brent- & Nationaldaten
│   ├── history_plz_*.csv   # Lokale Preishistorie pro PLZ
│   └── llm_analyses.json   # Gespeicherte KI-Analysen
├── .github/workflows/
│   └── collect_prices.yml  # Täglicher Cron-Job
├── .streamlit/
│   └── config.toml         # Theme-Konfiguration
└── requirements.txt
```

---

## Schnellstart

### Voraussetzungen

- Python 3.11+
- (Optional) Azure OpenAI Zugang für die KI-Analyse

### Installation

```bash
git clone https://github.com/<user>/heizoil-dashboard.git
cd heizoil-dashboard

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### Konfiguration

Erstelle eine `.env`-Datei im Projektverzeichnis:

```env
# Nur nötig für die KI-Analyse:
AZURE_OPENAI_ENDPOINT=https://xxx.openai.azure.com/openai/v1/
AZURE_OPENAI_API_KEY=dein-api-key
AZURE_OPENAI_DEPLOYMENT=dein-deployment-name
```

### Starten

```bash
streamlit run app.py
```

Das Dashboard ist dann unter `http://localhost:8501` erreichbar.

---

## Datensammlung

### Manuell

```bash
python collect_daily.py
```

### Automatisch (GitHub Actions)

Der Workflow `.github/workflows/collect_prices.yml` läuft täglich um 10:00 Uhr (MEZ) und committet neue Preisdaten direkt ins Repository.

---

## Tech-Stack

| Komponente | Technologie |
|------------|-------------|
| Frontend | Streamlit |
| Charts | Plotly |
| Daten | pandas, yfinance |
| KI-Analyse | OpenAI SDK → Azure OpenAI |
| Scraping | requests (heizoel24.de API) |
| CI/CD | GitHub Actions |

---

## Lizenz

MIT
