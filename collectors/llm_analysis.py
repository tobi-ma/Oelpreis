"""
LLM-gestützte Fundamentalanalyse des Ölmarkts via Azure OpenAI.

Ruft ein LLM auf, das die aktuelle geopolitische Lage, Angebots-/Nachfragesituation
und relevante Ereignisse analysiert und eine strukturierte Einschätzung zurückgibt.

Ergebnisse werden pro Tag in Neon Postgres (Tabelle heizoel.llm_analyses)
persistiert, damit sie Streamlit-Cloud-Restarts überleben.
"""

import json
import os
import requests as _requests
from datetime import date, datetime

from openai import OpenAI

SYSTEM_PROMPT = """\
Du bist ein Energie-Marktanalyst, spezialisiert auf den europäischen Heizölmarkt.
Deine Aufgabe: Analysiere die aktuelle geopolitische und wirtschaftliche Lage
und deren Auswirkungen auf den Ölpreis / Heizölpreis in Deutschland.

Berücksichtige insbesondere:
- OPEC+-Entscheidungen und Förderquoten
- Geopolitische Konflikte (Kriege, Sanktionen, Embargos)
- Waffenstillstände, Friedensverhandlungen und deren Auswirkungen
- Naturkatastrophen, Raffinerieausfälle, Pipelineprobleme
- USD/EUR-Wechselkurs
- Saisonale Muster (Heizperiode vs. Sommer)
- Wirtschaftliche Konjunktur (Rezessionsrisiken, Nachfrage aus China/Indien)
- Lagerbestände (US-Bestände, EU-Speicher)
- Aktuelle Nachrichtenlage

Antworte AUSSCHLIESSLICH mit validem JSON im folgenden Format:
{
  "geopolitik_score": <int 1-10, 1=sehr entspannt, 10=extrem angespannt>,
  "angebot_nachfrage_score": <int 1-10, 1=starkes Überangebot, 10=extreme Knappheit>,
  "preisdruck_richtung": "<'aufwärts'|'abwärts'|'seitwärts'>",
  "konfidenz": <int 1-10, wie sicher bist du dir>,
  "empfehlung": "<'jetzt kaufen'|'bald kaufen'|'abwarten'|'dringend abwarten'>",
  "ereignisse": [
    {
      "titel": "<kurzer Titel>",
      "auswirkung": "<'preistreibend'|'preissenkend'|'neutral'>",
      "zeithorizont": "<'kurzfristig'|'mittelfristig'|'langfristig'>",
      "beschreibung": "<1 Satz>"
    }
  ],
  "lageanalyse": "<3-5 Sätze: aktuelle Marktlage, wichtigste Einflussfaktoren>",
  "prognose": "<1-2 Sätze: konkrete Einschätzung für die nächsten 2-8 Wochen>",
  "risiken": "<1-2 Sätze: was könnte die Prognose kippen>",
  "kaufrisiko": {
    "kurzfristig": {
      "risiko": "<'niedrig'|'mittel'|'hoch'>",
      "beschreibung": "<1 Satz: Einschätzung für Kauf in den nächsten 1-2 Wochen>"
    },
    "mittelfristig": {
      "risiko": "<'niedrig'|'mittel'|'hoch'>",
      "beschreibung": "<1 Satz: Einschätzung für Kauf in einigen Wochen bis wenigen Monaten>"
    },
    "langfristig": {
      "risiko": "<'niedrig'|'mittel'|'hoch'>",
      "beschreibung": "<1 Satz: Einschätzung für Kauf in mehreren Monaten>"
    }
  }
}
"""


def _fetch_oil_news() -> str:
    """Holt aktuelle Ölmarkt-Nachrichten via RSS/Web für den LLM-Kontext."""
    headlines = []

    # Versuch 1: Google News RSS für Ölpreis
    feeds = [
        "https://news.google.com/rss/search?q=oil+price+OPEC&hl=de&gl=DE&ceid=DE:de",
        "https://news.google.com/rss/search?q=Heiz%C3%B6l+Preis&hl=de&gl=DE&ceid=DE:de",
        "https://news.google.com/rss/search?q=Brent+crude+oil&hl=en&gl=US&ceid=US:en",
    ]

    for feed_url in feeds:
        try:
            r = _requests.get(feed_url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                # Einfaches XML-Parsing ohne extra Dependency
                import re
                titles = re.findall(r"<title>(.*?)</title>", r.text)
                # Erste Title ist der Feed-Name, überspringen
                for t in titles[1:6]:  # Top 5 pro Feed
                    # HTML-Entities decoden
                    t = t.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'").replace("&quot;", '"')
                    if t not in headlines:
                        headlines.append(t)
        except Exception:
            continue

    if not headlines:
        return "Keine aktuellen Nachrichtenüberschriften verfügbar."

    return "Aktuelle Nachrichtenüberschriften zum Ölmarkt:\n" + "\n".join(f"- {h}" for h in headlines[:15])


def _build_user_prompt(brent_price: float, national_price: float, brent_trend: str) -> str:
    today = date.today().strftime("%d.%m.%Y")
    news = _fetch_oil_news()
    return (
        f"Heute ist der {today}.\n\n"
        f"Aktuelle Marktdaten:\n"
        f"- Brent Crude: {brent_price:.2f} USD/Barrel\n"
        f"- Heizöl-Bundesdurchschnitt Deutschland: {national_price:.2f} ct/Liter\n"
        f"- Brent 5-Tage-Trend: {brent_trend}\n\n"
        f"{news}\n\n"
        f"Analysiere anhand der obigen Nachrichtenlage und Marktdaten die aktuelle "
        f"Situation und gib deine Einschätzung als JSON ab. "
        f"Stütze dich auf die gelieferten Nachrichtenüberschriften, nicht auf dein internes Wissen über Daten nach deinem Cutoff."
    )


def run_llm_analysis(
    brent_price: float,
    national_price: float,
    brent_trend: str,
) -> dict:
    """
    Ruft Azure AI Model via OpenAI-kompatiblen Endpoint auf.
    Erwartet Umgebungsvariablen:
      AZURE_OPENAI_ENDPOINT — z.B. https://xxx.openai.azure.com/openai/v1/
      AZURE_OPENAI_API_KEY
      AZURE_OPENAI_DEPLOYMENT — Deployment/Model-Name, z.B. Kimi-K2.5
    """
    client = OpenAI(
        base_url=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        default_headers={"api-key": os.environ["AZURE_OPENAI_API_KEY"]},
    )

    user_prompt = _build_user_prompt(brent_price, national_price, brent_trend)

    response = client.chat.completions.create(
        model=os.environ["AZURE_OPENAI_DEPLOYMENT"],
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
        max_tokens=16000,  # Reasoning-Modelle brauchen mehr Budget
    )

    raw = response.choices[0].message.content
    if not raw:
        # Fallback für Reasoning-Modelle (z.B. Kimi, o1)
        reasoning = getattr(response.choices[0].message, "reasoning_content", None)
        raise ValueError(
            f"Modell hat keinen Content zurückgegeben. "
            f"Reasoning: {reasoning[:200] if reasoning else 'leer'}"
        )
    # Markdown-Codeblöcke entfernen falls vorhanden
    if raw.strip().startswith("```"):
        raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    analysis = json.loads(raw)

    # Metadaten hinzufügen
    analysis["_meta"] = {
        "date": date.today().isoformat(),
        "timestamp": datetime.now().isoformat(),
        "brent_price": brent_price,
        "national_price": national_price,
        "brent_trend": brent_trend,
        "model": response.model,
        "tokens_used": response.usage.total_tokens if response.usage else None,
    }

    return analysis


def save_analysis(analysis: dict) -> None:
    """Speichert eine Analyse in Neon Postgres (upsert nach Datum)."""
    from collectors import db
    db.save_llm_analysis(analysis)


def load_all_analyses() -> list[dict]:
    """Lädt alle gespeicherten Analysen aus Neon Postgres (neueste zuerst)."""
    from collectors import db
    return db.load_all_llm_analyses()


def has_analysis_today() -> bool:
    """Prüft ob heute bereits eine Analyse erstellt wurde."""
    from collectors import db
    return db.has_llm_analysis_today()


def get_latest_analysis() -> dict | None:
    """Gibt die neueste Analyse zurück oder None."""
    analyses = load_all_analyses()
    return analyses[0] if analyses else None
