"""
Tägliches Datensammel-Skript für den GitHub Actions Workflow.
Holt aktuelle Preise und speichert sie pro PLZ in separaten Dateien.

Speichert:
  - Brent Crude (USD/Barrel) — global
  - Bundesdurchschnitt Heizöl (ct/L) — global
  - Beste lokale Angebote (Top 3 Händler mit Preisen) — pro PLZ
"""

import csv
import os
import json
from datetime import date

from collectors.brent import get_brent_current
from collectors.national import get_national_current
from collectors.local import get_local_quotes

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
GLOBAL_HISTORY_FILE = os.path.join(DATA_DIR, "history_global.csv")
PLZ = "57258"
LITERS = 3000


def get_plz_history_file(plz: str) -> str:
    """Gibt den Pfad für die PLZ-spezifische History-Datei zurück."""
    return os.path.join(DATA_DIR, f"history_plz_{plz}.csv")


def main():
    today = date.today().isoformat()

    # Globale Daten (unabhängig von PLZ)
    brent = get_brent_current()
    national = get_national_current()

    # Globale Datei aktualisieren
    global_row = {
        "date": today,
        "brent_usd": brent.get("price") or "",
        "national_ct_per_liter": national.get("price") or "",
    }

    global_rows = []
    if os.path.exists(GLOBAL_HISTORY_FILE):
        with open(GLOBAL_HISTORY_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            global_rows = [r for r in reader if r.get("date") != today]

    global_rows.append(global_row)
    global_rows.sort(key=lambda r: r["date"])

    global_fieldnames = ["date", "brent_usd", "national_ct_per_liter"]
    with open(GLOBAL_HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=global_fieldnames)
        writer.writeheader()
        writer.writerows(global_rows)

    print(
        f"[{today}] Global: Brent {global_row['brent_usd']} USD | "
        f"National {global_row['national_ct_per_liter']} ct/L"
    )

    # Lokale Daten pro PLZ
    local_quotes = get_local_quotes(plz=PLZ, liters=LITERS)

    # Top 3 Händler als JSON speichern (Name, Preis, Rating)
    top_dealers = []
    if not local_quotes.empty:
        for _, dealer in local_quotes.head(3).iterrows():
            top_dealers.append({
                "name": dealer["dealer"],
                "price_ct_per_liter": round(float(dealer["price"]), 2),
                "rating": int(dealer["rating"]) if dealer.get("rating") else None,
            })

    plz_row = {
        "date": today,
        "best_local_ct_per_liter": local_quotes.iloc[0]["price"] if not local_quotes.empty else "",
        "top_3_dealers": json.dumps(top_dealers, ensure_ascii=False) if top_dealers else "",
    }

    plz_history_file = get_plz_history_file(PLZ)
    plz_rows = []
    if os.path.exists(plz_history_file):
        with open(plz_history_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            plz_rows = [r for r in reader if r.get("date") != today]

    plz_rows.append(plz_row)
    plz_rows.sort(key=lambda r: r["date"])

    plz_fieldnames = ["date", "best_local_ct_per_liter", "top_3_dealers"]
    with open(plz_history_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=plz_fieldnames)
        writer.writeheader()
        writer.writerows(plz_rows)

    # Zusammenfassung ausgeben
    dealers_str = " | ".join([f"{d['name']}: {d['price_ct_per_liter']} ct/L" for d in top_dealers]) if top_dealers else "–"
    print(
        f"[{today}] PLZ {PLZ}: Best local {plz_row['best_local_ct_per_liter']} ct/L | "
        f"Top 3: {dealers_str}"
    )


if __name__ == "__main__":
    main()
