"""
Tägliches Datensammel-Skript für den GitHub Actions Workflow.
Schreibt aktuelle Preise nach Neon Postgres (Schema: heizoel).

Was wird gesammelt:
  - Brent Crude (USD/Barrel)           → heizoel.global_prices
  - Bundesdurchschnitt Heizöl (ct/L)    → heizoel.global_prices
  - Für jede PLZ in heizoel.tracked_plzs:
      Beste lokale Angebote (Top 3)    → heizoel.local_prices

Die Liste der zu sammelnden PLZen wird aus der DB gelesen. Die App füllt
diese Tabelle, wenn Tobi im Dashboard eine neue PLZ eingibt. Falls die
Tabelle leer ist, wird "57258" als Fallback verwendet und automatisch
registriert.
"""
from __future__ import annotations

import os
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from collectors.brent import get_brent_current
from collectors.national import get_national_current
from collectors.local import get_local_quotes
from collectors import db

LITERS = 3000
DEFAULT_PLZ = "57258"


def main() -> None:
    today = date.today()

    # Schema sicherstellen (idempotent)
    db.init_schema()

    # Globale Daten
    brent = get_brent_current()
    national = get_national_current()
    brent_p = brent.get("price")
    national_p = national.get("price")

    db.upsert_global_price(today, brent_p, national_p)
    print(
        f"[{today}] Global: Brent {brent_p} USD | "
        f"National {national_p} ct/L"
    )

    # Lokale Daten pro getrackter PLZ
    plzs = db.get_tracked_plzs()
    if not plzs:
        print(f"  (no tracked PLZs — registering default: {DEFAULT_PLZ})")
        db.register_plz(DEFAULT_PLZ)
        plzs = [DEFAULT_PLZ]

    for plz in plzs:
        try:
            local_quotes = get_local_quotes(plz=plz, liters=LITERS)
        except Exception as e:
            print(f"  ✗ PLZ {plz}: fetch failed — {e}")
            continue

        top_dealers: list[dict] = []
        if not local_quotes.empty:
            for _, dealer in local_quotes.head(3).iterrows():
                top_dealers.append(
                    {
                        "name": dealer["dealer"],
                        "price_ct_per_liter": round(float(dealer["price"]), 2),
                        "rating": (
                            int(dealer["rating"])
                            if dealer.get("rating")
                            else None
                        ),
                    }
                )

        best_local = (
            float(local_quotes.iloc[0]["price"]) if not local_quotes.empty else None
        )
        db.upsert_local_price(today, plz, best_local, top_dealers)
        db.mark_plz_collected(plz)

        dealers_str = (
            " | ".join(
                f"{d['name']}: {d['price_ct_per_liter']} ct/L" for d in top_dealers
            )
            if top_dealers
            else "–"
        )
        print(
            f"[{today}] PLZ {plz}: Best {best_local} ct/L | Top 3: {dealers_str}"
        )


if __name__ == "__main__":
    main()
