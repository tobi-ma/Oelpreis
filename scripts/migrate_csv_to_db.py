"""
One-shot migration script: existing CSV / JSON files → Neon Postgres.

Reads:
  - data/history_global.csv
  - data/history_plz_*.csv  (for every PLZ)
  - data/llm_analyses.json  (if present)

Writes via collectors/db.py. Idempotent thanks to ON CONFLICT DO UPDATE.
Safe to run multiple times.
"""
from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys
from datetime import date

# Make sure the project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from collectors import db

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def _to_float(v: str) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def migrate_global() -> int:
    path = os.path.join(DATA_DIR, "history_global.csv")
    if not os.path.exists(path):
        print(f"  skip (not found): {path}")
        return 0

    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d_str = row.get("date", "").strip()
            if not d_str:
                continue
            d = date.fromisoformat(d_str)
            db.upsert_global_price(
                d,
                _to_float(row.get("brent_usd", "")),
                _to_float(row.get("national_ct_per_liter", "")),
            )
            count += 1
    return count


def migrate_local_plz() -> dict[str, int]:
    counts: dict[str, int] = {}
    pattern = os.path.join(DATA_DIR, "history_plz_*.csv")
    for path in sorted(glob.glob(pattern)):
        m = re.search(r"history_plz_(\d+)\.csv$", path)
        if not m:
            continue
        plz = m.group(1)
        # Register PLZ (idempotent)
        db.register_plz(plz)

        count = 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d_str = row.get("date", "").strip()
                if not d_str:
                    continue
                d = date.fromisoformat(d_str)
                best = _to_float(row.get("best_local_ct_per_liter", ""))
                dealers_raw = row.get("top_3_dealers", "")
                try:
                    dealers = json.loads(dealers_raw) if dealers_raw else []
                except json.JSONDecodeError:
                    dealers = []
                db.upsert_local_price(d, plz, best, dealers)
                count += 1
        counts[plz] = count
    return counts


def migrate_llm_analyses() -> int:
    path = os.path.join(DATA_DIR, "llm_analyses.json")
    if not os.path.exists(path):
        print(f"  skip (not found): {path}")
        return 0

    with open(path, encoding="utf-8") as f:
        analyses = json.load(f)

    count = 0
    for a in analyses:
        if not isinstance(a, dict):
            continue
        db.save_llm_analysis(a)
        count += 1
    return count


def main() -> None:
    print("Migrating CSV/JSON → Neon Postgres")
    print("=" * 50)

    print("\nEnsuring schema exists…")
    db.init_schema()
    print("  ✓")

    print("\nMigrating history_global.csv…")
    n = migrate_global()
    print(f"  ✓ {n} rows")

    print("\nMigrating history_plz_*.csv…")
    per_plz = migrate_local_plz()
    for plz, n in per_plz.items():
        print(f"  ✓ PLZ {plz}: {n} rows")
    if not per_plz:
        print("  (no files found)")

    print("\nMigrating llm_analyses.json…")
    n = migrate_llm_analyses()
    print(f"  ✓ {n} analyses")

    print("\n" + "=" * 50)
    print("Done. Verification:")
    with db.connection() as conn, conn.cursor() as cur:
        for table in ("global_prices", "local_prices", "tracked_plzs", "llm_analyses"):
            cur.execute(f"SELECT COUNT(*) AS n FROM heizoel.{table}")
            row = cur.fetchone()
            print(f"  heizoel.{table}: {row['n']} rows")


if __name__ == "__main__":
    main()
