"""
Neon Postgres DB layer for the Heizöl Dashboard.

Single source of truth for global prices, local prices, LLM analyses and
tracked PLZs. Replaces the previous CSV/JSON file persistence so that
Streamlit Cloud container restarts (which wipe local writes) no longer
lose data.

Schema lives in the `heizoel` namespace to keep the shared Neon DB tidy.
"""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL not set. Add it to .env or Streamlit secrets."
        )
    return url


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    """Short-lived connection using Neon's pooler URL."""
    conn = psycopg.connect(_database_url(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS heizoel;

CREATE TABLE IF NOT EXISTS heizoel.global_prices (
  date DATE PRIMARY KEY,
  brent_usd NUMERIC,
  national_ct_per_liter NUMERIC,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS heizoel.local_prices (
  date DATE NOT NULL,
  plz TEXT NOT NULL,
  best_local_ct_per_liter NUMERIC,
  top_dealers JSONB,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (date, plz)
);

CREATE TABLE IF NOT EXISTS heizoel.tracked_plzs (
  plz TEXT PRIMARY KEY,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_collected_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS heizoel.llm_analyses (
  date DATE PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  brent_price NUMERIC,
  national_price NUMERIC,
  brent_trend TEXT,
  model TEXT,
  tokens_used INTEGER,
  analysis JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_local_prices_plz_date
  ON heizoel.local_prices(plz, date DESC);

CREATE INDEX IF NOT EXISTS idx_llm_analyses_date
  ON heizoel.llm_analyses(date DESC);
"""


def init_schema() -> None:
    """Idempotent schema setup. Safe to call on every app startup."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)


# ── Global prices (Brent + national average) ─────────────────────────────────

def upsert_global_price(
    d: date,
    brent_usd: float | None,
    national_ct: float | None,
) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO heizoel.global_prices
              (date, brent_usd, national_ct_per_liter, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (date) DO UPDATE SET
              brent_usd = EXCLUDED.brent_usd,
              national_ct_per_liter = EXCLUDED.national_ct_per_liter,
              updated_at = NOW()
            """,
            (d, brent_usd, national_ct),
        )


def load_global_prices() -> list[dict[str, Any]]:
    """Returns all rows ordered by date ASC. Empty list if none."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT date, brent_usd, national_ct_per_liter
            FROM heizoel.global_prices
            ORDER BY date ASC
            """
        )
        return cur.fetchall()


# ── Local prices per PLZ ──────────────────────────────────────────────────────

def upsert_local_price(
    d: date,
    plz: str,
    best_local_ct: float | None,
    top_dealers: list[dict[str, Any]] | None,
) -> None:
    dealers_json = json.dumps(top_dealers or [], ensure_ascii=False)
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO heizoel.local_prices
              (date, plz, best_local_ct_per_liter, top_dealers, updated_at)
            VALUES (%s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (date, plz) DO UPDATE SET
              best_local_ct_per_liter = EXCLUDED.best_local_ct_per_liter,
              top_dealers = EXCLUDED.top_dealers,
              updated_at = NOW()
            """,
            (d, plz, best_local_ct, dealers_json),
        )


def load_local_prices(plz: str) -> list[dict[str, Any]]:
    """Returns all rows for one PLZ ordered by date ASC. Empty list if none."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT date, best_local_ct_per_liter, top_dealers
            FROM heizoel.local_prices
            WHERE plz = %s
            ORDER BY date ASC
            """,
            (plz,),
        )
        return cur.fetchall()


# ── Tracked PLZs (user-managed list of PLZs to collect daily) ────────────────

def register_plz(plz: str) -> None:
    """Add a PLZ to the tracked list. No-op if already tracked."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO heizoel.tracked_plzs (plz)
            VALUES (%s)
            ON CONFLICT (plz) DO NOTHING
            """,
            (plz,),
        )


def get_tracked_plzs() -> list[str]:
    """Used by collect_daily.py to know which PLZs to fetch."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT plz FROM heizoel.tracked_plzs ORDER BY plz")
        return [r["plz"] for r in cur.fetchall()]


def mark_plz_collected(plz: str) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE heizoel.tracked_plzs SET last_collected_at = NOW() WHERE plz = %s",
            (plz,),
        )


# ── LLM analyses (one per day, overwritable) ─────────────────────────────────

def save_llm_analysis(analysis: dict[str, Any]) -> None:
    """Upsert by date. `analysis['_meta']['date']` must be set."""
    meta = analysis.get("_meta", {}) or {}
    d_str = meta.get("date") or date.today().isoformat()
    d = date.fromisoformat(d_str)

    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO heizoel.llm_analyses
              (date, brent_price, national_price, brent_trend,
               model, tokens_used, analysis)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (date) DO UPDATE SET
              brent_price = EXCLUDED.brent_price,
              national_price = EXCLUDED.national_price,
              brent_trend = EXCLUDED.brent_trend,
              model = EXCLUDED.model,
              tokens_used = EXCLUDED.tokens_used,
              analysis = EXCLUDED.analysis,
              created_at = NOW()
            """,
            (
                d,
                meta.get("brent_price"),
                meta.get("national_price"),
                meta.get("brent_trend"),
                meta.get("model"),
                meta.get("tokens_used"),
                json.dumps(analysis, ensure_ascii=False),
            ),
        )


def load_all_llm_analyses() -> list[dict[str, Any]]:
    """Returns all analyses, newest first."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT analysis
            FROM heizoel.llm_analyses
            ORDER BY date DESC
            """
        )
        return [r["analysis"] for r in cur.fetchall()]


def has_llm_analysis_today() -> bool:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM heizoel.llm_analyses WHERE date = CURRENT_DATE LIMIT 1"
        )
        return cur.fetchone() is not None
