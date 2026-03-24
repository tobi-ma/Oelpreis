"""
Nationaler Heizöl-Marktpreis Deutschland via heizoel24.de.

Nutzt zwei Endpunkte:
  - /api/site/1/prices/history  — Verlaufsdaten (primär, saubereres Format)
  - /api/chartapi/GetAveragePriceHistory — Fallback

Liefert den täglichen Bundesdurchschnittspreis in ct/Liter (für 3000 L).
"""

import requests
import pandas as pd
from datetime import datetime, timedelta


HISTORY_URL = "https://www.heizoel24.de/api/site/1/prices/history"
CHART_API_URL = "https://www.heizoel24.de/api/chartapi/GetAveragePriceHistory"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.heizoel24.de/heizoelpreise",
}


def get_national_history(days: int = 365, liters: int = 3000) -> pd.DataFrame:
    """
    Bundesdurchschnitt der letzten `days` Tage in ct/Liter.

    Spalten:
        date  (datetime) — Datum
        price (float)    — Preis in ct/Liter
    """
    df = _fetch_history_primary(days=days, liters=liters)
    if df.empty:
        df = _fetch_history_fallback(days=days)
    return df


def _fetch_history_primary(days: int, liters: int) -> pd.DataFrame:
    """Primärer Endpoint: /api/site/1/prices/history — gibt DateTime+Price zurück."""
    # rangeType: 1 = 3 Monate, 2 = 6 Monate, 3 = 1 Jahr, 4 = 2 Jahre, 5 = 5 Jahre
    range_type = 3 if days <= 365 else (4 if days <= 730 else 5)
    params = {"amount": liters, "productId": 1, "rangeType": range_type}
    try:
        resp = requests.get(HISTORY_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return pd.DataFrame(columns=["date", "price"])

    if not isinstance(data, list) or not data:
        return pd.DataFrame(columns=["date", "price"])

    rows = [{"date": e["DateTime"], "price": float(e["Price"])} for e in data if "DateTime" in e and "Price" in e]
    if not rows:
        return pd.DataFrame(columns=["date", "price"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    cutoff = datetime.today() - timedelta(days=days)
    df = df[df["date"] >= pd.Timestamp(cutoff)].sort_values("date").reset_index(drop=True)
    return df


def _fetch_history_fallback(days: int) -> pd.DataFrame:
    """Fallback-Endpoint: /api/chartapi/GetAveragePriceHistory."""
    end = datetime.today()
    start = end - timedelta(days=days)
    params = {
        "countryId": 1,
        "minDate": start.strftime("%Y-%m-%dT00:00:00"),
        "maxDate": end.strftime("%Y-%m-%dT23:59:59"),
    }
    try:
        resp = requests.get(CHART_API_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return pd.DataFrame(columns=["date", "price"])

    values = data.get("Values") if isinstance(data, dict) else []
    if not values:
        return pd.DataFrame(columns=["date", "price"])

    rows = [{"date": e["date"], "price": float(e["value"])} for e in values if isinstance(e, dict)]
    if not rows:
        return pd.DataFrame(columns=["date", "price"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], unit="ms").dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)
    return df


def get_national_current(liters: int = 3000) -> dict:
    """
    Aktuellster verfügbarer Bundesdurchschnittspreis.

    Returns:
        {"price": float, "change_pct": float, "date": str}
    """
    df = get_national_history(days=5, liters=liters)
    if df.empty:
        return {"price": None, "change_pct": None, "date": None}
    latest = df.iloc[-1]
    result = {
        "price": round(float(latest["price"]), 2),
        "date": str(latest["date"].date()),
        "change_pct": None,
    }
    if len(df) >= 2:
        prev = df.iloc[-2]
        result["change_pct"] = round(
            (latest["price"] - prev["price"]) / prev["price"] * 100, 2
        )
    return result


def get_moving_average(df: pd.DataFrame, window: int = 30) -> pd.Series:
    """Gleitender Durchschnitt über `window` Tage."""
    return df["price"].rolling(window=window, min_periods=1).mean()
