"""
Brent Crude Oil Preis via yfinance.
Dient als globaler Frühindikator: Brent-Preisänderungen spiegeln sich
typischerweise 2–4 Wochen später in den deutschen Heizölpreisen wider.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


BRENT_TICKER = "BZ=F"


def get_brent_history(days: int = 180) -> pd.DataFrame:
    """
    Liefert Brent-Schlusskurse der letzten `days` Tage als DataFrame.

    Spalten:
        date  (datetime) — Handelstag
        price (float)    — Schlusskurs in USD/Barrel
    """
    end = datetime.today()
    start = end - timedelta(days=days + 10)  # etwas Puffer für Wochenenden
    ticker = yf.Ticker(BRENT_TICKER)
    df = ticker.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
    if df.empty:
        return pd.DataFrame(columns=["date", "price"])
    df = df[["Close"]].reset_index()
    df.columns = ["date", "price"]
    df = df.dropna(subset=["price"])  # Entferne Tage ohne Daten (z.B. noch offene Märkte)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()
    df = df.sort_values("date").tail(days).reset_index(drop=True)
    return df


def get_brent_current() -> dict:
    """
    Liefert aktuellen Brent-Preis und Änderung zum Vortag.

    Returns:
        {"price": float, "change_pct": float, "date": str}
    """
    df = get_brent_history(days=5)
    if len(df) < 2:
        return {"price": None, "change_pct": None, "date": None}
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    change_pct = (latest["price"] - prev["price"]) / prev["price"] * 100
    return {
        "price": round(float(latest["price"]), 2),
        "change_pct": round(float(change_pct), 2),
        "date": str(latest["date"].date()),
    }


def get_brent_trend(days: int = 5) -> str:
    """
    Kurzfristiger Trend der letzten `days` Tage.
    Returns: 'steigend', 'fallend', 'stabil'
    """
    df = get_brent_history(days=days + 5)
    if len(df) < days:
        return "unbekannt"
    recent = df.tail(days)
    change = (recent.iloc[-1]["price"] - recent.iloc[0]["price"]) / recent.iloc[0]["price"] * 100
    if change > 2:
        return "steigend"
    elif change < -2:
        return "fallend"
    else:
        return "stabil"
