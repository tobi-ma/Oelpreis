"""
Lokale Händlerpreise für PLZ 57258 (Freudenberg) via heizoel24.de.

Nutzt den internen Kalkulations-API-Endpunkt, der beim Bestellformular
der Seite verwendet wird. Der Endpoint ist zwar undokumentiert,
funktioniert aber zuverlässig mit einer Standard-Session.
"""

import requests
import pandas as pd

BASE_URL = "https://www.heizoel24.de"

# Parameter, die das Browser-Formular standardmäßig sendet
_DEFAULT_PARAMETERS = [
    {"Key": "MaxDelivery", "Id": 5, "Modifier": -1, "DesiredDate": None,
     "Name": "maximal", "ShortName": None, "DisplayName": "max. Lieferfrist",
     "CalculatorName": "siehe Angebot", "SubText": None, "InfoText": None,
     "OrderText": None, "IconKey": None, "HasSpecialView": False,
     "IsUpselling": False, "IsNew": False, "BlackList": [],
     "Selected": True, "HasSubItems": False, "UseIcon": False},
    {"Key": "DeliveryTimeWholeDay", "Id": 24, "Modifier": -1, "DesiredDate": None,
     "Name": "ganztägig möglich (7-18 Uhr)", "ShortName": None, "DisplayName": None,
     "CalculatorName": None, "SubText": None, "InfoText": None, "OrderText": None,
     "IconKey": None, "HasSpecialView": False, "IsUpselling": False, "IsNew": False,
     "BlackList": [], "Selected": True, "HasSubItems": False, "UseIcon": False},
    {"Key": None, "Id": -2, "Modifier": -1, "DesiredDate": None,
     "Name": "alle", "ShortName": "alle", "DisplayName": "alle", "CalculatorName": "alle",
     "SubText": None, "InfoText": None, "OrderText": None, "IconKey": None,
     "HasSpecialView": False, "IsUpselling": False, "IsNew": False, "BlackList": [],
     "Selected": True, "HasSubItems": False, "UseIcon": False},
    {"Key": "TruckBigTrailer", "Id": 11, "Modifier": -1, "DesiredDate": None,
     "Name": "mit Hänger", "ShortName": "groß", "DisplayName": "TKW mit Hänger",
     "CalculatorName": "mit Hänger", "SubText": None, "InfoText": None,
     "OrderText": None, "IconKey": None, "HasSpecialView": False,
     "IsUpselling": False, "IsNew": False, "BlackList": [],
     "Selected": True, "HasSubItems": False, "UseIcon": True},
    {"Key": "TubeLength40m", "Id": 9, "Modifier": -1, "DesiredDate": None,
     "Name": "bis 40m", "ShortName": "40m", "DisplayName": None, "CalculatorName": None,
     "SubText": None, "InfoText": None, "OrderText": None, "IconKey": None,
     "HasSpecialView": False, "IsUpselling": False, "IsNew": False, "BlackList": [],
     "Selected": True, "HasSubItems": False, "UseIcon": False},
]

DEFAULT_PLZ = "57258"
DEFAULT_LITERS = 3000


def _make_session() -> requests.Session:
    """Erstellt eine Session mit Browser-ähnlichen Headers und Session-Cookie."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9",
        "Content-Type": "application/json",
        "Referer": "https://www.heizoel24.de/bestellung",
        "Origin": "https://www.heizoel24.de",
    })
    # Session-Cookie holen (notwendig für API-Zugang)
    session.get(f"{BASE_URL}/session/renew", timeout=10)
    session.get(f"{BASE_URL}/api/kalkulation/init/1/1", timeout=10)
    return session


def get_local_quotes(plz: str = DEFAULT_PLZ, liters: int = DEFAULT_LITERS) -> pd.DataFrame:
    """
    Händlerpreise für eine bestimmte PLZ via heizoel24.de Kalkulations-API.

    Spalten:
        dealer       (str)   — Händlername
        price        (float) — Preis in ct/Liter
        total        (float) — Gesamtpreis in EUR
        rating       (int)   — Bewertung (0–100)
        rating_count (int)   — Anzahl Bewertungen
        url          (str)   — Link zum Händlerprofil
    """
    payload = {
        "ZipCode": plz,
        "Amount": liters,
        "Stations": 1,
        "Product": {"Id": 1, "ClimateNeutral": False},
        "Parameters": _DEFAULT_PARAMETERS,
        "CountryId": 1,
        "ProductGroupId": 1,
        "AppointmentPlus": False,
        "Ordering": 0,
        "UpsellCount": 0,
    }

    try:
        session = _make_session()
        resp = session.post(
            f"{BASE_URL}/api/kalkulation/berechnen",
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return pd.DataFrame(columns=["dealer", "price", "total", "rating", "rating_count", "url"])

    items = data.get("Items", [])
    if not items:
        return pd.DataFrame(columns=["dealer", "price", "total", "rating", "rating_count", "url"])

    rows = []
    for item in items:
        profile = item.get("ProfileLink") or ""
        url = f"{BASE_URL}{profile}" if profile else BASE_URL
        rows.append({
            "dealer": item.get("Name", "Unbekannt"),
            "price": round(float(item["UnitPrice"]), 2),
            "total": round(float(item["TotalPrice"]), 2),
            "rating": item.get("Rating"),
            "rating_count": item.get("RatingCount"),
            "url": url,
        })

    df = pd.DataFrame(rows).sort_values("price").reset_index(drop=True)
    return df


def get_best_local_price(plz: str = DEFAULT_PLZ, liters: int = DEFAULT_LITERS) -> dict:
    """
    Günstigstes lokales Angebot.

    Returns:
        {"dealer": str, "price": float, "total": float, "url": str}
    """
    df = get_local_quotes(plz=plz, liters=liters)
    if df.empty:
        return {"dealer": None, "price": None, "total": None, "url": None}
    best = df.iloc[0]
    return {
        "dealer": best["dealer"],
        "price": best["price"],
        "total": best["total"],
        "url": best["url"],
    }


def get_comparison_links(plz: str = DEFAULT_PLZ, liters: int = DEFAULT_LITERS) -> list[dict]:
    """Direktlinks zu Vergleichsportalen als Fallback."""
    return [
        {
            "name": "heizoel24.de",
            "url": f"https://www.heizoel24.de/heizoel/angebotsliste?zipCode={plz}&amount={liters}&stations=1&product=1&options=5,24,-2,11,9&cn=0&ap=0",
            "description": "Händlervergleich direkt öffnen",
        },
        {
            "name": "esyoil.com",
            "url": f"https://www.esyoil.com/bestellung?plz={plz}&menge={liters}",
            "description": "Zweiter Vergleichsdienst",
        },
    ]
