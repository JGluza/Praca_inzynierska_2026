"""
Pobieranie danych Open-Meteo (archive) dla Wrocławia do ETL.
Zakres: 2015-01-01 .. 2025-11-30 (pełny listopad).
Zapis: JSONL (1 rekord = 1 linia), gotowe pod MongoDB.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Tuple

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry


@dataclass(frozen=True)
class Location:
    name: str
    lat: float
    lon: float


LOCATIONS: List[Location] = [
    Location("Wroclaw_Centrum",  51.1105, 17.0312),
    Location("Wroclaw_Lotnisko", 51.1025, 16.8858),  # okolice Strachowic (stacji IMGW)
    Location("Wroclaw_Biskupin", 51.1000, 17.1000),
    Location("Wroclaw_Lesnica",  51.1480, 16.8670),
    Location("Wroclaw_PsiePole", 51.1450, 17.1150),
]

URL = "https://archive-api.open-meteo.com/v1/archive"

START_DATE = date(2015, 1, 1)
END_DATE = min(date.today(), date(2025, 11, 30))  # pełny listopad, + nie wychodzimy w przyszłość

TIMEZONE = "UTC"

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
]

OUT_FILE = Path("wroclaw_openmeteo_hourly_2015_2025_11_30.jsonl")


def build_client() -> openmeteo_requests.Client:
    # Cache 30 dni: stabilniej przy ponownych uruchomieniach, mniej requestów.
    cache_session = requests_cache.CachedSession(".cache_openmeteo", expire_after=60 * 60 * 24 * 30)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.4)
    return openmeteo_requests.Client(session=retry_session)


def year_chunks(start: date, end: date) -> Iterable[Tuple[str, str]]:
    # Dzieli zakres na kawałki roczne (mniej ryzyka limitów i mniej RAM).
    for y in range(start.year, end.year + 1):
        chunk_start = date(y, 1, 1)
        chunk_end = date(y, 12, 31)
        if y == start.year:
            chunk_start = start
        if y == end.year:
            chunk_end = end
        yield chunk_start.isoformat(), chunk_end.isoformat()


def fetch_chunk(
    client: openmeteo_requests.Client,
    loc: Location,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    params = {
        "latitude": loc.lat,
        "longitude": loc.lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARS,
        "timezone": TIMEZONE,
    }

    responses = client.weather_api(URL, params=params)
    if not responses:
        raise RuntimeError(f"Brak odpowiedzi z API dla {loc.name} {start_date}..{end_date}")

    response = responses[0]
    hourly = response.Hourly()

    # Oś czasu (UTC)
    timestamps = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    # Metadane pod MongoDB (GeoJSON Point pod indeks 2dsphere)
    geometry = {"type": "Point", "coordinates": [float(loc.lon), float(loc.lat)]}

    data = {
        "timestamp": timestamps,
        "location": loc.name,
        "source": "open-meteo",
        "geometry": [geometry] * len(timestamps),
        "lat": float(loc.lat),
        "lon": float(loc.lon),
    }

    # Zmienne godzinowe
    for idx, var in enumerate(HOURLY_VARS):
        values = hourly.Variables(idx).ValuesAsNumpy()
        if len(values) != len(timestamps):
            raise ValueError(
                f"Niespójna długość serii dla {loc.name} {start_date}..{end_date}: "
                f"{var}: {len(values)} vs time: {len(timestamps)}"
            )
        data[var] = values

    df = pd.DataFrame(data)

    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def main() -> int:
    client = build_client()

    OUT_FILE.write_text("", encoding="utf-8")
    total = 0

    print(f"Zapis: {OUT_FILE}")
    print(f"Zakres: {START_DATE.isoformat()} .. {END_DATE.isoformat()}")
    print(f"Zmienne: {', '.join(HOURLY_VARS)}")
    print(f"Lokalizacje: {len(LOCATIONS)}")

    for loc in LOCATIONS:
        for start_str, end_str in year_chunks(START_DATE, END_DATE):
            print(f"-> {loc.name}: {start_str}..{end_str}")
            df = fetch_chunk(client, loc, start_str, end_str)

            with OUT_FILE.open("a", encoding="utf-8") as f:
                df.to_json(f, orient="records", lines=True, force_ascii=False)

            total += len(df)

    print(f"Gotowe. Rekordów (godzin): {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
