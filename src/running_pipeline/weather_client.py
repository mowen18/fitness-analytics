"""Open-Meteo historical-archive client: coordinate normalization (D7),
archive requests, and hourly-array parsing.

No API key and no credentials — nothing weather-related lives in .env.
The free tier allows ~10,000 requests/day; a per-sync request budget plus
429 handling keep usage far below it (the same stop-cleanly contract the
Strava client applies to its rate limits).

Requests always pass timezone=UTC, so returned hourly timestamps are UTC
wall-clock and are stored as timestamptz without conversion.
"""

import json
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal

import requests

from running_pipeline.config import Settings

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# D8 (amended v2.0) hourly variables, in raw_weather.hourly column
# order. Open-Meteo's default units already match the columns: °C, °C,
# %, km/h, ° (wind_direction_10m: meteorological FROM convention, 0 =
# north — a value, never a stand-in for missing; D29).
HOURLY_VARIABLES = (
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
)

REQUEST_TIMEOUT_SECONDS = 30
# Backoff between retries of transient failures (connection errors, 5xx);
# one initial attempt plus one retry per entry.
TRANSIENT_RETRY_BACKOFF_SECONDS = (1, 2, 4)


class WeatherApiError(RuntimeError):
    """One archive request failed (after bounded retries when transient)."""


class WeatherStop(RuntimeError):
    """Stop the sync cleanly: keep committed batches, fetch the rest later."""


class WeatherRateLimitExceeded(WeatherStop):
    """Open-Meteo returned 429; the free-tier limit is exhausted."""


class WeatherBudgetExhausted(WeatherStop):
    """This sync reached WEATHER_REQUEST_BUDGET; a later run picks up the rest."""


def normalize_coordinate(value: float) -> Decimal:
    """Round to 2 decimal places per D7 (~1.1 km cell).

    Goes through the fixed-point string so the location_key text and the
    stored numeric columns can never disagree; "-0.00" collapses to "0.00".
    """
    text = f"{float(value):.2f}"
    if text == "-0.00":
        text = "0.00"
    return Decimal(text)


def location_key(latitude: float, longitude: float) -> str:
    """'{lat_2dp}_{lon_2dp}' per D7."""
    return f"{normalize_coordinate(latitude)}_{normalize_coordinate(longitude)}"


class WeatherClient:
    def __init__(self, settings: Settings, sleep: Callable[[float], None] = time.sleep):
        self._budget = settings.weather_request_budget
        self._session = requests.Session()
        self._sleep = sleep  # injectable so tests don't wait out backoffs
        self.requests_made = 0  # every HTTP attempt counts, retries included

    def fetch_hourly(
        self, latitude: Decimal, longitude: Decimal, start_date: date, end_date: date
    ) -> dict:
        """Hourly archive data for one 2-dp cell over [start_date, end_date] (UTC).

        Transient failures (connection errors, timeouts, 5xx) retry with
        bounded backoff; 429 and an exhausted request budget raise
        WeatherStop subclasses so the sync stops cleanly; the remaining
        failures raise WeatherApiError.
        """
        cell = f"{latitude}_{longitude}"
        params = {
            "latitude": str(latitude),
            "longitude": str(longitude),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "UTC",
        }
        remaining_backoff = list(TRANSIENT_RETRY_BACKOFF_SECONDS)
        max_attempts = 1 + len(TRANSIENT_RETRY_BACKOFF_SECONDS)
        while True:
            if self.requests_made >= self._budget:
                raise WeatherBudgetExhausted(
                    f"Stopping at the per-sync request budget "
                    f"({self.requests_made}/{self._budget} requests used)."
                )
            self.requests_made += 1
            try:
                response = self._session.get(
                    ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                if not remaining_backoff:
                    raise WeatherApiError(
                        f"Open-Meteo unreachable after {max_attempts} attempts "
                        f"for cell {cell}: {exc.__class__.__name__}"
                    ) from exc
                self._sleep(remaining_backoff.pop(0))
                continue

            if response.status_code >= 500:
                if not remaining_backoff:
                    raise WeatherApiError(
                        f"Open-Meteo failed after {max_attempts} attempts "
                        f"(HTTP {response.status_code}) for cell {cell}: "
                        f"{_error_reason(response)}"
                    )
                self._sleep(remaining_backoff.pop(0))
                continue
            if response.status_code == 429:
                raise WeatherRateLimitExceeded(
                    f"Open-Meteo rate limit exceeded (HTTP 429): {_error_reason(response)}"
                )
            if response.status_code != 200:
                raise WeatherApiError(
                    f"Open-Meteo request failed (HTTP {response.status_code}) "
                    f"for cell {cell}: {_error_reason(response)}"
                )
            return response.json()


def parse_hourly_rows(
    payload: dict, latitude: Decimal, longitude: Decimal, fetched_at: datetime
) -> list[dict]:
    """One upsert-ready row dict per returned hour.

    The key comes from the *requested* 2-dp coordinates, never the echoed
    ones (the API snaps to its own grid, which would break D7 cell
    identity). API nulls stay None — missing, never zero. Each row's
    payload preserves that hour's raw values plus the response units.
    """
    hourly = payload["hourly"]
    times = hourly["time"]
    units = payload.get("hourly_units", {})
    key = f"{latitude}_{longitude}"
    variable_columns = {
        "temperature_2m": "temperature_c",
        "apparent_temperature": "apparent_temperature_c",
        "relative_humidity_2m": "relative_humidity_pct",
        "wind_speed_10m": "wind_speed_kph",
        "wind_direction_10m": "wind_direction_deg",
    }
    rows = []
    for index, time_text in enumerate(times):
        raw = {name: hourly.get(name, [None] * len(times))[index] for name in HOURLY_VARIABLES}
        rows.append(
            {
                "location_key": key,
                "latitude": latitude,
                "longitude": longitude,
                # timezone=UTC was requested, so the naive strings are UTC.
                "weather_timestamp": datetime.fromisoformat(time_text).replace(tzinfo=UTC),
                **{column: raw[name] for name, column in variable_columns.items()},
                "payload": {"time": time_text, **raw, "units": units},
                "fetched_at": fetched_at,
            }
        )
    return rows


def _error_reason(response: requests.Response) -> str:
    """Open-Meteo error bodies are {"error": true, "reason": "..."}."""
    try:
        body = response.json()
    except ValueError:
        return "<non-JSON error body>"
    if isinstance(body, dict) and "reason" in body:
        return str(body["reason"])
    return json.dumps(body)
