"""Weather client tests: coordinate helpers (D7), archive request
building, hourly parsing, bounded retries, and stop conditions.

All HTTP is mocked with `responses`; all coordinates are deliberately
fake.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses

from running_pipeline.config import Settings
from running_pipeline.weather_client import (
    ARCHIVE_URL,
    WeatherApiError,
    WeatherBudgetExhausted,
    WeatherClient,
    WeatherRateLimitExceeded,
    location_key,
    normalize_coordinate,
    parse_hourly_rows,
)

LAT = Decimal("12.35")
LON = Decimal("-56.79")
START = date(2026, 6, 15)
END = date(2026, 6, 16)
FETCHED_AT = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)

UNITS = {
    "time": "iso8601",
    "temperature_2m": "°C",
    "apparent_temperature": "°C",
    "relative_humidity_2m": "%",
    "wind_speed_10m": "km/h",
    "wind_direction_10m": "°",
}


def make_settings(**overrides) -> Settings:
    defaults = {
        "strava_client_id": "12345",
        "strava_client_secret": "test-client-secret",
        "postgres_password": "test-db-password",
    }
    defaults.update(overrides)
    # _env_file=None keeps tests hermetic: never read the developer's .env.
    return Settings(_env_file=None, **defaults)


def make_client(sleeps: list[float] | None = None, **overrides) -> WeatherClient:
    sink = [] if sleeps is None else sleeps
    return WeatherClient(make_settings(**overrides), sleep=sink.append)


def archive_payload(times: list[str], **variable_overrides) -> dict:
    count = len(times)
    hourly = {
        "time": times,
        "temperature_2m": [20.1] * count,
        "apparent_temperature": [19.0] * count,
        "relative_humidity_2m": [55] * count,
        "wind_speed_10m": [12.3] * count,
        "wind_direction_10m": [180.0] * count,
    }
    hourly.update(variable_overrides)
    return {"latitude": 12.375, "longitude": -56.75, "hourly_units": UNITS, "hourly": hourly}


def query_of(call) -> dict:
    return parse_qs(urlparse(call.request.url).query)


# ── Coordinate normalization (D7: 2-dp, ~1.1 km cell) ─────────────────


def test_rounds_to_two_decimal_places():
    assert normalize_coordinate(12.346) == Decimal("12.35")
    assert normalize_coordinate(-56.789) == Decimal("-56.79")


def test_keeps_two_decimals_in_text_form():
    # The Decimal must render exactly as it appears inside location_key.
    assert str(normalize_coordinate(12.3)) == "12.30"
    assert str(normalize_coordinate(7)) == "7.00"


def test_negative_zero_collapses_to_zero():
    assert normalize_coordinate(-0.004) == Decimal("0.00")
    assert str(normalize_coordinate(-0.004)) == "0.00"


def test_location_key_format():
    assert location_key(12.346, -56.789) == "12.35_-56.79"
    assert location_key(0.0, -0.001) == "0.00_0.00"


# ── Request building ──────────────────────────────────────────────────


@responses.activate
def test_request_carries_utc_timezone_cell_and_variables():
    responses.get(ARCHIVE_URL, json=archive_payload(["2026-06-15T00:00"]))

    make_client().fetch_hourly(LAT, LON, START, END)

    query = query_of(responses.calls[0])
    assert query["latitude"] == ["12.35"]
    assert query["longitude"] == ["-56.79"]
    assert query["start_date"] == ["2026-06-15"]
    assert query["end_date"] == ["2026-06-16"]
    assert query["timezone"] == ["UTC"]
    assert query["hourly"] == [
        "temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_direction_10m"
    ]


# ── Hourly parsing ────────────────────────────────────────────────────


def test_parses_hourly_arrays_into_rows():
    payload = archive_payload(
        ["2026-06-15T09:00", "2026-06-15T10:00"],
        temperature_2m=[21.4, 22.0],
        relative_humidity_2m=[60, 58],
    )

    rows = parse_hourly_rows(payload, LAT, LON, FETCHED_AT)

    assert len(rows) == 2
    first = rows[0]
    # Key and numerics come from the requested cell, not the echoed
    # grid-snapped coordinates (12.375/-56.75 in the payload).
    assert first["location_key"] == "12.35_-56.79"
    assert (first["latitude"], first["longitude"]) == (LAT, LON)
    assert first["weather_timestamp"] == datetime(2026, 6, 15, 9, tzinfo=UTC)
    assert first["temperature_c"] == 21.4
    assert first["apparent_temperature_c"] == 19.0
    assert first["relative_humidity_pct"] == 60
    assert first["wind_speed_kph"] == 12.3
    assert first["wind_direction_deg"] == 180.0
    assert first["fetched_at"] == FETCHED_AT
    assert first["payload"] == {
        "time": "2026-06-15T09:00",
        "temperature_2m": 21.4,
        "apparent_temperature": 19.0,
        "relative_humidity_2m": 60,
        "wind_speed_10m": 12.3,
        "wind_direction_10m": 180.0,
        "units": UNITS,
    }
    assert rows[1]["weather_timestamp"] == datetime(2026, 6, 15, 10, tzinfo=UTC)


def test_api_nulls_stay_none_never_zero():
    payload = archive_payload(
        ["2026-07-04T09:00"],  # an hour the archive has no data for: all nulls
        temperature_2m=[None],
        apparent_temperature=[None],
        relative_humidity_2m=[None],
        wind_speed_10m=[None],
        wind_direction_10m=[None],
    )

    (row,) = parse_hourly_rows(payload, LAT, LON, FETCHED_AT)

    assert row["temperature_c"] is None
    assert row["apparent_temperature_c"] is None
    assert row["relative_humidity_pct"] is None
    assert row["wind_speed_kph"] is None
    assert row["wind_direction_deg"] is None
    assert row["payload"]["temperature_2m"] is None  # missing preserved as null
    # The key is present even when null: its presence is the D29 cache
    # marker for "direction was requested for this hour".
    assert "wind_direction_10m" in row["payload"]
    assert row["payload"]["wind_direction_10m"] is None


def test_zero_direction_survives_as_zero():
    # D29: 0° legitimately means "wind from due north" — it must never
    # collapse into the missing (None) representation.
    payload = archive_payload(["2026-06-15T09:00"], wind_direction_10m=[0.0])

    (row,) = parse_hourly_rows(payload, LAT, LON, FETCHED_AT)

    assert row["wind_direction_deg"] == 0.0
    assert row["wind_direction_deg"] is not None


# ── Bounded retries and stop conditions ───────────────────────────────


@responses.activate
def test_transient_5xx_retries_then_succeeds():
    responses.get(ARCHIVE_URL, status=502)
    responses.get(ARCHIVE_URL, json=archive_payload(["2026-06-15T00:00"]))
    sleeps: list[float] = []

    payload = make_client(sleeps).fetch_hourly(LAT, LON, START, END)

    assert payload["hourly"]["time"] == ["2026-06-15T00:00"]
    assert sleeps == [1]


@responses.activate
def test_connection_error_retries_then_succeeds():
    responses.get(ARCHIVE_URL, body=requests.ConnectionError("boom"))
    responses.get(ARCHIVE_URL, json=archive_payload(["2026-06-15T00:00"]))
    sleeps: list[float] = []

    make_client(sleeps).fetch_hourly(LAT, LON, START, END)

    assert sleeps == [1]


@responses.activate
def test_persistent_5xx_exhausts_bounded_attempts():
    for _ in range(4):
        responses.get(ARCHIVE_URL, status=503)
    sleeps: list[float] = []

    with pytest.raises(WeatherApiError, match="after 4 attempts"):
        make_client(sleeps).fetch_hourly(LAT, LON, START, END)

    assert sleeps == [1, 2, 4]


@responses.activate
def test_429_stops_cleanly_without_retry():
    responses.get(ARCHIVE_URL, status=429, json={"error": True, "reason": "Daily limit"})

    with pytest.raises(WeatherRateLimitExceeded, match="Daily limit"):
        make_client().fetch_hourly(LAT, LON, START, END)

    assert len(responses.calls) == 1


@responses.activate
def test_4xx_error_surfaces_api_reason():
    responses.get(
        ARCHIVE_URL, status=400, json={"error": True, "reason": "Parameter 'hourly' is invalid"}
    )

    with pytest.raises(WeatherApiError, match="Parameter 'hourly' is invalid"):
        make_client().fetch_hourly(LAT, LON, START, END)


@responses.activate
def test_budget_stops_before_the_request_that_would_exceed_it():
    responses.get(ARCHIVE_URL, json=archive_payload(["2026-06-15T00:00"]))
    responses.get(ARCHIVE_URL, json=archive_payload(["2026-06-16T00:00"]))
    client = make_client(weather_request_budget=2)

    client.fetch_hourly(LAT, LON, START, START)
    client.fetch_hourly(LAT, LON, END, END)
    with pytest.raises(WeatherBudgetExhausted, match="2/2"):
        client.fetch_hourly(LAT, LON, START, END)

    assert len(responses.calls) == 2  # the third request was never sent
    assert client.requests_made == 2
