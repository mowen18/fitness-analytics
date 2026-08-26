"""Weather ingestion tests.

Unit tests cover the pure fetch planner and the engine with fakes for
every DB seam (Phase 1 style). The @integration tests run the engine
against the scratch database (fake client, real SQL) and map directly to
the Phase 2 acceptance criteria; they skip visibly when Postgres is down.

All coordinates are deliberately fake (12.34-style), never real cells.
"""

import json
import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from running_pipeline import weather_ingestion
from running_pipeline.config import Settings
from running_pipeline.weather_client import (
    WeatherApiError,
    WeatherBudgetExhausted,
)
from running_pipeline.weather_ingestion import (
    RunLocation,
    WeatherSyncReport,
    count_runs_without_location,
    plan_fetches,
    select_eligible_runs,
    sync_weather,
)

SYNC_START = date(2024, 1, 1)
CELL = "12.35_-56.79"
OTHER_CELL = "48.86_2.35"
HOUR = datetime(2026, 6, 15, 9, tzinfo=UTC)


def make_settings(**overrides) -> Settings:
    defaults = {
        "strava_client_id": "12345",
        "strava_client_secret": "test-client-secret",
        "postgres_password": "test-db-password",
    }
    defaults.update(overrides)
    # _env_file=None keeps tests hermetic: never read the developer's .env.
    return Settings(_env_file=None, **defaults)


def run_location(activity_id, hour=HOUR, cell=CELL) -> RunLocation:
    lat, lon = cell.split("_")
    return RunLocation(activity_id, hour, Decimal(lat), Decimal(lon), cell)


class FakeWeatherClient:
    """Serves synthetic full-day archive payloads for any requested range.

    `null_times` (ISO-hour strings) come back with null measurements;
    `fail_cells` raise WeatherApiError; `stop_at_call` raises a budget
    stop on that (1-based) call. `wind_direction` adds a
    wind_direction_10m array (null on `null_times` hours); by default
    the payload OMITS the array — the shape of an archive response that
    carries no direction (parse_hourly_rows still records the key as an
    explicit null, which is the D29 completeness marker).
    """

    def __init__(self, null_times=(), fail_cells=(), stop_at_call=None, wind_direction=None):
        self.null_times = set(null_times)
        self.fail_cells = set(fail_cells)
        self.stop_at_call = stop_at_call
        self.wind_direction = wind_direction
        self.calls = []
        self.requests_made = 0

    def fetch_hourly(self, latitude, longitude, start_date, end_date):
        cell = f"{latitude}_{longitude}"
        self.calls.append((cell, start_date, end_date))
        if self.stop_at_call == len(self.calls):
            raise WeatherBudgetExhausted(f"budget reached at call {len(self.calls)}")
        if cell in self.fail_cells:
            raise WeatherApiError(f"cell {cell} unavailable")
        self.requests_made += 1
        times = []
        day = start_date
        while day <= end_date:
            times.extend(f"{day.isoformat()}T{hour:02d}:00" for hour in range(24))
            day += timedelta(days=1)
        measurements = [None if t in self.null_times else 20.5 for t in times]
        hourly = {
            "time": times,
            "temperature_2m": measurements,
            "apparent_temperature": measurements,
            "relative_humidity_2m": measurements,
            "wind_speed_10m": measurements,
        }
        if self.wind_direction is not None:
            hourly["wind_direction_10m"] = [
                None if t in self.null_times else self.wind_direction for t in times
            ]
        return {"hourly_units": {"temperature_2m": "°C"}, "hourly": hourly}


class FakeConn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


# ── Fetch planning (pure) ─────────────────────────────────────────────


def test_cached_hours_with_data_are_skipped():
    plan = plan_fetches([run_location(1)], {(CELL, HOUR): True}, full=False, gap_days=7)

    assert plan.batches == []
    assert (plan.hours_needed, plan.hours_cached) == (0, 1)


def test_all_null_cached_hours_are_refetched():
    # A cached row without data is the explicit missing marker (the
    # archive had no data for the hour): keep re-requesting it until
    # the archive fills it.
    plan = plan_fetches([run_location(1)], {(CELL, HOUR): False}, full=False, gap_days=7)

    assert (plan.hours_needed, plan.hours_cached) == (1, 0)
    assert len(plan.batches) == 1


def test_full_refetches_even_cached_hours():
    plan = plan_fetches([run_location(1)], {(CELL, HOUR): True}, full=True, gap_days=7)

    assert (plan.hours_needed, plan.hours_cached) == (1, 0)


def test_same_cell_and_hour_deduplicates():
    plan = plan_fetches([run_location(1), run_location(2)], {}, full=False, gap_days=7)

    assert plan.hours_needed == 1
    (batch,) = plan.batches
    assert (batch.start_date, batch.end_date) == (HOUR.date(), HOUR.date())


def test_dates_merge_within_gap_and_split_beyond_it():
    runs = [
        run_location(1, datetime(2026, 6, 1, 9, tzinfo=UTC)),
        run_location(2, datetime(2026, 6, 5, 18, tzinfo=UTC)),
        run_location(3, datetime(2026, 7, 1, 7, tzinfo=UTC)),  # 26-day gap: own batch
    ]

    plan = plan_fetches(runs, {}, full=False, gap_days=7)

    assert [(b.start_date, b.end_date) for b in plan.batches] == [
        (date(2026, 6, 1), date(2026, 6, 5)),
        (date(2026, 7, 1), date(2026, 7, 1)),
    ]


def test_one_batch_per_location():
    runs = [run_location(1), run_location(2, cell=OTHER_CELL)]

    plan = plan_fetches(runs, {}, full=False, gap_days=7)

    assert [b.location_key for b in plan.batches] == sorted([CELL, OTHER_CELL])


# ── Engine behavior (fakes for every DB seam) ─────────────────────────


@pytest.fixture
def engine_fakes(monkeypatch):
    """Replace the DB-touching seams; returns the recorder dict."""
    state = {"runs": [], "without_location": 0, "cache": {}, "upserted": [], "still_missing": 0}
    monkeypatch.setattr(
        weather_ingestion, "select_eligible_runs", lambda conn, floor: state["runs"]
    )
    monkeypatch.setattr(
        weather_ingestion,
        "count_runs_without_location",
        lambda conn, floor: state["without_location"],
    )
    monkeypatch.setattr(weather_ingestion, "_load_cache_state", lambda conn, pairs: state["cache"])
    monkeypatch.setattr(
        weather_ingestion,
        "_upsert_hour",
        lambda conn, row: state["upserted"].append(row) or "inserted",
    )
    monkeypatch.setattr(
        weather_ingestion, "_count_still_missing", lambda conn, pairs: state["still_missing"]
    )
    return state


def test_report_counts_across_batches(engine_fakes):
    engine_fakes["runs"] = [run_location(1), run_location(2, cell=OTHER_CELL)]
    client = FakeWeatherClient()
    conn = FakeConn()

    report = sync_weather(make_settings(), client, conn)

    assert report == WeatherSyncReport(
        eligible_runs=2, hours_needed=2, requests_made=2, inserted=48
    )
    assert conn.commits == 2  # one per batch
    assert len(engine_fakes["upserted"]) == 48  # two full synthetic days


def test_failed_batch_logs_and_continues(engine_fakes, caplog):
    engine_fakes["runs"] = [run_location(1), run_location(2, cell=OTHER_CELL)]
    engine_fakes["still_missing"] = 1
    client = FakeWeatherClient(fail_cells={CELL})

    with caplog.at_level(logging.INFO):
        report = sync_weather(make_settings(), client, FakeConn())

    assert report.failed_batches == 1
    assert report.inserted == 24  # the other cell still landed
    assert report.stopped_early is False
    assert report.hours_still_missing == 1
    assert "continuing" in caplog.text


def test_budget_stop_keeps_committed_batches(engine_fakes, caplog):
    engine_fakes["runs"] = [run_location(1), run_location(2, cell=OTHER_CELL)]
    client = FakeWeatherClient(stop_at_call=2)
    conn = FakeConn()

    with caplog.at_level(logging.INFO):
        report = sync_weather(make_settings(), client, conn)

    assert report.stopped_early is True
    assert report.inserted == 24
    assert conn.commits == 1  # the completed batch stayed committed
    assert "stopped early" in caplog.text
    assert "committed batches kept" in caplog.text


def test_indoor_runs_are_counted_explicitly(engine_fakes, caplog):
    engine_fakes["without_location"] = 5
    client = FakeWeatherClient()

    with caplog.at_level(logging.INFO):
        report = sync_weather(make_settings(), client, FakeConn())

    assert report == WeatherSyncReport(runs_without_location=5)
    assert client.calls == []
    assert "runs_without_location=5" in caplog.text


# ── Integration: DDL, extraction, and the acceptance criteria ─────────


@pytest.fixture
def db(integration_db):
    integration_db.execute(
        "TRUNCATE raw_strava.activities, raw_strava.activity_coordinates, raw_weather.hourly"
    )
    integration_db.commit()
    return integration_db


def insert_activity(
    conn,
    activity_id,
    *,
    sport_type="Run",
    start="2026-06-15T09:47:23Z",
    start_latlng=(12.34, -56.78),
    trainer=False,
):
    payload = {
        "id": activity_id,
        "sport_type": sport_type,
        "start_date": start,
        "start_latlng": list(start_latlng) if start_latlng else [],
        "trainer": trainer,
    }
    conn.execute(
        """
        INSERT INTO raw_strava.activities
            (activity_id, start_date_utc, activity_type, payload, fetched_at)
        VALUES (%s, %s, %s, %s, now())
        """,
        (activity_id, datetime.fromisoformat(start), sport_type, json.dumps(payload)),
    )


def hourly_row(location=CELL, hour="2026-06-15T09:00:00+00:00", temperature="21.4"):
    return (
        location,
        Decimal(location.split("_")[0]),
        Decimal(location.split("_")[1]),
        datetime.fromisoformat(hour),
        temperature,
        json.dumps({"time": hour}),
    )


_INSERT_HOURLY = """
    INSERT INTO raw_weather.hourly
        (location_key, latitude, longitude, weather_timestamp, temperature_c, payload, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, now())
"""


@pytest.mark.integration
def test_location_hour_unique_key_rejects_duplicates(db):
    db.execute(_INSERT_HOURLY, hourly_row())
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(_INSERT_HOURLY, hourly_row(temperature="99.9"))
    db.rollback()


@pytest.mark.integration
def test_extraction_keeps_outdoor_runs_and_excludes_the_rest(db):
    insert_activity(db, 1)  # outdoor Run — kept
    # Distinct start: sharing activity 1's default timestamp made the
    # [1, 2, 8] assertion ride on unspecified tie order (the flake the
    # v1.4 verification session caught) — order now carried by time.
    insert_activity(db, 2, sport_type="TrailRun", start="2026-06-15T10:47:23Z")  # kept
    insert_activity(db, 3, trainer=True)  # treadmill — excluded
    insert_activity(db, 4, start_latlng=None)  # no coordinates — excluded
    insert_activity(db, 5, sport_type="Walk")  # not a run — excluded
    insert_activity(db, 6, sport_type="VirtualRun")  # indoor by type — excluded
    insert_activity(db, 7, start="2023-12-31T23:59:59Z")  # before D5 floor — excluded
    # Map-privacy fallback: no payload coordinates, resolved row — kept.
    insert_activity(db, 8, start="2026-06-16T08:00:00Z", start_latlng=None)
    db.execute(
        "INSERT INTO raw_strava.activity_coordinates VALUES "
        "(8, 22.446, 33.786, 'map_polyline', now())"
    )
    # D23 ride types (v2.0 Phase C1): outdoor rides are weather-eligible;
    # VirtualRide is indoor by type (mirrors VirtualRun), trainer rides
    # and e-bike types stay out. Distinct starts, same flake rule.
    insert_activity(db, 9, sport_type="Ride", start="2026-06-17T08:00:00Z")  # kept
    insert_activity(db, 10, sport_type="GravelRide", start="2026-06-17T09:00:00Z")  # kept
    insert_activity(db, 11, sport_type="MountainBikeRide", start="2026-06-17T10:00:00Z")  # kept
    insert_activity(db, 12, sport_type="VirtualRide", start="2026-06-17T11:00:00Z")  # excluded
    # Trainer ride — excluded.
    insert_activity(db, 13, sport_type="Ride", trainer=True, start="2026-06-17T12:00:00Z")
    insert_activity(db, 14, sport_type="EBikeRide", start="2026-06-17T13:00:00Z")  # excluded
    db.commit()

    runs = select_eligible_runs(db, SYNC_START)

    assert [run.activity_id for run in runs] == [1, 2, 8, 9, 10, 11]
    resolved = runs[2]  # activity 8, the map-privacy resolved row
    assert resolved.location_key == "22.45_33.79"  # D7-normalized from the resolved row


@pytest.mark.integration
def test_extraction_normalizes_coordinates_and_truncates_to_hour(db):
    insert_activity(db, 1, start="2026-06-15T09:47:23Z", start_latlng=(12.346, -56.789))
    db.commit()

    (run,) = select_eligible_runs(db, SYNC_START)

    assert run.start_hour_utc == datetime(2026, 6, 15, 9, tzinfo=UTC)
    assert run.latitude == Decimal("12.35")
    assert run.longitude == Decimal("-56.79")
    assert run.location_key == "12.35_-56.79"


@pytest.mark.integration
def test_ineligible_count_covers_indoor_and_coordless_activities_only(db):
    insert_activity(db, 1)  # eligible — not counted
    insert_activity(db, 2, trainer=True)  # counted
    insert_activity(db, 3, start_latlng=None)  # counted
    insert_activity(db, 4, sport_type="Walk", trainer=True)  # not a run — not counted
    insert_activity(db, 5, trainer=True, start="2023-06-01T08:00:00Z")  # pre-floor
    # D23 rides follow the same buckets: trainer/coordless counted,
    # VirtualRide and e-bike types outside the population entirely
    # (neither eligible nor counted — the VirtualRun/Walk behavior).
    insert_activity(db, 6, sport_type="Ride", trainer=True)  # counted
    insert_activity(db, 7, sport_type="GravelRide", start_latlng=None)  # counted
    insert_activity(db, 8, sport_type="VirtualRide")  # not counted
    insert_activity(db, 9, sport_type="EBikeRide")  # not counted
    db.commit()

    assert count_runs_without_location(db, SYNC_START) == 4


# Criteria 1 + 2: every eligible run has a matching or explicitly-missing
# record, and it represents the run's start hour, not its date.
@pytest.mark.integration
def test_every_run_gets_matching_or_explicitly_missing_start_hour_row(db):
    insert_activity(db, 1, start="2026-06-15T09:47:23Z")
    insert_activity(db, 2, start="2026-07-04T05:30:00Z")  # its hour returns all-NULL
    db.commit()
    client = FakeWeatherClient(null_times={"2026-07-04T05:00"})

    report = sync_weather(make_settings(), client, db)

    matched = db.execute(
        "SELECT temperature_c FROM raw_weather.hourly "
        "WHERE location_key = '12.34_-56.78' AND weather_timestamp = %s",
        (datetime(2026, 6, 15, 9, tzinfo=UTC),),
    ).fetchone()
    assert matched == (Decimal("20.5"),)  # the 09:00 hour, not midnight

    missing = db.execute(
        "SELECT temperature_c, payload->'temperature_2m' FROM raw_weather.hourly "
        "WHERE location_key = '12.34_-56.78' AND weather_timestamp = %s",
        (datetime(2026, 7, 4, 5, tzinfo=UTC),),
    ).fetchone()
    assert missing == (None, None)  # explicit NULL row: missing, never zero
    assert report.hours_still_missing == 1


# Criterion 3: backfill re-runs create no duplicates.
@pytest.mark.integration
def test_full_rerun_creates_no_duplicates(db):
    insert_activity(db, 1)
    db.commit()
    settings = make_settings()

    first = sync_weather(settings, FakeWeatherClient(), db)
    second = sync_weather(settings, FakeWeatherClient(), db, full=True)

    assert first.inserted == 24
    assert (second.inserted, second.updated, second.skipped) == (0, 0, 24)
    assert db.execute("SELECT count(*) FROM raw_weather.hourly").fetchone()[0] == 24


# Criterion 4: repeated runs in similar locations hit the cache.
@pytest.mark.integration
def test_repeated_run_in_same_cell_and_hour_makes_no_requests(db):
    insert_activity(db, 1, start="2026-06-15T09:47:23Z")
    db.commit()
    settings = make_settings()
    sync_weather(settings, FakeWeatherClient(), db)

    insert_activity(db, 2, start="2026-06-15T09:05:00Z")  # same cell, same hour
    db.commit()
    client = FakeWeatherClient()
    report = sync_weather(settings, client, db)

    assert client.calls == []
    assert (report.hours_needed, report.hours_cached) == (0, 1)


# ── D29 wind direction (v2.0 Phase C3): typed landing + the two
#    cache-completeness arms ────────────────────────────────────────────


@pytest.mark.integration
def test_synced_hour_lands_wind_direction_in_typed_column(db):
    insert_activity(db, 1, start="2026-06-15T09:47:23Z")
    db.commit()

    sync_weather(make_settings(), FakeWeatherClient(wind_direction=270.0), db)

    (direction,) = db.execute(
        "SELECT wind_direction_deg FROM raw_weather.hourly "
        "WHERE location_key = '12.34_-56.78' AND weather_timestamp = %s",
        (datetime(2026, 6, 15, 9, tzinfo=UTC),),
    ).fetchone()
    assert direction == Decimal("270")


@pytest.mark.integration
def test_pre_migration_row_with_data_is_refetched_and_completed(db):
    # D29 completeness, arm 1: a pre-migration row (measurements
    # present, direction NULL, payload without the wind_direction_10m
    # key) must become fetch-eligible again — data alone no longer
    # means complete. The fixture direction is exactly 0.0 so the same
    # test pins 0° = north surviving as a value through the upsert AND
    # satisfying completeness (a 0-coerced-to-missing bug would loop).
    insert_activity(db, 1, start="2026-06-15T09:47:23Z")
    db.execute(_INSERT_HOURLY, hourly_row(location="12.34_-56.78"))
    db.commit()
    settings = make_settings()

    client = FakeWeatherClient(wind_direction=0.0)
    sync_weather(settings, client, db)

    assert len(client.calls) == 1  # the pre-migration hour was re-planned
    (direction,) = db.execute(
        "SELECT wind_direction_deg FROM raw_weather.hourly "
        "WHERE location_key = '12.34_-56.78' AND weather_timestamp = %s",
        (datetime(2026, 6, 15, 9, tzinfo=UTC),),
    ).fetchone()
    assert direction == Decimal("0")
    assert direction is not None  # a value, never coerced to missing

    again = FakeWeatherClient(wind_direction=0.0)
    report = sync_weather(settings, again, db)
    assert again.calls == []  # 0° is complete: the queue terminated
    assert (report.hours_needed, report.hours_cached) == (0, 1)


@pytest.mark.integration
def test_direction_less_archive_hour_completes_on_first_fetch(db):
    # D29 completeness, arm 2 (termination): an hour whose archive
    # response carries no direction must be marked resolved on first
    # fetch — the payload records wind_direction_10m as an explicit
    # null — or the planner would re-request the same hours forever.
    # Red proof by mutation (recorded in the commit body): with a naive
    # `wind_direction_deg IS NOT NULL`-only clause, the second sync
    # below repeats the fetch — the livelock this test pins.
    insert_activity(db, 1, start="2026-06-15T09:47:23Z")
    db.commit()
    settings = make_settings()

    sync_weather(settings, FakeWeatherClient(), db)  # no direction array served

    row = db.execute(
        "SELECT wind_direction_deg, payload ? 'wind_direction_10m', "
        "payload -> 'wind_direction_10m' FROM raw_weather.hourly "
        "WHERE location_key = '12.34_-56.78' AND weather_timestamp = %s",
        (datetime(2026, 6, 15, 9, tzinfo=UTC),),
    ).fetchone()
    assert row == (None, True, None)  # explicit null direction, key present

    again = FakeWeatherClient()
    report = sync_weather(settings, again, db)
    assert again.calls == []
    assert (report.hours_needed, report.hours_cached) == (0, 1)


# Criterion 5: missing coordinates or unavailable weather never fail the
# whole pipeline.
@pytest.mark.integration
def test_indoor_only_history_completes_with_zero_requests(db):
    insert_activity(db, 1, trainer=True, start_latlng=None)
    insert_activity(db, 2, sport_type="Walk", trainer=True, start_latlng=None)
    db.commit()
    client = FakeWeatherClient()

    report = sync_weather(make_settings(), client, db)

    assert report.eligible_runs == 0
    assert report.runs_without_location == 1  # the treadmill Run, not the Walk
    assert client.calls == []
    assert report.stopped_early is False


@pytest.mark.integration
def test_failed_cell_does_not_block_other_cells(db):
    insert_activity(db, 1, start_latlng=(12.34, -56.78))
    insert_activity(db, 2, start_latlng=(48.86, 2.35))
    db.commit()
    client = FakeWeatherClient(fail_cells={"12.34_-56.78"})

    report = sync_weather(make_settings(), client, db)

    assert report.failed_batches == 1
    assert report.inserted == 24  # the healthy cell committed
    assert report.hours_still_missing == 1
    landed = db.execute("SELECT DISTINCT location_key FROM raw_weather.hourly").fetchall()
    assert landed == [("48.86_2.35",)]
