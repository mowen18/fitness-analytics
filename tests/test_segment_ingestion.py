"""Segment-effort ingestion tests (D24, Phase C2).

Client fetch behavior is mocked HTTP; engine behavior runs with a fake
client against the scratch database (@integration), mapped to the D24
contract: resumability via status rows (no watermark), distinct
statuses, convergence on re-run, clean rate-limit stops, and the
per-invocation cap.
"""

import json
import logging
import time
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import psycopg
import pytest
import responses

from running_pipeline.config import Settings
from running_pipeline.segment_ingestion import SegmentSyncReport, sync_segment_efforts
from running_pipeline.strava_client import (
    API_BASE,
    RateLimitExceeded,
    RateLimitStatus,
    StravaApiError,
    StravaClient,
    TokenSet,
    TokenStore,
)


def make_settings(tmp_path, **overrides) -> Settings:
    defaults = {
        "strava_client_id": "12345",
        "strava_client_secret": "test-client-secret",
        "strava_refresh_token": "env-bootstrap-token",
        "postgres_password": "test-db-password",
        "token_file": tmp_path / "strava_tokens.json",
    }
    defaults.update(overrides)
    # _env_file=None keeps tests hermetic: never read the developer's .env.
    return Settings(_env_file=None, **defaults)


def make_client(tmp_path) -> StravaClient:
    settings = make_settings(tmp_path)
    TokenStore(settings.token_file).save(
        TokenSet(
            access_token="stored-access-token",
            refresh_token="stored-refresh-token",
            expires_at=int(time.time()) + 3600,
        )
    )
    return StravaClient(settings, sleep=lambda s: None)


def effort(effort_id, segment_id, *, elapsed=180, hr=141.0, cadence=87.0, segment_name=None):
    return {
        "id": effort_id,
        "elapsed_time": elapsed,
        "moving_time": elapsed,
        "start_date": "2026-06-15T09:05:00Z",
        "start_date_local": "2026-06-15T04:05:00Z",
        "average_heartrate": hr,
        "average_cadence": cadence,
        "pr_rank": None,
        "segment": {
            "id": segment_id,
            "name": segment_name or f"Segment {segment_id}",
            "distance": 1200.0,
            "average_grade": 1.4,
            "maximum_grade": 6.0,
            "city": "Testville",
            "state": "TS",
            "start_latlng": [12.34, -56.78],
            "end_latlng": [12.35, -56.79],
        },
    }


def detail_payload(activity_id, efforts=()):
    return {
        "id": activity_id,
        "sport_type": "Ride",
        "segment_efforts": list(efforts),
        # Estimated power stays raw-JSONB-only (D27): present in the
        # stored payload, modeled nowhere.
        "average_watts": 145.2,
        "device_watts": False,
    }


# ── Client: detail fetch ──────────────────────────────────────────────


@responses.activate
def test_detail_fetch_requests_every_effort(tmp_path):
    responses.get(f"{API_BASE}/activities/42", json=detail_payload(42, [effort(7, 100)]))

    payload = make_client(tmp_path).get_activity_detail(42, include_all_efforts=True)

    query = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert query["include_all_efforts"] == ["true"]
    assert payload["segment_efforts"][0]["id"] == 7


@responses.activate
def test_detail_fetch_omits_efforts_param_by_default(tmp_path):
    responses.get(f"{API_BASE}/activities/42", json=detail_payload(42))

    make_client(tmp_path).get_activity_detail(42)

    query = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert "include_all_efforts" not in query  # the coordinate path is unchanged


@responses.activate
def test_detail_404_means_unavailable_not_error(tmp_path):
    responses.get(
        f"{API_BASE}/activities/42",
        status=404,
        json={"message": "Record Not Found"},
    )

    assert make_client(tmp_path).get_activity_detail(42, include_all_efforts=True) is None


@responses.activate
def test_rate_limit_statuses_captured_from_detail_responses(tmp_path):
    responses.get(
        f"{API_BASE}/activities/42",
        json=detail_payload(42),
        headers={"X-ReadRateLimit-Limit": "100,1000", "X-ReadRateLimit-Usage": "95,300"},
    )
    client = make_client(tmp_path)

    client.get_activity_detail(42, include_all_efforts=True)

    (status,) = client.rate_limit_approaching()
    assert status.window == "read"
    assert status.short_usage == 95


# ── Engine (fake client, real SQL via the scratch database) ──────────


class FakeDetailClient:
    """Canned per-activity outcomes: a detail payload dict, None (the
    activity no longer exists), an exception instance to raise, or
    'approach' to fetch fine but then report rate-limit pressure."""

    def __init__(self, outcomes):
        self.outcomes = outcomes
        self.calls = []
        self.include_flags = []
        self._approaching = []

    def get_activity_detail(self, activity_id, include_all_efforts=False):
        self.calls.append(activity_id)
        self.include_flags.append(include_all_efforts)
        outcome = self.outcomes[activity_id]
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == "approach":
            self._approaching = [RateLimitStatus("read", 95, 100, 300, 1000)]
            return detail_payload(activity_id, [effort(1000 + activity_id, 100)])
        self._approaching = []
        return outcome

    def rate_limit_approaching(self):
        return self._approaching


def insert_ride(db, activity_id, *, sport_type="Ride", start=None):
    start = start or f"2026-06-{10 + activity_id:02d}T09:00:00Z"
    payload = {"id": activity_id, "sport_type": sport_type, "start_date": start}
    db.execute(
        """
        INSERT INTO raw_strava.activities
            (activity_id, start_date_utc, activity_type, payload, fetched_at)
        VALUES (%s, %s, %s, %s, now())
        """,
        (activity_id, datetime.fromisoformat(start), sport_type, json.dumps(payload)),
    )


def detail_statuses(db):
    return dict(
        db.execute(
            "SELECT activity_id, ingestion_status FROM raw_strava.activity_details ORDER BY 1"
        ).fetchall()
    )


def table_counts(db):
    return (
        db.execute("SELECT count(*) FROM raw_strava.activity_details").fetchone()[0],
        db.execute("SELECT count(*) FROM raw_strava.segment_efforts").fetchone()[0],
        db.execute("SELECT count(*) FROM raw_strava.segments").fetchone()[0],
    )


@pytest.fixture
def db(integration_db):
    integration_db.execute(
        "TRUNCATE raw_strava.activities, raw_strava.activity_details, "
        "raw_strava.segment_efforts, raw_strava.segments"
    )
    integration_db.commit()
    return integration_db


def segment_settings(tmp_path, **overrides):
    return make_settings(tmp_path, **overrides)


@pytest.mark.integration
def test_eligibility_selects_d23_rides_in_window(db, tmp_path):
    insert_ride(db, 1)  # Ride — eligible
    insert_ride(db, 2, sport_type="MountainBikeRide")  # eligible
    # VirtualRide is fetched (D23 grain); D28 holds its efforts out of
    # marts downstream — the gate is not the exclusion point.
    insert_ride(db, 3, sport_type="VirtualRide")
    insert_ride(db, 4, sport_type="Run")  # not a ride
    insert_ride(db, 5, sport_type="EBikeRide")  # outside the D23 grain
    insert_ride(db, 6, start="2023-05-01T09:00:00Z")  # before the D5 window
    db.commit()
    client = FakeDetailClient(
        {
            1: detail_payload(1, [effort(11, 100)]),
            2: detail_payload(2, [effort(21, 100)]),
            3: detail_payload(3, [effort(31, 100)]),
        }
    )

    report = sync_segment_efforts(segment_settings(tmp_path), client, db)

    assert client.calls == [1, 2, 3]
    assert all(client.include_flags)  # D24: every fetch asks for all efforts
    assert report == SegmentSyncReport(
        eligible=3, succeeded=3, efforts_stored=3, last_processed_id=3
    )
    assert detail_statuses(db) == {1: "success", 2: "success", 3: "success"}


@pytest.mark.integration
def test_statuses_recorded_distinctly_and_only_failed_retries(db, tmp_path):
    for activity_id in (1, 2, 3):
        insert_ride(db, activity_id)
    db.commit()
    settings = segment_settings(tmp_path)
    first_client = FakeDetailClient(
        {
            1: detail_payload(1, [effort(11, 100)]),
            2: None,  # the activity no longer exists on Strava: terminal
            3: StravaApiError("Strava API failed after 4 attempts (HTTP 503)"),
        }
    )

    first = sync_segment_efforts(settings, first_client, db)
    assert (first.succeeded, first.unavailable, first.failed) == (1, 1, 1)
    assert detail_statuses(db) == {1: "success", 2: "unavailable", 3: "failed"}
    row = db.execute(
        "SELECT payload, effort_count, error_message"
        " FROM raw_strava.activity_details WHERE activity_id = 3"
    ).fetchone()
    assert row[0] == {}  # explicit empty payload, never NULL
    assert row[1] is None  # missing effort count, not zero
    assert "503" in row[2]

    # Second run: only the failed ride is retried, and it heals.
    second_client = FakeDetailClient({3: detail_payload(3, [effort(31, 200), effort(32, 200)])})
    second = sync_segment_efforts(settings, second_client, db)

    assert second_client.calls == [3]
    assert second == SegmentSyncReport(
        eligible=1, succeeded=1, efforts_stored=2, last_processed_id=3
    )
    assert detail_statuses(db) == {1: "success", 2: "unavailable", 3: "success"}
    assert table_counts(db) == (3, 3, 2)


@pytest.mark.integration
def test_second_run_converges_fetching_nothing(db, tmp_path):
    """C2 acceptance criterion 1: re-running converges — the second run
    on the same fixtures selects nothing and no row count changes."""
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    settings = segment_settings(tmp_path)
    first = sync_segment_efforts(
        settings,
        FakeDetailClient(
            {
                1: detail_payload(1, [effort(11, 100), effort(12, 100), effort(13, 200)]),
                2: None,
            }
        ),
        db,
    )
    assert (first.succeeded, first.unavailable) == (1, 1)
    before = table_counts(db)

    second_client = FakeDetailClient({})
    second = sync_segment_efforts(settings, second_client, db)

    assert second_client.calls == []  # success and unavailable are terminal
    assert second == SegmentSyncReport()
    assert table_counts(db) == before == (2, 3, 2)


@pytest.mark.integration
def test_success_stores_efforts_and_upserts_shared_segments(db, tmp_path):
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    client = FakeDetailClient(
        {
            1: detail_payload(1, [effort(11, 100, segment_name="Old name"), effort(12, 100)]),
            2: detail_payload(2, [effort(21, 100, segment_name="New name")]),
        }
    )

    report = sync_segment_efforts(segment_settings(tmp_path), client, db)

    assert report.efforts_stored == 3
    assert table_counts(db) == (2, 3, 1)  # one row for the shared segment
    name = db.execute(
        "SELECT payload->>'name' FROM raw_strava.segments WHERE segment_id = 100"
    ).fetchone()[0]
    assert name == "New name"  # last write wins
    assert (
        db.execute(
            "SELECT effort_count FROM raw_strava.activity_details WHERE activity_id = 1"
        ).fetchone()[0]
        == 2
    )


@pytest.mark.integration
def test_rate_limit_429_stops_cleanly_keeping_committed_rows(db, tmp_path):
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    client = FakeDetailClient(
        {
            1: detail_payload(1, [effort(11, 100)]),
            2: RateLimitExceeded("HTTP 429: daily 1000/1000"),
        }
    )

    report = sync_segment_efforts(segment_settings(tmp_path), client, db)

    assert report.stopped_early is True
    assert report.succeeded == 1
    assert detail_statuses(db) == {1: "success"}  # no 'failed' row for ride 2


@pytest.mark.integration
def test_approaching_limit_stops_between_activities(db, tmp_path, caplog):
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    client = FakeDetailClient({1: "approach", 2: detail_payload(2, [effort(21, 100)])})

    with caplog.at_level(logging.INFO):
        report = sync_segment_efforts(segment_settings(tmp_path), client, db)

    assert client.calls == [1]  # ride 1's rows kept, 2 never attempted
    assert report.stopped_early is True
    assert detail_statuses(db) == {1: "success"}
    assert "stopping before the Strava rate limit" in caplog.text


@pytest.mark.integration
def test_batch_cap_limits_one_invocation_and_leaves_the_rest_eligible(db, tmp_path):
    for activity_id in (1, 2, 3):
        insert_ride(db, activity_id)
    db.commit()
    settings = segment_settings(tmp_path, segment_fetch_max_activities_per_run=2)
    client = FakeDetailClient(
        {1: detail_payload(1, [effort(11, 100)]), 2: detail_payload(2, [effort(21, 100)])}
    )

    report = sync_segment_efforts(settings, client, db)

    assert client.calls == [1, 2]
    assert report.eligible == 2  # the cap applies at selection time
    # A cap-limited run is complete, never a failure or an early stop.
    assert report.stopped_early is False
    assert report.failed == 0
    # The beyond-cap ride has no row, so it stays eligible next run.
    second_client = FakeDetailClient({3: detail_payload(3, [effort(31, 100)])})
    sync_segment_efforts(settings, second_client, db)
    assert second_client.calls == [3]
    assert detail_statuses(db) == {1: "success", 2: "success", 3: "success"}


@pytest.mark.integration
def test_malformed_effort_records_failed_and_continues(db, tmp_path):
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    malformed = detail_payload(1, [{"id": 11, "elapsed_time": 100}])  # no nested segment
    client = FakeDetailClient({1: malformed, 2: detail_payload(2, [effort(21, 100)])})

    report = sync_segment_efforts(segment_settings(tmp_path), client, db)

    assert (report.failed, report.succeeded) == (1, 1)
    assert detail_statuses(db) == {1: "failed", 2: "success"}
    message = db.execute(
        "SELECT error_message FROM raw_strava.activity_details WHERE activity_id = 1"
    ).fetchone()[0]
    assert "segment" in message
    assert (
        db.execute(
            "SELECT count(*) FROM raw_strava.segment_efforts WHERE activity_id = 1"
        ).fetchone()[0]
        == 0
    )


# ── DDL (@integration) ────────────────────────────────────────────────


@pytest.mark.integration
def test_activity_details_table_rejects_unknown_status(db):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            """
            INSERT INTO raw_strava.activity_details
                (activity_id, payload, fetched_at, ingestion_status)
            VALUES (1, '{}', now(), 'pending')
            """
        )
    db.rollback()


@pytest.mark.integration
def test_segment_efforts_table_one_row_per_effort(db):
    db.execute(
        "INSERT INTO raw_strava.segment_efforts"
        " (effort_id, activity_id, segment_id, payload, fetched_at)"
        " VALUES (1, 10, 100, '{}', now())"
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(
            "INSERT INTO raw_strava.segment_efforts"
            " (effort_id, activity_id, segment_id, payload, fetched_at)"
            " VALUES (1, 11, 200, '{}', now())"
        )
    db.rollback()
