"""Stream ingestion tests.

Client fetch behavior is mocked HTTP; engine behavior runs with a fake
client against fakes (unit) and the scratch database (@integration),
mapped to the plan's backfill requirements: resumability, distinct
statuses, clean rate-limit stops, and the per-invocation cap.
"""

import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import psycopg
import pytest
import responses
from click.testing import CliRunner

from running_pipeline import cli as cli_module
from running_pipeline.config import Settings
from running_pipeline.strava_client import (
    API_BASE,
    RIDE_STREAM_TYPES,
    STREAM_TYPES,
    RateLimitExceeded,
    RateLimitStatus,
    StravaApiError,
    StravaClient,
    TokenSet,
    TokenStore,
)
from running_pipeline.stream_ingestion import StreamSyncReport, sync_streams


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


def stream_payload(sample_count=3):
    return {
        "time": {"data": list(range(sample_count)), "original_size": sample_count},
        "heartrate": {"data": [140] * sample_count, "original_size": sample_count},
        "velocity_smooth": {"data": [3.2] * sample_count, "original_size": sample_count},
        "moving": {"data": [True] * sample_count, "original_size": sample_count},
        "grade_smooth": {"data": [0.1] * sample_count, "original_size": sample_count},
    }


# ── Client: stream fetch ──────────────────────────────────────────────


@responses.activate
def test_requests_all_five_stream_types_keyed_by_type(tmp_path):
    responses.get(f"{API_BASE}/activities/42/streams", json=stream_payload())

    payload = make_client(tmp_path).get_activity_streams(42)

    query = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert query["keys"] == ["time,heartrate,velocity_smooth,moving,grade_smooth"]
    assert query["key_by_type"] == ["true"]
    assert set(payload) == set(STREAM_TYPES)


@responses.activate
def test_ride_key_set_adds_distance_and_watts_to_the_run_keys(tmp_path):
    responses.get(f"{API_BASE}/activities/42/streams", json=stream_payload())

    make_client(tmp_path).get_activity_streams(42, keys=RIDE_STREAM_TYPES)

    query = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert query["keys"] == ["time,heartrate,velocity_smooth,moving,grade_smooth,distance,watts"]


@responses.activate
def test_404_means_unavailable_not_error(tmp_path):
    responses.get(
        f"{API_BASE}/activities/42/streams",
        status=404,
        json={"message": "Record Not Found"},
    )

    assert make_client(tmp_path).get_activity_streams(42) is None


@responses.activate
def test_rate_limit_statuses_captured_from_stream_responses(tmp_path):
    responses.get(
        f"{API_BASE}/activities/42/streams",
        json=stream_payload(),
        headers={"X-ReadRateLimit-Limit": "100,1000", "X-ReadRateLimit-Usage": "95,300"},
    )
    client = make_client(tmp_path)

    client.get_activity_streams(42)

    (status,) = client.rate_limit_approaching()
    assert status.window == "read"
    assert status.short_usage == 95


@responses.activate
def test_no_usage_headers_never_trips_the_guard(tmp_path):
    responses.get(f"{API_BASE}/activities/42/streams", json=stream_payload())
    client = make_client(tmp_path)

    client.get_activity_streams(42)

    assert client.rate_limit_approaching() == []


# ── Engine (fake client, real SQL via the scratch database) ──────────


class FakeStreamClient:
    """Canned per-activity outcomes: a dict payload, None (unavailable),
    an exception instance to raise, or 'approach' to fetch fine but then
    report rate-limit pressure."""

    def __init__(self, outcomes):
        self.outcomes = outcomes
        self.calls = []
        self.keys_requested = {}
        self._approaching = []

    def get_activity_streams(self, activity_id, keys=STREAM_TYPES):
        self.calls.append(activity_id)
        self.keys_requested[activity_id] = keys
        outcome = self.outcomes[activity_id]
        if isinstance(outcome, Exception):
            raise outcome
        if outcome == "approach":
            self._approaching = [RateLimitStatus("read", 95, 100, 300, 1000)]
            return stream_payload()
        self._approaching = []
        return outcome

    def rate_limit_approaching(self):
        return self._approaching


def insert_run(
    db, activity_id, *, has_heartrate=True, moving_time=3000, sport_type="Run", device_watts=None
):
    payload = {
        "id": activity_id,
        "sport_type": sport_type,
        "start_date": f"2026-06-{10 + activity_id:02d}T09:00:00Z",
        "has_heartrate": has_heartrate,
        "moving_time": moving_time,
    }
    if device_watts is not None:  # None: the key is absent, as on older payloads
        payload["device_watts"] = device_watts
    db.execute(
        """
        INSERT INTO raw_strava.activities
            (activity_id, start_date_utc, activity_type, payload, fetched_at)
        VALUES (%s, %s, %s, %s, now())
        """,
        (
            activity_id,
            datetime.fromisoformat(payload["start_date"]),
            sport_type,
            json.dumps(payload),
        ),
    )


def stream_statuses(db):
    return dict(
        db.execute(
            "SELECT activity_id, ingestion_status FROM raw_strava.streams ORDER BY 1"
        ).fetchall()
    )


@pytest.fixture
def db(integration_db):
    integration_db.execute("TRUNCATE raw_strava.activities, raw_strava.streams")
    integration_db.commit()
    return integration_db


def stream_settings(tmp_path, **overrides):
    return make_settings(tmp_path, **overrides)


@pytest.mark.integration
def test_eligibility_selects_hr_runs_of_sufficient_length(db, tmp_path):
    insert_run(db, 1)  # eligible (50 min)
    insert_run(db, 2, has_heartrate=False)  # no HR — D15 excludes
    insert_run(db, 3, moving_time=1000)  # under the 20-minute FETCH gate
    insert_run(db, 4, sport_type="Walk")  # not a run
    # 30 minutes: ineligible under the old 45-minute coupling, fetchable
    # since the v1.4 split — fetching is a data-availability decision;
    # the 45-minute analysis gates live in dbt.
    insert_run(db, 5, moving_time=1800)
    db.commit()
    client = FakeStreamClient({1: stream_payload(), 5: stream_payload()})

    report = sync_streams(stream_settings(tmp_path), client, db)

    assert client.calls == [1, 5]
    assert report == StreamSyncReport(eligible=2, succeeded=2, last_processed_id=5)
    assert stream_statuses(db) == {1: "success", 5: "success"}


@pytest.mark.integration
def test_statuses_recorded_distinctly_and_only_failed_retries(db, tmp_path):
    insert_run(db, 1)
    insert_run(db, 2)
    insert_run(db, 3)
    db.commit()
    settings = stream_settings(tmp_path)
    first_client = FakeStreamClient(
        {
            1: stream_payload(),
            2: None,  # Strava has no streams: terminal
            3: StravaApiError("Strava API failed after 4 attempts (HTTP 503)"),
        }
    )

    first = sync_streams(settings, first_client, db)
    assert (first.succeeded, first.unavailable, first.failed) == (1, 1, 1)
    assert stream_statuses(db) == {1: "success", 2: "unavailable", 3: "failed"}
    row = db.execute(
        "SELECT payload, sample_count, error_message FROM raw_strava.streams WHERE activity_id = 3"
    ).fetchone()
    assert row[0] == {}  # explicit empty payload, never NULL
    assert row[1] is None  # missing sample count, not zero
    assert "503" in row[2]

    # Second run: only the failed activity is retried, and it heals.
    second_client = FakeStreamClient({3: stream_payload(sample_count=5)})
    second = sync_streams(settings, second_client, db)

    assert second_client.calls == [3]
    assert second == StreamSyncReport(eligible=1, succeeded=1, last_processed_id=3)
    assert stream_statuses(db) == {1: "success", 2: "unavailable", 3: "success"}
    assert (
        db.execute("SELECT sample_count FROM raw_strava.streams WHERE activity_id = 3").fetchone()[
            0
        ]
        == 5
    )


@pytest.mark.integration
def test_rate_limit_429_stops_cleanly_keeping_committed_rows(db, tmp_path):
    insert_run(db, 1)
    insert_run(db, 2)
    db.commit()
    client = FakeStreamClient(
        {1: stream_payload(), 2: RateLimitExceeded("HTTP 429: daily 1000/1000")}
    )

    report = sync_streams(stream_settings(tmp_path), client, db)

    assert report.stopped_early is True
    assert report.succeeded == 1
    assert stream_statuses(db) == {1: "success"}  # no 'failed' row for activity 2


@pytest.mark.integration
def test_approaching_limit_stops_between_activities(db, tmp_path, caplog):
    insert_run(db, 1)
    insert_run(db, 2)
    db.commit()
    client = FakeStreamClient({1: "approach", 2: stream_payload()})

    with caplog.at_level(logging.INFO):
        report = sync_streams(stream_settings(tmp_path), client, db)

    assert client.calls == [1]  # activity 1's row kept, 2 never attempted
    assert report.stopped_early is True
    assert stream_statuses(db) == {1: "success"}
    assert "stopping before the Strava rate limit" in caplog.text


@pytest.mark.integration
def test_batch_cap_limits_one_invocation(db, tmp_path):
    for activity_id in (1, 2, 3):
        insert_run(db, activity_id)
    db.commit()
    settings = stream_settings(tmp_path, stream_max_activities_per_run=2)
    client = FakeStreamClient({1: stream_payload(), 2: stream_payload()})

    report = sync_streams(settings, client, db)

    assert client.calls == [1, 2]
    assert report.eligible == 2  # the cap applies at selection time
    assert stream_statuses(db) == {1: "success", 2: "success"}


# ── D31: ride power fetch (v2.4) ──────────────────────────────────────


def insert_ride(db, activity_id, *, device_watts=True, sport_type="Ride", **kwargs):
    insert_run(db, activity_id, sport_type=sport_type, device_watts=device_watts, **kwargs)


def stored_keys(db, activity_id):
    return db.execute(
        "SELECT stream_types_requested FROM raw_strava.streams WHERE activity_id = %s",
        (activity_id,),
    ).fetchone()[0]


@pytest.mark.integration
def test_ride_selection_is_real_power_past_the_fetch_gate(db, tmp_path):
    insert_ride(db, 1)  # eligible
    insert_ride(db, 2, has_heartrate=False)  # eligible: HR never gates rides (D25)
    insert_ride(db, 3, sport_type="GravelRide")  # eligible: a D23 ride type
    insert_ride(db, 4, device_watts=False)  # estimated power
    insert_ride(db, 5, device_watts=None)  # no device_watts key at all
    insert_ride(db, 6, sport_type="EBikeRide")  # outside the D23 grain
    insert_ride(db, 7, moving_time=1000)  # under the 20-minute fetch gate
    db.commit()
    client = FakeStreamClient({1: stream_payload(), 2: stream_payload(), 3: stream_payload()})

    report = sync_streams(stream_settings(tmp_path), client, db)

    assert client.calls == [1, 2, 3]
    assert report.eligible == 3
    assert stream_statuses(db) == {1: "success", 2: "success", 3: "success"}


@pytest.mark.integration
def test_ride_selection_respects_the_historical_window(db, tmp_path):
    insert_ride(db, 1)
    db.commit()
    settings = stream_settings(tmp_path, sync_start_date="2026-07-01")
    client = FakeStreamClient({})

    report = sync_streams(settings, client, db)

    assert client.calls == []
    assert report.eligible == 0


@pytest.mark.integration
def test_run_requests_keep_the_unchanged_key_set(db, tmp_path):
    insert_run(db, 1)
    # A real-power run must stay on the run key set: 90 live runs carry
    # device_watts = true, and the flag alone never selects ride keys.
    insert_run(db, 2, device_watts=True)
    db.commit()
    client = FakeStreamClient({1: stream_payload(), 2: stream_payload()})

    sync_streams(stream_settings(tmp_path), client, db)

    run_keys = ("time", "heartrate", "velocity_smooth", "moving", "grade_smooth")
    assert run_keys == STREAM_TYPES
    assert client.keys_requested == {1: run_keys, 2: run_keys}
    assert stored_keys(db, 1) == list(run_keys)
    assert stored_keys(db, 2) == list(run_keys)


@pytest.mark.integration
def test_ride_requests_use_and_record_the_ride_key_set(db, tmp_path):
    insert_ride(db, 1)
    insert_ride(db, 2)
    insert_ride(db, 3)
    db.commit()
    client = FakeStreamClient(
        {1: stream_payload(), 2: None, 3: StravaApiError("Strava API failed (HTTP 503)")}
    )

    sync_streams(stream_settings(tmp_path), client, db)

    assert client.keys_requested == {
        1: RIDE_STREAM_TYPES,
        2: RIDE_STREAM_TYPES,
        3: RIDE_STREAM_TYPES,
    }
    # Status-only rows record the keys actually sent, too.
    for activity_id in (1, 2, 3):
        assert stored_keys(db, activity_id) == list(RIDE_STREAM_TYPES)


@pytest.mark.integration
def test_ride_branch_never_selects_a_run(db, tmp_path):
    # No HR, so the run branch rejects it (D15). A ride branch that read
    # device_watts without the sport type would pick it up.
    insert_run(db, 1, has_heartrate=False, device_watts=True)
    db.commit()
    client = FakeStreamClient({})

    report = sync_streams(stream_settings(tmp_path), client, db)

    assert client.calls == []
    assert report.eligible == 0


@pytest.mark.integration
def test_successful_ride_row_without_watts_is_never_reselected(db, tmp_path):
    insert_ride(db, 1)
    db.commit()
    settings = stream_settings(tmp_path)
    # device_watts is true but Strava returned no watts array.
    assert "watts" not in stream_payload()
    first_client = FakeStreamClient({1: stream_payload()})
    sync_streams(settings, first_client, db)
    assert stream_statuses(db) == {1: "success"}

    second_client = FakeStreamClient({})
    report = sync_streams(settings, second_client, db)

    assert second_client.calls == []
    assert report.eligible == 0


@pytest.mark.integration
def test_failed_ride_retries_and_unavailable_ride_is_terminal(db, tmp_path):
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    settings = stream_settings(tmp_path)
    sync_streams(
        settings,
        FakeStreamClient({1: StravaApiError("Strava API failed (HTTP 503)"), 2: None}),
        db,
    )
    assert stream_statuses(db) == {1: "failed", 2: "unavailable"}

    client = FakeStreamClient({1: stream_payload()})
    sync_streams(settings, client, db)

    assert client.calls == [1]
    assert stream_statuses(db) == {1: "success", 2: "unavailable"}


@pytest.mark.integration
def test_rate_limit_on_a_ride_stops_cleanly_keeping_committed_rows(db, tmp_path):
    insert_ride(db, 1)
    insert_ride(db, 2)
    db.commit()
    client = FakeStreamClient(
        {1: stream_payload(), 2: RateLimitExceeded("HTTP 429: daily 1000/1000")}
    )

    report = sync_streams(stream_settings(tmp_path), client, db)

    assert report.stopped_early is True  # the CLI turns this into exit 3
    assert stream_statuses(db) == {1: "success"}


@pytest.mark.integration
def test_cli_exits_3_when_a_ride_fetch_hits_the_rate_limit(db, tmp_path, monkeypatch):
    insert_ride(db, 1)
    db.commit()
    client = FakeStreamClient({1: RateLimitExceeded("HTTP 429: daily 1000/1000")})

    @contextmanager
    def shared_connection(settings):
        yield db

    monkeypatch.setattr(cli_module, "load_settings", lambda: stream_settings(tmp_path))
    monkeypatch.setattr(cli_module, "StravaClient", lambda settings: client)
    monkeypatch.setattr(cli_module, "get_connection", shared_connection)

    result = CliRunner().invoke(cli_module.cli, ["sync-streams"])

    assert client.calls == [1]
    assert result.exit_code == 3, result.output
    assert stream_statuses(db) == {}


@pytest.mark.integration
def test_batch_cap_is_shared_with_runs_first(db, tmp_path):
    insert_ride(db, 1)  # earlier than the runs: order is by branch, not date
    insert_run(db, 2)
    insert_run(db, 3)
    insert_ride(db, 4)
    db.commit()
    settings = stream_settings(tmp_path, stream_max_activities_per_run=3)
    client = FakeStreamClient({1: stream_payload(), 2: stream_payload(), 3: stream_payload()})

    report = sync_streams(settings, client, db)

    assert client.calls == [2, 3, 1]
    assert report.eligible == 3


# ── DDL (@integration) ────────────────────────────────────────────────


@pytest.mark.integration
def test_streams_table_rejects_unknown_status(db):
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            """
            INSERT INTO raw_strava.streams
                (activity_id, payload, fetched_at, ingestion_status)
            VALUES (1, '{}', now(), 'pending')
            """
        )
    db.rollback()


@pytest.mark.integration
def test_streams_table_one_row_per_activity(db):
    db.execute(
        "INSERT INTO raw_strava.streams (activity_id, payload, fetched_at, ingestion_status)"
        " VALUES (1, '{}', now(), 'success')"
    )
    with pytest.raises(psycopg.errors.UniqueViolation):
        db.execute(
            "INSERT INTO raw_strava.streams (activity_id, payload, fetched_at, ingestion_status)"
            " VALUES (1, '{}', now(), 'failed')"
        )
    db.rollback()
