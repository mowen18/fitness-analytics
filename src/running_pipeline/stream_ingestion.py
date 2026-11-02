"""Activity-stream backfill: time-series samples for sample-grain analysis.

Resumable by construction: fetch eligibility (D15 revised v1.4 — a
data-availability gate, not a metric-eligibility gate) selects runs
with heart rate and >= STREAM_FETCH_MIN_MOVING_MINUTES that have no
streams row yet OR a 'failed' row — success and 'unavailable' (Strava
has no streams for the activity, which never changes after upload) are
terminal, so re-running converges instead of re-burning budget. Each
activity commits its own row, so an interruption loses at most the
in-flight fetch.

Rides enter through their own selection (D31, v2.4): real power only
(device_watts true), past the same fetch gate, no HR requirement. They
request RIDE_STREAM_TYPES; run requests stay on STREAM_TYPES.

At most STREAM_MAX_ACTIVITIES_PER_RUN activities per invocation; the
last successfully processed activity is logged. Rate limits stop the
run cleanly between fetches (committed rows kept), mirroring Phase 1.

Counts and activity ids are logged; token values never are.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time

import psycopg
from psycopg.types.json import Jsonb

from running_pipeline.config import Settings
from running_pipeline.strava_client import (
    RIDE_STREAM_TYPES,
    STREAM_TYPES,
    RateLimitStop,
    StravaApiError,
    StravaClient,
)

logger = logging.getLogger(__name__)

# D15 (revised v1.4): runs with HR, past the fetch gate, in the
# historical window, not already loaded (no row) or previously failed
# (retryable). Analysis gates live in dbt, not here.
_ELIGIBLE_SQL = """
    SELECT a.activity_id
    FROM raw_strava.activities a
    LEFT JOIN raw_strava.streams s USING (activity_id)
    WHERE a.activity_type = ANY(%(running_types)s)
      AND coalesce((a.payload->>'has_heartrate')::boolean, false)
      AND (a.payload->>'moving_time')::integer >= %(min_moving_seconds)s
      AND a.start_date_utc >= %(floor)s
      AND (s.activity_id IS NULL OR s.ingestion_status = 'failed')
    ORDER BY a.start_date_utc, a.activity_id
    LIMIT %(max_activities)s
"""

RUNNING_SPORT_TYPES = ("Run", "TrailRun", "VirtualRun")

# D31 (v2.4): D23 ride types with real power (device_watts literally
# true: false and an absent key both stay out), past the fetch gate, not
# already loaded or previously failed. No HR requirement (D25). 'success'
# stays terminal even when Strava returned no watts array.
_ELIGIBLE_RIDES_SQL = """
    SELECT a.activity_id
    FROM raw_strava.activities a
    LEFT JOIN raw_strava.streams s USING (activity_id)
    WHERE a.activity_type = ANY(%(ride_types)s)
      AND coalesce((a.payload->>'device_watts')::boolean, false)
      AND (a.payload->>'moving_time')::integer >= %(min_moving_seconds)s
      AND a.start_date_utc >= %(floor)s
      AND (s.activity_id IS NULL OR s.ingestion_status = 'failed')
    ORDER BY a.start_date_utc, a.activity_id
    LIMIT %(max_activities)s
"""

# Python-side twin of the ride_sport_types dbt var (D23: no e-bikes).
RIDE_SPORT_TYPES = ("Ride", "MountainBikeRide", "GravelRide", "VirtualRide")

_UPSERT_SQL = """
    INSERT INTO raw_strava.streams
        (activity_id, payload, stream_types_requested, sample_count,
         fetched_at, ingestion_status, error_message)
    VALUES (%(activity_id)s, %(payload)s, %(stream_types_requested)s,
            %(sample_count)s, %(fetched_at)s, %(ingestion_status)s, %(error_message)s)
    ON CONFLICT (activity_id) DO UPDATE SET
        payload                = EXCLUDED.payload,
        stream_types_requested = EXCLUDED.stream_types_requested,
        sample_count           = EXCLUDED.sample_count,
        fetched_at             = EXCLUDED.fetched_at,
        ingestion_status       = EXCLUDED.ingestion_status,
        error_message          = EXCLUDED.error_message
"""


@dataclass
class StreamSyncReport:
    eligible: int = 0
    succeeded: int = 0
    unavailable: int = 0
    failed: int = 0
    stopped_early: bool = False
    last_processed_id: int | None = None


def select_eligible_activity_ids(
    conn: psycopg.Connection,
    sync_start_date: date,
    min_moving_minutes: int,
    max_activities: int,
) -> list[int]:
    rows = conn.execute(
        _ELIGIBLE_SQL,
        {
            "running_types": list(RUNNING_SPORT_TYPES),
            "min_moving_seconds": min_moving_minutes * 60,
            "floor": datetime.combine(sync_start_date, time.min, tzinfo=UTC),
            "max_activities": max_activities,
        },
    ).fetchall()
    return [row[0] for row in rows]


def select_eligible_ride_ids(
    conn: psycopg.Connection,
    sync_start_date: date,
    min_moving_minutes: int,
    max_activities: int,
) -> list[int]:
    rows = conn.execute(
        _ELIGIBLE_RIDES_SQL,
        {
            "ride_types": list(RIDE_SPORT_TYPES),
            "min_moving_seconds": min_moving_minutes * 60,
            "floor": datetime.combine(sync_start_date, time.min, tzinfo=UTC),
            "max_activities": max_activities,
        },
    ).fetchall()
    return [row[0] for row in rows]


def sync_streams(
    settings: Settings, client: StravaClient, conn: psycopg.Connection
) -> StreamSyncReport:
    """Fetch and store streams for up to the configured batch of activities.

    Runs first, then real-power rides, sharing one batch cap.

    Per-activity durability (commit per row). A transient API failure
    records a retryable 'failed' row and continues; a rate-limit stop
    ends the run cleanly with committed rows kept.
    """
    activity_ids = select_eligible_activity_ids(
        conn,
        settings.sync_start_date,
        settings.stream_fetch_min_moving_minutes,
        settings.stream_max_activities_per_run,
    )
    ride_ids = select_eligible_ride_ids(
        conn,
        settings.sync_start_date,
        settings.stream_fetch_min_moving_minutes,
        settings.stream_max_activities_per_run - len(activity_ids),
    )
    fetches = [(activity_id, STREAM_TYPES) for activity_id in activity_ids]
    fetches += [(activity_id, RIDE_STREAM_TYPES) for activity_id in ride_ids]
    report = StreamSyncReport(eligible=len(fetches))
    logger.info(
        "stream backfill starting eligible=%d (batch cap %d)",
        report.eligible,
        settings.stream_max_activities_per_run,
    )

    for activity_id, stream_types in fetches:
        try:
            payload = client.get_activity_streams(activity_id, keys=stream_types)
        except RateLimitStop as stop:
            report.stopped_early = True
            logger.warning(
                "stream backfill stopped early at the rate limit before activity %d: %s "
                "— committed rows kept; re-run to resume",
                activity_id,
                stop,
            )
            break
        except StravaApiError as exc:
            _record(conn, activity_id, "failed", stream_types, error_message=str(exc))
            conn.commit()
            report.failed += 1
            report.last_processed_id = activity_id
            logger.warning(
                "stream fetch failed activity=%d (recorded for retry): %s", activity_id, exc
            )
            continue

        if payload is None:
            _record(
                conn,
                activity_id,
                "unavailable",
                stream_types,
                error_message="no streams (HTTP 404)",
            )
            report.unavailable += 1
        else:
            sample_count = len(payload.get("time", {}).get("data", []))
            _record(
                conn,
                activity_id,
                "success",
                stream_types,
                payload=payload,
                sample_count=sample_count,
            )
            report.succeeded += 1
        conn.commit()  # per-activity durability: interruption loses nothing committed
        report.last_processed_id = activity_id
        logger.info(
            "stream stored activity=%d status=%s samples=%s",
            activity_id,
            "unavailable" if payload is None else "success",
            "n/a" if payload is None else sample_count,
        )

        # Stop cleanly BETWEEN activities when usage approaches a limit,
        # exactly like the pagination guard between pages.
        triggered = client.rate_limit_approaching()
        if triggered:
            report.stopped_early = True
            logger.warning(
                "stream backfill stopping before the Strava rate limit: %s "
                "— committed rows kept; re-run to resume",
                "; ".join(status.describe() for status in triggered),
            )
            break

    logger.info(
        "stream backfill %s eligible=%d succeeded=%d unavailable=%d failed=%d last_processed=%s",
        "stopped early" if report.stopped_early else "complete",
        report.eligible,
        report.succeeded,
        report.unavailable,
        report.failed,
        report.last_processed_id if report.last_processed_id is not None else "none",
    )
    return report


def _record(
    conn: psycopg.Connection,
    activity_id: int,
    status: str,
    stream_types: tuple[str, ...],
    *,
    payload: dict | None = None,
    sample_count: int | None = None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        _UPSERT_SQL,
        {
            "activity_id": activity_id,
            # {} for failed/unavailable: payload is NOT NULL so "absent"
            # is always explicit, never an ambiguous NULL.
            "payload": Jsonb(payload if payload is not None else {}),
            "stream_types_requested": list(stream_types),
            "sample_count": sample_count,
            "fetched_at": datetime.now(UTC),
            "ingestion_status": status,
            "error_message": error_message,
        },
    )
