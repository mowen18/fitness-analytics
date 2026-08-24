"""Segment-effort ingestion: per-ride activity-detail fetches (D24, Phase C2).

Resumable by construction: fetch eligibility selects D23 rides in the
historical window that have no activity_details row yet OR a 'failed'
row — success and 'unavailable' (the activity no longer exists on
Strava) are terminal, so re-running converges instead of re-burning
budget. The status rows ARE the resumability mechanism; there is
deliberately no sync_state watermark (the weather-module precedent):
the anti-join needs no cursor, and a cursor could strand failed rows
behind it.

Each successful fetch commits ONE transaction: the activity_details
status row (full DetailedActivity payload — D27's estimated watts live
only there), every extracted segment effort (upserted by effort_id),
and each distinct nested segment summary (upserted by segment_id, last
write wins). An interruption therefore loses at most the in-flight
activity, and efforts can never exist without their status row.

At most SEGMENT_FETCH_MAX_ACTIVITIES_PER_RUN activities per invocation;
the last successfully processed activity is logged. Rate limits stop
the run cleanly between fetches (committed rows kept), mirroring the
stream backfill. Counts and activity ids are logged; token values
never are.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time

import psycopg
from psycopg.types.json import Jsonb

from running_pipeline.config import Settings
from running_pipeline.strava_client import RateLimitStop, StravaApiError, StravaClient

logger = logging.getLogger(__name__)

# D23 ride types (v2.0). VirtualRide is fetched like any other ride —
# D28 keeps its efforts in core, flagged, and holds them out of marts —
# while e-bike types are outside the ride grain entirely (D23).
RIDE_SPORT_TYPES = ("Ride", "MountainBikeRide", "GravelRide", "VirtualRide")

# D24: D23 rides in the historical window, not already fetched (no row)
# or previously failed (retryable). No HR or duration predicate — the
# fetch gate is a data-availability decision; analysis gates live in dbt.
_ELIGIBLE_SQL = """
    SELECT a.activity_id
    FROM raw_strava.activities a
    LEFT JOIN raw_strava.activity_details d USING (activity_id)
    WHERE a.activity_type = ANY(%(ride_types)s)
      AND a.start_date_utc >= %(floor)s
      AND (d.activity_id IS NULL OR d.ingestion_status = 'failed')
    ORDER BY a.start_date_utc, a.activity_id
    LIMIT %(max_activities)s
"""

_DETAIL_UPSERT_SQL = """
    INSERT INTO raw_strava.activity_details
        (activity_id, payload, effort_count, fetched_at, ingestion_status, error_message)
    VALUES (%(activity_id)s, %(payload)s, %(effort_count)s,
            %(fetched_at)s, %(ingestion_status)s, %(error_message)s)
    ON CONFLICT (activity_id) DO UPDATE SET
        payload          = EXCLUDED.payload,
        effort_count     = EXCLUDED.effort_count,
        fetched_at       = EXCLUDED.fetched_at,
        ingestion_status = EXCLUDED.ingestion_status,
        error_message    = EXCLUDED.error_message
"""

_EFFORT_UPSERT_SQL = """
    INSERT INTO raw_strava.segment_efforts
        (effort_id, activity_id, segment_id, payload, fetched_at)
    VALUES (%(effort_id)s, %(activity_id)s, %(segment_id)s, %(payload)s, %(fetched_at)s)
    ON CONFLICT (effort_id) DO UPDATE SET
        activity_id = EXCLUDED.activity_id,
        segment_id  = EXCLUDED.segment_id,
        payload     = EXCLUDED.payload,
        fetched_at  = EXCLUDED.fetched_at
"""

_SEGMENT_UPSERT_SQL = """
    INSERT INTO raw_strava.segments (segment_id, payload, fetched_at)
    VALUES (%(segment_id)s, %(payload)s, %(fetched_at)s)
    ON CONFLICT (segment_id) DO UPDATE SET
        payload    = EXCLUDED.payload,
        fetched_at = EXCLUDED.fetched_at
"""


class MalformedDetailError(RuntimeError):
    """A detail payload carries an effort without usable ids — recorded
    as a retryable 'failed' row, never a crashed batch."""


@dataclass
class SegmentSyncReport:
    eligible: int = 0
    succeeded: int = 0
    unavailable: int = 0
    failed: int = 0
    efforts_stored: int = 0
    stopped_early: bool = False
    last_processed_id: int | None = None


def select_eligible_activity_ids(
    conn: psycopg.Connection,
    sync_start_date: date,
    max_activities: int,
) -> list[int]:
    rows = conn.execute(
        _ELIGIBLE_SQL,
        {
            "ride_types": list(RIDE_SPORT_TYPES),
            "floor": datetime.combine(sync_start_date, time.min, tzinfo=UTC),
            "max_activities": max_activities,
        },
    ).fetchall()
    return [row[0] for row in rows]


def sync_segment_efforts(
    settings: Settings, client: StravaClient, conn: psycopg.Connection
) -> SegmentSyncReport:
    """Fetch and store segment efforts for up to the configured batch of rides.

    Per-activity durability (one commit per fetched ride, covering the
    status row, its efforts, and their segments). A transient API failure
    records a retryable 'failed' row and continues; a rate-limit stop
    ends the run cleanly with committed rows kept.
    """
    activity_ids = select_eligible_activity_ids(
        conn,
        settings.sync_start_date,
        settings.segment_fetch_max_activities_per_run,
    )
    report = SegmentSyncReport(eligible=len(activity_ids))
    logger.info(
        "segment-effort backfill starting eligible=%d (batch cap %d)",
        report.eligible,
        settings.segment_fetch_max_activities_per_run,
    )

    for activity_id in activity_ids:
        try:
            payload = client.get_activity_detail(activity_id, include_all_efforts=True)
        except RateLimitStop as stop:
            report.stopped_early = True
            logger.warning(
                "segment-effort backfill stopped early at the rate limit before "
                "activity %d: %s — committed rows kept; re-run to resume",
                activity_id,
                stop,
            )
            break
        except StravaApiError as exc:
            _record_status(conn, activity_id, "failed", error_message=str(exc))
            conn.commit()
            report.failed += 1
            report.last_processed_id = activity_id
            logger.warning(
                "detail fetch failed activity=%d (recorded for retry): %s", activity_id, exc
            )
            continue

        if payload is None:
            _record_status(conn, activity_id, "unavailable", error_message="no detail (HTTP 404)")
            report.unavailable += 1
            effort_count = None
        else:
            try:
                effort_count = _store_success(conn, activity_id, payload)
            except MalformedDetailError as exc:
                # Discard any partial writes so the failed status row
                # commits alone; the activity stays retryable.
                conn.rollback()
                _record_status(conn, activity_id, "failed", error_message=str(exc))
                conn.commit()
                report.failed += 1
                report.last_processed_id = activity_id
                logger.warning(
                    "detail malformed activity=%d (recorded for retry): %s", activity_id, exc
                )
                continue
            report.succeeded += 1
            report.efforts_stored += effort_count
        conn.commit()  # per-activity durability: interruption loses nothing committed
        report.last_processed_id = activity_id
        logger.info(
            "detail stored activity=%d status=%s efforts=%s",
            activity_id,
            "unavailable" if payload is None else "success",
            "n/a" if payload is None else effort_count,
        )

        # Stop cleanly BETWEEN activities when usage approaches a limit,
        # exactly like the pagination guard between pages.
        triggered = client.rate_limit_approaching()
        if triggered:
            report.stopped_early = True
            logger.warning(
                "segment-effort backfill stopping before the Strava rate limit: %s "
                "— committed rows kept; re-run to resume",
                "; ".join(status.describe() for status in triggered),
            )
            break

    logger.info(
        "segment-effort backfill %s eligible=%d succeeded=%d unavailable=%d "
        "failed=%d efforts_stored=%d last_processed=%s",
        "stopped early" if report.stopped_early else "complete",
        report.eligible,
        report.succeeded,
        report.unavailable,
        report.failed,
        report.efforts_stored,
        report.last_processed_id if report.last_processed_id is not None else "none",
    )
    return report


def _extract_efforts(payload: dict) -> list[dict]:
    """Validate before writing: every effort needs its own id and its
    nested segment summary's id, or nothing for this activity is stored."""
    efforts = payload.get("segment_efforts") or []
    for effort in efforts:
        if effort.get("id") is None or (effort.get("segment") or {}).get("id") is None:
            raise MalformedDetailError(
                "segment effort without effort/segment ids in the detail payload"
            )
    return efforts


def _store_success(conn: psycopg.Connection, activity_id: int, payload: dict) -> int:
    efforts = _extract_efforts(payload)
    fetched_at = datetime.now(UTC)
    _record_status(
        conn, activity_id, "success", payload=payload, effort_count=len(efforts)
    )
    # Distinct segments per activity: several efforts on the same segment
    # in one ride collapse to one upsert (last write wins either way).
    segments: dict[int, dict] = {}
    for effort in efforts:
        segment = effort["segment"]
        segments[segment["id"]] = segment
        conn.execute(
            _EFFORT_UPSERT_SQL,
            {
                "effort_id": effort["id"],
                "activity_id": activity_id,
                "segment_id": segment["id"],
                "payload": Jsonb(effort),
                "fetched_at": fetched_at,
            },
        )
    for segment_id, segment in segments.items():
        conn.execute(
            _SEGMENT_UPSERT_SQL,
            {"segment_id": segment_id, "payload": Jsonb(segment), "fetched_at": fetched_at},
        )
    return len(efforts)


def _record_status(
    conn: psycopg.Connection,
    activity_id: int,
    status: str,
    *,
    payload: dict | None = None,
    effort_count: int | None = None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        _DETAIL_UPSERT_SQL,
        {
            "activity_id": activity_id,
            # {} for failed/unavailable: payload is NOT NULL so "absent"
            # is always explicit, never an ambiguous NULL.
            "payload": Jsonb(payload if payload is not None else {}),
            "effort_count": effort_count,
            "fetched_at": datetime.now(UTC),
            "ingestion_status": status,
            "error_message": error_message,
        },
    )
