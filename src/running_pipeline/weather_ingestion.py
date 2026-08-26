"""Weather ingestion: attach Open-Meteo hourly weather to outdoor runs.

raw_weather.hourly is both the destination and the cache: needed
location-hours are whatever eligible runs require minus what the table
already holds, so every sync is inherently incremental and the first one
is a natural backfill. There is no sync_state watermark; `full=True`
re-fetches even cached hours (the archive occasionally revises data).

Eligible activities are outdoor runs and D23 rides: Run/TrailRun and
Ride/MountainBikeRide/GravelRide (VirtualRun and VirtualRide are
indoor), non-empty start coordinates, trainer flag not set, on or after
the D5 historical floor. Indoor activities are excluded by design — they
have no location, and outdoor weather would be wrong for them anyway;
they are counted explicitly, never silently dropped.

Missing weather is recorded explicitly as rows with NULL measurements
(never zero). Requests pass no `models` parameter, so the archive's
`best_match` default stitches ERA5 (0.25 deg, ~5-day delay), ERA5-Land
(0.1 deg), and low-latency ECMWF IFS analysis (9 km) per hour: recent
dates arrive immediately as real preliminary IFS values, and nothing in
the response says which dataset served which hour (fetched_at relative
to the observation date is the only heuristic proxy). All-NULL rows
occur only when the archive genuinely has no data for an hour — the
rare case — and are re-requested by later syncs until data appears. A
cache-completeness check must know whether the source's answer was
final; ours treats any non-NULL value as final, so preliminary values
are frozen until a `full` re-fetch, whose IS DISTINCT FROM upsert
absorbs revisions. Since D29 (v2.0 Phase C3) completeness additionally
requires wind direction to be RESOLVED: either a value in
wind_direction_deg, or a payload carrying the wind_direction_10m key
(present as an explicit null when the archive has no direction — the
client records every requested variable). Rows fetched before the
migration lack the key and become fetch-eligible again exactly once;
the first re-fetch resolves them either way, so the queue terminates.
A failed batch never fails the sync: it is logged, counted, and left
for the next run. Coordinates are never logged beyond the 2-dp cell
key.
"""

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb

from running_pipeline.config import Settings
from running_pipeline.weather_client import (
    WeatherApiError,
    WeatherClient,
    WeatherStop,
    location_key,
    normalize_coordinate,
    parse_hourly_rows,
)

logger = logging.getLogger(__name__)

# Outdoor-capable running sport types. VirtualRun is deliberately absent:
# virtual runs are indoor regardless of their trainer flag.
RUNNING_SPORT_TYPES = ("Run", "TrailRun")

# Outdoor-capable D23 ride types (v2.0 Phase C1). VirtualRide is
# deliberately absent, mirroring VirtualRun; e-bike types are not in the
# ride grain at all (D23: motor assist breaks HR-effort comparability).
# The dbt-side twin is the ride_sport_types var in dbt_project.yml.
RIDE_SPORT_TYPES = ("Ride", "MountainBikeRide", "GravelRide")

# Coordinates come from the payload's start_latlng OR the resolved
# activity_coordinates row (the map-privacy polyline fallback — see
# coordinate_ingestion). The resolved row wins when both exist: it is
# never worse, and for polyline-derived rows it is the only source.
_ELIGIBLE_RUNS_SQL = """
    SELECT a.activity_id,
           date_trunc('hour', a.start_date_utc) AS start_hour_utc,
           coalesce(c.latitude::float8,  (a.payload->'start_latlng'->>0)::float8) AS latitude,
           coalesce(c.longitude::float8, (a.payload->'start_latlng'->>1)::float8) AS longitude
    FROM raw_strava.activities a
    LEFT JOIN raw_strava.activity_coordinates c USING (activity_id)
    WHERE a.activity_type = ANY(%(running_types)s)
      AND NOT coalesce((a.payload->>'trainer')::boolean, false)
      AND a.start_date_utc >= %(floor)s
      AND (c.latitude IS NOT NULL
           OR jsonb_array_length(coalesce(a.payload->'start_latlng', '[]'::jsonb)) = 2)
    ORDER BY a.start_date_utc, a.activity_id
"""

# Same run population, opposite coordinate/trainer test: runs that can
# never get weather. Reported explicitly per the data-quality principle.
_INELIGIBLE_RUNS_SQL = """
    SELECT count(*)
    FROM raw_strava.activities a
    LEFT JOIN raw_strava.activity_coordinates c USING (activity_id)
    WHERE a.activity_type = ANY(%(running_types)s)
      AND a.start_date_utc >= %(floor)s
      AND (coalesce((a.payload->>'trainer')::boolean, false)
           OR (c.latitude IS NULL
               AND jsonb_array_length(coalesce(a.payload->'start_latlng', '[]'::jsonb)) <> 2))
"""

# An hour is complete when (a) any measurement is present — an all-NULL
# row is the explicit missing marker and stays eligible for re-fetching —
# AND (b) wind direction is resolved (D29): a wind_direction_deg value,
# or the payload carrying the wind_direction_10m key (an explicit null
# when the archive has no direction; the client records every requested
# variable, so the key's absence means "fetched before the migration").
# 0° is a value and satisfies (b) — north, never a stand-in for missing.
# The measurement arm is also the cache's blind spot: it cannot tell
# final ERA5 values from preliminary best_match (IFS) fill-in, so any
# non-NULL value counts as done and stays frozen until a full re-fetch.
_IS_COMPLETE = """((h.temperature_c IS NOT NULL
                OR h.apparent_temperature_c IS NOT NULL
                OR h.relative_humidity_pct IS NOT NULL
                OR h.wind_speed_kph IS NOT NULL)
               AND (h.wind_direction_deg IS NOT NULL
                    OR h.payload ? 'wind_direction_10m'))"""

_CACHE_STATE_SQL = f"""
    SELECT h.location_key, h.weather_timestamp, {_IS_COMPLETE} AS is_complete
    FROM raw_weather.hourly h
    JOIN unnest(%(keys)s::text[], %(hours)s::timestamptz[])
         AS need(location_key, weather_timestamp)
      ON h.location_key = need.location_key
     AND h.weather_timestamp = need.weather_timestamp
"""

_STILL_MISSING_SQL = f"""
    SELECT count(*)
    FROM unnest(%(keys)s::text[], %(hours)s::timestamptz[])
         AS need(location_key, weather_timestamp)
    WHERE NOT EXISTS (
        SELECT 1 FROM raw_weather.hourly h
        WHERE h.location_key = need.location_key
          AND h.weather_timestamp = need.weather_timestamp
          AND {_IS_COMPLETE}
    )
"""

# Same observability contract as the activity upsert: the WHERE clause
# turns identical re-fetches into no-ops (RETURNING yields no row), so
# "skipped" is observable rather than inferred; xmax = 0 marks an insert.
_UPSERT_SQL = """
    INSERT INTO raw_weather.hourly
        (location_key, latitude, longitude, weather_timestamp, temperature_c,
         apparent_temperature_c, relative_humidity_pct, wind_speed_kph,
         wind_direction_deg, payload, fetched_at)
    VALUES (%(location_key)s, %(latitude)s, %(longitude)s, %(weather_timestamp)s,
            %(temperature_c)s, %(apparent_temperature_c)s, %(relative_humidity_pct)s,
            %(wind_speed_kph)s, %(wind_direction_deg)s, %(payload)s, %(fetched_at)s)
    ON CONFLICT (location_key, weather_timestamp) DO UPDATE SET
        latitude               = EXCLUDED.latitude,
        longitude              = EXCLUDED.longitude,
        temperature_c          = EXCLUDED.temperature_c,
        apparent_temperature_c = EXCLUDED.apparent_temperature_c,
        relative_humidity_pct  = EXCLUDED.relative_humidity_pct,
        wind_speed_kph         = EXCLUDED.wind_speed_kph,
        wind_direction_deg     = EXCLUDED.wind_direction_deg,
        payload                = EXCLUDED.payload,
        fetched_at             = EXCLUDED.fetched_at
    WHERE hourly.payload IS DISTINCT FROM EXCLUDED.payload
    RETURNING (xmax = 0) AS inserted
"""


@dataclass(frozen=True)
class RunLocation:
    """An outdoor run's weather lookup target: normalized cell + UTC hour."""

    activity_id: int
    start_hour_utc: datetime
    latitude: Decimal  # 2-dp per D7, matches location_key
    longitude: Decimal
    location_key: str


@dataclass(frozen=True)
class FetchBatch:
    """One archive request: a 2-dp cell over an inclusive UTC date range."""

    location_key: str
    latitude: Decimal
    longitude: Decimal
    start_date: date
    end_date: date


@dataclass
class FetchPlan:
    batches: list[FetchBatch]
    hours_needed: int  # unique location-hours to fetch
    hours_cached: int  # unique location-hours already cached with data


@dataclass
class WeatherSyncReport:
    eligible_runs: int = 0
    runs_without_location: int = 0  # indoor or coordinate-less: explicit, never silent
    hours_needed: int = 0
    hours_cached: int = 0
    requests_made: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed_batches: int = 0
    hours_still_missing: int = 0  # run hours without data after this sync
    stopped_early: bool = False


def select_eligible_runs(conn: psycopg.Connection, sync_start_date: date) -> list[RunLocation]:
    """Outdoor runs and rides (per module docstring) with normalized coordinates."""
    rows = conn.execute(
        _ELIGIBLE_RUNS_SQL,
        {
            "running_types": list(RUNNING_SPORT_TYPES + RIDE_SPORT_TYPES),
            "floor": _floor(sync_start_date),
        },
    ).fetchall()
    return [
        RunLocation(
            activity_id=activity_id,
            start_hour_utc=start_hour_utc,
            latitude=normalize_coordinate(latitude),
            longitude=normalize_coordinate(longitude),
            location_key=location_key(latitude, longitude),
        )
        for activity_id, start_hour_utc, latitude, longitude in rows
    ]


def count_runs_without_location(conn: psycopg.Connection, sync_start_date: date) -> int:
    """Activities in the window that can never get weather (indoor or no coordinates)."""
    row = conn.execute(
        _INELIGIBLE_RUNS_SQL,
        {
            "running_types": list(RUNNING_SPORT_TYPES + RIDE_SPORT_TYPES),
            "floor": _floor(sync_start_date),
        },
    ).fetchone()
    return row[0]


def plan_fetches(
    runs: list[RunLocation],
    cache: dict[tuple[str, datetime], bool],
    *,
    full: bool,
    gap_days: int,
) -> FetchPlan:
    """Turn run location-hours into per-cell date-range batches (pure).

    `cache` maps (location_key, hour) to is_complete for hours already
    in raw_weather.hourly. Complete hours are skipped unless `full` —
    even when their data is preliminary best_match fill-in, because
    completeness cannot tell preliminary from final; all-NULL hours
    (the archive genuinely had no data) and hours whose wind direction
    is unresolved (D29: fetched before the migration) are re-requested
    so they self-heal. Needed dates per cell merge into one batch while
    gaps stay within `gap_days`; whole days are fetched (and later
    stored) because the archive returns them anyway.
    """
    pairs: dict[tuple[str, datetime], tuple[Decimal, Decimal]] = {}
    for run in runs:
        pairs.setdefault((run.location_key, run.start_hour_utc), (run.latitude, run.longitude))

    needed: dict[tuple[str, datetime], tuple[Decimal, Decimal]] = {}
    cached = 0
    for pair, coordinates in pairs.items():
        if not full and cache.get(pair, False):
            cached += 1
        else:
            needed[pair] = coordinates

    dates_by_cell: dict[str, set[date]] = {}
    coordinates_by_cell: dict[str, tuple[Decimal, Decimal]] = {}
    for (key, hour), coordinates in needed.items():
        dates_by_cell.setdefault(key, set()).add(hour.date())
        coordinates_by_cell[key] = coordinates

    batches = []
    for key in sorted(dates_by_cell):
        latitude, longitude = coordinates_by_cell[key]
        for start, end in _merge_date_ranges(sorted(dates_by_cell[key]), gap_days):
            batches.append(FetchBatch(key, latitude, longitude, start, end))
    return FetchPlan(batches=batches, hours_needed=len(needed), hours_cached=cached)


def sync_weather(
    settings: Settings,
    client: WeatherClient,
    conn: psycopg.Connection,
    *,
    full: bool = False,
) -> WeatherSyncReport:
    """Fetch and upsert hourly weather for every eligible run's start hour.

    Commits after each batch; a failed batch logs and continues
    (unavailable weather never fails the pipeline); 429 or the request
    budget stop the sync cleanly with committed batches kept.
    """
    report = WeatherSyncReport()
    runs = select_eligible_runs(conn, settings.sync_start_date)
    report.eligible_runs = len(runs)
    report.runs_without_location = count_runs_without_location(conn, settings.sync_start_date)

    run_pairs = sorted({(run.location_key, run.start_hour_utc) for run in runs})
    cache = _load_cache_state(conn, run_pairs)
    plan = plan_fetches(runs, cache, full=full, gap_days=settings.weather_batch_gap_days)
    report.hours_needed = plan.hours_needed
    report.hours_cached = plan.hours_cached

    mode = "full" if full else "incremental"
    logger.info(
        "weather sync starting mode=%s eligible_runs=%d runs_without_location=%d "
        "hours_needed=%d hours_cached=%d batches=%d",
        mode,
        report.eligible_runs,
        report.runs_without_location,
        report.hours_needed,
        report.hours_cached,
        len(plan.batches),
    )

    try:
        for batch in plan.batches:
            try:
                payload = client.fetch_hourly(
                    batch.latitude, batch.longitude, batch.start_date, batch.end_date
                )
            except WeatherApiError as exc:
                report.failed_batches += 1
                logger.warning(
                    "weather batch failed cell=%s range=%s..%s: %s — continuing",
                    batch.location_key,
                    batch.start_date,
                    batch.end_date,
                    exc,
                )
                continue
            rows = parse_hourly_rows(payload, batch.latitude, batch.longitude, datetime.now(UTC))
            outcomes = Counter(_upsert_hour(conn, row) for row in rows)
            conn.commit()  # batch-level durability: a later failure keeps this work
            report.inserted += outcomes["inserted"]
            report.updated += outcomes["updated"]
            report.skipped += outcomes["skipped"]
            logger.info(
                "weather batch cell=%s range=%s..%s hours=%d inserted=%d updated=%d skipped=%d",
                batch.location_key,
                batch.start_date,
                batch.end_date,
                len(rows),
                outcomes["inserted"],
                outcomes["updated"],
                outcomes["skipped"],
            )
    except WeatherStop as stop:
        report.stopped_early = True
        logger.warning(
            "weather sync stopped early: %s | inserted=%d updated=%d skipped=%d "
            "failed_batches=%d — committed batches kept, remaining hours fetched next run",
            stop,
            report.inserted,
            report.updated,
            report.skipped,
            report.failed_batches,
        )

    report.requests_made = client.requests_made
    report.hours_still_missing = _count_still_missing(conn, run_pairs)
    if not report.stopped_early:
        logger.info(
            "weather sync complete mode=%s requests=%d inserted=%d updated=%d skipped=%d "
            "failed_batches=%d hours_still_missing=%d",
            mode,
            report.requests_made,
            report.inserted,
            report.updated,
            report.skipped,
            report.failed_batches,
            report.hours_still_missing,
        )
    return report


def _upsert_hour(conn: psycopg.Connection, row: dict) -> str:
    result = conn.execute(_UPSERT_SQL, {**row, "payload": Jsonb(row["payload"])}).fetchone()
    if result is None:
        return "skipped"
    return "inserted" if result[0] else "updated"


def _load_cache_state(
    conn: psycopg.Connection, pairs: list[tuple[str, datetime]]
) -> dict[tuple[str, datetime], bool]:
    """(location_key, hour) -> is_complete for pairs already in raw_weather.hourly."""
    if not pairs:
        return {}
    rows = conn.execute(_CACHE_STATE_SQL, _pair_arrays(pairs)).fetchall()
    return {(key, hour): is_complete for key, hour, is_complete in rows}


def _count_still_missing(conn: psycopg.Connection, pairs: list[tuple[str, datetime]]) -> int:
    """Run location-hours not yet complete (absent, all-NULL, or
    direction unresolved per D29) — the drain loop's termination metric."""
    if not pairs:
        return 0
    return conn.execute(_STILL_MISSING_SQL, _pair_arrays(pairs)).fetchone()[0]


def _pair_arrays(pairs: list[tuple[str, datetime]]) -> dict:
    return {"keys": [key for key, _ in pairs], "hours": [hour for _, hour in pairs]}


def _merge_date_ranges(dates: list[date], gap_days: int) -> list[tuple[date, date]]:
    ranges = []
    start = end = dates[0]
    for day in dates[1:]:
        if (day - end).days <= gap_days:
            end = day
        else:
            ranges.append((start, end))
            start = end = day
    ranges.append((start, end))
    return ranges


def _floor(sync_start_date: date) -> datetime:
    return datetime.combine(sync_start_date, time.min, tzinfo=UTC)
