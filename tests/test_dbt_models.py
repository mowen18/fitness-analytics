"""dbt model behavior against the scratch database (@integration).

The real warehouse currently has no outdoor or HR-carrying runs, so the
weather-matching and eligibility logic in the dbt models can only be
exercised with synthetic fixtures. These tests seed the scratch DB
(conftest's running_analytics_test), run the real dbt project against it
in a subprocess, and assert model outputs; the last test proves the dbt
test suite FAILS when known-invalid fixtures are introduced — Phase 3
acceptance criterion 6.

All coordinates are deliberately fake.
"""

import json
import math
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from running_pipeline.config import Settings

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = REPO_ROOT / "dbt"
DBT_BIN = Path(sys.executable).parent / "dbt"
TEST_DB = "running_analytics_test"  # the conftest scratch database


@pytest.fixture
def db(integration_db):
    integration_db.execute(
        "TRUNCATE raw_strava.activities, raw_strava.streams, "
        "raw_strava.activity_coordinates, raw_weather.hourly, "
        "raw_strava.activity_details, raw_strava.segment_efforts, "
        "raw_strava.segments"
    )
    integration_db.commit()
    return integration_db


def run_dbt(*args: str) -> subprocess.CompletedProcess:
    """Run dbt against the scratch DB by overriding the profile env vars."""
    if not (DBT_DIR / "profiles.yml").exists():
        shutil.copy(DBT_DIR / "profiles.yml.example", DBT_DIR / "profiles.yml")
    settings = Settings()  # integration tests intentionally read .env
    env = {
        **os.environ,
        "POSTGRES_HOST": settings.postgres_host,
        "POSTGRES_PORT": str(settings.postgres_port),
        "POSTGRES_USER": settings.postgres_user,
        "POSTGRES_PASSWORD": settings.postgres_password.get_secret_value(),
        "POSTGRES_DB": TEST_DB,
    }
    return subprocess.run(
        [str(DBT_BIN), *args, "--profiles-dir", "."],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def insert_activity(
    conn,
    activity_id,
    *,
    sport_type="Run",
    start="2026-06-15T09:47:23Z",
    start_local="2026-06-15T04:47:23Z",
    start_latlng=(12.34, -56.78),
    trainer=False,
    distance=10000.0,
    moving_time=3000,
    elapsed_time=3100,
    average_heartrate=None,
    average_cadence=None,
    workout_type=None,
):
    payload = {
        "id": activity_id,
        "name": f"Fixture {activity_id}",
        "sport_type": sport_type,
        "start_date": start,
        "start_date_local": start_local,
        "timezone": "(GMT-06:00) America/Chicago",
        "start_latlng": list(start_latlng) if start_latlng else [],
        "trainer": trainer,
        "distance": distance,
        "moving_time": moving_time,
        "elapsed_time": elapsed_time,
        "total_elevation_gain": 42.0,
        "average_speed": distance / moving_time if moving_time else 0,
        "has_heartrate": average_heartrate is not None,
        "average_heartrate": average_heartrate,
        "average_cadence": average_cadence,
        "workout_type": workout_type,
    }
    conn.execute(
        """
        INSERT INTO raw_strava.activities
            (activity_id, start_date_utc, activity_type, payload, fetched_at)
        VALUES (%s, %s, %s, %s, now())
        """,
        (activity_id, datetime.fromisoformat(start), sport_type, json.dumps(payload)),
    )


def insert_weather(
    conn,
    *,
    hour,
    temperature,
    location="12.34_-56.78",
    apparent="same",
    wind_kph=None,
    wind_direction=None,
):
    # apparent="same" mirrors temperature (None stays all-NULL: the
    # missing-marker rows must stay missing in every measurement).
    # Passing apparent=None with a real temperature builds the partial
    # observation the v1.7 ladder must not drop: matched weather, no
    # feels-like value. wind_kph/wind_direction (C3, D29/D30) default to
    # NULL so every pre-C3 call site stays byte-identical; direction 0.0
    # is a value (north), never collapsed to missing.
    if apparent == "same":
        apparent = temperature
    lat, lon = (Decimal(part) for part in location.split("_"))
    conn.execute(
        """
        INSERT INTO raw_weather.hourly
            (location_key, latitude, longitude, weather_timestamp, temperature_c,
             apparent_temperature_c, relative_humidity_pct, wind_speed_kph,
             wind_direction_deg, payload, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        """,
        (
            location,
            lat,
            lon,
            datetime.fromisoformat(hour),
            temperature,
            apparent,
            None if temperature is None else 55,
            wind_kph,
            wind_direction,
            json.dumps({"time": hour}),
        ),
    )


@pytest.mark.integration
def test_dbt_build_matches_weather_and_flags_eligibility(db):
    # Outdoor run at 09:47 UTC with HR — the case real data cannot cover.
    insert_activity(db, 1, average_heartrate=145.0)
    # Treadmill run: stays in the grain, permanently unmatched, no HR.
    insert_activity(db, 2, trainer=True, start_latlng=None)
    # Walk: must not reach fct_runs at all.
    insert_activity(db, 3, sport_type="Walk", trainer=True, start_latlng=None)
    # Map-privacy case: payload carries no coordinates, but the resolved
    # activity_coordinates row (decoded polyline) must drive the match.
    insert_activity(db, 4, start="2026-06-15T11:47:23Z", start_latlng=None, average_heartrate=None)
    db.execute(
        "INSERT INTO raw_strava.activity_coordinates VALUES "
        "(4, 22.446, 33.786, 'map_polyline', now())"
    )
    insert_weather(db, hour="2026-06-15T11:00:00+00:00", temperature=25.0, location="22.45_33.79")
    # 09:00 carries data; 10:00 is an explicit "archive had no data" row.
    # 10:00 is nearer to 09:47, but a missing-marker must never win the
    # match, so the model has to pick 09:00 (47 minutes away).
    insert_weather(db, hour="2026-06-15T09:00:00+00:00", temperature=20.0)
    insert_weather(db, hour="2026-06-15T10:00:00+00:00", temperature=None)
    db.commit()

    result = run_dbt("build")

    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    rows = db.execute(
        """
        SELECT activity_id, weather_available, temperature_c, weather_match_minutes,
               long_run_eligible, pace_min_per_mi, apparent_temperature_c
        FROM analytics.fct_runs ORDER BY activity_id
        """
    ).fetchall()
    assert len(rows) == 3  # the Walk is filtered out

    outdoor, treadmill, resolved = rows
    assert outdoor[1] is True  # matched despite the nearer all-NULL row
    assert outdoor[2] == Decimal("20.0")  # the 09:00 observation, not NULL
    assert outdoor[3] == 47
    assert outdoor[4] is True  # 50 min moving >= the 45-minute long-run bar
    assert outdoor[5] == Decimal("8.05")
    # Feels-like rides the same lineage raw -> fct (v1.7 banding input);
    # the fixture mirrors it from temperature by default.
    assert outdoor[6] == Decimal("20.0")

    assert treadmill[1] is False  # no coordinates: explicit, not an error
    assert treadmill[2] is None  # missing weather stays NULL, never zero
    assert treadmill[4] is True  # 50 min moving

    # The map-privacy fallback: coordinates came from the resolved
    # activity_coordinates row, so weather still matches (11:00 obs at
    # the run's D7 cell, 47 minutes from the 11:47 start).
    assert resolved[1] is True
    assert resolved[2] == Decimal("25.0")
    assert resolved[3] == 47


def outdoor_run(
    db, activity_id, *, day, hr, cell="12.34_-56.78", temp_c=15.0, apparent_c="same", **kwargs
):
    """A valid-shaped run plus a matching observation at its cell.

    Defaults give speed 200 m/min (10 km in 50 min), so efficiency is
    200 / hr — hand-checkable. temp_c=None skips the weather row so the
    run lands in the explicit no_weather pseudo-band. apparent_c=None
    keeps the observation but strips its feels-like value (v1.7 gap
    witness); the default mirrors temp_c.
    """
    lat, lon = (float(part) for part in cell.split("_"))
    insert_activity(
        db,
        activity_id,
        start=f"{day}T09:47:23Z",
        start_local=f"{day}T04:47:23Z",
        start_latlng=(lat, lon),
        average_heartrate=hr,
        **kwargs,
    )
    if temp_c is not None:
        insert_weather(
            db, hour=f"{day}T09:00:00+00:00", temperature=temp_c, location=cell, apparent=apparent_c
        )


@pytest.mark.integration
def test_efficiency_marts_compute_metrics_exclusions_and_bands(db):
    # ── Week of Mon 2026-06-01: two valid runs (sufficient per D12).
    # temp_c values are chosen so the staged 1-dp value lands exactly on
    # the D14 band edges: 9.94°C -> 49.9°F (cold), 10.0°C -> 50.0°F
    # (mild). Bands read the apparent temperature (v1.7), which mirrors
    # dry-bulb here by default — the edges hold on the banding axis.
    outdoor_run(db, 1, day="2026-06-02", hr=140.0, cell="10.00_10.00", temp_c=9.94)
    outdoor_run(db, 2, day="2026-06-04", hr=150.0, cell="11.00_11.00", temp_c=10.0)
    # ── Week of Mon 2026-06-08: a valid run without weather, two runs
    # the old D9 ladder excluded (race tag, hard effort) that are VALID
    # under v1.1, and one run per remaining data-validity rule.
    outdoor_run(db, 3, day="2026-06-09", hr=145.0, temp_c=None)
    outdoor_run(db, 4, day="2026-06-10", hr=150.0, workout_type=1)  # race: valid now
    outdoor_run(db, 5, day="2026-06-11", hr=None)  # no HR
    outdoor_run(db, 6, day="2026-06-12", hr=160.0)  # hard effort: valid now
    # 10 min moving at an in-bounds pace (2 km), so the duration rung —
    # not the pace rung that precedes it — is the one that fires.
    outdoor_run(
        db, 7, day="2026-06-13", hr=140.0, distance=2000.0, moving_time=600, elapsed_time=700
    )
    outdoor_run(db, 11, day="2026-06-08", hr=210.0)  # outside sanity band
    outdoor_run(db, 12, day="2026-06-14", hr=140.0, distance=1000.0)  # ~80 min/mi
    # ── Week of Mon 2026-06-15: the 70°F edge on the banding axis —
    # bands read the APPARENT temperature (v1.7). Run 8 is the v1.7
    # migration witness: dry-bulb 21.11°C -> 70.0°F banded mild
    # pre-v1.7; apparent 21.17°C -> 70.1°F bands warm (70–80°F). Run 9
    # diverges in value (73.0°F feels-like) but not in band.
    outdoor_run(
        db, 8, day="2026-06-16", hr=145.0, cell="12.00_12.00", temp_c=21.11, apparent_c=21.17
    )
    outdoor_run(
        db, 9, day="2026-06-17", hr=145.0, cell="13.00_13.00", temp_c=21.17, apparent_c=22.78
    )
    # Gap witness (v1.7): a matched observation carrying dry-bulb but no
    # feels-like value — weather_available stays true while the banding
    # input is NULL. Must land in no_weather ("no matched feels-like
    # temperature"), never vanish.
    outdoor_run(db, 13, day="2026-06-18", hr=145.0, cell="15.00_15.00", apparent_c=None)
    # ── Valid treadmill run (isolated week of Mon 2026-05-18):
    # must land in the explicit 'indoor' pseudo-band, not 'no_weather'.
    insert_activity(
        db,
        10,
        start="2026-05-20T09:47:23Z",
        start_local="2026-05-20T04:47:23Z",
        start_latlng=None,
        trainer=True,
        average_heartrate=145.0,
    )
    # Trainer witness (v1.7): a trainer run WITH coordinates and matched
    # weather — 'indoor' must win alone. Before the seed-leg trainer
    # guard it also matched a seed band: the double-count the run-grain
    # totality test catches and the aggregate conservation test cannot.
    outdoor_run(db, 14, day="2026-05-21", hr=145.0, cell="14.00_14.00", trainer=True)
    # ── Week of Mon 2026-06-22: the v1.8 boundary witnesses, pinning
    # the 1-dp bound encoding at the two new cuts plus the unbounded
    # top. Run 15's staged 80.0 comes from 80.006 rounding DOWN — a
    # bound-encoding witness like run 8's 70.106 -> 70.1: exactly 80.0
    # is the INCLUSIVE top of 70–80°F (mirroring 70.0-stays-mild), and
    # 90.0 is the inclusive top of 80–90°F.
    outdoor_run(
        db, 15, day="2026-06-23", hr=145.0, cell="16.00_16.00", temp_c=26.0, apparent_c=26.67
    )
    outdoor_run(
        db, 16, day="2026-06-24", hr=145.0, cell="17.00_17.00", temp_c=30.0, apparent_c=32.22
    )
    outdoor_run(
        db, 17, day="2026-06-25", hr=145.0, cell="18.00_18.00", temp_c=33.0, apparent_c=35.0
    )
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    # Exclusion reasons: first failing validity rule (v1.1 order),
    # never a silent filter — and no intensity/intent rungs anymore.
    reasons = dict(
        db.execute(
            "SELECT activity_id, exclusion_reason FROM intermediate.int_run_efficiency ORDER BY 1"
        ).fetchall()
    )
    assert reasons == {
        1: None,
        2: None,
        3: None,
        4: None,  # race tag no longer excludes (v1.1)
        5: "no heart rate data",
        6: None,  # no intensity ceiling anymore (v1.1)
        7: "moving time under 15 minutes",
        8: None,
        9: None,
        10: None,  # treadmill runs are valid — weather isn't a rule
        11: "average HR outside 90–200 bpm sanity band",
        12: "pace outside 4.0–20.0 min/mi bounds",
        13: None,  # matched weather without feels-like: still valid
        14: None,  # trainer with matched weather: still valid
        15: None,  # 80.0°F boundary witness (v1.8)
        16: None,  # 90.0°F boundary witness (v1.8)
        17: None,  # 95.0°F top-band witness (v1.8)
    }

    # Efficiency traces to documented fields: 200 m/min at 140 bpm.
    (efficiency,) = db.execute(
        "SELECT aerobic_efficiency_m_per_heartbeat FROM intermediate.int_run_efficiency "
        "WHERE activity_id = 1"
    ).fetchone()
    assert float(efficiency) == pytest.approx(200.0 / 140.0, abs=0.0001)

    # Weekly mart: D12 sufficiency and NULL-not-zero efficiency.
    weeks = db.execute(
        """
        SELECT week_start_date::text, valid_run_count, is_sufficient,
               median_efficiency_m_per_beat
        FROM analytics.mart_weekly_training ORDER BY week_start_date
        """
    ).fetchall()
    assert [(w[0], w[1], w[2]) for w in weeks] == [
        ("2026-05-18", 2, True),  # treadmill + the trainer witness (v1.7)
        ("2026-06-01", 2, True),
        ("2026-06-08", 3, True),  # race + hard effort now count (v1.1)
        ("2026-06-15", 3, True),  # the gap witness joins the week (v1.7)
        ("2026-06-22", 3, True),  # the v1.8 boundary witnesses' week
    ]
    # Week 1 median interpolates between 200/150 and 200/140.
    assert float(weeks[1][3]) == pytest.approx((200.0 / 150.0 + 200.0 / 140.0) / 2, abs=0.0001)

    # Trend mart: the 28-day window ending Sun 2026-06-21 spans all
    # eight valid runs after Sun 2026-05-24 (runs 10 and 14 in the
    # 05-18 week fall outside). Weekly averages are never banded (v1.7).
    (rolling_count, rolling_median) = db.execute(
        """
        SELECT rolling_valid_run_count, rolling_median_efficiency
        FROM analytics.mart_efficiency_trend WHERE week_start_date = '2026-06-15'
        """
    ).fetchone()
    assert rolling_count == 8
    assert float(rolling_median) == pytest.approx(200.0 / 145.0, abs=0.0001)

    # Band mart: every band present, boundaries exact, nothing dropped.
    bands = {
        row[0]: (row[1], row[2])
        for row in db.execute(
            """
            SELECT band_key, valid_run_count, median_efficiency_m_per_beat
            FROM analytics.mart_efficiency_by_temp_band
            """
        ).fetchall()
    }
    assert bands["cold"][0] == 1  # 49.9°F
    # The 50.0°F edge plus the race and the hard effort at the 59.0°F
    # default. Run 8 left this band with v1.7: its 70.1°F feels-like
    # bands warm even though its 70.0°F dry-bulb banded mild before.
    assert bands["mild"][0] == 3
    # 70–80°F (v1.8 narrowed): the v1.7 migration witness (70.1°F),
    # 73.0°F, and the 80.0°F inclusive-top-edge witness.
    assert bands["warm"][0] == 3
    assert bands["hot"][0] == 1  # 90.0°F — inclusive top of 80–90°F
    assert bands["very_hot"][0] == 1  # 95.0°F — the unbounded top band
    # "Not applicable" and "missing" stay distinct — and indoor wins
    # alone for the trainer run with matched weather (v1.7 guard).
    assert bands["indoor"][0] == 2
    # Unmatched outdoor + matched-without-feels-like (v1.7): both are
    # explicit rows, never silent drops.
    assert bands["no_weather"][0] == 2
    assert sum(count for count, _ in bands.values()) == 13  # conservation

    # Run-level quality mart: every run visible with its verdict, band,
    # and an efficiency value even when invalid.
    quality = {
        row[0]: (row[1], row[2], row[3])
        for row in db.execute(
            "SELECT activity_id, exclusion_reason, temperature_band_label, "
            "       aerobic_efficiency_m_per_heartbeat "
            "FROM analytics.mart_run_quality"
        ).fetchall()
    }
    assert len(quality) == 17  # every run, valid or not
    assert quality[4][0] is None  # the race counts now (v1.1)
    assert quality[11][0] == "average HR outside 90–200 bpm sanity band"
    assert quality[11][2] is not None  # invalid runs keep their value
    assert quality[1][1] == "< 50°F"  # banded per-run by its own feels-like
    assert quality[8][1] == "70–80°F"  # the v1.7 migration witness (70.1°F)
    assert quality[15][1] == "70–80°F"  # 80.0°F: inclusive top edge (v1.8)
    assert quality[16][1] == "80–90°F"  # 90.0°F: inclusive top edge (v1.8)
    assert quality[17][1] == "> 90°F"  # 95.0°F: the unbounded top (v1.8)
    assert quality[3][1] == "weather unavailable"  # outdoor, unmatched
    assert quality[13][1] == "weather unavailable"  # matched, no feels-like
    assert quality[10][1] == "indoor"  # trainer: not applicable, not missing
    assert quality[14][1] == "indoor"  # trainer wins over matched weather
    assert quality[5][2] is None  # no HR -> no value (missing, not zero)


def insert_stream(db, activity_id, *, status="success", samples=None, drop=()):
    """Store a synthetic key_by_type streams payload (or a status-only row).

    `drop` removes stream types from an otherwise complete payload — the
    'missing required stream types' fixtures need a success row whose
    arrays are incomplete.
    """
    payload = {}
    sample_count = None
    if samples is not None:
        elapsed, hr, velocity, moving = samples
        sample_count = len(elapsed)
        payload = {
            "time": {"data": elapsed, "original_size": sample_count},
            "heartrate": {"data": hr, "original_size": sample_count},
            "velocity_smooth": {"data": velocity, "original_size": sample_count},
            "moving": {"data": moving, "original_size": sample_count},
            "grade_smooth": {"data": [0.0] * sample_count, "original_size": sample_count},
        }
        for stream_type in drop:
            payload.pop(stream_type, None)
    db.execute(
        """
        INSERT INTO raw_strava.streams
            (activity_id, payload, sample_count, fetched_at, ingestion_status)
        VALUES (%s, %s, %s, now(), %s)
        """,
        (activity_id, json.dumps(payload), sample_count, status),
    )


def steady_stream(*, seconds=3600, step=1, first_half_hr=140.0, second_half_hr=150.0, pause=None):
    """A hand-checkable synthetic run at constant 3.0 m/s (180 m/min).

    The D16 window for a 3600 s run is 600..3300 s, midpoint 1950. HR
    switches exactly at the midpoint, and the warm-up/cool-down carry
    absurd values (10 m/s sprint, 0.5 m/s crawl) so any trimming bug
    changes the expected numbers. `pause` marks [start, end) elapsed
    seconds as non-moving. Efficiency per half = 180 / HR, so
    decoupling_pct = (1 - first_half_hr / second_half_hr) * 100.
    """
    elapsed = list(range(0, seconds, step))
    hr, velocity, moving = [], [], []
    for t in elapsed:
        if t < 600:
            hr.append(120.0)
            velocity.append(10.0)  # would inflate half 1 if trimming broke
        elif t > seconds - 300:
            hr.append(190.0)
            velocity.append(0.5)  # would poison half 2 if trimming broke
        else:
            hr.append(first_half_hr if t <= (600 + seconds - 300) / 2 else second_half_hr)
            velocity.append(3.0)
        moving.append(not (pause and pause[0] <= t < pause[1]))
    return elapsed, hr, velocity, moving


def band_stream(
    *, seconds=3600, step=1, first_hr=135.0, second_hr=145.0, velocity=3.0, hr_schedule=None
):
    """A hand-checkable synthetic run for the D22 BAND window.

    The steady_stream pattern re-sized to the band trims: the D22
    window for the run is 300 .. (last sample - 120) s, and the
    warm-up/cool-down carry absurd values (10 m/s sprint at HR 120,
    0.5 m/s crawl at HR 190) so any trimming bug visibly shifts the
    band inventory and its medians. By default HR switches exactly at
    the band-window midpoint, so the run splits its dwell almost evenly
    between two decade bands at constant `velocity`; `hr_schedule(t)`
    overrides the two-band default for ladder fixtures (band cycling,
    short band visits).
    """
    window_start = 300  # band_warmup_trim_minutes * 60
    window_end = (seconds - step) - 120  # last sample - band_cooldown_trim_minutes * 60
    midpoint = (window_start + window_end) / 2
    elapsed = list(range(0, seconds, step))
    hr, vel, moving = [], [], []
    for t in elapsed:
        if t < window_start:
            hr.append(120.0)
            vel.append(10.0)  # would pollute the low bands if trimming broke
        elif t > seconds - 120:
            hr.append(190.0)
            vel.append(0.5)  # would poison the high bands if trimming broke
        else:
            if hr_schedule is not None:
                hr.append(hr_schedule(t))
            else:
                hr.append(first_hr if t <= midpoint else second_hr)
            vel.append(velocity)
        moving.append(True)
    return elapsed, hr, vel, moving


def drift_run(db, activity_id, *, day, moving_time=3600, hr=145.0, **kwargs):
    """An HR-carrying, drift-length activity (no weather needed)."""
    insert_activity(
        db,
        activity_id,
        start=f"{day}T09:00:00Z",
        start_local=f"{day}T04:00:00Z",
        start_latlng=None,
        trainer=False,
        distance=10800.0,
        moving_time=moving_time,
        elapsed_time=moving_time + 100,
        average_heartrate=hr,
        **kwargs,
    )


@pytest.mark.integration
def test_drift_decoupling_formula_and_analysis_window(db):
    # Two clean runs in one week: decoupling = (1 - HR1/HR2) * 100
    # exactly, because speed is constant across the window. The second
    # is tagged as a race: intensity/intent no longer gates drift
    # candidacy (D15 revised v1.1) — only duration and HR do.
    drift_run(db, 1, day="2026-06-15")
    insert_stream(db, 1, samples=steady_stream())  # 140 -> 150: 6.667 %
    drift_run(db, 2, day="2026-06-17", workout_type=1)
    insert_stream(db, 2, samples=steady_stream(second_half_hr=145.0))  # 3.448 %
    # Excluded candidates, one per D16 ladder rung that data can reach:
    drift_run(db, 3, day="2026-06-18")  # no streams row at all
    drift_run(db, 4, day="2026-06-19")
    insert_stream(db, 4, status="unavailable")
    drift_run(db, 5, day="2026-06-20", moving_time=2700)  # 45 min run,
    insert_stream(db, 5, samples=steady_stream(seconds=2400))  # 25-min window
    drift_run(db, 6, day="2026-06-21")
    insert_stream(db, 6, samples=steady_stream(pause=(600, 1420)))  # ~30 % paused
    drift_run(db, 7, day="2026-06-22")
    insert_stream(db, 7, samples=steady_stream(step=5))  # 5 s gaps > 3 s max
    # Not a candidate: short run (< 45 min) must not appear at all.
    insert_activity(
        db, 8, start_latlng=None, average_heartrate=140.0, moving_time=2400, elapsed_time=2500
    )
    # Not a candidate either: long enough, but no HR (the other half of
    # the revised D15 gate).
    drift_run(db, 9, day="2026-06-23", hr=None)
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    halves = {
        row[0]: (row[1], row[2])
        for row in db.execute(
            "SELECT activity_id, decoupling_pct, exclusion_reason "
            "FROM analytics.fct_drift_candidates"
        ).fetchall()
    }
    assert set(halves) == {1, 2, 3, 4, 5, 6, 7}  # 8 (short) and 9 (no HR) are not candidates

    # The formula, exactly (acceptance criterion 5).
    assert float(halves[1][0]) == pytest.approx((1 - 140 / 150) * 100, abs=0.01)
    assert float(halves[2][0]) == pytest.approx((1 - 140 / 145) * 100, abs=0.01)

    # Deterministic reasons, first failing check per run (criterion 4).
    assert halves[3] == (None, "streams not yet loaded")
    assert halves[4] == (None, "streams unavailable from Strava")
    assert halves[5][1] == "analysis window under 30 minutes after trimming"
    assert halves[6][1] == "excessive pauses (non-moving share above 0.25)"
    assert halves[7][1] == "insufficient sample coverage"

    # The documented window: 3600 s run -> 600..3300 -> 45 minutes.
    (window_min, first_hr, second_hr) = db.execute(
        "SELECT analysis_window_min, first_half_hr_bpm, second_half_hr_bpm "
        "FROM analytics.mart_run_drift WHERE activity_id = 1"
    ).fetchone()
    assert float(window_min) == 45.0
    assert (float(first_hr), float(second_hr)) == (140.0, 150.0)  # trim held

    # Only analyzed runs reach the mart; the trend week is sufficient.
    assert db.execute("SELECT count(*) FROM analytics.mart_run_drift").fetchone()[0] == 2
    (count, median, sufficient) = db.execute(
        "SELECT drift_run_count, median_decoupling_pct, is_sufficient "
        "FROM analytics.mart_drift_trend WHERE week_start_date = '2026-06-15'"
    ).fetchone()
    assert count == 2
    expected_median = ((1 - 140 / 150) * 100 + (1 - 140 / 145) * 100) / 2
    assert float(median) == pytest.approx(expected_median, abs=0.01)
    assert sufficient is True


@pytest.mark.integration
def test_dbt_tests_fail_on_known_invalid_fixtures(db):
    # Physically impossible: moving time exceeds elapsed time.
    insert_activity(db, 1, moving_time=4000, elapsed_time=3000)
    # Out-of-range humidity on an otherwise valid observation.
    insert_weather(db, hour="2026-06-15T09:00:00+00:00", temperature=20.0)
    db.execute("UPDATE raw_weather.hourly SET relative_humidity_pct = 150")
    db.commit()

    result = run_dbt("build")

    assert result.returncode != 0, "dbt build should fail on invalid fixtures"
    assert "assert_moving_time_not_longer_than_elapsed" in result.stdout
    assert "accepted_range_stg_weather__hourly_relative_humidity_pct" in result.stdout


@pytest.mark.integration
def test_relationships_test_fails_on_orphan_drift_candidate(db):
    # The fct_drift_candidates -> fct_runs relationships test was proven
    # red-first once (commit 6e478af); this re-proves it on every run.
    # Both models descend from int_run_efficiency, so no raw fixture can
    # produce an orphan — it must be injected into the built table.
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    # Insert AFTER the build and test WITHOUT rebuilding: a rebuild
    # would erase the orphan and a green run would prove nothing.
    db.execute("INSERT INTO analytics.fct_drift_candidates (activity_id) VALUES (999999999)")
    db.commit()

    selector = "relationships_fct_drift_candidates_activity_id__activity_id__ref_fct_runs_"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "relationships test should fail on an orphan candidate"
    assert selector in result.stdout

    db.execute("DELETE FROM analytics.fct_drift_candidates WHERE activity_id = 999999999")
    db.commit()

    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"relationships test still failing:\n{result.stdout}"


@pytest.mark.integration
def test_relationships_test_fails_on_orphan_band_candidate(db):
    # The fct_band_candidates -> fct_runs relationships test, proven red
    # the same way as the drift one (commit f8c7185): both models descend
    # from int_run_efficiency, so no raw fixture can produce an orphan —
    # it must be injected into the built table.
    drift_run(db, 1, day="2026-06-15")
    insert_stream(db, 1, samples=steady_stream())
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    # Insert AFTER the build and test WITHOUT rebuilding: a rebuild
    # would erase the orphan and a green run would prove nothing.
    db.execute("INSERT INTO analytics.fct_band_candidates (activity_id) VALUES (999999999)")
    db.commit()

    selector = "relationships_fct_band_candidates_activity_id__activity_id__ref_fct_runs_"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "relationships test should fail on an orphan candidate"
    assert selector in result.stdout

    db.execute("DELETE FROM analytics.fct_band_candidates WHERE activity_id = 999999999")
    db.commit()

    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"relationships test still failing:\n{result.stdout}"


@pytest.mark.integration
def test_totality_test_fails_on_unassignable_run(db):
    # The D14 ladder totality test was proven red-first on both arms
    # (commit 2c2dba2: a matched run without feels-like -> 0 assignments,
    # a trainer run with matched weather -> 2); this re-proves the
    # zero-assignment arm on every run. The mart and the test derive
    # from the same fct_runs, so no raw fixture can break the partition
    # once the ladder is correct — the bad row must be injected into the
    # built table.
    outdoor_run(db, 1, day="2026-06-02", hr=140.0)
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    # Insert AFTER the build and test WITHOUT rebuilding: a rebuild
    # would erase the row and a green run would prove nothing. NULL
    # is_trainer/weather_available fall through every ladder leg.
    db.execute("INSERT INTO analytics.fct_runs (activity_id, is_valid) VALUES (999999999, true)")
    db.commit()

    selector = "assert_temp_band_ladder_assigns_every_valid_run_once"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "totality test should fail on an unassignable valid run"
    assert selector in result.stdout

    db.execute("DELETE FROM analytics.fct_runs WHERE activity_id = 999999999")
    db.commit()

    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"totality test still failing:\n{result.stdout}"


@pytest.mark.integration
def test_band_candidates_exclusive_exhaustive(db):
    # D22 contract: every band candidate carries EXACTLY ONE of
    # (>= 1 band segment, exclusion_reason) — mutually exclusive,
    # jointly exhaustive. Fixtures put data on both sides so neither
    # direction passes vacuously: an analyzed hour-long run, and a
    # 25-minute HR run past the fetch gate but with no streams row yet.
    drift_run(db, 1, day="2026-06-15")
    insert_stream(db, 1, samples=steady_stream())
    drift_run(db, 2, day="2026-06-16", moving_time=1500)  # candidate, no streams
    # Not a candidate: HR run under the 20-minute fetch gate.
    insert_activity(
        db, 3, start_latlng=None, average_heartrate=140.0, moving_time=900, elapsed_time=1000
    )
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    candidates = {
        row[0]
        for row in db.execute("SELECT activity_id FROM analytics.fct_band_candidates").fetchall()
    }
    assert candidates == {1, 2}  # 3 is under the fetch gate

    violations = db.execute(
        """
        SELECT c.activity_id, c.exclusion_reason, coalesce(s.segment_count, 0)
        FROM analytics.fct_band_candidates c
        LEFT JOIN (
            SELECT activity_id, count(*) AS segment_count
            FROM analytics.fct_run_band_segments
            GROUP BY activity_id
        ) s USING (activity_id)
        WHERE (c.exclusion_reason IS NULL) != (coalesce(s.segment_count, 0) >= 1)
        """
    ).fetchall()
    assert violations == [], f"candidates violating the segments-XOR-reason contract: {violations}"


@pytest.mark.integration
def test_band_medians_dwell_and_exclusion_ladder(db):
    # Acceptance criterion 3: constant 3.0 m/s, HR stepping 135 -> 145
    # at the band-window midpoint -> two decade bands, near-equal dwell,
    # median velocity 3.0 in each. The poisoned warm-up/cool-down means
    # a trimming break shifts every number asserted below.
    drift_run(db, 1, day="2026-06-15")
    insert_stream(db, 1, samples=band_stream())
    # Same week, slower run: the weekly statistic must be the median
    # ACROSS RUNS of the run-level medians (D11), not a sample pool.
    drift_run(db, 2, day="2026-06-17")
    insert_stream(db, 2, samples=band_stream(first_hr=137.0, second_hr=147.0, velocity=2.5))
    # Criterion 4's dwell-minimum fixture: a 2-minute visit to the 150s
    # band must leave that band ABSENT while the run itself analyzes.
    drift_run(db, 3, day="2026-06-22")
    insert_stream(
        db,
        3,
        samples=band_stream(
            hr_schedule=lambda t: 155.0 if t < 420 else (135.0 if t <= 1889.5 else 145.0)
        ),
    )
    # One fixture per remaining reachable ladder rung (criterion 4):
    drift_run(db, 4, day="2026-06-18")  # no streams row at all
    drift_run(db, 5, day="2026-06-19")
    insert_stream(db, 5, status="unavailable")
    drift_run(db, 6, day="2026-06-19")
    insert_stream(db, 6, status="failed")
    drift_run(db, 7, day="2026-06-20")
    insert_stream(db, 7, samples=band_stream(), drop=("heartrate",))
    drift_run(db, 8, day="2026-06-20", moving_time=1500)  # 25-min run,
    insert_stream(db, 8, samples=band_stream(seconds=900))  # ~8-min window
    drift_run(db, 9, day="2026-06-21")
    insert_stream(db, 9, samples=band_stream(step=5))  # 5 s gaps > 3 s max
    drift_run(db, 10, day="2026-06-21", moving_time=1500)
    insert_stream(  # cycles FIVE bands every 60 s across an 18-block
        # window: no band collects more than 4 blocks (240 s < 5 min)
        db,
        10,
        samples=band_stream(seconds=1500, hr_schedule=lambda t: 135.0 + 10.0 * ((t // 60) % 5)),
    )
    # Not candidates: an HR run under the 20-minute FETCH gate, and a
    # long run without HR (the other half of the revised D15 gate).
    insert_activity(
        db, 11, start_latlng=None, average_heartrate=140.0, moving_time=900, elapsed_time=1000
    )
    drift_run(db, 12, day="2026-06-23", hr=None)
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    candidates = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT activity_id, exclusion_reason FROM analytics.fct_band_candidates"
        ).fetchall()
    }
    assert set(candidates) == {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}  # 11 (short) and 12 (no HR) are out

    # Deterministic reasons, first failing check per run (the ladder).
    assert candidates[4] == "streams not yet loaded"
    assert candidates[5] == "streams unavailable from Strava"
    assert candidates[6] == "stream fetch failed (retried by next backfill)"
    assert candidates[7] == "missing required stream types"
    assert candidates[8] == "analysis window under 10 minutes after trimming"
    assert candidates[9] == "insufficient sample coverage"
    assert candidates[10] == "no band meets the dwell minimum"
    assert candidates[1] is None and candidates[2] is None and candidates[3] is None

    segments = {
        (row[0], row[1]): (float(row[2]), float(row[3]), float(row[4]))
        for row in db.execute(
            "SELECT activity_id, band_key, dwell_s, median_velocity_m_per_s,"
            "       median_pace_min_per_mi"
            "  FROM analytics.fct_run_band_segments"
        ).fetchall()
    }
    # Run 1, hand-checked: window 300..3479 s; 135 bpm through t=1889
    # (1590 samples: the first carries the 3 s dwell cap, the rest 1 s
    # gaps), 145 bpm for t=1890..3479 (1590 samples, all 1 s gaps).
    # Median velocity 3.0 m/s -> 1609.344 / 180 = 8.94 min/mi.
    assert segments[(1, "130_139")] == (1592.0, 3.0, 8.94)
    assert segments[(1, "140_149")] == (1590.0, 3.0, 8.94)
    # Run 2: same shape at 2.5 m/s -> 1609.344 / 150 = 10.73 min/mi.
    assert segments[(2, "130_139")][1:] == (2.5, 10.73)
    assert segments[(2, "140_149")][1:] == (2.5, 10.73)
    # Run 3: the 2-minute 150s visit deposits NO segment (dwell 122 s
    # < 5 min); the run still contributes its two real bands.
    run3_bands = {band for (activity, band) in segments if activity == 3}
    assert run3_bands == {"130_139", "140_149"}

    # Weekly (D11 median-of-runs, D12 at week x band grain): the week
    # of 2026-06-15 has runs 1 (3.0 m/s) and 2 (2.5 m/s) in both bands
    # -> median velocity 2.75 -> 1609.344 / 165 = 9.75 min/mi.
    weekly = {
        row[0]: (row[1], float(row[2]), row[3])
        for row in db.execute(
            "SELECT band_key, contributing_run_count, median_pace_min_per_mi, is_sufficient"
            "  FROM analytics.mart_band_weekly WHERE week_start_date = '2026-06-15'"
        ).fetchall()
    }
    assert weekly["130_139"] == (2, 9.75, True)
    assert weekly["140_149"] == (2, 9.75, True)
    # Run 3's week has one contributing run: present but insufficient.
    (run3_sufficient,) = db.execute(
        "SELECT is_sufficient FROM analytics.mart_band_weekly"
        " WHERE week_start_date = '2026-06-22' AND band_key = '130_139'"
    ).fetchone()
    assert run3_sufficient is False

    # Trend: static rolling columns; the 28-day window ending the later
    # week's Sunday spans all three analyzed runs.
    trend = {
        row[0]: (float(row[1]), row[2], row[3])
        for row in db.execute(
            "SELECT week_start_date::text, rolling_median_pace_min_per_mi,"
            "       rolling_band_run_count, is_sufficient"
            "  FROM analytics.mart_band_trend WHERE band_key = '130_139'"
        ).fetchall()
    }
    assert trend["2026-06-15"] == (9.75, 2, True)  # runs 1 + 2
    assert trend["2026-06-22"] == (8.94, 3, False)  # runs 1 + 2 + 3: median 3.0 m/s

    # The reasons ride to the dashboard mart on the drift route.
    quality = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT activity_id, band_exclusion_reason FROM analytics.mart_run_quality"
        ).fetchall()
    }
    assert quality[4] == "streams not yet loaded"
    assert quality[1] is None  # analyzed
    assert quality[11] is None  # not a band candidate at all


# ── Cycling domain (v2.0 Phase C1): D23 grain, D25 ladder, weekly marts ─


def outdoor_ride(
    db,
    activity_id,
    *,
    day,
    sport_type="Ride",
    hr=None,
    cadence=None,
    cell="12.34_-56.78",
    temp_c=None,
    trainer=False,
    distance=32000.0,
    moving_time=7200,
):
    """A ride fixture, optionally with a matching observation at its cell.

    Defaults: 32 km in 2 h -> 9.9 mph, inside the D25 3-35 mph bounds.
    One fixture per day keeps starts distinct (the ordering-flake rule)
    and pins the training week via start_date_local.
    """
    lat, lon = (float(part) for part in cell.split("_"))
    insert_activity(
        db,
        activity_id,
        sport_type=sport_type,
        start=f"{day}T09:15:00Z",
        start_local=f"{day}T04:15:00Z",
        start_latlng=(lat, lon),
        trainer=trainer,
        distance=distance,
        moving_time=moving_time,
        elapsed_time=moving_time + 100,
        average_heartrate=hr,
        average_cadence=cadence,
    )
    if temp_c is not None:
        insert_weather(db, hour=f"{day}T09:00:00+00:00", temperature=temp_c, location=cell)


@pytest.mark.integration
def test_ride_grain_indoor_flags_and_ebike_absence(db):
    # C1 acceptance criterion 2: one row per D23 ride, e-bike types
    # absent, indoor flagged. Indoor rides get a matching observation at
    # their cell ON PURPOSE: weather_available must still be false (the
    # forced-NULL location_key, stronger than the running side).
    outdoor_ride(db, 101, day="2026-06-15", hr=140, cadence=85, temp_c=15.0)
    outdoor_ride(db, 102, day="2026-06-16", sport_type="GravelRide", cell="55.00_66.00")
    outdoor_ride(db, 103, day="2026-06-17", sport_type="MountainBikeRide")
    outdoor_ride(
        db, 104, day="2026-06-18", sport_type="VirtualRide", cell="21.00_-43.00", temp_c=16.0
    )
    outdoor_ride(db, 105, day="2026-06-19", trainer=True, cell="22.00_-44.00", temp_c=17.0)
    outdoor_ride(db, 106, day="2026-06-20", sport_type="EBikeRide")
    insert_activity(
        db,
        107,
        start="2026-06-21T09:15:00Z",
        start_local="2026-06-21T04:15:00Z",
        average_heartrate=140,
    )
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    rides = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT activity_id, sport_type, is_indoor, weather_available,"
            "       average_cadence_rpm FROM analytics.fct_rides"
        ).fetchall()
    }
    assert set(rides) == {101, 102, 103, 104, 105}  # e-bike and Run absent
    assert rides[101] == ("Ride", False, True, Decimal("85"))
    assert rides[102][1] is False and rides[102][3] is None  # cadence NULL, never 0
    assert rides[104][1] is True and rides[104][2] is False  # virtual: indoor, unmatched
    assert rides[105][1] is True and rides[105][2] is False  # trainer: indoor, unmatched

    run_rows = db.execute("SELECT activity_id FROM analytics.fct_runs").fetchall()
    assert [row[0] for row in run_rows] == [107]  # rides never leak into the running grain

    labels = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT activity_id, temperature_band_label FROM analytics.mart_ride_quality"
        ).fetchall()
    }
    assert labels[101] == "50–70°F"  # 15 °C -> 59 °F feels-like, seed band
    assert labels[102] == "weather unavailable"
    assert labels[104] == "indoor"
    assert labels[105] == "indoor"


@pytest.mark.integration
def test_ride_exclusion_ladder_first_failing_rule(db):
    # C1 acceptance criterion 3: every invalid ride carries the first
    # failing D25 rule; HR absence is NOT an exclusion (unlike running).
    outdoor_ride(db, 111, day="2026-06-15", hr=140)  # valid baseline
    # HR out of sanity AND speed out of bounds: the HR rung fires first.
    outdoor_ride(db, 112, day="2026-06-16", hr=210, moving_time=1800)
    outdoor_ride(db, 113, day="2026-06-17", hr=140, distance=48000.0, moving_time=2700)  # 39.8 mph
    outdoor_ride(db, 114, day="2026-06-18", hr=140, distance=0.0, moving_time=3600)  # speed NULL
    outdoor_ride(db, 115, day="2026-06-19", hr=140, distance=4023.36, moving_time=600)  # 10 min
    outdoor_ride(db, 116, day="2026-06-20")  # no HR: still valid
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    rows = {
        row[0]: (row[1], row[2])
        for row in db.execute(
            "SELECT activity_id, is_valid, exclusion_reason FROM analytics.fct_rides"
        ).fetchall()
    }
    assert rows[111] == (True, None)
    assert rows[112] == (False, "average HR outside 90–200 bpm sanity band")
    assert rows[113] == (False, "average speed outside 3–35 mph bounds")
    assert rows[114] == (False, "average speed outside 3–35 mph bounds")
    assert rows[115] == (False, "moving time under 15 minutes")
    assert rows[116] == (True, None)  # HR required only for HR aggregates
    assert all(valid == (reason is None) for valid, reason in rows.values())


@pytest.mark.integration
def test_weekly_cycling_sufficiency_flips_at_threshold(db):
    # C1 acceptance criterion 4: is_sufficient flips exactly at the
    # min_weekly_valid_rides threshold (2), counting VALID rides only.
    outdoor_ride(db, 121, day="2026-06-15", hr=140, cadence=90)  # week A, valid
    outdoor_ride(db, 122, day="2026-06-16", hr=145, cadence=80)  # week A, valid
    outdoor_ride(db, 123, day="2026-06-22", hr=140)  # week B, valid, no cadence
    outdoor_ride(db, 124, day="2026-06-23", hr=140, distance=4023.36, moving_time=600)  # invalid
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    weeks = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT week_start_date::text, ride_count, valid_ride_count,"
            "       is_sufficient, avg_cadence_rpm, rides_with_cadence"
            "  FROM analytics.mart_weekly_cycling"
        ).fetchall()
    }
    assert weeks["2026-06-15"] == (2, 2, True, Decimal("85"), 2)
    # Two rides but only one valid: below the threshold, and the cadence
    # average is NULL — never zero — when no valid ride carries cadence.
    assert weeks["2026-06-22"] == (2, 1, False, None, 0)


@pytest.mark.integration
def test_ebike_pin_test_fails_on_injected_ebike_row(db):
    # The D23 e-bike pin, proven red the injection way (the orphan
    # pattern above): the grain filter renders from ride_sport_types, so
    # no raw fixture can put an e-bike row into fct_rides — it must be
    # injected into the built table.
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    db.execute(
        "INSERT INTO analytics.fct_rides (activity_id, sport_type) VALUES (999999999, 'EBikeRide')"
    )
    db.commit()

    selector = "assert_fct_rides_excludes_ebike_types"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "e-bike pin should fail on an injected e-bike row"
    assert selector in result.stdout

    db.execute("DELETE FROM analytics.fct_rides WHERE activity_id = 999999999")
    db.commit()

    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"e-bike pin still failing:\n{result.stdout}"


# ── Cycling segments (v2.0 Phase C2): D24 chain, D28 trend mart ───────


def insert_segment(
    db, segment_id, *, name=None, distance=1200.0, start_latlng="default", end_latlng="default"
):
    # Endpoint kwargs (C3, D30): "default" keeps the C2 fixture cell;
    # None omits the key entirely (a Strava payload without endpoints).
    payload = {
        "id": segment_id,
        "name": name or f"Segment {segment_id}",
        "distance": distance,
        "average_grade": 1.4,
        "maximum_grade": 6.0,
        "city": "Testville",
        "state": "TS",
    }
    if start_latlng == "default":
        start_latlng = [12.34, -56.78]
    if end_latlng == "default":
        end_latlng = [12.35, -56.79]
    if start_latlng is not None:
        payload["start_latlng"] = start_latlng
    if end_latlng is not None:
        payload["end_latlng"] = end_latlng
    db.execute(
        "INSERT INTO raw_strava.segments (segment_id, payload, fetched_at) VALUES (%s, %s, now())",
        (segment_id, json.dumps(payload)),
    )


def insert_segment_effort(
    db,
    effort_id,
    activity_id,
    segment_id,
    *,
    elapsed,
    start,
    hr=None,
    cadence=None,
    pr_rank=None,
    moving=None,
    distance=None,
):
    # moving=None mirrors elapsed (the common case); distance=None
    # omits the key entirely (a payload without the field), so every
    # pre-speed call site stays byte-identical.
    payload = {
        "id": effort_id,
        "elapsed_time": elapsed,
        "moving_time": elapsed if moving is None else moving,
        "start_date": start,
        "start_date_local": start,
        "average_heartrate": hr,
        "average_cadence": cadence,
        "pr_rank": pr_rank,
    }
    if distance is not None:
        payload["distance"] = distance
    db.execute(
        "INSERT INTO raw_strava.segment_efforts"
        " (effort_id, activity_id, segment_id, payload, fetched_at)"
        " VALUES (%s, %s, %s, %s, now())",
        (effort_id, activity_id, segment_id, json.dumps(payload)),
    )


@pytest.mark.integration
def test_segment_sufficiency_flips_at_five_efforts(db):
    # C2 acceptance criterion 3, mart side: is_sufficient flips exactly
    # at segment_trend_min_efforts (5); the rolling median and the
    # cumulative best are pinned to hand-checked values.
    outdoor_ride(db, 201, day="2026-06-15", hr=140)
    insert_segment(db, 501, name="Four Efforts")
    insert_segment(db, 502, name="Five Efforts")
    for n in range(4):
        insert_segment_effort(
            db, 5010 + n, 201, 501, elapsed=300, start=f"2026-06-15T09:{20 + n:02d}:00Z"
        )
    for n, elapsed in enumerate([210, 200, 190, 220, 180]):
        insert_segment_effort(
            db, 5020 + n, 201, 502, elapsed=elapsed, start=f"2026-06-15T10:{10 + n:02d}:00Z"
        )
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    rows = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT effort_id, effort_seq, effort_count, is_sufficient,"
            "       rolling_median_elapsed_s, rolling_effort_count, best_elapsed_s"
            "  FROM analytics.mart_segment_trend"
        ).fetchall()
    }
    assert len(rows) == 9
    assert rows[5010] == (1, 4, False, Decimal("300.0"), 1, 300)  # 4 efforts: insufficient
    assert rows[5023] == (4, 5, True, Decimal("205.0"), 4, 190)  # window {210,200,190,220}
    # Fifth effort: full 5-effort window {210,200,190,220,180} -> 200.0;
    # cumulative best falls to 180.
    assert rows[5024] == (5, 5, True, Decimal("200.0"), 5, 180)


@pytest.mark.integration
def test_short_segment_flag_flips_at_median_120s(db):
    # C2 acceptance criterion 3, caveat side: short_segment flips
    # exactly at short_segment_seconds (120), strict < — a median of
    # exactly 120 is NOT short.
    outdoor_ride(db, 211, day="2026-06-15", hr=140)
    insert_segment(db, 503, name="Median 119")
    insert_segment(db, 504, name="Median 120")
    insert_segment_effort(db, 5031, 211, 503, elapsed=118, start="2026-06-15T09:20:00Z")
    insert_segment_effort(db, 5032, 211, 503, elapsed=120, start="2026-06-15T09:25:00Z")
    insert_segment_effort(db, 5041, 211, 504, elapsed=119, start="2026-06-15T09:30:00Z")
    insert_segment_effort(db, 5042, 211, 504, elapsed=121, start="2026-06-15T09:35:00Z")
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    flags = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT segment_id, bool_and(short_segment) FROM analytics.mart_segment_trend"
            " GROUP BY segment_id"
        ).fetchall()
    }
    assert flags == {503: True, 504: False}


@pytest.mark.integration
def test_virtual_effort_flagged_in_core_absent_from_mart_and_counted(db):
    # C2 acceptance criterion 4: a VirtualRide effort is flagged in
    # fct_segment_efforts, absent from mart_segment_trend, counted in
    # virtual_effort_count, and excluded from every displayed statistic.
    # A trainer-flagged OUTDOOR ride's effort stays in the mart: the
    # holdout is virtual, deliberately not is_indoor.
    outdoor_ride(db, 221, day="2026-06-15", hr=140)
    outdoor_ride(db, 222, day="2026-06-16", sport_type="VirtualRide")
    outdoor_ride(db, 223, day="2026-06-17", trainer=True)
    insert_segment(db, 505, name="Shared Hill")
    insert_segment_effort(db, 2211, 221, 505, elapsed=200, start="2026-06-15T09:20:00Z")
    insert_segment_effort(db, 2212, 221, 505, elapsed=210, start="2026-06-15T09:40:00Z")
    insert_segment_effort(db, 2221, 222, 505, elapsed=150, start="2026-06-16T09:20:00Z")
    insert_segment_effort(db, 2231, 223, 505, elapsed=220, start="2026-06-17T09:20:00Z")
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    core = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT effort_id, is_virtual_ride FROM analytics.fct_segment_efforts"
        ).fetchall()
    }
    assert core == {2211: False, 2212: False, 2221: True, 2231: False}

    mart = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT effort_id, effort_seq, effort_count, virtual_effort_count, best_elapsed_s"
            "  FROM analytics.mart_segment_trend"
        ).fetchall()
    }
    assert set(mart) == {2211, 2212, 2231}  # the virtual effort is absent
    assert mart[2211] == (1, 3, 1, 200)
    # The virtual effort's faster 150 s must never become the best, and
    # the trainer-outdoor effort takes seq 3.
    assert mart[2231] == (3, 3, 1, 200)


@pytest.mark.integration
def test_segment_pin_tests_fail_on_injected_rows(db):
    # The two NEW cross-relation pins, proven red the injection way (the
    # e-bike archetype above): the mart's virtual holdout and the
    # fct_segment_efforts -> fct_rides relationships test. Both are
    # unreachable from raw fixtures — the models enforce them by
    # construction — so the violating rows are injected into the built
    # tables.
    outdoor_ride(db, 231, day="2026-06-15", hr=140)
    outdoor_ride(db, 232, day="2026-06-16", sport_type="VirtualRide")
    insert_segment(db, 506)
    insert_segment_effort(db, 2311, 231, 506, elapsed=200, start="2026-06-15T09:20:00Z")
    insert_segment_effort(db, 2321, 232, 506, elapsed=150, start="2026-06-16T09:20:00Z")
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    # Pin 1: a virtual-flagged effort injected into the mart.
    db.execute(
        "INSERT INTO analytics.mart_segment_trend (effort_id, segment_id) VALUES (2321, 506)"
    )
    db.commit()
    selector = "assert_segment_trend_excludes_virtual_efforts"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "virtual holdout pin should fail on an injected virtual row"
    assert selector in result.stdout
    db.execute("DELETE FROM analytics.mart_segment_trend WHERE effort_id = 2321")
    db.commit()
    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"virtual holdout pin still failing:\n{result.stdout}"

    # Pin 2: an orphan effort (no parent in fct_rides) injected into core.
    db.execute(
        "INSERT INTO analytics.fct_segment_efforts"
        " (effort_id, activity_id, segment_id, elapsed_time_s, is_virtual_ride,"
        "  ride_is_valid, weather_available)"
        " VALUES (999999998, 999999997, 506, 200, false, true, false)"
    )
    db.commit()
    result = run_dbt("test", "--select", "fct_segment_efforts")
    assert result.returncode != 0, "relationships pin should fail on an orphan activity_id"
    assert "relationships_fct_segment_efforts" in result.stdout
    db.execute("DELETE FROM analytics.fct_segment_efforts WHERE effort_id = 999999998")
    db.commit()
    result = run_dbt("test", "--select", "fct_segment_efforts")
    assert result.returncode == 0, f"fct_segment_efforts tests still failing:\n{result.stdout}"


# ── Wind direction and headwind (v2.0 Phase C3): D29/D30 ──────────────

# The same IUGG mean radius the haversine_meters macro pins; the tests
# hand-compute expectations with it so a constant drift surfaces here.
EARTH_RADIUS_M = 6371008.8


def haversine_m(lat1, lon1, lat2, lon2):
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    half_dphi = math.radians(lat2 - lat1) / 2
    half_dlmb = math.radians(lon2 - lon1) / 2
    chord = math.sin(half_dphi) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(half_dlmb) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(chord))


@pytest.mark.integration
def test_winding_flag_flips_exactly_at_sinuosity_threshold(db):
    # C3 acceptance criterion 3, plus the D30 bearing pins: the
    # winding_segment flag flips exactly at winding_sinuosity_max (1.3),
    # strict > — D30 says "above", so a sinuosity of exactly 1.3000 is
    # NOT winding (the short_segment strict-< precedent, mirrored).
    # Bearings hand-checked: due north = 0, reversed = 180, and due east
    # ON THE EQUATOR = 90 (at any other latitude an east-pointing
    # segment's initial great-circle bearing is not exactly 90). A
    # 0.01°-latitude segment is R·0.01·π/180 ≈ 1111.95 m straight-line.
    north = haversine_m(12.34, -56.78, 12.35, -56.78)
    north_ends = {"start_latlng": [12.34, -56.78], "end_latlng": [12.35, -56.78]}
    insert_segment(db, 601, name="At The Bound", distance=round(1.3 * north, 3), **north_ends)
    insert_segment(db, 602, name="Just Above", distance=round(1.31 * north, 3), **north_ends)
    insert_segment(
        db,
        603,
        name="Southbound",
        start_latlng=[12.35, -56.78],
        end_latlng=[12.34, -56.78],
    )
    insert_segment(db, 604, name="Equator East", start_latlng=[0.0, 10.00], end_latlng=[0.0, 10.01])
    # Identical endpoints: zero straight-line, so bearing is undefined —
    # an unguarded atan2(0,0) would silently claim due north — and the
    # sinuosity division cannot happen. Unknown geometry stays NULL,
    # never false (missing-never-zero).
    insert_segment(db, 605, name="Loop", start_latlng=[12.34, -56.78], end_latlng=[12.34, -56.78])
    insert_segment(db, 606, name="No Endpoints", start_latlng=None, end_latlng=None)
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    rows = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT segment_id, bearing_deg, straight_line_m, sinuosity, winding_segment"
            "  FROM intermediate.int_segment_geometry"
        ).fetchall()
    }
    assert len(rows) == 6

    bearing, straight, sinuosity, winding = rows[601]
    assert float(bearing) == pytest.approx(0.0, abs=0.01)
    assert float(straight) == pytest.approx(1111.95, abs=0.01)
    assert sinuosity == Decimal("1.3000")
    assert winding is False  # exactly at the bound: NOT winding

    assert rows[602][2] == Decimal("1.3100")
    assert rows[602][3] is True  # just above the bound: winding

    assert float(rows[603][0]) == pytest.approx(180.0, abs=0.01)
    assert float(rows[604][0]) == pytest.approx(90.0, abs=0.01)

    loop_bearing, loop_straight, loop_sinuosity, loop_winding = rows[605]
    assert (loop_bearing, loop_sinuosity, loop_winding) == (None, None, None)
    assert float(loop_straight) == pytest.approx(0.0, abs=1e-9)

    assert rows[606] == (None, None, None, None)


@pytest.mark.integration
def test_headwind_sign_convention_and_effort_hour_match(db):
    # C3 acceptance criterion 1 (the D30 sign matrix, v2.0 verification
    # item 7 verbatim) plus the effort-hour wind match rules. Wind speed
    # is 16.09344 kph = exactly 10.0 mph on northbound segments
    # (bearing 0), so:
    #   wind FROM 0° (north)  -> headwind +10.0, crosswind 0.0 — and the
    #                            0° fixture doubles as the dbt-tier
    #                            "0 is a value" pin;
    #   wind FROM 180° (south)-> headwind -10.0 (tailwind), crosswind 0;
    #   wind FROM 90° (east)  -> headwind 0.0, crosswind 10.0 (full).
    # The wind is matched at the parent ride's start CELL but the
    # EFFORT's start time: the ride here matches the 09:00 observation
    # (direction 180) while the 11:40 effort matches 12:00 (direction
    # 0) — proving the effort-hour path is not the ride's. Beyond 60
    # minutes every effort_wind_* value is NULL; a matched hour with
    # speed but no direction carries the speed and NULL headwind
    # (missing direction is never zero). Winding efforts keep their
    # computed headwind — flagged, never nulled (D30).
    outdoor_ride(db, 241, day="2026-06-15", hr=140)
    north = haversine_m(12.34, -56.78, 12.35, -56.78)
    north_ends = {"start_latlng": [12.34, -56.78], "end_latlng": [12.35, -56.78]}
    insert_segment(db, 611, name="Northbound Straight", distance=1200.0, **north_ends)
    insert_segment(
        db, 612, name="Northbound Winding", distance=round(1.31 * north, 3), **north_ends
    )
    wind = {"wind_kph": 16.09344}
    insert_weather(
        db, hour="2026-06-15T09:00:00+00:00", temperature=20.0, wind_direction=180.0, **wind
    )
    insert_weather(
        db, hour="2026-06-15T12:00:00+00:00", temperature=21.0, wind_direction=0.0, **wind
    )
    insert_weather(
        db, hour="2026-06-15T14:00:00+00:00", temperature=22.0, wind_direction=90.0, **wind
    )
    insert_weather(
        db, hour="2026-06-15T18:00:00+00:00", temperature=23.0, wind_direction=None, **wind
    )
    insert_segment_effort(db, 2411, 241, 611, elapsed=200, start="2026-06-15T09:20:00Z")
    insert_segment_effort(db, 2412, 241, 611, elapsed=210, start="2026-06-15T11:40:00Z")
    insert_segment_effort(db, 2413, 241, 611, elapsed=220, start="2026-06-15T13:40:00Z")
    insert_segment_effort(db, 2414, 241, 611, elapsed=230, start="2026-06-15T16:30:00Z")
    insert_segment_effort(db, 2415, 241, 611, elapsed=240, start="2026-06-15T17:40:00Z")
    insert_segment_effort(db, 2416, 241, 612, elapsed=250, start="2026-06-15T09:25:00Z")
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    core = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT effort_id, bearing_deg, winding_segment, effort_wind_speed_mph,"
            "       effort_wind_direction_deg, effort_wind_match_minutes,"
            "       headwind_mph, crosswind_mph, weather_match_minutes"
            "  FROM analytics.fct_segment_efforts"
        ).fetchall()
    }
    assert len(core) == 6

    # Tailwind from due south; the ride itself matched the same 09:00
    # observation (15 minutes from its 09:15 start).
    assert core[2411] == (
        Decimal("0.0"),
        False,
        Decimal("10.0"),
        Decimal("180"),
        20,
        Decimal("-10.0"),
        Decimal("0.0"),
        15,
    )
    # Effort-hour, not ride-hour: direction 0 from the 12:00 observation
    # while the parent ride's own match stays the 09:00 hour. 0° = north
    # is a value and yields the full positive headwind.
    assert core[2412] == (
        Decimal("0.0"),
        False,
        Decimal("10.0"),
        Decimal("0"),
        20,
        Decimal("10.0"),
        Decimal("0.0"),
        15,
    )
    # Crosswind from due east: zero headwind, full crosswind.
    assert core[2413] == (
        Decimal("0.0"),
        False,
        Decimal("10.0"),
        Decimal("90"),
        20,
        Decimal("0.0"),
        Decimal("10.0"),
        15,
    )
    # Nearest observation 90 minutes away: beyond the 60-minute rule,
    # every effort-wind value is NULL (the match distance is context).
    assert core[2414] == (Decimal("0.0"), False, None, None, 90, None, None, 15)
    # Matched hour has speed but no direction: speed carried, headwind
    # and crosswind NULL — missing direction is never zero.
    assert core[2415] == (Decimal("0.0"), False, Decimal("10.0"), None, 20, None, None, 15)
    # Winding segment: headwind computed AND flagged, never nulled.
    assert core[2416] == (
        Decimal("0.0"),
        True,
        Decimal("10.0"),
        Decimal("180"),
        25,
        Decimal("-10.0"),
        Decimal("0.0"),
        15,
    )

    # The mart carries exactly the three pinned C3 columns, in lockstep
    # with core (amended D19: no allow-list change rides on this).
    mart = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT effort_id, headwind_mph, crosswind_mph, winding_segment"
            "  FROM analytics.mart_segment_trend"
        ).fetchall()
    }
    assert mart[2411] == (Decimal("-10.0"), Decimal("0.0"), False)
    assert mart[2412] == (Decimal("10.0"), Decimal("0.0"), False)
    assert mart[2414] == (None, None, False)
    assert mart[2416] == (Decimal("-10.0"), Decimal("0.0"), True)


@pytest.mark.integration
def test_wind_pin_tests_fail_on_injected_rows(db):
    # The two table-backed C3 pins, proven red the injection way (the
    # C2 archetype): the D30 formula/null pin on core and the mart-core
    # lockstep pin. Both are unreachable from raw fixtures — the models
    # enforce them by construction. The third C3 pin
    # (assert_segment_geometry_winding_matches_threshold) guards a VIEW,
    # so its red proof rides the boundary fixtures in
    # test_winding_flag_flips_exactly_at_sinuosity_threshold instead.
    outdoor_ride(db, 251, day="2026-06-15", hr=140)
    insert_segment(db, 507)
    insert_segment_effort(db, 2511, 251, 507, elapsed=200, start="2026-06-15T09:20:00Z")
    insert_weather(
        db,
        hour="2026-06-15T09:00:00+00:00",
        temperature=20.0,
        wind_kph=16.09344,
        wind_direction=0.0,
    )
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    # Pin 1: a core row whose stored headwind contradicts the formula.
    db.execute(
        "INSERT INTO analytics.fct_segment_efforts"
        " (effort_id, activity_id, segment_id, elapsed_time_s, is_virtual_ride,"
        "  ride_is_valid, weather_available, bearing_deg, effort_wind_speed_mph,"
        "  effort_wind_direction_deg, effort_wind_match_minutes, headwind_mph, crosswind_mph)"
        " VALUES (999999996, 999999997, 507, 200, false, true, false,"
        "         0.0, 10.0, 0.0, 20, -5.0, 0.0)"
    )
    db.commit()
    selector = "assert_headwind_matches_formula_and_null_rules"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "headwind formula pin should fail on an injected row"
    assert selector in result.stdout
    db.execute("DELETE FROM analytics.fct_segment_efforts WHERE effort_id = 999999996")
    db.commit()
    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"headwind formula pin still failing:\n{result.stdout}"

    # Pin 2: a mart row disagreeing with its core effort's headwind.
    db.execute(
        "INSERT INTO analytics.mart_segment_trend (effort_id, segment_id, headwind_mph)"
        " VALUES (2511, 507, 99.9)"
    )
    db.commit()
    selector = "assert_segment_trend_wind_matches_core"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "mart-core wind lockstep pin should fail on an injected row"
    assert selector in result.stdout
    db.execute("DELETE FROM analytics.mart_segment_trend WHERE headwind_mph = 99.9")
    db.commit()
    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"mart-core wind lockstep pin still failing:\n{result.stdout}"


# ── Effort speed (post-2.1): distance / ELAPSED time, in mph ──────────


@pytest.mark.integration
def test_effort_speed_formula_and_pins(db):
    # Effort speed uses ELAPSED time, the Strava convention for
    # segments (segments are "ranked according to 'Total Elapsed
    # Time'"; moving-time speed applies to activities, "not segments
    # or best efforts") — and D28's primary series. 1609.344 m in
    # 360 s is exactly 10.0 mph; moving_time is deliberately half of
    # elapsed, so a formula that divided by moving time would read
    # 20.0 — the denominator pin. A payload without a distance and an
    # effort with zero elapsed time both yield NULL, never zero.
    outdoor_ride(db, 261, day="2026-06-15", hr=140)
    insert_segment(db, 508)
    insert_segment_effort(
        db, 2611, 261, 508, elapsed=360, moving=180, distance=1609.344, start="2026-06-15T09:20:00Z"
    )
    insert_segment_effort(db, 2612, 261, 508, elapsed=300, start="2026-06-15T09:30:00Z")
    insert_segment_effort(
        db, 2613, 261, 508, elapsed=0, distance=500.0, start="2026-06-15T09:40:00Z"
    )
    db.commit()

    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    core = {
        row[0]: row[1:]
        for row in db.execute(
            "SELECT effort_id, effort_distance_m, speed_mph FROM analytics.fct_segment_efforts"
        ).fetchall()
    }
    assert core[2611] == (Decimal("1609.344"), Decimal("10.0"))
    assert core[2612] == (None, None)  # no distance in the payload
    assert core[2613] == (Decimal("500.0"), None)  # zero elapsed: guarded

    mart = {
        row[0]: row[1]
        for row in db.execute(
            "SELECT effort_id, speed_mph FROM analytics.mart_segment_trend"
        ).fetchall()
    }
    assert mart[2611] == Decimal("10.0")
    assert mart[2612] is None

    # Red-by-injection proofs for the two new pins (the C2/C3 pattern).
    db.execute(
        "INSERT INTO analytics.fct_segment_efforts"
        " (effort_id, activity_id, segment_id, elapsed_time_s, is_virtual_ride,"
        "  ride_is_valid, weather_available, effort_distance_m, speed_mph)"
        " VALUES (999999995, 999999994, 508, 360, false, true, false, 1609.344, 5.0)"
    )
    db.commit()
    selector = "assert_effort_speed_matches_formula"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "speed formula pin should fail on an injected row"
    assert selector in result.stdout
    db.execute("DELETE FROM analytics.fct_segment_efforts WHERE effort_id = 999999995")
    db.commit()
    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"speed formula pin still failing:\n{result.stdout}"

    db.execute(
        "INSERT INTO analytics.mart_segment_trend (effort_id, segment_id, speed_mph)"
        " VALUES (2611, 508, 99.9)"
    )
    db.commit()
    selector = "assert_segment_trend_speed_matches_core"
    result = run_dbt("test", "--select", selector)
    assert result.returncode != 0, "mart-core speed lockstep pin should fail on an injected row"
    assert selector in result.stdout
    db.execute("DELETE FROM analytics.mart_segment_trend WHERE speed_mph = 99.9")
    db.commit()
    result = run_dbt("test", "--select", selector)
    assert result.returncode == 0, f"mart-core speed lockstep pin still failing:\n{result.stdout}"


@pytest.mark.integration
def test_view_drops_take_the_cascade_lock(db):
    # dbt replaces a view by swapping in a new one and dropping the old
    # one with CASCADE. Two such drops at the same moment can deadlock:
    # since C3, int_segment_efforts reads stg_weather__hourly directly
    # and stg_strava__activities through two other views, so the two
    # cascades lock the same downstream views in different orders.
    # macros/drop_view.sql makes every view drop take one advisory lock
    # first, in the same statement, so the cascades run one at a time.
    # A dbt upgrade that stops dispatching to that macro would bring
    # the deadlock back with every model still green — so the lock is
    # pinned on the SQL dbt really sends (red before the macro existed).
    result = run_dbt("--debug", "run", "--select", "stg_weather__hourly")
    assert result.returncode == 0, f"dbt run failed:\n{result.stdout[-4000:]}"

    drops = re.findall(r"drop view if exists", result.stdout)
    locked = re.findall(r"pg_advisory_xact_lock\([^;]*\);\s*drop view if exists", result.stdout)
    assert drops, "no view drop found in the dbt debug output"
    assert len(locked) == len(drops), "a view drop ran without the cascade lock"
