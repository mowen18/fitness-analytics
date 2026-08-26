"""Streamlit app tests.

The marts-only rule (D19) is enforced mechanically at two levels: the
app source must never name a non-analytics schema, and the allow-list
must contain exactly the approved marts — core facts share the
analytics schema, so only a table-level pin can keep them out. The @integration
tests drive the real app headlessly (streamlit AppTest) against the
scratch database — once with empty marts (every view must explain
itself, not crash) and once with the drift fixtures (every view must
render its charts).
"""

import importlib.util
import os
from pathlib import Path

import pandas as pd
import pytest
import streamlit as st

from test_dbt_models import (
    db,  # noqa: F401 — shared truncating fixture
    drift_run,
    haversine_m,
    insert_segment,
    insert_segment_effort,
    insert_stream,
    insert_weather,
    outdoor_ride,
    run_dbt,
    steady_stream,
)

APP_PATH = Path(__file__).resolve().parent.parent / "app" / "streamlit_app.py"
TEST_DB = "running_analytics_test"
VIEW_NAMES = [
    "Aerobic efficiency",
    "Weekly training",
    "Cardiac drift",
    "Cycling training",
    "Cycling segments",
]

# The approved marts — D19 allows nothing else. Deliberately NOT the
# whole mart layer: mart_band_weekly stays out because the weekly band
# statistics travel inside mart_band_trend (v1.4), while
# mart_run_band_segments is the band chart's run-level scatter (v1.6).
# Every addition is proven red first — one name per revision through
# v1.6, the two cycling marts in v2.0 Phase C1, and mart_segment_trend
# in Phase C2, completing amended D19's list of exactly three.
MART_TABLES = frozenset(
    {
        "mart_weekly_training",
        "mart_efficiency_trend",
        "mart_efficiency_by_temp_band",
        "mart_run_quality",
        "mart_run_drift",
        "mart_drift_trend",
        "mart_band_trend",
        "mart_run_band_segments",
        "mart_weekly_cycling",
        "mart_ride_quality",
        "mart_segment_trend",
    }
)


def load_app_module():
    spec = importlib.util.spec_from_file_location("streamlit_app", APP_PATH)
    app = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(app)  # safe: the shell runs only under __main__
    return app


def test_app_reads_only_the_analytics_schema():
    source = APP_PATH.read_text()
    for forbidden in ("raw_strava", "raw_weather", "staging.", "intermediate."):
        assert forbidden not in source, f"app source references {forbidden}: marts only (D19)"
    assert 'SELECT * FROM analytics.{table}"' in source  # the single query site


def test_allow_list_is_pinned_to_the_approved_marts():
    app = load_app_module()
    assert set(app.ANALYTICS_TABLES) == MART_TABLES, "the allow-list is approved marts only (D19)"


def test_core_relations_are_refused_even_in_the_analytics_schema():
    """fct_runs lives in the same Postgres schema as the marts, so a
    schema-level check would let it through — the table-level guard in
    load() must refuse it before any connection is opened."""
    app = load_app_module()
    with pytest.raises(ValueError, match="not an approved analytics relation"):
        app.load("fct_runs")


def test_unlisted_marts_are_refused():
    """Allow-list growth is red-first and named per revision (v1.4:
    mart_band_trend; v1.6: mart_run_band_segments; v2.0 C1: the two
    cycling marts); mart_band_weekly is a real mart but the weekly
    statistics travel inside the trend mart, so the app must refuse it
    like anything else off the list."""
    app = load_app_module()
    with pytest.raises(ValueError, match="not an approved analytics relation"):
        app.load("mart_band_weekly")


def test_decimals_are_coerced_to_float_for_the_browser():
    """Postgres numerics arrive as Decimal; Arrow ships Decimal as
    decimal128, which Vega-Lite reads UNSCALED (0.7838 charted as 7838).
    to_dataframe must coerce to float64 — the regression test for the
    first-real-data chart bug."""
    from decimal import Decimal

    app = load_app_module()

    df = app.to_dataframe(
        [
            (1, Decimal("0.7838"), None, None, "mild"),
            (2, Decimal("14.5"), Decimal("70.0"), None, None),
        ],
        ["week", "efficiency", "temperature_f", "humidity", "band"],
    )

    assert df["efficiency"].dtype == "float64"
    assert df["temperature_f"].dtype == "float64"
    assert df["efficiency"].tolist() == [0.7838, 14.5]
    assert pd.isna(df["temperature_f"].iloc[0])  # None -> NaN, shown blank
    # All-NULL columns (no weather anywhere) must not stay object dtype,
    # or tables render the literal string "None".
    assert df["humidity"].dtype == "float64"
    assert df["humidity"].isna().all()
    # Text columns go nullable-string so missing text renders blank too.
    assert df["band"].dtype == "string"
    assert pd.isna(df["band"].iloc[1])
    assert df["week"].tolist() == [1, 2]  # non-Decimal columns untouched


def test_tooltip_1dp_formats_values_and_marks_missing():
    app = load_app_module()
    values = pd.Series([None, 84.24], dtype="float64")

    assert app.format_tooltip_1dp(values).tolist() == ["—", "84.2"]


def render(view: str):
    """Run the app headlessly on the scratch DB, switched to `view`."""
    from streamlit.testing.v1 import AppTest

    st.cache_data.clear()  # never let one test's frames leak into the next
    os.environ["POSTGRES_DB"] = TEST_DB  # Settings: env beats .env
    try:
        at = AppTest.from_file(str(APP_PATH), default_timeout=30).run()
        at.sidebar.radio[0].set_value(view)
        return at.run()
    finally:
        del os.environ["POSTGRES_DB"]


@pytest.mark.integration
def test_every_view_explains_empty_marts_without_crashing(db):  # noqa: F811
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    for view in VIEW_NAMES:
        at = render(view)
        assert not at.exception, f"{view} raised with empty marts: {at.exception}"
        # Missing data is explained, never a blank page (dashboard rule).
        assert at.info, f"{view} shows no empty-state explanation"


@pytest.mark.integration
def test_every_view_renders_with_populated_marts(db):  # noqa: F811
    drift_run(db, 1, day="2026-06-15")
    insert_stream(db, 1, samples=steady_stream())
    drift_run(db, 2, day="2026-06-17")
    insert_stream(db, 2, samples=steady_stream(second_half_hr=145.0))
    outdoor_ride(db, 50, day="2026-06-16", hr=140, cadence=88.0)
    db.commit()
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    for view in VIEW_NAMES:
        at = render(view)
        assert not at.exception, f"{view} raised with populated marts: {at.exception}"

    weekly = render("Weekly training")
    assert weekly.metric[0].value == "13.4"  # 2 × 10.8 km in miles
    cycling = render("Cycling training")
    assert cycling.metric[0].value == "19.9"  # the 32 km fixture ride in miles
    drift = render("Cardiac drift")
    # Sign convention must be stated on the view itself (D17).
    assert any("positive = " in c.value for c in drift.caption)
    # The eligibility and band-trend tables render alongside the weekly
    # trend table (v1.4: the band section lives INSIDE this view — the
    # D22 nesting choice, made under the original three-view cap;
    # v2.0's amended cap is five, with cycling as its own view).
    efficiency = render("Aerobic efficiency")
    assert len(efficiency.dataframe) >= 3
    # The D22 sign convention must be stated on the view itself too.
    assert any("falling min/mi" in c.value for c in efficiency.caption)


@pytest.mark.integration
def test_segment_view_gates_picker_and_captions_exclusions(db):  # noqa: F811
    # C2 acceptance criteria 3 and 4 at the view layer: the picker
    # offers only >= 5-effort segments, the short-segment caveat
    # renders, and the excluded-VirtualRide count appears in the
    # sample caption (readable from mart_segment_trend alone — D19).
    outdoor_ride(db, 61, day="2026-06-15", hr=140)
    outdoor_ride(db, 62, day="2026-06-16", sport_type="VirtualRide")
    insert_segment(db, 601, name="Sufficient Sprint")
    insert_segment(db, 602, name="Sparse Hill")
    for n, elapsed in enumerate([100, 101, 99, 100, 102]):  # median 100 s: short
        insert_segment_effort(
            db, 6010 + n, 61, 601, elapsed=elapsed, start=f"2026-06-15T09:{20 + n:02d}:00Z"
        )
    insert_segment_effort(db, 6021, 61, 602, elapsed=300, start="2026-06-15T10:30:00Z")
    insert_segment_effort(db, 6025, 62, 601, elapsed=95, start="2026-06-16T09:20:00Z")
    db.commit()
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    at = render("Cycling segments")
    assert not at.exception, f"Cycling segments raised: {at.exception}"
    options = at.selectbox[0].options
    assert "Sufficient Sprint" in options
    assert all("Sparse Hill" not in option for option in options)  # 1 effort: not offered
    captions = " ".join(c.value for c in at.caption)
    assert "1 VirtualRide effort(s)" in captions  # criterion 4: counted, not silent
    assert "Short segment" in captions  # criterion 3: the caveat renders
    assert "Lower = faster" in captions  # the sign convention is stated on the view


def _five_efforts(conn, activity_id, segment_id, first_effort_id):
    for n, elapsed in enumerate([200, 210, 190, 205, 195]):
        insert_segment_effort(
            conn,
            first_effort_id + n,
            activity_id,
            segment_id,
            elapsed=elapsed,
            start=f"2026-06-15T09:{20 + n:02d}:00Z",
        )


NORTH_ENDS = {"start_latlng": [12.34, -56.78], "end_latlng": [12.35, -56.78]}


@pytest.mark.integration
def test_segment_view_headwind_context_and_sign_caption(db):  # noqa: F811
    # C3 (D30) at the view layer: headwind context renders with its
    # sign convention stated on the view (the D17 idiom) plus the
    # spatial caveat, headwind and crosswind reach the effort table,
    # and a straight segment shows NO winding caveat.
    outdoor_ride(db, 71, day="2026-06-15", hr=140)
    insert_segment(db, 701, name="Windy Straight", **NORTH_ENDS)
    _five_efforts(db, 71, 701, 7010)
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

    at = render("Cycling segments")
    assert not at.exception, f"Cycling segments raised: {at.exception}"
    captions = " ".join(c.value for c in at.caption)
    assert "positive = headwind" in captions  # the D30 sign, on the view
    assert "ride's start cell" in captions  # the spatial caveat, displayed
    assert "Winding segment" not in captions  # straight course: no caveat
    table = at.dataframe[0].value
    assert "headwind_mph" in table.columns
    assert "crosswind_mph" in table.columns


@pytest.mark.integration
def test_segment_view_winding_caption_renders(db):  # noqa: F811
    # C3 (D30): sinuosity above the var flips the displayed winding
    # caveat — flagged, never a filter, so everything else still shows.
    outdoor_ride(db, 72, day="2026-06-15", hr=140)
    winding_distance = round(1.31 * haversine_m(12.34, -56.78, 12.35, -56.78), 3)
    insert_segment(db, 702, name="Switchback Hill", distance=winding_distance, **NORTH_ENDS)
    _five_efforts(db, 72, 702, 7020)
    insert_weather(
        db,
        hour="2026-06-15T09:00:00+00:00",
        temperature=20.0,
        wind_kph=16.09344,
        wind_direction=180.0,
    )
    db.commit()
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    at = render("Cycling segments")
    assert not at.exception, f"Cycling segments raised: {at.exception}"
    captions = " ".join(c.value for c in at.caption)
    assert "Winding segment" in captions


@pytest.mark.integration
def test_segment_view_degrades_without_wind_direction(db):  # noqa: F811
    # C3 acceptance criterion 5: with no wind direction anywhere (the
    # exact C2 data shape — pre-backfill), the view renders the
    # existing trend and explains the missing headwind context; never
    # a crash, never a silent absence.
    outdoor_ride(db, 73, day="2026-06-15", hr=140)
    insert_segment(db, 703, name="Directionless")
    _five_efforts(db, 73, 703, 7030)
    insert_weather(db, hour="2026-06-15T09:00:00+00:00", temperature=20.0)
    db.commit()
    result = run_dbt("build")
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}"

    at = render("Cycling segments")
    assert not at.exception, f"Cycling segments raised: {at.exception}"
    assert at.selectbox[0].options  # the C2 trend still renders
    captions = " ".join(c.value for c in at.caption)
    assert "no wind direction" in captions.lower()
    assert "reconcile-weather" in captions  # the fix is named, not implied
