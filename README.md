# Running and Cycling Analytics Pipeline

Incremental endurance-analytics pipeline evaluating whether aerobic
fitness is improving over time, using Strava activities and historical
hourly weather. It began as running-only; Revision v2.0 admitted
cycling as a peer domain with its own question, models, and marts.
Pipeline-first: the deliverables are ingestion, warehouse models,
metrics, tests, and docs — the dashboard is a thin cap.

**Questions this answers** (that standard Strava/Apple Fitness views can't):

* Is pace at a comparable heart rate improving over time?
* How does running efficiency vary under different weather conditions?
* Is cardiac drift decreasing during longer runs?
* How is weekly volume changing alongside these efficiency measures?
* On fixed segments, is ride effort time at comparable heart rate
  improving? (cycling)

**Full spec:** [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) (decisions
D1–D30 are locked, revisable only by recorded addendum; the plan's
Revisions section and [docs/decisions/](docs/decisions/) record every
change, v1.1 through v2.0).
**Status:** running domain complete — Phases 0–6 plus revisions through
v1.9 implemented and verified. Cycling domain (v2.0): Phases C1 and C2
merged — rides core models, segment-effort ingestion (D24), the
D28 segment trend mart, and the Cycling training + Cycling segments
views, with running output proven byte-identical at each phase.
Phase C3 (wind direction and headwind context, D29/D30 — Release 2.1)
is implemented and live-verified: the one-time direction backfill
drained in a single `make reconcile-weather` pass, every cached hour
now carries a direction or an explicit NULL, and running output stayed
byte-identical through both verification regimes.

## Architecture

```text
Strava API ──────┐
                 ├──> Python ingestion (src/running_pipeline)
Open-Meteo API ──┘             │
                               v
                    PostgreSQL container
                    running_analytics_db (port 5433)
                               │
                               v
                    dbt transformations (dbt/)
                    staging → intermediate → core → marts
                               │
                               v
                    Streamlit app (app/)
                    (reads marts only, D19)
```

## Prerequisites

- Docker Desktop
- Python 3.13 (`brew install python@3.13`)
- A [Strava API application](https://www.strava.com/settings/api) (free)

## Setup

```bash
# 1. Virtual environment + dependencies
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configuration — fill in your Strava app credentials and a DB password
cp .env.example .env

# 3. Database (Postgres 17 on host port 5433; schemas created on first boot)
docker compose up -d

# 4. One-time Strava authorization (browser flow, ~30 seconds)
running-pipeline authorize

# 5. Verify authenticated access
running-pipeline athlete
```

## Environment variables

All configuration lives in `.env` (gitignored); the full annotated contract is
[`.env.example`](.env.example). Highlights:

| Variable | Purpose |
|---|---|
| `STRAVA_CLIENT_ID` / `STRAVA_CLIENT_SECRET` | From your Strava API app settings |
| `STRAVA_REFRESH_TOKEN` | Optional bootstrap only — after the first refresh, the rotated token in `.secrets/strava_tokens.json` is authoritative |
| `POSTGRES_*` | Database connection (decision D2: `running_analytics_db` / `running_user` / port 5433) |
| `SYNC_START_DATE`, `SYNC_OVERLAP_DAYS` | Ingestion window (decisions D5, D6 — used from Phase 1) |
| `WEATHER_REQUEST_BUDGET`, `WEATHER_BATCH_GAP_DAYS` | Optional weather-sync bounds (Phase 2). Open-Meteo needs **no API key** — there are no weather credentials |

## Token handling

Strava rotates the refresh token on every refresh. The pipeline persists the
newest access/refresh/expiry trio to `.secrets/strava_tokens.json`
(gitignored, `0600` permissions, atomic writes) immediately after every
refresh. Tokens are never logged. If the file is lost, re-run
`running-pipeline authorize`.

## Commands

```bash
make up               # start Postgres
make down             # stop Postgres
make bootstrap        # (re-)apply every sql/*.sql — idempotent
make athlete          # print the authenticated athlete profile
make sync-activities  # incremental Strava activity sync (14-day overlap)
make reconcile        # full reconciliation from SYNC_START_DATE
make backfill-coordinates # resolve activity-start coordinates (payload, else polyline)
make sync-weather     # fetch hourly weather for outdoor runs and rides not yet covered
make reconcile-weather # re-fetch weather even for already-cached hours
make sync-streams     # backfill activity streams for fetch-eligible runs
make sync-segment-efforts # backfill segment efforts for D23 rides (detail fetches)
make app              # launch the Streamlit dashboard (five views)
make all              # full refresh: every sync, then dbt build
make dbt-build        # build all dbt models and run their tests
make dbt-test         # dbt tests only
make dbt-freshness    # source freshness (raw fetched_at ages)
make dbt-docs         # generate + serve dbt documentation locally
make dbt-dag          # regenerate the dbt DAG diagram embedded in this README
make test             # pytest (all external HTTP mocked; DB-integration
                      # tests skip visibly when Postgres is down)
make lint             # ruff check
make format           # ruff format
```

## Orchestration

The Makefile and CLI above are the pipeline's only execution layer.
Apache Airflow (v1.5, revising D18) sits on top as a thin scheduling
and observability layer: it owns no state and runs the same commands
an operator would type. It lives entirely outside the project —
`make airflow-install` builds its own venv at `~/.venvs/airflow`, and
`make airflow-start` runs `airflow standalone`
(`AIRFLOW_HOME=~/airflow`). The `running_pipeline` DAG mirrors
`make all` daily at 06:00 America/Chicago; a clean rate-limit stop
(CLI exit 3) marks the task SKIPPED and the chain still builds marts
from already-ingested data, while genuine failure halts it. Details,
constraints, and operational notes: `orchestration/README.md` and
`docs/decisions/v1.5-airflow-addendum.md`.

## Warehouse layout

Five schemas (decision D3): `raw_strava`, `raw_weather`, `staging`,
`intermediate`, `analytics`. Schemas and tables are created idempotently by
the [`sql/`](sql/) scripts on first container init (or `make bootstrap`).
Phase 1 owns `raw_strava.activities` — one row per activity, full API
payload in JSONB with sync-critical fields promoted to typed columns
(`activity_id` PK, `start_date_utc`, `activity_type`) — and
`raw_strava.sync_state`, which holds per-job sync watermarks. Phase 2 owns
`raw_weather.hourly` — one row per normalized location and UTC hour, typed
measurement columns plus the original per-hour payload in JSONB, unique on
`(location_key, weather_timestamp)`.

## Incremental sync strategy

`make sync-activities` loads every activity started on or after
`SYNC_START_DATE` (D5: 2024-01-01). Re-runs are idempotent: rows upsert by
`activity_id`, and rows whose payload is unchanged are skipped without
being rewritten, so inserted / updated / skipped counts are measured, not
inferred.

Each successful run records its own UTC start time as a watermark in
`raw_strava.sync_state`. The next incremental run re-fetches from
`watermark − SYNC_OVERLAP_DAYS` (D6: 14 days), so late uploads and recent
edits inside that window are captured automatically. One documented
limitation: the Strava list API filters on activity *start date*, so an
activity uploaded or edited more than 14 days after it occurred is only
caught by `make reconcile`, which re-fetches the whole historical window
on demand.

Transient API failures retry with bounded backoff (1s/2s/4s). When usage
approaches Strava's reported rate limits (≥90% of the 15-minute or daily
read limit), the sync stops cleanly: completed pages stay committed, the
watermark is not advanced, and the command exits with code 3 so the
interruption is visible — the next run simply re-covers the window. Failed
or interrupted runs never advance the watermark. Counts are logged on
every run; token values never are.

## Weather ingestion

`make sync-weather` attaches hourly weather (Open-Meteo historical archive,
decision D8) to each **outdoor run**: sport type `Run`/`TrailRun`, start
coordinates present, and Strava's `trainer` flag not set. Indoor and
virtual runs are excluded by design — they have no location, and outdoor
weather would be wrong for them — and are reported explicitly as
`runs_without_location`. Open-Meteo requires **no API key**; a per-sync
request budget (`WEATHER_REQUEST_BUDGET`) and 429 handling keep usage far
below its ~10k requests/day free tier, with the same stop-cleanly / exit
code 3 contract as the activity sync.

**Timezone handling:** everything is UTC end to end. Archive requests pass
`timezone=UTC`, returned hourly timestamps are stored as `timestamptz`,
and a run is matched to the observation at its start hour —
`date_trunc('hour', start_date_utc)` — never to a daily aggregate.

**The table is the cache.** There is no separate cache layer and no
watermark: each sync derives the location-hours eligible runs need,
subtracts what `raw_weather.hourly` already holds (unique on
`(location_key, weather_timestamp)`; coordinates are rounded to 2 decimal
places, a ~1.1 km cell, per decision D7), and batches the remainder into
one archive request per location and contiguous date range. Re-runs are
idempotent and repeated runs in the same cell hit the cache with zero
requests.

**Wind direction (D29, C3).** The hourly variable set is temperature,
apparent temperature, relative humidity, wind speed, and — since C3 —
`wind_direction_10m`, stored as `wind_direction_deg` (meteorological
FROM convention: **0° legitimately means north — a value; missing is
NULL, never zero**). A cached hour counts as complete only when its
direction is *resolved*: either a stored value, or a payload carrying
the `wind_direction_10m` key as an explicit null (the client records
every requested variable). Hours fetched before C3 lack the key, so
one `make reconcile-weather` pass re-fetches them — resumable across
runs within `WEATHER_REQUEST_BUDGET` by cache design — after which the
queue is empty and stays empty.

**Map-privacy fallback.** Strava's "hide entire map" setting strips
`start_latlng` from API payloads — even the owner's — so
`make backfill-coordinates` resolves each run's start coordinate with
explicit provenance in `raw_strava.activity_coordinates`: the payload's
`start_latlng` when present (free), else the first decoded point of the
detail endpoint's route polyline (one API call per run, resumable with
the same rate-limit contract as the other backfills), else an explicit
`unavailable` row. Weather eligibility and dbt staging prefer the
resolved coordinate over the payload.

## Warehouse models (dbt)

<!-- dbt-dag:start -->
```mermaid
flowchart LR

    subgraph sources["Sources"]
        source_running_analytics_raw_strava_activities[("raw_strava.activities")]
        source_running_analytics_raw_strava_activity_coordinates[("raw_strava.activity_coordinates")]
        source_running_analytics_raw_strava_activity_details[("raw_strava.activity_details")]
        source_running_analytics_raw_strava_segment_efforts[("raw_strava.segment_efforts")]
        source_running_analytics_raw_strava_segments[("raw_strava.segments")]
        source_running_analytics_raw_strava_streams[("raw_strava.streams")]
        source_running_analytics_raw_strava_sync_state[("raw_strava.sync_state")]
        source_running_analytics_raw_weather_hourly[("raw_weather.hourly")]
    end

    subgraph seeds["Seeds"]
        seed_running_analytics_hr_bands(["hr_bands"])
        seed_running_analytics_temperature_bands(["temperature_bands"])
    end

    subgraph staging["Staging"]
        model_running_analytics_stg_strava__activities["stg_strava__activities"]
        model_running_analytics_stg_strava__segment_efforts["stg_strava__segment_efforts"]
        model_running_analytics_stg_strava__segments["stg_strava__segments"]
        model_running_analytics_stg_weather__hourly["stg_weather__hourly"]
    end

    subgraph intermediate["Intermediate"]
        model_running_analytics_int_band_window_samples["int_band_window_samples"]
        model_running_analytics_int_ride_measures["int_ride_measures"]
        model_running_analytics_int_rides_with_weather["int_rides_with_weather"]
        model_running_analytics_int_run_band_assessment["int_run_band_assessment"]
        model_running_analytics_int_run_efficiency["int_run_efficiency"]
        model_running_analytics_int_run_stream_samples["int_run_stream_samples"]
        model_running_analytics_int_run_stream_state["int_run_stream_state"]
        model_running_analytics_int_runs_with_weather["int_runs_with_weather"]
        model_running_analytics_int_segment_efforts["int_segment_efforts"]
        model_running_analytics_int_segment_geometry["int_segment_geometry"]
    end

    subgraph core["Core"]
        model_running_analytics_fct_band_candidates["fct_band_candidates"]
        model_running_analytics_fct_drift_candidates["fct_drift_candidates"]
        model_running_analytics_fct_rides["fct_rides"]
        model_running_analytics_fct_run_band_segments["fct_run_band_segments"]
        model_running_analytics_fct_runs["fct_runs"]
        model_running_analytics_fct_segment_efforts["fct_segment_efforts"]
    end

    subgraph marts["Marts"]
        model_running_analytics_mart_band_trend["mart_band_trend"]
        model_running_analytics_mart_band_weekly["mart_band_weekly"]
        model_running_analytics_mart_drift_trend["mart_drift_trend"]
        model_running_analytics_mart_efficiency_by_temp_band["mart_efficiency_by_temp_band"]
        model_running_analytics_mart_efficiency_trend["mart_efficiency_trend"]
        model_running_analytics_mart_ride_quality["mart_ride_quality"]
        model_running_analytics_mart_run_band_segments["mart_run_band_segments"]
        model_running_analytics_mart_run_drift["mart_run_drift"]
        model_running_analytics_mart_run_quality["mart_run_quality"]
        model_running_analytics_mart_segment_trend["mart_segment_trend"]
        model_running_analytics_mart_weekly_cycling["mart_weekly_cycling"]
        model_running_analytics_mart_weekly_training["mart_weekly_training"]
    end

    model_running_analytics_fct_band_candidates --> model_running_analytics_mart_run_quality
    model_running_analytics_fct_drift_candidates --> model_running_analytics_mart_run_drift
    model_running_analytics_fct_drift_candidates --> model_running_analytics_mart_run_quality
    model_running_analytics_fct_rides --> model_running_analytics_mart_ride_quality
    model_running_analytics_fct_rides --> model_running_analytics_mart_weekly_cycling
    model_running_analytics_fct_run_band_segments --> model_running_analytics_mart_band_trend
    model_running_analytics_fct_run_band_segments --> model_running_analytics_mart_band_weekly
    model_running_analytics_fct_run_band_segments --> model_running_analytics_mart_run_band_segments
    model_running_analytics_fct_runs --> model_running_analytics_mart_band_weekly
    model_running_analytics_fct_runs --> model_running_analytics_mart_efficiency_by_temp_band
    model_running_analytics_fct_runs --> model_running_analytics_mart_efficiency_trend
    model_running_analytics_fct_runs --> model_running_analytics_mart_run_drift
    model_running_analytics_fct_runs --> model_running_analytics_mart_run_quality
    model_running_analytics_fct_runs --> model_running_analytics_mart_weekly_training
    model_running_analytics_fct_segment_efforts --> model_running_analytics_mart_segment_trend
    model_running_analytics_int_band_window_samples --> model_running_analytics_fct_run_band_segments
    model_running_analytics_int_band_window_samples --> model_running_analytics_int_run_band_assessment
    model_running_analytics_int_ride_measures --> model_running_analytics_fct_rides
    model_running_analytics_int_ride_measures --> model_running_analytics_int_segment_efforts
    model_running_analytics_int_rides_with_weather --> model_running_analytics_int_ride_measures
    model_running_analytics_int_run_band_assessment --> model_running_analytics_fct_band_candidates
    model_running_analytics_int_run_band_assessment --> model_running_analytics_fct_run_band_segments
    model_running_analytics_int_run_efficiency --> model_running_analytics_fct_drift_candidates
    model_running_analytics_int_run_efficiency --> model_running_analytics_fct_runs
    model_running_analytics_int_run_efficiency --> model_running_analytics_int_run_band_assessment
    model_running_analytics_int_run_stream_samples --> model_running_analytics_fct_drift_candidates
    model_running_analytics_int_run_stream_samples --> model_running_analytics_int_band_window_samples
    model_running_analytics_int_run_stream_samples --> model_running_analytics_int_run_band_assessment
    model_running_analytics_int_run_stream_state --> model_running_analytics_fct_drift_candidates
    model_running_analytics_int_run_stream_state --> model_running_analytics_int_run_band_assessment
    model_running_analytics_int_runs_with_weather --> model_running_analytics_int_run_efficiency
    model_running_analytics_int_segment_efforts --> model_running_analytics_fct_segment_efforts
    model_running_analytics_int_segment_geometry --> model_running_analytics_int_segment_efforts
    model_running_analytics_mart_band_weekly --> model_running_analytics_mart_band_trend
    model_running_analytics_mart_run_drift --> model_running_analytics_mart_drift_trend
    model_running_analytics_mart_weekly_training --> model_running_analytics_mart_efficiency_trend
    model_running_analytics_stg_strava__activities --> model_running_analytics_int_rides_with_weather
    model_running_analytics_stg_strava__activities --> model_running_analytics_int_runs_with_weather
    model_running_analytics_stg_strava__segment_efforts --> model_running_analytics_int_segment_efforts
    model_running_analytics_stg_strava__segments --> model_running_analytics_int_segment_efforts
    model_running_analytics_stg_strava__segments --> model_running_analytics_int_segment_geometry
    model_running_analytics_stg_weather__hourly --> model_running_analytics_int_rides_with_weather
    model_running_analytics_stg_weather__hourly --> model_running_analytics_int_runs_with_weather
    model_running_analytics_stg_weather__hourly --> model_running_analytics_int_segment_efforts
    seed_running_analytics_hr_bands --> model_running_analytics_int_band_window_samples
    seed_running_analytics_hr_bands --> model_running_analytics_mart_band_weekly
    seed_running_analytics_hr_bands --> model_running_analytics_mart_run_band_segments
    seed_running_analytics_temperature_bands --> model_running_analytics_mart_efficiency_by_temp_band
    seed_running_analytics_temperature_bands --> model_running_analytics_mart_ride_quality
    seed_running_analytics_temperature_bands --> model_running_analytics_mart_run_quality
    source_running_analytics_raw_strava_activities --> model_running_analytics_stg_strava__activities
    source_running_analytics_raw_strava_activity_coordinates --> model_running_analytics_stg_strava__activities
    source_running_analytics_raw_strava_segment_efforts --> model_running_analytics_stg_strava__segment_efforts
    source_running_analytics_raw_strava_segments --> model_running_analytics_stg_strava__segments
    source_running_analytics_raw_strava_streams --> model_running_analytics_int_run_stream_samples
    source_running_analytics_raw_strava_streams --> model_running_analytics_int_run_stream_state
    source_running_analytics_raw_weather_hourly --> model_running_analytics_stg_weather__hourly
```
<!-- dbt-dag:end -->

The dbt project lives in `dbt/` (decision D4) and is driven entirely
through the Make targets above; `dbt/profiles.yml` is auto-copied from
the committed example on first run and reads connection values from the
same `.env` contract as the Python pipeline — no separate credentials.
One node renders without edges by design: `raw_strava.sync_state` holds
operational sync watermarks, declared in `sources.yml` for source
inventory but deliberately not modeled downstream.

| Layer | Model | Schema | Grain |
|---|---|---|---|
| Staging | `stg_strava__activities` | `staging` | one row per activity, any sport type |
| Staging | `stg_weather__hourly` | `staging` | one row per D7 cell + UTC hour, metric & imperial units |
| Intermediate | `int_band_window_samples` | `intermediate` | one row per activity + pooled band-window sample: HR band + capped dwell contribution (D22) |
| Intermediate | `int_run_band_assessment` | `intermediate` | one row per band candidate — window stats + the D22 exclusion ladder, encoded once |
| Intermediate | `int_run_efficiency` | `intermediate` | one row per running activity — derived measures, weather context, efficiency and validity verdict; the sole parent of `fct_runs`, and also feeds `fct_drift_candidates` |
| Intermediate | `int_run_stream_samples` | `intermediate` | one row per activity + aligned stream sample |
| Intermediate | `int_run_stream_state` | `intermediate` | one row per stream-fetch attempt: status + required-array presence |
| Intermediate | `int_runs_with_weather` | `intermediate` | one row per running activity + nearest qualifying observation |
| Core | `fct_band_candidates` | `analytics` | one row per band candidate — projection of the assessment: window stats + exclusion verdict (D22) |
| Core | `fct_drift_candidates` | `analytics` | one row per drift candidate + halves and exclusion verdict |
| Core | `fct_run_band_segments` | `analytics` | one row per analyzed run × HR band with ≥ 5 min dwell: dwell, sample count, median velocity/pace |
| Core | `fct_runs` | `analytics` | one row per running activity — the mart-facing contract: measures, weather, validity + efficiency |
| Mart | `mart_band_trend` | `analytics` | one row per week × HR band + 28-day rolling median pace (the mart the dashboard reads) |
| Mart | `mart_band_weekly` | `analytics` | one row per week × HR band — median of per-run band medians (not app-facing; travels inside the trend mart) |
| Mart | `mart_drift_trend` | `analytics` | one row per week of drift runs + rolling median |
| Mart | `mart_efficiency_by_temp_band` | `analytics` | one row per D14 temp band (+ explicit weather-unavailable row) |
| Mart | `mart_efficiency_trend` | `analytics` | one row per week + 28-day rolling median |
| Mart | `mart_run_band_segments` | `analytics` | one row per analyzed run × HR band — the run's band median pace and dwell, the band chart's scatter (v1.6) |
| Mart | `mart_run_drift` | `analytics` | one row per analyzed drift run |
| Mart | `mart_run_quality` | `analytics` | one row per running activity + quality verdicts (validity, band, drift, HR band) |
| Mart | `mart_weekly_training` | `analytics` | one row per training week |
| Seed | `hr_bands` | `analytics` | the D22 10-bpm HR bands (open-ended edges), defined once, joined by range everywhere |
| Seed | `temperature_bands` | `analytics` | the D14 bands, defined once, joined by range everywhere |

Conventions worth knowing: the running-activity filter
(Run/TrailRun/VirtualRun) is applied after staging, never in it; weather
matches the *nearest* observation that actually carries measurements
(explicit "archive had no data" rows never match) and only counts as
matched within 60 minutes of the run's start; training weeks are local
wall-clock (`week_start_date` = Monday of the local week); metric
thresholds (the D9 run-validity rules as revised by v1.1, the 45-minute
long-run definition, the D22 band window and dwell rules) are dbt vars,
never inline SQL. Layer dependencies are directional and
machine-checked: marts read core, seeds, and other marts (today a
single mart-to-mart hop feeds each trend); intermediate never reads
core, and since v1.4 may read seeds — one deliberate matrix line, added
for the `hr_bands` sample-grain join and proven against the guard
before it changed. `source()` is a staging-only privilege with one
documented exception — `raw_strava.streams`, readable from intermediate
models only, because stream payloads' only useful transformation is a
grain change. All of it is enforced by a manifest-based test
(`tests/test_dbt_layering.py`), not convention. Every measurement column
carries an explicit unit suffix, and missing HR/weather stays NULL
through every layer.

**Missing weather is explicit, never zero.** Hours the archive genuinely
has no data for are stored as rows with NULL measurements and the original
payload preserved; later incremental syncs re-request those hours until
data appears. That is the rare case, not the normal one: requests pass no
`models` parameter, so the archive defaults to `best_match`, which serves
recent dates immediately with preliminary ECMWF IFS fill-in values that
the same request can later silently replace once ERA5 covers the date. A
cache-completeness check must know whether the source's answer was final;
ours treats any non-NULL value as final, so preliminary values are frozen
until a full re-fetch (`make reconcile-weather`), whose `IS DISTINCT FROM`
upsert absorbs any revisions. A failed
request for one location never fails the sync — it is logged, counted in
`failed_batches`, and retried next run. Exact coordinates are never logged;
only 2-decimal cell keys appear in logs and stored keys.

## Metric definitions

**Aerobic efficiency (D10)** — the project's primary metric:

```text
aerobic_efficiency_m_per_heartbeat = speed_m_per_min / average_hr_bpm
```

Approximate meters traveled per heartbeat. Higher = faster at the same
heart rate, or a lower heart rate at the same speed. **This is an
observational signal, not proof of physiological improvement.** The
approved framing is *"pace-at-heart-rate efficiency has increased across
runs with valid heart-rate data"* — never *"the metric proves aerobic
fitness improved."* Weather, terrain, sleep, and measurement noise all
move it, and **intensity mix is not controlled for**: the metric already
normalizes by HR, so hard efforts and races feed the same aggregates,
with average HR displayed alongside for context.

**Run validity (D9, revised by v1.1)** — a run feeds every efficiency
aggregate when all of the following hold (each threshold is a dbt var,
editable in `dbt/dbt_project.yml` without touching model SQL). There is
no intensity ceiling and no race/workout exclusion:

| Rule | Default |
|---|---|
| Heart-rate data present | required |
| Average HR within instrument-sanity band | 90–200 bpm |
| Pace within sanity bounds | 4:00–20:00 min/mi |
| Moving time | ≥ 15 min |

Invalid runs are never silently dropped: `int_run_efficiency` gives
every excluded run a human-readable `exclusion_reason` (the first failing
rule in a documented priority order), carried through `fct_runs` and
`mart_run_quality`.

**Weekly statistics (D11, D12)** — the weekly summary statistic is the
**median** efficiency across valid runs (mean shown as secondary);
a week is trend-worthy only with ≥ 2 valid runs (`is_sufficient`).
**Trend (D13)** — a 28-day rolling median over run-level efficiency
smooths single-week noise. **Temperature bands (D14, revised v1.7/v1.8)** — < 50 °F,
50–70 °F, 70–80 °F, 80–90 °F, > 90 °F, defined once in the
`temperature_bands` seed and joined by range against each run's
**apparent (feels-like) temperature**, which folds humidity and wind
into the banding. The 80 and 90 cuts sit on NOAA heat-index anchors
that coincide with empirical gaps in the observed distribution (the
v1.8 stopping rule: a boundary requires both). Valid runs without a
matched feels-like value appear in an explicit *weather unavailable*
row rather than vanishing from the comparison.

**Pace at heart-rate band (D22, Revision v1.4)** — the sample-grain
counterpart to efficiency: pool each run's valid, moving stream samples
after trimming the first 5 minutes (warm-up HR is still climbing —
untrimmed samples pair a too-low HR with full pace and flatter the low
bands) and the final 2 minutes; assign each sample a 10-bpm HR band
(`hr_bands` seed, joined by range like D14); a run contributes to a
band only with ≥ 5 minutes of dwell there, so transitions passing
through a band never deposit junk medians. Per run per band the metric
is the **median velocity** (pace derived from it for display); the
weekly statistic is the **median across contributing runs of those
run-level medians** (D11's median-of-runs philosophy), with the D12
2-run sufficiency flag at week × band grain and a 28-day rolling median
per band (D13). **Sign of interest: rising pace — falling min/mi — at
the same HR band is the observational signal of an improving aerobic
base**, with the same never-causal framing as efficiency and drift.
Dwell is capped per sample at the drift coverage gap (3 s), and runs
failing any check carry a deterministic `band_exclusion_reason` through
`fct_band_candidates` into `mart_run_quality`.

### Segment effort trend (cycling, D28)

The cycling primary question is answered on **fixed Strava segments**:
the course is held constant, so effort elapsed time at comparable
heart rate is the controlled analog of the running efficiency metric.
`mart_segment_trend` has one row per non-virtual effort and carries a
**rolling median over the last 5 efforts** (`segment_rolling_effort_window`)
plus the cumulative best. Trend display requires **≥ 5 efforts** on the
segment (`segment_trend_min_efforts`, the D12 spirit); segments whose
median effort is **under 120 s** (`short_segment_seconds`) carry a
displayed `short_segment` noise flag — ±1 s GPS sampling is material
at that length — but are never excluded. Efforts from `VirtualRide`
activities are flagged in core and held out of segment marts (D28),
reported per segment as `virtual_effort_count`; a parent ride failing
D25's sanity checks marks its efforts with the ride's exclusion reason
as a displayed caveat, never a filter. No power fields are modeled
anywhere in the chain (D27): estimated watts stay raw-JSONB-only.

### Headwind context (cycling, D29/D30)

On a fixed segment, wind is the largest remaining condition variable —
the headwind component is what explains slow days. Each segment gets a
**great-circle initial bearing** from its start to end coordinates and
a **sinuosity** (segment distance / haversine straight line,
`int_segment_geometry`); segments strictly above
`winding_sinuosity_max` (1.3) carry a displayed `winding_segment` flag
— the straight-line bearing misdescribes their course — but are never
excluded. Each effort is matched to the **nearest cached hourly
observation at the parent ride's start cell** (the documented spatial
caveat: wind is not observed at the segment's own location) at the
**effort's** start time, within 60 minutes. Then:

```text
headwind_mph  = effort_wind_speed_mph × cos(radians(direction − bearing))
crosswind_mph = effort_wind_speed_mph × |sin(radians(direction − bearing))|
```

**Sign convention (D30): positive = headwind** — wind opposing travel
along the segment's bearing; negative = tailwind; crosswind is the
unsigned perpendicular component. Headwind is **NULL whenever wind
direction, the 60-minute match, or the segment bearing is missing —
never zero** — and computed-and-flagged, not nulled, on winding
segments. The Cycling segments view colors effort points on a
diverging blue↔red scale (red = headwind) and degrades to the plain
trend with an explanation when direction is not yet cached.

## Stream ingestion and cardiac drift

`make sync-streams` backfills time-series streams (time, heart rate,
smoothed velocity, moving flag, grade) for fetch-eligible runs per D15
(revised v1.4): running activity, heart rate present, moving time ≥
`STREAM_FETCH_MIN_MOVING_MINUTES` (default 20), within the historical
window. Fetching is a **data-availability decision**, split in v1.4
from the analysis gates — drift candidacy and `long_run_eligible` keep
their unchanged 45-minute threshold in dbt, and drift candidates remain
a strict subset of band candidates by construction. The backfill is
**resumable by construction**: each
activity's outcome commits as its own row in `raw_strava.streams` with
an explicit status — `success` and `unavailable` (Strava has no streams
for that activity; that never changes) are terminal, `failed` is retried
automatically next run, and an absent row means not yet attempted. At
most `STREAM_MAX_ACTIVITIES_PER_RUN` (default 50) activities per
invocation; rate limits stop the run cleanly between fetches with the
same exit-code-3 contract as the other syncs.

**Cardiac drift (decoupling)** — per D16, each analyzed run drops
non-moving samples, trims the first 10 minutes (warm-up) and final
5 minutes (cool-down), requires ≥ 30 minutes remaining, splits the
window into two equal-duration halves, and computes efficiency per half:

```text
decoupling_pct = (first_half_efficiency − second_half_efficiency)
                 / first_half_efficiency × 100
```

**Sign convention (D17): positive = efficiency declined in the second
half; near zero = stable; negative = the second half improved.** A
rising decoupling trend over comparable long runs is the observational
signal of interest — never proof of a fitness change on its own.

Coverage and pause thresholds (the two checks D16 leaves unquantified)
are dbt vars: average sample spacing in the window ≤ 3 s, non-moving
share ≤ 25 %. Every drift candidate that can't be analyzed carries a
deterministic exclusion reason in `fct_drift_candidates`; drift trend
weeks below the D12 run count are flagged `is_sufficient = false` and
excluded from trend lines, but stay visible in every table — never
deleted.

## Dashboard

`make app` serves five Streamlit views under decision D19's cap of
five (amended by revision v2.0): **Aerobic Efficiency** (weekly +
28-day rolling trend, temperature-band comparison, and the D22
pace-at-HR-band section — the same analytical question with intensity
controlled by construction, so it lives inside this view), **Weekly
Training** (mileage, moving time, run counts), **Cardiac Drift**
(run-level decoupling with the rolling trend), **Cycling
Training** (v2.0 Phase C1: weekly ride volume with median/mean speed,
cadence, heart-rate, and temperature context; ride validity is D25's
data-validity rules only — no intensity gating, and heart rate is
never required for a ride to count), and **Cycling Segments** (v2.0
Phase C2: per-segment effort trend — efforts as points, the rolling
5-effort median as the only statistic line, cumulative best as a
reference, a picker limited to ≥ 5-effort segments, and the
short-segment, VirtualRide-exclusion, and ride-validity caveats
displayed). The app is deliberately thin: it
reads **only the approved mart tables** — enforced by an explicit
table-level allow-list in the app code plus a test that pins the list's
exact contents and refuses any other relation, core facts included
(core and marts share the `analytics` schema, so a schema check alone
could not tell them apart) — and contains no business logic; every
metric, threshold, and flag is computed and tested in dbt.
Sample counts appear beside every statistic; weeks below the D12 run
count are flagged `is_sufficient = false` and excluded from trend lines
(the pace-at-HR-band chart instead plots every run×band median as a
faint point from `mart_run_band_segments`, v1.6, under a rolling line
whose vertices are sufficient weeks only), but stay visible in every
table; the segments view captions how many VirtualRide efforts were
held out (D28) and how many tracked segments sit below the 5-effort
gate; and each empty view explains exactly what data would populate
it.

## Data-quality principles

1. **Missing never means zero.** Absent HR, weather, or streams stays
   NULL (or an explicit status row) through every layer, down to the
   dashboard's empty states.
2. **Raw data stays recoverable.** Full API payloads live in JSONB next
   to the typed columns that ingestion itself needs; remodeling is a
   re-transform, never a re-download.
3. **Exclusion is explained, never silent.** Every ineligible run
   carries a human-readable reason; every aggregate carries its sample
   count.
4. **Idempotency everywhere.** Re-running any sync or build converges;
   nothing duplicates and nothing is lost to interruption.

## Known limitations

* Activities uploaded or edited **more than 14 days after they
  occurred** are only caught by `make reconcile`, not incremental sync.
  (The July 2026 heart-rate re-import was ingested exactly this way:
  old-dated re-uploads are invisible to the incremental window.)
* Open-Meteo's archive, queried without a `models` parameter, defaults to
  `best_match` — a per-hour stitch of ERA5 (0.25°, ~5-day delay),
  ERA5-Land (0.1°), and low-latency ECMWF IFS analysis (9 km). Recent
  runs therefore get **real preliminary values immediately**, not NULLs,
  and the response never indicates which dataset served which hour
  (`fetched_at` relative to the observation date is the only heuristic
  proxy). Because the cache-completeness check treats any non-NULL value
  as final, preliminary values are frozen until `make reconcile-weather`
  re-fetches them (its `IS DISTINCT FROM` upsert absorbs revisions).
  Under a daily `make all`, nearly every new run's weather is fetched
  inside the IFS window and frozen, while historical backfill is final
  reanalysis — two quality regimes in one dataset. All-NULL rows still
  occur, and still self-heal on later syncs, only when the archive
  genuinely has no data for an hour — now the rare case.
* The **> 90 °F band is survival-censored** (v1.8): extreme-heat runs
  are systematically cut short and fail the 15-minute validity floor,
  so that band only ever contains extreme-heat runs that were
  completed. Its n=0/n=1 is the finding — never a fair efficiency
  comparison against the cooler bands.
* **Short segments are noise-flagged, not excluded** (v2.0/D28):
  where a segment's median effort is under 120 s, ±1 s GPS sampling is
  a material share of the measurement, so its trend carries a
  displayed `short_segment` caveat. Small changes there describe
  sampling as much as fitness.
* **VirtualRide efforts never reach segment marts** (D28): the
  simulated course makes times incomparable with outdoor efforts on
  the same segment. They stay in `fct_segment_efforts`, flagged, and
  the segments view captions how many were held out — excluded from
  statistics, never silently absent.
* The **dashboard screenshots** in `images/` are still pending capture
  now that the marts are populated. (dbt lineage is rendered directly
  in this README via `make dbt-dag`.)
