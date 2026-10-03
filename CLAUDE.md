# CLAUDE.md

## Source of truth
- The full spec is docs/PROJECT_PLAN.md plus revision addendums in
  docs/decisions/ (v1.5 onward). Read them before starting work.
- Decisions D1–D22 in the plan are locked (D18 as revised by the v1.5
  addendum). If a task conflicts with one, stop and ask — do not
  silently deviate.

## Current status
- Revision v1.1 is implemented: efficiency aggregates cover every run
  with valid HR data (`is_valid` — HR present, 90–200 bpm, pace
  4:00–20:00, ≥15 min). easy_run_eligible / easy_hr_max and the
  "qualifying easy runs" vocabulary are gone; marts expose
  valid_run_count etc. Drift candidacy is ≥45 min moving + HR present
- Revision v1.2 is implemented: layering is staging → intermediate →
  core → marts. int_run_efficiency computes measures + validity /
  efficiency and feeds fct_runs (37 cols; original 34 unchanged); marts
  read core/seeds/marts only; int_run_drift_halves is now core
  fct_drift_candidates (analytics schema). is_valid := exclusion_reason
  is null; layering enforced by tests/test_dbt_layering.py. source()
  lives in staging only, except raw_strava.streams readable from
  intermediate (int_run_stream_state feeds fct_drift_candidates).
- Revision v1.3 is implemented: trend-mart rolling columns have static
  names (rolling_median_efficiency / rolling_valid_run_count in
  mart_efficiency_trend; rolling_median_decoupling_pct /
  rolling_drift_run_count in mart_drift_trend) — trend_window_days
  changes the window, never the interface. The D14 band range predicate
  renders from the temperature_band_range macro (the seed stays the
  sole bounds definition). fct_drift_candidates.activity_id carries a
  relationships test to fct_runs (proven red on an orphan row first).
- Revision v1.4 is implemented (addendum in docs/PROJECT_PLAN.md): the
  45-min var split — the stream FETCH gate is
  stream_fetch_min_moving_minutes (20, settings field + dbt var, env
  STREAM_FETCH_MIN_MOVING_MINUTES); drift candidacy and
  long_run_eligible keep 45 unchanged. D22 pace-at-HR-band metric:
  hr_bands seed (10 bpm, joined by range via hr_band_range on
  round(hr_bpm)), int_band_window_samples + int_run_band_assessment
  (exclusion ladder, encoded once) → fct_band_candidates (its core
  projection) + fct_run_band_segments → mart_band_weekly →
  mart_band_trend; weekly statistic is the median across runs of
  run-level band medians (velocity space, pace derived); trims 5/2
  min, window ≥ 10 min of pooled moving dwell, dwell ≥ 5 min/band,
  coverage var reused from drift (also as the dwell cap). One
  deliberate layer-matrix widening, proven red first: seeds are
  allowed parents of intermediate models. Renders inside the Aerobic
  Efficiency view — D19's three-view cap is NOT amended; allow-list
  grew by exactly mart_band_trend (mart_band_weekly deliberately off
  it). Existing marts proven byte-identical pre-gate-change; band data
  for 20–45 min runs arrives only as post-merge `make sync-streams`
  backfill drains ('streams not yet loaded' until then).
- Airflow adoption (v1.5) is complete and merged to main
  (2026-07-16). Addendum (docs/decisions/v1.5-airflow-addendum.md,
  revising D18) adopted; Airflow 3.3.0 at ~/.venvs/airflow on Python 3.13.14;
  the running_pipeline DAG (orchestration/dags/running_pipeline_dag.py)
  mirrors `make all` daily 06:00 America/Chicago, proven on the real
  scheduler path (10/10 task successes, idempotent no-op counts).
  Airflow is a THIN scheduling and observability layer only; see the
  scope constraints below.
- Revision v1.6 + v1.6.1 merged to main (2026-07-20).
  mart_run_band_segments (run × band grain, explicit-column projection
  of core fct_run_band_segments + the hr_bands seed — no layer-matrix
  change; the app allow-list grew by exactly this name, red-first)
  feeds the band chart: one faint point per run per band under the
  28-day rolling median line, whose vertices key on window support
  (rolling_band_run_count >= ROLLING_LINE_MIN_WINDOW_RUNS, app-side
  constant, default 2) with segment-id path breaks — never bridging
  (v1.6.1 supersedes v1.6's "sufficient weeks only" line rule). D22's
  weekly definition and mart_band_weekly's output are unchanged,
  pinned by assert_band_weekly_matches_segment_mart (velocity space).
  Addendums: docs/decisions/v1.6-band-run-scatter.md + the v1.6.1
  entry appended to docs/PROJECT_PLAN.md.
- Revision v1.7 merged to main (2026-07-22). D14's banding input
  revised: run-level bands are assigned by apparent_temperature_f
  (feels-like — humidity and wind fold into the banding; no new API
  fields, raw columns, or bands; seed bounds and names unchanged, they
  now bound apparent temperature). mart_efficiency_trend dropped
  temperature_band_key/-label and its seed join (weekly averages are
  never banded — a chosen v1.3-rule interface change);
  avg_temperature_f stays dry-bulb context and joined the trend
  tooltip. The band ladder is a proven partition: no_weather means "no
  matched feels-like temperature" (dry-bulb fallback deliberately
  forbidden), the seed leg carries not is_trainer, and the run-grain
  assert_temp_band_ladder_assigns_every_valid_run_once (red-first on
  both arms; edited in LOCKSTEP with mart_efficiency_by_temp_band)
  guards it alongside the aggregate conservation test. Context
  columns: avg_apparent_temperature_f is deliberately mart-only —
  never surface it in the app; mart_run_quality.apparent_temperature_f
  renders as the "Air °F"/"Feels-like °F" pair. Zero band movers on
  the 2026-07-22 warehouse (pre-registered and verified). Addendum:
  docs/decisions/v1.7-apparent-temperature-banding.md.
- Revision v1.8 merged to main (2026-07-22). The D14 seed is
  five bands — <50 / 50–70 / 70–80 / 80–90 / >90 °F — splitting
  v1.7's open top band at NOAA heat-index anchors (80/90°F) that
  coincide with empirical gaps in the observed feels-like
  distribution. Stopping rule (in the addendum AND seeds.yml): a band
  boundary requires BOTH an external anchor AND an empirical gap —
  never add or move a bound without a revision satisfying both. The
  >90 band is survival-censored (extreme-heat runs fail the 15-min
  floor; n=0/n=1 is the finding — documented in marts.yml + README
  known limitations, never a fair comparison). Keys are stable
  machine identifiers: warm kept its key when its bounds narrowed
  (label "70–80°F"); new keys hot / very_hot; banding input and the
  50/70 boundaries unchanged from v1.7. App: five-blue ordinal ramp
  (documented sequential ramp, 100-step stride, ordinal-validated),
  chart height 266, dry-bulb figures labeled "air" everywhere they
  render near the feels-like pair. Redistribution reproduced exactly
  as pre-registered (valid 3/7/1, full-row 3/9/6); no red proof by
  declaration — the boundary fixtures (80.0/90.0/95.0) extend an
  already-green partition test. Addendum:
  docs/decisions/v1.8-five-band-recalibration.md.
- Revision v1.9 merged to main (2026-07-23), docs-only.
  PROJECT_PLAN.md §3's single "Explicitly Out of Scope" list split
  into "Non-Goals (this project's thesis)" (Strava-screen
  replication, ML performance/race-time prediction, mobile
  development — revised only by revisiting the project's purpose,
  never a routine addendum) and "Deferred (out of scope for Release
  1.0)" (real-time/event-driven ingestion, cloud-hosted components —
  enter scope only via the standard decision-addendum process). The
  Decision Log preamble now states out-of-scope items are
  release-scoped unless listed as non-goals. Governance
  clarification, not a loosening: no locked decision changed, no
  scope entered or left; the old list's five items map 1:1 onto the
  two new lists (two rephrased, mapping in the addendum). The
  webhook/FastAPI extension surfaced the ambiguity but is NOT
  admitted by v1.9 — it still needs its own addendum. Addendum:
  docs/decisions/v1.9-scope-taxonomy.md.
- Revision v2.0 adopted 2026-08-19: cycling admitted as a peer domain
  (purpose-level revision — it touches the non-goals list, hence
  major version). New decisions D23–D30; D8 amended (wind direction,
  Phase C3) and D19 amended (view cap 5; allow-list grows by exactly
  three named marts across C1–C2, red-first). Phases C1–C3 added
  (C1+C2 = Release 2.0, C3 = Release 2.1). Hard promise: running
  output byte-identical in every C-phase. See "Scope constraints —
  Cycling domain (v2.0)" below. Addendum:
  docs/decisions/v2.0-cycling-domain.md.
- Current phase: Phase C1 merged to main (2026-08-19) — Release 2.0
  begins. Coordinate/weather eligibility extended to D23 ride types
  (RIDE_SPORT_TYPES beside RUNNING_SPORT_TYPES in
  coordinate/weather_ingestion; weather excludes VirtualRide,
  mirroring VirtualRun; trainer predicate unchanged). New chain
  int_rides_with_weather → int_ride_measures → fct_rides →
  {mart_weekly_cycling, mart_ride_quality}. D25 ladder single-encoded
  in int_ride_measures (HR-outside-sanity-when-present → speed
  outside 3–35 mph → under 15 min; HR absence NEVER excludes — it
  gates only HR aggregates); is_indoor := is_trainer or VirtualRide
  forces location_key NULL (indoor rides permanently weatherless —
  stronger than the running side); average_cadence_rpm staged per
  D26 (crank rpm, cycling-chain-only consumer). New vars:
  ride_sport_types, valid_ride_min_moving_minutes,
  ride_speed_mph_floor/_ceiling, min_weekly_valid_rides; hr_sanity
  vars reused (one mechanism, two consumers). App: fourth view
  "Cycling training"; allow-list +2 (each proven red first); e-bike
  absence pinned by the var-rendered
  assert_fct_rides_excludes_ebike_types (red-proven by injection).
  Running output proven byte-identical: 13-relation ordered-CSV
  snapshot diffed interim (post-sync, pre-dbt) and final — both
  empty; ride weather sync inserted 624 rows, updated 0.
- Phase C2 merged to main (2026-08-24; Release 2.0 completes once the
  live backfill drains).
  D24 ingestion: per-ride detail fetch (include_all_efforts=true)
  into raw_strava.activity_details (status rows success/failed/
  unavailable, absent = not attempted; status rows ARE the resume
  mechanism — NO watermark: the C2 plan bullet's "sync-state
  watermark" was a drafting error, fixed in PROJECT_PLAN.md) +
  segment_efforts (effort_id PK) + segments (last-write-wins summary
  upsert), one commit per activity. Batch cap
  SEGMENT_FETCH_MAX_ACTIVITIES_PER_RUN (50, selection-time), exit-3
  contract, sync-segment-efforts CLI/Make/DAG task (parity-pinned).
  dbt: stg_strava__segment_efforts + stg_strava__segments →
  int_segment_efforts (inner join int_ride_measures = D23 grain by
  construction; parent-ride weather + ride_is_valid /
  ride_exclusion_reason carried as displayed caveats, never filters)
  → fct_segment_efforts (VirtualRide kept + flagged; relationships
  pin to fct_rides red-proven by injected orphan) →
  mart_segment_trend (segment × non-virtual effort: rolling 5-effort
  median, cumulative best, is_sufficient ≥ 5, short_segment on
  strict < 120 s median elapsed, virtual_effort_count for the view
  caption — the D19 allow-list route). Vars
  segment_rolling_effort_window / segment_trend_min_efforts /
  short_segment_seconds (D28 names verbatim); no layer-matrix
  change. App: fifth view "Cycling segments" (picker limited to
  sufficient segments; the rolling line STARTS at
  ROLLING_LINE_MIN_WINDOW_EFFORTS = 2 — effort-ordered series have
  no calendar gaps, so no path breaks); allow-list +1 red-first
  (MART_TABLES pin and VIEW_NAMES render both proven red). Running
  output byte-identical via the 13-relation ordered-CSV snapshot
  diff. Owner's post-merge steps: live `make sync-segment-efforts`
  backfill + the Strava My Results spot check (criterion 2).
- Phase C3 complete (2026-08-26, branch c3-wind — Release 2.1). D29:
  wind_direction_10m joins HOURLY_VARIABLES, typed into
  raw_weather.hourly.wind_direction_deg (idempotent ALTER in
  sql/raw_weather.sql — the repo's first in-place column migration;
  FROM convention, 0 = north is a value, missing NULL never zero).
  Cache completeness became _IS_COMPLETE: has-any-measurement AND
  direction resolved (a value OR the payload carrying the
  wind_direction_10m key — post-C3 payloads always carry it, null
  when the archive has none), so pre-migration rows re-fetch exactly
  once and the queue terminates (livelock pinned red-by-mutation);
  the payload-only IS DISTINCT FROM upsert guard is unchanged. D30:
  int_segment_geometry (segment grain — great-circle bearing via
  bearing_degrees macro, haversine straight-line via haversine_meters
  macro R=6371008.8, sinuosity 4 dp, winding_segment strictly > new
  var winding_sinuosity_max 1.3, unknown geometry NULL never false);
  int_segment_efforts gains the effort-hour wind match (nearest
  has_measurements observation at the parent ride's start cell to
  the EFFORT's start time, 60-minute rule, deterministic tie-break
  as a commented deviation) and computes headwind_mph (POSITIVE =
  HEADWIND, pinned) / crosswind_mph; fct_segment_efforts +8 columns;
  mart_segment_trend +exactly headwind_mph / crosswind_mph /
  winding_segment. NO allow-list change and NO layer-matrix change.
  Three singular pins (winding var-pin; headwind formula+null-iff;
  mart-core lockstep — the latter two red-proven by injection). App:
  segment points color on the diverging blue↔red pair when headwind
  exists (gray = no direction), sign + spatial-caveat captions,
  NaN-safe winding caption; direction-free segments render the EXACT
  C2 spec plus a D29 explanation (pinned by AppTest on
  direction-free fixtures). Running output byte-identical via the
  13-relation snapshot diff (tmp/c3-impl-before/ vs -after/, both
  empty). Live verification complete (2026-08-26, no code edits, no
  live bugs): bootstrap idempotency re-proven; the reconcile drained
  in ONE pass — 19 requests, updated 1752, inserted 264 (recent
  activity days riding the merged ranges) — and a follow-up
  incremental sync made 0 requests (queue termination live-proven);
  post-drain 2016/2016 hours carry a direction, 0 unresolved, 0
  residue (note: Open-Meteo encodes north as 360°, observed range
  1–360 — no live 0°; the 0-as-value contract is fixture-pinned and
  360 is tolerated everywhere, cosine being periodic); the live
  13-relation diff came back byte-identical (per-file checksums) —
  stronger than the allowed weather-descendants scope; the colored
  headwind view verified live. Slow days explained on real data: the
  most-ridden segment's slowest effort carried +9.5 mph headwind,
  its fastest a −5.7 tailwind.
- Segment view cleanup (app-only, 2026-08-27): metric row shows
  distance and average grade from mart_segment_trend; VirtualRide
  tile removed, count stays in the chart caption; standing captions
  reduced to SEGMENT_TREND_NOTE and HEADWIND_SIGN_NOTE, and tests
  assert those constants instead of phrases. D17 unchanged;
  enforcement moved from phrase match to constant check. Running
  views and their phrase assertions untouched.
- dbt view-drop deadlock fix (2026-10-03, branch test-speed). Since
  C3, two concurrent `drop view ... __dbt_backup cascade` statements
  (stg_strava__activities / stg_weather__hourly) could deadlock:
  int_segment_efforts reads weather directly and activities through
  two views, so the cascades lock the shared downstream views in
  different orders. It failed about 5% of test builds and could hit
  `make dbt-build`. Fix: dbt/macros/drop_view.sql overrides
  postgres__drop_view to take one transaction-scoped advisory lock in
  the same statement as the drop, so view drops run one at a time. No
  model, schema, or output change. Pinned by
  test_view_drops_take_the_cascade_lock (red first). Do not remove
  the macro; re-run the pin after any dbt-postgres upgrade.

## Scope constraints — Airflow adoption (v1.5)
- (a) Airflow owns no state — watermarks, per-item status rows, and
  destination-as-cache remain the pipeline's.
- (b) catchup=False and no templated date windows, because Strava
  filters by activity start date and interval-based windows would
  reintroduce the late-upload gap the 14-day overlap already solves.
- (c) tasks invoke existing Make targets unchanged — no pipeline code
  changes.
- (d) LocalExecutor-or-simpler, SQLite metadata DB, no
  Celery/Redis/Docker for Airflow in this release.
- (e) Airflow lives in its own venv, never in the project's
  dependencies.
- All DAG code must use the Airflow 3 API — schedule=, no
  schedule_interval, no execution_date.
- Exit-3 policy (addendum): CLI exit 3 = clean rate-limit stop →
  task SKIPPED (skip_on_exit_code=3), downstream none_failed; sync
  tasks call the CLI directly because make masks recipe exit codes —
  a narrow, documented amendment to constraint (c)'s letter.

## Conventions
- Postgres: running_analytics_db / running_user / host port 5433
- Schemas: raw_strava, raw_weather, staging, intermediate, analytics
- Never commit secrets, tokens, .env, or exact coordinates.
- Never log access or refresh tokens.
- If reality doesn't match the plan mid-task, stop and surface it.

## Commands
- Activate the venv first: `source .venv/bin/activate` (Python 3.13)
- Test:    `make test`    (pytest; all external HTTP mocked; DB-integration
  tests run against a scratch database and skip visibly when Postgres is down)
- Lint:    `make lint`    (ruff check src tests)
- Format:  `make format`  (ruff format src tests)
- DB:      `make up` / `make down` / `make bootstrap`
- Sync:    `make sync-activities` (incremental) / `make reconcile` (full)
- Weather: `make sync-weather` (incremental by nature) /
  `make reconcile-weather` (re-fetch cached hours)
- Streams: `make sync-streams` (resumable; success/unavailable are
  terminal statuses in raw_strava.streams, failed retries next run)
- CLI:     `running-pipeline athlete` / `running-pipeline authorize` /
  `running-pipeline sync-activities [--full]` /
  `running-pipeline sync-weather [--full]`
- Sync watermark lives in raw_strava.sync_state (last successful run's UTC
  start time); it advances only after a fully successful sync
- Weather has NO watermark and NO credentials: raw_weather.hourly is the
  cache (UNIQUE location_key + weather_timestamp); missing weather is a
  row with NULL measurements, never zero and never silently absent;
  everything is requested and matched in UTC
- Coordinates: `make backfill-coordinates` resolves run starts into
  raw_strava.activity_coordinates (payload start_latlng, else detail
  polyline first point, else explicit 'unavailable'). Needed because
  Strava's "hide entire map" privacy setting strips start_latlng from
  ALL API payloads; weather eligibility and dbt staging prefer the
  resolved row. Run it before sync-weather (make all does)
- dbt:     `make dbt-build` / `make dbt-test` / `make dbt-freshness` /
  `make dbt-docs` (project in dbt/ per D4; profiles.yml auto-copied from
  the example, reads .env; metric thresholds are dbt vars in
  dbt_project.yml; layers map to the D3 schemas via the
  generate_schema_name override — do not remove it)
- App:     `make app` (Streamlit, five views under D19's amended cap
  of five) / `make all`
  (every sync + dbt build)
- Airflow: `make airflow-install` (separate venv at ~/.venvs/airflow —
  never the project venv) / `make airflow-start` (airflow standalone,
  AIRFLOW_HOME=~/airflow, DAGs read from orchestration/dags)
- Airflow standalone first-start verified (2026-07-16, PATH fix in
  airflow-start: standalone respawns its components as bare `airflow`
  resolved via PATH, so the recipe must prepend $(AIRFLOW_VENV)/bin —
  absolute-path invocation alone is not enough)
- Rotated Strava tokens live in `.secrets/strava_tokens.json` (gitignored,
  0600). The STRAVA_REFRESH_TOKEN in .env is bootstrap-only and goes stale
  after the first refresh — that is by design, not a bug.

## Style for notes/
Write all content in notes/ in plain, simple language (ASD-STE100
style).

- Short sentences and one idea per sentence, when possible.
- Active voice. Say who or what does each action.
- Common words. No invented metaphors or coined phrases.
- Keep necessary technical terms (median, mart, idempotent) and
  define each inline at first use.

Example: write "Check where the filter lives before you edit,"
not "Execute a verification fork prior to modification."

## Verifying major chart and UI changes
Visual verification is required only for changes that materially affect a rendered chart or its surrounding UI.

This includes:

* Adding, removing, or replacing a chart
* Changing chart type, layout, scale, axis behavior, aggregation, filtering, or displayed fields
* Changing labels, legends, tooltips, annotations, colors, sizing, or responsive behavior in a way that could affect readability or interpretation
* Changing Streamlit layout, containers, tabs, controls, or state behavior that could affect how a chart is displayed or used
* Fixing a visual bug whose resolution cannot be confirmed reliably through tests or schema validation alone

Visual verification is not required for:

* Refactoring with no intended visual change
* Test-only, documentation-only, or configuration-only changes
* Data-model changes that do not alter the chart’s output
* Minor copy edits
* Dependency or formatting changes that do not affect rendering
* Changes already covered by a focused visual regression test

When visual verification is required:

1. Ensure Postgres is running and start the app with `make app` on port 8501.
2. Use the Playwright MCP tools to open the affected view, wait for the chart or UI element to render, and take a screenshot.
3. Open the screenshot and verify the affected behavior against the change’s intent before committing.
4. State in the report what was visually checked and what the screenshot showed.

Screenshot paths come from the take_screenshot tool result — read the
saved path there before opening the file (`./name.png` means the repo
root). Do not pass a filename: named screenshots bypass the configured
output dir and land in the repo root; without one, screenshots go to
tmp/playwright (set in .mcp.json). If an artifact appears missing,
search for it inside the repo only — never `~` or `/`.

Limit verification to the affected view or component. Do not inspect unrelated charts or pages unless the change could reasonably affect them.

If the app cannot run because the database or another required service is unavailable, fall back to rendering the affected chart with `chart.save` and `vl-convert`, then state that limitation in the report.

## Testing and validating changes
After the change:
1. Run the smallest directly relevant test first.
2. Run the full suite only if the targeted test passes and the change could
   affect unrelated modules.
3. Do not retry a failing command more than once without explaining the failure.

Test tiers:
- **While working:** the smallest tier that covers the change.
  `make test-fast` (no database) covers src/ changes that do not touch
  SQL. `make test-running` and `make test-cycling` add that domain's
  dbt integration tests to the fast tier (markers `dbt_running` /
  `dbt_cycling`). `make test-app-render` adds the Streamlit render
  tests (marker `app`).
- **Before each commit:** the full suite (`make test`) for any diff
  touching src/, dbt/, test fixtures, Makefile, or config. A diff
  confined to app/ and tests/test_app.py may commit on
  `make test-app-render`.
- **Before merge and at session end:** the full suite.

Every integration test in tests/test_dbt_models.py and
tests/test_app.py carries exactly one domain marker
(tests/test_marker_guard.py enforces it). The database-backed
ingestion tests carry none: they run only in the full suite. Tier
runs must report skip counts.

## Ambiguous requests
When a request has material ambiguity:

1. Do not edit files or run expensive commands.
2. Restate the intended outcome.
3. Identify assumptions that could alter the implementation.
4. Ask concise clarifying questions.
5. Propose a bounded plan and wait for approval.

Prefer the smallest relevant file search and targeted test. Do not perform
adjacent refactoring unless explicitly requested.

## Scope constraints — Cycling domain (v2.0)
- Running output is byte-identical: no running model, mart, view, or
  test changes in any C-phase. D15 is untouched — no ride streams.
- No power fields modeled while `device_watts` is false — estimated
  watts/kilojoules live in raw JSONB only (D27).
- Ride grain excludes `EBikeRide`/`EMountainBikeRide`; `VirtualRide`
  is indoor-flagged and its segment efforts never reach marts (D23,
  D28).
- Segment views must name their improvement over the Strava screen
  (reworded non-goal); plain replication stays out.
- Detail fetches keep the batch cap and the exit-3 rate-limit
  contract; status rows are the resumability mechanism.
- Wind direction uses the meteorological FROM convention; 0° = north
  is a value, missing is NULL, never zero. Headwind sign is pinned:
  positive = headwind (D30). Winding and short-segment flags are
  displayed, never filters.
- App cap is now 5; allow-list additions are exactly the three named
  marts, each proven red first; C3 adds none (amended D19).