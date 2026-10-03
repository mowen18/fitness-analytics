# Test suite speed — plan

Status: Draft 2026-10-03, from a code reading of `main` (2026-08-27).
Phase 0 measured 2026-10-03 (see "Measurements"). Where the measured
numbers differ from the estimates below, the measured numbers win.

## Problem

The full suite takes roughly 3-6 minutes. CLAUDE.md requires the full
suite for any diff that touches `src/`, `dbt/`, fixtures, the Makefile,
or config. Most real changes touch `dbt/`, so almost every small change
costs 5 minutes.

The suite will also keep growing. Each new feature adds integration
tests, and each of those starts its own dbt builds. v2.3 (training
load) alone will add five or more.

## Where the time goes (from reading the code)

The integration tests start dbt as a new process for every call:

| Source | dbt calls | What each call does |
|--------|-----------|---------------------|
| `test_dbt_models.py`, 19 tests | 19 × `dbt build` | Builds all ~40 models and runs all ~250 dbt data tests |
| `test_app.py`, 6 tests | 6 × `dbt build` | Same full build, then renders the Streamlit views |
| "Pin" tests (inject a bad row, prove the dbt test fails, remove it, prove it passes) | ~20 × `dbt test --select` | Starts dbt again to run one test |

That is about 45 dbt process starts per run. Each one pays two costs:

1. **Startup.** Python imports dbt and dbt reads the project. dbt's
   partial parsing helps after the first call, but the import cost is
   paid every time.
2. **Work.** A full build runs every model and every data test, even
   when the test checks only one model chain. The fixtures are tiny,
   so most of this time is per-query overhead, not data volume.

The tests that do not use the database (ingestion, token refresh,
layering, DAG parity, weather client) are probably a few seconds in
total.

Estimate only: the integration tests are about 95% of the 3.5 minutes.

## What does NOT change

- The full suite still runs before every commit that touches `src/`,
  `dbt/`, fixtures, the Makefile, or config, and before every merge.
  The speed-up is for the time between commits.
- No test is deleted and no assertion gets weaker.
- Each test keeps its own fixture data and its own `TRUNCATE`. Tests
  stay independent.
- Red-first stays: every pin test still proves its dbt test can fail.

## Phase 0 — Measure (about 10 minutes, no code changes)

Measure first. The results decide whether Phases 3 and 4 are worth
doing.

1. `pytest --durations=0 -q` and save the output.
2. `pytest -m "not integration" -q` and record the time.
3. In `dbt/`, time `dbt --version` (import cost only), `dbt parse`
   (parse cost), and one `dbt build` on an empty scratch database
   (startup plus all models and tests).
4. Repeat the `dbt build` with `--threads 8` to see whether thread
   count matters.

Write the numbers into this file under "Measurements". If startup is
more than half of each call, Phase 4 moves up. If the per-build work is
more than half, Phase 3 moves up.

## Phase 1 — Test tiers (no change to how any test works)

Goal: after a small change, run only the tests that can see it.

1. **`make test-fast`**: `pytest -m "not integration" -q`. For changes
   to `src/` that do not touch SQL. If this target already exists on
   your local branch, keep it as is.
2. **Domain markers on every integration test**:
   - `dbt_running`: run models, drift, bands, weather matching.
   - `dbt_cycling`: rides, segments, wind.
   - `app`: the Streamlit render tests.
   - (`dbt_load` joins when v2.3 lands.)
3. **Make targets**: `make test-running`, `make test-cycling`,
   `make test-app-render`. Each runs the fast tier plus one marker.
4. **Guard test**: every test marked `integration` in
   `test_dbt_models.py` and `test_app.py` carries exactly one domain
   marker. Without this guard, a new test with no marker would silently
   drop out of every domain tier. Prove it red first: remove one
   marker and watch it fail.
5. **CLAUDE.md "Testing and validating changes"**: replace the scoped-
   tier paragraph with three tiers:
   - **While working:** the smallest tier that covers the change.
   - **Before each commit:** the full suite (unchanged).
   - **Before merge and at session end:** the full suite (unchanged).

Expected result: a change to one cycling model costs the cycling tests
only, roughly a third to a half of the integration time.

## Phase 2 — Parallel workers (pytest-xdist)

Goal: the full suite itself gets faster.

1. Add `pytest-xdist` to the dev dependencies.
2. **One scratch database per worker.** In `conftest.py`, name the
   database `running_analytics_test_<worker_id>` (xdist gives each
   worker an id; without xdist it is `master`). Each worker creates
   and drops its own database.
3. **Remove the hard-coded database name.** `TEST_DB` is a module
   constant in `test_dbt_models.py` and is imported by `test_app.py`.
   It must come from the session fixture so `run_dbt` and the app's
   `POSTGRES_DB` override point at the worker's database.
4. **One dbt target folder per worker.** Pass
   `--target-path target/<worker_id>` and `--log-path` the same way.
   Without this, workers overwrite each other's `manifest.json` and
   partial-parse file.
5. **Worker count.** Start with `-n 4`. Each dbt build already uses 4
   threads, so 4 workers means up to 16 queries at once against the
   Docker Postgres. Go higher only if Phase 0 and a trial run show
   spare capacity. Do not use `-n auto` by default.
6. **Make targets.** `make test` uses `-n 4`. Add
   `make test-serial` for debugging, because parallel output is harder
   to read.

Proof: the same tests pass and the same tests fail, serial and
parallel. Run both once, compare the pass/fail lists, and record the
result.

Expected result: the integration part runs 2.5–3.5× faster on 4
workers. It is not 4× because some tests are longer than others and
Postgres is shared.

## Phase 3 — Build only what each test checks

Goal: each build does less work.

1. Give `run_dbt("build")` an optional selector. A test that checks
   `mart_segment_trend` builds `+mart_segment_trend`: that model, every
   model above it, and the dbt tests attached to them.
2. Convert tests one at a time. **Keep the full build** for:
   - `test_dbt_tests_fail_on_known_invalid_fixtures` (it proves that
     tests across several models fire).
   - The app render tests (every view reads a different mart).
   - Any test whose assertion depends on a dbt test outside the
     selected chain.
3. **Mutation check for each converted test.** Break the model the test
   checks (for example, flip a sign or change a threshold), confirm the
   test goes red, and then restore the model. This proves the narrower
   build still sees the code under test. Record each check in the
   commit message.

Expected result: depends on Phase 0. If the per-build work is large, the
narrow tests could drop by half or more.

## Phase 4 — Run dbt in the test process (only if Phase 0 says so)

Goal: pay the dbt import cost once per worker instead of once per call.

dbt-core has a Python entry point (`dbtRunner`) that runs commands
without starting a new process. Two costs make this the last phase:

- Many tests check `result.stdout` for a test name. `dbtRunner` returns
  result objects, not stdout, so those assertions need a rewrite to
  check the node name and status instead.
- dbt does not support two commands at once in one process. This is
  fine with xdist (one process per worker) but must stay serial within
  a worker.

Do this only if Phase 0 shows that startup is a large share of each
call.

## Rejected

- **One shared build for many tests.** Putting all fixtures into one
  database and building once would be fastest. It breaks test
  independence: fixtures share activity ids and weather cells, and a
  failure in one test's data could change another test's result.
- **Mocking dbt or the database.** The integration tests exist to run
  the real SQL. A mock would test nothing.
- **Running pin tests less often.** They are the proof that each dbt
  test can still fail. They stay in the full suite and in their domain
  tier.

## Order and size

| Phase | Effort | Risk | When |
|-------|--------|------|------|
| 0 Measure | 10 min | none | first |
| 1 Tiers and markers | 1 short session | low | next |
| 2 Parallel workers | 1 session | medium (shared names in fixtures) | after 1 |
| 3 Narrow builds | 1–2 sessions | medium (mutation checks needed) | if Phase 0 shows build work is large |
| 4 In-process dbt | 1 session | medium (assertion rewrites) | only if Phase 0 shows startup is large |

Phases 1 and 2 are independent of v2.3, so they can land before the
training-load work starts. v2.3's new tests then join the tiers and run
in parallel from the start.

## Measurements

Measured 2026-10-03 on `main` at 363f06d. Apple silicon, 12 logical
CPUs (8 performance cores), Postgres 17 in Docker, dbt-core 1.11.12,
pytest 9.1.1, Python 3.13.14. The dbt timings ran against a throwaway
empty database with its own target folder, after the suite finished.

| Measure | Time |
|---------|------|
| Full suite, serial | 227.9 s (3:47). 164 passed, 0 skipped |
| Non-integration tests only | 0.5 s (0.8 s wall). 96 passed, 68 deselected |
| `dbt --version` | 0.76 s (runs: 0.82, 0.76, 0.76) |
| `dbt parse` | 1.06 s with partial parsing; 1.76 s on a new target folder |
| One `dbt build`, 4 threads | 3.5 s (runs: 3.42, 3.57, 3.54). dbt reports 2.2–2.3 s of this as model and test work (34 models and seeds, 273 data tests) |
| One `dbt build`, 8 threads | 3.4 s (runs: 3.38, 3.41) |
| One `dbt test --select` (extra row) | 1.25 s (runs: 1.26, 1.24). The test itself is 0.10 s |
| Slowest 10 tests (from `--durations`) | See the list below |

Slowest 10 tests:

| Test | Time |
|------|------|
| `test_band_medians_dwell_and_exclusion_ladder` | 37.5 s |
| `test_drift_decoupling_formula_and_analysis_window` | 34.7 s |
| `test_every_view_renders_with_populated_marts` (app) | 22.8 s |
| `test_relationships_test_fails_on_orphan_band_candidate` | 15.3 s |
| `test_band_candidates_exclusive_exhaustive` | 12.9 s |
| `test_segment_pin_tests_fail_on_injected_rows` | 9.0 s |
| `test_wind_pin_tests_fail_on_injected_rows` | 8.8 s |
| `test_effort_speed_formula_and_pins` | 8.7 s |
| `test_totality_test_fails_on_unassignable_run` | 6.1 s |
| `test_ebike_pin_test_fails_on_injected_ebike_row` | 6.1 s |

### What the numbers show

1. **The dbt tests are the whole cost.** The 27 tests that start dbt
   take 226 s of the 228 s. The other 137 tests take under 2 s in
   total, including the 41 database tests that do not start dbt.
2. **Counts.** `test_dbt_models.py` has 21 integration tests, not 19.
   The suite starts dbt 47 times, not about 45: 27 builds (21 + 6) and
   20 `dbt test --select` calls.
3. **Five tests are half the suite, and their cost is data volume.**
   The five tests that load activity streams take 123 s (54%). A test
   with small fixtures takes about 3.6 s. So the estimate "the fixtures
   are tiny, so most of this time is per-query overhead" is right for
   22 tests and wrong for these five.
4. **Rough split of the 226 s.** Startup: 47 starts × about 1.2 s =
   55 s (24%). Build work on small fixtures: 27 builds × about 2.3 s =
   62 s (27%). Extra work on stream data in the five tests: about
   100 s (44%). Streamlit renders and the rest: under 10 s.
5. **The stream cost sits in a few nodes.** The dbt log of the serial
   run gives the time per node, summed over all 27 builds (375
   thread-seconds in total). The band chain takes 176 (47%):
   `fct_run_band_segments` 38, `fct_band_candidates` 26, and 112 in
   data tests on the views `int_band_window_samples` and
   `int_run_band_assessment` (each data test runs the view again).
   `fct_drift_candidates` takes 20. The tests on
   `int_run_stream_samples` take 19.
6. **Thread count does not matter** on small fixtures: 3.4 s with 8
   threads, 3.5 s with 4. Keep 4.
7. **By domain** (what the Phase 1 tiers will cost, serial): running,
   9 tests, 122 s (54%). Cycling, 12 tests, 62 s (27%). App, 6 tests,
   42 s (19%).
8. **Limit for Phase 2.** The longest test is 37.5 s. No worker count
   can make the full suite faster than that. The ideal on 4 workers is
   226 ÷ 4 = 57 s.

### Order of Phases 3 and 4

The order does not change. Phase 3 stays ahead of Phase 4.

- In a build call, startup is 1.06 s of 3.5 s (30%). The work is more
  than half, so the rule in Phase 0 moves Phase 3 up.
- In a `dbt test --select` call, startup is about 92%. But the 20
  calls take only 25 s in total (11% of the suite).
- Phase 4 can save the import cost of 47 starts: 47 × 0.76 s = 36 s
  (16%), or about 50 s if it also saves the parse.
- Phase 3 should start with the five stream tests. The drift test
  (34.7 s) builds the band chain, which it does not check, and the
  band tests build the drift chain. The other 22 tests can each save
  at most about 2 s.

## Found during Phase 1 — deadlock inside `dbt build`

The full suite failed twice during Phase 1, each time in a different
test. The test changes were not the cause. The cause was a Postgres
deadlock inside one `dbt build`.

- **What happens.** dbt replaces a view by swapping in a new one, then
  dropping the old one with `CASCADE`. The cascade also drops the old
  views downstream of it. Since C3, `int_segment_efforts` reads
  `stg_weather__hourly` directly and `stg_strava__activities` through
  two other views. So the two staging drops lock the same views in
  different orders. When both drops start at the same moment, Postgres
  stops one of them with "deadlock detected".
- **How often.** 5 deadlocks in 93 builds, all in builds with cycling
  fixtures. One of the five was on the tree without the Phase 1
  changes. The 32 builds before them had none.
- **Proof of the cause.** Two database sessions dropped two views of
  the same shape at the same moment. Plain drops: 45 deadlocks in 300
  rounds. With the lock described below: 0 in 300.
- **Fix.** `dbt/macros/drop_view.sql` makes every view drop take one
  advisory lock in the same statement. The drops then run one at a
  time. No model and no output changes.
  `test_view_drops_take_the_cascade_lock` pins the lock on the SQL
  that dbt sends (red before the macro existed). The pin adds one
  integration test, so the running domain now has 10 tests.
- **Reach.** `make dbt-build` on the real database sends the same
  statements, so the fix also covers the daily build.

---

## Claude Code kickoff prompt (Phases 0–2)

Start in plan mode. Do not edit files until I approve the plan.

Read `CLAUDE.md` and `docs/test-speed-plan.md` (committed before this
session). The plan is the spec. If the repo disagrees with it, stop
and tell me.

**Chunk 1 — Phase 0.** Run the measurements. Fill in the
"Measurements" table and commit. Tell me whether the numbers change the
order of Phases 3 and 4. Stop and wait for my go-ahead.

**Chunk 2 — Phase 1.** Markers, Make targets, the marker guard test
(red first), and the CLAUDE.md testing section. Full suite, lint,
commit.

**Chunk 3 — Phase 2.** xdist, per-worker databases, per-worker dbt
target and log paths, `make test` and `make test-serial`. Prove the
pass/fail lists match between serial and parallel runs. Full suite,
lint, commit.

Files you may touch: `pyproject.toml`, `Makefile`, `CLAUDE.md`,
`docs/test-speed-plan.md`, `tests/conftest.py`,
`tests/test_dbt_models.py`, `tests/test_app.py` (markers and the
database-name plumbing only), and one new test file for the marker
guard. Do not change any assertion, fixture value, or dbt model. If you
find a problem outside these files, report it. Do not fix it.

Use the feature branch `test-speed`. Merge to main only after the full
suite passes in both serial and parallel mode.

End-of-session report: the measurements, commits made, the red-first
proof for the guard test, the serial-vs-parallel comparison, the new
full-suite time, and skip counts.
