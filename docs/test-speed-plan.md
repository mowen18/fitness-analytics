# Test suite speed — plan

Status: Draft 2026-10-03, from a code reading of `main` (2026-08-27).
Not yet measured. Phase 0 checks the numbers before any change.

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

(Fill in after Phase 0.)

| Measure | Time |
|---------|------|
| Full suite, serial | |
| Non-integration tests only | |
| `dbt --version` | |
| `dbt parse` | |
| One `dbt build`, 4 threads | |
| One `dbt build`, 8 threads | |
| Slowest 10 tests (from `--durations`) | |

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
