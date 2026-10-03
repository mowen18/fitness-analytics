# Phase 0–3 targets. App targets arrive with their phase.

VENV := .venv/bin

# dbt runs from dbt/ (decision D4) with a local profile auto-copied from
# the committed example; .env supplies connection values via env_var().
DBT = cd dbt && set -a && . ../.env && set +a && ../$(VENV)/dbt

# pytest-xdist workers for the parallel test targets. Each worker has
# its own scratch database and dbt target folder. A dbt build already
# runs 4 threads, so 4 workers means up to 16 queries at once — raise
# this only after a trial run shows spare capacity; never `auto`.
# worksteal lets an idle worker take waiting tests from a busy one.
# The default scheduling hands out consecutive tests in chunks, which
# put the slow stream tests on one worker (full suite on 4 workers:
# 135 s default, 76 s worksteal).
PYTEST_WORKERS ?= 4
PYTEST_PARALLEL = -n $(PYTEST_WORKERS) --dist worksteal

# ── Airflow (v1.5): thin layer, own venv — never in project deps ──────
AIRFLOW_VENV     := $(HOME)/.venvs/airflow
AIRFLOW_HOME_DIR := $(HOME)/airflow
AIRFLOW_PYTHON   ?= python3.13
AIRFLOW_VERSION  ?=

# Every airflow-* recipe that runs the airflow binary MUST be prefixed
# with $(AIRFLOW_ENV): standalone respawns components as bare `airflow`
# via PATH, and bare CLI calls without LOAD_EXAMPLES=False pollute the
# metadata DB with example DAGs. (airflow-install is exempt — it only
# creates the venv and runs pip.)
AIRFLOW_ENV := PATH=$(AIRFLOW_VENV)/bin:$$PATH \
	AIRFLOW_HOME=$(AIRFLOW_HOME_DIR) \
	AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/orchestration/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False

.PHONY: help up down bootstrap athlete authorize sync-activities reconcile \
	backfill-coordinates sync-weather reconcile-weather sync-streams \
	sync-segment-efforts \
	dbt-profile dbt-build dbt-test dbt-freshness dbt-docs dbt-dag \
	dbt-manifest app all \
	test test-serial test-app test-fast test-running test-cycling \
	test-app-render \
	lint format airflow-install airflow-start

help:
	@grep -E '^[a-z-]+:' Makefile | sed 's/:.*//' | sort

up:            ## start Postgres (healthcheck-gated)
	docker compose up -d --wait

down:          ## stop Postgres (data volume preserved)
	docker compose down

bootstrap:     ## (re-)apply every sql/*.sql in order — idempotent
	for f in sql/*.sql; do \
		docker compose exec postgres psql -U running_user -d running_analytics_db \
			-v ON_ERROR_STOP=1 -f "/docker-entrypoint-initdb.d/$$(basename $$f)" \
			|| exit 1; \
	done

athlete:       ## print the authenticated athlete profile
	$(VENV)/running-pipeline athlete

authorize:     ## one-time Strava browser authorization
	$(VENV)/running-pipeline authorize

sync-activities:   ## incremental Strava activity sync (14-day overlap window)
	$(VENV)/running-pipeline sync-activities

reconcile:     ## full reconciliation: re-fetch everything from SYNC_START_DATE
	$(VENV)/running-pipeline sync-activities --full

backfill-coordinates:  ## resolve activity-start coordinates (payload, else detail polyline)
	$(VENV)/running-pipeline backfill-coordinates

sync-weather:  ## fetch hourly weather for outdoor runs and rides not yet covered
	$(VENV)/running-pipeline sync-weather

reconcile-weather:  ## re-fetch weather even for already-cached hours
	$(VENV)/running-pipeline sync-weather --full

sync-streams:  ## backfill activity streams for fetch-eligible runs (resumable)
	$(VENV)/running-pipeline sync-streams

sync-segment-efforts:  ## backfill segment efforts for D23 rides (resumable)
	$(VENV)/running-pipeline sync-segment-efforts

dbt-profile:   ## create dbt/profiles.yml from the example if absent
	@test -f dbt/profiles.yml || cp dbt/profiles.yml.example dbt/profiles.yml

dbt-build: dbt-profile     ## build all dbt models and run their tests
	$(DBT) build --profiles-dir .

dbt-test: dbt-profile      ## run dbt tests only
	$(DBT) test --profiles-dir .

dbt-freshness: dbt-profile ## check source freshness (raw fetched_at ages)
	$(DBT) source freshness --profiles-dir .

dbt-docs: dbt-profile      ## generate + serve dbt docs locally
	$(DBT) docs generate --profiles-dir . && $(DBT) docs serve --profiles-dir .

dbt-dag: dbt-profile       ## regenerate manifest and embed the dbt DAG in README.md
	$(DBT) parse --profiles-dir .
	$(VENV)/python -m running_pipeline.dbt_dag --update-readme

app:           ## launch the Streamlit dashboard (five views, marts only)
	$(VENV)/streamlit run app/streamlit_app.py

airflow-install:   ## create ~/.venvs/airflow + apache-airflow (official constraints)
	@command -v $(AIRFLOW_PYTHON) >/dev/null 2>&1 || { \
		echo "error: $(AIRFLOW_PYTHON) not found on PATH;" \
		     "override with: make airflow-install AIRFLOW_PYTHON=python3.x"; \
		exit 1; }
	test -d $(AIRFLOW_VENV) || $(AIRFLOW_PYTHON) -m venv $(AIRFLOW_VENV)
	PYVER=$$($(AIRFLOW_VENV)/bin/python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")') && \
	$(AIRFLOW_VENV)/bin/pip install --upgrade pip && \
	$(AIRFLOW_VENV)/bin/pip install \
		"apache-airflow$(if $(AIRFLOW_VERSION),==$(AIRFLOW_VERSION))" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(if $(AIRFLOW_VERSION),$(AIRFLOW_VERSION),latest)/constraints-$$PYVER.txt"

airflow-start:     ## airflow standalone (AIRFLOW_HOME=~/airflow, DAGs from orchestration/dags)
	$(AIRFLOW_ENV) $(AIRFLOW_VENV)/bin/airflow standalone

all: sync-activities backfill-coordinates sync-weather sync-streams sync-segment-efforts dbt-build  ## full refresh: all syncs + dbt

# Test builds write to dbt/target/<worker>, so nothing in a test run
# refreshes dbt/target/manifest.json — the file the layering guard
# (tests/test_dbt_layering.py) reads. Every test target parses first.
dbt-manifest: dbt-profile  ## refresh dbt/target/manifest.json for the layering guard
	$(DBT) parse --profiles-dir .

test: dbt-manifest         ## full suite on PYTEST_WORKERS parallel workers
	$(VENV)/pytest $(PYTEST_PARALLEL)

test-serial: dbt-manifest  ## full suite in one process — for debugging
	$(VENV)/pytest

test-app: dbt-manifest  ## unit + app tests — valid only for diffs confined to app/
	$(VENV)/pytest -m "not integration" -q
	$(VENV)/pytest tests/test_app.py -q

test-fast: dbt-manifest  ## no-database tier — src/ changes that do not touch SQL
	$(VENV)/pytest -m "not integration" -q

test-running: dbt-manifest  ## fast tier + the running dbt integration tests
	$(VENV)/pytest $(PYTEST_PARALLEL) -m "not integration or dbt_running" -q

test-cycling: dbt-manifest  ## fast tier + the cycling dbt integration tests
	$(VENV)/pytest $(PYTEST_PARALLEL) -m "not integration or dbt_cycling" -q

test-app-render: dbt-manifest  ## fast tier + the Streamlit render tests
	$(VENV)/pytest $(PYTEST_PARALLEL) -m "not integration or app" -q

lint:
	$(VENV)/ruff check src tests

format:
	$(VENV)/ruff format src tests
