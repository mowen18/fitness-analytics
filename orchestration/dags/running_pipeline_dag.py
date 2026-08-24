"""Daily pipeline run — thin scheduling over the existing pipeline (v1.5).

Mirrors `make all` as a linear chain. Policy and constraints live in
docs/decisions/v1.5-airflow-addendum.md: Airflow owns no state, no
templated date windows (the 14-day overlap owns incremental
correctness), and no pipeline logic here — each task runs the same
command an operator would.
"""

from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.utils.trigger_rule import TriggerRule

SCHEDULE_CRON = "0 6 * * *"
SCHEDULE_TZ = "America/Chicago"

# The CLI exits 3 on a clean rate-limit/budget stop: committed work is
# kept and every sync resumes next run, so the task is SKIPPED, not
# failed. GNU make would mask this code (any recipe failure -> make
# exits 2), so sync tasks call the CLI directly — the verbatim one-line
# body of the corresponding make recipe (see the addendum's
# exit-code-3 policy).
RATE_LIMIT_STOP_EXIT = 3

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI = ".venv/bin/running-pipeline"

with DAG(
    dag_id="running_pipeline",
    schedule=SCHEDULE_CRON,
    start_date=pendulum.datetime(2026, 1, 1, tz=SCHEDULE_TZ),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    sync_activities = BashOperator(
        task_id="sync_activities",
        bash_command=f"{CLI} sync-activities",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=RATE_LIMIT_STOP_EXIT,
    )

    backfill_coordinates = BashOperator(
        task_id="backfill_coordinates",
        bash_command=f"{CLI} backfill-coordinates",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=RATE_LIMIT_STOP_EXIT,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    sync_weather = BashOperator(
        task_id="sync_weather",
        bash_command=f"{CLI} sync-weather",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=RATE_LIMIT_STOP_EXIT,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    sync_streams = BashOperator(
        task_id="sync_streams",
        bash_command=f"{CLI} sync-streams",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=RATE_LIMIT_STOP_EXIT,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    sync_segment_efforts = BashOperator(
        task_id="sync_segment_efforts",
        bash_command=f"{CLI} sync-segment-efforts",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=RATE_LIMIT_STOP_EXIT,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="make dbt-build",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=None,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    (
        sync_activities
        >> backfill_coordinates
        >> sync_weather
        >> sync_streams
        >> sync_segment_efforts
        >> dbt_build
    )
