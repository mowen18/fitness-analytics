"""Parity guard: the DAG's bash_commands must equal the Makefile
recipes they mirror (v1.5 addendum, exit-code-3 policy).

The constraint-(c) amendment lets four DAG tasks call the CLI directly
because GNU make masks recipe exit codes — the price is a duplicated
command line per task. This test re-derives both sides as text and
fails on any drift. It must never import airflow or the DAG module:
the project venv has no apache-airflow (constraint (e)) and must stay
that way, so the DAG file is read with ast.parse, which never executes
it.
"""

import ast
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
MAKEFILE_PATH = REPO_ROOT / "Makefile"
DAG_PATH = REPO_ROOT / "orchestration" / "dags" / "running_pipeline_dag.py"

# task_id -> the make target whose one-line recipe body the task duplicates
SYNC_TASKS = {
    "sync_activities": "sync-activities",
    "backfill_coordinates": "backfill-coordinates",
    "sync_weather": "sync-weather",
    "sync_streams": "sync-streams",
    "sync_segment_efforts": "sync-segment-efforts",
}


def makefile_recipe(target: str) -> str:
    text = MAKEFILE_PATH.read_text()
    venv = re.search(r"^VENV\s*:=\s*(\S+)", text, re.MULTILINE).group(1)
    match = re.search(rf"^{re.escape(target)}:[^\n]*\n\t(.+)$", text, re.MULTILINE)
    assert match, f"Makefile has no single-line recipe for target {target!r}"
    return match.group(1).strip().replace("$(VENV)", venv)


def _resolve_string(node: ast.expr, constants: dict[str, str]) -> str:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Name):
                parts.append(constants[value.value.id])
            else:
                raise AssertionError("bash_command f-strings may use module constants only")
        return "".join(parts)
    raise AssertionError(f"bash_command is not a literal or f-string: {ast.dump(node)}")


def dag_bash_commands() -> dict[str, str]:
    tree = ast.parse(DAG_PATH.read_text())
    constants = {
        node.targets[0].id: node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }
    commands = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "BashOperator"):
            continue
        keywords = {kw.arg: kw.value for kw in node.keywords}
        commands[keywords["task_id"].value] = _resolve_string(keywords["bash_command"], constants)
    return commands


@pytest.mark.parametrize(("task_id", "target"), sorted(SYNC_TASKS.items()))
def test_sync_task_matches_make_recipe(task_id, target):
    assert dag_bash_commands()[task_id] == makefile_recipe(target)


def test_dbt_build_stays_behind_make():
    # The other half of the policy: the one recipe with real logic (and
    # no exit-3 contract) is never duplicated into the DAG.
    assert dag_bash_commands()["dbt_build"] == "make dbt-build"
