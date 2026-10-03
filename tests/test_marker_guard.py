"""Marker guard: every @integration test in the dbt and app test
modules carries exactly one domain marker (docs/test-speed-plan.md,
Phase 1).

The domain tiers (`make test-running`, `make test-cycling`,
`make test-app-render`) select tests by marker. An integration test
with no domain marker would silently drop out of every tier; one with
two would run in two. The marks are read from the imported modules,
never from the collected session: `-m` deselection empties the
session's item list in exactly the tiers where this guard must still
see every test.
"""

import inspect

import test_app
import test_dbt_models

DOMAIN_MARKERS = frozenset({"dbt_running", "dbt_cycling", "app"})
GUARDED_MODULES = (test_dbt_models, test_app)


def _mark_names(holder) -> list[str]:
    marks = getattr(holder, "pytestmark", [])
    if not isinstance(marks, list | tuple):
        marks = [marks]
    return [mark.name for mark in marks]


def integration_tests(module) -> dict[str, list[str]]:
    """Each @integration test in `module` -> the domain markers it carries."""
    module_marks = _mark_names(module)
    tests = {}
    for name, function in inspect.getmembers(module, inspect.isfunction):
        if not name.startswith("test_") or function.__module__ != module.__name__:
            continue  # helpers, and anything imported from another module
        marks = module_marks + _mark_names(function)
        if "integration" in marks:
            tests[name] = [mark for mark in marks if mark in DOMAIN_MARKERS]
    return tests


def test_every_integration_test_carries_exactly_one_domain_marker():
    offenders = {}
    for module in GUARDED_MODULES:
        tests = integration_tests(module)
        # A guard that finds nothing to check must not pass.
        assert tests, f"{module.__name__}: no @integration tests found"
        for name, domains in tests.items():
            if len(domains) != 1:
                offenders[f"{module.__name__}::{name}"] = domains
    assert offenders == {}, (
        f"integration tests without exactly one of {sorted(DOMAIN_MARKERS)}:\n"
        + "\n".join(f"  {test}: {domains}" for test, domains in sorted(offenders.items()))
    )
