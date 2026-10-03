"""Shared fixtures.

Unit tests stay hermetic (never read the developer's .env — see
make_settings in test modules). Integration tests are the exception:
they intentionally load .env because they need real local-DB
credentials, and they skip with an actionable reason when Postgres is
not running. They work in a scratch database (one per pytest-xdist
worker, dropped and recreated per session) so the real ingested data
is never touched.
"""

import os
from pathlib import Path

import psycopg
import pytest
from pydantic import ValidationError

from running_pipeline.config import Settings

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def xdist_worker() -> str:
    """This process's pytest-xdist worker id (gw0, gw1, ...); `master`
    in a serial run — the same value as xdist's worker_id fixture."""
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


def scratch_db_name() -> str:
    """The scratch database of this worker. The integration_db fixture,
    run_dbt, and the app render helper all call this, so they cannot
    point at different databases."""
    return f"running_analytics_test_{xdist_worker()}"


@pytest.fixture(scope="session")
def sql_dir() -> Path:
    return SQL_DIR


@pytest.fixture(scope="session")
def integration_db():
    """Connection to a scratch database with every sql/*.sql applied."""
    try:
        settings = Settings()
    except ValidationError:
        pytest.skip("integration: .env not configured (see .env.example)")

    server = {
        "host": settings.postgres_host,
        "port": settings.postgres_port,
        "user": settings.postgres_user,
        "password": settings.postgres_password.get_secret_value(),
        "connect_timeout": 3,
    }
    try:
        admin = psycopg.connect(dbname=settings.postgres_db, autocommit=True, **server)
    except psycopg.OperationalError:
        pytest.skip(
            "integration: Postgres not reachable on "
            f"{settings.postgres_host}:{settings.postgres_port} — start it with `make up`"
        )

    test_db = scratch_db_name()
    with admin:
        admin.execute(f'DROP DATABASE IF EXISTS "{test_db}" WITH (FORCE)')
        admin.execute(f'CREATE DATABASE "{test_db}"')

    conn = psycopg.connect(dbname=test_db, **server)
    for ddl_file in sorted(SQL_DIR.glob("*.sql")):
        conn.execute(ddl_file.read_text())
    conn.commit()

    try:
        yield conn
    finally:
        conn.close()
        with psycopg.connect(dbname=settings.postgres_db, autocommit=True, **server) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{test_db}" WITH (FORCE)')
