"""Real dev PostgreSQL fixtures for HMM evolution repository acceptance.

The provider refuses production coordinates and is opt-in.  Tests write only
rows carrying their own unique identifiers in the isolated hmm_evolution
schema, then remove exactly those rows.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg2
import pytest

from backend.db.init_hmm_evolution_schema import verify_schema

ENV_FILE = Path("F:/Dev/AIstock/.env")


def _parse_env() -> dict[str, str]:
    if not ENV_FILE.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _dev_db_credentials() -> dict[str, Any] | None:
    values = _parse_env()
    required = (
        "TDX_DB_DEV_HOST",
        "TDX_DB_DEV_PORT",
        "TDX_DB_DEV_NAME",
        "TDX_DB_DEV_USER",
        "TDX_DB_DEV_PASSWORD",
    )
    if not all(values.get(key) for key in required):
        return None
    credentials: dict[str, Any] = {
        "host": values["TDX_DB_DEV_HOST"],
        "port": int(values["TDX_DB_DEV_PORT"]),
        "dbname": values["TDX_DB_DEV_NAME"],
        "user": values["TDX_DB_DEV_USER"],
        "password": values["TDX_DB_DEV_PASSWORD"],
        "connect_timeout": 5,
    }
    if credentials["port"] != 5433 or "dev" not in credentials["dbname"].lower():
        return None
    return credentials


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: requires explicitly enabled real dev PostgreSQL",
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    try:
        parser.addoption(
            "--run-integration",
            action="store_true",
            default=False,
            help="run explicitly gated integration tests",
        )
    except ValueError:
        pass


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    if config.getoption("--run-integration"):
        return
    marker = pytest.mark.skip(reason="use --run-integration for real dev PostgreSQL")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(marker)


@pytest.fixture(scope="session")
def hmm_evolution_dev_db_credentials() -> dict[str, Any]:
    if os.environ.get("AISTOCK_HMM_EVOLUTION_DEV_INTEGRATION") != "1":
        pytest.skip("AISTOCK_HMM_EVOLUTION_DEV_INTEGRATION=1 is required")
    credentials = _dev_db_credentials()
    if credentials is None:
        pytest.skip("safe TDX_DB_DEV_* credentials for port 5433 are unavailable")
    return credentials


@pytest.fixture(scope="session")
def hmm_evolution_dev_conn_factory(
    hmm_evolution_dev_db_credentials: dict[str, Any],
):
    @contextmanager
    def provider() -> Iterator[Any]:
        connection = psycopg2.connect(**hmm_evolution_dev_db_credentials)
        connection.autocommit = False
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    with provider() as connection:
        verify_schema(connection)
    return provider
