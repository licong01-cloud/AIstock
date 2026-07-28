from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values
import psycopg2
import pytest

from backend.services.advisory_historical_range.query_repository import PostgresHistoricalRangeQueryRepository


_DEV_DB_KEYS = (
    "TDX_DB_DEV_HOST",
    "TDX_DB_DEV_PORT",
    "TDX_DB_DEV_NAME",
    "TDX_DB_DEV_USER",
    "TDX_DB_DEV_PASSWORD",
)
_DEFAULT_ENV_FILE = Path("F:/Dev/AIstock/.env")


def _standard_dev_env_values() -> dict[str, str]:
    env_file = Path(os.environ.get("AISTOCK_ENV_FILE") or _DEFAULT_ENV_FILE)
    file_values = dotenv_values(env_file, interpolate=False) if env_file.is_file() else {}
    return {
        key: str(os.environ.get(key) or file_values.get(key) or "").strip()
        for key in _DEV_DB_KEYS
    }


def _validated_dev_connect_kwargs(values: Mapping[str, str]) -> dict[str, object]:
    missing = sorted(key for key in _DEV_DB_KEYS if not str(values.get(key) or "").strip())
    if missing:
        raise ValueError(f"R5 DEV PostgreSQL target lacks standard env keys: {missing}")
    host = str(values["TDX_DB_DEV_HOST"])
    port = int(str(values["TDX_DB_DEV_PORT"]))
    dbname = str(values["TDX_DB_DEV_NAME"])
    user = str(values["TDX_DB_DEV_USER"])
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError(f"R5 DEV PostgreSQL host must be loopback, got {host!r}")
    if port != 5433:
        raise ValueError(f"R5 DEV PostgreSQL port must be 5433, got {port}")
    if "dev" not in dbname.lower():
        raise ValueError(f"R5 DEV PostgreSQL database must contain 'dev', got {dbname!r}")
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": values["TDX_DB_DEV_PASSWORD"],
    }


def test_r5_dev_target_uses_standard_aistock_database_keys() -> None:
    values = {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5433",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_DEV_USER": "tester",
        "TDX_DB_DEV_PASSWORD": "secret",
    }
    assert _validated_dev_connect_kwargs(values) == {
        "host": "127.0.0.1",
        "port": 5433,
        "dbname": "aistock_dev",
        "user": "tester",
        "password": "secret",
    }


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("TDX_DB_DEV_HOST", "192.168.50.215", "loopback"),
        ("TDX_DB_DEV_PORT", "5432", "port must be 5433"),
        ("TDX_DB_DEV_NAME", "aistock", "must contain 'dev'"),
    ],
)
def test_r5_dev_target_rejects_non_dev_database_identity(field: str, value: str, error: str) -> None:
    values = {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5433",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_DEV_USER": "tester",
        "TDX_DB_DEV_PASSWORD": "secret",
    }
    values[field] = value
    with pytest.raises(ValueError, match=error):
        _validated_dev_connect_kwargs(values)


_DEV_ENV_VALUES = _standard_dev_env_values()
_MISSING_DEV_DB_KEYS = tuple(key for key in _DEV_DB_KEYS if not _DEV_ENV_VALUES[key])


@pytest.mark.skipif(
    bool(_MISSING_DEV_DB_KEYS),
    reason=f"R5 DEV PostgreSQL read E2E requires standard keys: {_MISSING_DEV_DB_KEYS}",
)
def test_real_dev_postgres_batch_and_operation_projection() -> None:
    connect_kwargs = _validated_dev_connect_kwargs(_DEV_ENV_VALUES)

    def connect():
        return psycopg2.connect(
            **connect_kwargs,
            connect_timeout=5,
            application_name="advisory_phase1r_r5_read_e2e",
            options="-c default_transaction_read_only=on",
        )

    repository = PostgresHistoricalRangeQueryRepository(conn_factory=connect)
    page = repository.list_batches(limit=5)
    assert page["page"]["limit"] == 5
    assert isinstance(page["items"], list)
    if page["items"]:
        batch = repository.get_batch(str(page["items"][0]["batch_id"]))
        operations = repository.list_operations(batch_id=str(batch["batch_id"]), limit=5)
        assert isinstance(operations["items"], list)
