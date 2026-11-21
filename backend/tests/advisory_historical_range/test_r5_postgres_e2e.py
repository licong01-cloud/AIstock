from __future__ import annotations

import os
import asyncio
from contextlib import contextmanager
from pathlib import Path
from typing import Mapping
from uuid import uuid4

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


@pytest.fixture
def waiting_cancel_dev_case(tmp_path):
    """Exercise an explicitly supplied DEV case with all SQL rolled back.

    No case is selected from another developer's queue. Frozen artifacts remain
    real; only commit ownership is deferred to the outer test transaction.
    """
    batch_id = os.environ.get('AISTOCK_ADVISORY_CANCEL_DEV_BATCH_ID')
    artifact_root = os.environ.get('AISTOCK_ADVISORY_CANCEL_DEV_ARTIFACT_ROOT')
    if not batch_id or not artifact_root or _MISSING_DEV_DB_KEYS:
        pytest.skip('explicit owned DEV waiting batch and artifact root required')
    from backend.services.advisory_historical_range.composition import (
        build_explicit_historical_range_r5_runtime_factory,
        build_historical_range_r5_application_service,
    )
    settings = dotenv_values(_DEFAULT_ENV_FILE, interpolate=False)
    conn = psycopg2.connect(**_validated_dev_connect_kwargs(_DEV_ENV_VALUES), connect_timeout=5)
    conn.set_session(isolation_level='REPEATABLE READ', autocommit=False)

    class OuterTransaction:
        autocommit = None

        def __getattr__(self, name):
            return getattr(conn, name)

        def commit(self):
            pass

        def rollback(self):
            pass

        def set_session(self, **kwargs):
            # Native query helpers open/close independent read transactions in
            # production. Here real SQL shares the outer repeatable-read DEV
            # fixture transaction so it can observe its uncommitted test rows.
            # Read-only session enforcement is not the subject of this test.
            pass

    @contextmanager
    def factory():
        yield OuterTransaction()

    try:
        with conn.cursor() as cur:
            cur.execute('SELECT current_database()')
            assert 'dev' in cur.fetchone()[0].lower()
            cur.execute('SELECT status,waiting_stage,row_version FROM app.advisory_historical_range_batch '
                        'WHERE batch_id=%s FOR UPDATE NOWAIT', (batch_id,))
            before = cur.fetchone()
            if not before or before[:2] != ('WAITING_INPUT', 'DAY_INPUT'):
                pytest.skip('provided DEV case must be waiting on a real execution input')
            cur.execute("SELECT day.day_run_id,day.day_receipt_hash FROM app.advisory_historical_range_day_run day "
                        "JOIN app.advisory_historical_range_run run USING(range_run_id) "
                        "WHERE run.batch_id=%s AND day.status IN ('COMPLETE','VALID_NO_CANDIDATE') "
                        "ORDER BY day.day_run_id", (batch_id,))
            completed = cur.fetchall()
        runtime_root = tmp_path / 'runtime'
        runtime_root.mkdir()
        runtime_factory = build_explicit_historical_range_r5_runtime_factory(
            conn_factory=factory, artifact_root=Path(artifact_root), task_runtime_root=runtime_root,
            package_asset_root=Path(settings['AISTOCK_PACKAGE_ASSET_STORE_ROOT']),
            repository_root=Path(__file__).resolve().parents[3],
            policy_component_root=tmp_path / 'policies', calculation_evidence_root=tmp_path / 'calculation',
            dataset_store_root=tmp_path / 'datasets',
        )
        service = build_historical_range_r5_application_service(
            query_runtime_factory=runtime_factory, mutation_runtime_factory=runtime_factory,
        )
        yield service, conn, batch_id, before, completed
    finally:
        conn.rollback()
        conn.close()


def test_real_dev_cancel_waiting_batch_clears_waiting_stage_and_preserves_completed_days(waiting_cancel_dev_case):
    from fastapi import BackgroundTasks
    from backend.services.advisory_historical_range.api_models import HistoricalRangeCommandRequest

    service, conn, batch_id, before, completed = waiting_cancel_dev_case
    background = BackgroundTasks()
    service.cancel_batch(batch_id, HistoricalRangeCommandRequest(
        operation_idempotency_key='dev-cancel-regression-' + uuid4().hex,
        expected_row_version=before[2],
    ), background_tasks=background)
    asyncio.run(background())
    with conn.cursor() as cur:
        cur.execute('SELECT status,waiting_stage FROM app.advisory_historical_range_batch WHERE batch_id=%s', (batch_id,))
        assert cur.fetchone() == ('CANCELLED', None)
        cur.execute("SELECT day.day_run_id,day.day_receipt_hash FROM app.advisory_historical_range_day_run day "
                    "JOIN app.advisory_historical_range_run run USING(range_run_id) "
                    "WHERE run.batch_id=%s AND day.status IN ('COMPLETE','VALID_NO_CANDIDATE') "
                    "ORDER BY day.day_run_id", (batch_id,))
        assert cur.fetchall() == completed
        cur.execute("SELECT count(*) FROM app.advisory_historical_range_day_run day "
                    "JOIN app.advisory_historical_range_run run USING(range_run_id) "
                    "WHERE run.batch_id=%s AND day.status NOT IN ('COMPLETE','VALID_NO_CANDIDATE','CANCELLED','FAILED')",
                    (batch_id,))
        assert cur.fetchone()[0] == 0


def test_real_dev_cancel_stale_batch_version_keeps_waiting_state(waiting_cancel_dev_case):
    from backend.services.advisory_historical_range.models import HistoricalRangeContractError
    from backend.services.advisory_historical_range.repository import REASON_ROW_VERSION_CONFLICT

    service, conn, batch_id, before, _ = waiting_cancel_dev_case
    repository = service._mutation_runtime_factory().repository
    with pytest.raises(HistoricalRangeContractError) as caught:
        repository.cancel_execution_batch(batch_id=batch_id, expected_batch_row_version=before[2] - 1,
                                          attempts={})
    assert caught.value.reason_code == REASON_ROW_VERSION_CONFLICT
    with conn.cursor() as cur:
        cur.execute('SELECT status,waiting_stage,row_version FROM app.advisory_historical_range_batch WHERE batch_id=%s',
                    (batch_id,))
        assert cur.fetchone() == before
