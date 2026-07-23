from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest

from backend.services.advisory_historical_range.composition import (
    build_historical_range_batch_execution_service,
)
from backend.services.advisory_historical_range.repository import PostgresHistoricalRangeRepository
from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore


_REQUIRED = (
    "AISTOCK_PHASE1R_R3_DEV_DSN",
    "AISTOCK_PHASE1R_R3_DEV_BATCH_ID",
    "AISTOCK_PHASE1R_R3_DEV_ARTIFACT_ROOT",
    "AISTOCK_PHASE1R_R3_DEV_RUNTIME_ROOT",
    "AISTOCK_PHASE1R_R3_DEV_PACKAGE_ROOT",
    "AISTOCK_PHASE1R_R3_DEV_REPOSITORY_ROOT",
)


pytestmark = pytest.mark.skipif(
    any(not os.environ.get(name) for name in _REQUIRED),
    reason="explicit R3 DEV DSN, batch, and roots are required; no settings are guessed",
)


def _connection_factory():
    return psycopg2.connect(os.environ["AISTOCK_PHASE1R_R3_DEV_DSN"])


def test_formal_service_resumes_explicit_dev_batch_until_durable_boundary() -> None:
    batch_id = os.environ["AISTOCK_PHASE1R_R3_DEV_BATCH_ID"]
    artifact_root = Path(os.environ["AISTOCK_PHASE1R_R3_DEV_ARTIFACT_ROOT"])
    repository = PostgresHistoricalRangeRepository(
        conn_factory=_connection_factory,
        artifact_store=HistoricalRangeArtifactStore(root=artifact_root),
    )
    batch = repository.load_execution_batch(batch_id=batch_id)
    service = build_historical_range_batch_execution_service(
        conn_factory=_connection_factory,
        artifact_root=artifact_root,
        task_runtime_root=Path(os.environ["AISTOCK_PHASE1R_R3_DEV_RUNTIME_ROOT"]),
        package_asset_root=Path(os.environ["AISTOCK_PHASE1R_R3_DEV_PACKAGE_ROOT"]),
        repository_root=Path(os.environ["AISTOCK_PHASE1R_R3_DEV_REPOSITORY_ROOT"]),
        hmm_snapshot_provider=None,
    )
    result = service.resume_until_blocked(
        batch_id=batch_id,
        worker_id="pytest-r3-dev-e2e",
        operation_idempotency_key=f"pytest-r3-dev-e2e-{batch.row_version}",
        expected_batch_row_version=batch.row_version,
    )
    assert result.batch_id == batch_id
    assert result.executed_day_count >= result.successful_day_count
    assert not any(day_run_id.startswith("no-claim:") for day_run_id in result.blocking_day_run_ids)
