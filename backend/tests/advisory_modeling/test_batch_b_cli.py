from __future__ import annotations

from datetime import UTC, date, datetime
import json
import os
from pathlib import Path

import pytest

from backend.services.advisory_historical_range.api_models import (
    ExistingProgramInput,
    HistoricalRangeCreateRequest,
)
from backend.services.advisory_historical_range.models import (
    ExistingProgramSpecV1,
    HistoricalRangeContractError,
    HistoricalRangeResearchBatchRequestV1,
    REASON_IDEMPOTENCY_CONFLICT,
)
from backend.services.advisory_modeling.errors import AdvisoryModelingError
from scripts.advisory_short_rebound_batch_b import (
    REQUIRED_RUNTIME_KEYS,
    RestartSafeHistoricalRangeService,
    _database_config,
    _existing_directory,
    _load_environment,
    build_parser,
)
from backend.services.advisory_modeling.batch_b import (
    BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM,
)


def test_batch_b_cli_requires_every_explicit_root_and_env_source(tmp_path: Path) -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])

    relative = Path("relative-root")
    with pytest.raises(ValueError, match="explicit absolute"):
        _existing_directory(relative, field_name="artifact_root")

    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_HOST=localhost\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required Batch B configuration"):
        _load_environment(env_file)


def test_database_config_uses_only_loaded_env_values_without_logging_secrets() -> None:
    values = {
        "TDX_DB_HOST": "db.example",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
        "TDX_DB_USER": "research",
        "TDX_DB_PASSWORD": "secret",
    }

    config = _database_config(values)

    assert config == {
        "host": "db.example",
        "port": 5432,
        "dbname": "aistock",
        "user": "research",
        "password": "secret",
    }
    assert "secret" not in json.dumps({"keys": sorted(config)})


def test_batch_b_uses_the_frozen_bounded_candidate_prefetch_width() -> None:
    assert BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM == 8


def test_materialization_environment_loads_repository_root_from_explicit_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "TDX_DB_HOST": "localhost",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
        "TDX_DB_USER": "research",
        "TDX_DB_PASSWORD": "secret",
        **{key: f"F:/configured/{key.lower()}" for key in REQUIRED_RUNTIME_KEYS},
    }
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    for key in values:
        monkeypatch.delenv(key, raising=False)

    loaded, _ = _load_environment(env_file)

    assert loaded["AISTOCK_REPOSITORY_ROOT"] == values["AISTOCK_REPOSITORY_ROOT"]
    assert os.environ["AISTOCK_REPOSITORY_ROOT"] == values["AISTOCK_REPOSITORY_ROOT"]


def _create_request(*, program_version: int = 7) -> HistoricalRangeCreateRequest:
    return HistoricalRangeCreateRequest(
        program_specs=[
            ExistingProgramInput(
                source_kind="EXISTING_PROGRAM",
                program_id="advp_short_rebound",
                expected_program_version=program_version,
                expected_binding_version_id="advbind_short_rebound_v7",
            )
        ],
        start_trade_date=date(2019, 1, 2),
        end_trade_date=date(2026, 7, 3),
    )


def _stored_request(*, idempotency_key: str) -> HistoricalRangeResearchBatchRequestV1:
    return HistoricalRangeResearchBatchRequestV1(
        request_id="ahrq_original",
        client_idempotency_key=idempotency_key,
        program_specs=(
            ExistingProgramSpecV1(
                program_id="advp_short_rebound",
                expected_program_version=7,
                expected_binding_version_id="advbind_short_rebound_v7",
            ),
        ),
        start_trade_date=date(2019, 1, 2),
        end_trade_date=date(2026, 7, 3),
        requested_at=datetime(2026, 8, 3, tzinfo=UTC),
        requested_by="advisory-modeling-batch-b",
    )


class _ConflictingService:
    def __init__(self, *, active_lease: bool = False) -> None:
        self.batch_id = "ahrb_existing"
        self.active_lease = active_lease
        self.resume_calls = []

    def with_candidate_prefetch_per_program(self, _value: int):
        return self

    def create_batch(self, *_args, **_kwargs):
        raise HistoricalRangeContractError(
            REASON_IDEMPOTENCY_CONFLICT,
            "same client idempotency key resolved to different planning semantics",
            context={"existing_batch_id": self.batch_id},
        )

    def get_batch(self, _batch_id: str):
        key = "batch-b-exact-key"
        stored = _stored_request(idempotency_key=key)
        return {
            "batch_id": self.batch_id,
            "request_id": stored.request_id,
            "client_idempotency_key": key,
            "user_request_semantic_hash": stored.user_request_semantic_hash,
            "start_trade_date": stored.start_trade_date,
            "end_trade_date": stored.end_trade_date,
            "status": "PLANNING",
            "row_version": 31,
            "catalog_operation_id": "ahrop_catalog",
            "catalog_operation_status": "RUNNING",
            "catalog_lease_expired": not self.active_lease,
            "catalog_lease_expires_at": "2026-08-03T12:30:00+00:00",
            "request_payload_json": {"request": stored.model_dump(mode="json")},
        }

    def resume_batch(self, batch_id, request, *, background_tasks):
        self.resume_calls.append((batch_id, request, background_tasks))
        return {"ok": True, "data": {"batch": self.get_batch(batch_id)}}


def test_restart_safe_service_recovers_only_the_exact_existing_batch() -> None:
    delegate = _ConflictingService()
    service = RestartSafeHistoricalRangeService(service=delegate)
    tasks = object()

    result = service.create_batch(
        _create_request(),
        idempotency_key="batch-b-exact-key",
        background_tasks=tasks,
        requested_by="advisory-modeling-batch-b",
    )

    assert result["data"]["batch"]["batch_id"] == delegate.batch_id
    assert len(delegate.resume_calls) == 1
    batch_id, command, observed_tasks = delegate.resume_calls[0]
    assert batch_id == delegate.batch_id
    assert command.expected_row_version == 31
    assert command.operation_idempotency_key.endswith("catalog-recovery-31")
    assert observed_tasks is tasks


def test_restart_safe_service_preserves_real_semantic_conflicts() -> None:
    service = RestartSafeHistoricalRangeService(service=_ConflictingService())

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        service.create_batch(
            _create_request(program_version=8),
            idempotency_key="batch-b-exact-key",
            background_tasks=object(),
            requested_by="advisory-modeling-batch-b",
        )

    assert exc_info.value.reason_code == REASON_IDEMPOTENCY_CONFLICT


def test_restart_safe_service_does_not_steal_an_active_catalog_lease() -> None:
    delegate = _ConflictingService(active_lease=True)
    service = RestartSafeHistoricalRangeService(service=delegate)

    with pytest.raises(AdvisoryModelingError, match="still owned by an active worker"):
        service.create_batch(
            _create_request(),
            idempotency_key="batch-b-exact-key",
            background_tasks=object(),
            requested_by="advisory-modeling-batch-b",
        )

    assert delegate.resume_calls == []
