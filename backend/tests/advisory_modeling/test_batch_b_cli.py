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
    DATABASE_TARGET_DEV,
    DATABASE_TARGET_PRODUCTION,
    DB_KEYS,
    REQUIRED_RUNTIME_KEYS,
    RestartSafeHistoricalRangeService,
    _database_config,
    _existing_directory,
    _load_environment,
    build_parser,
    main as batch_b_main,
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


def test_cli_preserves_production_default_and_parses_explicit_dev_target() -> None:
    base_args = [
        "--request",
        "F:/configured/request.json",
        "--env-file",
        "F:/configured/.env",
        "--repository-root",
        "F:/Dev/AIstock",
        "--artifact-root",
        "F:/Dev/AIstock_artifacts/advisory_modeling-dev",
        "--spool-root",
        "F:/Dev/AIstock_artifacts/advisory_modeling-dev/spool",
    ]

    production = build_parser().parse_args(base_args)
    dev = build_parser().parse_args(
        [
            *base_args,
            "--database-target",
            DATABASE_TARGET_DEV,
            "--dev-historical-range-runtime-root",
            "F:/Dev/AIstock_artifacts/historical-range-dev",
        ]
    )

    assert production.database_target == DATABASE_TARGET_PRODUCTION
    assert production.dev_historical_range_runtime_root is None
    assert dev.database_target == DATABASE_TARGET_DEV
    assert dev.dev_historical_range_runtime_root == Path(
        "F:/Dev/AIstock_artifacts/historical-range-dev"
    )


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


def test_dev_materialization_uses_only_dev_database_profile_and_derived_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = tmp_path / "repository"
    runtime_root = tmp_path / "historical-range-dev"
    package_asset_root = tmp_path / "package-assets"
    repository_root.mkdir()
    runtime_root.mkdir()
    package_asset_root.mkdir()
    values = {
        "TDX_DB_HOST": "production.example",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "production_db",
        "TDX_DB_USER": "production_user",
        "TDX_DB_PASSWORD": "production-secret",
        "TDX_DB_DEV_HOST": "dev.example",
        "TDX_DB_DEV_PORT": "15432",
        "TDX_DB_DEV_NAME": "dev_db",
        "TDX_DB_DEV_USER": "dev_user",
        "TDX_DB_DEV_PASSWORD": "dev-secret",
        **{key: f"F:/shared/{key.lower()}" for key in REQUIRED_RUNTIME_KEYS},
    }
    values["AISTOCK_PACKAGE_ASSET_STORE_ROOT"] = str(package_asset_root)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    for key in (*DB_KEYS, *REQUIRED_RUNTIME_KEYS):
        monkeypatch.delenv(key, raising=False)

    loaded, _ = _load_environment(
        env_file,
        database_target=DATABASE_TARGET_DEV,
        repository_root=repository_root.resolve(),
        dev_historical_range_runtime_root=runtime_root,
    )

    assert _database_config(loaded) == {
        "host": "dev.example",
        "port": 15432,
        "dbname": "dev_db",
        "user": "dev_user",
        "password": "dev-secret",
    }
    assert os.environ["TDX_DB_HOST"] == "dev.example"
    assert loaded["AISTOCK_PACKAGE_ASSET_STORE_ROOT"] == str(package_asset_root.resolve())
    assert loaded["AISTOCK_REPOSITORY_ROOT"] == str(repository_root.resolve())
    for key in (
        "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT",
        "AISTOCK_ADVISORY_HISTORICAL_RANGE_TASK_RUNTIME_ROOT",
        "AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT",
        "AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT",
        "AISTOCK_ADVISORY_DATASET_STORE_ROOT",
    ):
        Path(loaded[key]).relative_to(runtime_root.resolve())
        assert "F:/shared" not in loaded[key]


def test_dev_materialization_rejects_missing_profile_without_default_database_fallback(
    tmp_path: Path,
) -> None:
    repository_root = tmp_path / "repository"
    runtime_root = tmp_path / "historical-range-dev"
    package_asset_root = tmp_path / "package-assets"
    repository_root.mkdir()
    runtime_root.mkdir()
    package_asset_root.mkdir()
    values = {
        "TDX_DB_HOST": "production.example",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "production_db",
        "TDX_DB_USER": "production_user",
        "TDX_DB_PASSWORD": "production-secret",
        "AISTOCK_PACKAGE_ASSET_STORE_ROOT": str(package_asset_root),
    }
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="TDX_DB_DEV_HOST"):
        _load_environment(
            env_file,
            database_target=DATABASE_TARGET_DEV,
            repository_root=repository_root.resolve(),
            dev_historical_range_runtime_root=runtime_root,
        )


def test_dev_materialization_requires_external_runtime_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = tmp_path / "repository"
    runtime_root = tmp_path / "historical-range-dev"
    package_asset_root = tmp_path / "package-assets"
    repository_root.mkdir()
    runtime_root.mkdir()
    package_asset_root.mkdir()
    values = {
        "TDX_DB_DEV_HOST": "dev.example",
        "TDX_DB_DEV_PORT": "15432",
        "TDX_DB_DEV_NAME": "dev_db",
        "TDX_DB_DEV_USER": "dev_user",
        "TDX_DB_DEV_PASSWORD": "dev-secret",
        "AISTOCK_PACKAGE_ASSET_STORE_ROOT": str(package_asset_root),
    }
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    for key in (*DB_KEYS, *REQUIRED_RUNTIME_KEYS):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(ValueError, match="dev_historical_range_runtime_root is required"):
        _load_environment(
            env_file,
            database_target=DATABASE_TARGET_DEV,
            repository_root=repository_root.resolve(),
        )
    loaded, _ = _load_environment(
        env_file,
        database_target=DATABASE_TARGET_DEV,
        repository_root=repository_root.resolve(),
        dev_historical_range_runtime_root=runtime_root,
    )
    assert loaded["TDX_DB_NAME"] == "dev_db"
    with pytest.raises(ValueError, match="outside repository_root"):
        _load_environment(
            env_file,
            database_target=DATABASE_TARGET_DEV,
            repository_root=repository_root.resolve(),
            dev_historical_range_runtime_root=repository_root,
        )


def test_production_target_rejects_dev_runtime_root_as_mixed_configuration(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "TDX_DB_HOST=production.example",
                "TDX_DB_PORT=5432",
                "TDX_DB_NAME=production_db",
                "TDX_DB_USER=production_user",
                "TDX_DB_PASSWORD=production-secret",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="only valid when database_target=dev"):
        _load_environment(
            env_file,
            require_runtime=False,
            database_target=DATABASE_TARGET_PRODUCTION,
            dev_historical_range_runtime_root=tmp_path,
        )


def test_cli_reports_incomplete_dev_profile_without_default_database_fallback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repository_root = tmp_path / "repository"
    artifact_root = tmp_path / "artifacts"
    runtime_root = tmp_path / "historical-range-dev"
    package_asset_root = tmp_path / "package-assets"
    repository_root.mkdir()
    artifact_root.mkdir()
    runtime_root.mkdir()
    package_asset_root.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "TDX_DB_HOST=production.example",
                "TDX_DB_PORT=5432",
                "TDX_DB_NAME=production_db",
                "TDX_DB_USER=production_user",
                "TDX_DB_PASSWORD=production-secret",
                f"AISTOCK_PACKAGE_ASSET_STORE_ROOT={package_asset_root}",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    exit_code = batch_b_main(
        [
            "--request",
            str(tmp_path / "request.json"),
            "--env-file",
            str(env_file),
            "--repository-root",
            str(repository_root),
            "--artifact-root",
            str(artifact_root),
            "--database-target",
            DATABASE_TARGET_DEV,
            "--dev-historical-range-runtime-root",
            str(runtime_root),
        ]
    )

    payload = json.loads(capsys.readouterr().err)
    assert exit_code == 3
    assert payload["reason_code"] == "MODEL_BATCH_B_EXECUTION_FAILED"
    assert "TDX_DB_DEV_HOST" in payload["message"]
    assert "production-secret" not in payload["message"]


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
