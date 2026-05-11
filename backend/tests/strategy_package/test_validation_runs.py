import inspect
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.strategy_packages as strategy_package_router
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository, StrategyPackageRepository
from backend.services.strategy_package.runtime_variant import RuntimeVariantKind
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validation_run import (
    PackageValidationRetrainMode,
    PackageValidationStatus,
    PackageValidationType,
)
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend" / "migrations" / "strategy_pkg_validation_run_20260509.sql"


def _migration_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _table_columns(sql: str) -> list[str]:
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS strategy_pkg\.package_validation_run \((.*?)\n\);",
        sql,
        flags=re.DOTALL,
    )
    assert match, "missing validation run table"
    columns: list[str] = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("CONSTRAINT"):
            if line.startswith("CONSTRAINT"):
                break
            continue
        columns.append(line.split()[0])
    return columns


def _service_with_manifest() -> tuple[StrategyPackageService, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest.package_id


def test_phase7_validation_run_migration_is_additive_and_commented() -> None:
    sql = _migration_sql()

    assert "CREATE SCHEMA IF NOT EXISTS strategy_pkg" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.package_validation_run" in sql
    assert "DROP " not in sql.upper()
    assert "COMMENT ON TABLE strategy_pkg.package_validation_run" in sql
    for column in _table_columns(sql):
        assert f"COMMENT ON COLUMN strategy_pkg.package_validation_run.{column}" in sql
    assert "status <> 'PASSED'" in sql
    assert "jsonb_array_length(evidence_json->'windows') > 0" in sql


def test_validation_run_appends_latest_fixed_weight_evidence_without_mutating_manifest() -> None:
    service, package_id = _service_with_manifest()
    before = service.get_package(package_id)
    run = service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.LATEST_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        source_data_version="qlib_2024q4_snapshot",
        target_data_version="qlib_2026q2_latest",
        backtest_start=date(2026, 1, 1),
        backtest_end=date(2026, 4, 30),
        created_by="unit_test",
    )
    after = service.get_package(package_id)

    assert run.validation_run_id.startswith("vr_")
    assert run.manifest_sha256 == before.manifest_sha256
    assert run.status == PackageValidationStatus.REQUESTED
    assert after.manifest_sha256 == before.manifest_sha256
    assert after.current_manifest().model_dump(mode="json") == before.current_manifest().model_dump(mode="json")
    assert service.list_validation_runs(package_id)[0].validation_run_id == run.validation_run_id


def test_validation_run_fail_fast_for_missing_latest_dataset_and_passed_evidence() -> None:
    service, package_id = _service_with_manifest()

    with pytest.raises(StrategyPackageValidationError, match="target_data_version"):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.LATEST_FIXED_WEIGHT,
            retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
            created_by="unit_test",
        )

    with pytest.raises(StrategyPackageValidationError, match="metrics and artifact manifest"):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
            retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
            status=PackageValidationStatus.PASSED,
            completed_at=datetime.now(timezone.utc),
            created_by="unit_test",
        )


def test_retrain_and_walk_forward_validation_require_seed_and_window_evidence() -> None:
    service, package_id = _service_with_manifest()

    with pytest.raises(StrategyPackageValidationError, match="seed_policy"):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.LATEST_RETRAIN,
            retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
            target_data_version="qlib_2026q2_latest",
            created_by="unit_test",
        )

    with pytest.raises(StrategyPackageValidationError, match="window evidence"):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.WALK_FORWARD_ROLLING,
            retrain_mode=PackageValidationRetrainMode.ROLLING_RETRAIN,
            target_data_version="qlib_2026q2_latest",
            seed_policy="fixed:20260509",
            evidence_json={"windows": []},
            created_by="unit_test",
        )

    run = service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.WALK_FORWARD_ROLLING,
        retrain_mode=PackageValidationRetrainMode.ROLLING_RETRAIN,
        target_data_version="qlib_2026q2_latest",
        seed_policy="fixed:20260509",
        evidence_json={"windows": [{"train": "2024", "test": "2025"}]},
        created_by="unit_test",
    )

    assert run.validation_type == PackageValidationType.WALK_FORWARD_ROLLING
    assert run.evidence_json["windows"][0]["test"] == "2025"


def test_runtime_variant_validation_captures_variant_hash_and_filters_list() -> None:
    service, package_id = _service_with_manifest()
    variant = service.create_runtime_variant(
        package_id,
        variant_name="risk cap",
        variant_kind=RuntimeVariantKind.RISK_POLICY,
        variant_config={"risk_policy": {"max_position_weight": 0.04}},
        created_by="unit_test",
    )

    run = service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.RUNTIME_VARIANT_BACKTEST,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        runtime_variant_id=variant.variant_id,
        created_by="unit_test",
    )

    assert run.runtime_variant_id == variant.variant_id
    assert run.runtime_variant_hash == variant.variant_hash
    assert service.list_validation_runs(package_id, runtime_variant_id=variant.variant_id)[0].validation_run_id == run.validation_run_id
    assert service.list_validation_runs(package_id, validation_type=PackageValidationType.RUNTIME_VARIANT_BACKTEST)[0].validation_run_id == run.validation_run_id

    with pytest.raises(StrategyPackageValidationError, match="runtime_variant_hash"):
        service.repository.save_validation_run(
            run.model_copy(update={"validation_run_id": "vr_bad_hash", "runtime_variant_hash": "wrong"})
        )


def test_runtime_variant_validation_rejects_missing_or_non_variant_fields() -> None:
    service, package_id = _service_with_manifest()

    with pytest.raises(StrategyPackageValidationError, match="variant id and hash"):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.RUNTIME_VARIANT_BACKTEST,
            retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
            created_by="unit_test",
        )


def test_validation_run_router_exposes_create_list_and_get(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeService:
        def __init__(self) -> None:
            self.service, self.real_package_id = _service_with_manifest()

        def create_validation_run(self, package_id, **kwargs):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert kwargs["validation_type"] == PackageValidationType.LATEST_FIXED_WEIGHT
            return self.service.create_validation_run(self.real_package_id, **kwargs)

        def list_validation_runs(self, package_id, *, validation_type=None, runtime_variant_id=None, limit=100):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert validation_type == PackageValidationType.LATEST_FIXED_WEIGHT
            assert runtime_variant_id is None
            assert limit == 5
            return []

        def get_validation_run(self, package_id, validation_run_id):  # type: ignore[no-untyped-def]
            assert (package_id, validation_run_id) == ("pkg_1", "vr_1")
            return self.service.create_validation_run(
                self.real_package_id,
                validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
                retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
                created_by="unit_test",
            ).model_copy(update={"validation_run_id": validation_run_id})

    fake = FakeService()
    monkeypatch.setattr(strategy_package_router, "StrategyPackageService", lambda: fake)
    app = FastAPI()
    app.include_router(strategy_package_router.router)
    client = TestClient(app)

    created = client.post(
        "/strategy-packages/pkg_1/validation-runs",
        json={
            "validation_type": "latest_fixed_weight",
            "retrain_mode": "no_retrain",
            "target_data_version": "qlib_2026q2_latest",
            "created_by": "unit_test",
        },
    )
    listed = client.get("/strategy-packages/pkg_1/validation-runs?validation_type=latest_fixed_weight&limit=5")
    fetched = client.get("/strategy-packages/pkg_1/validation-runs/vr_1")

    assert created.status_code == 200
    assert created.json()["validation_run"]["validation_type"] == "latest_fixed_weight"
    assert listed.status_code == 200
    assert listed.json()["validation_runs"] == []
    assert fetched.status_code == 200
    assert fetched.json()["validation_run"]["validation_run_id"] == "vr_1"


def test_postgres_validation_run_repository_is_append_only_and_keeps_package_table_read_only() -> None:
    save_source = inspect.getsource(StrategyPackageRepository.save_validation_run)
    list_source = inspect.getsource(StrategyPackageRepository.list_validation_runs)

    assert "INSERT INTO strategy_pkg.package_validation_run" in save_source
    assert "UPDATE strategy_pkg.package" not in save_source
    assert "DELETE FROM strategy_pkg.package" not in save_source
    assert "strategy_pkg.package_validation_run" in list_source
