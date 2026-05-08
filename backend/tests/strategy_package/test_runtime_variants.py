from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.strategy_packages as strategy_package_router
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository, StrategyPackageRepository
from backend.services.strategy_package.runtime_variant import (
    RuntimeVariantKind,
    RuntimeVariantValidationStatus,
    derive_locked_core_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


REPO_ROOT = Path(__file__).resolve().parents[3]
MIGRATION = REPO_ROOT / "backend" / "migrations" / "strategy_pkg_runtime_variant_20260509.sql"


def _migration_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _table_columns(sql: str) -> list[str]:
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS strategy_pkg\.package_runtime_variant \((.*?)\n\);",
        sql,
        flags=re.DOTALL,
    )
    assert match, "missing runtime variant table"
    columns: list[str] = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("CONSTRAINT"):
            continue
        columns.append(line.split()[0])
    return columns


def _service_with_manifest() -> tuple[StrategyPackageService, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest.package_id


def test_phase6_runtime_variant_migration_is_additive_and_commented() -> None:
    sql = _migration_sql()

    assert "CREATE SCHEMA IF NOT EXISTS strategy_pkg" in sql
    assert "CREATE TABLE IF NOT EXISTS strategy_pkg.package_runtime_variant" in sql
    assert "DROP " not in sql.upper()
    assert "COMMENT ON TABLE strategy_pkg.package_runtime_variant" in sql
    for column in _table_columns(sql):
        assert f"COMMENT ON COLUMN strategy_pkg.package_runtime_variant.{column}" in sql
    assert "paper_candidate = FALSE OR validation_status = 'VALIDATION_PASSED'" in sql


def test_runtime_variant_preserves_locked_core_hash_and_changes_variant_hash() -> None:
    service, package_id = _service_with_manifest()
    manifest = service.get_package(package_id).current_manifest()
    core_hash = derive_locked_core_hash(manifest)

    twap = service.create_runtime_variant(
        package_id,
        variant_name="TWAP lower cap",
        variant_kind=RuntimeVariantKind.EXECUTION_POLICY,
        variant_config={"minute_execution_policy": {"algo_code": "TWAP", "algo_config": {"max_participation": 0.08}}},
        created_by="unit_test",
    )
    vwap = service.create_runtime_variant(
        package_id,
        variant_name="VWAP lower cap",
        variant_kind=RuntimeVariantKind.EXECUTION_POLICY,
        variant_config={"minute_execution_policy": {"algo_code": "VWAP", "algo_config": {"max_participation": 0.08}}},
        created_by="unit_test",
    )

    assert twap.locked_core_hash == core_hash
    assert vwap.locked_core_hash == core_hash
    assert twap.variant_hash != vwap.variant_hash
    assert twap.manifest_sha256 == manifest.manifest_sha256
    assert twap.paper_candidate is False


def test_runtime_variant_rejects_frozen_core_mutation() -> None:
    service, package_id = _service_with_manifest()

    with pytest.raises(StrategyPackageValidationError, match="frozen StrategyPackage core"):
        service.create_runtime_variant(
            package_id,
            variant_name="bad model swap",
            variant_kind=RuntimeVariantKind.COMBINED,
            variant_config={"model_asset": {"model_id": "different_model"}},
            created_by="unit_test",
        )

    with pytest.raises(StrategyPackageValidationError, match="unsupported runtime keys"):
        service.create_runtime_variant(
            package_id,
            variant_name="unknown",
            variant_kind=RuntimeVariantKind.COMBINED,
            variant_config={"free_form": True},
            created_by="unit_test",
        )


def test_runtime_variant_repository_rejects_manifest_or_core_hash_mismatch() -> None:
    service, package_id = _service_with_manifest()
    repo = service.repository
    variant = service.create_runtime_variant(
        package_id,
        variant_name="risk cap",
        variant_kind=RuntimeVariantKind.RISK_POLICY,
        variant_config={"risk_policy": {"max_position_weight": 0.04}},
        created_by="unit_test",
    )

    with pytest.raises(StrategyPackageValidationError, match="manifest_sha256"):
        repo.save_runtime_variant(variant.model_copy(update={"manifest_sha256": "wrong"}))
    with pytest.raises(StrategyPackageValidationError, match="locked core hash"):
        repo.save_runtime_variant(variant.model_copy(update={"locked_core_hash": "wrong"}))


def test_runtime_variant_paper_candidate_requires_passed_validation_and_evidence() -> None:
    service, package_id = _service_with_manifest()

    with pytest.raises(StrategyPackageValidationError, match="pass validation"):
        service.create_runtime_variant(
            package_id,
            variant_name="paper too early",
            variant_kind=RuntimeVariantKind.RISK_POLICY,
            variant_config={"risk_policy": {"max_position_weight": 0.04}},
            paper_candidate=True,
            created_by="unit_test",
        )

    variant = service.create_runtime_variant(
        package_id,
        variant_name="risk cap",
        variant_kind=RuntimeVariantKind.RISK_POLICY,
        variant_config={"risk_policy": {"max_position_weight": 0.04}},
        created_by="unit_test",
    )
    with pytest.raises(StrategyPackageValidationError, match="validation evidence"):
        service.mark_runtime_variant_validation(
            package_id,
            variant.variant_id,
            validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
            paper_candidate=True,
            validation_evidence={},
        )

    passed = service.mark_runtime_variant_validation(
        package_id,
        variant.variant_id,
        validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
        paper_candidate=True,
        validation_evidence={"validation_run_id": "vr_1", "status": "passed"},
    )

    assert passed.paper_candidate is True
    assert passed.validation_status == RuntimeVariantValidationStatus.VALIDATION_PASSED


def test_runtime_variant_list_hides_retired_by_default() -> None:
    service, package_id = _service_with_manifest()
    variant = service.create_runtime_variant(
        package_id,
        variant_name="retire me",
        variant_kind=RuntimeVariantKind.HMM_OVERLAY,
        variant_config={"hmm_overlay": {"enabled": True}},
        created_by="unit_test",
    )

    service.mark_runtime_variant_validation(
        package_id,
        variant.variant_id,
        validation_status=RuntimeVariantValidationStatus.RETIRED,
        validation_evidence={"reason": "not needed"},
    )

    assert service.list_runtime_variants(package_id) == []
    assert service.list_runtime_variants(package_id, include_retired=True)[0].variant_id == variant.variant_id


def test_runtime_variant_router_exposes_create_list_and_validation_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeService:
        def create_runtime_variant(self, package_id, **kwargs):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert kwargs["variant_kind"] == RuntimeVariantKind.RISK_POLICY
            service, real_package_id = _service_with_manifest()
            return service.create_runtime_variant(real_package_id, **kwargs)

        def list_runtime_variants(self, package_id, *, include_retired=False, limit=100):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert include_retired is True
            assert limit == 5
            return []

        def mark_runtime_variant_validation(self, package_id, variant_id, **kwargs):  # type: ignore[no-untyped-def]
            assert (package_id, variant_id) == ("pkg_1", "rtv_1")
            service, real_package_id = _service_with_manifest()
            variant = service.create_runtime_variant(
                real_package_id,
                variant_name="risk",
                variant_kind=RuntimeVariantKind.RISK_POLICY,
                variant_config={"risk_policy": {"max_position_weight": 0.04}},
                created_by="unit_test",
            )
            return service.mark_runtime_variant_validation(real_package_id, variant.variant_id, **kwargs)

    monkeypatch.setattr(strategy_package_router, "StrategyPackageService", lambda: FakeService())
    app = FastAPI()
    app.include_router(strategy_package_router.router)
    client = TestClient(app)

    created = client.post(
        "/strategy-packages/pkg_1/runtime-variants",
        json={
            "variant_name": "risk",
            "variant_kind": "risk_policy",
            "variant_config": {"risk_policy": {"max_position_weight": 0.04}},
            "created_by": "unit_test",
        },
    )
    listed = client.get("/strategy-packages/pkg_1/runtime-variants?include_retired=true&limit=5")
    marked = client.post(
        "/strategy-packages/pkg_1/runtime-variants/rtv_1/validation",
        json={
            "validation_status": "VALIDATION_PASSED",
            "paper_candidate": True,
            "validation_evidence": {"validation_run_id": "vr_1"},
        },
    )

    assert created.status_code == 200
    assert created.json()["runtime_variant"]["paper_candidate"] is False
    assert listed.status_code == 200
    assert listed.json()["runtime_variants"] == []
    assert marked.status_code == 200
    assert marked.json()["runtime_variant"]["paper_candidate"] is True


def test_postgres_runtime_variant_repository_uses_append_only_create_and_no_manifest_update() -> None:
    save_source = inspect.getsource(StrategyPackageRepository.save_runtime_variant)
    status_source = inspect.getsource(StrategyPackageRepository.set_runtime_variant_validation)

    assert "strategy_pkg.package_runtime_variant" in save_source
    assert "ON CONFLICT (package_id, variant_hash) DO NOTHING" in save_source
    assert "UPDATE strategy_pkg.package_runtime_variant" in status_source
    assert "strategy_pkg.package\n" not in f"{save_source}\n{status_source}"
