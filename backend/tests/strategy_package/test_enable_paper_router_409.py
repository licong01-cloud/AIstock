from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import strategy_packages as router_module
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime_variant import RuntimeVariantKind, RuntimeVariantValidationStatus
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validation_run import (
    PackageValidationRetrainMode,
    PackageValidationStatus,
    PackageValidationType,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


@pytest.fixture
def app_and_repo(monkeypatch: pytest.MonkeyPatch) -> tuple[FastAPI, InMemoryStrategyPackageRepository]:
    repo = InMemoryStrategyPackageRepository()

    def _factory(*args, **kwargs):  # noqa: ANN001
        return StrategyPackageService(repository=repo)

    monkeypatch.setattr(router_module, "StrategyPackageService", _factory)
    app = FastAPI()
    app.include_router(router_module.router)
    return app, repo


def _seed_paper_ready_package(service: StrategyPackageService, package_id: str) -> None:
    completed_at = datetime.now(timezone.utc)
    service.record_package_asset(
        package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="weights/frozen.pkl",
        asset_sha256="sha256:weights",
    )
    variant = service.create_runtime_variant(
        package_id,
        variant_name="risk cap",
        variant_kind=RuntimeVariantKind.RISK_POLICY,
        variant_config={"risk_policy": {"max_position_weight": 0.04}},
        created_by="unit_test",
    )
    service.mark_runtime_variant_validation(
        package_id,
        variant.variant_id,
        validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
        paper_candidate=True,
        validation_evidence={"validation_run_id": "vr_candidate", "status": "passed"},
    )
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        status=PackageValidationStatus.PASSED,
        metrics_json={"annual_return": 0.12},
        artifact_manifest_json={"artifact_sha256": "sha256:original"},
        evidence_json={
            "regime_metrics": {
                "bull": {"annual_return": 0.101},
                "bear": {"annual_return": 0.102},
            }
        },
        completed_at=completed_at,
        created_by="unit_test",
    )
    for seed, annual_return in ((101, 0.101), (202, 0.102)):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.ORIGINAL_RETRAIN,
            retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
            seed_policy="fixed",
            random_seed=seed,
            status=PackageValidationStatus.PASSED,
            metrics_json={"annual_return": annual_return},
            artifact_manifest_json={"artifact_sha256": f"sha256:seed-{seed}"},
            evidence_json={
                "regime_metrics": {
                    "bull": {"annual_return": annual_return},
                    "bear": {"annual_return": annual_return + 0.0001},
                }
            },
            completed_at=completed_at,
            created_by="unit_test",
        )


def test_enable_paper_endpoint_returns_409_on_invalid_transition(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
) -> None:
    app, repo = app_and_repo
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(manifest)
    _seed_paper_ready_package(StrategyPackageService(repository=repo), record.package_id)
    repo.records[record.package_id] = repo.get(record.package_id).model_copy(
        update={"package_status": PackageStatus.PAPER_ENABLED}
    )

    response = TestClient(app).post(f"/strategy-packages/{record.package_id}/enable-paper")

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["error_code"] == "INVALID_STATE_TRANSITION"
    assert detail["context"]["from_status"] == PackageStatus.PAPER_ENABLED.value
    assert detail["context"]["to_status"] == PackageStatus.PAPER_ENABLED.value


def test_enable_paper_endpoint_keeps_validation_errors_at_400(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
) -> None:
    app, repo = app_and_repo
    manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(manifest)

    response = TestClient(app).post(f"/strategy-packages/{record.package_id}/enable-paper")

    assert response.status_code == 400, response.text
    detail = response.json()["detail"]
    assert detail["error_code"] == "STRATEGY_PACKAGE_VALIDATION_ERROR"
    assert detail["context"]["paper_ready"] is False
    assert "original_fixed_weight_retest_missing_passed_run_for_current_manifest" in detail["context"]["blockers"]
