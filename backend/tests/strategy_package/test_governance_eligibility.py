from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.strategy_packages as strategy_package_router
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime_variant import (
    RuntimeVariantKind,
    RuntimeVariantValidationStatus,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validation_run import (
    PackageValidationRetrainMode,
    PackageValidationStatus,
    PackageValidationType,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _service_with_manifest() -> tuple[StrategyPackageService, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED}))
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest.package_id


def _seed_paper_ready_package(service: StrategyPackageService, package_id: str) -> None:
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

    completed_at = datetime.now(timezone.utc) - timedelta(days=2)
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        status=PackageValidationStatus.PASSED,
        metrics_json={"annual_return": 0.12, "rank_ic": 0.052, "max_drawdown": -0.08},
        artifact_manifest_json={"artifact_sha256": "sha256:original"},
        evidence_json={
            "regime_metrics": {
                "bull": {"annual_return": 0.103, "rank_ic": 0.053},
                "bear": {"annual_return": 0.101, "rank_ic": 0.052},
            }
        },
        completed_at=completed_at,
        created_by="unit_test",
    )
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_RETRAIN,
        retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
        seed_policy="fixed",
        random_seed=101,
        metrics_json={"annual_return": 0.101, "rank_ic": 0.051},
        evidence_json={
            "regime_metrics": {
                "bull": {"annual_return": 0.102, "rank_ic": 0.052},
                "bear": {"annual_return": 0.1, "rank_ic": 0.05},
            }
        },
        created_by="unit_test",
        completed_at=completed_at,
    )
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_RETRAIN,
        retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
        seed_policy="fixed",
        random_seed=202,
        metrics_json={"annual_return": 0.103, "rank_ic": 0.052},
        evidence_json={
            "regime_metrics": {
                "bull": {"annual_return": 0.104, "rank_ic": 0.053},
                "bear": {"annual_return": 0.102, "rank_ic": 0.051},
            }
        },
        created_by="unit_test",
        completed_at=completed_at,
    )


def test_governance_eligibility_returns_read_only_summary_with_all_gates_ready() -> None:
    service, package_id = _service_with_manifest()
    _seed_paper_ready_package(service, package_id)
    before_events = [event.reason for event in service.list_status_events(package_id)]

    eligibility = service.governance_eligibility(package_id, metric_key="annual_return", limit=7)

    assert eligibility["package_id"] == package_id
    assert eligibility["manifest_sha256"] == service.get_package(package_id).manifest_sha256
    assert eligibility["paper_ready"] is True
    assert eligibility["blockers"] == []
    assert eligibility["satisfied_gates"] == [
        "manifest_identity",
        "original_fixed_weight_retest",
        "validation_stability",
        "protected_assets",
        "runtime_variant_candidate",
    ]
    assert eligibility["original_fixed_weight_retest"]["passed"] is True
    assert eligibility["original_fixed_weight_retest"]["status"] == "READY"
    assert eligibility["validation_stability"]["passed"] is True
    assert eligibility["validation_stability"]["summary"]["seed_stability"]["status"] == "STABLE"
    assert eligibility["protected_asset_status"]["passed"] is True
    assert eligibility["protected_asset_status"]["protected_asset_count"] == 1
    assert eligibility["runtime_variant_candidate_status"]["passed"] is True
    assert eligibility["runtime_variant_candidate_status"]["paper_candidate_count"] == 1
    assert [event.reason for event in service.list_status_events(package_id)] == before_events


def test_governance_eligibility_router_exposes_read_only_summary(monkeypatch) -> None:
    class FakeService:
        def __init__(self) -> None:
            self.service, self.real_package_id = _service_with_manifest()
            _seed_paper_ready_package(self.service, self.real_package_id)

        def governance_eligibility(self, package_id, *, metric_key="annual_return", limit=500):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert metric_key == "rank_ic"
            assert limit == 9
            return self.service.governance_eligibility(self.real_package_id, metric_key=metric_key, limit=limit)

    monkeypatch.setattr(strategy_package_router, "StrategyPackageService", lambda: FakeService())
    app = FastAPI()
    app.include_router(strategy_package_router.router)
    client = TestClient(app)

    response = client.get("/strategy-packages/pkg_1/governance-eligibility?metric_key=rank_ic&limit=9")

    assert response.status_code == 200
    payload = response.json()
    assert payload["package_id"] == "pkg_1"
    assert payload["eligibility"]["paper_ready"] is True
    assert payload["eligibility"]["original_fixed_weight_retest"]["status"] == "READY"
