import inspect

from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend.routers.strategy_packages as strategy_package_router
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validation_run import (
    PackageValidationRetrainMode,
    PackageValidationType,
)
from backend.services.strategy_package.validation_stability import StabilityStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def _service_with_manifest() -> tuple[StrategyPackageService, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest())
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest.package_id


def _add_seed_run(service: StrategyPackageService, package_id: str, seed: int, annual_return: float) -> None:
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_RETRAIN,
        retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
        seed_policy="fixed",
        random_seed=seed,
        metrics_json={"annual_return": annual_return},
        created_by="unit_test",
    )


def test_seed_stability_scores_multi_seed_runs_without_mutating_validation_rows() -> None:
    service, package_id = _service_with_manifest()
    _add_seed_run(service, package_id, 101, 0.100)
    _add_seed_run(service, package_id, 202, 0.095)
    _add_seed_run(service, package_id, 303, 0.105)
    before_ids = [run.validation_run_id for run in service.list_validation_runs(package_id)]

    summary = service.summarize_validation_stability(package_id, metric_key="annual_return")
    after_ids = [run.validation_run_id for run in service.list_validation_runs(package_id)]

    assert before_ids == after_ids
    assert summary.seed_stability.status == StabilityStatus.STABLE
    assert summary.seed_stability.sample_count == 3
    assert summary.seed_stability.stability_score is not None
    assert summary.seed_stability.stability_score > 0.85
    assert summary.seed_fragile is False


def test_seed_stability_marks_seed_fragile_when_metric_range_is_large() -> None:
    service, package_id = _service_with_manifest()
    _add_seed_run(service, package_id, 101, 0.20)
    _add_seed_run(service, package_id, 202, 0.02)

    summary = service.summarize_validation_stability(package_id, metric_key="annual_return")

    assert summary.seed_stability.status == StabilityStatus.FRAGILE
    assert summary.seed_stability.fragile is True
    assert summary.seed_fragile is True


def test_regime_stability_reads_regime_metric_evidence() -> None:
    service, package_id = _service_with_manifest()
    service.create_validation_run(
        package_id,
        validation_type=PackageValidationType.ORIGINAL_FIXED_WEIGHT,
        retrain_mode=PackageValidationRetrainMode.NO_RETRAIN,
        evidence_json={
            "regime_metrics": {
                "bull": {"annual_return": 0.14},
                "bear": {"annual_return": -0.03},
                "sideways": {"annual_return": 0.04},
            }
        },
        created_by="unit_test",
    )

    summary = service.summarize_validation_stability(package_id, metric_key="annual_return")

    assert summary.regime_stability.status == StabilityStatus.FRAGILE
    assert summary.regime_stability.sample_count == 3
    assert summary.regime_fragile is True
    assert {sample.label for sample in summary.regime_stability.samples} == {"bull", "bear", "sideways"}


def test_stability_summary_is_explicit_when_evidence_is_insufficient() -> None:
    service, package_id = _service_with_manifest()

    summary = service.summarize_validation_stability(package_id, metric_key="annual_return")

    assert summary.seed_stability.status == StabilityStatus.INSUFFICIENT_EVIDENCE
    assert summary.regime_stability.status == StabilityStatus.INSUFFICIENT_EVIDENCE
    assert summary.seed_fragile is None
    assert summary.regime_fragile is None
    assert "seed_stability_insufficient_evidence" in summary.warnings
    assert "regime_stability_insufficient_evidence" in summary.warnings


def test_validation_stability_router_exposes_read_only_summary(monkeypatch) -> None:
    class FakeService:
        def summarize_validation_stability(self, package_id, *, metric_key="annual_return", limit=500):  # type: ignore[no-untyped-def]
            assert package_id == "pkg_1"
            assert metric_key == "rank_ic"
            assert limit == 7
            service, real_package_id = _service_with_manifest()
            service.create_validation_run(
                real_package_id,
                validation_type=PackageValidationType.ORIGINAL_RETRAIN,
                retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
                seed_policy="fixed",
                random_seed=1,
                metrics_json={"rank_ic": 0.04},
                created_by="unit_test",
            )
            service.create_validation_run(
                real_package_id,
                validation_type=PackageValidationType.ORIGINAL_RETRAIN,
                retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
                seed_policy="fixed",
                random_seed=2,
                metrics_json={"rank_ic": 0.041},
                created_by="unit_test",
            )
            return service.summarize_validation_stability(real_package_id, metric_key=metric_key)

    monkeypatch.setattr(strategy_package_router, "StrategyPackageService", lambda: FakeService())
    app = FastAPI()
    app.include_router(strategy_package_router.router)
    client = TestClient(app)

    response = client.get("/strategy-packages/pkg_1/validation-stability?metric_key=rank_ic&limit=7")

    assert response.status_code == 200
    payload = response.json()["stability"]
    assert payload["metric_key"] == "rank_ic"
    assert payload["seed_stability"]["status"] == "STABLE"


def test_validation_stability_service_uses_read_only_repository_path() -> None:
    source = inspect.getsource(StrategyPackageService.summarize_validation_stability)

    assert "list_validation_runs" in source
    assert "save_validation_run" not in source
    assert "transition_status" not in source
