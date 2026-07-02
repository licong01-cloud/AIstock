from __future__ import annotations

from datetime import datetime, timedelta, timezone


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


def _service_with_manifest(
    package_status: PackageStatus = PackageStatus.BACKTEST_APPROVED,
) -> tuple[StrategyPackageService, str]:
    repo = InMemoryStrategyPackageRepository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": package_status}))
    repo.save_manifest(manifest)
    return StrategyPackageService(repository=repo), manifest.package_id


def _record_protected_model_asset(service: StrategyPackageService, package_id: str) -> None:
    service.record_package_asset(
        package_id,
        asset_type=StrategyPackageAssetType.MODEL_WEIGHT,
        asset_ref="weights/frozen.pkl",
        asset_sha256="sha256:weights",
    )


def _record_runtime_candidate(service: StrategyPackageService, package_id: str) -> None:
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


def _record_original_fixed_weight_retest(
    service: StrategyPackageService,
    package_id: str,
    *,
    completed_at: datetime,
) -> None:
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


def _record_stable_seed_runs(
    service: StrategyPackageService,
    package_id: str,
    *,
    completed_at: datetime,
) -> None:
    for seed, annual_return, rank_ic in (
        (101, 0.101, 0.051),
        (202, 0.103, 0.052),
    ):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.ORIGINAL_RETRAIN,
            retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
            seed_policy="fixed",
            random_seed=seed,
            metrics_json={"annual_return": annual_return, "rank_ic": rank_ic},
            created_by="unit_test",
            completed_at=completed_at,
        )


def _record_fragile_seed_runs(
    service: StrategyPackageService,
    package_id: str,
    *,
    completed_at: datetime,
) -> None:
    for seed, annual_return in (
        (101, 0.20),
        (202, 0.02),
    ):
        service.create_validation_run(
            package_id,
            validation_type=PackageValidationType.ORIGINAL_RETRAIN,
            retrain_mode=PackageValidationRetrainMode.FIXED_SEED_RETRAIN,
            seed_policy="fixed",
            random_seed=seed,
            metrics_json={"annual_return": annual_return},
            created_by="unit_test",
            completed_at=completed_at,
        )


def _seed_paper_ready_package(service: StrategyPackageService, package_id: str) -> None:
    completed_at = datetime.now(timezone.utc) - timedelta(days=2)
    _record_protected_model_asset(service, package_id)
    _record_runtime_candidate(service, package_id)
    _record_original_fixed_weight_retest(service, package_id, completed_at=completed_at)
    _record_stable_seed_runs(service, package_id, completed_at=completed_at)


def test_governance_eligibility_returns_read_only_summary_with_all_gates_ready() -> None:
    service, package_id = _service_with_manifest()
    _seed_paper_ready_package(service, package_id)
    before_events = [event.reason for event in service.list_status_events(package_id)]

    eligibility = service.governance_eligibility(package_id, metric_key="annual_return", limit=7)

    assert eligibility["package_id"] == package_id
    assert eligibility["manifest_sha256"] == service.get_package(package_id).manifest_sha256
    assert eligibility["live_strict_ready"] is True
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
    assert eligibility["runtime_variant_candidate_status"]["validated_variant_count"] == 1
    assert [event.reason for event in service.list_status_events(package_id)] == before_events


def test_governance_eligibility_blocks_missing_protected_asset_ledger() -> None:
    service, package_id = _service_with_manifest()
    completed_at = datetime.now(timezone.utc) - timedelta(days=2)
    _record_runtime_candidate(service, package_id)
    _record_original_fixed_weight_retest(service, package_id, completed_at=completed_at)
    _record_stable_seed_runs(service, package_id, completed_at=completed_at)

    eligibility = service.governance_eligibility(package_id, metric_key="annual_return")

    assert eligibility["live_strict_ready"] is False
    assert "protected_asset_ledger_missing" in eligibility["blockers"]
    assert "protected_assets" not in eligibility["satisfied_gates"]
    assert eligibility["protected_asset_status"]["passed"] is False
    assert eligibility["protected_asset_status"]["asset_count"] == 0


def test_governance_eligibility_treats_missing_runtime_candidate_as_warning() -> None:
    service, package_id = _service_with_manifest()
    completed_at = datetime.now(timezone.utc) - timedelta(days=2)
    _record_protected_model_asset(service, package_id)
    _record_original_fixed_weight_retest(service, package_id, completed_at=completed_at)
    _record_stable_seed_runs(service, package_id, completed_at=completed_at)

    eligibility = service.governance_eligibility(package_id, metric_key="annual_return")

    assert eligibility["live_strict_ready"] is True
    assert "runtime_variant_paper_candidate_missing" not in eligibility["blockers"]
    assert "runtime_variant_candidate" in eligibility["satisfied_gates"]
    assert eligibility["runtime_variant_candidate_status"]["passed"] is True
    assert eligibility["runtime_variant_candidate_status"]["validated_variant_count"] == 0
    assert "validated_runtime_variant_missing" in eligibility["runtime_variant_candidate_status"]["warnings"]


def test_governance_eligibility_blocks_fragile_validation_stability() -> None:
    service, package_id = _service_with_manifest()
    completed_at = datetime.now(timezone.utc) - timedelta(days=2)
    _record_protected_model_asset(service, package_id)
    _record_runtime_candidate(service, package_id)
    _record_original_fixed_weight_retest(service, package_id, completed_at=completed_at)
    _record_fragile_seed_runs(service, package_id, completed_at=completed_at)

    eligibility = service.governance_eligibility(package_id, metric_key="annual_return")

    assert eligibility["live_strict_ready"] is False
    assert "seed_stability=FRAGILE" in eligibility["blockers"]
    assert "validation_stability" not in eligibility["satisfied_gates"]
    assert eligibility["validation_stability"]["passed"] is False
    assert eligibility["validation_stability"]["seed_fragile"] is True
    assert eligibility["validation_stability"]["regime_fragile"] is False


def test_governance_eligibility_blocks_disallowed_package_status() -> None:
    service, package_id = _service_with_manifest(PackageStatus.DRAFT)
    _seed_paper_ready_package(service, package_id)

    eligibility = service.governance_eligibility(package_id, metric_key="annual_return")

    assert eligibility["live_strict_ready"] is False
    assert "package_status=DRAFT" in eligibility["blockers"]
    assert "manifest_identity" not in eligibility["satisfied_gates"]
    assert eligibility["manifest_identity"]["passed"] is False
    assert eligibility["manifest_identity"]["package_status"] == PackageStatus.DRAFT.value
