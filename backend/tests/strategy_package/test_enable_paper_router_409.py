"""Router-layer mapping tests for enable_paper HTTP endpoint (T8-C + R6).

T7 audit (commit adb362e) §5 / T9 corrections: `InvalidStateTransitionError`
must surface as HTTP 409 Conflict (state-machine violation), not the generic
HTTP 400 used for `StrategyPackageValidationError`. These tests guard against
regression and against the inverse mistake of collapsing both error classes
into 409.

R6 (codex/qe-governance) adds the governance enable_paper gate, which adds
a second 400 invariant: paper_ready blockers (e.g. missing fixed_weight
retest) must keep mapping to 400, separate from state-machine 409.

Per T9 audit-drift §6: the only path that reaches the state-machine check
without first being rejected by `validate_manifest_identity_for_paper_trading`
is `PAPER_ENABLED` re-entry (already-enabled package).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import strategy_packages as router_module
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.package_asset import StrategyPackageAssetType
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository,
    StrategyPackageRecord,
)
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
    """Wire the strategy_packages router on top of an in-memory repository.

    The router constructs `StrategyPackageService()` per-request without DI, so
    we monkey-patch the symbol the router imports. The patched factory returns
    a service bound to a single shared in-memory repo across requests.
    """

    repo = InMemoryStrategyPackageRepository()

    def _factory(*args, **kwargs):  # noqa: ANN001 -- mirror real signature
        return StrategyPackageService(repository=repo)

    monkeypatch.setattr(router_module, "StrategyPackageService", _factory)

    app = FastAPI()
    app.include_router(router_module.router)
    return app, repo


def _seed_paper_ready_package(service: StrategyPackageService, package_id: str) -> None:
    """Seed a package with all governance prerequisites for enable_paper (R6).

    Adds: model weight asset + risk-policy variant (validated, paper candidate) +
    original fixed-weight retest + 2 fixed-seed retests, with bull/bear regime
    metrics. This is the minimum surface needed to pass the
    `validate_manifest_identity_for_paper_trading` governance gate.
    """
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
    """PAPER_ENABLED re-entry -> 409 Conflict with state-transition context.

    This is the unique path that triggers `InvalidStateTransitionError` per
    T9-corrected audit (DRAFT/ARCHIVED would be caught by the manifest
    validator first). R6 governance: seed paper-ready prereqs first so the
    governance gate passes, then force PAPER_ENABLED to trigger the re-entry.
    """

    app, repo = app_and_repo

    saved_manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(saved_manifest)
    _seed_paper_ready_package(StrategyPackageService(repository=repo), record.package_id)
    repo.records[record.package_id] = repo.get(record.package_id).model_copy(
        update={"package_status": PackageStatus.PAPER_ENABLED}
    )

    client = TestClient(app)
    response = client.post(f"/strategy-packages/{record.package_id}/enable-paper")

    assert response.status_code == 409, response.text
    payload = response.json()
    detail = payload.get("detail")
    assert isinstance(detail, dict), payload
    assert detail.get("error_code") == "INVALID_STATE_TRANSITION"
    context = detail.get("context") or {}
    # Current (offending) status must be discoverable so callers can tell
    # state-race / re-entry from validation errors without parsing the message.
    assert context.get("from_status") == PackageStatus.PAPER_ENABLED.value
    assert context.get("to_status") == PackageStatus.PAPER_ENABLED.value


def test_enable_paper_endpoint_returns_400_on_validation_error(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
) -> None:
    """Sanity guard: `StrategyPackageValidationError` (digest tamper) must keep
    mapping to 400, not get accidentally collapsed into 409 by the
    state-transition rule.

    Trips the manifest sha256 invariant by injecting a record whose embedded
    manifest carries a deliberately wrong digest (mirrors T8-B Test 1
    reverse-engineered path).
    """

    app, repo = app_and_repo

    correct = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    bad_digest = "b" * 64
    assert correct.manifest_sha256 != bad_digest, "fixture digest collision"
    tampered_manifest = correct.model_copy(update={"manifest_sha256": bad_digest})
    now = datetime.now(timezone.utc)
    record = StrategyPackageRecord(
        package_id=tampered_manifest.package_id,
        package_name=tampered_manifest.package_name,
        package_version=tampered_manifest.package_version,
        source_type=tampered_manifest.source.source_type.value,
        source_id=tampered_manifest.source.source_id,
        loop_id=tampered_manifest.source.loop_id,
        run_id=tampered_manifest.source.run_id,
        package_status=PackageStatus.BACKTEST_APPROVED,
        manifest=tampered_manifest,
        manifest_sha256=bad_digest,
        created_at=now,
        updated_at=now,
    )
    repo.records[record.package_id] = record

    client = TestClient(app)
    response = client.post(f"/strategy-packages/{record.package_id}/enable-paper")

    assert response.status_code == 400, response.text
    detail = response.json().get("detail")
    assert isinstance(detail, dict)
    assert detail.get("error_code") == "STRATEGY_PACKAGE_VALIDATION_ERROR"


def test_enable_paper_endpoint_keeps_governance_blockers_at_400(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
) -> None:
    """R6 governance gate: paper_ready=False with missing fixed_weight retest
    must map to 400 (paper_ready blockers), not 409.
    """
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
