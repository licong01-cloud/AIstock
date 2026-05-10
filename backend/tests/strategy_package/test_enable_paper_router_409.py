"""Router-layer mapping tests for enable_paper HTTP endpoint (T8-C).

T7 audit (commit adb362e) §5 / T9 corrections: `InvalidStateTransitionError`
must surface as HTTP 409 Conflict (state-machine violation), not the generic
HTTP 400 used for `StrategyPackageValidationError`. These tests guard against
regression and against the inverse mistake of collapsing both error classes
into 409.

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
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository,
    StrategyPackageRecord,
)
from backend.services.strategy_package.service import StrategyPackageService
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
        # Caller of StrategyPackageService() in the router takes no kwargs; we
        # ignore any positional args defensively.
        return StrategyPackageService(repository=repo)

    monkeypatch.setattr(router_module, "StrategyPackageService", _factory)

    app = FastAPI()
    app.include_router(router_module.router)
    return app, repo


def test_enable_paper_endpoint_returns_409_on_invalid_transition(
    app_and_repo: tuple[FastAPI, InMemoryStrategyPackageRepository],
) -> None:
    """PAPER_ENABLED re-entry -> 409 Conflict with state-transition context.

    This is the unique path that triggers `InvalidStateTransitionError` per
    T9-corrected audit (DRAFT/ARCHIVED would be caught by the manifest
    validator first).
    """

    app, repo = app_and_repo

    # Seed: create a properly-frozen package, then force its persisted status
    # to PAPER_ENABLED. The validator's allowed-set
    # {BACKTEST_APPROVED, SELECTION_ENABLED, PAPER_ENABLED} accepts this, but
    # STATUS_TRANSITIONS[PAPER_ENABLED] = {BACKTEST_APPROVED, SELECTION_ENABLED}
    # rejects re-entry at repository.transition_status.
    saved_manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(saved_manifest)
    repo.records[record.package_id] = record.model_copy(
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
    """Sanity guard: `StrategyPackageValidationError` must keep mapping to 400,
    not get accidentally collapsed into 409 by the new state-transition rule.

    We trip the manifest sha256 invariant by injecting a record whose embedded
    manifest carries a deliberately wrong digest (mirrors the T8-B Test 1
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
