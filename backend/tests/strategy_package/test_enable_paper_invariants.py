"""Invariant tests for enable_paper() lifecycle behavior.

enable_paper() is a real StrategyPackage lifecycle transition again: asset
identity remains fail-fast and already-enabled re-entry is rejected with
state-machine context instead of being treated as compatibility no-op success.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import (
    InMemoryStrategyPackageRepository,
    StrategyPackageRecord,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import (
    InvalidStateTransitionError,
    StrategyPackageValidationError,
)
from backend.tests.strategy_package.test_enable_paper_router_409 import (
    _seed_paper_ready_package,
)
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def test_enable_paper_raises_on_manifest_sha256_mismatch() -> None:
    """enable_paper must fail-fast when stored manifest_sha256 disagrees with
    the recomputed payload digest. Locks §4.3 row D of the T7 audit.

    The validator (validators.py:32-42) recomputes
    compute_manifest_sha256(manifest) and compares to manifest.manifest_sha256;
    mismatch raises StrategyPackageValidationError. We reach that branch by
    injecting a record whose embedded manifest carries a deliberately wrong
    digest into the in-memory repository directly (bypassing save_manifest,
    which would re-freeze and overwrite the digest).

    The validator's sha-mismatch error is caught inside the alpha-core
    admission gate and surfaced as a blocker string. Live-strict governance
    evidence is no longer required for Paper simulation, but manifest identity
    remains a hard package eligibility gate.
    """

    repo = InMemoryStrategyPackageRepository()
    # Build a properly-frozen manifest so the rest of the payload is valid,
    # then mutate manifest_sha256 to a value that cannot match recomputation.
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

    service = StrategyPackageService(repository=repo)

    # Seed governance evidence to prove manifest identity remains the
    # blocking condition even when live-strict evidence exists.
    _seed_paper_ready_package(service, record.package_id)

    with pytest.raises(StrategyPackageValidationError) as exc_info:
        service.enable_paper(record.package_id)

    err = exc_info.value
    context = err.context or {}
    blockers_str = " ".join(context.get("blockers") or [])
    assert "alpha core" in str(err).lower()
    assert context.get("eligible") is False
    # Underlying sha mismatch must remain discoverable in the admission context
    # so reviewers can tell tampering from drift.
    assert "manifest_sha256" in blockers_str.lower() or "hash" in blockers_str.lower()
    assert context.get("manifest_sha256") == bad_digest
    assert str(context.get("status")).lower() == "blocked"


def test_enable_paper_rejects_already_enabled_reentry_with_context() -> None:
    """PAPER_ENABLED is an active lifecycle state; re-entry is not silent."""
    repo = InMemoryStrategyPackageRepository()
    saved_manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(saved_manifest)

    service = StrategyPackageService(repository=repo)
    # Seed read-only governance fixtures so this regression stays focused on
    # the PAPER_ENABLED re-entry state-machine raise point.
    _seed_paper_ready_package(service, record.package_id)

    # Force the persisted package_status to PAPER_ENABLED. current_manifest()
    # overlays this onto the manifest, so the validator's manifest-status gate
    # still sees an allowed value and we land in the state-machine check.
    repo.records[record.package_id] = repo.get(record.package_id).model_copy(
        update={"package_status": PackageStatus.PAPER_ENABLED}
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.enable_paper(record.package_id)

    err = exc_info.value
    assert err.context["package_id"] == record.package_id
    assert err.context["from_status"] == PackageStatus.PAPER_ENABLED.value
    assert err.context["to_status"] == PackageStatus.PAPER_ENABLED.value
    assert err.context["allowed_from"] == [
        PackageStatus.BACKTEST_APPROVED.value,
        PackageStatus.SELECTION_ENABLED.value,
    ]
