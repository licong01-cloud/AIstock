"""Invariant tests for enable_paper() fail-fast behavior.

Per T7 audit (commit adb362e + cross-tool drawer 7ee0dfe3): enable_paper() is
currently fail-fast clean (3 fail-fast / 0 silent-swallow / 0 mixed). These
tests guard against future regression to silent-fallback on:
  (a) manifest_sha256 mismatch
  (b) invalid status state transition

Out of scope: validation_status / paper_candidate / retest gate wiring (T8-A,
blocked on Codex Phase 3 schema integration into backend/services/validation/).
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

    R6: governance gate prereq seed; the new
    ``_require_governance_paper_ready`` gate now intercepts before the legacy
    raise point. The validator's sha-mismatch error is caught inside
    ``_manifest_identity_gate`` and surfaced as a blocker string in the
    governance eligibility context. We assert against the wrapped governance
    error: error message identifies the governance gate, and
    ``context["manifest_identity"]`` records the underlying sha mismatch (via
    its ``blockers`` list and the persisted ``manifest_sha256``).
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

    # R6: seed governance gate prereqs so we exercise the manifest-identity
    # blocker path rather than getting blocked by missing prereqs first.
    _seed_paper_ready_package(service, record.package_id)

    with pytest.raises(StrategyPackageValidationError) as exc_info:
        service.enable_paper(record.package_id)

    # R6: governance gate wraps the validator's sha-mismatch error.
    err = exc_info.value
    context = err.context or {}
    manifest_identity = context.get("manifest_identity") or {}
    blockers_str = " ".join(manifest_identity.get("blockers") or [])
    # Top-level error must surface governance gate failure.
    assert "governance" in str(err).lower() or "paper_ready" in str(err).lower()
    # Underlying sha mismatch must remain discoverable in the eligibility
    # context so reviewers can tell tampering from drift.
    assert "manifest_sha256" in blockers_str.lower() or "sha" in blockers_str.lower()
    assert manifest_identity.get("manifest_sha256") == bad_digest
    assert manifest_identity.get("passed") is False


def test_enable_paper_raises_on_invalid_status_transition() -> None:
    """enable_paper must fail-fast when the persisted package_status is not in
    STATUS_TRANSITIONS[PAPER_ENABLED] = {BACKTEST_APPROVED, SELECTION_ENABLED}.
    Locks §4.3 row E of the T7 audit.

    Audit-drift note: T7 §4.3 E example used 'DRAFT -> PAPER_ENABLED'. In the
    actual source, a DRAFT record's manifest carries package_status=DRAFT,
    which trips StrategyPackageValidator.validate_manifest_identity_for_paper_trading
    (validators.py:62-73) FIRST, raising StrategyPackageValidationError before
    the state-machine compare-and-set is reached. To assert specifically against
    InvalidStateTransitionError (the state-machine invariant), we use an already
    PAPER_ENABLED record: the validator's allowed-set
    {BACKTEST_APPROVED, SELECTION_ENABLED, PAPER_ENABLED} accepts it, but
    STATUS_TRANSITIONS[PAPER_ENABLED] = {BACKTEST_APPROVED, SELECTION_ENABLED}
    rejects re-entry, raising InvalidStateTransitionError at
    repository.transition_status (repository.py:678).
    """

    repo = InMemoryStrategyPackageRepository()
    saved_manifest = freeze_manifest(
        make_manifest().model_copy(update={"package_status": PackageStatus.BACKTEST_APPROVED})
    )
    record = repo.save_manifest(saved_manifest)

    service = StrategyPackageService(repository=repo)
    # R6: seed governance gate prereqs so the new
    # ``_require_governance_paper_ready`` gate passes and we reach the
    # legacy state-machine compare-and-set raise point.
    _seed_paper_ready_package(service, record.package_id)

    # Force the persisted package_status to PAPER_ENABLED. current_manifest()
    # overlays this onto the manifest, so the validator's manifest-status gate
    # still sees an allowed value and we land in the state-machine check.
    repo.records[record.package_id] = repo.get(record.package_id).model_copy(
        update={"package_status": PackageStatus.PAPER_ENABLED}
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.enable_paper(record.package_id)

    message = str(exc_info.value)
    context = exc_info.value.context
    # The current (offending) status name must appear so operators can diagnose.
    assert PackageStatus.PAPER_ENABLED.value in message or context.get("from_status") == PackageStatus.PAPER_ENABLED.value
    # The error must hint at the expected/allowed status set OR carry the
    # target status in context (guarded against future refactors that drop
    # diagnostics).
    allowed_hints = {"expected", "allowed", "valid", "BACKTEST_APPROVED", "SELECTION_ENABLED"}
    has_hint_in_message = any(hint.lower() in message.lower() for hint in allowed_hints)
    has_hint_in_context = context.get("to_status") == PackageStatus.PAPER_ENABLED.value
    assert has_hint_in_message or has_hint_in_context
