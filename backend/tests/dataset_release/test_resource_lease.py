from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.lease import (
    LeaseConflict,
    LeaseManager,
    LeaseToken,
    OrphanNotQuiescent,
)
from backend.services.dataset_release.state_machine import DatasetReleaseStateMachine, IntentSpec


def _queued_run(store: ControlStore, suffix: str) -> dict:
    return DatasetReleaseStateMachine(store).create_queued_run(
        intent=IntentSpec(
            logical_request_key=f"logical-{suffix}",
            resolved_intent_key=f"resolved-{suffix}",
            source_content_root=f"source-{suffix}",
            source_provenance_root=f"provenance-{suffix}",
            pit_snapshot_digest=f"pit-{suffix}",
        ),
        run_generation_digest=f"generation-{suffix}",
        operation_kind="BUILD",
        plan_ref=f"cas:plan-{suffix}",
    )


def test_host_and_release_leases_are_claimed_atomically(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    leases = LeaseManager(store)
    first_run = _queued_run(store, "first")
    second_run = _queued_run(store, "second")

    first = leases.claim_build(
        run_id=first_run["run_id"],
        release_id="release-first",
        owner_identity="worker-1",
        ttl_seconds=60,
    )

    assert first.host is not None and first.release is not None
    assert store.get_lease("host:heavy-dataset")["attempt_id"] == first.attempt_id
    assert store.get_lease("release:release-first")["attempt_id"] == first.attempt_id
    with pytest.raises(LeaseConflict, match="not FREE"):
        leases.claim_build(
            run_id=second_run["run_id"],
            release_id="release-second",
            owner_identity="worker-2",
            ttl_seconds=60,
        )

    assert store._many("SELECT * FROM attempts WHERE run_id=?", (second_run["run_id"],)) == []
    assert store.get_lease("release:release-second") is None
    assert store.get_run(second_run["run_id"])["state"] == "QUEUED"


def test_resolution_and_optional_host_lease_persist_worker_capability_identity(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    submission = store.submit(
        principal="operator",
        route="/runs",
        idempotency_key="resolution-capability",
        request_hash="resolution-capability-request",
        logical_request_key="resolution-capability-logical",
        request_ref="cas:request",
    )
    claim = LeaseManager(store).claim_resolution(
        submission_id=submission["submission_id"],
        owner_identity="resolution-worker",
        ttl_seconds=60,
        acquire_host=True,
        code_sha="code-sha",
        capability_digest="capability-digest",
        requested_ram=4 * 1024**3,
        db_connections=4,
        io_class="source-probe",
    )

    assert claim.resolution is not None and claim.host is not None
    for token in (claim.resolution, claim.host):
        durable = store.get_lease(token.resource_key)
        assert durable["code_sha"] == "code-sha"
        assert durable["capability_digest"] == "capability-digest"
        assert durable["requested_ram"] == 4 * 1024**3
        assert durable["db_connections"] == 4
        assert durable["io_class"] == "source-probe"


def test_stale_fence_cannot_heartbeat_or_release_current_owner(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    manager = LeaseManager(store)
    run = _queued_run(store, "stale")
    claim = manager.claim_build(
        run_id=run["run_id"],
        release_id="release-stale",
        owner_identity="worker",
        ttl_seconds=60,
    )
    assert claim.host is not None
    stale = LeaseToken(
        claim.host.resource_key,
        claim.host.fence + 1,
        claim.host.attempt_id,
        claim.host.owner_identity,
    )

    with pytest.raises(LeaseConflict, match="stale lease heartbeat"):
        manager.heartbeat((stale,), ttl_seconds=60)

    assert store.get_lease("host:heavy-dataset")["fence_counter"] == claim.host.fence
    assert store.get_lease("host:heavy-dataset")["state"] == "ACTIVE"


def test_expiry_alone_never_reclaims_active_lease(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    manager = LeaseManager(store)
    old_run = _queued_run(store, "old")
    new_run = _queued_run(store, "new")
    past = datetime.now(UTC) - timedelta(hours=1)
    manager.claim_build(
        run_id=old_run["run_id"],
        release_id="release-old",
        owner_identity="old-worker",
        ttl_seconds=1,
        now=past,
    )

    with pytest.raises(LeaseConflict, match="not FREE"):
        manager.claim_build(
            run_id=new_run["run_id"],
            release_id="release-new",
            owner_identity="new-worker",
            ttl_seconds=60,
        )

    assert store.get_lease("host:heavy-dataset")["owner_identity"] == "old-worker"


def test_orphan_hold_retains_pointer_and_leases_until_full_tree_quiescent(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    manager = LeaseManager(store)
    run = _queued_run(store, "orphan")
    claim = manager.claim_build(
        run_id=run["run_id"],
        release_id="release-orphan",
        owner_identity="worker",
        ttl_seconds=60,
    )

    manager.mark_orphan_hold(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        tree_status="unknown",
    )

    held = store.get_run(run["run_id"])
    assert held["state"] == "WAITING_ORPHAN_QUIESCENCE"
    assert held["active_attempt_id"] == claim.attempt_id
    assert store.get_lease("host:heavy-dataset")["state"] == "ORPHAN_HOLD"
    with pytest.raises(OrphanNotQuiescent):
        manager.release_orphan_after_quiescence(
            run_id=run["run_id"],
            attempt_id=claim.attempt_id,
            tree_quiescent=False,
        )

    manager.release_orphan_after_quiescence(
        run_id=run["run_id"],
        attempt_id=claim.attempt_id,
        tree_quiescent=True,
    )
    recovered = store.get_run(run["run_id"])
    assert recovered["state"] == "QUEUED"
    assert recovered["active_attempt_id"] is None
    assert store.get_attempt(claim.attempt_id)["state"] == "EXPIRED"
    assert store.get_lease("host:heavy-dataset")["state"] == "FREE"
    assert store.get_lease("release:release-orphan")["state"] == "FREE"

    successor = manager.claim_build(
        run_id=run["run_id"],
        release_id="release-orphan",
        owner_identity="successor",
        ttl_seconds=60,
    )
    assert successor.host.fence == claim.host.fence + 1
    assert successor.release.fence == claim.release.fence + 1


def test_resolution_claim_is_single_active_per_logical_request(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    manager = LeaseManager(store)
    first = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="one",
        request_hash="one",
        logical_request_key="same-logical",
        request_ref="cas:one",
    )
    second = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="two",
        request_hash="two",
        logical_request_key="same-logical",
        request_ref="cas:two",
    )
    manager.claim_resolution(
        submission_id=first["submission_id"],
        owner_identity="resolver-1",
        ttl_seconds=60,
    )

    with pytest.raises(LeaseConflict, match="not FREE"):
        manager.claim_resolution(
            submission_id=second["submission_id"],
            owner_identity="resolver-2",
            ttl_seconds=60,
        )

    assert store.get_submission(second["submission_id"])["state"] == "QUEUED_RESOLUTION"


def test_resolution_orphan_keeps_pointer_until_tree_is_quiescent(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    manager = LeaseManager(store)
    submission = store.submit(
        principal="operator",
        route="runs",
        idempotency_key="orphan",
        request_hash="orphan",
        logical_request_key="orphan-logical",
        request_ref="cas:orphan",
    )
    claim = manager.claim_resolution(
        submission_id=submission["submission_id"],
        owner_identity="resolver",
        ttl_seconds=60,
        acquire_host=True,
    )

    manager.mark_resolution_orphan_hold(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        tree_status="alive",
    )
    held = store.get_submission(submission["submission_id"])
    assert held["state"] == "WAITING_ORPHAN_QUIESCENCE"
    assert held["resolution_attempt_id"] == claim.attempt_id
    assert len(store._many("SELECT * FROM leases WHERE attempt_id=? AND state='ORPHAN_HOLD'", (claim.attempt_id,))) == 2

    with pytest.raises(OrphanNotQuiescent):
        manager.release_resolution_orphan_after_quiescence(
            submission_id=submission["submission_id"],
            resolution_attempt_id=claim.attempt_id,
            tree_quiescent=False,
        )
    manager.release_resolution_orphan_after_quiescence(
        submission_id=submission["submission_id"],
        resolution_attempt_id=claim.attempt_id,
        tree_quiescent=True,
    )
    recovered = store.get_submission(submission["submission_id"])
    assert recovered["state"] == "QUEUED_RESOLUTION"
    assert recovered["resolution_attempt_id"] is None
    assert store.get_resolution_attempt(claim.attempt_id)["state"] == "EXPIRED"
