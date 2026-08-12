from __future__ import annotations

from datetime import UTC, datetime

from backend.services.dataset_release.process_liveness import (
    LocalProcessTreeLivenessProbe,
    ProcessSnapshot,
)
from backend.services.dataset_release.worker import LeaseOwnerSnapshot


CREATED = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)
INSTANCE = "dsw_" + "a" * 32
CAPABILITY = "b" * 64
CODE_SHA = "c" * 40


def _owner(**changes) -> LeaseOwnerSnapshot:
    values = {
        "attempt_id": "attempt-one",
        "attempt_kind": "BUILD",
        "owner_identity": f"{INSTANCE}:{CAPABILITY[:16]}",
        "host": "fixture-host",
        "owner_pid": 4242,
        "owner_create_time": CREATED.isoformat(timespec="microseconds"),
        "worker_instance_id": INSTANCE,
        "code_sha": CODE_SHA,
        "capability_digest": CAPABILITY,
        "hybrid_wsl": False,
        "expires_at": "2026-08-11T09:01:00.000000+00:00",
        "lease_state": "ORPHAN_HOLD",
    }
    values.update(changes)
    return LeaseOwnerSnapshot(**values)


def _heartbeat(**identity_changes):
    identity = {
        "instance_id": INSTANCE,
        "host": "fixture-host",
        "pid": 4242,
        "process_create_time": CREATED.isoformat(timespec="microseconds"),
        "code_sha": CODE_SHA,
        "capability_digest": CAPABILITY,
    }
    identity.update(identity_changes)
    return {"identity": identity, "status": "IDLE"}


def _probe(rows, **kwargs) -> LocalProcessTreeLivenessProbe:
    return LocalProcessTreeLivenessProbe(
        identity_reader=lambda _instance: _heartbeat(),
        snapshot_reader=lambda: tuple(rows),
        local_host="fixture-host",
        **kwargs,
    )


def test_exact_pid_create_time_and_identity_is_alive() -> None:
    probe = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(4242, 1, CREATED.timestamp()),
        ]
    )

    assert probe(_owner()) == "alive"


def test_missing_root_is_dead_only_after_complete_windows_tree_snapshot() -> None:
    probe = _probe([ProcessSnapshot(1, 0, CREATED.timestamp() - 100)])

    assert probe(_owner()) == "dead"


def test_detached_descendant_keeps_orphan_alive() -> None:
    probe = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(5000, 4242, CREATED.timestamp() + 1),
        ]
    )

    assert probe(_owner()) == "alive"


def test_remote_host_pid_reuse_or_heartbeat_mismatch_is_unknown() -> None:
    reused = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(4242, 1, CREATED.timestamp() + 60),
        ]
    )
    mismatched_heartbeat = LocalProcessTreeLivenessProbe(
        identity_reader=lambda _instance: _heartbeat(capability_digest="d" * 64),
        snapshot_reader=lambda: (ProcessSnapshot(1, 0, CREATED.timestamp() - 100),),
        local_host="fixture-host",
    )

    assert reused(_owner()) == "unknown"
    assert reused(_owner(host="another-host")) == "unknown"
    assert mismatched_heartbeat(_owner()) == "unknown"


def test_incomplete_process_or_wsl_evidence_is_unknown_and_never_dead() -> None:
    unavailable = LocalProcessTreeLivenessProbe(
        identity_reader=lambda _instance: _heartbeat(),
        snapshot_reader=lambda: (_ for _ in ()).throw(RuntimeError("access denied")),
        local_host="fixture-host",
    )
    no_wsl_evidence = _probe([ProcessSnapshot(1, 0, CREATED.timestamp() - 100)])
    active_wsl = _probe(
        [ProcessSnapshot(1, 0, CREATED.timestamp() - 100)],
        wsl_quiescence_reader=lambda _owner: "active",
    )
    quiescent_wsl = _probe(
        [ProcessSnapshot(1, 0, CREATED.timestamp() - 100)],
        wsl_quiescence_reader=lambda _owner: "quiescent",
    )

    assert unavailable(_owner()) == "unknown"
    assert no_wsl_evidence(_owner(hybrid_wsl=True)) == "unknown"
    assert active_wsl(_owner(hybrid_wsl=True)) == "alive"
    assert quiescent_wsl(_owner(hybrid_wsl=True)) == "dead"


def test_unrelated_access_limited_process_only_blocks_when_it_could_belong_to_tree() -> None:
    old_unrelated = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(9000, None, CREATED.timestamp() - 10),
        ]
    )
    possible_descendant = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(9000, None, None),
        ]
    )

    assert old_unrelated(_owner()) == "dead"
    assert possible_descendant(_owner()) == "unknown"
