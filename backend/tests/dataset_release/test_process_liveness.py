from __future__ import annotations

from datetime import UTC, datetime

from backend.services.dataset_release.process_liveness import (
    LocalProcessTreeLivenessProbe,
    ProcessSnapshot,
    read_complete_process_snapshot,
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


def _heartbeat(
    *,
    status: str = "IDLE",
    claim_kind: str | None = None,
    claim_id: str | None = None,
    stop_requested: bool = False,
    **identity_changes,
):
    identity = {
        "instance_id": INSTANCE,
        "host": "fixture-host",
        "pid": 4242,
        "process_create_time": CREATED.isoformat(timespec="microseconds"),
        "code_sha": CODE_SHA,
        "capability_digest": CAPABILITY,
    }
    identity.update(identity_changes)
    return {
        "identity": identity,
        "status": status,
        "claim_kind": claim_kind,
        "claim_id": claim_id,
        "stop_requested": stop_requested,
    }


def _probe(rows, *, heartbeat=None, **kwargs) -> LocalProcessTreeLivenessProbe:
    heartbeat_payload = _heartbeat() if heartbeat is None else heartbeat
    return LocalProcessTreeLivenessProbe(
        identity_reader=lambda _instance: heartbeat_payload,
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


def test_windows_idle_pid_zero_is_excluded_from_complete_snapshot(monkeypatch) -> None:
    class FakeProcess:
        def __init__(self, pid: int, ppid: int, create_time: float) -> None:
            self.pid = pid
            self.info = {"pid": pid, "ppid": ppid, "create_time": create_time}

    monkeypatch.setattr(
        "backend.services.dataset_release.process_liveness.psutil.process_iter",
        lambda **_kwargs: iter(
            [
                FakeProcess(0, 0, CREATED.timestamp() - 1000),
                FakeProcess(4, 0, 0.0),
                FakeProcess(1, 0, CREATED.timestamp() - 100),
            ]
        ),
    )

    assert read_complete_process_snapshot() == (
        ProcessSnapshot(4, 0, None),
        ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
    )


def test_exact_current_worker_orphan_claim_is_quiescent_only_after_children_exit() -> None:
    heartbeat = _heartbeat(
        status="WAITING_ORPHAN_QUIESCENCE",
        claim_kind="orphan_resolution",
        claim_id="attempt-one",
    )
    quiescent = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(4242, 1, CREATED.timestamp()),
        ],
        heartbeat=heartbeat,
    )
    active_child = _probe(
        [
            ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
            ProcessSnapshot(4242, 1, CREATED.timestamp()),
            ProcessSnapshot(5000, 4242, CREATED.timestamp() + 1),
        ],
        heartbeat=heartbeat,
    )

    owner = _owner(attempt_kind="RESOLUTION")
    assert quiescent(owner) == "quiescent"
    assert active_child(owner) == "alive"


def test_current_worker_orphan_reclaim_requires_exact_heartbeat_claim() -> None:
    rows = [
        ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
        ProcessSnapshot(4242, 1, CREATED.timestamp()),
    ]
    wrong_attempt = _probe(
        rows,
        heartbeat=_heartbeat(
            status="WAITING_ORPHAN_QUIESCENCE",
            claim_kind="orphan_resolution",
            claim_id="another-attempt",
        ),
    )
    wrong_kind = _probe(
        rows,
        heartbeat=_heartbeat(
            status="WAITING_ORPHAN_QUIESCENCE",
            claim_kind="orphan_build",
            claim_id="attempt-one",
        ),
    )
    stopping = _probe(
        rows,
        heartbeat=_heartbeat(
            status="WAITING_ORPHAN_QUIESCENCE",
            claim_kind="orphan_resolution",
            claim_id="attempt-one",
            stop_requested=True,
        ),
    )

    owner = _owner(attempt_kind="RESOLUTION")
    assert wrong_attempt(owner) == "alive"
    assert wrong_kind(owner) == "alive"
    assert stopping(owner) == "unknown"


def test_current_worker_orphan_reclaim_requires_wsl_quiescence() -> None:
    heartbeat = _heartbeat(
        status="WAITING_ORPHAN_QUIESCENCE",
        claim_kind="orphan_resolution",
        claim_id="attempt-one",
    )
    rows = [
        ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
        ProcessSnapshot(4242, 1, CREATED.timestamp()),
    ]
    active = _probe(rows, heartbeat=heartbeat, wsl_quiescence_reader=lambda _owner: "active")
    unknown = _probe(rows, heartbeat=heartbeat)
    quiescent = _probe(rows, heartbeat=heartbeat, wsl_quiescence_reader=lambda _owner: "quiescent")

    owner = _owner(attempt_kind="RESOLUTION", hybrid_wsl=True)
    assert active(owner) == "alive"
    assert unknown(owner) == "unknown"
    assert quiescent(owner) == "quiescent"


def test_current_worker_orphan_claim_maps_build_and_publish_states_exactly() -> None:
    rows = [
        ProcessSnapshot(1, 0, CREATED.timestamp() - 100),
        ProcessSnapshot(4242, 1, CREATED.timestamp()),
    ]
    build = _probe(
        rows,
        heartbeat=_heartbeat(
            status="WAITING_ORPHAN_QUIESCENCE",
            claim_kind="orphan_build",
            claim_id="attempt-one",
        ),
    )
    publish = _probe(
        rows,
        heartbeat=_heartbeat(
            status="WAITING_PUBLISH_RECOVERY",
            claim_kind="orphan_publish",
            claim_id="attempt-one",
        ),
    )

    owner = _owner(attempt_kind="BUILD")
    assert build(owner) == "quiescent"
    assert publish(owner) == "quiescent"


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
    malformed_heartbeat = LocalProcessTreeLivenessProbe(
        identity_reader=lambda _instance: "not-a-heartbeat",
        snapshot_reader=lambda: (ProcessSnapshot(1, 0, CREATED.timestamp() - 100),),
        local_host="fixture-host",
    )

    assert reused(_owner()) == "unknown"
    assert reused(_owner(host="another-host")) == "unknown"
    assert mismatched_heartbeat(_owner()) == "unknown"
    assert malformed_heartbeat(_owner()) == "unknown"


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
