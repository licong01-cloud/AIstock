from __future__ import annotations

import json
from types import SimpleNamespace
from datetime import UTC, datetime, timedelta

from backend.services.dataset_release.control_store import ControlStore
from backend.services.dataset_release.publisher import DatasetPublisher
from backend.services.dataset_release.resource_supervisor import (
    WSL_GUARDIAN_STATE_SCHEMA,
)
from backend.services.dataset_release.runtime_adapters import (
    DurableWslQuiescenceReader,
    FencedPublishRecoveryAdapter,
    WslSystemdQuiescenceProbe,
)
from backend.services.dataset_release.worker import (
    LeaseOwnerSnapshot,
    ProcessorRegistry,
)
from backend.tests.dataset_release.test_publish_protocol import _prepared_fixture
from backend.tests.dataset_release.test_worker import Clock, _worker


def test_real_publish_recovery_adapter_completes_fenced_handoff(tmp_path) -> None:
    clock = Clock()
    store, _machine, _manager, candidate_root, spec = _prepared_fixture(tmp_path, "real-recovery-adapter")
    publisher = DatasetPublisher(store, candidate_root=candidate_root)
    publisher.prepare(spec)
    with store.transaction() as connection:
        connection.execute(
            "UPDATE leases SET expires_at=? WHERE attempt_id=?",
            ((clock() - timedelta(seconds=1)).isoformat(), spec.attempt_id),
        )
    worker, _ = _worker(
        store,
        clock=clock,
        registry=ProcessorRegistry(publish_recovery=FencedPublishRecoveryAdapter(store, publisher)),
        liveness_probe=lambda _owner: "dead",
    )

    report = worker.run_once()

    assert report.kind == "orphan_publish" and report.state == "SUCCEEDED"
    assert store.get_run(spec.run_id)["state"] == "SUCCEEDED"
    assert store.get_publish_record(spec.release_id)["state"] == "COMMITTED"


def test_durable_wsl_reader_requires_attempt_fence_bound_terminal_state(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    observed = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)
    attempt_id = "dsa_guardian_fixture"
    fence = 7
    with store.transaction() as connection:
        connection.execute(
            """
            INSERT INTO attempts(
                attempt_id,run_id,ordinal,attempt_kind,state,owner,attempt_fence,
                created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                attempt_id,
                "dsrun_guardian_fixture",
                fence,
                "BUILD",
                "ORPHAN_HOLD",
                "fixture",
                fence,
                observed.isoformat(),
                observed.isoformat(),
            ),
        )
    owner = LeaseOwnerSnapshot(
        attempt_id=attempt_id,
        attempt_kind="BUILD",
        owner_identity="fixture",
        host="fixture",
        owner_pid=123,
        owner_create_time=observed.isoformat(),
        worker_instance_id="dsw_" + "a" * 32,
        code_sha="b" * 40,
        capability_digest="c" * 64,
        hybrid_wsl=True,
        expires_at=observed.isoformat(),
        lease_state="ORPHAN_HOLD",
    )
    reader = DurableWslQuiescenceReader(store, active_ttl_seconds=5, now=lambda: observed)
    assert reader(owner) == "unknown"

    state_root = store.root / "guardian_states"
    state_root.mkdir()
    state_path = state_root / f"{attempt_id}-{fence}.json"
    base = {
        "schema_version": WSL_GUARDIAN_STATE_SCHEMA,
        "attempt_id": attempt_id,
        "fence": fence,
        "state": "ACTIVE",
        "launch_count": 1,
        "execution_id": "qlib-dump-daily",
        "unit": f"aistock-dataset-{attempt_id}-{fence}.service",
        "distro": "Ubuntu",
        "control_group": "/user.slice/fixture.service",
        "systemd_wait_completed": False,
        "active_processes": 1,
        "observed_utc": observed.isoformat(),
    }
    state_path.write_text(json.dumps(base), encoding="utf-8")
    assert reader(owner) == "active"

    stale = {**base, "observed_utc": (observed - timedelta(seconds=6)).isoformat()}
    state_path.write_text(json.dumps(stale), encoding="utf-8")
    assert reader(owner) == "unknown"

    terminal = {
        **base,
        "state": "QUIESCENT",
        "systemd_wait_completed": True,
        "active_processes": 0,
        "observed_utc": (observed - timedelta(days=1)).isoformat(),
    }
    state_path.write_text(json.dumps(terminal), encoding="utf-8")
    assert reader(owner) == "quiescent"

    wrong_fence = {**terminal, "fence": fence - 1}
    state_path.write_text(json.dumps(wrong_fence), encoding="utf-8")
    assert reader(owner) == "unknown"


def test_stale_active_requires_exact_recovery_proof_and_seals_receipt(tmp_path) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    observed = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)
    attempt_id = "dsa_crash_after_active"
    fence = 3
    with store.transaction() as connection:
        connection.execute(
            """
            INSERT INTO attempts(
                attempt_id,run_id,ordinal,attempt_kind,state,owner,attempt_fence,
                created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                attempt_id,
                "dsrun_crash_after_active",
                fence,
                "BUILD",
                "ORPHAN_HOLD",
                "fixture",
                fence,
                observed.isoformat(),
                observed.isoformat(),
            ),
        )
    state_root = store.root / "guardian_states"
    state_root.mkdir()
    group = "/user.slice/user-1000.slice/user@1000.service/app.slice/fixture.service"
    state = {
        "schema_version": WSL_GUARDIAN_STATE_SCHEMA,
        "attempt_id": attempt_id,
        "fence": fence,
        "state": "ACTIVE",
        "launch_count": 1,
        "execution_id": "qlib-dump-minute",
        "unit": f"aistock-dataset-{attempt_id}-{fence}.service",
        "distro": "Ubuntu",
        "control_group": group,
        "systemd_wait_completed": False,
        "active_processes": 1,
        "observed_utc": (observed - timedelta(minutes=1)).isoformat(),
    }
    (state_root / f"{attempt_id}-{fence}.json").write_text(json.dumps(state), encoding="utf-8")
    owner = LeaseOwnerSnapshot(
        attempt_id=attempt_id,
        attempt_kind="BUILD",
        owner_identity="fixture",
        host="fixture",
        owner_pid=123,
        owner_create_time=observed.isoformat(),
        worker_instance_id="dsw_" + "a" * 32,
        code_sha="b" * 40,
        capability_digest="c" * 64,
        hybrid_wsl=True,
        expires_at=observed.isoformat(),
        lease_state="ORPHAN_HOLD",
    )
    active = DurableWslQuiescenceReader(
        store,
        active_ttl_seconds=5,
        now=lambda: observed,
        recovery_probe=lambda _state: {
            "state": "active",
            "unit_state": "active",
            "control_group": group,
        },
    )
    assert active(owner) == "active"

    failed = DurableWslQuiescenceReader(
        store,
        active_ttl_seconds=5,
        now=lambda: observed,
        recovery_probe=lambda _state: (_ for _ in ()).throw(OSError("denied")),
    )
    assert failed(owner) == "unknown"

    recovered = DurableWslQuiescenceReader(
        store,
        active_ttl_seconds=5,
        now=lambda: observed,
        recovery_probe=lambda _state: {
            "state": "quiescent",
            "unit_state": "inactive",
            "cgroup_state": "absent",
            "control_group": group,
        },
    )
    assert recovered(owner) == "quiescent"
    receipt = store.root / "guardian_recovery_states" / f"{attempt_id}-{fence}.json"
    assert receipt.is_file()
    no_second_probe = DurableWslQuiescenceReader(
        store,
        active_ttl_seconds=5,
        now=lambda: observed + timedelta(days=1),
        recovery_probe=lambda _state: (_ for _ in ()).throw(AssertionError("durable receipt must be sufficient")),
    )
    assert no_second_probe(owner) == "quiescent"


def test_systemd_probe_requires_inactive_unit_and_empty_or_absent_exact_cgroup() -> None:
    group = "/user.slice/fixture.service"
    state = {
        "unit": "aistock-dataset-dsa_fixture-2.service",
        "distro": "Ubuntu",
        "control_group": group,
    }
    calls = []
    responses = iter(
        [
            SimpleNamespace(
                returncode=0,
                stdout=("LoadState=loaded\nActiveState=inactive\nSubState=dead\nControlGroup=\nMainPID=0\n"),
                stderr="",
            ),
            SimpleNamespace(
                returncode=0,
                stdout=json.dumps(
                    {
                        "schema_version": "dataset_release_wsl_cgroup_quiescence_v1",
                        "control_group": group,
                        "state": "empty",
                        "populated": 0,
                        "process_count": 0,
                    }
                ),
                stderr="",
            ),
        ]
    )

    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return next(responses)

    probe = WslSystemdQuiescenceProbe(
        expected_distro="Ubuntu",
        guardian_python="/usr/bin/python3",
        guardian_script_wsl="/repo/wsl_resource_guardian.py",
        runner=runner,
    )
    result = probe(state)

    assert result["state"] == "quiescent" and result["cgroup_state"] == "empty"
    assert calls[0][0][:4] == ["wsl.exe", "-d", "Ubuntu", "--"]
    assert "--read-quiescence" in calls[1][0]


def test_systemd_probe_closes_collected_unit_before_control_group_readback() -> None:
    unit = "aistock-dataset-dsa_pre_readback-5.service"
    state = {"unit": unit, "distro": "Ubuntu", "control_group": None}
    responses = iter(
        [
            SimpleNamespace(
                returncode=4,
                stdout="",
                stderr=f"Unit {unit} could not be found.\n",
            )
        ]
    )
    probe = WslSystemdQuiescenceProbe(
        expected_distro="Ubuntu",
        guardian_python="/usr/bin/python3",
        guardian_script_wsl="/repo/wsl_resource_guardian.py",
        runner=lambda _command, **_kwargs: next(responses),
    )

    assert probe(state) == {
        "state": "quiescent",
        "unit_state": "inactive",
        "load_state": "not-found",
        "sub_state": "dead",
        "control_group": None,
        "cgroup_state": "unit-not-found",
    }


def test_systemd_probe_pre_readback_active_unit_is_not_reclaimed() -> None:
    unit = "aistock-dataset-dsa_pre_readback-5.service"
    group = "/user.slice/discovered.service"
    probe = WslSystemdQuiescenceProbe(
        expected_distro="Ubuntu",
        guardian_python="/usr/bin/python3",
        guardian_script_wsl="/repo/wsl_resource_guardian.py",
        runner=lambda _command, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=(f"LoadState=loaded\nActiveState=active\nSubState=running\nControlGroup={group}\nMainPID=42\n"),
            stderr="",
        ),
    )

    assert probe({"unit": unit, "distro": "Ubuntu", "control_group": None}) == {
        "state": "active",
        "unit_state": "active",
        "control_group": group,
    }


def test_reader_recovers_stale_active_written_before_control_group_readback(
    tmp_path,
) -> None:
    store = ControlStore.initialize(tmp_path / "control")
    observed = datetime(2026, 8, 11, 9, 0, tzinfo=UTC)
    attempt_id, fence = "dsa_pre_readback_crash", 4
    with store.transaction() as connection:
        connection.execute(
            """
            INSERT INTO attempts(
                attempt_id,run_id,ordinal,attempt_kind,state,owner,attempt_fence,
                created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                attempt_id,
                "dsrun_pre_readback_crash",
                fence,
                "BUILD",
                "ORPHAN_HOLD",
                "fixture",
                fence,
                observed.isoformat(),
                observed.isoformat(),
            ),
        )
    root = store.root / "guardian_states"
    root.mkdir()
    (root / f"{attempt_id}-{fence}.json").write_text(
        json.dumps(
            {
                "schema_version": WSL_GUARDIAN_STATE_SCHEMA,
                "attempt_id": attempt_id,
                "fence": fence,
                "state": "ACTIVE",
                "launch_count": 1,
                "execution_id": "qlib-dump-daily",
                "unit": f"aistock-dataset-{attempt_id}-{fence}.service",
                "distro": "Ubuntu",
                "control_group": None,
                "systemd_wait_completed": False,
                "active_processes": 1,
                "observed_utc": (observed - timedelta(minutes=1)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    owner = LeaseOwnerSnapshot(
        attempt_id=attempt_id,
        attempt_kind="BUILD",
        owner_identity="fixture",
        host="fixture",
        owner_pid=123,
        owner_create_time=observed.isoformat(),
        worker_instance_id="dsw_" + "a" * 32,
        code_sha="b" * 40,
        capability_digest="c" * 64,
        hybrid_wsl=True,
        expires_at=observed.isoformat(),
        lease_state="ORPHAN_HOLD",
    )
    reader = DurableWslQuiescenceReader(
        store,
        active_ttl_seconds=5,
        now=lambda: observed,
        recovery_probe=lambda _state: {
            "state": "quiescent",
            "unit_state": "inactive",
            "cgroup_state": "unit-not-found",
            "control_group": None,
        },
    )

    assert reader(owner) == "quiescent"
