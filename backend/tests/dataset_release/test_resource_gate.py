from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from backend.services.dataset_release.resource_budget import GIB, HostMemorySnapshot
from backend.services.dataset_release.resource_budget import ResourceAdmissionClass
from backend.services.dataset_release.resource_gate import (
    ChildResourceCheckpoint,
    DiskSpaceGuard,
    DiskSpaceSnapshot,
    OwnedRuntimeSnapshot,
    RESOURCE_CHECKPOINT_SIGNAL_SCHEMA,
    ResourceCheckpointRequested,
    ResourceGate,
    WslRuntimeSnapshot,
)


def _host(*, available: int = 32 * GIB, headroom: int = 32 * GIB) -> HostMemorySnapshot:
    limit = 80 * GIB
    return HostMemorySnapshot(
        observed_monotonic=1.0,
        available_bytes=available,
        commit_total_bytes=limit - headroom,
        commit_limit_bytes=limit,
        pagefile_used_bytes=2 * GIB,
        pagefile_limit_bytes=32 * GIB,
        page_reads_per_second=0.0,
        low_memory_signaled=False,
    )


def _disk(_predicted: int | None = None, *, free: int = 128 * GIB) -> DiskSpaceSnapshot:
    return DiskSpaceSnapshot(
        control_free_bytes=free,
        candidate_free_bytes=free,
        effective_free_bytes=free,
        required_free_bytes=32 * GIB,
        predicted_remaining_new_bytes=_predicted,
        same_volume=True,
    )


def test_gate_requires_two_host_ready_samples_before_data_child_admission(
    dataset_profile,
) -> None:
    samples = []
    gate = ResourceGate(
        dataset_profile,
        host_probe=lambda: samples.append(_host()) or samples[-1],
        disk_probe=_disk,
        sleep=lambda _seconds: None,
    )

    admitted = gate.admit("source-stage", wsl_required=False, pressure_rung=0)

    assert admitted.decision.status == "READY"
    assert len(samples) == 2
    receipt = gate.receipt()
    assert receipt["sample_count"] == 2
    assert receipt["data_scope_changed"] is False


def test_gate_receipt_discloses_effective_sample_admission_thresholds(dataset_profile) -> None:
    gate = ResourceGate(
        dataset_profile,
        host_probe=lambda: _host(available=13 * GIB, headroom=13 * GIB),
        disk_probe=_disk,
        admission_class=ResourceAdmissionClass.SAMPLE,
        sleep=lambda _seconds: None,
    )

    admitted = gate.admit("sample-admission", wsl_required=False, pressure_rung=0)
    receipt = gate.receipt()

    assert admitted.decision.status == "READY"
    assert receipt["admission_class"] == "sample"
    assert receipt["effective_host_start_available_bytes"] == 12 * GIB
    assert receipt["effective_host_start_commit_headroom_bytes"] == 12 * GIB
    assert receipt["system_admission_thresholds_blocking"] is False


def test_disk_guard_uses_only_one_point_two_five_times_remaining_bytes(tmp_path, dataset_profile) -> None:
    control = tmp_path / "control"
    candidate = tmp_path / "candidate"
    control.mkdir()
    candidate.mkdir()
    profile = replace(
        dataset_profile,
        control_root=control,
        candidate_root=candidate,
    )
    guard = DiskSpaceGuard(
        profile,
        disk_usage=lambda _path: SimpleNamespace(free=60 * GIB),
        volume_probe=lambda _path: "fixture-volume",
    )

    unknown = guard.sample()
    small = guard.checkpoint(1 * GIB)
    ready = guard.checkpoint(40 * GIB)

    assert unknown.required_free_bytes == 0
    assert small.required_free_bytes == math.ceil(1.25 * GIB)
    assert ready.required_free_bytes == 50 * GIB
    with pytest.raises(ResourceCheckpointRequested) as exc:
        guard.checkpoint(60 * GIB)
    assert exc.value.context["reason_code"] == "RESOURCE_DISK_RESERVE"
    assert exc.value.context["disk_required_free_bytes"] == 75 * GIB
    assert exc.value.context["data_scope_changed"] is False


def test_system_pressure_is_receipt_warning_not_admission_block(dataset_profile) -> None:
    gate = ResourceGate(
        dataset_profile,
        host_probe=lambda: _host(available=4 * GIB, headroom=4 * GIB),
        disk_probe=_disk,
        sleep=lambda _seconds: None,
    )

    admitted = gate.admit("monthly-release", wsl_required=False, pressure_rung=0)
    receipt = gate.receipt()

    assert admitted.decision.status == "READY"
    assert receipt["checkpoint_requested"] is False
    assert receipt["system_warning_codes"] == [
        "SYSTEM_AVAILABLE_MEMORY_LOW",
        "SYSTEM_COMMIT_HEADROOM_LOW",
    ]
    assert receipt["fixed_disk_floor_blocking"] is False


def test_parent_disk_pressure_is_waiting_before_child_admission(dataset_profile) -> None:
    low_disk = DiskSpaceSnapshot(
        control_free_bytes=31 * GIB,
        candidate_free_bytes=31 * GIB,
        effective_free_bytes=31 * GIB,
        required_free_bytes=32 * GIB,
        predicted_remaining_new_bytes=None,
        same_volume=True,
    )
    gate = ResourceGate(
        dataset_profile,
        host_probe=lambda: _host(),
        disk_probe=lambda _predicted: low_disk,
        sleep=lambda _seconds: None,
    )

    admitted = gate.admit("source-stage", wsl_required=False, pressure_rung=0)

    assert admitted.decision.status == "WAITING_RESOURCE"
    assert admitted.decision.reason_code == "RESOURCE_DISK_RESERVE"


def test_runtime_gate_combines_owned_job_and_wsl_commit_and_requests_pressure_rung(
    dataset_profile,
) -> None:
    gate = ResourceGate(
        dataset_profile,
        host_probe=lambda: _host(),
        disk_probe=_disk,
        sleep=lambda _seconds: None,
    )
    gate.admit("build-stage", wsl_required=False, pressure_rung=0)
    wsl = WslRuntimeSnapshot(
        memory_current_bytes=7 * GIB,
        memory_peak_bytes=7 * GIB,
        memory_high_bytes=6 * GIB,
        memory_max_bytes=8 * GIB,
        swap_current_bytes=0,
        swap_max_bytes=0,
        available_bytes=20 * GIB,
        memory_events={"oom": 0, "oom_kill": 0},
    )
    gate.bind_owned_probe(
        lambda _wsl_required: OwnedRuntimeSnapshot(
            windows_job_commit_bytes=4 * GIB,
            windows_job_peak_commit_bytes=4 * GIB,
            windows_tree_rss_bytes=2 * GIB,
            windows_tree_peak_rss_bytes=2 * GIB,
            active_processes=1,
            wsl=wsl,
        )
    )

    first = gate.sample("build-stage", wsl_required=True, pressure_rung=0)
    second = gate.sample("build-stage", wsl_required=True, pressure_rung=0)

    assert first.decision.status == "READY"
    assert second.decision.status == "CHECKPOINT_PRESSURE"
    assert second.decision.pressure_rung == 1
    receipt = gate.receipt()
    assert receipt["aggregate_owned_peak_commit_bytes"] == 11 * GIB
    assert receipt["wsl_required"] is True


def test_child_chunk_checkpoint_is_identity_bound_and_never_changes_scope(tmp_path) -> None:
    signal = tmp_path / "resource-checkpoint.json"
    signal.write_text(
        json.dumps(
            {
                "schema_version": RESOURCE_CHECKPOINT_SIGNAL_SCHEMA,
                "attempt_id": "attempt-1",
                "fence": 7,
                "execution_id": "factor-stage",
                "reason_code": "RESOURCE_EMERGENCY",
                "pressure_rung": 1,
                "observed_at": datetime.now(UTC).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    checkpoint = ChildResourceCheckpoint(
        attempt_id="attempt-1",
        fence=7,
        execution_id="factor-stage",
        path=signal,
    )

    with pytest.raises(ResourceCheckpointRequested) as exc:
        checkpoint.checkpoint()

    assert exc.value.context["reason_code"] == "RESOURCE_EMERGENCY"
    assert exc.value.context["data_scope_changed"] is False


def test_child_checkpoint_rejects_stale_fence(tmp_path) -> None:
    signal = tmp_path / "resource-checkpoint.json"
    signal.write_text(
        json.dumps(
            {
                "schema_version": RESOURCE_CHECKPOINT_SIGNAL_SCHEMA,
                "attempt_id": "attempt-1",
                "fence": 8,
                "execution_id": "factor-stage",
            }
        ),
        encoding="utf-8",
    )
    checkpoint = ChildResourceCheckpoint(
        attempt_id="attempt-1",
        fence=7,
        execution_id="factor-stage",
        path=signal,
    )

    with pytest.raises(Exception, match="identity mismatched"):
        checkpoint.checkpoint()
