from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

import pytest

import backend.services.dataset_release.resource_budget as resource_budget_module
from backend.services.dataset_release.profile import DatasetProfile
from backend.services.dataset_release.resource_budget import (
    GIB,
    HostTelemetrySampler,
    HostMemorySnapshot,
    OwnedMemorySnapshot,
    ResourceBudget,
    ResourceSnapshot,
)


def _snapshot(
    *,
    available: int = 32 * GIB,
    commit_headroom: int = 32 * GIB,
    windows_commit: int = 2 * GIB,
    wsl_commit: int = 0,
    page_reads: float = 0.0,
    low_memory: bool = False,
    wsl_required: bool = False,
    wsl_available: int | None = None,
) -> ResourceSnapshot:
    commit_limit = 80 * GIB
    return ResourceSnapshot(
        stage="fixture",
        host=HostMemorySnapshot(
            observed_monotonic=1.0,
            available_bytes=available,
            commit_total_bytes=commit_limit - commit_headroom,
            commit_limit_bytes=commit_limit,
            pagefile_used_bytes=0,
            pagefile_limit_bytes=16 * GIB,
            page_reads_per_second=page_reads,
            low_memory_signaled=low_memory,
        ),
        owned=OwnedMemorySnapshot(
            windows_job_commit_bytes=windows_commit,
            wsl_cgroup_current_bytes=wsl_commit,
        ),
        wsl_available_bytes=wsl_available,
        wsl_required=wsl_required,
    )


def test_resource_budget_does_not_publish_a_second_policy_class() -> None:
    assert not hasattr(resource_budget_module, "ResourcePolicy")


def test_strict_host_sampler_collects_every_required_windows_gate_field() -> None:
    sampler = HostTelemetrySampler(
        virtual_memory=lambda: SimpleNamespace(available=24 * GIB),
        swap_memory=lambda: SimpleNamespace(used=2 * GIB, total=32 * GIB),
        commit_probe=lambda: (40 * GIB, 80 * GIB),
        page_reads_probe=lambda: 12.5,
        low_memory_probe=lambda: False,
        monotonic=lambda: 7.0,
    )

    sample = sampler()

    assert sample.available_bytes == 24 * GIB
    assert sample.commit_headroom_bytes == 40 * GIB
    assert sample.pagefile_used_bytes == 2 * GIB
    assert sample.page_reads_per_second == 12.5
    assert sample.low_memory_signaled is False


def test_strict_host_sampler_fails_closed_when_commit_telemetry_is_missing() -> None:
    sampler = HostTelemetrySampler(
        virtual_memory=lambda: SimpleNamespace(available=24 * GIB),
        swap_memory=lambda: SimpleNamespace(used=2 * GIB, total=32 * GIB),
        commit_probe=lambda: (None, None),
        page_reads_probe=lambda: 0.0,
        low_memory_probe=lambda: False,
    )

    with pytest.raises(resource_budget_module.ResourceTelemetryUnavailable):
        sampler()


def test_aggregate_private_commit_is_a_hard_failure(dataset_profile: DatasetProfile) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    decision = budget.classify(_snapshot(windows_commit=8 * GIB, wsl_commit=5 * GIB))
    assert decision.status == "FAILED"
    assert decision.reason_code == "FAILED_RESOURCE_HARD_LIMIT"
    assert decision.hard_failure is True


def test_system_commit_and_low_memory_gate_do_not_report_ready(
    dataset_profile: DatasetProfile,
) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    commit = budget.classify(_snapshot(commit_headroom=7 * GIB))
    low = budget.classify(_snapshot(low_memory=True))
    assert commit.reason_code == "RESOURCE_EMERGENCY"
    assert low.reason_code == "RESOURCE_EMERGENCY"


def test_sustained_page_reads_require_three_samples_and_low_headroom(
    dataset_profile: DatasetProfile,
) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    first = budget.classify(_snapshot(available=10 * GIB, page_reads=300))
    second = budget.classify(_snapshot(available=10 * GIB, page_reads=300))
    third = budget.classify(_snapshot(available=10 * GIB, page_reads=300))
    assert first.reason_code == "RESOURCE_ADMISSION_WAIT"
    assert second.reason_code == "RESOURCE_ADMISSION_WAIT"
    assert third.reason_code == "RESOURCE_EMERGENCY"


def test_wsl_telemetry_is_required_for_wsl_stage(dataset_profile: DatasetProfile) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    missing = budget.classify(_snapshot(wsl_required=True, wsl_available=None))
    emergency = budget.classify(_snapshot(wsl_required=True, wsl_available=5 * GIB, wsl_commit=2 * GIB))
    assert missing.reason_code == "BLOCKED_REQUIRED_TELEMETRY_UNAVAILABLE"
    assert emergency.reason_code == "RESOURCE_EMERGENCY"


def test_negative_owned_counter_is_rejected_as_invalid_telemetry(
    dataset_profile: DatasetProfile,
) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    decision = budget.classify(_snapshot(windows_commit=-1))
    assert decision.status == "BLOCKED"
    assert decision.reason_code == "BLOCKED_REQUIRED_TELEMETRY_UNAVAILABLE"
    assert decision.hard_failure is True


def test_hybrid_stage_uses_the_stricter_windows_job_cap(dataset_profile: DatasetProfile) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    decision = budget.classify(
        _snapshot(
            windows_commit=5 * GIB,
            wsl_required=True,
            wsl_available=20 * GIB,
        )
    )
    assert decision.status == "FAILED"
    assert decision.reason_code == "FAILED_WINDOWS_JOB_COMMIT_LIMIT"


def test_pressure_ladder_changes_physical_units_without_changing_scope(
    dataset_profile: DatasetProfile,
) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    first = budget.classify(_snapshot(windows_commit=6 * GIB, wsl_commit=5 * GIB))
    second = budget.classify(_snapshot(windows_commit=6 * GIB, wsl_commit=5 * GIB))
    assert first.status == "READY"
    assert second.status == "CHECKPOINT_PRESSURE"
    assert second.pressure_rung == 1


def test_approaching_emergency_reserve_advances_pressure_ladder(
    dataset_profile: DatasetProfile,
) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    decision = budget.classify(_snapshot(available=8 * GIB + 512 * 1024**2))
    assert decision.status == "CHECKPOINT_PRESSURE"
    assert decision.pressure_rung == 1


def test_pressure_ladder_is_projected_through_safe_profile_overrides(
    dataset_profile: DatasetProfile,
) -> None:
    tuned = dataset_profile.with_resource_overrides(
        {
            "h5_load_batch_size": 40,
            "minute_code_batch_size": 8,
            "date_chunk_months": 1,
            "parquet_row_group_rows": 40_000,
            "qlib_dump_workers": 3,
        },
        source="test",
    )
    budget = ResourceBudget(tuned, lambda _stage, _wsl: _snapshot())
    assert budget.pressure_ladder[0].h5_batch == 40
    assert budget.pressure_ladder[0].minute_batch == 8
    assert budget.pressure_ladder[0].chunk_months == 1
    assert budget.pressure_ladder[0].row_group_rows == 40_000
    assert budget.pressure_ladder[0].dump_workers == 3
    assert budget.pressure_ladder[-1].h5_batch == 20
    assert budget.pressure_ladder[-1].dump_workers == 2


def test_invalid_pressure_rung_fails_closed(dataset_profile: DatasetProfile) -> None:
    budget = ResourceBudget(dataset_profile, lambda _stage, _wsl: _snapshot())
    with pytest.raises(ValueError, match="outside profile ladder"):
        budget.classify(_snapshot(), pressure_rung=len(budget.pressure_ladder))


def test_wait_requires_two_consecutive_ready_samples(dataset_profile: DatasetProfile) -> None:
    samples: Iterator[ResourceSnapshot] = iter(
        [
            _snapshot(available=14 * GIB),
            _snapshot(),
            _snapshot(),
        ]
    )
    ticks = iter([0.0, 0.0, 1.0, 2.0, 3.0, 4.0])
    budget = ResourceBudget(
        dataset_profile.with_resource_overrides({"wait_deadline_seconds": 30}, source="test"),
        lambda _stage, _wsl: next(samples),
        sleep=lambda _seconds: None,
        monotonic=lambda: next(ticks),
    )
    decision = budget.wait_until_ready("fixture", wsl_required=False)
    assert decision.status == "READY"
    assert len(budget.telemetry) == 3


def test_wait_returns_pressure_checkpoint_instead_of_waiting_to_timeout(
    dataset_profile: DatasetProfile,
) -> None:
    samples: Iterator[ResourceSnapshot] = iter(
        [
            _snapshot(windows_commit=6 * GIB, wsl_commit=5 * GIB),
            _snapshot(windows_commit=6 * GIB, wsl_commit=5 * GIB),
        ]
    )
    ticks = iter([0.0, 0.0, 1.0])
    budget = ResourceBudget(
        dataset_profile,
        lambda _stage, _wsl: next(samples),
        sleep=lambda _seconds: None,
        monotonic=lambda: next(ticks),
    )
    decision = budget.wait_until_ready("fixture", wsl_required=False)
    assert decision.status == "CHECKPOINT_PRESSURE"
    assert decision.pressure_rung == 1


def test_wait_timeout_is_typed_and_does_not_lower_data_scope(
    dataset_profile: DatasetProfile,
) -> None:
    ticks = iter([0.0, 0.0, 1.0, 2.0, 3.0])
    budget = ResourceBudget(
        dataset_profile.with_resource_overrides({"wait_deadline_seconds": 1}, source="test"),
        lambda _stage, _wsl: _snapshot(available=10 * GIB),
        sleep=lambda _seconds: None,
        monotonic=lambda: next(ticks),
    )
    decision = budget.wait_until_ready("fixture", wsl_required=False)
    assert decision.status == "BLOCKED"
    assert decision.reason_code == "BLOCKED_RESOURCE_TIMEOUT"
