import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def module(name):
    path = Path(__file__).resolve().parents[3] / "backend/services/quantevolver" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def guard(monkeypatch):
    import sys

    if sys.platform == "win32":
        monkeypatch.setitem(sys.modules, "fcntl", SimpleNamespace())
    return module("qe_wsl_workload_guard")


def test_budget_allows_mixed_work_but_refuses_second_large_fit(guard):
    live = {"fit": {"peak_bytes": 24 * guard.GIB}}
    assert not guard.admits(live, 24 * guard.GIB, 40 * guard.GIB)
    assert guard.admits(live, 10 * guard.GIB, 40 * guard.GIB)
    assert guard.admits(live, 6 * guard.GIB, 40 * guard.GIB)
    assert guard.admits({str(i): {"peak_bytes": 6 * guard.GIB} for i in range(3)}, 6 * guard.GIB, 40 * guard.GIB)
    for invalid in [-1, 0, 41 * guard.GIB, True]:
        with pytest.raises(RuntimeError):
            guard.admits({}, invalid, 40 * guard.GIB)


def test_global_headroom_accounts_for_not_yet_allocated_reservations(guard):
    live = {"fit": {"peak_bytes": 24 * guard.GIB}}
    # Existing job reserved24 but allocated only10;14 is already promised.
    assert not guard.host_admits(live, 6 * guard.GIB, 20 * guard.GIB, 10 * guard.GIB)
    assert guard.host_admits(live, 6 * guard.GIB, 24 * guard.GIB, 10 * guard.GIB)


def test_dead_wrapper_does_not_release_running_scope_and_pid_reuse_is_checked(guard):
    rows = {"a": {"pid": 20, "start": "100", "unit": "a.scope", "peak_bytes": 24}}
    alive = guard.live_records(rows, start_reader=lambda _: "200", unit_reader=lambda _: {"ActiveState": "active"})
    assert alive == rows
    assert (
        guard.live_records(rows, start_reader=lambda _: "200", unit_reader=lambda _: {"ActiveState": "inactive"}) == {}
    )
    with pytest.raises(RuntimeError, match="unknown"):
        guard.live_records(rows, start_reader=lambda _: None, unit_reader=lambda _: {})


def test_limits_and_separation_are_required_not_silent_fallback(guard):
    good = {
        "LoadState": "loaded",
        "MemoryHigh": "infinity",
        "MemoryMax": str(56 * guard.GIB),
        "MemorySwapMax": str(32 * guard.GIB),
        "ControlGroup": "/user.slice/user-1000.slice/user@1000.service/qe-training.slice",
    }
    guard.validate_slice(good)
    for field, value in [
        ("MemoryHigh", str(36 * guard.GIB)),
        ("MemoryMax", "infinity"),
        ("ControlGroup", "/qe.slice/qe-execution.slice"),
    ]:
        with pytest.raises(RuntimeError):
            guard.validate_slice({**good, field: value})


def test_installer_never_changes_files_while_api_active(tmp_path, monkeypatch):
    import hashlib

    m = module("qe_wsl_resource_isolation")
    monkeypatch.setattr(m.sys, "platform", "linux")
    monkeypatch.setattr(m.os, "geteuid", lambda: 0, raising=False)
    service = tmp_path / "etc/systemd/system/rdagent-qe-api.service"
    service.parent.mkdir(parents=True)
    service.write_text("original")
    digest = hashlib.sha256(service.read_bytes()).hexdigest()

    def run(*a, **k):
        return SimpleNamespace(stdout="ActiveState=active\nMainPID=192\n")

    with pytest.raises(RuntimeError, match="active_no_changes"):
        m.install(digest, root=tmp_path, runner=run)
    assert not (tmp_path / "etc/systemd/system/qe-control.slice").exists()

    def run(*a, **k):
        return SimpleNamespace(stdout="ActiveState=inactive\nMainPID=0\n")

    result = m.install(digest, root=tmp_path, runner=run)
    assert result["service_control"] is False and result["state"].endswith("PENDING")
    assert service.read_text() == "original"


def profile(guard, kind="gpu"):
    return {
        "schema": guard.SCHEMA,
        "kind": kind,
        "peak_bytes": guard.PEAK_BYTES[kind],
        "budget_bytes": 88 * guard.GIB,
        "resident_budget_bytes": 56 * guard.GIB,
        "swap_bytes": 32 * guard.GIB,
        "job_swap_bytes": 16 * guard.GIB,
    }


def snapshot(guard, **changes):
    return {
        "available": 24 * guard.GIB,
        "swap_free": 200 * guard.GIB,
        "training_memory": 32 * guard.GIB,
        "training_swap": 0,
        "memory_full_avg10": 0.0,
        "io_full_avg10": 0.0,
        **changes,
    }


def test_bounded_swap_enables_second_gpu_but_not_a_third(guard):
    live = {"one": {"kind": "gpu", "peak_bytes": 32 * guard.GIB}}
    # RAM alone has20GiB headroom, so the second32GiB reservation really uses swap credit.
    assert guard.admission_reason(live, profile(guard), snapshot(guard)) is None
    live["two"] = dict(live["one"])
    assert guard.admission_reason(live, profile(guard), snapshot(guard)) == "gpu_job_limit"


def test_four_replays_fit_bounded_profile_not_a_fifth(guard):
    live = {}
    free = snapshot(guard, available=60 * guard.GIB, training_memory=0)
    for i in range(4):
        assert guard.admission_reason(live, profile(guard, "replay"), free) is None
        live[str(i)] = {"kind": "replay", "peak_bytes": 20 * guard.GIB}
    assert guard.admission_reason(live, profile(guard, "replay"), free) == "job_limit"


def test_swap_never_replaces_physical_reserve_or_exceeds_its_qe_allowance(guard):
    live = {"one": {"kind": "gpu", "peak_bytes": 32 * guard.GIB}}
    assert guard.admission_reason(live, profile(guard), snapshot(guard, available=3 * guard.GIB)) == "physical_host_reserve"
    exhausted = snapshot(guard, training_swap=32 * guard.GIB)
    assert guard.admission_reason(live, profile(guard), exhausted) == "host_capacity_after_existing_reservations"
    # A huge global swap device must not give QE more than its32GiB allowance.
    unallocated = snapshot(guard, available=4 * guard.GIB, training_memory=0)
    assert guard.admission_reason(live, profile(guard), unallocated) == "host_capacity_after_existing_reservations"


@pytest.mark.parametrize("field,value", [("memory_full_avg10", 10.0), ("io_full_avg10", 20.0)])
def test_sustained_pressure_blocks_only_new_admissions(guard, field, value):
    assert guard.admission_reason({}, profile(guard), snapshot(guard, **{field: value})) == "sustained_memory_or_io_pressure"


@pytest.mark.parametrize("field,value", [("available", True), ("swap_free", -1), ("memory_full_avg10", float("nan")), ("io_full_avg10", None)])
def test_unknown_or_invalid_live_capacity_never_becomes_safe_default(guard, field, value):
    with pytest.raises(RuntimeError, match="invalid"):
        guard.admission_reason({}, profile(guard), snapshot(guard, **{field: value}))


def test_unallocated_live_reservations_are_charged_before_new_job(guard):
    live = {str(i): {"kind": "replay", "peak_bytes": 20 * guard.GIB} for i in range(3)}
    free = snapshot(guard, training_memory=0, available=30 * guard.GIB)
    assert guard.admission_reason(live, profile(guard, "replay"), free) == "host_capacity_after_existing_reservations"


def test_only_versioned_profile_and_exact_known_leases_are_accepted(guard):
    for changes in [{"schema": "qe_wsl_workload_envelope_v1"}, {"swap_bytes": 256 * guard.GIB}, {"job_swap_bytes": 0}, {"peak_bytes": True}]:
        with pytest.raises(RuntimeError):
            guard.validate_policy({**profile(guard), **changes})
    with pytest.raises(RuntimeError, match="drain_required"):
        guard.admission_reason({"legacy": {"peak_bytes": 32 * guard.GIB}}, profile(guard), snapshot(guard))


def test_training_only_upgrade_preserves_api_and_refuses_active_or_changed_targets(tmp_path, monkeypatch):
    import hashlib

    m = module("qe_wsl_resource_isolation")
    monkeypatch.setattr(m.sys, "platform", "linux")
    monkeypatch.setattr(m.os, "geteuid", lambda: 0, raising=False)
    monkeypatch.setattr(m.subprocess, "run", lambda *a, **k: pytest.fail("no service control allowed"))
    unit = tmp_path / "etc/systemd/user/qe-training.slice"
    unit.parent.mkdir(parents=True)
    unit.write_bytes(m.LEGACY_TRAINING_UNIT.encode())
    sha = hashlib.sha256(unit.read_bytes()).hexdigest()
    group = tmp_path / "sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/qe.slice/qe-training.slice/job.scope"
    group.mkdir(parents=True)
    procs = group / "cgroup.procs"
    procs.write_text("321")
    with pytest.raises(RuntimeError, match="not_drained"):
        m.upgrade_training_profile(sha, root=tmp_path)
    assert unit.read_text() == m.LEGACY_TRAINING_UNIT
    procs.write_text("")
    pending = tmp_path / "proc/123/cmdline"
    pending.parent.mkdir(parents=True)
    pending.write_bytes(b"python\0qe_wsl_workload_guard.py\0")
    with pytest.raises(RuntimeError, match="waiting_guard"):
        m.upgrade_training_profile(sha, root=tmp_path)
    # A diagnostic mentioning the filename is not an active guard process.
    pending.write_bytes(b"python\0-c\0print('qe_wsl_workload_guard.py')\0")
    with pytest.raises(RuntimeError, match="changed_replan"):
        m.upgrade_training_profile("0" * 64, root=tmp_path)
    result = m.upgrade_training_profile(sha, root=tmp_path)
    assert result["service_control"] is False and result["api_units_modified"] is False
    assert unit.read_text() == m.UNITS[m.TRAINING_UNIT]
    assert not (tmp_path / "etc/systemd/system/qe-control.slice").exists()
    assert result["limits"]["gpu_peak_gib"] == 32 and result["limits"]["training_swap_gib"] == 32
