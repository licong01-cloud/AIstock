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
        "MemoryMax": str(40 * guard.GIB),
        "MemorySwapMax": str(8 * guard.GIB),
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
