"""Startup safety contracts; never launch an application or inspect live services."""
from contextlib import nullcontext
import base64
import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location("windows_start", Path(__file__).resolve().parents[3] / "scripts/start_ai_stock.py")
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)
SHA = "a" * 40


def test_inventory_encoding_is_explicit_and_missing_output_is_not_free(monkeypatch):
    run = Mock(return_value=Mock(stdout='{"processes":[],"listeners":[]}'))
    monkeypatch.setattr(launcher.subprocess, "run", run)
    assert launcher.inventory() == {"processes": [], "listeners": []}
    assert run.call_args.kwargs["encoding"] == "utf-8"
    assert "OutputEncoding" in run.call_args.args[0][-1]
    run.return_value.stdout = None
    with pytest.raises(RuntimeError, match="no JSON"):
        launcher.inventory()


def snapshot(service, listening=True, command=None):
    command = command or f'python "{launcher.ROOT}/scripts/start_ai_stock.py" --child {service}'
    return {"processes": [{"ProcessId": 10, "ParentProcessId": 0, "CommandLine": command,
                            "ExecutablePath": "C:/env/python.exe"}],
            "listeners": [{"LocalPort": launcher.PORTS[service], "OwningProcess": 10}] if listening else []}


@pytest.mark.parametrize("service", launcher.PORTS)
def test_ownership_starting_listener_and_foreign_process(service):
    assert launcher.state(service, snapshot(service)) == "listening"
    assert launcher.state(service, snapshot(service, False)) == "starting_or_stopping"
    with pytest.raises(RuntimeError, match="unknown process"):
        launcher.state(service, snapshot(service, command="unrelated.exe"))
    assert launcher.state(service, {"processes": [], "listeners": []}) == "absent"


def test_old_worker_and_dev_tdx_are_not_duplicate_candidates():
    worker = snapshot("backend", False, f'python "{launcher.ROOT}/scripts/monthly_unified_dataset_release_worker.py" --serve')
    assert launcher.state("backend", worker) == "old_monthly_worker"
    dev = snapshot("tdx", False, f'python "{launcher.ROOT}/scripts/start_tdx_go_backend.py" --database-target dev')
    assert launcher.state("tdx", dev) == "absent"


def test_backend_identity_and_tdx_cached_revision_are_checked(monkeypatch):
    payloads = {"/api/v1/health": {"status": "ok"}, "/api/v1/runtime-identity": {"status": "ready", "merge_commit": SHA},
                "/api/health": {"metadata_ready": True}, "/api/runtime-identity": {"data": {
                    "source_revision": SHA, "database_target": "production", "listen_address": ":19080"}}}
    monkeypatch.setattr(launcher, "request", lambda path, port: payloads[path])
    assert launcher.ready("backend", SHA) == launcher.ready("tdx", SHA) == SHA
    with pytest.raises(RuntimeError, match="old/different"):
        launcher.ready("backend", "b" * 40)
    payloads["/api/health"]["metadata_ready"] = False
    with pytest.raises(RuntimeError, match="invalid"):
        launcher.ready("tdx", SHA)


def test_check_is_non_mutating_and_build_receipt_is_not_restamped(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "ROOT", tmp_path)
    (tmp_path / ".env").touch()
    monkeypatch.setattr(launcher, "git_head", lambda: SHA)
    monkeypatch.setattr(launcher, "commands", lambda _: (["python"], tmp_path, {}))
    monkeypatch.setattr(launcher, "inventory", lambda: {"processes": [], "listeners": []})
    lock = Mock(side_effect=AssertionError("check must not acquire/write lock"))
    monkeypatch.setattr(launcher, "startup_lock", lock)
    assert launcher.start(True)["services_started"] is False
    process = Mock()
    process.wait.return_value = 0
    def build(*args, **kwargs):
        kwargs["stdout"].write(json.dumps({"source_revision": "b" * 40}))
        kwargs["stdout"].flush()
        return process
    run = Mock(side_effect=build)
    monkeypatch.setattr(launcher.subprocess, "Popen", run)
    assert launcher.tdx_build(tmp_path) == "b" * 40
    assert "--build-only" in run.call_args.args[0]
    process.wait.return_value = 2
    with pytest.raises(RuntimeError, match="TDX build failed"):
        launcher.tdx_build(tmp_path)
    process.wait.side_effect = launcher.subprocess.TimeoutExpired("build", 180)
    with pytest.raises(RuntimeError, match="not killed or retried"):
        launcher.tdx_build(tmp_path)
    process.kill.assert_not_called()


def test_existing_services_are_not_relaunched_and_failures_surface(tmp_path, monkeypatch):
    wait_ready = launcher.wait_ready
    monkeypatch.setattr(launcher, "ROOT", tmp_path)
    monkeypatch.setattr(launcher, "RUNTIME", tmp_path / "runtime")
    launcher.RUNTIME.mkdir()
    (tmp_path / ".env").touch()
    monkeypatch.setattr(launcher, "git_head", lambda: SHA)
    monkeypatch.setattr(launcher, "commands", lambda _: None)
    monkeypatch.setattr(launcher, "inventory", lambda: {})
    monkeypatch.setattr(launcher, "state", lambda *_: "listening")
    monkeypatch.setattr(launcher, "settle", lambda *_: "listening")
    monkeypatch.setattr(launcher, "startup_lock", nullcontext)
    monkeypatch.setattr(launcher, "wait_ready", lambda *_: SHA)
    monkeypatch.setattr(launcher, "launch", Mock(side_effect=AssertionError("duplicate")))
    assert len(launcher.start()["services"]) == 3
    # A child failure must be reported before waiting or accepting an unrelated listener.
    (tmp_path / "backend.exit.json").write_text('{"exit_code": 7}', encoding="utf-8")
    with pytest.raises(RuntimeError, match="exit_code.*7"):
        wait_ready("backend", SHA, tmp_path)


def test_old_process_wait_is_bounded_and_commands_never_reload(monkeypatch):
    monkeypatch.setattr(launcher, "inventory", lambda: {})
    monkeypatch.setattr(launcher, "state", lambda *_: "old_monthly_worker")
    with pytest.raises(RuntimeError, match="No duplicate started"):
        launcher.settle("backend", timeout=0)
    command, _, _ = launcher.commands("backend")
    assert command[1:4] == ["-m", "uvicorn", "backend.main:app"]
    assert "--reload" not in command


def test_old_process_exit_then_absent_start_is_not_a_duplicate(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "inventory", lambda: {})
    states = iter(["starting_or_stopping", "absent"])
    monkeypatch.setattr(launcher, "state", lambda *_: next(states))
    monkeypatch.setattr(launcher.time, "sleep", lambda _: None)
    assert launcher.settle("backend") == "absent"
    monkeypatch.setattr(launcher, "RUNTIME", tmp_path)
    with launcher.startup_lock():
        with pytest.raises(RuntimeError, match="Another one-click startup"):
            with launcher.startup_lock():
                pytest.fail("concurrent launcher acquired the lock")


def test_absent_services_launch_once_and_bind_expected_revisions(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "ROOT", tmp_path)
    monkeypatch.setattr(launcher, "RUNTIME", tmp_path / "runtime")
    launcher.RUNTIME.mkdir()
    (tmp_path / ".env").touch()
    monkeypatch.setattr(launcher, "git_head", lambda: SHA)
    monkeypatch.setattr(launcher, "commands", lambda _: None)
    monkeypatch.setattr(launcher, "inventory", lambda: {})
    monkeypatch.setattr(launcher, "state", lambda *_: "absent")
    monkeypatch.setattr(launcher, "startup_lock", nullcontext)
    monkeypatch.setattr(launcher, "tdx_build", lambda _: "b" * 40)
    events = []
    launch = Mock(side_effect=lambda service, _: events.append(("launch", service)))
    wait = Mock(side_effect=lambda service, *_: events.append(("ready", service)) or SHA)
    monkeypatch.setattr(launcher, "launch", launch)
    monkeypatch.setattr(launcher, "wait_ready", wait)
    assert len(launcher.start()["services"]) == launch.call_count == 3
    assert [c.args[1] for c in wait.call_args_list] == ["b" * 40, SHA, None]
    assert events == [("launch", "frontend"), ("launch", "tdx"), ("ready", "tdx"),
                      ("launch", "backend"), ("ready", "backend"), ("ready", "frontend")]


def test_utf8_is_propagated_to_real_redirected_python_even_with_gbk_parent(monkeypatch):
    monkeypatch.setenv("PYTHONIOENCODING", "gbk")
    monkeypatch.setenv("PYTHONUTF8", "0")
    _, _, env = launcher.commands("backend")
    result = launcher.subprocess.run(
        [launcher.sys.executable, "-c", "print(chr(0x2705) + chr(0x4e2d))"],
        env=env, capture_output=True, encoding="utf-8", check=True,
    )
    assert result.stdout.strip() == "\u2705\u4e2d"
    assert env["PYTHONUTF8"] == "1" and env["PYTHONUNBUFFERED"] == "1"


def test_child_supervisor_preserves_unicode_log_and_nonzero_exit(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "RUNTIME", tmp_path)
    directory = tmp_path / "assigned"
    directory.mkdir()
    _, _, env = launcher.commands("backend")
    monkeypatch.setattr(launcher, "commands", lambda _: (
        [launcher.sys.executable, "-c", "print(chr(0x2705)); raise SystemExit(7)"], tmp_path, env))
    assert launcher.child("backend", directory) == 7
    assert (directory / "backend.log").read_text(encoding="utf-8").strip() == "\u2705"
    assert json.loads((directory / "backend.exit.json").read_text()) == {"exit_code": 7}


@pytest.mark.parametrize("terminal", [True, False])
def test_terminal_tabs_and_console_share_absolute_utf8_supervisor(tmp_path, monkeypatch, terminal):
    monkeypatch.setenv("WT_SESSION", "present")
    monkeypatch.setenv("WT_WINDOW_ID", "7")
    monkeypatch.setattr(launcher.shutil, "which", lambda _: "wt.exe" if terminal else None)
    run, popen = Mock(), Mock()
    monkeypatch.setattr(launcher.subprocess, "run", run)
    monkeypatch.setattr(launcher.subprocess, "Popen", popen)
    directory = tmp_path / "space ' \u4e2d"
    launcher.launch("backend", directory)
    invoked = run if terminal else popen
    args = invoked.call_args.args[0]
    script = base64.b64decode(args[-1]).decode("utf-16-le")
    assert "'-X' 'utf8'" in script and "'--child' 'backend'" in script
    assert str(directory).replace("'", "''") in script
    assert launcher.sys.executable.replace("'", "''") in script
    assert "-NoProfile" in args and "-NoExit" in args
    if terminal:
        assert args[:4] == ["wt.exe", "-w", "7", "new-tab"]
        assert invoked.call_args.kwargs["check"] is True
        popen.assert_not_called()
        run.side_effect = launcher.subprocess.CalledProcessError(1, args)
        with pytest.raises(launcher.subprocess.CalledProcessError):
            launcher.launch("backend", directory)
        popen.assert_not_called()  # A failed tab dispatch never launches a duplicate fallback.
    else:
        assert invoked.call_args.kwargs["creationflags"] == launcher.subprocess.CREATE_NEW_CONSOLE
        run.assert_not_called()


def test_readiness_closed_port_polls_are_cheap_but_final_ownership_is_required(tmp_path, monkeypatch):
    connect = Mock(side_effect=[OSError("closed"), OSError("closed"), nullcontext()])
    monkeypatch.setattr(launcher.socket, "create_connection", connect)
    monkeypatch.setattr(launcher.time, "sleep", lambda _: None)
    inventory = Mock(return_value=snapshot("backend"))
    monkeypatch.setattr(launcher, "inventory", inventory)
    monkeypatch.setattr(launcher, "ready", Mock(return_value=SHA))
    assert launcher.wait_ready("backend", SHA, tmp_path) == SHA
    assert connect.call_count == 3 and inventory.call_count == 1
    connect.side_effect = None
    connect.return_value = nullcontext()
    inventory.return_value = snapshot("backend", command="foreign.exe")
    with pytest.raises(RuntimeError, match="unknown process"):
        launcher.wait_ready("backend", SHA, tmp_path)
