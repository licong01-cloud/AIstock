"""Startup safety contracts; never launch an application or inspect live services."""
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
from pathlib import Path
from threading import Barrier
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location("windows_start", Path(__file__).resolve().parents[3] / "scripts/start_ai_stock.py")
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)
SHA = "a" * 40


def test_all_service_tabs_are_requested_before_any_slow_setup():
    script = (launcher.ROOT / "start_all_ai_stock.bat").read_text(encoding="utf-8")
    dispatch = next(line for line in script.splitlines() if line.startswith("wt.exe "))
    assert dispatch.startswith("wt.exe -w 0 new-tab")
    assert dispatch.count("new-tab --title") == 3 and dispatch.count(" ; ") == 2
    for service, title in (("tdx", "TDX"), ("backend", "Backend"), ("frontend", "Frontend")):
        assert f'--title "AIstock {title}"' in dispatch
        assert f'cmd.exe /d /k call "%~f0" --service {service}' in dispatch
    assert script.index(dispatch) < script.index("activate AIstock")
    assert 'if /i "%~1"=="--check" if "%~2"=="" goto run' in script
    for forbidden in ("taskkill", "Stop-Process", "--reload", "go run", "pip install"):
        assert forbidden not in script


def test_only_backend_activates_and_activation_temp_is_private():
    all_script = (launcher.ROOT / "start_all_ai_stock.bat").read_text(encoding="utf-8")
    bypass = 'if /i "%~1"=="--service" if not "%~2"=="backend" goto launch'
    assert all_script.index(bypass) < all_script.index('call "%AISTOCK_CONDA_BAT%" activate AIstock')
    assert 'if /i "%~1"=="--check" goto launch' in all_script
    for name in ("start_all_ai_stock.bat", "start_backend.bat"):
        script = (launcher.ROOT / name).read_text(encoding="utf-8")
        activation = script.index('call "%AISTOCK_CONDA_BAT%" activate AIstock')
        assert script.index("uuid.uuid4().hex") < script.index('mkdir "%AISTOCK_CONDA_TEMP%"') < activation
        assert 'set "AISTOCK_CONDA_ID="' in script and 'if not defined AISTOCK_CONDA_ID exit /b 1' in script
        for variable in ("TEMP", "TMP", "TMPDIR"):
            assert script.index(f'set "{variable}=%AISTOCK_CONDA_TEMP%"') < activation
            assert script.index(f'set "{variable}=%AISTOCK_OLD_{variable}%"') > activation
        assert script.index('set "AISTOCK_CONDA_EXIT=%errorlevel%"') > activation
        assert 'if not "%AISTOCK_CONDA_EXIT%"=="0"' in script
        assert 'rmdir "%AISTOCK_CONDA_TEMP%" 2>nul' in script
        assert "rmdir /s" not in script.lower()
        assert 'if /i "%~1"=="--check-env" goto check_env' in script


@pytest.mark.skipif(os.name != "nt", reason="Concurrent CMD activation isolation is Windows-only")
@pytest.mark.parametrize("activation_exit", [0, 7])
def test_real_cmd_isolates_activation_and_preserves_failure(tmp_path, activation_exit):
    fake_conda = tmp_path / "conda.bat"
    fake_conda.write_text(f'@echo off\nset "CONDA_PREFIX={launcher.sys.prefix}"\nexit /b {activation_exit}\n')
    scripts = []
    for name in ("start_all_ai_stock.bat", "start_backend.bat"):
        body = (launcher.ROOT / name).read_text()
        for key, value in (("AISTOCK_CONDA_BAT", str(fake_conda)), ("AISTOCK_BACKEND_PYTHON", launcher.sys.executable)):
            assignment = next(line for line in body.splitlines() if line.startswith(f'set "{key}='))
            body = body.replace(assignment, f'set "{key}={value}"')
        script = tmp_path / name
        script.write_text(body)
        scripts.append(script)
    env = {**os.environ, "RANDOM": "11067"}  # Deliberately identical across processes.
    def check(script):
        return launcher.subprocess.run(["cmd.exe", "/d", "/c", "call", str(script), "--check-env"],
                                       env=env, input="\n", capture_output=True, encoding="utf-8", errors="replace", timeout=15)
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(check, [*scripts, scripts[0]]))
    paths = [next(line for line in result.stdout.splitlines() if line.startswith("Conda activation temp:")).split('"')[1]
             for result in results]
    assert len(set(paths)) == 3 and all(not Path(path).exists() for path in paths)
    for result in results:
        assert (result.returncode == 0) == (activation_exit == 0), result.stdout + result.stderr
        assert ("AIstock environment ready:" in result.stdout) == (activation_exit == 0)


@pytest.mark.skipif(os.name != "nt", reason="Real CMD dispatch contract is Windows-only")
def test_real_cmd_dispatches_three_tabs_once_without_starting_services(tmp_path):
    compiler = Path(os.environ["WINDIR"]) / "Microsoft.NET/Framework64/v4.0.30319/csc.exe"
    if not compiler.is_file():
        pytest.skip("Existing Windows .NET compiler unavailable; never install dependencies")
    # A recording executable stands in for WT. Neither it nor --check opens UI/services.
    source = tmp_path / "Capture.cs"
    source.write_text('class Capture { static void Main(string[] a) { '
                      'System.IO.File.WriteAllLines(System.Environment.GetEnvironmentVariable("TAB_ARGS"), a); } }')
    launcher.subprocess.run([str(compiler), "/nologo", f"/out:{tmp_path / 'wt.exe'}", str(source)],
                            check=True, capture_output=True)
    root = tmp_path / "space & \u4e2d"
    root.mkdir()
    script = root / "start_all_ai_stock.bat"
    script.write_bytes((launcher.ROOT / script.name).read_bytes())
    args_file = tmp_path / "args.txt"
    env = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"], "TAB_ARGS": str(args_file)}
    result = launcher.subprocess.run(["cmd.exe", "/d", "/c", "call", str(script)], env=env,
                                     capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
    args = args_file.read_text(encoding="utf-8-sig").splitlines()
    assert args[:2] == ["-w", "0"]
    assert args.count("new-tab") == 3 and args.count(";") == 2
    assert args.count(str(script)) == 3
    assert [args[i + 1] for i, arg in enumerate(args) if arg == "--service"] == list(launcher.PORTS)
    invalid = launcher.subprocess.run(["cmd.exe", "/d", "/c", "call", str(script), "--service", "invalid"],
                                      env=env, capture_output=True, timeout=15)
    assert invalid.returncode == 1
    # Real Go/Node dispatch must not even require an existing Conda BAT.
    body = script.read_text()
    for key, value in (("AISTOCK_CONDA_BAT", str(tmp_path / "missing-conda.bat")),
                       ("AISTOCK_BACKEND_PYTHON", str(tmp_path / "wt.exe"))):
        assignment = next(line for line in body.splitlines() if line.startswith(f'set "{key}='))
        body = body.replace(assignment, f'set "{key}={value}"')
    script.write_text(body)
    for service in ("tdx", "frontend"):
        result = launcher.subprocess.run(["cmd.exe", "/d", "/c", "call", str(script), "--service", service],
                                         env=env, capture_output=True, timeout=15)
        assert result.returncode == 0, result.stderr
        assert args_file.read_text(encoding="utf-8-sig").splitlines()[-2:] == ["--service", service]


def test_backend_only_tab_launcher_never_controls_other_services():
    script = (launcher.ROOT / "start_backend.bat").read_text(encoding="utf-8")
    assert 'if /i "%~1"=="--restart" goto dispatch' in script
    assert '"%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\\backend_lifecycle.py" %*' in script
    assert '"%AISTOCK_BACKEND_PYTHON%" -X utf8 "%AIROOT%scripts\\backend_lifecycle.py" --serve "%~2"' in script
    assert 'call "%AISTOCK_CONDA_BAT%" activate AIstock' in script
    assert script.index(':dispatch') < script.index('activate AIstock')
    for forbidden in ('taskkill', 'Stop-Process', '--reload', 'go run', 'go build', 'npm', '19080', '3000'):
        assert forbidden not in script


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


def test_check_is_non_mutating(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "ROOT", tmp_path)
    (tmp_path / ".env").touch()
    monkeypatch.setattr(launcher, "git_head", lambda: SHA)
    monkeypatch.setattr(launcher, "commands", lambda _: (["python"], tmp_path, {}))
    monkeypatch.setattr(launcher, "inventory", lambda: {"processes": [], "listeners": []})
    lock = Mock(side_effect=AssertionError("check must not acquire/write lock"))
    monkeypatch.setattr(launcher, "startup_lock", lock)
    assert launcher.start(True)["services_started"] is False


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
    with launcher.startup_lock("backend"), launcher.startup_lock("tdx"):
        with pytest.raises(RuntimeError, match="Another one-click startup"):
            with launcher.startup_lock("backend"):
                pytest.fail("duplicate backend acquired its service lock")


@pytest.mark.parametrize("service", launcher.PORTS)
def test_service_tab_selects_only_its_service(tmp_path, monkeypatch, service):
    monkeypatch.setattr(launcher, "ROOT", tmp_path)
    monkeypatch.setattr(launcher, "RUNTIME", tmp_path / "runtime")
    launcher.RUNTIME.mkdir()
    (tmp_path / ".env").touch()
    monkeypatch.setattr(launcher, "git_head", lambda: SHA)
    monkeypatch.setattr(launcher, "inventory", lambda: {})
    state = Mock(return_value="absent")
    monkeypatch.setattr(launcher, "state", state)
    monkeypatch.setattr(launcher, "startup_lock", nullcontext)
    launch, wait = Mock(), Mock(return_value=SHA)
    monkeypatch.setattr(launcher, "launch", launch)
    monkeypatch.setattr(launcher, "wait_ready", wait)
    result = launcher.start(service=service)
    assert list(result["services"]) == [service]
    state.assert_called_once_with(service, {})
    assert launch.call_count == wait.call_count == 1
    assert launch.call_args.args[0] == wait.call_args.args[0] == service


def test_tab_cli_passes_exact_service_and_rejects_mixed_child_mode(monkeypatch):
    start, child = Mock(return_value={}), Mock()
    monkeypatch.setattr(launcher, "start", start)
    monkeypatch.setattr(launcher, "child", child)
    monkeypatch.setattr(launcher.sys, "argv", ["start_ai_stock.py", "--service", "backend"])
    assert launcher.main() == 0
    start.assert_called_once_with(False, [], "backend")
    monkeypatch.setattr(launcher.sys, "argv", ["start_ai_stock.py", "--service", "backend", "--child", "tdx"])
    assert launcher.main() == 1
    child.assert_not_called()


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
    events = []
    launch = Mock(side_effect=lambda service, _: events.append(("launch", service)))
    barrier = Barrier(3)
    def ready(service, *_):
        barrier.wait(timeout=5)  # Serial readiness would fail, not take 3 x 120s.
        events.append(("ready", service))
        return SHA
    wait = Mock(side_effect=ready)
    monkeypatch.setattr(launcher, "launch", launch)
    monkeypatch.setattr(launcher, "wait_ready", wait)
    assert len(launcher.start()["services"]) == launch.call_count == 3
    expected = {call.args[0]: call.args[1] for call in wait.call_args_list}
    assert isinstance(expected["tdx"], Path)
    assert expected["backend"] == SHA and expected["frontend"] is None
    assert events[:3] == [("launch", "tdx"), ("launch", "backend"), ("launch", "frontend")]
    assert set(events[3:]) == {("ready", service) for service in launcher.PORTS}


def test_cached_tdx_is_executed_without_build_or_receipt_relabel(tmp_path, monkeypatch):
    helper = launcher.load_tdx_launcher()
    binary = tmp_path / "tdx-backend.exe"
    binary.write_bytes(b"test executable; never executed")
    receipt = {"schema_version": "aistock_tdx_build_v1", "source_revision": "b" * 40,
               "input_sha256": "inputs", "binary_sha256": helper.binary_digest(binary)}
    binary.with_suffix(".json").write_text(json.dumps(receipt), encoding="utf-8")
    monkeypatch.setattr(launcher, "load_tdx_launcher", lambda: helper)
    monkeypatch.setattr(helper, "prepare_launch", lambda *_: ([str(binary)], {}))
    monkeypatch.setattr(helper, "build_fingerprint", lambda _: "inputs")
    monkeypatch.setattr(helper, "ensure_binary", Mock(side_effect=AssertionError("no build")))
    monkeypatch.setattr(launcher.subprocess, "run", Mock(side_effect=AssertionError("no Go or service")))
    command, _, _ = launcher.cached_tdx_command(tmp_path)
    assert command == [str(binary)]
    saved = json.loads((tmp_path / "tdx-executable.json").read_text())
    assert saved == {**receipt, "build_action": "reused"}
    monkeypatch.setattr(helper, "build_fingerprint", lambda _: "changed")
    build = Mock(return_value={**receipt, "input_sha256": "changed", "build_action": "built"})
    monkeypatch.setattr(helper, "ensure_binary", build)
    assert launcher.cached_tdx_command(tmp_path)[0] == [str(binary)]
    build.assert_called_once()
    assert build.call_args.args[1]["AISTOCK_TDX_ENV_FILE"] == str((launcher.ROOT / ".env").resolve())
    assert json.loads((tmp_path / "tdx-executable.json").read_text())["build_action"] == "built"
    build.side_effect = RuntimeError("offline compilation failed")
    with pytest.raises(RuntimeError, match="compilation failed"):
        launcher.cached_tdx_command(tmp_path)
    binary.write_bytes(b"damaged executable")
    with pytest.raises(RuntimeError, match="incomplete or invalid"):
        launcher.cached_tdx_command(tmp_path)


def test_failed_tdx_dispatch_does_not_block_other_services(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher, "ROOT", tmp_path)
    monkeypatch.setattr(launcher, "RUNTIME", tmp_path / "runtime")
    launcher.RUNTIME.mkdir()
    (tmp_path / ".env").touch()
    monkeypatch.setattr(launcher, "git_head", lambda: SHA)
    monkeypatch.setattr(launcher, "commands", Mock(side_effect=AssertionError("service preparation belongs to its child")))
    monkeypatch.setattr(launcher, "inventory", Mock(return_value={}))
    monkeypatch.setattr(launcher, "state", lambda *_: "absent")
    monkeypatch.setattr(launcher, "startup_lock", nullcontext)
    calls = []
    def launch(service, directory):
        calls.append(service)
        if service == "tdx":
            raise OSError("tab dispatch failed")
    monkeypatch.setattr(launcher, "launch", launch)
    monkeypatch.setattr(launcher, "wait_ready", lambda *_: SHA)
    with pytest.raises(RuntimeError, match="tab dispatch failed"):
        launcher.start()
    assert calls == ["tdx", "backend", "frontend"]
    assert launcher.inventory.call_count == 1
    saved = json.loads(next(launcher.RUNTIME.glob("*/summary.json")).read_text())
    assert len(saved["errors"]) == 1 and "error" in saved["services"]["tdx"]
    assert saved["services"]["backend"]["observed_identity"] == SHA


@pytest.mark.parametrize("invalid", ['{"source_revision":"invalid"}', '[]'])
def test_tdx_readiness_requires_its_cached_build_revision(tmp_path, monkeypatch, invalid):
    receipt = tmp_path / "tdx-executable.json"
    receipt.write_text(json.dumps({"schema_version": "aistock_tdx_build_v1", "source_revision": "b" * 40}))
    monkeypatch.setattr(launcher.socket, "create_connection", lambda *a, **k: nullcontext())
    monkeypatch.setattr(launcher, "inventory", lambda: snapshot("tdx"))
    ready = Mock(return_value="b" * 40)
    monkeypatch.setattr(launcher, "ready", ready)
    assert launcher.wait_ready("tdx", receipt, tmp_path) == "b" * 40
    ready.assert_called_once_with("tdx", "b" * 40)
    receipt.write_text(invalid)
    with pytest.raises(RuntimeError, match="receipt is invalid"):
        launcher.wait_ready("tdx", receipt, tmp_path)


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
def test_services_inherit_original_console_without_window_routing(tmp_path, monkeypatch, terminal):
    monkeypatch.setenv("WT_SESSION", "present" if terminal else "")
    monkeypatch.setenv("WT_WINDOW_ID", "0")  # Neither a stale override nor MRU may reroute.
    run, popen = Mock(), Mock()
    monkeypatch.setattr(launcher.subprocess, "run", run)
    monkeypatch.setattr(launcher.subprocess, "Popen", popen)
    directory = tmp_path / "space ' \u4e2d"
    assert launcher.launch("backend", directory) is popen.return_value
    args = popen.call_args.args[0]
    assert args == [launcher.sys.executable, "-X", "utf8", str(Path(launcher.__file__).resolve()),
                    "--child", "backend", "--log-dir", str(directory)]
    assert popen.call_args.kwargs == {"cwd": launcher.ROOT}
    run.assert_not_called()
    popen.side_effect = OSError("process dispatch failed")
    with pytest.raises(OSError, match="dispatch failed"):
        launcher.launch("backend", directory)
    assert popen.call_count == 2  # No fallback or retry into any other window.


def test_parent_remains_foreground_for_its_children_even_after_readiness_failure(monkeypatch):
    monkeypatch.setattr(launcher.sys, "argv", ["start_ai_stock.py"])
    processes = [Mock(), Mock()]
    for process, code in zip(processes, [7, 0]):
        process.wait.return_value = code
    def start(check, children):
        children.extend(processes)
        raise RuntimeError("one service failed readiness")
    monkeypatch.setattr(launcher, "start", start)
    assert launcher.main() == 1
    for process in processes:
        process.wait.assert_called_once_with()
        process.kill.assert_not_called()
        process.terminate.assert_not_called()


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


def test_waiting_for_build_has_visible_bounded_progress(tmp_path, monkeypatch, capsys):
    clock = [0.0]
    monkeypatch.setattr(launcher.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(launcher.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    with pytest.raises(RuntimeError, match="readiness timeout"):
        launcher.wait_ready("tdx", tmp_path / "tdx-executable.json", tmp_path, timeout=6)
    output = capsys.readouterr().out
    assert "0s/6s" in output and "5s/6s" in output and "build receipt" in output
    assert clock[0] == 6
