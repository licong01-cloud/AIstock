"""Backend lifecycle regressions; real Windows tests use disposable Python trees only."""
from contextlib import contextmanager, nullcontext
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from unittest.mock import Mock

import psutil
import pytest

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("backend_lifecycle", ROOT / "scripts/backend_lifecycle.py")
lifecycle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lifecycle)
SHA = "a" * 40


@pytest.fixture
def pending(tmp_path, monkeypatch):
    monkeypatch.setattr(lifecycle, "RUNTIME", tmp_path)
    directory = tmp_path / "backend-request"
    directory.mkdir()
    data = {"nonce": directory.name, "mode": "restart", "head": SHA, "deadline": time.time() + 60}
    lifecycle.write_json(directory / "request.json", data)
    return directory, data


def info(pid=101, parent=1, command=None, cwd=None):
    return {"pid": pid, "ppid": parent, "exe": sys.executable, "cwd": str(cwd or ROOT),
            "create_time": float(pid), "cmdline": command or [sys.executable, "-m", "uvicorn",
                "backend.main:app", "--host", "0.0.0.0", "--port", "8001"]}


@pytest.mark.parametrize("restart", [False, True])
def test_dispatch_waits_for_new_invocation_receipt_and_always_requests_a_new_tab(pending, monkeypatch, restart):
    directory, _ = pending
    monkeypatch.setattr(lifecycle.shutil, "which", lambda _: "wt.exe")
    monkeypatch.setattr(lifecycle.startup, "git_head", lambda: SHA)
    monkeypatch.setattr(Path, "is_file", lambda self: self.name == ".env" or self.exists())
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        request_dir = Path(command[-1])
        data = lifecycle.request(request_dir)
        assert data["mode"] == ("restart" if restart else "start")
        lifecycle.result(request_dir, data, status="ready", pid=os.getpid(),
                         created=psutil.Process().create_time())

    monkeypatch.setattr(lifecycle.subprocess, "run", run)
    assert lifecycle.dispatch(restart) == lifecycle.dispatch(restart) == 0
    assert commands[0][-1] != commands[1][-1] != str(directory)
    for command in commands:
        assert command[1:4] == ["-w", "0", "new-tab"]
        assert command[-3:-1] == [str(ROOT / "start_backend.bat"), "--child"]


@pytest.mark.parametrize("outcome", ["activation_failed", "stale_receipt", "dispatch_failed", "timeout"])
def test_dispatch_never_reports_tab_dispatch_or_stale_receipt_as_success(pending, monkeypatch, outcome):
    monkeypatch.setattr(lifecycle.shutil, "which", lambda _: "wt.exe")
    monkeypatch.setattr(lifecycle.startup, "git_head", lambda: SHA)
    monkeypatch.setattr(Path, "is_file", lambda self: self.name == ".env" or self.exists())
    if outcome == "timeout":
        monkeypatch.setattr(lifecycle, "TIMEOUT", -1)

    def run(command, **kwargs):
        directory = Path(command[-1])
        if outcome == "dispatch_failed":
            raise subprocess.CalledProcessError(7, command)
        if outcome != "timeout":
            data = lifecycle.request(directory)
            lifecycle.result(directory, data, status="failed", error="activation failed")
            if outcome == "stale_receipt":
                receipt = json.loads((directory / "result.json").read_text())
                receipt["nonce"] = "old-request"
                lifecycle.write_json(directory / "result.json", receipt)

    monkeypatch.setattr(lifecycle.subprocess, "run", run)
    with pytest.raises((RuntimeError, subprocess.CalledProcessError)):
        lifecycle.dispatch(True)
    requests = [p for p in lifecycle.RUNTIME.iterdir() if p.name != "backend-request"]
    assert len(requests) == 1 and (requests[0] / "cancel").is_file()


def test_restart_selects_only_backend_python_roots_and_supervised_orphans():
    supervisor = info(100, command=[sys.executable, str(ROOT / "scripts/start_ai_stock.py"), "--service", "backend"])
    backend = info(parent=100)
    worker = info(102, 101, [sys.executable, str(ROOT / "scripts/monthly_unified_dataset_release_worker.py"),
                            "--serve", "--supervised", "--poll-seconds", "5.0"])
    unrelated = info(103, command=[sys.executable, "qe_experiment.py"])
    orphan = {**worker, "pid": 104, "ppid": 999, "create_time": 104.0}
    processes = {p["pid"]: p for p in [supervisor, backend, worker, unrelated, orphan]}
    assert {p["pid"] for p in lifecycle.targets(processes, {101})} == {100, 104}


@pytest.mark.parametrize("change", [{"cwd": "C:/OtherRepo"}, {"exe": "C:/other/python.exe"},
                                  {"cmdline": [sys.executable, "-m", "http.server", "8001"]}])
def test_unknown_or_other_repository_listener_is_never_killed(change, monkeypatch):
    monkeypatch.setattr(lifecycle, "snapshot", lambda: ({101: {**info(), **change}}, {101}))
    stop = Mock()
    monkeypatch.setattr(lifecycle, "stop_owned", stop)
    with pytest.raises(RuntimeError, match="unverified"):
        lifecycle.prepare(True)
    stop.assert_not_called()


def test_start_refuses_existing_backend_without_implicit_restart(monkeypatch):
    monkeypatch.setattr(lifecycle, "snapshot", lambda: ({101: info()}, {101}))
    stop = Mock()
    monkeypatch.setattr(lifecycle, "stop_owned", stop)
    with pytest.raises(RuntimeError, match="--restart explicitly"):
        lifecycle.prepare(False)
    stop.assert_not_called()


def test_pid_reuse_blocks_tree_termination(monkeypatch):
    monkeypatch.setattr(lifecycle.psutil, "Process", lambda _: Mock(create_time=lambda: 999.0))
    run = Mock()
    monkeypatch.setattr(lifecycle.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="PID was reused"):
        lifecycle.stop_owned(info())
    run.assert_not_called()


def test_already_exited_backend_does_not_break_restart(monkeypatch):
    def gone(_):
        raise psutil.NoSuchProcess(101)
    monkeypatch.setattr(lifecycle.psutil, "Process", gone)
    run = Mock()
    monkeypatch.setattr(lifecycle.subprocess, "run", run)
    lifecycle.stop_owned(info())
    run.assert_not_called()


@pytest.mark.parametrize("condition", ["cancel", "expired", "source_changed", "lock_busy"])
def test_failed_preconditions_do_not_stop_or_launch_a_backend(pending, monkeypatch, condition):
    directory, data = pending
    if condition == "cancel":
        (directory / "cancel").touch()
    if condition == "expired":
        data["deadline"] = 0
        lifecycle.write_json(directory / "request.json", data)
    monkeypatch.setattr(lifecycle.startup, "git_head", lambda: "b" * 40 if condition == "source_changed" else SHA)

    @contextmanager
    def lock(_):
        if condition == "lock_busy":
            raise RuntimeError("startup lock busy")
        yield

    monkeypatch.setattr(lifecycle.startup, "startup_lock", lock)
    prepare = Mock()
    monkeypatch.setattr(lifecycle, "prepare", prepare)
    with pytest.raises(RuntimeError):
        lifecycle.serve(directory)
    prepare.assert_not_called()
    assert json.loads((directory / "result.json").read_text())["status"] == "failed"


def test_success_releases_startup_lock_while_tab_supervises_the_job(pending, monkeypatch):
    directory, _ = pending
    locked = []

    @contextmanager
    def lock(_):
        locked.append(True)
        yield
        locked.pop()

    monkeypatch.setattr(lifecycle.startup, "startup_lock", lock)
    monkeypatch.setattr(lifecycle.startup, "git_head", lambda: SHA)
    monkeypatch.setattr(lifecycle, "prepare", Mock())
    monkeypatch.setattr(lifecycle, "managed_process", lambda *a: nullcontext(Mock(pid=101)))
    monkeypatch.setattr(lifecycle, "wait_ready", lambda *a: {"status": "ready", "pid": 101, "created": 101.0})
    def hold(*_):
        assert not locked, "A later restart must be able to acquire the startup lock"
        return 0
    monkeypatch.setattr(lifecycle, "hold", hold)
    assert lifecycle.serve(directory) == 0


@pytest.mark.parametrize("cause", ["wrong_listener", "source_changed", "exited", "identity_mismatch"])
def test_readiness_requires_live_new_pid_and_unchanged_source(pending, monkeypatch, cause):
    directory, data = pending
    process = Mock(pid=101, poll=lambda: 1 if cause == "exited" else None, returncode=1)
    monkeypatch.setattr(lifecycle.psutil, "Process", lambda _: Mock(create_time=lambda: 101.0))
    monkeypatch.setattr(lifecycle.socket, "create_connection", lambda *a, **k: nullcontext())
    def ready(*_):
        if cause == "identity_mismatch":
            raise RuntimeError("backend: old/different source")
        return SHA
    monkeypatch.setattr(lifecycle.startup, "ready", ready)
    monkeypatch.setattr(lifecycle.startup, "git_head", lambda: "b" * 40)
    monkeypatch.setattr(lifecycle, "snapshot", lambda: ({101: info()}, {999 if cause == "wrong_listener" else 101}))
    with pytest.raises(RuntimeError, match={"wrong_listener": "different/replaced", "source_changed": "Source changed",
                                           "exited": "exited before ready", "identity_mismatch": "old/different source"}[cause]):
        lifecycle.wait_ready(process, directory, data)


@pytest.mark.skipif(os.name != "nt", reason="Actual Windows Job process-tree behavior")
@pytest.mark.parametrize("abrupt", [False, True])
def test_real_windows_job_kills_disposable_child_and_grandchild_on_supervisor_exit(tmp_path, abrupt):
    pid_file = tmp_path / "grandchild.pid"
    child_code = ("import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
                  f"open({str(pid_file)!r},'w').write(str(p.pid)); time.sleep(60)")
    supervisor_code = (
        "import os,sys,time; sys.path.insert(0,sys.argv[1]); from scripts.backend_lifecycle import managed_process; "
        "from pathlib import Path\n"
        f"with managed_process([sys.executable,'-c',{child_code!r}],Path(sys.argv[2])) as p:\n"
        " print(p.pid,flush=True)\n"
        " deadline=time.monotonic()+10\n"
        " while not Path(sys.argv[3]).exists() and time.monotonic()<deadline: time.sleep(.05)\n"
        " assert Path(sys.argv[3]).exists()\n" + (" os._exit(0)\n" if abrupt else " pass\n"))
    completed = subprocess.run([sys.executable, "-X", "utf8", "-c", supervisor_code, str(ROOT),
                                str(tmp_path / "test.log"), str(pid_file)], capture_output=True,
                               text=True, timeout=20)
    assert completed.returncode == 0, completed.stderr
    child_pid, grandchild_pid = int(completed.stdout.strip()), int(pid_file.read_text())
    for pid in (child_pid, grandchild_pid):
        try:
            psutil.Process(pid).wait(timeout=10)
        except psutil.NoSuchProcess:
            pass


@pytest.mark.skipif(os.name != "nt", reason="Actual Windows suspended-process cleanup")
def test_failed_job_assignment_does_not_leave_a_suspended_process(tmp_path, monkeypatch):
    job = Mock(assign=Mock(side_effect=OSError("assignment denied")))
    monkeypatch.setattr(lifecycle, "Job", lambda: job)
    spawned = []
    original = subprocess.Popen
    def record(*args, **kwargs):
        process = original(*args, **kwargs)
        spawned.append(process)
        return process
    monkeypatch.setattr(lifecycle.subprocess, "Popen", record)
    with pytest.raises(OSError, match="assignment denied"):
        with lifecycle.managed_process([sys.executable, "-c", "import time; time.sleep(60)"], tmp_path / "test.log"):
            pytest.fail("Unmanaged child must never run")
    assert spawned[0].poll() is not None
    job.close.assert_called_once()


@pytest.mark.skipif(os.name != "nt", reason="Actual CMD to WT argument/activation failure propagation")
def test_real_bat_dispatches_fresh_tabs_and_returns_activation_failure_without_services(tmp_path):
    compiler = Path(os.environ["WINDIR"]) / "Microsoft.NET/Framework64/v4.0.30319/csc.exe"
    if not compiler.is_file():
        pytest.skip("Existing Windows .NET compiler unavailable")
    root = tmp_path / "space & \u4e2d"
    (root / "scripts").mkdir(parents=True)
    for name in ("backend_lifecycle.py", "start_ai_stock.py"):
        (root / "scripts" / name).write_bytes((ROOT / "scripts" / name).read_bytes())
    conda = tmp_path / "fake_conda.bat"
    conda.write_text("@echo off\nexit /b 7\n")
    body = (ROOT / "start_backend.bat").read_text()
    for key, value in (("AISTOCK_CONDA_BAT", str(conda)), ("AISTOCK_BACKEND_PYTHON", sys.executable)):
        old = next(line for line in body.splitlines() if line.startswith(f'set "{key}='))
        body = body.replace(old, f'set "{key}={value}"')
    (root / "start_backend.bat").write_text(body)
    (root / ".env").write_text("")
    for args in (["init", "-q"], ["add", "."], ["-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                                            "commit", "-qm", "fixture"]):
        subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
    source = tmp_path / "Capture.cs"
    source.write_text(r'''class Capture {
      static int Main(string[] a) {
        System.IO.File.AppendAllText(System.Environment.GetEnvironmentVariable("TAB_ARGS"),
          System.String.Join("\n", a)+"\nEND\n");
        int i=System.Array.IndexOf(a,"cmd.exe");
        var p=new System.Diagnostics.ProcessStartInfo("cmd.exe",
          "/d /c call \""+a[i+4]+"\" --child \""+a[i+6]+"\"");
        p.UseShellExecute=false; p.CreateNoWindow=true;
        var child=System.Diagnostics.Process.Start(p); child.WaitForExit(); return 0;
      }}''')
    subprocess.run([str(compiler), "/nologo", f"/out:{tmp_path / 'wt.exe'}", str(source)],
                   check=True, capture_output=True)
    args_file = tmp_path / "tabs.txt"
    env = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"], "TAB_ARGS": str(args_file),
           "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    for args in ([], ["--restart"]):
        completed = subprocess.run(["cmd.exe", "/d", "/c", "call", str(root / "start_backend.bat"), *args],
                                   env=env, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30)
        assert completed.returncode == 1, completed.stdout + completed.stderr
        assert "Conda activation failed" in completed.stdout + completed.stderr
    dispatches = [chunk.splitlines() for chunk in args_file.read_text(encoding="utf-8-sig").split("END\n") if chunk]
    assert len(dispatches) == 2
    assert all(args[:3] == ["-w", "0", "new-tab"] and args[-2] == "--child" for args in dispatches)
    assert dispatches[0][-1] != dispatches[1][-1]
    for args in dispatches:
        receipt = json.loads((Path(args[-1]) / "result.json").read_text())
        assert receipt["status"] == "failed"
        assert not (Path(args[-1]) / "backend.log").exists()


@pytest.mark.skipif(os.name != "nt", reason="Actual isolated-port startup/restart and Windows tree termination")
def test_real_isolated_restart_replaces_backend_and_worker_and_checks_identity(tmp_path, monkeypatch):
    # Only 8011 is used; no production application, database or Windows UI is started.
    import socket
    with socket.socket() as probe:
        try:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            probe.bind(("127.0.0.1", 8011))
        except OSError:
            pytest.skip("Isolated validation port 8011 is already in use")
    root = tmp_path / "isolated"
    (root / "scripts").mkdir(parents=True)
    for name in ("backend_lifecycle.py", "start_ai_stock.py"):
        source = (ROOT / "scripts" / name).read_text(encoding="utf-8").replace("8001", "8011")
        (root / "scripts" / name).write_text(source, encoding="utf-8")
    (root / "uvicorn").mkdir()
    (root / "uvicorn/__init__.py").write_text("")
    (root / "uvicorn/__main__.py").write_text('''import json,os,subprocess,sys,time
from http.server import BaseHTTPRequestHandler,HTTPServer
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
worker=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)'])
with open(os.environ['TEST_PID_LOG'],'a') as stream: stream.write(json.dumps([os.getpid(),worker.pid])+'\\n')
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  payload={'status':'healthy'} if self.path.endswith('/health') else {'status':'ready','merge_commit':head}
  body=json.dumps(payload).encode(); self.send_response(200); self.end_headers(); self.wfile.write(body)
 def log_message(self,*args): pass
HTTPServer(('127.0.0.1',8011),Handler).serve_forever()
''', encoding="utf-8")
    (root / ".env").write_text("")
    (root / "start_backend.bat").write_text("@echo off\nexit /b 99\n")  # WT dispatch is recorded below.
    for args in (["init", "-q"], ["add", "."], ["-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                                             "commit", "-qm", "fixture"]):
        subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
    monkeypatch.setattr(lifecycle, "ROOT", root)
    monkeypatch.setattr(lifecycle.startup, "ROOT", root)
    monkeypatch.setattr(lifecycle, "RUNTIME", tmp_path / "AIstock_runtime/startup")
    monkeypatch.setattr(lifecycle.shutil, "which", lambda _: "wt.exe")
    pid_log = tmp_path / "pids.jsonl"
    env = {**os.environ, "TEST_PID_LOG": str(pid_log), "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    original_run = subprocess.run
    supervisors, tabs, streams = [], [], []

    def run(command, **kwargs):
        if command[0] != "wt.exe":
            return original_run(command, **kwargs)
        assert command[1:4] == ["-w", "0", "new-tab"]
        tabs.append(command[-1])
        stream = (tmp_path / f"supervisor-{len(tabs)}.log").open("w", encoding="utf-8")
        streams.append(stream)
        supervisors.append(subprocess.Popen([sys.executable, "-X", "utf8", str(root / "scripts/backend_lifecycle.py"),
                                              "--serve", command[-1]], cwd=root, env=env, stdin=subprocess.DEVNULL,
                                             stdout=stream, stderr=subprocess.STDOUT))

    monkeypatch.setattr(lifecycle.subprocess, "run", run)
    monkeypatch.setattr(lifecycle, "TIMEOUT", 30)
    try:
        assert lifecycle.dispatch() == 0
        first = json.loads(pid_log.read_text().splitlines()[0])
        assert lifecycle.dispatch(True) == 0
        entries = [json.loads(line) for line in pid_log.read_text().splitlines()]
        assert len(entries) == 2 and entries[0] != entries[1]
        assert len(tabs) == len(set(tabs)) == 2
        for pid in first:
            assert not psutil.pid_exists(pid), "Restart must stop both old backend and its worker"
        for pid in entries[1]:
            assert psutil.pid_exists(pid)
    finally:
        for process in supervisors:
            if process.poll() is None:
                original_run(["taskkill.exe", "/PID", str(process.pid), "/T", "/F"], capture_output=True, timeout=15)
            process.wait(timeout=10)
        for stream in streams:
            stream.close()
    for line in pid_log.read_text().splitlines():
        for pid in json.loads(line):
            assert not psutil.pid_exists(pid), "Closing the service supervisor must release every descendant"
