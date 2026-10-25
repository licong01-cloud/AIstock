"""Explicit Windows backend-only start/restart, with a fresh tab and readiness receipt."""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import ctypes
from ctypes import wintypes
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from threading import Thread
import time
from uuid import uuid4

import psutil

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("backend_startup", ROOT / "scripts/start_ai_stock.py")
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Backend startup helper unavailable")
startup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(startup)
RUNTIME = startup.RUNTIME
TIMEOUT = 180


def write_json(path: Path, payload: dict) -> None:
    pending = path.with_suffix(".next.json")
    pending.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(pending, path)


def request(directory: Path) -> dict:
    if directory.resolve().parent != RUNTIME.resolve() or not directory.name.startswith("backend-"):
        raise RuntimeError("Invalid backend request directory")
    data = json.loads((directory / "request.json").read_text(encoding="utf-8"))
    if data.get("nonce") != directory.name or data.get("mode") not in ("start", "restart"):
        raise RuntimeError("Invalid backend request")
    return data


def active(directory: Path, data: dict) -> None:
    if (directory / "cancel").exists() or time.time() >= data["deadline"]:
        raise RuntimeError("Backend request cancelled or expired; no late startup permitted")


def result(directory: Path, data: dict, **values) -> None:
    write_json(directory / "result.json", {"nonce": data["nonce"], "expected_head": data["head"],
                                          "business_acceptance": "not_evaluated", **values})


def dispatch(restart: bool = False) -> int:
    terminal = shutil.which("wt.exe")
    if not terminal or not (ROOT / ".env").is_file():
        raise RuntimeError("Windows Terminal and the existing .env are required")
    head = startup.git_head()
    RUNTIME.mkdir(parents=True, exist_ok=True)
    directory = RUNTIME / ("backend-" + uuid4().hex)
    directory.mkdir()
    data = {"nonce": directory.name, "head": head, "mode": "restart" if restart else "start",
            "deadline": time.time() + TIMEOUT}
    write_json(directory / "request.json", data)
    print(f"Opening NEW backend tab; waiting for actual readiness. Logs: {directory}", flush=True)
    try:
        subprocess.run([terminal, "-w", "0", "new-tab", "--title", "AIstock Backend",
                        "--suppressApplicationTitle", "--startingDirectory", str(ROOT),
                        "cmd.exe", "/d", "/k", "call", str(ROOT / "start_backend.bat"),
                        "--child", str(directory)], check=True, timeout=15)
        while time.time() < data["deadline"]:
            receipt = directory / "result.json"
            if receipt.is_file():
                observed = json.loads(receipt.read_text(encoding="utf-8"))
                if observed.get("nonce") != data["nonce"] or observed.get("expected_head") != head:
                    raise RuntimeError("Backend receipt does not belong to this invocation")
                if observed.get("status") != "ready":
                    raise RuntimeError(f"Backend FAILED: {observed.get('error')}; logs: {directory}")
                process = psutil.Process(observed["pid"])
                if process.create_time() != observed["created"] or not process.is_running():
                    raise RuntimeError("New backend exited/replaced before readiness acknowledgement")
                print(f"Backend READY: new PID {process.pid}; source {head}; logs: {directory}", flush=True)
                return 0
            time.sleep(0.25)
        raise RuntimeError(f"Backend readiness timeout; request cancelled. Inspect new tab/logs: {directory}")
    except BaseException:
        (directory / "cancel").touch()
        raise


def snapshot() -> tuple[dict, set[int]]:
    # On Windows psutil.ppid() takes a system-wide parent snapshot per call.
    # Read parent IDs once through CIM, then inspect only plausible Python owners.
    script = r"""
    $ErrorActionPreference='Stop'
    [Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false)
    $p=@(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
      Select-Object ProcessId,ParentProcessId,ExecutablePath,CommandLine)
    ConvertTo-Json -InputObject $p -Depth 3 -Compress
    """
    completed = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                               check=True, capture_output=True, text=True, encoding="utf-8", timeout=15)
    rows = json.loads(completed.stdout)
    if not isinstance(rows, list):
        raise RuntimeError("Invalid backend process inventory")
    processes = {}
    for row in rows:
        if not row.get("ExecutablePath") or Path(row["ExecutablePath"]) != Path(sys.executable):
            continue
        command = str(row.get("CommandLine") or "").lower()
        if "uvicorn" not in command and not any(name in command for name in (
                "start_ai_stock.py", "backend_lifecycle.py", "monthly_unified_dataset_release_worker.py")):
            continue
        try:
            process = psutil.Process(row["ProcessId"])
            info = process.as_dict(attrs=["pid", "exe", "cmdline", "cwd", "create_time"])
            info["ppid"] = row["ParentProcessId"]
            processes[process.pid] = info
        except psutil.NoSuchProcess:
            continue
    listeners = {c.pid for c in psutil.net_connections(kind="tcp")
                 if c.status == psutil.CONN_LISTEN and c.laddr.port == 8001}
    if None in listeners:
        raise RuntimeError("Cannot identify owner of port 8001")
    return processes, listeners


def kind(info: dict) -> str | None:
    if (not info.get("exe") or not info.get("cwd")
            or Path(info["exe"]).resolve() != Path(sys.executable).resolve()
            or Path(info["cwd"]).resolve() != ROOT):
        return None
    args = list(info.get("cmdline") or [])[1:]
    if args[:2] == ["-X", "utf8"]:
        args = args[2:]
    if args == ["-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8001"]:
        return "backend"
    if not args:
        return None
    script = Path(args[0]).resolve()
    if script == ROOT / "scripts/start_ai_stock.py" and args[1:3] in (
            ["--child", "backend"], ["--service", "backend"]):
        return "supervisor"
    if script == ROOT / "scripts/backend_lifecycle.py" and args[1:2] == ["--serve"]:
        return "supervisor"
    if (script == ROOT / "scripts/monthly_unified_dataset_release_worker.py"
            and args[1:] == ["--serve", "--supervised", "--poll-seconds", "5.0"]):
        return "worker"
    return None


def targets(processes: dict, listeners: set[int]) -> list[dict]:
    if any(kind(processes.get(pid, {})) != "backend" for pid in listeners):
        raise RuntimeError("Port 8001 belongs to an unverified process; nothing was stopped")
    candidates = {pid for pid, info in processes.items() if pid != os.getpid() and kind(info)}
    roots = []
    for pid in candidates:
        parent, seen = processes[pid]["ppid"], {pid}
        while parent in processes and parent not in seen:
            seen.add(parent)
            if parent in candidates:
                break
            parent = processes[parent]["ppid"]
        else:
            roots.append(processes[pid])
    return roots


def stop_owned(info: dict) -> None:
    try:
        process = psutil.Process(info["pid"])
        if process.create_time() != info["create_time"]:
            raise RuntimeError("Backend PID was reused; refusing process control")
        # Recheck command/cwd/executable immediately before the exact tree operation.
        current = process.as_dict(attrs=["exe", "cwd", "cmdline"])
    except psutil.NoSuchProcess:
        return  # A concurrent natural exit already achieved the requested stop.
    if kind(current) is None:
        raise RuntimeError("Backend process ownership changed; refusing process control")
    descendants = [(p, p.create_time()) for p in process.children(recursive=True)]
    subprocess.run(["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                   check=True, capture_output=True, text=True, timeout=20)
    for child, created in [(process, info["create_time"]), *descendants]:
        try:
            if child.create_time() == created:
                child.wait(timeout=10)
        except psutil.NoSuchProcess:
            pass
    print(f"Stopped verified backend tree {process.pid}", flush=True)


def prepare(restart: bool) -> None:
    processes, listeners = snapshot()
    roots = targets(processes, listeners)
    if (roots or listeners) and not restart:
        raise RuntimeError("Backend already exists; use start_backend.bat --restart explicitly")
    for info in roots:
        stop_owned(info)
    processes, listeners = snapshot()
    if targets(processes, listeners) or listeners:
        raise RuntimeError("Old backend/worker remains; no duplicate backend started")
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        sock.bind(("0.0.0.0", 8001))


class Job:
    """Non-inheritable Windows Job: OS kills every descendant if this tab exits."""

    def __init__(self):
        class Basic(ctypes.Structure):
            _fields_ = [("per_process", ctypes.c_int64), ("per_job", ctypes.c_int64),
                        ("flags", wintypes.DWORD), ("min_ws", ctypes.c_size_t),
                        ("max_ws", ctypes.c_size_t), ("active", wintypes.DWORD),
                        ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD),
                        ("scheduling", wintypes.DWORD)]

        class Limits(ctypes.Structure):
            _fields_ = [("basic", Basic), ("io", ctypes.c_uint64 * 6),
                        ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                        ("peak_process", ctypes.c_size_t), ("peak_job", ctypes.c_size_t)]

        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        signatures = {
            "CreateJobObjectW": ([ctypes.c_void_p, wintypes.LPCWSTR], wintypes.HANDLE),
            "SetInformationJobObject": ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
            "OpenProcess": ([wintypes.DWORD, wintypes.BOOL, wintypes.DWORD], wintypes.HANDLE),
            "AssignProcessToJobObject": ([wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
            "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
        }
        for name, (args, returns) in signatures.items():
            function = getattr(self.api, name)
            function.argtypes, function.restype = args, returns
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = Limits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def assign(self, pid: int) -> None:
        handle = self.api.OpenProcess(0x0100 | 0x0001, False, pid)  # SET_QUOTA | TERMINATE
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            if not self.api.AssignProcessToJobObject(self.handle, handle):
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            self.api.CloseHandle(handle)

    def close(self) -> None:
        if self.handle:
            handle, self.handle = self.handle, None
            if not self.api.CloseHandle(handle):
                raise ctypes.WinError(ctypes.get_last_error())


@contextmanager
def managed_process(command: list[str], log: Path):
    job, process, reader = Job(), None, None
    try:
        _, cwd, env = startup.commands("backend")
        process = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   creationflags=0x4 | subprocess.CREATE_NEW_PROCESS_GROUP,
                                   text=True, encoding="utf-8", errors="replace", bufsize=1)
        # Start suspended: no backend code or worker may run before Job ownership.
        job.assign(process.pid)
        psutil.Process(process.pid).resume()

        def output():
            with log.open("a", encoding="utf-8") as stream:
                for line in process.stdout:
                    stream.write(line)
                    stream.flush()
                    print(line, end="", flush=True)

        reader = Thread(target=output, daemon=True)
        reader.start()
        yield process
    finally:
        job.close()
        if process is not None:
            if process.poll() is None:
                process.kill()  # Also handles assignment failure of a suspended child.
            process.wait(timeout=10)
            if reader is not None:
                reader.join(timeout=5)
            process.stdout.close()


def wait_ready(process, directory: Path, data: dict) -> dict:
    created = psutil.Process(process.pid).create_time()
    last = "not listening"
    progress = 0.0
    while True:
        active(directory, data)
        if process.poll() is not None:
            raise RuntimeError(f"Backend exited before ready ({process.returncode}); see backend.log")
        try:
            with socket.create_connection(("127.0.0.1", 8001), timeout=0.2):
                pass
            startup.ready("backend", data["head"])
        except (OSError, ValueError, RuntimeError) as exc:
            if isinstance(exc, RuntimeError) and "old/different source" in str(exc):
                raise
            last = str(exc)
        else:
            processes, listeners = snapshot()
            if listeners != {process.pid} or processes.get(process.pid, {}).get("create_time") != created:
                raise RuntimeError("Readiness came from a different/replaced backend process")
            if startup.git_head() != data["head"]:
                raise RuntimeError("Source changed during backend startup")
            active(directory, data)
            return {"status": "ready", "pid": process.pid, "created": created}
        if time.monotonic() >= progress:
            print(f"Waiting for NEW backend: {last}; log: {directory / 'backend.log'}", flush=True)
            progress = time.monotonic() + 5
        time.sleep(0.5)


def serve(directory: Path) -> int:
    data = request(directory)
    try:
        with ExitStack() as lifetime:
            with startup.startup_lock("backend"):
                active(directory, data)
                if startup.git_head() != data["head"]:
                    raise RuntimeError("Source changed before backend launch")
                print("Checking backend ownership and releasing old backend if explicitly requested...", flush=True)
                prepare(data["mode"] == "restart")
                active(directory, data)
                command, _, _ = startup.commands("backend")
                process = lifetime.enter_context(managed_process(command, directory / "backend.log"))
                result(directory, data, **wait_ready(process, directory, data))
            # Release the lock once ready so a later explicit restart can replace us.
            return hold(process, directory, data)
    except (Exception, KeyboardInterrupt) as exc:
        result(directory, data, status="failed", error=str(exc) or "Backend tab interrupted")
        raise


def hold(process, directory: Path, data: dict) -> int:
    # Cancellation remains effective even if a readiness receipt races the parent timeout.
    while process.poll() is None:
        if (directory / "cancel").exists():
            raise RuntimeError("Caller cancelled backend startup")
        time.sleep(0.5)
    result(directory, data, status="exited", exit_code=process.returncode)
    return process.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--serve", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--fail", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if sum((args.restart, args.serve is not None, args.fail is not None)) > 1:
            raise RuntimeError("Conflicting backend lifecycle arguments")
        if args.fail:
            data = request(args.fail)
            result(args.fail, data, status="failed", error="Conda activation failed; inspect the new backend tab")
            return 1
        return serve(args.serve) if args.serve else dispatch(args.restart)
    except (Exception, KeyboardInterrupt) as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
