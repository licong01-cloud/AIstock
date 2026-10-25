"""Bounded, non-destructive Windows startup. Never kill, reload, or install."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack, contextmanager
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
from urllib.request import ProxyHandler, build_opener
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
PORTS = {"tdx": 19080, "backend": 8001, "frontend": 3000}
RUNTIME = ROOT.parent / "AIstock_runtime" / "startup"


def git_head() -> str:
    value = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise RuntimeError("Cannot determine source HEAD")
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"],
                               cwd=ROOT, text=True).strip():
        raise RuntimeError("Tracked source is dirty; finish edits before starting services")
    return value


def inventory() -> dict:
    # One bounded snapshot, not a repository scan. Inspection failure is never 'free'.
    script = r"""
    $ErrorActionPreference='Stop'
    [Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false)
    $p=@(Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,CommandLine,ExecutablePath)
    $l=@(Get-NetTCPConnection -State Listen |
      Where-Object LocalPort -in @(8001,19080,3000) | Select-Object LocalPort,OwningProcess)
    @{processes=$p;listeners=$l} | ConvertTo-Json -Depth 4 -Compress
    """
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                            capture_output=True, text=True, encoding="utf-8", timeout=15, check=True)
    if not isinstance(result.stdout, str):
        raise RuntimeError("Windows process inventory produced no JSON")
    data = json.loads(result.stdout)
    if not isinstance(data.get("processes"), list) or not isinstance(data.get("listeners"), list):
        raise RuntimeError("Invalid Windows process/port inventory")
    return data


def owned(service: str, pid: int, data: dict) -> bool:
    processes = {p["ProcessId"]: p for p in data["processes"]}
    leaf = processes.get(pid, {})
    chain, seen = [], set()
    while pid in processes and pid not in seen:
        seen.add(pid)
        p = processes[pid]
        chain.append(str(p.get("CommandLine") or "").replace("/", "\\").lower())
        pid = p["ParentProcessId"]
    text = "\n".join(chain)
    root = str(ROOT).lower().rstrip("\\")
    located = any(root + suffix in text for suffix in ("\\", '"', " &", " &&"))
    if not located:
        return False
    own_command = chain[0] if chain else ""
    if (f"--child {service}" in own_command and "start_ai_stock.py" in own_command
            and "python" in str(leaf.get("ExecutablePath") or "").lower()):
        return True
    if service == "backend":
        return "uvicorn" in text and "backend.main:app" in text
    if service == "tdx":
        return (("start_tdx_go_backend.py" in text and "--database-target production" in text)
                or "\\production\\tdx-backend.exe" in text) and "--database-target dev" not in text
    return "node" in text and ("next" in text or "next-dev-isolated" in text)


def state(service: str, data: dict) -> str:
    listeners = {p["OwningProcess"] for p in data["listeners"] if p["LocalPort"] == PORTS[service]}
    if any(not owned(service, pid, data) for pid in listeners):
        raise RuntimeError(f"{service}: port {PORTS[service]} belongs to an unknown process; no process was stopped")
    if listeners:
        return "listening"
    if any(owned(service, p["ProcessId"], data) for p in data["processes"]):
        return "starting_or_stopping"
    if service == "backend":
        root = str(ROOT).lower().replace("/", "\\")
        if any(root + "\\scripts\\monthly_unified_dataset_release_worker.py" in
               str(p.get("CommandLine") or "").lower().replace("/", "\\")
               for p in data["processes"]):
            return "old_monthly_worker"
    return "absent"


def request(path: str, port: int) -> dict:
    with build_opener(ProxyHandler({})).open(f"http://127.0.0.1:{port}{path}", timeout=2) as response:
        if port == 3000:
            return {"status": response.status}
        return json.load(response)


def ready(service: str, expected: str | None = None) -> str:
    """Transport/readiness only, explicitly not final business acceptance."""
    port = PORTS[service]
    if service == "frontend":
        if request("/", port)["status"] != 200:
            raise RuntimeError("frontend HTTP readiness failed")
        return "frontend-ready"
    if service == "backend":
        health = request("/api/v1/health", port)
        identity = request("/api/v1/runtime-identity", port)
        revision = identity.get("merge_commit", "")
        valid = health.get("status") in ("ok", "healthy") and identity.get("status") == "ready"
    else:
        health = request("/api/health", port)
        identity = request("/api/runtime-identity", port).get("data", {})
        revision = identity.get("source_revision", "")
        valid = (health.get("metadata_ready") is True and identity.get("database_target") == "production"
                 and str(identity.get("listen_address", "")).endswith(":19080"))
    if not valid or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError(f"{service}: readiness or runtime identity is invalid")
    if expected is not None and revision != expected:
        raise RuntimeError(f"{service}: old/different source {revision}; expected {expected}. Close its service window first")
    return revision


def commands(service: str) -> tuple[list[str], Path, dict]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    # Popen's decoding does not change the child's encoding for redirected stdout.
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    if service == "backend":
        return [sys.executable, "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8001"], ROOT, env
    if service != "frontend":
        raise ValueError(f"No generic command for service: {service}")
    node = shutil.which("node.exe")
    if not node or not (ROOT / "frontend/node_modules/next/dist/bin/next").is_file():
        raise RuntimeError("Existing Node/Next.js dependencies missing; this launcher never installs dependencies")
    env["NEXT_DEV_PORT"] = "3000"
    env["NEXT_DIST_DIR"] = ".next-dev-3000"
    return [node, str(ROOT / "frontend/scripts/next-dev-isolated.mjs"), "--port", "3000"], ROOT / "frontend", env


def load_tdx_launcher():
    spec = importlib.util.spec_from_file_location("tdx_launcher", ROOT / "scripts/start_tdx_go_backend.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("TDX launcher module is unavailable")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper


def cached_tdx_command(directory: Path) -> tuple[list[str], Path, dict]:
    """Reuse the fixed EXE; build offline only on first use or changed inputs."""
    helper = load_tdx_launcher()
    command, env = helper.prepare_launch(ROOT / ".env", "production")
    receipt = helper.cached_build(Path(command[0]), helper.build_fingerprint(env))
    if receipt is None:
        print("TDX fixed EXE missing or source changed; preparing one offline build (other services do not wait)",
              flush=True)
        env["AISTOCK_TDX_ENV_FILE"] = str((ROOT / ".env").resolve())
        receipt = helper.ensure_binary(Path(command[0]), env)
    else:
        receipt = {**receipt, "build_action": "reused"}
    pending = directory / "tdx-executable.next.json"
    pending.write_text(json.dumps(receipt), encoding="utf-8")
    os.replace(pending, directory / "tdx-executable.json")
    print(f"TDX verified fixed EXE {receipt['build_action']}: {command[0]}", flush=True)
    return command, ROOT / "tdx-api-main/web", env


@contextmanager
def startup_lock(service: str = "all"):
    import msvcrt
    RUNTIME.mkdir(parents=True, exist_ok=True)
    if service not in (*PORTS, "all"):
        raise ValueError("Invalid startup lock service")
    with (RUNTIME / f"startup-{service}.lock").open("a+b") as lock:
        if lock.tell() == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise RuntimeError("Another one-click startup is still in progress; do not start a second copy") from exc
        try:
            yield
        finally:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def launch(service: str, directory: Path) -> subprocess.Popen:
    # The BAT entry already created this service's tab. Keep its child attached;
    # never resolve MRU again or create a second window from this delayed stage.
    command = [sys.executable, "-X", "utf8", str(Path(__file__).resolve()),
               "--child", service, "--log-dir", str(directory)]
    return subprocess.Popen(command, cwd=ROOT)


def child(service: str, directory: Path) -> int:
    if directory.resolve().parent != RUNTIME.resolve() or not directory.is_dir():
        raise RuntimeError("Invalid startup log directory")
    # Also protect direct --child invocation under a legacy GBK host.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    if service == "tdx":
        os.environ["TDX_HTTP_PORT"] = "19080"
    command, cwd, env = cached_tdx_command(directory) if service == "tdx" else commands(service)
    with (directory / f"{service}.log").open("a", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1)
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(f"[{service}] {line}", end="", flush=True)
        code = process.wait()
    (directory / f"{service}.exit.json").write_text(json.dumps({"exit_code": code}), encoding="utf-8")
    print(f"{service} exited ({code}); log: {directory / (service + '.log')}", flush=True)
    return code


def wait_ready(service: str, expected: str | Path | None, directory: Path, timeout: float = 120) -> str:
    started = time.monotonic()
    deadline, next_progress, last = started + timeout, started, "not listening"
    while time.monotonic() < deadline:
        if time.monotonic() >= next_progress:
            print(f"{service}: waiting for readiness ({int(time.monotonic() - started)}s/{timeout:g}s); "
                  f"{last}; logs: {directory}", flush=True)
            next_progress = time.monotonic() + 5
        exit_file = directory / f"{service}.exit.json"
        if exit_file.exists():
            raise RuntimeError(f"{service} exited before ready: {exit_file.read_text(encoding='utf-8')}; logs: {directory}")
        revision_hint = expected
        if isinstance(expected, Path):
            if not expected.exists():
                last = "waiting for TDX fixed EXE verification/build receipt"
                time.sleep(0.5)
                continue
            receipt = json.loads(expected.read_text(encoding="utf-8"))
            if not isinstance(receipt, dict):
                raise RuntimeError("TDX cached executable receipt is invalid")
            revision_hint = receipt.get("source_revision", "")
            if (receipt.get("schema_version") != "aistock_tdx_build_v1"
                    or not re.fullmatch(r"[0-9a-f]{40}", str(revision_hint))):
                raise RuntimeError("TDX cached executable receipt is invalid")
        # Avoid starting PowerShell/CIM for every failed readiness poll. A TCP
        # connection is only a cheap hint, never ownership or readiness evidence.
        try:
            with socket.create_connection(("127.0.0.1", PORTS[service]), timeout=0.2):
                listening = True
        except OSError:
            listening = False
        if listening:
            try:
                revision = ready(service, revision_hint)
            except (OSError, ValueError, RuntimeError) as exc:
                if isinstance(exc, RuntimeError) and "old/different source" in str(exc):
                    raise
                last = str(exc)
            else:
                # Re-read the PID/ancestry at acceptance; never trust a socket
                # opening or an HTTP 200 from an unknown replacement listener.
                if state(service, inventory()) == "listening":
                    print(f"{service}: ready ({time.monotonic() - started:.1f}s)", flush=True)
                    return revision
                last = "listener disappeared before ownership verification"
        else:
            last = "not listening"
        time.sleep(0.5)
    raise RuntimeError(f"{service} readiness timeout: {last}; logs: {directory}. No processes were killed or retried")


def settle(service: str, timeout: float = 120) -> str:
    deadline = time.monotonic() + timeout
    while True:
        current = state(service, inventory())
        if current in ("absent", "listening"):
            return current
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{service}: {current} did not settle; close its service window and wait. No duplicate started")
        time.sleep(2)


def start(check: bool = False, children: list | None = None, service: str | None = None) -> dict:
    if service is not None and service not in PORTS:
        raise ValueError("Invalid startup service")
    services = [service] if service is not None else list(PORTS)
    if not check:
        print("Checking source and startup prerequisites...", flush=True)
    head = git_head()
    if not (ROOT / ".env").is_file():
        raise RuntimeError("Missing .env; no services started")
    if check:
        for service in ("backend", "frontend"):
            commands(service)
        snapshot = inventory()
        states = {service: state(service, snapshot) for service in services}
        return {"check_only": True, "head": head, "states": states, "services_started": False}
    with ExitStack() as locks:
        # Tabs run independently; only duplicate launches of the same service
        # contend. A direct all-service invocation acquires the same three locks.
        for selected in services:
            locks.enter_context(startup_lock(selected))
        directory = RUNTIME / (time.strftime("%Y%m%d-%H%M%S-") + uuid4().hex[:8])
        directory.mkdir()
        print(f"Startup logs: {directory}", flush=True)
        result = {"logs": str(directory), "business_acceptance": "not_evaluated", "services": {}}
        print(f"Checking service ports and process ownership: {', '.join(services)}...", flush=True)
        snapshot = inventory()
        states = {service: state(service, snapshot) for service in services}
        expected_revisions, errors = {}, []
        # Old processes may need to settle; never delay the other absent services.
        order = sorted(services, key=lambda service: states[service] not in ("absent", "listening"))
        for service in order:
            if git_head() != head:
                raise RuntimeError("Source HEAD changed during startup; no further services started")
            result["services"][service] = {"prior_state": states[service]}
            try:
                current = states[service]
                if current not in ("absent", "listening"):
                    current = settle(service)
                expected = head if service == "backend" else None
                if current == "absent":
                    print(f"{service}: starting in this console", flush=True)
                    process = launch(service, directory)
                    if children is not None:
                        children.append(process)
                    if service == "tdx":
                        expected = directory / "tdx-executable.json"
                expected_revisions[service] = expected
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
                errors.append(f"{service}: {exc}")
                result["services"][service]["error"] = str(exc)
        # One shared readiness interval, not three serial 120-second waits.
        with ThreadPoolExecutor(max_workers=len(services)) as pool:
            waits = {pool.submit(wait_ready, service, expected, directory): service
                     for service, expected in expected_revisions.items()}
            for completed in as_completed(waits):
                service = waits[completed]
                try:
                    result["services"][service]["observed_identity"] = completed.result()
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
                    print(f"{service}: FAILED: {exc}", file=sys.stderr, flush=True)
                    errors.append(f"{service}: {exc}")
                    result["services"][service]["error"] = str(exc)
        if git_head() != head:
            errors.append("Source HEAD changed during startup; readiness cannot be attributed to this start")
        result["errors"] = errors
        (directory / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        if errors:
            raise RuntimeError("; ".join(errors) + f"; summary: {directory / 'summary.json'}")
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Read-only prerequisite/port check; no build or startup")
    parser.add_argument("--service", choices=PORTS, help="Start only the service assigned to this tab")
    parser.add_argument("--child", choices=PORTS, help=argparse.SUPPRESS)
    parser.add_argument("--log-dir", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    children: list[subprocess.Popen] = []
    code = 0
    try:
        if args.child:
            if args.check or args.service or args.log_dir is None:
                raise RuntimeError("Child launch requires only its assigned log directory")
            return child(args.child, args.log_dir)
        if args.log_dir is not None:
            raise RuntimeError("Log directory is valid only for an internal child")
        result = start(args.check, children, args.service) if args.service else start(args.check, children)
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        if args.child and args.log_dir and args.log_dir.resolve().parent == RUNTIME.resolve() and args.log_dir.is_dir():
            (args.log_dir / f"{args.child}.exit.json").write_text(
                json.dumps({"exit_code": 1, "error": str(exc)}), encoding="utf-8")
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        code = 1
    if children:
        print("Service processes remain attached to this tab; logs are in the startup log directory.",
              flush=True)
        # Keep the tab foreground while its child runs. Never kill/restart it.
        for process in children:
            if process.wait() != 0:
                code = 1
    return code


if __name__ == "__main__":
    raise SystemExit(main())
