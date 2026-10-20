"""Bounded, non-destructive Windows startup. Never kill, reload, or install."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import shutil
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
    $p=@(Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,CommandLine,ExecutablePath)
    $l=@(Get-NetTCPConnection -State Listen |
      Where-Object LocalPort -in @(8001,19080,3000) | Select-Object LocalPort,OwningProcess)
    @{processes=$p;listeners=$l} | ConvertTo-Json -Depth 4 -Compress
    """
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                            capture_output=True, text=True, timeout=15, check=True)
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
    if service == "backend":
        return [sys.executable, "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8001"], ROOT, env
    if service == "tdx":
        env["TDX_HTTP_PORT"] = "19080"
        return [sys.executable, str(ROOT / "scripts/start_tdx_go_backend.py"), "--database-target", "production",
                "--env-file", str(ROOT / ".env")], ROOT, env
    node = shutil.which("node.exe")
    if not node or not (ROOT / "frontend/node_modules/next/dist/bin/next").is_file():
        raise RuntimeError("Existing Node/Next.js dependencies missing; this launcher never installs dependencies")
    env["NEXT_DEV_PORT"] = "3000"
    env["NEXT_DIST_DIR"] = ".next-dev-3000"
    return [node, str(ROOT / "frontend/scripts/next-dev-isolated.mjs"), "--port", "3000"], ROOT / "frontend", env


def tdx_build(directory: Path) -> str:
    command, cwd, env = commands("tdx")
    log = directory / "tdx-build.log"
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen([*command, "--build-only"], cwd=cwd, env=env,
                                   stdout=output, stderr=subprocess.STDOUT)
        try:
            code = process.wait(timeout=180)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"TDX build still running; not killed or retried. Details: {log}") from exc
    if code:
        raise RuntimeError(f"TDX build failed ({code}); details: {log}")
    receipt = json.loads(log.read_text(encoding="utf-8").strip().splitlines()[-1])
    revision = receipt.get("source_revision", "")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("TDX build did not return a verified source revision")
    return revision


@contextmanager
def startup_lock():
    import msvcrt
    RUNTIME.mkdir(parents=True, exist_ok=True)
    with (RUNTIME / "startup.lock").open("a+b") as lock:
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


def launch(service: str, directory: Path) -> None:
    # One implementation, independent of Windows Terminal availability.
    command = [sys.executable, str(Path(__file__).resolve()), "--child", service, "--log-dir", str(directory)]
    console = os.environ.get("COMSPEC", r"C:\Windows\System32\cmd.exe")
    subprocess.Popen(f'"{console}" /d /k "{subprocess.list2cmdline(command)}"',
                     cwd=ROOT, creationflags=subprocess.CREATE_NEW_CONSOLE)


def child(service: str, directory: Path) -> int:
    if directory.resolve().parent != RUNTIME.resolve() or not directory.is_dir():
        raise RuntimeError("Invalid startup log directory")
    command, cwd, env = commands(service)
    with (directory / f"{service}.log").open("a", encoding="utf-8") as log:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1)
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        code = process.wait()
    (directory / f"{service}.exit.json").write_text(json.dumps({"exit_code": code}), encoding="utf-8")
    print(f"{service} exited ({code}); log: {directory / (service + '.log')}", flush=True)
    return code


def wait_ready(service: str, expected: str | None, directory: Path, timeout: float = 120) -> str:
    deadline, last = time.monotonic() + timeout, "not listening"
    while time.monotonic() < deadline:
        exit_file = directory / f"{service}.exit.json"
        if exit_file.exists():
            raise RuntimeError(f"{service} exited before ready: {exit_file.read_text(encoding='utf-8')}; logs: {directory}")
        current = state(service, inventory())
        if current == "listening":
            try:
                return ready(service, expected)
            except (OSError, ValueError, RuntimeError) as exc:
                if isinstance(exc, RuntimeError) and "old/different source" in str(exc):
                    raise
                last = str(exc)
        else:
            last = current
        time.sleep(2)
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


def start(check: bool = False) -> dict:
    head = git_head()
    if not (ROOT / ".env").is_file():
        raise RuntimeError("Missing .env; no services started")
    for service in PORTS:
        commands(service)
    snapshot = inventory()
    states = {service: state(service, snapshot) for service in PORTS}
    if check:
        return {"check_only": True, "head": head, "states": states, "services_started": False}
    with startup_lock():
        directory = RUNTIME / (time.strftime("%Y%m%d-%H%M%S-") + uuid4().hex[:8])
        directory.mkdir()
        print(f"Startup logs: {directory}", flush=True)
        result = {"logs": str(directory), "business_acceptance": "not_evaluated", "services": {}}
        for service in PORTS:
            current = settle(service)
            expected = head if service == "backend" else None
            if current == "absent":
                if service == "tdx":
                    expected = tdx_build(directory)
                if state(service, inventory()) != "absent":
                    raise RuntimeError(f"{service} appeared during preparation; rerun after inspection")
                launch(service, directory)
            revision = wait_ready(service, expected, directory)
            if git_head() != head:
                raise RuntimeError("Source HEAD changed during startup; readiness cannot be attributed to this start")
            result["services"][service] = {"prior_state": current, "observed_identity": revision}
        (directory / "summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Read-only prerequisite/port check; no build or startup")
    parser.add_argument("--child", choices=PORTS, help=argparse.SUPPRESS)
    parser.add_argument("--log-dir", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.child:
            if args.check or args.log_dir is None:
                raise RuntimeError("Child launch requires only its assigned log directory")
            return child(args.child, args.log_dir)
        print(json.dumps(start(args.check), ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        if args.child and args.log_dir and args.log_dir.resolve().parent == RUNTIME.resolve() and args.log_dir.is_dir():
            (args.log_dir / f"{args.child}.exit.json").write_text(
                json.dumps({"exit_code": 1, "error": str(exc)}), encoding="utf-8")
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
