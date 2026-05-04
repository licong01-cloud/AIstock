from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.services.validation.plan_catalog import FORBIDDEN_BACKEND_PORTS, REPO_ROOT, ValidationPlanCatalog


DEFAULT_EXECUTION_ROOT = REPO_ROOT / "tmp" / "validation" / "runner" / "jobs"
JOB_SCHEMA_VERSION = "aistock_validation_execution_job_v1"
EVIDENCE_SCHEMA_VERSION = "aistock_validation_runner_evidence_v1"
JOB_ID_RE = re.compile(r"^valjob_[0-9]{8}_[0-9]{6}_[0-9a-f]{8}$")


@dataclass(frozen=True)
class RunnerResult:
    return_code: int | None
    output: str
    timed_out: bool = False


RunnerExecutor = Callable[[list[str], dict[str, str], Path, int], RunnerResult]


class ValidationRunnerError(ValueError):
    """Raised when a requested validation execution violates the controlled-runner contract."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def default_runner_executor(command: list[str], env: dict[str, str], cwd: Path, timeout_seconds: int) -> RunnerResult:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            shell=False,
            check=False,
        )
        return RunnerResult(return_code=completed.returncode, output=completed.stdout or "")
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        return RunnerResult(return_code=None, output=output, timed_out=True)


class ValidationExecutionRunner:
    """Run allowlisted validation nox sessions and persist local job evidence."""

    def __init__(
        self,
        *,
        plan_catalog: ValidationPlanCatalog | None = None,
        execution_root: Path | None = None,
        repo_root: Path | None = None,
        executor: RunnerExecutor | None = None,
        run_inline: bool = False,
    ) -> None:
        self.plan_catalog = plan_catalog or ValidationPlanCatalog()
        self.execution_root = Path(execution_root or DEFAULT_EXECUTION_ROOT)
        self.repo_root = Path(repo_root or REPO_ROOT)
        self.executor = executor or default_runner_executor
        self.run_inline = run_inline
        self._lock = threading.Lock()

    def health(self) -> dict[str, Any]:
        jobs = self.list_jobs(page=1, page_size=1000)["items"]
        by_status: dict[str, int] = {}
        for job in jobs:
            status = str(job.get("status") or "unknown")
            by_status[status] = by_status.get(status, 0) + 1
        return {
            "mode": "controlled_execution",
            "execution_root": _repo_path(self.execution_root),
            "exists": self.execution_root.exists(),
            "job_count": len(jobs),
            "jobs_by_status": dict(sorted(by_status.items())),
            "allowed_command_type": "nox_session_allowlist_only",
            "arbitrary_shell_allowed": False,
            "production_8001_touched": False,
        }

    def list_jobs(self, *, page: int = 1, page_size: int = 20, status: str | None = None) -> dict[str, Any]:
        jobs = [job for job in self._load_jobs() if not status or job.get("status") == status]
        jobs.sort(key=lambda item: str(item.get("requested_at") or ""), reverse=True)
        start = (page - 1) * page_size
        items = jobs[start : start + page_size]
        return {"items": items, "total": len(jobs), "page": page, "page_size": page_size, "has_more": start + page_size < len(jobs)}

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        if not JOB_ID_RE.fullmatch(str(job_id)):
            return None
        path = self._job_path(job_id)
        return _read_json(path)

    def start_job(
        self,
        *,
        plan_key: str,
        requested_by: str = "operator",
        backend_port: int | None = None,
        frontend_port: int | None = None,
        timeout_seconds: int | None = None,
        confirm_text: str | None = None,
    ) -> dict[str, Any]:
        plan = self._validate_plan(plan_key, confirm_text=confirm_text)
        resolved_backend_port = self._resolve_port(plan, backend_port, "backend")
        resolved_frontend_port = self._resolve_port(plan, frontend_port, "frontend")
        timeout = self._resolve_timeout(plan, timeout_seconds)
        job_id = f"valjob_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        command = [sys.executable, "-m", "nox", "-s", str(plan["nox_session"])]
        env = self._runner_env(plan, resolved_backend_port, resolved_frontend_port)
        job = {
            "schema_version": JOB_SCHEMA_VERSION,
            "job_id": job_id,
            "status": "queued",
            "plan_key": plan["plan_key"],
            "title": plan.get("title"),
            "module": plan.get("module"),
            "level": plan.get("level"),
            "command_key": plan.get("command_key"),
            "nox_session": plan.get("nox_session"),
            "command": command,
            "cwd": _repo_path(self.repo_root),
            "requested_by": requested_by,
            "requested_at": _now_iso(),
            "started_at": None,
            "finished_at": None,
            "timeout_seconds": timeout,
            "return_code": None,
            "backend_port": resolved_backend_port,
            "frontend_port": resolved_frontend_port,
            "writes_database": bool(plan.get("writes_database")),
            "writes_artifacts": bool(plan.get("writes_artifacts")),
            "writes_business_state": bool(plan.get("writes_business_state")),
            "production_8001_touched": False,
            "arbitrary_shell_allowed": False,
            "log_path": _repo_path(self._log_path(job_id)),
            "evidence_path": _repo_path(self._evidence_path(job_id)),
            "error": None,
        }
        self._write_job(job)
        if self.run_inline:
            self._run_job(job_id, command, env, timeout)
            current = self.get_job(job_id)
            return current or job
        thread = threading.Thread(target=self._run_job, args=(job_id, command, env, timeout), daemon=True)
        thread.start()
        current = self.get_job(job_id)
        return current or job

    def _validate_plan(self, plan_key: str, *, confirm_text: str | None) -> dict[str, Any]:
        plan = self.plan_catalog.get_plan(plan_key)
        if plan is None:
            raise ValidationRunnerError(f"validation plan not found: {plan_key}")
        if not plan.get("enabled", True):
            raise ValidationRunnerError(f"validation plan is disabled: {plan_key}")
        if not plan.get("runner_enabled", False):
            raise ValidationRunnerError(f"validation plan is not enabled for controlled runner: {plan_key}")
        if plan.get("writes_business_state"):
            raise ValidationRunnerError("controlled runner refuses plans that write business state")
        if plan.get("requires_confirmation") and confirm_text != plan_key:
            raise ValidationRunnerError(f"validation plan {plan_key} requires confirm_text={plan_key!r}")
        return plan

    @staticmethod
    def _resolve_port(plan: dict[str, Any], requested_port: int | None, label: str) -> int | None:
        if label == "backend":
            required = bool(plan.get("requires_backend"))
            allowed_ports = [int(port) for port in plan.get("allowed_backend_ports") or []]
            forbidden = FORBIDDEN_BACKEND_PORTS
        else:
            required = bool(plan.get("requires_frontend"))
            allowed_ports = [int(port) for port in plan.get("allowed_frontend_ports") or []]
            forbidden = set()
        if requested_port is not None and int(requested_port) in forbidden:
            raise ValidationRunnerError(f"refusing forbidden production {label} port {requested_port}")
        if not required:
            if requested_port is not None:
                raise ValidationRunnerError(f"plan does not require {label}; refusing unexpected {label}_port")
            return None
        if not allowed_ports:
            raise ValidationRunnerError(f"plan requires {label} but has no allowed {label} ports")
        resolved = int(requested_port) if requested_port is not None else allowed_ports[0]
        if resolved not in allowed_ports:
            raise ValidationRunnerError(f"{label}_port {resolved} is not allowlisted for plan {plan['plan_key']}")
        return resolved

    @staticmethod
    def _resolve_timeout(plan: dict[str, Any], requested_timeout: int | None) -> int:
        plan_timeout = int(plan.get("max_duration_seconds") or 300)
        if plan_timeout <= 0:
            raise ValidationRunnerError("plan max_duration_seconds must be positive")
        if requested_timeout is None:
            return min(plan_timeout, 1800)
        timeout = int(requested_timeout)
        if timeout <= 0:
            raise ValidationRunnerError("timeout_seconds must be positive")
        if timeout > plan_timeout:
            raise ValidationRunnerError("timeout_seconds cannot exceed plan max_duration_seconds")
        return timeout

    @staticmethod
    def _runner_env(plan: dict[str, Any], backend_port: int | None, frontend_port: int | None) -> dict[str, str]:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["VALIDATION_RUNNER_JOB"] = "1"
        env["VALIDATION_RUNNER_PLAN_KEY"] = str(plan["plan_key"])
        if backend_port is not None:
            env["BACKEND_PORT"] = str(backend_port)
            env["NEXT_PUBLIC_API_BASE"] = f"http://127.0.0.1:{backend_port}/api/v1"
        if frontend_port is not None:
            env["FRONTEND_PORT"] = str(frontend_port)
        return env

    def _run_job(self, job_id: str, command: list[str], env: dict[str, str], timeout: int) -> None:
        job = self.get_job(job_id)
        if job is None:
            return
        job["status"] = "running"
        job["started_at"] = _now_iso()
        self._write_job(job)
        result = self.executor(command, env, self.repo_root, timeout)
        log_path = self._log_path(job_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(result.output, encoding="utf-8")
        job = self.get_job(job_id) or job
        job["finished_at"] = _now_iso()
        job["return_code"] = result.return_code
        if result.timed_out:
            job["status"] = "timeout"
            job["error"] = f"validation execution exceeded {timeout} seconds"
        elif result.return_code == 0:
            job["status"] = "passed"
        else:
            job["status"] = "failed"
            job["error"] = f"nox session exited with return_code={result.return_code}"
        self._write_job(job)
        self._write_evidence(job)

    def _write_evidence(self, job: dict[str, Any]) -> None:
        evidence_path = self._evidence_path(str(job["job_id"]))
        job_path = self._job_path(str(job["job_id"]))
        log_path = self._log_path(str(job["job_id"]))
        payload = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "job_id": job["job_id"],
            "plan_key": job["plan_key"],
            "status": job["status"],
            "production_8001_touched": False,
            "evidence": [
                self._evidence_entry("job_record", job_path),
                self._evidence_entry("execution_log", log_path),
            ],
        }
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    @staticmethod
    def _evidence_entry(kind: str, path: Path) -> dict[str, Any]:
        return {
            "kind": kind,
            "path": _repo_path(path),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() and path.is_file() else None,
            "sha256": _sha256(path),
        }

    def _load_jobs(self) -> list[dict[str, Any]]:
        if not self.execution_root.exists():
            return []
        jobs: list[dict[str, Any]] = []
        for path in self.execution_root.glob("*.json"):
            if path.name.endswith("_evidence.json"):
                continue
            payload = _read_json(path)
            if payload and payload.get("schema_version") == JOB_SCHEMA_VERSION:
                jobs.append(payload)
        return jobs

    def _write_job(self, job: dict[str, Any]) -> None:
        with self._lock:
            path = self._job_path(str(job["job_id"]))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(job, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _job_path(self, job_id: str) -> Path:
        return self.execution_root / f"{job_id}.json"

    def _log_path(self, job_id: str) -> Path:
        return self.execution_root / f"{job_id}.log"

    def _evidence_path(self, job_id: str) -> Path:
        return self.execution_root / f"{job_id}_evidence.json"
