from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.services.validation.history_store import COVERAGE_SCHEMA, DEFAULT_HISTORY_ROOT, EVIDENCE_SCHEMA, RUN_SCHEMA
from backend.services.validation.plan_catalog import FORBIDDEN_BACKEND_PORTS, REPO_ROOT, ValidationPlanCatalog


DEFAULT_EXECUTION_ROOT = REPO_ROOT / "tmp" / "validation" / "runner" / "jobs"
HISTORY_RELATIVE_ROOT = Path("tests") / "aistock_validation" / "history"
JOB_SCHEMA_VERSION = "aistock_validation_execution_job_v1"
EVIDENCE_SCHEMA_VERSION = "aistock_validation_runner_evidence_v1"
JOB_ID_RE = re.compile(r"^valjob_[0-9]{8}_[0-9]{6}_[0-9a-f]{8}$")
MAX_LOG_BYTES = 512 * 1024
MAX_LOG_TAIL_LINES = 2000
ARTIFACT_KIND_SUFFIXES = {
    "guardrail_md": ".txt",
}
_WORKSPACE_PATH_ALLOWLIST: tuple[str, ...] = (
    "F:/Dev/AIstock_worktrees/",
    "F:\\Dev\\AIstock_worktrees\\",
)


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


def _filename_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _is_inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _is_allowlisted_workspace_path(path: Path) -> bool:
    resolved = str(path.resolve()).replace("\\", "/")
    return any(resolved.startswith(prefix.replace("\\", "/")) for prefix in _WORKSPACE_PATH_ALLOWLIST)


def _safe_slug(value: Any, *, default: str = "validation") -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or default


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
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _read_tail_bytes(path: Path, max_bytes: int) -> tuple[bytes, bool]:
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > max_bytes:
            handle.seek(-max_bytes, os.SEEK_END)
            return handle.read(max_bytes), True
        return handle.read(), False


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _copy_if_exists(src: Path, dst: Path) -> bool:
    if not src.exists() or not src.is_file():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def _git_commit(repo_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    value = (completed.stdout or "").strip()
    return value or None


def _git_branch(repo_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(repo_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    value = (completed.stdout or "").strip()
    return value or None


def _history_id_for_path(path: Path, history_root: Path) -> str:
    relative = path.resolve().relative_to(history_root.resolve()).as_posix()
    digest = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:10]
    readable = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(relative).with_suffix("").as_posix())
    return f"{readable}__{digest}"


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
        history_root: Path | None = None,
        repo_root: Path | None = None,
        executor: RunnerExecutor | None = None,
        run_inline: bool = False,
        archive_enabled: bool = True,
    ) -> None:
        self.plan_catalog = plan_catalog or ValidationPlanCatalog()
        self.execution_root = Path(execution_root or DEFAULT_EXECUTION_ROOT)
        self.history_root = Path(history_root or DEFAULT_HISTORY_ROOT)
        self._history_root_explicit = history_root is not None
        self.repo_root = Path(repo_root or REPO_ROOT)
        self.executor = executor or default_runner_executor
        self.run_inline = run_inline
        self.archive_enabled = archive_enabled
        self._lock = threading.Lock()

    @staticmethod
    def _validate_workspace_path(workspace_path: str | None, repo_root: Path) -> Path:
        if workspace_path is None:
            return repo_root.resolve()
        resolved = Path(workspace_path).resolve()
        if not resolved.is_dir():
            raise ValidationRunnerError(f"workspace_path is not a directory: {workspace_path}")
        resolved_str = str(resolved).replace("\\", "/")
        allowed = any(
            resolved_str.startswith(prefix.replace("\\", "/"))
            for prefix in _WORKSPACE_PATH_ALLOWLIST
        )
        try:
            resolved.relative_to(repo_root.resolve())
            allowed = True
        except ValueError:
            pass
        if not allowed:
            raise ValidationRunnerError(
                f"workspace_path is not in the allowlist: {workspace_path}. "
                f"Must be under F:/Dev/AIstock_worktrees/ or the configured repo root."
            )
        return resolved

    def health(self) -> dict[str, Any]:
        jobs = self.list_jobs(page=1, page_size=1000)["items"]
        by_status: dict[str, int] = {}
        for job in jobs:
            status = str(job.get("status") or "unknown")
            by_status[status] = by_status.get(status, 0) + 1
        return {
            "mode": "controlled_execution",
            "execution_root": _repo_path(self.execution_root),
            "history_root": _repo_path(self.history_root),
            "exists": self.execution_root.exists(),
            "archive_enabled": self.archive_enabled,
            "job_count": len(jobs),
            "jobs_by_status": dict(sorted(by_status.items())),
            "allowed_command_type": "nox_session_allowlist_only",
            "arbitrary_shell_allowed": False,
            "production_8001_touched": False,
        }

    def list_jobs(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status: str | None = None,
        plan_key: str | None = None,
        module: str | None = None,
    ) -> dict[str, Any]:
        jobs = self._load_jobs()
        if status:
            status_l = status.lower()
            jobs = [job for job in jobs if str(job.get("status") or "").lower() == status_l]
        if plan_key:
            plan_l = plan_key.lower()
            jobs = [job for job in jobs if plan_l in str(job.get("plan_key") or "").lower()]
        if module:
            module_l = module.lower()
            jobs = [job for job in jobs if module_l in str(job.get("module") or "").lower()]
        jobs.sort(key=lambda item: str(item.get("requested_at") or ""), reverse=True)
        start = (page - 1) * page_size
        items = jobs[start : start + page_size]
        return {"items": items, "total": len(jobs), "page": page, "page_size": page_size, "has_more": start + page_size < len(jobs)}

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        if not JOB_ID_RE.fullmatch(str(job_id)):
            return None
        path = self._job_path(job_id)
        return _read_json(path)

    def get_job_log(self, job_id: str, *, tail_lines: int = 300) -> dict[str, Any] | None:
        job = self.get_job(job_id)
        if job is None:
            return None
        path = self._log_path(job_id)
        if not path.exists():
            archived = self._archive_path_from_job(job, "runner_log_archive_path")
            if archived and archived.exists():
                path = archived
        if not path.exists() or not path.is_file():
            return {
                "job_id": job_id,
                "exists": False,
                "path": _repo_path(path),
                "content": "",
                "tail_lines": tail_lines,
                "truncated": False,
                "size_bytes": None,
                "sha256": None,
            }
        safe_tail = max(1, min(int(tail_lines), MAX_LOG_TAIL_LINES))
        raw, truncated_bytes = _read_tail_bytes(path, MAX_LOG_BYTES)
        text = raw.decode("utf-8", errors="replace")
        lines = text.splitlines()
        truncated_lines = len(lines) > safe_tail
        content = "\n".join(lines[-safe_tail:])
        if text.endswith("\n") and content:
            content += "\n"
        return {
            "job_id": job_id,
            "exists": True,
            "path": _repo_path(path),
            "content": content,
            "tail_lines": safe_tail,
            "truncated": truncated_bytes or truncated_lines,
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
            "archive_path": (job.get("archive") or {}).get("runner_log_archive_path")
            if isinstance(job.get("archive"), dict)
            else None,
        }

    def get_job_evidence(self, job_id: str) -> dict[str, Any] | None:
        job = self.get_job(job_id)
        if job is None:
            return None
        runner_evidence_path = self._evidence_path(job_id)
        if not runner_evidence_path.exists():
            archived_runner_evidence_path = self._archive_path_from_job(job, "runner_evidence_archive_path")
            if archived_runner_evidence_path and archived_runner_evidence_path.exists():
                runner_evidence_path = archived_runner_evidence_path
        runner_evidence = _read_json(runner_evidence_path)
        standard_evidence_path = self._archive_path_from_job(job, "evidence_manifest_path")
        standard_evidence = _read_json(standard_evidence_path) if standard_evidence_path else None
        return {
            "job_id": job_id,
            "job": job,
            "runner_evidence": runner_evidence,
            "standard_evidence": standard_evidence,
            "runner_evidence_path": _repo_path(runner_evidence_path),
            "standard_evidence_path": _repo_path(standard_evidence_path) if standard_evidence_path else None,
        }

    def start_job(
        self,
        *,
        plan_key: str,
        requested_by: str = "operator",
        backend_port: int | None = None,
        frontend_port: int | None = None,
        timeout_seconds: int | None = None,
        confirm_text: str | None = None,
        workspace_path: str | None = None,
        expected_branch: str | None = None,
        expected_commit: str | None = None,
    ) -> dict[str, Any]:
        plan = self._validate_plan(plan_key, confirm_text=confirm_text)
        resolved_cwd = self._validate_workspace_path(workspace_path, self.repo_root)
        if expected_branch:
            actual_branch = _git_branch(resolved_cwd)
            if actual_branch != expected_branch:
                raise ValidationRunnerError(
                    f"expected_branch {expected_branch!r} does not match actual branch {actual_branch!r}"
                )
        if expected_commit:
            actual_commit = _git_commit(resolved_cwd)
            if actual_commit != expected_commit:
                raise ValidationRunnerError(
                    f"expected_commit {expected_commit!r} does not match actual commit {actual_commit!r}"
                )
        resolved_backend_port = self._resolve_port(plan, backend_port, "backend")
        resolved_frontend_port = self._resolve_port(plan, frontend_port, "frontend")
        timeout = self._resolve_timeout(plan, timeout_seconds)
        job_id = f"valjob_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        command = [sys.executable, "-m", "nox", "-s", str(plan["nox_session"])]
        env = self._runner_env(plan, resolved_backend_port, resolved_frontend_port, resolved_cwd)
        workspace_branch = _git_branch(resolved_cwd)
        workspace_commit = _git_commit(resolved_cwd)
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
            "cwd": _repo_path(resolved_cwd),
            "workspace_path": str(resolved_cwd),
            "workspace_branch": workspace_branch,
            "workspace_commit": workspace_commit,
            "workspace_is_root": resolved_cwd == self.repo_root.resolve(),
            "expected_branch": expected_branch,
            "expected_commit": expected_commit,
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
            "archive": {"status": "pending" if self.archive_enabled else "disabled"},
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
    def _runner_env(plan: dict[str, Any], backend_port: int | None, frontend_port: int | None, workspace_path: Path | None = None) -> dict[str, str]:
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
        if workspace_path is not None:
            env["VALIDATION_WORKSPACE_PATH"] = str(workspace_path)
        return env

    def _run_job(self, job_id: str, command: list[str], env: dict[str, str], timeout: int) -> None:
        job = self.get_job(job_id)
        if job is None:
            return
        job["status"] = "running"
        job["started_at"] = _now_iso()
        self._write_job(job)
        executor_error: str | None = None
        try:
            resolved_cwd = Path(job.get("workspace_path", str(self.repo_root)))
            result = self.executor(command, env, resolved_cwd, timeout)
        except Exception as exc:
            executor_error = f"{type(exc).__name__}: {exc}"
            result = RunnerResult(
                return_code=None,
                output=f"validation runner executor error: {executor_error}\n",
            )
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
            job["error"] = (
                f"validation runner executor raised: {executor_error}"
                if executor_error
                else f"nox session exited with return_code={result.return_code}"
            )
        try:
            if self.archive_enabled:
                job = self._archive_job(job)
            else:
                self._write_job(job)
                self._write_evidence(job)
        except Exception as exc:  # pragma: no cover - defensive path; tests cover normal archive behavior.
            job["archive"] = {"status": "failed", "error": str(exc)}
            self._write_job(job)
            self._write_evidence(job)

    def _archive_job(self, job: dict[str, Any]) -> dict[str, Any]:
        archive_paths = self._archive_paths(job)
        history_root = archive_paths["history_root"]  # type: ignore[assignment]
        copied_artifacts = self._copy_discovered_artifacts(
            job,
            str(archive_paths["base_stem"]),
            history_root=history_root,  # type: ignore[arg-type]
        )
        coverage_copy = next((item for item in copied_artifacts if item["kind"] == "coverage_snapshot"), None)
        run_id = _history_id_for_path(archive_paths["markdown_path"], history_root)  # type: ignore[arg-type]
        archive = {
            "status": "archived",
            "run_id": run_id,
            "history_root": _repo_path(history_root),  # type: ignore[arg-type]
            "run_record_path": _repo_path(archive_paths["markdown_path"]),  # type: ignore[arg-type]
            "metadata_path": _repo_path(archive_paths["metadata_path"]),  # type: ignore[arg-type]
            "evidence_manifest_path": _repo_path(archive_paths["standard_evidence_path"]),  # type: ignore[arg-type]
            "runner_job_archive_path": _repo_path(archive_paths["runner_job_path"]),  # type: ignore[arg-type]
            "runner_log_archive_path": _repo_path(archive_paths["runner_log_path"]),  # type: ignore[arg-type]
            "runner_evidence_archive_path": _repo_path(archive_paths["runner_evidence_path"]),  # type: ignore[arg-type]
            "coverage_snapshot_path": coverage_copy.get("path") if coverage_copy else None,
            "artifact_paths": [item["path"] for item in copied_artifacts],
        }
        job["archive"] = archive
        self._write_job(job)
        self._write_evidence(job)
        _copy_if_exists(self._job_path(str(job["job_id"])), archive_paths["runner_job_path"])  # type: ignore[arg-type]
        _copy_if_exists(self._log_path(str(job["job_id"])), archive_paths["runner_log_path"])  # type: ignore[arg-type]
        _copy_if_exists(self._evidence_path(str(job["job_id"])), archive_paths["runner_evidence_path"])  # type: ignore[arg-type]
        _write_json(archive_paths["metadata_path"], self._run_metadata(job, archive, copied_artifacts))  # type: ignore[arg-type]
        archive_paths["markdown_path"].parent.mkdir(parents=True, exist_ok=True)  # type: ignore[union-attr]
        archive_paths["markdown_path"].write_text(  # type: ignore[union-attr]
            self._run_markdown(job, archive, copied_artifacts),
            encoding="utf-8",
        )
        evidence_manifest = self._standard_evidence_manifest(job, archive, copied_artifacts, archive_paths)
        _write_json(archive_paths["standard_evidence_path"], evidence_manifest)  # type: ignore[arg-type]
        return job

    def _history_root_for_job(self, job: dict[str, Any]) -> Path:
        if self._history_root_explicit:
            return self.history_root
        workspace = Path(job.get("workspace_path", str(self.repo_root))).resolve()
        if workspace != self.repo_root.resolve():
            return workspace / HISTORY_RELATIVE_ROOT
        # Root-targeted ad hoc runs keep evidence in ignored runner storage.
        return self.execution_root / "history"

    def _archive_paths(self, job: dict[str, Any]) -> dict[str, Path | str]:
        module_slug = _safe_slug(job.get("module"), default="validation")
        level_slug = _safe_slug(job.get("level"), default="lx")
        plan_slug = _safe_slug(job.get("plan_key"), default="runner")
        base_stem = f"{_filename_timestamp()}_{level_slug}_{plan_slug}_{str(job['job_id'])[-8:]}_runner"
        history_root = self._history_root_for_job(job)
        out_dir = history_root / module_slug
        return {
            "base_stem": base_stem,
            "history_root": history_root,
            "markdown_path": out_dir / f"{base_stem}-validation.md",
            "metadata_path": out_dir / f"{base_stem}-validation.json",
            "standard_evidence_path": out_dir / f"{base_stem}-evidence.json",
            "runner_job_path": out_dir / f"{base_stem}-runner-job.json",
            "runner_log_path": out_dir / f"{base_stem}-runner-log.txt",
            "runner_evidence_path": out_dir / f"{base_stem}-runner-evidence.json",
        }

    def _copy_discovered_artifacts(self, job: dict[str, Any], base_stem: str, *, history_root: Path) -> list[dict[str, Any]]:
        copied: list[dict[str, Any]] = []
        for kind, source in self._artifact_candidates(job):
            if not source.exists() or not source.is_file():
                continue
            if kind == "coverage_snapshot":
                payload = _read_json(source)
                if not payload or payload.get("schema_version") != COVERAGE_SCHEMA:
                    continue
            suffix = ARTIFACT_KIND_SUFFIXES.get(kind, source.suffix or ".artifact")
            target = history_root / _safe_slug(job.get("module"), default="validation") / f"{base_stem}-{_safe_slug(kind)}{suffix}"
            _copy_if_exists(source, target)
            copied.append({"kind": kind, "source_path": _repo_path(source), "path": _repo_path(target)})
        return copied

    def _artifact_candidates(self, job: dict[str, Any]) -> list[tuple[str, Path]]:
        names: list[str] = []
        for value in (job.get("nox_session"), job.get("plan_key")):
            text = str(value or "").strip()
            if text and text not in names:
                names.append(text)
        candidates: list[tuple[str, Path]] = []
        for name in names:
            candidates.extend(
                [
                    ("coverage_snapshot", self.repo_root / "tmp" / "validation" / "coverage" / f"{name}_snapshot.json"),
                    ("coverage_json", self.repo_root / "tmp" / "validation" / "coverage" / f"{name}.json"),
                    ("coverage_xml", self.repo_root / "tmp" / "validation" / "coverage" / f"{name}.xml"),
                ]
            )
        nox_session = str(job.get("nox_session") or "")
        if nox_session == "validation_center_live_readonly":
            candidates.extend(
                [
                    ("smoke_json", self.repo_root / "tmp" / "validation" / "validation_center" / "readonly_smoke.json"),
                    ("evidence_json", self.repo_root / "tmp" / "validation" / "validation_center" / "readonly_smoke_evidence.json"),
                ]
            )
        if nox_session == "guardrail_changed_files":
            candidates.extend(
                [
                    ("guardrail_json", self.repo_root / "tmp" / "validation" / "guardrails" / "changed_files.json"),
                    ("guardrail_md", self.repo_root / "tmp" / "validation" / "guardrails" / "changed_files.md"),
                ]
            )
        if nox_session == "l0":
            candidates.extend(
                [
                    ("guardrail_json", self.repo_root / "tmp" / "validation" / "guardrails" / "l0_paths.json"),
                    ("guardrail_md", self.repo_root / "tmp" / "validation" / "guardrails" / "l0_paths.md"),
                ]
            )
        seen: set[Path] = set()
        unique: list[tuple[str, Path]] = []
        for kind, path in candidates:
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            unique.append((kind, path))
        return unique

    def _run_metadata(self, job: dict[str, Any], archive: dict[str, Any], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
        resolved_cwd = Path(job.get("workspace_path", str(self.repo_root)))
        return {
            "schema_version": RUN_SCHEMA,
            "module": job.get("module") or "validation",
            "module_slug": _safe_slug(job.get("module"), default="validation"),
            "level": str(job.get("level") or "").upper() or None,
            "title": f"Runner {job.get('plan_key')} {job.get('status')}",
            "status": job.get("status"),
            "git_commit": _git_commit(resolved_cwd),
            "operator": job.get("requested_by"),
            "workspace": {
                "path": job.get("workspace_path"),
                "branch": job.get("workspace_branch"),
                "commit": job.get("workspace_commit"),
                "is_root": job.get("workspace_is_root"),
                "expected_branch": job.get("expected_branch"),
                "expected_commit": job.get("expected_commit"),
            },
            "started_at": job.get("started_at") or job.get("requested_at"),
            "finished_at": job.get("finished_at"),
            "runner_job_id": job.get("job_id"),
            "runner_plan_key": job.get("plan_key"),
            "runner_nox_session": job.get("nox_session"),
            "runner_archive": archive,
            "coverage": self._coverage_metadata(archive),
            "quality_gates": [
                {
                    "metric": "runner_return_code",
                    "status": "passed" if job.get("return_code") == 0 else "failed",
                    "actual": job.get("return_code"),
                    "threshold": 0,
                },
                {
                    "metric": "production_8001_touched",
                    "status": "passed" if job.get("production_8001_touched") is False else "failed",
                    "actual": job.get("production_8001_touched"),
                    "threshold": False,
                },
                {
                    "metric": "arbitrary_shell_allowed",
                    "status": "passed" if job.get("arbitrary_shell_allowed") is False else "failed",
                    "actual": job.get("arbitrary_shell_allowed"),
                    "threshold": False,
                },
                {
                    "metric": "runner_archive",
                    "status": "passed",
                    "actual": archive.get("status"),
                    "threshold": "archived",
                },
            ],
            "pass_scope": {
                "level": job.get("level"),
                "real_backend": bool(job.get("backend_port")),
                "real_database": False,
                "real_node_api": False,
                "real_frontend_click": False,
                "writes_business_state": bool(job.get("writes_business_state")),
                "positive_business_success": False,
                "negative_failfast_only": False,
                "mock_api_used": False,
                "controlled_runner_post_enabled": True,
                "arbitrary_shell_allowed": False,
                "production_8001_touched": False,
                "current_commit_evidence": True,
            },
            "business_assertion": {
                "can_user_complete_operation": job.get("status") == "passed",
                "operation_name": f"Run allowlisted validation plan {job.get('plan_key')}",
                "evidence": {
                    "runner_job": archive.get("runner_job_archive_path"),
                    "runner_log": archive.get("runner_log_archive_path"),
                    "evidence_manifest": archive.get("evidence_manifest_path"),
                    "coverage_snapshot": archive.get("coverage_snapshot_path"),
                },
                "unresolved_blockers": [
                    "This runner proof validates the test plan execution infrastructure, not a trading business success path."
                ],
            },
            "evidence": artifacts,
            "residual_risks": [],
        }

    @staticmethod
    def _coverage_metadata(archive: dict[str, Any]) -> dict[str, Any]:
        if archive.get("coverage_snapshot_path"):
            return {
                "schema_version": COVERAGE_SCHEMA,
                "status": "collected",
                "snapshot_path": archive.get("coverage_snapshot_path"),
            }
        return {
            "schema_version": COVERAGE_SCHEMA,
            "status": "not_collected",
            "line": None,
            "branch": None,
            "diff_line": None,
            "diff_branch": None,
            "snapshot_path": None,
            "quality_gates": [],
        }

    def _run_markdown(self, job: dict[str, Any], archive: dict[str, Any], artifacts: list[dict[str, Any]]) -> str:
        artifact_lines = "\n".join(f"- `{item['kind']}`: `{item['path']}`" for item in artifacts) or "- none"
        status = str(job.get("status") or "unknown").upper()
        return (
            f"# Validation Runner Job {job.get('plan_key')}\n\n"
            f"- Module: {job.get('module')}\n"
            f"- Level: {job.get('level')}\n"
            f"- Job ID: `{job.get('job_id')}`\n"
            f"- Nox session: `{job.get('nox_session')}`\n"
            f"- Status: {job.get('status')}\n"
            f"- Return code: {job.get('return_code')}\n"
            f"- Workspace: `{job.get('workspace_path', 'N/A')}`\n"
            f"- Branch: `{job.get('workspace_branch', 'N/A')}`\n"
            f"- Commit: `{job.get('workspace_commit', 'N/A')}`\n"
            f"- Is root: {job.get('workspace_is_root', True)}\n"
            f"- Started at: {job.get('started_at')}\n"
            f"- Finished at: {job.get('finished_at')}\n"
            f"- Production 8001 touched: {job.get('production_8001_touched')}\n"
            f"- Arbitrary shell allowed: {job.get('arbitrary_shell_allowed')}\n\n"
            "## Archive\n\n"
            f"- Run metadata: `{archive.get('metadata_path')}`\n"
            f"- Evidence manifest: `{archive.get('evidence_manifest_path')}`\n"
            f"- Runner log: `{archive.get('runner_log_archive_path')}`\n"
            f"- Coverage snapshot: `{archive.get('coverage_snapshot_path') or '-'}`\n\n"
            "## Copied Artifacts\n\n"
            f"{artifact_lines}\n\n"
            "## Result\n\n"
            f"- Final status: {status}\n"
        )

    def _standard_evidence_manifest(
        self,
        job: dict[str, Any],
        archive: dict[str, Any],
        artifacts: list[dict[str, Any]],
        archive_paths: dict[str, Path | str],
    ) -> dict[str, Any]:
        evidence_paths = [
            ("run_metadata", archive_paths["metadata_path"]),
            ("run_record", archive_paths["markdown_path"]),
            ("runner_job", archive_paths["runner_job_path"]),
            ("runner_log", archive_paths["runner_log_path"]),
            ("runner_evidence", archive_paths["runner_evidence_path"]),
        ]
        for item in artifacts:
            path = self._path_from_repo_path(str(item["path"]))
            if path:
                evidence_paths.append((str(item["kind"]), path))
        evidence = [self._evidence_entry(kind, Path(path)) for kind, path in evidence_paths]
        missing = [item for item in evidence if not item["exists"]]
        return {
            "schema_version": EVIDENCE_SCHEMA,
            "generated_at": _now_iso(),
            "module": job.get("module") or "validation",
            "level": str(job.get("level") or "").upper() or None,
            "title": f"Runner evidence for {job.get('plan_key')}",
            "run_id": archive.get("run_id"),
            "git_commit": _git_commit(Path(job.get("workspace_path", str(self.repo_root)))),
            "operator": job.get("requested_by"),
            "runner_job_id": job.get("job_id"),
            "evidence": evidence,
            "missing_count": len(missing),
            "missing": missing,
        }

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
            "archive": job.get("archive"),
            "production_8001_touched": False,
            "evidence": [
                self._evidence_entry("job_record", job_path),
                self._evidence_entry("execution_log", log_path),
            ],
        }
        _write_json(evidence_path, payload)

    @staticmethod
    def _evidence_entry(kind: str, path: Path) -> dict[str, Any]:
        return {
            "kind": kind,
            "path": _repo_path(path),
            "exists": path.exists(),
            "is_dir": path.is_dir() if path.exists() else False,
            "size_bytes": path.stat().st_size if path.exists() and path.is_file() else None,
            "child_count": None,
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

    def _path_from_repo_path(self, raw_path: str) -> Path | None:
        path = Path(raw_path)
        if not path.is_absolute():
            path = self.repo_root / path
        if _is_inside(path, self.repo_root) or _is_inside(path, self.execution_root) or _is_allowlisted_workspace_path(path):
            return path
        return None

    def _archive_path_from_job(self, job: dict[str, Any], key: str) -> Path | None:
        archive = job.get("archive")
        if not isinstance(archive, dict):
            return None
        raw = archive.get(key)
        if not raw:
            return None
        path = self._path_from_repo_path(str(raw))
        if path is None:
            return None
        history_root = self._history_root_for_job(job)
        if not _is_inside(path, history_root):
            return None
        return path
