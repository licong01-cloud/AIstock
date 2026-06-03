from __future__ import annotations

import hashlib
import json
import os
import platform as py_platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from backend.services.validation.execution_runner import ValidationExecutionRunner
from backend.services.validation.plan_catalog import (
    REPO_ROOT,
    ValidationCatalogError,
    ValidationPlanCatalog,
)


PLATFORM_HEALTH_SCHEMA = "aistock_validation_platform_health_v1"
REPO_CONTEXT_SCHEMA = "aistock_validation_repo_context_v1"
CATALOG_SCHEMA = "aistock_validation_catalog_integrity_v1"
NIGHTLY_SCHEMA = "aistock_validation_nightly_summary_v1"
RUNNER_SCHEMA = "aistock_validation_runner_health_v1"
GITHUB_SCHEMA = "aistock_validation_github_connectivity_v1"

CONFIG_FILES = {
    "test_plans.yaml": Path("tests/aistock_validation/catalog/test_plans.yaml"),
    "module_registry.yaml": Path("tests/aistock_validation/catalog/module_registry.yaml"),
    "ui_targets.yaml": Path("tests/aistock_validation/catalog/ui_targets.yaml"),
    "resource_policies.yaml": Path("tests/aistock_validation/catalog/resource_policies.yaml"),
}

CommandRunner = Callable[[list[str], Path, int], tuple[int, str, str]]
GITHUB_TOKEN_ENV_ORDER = ("AISTOCK_RUNNER_HEALTH_TOKEN", "GH_TOKEN", "GITHUB_TOKEN")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_display_path(path: Path) -> str:
    try:
        return str(path.resolve()).replace("\\", "/")
    except OSError:
        return str(path).replace("\\", "/")


def _repo_path(path: Path, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return _safe_display_path(path)


def _sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _cron_hint(cron: str | None) -> str | None:
    if not cron:
        return None
    parts = cron.split()
    if len(parts) != 5:
        return f"GitHub cron (UTC): {cron}"
    minute, hour = parts[0], parts[1]
    if minute.isdigit() and hour.isdigit():
        local_hour = (int(hour) + 8) % 24
        return f"{local_hour:02d}:{int(minute):02d} Asia/Shanghai"
    return f"GitHub cron (UTC): {cron}"


def _default_command_runner(
    args: list[str],
    cwd: Path,
    timeout: int,
    env_overrides: Mapping[str, str] | None = None,
) -> tuple[int, str, str]:
    try:
        run_env = None
        if env_overrides:
            run_env = os.environ.copy()
            run_env.update(env_overrides)
        completed = subprocess.run(
            args,
            cwd=str(cwd),
            env=run_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            timeout=timeout,
            check=False,
        )
        return completed.returncode, completed.stdout or "", completed.stderr or ""
    except FileNotFoundError as exc:
        return 127, "", str(exc)
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        return 124, output, str(exc)
    except OSError as exc:
        return 1, "", str(exc)


class ValidationPlatformHealthService:
    """Aggregate repo-root, catalog, runner, and nightly health without mutating state."""

    def __init__(
        self,
        *,
        repo_root: Path | None = None,
        env: Mapping[str, str] | None = None,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self.repo_root_hint = Path(repo_root or REPO_ROOT).expanduser()
        self.env = env if env is not None else os.environ
        self._uses_default_command_runner = command_runner is None
        self.command_runner = command_runner or _default_command_runner
        self._github_token_cache: dict[str, tuple[str | None, str | None, list[str], str | None]] = {}

    def summary(self) -> dict[str, Any]:
        repo_root, repo_context = self._collect_repo_context()
        catalog_integrity = self._collect_catalog_integrity(repo_root)
        github_connectivity = self._collect_github_connectivity(repo_root)
        runner_health = self._collect_runner_health(repo_root, github_connectivity)
        nightly_summary = self._collect_nightly_summary(repo_root, github_connectivity, runner_health)
        reason_codes = _dedupe(
            [
                *repo_context.get("reason_codes", []),
                *catalog_integrity.get("reason_codes", []),
                *github_connectivity.get("reason_codes", []),
                *runner_health.get("reason_codes", []),
                *nightly_summary.get("reason_codes", []),
            ]
        )
        critical_states = [
            str(repo_context.get("state") or "unknown"),
            str(catalog_integrity.get("state") or "unknown"),
        ]
        component_states = [
            *critical_states,
            str(github_connectivity.get("state") or "unknown"),
            str(runner_health.get("state") or "unknown"),
            str(nightly_summary.get("state") or "unknown"),
        ]
        state = self._aggregate_state(critical_states, component_states)
        data_state = "complete"
        if any(component.get("data_state") != "complete" for component in (repo_context, catalog_integrity, github_connectivity, runner_health, nightly_summary)):
            data_state = "partial"
        return {
            "schema_version": PLATFORM_HEALTH_SCHEMA,
            "generated_at": _now_iso(),
            "state": state,
            "data_state": data_state,
            "repo_context": repo_context,
            "catalog_integrity": catalog_integrity,
            "github_connectivity": github_connectivity,
            "runner_health": runner_health,
            "nightly_summary": nightly_summary,
            "merge_gate_inputs": {
                "repo_context_state": repo_context.get("state"),
                "catalog_integrity_state": catalog_integrity.get("state"),
                "ci_state": "unknown",
                "nightly_state": nightly_summary.get("state"),
                "open_p0_p1_count": catalog_integrity.get("open_p0_p1_count", 0),
                "required_plan_failures": catalog_integrity.get("reason_codes", []),
                "missing_evidence": nightly_summary.get("local_evidence", {}).get("reason_codes", []),
            },
            "reason_codes": reason_codes,
            "runtime": self._runtime_context(),
            "production_8001_touched": False,
        }

    def repo_context(self) -> dict[str, Any]:
        repo_root, repo_context = self._collect_repo_context()
        _ = repo_root
        return repo_context

    def catalog_integrity(self) -> dict[str, Any]:
        repo_root, _repo_context = self._collect_repo_context()
        _ = _repo_context
        return self._collect_catalog_integrity(repo_root)

    def github_connectivity(self) -> dict[str, Any]:
        repo_root, _repo_context = self._collect_repo_context()
        _ = _repo_context
        return self._collect_github_connectivity(repo_root)

    def runner_health(self) -> dict[str, Any]:
        repo_root, _repo_context = self._collect_repo_context()
        _ = _repo_context
        github_connectivity = self._collect_github_connectivity(repo_root)
        return self._collect_runner_health(repo_root, github_connectivity)

    def nightly_summary(self) -> dict[str, Any]:
        repo_root, _repo_context = self._collect_repo_context()
        _ = _repo_context
        github_connectivity = self._collect_github_connectivity(repo_root)
        runner_health = self._collect_runner_health(repo_root, github_connectivity)
        return self._collect_nightly_summary(repo_root, github_connectivity, runner_health)

    def _collect_repo_context(self) -> tuple[Path, dict[str, Any]]:
        repo_root, source = self._resolve_repo_root()
        exists = repo_root.exists()
        is_dir = repo_root.is_dir()
        strict = self._env_flag("AISTOCK_VALIDATION_CONFIG_STRICT", default=True)
        allow_dirty = self._env_flag("AISTOCK_VALIDATION_ALLOW_DIRTY", default=False)
        expected_branch = (self.env.get("AISTOCK_VALIDATION_EXPECT_BRANCH") or "").strip()
        baseline_ref = (self.env.get("AISTOCK_VALIDATION_BASELINE_REF") or "origin/main").strip()

        reason_codes: list[str] = []
        warnings: list[str] = []
        state = "healthy"
        git_state = self._git_state(repo_root)
        branch = git_state.get("branch")
        commit = git_state.get("commit")
        dirty = bool(git_state.get("dirty"))
        untracked_count = int(git_state.get("untracked_count") or 0)
        ahead = git_state.get("ahead")
        behind = git_state.get("behind")
        git_errors = list(git_state.get("errors") or [])

        if not exists:
            state = "blocked"
            reason_codes.append("repo_root_missing")
        elif not is_dir:
            state = "blocked"
            reason_codes.append("repo_root_not_directory")

        if exists and is_dir:
            if not git_state.get("is_git_repo"):
                state = "blocked"
                reason_codes.append("repo_root_not_git_repo")
            if git_state.get("git_status_error"):
                warnings.append("git_status_unavailable")
                reason_codes.append("git_status_unavailable")
            if git_state.get("git_branch_error"):
                warnings.append("git_branch_unavailable")
                reason_codes.append("git_branch_unavailable")
            if git_state.get("git_commit_error"):
                warnings.append("git_commit_unavailable")
                reason_codes.append("git_commit_unavailable")

            if dirty and not allow_dirty:
                state = "blocked"
                reason_codes.append("repo_dirty")
            elif dirty and allow_dirty:
                warnings.append("dirty_repo_allowed")
                reason_codes.append("dirty_repo_allowed")

            if expected_branch:
                if branch and branch != expected_branch:
                    warnings.append("unexpected_branch")
                    reason_codes.append("unexpected_branch")
            elif branch and branch != "main":
                warnings.append("non_main_branch")
                reason_codes.append("non_main_branch")

            if baseline_ref and git_state.get("baseline_error"):
                warnings.append("baseline_ref_unavailable")
                reason_codes.append("baseline_ref_unavailable")
            if isinstance(ahead, int) and ahead > 0:
                warnings.append("ahead_of_baseline")
                reason_codes.append("ahead_of_baseline")
            if isinstance(behind, int) and behind > 0:
                warnings.append("behind_baseline")
                reason_codes.append("behind_baseline")

        config_files = self._config_files(repo_root)
        missing_config_files = [name for name, item in config_files.items() if not item["exists"]]
        if missing_config_files:
            reason_codes.extend([f"config_missing_{name.replace('.yaml', '')}" for name in missing_config_files])
            if strict:
                state = "blocked" if state != "blocked" else state
            else:
                warnings.append("config_missing")

        if state != "blocked" and (warnings or git_errors):
            state = "degraded"

        if state == "healthy" and git_errors:
            state = "degraded"

        if not exists or not is_dir:
            data_state = "unavailable"
        else:
            data_state = "complete"

        return repo_root, {
            "schema_version": REPO_CONTEXT_SCHEMA,
            "generated_at": _now_iso(),
            "repo_root": _safe_display_path(repo_root),
            "repo_root_source": source,
            "repo_root_exists": exists,
            "repo_root_is_directory": is_dir,
            "is_git_repo": bool(git_state.get("is_git_repo")),
            "branch": branch,
            "commit": commit,
            "baseline_ref": baseline_ref,
            "ahead": ahead,
            "behind": behind,
            "dirty": dirty,
            "untracked_count": untracked_count,
            "allow_dirty": allow_dirty,
            "expected_branch": expected_branch or None,
            "config_strict": strict,
            "config_hashes": {name: item["sha256"] for name, item in config_files.items()},
            "config_files": config_files,
            "warnings": warnings,
            "reason_codes": _dedupe(reason_codes),
            "state": state,
            "data_state": data_state,
            "production_8001_touched": False,
        }

    def _collect_catalog_integrity(self, repo_root: Path) -> dict[str, Any]:
        strict = self._env_flag("AISTOCK_VALIDATION_CONFIG_STRICT", default=True)
        catalog_path = repo_root / CONFIG_FILES["test_plans.yaml"]
        reason_codes: list[str] = []
        warnings: list[str] = []
        findings: list[dict[str, Any]] = []
        plan_count = 0
        enabled_plan_count = 0
        runner_enabled_plan_count = 0
        open_p0_p1_count = 0
        state = "healthy"
        data_state = "complete"
        error: str | None = None
        missing = False
        try:
            payload = ValidationPlanCatalog(catalog_path).load()
        except ValidationCatalogError as exc:
            state = "blocked" if strict else "degraded"
            data_state = "unavailable"
            error = str(exc)
            reason_codes.append("catalog_validation_error")
        else:
            missing = bool(payload.get("missing"))
            plans = payload.get("plans") or []
            plan_count = len(plans)
            enabled_plan_count = sum(1 for plan in plans if plan.get("enabled"))
            runner_enabled_plan_count = sum(1 for plan in plans if plan.get("runner_enabled"))
            open_p0_p1_count = sum(1 for plan in plans if str(plan.get("level") or "").upper() in {"L0", "L1"} and plan.get("enabled"))
            if missing:
                state = "blocked" if strict else "degraded"
                data_state = "unavailable"
                reason_codes.append("catalog_missing")
                warnings.append("catalog_missing")
        try:
            from backend.services.validation.catalog_integrity import CatalogIntegrityChecker

            integrity = CatalogIntegrityChecker(repo_root=repo_root).run()
            findings = list(integrity.get("findings") or [])
            summary = integrity.get("summary") if isinstance(integrity.get("summary"), dict) else {}
            plan_count = int(summary.get("plans") or plan_count)
            runner_enabled_plan_count = int(summary.get("runner_enabled_plans") or runner_enabled_plan_count)
            error_count = int(summary.get("error_count") or 0)
            warning_count = int(summary.get("warning_count") or 0)
            if error_count:
                state = "blocked" if strict else "degraded"
                reason_codes.append("catalog_integrity_failed")
            elif warning_count and state == "healthy":
                state = "degraded"
                warnings.append("catalog_integrity_warnings")
                reason_codes.append("catalog_integrity_warnings")
        except Exception as exc:  # noqa: BLE001
            if state == "healthy":
                state = "degraded"
            warnings.append("catalog_integrity_unavailable")
            reason_codes.append("catalog_integrity_unavailable")
            error = error or str(exc)
        return {
            "schema_version": CATALOG_SCHEMA,
            "generated_at": _now_iso(),
            "catalog_path": _repo_path(catalog_path, repo_root),
            "missing": missing,
            "plan_count": plan_count,
            "enabled_plan_count": enabled_plan_count,
            "runner_enabled_plan_count": runner_enabled_plan_count,
            "open_p0_p1_count": open_p0_p1_count,
            "finding_count": len(findings),
            "findings": findings[:50],
            "state": state,
            "data_state": data_state,
            "warnings": warnings,
            "reason_codes": _dedupe(reason_codes),
            "error": error,
            "production_8001_touched": False,
        }

    def _collect_github_connectivity(self, repo_root: Path) -> dict[str, Any]:
        reason_codes: list[str] = []
        state = "healthy"
        data_state = "complete"
        token, token_source, token_reasons, token_message = self._resolve_github_token(repo_root)
        if token:
            auth_source = token_source
            message = f"github token source {token_source}; token value redacted"
        else:
            code, out, err = self._run(["gh", "auth", "status", "--hostname", "github.com"], cwd=repo_root, timeout=8)
            if code != 0:
                data_state = "unavailable"
                state = "unavailable"
                reason_codes.extend(token_reasons or ["gh_auth_unavailable"])
                message = (token_message or err or out or "gh auth status unavailable").strip()
                auth_source = None
            else:
                auth_source = "gh:auth-status"
                message = "gh auth status ok; token value redacted"
        repo = self._github_repository(repo_root)
        if repo.get("error"):
            reason_codes.append(repo["reason_code"])
            if state == "healthy":
                state = "unavailable"
                data_state = "unavailable"
        return {
            "schema_version": GITHUB_SCHEMA,
            "generated_at": _now_iso(),
            "state": state,
            "data_state": data_state,
            "repository": repo.get("repository"),
            "repository_source": repo.get("source"),
            "auth_source": auth_source,
            "message": message,
            "reason_codes": _dedupe(reason_codes),
            "production_8001_touched": False,
        }

    def _collect_runner_health(self, repo_root: Path, github_connectivity: dict[str, Any]) -> dict[str, Any]:
        controlled_runner = self._controlled_runner_health(repo_root)
        github_runner = self._github_runner_health(repo_root, github_connectivity)
        reason_codes = _dedupe([*github_runner.get("reason_codes", []), *controlled_runner.get("reason_codes", [])])
        state = github_runner.get("state") or "unknown"
        data_state = github_runner.get("data_state") or "unavailable"
        if github_connectivity.get("data_state") == "unavailable":
            state = "unavailable"
            data_state = "unavailable"
        return {
            "schema_version": RUNNER_SCHEMA,
            "generated_at": _now_iso(),
            "state": state,
            "data_state": data_state,
            "github_runner": github_runner,
            "controlled_runner": controlled_runner,
            "reason_codes": reason_codes,
            "production_8001_touched": False,
        }

    def _collect_nightly_summary(
        self,
        repo_root: Path,
        github_connectivity: dict[str, Any],
        runner_health: dict[str, Any],
    ) -> dict[str, Any]:
        workflow = self._nightly_workflow(repo_root)
        reason_codes = list(workflow.get("reason_codes", []))
        state = "unknown"
        data_state = "complete"
        latest_run: dict[str, Any] | None = None
        jobs: list[dict[str, Any]] = []
        issue_sync = {"failure_issue_created": False, "state": "not_applicable"}

        if workflow.get("data_state") != "complete":
            data_state = "unavailable"
            state = "blocked"
        elif github_connectivity.get("data_state") == "unavailable":
            data_state = "unavailable"
            state = "unknown"
            reason_codes.append("github_unavailable_for_nightly")
        else:
            runs = self._nightly_runs(repo_root, workflow, github_connectivity)
            reason_codes.extend(runs.get("reason_codes", []))
            latest_run = runs.get("latest_run")
            if latest_run:
                jobs = self._nightly_run_jobs(repo_root, workflow, github_connectivity, latest_run)
                reason_codes.extend(jobs.get("reason_codes", []))
                latest_run = jobs.get("latest_run") or latest_run
                jobs = jobs.get("jobs", [])
                state = self._nightly_state(latest_run, runner_health.get("github_runner", {}))
            else:
                state = "unknown"

        local_evidence = self._local_nightly_evidence(repo_root)
        reason_codes.extend(local_evidence.get("reason_codes", []))
        return {
            "schema_version": NIGHTLY_SCHEMA,
            "generated_at": _now_iso(),
            "state": state,
            "data_state": data_state,
            "workflow": workflow,
            "latest_run": latest_run,
            "jobs": jobs,
            "runner": runner_health.get("github_runner"),
            "issue_sync": issue_sync,
            "local_evidence": local_evidence,
            "reason_codes": _dedupe(reason_codes),
            "production_8001_touched": False,
        }

    def _nightly_workflow(self, repo_root: Path) -> dict[str, Any]:
        path = repo_root / ".github" / "workflows" / "nightly.yml"
        if not path.exists():
            return {
                "name": "AIstock Nightly L3 + DR",
                "file": ".github/workflows/nightly.yml",
                "cron": None,
                "next_run_hint": None,
                "required_labels": ["self-hosted", "windows"],
                "exists": False,
                "state": "blocked",
                "data_state": "unavailable",
                "reason_codes": ["nightly_workflow_missing"],
            }
        text = path.read_text(encoding="utf-8", errors="replace")
        name_match = re.search(r"(?m)^\s*name:\s*['\"]?(.+?)['\"]?\s*$", text)
        cron_match = re.search(r"(?m)^\s*-\s*cron:\s*['\"]?([^'\"]+)['\"]?\s*$", text)
        labels: list[str] = []
        if re.search(r"\bself-hosted\b", text, re.IGNORECASE):
            labels.append("self-hosted")
        if re.search(r"\bwindows\b", text, re.IGNORECASE):
            labels.append("windows")
        if re.search(r"\bubuntu\b", text, re.IGNORECASE):
            labels.append("ubuntu")
        return {
            "name": (name_match.group(1).strip() if name_match else "AIstock Nightly L3 + DR"),
            "file": ".github/workflows/nightly.yml",
            "cron": cron_match.group(1).strip() if cron_match else None,
            "next_run_hint": _cron_hint(cron_match.group(1).strip()) if cron_match else None,
            "required_labels": labels or ["self-hosted", "windows"],
            "exists": True,
            "state": "healthy",
            "data_state": "complete",
            "reason_codes": [],
        }

    def _nightly_runs(
        self,
        repo_root: Path,
        workflow: dict[str, Any],
        github_connectivity: dict[str, Any],
    ) -> dict[str, Any]:
        repo = github_connectivity.get("repository")
        if not repo:
            return {
                "latest_run": None,
                "reason_codes": ["github_repository_unavailable"],
            }
        code, out, err = self._run(
            [
                "gh",
                "run",
                "list",
                "--workflow",
                workflow["file"],
                "--limit",
                "10",
                "--json",
                "databaseId,status,conclusion,createdAt,updatedAt,url,workflowName,headBranch,headSha,displayTitle",
                "--repo",
                repo,
            ],
            cwd=repo_root,
            timeout=20,
            env_overrides=self._github_command_env(repo_root),
        )
        if code != 0:
            return {
                "latest_run": None,
                "reason_codes": ["nightly_runs_unavailable"],
            }
        try:
            payload = json.loads(out or "[]")
        except json.JSONDecodeError:
            return {
                "latest_run": None,
                "reason_codes": ["nightly_runs_invalid_json"],
            }
        runs = payload if isinstance(payload, list) else []
        latest_run = self._normalize_run(runs[0]) if runs else None
        return {"latest_run": latest_run, "reason_codes": []}

    def _nightly_run_jobs(
        self,
        repo_root: Path,
        workflow: dict[str, Any],
        github_connectivity: dict[str, Any],
        latest_run: dict[str, Any],
    ) -> dict[str, Any]:
        repo = github_connectivity.get("repository")
        if not repo or not latest_run.get("run_id"):
            return {
                "jobs": [],
                "latest_run": latest_run,
                "reason_codes": [],
            }
        code, out, err = self._run(
            [
                "gh",
                "run",
                "view",
                str(latest_run["run_id"]),
                "--json",
                "jobs,status,conclusion,createdAt,updatedAt,url,workflowName,displayTitle",
                "--repo",
                repo,
            ],
            cwd=repo_root,
            timeout=20,
            env_overrides=self._github_command_env(repo_root),
        )
        if code != 0:
            latest_run = dict(latest_run)
            latest_run["jobs_unavailable"] = True
            return {
                "jobs": [],
                "latest_run": latest_run,
                "reason_codes": ["nightly_jobs_unavailable"],
            }
        try:
            payload = json.loads(out or "{}")
        except json.JSONDecodeError:
            latest_run = dict(latest_run)
            latest_run["jobs_unavailable"] = True
            return {
                "jobs": [],
                "latest_run": latest_run,
                "reason_codes": ["nightly_jobs_invalid_json"],
            }
        jobs_raw = payload.get("jobs") if isinstance(payload, dict) else []
        jobs = [self._normalize_job(item) for item in jobs_raw or [] if isinstance(item, dict)]
        merged_run = {**latest_run, **payload} if isinstance(payload, dict) else latest_run
        latest_run = self._normalize_run(merged_run)
        return {"jobs": jobs, "latest_run": latest_run, "reason_codes": []}

    def _controlled_runner_health(self, repo_root: Path) -> dict[str, Any]:
        try:
            runner = ValidationExecutionRunner(
                repo_root=repo_root,
                execution_root=repo_root / "tmp" / "validation" / "runner" / "jobs",
                history_root=repo_root / "tests" / "aistock_validation" / "history",
            )
            payload = runner.health()
        except Exception as exc:  # noqa: BLE001
            return {
                "schema_version": RUNNER_SCHEMA,
                "generated_at": _now_iso(),
                "state": "unavailable",
                "data_state": "unavailable",
                "mode": "controlled_execution",
                "job_count": 0,
                "jobs_by_status": {},
                "reason_codes": ["controlled_runner_unavailable"],
                "error": str(exc),
                "production_8001_touched": False,
            }
        payload = dict(payload)
        payload["schema_version"] = RUNNER_SCHEMA
        payload["generated_at"] = _now_iso()
        payload["state"] = "healthy" if payload.get("exists") else "unavailable"
        payload["data_state"] = "complete" if payload.get("exists") else "unavailable"
        payload["reason_codes"] = [] if payload.get("exists") else ["controlled_runner_missing"]
        return payload

    def _github_runner_health(self, repo_root: Path, github_connectivity: dict[str, Any]) -> dict[str, Any]:
        repo = github_connectivity.get("repository")
        github_env = self._github_command_env(repo_root)
        _, auth_source, _, _ = self._resolve_github_token(repo_root)
        auth_source = auth_source or github_connectivity.get("auth_source")
        if github_connectivity.get("data_state") == "unavailable":
            return {
                "schema_version": RUNNER_SCHEMA,
                "generated_at": _now_iso(),
                "state": "unavailable",
                "data_state": "unavailable",
                "required_labels": ["self-hosted", "windows"],
                "matching_runner_count": 0,
                "online_count": 0,
                "busy_count": 0,
                "offline_count": 0,
                "runners": [],
                "auth_source": auth_source,
                "reason_codes": ["github_connectivity_unavailable"],
                "production_8001_touched": False,
            }
        if not repo:
            return {
                "schema_version": RUNNER_SCHEMA,
                "generated_at": _now_iso(),
                "state": "unavailable",
                "data_state": "unavailable",
                "required_labels": ["self-hosted", "windows"],
                "matching_runner_count": 0,
                "online_count": 0,
                "busy_count": 0,
                "offline_count": 0,
                "runners": [],
                "auth_source": auth_source,
                "reason_codes": ["github_repository_unavailable"],
                "production_8001_touched": False,
            }
        code, out, err = self._run(
            [
                "gh",
                "api",
                f"repos/{repo}/actions/runners",
            ],
            cwd=repo_root,
            timeout=20,
            env_overrides=github_env,
        )
        if code != 0:
            return {
                "schema_version": RUNNER_SCHEMA,
                "generated_at": _now_iso(),
                "state": "unavailable",
                "data_state": "unavailable",
                "required_labels": ["self-hosted", "windows"],
                "matching_runner_count": 0,
                "online_count": 0,
                "busy_count": 0,
                "offline_count": 0,
                "runners": [],
                "auth_source": auth_source,
                "reason_codes": ["runner_api_unavailable"],
                "message": (err or out or "gh api actions runners unavailable").strip(),
                "production_8001_touched": False,
            }
        try:
            payload = json.loads(out or "{}")
        except json.JSONDecodeError:
            return {
                "schema_version": RUNNER_SCHEMA,
                "generated_at": _now_iso(),
                "state": "unavailable",
                "data_state": "unavailable",
                "required_labels": ["self-hosted", "windows"],
                "matching_runner_count": 0,
                "online_count": 0,
                "busy_count": 0,
                "offline_count": 0,
                "runners": [],
                "auth_source": auth_source,
                "reason_codes": ["runner_api_invalid_json"],
                "production_8001_touched": False,
            }
        runners_raw = payload.get("runners") if isinstance(payload, dict) else []
        runners = [self._normalize_runner(item) for item in runners_raw or [] if isinstance(item, dict)]
        required_labels = ["self-hosted", "windows"]
        matching = [runner for runner in runners if self._runner_matches_labels(runner, required_labels)]
        online = [runner for runner in matching if runner.get("status") == "online"]
        busy = [runner for runner in online if runner.get("busy")]
        offline = [runner for runner in matching if runner.get("status") != "online"]
        if not matching:
            state = "blocked"
            reason_codes = ["self_hosted_runner_missing"]
        elif not online:
            state = "blocked"
            reason_codes = ["self_hosted_runner_offline"]
        elif len(busy) == len(online):
            state = "queued"
            reason_codes = ["self_hosted_runner_busy"]
        else:
            state = "healthy"
            reason_codes = []
        return {
            "schema_version": RUNNER_SCHEMA,
            "generated_at": _now_iso(),
            "state": state,
            "data_state": "complete",
            "required_labels": required_labels,
            "matching_runner_count": len(matching),
            "online_count": len(online),
            "busy_count": len(busy),
            "offline_count": len(offline),
            "runners": runners,
            "auth_source": auth_source,
            "reason_codes": reason_codes,
            "production_8001_touched": False,
        }

    def _local_nightly_evidence(self, repo_root: Path) -> dict[str, Any]:
        history_root = repo_root / "tests" / "aistock_validation" / "history"
        if not history_root.exists():
            return {
                "count": 0,
                "latest_path": None,
                "reason_codes": ["nightly_history_root_missing"],
            }
        items: list[tuple[float, Path]] = []
        try:
            for path in history_root.rglob("*.md"):
                if "nightly" not in path.name.lower() and "nightly" not in str(path).lower():
                    continue
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    continue
                items.append((mtime, path))
        except OSError:
            return {
                "count": 0,
                "latest_path": None,
                "reason_codes": ["nightly_history_scan_failed"],
            }
        items.sort(key=lambda item: item[0], reverse=True)
        latest_path = _repo_path(items[0][1], repo_root) if items else None
        return {
            "count": len(items),
            "latest_path": latest_path,
            "reason_codes": [],
        }

    def _runtime_context(self) -> dict[str, Any]:
        return {
            "python_executable": sys.executable,
            "python_version": py_platform.python_version(),
            "platform": py_platform.platform(),
            "cwd": _safe_display_path(Path.cwd()),
            "module_repo_root": _safe_display_path(REPO_ROOT),
            "repo_root_hint": _safe_display_path(self.repo_root_hint),
            "env": {
                "AISTOCK_VALIDATION_REPO_ROOT": bool(self.env.get("AISTOCK_VALIDATION_REPO_ROOT")),
                "AISTOCK_VALIDATION_BASELINE_REF": self.env.get("AISTOCK_VALIDATION_BASELINE_REF") or "origin/main",
                "AISTOCK_VALIDATION_ALLOW_DIRTY": self.env.get("AISTOCK_VALIDATION_ALLOW_DIRTY") or "0",
                "AISTOCK_VALIDATION_EXPECT_BRANCH": self.env.get("AISTOCK_VALIDATION_EXPECT_BRANCH") or "",
                "AISTOCK_VALIDATION_CONFIG_STRICT": self.env.get("AISTOCK_VALIDATION_CONFIG_STRICT") or "1",
                "GITHUB_REPOSITORY": self.env.get("GITHUB_REPOSITORY") or "",
                "AISTOCK_RUNNER_HEALTH_TOKEN_SET": bool(self.env.get("AISTOCK_RUNNER_HEALTH_TOKEN")),
                "GH_TOKEN_SET": bool(self.env.get("GH_TOKEN")),
                "GITHUB_TOKEN_SET": bool(self.env.get("GITHUB_TOKEN")),
            },
        }

    def _config_files(self, repo_root: Path) -> dict[str, dict[str, Any]]:
        payload: dict[str, dict[str, Any]] = {}
        for name, rel_path in CONFIG_FILES.items():
            path = repo_root / rel_path
            digest = _sha256(path)
            payload[name] = {
                "path": _repo_path(path, repo_root),
                "exists": path.exists(),
                "sha256": f"sha256:{digest}" if digest else None,
            }
        return payload

    def _resolve_repo_root(self) -> tuple[Path, str]:
        env_root = (self.env.get("AISTOCK_VALIDATION_REPO_ROOT") or "").strip()
        if env_root:
            return Path(env_root).expanduser(), "env:AISTOCK_VALIDATION_REPO_ROOT"
        code, out, _err = self._run(["git", "rev-parse", "--show-toplevel"], cwd=self.repo_root_hint, timeout=10)
        if code == 0 and out.strip():
            return Path(out.strip()).expanduser(), "auto:git_rev_parse_show_toplevel"
        return self.repo_root_hint, "auto:repo_root_hint"

    def _git_state(self, repo_root: Path) -> dict[str, Any]:
        is_git_repo = False
        branch: str | None = None
        commit: str | None = None
        ahead: int | None = None
        behind: int | None = None
        dirty = False
        untracked_count = 0
        baseline_error: str | None = None
        git_status_error: str | None = None
        git_branch_error: str | None = None
        git_commit_error: str | None = None
        errors: list[str] = []

        code, out, err = self._run(["git", "rev-parse", "--is-inside-work-tree"], cwd=repo_root, timeout=10)
        if code == 0:
            is_git_repo = out.strip().lower() == "true"
        else:
            errors.append((err or out or "rev-parse --is-inside-work-tree failed").strip())

        if is_git_repo:
            code, out, err = self._run(["git", "branch", "--show-current"], cwd=repo_root, timeout=10)
            if code == 0:
                branch = out.strip() or None
            else:
                git_branch_error = (err or out or "branch status unavailable").strip()
                errors.append(git_branch_error)

            code, out, err = self._run(["git", "rev-parse", "HEAD"], cwd=repo_root, timeout=10)
            if code == 0:
                commit = out.strip() or None
            else:
                git_commit_error = (err or out or "commit unavailable").strip()
                errors.append(git_commit_error)

            code, out, err = self._run(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=repo_root, timeout=10)
            if code == 0:
                lines = [line for line in out.splitlines() if line.strip()]
                dirty = bool(lines)
                untracked_count = sum(1 for line in lines if line.startswith("??"))
            else:
                git_status_error = (err or out or "status unavailable").strip()
                errors.append(git_status_error)

            baseline_ref = (self.env.get("AISTOCK_VALIDATION_BASELINE_REF") or "origin/main").strip()
            code, out, err = self._run(
                ["git", "rev-list", "--left-right", "--count", f"{baseline_ref}...HEAD"],
                cwd=repo_root,
                timeout=15,
            )
            if code == 0:
                parts = out.strip().split()
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    behind = int(parts[0])
                    ahead = int(parts[1])
            else:
                baseline_error = (err or out or "baseline diff unavailable").strip()
                errors.append(baseline_error)

        return {
            "is_git_repo": is_git_repo,
            "branch": branch,
            "commit": commit,
            "ahead": ahead,
            "behind": behind,
            "dirty": dirty,
            "untracked_count": untracked_count,
            "baseline_error": baseline_error,
            "git_status_error": git_status_error,
            "git_branch_error": git_branch_error,
            "git_commit_error": git_commit_error,
            "errors": errors,
        }

    def _github_repository(self, repo_root: Path) -> dict[str, Any]:
        repo = (self.env.get("GITHUB_REPOSITORY") or "").strip()
        if repo:
            return {"repository": repo, "source": "env:GITHUB_REPOSITORY"}
        code, out, err = self._run(["git", "config", "--get", "remote.origin.url"], cwd=repo_root, timeout=10)
        if code != 0:
            return {"repository": None, "source": "git_remote_origin_url", "error": (err or out or "remote origin unavailable").strip(), "reason_code": "github_repository_unavailable"}
        parsed = self._parse_github_repo_url(out.strip())
        if not parsed:
            return {
                "repository": None,
                "source": "git_remote_origin_url",
                "error": f"unparseable github remote: {out.strip()}",
                "reason_code": "github_repository_unavailable",
            }
        return {"repository": parsed, "source": "git_remote_origin_url"}

    @staticmethod
    def _parse_github_repo_url(raw: str) -> str | None:
        text = raw.strip()
        patterns = [
            re.compile(r"github\.com[:/](?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?$", re.IGNORECASE),
            re.compile(r"https?://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?$", re.IGNORECASE),
        ]
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                return f"{match.group('owner')}/{match.group('repo')}"
        return None

    def _normalize_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        created_at = payload.get("createdAt") or payload.get("created_at")
        updated_at = payload.get("updatedAt") or payload.get("updated_at")
        created_dt = _parse_iso(str(created_at) if created_at else None)
        queue_duration_seconds = None
        if created_dt:
            queue_duration_seconds = max(0, int((datetime.now(timezone.utc) - created_dt).total_seconds()))
        return {
            "run_id": payload.get("databaseId") or payload.get("run_id"),
            "status": payload.get("status"),
            "conclusion": payload.get("conclusion"),
            "created_at": str(created_at) if created_at else None,
            "updated_at": str(updated_at) if updated_at else None,
            "queue_duration_seconds": queue_duration_seconds,
            "url": payload.get("url"),
            "workflow_name": payload.get("workflowName") or payload.get("workflow_name"),
            "head_branch": payload.get("headBranch") or payload.get("head_branch"),
            "head_sha": payload.get("headSha") or payload.get("head_sha"),
            "display_title": payload.get("displayTitle") or payload.get("display_title"),
            "jobs_unavailable": bool(payload.get("jobs_unavailable")),
        }

    def _normalize_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        labels = payload.get("labels") or []
        normalized_labels = []
        for label in labels:
            if isinstance(label, dict):
                name = label.get("name")
                if name:
                    normalized_labels.append(str(name))
            elif label:
                normalized_labels.append(str(label))
        return {
            "name": payload.get("name"),
            "status": payload.get("status"),
            "conclusion": payload.get("conclusion"),
            "started_at": payload.get("startedAt") or payload.get("started_at"),
            "completed_at": payload.get("completedAt") or payload.get("completed_at"),
            "runner_name": payload.get("runnerName") or payload.get("runner_name"),
            "labels": normalized_labels,
            "url": payload.get("url"),
        }

    def _normalize_runner(self, payload: dict[str, Any]) -> dict[str, Any]:
        labels = payload.get("labels") or []
        normalized_labels = []
        for label in labels:
            if isinstance(label, dict):
                name = label.get("name")
                if name:
                    normalized_labels.append(str(name))
            elif label:
                normalized_labels.append(str(label))
        runner_os = payload.get("os")
        if runner_os:
            normalized_labels.append(str(runner_os).lower())
        return {
            "id": payload.get("id"),
            "name": payload.get("name"),
            "status": str(payload.get("status") or "unknown").lower(),
            "busy": bool(payload.get("busy")),
            "os": runner_os,
            "labels": sorted({label.lower() for label in normalized_labels}),
        }

    @staticmethod
    def _runner_matches_labels(runner: dict[str, Any], required_labels: list[str]) -> bool:
        labels = {str(label).lower() for label in runner.get("labels") or []}
        return all(label.lower() in labels for label in required_labels)

    def _nightly_state(self, latest_run: dict[str, Any] | None, github_runner: dict[str, Any]) -> str:
        if not latest_run:
            return "unknown"
        status = str(latest_run.get("status") or "").lower()
        conclusion = str(latest_run.get("conclusion") or "").lower()
        if status in {"queued", "in_progress", "requested", "waiting"}:
            if github_runner.get("state") == "blocked":
                return "blocked"
            return "queued"
        if status == "completed":
            if conclusion == "success":
                return "healthy"
            if conclusion in {"failure", "cancelled", "timed_out", "action_required"}:
                return "failed"
        return "unknown"

    def _aggregate_state(self, critical_states: list[str], component_states: list[str]) -> str:
        critical = {state.lower() for state in critical_states if state}
        if "blocked" in critical:
            return "blocked"
        states = {state.lower() for state in component_states if state}
        if states & {"degraded", "unknown", "failed", "queued", "unavailable"}:
            return "degraded"
        if "blocked" in states:
            return "degraded"
        return "healthy"

    def _env_flag(self, key: str, *, default: bool) -> bool:
        raw = self.env.get(key)
        if raw is None:
            return default
        return str(raw).strip().lower() not in {"0", "false", "no", "off", ""}

    def _resolve_github_token(self, repo_root: Path) -> tuple[str | None, str | None, list[str], str | None]:
        cache_key = _safe_display_path(repo_root)
        cached = self._github_token_cache.get(cache_key)
        if cached is not None:
            return cached

        for key in GITHUB_TOKEN_ENV_ORDER:
            token = str(self.env.get(key) or "").strip()
            if token:
                result = (token, f"env:{key}", [], None)
                self._github_token_cache[cache_key] = result
                return result

        code, out, err = self._run(["gh", "auth", "token"], cwd=repo_root, timeout=8)
        token = (out or "").strip()
        if code == 0 and token:
            result = (token, "gh:auth-token", [], None)
            self._github_token_cache[cache_key] = result
            return result

        message = (err or out or "github token unavailable").strip()
        result = (None, None, ["github_token_unavailable", "gh_auth_unavailable"], message)
        self._github_token_cache[cache_key] = result
        return result

    def _github_command_env(self, repo_root: Path) -> dict[str, str] | None:
        token, _, _, _ = self._resolve_github_token(repo_root)
        if not token:
            return None
        return {"GH_TOKEN": token}

    def _run(
        self,
        args: list[str],
        *,
        cwd: Path,
        timeout: int,
        env_overrides: Mapping[str, str] | None = None,
    ) -> tuple[int, str, str]:
        try:
            if env_overrides and self._uses_default_command_runner:
                return _default_command_runner(args, cwd, timeout, env_overrides=env_overrides)
            return self.command_runner(args, cwd, timeout)
        except Exception as exc:  # noqa: BLE001
            return 1, "", str(exc)
