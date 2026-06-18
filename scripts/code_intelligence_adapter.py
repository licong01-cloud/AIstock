from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import nightly_discovery_input_pack  # noqa: E402

WORKFLOW_ROOT = Path("tmp") / "issue_workflow"
DEFAULT_CODEGRAPH_VERSION = "0.9.4"
DEFAULT_UNDERSTAND_ANYTHING_VERSION = "v2.7.6"
UNDERSTAND_ANYTHING_REPO = "Lum1104/Understand-Anything"
CATALOG_PATH = REPO_ROOT / "tests" / "aistock_validation" / "catalog" / "code_intelligence.yaml"
DEFAULT_UA_MODULES = ["issue_workflow", "validation_center", "paper_v2", "research_assistant", "qe"]
DEFAULT_CODEGRAPH_CRITICAL_FILES = [
    "scripts/code_intelligence_adapter.py",
    "scripts/llm_provider_adapter.py",
    "scripts/nightly_adaptive_scheduler.py",
    "scripts/aistock_issue_workflow.py",
    ".github/workflows/nightly.yml",
]
GRAPH_SOURCE_ROOT_ENV = "AISTOCK_CODE_INTELLIGENCE_GRAPH_SOURCE_ROOT"
CODE_INTELLIGENCE_ARTIFACT_ROOTS = (
    Path("tmp") / "validation" / "code-intelligence",
    Path("tests") / "aistock_validation" / "history" / "code-intelligence",
)
DEFAULT_UNDERSTAND_IGNORE = [
    ".git/",
    ".codegraph/",
    ".understand-anything/intermediate/",
    ".understand-anything/tmp/",
    ".understand-anything/diff-overlay.json",
    "frontend/.next*/",
    "frontend/node_modules/",
    "node_modules/",
    ".venv/",
    "venv/",
    "__pycache__/",
    ".pytest_cache/",
    ".ruff_cache/",
    "tmp/",
    "tests/aistock_validation/history/",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.gif",
    "*.mp4",
    "*.zip",
]
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _load_catalog(root: Path | None = None) -> dict[str, Any]:
    path = (root or REPO_ROOT) / "tests" / "aistock_validation" / "catalog" / "code_intelligence.yaml"
    if not path.exists():
        return {}
    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"_load_error": str(exc)}



class CodeIntelligenceError(ValueError):
    """Raised when code intelligence artifacts cannot be produced safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _emit(payload: dict[str, Any], output: str | None = None) -> None:
    if output and output != "-":
        _write_json(Path(output), payload)
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _compact_token(value: Any, *, limit: int = 180) -> str:
    text = str(value if value is not None else "unknown").strip()
    text = re.sub(r"\s+", "_", text)
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text or "unknown"


def _emit_compact_line(
    prefix: str,
    fields: dict[str, Any],
    *,
    payload: dict[str, Any],
    output: str | None = None,
    output_format: str = "compact",
) -> None:
    if output and output != "-":
        _write_json(Path(output), payload)
    if output == "-" or output_format == "full-json":
        sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")
        return
    gate = str(fields.get("workflow_gate") or fields.get("status") or "unknown").lower()
    marker = "PASS" if gate in {"ready", "ok", "configured"} else "WARN"
    body = " ".join(f"{key}={_compact_token(value)}" for key, value in fields.items() if value is not None)
    sys.stdout.write(f"{marker} {prefix} {body}\n")


def _repo_rel(path: Path, root: Path | None = None) -> str:
    root = root or REPO_ROOT
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return str(path).replace("\\", "/")


def _run_command(args: list[str], cwd: Path | None = None, timeout: int = 30) -> dict[str, Any]:
    cwd = cwd or REPO_ROOT
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}


def _strip_ansi(text: str | None) -> str:
    return ANSI_ESCAPE_RE.sub("", str(text or ""))


def _compact_text(text: str | None, *, max_chars: int = 2000) -> str:
    clean = _strip_ansi(text).strip()
    if len(clean) <= max_chars:
        return clean
    omitted = len(clean) - max_chars
    return f"{clean[:max_chars]}... <truncated {omitted} chars>"


def _compact_command_result(
    result: dict[str, Any],
    *,
    success_summary: str | None = None,
    include_output: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": bool(result.get("ok")),
        "returncode": result.get("returncode"),
    }
    if "skipped" in result:
        payload["skipped"] = result.get("skipped")
    if success_summary:
        payload["stdout_summary"] = success_summary
    if include_output or not result.get("ok"):
        stdout = _compact_text(str(result.get("stdout") or ""))
        stderr = _compact_text(str(result.get("stderr") or ""))
        if stdout:
            payload["stdout"] = stdout
        if stderr:
            payload["stderr"] = stderr
    return payload


def _git(args: list[str], cwd: Path | None = None, check: bool = False) -> dict[str, Any]:
    return _run_command(["git", *args], cwd=cwd, timeout=30)


def _git_text(args: list[str], cwd: Path | None = None) -> str:
    result = _git(args, cwd=cwd)
    if not result.get("ok"):
        raise CodeIntelligenceError(result.get("stderr") or result.get("stdout") or f"git {' '.join(args)} failed")
    return str(result.get("stdout") or "").strip()


def _canonical_repo_root(root: Path) -> Path:
    """Return the main checkout that owns this git worktree when it can be inferred."""
    result = _git(["rev-parse", "--path-format=absolute", "--git-common-dir"], cwd=root)
    if not result.get("ok"):
        return root
    common_dir = Path(str(result.get("stdout") or "").strip())
    if not common_dir.is_absolute():
        common_dir = root / common_dir
    return common_dir.parent if common_dir.name == ".git" else root


def _same_repo_remote(left: Path, right: Path) -> bool:
    left_remote = str(_git(["config", "--get", "remote.origin.url"], cwd=left).get("stdout") or "").strip()
    right_remote = str(_git(["config", "--get", "remote.origin.url"], cwd=right).get("stdout") or "").strip()
    if not left_remote or not right_remote:
        return True

    def normalize(value: str) -> str:
        normalized = value.strip().lower().replace("\\", "/").removesuffix(".git")
        normalized = normalized.removeprefix("https://").removeprefix("http://")
        normalized = normalized.removeprefix("ssh://")
        if normalized.startswith("git@github.com:"):
            normalized = "github.com/" + normalized.removeprefix("git@github.com:")
        return normalized

    def remote_path(value: str) -> Path | None:
        raw = value.strip().strip('"').strip("'")
        if raw.startswith("file:"):
            parsed = urlparse(raw)
            raw = unquote(parsed.path)
        path = Path(raw).expanduser()
        try:
            resolved = path.resolve()
        except OSError:
            return None
        return resolved if resolved.exists() else None

    left_path = remote_path(left_remote)
    right_path = remote_path(right_remote)
    try:
        if left_path and left_path == right.resolve():
            return True
        if right_path and right_path == left.resolve():
            return True
        if left_path and right_path and left_path == right_path:
            return True
    except OSError:
        pass

    return normalize(left_remote) == normalize(right_remote)


def _is_git_checkout(path: Path) -> bool:
    if (path / ".git").exists():
        return True
    result = _git(["rev-parse", "--is-inside-work-tree"], cwd=path)
    return bool(result.get("ok") and str(result.get("stdout") or "").strip().lower() == "true")


def _configured_graph_source_root(requested_root: Path) -> tuple[Path | None, str | None, list[str]]:
    configured = str(os.environ.get(GRAPH_SOURCE_ROOT_ENV) or "").strip()
    if not configured:
        return None, None, []
    warnings: list[str] = []
    try:
        source = Path(configured).expanduser().resolve()
    except OSError as exc:
        return None, None, [f"{GRAPH_SOURCE_ROOT_ENV} could not be resolved: {exc}"]
    if not source.exists():
        return None, None, [f"{GRAPH_SOURCE_ROOT_ENV} does not exist: {source}"]
    if not _is_git_checkout(source):
        return None, None, [f"{GRAPH_SOURCE_ROOT_ENV} is not a git checkout: {source}"]
    if source == requested_root:
        return source, "configured_env", warnings
    if not _same_repo_remote(requested_root, source):
        return None, None, [f"{GRAPH_SOURCE_ROOT_ENV} remote does not match requested checkout: {source}"]
    return source, "configured_env", warnings


def _graph_source_root(root: Path) -> tuple[Path, str, list[str]]:
    configured, source, warnings = _configured_graph_source_root(root)
    if configured is not None:
        return configured, source or "configured_env", warnings
    if _codegraph_index_path(root).exists():
        return root, "current_worktree", warnings
    canonical = _canonical_repo_root(root)
    if canonical != root and _codegraph_index_path(canonical).exists():
        return canonical, "canonical_worktree_root", warnings
    return root, "current_worktree", warnings


def _graph_commit_relation(root: Path, graph_root: Path, root_commit: Any, graph_commit: Any) -> str:
    if not root_commit or not graph_commit:
        return "unknown"
    if str(root_commit) == str(graph_commit):
        return "same"
    if _git_commit_is_ancestor(root, graph_commit, root_commit):
        return "ancestor"
    return "different"


def _codegraph_index_path(root: Path) -> Path:
    return root / ".codegraph" / "codegraph.db"


def _codegraph_graph_root(root: Path) -> Path:
    return _graph_source_root(root)[0]


def _understand_project_root(root: Path) -> Path:
    configured, _, _ = _configured_graph_source_root(root)
    if configured is not None:
        return configured
    canonical = _canonical_repo_root(root)
    return canonical if canonical != root else root


def _git_snapshot(root: Path) -> dict[str, Any]:
    branch = _git(["branch", "--show-current"], cwd=root)
    head = _git(["rev-parse", "HEAD"], cwd=root)
    status = _git(["status", "--short"], cwd=root)
    return {
        "ok": bool(branch.get("ok") and head.get("ok") and status.get("ok")),
        "branch": branch.get("stdout") or None,
        "head": head.get("stdout") or None,
        "dirty": bool((status.get("stdout") or "").strip()),
        "dirty_count": len([line for line in str(status.get("stdout") or "").splitlines() if line.strip()]),
        "status": status.get("stdout") or "",
    }


def _parse_codegraph_version(stdout: str) -> str | None:
    match = re.search(r"(\d+\.\d+\.\d+(?:[-+][\w.]+)?)", stdout or "")
    return match.group(1) if match else None


def _parse_codegraph_status(stdout: str) -> dict[str, Any]:
    clean = _strip_ansi(stdout)
    metrics: dict[str, Any] = {}
    for label, key in (
        ("Files", "files"),
        ("Nodes", "nodes"),
        ("Edges", "edges"),
    ):
        match = re.search(rf"^\s*{label}:\s*([\d,]+)\s*$", clean, re.MULTILINE)
        if match:
            metrics[key] = int(match.group(1).replace(",", ""))
    db_match = re.search(r"^\s*DB Size:\s*([0-9.]+\s+\w+)\s*$", clean, re.MULTILINE)
    if db_match:
        metrics["db_size"] = db_match.group(1)
    metrics["up_to_date"] = "Index is up to date" in clean
    return metrics


def _codegraph_command() -> str | None:
    override = os.environ.get("AISTOCK_CODEGRAPH_BIN")
    if override:
        return override
    return shutil.which("codegraph")


def _runner_context() -> str:
    if str(os.environ.get("GITHUB_ACTIONS") or "").lower() == "true":
        return "github_actions"
    if os.environ.get("CI"):
        return "ci"
    return "local"


def _codegraph_fallback(*, status: dict[str, Any], skip_external: bool) -> dict[str, Any]:
    context = _runner_context()
    if context in {"github_actions", "ci"}:
        if skip_external:
            detail = "CodeGraph external calls are disabled in this PR Quality runner; use artifact refs or local latest-freshness."
        elif not status.get("available"):
            detail = "CodeGraph CLI is not installed in this runner; use artifact refs or local latest-freshness."
        elif not status.get("index_exists"):
            detail = "CodeGraph index artifact is not present in this runner checkout; use artifact refs or local latest-freshness."
        else:
            detail = "CodeGraph index is not executable in this runner; use artifact refs or local latest-freshness."
        return {
            "used": True,
            "reason": "runner_artifact_unavailable",
            "detail": detail,
            "runner_context": context,
        }
    return {
        "used": True,
        "reason": "codegraph_unavailable_or_missing_index",
        "detail": None,
        "runner_context": context,
    }


def _codegraph_bootstrap_command() -> str:
    command = _codegraph_command() or "codegraph"
    return f"{command} init -i"


def _codegraph_reindex_command(graph_root: Path | str | None = None) -> str:
    command = _codegraph_command() or "codegraph"
    target = str(graph_root or REPO_ROOT)
    return f"{command} index {target}"


def _catalog_codegraph_critical_files(root: Path) -> list[str]:
    catalog = _load_catalog(root)
    config = catalog.get("codegraph") if isinstance(catalog.get("codegraph"), dict) else {}
    raw = config.get("critical_files") or config.get("index_must_include") or []
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if str(item).strip()]


def _codegraph_critical_files(root: Path) -> list[str]:
    candidates = _catalog_codegraph_critical_files(root) or DEFAULT_CODEGRAPH_CRITICAL_FILES
    selected: list[str] = []
    for item in candidates:
        normalized = _norm_repo_path(item)
        if not normalized or any(char in normalized for char in "*?["):
            continue
        if (root / normalized).is_file():
            selected.append(normalized)
    return sorted(set(selected))


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _latest_codegraph_freshness_artifact(root: Path) -> dict[str, Any] | None:
    candidates: list[tuple[str, float, Path, dict[str, Any]]] = []
    for relative_root in CODE_INTELLIGENCE_ARTIFACT_ROOTS:
        artifact_root = root / relative_root
        if not artifact_root.exists():
            continue
        for path in artifact_root.rglob("*.json"):
            payload = _read_json_if_exists(path)
            if (payload or {}).get("schema_version") != "aistock_codegraph_freshness_v1":
                continue
            generated_at = str(payload.get("generated_at") or "")
            try:
                modified_at = path.stat().st_mtime
            except OSError:
                modified_at = 0.0
            candidates.append((generated_at, modified_at, path, payload or {}))
    if not candidates:
        return None
    _, _, path, payload = sorted(candidates, key=lambda item: (item[0], item[1], str(item[2])), reverse=True)[0]
    return {
        "schema_version": payload.get("schema_version"),
        "artifact_type": "codegraph_freshness",
        "provider": payload.get("provider") or "codegraph",
        "workflow_gate": payload.get("workflow_gate"),
        "freshness": payload.get("freshness"),
        "freshness_basis": payload.get("freshness_basis"),
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "git_commit": payload.get("git_commit"),
        "graph_root": payload.get("graph_root"),
        "graph_root_source": payload.get("graph_root_source"),
        "artifact_path": _repo_rel(path, root),
        "summary_ref": payload.get("summary_ref"),
        "index_summary": payload.get("index_summary") or {},
        "warnings": payload.get("warnings") or [],
        "notes": payload.get("notes") or [],
        "blocking_for_issue_workflow": bool(payload.get("blocking_for_issue_workflow")),
    }


def _latest_understand_anything_summary_manifest(root: Path) -> dict[str, Any] | None:
    candidates: list[tuple[str, float, Path, dict[str, Any]]] = []
    for relative_root in CODE_INTELLIGENCE_ARTIFACT_ROOTS:
        artifact_root = root / relative_root
        if not artifact_root.exists():
            continue
        for path in artifact_root.rglob("ua-summary-manifest.json"):
            payload = _read_json_if_exists(path)
            if (payload or {}).get("schema_version") != "aistock_understand_anything_summary_manifest_v1":
                continue
            generated_at = str(payload.get("generated_at") or "")
            try:
                modified_at = path.stat().st_mtime
            except OSError:
                modified_at = 0.0
            candidates.append((generated_at, modified_at, path, payload or {}))
    if not candidates:
        return None
    _, _, path, payload = sorted(candidates, key=lambda item: (item[0], item[1], str(item[2])), reverse=True)[0]
    return {
        "schema_version": payload.get("schema_version"),
        "artifact_type": "understand_anything_summary_manifest",
        "workflow_gate": payload.get("workflow_gate"),
        "generated_at": payload.get("generated_at"),
        "artifact_path": _repo_rel(path, root),
        "summary_refs": [
            {
                "module": item.get("module"),
                "summary_ref": item.get("summary_ref"),
                "freshness": item.get("freshness"),
            }
            for item in payload.get("summary_refs") or []
            if isinstance(item, dict)
        ],
        "blocking_for_issue_workflow": bool(payload.get("blocking_for_issue_workflow")),
    }


def _codegraph_live_status_is_fresh(status: dict[str, Any] | None) -> bool:
    status = status or {}
    return bool(
        status.get("available")
        and status.get("index_exists")
        and (status.get("status_check") or {}).get("ok")
        and (status.get("index_summary") or {}).get("up_to_date")
    )


def _live_codegraph_freshness_payload(status: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "aistock_codegraph_freshness_v1",
        "artifact_type": "codegraph_live_status",
        "provider": "codegraph",
        "workflow_gate": "ready",
        "freshness": "fresh",
        "freshness_basis": "live_codegraph_status",
        "status": status.get("status"),
        "generated_at": _utc_now(),
        "git_commit": status.get("git_commit"),
        "graph_root": status.get("graph_root"),
        "graph_root_source": status.get("graph_root_source"),
        "artifact_path": None,
        "summary_ref": None,
        "index_summary": status.get("index_summary") or {},
        "warnings": [],
        "notes": [
            "Latest persisted freshness artifact was missing or stale, but live CodeGraph status reports the index is up to date."
        ],
        "blocking_for_issue_workflow": False,
    }


def _persist_effective_codegraph_freshness(
    root: Path,
    effective: dict[str, Any],
    *,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Persist compact effective freshness so clients do not inherit stale metadata."""
    target_dir = output_dir or root / "tmp" / "validation" / "code-intelligence" / "latest"
    json_path = target_dir / "codegraph-freshness.json"
    md_path = target_dir / "codegraph-freshness.md"
    payload = dict(effective)
    payload.update(
        {
            "schema_version": "aistock_codegraph_freshness_v1",
            "artifact_type": "codegraph_freshness",
            "provider": payload.get("provider") or "codegraph",
            "artifact_path": _repo_rel(json_path, root),
            "summary_ref": _repo_rel(md_path, root),
            "persisted_from": payload.get("artifact_type") or "effective_freshness",
            "persisted_at": _utc_now(),
        }
    )
    payload.setdefault("generated_at", _utc_now())
    payload.setdefault("workflow_gate", "ready" if payload.get("freshness") == "fresh" else "warning")
    payload.setdefault("blocking_for_issue_workflow", False)
    _write_json(json_path, payload)
    _write_text(md_path, render_codegraph_freshness_markdown(payload))
    return {
        "schema_version": payload.get("schema_version"),
        "artifact_type": "codegraph_freshness",
        "provider": payload.get("provider") or "codegraph",
        "workflow_gate": payload.get("workflow_gate"),
        "freshness": payload.get("freshness"),
        "freshness_basis": payload.get("freshness_basis"),
        "status": payload.get("status"),
        "generated_at": payload.get("generated_at"),
        "git_commit": payload.get("git_commit"),
        "graph_root": payload.get("graph_root"),
        "graph_root_source": payload.get("graph_root_source"),
        "artifact_path": _repo_rel(json_path, root),
        "summary_ref": _repo_rel(md_path, root),
        "index_summary": payload.get("index_summary") or {},
        "warnings": payload.get("warnings") or [],
        "notes": payload.get("notes") or [],
        "blocking_for_issue_workflow": bool(payload.get("blocking_for_issue_workflow")),
    }


def latest_codegraph_freshness(
    root: Path | None = None,
    *,
    live_status: dict[str, Any] | None = None,
    refresh_if_stale: bool = False,
    persist_effective: bool = False,
    output_dir: Path | None = None,
    max_age_hours: float = 36.0,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    latest = _latest_codegraph_freshness_artifact(root)
    warnings: list[str] = []
    notes: list[str] = []
    refreshed = False
    effective = latest
    effective_source = "artifact" if latest else "none"
    if refresh_if_stale and (latest is None or latest.get("freshness") != "fresh"):
        refreshed_payload = build_codegraph_freshness_artifact(
            root=root,
            output_dir=output_dir or root / "tmp" / "validation" / "code-intelligence" / "latest",
            max_age_hours=max_age_hours,
            skip_external=skip_external,
        )
        latest = _latest_codegraph_freshness_artifact(root) or refreshed_payload
        effective = latest
        effective_source = "refreshed_artifact"
        refreshed = True
    elif (latest is None or latest.get("freshness") != "fresh") and _codegraph_live_status_is_fresh(live_status):
        effective = _live_codegraph_freshness_payload(live_status or {})
        effective_source = "live_status"
        notes.extend(effective.get("notes") or [])
    if latest is None:
        if effective_source not in {"live_status", "live_status_critical_file_coverage"}:
            warnings.append("No CodeGraph freshness artifact found; use latest-freshness --refresh-if-stale or live doctor/status fallback.")
    elif latest.get("freshness") != "fresh":
        if effective_source not in {"live_status", "live_status_critical_file_coverage"}:
            warnings.append(f"Latest CodeGraph freshness is {latest.get('freshness') or 'unknown'}.")
    current_git_commit = _git_snapshot(root).get("head")
    if (
        latest
        and latest.get("git_commit")
        and current_git_commit
        and str(latest.get("git_commit")) != str(current_git_commit)
        and latest.get("freshness") == "fresh"
        and _codegraph_live_status_is_fresh(live_status)
        and str((live_status or {}).get("git_commit") or "") == str(current_git_commit)
    ):
        effective = _live_codegraph_freshness_payload(live_status or {})
        effective_source = "live_status_current_head"
        notes.extend(effective.get("notes") or [])
    stale_metadata_warning = bool(
        latest
        and latest.get("git_commit")
        and current_git_commit
        and str(latest.get("git_commit")) != str(current_git_commit)
        and (effective or {}).get("freshness") == "fresh"
        and str((effective or {}).get("git_commit") or latest.get("git_commit")) != str(current_git_commit)
    )
    needs_effective_persist = bool(
        persist_effective
        and latest
        and latest.get("git_commit")
        and current_git_commit
        and str(latest.get("git_commit")) != str(current_git_commit)
        and effective
        and (effective or {}).get("freshness") == "fresh"
        and str((effective or {}).get("git_commit") or "") == str(current_git_commit)
    )
    if needs_effective_persist:
        latest = _persist_effective_codegraph_freshness(root, effective, output_dir=output_dir)
        effective = latest
        effective_source = "persisted_effective_artifact"
        refreshed = True
        stale_metadata_warning = False
    if stale_metadata_warning:
        warnings.append(
            "Latest CodeGraph freshness artifact commit differs from current HEAD, but effective freshness is fresh; use as warning-only usable graph metadata."
        )
    file_coverage: dict[str, Any] | None = None
    if live_status and (effective or {}).get("freshness") == "fresh":
        file_coverage = _codegraph_index_file_coverage(root=root, status=live_status, skip_external=skip_external)
        if file_coverage.get("workflow_gate") == "warning":
            effective = {
                **(effective or {}),
                "freshness": "incomplete_index",
                "freshness_basis": "critical_file_coverage",
                "workflow_gate": "warning",
                "index_file_coverage": file_coverage,
            }
            effective_source = f"{effective_source}_critical_file_coverage"
            missing = ", ".join((file_coverage.get("missing_files") or [])[:5])
            suffix = f": {missing}" if missing else ""
            warnings.append(f"Effective CodeGraph index is missing critical workflow files{suffix}.")
    return {
        "schema_version": "aistock_codegraph_latest_freshness_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "warning" if warnings else "ready",
        "blocking_for_issue_workflow": False,
        "artifact_roots": [_repo_rel(root / item, root) for item in CODE_INTELLIGENCE_ARTIFACT_ROOTS],
        "latest": latest,
        "effective": effective,
        "effective_source": effective_source,
        "refreshed": refreshed,
        "current_git_commit": current_git_commit,
        "stale_metadata_warning": stale_metadata_warning,
        "index_file_coverage": file_coverage,
        "warnings": warnings,
        "notes": notes,
    }


def _understand_graph_path(root: Path) -> Path:
    return root / ".understand-anything" / "knowledge-graph.json"


def _understand_config_path(root: Path) -> Path:
    return root / ".understand-anything" / "config.json"


def _understand_ignore_path(root: Path) -> Path:
    return root / ".understand-anything" / ".understandignore"


def _git_commit_is_ancestor(root: Path, ancestor: Any, descendant: Any) -> bool:
    if not ancestor or not descendant:
        return False
    result = _git(["merge-base", "--is-ancestor", str(ancestor), str(descendant)], cwd=root)
    return bool(result.get("ok"))


def _user_home() -> Path:
    return Path(os.environ.get("USERPROFILE") or os.environ.get("HOME") or Path.home())


def _codex_understand_skill_path(home: Path | None = None) -> Path:
    return (home or _user_home()) / ".agents" / "skills" / "understand"


def _ua_plugin_root_candidates(home: Path | None = None) -> list[Path]:
    home = home or _user_home()
    return [
        home / ".understand-anything-plugin",
        home / ".understand-anything" / "repo" / "understand-anything-plugin",
        home / ".codex" / "understand-anything" / "understand-anything-plugin",
    ]


def _first_existing_dir(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists() and path.is_dir():
            return path
    return None


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"read_error": str(exc)}


def _claude_understand_plugin_status(*, skip_external: bool = False) -> dict[str, Any]:
    command = shutil.which("claude")
    if not command:
        return {"available": False, "enabled": False, "skipped": skip_external, "command": None}
    if skip_external:
        return {"available": True, "enabled": None, "skipped": True, "command": command}
    result = _run_command([command, "plugins", "list"], cwd=REPO_ROOT, timeout=30)
    stdout = str(result.get("stdout") or "")
    enabled = "understand-anything@understand-anything" in stdout and (
        "Status: √ enabled" in stdout or "Status: ✓ enabled" in stdout or "Status: enabled" in stdout
    )
    return {
        "available": bool(result.get("ok")),
        "enabled": enabled,
        "skipped": False,
        "command": command,
        "result": _compact_command_result(result, success_summary="understand-anything plugin list checked"),
    }


def _understand_generate_command(root: Path) -> str:
    return f"/understand {root} --language zh --no-auto-update"


def _understand_install_commands(home: Path | None = None) -> dict[str, str]:
    home = home or _user_home()
    install_ps1 = home / ".understand-anything" / "repo" / "install.ps1"
    if install_ps1.exists():
        codex_command = f"powershell -NoProfile -ExecutionPolicy Bypass -File {install_ps1} codex"
    else:
        codex_command = (
            "iwr -useb "
            f"https://raw.githubusercontent.com/{UNDERSTAND_ANYTHING_REPO}/main/install.ps1 | iex"
        )
    return {
        "codex": codex_command,
        "claude_code": (
            f"claude plugins marketplace add {UNDERSTAND_ANYTHING_REPO} --scope user; "
            "claude plugins install understand-anything --scope user"
        ),
    }


def configure_understand_anything(
    *,
    root: Path | None = None,
    language: str = "zh",
    auto_update: bool = False,
) -> dict[str, Any]:
    root = _understand_project_root(root or REPO_ROOT)
    ua_dir = root / ".understand-anything"
    ua_dir.mkdir(parents=True, exist_ok=True)
    config_path = _understand_config_path(root)
    ignore_path = _understand_ignore_path(root)
    config = _read_json_object(config_path)
    if config.get("read_error"):
        config = {}
    config.update(
        {
            "outputLanguage": language,
            "autoUpdate": bool(auto_update),
            "configuredBy": "aistock_code_intelligence_adapter",
            "officialToolRepo": UNDERSTAND_ANYTHING_REPO,
        }
    )
    _write_json(config_path, config)
    created_ignore = False
    if not ignore_path.exists():
        _write_text(
            ignore_path,
            "# AIstock Understand Anything ignore file\n"
            "# Keep generated caches and validation history out of the graph input.\n\n"
            + "\n".join(DEFAULT_UNDERSTAND_IGNORE)
            + "\n",
        )
        created_ignore = True
    status = understand_anything_status(root, skip_external=True)
    return {
        "schema_version": "aistock_understand_anything_configure_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "configured",
        "blocking_for_issue_workflow": False,
        "root": str(root),
        "config_path": _repo_rel(config_path, root),
        "understandignore_path": _repo_rel(ignore_path, root),
        "understandignore_created": created_ignore,
        "graph_path": _repo_rel(_understand_graph_path(root), root),
        "graph_exists": _understand_graph_path(root).exists(),
        "generate_graph_command": _understand_generate_command(root),
        "status": status,
    }


def codegraph_status(root: Path | None = None, *, skip_external: bool = False) -> dict[str, Any]:
    root = root or REPO_ROOT
    graph_root, graph_root_source, source_warnings = _graph_source_root(root)
    command = _codegraph_command()
    available = bool(command)
    version_result: dict[str, Any] = {"ok": False, "skipped": skip_external or not available}
    status_result: dict[str, Any] = {"ok": False, "skipped": skip_external or not available}
    version = None
    if available and not skip_external:
        version_result = _run_command([command, "--version"], cwd=root, timeout=20)
        version = _parse_codegraph_version(str(version_result.get("stdout") or version_result.get("stderr") or ""))
        status_result = _run_command([command, "status", str(graph_root)], cwd=root, timeout=30)
    status_summary = _parse_codegraph_status(str(status_result.get("stdout") or ""))
    index_path = _codegraph_index_path(graph_root)
    git = _git_snapshot(root)
    graph_git = _git_snapshot(graph_root) if graph_root != root else git
    graph_commit_relation = _graph_commit_relation(root, graph_root, git.get("head"), graph_git.get("head"))
    status = "ok" if available and index_path.exists() else ("missing_index" if available else "unavailable")
    if skip_external and available:
        status = "available_unchecked" if index_path.exists() else "missing_index"
    return {
        "provider": "codegraph",
        "enabled": True,
        "status": status,
        "available": available,
        "command": command,
        "expected_version": DEFAULT_CODEGRAPH_VERSION,
        "version": version,
        "version_check": _compact_command_result(
            version_result,
            success_summary=version or _compact_text(str(version_result.get("stdout") or ""), max_chars=120),
        ),
        "index_path": _repo_rel(index_path, root),
        "graph_root": str(graph_root),
        "graph_root_source": graph_root_source,
        "index_exists": index_path.exists(),
        "status_check": _compact_command_result(
            status_result,
            success_summary="Index is up to date" if status_summary.get("up_to_date") else None,
        ),
        "index_summary": status_summary,
        "git_commit": git.get("head"),
        "graph_root_git_commit": graph_git.get("head"),
        "graph_commit_relation": graph_commit_relation,
        "graph_source_warnings": source_warnings,
        "working_tree_dirty": git.get("dirty"),
        "channel": "mcp_or_cli",
        "bootstrap_command": _codegraph_bootstrap_command(),
    }


def _index_age(index_path: Path) -> dict[str, Any]:
    if not index_path.exists():
        return {"exists": False, "modified_at": None, "age_seconds": None}
    modified = datetime.fromtimestamp(index_path.stat().st_mtime, timezone.utc)
    age = max(0.0, (datetime.now(timezone.utc) - modified).total_seconds())
    return {
        "exists": True,
        "modified_at": modified.isoformat().replace("+00:00", "Z"),
        "age_seconds": round(age, 3),
    }


def _codegraph_index_file_coverage(
    *,
    root: Path,
    status: dict[str, Any],
    skip_external: bool = False,
) -> dict[str, Any]:
    critical_files = _codegraph_critical_files(root)
    graph_root = Path(str(status.get("graph_root") or root))
    command = str(status.get("command") or _codegraph_command() or "")
    base = {
        "strategy": "codegraph_files_exact_path_probe",
        "checked_files": critical_files,
        "present_files": [],
        "missing_files": [],
        "errors": [],
        "workflow_gate": "ready",
        "reindex_command": _codegraph_reindex_command(graph_root),
    }
    if not critical_files:
        return {**base, "skipped": True, "reason": "no_existing_critical_files"}
    if skip_external:
        return {**base, "skipped": True, "reason": "external_check_skipped", "workflow_gate": "warning"}
    if not (status.get("available") and status.get("index_exists") and command):
        return {**base, "skipped": True, "reason": "codegraph_unavailable_or_missing_index", "workflow_gate": "warning"}

    present: list[str] = []
    missing: list[str] = []
    errors: list[dict[str, str]] = []
    for repo_path in critical_files:
        result = _run_command(
            [
                command,
                "files",
                "--path",
                str(graph_root),
                "--pattern",
                repo_path,
                "--format",
                "flat",
                "--json",
            ],
            cwd=root,
            timeout=30,
        )
        if not result.get("ok"):
            errors.append({"file": repo_path, "error": _compact_text(str(result.get("stderr") or result.get("stdout") or ""), max_chars=180)})
            continue
        try:
            files_payload = json.loads(str(result.get("stdout") or "[]"))
        except json.JSONDecodeError:
            errors.append({"file": repo_path, "error": "codegraph files returned non-json output"})
            continue
        indexed_paths = {
            _norm_repo_path(item.get("path") or item.get("file") or "")
            for item in files_payload
            if isinstance(item, dict)
        }
        if repo_path in indexed_paths:
            present.append(repo_path)
        else:
            missing.append(repo_path)

    return {
        **base,
        "skipped": False,
        "present_files": sorted(present),
        "missing_files": sorted(missing),
        "errors": errors,
        "workflow_gate": "warning" if missing or errors else "ready",
    }


def build_codegraph_freshness_artifact(
    *,
    root: Path | None = None,
    output_dir: Path | None = None,
    max_age_hours: float = 36.0,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    output_dir = output_dir or root / "tmp" / "validation" / "code-intelligence"
    json_path = output_dir / "codegraph-freshness.json"
    md_path = output_dir / "codegraph-freshness.md"
    status = codegraph_status(root, skip_external=skip_external)
    index_path = Path(str(status.get("graph_root") or root)) / ".codegraph" / "codegraph.db"
    age = _index_age(index_path)
    warnings: list[str] = []
    notes: list[str] = []
    freshness = "fresh"
    freshness_basis = "codegraph_status"
    if not status.get("available"):
        freshness = "unavailable"
        freshness_basis = "availability"
        warnings.append("CodeGraph CLI is unavailable; Nightly should keep this as warning-only.")
    elif not status.get("index_exists"):
        freshness = "missing_index"
        freshness_basis = "index_presence"
        warnings.append(f"CodeGraph index is missing; bootstrap command: {status.get('bootstrap_command')}.")
    elif skip_external:
        freshness = "unverified"
        freshness_basis = "mtime_only"
        warnings.append("CodeGraph external status check was skipped; freshness is based on index mtime only.")
    elif not (status.get("status_check") or {}).get("ok"):
        freshness = "status_check_failed"
        freshness_basis = "status_check"
        warnings.append("CodeGraph status command failed; inspect compact status_check output in the JSON artifact.")
    elif not (status.get("index_summary") or {}).get("up_to_date"):
        freshness = "stale"
        freshness_basis = "codegraph_status"
        warnings.append("CodeGraph status did not report the index as up to date.")
    graph_commit_relation = status.get("graph_commit_relation")
    if status.get("graph_root_source") == "configured_env" and graph_commit_relation not in {"same", "ancestor"}:
        if freshness == "fresh":
            freshness = "stale"
            freshness_basis = "configured_graph_root_commit"
        warnings.append(
            f"Configured CodeGraph root commit is {graph_commit_relation or 'unknown'} relative to the requested checkout."
        )
    warnings.extend(str(item) for item in status.get("graph_source_warnings") or [])
    age_seconds = age.get("age_seconds")
    if isinstance(age_seconds, (int, float)) and age_seconds > max_age_hours * 3600:
        status_reports_fresh = (
            status.get("available")
            and status.get("index_exists")
            and not skip_external
            and (status.get("status_check") or {}).get("ok")
            and (status.get("index_summary") or {}).get("up_to_date")
        )
        if status_reports_fresh:
            notes.append(
                f"CodeGraph index mtime exceeds {max_age_hours:g} hours, "
                "but codegraph status reports the index is up to date."
            )
        else:
            freshness = "stale"
            freshness_basis = "mtime"
            warnings.append(f"CodeGraph index age exceeds {max_age_hours:g} hours.")
    file_coverage = _codegraph_index_file_coverage(root=root, status=status, skip_external=skip_external)
    if file_coverage.get("missing_files") or file_coverage.get("errors"):
        if freshness == "fresh":
            freshness = "incomplete_index"
            freshness_basis = "critical_file_coverage"
        missing = ", ".join((file_coverage.get("missing_files") or [])[:5])
        suffix = f": {missing}" if missing else ""
        warnings.append(f"CodeGraph index is missing critical workflow files{suffix}; run {file_coverage.get('reindex_command')}.")
    payload = {
        "schema_version": "aistock_codegraph_freshness_v1",
        "generated_at": _utc_now(),
        "provider": "codegraph",
        "workflow_gate": "warning" if warnings else "ready",
        "freshness": freshness,
        "freshness_basis": freshness_basis,
        "blocking_for_issue_workflow": False,
        "root": str(root),
        "graph_root": status.get("graph_root"),
        "graph_root_source": status.get("graph_root_source"),
        "git_commit": status.get("git_commit"),
        "graph_root_git_commit": status.get("graph_root_git_commit"),
        "graph_commit_relation": status.get("graph_commit_relation"),
        "working_tree_dirty": status.get("working_tree_dirty"),
        "max_age_hours": max_age_hours,
        "index_age": age,
        "index_file_coverage": file_coverage,
        "index_summary": status.get("index_summary"),
        "version": status.get("version") or status.get("expected_version"),
        "status": status.get("status"),
        "status_check": status.get("status_check"),
        "warnings": warnings,
        "notes": notes,
        "artifact_path": _repo_rel(json_path, root),
        "summary_ref": _repo_rel(md_path, root),
    }
    _write_json(json_path, payload)
    _write_text(md_path, render_codegraph_freshness_markdown(payload))
    return payload


def render_codegraph_freshness_markdown(payload: dict[str, Any]) -> str:
    age = payload.get("index_age") or {}
    summary = payload.get("index_summary") or {}
    coverage = payload.get("index_file_coverage") or {}
    lines = [
        "## CodeGraph Freshness",
        "",
        f"- workflow_gate: `{payload.get('workflow_gate') or 'unknown'}`",
        f"- freshness: `{payload.get('freshness') or 'unknown'}`",
        f"- status: `{payload.get('status') or 'unknown'}`",
        f"- version: `{payload.get('version') or 'unknown'}`",
        f"- graph_root_source: `{payload.get('graph_root_source') or 'unknown'}`",
        f"- git_commit: `{payload.get('git_commit') or 'unknown'}`",
        f"- graph_root_git_commit: `{payload.get('graph_root_git_commit') or 'unknown'}`",
        f"- graph_commit_relation: `{payload.get('graph_commit_relation') or 'unknown'}`",
        f"- index_modified_at: `{age.get('modified_at') or 'missing'}`",
        f"- index_age_seconds: `{age.get('age_seconds') if age.get('age_seconds') is not None else 'unknown'}`",
        f"- files/nodes/edges: `{summary.get('files', 'unknown')}` / `{summary.get('nodes', 'unknown')}` / `{summary.get('edges', 'unknown')}`",
        f"- critical_file_coverage: `{len(coverage.get('present_files') or [])}` / `{len(coverage.get('checked_files') or [])}`",
        "",
        "This artifact is warning-only and does not replace nox, pytest, Validation Center, or production gates.",
    ]
    warnings = payload.get("warnings") or []
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings]])
    notes = payload.get("notes") or []
    if notes:
        lines.extend(["", "### Notes", *[f"- {item}" for item in notes]])
    return "\n".join(lines)


def build_code_intelligence_run_manifest(
    *,
    root: Path | None = None,
    output_dir: Path | None = None,
    artifact_name: str | None = None,
    run_id: str | None = None,
    run_url: str | None = None,
    branch: str | None = None,
    commit: str | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    output_dir = output_dir or root / "tmp" / "validation" / "code-intelligence"
    git = _git_snapshot(root)
    artifact_name = artifact_name or (f"code-intelligence-{run_id}" if run_id else "code-intelligence-local")
    branch = branch or str(git.get("branch") or "")
    commit = commit or str(git.get("head") or "")
    json_path = output_dir / "code-intelligence-run-manifest.json"
    md_path = output_dir / "code-intelligence-run-manifest.md"
    freshness_json = output_dir / "codegraph-freshness.json"
    freshness_md = output_dir / "codegraph-freshness.md"
    ua_manifest = output_dir / "ua-summary-manifest.json"
    warnings: list[str] = []
    if not freshness_json.exists():
        warnings.append("CodeGraph freshness JSON is missing; this artifact can still be uploaded but latest-freshness will warn.")
    download_command = None
    if run_id:
        download_command = (
            f"gh run download {run_id} --repo licong01-cloud/AIstock "
            f"-n {artifact_name} -D tmp/validation/code-intelligence/downloaded/{run_id}"
        )
    payload = {
        "schema_version": "aistock_code_intelligence_run_manifest_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "warning" if warnings else "ready",
        "blocking_for_issue_workflow": False,
        "artifact_name": artifact_name,
        "artifact_type": "github_actions_artifact",
        "run_id": run_id,
        "run_url": run_url,
        "branch": branch,
        "commit": commit,
        "root": str(root),
        "output_dir": _repo_rel(output_dir, root),
        "download": {
            "gh_command": download_command,
            "local_latest_freshness_command": "python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale",
        },
        "consumable_refs": {
            "codegraph_freshness_json": _repo_rel(freshness_json, root) if freshness_json.exists() else None,
            "codegraph_freshness_md": _repo_rel(freshness_md, root) if freshness_md.exists() else None,
            "understand_anything_manifest_json": _repo_rel(ua_manifest, root) if ua_manifest.exists() else None,
        },
        "warnings": warnings,
        "artifact_path": _repo_rel(json_path, root),
        "summary_ref": _repo_rel(md_path, root),
    }
    _write_json(json_path, payload)
    _write_text(md_path, render_code_intelligence_run_manifest_markdown(payload))
    return payload


def render_code_intelligence_run_manifest_markdown(payload: dict[str, Any]) -> str:
    download = payload.get("download") if isinstance(payload.get("download"), dict) else {}
    refs = payload.get("consumable_refs") if isinstance(payload.get("consumable_refs"), dict) else {}
    lines = [
        "## Code Intelligence Run Manifest",
        "",
        f"- workflow_gate: `{payload.get('workflow_gate') or 'unknown'}`",
        f"- artifact_name: `{payload.get('artifact_name') or 'unknown'}`",
        f"- run_id: `{payload.get('run_id') or 'local'}`",
        f"- branch: `{payload.get('branch') or 'unknown'}`",
        f"- commit: `{payload.get('commit') or 'unknown'}`",
        f"- codegraph_freshness_json: `{refs.get('codegraph_freshness_json') or 'missing'}`",
        f"- codegraph_freshness_md: `{refs.get('codegraph_freshness_md') or 'missing'}`",
        f"- understand_anything_manifest_json: `{refs.get('understand_anything_manifest_json') or 'missing'}`",
        "",
        "### Agent Consumption",
        "",
        f"- Download artifact: `{download.get('gh_command') or 'not_applicable'}`",
        f"- Read latest freshness: `{download.get('local_latest_freshness_command') or 'python scripts/code_intelligence_adapter.py latest-freshness'}`",
        "",
        "This artifact is warning-only. It helps Codex and Claude Code avoid full repository scans, but it does not replace nox, pytest, Validation Center, or production gates.",
    ]
    warnings = payload.get("warnings") or []
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings]])
    text = ""
    for line in lines:
        text += line + "\n"
    return text.rstrip("\n")


def _artifact_ref(path: Path, root: Path) -> str | None:
    return _repo_rel(path, root) if path.exists() else None


def _artifact_summary(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    return {
        "schema_version": payload.get("schema_version"),
        "workflow_gate": payload.get("workflow_gate") or payload.get("gate"),
        "artifact": payload.get("artifact") or payload.get("artifact_path"),
    }


def build_llm_value_summary(
    *,
    root: Path | None = None,
    artifact_dir: Path | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    artifact_dir = artifact_dir or root / "tmp" / "validation" / "code-intelligence" / "latest"
    codegraph = _read_json_object(artifact_dir / "codegraph-freshness.json")
    code_summary = _read_json_object(artifact_dir / "code-intelligence-summary.json")
    adaptive = _read_json_object(artifact_dir / "llm-nightly-adaptive-scheduler.json")
    scheduler = _read_json_object(artifact_dir / "llm-nightly-scheduler-advice.json")
    hypotheses = _read_json_object(artifact_dir / "llm-hypotheses.json")
    prompt_eval = _read_json_object(artifact_dir / "llm-prompt-evaluation.json")
    rollout = _read_json_object(artifact_dir / "llm-guarded-rollout-gate.json")
    ua_manifest = _read_json_object(artifact_dir / "ua-summary-manifest.json")
    discovery_manifest = _read_json_object(artifact_dir / "discovery-plans" / "manifest.json")
    bug_candidate_manifest = _read_json_object(artifact_dir / "bug-candidates" / "manifest.json")
    adaptive_evidence = (
        adaptive.get("llm_invocation_evidence") if isinstance(adaptive.get("llm_invocation_evidence"), dict) else {}
    )
    scheduler_evidence = (
        scheduler.get("llm_invocation_evidence") if isinstance(scheduler.get("llm_invocation_evidence"), dict) else {}
    )
    hypothesis_evidence = (
        hypotheses.get("llm_invocation_evidence")
        if isinstance(hypotheses.get("llm_invocation_evidence"), dict)
        else {}
    )
    queue = adaptive.get("queue_summary") if isinstance(adaptive.get("queue_summary"), dict) else {}
    consumption = adaptive.get("advice_consumption") if isinstance(adaptive.get("advice_consumption"), dict) else {}
    ua_summary = (
        code_summary.get("understand_anything_summary")
        if isinstance(code_summary.get("understand_anything_summary"), dict)
        else {}
    )
    prompt_metrics = prompt_eval.get("metrics") if isinstance(prompt_eval.get("metrics"), dict) else {}
    provider = (
        adaptive.get("effective_provider")
        or adaptive.get("provider")
        or scheduler.get("effective_provider")
        or scheduler.get("provider")
        or hypotheses.get("effective_provider")
        or hypotheses.get("provider")
        or adaptive_evidence.get("provider")
        or scheduler_evidence.get("provider")
        or hypothesis_evidence.get("provider")
    )
    model = (
        adaptive.get("effective_model")
        or adaptive.get("model")
        or scheduler.get("effective_model")
        or scheduler.get("model")
        or hypotheses.get("effective_model")
        or hypotheses.get("model")
        or adaptive_evidence.get("model")
        or scheduler_evidence.get("model")
        or hypothesis_evidence.get("model")
    )
    warnings: list[str] = []
    if not codegraph:
        warnings.append("CodeGraph freshness artifact is missing.")
    if not adaptive and not scheduler:
        warnings.append("LLM scheduler advice artifact is missing.")
    if not hypotheses:
        warnings.append("LLM discovery hypothesis artifact is missing.")
    if not prompt_eval:
        warnings.append("LLM prompt evaluation artifact is missing.")
    if not rollout:
        warnings.append("LLM guarded rollout artifact is missing.")
    artifact_refs = {
        "codegraph_freshness_json": _artifact_ref(artifact_dir / "codegraph-freshness.json", root),
        "codegraph_freshness_md": _artifact_ref(artifact_dir / "codegraph-freshness.md", root),
        "code_intelligence_summary_json": _artifact_ref(artifact_dir / "code-intelligence-summary.json", root),
        "code_intelligence_summary_md": _artifact_ref(artifact_dir / "code-intelligence-summary.md", root),
        "understand_anything_manifest_json": _artifact_ref(artifact_dir / "ua-summary-manifest.json", root),
        "adaptive_scheduler_json": _artifact_ref(artifact_dir / "llm-nightly-adaptive-scheduler.json", root),
        "adaptive_scheduler_md": _artifact_ref(artifact_dir / "llm-nightly-adaptive-scheduler.md", root),
        "scheduler_advice_json": _artifact_ref(artifact_dir / "llm-nightly-scheduler-advice.json", root),
        "discovery_hypotheses_json": _artifact_ref(artifact_dir / "llm-hypotheses.json", root),
        "selected_plans_json": _artifact_ref(artifact_dir / "selected-plans.json", root),
        "discovery_plans_manifest_json": _artifact_ref(artifact_dir / "discovery-plans" / "manifest.json", root),
        "bug_candidate_queue_manifest_json": _artifact_ref(artifact_dir / "bug-candidates" / "manifest.json", root),
        "bug_candidate_queue_markdown": _artifact_ref(artifact_dir / "bug-candidates" / "candidate-summary.md", root),
        "test_plan_advice_json": _artifact_ref(artifact_dir / "llm-test-plan-advice.json", root),
        "prompt_evaluation_json": _artifact_ref(artifact_dir / "llm-prompt-evaluation.json", root),
        "guarded_rollout_json": _artifact_ref(artifact_dir / "llm-guarded-rollout-gate.json", root),
    }
    return {
        "schema_version": "aistock_llm_code_intelligence_value_summary_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "warning" if warnings else "ready",
        "blocking_for_issue_workflow": False,
        "artifact_dir": _repo_rel(artifact_dir, root),
        "codegraph": {
            "workflow_gate": codegraph.get("workflow_gate") or codegraph.get("gate"),
            "freshness": codegraph.get("freshness"),
            "status": codegraph.get("status"),
            "git_commit": codegraph.get("git_commit"),
            "graph_root_git_commit": codegraph.get("graph_root_git_commit"),
            "files": (codegraph.get("index_summary") or {}).get("files") or codegraph.get("files"),
            "nodes": (codegraph.get("index_summary") or {}).get("nodes") or codegraph.get("nodes"),
            "edges": (codegraph.get("index_summary") or {}).get("edges") or codegraph.get("edges"),
        },
        "understand_anything": {
            "status": code_summary.get("understand_anything_status")
            or ua_summary.get("status")
            or ("available" if ua_manifest else "unknown"),
            "summary_ref": code_summary.get("understand_anything_summary_ref") or ua_summary.get("summary_ref"),
            "manifest_ref": artifact_refs["understand_anything_manifest_json"],
            "summary_count": len(ua_manifest.get("summary_refs") or []) if isinstance(ua_manifest.get("summary_refs"), list) else 0,
        },
        "llm": {
            "provider": provider,
            "model": model,
            "llm_invoked": bool(
                adaptive.get("llm_invoked")
                or scheduler.get("llm_invoked")
                or hypotheses.get("llm_invoked")
                or adaptive_evidence.get("invoked")
                or scheduler_evidence.get("invoked")
                or hypothesis_evidence.get("invoked")
            ),
            "fallback_used": bool(
                adaptive_evidence.get("fallback_used")
                or scheduler_evidence.get("fallback_used")
                or hypothesis_evidence.get("fallback_used")
            ),
            "advice_consumed": bool(
                consumption.get("advice_consumed")
                or adaptive.get("advice_consumed")
                or hypotheses.get("selected_plan_keys")
            ),
            "allowed_plan_keys": queue.get("allowed_plan_keys")
            or adaptive.get("allowed_plan_keys")
            or hypotheses.get("selected_plan_keys")
            or [],
            "issue_creation": ((adaptive.get("issue_creation_policy") or {}) if isinstance(adaptive.get("issue_creation_policy"), dict) else {}).get("mode")
            or adaptive.get("issue_creation")
            or rollout.get("mode"),
            "adaptive_scheduler": _artifact_summary(adaptive),
            "scheduler_advice": _artifact_summary(scheduler),
            "discovery_hypotheses": _artifact_summary(hypotheses),
            "discovery_hypothesis_count": len(hypotheses.get("hypotheses") or []) if isinstance(hypotheses.get("hypotheses"), list) else 0,
            "selected_plan_count": len(hypotheses.get("selected_plan_keys") or []) if isinstance(hypotheses.get("selected_plan_keys"), list) else 0,
            "discovery_executed_plan_count": (discovery_manifest.get("summary") or {}).get("executed_count") if isinstance(discovery_manifest.get("summary"), dict) else 0,
            "discovery_anomaly_count": (discovery_manifest.get("summary") or {}).get("anomaly_count") if isinstance(discovery_manifest.get("summary"), dict) else 0,
            "bug_candidate_count": (bug_candidate_manifest.get("summary") or {}).get("candidate_count") if isinstance(bug_candidate_manifest.get("summary"), dict) else 0,
            "bug_candidate_issue_payload_count": (bug_candidate_manifest.get("summary") or {}).get("issue_payload_ready_count") if isinstance(bug_candidate_manifest.get("summary"), dict) else 0,
        },
        "prompt_evaluation": {
            "workflow_gate": prompt_eval.get("workflow_gate") or prompt_eval.get("gate") or (prompt_eval.get("policy_gate") or {}).get("workflow_gate"),
            "case_count": prompt_eval.get("case_count") or prompt_metrics.get("case_count"),
            "issue_body_completeness": prompt_eval.get("issue_body_completeness") or prompt_metrics.get("issue_body_completeness"),
            "false_positive_auto_file_rate": prompt_eval.get("false_positive_auto_file_rate") or prompt_metrics.get("false_positive_auto_file_rate"),
            "plan_recommendation_accuracy": prompt_eval.get("plan_recommendation_accuracy") or prompt_metrics.get("plan_recommendation_accuracy"),
        },
        "guarded_rollout": {
            "workflow_gate": rollout.get("workflow_gate") or rollout.get("gate"),
            "mode": rollout.get("mode"),
            "auto_file_allowed": rollout.get("auto_file_allowed"),
            "llm_can_enhance_issue": rollout.get("llm_can_enhance_issue"),
            "llm_enhancement_allowed": rollout.get("llm_enhancement_allowed"),
        },
        "artifact_refs": artifact_refs,
        "warnings": warnings,
    }


def render_llm_value_summary_markdown(payload: dict[str, Any]) -> str:
    codegraph = payload.get("codegraph") if isinstance(payload.get("codegraph"), dict) else {}
    ua = payload.get("understand_anything") if isinstance(payload.get("understand_anything"), dict) else {}
    llm = payload.get("llm") if isinstance(payload.get("llm"), dict) else {}
    prompt = payload.get("prompt_evaluation") if isinstance(payload.get("prompt_evaluation"), dict) else {}
    rollout = payload.get("guarded_rollout") if isinstance(payload.get("guarded_rollout"), dict) else {}
    refs = payload.get("artifact_refs") if isinstance(payload.get("artifact_refs"), dict) else {}
    allowed_plan_keys = ",".join(llm.get("allowed_plan_keys") or []) or "none"
    prompt_cases = prompt.get("case_count") or "unknown"
    prompt_completeness = prompt.get("issue_body_completeness")
    prompt_false_positive = prompt.get("false_positive_auto_file_rate")
    codegraph_ref = refs.get("codegraph_freshness_md") or refs.get("codegraph_freshness_json") or "missing"
    code_summary_ref = refs.get("code_intelligence_summary_md") or refs.get("code_intelligence_summary_json") or "missing"
    adaptive_ref = refs.get("adaptive_scheduler_md") or refs.get("adaptive_scheduler_json") or "missing"
    hypotheses_ref = refs.get("discovery_hypotheses_json") or "missing"
    discovery_manifest_ref = refs.get("discovery_plans_manifest_json") or "missing"
    bug_candidate_ref = refs.get("bug_candidate_queue_manifest_json") or "missing"
    lines = [
        "## LLM + Code Intelligence Value",
        "",
        f"- workflow_gate: `{payload.get('workflow_gate') or 'unknown'}`",
        f"- codegraph: `{codegraph.get('freshness') or 'unknown'}` / `{codegraph.get('status') or 'unknown'}`",
        f"- understand_anything: `{ua.get('status') or 'unknown'}` summaries=`{ua.get('summary_count') or 0}`",
        f"- llm_provider: `{llm.get('provider') or 'unknown'}`",
        f"- llm_model: `{llm.get('model') or 'unknown'}`",
        f"- llm_invoked: `{bool(llm.get('llm_invoked'))}`",
        f"- fallback_used: `{bool(llm.get('fallback_used'))}`",
        f"- advice_consumed: `{bool(llm.get('advice_consumed'))}`",
        f"- discovery_hypotheses: `hypotheses={llm.get('discovery_hypothesis_count') or 0}, selected_plans={llm.get('selected_plan_count') or 0}`",
        f"- discovery_plans: `executed={llm.get('discovery_executed_plan_count') or 0}, anomalies={llm.get('discovery_anomaly_count') or 0}`",
        f"- bug_candidates: `candidates={llm.get('bug_candidate_count') or 0}, issue_payload_drafts={llm.get('bug_candidate_issue_payload_count') or 0}`",
        f"- allowed_plan_keys: `{allowed_plan_keys}`",
        f"- issue_creation_mode: `{llm.get('issue_creation') or 'warning_only'}`",
        f"- prompt_eval: `cases={prompt_cases}, completeness={prompt_completeness if prompt_completeness is not None else 'unknown'}, false_positive={prompt_false_positive if prompt_false_positive is not None else 'unknown'}`",
        f"- guarded_rollout: `mode={rollout.get('mode') or 'unknown'}, can_enhance={rollout.get('llm_can_enhance_issue')}`",
        "",
        "### Compact Artifact Refs",
        "",
        f"- codegraph_freshness: `{codegraph_ref}`",
        f"- code_intelligence_summary: `{code_summary_ref}`",
        f"- adaptive_scheduler: `{adaptive_ref}`",
        f"- discovery_hypotheses: `{hypotheses_ref}`",
        f"- selected_plans: `{refs.get('selected_plans_json') or 'missing'}`",
        f"- discovery_plans: `{discovery_manifest_ref}`",
        f"- bug_candidate_queue: `{bug_candidate_ref}`",
        f"- prompt_evaluation: `{refs.get('prompt_evaluation_json') or 'missing'}`",
        f"- guarded_rollout: `{refs.get('guarded_rollout_json') or 'missing'}`",
        "",
        "This is a compact value summary. Raw JSON artifacts stay in the uploaded artifact bundle and are not inlined in the Nightly summary.",
    ]
    warnings = payload.get("warnings") or []
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings]])
    return "\n".join(lines).rstrip("\n")


def understand_anything_status(
    root: Path | None = None,
    *,
    skip_external: bool = False,
    runner_artifact_mode: bool = False,
) -> dict[str, Any]:
    requested_root = root or REPO_ROOT
    root = _understand_project_root(requested_root)
    graph_root_source = "configured_env" if root != requested_root and os.environ.get(GRAPH_SOURCE_ROOT_ENV) else (
        "canonical_worktree_root" if root != requested_root else "current_worktree"
    )
    graph_path = _understand_graph_path(root)
    config_path = _understand_config_path(root)
    ignore_path = _understand_ignore_path(root)
    graph = _read_json_object(graph_path)
    manifest: dict[str, Any] = {}
    project = graph.get("project") if isinstance(graph.get("project"), dict) else {}
    metadata = graph.get("metadata") if isinstance(graph.get("metadata"), dict) else {}
    graph_commit = project.get("gitCommitHash") or metadata.get("gitCommitHash") or graph.get("gitCommitHash")
    graph_analyzed_at = project.get("analyzedAt") or metadata.get("analyzedAt") or graph.get("analyzedAt")
    current_git_commit = _git_snapshot(requested_root).get("head")
    if graph_path.exists() and graph_commit and current_git_commit:
        if str(graph_commit) == str(current_git_commit):
            freshness = "fresh"
        elif _git_commit_is_ancestor(requested_root, graph_commit, current_git_commit):
            freshness = "base_current"
        else:
            freshness = "stale"
    elif graph_path.exists():
        freshness = "unknown"
    else:
        freshness = "missing"
    warnings: list[str] = []
    if freshness == "stale":
        warnings.append("Understand Anything graph commit differs from the requested worktree commit; use it as warning-only context.")
    notes: list[str] = []
    if freshness == "base_current":
        notes.append("Understand Anything graph matches an ancestor of the current worktree; use it for base-code context plus targeted reads of changed files.")
    latest_manifest = _latest_understand_anything_summary_manifest(root)
    summary_manifest_freshness = None
    if latest_manifest and isinstance(latest_manifest, dict):
        refs = latest_manifest.get("summary_refs") or []
        ref_freshness = {
            str(item.get("freshness"))
            for item in refs
            if isinstance(item, dict) and item.get("freshness")
        }
        if ref_freshness == {"fresh"}:
            summary_manifest_freshness = "fresh"
        elif ref_freshness:
            summary_manifest_freshness = ",".join(sorted(ref_freshness))
    if graph_path.exists() and graph:
        manifest = {
            "node_count": len(graph.get("nodes") or []),
            "edge_count": len(graph.get("edges") or []),
            "project": graph.get("project") or {},
            "version": graph.get("version"),
            "graph_commit": graph_commit,
            "analyzed_at": graph_analyzed_at,
            "freshness": freshness,
        }
        if graph.get("read_error"):
            manifest = {"read_error": graph.get("read_error")}
    home = _user_home()
    codex_skill = _codex_understand_skill_path(home)
    plugin_root = _first_existing_dir(_ua_plugin_root_candidates(home))
    claude_plugin = _claude_understand_plugin_status(skip_external=skip_external)
    configured = config_path.exists() or ignore_path.exists() or bool(plugin_root) or codex_skill.exists()
    runner_context = _runner_context()
    if graph_path.exists():
        status = "available"
    elif runner_artifact_mode and latest_manifest and runner_context in {"github_actions", "ci"}:
        status = "runner_artifact_available"
        notes.append("Understand Anything graph is not present in this runner, but a compact summary manifest is available.")
    elif runner_artifact_mode and runner_context in {"github_actions", "ci"}:
        status = "runner_artifact_unavailable"
        notes.append("Understand Anything local graph is not bundled with this runner checkout; this is warning-only and does not mean local clients are not configured.")
    elif configured:
        status = "configured_missing_graph"
    else:
        status = "not_configured"
    install_commands = _understand_install_commands(home)
    return {
        "provider": "understand_anything",
        "enabled": True,
        "status": status,
        "runner_context": runner_context,
        "expected_version": DEFAULT_UNDERSTAND_ANYTHING_VERSION,
        "graph_path": _repo_rel(graph_path, root),
        "graph_root": str(root),
        "graph_root_source": graph_root_source,
        "graph_exists": graph_path.exists(),
        "graph_commit": graph_commit,
        "graph_analyzed_at": graph_analyzed_at,
        "current_git_commit": current_git_commit,
        "freshness": freshness,
        "warnings": warnings,
        "notes": notes,
        "config_path": _repo_rel(config_path, root),
        "config_exists": config_path.exists(),
        "understandignore_path": _repo_rel(ignore_path, root),
        "understandignore_exists": ignore_path.exists(),
        "codex_skill_path": str(codex_skill),
        "codex_skill_exists": codex_skill.exists(),
        "plugin_root": str(plugin_root) if plugin_root else None,
        "plugin_root_exists": bool(plugin_root),
        "claude_plugin": claude_plugin,
        "configured_for_clients": {
            "codex": codex_skill.exists() or bool(plugin_root),
            "claude_code": bool(claude_plugin.get("enabled")) or bool(plugin_root),
        },
        "auto_update_required": False,
        "blocking_for_issue_workflow": False,
        "manifest": manifest,
        "latest_summary_manifest": latest_manifest,
        "latest_summary_manifest_freshness": summary_manifest_freshness,
        "install_commands": install_commands,
        "configure_command": "python scripts/code_intelligence_adapter.py ua-configure --language zh",
        "generate_graph_command": _understand_generate_command(root),
        "summary_command": f"python scripts/code_intelligence_adapter.py ua-summary-all --root {root}",
    }


def build_understand_anything_summary(
    *,
    module: str,
    root: Path | None = None,
    output_dir: Path | None = None,
    max_nodes: int | None = None,
) -> dict[str, Any]:
    requested_root = root or REPO_ROOT
    graph_root = _understand_project_root(requested_root)
    catalog = _load_catalog(requested_root)
    ua_config = catalog.get("understand_anything") if isinstance(catalog.get("understand_anything"), dict) else {}
    graph_path = _understand_graph_path(graph_root)
    output_dir = output_dir or requested_root / "tmp" / "validation" / "code-intelligence"
    safe_module = re.sub(r"[^A-Za-z0-9_.-]+", "-", module).strip("-") or "unknown"
    json_path = output_dir / f"ua-{safe_module}-summary.json"
    md_path = output_dir / f"ua-{safe_module}-summary.md"
    limit = max_nodes or int(ua_config.get("max_context_nodes_t3") or 60)
    status = understand_anything_status(requested_root, skip_external=True)
    graph_commit = status.get("graph_commit")
    current_git_commit = status.get("current_git_commit")
    graph_analyzed_at = status.get("graph_analyzed_at")
    freshness = status.get("freshness") or "unknown"
    node_count = 0
    edge_count = 0
    selected_nodes: list[dict[str, Any]] = []
    selected_edges: list[dict[str, Any]] = []
    warnings: list[str] = list(status.get("warnings") or [])
    if graph_path.exists():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8-sig"))
            nodes = [node for node in graph.get("nodes", []) if isinstance(node, dict)] if isinstance(graph, dict) else []
            edges = [edge for edge in graph.get("edges", []) if isinstance(edge, dict)] if isinstance(graph, dict) else []
            node_count = len(nodes)
            edge_count = len(edges)
            selected_nodes = [
                node for node in nodes
                if module.lower() in json.dumps(node, ensure_ascii=False).lower()
            ][:limit]
            selected_ids = {str(node.get("id") or node.get("name") or node.get("label")) for node in selected_nodes}
            selected_edges = [
                edge for edge in edges
                if str(edge.get("source") or edge.get("from")) in selected_ids
                or str(edge.get("target") or edge.get("to")) in selected_ids
            ][:limit]
        except Exception as exc:
            warnings.append(f"failed to read Understand Anything graph: {exc}")
    else:
        warnings.append("Understand Anything graph is missing; summary is a non-blocking placeholder.")
    payload = {
        "schema_version": "aistock_understand_anything_summary_v1",
        "generated_at": _utc_now(),
        "graph_provider": "understand_anything",
        "graph_version": ua_config.get("version") or DEFAULT_UNDERSTAND_ANYTHING_VERSION,
        "graph_commit": graph_commit,
        "current_git_commit": current_git_commit,
        "graph_analyzed_at": graph_analyzed_at,
        "freshness": freshness,
        "module": module,
        "status": "ok" if graph_path.exists() and not warnings else "fallback",
        "graph_path": _repo_rel(graph_path, graph_root),
        "graph_root": str(graph_root),
        "graph_root_source": "canonical_worktree_root" if graph_root != requested_root else "current_worktree",
        "graph_exists": graph_path.exists(),
        "summary_ref": _repo_rel(md_path, root),
        "artifact_path": _repo_rel(json_path, root),
        "node_count": node_count,
        "edge_count": edge_count,
        "nodes_used": len(selected_nodes),
        "edges_used": len(selected_edges),
        "selected_nodes": selected_nodes,
        "selected_edges": selected_edges,
        "provenance": "understand_anything_graph_summary_adapter",
        "approval_required_for_long_term_memory": True,
        "blocking_for_issue_workflow": False,
        "warnings": warnings,
        "understand_anything": status,
    }
    _write_json(json_path, payload)
    _write_text(md_path, render_understand_anything_summary_markdown(payload))
    return payload


def build_understand_anything_summary_manifest(
    *,
    modules: list[str] | None = None,
    root: Path | None = None,
    output_dir: Path | None = None,
    max_nodes: int | None = None,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    output_dir = output_dir or root / "tmp" / "validation" / "code-intelligence"
    module_list = [module for module in modules or DEFAULT_UA_MODULES if module.strip()]
    summaries = [
        build_understand_anything_summary(
            module=module,
            root=root,
            output_dir=output_dir,
            max_nodes=max_nodes,
        )
        for module in module_list
    ]
    payload = {
        "schema_version": "aistock_understand_anything_summary_manifest_v1",
        "generated_at": _utc_now(),
        "graph_provider": "understand_anything",
        "workflow_gate": "warning"
        if any(item.get("warnings") or item.get("freshness") in {"missing", "stale"} for item in summaries)
        else "ready",
        "modules": module_list,
        "summary_refs": [
            {
                "module": item.get("module"),
                "status": item.get("status"),
                "freshness": item.get("freshness"),
                "graph_commit": item.get("graph_commit"),
                "current_git_commit": item.get("current_git_commit"),
                "summary_ref": item.get("summary_ref"),
                "artifact_path": item.get("artifact_path"),
            }
            for item in summaries
        ],
        "fresh_summary_count": len([item for item in summaries if item.get("freshness") == "fresh"]),
        "base_current_summary_count": len([item for item in summaries if item.get("freshness") == "base_current"]),
        "stale_summary_count": len([item for item in summaries if item.get("freshness") == "stale"]),
        "missing_summary_count": len([item for item in summaries if item.get("freshness") in {"missing", "unknown"}]),
        "blocking_for_issue_workflow": False,
        "approval_required_for_long_term_memory": True,
    }
    manifest_path = output_dir / "ua-summary-manifest.json"
    _write_json(manifest_path, payload)
    return {**payload, "artifact_path": _repo_rel(manifest_path, root)}


def render_understand_anything_summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"## Understand Anything Summary: {payload.get('module') or 'unknown'}",
        "",
        f"- status: `{payload.get('status') or 'unknown'}`",
        f"- graph_version: `{payload.get('graph_version') or 'unknown'}`",
        f"- graph_commit: `{payload.get('graph_commit') or 'unknown'}`",
        f"- current_git_commit: `{payload.get('current_git_commit') or 'unknown'}`",
        f"- freshness: `{payload.get('freshness') or 'unknown'}`",
        f"- graph_analyzed_at: `{payload.get('graph_analyzed_at') or 'unknown'}`",
        f"- graph_path: `{payload.get('graph_path') or 'not_configured'}`",
        f"- nodes_used: `{payload.get('nodes_used', 0)}` / `{payload.get('node_count', 0)}`",
        f"- edges_used: `{payload.get('edges_used', 0)}` / `{payload.get('edge_count', 0)}`",
        "- approval_required_for_long_term_memory: `true`",
        "",
        "### Selected Nodes",
    ]
    nodes = payload.get("selected_nodes") or []
    lines.extend(f"- `{str(node.get('id') or node.get('name') or node.get('label') or node)[:160]}`" for node in nodes[:20])
    if not nodes:
        lines.append("- `none`")
    warnings = payload.get("warnings") or []
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings]])
    lines.extend([
        "",
        "This summary is a read-only graph artifact for Research Assistant / Validation Center context. It is not a test result and does not block issue workflow.",
        "",
    ])
    return "\n".join(lines)


def build_doctor_report(root: Path | None = None, *, skip_external: bool = False) -> dict[str, Any]:
    root = root or REPO_ROOT
    catalog = _load_catalog(root)
    codegraph = codegraph_status(root, skip_external=skip_external)
    freshness = latest_codegraph_freshness(root, live_status=codegraph)
    ua = understand_anything_status(root, skip_external=skip_external)
    warnings: list[str] = []
    if not codegraph.get("available"):
        warnings.append("CodeGraph CLI is unavailable; issue workflow will fall back to existing rg/catalog context.")
    elif not codegraph.get("index_exists"):
        warnings.append(f"CodeGraph index is missing; run {codegraph.get('bootstrap_command')} when code intelligence context is needed.")
    warnings.extend(f"freshness artifact: {item}" for item in freshness.get("warnings") or [])
    if ua.get("status") == "not_configured":
        warnings.append(
            "Understand Anything is not configured; install/configure it when graph-first context is needed."
        )
    elif not ua.get("graph_exists"):
        warnings.append(
            "Understand Anything graph is configured but missing; run the generate_graph_command before relying on UA summaries."
        )
    return {
        "schema_version": "aistock_code_intelligence_doctor_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "warning" if warnings else "ready",
        "warnings": warnings,
        "blocking": [],
        "repo_root": str(root),
        "catalog": catalog,
        "codegraph": codegraph,
        "codegraph_freshness": freshness,
        "understand_anything": ua,
        "bootstrap_commands": {
            "codegraph": codegraph.get("bootstrap_command"),
            "understand_anything_codex": (ua.get("install_commands") or {}).get("codex"),
            "understand_anything_claude_code": (ua.get("install_commands") or {}).get("claude_code"),
            "understand_anything_configure": ua.get("configure_command"),
            "understand_anything_generate_graph": ua.get("generate_graph_command"),
        },
    }


def _workflow_dir(item_id: str, root: Path | None = None) -> Path:
    return (root or REPO_ROOT) / WORKFLOW_ROOT / item_id


def _fallback_context(query: str, changed_files: list[str]) -> str:
    lines = [
        "# Code Intelligence Context",
        "",
        "- provider: `fallback`",
        "- status: `codegraph_unavailable_or_missing_index`",
        f"- query: `{query or 'n/a'}`",
        "",
        "## Changed / scoped files",
        *[f"- `{path}`" for path in changed_files or ["none"]],
        "",
        "## Guidance",
        "Use AIstock allowed_write_scope, file_ownership.yaml, and targeted reads. Do not run full-repo exploration unless this fallback is insufficient.",
    ]
    return "\n".join(lines)


def _repo_level_context(query: str, changed_files: list[str], status: dict[str, Any], reason: str | None) -> str:
    summary = status.get("index_summary") if isinstance(status.get("index_summary"), dict) else {}
    lines = [
        "# Code Intelligence Context",
        "",
        "- provider: `codegraph`",
        "- status: `repo_index_ready`",
        f"- graph_root: `{status.get('graph_root') or 'unknown'}`",
        f"- graph_root_source: `{status.get('graph_root_source') or 'unknown'}`",
        f"- context_detail: `{_compact_text(reason, max_chars=240) or 'repo-level index used'}`",
        f"- files: `{summary.get('files', 'unknown')}`",
        f"- nodes: `{summary.get('nodes', 'unknown')}`",
        f"- edges: `{summary.get('edges', 'unknown')}`",
        f"- query: `{query or 'n/a'}`",
        "",
        "## Changed / scoped files",
        *[f"- `{path}`" for path in changed_files or ["none"]],
        "",
        "## Guidance",
        "Use the CodeGraph repo index plus AIstock allowed_write_scope, file_ownership.yaml, and targeted reads. Broad repo scans remain unnecessary unless this scoped context is insufficient.",
    ]
    text = ""
    for line in lines:
        text = f"{text}\n{line}" if text else line
    return text


def _line_count(text: str) -> int:
    if not text:
        return 0
    return text.count("\n") + 1


def _changed_file_outline(root: Path, repo_path: str) -> dict[str, Any]:
    normalized = _norm_repo_path(repo_path)
    path = root / normalized
    exists = path.is_file()
    stat = path.stat() if exists else None
    lines: list[str] = []
    symbol_patterns = (
        re.compile(r"^\s*def\s+([A-Za-z_][\w]*)\s*\("),
        re.compile(r"^\s*class\s+([A-Za-z_][\w]*)\s*[:(]"),
        re.compile(r"^\s*(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_][\w]*)\s*\("),
        re.compile(r"^\s*(?:export\s+)?(?:const|let|var)\s+([A-Za-z_][\w]*)\s*="),
    )
    symbols: list[dict[str, Any]] = []
    if exists:
        text = _read_text_if_exists(path)
        lines = text.splitlines()
        for line_number, line in enumerate(lines, start=1):
            for pattern in symbol_patterns:
                match = pattern.search(line)
                if match:
                    symbols.append({"name": match.group(1), "line": line_number})
                    break
            if len(symbols) >= 12:
                break
    return {
        "path": normalized,
        "exists": exists,
        "size_bytes": stat.st_size if stat else None,
        "line_count": len(lines) if exists else None,
        "symbols": symbols,
    }


def _scoped_changed_file_context(root: Path, changed_files: list[str]) -> tuple[str, list[dict[str, Any]]]:
    outlines = [_changed_file_outline(root, path) for path in changed_files]
    lines = [
        "## Scoped Changed-File Context",
        "",
        "This section is generated from --changed-file and is shown before raw CodeGraph hits so agents can stay inside allowed_write_scope.",
    ]
    for outline in outlines:
        lines.extend(
            [
                "",
                f"### {outline['path']}",
                f"- exists: `{str(bool(outline['exists'])).lower()}`",
                f"- line_count: `{outline.get('line_count') if outline.get('line_count') is not None else 'unknown'}`",
                f"- size_bytes: `{outline.get('size_bytes') if outline.get('size_bytes') is not None else 'unknown'}`",
                "- symbols: "
                + (
                    ", ".join(f"`{item['name']}:{item['line']}`" for item in outline.get("symbols") or [])
                    if outline.get("symbols")
                    else "`none_detected`"
                ),
            ]
        )
    return "\n".join(lines), outlines


def _context_quality(context_text: str, changed_files: list[str], *, channel: str) -> dict[str, Any]:
    normalized_text = context_text.replace("\\", "/").lower()
    changed = [_norm_repo_path(path) for path in changed_files if str(path).strip()]
    matched = [path for path in changed if path.lower() in normalized_text]
    warnings: list[str] = []
    quality = "scoped"
    broad_scan_required = False
    if channel == "scoped_fallback":
        quality = "scoped_fallback"
    elif not changed:
        quality = "orientation_only"
    elif not matched and channel == "cli":
        quality = "no_direct_scope_hit"
        warnings.append(
            "CodeGraph detail context did not include scoped changed files; treat raw entry points as orientation-only."
        )
    elif not matched and channel in {"repo_index", "fallback"}:
        quality = channel
    return {
        "schema_version": "aistock_codegraph_context_quality_v1",
        "quality": quality,
        "channel": channel,
        "changed_files": changed,
        "matched_changed_files": matched,
        "noisy_context_warning": quality == "no_direct_scope_hit",
        "broad_scan_required": broad_scan_required,
        "next_action": "start_from_allowed_write_scope_and_affected_tests"
        if quality == "no_direct_scope_hit"
        else "use_context_refs_before_targeted_reads",
        "warnings": warnings,
    }


def _prepend_context_guidance(
    *,
    context_text: str,
    query: str,
    changed_files: list[str],
    status: dict[str, Any],
    quality: dict[str, Any],
) -> str:
    changed_inline = ", ".join(changed_files) if changed_files else "none"
    matched_inline = ", ".join(quality.get("matched_changed_files") or []) or "none"
    warning_line = (
        "- warning: CodeGraph raw context did not hit scoped files; do not trust unrelated entry points as fix scope."
        if quality.get("noisy_context_warning")
        else "- warning: none"
    )
    header = [
        "# Code Intelligence Context Guidance",
        "",
        f"- query: `{query or 'n/a'}`",
        f"- graph_root_source: `{status.get('graph_root_source') or 'unknown'}`",
        f"- context_quality: `{quality.get('quality') or 'unknown'}`",
        f"- scoped_files: `{changed_inline}`",
        f"- matched_scoped_files: `{matched_inline}`",
        f"- next_action: `{quality.get('next_action') or 'use_context_refs_before_targeted_reads'}`",
        warning_line,
        "",
        "Use this header as the decision guide. The raw CodeGraph section below is only supporting context; if it is noisy, read the allowed write scope and affected-tests artifact before any targeted source reads.",
        "",
        "---",
        "",
    ]
    return "\n".join(header) + context_text.lstrip()


def build_context_artifacts(
    *,
    item_id: str,
    query: str,
    changed_files: list[str] | None = None,
    root: Path | None = None,
    max_symbols: int = 12,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    changed = nightly_discovery_input_pack.unique_repo_paths(list(changed_files or []))
    output_dir = _workflow_dir(item_id, root)
    context_path = output_dir / "codegraph-context.md"
    manifest_path = output_dir / "code-intelligence.json"
    status = codegraph_status(root, skip_external=skip_external)
    fallback = _codegraph_fallback(status=status, skip_external=skip_external)
    fallback_used = True
    fallback_reason = str(fallback.get("reason") or "codegraph_unavailable_or_missing_index")
    graph_ready = bool(status.get("available") and status.get("index_exists") and not skip_external)
    context_text = _fallback_context(query, changed)
    channel = "fallback"
    if graph_ready:
        command = str(status["command"])
        graph_root = str(status.get("graph_root") or root)
        result = _run_command(
            [command, "context", query or " ", "--path", graph_root, "--max-nodes", str(max_symbols)],
            cwd=root,
            timeout=60,
        )
        if result.get("ok") and (result.get("stdout") or "").strip():
            fallback_used = False
            fallback_reason = None
            fallback = {"used": False, "reason": None, "detail": None, "runner_context": _runner_context()}
            context_text = str(result.get("stdout") or "")
            channel = "cli"
        else:
            fallback_reason = result.get("stderr") or result.get("stdout") or "codegraph_context_failed"
            fallback_used = False
            fallback = {
                "used": False,
                "reason": None,
                "detail": f"CodeGraph detail context failed; repo-level index summary used: {_compact_text(str(fallback_reason), max_chars=180)}",
                "runner_context": _runner_context(),
            }
            context_text = _repo_level_context(query, changed, status, str(fallback_reason))
            channel = "repo_index"
    quality = _context_quality(context_text, changed, channel=channel)
    scoped_context = ""
    scoped_file_context: dict[str, Any] = {"enabled": False, "outlines": []}
    if changed and quality.get("noisy_context_warning"):
        scoped_text, outlines = _scoped_changed_file_context(root, changed)
        scoped_context = scoped_text.rstrip() + "\n\n---\n\n"
        scoped_file_context = {
            "enabled": True,
            "outlines": outlines,
            "context_lines": _line_count(scoped_text),
        }
        merged_text = scoped_context + context_text.lstrip()
        quality = _context_quality(merged_text, changed, channel="scoped_fallback")
        quality["scoped_fallback_inserted"] = True
        quality["original_channel"] = channel
        channel = "scoped_fallback"
        context_text = merged_text
    context_text = _prepend_context_guidance(
        context_text=context_text,
        query=query,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        status=status,
        quality=quality,
    )
    _write_text(context_path, context_text)
    manifest = {
        "schema_version": "aistock_code_intelligence_context_v1",
        "generated_at": _utc_now(),
        "tool": "codegraph",
        "tool_version": status.get("version") or status.get("expected_version"),
        "repo_root": str(root),
        "git_commit": status.get("git_commit"),
        "working_tree_dirty": status.get("working_tree_dirty"),
        "query": query,
        "changed_files": changed,
        "status": "fallback" if fallback_used else ("repo_index_ready" if channel == "repo_index" else "ok"),
        "context_markdown": _repo_rel(context_path, root),
        "fallback": {**fallback, "used": fallback_used, "reason": fallback_reason},
        "graph_root": status.get("graph_root"),
        "graph_root_source": status.get("graph_root_source"),
        "channel": channel,
        "context_quality": quality,
        "scoped_file_context": scoped_file_context,
        "runner_context": _runner_context(),
        "codegraph_status": {
            "available": status.get("available"),
            "index_exists": status.get("index_exists"),
            "status": status.get("status"),
            "status_check": status.get("status_check"),
            "index_summary": status.get("index_summary"),
            "git_commit": status.get("git_commit"),
            "graph_root": status.get("graph_root"),
            "graph_root_source": status.get("graph_root_source"),
        },
    }
    _write_json(manifest_path, manifest)
    return {**manifest, "manifest_path": _repo_rel(manifest_path, root)}


def _norm_repo_path(path: str | Path) -> str:
    normalized = str(path).replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _read_text_if_exists(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _module_name_from_changed_file(path: str) -> str | None:
    normalized = _norm_repo_path(path)
    if not normalized.endswith(".py"):
        return None
    if not (
        normalized.startswith("scripts/")
        or normalized.startswith("backend/")
        or normalized.startswith("tests/")
    ):
        return None
    return normalized[:-3].replace("/", ".")


def _candidate_test_files(root: Path, changed_files: list[str]) -> list[str]:
    candidates: list[str] = []
    for pattern in ("backend/tests/**/*.py", "tests/**/*.py"):
        candidates.extend(
            _norm_repo_path(path.relative_to(root))
            for path in root.glob(pattern)
            if path.is_file()
        )
    for changed in changed_files:
        normalized = _norm_repo_path(changed)
        path = root / normalized
        if path.is_file() and re.search(r"(^|/)test_.*\.py$", normalized):
            candidates.append(normalized)
    return sorted(set(candidates))


def _discover_repo_test_fallbacks(
    *,
    root: Path,
    changed_files: list[str],
    filter_glob: str | None,
) -> tuple[list[str], dict[str, Any]]:
    """Find obvious tests when CodeGraph affected-tests under-recognizes Python refs."""
    modules = {
        module
        for changed in changed_files
        if (module := _module_name_from_changed_file(changed))
    }
    discovered: dict[str, list[str]] = {}
    for test_path in _candidate_test_files(root, changed_files):
        if filter_glob and not fnmatch.fnmatch(test_path, filter_glob):
            continue
        text = _read_text_if_exists(root / test_path)
        if not text:
            continue
        hits = sorted(module for module in modules if module in text)
        if hits:
            discovered[test_path] = hits
    direct_matches: dict[str, list[str]] = {}
    for changed in changed_files:
        normalized = _norm_repo_path(changed)
        if not (normalized.startswith("scripts/") and normalized.endswith(".py")):
            continue
        stem = Path(normalized).stem
        for candidate in (
            f"backend/tests/scripts/test_{stem}.py",
            f"tests/scripts/test_{stem}.py",
            f"tests/test_{stem}.py",
        ):
            if filter_glob and not fnmatch.fnmatch(candidate, filter_glob):
                continue
            if candidate in discovered or not (root / candidate).is_file():
                continue
            direct_matches.setdefault(candidate, []).append(f"direct:{normalized}")
            discovered[candidate] = direct_matches[candidate]
    return sorted(discovered), {
        "strategy": "python_import_text_scan_plus_direct_script_test_map",
        "modules": sorted(modules),
        "matched_tests": discovered,
        "direct_matches": direct_matches,
        "enabled": bool(modules),
    }


def build_affected_tests_artifact(
    *,
    item_id: str,
    changed_files: list[str] | None = None,
    root: Path | None = None,
    filter_glob: str | None = None,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    changed = nightly_discovery_input_pack.unique_repo_paths(list(changed_files or []))
    output_dir = _workflow_dir(item_id, root)
    out_path = output_dir / "affected-tests.json"
    status = codegraph_status(root, skip_external=skip_external)
    suggested: list[str] = []
    fallback = _codegraph_fallback(status=status, skip_external=skip_external)
    fallback_used = True
    fallback_reason = str(fallback.get("reason") or "codegraph_unavailable_or_missing_index")
    graph_ready = bool(changed and status.get("available") and status.get("index_exists") and not skip_external)
    if graph_ready:
        command = str(status["command"])
        graph_root = str(status.get("graph_root") or root)
        args = [command, "affected", "--path", graph_root, *changed, "--quiet"]
        if filter_glob:
            args.extend(["--filter", filter_glob])
        result = _run_command(args, cwd=root, timeout=60)
        if result.get("ok"):
            suggested = [line.strip() for line in str(result.get("stdout") or "").splitlines() if line.strip()]
            fallback_used = False
            fallback_reason = None
            fallback = {"used": False, "reason": None, "detail": None, "runner_context": _runner_context()}
        else:
            fallback_reason = result.get("stderr") or result.get("stdout") or "codegraph_affected_failed"
    codegraph_suggested = list(suggested)
    test_fallback_suggested, test_discovery = _discover_repo_test_fallbacks(
        root=root,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        filter_glob=filter_glob,
    )
    supplement = [path for path in test_fallback_suggested if path not in suggested]
    if supplement:
        suggested.extend(supplement)
    quality = "ok"
    if fallback_used:
        quality = "codegraph_fallback"
    elif supplement:
        quality = "partial_codegraph_plus_repo_fallback"
    payload = {
        "schema_version": "aistock_codegraph_affected_tests_v1",
        "generated_at": _utc_now(),
        "tool": "codegraph",
        "tool_version": status.get("version") or status.get("expected_version"),
        "changed_files": changed,
        "graph_root": status.get("graph_root"),
        "graph_root_source": status.get("graph_root_source"),
        "suggested_tests": suggested,
        "codegraph_suggested_tests": codegraph_suggested,
        "repo_fallback_suggested_tests": supplement,
        "filter": filter_glob,
        "status": "fallback" if fallback_used else "ok",
        "fallback": {**fallback, "used": fallback_used, "reason": fallback_reason},
        "test_discovery_fallback": {
            "used": bool(supplement),
            **test_discovery,
        },
        "quality": quality,
        "source": "codegraph affected"
        if not fallback_used and not supplement
        else ("codegraph affected + aistock repo fallback" if not fallback_used else "aistock fallback"),
        "runner_context": _runner_context(),
    }
    _write_json(out_path, payload)
    return {**payload, "artifact_path": _repo_rel(out_path, root)}


def build_summary(
    *,
    item_id: str,
    query: str,
    changed_files: list[str] | None = None,
    module: str | None = None,
    root: Path | None = None,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    changed = nightly_discovery_input_pack.unique_repo_paths(list(changed_files or []))
    context = build_context_artifacts(
        item_id=item_id,
        query=query,
        changed_files=changed,
        root=root,
        skip_external=skip_external,
    )
    affected = build_affected_tests_artifact(
        item_id=item_id,
        changed_files=changed,
        root=root,
        skip_external=skip_external,
    )
    ua_status = understand_anything_status(
        root,
        skip_external=True,
        runner_artifact_mode=_runner_context() in {"github_actions", "ci"},
    )
    freshness = latest_codegraph_freshness(root, live_status=context.get("codegraph_status"))
    ua_summary: dict[str, Any] | None = None
    if module:
        ua_payload = build_understand_anything_summary(
            module=module,
            root=root,
            output_dir=_workflow_dir(item_id, root),
        )
        ua_summary = {
            "module": ua_payload.get("module"),
            "status": ua_payload.get("status"),
            "artifact_path": ua_payload.get("artifact_path"),
            "summary_ref": ua_payload.get("summary_ref"),
            "graph_exists": ua_payload.get("graph_exists"),
            "node_count": ua_payload.get("node_count"),
            "edge_count": ua_payload.get("edge_count"),
            "nodes_used": ua_payload.get("nodes_used"),
            "edges_used": ua_payload.get("edges_used"),
            "blocking_for_issue_workflow": False,
        }
    verify_parts = [
        "python scripts/code_intelligence_adapter.py verify-clients",
        f"--item-id {item_id}",
    ]
    if module:
        verify_parts.append(f"--module {module}")
    verify_parts.extend(f"--changed-file {path}" for path in changed[:12])
    freshness_effective = freshness.get("effective") if isinstance(freshness, dict) else {}
    return {
        "schema_version": "aistock_code_intelligence_summary_v1",
        "generated_at": _utc_now(),
        "item_id": item_id,
        "module": module,
        "provider": "codegraph",
        "runner_context": _runner_context(),
        "status": "ok" if context["status"] in {"ok", "repo_index_ready"} or affected["status"] == "ok" else "fallback",
        "context_ref": context.get("context_markdown"),
        "manifest_ref": context.get("manifest_path"),
        "affected_tests_ref": affected.get("artifact_path"),
        "affected_tests_count": len(affected.get("suggested_tests") or []),
        "affected_quality": affected.get("quality"),
        "fallback_used": bool(context.get("fallback", {}).get("used") and affected.get("fallback", {}).get("used")),
        "graph_root": context.get("graph_root") or affected.get("graph_root"),
        "graph_root_source": context.get("graph_root_source") or affected.get("graph_root_source"),
        "latest_freshness": (freshness_effective or {}).get("freshness") if isinstance(freshness, dict) else None,
        "latest_freshness_ref": ((freshness_effective or {}).get("artifact_path") if isinstance(freshness, dict) else None),
        "latest_freshness_source": freshness.get("effective_source") if isinstance(freshness, dict) else None,
        "stale_metadata_warning": bool(freshness.get("stale_metadata_warning")) if isinstance(freshness, dict) else False,
        "consume_command": "python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale",
        "verify_command": " ".join(verify_parts),
        "context": context,
        "affected_tests": affected,
        "understand_anything": ua_status,
        "understand_anything_summary_ref": (ua_summary or {}).get("summary_ref"),
        "understand_anything_summary": ua_summary,
    }


def _client_file_status(path: Path, required_terms: list[str] | None = None) -> dict[str, Any]:
    exists = path.exists()
    missing_terms: list[str] = []
    if exists and required_terms:
        text = _read_text_if_exists(path).lower()
        missing_terms = [term for term in required_terms if term.lower() not in text]
    return {
        "path": str(path),
        "exists": exists,
        "status": "ready" if exists and not missing_terms else ("stale" if exists else "missing"),
        "missing_terms": missing_terms,
    }


def build_client_verification(
    *,
    root: Path | None = None,
    item_id: str = "VERIFY-CODE-INTELLIGENCE",
    query: str = "AIstock code intelligence workflow verification",
    changed_files: list[str] | None = None,
    module: str | None = "validation",
    output_dir: Path | None = None,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    changed = nightly_discovery_input_pack.unique_repo_paths(list(changed_files or []))
    output_dir = output_dir or root / "tmp" / "validation" / "code-intelligence" / item_id
    output_dir.mkdir(parents=True, exist_ok=True)
    status = codegraph_status(root, skip_external=skip_external)
    freshness = latest_codegraph_freshness(
        root,
        live_status=status,
        persist_effective=True,
        output_dir=root / "tmp" / "validation" / "code-intelligence" / "latest",
    )
    context = build_context_artifacts(
        item_id=item_id,
        query=query,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        root=root,
        max_symbols=8,
        skip_external=skip_external,
    )
    affected = build_affected_tests_artifact(
        item_id=item_id,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        root=root,
        skip_external=skip_external,
    )
    ua = understand_anything_status(
        root,
        skip_external=True,
        runner_artifact_mode=_runner_context() in {"github_actions", "ci"},
    )
    ua_summary: dict[str, Any] | None = None
    if module:
        ua_summary = build_understand_anything_summary(
            module=module,
            root=root,
            output_dir=output_dir,
        )
    home = _user_home()
    clients = {
        "codex_issue_skill": _client_file_status(
            home / ".codex" / "skills" / "fix-aistock-issue" / "SKILL.md",
            ["graph-first", "code intelligence", "aistock_issue_workflow.py"],
        ),
        "claude_issue_command": _client_file_status(
            home / ".claude" / "commands" / "fix-aistock-issue.md",
            ["graph-first", "code intelligence", "aistock_issue_workflow.py"],
        ),
        "codex_understand_skill": _client_file_status(
            home / ".understand-anything" / "repo" / "understand-anything-plugin" / "skills" / "understand" / "SKILL.md",
            ["understand"],
        ),
        "codex_understand_chat_skill": _client_file_status(
            home
            / ".understand-anything"
            / "repo"
            / "understand-anything-plugin"
            / "skills"
            / "understand-chat"
            / "SKILL.md",
            ["understand"],
        ),
    }
    warnings: list[str] = []
    blocking: list[str] = []
    if not status.get("available"):
        warnings.append("CodeGraph CLI is unavailable; graph-first context falls back to scoped files.")
    elif not status.get("index_exists"):
        warnings.append("CodeGraph index is missing; run bootstrap before expecting graph-backed context.")
    effective = freshness.get("effective") if isinstance(freshness.get("effective"), dict) else {}
    latest = freshness.get("latest") if isinstance(freshness.get("latest"), dict) else {}
    current_commit = status.get("git_commit")
    stale_metadata_warning = bool(
        latest
        and latest.get("git_commit")
        and current_commit
        and str(latest.get("git_commit")) != str(current_commit)
        and effective.get("freshness") == "fresh"
    )
    if stale_metadata_warning:
        warnings.append(
            "Latest persisted CodeGraph freshness artifact commit differs from current HEAD, but live/effective status is fresh."
        )
    context_quality = context.get("context_quality") if isinstance(context.get("context_quality"), dict) else {}
    warnings.extend(str(item) for item in context_quality.get("warnings") or [])
    if ua.get("freshness") == "stale":
        warnings.append(
            f"Understand Anything graph freshness is {ua.get('freshness')}; use as warning-only base context."
        )
    for name, client in clients.items():
        if client["status"] != "ready":
            warnings.append(f"{name} is {client['status']}.")
    graph_ready = bool(status.get("available") and status.get("index_exists"))
    required_clients_ready = all(
        clients[name]["status"] == "ready"
        for name in ("codex_issue_skill", "claude_issue_command")
    )
    if not required_clients_ready:
        blocking.append("Codex/Claude issue workflow entry is missing or stale.")
    gate = "blocked" if blocking else ("warning" if warnings else "ready")
    payload = {
        "schema_version": "aistock_code_intelligence_client_verification_v1",
        "generated_at": _utc_now(),
        "workflow_gate": gate,
        "blocking_for_issue_workflow": False,
        "root": str(root),
        "item_id": item_id,
        "runner_context": _runner_context(),
        "codegraph": {
            "status": status.get("status"),
            "available": status.get("available"),
            "index_exists": status.get("index_exists"),
            "graph_root": status.get("graph_root"),
            "graph_root_source": status.get("graph_root_source"),
            "git_commit": status.get("git_commit"),
            "graph_root_git_commit": status.get("graph_root_git_commit"),
            "graph_commit_relation": status.get("graph_commit_relation"),
            "index_summary": status.get("index_summary") or {},
        },
        "freshness": {
            "workflow_gate": freshness.get("workflow_gate"),
            "effective_freshness": effective.get("freshness"),
            "effective_source": freshness.get("effective_source"),
            "latest_artifact_ref": latest.get("artifact_path"),
            "latest_git_commit": latest.get("git_commit"),
            "current_git_commit": current_commit,
            "stale_metadata_warning": stale_metadata_warning,
        },
        "context": {
            "status": context.get("status"),
            "context_ref": context.get("context_markdown"),
            "quality": context_quality.get("quality"),
            "matched_changed_files": context_quality.get("matched_changed_files") or [],
            "noisy_context_warning": context_quality.get("noisy_context_warning"),
            "broad_scan_required": context_quality.get("broad_scan_required"),
            "scoped_fallback_inserted": context_quality.get("scoped_fallback_inserted"),
        },
        "affected_tests": {
            "status": affected.get("status"),
            "affected_tests_ref": affected.get("artifact_path"),
            "suggested_tests_count": len(affected.get("suggested_tests") or []),
            "quality": affected.get("quality"),
        },
        "understand_anything": {
            "status": ua.get("status"),
            "graph_exists": ua.get("graph_exists"),
            "freshness": ua.get("freshness"),
            "graph_commit": ua.get("graph_commit"),
            "current_git_commit": ua.get("current_git_commit"),
            "summary_ref": (ua_summary or {}).get("summary_ref"),
            "nodes_used": (ua_summary or {}).get("nodes_used"),
            "stale_but_usable": ua.get("freshness") in {"base_current", "stale"},
        },
        "clients": clients,
        "artifacts": {
            "context_ref": context.get("context_markdown"),
            "affected_tests_ref": affected.get("artifact_path"),
            "ua_summary_ref": (ua_summary or {}).get("summary_ref"),
        },
        "efficiency": {
            "broad_scan_required": False,
            "graph_ready": graph_ready,
            "large_graph_payload_inlined": False,
            "next_actions": [
                "read_task_card_code_intelligence_refs",
                "use_scoped_changed_file_context_before_raw_graph_hits",
                "run_required_validation_gates",
            ],
        },
        "warnings": warnings,
        "blocking": blocking,
    }
    summary_path = output_dir / "client-verification.json"
    _write_json(summary_path, payload)
    return {**payload, "artifact_path": _repo_rel(summary_path, root)}


def render_client_verification_summary(payload: dict[str, Any]) -> str:
    codegraph = payload.get("codegraph") if isinstance(payload.get("codegraph"), dict) else {}
    freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    affected = payload.get("affected_tests") if isinstance(payload.get("affected_tests"), dict) else {}
    ua = payload.get("understand_anything") if isinstance(payload.get("understand_anything"), dict) else {}
    clients = payload.get("clients") if isinstance(payload.get("clients"), dict) else {}
    ready_clients = sum(1 for item in clients.values() if isinstance(item, dict) and item.get("status") == "ready")
    lines = [
        "## Code Intelligence Client Verification",
        "",
        f"- workflow_gate: `{payload.get('workflow_gate') or 'unknown'}`",
        f"- codegraph: `{codegraph.get('status') or 'unknown'}` / index_exists `{str(bool(codegraph.get('index_exists'))).lower()}`",
        f"- effective_freshness: `{freshness.get('effective_freshness') or 'unknown'}` via `{freshness.get('effective_source') or 'unknown'}`",
        f"- stale_metadata_warning: `{str(bool(freshness.get('stale_metadata_warning'))).lower()}`",
        f"- context_quality: `{context.get('quality') or 'unknown'}`",
        f"- affected_tests: `{affected.get('suggested_tests_count', 0)}` / `{affected.get('quality') or 'unknown'}`",
        f"- understand_anything: `{ua.get('status') or 'unknown'}` / `{ua.get('freshness') or 'unknown'}`",
        f"- clients_ready: `{ready_clients}` / `{len(clients)}`",
        f"- context_ref: `{(payload.get('artifacts') or {}).get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{(payload.get('artifacts') or {}).get('affected_tests_ref') or 'not_generated'}`",
        f"- ua_summary_ref: `{(payload.get('artifacts') or {}).get('ua_summary_ref') or 'not_generated'}`",
    ]
    warnings = payload.get("warnings") or []
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings[:8]]])
    blocking = payload.get("blocking") or []
    if blocking:
        lines.extend(["", "### Blocking", *[f"- {item}" for item in blocking]])
    lines.extend(["", "Large graph payloads are not inlined; use artifact refs only when exact diagnostics are needed."])
    return "\n".join(lines)


def _inline(items: list[Any] | tuple[Any, ...] | None, *, default: str = "none") -> str:
    values = [str(item) for item in items or [] if str(item).strip()]
    return ", ".join(values) if values else default


def render_summary_markdown(payload: dict[str, Any]) -> str:
    context = payload.get("context") or {}
    affected = payload.get("affected_tests") or {}
    context_fallback = context.get("fallback") or {}
    affected_fallback = affected.get("fallback") or {}
    ua = payload.get("understand_anything") or {}
    ua_summary = payload.get("understand_anything_summary") or {}
    suggested_tests = [str(item) for item in affected.get("suggested_tests") or [] if str(item).strip()]
    warnings = []
    if context_fallback.get("used"):
        warnings.append(f"context fallback: {context_fallback.get('reason') or 'unknown'}")
    if affected_fallback.get("used"):
        warnings.append(f"affected-tests fallback: {affected_fallback.get('reason') or 'unknown'}")
    test_discovery = affected.get("test_discovery_fallback") or {}
    if test_discovery.get("used"):
        warnings.append("affected-tests supplemented by repo-local Python import scan")
    lines = [
        "## Code Intelligence Summary",
        "",
        f"- provider: `{payload.get('provider') or 'codegraph'}`",
        f"- runner_context: `{payload.get('runner_context') or 'local'}`",
        f"- status: `{payload.get('status') or 'unknown'}`",
        f"- fallback_used: `{str(bool(payload.get('fallback_used'))).lower()}`",
        f"- affected_quality: `{affected.get('quality') or 'unknown'}`",
        f"- context_ref: `{payload.get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{payload.get('affected_tests_ref') or 'not_generated'}`",
        f"- affected_tests_count: `{payload.get('affected_tests_count', 0)}`",
        f"- latest_freshness: `{payload.get('latest_freshness') or 'not_available'}`",
        f"- latest_freshness_ref: `{payload.get('latest_freshness_ref') or 'not_available'}`",
        f"- consume_command: `{payload.get('consume_command') or 'python scripts/code_intelligence_adapter.py latest-freshness --refresh-if-stale'}`",
        f"- changed_files: `{_inline(affected.get('changed_files') or context.get('changed_files'))}`",
        f"- understand_anything_status: `{ua.get('status') or 'unknown'}`",
        f"- understand_anything_summary_ref: `{payload.get('understand_anything_summary_ref') or 'not_generated'}`",
        f"- understand_anything_nodes_used: `{ua_summary.get('nodes_used', 0)}`",
        "",
        "### Graph-First Client Steps",
        "- Read `context_ref` and `affected_tests_ref` before using broad `rg`.",
        "- Read `understand_anything_summary_ref` when `understand_anything_status=available`.",
        f"- If UA graph is missing: `{ua.get('generate_graph_command') or '/understand --language zh --no-auto-update'}`.",
        "",
        "### Suggested Impacted Tests",
        *[f"- `{item}`" for item in suggested_tests or ["none"]],
    ]
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings]])
    runner_notes = []
    for item in (context_fallback, affected_fallback):
        if item.get("detail") and item.get("detail") not in runner_notes:
            runner_notes.append(str(item.get("detail")))
    if ua.get("runner_context") in {"github_actions", "ci"} and ua.get("status") in {
        "runner_artifact_unavailable",
        "runner_artifact_available",
    }:
        runner_notes.extend(str(item) for item in ua.get("notes") or [] if str(item).strip())
    if runner_notes:
        lines.extend(["", "### Runner Context", *[f"- {item}" for item in runner_notes[:4]]])
    lines.extend([
        "",
        "Code intelligence is warning-only. Final merge readiness still depends on AIstock nox, pytest, Validation Center, and production gates.",
        "",
    ])
    return "\n".join(lines)


def cmd_doctor(args: argparse.Namespace) -> int:
    payload = build_doctor_report(Path(args.root) if args.root else REPO_ROOT, skip_external=args.skip_external)
    codegraph = payload.get("codegraph") if isinstance(payload.get("codegraph"), dict) else {}
    freshness = payload.get("codegraph_freshness") if isinstance(payload.get("codegraph_freshness"), dict) else {}
    effective = freshness.get("effective") if isinstance(freshness.get("effective"), dict) else {}
    ua = payload.get("understand_anything") if isinstance(payload.get("understand_anything"), dict) else {}
    _emit_compact_line(
        "code-intelligence-doctor",
        {
            "workflow_gate": payload.get("workflow_gate"),
            "codegraph": codegraph.get("status"),
            "index": str(bool(codegraph.get("index_exists"))).lower(),
            "effective": effective.get("freshness"),
            "ua": ua.get("status"),
            "ua_freshness": ua.get("freshness"),
            "warnings": len(payload.get("warnings") or []),
            "blocking": len(payload.get("blocking") or []),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_freshness(args: argparse.Namespace) -> int:
    payload = build_codegraph_freshness_artifact(
        root=Path(args.root) if args.root else REPO_ROOT,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        max_age_hours=args.max_age_hours,
        skip_external=args.skip_external,
    )
    _emit_compact_line(
        "codegraph-freshness",
        {
            "workflow_gate": payload.get("workflow_gate"),
            "freshness": payload.get("freshness"),
            "basis": payload.get("freshness_basis"),
            "git_commit": payload.get("git_commit"),
            "warnings": len(payload.get("warnings") or []),
            "summary_ref": payload.get("summary_ref"),
            "artifact_path": payload.get("artifact_path"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_latest_freshness(args: argparse.Namespace) -> int:
    payload = latest_codegraph_freshness(
        root=Path(args.root) if args.root else REPO_ROOT,
        refresh_if_stale=args.refresh_if_stale,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        max_age_hours=args.max_age_hours,
        skip_external=args.skip_external,
    )
    effective = payload.get("effective") if isinstance(payload.get("effective"), dict) else {}
    latest = payload.get("latest") if isinstance(payload.get("latest"), dict) else {}
    _emit_compact_line(
        "latest-freshness",
        {
            "workflow_gate": payload.get("workflow_gate"),
            "effective": effective.get("freshness"),
            "source": payload.get("effective_source"),
            "current_commit": payload.get("current_git_commit"),
            "latest_commit": latest.get("git_commit"),
            "stale_metadata_warning": str(bool(payload.get("stale_metadata_warning"))).lower(),
            "refreshed": str(bool(payload.get("refreshed"))).lower(),
            "artifact_path": effective.get("artifact_path") or latest.get("artifact_path"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_run_manifest(args: argparse.Namespace) -> int:
    payload = build_code_intelligence_run_manifest(
        root=Path(args.root) if args.root else REPO_ROOT,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        artifact_name=args.artifact_name,
        run_id=args.run_id,
        run_url=args.run_url,
        branch=args.branch,
        commit=args.commit,
    )
    _emit(payload, args.output)
    return 0


def cmd_llm_value_summary(args: argparse.Namespace) -> int:
    payload = build_llm_value_summary(
        root=Path(args.root) if args.root else REPO_ROOT,
        artifact_dir=Path(args.artifact_dir) if args.artifact_dir else None,
    )
    if args.output_md:
        _write_text(Path(args.output_md), render_llm_value_summary_markdown(payload))
    _emit_compact_line(
        "llm-value-summary",
        {
            "workflow_gate": payload.get("workflow_gate"),
            "codegraph": (payload.get("codegraph") or {}).get("freshness"),
            "ua": (payload.get("understand_anything") or {}).get("status"),
            "provider": (payload.get("llm") or {}).get("provider"),
            "model": (payload.get("llm") or {}).get("model"),
            "llm_invoked": str(bool((payload.get("llm") or {}).get("llm_invoked"))).lower(),
            "fallback_used": str(bool((payload.get("llm") or {}).get("fallback_used"))).lower(),
            "advice_consumed": str(bool((payload.get("llm") or {}).get("advice_consumed"))).lower(),
            "summary_ref": args.output_md,
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_context(args: argparse.Namespace) -> int:
    payload = build_context_artifacts(
        item_id=args.item_id,
        query=args.query,
        changed_files=list(args.changed_file or []),
        root=Path(args.root) if args.root else REPO_ROOT,
        max_symbols=args.max_symbols,
        skip_external=args.skip_external,
    )
    quality = payload.get("context_quality") if isinstance(payload.get("context_quality"), dict) else {}
    fallback = payload.get("fallback") if isinstance(payload.get("fallback"), dict) else {}
    _emit_compact_line(
        "codegraph-context",
        {
            "status": payload.get("status"),
            "quality": quality.get("quality"),
            "changed_files": len(payload.get("changed_files") or []),
            "context_ref": payload.get("context_markdown"),
            "fallback_used": str(bool(fallback.get("used"))).lower(),
            "scoped_fallback": str(bool(quality.get("scoped_fallback_inserted"))).lower(),
            "graph_root_source": payload.get("graph_root_source"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_affected_tests(args: argparse.Namespace) -> int:
    changed = list(args.changed_file or [])
    if args.changed_files_file:
        changed.extend(nightly_discovery_input_pack.read_changed_files_file(Path(args.changed_files_file)))
    payload = build_affected_tests_artifact(
        item_id=args.item_id,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        root=Path(args.root) if args.root else REPO_ROOT,
        filter_glob=args.filter,
        skip_external=args.skip_external,
    )
    fallback = payload.get("fallback") if isinstance(payload.get("fallback"), dict) else {}
    _emit_compact_line(
        "codegraph-affected-tests",
        {
            "status": payload.get("status"),
            "quality": payload.get("quality"),
            "changed_files": len(payload.get("changed_files") or []),
            "suggested_tests": len(payload.get("suggested_tests") or []),
            "artifact_path": payload.get("artifact_path"),
            "fallback_used": str(bool(fallback.get("used"))).lower(),
            "graph_root_source": payload.get("graph_root_source"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_ua_summary(args: argparse.Namespace) -> int:
    payload = build_understand_anything_summary(
        module=args.module,
        root=Path(args.root) if args.root else REPO_ROOT,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        max_nodes=args.max_nodes,
    )
    _emit_compact_line(
        "ua-summary",
        {
            "status": payload.get("status"),
            "module": payload.get("module"),
            "freshness": payload.get("freshness"),
            "nodes_used": payload.get("nodes_used", 0),
            "edges_used": payload.get("edges_used", 0),
            "summary_ref": payload.get("summary_ref"),
            "artifact_path": payload.get("artifact_path"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_ua_summary_all(args: argparse.Namespace) -> int:
    payload = build_understand_anything_summary_manifest(
        modules=list(args.module or []),
        root=Path(args.root) if args.root else REPO_ROOT,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        max_nodes=args.max_nodes,
    )
    _emit_compact_line(
        "ua-summary-all",
        {
            "workflow_gate": payload.get("workflow_gate"),
            "modules": len(payload.get("modules") or []),
            "fresh": payload.get("fresh_summary_count", 0),
            "base_current": payload.get("base_current_summary_count", 0),
            "stale": payload.get("stale_summary_count", 0),
            "missing": payload.get("missing_summary_count", 0),
            "artifact_path": payload.get("artifact_path"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_ua_configure(args: argparse.Namespace) -> int:
    payload = configure_understand_anything(
        root=Path(args.root) if args.root else REPO_ROOT,
        language=args.language,
        auto_update=args.auto_update,
    )
    _emit(payload, args.output)
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    changed = list(args.changed_file or [])
    if args.changed_files_file:
        changed.extend(nightly_discovery_input_pack.read_changed_files_file(Path(args.changed_files_file)))
    payload = build_summary(
        item_id=args.item_id,
        query=args.query,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        module=args.module,
        root=Path(args.root) if args.root else REPO_ROOT,
        skip_external=args.skip_external,
    )
    if args.output_md:
        _write_text(Path(args.output_md), render_summary_markdown(payload))
    _emit_compact_line(
        "code-intelligence-summary",
        {
            "status": payload.get("status"),
            "latest_freshness": payload.get("latest_freshness"),
            "affected_tests": payload.get("affected_tests_count", 0),
            "context_ref": payload.get("context_ref"),
            "affected_tests_ref": payload.get("affected_tests_ref"),
            "ua_summary_ref": payload.get("understand_anything_summary_ref"),
            "fallback_used": str(bool(payload.get("fallback_used"))).lower(),
            "stale_metadata_warning": str(bool(payload.get("stale_metadata_warning"))).lower(),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def cmd_verify_clients(args: argparse.Namespace) -> int:
    changed = list(args.changed_file or [])
    if args.changed_files_file:
        changed.extend(nightly_discovery_input_pack.read_changed_files_file(Path(args.changed_files_file)))
    payload = build_client_verification(
        item_id=args.item_id,
        query=args.query,
        changed_files=nightly_discovery_input_pack.unique_repo_paths(changed),
        module=args.module,
        root=Path(args.root) if args.root else REPO_ROOT,
        output_dir=Path(args.output_dir) if args.output_dir else None,
        skip_external=args.skip_external,
    )
    if args.output_md:
        _write_text(Path(args.output_md), render_client_verification_summary(payload))
    codegraph = payload.get("codegraph") if isinstance(payload.get("codegraph"), dict) else {}
    freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
    ua = payload.get("understand_anything") if isinstance(payload.get("understand_anything"), dict) else {}
    clients = payload.get("clients") if isinstance(payload.get("clients"), dict) else {}
    ready_clients = sum(1 for item in clients.values() if isinstance(item, dict) and item.get("status") == "ready")
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    _emit_compact_line(
        "verify-clients",
        {
            "workflow_gate": payload.get("workflow_gate"),
            "codegraph": codegraph.get("status"),
            "effective": freshness.get("effective_freshness"),
            "ua": ua.get("status"),
            "ua_freshness": ua.get("freshness"),
            "clients_ready": f"{ready_clients}/{len(clients)}",
            "context_ref": artifacts.get("context_ref"),
            "affected_tests_ref": artifacts.get("affected_tests_ref"),
            "ua_summary_ref": artifacts.get("ua_summary_ref"),
            "artifact_path": payload.get("artifact_path"),
        },
        payload=payload,
        output=args.output,
        output_format=args.output_format,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock CodeGraph / Understand Anything thin adapter.")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Check code intelligence tool availability.")
    doctor.add_argument("--root")
    doctor.add_argument("--skip-external", action="store_true")
    doctor.add_argument("--output")
    doctor.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints readiness status only; full-json emits the complete payload.",
    )
    doctor.set_defaults(func=cmd_doctor)

    freshness = sub.add_parser("freshness", help="Build a warning-only CodeGraph freshness artifact.")
    freshness.add_argument("--root")
    freshness.add_argument("--output-dir")
    freshness.add_argument("--max-age-hours", type=float, default=36.0)
    freshness.add_argument("--skip-external", action="store_true")
    freshness.add_argument("--output")
    freshness.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints status and artifact refs only; full-json emits the complete payload.",
    )
    freshness.set_defaults(func=cmd_freshness)

    latest_freshness = sub.add_parser(
        "latest-freshness",
        help="Read the latest warning-only CodeGraph freshness artifact without invoking CodeGraph.",
    )
    latest_freshness.add_argument("--root")
    latest_freshness.add_argument("--refresh-if-stale", action="store_true")
    latest_freshness.add_argument("--output-dir")
    latest_freshness.add_argument("--max-age-hours", type=float, default=36.0)
    latest_freshness.add_argument("--skip-external", action="store_true")
    latest_freshness.add_argument("--output")
    latest_freshness.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints status and artifact refs only; full-json emits the complete payload.",
    )
    latest_freshness.set_defaults(func=cmd_latest_freshness)

    run_manifest = sub.add_parser(
        "run-manifest",
        help="Build a compact manifest for uploaded CodeGraph / Understand Anything CI artifacts.",
    )
    run_manifest.add_argument("--root")
    run_manifest.add_argument("--output-dir")
    run_manifest.add_argument("--artifact-name")
    run_manifest.add_argument("--run-id")
    run_manifest.add_argument("--run-url")
    run_manifest.add_argument("--branch")
    run_manifest.add_argument("--commit")
    run_manifest.add_argument("--output")
    run_manifest.set_defaults(func=cmd_run_manifest)

    llm_value = sub.add_parser(
        "llm-value-summary",
        help="Build a compact human-readable summary of Nightly LLM and code-intelligence value evidence.",
    )
    llm_value.add_argument("--root")
    llm_value.add_argument("--artifact-dir")
    llm_value.add_argument("--output")
    llm_value.add_argument("--output-md")
    llm_value.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints value status only; full-json emits the complete payload.",
    )
    llm_value.set_defaults(func=cmd_llm_value_summary)

    context = sub.add_parser("context", help="Build a CodeGraph-backed context artifact.")
    context.add_argument("--item-id", required=True)
    context.add_argument("--query", required=True)
    context.add_argument("--changed-file", action="append")
    context.add_argument("--root")
    context.add_argument("--max-symbols", type=int, default=12)
    context.add_argument("--skip-external", action="store_true")
    context.add_argument("--output")
    context.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints context refs only; full-json emits the complete payload.",
    )
    context.set_defaults(func=cmd_context)

    affected = sub.add_parser("affected-tests", help="Build a CodeGraph affected-tests artifact.")
    affected.add_argument("--item-id", required=True)
    affected.add_argument("--changed-file", action="append")
    affected.add_argument("--changed-files-file")
    affected.add_argument("--filter")
    affected.add_argument("--root")
    affected.add_argument("--skip-external", action="store_true")
    affected.add_argument("--output")
    affected.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints affected-test counts and refs only; full-json emits the complete payload.",
    )
    affected.set_defaults(func=cmd_affected_tests)

    ua_summary = sub.add_parser("ua-summary", help="Build a read-only Understand Anything graph summary artifact.")
    ua_summary.add_argument("--module", required=True)
    ua_summary.add_argument("--root")
    ua_summary.add_argument("--output-dir")
    ua_summary.add_argument("--max-nodes", type=int)
    ua_summary.add_argument("--output")
    ua_summary.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints status and artifact refs only; full-json emits the complete payload.",
    )
    ua_summary.set_defaults(func=cmd_ua_summary)

    ua_summary_all = sub.add_parser("ua-summary-all", help="Build read-only Understand Anything summaries for standard AIstock modules.")
    ua_summary_all.add_argument("--module", action="append")
    ua_summary_all.add_argument("--root")
    ua_summary_all.add_argument("--output-dir")
    ua_summary_all.add_argument("--max-nodes", type=int)
    ua_summary_all.add_argument("--output")
    ua_summary_all.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints counts and artifact refs only; full-json emits the complete manifest.",
    )
    ua_summary_all.set_defaults(func=cmd_ua_summary_all)

    ua_configure = sub.add_parser(
        "ua-configure",
        help="Create local Understand Anything config/ignore files; graph generation still runs through the UA client skill.",
    )
    ua_configure.add_argument("--root")
    ua_configure.add_argument("--language", default="zh")
    ua_configure.add_argument("--auto-update", action="store_true")
    ua_configure.add_argument("--output")
    ua_configure.set_defaults(func=cmd_ua_configure)

    summary = sub.add_parser("summary", help="Build context and affected-tests artifacts together.")
    summary.add_argument("--item-id", required=True)
    summary.add_argument("--query", required=True)
    summary.add_argument("--changed-file", action="append")
    summary.add_argument("--changed-files-file")
    summary.add_argument("--module")
    summary.add_argument("--root")
    summary.add_argument("--skip-external", action="store_true")
    summary.add_argument("--output")
    summary.add_argument("--output-md")
    summary.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints summary refs only; full-json emits the complete payload.",
    )
    summary.set_defaults(func=cmd_summary)

    verify_clients = sub.add_parser(
        "verify-clients",
        help="Build compact CodeGraph/Understand Anything/client readiness evidence for Codex and Claude Code.",
    )
    verify_clients.add_argument("--item-id", default="VERIFY-CODE-INTELLIGENCE")
    verify_clients.add_argument("--query", default="AIstock code intelligence workflow verification")
    verify_clients.add_argument("--changed-file", action="append")
    verify_clients.add_argument("--changed-files-file")
    verify_clients.add_argument("--module", default="validation")
    verify_clients.add_argument("--root")
    verify_clients.add_argument("--output-dir")
    verify_clients.add_argument("--skip-external", action="store_true")
    verify_clients.add_argument("--output")
    verify_clients.add_argument("--output-md")
    verify_clients.add_argument(
        "--output-format",
        choices=("compact", "full-json"),
        default="compact",
        help="Stdout format. Compact prints readiness status and artifact refs only; full-json emits the complete payload.",
    )
    verify_clients.set_defaults(func=cmd_verify_clients)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except CodeIntelligenceError as exc:
        _emit({"schema_version": "aistock_code_intelligence_error_v1", "error": str(exc), "workflow_gate": "blocked"})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
