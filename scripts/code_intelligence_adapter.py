from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = Path("tmp") / "issue_workflow"
DEFAULT_CODEGRAPH_VERSION = "0.9.4"
DEFAULT_UNDERSTAND_ANYTHING_VERSION = "v2.7.3"
CATALOG_PATH = REPO_ROOT / "tests" / "aistock_validation" / "catalog" / "code_intelligence.yaml"


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
    if output:
        _write_json(Path(output), payload)
    sys.stdout.write(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


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


def _git(args: list[str], cwd: Path | None = None, check: bool = False) -> dict[str, Any]:
    return _run_command(["git", *args], cwd=cwd, timeout=30)


def _git_text(args: list[str], cwd: Path | None = None) -> str:
    result = _git(args, cwd=cwd)
    if not result.get("ok"):
        raise CodeIntelligenceError(result.get("stderr") or result.get("stdout") or f"git {' '.join(args)} failed")
    return str(result.get("stdout") or "").strip()


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


def _codegraph_command() -> str | None:
    override = os.environ.get("AISTOCK_CODEGRAPH_BIN")
    if override:
        return override
    return shutil.which("codegraph")


def _understand_graph_path(root: Path) -> Path:
    return root / ".understand-anything" / "knowledge-graph.json"


def codegraph_status(root: Path | None = None, *, skip_external: bool = False) -> dict[str, Any]:
    root = root or REPO_ROOT
    command = _codegraph_command()
    available = bool(command)
    version_result: dict[str, Any] = {"ok": False, "skipped": skip_external or not available}
    status_result: dict[str, Any] = {"ok": False, "skipped": skip_external or not available}
    version = None
    if available and not skip_external:
        version_result = _run_command([command, "--version"], cwd=root, timeout=20)
        version = _parse_codegraph_version(str(version_result.get("stdout") or version_result.get("stderr") or ""))
        status_result = _run_command([command, "status", str(root)], cwd=root, timeout=30)
    index_path = root / ".codegraph" / "codegraph.db"
    git = _git_snapshot(root)
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
        "version_check": version_result,
        "index_path": _repo_rel(index_path, root),
        "index_exists": index_path.exists(),
        "status_check": status_result,
        "git_commit": git.get("head"),
        "working_tree_dirty": git.get("dirty"),
        "channel": "mcp_or_cli",
    }


def understand_anything_status(root: Path | None = None) -> dict[str, Any]:
    root = root or REPO_ROOT
    graph_path = _understand_graph_path(root)
    manifest: dict[str, Any] = {}
    if graph_path.exists():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8-sig"))
            manifest = {
                "node_count": len(graph.get("nodes") or []),
                "edge_count": len(graph.get("edges") or []),
                "project": graph.get("project") or {},
            }
        except Exception as exc:
            manifest = {"read_error": str(exc)}
    return {
        "provider": "understand_anything",
        "enabled": True,
        "status": "available" if graph_path.exists() else "not_required_missing",
        "expected_version": DEFAULT_UNDERSTAND_ANYTHING_VERSION,
        "graph_path": _repo_rel(graph_path, root),
        "graph_exists": graph_path.exists(),
        "auto_update_required": False,
        "blocking_for_issue_workflow": False,
        "manifest": manifest,
    }


def build_doctor_report(root: Path | None = None, *, skip_external: bool = False) -> dict[str, Any]:
    root = root or REPO_ROOT
    catalog = _load_catalog(root)
    codegraph = codegraph_status(root, skip_external=skip_external)
    ua = understand_anything_status(root)
    warnings: list[str] = []
    if not codegraph.get("available"):
        warnings.append("CodeGraph CLI is unavailable; issue workflow will fall back to existing rg/catalog context.")
    elif not codegraph.get("index_exists"):
        warnings.append("CodeGraph index is missing; run codegraph init -i when code intelligence context is needed.")
    if not ua.get("graph_exists"):
        warnings.append("Understand Anything graph is missing; this is non-blocking for normal issue workflow.")
    return {
        "schema_version": "aistock_code_intelligence_doctor_v1",
        "generated_at": _utc_now(),
        "workflow_gate": "warning" if warnings else "ready",
        "warnings": warnings,
        "blocking": [],
        "repo_root": str(root),
        "catalog": catalog,
        "codegraph": codegraph,
        "understand_anything": ua,
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
    changed = [path for path in changed_files or [] if path]
    output_dir = _workflow_dir(item_id, root)
    context_path = output_dir / "codegraph-context.md"
    manifest_path = output_dir / "code-intelligence.json"
    status = codegraph_status(root, skip_external=skip_external)
    fallback_used = True
    fallback_reason = "codegraph_unavailable_or_missing_index"
    context_text = _fallback_context(query, changed)
    if status.get("available") and status.get("index_exists") and not skip_external:
        command = str(status["command"])
        result = _run_command([command, "context", query or " ", "--max-nodes", str(max_symbols)], cwd=root, timeout=60)
        if result.get("ok") and (result.get("stdout") or "").strip():
            fallback_used = False
            fallback_reason = None
            context_text = str(result.get("stdout") or "")
        else:
            fallback_reason = result.get("stderr") or result.get("stdout") or "codegraph_context_failed"
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
        "status": "fallback" if fallback_used else "ok",
        "context_markdown": _repo_rel(context_path, root),
        "fallback": {"used": fallback_used, "reason": fallback_reason},
        "channel": "cli" if not fallback_used else "fallback",
    }
    _write_json(manifest_path, manifest)
    return {**manifest, "manifest_path": _repo_rel(manifest_path, root)}


def build_affected_tests_artifact(
    *,
    item_id: str,
    changed_files: list[str] | None = None,
    root: Path | None = None,
    filter_glob: str | None = None,
    skip_external: bool = False,
) -> dict[str, Any]:
    root = root or REPO_ROOT
    changed = [path for path in changed_files or [] if path]
    output_dir = _workflow_dir(item_id, root)
    out_path = output_dir / "affected-tests.json"
    status = codegraph_status(root, skip_external=skip_external)
    suggested: list[str] = []
    fallback_used = True
    fallback_reason = "codegraph_unavailable_or_missing_index"
    if changed and status.get("available") and status.get("index_exists") and not skip_external:
        command = str(status["command"])
        args = [command, "affected", *changed, "--quiet"]
        if filter_glob:
            args.extend(["--filter", filter_glob])
        result = _run_command(args, cwd=root, timeout=60)
        if result.get("ok"):
            suggested = [line.strip() for line in str(result.get("stdout") or "").splitlines() if line.strip()]
            fallback_used = False
            fallback_reason = None
        else:
            fallback_reason = result.get("stderr") or result.get("stdout") or "codegraph_affected_failed"
    payload = {
        "schema_version": "aistock_codegraph_affected_tests_v1",
        "generated_at": _utc_now(),
        "tool": "codegraph",
        "tool_version": status.get("version") or status.get("expected_version"),
        "changed_files": changed,
        "suggested_tests": suggested,
        "filter": filter_glob,
        "status": "fallback" if fallback_used else "ok",
        "fallback": {"used": fallback_used, "reason": fallback_reason},
        "source": "codegraph affected" if not fallback_used else "aistock fallback",
    }
    _write_json(out_path, payload)
    return {**payload, "artifact_path": _repo_rel(out_path, root)}


def build_summary(
    *,
    item_id: str,
    query: str,
    changed_files: list[str] | None = None,
    root: Path | None = None,
    skip_external: bool = False,
) -> dict[str, Any]:
    context = build_context_artifacts(
        item_id=item_id,
        query=query,
        changed_files=changed_files,
        root=root,
        skip_external=skip_external,
    )
    affected = build_affected_tests_artifact(
        item_id=item_id,
        changed_files=changed_files,
        root=root,
        skip_external=skip_external,
    )
    return {
        "schema_version": "aistock_code_intelligence_summary_v1",
        "generated_at": _utc_now(),
        "item_id": item_id,
        "provider": "codegraph",
        "status": "ok" if context["status"] == "ok" or affected["status"] == "ok" else "fallback",
        "context_ref": context.get("context_markdown"),
        "manifest_ref": context.get("manifest_path"),
        "affected_tests_ref": affected.get("artifact_path"),
        "fallback_used": bool(context.get("fallback", {}).get("used") and affected.get("fallback", {}).get("used")),
        "context": context,
        "affected_tests": affected,
        "understand_anything": understand_anything_status(root or REPO_ROOT),
    }


def _inline(items: list[Any] | tuple[Any, ...] | None, *, default: str = "none") -> str:
    values = [str(item) for item in items or [] if str(item).strip()]
    return ", ".join(values) if values else default


def render_summary_markdown(payload: dict[str, Any]) -> str:
    context = payload.get("context") or {}
    affected = payload.get("affected_tests") or {}
    context_fallback = context.get("fallback") or {}
    affected_fallback = affected.get("fallback") or {}
    ua = payload.get("understand_anything") or {}
    suggested_tests = [str(item) for item in affected.get("suggested_tests") or [] if str(item).strip()]
    warnings = []
    if context_fallback.get("used"):
        warnings.append(f"context fallback: {context_fallback.get('reason') or 'unknown'}")
    if affected_fallback.get("used"):
        warnings.append(f"affected-tests fallback: {affected_fallback.get('reason') or 'unknown'}")
    lines = [
        "## Code Intelligence Summary",
        "",
        f"- provider: `{payload.get('provider') or 'codegraph'}`",
        f"- status: `{payload.get('status') or 'unknown'}`",
        f"- fallback_used: `{str(bool(payload.get('fallback_used'))).lower()}`",
        f"- context_ref: `{payload.get('context_ref') or 'not_generated'}`",
        f"- affected_tests_ref: `{payload.get('affected_tests_ref') or 'not_generated'}`",
        f"- changed_files: `{_inline(affected.get('changed_files') or context.get('changed_files'))}`",
        f"- understand_anything_status: `{ua.get('status') or 'unknown'}`",
        "",
        "### Suggested Impacted Tests",
        *[f"- `{item}`" for item in suggested_tests or ["none"]],
    ]
    if warnings:
        lines.extend(["", "### Warnings", *[f"- {item}" for item in warnings]])
    lines.extend([
        "",
        "Code intelligence is warning-only. Final merge readiness still depends on AIstock nox, pytest, Validation Center, and production gates.",
        "",
    ])
    return "\n".join(lines)


def cmd_doctor(args: argparse.Namespace) -> int:
    _emit(build_doctor_report(Path(args.root) if args.root else REPO_ROOT, skip_external=args.skip_external), args.output)
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
    _emit(payload, args.output)
    return 0


def cmd_affected_tests(args: argparse.Namespace) -> int:
    changed = list(args.changed_file or [])
    if args.changed_files_file:
        changed.extend(Path(args.changed_files_file).read_text(encoding="utf-8").splitlines())
    payload = build_affected_tests_artifact(
        item_id=args.item_id,
        changed_files=changed,
        root=Path(args.root) if args.root else REPO_ROOT,
        filter_glob=args.filter,
        skip_external=args.skip_external,
    )
    _emit(payload, args.output)
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    changed = list(args.changed_file or [])
    if args.changed_files_file:
        changed.extend(Path(args.changed_files_file).read_text(encoding="utf-8").splitlines())
    payload = build_summary(
        item_id=args.item_id,
        query=args.query,
        changed_files=changed,
        root=Path(args.root) if args.root else REPO_ROOT,
        skip_external=args.skip_external,
    )
    if args.output_md:
        _write_text(Path(args.output_md), render_summary_markdown(payload))
    _emit(payload, args.output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AIstock CodeGraph / Understand Anything thin adapter.")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Check code intelligence tool availability.")
    doctor.add_argument("--root")
    doctor.add_argument("--skip-external", action="store_true")
    doctor.add_argument("--output")
    doctor.set_defaults(func=cmd_doctor)

    context = sub.add_parser("context", help="Build a CodeGraph-backed context artifact.")
    context.add_argument("--item-id", required=True)
    context.add_argument("--query", required=True)
    context.add_argument("--changed-file", action="append")
    context.add_argument("--root")
    context.add_argument("--max-symbols", type=int, default=12)
    context.add_argument("--skip-external", action="store_true")
    context.add_argument("--output")
    context.set_defaults(func=cmd_context)

    affected = sub.add_parser("affected-tests", help="Build a CodeGraph affected-tests artifact.")
    affected.add_argument("--item-id", required=True)
    affected.add_argument("--changed-file", action="append")
    affected.add_argument("--changed-files-file")
    affected.add_argument("--filter")
    affected.add_argument("--root")
    affected.add_argument("--skip-external", action="store_true")
    affected.add_argument("--output")
    affected.set_defaults(func=cmd_affected_tests)

    summary = sub.add_parser("summary", help="Build context and affected-tests artifacts together.")
    summary.add_argument("--item-id", required=True)
    summary.add_argument("--query", required=True)
    summary.add_argument("--changed-file", action="append")
    summary.add_argument("--changed-files-file")
    summary.add_argument("--root")
    summary.add_argument("--skip-external", action="store_true")
    summary.add_argument("--output")
    summary.add_argument("--output-md")
    summary.set_defaults(func=cmd_summary)
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
