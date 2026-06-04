"""AIstock MCP gateway configuration and static safety doctor."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from backend.mcp.common import assert_loopback_url
from backend.mcp.gateway import list_profiles_payload, self_check_payload

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_MCP = REPO_ROOT / ".mcp.json"
LEGACY_STANDALONE_SCRIPTS = {
    "scripts/aistock_mcp_server.py",
    "scripts/aistock_qe_experiment_mcp_server.py",
    "scripts/aistock_qe_archive_mcp_server.py",
}
DEFAULT_GATEWAY_SERVER = "aistock-gateway-lite"
BANNED_LLM_PATTERNS = (
    re.compile(r"subprocess\.(?:run|Popen|call|check_call|check_output)\([^\n]*(?:claude|codex|bun)", re.I),
    re.compile(r"Start-Process[^\n]*(?:claude|codex|bun)", re.I),
    re.compile(r"stream-json", re.I),
    re.compile(r"worker-service\\.cjs", re.I),
)


def _git(args: list[str]) -> str:
    result = subprocess.run(["git", *args], cwd=REPO_ROOT, text=True, capture_output=True, check=True)
    return result.stdout.strip()


def _load_project_mcp() -> dict[str, Any]:
    return json.loads(PROJECT_MCP.read_text(encoding="utf-8-sig"))


def _normalize_arg_path(value: str) -> str:
    return value.replace("\\", "/")


def _check_project_mcp() -> tuple[list[dict[str, Any]], list[str], list[str]]:
    servers = _load_project_mcp().get("mcpServers") or {}
    details: list[dict[str, Any]] = []
    errors: list[str] = []
    warnings: list[str] = []
    if DEFAULT_GATEWAY_SERVER not in servers:
        errors.append(f".mcp.json must register {DEFAULT_GATEWAY_SERVER}")
    for name, spec in sorted(servers.items()):
        args = [_normalize_arg_path(str(arg)) for arg in spec.get("args") or []]
        env = spec.get("env") or {}
        legacy_args = [arg for arg in args if arg in LEGACY_STANDALONE_SCRIPTS]
        if legacy_args:
            errors.append(f"{name} still points at legacy standalone MCP script: {legacy_args}")
        if not any(arg.endswith("scripts/aistock_mcp_gateway.py") for arg in args):
            errors.append(f"{name} does not use scripts/aistock_mcp_gateway.py")
        profile = next((arg.split("=", 1)[1] for arg in args if arg.startswith("--profile=")), None)
        if profile == "full":
            errors.append(f"{name} uses full profile; full must remain controlled-debug only")
        base_url = env.get("AISTOCK_MCP_BASE_URL")
        if base_url:
            try:
                assert_loopback_url(base_url, env_name=f"{name}.AISTOCK_MCP_BASE_URL")
            except ValueError as exc:
                errors.append(str(exc))
        details.append({"server": name, "profile": profile, "args": args, "base_url": base_url})
    return details, errors, warnings


def _project_mcp_guardrail(details: list[dict[str, Any]]) -> dict[str, Any]:
    legacy_servers = [
        item["server"]
        for item in details
        if any(arg in LEGACY_STANDALONE_SCRIPTS for arg in item.get("args") or [])
    ]
    full_profile_servers = [item["server"] for item in details if item.get("profile") == "full"]
    gateway_servers = [
        item["server"]
        for item in details
        if any(str(arg).endswith("scripts/aistock_mcp_gateway.py") for arg in item.get("args") or [])
    ]
    default_detail = next((item for item in details if item["server"] == DEFAULT_GATEWAY_SERVER), {})
    default_profile = default_detail.get("profile")
    return {
        "status": "pass" if default_profile == "lite" and not legacy_servers and not full_profile_servers else "fail",
        "default_server": DEFAULT_GATEWAY_SERVER,
        "default_profile": default_profile,
        "registered_server_count": len(details),
        "gateway_server_count": len(gateway_servers),
        "task_profile_servers": [item["server"] for item in details if item["server"] != DEFAULT_GATEWAY_SERVER],
        "legacy_standalone_servers": legacy_servers,
        "legacy_standalone_scripts": sorted(LEGACY_STANDALONE_SCRIPTS),
        "full_profile_servers": full_profile_servers,
        "new_client_session_required_for_tool_injection": True,
        "evidence_ref": ".mcp.json",
    }


def _check_static_no_llm() -> tuple[list[dict[str, str]], list[str], dict[str, Any]]:
    scanned_roots = [REPO_ROOT / "backend" / "mcp", REPO_ROOT / "scripts" / "aistock_mcp_gateway.py"]
    findings: list[dict[str, str]] = []
    errors: list[str] = []
    files: list[Path] = []
    for root in scanned_roots:
        if root.is_dir():
            files.extend(path for path in root.rglob("*.py") if path.is_file())
        elif root.is_file():
            files.append(root)
    for path in files:
        rel = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for pattern in BANNED_LLM_PATTERNS:
            match = pattern.search(text)
            if match:
                finding = {"path": rel, "pattern": pattern.pattern, "match": match.group(0)[:160]}
                findings.append(finding)
                errors.append(f"banned LLM/daemon launch pattern in {rel}: {pattern.pattern}")
    evidence = {
        "status": "pass" if not findings else "fail",
        "scanned_roots": [root.relative_to(REPO_ROOT).as_posix() if root.is_relative_to(REPO_ROOT) else str(root) for root in scanned_roots],
        "scanned_file_count": len(files),
        "banned_pattern_count": len(BANNED_LLM_PATTERNS),
        "finding_count": len(findings),
        "forbidden_process_families": ["claude", "codex", "bun", "stream-json", "worker-service.cjs"],
    }
    return findings, errors, evidence


def run_doctor(*, check_backend: bool = False) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        branch = _git(["branch", "--show-current"])
        head = _git(["rev-parse", "--short", "HEAD"])
        root = _git(["rev-parse", "--show-toplevel"]).replace("\\", "/")
    except Exception as exc:  # pragma: no cover - git should exist in repo validation
        branch = None
        head = None
        root = str(REPO_ROOT)
        errors.append(f"git metadata unavailable: {exc}")

    project_servers, project_errors, project_warnings = _check_project_mcp()
    static_findings, static_errors, static_guardrail = _check_static_no_llm()
    gateway = self_check_payload(profile="lite", check_backend=check_backend)
    profiles = list_profiles_payload()
    project_guardrail = _project_mcp_guardrail(project_servers)

    errors.extend(project_errors)
    errors.extend(static_errors)
    if gateway.get("status") != "pass":
        errors.extend(str(item) for item in gateway.get("errors") or [])
    warnings.extend(project_warnings)
    warnings.extend(gateway.get("warnings") or [])
    if "AIstock_worktrees" in root and not branch:
        warnings.append("detached worktree detected; verify client configs do not point at stale worktrees")

    return {
        "status": "fail" if errors else "pass",
        "repo": {"root": root, "branch": branch, "head": head},
        "project_mcp": {"path": PROJECT_MCP.relative_to(REPO_ROOT).as_posix(), "servers": project_servers},
        "gateway_lite": gateway,
        "profiles": profiles,
        "static_no_llm": {"findings": static_findings, **static_guardrail},
        "guardrails": {
            "standalone_default_retirement": project_guardrail,
            "no_background_llm_daemon": static_guardrail,
        },
        "errors": errors,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check AIstock MCP gateway readiness")
    parser.add_argument("--check-backend", action="store_true", help="Also attempt a short backend health request")
    parser.add_argument("--json", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()
    payload = run_doctor(check_backend=args.check_backend)
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.json else None))
    raise SystemExit(0 if payload["status"] == "pass" else 2)


if __name__ == "__main__":
    main()
