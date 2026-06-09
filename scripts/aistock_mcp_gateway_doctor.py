"""AIstock MCP gateway configuration and static safety doctor."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
import tomllib

if __package__ in {None, ""}:
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from backend.mcp.common import assert_loopback_url
from backend.mcp.gateway import list_profiles_payload, self_check_payload

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_MCP = REPO_ROOT / ".mcp.json"
USER_CODEX_CONFIG = Path.home() / ".codex" / "config.toml"
USER_CLAUDE_MCP_CONFIG = Path.home() / ".mcp.json"
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
TOKEN_RISK_PROCESS_PATTERNS = (
    re.compile(r"(?:^|[\\/\\s])claude(?:\.cmd|\.exe)?(?:\s|$)", re.I),
    re.compile(r"(?:^|[\\/\\s])codex(?:\.exe|\.js)?(?:\s|$)", re.I),
    re.compile(r"(?:^|[\\/\\s])bun(?:\.exe)?(?:\s|$)", re.I),
    re.compile(r"worker-service\.cjs", re.I),
    re.compile(r"stream-json", re.I),
)


def _git(args: list[str]) -> str:
    result = subprocess.run(["git", *args], cwd=REPO_ROOT, text=True, capture_output=True, check=True)
    return result.stdout.strip()


def _load_project_mcp() -> dict[str, Any]:
    return json.loads(PROJECT_MCP.read_text(encoding="utf-8-sig"))


def _normalize_arg_path(value: str) -> str:
    return value.replace("\\", "/")


def _arg_endswith_any(args: list[str], suffixes: set[str]) -> list[str]:
    return [arg for arg in args if any(arg.endswith(suffix) for suffix in suffixes)]


def _profile_from_args(args: list[str]) -> str | None:
    for index, arg in enumerate(args):
        if arg.startswith("--profile="):
            return arg.split("=", 1)[1]
        if arg == "--profile" and index + 1 < len(args):
            return args[index + 1]
    return None


def _modules_from_args(args: list[str]) -> str | None:
    for index, arg in enumerate(args):
        if arg.startswith("--modules="):
            return arg.split("=", 1)[1]
        if arg == "--modules" and index + 1 < len(args):
            return args[index + 1]
    return None


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
        legacy_args = _arg_endswith_any(args, LEGACY_STANDALONE_SCRIPTS)
        if legacy_args:
            errors.append(f"{name} still points at legacy standalone MCP script: {legacy_args}")
        if not any(arg.endswith("scripts/aistock_mcp_gateway.py") for arg in args):
            errors.append(f"{name} does not use scripts/aistock_mcp_gateway.py")
        profile = _profile_from_args(args)
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
        if _arg_endswith_any([str(arg) for arg in item.get("args") or []], LEGACY_STANDALONE_SCRIPTS)
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


def _looks_like_aistock_mcp_server(name: str, spec: dict[str, Any]) -> bool:
    text = " ".join(
        [
            name,
            str(spec.get("command") or ""),
            str(spec.get("cwd") or ""),
            " ".join(str(arg) for arg in spec.get("args") or []),
        ]
    ).lower()
    return name.lower().startswith("aistock") or name == "research-assistant" or "aistock" in text


def _scan_codex_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "status": "missing", "servers": [], "findings": [], "finding_count": 0}

    findings: list[dict[str, Any]] = []
    servers: list[dict[str, Any]] = []
    try:
        data = tomllib.loads(path.read_bytes().decode("utf-8-sig"))
    except Exception as exc:
        return {
            "path": str(path),
            "exists": True,
            "status": "warn",
            "servers": [],
            "findings": [{"severity": "warning", "code": "client_config_parse_failed", "message": str(exc)}],
            "finding_count": 1,
        }

    for name, spec in sorted((data.get("mcp_servers") or {}).items()):
        if not isinstance(spec, dict):
            continue
        if not _looks_like_aistock_mcp_server(name, spec):
            continue
        args = [_normalize_arg_path(str(arg)) for arg in spec.get("args") or []]
        command = _normalize_arg_path(str(spec.get("command") or ""))
        env = spec.get("env") or {}
        profile = _profile_from_args(args)
        modules = _modules_from_args(args)
        enabled = bool(spec.get("enabled", True))
        legacy_args = _arg_endswith_any(args, LEGACY_STANDALONE_SCRIPTS)
        uses_gateway = any(arg.endswith("scripts/aistock_mcp_gateway.py") for arg in args)
        detail = {
            "server": name,
            "enabled": enabled,
            "command": command,
            "args": args,
            "profile": profile,
            "modules": modules,
            "uses_gateway": uses_gateway,
            "legacy_args": legacy_args,
        }
        servers.append(detail)
        if not enabled:
            continue
        if legacy_args:
            findings.append(
                {
                    "severity": "warning",
                    "code": "legacy_standalone_mcp_config",
                    "server": name,
                    "message": f"{name} still points at legacy standalone MCP script: {legacy_args}",
                }
            )
        elif not uses_gateway:
            findings.append(
                {
                    "severity": "warning",
                    "code": "aistock_mcp_not_gateway",
                    "server": name,
                    "message": f"{name} is AIstock-related but does not use scripts/aistock_mcp_gateway.py",
                }
            )
        if profile == "full":
            findings.append(
                {
                    "severity": "error",
                    "code": "full_profile_client_config",
                    "server": name,
                    "message": f"{name} uses full profile; full must remain controlled-debug only",
                }
            )
        if modules:
            findings.append(
                {
                    "severity": "warning",
                    "code": "modules_arg_client_config",
                    "server": name,
                    "message": f"{name} uses --modules={modules}; prefer canonical --profile entries for client configs",
                }
            )
        for env_name in ("AISTOCK_MCP_BASE_URL", "AISTOCK_VALIDATION_BASE_URL", "AISTOCK_QE_EXPERIMENT_BASE_URL", "AISTOCK_QE_ARCHIVE_BASE_URL"):
            base_url = env.get(env_name)
            if not base_url:
                continue
            try:
                assert_loopback_url(str(base_url), env_name=f"{name}.{env_name}")
            except ValueError as exc:
                findings.append({"severity": "error", "code": "non_loopback_client_base_url", "server": name, "message": str(exc)})

    return {
        "path": str(path),
        "exists": True,
        "status": "warn" if findings else "pass",
        "servers": servers,
        "aistock_server_count": len(servers),
        "findings": findings,
        "finding_count": len(findings),
    }


def _scan_json_mcp_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False, "status": "missing", "servers": [], "findings": [], "finding_count": 0}

    findings: list[dict[str, Any]] = []
    servers: list[dict[str, Any]] = []
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {
            "path": str(path),
            "exists": True,
            "status": "warn",
            "servers": [],
            "findings": [{"severity": "warning", "code": "client_config_parse_failed", "message": str(exc)}],
            "finding_count": 1,
        }

    for name, spec in sorted((data.get("mcpServers") or {}).items()):
        if not isinstance(spec, dict) or not _looks_like_aistock_mcp_server(name, spec):
            continue
        args = [_normalize_arg_path(str(arg)) for arg in spec.get("args") or []]
        env = spec.get("env") or {}
        profile = _profile_from_args(args)
        modules = _modules_from_args(args)
        legacy_args = _arg_endswith_any(args, LEGACY_STANDALONE_SCRIPTS)
        uses_gateway = any(arg.endswith("scripts/aistock_mcp_gateway.py") for arg in args)
        detail = {
            "server": name,
            "command": _normalize_arg_path(str(spec.get("command") or "")),
            "args": args,
            "profile": profile,
            "modules": modules,
            "uses_gateway": uses_gateway,
            "legacy_args": legacy_args,
        }
        servers.append(detail)
        if legacy_args:
            findings.append(
                {
                    "severity": "warning",
                    "code": "legacy_standalone_mcp_config",
                    "server": name,
                    "message": f"{name} still points at legacy standalone MCP script: {legacy_args}",
                }
            )
        elif not uses_gateway:
            findings.append(
                {
                    "severity": "warning",
                    "code": "aistock_mcp_not_gateway",
                    "server": name,
                    "message": f"{name} is AIstock-related but does not use scripts/aistock_mcp_gateway.py",
                }
            )
        if profile == "full":
            findings.append(
                {
                    "severity": "error",
                    "code": "full_profile_client_config",
                    "server": name,
                    "message": f"{name} uses full profile; full must remain controlled-debug only",
                }
            )
        if modules:
            findings.append(
                {
                    "severity": "warning",
                    "code": "modules_arg_client_config",
                    "server": name,
                    "message": f"{name} uses --modules={modules}; prefer canonical --profile entries for client configs",
                }
            )
        for env_name in ("AISTOCK_MCP_BASE_URL", "AISTOCK_VALIDATION_BASE_URL", "AISTOCK_QE_EXPERIMENT_BASE_URL", "AISTOCK_QE_ARCHIVE_BASE_URL"):
            base_url = env.get(env_name)
            if not base_url:
                continue
            try:
                assert_loopback_url(str(base_url), env_name=f"{name}.{env_name}")
            except ValueError as exc:
                findings.append({"severity": "error", "code": "non_loopback_client_base_url", "server": name, "message": str(exc)})

    return {
        "path": str(path),
        "exists": True,
        "status": "warn" if findings else "pass",
        "servers": servers,
        "aistock_server_count": len(servers),
        "findings": findings,
        "finding_count": len(findings),
    }


def _scan_client_config(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".json":
        return _scan_json_mcp_config(path)
    return _scan_codex_config(path)


def _scan_client_configs(paths: list[Path]) -> dict[str, Any]:
    configs = [_scan_client_config(path) for path in paths]
    findings = [finding for config in configs for finding in config.get("findings") or []]
    return {
        "status": "warn" if findings else "pass",
        "configs": configs,
        "finding_count": len(findings),
        "findings": findings,
    }


def classify_process_record(record: dict[str, Any]) -> dict[str, Any]:
    name = str(record.get("name") or record.get("Name") or "")
    command_line = str(record.get("command_line") or record.get("CommandLine") or "")
    lower = f"{name} {command_line}".lower().replace("\\", "/")
    category = "ignored"
    severity = "info"
    profile = None
    modules = None
    token_risk = False
    legacy_scripts = [script for script in LEGACY_STANDALONE_SCRIPTS if script.lower() in lower]

    if legacy_scripts:
        category = "legacy_standalone_mcp"
        severity = "warning"
    elif "scripts/aistock_mcp_gateway.py" in lower:
        category = "gateway_mcp"
        profile_match = re.search(r"--profile(?:=|\s+)([^\s]+)", command_line)
        modules_match = re.search(r"--modules(?:=|\s+)([^\s]+)", command_line)
        profile = profile_match.group(1) if profile_match else None
        modules = modules_match.group(1) if modules_match else None
        if profile == "full":
            category = "full_profile_gateway"
            severity = "error"
    elif any(pattern.search(lower) for pattern in TOKEN_RISK_PROCESS_PATTERNS):
        category = "llm_or_daemon_token_risk"
        severity = "warning"
        token_risk = True

    return {
        "pid": record.get("pid") or record.get("ProcessId"),
        "ppid": record.get("ppid") or record.get("ParentProcessId"),
        "name": name,
        "category": category,
        "severity": severity,
        "profile": profile,
        "modules": modules,
        "token_risk": token_risk,
        "command_line": command_line,
    }


def _collect_process_records() -> list[dict[str, Any]]:
    if os.name == "nt":
        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            "Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,Name,CommandLine | ConvertTo-Json -Depth 3 -Compress",
        ]
        result = subprocess.run(command, text=True, capture_output=True, check=True, timeout=15)
        if not result.stdout.strip():
            return []
        payload = json.loads(result.stdout)
        return payload if isinstance(payload, list) else [payload]

    result = subprocess.run(["ps", "-eo", "pid=,ppid=,comm=,args="], text=True, capture_output=True, check=True, timeout=15)
    records: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) < 4:
            continue
        records.append({"ProcessId": int(parts[0]), "ParentProcessId": int(parts[1]), "Name": parts[2], "CommandLine": parts[3]})
    return records


def process_inventory_payload(records: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    raw_records = _collect_process_records() if records is None else records
    current_pid = os.getpid()
    classified = [classify_process_record(record) for record in raw_records]
    relevant = [item for item in classified if item["category"] != "ignored" and item["pid"] != current_pid]
    counts: dict[str, int] = {}
    for item in relevant:
        counts[item["category"]] = counts.get(item["category"], 0) + 1
    findings = [
        {
            "severity": item["severity"],
            "category": item["category"],
            "pid": item["pid"],
            "name": item["name"],
            "profile": item.get("profile"),
            "modules": item.get("modules"),
        }
        for item in relevant
        if item["severity"] in {"warning", "error"}
    ]
    return {
        "status": "warn" if findings else "pass",
        "scanned_process_count": len(raw_records),
        "relevant_process_count": len(relevant),
        "counts_by_category": counts,
        "findings": findings,
        "finding_count": len(findings),
        "processes": relevant,
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


def run_doctor(
    *,
    check_backend: bool = False,
    client_config_paths: list[Path] | None = None,
    fail_on_client_drift: bool = False,
    include_process_inventory: bool = False,
    fail_on_token_risk: bool = False,
) -> dict[str, Any]:
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
    client_configs = _scan_client_configs(client_config_paths or [USER_CODEX_CONFIG, USER_CLAUDE_MCP_CONFIG])
    static_findings, static_errors, static_guardrail = _check_static_no_llm()
    gateway = self_check_payload(profile="lite", check_backend=check_backend)
    profiles = list_profiles_payload()
    project_guardrail = _project_mcp_guardrail(project_servers)
    process_inventory = process_inventory_payload() if include_process_inventory or fail_on_token_risk else None

    errors.extend(project_errors)
    errors.extend(static_errors)
    if fail_on_client_drift and client_configs["finding_count"]:
        errors.extend(f"client config drift: {finding['message']}" for finding in client_configs["findings"])
    if fail_on_token_risk and process_inventory and process_inventory["finding_count"]:
        errors.extend(
            f"token-risk process detected: {finding['category']} pid={finding['pid']} name={finding['name']}"
            for finding in process_inventory["findings"]
        )
    if gateway.get("status") != "pass":
        errors.extend(str(item) for item in gateway.get("errors") or [])
    warnings.extend(project_warnings)
    warnings.extend(gateway.get("warnings") or [])
    warnings.extend(f"client config drift: {finding['message']}" for finding in client_configs["findings"])
    if process_inventory:
        warnings.extend(
            f"process inventory finding: {finding['category']} pid={finding['pid']} name={finding['name']}"
            for finding in process_inventory["findings"]
        )
    if "AIstock_worktrees" in root and not branch:
        warnings.append("detached worktree detected; verify client configs do not point at stale worktrees")

    payload = {
        "status": "fail" if errors else "pass",
        "repo": {"root": root, "branch": branch, "head": head},
        "project_mcp": {"path": PROJECT_MCP.relative_to(REPO_ROOT).as_posix(), "servers": project_servers},
        "client_configs": client_configs,
        "gateway_lite": gateway,
        "profiles": profiles,
        "static_no_llm": {"findings": static_findings, **static_guardrail},
        "guardrails": {
            "standalone_default_retirement": project_guardrail,
            "no_background_llm_daemon": static_guardrail,
            "client_config_drift": {"status": client_configs["status"], "finding_count": client_configs["finding_count"]},
        },
        "errors": errors,
        "warnings": warnings,
    }
    if process_inventory:
        payload["process_inventory"] = process_inventory
        payload["guardrails"]["process_token_risk"] = {
            "status": process_inventory["status"],
            "finding_count": process_inventory["finding_count"],
        }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Check AIstock MCP gateway readiness")
    parser.add_argument("--check-backend", action="store_true", help="Also attempt a short backend health request")
    parser.add_argument("--client-config", action="append", default=None, help="Additional Codex TOML config to scan for AIstock MCP drift")
    parser.add_argument("--fail-on-client-drift", action="store_true", help="Fail if user/client MCP config still points at legacy/full AIstock servers")
    parser.add_argument("--process-inventory", action="store_true", help="Include local process inventory for MCP/LLM/token-risk diagnostics")
    parser.add_argument("--fail-on-token-risk", action="store_true", help="Fail if process inventory finds legacy/full/LLM token-risk processes")
    parser.add_argument("--json", action="store_true", help="Pretty-print JSON output")
    args = parser.parse_args()
    client_paths = [Path(item) for item in args.client_config] if args.client_config else None
    payload = run_doctor(
        check_backend=args.check_backend,
        client_config_paths=client_paths,
        fail_on_client_drift=args.fail_on_client_drift,
        include_process_inventory=args.process_inventory,
        fail_on_token_risk=args.fail_on_token_risk,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.json else None))
    raise SystemExit(0 if payload["status"] == "pass" else 2)


if __name__ == "__main__":
    main()
