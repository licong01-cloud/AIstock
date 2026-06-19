from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from backend.mcp.gateway import self_check_payload
from scripts.aistock_mcp_gateway_doctor import process_inventory_payload, run_doctor


def _run_json(*args: str) -> dict:
    result = subprocess.run([sys.executable, *args], text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def test_gateway_cli_list_tools_profiles() -> None:
    lite = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=lite")
    full = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=full")
    validation = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=validation")
    qe = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=qe")
    qlib = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=qlib_data")
    data_full = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=data_full")
    assert lite["tool_count"] == 6
    assert full["legacy_tool_count"] == 370
    assert full["tool_count"] == 376
    assert validation["tool_count"] == 20
    assert qe["tool_count"] == 77
    assert qlib["modules"] == ["qlib_export"]
    assert qlib["tool_count"] == 15
    assert data_full["modules"] == ["local_data", "qlib_export"]
    assert data_full["tool_count"] == 62


def test_gateway_cli_startup_summary_is_structured() -> None:
    payload = _run_json("scripts/aistock_mcp_gateway.py", "--startup-summary", "--profile=validation")

    assert payload["schema_version"] == "aistock_mcp_gateway_startup_summary_v1"
    assert payload["status"] == "pass"
    assert payload["profile"] == "validation"
    assert payload["modules"] == ["validation"]
    assert payload["tool_count"] == 20
    assert payload["transport"] == "stdio"
    assert payload["base_url"].startswith("http://127.0.0.1:")
    assert len(payload["manifest_version"]) == 64


def test_self_check_fails_fast_on_backend_dependency_failure(monkeypatch) -> None:
    class FailingClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def get(self, path: str) -> None:
            raise RuntimeError(f"backend unavailable for {path}")

        def close(self) -> None:
            pass

    monkeypatch.setattr("backend.mcp.gateway.httpx.Client", FailingClient)

    payload = self_check_payload(profile="lite", check_backend=True)

    assert payload["status"] == "fail"
    assert payload["backend"]["checked"] is True
    assert payload["backend"]["reachable"] is False
    assert payload["backend"]["dependency_status"] == "unreachable"
    assert any("backend dependency unreachable" in item for item in payload["errors"])


def test_gateway_doctor_passes_project_config() -> None:
    payload = _run_json("scripts/aistock_mcp_gateway_doctor.py", "--json")
    assert payload["status"] == "pass"
    assert payload["gateway_lite"]["tool_count"] == 6
    assert payload["gateway_lite"]["startup_summary"]["schema_version"] == "aistock_mcp_gateway_startup_summary_v1"
    assert payload["gateway_lite"]["startup_summary"]["tool_count"] == 6
    assert payload["static_no_llm"]["findings"] == []
    assert payload["static_no_llm"]["status"] == "pass"
    assert payload["static_no_llm"]["finding_count"] == 0
    assert payload["static_no_llm"]["scanned_file_count"] > 0
    assert "claude" in payload["static_no_llm"]["forbidden_process_families"]
    all_args = [arg for server in payload["project_mcp"]["servers"] for arg in server["args"]]
    assert "scripts/aistock_mcp_server.py" not in all_args
    assert "scripts/aistock_qe_experiment_mcp_server.py" not in all_args
    assert "scripts/aistock_qe_archive_mcp_server.py" not in all_args


def test_gateway_doctor_exposes_default_retirement_guardrail() -> None:
    payload = _run_json("scripts/aistock_mcp_gateway_doctor.py", "--json")
    guardrail = payload["guardrails"]["standalone_default_retirement"]

    assert guardrail["status"] == "pass"
    assert guardrail["default_server"] == "aistock-gateway-lite"
    assert guardrail["default_profile"] == "lite"
    assert guardrail["registered_server_count"] == guardrail["gateway_server_count"]
    assert guardrail["legacy_standalone_servers"] == []
    assert guardrail["full_profile_servers"] == []
    assert guardrail["new_client_session_required_for_tool_injection"] is True
    assert guardrail["evidence_ref"] == ".mcp.json"

    llm_guardrail = payload["guardrails"]["no_background_llm_daemon"]
    assert llm_guardrail["status"] == "pass"
    assert llm_guardrail["finding_count"] == 0


def test_gateway_doctor_flags_legacy_user_client_config(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        """
[mcp_servers.aistock-validation]
command = "python"
args = ["F:/Dev/AIstock/scripts/aistock_mcp_server.py"]
cwd = "F:/Dev/AIstock"
env = { AISTOCK_VALIDATION_BASE_URL = "http://127.0.0.1:8001/api/v1/validation" }
enabled = true
""".strip(),
        encoding="utf-8",
    )

    payload = run_doctor(client_config_paths=[config])

    assert payload["status"] == "pass"
    assert payload["client_configs"]["status"] == "warn"
    assert payload["client_configs"]["finding_count"] == 1
    assert payload["client_configs"]["findings"][0]["code"] == "legacy_standalone_mcp_config"


def test_gateway_doctor_flags_legacy_claude_json_config(tmp_path: Path) -> None:
    config = tmp_path / ".mcp.json"
    config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "aistock-qe-experiment": {
                        "command": "python",
                        "args": ["F:/Dev/AIstock/scripts/aistock_qe_experiment_mcp_server.py"],
                        "env": {"AISTOCK_QE_EXPERIMENT_BASE_URL": "http://127.0.0.1:8001/api/v1"},
                    },
                    "aistock-qe": {
                        "command": "python",
                        "args": ["F:/Dev/AIstock/scripts/aistock_mcp_gateway.py", "--profile=qe"],
                        "env": {"AISTOCK_MCP_BASE_URL": "http://127.0.0.1:8001/api/v1"},
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    payload = run_doctor(client_config_paths=[config])

    assert payload["client_configs"]["status"] == "warn"
    assert payload["client_configs"]["finding_count"] == 1
    assert payload["client_configs"]["findings"][0]["server"] == "aistock-qe-experiment"
    assert payload["client_configs"]["findings"][0]["code"] == "legacy_standalone_mcp_config"


def test_gateway_doctor_can_fail_on_client_config_drift(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        """
[mcp_servers.aistock-qe]
command = "python"
args = ["F:/Dev/AIstock/scripts/aistock_mcp_gateway.py", "--profile=full"]
cwd = "F:/Dev/AIstock"
env = { AISTOCK_MCP_BASE_URL = "http://127.0.0.1:8001/api/v1" }
enabled = true
""".strip(),
        encoding="utf-8",
    )

    payload = run_doctor(client_config_paths=[config], fail_on_client_drift=True)

    assert payload["status"] == "fail"
    assert payload["client_configs"]["findings"][0]["code"] == "full_profile_client_config"
    assert any("client config drift" in item for item in payload["errors"])


def test_process_inventory_classifies_legacy_full_and_llm_processes() -> None:
    payload = process_inventory_payload(
        records=[
            {
                "ProcessId": 101,
                "ParentProcessId": 1,
                "Name": "python.exe",
                "CommandLine": "python F:/Dev/AIstock/scripts/aistock_mcp_server.py",
            },
            {
                "ProcessId": 102,
                "ParentProcessId": 1,
                "Name": "python.exe",
                "CommandLine": "python F:/Dev/AIstock/scripts/aistock_mcp_gateway.py --profile=full",
            },
            {
                "ProcessId": 103,
                "ParentProcessId": 1,
                "Name": "claude.exe",
                "CommandLine": "claude.cmd --output-format stream-json",
            },
            {
                "ProcessId": 104,
                "ParentProcessId": 1,
                "Name": "python.exe",
                "CommandLine": "python F:/Dev/AIstock/scripts/aistock_mcp_gateway.py --profile=lite",
            },
        ]
    )

    assert payload["status"] == "warn"
    assert payload["counts_by_category"]["legacy_standalone_mcp"] == 1
    assert payload["counts_by_category"]["full_profile_gateway"] == 1
    assert payload["counts_by_category"]["llm_or_daemon_token_risk"] == 1
    assert payload["counts_by_category"]["gateway_mcp"] == 1
    assert payload["finding_count"] == 3


def test_process_inventory_does_not_match_bun_inside_unrelated_words() -> None:
    payload = process_inventory_payload(
        records=[
            {
                "ProcessId": 201,
                "ParentProcessId": 1,
                "Name": "crashpad_handler.exe",
                "CommandLine": "crashpad_handler.exe --annotation=bundle_id=com.tencent.qqnt",
            }
        ]
    )

    assert payload["status"] == "pass"
    assert payload["relevant_process_count"] == 0


