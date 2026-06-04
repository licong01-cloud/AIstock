from __future__ import annotations

import json
import subprocess
import sys


def _run_json(*args: str) -> dict:
    result = subprocess.run([sys.executable, *args], text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def test_gateway_cli_list_tools_profiles() -> None:
    lite = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=lite")
    full = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=full")
    validation = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=validation")
    qe = _run_json("scripts/aistock_mcp_gateway.py", "--list-tools", "--profile=qe")
    assert lite["tool_count"] == 6
    assert full["legacy_tool_count"] == 203
    assert full["tool_count"] == 209
    assert validation["tool_count"] == 19
    assert qe["tool_count"] == 63


def test_gateway_doctor_passes_project_config() -> None:
    payload = _run_json("scripts/aistock_mcp_gateway_doctor.py", "--json")
    assert payload["status"] == "pass"
    assert payload["gateway_lite"]["tool_count"] == 6
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
