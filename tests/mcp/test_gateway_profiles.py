from __future__ import annotations

import asyncio

import pytest

from backend.mcp.gateway import create_gateway, list_tools_payload, self_check_payload
from backend.mcp.profiles import GATEWAY_MODULES, SCRIPT_BACKED_SERVERS, resolve_modules
from backend.mcp.tool_manifest import legacy_tool_count, platform_tool_count


def _tool_names_for_profile(profile: str) -> list[str]:
    async def _load() -> list[str]:
        mcp, _registry = create_gateway(profile=profile)
        return [tool.name for tool in await mcp.list_tools()]

    return asyncio.run(_load())


def _tool_input_schemas_for_profile(profile: str) -> dict[str, dict]:
    async def _load() -> dict[str, dict]:
        mcp, _registry = create_gateway(profile=profile)
        return {tool.name: tool.inputSchema for tool in await mcp.list_tools()}

    return asyncio.run(_load())


def test_lite_is_low_resource_default() -> None:
    payload = list_tools_payload(profile="lite")
    assert payload["modules"] == ["catalog"]
    assert payload["tool_count"] == platform_tool_count() == 6
    assert payload["tool_count"] <= 10
    assert payload["legacy_tool_count"] == 0


def test_full_profile_contains_all_migrated_and_platform_tools() -> None:
    payload = list_tools_payload(profile="full")
    assert payload["legacy_tool_count"] == legacy_tool_count() == 370
    assert payload["platform_tool_count"] == 6
    assert payload["tool_count"] == 376
    assert "validation" in payload["modules"]
    assert "qe_experiment" in payload["modules"]
    assert "qe_archive" in payload["modules"]
    assert "qlib_export" in payload["modules"]


def test_former_script_backed_modules_are_gateway_backed() -> None:
    assert SCRIPT_BACKED_SERVERS == set()
    assert {"validation", "qe_experiment", "qe_archive"}.issubset(GATEWAY_MODULES)
    assert resolve_modules(profile="validation") == ["validation"]
    assert resolve_modules(profile="qe") == ["qe_experiment", "qe_archive", "model_registry"]


def test_unknown_profile_fails_fast() -> None:
    with pytest.raises(ValueError, match="Unknown MCP profile"):
        resolve_modules(profile="not_a_profile")


def test_gateway_registration_counts() -> None:
    assert len(_tool_names_for_profile("lite")) == 6
    assert len(_tool_names_for_profile("validation")) == 20
    assert len(_tool_names_for_profile("qe")) == 81
    assert len(_tool_names_for_profile("qlib_data")) == 15
    assert len(_tool_names_for_profile("data_full")) == 62
    assert len(_tool_names_for_profile("full")) == 377


def test_qe_custom_evo_phase_pipeline_fields_are_exposed_in_mcp_schemas() -> None:
    schemas = _tool_input_schemas_for_profile("qe")
    expected_properties = {"phase_pipeline_enabled", "resource_telemetry_enabled"}

    for tool_name in (
        "qe_custom_evo_create_pending",
        "qe_custom_evo_update_config_confirmed",
        "qe_custom_evo_append_loops_confirmed",
        "qe_custom_evo_rerun_loop_confirmed",
    ):
        assert expected_properties <= set(schemas[tool_name]["properties"]), tool_name


def test_qlib_data_profiles_are_task_scoped() -> None:
    qlib = list_tools_payload(profile="qlib_data")
    data_full = list_tools_payload(profile="data_full")

    assert qlib["modules"] == ["qlib_export"]
    assert qlib["tool_count"] == 15
    assert data_full["modules"] == ["local_data", "qlib_export"]
    assert data_full["tool_count"] == 62
    assert resolve_modules(profile="backtest_data") == ["qlib_export"]


def test_paper_v2_profiles_are_task_scoped() -> None:
    monitor = list_tools_payload(profile="paper_v2_monitor")
    stable = list_tools_payload(profile="paper_v2_stable")
    assert monitor["modules"] == ["paper_v2_monitoring", "qmt_broker_monitoring"]
    assert monitor["tool_count"] == 42
    assert stable["modules"] == ["strategy_packages", "selection_center", "advisory", "paper_v2_monitoring", "qmt_broker_monitoring"]
    assert stable["tool_count"] == 128
    assert resolve_modules(profile="paper_v2_ops") == resolve_modules(profile="paper_v2_stable")


def test_self_check_passes_without_backend_requirement() -> None:
    payload = self_check_payload(profile="lite")
    assert payload["status"] == "pass"
    assert payload["profile"] == "lite"
    assert payload["tool_count"] == 6
    assert payload["backend"] == {"checked": False}


