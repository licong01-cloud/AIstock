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


def test_lite_is_low_resource_default() -> None:
    payload = list_tools_payload(profile="lite")
    assert payload["modules"] == ["catalog"]
    assert payload["tool_count"] == platform_tool_count() == 6
    assert payload["tool_count"] <= 10
    assert payload["legacy_tool_count"] == 0


def test_full_profile_contains_all_migrated_and_platform_tools() -> None:
    payload = list_tools_payload(profile="full")
    assert payload["legacy_tool_count"] == legacy_tool_count() == 203
    assert payload["platform_tool_count"] == 6
    assert payload["tool_count"] == 209
    assert "validation" in payload["modules"]
    assert "qe_experiment" in payload["modules"]
    assert "qe_archive" in payload["modules"]


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
    assert len(_tool_names_for_profile("validation")) == 19
    assert len(_tool_names_for_profile("qe")) == 63
    assert len(_tool_names_for_profile("full")) == 209


def test_self_check_passes_without_backend_requirement() -> None:
    payload = self_check_payload(profile="lite")
    assert payload["status"] == "pass"
    assert payload["profile"] == "lite"
    assert payload["tool_count"] == 6
    assert payload["backend"] == {"checked": False}
