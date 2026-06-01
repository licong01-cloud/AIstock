"""Unit tests for MCP profiles, registry, and dynamic gateway loading."""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest


class StubFastMCP:
    def __init__(self, _name: str) -> None:
        self.tools: dict[str, object] = {}

    def tool(self, name: str | None = None, **_kwargs):
        def decorator(func):
            self.tools[name or func.__name__] = func
            return func

        return decorator

    def run(self, **_kwargs) -> None:
        return None


def _install_stub_fastmcp() -> None:
    try:
        from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: F401
        return
    except ImportError:
        pass

    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = StubFastMCP
    mcp_module.server = server_module
    server_module.fastmcp = fastmcp_module
    sys.modules.setdefault("mcp", mcp_module)
    sys.modules.setdefault("mcp.server", server_module)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_module)


_install_stub_fastmcp()

from backend.mcp.profiles import resolve_modules  # noqa: E402
from backend.mcp.registry import ModuleRegistry  # noqa: E402


class FakeMCP:
    def __init__(self) -> None:
        self.tools: dict[str, object] = {}

    def tool(self, name: str | None = None, **_kwargs):
        def decorator(func):
            self.tools[name or func.__name__] = func
            return func

        return decorator


def _registry_tool_counts(registry: ModuleRegistry) -> dict[str, int]:
    counts = registry.tool_counts
    return counts() if callable(counts) else dict(counts)


def test_research_profile_is_only_current_module() -> None:
    assert resolve_modules(profile="research") == ["research"]
    assert resolve_modules(profile="research_assistant") == ["research_assistant"]
    assert resolve_modules(profile="research_with_assistant") == ["research", "research_assistant"]


@pytest.mark.parametrize(
    ("profile", "expected"),
    [
        ("factor_library", ["factor_library"]),
        ("factor_metrics", ["factor_metrics"]),
        ("factor_correlation", ["factor_correlation"]),
        ("model_registry", ["model_registry"]),
        ("strategy_governance", ["strategy_governance"]),
        ("execution_policy", ["execution_policy"]),
        ("external_research", ["external_research"]),
        ("factor_research", ["factor_library", "factor_metrics", "factor_correlation"]),
        ("strategy_ops", ["strategy_governance", "execution_policy"]),
        ("research_full", ["research", "research_assistant", "local_data", "factor_library", "factor_metrics", "factor_correlation", "model_registry", "strategy_governance", "execution_policy", "external_research"]),
        ("full", ["research", "research_assistant", "local_data", "factor_library", "factor_metrics", "factor_correlation", "model_registry", "strategy_governance", "execution_policy", "external_research"]),
    ],
)
def test_unified_profiles_are_available(profile: str, expected: list[str]) -> None:
    assert resolve_modules(profile=profile) == expected


@pytest.mark.parametrize("profile", ["operations", "research_with_qe", "paper_v2"])
def test_unknown_or_script_backed_profiles_are_rejected(profile: str) -> None:
    with pytest.raises(ValueError, match="Unknown|Script-backed"):
        resolve_modules(profile=profile)


def test_registry_client_applies_path_prefix_and_tracks_tool_count() -> None:
    registry = ModuleRegistry(mcp=FakeMCP(), base_url="http://127.0.0.1:8001/api/v1", env_name="test")

    client = registry.client("research-pipeline")
    assert client.base_url == "http://127.0.0.1:8001/api/v1/research-pipeline"

    registry.register_tool_count("research", 16)
    assert registry.tool_count("research") == 16
    assert registry.total_tool_count() == 16
    assert _registry_tool_counts(registry)["research"] == 16


def test_gateway_default_base_url_uses_production_loopback_port() -> None:
    from backend.mcp import gateway

    assert gateway.DEFAULT_BASE_URL == "http://127.0.0.1:8001/api/v1"
    assert ":8011" not in gateway.DEFAULT_BASE_URL


def test_gateway_uses_production_port_when_env_override_is_missing(monkeypatch) -> None:
    from backend.mcp import gateway

    monkeypatch.delenv("AISTOCK_MCP_BASE_URL", raising=False)

    _mcp, registry = gateway.create_gateway(
        profile="research_assistant",
        env_name="AISTOCK_MCP_BASE_URL",
    )

    assert registry.base_url == "http://127.0.0.1:8001/api/v1"
    assert ":8011" not in registry.base_url


def test_repo_aistock_mcp_config_uses_production_port_only() -> None:
    config = json.loads(Path(".mcp.json").read_text(encoding="utf-8"))
    for name, server in config["mcpServers"].items():
        if not name.startswith("aistock") and name != "research-assistant":
            continue
        env_values = server.get("env", {}).values()
        base_urls = [value for value in env_values if isinstance(value, str) and "127.0.0.1" in value]
        assert base_urls, f"{name} must define an explicit loopback backend URL"
        for value in base_urls:
            assert ":8001" in value, f"{name} must target production backend port 8001"
            assert ":8011" not in value, f"{name} must not target test backend port 8011"


def test_registry_exposes_common_sanitize_and_confirm_helpers() -> None:
    registry = ModuleRegistry(mcp=FakeMCP(), base_url="http://127.0.0.1:8001/api/v1", env_name="test")

    assert registry.sanitize("exp_1", "experiment_id") == "exp_1"
    with pytest.raises(ValueError):
        registry.sanitize("../exp_1", "experiment_id")

    assert registry.confirm("RUN_RESEARCH", "RUN_RESEARCH", "confirm_run") is None
    with pytest.raises(ValueError, match="confirm_run"):
        registry.confirm("WRONG", "RUN_RESEARCH", "confirm_run")


def test_gateway_dynamically_registers_fake_module(monkeypatch) -> None:
    from backend.mcp import gateway

    fake_module = types.ModuleType("backend.mcp.modules.fake_current")
    fake_module.TOOL_COUNT = 2

    def register(registry: ModuleRegistry) -> None:
        @registry.mcp.tool(name="fake_alpha")
        def fake_alpha() -> dict[str, bool]:
            return {"ok": True}

        @registry.mcp.tool(name="fake_beta")
        def fake_beta() -> dict[str, bool]:
            return {"ok": True}

        registry.register_tool_count("fake_current", fake_module.TOOL_COUNT)

    fake_module.register = register
    monkeypatch.setitem(sys.modules, "backend.mcp.modules.fake_current", fake_module)
    monkeypatch.setattr(gateway, "resolve_modules", lambda **_kwargs: ["fake_current"])

    _mcp, registry = gateway.create_gateway(
        modules=["fake_current"],
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
    )

    assert registry.tool_count("fake_current") == 2
    assert registry.total_tool_count() == 2


def test_gateway_loads_phase2_research_tools() -> None:
    from backend.mcp import gateway

    _mcp, registry = gateway.create_gateway(
        profile="research",
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
    )

    assert registry.tool_count("research") == 16
    assert registry.total_tool_count() == 16


def test_gateway_loads_research_assistant_tools() -> None:
    from backend.mcp import gateway
    from backend.mcp.modules.research_assistant import TOOL_COUNT as RESEARCH_ASSISTANT_TOOL_COUNT

    _mcp, registry = gateway.create_gateway(
        profile="research_assistant",
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
    )

    assert registry.tool_count("research_assistant") == RESEARCH_ASSISTANT_TOOL_COUNT
    assert registry.total_tool_count() == RESEARCH_ASSISTANT_TOOL_COUNT



def test_gateway_loads_local_data_tools() -> None:
    from backend.mcp import gateway
    from backend.mcp.modules import local_data

    _mcp, registry = gateway.create_gateway(
        profile="local_data",
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
    )

    assert registry.tool_count("local_data") == local_data.TOOL_COUNT
    assert registry.total_tool_count() == local_data.TOOL_COUNT


def test_gateway_loads_external_research_tools() -> None:
    from backend.mcp import gateway
    from backend.mcp.modules import external_research

    _mcp, registry = gateway.create_gateway(
        profile="external_research",
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
    )

    assert registry.tool_count("external_research") == external_research.TOOL_COUNT
    assert registry.total_tool_count() == external_research.TOOL_COUNT


def test_gateway_loads_research_full_profile() -> None:
    from backend.mcp import gateway

    _mcp, registry = gateway.create_gateway(
        profile="research_full",
        base_url="http://127.0.0.1:8001/api/v1",
        env_name="test",
    )

    assert registry.tool_count("research") == 16
    assert registry.tool_count("research_assistant") == 13
    assert registry.tool_count("local_data") == 47
    assert registry.tool_count("factor_library") == 10
    assert registry.tool_count("factor_metrics") == 7
    assert registry.tool_count("factor_correlation") == 8
    assert registry.tool_count("model_registry") == 9
    assert registry.tool_count("strategy_governance") == 9
    assert registry.tool_count("execution_policy") == 7
    assert registry.tool_count("external_research") == 4
    assert registry.total_tool_count() == 130
