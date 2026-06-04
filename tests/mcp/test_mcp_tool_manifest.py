from __future__ import annotations

import importlib

from backend.mcp.tool_manifest import (
    MODULE_TOOL_NAMES,
    TOOL_MANIFEST,
    TOOL_MANIFEST_BY_NAME,
    legacy_tool_count,
    platform_tool_count,
    validate_manifest,
)


def test_manifest_counts_and_required_metadata() -> None:
    assert legacy_tool_count() == 203
    assert platform_tool_count() == 6
    assert len(TOOL_MANIFEST) == 209
    assert len(TOOL_MANIFEST_BY_NAME) == 209
    assert validate_manifest() == []
    for entry in TOOL_MANIFEST:
        assert entry.tool_name
        assert entry.module
        assert entry.profile_tags
        assert entry.risk_level
        assert entry.backend_endpoint
        assert entry.response_budget
        assert entry.assistant_usable
        assert entry.migration_state == "gateway"


def test_module_tool_names_match_module_constants() -> None:
    for module, tool_names in MODULE_TOOL_NAMES.items():
        imported = importlib.import_module(f"backend.mcp.modules.{module}")
        assert tuple(imported.TOOL_NAMES) == tuple(tool_names)
        assert imported.TOOL_COUNT == len(tool_names)


def test_high_risk_tools_have_preflight_metadata() -> None:
    for name in [
        "qe_experiment_run_confirmed",
        "qe_archive_backfill_execute_confirmed",
        "start_validation_execution",
        "mcp_github_issue_create",
    ]:
        entry = TOOL_MANIFEST_BY_NAME[name]
        assert entry.requires_confirmation or entry.risk_level in {"long_running", "external_network", "write_confirmed"}
        assert entry.assistant_usable == "preflight_required"
