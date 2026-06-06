"""Lazy adapter for compatibility-only Validation MCP script helpers."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

LEGACY_VALIDATION_MODULE = "scripts.aistock_mcp_server"


def _legacy_validation() -> ModuleType:
    return import_module(LEGACY_VALIDATION_MODULE)


def report_bug(**kwargs: Any) -> Any:
    return _legacy_validation().report_bug(**kwargs)


def mcp_github_issue_list(**kwargs: Any) -> Any:
    return _legacy_validation().mcp_github_issue_list(**kwargs)


def mcp_github_issue_search(**kwargs: Any) -> Any:
    return _legacy_validation().mcp_github_issue_search(**kwargs)


def mcp_github_issue_create(**kwargs: Any) -> Any:
    return _legacy_validation().mcp_github_issue_create(**kwargs)


def assign_bug(**kwargs: Any) -> Any:
    return _legacy_validation().assign_bug(**kwargs)


def update_bug_status(**kwargs: Any) -> Any:
    return _legacy_validation().update_bug_status(**kwargs)


def mcp_github_issue_sync_bug(**kwargs: Any) -> Any:
    return _legacy_validation().mcp_github_issue_sync_bug(**kwargs)

