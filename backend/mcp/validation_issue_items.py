"""Shared compact issue payload helpers for MCP validation tools."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def compact_issue_item(item: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {
        "source": item.get("source"),
        "registry_is_source_of_truth": item.get("registry_is_source_of_truth"),
        "bug_id": item.get("bug_id"),
        "number": item.get("number"),
        "title": item.get("title") or "",
        "state": item.get("state"),
        "status": item.get("status"),
        "severity": item.get("severity"),
        "module": item.get("module"),
        "labels": item.get("labels") or [],
        "html_url": item.get("html_url"),
        "source_path": item.get("source_path"),
    }
    github_issue = item.get("github_issue")
    if isinstance(github_issue, Mapping):
        compact["github_issue"] = {
            key: github_issue.get(key)
            for key in ("number", "state", "title", "html_url")
            if github_issue.get(key) is not None
        }
    return {key: value for key, value in compact.items() if value not in (None, "", [])}

