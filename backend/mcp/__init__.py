"""Shared MCP platform primitives for AIstock gateway servers.

When pytest adds ``backend/`` to ``sys.path``, this package can be resolved as
top-level ``mcp`` before the installed MCP SDK. Extend the top-level package
search path so legacy imports such as ``mcp.server.fastmcp`` still resolve to
the SDK instead of failing on this in-repo package name.
"""

from __future__ import annotations

import site
from pathlib import Path


def _extend_shadowed_sdk_namespace() -> None:
    if __name__ != "mcp":
        return
    current_dir = Path(__file__).resolve().parent
    search_roots = []
    try:
        search_roots.extend(site.getsitepackages())
    except AttributeError:  # pragma: no cover - platform guard.
        pass
    try:
        search_roots.append(site.getusersitepackages())
    except AttributeError:  # pragma: no cover - platform guard.
        pass
    for root in search_roots:
        candidate = Path(root) / "mcp"
        if candidate.is_dir() and candidate.resolve() != current_dir:
            candidate_str = str(candidate)
            if candidate_str not in __path__:
                __path__.append(candidate_str)


_extend_shadowed_sdk_namespace()

from .common import AIstockApiClient, assert_loopback_url, confirm, sanitize_identifier
from .profiles import resolve_modules
from .registry import ModuleRegistry

__all__ = [
    "AIstockApiClient",
    "ModuleRegistry",
    "assert_loopback_url",
    "confirm",
    "resolve_modules",
    "sanitize_identifier",
]
