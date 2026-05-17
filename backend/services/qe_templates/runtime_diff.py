"""Runtime diff helpers for QE templates."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def build_runtime_diff(template_config: Mapping[str, Any], runtime_config: Mapping[str, Any]) -> dict[str, Any]:
    template_keys = set(template_config.keys())
    runtime_keys = set(runtime_config.keys())
    changed = {}
    for key in sorted(template_keys & runtime_keys):
        if template_config.get(key) != runtime_config.get(key):
            changed[key] = {"template": template_config.get(key), "runtime": runtime_config.get(key)}
    return {
        "added_keys": sorted(runtime_keys - template_keys),
        "removed_keys": sorted(template_keys - runtime_keys),
        "changed": changed,
    }
