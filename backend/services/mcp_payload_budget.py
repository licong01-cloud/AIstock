"""Summary-first payload helpers shared by MCP facades.

The helpers keep list/search/overview responses compact and make large sections
explicitly discoverable through detail tools or artifact references instead of
inlining raw payloads into MCP responses.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

DEFAULT_LIST_LIMIT = 20
MAX_LIST_LIMIT = 100
DEFAULT_OFFSET = 0

FORBIDDEN_SUMMARY_FIELDS = {
    "raw_payload",
    "payload",
    "raw_json",
    "metrics_json",
    "config_json",
    "train_config_json",
    "hyperparams_json",
    "architecture_config",
    "architecture_sha256",
    "default_search_space",
    "default_train_budget",
    "hyperparam_schema",
    "default_hyperparams",
    "search_space_json",
    "input_contract_json",
    "output_contract_json",
    "feature_schema_requirements",
    "label_requirements",
    "dependency_versions",
    "training_curves",
    "deterministic_flags_json",
    "seed_sequence",
    "factor_list_ordered",
    "feature_schema",
    "feature_schema_hash",
    "model_artifacts",
    "model_weights",
    "weights",
    "code_text",
    "code_text_preview",
    "source_code",
    "python_implementation",
    "full_logs",
    "logs",
    "matrix",
    "correlation_matrix",
    "daily_correlations",
    "parquet_rows",
    "rows",
    "scores_json",
    "manifest",
    "manifest_json",
    "runtime_config_contract",
    "performance_metrics",
    "interface_info",
    "best_performance",
    "raw_metrics",
}


def clamp_limit(limit: int | None, *, default: int = DEFAULT_LIST_LIMIT, max_limit: int = MAX_LIST_LIMIT) -> int:
    """Clamp list/search limits to the MCP response budget contract."""

    if limit is None:
        return default
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        return default
    if parsed <= 0:
        return default
    return min(parsed, max_limit)


def clamp_offset(offset: int | None) -> int:
    try:
        parsed = int(offset if offset is not None else DEFAULT_OFFSET)
    except (TypeError, ValueError):
        return DEFAULT_OFFSET
    return max(parsed, 0)


def pagination(limit: int, offset: int, total: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"limit": limit, "offset": offset, "next_offset": offset + limit}
    if total is not None:
        payload["total"] = int(total)
        payload["has_more"] = offset + limit < int(total)
    return payload


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(key): json_safe(val) for key, val in value.items()}
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    return value


def strip_forbidden_fields(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]], forbidden: set[str] | None = None) -> Any:
    """Drop heavy fields recursively from summary responses."""

    blocked = {key.lower() for key in (forbidden or FORBIDDEN_SUMMARY_FIELDS)}

    def clean(value: Any) -> Any:
        if isinstance(value, Mapping):
            result: dict[str, Any] = {}
            for key, item in value.items():
                key_text = str(key)
                if key_text.lower() in blocked:
                    continue
                result[key_text] = clean(item)
            return result
        if isinstance(value, list):
            return [clean(item) for item in value]
        if isinstance(value, tuple):
            return [clean(item) for item in value]
        return json_safe(value)

    return clean(payload)


def artifact_ref(kind: str, uri: str | None = None, metadata: Mapping[str, Any] | None = None, **extra: Any) -> dict[str, Any]:
    ref: dict[str, Any] = {"kind": kind, "inline": False}
    if uri:
        ref["uri"] = str(uri)
    if metadata:
        ref["metadata"] = strip_forbidden_fields(metadata)
    for key, value in extra.items():
        if value is not None:
            ref[key] = json_safe(value)
    return ref


def detail_ref(server: str, tool: str, args: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"server": server, "tool": tool, "args_hint": json_safe(dict(args or {}))}


def summary_envelope(
    *,
    domain: str,
    items: Sequence[Mapping[str, Any]],
    total: int | None = None,
    limit: int | None = None,
    offset: int | None = None,
    omitted_sections: Sequence[str] | None = None,
    detail_tool: str | None = None,
    detail_args_hint: Mapping[str, Any] | None = None,
    artifact_refs: Sequence[Mapping[str, Any]] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    safe_limit = clamp_limit(limit)
    safe_offset = clamp_offset(offset)
    payload: dict[str, Any] = {
        "ok": True,
        "domain": domain,
        "summary_first": True,
        "items": strip_forbidden_fields(list(items)),
        "pagination": pagination(safe_limit, safe_offset, total),
        "omitted_sections": list(omitted_sections or []),
    }
    if total is not None:
        payload["total"] = int(total)
    if detail_tool:
        payload["detail_tool"] = detail_tool
    if detail_args_hint is not None:
        payload["detail_args_hint"] = json_safe(dict(detail_args_hint))
    if artifact_refs:
        payload["artifact_refs"] = [strip_forbidden_fields(ref) for ref in artifact_refs]
    if extra:
        payload.update(strip_forbidden_fields(extra))
    return payload


def assert_summary_payload(payload: Mapping[str, Any]) -> None:
    """Fail fast when a summary endpoint accidentally returns heavy fields."""

    def walk(value: Any, path: str = "") -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                key_text = str(key)
                if key_text.lower() in FORBIDDEN_SUMMARY_FIELDS:
                    raise ValueError(f"summary payload contains forbidden field {path + key_text}")
                walk(item, f"{path}{key_text}.")
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                walk(item, f"{path}{idx}.")

    walk(payload)
