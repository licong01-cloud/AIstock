"""Archive policy resolver for QE completion and backfill flows."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .models import ArchivePolicyDecision, sha256_json

ARCHIVE_POLICY_AUTO = "AUTO"
ARCHIVE_POLICY_SKIP = "SKIP"
ARCHIVE_POLICY_MANUAL_ONLY = "MANUAL_ONLY"
SUPPORTED_ARCHIVE_POLICIES = {ARCHIVE_POLICY_AUTO, ARCHIVE_POLICY_SKIP, ARCHIVE_POLICY_MANUAL_ONLY}


def normalize_archive_policy(value: Any) -> tuple[str, str]:
    text = str(value or ARCHIVE_POLICY_AUTO).strip().upper()
    if text in SUPPORTED_ARCHIVE_POLICIES:
        return text, "valid"
    return ARCHIVE_POLICY_MANUAL_ONLY, f"invalid_archive_policy:{text or 'empty'}"


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _candidate_layers(payload: Mapping[str, Any], runtime_config: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    layers: list[tuple[str, Mapping[str, Any]]] = []
    payload_config = _mapping(payload.get("config"))
    payload_raw_config = _mapping(payload.get("raw_config"))
    payload_loop_config = _mapping(payload_raw_config.get("loop_config_json")) if payload_raw_config else None
    for layer_name, container in (
        ("runtime_config", runtime_config),
        ("runtime_config.runtime_flags", runtime_config.get("runtime_flags")),
        ("runtime_config.custom_params", runtime_config.get("custom_params")),
        ("runtime_config.model_params", runtime_config.get("model_params")),
        ("runtime_config.strategy_params", runtime_config.get("strategy_params")),
        ("payload", payload),
        ("payload.config", payload_config),
        ("payload.config.runtime_flags", payload_config.get("runtime_flags") if payload_config else None),
        ("payload.config.custom_params", payload_config.get("custom_params") if payload_config else None),
        ("payload.config.model_params", payload_config.get("model_params") if payload_config else None),
        ("payload.config.strategy_params", payload_config.get("strategy_params") if payload_config else None),
        ("payload.raw_config.custom_params", payload_raw_config.get("custom_params") if payload_raw_config else None),
        ("payload.raw_config.loop_config_json.model_params", payload_loop_config.get("model_params") if payload_loop_config else None),
        ("payload.raw_config.loop_config_json.strategy_params", payload_loop_config.get("strategy_params") if payload_loop_config else None),
    ):
        mapped = _mapping(container)
        if mapped is not None:
            layers.append((layer_name, mapped))
    return layers


def resolve_archive_policy(
    *,
    source_system: str,
    source_type: str,
    source_id: str,
    source_sub_id: str | None = None,
    payload: Mapping[str, Any] | None = None,
    runtime_config: Mapping[str, Any] | None = None,
    default_policy: str = ARCHIVE_POLICY_AUTO,
) -> ArchivePolicyDecision:
    """Resolve AUTO/SKIP/MANUAL_ONLY without throwing into QE runtime hooks."""

    payload_dict = dict(payload or {})
    runtime_dict = dict(runtime_config or {})
    candidates: list[tuple[str, Any]] = []
    for layer_name, container in _candidate_layers(payload_dict, runtime_dict):
        if "archive_policy" in container:
            candidates.append((layer_name, container.get("archive_policy")))
    source, raw_policy = candidates[0] if candidates else ("default", default_policy)
    policy, reason = normalize_archive_policy(raw_policy)
    if reason == "valid":
        reason_value = payload_dict.get("archive_reason") or runtime_dict.get("archive_reason")
        if reason_value is None:
            for _, container in _candidate_layers(payload_dict, runtime_dict):
                if container.get("archive_reason"):
                    reason_value = container.get("archive_reason")
                    break
        reason = str(reason_value or ("default_auto" if policy == ARCHIVE_POLICY_AUTO else f"policy_{policy.lower()}"))
    return ArchivePolicyDecision(
        source_system=source_system,
        source_type=source_type,
        source_id=source_id,
        source_sub_id=source_sub_id,
        archive_policy=policy,
        archive_policy_source=source,
        reason=reason,
        allow_override=bool(payload_dict.get("archive_allow_override") or runtime_dict.get("archive_allow_override") or policy == ARCHIVE_POLICY_MANUAL_ONLY),
        runtime_config=runtime_dict,
        payload_sha256=sha256_json(payload_dict) if payload_dict else None,
        runtime_config_sha256=sha256_json(runtime_dict) if runtime_dict else None,
    )
