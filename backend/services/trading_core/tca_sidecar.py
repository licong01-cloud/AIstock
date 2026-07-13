"""Dependency-light first-write helpers for execution TCA sidecars."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


TCA_OBSERVATION_KEY = "tca_observation_v1"
TCA_OBSERVATION_SCHEMA_VERSION = "miniqmt_tca_observation_v1"


class CaptureMergeOutcome(str, Enum):
    CREATED = "CREATED"
    IDEMPOTENT = "IDEMPOTENT"
    CONFLICT = "CONFLICT"
    IDENTITY_DRIFT = "IDENTITY_DRIFT"
    NOT_FOUND = "NOT_FOUND"


def canonical_json_sha256(payload: Any) -> str:
    encoded = json.dumps(_json_safe(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def new_run_tca_sidecar(*, execution_plan_id: str, execution_plan_hash: str) -> dict[str, Any]:
    return {"schema_version": TCA_OBSERVATION_SCHEMA_VERSION, "execution_plan_id": str(execution_plan_id), "execution_plan_hash": str(execution_plan_hash), "decision_capture_by_parent": {}, "capture_batch_id_by_parent": {}, "capture_errors": {}}


def new_batch_tca_sidecar(*, batch_id: str, logical_tca_scope_hash: str) -> dict[str, Any]:
    return {"schema_version": TCA_OBSERVATION_SCHEMA_VERSION, "logical_tca_scope_hash": str(logical_tca_scope_hash), "capture_batch_id": str(batch_id), "arrival_capture_by_parent": {}, "managed_preflight_eligibility_by_parent": {}, "capture_errors": {}}


def merge_parent_first_write(sidecar: dict[str, Any], *, section: str, parent_intent_id: str, value: Mapping[str, Any] | str) -> CaptureMergeOutcome:
    parent_id = str(parent_intent_id).strip()
    if not parent_id:
        raise ValueError("parent_intent_id is required")
    entries = sidecar.setdefault(section, {})
    if not isinstance(entries, dict):
        raise ValueError(f"TCA sidecar section must be an object: {section}")
    incoming = _json_safe(dict(value)) if isinstance(value, Mapping) else str(value)
    current = entries.get(parent_id)
    if current is None:
        entries[parent_id] = incoming
        return CaptureMergeOutcome.CREATED
    if _entry_sha256(current) == _entry_sha256(incoming):
        return CaptureMergeOutcome.IDEMPOTENT
    return CaptureMergeOutcome.CONFLICT


def preserve_tca_sidecar(existing_payload: Mapping[str, Any], incoming_payload: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(incoming_payload)
    existing_sidecar = existing_payload.get(TCA_OBSERVATION_KEY)
    if isinstance(existing_sidecar, Mapping):
        merged[TCA_OBSERVATION_KEY] = _json_safe(dict(existing_sidecar))
    return merged


def _entry_sha256(value: Any) -> str:
    if isinstance(value, Mapping):
        explicit = value.get("capture_sha256") or value.get("error_sha256")
        if isinstance(explicit, str) and explicit:
            return explicit
    return canonical_json_sha256(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
