"""Backtest-validated execution policy records for Strategy Packages."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from typing import Any, Mapping
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.errors import RuntimeConfigInvalidError


class ExecutionPolicyValidationStatus(str, Enum):
    BACKTEST_VALIDATED = "BACKTEST_VALIDATED"


BACKTEST_SUCCESS_STATUSES = {"SUCCEEDED", "COMPLETED", "BACKTEST_VALIDATED"}


ALLOWED_POLICY_JSON_KEYS = {
    "execution_level",
    "bar_freq",
    "algo_code",
    "algo_config",
    "fallback_algo_code",
    "data_requirements",
    "fallback_policy",
    "quality_report",
    "unfilled_handler",
    "unfilled_handler_params",
    "price_guard",
    "exit_guard",
    "schedule_window",
    "quote_contract",
    "quote_evidence",
}

PRICE_GUARD_POLICY_KEYS = {
    "contract",
    "enabled",
    "mode",
    "price_basis",
    "signal_ref_price",
    "buy",
    "sell",
    "guidance_status",
    "policy_sha256",
}
EXIT_GUARD_POLICY_KEYS = {
    "contract",
    "enabled",
    "mode",
    "price_basis",
    "stop_loss",
    "take_profit",
    "alpha_decay_exit",
    "time_stop",
    "t1_handling",
    "guidance_status",
    "policy_sha256",
}
ALGO_CONFIG_GUARD_FORBIDDEN_KEYS = {
    "price_guard",
    "exit_guard",
    "signal_ref_price",
    "max_open_gap_bps",
    "yellow_open_gap_bps",
    "yellow_size_multiplier",
    "max_chase_bps",
    "yellow_chase_bps",
    "near_limit_up_skip_bps",
    "breakout_addon",
    "rebalance_max_slippage_bps",
    "risk_exit_max_slippage_bps",
    "near_limit_down_rebalance_skip_bps",
    "stop_loss",
    "take_profit",
    "alpha_decay_exit",
    "time_stop",
    "t1_handling",
}


def compute_execution_policy_sha256(policy_json: dict[str, Any]) -> str:
    encoded = json.dumps(
        policy_json,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_execution_policy_json(policy_json: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(policy_json, dict) or not policy_json:
        raise RuntimeConfigInvalidError("execution policy JSON must be a non-empty object")
    unknown = sorted(set(policy_json).difference(ALLOWED_POLICY_JSON_KEYS))
    if unknown:
        raise RuntimeConfigInvalidError(
            "execution policy contains fields outside the backtest contract",
            context={"unknown_fields": unknown, "allowed_fields": sorted(ALLOWED_POLICY_JSON_KEYS)},
        )
    normalized = dict(policy_json)
    algo_code = str(normalized.get("algo_code") or "").strip().upper()
    if not algo_code:
        raise RuntimeConfigInvalidError("execution policy requires algo_code")
    normalized["algo_code"] = algo_code
    normalized.setdefault("algo_config", {})
    if not isinstance(normalized["algo_config"], dict):
        raise RuntimeConfigInvalidError("execution policy algo_config must be an object")
    _reject_guard_keys_in_algo_config(normalized["algo_config"])
    _validate_optional_guard_policy(normalized, "price_guard", PRICE_GUARD_POLICY_KEYS)
    _validate_optional_guard_policy(normalized, "exit_guard", EXIT_GUARD_POLICY_KEYS)
    normalized.setdefault("unfilled_handler_params", {})
    if not isinstance(normalized["unfilled_handler_params"], dict):
        raise RuntimeConfigInvalidError("unfilled_handler_params must be an object")
    if "max_participation_rate" in normalized["algo_config"]:
        value = float(normalized["algo_config"]["max_participation_rate"])
        if not isfinite(value) or value <= 0 or value > 1:
            raise RuntimeConfigInvalidError(
                "max_participation_rate must be in (0, 1]",
                context={"max_participation_rate": normalized["algo_config"]["max_participation_rate"]},
            )
    return normalized


FROZEN_EXECUTION_POLICY_ID_FIELDS = (
    "policy_version_id",
    "validated_execution_policy_id",
    "policy_id",
)


def validate_frozen_execution_policy_snapshot(
    snapshot: Mapping[str, Any] | None,
    *,
    expected_policy_id: str | None = None,
    expected_policy_sha256: str | None = None,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate one explicit immutable execution-policy snapshot.

    The accepted ID spellings are distinct persisted schema variants. Exactly
    one must be present; aliases cannot be combined and no unknown field is
    ignored. The hash always covers the normalized policy JSON.
    """

    error_context = dict(context or {})
    if not isinstance(snapshot, Mapping) or not snapshot:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot must be an explicit non-empty object",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_SNAPSHOT_MISSING",
            },
        )
    payload = dict(snapshot)
    present_id_fields = [field for field in FROZEN_EXECUTION_POLICY_ID_FIELDS if str(payload.get(field) or "").strip()]
    if len(present_id_fields) != 1:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot requires exactly one policy identity field",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_SNAPSHOT_SCHEMA_INVALID",
                "present_policy_id_fields": present_id_fields,
                "allowed_policy_id_fields": list(FROZEN_EXECUTION_POLICY_ID_FIELDS),
            },
        )
    id_field = present_id_fields[0]
    expected_fields = {id_field, "policy_sha256", "policy_json"}
    if set(payload) != expected_fields:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot fields are not exact",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_SNAPSHOT_SCHEMA_INVALID",
                "missing_fields": sorted(expected_fields - set(payload)),
                "unknown_fields": sorted(set(payload) - expected_fields),
            },
        )
    policy_id = str(payload[id_field]).strip()
    policy_sha256 = str(payload["policy_sha256"] or "").strip().lower()
    if not policy_id or not policy_sha256:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot identity is incomplete",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_SNAPSHOT_IDENTITY_INCOMPLETE",
                "policy_id": policy_id or None,
                "policy_sha256": policy_sha256 or None,
            },
        )
    raw_policy_json = payload["policy_json"]
    if not isinstance(raw_policy_json, dict) or not raw_policy_json:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot requires a non-empty policy_json",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_SNAPSHOT_SCHEMA_INVALID",
                "policy_id": policy_id,
            },
        )
    normalized = normalize_execution_policy_json(dict(raw_policy_json))
    computed_sha256 = compute_execution_policy_sha256(normalized)
    if policy_sha256 != computed_sha256:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot hash does not match normalized policy_json",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_HASH_CONFLICT",
                "policy_id": policy_id,
                "stored_policy_sha256": policy_sha256,
                "computed_policy_sha256": computed_sha256,
            },
        )
    expected_id = str(expected_policy_id or "").strip()
    expected_sha = str(expected_policy_sha256 or "").strip().lower()
    if expected_id and policy_id != expected_id:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot ID conflicts with the runtime release",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_IDENTITY_CONFLICT",
                "expected_policy_id": expected_id,
                "snapshot_policy_id": policy_id,
            },
        )
    if expected_sha and policy_sha256 != expected_sha:
        raise RuntimeConfigInvalidError(
            "LocalSim execution policy snapshot hash conflicts with the runtime release",
            context={
                **error_context,
                "reason_code": "LOCALSIM_EXECUTION_POLICY_IDENTITY_CONFLICT",
                "expected_policy_sha256": expected_sha,
                "snapshot_policy_sha256": policy_sha256,
            },
        )
    return {
        id_field: policy_id,
        "policy_sha256": policy_sha256,
        "policy_json": normalized,
    }


def _validate_optional_guard_policy(normalized: dict[str, Any], key: str, allowed_keys: set[str]) -> None:
    if key not in normalized:
        return
    value = normalized[key]
    if not isinstance(value, dict):
        raise RuntimeConfigInvalidError(
            f"execution policy {key} must be an object",
            context={
                "field": key,
                "reason_code": "UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR"
                if key == "price_guard"
                else "UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR",
            },
        )
    unknown = sorted(set(value).difference(allowed_keys))
    if unknown:
        raise RuntimeConfigInvalidError(
            f"execution policy {key} contains unsupported fields",
            context={"field": key, "unknown_fields": unknown, "allowed_fields": sorted(allowed_keys)},
        )


def _reject_guard_keys_in_algo_config(value: dict[str, Any]) -> None:
    hits = sorted(_find_forbidden_guard_keys(value))
    if not hits:
        return
    raise RuntimeConfigInvalidError(
        "execution policy algo_config must not carry PriceGuard/ExitGuard parameters",
        context={"forbidden_guard_keys": hits, "allowed_fields": sorted(ALLOWED_POLICY_JSON_KEYS)},
    )


def _find_forbidden_guard_keys(value: Any, *, prefix: str = "algo_config") -> set[str]:
    hits: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}"
            if key_text in ALGO_CONFIG_GUARD_FORBIDDEN_KEYS:
                hits.add(path)
            hits.update(_find_forbidden_guard_keys(child, prefix=path))
    elif isinstance(value, list):
        for idx, child in enumerate(value):
            hits.update(_find_forbidden_guard_keys(child, prefix=f"{prefix}[{idx}]"))
    return hits


class ValidatedExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_id: str = Field(default_factory=lambda: f"execpol_{uuid4().hex}")
    package_id: str
    manifest_sha256: str
    policy_name: str
    policy_json: dict[str, Any]
    policy_sha256: str | None = None
    algo_code: str | None = None
    algo_config: dict[str, Any] = Field(default_factory=dict)
    unfilled_handler: str | None = None
    unfilled_handler_params: dict[str, Any] = Field(default_factory=dict)
    source_backtest_id: str
    source_backtest_status: str
    validation_status: ExecutionPolicyValidationStatus = ExecutionPolicyValidationStatus.BACKTEST_VALIDATED
    paper_enabled: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("package_id", "manifest_sha256", "policy_name", "source_backtest_id", "source_backtest_status")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @model_validator(mode="after")
    def _normalize_and_hash(self) -> "ValidatedExecutionPolicy":
        normalized = normalize_execution_policy_json(self.policy_json)
        digest = compute_execution_policy_sha256(normalized)
        algo_code = normalized["algo_code"]
        updates = {
            "policy_json": normalized,
            "policy_sha256": self.policy_sha256 or digest,
            "algo_code": self.algo_code or algo_code,
            "algo_config": dict(normalized.get("algo_config") or {}),
            "unfilled_handler": normalized.get("unfilled_handler"),
            "unfilled_handler_params": dict(normalized.get("unfilled_handler_params") or {}),
        }
        if updates["policy_sha256"] != digest:
            raise ValueError("policy_sha256 does not match policy_json")
        if str(updates["algo_code"]).upper() != algo_code:
            raise ValueError("algo_code does not match policy_json.algo_code")
        for key, value in updates.items():
            object.__setattr__(self, key, value)
        return self
