"""Backtest-validated execution policy records for Strategy Packages."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from typing import Any
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


def _validate_optional_guard_policy(normalized: dict[str, Any], key: str, allowed_keys: set[str]) -> None:
    if key not in normalized:
        return
    value = normalized[key]
    if not isinstance(value, dict):
        raise RuntimeConfigInvalidError(
            f"execution policy {key} must be an object",
            context={"field": key, "reason_code": "UNSUPPORTED_PRICE_GUARD_CONFIG_ERROR" if key == "price_guard" else "UNSUPPORTED_EXIT_GUARD_CONFIG_ERROR"},
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
