"""Backtest-validated execution policy records for Strategy Packages."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import Enum
from math import isfinite
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.execution_algos import ALGO_REGISTRY
from backend.services.trading_core.errors import StrategyPackageValidationError, UnsupportedFeatureError


class ExecutionPolicyValidationStatus(str, Enum):
    BACKTEST_VALIDATED = "BACKTEST_VALIDATED"


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
}

BACKTEST_SUCCESS_STATUSES = {"SUCCEEDED", "COMPLETED", "BACKTEST_VALIDATED"}


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
        raise StrategyPackageValidationError("execution policy JSON must be a non-empty object")
    unknown = sorted(set(policy_json).difference(ALLOWED_POLICY_JSON_KEYS))
    if unknown:
        raise StrategyPackageValidationError(
            "execution policy contains fields outside the backtest contract",
            context={"unknown_fields": unknown, "allowed_fields": sorted(ALLOWED_POLICY_JSON_KEYS)},
        )
    normalized = dict(policy_json)
    algo_code = str(normalized.get("algo_code") or "").strip().upper()
    if not algo_code:
        raise StrategyPackageValidationError("execution policy requires algo_code")
    normalized["algo_code"] = algo_code
    normalized.setdefault("algo_config", {})
    if not isinstance(normalized["algo_config"], dict):
        raise StrategyPackageValidationError("execution policy algo_config must be an object")
    normalized.setdefault("unfilled_handler_params", {})
    if not isinstance(normalized["unfilled_handler_params"], dict):
        raise StrategyPackageValidationError("unfilled_handler_params must be an object")
    if "max_participation_rate" in normalized["algo_config"]:
        value = float(normalized["algo_config"]["max_participation_rate"])
        if not isfinite(value) or value <= 0 or value > 1:
            raise StrategyPackageValidationError(
                "max_participation_rate must be in (0, 1]",
                context={"max_participation_rate": normalized["algo_config"]["max_participation_rate"]},
            )
    return normalized


def ensure_policy_can_enter_paper(policy: "ValidatedExecutionPolicy") -> None:
    if policy.validation_status != ExecutionPolicyValidationStatus.BACKTEST_VALIDATED:
        raise StrategyPackageValidationError(
            "execution policy must be backtest validated before paper trading",
            context={"policy_id": policy.policy_id, "validation_status": policy.validation_status.value},
        )
    if policy.source_backtest_status.upper() not in BACKTEST_SUCCESS_STATUSES:
        raise StrategyPackageValidationError(
            "execution policy source backtest did not succeed",
            context={"policy_id": policy.policy_id, "source_backtest_status": policy.source_backtest_status},
        )
    if policy.algo_code not in ALGO_REGISTRY:
        raise UnsupportedFeatureError(
            "minute execution algorithm is not registered",
            context={"policy_id": policy.policy_id, "algo_code": policy.algo_code, "registered_algos": sorted(ALGO_REGISTRY)},
        )


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
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

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
        if self.paper_enabled:
            ensure_policy_can_enter_paper(self)
        return self
