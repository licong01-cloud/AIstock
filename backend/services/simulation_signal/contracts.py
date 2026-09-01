"""Immutable broker-neutral signal, target, and rebalance contracts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Any, Mapping, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.errors import RuntimeConfigInvalidError


class _FrozenDict(dict[str, Any]):
    """JSON-serializable mapping that rejects every in-place mutation."""

    @staticmethod
    def _immutable(*_args: Any, **_kwargs: Any) -> None:
        raise TypeError("frozen evidence mapping cannot be mutated")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable

    def __copy__(self) -> "_FrozenDict":
        return self

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_FrozenDict":
        return self


def _deep_freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict({str(key): _deep_freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze_json(item) for item in value)
    return value


SELECTION_ONLY_FORBIDDEN_KEYS = frozenset(
    {
        "broker_backend",
        "target_broker_backend",
        "broker_account_id",
        "account_id",
        "account_group_id",
        "strategy_slot_id",
        "capital_allocation",
        "capital",
        "initial_cash",
        "cash",
        "total_equity",
        "strategy_name",
        "order_remark",
        "order_remark_prefix",
        "execution_policy",
        "validated_execution_policy",
        "execution_policy_id",
        "execution_policy_version_id",
        "minute_execution_policy",
        "tail_policy",
        "tail_policy_id",
        "tail_policy_version_id",
        "target_position",
        "target_positions",
        "rebalance_intent",
        "rebalance_intents",
        "order_intent",
        "order_intents",
        "execution_plan",
        "broker_order",
        "broker_orders",
        "current_positions",
        "positions",
        "available_quantity",
        "t1_available",
        "t_plus_one",
        "board_lot",
        "order_quantity",
        "quantity",
    }
)


def canonical_json_sha256(payload: dict[str, Any] | list[Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _find_forbidden_key_paths(payload: Any, *, path: str = "") -> list[str]:
    matches: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            text_key = str(key)
            key_path = f"{path}.{text_key}" if path else text_key
            if text_key in SELECTION_ONLY_FORBIDDEN_KEYS:
                matches.append(key_path)
            matches.extend(_find_forbidden_key_paths(value, path=key_path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            key_path = f"{path}[{index}]" if path else f"[{index}]"
            matches.extend(_find_forbidden_key_paths(value, path=key_path))
    return matches


def assert_selection_only_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    matches = _find_forbidden_key_paths(payload)
    if matches:
        raise RuntimeConfigInvalidError(
            "Selection-only signal generation cannot contain broker, capital, target, rebalance or execution fields",
            context={
                **(context or {}),
                "forbidden_paths": matches,
                "forbidden_keys": sorted(SELECTION_ONLY_FORBIDDEN_KEYS),
            },
        )


class DailySelectionEvidence(BaseModel):
    """Content-addressed daily StrategyPackage selection evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_id: str
    target_trade_date: date
    cutoff_date: date | None = None
    package_id: str
    manifest_sha256: str
    release_id: str | None = None
    release_hash: str | None = None
    runtime_profile_version_id: str
    runtime_profile_hash: str
    source_type: str
    data_source: str
    candidate_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    artifact_hash: str
    evidence_payload_json: dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    created_by: str | None = None

    @field_validator(
        "evidence_id",
        "package_id",
        "manifest_sha256",
        "runtime_profile_version_id",
        "runtime_profile_hash",
        "source_type",
        "data_source",
        "artifact_hash",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field is required")
        return text

    @field_validator("release_id", "release_hash", "created_by")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return str(value).strip() or None

    @model_validator(mode="after")
    def _artifact_hash_matches_payload(self) -> "DailySelectionEvidence":
        assert_selection_only_payload_boundary(
            self.evidence_payload_json,
            context={"evidence_id": self.evidence_id, "package_id": self.package_id},
        )
        digest = canonical_json_sha256(self.evidence_payload_json)
        if self.artifact_hash != digest:
            raise ValueError("artifact_hash does not match evidence_payload_json")
        if self.evidence_id != f"dse_{digest[:16]}":
            raise ValueError("evidence_id does not match artifact_hash")
        object.__setattr__(self, "evidence_payload_json", _deep_freeze_json(self.evidence_payload_json))
        return self


class StrategyRuntimeReleaseView(Protocol):
    release_id: str
    release_hash: str
    package_id: str
    manifest_sha256: str
    runtime_profile_id: str
    runtime_profile_version_id: str
    runtime_profile_sha256: str
    release_config_json: dict[str, Any]


class SelectionEvidenceRepository(Protocol):
    def save_daily_selection_evidence(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence: ...


class InMemorySelectionEvidenceRepository:
    """Small signal-owned repository used by read-only previews and tests."""

    def __init__(self) -> None:
        self._by_id: dict[str, DailySelectionEvidence] = {}
        self._id_by_hash: dict[str, str] = {}

    def save_daily_selection_evidence(self, evidence: DailySelectionEvidence) -> DailySelectionEvidence:
        existing_id = self._id_by_hash.get(evidence.artifact_hash)
        if existing_id is not None:
            existing = self._by_id[existing_id]
            if existing != evidence:
                raise RuntimeConfigInvalidError(
                    "immutable daily selection evidence hash conflicts with stored content",
                    context={"artifact_hash": evidence.artifact_hash},
                )
            return existing
        existing = self._by_id.get(evidence.evidence_id)
        if existing is not None and existing != evidence:
            raise RuntimeConfigInvalidError(
                "immutable daily selection evidence id conflicts with stored content",
                context={"evidence_id": evidence.evidence_id},
            )
        stored = evidence.model_copy(deep=True)
        self._by_id[stored.evidence_id] = stored
        self._id_by_hash[stored.artifact_hash] = stored.evidence_id
        return stored


class TargetPortfolio(BaseModel):
    """Desired broker-neutral weights derived only from frozen signal evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_id: str
    trade_date: date
    weights: Mapping[str, float]
    target_hash: str

    @model_validator(mode="after")
    def _target_contract(self) -> "TargetPortfolio":
        weights = {str(symbol): float(weight) for symbol, weight in self.weights.items()}
        if any(weight < 0 or weight > 1 for weight in weights.values()):
            raise ValueError("target weights must be within [0, 1]")
        if sum(weights.values()) > 1.000000001:
            raise ValueError("target weights must not exceed 1")
        payload = {"evidence_id": self.evidence_id, "trade_date": self.trade_date.isoformat(), "weights": weights}
        if self.target_hash != canonical_json_sha256(payload):
            raise ValueError("target_hash mismatch")
        object.__setattr__(self, "weights", _FrozenDict(weights))
        return self


class RebalanceIntent(BaseModel):
    """Signal-layer desired-weight delta, never an order or broker command."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    target_hash: str
    trade_date: date
    desired_weight_delta: Mapping[str, float]
    intent_hash: str

    @model_validator(mode="after")
    def _rebalance_contract(self) -> "RebalanceIntent":
        deltas = {str(symbol): float(delta) for symbol, delta in self.desired_weight_delta.items()}
        payload = {
            "target_hash": self.target_hash,
            "trade_date": self.trade_date.isoformat(),
            "desired_weight_delta": deltas,
        }
        if self.intent_hash != canonical_json_sha256(payload):
            raise ValueError("intent_hash mismatch")
        object.__setattr__(self, "desired_weight_delta", _FrozenDict(deltas))
        return self
