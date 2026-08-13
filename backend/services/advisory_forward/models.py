from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class AdvisoryForwardRunV1:
    program_id: str
    program_version: int
    binding_version_id: str
    decision_as_of_trade_date: date
    target_trade_date: date
    forward_run_id: str = field(default_factory=lambda: f"advfwd_{uuid4().hex}")
    publication_status: str = "PENDING"
    settlement_status: str = "NOT_DUE"
    selection_run_id: str | None = None
    review_run_id: str | None = None
    list_version_id: str | None = None
    active_episode_state_hash: str | None = None
    publication_payload_sha256: str | None = None
    settlement_payload_sha256: str | None = None
    last_stage: str = "AFTER_CLOSE_PUBLISH"
    last_reason_code: str | None = None
    last_error_json: dict[str, Any] | None = None
    attempt_count: int = 1
    model_resolution_json: dict[str, Any] = field(default_factory=dict)
    run_payload_json: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    published_at: datetime | None = None
    settled_at: datetime | None = None

    def canonical_payload_hash(self) -> str:
        return canonical_json_sha256(_json_ready(asdict(self)))


@dataclass(frozen=True)
class AdvisoryForwardModelObservationV1:
    forward_run_id: str
    program_id: str
    binding_version_id: str
    decision_as_of_trade_date: date
    target_trade_date: date
    status: str
    observation_id: str = field(default_factory=lambda: f"advobs_{uuid4().hex}")
    reason_code: str | None = None
    message: str | None = None
    package_id: str | None = None
    manifest_sha256: str | None = None
    style_profile_id: str | None = None
    style_profile_hash: str | None = None
    model_descriptor_sha256: str | None = None
    bundle_id: str | None = None
    outcome_bundle_id: str | None = None
    price_range_bundle_id: str | None = None
    feature_schema_version: str | None = None
    candidate_count: int = 0
    shortlist_count: int = 0
    maturity_trade_date: date | None = None
    prediction_payload_json: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)

    def payload(self) -> dict[str, Any]:
        return _json_ready(asdict(self))

    def payload_sha256(self) -> str:
        payload = self.payload()
        payload.pop("created_at", None)
        payload.pop("updated_at", None)
        return canonical_json_sha256(payload)


def _json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
