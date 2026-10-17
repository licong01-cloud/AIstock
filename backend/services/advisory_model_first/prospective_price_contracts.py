from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.price_range_contracts import (
    canonical_json_sha256,
)


SHA256_PATTERN = r"^[0-9a-f]{64}$"
PROGRAM_ID_PATTERN = r"^advp_[A-Za-z0-9_-]{1,123}$"
BINDING_ID_PATTERN = r"^advb_[A-Za-z0-9_-]{1,123}$"
REQUEST_ID_PATTERN = r"^advprpros_[0-9a-f]{24}$"
SHANGHAI = ZoneInfo("Asia/Shanghai")


class FrozenAdvisoryPriceProspectiveRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["frozen_advisory_price_prospective_request_v1"] = (
        "frozen_advisory_price_prospective_request_v1"
    )
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    model_frozen_at: datetime
    target_open_at: datetime
    program_id: str = Field(pattern=PROGRAM_ID_PATTERN)
    binding_version_id: str = Field(pattern=BINDING_ID_PATTERN)
    list_version_id: str = Field(min_length=1)
    review_run_id: str = Field(min_length=1)
    selection_run_id: str = Field(min_length=1)
    candidate_count: int = Field(gt=0)
    candidate_symbols_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_as_of_trade_date: date
    target_trade_date: date
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    style_profile_id: str = Field(min_length=1)
    style_profile_hash: str = Field(pattern=SHA256_PATTERN)
    selection_runtime_semantics_hash: str = Field(pattern=SHA256_PATTERN)
    parent_bundle_id: str = Field(pattern=SHA256_PATTERN)
    parent_bundle_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    outcome_bundle_id: str = Field(pattern=SHA256_PATTERN)
    outcome_bundle_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    price_range_bundle_id: str = Field(pattern=SHA256_PATTERN)
    price_range_bundle_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    feature_schema_version: Literal["advisory_feature_schema_v1"] = "advisory_feature_schema_v1"
    feature_schema_hash: str = Field(pattern=SHA256_PATTERN)
    review_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    component_roles: dict[str, str]
    terminal_weights: dict[str, float]
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    evidence_level: Literal["PROSPECTIVE_OOS"] = "PROSPECTIVE_OOS"
    realized_outcome_access_allowed: Literal[False] = False

    @model_validator(mode="after")
    def validate_contract(self) -> "FrozenAdvisoryPriceProspectiveRequestV1":
        created_at = _aware_utc(self.created_at, field="created_at")
        model_frozen_at = _aware_utc(self.model_frozen_at, field="model_frozen_at")
        target_open_at = _aware_utc(self.target_open_at, field="target_open_at")
        expected_open = datetime.combine(
            self.target_trade_date,
            datetime.min.time().replace(hour=9, minute=30),
            tzinfo=SHANGHAI,
        ).astimezone(timezone.utc)
        if target_open_at != expected_open:
            raise ValueError("target_open_at must be the target trade date 09:30 Asia/Shanghai")
        if self.decision_as_of_trade_date >= self.target_trade_date:
            raise ValueError("prospective decision date must precede target trade date")
        if model_frozen_at > created_at:
            raise ValueError("prospective request predates the frozen model")
        if created_at >= target_open_at:
            raise ValueError("prospective request must be frozen before target open")
        if set(self.component_roles) != {"lstm", "fund"}:
            raise ValueError("prospective component_roles must identify lstm and fund")
        role_names = set(self.component_roles.values())
        if not role_names or set(self.terminal_weights) != role_names:
            raise ValueError("prospective terminal weights differ from component roles")
        if any(value < 0.0 for value in self.terminal_weights.values()):
            raise ValueError("prospective terminal weights must be nonnegative")
        if abs(sum(self.terminal_weights.values()) - 1.0) > 1e-8:
            raise ValueError("prospective terminal weights must sum to one")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected:
            raise ValueError("prospective request_sha256 mismatch")
        if self.request_id != f"advprpros_{expected[:24]}":
            raise ValueError("prospective request_id does not match request_sha256")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256"})


class AdvisoryPriceProspectivePredictionReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_price_prospective_prediction_receipt_v1"] = (
        "advisory_price_prospective_prediction_receipt_v1"
    )
    status: Literal["PUBLISHED", "ALREADY_MATERIALIZED"]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    prediction_bundle_id: str = Field(pattern=SHA256_PATTERN)
    prediction_sha256: str = Field(pattern=SHA256_PATTERN)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_as_of_trade_date: date
    target_trade_date: date
    published_at: datetime
    candidate_count: int = Field(gt=0)
    available_count: int = Field(ge=0)
    unavailable_count: int = Field(ge=0)
    elapsed_seconds: float = Field(ge=0.0)
    parent_bundle_id: str = Field(pattern=SHA256_PATTERN)
    outcome_bundle_id: str = Field(pattern=SHA256_PATTERN)
    price_range_bundle_id: str = Field(pattern=SHA256_PATTERN)
    evidence_level: Literal["PROSPECTIVE_OOS"] = "PROSPECTIVE_OOS"
    realized_outcome_accessed: Literal[False] = False
    binding_activated: Literal[False] = False
    database_written: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False

    @model_validator(mode="after")
    def validate_counts(self) -> "AdvisoryPriceProspectivePredictionReceiptV1":
        if self.available_count + self.unavailable_count != self.candidate_count:
            raise ValueError("prospective receipt candidate counts do not add up")
        _aware_utc(self.published_at, field="published_at")
        if self.receipt_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("prospective receipt_sha256 mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"status", "receipt_sha256"},
        )


def build_frozen_price_prospective_request(
    **values: Any,
) -> FrozenAdvisoryPriceProspectiveRequestV1:
    payload = dict(values)
    payload.setdefault("schema_version", "frozen_advisory_price_prospective_request_v1")
    payload.setdefault("created_at", datetime.now(timezone.utc))
    payload["component_roles"] = dict(payload["component_roles"])
    payload["terminal_weights"] = {str(key): float(value) for key, value in payload["terminal_weights"].items()}
    seed = FrozenAdvisoryPriceProspectiveRequestV1.model_construct(
        request_id="advprpros_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdvisoryPriceProspectiveRequestV1(
        request_id=f"advprpros_{digest[:24]}",
        request_sha256=digest,
        **payload,
    )


def build_advisory_price_prospective_prediction_receipt(
    **values: Any,
) -> AdvisoryPriceProspectivePredictionReceiptV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_price_prospective_prediction_receipt_v1")
    seed = AdvisoryPriceProspectivePredictionReceiptV1.model_construct(
        receipt_sha256="0" * 64,
        **payload,
    )
    return AdvisoryPriceProspectivePredictionReceiptV1(
        receipt_sha256=canonical_json_sha256(seed.functional_payload()),
        **payload,
    )


def target_open_utc(target_trade_date: date) -> datetime:
    return datetime.combine(
        target_trade_date,
        datetime.min.time().replace(hour=9, minute=30),
        tzinfo=SHANGHAI,
    ).astimezone(timezone.utc)


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)
