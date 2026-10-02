from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
REQUEST_ID_PATTERN = r"^advprpros_[0-9a-f]{24}$"
SETTLEMENT_ID_PATTERN = r"^advprsett_[0-9a-f]{24}$"
SHANGHAI = ZoneInfo("Asia/Shanghai")


class _FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class AdvisoryPriceOutcomeRefreshAuditV1(_FrozenContract):
    dataset: Literal["kline_daily_raw", "suspend_d"]
    trade_date: date
    data_source: str = Field(min_length=1)
    status: Literal["success"] = "success"
    quality_status: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    refreshed_at: datetime

    @model_validator(mode="after")
    def validate_audit(self) -> "AdvisoryPriceOutcomeRefreshAuditV1":
        _aware_utc(self.refreshed_at, field="refreshed_at")
        if self.quality_status in {"error", "empty_invalid", "low_coverage"}:
            raise ValueError("outcome refresh audit quality is not usable")
        return self


class AdvisoryPriceProspectiveOutcomeCandidateV1(_FrozenContract):
    symbol: str = Field(min_length=1)
    model_prediction_status: Literal["AVAILABLE", "UNAVAILABLE"]
    model_prediction_reason: str | None = None
    market_outcome_status: Literal["AVAILABLE", "NOT_APPLICABLE"]
    market_outcome_reason: str = Field(min_length=1)
    actual_open: float | None = Field(default=None, gt=0.0)
    decision_reference_price: float | None = Field(default=None, gt=0.0)
    calibrated_low: float | None = Field(default=None, gt=0.0)
    calibrated_mid: float | None = Field(default=None, gt=0.0)
    calibrated_high: float | None = Field(default=None, gt=0.0)
    covered: bool | None = None
    lower_miss: bool | None = None
    upper_miss: bool | None = None
    interval_width_bps: float | None = Field(default=None, ge=0.0)
    absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)

    @model_validator(mode="after")
    def validate_states(self) -> "AdvisoryPriceProspectiveOutcomeCandidateV1":
        band = (
            self.decision_reference_price,
            self.calibrated_low,
            self.calibrated_mid,
            self.calibrated_high,
        )
        metrics = (
            self.covered,
            self.lower_miss,
            self.upper_miss,
            self.interval_width_bps,
            self.absolute_mid_error_bps,
        )
        if self.market_outcome_status == "NOT_APPLICABLE":
            if self.market_outcome_reason != "target_authoritatively_suspended":
                raise ValueError("not-applicable outcome requires the suspension reason")
            if self.actual_open is not None or any(value is not None for value in (*band, *metrics)):
                raise ValueError("not-applicable outcome cannot carry price metrics")
        elif self.actual_open is None:
            raise ValueError("available market outcome requires actual_open")
        elif self.market_outcome_reason != "target_open_observed":
            raise ValueError("available market outcome requires the observed-open reason")
        if self.model_prediction_status == "UNAVAILABLE":
            if not self.model_prediction_reason:
                raise ValueError("unavailable model prediction requires its typed reason")
            if any(value is not None for value in (*band, *metrics)):
                raise ValueError("unavailable model prediction cannot carry interval metrics")
            return self
        if self.model_prediction_reason is not None:
            raise ValueError("available model prediction cannot carry an unavailable reason")
        if self.market_outcome_status == "NOT_APPLICABLE":
            return self
        if any(value is None for value in (*band, *metrics)):
            raise ValueError("available prediction and outcome require complete interval metrics")
        assert self.decision_reference_price is not None
        assert self.actual_open is not None
        assert self.calibrated_low is not None
        assert self.calibrated_mid is not None
        assert self.calibrated_high is not None
        if not self.calibrated_low <= self.calibrated_mid <= self.calibrated_high:
            raise ValueError("calibrated price interval is crossed")
        if sum(bool(value) for value in (self.covered, self.lower_miss, self.upper_miss)) != 1:
            raise ValueError("coverage and miss indicators must be mutually exclusive")
        expected = (
            self.calibrated_low <= self.actual_open <= self.calibrated_high,
            self.actual_open < self.calibrated_low,
            self.actual_open > self.calibrated_high,
        )
        if (self.covered, self.lower_miss, self.upper_miss) != expected:
            raise ValueError("coverage indicators differ from the observed open")
        expected_width = (
            (self.calibrated_high - self.calibrated_low) / self.decision_reference_price * 10_000.0
        )
        expected_error = (
            abs(self.actual_open - self.calibrated_mid) / self.decision_reference_price * 10_000.0
        )
        assert self.interval_width_bps is not None
        assert self.absolute_mid_error_bps is not None
        if not math.isclose(self.interval_width_bps, expected_width, rel_tol=1e-12, abs_tol=1e-9):
            raise ValueError("interval width differs from the frozen prices")
        if not math.isclose(self.absolute_mid_error_bps, expected_error, rel_tol=1e-12, abs_tol=1e-9):
            raise ValueError("mid error differs from the observed open")
        return self


class AdvisoryPriceProspectiveSettlementMetricsV1(_FrozenContract):
    candidate_count: int = Field(gt=0)
    market_available_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)
    model_available_market_available_count: int = Field(ge=0)
    model_unavailable_count: int = Field(ge=0)
    calibrated_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    lower_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    upper_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    mean_interval_width_bps: float | None = Field(default=None, ge=0.0)
    median_interval_width_bps: float | None = Field(default=None, ge=0.0)
    mean_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    median_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    crossing_count: Literal[0] = 0

    @model_validator(mode="after")
    def validate_counts(self) -> "AdvisoryPriceProspectiveSettlementMetricsV1":
        if self.market_available_count + self.not_applicable_count != self.candidate_count:
            raise ValueError("market outcome counts do not add up")
        if self.model_available_market_available_count > self.market_available_count:
            raise ValueError("model metric count exceeds available market outcomes")
        described = (
            self.calibrated_coverage,
            self.lower_miss_rate,
            self.upper_miss_rate,
            self.mean_interval_width_bps,
            self.median_interval_width_bps,
            self.mean_absolute_mid_error_bps,
            self.median_absolute_mid_error_bps,
        )
        if self.model_available_market_available_count == 0:
            if any(value is not None for value in described):
                raise ValueError("empty metric support cannot carry descriptive metrics")
        elif any(value is None for value in described):
            raise ValueError("non-empty metric support requires complete descriptive metrics")
        return self


class AdvisoryPriceProspectiveSettlementV1(_FrozenContract):
    schema_version: Literal["advisory_price_prospective_settlement_v1"] = (
        "advisory_price_prospective_settlement_v1"
    )
    settlement_id: str = Field(pattern=SETTLEMENT_ID_PATTERN)
    settlement_sha256: str = Field(pattern=SHA256_PATTERN)
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    prediction_bundle_id: str = Field(pattern=SHA256_PATTERN)
    prediction_sha256: str = Field(pattern=SHA256_PATTERN)
    price_range_bundle_id: str = Field(pattern=SHA256_PATTERN)
    package_id: str = Field(min_length=1)
    review_policy_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_as_of_trade_date: date
    target_trade_date: date
    predicted_at: datetime
    settled_at: datetime
    refresh_audits: tuple[AdvisoryPriceOutcomeRefreshAuditV1, AdvisoryPriceOutcomeRefreshAuditV1]
    candidates: tuple[AdvisoryPriceProspectiveOutcomeCandidateV1, ...]
    metrics: AdvisoryPriceProspectiveSettlementMetricsV1
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    evidence_level: Literal["PROSPECTIVE_OOS_SETTLED"] = "PROSPECTIVE_OOS_SETTLED"
    realized_outcome_accessed: Literal[True] = True
    database_written: Literal[False] = False
    binding_activated: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False

    @model_validator(mode="after")
    def validate_settlement(self) -> "AdvisoryPriceProspectiveSettlementV1":
        predicted = _aware_utc(self.predicted_at, field="predicted_at")
        settled = _aware_utc(self.settled_at, field="settled_at")
        if settled <= predicted:
            raise ValueError("settlement must occur after prediction")
        if self.decision_as_of_trade_date >= self.target_trade_date:
            raise ValueError("settlement decision date must precede target date")
        target_open = datetime.combine(
            self.target_trade_date,
            time(hour=9, minute=30),
            tzinfo=SHANGHAI,
        ).astimezone(timezone.utc)
        settlement_not_before = datetime.combine(
            self.target_trade_date,
            time(hour=18),
            tzinfo=SHANGHAI,
        ).astimezone(timezone.utc)
        if predicted >= target_open:
            raise ValueError("settlement source prediction was not published before target open")
        if settled < settlement_not_before:
            raise ValueError("settlement occurred before the target-date maturity clock")
        if len(self.candidates) != self.metrics.candidate_count:
            raise ValueError("settlement candidate count mismatch")
        symbols = tuple(row.symbol for row in self.candidates)
        if symbols != tuple(sorted(set(symbols))):
            raise ValueError("settlement candidates must be uniquely sorted")
        if tuple(row.dataset for row in self.refresh_audits) != ("kline_daily_raw", "suspend_d"):
            raise ValueError("settlement requires kline and suspend refresh audits")
        if any(row.trade_date != self.target_trade_date for row in self.refresh_audits):
            raise ValueError("refresh audit date differs from target date")
        if any(_aware_utc(row.refreshed_at, field="refreshed_at") > settled for row in self.refresh_audits):
            raise ValueError("refresh audit cannot postdate settlement")
        digest = canonical_json_sha256(self.functional_payload())
        if self.settlement_sha256 != digest or self.settlement_id != f"advprsett_{digest[:24]}":
            raise ValueError("settlement identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"settlement_id", "settlement_sha256"})


class AdvisoryPriceProspectiveSettlementReceiptV1(_FrozenContract):
    schema_version: Literal["advisory_price_prospective_settlement_receipt_v1"] = (
        "advisory_price_prospective_settlement_receipt_v1"
    )
    status: Literal["PUBLISHED", "ALREADY_MATERIALIZED"]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    settlement_id: str = Field(pattern=SETTLEMENT_ID_PATTERN)
    settlement_sha256: str = Field(pattern=SHA256_PATTERN)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    published_at: datetime
    target_trade_date: date
    candidate_count: int = Field(gt=0)
    market_available_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)
    realized_outcome_accessed: Literal[True] = True
    database_written: Literal[False] = False
    binding_activated: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryPriceProspectiveSettlementReceiptV1":
        _aware_utc(self.published_at, field="published_at")
        if self.market_available_count + self.not_applicable_count != self.candidate_count:
            raise ValueError("settlement receipt counts do not add up")
        if self.receipt_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("settlement receipt hash mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"status", "receipt_sha256"})


class AdvisoryPriceProspectiveConfirmationV1(_FrozenContract):
    schema_version: Literal["advisory_price_prospective_confirmation_v1"] = (
        "advisory_price_prospective_confirmation_v1"
    )
    confirmation_sha256: str = Field(pattern=SHA256_PATTERN)
    status: Literal["ACCUMULATING", "CONFIRMATION_EVIDENCE_READY"]
    price_range_bundle_id: str = Field(pattern=SHA256_PATTERN)
    package_id: str | None = None
    review_policy_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    target_trade_dates: tuple[date, ...]
    target_date_count: int = Field(ge=0)
    available_candidate_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)
    support_date_count: int = Field(ge=0)
    support_date_ratio: float = Field(ge=0.0, le=1.0)
    minimum_target_dates: Literal[20] = 20
    minimum_available_candidates: Literal[300] = 300
    minimum_candidates_per_supported_date: Literal[5] = 5
    minimum_supported_date_ratio: Literal[0.8] = 0.8
    support_gaps: tuple[str, ...]
    metrics: dict[str, float] | None = None
    cluster_bootstrap: dict[str, float | str] | None = None
    activation_recommended: Literal[False] = False
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    sealed_holdout_consumed: Literal[False] = False

    @model_validator(mode="after")
    def validate_confirmation(self) -> "AdvisoryPriceProspectiveConfirmationV1":
        if self.target_date_count != len(self.target_trade_dates):
            raise ValueError("confirmation target-date count mismatch")
        if tuple(sorted(set(self.target_trade_dates))) != self.target_trade_dates:
            raise ValueError("confirmation target dates must be uniquely sorted")
        ready = self.status == "CONFIRMATION_EVIDENCE_READY"
        if ready != (not self.support_gaps):
            raise ValueError("confirmation status differs from support gaps")
        if ready:
            if self.package_id is None or self.review_policy_sha256 is None:
                raise ValueError("ready confirmation requires frozen lineage")
            if self.metrics is None or self.cluster_bootstrap is None:
                raise ValueError("ready confirmation requires metrics and bootstrap")
        elif self.metrics is not None or self.cluster_bootstrap is not None:
            raise ValueError("accumulating confirmation cannot expose result metrics")
        if self.confirmation_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("confirmation hash mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"confirmation_sha256"})


def build_settlement(**values: Any) -> AdvisoryPriceProspectiveSettlementV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_price_prospective_settlement_v1")
    seed = AdvisoryPriceProspectiveSettlementV1.model_construct(
        settlement_id="advprsett_" + "0" * 24,
        settlement_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return AdvisoryPriceProspectiveSettlementV1(
        settlement_id=f"advprsett_{digest[:24]}",
        settlement_sha256=digest,
        **payload,
    )


def build_settlement_receipt(**values: Any) -> AdvisoryPriceProspectiveSettlementReceiptV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_price_prospective_settlement_receipt_v1")
    seed = AdvisoryPriceProspectiveSettlementReceiptV1.model_construct(
        receipt_sha256="0" * 64,
        **payload,
    )
    return AdvisoryPriceProspectiveSettlementReceiptV1(
        receipt_sha256=canonical_json_sha256(seed.functional_payload()),
        **payload,
    )


def build_confirmation(**values: Any) -> AdvisoryPriceProspectiveConfirmationV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_price_prospective_confirmation_v1")
    seed = AdvisoryPriceProspectiveConfirmationV1.model_construct(
        confirmation_sha256="0" * 64,
        **payload,
    )
    return AdvisoryPriceProspectiveConfirmationV1(
        confirmation_sha256=canonical_json_sha256(seed.functional_payload()),
        **payload,
    )


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)
