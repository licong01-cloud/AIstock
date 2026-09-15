from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.price_range_contracts import (
    canonical_json_sha256,
)


SHA256_PATTERN = r"^[0-9a-f]{64}$"
REPLAY_ID_PATTERN = r"^advprhist_[0-9a-f]{24}$"


class _FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class AdvisoryHistoricalPriceReplayRequestV1(_FrozenContract):
    schema_version: Literal["advisory_historical_price_replay_request_v1"] = (
        "advisory_historical_price_replay_request_v1"
    )
    replay_id: str = Field(pattern=REPLAY_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    price_range_bundle_id: str = Field(pattern=SHA256_PATTERN)
    price_range_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    prediction_source_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_start_trade_date: date
    decision_end_trade_date: date
    replay_as_of_date: date
    pit_universe_key: str = Field(min_length=1)
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    evidence_level: Literal["HISTORICAL_REPLAY"] = "HISTORICAL_REPLAY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    window_usage: Literal["CONSUMED_DEVELOPMENT_WINDOW"] = "CONSUMED_DEVELOPMENT_WINDOW"
    prediction_source: Literal["FROZEN_V4_TEST_PREDICTIONS"] = "FROZEN_V4_TEST_PREDICTIONS"
    metric_semantics_version: Literal["MODEL_SPACE_AND_BUSINESS_PRICE_V1"] = "MODEL_SPACE_AND_BUSINESS_PRICE_V1"
    database_written: Literal[False] = False
    binding_activated: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False

    @model_validator(mode="after")
    def validate_request(self) -> "AdvisoryHistoricalPriceReplayRequestV1":
        if self.decision_start_trade_date > self.decision_end_trade_date:
            raise ValueError("historical replay decision range is reversed")
        if self.decision_end_trade_date >= self.replay_as_of_date:
            raise ValueError("historical replay window must end before replay as-of date")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.replay_id != f"advprhist_{digest[:24]}":
            raise ValueError("historical replay request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"replay_id", "request_sha256"})


class AdvisoryHistoricalPricePredictionRowV1(_FrozenContract):
    decision_as_of_trade_date: date
    target_trade_date: date
    symbol: str = Field(min_length=1, max_length=32)
    calibrated_gap_q10: float = Field(gt=-1.0)
    calibrated_gap_q50: float = Field(gt=-1.0)
    calibrated_gap_q90: float = Field(gt=-1.0)

    @model_validator(mode="after")
    def validate_prediction(self) -> "AdvisoryHistoricalPricePredictionRowV1":
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("historical prediction target must follow decision date")
        if not self.calibrated_gap_q10 <= self.calibrated_gap_q50 <= self.calibrated_gap_q90:
            raise ValueError("historical prediction quantiles are crossed")
        return self


class AdvisoryHistoricalPriceOutcomeRowV1(_FrozenContract):
    decision_as_of_trade_date: date
    target_trade_date: date
    symbol: str = Field(min_length=1, max_length=32)
    model_prediction_status: Literal["AVAILABLE", "UNAVAILABLE"]
    model_prediction_reason: str | None = None
    market_outcome_status: Literal["AVAILABLE", "NOT_APPLICABLE", "UNAVAILABLE"]
    market_outcome_reason: str = Field(min_length=1)
    actual_open: float | None = Field(default=None, gt=0.0)
    decision_reference_price: float | None = Field(default=None, gt=0.0)
    calibrated_low: float | None = Field(default=None, gt=0.0)
    calibrated_mid: float | None = Field(default=None, gt=0.0)
    calibrated_high: float | None = Field(default=None, gt=0.0)
    calibrated_gap_q10: float | None = Field(default=None, gt=-1.0)
    calibrated_gap_q50: float | None = Field(default=None, gt=-1.0)
    calibrated_gap_q90: float | None = Field(default=None, gt=-1.0)
    actual_entry_gap_return: float | None = Field(default=None, gt=-1.0)
    model_space_covered: bool | None = None
    model_space_lower_miss: bool | None = None
    model_space_upper_miss: bool | None = None
    covered: bool | None = None
    lower_miss: bool | None = None
    upper_miss: bool | None = None
    interval_width_bps: float | None = Field(default=None, ge=0.0)
    absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)

    @model_validator(mode="after")
    def validate_outcome(self) -> "AdvisoryHistoricalPriceOutcomeRowV1":
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("historical outcome target must follow decision date")
        band = (
            self.decision_reference_price,
            self.calibrated_low,
            self.calibrated_mid,
            self.calibrated_high,
        )
        metrics = (
            self.calibrated_gap_q10,
            self.calibrated_gap_q50,
            self.calibrated_gap_q90,
            self.actual_entry_gap_return,
            self.model_space_covered,
            self.model_space_lower_miss,
            self.model_space_upper_miss,
            self.covered,
            self.lower_miss,
            self.upper_miss,
            self.interval_width_bps,
            self.absolute_mid_error_bps,
        )
        if self.market_outcome_status == "NOT_APPLICABLE":
            if self.market_outcome_reason != "target_authoritatively_suspended":
                raise ValueError("not-applicable outcome requires suspension reason")
            if self.actual_open is not None or any(value is not None for value in (*band, *metrics)):
                raise ValueError("not-applicable outcome cannot carry prices")
            if self.model_prediction_status != "AVAILABLE" or self.model_prediction_reason is not None:
                raise ValueError("not-applicable outcome must preserve the frozen prediction")
            return self
        if self.market_outcome_status == "UNAVAILABLE":
            if self.actual_open is not None or any(value is not None for value in (*band, *metrics)):
                raise ValueError("unavailable outcome cannot carry prices")
            if self.model_prediction_status != "AVAILABLE" or self.model_prediction_reason is not None:
                raise ValueError("unavailable outcome must preserve the frozen prediction")
            return self
        if self.actual_open is None:
            raise ValueError("available outcome requires target open")
        if self.model_prediction_status == "UNAVAILABLE":
            if not self.model_prediction_reason:
                raise ValueError("unavailable prediction requires typed reason")
            if any(value is not None for value in (*band, *metrics)):
                raise ValueError("unavailable prediction cannot carry interval metrics")
            return self
        if self.model_prediction_reason is not None or any(value is None for value in (*band, *metrics)):
            raise ValueError("available prediction requires complete interval metrics")
        assert self.decision_reference_price is not None
        assert self.calibrated_low is not None
        assert self.calibrated_mid is not None
        assert self.calibrated_high is not None
        assert self.actual_open is not None
        if not self.calibrated_low <= self.calibrated_mid <= self.calibrated_high:
            raise ValueError("historical replay price interval is crossed")
        assert self.calibrated_gap_q10 is not None
        assert self.calibrated_gap_q50 is not None
        assert self.calibrated_gap_q90 is not None
        assert self.actual_entry_gap_return is not None
        if not self.calibrated_gap_q10 <= self.calibrated_gap_q50 <= self.calibrated_gap_q90:
            raise ValueError("historical replay gap interval is crossed")
        expected_model_flags = (
            self.calibrated_gap_q10 <= self.actual_entry_gap_return <= self.calibrated_gap_q90,
            self.actual_entry_gap_return < self.calibrated_gap_q10,
            self.actual_entry_gap_return > self.calibrated_gap_q90,
        )
        if (
            self.model_space_covered,
            self.model_space_lower_miss,
            self.model_space_upper_miss,
        ) != expected_model_flags:
            raise ValueError("historical replay model-space flags differ from outcome")
        expected_flags = (
            self.calibrated_low <= self.actual_open <= self.calibrated_high,
            self.actual_open < self.calibrated_low,
            self.actual_open > self.calibrated_high,
        )
        if (self.covered, self.lower_miss, self.upper_miss) != expected_flags:
            raise ValueError("historical replay coverage flags differ from outcome")
        expected_width = (self.calibrated_high - self.calibrated_low) / self.decision_reference_price * 10_000.0
        expected_error = abs(self.actual_open - self.calibrated_mid) / self.decision_reference_price * 10_000.0
        assert self.interval_width_bps is not None
        assert self.absolute_mid_error_bps is not None
        if not math.isclose(self.interval_width_bps, expected_width, rel_tol=1e-12, abs_tol=1e-9):
            raise ValueError("historical replay width differs from frozen prices")
        if not math.isclose(self.absolute_mid_error_bps, expected_error, rel_tol=1e-12, abs_tol=1e-9):
            raise ValueError("historical replay mid error differs from outcome")
        return self


class AdvisoryHistoricalPriceReplayMetricsV1(_FrozenContract):
    decision_date_count: int = Field(gt=0)
    candidate_count: int = Field(gt=0)
    market_available_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)
    market_unavailable_count: int = Field(ge=0)
    model_available_market_available_count: int = Field(ge=0)
    model_unavailable_count: int = Field(ge=0)
    calibrated_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    lower_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    upper_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    mean_interval_width_bps: float | None = Field(default=None, ge=0.0)
    median_interval_width_bps: float | None = Field(default=None, ge=0.0)
    mean_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    median_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    model_space_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    model_space_lower_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    model_space_upper_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    tick_rounding_rescue_count: int = Field(ge=0)
    tick_rounding_harm_count: int = Field(ge=0)
    crossing_count: Literal[0] = 0

    @model_validator(mode="after")
    def validate_metrics(self) -> "AdvisoryHistoricalPriceReplayMetricsV1":
        if (
            self.market_available_count + self.not_applicable_count + self.market_unavailable_count
            != self.candidate_count
        ):
            raise ValueError("historical replay market counts do not add up")
        if self.decision_date_count > self.candidate_count:
            raise ValueError("historical replay decision count exceeds candidate count")
        if self.model_available_market_available_count + self.model_unavailable_count != self.market_available_count:
            raise ValueError("historical replay model support does not match available market rows")
        if (
            self.tick_rounding_rescue_count > self.model_available_market_available_count
            or self.tick_rounding_harm_count > self.model_available_market_available_count
        ):
            raise ValueError("historical replay tick effects exceed supported rows")
        described = (
            self.calibrated_coverage,
            self.lower_miss_rate,
            self.upper_miss_rate,
            self.mean_interval_width_bps,
            self.median_interval_width_bps,
            self.mean_absolute_mid_error_bps,
            self.median_absolute_mid_error_bps,
            self.model_space_coverage,
            self.model_space_lower_miss_rate,
            self.model_space_upper_miss_rate,
        )
        if self.model_available_market_available_count == 0:
            if any(value is not None for value in described):
                raise ValueError("empty replay support cannot carry metrics")
        elif any(value is None for value in described):
            raise ValueError("non-empty replay support requires complete metrics")
        else:
            assert self.calibrated_coverage is not None
            assert self.lower_miss_rate is not None
            assert self.upper_miss_rate is not None
            assert self.model_space_coverage is not None
            assert self.model_space_lower_miss_rate is not None
            assert self.model_space_upper_miss_rate is not None
            if not math.isclose(
                self.calibrated_coverage + self.lower_miss_rate + self.upper_miss_rate,
                1.0,
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError("historical replay business-space rates do not partition support")
            if not math.isclose(
                self.model_space_coverage + self.model_space_lower_miss_rate + self.model_space_upper_miss_rate,
                1.0,
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError("historical replay model-space rates do not partition support")
        return self


class AdvisoryHistoricalPriceReplayReceiptV1(_FrozenContract):
    schema_version: Literal["advisory_historical_price_replay_receipt_v1"] = (
        "advisory_historical_price_replay_receipt_v1"
    )
    status: Literal["PUBLISHED", "ALREADY_MATERIALIZED"]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    replay_id: str = Field(pattern=REPLAY_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    prediction_snapshot_sha256: str = Field(pattern=SHA256_PATTERN)
    outcome_sha256: str = Field(pattern=SHA256_PATTERN)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    published_at: datetime
    metrics: AdvisoryHistoricalPriceReplayMetricsV1
    evidence_level: Literal["HISTORICAL_REPLAY"] = "HISTORICAL_REPLAY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    realized_outcome_accessed: Literal[True] = True
    database_written: Literal[False] = False
    binding_activated: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdvisoryHistoricalPriceReplayReceiptV1":
        if self.published_at.tzinfo is None or self.published_at.utcoffset() is None:
            raise ValueError("historical replay published_at must be timezone-aware")
        if self.receipt_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("historical replay receipt hash mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"status", "receipt_sha256"})


def build_historical_price_replay_request(**values: Any) -> AdvisoryHistoricalPriceReplayRequestV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_historical_price_replay_request_v1")
    seed = AdvisoryHistoricalPriceReplayRequestV1.model_construct(
        replay_id="advprhist_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return AdvisoryHistoricalPriceReplayRequestV1(
        replay_id=f"advprhist_{digest[:24]}", request_sha256=digest, **payload
    )


def build_historical_price_replay_receipt(**values: Any) -> AdvisoryHistoricalPriceReplayReceiptV1:
    payload = dict(values)
    payload.setdefault("schema_version", "advisory_historical_price_replay_receipt_v1")
    seed = AdvisoryHistoricalPriceReplayReceiptV1.model_construct(receipt_sha256="0" * 64, **payload)
    return AdvisoryHistoricalPriceReplayReceiptV1(
        receipt_sha256=canonical_json_sha256(seed.functional_payload()), **payload
    )
