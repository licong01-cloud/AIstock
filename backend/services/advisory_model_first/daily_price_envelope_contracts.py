from __future__ import annotations

import math
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DAILY_PRICE_ENVELOPE_SCHEMA_VERSION = "advisory_daily_price_envelope_v1"
DAILY_PRICE_OBJECTIVE_CONTRACT = "RISK_MANAGED_ADVISORY"
DAILY_PRICE_BASIS = "UNADJUSTED_CNY_DECISION_CLOSE"
DAILY_ENTRY_CONDITION = "NEXT_TRADING_DAY_VALID_OPEN"
DAILY_PROJECTION_CONDITION = (
    "NEXT_TRADING_DAY_VALID_OPEN_AT_PREDICTED_ENTRY_MID"
)
ENTRY_ADMISSION_MODEL_STATUS = "RETIRED_NON_IDENTIFIABLE"


class _FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AdvisoryPriceBandV1(_FrozenContract):
    low: float = Field(gt=0)
    high: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_order(self) -> "AdvisoryPriceBandV1":
        _require_finite(self.low, self.high)
        if self.low > self.high:
            raise ValueError("price band low must not exceed high")
        return self


class AdvisoryEntryPriceRangeV1(AdvisoryPriceBandV1):
    condition: Literal["NEXT_TRADING_DAY_VALID_OPEN"] = DAILY_ENTRY_CONDITION
    mid: float = Field(gt=0)

    @model_validator(mode="after")
    def validate_mid(self) -> "AdvisoryEntryPriceRangeV1":
        _require_finite(self.mid)
        if not self.low <= self.mid <= self.high:
            raise ValueError("entry price mid must be inside the price band")
        return self


class AdvisoryEntryGapCalibrationV1(_FrozenContract):
    state: Literal["CALIBRATED", "UNCALIBRATED"]
    method: str | None = None
    delta: float | None = Field(default=None, ge=0)
    nominal_coverage: float = Field(gt=0, lt=1)

    @model_validator(mode="after")
    def validate_state(self) -> "AdvisoryEntryGapCalibrationV1":
        if self.state == "CALIBRATED":
            if not self.method or self.delta is None:
                raise ValueError("calibrated entry gap requires method and delta")
            _require_finite(self.delta)
        elif self.method is not None or self.delta is not None:
            raise ValueError("uncalibrated entry gap cannot advertise calibration values")
        return self


class AdvisoryTakeProfitPriceV1(AdvisoryPriceBandV1):
    horizon_trade_days: Literal[1, 3, 5, 10, 20]


class AdvisoryProtectivePriceV1(_FrozenContract):
    status: Literal[
        "NOT_APPLICABLE",
        "MODEL_BELOW_POLICY_ACTIVATION",
        "AVAILABLE_CONDITIONAL_ON_POLICY_ACTIVATION",
    ]
    policy_activation_price: float | None = Field(default=None, gt=0)
    model_peak_low: float | None = Field(default=None, gt=0)
    model_peak_high: float | None = Field(default=None, gt=0)
    floor_low: float | None = Field(default=None, gt=0)
    floor_high: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_values(self) -> "AdvisoryProtectivePriceV1":
        values = (
            self.policy_activation_price,
            self.model_peak_low,
            self.model_peak_high,
            self.floor_low,
            self.floor_high,
        )
        _require_finite(*(value for value in values if value is not None))
        if self.status == "NOT_APPLICABLE":
            if any(value is not None for value in values):
                raise ValueError("not-applicable protective price must be empty")
            return self
        if self.policy_activation_price is None:
            raise ValueError("protective price requires policy activation price")
        if self.model_peak_low is None or self.model_peak_high is None:
            raise ValueError("protective price requires model peak range")
        if self.model_peak_low > self.model_peak_high:
            raise ValueError("protective model peak range is invalid")
        if self.status == "MODEL_BELOW_POLICY_ACTIVATION":
            if self.floor_low is not None or self.floor_high is not None:
                raise ValueError("below-activation protective price cannot expose floors")
            return self
        if self.floor_low is None or self.floor_high is None:
            raise ValueError("available protective price requires floor range")
        if self.floor_low > self.floor_high:
            raise ValueError("protective floor range is invalid")
        return self


class AdvisoryStopLossPriceV1(AdvisoryPriceBandV1):
    status: Literal["AVAILABLE", "SINGLE_POINT"]
    hard_stop_price: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_status(self) -> "AdvisoryStopLossPriceV1":
        if self.hard_stop_price is not None:
            _require_finite(self.hard_stop_price)
            if self.low < self.hard_stop_price:
                raise ValueError("stop-loss range cannot loosen the hard stop")
        if self.status == "SINGLE_POINT" and self.low != self.high:
            raise ValueError("single-point stop loss must have equal bounds")
        return self


class AdvisoryRegulatoryPriceRangeV1(_FrozenContract):
    status: Literal["LIMITED", "NO_DAILY_LIMIT"]
    low: float | None = Field(default=None, gt=0)
    high: float | None = Field(default=None, gt=0)
    rule_id: str = Field(min_length=1)
    source: Literal["DECISION_TIME_BOARD_ST_RULE"]

    @model_validator(mode="after")
    def validate_bounds(self) -> "AdvisoryRegulatoryPriceRangeV1":
        if self.status == "LIMITED":
            if self.low is None or self.high is None:
                raise ValueError("limited regulatory range requires both bounds")
            _require_finite(self.low, self.high)
            if self.low > self.high:
                raise ValueError("regulatory price range is invalid")
        elif self.low is not None or self.high is not None:
            raise ValueError("no-limit regulatory range cannot expose bounds")
        return self


class AdvisoryReviewPolicyV1(_FrozenContract):
    review_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stop_loss_bps: int = Field(ge=0)
    take_profit_bps: int = Field(ge=0)
    trailing_stop_bps: int = Field(ge=0)
    take_profit_mode: Literal["fixed", "trailing", "none"]


class AdvisoryDailyPriceEnvelopeCandidateV1(_FrozenContract):
    symbol: str = Field(min_length=1)
    status: Literal["EXPERIMENTAL_SHADOW", "PRICE_RANGE_UNAVAILABLE"]
    availability_status: Literal["AVAILABLE", "UNAVAILABLE"]
    projection_condition: Literal[
        "NEXT_TRADING_DAY_VALID_OPEN_AT_PREDICTED_ENTRY_MID"
    ] = DAILY_PROJECTION_CONDITION
    decision_reference_price: float | None = Field(default=None, gt=0)
    decision_price_trade_date: date | None = None
    target_raw_price_multiplier: float | None = Field(default=None, gt=0)
    entry_price_range: AdvisoryEntryPriceRangeV1 | None = None
    calibrated_entry_price_range: AdvisoryEntryPriceRangeV1 | None = None
    entry_gap_calibration: AdvisoryEntryGapCalibrationV1 | None = None
    take_profit_price: AdvisoryTakeProfitPriceV1 | None = None
    protective_price: AdvisoryProtectivePriceV1 | None = None
    stop_loss_price: AdvisoryStopLossPriceV1 | None = None
    tick_size: float | None = Field(default=None, gt=0)
    regulatory_price_range: AdvisoryRegulatoryPriceRangeV1 | None = None
    review_policy: AdvisoryReviewPolicyV1 | None = None
    reason_code: str | None = None
    message: str | None = None

    @model_validator(mode="after")
    def validate_availability(self) -> "AdvisoryDailyPriceEnvelopeCandidateV1":
        values = (
            self.decision_reference_price,
            self.target_raw_price_multiplier,
            self.tick_size,
        )
        _require_finite(*(value for value in values if value is not None))
        required = (
            self.decision_reference_price,
            self.decision_price_trade_date,
            self.target_raw_price_multiplier,
            self.entry_price_range,
            self.entry_gap_calibration,
            self.take_profit_price,
            self.protective_price,
            self.stop_loss_price,
            self.tick_size,
            self.regulatory_price_range,
            self.review_policy,
        )
        if self.availability_status == "AVAILABLE":
            if self.status != "EXPERIMENTAL_SHADOW" or any(
                value is None for value in required
            ):
                raise ValueError("available price candidate has incomplete payload")
            assert self.entry_gap_calibration is not None
            if (
                self.entry_gap_calibration.state == "CALIBRATED"
            ) != (self.calibrated_entry_price_range is not None):
                raise ValueError(
                    "candidate calibration state does not match calibrated range"
                )
            if self.reason_code is not None or self.message is not None:
                raise ValueError("available price candidate cannot expose an error")
            return self
        if self.status != "PRICE_RANGE_UNAVAILABLE":
            raise ValueError("unavailable price candidate has inconsistent status")
        if not self.reason_code or not self.message:
            raise ValueError("unavailable price candidate requires a typed error")
        if any(value is not None for value in required):
            raise ValueError("unavailable price candidate cannot expose partial prices")
        if self.calibrated_entry_price_range is not None:
            raise ValueError("unavailable price candidate cannot expose calibration output")
        return self


class AdvisoryDailyPriceEnvelopeV1(_FrozenContract):
    schema_version: Literal["advisory_daily_price_envelope_v1"] = (
        DAILY_PRICE_ENVELOPE_SCHEMA_VERSION
    )
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = (
        DAILY_PRICE_OBJECTIVE_CONTRACT
    )
    status: Literal["EXPERIMENTAL_SHADOW", "PRICE_RANGE_UNAVAILABLE"]
    availability_status: Literal["AVAILABLE", "PARTIAL", "UNAVAILABLE"]
    decision_as_of_trade_date: date | None = None
    target_trade_date: date | None = None
    price_basis: Literal["UNADJUSTED_CNY_DECISION_CLOSE"] = DAILY_PRICE_BASIS
    calibration_state: Literal["UNCALIBRATED", "CALIBRATED_INTERVAL"]
    nominal_coverage: float | None = Field(default=None, gt=0, lt=1)
    package_id: str | None = None
    package_manifest_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    style_profile_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    parent_bundle_id: str | None = None
    outcome_bundle_id: str | None = None
    price_range_bundle_id: str | None = None
    model_version: str | None = None
    review_policy_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    source_bundle_schema_version: str | None = None
    entry_admission_model_status: Literal["RETIRED_NON_IDENTIFIABLE"] = (
        ENTRY_ADMISSION_MODEL_STATUS
    )
    candidates: tuple[AdvisoryDailyPriceEnvelopeCandidateV1, ...] = ()
    reason_code: str | None = None
    message: str | None = None

    @model_validator(mode="after")
    def validate_envelope(self) -> "AdvisoryDailyPriceEnvelopeV1":
        if self.status == "PRICE_RANGE_UNAVAILABLE":
            if self.availability_status != "UNAVAILABLE" or self.candidates:
                raise ValueError("unavailable envelope cannot expose candidates")
            if not self.reason_code or not self.message:
                raise ValueError("unavailable envelope requires a typed error")
            return self

        identity = (
            self.decision_as_of_trade_date,
            self.target_trade_date,
            self.nominal_coverage,
            self.package_id,
            self.package_manifest_sha256,
            self.style_profile_hash,
            self.parent_bundle_id,
            self.outcome_bundle_id,
            self.price_range_bundle_id,
            self.model_version,
            self.review_policy_sha256,
            self.source_bundle_schema_version,
        )
        if any(value is None or value == "" for value in identity):
            raise ValueError("available envelope has incomplete identity")
        assert self.decision_as_of_trade_date is not None
        assert self.target_trade_date is not None
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("target trade date must follow decision trade date")
        if not self.candidates:
            raise ValueError("available envelope requires candidates")
        symbols = [candidate.symbol for candidate in self.candidates]
        if len(set(symbols)) != len(symbols):
            raise ValueError("available envelope contains duplicate candidate symbols")
        for candidate in self.candidates:
            if (
                candidate.decision_price_trade_date is not None
                and candidate.decision_price_trade_date > self.decision_as_of_trade_date
            ):
                raise ValueError(
                    "candidate decision price date cannot exceed decision trade date"
                )
            if candidate.availability_status != "AVAILABLE":
                continue
            assert candidate.entry_gap_calibration is not None
            assert candidate.review_policy is not None
            expected_candidate_state = (
                "CALIBRATED"
                if self.calibration_state == "CALIBRATED_INTERVAL"
                else "UNCALIBRATED"
            )
            if candidate.entry_gap_calibration.state != expected_candidate_state:
                raise ValueError(
                    "candidate calibration state differs from envelope calibration state"
                )
            if candidate.entry_gap_calibration.nominal_coverage != self.nominal_coverage:
                raise ValueError(
                    "candidate nominal coverage differs from envelope nominal coverage"
                )
            if candidate.review_policy.review_policy_sha256 != self.review_policy_sha256:
                raise ValueError(
                    "candidate review policy differs from envelope review policy"
                )
        available_count = sum(
            item.availability_status == "AVAILABLE" for item in self.candidates
        )
        expected_availability = (
            "AVAILABLE"
            if available_count == len(self.candidates)
            else "UNAVAILABLE"
            if available_count == 0
            else "PARTIAL"
        )
        if self.availability_status != expected_availability:
            raise ValueError("envelope availability does not match candidate statuses")
        if self.reason_code is not None or self.message is not None:
            raise ValueError("available envelope cannot expose a top-level error")
        return self

    def as_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _require_finite(*values: float) -> None:
    if not all(math.isfinite(float(value)) for value in values):
        raise ValueError("price envelope numeric values must be finite")
