from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.price_range_contracts import canonical_json_sha256


SHA256_PATTERN = r"^[0-9a-f]{64}$"
REQUEST_ID_PATTERN = r"^advpradapt_[0-9a-f]{24}$"
POLICY_VERSION = "advisory_adaptive_price_calibration_policy_v1"
STATIC_ARM_ID = "STATIC_V4_CONTROL"
ADAPTIVE_ARM_ID = "ROLLING_20D_MATURED_CQR"
NOMINAL_COVERAGE = 0.8
LOOKBACK_TARGET_DATES = 20
MIN_FIT_TARGET_DATES = 5
MIN_FIT_ROWS = 100
BOOTSTRAP_SEED = 20260916
BOOTSTRAP_SAMPLES = 5000


class _FrozenContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class FrozenAdaptivePriceCalibrationRequestV1(_FrozenContract):
    schema_version: Literal["frozen_advisory_adaptive_price_calibration_request_v1"] = (
        "frozen_advisory_adaptive_price_calibration_request_v1"
    )
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    created_at: datetime
    output_root: str
    registry_path: str
    source_replay_root: str
    source_replay_id: str = Field(pattern=r"^advprhist_[0-9a-f]{24}$")
    source_request_file_sha256: str = Field(pattern=SHA256_PATTERN)
    source_prediction_file_sha256: str = Field(pattern=SHA256_PATTERN)
    source_outcome_file_sha256: str = Field(pattern=SHA256_PATTERN)
    source_manifest_file_sha256: str = Field(pattern=SHA256_PATTERN)
    source_receipt_file_sha256: str = Field(pattern=SHA256_PATTERN)
    source_prediction_snapshot_sha256: str = Field(pattern=SHA256_PATTERN)
    source_outcome_sha256: str = Field(pattern=SHA256_PATTERN)
    source_price_range_bundle_id: str = Field(pattern=SHA256_PATTERN)
    source_price_range_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    decision_start_trade_date: date
    decision_end_trade_date: date
    dataset_identity: str = Field(pattern=SHA256_PATTERN)
    schema_identity: Literal["ADVISORY_ADAPTIVE_PRICE_CALIBRATION_RESULT_V1"] = (
        "ADVISORY_ADAPTIVE_PRICE_CALIBRATION_RESULT_V1"
    )
    policy_identity: str = Field(pattern=SHA256_PATTERN)
    policy_version: Literal["advisory_adaptive_price_calibration_policy_v1"] = POLICY_VERSION
    static_arm_id: Literal["STATIC_V4_CONTROL"] = STATIC_ARM_ID
    adaptive_arm_id: Literal["ROLLING_20D_MATURED_CQR"] = ADAPTIVE_ARM_ID
    nominal_coverage: Literal[0.8] = NOMINAL_COVERAGE
    lookback_target_dates: Literal[20] = LOOKBACK_TARGET_DATES
    min_fit_target_dates: Literal[5] = MIN_FIT_TARGET_DATES
    min_fit_rows: Literal[100] = MIN_FIT_ROWS
    bootstrap_seed: Literal[20260916] = BOOTSTRAP_SEED
    bootstrap_samples: Literal[5000] = BOOTSTRAP_SAMPLES
    planned_trial_count: Literal[2] = 2
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    window_state: Literal["HISTORICAL_REPLAY_CONSUMED"] = "HISTORICAL_REPLAY_CONSUMED"
    binding_activated: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False
    database_written: Literal[False] = False
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    resource_max_rss_bytes: Literal[8589934592] = 8 * 1024**3

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenAdaptivePriceCalibrationRequestV1":
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("adaptive calibration created_at must be timezone-aware")
        if self.decision_start_trade_date > self.decision_end_trade_date:
            raise ValueError("adaptive calibration decision window is reversed")
        output_root = Path(self.output_root)
        source_root = Path(self.source_replay_root)
        registry_path = Path(self.registry_path)
        if not output_root.is_absolute() or not source_root.is_absolute() or not registry_path.is_absolute():
            raise ValueError("adaptive calibration paths must be absolute")
        try:
            source_root.resolve().relative_to((output_root.resolve() / "price_range_historical_replays"))
        except ValueError as exc:
            raise ValueError("adaptive calibration source replay escapes model root") from exc
        if source_root.name != self.source_replay_id:
            raise ValueError("adaptive calibration source replay path differs from replay id")
        if self.policy_identity != adaptive_policy_identity():
            raise ValueError("adaptive calibration policy identity mismatch")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"advpradapt_{expected[:24]}":
            raise ValueError("adaptive calibration request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at"},
        )


class AdaptivePriceArmMetricsV1(_FrozenContract):
    decision_date_count: int = Field(ge=0)
    candidate_count: int = Field(ge=0)
    market_available_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)
    market_unavailable_count: int = Field(ge=0)
    supported_row_count: int = Field(ge=0)
    model_unavailable_count: int = Field(ge=0)
    model_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    model_lower_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    model_upper_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    model_mean_width_bps: float | None = Field(default=None, ge=0.0)
    model_median_width_bps: float | None = Field(default=None, ge=0.0)
    model_mean_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    model_median_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    business_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    business_lower_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    business_upper_miss_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    business_mean_width_bps: float | None = Field(default=None, ge=0.0)
    business_median_width_bps: float | None = Field(default=None, ge=0.0)
    business_mean_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    business_median_absolute_mid_error_bps: float | None = Field(default=None, ge=0.0)
    crossing_count: Literal[0] = 0

    @model_validator(mode="after")
    def validate_metrics(self) -> "AdaptivePriceArmMetricsV1":
        values = tuple(
            getattr(self, name)
            for name in type(self).model_fields
            if name
            not in {
                "decision_date_count",
                "candidate_count",
                "market_available_count",
                "not_applicable_count",
                "market_unavailable_count",
                "supported_row_count",
                "model_unavailable_count",
                "crossing_count",
            }
        )
        if self.market_available_count + self.not_applicable_count + self.market_unavailable_count != self.candidate_count:
            raise ValueError("adaptive arm market counts do not add up")
        if self.supported_row_count + self.model_unavailable_count != self.market_available_count:
            raise ValueError("adaptive arm model support does not match market support")
        if (self.candidate_count == 0) != (self.decision_date_count == 0):
            raise ValueError("adaptive arm date and candidate support disagree")
        if self.decision_date_count > self.candidate_count:
            raise ValueError("adaptive arm decision dates exceed candidate rows")
        if self.supported_row_count == 0:
            if any(value is not None for value in values):
                raise ValueError("empty adaptive arm support cannot carry metrics")
            return self
        if self.decision_date_count <= 0 or any(value is None for value in values):
            raise ValueError("non-empty adaptive arm support requires complete metrics")
        assert self.model_coverage is not None
        assert self.model_lower_miss_rate is not None
        assert self.model_upper_miss_rate is not None
        assert self.business_coverage is not None
        assert self.business_lower_miss_rate is not None
        assert self.business_upper_miss_rate is not None
        for rates in (
            (self.model_coverage, self.model_lower_miss_rate, self.model_upper_miss_rate),
            (self.business_coverage, self.business_lower_miss_rate, self.business_upper_miss_rate),
        ):
            if abs(sum(rates) - 1.0) > 1e-12:
                raise ValueError("adaptive arm rates do not partition support")
        return self


class AdaptivePriceDailyStateV1(_FrozenContract):
    decision_as_of_trade_date: date
    target_trade_date: date
    mode: Literal["WARMUP_STATIC_FALLBACK", "ADAPTIVE_ACTIVE"]
    delta: float = Field(ge=0.0)
    fit_target_date_count: int = Field(ge=0, le=LOOKBACK_TARGET_DATES)
    fit_row_count: int = Field(ge=0)
    fit_target_start: date | None = None
    fit_target_end: date | None = None

    @model_validator(mode="after")
    def validate_state(self) -> "AdaptivePriceDailyStateV1":
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("adaptive target date must follow decision date")
        if (self.fit_target_start is None) != (self.fit_target_end is None):
            raise ValueError("adaptive fit date bounds must be both present or absent")
        if self.fit_target_start is not None:
            assert self.fit_target_end is not None
            if self.fit_target_start > self.fit_target_end or self.fit_target_end > self.decision_as_of_trade_date:
                raise ValueError("adaptive fit dates violate the PIT clock")
        if self.mode == "ADAPTIVE_ACTIVE" and (
            self.fit_target_date_count < MIN_FIT_TARGET_DATES or self.fit_row_count < MIN_FIT_ROWS
        ):
            raise ValueError("adaptive-active state lacks frozen support")
        if self.mode == "WARMUP_STATIC_FALLBACK" and self.delta != 0.0:
            raise ValueError("warm-up fallback must use zero delta")
        return self


class AdaptivePriceBootstrapV1(_FrozenContract):
    cluster_count: int = Field(gt=0)
    sample_count: Literal[5000] = BOOTSTRAP_SAMPLES
    seed: Literal[20260916] = BOOTSTRAP_SEED
    point: float
    lower_95: float
    upper_95: float

    @model_validator(mode="after")
    def validate_interval(self) -> "AdaptivePriceBootstrapV1":
        if not self.lower_95 <= self.point <= self.upper_95:
            raise ValueError("adaptive bootstrap interval does not contain its point")
        return self


class AdaptivePriceNavigationGateV1(_FrozenContract):
    support_pass: bool
    model_coverage_improvement_pass: bool
    bootstrap_lower_bound_pass: bool
    model_width_pass: bool
    miss_rates_pass: bool
    business_metrics_pass: bool
    identity_pass: bool
    all_pass: bool
    selected_candidate: Literal["ROLLING_20D_MATURED_CQR"] | None

    @model_validator(mode="after")
    def validate_gate(self) -> "AdaptivePriceNavigationGateV1":
        expected = all(
            (
                self.support_pass,
                self.model_coverage_improvement_pass,
                self.bootstrap_lower_bound_pass,
                self.model_width_pass,
                self.miss_rates_pass,
                self.business_metrics_pass,
                self.identity_pass,
            )
        )
        if self.all_pass != expected:
            raise ValueError("adaptive navigation gate summary is inconsistent")
        if (self.selected_candidate is not None) != self.all_pass:
            raise ValueError("adaptive selected candidate differs from gate result")
        return self


class AdaptivePriceCalibrationResultV1(_FrozenContract):
    schema_version: Literal["advisory_adaptive_price_calibration_result_v1"] = (
        "advisory_adaptive_price_calibration_result_v1"
    )
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    source_replay_id: str = Field(pattern=r"^advprhist_[0-9a-f]{24}$")
    evidence_level: Literal["HISTORICAL_REPLAY"] = "HISTORICAL_REPLAY"
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    result_class: Literal["EXPLORATORY"] = "EXPLORATORY"
    static_all: AdaptivePriceArmMetricsV1
    adaptive_all: AdaptivePriceArmMetricsV1
    static_active: AdaptivePriceArmMetricsV1
    adaptive_active: AdaptivePriceArmMetricsV1
    daily_states: tuple[AdaptivePriceDailyStateV1, ...]
    coverage_error_improvement: float = Field(ge=-1.0, le=1.0)
    model_width_ratio: float = Field(ge=0.0)
    business_width_ratio: float = Field(ge=0.0)
    tick_rounding_rescue_count: int = Field(ge=0)
    tick_rounding_harm_count: int = Field(ge=0)
    bootstrap: AdaptivePriceBootstrapV1
    gate: AdaptivePriceNavigationGateV1
    stage_timings_seconds: dict[str, float]
    peak_rss_bytes: int = Field(gt=0, le=8 * 1024**3)
    activation_recommended: Literal[False] = False
    binding_activated: Literal[False] = False
    database_written: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False
    result_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_result(self) -> "AdaptivePriceCalibrationResultV1":
        if not self.daily_states:
            raise ValueError("adaptive calibration result requires daily states")
        dates = [item.decision_as_of_trade_date for item in self.daily_states]
        if dates != sorted(set(dates)):
            raise ValueError("adaptive daily states must be unique and ordered")
        if set(self.stage_timings_seconds) != {
            "source_validation",
            "chronological_evaluation",
            "total_before_publish",
        } or any(value < 0.0 for value in self.stage_timings_seconds.values()):
            raise ValueError("adaptive stage timings are incomplete or negative")
        if self.static_all.candidate_count != self.adaptive_all.candidate_count:
            raise ValueError("adaptive all-row arms have different candidate support")
        if self.static_active.candidate_count != self.adaptive_active.candidate_count:
            raise ValueError("adaptive active arms have different candidate support")
        if len(dates) != self.static_all.decision_date_count:
            raise ValueError("adaptive daily states differ from all-row metrics")
        active_count = sum(item.mode == "ADAPTIVE_ACTIVE" for item in self.daily_states)
        if active_count != self.static_active.decision_date_count:
            raise ValueError("adaptive daily states differ from active metrics")
        if self.bootstrap.cluster_count > self.static_active.decision_date_count:
            raise ValueError("adaptive bootstrap exceeds active decision support")
        if self.tick_rounding_rescue_count > self.static_all.candidate_count or self.tick_rounding_harm_count > self.static_all.candidate_count:
            raise ValueError("adaptive tick effects exceed candidate support")
        expected_coverage_improvement = _coverage_error_improvement(
            self.static_active.model_coverage,
            self.adaptive_active.model_coverage,
        )
        expected_model_ratio = _metric_ratio(
            self.adaptive_active.model_mean_width_bps,
            self.static_active.model_mean_width_bps,
        )
        expected_business_ratio = _metric_ratio(
            self.adaptive_active.business_mean_width_bps,
            self.static_active.business_mean_width_bps,
        )
        for actual, expected, name in (
            (self.coverage_error_improvement, expected_coverage_improvement, "coverage improvement"),
            (self.model_width_ratio, expected_model_ratio, "model width ratio"),
            (self.business_width_ratio, expected_business_ratio, "business width ratio"),
        ):
            if not abs(actual - expected) <= 1e-12:
                raise ValueError(f"adaptive {name} is inconsistent")
        if self.result_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("adaptive calibration result hash mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"result_sha256"})


class AdaptivePriceCalibrationReceiptV1(_FrozenContract):
    schema_version: Literal["advisory_adaptive_price_calibration_receipt_v1"] = (
        "advisory_adaptive_price_calibration_receipt_v1"
    )
    status: Literal["PUBLISHED", "ALREADY_MATERIALIZED"]
    request_id: str = Field(pattern=REQUEST_ID_PATTERN)
    request_sha256: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    registry_records_sha256: str = Field(pattern=SHA256_PATTERN)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    published_at: datetime
    selected_trial_count: int = Field(ge=0, le=1)
    activation_recommended: Literal[False] = False
    binding_activated: Literal[False] = False
    database_written: Literal[False] = False
    sealed_holdout_consumed: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def validate_receipt(self) -> "AdaptivePriceCalibrationReceiptV1":
        if self.published_at.tzinfo is None or self.published_at.utcoffset() is None:
            raise ValueError("adaptive receipt published_at must be timezone-aware")
        if self.receipt_sha256 != canonical_json_sha256(self.functional_payload()):
            raise ValueError("adaptive receipt hash mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"status", "receipt_sha256"})


def adaptive_policy_identity() -> str:
    return canonical_json_sha256(
        {
            "policy_version": POLICY_VERSION,
            "static_arm_id": STATIC_ARM_ID,
            "adaptive_arm_id": ADAPTIVE_ARM_ID,
            "nominal_coverage": NOMINAL_COVERAGE,
            "lookback_target_dates": LOOKBACK_TARGET_DATES,
            "min_fit_target_dates": MIN_FIT_TARGET_DATES,
            "min_fit_rows": MIN_FIT_ROWS,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "bootstrap_samples": BOOTSTRAP_SAMPLES,
        }
    )


def _coverage_error_improvement(static: float | None, adaptive: float | None) -> float:
    if static is None or adaptive is None:
        raise ValueError("adaptive coverage improvement requires supported arms")
    return abs(static - NOMINAL_COVERAGE) - abs(adaptive - NOMINAL_COVERAGE)


def _metric_ratio(numerator: float | None, denominator: float | None) -> float:
    if numerator is None or denominator is None or denominator <= 0.0:
        raise ValueError("adaptive width ratio requires supported positive-width arms")
    return numerator / denominator


def build_adaptive_price_calibration_request(**values: Any) -> FrozenAdaptivePriceCalibrationRequestV1:
    payload = dict(values)
    payload.setdefault("created_at", datetime.now(timezone.utc))
    payload.setdefault("policy_identity", adaptive_policy_identity())
    seed = FrozenAdaptivePriceCalibrationRequestV1.model_construct(
        request_id="advpradapt_" + "0" * 24,
        request_sha256="0" * 64,
        **payload,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return FrozenAdaptivePriceCalibrationRequestV1(
        request_id=f"advpradapt_{digest[:24]}", request_sha256=digest, **payload
    )


def build_adaptive_price_result(**values: Any) -> AdaptivePriceCalibrationResultV1:
    payload = dict(values)
    seed = AdaptivePriceCalibrationResultV1.model_construct(result_sha256="0" * 64, **payload)
    return AdaptivePriceCalibrationResultV1(
        result_sha256=canonical_json_sha256(seed.functional_payload()), **payload
    )


def build_adaptive_price_receipt(**values: Any) -> AdaptivePriceCalibrationReceiptV1:
    payload = dict(values)
    seed = AdaptivePriceCalibrationReceiptV1.model_construct(receipt_sha256="0" * 64, **payload)
    return AdaptivePriceCalibrationReceiptV1(
        receipt_sha256=canonical_json_sha256(seed.functional_payload()), **payload
    )
