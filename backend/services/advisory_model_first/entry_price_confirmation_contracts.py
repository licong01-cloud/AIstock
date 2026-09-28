from __future__ import annotations

from datetime import date, datetime
import math
from typing import Annotated, Literal

from pydantic import Field, model_validator

from .daily_price_envelope_contracts import AdvisoryEntryPriceRangeV1
from .entry_price_contracts import AdvisoryEntryPriceEnvelopeV2, EntryPriceScope, Nonempty, Positive, Sha256, _Contract
from .price_range_contracts import canonical_json_sha256
from .research_control_contracts import AdvisoryResearchWindowContractV1, EvidenceReferenceV1


class EntryPriceConfirmationDay(_Contract):
    decision_as_of_trade_date: date
    target_trade_date: date
    list_version_id: Nonempty
    review_run_id: Nonempty
    selection_run_id: Nonempty
    candidate_symbols: tuple[Nonempty, ...] = Field(min_length=1)
    candidate_source_sha256: Sha256

    @model_validator(mode="after")
    def check_day(self) -> "EntryPriceConfirmationDay":
        if self.target_trade_date <= self.decision_as_of_trade_date:
            raise ValueError("confirmation target must follow decision date")
        if len(set(self.candidate_symbols)) != len(self.candidate_symbols):
            raise ValueError("confirmation day has duplicate candidates")
        return self


class EntryPriceExclusiveSlot(_Contract):
    """Recorded external QE-window coordination, never an internally manufactured resource lease."""
    authorization_ref: Nonempty
    request_sha256: Sha256
    starts_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def check_clock(self):
        if self.starts_at.utcoffset() is None or self.expires_at.utcoffset() is None or self.starts_at >= self.expires_at:
            raise ValueError("exclusive resource slot requires a timezone-aware positive interval")
        return self


class EntryPriceDataIdentity(_Contract):
    profile_generation: Nonempty
    release_id: Nonempty
    data_root_uri: Nonempty
    complete: Literal[True]
    dataset_identity: Nonempty
    vintage_evidence: EvidenceReferenceV1
    candidate_provenance: EvidenceReferenceV1
    latest_model_training_date: date
    latest_transform_fit_date: date
    latest_calibration_date: date
    latest_upstream_training_date: date
    # These references are verified before inference, not inferred from filename dates.
    consumption_review: EvidenceReferenceV1
    qualification: Literal[
        "ELIGIBLE_LOCKED_HISTORICAL_OOT", "CONSUMED_OR_NON_VINTAGE", "INPUT_UNAVAILABLE",
    ]


class EntryPriceConfirmationCriteria(_Contract):
    version: Literal["entry_price_confirmation_v1"] = "entry_price_confirmation_v1"
    minimum_dates: Literal[20] = 20
    minimum_rows: Literal[300] = 300
    minimum_date_fraction: Literal[0.8] = 0.8
    minimum_rows_per_date: Literal[5] = 5
    minimum_model_availability: Literal[0.95] = 0.95
    minimum_coverage: Literal[0.75] = 0.75
    maximum_coverage: Literal[0.85] = 0.85
    maximum_width_ratio: Literal[1.25] = 1.25
    block_days: Literal[5] = 5
    bootstrap_samples: Literal[5000] = 5000
    nominal_coverage: Literal[0.8] = 0.8


class EntryCoordinateReview(_Contract):
    """Consumed validation-only numerical parity; never fit a transform on confirmation data."""
    schema_version: Literal["advisory_entry_coordinate_review_v1"]
    status: Literal["PASS"]
    validation_labels_sha256: Sha256
    scope_sha256: Sha256
    projection_producer_version: Literal["advisory_entry_price_core_v1"]
    checked_validation_rows: int = Field(gt=0)
    unavailable_rows: Literal[0]
    tolerance_abs_gap: Literal[0.000001]
    maximum_abs_gap_difference: float = Field(ge=0, le=0.000001, allow_inf_nan=False)


class EntryPriceControl(_Contract):
    validation_labels: EvidenceReferenceV1
    validation_dates: tuple[date, ...] = Field(min_length=1)
    q10: float = Field(gt=-1, allow_inf_nan=False)
    q50: float = Field(gt=-1, allow_inf_nan=False)
    q90: float = Field(gt=-1, allow_inf_nan=False)
    quantile_method: Literal["linear"] = "linear"

    @model_validator(mode="after")
    def check_control(self) -> "EntryPriceControl":
        if not self.q10 < self.q90 or not self.q10 <= self.q50 <= self.q90:
            raise ValueError("control requires positive width and ordered quantiles")
        if tuple(sorted(set(self.validation_dates))) != self.validation_dates:
            raise ValueError("control validation dates must be sorted unique")
        return self


class AdvisoryEntryPriceConfirmationRequestV1(_Contract):
    schema_version: Literal["advisory_entry_price_confirmation_request_v1"] = (
        "advisory_entry_price_confirmation_request_v1"
    )
    request_id: str = Field(pattern=r"^advepc_[0-9a-f]{24}$")
    request_sha256: Sha256
    projection_producer_version: Literal["advisory_entry_price_core_v1"] = "advisory_entry_price_core_v1"
    program_id: Nonempty
    binding_version_id: Nonempty
    scope: EntryPriceScope
    data_identity: EntryPriceDataIdentity
    days: tuple[EntryPriceConfirmationDay, ...] = Field(min_length=1)
    # Full exchange calendar freezes continuity, not a retrospectively selected subset.
    target_calendar: tuple[date, ...] = Field(min_length=1)
    replay_as_of: date
    criteria: EntryPriceConfirmationCriteria = Field(default_factory=EntryPriceConfirmationCriteria)
    control: EntryPriceControl
    window_contract: AdvisoryResearchWindowContractV1
    registry_path: Nonempty
    hypothesis_family_id: Nonempty
    parent_lineage: tuple[Nonempty, ...] = Field(min_length=1)
    frontier_id: Nonempty
    candidate_id: Nonempty
    study_type: Literal["CONFIRMATION", "EXPLORATORY_SCREEN"]
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["DIRECTION_GATE", "NAVIGATION_ONLY"]
    evidence_level: Literal["LOCKED_HISTORICAL_OOT", "HISTORICAL_REPLAY"]
    database_written: Literal[False] = False
    binding_activated: Literal[False] = False

    @model_validator(mode="after")
    def check_request(self) -> "AdvisoryEntryPriceConfirmationRequestV1":
        targets = tuple(day.target_trade_date for day in self.days)
        if tuple(sorted(set(targets))) != targets or targets != self.target_calendar:
            raise ValueError("confirmation requires the entire ordered frozen target calendar")
        if targets[-1] >= self.replay_as_of:
            raise ValueError("confirmation outcomes must precede replay-as-of")
        decisions = tuple(day.decision_as_of_trade_date for day in self.days)
        if len(set(decisions)) != len(decisions):
            raise ValueError("confirmation decision dates must be unique")
        if len(self.days) > 1 and decisions[1:] != targets[:-1]:
            raise ValueError("confirmation date plan must be consecutive trading sessions")
        if any(len(day.candidate_symbols) > self.scope.target_count for day in self.days):
            raise ValueError("confirmation candidate group exceeds frozen Top20 scope")
        identity = self.data_identity
        if identity.qualification == "INPUT_UNAVAILABLE":
            raise ValueError("input-unavailable plan cannot become a runnable confirmation request")
        latest_fit = max(
            identity.latest_model_training_date, identity.latest_transform_fit_date,
            identity.latest_calibration_date, identity.latest_upstream_training_date,
        )
        if self.study_type == "CONFIRMATION":
            if (
                identity.qualification != "ELIGIBLE_LOCKED_HISTORICAL_OOT"
                or self.decision_use != "DIRECTION_GATE"
                or self.evidence_level != "LOCKED_HISTORICAL_OOT"
                or latest_fit >= decisions[0]
                or max(self.control.validation_dates) >= decisions[0]
            ):
                raise ValueError("confirmation requires unconsumed PIT input after all fit boundaries")
            if any(len(day.candidate_symbols) != self.scope.target_count for day in self.days):
                raise ValueError("v4 confirmation requires exact original Top20 groups")
        elif (
            self.decision_use != "NAVIGATION_ONLY" or self.evidence_level != "HISTORICAL_REPLAY"
            or identity.qualification != "CONSUMED_OR_NON_VINTAGE"
        ):
            raise ValueError("development replay cannot masquerade as confirmation")
        if (
            self.window_contract.package_id != self.scope.package_id
            or self.window_contract.manifest_sha256 != self.scope.package_manifest_sha256
            or self.window_contract.runtime_semantics_hash != self.scope.selection_runtime_semantics_hash
            or self.window_contract.baseline_policy_sha256 != self.scope.review_policy_sha256
        ):
            raise ValueError("research window scope differs from entry price scope")
        digest = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != digest or self.request_id != f"advepc_{digest[:24]}":
            raise ValueError("entry confirmation request hash mismatch")
        return self

    def functional_payload(self) -> dict:
        return self.model_dump(mode="json", exclude={"request_id", "request_sha256"})


def build_entry_price_confirmation_request(**values) -> AdvisoryEntryPriceConfirmationRequestV1:
    # Normalize defaults through field validation before generating the canonical identity.
    from pydantic import TypeAdapter

    payload = {}
    for name, field in AdvisoryEntryPriceConfirmationRequestV1.model_fields.items():
        if name in {"request_id", "request_sha256"}:
            continue
        value = values[name] if name in values else field.get_default(call_default_factory=True)
        adapter = TypeAdapter(field.rebuild_annotation())
        payload[name] = adapter.dump_python(adapter.validate_python(value), mode="json")
    unknown = set(values) - set(payload)
    if unknown:
        raise ValueError(f"unknown confirmation request fields: {sorted(unknown)}")
    digest = canonical_json_sha256(payload)
    return AdvisoryEntryPriceConfirmationRequestV1(
        **payload, request_id=f"advepc_{digest[:24]}", request_sha256=digest,
    )


FiniteGap = Annotated[float, Field(allow_inf_nan=False)]


class EntryPriceConfirmationPredictionRow(_Contract):
    symbol: Nonempty
    raw_gaps: tuple[FiniteGap, FiniteGap, FiniteGap] | None = None
    calibrated_gaps: tuple[FiniteGap, FiniteGap, FiniteGap] | None = None
    control_range: AdvisoryEntryPriceRangeV1 | None = None


class EntryPriceConfirmationPredictionDay(_Contract):
    envelope: AdvisoryEntryPriceEnvelopeV2
    rows: tuple[EntryPriceConfirmationPredictionRow, ...]
    candidate_source_sha256: Sha256
    feature_values_sha256: Sha256

    @model_validator(mode="after")
    def check_rows(self) -> "EntryPriceConfirmationPredictionDay":
        if tuple(row.symbol for row in self.rows) != tuple(row.symbol for row in self.envelope.candidates):
            raise ValueError("prediction rows differ from envelope roster")
        for continuous, candidate in zip(self.rows, self.envelope.candidates):
            if candidate.entry_price.status == "AVAILABLE" and (
                continuous.raw_gaps is None or continuous.calibrated_gaps is None
                or continuous.control_range is None or candidate.entry_price.calibrated_range is None
            ):
                raise ValueError("confirmation requires calibrated continuous, projected and control ranges")
            if candidate.entry_price.status == "AVAILABLE" and any(
                gap <= -1 for gap in (*continuous.raw_gaps, *continuous.calibrated_gaps)
            ):
                raise ValueError("available prediction cannot imply a nonpositive price")
            if candidate.entry_price.status != "AVAILABLE" and continuous.control_range is not None:
                raise ValueError("unavailable projection cannot carry control prices")
            if candidate.entry_price.status == "AVAILABLE":
                from .price_range_calibration import apply_entry_gap_interval_adjustment
                import numpy as np
                calibrated = apply_entry_gap_interval_adjustment(
                    q10=np.array([continuous.raw_gaps[0]]), q50=np.array([continuous.raw_gaps[1]]),
                    q90=np.array([continuous.raw_gaps[2]]), delta=candidate.entry_price.calibration.delta)
                if any(not math.isclose(float(expected[0]), actual, abs_tol=1e-12, rel_tol=0)
                       for expected, actual in zip(calibrated, continuous.calibrated_gaps)):
                    raise ValueError("continuous calibration differs from the frozen adjustment")
                validate_entry_projection(candidate, continuous.raw_gaps, candidate.entry_price.raw_range)
                validate_entry_projection(candidate, continuous.calibrated_gaps, candidate.entry_price.calibrated_range)
        return self


def validate_entry_projection(candidate, gaps, band):
    """Re-use the business clipping/tick operator when reading artifacts, not a duplicate formula."""
    from types import SimpleNamespace
    from .price_range_inference import _entry_band
    context = SimpleNamespace(decision_raw_close=candidate.decision_reference_price,
                              target_raw_price_multiplier=candidate.target_raw_price_multiplier, tick_size=candidate.tick_size)
    expected = _entry_band(symbol=candidate.symbol, context=context, regulatory=candidate.regulatory_price_range, entry_gaps=gaps)
    if tuple(expected) != (band.low, band.mid, band.high):
        raise ValueError("continuous prediction and business tick projection disagree")


class EntryPriceConfirmationOutcome(_Contract):
    symbol: Nonempty
    market_status: Literal["AVAILABLE", "NOT_APPLICABLE", "UNAVAILABLE"]
    raw_open: Positive | None = None
    reason_code: Nonempty | None = None

    @model_validator(mode="after")
    def check_market(self) -> "EntryPriceConfirmationOutcome":
        if self.market_status == "AVAILABLE":
            if self.raw_open is None or self.reason_code:
                raise ValueError("available outcome requires positive open and no error")
        elif self.raw_open is not None or not self.reason_code:
            raise ValueError("missing/suspended outcome requires reason and no price")
        if self.market_status == "NOT_APPLICABLE" and self.reason_code != "AUTHORITATIVE_SUSPENSION":
            raise ValueError("only authoritative suspension is not applicable")
        return self


class EntryPriceConfirmationSettlementDay(_Contract):
    target_trade_date: date
    outcomes: tuple[EntryPriceConfirmationOutcome, ...]
    source_sha256: Sha256

    @model_validator(mode="after")
    def check_rows(self) -> "EntryPriceConfirmationSettlementDay":
        if len({row.symbol for row in self.outcomes}) != len(self.outcomes):
            raise ValueError("settlement contains duplicate symbols")
        return self
