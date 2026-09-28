from __future__ import annotations

import math
from datetime import date
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .daily_price_envelope_contracts import (
    AdvisoryEntryGapCalibrationV1,
    AdvisoryEntryPriceRangeV1,
    AdvisoryProtectivePriceV1,
    AdvisoryRegulatoryPriceRangeV1,
    AdvisoryStopLossPriceV1,
    AdvisoryTakeProfitPriceV1,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Nonempty = Annotated[str, Field(min_length=1, pattern=r"\S")]
Positive = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EntryTrainingLineage(_Contract):
    parent_bundle_id: Sha256
    outcome_bundle_id: Sha256


class EntryComponentRoles(_Contract):
    lstm: Nonempty
    fund: Nonempty

    @model_validator(mode="after")
    def check_distinct(self) -> "EntryComponentRoles":
        if self.lstm == self.fund:
            raise ValueError("entry parent component roles must identify distinct legs")
        return self


class EntryUniverseSelection(_Contract):
    mode: Literal["stock_universe", "single_index", "index_union"]
    pool_ids: tuple[Nonempty, ...] = ()

    @model_validator(mode="after")
    def check_selection(self) -> "EntryUniverseSelection":
        from backend.services.advisory_universe import normalize_advisory_universe_selection

        normalized = normalize_advisory_universe_selection(self.model_dump(mode="json"))
        if normalized != self.model_dump(mode="json"):
            raise ValueError("entry universe must use canonical sorted unique pool identifiers")
        return self


class EntryPriceScope(_Contract):
    """Frozen input/model contract shared by research requests and role bindings."""

    package_id: Nonempty
    package_manifest_sha256: Sha256
    style_profile_id: Nonempty
    style_profile_hash: Sha256
    selection_runtime_semantics_hash: Sha256
    feature_schema_version: Literal["advisory_feature_schema_v1"] = "advisory_feature_schema_v1"
    feature_schema_sha256: Sha256
    parent_bundle_id: Sha256
    parent_bundle_manifest_sha256: Sha256
    outcome_bundle_id: Sha256
    price_range_bundle_id: Sha256
    price_range_bundle_manifest_sha256: Sha256
    review_policy_sha256: Sha256
    component_roles: EntryComponentRoles
    universe_selection: EntryUniverseSelection
    target_count: Literal[20] = 20

    @property
    def universe_identity_sha256(self) -> str:
        from .price_range_contracts import canonical_json_sha256

        return canonical_json_sha256(self.universe_selection.model_dump(mode="json"))

    @property
    def candidate_projection_sha256(self) -> str:
        from .price_range_contracts import canonical_json_sha256

        return canonical_json_sha256({
            "schema_version": "advisory_candidate_projection_v1",
            "component_roles": self.component_roles.model_dump(),
            "target_count": self.target_count,
        })


class EntryPriceValue(_Contract):
    status: Literal["AVAILABLE", "UNAVAILABLE"]
    raw_range: AdvisoryEntryPriceRangeV1 | None = None
    calibrated_range: AdvisoryEntryPriceRangeV1 | None = None
    calibration: AdvisoryEntryGapCalibrationV1 | None = None
    reason_code: Nonempty | None = None
    message: Nonempty | None = None

    @model_validator(mode="after")
    def check_state(self) -> "EntryPriceValue":
        if self.status == "AVAILABLE":
            if self.raw_range is None or self.calibration is None:
                raise ValueError("available entry requires range and calibration identity")
            if (self.calibration.state == "CALIBRATED") != (self.calibrated_range is not None):
                raise ValueError("entry calibration and range disagree")
            if self.reason_code is not None or self.message is not None:
                raise ValueError("available entry cannot contain an error")
        elif (
            self.raw_range is not None or self.calibrated_range is not None
            or self.calibration is not None or not self.reason_code or not self.message
        ):
            raise ValueError("unavailable entry requires error and no numeric output")
        return self


class AuxiliarySource(_Contract):
    outcome_bundle_id: Sha256
    review_policy_sha256: Sha256


T = TypeVar("T", bound=BaseModel)


class AuxiliaryPrice(_Contract, Generic[T]):
    status: Literal["AVAILABLE", "UNAVAILABLE"]
    payload: T | None = None
    source_identity: AuxiliarySource | None = None
    reason_code: Nonempty | None = None

    @model_validator(mode="after")
    def check_state(self) -> "AuxiliaryPrice[T]":
        if self.status == "AVAILABLE":
            if self.payload is None or self.source_identity is None or self.reason_code:
                raise ValueError("available auxiliary price requires payload and source")
        elif self.payload is not None or self.source_identity is not None or not self.reason_code:
            raise ValueError("unavailable auxiliary price must be empty with a reason")
        return self


class EntryPriceCandidateV2(_Contract):
    symbol: Nonempty
    decision_reference_price: Positive | None = None
    decision_price_trade_date: date | None = None
    target_raw_price_multiplier: Positive | None = None
    tick_size: Positive | None = None
    regulatory_price_range: AdvisoryRegulatoryPriceRangeV1 | None = None
    entry_price: EntryPriceValue
    take_profit: AuxiliaryPrice[AdvisoryTakeProfitPriceV1]
    protective: AuxiliaryPrice[AdvisoryProtectivePriceV1]
    stop_loss: AuxiliaryPrice[AdvisoryStopLossPriceV1]

    @model_validator(mode="after")
    def check_projection(self) -> "EntryPriceCandidateV2":
        if self.entry_price.status != "AVAILABLE":
            if any(value is not None for value in (
                self.decision_reference_price, self.decision_price_trade_date,
                self.target_raw_price_multiplier, self.tick_size, self.regulatory_price_range,
            )):
                raise ValueError("unavailable entry cannot contain partial projection")
            if any(role.status == "AVAILABLE" for role in (
                self.take_profit, self.protective, self.stop_loss,
            )):
                raise ValueError("auxiliary projection requires its entry anchor")
            return self
        if any(value is None for value in (
            self.decision_reference_price, self.decision_price_trade_date,
            self.target_raw_price_multiplier, self.tick_size, self.regulatory_price_range,
        )):
            raise ValueError("available entry requires complete PIT projection")
        assert self.tick_size is not None and self.regulatory_price_range is not None
        for band in (self.entry_price.raw_range, self.entry_price.calibrated_range):
            if band is None:
                continue
            for value in (band.low, band.mid, band.high):
                units = value / self.tick_size
                if not math.isclose(units, round(units), rel_tol=0, abs_tol=1e-6):
                    raise ValueError("entry price is not tick aligned")
                regulatory = self.regulatory_price_range
                if regulatory.status == "LIMITED" and not regulatory.low <= value <= regulatory.high:
                    raise ValueError("entry price is outside regulatory bounds")
        return self


class AdvisoryEntryPriceEnvelopeV2(_Contract):
    schema_version: Literal["advisory_entry_price_envelope_v2"] = "advisory_entry_price_envelope_v2"
    projection_producer_version: Literal["advisory_entry_price_core_v1"] = "advisory_entry_price_core_v1"
    role: Literal["ENTRY_PRICE"] = "ENTRY_PRICE"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    evidence_state: Literal["EXPERIMENTAL", "CONFIRMED_PRICE_DISTRIBUTION"] = "EXPERIMENTAL"
    availability_status: Literal["AVAILABLE", "PARTIAL", "UNAVAILABLE"]
    auxiliary_availability: Literal["AVAILABLE", "PARTIAL", "UNAVAILABLE"]
    program_id: Nonempty
    binding_version_id: Nonempty | None = None
    package_id: Nonempty | None = None
    package_manifest_sha256: Sha256 | None = None
    style_profile_hash: Sha256 | None = None
    review_policy_sha256: Sha256 | None = None
    universe_identity_sha256: Sha256 | None = None
    candidate_projection_sha256: Sha256 | None = None
    feature_schema_sha256: Sha256 | None = None
    role_binding_sha256: Sha256 | None = None
    price_range_bundle_id: Sha256 | None = None
    price_range_bundle_manifest_sha256: Sha256 | None = None
    training_lineage: EntryTrainingLineage | None = None
    decision_as_of_trade_date: date | None = None
    target_trade_date: date | None = None
    price_basis: Literal["UNADJUSTED_CNY_DECISION_CLOSE"] = "UNADJUSTED_CNY_DECISION_CLOSE"
    nominal_coverage: float | None = Field(default=None, gt=0, lt=1, allow_inf_nan=False)
    calibration_state: Literal["UNCALIBRATED", "CALIBRATED_INTERVAL"] = "UNCALIBRATED"
    candidate_count: int = Field(ge=0)
    available_count: int = Field(ge=0)
    unavailable_count: int = Field(ge=0)
    candidates: tuple[EntryPriceCandidateV2, ...] = ()
    reason_code: Nonempty | None = None
    message: Nonempty | None = None

    @model_validator(mode="after")
    def check_envelope(self) -> "AdvisoryEntryPriceEnvelopeV2":
        available = sum(row.entry_price.status == "AVAILABLE" for row in self.candidates)
        total = len(self.candidates)
        if (total, available, total - available) != (
            self.candidate_count, self.available_count, self.unavailable_count,
        ):
            raise ValueError("entry counts do not match candidate rows")
        if len({row.symbol for row in self.candidates}) != total:
            raise ValueError("entry candidates contain duplicate symbols")
        if self.availability_status != availability(available, total):
            raise ValueError("entry availability does not match rows")
        roles = [role for row in self.candidates for role in (
            row.take_profit, row.protective, row.stop_loss,
        )]
        if self.auxiliary_availability != availability(
            sum(role.status == "AVAILABLE" for role in roles), len(roles),
        ):
            raise ValueError("auxiliary availability does not match role payloads")
        if self.decision_as_of_trade_date is not None and self.target_trade_date is not None:
            if self.target_trade_date <= self.decision_as_of_trade_date:
                raise ValueError("target must follow decision date")
        if available or self.evidence_state == "CONFIRMED_PRICE_DISTRIBUTION":
            identity = (
                self.binding_version_id, self.package_id, self.package_manifest_sha256,
                self.style_profile_hash, self.review_policy_sha256, self.universe_identity_sha256,
                self.candidate_projection_sha256, self.feature_schema_sha256,
                self.role_binding_sha256, self.price_range_bundle_id,
                self.price_range_bundle_manifest_sha256, self.training_lineage,
                self.decision_as_of_trade_date, self.target_trade_date, self.nominal_coverage,
            )
            if any(value is None for value in identity):
                raise ValueError("available entry requires complete scope and model identity")
            if available and (self.reason_code or self.message):
                raise ValueError("available/partial envelope uses row-level errors")
        if not available and (not self.reason_code or not self.message):
            raise ValueError("unavailable envelope requires a typed error")
        for row in self.candidates:
            if row.entry_price.status != "AVAILABLE":
                continue
            if row.decision_price_trade_date > self.decision_as_of_trade_date:
                raise ValueError("decision price cannot use future data")
            calibration = row.entry_price.calibration
            expected = "CALIBRATED" if self.calibration_state == "CALIBRATED_INTERVAL" else "UNCALIBRATED"
            if calibration.state != expected or calibration.nominal_coverage != self.nominal_coverage:
                raise ValueError("entry calibration differs from envelope")
            for role in (row.take_profit, row.protective, row.stop_loss):
                if role.status == "AVAILABLE" and (
                    role.source_identity.review_policy_sha256 != self.review_policy_sha256
                    or role.source_identity.outcome_bundle_id != self.training_lineage.outcome_bundle_id
                ):
                    raise ValueError("auxiliary source differs from frozen entry lineage")
        return self

    def as_payload(self) -> dict:
        return self.model_dump(mode="json")


def availability(available: int, total: int) -> str:
    if available == 0:
        return "UNAVAILABLE"
    return "AVAILABLE" if available == total else "PARTIAL"
