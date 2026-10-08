"""P26 frozen roster metadata only; neither a training plan nor package admission."""
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

MATRIX_ORDER = (*FEATURES, *(f"missing_{name}" for name in FEATURES), "scenario_gap_bps/100")
DAILY_FIELDS = ("trade_date", "instrument", "raw_open_cny", "raw_high_cny", "raw_low_cny", "raw_close_cny",
                "adj_factor", "volume_hand", "up_limit", "down_limit", "suspended", "tradability_unknown",
                "price_placeholder_fields")
INDEX_FIELDS = ("trade_date", "instrument", "close")


class FrozenPopulationSourceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    candidate_format: Literal["GP5_LEGACY_TOP20", "FROZEN_ARM_TOP50"]
    candidate_ref: EvidenceReferenceV1
    decision_dates: tuple[date, ...] = Field(min_length=1, max_length=5000)
    # Only explicitly evidenced empty days are zero; an absent row is otherwise UNKNOWN.
    empty_decision_dates: tuple[date, ...] = ()
    arm_id: str | None = None
    lineage_ref: EvidenceReferenceV1 | None = None
    bundle_manifest_ref: EvidenceReferenceV1 | None = None
    run_id: str | None = None
    list_version_id: str | None = None
    source_policy_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    universe_identity: dict[str, Any] | None = None
    source_dataset_identity: str | None = None
    source_evidence: Literal["FROZEN_LEGACY"] = "FROZEN_LEGACY"

    @model_validator(mode="after")
    def validate_source(self):
        if (list(self.decision_dates) != sorted(set(self.decision_dates))
                or list(self.empty_decision_dates) != sorted(set(self.empty_decision_dates))
                or not set(self.empty_decision_dates).issubset(self.decision_dates)):
            raise ValueError("population original decision/empty schedule differs")
        if self.candidate_format == "FROZEN_ARM_TOP50":
            if not self.arm_id or self.lineage_ref is None or self.bundle_manifest_ref is None:
                raise ValueError("frozen arm needs its original request and bundle references")
        elif self.arm_id is not None:
            raise ValueError("anchor must use the original GP5 Top20 projection")
        return self


class PopulationMetadataRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    sources: tuple[FrozenPopulationSourceV1, ...] = Field(min_length=1, max_length=8)
    calendar_ref: EvidenceReferenceV1
    train_start: date = date(2024, 7, 4)
    train_end: date = date(2025, 5, 30)
    evaluation_start: date = date(2025, 6, 3)
    evaluation_end: date = date(2025, 9, 30)

    @model_validator(mode="after")
    def validate_request(self):
        if (self.train_start, self.train_end, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2025, 6, 3), date(2025, 9, 30)):
            raise ValueError("P26 approved development windows differ")
        if (self.sources[0].candidate_format != "GP5_LEGACY_TOP20"
                or any(s.candidate_format != "FROZEN_ARM_TOP50" for s in self.sources[1:])
                or len({s.source_id for s in self.sources}) != len(self.sources)
                or len({(s.package_id, s.manifest_sha256) for s in self.sources}) != len(self.sources)):
            raise ValueError("population source identities/anchor differ")
        return self


class PopulationInputPlanV1(BaseModel):
    """Data component only. P27's model/code/journal contract is not implemented here."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    metadata_request: PopulationMetadataRequestV1
    daily_ref: EvidenceReferenceV1
    index_ref: EvidenceReferenceV1
    source_receipt_ref: EvidenceReferenceV1
    preparation_implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode="json"), "policy_sha256": POLICY_SHA256,
            "matrix_order": MATRIX_ORDER, "hypothesis": "GP5-POPULATION-TRANSFER-1",
            "component": "INPUT_PREPARATION_ONLY_NO_MODEL_RUN", "physical_fit_count": 0})

    @property
    def component_id(self):
        return "advgp5popinputs_"+self.plan_sha256[:24]
