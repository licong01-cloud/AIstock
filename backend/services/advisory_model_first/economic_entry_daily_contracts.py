"""Fresh economic consumer identities, not a mutation of research provenance."""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Literal
from zoneinfo import ZoneInfo
import math

from pydantic import Field, TypeAdapter, model_validator

from backend.services.advisory_model_first.economic_entry_contracts import SHA256
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2, FrozenContract
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.services.advisory_model_first.entry_price_contracts import EntryComponentRoles, EntryUniverseSelection
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1


def economic_D_feature_semantics_v1(*, component_roles, terminal_weights, shared_builder_sha256, bar_policy_sha256,
                                    pit_universe_key, pit_rule_version):
    """Same stable recipe for training qualification and daily source comparison."""
    from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
    return {"schema_version": "economic_eight_D_feature_semantics_v1", "feature_names": list(ECONOMIC_FEATURE_NAMES),
        "component_roles": dict(component_roles), "terminal_weights": dict(terminal_weights),
        "shared_builder_sha256": shared_builder_sha256, "bar_policy_sha256": bar_policy_sha256,
        "pit_universe_key": pit_universe_key, "pit_rule_version": pit_rule_version,
        "candidate_sessions": 20, "benchmark_sessions": 20, "breadth_sessions": 2,
        "parent_rank_normalization": "original_frozen_Top20_group_size",
        "candidate_price_coordinate": "raw_adj_over_D_anchor_scale_invariant_return_ATR_ratios",
        "breadth_universe": "existing_SH_SZ_listed_sector_eligible_D_and_previous_session",
        "raw_price_unit_divisor": 1000., "query_price": "hypothetical_raw_CNY_over_D_visible_T_reference_bps"}


class EconomicEntryCandidateProjectionV1(FrozenContract):
    """A frozen projection contract, not an economic confirmation or role activation."""

    scope: EconomicModelScopeV2
    component_roles: EntryComponentRoles
    terminal_weights: dict[str, float]
    universe_selection: EntryUniverseSelection
    review_policy_sha256: str = Field(pattern=SHA256)
    target_count: Literal[20] = 20
    schema_version: Literal["economic_entry_candidate_projection_v1"] = "economic_entry_candidate_projection_v1"

    @model_validator(mode="before")
    @classmethod
    def require_actual_weights(cls, value):
        if isinstance(value, dict):
            weights = value.get("terminal_weights")
            if not isinstance(weights, dict) or any(type(number) not in (int, float) for number in weights.values()):
                raise ValueError("economic projection weights require actual numeric values, not boolean or string placeholders")
        return value

    @model_validator(mode="after")
    def validate_projection(self):
        roles = self.component_roles.model_dump()
        if (set(self.terminal_weights) != set(roles.values())
                or any(not math.isfinite(number) or number <= 0 for number in self.terminal_weights.values())
                or not math.isclose(sum(self.terminal_weights.values()), 1., rel_tol=0., abs_tol=1e-10)):
            raise ValueError("economic projection weights must match its real two legs")
        if (self.scope.universe_definition_evidence == "NATIVE_VERIFIED"
                and canonical_json_sha256(self.universe_selection.model_dump(mode="json")) != self.scope.universe_definition_sha256):
            raise ValueError("economic projection universe definition differs from its model scope")
        return self

    @property
    def projection_sha256(self):
        return canonical_json_sha256(self.model_dump(mode="json"))


class EconomicEntryDailyInputV1(FrozenContract):
    schema_version: Literal["economic_entry_daily_input_v1"] = "economic_entry_daily_input_v1"
    scope: EconomicModelScopeV2
    program_id: str = Field(min_length=1)
    binding_version_id: str = Field(min_length=1)
    run_id: str | None = Field(default=None, min_length=1)
    list_id: str | None = Field(default=None, min_length=1)
    restored_cohort_sha256: str | None = Field(default=None, pattern=SHA256)
    decision_date: date
    target_date: date
    feature_visible_through: date
    price_visible_through: date
    captured_at: datetime
    instrument: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    selection_rank: int = Field(ge=1, le=20, strict=True)
    candidate_roster_sha256: str = Field(pattern=SHA256)
    feature_source_sha256: str = Field(pattern=SHA256)
    feature_values_sha256: str = Field(pattern=SHA256)
    price_context_sha256: str | None = Field(default=None, pattern=SHA256)
    universe_membership_sha256: str | None = Field(default=None, pattern=SHA256)
    source_evidence: Literal["NATIVE_COMPLETE", "RECOVERED_LIMITED"]
    evidence_level: Literal["HISTORICAL_REPLAY", "PROSPECTIVE_INPUT"]
    evidence_limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_identity_clock(self):
        if not self.feature_visible_through <= self.decision_date == self.price_visible_through < self.target_date:
            raise ValueError("economic daily identity requires D-frozen sources before T")
        if self.captured_at.utcoffset() is None:
            raise ValueError("economic daily capture clock must be timezone aware")
        if any(not item.strip() for item in self.evidence_limitations):
            raise ValueError("economic daily limitations cannot be blank")
        if self.source_evidence == "RECOVERED_LIMITED" and not self.evidence_limitations:
            raise ValueError("recovered economic daily input requires explicit limitations")
        if (self.run_id is None) != (self.list_id is None):
            raise ValueError("economic native run/list references must be present or absent together")
        if self.run_id is None and (self.source_evidence != "RECOVERED_LIMITED" or self.restored_cohort_sha256 is None):
            raise ValueError("missing native run/list requires an explicit restored cohort, never synthetic IDs")
        if self.source_evidence == "NATIVE_COMPLETE" and (self.universe_membership_sha256 is None or self.run_id is None):
            raise ValueError("native economic daily input requires complete membership and original run/list evidence")
        if self.evidence_level == "PROSPECTIVE_INPUT":
            zone = ZoneInfo("Asia/Shanghai")
            closing = datetime.combine(self.decision_date, time(15), zone)
            opening = datetime.combine(self.target_date, time(9, 30), zone)
            if (self.source_evidence != "NATIVE_COMPLETE" or self.scope.universe_definition_evidence != "NATIVE_VERIFIED"
                    or not closing <= self.captured_at < opening):
                raise ValueError("prospective economic input needs native scope and a real D-close/T-open capture")
        return self

    @property
    def identity_sha256(self):
        return canonical_json_sha256(self.model_dump(mode="json"))


class EconomicEntryPriceNodeV1(FrozenContract):
    price_cny: float = Field(gt=0, allow_inf_nan=False, strict=True)
    status: Literal["ACCEPTABLE", "REJECTED", "OUT_OF_SUPPORT", "EXECUTABILITY_UNPROVEN", "RISK_CONTRACT_UNCONFIGURED"]
    expected_net_return_bps: float | None = Field(default=None, allow_inf_nan=False, strict=True)
    entry_net_max_loss_q90_bps: float | None = Field(default=None, ge=0, le=10000, allow_inf_nan=False, strict=True)
    support_observations: int = Field(ge=0, strict=True)
    support_decision_days: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def validate_state(self):
        values = (self.expected_net_return_bps, self.entry_net_max_loss_q90_bps)
        if self.status == "OUT_OF_SUPPORT":
            if any(value is not None for value in values):
                raise ValueError("unknown price node cannot manufacture numerical estimates")
        elif any(value is None for value in values):
            raise ValueError("supported price node needs both real estimates")
        if self.status == "ACCEPTABLE" and (self.expected_net_return_bps <= 0 or self.entry_net_max_loss_q90_bps > 800):
            raise ValueError("first aligned economic node must preserve its fixed decision contract")
        if self.support_decision_days > self.support_observations:
            raise ValueError("price support days cannot exceed observations")
        return self


class EconomicEntryValueRoleV1(FrozenContract):
    """Independent economic advice role; no money, orders or old ENTRY_PRICE binding."""
    schema_version: Literal["economic_entry_value_role_v1"] = "economic_entry_value_role_v1"
    role: Literal["ENTRY_VALUE"] = "ENTRY_VALUE"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    activation_mode: Literal["PRICE_CONDITIONS_ADVISORY_ONLY"] = "PRICE_CONDITIONS_ADVISORY_ONLY"
    program_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")
    binding_version_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")
    bundle_id: str = Field(pattern=r"^advecserve_[0-9a-f]{24}$")
    qualified_manifest: EvidenceReferenceV1
    scope: EconomicModelScopeV2
    confirmation_request_sha256: str = Field(pattern=SHA256)
    effective_from_target_date: date
    created_at: datetime
    previous_role_sha256: str | None = Field(default=None, pattern=SHA256)
    role_sha256: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_qualified_identity(self):
        if (self.created_at.utcoffset() is None or self.scope.universe_definition_evidence != "NATIVE_VERIFIED"
                or self.qualified_manifest.role != "ECONOMIC_ENTRY_VALUE_SERVING_MANIFEST"
                or canonical_json_sha256(self.model_dump(mode="json", exclude={"role_sha256"})) != self.role_sha256):
            raise ValueError("economic role must bind actual qualified native scope and an aware immutable identity")
        return self


def build_economic_entry_value_role_v1(**values):
    fields = EconomicEntryValueRoleV1.model_fields
    if set(values) - (set(fields) - {"role_sha256"}):
        raise ValueError("economic role has unknown fields or a caller-chosen hash")
    functional = {}
    for name, field in fields.items():
        if name == "role_sha256":
            continue
        adapter = TypeAdapter(field.rebuild_annotation())
        value = values[name] if name in values else field.get_default(call_default_factory=True)
        functional[name] = adapter.dump_python(adapter.validate_python(value), mode="json")
    return EconomicEntryValueRoleV1.model_validate({**functional, "role_sha256": canonical_json_sha256(functional)})
