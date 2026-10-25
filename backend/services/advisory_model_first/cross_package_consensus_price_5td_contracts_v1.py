"""GP5-CROSS-PACKAGE-CONSENSUS-1: an offline Advisory study, not package approval."""
from datetime import date
from types import MappingProxyType
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.d_clock_population_price_5td_contracts_v1 import (
    FEATURES, KEY, LABEL_COLUMNS, MARK_POLICY, MARK_POLICY_SHA256, MATRIX_ORDER as BASE_ORDER,
    PARAMETERS, ROSTER, SOURCE_COLUMNS, STATISTICS, DClockPopulationSourceV1,
)
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ("candidate_cross_package_consensus", "matched_strongest_rank")
RANK_COLUMNS = ("strongest_rank_strength", "cross_package_consensus_strength")
MATRIX_ORDERS = MappingProxyType({ARMS[0]: (*BASE_ORDER, *RANK_COLUMNS), ARMS[1]: (*BASE_ORDER, RANK_COLUMNS[0])})
CLOCK = "D_CLOSE_WITH_FROZEN_CROSS_PACKAGE_RANK_CONTEXT"
SCHEMA = "cross_package_consensus_price_5td_v1"
CONTEXT_SCHEMA = "frozen_cross_package_rank_context_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, orders=dict(MATRIX_ORDERS), parameters=dict(PARAMETERS),
    mark_policy_sha256=MARK_POLICY_SHA256, context_schema=CONTEXT_SCHEMA, clock=CLOCK,
    unique_variable="CROSS_PACKAGE_WEAKER_FROZEN_RANK_STRENGTH", statistics=STATISTICS))
PACKAGES = {
    "pkg_cc7eccb6202e4816b9d7c7d6b748bd37": ("93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6",
        "qe_20261007_141342_0221_L4", "qe_20261007_141342_0221"),
    "pkg_8dbd396d27184de6aa5fe288011220f6": ("57582c722d7a63f629db4cdb653b442c2e7ee151deb3f1d6d0514126d489c7d6",
        "qe_20261006_013449_0c29_L4", "qe_20261006_013449_0c29"),
}
CONTEXT_COLUMNS = (*RANK_COLUMNS, "context_availability", "context_sha256", "context_pair_sha256",
    "strongest_rank_fraction", "consensus_rank_fraction")


class CrossPackageConsensusStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["cross_package_consensus_price_5td_study_v1"] = "cross_package_consensus_price_5td_study_v1"
    sources: tuple[DClockPopulationSourceV1, DClockPopulationSourceV1]
    parent_manifest_ref: EvidenceReferenceV1
    parent_rows_ref: EvidenceReferenceV1
    parent_days_ref: EvidenceReferenceV1
    calendar_ref: EvidenceReferenceV1
    encoding_ref: EvidenceReferenceV1
    daily_ref: EvidenceReferenceV1
    parent_plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    parent_stage_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    parent_preregistered_stage_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    train_start: date
    train_end: date
    estimation_first_D: date
    evaluation_start: date
    evaluation_end: date
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    node_python_uri: str = Field(min_length=1)
    qe_api_base: str = Field(min_length=1)
    library_versions: dict[str, str]
    attempt_id: str = Field(default="exact_attempt_v1", pattern=r"^[a-z0-9_]+$")
    exact_retry_of: str | None = None
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"

    @model_validator(mode="after")
    def specification(self):
        from backend.mcp.common import assert_loopback_url
        assert_loopback_url(self.qe_api_base)
        refs = (self.parent_manifest_ref, self.parent_rows_ref, self.parent_days_ref,
                self.calendar_ref, self.encoding_ref, self.daily_ref)
        paths = [node_path(r.artifact_uri) for r in refs]
        if len({p.parent for p in paths}) != 1 or [p.name for p in paths] != [
                "manifest.json", "rows.parquet", "days.parquet", "calendar.json", "encoding.json", "daily.parquet"]:
            raise ValueError("one explicit existing prepared snapshot is required")
        if (self.train_start, self.train_end, self.estimation_first_D, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2024, 12, 24), date(2025, 6, 3), date(2025, 9, 30)):
            raise ValueError("only this preregistered development window is consumed; no global QE setting")
        if (len({s.package_id for s in self.sources}) != 2 or set(s.package_id for s in self.sources) != set(PACKAGES)
                or any((s.manifest_sha256, s.run_id, s.source_id) != PACKAGES[s.package_id] for s in self.sources)):
            raise ValueError("the preregistered pair's known identities differ; not a package qualification gate")
        if (set(self.library_versions) != {"numpy", "pandas", "scikit-learn", "pyarrow", "pydantic"}
                or any(not v.strip() for v in self.library_versions.values())
                or bool(self.exact_retry_of) != (self.attempt_id != "exact_attempt_v1")):
            raise ValueError("existing library/retry identity differs")
        return self

    @property
    def plan_sha256(self):
        body = self.model_dump(mode="json")
        body["sources"] = sorted(body["sources"], key=lambda s: s["package_id"])
        return sha(dict(plan=body, hypothesis="GP5-CROSS-PACKAGE-CONSENSUS-1",
                        schema_sha256=SCHEMA_SHA256, physical_fit_budget=2))

    @property
    def experiment_id(self):
        return "advgp5consensus_"+self.plan_sha256[:24]


__all__ = ["ARMS", "FEATURES", "KEY", "LABEL_COLUMNS", "MARK_POLICY", "MARK_POLICY_SHA256", "BASE_ORDER",
    "PARAMETERS", "ROSTER", "SOURCE_COLUMNS", "STATISTICS", "RANK_COLUMNS", "MATRIX_ORDERS", "CLOCK", "SCHEMA",
    "CONTEXT_SCHEMA", "SCHEMA_SHA256", "PACKAGES", "CONTEXT_COLUMNS", "CrossPackageConsensusStudyPlanV1", "node_path"]
