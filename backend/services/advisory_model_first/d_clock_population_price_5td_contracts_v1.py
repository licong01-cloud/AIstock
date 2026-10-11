"""GP5-D-CLOCK-POPULATION-ALIGNMENT-1; Advisory-only, never package admission."""
from datetime import date
from types import MappingProxyType
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, ROSTER, STOCK_FEATURES
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import node_path
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ("candidate_d_population", "matched_episode_sample")
MATRIX_ORDER = (*FEATURES, *(f"missing_{name}" for name in FEATURES))
PARAMETERS = MappingProxyType(dict(n_estimators=128, max_depth=6, min_samples_leaf=30,
    max_features=1., bootstrap=True, random_state=20261007, n_jobs=2, criterion="squared_error"))
MARK_POLICY = dict(contract="D_CLOCK_FIXED5_MARK_PATH_V1", sessions_including_entry=5,
    reference_contract="REFERENCE_D_CLOSE_NOT_FUTURE_T_PRECLOSE", price_basis="D_ANCHORED_CNY",
    endpoint="ORIGINAL_T_PLUS_4_CLOSE_MARK_NOT_FILL", suspension="VERIFIED_BRIDGE_CARRY_LAST_D_MARK",
    risk_measure="MARK_PATH_ADVERSE_RATIO_NOT_EXECUTABLE_DRAWDOWN", buy_bps=.95,
    hypothetical_sell_bps=5.95, risk_bps=800., future_entry_eligibility_required=False)
MARK_POLICY_SHA256 = sha(MARK_POLICY)
STATISTICS = dict(block_sessions=5, bootstrap_count=2000, bootstrap_seed=20261008,
    primary_endpoints=2, endpoint_alpha=.025, power=.8,
    intervention_unique_stock_days=20, intervention_days=12, intervention_day_fraction=.1)
SCHEMA = "d_clock_population_price_5td_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, order=MATRIX_ORDER, parameters=dict(PARAMETERS),
    mark_policy_sha256=MARK_POLICY_SHA256, legacy_valuation_policy_sha256=VALUATION_POLICY_SHA256,
    estimand="D_CLOSE_UNCONDITIONAL_MARK_FUNCTION", unique_variable="TRAINING_INCLUSION_MASK",
    quantile="DIRECT_RISK_INVERTED_CDF", statistics=STATISTICS))
SOURCE_COLUMNS = ("package_id", "manifest_sha256", "run_id", "source_id", *ROSTER)
LABEL_COLUMNS = ("label_information_end", "terminal_mark_ratio", "path_mark_min_ratio", "mark_label_status",
    "mark_label_reason", "mark_policy_sha256", "endpoint_execution_status", "carried_mark_sessions",
    "entry_execution_status", "entry_price_cny", "observed_gap_bps", "actual_fill_proven", "realized_return_bps")


class DClockPopulationSourceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    run_id: str = Field(min_length=1)
    universe_identity: dict | None = None


class DClockPopulationStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["d_clock_population_price_5td_study_v1"] = "d_clock_population_price_5td_study_v1"
    sources: tuple[DClockPopulationSourceV1, DClockPopulationSourceV1]
    parent_manifest_ref: EvidenceReferenceV1
    parent_rows_ref: EvidenceReferenceV1
    parent_days_ref: EvidenceReferenceV1
    calendar_ref: EvidenceReferenceV1
    parent_plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    parent_stage_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
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
        for ref in (self.parent_manifest_ref, self.parent_rows_ref, self.parent_days_ref, self.calendar_ref):
            node_path(ref.artifact_uri)
        if (not self.train_start < self.estimation_first_D <= self.train_end < self.evaluation_start <= self.evaluation_end
                or len({s.package_id for s in self.sources}) != 2 or len({s.source_id for s in self.sources}) != 2
                or set(self.library_versions) != {"numpy", "pandas", "scikit-learn", "pyarrow", "pydantic"}
                or any(not v.strip() for v in self.library_versions.values())):
            raise ValueError("D-clock study windows, original sources or existing libraries differ")
        # These are this registered study's dates, never a QE/global dataset setting.
        if (self.train_start, self.train_end, self.estimation_first_D, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2024, 12, 24), date(2025, 6, 3), date(2025, 9, 30)):
            raise ValueError("this D-clock study consumes only its approved development windows")
        if {s.package_id for s in self.sources} != {
                "pkg_cc7eccb6202e4816b9d7c7d6b748bd37", "pkg_8dbd396d27184de6aa5fe288011220f6"}:
            raise ValueError("this sampling comparison preserves its original two packages")
        if bool(self.exact_retry_of) != (self.attempt_id != "exact_attempt_v1"):
            raise ValueError("technical retry requires an explicit new attempt and prior identity")
        return self

    @property
    def plan_sha256(self):
        return sha(dict(plan=self.model_dump(mode="json"), hypothesis="GP5-D-CLOCK-POPULATION-ALIGNMENT-1",
            schema_sha256=SCHEMA_SHA256, physical_fit_budget=2, mark_policy_sha256=MARK_POLICY_SHA256))

    @property
    def experiment_id(self):
        return "advgp5dclock_"+self.plan_sha256[:24]


__all__ = ["ARMS", "FEATURES", "KEY", "ROSTER", "STOCK_FEATURES", "MATRIX_ORDER", "PARAMETERS",
    "MARK_POLICY", "MARK_POLICY_SHA256", "STATISTICS", "SCHEMA", "SCHEMA_SHA256", "SOURCE_COLUMNS",
    "LABEL_COLUMNS", "DClockPopulationStudyPlanV1", "node_path"]
