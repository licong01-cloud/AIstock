"""One new T-open information block; no package admission or runtime activation.

Authority: advisory_sector_relative_open_price_5td_v1_f2_design_20261018.md.
"""
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    FEATURES, KEY, ROSTER, STOCK_FEATURES,
)
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.market_open_hurdle_price_5td_contracts_v1 import (
    GAP_ORDER, INTERVENTION, MARKET_ORDER, PARAMETERS, SUPPORT, VALUE_FIELDS, FrozenPreparedRefV1,
)
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ParentScoreSourceV1, node_path
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

HYPOTHESIS = "GP5-SECTOR-RELATIVE-OPEN-1"
SCHEMA = "sector_relative_open_price_5td_v1"
CLOCK = "D_FROZEN_FUNCTION_WITH_T_OPEN_PEER_CONDITION"
CONDITION_KINDS = ("HYPOTHETICAL_T_OPEN", "OBSERVED_T_OPEN_VALUATION")
ARMS = ("matched_market_coverage", "candidate_sector_relative_open")
COMPONENTS = ("sign", "positive", "negative")
COHORT_ARMS = ("baseline", "rule_300bps", *ARMS)
ROSTER_KEY = ("package_id", "manifest_sha256", "run_id", *KEY)
META = (*ROSTER_KEY, "source_id", "selection_effective_rank", "candidate_group_size")
DAY_META = ("package_id", "decision_date", "target_date", "horizon_end", "original_candidates", "source_status")
CONTEXT_COLUMNS = ("market_open_gap_bps", "peer_open_median_bps", "peer_up_fraction", "peer_quote_coverage")
PEER_ORDER = ("b/100", "abs(g-b)/100", "2w-1")
MATRIX_ORDERS = {
    ARMS[0]: (*GAP_ORDER, *FEATURES, *(f"missing_{n}" for n in FEATURES), *MARKET_ORDER, "q"),
    ARMS[1]: (*GAP_ORDER, *FEATURES, *(f"missing_{n}" for n in FEATURES), *MARKET_ORDER, "q", *PEER_ORDER),
}
PEER_POLICY = dict(universe="aistock_equity_pit_canonical_v2", membership_clock="D_PIT",
    leave_one_out=True, minimum_quotes=5, statistic="ADJUSTED_OVERNIGHT_MARK_PROXY_NOT_PRE_CLOSE",
    unobserved="RETAIN_MEMBER_AND_REASON_NO_STATE_INFERENCE", bucket_bps=100, minimum_days=5)
POLICY = dict(population="ORIGINAL_TOP5_NO_REPLACEMENT", sessions_including_entry=5,
    information_clock=CLOCK, entry="T_OPEN_NOMINAL_POST_OBSERVATION_NOT_FILL",
    endpoint="T_PLUS_4_CLOSE_NOMINAL", buy_bps=.95, sell_bps=5.95,
    risk_object="TERMINAL_EXCESS_LOSS", threshold_bps=800., penalty_lambda=1.,
    action="expected_net_bps>0_AND_expected_utility_bps>0",
    no_advice="BASELINE_UNCHANGED_NO_ADVICE_NOT_MODEL_TAKE_NOT_CASH",
    valuation_policy_sha256=VALUATION_POLICY_SHA256)
POLICY_SHA256 = sha(POLICY)
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, clock=CLOCK, orders=MATRIX_ORDERS,
    policy=POLICY, parameters=PARAMETERS, support=SUPPORT, peer_policy=PEER_POLICY,
    intervention=INTERVENTION, condition_kinds=CONDITION_KINDS))
UNKNOWN = {"UNKNOWN_STOCK_INPUT", "UNKNOWN_PRICE_SCENARIO", "UNKNOWN_GAP_SUPPORT",
    "UNKNOWN_MARKET_OPEN", "UNKNOWN_MARKET_SUPPORT", "UNKNOWN_PEER_SUPPORT", "UNKNOWN_PEER_RANGE"}
PREVIOUS_FIT_COUNT = 152
FIT_BUDGET = 6
CHUNK_DAYS = 20


class SectorRelativeOpenStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["sector_relative_open_price_5td_study_v1"] = "sector_relative_open_price_5td_study_v1"
    sources: tuple[ParentScoreSourceV1, ParentScoreSourceV1]
    source_prepared: FrozenPreparedRefV1
    file_source_binding: dict
    train_start: date
    train_end: date
    evaluation_start: date
    evaluation_end: date
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    node_python_uri: str
    qe_api_base: str
    library_versions: dict[str, str]
    temporary_root_uri: str = "X:/AIstock_temp/ADVISORY/sector_relative_open_5td"
    attempt_id: str = Field(default="exact_attempt_v1", pattern=r"^[a-z0-9_-]{1,64}$")
    exact_retry_of: str | None = None
    previous_physical_fit_count: Literal[152] = PREVIOUS_FIT_COUNT
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"

    @model_validator(mode="after")
    def specification(self):
        from backend.mcp.common import assert_loopback_url
        assert_loopback_url(self.qe_api_base)
        tmp = node_path(self.temporary_root_uri)
        if not str(tmp).replace("\\", "/").lower().startswith(("x:/", "/mnt/x/")):
            raise ValueError("study temporary files must use X")
        if not self.node_python_uri.startswith("/") or "\\" in self.node_python_uri:
            raise ValueError("fit node Python must be an explicit existing POSIX locator")
        if self.exact_retry_of is not None:
            node_path(self.exact_retry_of)
        if ((self.train_start, self.train_end, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2025, 6, 3), date(2025, 9, 30))
                or len({s.package_id for s in self.sources}) != 2
                or len({s.source_id for s in self.sources}) != 2
                or set(self.library_versions) != {"numpy", "pandas", "scikit-learn", "scipy", "pyarrow", "pydantic"}
                or any(not isinstance(v, str) or not v.strip() for v in self.library_versions.values())):
            raise ValueError("study windows, two source identities or node libraries differ")
        body = {k: v for k, v in self.file_source_binding.items() if k != "binding_hash"}
        if (body.get("schema_version") != "advisory_outcome_frozen_files_v1"
                or sha(body) != self.file_source_binding.get("binding_hash")):
            raise ValueError("frozen file identity differs; no current profile fallback")
        node_path(body["profile"]["controller_paths"]["candidate_root"])
        required = {"daily_pv", "day_calendar", "day_meta_export", "day_selection_universe",
            "factor_meta", "sector_membership_spans", "sector_code_map", "suspend_data", "index_daily"}
        if not required.issubset(body["files"]):
            raise ValueError("the explicit common file snapshot is incomplete")
        return self

    @property
    def economic_contract_sha256(self):
        return sha(dict(sources=[s.model_dump(mode="json") for s in self.sources],
            roster=self.source_prepared.model_dump(mode="json"), schema_sha256=SCHEMA_SHA256,
            binding_hash=self.file_source_binding["binding_hash"],
            windows=[str(getattr(self, n)) for n in ("train_start", "train_end", "evaluation_start", "evaluation_end")]))

    @property
    def plan_sha256(self):
        return sha(dict(plan=self.model_dump(mode="json"), hypothesis=HYPOTHESIS,
            schema_sha256=SCHEMA_SHA256, physical_fit_budget=FIT_BUDGET, candidate_count=1))

    @property
    def experiment_id(self):
        return "advgp5sectoropen_"+self.plan_sha256[:24]


__all__ = ["ARMS", "CHUNK_DAYS", "CLOCK", "COHORT_ARMS", "COMPONENTS", "CONDITION_KINDS",
    "CONTEXT_COLUMNS", "DAY_META", "FEATURES", "FIT_BUDGET", "GAP_ORDER", "HYPOTHESIS", "INTERVENTION",
    "KEY", "MARKET_ORDER", "MATRIX_ORDERS", "META", "PARAMETERS", "PEER_POLICY", "POLICY",
    "POLICY_SHA256", "PREVIOUS_FIT_COUNT", "ROSTER", "ROSTER_KEY", "SCHEMA_SHA256", "STOCK_FEATURES",
    "SUPPORT", "SectorRelativeOpenStudyPlanV1", "UNKNOWN", "VALUATION_POLICY_SHA256", "VALUE_FIELDS", "node_path", "sha"]
