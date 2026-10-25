"""One original-slot economic hypothesis; no QE admission or runtime binding."""
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    FEATURES, KEY, POLICY_SHA256 as LABEL_POLICY_SHA256, STOCK_FEATURES as STOCK_FEATURES,
)
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ParentScoreSourceV1, node_path
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

HYPOTHESIS = "GP5-ORIGINAL-SLOT-HURDLE-VALUE-1"
SCHEMA = "original_slot_hurdle_price_5td_v1"
ARMS = ("matched_price_only", "candidate_daily_context")
COMPONENTS = ("sign", "positive", "negative")
COHORT_ARMS = ("baseline", "rule_300bps", *ARMS)
ROSTER_KEY = ("package_id", "manifest_sha256", "run_id", *KEY)
GAP_ORDER = ("g/100", "positive(g/100+3)", "positive(g/100)", "positive(g/100-3)")
MATRIX_ORDERS = {ARMS[0]: GAP_ORDER, ARMS[1]: (*GAP_ORDER, *FEATURES, *(f"missing_{n}" for n in FEATURES))}
POLICY = dict(population="ORIGINAL_TOP5_NO_REPLACEMENT", sessions_including_entry=5,
    entry="T_OPEN_NOMINAL", endpoint="T_PLUS_4_CLOSE_NOMINAL", buy_bps=.95, sell_bps=5.95,
    risk_object="TERMINAL_EXCESS_LOSS", threshold_bps=800., penalty_lambda=1., action="expected_utility_bps>0",
    label_policy_sha256=LABEL_POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256)
POLICY_SHA256 = sha(POLICY)
PARAMETERS = dict(sign=dict(C=1., solver="lbfgs", fit_intercept=True, max_iter=1000, tol=1e-8,
    class_weight=None, warm_start=False), positive=dict(alpha=1., solver="lbfgs", fit_intercept=True,
    max_iter=1000, tol=1e-8, warm_start=False), negative=dict(method="L-BFGS-B", maxiter=1000,
    ftol=1e-10, gtol=1e-7, initial_precision=10., precision_bounds=[.1, 1000.], l2=1.))
SUPPORT = dict(quantiles=[.025, .975], bucket_bps=100, minimum_stock_days=30, minimum_days=5)
INTERVENTION = dict(minimum_stock_days=30, minimum_days=20, minimum_day_fraction=.25)
COMPARISON_MAPPING = {"baseline": "baseline", "matched_core": ARMS[0]}
COMPARISON_MAPPING_SHA256 = sha(COMPARISON_MAPPING)
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, orders=MATRIX_ORDERS, policy_sha256=POLICY_SHA256,
    parameters=PARAMETERS, support=SUPPORT, intervention=INTERVENTION,
    comparison_mapping_sha256=COMPARISON_MAPPING_SHA256))
VALUE_FIELDS = ("p_positive", "p_negative", "p_zero", "positive_mean_bps", "negative_mean_bps",
    "expected_net_bps", "probability_terminal_loss_gt_800", "expected_terminal_excess_loss_bps", "expected_utility_bps")
UNKNOWN = {"UNKNOWN_STOCK_INPUT", "UNKNOWN_PRICE_SCENARIO", "UNKNOWN_GAP_SUPPORT"}


class FrozenPreparedRefV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_uri: str
    plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    stage_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    parent_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    files: dict[str, dict]

    @model_validator(mode="after")
    def reference(self):
        node_path(self.artifact_uri)
        if set(self.files) != {"rows.parquet", "days.parquet", "calendar.json"}:
            raise ValueError("source must bind its three original artifacts")
        import re
        for descriptor in self.files.values():
            if (set(descriptor) != {"sha256", "size_bytes"}
                    or not re.fullmatch(r"[a-f0-9]{64}", str(descriptor["sha256"]))
                    or type(descriptor["size_bytes"]) is not int or not 0 < descriptor["size_bytes"] <= 128*1024**2):
                raise ValueError("invalid bounded source artifact identity")
        return self


class OriginalSlotHurdleStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["original_slot_hurdle_price_5td_study_v1"] = "original_slot_hurdle_price_5td_study_v1"
    sources: tuple[ParentScoreSourceV1, ParentScoreSourceV1]
    source_prepared: FrozenPreparedRefV1
    train_start: date
    train_end: date
    evaluation_start: date
    evaluation_end: date
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    node_python_uri: str = Field(min_length=1)
    qe_api_base: str = Field(min_length=1)
    library_versions: dict[str, str]
    temporary_root_uri: str = "X:/AIstock_temp/advisory_original_slot_hurdle_5td"
    attempt_id: str = Field(default="exact_attempt_v1", pattern=r"^[a-z0-9_-]{1,64}$")
    exact_retry_of: str | None = None
    outputs_seen_before_design: Literal[True] = True
    previous_physical_fit_count: Literal[136] = 136
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"

    @model_validator(mode="after")
    def specification(self):
        from backend.mcp.common import assert_loopback_url
        assert_loopback_url(self.qe_api_base)
        tmp = node_path(self.temporary_root_uri)
        if not str(tmp).replace("\\", "/").lower().startswith(("x:/", "/mnt/x/")):
            raise ValueError("temporary artifacts must be on X")
        if self.exact_retry_of is not None:
            node_path(self.exact_retry_of)
        if ((self.train_start, self.train_end, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2025, 6, 3), date(2025, 9, 30))
                or len({s.package_id for s in self.sources}) != 2 or len({s.source_id for s in self.sources}) != 2
                or set(self.library_versions) != {"numpy", "pandas", "scikit-learn", "scipy", "pyarrow", "pydantic"}
                or any(not v.strip() for v in self.library_versions.values())):
            raise ValueError("approved windows, two source identities and existing environment are required")
        return self

    @property
    def economic_contract_sha256(self):
        return sha(dict(sources=[s.model_dump(mode="json") for s in self.sources],
            source=self.source_prepared.model_dump(mode="json"), schema_sha256=SCHEMA_SHA256,
            windows=[str(getattr(self, n)) for n in ("train_start", "train_end", "evaluation_start", "evaluation_end")]))

    @property
    def plan_sha256(self):
        return sha(dict(plan=self.model_dump(mode="json"), hypothesis=HYPOTHESIS, schema_sha256=SCHEMA_SHA256,
            physical_fit_budget=6, optimizer_fit_budget=2, evaluated_configurations=2))

    @property
    def experiment_id(self):
        return "advgp5hurdle_"+self.plan_sha256[:24]
