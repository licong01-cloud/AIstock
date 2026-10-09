"""One Advisory calibration hypothesis; immutable inputs, no package admission."""
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import (
    PARAMETERS, ParentScoreSourceV1, node_path,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ("matched_core", "candidate_risk_calibrated")
COHORT_ARMS = ("baseline", "rule_300bps", *ARMS)
ROSTER_KEY = ("package_id", "manifest_sha256", "run_id", *KEY)
MATRIX_ORDER = (*FEATURES, *(f"missing_{n}" for n in FEATURES), "scenario_gap_bps/100", "package_TCN")
SCHEMA = "risk_tail_calibrated_price_5td_v1"
CALIBRATION = dict(method="GLOBAL_TRAIN_ONLY_Q90_RESIDUAL_INVERTED_CDF", probability=.9, days=60,
    weight_rule="ORIGINAL_DAY_UNIQUE_STOCK_CLUSTER_SPLIT_PACKAGE_MASS", conditional_guarantee=False)
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, matrix_order=MATRIX_ORDER, parameters=dict(PARAMETERS),
    calibration=CALIBRATION, policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256))
UNKNOWN = {"UNKNOWN_STOCK_INPUT", "UNKNOWN_PACKAGE_ADAPTER", "UNKNOWN_PRICE_SCENARIO",
    "UNKNOWN_GAP_SUPPORT", "UNKNOWN_MODEL_DISTRIBUTION", "UNKNOWN_RISK_CALIBRATION"}


class FrozenPreparedRefV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_uri: str = Field(min_length=1)
    plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    stage_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def path(self):
        node_path(self.artifact_uri)
        return self


class RiskTailStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["risk_tail_calibrated_price_5td_study_v1"] = "risk_tail_calibrated_price_5td_study_v1"
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
    temporary_root_uri: str = "X:/AIstock_temp/advisory_risk_calibration_5td_20261010"
    attempt_id: str = Field(default="exact_attempt_v1", pattern=r"^[a-z0-9_-]{1,64}$")
    exact_retry_of: str | None = None
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"

    @model_validator(mode="after")
    def specification(self):
        from backend.mcp.common import assert_loopback_url
        assert_loopback_url(self.qe_api_base)
        node_path(self.temporary_root_uri)
        if self.exact_retry_of is not None:
            node_path(self.exact_retry_of)
        if ((self.train_start, self.train_end, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2025, 6, 3), date(2025, 9, 30))
                or len({s.package_id for s in self.sources}) != 2 or {s.context_bit for s in self.sources} != {0, 1}
                or len({s.source_id for s in self.sources}) != 2
                or set(self.library_versions) != {"numpy", "pandas", "scikit-learn", "pyarrow", "pydantic"}
                or any(not v.strip() for v in self.library_versions.values())):
            raise ValueError("this one study requires its original development windows/sources/environment")
        return self

    @property
    def economic_contract_sha256(self):
        return sha(dict(sources=[s.model_dump(mode="json") for s in self.sources],
            source=self.source_prepared.model_dump(mode="json"),
            windows=[str(getattr(self, n)) for n in ("train_start", "train_end", "evaluation_start", "evaluation_end")],
            schema_sha256=SCHEMA_SHA256))

    @property
    def plan_sha256(self):
        return sha(dict(plan=self.model_dump(mode="json"), schema_sha256=SCHEMA_SHA256,
            hypothesis="GP5-RISK-TAIL-CALIBRATION-1", physical_fit_budget=1, calibration_estimation_budget=1))

    @property
    def experiment_id(self):
        return "advgp5riskcal_"+self.plan_sha256[:24]


__all__ = ["ARMS", "COHORT_ARMS", "ROSTER_KEY", "MATRIX_ORDER", "PARAMETERS", "SCHEMA", "SCHEMA_SHA256",
    "CALIBRATION", "UNKNOWN", "RiskTailStudyPlanV1", "FrozenPreparedRefV1", "node_path"]
