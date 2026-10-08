"""One additional D information block; never an alpha or package admission rule."""
from datetime import date

from pydantic import BaseModel, ConfigDict

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    FEATURES, KEY, PARAMETERS, POLICY, POLICY_SHA256, ROSTER, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

MONEYFLOW_FEATURES = ("large_order_imbalance_D", "large_order_turnover_share_D", "large_order_imbalance_5D")
MODEL_FEATURES = (*FEATURES, *MONEYFLOW_FEATURES)
MATRIX_ORDER = (*FEATURES, *(f"missing_{name}" for name in FEATURES), "scenario_gap_bps/100",
                *MONEYFLOW_FEATURES, *(f"missing_{name}" for name in MONEYFLOW_FEATURES))
SCHEMA_SHA256 = sha(dict(family="generic_moneyflow_price_5td_v1", matrix_order=MATRIX_ORDER,
    moneyflow_units="FROZEN_DIMENSIONLESS_NO_RESCALE", missing="TRAIN_MEDIAN_PLUS_FLAG_NO_POPULATION_FILTER"))
INTERVENTION_SUPPORT = dict(minimum_episodes=20, minimum_days=12, minimum_day_fraction=.10)
STATISTICS = dict(block_sessions=5, bootstrap_count=2000, bootstrap_seed=20261008, power=.80)


class GenericMoneyflowPrice5TDPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    configuration: GenericPrice5TDConfigurationV1
    gp5_plan_ref: EvidenceReferenceV1
    gp5_prepared_ref: EvidenceReferenceV1
    gp5_trained_ref: EvidenceReferenceV1
    moneyflow_prepared_ref: EvidenceReferenceV1
    dataset_identity: str
    parent_lineage: tuple[str, ...]
    decision_dates: tuple[date, ...]
    implementation_sha256: str

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode="json"), "schema_sha256": SCHEMA_SHA256,
            "policy_sha256": POLICY_SHA256, "hypothesis": "GP5-MONEYFLOW-INFO-1",
            "objective_contract": "RISK_MANAGED_ADVISORY", "study_type": "EXPLORATORY_SCREEN",
            "decision_use": "NAVIGATION_ONLY", "physical_fit_budget": 2,
            "intervention_support": INTERVENTION_SUPPORT, "statistics": STATISTICS,
            "test": "EXCLUDED_NOT_CONSUMED"})

    @property
    def experiment_id(self):
        return "advgp5moneyflow_"+self.plan_sha256[:24]


__all__ = ["FEATURES", "KEY", "PARAMETERS", "POLICY", "POLICY_SHA256", "ROSTER", "MONEYFLOW_FEATURES",
           "MODEL_FEATURES", "MATRIX_ORDER", "SCHEMA_SHA256", "INTERVENTION_SUPPORT", "STATISTICS",
           "GenericPrice5TDConfigurationV1", "GenericMoneyflowPrice5TDPlanV1"]
