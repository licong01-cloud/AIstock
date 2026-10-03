"""Frozen multi-hypothesis Advisory research; no production or QE dispatch."""
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.advisory_universe import normalize_advisory_universe_selection
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ('matched', 'candidate')
MODEL_IDS = ('M2', 'M3', 'M4')
HYPOTHESES = dict(zip(MODEL_IDS, ('H-NONLINEAR-PRICE-2', 'H-HURDLE-PRICE-2', 'H-LOCAL-DISTRIBUTION-2'), strict=True))
FIT_BUDGETS = {'M2': 4, 'M3': 5, 'M4': 2}
KNOTS = (-3., -1., 0., 1., 3.)
GBDT = dict(n_estimators=200, learning_rate=.05, max_depth=3, min_samples_leaf=30,
            subsample=1., max_features=None, random_state=20261004, n_iter_no_change=None)


class PriceCampaignPlanV2(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    schema_version: Literal['economic_price_campaign_v2'] = 'economic_price_campaign_v2'
    campaign_id: Literal['advisory_price_r2_20261004'] = 'advisory_price_r2_20261004'
    model_id: Literal['M2', 'M3', 'M4']
    parent_plan_ref: EvidenceReferenceV1
    parent_prepared_manifest_ref: EvidenceReferenceV1
    feature_manifest_ref: EvidenceReferenceV1
    reusable_prepared_manifest_ref: EvidenceReferenceV1
    profile_path: str
    profile_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    universe_selection: dict
    implementation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    study_type: Literal['EXPLORATORY_SCREEN'] = 'EXPLORATORY_SCREEN'
    objective_contract: Literal['RISK_MANAGED_ADVISORY'] = 'RISK_MANAGED_ADVISORY'
    decision_use: Literal['NAVIGATION_ONLY'] = 'NAVIGATION_ONLY'
    deployable: Literal[False] = False

    @model_validator(mode='after')
    def validate_sources(self):
        for name, role in (('parent_plan_ref', 'campaign_parent_plan'),
                           ('parent_prepared_manifest_ref', 'campaign_prices_snapshot'),
                           ('feature_manifest_ref', 'campaign_d_snapshot'),
                           ('reusable_prepared_manifest_ref', 'campaign_value_labels')):
            if getattr(self, name).role != role:
                raise ValueError('campaign source role differs')
        if not Path(self.profile_path).is_absolute():
            raise ValueError('campaign needs an explicit absolute profile')
        if normalize_advisory_universe_selection(self.universe_selection) != self.universe_selection:
            raise ValueError('campaign universe is not canonical')
        return self

    @property
    def parameters(self):
        return dict(hypothesis=HYPOTHESES[self.model_id], d_features=list(D_FEATURES),
            gbdt=dict(GBDT), knots=list(KNOTS), ridge_alpha=10., quantile_alpha=.0001,
            quantile_solver_seconds=300, path_quantile=.1, logistic_c=1., glm_alpha=1.,
            glm_max_iter=500, glm_tol=.0001, nearest_k=100, nearest_min_days=20,
            nearest_max_distance=4., sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=FIT_BUDGETS[self.model_id], campaign_fit_budget=11,
            index_build_count=int(self.model_id == 'M4'), candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20,
            maximum_rows=7720, maximum_price_rows=500000, fit_budget_seconds=1800,
            maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30,
            minimum_intervention_days=12, minimum_intervention_ratio=.15,
            mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode='json'), 'parameters': self.parameters})

    @property
    def experiment_id(self):
        return 'advprice2_'+self.plan_sha256[:24]
