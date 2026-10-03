"""Frozen one-block Advisory exploratory contract; no runtime activation."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.services.advisory_universe import normalize_advisory_universe_selection

ARMS = ('matched', 'candidate')
KNOTS = (-3., -1., 0., 1., 3.)


class ContextValuePlanV1(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    schema_version: Literal['economic_context_value_study_v1'] = 'economic_context_value_study_v1'
    hypothesis: Literal['H-CONTEXT-VALUE-1'] = 'H-CONTEXT-VALUE-1'
    parent_plan_ref: EvidenceReferenceV1
    parent_prepared_manifest_ref: EvidenceReferenceV1
    feature_manifest_ref: EvidenceReferenceV1
    profile_path: str
    profile_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    authority_root: str
    universe_selection: dict
    implementation_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    study_type: Literal['EXPLORATORY_SCREEN'] = 'EXPLORATORY_SCREEN'
    objective_contract: Literal['RISK_MANAGED_ADVISORY'] = 'RISK_MANAGED_ADVISORY'
    decision_use: Literal['NAVIGATION_ONLY'] = 'NAVIGATION_ONLY'
    deployable: Literal[False] = False

    @model_validator(mode='after')
    def validate_sources(self):
        from pathlib import Path
        for ref, role in ((self.parent_plan_ref, 'context_parent_plan'),
                (self.parent_prepared_manifest_ref, 'context_prices_snapshot'),
                (self.feature_manifest_ref, 'context_d_snapshot')):
            if ref.role != role:
                raise ValueError('context source role differs')
        for path in (self.profile_path, self.authority_root):
            if not Path(path).is_absolute():
                raise ValueError('context requires explicit absolute profile/authority paths')
        if normalize_advisory_universe_selection(self.universe_selection) != self.universe_selection:
            raise ValueError('context universe selection is not canonical')
        return self

    @property
    def parameters(self):
        return {'d_features': list(D_FEATURES), 'knots': list(KNOTS), 'category_scale': .5,
            'mean_alpha': 10., 'quantile_alpha': .0001, 'path_quantile': .1, 'solver_time_limit_seconds': 300,
            'sklearn_version': '1.8.0', 'scipy_version': '1.16.3', 'gap_bin_bps': 100,
            'minimum_observations': 30, 'minimum_days': 5, 'gap_tail_quantiles': [.025, .975],
            'downside_reference_bps': 800., 'economic_increment_bps': 5., 'minimum_intervention_days': 12,
            'minimum_intervention_ratio': .15, 'minimum_model_take_episodes': 30,
            'mdd_deterioration_bps': 200., 'tail_deterioration_bps': 20.,
            'block_days': 5, 'bootstrap_repetitions': 2000, 'bootstrap_seed': 20261004,
            'physical_fit_budget': 4, 'candidate_count': 1, 'configuration_count': 2,
            'maximum_rows': 7720, 'maximum_price_rows': 500000, 'fit_budget_seconds': 1800,
            'maximum_rss_bytes': 2*1024**3, 'maximum_artifact_bytes': 2*1024**3,
            'policy_sha256': value_anchor_policy_sha256_v1(), 'cost_sha256': COST.policy_sha256}

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode='json'), 'parameters': self.parameters})

    @property
    def experiment_id(self):
        return 'advctxvalue_'+self.plan_sha256[:24]
