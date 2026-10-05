"""Frozen parent raw scores, not probabilities; common 13 versus 15 inputs."""
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import Field, model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import _number
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

RAW_FEATURES = ('parent_lstm_raw_score_D', 'parent_fund_raw_score_D')
STATUS = 'parent_raw_score_feature_status'
CLOCK = 'parent_raw_score_feature_visible_through'


class ParentRawScorePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_parent_raw_score_v1'] = 'economic_parent_raw_score_v1'
    campaign_id: Literal['advisory_parent_raw_score_v1_20261005'] = 'advisory_parent_raw_score_v1_20261005'
    model_id: Literal['M20'] = 'M20'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1
    prior_fit_journal_sha256: str = Field(pattern=r'^[a-f0-9]{64}$')
    component_raw_columns: dict[str, str]
    package_id: str = Field(min_length=1)
    package_manifest_sha256: str = Field(pattern=r'^[a-f0-9]{64}$')
    parent_normalization: Literal['zscore'] = 'zscore'

    @model_validator(mode='after')
    def validate_raw_sources(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'parent_raw_score_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('parent raw score original source/budget anchor differs')
        _raw_columns(self.component_raw_columns)
        return self

    def model_copy(self, *, update=None, deep=False):
        return type(self).model_validate(super().model_copy(update=update, deep=deep).model_dump())

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-PARENT-RAW-SCALE-PRICE-1', d_features=list(D_FEATURES),
            information_features=list(RAW_FEATURES), matched_dimension=13, candidate_dimension=15,
            component_raw_columns=self.component_raw_columns, package_id=self.package_id,
            package_manifest_sha256=self.package_manifest_sha256, parent_normalization=self.parent_normalization,
            source='EXISTING_FROZEN_PARENT_RAW_SCORE', source_select_budget=0, gbdt=dict(GBDT), path_quantile=.1,
            sklearn_version='1.8.0', scipy_version='1.16.3', physical_fit_budget=4,
            campaign_fit_budget=79, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_ranking_rows=20000,
            maximum_price_rows=500000, fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3,
            maximum_artifact_bytes=2*1024**3, economic_increment_bps=5., minimum_model_take_episodes=30,
            minimum_intervention_days=12, minimum_intervention_ratio=.15,
            mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advrawscore_'+self.plan_sha256[:24]


def _raw_columns(mapping):
    if (set(mapping) != {'lstm', 'fund'} or len(set(mapping.values())) != 2
            or any(not isinstance(v, str) or not v.startswith('raw__') or len(v) <= 5 for v in mapping.values())):
        raise ValueError('parent raw score role/column mapping differs')
    return [mapping[role] for role in ('lstm', 'fund')]


def parent_raw_score_rows_v1(*, base_rows, rankings, candidates, calendar, component_raw_columns,
        package_id, package_manifest_sha256):
    columns = _raw_columns(component_raw_columns)
    roster, _ = _roster_calendar(candidates, calendar)
    expected = set(roster[KEY].itertuples(index=False, name=None))
    if (not len(roster) or len(base_rows) > 7720 or len(rankings) > 20000
            or base_rows.duplicated(KEY).any()
            or set(base_rows[KEY].itertuples(index=False, name=None)) != expected):
        raise ValueError('parent raw score exact original population/budget differs')
    fields = [*KEY, *columns, 'trade_date', 'package_id', 'manifest_sha256']
    # Project requested original keys before consuming raw values; review-only/future
    # stocks are not sources of features, calibration, labels or replacement picks.
    selected = rankings.loc[rankings[KEY].apply(tuple, axis=1).isin(expected), fields].copy()
    if (selected.duplicated(KEY).any() or set(selected[KEY].itertuples(index=False, name=None)) != expected
            or not pd.to_datetime(selected.trade_date).eq(selected[KEY[0]]).all()
            or not selected.package_id.eq(package_id).all()
            or not selected.manifest_sha256.eq(package_manifest_sha256).all()):
        raise ValueError('parent raw score requested key/clock/package identity differs')
    if set((*RAW_FEATURES, STATUS, CLOCK)) & set(base_rows.columns):
        raise ValueError('parent raw score source field collision')
    selected = selected.loc[:, [*KEY, *columns]].rename(columns=dict(zip(columns, RAW_FEATURES, strict=True)))
    selected.loc[:, RAW_FEATURES] = selected.loc[:, RAW_FEATURES].map(_number)
    rows = base_rows.merge(selected, on=KEY, how='left', validate='one_to_one', sort=False)
    if not rows[KEY].equals(base_rows[KEY].reset_index(drop=True)):
        raise ValueError('parent raw score original order changed')
    available = np.isfinite(rows.loc[:, RAW_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    rows[CLOCK] = rows[KEY[0]]  # Original query cut, not a forged capture/publication time.
    return rows


def parent_raw_score_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('parent_raw_score_features') != list(RAW_FEATURES) or recipe.get('d_features') != list(D_FEATURES)
            or 'matched_information_features' in recipe
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (13 if name.startswith('matched') else 15) for name, body in models.items())):
        raise ValueError('parent raw score actual 13/15 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M20')


def parent_raw_score_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('parent raw score query arm/index/budget differs')
    if parent_raw_score_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent raw score fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *RAW_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('parent raw score query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(RAW_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M20', information_features=RAW_FEATURES)


def train_parent_raw_score_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M20', information_features=RAW_FEATURES, status_column=STATUS)


def parent_raw_score_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if parent_raw_score_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent raw score fitted identity changed')
    return information_price_set_v1(fitted=fitted, d_features={name: _number(v) for name, v in d_features.items()}, arm=arm,
        reference_cny=reference_cny, legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny,
        tick_cny=tick_cny, model_id='M20', information_features=RAW_FEATURES)
