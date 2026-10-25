"""One frozen sector/flow information union; matched 16 versus candidate 19."""
from decimal import Decimal
import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MONEYFLOW_FEATURES
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

JOINT_FEATURES = (*SECTOR_FEATURES, *MONEYFLOW_FEATURES)
STATUS = 'sector_moneyflow_feature_status'
CLOCK = 'sector_moneyflow_feature_visible_through'
SOURCE_STATUS = ('sector_feature_status', 'moneyflow_feature_status')
SOURCE_CLOCK = ('sector_feature_visible_through', 'moneyflow_feature_visible_through')


class SectorMoneyflowPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_sector_moneyflow_v1'] = 'economic_sector_moneyflow_v1'
    campaign_id: Literal['advisory_sector_moneyflow_v1_20261005'] = 'advisory_sector_moneyflow_v1_20261005'
    model_id: Literal['M19'] = 'M19'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1
    sector_prepared_manifest_ref: EvidenceReferenceV1
    moneyflow_prepared_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'sector_moneyflow_predecessor', 'evaluated'),
            (self.sector_prepared_manifest_ref, 'sector_moneyflow_sector_snapshot', 'prepared'),
            (self.moneyflow_prepared_manifest_ref, 'sector_moneyflow_flow_snapshot', 'prepared')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('sector moneyflow frozen source/budget anchor differs')
        return self

    def model_copy(self, *, update=None, deep=False):
        copied = super().model_copy(update=update, deep=deep)
        return type(self).model_validate(copied.model_dump())

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-SECTOR-FLOW-COHERENCE-PRICE-1', d_features=list(D_FEATURES),
            information_features=list(JOINT_FEATURES), matched_information_features=list(SECTOR_FEATURES),
            matched_dimension=16, candidate_dimension=19, source='EXISTING_FROZEN_M1_M6_PREPARED',
            source_select_budget=0, gbdt=dict(GBDT), path_quantile=.1,
            sklearn_version='1.8.0', scipy_version='1.16.3', physical_fit_budget=4,
            campaign_fit_budget=75, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advsectorflow_'+self.plan_sha256[:24]


def _number(value):
    if value is None or value is pd.NA or (isinstance(value, Decimal) and value.is_qnan()):
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('sector moneyflow needs real numeric or normal NULL')
    try:
        result = float(value)
    except (OverflowError, ValueError, TypeError) as error:
        raise ValueError('sector moneyflow malformed numeric') from error
    if np.isnan(result) and not isinstance(value, Decimal):
        return np.nan
    if not np.isfinite(result):
        raise ValueError('sector moneyflow numeric must be finite')
    return result


def sector_moneyflow_rows_v1(*, sector_rows, moneyflow_rows, candidates, calendar):
    roster, _ = _roster_calendar(candidates, calendar)
    expected = set(roster[KEY].itertuples(index=False, name=None))
    if not len(roster) or len(sector_rows) > 7720 or len(moneyflow_rows) > 7720:
        raise ValueError('sector moneyflow original population/budget differs')
    for source in (sector_rows, moneyflow_rows):
        if source.duplicated(KEY).any() or set(source[KEY].itertuples(index=False, name=None)) != expected:
            raise ValueError('sector moneyflow exact original keys differ')
    flow_fields = [*KEY, *MONEYFLOW_FEATURES, SOURCE_STATUS[1], SOURCE_CLOCK[1]]
    if set(flow_fields[3:]) & set(sector_rows.columns):
        raise ValueError('sector moneyflow source field collision')
    # Only requested flow fields are consumed; the second source's labels/base are not read.
    rows = sector_rows.merge(moneyflow_rows.loc[:, flow_fields], on=KEY, how='left', validate='one_to_one', sort=False)
    if not rows[KEY].equals(sector_rows[KEY].reset_index(drop=True)):
        raise ValueError('sector moneyflow original order changed')
    for clock, status in zip(SOURCE_CLOCK, SOURCE_STATUS, strict=True):
        visible = pd.to_datetime(rows[clock], errors='raise')
        if visible.isna().any() or not visible.le(rows[KEY[0]]).all() or not rows[status].map(lambda v: isinstance(v, str) and (v == 'AVAILABLE' or v.startswith('UNKNOWN'))).all():
            raise ValueError('sector moneyflow source clock/status differs')
    rows.loc[:, JOINT_FEATURES] = rows.loc[:, JOINT_FEATURES].map(_number)
    available = rows.loc[:, SOURCE_STATUS].eq('AVAILABLE').all(axis=1) & np.isfinite(rows.loc[:, JOINT_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    # Query cutoff D, not a forged publication/capture timestamp; original clocks remain above.
    rows[CLOCK] = rows[KEY[0]]
    rows['sector_moneyflow_unknown_fields'] = [json.dumps([name for name in JOINT_FEATURES if pd.isna(item[name])], separators=(',', ':')) for item in rows.to_dict('records')]
    return rows


def sector_moneyflow_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('matched_information_features') != list(SECTOR_FEATURES)
            or recipe.get('sector_moneyflow_features') != list(JOINT_FEATURES)
            or recipe.get('d_features') != list(D_FEATURES)
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (16 if name.startswith('matched') else 19) for name, body in models.items())):
        raise ValueError('sector moneyflow actual 16/19 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M19')


def sector_moneyflow_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('sector moneyflow query arm/index/budget differs')
    if sector_moneyflow_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('sector moneyflow fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *JOINT_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('sector moneyflow query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(JOINT_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M19',
        information_features=JOINT_FEATURES, matched_information_features=SECTOR_FEATURES)


def train_sector_moneyflow_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M19', information_features=JOINT_FEATURES, status_column=STATUS,
        matched_information_features=SECTOR_FEATURES)


def sector_moneyflow_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if sector_moneyflow_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('sector moneyflow fitted identity changed')
    features = {name: _number(value) for name, value in d_features.items()}
    return information_price_set_v1(fitted=fitted, d_features=features, arm=arm, reference_cny=reference_cny,
        legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny, tick_cny=tick_cny,
        model_id='M19', information_features=JOINT_FEATURES, matched_information_features=SECTOR_FEATURES)
