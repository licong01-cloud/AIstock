"""Frozen D-visible breadth history, not HMM, alpha generation or execution."""
from decimal import Decimal
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

BREADTH_STATE_FEATURES = ('market_up_ratio_mean20', 'market_up_ratio_slope20', 'market_weak_breadth_share20')
BREADTH_SOURCE_COLUMNS = (*KEY, 'market_up_ratio', 'feature_visible_through')


class BreadthStatePricePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_breadth_state_price_value_v1'] = 'economic_breadth_state_price_value_v1'
    campaign_id: Literal['advisory_breadth_state_price_v1_20261005'] = 'advisory_breadth_state_price_v1_20261005'
    model_id: Literal['M10'] = 'M10'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'breadth_state_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('breadth state original budget/predecessor anchor differs')
        if Path(self.predecessor_manifest_ref.artifact_uri).parent.parent.parent.resolve() != self.campaign_root.resolve():
            raise ValueError('breadth state predecessor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-BREADTH-STATE-1', d_features=list(D_FEATURES), information_features=list(BREADTH_STATE_FEATURES),
            breadth_source_columns=list(BREADTH_SOURCE_COLUMNS), breadth_source='ORIGINAL_FROZEN_D_FEATURES',
            window_sessions=20, weak_breadth_threshold=.5, source_select_budget=0,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=39, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advbreadthstatevalue_'+self.plan_sha256[:24]


def _known_breadth(value):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('breadth history value is not real numeric or NULL')
    if isinstance(value, Decimal) and value.is_nan():
        raise ValueError('breadth history decimal value is malformed, not NULL')
    number = float(value)
    if np.isnan(number):
        return number
    if not np.isfinite(number) or not 0 <= number <= 1:
        raise ValueError('breadth history value must be finite in [0,1] or NULL')
    return number


def _breadth_roster_calendar(candidates, calendar):
    roster = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if len(roster) > 7720 or not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('breadth history population/calendar differs')
    if not roster.empty:
        if len(days) < 2 or not roster[KEY[0]].isin(days[:-1]).all():
            raise ValueError('breadth history D is not original calendar')
        next_day = dict(zip(days[:-1], days[1:], strict=True))
        if not roster.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all():
            raise ValueError('breadth history T is not original next session')
    return roster, days


def _consumed_breadth_series(roster, days, breadth):
    if len(breadth) > 7720:
        raise ValueError('breadth history original row budget exceeded')
    needed = set()
    for decision in roster[KEY[0]].unique():
        position = days.get_loc(decision)
        needed.update(days[max(0, position-19):position+1])
    frame = breadth.loc[:, BREADTH_SOURCE_COLUMNS].copy()
    frame[KEY[0]] = frame[KEY[0]].map(_day)
    frame = _frame(frame.loc[frame[KEY[0]].isin(needed)], KEY, set(BREADTH_SOURCE_COLUMNS))
    if not frame.empty:
        following = dict(zip(days[:-1], days[1:], strict=True))
        if not frame.apply(lambda row: following[row[KEY[0]]] == row[KEY[1]], axis=1).all():
            raise ValueError('breadth history original source D/T differs')
    frame['market_up_ratio'] = frame.market_up_ratio.map(_known_breadth).astype(float)
    frame['feature_visible_through'] = frame.feature_visible_through.map(lambda value: pd.NaT if pd.isna(value) else _day(value))
    records = {}
    for decision, group in frame.groupby(KEY[0], sort=False):
        if group.market_up_ratio.nunique(dropna=False) != 1 or group.feature_visible_through.nunique(dropna=False) != 1:
            raise ValueError('breadth history same-D values/clocks conflict')
        value, visible = group.market_up_ratio.iloc[0], group.feature_visible_through.iloc[0]
        if pd.notna(visible) and visible > decision:
            raise ValueError('breadth history consumed future clock')
        records[decision] = float(value) if pd.notna(visible) else np.nan
    return pd.Series(records, dtype=float)


def breadth_state_rows_v1(*, candidates, breadth, calendar):
    roster, days = _breadth_roster_calendar(candidates, calendar)
    series = _consumed_breadth_series(roster, days, breadth)
    centered_time = np.arange(20, dtype=float)-9.5
    records = []
    for item in roster.to_dict('records'):
        decision = item[KEY[0]]
        position = days.get_loc(decision)
        window = days[max(0, position-19):position+1]
        values, status = [np.nan]*3, 'UNKNOWN_20D_HISTORY'
        if len(window) == 20:
            history = series.reindex(window)
            status = 'UNKNOWN_BREADTH_HISTORY'
            if history.notna().all():
                observed = history.to_numpy()
                values = [float(observed.mean()), float(centered_time@(observed-observed[0])/(centered_time@centered_time)),
                    float(np.count_nonzero(observed < .5)/20)]
                if not np.isfinite(values).all() or not 0 <= values[0] <= 1 or not 0 <= values[2] <= 1:
                    raise ValueError('breadth history fixed geometry differs')
                status = 'AVAILABLE'
        records.append({**item, **dict(zip(BREADTH_STATE_FEATURES, values, strict=True)),
            'breadth_state_feature_status': status, 'breadth_state_feature_visible_through': decision})
    return pd.DataFrame(records, columns=[*KEY, *BREADTH_STATE_FEATURES, 'breadth_state_feature_status', 'breadth_state_feature_visible_through'])


def breadth_state_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M10')


def breadth_state_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M10', information_features=BREADTH_STATE_FEATURES)


def train_breadth_state_price_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M10', information_features=BREADTH_STATE_FEATURES, status_column='breadth_state_feature_status')
