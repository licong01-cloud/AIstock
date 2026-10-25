"""Past daily limit state as price-value context; no minute execution policy."""
from decimal import Decimal
import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import _project
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar, volume_context_requests_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

LIMIT_STATE_FEATURES = ('prior_limit_up_touch_share20', 'prior_limit_down_touch_share20', 'D_limit_close_position')
SOURCE_COLUMNS = ['trade_date', 'instrument', 'raw_high_cny', 'raw_low_cny', 'raw_close_cny', 'up_limit', 'down_limit']
STATUS = 'limit_state_feature_status'
CLOCK = 'limit_state_feature_visible_through'
UNKNOWN = 'limit_state_unknown_fields'
TOLERANCE_CNY = .005


class LimitStatePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_limit_state_v1'] = 'economic_limit_state_v1'
    campaign_id: Literal['advisory_limit_state_v1_20261005'] = 'advisory_limit_state_v1_20261005'
    model_id: Literal['M15'] = 'M15'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'limit_state_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('limit state original budget/predecessor anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('limit state anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-PRIOR-LIMIT-STATE-1', d_features=list(D_FEATURES), information_features=list(LIMIT_STATE_FEATURES),
            stock_source_columns=list(SOURCE_COLUMNS), price_unit='cny', source='EXISTING_ORIGINAL_FROZEN_RAW_DAILY',
            window_sessions=20, tick_cny=.01, comparison_tolerance_cny=TOLERANCE_CNY, source_select_budget=0,
            maximum_projected_price_rows=154400, gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=59, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advlimitstatevalue_'+self.plan_sha256[:24]


def _number(value):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('limit state consumed value is not real numeric or NULL')
    if isinstance(value, Decimal) and value.is_qnan():
        return np.nan
    if isinstance(value, Decimal) and value.is_snan():
        raise ValueError('limit state signaling NaN is not NULL')
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError('limit state consumed value cannot be represented') from exc
    if np.isnan(number):
        return number
    if not np.isfinite(number) or number <= 0:
        raise ValueError('limit state known price/limit must be finite positive without underflow')
    return number


def _corridor(close, upper, lower):
    if np.isfinite(upper) and np.isfinite(lower) and lower > upper:
        raise ValueError('limit state lower bound exceeds upper bound')
    if not np.isfinite([close, upper, lower]).all():
        return np.nan, 'UNKNOWN_LIMIT_SOURCE'
    if lower == upper:
        return np.nan, 'UNKNOWN_LIMIT_CORRIDOR'
    if close < lower-TOLERANCE_CNY or close > upper+TOLERANCE_CNY:
        raise ValueError('limit state D close contradicts legal bounds')
    # Only tolerated exterior rounding maps to endpoints; interior formula stays exact.
    value = 0. if close < lower else 1. if close > upper else (close-lower)/(upper-lower)
    if not np.isfinite(value):
        raise ValueError('limit state derived corridor overflow')
    return value, None


def limit_state_rows_v1(*, candidates, prices, calendar):
    roster, days = _roster_calendar(candidates, calendar)
    if len(prices) > 500000:
        raise ValueError('limit state source row budget exceeded')
    pairs = set(volume_context_requests_v1(candidates=roster, calendar=days))
    source = _project(prices, SOURCE_COLUMNS, SOURCE_COLUMNS[:2], pairs).set_index(SOURCE_COLUMNS[:2])
    if len(source) > 154400:
        raise ValueError('limit state projected row budget exceeded')
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        history = days[max(0, position-19):position+1]
        values = [np.nan]*3
        reasons = dict.fromkeys(LIMIT_STATE_FEATURES, 'UNKNOWN_LIMIT_SOURCE')
        if (day, symbol) in source.index:
            point = source.loc[(day, symbol)]
            close, upper, lower = (_number(point[name]) for name in ('raw_close_cny', 'up_limit', 'down_limit'))
            values[2], reason = _corridor(close, upper, lower)
            if reason:
                reasons[LIMIT_STATE_FEATURES[2]] = reason
        if len(history) < 20:
            reasons.update(dict.fromkeys(LIMIT_STATE_FEATURES[:2], 'UNKNOWN_20D_HISTORY'))
        else:
            observed = source.reindex([(previous, symbol) for previous in history])
            fields = ('raw_high_cny', 'raw_low_cny', 'up_limit', 'down_limit')
            high, low, upper, lower = np.array([[_number(value) for value in observed[name]] for name in fields])
            if ((np.isfinite(high) & np.isfinite(low) & (high < low)).any()
                    or (np.isfinite(upper) & np.isfinite(lower) & (lower > upper)).any()
                    or (np.isfinite(high) & np.isfinite(upper) & (high > upper+TOLERANCE_CNY)).any()
                    or (np.isfinite(low) & np.isfinite(lower) & (low < lower-TOLERANCE_CNY)).any()):
                raise ValueError('limit state consumed history contradicts price/legal bounds')
            if np.isfinite(high).all() and np.isfinite(upper).all():
                values[0] = float(np.mean(high >= upper-TOLERANCE_CNY))
            if np.isfinite(low).all() and np.isfinite(lower).all():
                values[1] = float(np.mean(low <= lower+TOLERANCE_CNY))
        unknown = {name: reasons[name] for name, value in zip(LIMIT_STATE_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(LIMIT_STATE_FEATURES, values, strict=True)), STATUS: 'AVAILABLE' if not unknown else next(iter(unknown.values())),
            CLOCK: day, UNKNOWN: json.dumps(unknown, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *LIMIT_STATE_FEATURES, STATUS, CLOCK, UNKNOWN])


def limit_state_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M15')


def limit_state_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M15', information_features=LIMIT_STATE_FEATURES)


def train_limit_state_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M15', information_features=LIMIT_STATE_FEATURES, status_column=STATUS)
