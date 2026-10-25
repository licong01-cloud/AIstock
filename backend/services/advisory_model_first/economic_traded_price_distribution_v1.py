"""D-visible traded-price density, not actual holder cost or execution timing."""
import json
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
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar, volume_context_requests_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

TRADED_PRICE_FEATURES = ('traded_volume_at_or_below_D_close_share20', 'volume_weighted_close_dispersion20', 'traded_volume_within_one_D_atr_share20')
ATR_COLUMNS = (*KEY, 'atr14_close', 'feature_visible_through')
STATUS = 'traded_price_distribution_feature_status'
CLOCK = 'traded_price_distribution_feature_visible_through'
UNKNOWN = 'traded_price_distribution_unknown_fields'


class TradedPriceDistributionPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_traded_price_distribution_v1'] = 'economic_traded_price_distribution_v1'
    campaign_id: Literal['advisory_traded_price_distribution_v1_20261005'] = 'advisory_traded_price_distribution_v1_20261005'
    model_id: Literal['M11'] = 'M11'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1
    volume_snapshot_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'traded_price_distribution_predecessor', 'evaluated'),
            (self.volume_snapshot_manifest_ref, 'traded_price_distribution_volume_snapshot', 'prepared')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('traded price original budget/predecessor/volume anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('traded price anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-TRADED-PRICE-DISTRIBUTION-1', d_features=list(D_FEATURES), information_features=list(TRADED_PRICE_FEATURES),
            stock_source_columns=['trade_date', 'instrument', 'raw_close_cny', 'adj_factor'], atr_source_columns=list(ATR_COLUMNS),
            volume_source='EXISTING_FROZEN_M9_PREPARED', volume_unit='hands_100_shares', adjusted_quantity='100*volume_hand/adj_factor',
            window_sessions=20, atr_radius=1, source_select_budget=0, maximum_volume_rows=154400,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=43, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advtradedpricedistribution_'+self.plan_sha256[:24]


def _number(value, *, positive=False):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('traded price consumed value is not real numeric or NULL')
    if isinstance(value, Decimal) and value.is_nan():
        raise ValueError('traded price malformed Decimal is not NULL')
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError('traded price consumed value cannot be represented') from exc
    if np.isnan(number):
        return number
    if not np.isfinite(number) or (number <= 0 if positive else number < 0):
        raise ValueError('traded price consumed value has invalid finite range')
    return number


def _project(frame, columns, keys, allowed):
    result = frame.loc[:, columns].copy()
    result[keys[0]] = result[keys[0]].map(_day)
    if len(keys) == 3:
        result[keys[1]] = result[keys[1]].map(_day)
    selected = result.loc[[tuple(row) in allowed for row in result[keys].itertuples(index=False, name=None)]]
    return _frame(selected, keys, set(columns))


def _geometry(prices, quantities):
    # Only nonzero quantities consume historical prices; D is always the anchor.
    anchor = np.array([_number(value, positive=True) for value in prices[-1]], dtype=float)
    positive = quantities > 0
    observed = np.array([[_number(value, positive=True) for value in row] for row in prices[positive]], dtype=float)
    if np.isnan(anchor).any() or np.isnan(observed).any():
        return None
    with np.errstate(over='ignore', under='ignore', divide='ignore', invalid='ignore'):
        ratios = (observed[:, 0]/anchor[0])*(observed[:, 1]/anchor[1])
        log_weights = np.log(quantities[positive])-np.log(observed[:, 1])
        weights = np.exp(log_weights-log_weights.max())
    if not np.isfinite(ratios).all() or (ratios <= 0).any() or not np.isfinite(weights).all() or (weights <= 0).any():
        raise ValueError('traded price derived coordinates over/underflow')
    weights /= weights.sum()
    relative = ratios-1
    scale = np.abs(relative).max()
    scaled = relative/scale if scale else relative
    center = weights@scaled
    dispersion = float(np.sqrt(weights@((scaled-center)**2))*scale)
    if not np.isfinite(dispersion):
        raise ValueError('traded price derived dispersion overflow')
    return relative, weights, dispersion


def traded_price_distribution_rows_v1(*, candidates, prices, volumes, atr, calendar):
    roster, days = _roster_calendar(candidates, calendar)
    if len(prices) > 500000 or len(volumes) > 154400 or len(atr) > 7720:
        raise ValueError('traded price source row budget exceeded')
    pairs = set(volume_context_requests_v1(candidates=roster, calendar=days))
    price_columns = ['trade_date', 'instrument', 'raw_close_cny', 'adj_factor']
    stock = _project(prices, price_columns, price_columns[:2], pairs).set_index(price_columns[:2])
    quantity_columns = ['trade_date', 'instrument', 'volume_hand']
    quantity = _project(volumes, quantity_columns, quantity_columns[:2], pairs)
    quantity['volume_hand'] = quantity.volume_hand.map(_number).astype(float)
    quantity = quantity.set_index(quantity_columns[:2]).volume_hand
    keys = set(roster[KEY].itertuples(index=False, name=None))
    volatility = _project(atr, list(ATR_COLUMNS), KEY, keys)
    volatility['atr14_close'] = volatility.atr14_close.map(_number).astype(float)
    volatility['feature_visible_through'] = volatility.feature_visible_through.map(lambda value: pd.NaT if pd.isna(value) else _day(value))
    if (volatility.feature_visible_through.notna() & volatility.feature_visible_through.gt(volatility[KEY[0]])).any():
        raise ValueError('traded price consumed future ATR clock')
    volatility = volatility.set_index(KEY)
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        window = days[max(0, position-19):position+1]
        values, reason = [np.nan]*3, 'UNKNOWN_20D_HISTORY'
        if len(window) == 20:
            index = pd.MultiIndex.from_tuples([(session, symbol) for session in window], names=price_columns[:2])
            amounts = quantity.reindex(index).to_numpy()
            reason = 'UNKNOWN_VOLUME_HISTORY'
            if np.isfinite(amounts).all():
                reason = 'UNKNOWN_NO_TRADED_VOLUME'
                if amounts.max() > 0:
                    geometry = _geometry(stock.reindex(index).to_numpy(), amounts)
                    reason = 'UNKNOWN_PRICE_HISTORY'
                    if geometry is not None:
                        relative, weights, dispersion = geometry
                        values[:2] = [float(weights[relative <= 0].sum()), dispersion]
                        reason = 'UNKNOWN_ATR_INPUT_OR_CLOCK'
                        key = tuple(item[name] for name in KEY)
                        if key in volatility.index:
                            observed_atr = volatility.loc[key]
                            if pd.notna(observed_atr.feature_visible_through) and np.isfinite(observed_atr.atr14_close):
                                values[2] = float(weights[np.abs(relative) <= observed_atr.atr14_close].sum())
        unknown = {name: reason for name, value in zip(TRADED_PRICE_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(TRADED_PRICE_FEATURES, values, strict=True)),
            STATUS: 'AVAILABLE' if not unknown else reason, CLOCK: day,
            UNKNOWN: json.dumps(unknown, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *TRADED_PRICE_FEATURES, STATUS, CLOCK, UNKNOWN])


def traded_price_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M11')


def traded_price_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M11', information_features=TRADED_PRICE_FEATURES)


def train_traded_price_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M11', information_features=TRADED_PRICE_FEATURES, status_column=STATUS)
