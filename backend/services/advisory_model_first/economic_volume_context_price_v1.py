"""Fixed daily volume/price proxies, not investor cost, alpha mining or execution."""
from decimal import Decimal
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_market_risk_price_v1 import _known_price, _log_returns
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

VOLUME_CONTEXT_FEATURES = ('close_vs_volume_weighted_close20', 'signed_adjusted_volume_balance19', 'adjusted_volume_concentration20')


class VolumeContextPricePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_volume_context_price_value_v1'] = 'economic_volume_context_price_value_v1'
    campaign_id: Literal['advisory_volume_context_price_v1_20261005'] = 'advisory_volume_context_price_v1_20261005'
    model_id: Literal['M9'] = 'M9'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'volume_context_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('volume context original budget/predecessor anchor differs')
        if Path(self.predecessor_manifest_ref.artifact_uri).parent.parent.parent.resolve() != self.campaign_root.resolve():
            raise ValueError('volume context predecessor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-VOLUME-COST-1', d_features=list(D_FEATURES), information_features=list(VOLUME_CONTEXT_FEATURES),
            stock_source_columns=['trade_date', 'instrument', 'raw_close_cny', 'adj_factor'],
            volume_source='market.kline_daily_raw', volume_unit='hands_100_shares', adjusted_quantity='100*volume_hand/adj_factor',
            window_sessions=20, source_select_budget=1, source_read_seconds=30, maximum_volume_rows=154400,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=35, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advvolumecontextvalue_'+self.plan_sha256[:24]


def _known_volume(value):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('volume context quantity is not real numeric or NULL')
    if isinstance(value, Decimal) and value.is_nan():
        raise ValueError('volume context decimal quantity is malformed, not NULL')
    number = float(value)
    if np.isnan(number):
        return number
    if not np.isfinite(number) or number < 0:
        raise ValueError('volume context quantity must be finite nonnegative or NULL')
    return number


def _roster_calendar(candidates, calendar):
    roster = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if len(roster) > 7720 or not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('volume context population/calendar differs')
    if not roster.empty:
        if len(days) < 2 or not roster[KEY[0]].isin(days[:-1]).all():
            raise ValueError('volume context D is not original calendar')
        next_day = dict(zip(days[:-1], days[1:], strict=True))
        if not roster.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all():
            raise ValueError('volume context T is not original next session')
    return roster, days


def volume_context_requests_v1(*, candidates, calendar):
    roster, days = _roster_calendar(candidates, calendar)
    pairs = set()
    for day, _, symbol in roster.itertuples(index=False, name=None):
        position = days.get_loc(day)
        pairs.update((session, symbol) for session in days[max(0, position-19):position+1])
    return sorted(pairs)


def volume_context_rows_v1(*, candidates, prices, volumes, calendar):
    roster, days = _roster_calendar(candidates, calendar)
    stock_fields, quantity_fields = ['trade_date', 'instrument', 'raw_close_cny', 'adj_factor'], ['trade_date', 'instrument', 'volume_hand']
    if len(prices) > 500000 or len(volumes) > 154400:
        raise ValueError('volume context source row budget exceeded')
    stock, quantity = prices.loc[:, stock_fields].copy(), volumes.loc[:, quantity_fields].copy()
    for frame in (stock, quantity):
        frame['trade_date'] = frame.trade_date.map(_day)
        if not roster.empty:
            frame.drop(frame.index[frame.trade_date.gt(roster[KEY[0]].max())], inplace=True)
    stock = stock.loc[stock.instrument.isin(roster.instrument)]
    stock, quantity = _frame(stock, stock_fields[:2], set(stock_fields)), _frame(quantity, quantity_fields[:2], set(quantity_fields))
    allowed = set(volume_context_requests_v1(candidates=roster, calendar=days))
    if not set(quantity[['trade_date', 'instrument']].itertuples(index=False, name=None)).issubset(allowed):
        raise ValueError('volume context foreign quantity key')
    stock.loc[:, stock_fields[2:]] = stock.loc[:, stock_fields[2:]].map(_known_price).astype(float)
    quantity['volume_hand'] = quantity.volume_hand.map(_known_volume).astype(float)
    indexed, volume = stock.set_index(['instrument', 'trade_date']), quantity.set_index(['instrument', 'trade_date']).volume_hand
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        window = days[max(0, position-19):position+1]
        status, values = 'UNKNOWN_20D_WARMUP', [np.nan]*3
        if len(window) == 20:
            keys = pd.MultiIndex.from_product([[symbol], window], names=['instrument', 'trade_date'])
            group, hands = indexed.reindex(keys).loc[:, stock_fields[2:]].astype(float), volume.reindex(keys)
            status = 'UNKNOWN_VOLUME_CONTEXT_SOURCE'
            if group.notna().all().all() and hands.notna().all():
                adjusted = group.raw_close_cny.to_numpy()*group.adj_factor.to_numpy()
                amount = hands.to_numpy()*100/group.adj_factor.to_numpy()
                if not np.isfinite(adjusted).all() or (adjusted <= 0).any() or not np.isfinite(amount).all():
                    raise ValueError('volume context adjusted coordinates over/underflow')
                status = 'UNKNOWN_NO_TRADED_VOLUME'
                if amount.max() > 0 and amount[1:].max() > 0:
                    weights = amount/amount.max()
                    weights /= weights.sum()
                    changes = _log_returns(adjusted)
                    tail = amount[1:]/amount[1:].max()
                    tail /= tail.sum()
                    values = [float(adjusted[-1]/(weights@adjusted)-1), float(tail@np.sign(changes)), float(weights@weights)]
                    if not np.isfinite(values).all() or values[0] <= -1 or not -1-1e-12 <= values[1] <= 1+1e-12 or not 1/20-1e-12 <= values[2] <= 1+1e-12:
                        raise ValueError('volume context fixed geometry range differs')
                    status = 'AVAILABLE'
        records.append({**item, **dict(zip(VOLUME_CONTEXT_FEATURES, values, strict=True)),
            'volume_context_feature_status': status, 'volume_context_feature_visible_through': day})
    return pd.DataFrame(records, columns=[*KEY, *VOLUME_CONTEXT_FEATURES, 'volume_context_feature_status', 'volume_context_feature_visible_through'])


def volume_context_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M9')


def volume_context_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M9', information_features=VOLUME_CONTEXT_FEATURES)


def train_volume_context_price_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M9', information_features=VOLUME_CONTEXT_FEATURES, status_column='volume_context_feature_status')
