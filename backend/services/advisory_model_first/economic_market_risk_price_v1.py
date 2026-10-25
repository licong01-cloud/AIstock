"""Fixed D-visible market time-series risk and stock sensitivity, no alpha search."""
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

MARKET_RISK_FEATURES = ('market_volatility19_bps', 'market_drawdown20', 'stock_market_beta19')


class MarketRiskPricePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_market_risk_price_value_v1'] = 'economic_market_risk_price_value_v1'
    campaign_id: Literal['advisory_market_risk_price_v1_20261005'] = 'advisory_market_risk_price_v1_20261005'
    model_id: Literal['M8'] = 'M8'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'market_risk_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('market risk original budget/predecessor anchor differs')
        if Path(self.predecessor_manifest_ref.artifact_uri).parent.parent.parent.resolve() != self.campaign_root.resolve():
            raise ValueError('market risk predecessor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-MARKET-RISK-1', d_features=list(D_FEATURES), information_features=list(MARKET_RISK_FEATURES),
            stock_source_columns=['trade_date', 'instrument', 'raw_close_cny', 'adj_factor'],
            benchmark_source='market.index_daily', benchmark_instrument='000300.SH', benchmark_unit='index_points',
            window_sessions=20, volatility_ddof=0, volatility_unit='daily_bps_not_annualized', beta_clipped=False,
            source_select_budget=1, source_read_seconds=30, maximum_benchmark_rows=10000,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=31, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advmarketriskvalue_'+self.plan_sha256[:24]


def _known_price(value):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('market risk price/factor is not real numeric or NULL')
    if isinstance(value, Decimal) and value.is_nan():
        raise ValueError('market risk decimal price is malformed, not a NULL')
    number = float(value)
    if np.isnan(number):
        return number
    if not np.isfinite(number) or number <= 0:
        raise ValueError('market risk price/factor must be positive finite or NULL')
    return number


def _log_returns(values):
    with np.errstate(divide='ignore', invalid='ignore', over='ignore', under='ignore'):
        changes = np.log(values[1:]/values[:-1])
    if not np.isfinite(changes).all():
        raise ValueError('market risk log-return over/underflow')
    return changes


def market_risk_rows_v1(*, candidates, prices, benchmark, calendar):
    roster = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if len(roster) > 7720 or not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('market risk population/calendar differs')
    if not roster.empty:
        if len(days) < 2 or not roster[KEY[0]].isin(days[:-1]).all():
            raise ValueError('market risk D is not in original trading calendar')
        next_day = dict(zip(days[:-1], days[1:], strict=True))
        if not roster.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all():
            raise ValueError('market risk T is not original next session')
    if len(prices) > 500000 or len(benchmark) > 10000:
        raise ValueError('market risk source row budget exceeded')
    stock_fields, index_fields = ['trade_date', 'instrument', 'raw_close_cny', 'adj_factor'], ['trade_date', 'instrument', 'close']
    stock, market = prices.loc[:, stock_fields].copy(), benchmark.loc[:, index_fields].copy()
    for frame in (stock, market):
        frame['trade_date'] = frame.trade_date.map(_day)
    stock = stock.loc[stock.instrument.isin(roster.instrument)]
    if not roster.empty:
        stock = stock.loc[stock.trade_date.le(roster[KEY[0]].max())]
        market = market.loc[market.trade_date.le(roster[KEY[0]].max())]
    if not market.instrument.eq('000300.SH').all():
        raise ValueError('market risk foreign benchmark instrument')
    stock, market = _frame(stock, stock_fields[:2], set(stock_fields)), _frame(market, index_fields[:2], set(index_fields))
    stock.loc[:, stock_fields[2:]] = stock.loc[:, stock_fields[2:]].map(_known_price).astype(float)
    market['close'] = market.close.map(_known_price).astype(float)
    indexed, market_close = stock.set_index(['instrument', 'trade_date']), market.set_index('trade_date').close
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        window = days[max(0, position-19):position+1]
        status, values = 'UNKNOWN_20D_WARMUP', [np.nan]*3
        if len(window) == 20:
            keys = pd.MultiIndex.from_product([[symbol], window], names=['instrument', 'trade_date'])
            group, index = indexed.reindex(keys).loc[:, stock_fields[2:]].astype(float), market_close.reindex(window)
            status = 'UNKNOWN_MARKET_RISK_SOURCE'
            if group.notna().all().all() and index.notna().all():
                adjusted = group.raw_close_cny.to_numpy()*group.adj_factor.to_numpy()
                if not np.isfinite(adjusted).all() or (adjusted <= 0).any():
                    raise ValueError('market risk adjusted close over/underflow')
                sr, mr = _log_returns(adjusted), _log_returns(index.to_numpy())
                centered = mr-mr.mean()
                denominator = float(centered@centered)
                status = 'UNKNOWN_FLAT_BENCHMARK'
                if np.ptp(mr) > 0 and denominator > 0:
                    values = [float(10000*np.sqrt(denominator/19)), float(index.iloc[-1]/index.max()-1), float((sr-sr.mean())@centered/denominator)]
                    if not np.isfinite(values).all() or values[0] < 0 or not -1 <= values[1] <= 0:
                        raise ValueError('market risk fixed geometry range differs')
                    status = 'AVAILABLE'
        records.append({**item, **dict(zip(MARKET_RISK_FEATURES, values, strict=True)),
            'market_risk_feature_status': status, 'market_risk_feature_visible_through': day})
    return pd.DataFrame(records, columns=[*KEY, *MARKET_RISK_FEATURES, 'market_risk_feature_status', 'market_risk_feature_visible_through'])


def market_risk_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M8')


def market_risk_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M8', information_features=MARKET_RISK_FEATURES)


def market_risk_price_set_v1(**kwargs):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    return information_price_set_v1(**kwargs, model_id='M8', information_features=MARKET_RISK_FEATURES)


def train_market_risk_price_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M8', information_features=MARKET_RISK_FEATURES, status_column='market_risk_feature_status')
