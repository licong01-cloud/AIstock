"""Three fixed D moneyflow ratios, not institution identity or intraday execution."""
from pathlib import Path
from decimal import Decimal
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

MONEYFLOW_FEATURES = ('large_order_imbalance_D', 'large_order_turnover_share_D', 'large_order_imbalance_5D')
AMOUNT_FIELDS = tuple(f'{side}_{size}_amount' for size in ('sm', 'md', 'lg', 'elg') for side in ('buy', 'sell'))


class MoneyflowPricePlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_moneyflow_price_value_v1'] = 'economic_moneyflow_price_value_v1'
    campaign_id: Literal['advisory_price_moneyflow_v1_20261005'] = 'advisory_price_moneyflow_v1_20261005'
    model_id: Literal['M6'] = 'M6'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'moneyflow_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('moneyflow original budget/predecessor anchor differs')
        if Path(self.predecessor_manifest_ref.artifact_uri).parent.parent.parent.resolve() != self.campaign_root.resolve():
            raise ValueError('moneyflow predecessor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-MONEYFLOW-PRICE-1', d_features=list(D_FEATURES), information_features=list(MONEYFLOW_FEATURES),
            amount_fields=list(AMOUNT_FIELDS), source_table='market.moneyflow_ts', source_amount_unit='10k_cny',
            consumer_amount_unit='cny', moneyflow_unit_contract=MONEYFLOW_UNIT_CONTRACT_VERSION,
            window_sessions=5, source_read_seconds=30, source_select_budget=2, maximum_stock_days=38600,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=23, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advmoneyflowvalue_'+self.plan_sha256[:24]


def moneyflow_calendar_v1(candidates, calendar):
    roster = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if len(roster) > 7720 or not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('moneyflow population/calendar differs')
    if roster.empty:
        return roster, days, {}
    if len(days) < 2 or not roster[KEY[0]].isin(days[:-1]).all():
        raise ValueError('moneyflow D is not in original trading calendar')
    next_day = dict(zip(days[:-1], days[1:], strict=True))
    if not roster.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all():
        raise ValueError('moneyflow T is not original next session')
    windows = {day: days[max(0, days.get_loc(day)-4):days.get_loc(day)+1] for day in roster[KEY[0]].unique()}
    return roster, days, windows


def moneyflow_rows_v1(*, candidates, amounts, calendar):
    roster, _, windows = moneyflow_calendar_v1(candidates, calendar)
    if amounts.attrs.get('moneyflow_unit_contract') != MONEYFLOW_UNIT_CONTRACT_VERSION or amounts.attrs.get('moneyflow_amount_unit') != 'cny':
        raise ValueError('moneyflow must consume explicitly normalized CNY once')
    fields = ['trade_date', 'instrument', *AMOUNT_FIELDS]
    history = _frame(amounts.loc[:, fields], ['trade_date', 'instrument'], set(fields))
    if len(history) > 38600:
        raise ValueError('moneyflow stock-day budget exceeded')
    # A later batch's D data is never input to an earlier decision's window.
    if not roster.empty:
        history = history.loc[history.trade_date.le(roster[KEY[0]].max())]
    if not history.instrument.isin(roster.instrument).all():
        raise ValueError('moneyflow source contains a foreign security')
    raw = history.loc[:, AMOUNT_FIELDS]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_)) or (
            value is not None and value is not pd.NA and not isinstance(value, (int, float, Decimal, np.integer, np.floating)))).any().any():
        raise ValueError('moneyflow side amount is not real numeric or NULL')
    try:
        numeric = raw.astype(float)
    except (ValueError, TypeError) as error:
        raise ValueError('moneyflow malformed numeric amount') from error
    known = numeric.notna()
    if ((numeric.lt(0) | ~np.isfinite(numeric)) & known).any().any():
        raise ValueError('moneyflow side amount must be nonnegative finite or NULL')
    history.loc[:, AMOUNT_FIELDS] = numeric
    indexed = history.set_index(['instrument', 'trade_date'])
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        window = windows[day]
        status, values = 'UNKNOWN_5D_WARMUP', [np.nan]*3
        if len(window) == 5:
            keys = pd.MultiIndex.from_product([[symbol], window], names=['instrument', 'trade_date'])
            group = indexed.reindex(keys).loc[:, AMOUNT_FIELDS].astype(float)
            status = 'UNKNOWN_MONEYFLOW_SOURCE'
            if group.notna().all().all():
                buy = group.buy_lg_amount+group.buy_elg_amount
                sell = group.sell_lg_amount+group.sell_elg_amount
                gross = buy+sell
                total = group.iloc[-1].sum()
                status = 'UNKNOWN_ZERO_DENOMINATOR'
                if gross.iloc[-1] > 0 and total > 0 and gross.sum() > 0:
                    values = [float((buy.iloc[-1]-sell.iloc[-1])/gross.iloc[-1]), float(gross.iloc[-1]/total), float((buy-sell).sum()/gross.sum())]
                    if not np.isfinite(values).all() or not -1 <= values[0] <= 1 or not 0 <= values[1] <= 1 or not -1 <= values[2] <= 1:
                        raise ValueError('moneyflow fixed ratio range differs')
                    status = 'AVAILABLE'
        records.append({**item, **dict(zip(MONEYFLOW_FEATURES, values, strict=True)),
            'moneyflow_feature_status': status, 'moneyflow_feature_visible_through': day})
    return pd.DataFrame(records, columns=[*KEY, *MONEYFLOW_FEATURES, 'moneyflow_feature_status', 'moneyflow_feature_visible_through'])


def moneyflow_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M6')


def moneyflow_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M6', information_features=MONEYFLOW_FEATURES)


def moneyflow_price_set_v1(**kwargs):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    return information_price_set_v1(**kwargs, model_id='M6', information_features=MONEYFLOW_FEATURES)


def train_moneyflow_price_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M6', information_features=MONEYFLOW_FEATURES, status_column='moneyflow_feature_status')
