"""Three frozen D-visible close-path geometries, not intraday execution."""
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

PRICE_PATH_FEATURES = ('price_drawdown20', 'price_path_efficiency19', 'price_up_day_share19')


class PricePathPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_price_path_value_v1'] = 'economic_price_path_value_v1'
    campaign_id: Literal['advisory_price_path_v1_20261005'] = 'advisory_price_path_v1_20261005'
    model_id: Literal['M7'] = 'M7'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'price_path_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('price path original budget/predecessor anchor differs')
        if Path(self.predecessor_manifest_ref.artifact_uri).parent.parent.parent.resolve() != self.campaign_root.resolve():
            raise ValueError('price path predecessor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-PRICE-PATH-1', d_features=list(D_FEATURES), information_features=list(PRICE_PATH_FEATURES),
            source_columns=['trade_date', 'instrument', 'raw_close_cny', 'adj_factor'],
            price_unit='cny', adjusted_close='raw_close_cny_times_same_session_adj_factor', window_sessions=20,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=27, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advpricepathvalue_'+self.plan_sha256[:24]


def price_path_rows_v1(*, candidates, prices, calendar):
    roster = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if len(roster) > 7720 or not days.is_unique or not days.is_monotonic_increasing:
        raise ValueError('price path population/calendar differs')
    if not roster.empty:
        if len(days) < 2 or not roster[KEY[0]].isin(days[:-1]).all():
            raise ValueError('price path D is not in original trading calendar')
        next_day = dict(zip(days[:-1], days[1:], strict=True))
        if not roster.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all():
            raise ValueError('price path T is not original next session')
    fields = ['trade_date', 'instrument', 'raw_close_cny', 'adj_factor']
    if len(prices) > 500000:
        raise ValueError('price path source row budget exceeded')
    projected = prices.loc[:, fields].copy()
    projected['trade_date'] = projected.trade_date.map(_day)
    # Frozen source legitimately contains subsequent label sessions. Project
    # only original securities and D-visible sessions BEFORE checking values.
    projected = projected.loc[projected.instrument.isin(roster.instrument)]
    if not roster.empty:
        projected = projected.loc[projected.trade_date.le(roster[KEY[0]].max())]
    history = _frame(projected, fields[:2], set(fields))
    raw = history.loc[:, fields[2:]]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_)) or (
            value is not None and value is not pd.NA and not isinstance(value, (int, float, Decimal, np.integer, np.floating)))).any().any():
        raise ValueError('price path price/factor is not real numeric or NULL')
    numeric = raw.map(lambda value: np.nan if value is None or value is pd.NA else value).astype(float)
    if ((numeric.le(0) | ~np.isfinite(numeric)) & numeric.notna()).any().any():
        raise ValueError('price path price/factor must be positive finite or NULL')
    history.loc[:, fields[2:]] = numeric
    indexed = history.set_index(['instrument', 'trade_date'])
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        window = days[max(0, position-19):position+1]
        status, values = 'UNKNOWN_20D_WARMUP', [np.nan]*3
        if len(window) == 20:
            keys = pd.MultiIndex.from_product([[symbol], window], names=['instrument', 'trade_date'])
            group = indexed.reindex(keys).loc[:, fields[2:]].astype(float)
            status = 'UNKNOWN_PRICE_PATH_SOURCE'
            if group.notna().all().all():
                close = group.raw_close_cny.to_numpy()*group.adj_factor.to_numpy()
                if not np.isfinite(close).all() or (close <= 0).any():
                    raise ValueError('price path adjusted close over/underflow')
                changes = np.diff(np.log(close))
                denominator = float(np.abs(changes).sum())
                status = 'UNKNOWN_FLAT_PATH'
                if denominator > 0:
                    values = [float(close[-1]/close.max()-1), float(changes.sum()/denominator), float((changes > 0).sum()/19)]
                    if not np.isfinite(values).all() or not -1 <= values[0] <= 0 or not -1-1e-12 <= values[1] <= 1+1e-12 or not 0 <= values[2] <= 1:
                        raise ValueError('price path fixed geometry range differs')
                    status = 'AVAILABLE'
        records.append({**item, **dict(zip(PRICE_PATH_FEATURES, values, strict=True)),
            'price_path_feature_status': status, 'price_path_feature_visible_through': day})
    return pd.DataFrame(records, columns=[*KEY, *PRICE_PATH_FEATURES, 'price_path_feature_status', 'price_path_feature_visible_through'])


def price_path_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M7')


def price_path_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M7', information_features=PRICE_PATH_FEATURES)


def price_path_price_set_v1(**kwargs):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    return information_price_set_v1(**kwargs, model_id='M7', information_features=PRICE_PATH_FEATURES)


def train_price_path_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M7', information_features=PRICE_PATH_FEATURES, status_column='price_path_feature_status')
