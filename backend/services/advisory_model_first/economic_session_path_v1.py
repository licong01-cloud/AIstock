"""D-visible overnight/intraday decomposition, not a T-open or execution forecast."""
import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import _number, _project
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar, volume_context_requests_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

SESSION_PATH_FEATURES = ('overnight_log_return_mean19', 'intraday_log_return_mean19', 'overnight_absolute_log_return_mean19')
STATUS = 'session_path_feature_status'
CLOCK = 'session_path_feature_visible_through'
UNKNOWN = 'session_path_unknown_fields'


class SessionPathPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_session_path_v1'] = 'economic_session_path_v1'
    campaign_id: Literal['advisory_session_path_v1_20261005'] = 'advisory_session_path_v1_20261005'
    model_id: Literal['M12'] = 'M12'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'session_path_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('session path original budget/predecessor anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('session path anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-OVERNIGHT-INTRADAY-PATH-1', d_features=list(D_FEATURES), information_features=list(SESSION_PATH_FEATURES),
            stock_source_columns=['trade_date', 'instrument', 'raw_open_cny', 'raw_close_cny', 'adj_factor'],
            window_sessions=20, effective_intervals=19, first_open_consumed=False, source_select_budget=0,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=47, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advsessionpathvalue_'+self.plan_sha256[:24]


def session_path_rows_v1(*, candidates, prices, calendar):
    roster, days = _roster_calendar(candidates, calendar)
    if len(prices) > 500000:
        raise ValueError('session path source row budget exceeded')
    columns = ['trade_date', 'instrument', 'raw_open_cny', 'raw_close_cny', 'adj_factor']
    pairs = set(volume_context_requests_v1(candidates=roster, calendar=days))
    stock = _project(prices, columns, columns[:2], pairs).set_index(columns[:2])
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        window = days[max(0, position-19):position+1]
        values = [np.nan]*3
        reasons = dict.fromkeys(SESSION_PATH_FEATURES, 'UNKNOWN_20D_HISTORY')
        if len(window) == 20:
            index = pd.MultiIndex.from_tuples([(session, symbol) for session in window], names=columns[:2])
            group = stock.reindex(index)
            closes = group.raw_close_cny.map(lambda value: _number(value, positive=True)).to_numpy(dtype=float)
            factors = group.adj_factor.map(lambda value: _number(value, positive=True)).to_numpy(dtype=float)
            # Open on session 0 is not consumed by the 19-interval features.
            opens = group.raw_open_cny.iloc[1:].map(lambda value: _number(value, positive=True)).to_numpy(dtype=float)
            reasons = dict.fromkeys(SESSION_PATH_FEATURES, 'UNKNOWN_SESSION_PATH_SOURCE')
            if np.isfinite(opens).all() and np.isfinite(closes[1:]).all():
                intraday = np.log(closes[1:])-np.log(opens)
                values[1] = float(intraday.mean())
            if np.isfinite(opens).all() and np.isfinite(closes[:-1]).all() and np.isfinite(factors).all():
                overnight = np.log(opens)-np.log(closes[:-1])+np.diff(np.log(factors))
                values[0], values[2] = float(overnight.mean()), float(np.abs(overnight).mean())
            if any(pd.notna(value) and not np.isfinite(value) for value in values):
                raise ValueError('session path derived log geometry overflow')
        unknown = {name: reasons[name] for name, value in zip(SESSION_PATH_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(SESSION_PATH_FEATURES, values, strict=True)),
            STATUS: 'AVAILABLE' if not unknown else next(iter(unknown.values())), CLOCK: day,
            UNKNOWN: json.dumps(unknown, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *SESSION_PATH_FEATURES, STATUS, CLOCK, UNKNOWN])


def session_path_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M12')


def session_path_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M12', information_features=SESSION_PATH_FEATURES)


def train_session_path_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M12', information_features=SESSION_PATH_FEATURES, status_column=STATUS)
