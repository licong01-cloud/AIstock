"""Pure D-visible selection history; missing lists are NOT absent securities."""
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

STATE_FEATURES = ('selection_top5_rate5', 'selection_top20_rate20', 'selection_censored_rank_improvement1')


class SelectionStatePricePlanV1(PriceCampaignPlanV2):
    """One future fixed study, not permission to submit an experiment now."""
    schema_version: Literal['economic_selection_state_price_value_v1'] = 'economic_selection_state_price_value_v1'
    campaign_id: Literal['advisory_price_selection_state_v1_20261004'] = 'advisory_price_selection_state_v1_20261004'
    model_id: Literal['M5'] = 'M5'
    budget_anchor_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_budget_anchor(self):
        path = Path(self.budget_anchor_ref.artifact_uri)
        if (self.budget_anchor_ref.role != 'price_campaign_budget_anchor' or not path.is_absolute()
                or path.name != 'manifest.json' or path.parent.name != 'preregistered'
                or path.drive.upper() == 'C:'):
            raise ValueError('selection state requires original campaign budget anchor')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-SELECTION-STATE-VALUE-1', d_features=list(D_FEATURES), information_features=list(STATE_FEATURES),
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=19, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advselectionvalue_'+self.plan_sha256[:24]


def selection_state_rows_v1(*, candidates, rankings, calendar):
    fields = [*KEY, 'selection_effective_rank']
    roster = _frame(candidates.loc[:, fields], KEY, set(fields))
    if roster.empty or len(roster) > 7720 or len(rankings) > 500000:
        raise ValueError('selection state population budget differs')
    days = pd.DatetimeIndex([_day(day) for day in calendar])
    if not days.is_unique or not days.is_monotonic_increasing or len(days) < 2:
        raise ValueError('selection state original calendar differs')
    if (not roster[KEY[0]].isin(days[:-1]).all() or not roster[KEY[0]].lt(roster[KEY[1]]).all()
            or not roster.selection_effective_rank.between(1, 20).all()
            or roster.selection_effective_rank.mod(1).ne(0).any()
            or roster.selection_effective_rank.map(lambda v: isinstance(v, (bool, np.bool_))).any()
            or roster.duplicated([KEY[0], 'selection_effective_rank']).any()):
        raise ValueError('selection state candidate rank/clock differs')
    next_day = dict(zip(days[:-1], days[1:], strict=True))
    if not roster.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all():
        raise ValueError('selection state candidate is not next-session T')
    history = rankings.loc[:, fields].copy()
    history[KEY[0]] = pd.to_datetime(history[KEY[0]])
    # Do not validate/read later ranks, scores or labels to determine D status.
    history = history.loc[history[KEY[0]].le(roster[KEY[0]].max())]
    history = _frame(history, KEY, set(fields))
    if (not history[KEY[0]].isin(days[:-1]).all() or not history.selection_effective_rank.between(1, 40).all()
            or history.selection_effective_rank.mod(1).ne(0).any()
            or history.selection_effective_rank.map(lambda v: isinstance(v, (bool, np.bool_))).any()
            or history.duplicated([KEY[0], 'selection_effective_rank']).any()
            or not history.apply(lambda row: next_day[row[KEY[0]]] == row[KEY[1]], axis=1).all()):
        raise ValueError('selection state historical rank/clock contradicts source')
    complete = {}
    for day, group in history.groupby(KEY[0]):
        if len(group) > 40:
            raise ValueError('selection state has more than frozen Top40')
        if len(group) == 40:
            if set(group.selection_effective_rank) != set(range(1, 41)):
                raise ValueError('selection state complete Top40 has inconsistent ranks')
            complete[day] = group.set_index(KEY[2]).selection_effective_rank.to_dict()
    output = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        position = days.get_loc(day)
        status, block = 'UNKNOWN_20D_WARMUP', [None]*3
        if day in complete and complete[day].get(symbol) != item['selection_effective_rank']:
            raise ValueError('selection state current candidate differs from frozen Top20')
        if day not in complete:
            status = 'UNKNOWN_CURRENT_LIST'
        elif position >= 19:
            window = days[position-19:position+1]
            if any(prior not in complete for prior in window):
                status = 'UNKNOWN_HISTORICAL_LIST'
            else:
                # Absence in a PROVED complete Top40 is an observed censored
                # state, not an imputed full-universe rank or missing-price fill.
                recent5 = sum(complete[prior].get(symbol, 41) <= 5 for prior in window[-5:])/5
                recent20 = sum(complete[prior].get(symbol, 41) <= 20 for prior in window)/20
                improvement = complete[window[-2]].get(symbol, 41)-item['selection_effective_rank']
                status, block = 'AVAILABLE', [float(recent5), float(recent20), float(improvement)]
        output.append({**{key: item[key] for key in KEY}, **dict(zip(STATE_FEATURES, block, strict=True)),
            'state_feature_status': status, 'state_feature_visible_through': day})
    return pd.DataFrame(output)
