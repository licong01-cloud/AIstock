"""D-only valuation context for entry price value; not alpha or promised returns."""
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
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

VALUATION_FEATURES = ('earnings_yield_ttm_D', 'book_to_price_D', 'dividend_yield_ttm_D')
STATUS = 'valuation_feature_status'
CLOCK = 'valuation_feature_visible_through'
UNKNOWN = 'valuation_unknown_fields'


class ValuationContextPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_valuation_context_v1'] = 'economic_valuation_context_v1'
    campaign_id: Literal['advisory_valuation_context_v1_20261005'] = 'advisory_valuation_context_v1_20261005'
    model_id: Literal['M14'] = 'M14'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'valuation_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('valuation original budget/predecessor anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('valuation anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-DAILY-VALUATION-CONTEXT-1', d_features=list(D_FEATURES), information_features=list(VALUATION_FEATURES),
            valuation_source_columns=['trade_date', 'instrument', 'pe_ttm', 'pb', 'dv_ttm'],
            valuation_source='market.daily_basic', ratio_unit='dimensionless', dividend_unit='percent', D_only=True,
            source_select_budget=1, source_read_seconds=30, maximum_valuation_rows=7720,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=55, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advvaluationvalue_'+self.plan_sha256[:24]


def valuation_requests_v1(*, candidates, calendar):
    roster, _ = _roster_calendar(candidates, calendar)
    return sorted(set(roster[[KEY[0], KEY[2]]].itertuples(index=False, name=None)))


def _valuation_number(value, *, dividend=False):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('valuation consumed value is not real numeric or NULL')
    if isinstance(value, Decimal) and value.is_nan():
        raise ValueError('valuation malformed Decimal is not NULL')
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError('valuation consumed value cannot be represented') from exc
    if np.isnan(number):
        return number
    if not np.isfinite(number) or (dividend and number < 0) or (number == 0 and value != 0):
        raise ValueError('valuation consumed value has invalid finite range or underflow')
    return number


def valuation_context_rows_v1(*, candidates, basics, calendar):
    roster, _ = _roster_calendar(candidates, calendar)
    if len(basics) > 7720:
        raise ValueError('valuation source row budget exceeded')
    columns = ['trade_date', 'instrument', 'pe_ttm', 'pb', 'dv_ttm']
    pairs = set(valuation_requests_v1(candidates=roster, calendar=calendar))
    source = _project(basics, columns, columns[:2], pairs).set_index(columns[:2])
    records = []
    for item in roster.to_dict('records'):
        day, symbol = item[KEY[0]], item[KEY[2]]
        values = [np.nan]*3
        reasons = dict.fromkeys(VALUATION_FEATURES, 'UNKNOWN_VALUATION_SOURCE')
        if (day, symbol) in source.index:
            observed = source.loc[(day, symbol)]
            for position, field in enumerate(('pe_ttm', 'pb', 'dv_ttm')):
                number = _valuation_number(observed[field], dividend=position == 2)
                if np.isnan(number):
                    continue
                if position < 2 and number == 0:
                    reasons[VALUATION_FEATURES[position]] = 'UNKNOWN_ZERO_VALUATION_DENOMINATOR'
                    continue
                value = number/100. if position == 2 else 1./number
                if not np.isfinite(value) or (value == 0 and number != 0):
                    raise ValueError('valuation derived coordinate overflow/underflow')
                values[position] = value
        unknown = {name: reasons[name] for name, value in zip(VALUATION_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(VALUATION_FEATURES, values, strict=True)),
            STATUS: 'AVAILABLE' if not unknown else next(iter(unknown.values())), CLOCK: day,
            UNKNOWN: json.dumps(unknown, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *VALUATION_FEATURES, STATUS, CLOCK, UNKNOWN])

def valuation_context_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M14')


def valuation_context_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M14', information_features=VALUATION_FEATURES)


def train_valuation_context_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M14', information_features=VALUATION_FEATURES, status_column=STATUS)
