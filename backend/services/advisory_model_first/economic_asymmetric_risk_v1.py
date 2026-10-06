"""Historical upside/downside daily risk as conditional price context, no new SQL."""
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

ASYMMETRIC_RISK_FEATURES = ('downside_log_return_rms19', 'upside_log_return_rms19', 'downside_adjacent_log_return_energy18')
SOURCE_COLUMNS = ['trade_date', 'instrument', 'raw_close_cny', 'adj_factor']
STATUS = 'asymmetric_risk_feature_status'
CLOCK = 'asymmetric_risk_feature_visible_through'
UNKNOWN = 'asymmetric_risk_unknown_fields'


class AsymmetricRiskPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_asymmetric_risk_v1'] = 'economic_asymmetric_risk_v1'
    campaign_id: Literal['advisory_asymmetric_risk_v1_20261005'] = 'advisory_asymmetric_risk_v1_20261005'
    model_id: Literal['M18'] = 'M18'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'asymmetric_risk_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('asymmetric risk original budget/predecessor anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('asymmetric risk anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-ASYMMETRIC-HISTORICAL-RISK-PRICE-1', d_features=list(D_FEATURES), information_features=list(ASYMMETRIC_RISK_FEATURES),
            stock_source_columns=list(SOURCE_COLUMNS), source='ORIGINAL_FROZEN_PARENT_PRICE',
            information_units=['fraction', 'fraction', 'fraction'], source_select_budget=0, window_sessions=20, effective_returns=19,
            price_unit='cny', adjusted_log_close='log_raw_close_plus_log_same_session_adj_factor',
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=71, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advasymrisk_'+self.plan_sha256[:24]


def _positive(value):
    if value is None or value is pd.NA or (isinstance(value, Decimal) and value.is_qnan()):
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('asymmetric risk value must be real numeric or NULL')
    try:
        result = float(value)
    except (OverflowError, ValueError, TypeError) as error:
        raise ValueError('asymmetric risk malformed numeric') from error
    if np.isnan(result) and not isinstance(value, Decimal):
        return np.nan
    if not np.isfinite(result) or result <= 0:
        raise ValueError('asymmetric risk price/factor must be finite positive without underflow')
    return result


def _statistics(values):
    returns = np.diff(np.log(values).sum(axis=1))
    scale = np.abs(returns).max()
    if scale == 0:
        return [0., 0., 0.]
    normalized = returns/scale
    negative, positive = np.minimum(normalized, 0.), np.maximum(normalized, 0.)
    result = scale*np.sqrt([np.mean(negative**2), np.mean(positive**2), np.mean(negative[1:]*negative[:-1])])
    if not np.isfinite(result).all():
        raise ValueError('asymmetric risk derived statistics are not finite')
    return result.tolist()


def asymmetric_risk_rows_v1(*, candidates, prices, calendar):
    roster, days = _roster_calendar(candidates, calendar)
    if len(prices) > 500000:
        raise ValueError('asymmetric risk price source budget exceeded')
    pairs = set(volume_context_requests_v1(candidates=roster, calendar=days))
    observed = _project(prices, SOURCE_COLUMNS, SOURCE_COLUMNS[:2], pairs)
    observed.loc[:, SOURCE_COLUMNS[2:]] = observed.loc[:, SOURCE_COLUMNS[2:]].map(_positive)
    indexed = observed.set_index(SOURCE_COLUMNS[:2])
    records = []
    for item in roster.to_dict('records'):
        position = days.get_loc(item[KEY[0]])
        window = days[max(0, position-19):position+1]
        values, status = [np.nan]*3, 'UNKNOWN_20D_HISTORY'
        if len(window) == 20:
            keys = pd.MultiIndex.from_product([window, [item[KEY[2]]]], names=SOURCE_COLUMNS[:2])
            history = indexed.reindex(keys).loc[:, SOURCE_COLUMNS[2:]].to_numpy(dtype=float)
            status = 'UNKNOWN_PRICE_SOURCE'
            if np.isfinite(history).all():
                values, status = _statistics(history), 'AVAILABLE'
        missing = {name: status for name, value in zip(ASYMMETRIC_RISK_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(ASYMMETRIC_RISK_FEATURES, values, strict=True)), STATUS: status, CLOCK: item[KEY[0]],
            UNKNOWN: json.dumps(missing, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *ASYMMETRIC_RISK_FEATURES, STATUS, CLOCK, UNKNOWN])


def asymmetric_risk_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M18')


def asymmetric_risk_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M18', information_features=ASYMMETRIC_RISK_FEATURES)


def train_asymmetric_risk_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M18', information_features=ASYMMETRIC_RISK_FEATURES, status_column=STATUS)

