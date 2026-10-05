"""Ordered original five-session large-flow paths as price context, no new SQL."""
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
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import moneyflow_calendar_v1
from backend.data_service.moneyflow_contract import MONEYFLOW_UNIT_CONTRACT_VERSION
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

FLOW_PATH_FEATURES = ('large_flow_positive_share5', 'large_flow_trailing_positive_run_share5', 'large_flow_mean_abs_imbalance_step5')
AMOUNT_FIELDS = ('buy_lg_amount', 'sell_lg_amount', 'buy_elg_amount', 'sell_elg_amount')
SOURCE_COLUMNS = ['trade_date', 'instrument', *AMOUNT_FIELDS]
STATUS = 'flow_path_feature_status'
CLOCK = 'flow_path_feature_visible_through'
UNKNOWN = 'flow_path_unknown_fields'


class FlowPathPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_flow_path_v1'] = 'economic_flow_path_v1'
    campaign_id: Literal['advisory_flow_path_v1_20261005'] = 'advisory_flow_path_v1_20261005'
    model_id: Literal['M17'] = 'M17'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1
    flow_source_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'flow_path_predecessor', 'evaluated'),
                                (self.flow_source_manifest_ref, 'flow_path_source', 'prepared')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('flow path original budget/predecessor anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('flow path anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-ORDERED-FLOW-PERSISTENCE-PRICE-1', d_features=list(D_FEATURES), information_features=list(FLOW_PATH_FEATURES),
            flow_source_columns=list(SOURCE_COLUMNS), source='ORIGINAL_FROZEN_M6_CNY_FLOW',
            flow_source_manifest_sha256=self.flow_source_manifest_ref.sha256,
            information_units=['fraction', 'fraction', 'fraction'], source_select_budget=0, window_sessions=5,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=67, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_source_rows=38600, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advflowpath_'+self.plan_sha256[:24]


def _amount(value):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, Decimal) and value.is_qnan():
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('flow path amount must be real numeric or NULL')
    try:
        result = float(value)
    except (OverflowError, ValueError, TypeError) as error:
        raise ValueError('flow path malformed amount') from error
    if np.isnan(result) and not isinstance(value, Decimal):
        return np.nan
    if not np.isfinite(result) or result < 0 or (result == 0 and value != 0):
        raise ValueError('flow path amount must be finite nonnegative without underflow')
    return result


def _statistics(values):
    scale = values.max(axis=1)
    if (scale == 0).any():
        return None
    normalized = values / scale[:, None]
    imbalance = (normalized[:, 0] + normalized[:, 2] - normalized[:, 1] - normalized[:, 3]) / normalized.sum(axis=1)
    run = 0
    for value in imbalance[::-1]:
        if value <= 0:
            break
        run += 1
    return [float((imbalance > 0).mean()), run/5, float(np.abs(np.diff(imbalance)).mean())]


def flow_path_rows_v1(*, candidates, amounts, calendar):
    roster, _, windows = moneyflow_calendar_v1(candidates, calendar)
    if amounts.attrs.get('moneyflow_unit_contract') != MONEYFLOW_UNIT_CONTRACT_VERSION or amounts.attrs.get('moneyflow_amount_unit') != 'cny':
        raise ValueError('flow path requires existing once-normalized CNY identity')
    allowed = {(day, item[KEY[2]]) for item in roster.to_dict('records') for day in windows[item[KEY[0]]]}
    observed = _project(amounts, SOURCE_COLUMNS, ['trade_date', 'instrument'], allowed)
    if len(observed) > 38600:
        raise ValueError('flow path source budget exceeded')
    observed.loc[:, AMOUNT_FIELDS] = observed.loc[:, AMOUNT_FIELDS].map(_amount)
    indexed = observed.set_index(['trade_date', 'instrument'])
    records = []
    for item in roster.to_dict('records'):
        window = windows[item[KEY[0]]]
        values, status = [np.nan]*3, 'UNKNOWN_5D_HISTORY'
        if len(window) == 5:
            keys = pd.MultiIndex.from_product([window, [item[KEY[2]]]], names=['trade_date', 'instrument'])
            history = indexed.reindex(keys).loc[:, AMOUNT_FIELDS].to_numpy(dtype=float)
            status = 'UNKNOWN_FLOW_SOURCE'
            if np.isfinite(history).all():
                result = _statistics(history)
                status = 'UNKNOWN_ZERO_FLOW_DENOMINATOR' if result is None else 'AVAILABLE'
                if result is not None:
                    values = result
        missing = {name: status for name, value in zip(FLOW_PATH_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(FLOW_PATH_FEATURES, values, strict=True)), STATUS: status, CLOCK: item[KEY[0]],
            UNKNOWN: json.dumps(missing, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *FLOW_PATH_FEATURES, STATUS, CLOCK, UNKNOWN])


def flow_path_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M17')


def flow_path_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M17', information_features=FLOW_PATH_FEATURES)


def train_flow_path_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M17', information_features=FLOW_PATH_FEATURES, status_column=STATUS)
