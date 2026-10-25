"""Original candidate-cohort D returns as context, never label-selected or reranked."""
from decimal import Decimal
import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import GBDT, PriceCampaignPlanV2
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import _project
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

CANDIDATE_COHORT_FEATURES = ('cohort_ret1_mean', 'cohort_ret1_std', 'cohort_advance_share')
SOURCE_COLUMNS = [*KEY, 'ret_1', 'feature_visible_through']
STATUS = 'candidate_cohort_feature_status'
CLOCK = 'candidate_cohort_feature_visible_through'
UNKNOWN = 'candidate_cohort_unknown_fields'


class CandidateCohortPlanV1(PriceCampaignPlanV2):
    schema_version: Literal['economic_candidate_cohort_v1'] = 'economic_candidate_cohort_v1'
    campaign_id: Literal['advisory_candidate_cohort_v1_20261005'] = 'advisory_candidate_cohort_v1_20261005'
    model_id: Literal['M16'] = 'M16'
    budget_anchor_ref: EvidenceReferenceV1
    predecessor_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_anchors(self):
        for ref, role, stage in ((self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
                                (self.predecessor_manifest_ref, 'candidate_cohort_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:' or path.name != 'manifest.json' or path.parent.name != stage:
                raise ValueError('candidate cohort original budget/predecessor anchor differs')
            if path.parent.parent.parent.resolve() != self.campaign_root.resolve():
                raise ValueError('candidate cohort anchor cannot relocate cumulative budget')
        return self

    @property
    def campaign_root(self):
        return Path(self.budget_anchor_ref.artifact_uri).parent.parent.parent

    @property
    def parameters(self):
        return dict(hypothesis='H-CANDIDATE-COHORT-PRICE-1', d_features=list(D_FEATURES), information_features=list(CANDIDATE_COHORT_FEATURES),
            cohort_source_columns=list(SOURCE_COLUMNS), source='ORIGINAL_FROZEN_D_CANDIDATE_FEATURES',
            information_units=['fraction', 'fraction', 'dimensionless'], source_select_budget=0, ddof=0,
            gbdt=dict(GBDT), path_quantile=.1, sklearn_version='1.8.0', scipy_version='1.16.3',
            physical_fit_budget=4, campaign_fit_budget=63, index_build_count=0, candidate_count=1,
            minimum_train_rows=100, minimum_train_days=20, maximum_rows=7720, maximum_source_rows=7720, maximum_price_rows=500000,
            fit_budget_seconds=1800, maximum_rss_bytes=2*1024**3, maximum_artifact_bytes=2*1024**3,
            economic_increment_bps=5., minimum_model_take_episodes=30, minimum_intervention_days=12,
            minimum_intervention_ratio=.15, mdd_deterioration_bps=200., tail_deterioration_bps=20.,
            policy_sha256=value_anchor_policy_sha256_v1(), cost_sha256=COST.policy_sha256)

    @property
    def experiment_id(self):
        return 'advcandidatecohort_'+self.plan_sha256[:24]


def _number(value):
    if value is None or value is pd.NA:
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        raise ValueError('candidate cohort consumed return is not numeric or NULL')
    if isinstance(value, Decimal) and value.is_qnan():
        return np.nan
    if isinstance(value, Decimal) and value.is_snan():
        raise ValueError('candidate cohort signaling NaN is not NULL')
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError('candidate cohort consumed return cannot be represented') from exc
    if np.isnan(number):
        return number
    if not np.isfinite(number) or number <= -1 or number == 0 and value != 0:
        raise ValueError('candidate cohort return contradicts positive price ratio or representation')
    return number


def _statistics(values):
    scale = float(np.abs(values).max())
    normalized = values/scale if scale else values
    center = float(normalized.mean())
    result = [center*scale, float(np.sqrt(np.mean((normalized-center)**2)))*scale, float(np.mean(values > 0))]
    if not np.isfinite(result).all():
        raise ValueError('candidate cohort derived statistics cannot be represented')
    return result


def candidate_cohort_rows_v1(*, candidates, features, calendar):
    roster, _ = _roster_calendar(candidates, calendar)
    if len(features) > 7720:
        raise ValueError('candidate cohort source row budget exceeded')
    allowed = set(roster[KEY].itertuples(index=False, name=None))
    source = _project(features, SOURCE_COLUMNS, KEY, allowed)
    source['ret_1'] = source.ret_1.map(_number).astype(float)
    source['feature_visible_through'] = pd.to_datetime(source.feature_visible_through.map(lambda value: pd.NaT if pd.isna(value) else _day(value)))
    if (source.feature_visible_through.notna() & source.feature_visible_through.gt(source[KEY[0]])).any():
        raise ValueError('candidate cohort consumed future feature clock')
    source = source.set_index(KEY)
    groups = {}
    for day, group in roster.groupby(KEY[0], sort=False):
        index = pd.MultiIndex.from_tuples(list(group[KEY].itertuples(index=False, name=None)), names=KEY)
        observed = source.reindex(index)
        values = observed.ret_1.to_numpy(dtype=float)
        available = np.isfinite(values).all() and observed.feature_visible_through.notna().all()
        groups[day] = _statistics(values) if available else [np.nan]*3
    records = []
    for item in roster.to_dict('records'):
        values = groups[item[KEY[0]]]
        unknown = {name: 'UNKNOWN_COHORT_SOURCE' for name, value in zip(CANDIDATE_COHORT_FEATURES, values, strict=True) if np.isnan(value)}
        records.append({**item, **dict(zip(CANDIDATE_COHORT_FEATURES, values, strict=True)),
            STATUS: 'AVAILABLE' if not unknown else 'UNKNOWN_COHORT_SOURCE', CLOCK: item[KEY[0]],
            UNKNOWN: json.dumps(unknown, sort_keys=True, separators=(',', ':'))})
    return pd.DataFrame(records, columns=[*KEY, *CANDIDATE_COHORT_FEATURES, STATUS, CLOCK, UNKNOWN])


def candidate_cohort_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    return information_fit_identity_v1(recipe, models, support, model_id='M16')


def candidate_cohort_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    return information_nodes_v1(fitted=fitted, rows=rows, arm=arm, model_id='M16', information_features=CANDIDATE_COHORT_FEATURES)


def train_candidate_cohort_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M16', information_features=CANDIDATE_COHORT_FEATURES, status_column=STATUS)
