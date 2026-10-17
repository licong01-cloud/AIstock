"""One frozen sector/parent-raw union, same supervision, matched16/candidate18."""
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_score_v1 import CLOCK as RAW_CLOCK, RAW_FEATURES, STATUS as RAW_STATUS, ParentRawScorePlanV1, _raw_columns
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import _number
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

JOINT_FEATURES = (*SECTOR_FEATURES, *RAW_FEATURES)
STATUS = 'sector_parent_raw_feature_status'
CLOCK = 'sector_parent_raw_feature_visible_through'
SOURCE_STATUS = ('sector_feature_status', RAW_STATUS)
SOURCE_CLOCK = ('sector_feature_visible_through', RAW_CLOCK)


class SectorParentRawPlanV1(ParentRawScorePlanV1):
    schema_version: Literal['economic_sector_parent_raw_v1'] = 'economic_sector_parent_raw_v1'
    campaign_id: Literal['advisory_sector_parent_raw_v1_20261005'] = 'advisory_sector_parent_raw_v1_20261005'
    model_id: Literal['M22'] = 'M22'
    sector_prepared_manifest_ref: EvidenceReferenceV1
    raw_prepared_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_raw_sources(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'sector_parent_raw_predecessor', 'evaluated'),
            (self.sector_prepared_manifest_ref, 'sector_parent_raw_sector_snapshot', 'prepared'),
            (self.raw_prepared_manifest_ref, 'sector_parent_raw_score_snapshot', 'prepared')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('sector parent raw original source/budget anchor differs')
        _raw_columns(self.component_raw_columns)
        return self

    @property
    def parameters(self):
        return {**super().parameters, 'hypothesis': 'H-SECTOR-PARENT-RAW-CONDITIONAL-PRICE-1',
            'information_features': list(JOINT_FEATURES), 'matched_information_features': list(SECTOR_FEATURES),
            'matched_dimension': 16, 'candidate_dimension': 18, 'campaign_fit_budget': 87,
            'source': 'EXISTING_FROZEN_M1_M20_PREPARED'}

    @property
    def experiment_id(self):
        return 'advsectorraw_'+self.plan_sha256[:24]


def sector_parent_raw_rows_v1(*, sector_rows, raw_rows, candidates, calendar):
    roster, _ = _roster_calendar(candidates, calendar)
    expected = set(roster[KEY].itertuples(index=False, name=None))
    if not len(roster) or len(sector_rows) > 7720 or len(raw_rows) > 7720:
        raise ValueError('sector parent raw original population/budget differs')
    for source in (sector_rows, raw_rows):
        if source.duplicated(KEY).any() or set(source[KEY].itertuples(index=False, name=None)) != expected:
            raise ValueError('sector parent raw exact original keys differ')
    fields = [*KEY, *RAW_FEATURES, RAW_STATUS, RAW_CLOCK]
    if set((*fields[3:], STATUS, CLOCK)) & set(sector_rows.columns):
        raise ValueError('sector parent raw source field collision')
    # M1 is the only base/Y source; do not consume a second raw-source Y/base.
    rows = sector_rows.merge(raw_rows.loc[:, fields], on=KEY, how='left', validate='one_to_one', sort=False)
    if not rows[KEY].equals(sector_rows[KEY].reset_index(drop=True)):
        raise ValueError('sector parent raw original order changed')
    for clock, status in zip(SOURCE_CLOCK, SOURCE_STATUS, strict=True):
        visible = pd.to_datetime(rows[clock], errors='raise')
        if (visible.isna().any() or not visible.le(rows[KEY[0]]).all()
                or not rows[status].map(lambda value: isinstance(value, str) and (value == 'AVAILABLE' or value.startswith('UNKNOWN'))).all()):
            raise ValueError('sector parent raw source clock/status differs')
    rows.loc[:, JOINT_FEATURES] = rows.loc[:, JOINT_FEATURES].map(_number)
    available = rows.loc[:, SOURCE_STATUS].eq('AVAILABLE').all(axis=1) & np.isfinite(rows.loc[:, JOINT_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    rows[CLOCK] = rows[KEY[0]]  # Query cut, not restored vintage capture evidence.
    return rows


def sector_parent_raw_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('sector_parent_raw_features') != list(JOINT_FEATURES)
            or recipe.get('matched_information_features') != list(SECTOR_FEATURES)
            or recipe.get('d_features') != list(D_FEATURES)
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (16 if name.startswith('matched') else 18) for name, body in models.items())):
        raise ValueError('sector parent raw actual 16/18 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M22')


def sector_parent_raw_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('sector parent raw query arm/index/budget differs')
    if sector_parent_raw_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('sector parent raw fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *JOINT_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('sector parent raw query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(JOINT_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M22',
        information_features=JOINT_FEATURES, matched_information_features=SECTOR_FEATURES)


def train_sector_parent_raw_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M22', information_features=JOINT_FEATURES, status_column=STATUS,
        matched_information_features=SECTOR_FEATURES)


def sector_parent_raw_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if sector_parent_raw_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('sector parent raw fitted identity changed')
    features = {name: _number(value) for name, value in d_features.items()}
    return information_price_set_v1(fitted=fitted, d_features=features, arm=arm,
        reference_cny=reference_cny, legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny,
        tick_cny=tick_cny, model_id='M22', information_features=JOINT_FEATURES, matched_information_features=SECTOR_FEATURES)
