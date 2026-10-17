"""Historical normalized forecast change beyond raw trajectories; 17 versus 19."""
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_score_v1 import _raw_columns
from backend.services.advisory_model_first.economic_parent_raw_trajectory_v1 import CLOCK as TRAJECTORY_CLOCK, LAG_TRADE_DAYS, STATUS as TRAJECTORY_STATUS, TRAJECTORY_FEATURES, ParentRawTrajectoryPlanV1
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import _number
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

NORM_DELTA_FEATURES = ('parent_lstm_norm_delta5_D', 'parent_fund_norm_delta5_D')
INFORMATION_FEATURES = (*TRAJECTORY_FEATURES, *NORM_DELTA_FEATURES)
STATUS = 'parent_normalized_trajectory_feature_status'
CLOCK = 'parent_normalized_trajectory_feature_visible_through'


class ParentNormalizedTrajectoryPlanV1(ParentRawTrajectoryPlanV1):
    schema_version: Literal['economic_parent_normalized_trajectory_v1'] = 'economic_parent_normalized_trajectory_v1'
    campaign_id: Literal['advisory_parent_normalized_trajectory_v1_20261006'] = 'advisory_parent_normalized_trajectory_v1_20261006'
    model_id: Literal['M24'] = 'M24'
    trajectory_prepared_manifest_ref: EvidenceReferenceV1

    @model_validator(mode='after')
    def validate_raw_sources(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'parent_normalized_trajectory_predecessor', 'evaluated'),
            (self.raw_prepared_manifest_ref, 'parent_normalized_trajectory_raw_snapshot', 'prepared'),
            (self.trajectory_prepared_manifest_ref, 'parent_normalized_trajectory_base_snapshot', 'prepared')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('parent normalized trajectory original anchor differs')
        _raw_columns(self.component_raw_columns)
        return self

    @property
    def parameters(self):
        return {**super().parameters, 'hypothesis': 'H-PARENT-NORMALIZED-TRAJECTORY-CONDITIONAL-PRICE-1',
            'information_features': list(INFORMATION_FEATURES), 'matched_information_features': list(TRAJECTORY_FEATURES),
            'matched_dimension': 17, 'candidate_dimension': 19, 'campaign_fit_budget': 95,
            'source': 'EXISTING_FROZEN_PARENT_NORMALIZED_TRAJECTORY'}

    @property
    def experiment_id(self):
        return 'advnormtraj_'+self.plan_sha256[:24]


def parent_normalized_trajectory_rows_v1(*, base_rows, rankings, candidates, calendar,
        component_raw_columns, package_id, package_manifest_sha256):
    norm_columns = [name.replace('raw__', 'norm__', 1) for name in _raw_columns(component_raw_columns)]
    roster, days = _roster_calendar(candidates, calendar)
    expected = set(roster[KEY].itertuples(index=False, name=None))
    if (not len(roster) or len(base_rows) > 7720 or len(rankings) > 20000 or base_rows.duplicated(KEY).any()
            or set(base_rows[KEY].itertuples(index=False, name=None)) != expected
            or set((*NORM_DELTA_FEATURES, STATUS, CLOCK)) & set(base_rows.columns)):
        raise ValueError('parent normalized trajectory original keys/budget/field collision differs')
    rows = base_rows.reset_index(drop=True).copy()
    if (not pd.to_datetime(rows[TRAJECTORY_CLOCK]).eq(rows[KEY[0]]).all()
            or not rows[TRAJECTORY_STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all()):
        raise ValueError('parent normalized trajectory original raw clock/status differs')
    rows.loc[:, TRAJECTORY_FEATURES] = rows.loc[:, TRAJECTORY_FEATURES].map(_number)
    requested = []
    for d, _, instrument in rows[KEY].itertuples(index=False, name=None):
        idx = days.get_loc(d)
        requested.append(None if idx < LAG_TRADE_DAYS else (days[idx-LAG_TRADE_DAYS], days[idx-LAG_TRADE_DAYS+1], instrument))
    allowed = expected | {key for key in requested if key is not None}
    pairs = {(key[0], key[2]) for key in allowed}
    fields = [*KEY, *norm_columns, 'trade_date', 'package_id', 'manifest_sha256']
    # Exact original current/lag requests, not a recomputed cross-sectional zscore.
    selected = rankings.loc[rankings[[KEY[0], KEY[2]]].apply(tuple, axis=1).isin(pairs), fields].copy()
    selected_keys = set(selected[KEY].itertuples(index=False, name=None))
    if (selected.duplicated(KEY).any() or not selected_keys <= allowed or not expected <= selected_keys
            or not pd.to_datetime(selected.trade_date).eq(selected[KEY[0]]).all()
            or not selected.package_id.eq(package_id).all()
            or not selected.manifest_sha256.eq(package_manifest_sha256).all()):
        raise ValueError('parent normalized trajectory requested key/clock/package identity differs')
    selected.loc[:, norm_columns] = selected.loc[:, norm_columns].map(_number)
    values = {tuple(row[:3]): np.asarray(row[3:], dtype=float)
        for row in selected.loc[:, [*KEY, *norm_columns]].itertuples(index=False, name=None)}
    delta = np.full((len(rows), 2), np.nan)
    for i, (key, past_key) in enumerate(zip(rows[KEY].itertuples(index=False, name=None), requested, strict=True)):
        current, previous = values[key], values.get(past_key)
        if previous is not None and np.isfinite(current).all() and np.isfinite(previous).all():
            with np.errstate(over='ignore', invalid='ignore'):
                change = current-previous
            if not np.isfinite(change).all():
                raise ValueError('parent normalized trajectory finite difference overflow')
            delta[i] = change
    rows.loc[:, NORM_DELTA_FEATURES] = delta
    available = rows[TRAJECTORY_STATUS].eq('AVAILABLE') & np.isfinite(rows.loc[:, INFORMATION_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    rows[CLOCK] = rows[KEY[0]]  # D query cutoff, not original vintage capture evidence.
    return rows


def parent_normalized_trajectory_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('parent_normalized_trajectory_features') != list(INFORMATION_FEATURES)
            or recipe.get('matched_information_features') != list(TRAJECTORY_FEATURES)
            or recipe.get('d_features') != list(D_FEATURES)
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (17 if name.startswith('matched') else 19) for name, body in models.items())):
        raise ValueError('parent normalized trajectory actual 17/19 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M24')


def parent_normalized_trajectory_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('parent normalized trajectory query arm/index/budget differs')
    if parent_normalized_trajectory_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent normalized trajectory fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *INFORMATION_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('parent normalized trajectory query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(INFORMATION_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M24',
        information_features=INFORMATION_FEATURES, matched_information_features=TRAJECTORY_FEATURES)


def train_parent_normalized_trajectory_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M24', information_features=INFORMATION_FEATURES, status_column=STATUS,
        matched_information_features=TRAJECTORY_FEATURES)


def parent_normalized_trajectory_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if parent_normalized_trajectory_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent normalized trajectory fitted identity changed')
    return information_price_set_v1(fitted=fitted, d_features={name: _number(value) for name, value in d_features.items()}, arm=arm,
        reference_cny=reference_cny, legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny,
        tick_cny=tick_cny, model_id='M24', information_features=INFORMATION_FEATURES, matched_information_features=TRAJECTORY_FEATURES)
