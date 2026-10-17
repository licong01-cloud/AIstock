"""Original own-raw five-trading-day change; shared 15 versus 17 inputs."""
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_score_v1 import CLOCK as RAW_CLOCK, RAW_FEATURES, STATUS as RAW_STATUS, ParentRawScorePlanV1, _raw_columns, parent_raw_score_rows_v1
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import _number
from backend.services.advisory_model_first.economic_volume_context_price_v1 import _roster_calendar
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1

LAG_TRADE_DAYS = 5
DELTA_FEATURES = ('parent_lstm_raw_delta5_D', 'parent_fund_raw_delta5_D')
TRAJECTORY_FEATURES = (*RAW_FEATURES, *DELTA_FEATURES)
STATUS = 'parent_raw_trajectory_feature_status'
CLOCK = 'parent_raw_trajectory_feature_visible_through'


class ParentRawTrajectoryPlanV1(ParentRawScorePlanV1):
    schema_version: Literal['economic_parent_raw_trajectory_v1'] = 'economic_parent_raw_trajectory_v1'
    campaign_id: Literal['advisory_parent_raw_trajectory_v1_20261005'] = 'advisory_parent_raw_trajectory_v1_20261005'
    model_id: Literal['M23'] = 'M23'
    raw_prepared_manifest_ref: EvidenceReferenceV1
    lag_trade_days: Literal[5] = 5

    @model_validator(mode='after')
    def validate_raw_sources(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'parent_raw_trajectory_predecessor', 'evaluated'),
            (self.raw_prepared_manifest_ref, 'parent_raw_trajectory_raw_snapshot', 'prepared')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('parent raw trajectory original anchor differs')
        _raw_columns(self.component_raw_columns)
        return self

    @property
    def parameters(self):
        return {**super().parameters, 'hypothesis': 'H-PARENT-RAW-TRAJECTORY-PRICE-1',
            'information_features': list(TRAJECTORY_FEATURES), 'matched_information_features': list(RAW_FEATURES),
            'matched_dimension': 15, 'candidate_dimension': 17, 'campaign_fit_budget': 91,
            'source': 'EXISTING_FROZEN_PARENT_RAW_TRAJECTORY', 'lag_trade_days': self.lag_trade_days,
            'lag_semantics': 'EXACT_ORIGINAL_CALENDAR_5_TRADE_DAYS_NOT_EFFECTIVE_REVIEWS'}

    @property
    def experiment_id(self):
        return 'advrawtraj_'+self.plan_sha256[:24]


def parent_raw_trajectory_rows_v1(*, base_rows, rankings, candidates, calendar,
        component_raw_columns, package_id, package_manifest_sha256):
    raw_columns = _raw_columns(component_raw_columns)
    if set((*DELTA_FEATURES, STATUS, CLOCK)) & set(base_rows.columns):
        raise ValueError('parent raw trajectory source field collision')
    roster, days = _roster_calendar(candidates, calendar)
    # Verify current values against the same immutable M20 source, not a rerun.
    rows = parent_raw_score_rows_v1(base_rows=base_rows.drop(columns=[*RAW_FEATURES, RAW_STATUS, RAW_CLOCK]),
        rankings=rankings, candidates=roster, calendar=days, component_raw_columns=component_raw_columns,
        package_id=package_id, package_manifest_sha256=package_manifest_sha256)
    for name in RAW_FEATURES:
        if not np.array_equal(base_rows[name].map(_number).to_numpy(dtype=float), rows[name].to_numpy(dtype=float), equal_nan=True):
            raise ValueError('parent raw trajectory frozen current raw differs')
    if (not pd.to_datetime(base_rows[RAW_CLOCK]).reset_index(drop=True).eq(rows[KEY[0]]).all()
            or not base_rows[RAW_STATUS].reset_index(drop=True).equals(rows[RAW_STATUS])):
        raise ValueError('parent raw trajectory current raw clock/status differs')
    requested = []
    for d, _, instrument in rows[KEY].itertuples(index=False, name=None):
        idx = days.get_loc(d)
        requested.append(None if idx < LAG_TRADE_DAYS else (days[idx-LAG_TRADE_DAYS], days[idx-LAG_TRADE_DAYS+1], instrument))
    allowed = {key for key in requested if key is not None}
    requested_stocks = {(key[0], key[2]) for key in allowed}
    fields = [*KEY, *raw_columns, 'trade_date', 'package_id', 'manifest_sha256']
    # Missing historical keys are ordinary UNKNOWN; unrequested poison is never parsed.
    selected = rankings.loc[rankings[[KEY[0], KEY[2]]].apply(tuple, axis=1).isin(requested_stocks), fields].copy()
    if (not selected[KEY].apply(tuple, axis=1).isin(allowed).all()
            or selected.duplicated(KEY).any() or not pd.to_datetime(selected.trade_date).eq(selected[KEY[0]]).all()
            or not selected.package_id.eq(package_id).all()
            or not selected.manifest_sha256.eq(package_manifest_sha256).all()):
        raise ValueError('parent raw trajectory requested lag key/clock/package identity differs')
    selected.loc[:, raw_columns] = selected.loc[:, raw_columns].map(_number)
    past = {tuple(row[:3]): np.asarray(row[3:], dtype=float)
        for row in selected.loc[:, [*KEY, *raw_columns]].itertuples(index=False, name=None)}
    delta = np.full((len(rows), 2), np.nan)
    current = rows.loc[:, RAW_FEATURES].to_numpy(dtype=float)
    for i, key in enumerate(requested):
        previous = past.get(key)
        if previous is not None and np.isfinite(previous).all() and np.isfinite(current[i]).all():
            with np.errstate(over='ignore', invalid='ignore'):
                change = current[i]-previous
            if not np.isfinite(change).all():
                raise ValueError('parent raw trajectory finite difference overflow')
            delta[i] = change
    rows.loc[:, DELTA_FEATURES] = delta
    available = rows[RAW_STATUS].eq('AVAILABLE') & np.isfinite(rows.loc[:, TRAJECTORY_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    rows[CLOCK] = rows[KEY[0]]  # Query cutoff only, never a vintage capture assertion.
    return rows


def parent_raw_trajectory_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('parent_raw_trajectory_features') != list(TRAJECTORY_FEATURES)
            or recipe.get('matched_information_features') != list(RAW_FEATURES)
            or recipe.get('d_features') != list(D_FEATURES)
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (15 if name.startswith('matched') else 17) for name, body in models.items())):
        raise ValueError('parent raw trajectory actual 15/17 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M23')


def parent_raw_trajectory_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('parent raw trajectory query arm/index/budget differs')
    if parent_raw_trajectory_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent raw trajectory fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *TRAJECTORY_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('parent raw trajectory query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(TRAJECTORY_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M23',
        information_features=TRAJECTORY_FEATURES, matched_information_features=RAW_FEATURES)


def train_parent_raw_trajectory_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M23', information_features=TRAJECTORY_FEATURES, status_column=STATUS,
        matched_information_features=RAW_FEATURES)


def parent_raw_trajectory_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if parent_raw_trajectory_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent raw trajectory fitted identity changed')
    return information_price_set_v1(fitted=fitted, d_features={name: _number(value) for name, value in d_features.items()}, arm=arm,
        reference_cny=reference_cny, legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny,
        tick_cny=tick_cny, model_id='M23', information_features=TRAJECTORY_FEATURES, matched_information_features=RAW_FEATURES)
