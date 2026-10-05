"""Frozen cross-candidate scale proxies; common matched 15 versus candidate 19."""
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_parent_raw_score_v1 import CLOCK as RAW_CLOCK, RAW_FEATURES, STATUS as RAW_STATUS, ParentRawScorePlanV1, _raw_columns, parent_raw_score_rows_v1
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import _number

STATE_FEATURES = ('parent_lstm_raw_center_D', 'parent_lstm_raw_scale_D', 'parent_fund_raw_center_D', 'parent_fund_raw_scale_D')
SCALE_FEATURES = (*RAW_FEATURES, *STATE_FEATURES)
STATUS = 'parent_scale_state_feature_status'
CLOCK = 'parent_scale_state_feature_visible_through'


class ParentScaleStatePlanV1(ParentRawScorePlanV1):
    schema_version: Literal['economic_parent_scale_state_v1'] = 'economic_parent_scale_state_v1'
    campaign_id: Literal['advisory_parent_scale_state_v1_20261005'] = 'advisory_parent_scale_state_v1_20261005'
    model_id: Literal['M21'] = 'M21'
    score_dtypes: dict[str, Literal['float32', 'float64']]

    @model_validator(mode='after')
    def validate_raw_sources(self):
        for ref, role, stage in (
            (self.budget_anchor_ref, 'price_campaign_budget_anchor', 'preregistered'),
            (self.predecessor_manifest_ref, 'parent_scale_state_predecessor', 'evaluated')):
            path = Path(ref.artifact_uri)
            if (ref.role != role or not path.is_absolute() or path.drive.upper() == 'C:'
                    or path.name != 'manifest.json' or path.parent.name != stage
                    or path.parent.parent.parent.resolve() != self.campaign_root.resolve()):
                raise ValueError('parent scale state original anchor differs')
        _score_columns(self.component_raw_columns, self.score_dtypes)
        return self

    @property
    def parameters(self):
        return {**super().parameters, 'hypothesis': 'H-PARENT-CROSSSECTION-SCALE-STATE-PRICE-1',
            'information_features': list(SCALE_FEATURES), 'matched_information_features': list(RAW_FEATURES),
            'matched_dimension': 15, 'candidate_dimension': 19, 'campaign_fit_budget': 83,
            'source': 'EXISTING_FROZEN_PARENT_CROSSSECTION', 'score_dtypes': self.score_dtypes,
            'state_semantics': 'APPROXIMATE_FROZEN_SCALE_STATE_NOT_ORIGINAL_NATIVE_METADATA',
            'numeric_tolerance_dtype_epsilon_multiplier': 16}

    @property
    def experiment_id(self):
        return 'advscalestate_'+self.plan_sha256[:24]


def _score_columns(mapping, dtypes):
    raw = _raw_columns(mapping)
    norm = [name.replace('raw__', 'norm__', 1) for name in raw]
    if set(dtypes) != set((*raw, *norm)) or any(value not in ('float32', 'float64') for value in dtypes.values()):
        raise ValueError('parent scale state exact score dtype mapping differs')
    return raw, norm


def _moments(raw, norm, raw_dtype, norm_dtype):
    valid = np.isfinite(raw) & np.isfinite(norm)
    r, z = raw[valid], norm[valid]
    if len(r) < 2 or len(np.unique(z)) < 2:
        return np.nan, np.nan
    centered = z-z.mean()
    variance = np.dot(centered, centered)
    if not np.isfinite(variance) or variance <= 0:
        raise ValueError('parent scale state numeric variance is not computable')
    scale = np.dot(r-r.mean(), centered)/variance
    center = r.mean()-scale*z.mean()
    tolerance = 16*max(np.finfo(raw_dtype).eps, np.finfo(norm_dtype).eps)*max(1., np.max(np.abs(r)))
    residual = np.max(np.abs(r-(center+scale*z)))
    if not np.isfinite(center) or not np.isfinite(scale) or scale <= 0 or not np.isfinite(residual) or residual > tolerance:
        raise ValueError('parent scale state numeric relationship differs from frozen zscore')
    return center, scale


def parent_scale_state_rows_v1(*, base_rows, rankings, candidates, calendar, component_raw_columns,
        package_id, package_manifest_sha256, score_dtypes):
    raw, norm = _score_columns(component_raw_columns, score_dtypes)
    if set((*STATE_FEATURES, STATUS, CLOCK)) & set(base_rows.columns):
        raise ValueError('parent scale state source field collision')
    # Re-read the very same frozen predictions, never rerun the parent/Selection.
    rows = parent_raw_score_rows_v1(base_rows=base_rows.drop(columns=[*RAW_FEATURES, RAW_STATUS, RAW_CLOCK]),
        rankings=rankings, candidates=candidates, calendar=calendar, component_raw_columns=component_raw_columns,
        package_id=package_id, package_manifest_sha256=package_manifest_sha256)
    for name in RAW_FEATURES:
        previous = base_rows[name].map(_number).to_numpy(dtype=float)
        if not np.array_equal(previous, rows[name].to_numpy(dtype=float), equal_nan=True):
            raise ValueError('parent scale state frozen M20 raw base differs')
    if (not pd.to_datetime(base_rows[RAW_CLOCK]).reset_index(drop=True).eq(rows[KEY[0]]).all()
            or not base_rows[RAW_STATUS].reset_index(drop=True).equals(rows[RAW_STATUS])):
        raise ValueError('parent scale state frozen raw clock/status differs')
    expected = set(rows[KEY].itertuples(index=False, name=None))
    selected = rankings.loc[rankings[KEY].apply(tuple, axis=1).isin(expected), [*KEY, *norm]].copy()
    selected.loc[:, norm] = selected.loc[:, norm].map(_number)
    rows = rows.merge(selected, on=KEY, how='left', validate='one_to_one', sort=False)
    if not rows[KEY].equals(base_rows[KEY].reset_index(drop=True)):
        raise ValueError('parent scale state original order changed')
    rows.loc[:, STATE_FEATURES] = np.nan
    # D groups only: no expanding/rolling cross-date statistic and no outcomes.
    for _, indices in rows.groupby(KEY[0], sort=False).groups.items():
        for position in range(2):
            center, scale = _moments(rows.loc[indices, RAW_FEATURES[position]].to_numpy(dtype=float),
                rows.loc[indices, norm[position]].to_numpy(dtype=float), score_dtypes[raw[position]], score_dtypes[norm[position]])
            rows.loc[indices, list(STATE_FEATURES[2*position:2*position+2])] = center, scale
    available = rows[RAW_STATUS].eq('AVAILABLE') & np.isfinite(rows.loc[:, SCALE_FEATURES].to_numpy(dtype=float)).all(axis=1)
    rows[STATUS] = np.where(available, 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')
    rows[CLOCK] = rows[KEY[0]]  # Query cutoff, not vintage capture evidence.
    return rows.drop(columns=norm)


def parent_scale_state_fit_identity_v1(recipe, models, support):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_fit_identity_v1
    if (recipe.get('parent_scale_state_features') != list(SCALE_FEATURES)
            or recipe.get('matched_information_features') != list(RAW_FEATURES)
            or recipe.get('d_features') != list(D_FEATURES)
            or set(models) != {'matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'}
            or any(body.get('features') != (15 if name.startswith('matched') else 19) for name, body in models.items())):
        raise ValueError('parent scale state actual 15/19 recipe/heads differ')
    return information_fit_identity_v1(recipe, models, support, model_id='M21')


def _positive_scales(features):
    for name in STATE_FEATURES[1::2]:
        values = np.asarray(features[name], dtype=float)
        if (np.isfinite(values) & (values <= 0)).any():
            raise ValueError('parent scale state finite scale must be positive')


def parent_scale_state_nodes_v1(*, fitted, rows, arm):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_nodes_v1
    if arm not in ('matched', 'candidate') or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('parent scale state query arm/index/budget differs')
    if parent_scale_state_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent scale state fitted identity changed')
    query = rows.copy()
    for name in (*D_FEATURES, *SCALE_FEATURES, 'actual_gap_bps'):
        query[name] = query[name].map(_number).astype(float)
    _positive_scales(query)
    if STATUS in query:
        if not query[STATUS].isin(('AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT')).all():
            raise ValueError('parent scale state query status differs')
        query.loc[query[STATUS].ne('AVAILABLE'), list(SCALE_FEATURES)] = np.nan
    if query.empty:
        return pd.DataFrame(columns=['status', 'expected_net_bps', 'downside_q90_bps'], index=query.index)
    return information_nodes_v1(fitted=fitted, rows=query, arm=arm, model_id='M21',
        information_features=SCALE_FEATURES, matched_information_features=RAW_FEATURES)


def train_parent_scale_state_v1(*, rows, configuration, before_fit):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import train_information_price_v1
    _positive_scales(rows)
    return train_information_price_v1(rows=rows, configuration=configuration, before_fit=before_fit,
        model_id='M21', information_features=SCALE_FEATURES, status_column=STATUS,
        matched_information_features=RAW_FEATURES)


def parent_scale_state_price_set_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import information_price_set_v1
    if parent_scale_state_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('parent scale state fitted identity changed')
    features = {name: _number(value) for name, value in d_features.items()}
    _positive_scales(features)
    return information_price_set_v1(fitted=fitted, d_features=features, arm=arm,
        reference_cny=reference_cny, legal_low_cny=legal_low_cny, legal_high_cny=legal_high_cny,
        tick_cny=tick_cny, model_id='M21', information_features=SCALE_FEATURES, matched_information_features=RAW_FEATURES)
