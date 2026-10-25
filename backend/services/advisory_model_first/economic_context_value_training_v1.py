"""Matched additive mean/quantile heads with one causal classification block."""
from dataclasses import dataclass
import warnings

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_context_value_contracts_v1 import ARMS, KNOTS
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorEstimateV1, ValueAnchorGapSupportV1
from backend.services.advisory_model_first.economic_value_anchor_training_v1 import assemble_value_anchor_rows_v1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def assemble_context_value_rows_v1(*, inputs, labels, observations, context, configuration):
    base = assemble_value_anchor_rows_v1(inputs=inputs, labels=labels, observations=observations, configuration=configuration)
    fields = [*KEY, 'classification_l2_code', 'classification_known_from']
    classification = _frame(context.loc[:, fields], KEY, set(fields)).copy()
    known = classification.classification_l2_code.notna()
    clock = classification.classification_known_from.map(lambda v: pd.NaT if pd.isna(v) else _day(v))
    if (known & (clock.isna() | clock.gt(classification[KEY[0]]))).any():
        raise ValueError('context classification sees after D or lacks knowledge time')
    if not classification.loc[known, 'classification_l2_code'].map(lambda v: isinstance(v, str) and bool(v.strip())).all():
        raise ValueError('context known classification requires an explicit code')
    rows = base.merge(classification, on=KEY, how='outer', validate='one_to_one', indicator=True)
    if not rows._merge.eq('both').all():
        raise ValueError('context supervision must preserve exact frozen keys')
    return rows.drop(columns='_merge')


def context_support_v1(rows, *, train_end):
    domain = rows.loc[rows.split.eq('train') & rows.values_available & rows.classification_l2_code.notna()
        & rows[KEY[1]].le(pd.Timestamp(train_end)) & np.isfinite(rows.actual_gap_bps)].copy()
    if domain.empty or (domain.actual_gap_bps <= -10000).any():
        raise ValueError('context has no valid unlabelled train observations')
    low, high = domain.actual_gap_bps.quantile([.025, .975], interpolation='linear')
    categories = {code for code, group in domain.groupby('classification_l2_code')
        if len(group) >= 30 and group[KEY[0]].nunique() >= 5}
    domain['bucket'] = np.floor(domain.actual_gap_bps/100).astype(int)
    support = {}
    for (code, _), group in domain.groupby(['classification_l2_code', 'bucket'], sort=True):
        left, right = max(float(group.actual_gap_bps.min()), low), min(float(group.actual_gap_bps.max()), high)
        if code in categories and len(group) >= 30 and group[KEY[0]].nunique() >= 5 and left <= right:
            support.setdefault(code, []).append((float(left), float(right)))
    if not support:
        raise ValueError('context lacks common category/price support; no fit')
    return {code: ValueAnchorGapSupportV1(tuple(intervals)) for code, intervals in support.items()}


def _supported(rows, support):
    return pd.Series([code in support and np.isfinite(gap) and support[code].contains(float(gap))
        for code, gap in zip(rows.classification_l2_code, rows.actual_gap_bps, strict=True)], index=rows.index)


def context_basis_v1(rows, recipe, *, arm):
    if arm not in ARMS or recipe['d_features'] != list(D_FEATURES) or recipe['knots'] != list(KNOTS):
        raise ValueError('context basis family/recipe differs')
    raw = rows.loc[:, [*D_FEATURES, 'actual_gap_bps']]
    if raw.map(lambda v: isinstance(v, (bool, np.bool_))).any().any():
        raise ValueError('context boolean is not a numerical feature')
    values = raw.to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError('context basis requires complete finite features/price')
    categories = rows.classification_l2_code
    if not categories.isin(recipe['categories']).all():
        raise ValueError('context basis cannot fill unknown/unseen categories')
    matrix = {name: (values[:, index]-recipe['means'][index])/recipe['scales'][index]
        for index, name in enumerate(D_FEATURES)}
    gap = values[:, -1]/100.
    matrix['gap'] = gap
    for index, knot in enumerate(KNOTS):
        matrix[f'gap_hinge_{index}'] = np.maximum(gap-knot, 0.)
    if arm == 'candidate':
        for code in recipe['categories']:
            block = categories.eq(code).to_numpy(dtype=float)*.5
            matrix['category_'+code], matrix['category_gap_'+code] = block, block*gap
    result = pd.DataFrame(matrix, index=rows.index)
    if not np.isfinite(result.to_numpy()).all():
        raise ValueError('context basis overflowed')
    return result


@dataclass(frozen=True)
class ContextValueFitV1:
    recipe: dict
    models: dict
    support: dict
    diagnostics: dict
    model_sha256: str


def context_fit_identity_v1(recipe, models, support):
    return sha({'recipe': recipe, 'models': models,
        'support': {code: list(value.intervals_bps) for code, value in support.items()}})


def predict_context_value_v1(*, fitted, rows, arm):
    if context_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('context fitted identity changed')
    matrix = context_basis_v1(rows, fitted.recipe, arm=arm)
    model = fitted.models[arm]
    if model['feature_names'] != list(matrix.columns):
        raise ValueError('context model dimensions/order differ')
    estimates = [matrix.to_numpy()@np.asarray(model[name]['coef'], dtype=float)+float(model[name]['intercept'])
        for name in ('mean', 'path')]
    return [ValueAnchorEstimateV1(float(m), float(q)) for m, q in zip(*estimates, strict=True)]


def train_context_value_v1(*, rows, configuration, before_fit):
    import scipy
    import sklearn
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import QuantileRegressor, Ridge
    from threadpoolctl import threadpool_limits
    if (sklearn.__version__, scipy.__version__) != ('1.8.0', '1.16.3'):
        raise ValueError('context fit environment differs; no implicit install')
    support = context_support_v1(rows, train_end=configuration.train_end)
    common = rows.training_eligible & rows.values_available & _supported(rows, support)
    train = rows.loc[rows.split.eq('train') & common]
    validation = rows.loc[rows.split.eq('validation') & common]
    if len(train) < 30 or train[KEY[0]].nunique() < 5 or validation.empty:
        raise ValueError('context lacks common mature train/validation support')
    values = train.loc[:, D_FEATURES].to_numpy(dtype=float)
    scales = values.std(axis=0)
    recipe = {'d_features': list(D_FEATURES), 'means': values.mean(axis=0).tolist(),
        'scales': np.where(scales == 0, 1., scales).tolist(), 'categories': sorted(support), 'knots': list(KNOTS)}
    models, diagnostics = {}, {}
    for arm in ARMS:
        matrix = context_basis_v1(train, recipe, arm=arm)
        mean = Ridge(alpha=10, solver='svd', fit_intercept=True)
        path = QuantileRegressor(quantile=.1, alpha=.0001, solver='highs', fit_intercept=True, solver_options={'time_limit':300})
        for name, estimator, target in (('mean', mean, train.gross_value_ratio), ('path', path, train.path_min_value_ratio)):
            before_fit(arm, name)
            with warnings.catch_warnings():
                warnings.simplefilter('error', ConvergenceWarning)
                with threadpool_limits(limits=2):
                    estimator.fit(matrix, target)
        models[arm] = {'feature_names': list(matrix.columns),
            **{name: {'coef': estimator.coef_.tolist(), 'intercept': float(estimator.intercept_)} for name, estimator in (('mean', mean), ('path', path))}}
    fit = ContextValueFitV1(recipe, models, support, {}, context_fit_identity_v1(recipe, models, support))
    for arm in ARMS:
        predictions = predict_context_value_v1(fitted=fit, rows=validation, arm=arm)
        mean, lower = np.asarray([(item.mean_gross_value_ratio, item.path_min_ratio_q10) for item in predictions]).T
        error = validation.path_min_value_ratio.to_numpy()-lower
        diagnostics[arm] = {'mean_squared_error': float(np.mean((validation.gross_value_ratio.to_numpy()-mean)**2)),
            'path_pinball_loss': float(np.mean(np.maximum(.1*error, -.9*error))),
            'path_lower_coverage': float(np.mean(validation.path_min_value_ratio.to_numpy() < lower))}
    receipt = train.loc[:, [*KEY, 'label_information_end']].copy()
    for key in (*KEY[:2], 'label_information_end'):
        receipt[key] = receipt[key].map(lambda v: None if pd.isna(v) else pd.Timestamp(v).isoformat())
    diagnostics.update({'train_rows':len(train), 'validation_rows':len(validation), 'candidate_rows':len(rows),
        'common_supervision_sha256':sha(receipt.to_dict('records')), 'fitted_head_count':4,
        'candidate_count':1, 'configuration_count':2, 'test_used_for_training_or_calibration':False,
        'decision_use':'NAVIGATION_ONLY', 'deployable':False})
    return ContextValueFitV1(recipe, models, support, diagnostics, fit.model_sha256)
