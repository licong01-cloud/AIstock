"""Three fixed price-conditional estimators with non-executing JSON models."""
from dataclasses import dataclass
import warnings

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import ARMS, GBDT, KNOTS, MODEL_IDS
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, ValueAnchorEstimateV1
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import build_value_anchor_gap_support_v1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def query_matrix_v2(rows):
    raw = rows.loc[:, [*D_FEATURES, 'actual_gap_bps']]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any().any():
        raise ValueError('campaign numerical inputs cannot be booleans')
    values = raw.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values[:, -1] <= -10000).any():
        raise ValueError('campaign query needs finite positive-price coordinates')
    values[:, -1] /= 100
    return values


def _scaled(values, recipe):
    means, scales = np.asarray(recipe['means']), np.asarray(recipe['scales'])
    if means.shape != (13,) or scales.shape != (13,) or not np.isfinite([means, scales]).all() or (scales <= 0).any():
        raise ValueError('campaign scaler identity/dimensions differ')
    return (values-means)/scales


def _additive(values, recipe):
    gap = values[:, -1:]
    return np.column_stack((_scaled(values, recipe)[:, :12], gap,
        np.maximum(0., gap-np.asarray(KNOTS)[None, :])))


def _linear(model):
    return dict(kind='linear', coef=np.asarray(model.coef_).tolist(), intercept=float(model.intercept_))


def export_gbdt_v2(model):
    trees = []
    for estimator in model.estimators_.ravel():
        tree = estimator.tree_
        trees.append(dict(left=tree.children_left.tolist(), right=tree.children_right.tolist(),
            feature=tree.feature.tolist(), threshold=tree.threshold.tolist(), value=tree.value[:, 0, 0].tolist()))
    return dict(kind='gbdt', features=int(model.n_features_in_),
        initial=float(model.init_.constant_.ravel()[0]), learning_rate=float(model.learning_rate), trees=trees)


def predict_json_v2(model, values):
    values = np.asarray(values, dtype=float)
    if values.ndim != 2 or not np.isfinite(values).all():
        raise ValueError('JSON inference needs finite matrix')
    kind = model['kind']
    if kind in ('linear', 'gamma', 'logistic'):
        coef = np.asarray(model['coef'], dtype=float)
        intercept = np.asarray(model['intercept'], dtype=float)
        if not np.isfinite(coef).all() or not np.isfinite(intercept).all():
            raise ValueError('JSON coefficients must be finite')
        if kind == 'logistic':
            classes = np.asarray(model['classes'])
            if (coef.ndim != 2 or coef.shape[1] != values.shape[1] or intercept.shape != (coef.shape[0],)
                    or len(classes) not in (2, 3) or len(set(classes)) != len(classes)
                    or not set(classes).issubset({-1, 0, 1})
                    or coef.shape[0] != (1 if len(classes) == 2 else 3)):
                raise ValueError('JSON probability dimensions/classes differ')
            from scipy.special import expit
            score = values@coef.T+intercept
            if len(classes) == 2:
                probability = expit(score[:, 0])
                probabilities = np.column_stack((1-probability, probability))
            else:
                score -= score.max(axis=1, keepdims=True)
                probabilities = np.exp(score)
                probabilities /= probabilities.sum(axis=1, keepdims=True)
            result = np.zeros((len(values), 3))
            for column, label in enumerate(classes):
                result[:, int(label)+1] = probabilities[:, column]
        else:
            if coef.shape != (values.shape[1],) or intercept.ndim != 0:
                raise ValueError('JSON regression dimensions differ')
            result = values@coef+intercept
            if kind == 'gamma':
                with np.errstate(over='raise', invalid='raise'):
                    result = np.exp(result)
                if (result <= 0).any():
                    raise ValueError('Gamma amplitude underflowed; no default/clipping')
    elif kind == 'gbdt':
        if model['features'] != values.shape[1] or not model['trees'] or len(model['trees']) > 200:
            raise ValueError('JSON tree feature/count contract differs')
        result = np.full(len(values), float(model['initial']))
        tree_values = values.astype(np.float32)
        if not np.isfinite(tree_values).all() or not np.isfinite(model['learning_rate']) or model['learning_rate'] <= 0:
            raise ValueError('JSON tree coordinate/rate overflow')
        for tree in model['trees']:
            left, right, features = (np.asarray(tree[name]) for name in ('left', 'right', 'feature'))
            thresholds, leaves = (np.asarray(tree[name], dtype=float) for name in ('threshold', 'value'))
            size = len(left)
            if (not size or size > 15 or any(item.shape != (size,) for item in (right, features, thresholds, leaves))
                    or any(item.dtype.kind not in 'iu' for item in (left, right, features))
                    or not np.isfinite([thresholds, leaves]).all()):
                raise ValueError('JSON tree arrays malformed')
            visited, stack = set(), [0]
            while stack:
                node = stack.pop()
                if node < 0 or node >= size or node in visited:
                    raise ValueError('JSON tree graph is cyclic/invalid')
                visited.add(node)
                if left[node] == -1 and right[node] == -1:
                    continue
                if left[node] < 0 or right[node] < 0 or not 0 <= features[node] < values.shape[1]:
                    raise ValueError('JSON split contract differs')
                stack.extend((int(left[node]), int(right[node])))
            if len(visited) != size:
                raise ValueError('JSON tree contains unreachable nodes')
            nodes = np.zeros(len(values), dtype=int)
            for _ in range(size):
                active = np.flatnonzero(left[nodes] != -1)
                if not len(active):
                    break
                current = nodes[active]
                nodes[active] = np.where(tree_values[active, features[current]] <= thresholds[current], left[current], right[current])
            result += float(model['learning_rate'])*leaves[nodes]
    else:
        raise ValueError('unknown non-executing model kind')
    if not np.isfinite(result).all():
        raise ValueError('JSON model produced nonfinite values')
    return result


@dataclass(frozen=True)
class PriceCampaignFitV2:
    model_id: str
    recipe: dict
    models: dict
    support: object
    diagnostics: dict
    model_sha256: str


def fit_identity_v2(model_id, recipe, models, support):
    return sha(dict(model_id=model_id, recipe=recipe, models=models, support=list(support.intervals_bps)))


def _local(values, fitted):
    index = fitted.models['local']
    train = np.asarray(index['values'], dtype=float)
    y, lower = (np.asarray(index[name], dtype=float) for name in ('y', 'lower'))
    days = np.asarray(index['days'])
    if (train.ndim != 2 or train.shape[1] != 13 or len(train) < 100 or len(train) > 7720
            or any(item.shape != (len(train),) for item in (y, lower, days))
            or not np.isfinite(train).all() or not np.isfinite([y, lower]).all() or (y <= 0).any() or (lower <= 0).any()):
        raise ValueError('local training index malformed')
    output = []
    for row in _scaled(values, fitted.recipe):
        distances = np.sqrt(((train-row)**2).sum(axis=1))
        # Stored rows are canonical-key sorted; stable sort makes distance ties deterministic.
        selected = np.argsort(distances, kind='stable')[:100]
        if distances[selected[-1]] > 4 or len(set(days[selected])) < 20:
            output.append(None)
        else:
            output.append(ValueAnchorEstimateV1(float(y[selected].mean()), float(np.quantile(lower[selected], .1, method='linear'))))
    return output


def predict_campaign_v2(*, fitted, rows, arm):
    if fitted.model_id not in MODEL_IDS or arm not in ARMS or fitted.recipe['features'] != list(D_FEATURES)+['gap_bps_div_100']:
        raise ValueError('campaign model/arm/feature order differs')
    if fit_identity_v2(fitted.model_id, fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('campaign fitted identity changed')
    values = query_matrix_v2(rows)
    if fitted.model_id == 'M4' and arm == 'candidate':
        return _local(values, fitted)
    prefix = arm
    if fitted.model_id == 'M2':
        matrix = _additive(values, fitted.recipe) if arm == 'matched' else values
        mean, lower = (predict_json_v2(fitted.models[prefix+'_'+head], matrix) for head in ('mean', 'path'))
    else:
        lower = predict_json_v2(fitted.models['shared_path' if fitted.model_id == 'M3' else prefix+'_path'], values)
        if fitted.model_id == 'M3':
            if arm == 'matched':
                net = predict_json_v2(fitted.models['matched_mean'], values)
            else:
                matrix = _scaled(values, fitted.recipe)
                probabilities = predict_json_v2(fitted.models['probabilities'], matrix)
                positive, negative = (predict_json_v2(fitted.models[name], matrix) for name in ('positive', 'negative'))
                net = probabilities[:, 2]*positive-probabilities[:, 0]*negative
            if (net <= -10000).any():
                raise ValueError('net prediction implies nonpositive gross value')
            mean = (1+net/10000)*(1+values[:, -1]/100)*(1+COST.buy_cost_bps/10000)/(1-COST.sell_cost_bps/10000)
        else:
            mean = predict_json_v2(fitted.models[prefix+'_mean'], values)
    return [ValueAnchorEstimateV1(float(a), float(b)) for a, b in zip(mean, lower, strict=True)]


def train_campaign_v2(*, rows, configuration, model_id, before_fit):
    import sklearn
    import scipy
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.linear_model import GammaRegressor, LogisticRegression, QuantileRegressor, Ridge
    if model_id not in MODEL_IDS or sklearn.__version__ != '1.8.0' or scipy.__version__ != '1.16.3':
        raise ValueError('campaign exact model runtime differs')
    domain = rows.loc[rows.split.eq('train') & rows.values_available].copy()
    domain.loc[domain[KEY[1]].gt(pd.Timestamp(configuration.train_end)), 'actual_gap_bps'] = np.nan
    support = build_value_anchor_gap_support_v1(domain)
    supervised = rows.split.eq('train') & rows.training_eligible & rows.actual_gap_bps.map(
        lambda gap: bool(pd.notna(gap) and support.contains(float(gap))))
    train = rows.loc[supervised].sort_values(KEY).copy()
    if (len(train) < 100 or train[KEY[0]].nunique() < 20 or not support.intervals_bps
            or pd.to_datetime(train.label_information_end).gt(pd.Timestamp(configuration.train_end)).any()
            or train[KEY[1]].gt(pd.Timestamp(configuration.train_end)).any()):
        raise ValueError('campaign lacks mature shared training/support')
    values = query_matrix_v2(train)
    means, scales = values.mean(axis=0), values.std(axis=0)
    scales[scales == 0] = 1.
    recipe = dict(features=list(D_FEATURES)+['gap_bps_div_100'], means=means.tolist(), scales=scales.tolist(),
        training_keys_sha256=sha([[str(value) for value in key] for key in train[KEY].itertuples(index=False, name=None)]))
    models, count = {}, 0

    def fit(name, estimator, matrix, target, *, kind=None):
        nonlocal count
        before_fit(name)
        count += 1
        with warnings.catch_warnings():
            warnings.simplefilter('error', ConvergenceWarning)
            estimator.fit(matrix, target)
        if isinstance(estimator, GradientBoostingRegressor):
            body = export_gbdt_v2(estimator)
        elif kind == 'logistic':
            body = dict(kind=kind, coef=estimator.coef_.tolist(), intercept=estimator.intercept_.tolist(), classes=estimator.classes_.tolist())
        else:
            body = _linear(estimator)
            if kind:
                body['kind'] = kind
        prediction = predict_json_v2(body, matrix)
        if kind == 'logistic':
            expected = np.zeros((len(matrix), 3))
            for column, label in enumerate(estimator.classes_):
                expected[:, int(label)+1] = estimator.predict_proba(matrix)[:, column]
        else:
            expected = estimator.predict(matrix)
        if not np.allclose(prediction, expected, rtol=1e-10, atol=1e-10):
            raise ValueError('public estimator/JSON prediction parity failed')
        models[name] = body

    def tree(name, target, *, quantile=False):
        estimator = GradientBoostingRegressor(**GBDT, loss='quantile' if quantile else 'squared_error', alpha=.1)
        fit(name, estimator, values, target)

    if model_id == 'M2':
        tree('candidate_mean', train.gross_value_ratio)
        tree('candidate_path', train.path_min_value_ratio, quantile=True)
        matrix = _additive(values, recipe)
        fit('matched_mean', Ridge(alpha=10., solver='svd'), matrix, train.gross_value_ratio)
        fit('matched_path', QuantileRegressor(quantile=.1, alpha=.0001, solver='highs', solver_options={'time_limit': 300}), matrix, train.path_min_value_ratio)
    elif model_id == 'M3':
        net = 10000*(train.gross_value_ratio.to_numpy()*(1-COST.sell_cost_bps/10000)/((1+values[:, -1]/100)*(1+COST.buy_cost_bps/10000))-1)
        positive, negative = net > 0, net < 0
        if any(mask.sum() < 30 or train.loc[mask, KEY[0]].nunique() < 5 for mask in (positive, negative)):
            raise ValueError('hurdle lacks positive/negative amplitude support; no fit')
        matrix = _scaled(values, recipe)
        fit('probabilities', LogisticRegression(C=1., solver='lbfgs', max_iter=500, l1_ratio=0), matrix, np.sign(net).astype(int), kind='logistic')
        for name, mask, target in (('positive', positive, net), ('negative', negative, -net)):
            fit(name, GammaRegressor(alpha=1., max_iter=500, tol=.0001), matrix[mask], target[mask], kind='gamma')
        tree('matched_mean', net)
        tree('shared_path', train.path_min_value_ratio, quantile=True)
    else:
        before_fit('local_index')  # Durable index accounting, not a physical estimator fit.
        models['local'] = dict(kind='local', values=_scaled(values, recipe).tolist(),
            y=train.gross_value_ratio.tolist(), lower=train.path_min_value_ratio.tolist(),
            days=train[KEY[0]].dt.strftime('%Y-%m-%d').tolist())
        tree('matched_mean', train.gross_value_ratio)
        tree('matched_path', train.path_min_value_ratio, quantile=True)
    diagnostics = dict(train_rows=len(train), train_days=train[KEY[0]].nunique(), candidate_rows=len(rows),
        fitted_head_count=count, index_build_count=int(model_id == 'M4'), candidate_count=1,
        test_used_for_training_or_calibration=False, decision_use='NAVIGATION_ONLY', deployable=False)
    fitted = PriceCampaignFitV2(model_id, recipe, models, support, diagnostics, fit_identity_v2(model_id, recipe, models, support))
    validation = rows.loc[rows.split.eq('validation') & rows.training_eligible & rows.actual_gap_bps.map(
        lambda gap: bool(pd.notna(gap) and support.contains(float(gap))))]
    diagnostics['validation_diagnostics_only'] = {}
    for arm in ARMS:
        estimates = predict_campaign_v2(fitted=fitted, rows=validation, arm=arm)
        available = [(index, value) for index, value in zip(validation.index, estimates, strict=True) if value is not None]
        report = dict(supported_rows=len(validation), estimable_rows=len(available), used_for_selection=False)
        if available:
            indices, predictions = zip(*available, strict=True)
            observed = validation.loc[list(indices)]
            means = np.array([value.mean_gross_value_ratio for value in predictions])
            lower = np.array([value.path_min_ratio_q10 for value in predictions])
            error = observed.path_min_value_ratio.to_numpy()-lower
            report.update(mean_squared_error=float(np.mean((observed.gross_value_ratio.to_numpy()-means)**2)),
                path_pinball_loss=float(np.mean(np.maximum(.1*error, -.9*error))),
                path_lower_coverage=float(np.mean(observed.path_min_value_ratio.to_numpy() < lower)))
        diagnostics['validation_diagnostics_only'][arm] = report
    return fitted
