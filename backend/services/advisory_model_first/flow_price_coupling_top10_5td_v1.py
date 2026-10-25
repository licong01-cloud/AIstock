"""One fixed five-day flow/price covariance with matched marginal controls."""
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first import peer_residual_top10_5td_v1 as shared
from backend.services.advisory_model_first.peer_lead_lag_top10_5td_v1 import summarize as native_summary
from backend.services.advisory_model_first.flow_price_coupling_top10_5td_inputs_v1 import CONTROL, NEW
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES

base, evaluation, sha = shared.base, shared.evaluation, shared.sha
KEY, GROUP = shared.KEY, shared.GROUP
validate_population, group_center, panel = shared.validate_population, shared.group_center, shared.panel
MATCHED, FEATURES = (*INPUT_FEATURES, *CONTROL), (*INPUT_FEATURES, *CONTROL, NEW)
CONTRACT = dict(schema='flow_price_coupling_top10_5td_v1', hypothesis='FLOW_PRICE_COUPLING_5D_TOP10_5TD_1',
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY',
    matched=MATCHED, candidate=FEATURES, history_sessions=5, original_topk=50, top_k=10,
    ridge=dict(alpha=100., solver='lsqr', tol=1e-6, max_iter=2000, fit_intercept=False),
    start='2024-07-04', train_end='2025-09-30', end='2025-09-30', folds=base.FOLDS,
    max_fits=6, planned_candidates=1, prior_fits=207, prior_index=1, max_rows=30500,
    max_rss_bytes=2*1024**3, max_seconds=1800, max_output_bytes=200*1024**2,
    max_source_numeric_cells=20_000_000, final_allowed=False, outer_allowed=False, sealed_allowed=False,
    valuation_policy_sha256=base.VALUATION_POLICY_SHA256,
    evaluation_contract=base.CONTRACT['worth_confirming'], bootstrap=base.CONTRACT['bootstrap'],
    inner_rule='NATIVE_POSITIVE_TWO_FOLDS_COVERAGE80_INTERVENTION20D_20PCT_TWO_FOLDS_RISK100_UNKNOWN2PCT')


def numeric(frame):
    x = frame.loc[:, FEATURES].apply(pd.to_numeric, errors='raise').to_numpy(float)
    if np.isinf(x).any():
        raise ValueError('infinite known feature')
    return x


def support(x):
    return np.isfinite(x[:, len(INPUT_FEATURES):]).all(axis=1) & np.isfinite(x[:, :len(INPUT_FEATURES)]).any(axis=1)


def training(rows, cutoff):
    if pd.Timestamp(cutoff) > pd.Timestamp(CONTRACT['end']):
        raise ValueError('training cutoff outside inner window')
    part = rows.loc[rows[base.KEY[0]].between(pd.Timestamp(CONTRACT['start']), pd.Timestamp(cutoff))].copy()
    part = part.loc[part.label_information_end.le(pd.Timestamp(cutoff))].reset_index(drop=True)
    base._validate_finance(part)
    cash = part.valuation_status.eq('CASH_ENTRY_NOT_EXECUTABLE')
    if not part.loc[cash, 'hypothetical_liquidation_net_bps'].eq(0.).all() or part.duplicated(KEY).any():
        raise ValueError('cash label or unique training identity differs')
    if part.groupby(list(base.KEY))['hypothetical_liquidation_net_bps'].nunique(dropna=False).gt(1).any():
        raise ValueError('same stock/day has contradictory labels across packages')
    part = part.loc[part.valuation_status.isin(['AVAILABLE', 'CASH_ENTRY_NOT_EXECUTABLE']) & support(numeric(part))].reset_index(drop=True)
    if len(part) < 2 or part[base.KEY[0]].nunique() < 2:
        raise ValueError('insufficient mature common training support')
    return part


def fit(rows, cutoff, arm, *, before_fit=lambda: None):
    if arm not in ('matched', 'candidate'):
        raise ValueError('unregistered arm')
    part = training(rows, cutoff)
    fields = list(MATCHED if arm == 'matched' else FEATURES)
    raw = part.loc[:, fields].to_numpy(float)
    active = np.isfinite(raw).any(axis=0)
    inactive = [name for name, on in zip(fields, active, strict=True) if not on]
    fields, raw = [name for name, on in zip(fields, active, strict=True) if on], raw[:, active]
    q = np.nanquantile(raw, [.25, .5, .75], axis=0)
    median, scale = q[1], np.where(q[2] > q[0], q[2]-q[0], 1.)
    x = np.column_stack(((np.where(np.isnan(raw), median, raw)-median)/scale, np.isnan(raw).astype(float)))
    x = group_center(x, part)
    y = group_center(part.hypothetical_liquidation_net_bps.to_numpy(float)[:, None], part).ravel()
    group = part.groupby(GROUP, sort=False)
    weights = 1/(group.package_id.transform('size')*part.groupby(base.KEY[0]).package_id.transform('nunique'))
    weights = weights/weights.mean()
    before_fit()
    fitted = Ridge(**CONTRACT['ridge']).fit(x, y, sample_weight=weights)
    if not np.isfinite(fitted.coef_).all() or np.max(np.atleast_1d(fitted.n_iter_)) >= CONTRACT['ridge']['max_iter']:
        raise ValueError('Ridge finite/convergence contract failed')
    if not np.allclose(fitted.predict(x), x@np.asarray(fitted.coef_.tolist()), rtol=1e-12, atol=1e-9):
        raise ValueError('JSON model prediction parity failed')
    return dict(contract_sha256=sha(CONTRACT), arm=arm, fields=fields, median=median.tolist(), scale=scale.tolist(),
        coef=fitted.coef_.tolist(), iterations=np.atleast_1d(fitted.n_iter_).tolist(), cutoff=str(cutoff),
        inactive_fields=inactive, train_rows=len(part), train_days=int(part[base.KEY[0]].nunique()),
        training_keys_sha256=sha([[str(v) for v in row] for row in part[KEY].itertuples(index=False, name=None)]))


def predict(inputs, model):
    validate_population(inputs)
    if model['contract_sha256'] != sha(CONTRACT) or model['arm'] not in ('matched', 'candidate'):
        raise ValueError('model identity differs')
    allowed = MATCHED if model['arm'] == 'matched' else FEATURES
    fields = model['fields']
    if not fields or len(set(fields)) != len(fields) or not set(fields).issubset(allowed):
        raise ValueError('model fields differ')
    eligible = support(numeric(inputs))
    result = inputs.loc[:, [*IDENTITY, 'parent_score', 'source_id']].copy().reset_index(drop=True)
    result['predicted_relative_net_bps'], result['no_rank_advice'] = np.nan, ~eligible
    result['advisory_rank'] = result.selection_effective_rank.copy()
    selected = inputs.loc[eligible].copy()
    raw = selected.loc[:, fields].to_numpy(float)
    median, scale, coef = (np.asarray(model[name], float) for name in ('median', 'scale', 'coef'))
    if (median.shape != (len(fields),) or scale.shape != median.shape or coef.shape != (2*len(fields),)
            or not np.isfinite(np.r_[median, scale, coef]).all() or (scale <= 0).any()):
        raise ValueError('model numeric encoding differs')
    x = np.column_stack(((np.where(np.isnan(raw), median, raw)-median)/scale, np.isnan(raw).astype(float)))
    values = group_center(x, selected)@coef if len(selected) else np.array([])
    if not np.isfinite(values).all():
        raise ValueError('nonfinite prediction')
    result.loc[eligible, 'predicted_relative_net_bps'] = values
    for _, group in result.groupby(GROUP, sort=False):
        active = group.loc[~group.no_rank_advice]
        order = active.sort_values(['predicted_relative_net_bps', 'selection_effective_rank'], ascending=[False, True], kind='stable').index
        result.loc[order, 'advisory_rank'] = sorted(active.selection_effective_rank)
    result['selected'] = result.advisory_rank.le(CONTRACT['top_k'])
    evaluation.same_population(inputs, result)
    return result


def summarize(cohorts):
    if 'fold' not in cohorts or set(cohorts.fold) != {1, 2, 3}:
        raise ValueError('all three inner folds are required')
    report, daily = native_summary(cohorts)
    report['supports_future_confirmation_design'] = report['classification'] == 'INNER_SUPPORTED'
    report['proceed_outer'] = False
    return report, daily
