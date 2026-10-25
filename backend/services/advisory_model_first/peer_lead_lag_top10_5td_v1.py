"""One fixed conditional peer lag representation; independent research outputs."""
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first import peer_residual_top10_5td_v1 as shared
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES

base, evaluation, sha = shared.base, shared.evaluation, shared.sha
KEY, GROUP = shared.KEY, shared.GROUP
validate_population, group_center, panel = shared.validate_population, shared.group_center, shared.panel
COMPONENTS = ('lag_own_return1_bps', 'lag_peer_return1_bps', 'lag_alpha60_bps',
    'lag_own_phi60', 'lag_peer_gamma60', 'lag_sigma60_bps', 'lag_own_response_bps')
NEW = 'lag_peer_response_bps'
MATCHED, FEATURES = (*INPUT_FEATURES, *COMPONENTS), (*INPUT_FEATURES, *COMPONENTS, NEW)
CONTRACT = dict(schema='peer_lead_lag_top10_5td_v1', hypothesis='CONDITIONAL_PEER_LEAD_LAG_TOP10_5TD_1',
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY',
    matched=MATCHED, candidate=FEATURES, lag_sessions=1, history_pairs=60, signal_exclusion_sessions=5,
    minimum_peer_count=40, minimum_history_pairs=48, minimum_relative_determinant=1e-10,
    original_topk=50, top_k=10, ridge=dict(alpha=100., solver='lsqr', tol=1e-6, max_iter=2000, fit_intercept=False),
    start=base.CONTRACT['start'], train_end=base.CONTRACT['train_end'], outer_start=base.CONTRACT['outer_start'],
    end=base.CONTRACT['end'], sealed_start=base.CONTRACT['sealed_start'], folds=base.FOLDS,
    max_fits=8, planned_candidates=1, prior_fits=201, prior_index=1, max_rows=52400,
    max_rss_bytes=2*1024**3, max_seconds=1800, max_output_bytes=200*1024**2,
    valuation_policy_sha256=base.VALUATION_POLICY_SHA256,
    evaluation_contract=base.CONTRACT['worth_confirming'], bootstrap=base.CONTRACT['bootstrap'],
    inner_rule='NATIVE_POSITIVE_TWO_FOLDS_COVERAGE80_INTERVENTION20D_20PCT_TWO_FOLDS_RISK100_UNKNOWN2PCT')


def lag_features(prices, factors, *, day, calendar):
    """Project D first. Sixty lag pairs end at D-5; no future or filled returns."""
    day, calendar = pd.Timestamp(day), pd.DatetimeIndex(calendar)
    if calendar.has_duplicates or not calendar.is_monotonic_increasing or day not in calendar:
        raise ValueError('invalid original calendar')
    expected = calendar[calendar <= day][-67:]
    p, f = prices.loc[:day].tail(67), factors.loc[:day].tail(67)
    if (not p.index.equals(expected) or not f.index.equals(expected) or not p.columns.equals(f.columns)
            or len(p.columns) != 50 or p.columns.has_duplicates):
        raise ValueError('lag population or calendar mismatch')
    out = pd.DataFrame(np.nan, index=p.columns, columns=[*COMPONENTS, NEW])
    out['lag_reason'], out['lag_history_pairs'], out['lag_latest_peers'] = 'INSUFFICIENT_HISTORY', 0, 0
    out['lag_parameter_estimated'] = False
    if len(p) != 67:
        return out
    a, f = p.to_numpy(float), f.to_numpy(float)
    if any(np.isinf(v).any() or (v[np.isfinite(v)] <= 0).any() for v in (a, f)):
        raise ValueError('invalid known lag price/factor')
    known = np.isfinite(a) & np.isfinite(f)
    observed = known[1:] & known[:-1]
    r = np.where(observed, np.diff(np.log(a), axis=0)*10000, np.nan)
    count = observed.sum(axis=1)[:, None]-observed
    # Zero is only an observed-sum intermediate, never an imputed individual return.
    sums = np.nansum(r, axis=1)[:, None]-np.where(observed, r, 0.)
    peer = np.divide(sums, count, out=np.full_like(sums, np.nan), where=count >= CONTRACT['minimum_peer_count'])
    y, u, v = r[1:61], r[:60], peer[:60]
    joint = np.isfinite(y) & np.isfinite(u) & np.isfinite(v)
    n = joint.sum(axis=0)
    denominator = np.maximum(n, 1)
    means = [np.where(joint, value, 0.).sum(axis=0)/denominator for value in (y, u, v)]
    cy, cu, cv = [np.where(joint, value-mean, 0.) for value, mean in zip((y, u, v), means, strict=True)]
    uu, vv, uv = [(x*z).sum(axis=0)/denominator for x, z in ((cu, cu), (cv, cv), (cu, cv))]
    yu, yv = [(cy*x).sum(axis=0)/denominator for x in (cu, cv)]
    det, scale = uu*vv-uv*uv, uu*vv
    history = n >= CONTRACT['minimum_history_pairs']
    estimable = history & (uu > 0) & (vv > 0) & (det > CONTRACT['minimum_relative_determinant']*scale)
    phi = np.divide(yu*vv-yv*uv, det, out=np.full(50, np.nan), where=estimable)
    gamma = np.divide(yv*uu-yu*uv, det, out=np.full(50, np.nan), where=estimable)
    alpha = means[0]-phi*means[1]-gamma*means[2]
    error = np.where(joint, cy-phi*cu-gamma*cv, 0.)
    sigma = np.sqrt((error*error).sum(axis=0)/denominator)
    latest = np.isfinite(r[-1]) & np.isfinite(peer[-1])
    values = np.column_stack((r[-1], peer[-1], alpha, phi, gamma, sigma,
        alpha+phi*r[-1], gamma*peer[-1]))
    if not np.isfinite(values[estimable & latest]).all():
        raise ValueError('nonfinite conditional lag arithmetic')
    out.loc[:, [*COMPONENTS, NEW]] = values
    out.loc[~estimable, [*COMPONENTS, NEW]] = np.nan
    reason = np.full(50, 'INSUFFICIENT_HISTORY', dtype=object)
    reason[history] = 'DEGENERATE_CALIBRATION'
    reason[estimable] = 'UNKNOWN_SIGNAL_ENDPOINT_OR_PEERS'
    reason[estimable & latest] = 'AVAILABLE'
    out['lag_reason'], out['lag_history_pairs'], out['lag_latest_peers'] = reason, n, count[-1]
    out['lag_parameter_estimated'] = estimable
    return out


def attach(inputs, prices, factors, calendar, progress=lambda event: None):
    validate_population(inputs)
    records = []
    for number, ((package, day), group) in enumerate(inputs.groupby(GROUP, sort=True), 1):
        symbols = group.instrument.tolist()
        features = lag_features(prices.loc[:, symbols], factors.loc[:, symbols], day=day, calendar=calendar)
        features = features.rename_axis('instrument').reset_index()
        records.append(group[KEY].merge(features, on='instrument', validate='one_to_one', sort=False))
        if number % 100 == 0:
            progress(dict(phase='LAG_CONTEXT', package_days=number))
    fields = [*COMPONENTS, NEW, 'lag_reason', 'lag_history_pairs', 'lag_latest_peers', 'lag_parameter_estimated']
    frame = inputs.merge(pd.concat(records, ignore_index=True)[[*KEY, *fields]], on=KEY, how='left', validate='one_to_one', sort=False)
    for name in (*IDENTITY, 'parent_score', 'source_id'):
        if not inputs[name].reset_index(drop=True).equals(frame[name].reset_index(drop=True)):
            raise ValueError('original identity, score or order changed')
    numeric(frame)
    return frame


def numeric(frame):
    x = frame.loc[:, FEATURES].apply(pd.to_numeric, errors='raise').to_numpy(float)
    if np.isinf(x).any():
        raise ValueError('infinite known feature')
    return x


def support(x):
    return np.isfinite(x[:, len(INPUT_FEATURES):]).all(axis=1) & np.isfinite(x[:, :len(INPUT_FEATURES)]).any(axis=1)


def training(rows, cutoff):
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
    report, daily = evaluation.summarize(cohorts)
    groups = cohorts.groupby('decision_date', sort=True)
    interventions = groups.matched_intervention_count.sum().gt(0) & groups.mature.all()
    report.update(matched=report.pop('full_model'), versus_matched=report.pop('versus_full_model'),
        matched_intervention_days=int(interventions.sum()),
        matched_intervention_fraction=float(interventions.sum()/max(1, report['mature_decision_days'])))
    daily = daily.rename(columns={'candidate_full_model':'matched', 'versus_full_model':'versus_matched'})
    daily['matched_intervention'] = interventions.to_numpy()
    if 'fold' in cohorts:
        means = daily.groupby('fold').versus_matched.mean()
        report['mechanism_by_fold'] = {str(int(f)):float(v) if pd.notna(v) else None for f, v in means.items()}
        tail, mean = report['tail_delta_bps'], report['statistics']['mean_bps']
        risk_failed = (tail is not None and tail < -100) or report['unknown_excess'] > .02
        go = bool(report['proceed_outer'] and not risk_failed and tail is not None)
        state = 'INNER_NOT_SUPPORTED' if risk_failed or (mean is not None and mean <= 0) else 'INNER_INCONCLUSIVE'
        report.update(proceed_outer=go, classification='INNER_SUPPORTED' if go else state)
    elif report['classification'] == 'WORTH_INDEPENDENT_CONFIRMATION':
        report['classification'] = 'WORTH_NEW_CONFIRMATION_DESIGN'
    report['mechanism_not_a_second_product_gate'] = True
    return report, daily
