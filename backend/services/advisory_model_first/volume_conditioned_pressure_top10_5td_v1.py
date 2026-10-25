"""One frozen volume-conditioned research hypothesis, not native selection policy."""
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first import multiperiod_top10_5td_study_v1 as base
from backend.services.advisory_model_first import native_anchored_top10_5td_v1 as evaluation
from backend.services.advisory_model_first import serial_price_pressure_top10_5td_inputs_v1 as old
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

KEY = ['package_id', *base.KEY]
GROUP = ['package_id', base.KEY[0]]
SHARED = ('joint_pair_correlation', 'joint_triple_coverage')
NEW = ('volume_conditioned_serial_delta', 'volume_conditioned_closing_pressure')
MATCHED = (*old.CANDIDATE, *SHARED)
FEATURES = (*MATCHED, *NEW)
CONTRACT = dict(schema='volume_conditioned_pressure_top10_5td_v1',
    hypothesis='VOLUME_CONDITIONED_SERIAL_PRESSURE_TOP10_5TD_1',
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY',
    matched=MATCHED, candidate=FEATURES, minimum_triple_coverage=.8, folds=base.FOLDS,
    ridge=dict(alpha=100., solver='lsqr', tol=1e-6, max_iter=2000, fit_intercept=False),
    start=base.CONTRACT['start'], train_end=base.CONTRACT['train_end'], outer_start=base.CONTRACT['outer_start'],
    end=base.CONTRACT['end'], sealed_start=base.CONTRACT['sealed_start'], top_k=10, original_topk=50,
    max_fits=8, planned_candidates=1, prior_fits=189, prior_index=1, max_rows=52400,
    max_rss_bytes=2*1024**3, max_seconds=1800, max_output_bytes=200*1024**2,
    evaluation_contract=base.CONTRACT['worth_confirming'], bootstrap=base.CONTRACT['bootstrap'],
    valuation_policy_sha256=base.VALUATION_POLICY_SHA256,
    inner_rule='BOTH_POSITIVE_TWO_POSITIVE_FOLDS_COVERAGE80_INTERVENTION20D_20PCT_TWO_FOLDS')


def conditional(slots, close, volume, *, day):
    """Volume of the FIRST return's endpoint weights both adjacent same-day returns."""
    old.serial_stat(slots, close, day=day)
    p, v = np.asarray(close, float), np.asarray(volume, float)
    if v.shape != p.shape or np.isinf(v).any() or (v[np.isfinite(v)] < 0).any():
        raise ValueError('invalid volume shape or known value')
    edge = np.diff(pd.DatetimeIndex(slots).asi8) == pd.Timedelta(minutes=1).value
    possible = edge[:-1] & edge[1:]
    known = np.isfinite(p)
    mask = possible & known[:-2] & known[1:-1] & known[2:] & np.isfinite(v[1:-1])
    coverage = float(mask.sum()/possible.sum()) if possible.any() else 0.
    result = dict(joint_pair_correlation=np.nan, joint_triple_coverage=coverage,
        volume_conditioned_serial_delta=np.nan, weighted_pair_correlation=np.nan,
        joint_pairs=int(mask.sum()), joint_possible=int(possible.sum()),
        conditional_reason='INSUFFICIENT_JOINT_TRIPLE_COVERAGE')
    if coverage < CONTRACT['minimum_triple_coverage'] or mask.sum() < 2:
        return result
    a = np.log(p[1:-1][mask])-np.log(p[:-2][mask])
    b = np.log(p[2:][mask])-np.log(p[1:-1][mask])
    w = v[1:-1][mask]

    def correlation(weights):
        x, y = a-np.dot(weights, a), b-np.dot(weights, b)
        denominator = np.sqrt(np.dot(weights, x*x)*np.dot(weights, y*y))
        if denominator == 0:
            return np.nan
        value = float(np.dot(weights, x*y)/denominator)
        if not np.isfinite(value) or abs(value) > 1+1e-12:
            raise ValueError('invalid weighted correlation arithmetic')
        return float(np.clip(value, -1., 1.))

    rho = correlation(np.full(len(a), 1./len(a)))
    result['joint_pair_correlation'] = rho
    if pd.isna(rho):
        result['conditional_reason'] = 'CONSTANT_RETURN_VARIANCE'
    elif not w.max() > 0:
        result['conditional_reason'] = 'ZERO_OBSERVED_VOLUME'
    else:
        w = w/w.max()
        weighted = correlation(w/w.sum())
        if pd.isna(weighted):
            result['conditional_reason'] = 'ZERO_WEIGHTED_RETURN_VARIANCE'
        else:
            result.update(weighted_pair_correlation=weighted, volume_conditioned_serial_delta=weighted-rho,
                conditional_reason='AVAILABLE')
    return result


def minute_features(slots, arrays, *, day):
    result = old.minute_features(slots, arrays, day=day)
    result.update(conditional(slots, arrays['close'], arrays['volume'], day=day))
    result[NEW[1]] = result[NEW[0]]*result['closing_30m_return_bps']/10000
    if pd.notna(result[NEW[0]]) and pd.isna(result[NEW[1]]):
        result['conditional_reason'] = 'UNKNOWN_CLOSING_RETURN_ENDPOINT'
    return result


def validate_population(frame):
    if (frame.duplicated(KEY).any() or frame.empty or len(frame) > CONTRACT['max_rows']
            or frame.loc[:, [*IDENTITY, 'source_id']].isna().any().any()):
        raise ValueError('duplicate, empty or excess original population')
    for _, group in frame.groupby(GROUP, sort=False):
        if (len(group) != CONTRACT['original_topk'] or not group.candidate_group_size.eq(len(group)).all()
                or sorted(group.selection_effective_rank) != list(range(1, len(group)+1))
                or group.instrument.duplicated().any() or group.target_trade_date.nunique() != 1
                or not group.target_trade_date.gt(group[base.KEY[0]]).all()
                or group.manifest_sha256.nunique() != 1 or group.run_id.nunique() != 1):
            raise ValueError('original Top50 identity/rank/clock contract differs')


def attach(inputs, minute):
    validate_population(inputs)
    keys = ['instrument', base.KEY[0]]
    if minute.duplicated(keys).any() or set(map(tuple, minute[keys].to_numpy())) != set(map(tuple, inputs[keys].to_numpy())):
        raise ValueError('minute population differs from original input')
    result = inputs.merge(minute, on=keys, how='left', validate='many_to_one')
    missing = []
    for name in old.DAILY:
        if name not in result:
            # The frozen predecessor explicitly omitted these source fields.
            result[name] = np.nan
            missing.append(name)
    score = pd.to_numeric(result.parent_score, errors='raise')
    if np.isinf(score).any():
        raise ValueError('infinite native score')
    grouped = score.groupby([result[n] for n in GROUP], sort=False)
    std = grouped.transform(lambda x: x.std(ddof=0))
    result['native_score_z'] = (score-grouped.transform('mean'))/std.replace(0, np.nan)
    result['native_rank_fraction'] = (result.selection_effective_rank-1)/19  # Original descriptor, not a new scaling search.
    for name in (*IDENTITY, 'parent_score', 'source_id'):
        if not result[name].reset_index(drop=True).equals(inputs[name].reset_index(drop=True)):
            raise ValueError('original identity, order, dtype or score changed')
    numeric(result)
    return result, missing


def numeric(frame):
    x = frame.loc[:, FEATURES].apply(pd.to_numeric, errors='raise').to_numpy(float)
    if np.isinf(x).any():
        raise ValueError('infinite known feature')
    return x


def support(x):
    return np.isfinite(x[:, -2:]).all(axis=1) & np.isfinite(x[:, :len(old.CANDIDATE)]).any(axis=1)


def group_center(x, frame):
    keys = pd.MultiIndex.from_frame(frame[GROUP]).factorize()[0]
    return x-pd.DataFrame(x).groupby(keys, sort=False).transform('mean').to_numpy()


def training(rows, cutoff):
    # Select D, then H, before decoding any feature/outcome values.
    part = rows.loc[rows[base.KEY[0]].between(pd.Timestamp(CONTRACT['start']), pd.Timestamp(cutoff))].copy()
    part = part.loc[part.label_information_end.le(pd.Timestamp(cutoff))].reset_index(drop=True)
    base._validate_finance(part)
    cash = part.valuation_status.eq('CASH_ENTRY_NOT_EXECUTABLE')
    if not part.loc[cash, 'hypothetical_liquidation_net_bps'].eq(0.).all():
        raise ValueError('legal cash label is not zero')
    if part.duplicated(KEY).any():
        raise ValueError('duplicate supervised identity')
    if part.groupby(list(base.KEY))['hypothetical_liquidation_net_bps'].nunique(dropna=False).gt(1).any():
        raise ValueError('same stock/day has contradictory labels across packages')
    eligible = part.valuation_status.isin(['AVAILABLE', 'CASH_ENTRY_NOT_EXECUTABLE']) & support(numeric(part))
    selected = part.loc[eligible].reset_index(drop=True)
    if selected[base.KEY[0]].nunique() < 2 or len(selected) < 2:
        raise ValueError('insufficient mature common training support')
    return selected


def fit(rows, cutoff, arm, *, before_fit=lambda: None):
    if arm not in ('matched', 'candidate'):
        raise ValueError('unregistered arm')
    part = training(rows, cutoff)
    columns = list(MATCHED if arm == 'matched' else FEATURES)
    raw = part.loc[:, columns].to_numpy(float)
    active = np.isfinite(raw).any(axis=0)
    fields = [name for i, name in enumerate(columns) if active[i]]
    raw = raw[:, active]
    q = np.nanquantile(raw, [.25, .5, .75], axis=0)
    median, scale = q[1], np.where(q[2] > q[0], q[2]-q[0], 1.)
    x = np.column_stack(((np.where(np.isnan(raw), median, raw)-median)/scale, np.isnan(raw).astype(float)))
    x = group_center(x, part)
    y = group_center(part.hypothetical_liquidation_net_bps.to_numpy(float)[:, None], part).ravel()
    group = part.groupby(GROUP, sort=False)
    counts = part.groupby(base.KEY[0]).package_id.transform('nunique')
    weight = 1/(group.package_id.transform('size')*counts)
    weight = weight/weight.mean()
    before_fit()
    model = Ridge(**CONTRACT['ridge']).fit(x, y, sample_weight=weight)
    if not np.isfinite(model.coef_).all() or np.max(np.atleast_1d(model.n_iter_)) >= CONTRACT['ridge']['max_iter']:
        raise ValueError('Ridge finite/convergence contract failed')
    if not np.allclose(model.predict(x), x@np.asarray(model.coef_.tolist()), rtol=1e-12, atol=1e-9):
        raise ValueError('JSON model prediction parity failed')
    return dict(contract_sha256=sha(CONTRACT), arm=arm, fields=fields, median=median.tolist(), scale=scale.tolist(),
        coef=model.coef_.tolist(), iterations=np.atleast_1d(model.n_iter_).tolist(), cutoff=str(cutoff),
        inactive_fields=[n for n, a in zip(columns, active, strict=True) if not a], train_rows=len(part),
        train_days=int(part[base.KEY[0]].nunique()), training_keys_sha256=sha([[str(v) for v in r] for r in part[KEY].itertuples(index=False, name=None)]))


def predict(inputs, model):
    validate_population(inputs)
    if model['contract_sha256'] != sha(CONTRACT) or model['arm'] not in ('candidate', 'matched'):
        raise ValueError('model identity differs')
    allowed = MATCHED if model['arm'] == 'matched' else FEATURES
    fields = model['fields']
    if not fields or len(set(fields)) != len(fields) or not set(fields).issubset(allowed):
        raise ValueError('model fields differ')
    eligible = support(numeric(inputs))
    result = inputs.loc[:, [*IDENTITY, 'parent_score', 'source_id']].copy().reset_index(drop=True)
    result['predicted_relative_net_bps'] = np.nan
    result['no_rank_advice'] = ~eligible
    result['advisory_rank'] = result.selection_effective_rank.copy()
    selected = inputs.loc[eligible].copy()
    raw = selected.loc[:, fields].to_numpy(float)
    median, scale, coef = (np.asarray(model[n], float) for n in ('median', 'scale', 'coef'))
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


def panel(candidate, matched, finance):
    for name in (*IDENTITY, 'parent_score', 'source_id', 'no_rank_advice'):
        if not candidate[name].reset_index(drop=True).equals(matched[name].reset_index(drop=True)):
            raise ValueError('arms changed original population or common support')
    result = evaluation.comparison_panel(candidate, matched, finance)
    different = candidate.loc[:, GROUP].copy()
    different['matched_intervention'] = candidate.selected.to_numpy() != matched.selected.to_numpy()
    counts = different.groupby(GROUP).matched_intervention.sum().div(2).rename('matched_intervention_count').reset_index()
    return result.merge(counts.rename(columns={base.KEY[0]:'decision_date'}), on=['package_id', 'decision_date'], validate='one_to_one')


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
        go = bool(report['proceed_outer'] and means.notna().all() and means.gt(0).sum() >= 2
            and report['versus_matched']['mean_bps'] is not None and report['versus_matched']['mean_bps'] > 0
            and interventions.sum() >= 20 and report['matched_intervention_fraction'] >= .2
            and daily.loc[daily.matched_intervention, 'fold'].nunique() >= 2)
        report.update(proceed_outer=go, classification='INNER_SUPPORTED' if go else 'INNER_NOT_SUPPORTED')
    elif report['classification'] == 'WORTH_INDEPENDENT_CONFIRMATION':
        report['classification'] = 'WORTH_NEW_CONFIRMATION_DESIGN'
    report['mechanism_not_a_second_product_gate'] = True
    return report, daily
