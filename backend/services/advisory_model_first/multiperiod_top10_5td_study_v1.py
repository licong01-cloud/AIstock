"""Finite, time-forward model comparison; private ranks never replace native ranks."""
import math

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.group_centered_rank_5td_study_v1 import _validate_finance
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE, IDENTITY, INPUT_FEATURES

CONFIGS = tuple([dict(id='ridge_'+str(a), family='ridge', alpha=a) for a in (1., 10., 100.)]
    + [dict(id='hgb_'+str(n), family='hgb', max_leaf_nodes=n) for n in (3, 7, 15)])
FOLDS = (('2025-03-31', '2025-04-01', '2025-05-30'),
         ('2025-05-30', '2025-06-01', '2025-07-31'),
         ('2025-07-31', '2025-08-01', '2025-09-30'))
CONTRACT = dict(schema='advisory_multiperiod_top10_5td_v1', start='2024-07-04', end='2026-08-28',
    train_end='2025-09-30', outer_start='2025-10-01', sealed_start='2026-08-31', top_k=10,
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY',
    configs=CONFIGS, folds=FOLDS, physical_fit_budget=19, seed=20261021, features=INPUT_FEATURES,
    tree=dict(max_iter=120, learning_rate=.05, min_samples_leaf=100, l2_regularization=10., early_stopping=False),
    ridge=dict(solver='lsqr', tol=1e-6, max_iter=2000, fit_intercept=False),
    bootstrap=dict(block_sessions=10, replications=10000, seed=20261021),
    worth_confirming=dict(mean_bps=10., ci_lower_bps=0., intervention_days=20, intervention_fraction=.2,
        intervention_blocks=2, paired_coverage=.8, max_unknown_excess=.02, max_tail_deterioration_bps=100.),
    valuation_policy_sha256=VALUATION_POLICY_SHA256)


def numeric(frame):
    values = frame.loc[:, INPUT_FEATURES].apply(pd.to_numeric, errors='raise').to_numpy(float)
    if np.isinf(values).any():
        raise ValueError('nonfinite known D inputs')
    return values


def fit_encoding(frame):
    x = numeric(frame)
    active = np.isfinite(x).any(axis=0)
    median, scale = np.zeros(x.shape[1]), np.ones(x.shape[1])
    for j in np.flatnonzero(active):
        q = np.quantile(x[np.isfinite(x[:, j]), j], [.25, .5, .75])
        median[j], scale[j] = q[1], q[2]-q[0] if q[2] > q[0] else 1.
    return dict(median=median.tolist(), scale=scale.tolist(), active=active.tolist())


def encode(frame, encoding):
    x = numeric(frame)
    active = np.asarray(encoding['active'], bool)
    observed = np.isfinite(x) & active
    raw = (np.where(observed, x, encoding['median'])-encoding['median'])/encoding['scale']
    raw[:, ~active] = 0.
    return np.concatenate([raw, (~observed & active).astype(float)], axis=1), observed.any(axis=1)


def training_population(rows, end):
    # Clock projection precedes decoding feature or target values (including poison).
    d = rows[KEY[0]]
    part = rows.loc[d.ge(pd.Timestamp(CONTRACT['start'])) & d.le(pd.Timestamp(end))].copy()
    part = part.loc[part.label_information_end.le(pd.Timestamp(end))].copy()
    _validate_finance(part)
    if part.groupby(list(KEY))[[*INPUT_FEATURES[:6], 'hypothetical_liquidation_net_bps']].nunique(dropna=False).gt(1).any().any():
        raise ValueError('same stock/D has contradictory shared finance')
    known = part.valuation_status.isin(['AVAILABLE', 'CASH_ENTRY_NOT_EXECUTABLE']) & np.isfinite(numeric(part)).any(axis=1)
    part = part.loc[known].reset_index(drop=True)
    if len(part) < 2 or part[KEY[0]].nunique() < 2:
        raise ValueError('insufficient mature train population')
    groups = part.groupby(['package_id', KEY[0]], sort=False)
    y = part.hypothetical_liquidation_net_bps-groups.hypothetical_liquidation_net_bps.transform('mean')
    packages = part.groupby(KEY[0]).package_id.transform('nunique')
    weight = 1/(groups.package_id.transform('size')*packages)
    weight = weight/weight.mean()
    return part, y.to_numpy(float), weight.to_numpy(float)


def fit_model(rows, end, config, *, before_fit=lambda: None, after_fit=lambda: None):
    part, target, weight = training_population(rows, end)
    encoding = fit_encoding(part)
    x, _ = encode(part, encoding)
    if config['family'] == 'ridge':
        model = Ridge(alpha=config['alpha'], **CONTRACT['ridge'])
    elif config['family'] == 'hgb':
        model = HistGradientBoostingRegressor(max_leaf_nodes=config['max_leaf_nodes'], random_state=CONTRACT['seed'], **CONTRACT['tree'])
    else:
        raise ValueError('unregistered model family')
    before_fit()
    model.fit(x, target, sample_weight=weight)
    after_fit()
    return model, encoding


def predict(rows, model, encoding):
    result = rows.loc[:, IDENTITY].copy()
    x, known = encode(rows, encoding)
    values = model.predict(x)
    if not np.isfinite(values).all():
        raise ValueError('model emitted nonfinite scores')
    result['predicted_relative_net_bps'] = values
    result['no_rank_advice'] = ~known
    result['advisory_rank'] = result.selection_effective_rank.copy()
    for _, group in result.groupby(['package_id', KEY[0]], sort=False):
        indices = group.index[~group.no_rank_advice]
        slots = sorted(result.loc[indices, 'selection_effective_rank'])
        ordered = result.loc[indices].sort_values(['predicted_relative_net_bps', 'selection_effective_rank'], ascending=[False, True], kind='stable').index
        result.loc[ordered, 'advisory_rank'] = slots
    result['selected'] = result.advisory_rank.le(10)
    return result


def cohort(predictions, finance):
    if predictions.duplicated(['package_id', *KEY]).any() or finance.duplicated(['package_id', *KEY]).any():
        raise ValueError('duplicate prediction/valuation keys')
    if set(map(tuple, predictions[['package_id', *KEY]].to_numpy())) != set(map(tuple, finance[['package_id', *KEY]].to_numpy())):
        raise ValueError('prediction and valuation populations differ')
    _validate_finance(finance)
    joined = predictions.merge(finance, on=['package_id', *KEY], validate='one_to_one')
    groups = []
    for (package, d), g in joined.groupby(['package_id', KEY[0]], sort=True):
        n = len(g)
        if (sorted(g.selection_effective_rank) != list(range(1, n+1)) or sorted(g.advisory_rank) != list(range(1, n+1))
                or not g.candidate_group_size.eq(n).all() or not g.selected.map(lambda v: isinstance(v, (bool, np.bool_))).all()
                or not g.selected.eq(g.advisory_rank.le(10)).all()):
            raise ValueError('derived ranks/counts/selection do not preserve original slots')
        chosen, baseline = g.selected.astype(bool), g.selection_effective_rank.le(10)
        known = g.valuation_status.isin(['AVAILABLE', 'CASH_ENTRY_NOT_EXECUTABLE']) & g.hypothetical_liquidation_net_bps.notna()
        row = dict(package_id=package, decision_date=d, mature=not g.valuation_status.eq('IMMATURE').any(),
            intervention_count=int((chosen & ~baseline).sum()), original_candidates=len(g),
            candidate_unknown=int((chosen & ~known).sum()), baseline_unknown=int((baseline & ~known).sum()))
        for name, mask in (('candidate', chosen), ('baseline', baseline)):
            row[name] = float(g.loc[mask, 'hypothetical_liquidation_net_bps'].mean()) if mask.sum() == 10 and known.loc[mask].all() else np.nan
        row['delta'] = row['candidate']-row['baseline']
        y = g.hypothetical_liquidation_net_bps
        if np.isfinite(row['delta']):
            removed, added = baseline & ~chosen, chosen & ~baseline
            row.update(avoided_loss=float(-y.loc[removed].clip(upper=0).sum()/10),
                missed_profit=float(y.loc[removed].clip(lower=0).sum()/10),
                added_profit=float(y.loc[added].clip(lower=0).sum()/10),
                added_loss=float(-y.loc[added].clip(upper=0).sum()/10))
        groups.append(row)
    return pd.DataFrame(groups)


def select_configuration(rows, *, before_fit=lambda key: None, after_fit=lambda key: None):
    """Outer rows are removed before feature/finance validation or model selection."""
    inner = rows.loc[rows[KEY[0]].le(pd.Timestamp(CONTRACT['train_end']))].copy()
    predictions, panels = [], []
    for config in CONFIGS:
        for fold, (end, first, last) in enumerate(FOLDS, 1):
            key = config['id']+'_fold'+str(fold)
            model, encoding = fit_model(inner, end, config, before_fit=lambda k=key: before_fit(k), after_fit=lambda k=key: after_fit(k))
            validation = inner.loc[inner[KEY[0]].between(first, last)].copy()
            p = predict(validation, model, encoding)
            f = validation.copy()
            # The fold's validation cutoff, not a later fold's known outcome.
            immature = f.label_information_end.isna() | f.label_information_end.gt(pd.Timestamp(last))
            f.loc[immature, 'valuation_status'] = 'IMMATURE'
            f.loc[immature, 'hypothetical_liquidation_net_bps'] = np.nan
            panel = cohort(p, f.loc[:, ['package_id', *KEY, *FINANCE]])
            for frame in (p, panel):
                frame['config_id'], frame['fold'] = config['id'], fold
            predictions.append(p)
            panels.append(panel)
    panel = pd.concat(panels, ignore_index=True)
    wide = panel.pivot(index=['decision_date', 'package_id'], columns='config_id', values='delta')
    package_complete = wide.notna().all(axis=1)
    valid = package_complete.groupby(level=0).all() & package_complete.groupby(level=0).size().eq(inner.package_id.nunique())
    days = valid.index[valid]
    common = wide.loc[wide.index.get_level_values(0).isin(days)]
    if common.empty:
        raise ValueError('no complete common inner validation days')
    means = common.groupby(level=0).mean().mean().to_dict()
    best = max(range(len(CONFIGS)), key=lambda i: (means[CONFIGS[i]['id']], -i))
    return dict(config=CONFIGS[best], common_validation_days=len(days), mean_delta_bps=means,
        physical_fit_count=18, selection='MAX_COMMON_D_MEAN_THEN_REGISTERED_CONFIG_ORDER'), pd.concat(predictions, ignore_index=True), panel


def paired_interval(values):
    """Preserve holes and the original D axis in synchronous block resampling."""
    a = np.asarray(values, float)
    known = np.isfinite(a)
    n = int(known.sum())
    if n < 2:
        return dict(mean_bps=float(a[known].mean()) if n else None, ci95_bps=None, se_bps=None, mde80_bps=None, paired_days=n,
            declared_days=len(a), conservative_blocks=n//10, positive_acf_effective_days=None)
    centered = a-np.nanmean(a)
    variance = float(np.nanmean(centered**2))
    rho = []
    for lag in range(1, min(10, len(a))):
        ok = known[:-lag] & known[lag:]
        rho.append(max(0., float(np.mean(centered[:-lag][ok]*centered[lag:][ok])/variance)) if ok.any() and variance else 0.)
    ess = n/(1+2*sum(r*(1-(i+1)/len(a)) for i, r in enumerate(rho)))
    rng = np.random.default_rng(20261021)
    samples = []
    for _ in range(10000):
        starts = rng.integers(0, len(a), math.ceil(len(a)/10))
        indices = ((starts[:, None]+np.arange(10)) % len(a)).ravel()[:len(a)]
        sample = a[indices]
        if np.isfinite(sample).any():
            samples.append(float(np.nanmean(sample)))
    se = float(np.std(samples, ddof=1)) if len(samples) > 1 else None
    return dict(mean_bps=float(np.nanmean(a)), ci95_bps=np.quantile(samples, [.025, .975]).tolist() if len(samples) >= 9500 else None,
        se_bps=se, mde80_bps=(1.96+.841621)*se if se is not None else None, paired_days=n, declared_days=len(a),
        conservative_blocks=n//10, positive_acf_effective_days=float(min(n, max(1., ess))), bootstrap_valid_samples=len(samples))


def classify(stat, *, tail_delta, intervention_days, intervention_fraction, intervention_blocks, coverage, unknown_excess):
    ci, mean = stat['ci95_bps'], stat['mean_bps']
    if mean is None or ci is None or tail_delta is None:
        return 'INCONCLUSIVE'
    if ci[1] < 0 or mean <= 0 or tail_delta < -100:
        return 'NOT_WORTH_CONTINUING_THIS_CANDIDATE'
    if (mean >= 10 and ci[0] > 0 and intervention_days >= 20 and intervention_fraction >= .2
            and intervention_blocks >= 2 and coverage >= .8 and unknown_excess <= .02):
        return 'WORTH_INDEPENDENT_CONFIRMATION'
    return 'INCONCLUSIVE'


def _distribution(values):
    a = np.asarray(values, float)
    a = a[np.isfinite(a)]
    if not len(a):
        return dict(known=0, mean_bps=None, win_rate=None, percentiles_bps=None, lower_tail10_mean_bps=None)
    return dict(known=len(a), mean_bps=float(a.mean()), win_rate=float((a > 0).mean()), percentiles_bps=np.quantile(a, [.05, .5, .95]).tolist(),
        lower_tail10_mean_bps=float(np.sort(a)[:max(1, math.ceil(.1*len(a)))].mean()))


def evaluate(predictions, finance):
    panel = cohort(predictions, finance)
    count = predictions.package_id.nunique()
    grouped = panel.groupby('decision_date', sort=True)
    complete = grouped.delta.count().eq(count)
    daily = grouped[['candidate', 'baseline', 'delta']].mean()
    daily.loc[~complete] = np.nan
    mature = grouped.mature.all()
    interventions = grouped.intervention_count.sum().gt(0) & mature
    periods = pd.DatetimeIndex(panel.decision_date).to_period('Q').astype(str)
    panel['period'] = periods
    stat = paired_interval(daily.delta.to_numpy())
    candidate, baseline = _distribution(daily.candidate), _distribution(daily.baseline)
    tail = (candidate['lower_tail10_mean_bps']-baseline['lower_tail10_mean_bps']) if candidate['known'] else None
    coverage = float((complete & mature).sum()/mature.sum()) if mature.any() else 0.
    fraction = float(interventions.sum()/mature.sum()) if mature.any() else 0.
    blocks = len(set(pd.DatetimeIndex(interventions.index[interventions]).to_period('Q')))
    known_panel = panel.loc[panel.mature]
    excess = float((known_panel.candidate_unknown-known_panel.baseline_unknown).sum()/(10*len(known_panel))) if len(known_panel) else 0.
    result = dict(statistics=stat, candidate=candidate, baseline=baseline, tail_delta_bps=tail, intervention_days=int(interventions.sum()),
        intervention_fraction=fraction, intervention_blocks=blocks, common_mature_coverage=coverage, unknown_excess=excess,
        candidate_unknown_slots=int(panel.candidate_unknown.sum()), baseline_unknown_slots=int(panel.baseline_unknown.sum()),
        original_package_days=len(panel), original_candidates=len(predictions), original_decision_days=len(daily),
        mature_decision_days=int(mature.sum()), immutable_predictions_before_finance=True,
        actual_fill_proven=False, independent_oos_evidence=False, activation_evidence=False, decision_use='NAVIGATION_ONLY',
        valuation_policy_sha256=VALUATION_POLICY_SHA256)
    result['classification'] = classify(stat, tail_delta=tail, intervention_days=int(interventions.sum()),
        intervention_fraction=fraction, intervention_blocks=blocks, coverage=coverage, unknown_excess=excess)
    result['by_package'] = {str(p):dict(candidate=_distribution(g.candidate), baseline=_distribution(g.baseline),
        delta=_distribution(g.delta), interventions=int(g.intervention_count.gt(0).sum())) for p, g in panel.groupby('package_id')}
    result['by_period'] = {str(p):dict(candidate=_distribution(g.candidate), baseline=_distribution(g.baseline),
        delta=_distribution(g.delta)) for p, g in daily.groupby(daily.index.to_period('Q'))}
    primary = panel.loc[panel.decision_date.isin(complete.index[complete])]
    for name in ('avoided_loss', 'missed_profit', 'added_profit', 'added_loss'):
        result[name+'_bps'] = float(primary[name].mean()) if name in primary and len(primary) else None
    result['attribution_population'] = 'PRIMARY_COMMON_COMPLETE_D_PACKAGE_EQUAL'
    result['win_rate_definition'] = 'POSITIVE_FIVE_SESSION_TOP10_COHORT_NOT_FILL_WIN_RATE'
    result['exit_execution_status'] = {str(k): int(v) for k, v in finance.exit_execution_status.value_counts(dropna=False).items()}
    return result, panel, daily.reset_index()
