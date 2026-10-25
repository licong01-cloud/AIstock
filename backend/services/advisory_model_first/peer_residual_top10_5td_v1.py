"""One candidate-peer residual representation study, never native selection."""
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first import multiperiod_top10_5td_study_v1 as base
from backend.services.advisory_model_first import native_anchored_top10_5td_v1 as evaluation
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import IDENTITY, INPUT_FEATURES
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

KEY = ['package_id', *base.KEY]
GROUP = ['package_id', base.KEY[0]]
COMPONENTS = ('peer_stock_log_return5_bps', 'peer_log_return5_bps', 'peer_alpha60_bps', 'peer_beta60', 'peer_residual_sigma60_bps')
NEW = 'peer_residual_reversal_z5'
MATCHED = (*INPUT_FEATURES, *COMPONENTS)
FEATURES = (*MATCHED, NEW)
CONTRACT = dict(schema='peer_residual_top10_5td_v1', hypothesis='PEER_RESIDUAL_REVERSAL_TOP10_5TD_1',
    objective_contract='ALPHA_RANKING', study_type='EXPLORATORY_SCREEN', decision_use='NAVIGATION_ONLY',
    matched=MATCHED, candidate=FEATURES, history_sessions=60, signal_sessions=5,
    minimum_peer_count=40, minimum_history_pairs=48, original_topk=50, top_k=10,
    ridge=dict(alpha=100., solver='lsqr', tol=1e-6, max_iter=2000, fit_intercept=False),
    start=base.CONTRACT['start'], train_end=base.CONTRACT['train_end'], outer_start=base.CONTRACT['outer_start'],
    end=base.CONTRACT['end'], sealed_start=base.CONTRACT['sealed_start'], folds=base.FOLDS,
    max_fits=8, planned_candidates=1, prior_fits=195, prior_index=1, max_rows=52400,
    max_rss_bytes=2*1024**3, max_seconds=1800, max_output_bytes=200*1024**2,
    valuation_policy_sha256=base.VALUATION_POLICY_SHA256,
    evaluation_contract=base.CONTRACT['worth_confirming'], bootstrap=base.CONTRACT['bootstrap'],
    inner_rule='BOTH_POSITIVE_TWO_POSITIVE_FOLDS_COVERAGE80_INTERVENTION20D_20PCT_TWO_FOLDS_AND_TWO_POSITIVE_Z_COEFFICIENTS')


def residual_features(prices, factors, *, day, calendar):
    """Both panels use original calendar rows; project D BEFORE decoding numbers."""
    day = pd.Timestamp(day)
    calendar = pd.DatetimeIndex(calendar)
    if calendar.has_duplicates or not calendar.is_monotonic_increasing or day not in calendar:
        raise ValueError('invalid original calendar')
    expected = calendar[calendar <= day][-66:]
    p = prices.loc[:day].tail(66)
    f = factors.loc[:day].tail(66)
    if (not p.index.equals(expected) or not p.index.equals(f.index) or not p.columns.equals(f.columns) or len(p.columns) != 50
            or p.columns.has_duplicates or p.index.has_duplicates or not p.index.is_monotonic_increasing
            or not len(p) or p.index[-1] != day):
        raise ValueError('peer population or calendar mismatch')
    result = pd.DataFrame({n:np.nan for n in (*COMPONENTS, NEW)}, index=p.columns)
    result['peer_reason'] = 'INSUFFICIENT_HISTORY'
    result['peer_history_pairs'] = 0
    result['peer_signal_min_peers'] = 0
    if len(p) != 66:
        return result
    a, f = p.to_numpy(float), f.to_numpy(float)
    if any(np.isinf(x).any() or (x[np.isfinite(x)] <= 0).any() for x in (a, f)):
        raise ValueError('invalid known peer price/factor')
    observed = np.isfinite(a) & np.isfinite(f)
    pairs = observed[1:] & observed[:-1]
    with np.errstate(invalid='ignore'):
        returns = np.where(pairs, np.diff(np.log(a), axis=0)*10000, np.nan)
    count = pairs.sum(axis=1)[:, None]-pairs
    # nansum is an intermediate observed sum, NEVER an imputed individual return.
    sums = np.nansum(returns, axis=1)[:, None]-np.where(pairs, returns, 0.)
    peer = np.divide(sums, count, out=np.full_like(sums, np.nan), where=count >= CONTRACT['minimum_peer_count'])
    for i, symbol in enumerate(p.columns):
        joint = np.isfinite(returns[:60, i]) & np.isfinite(peer[:60, i])
        result.loc[symbol, 'peer_history_pairs'] = int(joint.sum())
        result.loc[symbol, 'peer_signal_min_peers'] = int(count[-5:, i].min())
        if joint.sum() < CONTRACT['minimum_history_pairs']:
            continue
        if not np.isfinite(returns[-5:, i]).all() or not np.isfinite(peer[-5:, i]).all():
            result.loc[symbol, 'peer_reason'] = 'UNKNOWN_SIGNAL_ENDPOINT_OR_PEERS'
            continue
        y, x = returns[:60, i][joint], peer[:60, i][joint]
        variance = float(np.mean((x-x.mean())**2))
        if variance == 0:
            result.loc[symbol, 'peer_reason'] = 'ZERO_PEER_VARIANCE'
            continue
        beta = float(np.mean((x-x.mean())*(y-y.mean()))/variance)
        alpha = float(y.mean()-beta*x.mean())
        sigma = float(np.std(y-alpha-beta*x, ddof=0))
        precision = 64*np.finfo(float).eps*max(1., float(np.max(np.abs(y))), float(np.max(np.abs(beta*x))))
        if sigma <= precision:
            result.loc[symbol, 'peer_reason'] = 'ZERO_RESIDUAL_VARIANCE'
            continue
        own5, peer5 = float(returns[-5:, i].sum()), float(peer[-5:, i].sum())
        z = -(own5-5*alpha-beta*peer5)/(np.sqrt(5)*sigma)
        values = [own5, peer5, alpha, beta, sigma, z]
        if not np.isfinite(values).all():
            raise ValueError('nonfinite peer residual arithmetic')
        result.loc[symbol, [*COMPONENTS, NEW]] = values
        result.loc[symbol, 'peer_reason'] = 'AVAILABLE'
    return result


def attach(inputs, prices, factors, calendar, progress=lambda event: None):
    validate_population(inputs)
    records = []
    for number, ((package, day), group) in enumerate(inputs.groupby(GROUP, sort=True), 1):
        symbols = group.instrument.tolist()
        features = residual_features(prices.loc[:, symbols], factors.loc[:, symbols], day=day, calendar=calendar)
        features = features.rename_axis('instrument').reset_index()
        features['package_id'], features[base.KEY[0]] = package, day
        features['target_trade_date'] = group.target_trade_date.iloc[0]
        records.append(features)
        if number % 100 == 0:
            progress(dict(phase='PEER_CONTEXT', package_days=number))
    fields = [*COMPONENTS, NEW, 'peer_reason', 'peer_history_pairs', 'peer_signal_min_peers']
    result = inputs.merge(pd.concat(records, ignore_index=True)[[*KEY, *fields]], on=KEY, how='left', validate='one_to_one', sort=False)
    for name in (*IDENTITY, 'parent_score', 'source_id'):
        if not result[name].reset_index(drop=True).equals(inputs[name].reset_index(drop=True)):
            raise ValueError('original identity, score or order changed')
    numeric(result)
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


def numeric(frame):
    x = frame.loc[:, FEATURES].apply(pd.to_numeric, errors='raise').to_numpy(float)
    if np.isinf(x).any():
        raise ValueError('infinite known feature')
    return x


def support(x):
    return np.isfinite(x[:, len(INPUT_FEATURES):]).all(axis=1) & np.isfinite(x[:, :len(INPUT_FEATURES)]).any(axis=1)


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


def direction_supported(coefficients):
    if set(coefficients) != {'1','2','3'} or not np.isfinite(list(coefficients.values())).all():
        raise ValueError('invalid frozen fold coefficients')
    return sum(v > 0 for v in coefficients.values()) >= 2
