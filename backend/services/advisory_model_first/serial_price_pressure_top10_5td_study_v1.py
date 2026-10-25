"""Two fixed Ridge arms and one paired economic readout; no production activation."""
from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.group_centered_rank_5td_study_v1 import FINANCE, _validate_finance
from backend.services.advisory_model_first.serial_price_pressure_top10_5td_inputs_v1 import (
    ARMS, CANDIDATE, CONTRACT, KEY, MATCHED, POLICY_SHA256, ROSTER,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

GROUP = ['package_id', 'decision_as_of_trade_date']


def numeric(frame, columns):
    raw = frame.loc[:, list(columns)].apply(pd.to_numeric, errors='raise').to_numpy(float)
    if np.isinf(raw).any():
        raise ValueError('infinite research feature')
    return raw


def fit_encoding(raw):
    active = np.flatnonzero(~np.isnan(raw).all(axis=0))
    known = raw[:, active]
    median = np.nanmedian(known, axis=0) if len(active) else np.array([])
    scale = np.nanquantile(known, .75, axis=0)-np.nanquantile(known, .25, axis=0) if len(active) else np.array([])
    scale[scale == 0] = 1.
    return dict(active=active.tolist(), median=median.tolist(), scale=scale.tolist(),
        inactive_fields=[name for i, name in enumerate(CANDIDATE) if i not in active])


def encode(raw, encoding, columns):
    indices = [i for i, a in enumerate(encoding['active']) if CANDIDATE[a] in columns]
    chosen = [encoding['active'][i] for i in indices]
    values = raw[:, chosen]
    median, scale = np.asarray(encoding['median'])[indices], np.asarray(encoding['scale'])[indices]
    matrix = np.column_stack(((np.where(np.isnan(values), median, values)-median)/scale, np.isnan(values).astype(float)))
    return matrix, np.isfinite(values).any(axis=1), [CANDIDATE[i] for i in chosen]


def group_center(values, frame):
    groups = pd.MultiIndex.from_frame(frame[GROUP]).factorize()[0]
    return values-pd.DataFrame(values).groupby(groups, sort=False).transform('mean').to_numpy()


def training_population(rows, *, train_start, train_end):
    # Clock filtering precedes finance/feature decoding; evaluation poison is never fitted.
    train = rows.loc[rows.decision_as_of_trade_date.between(train_start, train_end)].copy()
    if train.duplicated(KEY).any():
        raise ValueError('duplicate supervised identity')
    clock_known = train.label_information_end.le(pd.Timestamp(train_end))
    _validate_finance(train.loc[clock_known])
    eligible = clock_known & train.valuation_status.eq('AVAILABLE')
    supervised = train.loc[eligible].reset_index(drop=True)
    if len(supervised) < 2 or supervised.decision_as_of_trade_date.nunique() < 2:
        raise ValueError('insufficient mature train supervision')
    return train.reset_index(drop=True), supervised


def train_models(rows, *, train_start, train_end, frozen_encoding=None, before_fit=lambda arm: None, after_fit=lambda arm: None):
    from sklearn.linear_model import Ridge
    train, supervised = training_population(rows, train_start=train_start, train_end=train_end)
    encoding = fit_encoding(numeric(train, CANDIDATE)) if frozen_encoding is None else frozen_encoding
    raw = numeric(supervised, CANDIDATE)
    y = supervised.hypothetical_liquidation_net_bps.to_numpy(float)
    yy = group_center(y[:, None], supervised).ravel()
    sizes = supervised.groupby(GROUP).package_id.transform('size').to_numpy()
    package_counts = supervised[GROUP].drop_duplicates().groupby(GROUP[1]).size()
    weights = len(supervised)/(supervised.decision_as_of_trade_date.nunique()*sizes*
        supervised.decision_as_of_trade_date.map(package_counts).to_numpy())
    key_values = [[str(p), pd.Timestamp(d).isoformat(), str(s)] for p, d, s in supervised[KEY].itertuples(index=False, name=None)]
    models = {}
    for arm, columns in (('matched', MATCHED), ('candidate', CANDIDATE)):
        xx, _, fields = encode(raw, encoding, columns)
        xx = group_center(xx, supervised)
        if not xx.shape[1]:
            models[arm] = dict(coef=[], fields=[], columns=[], status='NO_TRAIN_IDENTIFIABLE_INPUT', iterations=[], json_parity_max_abs=0.)
            continue
        before_fit(arm)
        model = Ridge(alpha=CONTRACT['alpha'], solver=CONTRACT['solver'], tol=CONTRACT['tol'],
            max_iter=CONTRACT['max_iter'], fit_intercept=False).fit(xx, yy, sample_weight=weights)
        iterations = [] if model.n_iter_ is None else np.atleast_1d(model.n_iter_).tolist()
        if not np.isfinite(model.coef_).all() or any(n >= CONTRACT['max_iter'] for n in iterations):
            raise ValueError('Ridge failed finite/convergence contract')
        native = model.predict(xx)
        restored = xx @ np.asarray(model.coef_.tolist())
        if not np.allclose(native, restored, rtol=1e-12, atol=1e-9):
            raise ValueError('JSON/native Ridge prediction parity failed')
        models[arm] = dict(coef=model.coef_.tolist(), fields=fields, columns=[*fields, *(f'missing_{x}' for x in fields)],
            iterations=iterations, status='FITTED', json_parity_max_abs=float(np.max(np.abs(native-restored))))
        after_fit(arm)
    day_y = supervised.groupby(GROUP[1]).hypothetical_liquidation_net_bps.mean()
    return dict(schema=CONTRACT['schema'], policy_sha256=POLICY_SHA256, encoding=encoding, models=models,
        train_start=str(train_start), train_end=str(train_end), train_source_rows=len(train), supervised_rows=len(supervised),
        supervised_unique_stock_days=len(supervised[[GROUP[1], 'instrument']].drop_duplicates()),
        supervised_days=len(day_y), supervision_key_sha256=sha(key_values), weights_sha256=sha(weights.tolist()),
        physical_fit_count=sum(m['status'] == 'FITTED' for m in models.values()),
        training_scale_mde_proxy_bps=float(3.083*day_y.std(ddof=1)/np.sqrt(len(day_y))),
        training_scale_mde_semantics='TRAIN_LABEL_DAY_SCALE_PROXY_NOT_PAIRED_CONFIRMATORY_POWER')


def choose_top10(scores, ranks):
    """Derived additive ranking, <=2 strict-improvement swaps from original Top10."""
    scores, ranks = np.asarray(scores, float), np.asarray(ranks)
    if len(scores) != 20 or sorted(ranks) != list(range(1, 21)) or np.isinf(scores).any():
        raise ValueError('derived ranking needs unchanged original Top20')
    chosen = set(np.flatnonzero(ranks <= 10))
    inside = sorted([i for i in chosen if np.isfinite(scores[i])], key=lambda i: (scores[i], -ranks[i]))
    outside = sorted([i for i in range(20) if i not in chosen and np.isfinite(scores[i])], key=lambda i: (-scores[i], ranks[i]))
    for i, j in list(zip(inside, outside))[:CONTRACT['max_replacements']]:
        if scores[j] <= scores[i]:
            break
        chosen.remove(i)
        chosen.add(j)
    def order(i):
        return (0 if np.isfinite(scores[i]) else 1, -scores[i] if np.isfinite(scores[i]) else 0., ranks[i])
    order_all = sorted(chosen, key=order)+sorted(set(range(20))-chosen, key=order)
    derived = np.empty(20, int)
    derived[order_all] = np.arange(1, 21)
    return np.asarray([i in chosen for i in range(20)]), derived


def predict(inputs, fitted):
    if fitted['policy_sha256'] != POLICY_SHA256 or inputs.duplicated(KEY).any():
        raise ValueError('prediction policy/population mismatch')
    frame = inputs.reset_index(drop=True)
    raw = numeric(frame, CANDIDATE)
    output = frame.loc[:, list(ROSTER)].copy()  # Never reconstruct native float32 scores from row dicts.
    output['native_selected'] = output.selection_effective_rank.le(10)
    for arm, columns in (('matched', MATCHED), ('candidate', CANDIDATE)):
        xx, known, fields = encode(raw, fitted['encoding'], columns)
        model = fitted['models'][arm]
        if fields != model['fields'] or len(model['coef']) != xx.shape[1]:
            raise ValueError('model encoding/column identity differs')
        scores = group_center(xx, frame) @ np.asarray(model['coef'])
        scores[~known] = np.nan
        output[arm+'_score'] = scores
        output[arm+'_status'] = np.where(known, 'SCORED_RELATIVE_ONLY', 'NO_RANK_ADVICE')
        output[arm+'_selected'] = False
        output[arm+'_rank'] = 0
        for idx in frame.groupby(GROUP, sort=False).indices.values():
            selected, ranks = choose_top10(scores[idx], frame.selection_effective_rank.to_numpy()[idx])
            output.loc[idx, arm+'_selected'] = selected
            output.loc[idx, arm+'_rank'] = ranks
    output['research_policy_sha256'] = POLICY_SHA256
    return output


def paired_interval(values):
    values = np.asarray(values, float)
    known = np.isfinite(values)
    result = dict(mean_bps=float(values[known].mean()) if known.any() else None, complete_days=int(known.sum()),
        original_days=len(values), ci=None, paired_mde_bps=None, status='INCONCLUSIVE')
    blocks = [values[i:i+5] for i in range(max(0, len(values)-4)) if known[i:i+5].all()]
    if known.sum() < CONTRACT['minimum_complete_days'] or known.mean() < CONTRACT['minimum_complete_fraction'] or not blocks:
        return result
    rng = np.random.default_rng(CONTRACT['bootstrap_seed'])
    blocks = np.asarray(blocks)
    count = int(np.ceil(known.sum()/5))
    draws = np.asarray([blocks[rng.integers(len(blocks), size=count)].ravel()[:known.sum()].mean()
        for _ in range(CONTRACT['bootstrap_count'])])
    result.update(ci=list(map(float, np.quantile(draws, [.0125, .9875]))), paired_mde_bps=float(3.083*draws.std(ddof=1)),
        status='EXPLORATORY_PAIRED_INTERVAL', legal_contiguous_blocks=len(blocks), confidence=.975)
    return result


def distribution(values):
    values = np.asarray(values, float)
    values = values[np.isfinite(values)]
    def mean(v):
        return float(v.mean()) if len(v) else None
    return dict(known_count=len(values), win_rate=mean((values > 0).astype(float)), mean_bps=mean(values),
        average_win_bps=mean(values[values > 0]), average_loss_bps=mean(values[values < 0]),
        quantiles_bps=list(map(float, np.quantile(values, [0, .05, .25, .5, .75, .95, 1]))) if len(values) else None)


def market_stratum(value):
    return 'UNKNOWN' if not np.isfinite(value) else 'POSITIVE' if value > 0 else 'NEGATIVE' if value < 0 else 'ZERO'


def evaluate(inputs, finance, days, predictions, *, evaluation_end):
    if (finance.duplicated(KEY).any() or predictions.duplicated(KEY).any()
            or not predictions.loc[:, list(ROSTER)].equals(inputs.loc[:, list(ROSTER)].reset_index(drop=True))):
        raise ValueError('evaluation native projection/identity differs')
    frame = predictions.merge(finance.loc[:, [*ROSTER, *FINANCE]], on=list(ROSTER), how='left', validate='one_to_one', indicator=True)
    if not frame['_merge'].eq('both').all():
        raise ValueError('evaluation finance population differs')
    _validate_finance(frame)
    mature = frame.label_information_end.le(pd.Timestamp(evaluation_end))
    returns = np.full(len(frame), np.nan)
    available = frame.valuation_status.eq('AVAILABLE') & mature
    returns[available] = frame.loc[available, 'hypothetical_liquidation_net_bps']
    cash = frame.valuation_status.eq('CASH_ENTRY_NOT_EXECUTABLE') & mature
    returns[cash] = 0.
    frame['net'] = returns
    frame = frame.merge(inputs.loc[:, [*KEY, 'csi300_ret_5']], on=KEY, how='left', validate='one_to_one')
    group_rows, skills, intervention = [], [], {op: {} for op in ('native', 'matched')}
    for (package, day), group in frame.groupby(GROUP, sort=True):
        market = group.csi300_ret_5.dropna().unique()
        if len(market) > 1:
            raise ValueError('D market context contradicts itself')
        item = dict(package_id=package, decision_date=pd.Timestamp(day).isoformat(),
            stratum=market_stratum(float(market[0]) if len(market) else np.nan))
        for arm in ARMS:
            selected = group[arm+'_selected'].to_numpy(bool)
            values = group.net.to_numpy()[selected]
            if selected.sum() != 10:
                raise ValueError('fixed ten slots changed')
            item[arm] = float(values.mean()) if np.isfinite(values).all() else None
            item[arm+'_known_slots'] = int(np.isfinite(values).sum())
        for opponent in ('native', 'matched'):
            difference = group.candidate_selected != group[opponent+'_selected']
            item['vs_'+opponent+'_changed_stocks'] = int(difference.sum())
            intervention[opponent].setdefault(str(package), []).append((str(day), bool(difference.any())))
            missed = group.loc[group[opponent+'_selected'] & ~group.candidate_selected, 'net'].to_numpy()
            item['vs_'+opponent+'_missed_upside_bps'] = float(np.maximum(missed, 0).sum()) if np.isfinite(missed).all() else None
        group_rows.append(item)
        for arm in ARMS[1:]:
            known = np.isfinite(group.net) & np.isfinite(group[arm+'_score'])
            yy, pp = group.loc[known, 'net'], group.loc[known, arm+'_score']
            mse = float(np.mean(np.square((yy-yy.mean())-(pp-pp.mean())))) if len(yy) else None
            rankic = float(yy.rank().corr(pp.rank())) if len(yy) > 1 and yy.nunique() > 1 and pp.nunique() > 1 else None
            skills.append(dict(package_id=str(package), decision_date=str(day), arm=arm, known_rows=len(yy), mse=mse, rankic=rankic))
    expected = {(str(r.package_id), pd.Timestamp(r.decision_date).isoformat()) for r in days.itertuples(index=False)}
    if {(str(r['package_id']), r['decision_date']) for r in group_rows} != expected:
        raise ValueError('original package/date axis changed')
    axis = sorted(set(r['decision_date'] for r in group_rows))
    daily = []
    for day in axis:
        gg = [g for g in group_rows if g['decision_date'] == day]
        complete = all(g[a] is not None for g in gg for a in ARMS)
        row = dict(decision_date=day, complete=complete,
            stratum=gg[0]['stratum'] if len({g['stratum'] for g in gg}) == 1 else 'UNKNOWN')
        for arm in ARMS:
            row[arm] = float(np.mean([g[arm] for g in gg])) if all(g[arm] is not None for g in gg) else None
        for opponent in ('native', 'matched'):
            row['candidate_minus_'+opponent] = row['candidate']-row[opponent] if complete else None
            row['vs_'+opponent+'_intervention'] = any(g['vs_'+opponent+'_changed_stocks'] > 0 for g in gg)
        daily.append(row)
    endpoints, support = {}, {}
    for opponent in ('native', 'matched'):
        endpoints[opponent] = paired_interval([np.nan if r['candidate_minus_'+opponent] is None else r['candidate_minus_'+opponent] for r in daily])
        by_package = {p: sum(v for _, v in vv) for p, vv in intervention[opponent].items()}
        support[opponent] = dict(days=sum(r['vs_'+opponent+'_intervention'] for r in daily), per_package_days=by_package)
    enough = all(s['days'] >= CONTRACT['minimum_intervention_days'] and
        all(n >= CONTRACT['minimum_package_intervention_days'] for n in s['per_package_days'].values()) for s in support.values())
    passed = enough and all(e['mean_bps'] is not None and e['mean_bps'] >= CONTRACT['minimum_gain_bps']
        and e['ci'] is not None and e['ci'][0] > 0 for e in endpoints.values())
    return dict(outcome='SUPPORT_NEW_CONFIRMATION_DESIGN_ONLY' if passed else 'STOP_THIS_CANDIDATE',
        objective_contract='ALPHA_RANKING', decision_use='NAVIGATION_ONLY', economic_confirmation=False, activation=False,
        original_rows=len(frame), original_days=len(axis), original_package_days=len(group_rows),
        common_complete_days=sum(r['complete'] for r in daily), endpoints=endpoints, intervention_support=support,
        valuation_status_counts=frame.valuation_status.value_counts().to_dict(), mature_rows=int(mature.sum()),
        role_slot_distribution={a: distribution(frame.loc[frame[a+'_selected'], 'net']) for a in ARMS},
        role_unknown_slots={a: int(frame.loc[frame[a+'_selected'], 'net'].isna().sum()) for a in ARMS},
        role_daily_distribution={a: distribution([np.nan if d[a] is None else d[a] for d in daily]) for a in ARMS},
        strata={s: dict(original_days=sum(d['stratum'] == s for d in daily),
            endpoints={o: distribution([np.nan if d['candidate_minus_'+o] is None else d['candidate_minus_'+o]
                for d in daily if d['stratum'] == s]) for o in ('native', 'matched')})
            for s in ('POSITIVE', 'NEGATIVE', 'ZERO', 'UNKNOWN')},
        daily=daily, package_days=group_rows, skills=skills,
        measurement='TEN_FIXED_SLOTS_FIVE_SESSION_NOMINAL_COHORT_NOT_NAV_OR_ACTUAL_FILL')
