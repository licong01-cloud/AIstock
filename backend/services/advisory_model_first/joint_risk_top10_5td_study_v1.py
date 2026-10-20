"""One frozen downside-moment estimator; separate forecast skill and list value."""
from __future__ import annotations

from functools import lru_cache
from itertools import combinations

import numpy as np
import pandas as pd
from sklearn.covariance import LedoitWolf

from backend.services.advisory_model_first.group_centered_rank_5td_study_v1 import _validate_finance
from backend.services.advisory_model_first.joint_risk_top10_5td_inputs_v1 import (
    CONTRACT, KEY, POLICY_SHA256, ROSTER, RiskPricePanel, history_vectors,
)
from backend.services.advisory_model_first.parent_score_price_5td_evaluation_v1 import paired_statistics

ARMS = ('baseline','marginal','joint')


def estimate(z):
    if (z.ndim!=2 or z.shape[1]!=20 or len(z)<CONTRACT['minimum_common_observations']
            or not np.isfinite(z).all() or (z>0).any()):
        raise ValueError('invalid downside estimation support')
    model = LedoitWolf(assume_centered=False,store_precision=False,block_size=1000).fit(z)
    covariance, location = model.covariance_,model.location_
    means = np.outer(location,location)
    joint, marginal = covariance+means,np.diag(np.diag(covariance))+means
    if not np.isfinite(joint).all() or np.linalg.eigvalsh(joint).min() < -1e-6:
        raise ValueError('invalid joint downside moment')
    return joint,marginal,dict(location_bps=location.tolist(),shrinkage=float(model.shrinkage_))


@lru_cache(maxsize=1)
def _sets():
    baseline = set(range(10))
    result = [list(range(10))]
    for count in (1,2):
        for outgoing in combinations(range(10),count):
            for incoming in combinations(range(10,20),count):
                result.append(sorted((baseline-set(outgoing))|set(incoming)))
    indices = np.asarray(result,dtype=np.int64)
    masks = np.zeros((len(result),20))
    masks[np.arange(len(result))[:,None],indices] = 1
    indices.setflags(write=False)
    masks.setflags(write=False)
    return indices,masks


def select_sets(moment):
    if (moment.shape!=(20,20) or not np.isfinite(moment).all()
            or not np.allclose(moment,moment.T,atol=1e-8,rtol=0)):
        raise ValueError('invalid selection moment')
    indices,masks = _sets()
    scores = ((masks@moment)*masks).sum(axis=1)/100
    return indices[int(np.argmin(scores))].tolist()


def predict(roster, panel, *, before_update=lambda: None):
    panel = panel if isinstance(panel,RiskPricePanel) else RiskPricePanel(panel)
    rows, forecasts, updates = [],[],0
    # Whitelist ensures future finance/eligibility columns cannot affect predictions.
    for (package,day), group in roster.loc[:,ROSTER].groupby(KEY[:2],sort=True):
        group = group.reset_index(drop=True)
        if group.selection_effective_rank.tolist()!=list(range(1,21)):
            raise ValueError('prediction changed native Top20 order')
        z,receipt = history_vectors(panel,group.instrument.tolist(),day)
        selected = {a:list(range(10)) for a in ARMS}
        forecast = dict(package_id=package,decision_as_of_trade_date=pd.Timestamp(day).isoformat(),
                        target_trade_date=pd.Timestamp(group.target_trade_date.iloc[0]).isoformat(),
                        instruments=group.instrument.tolist(),risk_policy_sha256=POLICY_SHA256,**receipt)
        if receipt['status']=='AVAILABLE':
            if updates>=CONTRACT['max_updates']:
                raise ValueError('risk update budget exceeded')
            before_update()
            joint,marginal,info = estimate(z)
            updates += 1
            forecast.update(info)
            forecast['joint_moment_bps2'] = joint.tolist()
            for name,moment in (('joint',joint),('marginal',marginal)):
                selected[name] = select_sets(moment)
                forecast[name+'_baseline_risk_bps2'] = float(moment[:10,:10].sum()/100)
        else:
            forecast.update(joint_baseline_risk_bps2=None,marginal_baseline_risk_bps2=None)
        for i,row in enumerate(group.to_dict('records')):
            rows.append(dict(**row,**{a+'_selected':i in selected[a] for a in ARMS},
                             status=receipt['status'],risk_policy_sha256=POLICY_SHA256))
        forecasts.append(forecast)
    predictions = pd.DataFrame(rows).set_index(KEY).loc[pd.MultiIndex.from_frame(roster[KEY])].reset_index()
    return predictions,forecasts,updates


def _block_skill(values):
    """Keep original time-axis holes; inference describes observable dates only."""
    known = np.asarray([np.nan if v is None else v for v in values],dtype=float)
    mask = np.isfinite(known)
    result = dict(original_days=len(known),observed_days=int(mask.sum()),
        coverage=float(mask.mean()) if len(mask) else 0,mean_mse_improvement_bps4=None,
        interval_95_bps4=None,mde_80_bps4=None,inference='INSUFFICIENT_SUPPORT',supported=False)
    if not len(known) or not mask.any():
        return result
    result['mean_mse_improvement_bps4'] = float(known[mask].mean())
    if mask.sum()<CONTRACT['minimum_skill_days'] or mask.mean()<CONTRACT['minimum_skill_coverage']:
        return result
    rng = np.random.default_rng(CONTRACT['bootstrap_seed'])
    n,b = len(known),CONTRACT['block_sessions']
    starts = rng.integers(0,n-b+1,size=(CONTRACT['bootstrap_count'],(n+b-1)//b))
    positions = (starts[:,:,None]+np.arange(b)).reshape(len(starts),-1)[:,:n]
    samples = known[positions]
    counts = np.isfinite(samples).sum(axis=1)
    if (counts==0).any():
        return result
    means = np.nansum(samples,axis=1)/counts
    low,high = np.quantile(means,[.025,.975])
    result.update(interval_95_bps4=[float(low),float(high)],
        mde_80_bps4=float(2.801585218*means.std(ddof=1)),
        inference='ORIGINAL_AXIS_BLOCK5_OBSERVABLE_DATES_CONDITIONAL_EXPLORATORY',
        supported=bool(result['mean_mse_improvement_bps4']>0 and low>0))
    return result


def audit_skill(roster, forecasts, panel, *, evaluation_start, evaluation_end):
    panel = panel if isinstance(panel,RiskPricePanel) else RiskPricePanel(panel)
    by_key = {(f['package_id'],pd.Timestamp(f['decision_as_of_trade_date'])):f for f in forecasts}
    if (len(by_key)!=len(forecasts) or set(by_key)!=set(roster[KEY[:2]].itertuples(index=False,name=None))
            or any(f['risk_policy_sha256']!=POLICY_SHA256
                   or pd.Timestamp(f['decision_cutoff'])!=pd.Timestamp(f['decision_as_of_trade_date'])
                   or f['maximum_observation_end'] is not None and pd.Timestamp(f['maximum_observation_end'])>pd.Timestamp(f['decision_cutoff'])
                   for f in forecasts)):
        raise ValueError('risk forecast identity/cutoff mismatch')
    records = []
    for (package,day), group in roster.groupby(KEY[:2],sort=True):
        if not pd.Timestamp(evaluation_start)<=day<=pd.Timestamp(evaluation_end):
            continue
        forecast = by_key[(package,day)]
        if (group.selection_effective_rank.tolist()!=list(range(1,21))
                or forecast['instruments']!=group.instrument.tolist()
                or pd.Timestamp(forecast['target_trade_date'])!=group.target_trade_date.iloc[0]):
            raise ValueError('risk audit changed original order/T')
        position = panel.close.index.get_loc(day)
        values,actual,reason = None,None,'IMMATURE_RISK_LABEL'
        if position+5<len(panel.close.index) and panel.close.index[position+5]<=pd.Timestamp(evaluation_end):
            reason = 'UNKNOWN_RISK_ENDPOINT_OR_NO_ADVICE'
            symbols = group.instrument.iloc[:10].tolist()
            prices = panel.close.iloc[[position,position+5]].loc[:,symbols].to_numpy(float)
            factors = panel.factor.iloc[[position,position+5]].loc[:,symbols].to_numpy(float)
            if any(np.isinf(a).any() or (np.isfinite(a)&(a<=0)).any() for a in (prices,factors)):
                raise ValueError('invalid observed future risk price/factor')
            if np.isfinite(prices).all() and np.isfinite(factors).all() and forecast['status']=='AVAILABLE':
                actual = float(np.minimum((prices[1]/prices[0]-1)*10000,0).mean()**2)
                values = {a:float((forecast[a+'_baseline_risk_bps2']-actual)**2) for a in ('marginal','joint')}
                reason = 'AVAILABLE'
        records.append(dict(package_id=package,date=day.date().isoformat(),loss=values,
            actual_risk_bps2=actual,label_status=reason))
    packages = set(roster.package_id)
    dates = sorted({r['date'] for r in records})
    deltas = []
    for day in dates:
        group = [r for r in records if r['date']==day]
        if len(group)!=len(packages):
            raise ValueError('risk audit removed an original package-date')
        deltas.append(None if any(r['loss'] is None for r in group) else
            float(np.mean([r['loss']['marginal']-r['loss']['joint'] for r in group])))
    statistics = _block_skill(deltas)
    common = [r for r in records if r['loss'] is not None]
    return dict(status='LEARNABILITY_SUPPORTED' if statistics['supported'] else 'EXACT_RISK_ESTIMATOR_NOT_SUPPORTED',
        worth_economic_evaluation=statistics['supported'],statistics=statistics,dates=dates,
        mean_loss_bps4={a:float(np.mean([r['loss'][a] for r in common])) if common else None for a in ('marginal','joint')},
        label_status_counts=pd.Series([r['label_status'] for r in records]).value_counts().to_dict(),
        mse_improvement_bps4=deltas,groups=records,original_top10_forecast_only=True,
        evaluation_finance_decoded=False,objective_contract='RISK_MANAGED_ADVISORY',
        decision_use='NAVIGATION_ONLY',independent_oos_evidence=False,deployable=False)


def _mean(values):
    return None if not values or any(v is None for v in values) else float(np.mean(values))


def evaluate_economics(rows, days, predictions, *, evaluation_end):
    """Original finance policy/ten slots; never use unknowns or cash to backfill."""
    if rows.duplicated(KEY).any() or predictions.duplicated(KEY).any() or not predictions.risk_policy_sha256.eq(POLICY_SHA256).all():
        raise ValueError('economic prediction identity mismatch')
    left = rows.loc[:,ROSTER].reset_index(drop=True)
    right = predictions.loc[:,ROSTER].reset_index(drop=True)
    if not left.equals(right):
        raise ValueError('economic prediction changed source roster/score/order')
    _validate_finance(rows)
    frame = rows.copy()
    for arm in ARMS:
        frame[arm+'_selected'] = predictions[arm+'_selected'].to_numpy()
    records, groups = [],[]
    for day in days.itertuples(index=False):
        group = frame.loc[frame.package_id.eq(day.package_id) & frame.decision_as_of_trade_date.eq(day.decision_date)]
        if len(group)!=20 or not group.label_information_end.eq(day.horizon_end).all():
            raise ValueError('economic D/T/H population changed')
        mature = day.horizon_end<=pd.Timestamp(evaluation_end)
        contributions = {a:[] for a in ARMS}
        for row in group.to_dict('records'):
            state = row['valuation_status']
            value = (float(row['hypothetical_liquidation_net_bps']) if state=='AVAILABLE' else
                     0. if state=='CASH_ENTRY_NOT_EXECUTABLE' else None) if mature else None
            record = dict(package_id=day.package_id,date=day.decision_date.date().isoformat(),
                instrument=row['instrument'],mature=bool(mature),value_bps=value,state=state,
                selected={a:bool(row[a+'_selected']) for a in ARMS})
            records.append(record)
            for arm in ARMS:
                if record['selected'][arm]:
                    contributions[arm].append(value)
        if any(len(v)!=10 for v in contributions.values()):
            raise ValueError('economic fixed ten slots changed')
        groups.append(dict(package_id=day.package_id,date=day.decision_date.date().isoformat(),
            mature=bool(mature),values={a:_mean(v) for a,v in contributions.items()}))
    dates = sorted({g['date'] for g in groups if g['mature']})
    panels = {a:[_mean([g['values'][a] for g in groups if g['date']==d]) for d in dates] for a in ARMS}
    diffs = {a:[None if c is None or b is None else c-b for c,b in zip(panels['joint'],panels[a],strict=True)] for a in ARMS[:2]}
    native = paired_statistics(dict(baseline=diffs['baseline'],matched_core=diffs['marginal']))
    stats = dict(baseline=native['baseline'],marginal=native['matched_core'])
    support,attribution = {},{}
    for arm in ARMS[:2]:
        paired_dates = {d for d,v in zip(dates,diffs[arm],strict=True) if v is not None}
        changes = [r for r in records if r['date'] in paired_dates and r['selected']['joint']!=r['selected'][arm]]
        changed_days = {r['date'] for r in changes}
        support[arm] = dict(unique_stock_days=len({(r['date'],r['instrument']) for r in changes}),
            days=len(changed_days),fraction=len(changed_days)/len(dates) if dates else 0.,
            months=len({d[:7] for d in changed_days}))
        divisor = len(paired_dates)*len(days.package_id.unique())*10
        incoming = sum(r['value_bps'] for r in changes if r['selected']['joint'])
        outgoing = sum(r['value_bps'] for r in changes if r['selected'][arm])
        incremental = (incoming-outgoing)/divisor if divisor else None
        if incremental is not None and not np.isclose(incremental,stats[arm]['mean_increment_bps'],atol=1e-8,rtol=0):
            raise ValueError('economic attribution does not reconcile')
        attribution[arm] = dict(incoming_bps=incoming/divisor if divisor else None,
                               outgoing_bps=outgoing/divisor if divisor else None,increment_bps=incremental)
    risk = {a:_mean([float(v<CONTRACT['tail_bps']) for v in panels[a]]) if all(v is not None for v in panels[a]) else None for a in ARMS}
    def supported(arm):
        s,i = stats[arm],support[arm]
        ci = s['simultaneous_95_bonferroni_bps']
        return (ci is not None and ci[0]>=0 and s['mean_increment_bps']>=0
            and risk['joint'] is not None and risk[arm] is not None and risk['joint']<risk[arm]
            and i['unique_stock_days']>=30 and i['days']>=20 and i['fraction']>=.25 and i['months']>=2)
    def summarize(subset):
        return {a:dict(group_count=len(subset),known_group_count=sum(g['values'][a] is not None for g in subset),
                       complete_mean_bps=_mean([g['values'][a] for g in subset])) for a in ARMS}
    strata = {'package':{p:summarize([g for g in groups if g['mature'] and g['package_id']==p]) for p in sorted(days.package_id.unique())},
              'month':{m:summarize([g for g in groups if g['mature'] and g['date'].startswith(m)]) for m in sorted({d[:7] for d in dates})}}
    slots = {}
    for arm in ARMS:
        rr = [r for r in records if r['selected'][arm]]
        available = [r for r in rr if r['mature'] and r['state']=='AVAILABLE']
        slots[arm] = dict(original_selected=len(rr),mature_selected=sum(r['mature'] for r in rr),
            available_selected=len(available),available_win_rate=_mean([float(r['value_bps']>0) for r in available]),
            unavailable_entry=sum(r['mature'] and r['state']=='CASH_ENTRY_NOT_EXECUTABLE' for r in rr),
            unknown_selected=sum(r['mature'] and r['value_bps'] is None for r in rr))
    return dict(status='WORTH_CONFIRMING' if all(supported(a) for a in ARMS[:2]) else 'EXACT_CANDIDATE_NOT_SUPPORTED',
        statistics=stats,fixed_ten_slot_mean_bps={a:_mean(v) for a,v in panels.items()},
        tail_event_fraction=risk,support=support,attribution=attribution,slots=slots,strata=strata,
        original_days=int(days.decision_date.nunique()),mature_days=len(dates),groups=groups,
        panels=panels,dates=dates,measurement='FIXED_TEN_SLOT_5TD_NET_NOMINAL_NOT_NAV_NOT_FILL',
        evaluation_finance_decoded=True,independent_oos_evidence=False,deployable=False)
