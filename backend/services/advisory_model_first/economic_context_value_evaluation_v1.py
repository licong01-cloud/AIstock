"""Four-arm full-population development navigation, never confirmation."""
from decimal import Decimal
import json

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_context_value_contracts_v1 import ARMS
from backend.services.advisory_model_first.economic_context_value_inference_v1 import context_value_nodes_v1
from backend.services.advisory_model_first.economic_context_value_pipeline_v1 import _ledger, _publish, _record, load_context_value_fit_v1, load_context_value_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_aligned_evaluation import (
    aligned_intervention_attribution_v3, shadow_endpoint_execution_audit_v3, shadow_portfolio_intervention_support_v3,
)
from backend.services.advisory_model_first.economic_entry_evaluation import matched_entry_priorities
from backend.services.advisory_model_first.economic_entry_labels import KEY, _frame
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, read_stage
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_sha256_v1, value_anchor_policy_v1
from backend.services.advisory_model_first.economic_value_anchor_evaluation_v1 import _holding_audit
from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import _positive, value_anchor_shadow_inputs_v1
from backend.services.advisory_model_first.economic_value_anchor_pipeline_v1 import _value_prices
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


def context_actual_decisions_v1(*,fitted,candidates,inputs,prices,references,identity,arm):
    roster=_frame(candidates.loc[:,KEY+['selection_effective_rank']],KEY,set(KEY)|{'selection_effective_rank'})
    ranks=roster.selection_effective_rank
    if (arm not in ARMS or not (roster[KEY[0]]<roster[KEY[1]]).all() or not ranks.between(1,20).all()
            or ranks.mod(1).ne(0).any() or roster.duplicated([KEY[0],'selection_effective_rank']).any()
            or ranks.map(lambda v:isinstance(v,(bool,np.bool_))).any()):
        raise ValueError('context actual roster rank/clock/family differs')
    fields=[*KEY,*D_FEATURES,'feature_visible_through','classification_l2_code','classification_known_from']
    features=_frame(inputs.loc[:,fields],KEY,set(fields))
    if (not pd.to_datetime(features.feature_visible_through).eq(features[KEY[0]]).all()
            or not set(roster[KEY].itertuples(index=False,name=None)).issubset(set(features[KEY].itertuples(index=False,name=None)))):
        raise ValueError('context actual query has a foreign candidate/feature clock')
    known=features.classification_l2_code.notna()
    clock=pd.to_datetime(features.classification_known_from)
    if (known & (clock.isna() | clock.gt(features[KEY[0]]))).any():
        raise ValueError('context actual query sees a future classification')
    quote_fields=['trade_date','instrument','raw_open_cny','suspended','tradability_unknown','up_limit','down_limit','source_sha256','price_coordinate_sha256']
    quotes=_frame(prices.loc[:,quote_fields],quote_fields[:2],set(quote_fields))
    ref_fields=[*KEY,'target_reference_raw_cny','reference_visible_through','source_sha256']
    refs=_frame(references.loc[:,ref_fields],KEY,set(ref_fields))
    if (not quotes.source_sha256.eq(identity.price_source_sha256).all() or not quotes.price_coordinate_sha256.eq(identity.price_coordinate_sha256).all()
            or not refs.source_sha256.eq(identity.reference_source_sha256).all()
            or not pd.to_datetime(refs.reference_visible_through).le(refs[KEY[0]]).all()
            or any(not quotes[name].map(lambda v:type(v) is bool).all() for name in ('suspended','tradability_unknown'))):
        raise ValueError('context observed source/coordinate/clock/trading identity differs')
    feature_map=features.set_index(KEY).to_dict('index')
    quote_map=quotes.set_index(quote_fields[:2]).to_dict('index')
    ref_map=refs.set_index(KEY).to_dict('index')
    output,query,positions=[],[],[]
    for item in roster.to_dict('records'):
        key=tuple(item[name] for name in KEY)
        row={**item,'model_action':'UNAVAILABLE','market_admissible':False,'reason_code':'MARKET_UNPROVEN',
            'actual_gap_bps':None,'expected_net_return_bps':None,'downside_q90_bps':None}
        quote,ref=quote_map.get((key[1],key[2])),ref_map.get(key)
        if quote is not None and ref is not None:
            price,reference,upper,lower=(_positive(quote['raw_open_cny']),_positive(ref['target_reference_raw_cny']),_positive(quote['up_limit']),_positive(quote['down_limit']))
            if None not in (price,upper,lower) and (lower>upper or price<lower-1e-9 or price>upper+1e-9):
                raise ValueError('context quote contradicts legal limits')
            if (not quote['suspended'] and not quote['tradability_unknown'] and None not in (price,reference,upper,lower)
                    and price<upper and not Decimal(str(price))%Decimal('.01')):
                gap=float((Decimal(str(price))/Decimal(str(reference))-1)*10000)
                row.update(market_admissible=True,actual_gap_bps=gap,reason_code='UNKNOWN_CONTEXT_OR_SUPPORT')
                query.append({**feature_map[key],'actual_gap_bps':gap})
                positions.append(len(output))
        output.append(row)
    if query:
        nodes=context_value_nodes_v1(fitted=fitted,rows=pd.DataFrame(query),arm=arm)
        for index,node in zip(positions,nodes.to_dict('records'),strict=True):
            output[index].update(model_action={'ACCEPTABLE':'TAKE','AVOID':'SKIP'}.get(node['status'],'UNAVAILABLE'),
                reason_code=node['status'],expected_net_return_bps=node['expected_net_bps'],downside_q90_bps=node['downside_q90_bps'])
    return pd.DataFrame(output)


def _interval(values):
    values=np.asarray(values,dtype=float)
    if values.ndim!=1 or not len(values) or not np.isfinite(values).all():
        raise ValueError('context inference requires the complete finite paired daily series')
    random=np.random.default_rng(20261004)
    width=min(5,len(values))
    means=[]
    for _ in range(2000):
        starts=random.integers(0,len(values)-width+1,size=int(np.ceil(len(values)/width)))
        means.append(np.concatenate([values[start:start+width] for start in starts])[:len(values)].mean())
    return {'mean_bps':float(values.mean()),'ci95_bps':np.quantile(means,[.025,.975]).tolist(),'standard_error_proxy_bps':float(np.std(means,ddof=1)),
        'block_days':5,'repetitions':2000,'seed':20261004,'evidence_use':'NAVIGATION_ONLY'}


def context_train_noise_proxy_v1(*,rankings,prices,calendar,configuration):
    cutoff=pd.Timestamp(configuration.train_end)
    ranked=rankings.loc[rankings[KEY[0]].between(pd.Timestamp(configuration.train_start),cutoff) & rankings[KEY[1]].le(cutoff)]
    quotes=prices.loc[prices.trade_date.le(cutoff)]
    days=[day for day in calendar if pd.Timestamp(day)<=cutoff]
    market,cash,suspend,clock=value_anchor_shadow_inputs_v1(prices=quotes,calendar=days)
    result=replay_shadow_portfolio(rankings=ranked,daily=market,benchmark_daily=cash,suspend_rows=suspend,
        trading_calendar=clock,policy=value_anchor_policy_v1(),policy_sha256=value_anchor_policy_sha256_v1(),cost_policy=COST,
        request_id='advctxvalue_training_noise_proxy',candidate_decision_dates=sorted(ranked[KEY[0]].unique()))
    if result.daily.empty or _holding_audit(result.episodes,quotes,pd.DatetimeIndex(ranked[KEY[1]].unique())):
        return {'status':'UNPROVEN_TRAINING_MARK','activation_supported':False,'mde_proxy_bps':None}
    values=result.daily.net_return_bps.to_numpy(dtype=float)
    interval=_interval(values)
    return {'status':'MDE_PROXY_NOT_CONFIRMATION_POWER','mde_proxy_bps':2.802*interval['standard_error_proxy_bps'],
        'training_valuation_days':len(values),'visible_through':cutoff.date().isoformat(),'used_candidate_returns':False,
        'test_used':False,'activation_supported':False}


def context_navigation_v1(*,metrics,increments,interventions,take_episodes,decision_days):
    if decision_days<=0:
        raise ValueError('context intervention denominator is empty')
    gates={'net_increment':all(value['mean_bps']>=5 for value in increments.values()),
        'interventions':all(len(value)>=12 and len(value)/decision_days>=.15 for value in interventions.values()),
        'model_takes':take_episodes>=30,
        'mdd':all(metrics['candidate']['max_drawdown']>=metrics[arm]['max_drawdown']-.02 for arm in ('baseline','matched')),
        'tail':all(metrics['candidate']['tail_mean_bps']>=metrics[arm]['tail_mean_bps']-20 for arm in ('baseline','matched'))}
    return {'gates':gates,'navigation':'CONSIDER_CONFIRMATION_DESIGN_ONLY' if all(gates.values()) else 'STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION'}


def evaluate_context_value_v1(*,plan_path,output_root):
    plan,root,registered,source=load_context_value_v1(plan_path=plan_path,output_root=output_root)
    parent,frozen,identity,_,_,_=source
    prepared=read_stage(root/'prepared',stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'])
    trained=read_stage(root/'trained',stage='trained',plan_sha256=plan.plan_sha256,parent_sha256=prepared['stage_sha256'])
    _ledger(plan,root,'TRAINED',root/'trained/manifest.json')
    if (root/'evaluated').exists():
        read_stage(root/'evaluated',stage='evaluated',plan_sha256=plan.plan_sha256,parent_sha256=trained['stage_sha256'])
        _record(plan,root,parent,'EVALUATED',root/'evaluated/manifest.json',generated=1,evaluated=1)
        return root/'evaluated'
    fitted=load_context_value_fit_v1(plan_path=plan_path,output_root=output_root)
    rankings=pd.read_parquet(frozen/'frozen_rankings.parquet')
    rankings=rankings.loc[rankings[KEY[0]].ge(pd.Timestamp(parent.configuration.test_start))]
    candidates=rankings.loc[rankings.is_candidate_decision & rankings.selection_effective_rank.le(20)
        & rankings[KEY[0]].le(pd.Timestamp(parent.configuration.test_end))]
    prices,refs=_value_prices(frozen,parent.configuration),pd.read_parquet(frozen/'references.parquet')
    inputs=pd.read_parquet(root/'prepared/rows.parquet')
    actions={arm:context_actual_decisions_v1(fitted=fitted,candidates=candidates,inputs=inputs,prices=prices,
        references=refs,identity=identity,arm=arm) for arm in ARMS}
    baseline,rule=actions['matched'].copy(),actions['matched'].copy()
    baseline['model_action']='TAKE'
    rule['model_action']=np.where(rule.actual_gap_bps.between(-300,300),'TAKE','SKIP')
    actions={'baseline':baseline,'rule':rule,**actions}
    market,cash,suspend,calendar=value_anchor_shadow_inputs_v1(prices=prices,calendar=json.loads((frozen/'calendar.json').read_text(encoding='utf-8')))
    targets=pd.DatetimeIndex(rankings[KEY[1]].unique()).sort_values()
    dates=sorted(candidates[KEY[0]].unique())
    portfolios,audits,artifacts={},{},{}
    for arm,action in actions.items():
        result=replay_shadow_portfolio(rankings=rankings,daily=market,benchmark_daily=cash,suspend_rows=suspend,
            trading_calendar=calendar,policy=value_anchor_policy_v1(),policy_sha256=value_anchor_policy_sha256_v1(),cost_policy=COST,
            request_id=plan.experiment_id+'_'+arm,candidate_decision_dates=dates,
            entry_priorities=matched_entry_priorities(action.loc[action.market_admissible],arm='model'))
        portfolios[arm]=result
        audits[arm]={'endpoints':shadow_endpoint_execution_audit_v3(result.episodes,prices),'holding':_holding_audit(result.episodes,prices,targets)}
        artifacts[arm+'_daily.parquet'],artifacts[arm+'_episodes.parquet'],artifacts[arm+'_decisions.parquet']=_parquet_bytes(result.daily),_parquet_bytes(result.episodes),_parquet_bytes(action)
    report={'plan_sha256':plan.plan_sha256,'audits':audits,'decision_use':'NAVIGATION_ONLY','deployable':False,
        'economic_effectiveness':'NOT_CONFIRMED','source_evidence':identity.source_evidence,'evidence_limitations':identity.evidence_limitations,
        'sealed_accessed':False,'database_written':False,'real_fill_proven':False,'candidate_rows':len(candidates),'decision_days':len(dates),
        'fitted_head_count':4,'candidate_count':1,'metrics':None,'increments':None,'navigation':'BLOCKED_EXECUTION_OR_MARK_UNPROVEN'}
    if not any(audit['endpoints']['restricted_episode_count'] or audit['holding'] for audit in audits.values()):
        series,metrics={},{ }
        for arm,result in portfolios.items():
            daily=result.daily.set_index('target_trade_date').net_return_bps
            values=daily.reindex(targets)
            if result.metrics['active_episode_count'] or (values.isna() & (values.index<=daily.index.max())).any():
                raise ValueError('context replay has missing or unfinished valuations')
            values=values.fillna(0.)  # Proven terminal empty-cash days only.
            wealth=np.r_[1.,(1+values/10000).cumprod().to_numpy()]
            metrics[arm]={**result.metrics,'common_days':len(values),'common_return':float(wealth[-1]-1),
                'max_drawdown':float((wealth/np.maximum.accumulate(wealth)-1).min()),
                'tail_mean_bps':float(values.nsmallest(max(1,int(np.ceil(.05*len(values))))).mean()),'benchmark':'ZERO_RETURN_CASH_NOT_INDEX'}
            series[arm]=values
        paired=pd.DataFrame(series)
        increments={arm:_interval(paired.candidate-paired[arm]) for arm in ('baseline','matched')}
        entry_targets=set(pd.to_datetime(candidates[KEY[1]]).dt.strftime('%Y-%m-%d'))
        interventions={arm:[day for day in shadow_portfolio_intervention_support_v3(portfolios[arm].episodes,portfolios['candidate'].episodes,targets)['entry_action_difference_days']
            if day in entry_targets] for arm in ('baseline','matched')}
        attribution={arm:aligned_intervention_attribution_v3(actions[arm],portfolios['baseline'].episodes,portfolios[arm].episodes) for arm in ARMS}
        navigation=context_navigation_v1(metrics=metrics,increments=increments,interventions=interventions,
            take_episodes=attribution['candidate']['actual_model_take_episodes'],decision_days=len(dates))
        report.update(metrics=metrics,increments=increments,interventions=interventions,attribution=attribution,**navigation)
        artifacts['paired_daily.parquet']=_parquet_bytes(paired.reset_index())
    artifacts['evaluation.json']=_json_bytes(report)
    path=_publish(study_root=root,stage='evaluated',plan_sha256=plan.plan_sha256,parent_sha256=trained['stage_sha256'],artifacts=artifacts)
    _record(plan,root,parent,'EVALUATED',path/'manifest.json',generated=1,evaluated=1)
    return path
