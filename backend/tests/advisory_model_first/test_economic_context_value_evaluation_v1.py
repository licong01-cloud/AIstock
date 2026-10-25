from types import SimpleNamespace
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_context_value_evaluation_v1 import context_actual_decisions_v1, context_navigation_v1
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage, read_stage
from backend.services.advisory_model_first import economic_context_value_evaluation_v1 as evaluation
from backend.tests.advisory_model_first.test_economic_value_anchor_evaluation_v1 import args
from backend.tests.advisory_model_first.test_economic_context_value_contracts_v1 import plan_fixture

pytest_plugins=['backend.tests.advisory_model_first.test_economic_value_anchor_labels_v1']


def test_only_open_and_D_clock_are_used_market_unknown_is_not_buy(scene,monkeypatch):
    value=args(scene)
    value['inputs']['classification_l2_code']='110100'
    value['inputs']['classification_known_from']=pd.Timestamp('2024-01-01')
    value['fitted']=SimpleNamespace()
    value['arm']='candidate'
    monkeypatch.setattr(evaluation,'context_value_nodes_v1',lambda **kwargs:pd.DataFrame({'status':['ACCEPTABLE']*len(kwargs['rows']),
        'expected_net_bps':100.,'downside_q90_bps':300.}))
    before=context_actual_decisions_v1(**value)
    value['prices'].loc[:,['raw_high_cny','raw_low_cny','raw_close_cny']]=.001
    value['inputs']['gross_value_ratio']=999.
    after=context_actual_decisions_v1(**value)
    pd.testing.assert_frame_equal(before,after)
    assert before.model_action.eq('TAKE').all()
    value['prices'].loc[value['prices'].trade_date.eq(scene['calendar'][1]),'tradability_unknown']=True
    blocked=context_actual_decisions_v1(**value)
    assert not blocked.market_admissible.any()
    value['inputs']['classification_known_from']=value['inputs'][KEY[1]]
    with pytest.raises(ValueError,match='future classification'):
        context_actual_decisions_v1(**value)


def test_lower_drawdown_or_unknown_gains_cannot_replace_net_increment_or_support():
    metrics={arm:{'max_drawdown':-.02,'tail_mean_bps':-50.} for arm in ('baseline','matched','candidate')}
    increments={arm:{'mean_bps':6.} for arm in ('baseline','matched')}
    interventions={arm:list(range(12)) for arm in ('baseline','matched')}
    inputs=dict(metrics=metrics,increments=increments,interventions=interventions,take_episodes=30,decision_days=80)
    assert context_navigation_v1(**inputs)['navigation']=='CONSIDER_CONFIRMATION_DESIGN_ONLY'
    increments['matched']['mean_bps']=0.
    assert context_navigation_v1(**inputs)['navigation'].startswith('STOP_')
    increments['matched']['mean_bps']=6.
    inputs['take_episodes']=0
    assert not context_navigation_v1(**inputs)['gates']['model_takes']


@pytest.mark.parametrize('blocked',[False,True])
def test_four_arm_whole_replay_atomic_and_unknown_contribution_not_model_take(scene,tmp_path,monkeypatch,blocked):
    plan=plan_fixture()
    root,frozen=tmp_path/plan.experiment_id,tmp_path/'frozen'
    frozen.mkdir()
    rankings=scene['rankings'].copy()
    rankings['is_candidate_decision']=rankings.decision_as_of_trade_date.eq(scene['calendar'][0])
    if blocked:
        scene['prices'].loc[scene['prices'].trade_date.eq(scene['calendar'][2]),'tradability_unknown']=True
    for name,frame in (('frozen_rankings',rankings),('prices',scene['prices']),('references',scene['references'])):
        (frozen/(name+'.parquet')).write_bytes(_parquet_bytes(frame))
    (frozen/'calendar.json').write_bytes(_json_bytes([day.isoformat() for day in scene['calendar']]))
    registered_path=publish_stage(study_root=root,stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None,artifacts={'plan.json':_json_bytes(plan.model_dump(mode='json'))})
    registered=read_stage(registered_path,stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None)
    value=args(scene)
    value['inputs']['classification_l2_code']=None
    value['inputs']['classification_known_from']=pd.NaT
    prepared_path=publish_stage(study_root=root,stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'],
        artifacts={'rows.parquet':_parquet_bytes(value['inputs'])})
    prepared=read_stage(prepared_path,stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=registered['stage_sha256'])
    publish_stage(study_root=root,stage='trained',plan_sha256=plan.plan_sha256,parent_sha256=prepared['stage_sha256'],
        artifacts={'unit.json':_json_bytes({'synthetic_not_real_model':True})})
    parent=SimpleNamespace(configuration=SimpleNamespace(train_start=scene['calendar'][0],label_cutoff=scene['calendar'][-1],
        test_start=scene['calendar'][0],test_end=scene['calendar'][0]))
    monkeypatch.setattr(evaluation,'load_context_value_v1',lambda **kwargs:(plan,root,registered,(parent,frozen,scene['parent_identity'],None,None,None)))
    monkeypatch.setattr(evaluation,'load_context_value_fit_v1',lambda **kwargs:SimpleNamespace())
    monkeypatch.setattr(evaluation,'_ledger',lambda *a:None)
    monkeypatch.setattr(evaluation,'_record',lambda *a,**k:None)
    monkeypatch.setattr(evaluation,'context_value_nodes_v1',lambda **kwargs:pd.DataFrame({'status':['UNKNOWN_CONTEXT_OR_SUPPORT']*len(kwargs['rows']),
        'expected_net_bps':None,'downside_q90_bps':None}))
    output=evaluation.evaluate_context_value_v1(plan_path=registered_path/'plan.json',output_root=tmp_path)
    report=json.loads((output/'evaluation.json').read_text(encoding='utf-8'))
    assert not report['deployable'] and not report['sealed_accessed']
    if blocked:
        assert report['navigation']=='BLOCKED_EXECUTION_OR_MARK_UNPROVEN' and report['metrics'] is None
    else:
        assert len(report['metrics'])==4 and report['increments']['matched']['mean_bps']==0
        assert report['attribution']['candidate']['actual_model_take_episodes']==0
        assert report['attribution']['candidate']['research_unknown_control_episodes']>0
        assert report['navigation'].startswith('STOP_')
