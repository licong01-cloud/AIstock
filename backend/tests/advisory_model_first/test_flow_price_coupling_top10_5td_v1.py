"""Direct contracts for the fixed covariance and bounded six-fit study."""
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import flow_price_coupling_top10_5td_v1 as core
from backend.services.advisory_model_first import flow_price_coupling_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first import flow_price_coupling_top10_5td_inputs_v1 as inp
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def population():
    rows = []
    for d in pd.to_datetime(['2024-11-01','2024-11-04','2025-04-01']):
        for pkg in ('a','b'):
            for rank in range(1,51):
                y = float(100-rank)
                rows.append(dict(package_id=pkg,manifest_sha256='a'*64,run_id='run_'+pkg,source_id='source',
                    decision_as_of_trade_date=d,target_trade_date=d+pd.offsets.BDay(1),instrument=f'{rank-1:06}.SZ',
                    selection_effective_rank=rank,candidate_group_size=50,parent_score=-rank,
                    **{n:np.sin(rank*(i+1)/50) for i,n in enumerate(core.FEATURES)},
                    label_information_end=d+pd.offsets.BDay(5),valuation_status='AVAILABLE',
                    hypothetical_liquidation_net_bps=y,observed_gap_bps=0.,
                    valuation_gross_terminal_ratio=(1+y/10000)*(1+POLICY['buy_bps']/10000)/(1-POLICY['sell_bps']/10000),
                    exit_execution_status='NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF',valuation_policy_sha256=core.base.VALUATION_POLICY_SHA256))
    return pd.DataFrame(rows)

def measurement_inputs(flow=(.1, .2, .4, .3, .5)):
    money = np.zeros((5, 8), float)
    money[:, 0] = 100
    money[:, 4], money[:, 5] = 50*(1+np.array(flow)), 50*(1-np.array(flow))
    prices = np.column_stack((np.ones(5)*10, 10*np.exp(np.array([-200,-100,100,200,0])/10000)))
    return money, prices


def test_fixed_measurement_non_equivalence_units_and_constants():
    a,p = measurement_inputs()
    b,_ = measurement_inputs((.1,.3,.2,.4,.5))
    fa,fb = inp.measure(a,p),inp.measure(b,p)
    np.testing.assert_allclose([fa[k] for k in inp.CONTROL], [fb[k] for k in inp.CONTROL], atol=1e-10)
    np.testing.assert_allclose([fa[inp.NEW],fb[inp.NEW]], [12,10], atol=1e-10)
    for money,prices in ((a*10000,p), (a,p*np.arange(1,6)[:,None])):
        np.testing.assert_allclose([fa[k] for k in inp.FIELDS],
            [inp.measure(money,prices)[k] for k in inp.FIELDS], atol=1e-9)
    constant,_ = measurement_inputs((.2,)*5)
    assert inp.measure(constant,p)[inp.NEW] == pytest.approx(0,abs=1e-12)
    assert inp.measure(a,np.ones_like(p)*10)[inp.NEW] == 0
    assert len(core.MATCHED) == 19 and len(core.FEATURES) == 20


def test_unknown_is_common_and_bad_known_values_fail():
    m,p = measurement_inputs()
    m[2,0] = np.nan
    result = inp.measure(m,p)
    assert result['fp_reason'] == 'UNKNOWN_MISSING_REQUIRED_SOURCE'
    assert all(np.isnan(result[k]) for k in inp.FIELDS)
    assert inp.measure(np.zeros((5,8)),p)['fp_reason'] == 'UNKNOWN_ZERO_LARGE_FLOW_MASS'
    for value in (-1., np.inf):
        m,_ = measurement_inputs()
        m[0,0] = value
        with pytest.raises(ValueError,match='invalid known'):
            inp.measure(m,p)
    m,p = measurement_inputs()
    for prices in (np.zeros_like(p), np.full_like(p,np.inf)):
        with pytest.raises(ValueError,match='invalid known'):
            inp.measure(m,prices)
    for money in (m.astype(bool),m.astype(str)):
        with pytest.raises(ValueError,match='non-numeric'):
            inp.measure(money,p)


def test_attach_projects_future_preserves_missing_session_and_population():
    cal = pd.bdate_range('2024-10-28',periods=7)
    rows = population().iloc[:100].drop(columns=list(core.FEATURES[8:])).copy()
    rows[core.base.KEY[0]],rows.target_trade_date = cal[4],cal[5]
    m,p = measurement_inputs()
    members = sorted(rows.instrument.unique())
    idx = pd.MultiIndex.from_product([cal[:5],members],names=['datetime','instrument'])
    frames = {name:pd.DataFrame(np.repeat(values,len(members),axis=0),index=idx,columns=inp.COLUMNS[name])
        for name,values in (('moneyflow',m),('daily_pv',p))}
    expected = inp.attach(rows,frames,cal)
    for name,frame in frames.items():
        future_index = pd.MultiIndex.from_product([cal[5:],members],names=idx.names)
        frames[name] = pd.concat([frame,pd.DataFrame(np.inf,index=future_index,columns=frame.columns)])
    pd.testing.assert_frame_equal(expected,inp.attach(rows,frames,cal))
    frames['moneyflow'] = frames['moneyflow'].drop(index=(cal[1],members[0]))
    actual = inp.attach(rows,frames,cal)
    assert actual.loc[actual.instrument.eq(members[0]),'fp_reason'].eq('UNKNOWN_MISSING_REQUIRED_SOURCE').all()
    assert actual[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])


def test_h5_projection_budget_and_content_receipt(tmp_path,monkeypatch):
    cal = pd.bdate_range('2024-10-28',periods=7)
    calendar_path = tmp_path/'day.txt'
    calendar_path.write_text('\n'.join(str(d.date()) for d in cal))
    paths = dict(day_calendar=calendar_path)
    idx = pd.MultiIndex.from_product([cal,['000001.SZ','000002.SZ']],names=['datetime','instrument'])
    for name,cols in inp.COLUMNS.items():
        paths[name] = tmp_path/(name+'.h5')
        frame = pd.DataFrame(1.,index=idx,columns=[*cols,'forbidden'])
        frame['forbidden'] = np.inf
        frame.loc[(cal[5:],slice(None)),cols] = np.inf
        frame.to_hdf(paths[name],key='data',format='table',data_columns=['datetime','instrument'])
    stamps = {name:list(inp.stamp(p)) for name,p in paths.items()}
    monkeypatch.setattr(inp,'sources',lambda plan,expected:(paths,{},stamps))
    rows = pd.DataFrame(dict(decision_as_of_trade_date=[cal[4]],instrument=['000001.SZ']))
    frames,_,receipt = inp.read_panel({'binding':{'binding_hash':'fixed'}},rows,stamps,lambda:None)
    assert receipt['consumed_numeric_cells'] == 50
    assert all(len(f) == 5 and np.isfinite(f.to_numpy()).all() for f in frames.values())
    first_hash = receipt['reads'][0]['slice_sha256']
    frame = pd.read_hdf(paths['moneyflow'])
    frame.iloc[0,0] = 2.
    frame.to_hdf(paths['moneyflow'],key='data',mode='w',format='table',data_columns=['datetime','instrument'])
    stamps['moneyflow'] = list(inp.stamp(paths['moneyflow']))
    assert inp.read_panel({'binding':{'binding_hash':'fixed'}},rows,stamps,lambda:None)[2]['reads'][0]['slice_sha256'] != first_hash
    monkeypatch.setattr(inp,'MAX_CELLS',49)
    with pytest.raises(ValueError,match='budget'):
        inp.read_panel({'binding':{'binding_hash':'fixed'}},rows,stamps,lambda:None)


def test_training_maturity_common_support_json_slots_and_original_identity():
    rows = population()
    a = core.fit(rows,'2025-03-31','candidate')
    bad = rows.copy()
    # Both future D and immature outcomes on a past D must be projected first.
    bad.loc[bad.decision_as_of_trade_date.gt('2025-03-31'),'hypothetical_liquidation_net_bps'] = np.inf
    assert core.fit(bad,'2025-03-31','candidate') == a
    immature = bad.selection_effective_rank.eq(1) & bad.decision_as_of_trade_date.eq('2024-11-01')
    bad.loc[immature,'label_information_end'] = pd.Timestamp('2025-04-02')
    bad.loc[immature,'hypothetical_liquidation_net_bps'] = np.inf
    assert core.fit(bad,'2025-03-31','candidate') == core.fit(rows.loc[~immature],'2025-03-31','candidate')
    b = core.fit(rows,'2025-03-31','matched')
    assert a['training_keys_sha256'] == b['training_keys_sha256']
    rows.loc[rows.selection_effective_rank.eq(1),core.NEW] = np.nan
    prediction = core.predict(rows,json.loads(json.dumps(a)))
    assert prediction.no_rank_advice.equals(core.predict(rows,b).no_rank_advice)
    assert prediction.loc[prediction.no_rank_advice,'advisory_rank'].eq(1).all()
    assert prediction.groupby(core.GROUP).selected.sum().eq(10).all()
    assert prediction[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])
    a['contract_sha256'] = 'bad'
    with pytest.raises(ValueError,match='identity'):
        core.predict(rows,a)

def test_native_product_endpoint_is_not_a_matched_or_sign_gate():
    rows = []
    for i,d in enumerate(pd.bdate_range('2025-04-01',periods=60)):
        for pkg in ('a','b'):
            rows.append(dict(package_id=pkg,decision_date=d,fold=i//20+1,mature=True,original_candidates=50,
                candidate=30.,baseline=10.,candidate_full_model=50.,delta=20.,intervention_count=1,
                matched_intervention_count=1,candidate_unknown=0,baseline_unknown=0))
    cohort = pd.DataFrame(rows)
    report,_ = core.summarize(cohort)
    assert report['supports_future_confirmation_design'] and not report['proceed_outer'] and report['classification'] == 'INNER_SUPPORTED'
    assert report['versus_matched']['mean_bps'] < 0
    cohort.candidate = 0.
    assert core.summarize(cohort)[0]['classification'] == 'INNER_NOT_SUPPORTED'
    cohort.candidate,cohort.intervention_count = 30.,0
    assert core.summarize(cohort)[0]['classification'] == 'INNER_INCONCLUSIVE'

def test_claim_clock_and_interrupted_fit_are_not_silently_retried(tmp_path):
    study = object.__new__(pipe.Study)
    study.root,study.digest,study.budget = tmp_path,'a'*64,lambda:None
    study.claim('fold1_matched')
    with pytest.raises(ValueError,match='unpublished fit'):
        pipe.fit_pair(study,None,None,None,prefix='fold1',cutoff='2025-03-31',first='2025-04-01',last='2025-05-30',progress=lambda x:None)
    study.claim('fold1_matched','b'*64)
    with pytest.raises(ValueError,match='identity'):
        study.claim('fold1_matched','c'*64)
    with pytest.raises(ValueError,match='window'):
        study.finance('2026-08-31','2026-09-01',cutoff='2026-09-01')

def test_both_predictions_precede_evaluation_and_retry_does_not_refit(tmp_path,monkeypatch):
    rows = population()
    study = object.__new__(pipe.Study)
    study.root,study.digest,study.budget = tmp_path,'a'*64,lambda:None
    calls = []
    def finance(first,last,*,cutoff,outer=False):
        calls.append((first,last))
        if first == '2025-04-01':
            assert all((tmp_path/f'fold1_{a}/trained/manifest.json').exists() for a in ('matched','candidate'))
        return rows.loc[rows.decision_as_of_trade_date.between(pd.Timestamp(first),pd.Timestamp(last)),[*core.KEY,*FINANCE]]
    study.finance = finance
    kwargs = dict(prefix='fold1',cutoff='2025-03-31',first='2025-04-01',last='2025-05-30',progress=lambda e:None)
    inputs = rows.drop(columns=list(FINANCE))
    targets = inputs.loc[inputs.decision_as_of_trade_date.eq('2025-04-01')].reset_index(drop=True)
    cohort,parent = pipe.fit_pair(study,None,inputs,targets,**kwargs)
    assert len(cohort) == 2 and len(calls) == 2
    monkeypatch.setattr(core,'fit',lambda *a,**k:pytest.fail('exact retry attempted refit'))
    restored,digest = pipe.fit_pair(study,None,inputs,targets,evaluate=False,**kwargs)
    assert restored is None and digest == parent and len(calls) == 2

def test_exact_six_claims_and_no_outer_or_final(tmp_path):
    study = object.__new__(pipe.Study)
    study.root,study.digest,study.budget = tmp_path,'a'*64,lambda:None
    for fold in range(1,4):
        for arm in ('matched','candidate'):
            study.claim(f'fold{fold}_{arm}')
    assert len(list((tmp_path/'fit_claims').glob('*started.json'))) == 6
    with pytest.raises(ValueError,match='budget'):
        study.claim('fold1_matched')
    with pytest.raises(ValueError,match='unregistered'):
        study.claim('final_candidate')
    for options in (dict(first='2025-10-01',last='2025-10-02',cutoff='2025-10-02'),
            dict(first='2025-04-01',last='2025-05-30',cutoff='2025-05-30',outer=True)):
        with pytest.raises(ValueError,match='window'):
            study.finance(**options)
    with pytest.raises(ValueError,match='unregistered fold'):
        pipe.fit_pair(study,None,None,None,prefix='final',cutoff='2025-09-30',
            first='2025-10-01',last='2026-01-01',progress=lambda e:None)


def test_finance_values_project_maturity_before_decode(tmp_path,monkeypatch):
    study = object.__new__(pipe.Study)
    study.source = SimpleNamespace(root=tmp_path)
    rows = population()
    calls = []
    def read(path,columns=None,filters=None):
        assert path == tmp_path/'prepared/train_finance.parquet'
        calls.append((columns,filters))
        selected = rows.loc[rows.decision_as_of_trade_date.le('2025-03-31')]
        if columns:
            assert columns == [*core.KEY,'label_information_end']
            return selected[columns]
        assert filters[-1] == ('label_information_end','<=',pd.Timestamp('2025-03-31'))
        return selected[[*core.KEY,*FINANCE]]
    monkeypatch.setattr(pd,'read_parquet',read)
    study.finance('2024-07-04','2025-03-31',cutoff='2025-03-31')
    assert len(calls) == 2


def test_immutable_stage_rejects_tamper(tmp_path):
    study = object.__new__(pipe.Study)
    study.root,study.digest,study.budget = tmp_path,'a'*64,lambda:None
    digest = study.publish('fold1_candidate/trained',None,{'model.json':b'{}'})
    assert study.read('fold1_candidate/trained',None) == digest
    (tmp_path/'fold1_candidate/trained/model.json').write_bytes(b'{"changed":true}')
    with pytest.raises(AdvisoryModelFirstError,match='hash mismatch'):
        study.read('fold1_candidate/trained',None)
