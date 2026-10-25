"""Direct causal, numerical and recovery contracts; not profitability evidence."""
import hashlib
import json
import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import peer_lead_lag_top10_5td_v1 as core
from backend.services.advisory_model_first import peer_lead_lag_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def panels():
    cal = pd.bdate_range('2024-07-01', periods=70)
    t, k = np.arange(69)[:,None], np.arange(50)[None,:]
    r = 20*np.sin(t*.63)*(1+k/50)+5*np.cos(t*1.37+k)
    p = pd.DataFrame(10*np.exp(np.vstack([np.zeros((1,50)),np.cumsum(r,axis=0)])/10000),
        index=cal,columns=[f'{i:06}.SZ' for i in range(50)])
    return p,p*0+1,cal


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


def test_conditional_ols_leave_one_out_cutoff_and_units():
    p,f,cal = panels()
    a = core.lag_features(p,f,day=cal[66],calendar=cal)
    r = np.diff(np.log(p.iloc[:67].to_numpy()),axis=0)*10000
    x = np.column_stack((np.ones(60),r[:60,0],r[:60,1:].mean(axis=1)))
    coef = np.linalg.lstsq(x,r[1:61,0],rcond=None)[0]
    np.testing.assert_allclose(a.iloc[0][['lag_alpha60_bps','lag_own_phi60','lag_peer_gamma60']].to_numpy(float),coef,atol=1e-8)
    assert a.iloc[0][core.NEW] == pytest.approx(coef[2]*r[-1,1:].mean())
    scaled = core.lag_features(p*1000,f*7,day=cal[66],calendar=cal)
    np.testing.assert_allclose(a[list(core.FEATURES[-8:])],scaled[list(core.FEATURES[-8:])],atol=1e-7)
    p.iloc[62:67,0] *= .99
    changed = core.lag_features(p,f,day=cal[66],calendar=cal)
    np.testing.assert_array_equal(a.iloc[0][['lag_alpha60_bps','lag_own_phi60','lag_peer_gamma60']],
        changed.iloc[0][['lag_alpha60_bps','lag_own_phi60','lag_peer_gamma60']])
    assert changed.iloc[0].lag_peer_return1_bps == a.iloc[0].lag_peer_return1_bps


@pytest.mark.parametrize('missing,reason,count',[(12,'AVAILABLE',48),(13,'INSUFFICIENT_HISTORY',47)])
def test_history_support(missing,reason,count):
    p,f,cal = panels()
    p.iloc[:missing,0] = np.nan
    a = core.lag_features(p,f,day=cal[66],calendar=cal)
    assert (a.iloc[0].lag_reason,a.iloc[0].lag_history_pairs) == (reason,count)
    assert len(a) == 50


@pytest.mark.parametrize('missing,available',[(9,True),(10,False)])
def test_latest_peer_support(missing,available):
    p,f,cal = panels()
    p.iloc[66,1:1+missing] = np.nan
    a = core.lag_features(p,f,day=cal[66],calendar=cal)
    assert (a.iloc[0].lag_reason == 'AVAILABLE') == available
    assert a.iloc[0].lag_latest_peers == 49-missing


def test_future_is_not_decoded_and_known_bad_values_fail():
    p,f,cal = panels()
    expected = core.lag_features(p,f,day=cal[66],calendar=cal)
    p.iloc[67:],f.iloc[67:] = np.inf,-1
    pd.testing.assert_frame_equal(expected,core.lag_features(p,f,day=cal[66],calendar=cal))
    with pytest.raises(ValueError,match='calendar'):
        core.lag_features(p.drop(index=cal[2]),f.drop(index=cal[2]),day=cal[66],calendar=cal)
    p.iloc[:67] = 10
    assert set(core.lag_features(p,f,day=cal[66],calendar=cal).lag_reason) == {'DEGENERATE_CALIBRATION'}
    p.iloc[5,0] = 0
    with pytest.raises(ValueError,match='known lag'):
        core.lag_features(p,f,day=cal[66],calendar=cal)


def test_perfect_conditional_fit_is_not_rejected_as_zero_sigma():
    p,f,cal = panels()
    rng = np.random.default_rng(6)
    peer = rng.normal(size=69)
    own = np.zeros(69)
    for t in range(1,69):
        own[t] = 1+.2*own[t-1]+.7*peer[t-1]
    r = np.tile(peer[:,None],(1,50))
    r[:,0] = own
    p.iloc[:] = 10*np.exp(np.vstack([np.zeros((1,50)),np.cumsum(r,axis=0)])/10000)
    a = core.lag_features(p,f,day=cal[66],calendar=cal)
    assert a.iloc[0].lag_reason == 'AVAILABLE' and a.iloc[0].lag_sigma60_bps < 1e-7
    assert a.iloc[0].lag_peer_gamma60 == pytest.approx(.7,abs=1e-8)


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


def test_attach_and_panel_preserve_population_and_atomic_stage(tmp_path):
    p,f,cal = panels()
    rows = population().iloc[:100].drop(columns=[*core.COMPONENTS,core.NEW]).copy()
    rows[core.base.KEY[0]],rows['target_trade_date'] = cal[66],cal[67]
    attached = core.attach(rows,p,f,cal)
    assert attached[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])
    assert attached.lag_reason.eq('AVAILABLE').all()
    packet = core.fit(population(),'2025-03-31','candidate')
    prediction = core.predict(population(),packet)
    result = core.panel(prediction,prediction,population()[[*core.KEY,*FINANCE]])
    assert (result.candidate-result.candidate_full_model).eq(0).all()
    study = object.__new__(pipe.Study)
    study.root,study.digest,study.budget = tmp_path,'a'*64,lambda:None
    digest = study.publish('fold1_candidate/trained',None,{'model.json':b'{}'})
    assert study.read('fold1_candidate/trained',None) == digest
    (tmp_path/'fold1_candidate/trained/model.json').write_bytes(b'{"changed":true}')
    with pytest.raises(AdvisoryModelFirstError,match='hash mismatch'):
        study.read('fold1_candidate/trained',None)


def test_native_product_endpoint_is_not_a_matched_or_sign_gate():
    rows = []
    for i,d in enumerate(pd.bdate_range('2025-04-01',periods=60)):
        for pkg in ('a','b'):
            rows.append(dict(package_id=pkg,decision_date=d,fold=i//20+1,mature=True,original_candidates=50,
                candidate=30.,baseline=10.,candidate_full_model=50.,delta=20.,intervention_count=1,
                matched_intervention_count=1,candidate_unknown=0,baseline_unknown=0))
    cohort = pd.DataFrame(rows)
    report,_ = core.summarize(cohort)
    assert report['proceed_outer'] and report['classification'] == 'INNER_SUPPORTED'
    assert report['versus_matched']['mean_bps'] < 0
    cohort.candidate = 0.
    assert core.summarize(cohort)[0]['classification'] == 'INNER_NOT_SUPPORTED'
    cohort.candidate,cohort.intervention_count = 30.,0
    assert core.summarize(cohort)[0]['classification'] == 'INNER_INCONCLUSIVE'


def test_range_extension_is_not_a_new_roster(monkeypatch):
    _,_,cal = panels()
    inputs = population().iloc[:100].copy()
    inputs[core.base.KEY[0]] = cal[66]
    original = inputs.copy(deep=True)
    monkeypatch.setattr(pipe.shared,'file_dataset',lambda plan:SimpleNamespace(calendar=cal,close=lambda:None))
    def reader(plan,query,budget):
        assert cal.get_loc(query[core.base.KEY[0]].min())-65 == 0
        assert query[core.base.KEY[0]].max() == cal[66]
        assert set(query.instrument) == set(inputs.instrument)
        return 'native-reader'
    monkeypatch.setattr(pipe.shared,'read_panel',reader)
    assert pipe.read_panel({},inputs,lambda:None) == 'native-reader'
    pd.testing.assert_frame_equal(inputs,original)


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


def test_recovery_checks_content_even_when_stat_lease_matches(tmp_path,monkeypatch):
    raw = tmp_path/'close.bin'
    raw.write_bytes(np.array([0,10,11],dtype='<f4').tobytes())
    stat = pipe.shared.stamp(raw)
    dataset = SimpleNamespace(root=tmp_path,identity='hash',_bin_lease={'close.bin':stat},assert_unchanged=lambda:None,close=lambda:None)
    monkeypatch.setattr(pipe.shared,'file_dataset',lambda plan:dataset)
    receipt = dict(binding_hash='hash',pins=[dict(path='close.bin',stat=list(stat),start_index=0,end_index=1,
        bin_offset=0,consumed_sha256=hashlib.sha256(raw.read_bytes()).hexdigest())])
    study = SimpleNamespace(source=SimpleNamespace(plan={}))
    pipe.shared.verify_input_files(study,receipt)
    raw.write_bytes(np.array([0,10,12],dtype='<f4').tobytes())
    os.utime(raw,ns=(stat[1],stat[1]))
    with pytest.raises(ValueError,match='content drift'):
        pipe.shared.verify_input_files(study,receipt)


def test_code_identity_ignores_newlines_not_source_edits(tmp_path,monkeypatch):
    monkeypatch.setattr(pipe,'__file__',str(tmp_path/'pipe.py'))
    monkeypatch.setattr(pipe.shared,'implementation_sha256',lambda:'a'*64)
    names = ('peer_lead_lag_top10_5td_v1','peer_lead_lag_top10_5td_pipeline_v1')
    content = b"literal = b'\\r\\n'\nx = 1\n"
    for name in names:
        (tmp_path/(name+'.py')).write_bytes(content)
    original = pipe.implementation_sha256()
    for name in names:
        (tmp_path/(name+'.py')).write_bytes(content.replace(b'\n',b'\r\n'))
    assert pipe.implementation_sha256() == original
    (tmp_path/(names[0]+'.py')).write_bytes(content.replace(b'x = 1',b'x = 2'))
    assert pipe.implementation_sha256() != original
