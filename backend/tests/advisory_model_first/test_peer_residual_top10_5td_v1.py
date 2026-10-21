"""Small mathematical and causal fixtures, not research or economic evidence."""
from copy import deepcopy
import hashlib
import json
import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import peer_residual_top10_5td_v1 as core
from backend.services.advisory_model_first import peer_residual_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def panels():
    days = pd.bdate_range('2024-07-01', periods=70)
    symbols = [f'{i:06}.SZ' for i in range(50)]
    t, k = np.arange(69)[:,None], np.arange(50)[None,:]
    r = 20*np.sin(t*.63)*(1+k/50)+5*np.cos(t*1.37+k)
    p = pd.DataFrame(10*np.exp(np.vstack([np.zeros((1,50)), np.cumsum(r,axis=0)])/10000), index=days,columns=symbols)
    return p, p*0+1, days


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


def test_leave_one_out_formula_signal_separation_and_unit_invariance():
    p,f,cal = panels()
    day = cal[65]
    a = core.residual_features(p,f,day=day,calendar=cal)
    r = np.diff(np.log(p.iloc[:66].to_numpy()),axis=0)*10000
    x,y = r[:60,1:].mean(axis=1),r[:60,0]
    beta = np.cov(y,x,ddof=0)[0,1]/np.var(x)
    alpha = y.mean()-beta*x.mean()
    expected = -(r[-5:,0].sum()-5*alpha-beta*r[-5:,1:].mean(axis=1).sum())/(np.sqrt(5)*np.std(y-alpha-beta*x))
    assert a.iloc[0][core.NEW] == pytest.approx(expected)
    scaled = core.residual_features(p*1000,f*7,day=day,calendar=cal)
    np.testing.assert_allclose(a[list(core.FEATURES[-6:])],scaled[list(core.FEATURES[-6:])],rtol=1e-9,atol=1e-9)
    q = p.copy()
    q.iloc[65,0] *= .99
    changed = core.residual_features(q,f,day=day,calendar=cal)
    assert changed.iloc[0].peer_beta60 == a.iloc[0].peer_beta60
    assert changed.iloc[0].peer_log_return5_bps == a.iloc[0].peer_log_return5_bps
    assert changed.iloc[0][core.NEW] > a.iloc[0][core.NEW]


@pytest.mark.parametrize('missing_peers,available',[(9,True),(10,False)])
def test_peer_coverage_is_observed_not_imputed(missing_peers,available):
    p,f,cal = panels()
    p.iloc[65,1:1+missing_peers] = np.nan
    a = core.residual_features(p,f,day=cal[65],calendar=cal)
    assert (a.iloc[0].peer_reason == 'AVAILABLE') == available
    assert a.iloc[0].peer_signal_min_peers == 49-missing_peers
    assert len(a) == 50


@pytest.mark.parametrize('missing_closes,expected_pairs,reason',[
    (12,48,'AVAILABLE'),(13,47,'INSUFFICIENT_HISTORY')])
def test_history_pair_support_boundary(missing_closes,expected_pairs,reason):
    p,f,cal=panels()
    p.iloc[:missing_closes,0]=np.nan
    result=core.residual_features(p,f,day=cal[65],calendar=cal)
    assert result.iloc[0].peer_history_pairs==expected_pairs
    assert result.iloc[0].peer_reason==reason
    assert len(result)==50
    assert np.isfinite(result.iloc[0][core.NEW])==(reason=='AVAILABLE')


def test_future_poison_missing_calendar_and_zero_variance():
    p,f,cal = panels()
    day=cal[65]
    expected=core.residual_features(p,f,day=day,calendar=cal)
    p.iloc[66:]=np.inf
    f.iloc[66:]=-1
    pd.testing.assert_frame_equal(expected,core.residual_features(p,f,day=day,calendar=cal))
    with pytest.raises(ValueError,match='calendar'):
        core.residual_features(p.drop(index=cal[20]),f.drop(index=cal[20]),day=day,calendar=cal)
    p.iloc[:66]=10
    f.iloc[:66]=1
    assert set(core.residual_features(p,f,day=day,calendar=cal).peer_reason)=={'ZERO_PEER_VARIANCE'}
    p.iloc[10,0]=0
    with pytest.raises(ValueError,match='known peer'):
        core.residual_features(p,f,day=day,calendar=cal)


def test_shared_training_support_json_prediction_unknown_slots_and_future_labels():
    rows=population()
    cutoff='2025-03-31'
    model=core.fit(rows,cutoff,'candidate')
    bad=rows.copy()
    bad.loc[bad.decision_as_of_trade_date.gt(cutoff),'hypothetical_liquidation_net_bps']=np.inf
    assert core.fit(bad,cutoff,'candidate')==model
    other=core.fit(rows,cutoff,'matched')
    assert other['training_keys_sha256']==model['training_keys_sha256']
    rows.loc[rows.selection_effective_rank.eq(1),core.NEW]=np.nan
    a=core.predict(rows,json.loads(json.dumps(model)))
    b=core.predict(rows,other)
    assert a.no_rank_advice.equals(b.no_rank_advice)
    assert a.loc[a.no_rank_advice,'advisory_rank'].eq(1).all()
    assert a.groupby(core.GROUP).selected.sum().eq(10).all()
    assert a[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])
    fake=deepcopy(model)
    fake['contract_sha256']='bad'
    with pytest.raises(ValueError,match='identity'):
        core.predict(rows,fake)


def test_finance_cutoff_claim_identity_and_native_cost(tmp_path):
    rows=population()
    packet=core.fit(rows,'2025-03-31','candidate')
    predictions=core.predict(rows,packet)
    result=core.panel(predictions,predictions,rows[[*core.KEY,*FINANCE]])
    assert (result.candidate-result.candidate_full_model).eq(0).all()
    study=object.__new__(pipe.Study)
    study.root=tmp_path
    study.digest='a'*64
    study.budget=lambda:None
    study.claim('fold1_candidate')
    study.claim('fold1_candidate','b'*64)
    study.claim('fold1_candidate','b'*64)
    with pytest.raises(ValueError,match='identity'):
        study.claim('fold1_candidate','c'*64)
    with pytest.raises(ValueError,match='unregistered'):
        study.claim('other_candidate')
    with pytest.raises(ValueError,match='window'):
        study.finance('2026-08-31','2026-09-01',cutoff='2026-09-01')


def test_recovery_detects_raw_input_drift(tmp_path,monkeypatch):
    raw=tmp_path/'close.bin'
    raw.write_bytes(np.array([0,10,11],dtype='<f4').tobytes())
    original=pipe.stamp(raw)
    dataset=SimpleNamespace(root=tmp_path,identity='hash',_bin_lease={'close.bin':original},
        assert_unchanged=lambda:None,close=lambda:None)
    monkeypatch.setattr(pipe,'file_dataset',lambda plan:dataset)
    study=SimpleNamespace(source=SimpleNamespace(plan={}))
    receipt=dict(binding_hash='hash',pins=[dict(path='close.bin',stat=list(original),start_index=0,end_index=1,
        bin_offset=0,consumed_sha256=hashlib.sha256(raw.read_bytes()).hexdigest())])
    pipe.verify_input_files(study,receipt)
    raw.write_bytes(np.array([0,10,12],dtype='<f4').tobytes())
    os.utime(raw,ns=(original[1],original[1]))
    with pytest.raises(ValueError,match='content drift'):
        pipe.verify_input_files(study,receipt)
    raw.write_bytes(b'drifted')
    with pytest.raises(ValueError,match='drift'):
        pipe.verify_input_files(study,receipt)


def test_perfect_shared_return_does_not_create_huge_fake_pressure():
    p,f,cal=panels()
    p.loc[:,:]=np.tile(p.iloc[:,0].to_numpy()[:,None],(1,50))
    result=core.residual_features(p,f,day=cal[65],calendar=cal)
    assert result[core.NEW].isna().all()
    assert set(result.peer_reason)=={'ZERO_RESIDUAL_VARIANCE'}


def test_attach_preserves_original_context_and_stages_detect_tampering(tmp_path):
    p,f,cal=panels()
    rows=population().iloc[:100].copy()
    rows[core.base.KEY[0]]=cal[65]
    rows['target_trade_date']=cal[66]
    rows=rows.drop(columns=[*core.COMPONENTS,core.NEW])
    attached=core.attach(rows,p,f,cal)
    assert len(attached)==100 and attached[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])
    assert attached.peer_reason.eq('AVAILABLE').all()
    study=object.__new__(pipe.Study)
    study.root=tmp_path
    study.digest='a'*64
    study.budget=lambda:None
    digest=study.publish('fold1_candidate/trained',None,{'model.json':b'{"coef":[1]}'})
    assert study.read('fold1_candidate/trained',None)==digest
    (tmp_path/'fold1_candidate/trained/model.json').write_bytes(b'{"coef":[2]}')
    with pytest.raises(AdvisoryModelFirstError,match='hash mismatch'):
        study.read('fold1_candidate/trained',None)


@pytest.mark.parametrize('values,expected',[((1.,2.,-1.),True),((1.,0.,-1.),False),((-1.,-2.,-3.),False)])
def test_reversal_direction_is_frozen_not_flipped(values,expected):
    assert core.direction_supported(dict(zip(('1','2','3'),values)))==expected
