"""Direct share-endpoint, clock, ranking and six-fit research contracts."""
import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import float_share_change_top10_5td_v1 as core
from backend.services.advisory_model_first import float_share_change_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first import float_share_change_top10_5td_inputs_v1 as inp
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


def test_share_change_is_not_current_fraction_or_proportional_split():
    a=np.array([[100.,40.],[100.,50.]])
    b=np.array([[100.,50.],[100.,50.]])
    fa,fb=inp.measure(a),inp.measure(b)
    np.testing.assert_allclose([fa[k] for k in inp.CONTROL],[fb[k] for k in inp.CONTROL])
    assert fa[inp.NEW] == pytest.approx(1000) and fb[inp.NEW] == 0
    unit=inp.measure(a*10000)
    assert unit[inp.NEW] == pytest.approx(fa[inp.NEW])
    assert unit['ff_log_total_shares_lag1']-fa['ff_log_total_shares_lag1'] == pytest.approx(np.log(10000))
    split=inp.measure(a*np.array([[2],[3]]))
    assert split[inp.NEW] == pytest.approx(fa[inp.NEW])
    assert split['ff_log_total_growth5'] == pytest.approx(np.log(3/2))
    assert len(core.MATCHED)==11 and len(core.FEATURES)==12


def test_unknown_is_not_zero_and_known_invalid_shares_fail():
    assert inp.measure(np.array([[100.,np.nan],[100.,50.]]))['ff_reason']=='UNKNOWN_ENDPOINT'
    for bad in (np.zeros((2,2)),np.array([[100.,101.],[100.,50.]]),-np.ones((2,2)),
                np.full((2,2),np.inf),np.ones((2,2),bool),np.ones((3,2))):
        with pytest.raises(ValueError):
            inp.measure(bad)


def test_only_d_minus_six_and_d_minus_one_reach_formula():
    cal=pd.bdate_range('2024-10-24',periods=10)
    rows=population().iloc[:100].drop(columns=list(core.FEATURES[8:])).copy()
    rows[core.base.KEY[0]],rows.target_trade_date=cal[6],cal[7]
    members=sorted(rows.instrument.unique())
    idx=pd.MultiIndex.from_product([cal,members],names=['datetime','instrument'])
    frame=pd.DataFrame({'db_total_share':100.,'db_float_share':50.},index=idx)
    frame.loc[(cal[0],slice(None)),'db_float_share']=40.
    expected=inp.attach(rows,{'daily_basic':frame},cal)
    for day in list(cal[1:5])+list(cal[6:]):
        frame.loc[(day,slice(None)),:]=np.inf
    pd.testing.assert_frame_equal(expected,inp.attach(rows,{'daily_basic':frame},cal))
    frame=frame.drop(index=(cal[0],members[0]))
    actual=inp.attach(rows,{'daily_basic':frame},cal)
    assert actual.loc[actual.instrument.eq(members[0]),'ff_reason'].eq('UNKNOWN_ENDPOINT').all()
    assert actual[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])


def test_projected_h5_ignores_d_future_other_columns_and_members(tmp_path,monkeypatch):
    cal=pd.bdate_range('2024-10-24',periods=10)
    calendar=tmp_path/'day.txt'
    calendar.write_text('\n'.join(str(d.date()) for d in cal))
    path=tmp_path/'daily_basic.h5'
    idx=pd.MultiIndex.from_product([cal,['000001.SZ','000002.SZ']],names=['datetime','instrument'])
    frame=pd.DataFrame(1.,index=idx,columns=[*inp.COLUMNS['daily_basic'],'forbidden'])
    frame['forbidden']=np.inf
    frame.loc[(cal[6:],slice(None)),inp.COLUMNS['daily_basic']]=np.inf
    frame.to_hdf(path,key='data',format='table',data_columns=['datetime','instrument'])
    paths={'day_calendar':calendar,'daily_basic':path}
    stamps={n:list(inp.stamp(p)) for n,p in paths.items()}
    monkeypatch.setattr(inp,'sources',lambda plan,expected:(paths,{},stamps))
    rows=pd.DataFrame({'decision_as_of_trade_date':[cal[6]],'instrument':['000001.SZ']})
    frames,_,receipt=inp.read_panel({'binding':{'binding_hash':'fixed'}},rows,stamps,lambda:None)
    assert receipt['consumed_numeric_cells']==12 and len(frames['daily_basic'])==6
    assert np.isfinite(frames['daily_basic'].to_numpy()).all()
    monkeypatch.setattr(inp,'MAX_CELLS',11)
    with pytest.raises(ValueError,match='budget'):
        inp.read_panel({'binding':{'binding_hash':'fixed'}},rows,stamps,lambda:None)


def test_sparse_nonconstant_training_scale_preserves_unit_invariance():
    rows=population()
    rows[core.NEW]=np.where(rows.selection_effective_rank.eq(50),1000.,0.)
    a=core.fit(rows,'2025-03-31','candidate')
    idx=a['fields'].index(core.NEW)
    assert a['median'][idx]==0 and a['scale'][idx]>1
    scaled=rows.copy()
    scaled[core.NEW]*=10000
    b=core.fit(scaled,'2025-03-31','candidate')
    assert b['scale'][idx]==pytest.approx(a['scale'][idx]*10000)
    np.testing.assert_allclose(core.predict(rows,a).predicted_relative_net_bps,
                               core.predict(scaled,b).predicted_relative_net_bps,rtol=1e-9,atol=1e-8)
    matched=core.fit(rows,'2025-03-31','matched')
    for key in ('fields','median','scale'):
        assert a[key][:len(matched[key])]==matched[key]
    constant=rows.copy()
    constant[core.NEW]=0.
    c=core.fit(constant,'2025-03-31','candidate')
    assert c['scale'][c['fields'].index(core.NEW)]==1


def test_screen_alignment_binds_original_keys_missingness_and_values(monkeypatch):
    frame=population().iloc[:100].copy()
    old=frame[list(core.IDENTITY)].copy()
    old['float_fraction_delta5_bps']=frame[core.NEW]
    old['reason']='AVAILABLE'
    frame['ff_reason']='AVAILABLE'
    monkeypatch.setattr(inp,'screen_evidence',lambda:{'original_rows':100})
    monkeypatch.setattr(pd,'read_parquet',lambda path:old)
    assert inp.verify_screen_alignment(frame)['exact_equal_including_nan']
    bad=frame.copy()
    bad.loc[0,core.NEW]+=1
    with pytest.raises(ValueError,match='differ'):
        inp.verify_screen_alignment(bad)


def test_screen_receipt_requires_unchanged_label_free_evidence(tmp_path,monkeypatch):
    fields={'input_spec.json':'spec_sha256','screen.py':'code_sha256',
            'task_plan.md':'plan_sha256','factor_audit.json':'factor_audit_sha256',
            'screen_rows.parquet':'output_sha256'}
    receipt={'decision':'INPUT_SUPPORTED_FOR_F1','fit_count':0,'label_reads':0}
    for name,key in fields.items():
        path=tmp_path/name
        path.write_bytes(b'fixed-evidence')
        receipt[key]=inp.file_sha(path)
    path=tmp_path/'screen_receipt.json'
    path.write_text(json.dumps(receipt),encoding='utf-8')
    monkeypatch.setattr(inp,'node_path',lambda unused:tmp_path)
    monkeypatch.setattr(inp,'SCREEN_SHA',inp.file_sha(path))
    assert inp.screen_evidence()==receipt
    (tmp_path/'input_spec.json').write_bytes(b'changed-evidence')
    with pytest.raises(ValueError,match='drift'):
        inp.screen_evidence()
