"""Small, contract-focused matrix for the single joint-risk research leaf."""
import base64
import json
import zlib

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.joint_risk_top10_5td_inputs_v1 import (
    POLICY_SHA256, ROSTER, history_vectors, load_panel, validate_roster,
)
from backend.services.advisory_model_first.joint_risk_top10_5td_study_v1 import (
    _block_skill, audit_skill, estimate, evaluate_economics, predict, select_sets,
)
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def roster(calendar, symbols, positions=(130,), package='p'):
    rows,days = [],[]
    for p in positions:
        for rank,symbol in enumerate(symbols,1):
            rows.append(dict(package_id=package,manifest_sha256='a'*64,run_id='r',
                decision_as_of_trade_date=calendar[p],target_trade_date=calendar[p+1],
                instrument=symbol,selection_effective_rank=rank,candidate_group_size=20,parent_score=20-rank))
        days.append(dict(package_id=package,decision_date=calendar[p],target_date=calendar[p+1],
            horizon_end=calendar[p+5],original_candidates=20,source_status='AVAILABLE'))
    return pd.DataFrame(rows),pd.DataFrame(days)


def panel(n=150):
    calendar = pd.bdate_range('2024-01-02', periods=n)
    symbols = [f'{i:06d}.SZ' for i in range(1, 21)]
    rng = np.random.default_rng(20261020)
    closes = 10*np.exp(np.cumsum(rng.normal(0, .02, (n,20)), axis=0))
    return pd.DataFrame({'trade_date':np.repeat(calendar,20),
        'instrument':symbols*n, 'close':closes.ravel(), 'factor':1.}), calendar, symbols


def test_past_only_history_and_matched_joint_structure():
    frame, calendar, symbols = panel()
    z, receipt = history_vectors(frame, symbols, calendar[130])
    assert z.shape == (120,20) and (z <= 0).all()
    changed = frame.copy()
    changed.loc[changed.trade_date > calendar[130], 'close'] *= 100
    np.testing.assert_array_equal(z, history_vectors(changed, symbols, calendar[130])[0])
    assert receipt['maximum_observation_end'] == calendar[130].date().isoformat()
    joint, marginal, info = estimate(z)
    np.testing.assert_allclose(np.diag(joint), np.diag(marginal))
    off = ~np.eye(20,dtype=bool)
    np.testing.assert_allclose(marginal[off], np.outer(info['location_bps'],info['location_bps'])[off])
    assert np.linalg.eigvalsh(joint).min() >= -1e-6
    chosen = select_sets(joint)
    assert len(chosen)==10 and len(set(range(10))-set(chosen)) <= 2
    assert select_sets(np.zeros((20,20))) == list(range(10))


def test_missing_history_is_not_filled_or_removed():
    frame, calendar, symbols = panel()
    frame.loc[frame.instrument.eq(symbols[0]), 'close'] = np.nan
    z, receipt = history_vectors(frame,symbols,calendar[130])
    assert len(z)==0 and receipt['status']=='NO_RANK_ADVICE'
    frame.loc[frame.instrument.eq(symbols[1]), 'factor'] = -1
    with pytest.raises(ValueError, match='non-positive|invalid'):
        history_vectors(frame,symbols,calendar[130])


def test_native_roster_prediction_identity_and_unknown_slots():
    frame,calendar,symbols = panel()
    rr,dd = roster(calendar,symbols)
    rr['parent_score'] = rr.parent_score.astype(np.float32)
    sources = [dict(package_id='p',manifest_sha256='a'*64,run_id='r')]
    original = validate_roster(rr,dd,calendar,sources,calendar[130],calendar[130])
    pred,forecasts,count = predict(original,frame)
    pd.testing.assert_frame_equal(pred.loc[:,ROSTER],original)
    assert count==1 and forecasts[0]['risk_policy_sha256']==POLICY_SHA256
    for arm in ('baseline','marginal','joint'):
        assert pred[arm+'_selected'].sum()==10
    future = original.assign(hypothetical_liquidation_net_bps=1e12,valuation_status='CASH_ENTRY_NOT_EXECUTABLE')
    pd.testing.assert_frame_equal(predict(future,frame)[0],pred)
    frame.loc[frame.instrument.eq(symbols[0]),'close'] = np.nan
    unknown,_,updates = predict(original,frame)
    assert updates==0 and unknown.status.eq('NO_RANK_ADVICE').all()
    np.testing.assert_array_equal(unknown.joint_selected,unknown.baseline_selected)
    with pytest.raises(ValueError,match='D/T/H'):
        validate_roster(rr,dd.assign(target_date=calendar[132]),calendar,sources,calendar[130],calendar[130])
    with pytest.raises(ValueError,match='duplicate'):
        validate_roster(pd.concat([rr,rr.iloc[:1]]),dd,calendar,sources,calendar[130],calendar[130])


def test_original_axis_skill_support_and_future_target_cutoff():
    values = [1.]*70+[None]*10
    result = _block_skill(values)
    assert result['original_days']==80 and result['observed_days']==70 and result['supported']
    assert not _block_skill([1.]*49+[None]*31)['supported']
    assert not _block_skill([-1.]*80)['supported']
    frame,calendar,symbols = panel()
    rr,_ = roster(calendar,symbols)
    _,forecasts,_ = predict(rr,frame)
    result = audit_skill(rr,forecasts,frame,evaluation_start=calendar[130],evaluation_end=calendar[130])
    assert result['groups'][0]['loss'] is None and not result['evaluation_finance_decoded']
    forecasts[0]['maximum_observation_end'] = calendar[131].date().isoformat()
    with pytest.raises(ValueError,match='cutoff'):
        audit_skill(rr,forecasts,frame,evaluation_start=calendar[130],evaluation_end=calendar[140])
    # Deterministic proper-loss oracle only tests estimator-free evaluation math.
    frame,calendar,symbols = panel(300)
    frame['close'] = 10.
    rr,_ = roster(calendar,symbols,range(225,285))
    forecasts = [dict(package_id='p',decision_as_of_trade_date=d.isoformat(),target_trade_date=calendar[i+1].isoformat(),
        instruments=symbols,risk_policy_sha256=POLICY_SHA256,decision_cutoff=d.date().isoformat(),
        maximum_observation_end=d.date().isoformat(),status='AVAILABLE',
        joint_baseline_risk_bps2=0.,marginal_baseline_risk_bps2=1.) for i,d in enumerate(calendar) if 225<=i<285]
    result = audit_skill(rr,forecasts,frame,evaluation_start=calendar[225],evaluation_end=calendar[284])
    assert result['worth_economic_evaluation'] and result['statistics']['observed_days']==55
    assert result['label_status_counts']['IMMATURE_RISK_LABEL']==5


def test_native_finance_cost_cash_unknown_maturity_and_roster():
    frame,calendar,symbols = panel()
    rr,dd = roster(calendar,symbols)
    pred,_,_ = predict(rr,frame)
    for arm in ('baseline','marginal','joint'):
        pred[arm+'_selected'] = pred.selection_effective_rank.le(10)
    net = 10000*(1.01*(1-POLICY['sell_bps']/10000)/(1+POLICY['buy_bps']/10000)-1)
    finance = rr.assign(valuation_status='AVAILABLE',hypothetical_liquidation_net_bps=net,
        valuation_gross_terminal_ratio=1.01,observed_gap_bps=0.,exit_execution_status='EXECUTABLE',
        valuation_policy_sha256=VALUATION_POLICY_SHA256,label_information_end=calendar[135])
    finance.loc[0,'valuation_status'] = 'CASH_ENTRY_NOT_EXECUTABLE'
    result = evaluate_economics(finance,dd,pred,evaluation_end=calendar[140])
    assert result['fixed_ten_slot_mean_bps']['baseline']==pytest.approx(net*.9)
    assert result['slots']['baseline']['unavailable_entry']==1
    finance.loc[1,'valuation_status'] = 'UNKNOWN_MARK'
    assert evaluate_economics(finance,dd,pred,evaluation_end=calendar[140])['fixed_ten_slot_mean_bps']['baseline'] is None
    assert evaluate_economics(finance,dd,pred,evaluation_end=calendar[130])['mature_days']==0
    with pytest.raises(ValueError,match='cost'):
        evaluate_economics(finance.assign(hypothetical_liquidation_net_bps=net+1),dd,pred,evaluation_end=calendar[140])
    with pytest.raises(ValueError,match='roster'):
        evaluate_economics(finance,dd,pred.assign(parent_score=-1),evaluation_end=calendar[140])


def market(tmp_path, monkeypatch):
    """Small real Bin bytes with a narrow dataset-authority double; no business DB."""
    from backend.services.advisory_model_first import joint_risk_top10_5td_inputs_v1 as inputs
    calendar = pd.bdate_range('2023-01-02',periods=300)
    symbols = [f'{i:06d}.SZ' for i in range(1,21)]
    root = tmp_path/'market'
    root.mkdir()
    meta = root/'day'/'meta.json'
    meta.parent.mkdir()
    meta.write_text('{}',encoding='utf-8')
    lease = []
    for symbol in symbols:
        for field,value in (('close',10),('factor',1)):
            path = root/'day'/'features'/symbol.lower()/f'{field}.day.bin'
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(np.asarray([0]+[value]*len(calendar),dtype='<f4').tobytes())
            s = path.stat()
            lease.append([path.relative_to(root).as_posix(),s.st_size,s.st_mtime_ns])
    binding = dict(bin_stat_lease=dict(zlib_base64=base64.b64encode(zlib.compress(json.dumps(lease).encode())).decode()))
    binding['binding_hash'] = sha(binding)
    class Dataset:
        def __init__(self,binding,**kwargs):
            self.binding = binding
            self.root,self.calendar,self.identity = root,tuple(d.date() for d in calendar),binding['binding_hash']
        def require_day(self,d):
            if d not in self.calendar:
                raise ValueError('date uncovered')
        def file(self,key):
            assert key=='day_meta_export'
            return meta
        def assert_unchanged(self):
            pass
        def close(self):
            pass
    monkeypatch.setattr(inputs,'FrozenDataset',Dataset)
    monkeypatch.setattr('backend.data_service.frozen_dataset.FrozenDataset',Dataset)
    return binding,calendar,symbols,root


def test_bounded_native_bin_reader_and_generation_drift(tmp_path,monkeypatch):
    binding,calendar,symbols,root = market(tmp_path,monkeypatch)
    frame,receipt = load_panel(binding,symbols,start=calendar[125].date(),end=calendar[249].date(),run_id='test')
    assert len(frame)==2500 and receipt['bytes_read']==20*2*(125+1)*4
    assert frame.close.eq(10).all() and len(receipt['pins'])==40
    path = root/'day/features/000001.sz/close.day.bin'
    path.write_bytes(path.read_bytes()+np.asarray([10],dtype='<f4').tobytes())
    with pytest.raises(ValueError,match='generation'):
        load_panel(binding,symbols,start=calendar[125].date(),end=calendar[249].date(),run_id='test')


def test_atomic_pipeline_no_economic_decode_retry_partial_and_drift(tmp_path,monkeypatch):
    from backend.services.advisory_model_first import joint_risk_top10_5td_pipeline_v1 as pipeline
    binding,calendar,symbols,_ = market(tmp_path,monkeypatch)
    rr,dd = roster(calendar,symbols,range(125,285))
    source = tmp_path/'source'
    plan = dict(train_start=calendar[125].date().isoformat(),train_end=calendar[224].date().isoformat(),
        evaluation_start=calendar[225].date().isoformat(),evaluation_end=calendar[284].date().isoformat(),
        sources=[dict(package_id='p',manifest_sha256='a'*64,run_id='r')])
    digest = sha(plan)
    pre = publish_stage(study_root=source,stage='preregistered',plan_sha256=digest,parent_sha256=None,artifacts={'plan.json':_json_bytes(plan)})
    parent_sha = json.loads((pre/'manifest.json').read_bytes())['stage_sha256']
    prep = publish_stage(study_root=source,stage='prepared',plan_sha256=digest,parent_sha256=parent_sha,
        artifacts={'rows.parquet':parquet_bytes(rr),'days.parquet':parquet_bytes(dd),
                   'calendar.json':_json_bytes([d.isoformat() for d in calendar])})
    args = dict(source_root=source,source_plan_sha256=digest,
        source_prepared_sha256=json.loads((prep/'manifest.json').read_bytes())['stage_sha256'],binding=binding,
        output_root=tmp_path/'research',source_commit='1'*40)
    result = pipeline.run_study(**args)
    assert result['statistical_updates']==60 and result['economics'] is None
    assert not result['skill']['evaluation_finance_decoded'] and result['sql_count']==0
    # Parent has NO finance columns: an accidental economic read above would fail.
    monkeypatch.setattr(pipeline,'predict',lambda *a,**k:pytest.fail('exact retry fitted'))
    assert pipeline.run_study(**args)['exact_retry']
    incomplete = dict(args,output_root=tmp_path/'partial')
    monkeypatch.setattr(pipeline,'predict',lambda *a,**k:(_ for _ in ()).throw(RuntimeError('interrupt')))
    with pytest.raises(RuntimeError,match='interrupt'):
        pipeline.run_study(**incomplete)
    with pytest.raises(ValueError,match='partial'):
        pipeline.run_study(**incomplete)
    (source/'prepared/rows.parquet').write_bytes((source/'prepared/rows.parquet').read_bytes()+b'drift')
    with pytest.raises(Exception,match='hash mismatch'):
        pipeline.run_study(**args)
