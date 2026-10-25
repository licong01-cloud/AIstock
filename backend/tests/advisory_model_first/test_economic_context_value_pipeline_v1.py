from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first import economic_context_value_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.tests.advisory_model_first.test_economic_context_value_contracts_v1 import plan_fixture


def test_partial_fit_and_unknown_qe_state_are_durably_blocked_before_model_call(tmp_path,monkeypatch):
    plan=plan_fixture()
    root=tmp_path/plan.experiment_id
    registered=publish_stage(study_root=root,stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None,artifacts={'plan.json':_json_bytes(plan.model_dump(mode='json'))})
    manifest=read_stage(registered,stage='preregistered',plan_sha256=plan.plan_sha256,parent_sha256=None)
    publish_stage(study_root=root,stage='prepared',plan_sha256=plan.plan_sha256,parent_sha256=manifest['stage_sha256'],artifacts={'unit.json':_json_bytes({'test_fixture':True})})
    monkeypatch.setattr(pipeline,'load_context_value_v1',lambda **kwargs:(plan,root,manifest,(SimpleNamespace(configuration=None),)))
    monkeypatch.setattr(pipeline,'_ledger',lambda *a:None)
    (root/'fit_attempt.json').write_bytes(b'{"test_partial":true}')
    monkeypatch.setattr(pipeline,'train_context_value_v1',lambda **kwargs:pytest.fail('must not refit'))
    with pytest.raises(ValueError,match='partial fit'):
        pipeline.train_context_value_study_v1(plan_path=registered/'plan.json',output_root=tmp_path,qe_training_idle=True)
    with pytest.raises(ValueError,match='QE training'):
        pipeline.train_context_value_study_v1(plan_path=registered/'plan.json',output_root=tmp_path,qe_training_idle=None)


def test_source_recipe_binds_shared_consumer_and_policy_code():
    assert len(pipeline.context_implementation_sha256_v1())==64


def test_train_and_query_price_support_share_exact_tick_coordinate():
    import pandas as pd
    from backend.services.advisory_model_first.economic_entry_labels import KEY
    keys=pd.DataFrame({KEY[0]:[pd.Timestamp('2025-01-02')],KEY[1]:[pd.Timestamp('2025-01-03')],KEY[2]:['000001.SZ']})
    refs=keys.assign(target_reference_raw_cny=10.)
    prices=pd.DataFrame({'trade_date':[pd.Timestamp('2025-01-03')],'instrument':['000001.SZ'],
        'raw_open_cny':[10.02],'suspended':[False],'tradability_unknown':[False]})
    assert pipeline.context_observations_v1(keys,prices,refs).actual_gap_bps.iloc[0]==20.
    missing=pipeline.context_observations_v1(keys,prices.iloc[:0],refs)
    assert len(missing)==len(keys) and missing.actual_gap_bps.isna().all()
