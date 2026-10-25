import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_asymmetric_risk_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_asymmetric_risk_v1 import ASYMMETRIC_RISK_FEATURES, CLOCK, STATUS, AsymmetricRiskPlanV1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_asymmetric_risk_v1 import asymmetric_risk_fit_fixture, asymmetric_risk_fixture
from backend.tests.advisory_model_first.test_economic_flow_path_pipeline_v1 import flow_path_plan_fixture


def asymmetric_risk_plan_fixture(tmp_path):
    flow, cohort, limit, valuation, free, session, traded, breadth, volume, market, path, original, parent, events = flow_path_plan_fixture(tmp_path)
    ancestor = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(flow.model_dump(mode='json'))} if stage == 'preregistered' else {'unit.json': b'{}'}
        saved = publish_stage(study_root=tmp_path/flow.experiment_id, stage=stage, plan_sha256=flow.plan_sha256, parent_sha256=ancestor, artifacts=artifacts)
        ancestor = read_stage(saved, stage=stage, plan_sha256=flow.plan_sha256, parent_sha256=ancestor)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(flow, saved.parent, parent, stage.upper(), saved/'manifest.json')
    events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=flow.model_id, campaign_id=flow.campaign_id,
        experiment_id=flow.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    plan = AsymmetricRiskPlanV1(**flow.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref', 'flow_source_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='asymmetric_risk_predecessor'))
    return plan, flow, cohort, limit, valuation, free, session, traded, breadth, volume, market, path, original, parent, events


def test_prepare_zero_SQL_original_order_UNKNOWN_exact_retry_partial_QE_and_tamper(tmp_path, monkeypatch):
    plan, _, _, _, _, _, _, _, _, _, _, _, _, parent, _ = asymmetric_risk_plan_fixture(tmp_path)
    with pytest.raises(ValueError):
        pipeline.preregister_asymmetric_risk_v1(plan=plan.model_copy(update={'model_id': 'M17'}), output_root=tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = asymmetric_risk_fixture()
    candidates = args['candidates'].assign(selection_effective_rank=1, is_candidate_decision=True)
    frozen, reusable = tmp_path/'frozen', tmp_path/'reusable'
    frozen.mkdir()
    reusable.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates.assign(**dict.fromkeys(D_FEATURES, 0.), feature_visible_through=candidates[KEY[0]]).to_parquet(reusable/'rows.parquet', index=False)
    args['prices'].loc[0, 'raw_close_cny'] = None
    args['prices'].to_parquet(frozen/'prices.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_asymmetric_risk_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, reusable, {})))
    prepared = pipeline.prepare_asymmetric_risk_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    result = pd.read_parquet(prepared/'rows.parquet')
    assert result[KEY].equals(candidates[KEY]) and result[STATUS].tolist() == ['UNKNOWN_PRICE_SOURCE', 'AVAILABLE']
    receipt = json.loads((prepared/'asymmetric_risk_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and not receipt['database_accessed'] and not receipt['database_written']
    assert receipt['native_identity'] == 'UNPROVEN' and receipt['source_manifest_sha256'] == plan.parent_prepared_manifest_ref.sha256
    assert receipt['source_columns'] == plan.parameters['stock_source_columns'] and receipt['information_units'] == ['fraction']*3
    monkeypatch.setattr(pipeline.pd, 'read_parquet', lambda *_args, **_kwargs: pytest.fail('published prepare must not rebuild risk'))
    assert pipeline.prepare_asymmetric_risk_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_asymmetric_risk_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_asymmetric_risk_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'asymmetric_risk_source_receipt.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_asymmetric_risk_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M18_T_open_observation_D_clock_unknown_not_future_close():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(ASYMMETRIC_RISK_FEATURES, (.01, .02, .005), strict=True)),
        'feature_visible_through': key[KEY[0]], CLOCK: key[KEY[0]]}])
    args = dict(fitted=asymmetric_risk_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.asymmetric_risk_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[ASYMMETRIC_RISK_FEATURES[0]] = None
    assert pipeline.asymmetric_risk_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs[CLOCK] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.asymmetric_risk_actual_decisions_v1(**args)

