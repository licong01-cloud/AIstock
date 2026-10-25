import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_breadth_state_price_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_breadth_state_price_v1 import BREADTH_STATE_FEATURES, BreadthStatePricePlanV1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_breadth_state_price_v1 import breadth_state_fit_fixture, breadth_state_fixture
from backend.tests.advisory_model_first.test_economic_volume_context_price_pipeline_v1 import volume_context_plan_fixture


def breadth_state_plan_fixture(tmp_path):
    volume, market, path, original, parent, events = volume_context_plan_fixture(tmp_path)
    ancestor = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(volume.model_dump(mode='json'))} if stage == 'preregistered' else {'unit.json': b'{}'}
        saved = publish_stage(study_root=tmp_path/volume.experiment_id, stage=stage, plan_sha256=volume.plan_sha256, parent_sha256=ancestor, artifacts=artifacts)
        ancestor = read_stage(saved, stage=stage, plan_sha256=volume.plan_sha256, parent_sha256=ancestor)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(volume, saved.parent, parent, stage.upper(), saved/'manifest.json')
    events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=volume.model_id, campaign_id=volume.campaign_id,
        experiment_id=volume.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    plan = BreadthStatePricePlanV1(**volume.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='breadth_state_predecessor'))
    return plan, volume, market, path, original, parent, events


def test_original_freeze_prepare_retry_partial_fit_QE_and_no_new_SQL(tmp_path, monkeypatch):
    plan, _, _, _, _, parent, _ = breadth_state_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = breadth_state_fixture()
    candidates = args['breadth'][KEY].assign(selection_effective_rank=[1, 2]*22, is_candidate_decision=True)
    frozen, reusable, feature = tmp_path/'frozen', tmp_path/'reusable', tmp_path/'feature'
    for folder in (frozen, reusable, feature):
        folder.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates[KEY].to_parquet(reusable/'rows.parquet', index=False)
    args['breadth'].to_parquet(feature/'features.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_breadth_state_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, feature, reusable, {})))
    prepared = pipeline.prepare_breadth_state_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    result = pd.read_parquet(prepared/'rows.parquet')
    assert result[KEY].equals(candidates[KEY]) and result.breadth_state_feature_status.value_counts().to_dict() == {'UNKNOWN_20D_HISTORY': 38, 'AVAILABLE': 6}
    receipt = json.loads((prepared/'breadth_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and receipt['database_accessed'] is False and receipt['native_identity'] == 'UNPROVEN'
    monkeypatch.setattr(pd, 'read_parquet', lambda *_, **__: pytest.fail('published retry must not prepare again'))
    assert pipeline.prepare_breadth_state_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_breadth_state_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_breadth_state_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'breadth_daily.parquet').write_bytes(b'corrupted')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_breadth_state_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M10_actual_price_observation_uses_explicit_D_clock_not_future_close():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(BREADTH_STATE_FEATURES, (.4, .01, .6), strict=True)),
        'feature_visible_through': key[KEY[0]], 'breadth_state_feature_visible_through': key[KEY[0]]}])
    args = dict(fitted=breadth_state_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.breadth_state_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[BREADTH_STATE_FEATURES[0]] = None
    assert pipeline.breadth_state_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs['breadth_state_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.breadth_state_actual_decisions_v1(**args)
