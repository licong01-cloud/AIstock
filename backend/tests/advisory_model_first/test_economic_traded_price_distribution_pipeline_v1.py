import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_traded_price_distribution_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.economic_traded_price_distribution_v1 import CLOCK, STATUS, TRADED_PRICE_FEATURES, TradedPriceDistributionPlanV1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_breadth_state_price_pipeline_v1 import breadth_state_plan_fixture
from backend.tests.advisory_model_first.test_economic_traded_price_distribution_v1 import traded_price_fit_fixture, traded_price_fixture


def traded_price_plan_fixture(tmp_path):
    breadth, volume, market, path, original, parent, events = breadth_state_plan_fixture(tmp_path)
    ancestor = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(breadth.model_dump(mode='json'))} if stage == 'preregistered' else {'unit.json': b'{}'}
        saved = publish_stage(study_root=tmp_path/breadth.experiment_id, stage=stage, plan_sha256=breadth.plan_sha256, parent_sha256=ancestor, artifacts=artifacts)
        ancestor = read_stage(saved, stage=stage, plan_sha256=breadth.plan_sha256, parent_sha256=ancestor)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(breadth, saved.parent, parent, stage.upper(), saved/'manifest.json')
    events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=breadth.model_id, campaign_id=breadth.campaign_id,
        experiment_id=breadth.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    plan = TradedPriceDistributionPlanV1(**breadth.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='traded_price_distribution_predecessor'),
        volume_snapshot_manifest_ref=evidence_reference_for_file(tmp_path/volume.experiment_id/'prepared/manifest.json', role='traded_price_distribution_volume_snapshot'))
    return plan, breadth, volume, market, path, original, parent, events


def test_prepare_original_frozen_volume_zero_SQL_retry_partial_and_unknown_QE(tmp_path, monkeypatch):
    plan, _, _, _, _, _, parent, _ = traded_price_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = traded_price_fixture()
    candidates = args['candidates'].assign(selection_effective_rank=1, is_candidate_decision=True)
    frozen, reusable, feature, snapshot = [tmp_path/name for name in ('frozen', 'reusable', 'feature', 'snapshot')]
    for folder in (frozen, reusable, feature, snapshot):
        folder.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates[KEY].to_parquet(reusable/'rows.parquet', index=False)
    args['prices'].to_parquet(frozen/'raw_daily.parquet', index=False)
    args['atr'].to_parquet(feature/'features.parquet', index=False)
    args['volumes'].to_parquet(snapshot/'volume_daily.parquet', index=False)
    (snapshot/'volume_source_receipt.json').write_text('{"native_identity":"UNPROVEN"}', encoding='utf-8')
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_traded_price_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, feature, reusable, {}, snapshot)))
    prepared = pipeline.prepare_traded_price_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    result = pd.read_parquet(prepared/'rows.parquet')
    assert result[KEY].equals(candidates[KEY]) and result[STATUS].tolist() == ['UNKNOWN_20D_HISTORY', 'AVAILABLE', 'AVAILABLE']
    receipt = json.loads((prepared/'traded_price_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and receipt['database_accessed'] is False and receipt['native_identity'] == 'UNPROVEN'
    monkeypatch.setattr(pd, 'read_parquet', lambda *_, **__: pytest.fail('published retry must not prepare again'))
    assert pipeline.prepare_traded_price_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_traded_price_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_traded_price_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'traded_price_source_receipt.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_traded_price_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_original_volume_snapshot_requires_artifact_identity_and_actual_M9_content(tmp_path):
    plan, _, volume, _, _, _, _, _ = traded_price_plan_fixture(tmp_path)
    with pytest.raises(ValueError, match='artifacts are missing'):
        pipeline._volume_snapshot(plan, volume)
    wrong = plan.model_copy(update={'volume_snapshot_manifest_ref': plan.predecessor_manifest_ref})
    with pytest.raises(ValueError, match='original M9'):
        pipeline._volume_snapshot(wrong, volume)
    artifact = tmp_path/volume.experiment_id/'prepared/unit.json'
    artifact.write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline._volume_snapshot(plan, volume)


def test_M11_T_open_observation_retains_D_clock_missing_and_no_T_close():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(TRADED_PRICE_FEATURES, (.55, .1, .4), strict=True)),
        'feature_visible_through': key[KEY[0]], CLOCK: key[KEY[0]]}])
    args = dict(fitted=traded_price_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.traded_price_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[TRADED_PRICE_FEATURES[0]] = None
    assert pipeline.traded_price_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs[CLOCK] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.traded_price_actual_decisions_v1(**args)
