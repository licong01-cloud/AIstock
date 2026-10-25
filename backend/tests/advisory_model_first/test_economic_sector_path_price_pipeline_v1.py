from dataclasses import asdict
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_sector_path_price_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_sector_path_price_v1 import CLOCK, INFORMATION_FEATURES, STATUS, SectorPathPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_parent_normalized_trajectory_pipeline_v1 import normalized_plan_fixture as predecessor_plan_fixture
from backend.tests.advisory_model_first.test_economic_parent_normalized_trajectory_v1 import normalized_fit_fixture as predecessor_fit_fixture
from backend.tests.advisory_model_first.test_economic_sector_path_price_v1 import path_fit_fixture, path_rows_fixture
from pathlib import Path


def path_plan_fixture(tmp_path):
    prior, _, parent = predecessor_plan_fixture(tmp_path)
    fitted = predecessor_fit_fixture()
    ancestor = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(prior.model_dump(mode='json'))} if stage == 'preregistered' else {'metadata.json': _json_bytes(dict(parameters=prior.parameters, models=fitted.models, recipe=fitted.recipe, model_sha256=fitted.model_sha256, support=asdict(fitted.support), diagnostics=fitted.diagnostics))} if stage == 'trained' else {'unit.json': b'{}'}
        saved = publish_stage(study_root=tmp_path/prior.experiment_id, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=ancestor, artifacts=artifacts)
        ancestor = read_stage(saved, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=ancestor)['stage_sha256']
        _record(prior, saved.parent, parent, stage.upper(), saved/'manifest.json')
    journal = tmp_path/'campaign_fit_journal.jsonl'
    events = [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=prior.model_id, campaign_id=prior.campaign_id, experiment_id=prior.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    with journal.open('ab') as handle:
        handle.write(b''.join(json.dumps(event).encode()+b'\n' for event in events))
    from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
    from backend.services.advisory_model_first.economic_selection_state_pipeline_v1 import SOURCE_FIELDS
    from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture
    events_all = [json.loads(line) for line in journal.read_bytes().splitlines()]
    sector_id = next(event['experiment_id'] for event in events_all if event['model_id'] == 'M1')
    sector_root = tmp_path/sector_id
    original = SectorPricePlanV1.model_validate_json((sector_root/'preregistered/plan.json').read_text(encoding='utf-8'))
    registered_parent = read_stage(sector_root/'preregistered', stage='preregistered', plan_sha256=original.plan_sha256, parent_sha256=None)['stage_sha256']
    stage_parent = read_stage(sector_root/'prepared', stage='prepared', plan_sha256=original.plan_sha256, parent_sha256=registered_parent)['stage_sha256']
    sector_fit = sector_fit_fixture()
    for stage in ('trained', 'evaluated'):
        artifacts = {'metadata.json': _json_bytes(dict(parameters=original.parameters, models=sector_fit.models, recipe=sector_fit.recipe, model_sha256=sector_fit.model_sha256, support=asdict(sector_fit.support), diagnostics=sector_fit.diagnostics))} if stage == 'trained' else {'unit.json': b'{}'}
        published = publish_stage(study_root=sector_root, stage=stage, plan_sha256=original.plan_sha256, parent_sha256=stage_parent, artifacts=artifacts)
        stage_parent = read_stage(published, stage=stage, plan_sha256=original.plan_sha256, parent_sha256=stage_parent)['stage_sha256']
        _record(original, sector_root, parent, stage.upper(), published/'manifest.json')
    plan = SectorPathPricePlanV1(**{name: getattr(prior, name) for name in SOURCE_FIELDS},
        implementation_sha256=prior.implementation_sha256, crosswalk_ref=original.crosswalk_ref,
        budget_anchor_ref=prior.budget_anchor_ref,
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='sector_path_price_predecessor'),
        sector_prepared_manifest_ref=evidence_reference_for_file(sector_root/'prepared/manifest.json', role='sector_path_price_base_snapshot'),
        prior_fit_journal_sha256=hashlib.sha256(journal.read_bytes()).hexdigest())
    return plan, prior, parent


def test_actual95_prefix_M24_stage_bound_budget_typed_clone_and_foreign_events(tmp_path):
    plan, prior, _ = path_plan_fixture(tmp_path)
    assert pipeline.verify_sector_path_price_budget_v1(plan) == prior
    with pytest.raises(ValueError):
        plan.model_copy(update={'model_id': 'M24'})
    with pytest.raises(ValueError):
        plan.model_copy(update={'sector_prepared_manifest_ref': plan.predecessor_manifest_ref})
    with pytest.raises(ValueError, match='reset'):
        pipeline._raw_root(plan, tmp_path/'other')
    root = tmp_path/plan.experiment_id
    root.mkdir()
    for head in ('matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'):
        pipeline._raw_fit_event(plan, root, head)
    assert pipeline.verify_sector_path_price_budget_v1(plan) == prior
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._raw_fit_event(plan, root, 'extra')
    journal = tmp_path/'campaign_fit_journal.jsonl'
    original = journal.read_bytes()
    journal.write_bytes(original.replace(b'STARTED', b'UNKNOWN', 1))
    with pytest.raises(ValueError, match='prefix'):
        pipeline.verify_sector_path_price_budget_v1(plan)
    journal.write_bytes(original)
    lines = original.splitlines(keepends=True)
    last = json.loads(lines[-1])
    last['head'] = 'matched_mean'
    journal.write_bytes(b''.join(lines[:-1])+json.dumps(last).encode()+b'\n')
    with pytest.raises(ValueError, match='duplicate'):
        pipeline.verify_sector_path_price_budget_v1(plan)


def test_prepare_zero_SQL_original_labels_unknown_retry_partial_and_tamper(tmp_path, monkeypatch):
    plan, _, parent = path_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = path_rows_fixture()
    args['base_rows'].loc[0, 'sector_feature_status'] = 'UNKNOWN_CLASSIFICATION_OR_MAPPING'
    candidates = args['candidates'].assign(selection_effective_rank=[1, 2], is_candidate_decision=True)
    frozen = tmp_path/'frozen'
    frozen.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    reusable = tmp_path/'reusable'
    reusable.mkdir()
    args['base_rows'].assign(feature_visible_through=args['base_rows'][KEY[0]]).to_parquet(Path(plan.sector_prepared_manifest_ref.artifact_uri).parent/'rows.parquet', index=False)
    monkeypatch.setattr(pipeline, 'load_sector_path_quotes_v1', lambda **_: (args['quotes'], args['crosswalk'], dict(selects=0, database_accessed=False, database_written=False, native_identity='UNPROVEN')))
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_sector_path_price_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, reusable, {})))
    prepared = pipeline.prepare_sector_path_price_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    rows = pd.read_parquet(prepared/'rows.parquet')
    assert rows[KEY].equals(candidates[KEY]) and rows.gross_value_ratio.eq(1.04).all() and len(rows) == 2
    assert rows[STATUS].tolist() == ['UNKNOWN_SOURCE_OR_INPUT', 'AVAILABLE']
    receipt = json.loads((prepared/'sector_path_price_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and not receipt['database_accessed'] and not receipt['database_written'] and receipt['native_identity'] == 'UNPROVEN'
    monkeypatch.setattr(pipeline.pd, 'read_parquet', lambda *_args, **_kwargs: pytest.fail('published prepare cannot rebuild'))
    assert pipeline.prepare_sector_path_price_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_sector_path_price_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_sector_path_price_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'sector_path_price_source_receipt.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_sector_path_price_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M25_T_open_only_D_sector_path_inputs_shared_unknown_and_future_clock():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys((*D_FEATURES, *INFORMATION_FEATURES), .01), 'feature_visible_through': key[KEY[0]], CLOCK: key[KEY[0]], STATUS: 'AVAILABLE'}])
    args = dict(fitted=path_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999., suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64))
    for arm in ('matched', 'candidate'):
        assert pipeline.sector_path_price_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['TAKE']
        inputs[STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
        assert pipeline.sector_path_price_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['UNAVAILABLE']
        inputs[STATUS] = 'AVAILABLE'
    inputs[CLOCK] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.sector_path_price_actual_decisions_v1(**args, arm='candidate')


def test_original_M1_snapshot_and_crosswalk_are_not_replaced(tmp_path, monkeypatch):
    plan, _, _ = path_plan_fixture(tmp_path)
    monkeypatch.setattr(pipeline, 'sector_path_price_implementation_sha256_v1', lambda: plan.implementation_sha256)
    monkeypatch.setattr(pipeline, 'campaign_sources_v2', lambda _: ('same frozen original',))
    assert pipeline.sector_path_price_sources_v1(plan) == ('same frozen original',)
    with pytest.raises(ValueError, match='crosswalk'):
        pipeline.sector_path_price_sources_v1(plan.model_copy(update={'crosswalk_ref': plan.crosswalk_ref.model_copy(update={'sha256': 'b'*64})}))
    wrong_base = evidence_reference_for_file(Path(plan.predecessor_manifest_ref.artifact_uri).parent.parent/'prepared/manifest.json', role='sector_path_price_base_snapshot')
    with pytest.raises(ValueError):
        pipeline.sector_path_price_sources_v1(plan.model_copy(update={'sector_prepared_manifest_ref': wrong_base}))
    with pytest.raises(ValueError):
        plan.model_copy(update={'sector_prepared_manifest_ref': plan.sector_prepared_manifest_ref.model_copy(update={'role': 'other'})})
