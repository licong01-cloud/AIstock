from dataclasses import asdict
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_parent_normalized_trajectory_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_parent_normalized_trajectory_v1 import CLOCK, INFORMATION_FEATURES, STATUS, ParentNormalizedTrajectoryPlanV1
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_parent_raw_trajectory_pipeline_v1 import trajectory_plan_fixture as predecessor_plan_fixture
from backend.tests.advisory_model_first.test_economic_parent_raw_trajectory_v1 import trajectory_fit_fixture as predecessor_fit_fixture
from backend.tests.advisory_model_first.test_economic_parent_normalized_trajectory_v1 import normalized_fit_fixture, normalized_rows_fixture
from pathlib import Path


def normalized_plan_fixture(tmp_path):
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
    plan = ParentNormalizedTrajectoryPlanV1(**prior.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref', 'prior_fit_journal_sha256', 'raw_prepared_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='parent_normalized_trajectory_predecessor'),
        trajectory_prepared_manifest_ref=evidence_reference_for_file(saved.parent/'prepared/manifest.json', role='parent_normalized_trajectory_base_snapshot'),
        raw_prepared_manifest_ref=evidence_reference_for_file(Path(prior.raw_prepared_manifest_ref.artifact_uri), role='parent_normalized_trajectory_raw_snapshot'),
        prior_fit_journal_sha256=hashlib.sha256(journal.read_bytes()).hexdigest())
    return plan, prior, parent


def test_actual91_prefix_M23_stage_bound_budget_typed_clone_and_foreign_events(tmp_path):
    plan, prior, _ = normalized_plan_fixture(tmp_path)
    assert pipeline.verify_parent_normalized_trajectory_budget_v1(plan) == prior
    with pytest.raises(ValueError):
        plan.model_copy(update={'model_id': 'M23'})
    with pytest.raises(ValueError):
        plan.model_copy(update={'lag_trade_days': 3})
    with pytest.raises(ValueError):
        plan.model_copy(update={'component_raw_columns': {'lstm': 'raw__same', 'fund': 'raw__same'}})
    with pytest.raises(ValueError, match='reset'):
        pipeline._raw_root(plan, tmp_path/'other')
    root = tmp_path/plan.experiment_id
    root.mkdir()
    for head in ('matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'):
        pipeline._raw_fit_event(plan, root, head)
    assert pipeline.verify_parent_normalized_trajectory_budget_v1(plan) == prior
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._raw_fit_event(plan, root, 'extra')
    journal = tmp_path/'campaign_fit_journal.jsonl'
    original = journal.read_bytes()
    journal.write_bytes(original.replace(b'STARTED', b'UNKNOWN', 1))
    with pytest.raises(ValueError, match='prefix'):
        pipeline.verify_parent_normalized_trajectory_budget_v1(plan)
    journal.write_bytes(original)
    lines = original.splitlines(keepends=True)
    last = json.loads(lines[-1])
    last['head'] = 'matched_mean'
    journal.write_bytes(b''.join(lines[:-1])+json.dumps(last).encode()+b'\n')
    with pytest.raises(ValueError, match='duplicate'):
        pipeline.verify_parent_normalized_trajectory_budget_v1(plan)


def test_prepare_zero_SQL_original_labels_unknown_retry_partial_and_tamper(tmp_path, monkeypatch):
    plan, _, parent = normalized_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = normalized_rows_fixture()
    args['rankings'].loc[2, 'norm__lstm'] = None
    args['rankings']['selection_effective_rank'] = [1, 2, 1, 2]
    args['rankings']['is_candidate_decision'] = [True, True, False, False]
    candidates = args['rankings'].loc[args['rankings'].is_candidate_decision]
    frozen = tmp_path/'frozen'
    frozen.mkdir()
    args['rankings'].to_parquet(frozen/'frozen_rankings.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    reusable = tmp_path/'reusable'
    reusable.mkdir()
    args['base_rows'].assign(feature_visible_through=args['base_rows'][KEY[0]]).to_parquet(Path(plan.trajectory_prepared_manifest_ref.artifact_uri).parent/'rows.parquet', index=False)
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_parent_normalized_trajectory_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, reusable, {})))
    prepared = pipeline.prepare_parent_normalized_trajectory_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    rows = pd.read_parquet(prepared/'rows.parquet')
    assert rows[KEY].equals(candidates[KEY]) and rows.gross_value_ratio.eq(1.04).all() and len(rows) == 2
    assert rows[STATUS].tolist() == ['UNKNOWN_SOURCE_OR_INPUT', 'AVAILABLE']
    receipt = json.loads((prepared/'parent_normalized_trajectory_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and not receipt['database_accessed'] and not receipt['database_written'] and receipt['native_identity'] == 'UNPROVEN'
    monkeypatch.setattr(pipeline.pd, 'read_parquet', lambda *_args, **_kwargs: pytest.fail('published prepare cannot rebuild'))
    assert pipeline.prepare_parent_normalized_trajectory_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_parent_normalized_trajectory_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_parent_normalized_trajectory_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'parent_normalized_trajectory_source_receipt.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_parent_normalized_trajectory_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M24_T_open_only_D_normalized_trajectory_inputs_shared_unknown_and_future_clock():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys((*D_FEATURES, *INFORMATION_FEATURES), .01), 'feature_visible_through': key[KEY[0]], CLOCK: key[KEY[0]], STATUS: 'AVAILABLE'}])
    args = dict(fitted=normalized_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999., suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64))
    for arm in ('matched', 'candidate'):
        assert pipeline.parent_normalized_trajectory_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['TAKE']
        inputs[STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
        assert pipeline.parent_normalized_trajectory_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['UNAVAILABLE']
        inputs[STATUS] = 'AVAILABLE'
    inputs[CLOCK] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.parent_normalized_trajectory_actual_decisions_v1(**args, arm='candidate')


def test_original_snapshots_family_and_role_package_coordinate_are_bound(tmp_path, monkeypatch):
    plan, _, _ = normalized_plan_fixture(tmp_path)
    monkeypatch.setattr(pipeline, 'parent_normalized_trajectory_implementation_sha256_v1', lambda: plan.implementation_sha256)
    monkeypatch.setattr(pipeline, 'campaign_sources_v2', lambda _: ('same frozen original',))
    assert pipeline.parent_normalized_trajectory_sources_v1(plan) == ('same frozen original',)
    for change in ({'package_id': 'other'}, {'package_manifest_sha256': 'b'*64},
            {'component_raw_columns': {'lstm': 'raw__new_leg', 'fund': 'raw__fund'}}):
        with pytest.raises(ValueError, match='coordinate'):
            pipeline.parent_normalized_trajectory_sources_v1(plan.model_copy(update=change))
    with pytest.raises(ValueError):
        plan.model_copy(update={'raw_prepared_manifest_ref': plan.raw_prepared_manifest_ref.model_copy(update={'role': 'other'})})
    wrong = evidence_reference_for_file(Path(plan.predecessor_manifest_ref.artifact_uri), role='parent_normalized_trajectory_raw_snapshot')
    with pytest.raises(ValueError):
        pipeline.parent_normalized_trajectory_sources_v1(plan.model_copy(update={'raw_prepared_manifest_ref': wrong}))

def test_base_cannot_replace_M23_and_raw_cannot_replace_original_M20(tmp_path, monkeypatch):
    plan, _, _ = normalized_plan_fixture(tmp_path)
    monkeypatch.setattr(pipeline, 'parent_normalized_trajectory_implementation_sha256_v1', lambda: plan.implementation_sha256)
    monkeypatch.setattr(pipeline, 'campaign_sources_v2', lambda _: ('same frozen original',))
    wrong_base = evidence_reference_for_file(Path(plan.raw_prepared_manifest_ref.artifact_uri), role='parent_normalized_trajectory_base_snapshot')
    with pytest.raises(ValueError, match='actual M23 prepared'):
        pipeline.parent_normalized_trajectory_sources_v1(plan.model_copy(update={'trajectory_prepared_manifest_ref': wrong_base}))
    wrong_raw = evidence_reference_for_file(Path(plan.trajectory_prepared_manifest_ref.artifact_uri), role='parent_normalized_trajectory_raw_snapshot')
    with pytest.raises(ValueError, match='actual M23 original reference'):
        pipeline.parent_normalized_trajectory_sources_v1(plan.model_copy(update={'raw_prepared_manifest_ref': wrong_raw}))
