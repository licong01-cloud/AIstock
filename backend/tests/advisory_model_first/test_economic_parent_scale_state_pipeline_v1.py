from dataclasses import asdict
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.services.advisory_model_first import economic_parent_scale_state_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_parent_scale_state_v1 import CLOCK, SCALE_FEATURES, STATUS, ParentScaleStatePlanV1
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_parent_raw_score_pipeline_v1 import raw_plan_fixture
from backend.tests.advisory_model_first.test_economic_parent_raw_score_v1 import MAPPING, raw_fit_fixture
from backend.tests.advisory_model_first.test_economic_parent_scale_state_v1 import DTYPES, scale_fit_fixture, scale_rows_fixture


def scale_plan_fixture(tmp_path):
    prior, _, parent = raw_plan_fixture(tmp_path)
    fitted = raw_fit_fixture()
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
    plan = ParentScaleStatePlanV1(**prior.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref', 'prior_fit_journal_sha256'}),
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='parent_scale_state_predecessor'),
        prior_fit_journal_sha256=hashlib.sha256(journal.read_bytes()).hexdigest(), score_dtypes=DTYPES)
    return plan, prior, parent


def test_actual79_prefix_M20_stage_bound_budget_typed_clone_and_foreign_events(tmp_path):
    plan, prior, _ = scale_plan_fixture(tmp_path)
    assert pipeline.verify_parent_scale_state_budget_v1(plan) == prior
    with pytest.raises(ValueError):
        plan.model_copy(update={'model_id': 'M20'})
    with pytest.raises(ValueError):
        plan.model_copy(update={'component_raw_columns': {'lstm': 'raw__same', 'fund': 'raw__same'}})
    with pytest.raises(ValueError, match='reset'):
        pipeline._raw_root(plan, tmp_path/'other')
    root = tmp_path/plan.experiment_id
    root.mkdir()
    for head in ('matched_mean', 'matched_path', 'candidate_mean', 'candidate_path'):
        pipeline._raw_fit_event(plan, root, head)
    assert pipeline.verify_parent_scale_state_budget_v1(plan) == prior
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._raw_fit_event(plan, root, 'extra')
    journal = tmp_path/'campaign_fit_journal.jsonl'
    original = journal.read_bytes()
    journal.write_bytes(original.replace(b'STARTED', b'UNKNOWN', 1))
    with pytest.raises(ValueError, match='prefix'):
        pipeline.verify_parent_scale_state_budget_v1(plan)
    journal.write_bytes(original)
    lines = original.splitlines(keepends=True)
    last = json.loads(lines[-1])
    last['head'] = 'matched_mean'
    journal.write_bytes(b''.join(lines[:-1])+json.dumps(last).encode()+b'\n')
    with pytest.raises(ValueError, match='duplicate'):
        pipeline.verify_parent_scale_state_budget_v1(plan)


def test_prepare_zero_SQL_original_labels_unknown_retry_partial_and_tamper(tmp_path, monkeypatch):
    plan, prior, parent = scale_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = scale_rows_fixture(missing=True)
    candidates = args['rankings'].assign(selection_effective_rank=[1, 2, 3], is_candidate_decision=True)
    frozen = tmp_path/'frozen'
    frozen.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    reusable = tmp_path/'reusable'
    reusable.mkdir()
    args['base_rows'].assign(feature_visible_through=args['base_rows'][KEY[0]]).to_parquet(tmp_path/prior.experiment_id/'prepared/rows.parquet', index=False)
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_parent_scale_state_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, reusable, {})))
    prepared = pipeline.prepare_parent_scale_state_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    rows = pd.read_parquet(prepared/'rows.parquet')
    assert rows[KEY].equals(candidates[KEY]) and rows.gross_value_ratio.eq(1.04).all() and len(rows) == 3
    assert rows[STATUS].tolist() == ['AVAILABLE', 'AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT']
    receipt = json.loads((prepared/'parent_scale_state_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and not receipt['database_accessed'] and not receipt['database_written'] and receipt['native_identity'] == 'UNPROVEN'
    monkeypatch.setattr(pipeline.pd, 'read_parquet', lambda *_args, **_kwargs: pytest.fail('published prepare cannot rebuild'))
    assert pipeline.prepare_parent_scale_state_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_parent_scale_state_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_parent_scale_state_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'parent_scale_state_source_receipt.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_parent_scale_state_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M21_T_open_only_D_scale_inputs_shared_unknown_and_future_clock():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys((*D_FEATURES, *SCALE_FEATURES), .01), 'feature_visible_through': key[KEY[0]], CLOCK: key[KEY[0]], STATUS: 'AVAILABLE'}])
    args = dict(fitted=scale_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999., suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64))
    for arm in ('matched', 'candidate'):
        assert pipeline.parent_scale_state_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['TAKE']
        inputs[STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
        assert pipeline.parent_scale_state_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['UNAVAILABLE']
        inputs[STATUS] = 'AVAILABLE'
    inputs[CLOCK] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.parent_scale_state_actual_decisions_v1(**args, arm='candidate')


def test_original_roles_normalized_coordinate_and_actual_schema_dtype_are_bound(tmp_path, monkeypatch):
    plan, _, _ = scale_plan_fixture(tmp_path)
    source_dir = tmp_path/'source'
    source_dir.mkdir()
    request = dict(package_id=plan.package_id, manifest_sha256=plan.package_manifest_sha256,
        selection_runtime_semantics=dict(normalization_method='zscore'), terminal_weights={'lstm': .7, 'fund': .3})
    feature = dict(recipe=dict(roles={'lstm': 'lstm', 'fund': 'fund'}, terminal_weights=request['terminal_weights']))
    (source_dir/'frozen_request.json').write_text(json.dumps(request), encoding='utf-8')
    (source_dir/'identity.json').write_text(json.dumps(feature), encoding='utf-8')
    scale_rows_fixture()['rankings'].to_parquet(source_dir/'frozen_rankings.parquet', index=False)
    monkeypatch.setattr(pipeline, 'parent_scale_state_implementation_sha256_v1', lambda: plan.implementation_sha256)
    monkeypatch.setattr(pipeline, 'campaign_sources_v2', lambda _: (None, source_dir, None, source_dir, None, {}))
    assert pipeline.parent_scale_state_sources_v1(plan)[1] == source_dir
    for change in ('normalization', 'weights', 'roles', 'package'):
        poisoned = json.loads(json.dumps(request))
        poisoned_feature = json.loads(json.dumps(feature))
        if change == 'normalization':
            poisoned['selection_runtime_semantics']['normalization_method'] = 'rank'
        elif change == 'weights':
            poisoned['terminal_weights']['lstm'] = .5
        elif change == 'roles':
            poisoned_feature['recipe']['roles']['lstm'] = 'new_leg'
        else:
            poisoned['package_id'] = 'other'
        (source_dir/'frozen_request.json').write_text(json.dumps(poisoned), encoding='utf-8')
        (source_dir/'identity.json').write_text(json.dumps(poisoned_feature), encoding='utf-8')
        with pytest.raises(ValueError, match='coordinate'):
            pipeline.parent_scale_state_sources_v1(plan)

    (source_dir/'frozen_request.json').write_text(json.dumps(request), encoding='utf-8')
    (source_dir/'identity.json').write_text(json.dumps(feature), encoding='utf-8')
    wrong_dtype = plan.model_copy(update={'score_dtypes': {**DTYPES, 'raw__lstm': 'float64'}})
    with pytest.raises(ValueError, match='dtype'):
        pipeline.parent_scale_state_sources_v1(wrong_dtype)
    schema = pq.read_schema(source_dir/'frozen_rankings.parquet')
    assert pipeline.score_dtype_map_v1(schema, MAPPING) == DTYPES
    with pytest.raises(ValueError, match='dtype'):
        pipeline.score_dtype_map_v1(pa.schema([(name, pa.int64()) for name in DTYPES]), MAPPING)
