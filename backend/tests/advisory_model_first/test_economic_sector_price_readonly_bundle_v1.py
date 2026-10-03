from dataclasses import replace
import hashlib
import json

import pytest

from backend.services.advisory_model_first import economic_sector_price_readonly_bundle_v1 as reader
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryStudyPlanV1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1, sector_price_set_v1
from backend.services.advisory_model_first.research_control import build_trial_record, evidence_reference_for_file


def _study(tmp_path, mode=None):
    def write(path, body):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(body, sort_keys=True), encoding='utf-8')
        return dict(sha256=hashlib.sha256(path.read_bytes()).hexdigest(), size_bytes=path.stat().st_size)

    def ref(path, role):
        return evidence_reference_for_file(path, role=role)

    dataset = tmp_path / 'parent/dataset.json'
    write(dataset, dict(package_id='pkg_fixture', program_id='advp_fixture', manifest_sha256='a'*64,
                        policy_dataset_bundle_id='b'*64))
    parent_path = tmp_path / 'parent/plan.json'
    opaque = dict(artifact_uri=str(tmp_path / 'NEVER_OPEN_DATA'), sha256='a'*64, size_bytes=1, role='fixture')
    parent = EconomicEntryStudyPlanV1(configuration=dict(train_start='2024-01-02', train_end='2024-03-01',
        validation_start='2024-03-04', validation_end='2024-03-15', test_start='2024-03-18', test_end='2024-03-29',
        label_cutoff='2024-04-30', downside_budget_bps=800.), dataset_identity='b'*64,
        dataset_manifest_ref=ref(dataset, 'economic_original_policy_dataset_manifest'), feature_ref=opaque,
        window_contract_ref=opaque, policy_identity='c'*64, implementation_sha256='d'*64, parent_lineage=('fixture',))
    write(parent_path, parent.model_dump(mode='json'))
    plan = SectorPricePlanV1(parent_plan_ref=ref(parent_path, 'campaign_parent_plan'),
        parent_prepared_manifest_ref={**opaque, 'role': 'campaign_prices_snapshot'},
        feature_manifest_ref={**opaque, 'role': 'campaign_d_snapshot'},
        reusable_prepared_manifest_ref={**opaque, 'role': 'campaign_value_labels'},
        crosswalk_ref={**opaque, 'role': 'sector_structural_crosswalk'}, profile_path=str(tmp_path / 'NEVER_OPEN_PROFILE'),
        profile_sha256='e'*64, universe_selection=dict(mode='stock_universe', pool_ids=[]),
        implementation_sha256=reader.sector_implementation_sha256_v1())
    root = tmp_path / plan.experiment_id
    tree = dict(left=[-1], right=[-1], feature=[-2], threshold=[-2.0], value=[0.0])
    models = {arm+'_'+head: dict(kind='gbdt', features=16 if arm == 'candidate' else 13,
        initial=1.02 if head == 'mean' else .99, learning_rate=.05, trees=[tree]*200)
        for arm in ('candidate', 'matched') for head in ('mean', 'path')}
    recipe = dict(d_features=list(reader.D_FEATURES), sector_features=list(reader.SECTOR_FEATURES), common_supervision_sha256='f'*64)
    support = dict(intervals_bps=[[-150.0, -100.0], [0.0, 100.0]])
    preparation = dict(candidate_rows=120, fits=0, native_identity='UNPROVEN', source_evidence='RECOVERED_LIMITED',
                       sealed_accessed=False, database_written=False)
    diagnostic = dict(fitted_head_count=4, index_build_count=0, candidate_count=1, train_rows=100, train_days=20,
                      test_used_for_training_or_calibration=False, decision_use='NAVIGATION_ONLY', deployable=False)
    report = dict(plan_sha256=plan.plan_sha256, model_id='M1', navigation='CONSIDER_CONFIRMATION_DESIGN_ONLY',
        economic_effectiveness='NOT_CONFIRMED', decision_use='NAVIGATION_ONLY', source_evidence='RECOVERED_LIMITED',
        deployable=False, sealed_accessed=False, database_written=False, real_fill_proven=False,
        gates=dict.fromkeys(('interventions', 'mdd', 'model_takes', 'net_increment', 'tail'), True))
    if mode == 'dimension':
        models['candidate_mean']['features'] = 13
    elif mode == 'cycle':
        models['candidate_mean']['trees'] = [dict(left=[0], right=[0], feature=[0], threshold=[0.0], value=[0.0])]*200
    elif mode == 'boolean':
        models['candidate_mean']['initial'] = True
    elif mode == 'order':
        recipe['d_features'].reverse()
    elif mode == 'support':
        support['intervals_bps'][1] = [-120., 100.]
    elif mode == 'qualification':
        preparation['native_identity'] = 'COMPLETE'
    elif mode == 'test_training':
        diagnostic['test_used_for_training_or_calibration'] = True
    fitted_support = reader.ValueAnchorGapSupportV1(((-150., -100.), (0., 100.)))
    metadata = dict(recipe=recipe, models=models, support=support, diagnostics=diagnostic,
        model_sha256=reader.sector_fit_identity_v1(recipe, models, fitted_support), parameters=plan.parameters)
    files = {
        'preregistered': {'plan.json': plan.model_dump(mode='json'), 'recipe.json': plan.parameters,
            'source_receipt.json': dict(implementation_sha256=plan.implementation_sha256),
            'profile_identity.json': dict(profile_sha256=plan.profile_sha256)},
        'prepared': {'preparation.json': preparation}, 'trained': {'metadata.json': metadata},
        'evaluated': {'evaluation.json': report}}
    previous, records = None, []
    for stage, artifacts in files.items():
        desc = {name: write(root/stage/name, body) for name, body in artifacts.items()}
        manifest = dict(schema_version='economic_entry_stage_v1', stage=stage, plan_sha256=plan.plan_sha256,
                        parent_sha256=previous, files=desc)
        manifest['stage_sha256'] = reader.sha(manifest)
        target = root/stage/'manifest.json'
        write(target, manifest)
        previous = manifest['stage_sha256']
        row = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.campaign_id+'_M1', research_stage=stage.upper(),
            study_type=plan.study_type, hypothesis_family_id='economic_entry_price_value',
            parent_lineage=(parent.experiment_id, plan.campaign_id), unique_variable=plan.parameters['hypothesis'],
            objective_contract=plan.objective_contract, dataset_identity=parent.dataset_identity,
            schema_identity=plan.schema_version, policy_identity=reader.sha(dict(parent_source_policy=parent.policy_identity,
                value_policy=plan.parameters['policy_sha256'], cost=reader.COST.policy_sha256)), planned_trial_count=1,
            generated_trial_count=int(stage in ('trained', 'evaluated')), evaluated_trial_count=int(stage == 'evaluated'),
            selected_trial_count=0, result_class='CONTROL_READY' if stage == 'preregistered' else 'EXPLORATORY',
            decision_use='NAVIGATION_ONLY', consumed_windows=(dict(window_id='P0C_DEVELOPMENT_V1', dataset_identity=parent.dataset_identity,
                start_date='2024-01-03' if mode == 'registry_window' else parent.configuration.train_start,
                end_date=parent.configuration.label_cutoff),),
            evidence_refs=(ref(target, 'campaign_'+stage),))
        records.append(row.model_dump(mode='json'))
    (tmp_path/'trial_registry.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in records), encoding='utf-8')
    return dict(plan_ref=ref(root/'preregistered/plan.json', 'sector_readonly_plan'),
                trained_manifest_ref=ref(root/'trained/manifest.json', 'sector_readonly_trained'),
                evaluated_manifest_ref=ref(root/'evaluated/manifest.json', 'sector_readonly_evaluated'))


def test_reader_keeps_research_only_holes_and_does_not_read_data(tmp_path, monkeypatch):
    import pandas as pd
    from backend.services.advisory_model_first import economic_sector_price_pipeline_v1 as pipeline
    refs = _study(tmp_path)
    def forbidden(*args, **kwargs):
        pytest.fail('model reader invoked a data/source/fit path')
    monkeypatch.setattr(pd, 'read_parquet', forbidden)
    monkeypatch.setattr(pipeline, 'sector_sources_v1', forbidden)
    monkeypatch.setattr(pipeline, 'train_sector_price_v1', forbidden)
    loaded = reader.load_sector_research_bundle_v1(**refs)
    assert not loaded.deployable and loaded.native_identity == 'UNPROVEN'
    assert loaded.scope['parent_model_information_end'] == 'UNPROVEN'
    price_set = sector_price_set_v1(fitted=loaded.fitted, d_features=dict.fromkeys((*reader.D_FEATURES, *reader.SECTOR_FEATURES), 0.),
                                  arm='candidate', reference_cny=10., legal_low_cny=9.9, legal_high_cny=10.1)
    assert price_set.intervals_cny and all(not lo < 9.95 < hi for lo, hi in price_set.intervals_cny)
    loaded.verify_unchanged()


@pytest.mark.parametrize('mode', ['dimension', 'cycle', 'boolean', 'order', 'support', 'qualification', 'test_training', 'registry_window'])
def test_frozen_model_or_qualification_changes_fail_closed(tmp_path, mode):
    with pytest.raises(ValueError):
        reader.load_sector_research_bundle_v1(**_study(tmp_path, mode))


@pytest.mark.parametrize('body', [b'{"a":1,"a":2}', b'{"a":1e999}', b'{"a":NaN}', b'[]'])
def test_strict_json(body):
    with pytest.raises(ValueError):
        reader._object(body)


def test_external_pin_budget_and_live_drift(tmp_path, monkeypatch):
    refs = _study(tmp_path)
    wrong = {**refs, 'trained_manifest_ref': refs['trained_manifest_ref'].model_copy(update={'sha256': '0'*64})}
    with pytest.raises(ValueError, match='pinned'):
        reader.load_sector_research_bundle_v1(**wrong)
    loaded = reader.load_sector_research_bundle_v1(**refs)
    with pytest.raises(ValueError):
        replace(loaded, deployable=True).verify_unchanged()
    loaded.scope['universe_selection']['mode'] = 'csi300'
    with pytest.raises(ValueError, match='in-memory'):
        loaded.verify_unchanged()
    loaded = reader.load_sector_research_bundle_v1(**refs)
    loaded.fitted.models['candidate_mean']['initial'] = 1.03
    with pytest.raises(ValueError, match='in-memory'):
        loaded.verify_unchanged()
    monkeypatch.setattr(reader, 'JSON_LIMIT', 10)
    with pytest.raises(ValueError, match='budget'):
        reader.load_sector_research_bundle_v1(**refs)


def test_consumed_bytes_and_original_registry_are_pinned(tmp_path, monkeypatch):
    refs = _study(tmp_path)
    original = reader._read_file
    changed = False
    def race(path, maximum):
        nonlocal changed
        body = original(path, maximum)
        if path.name == 'metadata.json' and not changed:
            changed = True
            path.write_bytes(body+b'\n')
        return body
    monkeypatch.setattr(reader, '_read_file', race)
    with pytest.raises(ValueError, match='source changed'):
        reader.load_sector_research_bundle_v1(**refs)


def test_foreign_stage_and_diagnostic_memory_do_not_become_verified(tmp_path):
    refs = _study(tmp_path)
    with pytest.raises(ValueError, match='another study'):
        reader.load_sector_research_bundle_v1(**{**refs, 'evaluated_manifest_ref': refs['trained_manifest_ref'].model_copy(
            update={'role': 'sector_readonly_evaluated'})})
    loaded = reader.load_sector_research_bundle_v1(**refs)
    loaded.fitted.diagnostics['test_used_for_training_or_calibration'] = True
    with pytest.raises(ValueError, match='in-memory'):
        loaded.verify_unchanged()
