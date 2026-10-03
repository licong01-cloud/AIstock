import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_selection_state_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1, SectorPriceFitV1
from backend.services.advisory_model_first.economic_selection_state_price_v1 import STATE_FEATURES, SelectionStatePricePlanV1, selection_state_fit_identity_v1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
from backend.tests.advisory_model_first.test_economic_price_campaign_models_v2 import rows_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def anchored_fixture(tmp_path):
    original = plan_fixture()
    _, configuration = rows_fixture()
    configuration.label_cutoff = configuration.test_end
    parent = SimpleNamespace(configuration=configuration, experiment_id='parent_fixture', dataset_identity='data_fixture', policy_identity='policy_fixture')
    registered = publish_stage(study_root=tmp_path/original.experiment_id, stage='preregistered', plan_sha256=original.plan_sha256,
        parent_sha256=None, artifacts={'plan.json': _json_bytes(original.model_dump(mode='json'))})
    _record(original, registered.parent, parent, 'PREREGISTERED', registered/'manifest.json')
    plan = SelectionStatePricePlanV1(**original.model_dump(exclude={'schema_version', 'campaign_id', 'model_id'}),
        budget_anchor_ref=evidence_reference_for_file(registered/'manifest.json', role='price_campaign_budget_anchor'))
    events = []
    for model, count in (('M2', 4), ('M3', 5), ('M4', 2)):
        study = plan_fixture(model)
        if model != 'M2':
            path = publish_stage(study_root=tmp_path/study.experiment_id, stage='preregistered', plan_sha256=study.plan_sha256,
                parent_sha256=None, artifacts={'plan.json': _json_bytes(study.model_dump(mode='json'))})
            _record(study, path.parent, parent, 'PREREGISTERED', path/'manifest.json')
        events += [dict(kind='PHYSICAL_FIT', campaign_id=study.campaign_id, model_id=model, state='STARTED', experiment_id=study.experiment_id, head=str(head)) for head in range(count)]
    events.append(dict(kind='INDEX_BUILD', campaign_id=original.campaign_id, model_id='M4', state='STARTED', experiment_id=study.experiment_id, head='local_index'))
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    return plan, parent, events


def test_original_anchor_and_journal_cannot_be_reset_or_relocated(tmp_path):
    plan, _, _ = anchored_fixture(tmp_path)
    root, events = pipeline.verify_selection_budget_anchor_v1(plan)
    assert root == tmp_path and sum(event['kind'] == 'PHYSICAL_FIT' for event in events) == 11
    with pytest.raises(ValueError, match='reset'):
        pipeline._selection_root(plan, tmp_path/'new_root')
    (tmp_path/'campaign_fit_journal.jsonl').write_text('', encoding='utf-8')
    with pytest.raises(ValueError, match='journal'):
        pipeline.verify_selection_budget_anchor_v1(plan)


@pytest.mark.parametrize('navigation', ['STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION', 'CONSIDER_CONFIRMATION_DESIGN_ONLY'])
def test_terminal_predecessor_requires_real_chain_and_rejects_positive_navigation(tmp_path, navigation):
    plan, parent, events = anchored_fixture(tmp_path)
    with pytest.raises(ValueError, match='terminal M1'):
        pipeline.verify_selection_predecessor_v1(plan, tmp_path, events)
    crosswalk = tmp_path/'crosswalk.json'
    crosswalk.write_text('{}', encoding='utf-8')
    old = SectorPricePlanV1(**plan_fixture().model_dump(exclude={'schema_version', 'model_id'}),
        crosswalk_ref=evidence_reference_for_file(crosswalk, role='sector_structural_crosswalk'))
    root, previous = tmp_path/old.experiment_id, None
    report = dict(plan_sha256=old.plan_sha256, decision_use='NAVIGATION_ONLY', deployable=False, sealed_accessed=False,
        navigation=navigation)
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(old.model_dump(mode='json'))} if stage == 'preregistered' else {'evaluation.json': _json_bytes(report)} if stage == 'evaluated' else {'unit.json': b'{}'}
        path = publish_stage(study_root=root, stage=stage, plan_sha256=old.plan_sha256, parent_sha256=previous, artifacts=artifacts)
        previous = read_stage(path, stage=stage, plan_sha256=old.plan_sha256, parent_sha256=previous)['stage_sha256']
    _record(old, root, parent, 'EVALUATED', root/'evaluated/manifest.json')
    events += [dict(kind='PHYSICAL_FIT', model_id='M1', experiment_id=old.experiment_id) for _ in range(4)]
    if navigation == 'CONSIDER_CONFIRMATION_DESIGN_ONLY':
        with pytest.raises(ValueError, match='positive'):
            pipeline.verify_selection_predecessor_v1(plan, tmp_path, events)
    else:
        assert pipeline.verify_selection_predecessor_v1(plan, tmp_path, events) == old.experiment_id


def test_shared_M5_budget19_partial_fit_and_QE_unknown_refusal(tmp_path, monkeypatch):
    plan, parent, events = anchored_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    publish_stage(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=manifest['stage_sha256'], artifacts={'unit.json': b'{}'})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    events += [dict(kind='PHYSICAL_FIT', model_id='M1') for _ in range(4)]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    for index in range(4):
        pipeline._selection_fit_event(plan, root, str(index))
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._selection_fit_event(plan, root, 'extra')
    monkeypatch.setattr(pipeline, 'load_selection_state_v1', lambda **_: (plan, root, manifest, (SimpleNamespace(configuration=None),)))
    (root/'fit_attempt.json').write_text('{}', encoding='utf-8')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_selection_state_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_selection_state_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)


def test_actual_M5_query_consumes_T_open_not_future_close_and_preserves_unknown():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    original = sector_fit_fixture()
    recipe = dict(d_features=list(D_FEATURES), selection_features=list(STATE_FEATURES))
    fitted = SectorPriceFitV1(recipe, original.models, original.support, original.diagnostics,
        selection_state_fit_identity_v1(recipe, original.models, original.support))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, .0), **dict(zip(STATE_FEATURES, (.4, .6, 1.), strict=True)),
        'feature_visible_through': key[KEY[0]], 'state_feature_visible_through': key[KEY[0]]}])
    args = dict(fitted=fitted, candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.selection_state_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[STATE_FEATURES[0]] = None
    assert pipeline.selection_state_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs['state_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.selection_state_actual_decisions_v1(**args)
