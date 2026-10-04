import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_moneyflow_price_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_moneyflow_price_source_v1 import MoneyflowSourceBatchV1
from backend.services.advisory_model_first.economic_moneyflow_price_v1 import MONEYFLOW_FEATURES, MoneyflowPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_moneyflow_price_v1 import flow_fixture, moneyflow_fit_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_pipeline_v1 import sector_plan_fixture
from backend.tests.advisory_model_first.test_economic_selection_state_pipeline_v1 import anchored_fixture


def moneyflow_plan_fixture(tmp_path):
    prior, parent, events = anchored_fixture(tmp_path)
    sector = sector_plan_fixture(tmp_path)
    path = publish_stage(study_root=tmp_path/sector.experiment_id, stage='preregistered', plan_sha256=sector.plan_sha256,
        parent_sha256=None, artifacts={'plan.json': _json_bytes(sector.model_dump(mode='json'))})
    _record(sector, path.parent, parent, 'PREREGISTERED', path/'manifest.json')
    previous = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(prior.model_dump(mode='json'))} if stage == 'preregistered' else {'unit.json': b'{}'}
        path = publish_stage(study_root=tmp_path/prior.experiment_id, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=previous, artifacts=artifacts)
        previous = read_stage(path, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=previous)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(prior, path.parent, parent, stage.upper(), path/'manifest.json')
    for plan in (sector, prior):
        events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=plan.model_id, campaign_id=plan.campaign_id,
            experiment_id=plan.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    plan = MoneyflowPricePlanV1(**prior.model_dump(exclude={'schema_version', 'campaign_id', 'model_id'}),
        predecessor_manifest_ref=evidence_reference_for_file(path/'manifest.json', role='moneyflow_predecessor'))
    return plan, parent, events


def test_budget_actual19_plus_four_cannot_reset_foreign_or_partial(tmp_path, monkeypatch):
    plan, parent, events = moneyflow_plan_fixture(tmp_path)
    assert pipeline.verify_moneyflow_budget_v1(plan) == tmp_path
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    publish_stage(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=manifest['stage_sha256'], artifacts={'unit.json': b'{}'})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    with pytest.raises(ValueError, match='reset'):
        pipeline._moneyflow_root(plan, tmp_path/'new_root')
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            pipeline._moneyflow_fit_event(plan, root, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(plan) == tmp_path
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._moneyflow_fit_event(plan, root, 'extra')
    monkeypatch.setattr(pipeline, 'load_moneyflow_study_v1', lambda **_: (plan, root, manifest, (SimpleNamespace(configuration=None),)))
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_moneyflow_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_moneyflow_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    journal = tmp_path/'campaign_fit_journal.jsonl'
    journal.write_text('', encoding='utf-8')
    with pytest.raises(ValueError, match='journal'):
        pipeline.verify_moneyflow_budget_v1(plan)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in [*events, dict(kind='PHYSICAL_FIT', model_id='FOREIGN', state='STARTED')]), encoding='utf-8')
    with pytest.raises(ValueError, match='foreign'):
        pipeline.verify_moneyflow_budget_v1(plan)


def test_prepare_freezes_raw_snapshot_and_exact_retry_does_not_requery(tmp_path, monkeypatch):
    plan, parent, _ = moneyflow_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = flow_fixture()
    candidates = args['candidates'].copy()
    candidates['selection_effective_rank'] = 1
    candidates['is_candidate_decision'] = True
    frozen, reusable = tmp_path/'frozen', tmp_path/'reusable'
    frozen.mkdir()
    reusable.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates.loc[:, KEY].to_parquet(reusable/'rows.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates))
    source = (parent, frozen, identity, None, reusable, {})
    monkeypatch.setattr(pipeline, 'load_moneyflow_study_v1', lambda **_: (plan, root, manifest, source))
    batch = MoneyflowSourceBatchV1(args['amounts'], args['calendar'], {'database_written': False})
    monkeypatch.setattr(pipeline, 'load_moneyflow_source_v1', lambda **_: batch)
    prepared = pipeline.prepare_moneyflow_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    rows = pd.read_parquet(prepared/'rows.parquet')
    assert rows[KEY].equals(candidates[KEY]) and rows.moneyflow_feature_status.tolist() == ['UNKNOWN_5D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    assert (prepared/'amounts_cny.parquet').is_file() and (prepared/'source_calendar.json').is_file()
    monkeypatch.setattr(pipeline, 'load_moneyflow_source_v1', lambda **_: pytest.fail('published snapshot must not requery'))
    assert pipeline.prepare_moneyflow_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (prepared/'source_calendar.json').write_bytes(b'[]')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_moneyflow_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_actual_M6_query_consumes_T_open_not_future_HLC_and_preserves_unknown():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(MONEYFLOW_FEATURES, (.4, .6, .2), strict=True)),
        'feature_visible_through': key[KEY[0]], 'moneyflow_feature_visible_through': key[KEY[0]]}])
    args = dict(fitted=moneyflow_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.moneyflow_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[MONEYFLOW_FEATURES[0]] = None
    assert pipeline.moneyflow_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs['moneyflow_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.moneyflow_actual_decisions_v1(**args)
