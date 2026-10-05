import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_price_path_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.economic_price_path_value_v1 import PRICE_PATH_FEATURES, PricePathPlanV1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_moneyflow_price_pipeline_v1 import moneyflow_plan_fixture
from backend.tests.advisory_model_first.test_economic_price_path_value_v1 import price_path_fit_fixture, price_path_fixture


def price_path_plan_fixture(tmp_path):
    prior, parent, events = moneyflow_plan_fixture(tmp_path)
    previous = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(prior.model_dump(mode='json'))} if stage == 'preregistered' else {'unit.json': b'{}'}
        path = publish_stage(study_root=tmp_path/prior.experiment_id, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=previous, artifacts=artifacts)
        previous = read_stage(path, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=previous)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(prior, path.parent, parent, stage.upper(), path/'manifest.json')
    events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=prior.model_id, campaign_id=prior.campaign_id,
        experiment_id=prior.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    plan = PricePathPlanV1(**prior.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(path/'manifest.json', role='price_path_predecessor'))
    return plan, prior, parent, events


def test_prepare_original_snapshot_preserves_all_keys_and_exact_retry(tmp_path, monkeypatch):
    plan, _, parent, _ = price_path_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = price_path_fixture()
    candidates = args['candidates'].copy()
    candidates['selection_effective_rank'] = 1
    candidates['is_candidate_decision'] = True
    frozen, reusable = tmp_path/'frozen', tmp_path/'reusable'
    frozen.mkdir()
    reusable.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates.loc[:, KEY].to_parquet(reusable/'rows.parquet', index=False)
    args['prices'].to_parquet(frozen/'raw_daily.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    source = (parent, frozen, identity, None, reusable, {})
    monkeypatch.setattr(pipeline, 'load_price_path_study_v1', lambda **_: (plan, root, manifest, source))
    prepared = pipeline.prepare_price_path_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    rows = pd.read_parquet(prepared/'rows.parquet')
    assert rows[KEY].equals(candidates[KEY]) and rows.price_path_feature_status.tolist() == ['UNKNOWN_20D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    monkeypatch.setattr(pipeline, 'price_path_rows_v1', lambda **_: pytest.fail('published snapshot must not recompute'))
    assert pipeline.prepare_price_path_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (prepared/'preparation.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_price_path_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_actual_query_uses_T_open_not_HLC_and_path_clock_is_strict():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(PRICE_PATH_FEATURES, (-.1, .5, .6), strict=True)),
        'feature_visible_through': key[KEY[0]], 'price_path_feature_visible_through': key[KEY[0]]}])
    args = dict(fitted=price_path_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.price_path_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[PRICE_PATH_FEATURES[0]] = None
    assert pipeline.price_path_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs['price_path_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.price_path_actual_decisions_v1(**args)


def test_partial_fit_and_unknown_qe_cannot_trigger_fit(tmp_path, monkeypatch):
    plan, _, parent, _ = price_path_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    publish_stage(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=manifest['stage_sha256'], artifacts={'unit.json': b'{}'})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    monkeypatch.setattr(pipeline, 'load_price_path_study_v1', lambda **_: (plan, root, manifest, (SimpleNamespace(configuration=None),)))
    monkeypatch.setattr(pipeline, 'train_price_path_v1', lambda **_: pytest.fail('must not fit'))
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_price_path_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_price_path_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
