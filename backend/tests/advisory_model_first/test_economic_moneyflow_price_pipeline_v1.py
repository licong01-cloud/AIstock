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


def test_explicit_M7_extension27_keeps_M6_default23_and_requires_terminal_identity(tmp_path):
    from backend.services.advisory_model_first.economic_price_path_pipeline_v1 import _price_path_fit_event
    from backend.tests.advisory_model_first.test_economic_price_path_pipeline_v1 import price_path_plan_fixture
    extension, prior, _, events = price_path_plan_fixture(tmp_path)
    assert pipeline.verify_moneyflow_budget_v1(prior) == tmp_path
    assert pipeline.verify_moneyflow_budget_v1(prior, price_path_extension=extension) == tmp_path
    (tmp_path/extension.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _price_path_fit_event(extension, tmp_path/extension.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(prior, price_path_extension=extension) == tmp_path
    with pytest.raises(ValueError, match='foreign'):
        pipeline.verify_moneyflow_budget_v1(prior)
    with pytest.raises(ValueError, match='cumulative'):
        _price_path_fit_event(extension, tmp_path/extension.experiment_id, 'extra')
    journal = tmp_path/'campaign_fit_journal.jsonl'
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events[:-1]), encoding='utf-8')
    with pytest.raises(ValueError, match='four completed'):
        pipeline.verify_moneyflow_budget_v1(prior, price_path_extension=extension)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    foreign = extension.model_copy(update={'profile_sha256': 'f'*64})
    with pytest.raises(ValueError, match='root/source'):
        pipeline.verify_moneyflow_budget_v1(prior, price_path_extension=foreign)
    (tmp_path/prior.experiment_id/'evaluated/unit.json').write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.verify_moneyflow_budget_v1(prior, price_path_extension=extension)


def test_explicit_M8_extension31_keeps_old_defaults_and_original27_terminal(tmp_path):
    from backend.services.advisory_model_first.economic_market_risk_price_pipeline_v1 import _market_risk_fit_event
    from backend.tests.advisory_model_first.test_economic_market_risk_price_pipeline_v1 import market_risk_plan_fixture
    newest, previous, original, _, events = market_risk_plan_fixture(tmp_path)
    assert pipeline.verify_moneyflow_budget_v1(original, price_path_extension=previous) == tmp_path
    with pytest.raises(ValueError, match='requires original explicit'):
        pipeline.verify_moneyflow_budget_v1(original, market_risk_extension=newest)
    (tmp_path/newest.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _market_risk_fit_event(newest, tmp_path/newest.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=newest) == tmp_path
    with pytest.raises(ValueError, match='foreign'):
        pipeline.verify_moneyflow_budget_v1(original, price_path_extension=previous)
    with pytest.raises(ValueError, match='cumulative'):
        _market_risk_fit_event(newest, tmp_path/newest.experiment_id, 'extra')
    journal = tmp_path/'campaign_fit_journal.jsonl'
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events[:-1]), encoding='utf-8')
    with pytest.raises(ValueError, match='four completed M7'):
        pipeline.verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=newest)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    foreign = newest.model_copy(update={'profile_sha256': 'f'*64})
    with pytest.raises(ValueError, match='root/source'):
        pipeline.verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=foreign)
    (tmp_path/previous.experiment_id/'evaluated/unit.json').write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.verify_moneyflow_budget_v1(original, price_path_extension=previous, market_risk_extension=newest)


def test_explicit_M9_extension35_requires_original31_and_preserves_old_caps(tmp_path):
    from backend.services.advisory_model_first.economic_volume_context_price_pipeline_v1 import _volume_context_fit_event
    from backend.tests.advisory_model_first.test_economic_volume_context_price_pipeline_v1 import volume_context_plan_fixture
    newest, market, path, original, _, _ = volume_context_plan_fixture(tmp_path)
    kwargs = dict(price_path_extension=path, market_risk_extension=market, volume_context_extension=newest)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='explicit M8'):
        pipeline.verify_moneyflow_budget_v1(original, price_path_extension=path, volume_context_extension=newest)
    (tmp_path/newest.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _volume_context_fit_event(newest, tmp_path/newest.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    for old_kwargs in ({}, dict(price_path_extension=path), dict(price_path_extension=path, market_risk_extension=market)):
        with pytest.raises(ValueError, match='foreign'):
            pipeline.verify_moneyflow_budget_v1(original, **old_kwargs)
    with pytest.raises(ValueError):
        _volume_context_fit_event(newest, tmp_path/newest.experiment_id, 'extra')
    foreign = newest.model_copy(update={'profile_sha256': 'f'*64})
    with pytest.raises(ValueError, match='source'):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'volume_context_extension': foreign})
    journal = tmp_path/'campaign_fit_journal.jsonl'
    events = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()]
    assert sum(event['kind'] == 'PHYSICAL_FIT' for event in events) == 35
    partial = [event for event in events if not (event['model_id'] == 'M8' and event['head'] == 'candidate_path')]
    journal.write_text(''.join(json.dumps(event)+'\n' for event in partial), encoding='utf-8')
    with pytest.raises(ValueError, match='actual four completed M8'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)


def test_explicit_M10_extension39_needs_original35_and_keeps_old_defaults(tmp_path):
    from backend.services.advisory_model_first.economic_breadth_state_price_pipeline_v1 import _breadth_state_fit_event
    from backend.tests.advisory_model_first.test_economic_breadth_state_price_pipeline_v1 import breadth_state_plan_fixture
    newest, volume, market, path, original, _, events = breadth_state_plan_fixture(tmp_path)
    kwargs = dict(price_path_extension=path, market_risk_extension=market, volume_context_extension=volume, breadth_state_extension=newest)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='explicit M9'):
        pipeline.verify_moneyflow_budget_v1(original, price_path_extension=path, market_risk_extension=market, breadth_state_extension=newest)
    (tmp_path/newest.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _breadth_state_fit_event(newest, tmp_path/newest.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    for old_kwargs in ({}, dict(price_path_extension=path), dict(price_path_extension=path, market_risk_extension=market),
                       dict(price_path_extension=path, market_risk_extension=market, volume_context_extension=volume)):
        with pytest.raises(ValueError, match='foreign'):
            pipeline.verify_moneyflow_budget_v1(original, **old_kwargs)
    with pytest.raises(ValueError, match='cumulative'):
        _breadth_state_fit_event(newest, tmp_path/newest.experiment_id, 'extra')
    with pytest.raises(ValueError):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'breadth_state_extension': newest.model_copy(update={'model_id': 'M9'})})
    with pytest.raises(ValueError, match='source'):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'breadth_state_extension': newest.model_copy(update={'profile_sha256': 'f'*64})})
    journal = tmp_path/'campaign_fit_journal.jsonl'
    current = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()]
    assert sum(event['kind'] == 'PHYSICAL_FIT' for event in current) == 39
    partial = [event for event in current if not (event['model_id'] == 'M9' and event['head'] == 'candidate_path')]
    journal.write_text(''.join(json.dumps(event)+'\n' for event in partial), encoding='utf-8')
    with pytest.raises(ValueError, match='actual four completed M9'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    (tmp_path/volume.experiment_id/'evaluated/unit.json').write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)


def test_explicit_M12_extension47_requires_original43_and_preserves_old_defaults(tmp_path):
    from backend.services.advisory_model_first.economic_session_path_pipeline_v1 import _session_path_fit_event
    from backend.tests.advisory_model_first.test_economic_session_path_pipeline_v1 import session_path_plan_fixture
    newest, traded, breadth, volume, market, path, original, _, events = session_path_plan_fixture(tmp_path)
    old_kwargs = dict(price_path_extension=path, market_risk_extension=market, volume_context_extension=volume, breadth_state_extension=breadth, traded_price_extension=traded)
    kwargs = dict(**old_kwargs, session_path_extension=newest)
    assert pipeline.verify_moneyflow_budget_v1(original, **old_kwargs) == tmp_path
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='explicit M11'):
        pipeline.verify_moneyflow_budget_v1(original, session_path_extension=newest)
    (tmp_path/newest.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _session_path_fit_event(newest, tmp_path/newest.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='foreign'):
        pipeline.verify_moneyflow_budget_v1(original, **old_kwargs)
    with pytest.raises(ValueError, match='cumulative'):
        _session_path_fit_event(newest, tmp_path/newest.experiment_id, 'extra')
    with pytest.raises(ValueError):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'session_path_extension': newest.model_copy(update={'model_id': 'M11'})})
    with pytest.raises(ValueError, match='source'):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'session_path_extension': newest.model_copy(update={'profile_sha256': 'f'*64})})
    journal = tmp_path/'campaign_fit_journal.jsonl'
    current = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()]
    assert sum(event['kind'] == 'PHYSICAL_FIT' for event in current) == 47
    partial = [event for event in current if not (event['model_id'] == 'M11' and event['head'] == 'candidate_path')]
    journal.write_text(''.join(json.dumps(event)+'\n' for event in partial), encoding='utf-8')
    with pytest.raises(ValueError, match='actual four completed M11'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    (tmp_path/traded.experiment_id/'evaluated/unit.json').write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)


def test_explicit_M13_extension51_requires_original47_and_preserves_old_defaults(tmp_path):
    from backend.services.advisory_model_first.economic_free_float_turnover_pipeline_v1 import _free_float_turnover_fit_event
    from backend.tests.advisory_model_first.test_economic_free_float_turnover_pipeline_v1 import free_float_plan_fixture
    newest, session, traded, breadth, volume, market, path, original, _, events = free_float_plan_fixture(tmp_path)
    old_kwargs = dict(price_path_extension=path, market_risk_extension=market, volume_context_extension=volume, breadth_state_extension=breadth, traded_price_extension=traded, session_path_extension=session)
    kwargs = dict(**old_kwargs, free_float_extension=newest)
    assert pipeline.verify_moneyflow_budget_v1(original, **old_kwargs) == tmp_path
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='explicit M12'):
        pipeline.verify_moneyflow_budget_v1(original, free_float_extension=newest)
    (tmp_path/newest.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _free_float_turnover_fit_event(newest, tmp_path/newest.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='foreign'):
        pipeline.verify_moneyflow_budget_v1(original, **old_kwargs)
    with pytest.raises(ValueError, match='cumulative'):
        _free_float_turnover_fit_event(newest, tmp_path/newest.experiment_id, 'extra')
    with pytest.raises(ValueError):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'free_float_extension': newest.model_copy(update={'model_id': 'M12'})})
    with pytest.raises(ValueError, match='source'):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'free_float_extension': newest.model_copy(update={'profile_sha256': 'f'*64})})
    journal = tmp_path/'campaign_fit_journal.jsonl'
    current = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()]
    assert sum(event['kind'] == 'PHYSICAL_FIT' for event in current) == 51
    partial = [event for event in current if not (event['model_id'] == 'M12' and event['head'] == 'candidate_path')]
    journal.write_text(''.join(json.dumps(event)+'\n' for event in partial), encoding='utf-8')
    with pytest.raises(ValueError, match='actual four completed M12'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    (tmp_path/session.experiment_id/'evaluated/unit.json').write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)



def test_explicit_M11_extension43_requires_original39_and_preserves_old_caps(tmp_path):
    from backend.services.advisory_model_first.economic_traded_price_distribution_pipeline_v1 import _traded_price_fit_event
    from backend.tests.advisory_model_first.test_economic_traded_price_distribution_pipeline_v1 import traded_price_plan_fixture
    newest, breadth, volume, market, path, original, _, events = traded_price_plan_fixture(tmp_path)
    old_kwargs = dict(price_path_extension=path, market_risk_extension=market, volume_context_extension=volume, breadth_state_extension=breadth)
    kwargs = dict(**old_kwargs, traded_price_extension=newest)
    assert pipeline.verify_moneyflow_budget_v1(original, **old_kwargs) == tmp_path
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='explicit M10'):
        pipeline.verify_moneyflow_budget_v1(original, traded_price_extension=newest)
    (tmp_path/newest.experiment_id).mkdir()
    for arm in ('matched', 'candidate'):
        for head in ('mean', 'path'):
            _traded_price_fit_event(newest, tmp_path/newest.experiment_id, arm+'_'+head)
    assert pipeline.verify_moneyflow_budget_v1(original, **kwargs) == tmp_path
    with pytest.raises(ValueError, match='foreign'):
        pipeline.verify_moneyflow_budget_v1(original, **old_kwargs)
    with pytest.raises(ValueError, match='cumulative'):
        _traded_price_fit_event(newest, tmp_path/newest.experiment_id, 'extra')
    with pytest.raises(ValueError):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'traded_price_extension': newest.model_copy(update={'model_id': 'M10'})})
    with pytest.raises(ValueError, match='source'):
        pipeline.verify_moneyflow_budget_v1(original, **{**kwargs, 'traded_price_extension': newest.model_copy(update={'profile_sha256': 'f'*64})})
    journal = tmp_path/'campaign_fit_journal.jsonl'
    current = [json.loads(line) for line in journal.read_text(encoding='utf-8').splitlines()]
    assert sum(event['kind'] == 'PHYSICAL_FIT' for event in current) == 43
    partial = [event for event in current if not (event['model_id'] == 'M10' and event['head'] == 'candidate_path')]
    journal.write_text(''.join(json.dumps(event)+'\n' for event in partial), encoding='utf-8')
    with pytest.raises(ValueError, match='actual four completed M10'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)
    journal.write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    (tmp_path/breadth.experiment_id/'evaluated/unit.json').write_bytes(b'{"tampered":true}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.verify_moneyflow_budget_v1(original, **kwargs)
