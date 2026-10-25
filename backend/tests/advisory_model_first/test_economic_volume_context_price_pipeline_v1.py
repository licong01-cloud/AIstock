import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_volume_context_price_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.economic_volume_context_price_v1 import VOLUME_CONTEXT_FEATURES, VolumeContextPricePlanV1, volume_context_requests_v1
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_market_risk_price_pipeline_v1 import market_risk_plan_fixture
from backend.tests.advisory_model_first.test_economic_volume_context_price_v1 import volume_context_fit_fixture, volume_context_fixture


def volume_context_plan_fixture(tmp_path):
    previous, path_plan, original, parent, events = market_risk_plan_fixture(tmp_path)
    ancestor = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(previous.model_dump(mode='json'))} if stage == 'preregistered' else {'unit.json': b'{}'}
        path = publish_stage(study_root=tmp_path/previous.experiment_id, stage=stage, plan_sha256=previous.plan_sha256, parent_sha256=ancestor, artifacts=artifacts)
        ancestor = read_stage(path, stage=stage, plan_sha256=previous.plan_sha256, parent_sha256=ancestor)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(previous, path.parent, parent, stage.upper(), path/'manifest.json')
    events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=previous.model_id, campaign_id=previous.campaign_id,
        experiment_id=previous.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    plan = VolumeContextPricePlanV1(**previous.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(path/'manifest.json', role='volume_context_predecessor'))
    return plan, previous, path_plan, original, parent, events


@pytest.mark.parametrize('defect', ['none', 'missing', 'duplicate', 'foreign_symbol', 'schema', 'timeout'])
def test_exact_volume_pairs_one_SELECT_real_readonly_session_rollback_close(monkeypatch, defect):
    import backend.db.pg_pool as pool
    args = volume_context_fixture()
    args['candidates'] = args['candidates'].iloc[:1]
    pairs = volume_context_requests_v1(candidates=args['candidates'], calendar=args['calendar'])
    rows = [(day.date(), symbol, 100) for day, symbol in pairs]
    if defect == 'missing':
        rows = rows[1:]
    elif defect == 'duplicate':
        rows[-1] = rows[0]
    elif defect == 'foreign_symbol':
        rows[0] = (rows[0][0], '000002.SZ', 100)
    cursor, connection = MagicMock(), MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor
    cursor.description = [SimpleNamespace(name=name) for name in ('trade_date', 'instrument', 'wrong' if defect == 'schema' else 'volume_hand')]
    cursor.fetchall.return_value = rows
    if defect == 'timeout':
        def fail_select(sql, params=None):
            if sql.lstrip().startswith('SELECT'):
                raise TimeoutError('bounded readonly source timeout')
        cursor.execute.side_effect = fail_select
    monkeypatch.setattr(pool, '_db_cfg', lambda: dict(dbname='unit', user='readonly-unit', host='unit.invalid'))
    session = BoundedEntryReadSession(EntryWorkBudget(30), connector=lambda **_: connection)
    if defect in ('none', 'missing'):
        frame, receipt = pipeline.load_volume_context_v1(candidates=args['candidates'], calendar=args['calendar'], session_factory=lambda: session)
        assert len(frame) == len(rows) and receipt['selects'] == 1 and receipt['requested_pairs'] == len(pairs)
        assert receipt['native_identity'] == 'UNPROVEN' and not receipt['database_written']
    else:
        with pytest.raises((TimeoutError, ValueError, AdvisoryModelFirstError)):
            pipeline.load_volume_context_v1(candidates=args['candidates'], calendar=args['calendar'], session_factory=lambda: session)
    connection.set_session.assert_called_once_with(isolation_level='REPEATABLE READ', readonly=True, autocommit=False)
    selects = [call.args for call in cursor.execute.call_args_list if call.args[0].lstrip().startswith('SELECT')]
    assert len(selects) == 1 and 'JOIN unnest' in selects[0][0] and 'trade_date BETWEEN %s AND %s' in selects[0][0]
    assert selects[0][1] == ([day.date() for day, _ in pairs], [symbol for _, symbol in pairs], pairs[0][0].date(), pairs[-1][0].date(), len(pairs)+1)
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_prepare_snapshot_retry_no_requery_and_partial_fit_QE_guard(tmp_path, monkeypatch):
    plan, _, _, _, parent, _ = volume_context_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = volume_context_fixture()
    candidates = args['candidates'].assign(selection_effective_rank=1, is_candidate_decision=True)
    frozen, reusable = tmp_path/'frozen', tmp_path/'reusable'
    frozen.mkdir()
    reusable.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates[KEY].to_parquet(reusable/'rows.parquet', index=False)
    args['prices'].to_parquet(frozen/'raw_daily.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_volume_context_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, reusable, {})))
    monkeypatch.setattr(pipeline, 'load_volume_context_v1', lambda **_: (args['volumes'], {'selects': 1}))
    prepared = pipeline.prepare_volume_context_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    result = pd.read_parquet(prepared/'rows.parquet')
    assert result[KEY].equals(candidates[KEY]) and result.volume_context_feature_status.tolist() == ['UNKNOWN_20D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    monkeypatch.setattr(pipeline, 'load_volume_context_v1', lambda **_: pytest.fail('published snapshot must not requery'))
    assert pipeline.prepare_volume_context_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_volume_context_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_volume_context_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'volume_daily.parquet').write_bytes(b'corrupted')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_volume_context_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M9_actual_decision_uses_explicit_D_clock_not_future_T_close():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(VOLUME_CONTEXT_FEATURES, (.05, .1, .08), strict=True)),
        'feature_visible_through': key[KEY[0]], 'volume_context_feature_visible_through': key[KEY[0]]}])
    args = dict(fitted=volume_context_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.volume_context_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[VOLUME_CONTEXT_FEATURES[0]] = None
    assert pipeline.volume_context_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs['volume_context_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.volume_context_actual_decisions_v1(**args)
