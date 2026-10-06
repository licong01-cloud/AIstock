import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_market_risk_price_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_market_risk_price_v1 import MARKET_RISK_FEATURES, MarketRiskPricePlanV1
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_market_risk_price_v1 import market_risk_fit_fixture, market_risk_fixture
from backend.tests.advisory_model_first.test_economic_price_path_pipeline_v1 import price_path_plan_fixture


def market_risk_plan_fixture(tmp_path):
    previous, original, parent, events = price_path_plan_fixture(tmp_path)
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
    plan = MarketRiskPricePlanV1(**previous.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(path/'manifest.json', role='market_risk_predecessor'))
    return plan, previous, original, parent, events


@pytest.mark.parametrize('defect', ['none', 'missing', 'duplicate', 'foreign_date', 'schema', 'timeout'])
def test_one_index_SELECT_uses_real_readonly_session_and_always_rollback_close(monkeypatch, defect):
    import backend.db.pg_pool as pool
    days, calls = pd.bdate_range('2025-01-02', periods=3), []
    rows = [(day.date(), '000300.SH', 100.) for day in days]
    if defect == 'duplicate':
        rows += [rows[0]]
    if defect == 'foreign_date':
        rows[0] = ((days[0]-pd.Timedelta(days=1)).date(), '000300.SH', 100.)
    if defect == 'missing':
        rows = rows[1:]

    class Cursor:
        description = [SimpleNamespace(name=name) for name in ('trade_date', 'instrument', 'wrong' if defect == 'schema' else 'close')]
        def execute(self, sql, params=None):
            calls.append((sql, params))
            if sql.lstrip().startswith('SELECT') and defect == 'timeout':
                raise TimeoutError('bounded source timeout')
        def fetchall(self):
            return rows
        def close(self):
            calls.append(('cursor_close',))

    class Connection:
        closed = False
        def set_session(self, **kwargs):
            calls.append(('set_session', kwargs))
        def cursor(self):
            return Cursor()
        def rollback(self):
            calls.append(('rollback',))
        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(pool, '_db_cfg', lambda: dict(dbname='unit', user='readonly-unit', host='unit.invalid'))
    session = BoundedEntryReadSession(EntryWorkBudget(30), connector=lambda **_: connection)
    if defect in ('none', 'missing'):
        result, receipt = pipeline.load_market_risk_index_v1(dates=days, session_factory=lambda: session)
        assert len(result) == len(rows) and receipt['selects'] == 1 and receipt['native_identity'] == 'UNPROVEN'
    else:
        with pytest.raises((TimeoutError, ValueError, AdvisoryModelFirstError)):
            pipeline.load_market_risk_index_v1(dates=days, session_factory=lambda: session)
    assert calls[0] == ('set_session', dict(isolation_level='REPEATABLE READ', readonly=True, autocommit=False))
    selects = [(sql, args) for sql, *rest in calls if sql.lstrip().startswith('SELECT') for args in rest]
    assert len(selects) == 1 and 'trade_date BETWEEN %s AND %s' in selects[0][0]
    assert selects[0][1][2:4] == (days[0].date(), days[-1].date())
    assert ('rollback',) in calls and connection.closed


def test_prepare_freezes_index_snapshot_and_retry_does_not_requery(tmp_path, monkeypatch):
    plan, _, _, parent, _ = market_risk_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = market_risk_fixture()
    candidates = args['candidates'].assign(selection_effective_rank=1, is_candidate_decision=True)
    frozen, reusable = tmp_path/'frozen', tmp_path/'reusable'
    frozen.mkdir()
    reusable.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    candidates.loc[:, KEY].to_parquet(reusable/'rows.parquet', index=False)
    args['prices'].to_parquet(frozen/'raw_daily.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_market_risk_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, reusable, {})))
    monkeypatch.setattr(pipeline, 'load_market_risk_index_v1', lambda **_: (args['benchmark'], {'selects': 1}))
    prepared = pipeline.prepare_market_risk_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    result = pd.read_parquet(prepared/'rows.parquet')
    assert result[KEY].equals(candidates[KEY]) and result.market_risk_feature_status.tolist() == ['UNKNOWN_20D_WARMUP', 'AVAILABLE', 'AVAILABLE']
    monkeypatch.setattr(pipeline, 'load_market_risk_index_v1', lambda **_: pytest.fail('published snapshot must not requery'))
    assert pipeline.prepare_market_risk_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (prepared/'index_close.parquet').write_bytes(b'corrupted')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_market_risk_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_actual_T_open_only_market_feature_clock_and_partial_QE_guard(tmp_path, monkeypatch):
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict(zip(MARKET_RISK_FEATURES, (150., -.1, 1.), strict=True)),
        'feature_visible_through': key[KEY[0]], 'market_risk_feature_visible_through': key[KEY[0]]}])
    args = dict(fitted=market_risk_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999.,
            suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.market_risk_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[MARKET_RISK_FEATURES[0]] = None
    assert pipeline.market_risk_actual_decisions_v1(**args).model_action.tolist() == ['UNAVAILABLE']
    inputs['market_risk_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.market_risk_actual_decisions_v1(**args)
    plan, _, _, parent, _ = market_risk_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    publish_stage(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=manifest['stage_sha256'], artifacts={'unit.json': b'{}'})
    _record(plan, root, parent, 'PREPARED', root/'prepared/manifest.json')
    monkeypatch.setattr(pipeline, 'load_market_risk_study_v1', lambda **_: (plan, root, manifest, (SimpleNamespace(configuration=None),)))
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_market_risk_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_market_risk_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
