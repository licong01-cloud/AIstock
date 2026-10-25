from dataclasses import asdict
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_sector_moneyflow_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_price_campaign_pipeline_v2 import _record
from backend.services.advisory_model_first.economic_sector_moneyflow_v1 import CLOCK, JOINT_FEATURES, MONEYFLOW_FEATURES, STATUS, SectorMoneyflowPlanV1
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_asymmetric_risk_pipeline_v1 import asymmetric_risk_plan_fixture
from backend.tests.advisory_model_first.test_economic_asymmetric_risk_v1 import asymmetric_risk_fit_fixture
from backend.tests.advisory_model_first.test_economic_sector_moneyflow_v1 import joint_fit_fixture, joint_rows_fixture


def joint_plan_fixture(tmp_path):
    prior, *older = asymmetric_risk_plan_fixture(tmp_path)
    parent, events = older[-2:]
    fitted = asymmetric_risk_fit_fixture()
    ancestor = None
    for stage in ('preregistered', 'prepared', 'trained', 'evaluated'):
        artifacts = {'plan.json': _json_bytes(prior.model_dump(mode='json'))} if stage == 'preregistered' else {'metadata.json': _json_bytes(dict(parameters=prior.parameters, models=fitted.models, recipe=fitted.recipe, model_sha256=fitted.model_sha256, support=asdict(fitted.support), diagnostics=fitted.diagnostics))} if stage == 'trained' else {'unit.json': b'{}'}
        saved = publish_stage(study_root=tmp_path/prior.experiment_id, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=ancestor, artifacts=artifacts)
        ancestor = read_stage(saved, stage=stage, plan_sha256=prior.plan_sha256, parent_sha256=ancestor)['stage_sha256']
        if stage in ('preregistered', 'evaluated'):
            _record(prior, saved.parent, parent, stage.upper(), saved/'manifest.json')
    events += [dict(kind='PHYSICAL_FIT', state='STARTED', model_id=prior.model_id, campaign_id=prior.campaign_id, experiment_id=prior.experiment_id, head=arm+'_'+head) for arm in ('matched', 'candidate') for head in ('mean', 'path')]
    (tmp_path/'campaign_fit_journal.jsonl').write_text(''.join(json.dumps(event)+'\n' for event in events), encoding='utf-8')
    sector_id = next(event['experiment_id'] for event in events if event['model_id'] == 'M1')
    sector = SectorPricePlanV1.model_validate_json((tmp_path/sector_id/'preregistered/plan.json').read_text(encoding='utf-8'))
    registered = read_stage(tmp_path/sector_id/'preregistered', stage='preregistered', plan_sha256=sector.plan_sha256, parent_sha256=None)
    sector_prepared = publish_stage(study_root=tmp_path/sector_id, stage='prepared', plan_sha256=sector.plan_sha256, parent_sha256=registered['stage_sha256'], artifacts={'unit.json': b'{}'})
    _record(sector, sector_prepared.parent, parent, 'PREPARED', sector_prepared/'manifest.json')
    original = older[-3]
    plan = SectorMoneyflowPlanV1(**prior.model_dump(exclude={'schema_version', 'campaign_id', 'model_id', 'predecessor_manifest_ref'}),
        predecessor_manifest_ref=evidence_reference_for_file(saved/'manifest.json', role='sector_moneyflow_predecessor'),
        sector_prepared_manifest_ref=evidence_reference_for_file(sector_prepared/'manifest.json', role='sector_moneyflow_sector_snapshot'),
        moneyflow_prepared_manifest_ref=evidence_reference_for_file(tmp_path/original.experiment_id/'prepared/manifest.json', role='sector_moneyflow_flow_snapshot'))
    return plan, prior, *older


def test_prepare_zero_SQL_original_labels_order_unknown_retry_partial_and_tamper(tmp_path, monkeypatch):
    plan, *previous = joint_plan_fixture(tmp_path)
    parent = previous[-2]
    with pytest.raises(ValueError):
        plan.model_copy(update={'model_id': 'M18'})
    with pytest.raises(ValueError):
        plan.model_copy(update={'sector_prepared_manifest_ref': plan.moneyflow_prepared_manifest_ref})
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    args = joint_rows_fixture()
    candidates = args['candidates'].assign(selection_effective_rank=[1, 2], is_candidate_decision=True)
    frozen = tmp_path/'frozen'
    frozen.mkdir()
    candidates.to_parquet(frozen/'frozen_rankings.parquet', index=False)
    (frozen/'calendar.json').write_text(json.dumps([str(day.date()) for day in args['calendar']]), encoding='utf-8')
    args['sector_rows'].assign(feature_visible_through=args['sector_rows'][KEY[0]]).to_parquet(Path(plan.sector_prepared_manifest_ref.artifact_uri).parent/'rows.parquet', index=False)
    args['moneyflow_rows'].loc[1, MONEYFLOW_FEATURES[0]] = None
    args['moneyflow_rows'].to_parquet(Path(plan.moneyflow_prepared_manifest_ref.artifact_uri).parent/'rows.parquet', index=False)
    identity = SimpleNamespace(candidate_roster_sha256=candidate_roster_sha256(candidates), source_evidence='LEGACY_FROZEN')
    monkeypatch.setattr(pipeline, 'load_sector_moneyflow_study_v1', lambda **_: (plan, root, manifest, (parent, frozen, identity, None, None, {})))
    prepared = pipeline.prepare_sector_moneyflow_v1(plan_path=registered/'unit.json', output_root=tmp_path)
    rows = pd.read_parquet(prepared/'rows.parquet')
    assert rows[KEY].equals(candidates[KEY]) and rows.gross_value_ratio.eq(1.04).all()
    assert rows[STATUS].tolist() == ['AVAILABLE', 'UNKNOWN_SOURCE_OR_INPUT']
    receipt = json.loads((prepared/'sector_moneyflow_source_receipt.json').read_text(encoding='utf-8'))
    assert receipt['selects'] == 0 and not receipt['database_accessed'] and not receipt['database_written'] and receipt['native_identity'] == 'UNPROVEN'
    monkeypatch.setattr(pipeline.pd, 'read_parquet', lambda *_args, **_kwargs: pytest.fail('published prepare cannot rebuild'))
    assert pipeline.prepare_sector_moneyflow_v1(plan_path=registered/'unit.json', output_root=tmp_path) == prepared
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_sector_moneyflow_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_sector_moneyflow_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'sector_moneyflow_source_receipt.json').write_bytes(b'{}')
    with pytest.raises(Exception, match='hash|size|artifact'):
        pipeline.prepare_sector_moneyflow_v1(plan_path=registered/'unit.json', output_root=tmp_path)


def test_M19_actual_T_open_D_inputs_joint_unknown_and_future_clock():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys((*D_FEATURES, *JOINT_FEATURES), .01), 'feature_visible_through': key[KEY[0]], CLOCK: key[KEY[0]], STATUS: 'AVAILABLE'}])
    args = dict(fitted=joint_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.1, raw_close_cny=-999., suspended=False, tradability_unknown=False, up_limit=11., down_limit=9., source_sha256='a'*64, price_coordinate_sha256='b'*64)]),
        references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64))
    for arm in ('matched', 'candidate'):
        assert pipeline.sector_moneyflow_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['TAKE']
        inputs[STATUS] = 'UNKNOWN_SOURCE_OR_INPUT'
        assert pipeline.sector_moneyflow_actual_decisions_v1(**args, arm=arm).model_action.tolist() == ['UNAVAILABLE']
        inputs[STATUS] = 'AVAILABLE'
    inputs[CLOCK] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.sector_moneyflow_actual_decisions_v1(**args, arm='candidate')
