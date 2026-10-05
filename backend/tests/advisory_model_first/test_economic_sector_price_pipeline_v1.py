import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_sector_price_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.economic_sector_price_source_v1 import SECTOR_FEATURES
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPricePlanV1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture
from backend.tests.advisory_model_first.test_economic_sector_price_value_v1 import sector_fit_fixture


def sector_plan_fixture(tmp_path):
    crosswalk = tmp_path/'crosswalk.json'
    crosswalk.write_bytes(b'{}')
    return SectorPricePlanV1(**{**plan_fixture().model_dump(exclude={'model_id', 'schema_version'}),
        'crosswalk_ref': evidence_reference_for_file(crosswalk, role='sector_structural_crosswalk')})


def test_cumulative15_not_reset_partial_unknown_qe_and_identity(tmp_path, monkeypatch):
    plan = sector_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={'unit.json': b'{}'})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    publish_stage(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=manifest['stage_sha256'], artifacts={'unit.json': b'{}'})
    journal = tmp_path/'campaign_fit_journal.jsonl'
    journal.write_bytes(b''.join(_json_bytes(dict(kind='PHYSICAL_FIT')).replace(b'\n', b'')+b'\n' for _ in range(11)))
    for index in range(4):
        pipeline._fit_event(plan, root, str(index))
    assert len([json.loads(line) for line in journal.read_text().splitlines()]) == 15
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._fit_event(plan, root, 'extra')
    monkeypatch.setattr(pipeline, 'load_sector_price_v1', lambda **kwargs: (plan, root, manifest, (SimpleNamespace(configuration=None),)))
    monkeypatch.setattr(pipeline, '_ledger', lambda *args: None)
    monkeypatch.setattr(pipeline, 'train_sector_price_v1', lambda **kwargs: pytest.fail('must not fit'))
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial'):
        pipeline.train_sector_price_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_sector_price_study_v1(plan_path=registered/'unit.json', output_root=tmp_path, qe_training_idle=None)
    assert len(pipeline.sector_implementation_sha256_v1()) == 64


def test_actual_query_T_open_only_unknown_preserves_candidate_and_clock():
    key = dict(zip(KEY, [pd.Timestamp('2025-01-02'), pd.Timestamp('2025-01-03'), '000001.SZ'], strict=True))
    inputs = pd.DataFrame([{**key, **dict.fromkeys(D_FEATURES, 0.), **dict.fromkeys(SECTOR_FEATURES, 0.),
        'feature_visible_through': key[KEY[0]], 'sector_feature_visible_through': key[KEY[0]]}])
    prices = pd.DataFrame([dict(trade_date=key[KEY[1]], instrument=key[KEY[2]], raw_open_cny=10.10,
        raw_close_cny=-999., suspended=False, tradability_unknown=False, up_limit=11., down_limit=9.,
        source_sha256='a'*64, price_coordinate_sha256='b'*64)])
    args = dict(fitted=sector_fit_fixture(), candidates=pd.DataFrame([{**key, 'selection_effective_rank': 1}]), inputs=inputs,
        prices=prices, references=pd.DataFrame([{**key, 'target_reference_raw_cny': 10., 'reference_visible_through': key[KEY[0]], 'source_sha256': 'c'*64}]),
        identity=SimpleNamespace(price_source_sha256='a'*64, price_coordinate_sha256='b'*64, reference_source_sha256='c'*64), arm='candidate')
    assert pipeline.sector_actual_decisions_v1(**args).model_action.tolist() == ['TAKE']
    inputs[SECTOR_FEATURES[0]] = None
    result = pipeline.sector_actual_decisions_v1(**args)
    assert result.model_action.tolist() == ['UNAVAILABLE'] and result.market_admissible.all()
    inputs['sector_feature_visible_through'] = key[KEY[1]]
    with pytest.raises(ValueError, match='future feature'):
        pipeline.sector_actual_decisions_v1(**args)


@pytest.mark.parametrize(('model', 'cap'), [('M1', 15), ('M5', 19), ('M6', 23), ('M7', 27), ('M8', 31), ('M9', 35), ('M10', 39), ('M11', 43), ('M12', 47), ('M13', 51), ('M14', 55), ('M15', 59), ('M16', 63), ('M17', 67), ('M18', 71)])
def test_fixed_budget_routes_keep_existing_caps(tmp_path, model, cap):
    plan = sector_plan_fixture(tmp_path)
    root = tmp_path/plan.experiment_id
    root.mkdir()
    journal = tmp_path/'campaign_fit_journal.jsonl'
    journal.write_text(''.join(json.dumps(dict(kind='PHYSICAL_FIT'))+'\n' for _ in range(cap-1)), encoding='utf-8')
    pipeline._fit_event(plan, root, 'one', model_id=model, campaign_fit_budget=cap)
    with pytest.raises(ValueError, match='cumulative'):
        pipeline._fit_event(plan, root, 'extra', model_id=model, campaign_fit_budget=cap)
    with pytest.raises(ValueError, match='fixed information'):
        pipeline._fit_event(plan, root, 'wrong', model_id=model, campaign_fit_budget=cap+1)
