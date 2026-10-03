from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first import economic_price_campaign_pipeline_v2 as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.tests.advisory_model_first.test_economic_price_campaign_contracts_v2 import plan_fixture


def test_partial_fit_unknown_qe_and_stage_mutation_refused_before_fit(tmp_path, monkeypatch):
    plan = plan_fixture()
    root = tmp_path/plan.experiment_id
    registered = publish_stage(study_root=root, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={'plan.json': _json_bytes(plan.model_dump(mode='json'))})
    manifest = read_stage(registered, stage='preregistered', plan_sha256=plan.plan_sha256, parent_sha256=None)
    prepared = publish_stage(study_root=root, stage='prepared', plan_sha256=plan.plan_sha256, parent_sha256=manifest['stage_sha256'],
        artifacts={'unit.json': _json_bytes({'fixture': True})})
    monkeypatch.setattr(pipeline, 'load_campaign_v2', lambda **kwargs: (plan, root, manifest, (SimpleNamespace(configuration=None),)))
    monkeypatch.setattr(pipeline, '_ledger', lambda *args: None)
    monkeypatch.setattr(pipeline, 'train_campaign_v2', lambda **kwargs: pytest.fail('must not refit'))
    (root/'fit_attempt.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='partial fit'):
        pipeline.train_campaign_study_v2(plan_path=registered/'plan.json', output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match='QE training'):
        pipeline.train_campaign_study_v2(plan_path=registered/'plan.json', output_root=tmp_path, qe_training_idle=None)
    (prepared/'unit.json').write_bytes(b'corrupt')
    with pytest.raises(AdvisoryModelFirstError, match='hash mismatch'):
        pipeline.train_campaign_study_v2(plan_path=registered/'plan.json', output_root=tmp_path, qe_training_idle=True)


def test_source_identity_binds_borrowed_sources_and_shadow():
    assert len(pipeline.implementation_sha256_v2()) == 64
