from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first import economic_value_anchor_pipeline_v1 as pipeline
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.tests.advisory_model_first.test_economic_value_anchor_training_v1 import training_fixture


def test_source_implementation_identity_binds_scene_simulator_and_family():
    assert len(pipeline.value_anchor_implementation_sha256_v1()) == 64


def test_partial_fit_is_durably_blocked_before_implicit_refit(tmp_path, monkeypatch):
    args, plan = training_fixture()
    root = tmp_path/plan.experiment_id
    registered_path = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None, artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json"))})
    registered = read_stage(registered_path, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=registered["stage_sha256"], artifacts={"preparation.json": _json_bytes({"unit_fixture": True})})
    monkeypatch.setattr(pipeline, "load_value_anchor_v1", lambda **kwargs: (plan, root, registered, (SimpleNamespace(configuration=args["configuration"]),)))
    monkeypatch.setattr(pipeline, "_ledger", lambda *a: None)
    marker = root/"fit_attempt.json"
    marker.write_bytes(b'{"unit_fixture_partial_fit":true}')
    monkeypatch.setattr(pipeline, "train_value_anchor_v1", lambda **kwargs: pytest.fail("must not refit"))
    with pytest.raises(ValueError, match="partial fit"):
        pipeline.train_value_anchor_study_v1(plan_path=registered_path/"plan.json", output_root=tmp_path, qe_training_idle=True)
    with pytest.raises(ValueError, match="overlap QE"):
        pipeline.train_value_anchor_study_v1(plan_path=registered_path/"plan.json", output_root=tmp_path, qe_training_idle=False)


def test_c_root_or_relative_publication_is_not_allowed():
    _, plan = training_fixture()
    for path in ("C:/tmp/value_anchor", "relative"):
        with pytest.raises(ValueError):
            pipeline._root(plan, path)


def test_unconsumed_price_clock_is_rejected_before_OHLC_read(tmp_path, monkeypatch):
    calls = []

    def dates_only(path, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame({"trade_date": [pd.Timestamp("2026-01-05")]})

    monkeypatch.setattr(pipeline.pd, "read_parquet", dates_only)
    with pytest.raises(ValueError, match="OHLC not read"):
        pipeline._value_prices(tmp_path, SimpleNamespace(train_start="2025-01-01", label_cutoff="2025-12-31"))
    assert calls == [{"columns": ["trade_date"]}]
