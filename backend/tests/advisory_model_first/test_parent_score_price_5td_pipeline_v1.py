"""Atomic stages and serialized attempts use X-only fixtures, not live QE or SQL."""
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.parent_score_price_5td_contracts_v1 import ARMS, node_path
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes, training_encoding
from backend.services.advisory_model_first import parent_score_price_5td_pipeline_v1 as pipeline
from backend.tests.advisory_model_first.test_parent_score_price_5td_inputs_v1 import sample_plan, sample_rows


def prepared(tmp_path):
    plan, calendar = sample_plan(tmp_path)
    pipeline.preregister_parent_score_study(plan=plan, output_root=tmp_path)
    root = tmp_path/plan.experiment_id
    rows, days = sample_rows(plan, calendar)
    encoding = training_encoding(rows=rows, plan=plan)
    initial = pipeline._stage(plan, root, "preregistered")
    pipeline._publish(plan, root, "prepared", initial["stage_sha256"], {
        "rows.parquet": parquet_bytes(rows), "days.parquet": parquet_bytes(days),
        "encoding.json": pipeline._json_bytes(encoding), "calendar.json": pipeline._json_bytes([d.isoformat() for d in calendar])})
    return plan, root


def observation(count=0):
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=count, custom_evo=0, multi_alpha=0))


def test_busy_qe_and_source_drift_do_not_claim_or_fit(tmp_path, monkeypatch):
    plan, root = prepared(tmp_path)
    monkeypatch.setattr(pipeline, "_node", lambda p: dict(fixture=True))
    with pytest.raises(ValueError, match="QE running/pending"):
        pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=lambda: observation(1))
    assert not (root/"fit_attempt.json").exists() and not (root/"fits").exists()
    from backend.services.advisory_model_first import parent_score_price_5td_model_v1 as model
    def overlapping(**kwargs):
        kwargs["before_fit"](kwargs["arm"])
        kwargs["after_fit"](kwargs["arm"])
    monkeypatch.setattr(model, "train_parent_score_model", overlapping)
    observations = iter([observation(0), observation(1)])
    with pytest.raises(ValueError, match="QE became active"):
        pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=lambda: next(observations))
    assert (root/"fit_attempt.json").is_file() and not (root/"trained").exists()
    monkeypatch.setattr(pipeline, "implementation_sha256", lambda: "f"*64)
    with pytest.raises(ValueError, match="implementation changed"):
        pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=observation)


def test_one_partial_attempt_is_retained_never_implicitly_refitted(tmp_path, monkeypatch):
    plan, root = prepared(tmp_path)
    monkeypatch.setattr(pipeline, "_node", lambda p: dict(fixture=True))
    from backend.services.advisory_model_first import parent_score_price_5td_model_v1 as model
    started = []
    def train(**kwargs):
        kwargs["before_fit"](kwargs["arm"])
        started.append(kwargs["arm"])
        kwargs["after_fit"](kwargs["arm"])
        raise RuntimeError("fixture fit interrupted")
    monkeypatch.setattr(model, "train_parent_score_model", train)
    with pytest.raises(RuntimeError, match="interrupted"):
        pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=observation)
    assert started == [ARMS[0]] and (root/"fit_attempt.json").is_file()
    with pytest.raises(ValueError, match="never implicitly refit"):
        pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=observation)
    assert started == [ARMS[0]] and not (root/"trained").exists()
    with pytest.raises(FileExistsError):
        pipeline._exclusive_json(root/"fit_attempt.json", dict(replace=True))
    assert json.loads((root/"fit_attempt.json").read_bytes())["status"] == "STARTED"


def test_exact_two_attempts_register_atomic_models_and_reuse_no_refit(tmp_path, monkeypatch):
    plan, root = prepared(tmp_path)
    monkeypatch.setattr(pipeline, "_node", lambda p: dict(fixture=True))
    from backend.services.advisory_model_first import parent_score_price_5td_model_v1 as model
    started = []
    def train(**kwargs):
        kwargs["before_fit"](kwargs["arm"])
        started.append(kwargs["arm"])
        kwargs["after_fit"](kwargs["arm"])
        return SimpleNamespace(model_sha256="a"*64, arm=kwargs["arm"])
    monkeypatch.setattr(model, "train_parent_score_model", train)
    monkeypatch.setattr(model, "model_payload", lambda f: dict(fixture=True, arm=f.arm))
    target = pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=observation)
    assert target == root/"trained" and started == list(ARMS)
    assert json.loads((target/"receipt.json").read_bytes())["physical_fit_count"] == 2
    assert pipeline.train_parent_score_study(plan=plan, output_root=tmp_path, qe_idle_probe=observation) == target
    assert started == list(ARMS)
    assert len((root/"fit_journal.jsonl").read_text().splitlines()) == 6
    for uri in ("C:/Temp/no", "/mnt/c/no", "relative/no"):
        with pytest.raises(ValueError, match="non-C|forbidden"):
            node_path(uri)
