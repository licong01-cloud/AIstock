from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import authorize_research_window_access
from backend.services.advisory_model_first.research_control_contracts import build_window_access_request, build_window_contract


def test_atomic_stage_exact_retry_parent_binding_and_tamper(tmp_path):
    args = dict(study_root=tmp_path / "study", stage="prepared", plan_sha256="1" * 64,
                parent_sha256="2" * 64, artifacts={"labels.json": b"[]\n", "features.parquet": b"unit-fixture"})
    path = publish_stage(**args)
    first = read_stage(path, stage="prepared", plan_sha256="1" * 64, parent_sha256="2" * 64)
    assert publish_stage(**args) == path
    assert len(list((path.parent / "unpublished").iterdir())) == 0
    with pytest.raises(AdvisoryModelFirstError, match="parent chain mismatch"):
        read_stage(path, stage="prepared", plan_sha256="1" * 64, parent_sha256="3" * 64)
    (path / "labels.json").write_bytes(b"tampered\n")
    with pytest.raises(AdvisoryModelFirstError, match="hash mismatch"):
        read_stage(path, stage="prepared", plan_sha256="1" * 64, parent_sha256="2" * 64)
    assert first["files"]["labels.json"]["size_bytes"] == 3


def test_interrupted_generation_never_publishes_or_overwrites_old_artifacts(tmp_path, monkeypatch):
    import backend.services.advisory_model_first.economic_entry_pipeline as module
    args = dict(study_root=tmp_path / "study", stage="trained", plan_sha256="1" * 64,
                parent_sha256="2" * 64, artifacts={"model_metadata.json": b"{}\n"})
    original = module.os.rename
    with monkeypatch.context() as patched:
        patched.setattr(module.os, "rename", lambda *args: (_ for _ in ()).throw(OSError("injected publication interruption")))
        with pytest.raises(OSError, match="interruption"):
            publish_stage(**args)
    assert not (tmp_path / "study" / "trained").exists()
    preserved = list((tmp_path / "study" / "unpublished").iterdir())
    assert len(preserved) == 1 and (preserved[0] / "manifest.json").is_file()
    assert module.os.rename == original
    published = publish_stage(**args)
    assert preserved[0].is_dir()
    with pytest.raises(AdvisoryModelFirstError, match="conflicts"):
        publish_stage(**{**args, "artifacts": {"model_metadata.json": b'{"different":true}\n'}})
    assert (published / "model_metadata.json").read_bytes() == b"{}\n"


@pytest.mark.parametrize("defect", ["traversal", "foreign_file", "truncated_manifest"])
def test_artifact_shape_is_fail_closed(tmp_path, defect):
    args = dict(study_root=tmp_path / "study", stage="prepared", plan_sha256="1" * 64,
                parent_sha256="2" * 64, artifacts={"labels.json": b"[]\n"})
    if defect == "traversal":
        with pytest.raises(AdvisoryModelFirstError, match="unsafe"):
            publish_stage(**{**args, "artifacts": {"../other.json": b"[]\n"}})
        return
    path = publish_stage(**args)
    if defect == "foreign_file":
        (path / "unknown.txt").write_text("foreign", encoding="utf-8")
    else:
        (path / "manifest.json").write_text("{", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError):
        read_stage(path, stage="prepared", plan_sha256="1" * 64, parent_sha256="2" * 64)


def test_development_source_access_never_consumes_sealed_holdout(tmp_path):
    from backend.services.advisory_model_first.research_control import research_policy_identity
    contract = build_window_contract(
        package_id="package", manifest_sha256="a" * 64, runtime_semantics_hash="b" * 64,
        baseline_policy_sha256="c" * 64, shadow_policy_sha256="d" * 64, cost_policy_sha256="e" * 64,
        source_policy="PIT_DAILY_PARENT_PREDICTIONS_AND_MARKET_OUTCOMES_V1",
        artifact_root_uri=tmp_path.as_posix(), sealed_consumption_receipt_uri=(tmp_path / "sealed_holdout_consumption_receipt.json").as_posix(),
        windows=(
            dict(window_id="dev", dataset_identity="dataset", start_date=date(2024, 1, 1), end_date=date(2025, 1, 1), state="DEVELOPMENT_CONSUMED", purpose="consumed"),
            dict(window_id="test", dataset_identity="test", start_date=date(2024, 12, 1), end_date=date(2025, 1, 1), state="FROZEN_TEST_CONSUMED", purpose="consumed"),
            dict(window_id="replay", dataset_identity="replay", start_date=date(2025, 2, 1), end_date=date(2025, 3, 1), state="HISTORICAL_REPLAY_CONSUMED", purpose="consumed"),
            dict(window_id="sealed", dataset_identity="sealed", start_date=date(2026, 8, 31), end_date=date(2026, 11, 30), state="SEALED_UNCONSUMED", purpose="sealed"),
        ),
    )
    common = dict(contract_sha256=contract.contract_sha256, study_type="EXPLORATORY_SCREEN",
                  objective_contract="RISK_MANAGED_ADVISORY", decision_use="NAVIGATION_ONLY", dataset_identity="dataset",
                  policy_identity=research_policy_identity(baseline_policy_sha256="c" * 64, shadow_policy_sha256="d" * 64, cost_policy_sha256="e" * 64))
    good = build_window_access_request(**common, start_date=date(2024, 1, 1), end_date=date(2025, 1, 1))
    assert authorize_research_window_access(contract=contract, request=good)["sealed_holdout_accessed"] is False
    bad = build_window_access_request(**common, start_date=date(2024, 1, 1), end_date=date(2026, 9, 1))
    with pytest.raises(AdvisoryModelFirstError, match="sealed holdout"):
        authorize_research_window_access(contract=contract, request=bad)
    assert not Path(contract.sealed_consumption_receipt_uri).exists()
    json.loads(contract.model_dump_json())  # The contract is serializable without a DB or release operation.
