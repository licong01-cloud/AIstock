from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, BASE_FEATURES, StateModelSetError
from scripts.hmm_risk import prepare_state_model_set as subject


def _request() -> dict:
    dataset = {"schema_version": "dataset_v1", "calendar_benchmark": {"schema_version": "calendar_v1"}}
    mapping = {"schema_version": "mapping_v1"}
    l2_stock_fact = {"schema_version": "l2_dataset_v1"}
    return {
        "schema_version": subject.REQUEST_SCHEMA,
        "source": {
            "universe_key": "frozen-universe",
            "universe_rule_version": "frozen-rule",
            "source_start": "2020-07-30",
            "source_end": "2025-04-30",
        },
        "producer_commit": "c" * 40,
        "dataset_manifest_hash": subject.canonical_sha256(dataset),
        "mapping_manifest_hash": subject.canonical_sha256(mapping),
        "l2_stock_fact_manifest_hash": subject.canonical_sha256(l2_stock_fact),
        "families": [
            {
                "family": "legacy_covfix",
                "feature_names": list(BASE_FEATURES),
                "preprocess_family": "identity",
            },
            {
                "family": "autocycle_all_core",
                "feature_names": list(ALL_CORE_FEATURES),
                "preprocess_family": "winsor_zscore_1_99_train_global_v1",
            },
        ],
    }


def _preflight_inputs() -> dict:
    return {
        "database": {"host": "127.0.0.1", "port": 5432, "dbname": "aistock"},
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "stock_facts": {
                "schema_version": "l1_facts_v1",
                "aggregate_row_count": 33_221,
                "invalid_l1_date_count": 2_491,
            },
            "calendar_benchmark": {"schema_version": "calendar_v1"},
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {
            "schema_version": "l2_dataset_v1",
            "aggregate_row_count": 145_805,
            "invalid_sector_date_count": 4_067,
        },
        "panel": [object()] * 35_712,
        "l2_panel": [object()] * 150_912,
    }


def test_legacy_fixed_seed_ready_writer_is_disabled() -> None:
    with pytest.raises(StateModelSetError, match="legacy fixed-seed preparation is disabled"):
        subject.prepare({}, artifact_root=None, output_root=None, db_prefix="TDX_DB_DEV_")


def test_formal_producer_identity_rejects_dirty_worktree(monkeypatch) -> None:
    monkeypatch.setattr(subject, "_git_commit", lambda: "d" * 40)
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=" M scripts/hmm_risk/prepare_state_model_set.py\n"),
    )

    with pytest.raises(StateModelSetError, match="formal B3 producer worktree must be clean"):
        subject._formal_producer_commit()


def test_preflight_freezes_current_identities_without_fit_selection_or_writes(monkeypatch) -> None:
    request = _request()
    old_producer = request["producer_commit"]
    inputs = _preflight_inputs()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    candidate = report["request_candidate"]
    assert report["schema_version"] == subject.B3_PREFLIGHT_SCHEMA
    assert report["status"] == "candidate_ready"
    assert report["source_template_producer_commit"] == old_producer
    assert candidate["producer_commit"] == "d" * 40
    assert candidate["dataset_manifest_hash"] == subject.canonical_sha256(inputs["dataset_manifest"])
    assert candidate["mapping_manifest_hash"] == subject.canonical_sha256(inputs["mapping_manifest"])
    assert candidate["l2_stock_fact_manifest_hash"] == subject.canonical_sha256(inputs["l2_stock_fact_manifest"])
    assert report["request_candidate_sha256"] == subject.canonical_sha256(candidate)
    assert report["l1_sector_count"] == 31
    assert report["l1_aggregate_row_count"] == 33_221
    assert report["l1_invalid_sector_date_count"] == 2_491
    assert report["l1_panel_row_count"] == 35_712
    assert report["l2_sector_count"] == 131
    assert report["l2_aggregate_row_count"] == 145_805
    assert report["l2_invalid_sector_date_count"] == 4_067
    assert report["l2_panel_row_count"] == 150_912
    for field in (
        "fit_performed",
        "selection_performed",
        "formal_acceptance_thresholds_applied",
        "hard_semantic_authority_changed",
        "model_write_performed",
        "ready_artifact_write_performed",
        "database_write_performed",
        "runtime_action_performed",
    ):
        assert report[field] is False
    assert request["producer_commit"] == old_producer


def test_request_template_loader_accepts_unfrozen_template_but_formal_loader_rejects_it(tmp_path) -> None:
    request = _request()
    for field in ("dataset_manifest_hash", "mapping_manifest_hash", "l2_stock_fact_manifest_hash"):
        request.pop(field)
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")

    assert subject._load_request_template(request_path) == request
    with pytest.raises(StateModelSetError, match="dataset_manifest_hash must be a SHA-256 identity"):
        subject._load_request(request_path)


def test_main_preflight_writes_immutable_candidate_and_receipt(monkeypatch, tmp_path, capsys) -> None:
    request = _request()
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    candidate_path = tmp_path / "candidate.json"
    report_path = tmp_path / "preflight.json"
    inputs = _preflight_inputs()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(subject, "_read_env_file", lambda path: None)
    monkeypatch.setattr(
        subject,
        "parse_args",
        lambda: SimpleNamespace(
            request=str(request_path),
            env_file=str(tmp_path / "unused.env"),
            db_env_prefix="TDX_DB_",
            b3_preflight_output=str(report_path),
            b3_request_candidate_output=str(candidate_path),
        ),
    )

    assert subject.main() == 0

    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    preflight = json.loads(report_path.read_text(encoding="utf-8"))
    cli_receipt = json.loads(capsys.readouterr().out)
    assert subject.canonical_sha256(candidate) == preflight["request_candidate_sha256"]
    assert cli_receipt["report_sha256"] == subject.canonical_sha256(preflight)
    assert cli_receipt["request_candidate_sha256"] == subject.canonical_sha256(candidate)
    assert cli_receipt["fit_performed"] is False
    assert cli_receipt["selection_performed"] is False
    assert cli_receipt["ready_artifact_write_performed"] is False


def test_formal_single_pass_runs_both_families_and_levels_without_selection_or_validation(monkeypatch) -> None:
    request = _request()
    inputs = {
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "calendar_benchmark": {"schema_version": "calendar_v1"},
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(
        subject,
        "_direct_train_series_for_family",
        lambda inputs, family: {"L1": {"L1": object()}, "L2": {"L2": object()}},
    )
    calls = []

    def fake_repeat(series, *, family, level, feature_names, preprocess_family, process_identity):
        calls.append((family, level, process_identity, len(feature_names), preprocess_family))
        return ({"family": family, "level": level, "schedule": list(range(42, 50))}, {})

    monkeypatch.setattr(subject, "run_level_repeat", fake_repeat)
    receipt = subject.prepare_b3_single_pass(
        request,
        db_prefix="TDX_DB_DEV_",
        process_identity="fresh_process_1",
    )

    assert len(calls) == 4
    assert set(receipt["level_repeats"]) == {
        "legacy_covfix:L1",
        "legacy_covfix:L2",
        "autocycle_all_core:L1",
        "autocycle_all_core:L2",
    }
    assert receipt["selection_performed"] is False
    assert receipt["validation_accessed_for_selection"] is False
    assert receipt["future_utility_accessed_for_selection"] is False
    assert receipt["ready_artifact_write_performed"] is False


def test_formal_single_pass_rejects_frozen_manifest_drift(monkeypatch) -> None:
    request = _request()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(
        subject,
        "_load_l1_source_inputs",
        lambda request, db_prefix: {
            "dataset_manifest": {"schema_version": "drifted"},
            "mapping_manifest": {"schema_version": "mapping_v1"},
            "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
        },
    )
    with pytest.raises(StateModelSetError, match="dataset manifest hash mismatch"):
        subject.prepare_b3_single_pass(
            request,
            db_prefix="TDX_DB_DEV_",
            process_identity="fresh_process_1",
        )
