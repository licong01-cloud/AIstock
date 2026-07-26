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


def _approve_preflight_inputs(monkeypatch, inputs: dict) -> None:
    monkeypatch.setattr(
        subject,
        "B3_APPROVED_FROZEN_IDENTITIES",
        {
            "dataset_manifest_hash": subject.canonical_sha256(inputs["dataset_manifest"]),
            "mapping_manifest_hash": subject.canonical_sha256(inputs["mapping_manifest"]),
            "l2_stock_fact_manifest_hash": subject.canonical_sha256(inputs["l2_stock_fact_manifest"]),
        },
    )


def _coverage_preflight(*, valid: bool = True) -> dict:
    body = {
        "schema_version": "hmm_risk_b3_train_coverage_preflight_set_v1",
        "reports": {},
        "report_count": 4,
        "train_coverage_valid": valid,
        "failure_reason_codes": [] if valid else ["hmm_risk_model_train_observation_coverage_insufficient"],
        "fit_performed": False,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
    }
    return {**body, "receipt_sha256": subject.canonical_sha256(body)}


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


def test_formal_b3_rejects_well_formed_but_unapproved_frozen_identity() -> None:
    identities = dict(subject.B3_APPROVED_FROZEN_IDENTITIES)
    identities["dataset_manifest_hash"] = "f" * 64

    with pytest.raises(StateModelSetError, match="formal B3 frozen identity mismatch: dataset_manifest_hash"):
        subject._require_approved_b3_identities(identities)


def test_preflight_rejects_live_manifest_drift_before_candidate_ready(monkeypatch) -> None:
    inputs = _preflight_inputs()
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)

    with pytest.raises(StateModelSetError, match="formal B3 frozen identity mismatch"):
        subject.prepare_b3_preflight_candidate(_request(), db_prefix="TDX_DB_")


def test_preflight_freezes_current_identities_without_fit_selection_or_writes(monkeypatch) -> None:
    request = _request()
    old_producer = request["producer_commit"]
    inputs = _preflight_inputs()
    _approve_preflight_inputs(monkeypatch, inputs)
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda inputs, request: _coverage_preflight())

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    candidate = report["request_candidate"]
    assert report["schema_version"] == subject.B3_PREFLIGHT_SCHEMA
    assert report["status"] == "candidate_ready"
    assert report["source_template_producer_commit"] == old_producer
    assert candidate["producer_commit"] == "d" * 40
    assert candidate["dataset_manifest_hash"] == subject.canonical_sha256(inputs["dataset_manifest"])
    assert candidate["mapping_manifest_hash"] == subject.canonical_sha256(inputs["mapping_manifest"])
    assert candidate["l2_stock_fact_manifest_hash"] == subject.canonical_sha256(inputs["l2_stock_fact_manifest"])
    assert candidate["train_coverage_contract_version"] == subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION
    assert candidate["train_coverage_receipt_sha256"] == report["train_coverage"]["receipt_sha256"]
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


def test_preflight_blocks_insufficient_train_coverage_without_request_candidate(monkeypatch) -> None:
    request = _request()
    inputs = _preflight_inputs()
    _approve_preflight_inputs(monkeypatch, inputs)
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(valid=False),
    )

    report = subject.prepare_b3_preflight_candidate(request, db_prefix="TDX_DB_")

    assert report["status"] == "blocked"
    assert report["request_candidate"] is None
    assert report["request_candidate_sha256"] is None
    assert report["train_coverage_valid"] is False
    assert report["failure_reason_codes"] == ["hmm_risk_model_train_observation_coverage_insufficient"]
    assert report["fit_performed"] is False


def test_request_template_loader_accepts_unfrozen_template_but_formal_loader_rejects_it(tmp_path) -> None:
    request = _request()
    for field in ("dataset_manifest_hash", "mapping_manifest_hash", "l2_stock_fact_manifest_hash"):
        request.pop(field)
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")

    assert subject._load_request_template(request_path) == request
    with pytest.raises(StateModelSetError, match="dataset_manifest_hash must be a SHA-256 identity"):
        subject._load_request(request_path)


def test_formal_request_rejects_missing_train_coverage_identity() -> None:
    with pytest.raises(StateModelSetError, match="train coverage identity is missing or invalid"):
        subject._require_formal_train_coverage_identity(_request())


def test_main_preflight_writes_immutable_candidate_and_receipt(monkeypatch, tmp_path, capsys) -> None:
    request = _request()
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    candidate_path = tmp_path / "candidate.json"
    report_path = tmp_path / "preflight.json"
    inputs = _preflight_inputs()
    _approve_preflight_inputs(monkeypatch, inputs)
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda inputs, request: _coverage_preflight())
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


def test_main_blocked_preflight_does_not_overwrite_stale_candidate(monkeypatch, tmp_path, capsys) -> None:
    request = _request()
    request_path = tmp_path / "request-template.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text('{"stale":true}\n', encoding="utf-8")
    report_path = tmp_path / "preflight.json"
    inputs = _preflight_inputs()
    _approve_preflight_inputs(monkeypatch, inputs)
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "d" * 40)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(
        subject,
        "_b3_train_coverage_preflight",
        lambda inputs, request: _coverage_preflight(valid=False),
    )
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

    assert subject.main() == 1

    preflight = json.loads(report_path.read_text(encoding="utf-8"))
    cli_receipt = json.loads(capsys.readouterr().out)
    assert candidate_path.read_text(encoding="utf-8") == '{"stale":true}\n'
    assert preflight["status"] == "blocked"
    assert preflight["request_candidate"] is None
    assert cli_receipt["request_candidate_path"] is None
    assert cli_receipt["request_candidate_sha256"] is None


def test_formal_parent_persists_typed_child_failure_receipt(monkeypatch, tmp_path) -> None:
    args = SimpleNamespace(
        request=str(tmp_path / "request.json"),
        output_root=str(tmp_path / "model-sets"),
        env_file=str(tmp_path / "env"),
        db_env_prefix="TDX_DB_",
        b3_preparation_output=str(tmp_path / "formal-receipt.json"),
    )
    stderr = json.dumps(
        {
            "schema_version": "hmm_risk_state_model_set_preparation_error_v1",
            "status": "failed",
            "error_type": "StateModelSetError",
            "error": "801010.SI train-only observation coverage is insufficient: 10",
        }
    ).encode()
    monkeypatch.setattr(
        subject.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout=b"", stderr=stderr),
    )

    with pytest.raises(StateModelSetError, match="801010.SI train-only observation coverage is insufficient: 10"):
        subject.run_b3_repeated(args, _request())

    failure_path = tmp_path / "formal-receipt.fresh_process_1.failure.json"
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert failure["error_type"] == "StateModelSetError"
    assert failure["error"] == "801010.SI train-only observation coverage is insufficient: 10"
    assert failure["fit_grid_completed"] is False
    assert failure["selection_performed"] is False
    assert failure["ready_artifact_write_performed"] is False


def test_child_failure_receipt_bounds_untrusted_error_text(monkeypatch, tmp_path) -> None:
    args = SimpleNamespace(b3_preparation_output=str(tmp_path / "formal-receipt.json"))
    stderr = json.dumps(
        {
            "schema_version": "hmm_risk_state_model_set_preparation_error_v1",
            "error_type": "E" * 500,
            "error": "x" * 5000,
        }
    ).encode()

    _, failure = subject._persist_b3_child_failure(
        args,
        process_identity="fresh_process_1",
        returncode=1,
        stdout=b"",
        stderr=stderr,
    )

    assert len(failure["error_type"]) == 256
    assert len(failure["error"]) == 4000


def test_formal_single_pass_runs_both_families_and_levels_without_selection_or_validation(monkeypatch) -> None:
    request = _request()
    coverage = _coverage_preflight()
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": coverage["receipt_sha256"],
        }
    )
    inputs = {
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "calendar_benchmark": {"schema_version": "calendar_v1"},
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_approved_b3_identities", lambda request: None)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda inputs, request: coverage)
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
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": "a" * 64,
        }
    )
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_approved_b3_identities", lambda request: None)
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


def test_formal_single_pass_rejects_stale_train_coverage_receipt_before_fit(monkeypatch) -> None:
    request = _request()
    request.update(
        {
            "train_coverage_contract_version": subject.B3_TRAIN_COVERAGE_PREFLIGHT_VERSION,
            "train_coverage_receipt_sha256": "a" * 64,
        }
    )
    inputs = {
        "dataset_manifest": {
            "schema_version": "dataset_v1",
            "calendar_benchmark": {"schema_version": "calendar_v1"},
        },
        "mapping_manifest": {"schema_version": "mapping_v1"},
        "l2_stock_fact_manifest": {"schema_version": "l2_dataset_v1"},
    }
    monkeypatch.setattr(subject, "_formal_producer_commit", lambda: "c" * 40)
    monkeypatch.setattr(subject, "_require_approved_b3_identities", lambda request: None)
    monkeypatch.setattr(subject, "_load_l1_source_inputs", lambda request, db_prefix: inputs)
    monkeypatch.setattr(subject, "_b3_train_coverage_preflight", lambda inputs, request: _coverage_preflight())
    fit_called = False

    def unexpected_fit(*args, **kwargs):
        nonlocal fit_called
        fit_called = True
        raise AssertionError("fit must not start for a stale coverage receipt")

    monkeypatch.setattr(subject, "run_level_repeat", unexpected_fit)

    with pytest.raises(StateModelSetError, match="train coverage receipt hash mismatch"):
        subject.prepare_b3_single_pass(
            request,
            db_prefix="TDX_DB_DEV_",
            process_identity="fresh_process_1",
        )
    assert fit_called is False
