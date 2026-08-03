from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

from backend.services.hmm_risk import b3_d1_inactive_dimension as subject
from backend.services.hmm_risk import b3_training as training_subject
from backend.services.hmm_risk.b3_training import B3CoreFitEvidence, B3TrainOnlySeries
from backend.services.hmm_risk.state_model_set import ALL_CORE_FEATURES, canonical_sha256


def _source_identities(label: str) -> list[dict[str, object]]:
    return [
        {
            "seed": seed,
            "diagnostic_entry_sha256": canonical_sha256({"label": label, "seed": seed, "kind": "diagnostic"}),
            "source_entry_receipt_sha256": canonical_sha256({"label": label, "seed": seed, "kind": "source"}),
        }
        for seed in range(42, 50)
    ]


def _install_authority(monkeypatch: pytest.MonkeyPatch) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    treatment = _source_identities("treatment")
    control = _source_identities("control")
    monkeypatch.setattr(subject, "TREATMENT_PROFILE_RECEIPT_SHA256", "1" * 64)
    monkeypatch.setattr(subject, "CONTROL_PROFILE_RECEIPT_SHA256", "2" * 64)
    monkeypatch.setattr(subject, "TREATMENT_SOURCE_SET_SHA256", canonical_sha256(treatment))
    monkeypatch.setattr(subject, "CONTROL_SOURCE_SET_SHA256", canonical_sha256(control))
    monkeypatch.setattr(
        subject,
        "TREATMENT_TRAIN_INPUT_MANIFEST_SHA256",
        canonical_sha256(_series(subject.TREATMENT_SECTOR).train_input_manifest),
    )
    monkeypatch.setattr(
        subject,
        "CONTROL_TRAIN_INPUT_MANIFEST_SHA256",
        canonical_sha256(_series(subject.CONTROL_SECTOR, inactive_value=7.0).train_input_manifest),
    )
    monkeypatch.setattr(subject, "PREPROCESS_IDENTITY_SHA256", canonical_sha256(_preprocess()))
    monkeypatch.setattr(subject, "FEATURE_DEFINITION_SHA256", "4" * 64)
    return treatment, control


def _series(sector_code: str, *, inactive_value: float = 0.0) -> B3TrainOnlySeries:
    rows = 120
    observations = np.arange(rows * len(ALL_CORE_FEATURES), dtype=np.float64).reshape(rows, len(ALL_CORE_FEATURES))
    observations = observations / 1000.0
    observations[:, subject.INACTIVE_FEATURE_INDEX] = inactive_value
    dates = tuple(date(2023, 1, 1) + timedelta(days=index) for index in range(rows))
    date_values = [value.isoformat() for value in dates]
    manifest = {
        "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
        "direct_sector_level": "L2",
        "sector_code": sector_code,
        "train_dates": date_values,
        "train_dates_sha256": canonical_sha256(date_values),
        "train_observation_sha256": canonical_sha256(observations.tolist()),
        "dataset_manifest_hash": "a" * 64,
        "mapping_manifest_hash": "b" * 64,
        "calendar_manifest_hash": "c" * 64,
        "feature_domain_policy_sha256": "d" * 64,
    }
    return B3TrainOnlySeries(
        sector_code=sector_code,
        sector_name=sector_code,
        train_observations=observations,
        train_dates=dates,
        pit_l2_constituents=("000001.SZ",),
        pit_constituent_manifest_hash="e" * 64,
        observation_manifest_hash="f" * 64,
        train_input_manifest=manifest,
    )


def _preprocess() -> dict[str, object]:
    return {"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None}


def _projection_kwargs(role: str) -> dict[str, str]:
    treatment = role == subject.TREATMENT_ROLE
    return {
        "profile_receipt_sha256": "1" * 64 if treatment else "2" * 64,
        "source_set_sha256": subject.TREATMENT_SOURCE_SET_SHA256 if treatment else subject.CONTROL_SOURCE_SET_SHA256,
        "preprocess_identity_sha256": subject.PREPROCESS_IDENTITY_SHA256,
        "feature_definition_sha256": "4" * 64,
    }


def _core(feature_count: int) -> B3CoreFitEvidence:
    status = {"status": "accepted"}
    return B3CoreFitEvidence(
        initialization={"status": "accepted"},
        monitor_evidence={"history": [-2.0, -1.0], "status": "accepted"},
        likelihood={"status": "accepted"},
        covariance={"status": "accepted"},
        train_occupancy={"status": "accepted"},
        startprob=np.array([0.2, 0.3, 0.5]),
        transmat=np.array([[0.8, 0.1, 0.1], [0.1, 0.8, 0.1], [0.1, 0.1, 0.8]]),
        means=np.ones((3, feature_count)),
        covars=np.full((3, feature_count), 0.5),
        terminal_likelihood=-1.0,
        model_entry_status=status["status"],
        model_entry_valid=True,
    )


def test_treatment_projects_full20_after_preprocess_and_never_fabricates_inactive_parameters(monkeypatch):
    _install_authority(monkeypatch)
    item = _series(subject.TREATMENT_SECTOR)

    projected, receipt = subject.build_projection(
        item,
        preprocess=_preprocess(),
        role=subject.TREATMENT_ROLE,
        **_projection_kwargs(subject.TREATMENT_ROLE),
    )

    assert projected.shape == (120, 19)
    assert receipt["full_feature_count"] == 20
    assert receipt["likelihood_feature_count"] == 19
    assert receipt["active_feature_indices"] == list(range(19))
    assert receipt["inactive_feature_indices"] == [19]
    assert receipt["active_feature_mask"] == [True] * 19 + [False]
    assert receipt["dynamic_activation"] is False
    assert receipt["exact_zero_evidence"]["raw_unique_bit_pattern_count"] == 1


def test_treatment_accepts_nonzero_constant_created_by_approved_full20_preprocess(monkeypatch):
    _install_authority(monkeypatch)
    preprocess = {
        "family": "winsor_zscore_1_99_train_global_v1",
        "winsor_low": [-100.0] * 19 + [-1.0],
        "winsor_high": [100.0] * 19 + [-0.1],
        "center": [0.0] * 19 + [-0.2],
        "scale": [1.0] * 19 + [0.1],
    }
    monkeypatch.setattr(subject, "PREPROCESS_IDENTITY_SHA256", canonical_sha256(preprocess))

    projected, receipt = subject.build_projection(
        _series(subject.TREATMENT_SECTOR),
        preprocess=preprocess,
        role=subject.TREATMENT_ROLE,
        profile_receipt_sha256="1" * 64,
        source_set_sha256=subject.TREATMENT_SOURCE_SET_SHA256,
        preprocess_identity_sha256=subject.PREPROCESS_IDENTITY_SHA256,
        feature_definition_sha256="4" * 64,
    )

    evidence = receipt["exact_zero_evidence"]
    assert projected.shape[1] == 19
    assert evidence["raw_all_exact_zero"] is True
    assert evidence["preprocessed_variance_ddof0"] == 0.0
    assert evidence["preprocessed_vector_identity"] != evidence["raw_vector_identity"]


@pytest.mark.parametrize("inactive_value", [1.0, 1e-14, float("inf")])
def test_treatment_rejects_nonzero_nearzero_and_nonfinite_inactive_values(monkeypatch, inactive_value):
    _install_authority(monkeypatch)
    item = _series(
        subject.TREATMENT_SECTOR,
        inactive_value=inactive_value if np.isfinite(inactive_value) else 0.0,
    )
    if not np.isfinite(inactive_value):
        item.train_observations[:, subject.INACTIVE_FEATURE_INDEX] = inactive_value

    with pytest.raises(subject.D1InactiveDimensionError):
        subject.build_projection(
            item,
            preprocess=_preprocess(),
            role=subject.TREATMENT_ROLE,
            **_projection_kwargs(subject.TREATMENT_ROLE),
        )


def test_treatment_rejects_mixed_positive_and_negative_zero_bit_patterns(monkeypatch):
    _install_authority(monkeypatch)
    item = _series(subject.TREATMENT_SECTOR)
    item.train_observations[0, subject.INACTIVE_FEATURE_INDEX] = -0.0
    item.train_input_manifest["train_observation_sha256"] = canonical_sha256(item.train_observations.tolist())
    monkeypatch.setattr(
        subject,
        "TREATMENT_TRAIN_INPUT_MANIFEST_SHA256",
        canonical_sha256(item.train_input_manifest),
    )

    with pytest.raises(subject.D1InactiveDimensionError, match="exact zero"):
        subject.build_projection(
            item,
            preprocess=_preprocess(),
            role=subject.TREATMENT_ROLE,
            **_projection_kwargs(subject.TREATMENT_ROLE),
        )


def test_projection_rejects_train_input_manifest_drift_before_fit(monkeypatch):
    _install_authority(monkeypatch)
    item = _series(subject.TREATMENT_SECTOR)
    item.train_input_manifest["dataset_manifest_hash"] = "9" * 64

    with pytest.raises(subject.D1InactiveDimensionError, match="train input manifest"):
        subject.build_projection(
            item,
            preprocess=_preprocess(),
            role=subject.TREATMENT_ROLE,
            **_projection_kwargs(subject.TREATMENT_ROLE),
        )


def test_identity20_control_uses_same_projection_path_without_dropping_a_dimension(monkeypatch):
    _install_authority(monkeypatch)
    item = _series(subject.CONTROL_SECTOR, inactive_value=7.0)

    projected, receipt = subject.build_projection(
        item,
        preprocess=_preprocess(),
        role=subject.CONTROL_ROLE,
        **_projection_kwargs(subject.CONTROL_ROLE),
    )

    np.testing.assert_array_equal(projected, item.train_observations)
    assert receipt["likelihood_feature_count"] == 20
    assert receipt["active_feature_indices"] == list(range(20))
    assert receipt["inactive_feature_indices"] == []
    assert receipt["exact_zero_evidence"] is None


def test_projection_rejects_preprocess_payload_drift_even_when_caller_claims_approved_hash(monkeypatch):
    _install_authority(monkeypatch)
    drifted = {**_preprocess(), "family": "drifted"}

    with pytest.raises(subject.D1InactiveDimensionError, match="preprocess payload"):
        subject.build_projection(
            _series(subject.TREATMENT_SECTOR),
            preprocess=drifted,
            role=subject.TREATMENT_ROLE,
            **_projection_kwargs(subject.TREATMENT_ROLE),
        )


def test_treatment_attempt_passes_only_19_dimensions_to_shared_hmm_core(monkeypatch):
    treatment, _ = _install_authority(monkeypatch)
    captured: list[tuple[int, int]] = []

    def fake_fit(item, *, train, seed):
        captured.append(train.shape)
        return _core(train.shape[1])

    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", fake_fit)
    attempt = subject.fit_controlled_attempt(
        _series(subject.TREATMENT_SECTOR),
        preprocess=_preprocess(),
        role=subject.TREATMENT_ROLE,
        seed=42,
        process_identity="process-a",
        numeric_environment={"environment": "fixed"},
        source_identity=treatment[0],
        **_projection_kwargs(subject.TREATMENT_ROLE),
    )

    assert captured == [(120, 19)]
    assert attempt["status"] == "fit_completed"
    assert attempt["parameter_payload"]["means"]["shape"] == [3, 19]
    assert attempt["selection_performed"] is False
    assert attempt["model_write_performed"] is False
    assert attempt["ready_artifact_write_performed"] is False


def test_treatment_attempt_rejects_parameters_that_reintroduce_the_inactive_dimension(monkeypatch):
    treatment, _ = _install_authority(monkeypatch)
    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", lambda *args, **kwargs: _core(20))

    attempt = subject.fit_controlled_attempt(
        _series(subject.TREATMENT_SECTOR),
        preprocess=_preprocess(),
        role=subject.TREATMENT_ROLE,
        seed=42,
        process_identity="process-a",
        numeric_environment={"environment": "fixed"},
        source_identity=treatment[0],
        **_projection_kwargs(subject.TREATMENT_ROLE),
    )

    assert attempt["status"] == "fit_failed"
    assert attempt["failure_stage"] == "parameter_shape"
    assert attempt["failure_reason_codes"] == ["hmm_risk_model_inactive_dimension_parameter_shape_invalid"]


def test_identity20_control_compares_frozen_entry_and_model_hashes_without_refit(monkeypatch):
    _, control = _install_authority(monkeypatch)
    item = _series(subject.CONTROL_SECTOR, inactive_value=7.0)
    evidence = _core(20)
    environment = {"environment": "fixed"}
    expected = subject._legacy_compatible_hashes(
        item,
        preprocess=_preprocess(),
        seed=42,
        numeric_environment=environment,
        core=evidence,
    )
    captured: list[tuple[int, int]] = []

    def fake_fit(item, *, train, seed):
        captured.append(train.shape)
        return evidence

    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", fake_fit)
    attempt = subject.fit_controlled_attempt(
        item,
        preprocess=_preprocess(),
        role=subject.CONTROL_ROLE,
        seed=42,
        process_identity="process-a",
        numeric_environment=environment,
        source_identity=control[0],
        expected_control_entry_receipt_sha256=expected["entry_receipt_sha256"],
        expected_control_model_payload_sha256=expected["model_payload_sha256"],
        **_projection_kwargs(subject.CONTROL_ROLE),
    )

    assert captured == [(120, 20)]
    assert attempt["status"] == "fit_completed"
    assert attempt["control_payload_bitwise_equal"] is True

    drifted = subject.fit_controlled_attempt(
        item,
        preprocess=_preprocess(),
        role=subject.CONTROL_ROLE,
        seed=42,
        process_identity="process-a",
        numeric_environment=environment,
        source_identity=control[0],
        expected_control_entry_receipt_sha256="0" * 64,
        expected_control_model_payload_sha256=expected["model_payload_sha256"],
        **_projection_kwargs(subject.CONTROL_ROLE),
    )
    assert drifted["status"] == "fit_failed"
    assert drifted["failure_reason_codes"] == ["hmm_risk_model_inactive_dimension_control_drift"]


def test_control_compatibility_builder_matches_formal_training_payload_hashes(monkeypatch):
    _install_authority(monkeypatch)
    item = _series(subject.CONTROL_SECTOR, inactive_value=7.0)
    evidence = _core(20)
    environment = {"environment": "fixed"}
    monkeypatch.setattr(training_subject, "fit_b3_preprocessed_train_only", lambda *args, **kwargs: evidence)

    entry, fitted = training_subject._fit_b3_train_only(
        item,
        family="autocycle_all_core",
        level="L2",
        feature_names=ALL_CORE_FEATURES,
        preprocess=_preprocess(),
        seed=42,
        numeric_environment=environment,
    )
    compatibility = subject._legacy_compatible_hashes(
        item,
        preprocess=_preprocess(),
        seed=42,
        numeric_environment=environment,
        core=evidence,
    )

    assert compatibility["entry_receipt_sha256"] == entry["entry_receipt_sha256"]
    assert compatibility["model_payload_sha256"] == fitted.model_payload_sha256


def _attempt(
    role: str,
    seed: int,
    process_identity: str,
    *,
    status: str = "fit_completed",
    failure_reason: str | None = None,
) -> dict:
    failure_reasons = []
    if status != "fit_completed":
        failure_reasons = [failure_reason or "hmm_risk_model_inactive_dimension_projection_invalid"]
    body = {
        "schema_version": subject.ATTEMPT_SCHEMA_VERSION,
        "algorithm_version": subject.ALGORITHM_VERSION,
        "process_identity": process_identity,
        "role": role,
        "family": "autocycle_all_core",
        "level": "L2",
        "sector_code": subject.TREATMENT_SECTOR if role == subject.TREATMENT_ROLE else subject.CONTROL_SECTOR,
        "seed": seed,
        "diagnostic_entry_sha256": canonical_sha256({"label": role, "seed": seed, "kind": "diagnostic"}),
        "source_entry_receipt_sha256": canonical_sha256({"label": role, "seed": seed, "kind": "source"}),
        "status": "fit_completed" if status == "fit_completed" else "fit_failed",
        "fit_status": "accepted" if status == "fit_completed" else "failed",
        "failure_stage": None
        if status == "fit_completed"
        else ("projection" if failure_reason is None else "covariance"),
        "failure_reason_codes": failure_reasons,
        "failure_message": None if status == "fit_completed" else "projection failed",
        "projection_receipt": {"projection_status": "accepted"},
        "projection_sha256": "a" * 64,
        "likelihood_feature_count": 19 if role == subject.TREATMENT_ROLE else 20,
        "parameter_payload": {"sha256": "b" * 64},
        "numeric_environment": {"environment": "fixed"},
        "numeric_environment_sha256": canonical_sha256({"environment": "fixed"}),
        "initialization_evidence": {"status": "accepted"},
        "monitor_evidence": {"status": "accepted"},
        "likelihood": {"status": "accepted"},
        "covariance": {"status": "accepted"},
        "train_occupancy": {"status": "accepted"},
        "final_train_log_likelihood": -1.0,
        "control_compatible_payload_hashes": {"entry": "c" * 64} if role == subject.CONTROL_ROLE else None,
        "expected_control_entry_receipt_sha256": "d" * 64 if role == subject.CONTROL_ROLE else None,
        "expected_control_model_payload_sha256": "e" * 64 if role == subject.CONTROL_ROLE else None,
        "control_payload_bitwise_equal": True if role == subject.CONTROL_ROLE else None,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "attempt_receipt_sha256": canonical_sha256(body)}


def _process(monkeypatch, process_identity: str, *, failed_treatment_seed: int | None = None):
    treatment, control = _install_authority(monkeypatch)
    attempts = [
        _attempt(
            subject.TREATMENT_ROLE,
            seed,
            process_identity,
            status="projection_failed" if seed == failed_treatment_seed else "fit_completed",
        )
        for seed in range(42, 50)
    ] + [_attempt(subject.CONTROL_ROLE, seed, process_identity) for seed in range(42, 50)]
    return subject.build_process_receipt(
        process_identity=process_identity,
        producer_commit="1" * 40,
        attempts=attempts,
        treatment_source_identities=treatment,
        control_source_identities=control,
    )


def test_process_runner_never_early_stops_after_a_failed_attempt(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    calls: list[tuple[str, int]] = []

    def fake_attempt(item, *, role, seed, process_identity, **kwargs):
        calls.append((role, seed))
        status = "projection_failed" if role == subject.TREATMENT_ROLE and seed == 42 else "fit_completed"
        return _attempt(role, seed, process_identity, status=status)

    monkeypatch.setattr(subject, "fit_controlled_attempt", fake_attempt)
    receipt = subject.run_controlled_process(
        treatment_item=_series(subject.TREATMENT_SECTOR),
        control_item=_series(subject.CONTROL_SECTOR, inactive_value=7.0),
        preprocess=_preprocess(),
        process_identity="process-a",
        producer_commit="1" * 40,
        numeric_environment={"environment": "fixed"},
        treatment_source_identities=treatment,
        control_source_identities=control,
        frozen_control_hashes={
            seed: {"entry_receipt_sha256": "a" * 64, "model_payload_sha256": "b" * 64} for seed in range(42, 50)
        },
    )

    assert calls == [(role, seed) for seed in range(42, 50) for role in (subject.TREATMENT_ROLE, subject.CONTROL_ROLE)]
    assert receipt["attempt_count"] == 16
    assert receipt["terminal_attempt_count"] == 16


def test_process_runner_validates_all_frozen_control_authority_before_first_fit(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    calls: list[tuple[str, int]] = []

    def fake_attempt(item, *, role, seed, process_identity, **kwargs):
        calls.append((role, seed))
        return _attempt(role, seed, process_identity)

    monkeypatch.setattr(subject, "fit_controlled_attempt", fake_attempt)
    invalid_hashes = {
        seed: {"entry_receipt_sha256": "a" * 64, "model_payload_sha256": "b" * 64} for seed in range(42, 50)
    }
    invalid_hashes[49]["entry_receipt_sha256"] = "invalid"

    with pytest.raises(subject.D1InactiveDimensionError):
        subject.run_controlled_process(
            treatment_item=_series(subject.TREATMENT_SECTOR),
            control_item=_series(subject.CONTROL_SECTOR, inactive_value=7.0),
            preprocess=_preprocess(),
            process_identity="process-a",
            producer_commit="1" * 40,
            numeric_environment={"environment": "fixed"},
            treatment_source_identities=treatment,
            control_source_identities=control,
            frozen_control_hashes=invalid_hashes,
        )
    assert calls == []


def test_process_receipt_rejects_self_consistent_attempt_with_forbidden_side_effect(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    attempts = [
        _attempt(role, seed, "process-a")
        for seed in range(42, 50)
        for role in (subject.TREATMENT_ROLE, subject.CONTROL_ROLE)
    ]
    attempts[0]["selection_performed"] = True
    body = {key: value for key, value in attempts[0].items() if key != "attempt_receipt_sha256"}
    attempts[0]["attempt_receipt_sha256"] = canonical_sha256(body)

    with pytest.raises(subject.D1InactiveDimensionError, match="attempt receipt identity"):
        subject.build_process_receipt(
            process_identity="process-a",
            producer_commit="1" * 40,
            attempts=attempts,
            treatment_source_identities=treatment,
            control_source_identities=control,
        )


def test_controlled_report_separates_diagnostic_completion_mechanism_and_d5_readiness(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")

    report = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)

    assert report["status"] == "diagnostic_complete"
    assert report["mechanism_assessment"] == "constant_dimension_effect_supported"
    assert report["d5_compatibility_evidence_ready"] is True
    assert report["canonical_payload_bitwise_equal"] is True
    assert report["attempt_count"] == 32
    assert report["selection_performed"] is False
    assert report["ready_artifact_write_performed"] is False


def test_v2_writer_binds_c010_a5_and_v1_durable_readback_remains_supported(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")
    current = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)

    assert current["schema_version"] == subject.REPORT_SCHEMA_VERSION
    assert current["source_authority"]["c010_a5_report_sha256"] == subject.C010_A5_REPORT_SHA256
    assert current["source_authority"]["c010_a5_partition_sha256"] == subject.C010_A5_PARTITION_SHA256

    legacy_processes = []
    for process in (first, second):
        legacy = dict(process)
        legacy["schema_version"] = subject.PROCESS_SCHEMA_VERSION_V1
        legacy["source_authority"] = dict(subject.SOURCE_AUTHORITY_V1)
        legacy_body = {key: value for key, value in legacy.items() if key != "process_receipt_sha256"}
        legacy["process_receipt_sha256"] = canonical_sha256(legacy_body)
        legacy_processes.append(legacy)
    legacy_report = subject.build_controlled_refit_report(
        legacy_processes[0],
        legacy_processes[1],
        producer_commit="1" * 40,
        _schema_version=subject.REPORT_SCHEMA_VERSION_V1,
        _source_authority=subject.SOURCE_AUTHORITY_V1,
    )

    assert subject.validate_controlled_refit_report(legacy_report) == legacy_report


def test_controlled_report_keeps_downstream_failure_reason_without_rejecting_the_d1_mechanism(monkeypatch):
    treatment, control = _install_authority(monkeypatch)

    def process(process_identity: str) -> dict:
        attempts = [
            _attempt(
                subject.TREATMENT_ROLE,
                seed,
                process_identity,
                status="fit_failed" if seed == 42 else "fit_completed",
                failure_reason=("hmm_risk_model_covariance_acceptance_failed" if seed == 42 else None),
            )
            for seed in range(42, 50)
        ] + [_attempt(subject.CONTROL_ROLE, seed, process_identity) for seed in range(42, 50)]
        return subject.build_process_receipt(
            process_identity=process_identity,
            producer_commit="1" * 40,
            attempts=attempts,
            treatment_source_identities=treatment,
            control_source_identities=control,
        )

    report = subject.build_controlled_refit_report(
        process("fresh_process_1"),
        process("fresh_process_2"),
        producer_commit="1" * 40,
    )

    assert report["status"] == "diagnostic_complete"
    assert report["mechanism_assessment"] == "constant_dimension_effect_supported"
    assert report["d5_compatibility_evidence_ready"] is False
    assert report["mechanism_assessment_reason_codes"] == ["hmm_risk_model_covariance_acceptance_failed"]


def test_controlled_report_rejects_projection_mechanism_without_hiding_other_attempts(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1", failed_treatment_seed=45)
    second = _process(monkeypatch, "fresh_process_2", failed_treatment_seed=45)

    report = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)

    assert report["status"] == "diagnostic_complete"
    assert report["mechanism_assessment"] == "constant_dimension_mechanism_rejected"
    assert report["d5_compatibility_evidence_ready"] is False
    assert report["attempt_count"] == 32
    assert report["mechanism_assessment_reason_codes"] == ["hmm_risk_model_inactive_dimension_projection_invalid"]


def test_repeat_mismatch_is_inconclusive_not_fake_success(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")
    second["comparable_payload_sha256"] = "0" * 64
    body = {key: value for key, value in second.items() if key != "process_receipt_sha256"}
    second["process_receipt_sha256"] = canonical_sha256(body)

    report = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)

    assert report["status"] == "diagnostic_incomplete"
    assert report["mechanism_assessment"] == "inconclusive"
    assert report["d5_compatibility_evidence_ready"] is False
    assert "hmm_risk_model_inactive_dimension_repeat_mismatch" in report["mechanism_assessment_reason_codes"]


def test_self_consistent_process_schema_drift_is_inconclusive_not_mechanism_success(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")
    first["schema_version"] = "tampered_process_schema_v999"
    body = {key: value for key, value in first.items() if key != "process_receipt_sha256"}
    first["process_receipt_sha256"] = canonical_sha256(body)

    report = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)

    assert report["status"] == "diagnostic_incomplete"
    assert report["mechanism_assessment"] == "inconclusive"
    assert report["d5_compatibility_evidence_ready"] is False
    assert "hmm_risk_model_inactive_dimension_contract_invalid" in report["mechanism_assessment_reason_codes"]


def test_controlled_report_rejects_self_consistent_process_with_forbidden_side_effect(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")
    first["model_write_performed"] = True
    body = {key: value for key, value in first.items() if key != "process_receipt_sha256"}
    first["process_receipt_sha256"] = canonical_sha256(body)

    with pytest.raises(subject.D1InactiveDimensionError, match="forbidden side-effect"):
        subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)


def test_writer_is_immutable_and_canonical_readback_is_verified(tmp_path, monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")
    report = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)
    target = tmp_path / "d1.json"

    identity = subject.write_controlled_refit_report(target, report)

    assert identity == canonical_sha256(report)
    assert subject.write_controlled_refit_report(target, report) == identity
    target.write_text("{}\n", encoding="utf-8")
    with pytest.raises(subject.D1InactiveDimensionError, match="collision"):
        subject.write_controlled_refit_report(target, report)

    invalid = {**report, "receipt_sha256": "0" * 64}
    with pytest.raises(subject.D1InactiveDimensionError, match="report readback differs"):
        subject.write_controlled_refit_report(tmp_path / "invalid.json", invalid)


def test_report_writer_rejects_self_consistent_fake_success(monkeypatch, tmp_path):
    report = subject.build_controlled_refit_report(
        _process(monkeypatch, "fresh_process_1"),
        _process(monkeypatch, "fresh_process_2"),
        producer_commit="1" * 40,
    )
    report["d5_compatibility_evidence_ready"] = False
    body = {key: value for key, value in report.items() if key != "receipt_sha256"}
    report["receipt_sha256"] = canonical_sha256(body)

    with pytest.raises(subject.D1InactiveDimensionError, match="report readback differs"):
        subject.write_controlled_refit_report(tmp_path / "fake-success.json", report)


def test_controlled_report_requires_approved_fresh_process_identities(monkeypatch):
    report = subject.build_controlled_refit_report(
        _process(monkeypatch, "process-a"),
        _process(monkeypatch, "process-b"),
        producer_commit="1" * 40,
    )

    assert report["status"] == "diagnostic_incomplete"
    assert report["mechanism_assessment"] == "inconclusive"
    assert "hmm_risk_model_inactive_dimension_contract_invalid" in report["mechanism_assessment_reason_codes"]
