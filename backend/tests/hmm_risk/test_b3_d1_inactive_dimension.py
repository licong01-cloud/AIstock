from __future__ import annotations

from copy import deepcopy
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
        "mapping_manifest_hash": subject.C010_A5_MAPPING_SHA256,
        "calendar_manifest_hash": "c" * 64,
        "l2_stock_fact_manifest_hash": "6" * 64,
        "feature_domain_policy_sha256": "d" * 64,
    }
    return B3TrainOnlySeries(
        sector_code=sector_code,
        sector_name=sector_code,
        train_observations=observations,
        train_dates=dates,
        pit_l2_constituents=(sector_code,),
        pit_constituent_manifest_hash="e" * 64,
        observation_manifest_hash="f" * 64,
        train_input_manifest=manifest,
    )


def _preprocess() -> dict[str, object]:
    return {"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None}


def _lineage_migration_receipt(*, producer_commit: str = "1" * 40) -> dict[str, object]:
    pairs = {
        label: {
            "approved_receipt_sha256": canonical_sha256({"label": label, "side": "approved"}),
            "current_receipt_sha256": canonical_sha256({"label": label, "side": "current"}),
            "approved_semantic_payload_sha256": canonical_sha256({"label": label, "semantic": True}),
            "current_semantic_payload_sha256": canonical_sha256({"label": label, "semantic": True}),
        }
        for label in ("eligibility", "expected_opportunity", "provider_absence_partition")
    }
    body = {
        "schema_version": subject.C010_A5_LINEAGE_MIGRATION_SCHEMA_VERSION,
        "producer_commit": producer_commit,
        "source_a5_report_sha256": subject.C010_A5_REPORT_SHA256,
        "source_a5_partition_sha256": subject.C010_A5_PARTITION_SHA256,
        "status": "accepted",
        "excluded_non_business_fields": list(subject.C010_A5_LINEAGE_EXCLUDED_FIELDS),
        "receipt_pairs": pairs,
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    return {**body, "receipt_sha256": canonical_sha256(body)}


def _migration_receipts(*, producer_commit: str = "1" * 40) -> dict[str, dict[str, object]]:
    treatment = _series(subject.TREATMENT_SECTOR)
    control = _series(subject.CONTROL_SECTOR, inactive_value=7.0)
    return {
        role: subject.build_input_migration_receipt(
            item,
            historical_train_input_manifest=item.train_input_manifest,
            role=role,
            current_policy_sha256=str(item.train_input_manifest["feature_domain_policy_sha256"]),
            producer_commit=producer_commit,
            historical_observation_manifest_hash=(
                item.observation_manifest_hash if role == subject.CONTROL_ROLE else None
            ),
            historical_pit_constituent_manifest_hash=(
                item.pit_constituent_manifest_hash if role == subject.CONTROL_ROLE else None
            ),
            c010_a5_lineage_migration_receipt=_lineage_migration_receipt(producer_commit=producer_commit),
        )
        for role, item in (
            (subject.TREATMENT_ROLE, treatment),
            (subject.CONTROL_ROLE, control),
        )
    }


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
        initialization=_current_initialization_evidence(),
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


def _current_initialization_evidence() -> dict[str, object]:
    return {
        "schema_version": "hmm_risk_b3_manual_initialization_v1",
        "contract_version": training_subject.D3_CONTRACT_VERSION,
        "diagnostic_source_contract": "hmm_risk_c008_b3_diag04_manual_initialization_v1",
        "formal_initialization_contract_applied": True,
    }


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


def test_a5_input_migration_preserves_sector_core_and_normalizes_control_lineage(monkeypatch):
    _install_authority(monkeypatch)
    base = _series(subject.CONTROL_SECTOR, inactive_value=7.0)
    historical_manifest = {
        **dict(base.train_input_manifest),
        "l2_stock_fact_manifest_hash": "6" * 64,
        "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
    }
    current_manifest = {
        **historical_manifest,
        "dataset_manifest_hash": "7" * 64,
        "mapping_manifest_hash": "8" * 64,
        "calendar_manifest_hash": "9" * 64,
        "l2_stock_fact_manifest_hash": "a" * 64,
        "feature_domain_policy_sha256": "b" * 64,
    }
    monkeypatch.setattr(subject, "CONTROL_TRAIN_INPUT_MANIFEST_SHA256", canonical_sha256(historical_manifest))
    monkeypatch.setattr(subject, "C010_A5_MAPPING_SHA256", current_manifest["mapping_manifest_hash"])
    current = B3TrainOnlySeries(
        sector_code=base.sector_code,
        sector_name=base.sector_name,
        train_observations=base.train_observations,
        train_dates=base.train_dates,
        pit_l2_constituents=(subject.CONTROL_SECTOR,),
        pit_constituent_manifest_hash="c" * 64,
        observation_manifest_hash=base.observation_manifest_hash,
        train_input_manifest=current_manifest,
    )
    migration = subject.build_input_migration_receipt(
        current,
        historical_train_input_manifest=historical_manifest,
        role=subject.CONTROL_ROLE,
        current_policy_sha256=current_manifest["feature_domain_policy_sha256"],
        producer_commit="1" * 40,
        historical_observation_manifest_hash=base.observation_manifest_hash,
        historical_pit_constituent_manifest_hash="d" * 64,
        c010_a5_lineage_migration_receipt=_lineage_migration_receipt(),
    )

    projected, projection = subject.build_projection(
        current,
        preprocess=_preprocess(),
        role=subject.CONTROL_ROLE,
        input_migration_receipt=migration,
        **_projection_kwargs(subject.CONTROL_ROLE),
    )

    assert projected.shape == (120, 20)
    assert projection["historical_train_input_manifest_sha256"] == canonical_sha256(historical_manifest)
    assert projection["train_input_manifest_sha256"] == canonical_sha256(current_manifest)
    assert projection["input_migration_receipt_sha256"] == migration["receipt_sha256"]

    core = _core(20)
    current_hashes = subject._legacy_compatible_hashes(
        current,
        preprocess=_preprocess(),
        seed=42,
        numeric_environment={"environment": "fixed"},
        core=core,
        input_migration_receipt=migration,
    )
    historical = B3TrainOnlySeries(
        sector_code=base.sector_code,
        sector_name=base.sector_name,
        train_observations=base.train_observations,
        train_dates=base.train_dates,
        pit_l2_constituents=(subject.CONTROL_SECTOR,),
        pit_constituent_manifest_hash="d" * 64,
        observation_manifest_hash=base.observation_manifest_hash,
        train_input_manifest=historical_manifest,
    )
    historical_hashes = subject._legacy_compatible_hashes(
        historical,
        preprocess=_preprocess(),
        seed=42,
        numeric_environment={"environment": "fixed"},
        core=core,
    )
    assert current_hashes == historical_hashes

    drifted = B3TrainOnlySeries(
        **{
            **current.__dict__,
            "train_input_manifest": {**current_manifest, "train_observation_sha256": "e" * 64},
        }
    )
    with pytest.raises(subject.D1InactiveDimensionError, match="train core changed"):
        subject.build_input_migration_receipt(
            drifted,
            historical_train_input_manifest=historical_manifest,
            role=subject.CONTROL_ROLE,
            current_policy_sha256=current_manifest["feature_domain_policy_sha256"],
            producer_commit="1" * 40,
            historical_observation_manifest_hash=base.observation_manifest_hash,
            historical_pit_constituent_manifest_hash="d" * 64,
            c010_a5_lineage_migration_receipt=_lineage_migration_receipt(),
        )


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
    input_migration_receipt_sha256: str | None = None,
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
        "input_migration_receipt_sha256": input_migration_receipt_sha256,
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
    migrations = _migration_receipts()
    attempts = [
        _attempt(
            subject.TREATMENT_ROLE,
            seed,
            process_identity,
            status="projection_failed" if seed == failed_treatment_seed else "fit_completed",
            input_migration_receipt_sha256=migrations[subject.TREATMENT_ROLE]["receipt_sha256"],
        )
        for seed in range(42, 50)
    ] + [
        _attempt(
            subject.CONTROL_ROLE,
            seed,
            process_identity,
            input_migration_receipt_sha256=migrations[subject.CONTROL_ROLE]["receipt_sha256"],
        )
        for seed in range(42, 50)
    ]
    return subject.build_process_receipt(
        process_identity=process_identity,
        producer_commit="1" * 40,
        attempts=attempts,
        treatment_source_identities=treatment,
        control_source_identities=control,
        input_migration_receipts=migrations,
    )


def test_process_runner_never_early_stops_after_a_failed_attempt(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    migrations = _migration_receipts()
    calls: list[tuple[str, int]] = []

    def fake_attempt(item, *, role, seed, process_identity, **kwargs):
        calls.append((role, seed))
        status = "projection_failed" if role == subject.TREATMENT_ROLE and seed == 42 else "fit_completed"
        return _attempt(
            role,
            seed,
            process_identity,
            status=status,
            input_migration_receipt_sha256=kwargs["input_migration_receipt"]["receipt_sha256"],
        )

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
        treatment_input_migration_receipt=migrations[subject.TREATMENT_ROLE],
        control_input_migration_receipt=migrations[subject.CONTROL_ROLE],
    )

    assert calls == [(role, seed) for seed in range(42, 50) for role in (subject.TREATMENT_ROLE, subject.CONTROL_ROLE)]
    assert receipt["attempt_count"] == 16
    assert receipt["terminal_attempt_count"] == 16


def test_process_runner_validates_all_frozen_control_authority_before_first_fit(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    migrations = _migration_receipts()
    calls: list[tuple[str, int]] = []

    def fake_attempt(item, *, role, seed, process_identity, **kwargs):
        calls.append((role, seed))
        return _attempt(
            role,
            seed,
            process_identity,
            input_migration_receipt_sha256=kwargs["input_migration_receipt"]["receipt_sha256"],
        )

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
            treatment_input_migration_receipt=migrations[subject.TREATMENT_ROLE],
            control_input_migration_receipt=migrations[subject.CONTROL_ROLE],
        )
    assert calls == []


def test_process_receipt_rejects_self_consistent_attempt_with_forbidden_side_effect(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    migrations = _migration_receipts()
    attempts = [
        _attempt(
            role,
            seed,
            "process-a",
            input_migration_receipt_sha256=migrations[role]["receipt_sha256"],
        )
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
            input_migration_receipts=migrations,
        )


def test_process_receipt_rejects_self_consistent_input_migration_envelope_drift(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    migrations = _migration_receipts()
    drifted = deepcopy(migrations[subject.TREATMENT_ROLE])
    drifted["migrated_identity_fields"]["dataset_manifest_hash"]["current"] = "9" * 64
    body = {key: value for key, value in drifted.items() if key != "receipt_sha256"}
    drifted["receipt_sha256"] = canonical_sha256(body)
    migrations[subject.TREATMENT_ROLE] = drifted
    attempts = [
        _attempt(
            role,
            seed,
            "process-a",
            input_migration_receipt_sha256=migrations[role]["receipt_sha256"],
        )
        for seed in range(42, 50)
        for role in (subject.TREATMENT_ROLE, subject.CONTROL_ROLE)
    ]

    with pytest.raises(subject.D1InactiveDimensionError, match="migration receipt envelope"):
        subject.build_process_receipt(
            process_identity="process-a",
            producer_commit="1" * 40,
            attempts=attempts,
            treatment_source_identities=treatment,
            control_source_identities=control,
            input_migration_receipts=migrations,
        )

    lineage = _lineage_migration_receipt()
    lineage["receipt_pairs"]["eligibility"]["current_semantic_payload_sha256"] = "9" * 64
    lineage_body = {key: value for key, value in lineage.items() if key != "receipt_sha256"}
    lineage["receipt_sha256"] = canonical_sha256(lineage_body)
    with pytest.raises(subject.D1InactiveDimensionError, match="execution-lineage migration receipt"):
        subject._validate_c010_a5_lineage_migration_envelope(
            lineage,
            expected_producer_commit="1" * 40,
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


def test_v3_writer_binds_c010_a5_mapping_and_v1_v2_durable_readback_remains_supported(monkeypatch):
    first = _process(monkeypatch, "fresh_process_1")
    second = _process(monkeypatch, "fresh_process_2")
    current = subject.build_controlled_refit_report(first, second, producer_commit="1" * 40)

    assert current["schema_version"] == subject.REPORT_SCHEMA_VERSION
    assert current["source_authority"]["c010_a5_report_sha256"] == subject.C010_A5_REPORT_SHA256
    assert current["source_authority"]["c010_a5_partition_sha256"] == subject.C010_A5_PARTITION_SHA256
    assert current["source_authority"]["c010_a5_mapping_sha256"] == subject.C010_A5_MAPPING_SHA256

    for process_schema, report_schema, source_authority in (
        (subject.PROCESS_SCHEMA_VERSION_V1, subject.REPORT_SCHEMA_VERSION_V1, subject.SOURCE_AUTHORITY_V1),
        (subject.PROCESS_SCHEMA_VERSION_V2, subject.REPORT_SCHEMA_VERSION_V2, subject.SOURCE_AUTHORITY_V2),
    ):
        legacy_processes = []
        for process in (first, second):
            legacy = deepcopy(process)
            legacy["schema_version"] = process_schema
            legacy["source_authority"] = dict(source_authority)
            legacy.pop("input_migration_receipts", None)
            for attempt in legacy["attempts"]:
                attempt["schema_version"] = subject.ATTEMPT_SCHEMA_VERSION_V1
                attempt.pop("input_migration_receipt_sha256", None)
                attempt_body = {key: value for key, value in attempt.items() if key != "attempt_receipt_sha256"}
                attempt["attempt_receipt_sha256"] = canonical_sha256(attempt_body)
            legacy_comparable = [
                {
                    key: value
                    for key, value in attempt.items()
                    if key not in {"process_identity", "attempt_receipt_sha256"}
                }
                for attempt in legacy["attempts"]
            ]
            legacy["comparable_payload_sha256"] = canonical_sha256(legacy_comparable)
            legacy_body = {key: value for key, value in legacy.items() if key != "process_receipt_sha256"}
            legacy["process_receipt_sha256"] = canonical_sha256(legacy_body)
            legacy_processes.append(legacy)
        legacy_report = subject.build_controlled_refit_report(
            legacy_processes[0],
            legacy_processes[1],
            producer_commit="1" * 40,
            _schema_version=report_schema,
            _source_authority=source_authority,
        )

        assert subject.validate_controlled_refit_report(legacy_report) == legacy_report


def test_controlled_report_keeps_downstream_failure_reason_without_rejecting_the_d1_mechanism(monkeypatch):
    treatment, control = _install_authority(monkeypatch)
    migrations = _migration_receipts()

    def process(process_identity: str) -> dict:
        attempts = [
            _attempt(
                subject.TREATMENT_ROLE,
                seed,
                process_identity,
                status="fit_failed" if seed == 42 else "fit_completed",
                failure_reason=("hmm_risk_model_covariance_acceptance_failed" if seed == 42 else None),
                input_migration_receipt_sha256=migrations[subject.TREATMENT_ROLE]["receipt_sha256"],
            )
            for seed in range(42, 50)
        ] + [
            _attempt(
                subject.CONTROL_ROLE,
                seed,
                process_identity,
                input_migration_receipt_sha256=migrations[subject.CONTROL_ROLE]["receipt_sha256"],
            )
            for seed in range(42, 50)
        ]
        return subject.build_process_receipt(
            process_identity=process_identity,
            producer_commit="1" * 40,
            attempts=attempts,
            treatment_source_identities=treatment,
            control_source_identities=control,
            input_migration_receipts=migrations,
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


def _refit02_authorities(
    monkeypatch: pytest.MonkeyPatch,
    *,
    treatment_inactive_value: float = 0.0,
) -> tuple[B3TrainOnlySeries, B3TrainOnlySeries, dict, dict]:
    treatment = _series(subject.TREATMENT_SECTOR, inactive_value=treatment_inactive_value)
    harness = _series(subject.CONTROL_SECTOR, inactive_value=7.0)
    historical_treatment = {**dict(treatment.train_input_manifest), "dataset_manifest_hash": "8" * 64}
    historical_harness = {**dict(harness.train_input_manifest), "dataset_manifest_hash": "8" * 64}
    monkeypatch.setattr(
        subject,
        "TREATMENT_TRAIN_INPUT_MANIFEST_SHA256",
        canonical_sha256(historical_treatment),
    )
    monkeypatch.setattr(
        subject,
        "CONTROL_TRAIN_INPUT_MANIFEST_SHA256",
        canonical_sha256(historical_harness),
    )
    authority = subject.build_refit02_current_a5_authority(
        treatment_item=treatment,
        harness_item=harness,
        preprocess=_preprocess(),
        current_policy_sha256="d" * 64,
        producer_commit="1" * 40,
    )
    historical = subject.build_refit02_historical_reference_receipt(
        treatment_item=treatment,
        harness_item=harness,
        historical_treatment_manifest=historical_treatment,
        historical_harness_manifest=historical_harness,
    )
    return treatment, harness, authority, historical


def _refit02_fit(item, *, train, seed):
    del seed
    return _core(int(train.shape[1]))


def _refit02_negative_initialization_blocker(item, *, train, seed):
    del item, train, seed
    raise training_subject.B3TrainingStageError(
        "initialization",
        "hmm_risk_model_initialization_failed",
        ValueError("matched identity20 initialization blocker"),
    )


def _refit02_process(monkeypatch: pytest.MonkeyPatch, process_identity: str) -> dict:
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)
    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", _refit02_fit)
    return subject.run_refit02_process(
        treatment_item=treatment,
        harness_item=harness,
        preprocess=_preprocess(),
        process_identity=process_identity,
        producer_commit="1" * 40,
        numeric_environment={"environment": "fixed-single-thread"},
        current_authority=authority,
        historical_reference=historical,
    )


def _as_legacy_refit02_process(process: dict) -> dict:
    attempts = deepcopy(process["attempts"])
    for attempt in attempts:
        attempt["schema_version"] = subject.REFIT02_ATTEMPT_SCHEMA_VERSION_LEGACY
        attempt["fit_budget_contract_version"] = subject.REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
        if attempt["role"] == subject.REFIT02_MATCHED_NEGATIVE_ROLE:
            attempt.update(
                {
                    "status": "fit_failed",
                    "fit_status": "failed",
                    "fit_performed": False,
                    "role_outcome": "negative_control_blocker_reproduced",
                    "negative_control_blocker_reproduced": True,
                    "failure_stage": "initialization",
                    "failure_reason_codes": ["hmm_risk_model_initialization_failed"],
                    "failure_message": "legacy matched identity20 initialization blocker",
                    "parameter_payload": None,
                    "initialization_evidence": None,
                    "monitor_evidence": None,
                    "likelihood": None,
                    "covariance": None,
                    "train_occupancy": None,
                    "final_train_log_likelihood": None,
                }
            )
        body = {key: value for key, value in attempt.items() if key != "attempt_receipt_sha256"}
        attempt["attempt_receipt_sha256"] = canonical_sha256(body)
    return subject.build_refit02_process_receipt(
        process_identity=process["process_identity"],
        producer_commit=process["producer_commit"],
        attempts=attempts,
        current_authority=process["current_authority"],
        historical_reference=process["historical_reference"],
    )


def _as_original_refit02_process(process: dict) -> dict:
    attempts = deepcopy(process["attempts"])
    for attempt in attempts:
        attempt["schema_version"] = subject.REFIT02_ATTEMPT_SCHEMA_VERSION_ORIGINAL
        attempt.pop("fit_budget_contract_version", None)
        body = {key: value for key, value in attempt.items() if key != "attempt_receipt_sha256"}
        attempt["attempt_receipt_sha256"] = canonical_sha256(body)
    return subject.build_refit02_process_receipt(
        process_identity=process["process_identity"],
        producer_commit=process["producer_commit"],
        attempts=attempts,
        current_authority=process["current_authority"],
        historical_reference=process["historical_reference"],
    )


def test_refit02_current_authority_binds_matched_input_and_historical_drift(monkeypatch):
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)

    assert authority["current_profile_eligible"] is True
    role_inputs = authority["experiment_authority"]["role_inputs"]
    assert role_inputs[subject.REFIT02_TREATMENT_ROLE] == role_inputs[subject.REFIT02_MATCHED_NEGATIVE_ROLE]
    assert role_inputs[subject.REFIT02_HARNESS_ROLE]["sector_code"] == subject.CONTROL_SECTOR
    assert all(pair["historical_reference_status"] == "drift_observed" for pair in historical["pairs"].values())
    assert (
        subject.validate_refit02_current_a5_authority(
            authority,
            treatment_item=treatment,
            harness_item=harness,
            preprocess=_preprocess(),
        )
        == authority
    )
    assert (
        subject.validate_refit02_historical_reference_receipt(
            historical,
            current_authority=authority,
        )
        == historical
    )


def test_refit02_historical_reference_rejects_self_consistent_changed_path_tamper(monkeypatch):
    _, _, authority, historical = _refit02_authorities(monkeypatch)
    tampered = deepcopy(historical)
    tampered["pairs"][subject.TREATMENT_SECTOR]["changed_paths"] = []
    body = {key: value for key, value in tampered.items() if key != "receipt_sha256"}
    tampered["receipt_sha256"] = canonical_sha256(body)

    with pytest.raises(subject.D1InactiveDimensionError, match="historical reference pair"):
        subject.validate_refit02_historical_reference_receipt(
            tampered,
            current_authority=authority,
        )


def test_refit02_current_authority_rejects_self_consistent_eligibility_tamper(monkeypatch):
    _, _, authority, _ = _refit02_authorities(monkeypatch)
    tampered = deepcopy(authority)
    tampered["current_profile_eligible"] = False
    body = {key: value for key, value in tampered.items() if key != "receipt_sha256"}
    tampered["receipt_sha256"] = canonical_sha256(body)

    with pytest.raises(subject.D1InactiveDimensionError, match="authority envelope"):
        subject._validate_refit02_current_authority_envelope(tampered)


def test_refit02_current_profile_change_returns_not_applicable_without_attempts(monkeypatch):
    _, _, authority, historical = _refit02_authorities(monkeypatch, treatment_inactive_value=0.25)

    report = subject.build_refit02_not_applicable_report(
        authority,
        historical,
        producer_commit="1" * 40,
    )

    assert authority["current_profile_eligible"] is False
    assert report["status"] == "not_applicable"
    assert report["attempt_count"] == 0
    assert report["planned_hmm_fit_count"] == 0
    assert report["selection_performed"] is False
    assert report["ready_artifact_write_performed"] is False
    assert subject.validate_refit02_report(report) == report


def test_refit02_process_runs_24_terminal_attempts_with_24_planned_true_fits(monkeypatch):
    process = _refit02_process(monkeypatch, "fresh_process_1")

    assert process["attempt_count"] == 24
    assert process["terminal_attempt_count"] == 24
    assert process["planned_hmm_fit_count"] == 24
    assert process["actual_hmm_fit_invocation_count"] == 24
    assert len(process["attempts"]) == 24
    negative = [value for value in process["attempts"] if value["role"] == subject.REFIT02_MATCHED_NEGATIVE_ROLE]
    assert len(negative) == 8
    assert all(value["status"] == "fit_completed" for value in negative)
    assert all(value["fit_performed"] is True for value in negative)
    assert all(value["role_outcome"] == "matched_control_fit_completed" for value in negative)
    assert all(value["negative_control_blocker_reproduced"] is None for value in negative)
    assert process["selection_performed"] is False
    assert process["ready_artifact_write_performed"] is False


def test_refit02_under_budget_initialization_failure_preserves_terminal_evidence(monkeypatch):
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)

    def fit_with_one_initialization_failure(item, *, train, seed):
        if item.sector_code == subject.TREATMENT_SECTOR and train.shape[1] == 19 and seed == 42:
            raise training_subject.B3TrainingStageError(
                "initialization",
                "hmm_risk_model_initialization_failed",
                ValueError("treatment initialization failed before HMM fit"),
            )
        return _core(int(train.shape[1]))

    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", fit_with_one_initialization_failure)
    processes = [
        subject.run_refit02_process(
            treatment_item=treatment,
            harness_item=harness,
            preprocess=_preprocess(),
            process_identity=identity,
            producer_commit="1" * 40,
            numeric_environment={"environment": "fixed-single-thread"},
            current_authority=authority,
            historical_reference=historical,
        )
        for identity in ("fresh_process_1", "fresh_process_2")
    ]

    assert all(process["attempt_count"] == 24 for process in processes)
    assert all(process["terminal_attempt_count"] == 24 for process in processes)
    assert all(process["actual_hmm_fit_invocation_count"] == 23 for process in processes)
    failed = [
        attempt
        for attempt in processes[0]["attempts"]
        if attempt["role"] == subject.REFIT02_TREATMENT_ROLE and attempt["seed"] == 42
    ]
    assert len(failed) == 1
    assert failed[0]["fit_performed"] is False
    assert failed[0]["failure_stage"] == "initialization"
    report = subject.build_refit02_report(processes[0], processes[1], producer_commit="1" * 40)
    assert report["status"] == "diagnostic_failed"
    assert report["mechanism_assessment"] == "inconclusive"
    assert report["actual_hmm_fit_invocation_count"] == 46
    assert "hmm_risk_model_initialization_failed" in report["mechanism_assessment_reason_codes"]


def test_refit02_two_process_report_separates_completion_mechanism_and_d5_readiness(monkeypatch):
    first = _refit02_process(monkeypatch, "fresh_process_1")
    second = _refit02_process(monkeypatch, "fresh_process_2")

    report = subject.build_refit02_report(first, second, producer_commit="1" * 40)

    assert report["status"] == "diagnostic_complete"
    assert report["diagnostic_contract"] == subject.REFIT02_DIAGNOSTIC_CONTRACT
    assert report["mechanism_assessment"] == "constant_dimension_mechanism_rejected"
    assert report["d5_compatibility_evidence_ready"] is False
    assert report["canonical_payload_bitwise_equal"] is True
    assert report["attempt_count"] == 48
    assert report["planned_hmm_fit_count"] == 48
    assert report["actual_hmm_fit_invocation_count"] == 48
    assert (
        "hmm_risk_model_inactive_dimension_matched_control_fit_completed" in report["mechanism_assessment_reason_codes"]
    )
    assert report["formal_model_set_acceptance_performed"] is False
    assert report["hard_semantic_authority_changed"] is False
    assert report["selection_performed"] is False
    assert report["ready_artifact_write_performed"] is False
    assert subject.validate_refit02_report(report) == report


def test_refit02_matched_control_blocker_with_19d_fit_supports_dimension_effect(monkeypatch):
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)

    def fit_with_matched_control_blocker(item, *, train, seed):
        del seed
        if item.sector_code == subject.TREATMENT_SECTOR and train.shape[1] == 20:
            raise training_subject.B3TrainingStageError(
                "initialization",
                "hmm_risk_model_initialization_failed",
                ValueError("matched identity20 initialization blocker"),
            )
        return _core(int(train.shape[1]))

    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", fit_with_matched_control_blocker)
    processes = [
        subject.run_refit02_process(
            treatment_item=treatment,
            harness_item=harness,
            preprocess=_preprocess(),
            process_identity=identity,
            producer_commit="1" * 40,
            numeric_environment={"environment": "fixed-single-thread"},
            current_authority=authority,
            historical_reference=historical,
        )
        for identity in ("fresh_process_1", "fresh_process_2")
    ]

    report = subject.build_refit02_report(processes[0], processes[1], producer_commit="1" * 40)

    assert all(process["planned_hmm_fit_count"] == 24 for process in processes)
    assert all(process["actual_hmm_fit_invocation_count"] == 16 for process in processes)
    assert report["status"] == "diagnostic_complete"
    assert report["mechanism_assessment"] == "constant_dimension_effect_supported"
    assert report["d5_compatibility_evidence_ready"] is True
    assert report["planned_hmm_fit_count"] == 48
    assert report["actual_hmm_fit_invocation_count"] == 32
    assert "hmm_risk_model_initialization_failed" in report["mechanism_assessment_reason_codes"]
    assert subject.validate_refit02_report(report) == report


def test_refit02_v6_writer_is_immutable_and_rejects_self_consistent_fake_readiness(monkeypatch, tmp_path):
    report = subject.build_refit02_report(
        _refit02_process(monkeypatch, "fresh_process_1"),
        _refit02_process(monkeypatch, "fresh_process_2"),
        producer_commit="1" * 40,
    )
    target = tmp_path / "refit02.json"

    identity = subject.write_controlled_refit_report(target, report)

    assert identity == canonical_sha256(report)
    assert subject.write_controlled_refit_report(target, report) == identity
    tampered = deepcopy(report)
    tampered["d5_compatibility_evidence_ready"] = True
    body = {key: value for key, value in tampered.items() if key != "receipt_sha256"}
    tampered["receipt_sha256"] = canonical_sha256(body)
    with pytest.raises(subject.D1InactiveDimensionError, match="differs from its writer authority"):
        subject.write_controlled_refit_report(tmp_path / "fake-readiness.json", tampered)


def test_refit02_v6_writer_persists_complete_matched_control_rejection_report(monkeypatch, tmp_path):
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)
    monkeypatch.setattr(
        subject,
        "fit_b3_preprocessed_train_only",
        lambda item, train, seed: _core(int(train.shape[1])),
    )
    processes = [
        subject.run_refit02_process(
            treatment_item=treatment,
            harness_item=harness,
            preprocess=_preprocess(),
            process_identity=identity,
            producer_commit="1" * 40,
            numeric_environment={"environment": "fixed-single-thread"},
            current_authority=authority,
            historical_reference=historical,
        )
        for identity in ("fresh_process_1", "fresh_process_2")
    ]
    report = subject.build_refit02_report(processes[0], processes[1], producer_commit="1" * 40)

    assert report["status"] == "diagnostic_complete"
    assert report["mechanism_assessment"] == "constant_dimension_mechanism_rejected"
    assert report["attempt_count"] == 48
    assert report["actual_hmm_fit_invocation_count"] == 48
    assert "failed_process_receipt" not in report
    assert subject.validate_refit02_report(report) == report
    assert subject.write_controlled_refit_report(tmp_path / "diagnostic-failed.json", report) == canonical_sha256(
        report
    )


def test_refit02_matched_control_fit_rejects_dimension_mechanism_even_when_treatment_d4_fails(monkeypatch):
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)

    def fit_with_downstream_failure(item, *, train, seed):
        del seed
        if item.sector_code == subject.TREATMENT_SECTOR and train.shape[1] == 19:
            raise training_subject.B3TrainingStageError(
                "covariance",
                "hmm_risk_model_covariance_bounds_failed",
                ValueError("post-fit covariance rejected"),
            )
        return _core(int(train.shape[1]))

    monkeypatch.setattr(subject, "fit_b3_preprocessed_train_only", fit_with_downstream_failure)
    processes = [
        subject.run_refit02_process(
            treatment_item=treatment,
            harness_item=harness,
            preprocess=_preprocess(),
            process_identity=identity,
            producer_commit="1" * 40,
            numeric_environment={"environment": "fixed-single-thread"},
            current_authority=authority,
            historical_reference=historical,
        )
        for identity in ("fresh_process_1", "fresh_process_2")
    ]

    report = subject.build_refit02_report(processes[0], processes[1], producer_commit="1" * 40)

    assert report["status"] == "diagnostic_complete"
    assert report["mechanism_assessment"] == "constant_dimension_mechanism_rejected"
    assert report["d5_compatibility_evidence_ready"] is False
    assert "hmm_risk_model_covariance_bounds_failed" in report["mechanism_assessment_reason_codes"]


def test_refit02_matched_control_runs_real_hmm_fit_and_rejects_dimension_mechanism(monkeypatch):
    treatment, harness, authority, historical = _refit02_authorities(monkeypatch)
    fit_calls = []

    def fit_all_roles(item, *, train, seed):
        fit_calls.append((item.sector_code, int(train.shape[1]), seed))
        return _core(int(train.shape[1]))

    monkeypatch.setattr(
        subject,
        "fit_b3_preprocessed_train_only",
        fit_all_roles,
    )
    process = subject.run_refit02_process(
        treatment_item=treatment,
        harness_item=harness,
        preprocess=_preprocess(),
        process_identity="fresh_process_1",
        producer_commit="1" * 40,
        numeric_environment={"environment": "fixed-single-thread"},
        current_authority=authority,
        historical_reference=historical,
    )
    negative = [value for value in process["attempts"] if value["role"] == subject.REFIT02_MATCHED_NEGATIVE_ROLE]
    assert process["planned_hmm_fit_count"] == 24
    assert process["actual_hmm_fit_invocation_count"] == 24
    assert len(fit_calls) == 24
    assert all(value["negative_control_blocker_reproduced"] is None for value in negative)
    assert all(value["fit_performed"] is True for value in negative)
    assert all(value["status"] == "fit_completed" for value in negative)
    assert all(value["role_outcome"] == "matched_control_fit_completed" for value in negative)
    assert all(value["initialization_evidence"]["formal_initialization_contract_applied"] is True for value in negative)


def test_refit02_current_matched_fit_and_initialization_tamper_fail_closed(monkeypatch):
    process = _refit02_process(monkeypatch, "fresh_process_1")
    negative = next(value for value in process["attempts"] if value["role"] == subject.REFIT02_MATCHED_NEGATIVE_ROLE)

    fit_tampered = deepcopy(negative)
    fit_tampered["fit_performed"] = False
    fit_tampered["status"] = "fit_failed"
    fit_tampered["fit_status"] = "failed"
    fit_tampered["role_outcome"] = "negative_control_not_reproduced"
    fit_tampered["negative_control_blocker_reproduced"] = False
    fit_tampered["failure_stage"] = "initialization"
    fit_tampered["failure_reason_codes"] = ["hmm_risk_model_inactive_dimension_negative_control_not_reproduced"]
    fit_tampered["attempt_receipt_sha256"] = canonical_sha256(
        {key: value for key, value in fit_tampered.items() if key != "attempt_receipt_sha256"}
    )
    with pytest.raises(subject.D1InactiveDimensionError) as fit_error:
        subject._validate_refit02_attempt_receipt(
            fit_tampered,
            process_identity="fresh_process_1",
            current_authority=process["current_authority"],
        )
    assert fit_error.value.reason_code == "hmm_risk_model_inactive_dimension_contract_invalid"

    initialization_tampered = deepcopy(negative)
    initialization_tampered["initialization_evidence"] = {
        **_current_initialization_evidence(),
        "formal_initialization_contract_applied": False,
    }
    initialization_tampered["attempt_receipt_sha256"] = canonical_sha256(
        {key: value for key, value in initialization_tampered.items() if key != "attempt_receipt_sha256"}
    )
    with pytest.raises(subject.D1InactiveDimensionError, match="initialization evidence is not authoritative"):
        subject._validate_refit02_attempt_receipt(
            initialization_tampered,
            process_identity="fresh_process_1",
            current_authority=process["current_authority"],
        )


def test_refit02_current_and_legacy_schemas_are_explicit_and_cannot_mix(monkeypatch):
    current_first = _refit02_process(monkeypatch, "fresh_process_1")
    current_second = _refit02_process(monkeypatch, "fresh_process_2")
    legacy_first = _as_legacy_refit02_process(current_first)
    legacy_second = _as_legacy_refit02_process(current_second)
    original_first = _as_original_refit02_process(legacy_first)
    original_second = _as_original_refit02_process(legacy_second)

    assert legacy_first["schema_version"] == subject.REFIT02_PROCESS_SCHEMA_VERSION_LEGACY
    assert legacy_first["fit_budget_contract_version"] == subject.REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
    legacy_report = subject.build_refit02_report(legacy_first, legacy_second, producer_commit="1" * 40)
    assert legacy_report["schema_version"] == subject.REFIT02_REPORT_SCHEMA_VERSION_LEGACY
    assert legacy_report["diagnostic_contract"] == subject.REFIT02_DIAGNOSTIC_CONTRACT_LEGACY
    assert legacy_report["fit_budget_contract_version"] == subject.REFIT02_FIT_BUDGET_CONTRACT_VERSION_LEGACY
    assert subject.validate_refit02_report(legacy_report) == legacy_report

    assert original_first["schema_version"] == subject.REFIT02_PROCESS_SCHEMA_VERSION_ORIGINAL
    assert "fit_budget_contract_version" not in original_first
    original_report = subject.build_refit02_report(original_first, original_second, producer_commit="1" * 40)
    assert original_report["schema_version"] == subject.REFIT02_REPORT_SCHEMA_VERSION_ORIGINAL
    assert original_report["diagnostic_contract"] == subject.REFIT02_DIAGNOSTIC_CONTRACT_LEGACY
    assert "fit_budget_contract_version" not in original_report
    assert subject.validate_refit02_report(original_report) == original_report

    mixed_attempts = deepcopy(current_first["attempts"])
    mixed_attempts[0] = deepcopy(legacy_first["attempts"][0])
    with pytest.raises(subject.D1InactiveDimensionError, match="cannot mix current and legacy"):
        subject.build_refit02_process_receipt(
            process_identity="fresh_process_1",
            producer_commit="1" * 40,
            attempts=mixed_attempts,
            current_authority=current_first["current_authority"],
            historical_reference=current_first["historical_reference"],
        )

    downgraded_current = deepcopy(current_first["attempts"][0])
    downgraded_current.pop("fit_budget_contract_version")
    downgraded_current["attempt_receipt_sha256"] = canonical_sha256(
        {key: value for key, value in downgraded_current.items() if key != "attempt_receipt_sha256"}
    )
    with pytest.raises(subject.D1InactiveDimensionError, match="attempt receipt is invalid"):
        subject._validate_refit02_attempt_receipt(
            downgraded_current,
            process_identity="fresh_process_1",
            current_authority=current_first["current_authority"],
        )
