from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path

import numpy as np
import pytest

from backend.services.hmm_risk import b3_remediation_diagnostic as diagnostic
from backend.services.hmm_risk.b3_training import B3TrainOnlySeries
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_json_bytes, canonical_sha256


def _series(values: np.ndarray, *, code: str = "801010.SI") -> B3TrainOnlySeries:
    rows = int(values.shape[0])
    dates = tuple(date(2022, 1, 1) + timedelta(days=index) for index in range(rows))
    date_strings = [value.isoformat() for value in dates]
    manifest = {
        "schema_version": "hmm_risk_d4_train_frozen_input_manifest_v1",
        "direct_sector_level": "L1",
        "sector_code": code,
        "train_dates": date_strings,
        "train_dates_sha256": canonical_sha256(date_strings),
        "train_observation_sha256": canonical_sha256(values.tolist()),
        "dataset_manifest_hash": "1" * 64,
        "mapping_manifest_hash": "2" * 64,
        "calendar_manifest_hash": "3" * 64,
        "feature_domain_policy_sha256": "4" * 64,
    }
    return B3TrainOnlySeries(
        sector_code=code,
        sector_name=code,
        train_observations=values,
        train_dates=dates,
        pit_l2_constituents=("L2-1",),
        pit_constituent_manifest_hash="5" * 64,
        observation_manifest_hash="6" * 64,
        train_input_manifest=manifest,
    )


def _completed_entry(
    *,
    code: str,
    role: str,
    relative: float,
    mstep: float,
    lower_slack: float,
    upper_slack: float,
    anomaly_count: int,
) -> dict:
    body = {
        "role": role,
        "family": "legacy_covfix",
        "level": "L2",
        "seed": 42,
        "sector_code": code,
        "source_entry_receipt_sha256": canonical_sha256({"source": code}),
        "formal_failed_stages": [],
        "train_input_manifest_sha256": canonical_sha256({"train": code}),
        "status": "fit_completed",
        "training_receipt": {
            "likelihood": {"likelihood_status": "accepted" if relative >= 0 else "failed"},
            "covariance": {
                "evidence": {
                    "mstep_max_abs_relative_residual": mstep,
                    "below_count": anomaly_count,
                    "above_count": 0,
                }
            },
            "validation_accessed": False,
            "future_utility_accessed": False,
            "selection_performed": False,
        },
        "signed_distances": {
            "likelihood_delta_distances": [
                {
                    "relative": relative,
                    "terminal": True,
                    "index": 1,
                    "previous": -10.0,
                    "current": -10.0 + relative,
                    "absolute": relative,
                }
            ],
            "covariance_min_lower_slack": lower_slack,
            "covariance_min_upper_slack": upper_slack,
            "covariance_min_mstep_slack": 1.0,
            "state_distances": {
                "0": {
                    "count_slack": -1 if code.endswith("0") else 1,
                    "month_slack": 1,
                    "run_slack": 1,
                    "incoming_transition_slack": 1,
                    "outgoing_transition_slack": 1,
                    "run_concentration_slack": 0.1,
                    "run_lengths": [2, 3],
                }
            },
        },
        "hard_sequence_detail": {
            "hard_assignment_sha256": canonical_sha256([0, 0, 1]),
            "run_lengths_by_state": {"0": [2, 3]},
            "run_lengths_sha256": canonical_sha256({"0": [2, 3]}),
            "transition_counts": [[1, 1, 0], [0, 1, 0], [0, 0, 0]],
        },
        "fitted_model_payload": {
            "preprocess": {
                "family": "identity",
                "winsor_low": None,
                "winsor_high": None,
                "center": None,
                "scale": None,
            }
        },
        "validation_accessed": False,
        "future_utility_accessed": False,
        "selection_performed": False,
        "model_write_performed": False,
    }
    return {**body, "diagnostic_entry_sha256": canonical_sha256(body)}


def test_profile_variance_keeps_raw_negative_zero_identity_and_normalizes_unique_values() -> None:
    values = np.tile(np.array([[0.0, -0.0], [1.0, 1.0]], dtype="<f8"), (60, 1))
    evidence = diagnostic.build_profile_variance_evidence(
        _series(values),
        family="legacy_covfix",
        level="L1",
        feature_names=("f0", "f1"),
        preprocess={"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None},
        feature_definition={"schema_version": "formula-v1"},
        source_provenance={"receipt_sha256": "7" * 64},
    )

    assert evidence["row_count"] == 120
    assert evidence["raw_observation_dtype"] == "<f8"
    assert evidence["features"][0]["unique_finite_value_count"] == 2
    assert evidence["features"][1]["unique_finite_value_count"] == 2
    assert evidence["features"][0]["raw_float64_sha256"] != evidence["features"][1]["raw_float64_sha256"]
    assert evidence["features"][0]["raw"]["var_ddof0"] == pytest.approx(0.25)
    assert evidence["features"][0]["preprocessed"]["var_ddof0"] == pytest.approx(0.25)


def test_profile_variance_fails_closed_on_temporal_or_numeric_drift() -> None:
    values = np.ones((120, 2), dtype=np.float64)
    series = _series(values)
    invalid_dates = B3TrainOnlySeries(**{**series.__dict__, "train_dates": tuple(reversed(series.train_dates))})
    with pytest.raises(StateModelSetError, match="strictly increasing"):
        diagnostic.build_profile_variance_evidence(
            invalid_dates,
            family="legacy_covfix",
            level="L1",
            feature_names=("f0", "f1"),
            preprocess={"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None},
            feature_definition={"schema_version": "formula-v1"},
            source_provenance={"receipt_sha256": "7" * 64},
        )
    non_finite = values.copy()
    non_finite[0, 0] = np.nan
    invalid_numeric = B3TrainOnlySeries(**{**series.__dict__, "train_observations": non_finite})
    with pytest.raises(StateModelSetError, match="non-finite"):
        diagnostic.build_profile_variance_evidence(
            invalid_numeric,
            family="legacy_covfix",
            level="L1",
            feature_names=("f0", "f1"),
            preprocess={"family": "identity", "winsor_low": None, "winsor_high": None, "center": None, "scale": None},
            feature_definition={"schema_version": "formula-v1"},
            source_provenance={"receipt_sha256": "7" * 64},
        )


def test_variance_distribution_uses_fixed_linear_quantiles_without_floor() -> None:
    profiles = []
    for index, variance in enumerate((1.0, 4.0, 9.0)):
        profiles.append(
            {
                "family": "legacy_covfix",
                "level": "L1",
                "features": [
                    {
                        "feature_name": "f0",
                        "raw": {"var_ddof0": variance},
                        "preprocessed": {"var_ddof0": variance},
                    }
                ],
            }
        )
    result = diagnostic.aggregate_variance_distributions(profiles)
    distribution = result["legacy_covfix:L1:f0"]["raw"]
    assert distribution["positive_count"] == 3
    assert distribution["quantiles"]["0.5"] == pytest.approx(4.0)
    assert distribution["quantiles"]["0.25"] == pytest.approx(2.5)
    assert "floor" not in distribution


def test_projection_excludes_d6_and_requires_no_access_flags() -> None:
    entries = [
        _completed_entry(
            code=f"L2-{index}",
            role="control" if index == 3 else "rejected",
            relative=float(index),
            mstep=float(index + 1),
            lower_slack=1.0,
            upper_slack=1.0,
            anomaly_count=0,
        )
        for index in range(4)
    ]
    blocker = {
        "schema_version": "hmm_risk_c008_b3_formal_blocker_diag01_v1",
        "diagnostic_contract": "C-008-B3-FORMAL-BLOCKER-DIAG-01",
        "diagnostic_producer_commit": "a" * 40,
        "formal_authority": {"producer_commit": "b" * 40},
        "numeric_environment": {"packages": {}},
        "numeric_environment_sha256": canonical_sha256({"packages": {}}),
        "targeted_evidence": entries,
        "d6_replay": [{"validation_observations": "forbidden"}],
    }
    projection = diagnostic.build_train_only_projection(blocker)
    assert "d6_replay" not in json.dumps(projection, sort_keys=True)
    assert projection["targeted_evidence_count"] == 4
    drifted = {**blocker, "targeted_evidence": [{**entries[0], "validation_accessed": True}, *entries[1:]]}
    with pytest.raises(StateModelSetError, match="train projection"):
        diagnostic.build_train_only_projection(drifted)


def test_completed_entry_analysis_builds_exact_matrix_correlations_and_structure() -> None:
    entries = [
        _completed_entry(
            code=f"L2-{index}",
            role="control" if index >= 3 else "rejected",
            relative=float(index + 1),
            mstep=float(index + 2),
            lower_slack=-1.0 if index == 0 else 1.0,
            upper_slack=1.0,
            anomaly_count=1 if index == 0 else 0,
        )
        for index in range(5)
    ]
    result = diagnostic.analyze_completed_entries(entries)
    assert result["entry_count"] == 5
    assert result["cross_matrix"]["accepted"]["failed_bounds_only"]["count"] == 1
    overall = result["correlations"]["overall"]["mstep_max_abs_relative_residual"]
    assert overall["status"] == "complete"
    assert overall["pearson"] == pytest.approx(1.0)
    assert overall["spearman"] == pytest.approx(1.0)
    assert result["train_structure"]["failure_intersections"]["count"] == 1


def test_completed_entry_analysis_rejects_non_finite_instead_of_faking_insufficient() -> None:
    entry = _completed_entry(
        code="L2-0",
        role="rejected",
        relative=0.0,
        mstep=0.01,
        lower_slack=1.0,
        upper_slack=1.0,
        anomaly_count=0,
    )
    entry["signed_distances"]["likelihood_delta_distances"][-1]["relative"] = float("nan")
    with pytest.raises(StateModelSetError, match="non-finite"):
        diagnostic.analyze_completed_entries([entry])


def test_initialization_source_requires_exact_persistent_and_singleton_identities() -> None:
    failures = []
    for seed in range(42, 50):
        failures.append(
            {
                "status": "fit_failed",
                "family": "autocycle_all_core",
                "level": "L2",
                "sector_code": "801207.SI",
                "seed": seed,
                "source_entry_receipt_sha256": canonical_sha256({"seed": seed}),
                "projection_entry_sha256": canonical_sha256({"projection": seed}),
                "diagnostic_entry_sha256": canonical_sha256({"diagnostic": seed}),
                "diagnostic_failure_evidence": {"reference_variance": [1.0] * 19 + [0.0]},
            }
        )
    for identity, counts in diagnostic.EXPECTED_SINGLETON_FAILURES.items():
        family, level, sector_code, seed = identity
        failures.append(
            {
                "status": "fit_failed",
                "family": family,
                "level": level,
                "sector_code": sector_code,
                "seed": seed,
                "source_entry_receipt_sha256": canonical_sha256({"singleton": identity}),
                "projection_entry_sha256": canonical_sha256({"projection": identity}),
                "diagnostic_entry_sha256": canonical_sha256({"diagnostic": identity}),
                "diagnostic_failure_evidence": {"cluster_counts": counts},
            }
        )
    evidence = diagnostic.build_initialization_source_evidence({"targeted_evidence": failures})
    assert evidence["persistent_zero_variance_count"] == 8
    assert evidence["singleton_cluster_count"] == 3

    drifted = [dict(entry) for entry in failures]
    drifted[-1] = {**drifted[-1], "diagnostic_failure_evidence": {"cluster_counts": [1, 1, 596]}}
    with pytest.raises(StateModelSetError, match="singleton"):
        diagnostic.build_initialization_source_evidence({"targeted_evidence": drifted})


def test_artifact_writer_is_idempotent_and_rejects_collision(tmp_path) -> None:
    path = tmp_path / "diag.json"
    report = {"schema_version": diagnostic.SCHEMA_VERSION, "status": "diagnostic_complete"}
    first = diagnostic.write_diagnostic_artifact(path, report)
    second = diagnostic.write_diagnostic_artifact(path, report)
    assert first == second == canonical_sha256(report)
    with pytest.raises(StateModelSetError, match="collision"):
        diagnostic.write_diagnostic_artifact(path, {**report, "status": "failed"})


def test_artifact_writer_never_overwrites_concurrent_publisher(monkeypatch, tmp_path) -> None:
    path = tmp_path / "diag.json"
    report = {"schema_version": diagnostic.SCHEMA_VERSION, "status": "diagnostic_complete"}
    concurrent = {"schema_version": diagnostic.SCHEMA_VERSION, "status": "failed"}
    concurrent_payload = canonical_json_bytes(concurrent) + b"\n"

    def publish_first(_source, target) -> None:
        Path(target).write_bytes(concurrent_payload)
        raise FileExistsError

    monkeypatch.setattr(diagnostic.os, "link", publish_first)
    with pytest.raises(StateModelSetError, match="collision"):
        diagnostic.write_diagnostic_artifact(path, report)
    assert path.read_bytes() == concurrent_payload
