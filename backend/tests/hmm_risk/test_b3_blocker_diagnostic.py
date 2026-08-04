from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

from backend.services.hmm_risk import b3_blocker_diagnostic as diagnostic
from backend.services.hmm_risk.b3_acceptance import RESTART_SCHEDULE
from backend.services.hmm_risk.b3_training import B3FittedModel, B3TrainingStageError, B3TrainOnlySeries
from backend.services.hmm_risk.state_model_set import L1TrainingSeries, StateModelSetError, canonical_sha256


def _receipt(body: dict, field: str) -> dict:
    return {**body, field: canonical_sha256(body)}


def _codes(count: int) -> list[str]:
    return [f"S{index:03d}" for index in range(count)]


def _candidate(codes: list[str], seed: int, rejected: int) -> dict:
    hashes = [canonical_sha256({"seed": seed, "sector_code": code}) for code in codes]
    summary = [
        {
            "sector_code": code,
            "entry_receipt_sha256": hashes[index],
            "failed_stages": [
                {
                    "stage": "train_occupancy",
                    "status": "failed",
                    "valid": False,
                    "primary_reason_code": "hmm_risk_model_train_run_coverage_insufficient",
                    "failure_reason_codes": ["hmm_risk_model_train_run_coverage_insufficient"],
                    "blocking_reason_codes": [],
                }
            ],
        }
        for index, code in enumerate(codes[:rejected])
    ]
    return {
        "seed": seed,
        "schedule_index": RESTART_SCHEDULE.index(seed),
        "eligible": False,
        "entry_receipt_hashes": hashes,
        "rejection_summary": summary,
        "rejection_summary_sha256": canonical_sha256(summary),
    }


def _selection(family: str, level: str, rejected_counts: list[int]) -> dict:
    codes = _codes(31 if level == "L1" else 131)
    evidence = {
        "family": family,
        "level": level,
        "canonical_sector_codes": codes,
        "candidates": [
            _candidate(codes, seed, rejected) for seed, rejected in zip(RESTART_SCHEDULE, rejected_counts, strict=True)
        ],
    }
    return _receipt(
        {
            "level_selection_status": "blocked",
            "level_selection_valid": False,
            "evidence": evidence,
        },
        "receipt_sha256",
    )


def _formal_report() -> tuple[dict, dict[str, str]]:
    identity = canonical_sha256({"identity": "formal"})
    parameter_profile = canonical_sha256({"parameter": "profile"})
    selected_codes = ["801010.SI", *[f"801{index:03d}.SI" for index in range(20, 300, 10)], "801970.SI", "801980.SI"]
    assert len(selected_codes) == 31 and selected_codes == sorted(selected_codes)
    selected_entries = []
    for code in selected_codes:
        status = "failed" if code == "801980.SI" else "accepted"
        selected_entries.append(
            _receipt(
                {
                    "sector_code": code,
                    "seed": 43,
                    "parameter_profile_sha256": parameter_profile,
                    "semantic": {"semantic_evidence": {"semantic_evidence_status": status}},
                },
                "selected_entry_sha256",
            )
        )
    selected_artifact = _receipt(
        {"selected_seed": 43, "entry_count": 31, "entries": selected_entries},
        "artifact_sha256",
    )
    authority = {
        "producer_commit": identity,
        "receipt_sha256": "",
        "report_sha256": "",
        "dataset_manifest_hash": identity,
        "mapping_manifest_hash": identity,
        "calendar_manifest_hash": identity,
        "l2_stock_fact_manifest_hash": identity,
        "semantic_dataset_manifest_hash": identity,
        "semantic_mapping_manifest_hash": identity,
        "semantic_calendar_manifest_hash": identity,
        "semantic_l2_stock_fact_manifest_hash": identity,
        "feature_domain_policy_sha256": identity,
        "formula_version": "hmm_risk_l1_sector_factor_formula_v2_c010",
    }
    selections = {
        "autocycle_all_core:L1": _selection("autocycle_all_core", "L1", [2, 1, 1, 1, 1, 1, 1, 1]),
        "autocycle_all_core:L2": _selection("autocycle_all_core", "L2", [10, 10, 9, 9, 9, 9, 9, 9]),
        "legacy_covfix:L1": _receipt({"level_selection_status": "selected"}, "receipt_sha256"),
        "legacy_covfix:L2": _selection("legacy_covfix", "L2", [9, 9, 9, 8, 8, 8, 8, 8]),
    }
    body = {
        "schema_version": "hmm_risk_b3_repeated_preparation_receipt_v1",
        "producer_commit": identity,
        "dataset_manifest_hash": identity,
        "mapping_manifest_hash": identity,
        "calendar_manifest_hash": identity,
        "l2_stock_fact_manifest_hash": identity,
        "semantic_dataset_manifest_hash": identity,
        "semantic_mapping_manifest_hash": identity,
        "semantic_calendar_manifest_hash": identity,
        "semantic_l2_stock_fact_manifest_hash": identity,
        "feature_domain_policy_sha256": identity,
        "formula_version": authority["formula_version"],
        "status": "blocked",
        "selection_performed": True,
        "selection_used_validation": False,
        "selection_used_future_utility": False,
        "selection_followed_by_refit": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
        "family_model_set_statuses": {"autocycle_all_core": "blocked", "legacy_covfix": "blocked"},
        "selections": selections,
        "selected_artifacts": {"legacy_covfix:L1": selected_artifact},
    }
    report = _receipt(body, "receipt_sha256")
    authority["receipt_sha256"] = report["receipt_sha256"]
    authority["report_sha256"] = canonical_sha256(report)
    return report, authority


def test_target_manifest_derives_exact_approved_budget_and_controls() -> None:
    report, authority = _formal_report()

    target = diagnostic.derive_target_manifest(report, authority=authority)

    assert target["rejected_pair_count"] == 150
    assert target["control_pair_count"] == 24
    assert target["target_pair_count"] == 174
    assert target["total_fit_budget"] == 348
    assert [(item["role"], item["sector_code"]) for item in target["d6_replay_targets"]] == [
        ("rejected", "801980.SI"),
        ("control_before", "801970.SI"),
        ("control_after", "801010.SI"),
    ]


@pytest.mark.parametrize("mutation", ["report_hash", "rejection_hash", "missing_control"])
def test_target_manifest_fails_closed_on_authority_or_target_drift(mutation: str) -> None:
    report, authority = _formal_report()
    if mutation == "report_hash":
        authority["report_sha256"] = "0" * 64
    elif mutation == "rejection_hash":
        candidate = report["selections"]["autocycle_all_core:L1"]["evidence"]["candidates"][0]
        candidate["rejection_summary"][0]["entry_receipt_sha256"] = "0" * 64
        selection = report["selections"]["autocycle_all_core:L1"]
        selection["receipt_sha256"] = canonical_sha256(
            {key: value for key, value in selection.items() if key != "receipt_sha256"}
        )
        report["receipt_sha256"] = canonical_sha256(
            {key: value for key, value in report.items() if key != "receipt_sha256"}
        )
        authority["receipt_sha256"] = report["receipt_sha256"]
        authority["report_sha256"] = canonical_sha256(report)
    else:
        for candidate in report["selections"]["autocycle_all_core:L1"]["evidence"]["candidates"]:
            candidate["rejection_summary"] = [
                {
                    **row,
                    "sector_code": code,
                    "entry_receipt_sha256": candidate["entry_receipt_hashes"][index],
                }
                for index, (row, code) in enumerate(zip(candidate["rejection_summary"] * 31, _codes(31), strict=False))
            ][:31]
            candidate["rejection_summary_sha256"] = canonical_sha256(candidate["rejection_summary"])
        selection = report["selections"]["autocycle_all_core:L1"]
        selection["receipt_sha256"] = canonical_sha256(
            {key: value for key, value in selection.items() if key != "receipt_sha256"}
        )
        report["receipt_sha256"] = canonical_sha256(
            {key: value for key, value in report.items() if key != "receipt_sha256"}
        )
        authority["receipt_sha256"] = report["receipt_sha256"]
        authority["report_sha256"] = canonical_sha256(report)

    with pytest.raises(StateModelSetError):
        diagnostic.derive_target_manifest(report, authority=authority)


def _train_only_series(code: str = "S000") -> B3TrainOnlySeries:
    rows = np.zeros((120, 7), dtype=np.float64)
    dates = tuple(date(2023, 1, 1) + timedelta(days=index) for index in range(120))
    return B3TrainOnlySeries(
        sector_code=code,
        sector_name=code,
        train_observations=rows,
        train_dates=dates,
        pit_l2_constituents=("L2",),
        pit_constituent_manifest_hash="1" * 64,
        observation_manifest_hash="2" * 64,
        train_input_manifest={},
    )


def test_targeted_failure_preserves_stage_evidence_without_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    class InitializationFailure(ValueError):
        evidence = {"cluster_counts": [119, 1, 0], "empty_cluster": 2}

    cause = InitializationFailure("empty cluster")
    error = B3TrainingStageError(
        "fit",
        "hmm_risk_model_initialization_failed",
        cause,
    )
    monkeypatch.setattr(diagnostic, "_fit_preprocess", lambda *_args, **_kwargs: {"family": "identity"})
    monkeypatch.setattr(
        diagnostic,
        "c008_b3_diag04_fixed_numeric_environment",
        lambda: {"packages": {"hmmlearn": "0.3.3"}},
    )
    monkeypatch.setattr(diagnostic, "fit_b3_target_entry", lambda *_args, **_kwargs: (_ for _ in ()).throw(error))
    item = _train_only_series()
    formal_body = {
        "schema_version": "hmm_risk_b3_training_entry_receipt_v1",
        "contract_version": diagnostic.formal_b3_parameter_profile()["contract"],
        "retrain_contract_version": None,
        "family": "legacy_covfix",
        "level": "L1",
        "seed": 42,
        "sector_code": item.sector_code,
        "feature_count": 7,
        "training_rows": 120,
        "fit_status": "failed",
        "model_entry_status": "failed",
        "model_entry_valid": False,
        "failure_stage": "fit",
        "failure_reason_codes": ["hmm_risk_model_initialization_failed"],
        "failure_type": "InitializationFailure",
        "failure_message": "fit: empty cluster",
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "artifact_write_performed": False,
    }
    target = {
        "role": "rejected",
        "family": "legacy_covfix",
        "level": "L1",
        "seed": 42,
        "sector_code": item.sector_code,
        "source_entry_receipt_sha256": canonical_sha256(formal_body),
        "formal_failed_stages": [],
    }

    [result] = diagnostic.run_targeted_level(
        {item.sector_code: item},
        [target],
        family="legacy_covfix",
        level="L1",
        feature_names=("f",) * 7,
        preprocess_family="identity",
    )

    assert result["status"] == "fit_failed"
    assert result["formal_entry_receipt_reproduced"] is True
    assert result["diagnostic_failure_evidence"] == cause.evidence
    assert result["validation_accessed"] is False
    assert result["selection_performed"] is False

    with pytest.raises(StateModelSetError, match="control replayed as a fit failure"):
        diagnostic.run_targeted_level(
            {item.sector_code: item},
            [{**target, "role": "control"}],
            family="legacy_covfix",
            level="L1",
            feature_names=("f",) * 7,
            preprocess_family="identity",
        )


def test_signed_distance_receipt_keeps_each_acceptance_boundary() -> None:
    entry = {
        "likelihood": {"evidence": {"deltas": [{"relative": -1e-5, "absolute": -0.2, "terminal": True}]}},
        "covariance": {
            "evidence": {
                "raw_covars": [[1.0], [2.0], [3.0]],
                "dynamic_lower_reference": [[0.5], [0.5], [0.5]],
                "dynamic_upper_reference": [[4.0], [4.0], [4.0]],
                "mstep_relative_residual": [[0.01], [0.02], [0.03]],
            }
        },
        "train_occupancy": {
            "evidence": {
                "count_threshold": 5,
                "row_sum_max_abs_error": 1e-14,
                "top1_top2_min_margin": 0.1,
                "states": {
                    str(state): {
                        "hard_count": 10,
                        "normalized_occupancy": 0.1,
                        "calendar_month_count": 4,
                        "contiguous_run_count": 4,
                        "incoming_transition_count": 3,
                        "outgoing_transition_count": 3,
                        "maximum_single_run_share": 0.5,
                    }
                    for state in range(3)
                },
            }
        },
    }
    hard = {"run_lengths_by_state": {str(state): [2, 3, 5] for state in range(3)}}

    distances = diagnostic._signed_distances(entry, hard)

    assert distances["likelihood_delta_distances"][0]["terminal_negative_distance_to_minus_2e_5"] == pytest.approx(1e-5)
    assert distances["covariance_min_mstep_slack"] == pytest.approx(-0.01)
    assert distances["state_distances"]["0"]["count_slack"] == 5
    assert distances["posterior_normalization_slack"] > 0.0


def test_matched_comparison_closes_all_rejected_pairs_without_changing_acceptance() -> None:
    evidence = []
    groups = (
        [("autocycle_all_core", "L1", seed) for seed in RESTART_SCHEDULE]
        + [("autocycle_all_core", "L2", seed) for seed in RESTART_SCHEDULE]
        + [("legacy_covfix", "L2", seed) for seed in RESTART_SCHEDULE]
    )

    def add_entry(body: dict) -> None:
        evidence.append({**body, "diagnostic_entry_sha256": canonical_sha256(body)})

    for group_index, (family, level, seed) in enumerate(groups):
        common = {
            "family": family,
            "level": level,
            "seed": seed,
            "source_entry_receipt_sha256": canonical_sha256({"group": group_index, "role": "source"}),
            "train_input_manifest_sha256": canonical_sha256({"group": group_index, "role": "input"}),
            "fitted_model_payload": {"model_payload_sha256": canonical_sha256({"group": group_index})},
            "status": "fit_completed",
            "formal_entry_receipt_reproduced": True,
            "missing_evidence": [],
            "validation_accessed": False,
            "future_utility_accessed": False,
            "selection_performed": False,
            "model_write_performed": False,
        }
        add_entry(
            {
                **common,
                "role": "control",
                "sector_code": f"CONTROL-{group_index:02d}",
                "formal_failed_stages": [],
                "signed_distances": {"covariance_min_lower_slack": 0.25},
            }
        )
        rejected_count = 7 if group_index < 6 else 6
        for rejected_index in range(rejected_count):
            add_entry(
                {
                    **common,
                    "role": "rejected",
                    "sector_code": f"REJECTED-{group_index:02d}-{rejected_index:02d}",
                    "formal_failed_stages": [
                        {
                            "stage": "covariance",
                            "valid": False,
                            "failure_reason_codes": ["hmm_risk_model_covariance_acceptance_failed"],
                            "blocking_reason_codes": [],
                        }
                    ],
                    "signed_distances": {"covariance_min_lower_slack": -0.1},
                }
            )

    comparison = diagnostic.build_matched_comparisons(evidence)

    assert comparison["aggregate"]["comparison_count"] == 150
    assert comparison["aggregate"]["classification_counts"] == {"covariance_failure": 150}
    assert comparison["aggregate"]["stage_counts"] == {"covariance": 150}
    assert comparison["aggregate"]["missing_evidence_entry_count"] == 0
    assert comparison["comparisons"][0]["matched_numeric_differences"] == [
        {
            "path": "covariance_min_lower_slack",
            "rejected_value": -0.1,
            "control_value": 0.25,
            "rejected_minus_control": -0.35,
        }
    ]


def _semantic_series(code: str) -> L1TrainingSeries:
    train_dates = tuple(date(2023, 1, 1) + timedelta(days=index) for index in range(120))
    validation_dates = tuple(date(2024, 7, 1) + timedelta(days=index) for index in range(30))
    return L1TrainingSeries(
        sector_code=code,
        sector_name=code,
        train_observations=np.zeros((120, 7)),
        train_dates=train_dates,
        validation_observations=np.zeros((30, 7)),
        validation_dates=validation_dates,
        validation_future_utility=np.zeros(30),
        pit_l2_constituents=("L2",),
        pit_constituent_manifest_hash="1" * 64,
        observation_manifest_hash="2" * 64,
    )


def test_d6_replay_uses_selected_model_without_refit_or_reselection(monkeypatch: pytest.MonkeyPatch) -> None:
    codes = ("801980.SI", "801970.SI", "801010.SI")
    states = {
        str(state): {
            "hard_count": 10,
            "normalized_occupancy": 1 / 3,
            "calendar_month_count": 2,
            "contiguous_run_count": 10,
            "incoming_transition_count": 9,
            "outgoing_transition_count": 9,
            "maximum_single_run_share": 0.1,
        }
        for state in range(3)
    }
    semantic = {
        "assignment": {
            "evidence": {
                "states": states,
                "row_sum_max_abs_error": 0.0,
                "top1_top2_min_margin": 1.0,
            }
        },
        "semantic_evidence": {
            "semantic_evidence_status": "failed",
            "evidence": {
                "state_utility": {
                    "0": {"count": 10, "mean": -0.01, "sample_variance_ddof_1": 0.001},
                    "1": {"count": 10, "mean": 0.0, "sample_variance_ddof_1": 0.001},
                    "2": {"count": 10, "mean": 0.01, "sample_variance_ddof_1": 0.001},
                }
            },
        },
    }
    entries = {
        code: {"sector_code": code, "selected_entry_sha256": canonical_sha256({"code": code}), "semantic": semantic}
        for code in codes
    }
    report = {"selected_artifacts": {"legacy_covfix:L1": {"entries": list(entries.values())}}}
    targets = {
        "d6_replay_targets": [
            {
                "role": "rejected",
                "sector_code": codes[0],
                "selected_entry_sha256": entries[codes[0]]["selected_entry_sha256"],
            },
            {
                "role": "control_before",
                "sector_code": codes[1],
                "selected_entry_sha256": entries[codes[1]]["selected_entry_sha256"],
            },
            {
                "role": "control_after",
                "sector_code": codes[2],
                "selected_entry_sha256": entries[codes[2]]["selected_entry_sha256"],
            },
        ]
    }
    model = B3FittedModel(
        family="legacy_covfix",
        level="L1",
        seed=43,
        sector_code="unused",
        feature_names=("f",) * 7,
        preprocess={"family": "identity"},
        startprob=np.array([1 / 3] * 3),
        transmat=np.eye(3),
        means=np.zeros((3, 7)),
        covars=np.ones((3, 7)),
        parameter_profile_sha256="3" * 64,
        numeric_environment_sha256="4" * 64,
        observation_manifest_hash="5" * 64,
        pit_constituent_manifest_hash="6" * 64,
        model_payload_sha256="7" * 64,
    )
    posterior = np.eye(3)[np.arange(30) % 3]
    hard_detail = diagnostic._hard_sequence_detail(posterior, tuple(range(30)))
    semantic["assignment"]["evidence"].update(
        {
            "hard_assignment_sha256": hard_detail["hard_assignment_sha256"],
            "transition_counts": hard_detail["transition_counts"],
        }
    )
    monkeypatch.setattr(diagnostic, "_selected_model", lambda _entry: model)
    monkeypatch.setattr(diagnostic, "causal_forward_posteriors", lambda *_args, **_kwargs: posterior)
    monkeypatch.setattr(diagnostic, "evaluate_semantic_validation", lambda *_args, **_kwargs: semantic)

    result = diagnostic.replay_selected_d6(
        report,
        {code: _semantic_series(code) for code in codes},
        targets,
    )

    assert len(result) == 3
    assert all(item["refit_performed"] is False for item in result)
    assert all(item["selection_reexecuted"] is False for item in result)
    assert all(item["soft_evidence_used_for_acceptance"] is False for item in result)
    assert all(len(item["adjacent_utility_gap_distances"]) == 2 for item in result)
