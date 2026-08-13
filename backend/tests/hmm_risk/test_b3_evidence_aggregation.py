from __future__ import annotations

import copy
import os
from pathlib import Path

import pytest

from backend.services.hmm_risk.b3_evidence_aggregation import (
    AGGREGATION_SCHEMA_VERSION,
    B3EvidenceAggregationError,
    EXPECTED_SEEDS,
    K2_HYPOTHESIS,
    aggregate_transition_dwell_evidence,
    build_aggregation_failure,
    write_aggregation_report,
)
from backend.services.hmm_risk.state_model_set import canonical_json_bytes, canonical_sha256


def _window(*, observed: bool) -> dict:
    reasons = [] if observed else ["hmm_risk_train_stability_transition_insufficient"]
    return {
        "status": "train_window_structurally_observed" if observed else "train_window_structurally_unobserved",
        "reason_codes": reasons,
    }


def _child(process_identity: str, *, invalid_sector: str | None = None) -> dict:
    sector_codes = [f"801{index:03d}.SI" for index in range(1, 132)]
    entries = []
    profiles = []
    for seed in EXPECTED_SEEDS:
        for index, sector_code in enumerate(sector_codes):
            entry = {
                "artifact_write_performed": False,
                "seed": seed,
                "sector_code": sector_code,
                "fit_status": "accepted",
                "model_entry_status": "accepted",
                "model_entry_valid": True,
                "likelihood": {
                    "convergence_valid": True,
                    "likelihood_valid": True,
                    "likelihood_status": "accepted",
                    "failure_reason_codes": [],
                    "blocking_reason_codes": [],
                },
                "covariance": {
                    "covariance_valid": True,
                    "covariance_status": "accepted",
                    "failure_reason_codes": [],
                    "blocking_reason_codes": [],
                },
                "train_occupancy": {
                    "train_occupancy_valid": True,
                    "train_occupancy_status": "accepted",
                    "failure_reason_codes": [],
                    "blocking_reason_codes": [],
                },
            }
            if index == 1 and seed == 42:
                entry["model_entry_status"] = "failed"
                entry["model_entry_valid"] = False
                entry["covariance"] = {
                    "covariance_valid": False,
                    "covariance_status": "failed",
                    "failure_reason_codes": ["hmm_risk_model_covariance_bounds_failed"],
                    "blocking_reason_codes": [],
                }
            entries.append(entry)
            persistent = index == 0
            profiles.append(
                {
                    "both_windows_structurally_observed": not persistent,
                    "seed": seed,
                    "sector_code": sector_code,
                    "early": _window(observed=not persistent),
                    "late": _window(observed=not persistent),
                }
            )
    domains = []
    date_count = 2 if invalid_sector else 1
    for index, code in enumerate(sector_codes):
        for day_index in range(date_count):
            if code == invalid_sector and day_index == 1:
                continue
            domains.append(
                {
                    "direct_sector_level": "L2",
                    "sector_code": code,
                    "trade_date": f"2024-01-0{day_index + 2}",
                    "price_expected_weight": float(index + 1),
                    "moneyflow_contributor_amount": float(1000 - index),
                }
            )
    invalid_domains = (
        [
            {
                "direct_sector_level": "L2",
                "price_domain_reason_code": "hmm_risk_c010_price_domain_coverage_insufficient",
                "sector_code": invalid_sector,
                "trade_date": "2024-01-03",
            }
        ]
        if invalid_sector
        else []
    )
    child = {
        "schema_version": "hmm_risk_c008_b3_transition_dwell_b_single_pass_v1",
        "contract_version": "hmm_risk_c008_b3_transition_dwell_b_v1",
        "process_identity": process_identity,
        "feature_domain_policy_manifest": {
            "aggregate_receipt": {
                "l2_domain_receipts": domains,
                "l2_invalid_price_domain": invalid_domains,
                "missing_price_row_count": 0,
            },
        },
        "level_repeat": {
            "entries": entries,
            "entry_count": len(entries),
            "entry_payload_sha256": canonical_sha256(entries),
            "models": [{"large_payload_must_not_be_copied": [1, 2, 3]}],
        },
        "profiles": profiles,
        "provider_absence_partition_receipt": {},
        "selection_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
    }
    return child


def _write_canonical(path: Path, value: dict) -> None:
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _parent(tmp_path: Path, *, invalid_sector: str | None = None) -> Path:
    child_receipts = []
    for process_identity in ("fresh_process_1", "fresh_process_2"):
        child = _child(process_identity, invalid_sector=invalid_sector)
        child_path = tmp_path / f"{process_identity}.json"
        _write_canonical(child_path, child)
        child_receipts.append(
            {
                "process_identity": process_identity,
                "receipt_path": str(child_path),
                "receipt_sha256": canonical_sha256(child),
                "entry_payload_sha256": child["level_repeat"]["entry_payload_sha256"],
                "model_payload_sha256": "a" * 64,
                "profile_payload_sha256": canonical_sha256(child["profiles"]),
            }
        )
    body = {
        "schema_version": "hmm_risk_c008_b3_transition_dwell_b_diagnostic_v1",
        "contract_version": "hmm_risk_c008_b3_transition_dwell_b_v1",
        "status": "diagnostic_complete_no_complete_candidate",
        "planned_fit_count": 2096,
        "terminal_entry_count": 2096,
        "canonical_payload_bitwise_equal": True,
        "candidate_seed_count": 0,
        "diagnostic_complete_candidate_seeds": [],
        "fresh_process_receipts": child_receipts,
        "control_authority": {
            "family": "autocycle_all_core",
            "level": "L2",
            "schedule": list(EXPECTED_SEEDS),
        },
        "selection_performed": False,
        "d5_executed": False,
        "d6_executed": False,
        "semantic_mapping_performed": False,
        "formal_d5_stability_gate_applied": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    parent = {**body, "report_sha256": canonical_sha256(body)}
    parent_path = tmp_path / "parent.json"
    _write_canonical(parent_path, parent)
    return parent_path


def test_aggregate_transition_dwell_evidence_is_zero_refit_compact_and_fail_closed(tmp_path: Path) -> None:
    report = aggregate_transition_dwell_evidence(_parent(tmp_path))

    assert report["schema_version"] == AGGREGATION_SCHEMA_VERSION
    assert report["status"] == "diagnostic_complete"
    assert report["record_count"] == 1048
    assert report["dominant_failure_type_counts"] == {
        "accepted": 1039,
        "covariance": 1,
        "transition_run_dwell": 8,
    }
    assert report["persistent_cross_window_sector_seed_counts"] == [
        {"sector_code": "801001.SI", "failed_seed_count": 8}
    ]
    assert report["k3_structure_collapse_hypothesis"] == {
        "status": "diagnostic_hypothesis_only",
        "reason_code": K2_HYPOTHESIS,
        "sector_codes": ["801001.SI"],
        "k2_fit_performed": False,
        "k_selected": False,
    }
    assert report["coverage_bias"]["canonical_l2_denominator"] == 131
    assert report["coverage_bias"]["domain_date_count_per_sector"] == 1
    assert report["coverage_bias"]["domain_trade_date_start"] == "2024-01-02"
    assert report["coverage_bias"]["domain_trade_date_end"] == "2024-01-02"
    assert report["coverage_bias"]["domain_trade_date_sha256"] == canonical_sha256(["2024-01-02"])
    assert report["coverage_bias"]["valid_domain_receipt_count"] == 131
    assert report["coverage_bias"]["invalid_domain_receipt_count"] == 0
    assert report["coverage_bias"]["l1_parent_and_industry_mapping_status"] == "insufficient_evidence"
    assert len(report["coverage_bias"]["size_quintiles"]) == 5
    assert len(report["coverage_bias"]["liquidity_quintiles"]) == 5
    assert all(
        report[field] is False
        for field in (
            "selection_performed",
            "family_selection_performed",
            "refit_performed",
            "d5_executed",
            "d6_executed",
            "model_write_performed",
            "ready_artifact_write_performed",
            "database_write_performed",
            "runtime_action_performed",
            "formal_product_thresholds_applied",
        )
    )
    assert "models" not in report and "large_payload_must_not_be_copied" not in canonical_json_bytes(report).decode()


def test_aggregate_transition_dwell_evidence_rejects_child_hash_drift(tmp_path: Path) -> None:
    parent_path = _parent(tmp_path)
    child_path = tmp_path / "fresh_process_2.json"
    child_path.write_bytes(child_path.read_bytes() + b" ")

    with pytest.raises(B3EvidenceAggregationError, match="exactly one LF|canonical hash"):
        aggregate_transition_dwell_evidence(parent_path)


def test_aggregate_transition_dwell_evidence_closes_valid_and_invalid_domain_denominator(tmp_path: Path) -> None:
    report = aggregate_transition_dwell_evidence(_parent(tmp_path, invalid_sector="801001.SI"))

    coverage = report["coverage_bias"]
    assert coverage["domain_date_count_per_sector"] == 2
    assert coverage["valid_domain_receipt_count"] == 261
    assert coverage["invalid_domain_receipt_count"] == 1
    assert coverage["invalid_domain_reason_counts"] == {"hmm_risk_c010_price_domain_coverage_insufficient": 1}
    assert coverage["invalid_domain_sector_counts"] == [{"sector_code": "801001.SI", "invalid_date_count": 1}]


def test_aggregate_transition_dwell_evidence_rejects_incomplete_grid(tmp_path: Path) -> None:
    parent_path = _parent(tmp_path)
    parent = __import__("json").loads(parent_path.read_text(encoding="utf-8"))
    child_path = Path(parent["fresh_process_receipts"][0]["receipt_path"])
    child = __import__("json").loads(child_path.read_text(encoding="utf-8"))
    child["level_repeat"]["entries"].pop()
    child["level_repeat"]["entry_payload_sha256"] = canonical_sha256(child["level_repeat"]["entries"])
    _write_canonical(child_path, child)
    parent["fresh_process_receipts"][0]["receipt_sha256"] = canonical_sha256(child)
    parent["fresh_process_receipts"][0]["entry_payload_sha256"] = child["level_repeat"]["entry_payload_sha256"]
    body = {key: value for key, value in parent.items() if key != "report_sha256"}
    parent["report_sha256"] = canonical_sha256(body)
    _write_canonical(parent_path, parent)

    with pytest.raises(B3EvidenceAggregationError, match="entry/profile closure"):
        aggregate_transition_dwell_evidence(parent_path)


def test_aggregate_transition_dwell_evidence_rejects_duplicate_domain_key_hidden_by_count(tmp_path: Path) -> None:
    parent_path = _parent(tmp_path, invalid_sector="801001.SI")
    parent = __import__("json").loads(parent_path.read_text(encoding="utf-8"))
    child_path = Path(parent["fresh_process_receipts"][0]["receipt_path"])
    child = __import__("json").loads(child_path.read_text(encoding="utf-8"))
    child["feature_domain_policy_manifest"]["aggregate_receipt"]["l2_invalid_price_domain"][0]["trade_date"] = (
        "2024-01-02"
    )
    _write_canonical(child_path, child)
    parent["fresh_process_receipts"][0]["receipt_sha256"] = canonical_sha256(child)
    body = {key: value for key, value in parent.items() if key != "report_sha256"}
    parent["report_sha256"] = canonical_sha256(body)
    _write_canonical(parent_path, parent)

    with pytest.raises(B3EvidenceAggregationError, match="key is duplicated"):
        aggregate_transition_dwell_evidence(parent_path)


def test_aggregate_transition_dwell_evidence_rejects_missing_accepted_stage_evidence(tmp_path: Path) -> None:
    parent_path = _parent(tmp_path)
    parent = __import__("json").loads(parent_path.read_text(encoding="utf-8"))
    child_path = Path(parent["fresh_process_receipts"][0]["receipt_path"])
    child = __import__("json").loads(child_path.read_text(encoding="utf-8"))
    child["level_repeat"]["entries"][0].pop("covariance")
    child["level_repeat"]["entry_payload_sha256"] = canonical_sha256(child["level_repeat"]["entries"])
    _write_canonical(child_path, child)
    parent["fresh_process_receipts"][0]["receipt_sha256"] = canonical_sha256(child)
    parent["fresh_process_receipts"][0]["entry_payload_sha256"] = child["level_repeat"]["entry_payload_sha256"]
    body = {key: value for key, value in parent.items() if key != "report_sha256"}
    parent["report_sha256"] = canonical_sha256(body)
    _write_canonical(parent_path, parent)

    with pytest.raises(B3EvidenceAggregationError, match="covariance evidence is missing"):
        aggregate_transition_dwell_evidence(parent_path)


def test_write_aggregation_report_is_idempotent_and_rejects_collision(tmp_path: Path) -> None:
    report = aggregate_transition_dwell_evidence(_parent(tmp_path))
    output = tmp_path / "result.json"

    write_aggregation_report(output, report)
    write_aggregation_report(output, report)
    drift = copy.deepcopy(report)
    drift["status"] = "different"
    with pytest.raises(B3EvidenceAggregationError, match="collision"):
        write_aggregation_report(output, drift)


def test_write_aggregation_report_never_overwrites_a_concurrent_first_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = aggregate_transition_dwell_evidence(_parent(tmp_path))
    output = tmp_path / "race.json"
    competing = copy.deepcopy(report)
    competing["status"] = "competing-writer"
    competing_payload = canonical_json_bytes(competing) + b"\n"

    def _competing_link(_source: str, destination: Path) -> None:
        Path(destination).write_bytes(competing_payload)
        raise FileExistsError

    monkeypatch.setattr(os, "link", _competing_link)
    with pytest.raises(B3EvidenceAggregationError, match="collision"):
        write_aggregation_report(output, report)
    assert output.read_bytes() == competing_payload


def test_aggregation_failure_is_typed_durable_and_never_claims_side_effects(tmp_path: Path) -> None:
    failure = build_aggregation_failure(
        parent_path=tmp_path / "missing-parent.json",
        error=B3EvidenceAggregationError("source child is missing"),
    )

    assert failure["status"] == "insufficient_evidence"
    assert failure["primary_reason_code"] == "hmm_risk_p2_2_evidence_aggregation_failed"
    assert failure["error_type"] == "B3EvidenceAggregationError"
    assert failure["error"] == "source child is missing"
    assert all(
        failure[field] is False
        for field in (
            "selection_performed",
            "family_selection_performed",
            "refit_performed",
            "d5_executed",
            "d6_executed",
            "model_write_performed",
            "ready_artifact_write_performed",
            "database_write_performed",
            "runtime_action_performed",
            "formal_product_thresholds_applied",
        )
    )
    output = tmp_path / "failure.json"
    write_aggregation_report(output, failure)
    assert output.read_bytes() == canonical_json_bytes(failure) + b"\n"
