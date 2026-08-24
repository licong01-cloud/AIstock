from __future__ import annotations

import hashlib

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_utility_pipeline import (
    _verify_training_cutoffs,
    _calendar_identity_sha256,
    _verify_bound_data_identities,
    _verify_feature_v2_coverage,
    _verify_policy_source_coverage,
    build_policy_utility_advancement_receipt,
    compare_policy_arm_rows,
    compare_policy_utility_reference,
    run_policy_utility_pipeline,
)
from backend.tests.advisory_model_first.test_policy_utility_contracts import _request


def _candidate(primary_shift=1.0, drawdown_shift=0.01, turnover_shift=-0.01):
    return pd.DataFrame(
        {
            "path_id": [f"path_{index:02d}" for index in range(28)],
            "policy_mean_daily_net_excess_return_bps": [11.0 + primary_shift] * 28,
            "policy_maximum_drawdown": [-0.10 + drawdown_shift] * 28,
            "policy_mean_turnover_fraction": [0.20 + turnover_shift] * 28,
        }
    )


def _reference():
    trial = pd.DataFrame(
        {
            "family_id": ["FAMILY_CORE_HMM"] * 28,
            "seed": [20260817] * 28,
            "path_id": [f"path_{index:02d}" for index in range(28)],
            "policy_mean_daily_net_excess_return_bps": [11.0] * 28,
            "policy_maximum_drawdown": [-0.10] * 28,
            "policy_mean_turnover_fraction": [0.20] * 28,
        }
    )
    return {
        "manifest": {"bundle_id": "a" * 64},
        "winner": {"family_id": "FAMILY_CORE_HMM", "seed": 20260817},
        "trial_metrics": trial,
    }


def _parity_reference_rows():
    return _candidate(primary_shift=0.0, drawdown_shift=0.0, turnover_shift=0.0)


def test_policy_utility_advancement_requires_all_preregistered_checks() -> None:
    comparison = compare_policy_arm_rows(
        candidate_rows=_candidate(),
        reference_rows=_parity_reference_rows(),
        reference_role="ARM_P0D_V2_BINARY_PARITY",
    )
    receipt = build_policy_utility_advancement_receipt(
        p0d_comparison=comparison,
        candidate_minus_selection_mean_primary_metric_bps=0.1,
        candidate_path_ids=[f"path_{index:02d}" for index in range(28)],
    )
    assert receipt["experiment_status"] == "ADVANCED_TO_STAGE_B"
    assert all(receipt["checks"].values())


def test_policy_utility_negative_stop_and_incomplete_cpcv_are_explicit() -> None:
    comparison = compare_policy_arm_rows(
        candidate_rows=_candidate(primary_shift=-1.0, drawdown_shift=-0.01, turnover_shift=0.01),
        reference_rows=_parity_reference_rows(),
        reference_role="ARM_P0D_V2_BINARY_PARITY",
    )
    paths = [f"path_{index:02d}" for index in range(28)]
    negative = build_policy_utility_advancement_receipt(
        p0d_comparison=comparison,
        candidate_minus_selection_mean_primary_metric_bps=-0.1,
        candidate_path_ids=paths,
    )
    incomplete = build_policy_utility_advancement_receipt(
        p0d_comparison=comparison,
        candidate_minus_selection_mean_primary_metric_bps=1.0,
        candidate_path_ids=paths[:-1],
    )
    assert negative["experiment_status"] == "NEGATIVE_STOP_NOT_ADVANCED"
    assert incomplete["experiment_status"] == "NEGATIVE_STOP_INCOMPLETE_CPCV"
    assert not negative["advanced_to_stage_b"]
    assert negative["pbo_is_gate"] is False


def test_policy_utility_ties_do_not_pass_strict_advancement_thresholds() -> None:
    comparison = compare_policy_arm_rows(
        candidate_rows=_candidate(primary_shift=0.0, drawdown_shift=0.0, turnover_shift=0.0),
        reference_rows=_parity_reference_rows(),
        reference_role="ARM_P0D_V2_BINARY_PARITY",
    )
    receipt = build_policy_utility_advancement_receipt(
        p0d_comparison=comparison,
        candidate_minus_selection_mean_primary_metric_bps=0.0,
        candidate_path_ids=[f"path_{index:02d}" for index in range(28)],
    )
    assert receipt["experiment_status"] == "NEGATIVE_STOP_NOT_ADVANCED"
    assert receipt["checks"]["candidate_minus_p0d_mean_primary_gt_zero"] is False
    assert receipt["checks"]["candidate_vs_p0d_path_win_rate_gt_half"] is False
    assert receipt["checks"]["paired_mean_maximum_drawdown_difference_gte_zero"] is True
    assert receipt["checks"]["paired_mean_turnover_fraction_difference_lte_zero"] is True


def test_policy_utility_advancement_rejects_legacy_reference() -> None:
    legacy = compare_policy_utility_reference(
        winner_rows=_candidate(), reference=_reference(), reference_role="LEGACY_P0_D_LINEAGE"
    )
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        build_policy_utility_advancement_receipt(
            p0d_comparison=legacy,
            candidate_minus_selection_mean_primary_metric_bps=1.0,
            candidate_path_ids=[f"path_{index:02d}" for index in range(28)],
        )
    assert exc_info.value.reason_code == "ADVISORY_POLICY_UTILITY_REFERENCE_NOT_PARITY"


def test_policy_utility_future_label_poison_fails_frozen_cutoff_readback() -> None:
    request = _request()
    labels = pd.DataFrame(
        {
            "label_status": ["MATURED"],
            "decision_as_of_trade_date": ["2026-02-02"],
            "label_information_end": ["2026-03-10"],
        }
    )
    _verify_training_cutoffs(request, labels)
    poisoned = pd.concat(
        [
            labels,
            pd.DataFrame(
                {
                    "label_status": ["MATURED"],
                    "decision_as_of_trade_date": ["2026-02-03"],
                    "label_information_end": ["2026-03-11"],
                }
            ),
        ],
        ignore_index=True,
    )
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        _verify_training_cutoffs(request, poisoned)
    assert excinfo.value.reason_code == "ADVISORY_POLICY_UTILITY_SOURCE_INVALID"


def test_policy_utility_pipeline_rejects_invalid_request_with_typed_error(tmp_path) -> None:
    request_path = tmp_path / "request.json"
    request_path.write_text("{}", encoding="utf-8")
    with pytest.raises(AdvisoryModelFirstError) as excinfo:
        run_policy_utility_pipeline(request_path)
    assert excinfo.value.reason_code == "ADVISORY_POLICY_UTILITY_REQUEST_INVALID"


def test_feature_v2_coverage_requires_exact_7720_identity_rows() -> None:
    dates = pd.bdate_range("2024-01-02", periods=386)
    rows = [
        {
            "decision_as_of_trade_date": date,
            "instrument": f"S{rank:02d}",
            "is_candidate_decision": True,
            "selection_effective_rank": rank,
        }
        for date in dates
        for rank in range(1, 21)
    ]
    rankings = pd.DataFrame(rows)
    features = rankings[["decision_as_of_trade_date", "instrument"]].copy()
    _verify_feature_v2_coverage(_request(), features, rankings)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        _verify_feature_v2_coverage(_request(), features.iloc[:-1], rankings)
    assert exc_info.value.reason_code == "ADVISORY_FEATURE_V2_COVERAGE_INVALID"


def test_bound_calendar_and_suspend_identity_are_read_back(tmp_path) -> None:
    calendar = pd.bdate_range("2026-06-26", periods=3)
    suspend_root = tmp_path / "suspend"
    suspend_root.mkdir()
    suspend_path = suspend_root / "suspend_d.parquet"
    pd.DataFrame(
        {
            "trade_date": [calendar[0]],
            "ts_code": ["000001.SZ"],
            "suspend_type": ["S"],
        }
    ).to_parquet(suspend_path, index=False)
    suspend_sha = hashlib.sha256(suspend_path.read_bytes()).hexdigest()
    request = _request(
        suspend_data_root=str(suspend_root),
        market_calendar_identity={
            "identity_kind": "MARKET_CALENDAR",
            "sha256": _calendar_identity_sha256(calendar),
            "cutoff_trade_date": "2026-06-30",
            "row_count": len(calendar),
        },
        suspend_sidecar_identity={
            "identity_kind": "SUSPEND_SIDECAR",
            "sha256": suspend_sha,
            "cutoff_trade_date": "2026-06-30",
            "row_count": 1,
        },
    )
    _verify_bound_data_identities(request, calendar)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        _verify_bound_data_identities(request, calendar[:-1])
    assert exc_info.value.reason_code == "ADVISORY_POLICY_UTILITY_DATASET_INVALID"


def test_policy_source_requires_one_to_one_ranking_and_label_coverage() -> None:
    dates = pd.bdate_range("2024-01-02", periods=386)
    rankings = pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": date,
                "target_trade_date": date + pd.offsets.BDay(),
                "instrument": f"S{rank:02d}",
                "is_candidate_decision": True,
                "selection_effective_rank": rank,
            }
            for date in dates
            for rank in range(1, 21)
        ]
    )
    labels = rankings[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    labels["label_status"] = "MATURED"
    _verify_policy_source_coverage(_request(), rankings, labels)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        _verify_policy_source_coverage(_request(), rankings, labels.iloc[:-1])
    assert exc_info.value.reason_code == "ADVISORY_POLICY_UTILITY_DATASET_INVALID"
