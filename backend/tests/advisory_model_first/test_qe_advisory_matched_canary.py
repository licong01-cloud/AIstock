from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.qe_advisory_matched_canary import (
    PROFILE_READY_MARKER,
    SOURCE_TASK_ID,
    build_qe_advisory_matched_canary_report,
    build_qe_exact_retry_preflight,
)


def _task_payload() -> dict:
    return {
        "data": {
            "task_id": SOURCE_TASK_ID,
            "task_type": "custom_evo",
            "status": "failed",
            "max_loops": 2,
            "current_loop": 0,
            "loops": [
                {
                    "loop_index": 1,
                    "loop_id": f"{SOURCE_TASK_ID}_loop_1",
                    "status": "failed",
                    "experiment_id": None,
                },
                {
                    "loop_index": 2,
                    "loop_id": f"{SOURCE_TASK_ID}_loop_2",
                    "status": "failed",
                    "experiment_id": None,
                },
            ],
        }
    }


def _loop_payload(loop_index: int, seed: int) -> dict:
    factor_names = [
        "m_intraday_range_60d_min_ratio",
        "m_turnover_acceleration",
        "m_sector_mf_divergence_lg",
    ]
    registration = {
        "schema_version": "qe_run_registration_v1",
        "task_id": SOURCE_TASK_ID,
        "loop_index": loop_index,
        "factor_digest": "07fd78abfede758204f6c084cc6d9aaf3c07a57eae82ec73f965b1854a301345",
        "dataset_release_id": "qe_hmm_full_v2_20260831",
        "dataset_generation": "20260907-v2",
        "dataset_cutoff": "2026-08-31",
        "universe_mode": "stock_universe",
    }
    return {
        "data": {
            "task_id": SOURCE_TASK_ID,
            "loop_id": f"{SOURCE_TASK_ID}_loop_{loop_index}",
            "loop_index": loop_index,
            "status": "failed",
            "config_json": {
                "model_id": "__seed_LSTM_10D_hs64_d02__",
                "factor_list": factor_names,
                "data_split": {
                    "train_start": "2022-03-23",
                    "train_end": "2025-05-08",
                    "valid_start": "2025-06-10",
                    "valid_end": "2025-12-02",
                    "test_start": "2026-01-05",
                    "test_end": "2026-06-30",
                    "backtest_end": "2026-06-29",
                },
                "label_horizon": 20,
                "strategy_id": "score_weighted_topk_v2",
                "execution_algo": "TWAP",
                "execution_algo_params": {},
                "bar_freq": "1m",
                "backtest_freq": "1min",
                "execution_manifest_sha256": f"{loop_index}" * 64,
                "model_params": {
                    "topk": 50,
                    "n_drop": 1,
                    "min_n_drop": 0,
                    "max_n_drop": 1,
                    "risk_policy": {
                        "enabled": True,
                        "providers": ["st_pit"],
                        "hard_actions": ["block_buy", "force_exit"],
                        "score_overlay": {
                            "enabled": False,
                            "positive_multiplier_cap": 1.1,
                            "negative_multiplier_floor": 0.7,
                        },
                        "policy_version": "stock_event_risk_policy_v1",
                        "st_universe_key": "shsz_st_pit_active_v1",
                        "strict_data_ready": True,
                        "visible_time_mode": "next_trading_session",
                    },
                },
                "_qe_run_registration": registration,
                "runtime_flags": {
                    "random_seed": seed,
                    "model_family": "LSTM",
                    "refit_mode": "rolling",
                    "vintage": "2026H1",
                    "rolling_train_days": 756,
                    "refit_cadence_trading_days": 120,
                    "observation_panel_id": "ma_e19_ce3_reference_v1",
                },
            },
        }
    }


def _observation(seed: int, *, candidate_bps: float, parent_bps: float) -> dict:
    hash_value = "a" * 64
    return {
        "random_seed": seed,
        "qe_experiment_id": f"qe_exact_retry_{seed}",
        "qe_status": "completed",
        "candidate_source_lineage_id": "q_canary_lstm_2026h1",
        "parent_package_id": "parent_package_v1",
        "dataset_release_id": "qe_hmm_full_v2_20260831",
        "dataset_cutoff": "2026-08-31",
        "universe_mode": "stock_universe",
        "factor_digest": "07fd78abfede758204f6c084cc6d9aaf3c07a57eae82ec73f965b1854a301345",
        "decision_date_start": "2026-01-05",
        "decision_date_end": "2026-06-30",
        "advisory_decision_day_count": 116,
        "advisory_intervention_day_count": 58,
        "advisory_intervention_day_coverage": 0.5,
        "policy": {
            "policy_sha256": hash_value,
            "cost_policy_sha256": "c" * 64,
            "pit_identity_sha256": "d" * 64,
            "target_count": 5,
            "rank_enter_threshold": 5,
            "rank_exit_threshold": 50,
            "daily_replacement_budget": 1,
            "entry_price_basis": "next_open_executable",
            "exit_price_basis": "next_open_executable",
            "dynamic_position_weights": False,
            "silent_backfill_allowed": False,
        },
        "qe_top50_net_return_bps": float(seed) / 10,
        "qe_top50_turnover": 0.2,
        "qe_top50_max_drawdown": 0.08,
        "advisory_candidate_net_excess_return_bps": candidate_bps,
        "advisory_parent_net_excess_return_bps": parent_bps,
        "advisory_candidate_coverage": 0.9,
        "advisory_parent_coverage": 0.91,
        "advisory_candidate_turnover": 0.12,
        "advisory_parent_turnover": 0.11,
        "advisory_candidate_max_drawdown": 0.07,
        "advisory_parent_max_drawdown": 0.08,
        "sealed_holdout_accessed": False,
    }


def test_preflight_is_read_only_and_profile_gated() -> None:
    loops = [_loop_payload(2, 2718), _loop_payload(1, 314)]

    blocked = build_qe_exact_retry_preflight(
        _task_payload(), loops, profile_status_marker="NOT_READY"
    )
    ready = build_qe_exact_retry_preflight(
        _task_payload(), loops, profile_status_marker=PROFILE_READY_MARKER
    )

    assert blocked.status == "BLOCKED_QE_PROFILE_NOT_READY"
    assert blocked.reason_code == "ADVISORY_QE_CANARY_PROFILE_NOT_READY"
    assert ready.status == "READY_FOR_NEW_QE_TASK_CREATION"
    assert ready.reason_code is None
    assert ready.source_task_action == "PRESERVE_FAILED_RECORD"
    assert ready.new_task_identity_required is True
    assert ready.qe_task_submission_performed is False
    assert ready.source_task_mutated is False
    assert tuple(item.random_seed for item in ready.loops) == (314, 2718)
    assert {item.shared_contract_sha256 for item in ready.loops} == {
        ready.shared_contract_sha256
    }


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("config_json", "model_id"), "different_model"),
        (("config_json", "factor_list"), ["different_factor"]),
        (("config_json", "model_params", "topk"), 49),
        (
            ("config_json", "model_params", "risk_policy"),
            {"policy_id": "different"},
        ),
        (("config_json", "data_split", "test_end"), "2026-06-29"),
        (("config_json", "execution_algo"), "VWAP"),
    ],
)
def test_preflight_rejects_exact_retry_semantic_drift(path: tuple[str, ...], value) -> None:
    loop_one = _loop_payload(1, 314)
    loop_two = _loop_payload(2, 2718)
    target = loop_two["data"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_qe_exact_retry_preflight(
            _task_payload(),
            [loop_one, loop_two],
            profile_status_marker=PROFILE_READY_MARKER,
        )

    assert caught.value.reason_code in {
        "ADVISORY_QE_CANARY_CONTRACT_DRIFT",
        "ADVISORY_QE_CANARY_PUBLIC_PAYLOAD_INVALID",
    }


def test_preflight_rejects_same_drift_in_both_loops() -> None:
    loops = [_loop_payload(1, 314), _loop_payload(2, 2718)]
    for loop in loops:
        loop["data"]["config_json"]["model_id"] = "same_but_not_frozen_model"

    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_qe_exact_retry_preflight(
            _task_payload(), loops, profile_status_marker=PROFILE_READY_MARKER
        )

    assert caught.value.reason_code == "ADVISORY_QE_CANARY_CONTRACT_DRIFT"


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "experiment", "status"])
def test_preflight_rejects_invalid_source_identity(mutation: str) -> None:
    task = _task_payload()
    loops = [_loop_payload(1, 314), _loop_payload(2, 2718)]
    if mutation == "missing":
        loops.pop()
    elif mutation == "duplicate":
        loops[1]["data"]["loop_index"] = 1
    elif mutation == "experiment":
        task["data"]["loops"][0]["experiment_id"] = "existing"
    else:
        task["data"]["status"] = "completed"

    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_qe_exact_retry_preflight(
            task, loops, profile_status_marker=PROFILE_READY_MARKER
        )

    assert caught.value.reason_code == "ADVISORY_QE_CANARY_SOURCE_TASK_INVALID"


def test_matched_report_keeps_qe_and_advisory_roles_separate() -> None:
    report = build_qe_advisory_matched_canary_report(
        [
            _observation(2718, candidate_bps=9, parent_bps=5),
            _observation(123, candidate_bps=7, parent_bps=5),
            _observation(314, candidate_bps=4, parent_bps=5),
        ]
    )

    assert report.seed_roster == (123, 314, 2718)
    assert report.advisory_delta_bps_by_seed == {123: 2.0, 314: -1.0, 2718: 4.0}
    assert report.advisory_delta_mean_bps == pytest.approx(5 / 3)
    assert report.advisory_delta_std_bps == pytest.approx(2.0548046677)
    assert report.positive_delta_seed_count == 2
    assert report.all_seeds_positive is False
    assert report.qe_top50_net_return_mean_bps == pytest.approx((12.3 + 31.4 + 271.8) / 3)
    assert report.qe_top50_turnover_mean == pytest.approx(0.2)
    assert report.qe_top50_max_drawdown_max == pytest.approx(0.08)
    assert report.advisory_intervention_day_count_min == 58
    assert report.advisory_intervention_day_coverage_min == pytest.approx(0.5)
    assert report.activation_authorized is False
    assert report.strategy_package_mutated is False
    assert report.qe_task_submission_performed is False
    assert report.decision_use == "NAVIGATION_ONLY"


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("missing_seed", "ADVISORY_QE_CANARY_OBSERVATION_INVALID"),
        ("duplicate_seed", "ADVISORY_QE_CANARY_OBSERVATION_INVALID"),
        ("policy", "ADVISORY_QE_CANARY_POLICY_MISMATCH"),
        ("release", "ADVISORY_QE_CANARY_CONTRACT_DRIFT"),
        ("sealed", "ADVISORY_QE_CANARY_SEALED_ACCESS_FORBIDDEN"),
        ("support", "ADVISORY_QE_CANARY_OBSERVATION_INVALID"),
    ],
)
def test_matched_report_fails_closed_on_identity_or_evidence_drift(
    mutation: str, expected_reason: str
) -> None:
    rows = [
        _observation(123, candidate_bps=7, parent_bps=5),
        _observation(314, candidate_bps=4, parent_bps=5),
        _observation(2718, candidate_bps=9, parent_bps=5),
    ]
    if mutation == "missing_seed":
        rows.pop()
    elif mutation == "duplicate_seed":
        rows[2]["random_seed"] = 314
    elif mutation == "policy":
        rows[2]["policy"]["policy_sha256"] = "e" * 64
    elif mutation == "release":
        rows[2]["dataset_release_id"] = "different"
    elif mutation == "sealed":
        rows[2]["sealed_holdout_accessed"] = True
    else:
        rows[2]["advisory_intervention_day_coverage"] = 0.4

    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_qe_advisory_matched_canary_report(rows)

    assert caught.value.reason_code == expected_reason


def test_matched_report_rejects_same_source_drift_in_all_seeds() -> None:
    rows = [
        _observation(123, candidate_bps=7, parent_bps=5),
        _observation(314, candidate_bps=4, parent_bps=5),
        _observation(2718, candidate_bps=9, parent_bps=5),
    ]
    for row in rows:
        row["dataset_release_id"] = "same_but_not_frozen_release"

    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_qe_advisory_matched_canary_report(rows)

    assert caught.value.reason_code == "ADVISORY_QE_CANARY_CONTRACT_DRIFT"


def test_module_has_no_qe_runtime_or_external_io_dependency() -> None:
    path = (
        Path(__file__).resolve().parents[2]
        / "services"
        / "advisory_model_first"
        / "qe_advisory_matched_canary.py"
    )
    source = path.read_text(encoding="utf-8").lower()

    forbidden = (
        "backend.services.quantevolver",
        "backend.mcp.modules.qe_experiment",
        "import requests",
        "import httpx",
        "import psycopg",
        "import sqlalchemy",
        "import subprocess",
        "create_qe_task",
        "submit_qe",
    )
    assert all(token not in source for token in forbidden)
