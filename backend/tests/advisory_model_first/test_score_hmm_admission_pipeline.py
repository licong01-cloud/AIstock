from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import score_hmm_admission_pipeline as pipeline
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    SCORE_HMM_ARM_IDS,
    SCORE_HMM_ARM_SCHEMA_HASHES,
    SCORE_HMM_EVIDENCE_ROLES,
    FrozenAdvisoryScoreHMMAdmissionRequestV1,
    build_score_hmm_request,
)
from backend.services.advisory_model_first.score_hmm_admission_pipeline import (
    _assert_parent_ranking_parity,
    _evaluate_one_arm,
    _fit_crossfit_head,
    _target_key_overlap,
    build_calibration_metrics,
    build_score_hmm_targets,
    compute_pre_run_mde,
    validate_score_hmm_label_interval_isolation,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def build_test_request(**overrides: object) -> FrozenAdvisoryScoreHMMAdmissionRequestV1:
    refs = tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=f"/tmp/score-hmm-evidence-{index}",
            sha256=f"{index + 1:064x}",
            size_bytes=index + 1,
        )
        for index, role in enumerate(SCORE_HMM_EVIDENCE_ROLES)
    )
    calendar = tuple(value.date() for value in pd.bdate_range("2023-01-02", "2026-03-10"))
    calendar_sha = canonical_json_sha256({"market_sessions": [value.isoformat() for value in calendar]})
    n1_bundle_id = "1" * 64
    policy_bundle_id = "2" * 64
    pit_spans_sha = "3" * 64
    market_calendar_sha = "4" * 64
    suspend_sha = "5" * 64
    factor_schema = "6" * 64
    policy_identity = "7" * 64
    warmup_pit_spans_sha = "8" * 64
    dataset_identity = canonical_json_sha256(
        {
            "n1_bundle_id": n1_bundle_id,
            "policy_bundle_id": policy_bundle_id,
            "pit_spans_sha256": pit_spans_sha,
            "market_warmup_pit_spans_sha256": warmup_pit_spans_sha,
            "market_calendar_sha256": market_calendar_sha,
            "suspend_sidecar_sha256": suspend_sha,
            "factor_schema_identity": factor_schema,
            "arm_schema_hashes": SCORE_HMM_ARM_SCHEMA_HASHES,
            "policy_identity": policy_identity,
            "evidence_refs": [item.model_dump(mode="json") for item in refs],
        }
    )
    values: dict[str, object] = {
        "created_at": datetime(2026, 9, 5, tzinfo=timezone.utc),
        "program_id": "advp_test",
        "binding_version_id": "advb_test",
        "package_id": "pkg_test",
        "manifest_sha256": "8" * 64,
        "style_profile_id": "style_test",
        "style_profile_hash": "9" * 64,
        "package_asset_closure_hash": "a" * 64,
        "selection_runtime_semantics_id": "semantics_test",
        "selection_runtime_semantics_hash": "b" * 64,
        "terminal_weights": {LSTM_LEG_ID: 0.7, FUND_LEG_ID: 0.3},
        "representative_model_asset_sha256": {LSTM_LEG_ID: "c" * 64, FUND_LEG_ID: "d" * 64},
        "n1_bundle_path": f"/tmp/{n1_bundle_id}",
        "n1_bundle_id": n1_bundle_id,
        "n1_request_sha256": "e" * 64,
        "n1_manifest_file_sha256": "f" * 64,
        "n1_rankings_sha256": "0" * 64,
        "n1_cpcv_sha256": "1" * 64,
        "n1_regime_daily_sha256": "2" * 64,
        "policy_bundle_path": f"/tmp/{policy_bundle_id}",
        "policy_bundle_id": policy_bundle_id,
        "policy_request_sha256": "3" * 64,
        "policy_manifest_file_sha256": "4" * 64,
        "policy_labels_sha256": "5" * 64,
        "policy_rankings_sha256": "6" * 64,
        "baseline_policy_sha256": "7" * 64,
        "shadow_policy_sha256": "8" * 64,
        "cost_policy_sha256": "9" * 64,
        "split_policy_sha256": "a" * 64,
        "policy_identity": policy_identity,
        "pit_snapshot_path": "/tmp/pit.json",
        "pit_snapshot_file_sha256": "b" * 64,
        "pit_spans_sha256": pit_spans_sha,
        "market_warmup_pit_snapshot_path": "/tmp/market-warmup-pit.json",
        "market_warmup_pit_snapshot_file_sha256": "c" * 64,
        "market_warmup_pit_spans_sha256": warmup_pit_spans_sha,
        "qlib_daily_root": "/tmp/qlib",
        "factor_data_root": "/tmp/factors",
        "suspend_data_root": "/tmp/suspend",
        "market_calendar_sha256": market_calendar_sha,
        "market_calendar_row_count": len(calendar),
        "suspend_sidecar_sha256": suspend_sha,
        "suspend_sidecar_row_count": 1,
        "factor_schema_identity": factor_schema,
        "trading_calendar": calendar,
        "trading_calendar_sha256": calendar_sha,
        "pre_run_effective_sample_size": 128.0,
        "pre_run_mde_bps": 12.5,
        "pre_run_power_sufficient_for_5bps": False,
        "registry_path": "/tmp/advisory_research_trial_registry_v1.jsonl",
        "registry_sha256_at_request": "c" * 64,
        "registry_record_count_at_request": 31,
        "cumulative_evaluated_trial_count_prior": 1_281,
        "current_route_path": "/tmp/current_route.md",
        "current_route_sha256": "d" * 64,
        "cumulative_candidate_index_prior": 83,
        "reserved_candidate_indices": (84, 85, 86, 87, 88),
        "auxiliary_route_path": "/tmp/current_auxiliary_route.md",
        "evidence_refs": refs,
        "dataset_identity": dataset_identity,
        "repository_root": "/tmp/repo",
        "repository_commit": "e" * 40,
        "output_root": "/tmp/output",
    }
    values.update(overrides)
    return build_score_hmm_request(**values)


def _small_score_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-02")],
            "target_trade_date": [pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-03")],
            "instrument": ["000001.SZ", "000002.SZ"],
            "selection_effective_rank": [1, 2],
        }
    )


def test_primary_and_secondary_target_clocks_are_bound_without_dropping_censored_rows() -> None:
    score = _small_score_panel()
    primary = score[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    primary["net_return_bps"] = [12.0, np.nan]
    primary["net_excess_return_bps"] = [10.0, np.nan]
    primary["label_status"] = ["MATURED", "RIGHT_CENSORED_AT_CUTOFF"]
    secondary = score[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    for horizon in (1, 5, 10, 20):
        secondary[f"stock_net_return_{horizon}"] = [0.01 * horizon, np.nan]
        secondary[f"excess_return_{horizon}"] = [0.008 * horizon, np.nan]
        secondary[f"label_status_{horizon}"] = ["MATURE_EXECUTABLE", "RIGHT_CENSORED"]

    result = build_score_hmm_targets(
        score_features=score,
        primary_labels=primary,
        secondary_labels=secondary,
    )

    assert len(result.panel) == 2
    assert result.panel["primary_target_known"].tolist() == [True, False]
    assert result.panel["h20_target_known"].tolist() == [True, False]
    assert result.panel.loc[0, "h20_net_return_bps"] == pytest.approx(2_000.0)
    assert set(result.coverage["target_head"]) == {"PRIMARY", "H1", "H5", "H10", "H20"}


def test_target_builder_fails_on_key_drift() -> None:
    score = _small_score_panel()
    primary = pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2025-01-02")],
            "target_trade_date": [pd.Timestamp("2025-01-03")],
            "instrument": ["000001.SZ"],
            "net_return_bps": [1.0],
            "net_excess_return_bps": [1.0],
            "label_status": ["MATURED"],
        }
    )
    secondary = primary[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    for horizon in (1, 5, 10, 20):
        secondary[f"stock_net_return_{horizon}"] = 0.01
        secondary[f"excess_return_{horizon}"] = 0.01
        secondary[f"label_status_{horizon}"] = "MATURE_EXECUTABLE"
    with pytest.raises(AdvisoryModelFirstError, match="exact Top20 keys"):
        build_score_hmm_targets(score_features=score, primary_labels=primary, secondary_labels=secondary)


def test_target_builder_rejects_target_trade_date_drift_without_mutating_inputs() -> None:
    score = _small_score_panel()
    primary = score[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    primary["net_return_bps"] = [1.0, 2.0]
    primary["net_excess_return_bps"] = [1.0, 2.0]
    primary["label_status"] = "MATURED"
    secondary = score[["decision_as_of_trade_date", "target_trade_date", "instrument"]].copy()
    for horizon in (1, 5, 10, 20):
        secondary[f"stock_net_return_{horizon}"] = 0.01
        secondary[f"excess_return_{horizon}"] = 0.01
        secondary[f"label_status_{horizon}"] = "MATURE_EXECUTABLE"
    original_primary = primary.copy(deep=True)
    secondary.loc[1, "target_trade_date"] = pd.Timestamp("2025-01-06")

    with pytest.raises(AdvisoryModelFirstError) as caught:
        build_score_hmm_targets(score_features=score, primary_labels=primary, secondary_labels=secondary)

    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_TARGET_INVALID"
    pd.testing.assert_frame_equal(primary, original_primary)


def test_fixed_inner_oof_head_accepts_tuple_feature_contract_and_uses_six_groups() -> None:
    train_dates = pd.bdate_range("2024-01-02", periods=6)
    validation_date = pd.Timestamp("2024-01-10")
    dates = np.repeat(train_dates.append(pd.DatetimeIndex([validation_date])), 20)
    index = np.arange(len(dates), dtype=float)
    frame = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "feature_a": np.sin(index / 7.0),
            "feature_b": np.cos(index / 11.0),
            "target": np.where((index.astype(int) % 2) == 0, 10.0 + index / 100.0, -10.0 - index / 100.0),
            "known": True,
        }
    )
    train_mask = frame["decision_as_of_trade_date"].isin(train_dates)
    validation_mask = frame["decision_as_of_trade_date"].eq(validation_date)
    block_by_date = {day: number for number, day in enumerate(train_dates)}

    predictions, receipt = _fit_crossfit_head(
        frame=frame,
        train_mask=train_mask,
        validation_mask=validation_mask,
        feature_columns=("feature_a", "feature_b"),
        value_column="target",
        known_column="known",
        block_by_date=block_by_date,
        request=build_test_request(),
        arm_id="PACKAGE_SCORE_CALIBRATION_ONLY",
        path_id="path-00",
        head="PRIMARY",
    )

    assert receipt["inner_oof_group_count"] == 6
    assert receipt["inner_oof_row_count"] == 120
    assert len(predictions["positive_probability"]) == 20
    assert np.isfinite(predictions["expected_net_return_lcb80_bps"]).all()


def test_pre_run_mde_reports_effective_sample_size_and_fixed_power_threshold() -> None:
    baseline = pd.DataFrame({"net_return_bps": np.sin(np.arange(386) / 8.0) * 20.0})
    result = compute_pre_run_mde(baseline)
    assert 1.0 <= result["effective_sample_size"] <= 386.0
    assert result["minimum_economic_effect_bps"] == 5.0
    assert result["power_sufficient_for_5bps"] == (result["mde_bps"] <= 5.0)


def _ranking_projection() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "decision_as_of_trade_date": [pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-02")],
            "target_trade_date": [pd.Timestamp("2025-01-03"), pd.Timestamp("2025-01-03")],
            "instrument": ["000001.SZ", "000002.SZ"],
            "selection_effective_rank": [1, 2],
            "combined_score": [2.0, 1.0],
            f"raw__{LSTM_LEG_ID}": [3.0, 2.0],
            f"norm__{LSTM_LEG_ID}": [1.0, 0.0],
            f"rank__{LSTM_LEG_ID}": [1, 2],
            f"weight__{LSTM_LEG_ID}": [0.7, 0.7],
            f"raw__{FUND_LEG_ID}": [2.0, 1.0],
            f"norm__{FUND_LEG_ID}": [1.0, 0.0],
            f"rank__{FUND_LEG_ID}": [1, 2],
            f"weight__{FUND_LEG_ID}": [0.3, 0.3],
        }
    )


def test_extended_pit_rank_context_must_reproduce_frozen_n1_candidate_projection() -> None:
    expected = _ranking_projection()
    _assert_parent_ranking_parity(expected=expected, actual=expected.sample(frac=1.0, random_state=7))
    drifted = expected.copy()
    drifted.loc[0, "combined_score"] += 0.01
    with pytest.raises(AdvisoryModelFirstError) as caught:
        _assert_parent_ranking_parity(expected=expected, actual=drifted)
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH"


def test_legacy_policy_labels_are_diagnostic_only_when_rank_semantics_differ() -> None:
    rankings = _ranking_projection()
    labels = rankings[["decision_as_of_trade_date", "target_trade_date", "instrument"]].iloc[[0]].copy()
    labels.loc[len(labels)] = [pd.Timestamp("2025-01-02"), pd.Timestamp("2025-01-03"), "000003.SZ"]
    overlap = _target_key_overlap(rankings, labels, depth=20)
    assert overlap["matched_key_count"] == 1
    assert overlap["aligned_only_key_count"] == 1
    assert overlap["legacy_only_key_count"] == 1
    assert overlap["legacy_labels_used_as_target"] is False


def test_calibration_keeps_completely_unavailable_arms_typed_instead_of_failing() -> None:
    predictions = pd.DataFrame(
        {
            "arm_id": list(SCORE_HMM_ARM_IDS[:3]),
            "prediction_status": ["SOURCE_UNAVAILABLE"] * 3,
        }
    )

    metrics = build_calibration_metrics(predictions)

    assert len(metrics) == 15
    assert set(metrics["status"]) == {"SOURCE_UNAVAILABLE"}
    assert set(metrics["reason_code"]) == {"ARM_HAS_NO_AVAILABLE_OOF_PREDICTIONS"}


def test_economic_intervals_use_the_frozen_familywise_alpha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = pd.bdate_range("2024-01-02", periods=80)
    observed_alpha: list[float] = []

    def _interval(values, *, block_length: int, repetitions: int, seed: int, alpha: float):
        del values, block_length, repetitions, seed
        observed_alpha.append(alpha)
        return 1.0, 2.0

    monkeypatch.setattr(pipeline, "_moving_block_interval", _interval)
    monkeypatch.setattr(pipeline, "_deflated_sharpe_diagnostic", lambda *_args, **_kwargs: {})
    arm_daily = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "net_return_bps": np.linspace(-2.0, 5.0, len(dates)),
            "net_excess_return_bps": np.linspace(-1.0, 4.0, len(dates)),
            "drawdown": np.linspace(0.0, -0.05, len(dates)),
            "cash_slot_count": 1,
            "turnover_fraction": 0.2,
        }
    )
    baseline_daily = arm_daily.assign(
        net_return_bps=arm_daily["net_return_bps"] - 1.0,
        net_excess_return_bps=arm_daily["net_excess_return_bps"] - 1.0,
        drawdown=np.linspace(0.0, -0.06, len(dates)),
    )
    decisions = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "action": np.where(np.arange(len(dates)) % 2 == 0, "TAKE", "SKIP"),
        }
    )
    episodes = pd.DataFrame(
        {
            "status": ["EXITED", "EXITED"],
            "net_return_bps": [10.0, 20.0],
            "entry_trade_date": dates[:2],
            "instrument": ["000001.SZ", "000002.SZ"],
        }
    )
    regime = pd.DataFrame(
        {
            "decision_as_of_trade_date": dates,
            "regime": np.where(np.arange(len(dates)) % 2 == 0, "UP_OR_FLAT", "DOWN"),
        }
    )
    calibration = pd.DataFrame(
        {
            "arm_id": [SCORE_HMM_ARM_IDS[0]],
            "target_head": ["PRIMARY"],
            "status": ["EVALUATED"],
            "brier": [0.20],
            "base_rate_brier": [0.25],
            "probability_std": [0.1],
        }
    )
    request = build_test_request()

    _evaluate_one_arm(
        arm_id=SCORE_HMM_ARM_IDS[0],
        decisions=decisions,
        arm_daily=arm_daily,
        arm_episodes=episodes,
        baseline_daily=baseline_daily,
        baseline_episodes=episodes,
        regime_daily=regime,
        calibration_metrics=calibration,
        request=request,
    )

    assert observed_alpha == [request.familywise_alpha, request.familywise_alpha]


def test_reused_cpcv_paths_must_isolate_each_new_label_information_interval() -> None:
    decisions = pd.to_datetime(["2024-01-02", "2024-02-01"])
    targets = pd.to_datetime(["2024-01-03", "2024-02-02"])
    primary = pd.DataFrame(
        {
            "decision_as_of_trade_date": decisions,
            "target_trade_date": targets,
            "label_status": ["MATURED", "MATURED"],
            "label_information_end": pd.to_datetime(["2024-01-10", "2024-02-20"]),
        }
    )
    secondary = pd.DataFrame(
        {
            "decision_as_of_trade_date": decisions,
            "target_trade_date": targets,
            **{f"label_status_{horizon}": ["MATURE_EXECUTABLE"] * 2 for horizon in (1, 5, 10, 20)},
            **{
                f"actual_exit_date_{horizon}": pd.to_datetime(["2024-01-10", "2024-02-20"])
                for horizon in (1, 5, 10, 20)
            },
        }
    )
    cpcv = {
        "paths": [
            {
                "path_id": "path-00",
                "status": "READY",
                "train_dates": ["2024-01-02"],
                "validation_dates": ["2024-02-01"],
            }
        ]
    }

    receipt = validate_score_hmm_label_interval_isolation(
        primary_labels=primary,
        secondary_labels=secondary,
        cpcv_payload=cpcv,
    )
    assert receipt["status"] == "PASS_NO_INFORMATION_INTERVAL_OVERLAP"
    assert receipt["check_count"] == 5

    leaking = primary.copy()
    leaking.loc[0, "label_information_end"] = pd.Timestamp("2024-02-05")
    with pytest.raises(AdvisoryModelFirstError) as caught:
        validate_score_hmm_label_interval_isolation(
            primary_labels=leaking,
            secondary_labels=secondary,
            cpcv_payload=cpcv,
        )
    assert caught.value.reason_code == "ADVISORY_SCORE_HMM_CROSSFIT_INVALID"
