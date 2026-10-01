from __future__ import annotations

import copy
import json

import pytest

from backend.services.hmm_risk import qe_assistance_three_arm as subject
from backend.services.hmm_risk.qe_assistance_transport import BINDING_PARAM
from backend.services.hmm_risk.qe_assistance_transport import BINDING_FILE
from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
from backend.routers.quantevolver_evolution import CustomEvolutionCreateRequest


def _dataset_identity() -> dict:
    return {
        "schema_version": subject.ACTIVE_DATASET_IDENTITY_SCHEMA_VERSION,
        "generation": "20260917-v9",
        "release_id": "qe_hmm_full_v2_20260831",
        "cutoff": "2026-08-31",
        "profile_sha256": "c" * 64,
        "candidate_roots": [
            "/home/lc999/data/dataset_candidates/20260831-qe_hmm_full_v2-direct-20260917-r6-candidate",
            "X:\\AIstock_dataset_candidates\\backtest_dataset_candidates\\20260831-qe_hmm_full_v2-direct-20260917-r6-candidate",
        ],
    }


def _source(*, hmm: bool) -> dict:
    model_params = {
        "disable_alpha158": True,
        "label_horizon": 10,
        "stock_pool": "filtered_pool_20260502",
        "unfilled_handler": "TAIL_SUBSTITUTE",
        "unfilled_backup_depth": 3,
    }
    if hmm:
        model_params.update(
            {
                "enable_sector_hmm": True,
                "hmm_model_version_id": "legacy-snapshot-id",
                "hmm_signal_preset": "preset_A",
                "sector_hmm_model_path": "/legacy/model",
            }
        )
    return {
        "factor_list": ["factor_a", "factor_b"],
        "model_id": "model",
        "strategy_id": "score_weighted_topk_v2",
        "model_params": model_params,
        "strategy_params": {"stock_pool": "filtered_pool_20260502", "label_horizon": 10},
        "execution_algo": "V25_TWO_STAGE",
        "execution_algo_params": {"device": "cpu"},
        "stock_pool": "filtered_pool_20260502",
        "label_horizon": 10,
        "disable_alpha158": True,
        "suspend_filter_strict": True,
        "data_split": {},
    }


def _binding() -> dict:
    digest = "a" * 64
    return {
        "schema_version": "hmm_risk_qe_assistance_artifact_binding_v1",
        "artifact_schema_version": "hmm_risk_qe_assistance_coefficients_v1",
        "remote_path": f"/home/lc999/aistock_immutable_assets/hmm_qe_assistance/{digest}/hmm_sector_coefficients.json",
        "file_sha256": digest,
        "canonical_sha256": "b" * 64,
        "size_bytes": 100,
    }


def test_request_freezes_one_prediction_source_and_three_exact_arms() -> None:
    request = subject.build_three_arm_request(
        no_hmm_source=_source(hmm=False),
        legacy_hmm_source=_source(hmm=True),
        artifact_binding=_binding(),
        active_dataset_identity=_dataset_identity(),
        task_name="formal-three-arm",
    )
    loops = request["loops"]
    assert len(loops) == 3
    assert [loop["label"] for loop in loops] == [
        "QE-HMM-3ARM:no-HMM",
        "QE-HMM-3ARM:legacy-static",
        "QE-HMM-3ARM:new-PIT-v1.6",
    ]
    assert all(loop["prediction_replay"] for loop in loops)
    assert all(loop["prediction_source_task_id"] == subject.SOURCE_TASK_ID for loop in loops)
    assert all(loop["prediction_source_loop_index"] == subject.SOURCE_LOOP_INDEX for loop in loops)
    assert all(loop["prediction_source_sha256"] == subject.EXPECTED_SOURCE_FILE_SHA256 for loop in loops)
    assert all(loop["data_split"] == subject.FORMAL_DATA_SPLIT for loop in loops)
    assert all(loop["data_split"]["backtest_end"] == subject.WINDOW_END for loop in loops)
    assert loops[0].get("enable_sector_hmm") is None
    assert loops[1]["enable_sector_hmm"] is True
    assert loops[2]["enable_sector_hmm"] is True
    assert loops[2]["strategy_params"][BINDING_PARAM] == _binding()
    assert request["frozen_identity"]["active_dataset_identity"] == _dataset_identity()
    assert all("custom_params" not in loop for loop in loops)
    assert request["api_request"]["auto_start"] is False
    parsed = CustomEvolutionCreateRequest.model_validate(request["api_request"])
    assert "source_label_horizon" not in parsed.model_dump()["loops"][2]


def test_request_rejects_non_hmm_source_drift() -> None:
    legacy = _source(hmm=True)
    legacy["execution_algo"] = "OTHER"
    with pytest.raises(subject.QEAssistanceThreeArmError, match="outside approved HMM") as exc:
        subject.build_three_arm_request(
            no_hmm_source=_source(hmm=False),
            legacy_hmm_source=legacy,
            artifact_binding=_binding(),
            active_dataset_identity=_dataset_identity(),
            task_name="formal-three-arm",
        )
    assert exc.value.reason_code == subject.REASON_IDENTITY


def test_request_rejects_invalid_active_dataset_identity() -> None:
    identity = _dataset_identity()
    identity["candidate_roots"] = [identity["candidate_roots"][0], identity["candidate_roots"][0]]

    with pytest.raises(subject.QEAssistanceThreeArmError, match="active dataset identity") as exc:
        subject.build_three_arm_request(
            no_hmm_source=_source(hmm=False),
            legacy_hmm_source=_source(hmm=True),
            artifact_binding=_binding(),
            active_dataset_identity=identity,
            task_name="formal-three-arm",
        )

    assert exc.value.reason_code == subject.REASON_IDENTITY


def test_new_arm_builds_without_legacy_snapshot_lookup(monkeypatch) -> None:
    request = subject.build_three_arm_request(
        no_hmm_source=_source(hmm=False),
        legacy_hmm_source=_source(hmm=True),
        artifact_binding=_binding(),
        active_dataset_identity=_dataset_identity(),
        task_name="formal-three-arm",
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
        lambda _value: pytest.fail("artifact-bound HMM must not resolve a legacy snapshot"),
    )
    server_prepared_loop = dict(request["loops"][2])
    server_prepared_loop["source_label_horizon"] = 10
    config = build_config_from_custom_evo_loop(
        server_prepared_loop,
        {"node_id": subject.TARGET_NODE_ID, "long_trend_profile_id": None},
    )
    assert config.hmm is not None
    assert config.hmm.sector_hmm_model_path == _binding()["remote_path"]
    assert config.build_custom_params()[BINDING_PARAM] == _binding()


def test_composer_emits_compact_binding_and_remote_artifact_path(monkeypatch) -> None:
    import backend.services.quantevolver.config_composer as composer_module

    composer = ConfigComposer()
    monkeypatch.setattr(composer_module, "load_active_qe_profile", lambda: None)
    monkeypatch.setattr(composer, "_get_factors_info", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(composer, "_get_model_info", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        composer,
        "_get_strategy_info",
        lambda *_args, **_kwargs: {
            "portfolio_config": {"class": "ScoreWeightedTopkStrategyV2"},
        },
    )
    monkeypatch.setattr(
        composer,
        "_fetch_workspace_config",
        lambda *_args, **_kwargs: {
            "workspace_base": "/tmp/qe_workspace",
            "qlib_data_path": "/tmp/qlib",
            "qlib_minute_path": "/tmp/qlib_minute",
            "factor_data_dir": "/tmp/factors",
        },
    )
    monkeypatch.setattr(
        composer,
        "_prepare_risk_policy_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(
        composer,
        "_prepare_suspend_filter_runtime",
        lambda **kwargs: (kwargs["custom_params"], None),
    )
    monkeypatch.setattr(composer, "_get_read_exp_res_content", lambda: "# read_exp_res")
    monkeypatch.setattr(
        composer,
        "_compose_conf_yaml",
        lambda **kwargs: json.dumps(kwargs["custom_params"], sort_keys=True),
    )

    result = composer.compose_experiment_in_memory(
        factor_names=["factor_a"],
        model_id="model",
        strategy_id="score_weighted_topk_v2",
        data_split={
            "train_start": "2022-01-01",
            "train_end": "2022-12-31",
            "valid_start": "2023-01-01",
            "valid_end": "2023-06-30",
            "test_start": "2023-07-01",
            "test_end": "2024-06-30",
            "backtest_end": "2024-06-30",
        },
        custom_params={
            "enable_sector_hmm": True,
            "backtest_freq": "day",
            BINDING_PARAM: _binding(),
        },
        skip_db_save=True,
        execution_algo="CLOSE_PRICE",
        execution_algo_params={},
    )

    files = result["experiment_files"]
    assert BINDING_FILE in files
    assert "hmm_sector_coefficients.json" not in files
    assert _binding()["remote_path"] in files["conf.yaml"]
    assert BINDING_PARAM not in files["conf.yaml"]
    assert len(files[BINDING_FILE]) < 1024


def _result(arm: str, value: float) -> dict:
    return {
        "arm": arm,
        "status": "completed",
        "metrics": {
            "cost_after_annualized_return": value,
            "cost_after_compounded_return": value,
            "information_ratio": 1.0,
            "max_drawdown": -0.1,
            "average_turnover": 2.0,
            "total_cost_drag": 0.01,
            "average_cost_drag": 0.0001,
        },
        "artifact_hashes": {
            "holdings_sha256": "1" * 64,
            "orders_sha256": "2" * 64,
            "fills_sha256": "3" * 64,
        },
        "holdings_changed_count": 0 if arm == "no_hmm" else 3,
        "orders_changed_count": 0 if arm == "no_hmm" else 2,
        "fills_changed_count": 0 if arm == "no_hmm" else 1,
        "completed_dates": 423,
        "first_trade_date": "2024-07-02",
        "last_trade_date": "2026-03-31",
    }


def test_result_requires_all_arms_and_does_not_auto_select() -> None:
    result = subject.compare_three_arm_results(
        [_result("no_hmm", 0.10), _result("legacy_static_hmm", 0.11), _result("new_pit_hmm", 0.12)]
    )
    assert result["status"] == subject.STATUS_BENEFIT
    assert result["benefit_vs_no_hmm_observed"] is True
    assert result["superior_to_old_hmm_observed"] is True
    assert result["superior_to_old_hmm_observation_code"] == subject.STATUS_SUPERIOR
    assert result["delta_legacy_static_hmm_vs_no_hmm"] == pytest.approx(0.01)
    assert not any(result["numeric_ties"].values())
    assert result["automatic_model_selection_performed"] is False
    assert result["automatic_runtime_activation_performed"] is False

    missing = copy.deepcopy([_result("no_hmm", 0.10), _result("new_pit_hmm", 0.12)])
    with pytest.raises(subject.QEAssistanceThreeArmError) as exc:
        subject.compare_three_arm_results(missing)
    assert exc.value.reason_code == subject.REASON_RESULT


def test_result_records_old_hmm_superiority_without_overriding_primary_state() -> None:
    result = subject.compare_three_arm_results(
        [_result("no_hmm", 0.03), _result("legacy_static_hmm", 0.01), _result("new_pit_hmm", 0.02)]
    )

    assert result["status"] == subject.STATUS_NO_INCREMENTAL
    assert result["benefit_vs_no_hmm_observed"] is False
    assert result["superior_to_old_hmm_observed"] is True


def test_result_requires_durable_counts_dates_and_lowercase_hashes() -> None:
    rows = [_result("no_hmm", 0.01), _result("legacy_static_hmm", 0.02), _result("new_pit_hmm", 0.03)]
    rows[2]["artifact_hashes"]["fills_sha256"] = "G" * 64
    with pytest.raises(subject.QEAssistanceThreeArmError, match="durable business hashes"):
        subject.compare_three_arm_results(rows)

    rows[2] = _result("new_pit_hmm", 0.03)
    rows[2]["completed_dates"] = 0
    with pytest.raises(subject.QEAssistanceThreeArmError, match="completed-date identity"):
        subject.compare_three_arm_results(rows)


def test_result_records_numeric_ties_without_promoting_them() -> None:
    result = subject.compare_three_arm_results(
        [_result("no_hmm", 0.02), _result("legacy_static_hmm", 0.02), _result("new_pit_hmm", 0.02)]
    )
    assert result["status"] == subject.STATUS_NO_INCREMENTAL
    assert result["benefit_vs_no_hmm_observed"] is False
    assert result["superior_to_old_hmm_observed"] is False
    assert all(result["numeric_ties"].values())


def test_new_arm_typed_failure_is_not_converted_to_success() -> None:
    failed = {
        "arm": "new_pit_hmm",
        "status": "failed",
        "failure_reason": "hmm_risk_qe_assistance_pit_mapping_missing",
    }
    result = subject.compare_three_arm_results([_result("no_hmm", 0.02), _result("legacy_static_hmm", 0.03), failed])
    assert result["status"] == subject.STATUS_NEW_UNAVAILABLE
    assert result["arms"]["new_pit_hmm"]["failure_reason"] == failed["failure_reason"]
    assert result["delta_new_vs_no_hmm"] is None
    assert result["automatic_model_selection_performed"] is False
