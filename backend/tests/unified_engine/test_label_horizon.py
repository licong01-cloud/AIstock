import pytest

from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.experiment_config import ExperimentConfig, normalize_label_horizon
from backend.services.quantevolver.experiment_config_builders import (
    build_config_from_custom_evo_loop,
    build_config_from_strategy_evo_loop,
)
from backend.services.quantevolver.qe_evolution_service import AutoEvolutionScheduler


DATA_SPLIT = {
    "train_start": "2020-01-01",
    "train_end": "2020-12-31",
    "valid_start": "2021-01-01",
    "valid_end": "2021-06-30",
    "test_start": "2021-07-01",
    "test_end": "2021-12-31",
    "backtest_end": "2021-12-31",
}


def test_experiment_config_omits_legacy_1d_label_horizon():
    cfg = ExperimentConfig(factor_names=["Alpha001"], model_id="model_lgbm_v1")

    assert "label_horizon" not in cfg.build_custom_params()


def test_experiment_config_injects_non_default_label_horizon():
    cfg = ExperimentConfig(
        factor_names=["Alpha001"],
        model_id="model_lgbm_v1",
        label_horizon=20,
    )

    assert cfg.build_custom_params()["label_horizon"] == 20


def test_normalize_label_horizon_accepts_20d():
    assert normalize_label_horizon(20) == 20


def test_experiment_config_rejects_invalid_label_horizon():
    with pytest.raises(ValueError, match="label_horizon"):
        ExperimentConfig(
            factor_names=["Alpha001"],
            model_id="model_lgbm_v1",
            label_horizon=2,
        )
    with pytest.raises(ValueError, match="label_horizon"):
        ExperimentConfig(
            factor_names=["Alpha001"],
            model_id="model_lgbm_v1",
            label_horizon=21,
        )


def test_config_composer_keeps_legacy_1d_close_formula():
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info=None,
        data_split=DATA_SPLIT,
        custom_params={},
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert 'label: ["Ref($close, -2) / Ref($close, -1) - 1"]' in yaml_text


def test_config_composer_uses_horizon_aware_formula():
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info=None,
        data_split=DATA_SPLIT,
        custom_params={"label_type": "vwap", "label_horizon": 20},
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert 'label: ["Ref($vwap, -21) / Ref($vwap, -1) - 1"]' in yaml_text


def test_config_composer_does_not_pass_blacklist_metadata_to_strategy():
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info={
            "strategy_id": "score_weighted_topk_v2",
            "source_code": "class ScoreWeightedTopkStrategyV2(object):\n    pass\n",
            "portfolio_config": {
                "class": "ScoreWeightedTopkStrategyV2",
                "kwargs": {},
            },
        },
        data_split=DATA_SPLIT,
        custom_params={
            "topk": 50,
            "n_drop": 5,
            "sector_blacklist": ["801125.SI"],
            "sector_blacklist_enabled": True,
            "sector_blacklist_snapshot": {
                "items": [{"sw2_code": "801125.SI", "sw2_name": "White Liquor"}],
            },
            "blacklist_enabled": True,
        },
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert "ScoreWeightedTopkStrategyV2" in yaml_text
    assert "sector_blacklist" not in yaml_text
    assert "sector_blacklist_enabled" not in yaml_text
    assert "sector_blacklist_snapshot" not in yaml_text
    assert "blacklist_enabled" not in yaml_text


def test_strategy_backtest_only_rejects_label_horizon_conflict():
    base = {
        "factor_list": ["Alpha001"],
        "model_id": "model_lgbm_v1",
        "strategy_id": "TopkDropoutStrategy",
        "model_params": {"label_horizon": 5},
        "data_split": DATA_SPLIT,
    }
    loop = {"strategy_params": {}, "label_horizon": 3}
    task = {"task_id": "task", "label_horizon": 5}

    with pytest.raises(ValueError, match="backtest-only label_horizon"):
        build_config_from_strategy_evo_loop(base, loop, task)


def test_custom_backtest_only_requires_source_label_horizon():
    loop = {
        "factor_keys": ["Alpha001||v1"],
        "model_id": "model_lgbm_v1",
        "strategy_id": "TopkDropoutStrategy",
        "strategy_params": {},
        "backtest_only": True,
        "model_source_task_id": "src",
        "model_source_loop_index": 1,
    }

    with pytest.raises(ValueError, match="source_label_horizon"):
        build_config_from_custom_evo_loop(loop, {"task_id": "task"})


def test_scheduler_reads_top_level_loop_label_horizon():
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    assert scheduler._extract_label_horizon_from_config(
        {"label_horizon": 5, "model_params": {}},
        context="loop",
    ) == 5


def test_scheduler_rejects_loop_label_horizon_conflict():
    scheduler = AutoEvolutionScheduler.__new__(AutoEvolutionScheduler)

    with pytest.raises(ValueError, match="conflicts"):
        scheduler._extract_label_horizon_from_config(
            {"label_horizon": 5, "model_params": {"label_horizon": 3}},
            context="loop",
        )
