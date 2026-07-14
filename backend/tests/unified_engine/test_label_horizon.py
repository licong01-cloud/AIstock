import pandas as pd
import pytest

from backend.services.quantevolver.config_composer import (
    ConfigComposer,
    _requires_qe_custom_loaders,
)
from backend.services.quantevolver.experiment_config import (
    ALLOWED_LABEL_HORIZONS,
    ExperimentConfig,
    normalize_label_horizon,
)
from backend.services.quantevolver.experiment_config_builders import (
    build_config_from_custom_evo_loop,
    build_config_from_strategy_evo_loop,
)
from backend.services.quantevolver.qe_custom_loaders import (
    DynamicFactorsOnlyLoader,
    LongHorizonLabelMaturityPurge,
)
from backend.services.quantevolver.label_horizon_schema import (
    parse_label_horizon_constraint_values,
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


@pytest.mark.parametrize("horizon", ALLOWED_LABEL_HORIZONS)
def test_normalize_label_horizon_accepts_supported_horizons(horizon):
    assert normalize_label_horizon(horizon) == horizon


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
    with pytest.raises(ValueError, match="label_horizon"):
        ExperimentConfig(
            factor_names=["Alpha001"],
            model_id="model_lgbm_v1",
            label_horizon=181,
        )


def test_schema_constraint_parser_requires_exact_horizon_values():
    definition = (
        "CHECK ((label_horizon = ANY (ARRAY[1, 3, 5, 10, 20, 30, 40, 60, 120, 180])))"
    )

    assert parse_label_horizon_constraint_values(definition) == frozenset(
        ALLOWED_LABEL_HORIZONS
    )
    assert 3 not in parse_label_horizon_constraint_values(
        "CHECK (label_horizon IN (1, 5, 10, 20, 30, 40, 60, 120, 180))"
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


def test_config_composer_emits_long_horizon_maturity_purge():
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info=None,
        data_split=DATA_SPLIT,
        custom_params={"label_type": "close", "label_horizon": 180},
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert 'label: ["Ref($close, -181) / Ref($close, -1) - 1"]' in yaml_text
    assert "class: LongHorizonLabelMaturityPurge" in yaml_text
    assert "module_path: qe_custom_loaders" in yaml_text
    assert "label_horizon: 180" in yaml_text
    assert "test_end: 2021-12-31" in yaml_text


def test_long_horizon_workspace_requires_qe_runtime_module():
    assert _requires_qe_custom_loaders(
        has_custom_factors=False,
        disable_alpha158=False,
        custom_params={"label_horizon": 180},
    ) is True
    assert _requires_qe_custom_loaders(
        has_custom_factors=False,
        disable_alpha158=False,
        custom_params={"label_horizon": 20},
    ) is False


def test_config_composer_keeps_existing_20d_learning_contract():
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info=None,
        data_split=DATA_SPLIT,
        custom_params={"label_horizon": 20},
        has_custom_factors=False,
        has_alpha158=False,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert "LongHorizonLabelMaturityPurge" not in yaml_text


def test_data_split_rejects_overlapping_inclusive_boundaries():
    overlapping = dict(DATA_SPLIT)
    overlapping["train_end"] = overlapping["valid_start"]

    with pytest.raises(ValueError, match="必须早于 valid_start"):
        ConfigComposer._validate_data_split(overlapping)


def test_dynamic_factors_only_loader_builds_horizon_aware_label():
    assert (
        DynamicFactorsOnlyLoader.build_label_expr("close", 10)
        == "Ref($close, -11) / Ref($close, -1) - 1"
    )
    assert (
        DynamicFactorsOnlyLoader.build_label_expr("vwap", 20)
        == "Ref($vwap, -21) / Ref($vwap, -1) - 1"
    )
    assert (
        DynamicFactorsOnlyLoader.build_label_expr("close", 180)
        == "Ref($close, -181) / Ref($close, -1) - 1"
    )


def test_long_horizon_purge_masks_learning_labels_but_keeps_features():
    calendar = pd.bdate_range("2020-01-01", periods=900)
    index = pd.MultiIndex.from_product(
        [calendar, ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame(
        {
            ("feature", "F0"): 1.0,
            ("label", "LABEL0"): 2.0,
        },
        index=index,
    )
    processor = LongHorizonLabelMaturityPurge(
        label_horizon=180,
        train_start=calendar[0].date().isoformat(),
        train_end=calendar[249].date().isoformat(),
        valid_start=calendar[250].date().isoformat(),
        valid_end=calendar[499].date().isoformat(),
        test_start=calendar[500].date().isoformat(),
        test_end=calendar[899].date().isoformat(),
    )

    result = processor(frame.copy())

    assert result.loc[(calendar[68], "000001.SZ"), ("label", "LABEL0")] == 2.0
    assert pd.isna(result.loc[(calendar[69], "000001.SZ"), ("label", "LABEL0")])
    assert pd.isna(result.loc[(calendar[899], "000001.SZ"), ("label", "LABEL0")])
    assert result.loc[(calendar[899], "000001.SZ"), ("feature", "F0")] == 1.0
    assert processor.purge_summary["train"]["masked_rows"] == 181
    assert processor.purge_summary["valid"]["masked_rows"] == 181
    assert processor.purge_summary["test"]["masked_rows"] == 181


def test_long_horizon_purge_fails_when_segment_has_no_mature_sample():
    calendar = pd.bdate_range("2020-01-01", periods=500)
    index = pd.MultiIndex.from_product(
        [calendar, ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    frame = pd.DataFrame({("label", "LABEL0"): 1.0}, index=index)
    processor = LongHorizonLabelMaturityPurge(
        label_horizon=180,
        train_start=calendar[0].date().isoformat(),
        train_end=calendar[100].date().isoformat(),
        valid_start=calendar[101].date().isoformat(),
        valid_end=calendar[300].date().isoformat(),
        test_start=calendar[301].date().isoformat(),
        test_end=calendar[499].date().isoformat(),
    )

    with pytest.raises(ValueError, match="train segment"):
        processor(frame)


def test_config_composer_passes_no_alpha_label_horizon_to_dynamic_loader():
    yaml_text = ConfigComposer()._compose_conf_yaml(
        factors_info=[],
        model_info=None,
        strategy_info=None,
        data_split=DATA_SPLIT,
        custom_params={
            "label_type": "vwap",
            "label_horizon": 10,
        },
        has_custom_factors=True,
        has_alpha158=False,
        disable_alpha158=True,
        backtest_freq="day",
        execution_algo="CLOSE_PRICE",
    )

    assert "class: DynamicFactorsOnlyLoader" in yaml_text
    assert 'label_type: "vwap"' in yaml_text
    assert "label_horizon: 10" in yaml_text


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
