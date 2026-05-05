"""
Level 1 单元测试 — ExperimentConfig + HmmConfig

纯逻辑测试，无外部依赖（无 DB、无 HMM 服务）。
验证 build_custom_params() 产出与各路径现有代码完全一致。
"""
import pytest
import types
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.services.quantevolver.experiment_config import (
    ExperimentConfig,
    HmmConfig,
    default_qe_risk_policy,
)
from backend.services.quantevolver.experiment_config_builders import (
    _build_hmm_config_from_fields,
    _pop_hmm_fields,
    _resolve_hmm_config_json,
    build_config_from_exp_record,
    build_config_from_evolution_loop,
    build_config_from_strategy_evo_loop,
    build_config_from_custom_evo_loop,
    build_config_from_multi_alpha,
)
from tests.fixtures.sample_configs import (
    EXP_RECORD_MINIMAL,
    EXP_RECORD_WITH_HMM,
    EVOLUTION_CONFIG_MINIMAL,
    EVOLUTION_TASK_MINIMAL,
    EVOLUTION_TASK_WITH_UNFILLED,
    EVOLUTION_TASK_WITH_STRATEGY_PARAMS,
    EVOLUTION_TASK_WITH_HMM,
    STRATEGY_EVO_BASE_CONFIG,
    STRATEGY_EVO_LOOP_NO_HMM,
    STRATEGY_EVO_LOOP_WITH_HMM,
    STRATEGY_EVO_TASK,
    CUSTOM_EVO_LOOP_MINIMAL,
    CUSTOM_EVO_LOOP_FULL,
    CUSTOM_EVO_TASK,
    HMM_SNAPSHOT,
)


@pytest.fixture(autouse=True)
def _stub_optional_hmm_config_json(monkeypatch, request):
    if request.node.name == "test_hmm_config_resolves_hidden_snapshot_config":
        return
    monkeypatch.setattr(
        "backend.services.quantevolver.experiment_config_builders._resolve_hmm_config_json",
        lambda _snapshot_id: None,
    )


MULTI_ALPHA_CONFIG = {
    "alpha_groups": [
        {"group_name": "momentum", "factor_names": ["Alpha001"], "model_id": "model_lgbm_v1"},
        {"group_name": "flow", "factor_names": ["Alpha002"], "model_id": "model_lgbm_v1"},
    ],
    "meta_model": {"method": "equal"},
    "execution_mode": "serial",
}


# ── HmmConfig 单元测试 ─────────────────────────────────────────────────────────

class TestHmmConfig:
    def test_disabled_hmm_no_validation(self):
        hmm = HmmConfig(enable_sector_hmm=False)
        assert hmm.enable_sector_hmm is False
        assert hmm.hmm_model_version_id is None

    def test_enabled_hmm_requires_version_id(self):
        with pytest.raises(ValueError, match="hmm_model_version_id"):
            HmmConfig(enable_sector_hmm=True, sector_hmm_model_path="/some/path")

    def test_enabled_hmm_requires_model_path(self):
        with pytest.raises(ValueError, match="sector_hmm_model_path"):
            HmmConfig(enable_sector_hmm=True, hmm_model_version_id="snap_001")

    def test_enabled_hmm_valid(self):
        hmm = HmmConfig(
            enable_sector_hmm=True,
            hmm_model_version_id="snap_001",
            sector_hmm_model_path="/mnt/f/models/hmm.pkl",
            hmm_signal_preset="conservative",
        )
        assert hmm.enable_sector_hmm is True
        assert hmm.sector_hmm_model_path == "/mnt/f/models/hmm.pkl"


# ── ExperimentConfig 单元测试 ──────────────────────────────────────────────────

class TestExperimentConfig:
    def test_empty_factor_names_raises(self):
        with pytest.raises(ValueError, match="factor_names"):
            ExperimentConfig(factor_names=[], model_id="lgbm")

    def test_minimal_config(self):
        cfg = ExperimentConfig(factor_names=["f1", "f2"], model_id="lgbm")
        assert cfg.factor_names == ["f1", "f2"]
        assert cfg.model_id == "lgbm"

    def test_build_custom_params_empty(self):
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")
        params = cfg.build_custom_params()
        assert params == {"risk_policy": default_qe_risk_policy()}

    def test_build_custom_params_rejects_disabled_risk_policy(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            extra_params={"risk_policy": {"enabled": False}},
        )
        with pytest.raises(ValueError, match="risk_policy.enabled=false"):
            cfg.build_custom_params()

    def test_build_custom_params_pops_initial_cash(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            strategy_params={"topk": 50, "initial_cash": 1_000_000},
        )
        params = cfg.build_custom_params()
        assert "initial_cash" not in params
        assert params["topk"] == 50

    def test_build_strategy_params_keeps_initial_cash(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            strategy_params={"topk": 50, "initial_cash": 1_000_000},
        )
        sp = cfg.build_strategy_params()
        assert sp["initial_cash"] == 1_000_000

    def test_build_custom_params_hmm(self):
        hmm = HmmConfig(
            enable_sector_hmm=True,
            hmm_model_version_id="snap_001",
            sector_hmm_model_path="/mnt/f/models/hmm.pkl",
            hmm_signal_preset="conservative",
        )
        cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm", hmm=hmm)
        params = cfg.build_custom_params()
        assert params["enable_sector_hmm"] is True
        assert params["hmm_model_version_id"] == "snap_001"
        assert params["sector_hmm_model_path"] == "/mnt/f/models/hmm.pkl"
        assert params["hmm_signal_preset"] == "conservative"

    def test_build_custom_params_unfilled_handler(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            unfilled_handler="cancel_and_resubmit",
            unfilled_handler_params={"trigger_minute": 145, "backup_depth": 3},
        )
        params = cfg.build_custom_params()
        assert params["unfilled_handler"] == "cancel_and_resubmit"
        assert params["unfilled_trigger_minute"] == 145
        assert params["unfilled_backup_depth"] == 3

    def test_build_custom_params_sector_blacklist(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            sector_blacklist=["SW_Coal", "SW_Steel"],
        )
        params = cfg.build_custom_params()
        assert params["sector_blacklist"] == ["SW_Coal", "SW_Steel"]

    def test_build_custom_params_model_params_base_overridden_by_strategy(self):
        """strategy_params should override model_params_base for same keys."""
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            model_params_base={"topk": 50, "n_drop": 5},
            strategy_params={"topk": 30},  # overrides
        )
        params = cfg.build_custom_params()
        assert params["topk"] == 30  # strategy_params wins
        assert params["n_drop"] == 5  # from model_params_base

    def test_build_custom_params_label_type(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            label_type="Ref($close, -2)/Ref($close, -1) - 1",
        )
        params = cfg.build_custom_params()
        assert params["label_type"] == "Ref($close, -2)/Ref($close, -1) - 1"

    def test_build_custom_params_stock_pool(self):
        cfg = ExperimentConfig(
            factor_names=["f1"],
            model_id="lgbm",
            stock_pool="/mnt/f/data/csi300.txt",
        )
        params = cfg.build_custom_params()
        assert params["stock_pool"] == "/mnt/f/data/csi300.txt"


# ── Builder 函数测试 ───────────────────────────────────────────────────────────

class TestBuildConfigFromExpRecord:
    def test_minimal(self):
        cfg = build_config_from_exp_record(EXP_RECORD_MINIMAL)
        assert cfg.factor_names == ["Alpha001", "Alpha002"]
        assert cfg.model_id == "model_lgbm_v1"
        assert cfg.hmm is None

    def test_with_hmm(self):
        cfg = build_config_from_exp_record(EXP_RECORD_WITH_HMM)
        assert cfg.hmm is not None
        assert cfg.hmm.enable_sector_hmm is True
        assert cfg.hmm.sector_hmm_model_path == "/mnt/f/models/hmm_v2/model.pkl"
        assert cfg.execution_algo == "twap"
        assert cfg.execution_algo_params == {"interval": 5}

    def test_hmm_keys_not_in_extra_params(self):
        """HMM keys must be extracted from custom_params, not left in extra_params."""
        cfg = build_config_from_exp_record(EXP_RECORD_WITH_HMM)
        extra = cfg.extra_params or {}
        for key in ("enable_sector_hmm", "sector_hmm_model_path",
                    "hmm_model_version_id", "hmm_signal_preset"):
            assert key not in extra, f"{key} should not be in extra_params"

    def test_execution_algo_not_in_extra_params(self):
        cfg = build_config_from_exp_record(EXP_RECORD_WITH_HMM)
        extra = cfg.extra_params or {}
        assert "execution_algo" not in extra
        assert "execution_algo_params" not in extra


    def test_invalid_unfilled_handler_params_raises(self):
        """unfilled_handler_params 解析后非 dict 时应 raise，不能静默返回 {}"""
        import json
        from backend.services.quantevolver.experiment_config_builders import _build_unfilled_handler_params
        # 合法 JSON 但类型是 list，不是 dict
        with pytest.raises(ValueError, match="unfilled_handler_params must be a dict"):
            _build_unfilled_handler_params(json.dumps([1, 2, 3]))
    def test_minimal(self):
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_MINIMAL
        )
        assert cfg.factor_names == ["Alpha001", "Alpha003"]
        assert cfg.stock_pool == "csi300"
        assert cfg.label_type == "Ref($close, -2)/Ref($close, -1) - 1"
        assert cfg.hmm is None

    def test_task_level_hmm_applies_to_auto_evolution_loop(self):
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_WITH_HMM
        )
        params = cfg.build_custom_params()
        assert cfg.hmm is not None
        assert params["enable_sector_hmm"] is True
        assert params["hmm_model_version_id"] == "hmm_snap_001"
        assert params["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]
        assert params["hmm_signal_preset"] == "preset_B"
        assert "enable_sector_hmm" not in (cfg.strategy_params or {})

    def test_task_level_hmm_overrides_reviewer_loop_config(self):
        reviewer_config = {
            **EVOLUTION_CONFIG_MINIMAL,
            "model_params": {
                "topk": 50,
                "enable_sector_hmm": False,
                "hmm_model_version_id": "reviewer_should_not_win",
            },
        }
        cfg = build_config_from_evolution_loop(
            reviewer_config, EVOLUTION_TASK_WITH_HMM
        )
        params = cfg.build_custom_params()
        assert params["enable_sector_hmm"] is True
        assert params["hmm_model_version_id"] == "hmm_snap_001"
        assert params["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]

    def test_unfilled_handler(self):
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_WITH_UNFILLED
        )
        params = cfg.build_custom_params()
        assert params["unfilled_handler"] == "cancel_and_resubmit"
        assert params["unfilled_trigger_minute"] == 145
        assert params["unfilled_backup_depth"] == 3

    def test_strategy_params_merged_into_custom_params(self):
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_WITH_STRATEGY_PARAMS
        )
        params = cfg.build_custom_params()
        assert params["topk"] == 30
        assert params["n_drop"] == 3

    def test_model_params_base_in_custom_params(self):
        cfg = build_config_from_evolution_loop(
            EVOLUTION_CONFIG_MINIMAL, EVOLUTION_TASK_MINIMAL
        )
        params = cfg.build_custom_params()
        # model_params has topk=50
        assert params.get("topk") == 50

    def test_auto_evolution_inherits_hmm_from_base_experiment_config(self):
        base_config = {
            **EVOLUTION_CONFIG_MINIMAL,
            "model_params": {
                "topk": 50,
                "enable_sector_hmm": True,
                "hmm_model_version_id": "hmm_snap_001",
                "sector_hmm_model_path": HMM_SNAPSHOT["model_path"],
                "hmm_signal_preset": "preset_A",
            },
        }
        cfg = build_config_from_evolution_loop(base_config, EVOLUTION_TASK_MINIMAL)
        params = cfg.build_custom_params()
        assert params["enable_sector_hmm"] is True
        assert params["hmm_model_version_id"] == "hmm_snap_001"
        assert params["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]


class TestBuildConfigFromStrategyEvoLoop:
    def test_no_hmm(self):
        cfg = build_config_from_strategy_evo_loop(
            STRATEGY_EVO_BASE_CONFIG, STRATEGY_EVO_LOOP_NO_HMM, STRATEGY_EVO_TASK
        )
        assert cfg.hmm is None
        assert cfg.sector_blacklist == ["SW_Coal", "SW_Steel"]
        params = cfg.build_custom_params()
        assert params["sector_blacklist"] == ["SW_Coal", "SW_Steel"]
        assert params["topk"] == 30  # strategy_params override

    def test_with_hmm(self):
        mock_snapshot = HMM_SNAPSHOT.copy()
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=mock_snapshot["model_path"],
        ):
            cfg = build_config_from_strategy_evo_loop(
                STRATEGY_EVO_BASE_CONFIG, STRATEGY_EVO_LOOP_WITH_HMM, STRATEGY_EVO_TASK
            )
        assert cfg.hmm is not None
        assert cfg.hmm.enable_sector_hmm is True
        assert cfg.hmm.sector_hmm_model_path == mock_snapshot["model_path"]
        params = cfg.build_custom_params()
        assert params["enable_sector_hmm"] is True
        assert params["sector_hmm_model_path"] == mock_snapshot["model_path"]


class TestBuildConfigFromCustomEvoLoop:
    def test_minimal(self):
        cfg = build_config_from_custom_evo_loop(CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK)
        assert cfg.factor_names == ["Alpha001", "Alpha002"]
        assert cfg.hmm is None
        params = cfg.build_custom_params()
        assert params.get("topk") == 50

    def test_full_config(self):
        mock_path = HMM_SNAPSHOT["model_path"]
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=mock_path,
        ):
            cfg = build_config_from_custom_evo_loop(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK)

        params = cfg.build_custom_params()

        # HMM
        assert params["enable_sector_hmm"] is True
        assert params["sector_hmm_model_path"] == mock_path
        assert params["hmm_signal_preset"] == "conservative"

        # sector_blacklist
        assert params["sector_blacklist"] == ["SW_Coal", "SW_Steel"]

        # stock_pool
        assert params["stock_pool"] == "/mnt/f/data/stock_pools/csi300.txt"

        # label_type
        assert params["label_type"] == "Ref($close, -2)/Ref($close, -1) - 1"

        # unfilled
        assert params["unfilled_handler"] == "cancel_and_resubmit"
        assert params["unfilled_trigger_minute"] == 145
        assert params["unfilled_backup_depth"] == 3

        # initial_cash not present
        assert "initial_cash" not in params

    def test_factor_keys_split(self):
        """factor_keys with '||' separator should be split correctly."""
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=HMM_SNAPSHOT["model_path"],
        ):
            cfg = build_config_from_custom_evo_loop(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK)
        assert cfg.factor_names == ["Alpha001", "mf_rsi_14d"]

    def test_hmm_snapshot_not_found_raises(self):
        loop = {**CUSTOM_EVO_LOOP_FULL, "hmm_model_version_id": "nonexistent_snap"}
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            side_effect=ValueError("HMM snapshot 'nonexistent_snap' does not exist"),
        ):
            with pytest.raises(ValueError, match="nonexistent_snap"):
                build_config_from_custom_evo_loop(loop, CUSTOM_EVO_TASK)

    def test_multi_alpha_custom_loop_with_hmm(self):
        loop = {
            **CUSTOM_EVO_LOOP_FULL,
            "alpha_mode": "multi",
            "multi_alpha_config": MULTI_ALPHA_CONFIG,
        }
        with patch(
            "backend.services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=HMM_SNAPSHOT["model_path"],
        ):
            cfg = build_config_from_custom_evo_loop(loop, CUSTOM_EVO_TASK)
        params = cfg.build_custom_params()
        assert cfg.alpha_mode == "multi"
        assert cfg.multi_alpha_config is not None
        assert params["enable_sector_hmm"] is True
        assert params["hmm_model_version_id"] == "hmm_snap_001"

    def test_hmm_config_resolves_hidden_snapshot_config(self):
        svc = MagicMock()
        svc.get_snapshot.return_value = {"snapshot_id": "snap_hidden", "config_id": "cfg_hidden"}
        svc.get_config.return_value = {
            "config_id": "cfg_hidden",
            "model_type": "sector_hmm_experimental_stacking_20260504",
            "config_json": {"runtime_preset": "preset_A", "precomputed_only": True},
        }
        fake_module = types.SimpleNamespace(HMMTrainingService=MagicMock(return_value=svc))
        with patch.dict(sys.modules, {"backend.services.hmm_training_service": fake_module}):
            cfg_json = _resolve_hmm_config_json("snap_hidden")

        assert cfg_json["runtime_preset"] == "preset_A"
        svc.get_config.assert_called_once_with("cfg_hidden")


class TestBuildConfigFromMultiAlpha:
    def test_single_multi_alpha_builder_accepts_hmm_config(self):
        cfg = build_config_from_multi_alpha(
            multi_alpha_config=MULTI_ALPHA_CONFIG,
            data_split={"train_start": "2020-01-01", "test_end": "2023-01-01"},
            strategy_id="TopkDropoutStrategy",
            hmm_config={
                "enable_sector_hmm": True,
                "hmm_model_version_id": "hmm_snap_001",
                "sector_hmm_model_path": HMM_SNAPSHOT["model_path"],
                "hmm_signal_preset": "preset_A",
            },
        )
        params = cfg.build_custom_params()
        assert cfg.alpha_mode == "multi"
        assert cfg.multi_alpha_config is not None
        assert params["enable_sector_hmm"] is True
        assert params["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]

    def test_multi_alpha_auto_loop_hmm_uses_task_level_policy(self):
        base_custom_params = {
            "enable_sector_hmm": True,
            "hmm_model_version_id": "base_hmm",
            "sector_hmm_model_path": "/mnt/f/base_hmm/models.json",
            "hmm_signal_preset": "preset_A",
        }
        base_strategy_params = {
            "topk": 20,
            "enable_sector_hmm": False,
            "hmm_model_version_id": "strategy_should_not_win",
        }
        task_strategy_params = dict(EVOLUTION_TASK_WITH_HMM["strategy_params"])

        custom_clean = dict(base_custom_params)
        strategy_clean = dict(base_strategy_params)
        task_clean = dict(task_strategy_params)
        hmm_cfg = _build_hmm_config_from_fields(
            _pop_hmm_fields(custom_clean),
            _pop_hmm_fields(strategy_clean),
            _pop_hmm_fields(task_clean),
        )

        cfg = build_config_from_multi_alpha(
            multi_alpha_config=MULTI_ALPHA_CONFIG,
            data_split={"train_start": "2020-01-01", "test_end": "2023-01-01"},
            strategy_id="TopkDropoutStrategy",
            strategy_params=strategy_clean,
            hmm_config=hmm_cfg,
        )
        params = cfg.build_custom_params()
        assert params["topk"] == 20
        assert params["enable_sector_hmm"] is True
        assert params["hmm_model_version_id"] == "hmm_snap_001"
        assert params["sector_hmm_model_path"] == HMM_SNAPSHOT["model_path"]
        assert "enable_sector_hmm" not in cfg.build_strategy_params()
