"""
Level 3 A/B 对比测试 — 新旧路径 custom_params 一致性验证

验证统一引擎（ExperimentConfig.build_custom_params）产出的参数
与旧路径（submit_custom_evo_loop 手动组装）完全一致。

不依赖真实 DB 或 RDAgent，使用 Mock 拦截所有外部调用。
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, call
from typing import Any

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.quantevolver.experiment_config_builders import build_config_from_custom_evo_loop
from services.quantevolver.executors.base import ExecutionContext
from services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
from tests.fixtures.sample_configs import (
    CUSTOM_EVO_LOOP_MINIMAL,
    CUSTOM_EVO_LOOP_FULL,
    CUSTOM_EVO_TASK,
    HMM_SNAPSHOT,
)


# ── 辅助：模拟旧路径的参数组装逻辑 ────────────────────────────────────────────

def _legacy_build_custom_params(loop_config: dict, task: dict, hmm_model_path: str | None = None) -> dict:
    """
    完全复制 submit_custom_evo_loop 中的手动参数组装逻辑
    (qe_evolution_service.py:3351-3404)，用于 A/B 对比。
    """
    import json

    strategy_params = dict(loop_config.get("strategy_params") or {})
    loop_custom_params = dict(strategy_params)

    if loop_config.get("enable_sector_hmm"):
        loop_custom_params["enable_sector_hmm"] = True
        hmm_ver = loop_config.get("hmm_model_version_id")
        loop_custom_params["hmm_model_version_id"] = hmm_ver
        loop_custom_params["sector_hmm_model_path"] = hmm_model_path
        hmm_preset = loop_config.get("hmm_signal_preset")
        if hmm_preset:
            loop_custom_params["hmm_signal_preset"] = hmm_preset

    if loop_config.get("sector_blacklist"):
        loop_custom_params["sector_blacklist"] = loop_config["sector_blacklist"]

    if loop_config.get("stock_pool"):
        loop_custom_params["stock_pool"] = loop_config["stock_pool"]

    label_type = loop_config.get("label_type")
    if label_type:
        loop_custom_params["label_type"] = label_type

    _uf = loop_config.get("unfilled_handler")
    if _uf:
        loop_custom_params["unfilled_handler"] = _uf
        _uf_params = loop_config.get("unfilled_handler_params") or {}
        if isinstance(_uf_params, str):
            _uf_params = json.loads(_uf_params)
        if _uf_params.get("trigger_minute"):
            loop_custom_params["unfilled_trigger_minute"] = _uf_params["trigger_minute"]
        if _uf_params.get("backup_depth"):
            loop_custom_params["unfilled_backup_depth"] = _uf_params["backup_depth"]

    loop_custom_params.pop("initial_cash", None)
    return loop_custom_params


def _legacy_build_strategy_params(loop_config: dict) -> dict:
    """旧路径的 strategy_params（_sp = strategy_params.copy()）"""
    return dict(loop_config.get("strategy_params") or {})


# ── A/B 对比测试 ───────────────────────────────────────────────────────────────

class TestABComparisonPath4:
    """验证新旧路径在 Path 4（自定义演进）的参数完全一致。"""

    def _get_unified_params(self, loop_config, task, hmm_path=None):
        """通过统一引擎获取 custom_params 和 strategy_params。"""
        with patch(
            "services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=hmm_path or HMM_SNAPSHOT["model_path"],
        ):
            cfg = build_config_from_custom_evo_loop(loop_config, task)
        return cfg.build_custom_params(), cfg.build_strategy_params()

    def _get_legacy_params(self, loop_config, task, hmm_path=None):
        """通过旧路径获取 custom_params 和 strategy_params。"""
        return (
            _legacy_build_custom_params(loop_config, task, hmm_path),
            _legacy_build_strategy_params(loop_config),
        )

    def test_minimal_config_custom_params_identical(self):
        """最小配置：新旧 custom_params 完全一致"""
        unified_cp, _ = self._get_unified_params(CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK)
        legacy_cp, _ = self._get_legacy_params(CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK)
        assert unified_cp == legacy_cp, f"差异: {set(unified_cp.items()) ^ set(legacy_cp.items())}"

    def test_minimal_config_strategy_params_identical(self):
        """最小配置：新旧 strategy_params 完全一致"""
        _, unified_sp = self._get_unified_params(CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK)
        _, legacy_sp = self._get_legacy_params(CUSTOM_EVO_LOOP_MINIMAL, CUSTOM_EVO_TASK)
        assert unified_sp == legacy_sp

    def test_full_config_custom_params_identical(self):
        """完整配置（HMM + blacklist + stock_pool + label_type + unfilled）：新旧完全一致"""
        hmm_path = HMM_SNAPSHOT["model_path"]
        unified_cp, _ = self._get_unified_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)
        legacy_cp, _ = self._get_legacy_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)
        assert unified_cp == legacy_cp, (
            f"新旧路径 custom_params 不一致:\n"
            f"  仅新路径有: {dict(set(unified_cp.items()) - set(legacy_cp.items()))}\n"
            f"  仅旧路径有: {dict(set(legacy_cp.items()) - set(unified_cp.items()))}"
        )

    def test_full_config_strategy_params_identical(self):
        """完整配置：新旧 strategy_params 完全一致"""
        hmm_path = HMM_SNAPSHOT["model_path"]
        _, unified_sp = self._get_unified_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)
        _, legacy_sp = self._get_legacy_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)
        assert unified_sp == legacy_sp

    def test_initial_cash_absent_in_both(self):
        """initial_cash 在新旧路径的 custom_params 中都不存在"""
        loop = {**CUSTOM_EVO_LOOP_MINIMAL, "strategy_params": {"topk": 50, "initial_cash": 5_000_000}}
        unified_cp, _ = self._get_unified_params(loop, CUSTOM_EVO_TASK)
        legacy_cp, _ = self._get_legacy_params(loop, CUSTOM_EVO_TASK)
        assert "initial_cash" not in unified_cp
        assert "initial_cash" not in legacy_cp

    def test_hmm_keys_present_in_both(self):
        """HMM 启用时，新旧路径都注入相同的 HMM 键"""
        hmm_path = HMM_SNAPSHOT["model_path"]
        unified_cp, _ = self._get_unified_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)
        legacy_cp, _ = self._get_legacy_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)

        for key in ("enable_sector_hmm", "hmm_model_version_id", "sector_hmm_model_path", "hmm_signal_preset"):
            assert unified_cp.get(key) == legacy_cp.get(key), f"HMM 键 {key!r} 不一致"

    def test_unfilled_handler_flattened_in_both(self):
        """unfilled_handler_params 在新旧路径都被展开为顶层键"""
        hmm_path = HMM_SNAPSHOT["model_path"]
        unified_cp, _ = self._get_unified_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)
        legacy_cp, _ = self._get_legacy_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)

        assert unified_cp["unfilled_trigger_minute"] == legacy_cp["unfilled_trigger_minute"]
        assert unified_cp["unfilled_backup_depth"] == legacy_cp["unfilled_backup_depth"]

    def test_compose_call_params_match_legacy(self):
        """BacktestExecutor 传给 compose_experiment_in_memory 的参数与旧路径一致"""
        hmm_path = HMM_SNAPSHOT["model_path"]
        legacy_cp, legacy_sp = self._get_legacy_params(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK, hmm_path)

        # 新路径通过 BacktestExecutor
        composer = MagicMock()
        composer.compose_experiment_in_memory.return_value = {
            "experiment_files": {}, "wsl_command": "python qrun_limit_minute.py conf.yaml"
        }
        client = AsyncMock()
        client.create_and_run_loop.return_value = "Loop1"
        executor = BacktestExecutor(composer, client)

        with patch(
            "services.quantevolver.experiment_config_builders._resolve_hmm_snapshot",
            return_value=hmm_path,
        ):
            cfg = build_config_from_custom_evo_loop(CUSTOM_EVO_LOOP_FULL, CUSTOM_EVO_TASK,
                                                     experiment_name="task/Loop1")

        ctx = ExecutionContext(task_id="task", loop_index=1, experiment_name="task/Loop1")
        asyncio.get_event_loop().run_until_complete(executor.submit(cfg, ctx))

        compose_kwargs = composer.compose_experiment_in_memory.call_args.kwargs
        assert compose_kwargs["custom_params"] == legacy_cp, (
            f"compose custom_params 不一致:\n"
            f"  仅新路径: {dict(set(compose_kwargs['custom_params'].items()) - set(legacy_cp.items()))}\n"
            f"  仅旧路径: {dict(set(legacy_cp.items()) - set(compose_kwargs['custom_params'].items()))}"
        )
        assert compose_kwargs["strategy_params"] == legacy_sp
        assert compose_kwargs["execution_algo"] == "twap"
        assert compose_kwargs["factor_names"] == ["Alpha001", "mf_rsi_14d"]
