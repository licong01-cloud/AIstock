"""
Level 1/2 测试 — 分析层

Level 1: MetricsNormalizer 纯逻辑测试（无外部依赖）
Level 2: BacktestResultAnalyzer 集成测试（Mock DB + Mock RDAgent）
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, call

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from services.quantevolver.analysis.metrics_normalizer import (
    normalize_metrics,
    METRIC_ALIASES,
)
from services.quantevolver.analysis.backtest_analyzer import (
    BacktestResultAnalyzer,
    BacktestResult,
)
from services.quantevolver.analysis.metrics_store import MetricsStore
from services.quantevolver.experiment_config import ExperimentConfig


# ── 测试数据 ───────────────────────────────────────────────────────────────────

RAW_METRICS_FULL = {
    "Rank IC": 0.045,
    "IC": 0.038,
    "1day.excess_return_with_cost.information_ratio": 1.23,
    "1day.excess_return_with_cost.annualized_return": 0.15,
    "1day.excess_return_without_cost.annualized_return": 0.18,
    "1day.excess_return_with_cost.max_drawdown": -0.08,
    "1day.excess_return_without_cost.max_drawdown": -0.06,
    "1day.excess_return_without_cost.information_ratio": 1.45,
    "1day.excess_return_with_cost.mean": 0.0006,
    "1day.excess_return_without_cost.mean": 0.0007,
}

MOCK_ENHANCED = {
    "ic_diagnostics": {"dates": ["2024-01-01"], "ic_series": [0.04]},
    "return_curves": {"return_dates": ["2024-01-01"], "cumulative_excess_no_cost": [0.01]},
    "training_diagnostics": {"train_loss_curve": [0.5, 0.3], "val_loss_curve": [0.6, 0.4]},
    "summary": {"ic": 0.038, "sharpe": 1.23},
}

MINIMAL_CONFIG = ExperimentConfig(
    factor_names=["Alpha001", "Alpha002"],
    model_id="model_lgbm_v1",
    strategy_id="TopkDropoutStrategy",
)


# ── Level 1: MetricsNormalizer ─────────────────────────────────────────────────

class TestNormalizeMetrics:
    def test_all_aliases_mapped(self):
        result = normalize_metrics(RAW_METRICS_FULL)
        assert result["Rank_IC"] == 0.045
        assert result["sharpe"] == 1.23
        assert result["annualized_return"] == 0.15
        assert result["annualized_return_no_cost"] == 0.18
        assert result["max_drawdown"] == -0.08
        assert result["max_drawdown_no_cost"] == -0.06
        assert result["sharpe_no_cost"] == 1.45
        assert result["daily_return"] == 0.0006
        assert result["daily_return_no_cost"] == 0.0007

    def test_original_keys_preserved(self):
        """原始长键必须保留，不能删除"""
        result = normalize_metrics(RAW_METRICS_FULL)
        for raw_key in RAW_METRICS_FULL:
            assert raw_key in result, f"原始键 {raw_key!r} 不应被删除"

    def test_short_key_not_overwritten_if_exists(self):
        """如果短键已存在，不应被覆盖"""
        metrics = {
            "1day.excess_return_with_cost.information_ratio": 1.23,
            "sharpe": 9.99,  # 已存在
        }
        result = normalize_metrics(metrics)
        assert result["sharpe"] == 9.99  # 保持原值

    def test_empty_metrics(self):
        assert normalize_metrics({}) == {}

    def test_unknown_keys_passed_through(self):
        metrics = {"custom_metric": 42.0}
        result = normalize_metrics(metrics)
        assert result["custom_metric"] == 42.0

    def test_partial_metrics(self):
        metrics = {"Rank IC": 0.05, "IC": 0.04}
        result = normalize_metrics(metrics)
        assert result["Rank_IC"] == 0.05
        assert result["IC"] == 0.04
        assert "sharpe" not in result

    def test_matches_existing_code_behavior(self):
        """验证与现有代码 qe_evolution_service.py:1212-1225 行为完全一致"""
        # 模拟现有代码逻辑
        metrics_legacy = dict(RAW_METRICS_FULL)
        _METRIC_ALIASES_LEGACY = {
            "Rank IC": "Rank_IC",
            "1day.excess_return_with_cost.information_ratio": "sharpe",
            "1day.excess_return_with_cost.annualized_return": "annualized_return",
            "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
            "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
            "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
            "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
            "1day.excess_return_with_cost.mean": "daily_return",
            "1day.excess_return_without_cost.mean": "daily_return_no_cost",
        }
        for src, dst in _METRIC_ALIASES_LEGACY.items():
            if src in metrics_legacy and dst not in metrics_legacy:
                metrics_legacy[dst] = metrics_legacy[src]

        # 新代码
        metrics_new = normalize_metrics(RAW_METRICS_FULL)

        # 所有短键必须完全一致
        for dst in _METRIC_ALIASES_LEGACY.values():
            assert metrics_new.get(dst) == metrics_legacy.get(dst), \
                f"短键 {dst!r} 不一致: new={metrics_new.get(dst)}, legacy={metrics_legacy.get(dst)}"


# ── Level 2: BacktestResultAnalyzer ───────────────────────────────────────────

def make_mock_client(
    metrics: dict = None,
    enhanced: dict = None,
) -> AsyncMock:
    client = AsyncMock()
    client.get_loop_metrics.return_value = metrics or RAW_METRICS_FULL
    client.get_enhanced_metrics.return_value = enhanced or MOCK_ENHANCED
    return client


def make_mock_store() -> MagicMock:
    store = MagicMock(spec=MetricsStore)
    store.save_experiment_record.return_value = "task_001_L1"
    return store


class TestBacktestResultAnalyzer:
    def test_analyze_returns_normalized_metrics(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        result = asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1)
        )

        assert isinstance(result, BacktestResult)
        assert result.metrics["sharpe"] == 1.23
        assert result.metrics["annualized_return"] == 0.15
        assert result.metrics["Rank_IC"] == 0.045

    def test_analyze_fetches_enhanced_when_requested(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        result = asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1, fetch_enhanced=True)
        )

        client.get_enhanced_metrics.assert_called_once_with("task_001", "Loop1")
        assert result.enhanced == MOCK_ENHANCED

    def test_analyze_skips_enhanced_when_not_requested(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        result = asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1, fetch_enhanced=False)
        )

        client.get_enhanced_metrics.assert_not_called()
        assert result.enhanced is None

    def test_analyze_saves_to_db_by_default(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        result = asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1)
        )

        store.save_experiment_record.assert_called_once()
        store.save_loop_metrics.assert_called_once()
        assert result.experiment_id == "task_001_L1"

    def test_analyze_skips_db_when_save_to_db_false(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        result = asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1, save_to_db=False)
        )

        store.save_experiment_record.assert_not_called()
        store.save_loop_metrics.assert_not_called()
        assert result.experiment_id is None

    def test_analyze_saves_sota_registry_when_is_sota(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1, is_sota=True)
        )

        store.save_sota_registry.assert_called_once_with("task_001_Loop1")

    def test_analyze_does_not_save_sota_registry_when_not_sota(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=1, is_sota=False)
        )

        store.save_sota_registry.assert_not_called()

    def test_save_experiment_record_called_with_correct_args(self):
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop1", MINIMAL_CONFIG, loop_index=3, is_sota=True)
        )

        call_kwargs = store.save_experiment_record.call_args.kwargs
        assert call_kwargs["task_id"] == "task_001"
        assert call_kwargs["loop_index"] == 3
        assert call_kwargs["config"] is MINIMAL_CONFIG
        assert call_kwargs["is_sota"] is True

    def test_loop_id_format_passed_to_client(self):
        """client.get_loop_metrics 应收到 RDAgent 格式的 loop_id（如 'Loop1'）"""
        client = make_mock_client()
        store = make_mock_store()
        analyzer = BacktestResultAnalyzer(client, store)

        asyncio.get_event_loop().run_until_complete(
            analyzer.analyze("task_001", "Loop3", MINIMAL_CONFIG, loop_index=3)
        )

        client.get_loop_metrics.assert_called_once_with("task_001", "Loop3")
