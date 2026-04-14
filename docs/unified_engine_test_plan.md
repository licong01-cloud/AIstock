# QE 统一引擎 — 测试验证方案

> 版本: v1.0 | 日期: 2026-04-09 | 配套文档: unified_engine_design.md

---

## 1. 测试目标

在统一引擎正式上线前，通过后台脚本（不依赖前端 UI）完成全流程验证：

1. **配置层正确性** — ExperimentConfig 产出的 custom_params 与现有 4 条路径完全一致
2. **执行层正确性** — BacktestExecutor 传给 ConfigComposer 和 QEWorkspaceClient 的参数与现有路径一致
3. **分析层正确性** — BacktestResultAnalyzer 产出的 metrics 和 DB 记录与现有路径一致
4. **端到端正确性** — 统一引擎提交的回测任务能正常完成，结果与旧路径一致
5. **参数组合覆盖** — 所有配置参数的排列组合都被验证

---

## 2. 测试分层

```
┌─────────────────────────────────────────────┐
│  Level 4: 端到端对比测试 (E2E A/B)           │  ← 真实回测，新旧对比
├─────────────────────────────────────────────┤
│  Level 3: API 集成测试                       │  ← 调用真实 API，验证全链路
├─────────────────────────────────────────────┤
│  Level 2: 组件集成测试                       │  ← 真实 DB + Mock RDAgent
├─────────────────────────────────────────────┤
│  Level 1: 单元测试                           │  ← 纯逻辑，无外部依赖
└─────────────────────────────────────────────┘
```

---

## 3. 测试基础设施

### 3.1 目录结构

```
backend/tests/
├── conftest.py                          # pytest fixtures (DB, mock clients)
├── unified_engine/
│   ├── __init__.py
│   ├── test_experiment_config.py        # Level 1: 配置层单元测试
│   ├── test_config_builders.py          # Level 1: builder 函数测试
│   ├── test_backtest_executor.py        # Level 2: 执行层集成测试
│   ├── test_metrics_normalizer.py       # Level 1: 指标归一化测试
│   ├── test_metrics_store.py            # Level 2: DB 写入测试
│   ├── test_backtest_analyzer.py        # Level 2: 分析层集成测试
│   ├── test_ab_comparison.py            # Level 3: 新旧路径参数对比
│   └── test_e2e_backtest.py             # Level 4: 端到端回测对比
├── fixtures/
│   ├── sample_configs.py                # 测试用配置数据
│   ├── sample_metrics.py                # 测试用指标数据
│   └── mock_rdagent.py                  # RDAgent Mock 服务
└── scripts/
    ├── run_all_tests.sh                 # 一键运行全部测试
    ├── run_ab_comparison.py             # A/B 对比脚本（独立运行）
    └── run_e2e_validation.py            # 端到端验证脚本（独立运行）
```

### 3.2 conftest.py — 核心 Fixtures

```python
# backend/tests/conftest.py

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_composer():
    """Mock ConfigComposer — 记录调用参数但不执行真实组合"""
    composer = MagicMock()
    composer.compose_experiment_in_memory.return_value = {
        "experiment_files": {"conf.yaml": "mock_content"},
        "wsl_command": "cd /mnt/f/... && python qrun_limit_minute.py conf.yaml",
    }
    return composer

@pytest.fixture
def mock_workspace_client():
    """Mock QEWorkspaceClient — 记录调用参数但不提交到 RDAgent"""
    client = AsyncMock()
    client.create_and_run_loop.return_value = "mock_job_id"
    client.get_loop_metrics.return_value = {
        "Rank IC": 0.045,
        "IC": 0.038,
        "1day.excess_return_with_cost.information_ratio": 1.23,
        "1day.excess_return_with_cost.annualized_return": 0.15,
        "1day.excess_return_with_cost.max_drawdown": -0.08,
    }
    client.get_enhanced_metrics.return_value = {
        "ic_diagnostics": {"dates": ["2024-01-01"], "ic_series": [0.04]},
        "return_curves": {"return_dates": ["2024-01-01"], "cumulative_excess_no_cost": [0.01]},
        "training_diagnostics": {"train_loss_curve": [0.5, 0.3], "val_loss_curve": [0.6, 0.4]},
        "summary": {"ic": 0.038, "sharpe": 1.23},
    }
    return client

@pytest.fixture
def mock_hmm_service():
    """Mock HMMTrainingService"""
    svc = MagicMock()
    svc.get_snapshot.return_value = {
        "snapshot_id": "snap_test_001",
        "config_id": "cfg_test",
        "model_path": "/mnt/f/Dev/AIstock/data/hmm_models/cfg_test/20260401/models.json",
        "status": "completed",
        "sector_count": 31,
    }
    return svc
```

### 3.3 测试数据 — sample_configs.py

```python
# backend/tests/fixtures/sample_configs.py

"""
覆盖所有参数组合的测试配置矩阵。
每个配置代表一种真实使用场景。
"""

# 最小配置 — 仅必填字段
MINIMAL_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
}

# 完整配置 — 所有字段都填
FULL_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "mf_rsi_14d", "mf_volume_ratio"],
    "model_id": "LGBModel",
    "strategy_id": "topk_dropout",
    "data_split": {
        "train_start": "2018-01-01", "train_end": "2022-12-31",
        "valid_start": "2023-01-01", "valid_end": "2023-06-30",
        "test_start": "2023-07-01", "test_end": "2024-06-30",
    },
    "label_type": "Ref($close, -2)/Ref($close, -1) - 1",
    "hmm": {
        "enable_sector_hmm": True,
        "hmm_model_version_id": "snap_test_001",
        "sector_hmm_model_path": "/mnt/f/.../models.json",
        "hmm_signal_preset": "preset_A",
    },
    "stock_pool": "/mnt/f/.../filtered_pool_sw2.txt",
    "sector_blacklist": ["801780", "801170"],
    "execution_algo": "VWAP_TWAP_HYBRID",
    "execution_algo_params": {"vwap_ratio": 0.6, "twap_ratio": 0.4},
    "unfilled_handler": "TAIL_SUBSTITUTE",
    "unfilled_handler_params": {"unfilled_backup_depth": 15, "unfilled_trigger_minute": 55},
    "strategy_params": {"topk": 50, "n_drop": 5, "hold_thresh": 2, "risk_degree": 0.95},
    "extra_params": {},
}

# HMM 启用配置
HMM_ENABLED_CONFIG = {
    **MINIMAL_CONFIG,
    "hmm": {
        "enable_sector_hmm": True,
        "hmm_model_version_id": "snap_test_001",
        "sector_hmm_model_path": "/mnt/f/.../models.json",
        "hmm_signal_preset": "preset_A",
    },
}

# HMM 禁用配置
HMM_DISABLED_CONFIG = {
    **MINIMAL_CONFIG,
    "hmm": None,
}

# 行业黑名单配置
BLACKLIST_CONFIG = {
    **MINIMAL_CONFIG,
    "stock_pool": "/mnt/f/.../filtered_pool.txt",
    "sector_blacklist": ["801780", "801170", "801050"],
}

# 尾盘处理 — TAIL_BOOST
TAIL_BOOST_CONFIG = {
    **MINIMAL_CONFIG,
    "execution_algo": "VWAP_TWAP_HYBRID",
    "unfilled_handler": "TAIL_BOOST",
    "unfilled_handler_params": {"unfilled_trigger_minute": 55},
}

# 尾盘处理 — TAIL_SUBSTITUTE
TAIL_SUBSTITUTE_CONFIG = {
    **MINIMAL_CONFIG,
    "execution_algo": "VWAP_TWAP_HYBRID",
    "unfilled_handler": "TAIL_SUBSTITUTE",
    "unfilled_handler_params": {"unfilled_backup_depth": 15, "unfilled_trigger_minute": 55},
}

# 自定义 data_split
CUSTOM_SPLIT_CONFIG = {
    **MINIMAL_CONFIG,
    "data_split": {
        "train_start": "2020-01-01", "train_end": "2024-06-30",
        "valid_start": "2024-07-01", "valid_end": "2024-09-30",
        "test_start": "2024-10-01", "test_end": "2025-03-31",
    },
}

# 参数组合矩阵 — 用于自动化遍历
CONFIG_MATRIX = [
    MINIMAL_CONFIG,
    FULL_CONFIG,
    HMM_ENABLED_CONFIG,
    HMM_DISABLED_CONFIG,
    BLACKLIST_CONFIG,
    TAIL_BOOST_CONFIG,
    TAIL_SUBSTITUTE_CONFIG,
    CUSTOM_SPLIT_CONFIG,
]
```

### 3.4 完整参数维度清单

当前系统支持的全部配置维度：

| 维度 | 可选值 | 影响范围 |
|------|--------|---------|
| backtest_freq | `1min` (默认), `day` | 数据源、执行脚本、执行策略 |
| model_type | LGBModel, XGBModel, CatBoostModel, LinearModel, GeneralPTNN | 训练配置、模型文件 |
| quick_train | true, false | 训练轮次/batch_size |
| strategy_id | None (默认TopkDropout), 自定义策略 | custom_strategy.py 生成 |
| execution_algo | None, CLOSE_PRICE, TWAP, VWAP, SBB_EMA, AC_OPTIMAL, POV | 分钟线执行方式 |
| unfilled_handler | None, TAIL_BOOST, TAIL_SUBSTITUTE | 尾盘处理 |
| hmm | None, enabled+preset_A, enabled+preset_B | HMM 行业热度 |
| stock_pool | None, 自定义路径 | 股票池过滤 |
| sector_blacklist | None, SW2 代码列表 | 行业黑名单 |
| label_type | None (默认), 自定义表达式 | 标签计算 |
| data_split | None (默认), 自定义日期 | 训练/测试划分 |

### 3.5 扩展测试配置 — 覆盖所有维度

```python
# tests/fixtures/sample_configs.py (续)

# ── 回测频率维度 ──

DAILY_FREQ_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
    "extra_params": {"backtest_freq": "day"},
}

MINUTE_FREQ_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
    # backtest_freq 默认 1min，不需要显式设置
}

# ── 模型类型维度 ──

LGB_MODEL_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
}

XGB_MODEL_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "XGBModel",
}

CATBOOST_MODEL_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "CatBoostModel",
}

LINEAR_MODEL_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LinearModel",
}

# ── 快速训练维度 ──

QUICK_TRAIN_CONFIG = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
    "extra_params": {"quick_train": True},
}

# ── 执行算法维度 ──

CLOSE_PRICE_CONFIG = {
    **MINIMAL_CONFIG,
    "execution_algo": "CLOSE_PRICE",
    "extra_params": {"backtest_freq": "day"},  # CLOSE_PRICE 仅支持日频
}

TWAP_CONFIG = {
    **MINIMAL_CONFIG,
    "execution_algo": "TWAP",
}

VWAP_CONFIG = {
    **MINIMAL_CONFIG,
    "execution_algo": "VWAP",
}

SBB_EMA_CONFIG = {
    **MINIMAL_CONFIG,
    "execution_algo": "SBB_EMA",
}

# ── 策略维度 ──

DEFAULT_STRATEGY_CONFIG = {
    **MINIMAL_CONFIG,
    "strategy_id": None,  # 使用默认 TopkDropoutStrategy
}

CUSTOM_STRATEGY_CONFIG = {
    **MINIMAL_CONFIG,
    "strategy_id": "topk_dropout",  # 触发 custom_strategy.py 生成
    "strategy_params": {"topk": 30, "n_drop": 3, "hold_thresh": 2, "risk_degree": 0.95},
}

# ── 组合场景 ──

# 日频 + CLOSE_PRICE + 无尾盘处理（最简日频场景）
DAILY_SIMPLE = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
    "execution_algo": "CLOSE_PRICE",
    "extra_params": {"backtest_freq": "day"},
}

# 分钟线 + TWAP + TAIL_BOOST + HMM（完整分钟线场景）
MINUTE_FULL = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1", "mf_rsi_14d"],
    "model_id": "LGBModel",
    "strategy_id": "topk_dropout",
    "execution_algo": "TWAP",
    "unfilled_handler": "TAIL_BOOST",
    "unfilled_handler_params": {"unfilled_trigger_minute": 55},
    "hmm": {
        "enable_sector_hmm": True,
        "hmm_model_version_id": "snap_test_001",
        "sector_hmm_model_path": "/mnt/f/.../models.json",
        "hmm_signal_preset": "preset_A",
    },
    "strategy_params": {"topk": 50, "n_drop": 5, "hold_thresh": 2, "risk_degree": 0.95},
}

# 分钟线 + VWAP + TAIL_SUBSTITUTE + 行业黑名单
MINUTE_BLACKLIST = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "LGBModel",
    "execution_algo": "VWAP",
    "unfilled_handler": "TAIL_SUBSTITUTE",
    "unfilled_handler_params": {"unfilled_backup_depth": 15, "unfilled_trigger_minute": 55},
    "stock_pool": "/mnt/f/.../filtered_pool.txt",
    "sector_blacklist": ["801780", "801170"],
    "strategy_params": {"topk": 30, "n_drop": 3},
}

# XGB + 快速训练 + 自定义 data_split
XGB_QUICK_CUSTOM_SPLIT = {
    "factor_names": ["Alpha158_feature_0", "Alpha158_feature_1"],
    "model_id": "XGBModel",
    "data_split": {
        "train_start": "2020-01-01", "train_end": "2024-06-30",
        "valid_start": "2024-07-01", "valid_end": "2024-09-30",
        "test_start": "2024-10-01", "test_end": "2025-03-31",
    },
    "extra_params": {"quick_train": True},
}

# ── 完整测试矩阵 ──

CONFIG_MATRIX_FULL = [
    # 基础
    MINIMAL_CONFIG,
    FULL_CONFIG,
    # 回测频率
    DAILY_FREQ_CONFIG,
    MINUTE_FREQ_CONFIG,
    # 模型类型
    LGB_MODEL_CONFIG,
    XGB_MODEL_CONFIG,
    CATBOOST_MODEL_CONFIG,
    LINEAR_MODEL_CONFIG,
    # 快速训练
    QUICK_TRAIN_CONFIG,
    # HMM
    HMM_ENABLED_CONFIG,
    HMM_DISABLED_CONFIG,
    # 行业黑名单
    BLACKLIST_CONFIG,
    # 执行算法
    CLOSE_PRICE_CONFIG,
    TWAP_CONFIG,
    VWAP_CONFIG,
    SBB_EMA_CONFIG,
    # 尾盘处理
    TAIL_BOOST_CONFIG,
    TAIL_SUBSTITUTE_CONFIG,
    # 策略
    DEFAULT_STRATEGY_CONFIG,
    CUSTOM_STRATEGY_CONFIG,
    # 组合场景
    DAILY_SIMPLE,
    MINUTE_FULL,
    MINUTE_BLACKLIST,
    XGB_QUICK_CUSTOM_SPLIT,
    CUSTOM_SPLIT_CONFIG,
]
```

### 3.6 E2E 端到端测试配置（小数据量，真实回测）

```python
# tests/fixtures/e2e_configs.py
# 使用 quick_train + 短时间窗口，单次回测约 2-5 分钟

E2E_BASE = {
    "factor_keys": ["Alpha158_feature_0||rdagent", "Alpha158_feature_1||rdagent"],
    "strategy_params": {"topk": 30, "n_drop": 3, "hold_thresh": 2, "risk_degree": 0.95},
}

E2E_SCENARIOS = {
    # ── 回测频率 ──
    "minute_lgb": {
        **E2E_BASE,
        "model_id": "LGBModel",
        # 默认 1min
    },
    "daily_lgb": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "backtest_freq": "day",
        "execution_algo": "CLOSE_PRICE",
    },

    # ── 模型类型 ──
    "minute_xgb": {
        **E2E_BASE,
        "model_id": "XGBModel",
    },
    "minute_linear": {
        **E2E_BASE,
        "model_id": "LinearModel",
    },

    # ── 执行算法 ──
    "minute_twap": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "execution_algo": "TWAP",
    },
    "minute_vwap": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "execution_algo": "VWAP",
    },

    # ── 尾盘处理 ──
    "minute_tail_boost": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "execution_algo": "TWAP",
        "unfilled_handler": "TAIL_BOOST",
        "unfilled_handler_params": {"trigger_minute": 55},
    },
    "minute_tail_substitute": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "execution_algo": "TWAP",
        "unfilled_handler": "TAIL_SUBSTITUTE",
        "unfilled_handler_params": {"backup_depth": 15, "trigger_minute": 55},
    },

    # ── HMM ──
    "minute_hmm": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "enable_sector_hmm": True,
        "hmm_model_version_id": "",  # 运行时填入
        "hmm_signal_preset": "preset_A",
    },

    # ── 行业黑名单 ──
    "minute_blacklist": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "stock_pool": "",  # 运行时生成
        "sector_blacklist": ["801780", "801170"],
    },

    # ── 组合场景：分钟线全功能 ──
    "minute_full_combo": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "execution_algo": "TWAP",
        "unfilled_handler": "TAIL_BOOST",
        "unfilled_handler_params": {"trigger_minute": 55},
        "enable_sector_hmm": True,
        "hmm_model_version_id": "",
        "hmm_signal_preset": "preset_A",
    },

    # ── 组合场景：日频简单 ──
    "daily_simple": {
        **E2E_BASE,
        "model_id": "LGBModel",
        "backtest_freq": "day",
        "execution_algo": "CLOSE_PRICE",
    },
}

# 模型训练验证场景（验证训练产出）
E2E_TRAINING_SCENARIOS = {
    "train_lgb": {"model_id": "LGBModel", "expect_model_type": "lgb"},
    "train_xgb": {"model_id": "XGBModel", "expect_model_type": "xgb"},
    "train_linear": {"model_id": "LinearModel", "expect_model_type": "linear"},
    "train_quick": {"model_id": "LGBModel", "quick_train": True, "expect_faster": True},
}
```

---

## 4. Level 1: 单元测试

### 4.1 test_experiment_config.py — 配置层核心测试

```python
import pytest
from services.quantevolver.experiment_config import ExperimentConfig, HmmConfig
from tests.fixtures.sample_configs import CONFIG_MATRIX, MINIMAL_CONFIG, FULL_CONFIG

class TestExperimentConfigValidation:
    """校验逻辑测试"""

    def test_minimal_config_valid(self):
        config = ExperimentConfig(**MINIMAL_CONFIG)
        assert config.factor_names == MINIMAL_CONFIG["factor_names"]
        assert config.model_id == MINIMAL_CONFIG["model_id"]

    def test_empty_factors_rejected(self):
        with pytest.raises(ValueError, match="factor_names"):
            ExperimentConfig(factor_names=[], model_id="LGBModel")

    def test_hmm_enabled_without_version_rejected(self):
        with pytest.raises(ValueError, match="hmm_model_version_id"):
            ExperimentConfig(
                factor_names=["f1"], model_id="LGBModel",
                hmm=HmmConfig(enable_sector_hmm=True, hmm_model_version_id=None),
            )

    def test_hmm_disabled_no_validation(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            hmm=HmmConfig(enable_sector_hmm=False),
        )
        assert config.hmm.enable_sector_hmm is False


class TestBuildCustomParams:
    """build_custom_params() 输出测试"""

    def test_minimal_returns_empty(self):
        config = ExperimentConfig(**MINIMAL_CONFIG)
        params = config.build_custom_params()
        assert params == {}

    def test_hmm_injection(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            hmm=HmmConfig(
                enable_sector_hmm=True,
                hmm_model_version_id="snap_001",
                sector_hmm_model_path="/mnt/f/.../models.json",
                hmm_signal_preset="preset_A",
            ),
        )
        params = config.build_custom_params()
        assert params["enable_sector_hmm"] is True
        assert params["sector_hmm_model_path"] == "/mnt/f/.../models.json"
        assert params["hmm_signal_preset"] == "preset_A"

    def test_stock_pool_injection(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            stock_pool="/mnt/f/.../pool.txt",
        )
        params = config.build_custom_params()
        assert params["stock_pool"] == "/mnt/f/.../pool.txt"

    def test_sector_blacklist_injection(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            sector_blacklist=["801780", "801170"],
        )
        params = config.build_custom_params()
        assert params["sector_blacklist"] == ["801780", "801170"]

    def test_unfilled_handler_injection(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            unfilled_handler="TAIL_SUBSTITUTE",
            unfilled_handler_params={"unfilled_backup_depth": 15, "unfilled_trigger_minute": 55},
        )
        params = config.build_custom_params()
        assert params["unfilled_handler"] == "TAIL_SUBSTITUTE"
        assert params["unfilled_backup_depth"] == 15
        assert params["unfilled_trigger_minute"] == 55

    def test_label_type_injection(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            label_type="Ref($close, -2)/Ref($close, -1) - 1",
        )
        params = config.build_custom_params()
        assert params["label_type"] == "Ref($close, -2)/Ref($close, -1) - 1"

    def test_extra_params_merge(self):
        config = ExperimentConfig(
            factor_names=["f1"], model_id="LGBModel",
            extra_params={"multi_alpha": {"mode": "ensemble"}, "custom_key": 42},
        )
        params = config.build_custom_params()
        assert params["multi_alpha"] == {"mode": "ensemble"}
        assert params["custom_key"] == 42

    def test_full_config_all_params(self):
        """完整配置 — 所有参数都应出现在 custom_params 中"""
        config = ExperimentConfig(**FULL_CONFIG)
        params = config.build_custom_params()
        assert "enable_sector_hmm" in params
        assert "stock_pool" in params
        assert "sector_blacklist" in params
        assert "unfilled_handler" in params
        assert "label_type" in params

    @pytest.mark.parametrize("config_data", CONFIG_MATRIX)
    def test_all_matrix_configs_valid(self, config_data):
        """参数矩阵中所有配置都应能成功构造"""
        config = ExperimentConfig(**config_data)
        params = config.build_custom_params()
        assert isinstance(params, dict)

    def test_json_serialization_roundtrip(self):
        """JSON 序列化/反序列化一致性"""
        config = ExperimentConfig(**FULL_CONFIG)
        json_str = config.model_dump_json()
        restored = ExperimentConfig.model_validate_json(json_str)
        assert config.build_custom_params() == restored.build_custom_params()
```

### 4.2 test_metrics_normalizer.py — 指标归一化测试

```python
from services.quantevolver.analysis.metrics_normalizer import normalize_metrics, METRIC_ALIASES

class TestMetricsNormalizer:

    def test_known_keys_renamed(self):
        raw = {
            "Rank IC": 0.045,
            "1day.excess_return_with_cost.information_ratio": 1.23,
            "1day.excess_return_with_cost.annualized_return": 0.15,
        }
        result = normalize_metrics(raw)
        assert result["Rank_IC"] == 0.045
        assert result["sharpe"] == 1.23
        assert result["annualized_return"] == 0.15

    def test_unknown_keys_passthrough(self):
        raw = {"custom_metric": 0.99}
        result = normalize_metrics(raw)
        assert result["custom_metric"] == 0.99

    def test_empty_input(self):
        assert normalize_metrics({}) == {}

    def test_all_aliases_covered(self):
        """确保 METRIC_ALIASES 覆盖所有已知的 QLib 长键"""
        expected_short_keys = {
            "Rank_IC", "IC", "sharpe", "annualized_return",
            "annualized_return_no_cost", "max_drawdown",
            "max_drawdown_no_cost", "sharpe_no_cost",
        }
        assert set(METRIC_ALIASES.values()) == expected_short_keys
```

---

## 5. Level 2: 组件集成测试

### 5.1 test_backtest_executor.py — 执行层测试

```python
import pytest
from services.quantevolver.experiment_config import ExperimentConfig
from services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
from services.quantevolver.executors.base import ExecutionContext
from tests.fixtures.sample_configs import CONFIG_MATRIX, FULL_CONFIG

class TestBacktestExecutor:

    @pytest.fixture
    def executor(self, mock_composer, mock_workspace_client):
        return BacktestExecutor(composer=mock_composer, client=mock_workspace_client)

    @pytest.fixture
    def ctx(self):
        return ExecutionContext(
            task_id="test_task_001", loop_index=1,
            experiment_name="test_task_001/Loop1",
        )

    @pytest.mark.asyncio
    async def test_full_train_submit(self, executor, ctx, mock_composer):
        config = ExperimentConfig(**FULL_CONFIG)
        result = await executor.submit(config, ctx)
        assert result.status == "submitted"
        assert result.job_id == "mock_job_id"
        # 验证 compose_experiment_in_memory 被正确调用
        call_args = mock_composer.compose_experiment_in_memory.call_args
        assert call_args.kwargs["factor_names"] == FULL_CONFIG["factor_names"]
        assert call_args.kwargs["model_id"] == FULL_CONFIG["model_id"]
        assert call_args.kwargs["skip_db_save"] is True

    @pytest.mark.asyncio
    async def test_backtest_only_requires_model_source(self, executor, ctx):
        config = ExperimentConfig(**FULL_CONFIG)
        with pytest.raises(ValueError, match="model_source"):
            await executor.submit(config, ctx, mode=BacktestMode.BACKTEST_ONLY)

    @pytest.mark.asyncio
    async def test_backtest_only_injects_flag(self, executor, ctx, mock_composer, mock_workspace_client):
        mock_composer.compose_experiment_in_memory.return_value = {
            "experiment_files": {},
            "wsl_command": "cd /mnt/f/... && python qrun_limit_minute.py conf.yaml && python read_exp_res.py",
        }
        config = ExperimentConfig(**FULL_CONFIG)
        model_source = {"source_task_id": "src_task", "source_loop": 1}
        result = await executor.submit(config, ctx, mode=BacktestMode.BACKTEST_ONLY, model_source=model_source)
        # 验证 wsl_command 包含 --backtest-only
        submit_call = mock_workspace_client.create_and_run_loop.call_args
        assert "--backtest-only" in submit_call.kwargs["wsl_command"]
        assert submit_call.kwargs["model_source"] == model_source

    @pytest.mark.asyncio
    @pytest.mark.parametrize("config_data", CONFIG_MATRIX)
    async def test_all_matrix_configs_submit(self, executor, ctx, config_data):
        """参数矩阵中所有配置都应能成功提交"""
        config = ExperimentConfig(**config_data)
        result = await executor.submit(config, ctx)
        assert result.status == "submitted"

    @pytest.mark.asyncio
    async def test_custom_params_passed_correctly(self, executor, ctx, mock_composer):
        """验证 build_custom_params() 的输出被完整传递给 ConfigComposer"""
        config = ExperimentConfig(**FULL_CONFIG)
        expected_params = config.build_custom_params()
        await executor.submit(config, ctx)
        actual_params = mock_composer.compose_experiment_in_memory.call_args.kwargs["custom_params"]
        assert actual_params == expected_params
```

### 5.2 test_backtest_analyzer.py — 分析层测试

```python
import pytest
from services.quantevolver.analysis.backtest_analyzer import BacktestResultAnalyzer
from services.quantevolver.analysis.metrics_store import MetricsStore
from services.quantevolver.experiment_config import ExperimentConfig
from tests.fixtures.sample_configs import MINIMAL_CONFIG

class TestBacktestResultAnalyzer:

    @pytest.fixture
    def mock_store(self):
        store = MagicMock(spec=MetricsStore)
        store.save_experiment_record.return_value = "exp_test_001"
        return store

    @pytest.fixture
    def analyzer(self, mock_workspace_client, mock_store):
        return BacktestResultAnalyzer(
            workspace_client=mock_workspace_client,
            metrics_store=mock_store,
        )

    @pytest.mark.asyncio
    async def test_analyze_with_enhanced(self, analyzer, mock_workspace_client):
        config = ExperimentConfig(**MINIMAL_CONFIG)
        result = await analyzer.analyze("task_001", "loop_001", config, loop_index=1)
        assert "sharpe" in result.metrics  # 归一化后的键
        assert result.enhanced is not None
        mock_workspace_client.get_enhanced_metrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyze_without_enhanced(self, analyzer, mock_workspace_client):
        config = ExperimentConfig(**MINIMAL_CONFIG)
        result = await analyzer.analyze("task_001", "loop_001", config, loop_index=1, fetch_enhanced=False)
        assert result.enhanced is None
        mock_workspace_client.get_enhanced_metrics.assert_not_called()

    @pytest.mark.asyncio
    async def test_analyze_saves_to_db(self, analyzer, mock_store):
        config = ExperimentConfig(**MINIMAL_CONFIG)
        result = await analyzer.analyze("task_001", "loop_001", config, loop_index=1, save_to_db=True)
        assert result.experiment_id == "exp_test_001"
        mock_store.save_experiment_record.assert_called_once()
        mock_store.save_loop_metrics.assert_called_once()
        mock_store.save_factor_model_records.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyze_skip_db(self, analyzer, mock_store):
        config = ExperimentConfig(**MINIMAL_CONFIG)
        result = await analyzer.analyze("task_001", "loop_001", config, loop_index=1, save_to_db=False)
        assert result.experiment_id is None
        mock_store.save_experiment_record.assert_not_called()
```

---

## 6. Level 3: A/B 对比测试 — 新旧路径参数一致性

这是最关键的测试：用真实 DB 数据，分别走旧路径和新路径组装参数，对比输出是否完全一致。

### 6.1 test_ab_comparison.py

```python
"""
A/B 对比测试：新旧路径参数一致性验证。

运行方式: pytest tests/unified_engine/test_ab_comparison.py -v
前提: 需要连接真实 DB，且 DB 中有已完成的 QE 任务数据。
"""
import pytest
import json
from services.quantevolver.experiment_config_builders import (
    build_from_experiment_record,
    build_from_base_or_reviewer,
    build_from_loop_config,
)
from services.hmm_training_service import HMMTrainingService
from db.pg_pool import get_conn
import psycopg2.extras


def _load_legacy_custom_params_path1(experiment_id: str) -> dict:
    """复现 Path 1 (单次实验) 的旧路径参数组装逻辑，返回 custom_params"""
    # 从 routers/quantevolver.py:3255-3354 提取的逻辑
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM qe_experiments WHERE experiment_id = %s", (experiment_id,))
            exp = cur.fetchone()
    if not exp:
        pytest.skip(f"Experiment {experiment_id} not found")
    custom_params = json.loads(exp["custom_params"]) if exp["custom_params"] else {}
    return custom_params


def _load_legacy_custom_params_path4(task_id: str, loop_index: int) -> dict:
    """复现 Path 4 (自定义演进) 的旧路径参数组装逻辑"""
    # 从 qe_evolution_service.py:3351-3401 提取的逻辑
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT strategy_evo_config FROM qe_evolution_tasks WHERE task_id = %s
            """, (task_id,))
            task = cur.fetchone()
    if not task:
        pytest.skip(f"Task {task_id} not found")
    config = json.loads(task["strategy_evo_config"]) if isinstance(task["strategy_evo_config"], str) else task["strategy_evo_config"]
    loops = config.get("loops", [])
    loop_config = next((l for l in loops if l.get("loop_index") == loop_index), None)
    if not loop_config:
        pytest.skip(f"Loop {loop_index} not found in task {task_id}")
    # 复现旧路径的参数组装
    strategy_params = loop_config.get("strategy_params") or {}
    loop_custom_params = dict(strategy_params)
    if loop_config.get("enable_sector_hmm"):
        loop_custom_params["enable_sector_hmm"] = True
        loop_custom_params["hmm_model_version_id"] = loop_config.get("hmm_model_version_id")
    if loop_config.get("stock_pool"):
        loop_custom_params["stock_pool"] = loop_config["stock_pool"]
    if loop_config.get("sector_blacklist"):
        loop_custom_params["sector_blacklist"] = loop_config["sector_blacklist"]
    if loop_config.get("label_type"):
        loop_custom_params["label_type"] = loop_config["label_type"]
    uf = loop_config.get("unfilled_handler")
    if uf:
        loop_custom_params["unfilled_handler"] = uf
        uf_params = loop_config.get("unfilled_handler_params") or {}
        if uf_params.get("trigger_minute"):
            loop_custom_params["unfilled_trigger_minute"] = uf_params["trigger_minute"]
        if uf_params.get("backup_depth"):
            loop_custom_params["unfilled_backup_depth"] = uf_params["backup_depth"]
    return loop_custom_params


class TestABComparison:
    """新旧路径参数对比"""

    @pytest.fixture
    def hmm_svc(self):
        return HMMTrainingService()

    def _get_recent_experiments(self, limit=5) -> list:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT experiment_id FROM qe_experiments
                    WHERE status = 'completed' AND custom_params IS NOT NULL
                    ORDER BY created_at DESC LIMIT %s
                """, (limit,))
                return [r["experiment_id"] for r in cur.fetchall()]

    def _get_recent_custom_evo_tasks(self, limit=3) -> list:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT task_id FROM qe_evolution_tasks
                    WHERE task_type = 'custom_evo' AND status IN ('completed', 'running')
                    ORDER BY created_at DESC LIMIT %s
                """, (limit,))
                return [r["task_id"] for r in cur.fetchall()]

    def test_path1_params_match(self, hmm_svc):
        """Path 1: 单次实验 — 新旧路径 custom_params 一致"""
        for exp_id in self._get_recent_experiments():
            with get_conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT * FROM qe_experiments WHERE experiment_id = %s", (exp_id,))
                    exp = cur.fetchone()
            # 旧路径
            legacy_params = _load_legacy_custom_params_path1(exp_id)
            # 新路径
            config = build_from_experiment_record(dict(exp), hmm_svc)
            unified_params = config.build_custom_params()
            # 对比（忽略旧路径中不属于 custom_params 的字段）
            for key in unified_params:
                assert unified_params[key] == legacy_params.get(key), \
                    f"Mismatch on {key}: unified={unified_params[key]}, legacy={legacy_params.get(key)}"

    def test_path4_params_match(self, hmm_svc):
        """Path 4: 自定义演进 — 新旧路径 custom_params 一致"""
        for task_id in self._get_recent_custom_evo_tasks():
            legacy_params = _load_legacy_custom_params_path4(task_id, loop_index=1)
            with get_conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT strategy_evo_config FROM qe_evolution_tasks WHERE task_id = %s", (task_id,))
                    task = cur.fetchone()
            sev_config = json.loads(task["strategy_evo_config"]) if isinstance(task["strategy_evo_config"], str) else task["strategy_evo_config"]
            loop_cfg = next(l for l in sev_config["loops"] if l.get("loop_index") == 1)
            config = build_from_loop_config(loop_cfg, hmm_svc)
            unified_params = config.build_custom_params()
            for key in set(list(unified_params.keys()) + list(legacy_params.keys())):
                assert unified_params.get(key) == legacy_params.get(key), \
                    f"Task {task_id} mismatch on {key}: unified={unified_params.get(key)}, legacy={legacy_params.get(key)}"
```

---

## 7. Level 4: 端到端回测对比

### 7.1 scripts/run_e2e_validation.py

独立运行的端到端验证脚本，不依赖 pytest。通过 API 创建真实回测任务，等待完成后对比结果。

```python
"""
端到端验证脚本 — 通过 API 创建真实回测任务，对比新旧引擎结果。

用法:
    python scripts/run_e2e_validation.py --api http://127.0.0.1:8001/api/v1
    python scripts/run_e2e_validation.py --api http://127.0.0.1:8001/api/v1 --skip-legacy  # 只跑统一引擎
    python scripts/run_e2e_validation.py --api http://127.0.0.1:8001/api/v1 --path custom_evo  # 只验证自定义演进

前提:
    - AIstock 后端运行中
    - RDAgent 运行中
    - DB 中有可用的因子和模型
"""
import argparse
import json
import time
import sys
import httpx

API = "http://127.0.0.1:8001/api/v1"

# ── 小数据量测试配置 ──
# 使用短时间窗口 + 少量因子，单次回测约 3-5 分钟
TEST_CONFIGS = {
    "custom_evo_minimal": {
        "task_name": "[E2E Test] Custom Evo Minimal",
        "target_desc": "E2E validation - minimal config",
        "loops": [{
            "label": "E2E-minimal",
            "loop_index": 1,
            "factor_keys": ["Alpha158_feature_0||rdagent", "Alpha158_feature_1||rdagent"],
            "model_id": "LGBModel",
            "strategy_params": {"topk": 30, "n_drop": 3, "hold_thresh": 2, "risk_degree": 0.95},
        }],
        "execution_mode": "serial",
    },
    "custom_evo_hmm": {
        "task_name": "[E2E Test] Custom Evo + HMM",
        "target_desc": "E2E validation - HMM enabled",
        "loops": [{
            "label": "E2E-hmm",
            "loop_index": 1,
            "factor_keys": ["Alpha158_feature_0||rdagent", "Alpha158_feature_1||rdagent"],
            "model_id": "LGBModel",
            "enable_sector_hmm": True,
            "hmm_model_version_id": "",  # 运行时从 DB 查询填入
            "hmm_signal_preset": "preset_A",
            "strategy_params": {"topk": 30, "n_drop": 3},
        }],
        "execution_mode": "serial",
    },
    "custom_evo_full": {
        "task_name": "[E2E Test] Custom Evo Full Params",
        "target_desc": "E2E validation - all params",
        "loops": [{
            "label": "E2E-full",
            "loop_index": 1,
            "factor_keys": ["Alpha158_feature_0||rdagent", "Alpha158_feature_1||rdagent"],
            "model_id": "LGBModel",
            "execution_algo": "VWAP_TWAP_HYBRID",
            "execution_algo_params": {"vwap_ratio": 0.6},
            "unfilled_handler": "TAIL_BOOST",
            "unfilled_handler_params": {"trigger_minute": 55},
            "strategy_params": {"topk": 30, "n_drop": 3, "hold_thresh": 2, "risk_degree": 0.95},
        }],
        "execution_mode": "serial",
    },
}


def resolve_hmm_snapshot(api: str) -> str:
    """查询最新的已完成 HMM 快照 ID"""
    r = httpx.get(f"{api}/hmm-training/configs", params={"model_type": "sector_hmm"})
    configs = r.json()
    if not configs:
        return ""
    for cfg in configs:
        r2 = httpx.get(f"{api}/hmm-training/configs/{cfg['config_id']}/snapshots")
        snapshots = [s for s in r2.json() if s.get("status") == "completed"]
        if snapshots:
            return snapshots[0]["snapshot_id"]
    return ""


def create_task(api: str, config: dict, engine_mode: str) -> str:
    """创建自定义演进任务，返回 task_id"""
    payload = {**config, "engine_mode": engine_mode} if engine_mode else config
    r = httpx.post(f"{api}/quantevolver/evolution/custom-tasks",
                   json=payload, timeout=30)
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Create failed: {data}")
    return data["task_id"]


def wait_task(api: str, task_id: str, timeout_min: int = 15) -> dict:
    """轮询等待任务完成"""
    deadline = time.time() + timeout_min * 60
    while time.time() < deadline:
        r = httpx.get(f"{api}/quantevolver/evolution/tasks/{task_id}", timeout=10)
        data = r.json().get("data", r.json())
        status = data.get("status")
        if status in ("completed", "failed"):
            return data
        print(f"  [{task_id}] status={status}, waiting...")
        time.sleep(30)
    raise TimeoutError(f"Task {task_id} did not complete in {timeout_min} min")


def get_metrics(api: str, task_id: str, loop_index: int = 1) -> dict:
    """获取 Loop 的 metrics"""
    loop_id = f"{task_id}_Loop{loop_index}"
    r = httpx.get(f"{api}/quantevolver/evolution/tasks/{task_id}/loops/{loop_id}/enhanced-metrics", timeout=30)
    return r.json()


def compare_metrics(legacy: dict, unified: dict, tolerance: float = 1e-6) -> list:
    """对比两组 metrics，返回差异列表"""
    diffs = []
    summary_l = legacy.get("summary", {})
    summary_u = unified.get("summary", {})
    for key in set(list(summary_l.keys()) + list(summary_u.keys())):
        v_l = summary_l.get(key)
        v_u = summary_u.get(key)
        if v_l is None or v_u is None:
            if v_l != v_u:
                diffs.append(f"  {key}: legacy={v_l}, unified={v_u} (one is None)")
            continue
        if isinstance(v_l, (int, float)) and isinstance(v_u, (int, float)):
            if abs(v_l - v_u) > tolerance:
                diffs.append(f"  {key}: legacy={v_l:.6f}, unified={v_u:.6f}, diff={abs(v_l-v_u):.2e}")
        elif v_l != v_u:
            diffs.append(f"  {key}: legacy={v_l}, unified={v_u}")
    return diffs


def run_e2e(api: str, test_name: str, config: dict):
    """运行单个 E2E 测试：创建 legacy + unified 任务，对比结果"""
    print(f"\n{'='*60}")
    print(f"E2E Test: {test_name}")
    print(f"{'='*60}")

    # 创建 legacy 任务
    print("[1/4] Creating legacy task...")
    legacy_id = create_task(api, config, engine_mode="legacy")
    print(f"  legacy task_id: {legacy_id}")

    # 创建 unified 任务
    print("[2/4] Creating unified task...")
    unified_id = create_task(api, config, engine_mode="unified")
    print(f"  unified task_id: {unified_id}")

    # 等待完成
    print("[3/4] Waiting for tasks to complete...")
    legacy_result = wait_task(api, legacy_id)
    unified_result = wait_task(api, unified_id)

    if legacy_result["status"] == "failed":
        print(f"  FAIL: legacy task failed")
        return False
    if unified_result["status"] == "failed":
        print(f"  FAIL: unified task failed")
        return False

    # 对比 metrics
    print("[4/4] Comparing metrics...")
    legacy_metrics = get_metrics(api, legacy_id)
    unified_metrics = get_metrics(api, unified_id)
    diffs = compare_metrics(legacy_metrics, unified_metrics)

    if diffs:
        print(f"  DIFFERENCES FOUND ({len(diffs)}):")
        for d in diffs:
            print(f"    {d}")
        return False
    else:
        print("  PASS: All metrics match")
        return True


def main():
    parser = argparse.ArgumentParser(description="QE Unified Engine E2E Validation")
    parser.add_argument("--api", default=API, help="AIstock API base URL")
    parser.add_argument("--path", choices=["custom_evo_minimal", "custom_evo_hmm", "custom_evo_full", "all"],
                        default="all", help="Which test to run")
    parser.add_argument("--skip-legacy", action="store_true", help="Only run unified engine (no comparison)")
    args = parser.parse_args()

    # 解析 HMM 快照
    hmm_snap = resolve_hmm_snapshot(args.api)
    if hmm_snap:
        TEST_CONFIGS["custom_evo_hmm"]["loops"][0]["hmm_model_version_id"] = hmm_snap
        print(f"Resolved HMM snapshot: {hmm_snap}")
    else:
        print("WARNING: No HMM snapshot found, skipping HMM test")

    tests = [args.path] if args.path != "all" else list(TEST_CONFIGS.keys())
    results = {}

    for test_name in tests:
        if test_name == "custom_evo_hmm" and not hmm_snap:
            print(f"Skipping {test_name} (no HMM snapshot)")
            continue
        config = TEST_CONFIGS[test_name]
        passed = run_e2e(args.api, test_name, config)
        results[test_name] = passed

    # 汇总
    print(f"\n{'='*60}")
    print("E2E VALIDATION SUMMARY")
    print(f"{'='*60}")
    all_pass = True
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_pass = False

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
```

---

## 8. 执行脚本

### 8.1 scripts/run_all_tests.sh

```bash
#!/bin/bash
# 一键运行全部测试
set -e

cd "$(dirname "$0")/.."
echo "=== Level 1: Unit Tests ==="
python -m pytest tests/unified_engine/test_experiment_config.py -v
python -m pytest tests/unified_engine/test_metrics_normalizer.py -v

echo "=== Level 2: Integration Tests ==="
python -m pytest tests/unified_engine/test_backtest_executor.py -v
python -m pytest tests/unified_engine/test_backtest_analyzer.py -v

echo "=== Level 3: A/B Comparison (requires DB) ==="
python -m pytest tests/unified_engine/test_ab_comparison.py -v

echo "=== All tests passed ==="
```

### 8.2 测试执行顺序

| 阶段 | 命令 | 依赖 | 耗时 |
|------|------|------|------|
| Level 1 | `pytest tests/unified_engine/test_experiment_config.py test_metrics_normalizer.py` | 无 | <5s |
| Level 2 | `pytest tests/unified_engine/test_backtest_executor.py test_backtest_analyzer.py` | 无 | <5s |
| Level 3 | `pytest tests/unified_engine/test_ab_comparison.py` | DB 连接 + 已有任务数据 | <10s |
| Level 4 | `python scripts/run_e2e_validation.py --path custom_evo_minimal` | AIstock + RDAgent 运行中 | ~5min |
| Level 4 全量 | `python scripts/run_e2e_validation.py --path all` | 同上 | ~15min |

---

## 9. 参数覆盖矩阵

确保以下所有参数组合都被测试覆盖：

| 参数维度 | 可选值 | Level 1 (单元) | Level 2 (组件) | Level 3 (A/B) | Level 4 (E2E) |
|---------|--------|---------------|---------------|---------------|---------------|
| **backtest_freq** | 1min | CONFIG_MATRIX_FULL | parametrize | 真实数据 | minute_lgb |
| | day | DAILY_FREQ / DAILY_SIMPLE | parametrize | 真实数据 | daily_lgb / daily_simple |
| **model_type** | LGBModel | LGB_MODEL_CONFIG | parametrize | 真实数据 | minute_lgb |
| | XGBModel | XGB_MODEL_CONFIG | parametrize | 真实数据 | minute_xgb |
| | CatBoostModel | CATBOOST_MODEL_CONFIG | parametrize | - | - |
| | LinearModel | LINEAR_MODEL_CONFIG | parametrize | - | minute_linear |
| **quick_train** | true | QUICK_TRAIN_CONFIG | parametrize | - | train_quick |
| | false | 默认 | 默认 | 默认 | 默认 |
| **strategy_id** | None (默认) | DEFAULT_STRATEGY | parametrize | 真实数据 | 默认 |
| | topk_dropout | CUSTOM_STRATEGY | parametrize | 真实数据 | - |
| **execution_algo** | None (默认TWAP) | MINIMAL_CONFIG | parametrize | 真实数据 | minute_lgb |
| | CLOSE_PRICE | CLOSE_PRICE_CONFIG | parametrize | - | daily_lgb |
| | TWAP | TWAP_CONFIG | parametrize | - | minute_twap |
| | VWAP | VWAP_CONFIG | parametrize | - | minute_vwap |
| | SBB_EMA | SBB_EMA_CONFIG | parametrize | - | - |
| **unfilled_handler** | None | MINIMAL_CONFIG | parametrize | 真实数据 | minute_lgb |
| | TAIL_BOOST | TAIL_BOOST_CONFIG | parametrize | - | minute_tail_boost |
| | TAIL_SUBSTITUTE | TAIL_SUBSTITUTE_CONFIG | parametrize | - | minute_tail_substitute |
| **hmm** | disabled | HMM_DISABLED | parametrize | 真实数据 | minute_lgb |
| | enabled+preset_A | HMM_ENABLED | parametrize | 真实数据 | minute_hmm |
| **stock_pool** | None | MINIMAL_CONFIG | parametrize | 真实数据 | minute_lgb |
| | 自定义路径 | BLACKLIST_CONFIG | parametrize | - | minute_blacklist |
| **sector_blacklist** | None | MINIMAL_CONFIG | parametrize | 真实数据 | minute_lgb |
| | SW2 列表 | BLACKLIST_CONFIG | parametrize | - | minute_blacklist |
| **label_type** | None | MINIMAL_CONFIG | parametrize | 真实数据 | 默认 |
| | 自定义表达式 | FULL_CONFIG | parametrize | - | - |
| **data_split** | None (默认) | MINIMAL_CONFIG | parametrize | 真实数据 | 默认 |
| | 自定义日期 | CUSTOM_SPLIT | parametrize | - | - |
| **mode** | FULL_TRAIN | - | test_full_train | - | 全部 E2E |
| | BACKTEST_ONLY | - | test_backtest_only | - | (Phase 5.2 策略演进) |
| **组合: 分钟线全功能** | TWAP+TAIL_BOOST+HMM | MINUTE_FULL | parametrize | - | minute_full_combo |
| **组合: 日频简单** | day+CLOSE_PRICE | DAILY_SIMPLE | parametrize | - | daily_simple |
| **组合: 黑名单+尾盘** | VWAP+TAIL_SUB+blacklist | MINUTE_BLACKLIST | parametrize | - | minute_blacklist |
| **组合: XGB+快速训练** | XGB+quick_train+split | XGB_QUICK_CUSTOM | parametrize | - | - |

### 模型训练验证矩阵

| 场景 | 模型 | 验证内容 | Level |
|------|------|---------|-------|
| train_lgb | LGBModel | 训练完成 + metrics 非空 + IC > 0 | E2E |
| train_xgb | XGBModel | 训练完成 + metrics 非空 | E2E |
| train_linear | LinearModel | 训练完成 + metrics 非空 | E2E |
| train_quick | LGBModel + quick_train | 训练时间 < 正常训练的 40% | E2E |
| backtest_only | LGBModel | 跳过训练 + 复用模型 + metrics 非空 | E2E (Phase 5.2) |

---

## 10. 验收标准

| 级别 | 通过标准 |
|------|---------|
| Level 1 | 全部 pass，0 failures |
| Level 2 | 全部 pass，mock 调用参数与预期一致 |
| Level 3 | 新旧路径 custom_params 完全一致（0 diff） |
| Level 4 | 新旧引擎回测 summary metrics 完全一致（tolerance < 1e-6） |

Level 1-3 全部通过后才能进入 Level 4。Level 4 通过后才能将 engine_mode 默认值改为 unified。
