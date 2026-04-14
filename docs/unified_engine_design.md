# QE 统一配置引擎 + 可插拔执行层 + 分析层 设计方案

> 版本: v1.0 | 日期: 2026-04-09 | 状态: 设计评审

---

## 1. 背景与问题

### 1.1 当前架构现状

QE 系统存在 **4 套独立的参数组装+执行代码**，各自 100-200 行：

| 路径 | 入口函数 | 文件位置 | 代码量 |
|------|---------|---------|--------|
| Path 1: 单次实验 | `run_experiment()` | `routers/quantevolver.py:3255` | ~100 行 |
| Path 2: 自动演进 | `submit_next_loop()` | `qe_evolution_service.py:947` | ~200 行 |
| Path 3: 策略演进 | `submit_strategy_evo_loop()` | `qe_evolution_service.py:2695` | ~150 行 |
| Path 4: 自定义演进 | `submit_custom_evo_loop()` | `qe_evolution_service.py:3301` | ~150 行 |

4 条路径最终都调用同一个管道：
```
ConfigComposer.compose_experiment_in_memory() → QEWorkspaceClient.create_and_run_loop()
```

### 1.2 已知问题

1. **参数注入不一致**：每条路径各自组装参数，导致功能覆盖不同

| 参数 | Path1 | Path2 | Path3 | Path4 |
|------|-------|-------|-------|-------|
| HMM | ✅ | ❌ broken | ✅ | ✅ |
| sector_blacklist | ❌ | ❌ | ❌ | ✅ |
| label_type | ❌ | ✅ | ❌ | ✅ |
| backtest_only | ❌ | ❌ | ✅ | ❌ |

2. **结果分析代码重复**：`_METRIC_ALIASES` 字典在两个函数中完全重复，metrics 提取逻辑 90% 相似
3. **新功能修改成本 ×4**：每个新参数（多 alpha、HMM 大盘仓位）需要在 4 处修改
4. **模拟盘/实盘无法复用 QE 配置**：配置与执行耦合，无法跨场景共享

### 1.3 未来需求

- 多 alpha 信号支持
- HMM 大盘趋势判断仓位
- QE 验证通过 → 一键部署模拟盘
- QMT 模拟盘/实盘执行
- 实盘选股信号输出

---

## 2. 设计目标

1. **配置组装一次，执行目标可切换** — 同一份 ExperimentConfig 可以交给回测/模拟盘/实盘
2. **新增配置参数只改一处** — 所有路径自动获得支持
3. **新增执行目标不需要重写配置逻辑** — 只需实现新的 Executor
4. **新旧路径并行运行** — 前端 UI 切换，验证通过后再统一
5. **结果分析独立于执行** — 不同执行器完成后调用统一分析

---

## 3. 整体架构

```
┌─────────────────────────────────────────────────────────┐
│                     调用方 (Callers)                      │
│  单次实验 │ 自动演进 │ 策略演进 │ 自定义演进 │ 模拟盘部署  │
└─────────┬───────────┬──────────┬───────────┬────────────┘
          │           │          │           │
          ▼           ▼          ▼           ▼
┌─────────────────────────────────────────────────────────┐
│           Layer 1: ExperimentConfig (配置层)               │
│                                                           │
│  ExperimentConfig — 与执行无关的纯配置描述                  │
│  ├── 核心: factor_names, model_id, strategy_id            │
│  ├── 数据: data_split, label_type                         │
│  ├── HMM: HmmConfig 子模型                                │
│  ├── 过滤: stock_pool, sector_blacklist                   │
│  ├── 执行算法: execution_algo, unfilled_handler           │
│  ├── 策略参数: strategy_params                            │
│  └── 扩展: extra_params (多alpha/大盘仓位等)              │
│                                                           │
│  build_custom_params() → dict  (唯一参数注入点)            │
│  resolve_hmm() — snapshot_id → model_path 解析            │
│  validate() — 因子可用性/HMM快照存在性校验                 │
└───────────────────────┬─────────────────────────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
┌──────────────┐ ┌────────────┐ ┌──────────────┐
│  Backtest    │ │ PaperTrade │ │ QMT          │
│  Executor    │ │ Executor   │ │ Executor     │
│ (RDAgent WSL)│ │ (AIstock)  │ │ (券商接口)   │
└──────┬───────┘ └─────┬──────┘ └──────┬───────┘
       │               │               │
       ▼               ▼               ▼
┌─────────────────────────────────────────────────────────┐
│           Layer 3: AnalysisEngine (分析层)                 │
│                                                           │
│  MetricsNormalizer — QLib长键→短键映射 (消除重复)          │
│  MetricsStore — 统一 DB 写入接口                          │
│  BacktestResultAnalyzer — 回测结果提取+展示               │
│  (未来) PortfolioAnalyzer — 模拟盘/实盘绩效分析           │
└─────────────────────────────────────────────────────────┘
```

---

## 4. Layer 1: 配置层 (ExperimentConfig)

### 4.1 文件位置

```
backend/services/quantevolver/experiment_config.py  (新建, ~200 行)
```

### 4.2 数据模型定义

```python
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, model_validator


class HmmConfig(BaseModel):
    """HMM 行业热度配置子模型"""
    enable_sector_hmm: bool = False
    hmm_model_version_id: str | None = None       # 快照 ID
    sector_hmm_model_path: str | None = None       # 由 snapshot_id 解析的实际文件路径
    hmm_signal_preset: str | None = None           # preset_A / preset_B / ...
    hmm_signal_presets: dict[str, Any] | None = None  # 从 DB config_json 注入的完整 presets


class ExperimentConfig(BaseModel):
    """
    与执行目标无关的实验配置描述。
    
    调用方负责从各自的来源（DB记录 / reviewer输出 / loop_config / portfolio_config）
    构造此对象。配置层不关心"谁创建了我"，只关心"跑什么"。
    """

    # ── 核心实验参数 ──
    factor_names: list[str]                                  # 因子列表
    model_id: str                                            # 模型 ID
    strategy_id: str | None = None                           # 策略 ID
    data_split: dict[str, str] | None = None                 # 训练/验证/测试划分
    label_type: str | None = None                            # 标签类型

    # ── HMM 配置 ──
    hmm: HmmConfig | None = None

    # ── 股票池 & 行业过滤 ──
    stock_pool: str | None = None                            # 股票池文件路径
    sector_blacklist: list[str] | None = None                # 行业黑名单 (SW2 codes)

    # ── 执行算法 ──
    execution_algo: str | None = None                        # TWAP / VWAP / CLOSE_PRICE
    execution_algo_params: dict[str, Any] | None = None
    unfilled_handler: str | None = None                      # TAIL_BOOST / TAIL_SUBSTITUTE
    unfilled_handler_params: dict[str, Any] | None = None

    # ── 策略参数 ──
    strategy_params: dict[str, Any] | None = None            # topk, n_drop, hold_thresh, risk_degree 等

    # ── 扩展点（未来功能） ──
    extra_params: dict[str, Any] | None = None               # 多alpha信号 / HMM大盘仓位 / 其他

    # ── 校验 ──
    @model_validator(mode="after")
    def validate_hmm(self) -> "ExperimentConfig":
        if self.hmm and self.hmm.enable_sector_hmm:
            if not self.hmm.hmm_model_version_id:
                raise ValueError("enable_sector_hmm=True 但 hmm_model_version_id 未配置")
        if not self.factor_names:
            raise ValueError("factor_names 不能为空")
        return self

    def build_custom_params(self) -> dict[str, Any]:
        """
        唯一的参数注入点 — 所有执行器共享。
        将结构化的配置字段转换为 compose_experiment_in_memory 期望的 custom_params dict。
        """
        params: dict[str, Any] = {}

        # HMM
        if self.hmm and self.hmm.enable_sector_hmm:
            params["enable_sector_hmm"] = True
            params["hmm_model_version_id"] = self.hmm.hmm_model_version_id
            params["sector_hmm_model_path"] = self.hmm.sector_hmm_model_path
            if self.hmm.hmm_signal_preset:
                params["hmm_signal_preset"] = self.hmm.hmm_signal_preset

        # 股票池 & 行业过滤
        if self.stock_pool:
            params["stock_pool"] = self.stock_pool
        if self.sector_blacklist:
            params["sector_blacklist"] = self.sector_blacklist

        # 尾盘处理
        if self.unfilled_handler:
            params["unfilled_handler"] = self.unfilled_handler
            if self.unfilled_handler_params:
                for k, v in self.unfilled_handler_params.items():
                    params[k] = v

        # 标签类型
        if self.label_type:
            params["label_type"] = self.label_type

        # 扩展参数（多alpha、大盘仓位等未来功能直接 merge）
        if self.extra_params:
            params.update(self.extra_params)

        return params
```

### 4.3 配置层设计原则

1. **纯数据，无副作用** — `ExperimentConfig` 不调用任何外部服务，不访问 DB
2. **HMM 路径解析在构造时完成** — 调用方负责调用 `HMMTrainingService.get_snapshot()` 解析 `hmm_model_version_id` → `sector_hmm_model_path`，然后传入已解析的完整 `HmmConfig`
3. **Pydantic 校验** — 构造时自动校验必填字段、HMM 配置一致性
4. **JSON 序列化** — 可直接 `.model_dump_json()` 用于日志、审计、A/B 对比

### 4.4 辅助函数：从各来源构造 ExperimentConfig

```python
# experiment_config_builders.py (新建, ~150 行)

def build_from_experiment_record(exp: dict, hmm_svc: HMMTrainingService) -> ExperimentConfig:
    """Path 1: 从 qe_experiments 表记录构造"""
    ...

def build_from_base_or_reviewer(task: dict, config: dict, hmm_svc: HMMTrainingService) -> ExperimentConfig:
    """Path 2: 从 base_experiment 或 reviewer 输出构造"""
    ...

def build_from_loop_config(loop_config: dict, hmm_svc: HMMTrainingService) -> ExperimentConfig:
    """Path 3/4: 从 strategy_evo_config.loops[i] 构造"""
    ...

def build_from_portfolio_config(portfolio: dict) -> ExperimentConfig:
    """未来: 从模拟盘 portfolio_config 构造"""
    ...
```

---

## 5. 前端切换机制

### 7.1 设计思路

前端在 QE 演进任务创建页面提供 UI 开关，用户选择"原有架构"或"统一引擎"。
后端根据此标记分发到新旧代码路径。新旧并行运行，验证通过后再统一切换。

### 7.2 前端 UI

在 `evolution/page.tsx` 任务创建表单中增加引擎选择按钮组（放在执行设置区域）：

```typescript
const [engineMode, setEngineMode] = useState<"legacy" | "unified">("legacy");
```

提交时将 `engine_mode` 字段随请求发送。

### 7.3 后端路由

任务创建请求模型增加字段：

```python
engine_mode: str = "legacy"   # "legacy" | "unified"
```

service 层根据 `engine_mode` 分发：

```python
async def submit_next_loop(self, task_id: str):
    task = self._load_task(task_id)
    if task.get("engine_mode") == "unified":
        return await self._submit_loop_unified(task_id)
    else:
        return await self._submit_loop_legacy(task_id)  # 现有代码不动
```

### 7.4 切换策略

1. Phase 1: 默认 `"legacy"`，统一引擎仅供手动测试
2. Phase 2: 验证通过后默认改为 `"unified"`，保留 `"legacy"` 回退
3. Phase 3: 确认稳定后删除 `"legacy"` 路径和 UI 开关

---

## 6. 分阶段实施计划

### Phase 0: 准备工作 (1 天)

| 任务 | 文件 | 说明 |
|------|------|------|
| 创建目录结构 | `services/quantevolver/executors/` | 新建 `__init__.py` |
| | `services/quantevolver/analysis/` | 新建 `__init__.py` |

验证: 目录创建成功，import 不报错

### Phase 1: 配置层 (2-3 天)

| 任务 | 文件 | 代码量 |
|------|------|--------|
| 1.1 实现 ExperimentConfig + HmmConfig | `experiment_config.py` | ~200 行 |
| 1.2 实现 4 个 builder 函数 | `experiment_config_builders.py` | ~150 行 |
| 1.3 单元测试 | `tests/test_experiment_config.py` | ~100 行 |

1.1 详细任务:
- 定义 HmmConfig 子模型
- 定义 ExperimentConfig 主模型（所有 4 条路径的参数超集）
- 实现 `build_custom_params()` 方法
- 实现 Pydantic model_validator 校验

1.2 详细任务:
- `build_from_experiment_record()`: 参考 `routers/quantevolver.py:3255-3354`
- `build_from_base_or_reviewer()`: 参考 `qe_evolution_service.py:1004-1094`
- `build_from_loop_config()`: 参考 `qe_evolution_service.py:3351-3401`
- 每个 builder 负责 HMM snapshot_id -> model_path 解析

1.3 验证方式:
- 对每条路径用真实 DB 数据构造 ExperimentConfig
- 调用 `build_custom_params()`，与现有路径产出的 custom_params 做 diff
- 确认完全一致

### Phase 2: 执行层 (2-3 天)

| 任务 | 文件 | 代码量 |
|------|------|--------|
| 2.1 实现 BaseExecutor + ExecutionContext | `executors/base.py` | ~50 行 |
| 2.2 实现 BacktestExecutor | `executors/backtest.py` | ~120 行 |
| 2.3 集成测试 | `tests/test_backtest_executor.py` | ~80 行 |

2.2 详细任务:
- 封装 `ConfigComposer.compose_experiment_in_memory()` 调用
- 封装 `--backtest-only` 注入逻辑
- 封装 `QEWorkspaceClient.create_and_run_loop()` 调用
- 返回 ExecutionResult

2.3 验证方式:
- Mock ConfigComposer 和 QEWorkspaceClient
- 验证传给 compose_experiment_in_memory 的参数与现有路径一致
- 验证 BACKTEST_ONLY 模式正确注入 --backtest-only

### Phase 3: 分析层 (2-3 天)

| 任务 | 文件 | 代码量 |
|------|------|--------|
| 3.1 实现 MetricsNormalizer | `analysis/metrics_normalizer.py` | ~60 行 |
| 3.2 实现 MetricsStore | `analysis/metrics_store.py` | ~120 行 |
| 3.3 实现 BacktestResultAnalyzer | `analysis/backtest_analyzer.py` | ~150 行 |
| 3.4 单元测试 | `tests/test_analysis.py` | ~100 行 |

3.1: 提取 `_METRIC_ALIASES` 为独立模块（消除重复），实现 `normalize_metrics()`
3.2: 统一 DB 写入（save_loop_metrics / save_experiment_record / save_factor_model_records）
3.3: 统一调用 get_loop_metrics + get_enhanced_metrics，产出与前端 ExperimentDetailPage 对齐的 BacktestResult

### Phase 4: 前端切换 + 端到端集成 (2 天)

| 任务 | 文件 | 说明 |
|------|------|------|
| 4.1 前端增加引擎选择 UI | `evolution/page.tsx` | 引擎切换按钮 |
| 4.2 后端增加 engine_mode 分发 | `qe_evolution_service.py` | if/else 分发新旧路径 |
| 4.3 实现统一路径 submit | `qe_evolution_service.py` | 调用 ExperimentConfig + BacktestExecutor |
| 4.4 实现统一路径 process | `qe_evolution_service.py` | 调用 BacktestResultAnalyzer |

验证: 创建自定义演进任务选择"统一引擎"，确认 Loop 正常提交、完成、指标正确

### Phase 5: 逐路径验证 (3-5 天)

| 任务 | 验证内容 |
|------|---------|
| 5.1 验证 Path 4（自定义演进） | 新旧引擎对比指标一致 |
| 5.2 验证 Path 3（策略演进） | 验证 backtest-only 模式 |
| 5.3 验证 Path 1（单次实验） | 验证结果详情页数据一致 |
| 5.4 验证 Path 2（自动演进） | 验证 Agent 分析 + SOTA 判断正常 |

每个验证步骤: 用相同配置分别创建 legacy 和 unified 任务，等待完成后对比 DB 记录和前端展示。

### Phase 6: 默认切换 + 清理 (1-2 天)

| 任务 | 说明 |
|------|------|
| 6.1 默认引擎改为 unified | 前端默认值改为 "unified" |
| 6.2 保留 legacy 回退 | UI 开关保留，标注"旧版（即将移除）" |
| 6.3 观察期 1-2 周 | 确认无问题 |
| 6.4 删除旧代码 | 移除 4 套旧参数组装代码（净减 ~500 行） |
| 6.5 移除 UI 开关 | 删除前端引擎选择按钮 |


---

## 7. Layer 2: 执行层 (Executors)

### 5.1 文件结构

```
backend/services/quantevolver/executors/
├── __init__.py
├── base.py              # BaseExecutor + ExecutionContext + ExecutionResult (~50 行)
├── backtest.py          # BacktestExecutor — RDAgent WSL 回测 (~120 行)
├── paper_trading.py     # PaperTradingExecutor — AIstock 模拟盘 (未来)
└── qmt.py               # QMTExecutor — 券商接口 (未来)
```

### 5.2 基础接口

```python
# executors/base.py

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any
from pydantic import BaseModel


class ExecutionContext(BaseModel):
    """执行上下文 — 与实验配置正交的执行环境参数"""
    task_id: str
    loop_index: int
    experiment_name: str
    node_id: str | None = None
    callback_url: str | None = None


class ExecutionResult(BaseModel):
    """执行结果"""
    job_id: str
    status: str                          # submitted / running / completed / failed
    experiment_files: dict[str, str] | None = None   # 生成的实验文件（用于审计）
    wsl_command: str | None = None       # 实际执行的命令（用于调试）
    detail: dict[str, Any] | None = None


class BaseExecutor(ABC):
    """执行器抽象接口 — 所有执行目标实现此接口"""

    @abstractmethod
    async def submit(
        self,
        config: "ExperimentConfig",
        ctx: ExecutionContext,
        **kwargs,
    ) -> ExecutionResult:
        """提交实验到执行目标"""
        ...
```

### 5.3 BacktestExecutor — RDAgent WSL 回测

```python
# executors/backtest.py

import re
from enum import Enum
from typing import Any

from ..experiment_config import ExperimentConfig
from ..config_composer import ConfigComposer
from ..qe_workspace_client import QEWorkspaceClient
from .base import BaseExecutor, ExecutionContext, ExecutionResult


class BacktestMode(str, Enum):
    FULL_TRAIN = "full_train"           # 完整训练 + 回测
    BACKTEST_ONLY = "backtest_only"     # 复用已训练模型，仅回测


class BacktestExecutor(BaseExecutor):
    """
    RDAgent WSL 回测执行器。
    
    封装 ConfigComposer + QEWorkspaceClient 的两步调用，
    替代当前 4 套重复的参数组装代码。
    """

    def __init__(self, composer: ConfigComposer, client: QEWorkspaceClient):
        self.composer = composer
        self.client = client

    async def submit(
        self,
        config: ExperimentConfig,
        ctx: ExecutionContext,
        mode: BacktestMode = BacktestMode.FULL_TRAIN,
        model_source: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        """
        提交回测任务到 RDAgent。
        
        Args:
            config: 实验配置（配置层产出）
            ctx: 执行上下文（task_id, loop_index 等）
            mode: 执行模式 — FULL_TRAIN 或 BACKTEST_ONLY
            model_source: backtest_only 模式下的已训练模型来源
        """
        # 1. 校验
        if mode == BacktestMode.BACKTEST_ONLY and not model_source:
            raise ValueError("model_source is required when mode=BACKTEST_ONLY")

        # 2. 构建 custom_params（配置层的唯一注入点）
        custom_params = config.build_custom_params()

        # 3. 调用 ConfigComposer（已有统一层，不改）
        compose_res = self.composer.compose_experiment_in_memory(
            factor_names=config.factor_names,
            model_id=config.model_id,
            strategy_id=config.strategy_id,
            data_split=config.data_split,
            custom_params=custom_params,
            execution_algo=config.execution_algo,
            execution_algo_params=config.execution_algo_params,
            strategy_params=config.strategy_params,
            node_id=ctx.node_id,
            experiment_name=ctx.experiment_name,
            skip_db_save=True,
        )

        experiment_files = compose_res.get("experiment_files", {})
        wsl_command = compose_res.get("wsl_command", "")

        # 4. 注入 --backtest-only（如果需要）
        if mode == BacktestMode.BACKTEST_ONLY:
            wsl_command = re.sub(
                r"(python\s+qrun_limit_minute\.py\s+\S+\.yaml)",
                r"\1 --backtest-only",
                wsl_command,
            )

        # 5. 提交到 RDAgent
        job_id = await self.client.create_and_run_loop(
            task_id=ctx.task_id,
            loop_index=ctx.loop_index,
            config=compose_res,
            experiment_files=experiment_files,
            wsl_command=wsl_command,
            model_source=model_source,
            callback_url=ctx.callback_url,
        )

        return ExecutionResult(
            job_id=job_id,
            status="submitted",
            experiment_files=experiment_files,
            wsl_command=wsl_command,
        )
```

### 5.4 各路径迁移后的调用方式

#### Path 1: 单次实验
```python
exp = db.get_experiment(experiment_id)
config = build_from_experiment_record(exp, hmm_svc)
ctx = ExecutionContext(task_id=exp["task_id"], loop_index=0, experiment_name=exp["name"])
result = await backtest_executor.submit(config, ctx)
```

#### Path 2: 自动演进
```python
base_config = load_from_base_or_reviewer(task, prev_loop)
config = build_from_base_or_reviewer(task, base_config, hmm_svc)
ctx = ExecutionContext(task_id=task_id, loop_index=loop_index, experiment_name=f"{task_id}/Loop{loop_index}")
result = await backtest_executor.submit(config, ctx)
```

#### Path 3: 策略演进 (backtest-only)
```python
loop_cfg = strategy_evo_config["loops"][i]
config = build_from_loop_config(loop_cfg, hmm_svc)
ctx = ExecutionContext(task_id=task_id, loop_index=i, experiment_name=f"{task_id}/StratEvo{i}")
result = await backtest_executor.submit(
    config, ctx,
    mode=BacktestMode.BACKTEST_ONLY,
    model_source=resolve_model_source(loop_cfg),
)
```

#### Path 4: 自定义演进
```python
loop_cfg = custom_evo_config["loops"][i]
config = build_from_loop_config(loop_cfg, hmm_svc)
ctx = ExecutionContext(task_id=task_id, loop_index=i, experiment_name=f"{task_id}/Custom{i}")
result = await backtest_executor.submit(config, ctx)
```

### 5.5 执行层设计原则

1. **执行器无状态** — 依赖通过构造函数注入（composer, client），方便测试
2. **配置层与执行层通过 `ExperimentConfig` 解耦** — 执行器不关心配置从哪来
3. **`--backtest-only` 是执行层的概念** — 配置层不知道也不关心执行模式
4. **`model_source` 是执行层的概念** — 模型复用是执行优化，不是配置属性

---

## 8. Layer 3: 分析层 (AnalysisEngine)

### 6.1 当前问题

结果分析代码存在 3 套独立实现：

| 路径 | 指标提取 | 归一化 | DB 存储 | Agent 分析 |
|------|---------|--------|--------|-----------|
| `process_completed_loop` | `get_loop_metrics` + `get_enhanced_metrics` | `_METRIC_ALIASES` | loops + experiments + sota_registry + factor/model_records | ✅ 完整 |
| `process_strategy_evo_completed_loop` | `get_loop_metrics`（无 enhanced） | **重复的** `_METRIC_ALIASES` | loops + experiments | ❌ 无 |
| 单次实验 sync-metrics | `_update_experiment_with_metrics` | 另一套手动映射 | experiments 列更新 | ❌ |

`_METRIC_ALIASES` 字典在两个函数中完全重复。metrics 提取和归一化逻辑 90% 相似。

### 6.2 文件结构

```
backend/services/quantevolver/analysis/
├── __init__.py
├── metrics_normalizer.py    # QLib 长键→短键映射 + 指标定义 (~60 行)
├── metrics_store.py         # 统一 DB 写入接口 (~120 行)
├── backtest_analyzer.py     # 回测结果提取 + 展示数据组装 (~150 行)
└── (未来) portfolio_analyzer.py  # 模拟盘/实盘绩效分析
```

### 6.3 MetricsNormalizer — 消除 _METRIC_ALIASES 重复

```python
# analysis/metrics_normalizer.py

# 唯一的指标映射定义 — 所有路径共享
METRIC_ALIASES: dict[str, str] = {
    "Rank IC": "Rank_IC",
    "IC": "IC",
    "1day.excess_return_with_cost.information_ratio": "sharpe",
    "1day.excess_return_with_cost.annualized_return": "annualized_return",
    "1day.excess_return_without_cost.annualized_return": "annualized_return_no_cost",
    "1day.excess_return_with_cost.max_drawdown": "max_drawdown",
    "1day.excess_return_without_cost.max_drawdown": "max_drawdown_no_cost",
    "1day.excess_return_without_cost.information_ratio": "sharpe_no_cost",
}


def normalize_metrics(raw_metrics: dict) -> dict:
    """
    将 QLib 原始指标键名转换为前端友好的短键名。
    所有回测结果处理路径共享此函数。
    """
    normalized = {}
    for raw_key, value in raw_metrics.items():
        short_key = METRIC_ALIASES.get(raw_key, raw_key)
        normalized[short_key] = value
    return normalized
```

### 6.4 MetricsStore — 统一 DB 写入

```python
# analysis/metrics_store.py

class MetricsStore:
    """统一的指标存储接口 — 替代当前 3 套不同的 INSERT/UPDATE 逻辑"""

    def save_loop_metrics(
        self,
        loop_id: str,
        metrics: dict,
        experiment_id: str | None = None,
    ) -> None:
        """更新 qe_evolution_loops.metrics_json + status"""
        ...

    def save_experiment_record(
        self,
        task_id: str,
        loop_id: str,
        loop_index: int,
        config: "ExperimentConfig",
        metrics: dict,
        is_sota: bool = False,
    ) -> str:
        """插入 qe_experiments 记录，返回 experiment_id"""
        ...

    def update_experiment_columns(
        self,
        experiment_id: str,
        metrics: dict,
    ) -> None:
        """更新 qe_experiments 的 sharpe/annualized_return/max_drawdown 等快查列"""
        ...

    def save_factor_model_records(
        self,
        task_id: str,
        loop_index: int,
        factor_names: list[str],
        model_id: str,
        metrics: dict,
    ) -> None:
        """写入 qe_loop_factor_records + qe_loop_model_records"""
        ...
```

### 6.5 BacktestResultAnalyzer — 回测结果提取+展示

参考现有 QE 单次实验详情页的数据结构，统一所有回测路径的结果分析。

```python
# analysis/backtest_analyzer.py

class BacktestResultAnalyzer:
    """
    统一的回测结果分析器。
    
    替代当前分散在 process_completed_loop / process_strategy_evo_completed_loop /
    _update_experiment_with_metrics 中的指标提取逻辑。
    
    产出与前端 ExperimentDetailPage 期望的数据结构完全一致。
    """

    def __init__(self, workspace_client: QEWorkspaceClient, metrics_store: MetricsStore):
        self.client = workspace_client
        self.store = metrics_store

    async def analyze(
        self,
        task_id: str,
        loop_id: str,
        config: "ExperimentConfig",
        loop_index: int,
        save_to_db: bool = True,
        fetch_enhanced: bool = True,
    ) -> "BacktestResult":
        """
        完整的回测结果分析流程。
        
        Args:
            task_id: 任务 ID
            loop_id: Loop ID
            config: 实验配置（用于记录到 DB）
            loop_index: Loop 序号
            save_to_db: 是否写入 DB
            fetch_enhanced: 是否获取增强指标（IC时序、收益曲线、训练曲线）
        
        Returns:
            BacktestResult — 包含所有前端展示需要的数据
        """
        # 1. 获取基础指标
        raw_metrics = await self.client.get_loop_metrics(task_id, loop_id)
        metrics = normalize_metrics(raw_metrics)

        # 2. 获取增强指标（可选）
        enhanced = None
        if fetch_enhanced:
            enhanced = await self.client.get_enhanced_metrics(task_id, loop_id)

        # 3. 组装结果
        result = BacktestResult(
            metrics=metrics,
            enhanced=enhanced,
        )

        # 4. 写入 DB（可选）
        if save_to_db:
            experiment_id = self.store.save_experiment_record(
                task_id=task_id,
                loop_id=loop_id,
                loop_index=loop_index,
                config=config,
                metrics=metrics,
            )
            self.store.save_loop_metrics(loop_id, metrics, experiment_id)
            self.store.save_factor_model_records(
                task_id, loop_index, config.factor_names, config.model_id, metrics,
            )
            result.experiment_id = experiment_id

        return result


class BacktestResult(BaseModel):
    """回测结果 — 与前端 ExperimentDetailPage 数据结构对齐"""

    experiment_id: str | None = None

    # 核心指标（归一化后）
    metrics: dict[str, Any]
    # {ic, icir, rank_ic, sharpe, annualized_return, max_drawdown, ...}

    # 增强指标（用于图表展示）
    enhanced: dict[str, Any] | None = None
    # {
    #   ic_diagnostics: {dates, ic_series, rank_ic_series, ic_rolling_30d_mean, ...},
    #   return_curves: {return_dates, cumulative_excess_no_cost, cumulative_benchmark, drawdown_series, ...},
    #   training_diagnostics: {train_loss_curve, val_loss_curve, best_epoch, ...},
    #   summary: {ic, icir, rank_ic, annualized_return, max_drawdown, ...},
    #   top_stocks, bottom_stocks, all_stocks, stock_trades,
    #   trade_diagnostics: {avg_turnover, total_turnover, cost_drag_annualized, ...},
    #   feature_importance, factor_analysis, absolute_returns,
    # }
```

### 6.6 各路径迁移后的结果处理

#### 自动演进 (process_completed_loop) — 保留 Agent 分析
```python
# 1. 统一分析
result = await analyzer.analyze(task_id, loop_id, config, loop_index, fetch_enhanced=True)

# 2. Agent 分析（仅自动演进需要，保留在调用方）
analyst_result = await self.agents.run_analyst(loop_index, config, result.metrics, ...)
eval_result = await self.agents.run_evaluator(result.metrics, historical_sota, ...)
# ... SOTA 判断、方向决策、多 Agent 调度 ...
```

#### 策略演进 / 自定义演进 (process_strategy_evo_completed_loop) — 简化
```python
# 统一分析，跳过 Agent
result = await analyzer.analyze(task_id, loop_id, config, loop_index, fetch_enhanced=False)
# 完成。不需要 Agent 分析。
```

#### 单次实验 (sync-metrics) — 统一
```python
result = await analyzer.analyze(task_id, loop_id, config, loop_index=0, fetch_enhanced=True)
# 完成。前端直接用 result.enhanced 渲染详情页。
```

### 6.7 分析层设计原则

1. **`BacktestResultAnalyzer.analyze()` 是唯一的回测结果处理入口** — 消除 3 套重复代码
2. **Agent 分析不在分析层内** — Agent 是自动演进的决策逻辑，不是通用分析，保留在调用方
3. **`fetch_enhanced` 控制分析深度** — 策略演进不需要 IC 时序图，跳过以节省时间
4. **`save_to_db` 控制副作用** — 方便测试和 dry-run
5. **`BacktestResult` 与前端数据结构对齐** — 前端不需要额外转换

### 6.8 未来扩展：模拟盘/实盘分析

```python
# analysis/portfolio_analyzer.py (未来)

class PortfolioAnalyzer:
    """
    模拟盘/实盘绩效分析。
    
    复用 MetricsStore 的存储接口，但计算逻辑独立
    （从 daily_snapshot 计算，不依赖 QLib）。
    """

    def analyze_portfolio(self, portfolio_id: int, date_range: tuple) -> PortfolioResult:
        """
        产出与 paper_trading/performance_calculator.py 相同的指标，
        但通过 MetricsStore 统一存储，可与回测指标做对比。
        """
        ...
```

---

## 9. 文件清单总览

### 新建文件

| 文件路径 | 用途 | 预估代码量 |
|---------|------|-----------|
| `backend/services/quantevolver/experiment_config.py` | ExperimentConfig + HmmConfig | ~200 行 |
| `backend/services/quantevolver/experiment_config_builders.py` | 4 个 builder 函数 | ~150 行 |
| `backend/services/quantevolver/executors/__init__.py` | 包初始化 | ~5 行 |
| `backend/services/quantevolver/executors/base.py` | BaseExecutor + ExecutionContext | ~50 行 |
| `backend/services/quantevolver/executors/backtest.py` | BacktestExecutor | ~120 行 |
| `backend/services/quantevolver/analysis/__init__.py` | 包初始化 | ~5 行 |
| `backend/services/quantevolver/analysis/metrics_normalizer.py` | 指标归一化 | ~60 行 |
| `backend/services/quantevolver/analysis/metrics_store.py` | 统一 DB 写入 | ~120 行 |
| `backend/services/quantevolver/analysis/backtest_analyzer.py` | 回测结果分析 | ~150 行 |

新建总计: ~860 行

### 修改文件

| 文件路径 | 修改内容 |
|---------|---------|
| `backend/services/quantevolver/qe_evolution_service.py` | 增加 engine_mode 分发 + 统一路径函数 |
| `backend/routers/quantevolver.py` | 单次实验增加统一路径 |
| `backend/routers/quantevolver_evolution.py` | 请求模型增加 engine_mode 字段 |
| `frontend/src/app/quantevolver/evolution/page.tsx` | 增加引擎选择 UI |

### 不修改的文件（只读参考）

| 文件路径 | 说明 |
|---------|------|
| `backend/services/quantevolver/config_composer.py` | 已有统一组合层，不改 |
| `backend/services/quantevolver/qe_workspace_client.py` | 已有统一提交层，不改 |
| `backend/services/hmm_training_service.py` | HMM 快照解析，被 builder 调用 |

---

## 10. 风险评估与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 新引擎参数组装与旧路径不一致 | 回测结果不同 | Phase 5 A/B 对比验证 |
| build_custom_params() 遗漏某个参数 | 功能缺失 | 单元测试覆盖所有参数组合 |
| 分析层 DB 写入格式不兼容 | 前端展示异常 | 对比新旧路径的 DB 记录 |
| 自动演进的 Agent 分析依赖旧数据格式 | Agent 决策异常 | Phase 5.4 专项验证 |
| 前端切换 UI 引入新 bug | 页面报错 | 默认 legacy，手动切换测试 |

---

## 11. 未来路线图

### 近期 (Phase 7+)

| 功能 | 改动范围 |
|------|---------|
| 多 alpha 信号 | ExperimentConfig 加字段 + build_custom_params() 加一行 |
| HMM 大盘仓位 | 同上 |
| 因子可用性校验统一 | ExperimentConfig.validate() 增加校验逻辑 |

### 中期

| 功能 | 改动范围 |
|------|---------|
| PaperTradingExecutor | 新建 executors/paper_trading.py，从 ExperimentConfig 生成 portfolio_config |
| QE 验证通过 -> 一键部署模拟盘 | 同一个 ExperimentConfig，换执行器 |
| PortfolioAnalyzer | 新建 analysis/portfolio_analyzer.py，复用 MetricsStore |

### 远期

| 功能 | 改动范围 |
|------|---------|
| QMTExecutor | 新建 executors/qmt.py，转换为 QMT 订单格式 |
| 模拟盘 -> 一键切实盘 | 同一个 ExperimentConfig，QMTExecutor(mode=LIVE) |
| SignalOnlyExecutor | 新建 executors/signal.py，只输出选股信号 |
| LiveAnalyzer | 新建 analysis/live_analyzer.py，实盘滑点/执行质量分析 |

---

## 12. 总结

| 维度 | 当前 | 重构后 |
|------|------|--------|
| 参数组装代码 | 4 套 x ~150 行 = ~600 行 | 1 套 ~200 行 (ExperimentConfig) |
| 新功能修改点 | 4 处 | 1 处 |
| 结果分析代码 | 3 套重复 | 1 套统一 (BacktestResultAnalyzer) |
| 执行目标 | 仅回测 | 回测 + 模拟盘 + QMT (可插拔) |
| 迁移风险 | - | 零（新旧并行，UI 切换） |
| 净代码变化 | - | 新建 ~860 行，最终删除 ~500 行旧代码 |
