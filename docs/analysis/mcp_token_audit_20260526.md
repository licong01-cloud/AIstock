# AIstock MCP 服务 Token 消耗审计报告

> 日期：2026-05-26
> 范围：4 个 AIstock MCP 服务共 65 个工具
> 目标：识别 token 浪费风险，提出按需获取的架构改进方案

---

## 一、审计背景

AIstock 的 Claude Code MCP 服务通过 `LoopbackApiClient` 调用本地 FastAPI 后端。当前所有工具返回**完整 JSON 响应**，无字段过滤、无响应大小上限、无分层获取机制。在 LLM context window 中，大量冗余数据直接消耗 token 预算。

### 审计范围

| MCP 服务 | 工具数 | 入口脚本 |
|----------|--------|----------|
| aistock-qe-experiment | 22 | `scripts/aistock_qe_experiment_mcp_server.py` |
| aistock-qe-archive | 20 | `scripts/aistock_qe_archive_mcp_server.py` |
| aistock-research | 16 | `scripts/aistock_mcp_gateway.py --modules=research` |
| aistock-validation | 11 | `scripts/aistock_mcp_server.py` |

### 共性根因

`LoopbackApiClient._decode()` (`scripts/aistock_mcp_common.py:107`)：

```python
return response.json()  # 全量返回，无截断/过滤
```

httpx、FastMCP、后端路由层均无 response body 大小上限。

---

## 二、风险等级定义

| 等级 | 单次调用预估 token 消耗 | 判定标准 |
|------|------------------------|----------|
| CRITICAL | 30,000+ tokens | 无界子列表、含大 JSONB 嵌套、默认 limit 过大 |
| HIGH | 5,000~30,000 tokens | 大型 JSONB 字段、默认 limit >20、时间序列数组 |
| MEDIUM | 1,000~5,000 tokens | 中等 JSONB、可接受的默认 limit |
| LOW | <1,000 tokens | 标量字段为主、响应确定且小 |

---

## 三、aistock-qe-experiment — QE 实验服务

### 3.1 工具风险矩阵

| 风险 | 工具 | 问题 |
|------|------|------|
| CRITICAL | `qe_custom_evo_get_task` | 返回所有 loop 的完整 `config_json` + `metrics_json` + `agent_analysis`；10轮任务可 500KB~50MB |
| CRITICAL | `qe_experiment_list` | 默认 limit=50，每行含完整 `result_metrics` JSONB（含 enhanced_metrics、IC序列、stock_trades） |
| HIGH | `qe_experiment_get` | `SELECT *` 含完整 `result_metrics` JSONB |
| HIGH | `qe_experiment_get_enhanced_metrics` | 完整时间序列 + 逐股票诊断（50~200KB） |
| HIGH | `qe_experiment_get_logs_tail` | 默认 500 行原始日志，最大 5000 行 |
| MEDIUM | `qe_custom_evo_list_tasks` | limit=50，每条含 `strategy_evo_config` JSONB |
| MEDIUM | `qe_custom_evo_get_logs_tail` | 同 logs_tail |
| LOW | `qe_experiment_get_status` | 仅 status 字符串 |
| LOW | `qe_experiment_get_trade_stats` | ~25 个标量字段，<2KB |
| LOW | `qe_template_*` 系列 | 模板配置通常较小 |
| LOW | `*_confirmed` 操作类 | 仅返回操作确认结果 |

### 3.2 关键数据结构分析

#### `qe_experiment_list` — SQL 查询

```sql
SELECT experiment_id, experiment_name, status, factor_names, model_id, strategy_id,
       workspace_path, wsl_command,
       result_metrics,          -- ★ 完整 JSONB，含 enhanced_metrics
       ic, icir, rank_ic, rank_icir,
       annualized_return, max_drawdown, information_ratio,
       annualized_return_no_cost, max_drawdown_no_cost, information_ratio_no_cost,
       created_at, updated_at, custom_params, alpha_mode, multi_alpha_config
FROM qe_experiments
ORDER BY created_at DESC LIMIT 50
```

- **无 JOIN loops 表**，每个 experiment 就是 `qe_experiments` 一行
- 但 `result_metrics` 是完整 JSONB，对于演进实验包含：

```
result_metrics (~50KB~2MB per row)
├── IC, ICIR, Rank IC, annualized_return ...     ← 标量 (~1KB)
└── enhanced_metrics                              ← 大头
    ├── ic_series: [244个float]
    ├── rank_ic_series: [244个float]
    ├── ic_rolling_30d_mean/std: [244个float]
    ├── return_curves/
    │   ├── cumulative_excess_no_cost: [244个float]
    │   ├── cumulative_excess_with_cost: [244个float]
    │   └── drawdown_series: [244个float]
    ├── training_diagnostics/
    │   ├── train_loss: [200个float]
    │   └── val_loss: [200个float]
    ├── stock_trades: { "SH600000": [...], ... }  ← ★ 最大，可达 MB 级
    ├── all_stocks: [100~500条]
    └── factor_analysis/feature_importance
```

- DB 已有提取好的标量列（ic, icir 等 12 个 double precision），列表查询时 `result_metrics` 完全多余

**预估 token 消耗**：50 行 × 50KB~2MB = **2.5MB~100MB** JSON

#### `qe_custom_evo_get_task` — 双查询

```sql
-- 查询1: 任务元信息
SELECT * FROM qe_evolution_tasks WHERE task_id = %s

-- 查询2: 所有 loop
SELECT * FROM qe_evolution_loops WHERE task_id = %s ORDER BY loop_index ASC
```

每个 loop 返回三个完整 JSONB：

| 字段 | 典型大小 | 最大大小 | 主要内容 |
|------|---------|---------|---------|
| `config_json` | 2~8 KB | ~15 KB | 因子列表、模型参数、strategy 参数、execution_manifest（有重复） |
| `metrics_json` | 50 KB~2 MB | 5+ MB | 标量指标 + 完整 enhanced_metrics（IC序列/收益曲线/每笔交易） |
| `agent_analysis` | 5~50 KB | ~200 KB | LLM 分析报告、方向决策、llm_trace（仅标准演进有） |

后端代码已有注释承认问题（`qe_evolution_service.py:1066`）：
> *"enhanced_metrics contains top_stocks/stock_trades/return_curves etc. massive data that would overflow context when accumulated across multiple loops"*

**预估 token 消耗**：10 轮演进 = **500KB~50MB**

---

## 四、aistock-qe-archive — 归档服务

### 4.1 工具风险矩阵

| 风险 | 工具 | 默认 limit | 后端 max | 问题 |
|------|------|-----------|---------|------|
| CRITICAL | `query_factor_importance` | 50 | 500 | 7表 LEFT JOIN，每行 ~55 字段含多个大 JSONB，同 run_id config 重复 |
| HIGH | `query_factor_importance_stability` | 50 | 500 | `ARRAY_AGG` 无上限增长 |
| HIGH | `list_runs` | **100** | 500 | 每行 ~27 字段 + 5 个关联子查询 |
| HIGH | `get_backfill_run` | N/A | N/A | 子查询 `items` 无 limit |
| MEDIUM | `list_outbox` | 50 | 500 | 每条含 `payload` JSON |
| MEDIUM | `list_jobs` | 50 | 500 | 每条含 `stats` JSON |
| MEDIUM | `list_skips` | 100 | 500 | 每条含 `metadata` JSON |
| MEDIUM | `list_backfill_runs` | 50 | 500 | `SELECT *` 含 `request_payload` JSON |
| LOW | `qe_archive_health` | N/A | N/A | 11 个计数字段 |
| LOW | `query_factor_usage` | 50 | 500 | 每行仅 5 字段 |
| LOW | `query_seed_trials` | 50 | 500 | 每行仅 9 字段 |
| LOW | `*_confirmed` 操作类 | N/A | N/A | 执行结果 |

### 4.2 `query_factor_importance` — 7 表 JOIN 详情

Repository 方法 `qe_archive/repository.py:1526-1641`，JOIN 7 张表：

- `run_factor_importance i` (15 columns)
- `run r` (14 columns)
- `run_config c` (8 JSON columns: `model_params`, `strategy_config`, `data_split`, `execution_config`, `runtime_flags`)
- `run_reproducibility_manifest repro` (6 columns: `package_versions` JSON, `deterministic_flags` JSON)
- `run_account_summary acc` (7 columns)
- `run_data_context dc` (8 date columns)
- `run_source s` (5 columns)

默认 limit=50 → **2,750 个数据字段**，`model_params`、`strategy_config`、`package_versions` 等大 JSONB 在同 run_id 的每行重复。

---

## 五、aistock-research — 研究管线

### 5.1 工具风险矩阵

| 风险 | 工具 | 默认 limit | 问题 |
|------|------|-----------|------|
| CRITICAL | `research_get_experiment` | N/A | 6 个无界子查询合并，`attempts` 含 `input_json`+`result_json`，`events` 无 limit |
| HIGH | `research_list_backtest_records` | 100 | 每条含 4 个无界 JSON，100条可 30,000~80,000 tokens |
| HIGH | `research_list_experiments` | 50 | 每条含 3 个 JSONB |
| HIGH | `research_list_artifact_refs` | 100 | 每条含 `metadata_json` |
| MEDIUM | `research_get_stage_result` | N/A | attempts 无 limit，每条含 `input_json`+`result_json` |
| LOW | `research_get_pipeline_types` | N/A | 小型静态字典 |
| LOW | `research_compare_baseline` | N/A | 单条对比记录 |
| LOW | `research_create_issue` | N/A | 单条事件 |
| LOW | `research_promote`/`research_reject` | N/A | 实验更新结果 |

### 5.2 `research_get_experiment` — 6 查询合并

```python
return {
    **experiment,              # 实验元数据
    "stages": list_stage_plans(experiment_id),           # 所有阶段计划
    "attempts": list_stage_attempts(experiment_id),      # 无 limit! ★
    "artifact_refs": list_artifact_refs(experiment_id),  # 无 limit! ★
    "external_run_links": ...,                           # 无 limit!
    "comparisons": list_comparisons(experiment_id),      # 无 limit!
    "events": list_pipeline_events(experiment_id),       # 无 limit! ★
}
```

- 被 `research_create_experiment` **内部调用**，连创建操作都触发全量查询
- 每个 `StageAttemptRecord` 含 `input_json` + `result_json`（无界 JSON）

### 5.3 `research_list_backtest_records` — 最肥的列表

每条 `BacktestRecord` 含 **4 个无界 JSON**：

| JSON 字段 | 内容 |
|-----------|------|
| `metrics_json` | 完整回测指标 |
| `hmm_config_summary_json` | HMM 配置 |
| `config_summary_json` | 非 HMM 配置 |
| `source_payload_json` | 来源负载 |

默认 limit=100 → **30,000~80,000 tokens**。

---

## 六、aistock-validation — 验证中心

### 6.1 工具风险矩阵

| 风险 | 工具 | 默认参数 | 问题 |
|------|------|---------|------|
| HIGH | `get_module_quality_summary` | commit_limit=50 | 始终返回全部 56 个模块；内部调 `list_bugs(page_size=10000)`；单次调用 8,000~15,000 tokens |
| HIGH | `list_bugs` | page_size=50 | MCP 默认 > 后端默认(20)；每条含完整 events 数组 |
| HIGH | `get_validation_execution_log` | tail=100 | 原始终端输出，dense 且重复；可请求到 2000 行 |
| MEDIUM | `mcp_github_issue_list` | page_size=50 | 先加载全部 105 个 bug JSON 再分页 |
| MEDIUM | `mcp_github_issue_search` | page_size=50 | 同上，先全量加载再过滤 |
| LOW | `list_findings` | page_size=20 | 默认合理 |
| LOW | `list_validation_runs` | page_size=20 | 默认合理 |
| LOW | `get_bug_agent_context` | N/A | 单条结构化上下文 |

### 6.2 `get_module_quality_summary` — 全量模块问题

- `module` 参数只在 MCP 层前端过滤，后端始终返回全部 56 个模块
- 内部调用 `list_bugs(page_size=10000)` + `list_findings(page_size=10000)` 计算质量分
- 每个模块含 ~40 字段（workspace/commits/coverage/quality/priority 子对象）
- 单次调用估计 **8,000~15,000 tokens**

---

## 七、共性问题汇总

### 7.1 无响应大小保护

`LoopbackApiClient._decode()` 返回原始 `response.json()`，四个服务均无截断、过滤或大小上限。

### 7.2 列表端点默认 limit 过大

| 服务 | 工具 | MCP 默认 | 后端最大 | 每行宽度 |
|------|------|---------|---------|---------|
| experiment | `experiment_list` | 50 | 无限 | ~30 字段 + 完整 result_metrics |
| archive | `list_runs` | **100** | 500 | ~27 字段 |
| archive | `query_factor_importance` | 50 | 500 | ~55 字段（7表 JOIN） |
| research | `list_backtest_records` | **100** | 500 | 4 个无界 JSON + 27 标量 |
| research | `list_artifact_refs` | 100 | 500 | 1 个无界 JSON |
| validation | `list_bugs` | 50 | 100 | 20 字段含 events 数组 |

### 7.3 detail 端点内嵌无界子列表

| 服务 | 工具 | 无界子列表 |
|------|------|-----------|
| experiment | `evo_get_task` | loops[] 每条含 3 个完整 JSONB |
| research | `get_experiment` | 6 个子查询全无 limit |
| archive | `get_backfill_run` | items[] 无 limit |
| validation | `get_module_quality_summary` | 56 模块 × 每模块 40 字段 |

### 7.4 重复数据 / 冗余嵌套

| 服务 | 工具 | 重复内容 |
|------|------|---------|
| archive | `query_factor_importance` | 同 run_id 的 config/repro/account 在每行重复 |
| experiment | `evo_get_task` | `config_json` 内 `execution_manifest` 与顶层字段重复 |
| research | `list_backtest_records` | `metrics_json` 与标量指标列重复 |
| research | `get_experiment` | stages 计划信息与 attempts 实际执行重复 |

---

## 八、改进方案

### 8.1 设计原则：三层分级 + 按需深入

```
Layer 0 — 列表摘要
  只返回 ID + 标量指标 + 状态，不含任何 JSONB 大对象
  适用场景：浏览实验列表、选择要深入的对象

Layer 1 — 单条详情
  返回配置 + 标量指标，大 JSONB 有独立端点
  适用场景：查看实验配置、比较参数差异

Layer 2 — 完整数据
  按需获取特定 section（enhanced_metrics / stock_trades / analysis）
  适用场景：深入分析单个实验/loop 的表现
```

### 8.2 通用基础设施改动

#### 改动 1：`LoopbackApiClient` 全局响应大小保护

```python
MAX_RESPONSE_BYTES = 100_000  # 100KB

@staticmethod
def _decode(response, method, path):
    if response.status_code >= 400:
        raise RuntimeError(...)
    data = response.json()
    raw_size = len(response.content)
    if raw_size > MAX_RESPONSE_BYTES:
        return {
            "_truncated": True,
            "_original_size_bytes": raw_size,
            "_hint": f"Response exceeded {MAX_RESPONSE_BYTES} bytes. Use specific detail endpoints.",
            "data": data,  # 仍然返回但标记
        }
    return data
```

一次改动，四个服务全部受益。

#### 改动 2：降低默认 limit

| 当前默认 | 建议默认 |
|---------|---------|
| 100 | 20 |
| 50 | 10~15 |

#### 改动 3：列表端点增加 `fields` 参数（投影）

```
GET /experiments?fields=experiment_id,status,ic,icir,annualized_return
```

只返回指定字段，而非全量。后端在 SQL 层做列选择。

### 8.3 aistock-qe-experiment 专用改动

#### `qe_experiment_list` — 去掉 result_metrics

```sql
-- 当前: 30 列含 result_metrics
-- 改进: 只返回标量列
SELECT experiment_id, experiment_name, status, factor_names, model_id, strategy_id,
       ic, icir, rank_ic, rank_icir,
       annualized_return, max_drawdown, information_ratio,
       annualized_return_no_cost, max_drawdown_no_cost, information_ratio_no_cost,
       created_at, updated_at, loop_index, is_evolution_loop, alpha_mode
FROM qe_experiments
ORDER BY created_at DESC LIMIT 15
```

默认 limit 50→15，去掉 `result_metrics`、`custom_params`、`multi_alpha_config`。

#### `qe_experiment_get` — 排除 result_metrics

`SELECT *` 改为显式列名，排除 `result_metrics`。已有独立的 `/enhanced-metrics` 端点提供完整数据。

#### `qe_custom_evo_get_task` — 增加 detail 参数

```python
def qe_custom_evo_get_task(task_id: str, detail: str = "summary") -> dict:
    # summary: loop_index + status + ic + icir + rank_ic + annualized_return + action_type + is_sota
    # full: 当前行为（向后兼容）
```

新增 `detail="summary"` 默认值，只返回每 loop 的标量指标表（~0.5KB/loop vs 当前 50KB~2MB/loop）。

#### 新增：`/loops/comparison` 专用端点

```
GET /evolution/tasks/{id}/loops/comparison

返回：
{
  "task_id": "xxx",
  "task_name": "xxx",
  "loops": [
    {
      "loop_index": 1,
      "action_type": "initial",
      "status": "completed",
      "is_sota": false,
      "factors": ["alpha158", "mf_volatility_5d"],
      "factor_count": 2,
      "ic": 0.0412, "icir": 1.23,
      "rank_ic": 0.0389, "rank_icir": 1.15,
      "annualized_return": 0.0823,
      "max_drawdown": -0.0512,
      "information_ratio": 1.45,
      "ann_return_no_cost": 0.1023,
      "max_drawdown_no_cost": -0.0389,
      "ir_no_cost": 2.01,
      "train_loss_final": 0.00123,
      "overfit_ratio": 0.85,
      "daily_win_rate": 0.52,
    },
    ... (loop 2, 3, ... N)
  ]
}
```

- 一次请求获取全部 loop 的可比指标
- 每个loop ~30 标量字段 (~0.5KB)
- 10 loops = ~5KB，而非当前 50MB
- 包含因子列表用于比较因子演进路径

### 8.4 aistock-qe-archive 专用改动

| 工具 | 改动 |
|------|------|
| `query_factor_importance` | 拆为摘要（因子+重要性分数，无 JOIN config/repro）和详情（完整 JOIN），默认 limit 50→10 |
| `list_runs` | 默认 100→20 |
| `get_backfill_run` | items 加 limit（默认 50） |
| `list_backfill_runs` | `SELECT *` 改为排除 `request_payload` |

### 8.5 aistock-research 专用改动

| 工具 | 改动 |
|------|------|
| `research_get_experiment` | 拆为 `summary`（只含 experiment + stages）和 `detail`（当前行为）；events/attempts/artifacts 默认不返回 |
| `research_list_backtest_records` | 默认 100→10；增加 `detail` 参数，默认排除 4 个 JSON payload 字段 |
| `get_stage_result` | attempts 限制最近 5 条，`input_json`/`result_json` 截断到关键字段 |

### 8.6 aistock-validation 专用改动

| 工具 | 改动 |
|------|------|
| `get_module_quality_summary` | `module` 参数传到后端做 SQL WHERE 过滤，而非 MCP 层前端过滤 |
| `list_bugs` | 增加 compact 模式，只返回 bug_id + title + status + severity |
| `mcp_github_issue_list/search` | 改用流式分页，避免全量加载后再过滤 |

### 8.7 跨 Loop 横向对比操作流程（推荐）

以"分析一个 10 轮演进任务的各 loop 表现"为例：

```
Step 1: GET /loops/comparison           (~5KB)
  → 获得 10 个 loop 的标量指标表
  → Claude 可直接分析：IC 趋势、最优 loop、SOTA 判断

Step 2: 按需深入某个 loop
  → GET /enhanced-metrics?loop_index=3  (~30KB)
  → 只看最佳/异常 loop 的 IC 序列、收益曲线

Step 3: 按需看因子变化
  → comparison 已含 factor_list
  → 无需额外请求即可看因子演进路径

Step 4: 需要看 LLM 分析时
  → GET /loops/3/analysis               (~10KB)
  → 只取感兴趣的 loop
```

### 8.8 改进效果预估

| 操作 | 当前 token 消耗 | 改进后 | 节省 |
|------|----------------|--------|------|
| 列出实验 | 50×2MB = 100MB | 15×0.5KB = 7.5KB | **99.99%** |
| 看一个实验详情 | ~2MB | ~2KB | **99.9%** |
| 10 轮 loop 横向对比 | ~50MB | ~5KB | **99.99%** |
| 深入 1 个 loop | 已含在 50MB 中 | ~30KB | 按需获取 |
| 查询因子重要性 | ~55KB×50行 | ~10 行摘要 | **~95%** |
| 查看研究实验详情 | 6 个无界子查询 | 分层获取 | **~90%** |

---

## 九、实施优先级建议

| 优先级 | 改动 | 影响范围 | 复杂度 |
|--------|------|---------|--------|
| P0 | `LoopbackApiClient` 全局响应大小保护 | 四个服务通用 | 低 |
| P0 | `qe_experiment_list` 去掉 result_metrics + 降 limit | 使用频率最高 | 低 |
| P0 | `evo_get_task` 增加 summary 模式 | 使用频率最高 | 低 |
| P1 | 新增 `/loops/comparison` 端点 | 跨 loop 分析必需 | 中 |
| P1 | `query_factor_importance` 拆分摘要/详情 | 消除最大 JOIN 浪费 | 中 |
| P1 | `research_get_experiment` 拆分 summary/detail | 消除 6 个无界子查询 | 中 |
| P2 | 所有列表端点降默认 limit | 全面优化 | 低 |
| P2 | `get_module_quality_summary` 后端过滤 | 优化验证中心 | 低 |
| P2 | `list_bugs` compact 模式 | 优化验证中心 | 低 |
| P3 | `fields` 参数投影机制 | 通用能力 | 中 |

---

## 十、关键代码位置索引

| 文件 | 位置 | 说明 |
|------|------|------|
| `scripts/aistock_mcp_common.py:107` | `_decode()` | 全量返回 response.json() |
| `scripts/aistock_qe_experiment_mcp_server.py` | 全文件 | QE 实验 MCP 服务 |
| `scripts/aistock_qe_archive_mcp_server.py` | 全文件 | QE 归档 MCP 服务 |
| `scripts/aistock_mcp_gateway.py` | gateway | Research/Validation MCP 入口 |
| `backend/services/quantevolver/config_composer.py:1941` | `list_experiments()` | 含 result_metrics 的列表 SQL |
| `backend/services/quantevolver/config_composer.py:2070` | `get_experiment_detail()` | SELECT * 查询 |
| `backend/services/quantevolver/qe_evolution_service.py:2708` | `get_task_detail()` | 双查询含所有 loop JSONB |
| `backend/services/quantevolver/qe_evolution_service.py:1066` | 注释 | 承认 enhanced_metrics 过大 |
| `backend/services/qe_archive/repository.py:1526` | `query_factor_importance` | 7 表 JOIN |
| `backend/services/qe_archive/repository.py:1271` | `list_runs` | 默认 limit=100 |
| `backend/services/research_pipeline/service.py:144` | `get_experiment()` | 6 查询合并 |
| `backend/routers/qe_archive.py` | 全文件 | 归档路由 |
| `backend/routers/research_pipeline.py` | 全文件 | 研究管线路由 |
| `backend/routers/validation.py` | 全文件 | 验证中心路由 |
