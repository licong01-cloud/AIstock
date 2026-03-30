# QE 模块待完成工作清单

> 版本: v1.1 | 日期: 2026-03-06
> 基于: QE v2 整改方案（100% 完成）+ QE v3 实施方案（代码 95%+ 完成）+ 数据补齐审查 + 性能/超时分析
> 状态: v2 全部完成，v3 代码开发基本完成，剩余为数据补齐、性能优化、LLM 分析升级和验证

---

## 一、工作总览

| 类别 | 项数 | P0 | P1 | P2 |
|------|------|----|----|----|
| A. QE 演进 Loop 数据补齐 | 2 | 2 | 0 | 0 |
| B. 因子独立指标计算优化 | 5 | 4 | 1 | 0 |
| C. 模型训练数据补齐 | 4 | 3 | 1 | 0 |
| D. 因子 LLM 批量重分析 | 2 | 2 | 0 | 0 |
| E. 模型 LLM 分析升级（双输出） | 4 | 2 | 2 | 0 |
| F. v3 代码遗留 | 2 | 0 | 1 | 1 |
| G. Phase 5 验证 | 14 | 4 | 8 | 2 |
| **合计** | **33** | **17** | **13** | **3** |

---

## 二、执行顺序与详细任务

### 第 1 步：QE 演进 Loop 数据补齐（A）

> 解决用户可见的前端数据空白问题。修复已部署（指标映射 + 增强端点 + DB 回退 + 配置结构化展示），
> 但已入库的历史 Loop 数据需要一次性补齐。

| # | 任务 | 优先级 | 涉及 | 说明 |
|---|------|--------|------|------|
| **A1** | 已入库 Loop 指标映射 SQL 补齐 | **P0** | SQL 脚本 | 已完成的 Loop0/1/2 的 `metrics_json` 只有 QLib 长键名（如 `1day.excess_return_with_cost.information_ratio`），前端查找 `sharpe`/`annualized_return`/`Rank_IC` 短键时显示 "-"。需一次性 SQL UPDATE 补写短键 |
| **A2** | 已入库 Loop 增强诊断回填脚本 | **P0** | Python 脚本 | 已完成 Loop 的 `metrics_json.enhanced_metrics` 为空（当时 RDAgent API 未返回或端点读错文件）。需脚本逐个调用修复后的 enhanced-metrics API，触发数据写入 DB 缓存 |

**A1 实现要点**：
```sql
-- 对 qe_evolution_loops 表中已有的 metrics_json 做 key 映射
UPDATE qe_evolution_loops
SET metrics_json = metrics_json
    || CASE WHEN metrics_json ? 'Rank IC' AND NOT metrics_json ? 'Rank_IC'
            THEN jsonb_build_object('Rank_IC', metrics_json->'Rank IC') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.information_ratio' AND NOT metrics_json ? 'sharpe'
            THEN jsonb_build_object('sharpe', metrics_json->'1day.excess_return_with_cost.information_ratio') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.annualized_return' AND NOT metrics_json ? 'annualized_return'
            THEN jsonb_build_object('annualized_return', metrics_json->'1day.excess_return_without_cost.annualized_return') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.annualized_return' AND NOT metrics_json ? 'annualized_return_with_cost'
            THEN jsonb_build_object('annualized_return_with_cost', metrics_json->'1day.excess_return_with_cost.annualized_return') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.max_drawdown' AND NOT metrics_json ? 'max_drawdown'
            THEN jsonb_build_object('max_drawdown', metrics_json->'1day.excess_return_with_cost.max_drawdown') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.information_ratio' AND NOT metrics_json ? 'sharpe_no_cost'
            THEN jsonb_build_object('sharpe_no_cost', metrics_json->'1day.excess_return_without_cost.information_ratio') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.max_drawdown' AND NOT metrics_json ? 'max_drawdown_no_cost'
            THEN jsonb_build_object('max_drawdown_no_cost', metrics_json->'1day.excess_return_without_cost.max_drawdown') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.mean' AND NOT metrics_json ? 'daily_return'
            THEN jsonb_build_object('daily_return', metrics_json->'1day.excess_return_with_cost.mean') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.mean' AND NOT metrics_json ? 'daily_return_no_cost'
            THEN jsonb_build_object('daily_return_no_cost', metrics_json->'1day.excess_return_without_cost.mean') ELSE '{}'::jsonb END
WHERE status = 'completed' AND metrics_json IS NOT NULL;
```

**A2 实现要点**：
```python
# 遍历所有 completed 且 enhanced_metrics 为空的 Loop
# 对每个 Loop 调用 GET /quantevolver/evolution/tasks/{task_id}/loops/{loop_id}/enhanced-metrics
# API 内部会从 RDAgent 获取数据并缓存到 DB
```

**验证**：打开演进页面 → 已完成 Loop 的 Sharpe/年化/Rank IC 有数值 + 5 个增强 Tab 有图表

---

### 第 2 步：因子独立指标计算优化（B）

> **必须排在因子 LLM 重分析之前**。LLM 分析依赖多持有期 IC、多窗口稳定性等独立指标数据，
> 若指标未计算，LLM 分析时 metrics_block 中这些字段全为 N/A，评级不准确。

**当前问题**：
- `aistock_factor_metrics` 表中大量因子指标未计算（尤其是新同步的 Task 因子）
- 当前 `sync_factor_metrics_batch()` 纯序列化执行，50 个 Task 需 2.5+ 小时
- RDAgent 侧 `ProcessPoolExecutor(max_workers=2)` 限制了并行吞吐
- 批量 API 端点存在但 AIStock 同步层未使用，且端点内部也是序列化的
- **RDAgent Uvicorn 默认 60s 超时**：因子数 > 20 的 Task 计算需 60-90 秒，超时返回 504

**504 超时根因链**：
```
AIStock HTTP client (timeout=1200s) ✅
  → RDAgent Uvicorn (DEFAULT 60s) ← ❌ 瓶颈！
    → ProcessPoolExecutor(max_workers=2)
      → engine.py: 23 factors × 4 windows ≈ 60-90s → 超时
```

| # | 任务 | 优先级 | 涉及文件 | 说明 |
|---|------|--------|---------|------|
| **B1** | RDAgent Uvicorn 超时修复 | **P0** | `start_api.py` | 添加 `--timeout-keep-alive 300` 参数，防止大 Task 504 超时。立即修复已知的 23 因子 Task 报错 |
| **B2** | 因子引擎内部并行计算 | **P0** | `engine.py` | `compute_all_factors_metrics()` 已有 `max_workers=4` 参数但**从未使用**，外层 `for fname in factor_names` 循环 100% 串行。各因子间零数据依赖，使用 `ThreadPoolExecutor(max_workers=4)` 并行化外层循环。23 因子 Task 从 60-90s 降至 ~20s |
| **B3** | AIStock 异步并行因子指标同步 | **P0** | `rdagent_factor_metrics_sync.py`, `rdagent_sync_admin.py` | 新增 `sync_factor_metrics_batch_async()`，使用 `asyncio.Semaphore(4)` 限制并发，按 Task 为单位并行调用 RDAgent 指标 API。新增 SSE 流式端点推送进度 |
| **B4** | RDAgent 侧 API 并行优化 | **P0** | `sota_factors_api.py` | `ProcessPoolExecutor` 扩容 2→8 workers；批量端点 `/v2/batch/factor_metrics` 内部改 for 循环为 `asyncio.gather()` 并行提交 |
| **B5** | 执行全量因子指标计算 | P1 | 运维操作 | 从 `aistock_factor_catalog.source_task_id` 提取所有 Task，使用并行版批量计算。预计 50 个 Task 从 150 分钟降至 30-40 分钟 |

**B1 修复**：
```python
# start_api.py — 修改 Uvicorn 启动参数
# 改前:
uvicorn.run("rdagent.app.api_endpoints.main:app", host="0.0.0.0", port=19723, reload=True)
# 改后:
uvicorn.run("rdagent.app.api_endpoints.main:app", host="0.0.0.0", port=19723, reload=True,
            timeout_keep_alive=300)
```

**B2 引擎内部并行实现**：
```python
# engine.py — compute_all_factors_metrics() 中
# 改前: 串行循环
for fi, fname in enumerate(factor_names):
    # ~150 行计算逻辑 ...
    for window_name, window_spec in EVAL_WINDOWS.items():
        ...

# 改后: ThreadPoolExecutor 并行（max_workers 参数已预留）
from concurrent.futures import ThreadPoolExecutor, as_completed

def _compute_single_factor(fname, factor_mats, fwd_arrs, close_unstacked, dates, ...):
    """单个因子的全部 4 窗口计算（线程安全，只读共享数据）"""
    f_mat_df = factor_mats[fname]
    f_arr_full = f_mat_df.values
    results, reports = [], []
    grp_full = _group_returns_from_matrices(f_arr_full, fwd_arr)
    for window_name, window_spec in EVAL_WINDOWS.items():
        # ... 原有计算逻辑不变 ...
        results.append(result_dict)
        reports.append(report_dict)
    return results, reports

# 主函数中用线程池替代 for 循环
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {
        executor.submit(_compute_single_factor, fname, factor_mats, fwd_arrs, ...): fname
        for fname in factor_names
    }
    for future in as_completed(futures):
        factor_results, factor_reports = future.result()
        all_results.extend(factor_results)
        all_reports.extend(factor_reports)
```

**B2 关键特性**：
- 各因子仅**只读**共享矩阵（`factor_mats`、`fwd_ret_mats`、`close_unstacked`），零写冲突
- numpy/pandas 矩阵运算自动释放 GIL，`ThreadPoolExecutor` 可实现真正并行
- `max_workers=4` 参数已预留在函数签名中，无需改接口
- 23 因子 × 4 窗口 = 92 计算单元，4 线程并行可将单 Task 耗时从 60-90s 降至 **~15-25s**

**B3 核心实现**：
```python
async def sync_factor_metrics_batch_async(
    task_ids: List[str],
    concurrency: int = 4,
) -> AsyncGenerator[Dict, None]:
    """异步并发同步因子指标（SSE 流式返回进度）"""
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0
    total = len(task_ids)

    async def sync_one(task_id: str) -> MetricsSyncResult:
        async with semaphore:
            return await asyncio.to_thread(
                sync_factor_metrics_for_task, task_id
            )

    tasks = [sync_one(tid) for tid in task_ids]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        completed += 1
        yield {
            "type": "progress",
            "task_id": result.task_id,
            "ok": result.ok,
            "current": completed,
            "total": total,
            "metrics_inserted": result.metrics_inserted,
        }

    yield {"type": "done", "total": total, "success": sum(...)}
```

**B4 RDAgent 侧改动**：
```python
# sota_factors_api.py 行 45
# 改前:
_metrics_process_pool = ProcessPoolExecutor(max_workers=2)
# 改后:
_metrics_process_pool = ProcessPoolExecutor(max_workers=8)

# 批量端点改 for → gather
@router.post("/v2/batch/factor_metrics")
async def get_batch_factor_metrics(request: BatchFactorMetricsRequest):
    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(_metrics_process_pool, _compute_task_factor_metrics, tid)
        for tid in request.task_ids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    ...
```

**预期性能提升**：

| 优化层级 | 当前 | 优化后 | 效果 |
|---------|------|--------|------|
| 单 Task 因子计算 (B2) | 23因子串行 60-90s | 4线程并行 15-25s | **3-4x 加速** |
| Task 间并行 (B3+B4) | 1 Task 串行 | 4 Task 并发 | **4x 吞吐** |
| ProcessPool (B4) | max_workers=2 | max_workers=8 | **4x 容量** |
| **综合 50 Task** | **~150 分钟** | **~30-40 分钟** | **~4x 加速** |

**验证**：`SELECT COUNT(DISTINCT factor_name) FROM aistock_factor_metrics WHERE eval_window = 'full'` 应覆盖所有 SOTA 因子

---

### 第 3 步：模型训练数据补齐（C）

> 模型库主表 `aistock_model_catalog` 完全缺失训练过程数据。
> 演进系统内部的 `qe_loop_model_records` 有完整训练数据，但模型选择和分析无法利用历史训练质量。
> **可与第 2 步 B 并行执行**。

**当前数据流缺口**：
```
RD-Agent read_exp_res.py
  → qlib_results_enhanced.json
    ├─ training_diagnostics ──→ ❌ aistock_model_catalog (未同步)
    │                          ✅ qe_loop_model_records  (演进内部用)
    ├─ train_loss_curve     ──→ ❌ aistock_model_catalog
    └─ val_loss_curve       ──→ ❌ aistock_model_catalog
```

| # | 任务 | 优先级 | 涉及文件 | 说明 |
|---|------|--------|---------|------|
| **C1** | `aistock_model_catalog` 新增训练诊断列 | **P0** | `init_catalog_db.py` | 新增 8 个列: `best_epoch`(INT), `total_epochs`(INT), `convergence_ratio`(FLOAT), `overfit_ratio`(FLOAT), `training_failed`(BOOL), `train_loss_final`(FLOAT), `val_loss_final`(FLOAT), `training_curves`(JSONB, 含 train_loss/val_loss 数组) |
| **C2** | Task 同步时提取 SOTA 模型训练数据 | **P0** | `rdagent_model_catalog_sync.py` | 同步 SOTA 模型时从对应 Loop 的 `qlib_results_enhanced.json` 读取 `training_diagnostics`，写入新增列 |
| **C3** | 已入库 SOTA 模型训练数据回填 | **P0** | 新建脚本 | 对已同步的 SOTA 模型，从 RDAgent 侧 `qlib_results_enhanced.json` 提取训练数据回填 |
| **C4** | 模型分析 Agent 注入训练诊断 | P1 | `model_analyst.py` | `_get_model_info()` 查询训练列，LLM prompt 增加训练质量维度（收敛性、过拟合度、最佳 epoch）。**E 节模型 LLM 分析升级的前置** |

**C1 DDL**：
```sql
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS best_epoch INTEGER;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS total_epochs INTEGER;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS convergence_ratio DOUBLE PRECISION;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS overfit_ratio DOUBLE PRECISION;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS training_failed BOOLEAN DEFAULT FALSE;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS train_loss_final DOUBLE PRECISION;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS val_loss_final DOUBLE PRECISION;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS training_curves JSONB;
```

**C2 同步改造要点**：
```python
# rdagent_model_catalog_sync.py 中，同步 SOTA 模型时
# 额外调用 RDAgent enhanced-metrics API 获取 training_diagnostics
# 或直接从已有的 loop metrics 中提取

training_diag = enhanced_data.get("training_diagnostics", {})
model_record["best_epoch"] = training_diag.get("best_epoch")
model_record["total_epochs"] = training_diag.get("total_epochs")
model_record["convergence_ratio"] = training_diag.get("convergence_ratio")
model_record["overfit_ratio"] = training_diag.get("overfit_ratio")
model_record["training_failed"] = training_diag.get("training_failed", False)
model_record["train_loss_final"] = training_diag.get("final_train_loss")
model_record["val_loss_final"] = training_diag.get("final_val_loss")
model_record["training_curves"] = json.dumps({
    "train_loss": training_diag.get("train_loss_curve", []),
    "val_loss": training_diag.get("val_loss_curve", []),
})
```

**验证**：`SELECT model_id, best_epoch, convergence_ratio, overfit_ratio FROM aistock_model_catalog WHERE is_sota = TRUE` 应全部有值

---

### 第 4 步：因子 LLM 批量重分析（D）

> **前置条件**: B5（因子独立指标已计算完毕），否则 LLM 分析时多持有期 IC、多窗口数据全为 N/A。
> 代码已在 v3 Phase 0 中全部完成（异步并发 + SSE 流式 + 8 维度评级 + 因子分类权威定义 + 双重校验）。
> **因子分析已实现双输出**: 人类可读文本（`description`）+ JSON 结构化数据（`factor_profile` JSONB，含分类/评级/维度/IC/Sharpe 等 14 个字段），供 QE 演进 Agent 读取。

| # | 任务 | 优先级 | 说明 |
|---|------|--------|------|
| **D1** | 批量重跑全量因子分析 | **P0** | 使用 `batch_analyze_all_factors_async()` 对 100+ 因子重分析。纠正分类（时序/截面）+ 升级评级（5→8 维度）。异步并发 Semaphore(5)，预计 12 分钟 |
| **D2** | 抽样验证因子分类/评级 | **P0** | 分类验证: 20 个含时间窗口因子（MA5, STD20, ROC10 等）确认为 time_series；评级验证: 对比重跑前后评级变化是否合理；SSE 流式正常 |

**执行方式**：
```bash
# 通过管理 API 触发
POST /api/v1/quantevolver/factor-analyst/batch-analyze-stream
Content-Type: application/json

# 前端打开因子管理页 → 点击"批量分析" → SSE 进度条实时显示
```

**验证**：
- 分类: `SELECT factor_name, factor_dimension FROM qe_factor_classification WHERE factor_dimension = 'time_series'` 应包含 MA/STD/ROC/EMA 类因子
- 评级: grade 分布应合理（S < A < B < C）
- 双输出: `SELECT factor_name, factor_profile FROM qe_factor_classification LIMIT 5` 确认 factor_profile JSONB 非空

---

### 第 5 步：模型 LLM 分析升级 — 双输出（E）— 新增

> **前置条件**: C4（模型分析 Agent 已注入训练诊断数据）。
> **当前问题**: 模型分析 `model_analyst.py` 仅输出 `description`（100-300 字文本），**无结构化 JSON**，
> QE 演进 Agent 无法程序化读取模型质量指标来辅助模型选择和超参调整决策。
>
> **对比因子分析**: `factor_analyst.py` 已实现完整双输出 —— `description`（文本）+ `factor_profile`（JSONB，14+ 字段），
> DB 存储 14 列结构化数据。模型分析需对齐此能力。

**当前模型分析 vs 目标**：

| 能力 | 当前 model_analyst | 目标（对齐 factor_analyst） |
|------|-------------------|---------------------------|
| 文本输出 | `description`（300字） | `description`（300字） |
| JSON 输出 | ❌ 无 | `analysis_profile` JSONB |
| DB 存储字段 | 1 个 (description) | 8+ 个结构化字段 |
| LLM 解析 | 基础字符串提取 | JSON 解析 + 结构化评级 |
| 评级维度 | ❌ 无 | 训练质量/泛化能力/收敛性/稳定性 |
| Agent 可读 | ❌ 仅文本 | JSON 结构化数据 |

| # | 任务 | 优先级 | 涉及文件 | 说明 |
|---|------|--------|---------|------|
| **E1** | `aistock_model_catalog` 新增分析结果列 | **P0** | `init_catalog_db.py` | 新增: `analysis_profile`(JSONB), `model_grade`(VARCHAR), `grade_reason`(TEXT), `training_quality_score`(FLOAT) |
| **E2** | 升级 `analyze_single_model()` 双输出 | **P0** | `model_analyst.py` | LLM prompt 增加训练诊断维度；解析返回 JSON 包含: grade(S/A/B/C/D), 训练质量评分, 收敛性, 过拟合风险, 模型架构评估；同时生成文本 description + JSON profile |
| **E3** | 批量重跑全量模型分析 | P1 | 运维操作 | 使用 `batch_analyze_all_models_async()` 对所有 SOTA 模型重分析。已有 Semaphore(3) 并发 |
| **E4** | 模型分析结果验证 | P1 | 运维验证 | 检查: `SELECT model_id, model_grade, analysis_profile FROM aistock_model_catalog WHERE is_sota = TRUE` 确认双输出完整 |

**E1 DDL**：
```sql
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS analysis_profile JSONB;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS model_grade VARCHAR(2);
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS grade_reason TEXT;
ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS training_quality_score DOUBLE PRECISION;
```

**E2 `analyze_single_model()` 升级要点**：
```python
# 参照 factor_analyst.py 的双输出模式

# 1. _get_model_info() 增加训练诊断字段查询
cur.execute("""
    SELECT model_id, model_name, model_type, display_name,
           ic, annualized_return, max_drawdown, information_ratio,
           is_sota, task_run_id, loop_id,
           hypothesis_text, model_architecture,
           model_hyperparameters, model_training_hyperparameters,
           -- 新增训练诊断字段
           best_epoch, total_epochs, convergence_ratio, overfit_ratio,
           training_failed, train_loss_final, val_loss_final,
           generated_at_utc
    FROM aistock_model_catalog WHERE model_id = %s
""")

# 2. LLM prompt 增加训练质量评估维度
ANALYSIS_PROMPT = """
分析以下量化模型，给出:
1. 模型等级 (S/A/B/C/D)
2. 训练质量评分 (0-100)
3. 评级理由（中文，50字内）
4. 结构化 JSON profile

评估维度:
- 回测表现: IC={ic}, Sharpe={sharpe}, 年化={ann_ret}, 最大回撤={max_dd}
- 训练质量: best_epoch={best_epoch}/{total_epochs}, 收敛率={convergence}, 过拟合率={overfit}
- 损失曲线: train_loss={train_loss}, val_loss={val_loss}
- 模型架构: {architecture}

请以 JSON 格式返回:
{{"grade": "B", "training_quality_score": 72, "grade_reason": "...", "profile": {{...}}}}
"""

# 3. 返回结构对齐 factor_analyst
return {
    "ok": True,
    "model_id": model_id,
    "model_name": model_info.get("model_name"),
    "description": description,       # 人类可读文本
    "model_grade": grade,              # S/A/B/C/D
    "grade_reason": grade_reason,      # 评级理由
    "training_quality_score": score,   # 0-100
    "analysis_profile": profile,       # 完整 JSON（供 Agent 读取）
}

# 4. DB UPDATE 存储双输出
cur.execute("""
    UPDATE aistock_model_catalog
    SET description = %s,
        model_grade = %s,
        grade_reason = %s,
        training_quality_score = %s,
        analysis_profile = %s
    WHERE model_id = %s
""", (description, grade, grade_reason, score, json.dumps(profile), model_id))
```

**验证**：
- 双输出: `SELECT model_id, description, model_grade, analysis_profile FROM aistock_model_catalog WHERE is_sota = TRUE LIMIT 3` 确认 description 有文本 + analysis_profile 有 JSON
- 评级: grade 分布应合理
- Agent 可读: QE 演进 Agent 的 `_build_analysis_context()` 可从 `analysis_profile` 提取结构化数据

---

### 第 6 步：v3 代码遗留（F）

| # | 来源 | 任务 | 优先级 | 说明 |
|---|------|------|--------|------|
| **F1** | v3 1.3 | RDAgent 侧 Loop 完成后发送 webhook | P1 | `qe_evolution_api.py` 中无 webhook 回调。当前靠 60s 定时扫描兜底，功能可用但延迟最大 60s。如果可接受则推迟 |
| **F2** | v3 1.6 | 前端轮询目的注释说明 | P2 | `evolution/page.tsx` 的 10s/15s 轮询处缺少"仅用于 UI 监控，不触发下一轮 Loop"注释。低优先级 |

**F1 实现要点**（如决定实施）：
```python
# qe_evolution_api.py — Loop 完成后回调 AIstock
async def _notify_aistock(task_id: str, loop_id: str, success: bool):
    callback_url = os.getenv("AISTOCK_WEBHOOK_URL", "http://localhost:8001/api/v1/quantevolver")
    secret = os.getenv("QE_WEBHOOK_SECRET", "")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{callback_url}/webhook/loop-completed",
                json={"task_id": task_id, "loop_id": loop_id, "success": success},
                headers={"X-Webhook-Secret": secret},
            )
    except Exception as e:
        logger.warning(f"Webhook callback failed (timer scan will recover): {e}")
```

---

### 第 7 步：Phase 5 验证（G）

| # | 验证项 | 前置 | 优先级 |
|---|--------|------|--------|
| 5.1 | 演进端到端测试（webhook → 下一轮 → 定时扫描兜底） | F1 | P1 |
| 5.2 | 因子分析数据验证（metrics_block 含多持有期 IC + 多窗口） | D1 | **P0** |
| 5.3 | AI 配置生成验证（smart-select prompt 含增强数据） | — | P1 |
| 5.4 | 组合评估验证（评估结果引用因子独立指标） | — | P1 |
| 5.5 | 热重载验证（uvicorn reload 不再挂起） | — | **P0** |
| 5.6 | 入口 D 端到端测试（RDAgent Task → 真实实验 → 演进） | — | P1 |
| 5.7 | 统一目标输入验证（所有入口要求演进目标/指引） | — | P1 |
| 5.8 | 因子分类+评级修复验证 | D1+D2 | **P0** |
| 5.9 | 双重校验机制验证（规则置信度>=2 以规则为准） | D1 | P1 |
| 5.10 | EvolutionFactorAgent 7 步编排器端到端测试 | — | P1 |
| 5.11 | Token 预算验证（3 次 LLM 调用均 < 8000 tokens） | — | P2 |
| 5.12 | 批量分析性能验证（5x 加速 + SSE + beforeunload） | D1 | P1 |
| **5.13** | **模型双输出验证（description + analysis_profile 均非空）** | **E3** | **P0** |
| **5.14** | **因子双输出验证（description + factor_profile 均非空）** | **D1** | **P1** |

---

## 三、依赖关系图

```
A1 (Loop指标映射)     ──→ 前端核心指标显示修复
A2 (Loop增强回填)     ──→ 前端5个诊断Tab数据修复

         ┌── B1 (Uvicorn超时) + B2 (引擎内部并行) ──→ B3 (AIStock并行) + B4 (RDAgent并行) ──→ B5 (全量计算)
         │                                                                                        │
独立并行 ─┤                                                                                        ▼
         │                                                                                 D1 (因子LLM重分析)
         │                                                                                 D2 (因子分析验证)
         │
         └── C1 (模型表DDL) ──→ C2 (同步改造) ──→ C3 (回填)
                                 C4 (分析Agent注入训练诊断)
                                                    │
                                                    ▼
                                              E1 (模型分析DDL)
                                              E2 (双输出升级)
                                                    │
                                                    ▼
                                              E3 (模型LLM重分析)
                                              E4 (模型分析验证)

F1 (webhook) ─── 独立，可延后
F2 (注释) ────── 独立，可延后

                    D + E + F 完成后
                         │
                         ▼
                   G (Phase 5 全量验证)
```

**关键依赖链**：
1. **B1+B2 → B3/B4 → B5 → D1**: 超时修复 + 引擎并行 → API 并行 → 全量计算 → 因子 LLM 重分析
2. **C1 → C2 → C3/C4 → E1 → E2 → E3**: 模型训练数据 → 分析注入 → 双输出升级 → 模型重分析
3. **B 和 C 可并行执行**，D 和 E 各自等待其依赖链完成后可并行执行

---

## 四、资源与时间估算

| 步骤 | 开发时间 | 执行时间 | 说明 |
|------|---------|---------|------|
| A (Loop补齐) | 1 小时 | 5 分钟 | SQL + 简单脚本 |
| B (因子指标优化) | 4-5 小时 | 30-40 分钟 | 超时修复 + 引擎并行 + API 并行改造 + 全量计算 |
| C (模型训练数据) | 3-4 小时 | 10 分钟 | DDL + 同步改造 + 回填脚本 |
| D (因子重分析) | 0（代码已完成） | 12 分钟 | 直接执行 |
| E (模型分析升级) | 3-4 小时 | 15 分钟 | DDL + 双输出改造 + 全量重分析 |
| F (代码遗留) | 1-2 小时 | — | 可选 |
| G (验证) | 3-4 小时 | — | 手动验证（含新增 2 项） |
| **合计** | **~17 小时** | **~75 分钟** | |

---

## 五、已完成工作参考

### v2 整改（100% 完成）

- [x] Bug修复: metrics 404（task_id 拼接错误）
- [x] 实验ID改为日期时间格式 `qe_YYYYMMDD_HHMMSS`
- [x] 统一Workspace结构 `qe_workspace/{task_id}/Loop{N}/`
- [x] 统一实验结果统计（loop_index/parent_experiment_id/is_evolution_loop）
- [x] 实验删除功能（workspace + DB 级联清理）
- [x] rdagent命名清理（文件/类/属性/变量/前端）
- [x] API路由重构（嵌套双参数）
- [x] 数据库迁移（DDL + 历史数据回填）
- [x] QE与RDAgent隔离验证

### v3 实施方案代码完成情况

- [x] Phase 0: 因子分析 Agent 修复（10/10 代码任务完成）
  - [x] 0.1 因子分类权威定义
  - [x] 0.2 双重校验机制
  - [x] 0.3-0.4 多持有期IC + 多窗口指标读取
  - [x] 0.5-0.6 metrics_block 增强 + 8维度评级
  - [x] 0.7-0.10 异步并发 + SSE 流式 + 前端进度
- [x] Phase 1: 演进架构优化（4/6 完成）
  - [x] 1.1 submit_next_loop + process_completed_loop
  - [x] 1.2 webhook 端点
  - [ ] 1.3 RDAgent 侧 webhook 回调 → **F1**
  - [x] 1.4 定时扫描器 60s
  - [x] 1.5 FastAPI lifespan 注册
  - [ ] 1.6 前端注释 → **F2**
- [x] Phase 2: 因子数据扩展（4/4 完成）
- [x] Phase 3: 提示词/LLM 更新（17/17 完成）
  - [x] 三层漏斗（prefilter → screening → deep_select）
  - [x] EvolutionFactorAgent 7 步编排器
- [x] Phase 4: 多入口演进体系（10/11 完成）
  - [x] DB 迁移、SOTA 资产查询、实验创建
  - [x] 前端创建对话框、compose 模式打通
  - [x] 演进目标/指引注入 Agent 提示词

### 本次会话已完成的修复

- [x] 修复 1: 核心指标字段名映射（qe_evolution_service.py）
- [x] 修复 2A: RDAgent 增强指标端点改读 qlib_results_enhanced.json
- [x] 修复 2B: AIstock 代理端点增加 DB 回退
- [x] 修复 3: 实验配置结构化展示（action badge + 因子列表 + 模型信息 + 可折叠JSON）
- [x] 修复 2A 补丁: trade_diagnostics / prediction_diagnostics key 名修正
