# QE 实验模块 v3 实施方案

> 版本: v3.8 | 日期: 2026-03-04 | 更新: Phase 6 合并到 Phase 0，一次性完成所有因子LLM分析功能（数据修复+性能优化+用户体验）
> 基于: v2 已完成的整改（ID改造、workspace统一、删除功能、命名清理、API路由重构）
> 范围: 演进任务架构优化 + 因子数据增强 + AI智能配置升级 + 提示词更新 + 多入口演进体系

---

## 一、背景与目标

v2 整改已完成 12 项核心任务（DB迁移、路由重构、命名清理、前端适配等），系统基础架构已稳定。v3 聚焦四个方向：

1. **演进任务架构优化**：消除 `start_task_loop` 中的 2 秒轮询阻塞，改为事件驱动 + 按需监控
2. **因子数据增强**：将已有的多维度独立指标（多持有期IC、IC衰减、分组单调性等）和 factor_profile JSON 数据注入 AI 决策链路
3. **AI 智能配置与分析升级**：更新 FactorAnalyst、PortfolioArchitect、EvolutionAgents 读取的数据范围和提示词
4. **多入口演进体系**：统一所有演进入口（从QE实验、从手动配置、从AI自动配置、从RDAgent Task SOTA），要求演进目标和指引描述，提供实时日志监控

**注**: 批量分析性能优化（异步并发+SSE流式+进度显示）已合并到 Phase 0，与因子分析数据修复一次性完成。

---

## 二、演进任务架构优化

### 2.1 现状问题

当前 `qe_evolution_service.py` 的 `start_task_loop()` 方法存在以下问题：

```
start_task_loop() 作为 BackgroundTask 启动
  └─ while current_loop < max_loops:
       └─ 提交 Loop 到 RDAgent
       └─ while True:  ← 每 2 秒轮询一次，单 Loop 最多等 8 小时
            └─ await workspace_client.get_loop_status(task_id, loop_id)
            └─ await asyncio.sleep(2)
       └─ 获取 metrics → Agent 分析 → 准备下一轮 config
```

**问题**：
- 2 秒轮询频率过高，单次实验通常运行 30 分钟以上，99.9% 的轮询是无效的
- BackgroundTask 中的 while True 循环阻塞 uvicorn shutdown，导致热重载挂起（TASK 22 已确认）
- 即使无人查看，后台也在持续轮询消耗资源

### 2.2 目标架构：PID文件 + 结果文件事件驱动 + 按需UI监控

将演进任务的 Loop 状态检测改为与单次实验一致的机制（TASK 14 已验证可行）：

```
RDAgent 侧（每个 Loop）：
  启动时 → 创建 pid 文件 (qe_workspace/{task_id}/Loop{N}/run.pid)
  结束时 → 生成结果文件 (qe_workspace/{task_id}/Loop{N}/qlib_results_enhanced.json)
           → 删除 pid 文件

AIstock 侧（演进调度器）：
  提交 Loop → 记录 DB 状态为 running
  不再主动轮询 → 改为被动等待

  触发下一轮的两种方式：
  方式A（推荐）：RDAgent 侧 Loop 完成时回调 AIstock webhook
  方式B（兜底）：AIstock 侧定时任务（如 60 秒间隔）扫描所有 running 状态的演进 Loop

AIstock 前端（UI 监控轮询）：
  打开演进任务监控界面时 → 启动 5 秒间隔的状态轮询
  关闭/离开页面时 → 停止轮询
  轮询目的：实时展示任务进展给用户，不负责触发下一轮 Loop
```

**关键澄清**：前端打开页面时启动的轮询，目的是**实现 UI 对任务进展的实时监控展示**，让用户看到当前 Loop 的运行状态、已完成的 Loop 数量、最新指标等信息。这个轮询**不负责触发下一轮 Loop 任务**，下一轮 Loop 的触发由后端调度器（webhook 回调或定时扫描）独立完成。

### 2.3 方案对比

| 维度 | 当前方案（2秒轮询） | 方案A：Webhook回调 | 方案B：定时扫描 |
|------|---------------------|-------------------|----------------|
| Loop完成检测延迟 | ~2秒 | ~0秒（实时） | ≤60秒 |
| 资源消耗 | 高（每2秒一次HTTP） | 极低（仅完成时1次） | 低（每60秒扫描） |
| 热重载兼容 | ❌ 阻塞shutdown | ✅ 无长轮询 | ✅ 可优雅停止 |
| 实现复杂度 | 低（已有） | 中（需RDAgent侧改动） | 低 |
| 可靠性 | 高（主动检测） | 中（需处理回调失败） | 高（兜底扫描） |

### 2.4 推荐方案：Webhook + 定时扫描双保险

```
┌─────────────┐     Loop完成      ┌──────────────┐
│  RDAgent    │ ──── webhook ────→ │  AIstock     │
│  QE API     │  POST /webhook/   │  演进调度器   │
│             │  loop-completed   │              │
└─────────────┘                   └──────────────┘
                                        │
                                   触发下一轮
                                   Agent分析
                                        │
                                        ▼
                                  ┌──────────────┐
                                  │  定时扫描器   │ ← 每60秒检查一次
                                  │  (兜底)      │    running状态的Loop
                                  └──────────────┘    是否已有结果文件
```

### 2.5 详细设计

#### 2.5.1 RDAgent 侧改动（qe_evolution_api.py）

Loop 执行完成后，回调 AIstock：

```python
# Loop 完成后的回调逻辑（伪代码）
async def on_loop_completed(task_id: str, loop_id: str, success: bool):
    callback_url = os.getenv("AISTOCK_WEBHOOK_URL", "http://localhost:8001/api/v1/quantevolver")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(f"{callback_url}/webhook/loop-completed", json={
                "task_id": task_id,
                "loop_id": loop_id,
                "success": success,
            })
    except Exception as e:
        logger.warning(f"Webhook callback failed: {e}")
        # 失败不影响 Loop 本身，AIstock 侧定时扫描会兜底
```

#### 2.5.2 AIstock 侧新增 Webhook 端点（quantevolver_evolution.py）

```python
@router.post("/webhook/loop-completed")
async def on_loop_completed_webhook(payload: dict):
    """接收 RDAgent 侧 Loop 完成回调，触发后续处理"""
    task_id = payload["task_id"]
    loop_id = payload["loop_id"]
    # 异步触发：获取 metrics → Agent 分析 → 准备下一轮
    asyncio.create_task(_process_completed_loop(task_id, loop_id))
    return {"ok": True}
```

#### 2.5.3 演进调度器重构（qe_evolution_service.py）

将 `start_task_loop` 从"长轮询驱动"改为"事件驱动"：

```python
# 改前：一个巨大的 while True 循环
async def start_task_loop(self, task_id):
    while current_loop < max_loops:
        # 提交 Loop
        # while True: await sleep(2) 轮询  ← 阻塞点
        # 获取 metrics
        # Agent 分析
        # 准备下一轮

# 改后：拆分为独立步骤
async def submit_next_loop(self, task_id):
    """提交下一个 Loop，然后立即返回（不等待完成）"""
    # 组装配置 → 提交到 RDAgent → 更新 DB 状态 → return

async def process_completed_loop(self, task_id, loop_id):
    """Loop 完成后的处理（由 webhook 或定时扫描触发）"""
    # 获取 metrics → Agent 分析 → 更新 DB
    # 如果还有剩余 Loop → 调用 submit_next_loop()
    # 如果已达 max_loops → 标记任务完成
```

#### 2.5.4 定时扫描器（兜底）

```python
# 新增：定时扫描 running 状态的演进 Loop
async def scan_running_loops():
    """每 60 秒扫描一次，检查 running 状态的 Loop 是否已完成"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT task_id, loop_id, loop_index 
                FROM qe_evolution_loops 
                WHERE status = 'running'
            """)
            running_loops = cur.fetchall()
    
    for task_id, loop_id, loop_index in running_loops:
        status = await workspace_client.get_loop_status(task_id, f"Loop{loop_index}")
        if status.get("status") == "completed":
            await process_completed_loop(task_id, f"Loop{loop_index}")
```

#### 2.5.5 前端按需监控轮询（evolution/page.tsx）

```typescript
// 当前已有：每 10 秒刷新任务列表 + 每 15 秒刷新任务详情（如果有选中任务）
// 这个机制保持不变，它的作用是：
// 1. 让用户实时看到演进任务的进展（当前 Loop 编号、状态、最新指标）
// 2. 用户离开页面后自动停止（useEffect cleanup）
// 3. 不负责触发下一轮 Loop（那是后端调度器的职责）
```

### 2.6 实施任务清单

| # | 任务 | 文件 | 说明 |
|---|------|------|------|
| A1 | 拆分 `start_task_loop` 为 `submit_next_loop` + `process_completed_loop` | `qe_evolution_service.py` | 消除 while True 轮询 |
| A2 | 新增 webhook 端点 `/webhook/loop-completed` | `quantevolver_evolution.py` | 接收 RDAgent 回调 |
| A3 | RDAgent 侧 Loop 完成后发送 webhook | `qe_evolution_api.py` | 回调 AIstock |
| A4 | 新增定时扫描器 `scan_running_loops` | `qe_evolution_service.py` | 兜底机制 |
| A5 | 注册定时扫描到 FastAPI lifespan | `main.py` 或 `quantevolver_evolution.py` | 60秒间隔 |
| A6 | 前端演进监控说明注释 | `evolution/page.tsx` | 明确轮询目的是UI监控 |


---

## 三、因子数据增强：现有数据盘点与差距分析

### 3.1 当前因子数据全景

系统中因子相关数据分布在三张表中：

#### 表1：aistock_factor_catalog（因子目录）
```
factor_name, source, expression, description_cn, formula_hint,
factor_type (CrossSection/TimeSeries), data_source (daily_pv/moneyflow/...),
performance_metrics (JSONB), best_performance_sharpe, best_performance_ann_ret,
is_sota_factor, tags (JSONB), variables (JSONB)
```
**特点**：因子基础元数据，含表达式、类型、数据来源，但 performance_metrics 是旧的聚合指标。

#### 表2：aistock_factor_metrics（独立评测指标 — 17+5 项）
```
核心IC指标：
  ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir, ic_positive_ratio

多持有期IC（新增）：
  ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d

纯多头收益指标：
  top_annual_return, top_excess_annual_return, top_sharpe,
  top_max_drawdown, top_excess_sharpe, benchmark_annual_return

辅助指标：
  group_return_monotonicity, turnover, ic_decay_half_life, coverage, n_trading_days

多窗口支持：
  eval_window: full / out_sample / recent_6m / recent_3m
```
**特点**：最丰富的量化指标源，支持多评估窗口，但目前只有 `_get_independent_metrics()` 读取 full 窗口的部分字段。

#### 表3：qe_factor_classification（因子分类分析）
```
category, grade, grade_reason, classification_reason,
ic_value, sharpe_value, ann_ret_value,
llm_analysis, description, factor_dimension,
factor_profile (JSONB) — 包含：
  {
    category, category_reason, grade, grade_reason, dimension,
    metrics_summary: {ic_mean, icir, rank_ic_mean, ...全部独立指标},
    usage_guidance: {
      optimal_holding_period, market_regime_fit,
      complement_categories, conflict_categories,
      combo_role, suggested_weight_range
    },
    risk_notes: [...],
    experiment_track: {total_experiments, avg_ic, best_ic, ...}
  }
```
**特点**：LLM 分析后的结构化结果，factor_profile 是最有价值的综合画像数据。

### 3.2 各 AI 组件当前读取的数据 vs 可用数据

| AI 组件 | 当前读取 | 未读取但可用 |
|---------|---------|-------------|
| **FactorAnalyst._analyze_factor_v2** | ic_mean, icir, rank_ic_mean, rank_icir, ic_positive_ratio, ic_decay_half_life, top_excess_sharpe, top_excess_annual_return, group_return_monotonicity, turnover, coverage, n_trading_days | ❌ 多持有期IC (rank_ic_1d/5d/10d/20d), ❌ ic_csz_mean, ❌ 多窗口数据 (out_sample/recent_6m/recent_3m), ❌ top_annual_return, ❌ top_max_drawdown, ❌ benchmark_annual_return |
| **FactorAnalyst.recommend_factor_combination** | ic_value, sharpe_value, ann_ret_value (来自 qe_factor_classification) | ❌ factor_profile.usage_guidance (互补/冲突类别), ❌ factor_profile.risk_notes, ❌ 多持有期IC, ❌ 相关性矩阵 |
| **PortfolioArchitect._analyze_factors** | category, grade, ic_value, sharpe_value, ann_ret_value | ❌ factor_profile (完整画像), ❌ 独立指标详情, ❌ usage_guidance |
| **PortfolioArchitect._get_factor_details_for_llm** | expression, factor_type, data_source, description_cn, formula_hint | ❌ 独立指标, ❌ factor_profile, ❌ 多持有期IC |
| **PortfolioArchitect._get_factor_metadata_summary** | category, grade, ic_value, sharpe_value, ann_ret_value, description, classification_reason | ❌ factor_profile.usage_guidance, ❌ 独立指标详情 |
| **PortfolioArchitect._generate_with_llm** | 因子按类别汇总(grade分布+代表因子), 模型摘要(IC+年化) | ❌ 因子详细指标, ❌ 因子间相关性, ❌ 多持有期IC |
| **EvolutionAgents.run_analyst** | config (JSON), metrics (JSON), evolution_history (JSON) | ❌ 因子独立指标, ❌ factor_profile, ❌ 因子间相关性 |
| **EvolutionAgents.run_researcher** | analyst_report, sota_status, current_config, evolution_history | ❌ 可用因子库摘要, ❌ 因子独立指标 |

### 3.3 数据增强目标

将以下数据注入 AI 决策链路：

1. **多持有期 IC**：rank_ic_1d/5d/10d/20d — 帮助 AI 判断因子的最佳持有周期
2. **多窗口指标**：out_sample / recent_6m / recent_3m — 帮助 AI 判断因子是否衰退
3. **factor_profile.usage_guidance**：互补/冲突类别、组合角色、建议权重 — 帮助 AI 做因子组合决策
4. **factor_profile.risk_notes**：风险提示 — 帮助 AI 识别组合风险
5. **因子间相关性**：qe_factor_correlations 表 — 帮助 AI 避免选择高度相关的因子
6. **实验历史摘要**：experiment_track — 帮助 AI 了解因子在实际实验中的表现


---

## 四、AI 智能配置升级

### 4.1 FactorAnalyst 升级

#### 4.1.1 `_get_independent_metrics` 扩展

当前只读取 full 窗口的 17 个字段，需扩展为：

```python
# 改前：只读 full 窗口
SELECT ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir, 
       ic_positive_ratio, top_annual_return, top_excess_annual_return,
       top_sharpe, top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
       group_return_monotonicity, turnover, ic_decay_half_life, coverage, n_trading_days
FROM aistock_factor_metrics
WHERE factor_name = %s AND eval_window = 'full'

# 改后：读取 full + 多持有期IC，并额外查询多窗口数据
SELECT ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir,
       ic_positive_ratio, ic_csz_mean,
       rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
       top_annual_return, top_excess_annual_return,
       top_sharpe, top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
       group_return_monotonicity, turnover, ic_decay_half_life, coverage, n_trading_days
FROM aistock_factor_metrics
WHERE factor_name = %s AND eval_window = 'full'
ORDER BY calculated_at DESC LIMIT 1
```

新增方法获取多窗口对比数据：

```python
def _get_multi_window_metrics(self, factor_name: str) -> Optional[Dict]:
    """获取因子在不同评估窗口下的指标对比"""
    # SELECT eval_window, ic_mean, icir, rank_ic_mean, rank_icir
    # FROM aistock_factor_metrics
    # WHERE factor_name = %s AND eval_window IN ('full','out_sample','recent_6m','recent_3m')
    # ORDER BY eval_window
```

#### 4.1.2 `_analyze_factor_v2` 提示词增强

当前 metrics_block 只包含基础指标，需增加：

```python
# 新增到 metrics_block
metrics_block += f"""
## 多持有期IC（因子在不同持有周期的预测能力）
- Rank IC 1日: {_fmt(metrics.get('rank_ic_1d'))}
- Rank IC 5日: {_fmt(metrics.get('rank_ic_5d'))}
- Rank IC 10日: {_fmt(metrics.get('rank_ic_10d'))}
- Rank IC 20日: {_fmt(metrics.get('rank_ic_20d'))}
- IC截面Z-Score: {_fmt(metrics.get('ic_csz_mean'))}

## 多窗口稳定性（因子是否衰退）
- 全量IC: {_fmt(full.get('ic_mean'))} | 样本外IC: {_fmt(oos.get('ic_mean'))}
- 近6月IC: {_fmt(r6m.get('ic_mean'))} | 近3月IC: {_fmt(r3m.get('ic_mean'))}
- 全量ICIR: {_fmt(full.get('icir'))} | 近3月ICIR: {_fmt(r3m.get('icir'))}

## 纯多头收益
- 多头年化: {_fmt(metrics.get('top_annual_return'), True)}
- 多头最大回撤: {_fmt(metrics.get('top_max_drawdown'), True)}
- 基准年化: {_fmt(metrics.get('benchmark_annual_return'), True)}"""
```

#### 4.1.3 系统提示词更新

在 `_get_default_v2_system_prompt` 中增加评级维度：

```
评级维度权重（更新）：
  IC均值 20% + ICIR 15% + 多持有期IC一致性 15% + IC衰减 10% + 
  多窗口稳定性 15% + Sharpe 10% + 分组单调性 10% + 换手率 5%

新增评估要点：
- 多持有期IC：如果 rank_ic_5d > rank_ic_1d，说明因子适合中期持有
- 多窗口稳定性：如果 recent_3m IC 显著低于 full IC，说明因子可能衰退
- IC截面Z-Score：衡量因子在截面上的区分度
```

#### 4.1.4 `recommend_factor_combination` 升级

当前只按 grade 排序选因子，需利用 factor_profile 做更智能的组合：

```python
# 新增逻辑：
# 1. 读取 factor_profile.usage_guidance.complement_categories 和 conflict_categories
# 2. 优先选择互补类别的因子，避免冲突类别
# 3. 读取 qe_factor_correlations 表，避免选择相关性 > 0.7 的因子对
# 4. 考虑多持有期IC，确保组合中包含不同最佳持有期的因子
```

### 4.2 PortfolioArchitect 升级

#### 4.2.1 `_get_factor_details_for_llm` 扩展

当前只读 catalog 表的基础字段，需联查独立指标和 factor_profile：

```python
# 改后：联查三张表
SELECT c.factor_name, c.source, c.expression, c.factor_type,
       c.data_source, c.description_cn, c.formula_hint,
       fc.category, fc.grade, fc.factor_dimension,
       fc.factor_profile,
       m.ic_mean, m.icir, m.rank_ic_mean, m.rank_icir,
       m.rank_ic_1d, m.rank_ic_5d, m.rank_ic_10d, m.rank_ic_20d,
       m.top_excess_sharpe, m.top_excess_annual_return,
       m.group_return_monotonicity, m.ic_decay_half_life
FROM aistock_factor_catalog c
LEFT JOIN qe_factor_classification fc 
  ON c.factor_name = fc.factor_name AND c.source = fc.factor_source
LEFT JOIN LATERAL (
    SELECT * FROM aistock_factor_metrics 
    WHERE factor_name = c.factor_name AND eval_window = 'full'
    ORDER BY calculated_at DESC LIMIT 1
) m ON TRUE
WHERE c.factor_name IN (...)
```

#### 4.2.2 `_llm_evaluate_combination` 提示词增强

当前 factor_details 只包含表达式和类型，需增加：

```python
# 改后的因子详情构建
for name, source in parsed_keys:
    d = detail_map.get(name, {})
    profile = d.get("factor_profile") or {}
    usage = profile.get("usage_guidance", {})
    
    factor_details_parts.append(
        f"- {name} | 来源={source} | 类别={cat_info}"
        f" | IC={d.get('ic_mean', 'N/A'):.4f}"
        f" | ICIR={d.get('icir', 'N/A'):.4f}"
        f" | 最佳持有期={usage.get('optimal_holding_period', 'N/A')}"
        f" | 组合角色={usage.get('combo_role', 'N/A')}"
        f" | 互补类别={usage.get('complement_categories', [])}"
        f" | 风险={profile.get('risk_notes', [])[:2]}"
    )
```

#### 4.2.3 `_generate_with_llm` 提示词增强

当前因子摘要只有 grade 分布和代表因子名，需增加指标摘要：

```python
# 改后的因子库摘要
for cat, cat_factors in factor_metadata["by_category"].items():
    # 现有：grade分布 + 代表因子名
    # 新增：
    avg_ic = sum(f.get("ic_mean", 0) or 0 for f in cat_factors) / max(len(cat_factors), 1)
    avg_icir = sum(f.get("icir", 0) or 0 for f in cat_factors) / max(len(cat_factors), 1)
    
    factor_summary_lines.append(
        f"  {cat}({cat_name}): 共{len(cat_factors)}个因子, {grade_str}. "
        f"平均IC={avg_ic:.4f}, 平均ICIR={avg_icir:.4f}. "
        f"代表因子: {', '.join(top_factors)}"
    )
```

#### 4.2.4 `_get_factor_metadata_summary` 扩展

```python
# 改后：联查独立指标
SELECT fc.factor_name, fc.factor_source, fc.category, fc.grade,
       fc.ic_value, fc.sharpe_value, fc.ann_ret_value,
       fc.description, fc.classification_reason,
       fc.factor_profile,
       m.ic_mean, m.icir, m.rank_ic_mean, m.rank_icir,
       m.rank_ic_5d, m.rank_ic_10d, m.rank_ic_20d,
       m.ic_decay_half_life, m.group_return_monotonicity
FROM qe_factor_classification fc
LEFT JOIN LATERAL (
    SELECT * FROM aistock_factor_metrics
    WHERE factor_name = fc.factor_name AND eval_window = 'full'
    ORDER BY calculated_at DESC LIMIT 1
) m ON TRUE
WHERE fc.grade IS NOT NULL
ORDER BY ...
```

### 4.3 EvolutionAgents 升级

#### 4.3.1 `run_analyst` 数据增强

当前只传入 config + metrics + evolution_history，需增加因子独立指标：

```python
# 改后：在调用 run_analyst 前，为 config 中的每个因子查询独立指标
factor_profiles = {}
for factor_name in config.get("factor_list", []):
    profile = self._get_factor_profile(factor_name)  # 新方法
    if profile:
        factor_profiles[factor_name] = profile

# 传入 analysis_context
analyst_report = await self.agents.run_analyst(
    current_loop, config, metrics,
    analysis_context={"factor_profiles": factor_profiles},
    evolution_history=evolution_history,
)
```

#### 4.3.2 `run_researcher` 数据增强

当前 researcher 只看到 analyst_report + current_config，不知道因子库中还有哪些可用因子。需增加：

```python
# 改后：传入可用因子库摘要
from .factor_analyst import FactorAnalyst
fa = FactorAnalyst()
available_factors_summary = fa.recommend_factor_combination(
    target_count=50, min_grade="C"
)

researcher_context = {
    "available_factors": available_factors_summary,
    "factor_correlations": self._get_relevant_correlations(config.get("factor_list", [])),
}

next_config_draft = await self.agents.run_researcher(
    analyst_report, is_sota, config,
    evolution_history=evolution_history,
    researcher_context=researcher_context,  # 新参数
)
```

#### 4.3.3 提示词更新（qe_agent_prompts 表）

需更新以下提示词记录：

| agent_type | prompt_key | 更新内容 |
|-----------|-----------|---------|
| `evolution_analyst` | `diagnose_experiment` | user_prompt_template 增加 `{factor_profiles}` 占位符，system_prompt 增加"分析每个因子的独立指标表现，识别拖后腿的因子" |
| `evolution_researcher` | `propose_config` | user_prompt_template 增加 `{available_factors}` 和 `{factor_correlations}` 占位符，system_prompt 增加"参考可用因子库，选择与当前因子互补且低相关的替换因子" |
| `evolution_evaluator` | `evaluate_sota` | system_prompt 增加"考虑多持有期IC的一致性，不仅看单一IC指标" |
| `portfolio_architect` | `evaluate_combination` | user_prompt_template 增加因子独立指标和 factor_profile 数据 |
| `portfolio_architect` | `smart_select` | user_prompt_template 增加因子平均IC/ICIR统计 |
| `factor_analyst` | `analyze_factor_v2` | user_prompt_template 增加多持有期IC和多窗口数据块 |

### 4.4 因子分类提示词修复

#### 4.4.1 问题

当前系统提示词（`_get_default_v2_system_prompt`，line 593）对截面/时序的定义仅一句话：
> "判断因子是截面型(cross_sectional)还是时序型(time_series)"

**没有给出任何定义和判断标准**，导致 LLM 将绝大部分因子（包括 MA5、STD20、ROC10 等明显的时序因子）错误分类为截面因子。

规则引擎 `_determine_factor_dimension()` 的判断相对准确，但 **LLM 结果会覆盖规则结果**（line 697）。

#### 4.4.2 修复内容

在 `_get_default_v2_system_prompt` 的第 3 条任务中，替换为带权威定义的版本：

```
3. **维度判断**：判断因子的计算维度是截面型还是时序型。

   定义（按因子信号的计算方式区分，非模型应用方式）：

   ■ 时序因子 (time_series)：
     因子值由同一只股票在 [t-N, t] 时间窗口内的自身历史数据计算。
     判断依据：表达式中包含时间窗口操作（Ref、Mean、Std、Max、Min、
     Sum 等带有天数参数的函数），或使用滞后、滑动窗口、差分等。
     示例：MA($close, 5)、Std($return, 20)、Ref($close, 60)/$close、
     ROC($close, 10)、EMA($volume, 10)

   ■ 截面因子 (cross_sectional)：
     因子值的含义依赖于同一时间点多只股票之间的横向比较/标准化。
     判断依据：表达式中包含显式的截面操作（Rank、CSRank、ZScore、
     Percentile、行业中性化、市场中性化等）。
     示例：Rank(PE)、ZScore(MarketCap)、IndustryNeutral(ROE)

   注意：大多数 Alpha 类因子（使用 Ref/Mean/Std/Max/Min + 天数参数）
   都是时序因子。仅当表达式中有显式截面排名/标准化操作时才判为截面因子。
   即使时序因子在模型中被横向排序使用，因子本身的计算维度仍然是时序的。
```

#### 4.4.3 双重校验机制

LLM 分类结果应与规则引擎结果做一致性校验：

```python
# 在 analyze_single_factor() 中，LLM 返回后增加校验
rule_dimension = _determine_factor_dimension(factor_name, category, code_text, expression)
llm_dimension = v2_result.get("dimension", "time_series")

if rule_dimension != llm_dimension:
    # 规则引擎有明确证据（关键词匹配数 >= 2）时，以规则为准
    # 规则引擎无明确证据时，以 LLM 为准
    logger.warning(f"Factor {factor_name}: rule={rule_dimension}, LLM={llm_dimension}")
    if rule_confidence >= 2:  # 匹配到 2 个以上时序/截面关键词
        factor_dimension = rule_dimension
    else:
        factor_dimension = llm_dimension
```

### 4.5 演进因子 Agent 架构重设计：多步骤编排器

#### 4.5.1 问题

当前 `run_researcher` 是单次 LLM 调用，将所有数据塞进一个 prompt：
- 无法基于上轮结果做精准的因子淘汰/新增决策
- 无法排除历史已用因子
- 总按指标排序选 top-N，低分因子永无机会
- 无法参考 RDAgent TASK 中的历史表现
- Token 膨胀不可控

#### 4.5.2 新架构：EvolutionFactorAgent（7 步编排）

**设计原则**：
- 工具调用（SQL/Python）和 LLM 调用严格分离
- 每次 LLM 调用 < 8000 tokens，聚焦单一任务
- 因子淘汰和新增基于数据驱动的级联筛选
- 加权随机抽样替代 top-N，给低分因子探索机会
- RDAgent TASK 历史作为辅助加权依据

```
Step 1: experiment_analyzer [工具]  → 分析上轮结果，识别淘汰/保留因子
Step 2: direction_decider  [LLM#1] → 决定探索方向（类别/维度/数据源）
Step 3: factor_scout       [工具]  → SQL 搜索 + 加权随机抽样候选因子
Step 4: candidate_screener [LLM#2] → 从候选中筛选 15 个
Step 5: detail_fetcher     [工具]  → 获取 15 个因子的完整数据+相关性
Step 6: combo_designer     [LLM#3] → 设计最终因子组合
Step 7: config_validator   [工具]  → 规则校验配置合法性
```

**Token 预算**：3 次 LLM 调用总计 ~12,000 tokens（每次 < 8000），远优于单次 20,000+。

#### 4.5.3 Step 1: 实验结果分析器（工具调用，无 LLM）

```python
def analyze_experiment_result(prev_config, prev_metrics, prev_prev_metrics, evolution_history):
    """
    纯 Python 分析，不消耗 LLM token。
    """
    # 识别上轮新增的因子
    prev_factors = set(prev_prev_config.get("factor_list", []))
    curr_factors = set(prev_config.get("factor_list", []))
    newly_added = curr_factors - prev_factors
    removed = prev_factors - curr_factors

    # 判断新增因子的效果
    ic_delta = prev_metrics.get("IC", 0) - prev_prev_metrics.get("IC", 0)
    icir_delta = prev_metrics.get("ICIR", 0) - prev_prev_metrics.get("ICIR", 0)

    factors_to_eliminate = []
    if ic_delta < -0.005 or icir_delta < -0.1:
        # 指标下降 → 淘汰上轮新增的因子
        factors_to_eliminate = list(newly_added)

    # 统计当前因子分布
    dimension_dist = Counter()  # {time_series: N, cross_sectional: M}
    category_dist = Counter()   # {MOM: N, VOL: M, ...}
    data_source_dist = Counter() # {daily_pv: N, moneyflow: M, ...}
    for f in curr_factors:
        info = get_factor_classification(f)
        dimension_dist[info["dimension"]] += 1
        category_dist[info["category"]] += 1
        data_source_dist[info["data_source"]] += 1

    # 收集历史已用因子（整个演进任务中所有 Loop 用过的因子）
    all_used_factors = set()
    for loop in evolution_history.get("loops", []):
        all_used_factors.update(loop.get("factor_list", []))

    return {
        "retain_factors": list(curr_factors - set(factors_to_eliminate)),
        "eliminate_factors": factors_to_eliminate,
        "newly_added_last_round": list(newly_added),
        "ic_delta": ic_delta, "icir_delta": icir_delta,
        "dimension_dist": dict(dimension_dist),
        "category_dist": dict(category_dist),
        "data_source_dist": dict(data_source_dist),
        "all_used_factors": list(all_used_factors),  # 排除列表
    }
```

#### 4.5.4 Step 3: 因子搜索器（加权随机 + TASK 历史参考）

```python
def search_candidate_factors(direction, exclude_list, n_total=35, exploration_ratio=0.3):
    """
    加权随机抽样：不是简单 top-N，给低分因子探索机会。
    RDAgent TASK 中曾是 SOTA 因子的，额外加权。
    """
    sql = """
        SELECT fc.factor_name, fc.category, fc.grade,
               fc.factor_dimension, fc.factor_profile->>'data_source' as data_source,
               m.ic_mean, m.icir,
               -- RDAgent TASK 历史：是否曾是 SOTA 因子
               BOOL_OR(c.is_sota_factor) AS was_sota_in_task,
               MAX(c.best_performance_sharpe) AS task_best_sharpe
        FROM qe_factor_classification fc
        LEFT JOIN LATERAL (
            SELECT * FROM aistock_factor_metrics
            WHERE factor_name = fc.factor_name AND eval_window = 'full'
            ORDER BY calculated_at DESC LIMIT 1
        ) m ON TRUE
        LEFT JOIN aistock_factor_catalog c ON c.factor_name = fc.factor_name
        WHERE fc.factor_name NOT IN :exclude_list
          AND fc.grade IS NOT NULL
          AND (:target_categories IS NULL OR fc.category IN :target_categories)
          AND (:target_dimensions IS NULL OR fc.factor_dimension IN :target_dimensions)
        GROUP BY fc.factor_name, fc.category, fc.grade, fc.factor_dimension, m.ic_mean, m.icir
    """
    all_candidates = execute_query(sql, ...)

    # 高分因子池 (S/A/B)
    high_pool = [f for f in all_candidates if f["grade"] in ("S", "A", "B")]
    # 探索因子池 (C/D)
    explore_pool = [f for f in all_candidates if f["grade"] in ("C", "D")]

    n_exploit = int(n_total * (1 - exploration_ratio))
    n_explore = n_total - n_exploit

    # 高分池：按 composite_score 排序取 top
    exploit_picks = sorted(high_pool, key=composite_score, reverse=True)[:n_exploit]

    # 探索池：加权随机（TASK SOTA 因子权重 ×3）
    weights = [3.0 if f["was_sota_in_task"] else 1.0 for f in explore_pool]
    explore_picks = random.choices(explore_pool, weights=weights, k=min(n_explore, len(explore_pool)))

    return exploit_picks + explore_picks
```

#### 4.5.5 实现方式

**不建议使用 MCP**——架构过重，且当前场景不需要外部工具集成。

**推荐方式**：Python 编排器 + 内部工具函数 + litellm 调用

```python
class EvolutionFactorAgent:
    """多步骤演进因子 Agent。每步独立调用工具或 LLM。"""

    async def propose_next_config(self, task_id, current_config, metrics, evolution_history):
        # Step 1: 工具 - 分析实验结果
        analysis = self.analyze_experiment_result(current_config, metrics, evolution_history)

        # Step 2: LLM#1 - 决定探索方向
        direction = await self.decide_direction(analysis, evolution_objective)

        # Step 3: 工具 - 搜索候选因子
        candidates = self.search_candidates(direction, analysis["all_used_factors"])

        # Step 4: LLM#2 - 筛选候选
        shortlist = await self.screen_candidates(candidates, analysis["retain_factors"])

        # Step 5: 工具 - 获取详细数据
        details = self.fetch_factor_details(shortlist)

        # Step 6: LLM#3 - 设计最终组合
        new_factors = await self.design_combination(details, analysis["retain_factors"])

        # Step 7: 工具 - 校验配置
        final_config = self.validate_config(analysis["retain_factors"] + new_factors)

        return final_config
```

这个架构替换现有的单次 `run_researcher` 调用。`run_analyst` 保持不变（只分析当前配置因子，token 可控）。

## 五、多入口演进体系设计

### 5.1 核心设计原则

**所有演进入口统一遵循同一流程：创建真实 QE 实验 → 执行获取基线指标 → 从已完成实验发起演进。**

禁止创建虚拟实验、跳过执行等绕过手段。每个入口的区别仅在于"初始配置从哪里来"，配置就绪后一律走 `compose_experiment()` → 执行 → 完成 → 演进 的标准路径。

### 5.2 现有实现分析

compose 页面（`compose/page.tsx`）已实现双轨设计：
- **AI 智能实验设计**：`smart-select` → 自动生成因子+模型配置
- **手工分步选择**：因子 → 模型 → 策略 → 评估
- **任务分流模式**：`dispatchMode = "independent" | "evolution"`
  - independent：创建实验 → 一键执行回测
  - evolution：创建实验 → 前往演进监控大屏（**当前未打通**）

**当前 evolution 模式的断裂问题**：
- 前端发送 `dispatch_mode` 和 `evolution_params` 到 `config/generate`
- 但后端 `GenerateConfigRequest` 没有这两个字段 → **被静默丢弃**
- evolution 模式生成实验后只显示"前往演进监控大屏"链接，不自动创建演进任务
- 用户到演进页面后仍需手动填写 `base_experiment_id`

v3 需要修复这个断裂。

### 5.3 演进入口全景（修正版）

```
┌──────────────────────────────────────────────────────────────────────┐
│                    所有入口的统一流程                                   │
│                                                                      │
│   配置来源（区别点）                                                   │
│   ├─ A: 选择已有已完成的 QE 实验                                      │
│   ├─ B: compose 页面手动选择因子/模型/策略                             │
│   ├─ C: compose 页面 AI smart-select 生成                             │
│   └─ D: 从 RDAgent Task SOTA 因子/模型构建（新增）                     │
│          │                                                           │
│          ▼                                                           │
│   ┌──────────────────────────────────────────────────────────┐       │
│   │  compose_experiment() → 创建真实 QE 实验 (status=created) │       │
│   └────────────────────────┬─────────────────────────────────┘       │
│                            ▼                                         │
│   ┌──────────────────────────────────────────────────────────┐       │
│   │  执行实验 → RDAgent 回测 → 获取基线指标 (status=completed) │       │
│   └────────────────────────┬─────────────────────────────────┘       │
│                            ▼                                         │
│   ┌──────────────────────────────────────────────────────────┐       │
│   │  从已完成实验发起演进 (base_experiment_id = 真实实验ID)    │       │
│   │  → 填写演进目标/指引 → 创建演进任务 → 自动迭代             │       │
│   └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│   统一运行时能力：                                                     │
│   ✅ SSE 实时日志流  ✅ Loop 拓扑树  ✅ 增强指标看板                    │
│   ✅ SOTA 资产同步   ✅ 演进轨迹总览                                   │
└──────────────────────────────────────────────────────────────────────┘
```

### 5.4 各入口详细说明

#### 入口A：从已有 QE 实验开始（已实现，完善）

- **触发方式**：在实验列表选择已完成实验 → 点击"发起演进"
- **数据来源**：`qe_experiments` 表中的配置（factor_names, model_id, strategy_id 等）
- **实验已存在且已执行**：直接使用 `base_experiment_id` 创建演进任务
- **v3 改动**：增加 `evolution_guidance` 必填字段

#### 入口B：从手动配置开始（已有 UI，需打通后端）

- **触发方式**：compose 页面手工选择因子/模型/策略 → 选择 evolution 分流模式
- **数据来源**：用户在 compose 页面的选择
- **执行流程**：
  1. `compose_experiment()` 创建真实实验（status='created'）
  2. 自动执行该实验（或提示用户手动执行）
  3. 实验完成后，自动创建演进任务（`base_experiment_id` = 该实验）
- **v3 改动**：后端 `GenerateConfigRequest` 增加 `dispatch_mode` + `evolution_params`，打通 evolution 模式

#### 入口C：从 AI smart-select 开始（已有 UI，需打通后端）

- **触发方式**：compose 页面 AI 智能生成 → 选择 evolution 分流模式
- **数据来源**：PortfolioArchitect.generate_from_requirement() 生成的配置
- **执行流程**：与入口B 相同（AI 只是配置来源不同，后续流程完全一致）

#### 入口D：从 RDAgent Task SOTA 开始（新增）

- **触发方式**：选择已同步的 RDAgent Task → 提取 SOTA 因子和模型
- **数据来源**：`aistock_factor_catalog` + `aistock_model_catalog` 中的 SOTA 资产
- **执行流程**：
  1. `get_task_sota_assets()` 查询 SOTA 因子/模型
  2. 调用 `compose_experiment()` 创建**真实** QE 实验（与入口 B/C 完全一致）
  3. 执行该实验获取基线指标
  4. 实验完成后创建演进任务
- **特殊选项**：可选包含 Alpha158 基线因子

### 5.5 入口D 详细设计：从 RDAgent Task SOTA 开始

#### 5.5.1 SOTA 资产查询

```python
async def get_task_sota_assets(task_id: str, include_alpha_baseline: bool = False) -> Dict:
    """从因子/模型目录中提取指定 RDAgent Task 的 SOTA 资产"""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 查询 SOTA 因子
            cur.execute("""
                SELECT c.factor_name, c.source, c.expression, c.factor_type,
                       c.data_source, c.description_cn,
                       m.ic_mean, m.icir, m.rank_ic_mean, m.rank_icir
                FROM aistock_factor_catalog c
                LEFT JOIN LATERAL (
                    SELECT * FROM aistock_factor_metrics
                    WHERE factor_name = c.factor_name AND eval_window = 'full'
                    ORDER BY calculated_at DESC LIMIT 1
                ) m ON TRUE
                WHERE c.source_task_id = %s AND c.is_sota_factor = TRUE
                ORDER BY m.ic_mean DESC NULLS LAST
            """, (task_id,))
            sota_factors = cur.fetchall()

            # 查询 SOTA 模型
            cur.execute("""
                SELECT model_id, model_type, model_name, code_text,
                       model_architecture, model_hyperparameters,
                       ic, annualized_return, sharpe
                FROM aistock_model_catalog
                WHERE source_task_id = %s AND is_sota = TRUE
                ORDER BY ic DESC NULLS LAST LIMIT 1
            """, (task_id,))
            sota_model = cur.fetchone()

            # 可选：Alpha158 基线因子
            alpha_baseline = []
            if include_alpha_baseline:
                cur.execute("SELECT factor_name, expression FROM aistock_alpha158_meta ORDER BY factor_name")
                alpha_baseline = cur.fetchall()

    return {
        "task_id": task_id,
        "sota_factors": sota_factors or [],
        "sota_model": sota_model,
        "alpha_baseline_factors": alpha_baseline,
        "total_factor_count": len(sota_factors) + len(alpha_baseline),
    }
```

#### 5.5.2 创建真实实验（非虚拟实验）

入口D 与入口 B/C 一样，通过 `ConfigComposer.compose_experiment()` 创建真实实验：

```python
async def create_experiment_from_task_sota(
    task_id: str,
    include_alpha_baseline: bool = False,
    data_split: Optional[Dict] = None,
    strategy_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
) -> str:
    """
    从 RDAgent Task SOTA 资产创建真实 QE 实验。
    走标准 compose_experiment() 流程，生成完整的实验文件和 DB 记录。
    """
    assets = await get_task_sota_assets(task_id, include_alpha_baseline)
    if not assets["sota_factors"]:
        raise ValueError(f"Task {task_id} 没有 SOTA 因子")

    # 构建因子列表
    factor_names = [f["factor_name"] for f in assets["sota_factors"]]
    if include_alpha_baseline and assets["alpha_baseline_factors"]:
        baseline_names = [f["factor_name"] for f in assets["alpha_baseline_factors"]]
        factor_names = list(dict.fromkeys(factor_names + baseline_names))

    # 构建模型参数
    model_id = assets["sota_model"]["model_id"] if assets["sota_model"] else None
    model_params = {}
    if assets["sota_model"]:
        model_params = {
            "model_type": assets["sota_model"].get("model_type"),
            "hyperparameters": assets["sota_model"].get("model_hyperparameters"),
        }

    # 调用标准的实验创建流程（与手动/AI生成完全一致）
    from .config_composer import ConfigComposer
    cc = ConfigComposer()
    result = cc.compose_experiment(
        factor_names=factor_names,
        model_id=model_id,
        strategy_id=strategy_id or "TopkDropoutStrategy",
        data_split=data_split,
        custom_params=model_params,
        experiment_name=experiment_name or f"SOTA_{task_id[:12]}",
        evolution_goal=f"从 RDAgent Task {task_id} 的 SOTA 资产开始演进",
    )

    return result["experiment_id"]  # 返回真实实验 ID
```

#### 5.5.3 完整流程（入口D）

```
1. 前端选择 RDAgent Task → 调用 GET /evolution/source-tasks/{id}/preview 预览
2. 用户确认 → 调用 POST /evolution/from-task-sota 创建真实实验
   → create_experiment_from_task_sota() → compose_experiment()
   → 返回 experiment_id (status='created')
3. 自动执行实验 → POST /experiments/{id}/run → RDAgent 回测
   → status: created → running → completed
4. 实验完成后 → 自动创建演进任务 (base_experiment_id = 真实实验ID)
   或通知用户手动到演进页面创建
```

**不需要虚拟实验**：入口D 产出的是一个有真实回测指标的 QE 实验，与入口 A/B/C 产出的实验完全等价。

### 5.6 compose 页面 evolution 模式打通

#### 5.6.1 后端改动

`GenerateConfigRequest` 增加 evolution 相关字段：

```python
class GenerateConfigRequest(BaseModel):
    factor_names: List[str]
    factor_sources: Optional[Dict[str, str]] = None
    model_id: Optional[str] = None
    strategy_id: Optional[str] = None
    data_split: Optional[Dict[str, str]] = None
    custom_params: Optional[Dict[str, Any]] = None
    experiment_name: Optional[str] = None
    # 新增：evolution 模式参数
    dispatch_mode: Optional[str] = Field(None, description="independent | evolution")
    evolution_params: Optional[Dict] = Field(None, description="{loops, objective}")
```

`config/generate` 端点在 `dispatch_mode == "evolution"` 时：
1. 照常调用 `compose_experiment()` 创建实验
2. 自动执行该实验
3. 返回 `experiment_id` + `evolution_pending: true` 标记
4. 前端收到后可自动跳转到演进页面，带上 `base_experiment_id`

#### 5.6.2 前端改动

evolution 模式完成后，不再只是跳转链接，而是：
1. 显示实验执行进度（SSE 日志）
2. 实验完成后，自动弹出演进任务创建对话框，预填 `base_experiment_id`
3. 用户补充演进目标和指引后，一键启动演进

### 5.7 统一演进目标输入

所有四种入口在创建演进任务时，都必须提供：

| 字段 | 说明 | 示例 |
|------|------|------|
| `target_desc` | 演进目标描述（给 Agent 的高层目标） | "提升 ICIR 到 0.08 以上，降低最大回撤到 15% 以内" |
| `evolution_guidance` | 演进指引/策略偏好（给 Agent 的具体指引） | "优先探索树模型深度参数，尝试替换低IC因子，保持换手率在合理范围" |

这两个字段将传递给 EvolutionAgents 的 Analyst 和 Researcher，影响其分析和决策：

```python
# Analyst 提示词中注入
analyst_context = f"""
## 演进目标
{target_desc}

## 演进指引
{evolution_guidance}

请基于以上目标和指引，分析当前实验结果，识别改进方向。
"""

# Researcher 提示词中注入
researcher_context = f"""
## 演进目标
{target_desc}

## 演进指引
{evolution_guidance}

请基于 Analyst 的诊断报告和以上目标指引，提出下一轮配置调整方案。
"""
```

### 5.8 DB 变更

#### 5.8.1 qe_evolution_tasks 表新增列

```sql
ALTER TABLE qe_evolution_tasks ADD COLUMN IF NOT EXISTS evolution_guidance TEXT;
ALTER TABLE qe_evolution_tasks ADD COLUMN IF NOT EXISTS source_type TEXT DEFAULT 'qe_experiment';
ALTER TABLE qe_evolution_tasks ADD COLUMN IF NOT EXISTS source_task_id TEXT;
```

#### 5.8.2 字段说明

| 列名 | 类型 | 说明 |
|------|------|------|
| `evolution_guidance` | TEXT | 演进指引/策略偏好，传递给 Agent |
| `source_type` | TEXT | 来源类型：`qe_experiment` / `manual_config` / `ai_config` / `rdagent_task_sota` |
| `source_task_id` | TEXT | 当 source_type = rdagent_task_sota 时，记录来源的 RDAgent Task ID |

### 5.9 新增 API 端点

#### 5.9.1 查询可用的 RDAgent Task 列表

```python
@router.get("/evolution/source-tasks")
async def list_evolution_source_tasks():
    """
    返回可作为演进起点的 RDAgent Task 列表。
    条件：有 SOTA 因子 且 有 SOTA 模型。
    """
    sql = """
        SELECT 
            c.source_task_id,
            COUNT(DISTINCT c.factor_name) FILTER (WHERE c.is_sota_factor = TRUE) AS sota_factor_count,
            COUNT(DISTINCT c.factor_name) AS total_factor_count,
            (SELECT COUNT(*) FROM aistock_model_catalog m 
             WHERE m.source_task_id = c.source_task_id AND m.is_sota = TRUE) AS sota_model_count,
            (SELECT m2.model_type FROM aistock_model_catalog m2 
             WHERE m2.source_task_id = c.source_task_id AND m2.is_sota = TRUE 
             ORDER BY m2.ic DESC NULLS LAST LIMIT 1) AS best_model_type,
            (SELECT m3.ic FROM aistock_model_catalog m3 
             WHERE m3.source_task_id = c.source_task_id AND m3.is_sota = TRUE 
             ORDER BY m3.ic DESC NULLS LAST LIMIT 1) AS best_model_ic
        FROM aistock_factor_catalog c
        WHERE c.source_task_id IS NOT NULL
        GROUP BY c.source_task_id
        HAVING COUNT(DISTINCT c.factor_name) FILTER (WHERE c.is_sota_factor = TRUE) > 0
        ORDER BY c.source_task_id DESC
    """
    # 返回 task 列表，含 SOTA 因子数、模型类型、最佳IC等摘要信息
```

#### 5.9.2 预览 Task SOTA 资产

```python
@router.get("/evolution/source-tasks/{task_id}/preview")
async def preview_task_sota_assets(
    task_id: str,
    include_alpha_baseline: bool = Query(False),
):
    """
    预览指定 RDAgent Task 的 SOTA 资产详情。
    用于前端展示：SOTA 因子列表（含指标）、SOTA 模型信息、Alpha基线因子数量。
    """
    assets = await get_task_sota_assets(task_id, include_alpha_baseline)
    return {"status": "success", "data": assets}
```

#### 5.9.3 扩展现有创建端点

```python
# 改后的 create_evolution_task 端点
@router.post("/evolution/tasks")
async def create_evolution_task(req: EvolutionTaskCreateRequest):
    """
    创建演进任务（统一入口，所有来源最终都指向一个已完成的真实实验）。
    - source_type=qe_experiment: 直接使用已有已完成实验的 base_experiment_id
    - source_type=rdagent_task_sota: 先从 SOTA 资产创建真实实验 → 执行 → 完成后演进
    """
    if not req.target_desc or not req.evolution_guidance:
        raise HTTPException(400, "演进目标描述和演进指引为必填项")

    if req.source_type == "rdagent_task_sota":
        # 1. 从 SOTA 创建真实实验
        experiment_id = await create_experiment_from_task_sota(
            task_id=req.source_task_id,
            include_alpha_baseline=req.include_alpha_baseline,
            data_split=req.data_split,
            strategy_id=req.strategy_id,
        )
        # 2. 执行该实验获取基线指标
        await run_experiment_and_wait(experiment_id)
        # 3. 使用已完成的真实实验创建演进任务
        req.base_experiment_id = experiment_id

    task_id = await scheduler.create_task(req)
    await scheduler.submit_next_loop(task_id)  # 事件驱动，不用 start_task_loop
    return {"status": "success", "task_id": task_id}
```

> 注意：`run_experiment_and_wait` 是一个需要新实现的方法，提交实验到 RDAgent 并等待完成。
> 对于入口 B/C（compose 页面 evolution 模式），实验执行在 compose 流程中完成，到达演进页面时实验已 completed，直接走 `base_experiment_id` 路径。

### 5.10 前端 UI 变更

#### 5.10.1 演进任务创建对话框改造

当前创建对话框只有 4 个字段（task_name, base_experiment_id, target_desc, max_loops）。需改造为：

```
┌─────────────────────────────────────────────┐
│         新建演进任务                          │
├─────────────────────────────────────────────┤
│                                             │
│  任务名称: [________________________]        │
│                                             │
│  ── 演进起点 ──                              │
│  ○ 从QE实验开始                              │
│    实验ID: [________________________]        │
│  ○ 从RDAgent Task SOTA开始（新增）           │
│    Task: [▼ 下拉选择已同步的Task ___]        │
│    ☑ 包含Alpha158基线因子                    │
│    [预览SOTA资产]                            │
│                                             │
│  ── 演进目标（必填）──                        │
│  目标描述:                                   │
│  [________________________________]          │
│  [________________________________]          │
│                                             │
│  演进指引（必填）:                            │
│  [________________________________]          │
│  [________________________________]          │
│                                             │
│  最大演进轮次: [10]                          │
│                                             │
│  ── SOTA资产预览（选择Task后显示）──          │
│  ┌─────────────────────────────────┐        │
│  │ SOTA因子: 12个                   │        │
│  │ 最佳IC: 0.0523 (factor_xyz)     │        │
│  │ SOTA模型: LGBModel (IC=0.0487)  │        │
│  │ Alpha基线: +158个因子            │        │
│  └─────────────────────────────────┘        │
│                                             │
│              [取消]  [创建并启动演进]          │
└─────────────────────────────────────────────┘
```

#### 5.10.2 前端状态管理变更

```typescript
// 改后的 newTask 状态
const [newTask, setNewTask] = useState({
    task_name: "",
    target_desc: "",
    evolution_guidance: "",  // 新增
    max_loops: 10,
    source_type: "qe_experiment" as "qe_experiment" | "rdagent_task_sota",  // 新增
    base_experiment_id: "",
    source_task_id: "",  // 新增
    include_alpha_baseline: false,  // 新增
});

// 新增：可用 Task 列表
const [sourceTasks, setSourceTasks] = useState<SourceTask[]>([]);
// 新增：SOTA 资产预览
const [sotaPreview, setSotaPreview] = useState<SotaPreview | null>(null);
```

#### 5.10.3 Task 选择下拉框

```typescript
// 获取可用 Task 列表
useEffect(() => {
    if (showCreateTask && newTask.source_type === "rdagent_task_sota") {
        fetch(`${API}/quantevolver/evolution/source-tasks`)
            .then(res => res.json())
            .then(data => setSourceTasks(data.data || []));
    }
}, [showCreateTask, newTask.source_type]);

// 选择 Task 后预览 SOTA 资产
const handleTaskSelect = async (taskId: string) => {
    setNewTask(prev => ({ ...prev, source_task_id: taskId }));
    const res = await fetch(
        `${API}/quantevolver/evolution/source-tasks/${taskId}/preview?include_alpha_baseline=${newTask.include_alpha_baseline}`
    );
    const data = await res.json();
    setSotaPreview(data.data);
};
```

### 5.11 实时日志监控

演进任务的实时日志监控已在 v2 中实现（SSE 流），所有入口创建的演进任务都自动享有：

- **SSE 日志流**：`GET /evolution/tasks/{task_id}/logs` — 已有
- **前端 EventSource 连接**：`createSSE(activeTaskId)` — 已有
- **自动重连机制**：最多 100 次重连，每次间隔 3 秒 — 已有
- **Loop 拓扑树**：实时展示每个 Loop 的状态和 SOTA 标记 — 已有

无需额外开发，新入口创建的任务会自动获得这些能力。


---

## 六、完整实施任务清单

### Phase 0：因子分析 Agent 全面修复（数据基础+性能优化+用户体验，最高优先级）

> **前置理由**：因子分类（截面/时序）和评级（S/A/B/C/D）是所有 AI 决策的数据基础。
>
> **分类问题**：当前绝大部分因子被错分为截面因子（如 MA5、STD20、ROC10 等明显的时序因子），导致下游所有依赖 `factor_dimension` 的模块基于错误数据运行。
>
> **评级问题**：当前评级维度仅 5 项（IC 30% + ICIR 25% + IC衰减 15% + Sharpe 15% + 分组单调性 15%），缺少多持有期IC一致性、多窗口稳定性、换手率等关键维度，且 LLM 分析时未注入多持有期IC和多窗口数据。
>
> **性能问题**：当前批量分析是同步阻塞函数，1000个因子需要60分钟，用户体验差且长时间占用线程。
>
> **合并理由**：将数据修复（0.1-0.6）、性能优化（0.7-0.10）、批量重跑（0.11）一次性完成，避免重复 LLM 调用成本，且批量重跑直接使用优化后的异步并发版本（60分钟→12分钟）。
>
> 此 Phase **无前置依赖**，可立即启动，与 Phase 1 并行。

| # | 任务 | 文件 | 优先级 | 说明 |
|---|------|------|--------|------|
| 0.1 | 修复 `_get_default_v2_system_prompt` 因子分类定义 | `factor_analyst.py` | **P0** | 增加截面/时序因子权威定义和判断标准（见 4.4.2） |
| 0.2 | 实现因子分类双重校验机制 | `factor_analyst.py` | **P0** | LLM 分类结果与规则引擎交叉校验，规则置信度≥2时以规则为准（见 4.4.3） |
| 0.3 | 扩展 `_get_independent_metrics` 读取多持有期IC | `factor_analyst.py` | **P0** | 增加 ic_csz_mean, rank_ic_1d/5d/10d/20d（原 Phase 2 任务 2.1） |
| 0.4 | 新增 `_get_multi_window_metrics` 方法 | `factor_analyst.py` | **P0** | 读取 out_sample/recent_6m/recent_3m 窗口数据（原 Phase 2 任务 2.2） |
| 0.5 | 更新 `_analyze_factor_v2` 的 metrics_block | `factor_analyst.py` | **P0** | 注入多持有期IC、多窗口稳定性、纯多头收益数据块（原 Phase 3 任务 3.1） |
| 0.6 | 更新 `_get_default_v2_system_prompt` 评级维度 | `factor_analyst.py` | **P0** | 5维度→8维度：IC 20% + ICIR 15% + 多持有期IC一致性 15% + IC衰减 10% + 多窗口稳定性 15% + Sharpe 10% + 分组单调性 10% + 换手率 5%（原 Phase 3 任务 3.2） |
| 0.7 | 实现异步并发批量分析 | `factor_analyst.py` | **P0** | 新增 `batch_analyze_all_factors_async()` 方法：使用 `asyncio.Semaphore(5)` 限制并发，`asyncio.gather()` 并发执行，每个因子用 `asyncio.to_thread()` 包装，`yield` 流式返回进度（5倍速度提升：60分钟→12分钟） |
| 0.8 | 修改批量分析API为SSE流式 | `quantevolver.py` | **P0** | `/factor-analyst/batch-analyze` 改为返回 `StreamingResponse`，content_type='text/event-stream'，实时推送进度事件（processed/total/current_factor/error） |
| 0.9 | 模型批量分析同步改造 | `model_analyst.py` | P0 | 同步改造 `batch_analyze_all_models()` 为异步并发+SSE流式模式 |
| 0.10 | 前端SSE进度显示 | `quantevolver/factors/page.tsx` | P0 | 批量分析按钮点击后显示进度对话框，使用 EventSource 接收SSE事件，实时更新进度条和当前处理因子，完成后刷新列表。**增加 beforeunload 监听**：分析进行中时阻止页面刷新/关闭（弹窗提醒），完成后自动移除监听器 |
| 0.11 | 批量重跑因子分析 | 运维操作 | **P0** | 0.1-0.10 全部完成后，对全量因子（100+）使用优化后的异步并发版本重新执行分析，一次性纠正分类 + 评级（耗时从60分钟降至12分钟） |
| 0.12 | 抽样验证 | 运维操作 | P0 | 分类验证：20个含时间窗口因子确认为 time_series；评级验证：对比重跑前后评级变化是否合理；性能验证：确认异步并发和SSE流式正常工作 |

### Phase 1：演进任务架构优化（消除轮询阻塞）

| # | 任务 | 文件 | 优先级 | 说明 |
|---|------|------|--------|------|
| 1.1 | 拆分 `start_task_loop` 为事件驱动架构 | `qe_evolution_service.py` | P0 | 拆为 `submit_next_loop()` + `process_completed_loop()`，消除 while True 轮询 |
| 1.2 | 新增 webhook 端点 `/webhook/loop-completed` | `quantevolver_evolution.py` | P0 | 接收 RDAgent 回调 |
| 1.3 | RDAgent 侧增加 Loop 完成回调 | `qe_evolution_api.py` | P0 | Loop 执行完成后 POST 回调 AIstock |
| 1.4 | 新增定时扫描器（兜底） | `qe_evolution_service.py` | P1 | 每 60 秒扫描 running 状态的 Loop，处理 webhook 丢失的情况 |
| 1.5 | 注册定时扫描到 FastAPI lifespan | `main.py` | P1 | 使用 asyncio.create_task + graceful shutdown |
| 1.6 | 前端监控轮询注释说明 | `evolution/page.tsx` | P2 | 明确注释：页面轮询目的是 UI 实时监控展示，不触发下一轮 Loop |

### Phase 2：因子数据读取扩展（PortfolioArchitect / EvolutionAgents 侧）

> **注意**：`factor_analyst.py` 侧的数据查询扩展（`_get_independent_metrics`、`_get_multi_window_metrics`）已提升到 Phase 0（0.3-0.4）。本 Phase 聚焦其他 AI 组件的数据读取。

| # | 任务 | 文件 | 优先级 | 说明 |
|---|------|------|--------|------|
| 2.1 | 扩展 `_get_factor_details_for_llm` 联查独立指标 | `portfolio_architect.py` | P0 | LEFT JOIN aistock_factor_metrics + qe_factor_classification |
| 2.2 | 扩展 `_get_factor_metadata_summary` 联查独立指标 | `portfolio_architect.py` | P0 | 增加 ic_mean, icir, rank_ic_5d/10d/20d 等 |
| 2.3 | 新增 `_get_factor_profile` 方法 | `qe_evolution_service.py` | P1 | 为演进 Agent 提供因子画像数据 |
| 2.4 | 新增 `_get_relevant_correlations` 方法 | `qe_evolution_service.py` | P1 | 查询当前因子组合的相关性矩阵 |

### Phase 3：提示词与 LLM 调用更新

> **注意**：`factor_analyst.py` 的 metrics_block 和评级维度更新已提升到 Phase 0（0.5-0.6）。本 Phase 聚焦 PortfolioArchitect / EvolutionAgents 侧的提示词更新、三层漏斗和 EvolutionFactorAgent。

| # | 任务 | 文件 | 优先级 | 说明 |
|---|------|------|--------|------|
| 3.1 | 更新 `_llm_evaluate_combination` 因子详情构建 | `portfolio_architect.py` | P0 | 增加 IC/ICIR/最佳持有期/组合角色/互补类别/风险 |
| 3.2 | 更新 `_generate_with_llm` 因子库摘要 | `portfolio_architect.py` | P1 | 增加平均IC/ICIR统计 |
| 3.3 | 更新 `recommend_factor_combination` 选择逻辑 | `factor_analyst.py` | P1 | 利用 usage_guidance 互补/冲突类别 + 相关性过滤 |
| 3.4 | 更新 `run_analyst` 传入 factor_profiles | `qe_evolution_service.py` | P0 | 为 Analyst Agent 提供因子独立指标 |
| 3.5 | 更新 `run_researcher` 传入可用因子库 | `qe_evolution_service.py` | P1 | 为 Researcher Agent 提供因子库摘要和相关性 |
| 3.6 | 更新 DB 中 6 条提示词记录 | `qe_agent_prompts` 表 | P0 | 见第四节 4.3.3 的提示词更新清单 |
| 3.7 | `_prefilter_factors` 规则预筛方法 | `factor_analyst.py` | P0 | 三层漏斗第0层：SQL 级预筛 grade/IC/coverage 过滤，每类别 top N |
| 3.8 | `_build_screening_prompt` + `_llm_screening` 轻量筛选 | `portfolio_architect.py` | P0 | 三层漏斗第1层：摘要数据 (~2000 tokens) → LLM 选候选 |
| 3.9 | `_llm_deep_select` 精选组合方法 | `portfolio_architect.py` | P0 | 三层漏斗第2层：完整数据+相关性 (~8000 tokens) → LLM 精选 |
| 3.10 | `run_researcher` 改为两步漏斗 | `qe_evolution_agents.py` | P0 | 先摘要→方向，再候选详情→具体选择 |
| 3.11 | `_get_factor_library_summary` 轻量摘要方法 | `qe_evolution_service.py` | P0 | 每因子仅 name+category+grade+IC+ICIR (~30 tokens) |
| 3.12 | `EvolutionFactorAgent` 核心编排器类 | `qe_evolution_agents.py` | **P0** | 7步编排框架：`propose_next_config` 方法（见 4.5.2-4.5.5） |
| 3.13 | `analyze_experiment_result` 工具函数 | `qe_evolution_agents.py` | P0 | Step 1: 纯Python分析上轮结果，识别淘汰/保留因子，收集历史已用因子排除列表 |
| 3.14 | `search_candidate_factors` 加权随机搜索 | `qe_evolution_agents.py` | P0 | Step 3: 70%高分exploit + 30%低分explore，RDAgent TASK SOTA因子×3权重 |
| 3.15 | 3个LLM调用方法 (direction/screening/combo) | `qe_evolution_agents.py` | P0 | Step 2/4/6: 每次<8000 tokens，聚焦单一任务 |
| 3.16 | `validate_config` 配置校验工具 | `qe_evolution_agents.py` | P1 | Step 7: 规则校验因子数量、类别分布、相关性阈值 |
| 3.17 | 替换 `run_researcher` 为 `EvolutionFactorAgent` | `qe_evolution_service.py` | P0 | 在 process_completed_loop 中用新 Agent 替换单次 LLM 调用 |

### Phase 4：多入口演进体系（新增）

| # | 任务 | 文件 | 优先级 | 说明 |
|---|------|------|--------|------|
| 4.1 | DB 迁移：qe_evolution_tasks 新增列 | `init_catalog_db.py` | P0 | 新增 evolution_guidance, source_type, source_task_id |
| 4.2 | 新增 `get_task_sota_assets` 方法 | `qe_evolution_service.py` | P0 | 从 catalog 表查询指定 Task 的 SOTA 因子和模型 |
| 4.3 | 新增 `create_experiment_from_task_sota` 方法 | `qe_evolution_service.py` | P0 | 调用 compose_experiment() 创建**真实**实验（非虚拟实验） |
| 4.4 | 扩展 `EvolutionTaskCreateRequest` | `quantevolver_evolution.py` | P0 | 增加 source_type, source_task_id, include_alpha_baseline, evolution_guidance |
| 4.5 | 扩展 `create_task` 方法支持多来源 | `qe_evolution_service.py` | P0 | 入口D: 创建真实实验→执行→完成后演进（统一流程） |
| 4.6 | 后端 `GenerateConfigRequest` 增加 dispatch_mode | `quantevolver.py` | P0 | 打通 compose 页面 evolution 模式的后端处理 |
| 4.7 | 新增 `GET /evolution/source-tasks` 端点 | `quantevolver_evolution.py` | P0 | 返回可作为演进起点的 RDAgent Task 列表 |
| 4.8 | 新增 `GET /evolution/source-tasks/{id}/preview` 端点 | `quantevolver_evolution.py` | P1 | 预览 Task SOTA 资产详情 |
| 4.9 | 前端：演进创建对话框增加 RDAgent Task 入口 | `evolution/page.tsx` | P0 | Task 下拉、SOTA 预览、演进目标/指引必填 |
| 4.10 | 前端：compose evolution 模式完善 | `compose/page.tsx` | P1 | 实验执行完成后自动弹出演进创建对话框 |
| 4.11 | 演进目标/指引注入 Agent 提示词 | `qe_evolution_service.py` | P0 | 在 run_analyst/run_researcher 调用时注入 target_desc 和 evolution_guidance |

### Phase 5：验证

| # | 任务 | 说明 |
|---|------|------|
| 5.1 | 演进任务端到端测试 | 创建实验 → 完成 → 发起演进 → 验证 webhook 触发下一轮 → 验证定时扫描兜底 |
| 5.2 | 因子分析数据验证 | 已纳入 Phase 0.8（验证 metrics_block 包含多持有期IC和多窗口数据） |
| 5.3 | AI 配置生成验证 | 调用 smart-select API，验证 LLM 收到的 prompt 包含增强数据 |
| 5.4 | 组合评估验证 | 调用 evaluate-portfolio API，验证评估结果引用了因子独立指标 |
| 5.5 | 热重载验证 | 修改 quantevolver.py → 验证 uvicorn 可正常 reload（不再挂起） |
| 5.6 | 入口D端到端测试 | 选择 RDAgent Task → 创建真实实验 → 执行获取基线 → 创建演进 → 验证初始配置正确 → 验证演进正常运行 |
| 5.7 | 统一目标输入验证 | 验证所有入口都要求填写演进目标和指引，且 Agent 提示词中包含这些内容 |
| 5.8 | 因子分类+评级修复验证 | 已纳入 Phase 0.8（分类：时序因子不再错分；评级：8维度体系生效） |
| 5.9 | 双重校验机制验证 | 构造 LLM 分类与规则分类不一致的 case → 验证规则置信度≥2时以规则为准 |
| 5.10 | EvolutionFactorAgent 端到端测试 | 启动演进任务 → 验证 7 步编排器正常执行 → 验证因子排除列表有效 → 验证加权随机抽样命中低分因子 |
| 5.11 | Token 预算验证 | 检查 EvolutionFactorAgent 3次 LLM 调用的 token 用量均 < 8000 |
| 5.12 | 批量分析性能验证 | 已纳入 Phase 0.12（验证异步并发5倍速度提升、SSE流式进度正常、beforeunload弹窗提醒生效） |


---

## 七、涉及修改的文件清单

### AIstock 后端

| 文件 | 改动要点 |
|------|----------|
| `services/quantevolver/qe_evolution_service.py` | 拆分 start_task_loop、新增 submit_next_loop/process_completed_loop、新增 get_task_sota_assets/create_experiment_from_task_sota、新增 _get_factor_profile/_get_relevant_correlations、更新 run_analyst/run_researcher 调用、扩展 create_task 支持多来源 |
| `services/quantevolver/factor_analyst.py` | 扩展 _get_independent_metrics、新增 _get_multi_window_metrics、更新 _analyze_factor_v2 metrics_block、更新系统提示词、升级 recommend_factor_combination、**修复因子分类定义**（增加截面/时序权威定义）、**新增双重校验机制**（LLM ↔ 规则引擎交叉验证）、新增 _prefilter_factors 规则预筛、**新增 batch_analyze_all_factors_async 异步并发方法（yield流式返回进度）** |
| `services/quantevolver/model_analyst.py` | **新增 batch_analyze_all_models_async 异步并发+SSE流式方法** |
| `services/quantevolver/portfolio_architect.py` | 扩展 _get_factor_details_for_llm、扩展 _get_factor_metadata_summary、更新 _llm_evaluate_combination、更新 _generate_with_llm |
| `services/quantevolver/qe_evolution_agents.py` | run_analyst 增加 factor_profiles 参数处理、**新增 EvolutionFactorAgent 类**（7步编排器，替换 run_researcher 单次调用）、新增 analyze_experiment_result/search_candidate_factors/validate_config 工具函数、新增 3 个独立 LLM 调用方法 |
| `routers/quantevolver.py` | **修改 /factor-analyst/batch-analyze 为SSE流式**（返回 StreamingResponse）、**同步改造 /model-analyst/batch-analyze 为SSE流式** |
| `routers/quantevolver_evolution.py` | 扩展 EvolutionTaskCreateRequest、新增 webhook 端点、新增 source-tasks 和 preview 端点、注册定时扫描 |
| `init_catalog_db.py` | qe_evolution_tasks 新增 evolution_guidance/source_type/source_task_id 列 |
| DB `qe_agent_prompts` 表 | 更新 6 条提示词记录 |

### AIstock 前端

| 文件 | 改动要点 |
|------|----------|
| `evolution/page.tsx` | 改造创建对话框为多入口模式、增加 Task 下拉选择、SOTA 预览、演进目标/指引必填、增加注释说明轮询目的 |
| `quantevolver/factors/page.tsx` | **批量分析按钮改造**：点击后显示进度对话框，使用 EventSource 接收SSE事件，实时更新进度条和当前处理因子，完成后刷新列表 |

### RDAgent 侧

| 文件 | 改动要点 |
|------|----------|
| `qe_evolution_api.py` | Loop 完成后发送 webhook 回调 |

---

## 八、风险与注意事项

1. **Webhook 可靠性**：网络抖动可能导致回调丢失，定时扫描器是必要的兜底机制
2. **提示词更新**：更新 DB 中的提示词记录需要编写迁移脚本或通过管理界面操作，不能直接改代码中的兜底提示词
3. **多表联查性能**：`_get_factor_details_for_llm` 联查三张表，需确保 factor_name 上有索引（已有）
4. **向后兼容**：factor_profile 可能为 NULL（旧因子未经 LLM 分析），所有读取处需做空值保护
5. **LLM Token 消耗**：增加的数据会增加 prompt 长度，需监控 token 用量，必要时对因子数量做截断
6. **SOTA 因子/模型缺失**：部分 RDAgent Task 可能没有标记 SOTA 的因子或模型，前端需做空状态提示
7. **Alpha 基线因子冲突**：如果 SOTA 因子中已包含与 Alpha158 同名的因子，混入基线时需去重
8. **入口D 实验执行等待**：从 SOTA 创建的真实实验需要执行完成后才能开始演进，等待时间取决于回测耗时（通常 15-60 分钟），需在前端给出明确进度提示

---

## 九、方案审查：问题、隐患与改进建议

> 以下为基于代码层分析发现的方案问题，按严重程度排序。

### 9.1 架构设计问题

#### 问题 1：Webhook + 定时扫描竞态条件 [严重]

**位置**：Section 2.5.2 + 2.5.4

**问题**：`process_completed_loop(task_id, loop_id)` 可被 webhook 和 timer scan 同时触发。两者都能检测到同一个已完成的 Loop，导致：
- 重复调用 Agent 分析（浪费 LLM token）
- 重复提交下一轮 Loop（创建两个并行的 Loop N+1）
- DB 状态不一致

**当前代码无保护**：`qe_evolution_service.py` 中只有 `_running_tasks` 集合保护 task 级别，没有 loop 级别的幂等保护。

**修复方案**：在 `process_completed_loop` 入口使用 DB 级别的 CAS（Compare-And-Set）操作：

```python
async def process_completed_loop(self, task_id: str, loop_id: str):
    """Loop 完成后的处理（幂等：只有第一个调用者能处理）"""
    # CAS: 只有 status='running' → 'processing' 成功的调用者继续
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qe_evolution_loops
                SET status = 'processing'
                WHERE task_id = %s AND loop_id = %s AND status = 'running'
                RETURNING loop_id
            """, (task_id, loop_id))
            if not cur.fetchone():
                logger.info(f"Loop {loop_id} already being processed, skip")
                return  # 另一个触发源已在处理
        conn.commit()

    try:
        # 获取 metrics → Agent 分析 → 更新 DB → 提交下一轮
        ...
        # 最终标记为 completed
    except Exception as e:
        # 标记为 failed，允许 timer scan 重试
        ...
```

#### 问题 2：Webhook Handler fire-and-forget 异常丢失 [严重]

**位置**：Section 2.5.2

**问题**：`asyncio.create_task(_process_completed_loop(...))` 的异常会被静默吞掉。如果 Agent 分析或下一轮提交失败，无日志无告警，Loop 卡在中间状态。

**修复方案**：用 `asyncio.create_task` + `add_done_callback` 捕获异常：

```python
@router.post("/webhook/loop-completed")
async def on_loop_completed_webhook(payload: LoopCompletedPayload):
    task = asyncio.create_task(
        scheduler.process_completed_loop(payload.task_id, payload.loop_id)
    )
    task.add_done_callback(_log_task_exception)  # 异常日志回调
    return {"ok": True}

def _log_task_exception(task: asyncio.Task):
    if task.exception():
        logger.error(f"process_completed_loop failed: {task.exception()}", exc_info=task.exception())
```

#### 问题 3：compose 页面 evolution 模式前后端断裂 [中等]

**位置**：Section 5.6（compose/page.tsx + quantevolver.py）

**问题**：前端 compose 页面已实现 `dispatchMode = "evolution"` 模式和参数输入（loops, objective），但：
- 后端 `GenerateConfigRequest` 没有 `dispatch_mode` 和 `evolution_params` 字段 → 被静默丢弃
- evolution 模式生成实验后只显示"前往演进监控大屏"链接，不自动创建演进任务
- 用户到演进页面后仍需手动填写 base_experiment_id

**修复方案**：已在 Section 5.6 中给出——后端增加字段处理 + 前端实验完成后自动弹出演进创建对话框。

#### 问题 4：create_evolution_task 端点与架构目标矛盾 [中等]

**位置**：Section 5.6.3（第 947 行伪代码）

**问题**：方案目标是消除 `start_task_loop` 的 while-True 轮询，但新增端点仍调用 `background_tasks.add_task(scheduler.start_task_loop, task_id)`。

**修复方案**：新端点应调用 `submit_next_loop`：

```python
@router.post("/evolution/tasks")
async def create_evolution_task(req: EvolutionTaskCreateRequest):
    task_id = await scheduler.create_task(req)
    # 改为：提交第一轮 Loop（不阻塞，立即返回）
    await scheduler.submit_next_loop(task_id)
    return {"status": "success", "task_id": task_id}
```

#### 问题 5：入口 B/C 未实现 [低]

**位置**：Section 5.3.3

**问题**：`EvolutionTaskCreateRequest.source_type` 定义了 4 种值（`qe_experiment`, `manual_config`, `ai_config`, `rdagent_task_sota`），但 `create_task` 方法只实现了 `qe_experiment` 和 `rdagent_task_sota` 的处理逻辑。`manual_config` 和 `ai_config` 只有定义没有实现。

**建议**：v3 范围内至少补充 `manual_config`（直接传 JSON config）的实现；`ai_config` 可归入 Phase 4 后续。或者在方案中明确标注 B/C 入口为 v3.x 后续迭代。

### 9.2 缺失的关键机制

#### 问题 6：失败处理策略 — 请求级重试 vs Loop 级重试 [中等]

**当前代码**（`qe_evolution_service.py` line 491-498）：一个 Loop 失败 → 整个任务标记 `failed` → 所有后续 Loop 取消。

**分析结论：Loop 级重试无业务价值。** 原因：

| 失败类型 | 重试价值 | 正确处理方式 |
|----------|----------|-------------|
| RDAgent HTTP 错误/超时 | 无（同样配置同样结果） | httpx 客户端层请求级重试 |
| RDAgent Loop 训练失败 | 无（同样代码同样数据） | 记录原因，人工诊断后 `/resume` |
| LLM API 限流/宕机 | 无（应在调用层处理） | litellm 内置重试 + 指数退避 |
| LLM 输出格式错误 | 不确定（根因是 prompt 质量） | 改善 prompt，不盲目重试 |
| 配置/提示词缺失 | 无（配置问题不会自修复） | 修复配置后手动 `/resume` |

**建议**：
1. 在 `workspace_client`（httpx）层增加请求级重试（3 次 + 指数退避）
2. 在 LLM 调用层确认 litellm 的内置重试已启用
3. Loop 失败时记录**详细失败原因和上下文**到 `qe_evolution_loops.error_detail`（新增 TEXT 列），便于诊断
4. 保留现有 `/tasks/{task_id}/resume` 端点作为人工恢复入口
5. 不做 Loop 级自动重试

#### 问题 7：定时扫描器无优雅停机实现 [中等]

**位置**：Section 2.5.4 + Task A5

**问题**：方案提到注册到 FastAPI lifespan，但没有具体的取消实现。

**建议补充**：

```python
# main.py lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    shutdown_event = asyncio.Event()
    scan_task = asyncio.create_task(_timer_scan_loop(shutdown_event))
    yield
    shutdown_event.set()  # 通知停止
    scan_task.cancel()
    try:
        await scan_task
    except asyncio.CancelledError:
        pass

async def _timer_scan_loop(shutdown: asyncio.Event):
    while not shutdown.is_set():
        await scan_running_loops()
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=60)
        except asyncio.TimeoutError:
            continue  # 60秒未收到停机信号，继续扫描
```

#### 问题 8：data_split 硬编码默认值 [低]

**位置**：Section 5.3.2 `build_config_from_task_sota`

**问题**：默认日期范围 `2008-01-01 ~ 2025-12-31` 硬编码在代码中。应从配置文件或 RDAgent Task 的原始设置继承。

**建议**：读取 `.env` 或系统配置中的默认 data_split，或从 RDAgent Task 的 conf.yaml 导出时一并记录。

### 9.3 安全问题

#### 问题 9：Webhook 端点无认证 [严重]

**位置**：Section 2.5.2

**问题**：`POST /webhook/loop-completed` 接受任意 POST 请求。攻击者可构造请求触发任意 task 的处理流程。

**修复方案**：共享密钥验证

```python
# RDAgent 侧发送
headers = {"X-Webhook-Secret": os.getenv("QE_WEBHOOK_SECRET")}
await client.post(url, json=payload, headers=headers)

# AIstock 侧验证
@router.post("/webhook/loop-completed")
async def on_loop_completed_webhook(request: Request, payload: LoopCompletedPayload):
    expected = os.getenv("QE_WEBHOOK_SECRET")
    if expected and request.headers.get("X-Webhook-Secret") != expected:
        raise HTTPException(401, "Invalid webhook secret")
    ...
```

新增环境变量：
```
QE_WEBHOOK_SECRET=<random-32-char-string>
```

### 9.4 性能与可扩展性

#### 问题 10：LLM Token 预算管理缺失 → 采用三层漏斗模式 [严重 → 已设计解决方案]

**位置**：Section 三、四，以及 PortfolioArchitect / EvolutionAgents 的所有 LLM 调用

**问题**：
- 当前代码几乎没有 token 管理（仅 `_generate_with_llm` 有一处 `cat_factors[:5]` 截断）
- v3 增强后，50+ 因子 × (基础指标 + 多持有期IC + 多窗口 + factor_profile) ≈ 400 tokens/因子 = 20,000+ tokens
- 加上 system_prompt + evolution_history + config，总 prompt 可达 30,000+ tokens
- 虽然 DeepSeek 支持 64K context，但 prompt 过长会严重降低 LLM 输出质量

**解决方案：三层漏斗模式（Funnel Pattern）**

所有涉及"从因子库中选择因子"的 Agent 调用，统一采用三层漏斗：

```
┌─────────────────────────────────────────────────────────────┐
│ 第0层：规则预筛（SQL查询，无 LLM 调用，0 tokens）             │
│                                                             │
│ 输入：全量因子（100+）                                       │
│ 逻辑：WHERE grade >= 'C' AND ic_mean > 0.02                 │
│       AND coverage > 0.8                                    │
│       ORDER BY composite_score DESC                         │
│       每类别最多 top N                                       │
│ 输出：40-60 个候选因子                                       │
├─────────────────────────────────────────────────────────────┤
│ 第1层：LLM 方向筛选（轻量 prompt ≈ 2000 tokens）             │
│                                                             │
│ 输入：候选因子摘要表                                         │
│       每因子仅 5 个字段：名称 + 类别 + 评级 + IC + ICIR       │
│       ≈ 30 tokens/因子 × 50 = 1500 tokens                   │
│ 任务：根据演进目标，选出 20 个候选因子，                      │
│       说明选择方向和类别分布策略                               │
│ 输出：20 个候选因子名 + 选择理由                              │
├─────────────────────────────────────────────────────────────┤
│ 第2层：LLM 精选组合（完整 prompt ≈ 6000-8000 tokens）        │
│                                                             │
│ 输入：仅 20 个候选因子的完整数据                              │
│       - 全部独立指标（多持有期IC、多窗口稳定性）               │
│       - factor_profile（usage_guidance, risk_notes）          │
│       - 候选因子间的相关性矩阵（20×20 上三角）                │
│       ≈ 300 tokens/因子 × 20 = 6000 tokens                   │
│ 任务：分析候选因子的详细指标和相互关系，                      │
│       选出最终 10-12 个因子的最优组合                          │
│ 输出：最终因子组合 + 详细分析                                 │
└─────────────────────────────────────────────────────────────┘
```

**各 Agent 的漏斗适配：**

| Agent 场景 | 是否需要漏斗 | 适配方式 |
|-----------|-------------|---------|
| `_analyze_factor_v2` (单因子分析) | 否 | 每次只分析 1 个因子，token 可控 (~3500) |
| `_llm_evaluate_combination` (组合评估) | 否 | 评估给定组合的因子（~20个），无需从库中选 |
| `_generate_with_llm` (smart-select) | **是** | 当前把全量因子塞进 prompt，改为三层漏斗 |
| `recommend_factor_combination` | **是** | 当前按 grade 排序取 top N，改为漏斗+LLM |
| `run_analyst` (演进分析) | 否 | 分析当前配置中的因子（~20个），不涉及全库 |
| `run_researcher` (演进建议) | **是** | 需要知道可用因子库 → 漏斗模式提供候选 |

**PortfolioArchitect 改造示例：**

```python
async def generate_from_requirement(self, requirement, max_factors=30):
    # 第0层: SQL 规则预筛（无 LLM）
    candidates = self._prefilter_factors(
        min_grade="C", min_ic=0.02, max_per_category=10
    )  # ~50 factors, 0 tokens

    # 第1层: LLM 方向筛选（轻量 prompt）
    screening_prompt = self._build_screening_prompt(
        candidates=candidates,  # 每因子仅 name+category+grade+IC+ICIR
        requirement=requirement,
        target_count=max_factors * 2,
    )  # ~2000 tokens input, ~500 tokens output

    shortlist = await self._llm_screening(screening_prompt)

    # 第2层: LLM 精选（完整数据）
    detailed_data = self._get_detailed_factor_data(shortlist)  # 联查三表
    correlations = self._get_factor_correlations(shortlist)     # 相关性矩阵
    final_combo = await self._llm_deep_select(
        candidates=detailed_data,
        correlations=correlations,
        requirement=requirement,
        target_count=max_factors,
    )  # ~8000 tokens input, ~1000 tokens output

    return final_combo
```

**EvolutionAgents run_researcher 改造示例：**

```python
async def run_researcher(self, analyst_report, sota_status, config, evolution_history):
    # 第1步: 传当前因子 + 可用因子库摘要 → LLM 指出替换方向
    factor_summary = self._get_factor_library_summary()  # 每因子30 tokens
    direction = await self._llm_propose_direction(
        analyst_report, config, factor_summary
    )  # ~3000 tokens input

    # 第2步: 按方向查询候选因子详情 → LLM 做具体选择
    if direction.get("factor_changes"):
        candidates = self._get_candidate_factors(
            categories=direction["target_categories"],
            exclude=config["factor_list"],
            limit=20,
        )
        detailed = self._get_detailed_factor_data(candidates)
        new_factors = await self._llm_select_replacements(
            detailed, config, direction
        )  # ~6000 tokens input
        config["factor_list"] = new_factors

    return config
```

**实施任务**（已合并到 Phase 3 主表 3.7-3.11）：

| # | 任务 | 文件 | 优先级 |
|---|------|------|--------|
| 3.7 | `_prefilter_factors` 规则预筛方法 | `factor_analyst.py` | P0 |
| 3.8 | `_build_screening_prompt` + `_llm_screening` 轻量筛选 | `portfolio_architect.py` | P0 |
| 3.9 | `_llm_deep_select` 精选组合方法 | `portfolio_architect.py` | P0 |
| 3.10 | `run_researcher` 改为两步漏斗 | `qe_evolution_agents.py` | P0 |
| 3.11 | `_get_factor_library_summary` 轻量摘要方法 | `qe_evolution_service.py` | P0 |

#### 问题 11：Timer Scan N+1 HTTP 查询 [低]

**位置**：Section 2.5.4

**问题**：`scan_running_loops` 对每个 running 的 Loop 发 HTTP 请求检查状态。

**优化建议**：直接检查结果文件是否存在（方案 2.2 已规定文件路径），避免 HTTP 调用：

```python
async def scan_running_loops():
    for task_id, loop_id, loop_index in running_loops:
        result_path = f"{QE_WORKSPACE}/{task_id}/Loop{loop_index}/qlib_results_enhanced.json"
        pid_path = f"{QE_WORKSPACE}/{task_id}/Loop{loop_index}/run.pid"
        if os.path.exists(result_path) and not os.path.exists(pid_path):
            await process_completed_loop(task_id, f"Loop{loop_index}")
```

### 9.5 Webhook 重试增强建议

**位置**：Section 2.5.1（RDAgent 侧）

**当前设计**：webhook 失败仅 `logger.warning`，完全依赖 timer scan 兜底。

**建议增加简单重试**（减少对 timer 的依赖）：

```python
async def on_loop_completed(task_id: str, loop_id: str, success: bool):
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(f"{callback_url}/webhook/loop-completed",
                    json={"task_id": task_id, "loop_id": loop_id, "success": success},
                    headers={"X-Webhook-Secret": os.getenv("QE_WEBHOOK_SECRET", "")},
                )
                if resp.status_code == 200:
                    return
        except Exception as e:
            logger.warning(f"Webhook attempt {attempt+1}/3 failed: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)  # 1s, 2s backoff
    logger.error(f"Webhook failed after 3 attempts, relying on timer scan")
```

---

## 十、实施清单补充项

基于第九节的分析和第四节 4.4/4.5 的新增设计，以下任务需追加到各 Phase。

> **变更追溯**：
> - 原 Phase 2 的 2.1-2.2（因子分析侧数据查询）→ 已提升到 Phase 0（0.3-0.4）
> - 原 Phase 3 的 3.1-3.2（因子分析侧提示词更新）→ 已提升到 Phase 0（0.5-0.6）
> - 原 Phase 3 的 3.14-3.15（因子分类修复）→ 已提升到 Phase 0（0.1-0.2）
> - Phase 2 重新编号：2.1-2.4（聚焦 PortfolioArchitect / EvolutionAgents 侧）
> - Phase 3 重新编号：3.1-3.17（聚焦提示词 + 漏斗 + EvolutionFactorAgent）

| Phase | # | 任务 | 文件 | 优先级 | 说明 |
|-------|---|------|------|--------|------|
| 1 | A7 | `process_completed_loop` 增加 CAS 幂等保护 | `qe_evolution_service.py` | **P0** | DB 级别 status='running'→'processing' CAS，防止 webhook+timer 竞态 |
| 1 | A8 | Webhook handler 异常捕获 | `quantevolver_evolution.py` | P0 | `add_done_callback` 记录异步任务异常 |
| 1 | A9 | Webhook 认证（共享密钥） | `quantevolver_evolution.py` + `qe_evolution_api.py` | **P0** | `X-Webhook-Secret` header 验证 |
| 1 | A10 | Webhook 发送增加 3 次重试 | `qe_evolution_api.py` | P1 | 指数退避重试，减少 timer 依赖 |
| 1 | A11 | Timer scan 直接检查文件而非 HTTP | `qe_evolution_service.py` | P1 | 检查 result file + pid file，避免 N+1 查询 |
| 1 | A12 | Timer scan 优雅停机实现 | `main.py` | P1 | `asyncio.Event` + `wait_for` 模式 |
| 1 | A13 | workspace_client 请求级重试 | `qe_workspace_client.py` | P1 | httpx 层 3 次重试 + 指数退避（替代 Loop 级重试） |
| 1 | A14 | Loop 失败详细原因记录 | `qe_evolution_service.py` + DB | P1 | 新增 error_detail TEXT 列，记录失败上下文 |
| 4 | 4.12 | compose evolution 模式前后端打通 | `quantevolver.py` + `compose/page.tsx` | **P0** | GenerateConfigRequest 增加 dispatch_mode |
| 4 | 4.13 | create_evolution_task 端点调用 submit_next_loop | `quantevolver_evolution.py` | P0 | 与 Phase 1 架构改动对齐 |

---

## 十一、实施顺序建议（更新版）

```
Phase 0（因子分析Agent全面修复+性能优化）────────────────────→ 最先启动，与 Phase 1 并行
  包含：分类修复 + 数据查询扩展 + 评级更新 + 异步并发 + SSE流式 + 批量重跑
     ↓ 因子库分类+评级数据准确后
Phase 1（演进架构 + 安全加固）─┐
  包含：A1-A14                  ├─→ Phase 3（提示词 + Token管理 + Agent架构）─→ Phase 5（验证）
Phase 2（数据读取扩展）──────────┘
  聚焦 PortfolioArchitect / EvolutionAgents 侧

Phase 4（多入口演进 + 前后端打通）─────────────────────────────→ Phase 5（验证）

依赖关系：
  - Phase 0 无前置依赖，可立即启动，与 Phase 1 并行
  - Phase 0.11（批量重跑）完成后，因子库分类+评级数据才可信
  - Phase 2 可与 Phase 0 / Phase 1 并行（不同文件，无冲突）
  - Phase 3 依赖 Phase 0（分类+评级准确）+ Phase 2（其他组件数据查询就位）
  - Phase 3 中 3.12-3.17（EvolutionFactorAgent）依赖 3.7-3.11（漏斗模式）
  - Phase 4.12（compose evolution 打通）建议在 Phase 4 最前面做
  - Phase 5 在所有改动完成后统一验证
  - A7（CAS幂等）和 A9（Webhook认证）是 Phase 1 的前置必完项

预估工作量（更新）：
  Phase 0: 2.5-3 天（数据修复 0.1-0.6: 1天 + 异步并发+SSE 0.7-0.10: 1天 + 批量重跑+验证 0.11-0.12: 0.5-1天）— 与 Phase 1 并行
  Phase 1: 3-4 天（核心架构 + 安全加固 + 停机机制）
  Phase 2: 1 天（PortfolioArchitect/EvolutionAgents 侧 SQL 查询扩展，已精简为 4 项）
  Phase 3: 4-6 天（三层漏斗 + EvolutionFactorAgent 7步编排器 + 提示词调优）
  Phase 4: 3-4 天（多入口体系 + evolution 模式打通 + 前端改造）
  Phase 5: 1-2 天（端到端验证）
  总计: 14.5-20 天（Phase 0 与 Phase 1 并行，Phase 0 增加1天但删除 Phase 6，总工作量减少0.5-1天）
```
