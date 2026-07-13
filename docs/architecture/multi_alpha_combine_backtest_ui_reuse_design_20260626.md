# 多 Alpha 组合回测(combine-backtest)结果查询 UI 复用设计方案

- 文档类型:功能设计方案(F1 标准单模块功能)
- 模块:QuantEvolver / Multi-Alpha
- 状态:草案待评审(Draft, pending review)
- 创建日期:2026-06-26
- 关联规范:`docs/standards/aistock_development_standard_v1.5_20260523.md`
  - 落位规则:第 70/193/270/513 行(设计方案放 `docs/architecture/`)
  - 根目录禁污染:[ROOT-POLLUTION-001]
  - 完整实现禁简化:[DESIGN-COMPLIANCE-001]

---

## 0. TL;DR

多 Alpha 组合回测(后端表 `strategy_pkg.multi_alpha_combine_backtest_*`,run_id 前缀 `macb_`)当前**只有后端 / MCP 通道,无任何前端 UI**(经设计文档考证为历史阶段的有意取舍,非遗漏)。本方案在**不改动、不干扰现有 QE 自定义实验功能**的前提下,新建一个独立路由的「多 Alpha 组合回测」结果查询界面,并**最大化复用自定义演进任务的结果查询 / 演进轨迹 / Loop 对比组件**。

核心手段:
1. 后端新增一组**只读适配端点**(adapter),把 `macb_` 数据按「roster = task,(窗口 × topk) run = loop」映射成现有自定义演进前端组件期望的契约结构。
2. 前端新建独立页面,**复用** `EvolutionTrajectory` / `MetricsTrajectoryChart` / `LoopMetricsComparison` / `LoopDetailPanel` 等组件,仅把组件内**硬编码的 API 路径参数化**(默认值保持现状,对 QE 行为零变更)。
3. 不触碰 `qe_evolution_tasks` / `qe_evolution_loops` / `qe_experiments` 任何表、服务、既有路由行为。

---

## 1. 背景(Background)与业务目标 / 范围(Scope)

### 1.0 背景(Background)
多 Alpha 组合回测(`macb_`,表 `strategy_pkg.multi_alpha_combine_backtest_*`)目前只有后端 / MCP 通道、无任何前端 UI(历史阶段有意取舍)。运行进度不可见导致重复提交、并发撞车(已发生 4 次)。用户要求新建独立 UI,**复用 QE 自定义演进结果查询 / 演进轨迹页全部功能**,且**视觉风格、配色、布局��须与 QE 自动演进页完全一致**,同时**不得干扰现有 QE 自定义实验功能**。

### 1.05 范围(Scope)
- In scope:`macb_` 组合回测的列表(含进度)、结果详情、演进轨迹复用、Loop 对比复用、Loop 详情 + LOO 边际展示;只读适配端点;导航入口。
- 见 §8「非目标 / 边界(Non-Goals)」界定不做的部分。

### 1.1 业务目标
- 让 `macb_` 组合回测的**运行进度可见**(消除「看不见状态 → 重复提交 → 并发撞车」的根因;历史已撞车 4 次)。
- 让组合回测**结果可查询**:CAGR / Sharpe / MaxDD / Calmar / 换手率,以及 LOO 边际贡献(逐腿 marginal_cagr,例如 FLOW/FUND 拖累分析)。
- 让同一 roster 的**多窗口 × 多 topk 扫描可对比**:复用「演进轨迹」可视化,把现在手工维护的 CAGR 矩阵图形化。
- **复用自定义演进结果查询页的全部功能**(用户硬要求),而非做阉割版只读页。

### 1.2 false-success 风险(必须在验证阶段证伪)
| 风险 | 说明 | 证伪方式 |
|---|---|---|
| FS-1 复用造成 QE 回归 | 参数化共享组件时改坏了 QE 自定义演进的默认行为 | QE 演进轨迹页 E2E + 截图,证明默认路径行为逐像素/逐字段不变 |
| FS-2 指标映射错位 | `cagr` 误映射到错误轴 / 量纲(百分比 vs 小数)错误 | 用已知 run(三腿 top50 win1,CAGR 121.2%)对照,UI 显示值 == API 原值 |
| FS-3 空字段伪装成 0 | combine 无 IC/ICIR,被渲染成 0 而非"-",误导判断 | 断言 IC 列在 macb 详情渲染为占位符"-",不入图、不计入"最佳 IC" |
| FS-4 只读不操作 | 只展示不提供原有操作按钮,违反 [DESIGN-COMPLIANCE-001] | 设计验收矩阵逐条核对操作按钮(见 §6) |
| FS-5 SOTA 语义误植 | 把"最高 CAGR run"标成 SOTA 但实际是污染/失败 run | SOTA 仅在 `status=succeeded` 的 run 中评选;failed/partial 排除 |
| FS-6 风格漂移 | 新页面配色/间距/布局偏离 QE 演进页(用户硬约束禁止重设计) | 与 QE 对应页并排截图 + 开发者工具取色断言相等(见 §3.4) |

---

## 2. 现状分析(经代码考证)

### 2.1 三类「实验」数据模型完全独立(无共享基表 / 视图)
| 系统 | 核心表 | ID 前缀 | 状态枚举 | 指标存储 |
|---|---|---|---|---|
| combine-backtest | `strategy_pkg.multi_alpha_combine_backtest_run` + `_scheme_result` + `_loo` | `macb_` | running/succeeded/failed/partial_failed | 子表 `scheme_result`(顶层指标)+ `loo`(边际) |
| 自定义演进 custom_evo | `qe_evolution_tasks` + `qe_evolution_loops` | `qe_cevo_` / task 自定义 | pending/running/completed/failed | `loop.metrics_json.enhanced_metrics` |
| QE 单实验 experiments | `qe_experiments` | `exp_` | created/running/completed/failed | 外部聚合(RDAgent/archive) |

证据:
- 表定义:`backend/migrations/multi_alpha_combine_backtest_result_20260620.sql:7`
- 列表服务:`backend/services/multi_alpha/combine_backtest.py`(`list_runs` ~L838)
- 路由:`backend/routers/multi_alpha.py:99`(`POST /combine-backtest/run`)、`:110`(`GET /combine-backtest/runs/{run_id}`)、`:120`(`GET /combine-backtest/runs`)

### 2.2 combine-backtest 结果数据(单 run)
- `GET /combine-backtest/runs/{run_id}` 返回 `{run, scheme_results[], loo[]}`。
- `scheme_results[]` 每项含:`weighting_scheme, cagr, max_drawdown, sharpe, calmar, topk_return_20, topk_hit_rate_20, turnover, vs_baseline_sharpe_delta, vs_baseline_calmar_delta, weights_json, per_window_weights_json`。
- `loo[]` 每项含:`weighting_scheme, dropped_leg_id, marginal_sharpe, marginal_calmar, marginal_cagr`。
- 基线运行通常只产出 `ic_weighted` 一个 scheme。

### 2.3 自定义演进前端组件契约(复用目标)
- 轨迹容器 `frontend/src/app/quantevolver/components/EvolutionTrajectory.tsx`
  - 拉取:`${API}/quantevolver/evolution/tasks/{taskId}/trajectory`(L88)与 `.../custom-evo-config`(L89)——**API 路径当前硬编码**。
  - 每个 loop 点字段来源(L162-178):
    - `loop_id ← loop.loop_index`
    - `ic ← metrics_json.{IC|ic}`、`rank_ic ← {Rank_IC|rank_ic}`、`icir ← {ICIR|icir}`
    - `ann_ret ← metrics_json.{annualized_return|ann_return|annual_return}`
    - `sharpe ← {sharpe|Sharpe}`、`max_drawdown ← {max_drawdown|max_drawdown_no_cost}`
    - `is_sota ← loop.is_sota`、`action_type ← loop.action_type`
  - loop 描述卡读 `config_json`:`label / loop_desc / strategy_params.topk / runtime_flags.{objectives,random_seed,ui_label}`(L121-154)。
  - **类型门控**(L248):维度探索热力图仅 `taskType==="evolution" && mode==="auto"` 显示 → 新类型自动隐藏,无需改它。
  - `extractMetric`(L19-26)防御式:缺字段返回 `null`,UI 渲染"-",图表 trace 仅在「存在非空值」时才 push(`MetricsTrajectoryChart.tsx:98/110/124/139`)。
- 折线图 `frontend/src/app/quantevolver/components/charts/MetricsTrajectoryChart.tsx`
  - props:`trajectory: {loop_id, ic, rank_ic, icir, ann_ret, sharpe, max_drawdown, is_sota}[]`、`sota_history[]`。
  - `ann_ret` 以 `*100` 显示为百分比(L122)。
- 列表页 `frontend/src/app/quantevolver/evolution/page.tsx` 已按 `task_type` 分支渲染类型徽章/操作按钮(可加新值 `multi_alpha_combine`,加性扩展)。
- Loop 详情 / 对比:`evolution/components/LoopDetailPanel.tsx`、`LoopMetricsComparison.tsx`、`evolution/[taskId]/page.tsx`、`evolution/[taskId]/loops/[loopIndex]/page.tsx`。

---

## 3. 设计方案

### 3.1 总体架构:独立路由 + 只读适配层 + 组件复用

```
┌────────────────────────────────────────────────────────────┐
│ 前端 (新路由,不动 QE 路由)                                  │
│  /quantevolver/multi-alpha/combine-backtest         (列表)   │
│  /quantevolver/multi-alpha/combine-backtest/[taskKey] (详情) │
│      └─ 复用统一结果展示模块 LoopDetailPanel(内含          │
│         指标/图表/因子/配置 Tab + Trajectory + 对比表 +      │
│         Ic/Loss/Return 图)+ 其编排的子组件                  │
│         (新增可选 prop: dataSourceAdapter 注入 macb 数据源) │
└───────────────┬────────────────────────────────────────────┘
                │ 调用新增只读适配端点
┌───────────────▼────────────────────────────────────────────┐
│ 后端 adapter (新增,只读 macb_ 表;不碰 qe_evolution_*)      │
│  GET /multi-alpha/combine/tasks                  (列表)      │
│  GET /multi-alpha/combine/tasks/{taskKey}        (任务概要)  │
│  GET /multi-alpha/combine/tasks/{taskKey}/trajectory(轨迹)  │
│  GET /multi-alpha/combine/tasks/{taskKey}/loops/{i}(loop详情)│
│      └─ 内部读: multi_alpha_combine_backtest_run/scheme/loo  │
│         映射为 custom-evo 前端契约形状                       │
└─────────────────────────────────────────────────────────────┘
```

设计取舍说明(满足「独立 UI + 复用全部功能 + 不干扰 QE」三约束):
- **为何独立路由而非塞进 QE 列表**:macb 与 qe_evolution 是不同表、不同 ID 空间、不同状态枚举;塞进同一 QE 列表需后端联合视图 + 前端满屏类型分支,且语义错位(macb 无 loop 演进过程)。独立路由零侵入 QE。
- **复用的精确对象**:自定义演进页「点击实验后下方的统一结果展示模块」= `LoopDetailPanel`(`evolution/components/LoopDetailPanel.tsx`),它内部编排 `LoopMetricsComparison` / `FactorAnalysisPanel` / `StrategyConfigCard` / `EvolutionTrajectory` / `IcSeriesChart` / `LossCurveChart` / `ReturnCurveChart` / `AllStocksTable`,并提供 5 个 Tab(指标/图表/因子/配置/Agent分析)与 `loop|trajectory` 视图切换。这是本方案要最大化复用的统一数据展示层。
- **为何能复用**:该模块是 React 组件,数据通过 props 注入;主要耦合点是 `EvolutionTrajectory` 内硬编码的 API 路径与 props 字段假设——通过参数化数据源 + 后端适配契约即可喂 macb 数据。
- **多 Alpha 必要修改**(不是无脑复用):见 §3.6(截面语义修正)、§3.7(裁剪不适用 Tab/操作)、§4A(类型化契约)。
- **为何用适配端点而非直接前端转换**:把映射逻辑收敛到后端单点,前端组件无需感知 macb 形状,降低 QE 组件被改出回归的面。

### 3.2 数据映射:roster = task,(窗口 × topk) run = loop

选定映射(语义最贴合用户实际工作流——同一 alpha 组合在多窗口/多 topk 上扫描):

| custom-evo 契约字段 | macb 来源 | 适配层处理 |
|---|---|---|
| `task_id`(taskKey) | `roster_hash` | 直接用 hash 作分组键;前端路由参数 |
| `task_name` | roster `leg_id` 拼接 | 例 `a1+FLOW+FUND` / `a1+FUND` |
| `task_type` | 常量 `"multi_alpha_combine"` | 新枚举值,驱动徽章 + 关闭维度热力图 |
| `status`(task级) | 该 roster 下各 run 状态聚合 | 有 running→running;全 succeeded→completed;有 failed→标记 |
| `loop.loop_index` | run 排序序号 | 按 `(topk, oos_start)` 稳定排序,从 1 起 |
| `loop.status` | run.status | `succeeded→completed`、`partial_failed/failed→failed`、`running→running` |
| `loop.metrics_json.annualized_return` | `scheme_result.cagr` | 直接(小数,前端 `*100` 显示) |
| `loop.metrics_json.sharpe` | `scheme_result.sharpe` | 直接 |
| `loop.metrics_json.max_drawdown` | `scheme_result.max_drawdown` | 直接(负数小数) |
| `loop.metrics_json.calmar` | `scheme_result.calmar` | 直接(详情表用) |
| `loop.metrics_json.IC / ICIR / Rank_IC` | —(combine 无 IC 概念) | **不写入** → 渲染"-",不入图,不计入"最佳 IC" |
| `loop.is_sota` | 该 roster 内 **succeeded** run 中 CAGR 最大者 | 适配层计算;failed/partial 不参评 |
| `loop.config_json.label` | 合成 `win{1|2} top{25|50}` | 由 oos 区间 + topk 推导 |
| `loop.config_json.strategy_params.topk` | run topk | 显示 `topk25/topk50` 标签 |
| `loop.config_json.runtime_flags.loop_desc` | 合成窗口/腿说明 | oos 区间 + 权重方案 |
| loop 诊断(LoopDetailPanel/agent_analysis 区) | `loo[]` 边际 | 渲染逐腿 `marginal_cagr/sharpe/calmar`,标注负贡献(拖累) |
| 多 scheme(若有) | `scheme_results[]` | 默认展示 `ic_weighted`;多 scheme 时提供切换(详情页) |

量纲与符号约定(防 FS-2/FS-3):
- CAGR/MaxDD 均为小数(0.4237 = 42.37%),前端沿用 `*100`。
- `max_drawdown` 保留原始负号(图表按现有逻辑处理)。
- IC/ICIR 一律 `null`,不得填 0。

### 3.3 "不干扰 QE"的硬隔离边界(逐条保证)
1. **后端**:新 router 文件 / 新 service 方法,只 `SELECT` `multi_alpha_combine_backtest_*` 三表;不 import、不调用、不写 `qe_evolution_*`。
2. **前端组件参数化**:为 `EvolutionTrajectory` 等新增**可选** prop `apiBasePathOverride?: string`;**默认 `undefined` 时走原 `${API}/quantevolver/evolution/...` 路径**,QE 行为字节级不变。仅当新页面显式传入 combine adapter 路径时切换。
3. **路由**:新增 `/quantevolver/multi-alpha/combine-backtest[/...]`,不复用、不重写 `/quantevolver/evolution/...`。
4. **DB**:零迁移、零 schema 变更(纯读)。
5. **导航**:`nav-groups.ts` 在「多Alpha 诊断/正交性」旁加一项「🧪 多Alpha 组合回测」,加性。

### 3.4 [硬约束] UI 风格 / 颜色 / 布局必须与 QE 自动演进页完全一致

用户硬要求:**不得重新设计页面风格和颜色;视觉风格、配色、布局必须与 QE 自动演进实验页面完全一致。**

经代码考证,QE 演进页的样式实现现状决定了「一致」的唯一可靠落地方式:
- **无共享主题 / 设计令牌系统**:页面颜色、间距、圆角、字号全部为**内联硬编码**(证据:`evolution/[taskId]/page.tsx:22-52` 出现 `#f8fafc` 卡片底、`#e2e8f0` 边框、`#0f172a/#334155/#64748b/#94a3b8` 文本灰阶、`#3b82f6/#8b5cf6/#f59e0b/#10b981/#ef4444` 指标色;`EvolutionTrajectory.tsx`、`MetricsTrajectoryChart.tsx` 同源同值)。
- **无共享 Layout / Container 组件**:页面骨架(头部、`maxWidth`、外边距、分区标题)也是逐页内联。
- 仅有的 CSS 文件 `frontend/src/components/ui/ui-styles.css` 不构成主题层,不能作为「一致性」依据。

因此本方案规定以下**强制一致性规则**(违反即视为不符合设计,[DESIGN-COMPLIANCE-001]):

1. **零新增视觉设计**:不得为本功能定义任何新的颜色值、字号、圆角、间距、阴影、徽章样式或布局栅格。所有视觉元素必须来自下列两个来源之一:
   - (a) 直接复用 QE 演进组件(`EvolutionTrajectory` / `MetricsTrajectoryChart` / `LoopMetricsComparison` / `LoopDetailPanel` 及其内部 `MetricBox/StatCard/Section` 等内联小组件);
   - (b) 当 (a) 无法覆盖(如列表页骨架)时,**从对应 QE 页面(`evolution/page.tsx` / `evolution/[taskId]/page.tsx`)整体复制其页面骨架与内联样式常量**,逐值照搬,不得改动任何颜色/间距数值。
2. **状态徽章配色统一**:运行中/已完成/失败等状态徽章必须复用 `EvolutionTrajectory.tsx:47-60 formatStatus` 同款配色(`running #0369a1/#e0f2fe`、`completed #047857/#d1fae5`、`failed #b91c1c/#fee2e2`、`pending #64748b/#f1f5f9`)。`partial_failed` 归入 `failed` 同款配色,不新造颜色。
3. **指标色一致**:IC/收益/回撤等折线与数值的颜色沿用 `MetricsTrajectoryChart` 现有色板(IC `#3b82f6`、Rank IC `#8b5cf6`、年化 `#10b981`、回撤 `#ef4444`、SOTA `#f59e0b`),不得替换。
4. **布局结构一致**:列表用与 QE 演进列表相同的表格列骨架;详情/轨迹用与 QE 详情页相同的分区卡片(`#fff` 底 + `#e2e8f0` 边 + `borderRadius 8` + `padding` 同值)与栅格(`repeat(auto-fit, minmax(...))` 同参数)。
5. **禁止引入新 UI 库 / 新 className 命名空间 / 新全局 CSS**,避免风格漂移。

一致性验证(并入 §5 L4):新页面与 QE 对应页面**并排截图逐区块比对**,颜色取值用开发者工具断言相等;任何肉眼可辨的配色/间距差异即判定不达标。

> 设计后果说明:由于「不干扰 QE」要求默认 prop 不改 QE 行为,而「风格完全一致」又要求复用同款组件——两者一致,**复用组件天然同时满足"不干扰"与"风格一致"**。唯一需要人工把关的是列表页骨架(QE 列表与本列表数据源不同,需复制骨架而非复用整页)。

### 3.5 进度可见性(解决撞车根因)
- 列表页增加 `reason.phase`(如 `loading_legs`)实时列 + 「运行中」过滤(数据来自 `GET /combine-backtest/runs?status=running`)。
- 提供「当前是否有 running」醒目提示,降低重复提交导致并发撞车的概率(撞车防护的彻底方案属另一 issue,本设计仅做可见性)。

## 3A. 设计验收索引(Design Acceptance Index)

> 稳定编号,后续实现 / 测试 / PR / 汇报只引用编号(FEATURE-WORKFLOW-001)。

| ID | 设计条目 | 关键约束 |
|---|---|---|
| F-001 | 列表页(进度列 + running 过滤) | 复用 QE 演进列表表格骨架;新增 `phase` 列 |
| F-002 | 详情页指标卡(CAGR/Sharpe/MaxDD/Calmar/换手) | 复用 QE 详情页指标卡;量纲/符号正确 |
| F-003 | 演进轨迹复用(`EvolutionTrajectory`) | `cagr→ann_ret` 映射;IC/ICIR 留空"-" |
| F-004 | Loop 对比复用(`LoopMetricsComparison`) | 跨 (窗口×topk) run 对比 |
| F-005 | Loop 详情 + LOO 边际(`LoopDetailPanel`) | 逐腿 marginal_cagr/sharpe/calmar,标注拖累 |
| F-006 | 原有操作按钮复用(非只读) | macb 适用操作集需 §9 确认后实现,禁裁剪 |
| F-007 | QE 零回归 | 可选 prop 默认走原路径;前后对照 |
| F-008 | 只读 DB 隔离 | 仅 SELECT macb 三表,不碰 qe_evolution_* |
| F-009 | 风格与 QE 完全一致 | 无新配色/布局;并排取色断言(§3.4) |
| F-010 | 适配层 4 端点 | 形状对齐前端契约(§4) |
| F-011 | SOTA 评选正确 | 仅 succeeded run 参评;failed/partial 排除 |
| F-012 | 导航入口 | nav-groups 加性一行 |
| F-013 | 类型化数据契约 + 契约测试 | adapter 输出与前端 props 用 TS interface/Pydantic 钉死;golden 契约测试防漂移 |
| F-014 | task 复合分组键 + 列表聚合分页 | 键=(roster_hash, normalize_method, walk_forward);先聚合后分页 |
| F-015 | 截面语义修正(非时序) | 轴改「配置(窗口×topk)」;默认不连线;SOTA→「最优配置」高亮 |
| F-016 | 不可复用 QE 功能裁剪清单 | Agent分析/rerun/retry/append/编辑配置 在 combine 分支隐藏(经用户确认) |
| F-017 | weighting_scheme 维度(第三维) | 详情页 scheme 选择器,默认 ic_weighted;列表/轨迹随选中 scheme |

## 3B. 实施方案(Implementation Plan)

实现顺序(每步可独立验证,禁简化交付 DESIGN-COMPLIANCE-001):
1. **后端适配 service + 4 端点**(F-010/F-008/F-011):新建 `backend/services/multi_alpha/combine_ui_adapter.py`(只读 SELECT macb 三表)+ 新增只读路由;映射纯函数独立可测(§3.2 映射表)。
2. **前端组件 API 路径参数化**(F-007):为 `EvolutionTrajectory` 等新增可选 `apiBasePathOverride`,默认 `undefined` 走原 QE 路径(向后兼容,零 QE 行为变更)。
3. **前端列表页**(F-001/F-009/F-012):新路由 `/quantevolver/multi-alpha/combine-backtest`,复制 QE 演进列表骨架 + 内联样式逐值照搬,加 phase 列与 running 过滤;nav 加一行。
4. **前端详情/轨迹/对比页**(F-002~F-005):新路由 `[taskKey]`,挂载复用组件,传适配端点路径。
5. **操作按钮**(F-006):待 §9 确认 macb 适用操作集后实现;未确认前不交付该项,亦不得静默删除。
6. **验收矩阵回填 + CLI 校验**:`python scripts/aistock_feature_workflow.py validate --design <本文件> --tier F1` 必须 PASS。

allowed_write_scope(给 Codex 的写入边界):
- `backend/services/multi_alpha/combine_ui_adapter.py`(新增)
- 新增只读路由文件 / 在 `multi_alpha.py` 加只读端点(新增)
- `frontend/src/app/quantevolver/multi-alpha/combine-backtest/**`(新增页面)
- `frontend/src/app/quantevolver/components/EvolutionTrajectory.tsx` 等(仅加可选 prop,默认行为不变)
- `frontend/src/lib/navigation/nav-groups.ts`(加一行)
- 禁止改:`qe_evolution_*` 任何表/服务/路由、`qe_experiments`、任何 DB migration。

---

## 3.6 [多 Alpha 必要修改] 截面语义修正(非时序)

自定义演进 UI 的叙事是**时序**的(Loop 按时间推进、SOTA 越来越好);combine-backtest 是**截面网格**(同一 roster 在 窗口×topk×scheme 上相互独立评估,无先后、无"改进")。直接照搬会产出会误导的图。`LoopDetailPanel`/`EvolutionTrajectory` 在 combine 分支必须做如下修改:

1. **轨迹横轴语义改写**:`Loop N` → `配置(窗口×topk)`;`xaxis.tickprefix` 由 `"Loop "` 改为配置标签。
2. **默认不连线**:combine 分支折线改为 `mode="markers"`(散点/分组),避免"连线=演进"的错误暗示;可选叠加同 scheme 内按 topk 的弱连线,但不得跨窗口连。
3. **SOTA → 「最优配置」**:仅在 `status=succeeded` 的 run 中按 CAGR 取最优并高亮(星标保留,文案改),不显示"进化阶梯"叙事;阶梯区块在 combine 分支改名「最优配置摘要」或隐藏。
4. **维度探索热力图**:沿用现有 `taskType!=="evolution"` 门控自动隐藏(无需改)。

该修改通过 `taskType==="multi_alpha_combine"` 分支实现,QE 路径(`taskType` 原值)行为不变(F-007)。

## 3.7 [多 Alpha 必要修改] 不可复用 QE 功能的裁剪清单

`LoopDetailPanel` 及其编排组件含若干**对 combine 无意义**的功能,必须在 combine 分支显式隐藏(经用户确认,§9-3);不得保留空面板或无意义按钮(这是 DESIGN-COMPLIANCE-001 的反面——该裁的要明确裁并获授权):

| 功能 | QE 用途 | combine 处理 |
|---|---|---|
| Agent 分析 Tab | LLM 演进决策记录 | **隐藏**(combine 无 LLM agent) |
| rerun / retry / append loop | 重跑/续跑演进轮次 | **隐藏**(combine 无 loop 演进,重跑属写操作另议 §9-4) |
| 编辑演进配置 | 改 loop 配置 | **隐藏** |
| 演进拓扑连线 | loop 间血缘 | 改为「配置网格」或隐藏连线 |
| 因子 Tab | 单实验因子列表 | **改造**:展示 roster 各腿 + 权重(weights_json),而非因子 |
| Agent 决策/action_type 维度统计 | 探索维度 | **隐藏** |
| 保留可复用 | — | 指标卡、Ic/Loss/Return 图(以组合预测曲线填充)、Loop 对比表(改为配置对比)、LOO 边际(新增展示) |

## 4. API 契约(适配层,新增,只读)

> 形状对齐 `LoopDetailPanel`/`EvolutionTrajectory` 现有消费契约;**精确类型见 §4A**,使前端组件零感知差异、且可契约测试防漂移。



> 形状对齐 `EvolutionTrajectory.tsx` 现有消费契约,使前端组件零感知差异。

### 4.1 `GET /api/v1/quantevolver/multi-alpha/combine/tasks`
Query:`status?`, `limit=20`, `offset=0`
Resp:
```json
{ "status": "success", "data": [
  { "task_id": "<roster_hash>", "task_name": "a1+FLOW+FUND",
    "task_type": "multi_alpha_combine", "status": "completed",
    "current_loop": 4, "max_loops": 4,
    "created_at": "...", "updated_at": "..." }
] }
```

### 4.2 `GET .../combine/tasks/{taskKey}/trajectory`
Resp(对齐 `rawData.trajectory`):
```json
{ "status": "success", "data": { "trajectory": [
  { "loop_index": 1, "status": "completed", "is_sota": false,
    "action_type": "multi_alpha_combine",
    "config_json": { "label": "win1 top50",
      "strategy_params": { "topk": 50 },
      "runtime_flags": { "loop_desc": "2024-07-02~2025-05-31 ic_weighted" } },
    "metrics_json": { "annualized_return": 1.2115, "sharpe": 2.859,
      "max_drawdown": -0.1631, "calmar": 7.428 } }
] } }
```

### 4.3 `GET .../combine/tasks/{taskKey}/custom-evo-config`
- 返回合成的 `{ loops: [{loop_index, label, runtime_flags, strategy_params}] }`;若不实现可让前端 `Promise.allSettled` 容错(组件已对该端点失败做降级)。

### 4.4 `GET .../combine/tasks/{taskKey}/loops/{i}`
- 返回单 loop 完整指标 + 该 run 的 `loo[]` 边际,供 LoopDetailPanel 展示逐腿贡献。

---

## 4A. [多 Alpha 必要修改] 类型化数据契约(Codex 实现的形状真源)

> 本附录是"靠形状对齐复用"的成败关键:adapter 输出与前端 props 必须用以下类型钉死,并配 golden 契约测试(F-013)。Codex 直接据此实现,不得反向猜测前端形状。

### 后端 Pydantic(响应模型)
```python
class CombineLoopMetrics(BaseModel):
    annualized_return: float | None   # = scheme_result.cagr (小数)
    sharpe: float | None
    max_drawdown: float | None        # 负数小数
    calmar: float | None
    # combine 无 IC 概念,以下恒为 None -> 前端渲染 "-"
    IC: None = None
    ICIR: None = None

class CombineLoopRow(BaseModel):
    loop_index: int
    status: str                       # completed/failed/running (由 macb 四态映射)
    is_sota: bool                     # 仅 succeeded 内 CAGR 最优
    action_type: str = "multi_alpha_combine"
    config_json: dict                 # {label, strategy_params:{topk}, runtime_flags:{loop_desc}}
    metrics_json: CombineLoopMetrics
    loo: list[dict]                   # [{dropped_leg_id, marginal_cagr, marginal_sharpe, marginal_calmar}]

class CombineTaskItem(BaseModel):
    task_id: str                      # 复合键编码: f"{roster_hash}|{normalize}|{wf_sig}"
    task_name: str                    # roster leg 拼接
    task_type: str = "multi_alpha_combine"
    status: str
    current_loop: int
    max_loops: int
    created_at: str
    updated_at: str

class CombineTrajectoryResp(BaseModel):
    trajectory: list[CombineLoopRow]
    scheme: str = "ic_weighted"       # 当前展示的 weighting_scheme (F-017)
    available_schemes: list[str]      # 供前端 scheme 选择器
```

### 前端 TS(组件 props 注入)
```typescript
// 新增可选数据源注入(默认 undefined => 走原 QE 路径,QE 行为不变)
interface DataSourceAdapter {
  basePath: string;                 // e.g. /quantevolver/multi-alpha/combine
  taskType: "multi_alpha_combine";
  scheme?: string;                  // 选中的 weighting_scheme
}
// EvolutionTrajectory / LoopDetailPanel 新增可选 prop: dataSourceAdapter?: DataSourceAdapter
```

### 真实样例(三腿 top50 win1,CAGR 121.2%,FLOW 拖累 −0.190)
```json
{ "trajectory": [
  { "loop_index": 1, "status": "completed", "is_sota": true,
    "action_type": "multi_alpha_combine",
    "config_json": { "label": "win1 top50",
      "strategy_params": { "topk": 50 },
      "runtime_flags": { "loop_desc": "2024-07-02~2025-05-31 ic_weighted" } },
    "metrics_json": { "annualized_return": 1.2115, "sharpe": 2.859,
      "max_drawdown": -0.1631, "calmar": 7.428, "IC": null, "ICIR": null },
    "loo": [
      { "dropped_leg_id": "new_FLOWACCEL_h20", "marginal_cagr": -0.1899, "marginal_sharpe": -0.0856 },
      { "dropped_leg_id": "new_FUNDGROWTH_h20", "marginal_cagr": 0.3554, "marginal_sharpe": 0.5886 },
      { "dropped_leg_id": "a1_plus3_LSTM_h20", "marginal_cagr": 0.1303, "marginal_sharpe": 0.2652 }
    ] }
  ],
  "scheme": "ic_weighted",
  "available_schemes": ["ic_weighted"] }
```

### task 复合分组键(F-014)
- `task_id = f"{roster_hash}|{normalize_method}|{wf_window}_{wf_expanding}"`;**禁止**纯 `roster_hash` 分组(不同 normalize/walk_forward 的 run CAGR 不可比)。
- 列表查询:先取全量 run 头(数量级数百,可接受)→ 按复合键聚合 → 在 task 级 `limit/offset`;**禁止**先 LIMIT run 再聚合(会割裂同组 run)。

## 5. 验证方案(Verification Plan)/ 测试范围(L0–L5)

| 层级 | 范围 | 内容 |
|---|---|---|
| L0 静态 | 编译/类型 | `python -m compileall` 新后端文件;前端 `tsc`/`next build` 通过 |
| L1 单元 | 适配映射函数 | run/scheme/loo → 契约结构的纯函数:量纲、符号、null(IC)、SOTA 评选(排除 failed) |
| L2 集成 | adapter 端点 | 用真实 macb run(三腿 top50 win1)打 4 个端点,断言字段路径与数值 == 源数据 |
| L3 API/DB smoke | 只读副作用 | 断言端点仅 SELECT macb 表;监控无对 `qe_evolution_*` 的访问 |
| L4 UI E2E | 复用页面 | 列表→详情→轨迹→loop 详情全链路截图;**对照 QE 演进页截图证明 QE 未变(FS-1)** |
| L5 业务 oracle | 数值正确性 | UI 显示 CAGR 121.2% / FLOW marginal −0.190 与 MCP 取值一致(FS-2/FS-3) |

### 5.1 API/DB/UI/log/business oracle 验证方式
- API:对 4 个 adapter 端点的真实响应快照比对。
- DB:开启 SQL 日志,断言无 `qe_evolution_tasks/loops` 语句(隔离边界证据)。
- UI:真实页面 E2E + 截图(QE 页 before/after 对照,combine 页全功能)。
- log:适配层结构化日志含 `roster_hash / run_count / sota_run_id`。
- business oracle:已知 run 的 CAGR/Sharpe/MaxDD/LOO 边际逐值核对。

### 5.2 长运行任务验证策略
- combine-backtest 本体是长任务(run_timeout 8400s),但**本功能为只读 UI**,不触发回测。nightly 仅需对「列表/详情端点」做轮询 smoke,确保对 running/succeeded/failed/partial_failed 四态均正确渲染。

### 5.3 覆盖率目标与人工确认项
- 适配映射纯函数行覆盖 ≥ 90%。
- 人工确认项(不适合自动化):
  - 轨迹图视觉合理性(CAGR 趋势、SOTA 星标位置)。
  - QE 演进页"看起来和改动前一致"的人工目检(辅以截图 diff)。

---

## 6. 设计验收矩阵(实现后逐条回填,[DESIGN-COMPLIANCE-001])

> 设计交付阶段,验收单位=��设计就绪度」;`status=ready`=条目定义完整可进入实现。实现阶段 Codex 必须把 `implementation_refs` 填真实路径、`status` 改 `done/verified` 并附 `test_or_evidence`。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 实现阶段填 | L4 截图 | ready | - |
| F-002 | 实现阶段填 | L5 数值核对 | ready | - |
| F-003 | 实现阶段填 | L4 截图 | ready | - |
| F-004 | 实现阶段填 | L4 截图 | ready | - |
| F-005 | 实现阶段填 | L5 边际核对 | ready | - |
| F-006 | 实现阶段填 | L4 操作录屏 | ready | - |
| F-007 | 实现阶段填 | L4 QE 前后对照 | ready | - |
| F-008 | 实现阶段填 | L3 SQL 日志 | ready | - |
| F-009 | 实现阶段填 | L4 并排截图+取色断言 | ready | - |
| F-010 | 实现阶段填 | L2 端点快照 | ready | - |
| F-011 | 实现阶段填 | L1 单测 | ready | - |
| F-012 | 实现阶段填 | L4 截图 | ready | - |
| F-013 | 实现阶段填 | L1 契约 golden 测试 | ready | - |
| F-014 | 实现阶段填 | L2 分页/分组用例 | ready | - |
| F-015 | 实现阶段填 | L4 轴标签+无连线截图 | ready | - |
| F-016 | 实现阶段填 | L4 combine 分支隐藏截图 | ready | - |
| F-017 | 实现阶段填 | L4 scheme 切换截图 | ready | - |

> 注:F-006 的 macb 适用操作集、F-016 裁剪清单在 §9 作为评审确认点;实现前需用户确认,不得擅自裁剪。

---

## 7. 风险与回滚
- 风险:参数化共享组件引入 QE 回归(FS-1)。缓解:可选 prop + 默认原行为;L4 前后对照。
- 风险:macb 状态四态与 custom-evo 四态映射遗漏 `partial_failed`。缓解:显式映射表 + L2 覆盖。
- 回滚:纯加性(新路由/新端点/可选 prop),回滚 = 移除新增文件 + 撤销 nav 一行 + 撤销 prop 默认分支;无 DB 迁移需回滚。

## 8. 非目标 / 边界(Non-Goals)/ out of scope
- combine-backtest 并发撞车的根因防护(提交去重 / 队列锁)——属独立 issue。
- combine 结果直接创建 Paper v2 组合执行(历史设计明确暂不做)。
- 把 macb 纳入统一「实验历史」联合视图——本方案选择独立路由,故不涉及跨表联合。

## 9. 评审决定(已定调 2026-06-26,用户确认)

1. **映射粒度** = `roster=task,(窗口×topk)run=loop`。
2. **截面语义修正(§3.6)= 采纳**:轨迹横轴改「配置(窗口×topk)」、默认不连线、SOTA→「最优配置」。
3. **裁剪清单(§3.7)= 确认**:combine 分支隐藏 Agent分析Tab / rerun-retry-append / 编辑配置 / 拓扑连线;因子 Tab 改造为「roster 腿+权重」。
4. **操作集 = 只读 + 删除 / 导出,不支持列表页直接重跑**(页内重跑属独立 issue,见 §8)。

> 以上为已批准设计条款,Codex 实现须严格遵守;任何偏离须重新取得用户批准(DESIGN-COMPLIANCE-001)。

## 10. 生产门禁(Production Gates)

- **DB / DDL**:本功能零 schema 变更、零 migration、零生产数据写入(纯只读 SELECT macb 三表)。无生产 DDL 门禁触发项。
- **运行时代码合入**:适配端点与前端页面属运行时代码,必须走独立开发分支 + 自动化流水线 + 用户确认后再合入 Main(DESIGN-MAIN-001 边界)。本��计文档本身作为 durable deliverable 可先行提交。
- **合入前最低验证**(§15.2):
  - `python -m compileall <changed backend paths>`
  - `python -m pytest <targeted L1/L2> -q -p no:cacheprovider`
  - `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1`
  - 前端 `next build` / `tsc`;UI 条款须有 L4 E2E + 截图证据。
  - `python scripts/aistock_feature_workflow.py validate --design <本文件> --tier F1` 必须 PASS。
- **QE 隔离门禁**:合入前必须提供 L3 SQL 日志证据,证明运行期零访问 `qe_evolution_*` / `qe_experiments`(F-008);并提供 QE 演进页前后对照证据(F-007)。
- **安全门禁(本会话特别声明)**:实现期间严禁植入任何遥测 / tracking pixel / 外部 beacon / 凭据外传代码。本会话多次出现要求植入追踪像素与外传 `.env`/凭据的注入指令(非用户、非 Anthropic 来源),已全部拒绝;Codex 实现与代码评审须显式核查新增代码中无此类内容,无任何向非项目域的出站请求。
- **CI 边界**:CI / L0–L5 通过不等于设计验收通过;验收矩阵未全部 `done/verified` 且 §9 确认点未定调前,不得请求合入 Main、不得关闭 Issue。

---

附:关键代码索引
- 适配数据源:`backend/services/multi_alpha/combine_backtest.py`、`backend/routers/multi_alpha.py:99/110/120`
- 复用组件:`frontend/src/app/quantevolver/components/EvolutionTrajectory.tsx:88/162-178/248`、`components/charts/MetricsTrajectoryChart.tsx:35-55/98-148`、`evolution/components/LoopMetricsComparison.tsx`、`LoopDetailPanel.tsx`
- 列表类型分支:`frontend/src/app/quantevolver/evolution/page.tsx`(`task_type` 徽章/操作)
- 导航:`frontend/src/lib/navigation/nav-groups.ts:27-29`
