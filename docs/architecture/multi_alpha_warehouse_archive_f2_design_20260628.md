# 多 Alpha 架构实验:QE 数仓归档 + 腿级来源物化 + UI 详情可见 F2 设计

- 日期:2026-06-28
- 文档类型:F2 跨模块架构设计(数仓 schema + 归档 ETL + 业务表 + UI)
- 落位:`docs/architecture/`（[DOC-LOCATION-001]）
- 模块:QuantEvolver / Multi-Alpha / QE-Archive(数仓)
- worktree:`docs-multi-alpha-warehouse-archive-20260628`;branch:`docs/multi-alpha-warehouse-archive-design-20260628`
- 状态:🔶 F2 设计待评审
- 关联:`multi_alpha_combine_backtest_ui_reuse_design_20260626.md`(已合,UI 复用底座)、`multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`(远端派发,失败治理)

> 用户三目标(2026-06-28):①找到并长期解决实验失败;②QE 数仓能分析获取**所有**多 Alpha 实验数据,不丢弃;③UI 可见每个 loop/腿的详细配置、来源、叠加方式,且详情入仓、长期历史可查。**已定方向**:腿级来源**完整物化**;归档**双链路**(outbox 实时 + source_assembler/backfill 补历史);先出完整 F2 设计。

---

## 0. TL;DR

多 Alpha 组合回测(`macb_`)当前是**数仓孤岛**:数据只在 `strategy_pkg.multi_alpha_combine_backtest_{run,scheme_result,loo}` 三张业务表,从未进 `qe_archive`(实测 `qe_archive.run` 中 macb 行数=0);腿的真实来源(因子集/模型/训练窗口)不在 macb 表内,只靠 `roster_json.seed_run_ids` 指针反查——而 macb 未归档,指针链一旦 seed run 被清理即永久断裂。本设计把 macb 升级为**数仓一等公民**:新增 macb 维度/事实表 + 双链路归档 + 归档时**物化腿级来源快照**(不存指针)+ UI 详情面板,并联动失败治理(远端派发 + partial_failed 状态 + conf bug),实现"所有历史数据长期可查"。

---

## 1. 背景(Background)

### 1.1 现状考证(代码 + DB 实测)
- macb 三业务表:`run`(roster_json/oos/normalize/walk_forward/backtest_config/baseline/status/reason)、`scheme_result`(weighting_scheme/weights_json/per_window_weights_json/cagr/sharpe/mdd/calmar/turnover/vs_baseline_delta/pred_persisted/skipped)、`loo`(dropped_leg_id/marginal_{cagr,sharpe,calmar})。
- 腿配置现状:`roster_json` 每腿仅 `{leg_id, metadata:{}(空), seed_run_ids:[...]}`。**腿的因子集/模型/窗口/来源不在 macb 表**,需反查 `qe_archive.run`。
- 叠加方式现状:`scheme_result.weights_json`(静态权重,如 ic_weighted a1=0.697/fund=0.303)+ `per_window_weights_json`(185 时点滚动权重)。
- 数仓现状:`qe_archive`(67 表)只归档 `run_type∈{evolution_loop(714), single_experiment(26)}`;**无任何 macb/roster/combine 表**;`source_assembler` 硬编码只扫 `qe_experiments` + `qe_evolution_loops`;macb **不发任何 outbox 事件**。
- 失败现状:28 run(15 成功/13 失败)。失败子任务根因:`node_capacity_exhausted` ~71%、`exit_code=3221225786`(^C 中断)~20%、`pred_backtest_conf_parse_failed`(YAML 构造器,真 bug)少量。`partial_failed` 因 PG schema 不支持被压成 `failed`(reason 里 `logical_status=partial_failed` 已记)→ UI"看起来都失败"被放大。

### 1.2 问题陈述
- **数据丢失风险(P0)**:macb 永不入仓 + 腿来源是断链指针 → 历史不可复原,违背"不丢数据/长期可查"。
- **失败常态化**:~90% 失败是本地单节点并发争抢,非逻辑 bug;但缺乏长期解 + 状态语义缺失(partial_failed)放大观感。
- **详情不可见**:UI 无法展示每腿配置/来源/叠加方式(数据要么空 metadata,要么断链)。

---

## 1A. 范围(Scope)
- **In scope**:
  - qe_archive 新增 macb 维度/事实表(run/leg/scheme/loo + 腿级来源物化)。
  - 双链路归档:macb run 完成发 outbox 事件 + 新 handler 实时归档;source_assembler/backfill 补历史 28 run。
  - 业务表加 `partial_failed` 状态支持(消除"看似全失败")。
  - UI 详情面板:每腿配置/来源/叠加方式 + 失败 reason 可见(承接 UI 复用底座)。
  - 失败治理联动:对接远端派发(Phase 2 容量守卫统一)+ 修 `conf_parse` bug。
- **Out of scope**:见 §8。

---

## 2. 架构(Architecture)

### 2.1 总体:macb → 数仓一等公民(三层)
```
┌──────────────────────────────────────────────────────────────┐
│ 业务层 strategy_pkg.multi_alpha_combine_backtest_*            │
│  run / scheme_result / loo (+ partial_failed 状态)            │
│  run 完成 → emit outbox: qe.multi_alpha.combine.completed     │
└───────────────┬──────────────────────────────────────────────┘
       双链路    │ (A) 实时: outbox 事件 + MultiAlphaCombineArchiveHandler
                │ (B) 补历史: source_assembler macb 分支 + backfill
┌───────────────▼──────────────────────────────────────────────┐
│ 数仓层 qe_archive (新增 macb 维度/事实表, run_type=multi_alpha_combine) │
│  multi_alpha_run    : roster 维度(roster_hash/窗口/normalize/wf/baseline/status) │
│  multi_alpha_leg/leg_source: 腿级物化快照 + **精确溯源**(exp_id/loop_id/loop_index) │
│  multi_alpha_scheme : 叠加方式(weighting_scheme/weights/per_window_weights/指标) │
│  multi_alpha_loo    : 腿边际(dropped_leg/marginal_*)             │
│  + 复用 qe_archive.run 注册一条 run_type=multi_alpha_combine 头  │
└───────────────┬──────────────────────────────────────────────┘
┌───────────────▼──────────────────────────────────────────────┐
│ 分析/UI 层                                                    │
│  UI 详情面板(每腿配置/精确来源/叠加)读**业务表**,不读数仓        │
│  数仓 SQL/视图: 跨实验统计、roster 演进、scheme 对比          │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 关键设计原则
0. **职责严格分离(用户明确边界)**:**UI 读 QE 多 Alpha 业务配置表,数仓不参与实验构建,只做事后分析统计**。数仓归档是业务的旁路,归档故障不得影响实验运行与 UI 展示;UI 不读、不回退到数仓。
1. **腿来源精确溯源(用户硬要求)**:每条腿的每个 seed 必须能追溯到**来自哪个 QE 实验(experiment_id)、哪个 loop(loop_id+loop_index)、哪种实验类型(evolution_loop / single_experiment)**。这是单实验 / 自定义演进 / 多 Alpha 三层**全链路追踪与分析**的数据基础,且**必须进数仓**(`multi_alpha_leg_source` 表)。seed 有两种标识格式(实测),归档时都要解析到精确来源,失败显式记录不静默。
2. **腿级来源完整物化**:归档时把每腿的因子集 hash/因子清单、模型类型/family、训练窗口、freq/label_horizon、来源 seed 的精确实验/loop 坐标**复制进数仓**。seed run 即使被清理,roster 来源仍可从数仓完整复原。**不存裸指针**。
3. **叠加方式可复原**:`multi_alpha_scheme` 存 weighting_scheme + 静态 weights + per_window_weights(滚动权重全量),配合 normalize_method/walk_forward(run 级),完整描述"多腿如何叠加"。
4. **双链路 + 幂等**:实时 outbox(新 run)+ backfill(历史/补偿);两链路写同一套数仓表,以 `(run_id)` 幂等 upsert,重复归档不产生脏数据。
5. **避开 outbox 黑洞覆辙**(记忆 qe_archive_paper_outbox_bug):新增独立事件类型并**注册进 worker 白名单 + handler 真接生产**(非仅 tests 引用);macb 事件与 paper telemetry 不混写;消费失败显式 + 不可归档数据显式 skip(复用 `_claim_policy_skip_events` 模式),不留静默黑洞。

### 2.3 数据流(稳态)
```
macb run succeeded/partial_failed/failed
  → service emit outbox(event_type=qe.multi_alpha.combine.completed, payload={run_id})
  → qe_archive worker claim(白名单含新类型) → MultiAlphaCombineArchiveHandler
     → 读 macb 3 业务表 + 解析每 seed→(exp_id,loop_id,loop_index,run_type) + 物化腿快照
     → upsert qe_archive.multi_alpha_{run,leg,leg_source,scheme,loo} + qe_archive.run 头
  → 失败 → loud 错误 + outbox 重试;不可归档(数据缺失)→ 显式 skip 记录
历史补偿:backfill_service 扫 macb 表 → 同一 handler 逻辑 upsert
```

---

## 3. 契约(Contracts:DB / 事件 / API / UI）

### 3.1 DB schema(qe_archive 新增,DDL 走生产门禁)
> 所有新表/列必须 `COMMENT ON`（[DB-COMMENT-001]）。jsonb 列注明 schema/version/source。

- `qe_archive.multi_alpha_run`(roster 维度 + run 头)
  - PK `run_id`;`roster_hash`、`oos_start/oos_end`、`normalize_method`、`walk_forward_json`、`baseline_leg_id`、`leg_count`、`status`(含 `partial_failed`)、`logical_status`、`reason_json`、`source_created_at`、`archived_at`。
- `qe_archive.multi_alpha_leg`(腿级**物化快照**,每 run × leg 一行)
  - PK `(run_id, leg_id)`;`leg_order`、`seed_run_ids jsonb`、**物化字段**:`factor_set_hash`、`factor_names jsonb`、`factor_count`、`model_type`、`model_family`、`freq`、`label_horizon`、`seed_count`、`source_run_meta jsonb`(seed run 关键元数据快照)、`provenance_complete bool`(物化是否完整,缺源时 false + reason)。
- `qe_archive.multi_alpha_leg_source`(**腿来源精确溯源**,每 run × leg × seed 一行 —— 全链路追踪基础,用户硬要求)
  - PK `(run_id, leg_id, source_seq)`;`seed_ref`(原始 seed 标识,如 `qear_run_*` 或 `qe_*_Lx`)、`seed_ref_kind`(`archive_run_id` / `evolution_loop_id`,兼容两种标识体系)、**精确来源**:`source_experiment_id`、`source_task_id`、`source_loop_id`、`source_loop_index`、`source_run_type`(evolution_loop/single_experiment)、`source_model_type`、`source_factor_set_hash`、`resolved bool`、`resolve_method`(archive_run_id 直查 / evolution_loop_id 解析)、`resolve_note`(未解析时记原因)。
  - **设计要点**:每条腿的每个 seed 都必须能回答"来自哪个 QE 实验(experiment_id)、哪个 loop(loop_id+index)、哪种实验类型"。这是单实验 / 自定义演进 / 多 Alpha 三层全链路追踪与分析的数据基础。seed 标识有两种格式(实测):`qear_run_<hash>`→直查 `qe_archive.run`;`qe_<task>_L<idx>`→解析 task+index 后查 `qe_evolution_loops`/`qe_archive.run`。两种都必须解析到 (experiment_id, loop_id, loop_index),解析失败显式记 `resolved=false`+reason,不静默。
- `qe_archive.multi_alpha_scheme`(叠加方式 + 指标,每 run × scheme 一行)
  - PK `(run_id, weighting_scheme)`;`weights_json`、`per_window_weights_json`、`cagr/max_drawdown/sharpe/calmar/topk_return_20/topk_hit_rate_20/turnover`、`vs_baseline_sharpe_delta/vs_baseline_calmar_delta`、`pred_persisted/skipped/skipped_reason`、`is_best bool`(succeeded 内最优)。
- `qe_archive.multi_alpha_loo`(腿边际,每 run × scheme × dropped_leg 一行)
  - `(run_id, weighting_scheme, dropped_leg_id)`;`marginal_cagr/sharpe/calmar`。
- `qe_archive.run` 头:为每个 macb run 写一行 `run_type='multi_alpha_combine'`、`source_system='multi_alpha'`、`status` 映射(复用现有列;不破坏 evolution_loop/single_experiment)。

### 3.2 事件契约(outbox)
- `event_type = "qe.multi_alpha.combine.completed"`;payload `{run_id, roster_hash, status}`。
- 注册:`worker_service.SUPPORTED_WORKER_EVENT_TYPES` 追加该类型 + handler map 绑定 `MultiAlphaCombineArchiveHandler`。
- 不可归档(数据缺失/源被删)→ 显式 policy skip(可见、可审计),禁静默。

### 3.3 归档 API（只读查询,复用现有 qe_archive 查询面 + macb 专属）
- 复用现有 `qe_archive` 只读端点暴露 macb 维度;新增 macb 专属聚合(roster 演进、scheme 对比、跨窗口矩阵)。

### 3.4 UI 契约(详情面板)
> **架构边界(用户明确)**:UI 读 **QE 多 Alpha 自己的业务配置表**(`strategy_pkg.multi_alpha_combine_backtest_*` + 腿来源解析),**不读数仓**。数仓**不参与实验构建**,仅用于事后分析统计。两者职责严格分离。
- macb 详情页(承接 `multi_alpha_combine_backtest_ui_reuse_design`):每个 loop(=窗口×topk run)可点开详情,展示:
  - **每腿**:leg_id、因子集(factor_names/hash)、模型类型、训练窗口、**精确来源(来自哪个 QE 实验 experiment_id + 哪个 loop_id/index + 实验类型)**、seed 数量、provenance 解析状态。
  - **叠加方式**:weighting_scheme、静态权重、per_window 滚动权重(图)、normalize/walk_forward。
  - **失败可见**:status(含 partial_failed)、reason(失败子任务 + stderr_tail 摘要),消除"看似全失败"。
- **数据源**:UI 一律读业务表 + 实时来源解析(业务侧轻量解析 seed→experiment/loop);数仓物化是**事后分析旁路**,UI 不依赖、不回退到数仓。归档失败不影响 UI 展示。

---

## 3A. 设计验收索引(Design Acceptance Index)

| ID | 设计条目 |
|---|---|
| F-001 | qe_archive macb **五表** schema(run/leg/leg_source/scheme/loo)+ COMMENT |
| F-002 | 腿级来源**完整物化**(factor/model/window/seed meta 复制,非指针)+ provenance_complete |
| F-003 | 叠加方式可复原(scheme weights + per_window + normalize/wf)|
| F-004 | outbox 事件类型 `qe.multi_alpha.combine.completed` + 白名单注册 |
| F-005 | MultiAlphaCombineArchiveHandler 实时归档,**真接生产**(非仅 tests）|
| F-006 | source_assembler/backfill macb 分支补历史 28 run |
| F-007 | 双链路幂等 upsert（run_id 幂等,重复归档无脏数据）|
| F-008 | 不可归档数据显式 skip（避 outbox 黑洞,可审计）|
| F-009 | `partial_failed` 状态入业务表 schema + 映射数仓 + UI 展示 |
| F-010 | UI 腿级详情面板（配置/来源/叠加/失败 reason）**读业务表,不读数仓** |
| F-011 | 数仓跨实验分析面（roster 演进/scheme 对比/窗口矩阵 SQL/视图）|
| F-012 | 失败治理联动：对接远端派发 Phase2 容量统一 + 修 conf_parse bug |
| F-013 | 历史不丢保证：backfill 全量 + provenance 校验 + 归档覆盖率报告 |
| F-014 | QE 隔离零回归：不破坏 evolution_loop/single_experiment 归档与 paper telemetry |
| F-015 | **腿来源精确溯源入仓**:每 seed 解析到 (experiment_id, loop_id, loop_index, run_type),兼容 `qear_run_*` 与 `qe_*_Lx` 两种标识,解析失败显式记录 |
| F-016 | **三层全链路追踪分析面**:基于 leg_source 关联单实验 / 自定义演进 / 多 Alpha,支持「某 loop 被哪些 roster 复用」「某 roster 各腿溯源」双向查询 |

## 3B. 实施方案(Implementation Plan,分阶段)

**Phase A — 数仓 schema + 物化归档 + 精确溯源(目标 2 核心,先保数据)**
1. DDL:qe_archive macb **五表**(run/leg/leg_source/scheme/loo)+ COMMENT（F-001）；migration forward/rollback。
2. `partial_failed` 入业务表 status 约束 + reason.logical_status 回填（F-009 schema 部分）。
3. 腿来源解析器:seed → (experiment_id, loop_id, loop_index, run_type),兼容 `qear_run_*`(直查 qe_archive.run)与 `qe_*_Lx`(解析 task+index 查 qe_evolution_loops/archive）两种格式，失败显式记录（F-015）。
4. MultiAlphaCombineArchiveHandler：读业务表 + 物化腿快照 + leg_source 精确溯源（F-002/003/005/015）；幂等 upsert（F-007）；缺源/解析失败显式 skip/标记（F-008）。
5. outbox 事件 emit + 白名单注册（F-004）。
6. backfill macb 分支：补历史 28 run + 溯源（F-006）；归档覆盖率 + 溯源解析率报告（F-013）。
7. 隔离验证：evolution_loop/single_experiment/paper telemetry 零回归（F-014）。

**Phase B — UI 详情(读业务表) + 数仓分析面（目标 3）**
8. UI 腿级详情面板（F-010），**读 macb 业务表 + 实时来源解析,不读数仓**。
9. 数仓跨实验分析 SQL/视图（F-011）+ 三层全链路追踪双向查询（F-016)。

**Phase C — 失败治理收口（目标 1 长期解）**
10. 对接远端派发 Phase 2 容量守卫跨来源统一（F-012，消除并发争抢失败）。
10. 修 `pred_backtest_conf_parse_failed`（YAML 构造器）真 bug。

allowed_write_scope:
- `backend/db/` `backend/migrations/`（macb 数仓 DDL）
- `backend/services/qe_archive/`（handler/backfill/source_assembler/worker 注册）
- `backend/services/multi_alpha/`（outbox emit、partial_failed 状态、conf bug）
- `frontend/src/app/quantevolver/multi-alpha/`（详情面板）
- 禁改:evolution_loop/single_experiment 归档逻辑既有行为、paper_v2 telemetry sink、qe_evolution_* 业务表。

## 4. 验证方案(Verification Plan,L0–L5)

| 层级 | 范围 | 内容 |
|---|---|---|
| L0 | 编译/迁移 | compileall;migration forward+rollback 干净 |
| L1 | 单元 | 物化函数（腿快照字段映射）、幂等 upsert、partial_failed 映射、缺源 skip、白名单注册 |
| L2 | 集成 | handler 端到端归档一个真实 macb run → 数仓四表行数/字段对账业务表；backfill 28 run |
| L3 | 隔离/DB | 断言归档期零破坏 evolution_loop/single_experiment 行;paper telemetry 不混写;outbox 无黑洞（不可归档显式 skip 计数）|
| L4 | UI E2E | 详情面板(读业务表)展示每腿配置/精确来源(exp/loop)/叠加 + 失败 reason 截图 |
| L5 | 业务 oracle | 数仓 macb 指标 == 业务表（CAGR/Sharpe/weights/marginal 逐值）；UI 显示 == 业务表；两种 seed 格式溯源到 exp/loop 对账正确；provenance 删 seed 后仍可从数仓复原|

- 覆盖率：新增 Python line≥80%/branch≥70%。
- 历史不丢断言：backfill 后 `qe_archive.multi_alpha_run` 行数 == 业务表 run 数；provenance_complete 比例报告。

## 5. 设计验收矩阵(Design Acceptance Matrix)

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 实现阶段填 | L0 migration + L2 schema | ready | - |
| F-002 | 实现阶段填 | L5 provenance 复原 | ready | - |
| F-003 | 实现阶段填 | L5 weights 对账 | ready | - |
| F-004 | 实现阶段填 | L1 白名单注册 | ready | - |
| F-005 | 实现阶段填 | L2 端到端归档 | ready | - |
| F-006 | 实现阶段填 | L2 backfill 28 run | ready | - |
| F-007 | 实现阶段填 | L1 幂等 upsert | ready | - |
| F-008 | 实现阶段填 | L3 skip 计数 | ready | - |
| F-009 | 实现阶段填 | L1 状态映射 + L4 UI | ready | - |
| F-010 | 实现阶段填 | L4 详情截图 | ready | - |
| F-011 | 实现阶段填 | L2 分析 SQL | ready | - |
| F-012 | 实现阶段填 | 对接远端派发 Phase2 | ready | - |
| F-013 | 实现阶段填 | L5 覆盖率报告 | ready | - |
| F-014 | 实现阶段填 | L3 隔离断言 | ready | - |
| F-015 | 实现阶段填 | L5 两种 seed 格式溯源对账 | ready | - |
| F-016 | 实现阶段填 | L2 双向追踪查询 | ready | - |

## 6. 回滚 / 发布(Rollout / Rollback)
- **发布顺序**:Phase A（schema+归档）→ B（UI/分析）→ C（失败治理）。每 Phase 独立 PR + 用户确认后合 main。
- **DDL 门禁**:macb 四表 + partial_failed 约束属生产 DDL，必须 migration forward/rollback + 用户授权应用，禁业务 service 隐式建表。
- **回滚**:
  - 归档为**加性**（新表 + 新事件类型 + 新 handler）；回滚 = 摘除 handler 注册 + 停 emit + drop 新表（migration rollback）；不影响 evolution_loop/single_experiment/paper。
  - backfill 幂等可重跑；误归档可按 run_id 清理。
  - partial_failed：先扩 status 约束（向后兼容,旧 failed 不变），回滚需先确认无新状态行。
- **灰度**:handler 可先 dry-run（归档到影子表/只记不写）验证物化正确，再切正式写入。

## 7. 风险与失败模式(Risks)
- **R-1 outbox 黑洞重演**：macb 事件未注册白名单或 handler 未接生产 → 静默丢失。缓解：F-004/F-005 强制白名单注册 + 生产接线 + L3 skip 计数断言（吸取 paper_v2 教训）。
- **R-2 物化不完整**：seed run 已被清理 → 腿来源无法物化。缓解:provenance_complete=false + reason 记录,**显式可见不静默**;backfill 优先在 seed 仍在时尽快补历史。
- **R-3 QE 归档回归**：改 source_assembler/worker 误伤 evolution_loop。缓解:F-014 + L3 隔离断言 + 前后行数对照。
- **R-4 partial_failed 兼容**：扩 status 约束影响既有查询。缓解:向后兼容扩展,旧值不变,UI/查询显式处理新值。
- **R-5 双链路重复**：outbox + backfill 同 run 重复写。缓解:run_id 幂等 upsert（F-007）+ L1 用例。
- **R-6 大 jsonb 膨胀**：per_window_weights（185 点）× factor_names 物化。缓解:评估行宽,必要时 factor_names 用 hash + 旁表;[MEMORY-DATAFRAME-001] 边界。

## 8. 非目标 / 边界(Non-Goals)
- 不做：combine 并发去重锁彻底方案（属远端派发/独立 issue）；macb 结果直建 Paper v2；重构 evolution_loop/single_experiment 归档模型；把 macb 塞进 evolution_loop run_type（用独立 multi_alpha_combine 类型,不复用单 loop 语义）。

## 9. 生产门禁(Production Gates)
- **DDL**:macb 四表 + partial_failed 约束 = 生产 DDL,必须 migration forward/rollback + COMMENT + 用户授权应用,业务 service 不得隐式 DDL。
- **运行时合入**:handler/emit/UI 走独立分支 + 流水线 + 用户确认;CI 通过≠设计验收通过。
- **隔离门禁**:合入前 L3 证据证明 evolution_loop/single_experiment/paper telemetry 零回归;outbox 无新黑洞(skip 显式计数)。
- **数据完整性门禁**:backfill 后归档覆盖率报告（run 数对齐 + provenance_complete 比例）作为 Phase A 验收硬证据。
- **合入前最低验证**:compileall;targeted L1/L2;`aistock_guardrail_scan --fail-on-severity P1`;`aistock_feature_workflow validate --tier F2` PASS;UI 条款 L4 截图。

---

## 附:关键代码/表索引
- macb 业务表:`strategy_pkg.multi_alpha_combine_backtest_{run,scheme_result,loo}`;UI adapter `backend/services/multi_alpha/combine_ui_adapter.py`
- 数仓:`qe_archive`（67 表）;`qe_archive.run`(run_type evolution_loop/single_experiment);`source_assembler.py`(硬编码扫 qe_experiments+qe_evolution_loops);`worker_service.py:16`(SUPPORTED_WORKER_EVENT_TYPES 白名单);`event_capture.py`(emit qe.loop/experiment.completed);`worker.py:201`(_claim_policy_skip_events 显式 skip);`handlers/`(factor_value/paper_v2)
- 失败 reason:macb `run.reason.{reason_code,logical_status,failed_child_tasks}`;主因 node_capacity_exhausted（远端派发解）+ conf_parse（真 bug）
- 远端派发(失败长期解):`multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`
