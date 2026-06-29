# QE → 模拟盘 全链路闭环 Gap 分析 + 多Alpha 策略包进模拟盘兼容设计 + 多Alpha UI 页面处置

- 日期：2026-06-28
- 作者：战略 session（AIstock + RDAgent + QE + Paper v2 总指挥）
- 范围：①QE 单次/自定义演进/多Alpha 回测 → 策略包 → 选股 → 荐股 → 模拟盘 的功能与 UI 闭环现状审计；②多Alpha 策略包以"与单Alpha 完全兼容"形式进入模拟盘/选股/荐股的实现设计（P1b-LocalSim）；③`/quantevolver/multi-alpha/{diagnostics, orthogonality, evolve-wizard}` 三页面的退役/保留/整改处置设计。
- 性质：**分析 + 设计文档，不含任何代码实现、DDL、迁移、服务启动**。所有实现走后续独立 worktree + PR + Tier2。

---

## 0. 结论速览（TL;DR）

链路当前状态（DB + 代码双重核实）：

```
QE单次实验 ✅ → 自定义演进 ✅ → 多Alpha combine回测 ✅ → 多Alpha策略包 ⚠️(路径在,有隐性前置) 
   → 选股 ✅(多Alpha可选) → 荐股 ✅(alpha无关) → 模拟盘 🔴(多Alpha被硬卡, LocalSim侧本可放行)
```

三条核心结论：

1. **单Alpha 全链路已闭环**（含 UI）。多Alpha 在"建包→选股→荐股"已闭环，**唯一真断点在"建模拟盘组合(create paper portfolio)"**：每个多Alpha 包被写死 `paper_admission.eligible=False`，且**全后端不存在把它翻 True 的 writer**。
2. **这个断点是设计上的有意闸门（P1b 未实现），不是缺执行能力**。运行时推理层（`multi_alpha_live.py` / `selection_artifact.py` / `day_runner.py`）已完整建好并能产撮合单。设计文档（`multi_alpha_paper_v2_route_architecture_20260626.md` A-2/A-3）已证实：**LocalSim 撮合路径不依赖路线 A**，因此 **LocalSim 侧的多Alpha 模拟盘可以在不等路线 A 的前提下放行**；只有 MiniQMT 真实 paper 必须继续锁在路线 A 后。
3. 三个多Alpha 页面服务的是**已废弃的 `alpha_mode=multi` 元模型架构**（DB 实测最后创建于 2026-04-26，两个月零新增），与当前生产主线 combine-backtest（今天仍在跑）是两套不同对象：
   - `evolve-wizard` → **彻底退役**（死架构入口，留着会误导）。
   - `diagnostics` → **整改重定向**到 combine-backtest 对象后保留（其诊断 UI 框架有价值）。
   - `orthogonality` → **保留并内嵌** combine-backtest 详情页（它正服务当前主线）。

---

## 0.5 背景 / 范围 / 非目标（Background / Scope / Non-Goals）

### 背景（Background）
单Alpha 策略包已打通 QE→选股→荐股→模拟盘全链路。多Alpha 自 combine-backtest 架构上线后已能建包、选股、荐股，但**无法进入模拟盘**：promotion 时把 `paper_admission.eligible=False` 固化进冻结 manifest，且全后端无放行 writer（P1b 未实现）。同时存在三个绑定**已废弃 `alpha_mode=multi` 元模型架构**的 UI 页面，需判定处置。

### 范围（Scope）
1. 全链路（QE 单次/自定义演进/多Alpha 回测 → 策略包 → 选股 → 荐股 → 模拟盘）功能与 UI 闭环现状审计（§1-§2）。
2. 多Alpha 策略包以"与单Alpha 完全兼容"形式进入模拟盘/选股/荐股的实现设计 P1b-LocalSim（§3）。
3. `/quantevolver/multi-alpha/{diagnostics,orthogonality,evolve-wizard}` 三页面退役/保留/整改设计（§4）。

### 非目标（Non-Goals / 边界）
- 本文档**不含任何代码实现、DDL、迁移、服务启动**；所有实现走后续独立 worktree + PR + Tier2。
- **不改 `PaperPortfolio` 单 `package_id` 主契约**（F-008 不回归）。
- **MiniQMT 真实 paper 不在本轮**：仍锁路线 A D4 + canary 后（P1b-MiniQMT）。本轮只放行 LocalSim venue。
- 不清理后端 `alpha_mode=multi` 存量实验数据（20 条只读保留）。

## 设计验收索引（Design Acceptance Index）

| 设计项 | 标题 | 章节 |
|---|---|---|
| F-001 | 审计：定位唯一真断点 = 多Alpha create paper portfolio | §2.7 |
| F-002 | venue 分治：LocalSim 可现放行，MiniQMT 仍锁路线 A | §3.2, §3.3 |
| F-003 | manifest 外独立 admission 记录表（不破 sha256 冻结） | §3.3-C1, §3.4 |
| F-004 | LocalSim dry-run validator 复用信号层产真证据，fail-loud | §3.3-C2, §3.6 |
| F-005 | eligibility 联合判定升级（默认 fail-closed，单Alpha 旁路） | §3.3-C4, §3.5 |
| F-006 | 与单Alpha 完全兼容（选股/荐股/模拟盘同接口同契约） | §3.1, §3.5 |
| F-007 | evolve-wizard 彻底退役 | §4.2 |
| F-008 | diagnostics 整改重定向 combine 对象后保留 | §4.3 |
| F-009 | orthogonality 保留并内嵌 combine 详情 | §4.4 |
| F-010 | 统一包抽象核实 + 多Alpha 一步导出 parity（S1） | §3.8 |

## 1. 审计方法与证据基线（Background 证据）

- 后端代码：`F:\Dev\AIstock\backend`；前端：`F:\Dev\AIstock\frontend`（Next.js App Router，`frontend\src\app`）。
- DB 实测（只读，psycopg2 读 `.env` 的 `TDX_DB_*`），2026-06-28：

| 对象 | 数量 | 最近创建 | 含义 |
|---|---|---|---|
| `qe_experiments` alpha_mode=single | 903 | 2026-06-25 | 生产主力 |
| `qe_experiments` alpha_mode=multi | 20 | **2026-04-26** | 旧元模型架构，**两月零新增** |
| `strategy_pkg.multi_alpha_combine_backtest_run` succeeded | 15 | **2026-06-28** | 当前多Alpha 主线，今天仍跑 |
| `…combine_backtest_run` failed | 13 | 2026-06-26 | 多为 node_capacity_exhausted |
| `strategy_pkg.package` | 15 全 single_alpha | 2026-06-16 | **0 条 multi_alpha 包曾被实际提升** |

关键代码证据（load-bearing）：

- `backend/services/strategy_package/multi_alpha_promotion.py:912` —— `_paper_admission()` **无条件** 返回 `{"eligible": False, "blocking": ["multi_alpha_runtime_not_validated_until_dry_run"]}`。
- 同文件 `:37` 常量 `MULTI_ALPHA_PAPER_ADMISSION_BLOCKER`；`:255` 与 `:369` 是它的两处写入点（promotion 时固化进 manifest.source_evidence.multi_alpha.paper_admission）。
- `backend/services/strategy_package/asset_eligibility.py:350-368` —— `_multi_alpha_runtime_blockers()` 从 `manifest.source_evidence.multi_alpha.paper_admission.blocking` 读出该 blocker → 输出 `hard FAIL`。
- `backend/services/paper_trading_v2/service.py:223` —— `create_portfolio` 调 `asset_eligibility_service.require_eligible(record)` → 多Alpha 包被拒。
- 运行时（已建好、能跑）：`multi_alpha_live.py:355` `MultiAlphaLivePredictionProvider.generate_artifacts`；`selection_artifact.py:397` MULTI_ALPHA 早分支；`runtime.py:312` `_has_multi_alpha_runtime` 默认 True；`paper_trading_v2/day_runner.py:383-538` alpha-mode 无关消费 authoritative artifact。
- 设计依据：`docs/analysis/multi_alpha_paper_v2_route_architecture_20260626.md` —— P1a 范围（§A-3 表）、A-2 执行层零干扰、A-3「P1b 依赖路线 A」「LocalSim 不受本决策影响，保持原状」。
- P1a 自审：`docs/handoff/multi_alpha_paper_v2_p1a_live_selection_selfaudit_20260628.md` —— 明确「P1b 真实 Paper dry-run 不在本轮范围」。

---

## 2. 全链路逐段闭环判定

约定：✅闭环 / ⚠️有缺陷可绕 / 🔴真断点。所有路由前缀 `/api/v1`。

### 2.1 QE 单次实验 ✅
- 创建 `POST /quantevolver/experiments/pending`；运行 `.../{id}/run`；同步 `.../{id}/sync-results`。
- 服务 `services/quantevolver/qe_evolution_service.py`；输出表 `qe_experiments`（`result_metrics` JSONB + 扁平 IC/CAGR 列）；产物 `pred.pkl/label.pkl` → prediction store。
- UI：`/quantevolver/experiments`、`/compose`。
- 缺陷（非阻断）：结果**拉取式**，未 sync 的完成任务显示 running、metrics 空；`pred.pkl` 上传 **env 门控**，未配则中心库 missing。

### 2.2 自定义演进 ✅
- 创建 `POST /quantevolver/evolution/custom-tasks`；运行 `.../{id}/custom-evo/run`（token `QE_CUSTOM_EVO_RUN`）。
- 表 `qe_evolution_tasks` / `qe_evolution_loops`（每 loop 一行 + 一个 `qe_experiments` 行 + 一份 `pred.pkl`）。
- loop 可由 `(qe_task_id, qe_loop_id)` 寻址，喂下游建包/combine 腿。
- UI：`/quantevolver/evolution/[taskId]/loops/[loopIndex]`。

### 2.3 多Alpha combine 回测 ✅
- 运行 `POST /multi_alpha/combine-backtest/run`（confirm `MULTI_ALPHA_COMBINE_BACKTEST_RUN`，顶层须传 topk）；查询 `.../runs/{id}`、`.../runs`；UI 适配器 `combine_ui_adapter.py` 暴露 `/combine/tasks*`。
- **腿(roster) = `leg_id` + `seed_run_ids`（QE 预测 run id），不是 package_id**；panel 由 prediction-store 预测拼（`panels.py`），combiner 按 weighting_scheme 融合。
- 表 `strategy_pkg.multi_alpha_combine_backtest_run/_scheme_result/_loo`。
- UI：`/quantevolver/multi-alpha/combine-backtest` + `[taskKey]` 详情。

### 2.4 combine run → 多Alpha 策略包 ⚠️（路径存在，有隐性多步前置）
- 端点 `POST /strategy-packages/from-multi-alpha-combine-run`（token `MULTI_ALPHA_PACKAGE_PROMOTE`）→ `MultiAlphaPackagePromotionService.promote_from_combine_run`。
- 校验：`status==succeeded`、`weighting_scheme=='ic_weighted'`、roster≥2 腿、恰一个 succeeded scheme_result 指标有限、combined prediction ref（uri+sha256）、`weight_policy.mode=='frozen_backtest_terminal_weights'`（live rolling 拒绝）。
- 产物：`strategy_pkg.package`(`alpha_mode='multi_alpha'`, 状态 `ASSET_VALIDATED`) + `strategy_package_components`(depth-1 边，触发器强制 parent=multi/child=single)。
- **隐性前置（设计需正视）**：combine 腿只带 `seed_run_ids`，提升时操作者必须**先把每条腿单独建成 single_alpha 包**（`from-qe-experiment`/`from-qe-loop`，须 frozen、sha 有效、seed 覆盖 roster），再把 `component_package_ids: {leg_id→package_id}` 传给 promotion。**无自动流**。
- **缺陷 ⚠️**：promotion 当场把 `paper_admission.eligible=False` 写进 manifest（见 §1）。

### 2.5 选股（选股中心）✅（多Alpha 可选）
- `POST /selection-center/runs`；`GET /selection-center/selectable-packages`；`.../runs/{id}/add-to-watchlist`；`.../create-paper-portfolio`。
- 服务 `selection_center/service.py` → `simulation_runtime/selection.py`；表 `selection.run/package_result/aggregate_result/excluded_result/paper_portfolio_link`。
- "可选" = `enable-selection` 的 **asset-eligibility 动态校验**（无落库标志位）。多Alpha 包靠 `selection_artifact.py:397` → `MultiAlphaLivePredictionProvider` 在线推理 → **可被选股**。
- UI：`/paper-v2/selection`（完整生产页）。
- 注意：`create_paper_portfolio_from_run`(`service.py:939`) 拒绝**多 package** run，但单个 combine 后的多Alpha 包是 `SINGLE_PACKAGE` 模式，不在此处被卡（卡在 paper v2 create_portfolio，见 2.7）。

### 2.6 荐股（advisory）✅（alpha 无关）
- `GET/POST /advisory/programs`、`.../bindings(/active|/apply)`、`/leaderboard`、`/reviews(/preview|/run)`、`/returns`、`/list-versions`。
- 服务 `advisory_program.py`；表 `app.advisory_program/_program_package/_strategy_binding_version/_review_run/_recommendation_list_version(+_item)/…`。
- 桥接：advisory review 时**实跑 selection run**（`run_packages(program.package_ids)`），把 `aggregate_results → AdvisoryCandidate`，provenance 存 `selection_run_id`（单向耦合，无 join 表）。
- advisory **alpha-mode 无关**（零 `multi_alpha` 引用），多Alpha 包像普通包一样绑定。
- UI：`/paper-v2/advisory`（完整）。
- 缺陷（装饰性）：advisory 只记 per-package 分，不出 per-leg 归因。

### 2.7 模拟盘（Paper Trading v2）🔴（多Alpha 真断点）
- 组合绑 **单 `package_id` + 冻结 `manifest_sha256`**（`models.py:45-69`, `service.py:208`）；**无 advisory program_id 绑定**（advisory 与 paper v2 是 package 的并行消费者，不串联）。
- 日级链路：`day_runner.py:383` → `selection_artifact.generate_from_live_inference` → `runtime.build_signal_snapshot` → `target_engine.build_targets` → `rebalance_engine.build_order_intents` → orders → 撮合（LocalSim 分钟撮合 `broker/localsim.py:240` 或 MiniQMTSim）。
- 多Alpha 运行时**已完整支持**（`day_runner` alpha 无关；`selection_artifact.py:397` → `MultiAlphaLivePredictionProvider` 产 `live_multi_alpha_inference_v1` authoritative artifact，同格式）。
- 🔴 **硬卡**：`create_portfolio` → `require_eligible` → `_multi_alpha_runtime_blockers` 对该 blocker 硬 FAIL → **`POST /portfolios` 拒绝任何多Alpha 包**。`_paper_admission()` 永远 `eligible:False`，**全后端无 writer 翻 True**。
- UI：`/paper-v2/portfolios`、`/running`、`/packages`、`/simulation-runtime` 均完整 —— **缺的是后端放行逻辑，不是 UI**。

### 2.8 链路全景表

| 步骤 | 单Alpha | 多Alpha | 断点性质 |
|---|---|---|---|
| 建包 | ✅ | ⚠️ 隐性多步前置 | 流程繁琐非阻断 |
| enable-selection | ✅ | ✅ | - |
| 选股出股票 | ✅ | ✅ (multi_alpha_live) | - |
| 加自选 | ✅ | ✅ | - |
| 荐股 program/review | ✅ | ✅ (无 leg 归因) | 装饰性缺陷 |
| **建模拟盘组合** | ✅ | 🔴 **BLOCKED** | **唯一真断点** |
| 模拟盘出撮合单 | ✅ | ✅(若组合能建) | 运行时已建好 |

### 2.9 次要结构问题（非阻断）

1. **`enable-selection`/`enable-paper` 的生命周期落库需复核（修正前述探子结论）**：DB 实测单Alpha 包**确有** `SELECTION_ENABLED`/`PAPER_ENABLED` 状态记录（如 `pkg_5a5ccb56...`=PAPER_ENABLED），故"这些是不可达死状态"对单Alpha **不成立**。待批 1/批 2 核实多Alpha 是否同样可达 parity；探子早期"死状态"判断仅对部分 transition 路径成立，以 DB 实测为准。
2. **状态机封顶 `BACKTEST_APPROVED`**：governance promote 上不去更高态。
3. **live rolling 权重不支持**：`weight_policy.mode='live_rolling_ic_weighted'` 无条件拒绝（"before P1 weight service"）。
4. **pred.pkl 上传 env 门控**：最脆弱的 join。

---

## 3. 架构（Architecture）：多Alpha 策略包以"与单Alpha 完全兼容"形式进入模拟盘/选股/荐股（P1b-LocalSim）

### 3.1 兼容性的本质（为什么"已经几乎兼容"）

设计文档 F-008 是地基：**`PaperPortfolio` 永远只绑一个 `package_id`，多Alpha 不改这个契约**。多Alpha parent 包对 paper v2/选股/荐股 而言就是"一个 package"，组合在信号层内部透明展开（`runtime.py` SignalSnapshot 层），执行层只收 `(instrument, score)` 序列。因此：

- 选股 ✅、荐股 ✅ 已经是"完全兼容"形态（同一个 `selectable-packages` / `program.package_ids` 接口，无 alpha 分叉）。
- 模拟盘 day runner 也是 alpha 无关的。
- **唯一不兼容的，是 `paper_admission` 这一个被写死 False 的闸门**。要做到"与单Alpha 完全兼容地进模拟盘"，本质工作 = **给这个闸门一个受控的、留痕的、可回滚的放行路径**，而不是去拆契约。

### 3.2 设计原则（硬约束）

1. **不改 `PaperPortfolio` 单 package_id 主契约**（F-008 不回归）。
2. **venue 分治**：LocalSim 撮合不依赖路线 A（设计文档 A-2/A-3 已证实），**可现在放行**；MiniQMT 真实 paper 继续锁路线 A 后（P1b-MiniQMT，本轮不做）。放行记录必须带 `venue` 维度，避免 LocalSim 放行被误用于 MiniQMT。
3. **放行 = 真实 dry-run 留证，不是改个布尔**。必须有一次受控 LocalSim dry-run 产出 component/weight/combined artifacts + target positions + orders preview 作为证据，验证通过才清 blocker（设计文档 §453-459 验收 5 条）。
4. **manifest sha256 冻结不破**：blocker 当前固化在 frozen manifest 的 `source_evidence` 内；**不能原地改 manifest**（会破 sha）。放行状态必须存在 **manifest 之外的独立 admission 记录表**，eligibility 读"包 + admission 记录"联合判定。
5. **no-silent**：dry-run 任一失败（缺 seed / 权重窗口不足 / child sha 漂移 / coverage 低 / topk 不符）→ failed + 具体 reason_code（复用 P1a 11 个 reason_code）。
6. **单Alpha 零回归**：单Alpha 包无此 blocker，新逻辑对其完全旁路。

### 3.3 放行链路设计（端到端）

```
[已有] 多Alpha包 ASSET_VALIDATED (paper_admission.eligible=False, blocking=[runtime_not_validated...])
   │
   ▼  ① 触发受控 LocalSim dry-run  (新端点, venue=localsim)
POST /strategy-packages/{id}/paper-runtime-dry-run   {venue:"localsim", trade_date, runtime_variant:"topk25|topk50"}
   │   → MultiAlphaPaperDryRunValidator
   │      复用 day_runner 信号层(只到 build_order_intents 的 preview, 不真撮合/不真下单)
   │      产 component artifacts + weight artifact + combined selection artifact + target positions + orders preview
   │      确定性: 同 manifest/runtime_config/trade_date 重跑 combined score 逐行一致
   │      失败 → reason_code, 不写 admission
   ▼
② 写独立 admission 记录(manifest外, 不破sha)
strategy_pkg.multi_alpha_paper_admission  (package_id, manifest_sha256, venue, runtime_variant,
   eligible=true, dry_run_run_id, artifact_shas, validated_at, validated_by, evidence_json)
   │
   ▼  ③ eligibility 联合判定升级
asset_eligibility._multi_alpha_runtime_blockers(manifest, *, venue, admission_reader):
   if 包是multi且blocking非空:
       查 admission 记录(package_id+manifest_sha256+venue)
       命中 eligible=true → 清该 blocker(返回 PASS)
       未命中 → 维持 hard FAIL  (默认仍 fail-closed)
   │
   ▼  ④ create_portfolio(venue=localsim) 通过 require_eligible → 多Alpha组合可建
   │     MiniQMT venue: admission 无 localsim→miniqmt 迁移, 仍 FAIL (锁路线A)
   ▼
⑤ (可选)生命周期落库: PAPER_ENABLED 由死状态激活, 仅在该 venue admission 通过后允许 transition
   │
   ▼  ⑥ day runner 正常出撮合单(LocalSim), 与单Alpha同路
```

### 3.4 契约：需要新增/修改的组件清单（Contracts — API/DB/UI/MCP，设计级非实现）

| # | 组件 | 类型 | 说明 | 兼容性保证 |
|---|---|---|---|---|
| C1 | `strategy_pkg.multi_alpha_paper_admission` 表 | 新 DDL（走 DDL gate） | manifest 外的放行记录，带 `venue`/`runtime_variant`/`dry_run_run_id`/`artifact_shas`/`evidence_json`，唯一键 `(package_id, manifest_sha256, venue, runtime_variant)` | 单Alpha 不写此表 |
| C2 | `MultiAlphaPaperDryRunValidator` | 新服务 | 复用 day_runner 信号层做 LocalSim dry-run preview，产 artifacts+targets+orders preview，确定性校验，fail-loud | 不触执行层；不真撮合 |
| C3 | `POST /strategy-packages/{id}/paper-runtime-dry-run` | 新端点 | confirm token；入参 `venue/trade_date/runtime_variant`；成功写 C1 | 仅 multi_alpha 包受理，single 包 400 不适用 |
| C4 | `asset_eligibility._multi_alpha_runtime_blockers` 升级 | 改 | 增 `venue` + admission_reader 参数，联合判定；**默认 fail-closed** | single 包提前 return（`alpha_mode!=multi_alpha` 直接 []），零回归 |
| C5 | `paper_trading_v2/service.create_portfolio` | 改 | `require_eligible` 传入 portfolio 的 `venue`，按 venue 查 admission | 单Alpha 路径不变（无 blocker） |
| C6 | 生命周期激活（可选，第二批） | 改 | `PAPER_ENABLED` 仅在对应 venue admission 通过后允许 transition；把死状态接活 | 与现有状态机兼容追加 |
| C7 | promotion 文案微调 | 改 | `_paper_admission()` 注释/常量说明"由 venue-aware dry-run 清除"，blocker 文案不变（兼容已存包） | 不改返回值，存量包仍 fail-closed |

### 3.5 与单Alpha"完全兼容"的逐项对照（验收口径）

| 接口/页面 | 单Alpha 行为 | 多Alpha 放行后行为 | 是否一致 |
|---|---|---|---|
| `selectable-packages` | eligible 即出 | 同（已一致） | ✅ |
| `selection-center/runs` | 跑包出股票 | 同（multi_alpha_live 透明） | ✅ |
| advisory 绑包/review | 按 package_id | 同（alpha 无关） | ✅ |
| `create paper portfolio` | require_eligible 通过 | **dry-run 通过后**同样通过 | ✅（多一步留证前置） |
| day runner 出单 | 标准链路 | 同链路 | ✅ |
| portfolio 契约 | 单 package_id | 单 package_id（parent 包） | ✅ |

差异仅一处且是**有意的合规前置**：多Alpha 包进模拟盘前须跑一次 LocalSim dry-run 留证（单Alpha 无需）。这是风控要求不是不兼容 —— 对应设计文档 F-009「real Paper dry-run」。

### 3.6 分批实施建议

- **批 1（P1b-LocalSim 放行，本设计核心）**：C1–C5。让多Alpha 包能跑通 LocalSim dry-run → 清 blocker → 建 LocalSim 组合 → 出撮合单。验收 = 设计文档 §453-459 五条（含"单 a1 单腿 dry-run 继续通过"回归）。
- **批 2（生命周期落库）**：C6，把 `PAPER_ENABLED` 等死状态接活（顺带修 §2.9.1 装饰性状态）。可与批 1 合并或紧随。
- **批 3（P1b-MiniQMT，本轮不做）**：MiniQMT venue admission，**必须等路线 A D4 + canary 稳定**，且不与"路线 A 影子 N=1 攒证"同批交易日（设计文档 §575 时序约束）。
- **批 4（P2 运营化）**：advisory per-leg 归因、live approval 展示 multi-alpha readiness、drift monitor（child retired / artifact stale）。

### 3.7 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| LocalSim 放行被误用到 MiniQMT | 绕过路线 A 锁 | admission 带 `venue`，MiniQMT 查不到 localsim 记录 → 仍 fail-closed |
| 原地改 manifest 破 sha | 包不可复现 | admission 存 manifest 外独立表，eligibility 联合判定 |
| child 包漂移后 admission 仍有效 | parent 不可复现 | admission 唯一键含 `manifest_sha256`；parent manifest 固化 child sha，漂移即新 sha → admission 不命中 → 重新 dry-run |
| dry-run 用 fake provider 蒙混 | 假证据 | 验收要求真实 LocalSim 信号层产物 + 确定性逐行一致 + orders preview，禁 fake |
| 单Alpha 回归 | 误伤生产 | C4 对 `alpha_mode!=multi_alpha` 提前 return []；回归测试单Alpha paper 全绿 |

---

### 3.8 统一包抽象核实 + 多Alpha 一步导出 parity（F-010 / S1）

> 目标（用户要求）：未来在模拟盘、选股、荐股中，**单Alpha 与多Alpha 是同一种策略包**，可**直接从 QE 单Alpha 实验或多Alpha 实验导出成策略包**，然后进入模拟盘/选股/荐股。

#### 3.8.1 核实结论：包抽象**已经统一**（架构层无需重做）

代码核实（`backend/services/strategy_package/models.py:234-310`）：单/多Alpha 共用**同一 `StrategyPackageManifest`**，由 `alpha_mode ∈ {single_alpha, multi_alpha}` 判别：

- `alpha_components`：单 = 恰 1 个；多 = ≥2 个（`:271-274`）。
- `alpha_combination_policy`：单 = `identity`(权重恒 1.0，`:287-293`)；多 = `ic_weighted` 等。
- 单/多落**同一张 `strategy_pkg.package` 表**，同一 manifest 冻结/sha256 机制。

消费层对 alpha_mode **基本无感**（F-008 单 `package_id` 契约）：

- paper `day_runner.py`：**0 处** alpha_mode 分支。
- `selection_center/service.py`：`SelectionMode.SINGLE_PACKAGE` 是"选股跑几个包"，**与 alpha_mode 无关**；多Alpha 父包被当普通单包跑（`:159/792/939/1213`）。
- advisory：alpha 无关（零 multi_alpha 引用）。
- **唯一真分支 = `strategy_package/runtime.py` 信号快照层 7 行**（`:136/197/268-286`），把 multi_alpha 派给 `MultiAlphaLivePredictionProvider`，下游执行层只收 `(instrument, score)`。这是**有意的统一接缝**，非两套并行。

→ **"模拟盘/选股/荐股把单与多当同一种包"在架构层已成立**，不需要拆双轨。距用户完整愿景的缺口集中在**导出对称性**与**准入打通**。

#### 3.8.2 缺口：导出不对称（S1 核心）

| 维度 | 单Alpha | 多Alpha（现状） |
|---|---|---|
| 导出调用 | 一次 `from-qe-experiment` / `from-qe-evolution-loop` | 三步：①每条腿先单独建 single_alpha 包 → ②要有 combine-backtest run → ③`from-multi-alpha-combine-run` 还须手传 `component_package_ids` |
| SourceType 血缘 | `QE_EXPERIMENT`/`QE_EVOLUTION_LOOP`（`models.py:13-16`） | **枚举无 `multi_alpha_combine_run` 值** |
| UI 导出入口 | 实验页有导出 | **无**（combine 前端零导出按钮，多Alpha 导出 MCP-only） |

术语对齐：当前架构里**"多Alpha 实验"= combine-backtest run**（旧 `alpha_mode=multi` 元模型实验已废弃、无导出路径，见 §4）。故"从多Alpha 实验导出" 实指"从 combine-backtest run 导出"。

#### 3.8.3 S1 设计（多Alpha 一步导出 parity）

| # | 组件 | 类型 | 说明 |
|---|---|---|---|
| S1-1 | `from-multi-alpha-combine-run` 自动建 component 包 | 改 promotion 服务 | 由 roster 各腿 `seed_run_ids` **自动物化/复用** component single_alpha 包，免操作者手传 `component_package_ids`；已存在等价 frozen 单包则复用（按 seed 覆盖 + sha 匹配），缺失则自动建。幂等。 |
| S1-2 | `SourceType.MULTI_ALPHA_COMBINE_RUN` | 改枚举 + 血缘 | 正式血缘值，`source_id` = combine run_id；manifest.source 如实记录，便于审计与 UI 反查。 |
| S1-3 | combine-backtest 详情页"导出为策略包"按钮 | 前端 | 与单Alpha 实验导出**对称**；选 scheme(ic_weighted)+runtime_variant → 调 `from-multi-alpha-combine-run`。 |
| S1-4 | 统一导出入口语义 | UI/MCP | "导出策略包"入口并列两个 source：QE 单Alpha 实验 / 多Alpha combine run；导出后均落同一包列表、同一后续流程（选股/荐股/模拟盘）。 |

兼容性保证：S1 **不改 manifest 结构**（已统一）、**不改 F-008 契约**、**不改单Alpha 导出路径**（仅补多Alpha 对称入口）。S1-1 自动建包须 fail-loud（seed 缺失/sha 漂移/腿不可解析 → 具体 reason_code，禁静默兜底）。

#### 3.8.4 S1 与批 1 的关系（可并行）

- 批 1（C1–C5，已派 Codex）：改 `paper_admission`/`asset_eligibility`/`paper_trading_v2`，打通**准入**。
- S1（F-010）：改 `multi_alpha_promotion`/`SourceType`/combine 前端，打通**导出对称**。
- 两者**无文件冲突**，可并行。先后顺序不限；完整愿景 = 批 1（进得去）+ S1（导得出且对称）+ 批 2（生命周期 parity）。

## 4. 设计：三个多Alpha UI 页面处置

### 4.1 处置依据

三页面服务对象 = `alpha_mode=multi` 元模型实验（单实验内 meta-model 套多因子组），DB 实测 **2026-04-26 后零新增**；当前生产主线已全面转向 combine-backtest（独立单腿事后融合，今天仍跑）。R19 结论已判定"所有因子塞一个模型"路线失败。`strategy_pkg.package` 实测 0 条 multi_alpha 包 —— 即三页面下游从未真正落地过包。

### 4.2 `evolve-wizard` → 彻底退役

- 文件：`frontend/src/app/quantevolver/multi-alpha/evolve-wizard/page.tsx`（971 行，mtime 2026-04-26）+ `components/MultiAlphaGroupEditor.tsx`。
- 它产出的是 `alpha_mode=multi` 实验（`POST /quantevolver/config/generate {alpha_mode:"multi", dispatch_mode:"independent", parent_multi_alpha_id}`）—— **死架构的创建入口**。
- 退役动作：
  1. 删 nav 入口（`lib/navigation/nav-groups.ts:30`）。
  2. 删 `compose/page.tsx:2187` 与 diagnostics 详情页的入口链接（推荐按钮）。
  3. 路由页改为退役占位（指向 combine-backtest）或直接删除路由 + 组件。
  4. 后端 `alpha_mode=multi` 的 `config/generate` 分支**本轮不删**（存量 20 实验只读需要），仅断 UI 入口；后端清理列入更后批次。
- 风险：低。无下游依赖（0 multi_alpha 包）。退役前 grep 确认无其它入口。

### 4.3 `diagnostics` → 整改重定向后保留

- 文件：index `…/diagnostics/page.tsx`（mtime 2026-04-16）+ 详情 `…/diagnostics/[expId]/page.tsx`（1190 行，mtime 2026-06-06）。
- 详情页的诊断 UI 框架**有价值**：组间预测相关、meta-weight 条、统一回测执行证据（run-id/node/artifact）、组合回测指标、覆盖率、瓶颈识别、优化建议。
- 问题：它绑 `GET /quantevolver/multi-alpha/{expId}/diagnostics`（旧实验对象），与 combine-backtest 详情页功能重叠（都是"看一个多Alpha 对象的指标/相关/回测"）但对象不同 → 两个详情页并存。
- 整改方向（择一，建议 A）：
  - **A（推荐）**：把诊断 UI 框架**重定向到 combine-backtest 对象**。后端已有 `GET /selection-center/runs/{id}/fusion-diagnostics` 与 combine `_scheme_result/_loo`；把详情页的"组间相关/瓶颈/优化建议"接到 combine run 的腿/scheme 数据上，与 combine-backtest `[taskKey]` 详情页**合并为一页**（诊断作为详情页的一个 tab）。index 页退役（被 combine-backtest 列表取代）。
  - **B（保守）**：维持现状只读保留，加"基于已废弃 alpha_mode=multi 架构，仅供存量 20 实验回看"的醒目标注，不再从主 nav 入口（移到归档子菜单）。
- 风险：A 是整改工作量（前端重接 + 可能补 combine 诊断端点），但消除双详情页；B 零工作量但留技术债。

### 4.4 `orthogonality` → 保留并内嵌

- 文件：`…/orthogonality/page.tsx`（256 行，mtime 2026-06-19）+ `lib/multi-alpha/orthogonality.ts`。
- 绑 `GET /multi-alpha/orthogonality?run_ids=…&k=N`，读 qe-archive prediction-store —— **服务当前 combine 主线**（择腿前看预测相关 + TopK Jaccard 重叠），是正交化方法论的唯一工具，今天仍配套在用。
- 处置：**保留**。优化（非必须）：它本该被 combine-backtest 详情页直接复用 —— 把正交性矩阵做成 combine-backtest 详情页/建腿流程内嵌的一个 tab（择腿时即看相关性），而非孤立的"粘贴 run_id"入口。孤立入口可保留为高级工具。
- 风险：极低。纯增强，不退役。

### 4.5 三页面处置总表

| 页面 | 绑定架构 | DB 活跃 | 处置 | 工作量 | 风险 |
|---|---|---|---|---|---|
| evolve-wizard | 旧(alpha_mode=multi) | 04-26 冻结 | **彻底退役** | 低(断入口) | 低 |
| diagnostics | 旧(alpha_mode=multi) | 04-26 冻结 | **整改重定向 combine** (A) 或归档保留(B) | 中(A)/零(B) | 中(A)/技术债(B) |
| orthogonality | 新(combine) | 06-19 在用 | **保留** + 内嵌 combine 详情(可选) | 零(保留)/低(内嵌) | 低 |

---

## 5. 实施方案 / 排期建议（Implementation Plan，待用户拍板优先级）

1. **优先级 1（用户已指定）**：批 1 P1b-LocalSim 放行（§3.6 C1–C5）—— 打通多Alpha 进模拟盘，与单Alpha 完全兼容。派 Codex 实现 → 我 Tier2。
2. **优先级 2**：三页面处置 —— evolve-wizard 退役 + diagnostics 整改(A) + orthogonality 保留。可与批 1 并行（前端 vs 后端，零冲突）。
3. **优先级 3**：批 2 生命周期落库（C6，顺带修死状态）。
4. **暂缓（依赖路线 A）**：批 3 P1b-MiniQMT、批 4 P2 运营化。

---

## 6. 验证方案（Verification Plan，本设计文档自身）

1. `python scripts/aistock_feature_workflow.py validate --design <本文档> --tier F2`（若该校验器对纯分析文档适用）。
2. `git diff --check`。
3. `git diff --name-only` 确认本轮**只新增本分析文档**，无后端/前端/迁移/运行时改动。

## 7. 生产门禁声明

- `production_ddl_gate=noop`：本文档不交付 DDL（C1 表在实现阶段单独进 DDL gate）。
- `production_frontend_dependency_gate=noop`：不改前端。
- `production_backend_dependency_gate=noop`：不改后端依赖。
- 不启/重启服务，不写生产 DB，不执行 DDL/DML，不触碰 Research Assistant。

## 8. Rollout / Rollback（发布与回滚）

### Rollout（实现阶段，本文档不执行）
1. 批 1（C1–C5）合并后：多Alpha 包仅在 **LocalSim venue** 跑通 dry-run 留证后可建组合；MiniQMT venue 仍 fail-closed。
2. dry-run 通过才允许把该 parent package（按 venue + runtime_variant）transition 到 `PAPER_ENABLED`（批 2 C6）。
3. paper auto-run 必须显式选择该 parent package 与 runtime_config hash；topk25/topk50 各自独立 admission 记录与 dry-run 证据。
4. 单Alpha 路径全程不变（无 blocker、不写 admission 表）。

### Rollback（回滚）
1. 关 `multi_alpha_live_inference_enabled` feature flag → 多Alpha selection artifact 生成 fail-loud，**不影响单Alpha**。
2. 删/失效对应 venue 的 admission 记录 → eligibility 立即回到 fail-closed，多Alpha 组合无法新建。
3. portfolio 可回切已验证的单腿包（portfolio 仍绑单 package_id，无需迁移 schema）。
4. 保留所有 component/weight/combined artifacts 与历史 dry-run 证据，不删除。

## 9. 设计验收矩阵（Design Acceptance Matrix）

> 本文档为**分析+设计**交付物；status 口径针对"设计是否完成并自洽"，非"实现是否完成"。实现验收在后续 PR 的自审矩阵中按 §3.6 验收条目逐项 pass。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §2.7；`multi_alpha_promotion.py:912`、`asset_eligibility.py:350-368`、`paper_trading_v2/service.py:223` | DB 实测 0 条 multi_alpha 包 + grep 确认无 `eligible:True` writer，断点唯一性已坐实 | done | - |
| F-002 | §3.2、§3.3；route doc A-2/A-3（LocalSim 不依赖路线 A） | 设计文档 A-2 代码核实执行层零干扰；venue 分治方案已定 | done | MiniQMT venue 本轮 non-goal，approved deviation（用户指定优先打通模拟盘，LocalSim 先行） |
| F-003 | §3.3-C1、§3.4 C1 | 设计 admission 表存 manifest 外，唯一键含 manifest_sha256，不破冻结 sha | done | - |
| F-004 | §3.3-C2、§3.6 批1 | dry-run 复用 day_runner 信号层产 component/weight/combined artifacts + targets + orders preview；确定性逐行一致；禁 fake | done | - |
| F-005 | §3.3-C4、§3.5；`asset_eligibility.py:351`（alpha_mode!=multi 提前 return） | 联合判定默认 fail-closed；单Alpha 提前 return [] 零回归 | done | - |
| F-006 | §3.1、§3.5 兼容对照表 | F-008 单 package_id 契约不变；选股/荐股已一致，模拟盘放行后同接口 | done | - |
| F-007 | §4.2 | evolve-wizard 绑死架构（DB 04-26 零新增、0 下游包），退役动作清单已定 | done | 后端 alpha_mode=multi 分支本轮不删（存量只读），approved deviation（用户指定本轮只断 UI 入口） |
| F-008 | §4.3 | diagnostics 诊断 UI 框架有价值，整改重定向 combine 对象（方案 A 推荐/B 保守）已定 | done | - |
| F-009 | §4.4 | orthogonality 服务当前 combine 主线（06-19 在用），保留 + 可选内嵌 combine 详情 | done | - |
| F-010 | §3.8；`models.py:234-310`（统一 manifest）、`runtime.py:136-286`（唯一接缝）、`models.py:13-16`（SourceType 待补）、combine 前端无导出按钮 | 核实单/多共用同一 manifest+表+消费契约，包抽象已统一；S1-1~S1-4 设计补多Alpha 一步导出 parity | done | - |


- 闸门写死：`backend/services/strategy_package/multi_alpha_promotion.py:37,255,369,912`
- eligibility 读闸：`backend/services/strategy_package/asset_eligibility.py:350-368`
- portfolio 强制：`backend/services/paper_trading_v2/service.py:223`
- 运行时(已建好)：`multi_alpha_live.py:355`、`selection_artifact.py:397`、`runtime.py:312`、`day_runner.py:383-538`
- 选股多package拒绝(另一处)：`selection_center/service.py:939`
- 权威设计：`docs/analysis/multi_alpha_paper_v2_route_architecture_20260626.md`（P1a §441-459，A-2 §553-562，A-3 §564-575）
- P1a 自审：`docs/handoff/multi_alpha_paper_v2_p1a_live_selection_selfaudit_20260628.md`
- 前端三页面：`frontend/src/app/quantevolver/multi-alpha/{diagnostics,orthogonality,evolve-wizard}/`；nav：`frontend/src/lib/navigation/nav-groups.ts:27-30`
