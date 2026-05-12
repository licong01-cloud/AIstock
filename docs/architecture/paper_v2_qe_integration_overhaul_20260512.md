# Paper v2 / QE Integration 整改方案

**日期**: 2026-05-12
**类型**: 架构整改 (用户主导决策, Codex 主导设计 + 实施)
**作者**: User vision + Strategy session 整理
**状态**: 待 Codex 设计具体方案后实施
**前提**: 12 天 Sprint 后 paper-v2 完全不可用, 12 个 prod packages 全部无法选股. 这是设计断档导致, 必须根本性整改.

---

## §1 用户期望的完整流程

### 1.1 业务流 (User Vision)

```
用户在 UI 选择
  ├─ 因子集 (factor library)
  ├─ 模型 (model registry / aistock_model_catalog)
  ├─ 策略 (strategy template)
  └─ 配置 (资金 / 起止日期 / 风控参数 / 行情源 / etc)
       ↓
启动 QE 实验
  ├─ 单次实验 (single-shot backtest)
  └─ 自定义演进 loop (iterative refinement)
       ↓ (Qlib 回测引擎)
查看回测结果
  ├─ 收益率 / Sharpe / Max DD / 因子贡献度
  ├─ 历次实验对比
  └─ 选股 trail (历史选股记录)
       ↓
用户选最优实验 (单次实验 OR loop 内某 iteration)
       ↓ 一键转入 paper-v2
paper-v2 自动:
  ├─ 提取 QE 实验完整配置 (factor + model + weights + scoring policy)
  ├─ 加载真实行情 (TDX/DB minute bars)
  ├─ 跑选股 (与 QE 实验逻辑等价)
  ├─ LocalSim 撮合 (with realistic slippage / lot rules / 涨跌停 / cost)
  └─ 输出 fills + audit chain
       ↓
用户验证:
  ├─ paper-v2 选股 vs QE 回测选股 (逐日对比, 容忍小数 epsilon)
  ├─ paper-v2 fill 价格 vs QE 回测假设价格 (差异分析)
  └─ paper-v2 持仓收益 vs QE 回测持仓收益
       ↓ (验证回测真实性)
后续: 实盘 (R-Q9.1 决策后, 加 miniqmt_live broker)
```

### 1.2 流程不变量 (Invariants)

| 不变量 | 说明 |
|---|---|
| **QE 配置 ≡ paper-v2 配置** | factor list / model / weights / scoring / portfolio rules 完全一致 |
| **回测 score ≡ paper-v2 score** | 同一 trade_date 同一股票得分相同 (允许浮点精度容差) |
| **可逆性** | 任何 paper-v2 portfolio 可追溯到源 QE 实验 + iteration |
| **审计完整** | 选股 / fill / capture 字段 / audit chain 全保留 |
| **未来兼容**: 实盘门禁保留 | R6 governance gate / Selection Center health 是为**真实盘**准备, **不废除**, 但与 paper-v2 解耦 |

---

## §2 当前实际状态 (Sprint 2026-05-11~12 后)

### 2.1 各模块状态

| 模块 | 代码实现 | 集成可用 |
|---|---|---|
| factor library | ✅ | ✅ |
| model registry (aistock_model_catalog) | ✅ | ✅ |
| QE evolution (single-shot + loop) | ✅ | ⚠️ event loop 阻塞 (Codex Task 18 修复中) |
| Qlib 回测调用 | ✅ | ✅ |
| strategy_package (manifest + assets + variant + validation_run + status_event) | ✅ | ⚠️ legacy QE 实验产 manifest 无 ST PIT contract |
| Selection Center (health + live inference + artifact cache) | ✅ | ❌ 严格 enforce ST PIT, legacy package 全 BLOCKED |
| R6 governance gate (enable_paper) | ✅ | ❌ 要求 evidence (4 类), legacy package 全 0 evidence |
| paper-v2 daemon (capture + outbox + LocalSim) | ✅ | ❌ 没有 PAPER_ENABLED package 可跑 |
| Frontend UI (validation / qe / paper-v2 / strategy / etc) | ✅ | ⚠️ 后端门禁返 400, UI 走不通 |
| QE 实验 → strategy_package manifest 提取 | ⚠️ | legacy QE 不产 ST PIT contract |
| ModelAssetResolver (Linux → Windows model 路径) | ✅ | ⚠️ 新算法 (V25_1_SMALL_CAP) cache 漏建 (已 Phase A 修) |
| Stock universe PIT | ✅ | ❌ 在 QE submit hot path, 阻塞 event loop |

### 2.2 端到端 lifecycle 状态

**从未跑通过一次完整流程**:
- 历史所有 E2E 测试用 mock + synthetic fixtures
- R5/R6/R7 各 module 独立 GREEN, 但**组合验证 Gate 从未存在**
- 12 个 prod packages 全部 BACKTEST_APPROVED, **0 个能 PAPER_ENABLED 真跑**

---

## §3 端到端断档问题 (设计层缺陷)

### 3.1 主要断档点

```
QE 实验 → strategy_package
  ❌ legacy QE 实验不产 ST PIT risk_policy contract
  ❌ Backtest 结果不自动写 strategy_pkg.package_validation_run
  ❌ Regime metrics 不自动按 market.regime_label 切分计算
  ❌ Multi-seed retrain (R6 要求 ≥ 2 ORIGINAL_RETRAIN) 不自动跑
  ❌ Model weight artifact 不自动 export + 注册 package_asset
  ❌ Runtime variant (risk_policy) 不自动创建

strategy_package → enable_paper (R6 governance gate)
  ❌ Evidence 4 类全缺 (上面 6 个 ❌ 直接导致)
  ❌ Gate 设计为 mandatory, 没 opt-out for paper-v2 (vs 实盘)

enable_paper → Selection Center
  ❌ ST PIT contract enforce, legacy manifest 无
  ❌ Selection artifact 不自动 cold-start 生成
  ❌ HMM coefficient (if enabled) 不自动 preflight

Selection Center → paper-v2 daemon
  ❌ 没 PAPER_ENABLED package, daemon 无可启动 portfolio
  ⚠️ 即使启动, 也无 selection score 输入

paper-v2 daemon → fills
  ✅ LocalSim 撮合本身 OK
  ✅ Capture 字段 (T5/T6.1/T6.2) OK
  ✅ Outbox emit OK
  ✅ Audit chain OK
  ❌ 但全链路从未真跑过一次
```

### 3.2 设计层 root cause

1. **缺 end-to-end ownership**: 各模块各自正确, 但**模块间数据流没人负责**
2. **门禁定位错配**: R6 gate / Selection Center health 按"实盘级别"设计, 加在 paper-v2 上 → 永远过不了
3. **数据流未定义**: 各模块声明要什么数据 (evidence 4 类 / artifact / contract), 但**没人定义这些数据从哪里生成**
4. **QE 实验产物与 paper-v2 期望不匹配**: 
   - QE 实验输出: backtest 结果 + model checkpoint + score CSV (legacy 模式)
   - paper-v2 期望: ST PIT manifest + 4 类 evidence + Selection artifact + HMM coef
   - **中间没人翻译**
5. **回测 vs 模拟盘等价性未实现**: 
   - QE 用 Qlib 回测 (calendar 提取 + factor + model.predict + portfolio)
   - paper-v2 daemon 用 LocalSim (TDX 行情 + factor 重算 + model.predict + 撮合)
   - **两边 score 计算 path 不同, 等价性未保证**

### 3.3 用户视角的不可用症状

```
用户启动 QE 实验
  → 实验跑完
  → 看回测结果 ✓
  → 转入 paper-v2 → 添加策略包
  → ❌ DATA_UNAVAILABLE: V25_1_SMALL_CAP early_model_path (Phase A 已修)
  → ❌ STRATEGY_PACKAGE_VALIDATION_ERROR: Selection Center health preflight (设计 over-strict)
  → 用户在所有可能策略包都 fail
  → paper-v2 daemon 启动不起来
  → 无法验证回测真实性
```

---

## §4 整改方向 (高层, Codex 设计具体方案)

### 4.1 核心原则

1. **保留实盘门禁**: R6 governance / Selection Center health / 5-guard / ST PIT contract enforce **不废除**, 留给真实盘 (miniqmt_live 接入后启用)
2. **paper-v2 解耦门禁**: paper-v2 默认 **lenient mode** (验证回测真实性, 不审批策略), 实盘 **strict mode**
3. **填补数据流**: 明确每个模块要的数据从哪里生成 + 自动化 pipeline
4. **保证等价性**: QE 实验 ↔ paper-v2 配置完全一致, score 计算 path 统一
5. **End-to-end ownership**: 1 个 owner 负责跨模块流程通畅 (建议 Codex 接管)

### 4.2 整改 4 大方向

#### 方向 1: paper-v2 / 实盘双模式

- `package_status` 增加细粒度: `PAPER_ENABLED_LENIENT` (paper-v2 only, evidence optional) vs `PAPER_ENABLED_STRICT` (实盘前置, evidence mandatory)
- R6 governance gate 改 mode-aware:
  - LENIENT: 仅检查 manifest sha256 + 基础 status
  - STRICT: 检查全 4 类 evidence + ST PIT contract + protected_asset_ledger
- Selection Center health preflight 改 mode-aware:
  - paper-v2 mode: 仅检查 manifest + factor + model 存在
  - 实盘 mode: 全 7 health checks

#### 方向 2: QE → paper-v2 直通 pipeline

- 用户在 UI 选 QE 实验某 iteration → "Send to paper-v2" 按钮
- Backend 自动:
  1. 提取 QE manifest (factor + model + weights + scoring + portfolio rules)
  2. Freeze 为 strategy_package_manifest
  3. 注册 package_asset (model weight artifact, ResolverAuto-resolve)
  4. 注册 runtime_variant (默认 lenient risk_policy)
  5. Set status = PAPER_ENABLED_LENIENT (跳过 R6 strict gate)
  6. 触发 paper-v2 portfolio create + daemon start
- 整个流程 **1 click, 无人工 evidence 准备**

#### 方向 3: 等价性保证 (QE 回测 ↔ paper-v2 选股)

- 提取 QE 实验的 **scoring pipeline definition** (factor list + transform + model.predict + ranking + portfolio constructor) 作为 strategy_package_manifest 一部分
- paper-v2 daemon 加载 manifest 后, 用**同一 pipeline** 在真实行情上跑
- 加 verify mode: paper-v2 启动时跑 1 个历史 trade_date, 比对 QE 回测同日选股 (允许 epsilon)
- 不一致 → BLOCK, 不一致原因输出 diff

#### 方向 4: 端到端 ownership + 测试

- Codex 接管 end-to-end ownership (整改期间不分多 agent 工作)
- 每个改动跑 1 次真实 case 端到端验证 (不只 isolated unit test)
- UI 自动化测试 (Playwright) 守住"不退化"
- bugs/ JSON + GitHub Issues 集成追踪每个真实 user-reported issue

---

## §5 Codex 26 个任务后续安排

### 5.1 立即重新优先级 (Codex 接手后调整)

| Task | 当前状态 | 整改后建议 |
|---|---|---|
| **Task 18** P0 event loop hotfix | in_progress | ⚡ **继续** (生产阻塞, 完成后 cherry-pick to main) |
| **Task 26** execution algo Phase B (5 tracks) | pending | 继续 (model artifact 自动化与本整改 §4.2 方向 2 衔接) |
| Task 14 archive register doc | pending | 低优 (可做可不做) |
| Task 15 verify rollback script | pending | **取消** (synthetic rollback 不再需要, 走 lenient mode) |
| Task 16 baseline post-cleanup main | pending | 继续 (流水线兜底验证) |
| Task 17 Selection Center analysis | pending | **取消** (本文档已 cover, 不需重复 design) |
| Task 19 ST PIT async design | pending | **整合到 §4 方向 1 (双模式)**, 不单独做 |
| Task 20 5-缺陷 RCA + 重构 design | pending | **整合到本文档**, 不单独 4h |
| Task 21 全库 async/sync audit | pending | 继续 (P0 价值 - 防其他类似阻塞) |
| Task 22 R7 roadmap + bug tracker | pending | **重新定义** = 本文档 + GitHub Issues 集成 design |
| Task 23 UI UAT plan | pending | **推迟到本整改方案验证后** (有可走流程才测) |
| Task 24 41 bug entries audit | pending | 继续 (与 GitHub Issues 集成衔接) |
| Task 25 QE → paper-enabled pipeline | pending | **整合到 §4 方向 2** (核心整改任务) |
| Task 119 GitHub Issues 集成 | pending | 继续 (per 现有 design doc) |
| Task 120 execution algo Phase B | pending | 同 Task 26 |
| Tasks 9/10/11/12 (旧) | various | 不在本整改范围 |

### 5.2 整改主任务 (Codex 接手设计实施)

**新 Task A**: 双模式 strategy_package + governance gate
- 设计 PAPER_ENABLED_LENIENT vs PAPER_ENABLED_STRICT 模式
- 实施 mode-aware R6 gate + Selection Center health
- 流水线验证 + cherry-pick to main

**新 Task B**: QE → paper-v2 直通 pipeline
- UI: 在 QE 实验结果页加 "Send to paper-v2" 按钮
- Backend: 提取 manifest + freeze + 注册 assets/variant + 跳过 strict gate
- 流水线验证 + 1 真实 case 端到端

**新 Task C**: 等价性保证 (scoring pipeline 统一)
- 提取 QE 实验 scoring definition 到 manifest
- paper-v2 daemon 用同一 pipeline
- 加 verify mode (1 历史 day 对比)

**新 Task D**: 端到端真实 case 走通
- 选 pkg_b2faccade8d549af9621c51d285bdc06 (现有 QE 实验产物) 作为 case
- 走完整流程: send to paper-v2 → portfolio create → daemon start → 跑 1 模拟交易日 → 验证 fills + capture + outbox + audit
- 这是**整改完成的标志**, 不是 isolated test

### 5.3 Sprint 概念取消

不再用 "R7 / R7.5 / R8" 概念。改为:
- **Phase 整改**: Task A + B + C + D 一气呵成 (~1-2 周)
- **Phase 加固**: 添加 production-grade 自动化 (R6 strict mode 启用 / 实盘准备), 在整改后逐步
- **Phase UI 测试**: 整改完成后用 Playwright + GitHub Issues 守住

---

## §6 未来协作模式

### 6.1 Strategy session (我) 的定位

**应该做** (token-efficient):
- 技术诊断 (像 ST PIT event loop / Selection Center failure / V25_1 model artifact root cause)
- 决策辅助 (给 2-3 选项 + tradeoff)
- 思路梳理 (从 symptom 到 root cause)
- 架构讨论 (复杂设计, 与 Codex 互验意见)
- 小代码改动 (≤ 2 文件)

**不应该做** (token-wasteful):
- 派 Codex N 个并行任务 (协调成本高)
- 写大型 design doc (本文档是例外, 整改前置)
- Cross-tool 4-layer review (形式主义)
- 不停 cherry-pick / commit 协调
- 替用户决策

### 6.2 Codex 的定位

**应该做**:
- 单 big task 实施 (一次 1 个, 不并行 12+)
- 设计具体方案 (基于本文档高层方向)
- 流水线验证
- 直接 push 代码 (不必每次 cherry-pick from worktree)
- 端到端 ownership 期间负责跨模块协调

**不应该做**:
- 自驱设计大架构 (等用户/我给方向)
- 写过多 design doc

### 6.3 复杂架构讨论模式

**新流程**:
1. 用户提出问题 / 需求
2. 战略 session 快速诊断 + 给 2-3 方向 (~500 tokens)
3. 如方向不明确: 战略 session 和 Codex 互相讨论方案 (drawer / 文档) — **token 投入 worth it for big decisions**
4. 用户基于讨论决策
5. Codex 实施 + 流水线 + push
6. 用户验证

**关键**: 我和 Codex 是**架构讨论的伙伴**, 不是 indirect coding pipeline. 价值在 **insight per token**, 不是 **task throughput**.

---

## §7 启动条件 (用户决策)

本文档完成后, 等用户决策:

1. **同意 §4 整改方向** (4 大方向 + 双模式 + 直通 pipeline + 等价性 + e2e ownership)?
2. **同意 §5 Codex 任务重新优先级** (取消 Task 15/17, 整合 19/20/22/25, 继续 18/21/24/26/119)?
3. **是否启动 §5.2 Task A/B/C/D 整改**?
4. **是否同意 §6 新协作模式** (战略 session 减派单, Codex 端到端 ownership)?

### 启动后 Codex 工作

Codex 接到本文档后:
1. 阅读本文档完整 §1-§6
2. 设计 Task A/B/C/D 具体实施方案 (含 schema 改动 + API 改动 + UI 改动)
3. 与战略 session 讨论 (drawer / 本 doc 评论) 任何架构疑问
4. 用户确认后实施
5. 流水线验证后 push to main
6. 端到端跑 pkg_b2fac 走通 (Task D 是验收标志)

---

## §8 反思与承诺

### 战略 session 自我反思

12 天 Sprint:
- 派 26 Codex tasks
- 写 19 design docs
- 协调 130+ cross-tool drawers
- 设计 R6 governance 严格门禁
- 设计 Selection Center 7 health checks
- 推动多窗口 dual-party verify

**结果**: 0 用户可用功能 + 几千美金 API 费用

### 承诺

1. 不再派 Codex N 任务 (本整改主任务 Codex 端到端处理)
2. 不再主动写大型 design doc (本文档是整改前置, 例外)
3. 不再设计 over-strict 门禁 (双模式: paper-v2 lenient, 实盘 strict)
4. 不再用 synthetic / mock 绕过 (真 case 走通)
5. 与 Codex 架构讨论时 token-efficient (insight per token)

### 期待用户

- 给方向 + 决策 (不需细节)
- 不期待我"指挥", 期待我"诊断 + 建议"
- 与 Codex 直接对话 (不通过我 indirect 派单)
- 在真复杂架构问题上让我和 Codex 讨论

---

## 附: 关键文件引用

| 文件 | 内容 |
|---|---|
| `docs/architecture/github_issues_integration_design_20260512.md` | GitHub Issues 集成 |
| `docs/architecture/execution_algo_model_artifact_resolution_design_20260512.md` | Model artifact 自动 sync (Task 26) |
| `docs/architecture/r7_retrain_regime_metrics_automation_design_20260512.md` | Retrain pipeline (Codex Task 12, 整合到本文档方向 2) |
| `docs/handoff/branch_audit_cleanup_plan_20260512.md` | 仓库分支审计 (已执行) |
| `docs/handoff/r6_prod_cutover_20260512_state.md` | R6 prod cutover 状态 (含 synthetic evidence rollback script) |
| `tests/aistock_validation/bugs/` | 41 BUG entries (R8 GitHub Issues import) |

main HEAD 当前: 含 R5 + R6 + cutover artifacts + 所有 design docs + branch cleanup
