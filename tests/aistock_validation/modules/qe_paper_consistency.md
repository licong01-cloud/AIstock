# QE Backtest ↔ Paper v2 一致性测试矩阵

日期：2026-05-09
归属：Claude Code 工作面（cross-adapter 一致性维护）
设计依据：`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` 附录 A.3.4 Mode G + `strategy_engine_design_20260508.md` §3.6 / §11

## 模块定位

本矩阵覆盖**同一 frozen StrategyPackage v2 manifest** 在两个执行端（QE Qlib backtest / Paper v2 LocalSim）之间的一致性 cross-test：决策侧（OrderIntent 序列）必须 byte-equal；NAV 差异由撮合层差异产生，不在本矩阵范围。

| 维度 | 取值 |
| --- | --- |
| 模块 ID | `qe_paper_consistency` |
| 风险等级 | high（cross-adapter 漂移直接破坏 Mode G 等价性，是 paper → live 切换前的物理 gate） |
| 工作面 | Claude Code 维护 cross-test 矩阵 + 测试执行；QE 端 fixture 来自 Codex 工作面（read-only） |
| 是否触动 main | 仅文档 + 测试 fixture；不修 QE / Engine 实施代码 |

## L0 静态守卫

L0 trigger：本矩阵变更 / 新加 cross-adapter case / 修改 fixture 加载脚本。

- L0-G1：本矩阵 markdown lint 通过；术语与 §A.3.4 / §11 一致
- L0-G2：禁止本矩阵 case 直接调用 QE workspace 文件系统（per `codex_project_memory.md`：QE worker filesystem 是 read-only，必须经 mutation API）
- L0-G3：禁止 fixture 含生产命名空间 ID（必须用 `pkg_dev_*` / `mfst_dev_*` / `qe_dev_*` 前缀）
- L0-G4：fixture 数据快照 sha256 必须固定（不允许"动态 fetch latest"）
- L0-G5：禁止 silent fallback / `except: pass`（参考 `feedback_no_silent_errors.md`）

pass criteria：
- L0 nox session 通过
- grep 生产 ID 模式 0 命中（`pkg_[a-f0-9]{8,}` / `mfst_[a-f0-9]{8,}` 不含 `_dev_`）
- 数据快照 sha256 校验通过

## L1 单能力

L1 trigger：单个一致性维度（OrderIntent 字段 / DecisionTrace digest / 单个 score 序列）变更。

### L1-C1：OrderIntent schema 一致性
- 用同 (spec_dev, scores_dev, portfolio_dev_a, seed_dev) 喂 Engine
- 通过 QE backtest adapter 拿 OrderIntentBatch_qe；通过 Paper v2 LocalSim adapter 拿 OrderIntentBatch_paper
- assert OrderIntent 字段集严格一致：`intent_id / package_id / portfolio_id / trade_date / symbol / side / target_quantity / target_weight / reference_price / reason / parent_intent_id / decision_trace_id`
- pass：字段缺失 / 类型差异 / 字段顺序差异任一即 fail

### L1-C2：DecisionTrace.inputs_digest 一致性
- 同输入两 adapter 跑出的 trace 的 `inputs_digest` 必须 byte-equal
- 仅改 master_seed → digest 不同（验 R-Q5 强制写入）
- 仅改 broker_compatible → digest 不同（验 R-Q9 D4 折入）
- 仅改 RuntimeOverlay 中 topk → digest 不同
- pass：digest 严格随每个 input 维度变化；对相同输入跑两次产生 byte-equal trace

### L1-C3：典型 SeedContractError 跨 adapter 一致
- 注入相同 SeedBundle 缺陷（缺 lgb 子 seed）
- 两 adapter 都必须抛 `SeedContractError`，error.context 同字段
- pass：错误类 + 关键 message 字段一致；不允许一端抛 `RuntimeError` / 一端抛 `SeedContractError`

### L1-C4：典型 BrokerCompatibilityMismatchError 跨 adapter 一致
- spec.broker_compatible="LocalSim_only" + portfolio.broker.backend_id="minqmt_sim"
- 两 adapter（QE / Paper）的 init() 都必须抛同类型 typed error
- pass：error.context 字段集一致（package_id / broker_compatible_value / target_backend_id）

### L1-C5：score 来源 provenance 一致
- ScoreFrame.source ∈ {"qe_artifact", "live_inference", "backtest_replay"}
- assert QE adapter 喂 source="qe_artifact"；Paper LocalSim 重放喂 source="backtest_replay"
- spec.manifest_sha256 与 ScoreFrame.manifest_sha256 不一致 → 两 adapter 都抛 `ScoreFrameMismatchError`
- pass：source 标签清晰；mismatch 一致 fail-fast

## L2 组件 / API / DB 流

L2 trigger：跨 adapter 完整决策流；DecisionTrace 持久化；fixture 加载 / 重放。

### L2-F1：Mode G smoke (5 case + 4 broker case，共 9 case)

源：`strategy_engine_design_20260508.md` §11 + §3.6.6。每个 case 跑同 (spec, scores, portfolio, seed) 在 QE adapter 和 Paper LocalSim adapter，比较 OrderIntent 序列。

- `xtest_modeg_smoke_lgb_single_alpha`：1 个 LGB single_alpha package + 1 日固定 score → byte-equal
- `xtest_modeg_with_dynamic_ndrop`：含 dynamic_ndrop + threshold_method=adaptive
- `xtest_modeg_with_hold_thresh`：含 hold_thresh=5 + portfolio 含未达 thresh 的 lots
- `xtest_modeg_overlay_topk`：同 manifest 不同 runtime variant（topk 改动）
- `xtest_modeg_l4_decision_determinism_strict`：同输入两次 → DecisionTrace + OrderIntent byte-equal
- `xtest_modeg_localsim_vs_minqmtsim_orderintents`：MiniQMTSim 实施后启用（v0.6 触发）
- `xtest_modeg_multi_package_localsim_isolation`：LocalSim 多 portfolio 各自独立 broker 实例
- `xtest_modeg_minqmt_capacity_reject`：MiniQMTSim 实施后启用
- `xtest_modeg_broker_compat_reject`：LocalSim_only 包配 minqmt_sim → 拒收

pass criteria（L2 严格）：
- 9 case 全跑完；前 5 case 必须 byte-equal（撮合层差异不在 cover 范围）
- byte-equal 失败 → 定位差异源（dict 迭代 / set 序列化 / float 精度 / 库版本 / 子 seed），修复至通过
- broker 维度 4 case：对应 backend 实施完成前标 `status: blocked_by_<task_id>`

### L2-F2：QE backtest 重放在 Paper v2 路径
- 走 paper_v2 `MinuteDataSource.DB_HISTORICAL`（CATCHUP_THEN_LIVE 路径子集）重放 QE 同一交易日
- assert OrderIntent 序列 byte-equal QE backtest 输出
- assert ledger 状态变化路径与 QE 持仓变化记录一致（仓位 / 现金 / NAV 计算可能因撮合差异而不同；本 case 不比 NAV）
- pass：OrderIntent byte-equal；持仓变化方向一致

### L2-F3：fixture 数据 dev namespace 隔离
- 加载 `tests/fixtures/qe_paper_consistency/` 下的 4 个测试 manifest（dev 命名空间）
- assert 所有 ID 含 `_dev_` 前缀
- assert 加载路径不触碰生产 strategy_package / portfolio / 资产目录
- pass：dev 隔离 100% 严格

### L2-F4：DecisionTrace 持久化一致
- QE adapter 与 Paper adapter 各自把 DecisionTrace 写入 `tests/aistock_validation/history/qe_paper_consistency/`
- assert trace 文件 sha256 byte-equal（同输入跑出的 trace 二次加载后应一致）
- pass：持久化层不引入额外漂移

## L3 模块 UI/API 回归

> 本模块**无独立 UI**；L3 通过 Validation Center "cross-test 状态" 列展示。

### L3-I1：Validation Center 显示 cross-test 状态
- Validation Center API `/api/v1/validation/findings?module=qe_paper_consistency&assigned_agent=...`
- 触发 L2-F1 的 9 case 跑批 → finding_store 写入对应 finding
- assert UI 显示 cross-test 进度（total / passed / blocked）
- pass：findings API 可读；UI 不显示原始 Python traceback；按 §6.3 中文映射

### L3-I2：cross-tester 角色权限边界
- 当前 session 标 cross-tester role
- 尝试 Edit/Write 到非 test/ 目录文件 → hook 拦截（参考 audit §20.6.1）
- pass：tester role 只能写 GitHub Issue + finding_store；不能修目标分支代码

### L3-I3：失败 case re-test 自动 trigger
- 注入 case fail（modify spec_dev → 跑 L2-F1）
- assert finding_store 写 finding；assigned_agent 自动设为 developer_agent
- 修复后重 commit → assert finding 状态 `NEW → FIXING → VERIFIED → CLOSED`
- pass：状态机转移完整；reopened 路径可触发

## Pass Criteria 汇总

| 等级 | 必须项 |
| --- | --- |
| L0 | dev namespace 100% 严格；fixture sha256 固定；术语扫描通过 |
| L1 | 5 类一致性维度（schema / digest / SeedContractError / BrokerCompatibilityMismatchError / score provenance）全绿 |
| L2 | Mode G smoke 9 case（前 5 case 严格 byte-equal；broker 4 case 视实施进度）；QE 重放 OrderIntent byte-equal；fixture 隔离 |
| L3 | Validation Center API + UI 一致；cross-tester 权限隔离生效；finding 状态机完整 |

## 失败处理预期

- L0 失败 → 阻断本矩阵 PR；先修 fixture / 术语
- L1 失败 → 阻断对应 adapter 实施合 main；定位差异类型后修复（必须经 §A.4.3 类别 C 硬约束流程）
- L2 byte-equal 失败 → **不允许标"adapter 已 ready"**；先修复至 byte-equal
- L3 失败 → 阻断 Validation Center release；先修 finding 状态机 / cross-tester hook

## 与 Codex 模块的边界

| 不属于本模块（Codex 范围） | 落地位置 |
| --- | --- |
| QE 回测引擎实现细节（Qlib backtest / unified executor） | `qe.md`（已有；Codex 维护） |
| StrategyPackage v2 manifest schema 修订 | `strategy_package_v2.md`（Codex 维护） |
| Master Seed Contract Phase 4 整体执行 | `qe_reproducibility.md`（Codex 维护） |
| Validation Modes A-F 主体定义（本矩阵仅"Mode G"延伸） | `qe_validation_modes.md`（Codex 维护） |
| Model Registry 4 层 schema | `model_registry.md`（Codex 维护） |

本模块仅覆盖 cross-adapter 一致性 cross-test；**不**重新定义 QE 回测语义、不修改 manifest schema、不动 Codex 治理流程。

## 取材源

- `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` 附录 A.3.4 / A.5.1 / A.5.2 Phase 4 L4 gate
- `docs/architecture/strategy_engine_design_20260508.md` §3.6 / §11 / §17.1 R-Q1
- `docs/standards/cross_test_framework_template_20260508.md` §3.5.3 跨 backend 等价性矩阵 + §2.4.2 master_seed 一致性

## Deferred Scope

- MiniQMTSim adapter 跨向一致性 case（L2-F1 后 4 case）：等 PR-005 实施
- Live adapter（minqmt_live）三向一致性：等用户授权
- 多 alpha / ensemble 路径下的等价性：等 §A.6.1 多 alpha 启动
- NAV 一致性 / 撮合层等价性：明确**不在本模块范围**（撮合差异由 §3.6.2 关键不变量界定，由 paper_v2_blockers.md / trading_core.md 各自覆盖）
