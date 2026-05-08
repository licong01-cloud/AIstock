# Cross-Test 框架通用模板（v0.3, 2026-05-08）

> **定位**：本文档是 cross-test checklist 的**空模板**，供各模块负责人填写自己模块的具体测试矩阵。
> **不写**任何具体模块（如 QE / Model Registry / config_composer / Strategy Package 等 Codex 维护模块）的实测矩阵 —— 那是各模块负责人的活。
> **作者范围**：本模板由 Claude Code（cross-test 角色，paper-v2-vnpy-mvp 团队）起草，对应授权 A5（《agent_teams_session_handoff_20260508.md》§2）。
> **配套文档**：
> - 主体设计：`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`（§20-§21、§A.4.5、§A.5）
> - 推导背景：`docs/analysis/paper_v2_user_requirement_audit_20260507.md`（§20-§21）
> - 平台基础：`backend/services/validation/`（4059 行，已含 `assigned_agent` / `agent_context` schema）

---

## 1. Cross-Test 是什么

### 1.1 一句话定义

**Cross-test = 跨模块边界的一致性测试**：当模块 A 与模块 B 通过契约（数据结构 / RPC / 文件 / DB schema / 状态机）协作时，验证 **A 看见的边界** 与 **B 看见的边界** 在所有合法输入下行为一致、状态同步、错误能被对方理解。

### 1.2 与单元测试 / 集成测试的区别

| 维度 | 单元测试 (L0/L1) | 集成测试 (L2) | **Cross-test (L2/L3)** |
| --- | --- | --- | --- |
| 测试对象 | 单个函数 / 类 | 单模块多组件协作 | **两个或多个模块的边界一致性** |
| 谁写谁跑 | 开发者本人 | 开发者本人 | **开发者写测试矩阵，cross-tester（不同 agent）执行** |
| 关注点 | 算法 / 业务正确性 | 模块内部装配正确性 | **契约对齐、状态同步、ID/seed 隔离、错误传播** |
| 失败时的修复责任 | 开发者 | 开发者 | **报告 bug 入 GitHub Issues + Validation Center；不动代码** |

### 1.3 Cross-test 的工程价值

参见主体设计 `qe_sota_strategy_package_asset_governance_design_20260508.md` §A.4.5 与 audit §20.1：

- **消除自测偏差**：开发者本人无法看到自己的盲点；cross-tester 用不同思考路径切入
- **职责清晰**：测试只填 bug，不修代码（硬约束，由 hook + prompt + 权限三重保障）
- **单一 bug 真源**：`GitHub Issues + AIstock Validation Center` 是唯一登记入口
- **审计可追溯**：每个 bug 含 `developer_agent / tester_agent / fingerprint / fix_commit`

---

## 2. 通用 Checklist 章节模板（空模板）

> **使用方式**：复制本节，将 `<MODULE_A>` / `<MODULE_B>` 等占位符替换为具体模块名，按节填写。每模块至少写一份位于 `tests/aistock_validation/modules/<module>.md`。
> 单 cross-test 用例最少包含 §2.1 - §2.6；§2.7 / §2.8 视复杂度可选。

---

### 2.1 测试目标 / 测试边界

> **填写指南**：明确两个或多个模块及它们之间的契约形式（数据结构 / API / 文件 / DB / 状态机），声明本次 cross-test 要验证的具体边界一致性。

```yaml
test_id: <唯一 ID，如 xtest_<module_a>_<module_b>_<topic>_<level>>
title: <一句话目标>
modules_under_test:
  primary: <MODULE_A>
  secondary: <MODULE_B>
  # （可选）third: <MODULE_C>
contract_form:                    # 契约形式（多选）
  - <data_structure | rpc | file_artifact | db_schema | state_machine | event_topic>
contract_artifacts:               # 契约的具体载体
  - <例：manifest schema 文件路径 / RPC proto 文件 / DB 表名>
test_level: <L1 | L2 | L3 | L4>   # 参考 tests/aistock_validation/catalog/test_levels.md
developer_agent: <claude-code | codex>
tester_agent:    <claude-code | codex>   # 必须 ≠ developer_agent
goal_statement: |
  本次测试要验证：当 <MODULE_A> 通过 <contract_form> 向 <MODULE_B> 传递 <X> 时，
  在 <条件 Y> 下，<MODULE_B> 观察到的 <字段/状态/时序> 与 <MODULE_A> 写入时严格一致。
```

**反模式（不要写在这里）**：

- ❌ 单模块内部逻辑（属于单元测试）
- ❌ 端到端业务流程（属于 L4 端到端，应单独立 case）
- ❌ "顺便测一下 <MODULE_C>"（每个 cross-test 只聚焦一对/一组明确边界）

---

### 2.2 输入契约校验项

> **填写指南**：列出 `<MODULE_A>` 输出 / `<MODULE_B>` 输入这一侧的所有契约字段，逐项给出"必校验"维度。

| # | 字段 / 接口元素 | 类型 | 必校验维度 | 失败示例（用于构造负 case） |
| --- | --- | --- | --- | --- |
| I-1 | `<field_name>` | `<type>` | 存在性 / 类型 / 取值范围 / 编码 / 长度 | `<示例>` |
| I-2 | `<rpc_method>` | RPC | 参数完整性 / 序列化兼容 / 版本字段 | 缺字段 / 多字段 / 老 schema 调用新版 |
| I-3 | `<artifact_file>` | 文件 | 路径 / 权限 / sha256 / 大小上限 | 路径漂移 / 软链接 / 文件被篡改 |

**通用必校验项（适用所有 cross-test）**：

- [ ] **schema_version 字段**存在且与契约文档一致
- [ ] **空 / 缺省值**处理：缺省时 `<MODULE_B>` 不应静默回落（参考 `feedback_no_silent_errors.md`）
- [ ] **字符编码**：UTF-8 + 中文 / emoji 不破坏序列化
- [ ] **数值精度**：Decimal vs float 边界（参考 `limit_threshold_analysis.md` Decimal bug）
- [ ] **时区**：所有时间戳带 tz 或显式 UTC

---

### 2.3 输出契约校验项

> **填写指南**：列出 `<MODULE_B>` 处理后回传 / 写出的所有产出，逐项给出"必校验"维度。

| # | 输出 | 类型 | 必校验维度 | 备注 |
| --- | --- | --- | --- | --- |
| O-1 | `<return_value>` | `<type>` | 类型 / 取值范围 / 一致性 | |
| O-2 | `<persisted_state>` | DB / 文件 | 写入完整性 / 事务 / 幂等 | |
| O-3 | `<event_emitted>` | 事件 | 触发时机 / payload 字段完整 | |

**通用必校验项**：

- [ ] **回写一致性**：`<MODULE_A>` 后续读到的状态 = `<MODULE_B>` 写入的状态
- [ ] **幂等性**：同样输入跑两次产出 byte-equal（关键场景）
- [ ] **审计字段**：`created_at / created_by_agent / fingerprint` 完整

---

### 2.4 状态 / 时序一致性项

> **填写指南**：cross-test 区别于普通集成测试的核心维度。列出所有跨模块共享的状态与时序约定。

#### 2.4.1 ID Namespace 隔离（dev / prod）

参考交接文档 §3.1：

- [ ] 所有测试数据 ID 含 dev 前缀（如 `pkg_dev_*` / `mfst_dev_*` / `qe_dev_*`）
- [ ] 不在生产 ID 命名空间下创建测试数据
- [ ] dev / prod 切换不通过环境变量软切，而是通过 ID 前缀硬隔离

<details>
<summary><strong>示例片段（取材自 Strategy Engine 设计 §3.2 / §13 / §17；占位形式 <code>&lt;MODULE_A&gt;=Engine</code> / <code>&lt;MODULE_B&gt;=Adapter</code>）</strong></summary>

> **本片段仅作框架示范**。Engine 自身的具体测试矩阵由 engine-design / 各 adapter 负责人填写到 `tests/aistock_validation/modules/strategy_engine_*.md`；本模板不替模块负责人填写实测矩阵。

```yaml
# Engine ↔ Adapter 跨边界 ID / 字段隔离 checklist 示例
state_isolation_checks:

  - name: package_id_namespace_isolation
    description: |
      `<MODULE_B>`（Adapter）传入 `<MODULE_A>`（Engine）的 `StrategySpec.package_id`
      在 dev / prod 之间通过 ID 前缀硬隔离；`<MODULE_A>` 在 init() 阶段必须显式拒绝
      跨命名空间引用。
    method: |
      1. 构造 spec.package_id="pkg_dev_<hash>"（dev 命名空间）
      2. 构造 PortfolioState.portfolio_id="port_prod_<hash>"（prod 命名空间）
      3. 调 `<MODULE_A>`.init(spec, ...) + decide_eod(..., portfolio=...)
      4. 期望抛 NamespaceMismatchError；不允许静默放行
    expected_failure_mode: explicit_typed_exception
    forbidden_behavior:
      - 静默使用 prod portfolio 跑 dev spec
      - 用环境变量软切命名空间

  - name: runtime_overlay_allowlist_source_of_truth
    description: |
      RuntimeOverlay 允许字段集**派生自 Codex `package_runtime_variant` schema**
      （依据 Engine 设计 §17 R-Q6 裁决，Codex 端是 source of truth）；`<MODULE_A>`
      不得在 Engine 端硬编码 allow-list。cross-test 验证：当 Codex schema 添加 / 移除
      字段时，`<MODULE_A>` 的拒收行为同步变化。
      **实施依赖**：本 cross-test 等 Codex Phase 6（runtime variant schema）合入集成
      分支后才能跑全；在此之前模板 case 标 status=blocked_by_codex_phase_6。
    overlay_identity_fields:
      # RuntimeOverlay 必含字段（Engine 设计 §3.2）
      - runtime_variant_id: str
      - runtime_variant_hash: str
    method: |
      1. 取一个 schema 版本 v_n 下的合法字段集 F_n（如 topk / n_drop /
         threshold_overlay / minute_execution_algo / cost_overrides / hmm_toggle /
         sector_blacklist / capital_capacity）+ 必含 runtime_variant_id /
         runtime_variant_hash
      2. 构造 overlay 含 F_n 之外的字段 X（例：factor_set / model_weights /
         preprocessor —— 这三类按 Engine 设计 §3.2 显式禁止）
      3. 调 `<MODULE_A>`.init(..., overlay=overlay)
      4. 期望抛 RuntimeOverlayValidationError；含 rejected_field=X
      5. 同时验证 runtime_variant_hash 与 runtime_variant_id 不一致时也拒收
         （hash 是身份指纹，篡改单字段必须被发现）
    expected_failure_mode: explicit_typed_exception
    forbidden_behavior:
      - setattr 静默忽略未识别字段
      - "看起来像"已知字段就放行（拼写近似不能 fallback）
      - runtime_variant_hash 缺失时用字段集重算"凑一个"（必须以 Codex 端写入版本为准）

  - name: broker_backend_namespace_isolation
    description: |
      ID Namespace 在 broker_backend 维度上的扩展（取材 Engine 设计 §3.6.1 / §3.6.2 /
      §17 R-Q9）。`portfolio.broker.backend_id` 是命名空间硬隔离的一部分：dev 测试
      的 LocalSim portfolio 不可在 prod 的 MiniQMTSim portfolio 路径上跑；反之亦然。
    backend_id_enum:
      # 取自 Engine 设计 §3.6.1 BrokerBackend.backend_id Literal
      - local_sim       # LocalSim：进程内 ledger + TDX 行情，多包并行
      - minqmt_sim      # MiniQMTSim：xtquant `xttrade` 仿真账户 + miniQMT 行情，单包
      - minqmt_live     # 未来实盘（不在本期 cross-test 范围）
    method: |
      1. 构造 spec.package_id="pkg_dev_<hash>"（dev 命名空间）
      2. 构造 PortfolioState.portfolio_id="port_dev_<hash>"，其
         portfolio.broker.backend_id="local_sim"
      3. 构造干扰 portfolio：portfolio_id="port_prod_<hash>" + backend_id="minqmt_sim"
      4. 调 `<MODULE_A>`.init(spec, ..., portfolio=干扰 portfolio)
      5. 期望抛 NamespaceMismatchError（含 backend_id + 命名空间前缀字段）
    expected_failure_mode: explicit_typed_exception
    forbidden_behavior:
      - dev portfolio 配 prod broker（或反之）静默放行
      - 用环境变量软切 backend_id（必须由 portfolio 配置硬绑）
      - 测试中混用 minqmt_live（实盘 backend_id 不应出现在 cross-test fixture）

  - name: asset_path_dev_suffix_enforcement
    description: |
      参考交接文档 §3.1：所有资产路径用 dev 后缀硬隔离（如
      `rdagent_assets/strategy_package_runtime_dev/`），ModelArtifactHandle.weight_uri
      指向 prod 路径但 spec.package_id 为 dev 命名空间时 `<MODULE_A>` 拒收。
    method: |
      1. 构造 ModelArtifactHandle.weight_uri 指向 prod 资产目录（无 _dev 后缀）
      2. spec.package_id 为 dev 命名空间
      3. `<MODULE_A>`.init() 校验 dev/prod 路径与 ID 命名空间一致性
      4. 期望抛 AssetNamespaceMismatchError
    expected_failure_mode: explicit_typed_exception
```

</details>

#### 2.4.2 Seed / 随机性一致性

参考主体设计 §A.5.2 Master Seed Contract + Engine 设计 §17 R-Q5：

- [ ] 同一 `master_seed` 跨两次执行，输出 byte-equal（NAV 差异 < 0.01bp / 持仓 100% 相同）
- [ ] 子 seed 派生规则在两侧模块完全一致
- [ ] 库版本（numpy / torch / lgbm 等）记录在 manifest，跨执行可比
- [ ] 决策路径审计载体（如 Engine `DecisionTrace.inputs_digest`）**强制**写入 seed 摘要，不允许"按需写入"（避免 Mode G 漂移监控漏帧）

<details>
<summary><strong>示例片段（取材自 Strategy Engine 设计 §3.2 / §3.5 / §7 / §11 / §17；占位形式 <code>&lt;MODULE_A&gt;=Engine</code> / <code>&lt;MODULE_B&gt;=Adapter</code>）</strong></summary>

> **本片段仅作框架示范**。Engine Mode G 自测最小集（5 个 case 名见 Engine 设计 §11）由 engine-design 负责人写入对应 modules 文件；本模板仅演示如何把 master_seed 一致性校验拆成 cross-test checklist 项。

```yaml
seed_consistency_checks:

  - name: seed_bundle_contract_propagation
    description: |
      `<MODULE_B>`（Adapter）从 manifest.seed_contract_ref 读出 SeedBundle 后传给
      `<MODULE_A>`（Engine）.init()；`<MODULE_A>` 必须 byte-equal 校验所有子字段。
    fields_under_test:
      - seed_policy ∈ {fixed, multi_seed, random_logged, unset_legacy}
      - master_seed: int | None
      - seed_sequence: dict[str,int]   # numpy / torch / lgb 等子 seed
      - deterministic_algorithms: bool
      - cudnn_deterministic: bool
      - library_versions: dict[str,str]
    method: |
      1. `<MODULE_B>` 构造 SeedBundle(policy="fixed", master_seed=42,
         seed_sequence={"numpy":42,"torch":42,"lgb":42}, ...)
      2. 调 `<MODULE_A>`.init(spec, overlay, seed_bundle, model_handle)
      3. 通过 `<MODULE_A>` 暴露的 audit hook 读回 SeedBundle 摘要
      4. assert 摘要逐字段 byte-equal `<MODULE_B>` 写入版本
    forbidden_behavior:
      - 子 seed 缺失时用 master_seed 派生填补（必须显式抛错）
      - library_versions 漂移时用"最近邻版本"放行

  - name: seed_contract_violation_propagates_typed_error
    description: |
      §2.5 错误传播在 seed 维度的具体化：参考 Engine 设计 §7.2 列举的 4 种
      SeedContractError 触发条件，逐一构造负 case，验证错误类型 + 关键字段进 message。
    negative_cases:
      - id: seed_NEG_01_invalid_policy
        setup: SeedBundle.seed_policy = "lottery_random"   # 非法枚举
        expect: SeedContractError, message 含 "seed_policy" + 实际值
      - id: seed_NEG_02_unset_legacy_with_frozen_core
        setup: |
          spec.frozen_alpha_core 要求 fixed seed
          SeedBundle.seed_policy = "unset_legacy"
        expect: SeedContractError, message 含 "frozen_alpha_core" + "unset_legacy"
      - id: seed_NEG_03_missing_subseed
        setup: |
          model family 为 LGB 但 seed_sequence 缺 "lgb" key
        expect: SeedContractError, message 含 missing key + model family
      - id: seed_NEG_04_library_version_drift
        setup: |
          library_versions["numpy"] != frozen_alpha_core 期望版本
        expect: SeedContractError, message 含两侧版本（**不静默放行**）
    forbidden_behavior:
      - except: pass + 用默认 seed 继续（参考 feedback_no_silent_errors）
      - 用 random.SystemRandom() 填补缺失子 seed

  - name: master_seed_audit_mandatory_in_decision_trace
    description: |
      依据 Engine 设计 §17 R-Q5 裁决：seed_bundle 摘要**强制**写入
      DecisionTrace.inputs_digest（`sha256(spec + scores + portfolio + seed)`）；
      不允许"按需写入"。cross-test 验证：DecisionTrace.inputs_digest 必须随 seed
      变化而变化；若 seed 摘要漏写，digest 不能体现 seed 变化 → fail。
    method: |
      1. 同 (spec, scores, portfolio) 用 master_seed=42 跑一次 → trace_1.inputs_digest
      2. 同 (spec, scores, portfolio) 用 master_seed=43 跑一次 → trace_2.inputs_digest
      3. assert trace_1.inputs_digest != trace_2.inputs_digest（seed 必须进 hash）
      4. 同 master_seed=42 重复跑 → digest 必须 byte-equal（确定性）
    expected: 两次同 seed 跑出 byte-equal trace；不同 seed 跑出不同 digest
    forbidden_behavior:
      - DecisionTrace 仅在 spec.seed_dependent_pipeline=true 时才写 seed（被 R-Q5 否决）

  - name: l4_byte_equal_two_runs_strict
    description: |
      Engine 设计 §7.4 + 主体 §A.5.2 Phase 4 L4 gate 的 cross-test 化表达。
      `<MODULE_A>` 决策路径必须 byte-deterministic given (spec, scores, portfolio,
      seed_bundle)。
    method: |
      1. fix master_seed + 完整 SeedBundle
      2. 跑 `<MODULE_A>`.decide_eod() 两次（同 process / 跨 process 各一次）
      3. dump OrderIntentBatch + DecisionTrace
      4. assert byte-equal
    pass_criteria:
      - OrderIntentBatch.intents 列表 byte-equal（含字段顺序）
      - DecisionTrace.pipeline_steps 每步 input_digest / output_digest 一致
      - NAV 序列每日差异 |Δ| < 0.01bp（如 cross-test 跑到 backtest 层）
    common_pitfalls_to_check:
      - dict 迭代顺序不固定（用 sorted）
      - set 序列化非确定（用 sorted list）
      - float 精度差异（用 Decimal 或固定 ulp 容差）
    forbidden_behavior:
      - 两次结果不一致时把差异归因为"GPU 非确定性"而不修复

  - name: mode_g_cross_adapter_equivalence
    description: |
      Engine 设计 §11 Mode G 在 cross-test 框架内的表达：同 (spec, scores, portfolio,
      seed_bundle)，三个不同的 `<MODULE_B>`（QE / Paper / Live）各自调 `<MODULE_A>`
      必产 byte-equal OrderIntent。
      依据 Engine 设计 §17 R-Q1 裁决：Mode G 为 Engine 合 main 硬 gate（外部决策项
      OPEN-EXT-1：是否推 Codex 主体设计 §6 正式纳入待用户授权；本模板**不替**主体
      设计做修订决定）。
    method: |
      1. 准备共享 fixture：(spec_dev, scores_dev, portfolio_dev, seed_bundle_dev)
      2. 三个 `<MODULE_B>` 各自调 `<MODULE_A>`.decide_eod() 拿 OrderIntentBatch
      3. assert 三批 intents byte-equal（含 reason / parent_intent_id /
         decision_trace_id 之外的所有字段）
    note: |
      具体 case 名（如 engine_modeg_smoke_lgb_single_alpha 等 5 例）由 engine-design
      负责人填写到 tests/aistock_validation/modules/strategy_engine_modeg.md，本模板
      不预填。
    additional_broker_dimension_hints:
      # 取材 Engine 设计 §3.6.6（4 条 broker 维度用例命名模式参考；不引用具体 case 名）
      - hint_1_naming_pattern: <module>_modeg_<backend_a>_vs_<backend_b>_orderintents
        intent: |
          同 (manifest, scores, portfolio, seed)，跨 backend 的 adapter 必须输出
          byte-equal OrderIntent；NAV 不比（撮合层差异由 §3.6.2 关键不变量界定）
      - hint_2_naming_pattern: <module>_modeg_multi_package_<localsim>_isolation
        intent: |
          多包并行场景（仅 backend.bind_capacity().max_concurrent_packages > 1
          的 backend 适用），验证 portfolio_id 切片正确
      - hint_3_naming_pattern: <module>_modeg_<single_binding_backend>_capacity_reject
        intent: |
          单包 backend 上注入第二包 → adapter 抛 BrokerBindCapacityExceededError（D2 强制）
      - hint_4_naming_pattern: <module>_modeg_broker_compat_reject
        intent: |
          spec.broker_compatibility 与 portfolio.broker.backend_id 不交集 →
          init() 抛 BrokerCompatibilityMismatchError（D4 校验）
```

</details>

#### 2.4.3 时序一致性

- [ ] 时间戳精度（s / ms / us）双方约定一致
- [ ] 事件先后顺序：`<MODULE_A>` 写入 → `<MODULE_B>` 读取的可见性约束（事务 / flush / fsync）
- [ ] 跨进程：是否存在 race（如 `<MODULE_B>` 在 `<MODULE_A>` 提交事务前读到中间状态）

#### 2.4.4 资产路径与生命周期

- [ ] 资产路径含 dev 后缀（如 `rdagent_assets/strategy_package_runtime_dev/`）
- [ ] frozen 资产不被任一侧修改（sha256 校验）
- [ ] lifecycle event 在两侧观察一致

#### 2.4.5 行情通道与撮合端强绑定（broker_backend 维度）

参考 Strategy Engine 设计 §3.6.4 / §17 R-Q9 D3：行情通道**不可与撮合端解耦切换**。
跨 backend 配错 → 必须 fail-fast。

- [ ] LocalSim ↔ TDX 实时行情：唯一允许组合
- [ ] MiniQMTSim ↔ miniQMT 行情（xtquant `xtdata`）：唯一允许组合
- [ ] MiniQMTLive ↔ miniQMT 行情：未来实盘，本期不在 cross-test 范围
- [ ] BrokerBackend 实例化时**绑定**对应行情通道；`market_data_channel()` 返回的 channel 不接受 hot-swap
- [ ] 跨配（如 LocalSim 配 miniQMT 行情、MiniQMTSim 配 TDX）→ adapter 必须显式抛错

<details>
<summary><strong>示例片段（取材自 Engine 设计 §3.6.4 / §10.1；占位形式 <code>&lt;MODULE_A&gt;=Engine</code> / <code>&lt;MODULE_B&gt;=Adapter</code>）</strong></summary>

> 本片段仅作框架示范。具体撮合算法与 vnpy_xt 集成测试由 LocalSim / MiniQMTSim adapter 负责人填到对应 modules 文件；本模板不预填。

```yaml
market_channel_strong_binding_checks:

  - name: localsim_must_pair_tdx
    description: |
      LocalSim adapter 实例化时 market_data_channel() 必须返回 TDX 通道；
      若注入 miniQMT 行情通道 → adapter 显式抛错（不静默 fallback）。
    method: |
      1. 实例化 LocalSimBroker
      2. 强行替换 market_data_channel 指向 miniQMT xtdata
      3. 调 `<MODULE_B>`.bootstrap_portfolio(broker=...)
      4. 期望抛 broker 实例化级别的 typed error；不允许 hot-swap
    forbidden_behavior:
      - 静默接受任意行情源
      - 用环境变量切行情通道（行情绑定必须在 broker 实例化时硬绑）

  - name: minqmtsim_must_pair_minqmt_quote
    description: |
      MiniQMTSim adapter 实例化时 market_data_channel() 必须返回 xtquant xtdata；
      若注入 TDX → adapter 显式抛错。
    method: |
      1. 实例化 MiniQMTSimBroker
      2. 强行替换 market_data_channel 指向 TDX
      3. 调 `<MODULE_B>`.bootstrap_portfolio(broker=...)
      4. 期望抛 broker 实例化级别的 typed error
    rationale_reference: |
      Engine 设计 §3.6.4 三条理由：(1) 价格幻觉破坏 Mode G 等价性物理基础
      (2) 实盘切换 MiniQMTSim → MiniQMTLive 仅切 trading 层不动行情 (3) 故障归因清晰
    forbidden_behavior:
      - 用 TDX 数据驱动 miniQMT 撮合（产生信号-撮合双源价格）

  - name: market_channel_hot_swap_reject
    description: |
      运行期 hot-swap 行情通道一律拒绝，无论是否同源；行情通道必须在 broker
      实例化时绑定，session 期间不可变。
    method: |
      1. 已绑定 broker（如 LocalSim + TDX）启动一个 EngineSession
      2. 尝试在运行期把 broker.market_data_channel 替换为另一 TDX 实例
      3. 期望抛 typed error；不允许"同源"作为放行理由
    forbidden_behavior:
      - 因"同是 TDX"就允许换实例
      - 把 hot-swap 包装成"reconnect" 路径绕过校验
```

</details>

#### 2.4.6 多策略包并行兼容性（BrokerBindCapacity）

参考 Strategy Engine 设计 §3.6.3 / §10.1 / §17 R-Q9 D2：不同 backend 的并发能力不同，
`BrokerBindCapacity.max_concurrent_packages` 是硬限制。

- [ ] adapter 在 portfolio bootstrap 时调 `broker.bind_capacity()` 取容量
- [ ] LocalSim：`max_concurrent_packages >= 1`，可同时绑定 N 个 StrategyPackage（每包独立 portfolio_id + ledger 切片）
- [ ] MiniQMTSim：`max_concurrent_packages == 1`（单进程内 miniQMT 账户 = 一条 trading session）
- [ ] 已绑定 MiniQMTSim 的 portfolio 上注入第二包 → 抛 `BrokerBindCapacityExceededError`（不静默替换）
- [ ] 不允许把 LocalSim 多包共享的 ledger 切片混入 OrderIntent.portfolio_id（隔离要求）
- [ ] 不允许用 LocalSim 的 PortfolioState 喂 MiniQMTSim 的 Engine（必须用 `broker.query_positions()` 实时拉取）

<details>
<summary><strong>示例片段（取材自 Engine 设计 §3.6.3 / §10.1；占位形式 <code>&lt;MODULE_A&gt;=Engine</code> / <code>&lt;MODULE_B&gt;=Adapter</code>）</strong></summary>

```yaml
broker_bind_capacity_checks:

  - name: minqmt_sim_singleton_capacity
    description: |
      MiniQMTSim 是单包 backend；已存在绑定时再注入新 package 必须抛
      BrokerBindCapacityExceededError（取自 Engine 设计 §10.1）。
    method: |
      1. 实例化 MiniQMTSimBroker；assert broker.bind_capacity().max_concurrent_packages == 1
      2. `<MODULE_B>`.bind_package(broker, package_a) → 成功
      3. `<MODULE_B>`.bind_package(broker, package_b) → 期望抛
         BrokerBindCapacityExceededError，message 含 broker.backend_id + package_a.id
      4. 验证：原绑定 package_a 仍然有效（错误不应导致 session 中断或替换）
    forbidden_behavior:
      - 静默替换原 binding（"覆盖式"绑定）
      - 把第二包丢进 queue 等 package_a 完成（无依据的隐式排队）

  - name: localsim_multi_package_isolation
    description: |
      LocalSim 多包并行时，每包的 OrderIntent.portfolio_id 必须独立切片；
      包 A 的 ledger 不可被包 B 看见。
    method: |
      1. 同一 LocalSimBroker 实例绑定 package_a + package_b（不同 portfolio_id）
      2. 触发包 A 的 decide_eod() 产 OrderIntent_a
      3. 触发包 B 的 decide_eod() 产 OrderIntent_b
      4. assert OrderIntent_a.portfolio_id != OrderIntent_b.portfolio_id
      5. assert package_a 的 PortfolioState.positions 与 package_b 的不重叠
    forbidden_behavior:
      - 共享 ledger 视图（包 A 决策时看到包 B 持仓）
      - 包 A 的成交事件回写到包 B 的 portfolio

  - name: portfolio_state_source_per_backend
    description: |
      不同 backend 的 PortfolioState 来源不同（Engine 设计 §3.6.2 表）：
        - LocalSim → adapter 内存 ledger 镜像
        - MiniQMTSim → broker.query_positions() 实时拉取（xtquant query_stock_positions）
      cross-test 验证：源不能跨 backend 互换。
    method: |
      1. MiniQMTSim 的 EngineSession 在 decide_eod() 前，adapter 必须调
         broker.query_positions()；不允许复用 LocalSim 风格的内存 ledger
      2. 注入 stub：返回 LocalSim 风格 PortfolioState（含 cash + positions dict）
      3. assert 调用路径上有显式 broker.query_positions() 标记；缺失即 fail
    forbidden_behavior:
      - 把 LocalSim 的 PortfolioState 喂 MiniQMTSim 的 Engine
      - 把 query_positions() 结果缓存超过当前 trade_date 边界
```

</details>

#### 2.4.7 broker_compatibility 字段相容性校验

参考 Strategy Engine 设计 §3.6.5 / §10.1 / §17 R-Q9 D4 / OPEN-EXT-3：
`StrategyPackage v2 manifest.broker_compatibility` 是 backend_id 白名单；
`portfolio.broker.backend_id` 必须在白名单内，否则 `<MODULE_A>`.init() 抛
`BrokerCompatibilityMismatchError`。

**实施依赖**：本字段需走 Codex 主体附录 A.4.4 双 PR 模式（Codex 端 schema additive +
Engine 端 reader + 切默认产出 + 全包重 freeze）。在 Codex schema 落地前，cross-test
需用 `StrategySpec.custom_extension.broker_compatibility` 占位（与 R-Q2 audit-only
语义一致）；模板 case 标 status=blocked_by_open_ext_3。

- [ ] 字段语义：`broker_compatibility: list[str]`，元素 ∈ `{local_sim, minqmt_sim, minqmt_live}`
- [ ] 空数组 → 包不可投产（adapter init 阶段抛错）
- [ ] LEGACY 包迁移默认 `["local_sim"]`（保持现状），不自动获得 minqmt_sim 兼容性
- [ ] `portfolio.broker.backend_id in spec.broker_compatibility`：违反则抛 `BrokerCompatibilityMismatchError`
- [ ] DecisionTrace.inputs_digest 折入 `spec.broker_compatibility`（影响等价性比对）
- [ ] 不允许 runtime overlay 修改 `broker_compatibility`（与 frozen alpha core 等同保护）
- [ ] `minqmt_live` 标记需主体晋级流程额外 gate（不在 Engine 设计 / 本模板范围）

<details>
<summary><strong>示例片段（取材自 Engine 设计 §3.6.5 / §10.1 / §17 OPEN-EXT-3；占位形式 <code>&lt;MODULE_A&gt;=Engine</code> / <code>&lt;MODULE_B&gt;=Adapter</code>）</strong></summary>

> **schema 来源约束**：本片段中 `broker_compatibility` 字段在 Codex 主体设计 §5.4 加入前由 `custom_extension.broker_compatibility` 占位。本模板**不替**主体设计做 schema 修订决定（依据 OPEN-EXT-3，待用户单独授权双 PR）。

```yaml
broker_compatibility_field_checks:
  status: blocked_by_open_ext_3
  schema_placeholder_path: spec.custom_extension.broker_compatibility   # Codex schema 落地前占位

  - name: broker_compat_intersection_check
    description: |
      portfolio.broker.backend_id 必须 ∈ spec.broker_compatibility；
      不交集时 `<MODULE_A>`.init() 抛 BrokerCompatibilityMismatchError（取自 §10.1）。
    method: |
      1. 构造 spec.broker_compatibility=["local_sim"]
      2. 构造 portfolio.broker.backend_id="minqmt_sim"
      3. 调 `<MODULE_A>`.init(spec, ..., portfolio=...)
      4. 期望抛 BrokerCompatibilityMismatchError，message 含
         portfolio.broker.backend_id + spec.broker_compatibility 实际取值
    forbidden_behavior:
      - 把不交集 backend 默认放行（"也许能跑"逻辑）
      - 用 backend_id 子串匹配代替严格 equality（minqmt_sim ≠ minqmt_live）

  - name: empty_broker_compat_reject
    description: |
      broker_compatibility=[] → 包不可投产；adapter init 阶段抛错。
    method: |
      1. 构造 spec.broker_compatibility=[]
      2. 调 `<MODULE_A>`.init(spec, ...)
      3. 期望抛 typed error；message 指向"包未声明 broker 兼容性"
    forbidden_behavior:
      - 空数组 fallback 到 ["local_sim"]（必须显式声明，不自动迁移）

  - name: legacy_default_local_sim_only
    description: |
      LEGACY_NON_ST_PIT 老包迁移时，broker_compatibility 默认填 ["local_sim"]；
      cross-test 验证默认值不会自动扩到 minqmt_sim。
    method: |
      1. 模拟 LEGACY 包（无 broker_compatibility 显式声明）
      2. 经 Engine reader 读取 → 默认 ["local_sim"]
      3. 配 portfolio.broker.backend_id="minqmt_sim" → init() 抛
         BrokerCompatibilityMismatchError
    forbidden_behavior:
      - LEGACY 默认包含 minqmt_sim（仅 freeze 后显式 promote 才允许）
      - reader 把缺失字段视为"全 backend 兼容"

  - name: broker_compat_in_inputs_digest
    description: |
      DecisionTrace.inputs_digest 必须折入 spec.broker_compatibility；
      改 broker_compatibility 必然改变 digest（Mode G 等价性比对粒度）。
    method: |
      1. 同 (scores, portfolio, seed) 用 broker_compatibility=["local_sim"] 跑 → digest_1
      2. 同 (scores, portfolio, seed) 用 broker_compatibility=["local_sim","minqmt_sim"]
         跑 → digest_2
      3. assert digest_1 != digest_2
    forbidden_behavior:
      - 把 broker_compatibility 排除在 inputs_digest 之外（漂移监控漏帧）

  - name: broker_compat_overlay_reject
    description: |
      runtime overlay 不得修改 broker_compatibility（与 frozen alpha core 等同保护）。
    method: |
      1. 构造 RuntimeOverlay 含 broker_compatibility 字段（越权）
      2. 调 `<MODULE_A>`.init(..., overlay=overlay)
      3. 期望抛 RuntimeOverlayValidationError，rejected_field=broker_compatibility
    forbidden_behavior:
      - overlay 提升 broker 兼容性（必须经主体晋级流程，不允许 runtime 旁路）
```

</details>

---

### 2.5 错误传播校验项

> **填写指南**：参考 `feedback_no_silent_errors.md`（禁止 `except: pass` / fallback 默认值 / 空 catch）。错误必须从 `<MODULE_B>` 显式传到 `<MODULE_A>`，且消息可定位到根因。

#### 2.5.1 必校验错误路径

| 错误类型 | 触发方式 | 期望行为（不可妥协） |
| --- | --- | --- |
| **契约违反** | 输入字段缺失 / 类型错误 | `<MODULE_B>` 抛 typed exception，含字段名；不静默忽略 |
| **资源不存在** | 引用文件 / DB 行不存在 | 显式 `NotFoundError`，含资源 ID；不返回空对象 |
| **业务规则违反** | 输入数值超界（如 hold_thresh 越权） | 显式 `ValidationError`，含规则名 + 实际值 |
| **下游故障** | 模拟 `<MODULE_C>`（依赖）挂掉 | 错误必须冒泡，不可降级到默认值 |
| **超时** | 注入延迟 | 显式 `TimeoutError`，不静默成功 |

#### 2.5.2 反模式（用作负 case）

- [ ] `except: pass` —— 任一侧出现就 fail
- [ ] fallback 到默认值且不日志 —— 任一侧出现就 fail
- [ ] 错误吞掉返回 `None / {} / []` 让上游误以为成功 —— 任一侧出现就 fail
- [ ] 配置缺失时静默使用空字符串 / 空密码（参考 `feedback_no_empty_db_password.md`）

#### 2.5.3 错误信息可读性

- [ ] 错误消息含 `module_name + operation + relevant_ids`
- [ ] 错误堆栈跨进程能拼回（不被 RPC 层截断）
- [ ] cross-tester 拿到错误 5 分钟内能定位到 suspected_files

---

### 2.6 回归触发条件

> **填写指南**：列出哪些类型的 commit / 文件变更必须触发本 cross-test。配合 `backend/services/validation/cross_test_router.py`（待补，参见 audit §21.5）的自动路由逻辑。

```yaml
regression_triggers:
  file_paths:                              # 文件级触发
    - path: <MODULE_A_path>/**
      reason: <原因>
    - path: <MODULE_B_path>/**
      reason: <原因>
    - path: <contract_artifact_path>
      reason: 契约定义变更必须 cross-test
  branch_prefixes:                         # 分支前缀触发
    - prefix: claude/*                     # claude 改动 → codex cross-test
      tester: codex
    - prefix: codex/*                      # codex 改动 → claude-code cross-test
      tester: claude-code
  schema_changes:
    - <DB 表 / proto / yaml schema>
  manual_trigger_cases:                    # 必须手动触发的场景
    - <如：跨集成分支 merge 前>
    - <如：依赖库主版本升级>
```

---

### 2.7 性能 / 容量基线（可选）

> **填写指南**：仅当 `<MODULE_A>` ↔ `<MODULE_B>` 的契约对延迟 / 吞吐 / 内存敏感时填写。否则跳过。

| 指标 | 基线值 | 上界（fail 阈值） | 测量方式 |
| --- | --- | --- | --- |
| `<MODULE_B>` 处理 1 个契约消息延迟 P50 | `<value>` | `<2x baseline>` | |
| 内存峰值 | `<value>` | `<1.5x baseline>` | OOM 历史参考 `oom_fixes.md` |
| 吞吐 | `<value>` | `<0.5x baseline>` | |

---

### 2.8 Test Plan 与 Validation Center 衔接

> **填写指南**：本 cross-test 用例必须落到 `tests/aistock_validation/modules/<module>.md` 测试矩阵，并通过 Validation Center API 注册。参见 audit §21.1。

```yaml
validation_center_registration:
  module_path: tests/aistock_validation/modules/<module>.md
  plan_id: <plan id in test_plans.yaml>
  agent_context:                           # 参考 finding_store agent_context schema
    schema: aistock_validation_agent_context_v1
    developer_agent: <claude-code | codex>
    tester_agent:    <claude-code | codex>
    reproduce_command: <CLI 一行可复现>
    suspected_files:  []                   # tester fail 时填充
    safety_constraints:
      - 禁止重启生产 8001
      - 禁止改 main 上代码
      - tester 仅能 Edit/Write 到 tests/ 目录
    required_verification_commands:
      - rtk python -m pytest <path> -v
```

---

## 3. 填写指南 + 示例片段

> **以下示例均使用占位符 `<MODULE_A>` `<MODULE_B>`，不指向任何具体 Codex 模块**。

### 3.1 示例：契约校验项填写片段

```yaml
test_id: xtest_<module_a>_<module_b>_manifest_l2
title: <MODULE_A> 写出的 manifest 在 <MODULE_B> 读取时字段完整且类型一致
modules_under_test:
  primary:   <MODULE_A>
  secondary: <MODULE_B>
contract_form: [file_artifact, data_structure]
contract_artifacts:
  - <path/to/manifest_schema.yaml>
test_level: L2
developer_agent: <claude-code or codex>
tester_agent:    <the other one>
```

### 3.2 示例：错误传播负 case 片段

```python
# 占位符示例 —— 不是真实代码
def test_<module_b>_propagates_<module_a>_validation_error():
    """
    Cross-test:
      <MODULE_A> 写入 manifest 时故意省略 schema_version 字段，
      <MODULE_B> 读取时必须抛 ValidationError，不静默 fallback。
    """
    bad_manifest = build_manifest_without_schema_version()
    write_to_<module_a>_outbox(bad_manifest)

    with pytest.raises(ValidationError) as ei:
        <MODULE_B>.consume_latest()

    assert "schema_version" in str(ei.value)
    assert "<MODULE_A>" in str(ei.value) or ei.value.source_module == "<MODULE_A>"
```

### 3.3 示例：状态/时序一致性片段

```yaml
state_consistency_checks:
  - name: master_seed_propagation
    description: <MODULE_A> 在 manifest 写入的 master_seed 必须在 <MODULE_B> 读取时位 byte-equal
    method: |
      1. <MODULE_A> 写 master_seed=42
      2. <MODULE_B> 读 manifest，取 master_seed
      3. assert <MODULE_B>.master_seed == 42
      4. <MODULE_B> 完整执行后，dump 内部 RNG state
      5. 重跑步骤 1-4，dump 应 byte-equal
  - name: id_namespace_isolation
    description: 测试期间所有 ID 含 dev 前缀，绝不使用生产前缀
    method: |
      grep -rE "(pkg|mfst|qe)_[a-f0-9]{8,}" --include="*.json" tests/
      ↑ 任一命中 = fail（生产 ID 不应在测试输出中出现）
```

### 3.4 示例：填写时常见错误

| 反模式 | 为什么错 | 正确做法 |
| --- | --- | --- |
| 测试目标写"测试 <MODULE_A>" | 单模块测试不是 cross-test | 必须包含 ≥2 个模块 + 契约形式 |
| 一个 cross-test 覆盖 5+ 模块 | 失败时定位困难 | 拆成多个 cross-test，每个聚焦一对边界 |
| 输入校验只列字段名 | 失败时不知道怎么构造负 case | 必须给"失败示例"列 |
| 错误传播只验"会抛异常" | 拿不到根因，cross-tester 无法填 bug | 必须验异常类型 + 关键字段在 message 里 |
| 不声明 `developer_agent` / `tester_agent` | 路由失败 / 责任不清 | 两个字段必填，且不能相同 |

---

## 4. 与 Validation Center / GitHub Issue 流程的衔接

参考主体设计 §20-§21（即 `paper_v2_user_requirement_audit_20260507.md`）以及 §A.4.5：

### 4.1 已就位的能力（直接复用）

| 能力 | 位置 | 用法 |
| --- | --- | --- |
| `assigned_agent` 字段 + `agent_context` schema | `backend/services/validation/finding_store.py` | cross-tester fail 时通过此字段挂入 bug |
| `aistock_validation_agent_context_v1` schema | 同上 | 填 `reproduce_command / suspected_files / safety_constraints / required_verification_commands` |
| 28 个 Validation Center API 端点 | `backend/routers/validation.py`（513 行） | 读 plans / runs / findings / bugs |
| 模块归属 | `tests/aistock_validation/catalog/file_ownership.yaml` | 决定 cross-tester 归属 |
| L0-L5 等级定义 | `tests/aistock_validation/catalog/test_levels.md` | cross-test 标 L 等级 |
| Git 活动追踪 | `backend/services/validation/git_activity_provider.py` | 按分支前缀路由 |

### 4.2 仍需补的衔接点（不在本模板范围内）

按 audit §21.2.1，以下能力当前**未就位或部分就位**——本模板不依赖这些自动化能力，**MVP 阶段用人工 cross-test**（参考 audit §21.5.1）：

- Cross-test 自动路由（`cross_test_router.py` 待新建）
- Tester-mode hook（PreToolUse 拦截非 test 文件 Edit/Write）
- Bug 状态机（`NEW → TRIAGED → ASSIGNED → FIXING → FIXED → VERIFIED → CLOSED → REOPENED`）
- Re-test 自动触发
- Validation MCP server（参考 mempalace MCP）

模板填写者应**假设这些能力为人工执行**；当平台增强落地后，本模板的 §2.6 / §2.8 字段直接被自动路由消费，无需重写。

### 4.3 Cross-test 失败时的工作流（MVP 期人工版）

```
1. cross-tester 跑 test plan，发现 fail
   ↓
2. cross-tester 不修代码，改为：
   a. 在 GitHub 创建 Issue，标 label: cross-test-fail
   b. body 含 §2.8 yaml 中的 agent_context（可机读）
   c. 通过 Validation Center API POST /findings（含 assigned_agent = developer_agent）
   ↓
3. 通知 developer_agent（人工 / SendMessage / Issue assignee）
   ↓
4. developer_agent 修复，push
   ↓
5. 人工 re-trigger cross-tester 跑同 test_id
   ↓
6. 通过：Issue 标 verified；Validation Center finding 标 CLOSED
   失败：Issue 标 reopened；回到步骤 4
```

### 4.4 与 §A.5 每 Phase 必备测试矩阵的关系

- 主体设计 §A.5 规定每个 Codex Phase 的 PR 必须含**实施代码 + 测试矩阵 + ≥3 个 case**
- 本模板提供**写测试矩阵的格式骨架**，不预填任何 Codex 模块的具体内容
- Codex 在写各 Phase 测试矩阵时（位于 `tests/aistock_validation/modules/<module>.md`），**应使用本模板的 §2.1 - §2.8 节结构**
- Cross-tester（Claude Code）执行时，按本模板的 §4.3 工作流报 bug

---

## 5. 与现有标准的关系

| 标准文档 | 关系 |
| --- | --- |
| `docs/standards/aistock_development_standard_v1.1_20260504.md` | 上位标准；本模板是其在 cross-test 维度的扩展 |
| `tests/aistock_validation/catalog/test_levels.md` | L0-L5 定义；本模板 §2.1 / §2.8 直接引用 |
| `feedback_aistock_codex_alignment.md` | Claude/Codex 协调 13 条；本模板 §1.3 / §4.3 落实其中"测试只填 bug"条款 |
| `feedback_no_silent_errors.md` | 错误必须传播；本模板 §2.5 落实 |
| `feedback_no_empty_db_password.md` | 配置必须显式；本模板 §2.5.2 引用 |
| `docs/codex_project_memory.md` 762-764 | GitHub Issues + Validation DB 单源；本模板 §4.1 / §4.3 落实 |

---

## 6. 版本与维护

- **v0.1 (2026-05-08)**：初版骨架（Claude Code, paper-v2-vnpy-mvp 团队 cross-test 角色）
- **v0.2 (2026-05-08)**：§2.4.1 / §2.4.2 占位示例替换为取材自 Strategy Engine 设计的具体片段（折叠块）：
  - §2.4.1 加 `package_id_namespace_isolation` / `runtime_overlay_allowlist_source_of_truth` / `asset_path_dev_suffix_enforcement` 三例
  - §2.4.2 加 `seed_bundle_contract_propagation` / `seed_contract_violation_propagates_typed_error` / `master_seed_audit_mandatory_in_decision_trace` / `l4_byte_equal_two_runs_strict` / `mode_g_cross_adapter_equivalence` 五例
  - 引用源：`docs/architecture/strategy_engine_design_20260508.md` §3.2 / §3.5 / §7 / §11 / §17 (R-Q1 / R-Q5 / R-Q6)
  - 占位形式仍保持 `<MODULE_A>=Engine` / `<MODULE_B>=Adapter`；未为 Engine 写实测矩阵
  - 未引用 R-Q4 / OPEN-EXT-2 公告事件信号项（待用户后续单独授权）
- **v0.2 微调 (2026-05-08, 同日)**：对照 lead 取材索引补两处：
  - §2.4.1 `runtime_overlay_allowlist_source_of_truth` 显式列 `runtime_variant_id` + `runtime_variant_hash` 身份字段（取自 Engine §3.2）
  - 加"实施依赖 Codex Phase 6"标记 + status=blocked_by_codex_phase_6 占位
  - 补 hash 完整性校验步骤（method 步骤 5）+ "hash 缺失时不允许重算凑配"forbidden_behavior
- **v0.3 (2026-05-08)**：取材 Engine 设计 §3.6 / §10.1 / §17 R-Q9 / OPEN-EXT-3，引入 `broker_backend` 维度：
  - §2.4.1 ID Namespace 加 `broker_backend_namespace_isolation` case；列出 `backend_id` Literal `{local_sim, minqmt_sim, minqmt_live}`（注：与 Engine 设计 §3.6.1 一致，**注意 `minqmt_*` 拼写不带 i**）
  - §2.4.2 Mode G 折叠块尾追加 `additional_broker_dimension_hints`（4 条命名模式提示，取材 Engine §3.6.6；不引用具体 case 名）
  - 新增 §2.4.5「行情通道与撮合端强绑定」+ 折叠示例（3 case：`localsim_must_pair_tdx` / `minqmtsim_must_pair_minqmt_quote` / `market_channel_hot_swap_reject`）
  - 新增 §2.4.6「多策略包并行兼容性（BrokerBindCapacity）」+ 折叠示例（3 case：`minqmt_sim_singleton_capacity` / `localsim_multi_package_isolation` / `portfolio_state_source_per_backend`）
  - 新增 §2.4.7「broker_compatibility 字段相容性校验」+ 折叠示例（5 case：`broker_compat_intersection_check` / `empty_broker_compat_reject` / `legacy_default_local_sim_only` / `broker_compat_in_inputs_digest` / `broker_compat_overlay_reject`）；标 status=blocked_by_open_ext_3 + schema_placeholder_path 占位
  - 引用错误类型：`BrokerCompatibilityMismatchError` / `BrokerBindCapacityExceededError`（来自 Engine §10.1，**以 Engine 文档为准**）
  - 占位形式仍 `<MODULE_A>=Engine` / `<MODULE_B>=Adapter`；未为任何具体 LocalSim / MiniQMTSim adapter 写实测矩阵
  - 未替 Codex 主体设计做 schema 修订决定（OPEN-EXT-3 待用户授权双 PR）
- **下次更新触发**：
  - **v0.4 待办（已识别，不返工）**：engine-design 第二条简报对 §10.1 增补 `MiniQMTSingletonViolation`（D2）/ `BrokerMarketSourceMismatchError`（D3）/ adapter 端 `BrokerSubmitError` / `BrokerRejectedError` / `BrokerConnectivityError`；v0.3 起草时这些尚未落入 Engine 文档。下次小更新升 v0.4：
    - §2.4.5 行情通道强绑定示例 → 引用 `BrokerMarketSourceMismatchError`（替换"broker 实例化级别 typed error"占位措辞）
    - §2.4.6 `minqmt_sim_singleton_capacity` → 视 Engine §10.1 最终归属，决定改用 `MiniQMTSingletonViolation` 还是保持 `BrokerBindCapacityExceededError`（前者更具体，后者更通用；以 Engine 文档为准）
    - §2.5.1 错误传播表加一行 broker 维度（adapter 端 3 个错误的负 case 模式：submit / rejected / connectivity）
  - OPEN-EXT-1（推 Codex 主体设计 §6 正式纳入 Mode G）授权后，把 §2.4.2 `mode_g_cross_adapter_equivalence` 示例的"单方面声明"措辞改为"主体 §6 正式定义"
  - OPEN-EXT-2（on_event 与 announcement_event_risk_signal 对齐）授权后，新增 §2.4.x 事件触发一致性示例
  - OPEN-EXT-3（`broker_compatibility` 字段走 Codex 主体附录 A.4.4 双 PR）授权 + Codex 端 schema additive 合入后，把 §2.4.7 的 `schema_placeholder_path: spec.custom_extension.broker_compatibility` 切到一等公民字段路径，并去掉 status=blocked_by_open_ext_3 标记
  - audit §21.5.2 平台增强落地（双 agent 字段 / cross-test 路由）后，更新 §4.1 / §4.2
  - 主体设计 §A.5.1 任一 Phase 测试矩阵填出后，把模板里反复出现的真实模式回流到 §3
  - cross-test 实战中发现遗漏维度时，扩 §2

**本模板不维护任何具体模块的测试矩阵；具体矩阵由各模块负责人在 `tests/aistock_validation/modules/<module>.md` 编写**。
