# AIstock 测试计划与生产隔离验证资源策略详细设计方案

日期：2026-05-19
状态：设计草案
分支：`codex/validation-platform-p0p1-design-20260519`
工作区：`F:\Dev\AIstock_worktrees\validation-platform-p0p1-design-20260519`
范围：补齐流水线测试计划目录、生产相邻验证资源策略、近期功能测试计划接入方案；不实现运行时代码，不触碰生产服务，不写生产 DB。

## 1. 执行结论

AIstock 的流水线不能只按“是否写数据库”粗略区分安全与危险。模拟盘、QE 实验、数据同步、Qlib 候选数据、Research Pipeline 等功能要证明真实可用，必须允许创建少量可清理的验证资源；但这些资源必须和生产资源隔离，并且被流水线目录显式声明、审计、清理。

本方案定义三类交付物：

1. **测试计划目录扩展**：在 `tests/aistock_validation/catalog/test_plans.yaml` 中新增 `resource_policy`、`runtime_policy`、`cleanup_policy`、`promotion_policy` 等字段。
2. **生产隔离验证资源策略**：新增 `tests/aistock_validation/catalog/resource_policies.yaml`，统一定义 dev DB、shadow schema、validation account、validation experiment、candidate path 等资源边界。
3. **近期功能测试计划接入清单**：为 Research Pipeline、Data Sync Autonomy、QE MCP、MiniQMT、Qlib Candidate、Paper v2 Live、QE validation experiment 建立明确的测试计划草案和验收标准。

建议先完成目录 schema、resource policy、catalog integrity 校验，再逐个实现具体测试计划。这样后续开发不会反复讨论“这个测试能不能创建实验、能不能写少量数据、清理怎么做”。

## 2. 设计目标

- 让测试计划本身声明是否创建验证资源、是否写 dev DB、是否写 shadow schema、是否需要 cleanup、是否允许 nightly 执行。
- 让生产相邻功能可以通过小样本真实闭环验证，而不是只能 mock。
- 让任何测试资源都有 `validation_run_id`、TTL、cleanup command、evidence manifest。
- 让 catalog integrity 自动阻断未声明资源策略的危险测试计划。
- 让近期新增功能进入统一 Validation Center catalog，而不是只存在 nox session 或单独脚本中。

## 3. 适用功能范围

| 功能类型 | 示例 | 是否允许创建验证资源 | 默认执行层级 |
|---|---|---|---|
| 纯后端契约 | API schema、service contract | 不需要 | L2 |
| UI mock | 前端页面、卡片、详情页 | 不需要 | L2/L3 |
| Dev port 只读 | Validation Center live readonly、Research Pipeline readonly | 不写业务数据 | L3 |
| 模拟盘隔离验证 | Paper v2、MiniQMT sim、strategy ledger | 允许 validation account/portfolio | L3/L4 |
| QE 小样本实验 | validation experiment、1 loop、small universe | 允许 validation experiment/workspace | L4 |
| 数据同步小样本 | dry-run、shadow schema、small symbol/date | 允许 shadow write | L3/L4 |
| Qlib 候选数据 | candidate path、mini provider smoke | 允许 candidate 文件路径 | L4/L5 |
| DR 验证 | dump validity、schema diff、retention | 只读生产 DB + 备份目录 | L4 |

## 4. 测试计划目录 schema 扩展

### 4.1 当前字段保留

现有字段继续保留：

- `plan_key`
- `title`
- `module`
- `level`
- `command_key`
- `nox_session`
- `enabled`
- `requires_backend`
- `requires_frontend`
- `allowed_backend_ports`
- `allowed_frontend_ports`
- `writes_database`
- `writes_artifacts`
- `writes_business_state`
- `runner_enabled`
- `mock_api_used`
- `max_duration_seconds`
- `evidence_kinds`

### 4.2 新增 resource_policy

建议在每个 L3/L4/L5 或生产相邻计划中增加：

```yaml
resource_policy:
  resource_mode: none | readonly | isolated_write | candidate_write | prod_readonly | prod_approved_write
  business_state_write: none | isolated | prod_readonly | prod_approved
  allowed_db_targets: []
  forbidden_db_targets: [prod_db]
  creates_validation_resources: false
  resource_types: []
  validation_namespace_required: true
  validation_run_id_required: true
  cleanup_required: false
  cleanup_command: null
  ttl_hours: 72
  max_sample_symbols: null
  max_date_window_days: null
  max_artifact_mb: null
  production_promotion_required: false
  manual_approval_required: false
```

字段说明：

| 字段 | 含义 | 校验要求 |
|---|---|---|
| `resource_mode` | 资源访问模式 | 必须是枚举值 |
| `business_state_write` | 是否写业务状态 | 非 `none` 必须有 cleanup 或批准 |
| `allowed_db_targets` | 允许 DB 目标 | 只能是 `dev_db`、`shadow_schema`、`temp_db`、`prod_readonly` 等 |
| `forbidden_db_targets` | 禁止目标 | 默认必须包含 `prod_db` |
| `creates_validation_resources` | 是否创建资源 | true 时必须声明 resource_types |
| `resource_types` | 资源类型 | 如 `paper_portfolio`、`qe_experiment`、`qlib_candidate_path` |
| `validation_namespace_required` | 是否必须 validation 前缀 | 生产相邻写入必须 true |
| `cleanup_required` | 是否必须清理 | 创建临时资源时必须 true，除非只保留 evidence |
| `cleanup_command` | 清理命令 | cleanup_required=true 时必须非空 |
| `ttl_hours` | 资源最长保留时间 | L2/L3 默认不超过 72 小时 |
| `production_promotion_required` | 是否属于生产启用前验证 | Qlib 替换、真实交易相关计划必须 true |
| `manual_approval_required` | 是否需要人工确认 | 触碰 prod_readonly 以外生产资源必须 true |

### 4.3 新增 runtime_policy

```yaml
runtime_policy:
  default_trigger: pr | manual | nightly | release_gate
  allow_mcp_start: false
  allow_github_actions: true
  allow_local_manual: true
  allow_parallel: false
  requires_self_hosted_runner: false
  requires_market_hours: false
  requires_tdx: false
  requires_wsl: false
  requires_gpu: false
  timeout_seconds: 1800
```

设计目的：

- 防止长耗时任务误进 PR CI。
- 防止 MCP UI 直接启动需要人工确认的任务。
- 明确哪些任务只能 nightly 或 release gate 执行。

### 4.4 新增 cleanup_policy

```yaml
cleanup_policy:
  strategy: none | immediate | ttl | manual_review
  cleanup_command: null
  verify_cleanup_command: null
  retain_evidence: true
  retain_summary_days: 90
  cleanup_failure_severity: P1
```

### 4.5 新增 evidence_policy

```yaml
evidence_policy:
  required_files:
    - tmp/validation/<plan_key>/summary.json
  required_fields:
    - validation_run_id
    - resource_manifest
    - cleanup_status
  artifact_retention_days: 30
  max_log_excerpt_chars: 4000
```

## 5. 资源策略目录设计

新增文件：`tests/aistock_validation/catalog/resource_policies.yaml`

### 5.1 文件结构

```yaml
schema_version: aistock_validation_resource_policies_v1
policies:
  isolated_dev_db:
    description: Dev DB or temporary test database writes only.
    allowed_db_targets: [dev_db, temp_db]
    forbidden_db_targets: [prod_db]
    validation_namespace_required: true
    cleanup_required: true
    default_ttl_hours: 72

  shadow_schema_small_sample:
    description: Small sample write into shadow schema with validation_run_id.
    allowed_db_targets: [shadow_schema]
    forbidden_db_targets: [prod_db]
    validation_namespace_required: true
    cleanup_required: true
    max_sample_symbols: 20
    max_date_window_days: 5
    default_ttl_hours: 72

  validation_paper_account:
    description: Isolated paper portfolio/account/session for simulation validation.
    resource_types: [paper_portfolio, paper_session, paper_order, paper_ledger]
    validation_namespace_required: true
    cleanup_required: true
    default_ttl_hours: 72

  validation_qe_experiment:
    description: Small QE validation experiment with bounded loop/seed/universe.
    resource_types: [qe_experiment, qe_workspace, qe_artifact]
    validation_namespace_required: true
    cleanup_required: true
    max_sample_symbols: 50
    max_runtime_minutes: 60
    default_ttl_hours: 168

  qlib_candidate_path:
    description: Non-production Qlib/H5/Bin candidate path validation.
    resource_types: [qlib_candidate_path, qlib_report]
    forbidden_paths: [/home/lc999/data/qlib_bin, /home/lc999/data/qlib_minute_bin]
    validation_namespace_required: true
    cleanup_required: false
    production_promotion_required: true

  prod_db_readonly:
    description: Read-only production DB validation, e.g. DR and data freshness checks.
    allowed_db_targets: [prod_readonly]
    forbidden_operations: [insert, update, delete, truncate, drop, alter]
    cleanup_required: false
    manual_approval_required: false
```

### 5.2 Catalog integrity 必须校验

新增规则：

| 规则 | 失败级别 | 说明 |
|---|---|---|
| RESOURCE-001 | P0 | `writes_business_state=true` 但缺少 `resource_policy` |
| RESOURCE-002 | P0 | `creates_validation_resources=true` 但缺少 resource_types |
| RESOURCE-003 | P0 | `cleanup_required=true` 但缺少 cleanup_command 或 cleanup_policy |
| RESOURCE-004 | P0 | `resource_mode=prod_approved_write` 但缺少 manual_approval_required |
| RESOURCE-005 | P0 | forbidden_db_targets 未包含 prod_db，且非 prod_readonly 计划 |
| RESOURCE-006 | P1 | L4/L5 计划缺少 runtime timeout 或 evidence policy |
| RESOURCE-007 | P1 | candidate path 未声明 forbidden production paths |
| RESOURCE-008 | P1 | max_sample_symbols / max_date_window_days 缺失，且计划会写数据 |

## 6. 近期功能测试计划草案

### 6.1 Research Pipeline

#### `research_pipeline_backend`

```yaml
- plan_key: research_pipeline_backend
  title: Research Pipeline backend contract tests
  module: research_pipeline
  level: L2
  command_key: nox_research_pipeline_backend
  nox_session: research_pipeline_backend
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: false
  writes_artifacts: false
  writes_business_state: false
  runner_enabled: true
  max_duration_seconds: 300
  evidence_kinds: [pytest]
  resource_policy:
    resource_mode: none
    business_state_write: none
```

验收：

- schema/service/router pytest 通过。
- 不启动生产 backend。
- 不写生产 DB。
- 如果使用 fixture DB，必须是 temp/dev DB。

#### `research_pipeline_ui`

```yaml
- plan_key: research_pipeline_ui
  title: Research Pipeline UI route and readonly API smoke
  module: research_pipeline
  level: L2
  command_key: nox_research_pipeline_ui
  nox_session: research_pipeline_ui
  enabled: true
  requires_backend: false
  requires_frontend: true
  writes_database: false
  writes_artifacts: true
  writes_business_state: false
  mock_api_used: true
  runner_enabled: false
  max_duration_seconds: 600
  evidence_kinds: [playwright]
  resource_policy:
    resource_mode: readonly
    business_state_write: none
```

额外目录要求：

- `/research-pipeline` 必须登记到 `ui_targets.yaml`。
- `module_registry.yaml` 新增 `research_pipeline` 或归属到 `analysis/research` 时必须明确。

### 6.2 Data Sync Autonomy

#### `data_sync_autonomy_backend`

```yaml
- plan_key: data_sync_autonomy_backend
  title: Data Sync Autonomy backend state-machine and dry-run tests
  module: local_data
  level: L2
  command_key: nox_data_sync_autonomy_backend
  nox_session: data_sync_autonomy_backend
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: false
  writes_artifacts: true
  writes_business_state: false
  runner_enabled: true
  max_duration_seconds: 300
  evidence_kinds: [pytest, dry_run]
  resource_policy:
    resource_mode: readonly
    business_state_write: none
```

验收：

- 状态机、reconciliation、dry-run 通过。
- 不写生产 DB。
- 如果测试需要 DB，使用 fake repository 或 dev DB fixture。

#### `data_sync_autonomy_shadow_l3`

```yaml
- plan_key: data_sync_autonomy_shadow_l3
  title: Data Sync Autonomy shadow-schema small-sample validation
  module: local_data
  level: L3
  command_key: nox_data_sync_autonomy_shadow_l3
  nox_session: data_sync_autonomy_shadow_l3
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: true
  writes_artifacts: true
  writes_business_state: true
  runner_enabled: false
  max_duration_seconds: 1800
  evidence_kinds: [db_smoke, reconciliation, cleanup]
  resource_policy:
    resource_mode: shadow_schema_small_sample
    business_state_write: isolated
    allowed_db_targets: [shadow_schema]
    forbidden_db_targets: [prod_db]
    creates_validation_resources: true
    resource_types: [shadow_schema_rows, validation_sync_job]
    validation_namespace_required: true
    validation_run_id_required: true
    cleanup_required: true
    cleanup_command: python scripts/validation_resource_cleanup.py --resource-kind data_sync_shadow --run-id ${VALIDATION_RUN_ID} --apply
    ttl_hours: 72
    max_sample_symbols: 10
    max_date_window_days: 3
```

验收：

- 只写 shadow schema 或 dev DB。
- 每行/每个 job 都带 `validation_run_id`。
- cleanup 后验证 shadow rows 为 0 或标记 archived。

### 6.3 QE MCP / QE Template

#### `qe_mcp_backend`

```yaml
- plan_key: qe_mcp_backend
  title: QE MCP and template backend contract tests
  module: qe_mcp
  level: L2
  command_key: nox_qe_mcp_backend
  nox_session: qe_mcp_backend
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: false
  writes_artifacts: false
  writes_business_state: false
  runner_enabled: true
  max_duration_seconds: 300
  evidence_kinds: [pytest]
  resource_policy:
    resource_mode: none
    business_state_write: none
```

#### `qe_mcp_l3`

```yaml
- plan_key: qe_mcp_l3
  title: QE MCP L3 dev-port archive/template integration
  module: qe_mcp
  level: L3
  command_key: nox_qe_mcp_l3
  nox_session: qe_mcp_l3
  enabled: true
  requires_backend: true
  requires_frontend: true
  writes_database: false
  writes_artifacts: true
  writes_business_state: false
  runner_enabled: false
  allowed_backend_ports: [8012, 8013]
  allowed_frontend_ports: [3012, 3013]
  max_duration_seconds: 1200
  evidence_kinds: [pytest, playwright, mcp]
  resource_policy:
    resource_mode: readonly
    business_state_write: none
```

### 6.4 QE 小样本验证实验

#### `qe_smoke_validation_experiment_l4`

```yaml
- plan_key: qe_smoke_validation_experiment_l4
  title: QE small validation experiment nightly smoke
  module: qe
  level: L4
  command_key: nox_qe_smoke_validation_experiment_l4
  nox_session: qe_smoke_validation_experiment_l4
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: true
  writes_artifacts: true
  writes_business_state: true
  runner_enabled: false
  max_duration_seconds: 3600
  evidence_kinds: [qe_run, artifact_manifest, cleanup]
  runtime_policy:
    default_trigger: nightly
    allow_mcp_start: false
    allow_github_actions: true
    allow_local_manual: true
    requires_self_hosted_runner: true
    requires_gpu: false
    timeout_seconds: 3600
  resource_policy:
    resource_mode: isolated_write
    business_state_write: isolated
    allowed_db_targets: [dev_db]
    forbidden_db_targets: [prod_db]
    creates_validation_resources: true
    resource_types: [qe_experiment, qe_workspace, qe_artifact]
    validation_namespace_required: true
    validation_run_id_required: true
    cleanup_required: true
    cleanup_command: python scripts/validation_resource_cleanup.py --resource-kind qe_experiment --run-id ${VALIDATION_RUN_ID} --apply
    ttl_hours: 168
    max_sample_symbols: 50
    max_date_window_days: 120
```

验收：

- 创建 `validation_*` QE 实验。
- loop 数量、seed、股票池、日期范围受限。
- 生成 artifact manifest。
- cleanup 可删除 workspace 或标记 archived。

### 6.5 MiniQMT / 模拟盘

#### `miniqmt_sim_backend`

```yaml
- plan_key: miniqmt_sim_backend
  title: MiniQMT simulation backend contract tests
  module: qmt
  level: L2
  command_key: nox_miniqmt_sim_backend
  nox_session: miniqmt_sim_backend
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: false
  writes_artifacts: false
  writes_business_state: false
  runner_enabled: true
  max_duration_seconds: 300
  evidence_kinds: [pytest]
  resource_policy:
    resource_mode: none
    business_state_write: none
```

#### `miniqmt_strategy_ledger_l3`

```yaml
- plan_key: miniqmt_strategy_ledger_l3
  title: MiniQMT validation account order/fill/ledger integration
  module: qmt
  level: L3
  command_key: nox_miniqmt_strategy_ledger_l3
  nox_session: miniqmt_strategy_ledger_l3
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: true
  writes_artifacts: true
  writes_business_state: true
  runner_enabled: false
  max_duration_seconds: 1800
  evidence_kinds: [pytest, ledger, cleanup]
  resource_policy:
    resource_mode: isolated_write
    business_state_write: isolated
    allowed_db_targets: [dev_db]
    forbidden_db_targets: [prod_db]
    creates_validation_resources: true
    resource_types: [validation_account, validation_order, validation_fill, validation_ledger]
    validation_namespace_required: true
    validation_run_id_required: true
    cleanup_required: true
    cleanup_command: python scripts/validation_resource_cleanup.py --resource-kind miniqmt_validation_account --run-id ${VALIDATION_RUN_ID} --apply
    ttl_hours: 72
```

验收：

- 只能使用 validation account。
- 不允许真实 QMT account。
- 订单、成交、ledger 都带 validation_run_id。
- 验证 T+1、冻结资金、卖出落账、rebalance drop holdings 等关键 BUG 场景。

### 6.6 Qlib Candidate Smoke

#### `qlib_candidate_smoke_l4`

```yaml
- plan_key: qlib_candidate_smoke_l4
  title: Qlib candidate data path smoke and mini backtest
  module: qlib_data
  level: L4
  command_key: nox_qlib_candidate_smoke_l4
  nox_session: qlib_candidate_smoke_l4
  enabled: true
  requires_backend: false
  requires_frontend: false
  writes_database: false
  writes_artifacts: true
  writes_business_state: false
  runner_enabled: false
  max_duration_seconds: 3600
  evidence_kinds: [qlib_provider, mini_backtest, artifact_manifest]
  resource_policy:
    resource_mode: candidate_write
    business_state_write: none
    allowed_db_targets: []
    forbidden_db_targets: [prod_db]
    creates_validation_resources: true
    resource_types: [qlib_candidate_path, qlib_report]
    validation_namespace_required: true
    validation_run_id_required: true
    cleanup_required: false
    ttl_hours: 168
    max_sample_symbols: 100
    max_date_window_days: 365
    production_promotion_required: true
    manual_approval_required: false
```

验收：

- 只读非生产 candidate path。
- 禁止覆盖 `/home/lc999/data/qlib_bin`、`/home/lc999/data/qlib_minute_bin`。
- 能初始化 Qlib provider，读取 calendar/instrument/feature。
- 能跑 mini backtest smoke 并输出 report。

### 6.7 Paper v2 Live / 模拟盘验证

当前已有 `paper_v2_live`。建议补充 resource policy：

```yaml
resource_policy:
  resource_mode: isolated_write
  business_state_write: isolated
  allowed_db_targets: [dev_db]
  forbidden_db_targets: [prod_db]
  creates_validation_resources: true
  resource_types: [paper_portfolio, paper_session, paper_order, paper_fill, paper_ledger]
  validation_namespace_required: true
  validation_run_id_required: true
  cleanup_required: true
  cleanup_command: python scripts/validation_resource_cleanup.py --resource-kind paper_v2_live --run-id ${VALIDATION_RUN_ID} --apply
  ttl_hours: 72
runtime_policy:
  default_trigger: nightly
  allow_mcp_start: false
  requires_self_hosted_runner: true
  requires_market_hours: false
  requires_tdx: false
```

`--require-live-bars` 应保持人工显式开启，不进入默认 nightly。

## 7. Catalog Integrity 规则扩展

### 7.1 新增完整性校验范围

`validation_catalog_integrity` 应读取：

- `test_plans.yaml`
- `module_registry.yaml`
- `ui_targets.yaml`
- `file_ownership.yaml`
- `resource_policies.yaml`
- `backend/services/validation/plan_catalog.py`
- `noxfile.py`
- `.github/workflows/*.yml`
- 前端导航源 `frontend/src/lib/navigation/nav-groups.ts`

### 7.2 新增发现类型

| finding_id | 严重级别 | 描述 |
|---|---|---|
| `RESOURCE-001` | P0 | `writes_business_state=true` 缺少 `resource_policy` |
| `RESOURCE-002` | P0 | 创建 validation resource 但缺 resource_types |
| `RESOURCE-003` | P0 | 需要 cleanup 但缺 cleanup command |
| `RESOURCE-004` | P0 | 允许 prod write 但缺人工批准字段 |
| `RESOURCE-005` | P0 | 生产相邻计划未禁止 prod_db |
| `RESOURCE-006` | P1 | L4/L5 缺 runtime timeout |
| `RESOURCE-007` | P1 | candidate path 缺 forbidden production paths |
| `RESOURCE-008` | P1 | 小样本写入缺 max_sample_symbols/max_date_window_days |
| `PLAN-ALLOWLIST-001` | P0 | command_key 不在 `plan_catalog.py` |
| `PLAN-NOX-001` | P0 | nox_session 不存在 |
| `UI-ROUTE-001` | P1 | 前端 route 未登记 ui target |
| `MODULE-OWNERSHIP-001` | P1 | 新文件无法映射模块 |

## 8. 实施计划

### 阶段 1：目录和设计先行

- 新增本设计文档。
- 在 P0/P1 平台设计文档中引用本方案。
- 新增 `resource_policies.yaml` 的 schema 草案。
- 先不把所有草案 plan 直接写入 `test_plans.yaml`，避免当前 nox session 未实现导致 catalog 失败。

### 阶段 2：实现 catalog integrity

- 实现 `validation_catalog_integrity` nox session。
- 支持 resource policy 校验。
- 将其纳入 PR CI static-gate。

### 阶段 3：逐项接入近期功能

按顺序：

1. Research Pipeline：补 `test_plans.yaml`、`ui_targets.yaml`、`plan_catalog.py`。
2. Data Sync Autonomy：等功能分支完成后补 resource policy。
3. QE MCP：补 backend/L3 计划。
4. MiniQMT：补 L2/L3 validation account 计划。
5. Qlib Candidate：补 L4 candidate smoke。

### 阶段 4：实现清理脚本与 evidence manifest

- 新增 `scripts/validation_resource_cleanup.py`。
- 新增 `tmp/validation/resources/<run_id>.json` manifest。
- 清理失败自动进入 BUG。

## 9. 合入门禁建议

在 catalog integrity 实现前：

- 生产相邻功能合入前必须在 PR 描述中列明测试计划和资源隔离方式。
- 若测试计划还未实现，功能状态必须标记为 `validation_plan_pending` 或 `L4 pending`。

在 catalog integrity 实现后：

- 新增 route 未登记 ui target：阻断合入。
- 新增 nox session 未登记 test plan：P1，按模块风险可阻断。
- 新增生产相邻 test plan 缺 resource policy：阻断合入。
- 计划使用 prod write 且无人工批准字段：阻断合入。

## 10. 验收标准

本设计完成后，下一阶段实现应满足：

1. `validation_catalog_integrity` 能检测 test plan 与 nox/allowlist/ui route/resource policy 不一致。
2. 任何 `writes_business_state=true` 的测试计划必须声明 resource policy。
3. Research Pipeline 的 `/research-pipeline` route 缺登记问题可通过 catalog 补齐并验证。
4. Data Sync Autonomy 能明确区分 backend dry-run 测试和 shadow schema 小样本测试。
5. MiniQMT 相关测试不能使用真实账户，只能使用 validation account。
6. QE 小样本实验必须有 validation experiment id、TTL、cleanup command。
7. Qlib candidate smoke 必须证明没有覆盖生产路径。

## 11. 建议结论

建议先按本方案补齐测试计划目录 schema、resource policy 和 catalog integrity，再开始生产相邻功能的大规模开发。这样后续每个功能的测试资源边界、清理责任、夜间验证层级、合入门禁都能被机器检查，而不是依赖人工记忆。
