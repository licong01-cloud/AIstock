# AIstock Validation Center commit 模块质量矩阵与工作区未提交监控设计方案

> 日期：2026-05-05
> 状态：detailed design draft v1.0
> 文档位置：`docs/architecture/aistock_validation_commit_module_quality_design_20260505.md`
> 依赖设计：`docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`、`docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`、`docs/architecture/aistock_validation_menu_route_coverage_design_20260505.md`、`docs/standards/aistock_development_standard_v1.1_20260504.md`
> 适用范围：AIstock 内置 Validation Center 的模块注册表、文件归属、commit 影响分析、Git/GitHub 同步状态、工作区未提交文件监控、模块质量矩阵、测试优先级治理。
> 明确边界：本方案不新建独立微服务；不让 UI 执行任意 shell；不重启生产 `8001`；不以 LLM 作为 commit 模块归属的权威来源；不把历史通过证据误报为当前 commit 已通过。

## 1. 结论先行

Validation Center 后续应升级为“代码变更、模块风险、测试覆盖、未提交状态、遗留债务”的统一质量驾驶舱。核心结论如下：

1. **模块注册表是权威来源**：AIstock 的业务模块、技术模块、文件归属、UI route、API route、测试计划、风险等级，统一由机器可读 catalog 管理。
2. **每个代码文件必须有确定性模块归属**：commit 属于哪个模块，必须由修改文件集合自动推导，不能每次依赖 LLM 语义分析。
3. **新增文件在开发阶段就要归属模块**：Codex、Claude Code 或人工新增文件时，文件路径必须被模块规则匹配；否则 L0 guardrail 失败。
4. **工作区未提交状态必须可视化**：Validation Center UI 需要实时显示已修改、已暂存、未跟踪、删除、重命名但尚未提交的文件，并按模块归类提醒尽快提交或处理。
5. **本地已提交但未推送 GitHub 也要提示**：除未提交文件外，还要显示本地分支相对 upstream/GitHub 的 ahead commits，避免“本地已 commit、但未 push”的质量证据断链。
6. **模块质量矩阵基于事实数据生成**：Git commit、changed files、module registry、validation runs、coverage snapshots、findings、bugs、workspace dirty status 共同生成模块状态颜色和优先级。
7. **LLM 只做辅助，不做裁判**：LLM 可以建议未归属文件应属于哪个模块，也可以解释风险，但最终必须落到 `module_registry.yaml` / `file_ownership.yaml` 并通过流水线校验。
8. **历史遗留问题进入 baseline，不阻塞全部开发**：当前全库历史问题较多，第一阶段先建立基线；新增/修改文件严格执行规则，历史遗留按模块逐步治理。

目标形态：

```text
Git working tree / commit history / GitHub upstream
  -> git_status_provider 读取本地事实
  -> module_registry + file_ownership 确定文件归属
  -> commit_impact_analyzer 计算 commit/module/layer/risk
  -> validation_history 聚合 run/coverage/evidence/bugs/findings
  -> module_quality_service 生成模块质量矩阵
  -> Validation Center UI 显示：未提交文件、未推送 commit、模块质量、测试优先级、遗留问题
```

## 2. Phase 0 现有资料与能力发现

| 资料 | 关键发现 | 本方案复用方式 |
|---|---|---|
| `docs/codex_project_memory.md` | 明确 AIstock 服务结构、QE/Paper/HMM 高风险边界、生产 `8001` 隔离、DB comment 规范、测试标准 | 作为硬约束 |
| `docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md` | 已定义 L0-L5、run metadata、evidence、coverage、bug/finding、guardrail baseline | 扩展 commit/module/workspace 质量维度 |
| `docs/architecture/aistock_validation_menu_route_coverage_design_20260505.md` | 菜单路由覆盖视图已设计为 `NAV_GROUPS` + `ui_targets.yaml` + route coverage API | route coverage 关联到 module registry |
| `docs/standards/aistock_development_standard_v1.1_20260504.md` | 已包含目录规范、禁止根目录污染、禁止硬编码绝对路径、禁止静默 fallback、测试和提交规范 | 落到 module guard 和 changed-file guard |
| `backend/routers/validation.py` | 已有 `/validation/health`、`/plans`、`/runs`、`/coverage`、`/evidence`、`/findings`、`/bugs`、`/executions`、`/summary` 等 API | 新增 Git/module/workspace API 时复用同一路由前缀和 `ValidationResponse` |
| `backend/services/validation/history_store.py` | 已能读取 `tests/aistock_validation/history` 下 run markdown、coverage JSON、evidence JSON | 模块质量矩阵复用历史 run/coverage/evidence |
| `tests/aistock_validation/catalog/test_plans.yaml` | 已有 allowlisted nox plan、端口约束、runner_enabled、evidence_kinds | module registry 需要关联 `test_plans` |
| `tests/aistock_validation/modules/*.md` | 已有若干模块测试矩阵文档 | 迁移或引用到机器可读 module registry |

当前 Validation Center 已有 API 可继续复用：

- `GET /api/v1/validation/health`
- `GET /api/v1/validation/plans`
- `GET /api/v1/validation/runs`
- `GET /api/v1/validation/coverage`
- `GET /api/v1/validation/evidence`
- `GET /api/v1/validation/findings`
- `GET /api/v1/validation/bugs`
- `GET /api/v1/validation/executions`
- `GET /api/v1/validation/summary`

新增 API 必须继续满足：只读 API 不执行任意 shell；可执行 API 只能调用 allowlisted nox session；不触碰生产 `8001`；返回结构化 reason codes，不用自然语言推断替代事实字段。

## 3. 用户目标

用户需要在 AIstock UI 中随时回答以下问题：

1. 当前工作区有哪些文件被修改但还没有提交 Git？
2. 哪些文件已经暂存，哪些只是修改未暂存，哪些是未跟踪新文件，哪些被删除或重命名？
3. 这些未提交文件分别属于哪些业务模块或技术模块？
4. 是否存在新增文件没有模块归属？
5. 是否存在本地已 commit 但尚未 push 到 GitHub 的提交？
6. 最近一天、一周、一个月有哪些 commit？分别影响哪些模块？
7. 某个模块最近有多少变更？是否已经跑过完整流水线？
8. 某个模块当前覆盖率、测试等级、质量问题、bug、历史遗留债务如何？
9. 哪些模块因为频繁修改但缺乏 L2/L3/L4 验证，需要提高测试优先级？
10. 新增代码文件是否违反目录规范、模块归属规范或 guardrail 红线？

## 4. 权威模块体系设计

### 4.1 模块不等于菜单

AIstock 的菜单结构主要面向用户导航，不能作为质量治理的唯一模块边界。模块体系需要同时覆盖：

| 维度 | 示例 | 用途 |
|---|---|---|
| 产品业务模块 | QE 单次实验、QE 自动演进、因子库、Paper v2、Selection Center、HMM、QMT | 用户功能质量、业务验证、UI 覆盖 |
| 技术逻辑模块 | 统一配置层、统一执行层、统一数据服务、DB schema、artifact store、worker API | 代码 ownership、跨功能风险、测试优先级 |
| 数据模块 | Tushare、TDX、Qlib exporter、因子数据、PIT universe、suspend_d、stk_limit | 数据质量、刷新、PIT、回测真实性 |
| 横切基础模块 | Validation Center、监控、配置、安全、日志、任务调度、测试基础设施 | 平台治理、工程质量 |
| 文档/规范模块 | architecture、standards、analysis、operations | 文档归属和规范检查 |

权威关系：

```text
module_registry.yaml 是模块权威
NAV_GROUPS 是正式菜单权威
ui_targets.yaml 是页面测试目标权威
commit impact 由 changed files + file_ownership 推导
```

### 4.2 初版模块层级

| 一级模块 | 子模块示例 | 风险等级 |
|---|---|---|
| `qe` | `qe.single_experiment`、`qe.custom_evolution`、`qe.auto_evolution`、`qe.enhanced_metrics`、`qe.data_completeness`、`qe.archive` | high |
| `paper_v2` | `paper_v2.portfolios`、`paper_v2.day_runner`、`paper_v2.live_session`、`paper_v2.execution_algo`、`paper_v2.ledger` | critical |
| `selection_center` | `selection_center.runtime`、`selection_center.package_selection`、`selection_center.industry_filter`、`selection_center.hmm_runtime` | high |
| `strategy_package` | `strategy_package.manifest`、`strategy_package.model_assets`、`strategy_package.selection_artifacts` | high |
| `hmm` | `hmm.training`、`hmm.snapshots`、`hmm.coefficients`、`hmm.qe_runtime` | high |
| `factor_library` | `factor_library.catalog`、`factor_library.metrics`、`factor_library.correlation`、`factor_library.generation` | high |
| `model_library` | `model_library.registry`、`model_library.training`、`model_library.attribution` | high |
| `qlib_data` | `qlib_data.exporter`、`qlib_data.bin`、`qlib_data.pit_universe`、`qlib_data.minute` | critical |
| `local_data` | `local_data.ingestion`、`local_data.dataset_audit`、`local_data.tushare`、`local_data.tdx` | high |
| `qmt` | `qmt.client`、`qmt.router`、`qmt.account` | critical |
| `rdagent` | `rdagent.task`、`rdagent.assets`、`rdagent.sync` | medium/high |
| `validation` | `validation.center`、`validation.runner`、`validation.coverage`、`validation.guardrails`、`validation.module_quality` | medium/high |
| `platform` | `platform.api`、`platform.db`、`platform.config`、`platform.scheduler`、`platform.monitoring` | high |
| `frontend_common` | `frontend_common.navigation`、`frontend_common.api_client`、`frontend_common.components` | medium |
| `docs` | `docs.architecture`、`docs.standards`、`docs.analysis`、`docs.operations` | low/medium |
| `debug_tools` | `debug_tools.one_off`、`debug_tools.diagnostics` | medium |
| `tests` | `tests.backend`、`tests.frontend`、`tests.validation_history`、`tests.fixtures` | medium |

## 5. 机器可读 registry 设计

### 5.1 推荐文件位置

建议新增：

```text
tests/aistock_validation/catalog/module_registry.yaml
tests/aistock_validation/catalog/file_ownership.yaml
```

后续如果需要与开发规范保持双向同步，可把摘要同步到 `docs/standards` 的机器可读规范，但流水线运行时应从 `tests/aistock_validation/catalog` 读取，避免 architecture 文档成为运行依赖。

### 5.2 `module_registry.yaml` 示例

```yaml
schema_version: aistock_module_registry_v1
modules:
  - module_id: qe.single_experiment
    display_name: QE 单次实验
    parent_module: qe
    domain: qe
    module_type: product_feature
    risk_level: high
    description: QE 单次实验创建、配置、运行、状态同步、详情展示和增强指标读取。
    ui_routes:
      - /quantevolver
      - /quantevolver/experiments
    api_routes:
      - /api/v1/quantevolver/*
    source_path_groups:
      backend_router:
        - backend/routers/quantevolver.py
      backend_service:
        - backend/services/quantevolver/**
      frontend_ui:
        - frontend/src/app/quantevolver/**
      tests:
        - backend/tests/**/test_qe_*.py
        - frontend/tests/qe/**
    test_plans:
      required_on_change:
        - l0
        - qe_read_l3
      recommended:
        - qe_data_contract_backend
    quality_policy:
      require_mapping: true
      require_tests_on_change: true
      require_business_evidence_for_done: true
      stale_after_days: 7
```

关键字段：

| 字段 | 含义 |
|---|---|
| `module_id` | 稳定机器 ID，使用小写点分层级 |
| `display_name` | UI 中文显示名 |
| `parent_module` | 父模块，支持聚合 |
| `domain` | 业务域，例如 `qe`、`paper_v2`、`platform` |
| `module_type` | `product_feature`、`technical_layer`、`data_pipeline`、`cross_cutting`、`docs`、`tests` |
| `risk_level` | `low`、`medium`、`high`、`critical` |
| `ui_routes` | 关联正式菜单或隐藏页面 route |
| `api_routes` | 关联后端 API route pattern |
| `source_path_groups` | 按 layer 组织的典型源码路径 |
| `test_plans` | required/recommended/nightly 测试计划 |
| `quality_policy` | 当前模块质量门禁策略 |

### 5.3 `file_ownership.yaml` 示例

```yaml
schema_version: aistock_file_ownership_v1
rules:
  - rule_id: qe_services
    include:
      - backend/services/quantevolver/**
    exclude:
      - backend/services/quantevolver/archive/**
    primary_module: qe.core
    impact_modules:
      - qe.single_experiment
      - qe.custom_evolution
      - qe.auto_evolution
    layer: backend_service
    risk_level: high
    ownership_reason: QE 核心服务被单次实验和演进流程共用。

  - rule_id: validation_center_frontend
    include:
      - frontend/src/app/validation-center/**
      - frontend/src/lib/validation/**
    primary_module: validation.center
    impact_modules:
      - validation.module_quality
      - validation.runner
    layer: frontend_ui
    risk_level: medium

  - rule_id: docs_architecture
    include:
      - docs/architecture/**
    primary_module: docs.architecture
    layer: docs
    risk_level: low
```

规则要求：

- 每个文件必须匹配一个且仅一个 `primary_module`。
- 一个文件可以有多个 `impact_modules`。
- 多规则匹配时必须通过 `priority` 或更具体路径消歧，不能静默任选。
- `exclude` 必须优先于 `include`。
- Windows 路径统一规范化为 repo-relative POSIX 风格，例如 `backend/routers/validation.py`。
- `docs/architecture`、`docs/standards`、`docs/analysis`、`debug_tools`、`tests` 也必须有归属，不能只覆盖业务代码。

## 6. 全库模块梳理与基线扫描

### 6.1 必须先做基线的原因

当前项目经历多个开发工具和探索阶段，存在历史遗留文件、根目录文件、旧文档、一次性脚本、未归类测试和可能废弃代码。如果直接启用严格阻断，会导致流水线无法落地。因此应先建立只读基线：

```text
全库文件列表
  -> file_ownership 规则匹配
  -> 分类为 mapped / unmapped / ambiguous / ignored / deprecated_candidate
  -> 输出 baseline report
  -> 新增/修改文件按严格规则执行
  -> 历史问题进入治理计划
```

### 6.2 扫描范围

- 受 Git 跟踪的所有文件：`git ls-files`。
- 未跟踪但未被 `.gitignore` 排除的文件：`git ls-files --others --exclude-standard`。
- 工作区修改文件：`git status --porcelain=v2 -z`。
- 删除和重命名文件用于 commit impact，不作为当前归属覆盖率的 active 文件统计。

### 6.3 基线输出

建议输出：

```text
tests/aistock_validation/baselines/module_ownership_baseline.json
tests/aistock_validation/history/standards/<timestamp>_l1_module-ownership-baseline.md
```

核心字段：

```json
{
  "schema_version": "aistock_module_ownership_baseline_v1",
  "generated_at": "2026-05-05T00:00:00+08:00",
  "git_head": "...",
  "totals": {
    "tracked_files": 1234,
    "untracked_files": 12,
    "mapped_files": 1100,
    "unmapped_files": 100,
    "ambiguous_files": 34
  },
  "by_module": [
    {
      "module_id": "qe.single_experiment",
      "file_count": 82,
      "risk_level": "high"
    }
  ],
  "unmapped_files": [],
  "ambiguous_files": [],
  "deprecated_candidates": []
}
```

历史遗留处理：

| 文件类别 | 第一阶段处理 | 后续治理 |
|---|---|---|
| 已归属 active 文件 | 正常纳入模块质量矩阵 | 按模块覆盖率和测试计划治理 |
| 未归属历史文件 | 记录 baseline，不立即阻断 | 修改该文件时必须补归属 |
| 多重归属历史文件 | 记录冲突，不立即阻断 | 优先修复高风险模块冲突 |
| 根目录历史垃圾 | 记录为 `legacy_root_pollution` | 逐步迁移/归档/删除 |
| 可能废弃代码 | 记录为 `deprecated_candidate` | 通过引用扫描和用户确认后清理 |
| 新增未归属文件 | 直接 L0 失败 | 开发阶段补 registry 或移动到规范目录 |

## 7. commit 影响分析设计

commit 影响模块由 changed files 决定：

```text
commit hash
  -> git show --name-status / git log --numstat
  -> changed_files[]
  -> file_ownership.match(file)
  -> primary_modules + impact_modules + layers + risk_levels
  -> required_test_plans
  -> stale validation / missing coverage / quality warnings
```

算法规则：

1. 每个 changed file 先规范化路径。
2. 删除文件使用删除前路径归属模块。
3. 重命名文件同时计算 old path 和 new path；若模块改变，标记 `module_boundary_move`。
4. 文档文件也参与 commit 归属，但风险等级通常较低。
5. 测试文件归属到 `tests.*`，同时通过路径反向关联被测模块。
6. 公共基础设施文件使用 `primary_module=platform.*`，并通过 `impact_modules` 扩散到相关业务模块。
7. 若任何 changed file `unmapped` 或 `ambiguous`，commit impact 状态为 `needs_mapping`。

commit 分析输出示例：

```json
{
  "commit_hash": "abc123",
  "short_hash": "abc123",
  "author": "...",
  "committed_at": "2026-05-05T12:00:00+08:00",
  "subject": "feat(validation): add module quality view",
  "changed_file_count": 6,
  "primary_modules": ["validation.center", "validation.module_quality"],
  "impact_modules": ["validation.runner", "frontend_common.navigation"],
  "layers": ["backend_service", "backend_router", "frontend_ui", "tests"],
  "max_risk_level": "high",
  "required_test_plans": ["l0", "validation_center_backend", "validation_center_ui"],
  "validation_status": "stale",
  "reason_codes": ["no_l3_run_after_commit"],
  "files": [
    {
      "path": "backend/routers/validation.py",
      "change_type": "modified",
      "primary_module": "validation.center",
      "impact_modules": ["validation.module_quality"],
      "layer": "backend_router"
    }
  ]
}
```

UI 需要支持今日、最近 7 天、最近 30 天、自定义时间范围的 commit 汇总，展示 commit 数、文件数、影响模块、高风险变更、未验证 commit、未归属文件和未推送 commit。

## 8. 工作区未提交文件监控设计

### 8.1 需求拆分

“有修改，但是没有提交 Github”需要拆成两个层次：

1. **工作区未提交文件**：文件已修改/新增/删除/重命名，但还没有进入本地 Git commit。
2. **本地未推送 GitHub commit**：文件已经进入本地 commit，但该 commit 尚未 push 到 upstream/GitHub。

Validation Center 应同时展示两类风险，避免只看 `git status` 而漏掉本地 ahead commit。

### 8.2 本地 Git status 读取方式

后端新增 `GitWorkspaceStatusProvider`，只允许执行固定的、只读的 Git 命令：

```powershell
git status --porcelain=v2 --branch -z
git diff --name-status -z HEAD
git diff --cached --name-status -z
git ls-files --others --exclude-standard -z
git log --oneline --decorate --max-count=50
git rev-parse --abbrev-ref HEAD
git rev-parse --abbrev-ref --symbolic-full-name @{u}
git rev-list --left-right --count HEAD...@{u}
```

第一阶段不自动执行 `git fetch`，因为它会访问网络并更新本地 remote refs。UI 应标注：

```text
GitHub 同步状态基于本地 upstream 跟踪分支；如需确认远端最新状态，可后续增加受控手动刷新。
```

### 8.3 工作区状态分类

| 状态 | Git 来源 | UI 含义 |
|---|---|---|
| `staged_added` | index added | 已暂存新增文件，待 commit |
| `staged_modified` | index modified | 已暂存修改，待 commit |
| `staged_deleted` | index deleted | 已暂存删除，待 commit |
| `unstaged_modified` | worktree modified | 已修改但未暂存 |
| `unstaged_deleted` | worktree deleted | 已删除但未暂存 |
| `renamed` | rename status | 文件重命名，展示 old/new path |
| `untracked` | `git ls-files --others` | 新文件未纳入 Git，必须判断模块归属 |
| `conflicted` | unmerged status | 合并冲突，最高优先级提醒 |
| `ignored` | 默认不展示 | 可通过高级开关显示 |

每个文件必须补充：

- repo-relative path。
- change status。
- staged/unstaged/untracked flags。
- primary module。
- impact modules。
- layer。
- risk level。
- ownership status：`mapped`、`unmapped`、`ambiguous`、`ignored_by_policy`。
- 是否位于禁止目录。
- 是否为大文件或生成物候选。
- 推荐操作：commit、add ownership、move to debug_tools、ignore、confirm delete 等。

### 8.4 新增 API

| API | 方法 | 用途 |
|---|---|---|
| `/api/v1/validation/git/workspace-status` | GET | 当前工作区未提交文件、staged/untracked、模块归属、风险 |
| `/api/v1/validation/git/branch-status` | GET | 当前分支、upstream、ahead/behind、本地未推送 commit 数 |
| `/api/v1/validation/git/commits` | GET | 最近 commit 列表，支持 day/week/month/page |
| `/api/v1/validation/git/commits/{commit_hash}` | GET | 单个 commit 的模块影响详情 |
| `/api/v1/validation/git/unpushed-commits` | GET | 本地 ahead commits 及模块影响 |
| `/api/v1/validation/modules` | GET | 模块 registry 列表和模块质量摘要 |
| `/api/v1/validation/modules/{module_id}` | GET | 模块详情、文件、route、plan、run、coverage、bug/finding |
| `/api/v1/validation/module-quality` | GET | 模块质量矩阵 |
| `/api/v1/validation/module-ownership/scan` | GET | 只读全库文件归属扫描摘要 |

所有 API 返回 `ValidationResponse` 包装，保持与当前 Validation Center 一致。

### 8.5 `workspace-status` response 示例

```json
{
  "schema_version": "aistock_git_workspace_status_v1",
  "generated_at": "2026-05-05T21:00:00+08:00",
  "repo_root": "F:/Dev/AIstock",
  "branch": "main",
  "upstream": "origin/main",
  "head_commit": "abc123",
  "dirty": true,
  "summary": {
    "changed_files": 12,
    "staged_files": 2,
    "unstaged_files": 7,
    "untracked_files": 3,
    "conflicted_files": 0,
    "unmapped_files": 1,
    "ambiguous_files": 0,
    "critical_risk_files": 2
  },
  "files": [
    {
      "path": "backend/routers/validation.py",
      "old_path": null,
      "status": "unstaged_modified",
      "primary_module": "validation.center",
      "impact_modules": ["validation.module_quality"],
      "layer": "backend_router",
      "risk_level": "medium",
      "ownership_status": "mapped",
      "recommended_action": "run_validation_and_commit"
    },
    {
      "path": "debug_tmp.py",
      "status": "untracked",
      "primary_module": null,
      "impact_modules": [],
      "layer": null,
      "risk_level": "medium",
      "ownership_status": "unmapped",
      "reason_codes": ["root_directory_new_file", "no_module_mapping"],
      "recommended_action": "move_to_debug_tools_or_add_mapping"
    }
  ],
  "by_module": [
    {
      "module_id": "validation.center",
      "changed_file_count": 4,
      "max_risk_level": "medium",
      "required_test_plans": ["l0", "validation_center_backend", "validation_center_ui"]
    }
  ],
  "reason_codes": ["workspace_dirty", "untracked_files_present"]
}
```

### 8.6 UI 行为

Validation Center 首页顶部增加“Git 工作区状态”区域：

```text
当前分支 main | upstream origin/main | HEAD abc123
工作区有 12 个未提交文件 | 本地 ahead 2 commits | 未归属新文件 1 个
```

详情区分为四个 tab：

| Tab | 内容 |
|---|---|
| 未提交文件 | staged/unstaged/untracked/deleted/renamed 文件列表，支持按模块/状态/风险过滤 |
| 未推送 commit | 本地 ahead commits，按 commit 展示影响模块和验证状态 |
| 模块影响 | 当前工作区所有变更聚合到模块后的风险和推荐测试计划 |
| 处理建议 | 需要补模块归属、需要移动目录、需要跑哪些 nox、需要尽快 commit/push 的提醒 |

提示等级：

| 等级 | 触发条件 | UI 提示 |
|---|---|---|
| P0 | conflicted files、critical 模块未提交、生产风险文件未提交 | 红色 banner，建议立即处理 |
| P1 | high 模块未提交、unmapped 新文件、根目录新增文件 | 橙色提醒，提交前必须修复 |
| P2 | 普通修改超过 4 小时未提交、当天变更多 | 黄色提醒，建议分批提交 |
| P3 | 文档或低风险文件未提交 | 灰/蓝提示 |

UI 不自动 commit 或 push。UI 只负责展示、提醒、生成验证建议和复制命令；实际 commit/push 仍由开发工具或人工执行。

### 8.7 刷新和性能

- 默认每 30 秒刷新一次工作区状态。
- 页面失焦时暂停或降频到 120 秒。
- 提供手动刷新按钮。
- 不自动执行网络请求或 `git fetch`。
- 不读取文件内容，不展示 diff 正文，避免意外暴露 secrets。
- 对超大未跟踪目录只展示摘要和前 N 个文件，避免 UI 卡死。
- Git status 结果缓存 5-10 秒，commit log 缓存 30-60 秒。

## 9. 模块质量矩阵设计

模块质量矩阵聚合以下输入：

| 输入 | 来源 |
|---|---|
| 模块定义 | `module_registry.yaml` |
| 文件归属 | `file_ownership.yaml` |
| 当前工作区变更 | `git workspace-status` |
| 最近 commit | 本地 git log / 后续 GitHub API |
| 未推送 commit | `rev-list HEAD...@{u}` |
| 测试计划 | `test_plans.yaml` |
| run 历史 | `ValidationHistoryStore.list_runs()` |
| coverage | `ValidationHistoryStore.list_coverage_snapshots()` |
| evidence | `ValidationHistoryStore.list_evidence_manifests()` |
| findings/bugs | `ValidationFindingStore` |
| UI route 覆盖 | `ui_targets.yaml` + route coverage service |

模块质量字段示例：

```json
{
  "module_id": "qe.enhanced_metrics",
  "display_name": "QE 增强指标",
  "risk_level": "high",
  "file_count": 42,
  "workspace_changed_file_count": 3,
  "recent_commit_count_7d": 8,
  "unpushed_commit_count": 1,
  "latest_commit": "abc123",
  "latest_validated_commit": "def456",
  "validation_status": "stale",
  "highest_recent_level": "L3",
  "coverage": {
    "line": 81.2,
    "branch": 70.1,
    "diff_line": 88.0,
    "status": "passed"
  },
  "findings": {
    "open_p0": 0,
    "open_p1": 2,
    "legacy_baseline": 13
  },
  "required_test_plans": ["l0", "qe_data_contract_backend", "qe_read_l3"],
  "priority_score": 86,
  "status_color": "orange",
  "reason_codes": [
    "workspace_changes_present",
    "latest_l3_before_latest_commit",
    "open_p1_findings"
  ]
}
```

状态颜色：

| 颜色 | 条件 |
|---|---|
| 绿色 | 无未提交/未推送变更，最近 commit 已通过要求等级，coverage gate 通过，无 P0/P1 |
| 黄色 | 有低/中风险未提交变更，或验证等级不足但无高风险 |
| 橙色 | high 模块有未验证 commit、unmapped 文件、coverage 缺失、P1 finding |
| 红色 | critical 模块未验证、P0 bug/finding、测试失败、冲突文件、禁止路径新增文件 |
| 灰色 | 仅历史基线问题，当前没有新增风险 |

优先级评分采用可解释规则，第一阶段不引入机器学习排序：

| 因子 | 权重建议 |
|---|---:|
| critical 模块 | +40 |
| high 模块 | +25 |
| 当前工作区有修改 | +15 |
| 本地有未 push commit | +10 |
| 最近 7 天 commit >= 5 | +10 |
| 最新 commit 没有对应 L2/L3 | +20 |
| coverage gate failed/missing | +20 |
| open P0 | +50 |
| open P1 | +25 |
| unmapped 新文件 | +30 |
| 历史 baseline 问题每 10 个 | +3 |

## 10. GitHub 对接设计

第一阶段建议仅依赖本地 Git：

- `git status` 读取工作区变更。
- `git log` 读取 commit 历史。
- `@{u}` 读取 upstream ahead/behind。
- 不强制联网。
- 不依赖 GitHub token。

第二阶段可增加 GitHub API 只读增强：

| 能力 | 用途 |
|---|---|
| PR 列表 | 显示 PR 与模块影响 |
| commit status/check runs | 展示 GitHub CI 状态 |
| issues | bug 权威记录同步 |
| compare API | 远端 ahead/behind 精确确认 |
| commit file list | 与本地分析结果交叉验证 |

GitHub 增强不应替代本地模块归属规则。即使使用 GitHub API，commit 模块归属仍由 AIstock 本地 `file_ownership.yaml` 计算。

安全边界：

- 不在 UI 保存 GitHub token 明文。
- 不允许 UI 发起任意 git 命令。
- 不允许 UI 自动 push。
- 不允许 API 返回文件内容或 diff 内容，除非后续增加明确权限和脱敏策略。
- 不允许网络失败导致本地质量状态被标记为通过；只能标记为 `github_status_unknown`。

## 11. 新增/修改文件开发规范

开发工具新增文件时必须满足：

1. 文件位于允许目录。
2. 文件被 `file_ownership.yaml` 匹配。
3. 文件有唯一 `primary_module`。
4. 高风险模块新增文件关联测试计划。
5. 临时诊断脚本必须放在 `debug_tools/`。
6. 设计文档放在 `docs/architecture/`。
7. 规范文档放在 `docs/standards/`。
8. 分析文档放在 `docs/analysis/`。
9. 业务复用脚本放在 `scripts/` 或模块内，并补测试。
10. 禁止在根目录新增一次性脚本、临时输出、报告文件。

若历史文件原本在 baseline 中是 `unmapped`，但当前 commit 修改了它，则必须补充归属规则、移动到规范目录、标记为 deprecated 并给出清理计划，或明确说明为什么排除并由 guardrail 记录 reason。不能继续扩大历史未归属状态。

Codex/Claude Code 开发流程应在任务开始和提交前执行：

```powershell
git status --short
python -m nox -s guardrail_changed_files
```

未来可通过专用 skill 或命令读取：

```text
GET /api/v1/validation/git/workspace-status
GET /api/v1/validation/module-quality
```

## 12. 后端实现设计

建议新增服务：

```text
backend/services/validation/module_registry.py
backend/services/validation/file_ownership.py
backend/services/validation/git_status_provider.py
backend/services/validation/commit_impact.py
backend/services/validation/module_quality.py
backend/services/validation/workspace_recommendations.py
```

职责：

| 服务 | 职责 |
|---|---|
| `module_registry.py` | 读取/校验 `module_registry.yaml`，提供模块查询 |
| `file_ownership.py` | 路径归属匹配、冲突检测、baseline scan |
| `git_status_provider.py` | 执行 allowlisted read-only git 命令并解析 status/log |
| `commit_impact.py` | changed files -> modules/layers/risk/test plans |
| `module_quality.py` | 聚合 git/history/coverage/findings/bugs 生成质量矩阵 |
| `workspace_recommendations.py` | 生成提交、测试、归属修复建议 |

`git_status_provider.py` 必须内置命令 allowlist，不接受 UI 参数拼 shell。

允许：

- `git status --porcelain=v2 --branch -z`
- 固定格式和固定数量的 `git log`
- 固定 commit hash 的 `git show --name-status`，hash 必须正则校验
- `git rev-list --left-right --count HEAD...@{u}`
- 固定参数的 `git ls-files`

禁止：

- `git reset`
- `git checkout`
- `git clean`
- `git add`
- `git commit`
- `git push`
- 任意 shell 拼接
- UI 传入命令片段

## 13. 前端 UI 设计

建议在 `/validation-center` 页面增加四个主区域：

1. **工作区状态总览**。
2. **commit 与 GitHub 同步**。
3. **模块质量矩阵**。
4. **模块详情 / 文件归属 / 推荐验证**。

可以作为现有页面的新增 tab：

```text
[总览] [工作区] [模块质量] [Commit] [页面路由覆盖] [执行记录] [覆盖率] [缺陷]
```

工作区 Tab：

- 顶部 summary cards：dirty count、untracked count、unmapped count、critical count、ahead commits。
- 文件列表支持过滤：状态、模块、风险、归属状态、路径搜索。
- 点击文件显示右侧详情：匹配规则、影响模块、推荐测试计划、相关最近 run。
- 对 untracked/unmapped 文件显示醒目提示。
- 提供“复制建议命令”按钮，但不直接执行 git 操作。

模块质量 Tab：

- 模块矩阵按一级模块分组。
- 每个模块显示状态颜色、commit 热度、未提交文件数、未推送 commit 数、最近验证等级、coverage、bug/finding。
- 点击模块进入详情。
- 支持筛选：只看 high/critical、只看 dirty、只看 stale、只看 failed、只看 unmapped。

Commit Tab：

- 显示今日/7天/30天 commit 汇总。
- 显示每个 commit 的模块影响。
- 标记 commit 是否有后续验证 run。
- 标记 commit 是否包含 unmapped/ambiguous 文件。
- 显示本地 ahead commits，提示尚未推送 GitHub。

## 14. 数据持久化策略

第一阶段不新增 DB 表，建议只读本地 Git 和 catalog 文件，动态计算：

- 工作区 status 实时读取。
- commit impact 动态计算或短缓存。
- 模块 registry 从 YAML 读取。
- 历史 run/coverage/evidence 从现有 history store 读取。

优点：不影响生产 DB；不需要 migration；不需要服务重启生产 `8001`；便于快速验证 schema 和 UI。

第二阶段如果需要趋势分析和快照，可新增 schema，例如 `validation`：

- `validation.git_commit`
- `validation.git_commit_file`
- `validation.commit_module_impact`
- `validation.workspace_status_snapshot`
- `validation.module_quality_snapshot`

若创建 DB 表，必须遵守 AIstock 规范：每张表必须有 `COMMENT ON TABLE`；每个字段必须有 `COMMENT ON COLUMN`；字段 comment 需要说明来源、单位、质量语义；DDL 不得由业务请求隐式执行；必须有 schema/comment smoke test。

## 15. 测试方案

设计阶段测试用例：

| 层级 | 测试内容 | 验证点 |
|---|---|---|
| L0 | module registry YAML schema 校验 | 必填字段、module_id 唯一、risk_level 合法 |
| L0 | file ownership 规则校验 | include/exclude 合法、无循环、无冲突优先级缺失 |
| L0 | changed files guard | 新增文件未归属失败，历史 baseline 未归属仅记录 |
| L1 | path matcher unit tests | Windows/POSIX 路径、exclude 优先、多规则冲突 |
| L1 | git status parser unit tests | staged/unstaged/untracked/renamed/deleted/conflicted 解析 |
| L1 | commit impact unit tests | changed files 正确映射 module/layer/risk/test plans |
| L2 | Validation API tests | workspace-status、modules、module-quality 分页和过滤 |
| L2 | safety tests | UI 参数不能注入 git command，禁止非 allowlisted 命令 |
| L3 | Validation Center UI E2E | 工作区列表、模块矩阵、commit 列表、筛选、详情 |
| L3 | dirty workspace fixture | 用临时 git repo 或 fixture 模拟未提交文件，不污染真实工作区 |
| L4 | 跨模块质量趋势 | 真实历史 run/coverage/evidence 与模块矩阵聚合一致 |

推荐新增 nox session：

```text
validation_module_registry_l0
validation_git_status_backend
validation_module_quality_backend
validation_module_quality_ui
validation_module_quality_l3
```

测试数据策略：

- 不在真实 repo 中制造大量 dirty 文件。
- 单元测试使用临时目录初始化 git repo。
- API 测试注入 fake `GitWorkspaceStatusProvider`。
- UI E2E 使用 mock API 或临时 fixture；若使用真实工作区，只做只读显示，不断言具体文件名。
- 不能因当前多窗口 dirty workspace 导致测试不稳定。

## 16. 分阶段实施计划

### Phase A：设计和基线

1. 新增本设计文档。
2. 创建 `module_registry.yaml` 初版 schema 和核心模块清单。
3. 创建 `file_ownership.yaml` 初版规则。
4. 实现只读 ownership baseline scanner。
5. 生成全库模块归属基线报告。
6. L0 只记录历史未归属，不阻断。

验收：module registry 可被解析；全库扫描能输出 mapped/unmapped/ambiguous 统计；不修改生产运行路径。

### Phase B：工作区未提交监控

1. 实现 `GitWorkspaceStatusProvider`。
2. 实现 workspace changed files -> module ownership。
3. 新增 `/validation/git/workspace-status` 和 `/validation/git/branch-status`。
4. UI 增加 Git 工作区状态总览和未提交文件列表。
5. 增加 L1/L2/API/UI 测试。

验收：UI 能显示 staged/unstaged/untracked/deleted/renamed；UI 能显示文件所属模块和未归属提醒；不执行任何写 Git 命令。

### Phase C：commit 影响与未推送 commit

1. 实现 commit log parser。
2. 实现 commit impact analyzer。
3. 新增 `/validation/git/commits` 和 `/validation/git/unpushed-commits`。
4. UI 增加今日/周/月 commit 汇总和未推送 commit 提醒。
5. 根据 commit 影响显示推荐测试计划。

验收：commit 模块归属完全由文件规则推导；本地 ahead commits 可见；未归属文件导致 commit impact 标记 `needs_mapping`。

### Phase D：模块质量矩阵

1. 实现 `ModuleQualityService`。
2. 聚合 workspace、commit、runs、coverage、evidence、bugs、findings。
3. 新增 `/validation/modules` 和 `/validation/module-quality`。
4. UI 增加模块质量矩阵。
5. 与菜单路由覆盖视图互相跳转。

验收：每个模块显示颜色、优先级、最近验证、coverage、bugs/findings；high/critical 模块未验证 commit 可醒目提示；历史 baseline 问题和新增问题区分展示。

### Phase E：GitHub 只读增强

1. 可选接入 GitHub API。
2. 显示 PR、check runs、issues、远端状态。
3. bug registry 与 GitHub Issues 建立引用。
4. 保持本地 registry 为模块归属权威。

验收：GitHub 网络失败不影响本地质量分析；不保存明文 token；不自动 push/merge。

## 17. 与既有方案的整合关系

| 既有方案 | 整合方式 |
|---|---|
| 自动化测试覆盖可观测总体方案 | 本方案补充代码变更和模块质量维度 |
| 菜单路由覆盖视图 | route 作为 module registry 的一个关联目标，不反向定义模块 |
| 开发规范 v1.1 | 新增文件归属、禁止根目录污染、测试先行等规则落到 guardrail |
| QE 数据完整性和数仓规划 | QE 相关模块标为 high/critical，commit 后必须有 QE 数据完整性测试建议 |
| Paper v2 测试缺口 | Paper v2 模块标为 critical，未验证 commit 在矩阵中高亮 |
| Bug/finding registry | bug/finding 按模块聚合，形成质量债和修复闭环 |

## 18. 风险与防护

| 风险 | 防护 |
|---|---|
| 当前工作区多窗口并行修改，UI 显示大量 dirty 文件 | UI 只展示事实，不自动处理；支持按模块/状态过滤 |
| 历史未归属文件太多导致无法落地 | 第一阶段 baseline 记录，不阻断；新增/修改严格 |
| 模块规则过粗导致 commit 归属不准确 | 每个文件唯一 primary module，公共文件用 impact_modules 扩散 |
| LLM 归类不稳定 | LLM 只建议，最终写入 YAML 并通过测试 |
| Git 命令安全风险 | allowlist 固定命令；不接受任意 shell；不执行写命令 |
| GitHub 网络/API 不稳定 | 第一阶段不依赖 GitHub；远端状态未知时不误报通过 |
| UI 自动刷新影响性能 | 缓存、分页、截断大目录、页面失焦降频 |
| 暴露敏感 diff 内容 | 第一阶段只返回路径和状态，不返回文件内容/diff |
| 当前测试因真实 dirty workspace 不稳定 | 测试使用 fake provider 或临时 git repo fixture |

## 19. 建议的下一步

推荐按以下顺序执行：

1. 先确认本设计方案。
2. 建立 `module_registry.yaml` 和 `file_ownership.yaml` 初版，覆盖核心模块和目录规范。
3. 实现只读全库模块归属扫描，生成 baseline。
4. 实现工作区未提交文件 API 和 UI，因为这是最直接需要的可观测能力。
5. 再实现 commit impact 和未推送 commit 展示。
6. 最后实现完整模块质量矩阵和测试优先级评分。

第一批开发不应直接接入 GitHub API，也不应新增 DB 表；应先用本地 Git + YAML catalog + 现有 Validation history 完成稳定的可验证闭环。
