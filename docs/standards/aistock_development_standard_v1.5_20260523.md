# AIstock 项目开发规范 v1.5

> 版本：1.5
> 更新日期：2026-07-18
> 状态：唯一人类可读开发规范
> 权威文件：`docs/standards/aistock_development_standard_v1.5_20260523.md`
> 机器派生目录：`docs/standards/aistock_development_standard_v1.5_20260523.yaml`
> 历史材料：`docs/standards/archive/`

## 1. 权威边界

本文统一定义 AIstock 的开发、测试、交付和生产 aftercare 规则，适用于 backend、frontend、数据管线、QE/RD-Agent、Paper Trading、交易接入、脚本、文档和 Agent 工作流。

- 本文是唯一规范源；同主题文档、quickstart、Codex skill 和 Claude command 只提供场景入口或操作说明，并引用本文。
- YAML 是本文的机器派生目录，用于扫描和流水线；规则含义以本文为准，稳定 rule ID 供工具引用。
- 规范路径保持稳定。修订直接更新本文和同版本 YAML，历史由 Git 和 `docs/standards/archive/` 保存。
- `docs/standards/README.md` 只维护权威入口和场景路由。
- 规则变更与对应测试在同一 PR 中提交；客户端入口在合入后由 `install-client` 同步。

## 2. 统一执行流程

### 2.1 任务分流

1. 从 `aistock-task-router` 进入，选择且只选择一个任务 lane：BUG、feature、docs、merge aftercare、read-only triage 或 validation delegation。
2. BUG 使用 `scripts/aistock_issue_workflow.py`；非平凡 feature 使用 `scripts/aistock_feature_workflow.py`；其他任务使用对应 skill/command。
3. 先读取项目记忆、任务卡和 Context Pack 中与当前任务直接相关的内容，再按精确符号或 CodeGraph/UA 引用定位代码。
4. 上下文压缩或重启后使用 `resume` 和 task-card digest 恢复，只有摘要变化或证据缺失时才重新读取规则文件。

### 2.2 工作空间和范围

1. 非平凡 feature、BUG 和流程规范变更从最新 `origin/main` 创建 task branch 与独立 worktree。
2. `F:\Dev\AIstock` 作为同步和运行基线；实现发生在 `F:\Dev\AIstock_worktrees\<task>`。
3. BUG JSON 与 GitHub Issue 在同一工作流同步，`allowed_write_scope` 在编辑前确定；范围变化先更新登记信息，再继续实现。
4. 每个提交只包含当前任务文件；发现并发写入或远端分支意外移动时保留现场并交由用户决定。

### 2.3 实现和证据

1. 先明确期望行为、直接受影响模块、风险级别、验收条款和最小验证矩阵。
2. 实现沿用现有服务边界、配置来源、数据语义和 UI 设计系统；受保护资产通过其正式版本、迁移或服务入口变更。
3. 失败返回结构化错误或 `partial/failed`，日志包含输入摘要、失败阶段和复现线索。
4. PR 证据记录 changed files、直接测试、scope check、生产依赖门禁和剩余风险。

### 2.4 PR、合入和 aftercare

1. PR 前运行本任务的最小本地 gate，并执行 BUG `finish --plan-only` 或 feature design validation。
2. CI 只执行变更模块的必要计划和全仓通用轻量检查；深度回归由 Validation Center/CI/nightly 去重执行。
3. 合入后同步 BUG/GitHub 状态、根目录 `main`、task worktree/branch 和客户端入口。
4. 生产 DDL、依赖安装、运行时激活分别报告为 `noop`、`applied_and_verified` 或 `pending`；代码合入与运行时激活是两个独立结果。

## 3. 风险与工作量分级

### 3.1 质量严重度

| 等级 | 适用风险 | 处理方式 |
|---|---|---|
| P0 | 交易、资金、回测结论、资产完整性、生产隔离 | 修复当前风险并提供直接安全证据后进入合入流程 |
| P1 | 数据完整、可复现、长期稳定、流程一致性 | 在当前模块完成修复和回归证据 |
| P2 | 性能、可维护性、UI 可读性、覆盖率趋势 | 以 warning、模块计划或同模块重构处理 |
| P3 | 命名、注释、局部风格 | 在相关改动中顺手改进 |

### 3.2 任务执行级别

| 等级 | 典型任务 | 本地验证 |
|---|---|---|
| T0 | 文档、配置说明、registry 或客户端入口小改 | 解析/格式、直接 contract、`git diff --check` |
| T1 | 单模块低风险 BUG | changed-file 检查、直接 fix-point 测试、模块 L0 |
| T2 | 多文件或高风险单模块变更 | T1 加关键不变量、接口或 fail-closed 测试 |
| T3 | 跨模块、架构、生产关键路径 | 设计验收、依赖模块计划、委托深度验证 |

严重度描述业务后果，任务级别描述执行成本。工作流根据 changed files、所有权、层数和生产影响选择最轻且足够的级别。

## 4. 测试和验证路由

### 4.1 changed-file 路由

1. changed files 先映射到 `file_ownership.yaml` 和 `module_registry.yaml`，再从 `test_plans.yaml` 选择该模块的 required plan。
2. 功能或 BUG 的直接测试覆盖实际修改点、关键分支和失败语义；测试在修复前应能暴露问题，在修复后证明预期结果。
3. 通用 gate 仅包含真正跨模块且成本低的检查，例如编译/静态检查、scope、catalog integrity 和 `git diff --check`。
4. 其他模块的测试只在存在明确依赖边、共享契约变化或跨模块验收条款时加入，并在证据中说明原因。
5. 同模块、同风险、同验证链的 issue 可批处理，保留每个 issue 的提交映射与关闭证据。

### 4.2 本地验证预算

1. 本地循环先运行失败 nodeid、`pytest --lf` 或最小 contract smoke；行为稳定后运行一次最终小矩阵。
2. 超过任务卡命令预算、约 30 分钟，或需要 UI/API/business-flow/跨模块深度覆盖时，使用 Validation Center/CI/nightly。
3. nightly 对当天合入变更按模块和计划去重，返回紧凑 receipt；PR 只记录直接相关的通过证据和委托计划。
4. 过期、重复、只验证实现细节或与模块无依赖关系的测试从 active plan 移除或归档。

### 4.3 测试价值标准

有效测试至少满足一项：

- 证明用户可观察行为或批准的验收条款。
- 证明业务不变量、数据口径、失败语义或安全边界。
- 保护稳定 API、schema、artifact 或模块间契约。
- 复现真实 BUG，并能区分修复前后的行为。

测试名称、fixture 和断言应表达业务原因；仅重复实现、恒真断言、无依赖跨模块运行和已废弃流程不进入 active gate。

## 5. Feature 和设计验收

<a id="rule-feature-workflow-001"></a>
### 5.1 [FEATURE-WORKFLOW-001] 分级设计流程

- F0 使用轻量 Feature Card；F1 使用单模块设计；F2 使用跨模块或生产关键架构设计。
- F0/F1/F2 保持稳定的 `Design Acceptance Index`，实现条目引用代码、测试和运行证据。
- PR 前运行 `python scripts/aistock_feature_workflow.py validate --design <path> --tier F0|F1|F2`。
- 用户批准的范围变化先更新设计与验收索引，再进入实现。

<a id="rule-design-compliance-001"></a>
### 5.2 [DESIGN-COMPLIANCE-001] 四项设计符合性检查

完成、请求合入或报告验收通过前逐项确认：

1. **禁止简化交付**：禁止把未批准的简化版、子集版、POC、占位、mock-only 或 partial 实现声明为完整交付。
2. **禁止静默错误**：禁止吞掉错误、伪造成功、用默认值掩盖失败或以空结果冒充完整结果。
3. **禁止改变业务逻辑**：禁止改变经批准设计、验收标准和用户需求定义的业务语义；批准后的范围调整同步回设计。
4. **禁止私增门禁审批**：禁止私自增加设计之外的门禁、审批、人工确认或发布阻断；必要的新控制先说明成本、风险和替代方案并取得批准。

验收矩阵格式：`设计/需求条款 -> 实现位置 -> 测试/API/UI/DB 证据 -> 结论`。未完成条目明确记录 gap、影响和下一步。

<a id="rule-design-main-001"></a>
### 5.3 [DESIGN-MAIN-001] 设计交付

详细设计包含测试方案、结果验证方法和可合入标准。验证通过后通过独立分支和 PR 进入 `main`，并在需要时由后续 feature/BUG 实现引用。

## 6. 工程和业务执行规则

<a id="rule-arch-wsl-001"></a>
### 6.1 [ARCH-WSL-001] Worker artifact 边界

Windows backend 通过 worker API、AIstock-owned artifact store、入库 payload 或带 manifest 的 artifact URI 获取 WSL/远端产物。worker 内部脚本、文档示例和测试 fixture 由各自运行环境管理。

<a id="rule-prod-port-001"></a>
### 6.2 [PROD-PORT-001] 生产端口隔离

开发与验证使用 backend `8011/8012`、frontend `3011/3012`。生产 `8001/3000/19080` 的启动、停止和重启在用户明确授权后单独执行并记录结果。

<a id="rule-prod-ddl-001"></a>
### 6.3 [PROD-DDL-001] Production DDL gate

数据库 DDL 和 DML 先在现有 DEV 数据库完成验证。验证环境沿用该 DEV 数据库；生产库备份、导出或快照属于独立运维事项。生产 DDL/DML 在用户明确授权具体生产目标后执行。DEV 验证、生产授权、迁移执行和回读校验分别记录；无 schema 变化时 `production_ddl_gate=noop`。schema 变更以 migration/bootstrap 文件交付，并在获授权的生产目标执行 preflight、DDL/DML、前后 schema/comment 校验和 API/scheduler smoke。

<a id="rule-err-fallback-001"></a>
### 6.4 [ERR-FALLBACK-001] 错误可见性

异常路径返回结构化错误、业务异常或 `partial/failed`；Parser 汇总缺失字段，UI 展示错误码、原因和修复建议。该规则执行 5.2 的“静默错误”检查。

<a id="rule-trading-fallback-001"></a>
### 6.5 [TRADING-FALLBACK-001] 交易和回测语义

Paper Trading、QE、HMM 和执行算法在分钟线、pre_close、limit、suspend、模型上下文或 coefficient 缺失时进入明确失败状态。显式降级模式需配置可见、日志可审计、测试覆盖且保持批准的业务语义。

<a id="rule-config-hardcode-001"></a>
### 6.6 [CONFIG-HARDCODE-001] 配置来源

路径、端口、worker 地址、artifact root、数据库连接和密钥来自显式请求、配置、环境变量、DB catalog 或 manifest。effective config 在 QE/Paper/回测前展开并持久化，密钥通过受控 secret 来源提供。

<a id="rule-qe-artifact-001"></a>
### 6.7 [QE-ARTIFACT-001] Artifact manifest

QE/RD-Agent/Qlib 产物记录 `artifact_id/type/uri/storage_tier`、hash、size/row_count/schema、时间、来源任务/loop、producer、quality status 和 missing sections。数仓保存独立于 runtime DB 与 worker workspace 的可追溯副本。

<a id="rule-ui-rawjson-001"></a>
### 6.8 [UI-RAWJSON-001] 操作员业务视图

操作员 UI 使用中文业务标签、表格、卡片、图表、错误态和缺失原因；raw JSON 作为可选高级调试视图。

<a id="rule-ui-design-system-001"></a>
### 6.9 [UI-DESIGN-SYSTEM-001] UI 设计基准

新增 UI 沿用已批准的 shadcn-style operator shell；Research Assistant 使用 assistant-ui conversation primitives。旧 Paper v2 样式只服务其现有范围，跨模块复用通过公共组件和设计 token 实现。

<a id="rule-resource-timeout-001"></a>
### 6.10 [RESOURCE-TIMEOUT-001] 资源生命周期

HTTP、subprocess、DB 长查询和批处理设置 timeout、取消、日志、退出码和资源释放；长任务提供 heartbeat、状态持久化与幂等恢复。

<a id="rule-db-comment-001"></a>
### 6.11 [DB-COMMENT-001] 数据库语义

新表和字段随 migration 提供 `COMMENT ON TABLE/COLUMN`，说明业务语义、单位、来源、可空与质量语义；JSONB comment 包含 schema/version/source/quality 约束。

<a id="rule-root-pollution-001"></a>
### 6.12 [ROOT-POLLUTION-001] 根目录归属

根目录保存稳定入口和顶层配置。一次性文件进入 `debug_tools/<module>/<date_or_issue>/`，临时输出进入 `tmp/`，可复用业务工具进入 `scripts/`。

<a id="rule-script-location-001"></a>
### 6.13 [SCRIPT-LOCATION-001] 脚本归属

诊断脚本提供输入摘要、复现命令、非零失败退出码和写入型 dry-run；重复使用的脚本升级为 `scripts/` 或 service 并补直接测试。

<a id="rule-doc-location-001"></a>
### 6.14 [DOC-LOCATION-001] 文档归属

规范位于 `docs/standards/`，设计位于 `docs/architecture/`，分析位于 `docs/analysis/`，运维位于 `docs/operations/`，用户说明位于 `docs/user_guides/`，验证历史位于 `tests/aistock_validation/history/`。同主题保留一个主文档，其他文件引用主文档。

<a id="rule-memory-dataframe-001"></a>
### 6.15 [MEMORY-DATAFRAME-001] 数据规模边界

大 CSV/parquet/pickle/H5/Qlib 读取使用 columns、date/symbol range、chunk/batch 或明确容量评估；cache 提供 max size、TTL、clear 或生命周期；长任务记录规模和阶段耗时。

<a id="rule-algo-complexity-001"></a>
### 6.16 [ALGO-COMPLEXITY-001] 算法复杂度

多维量化循环和大表 join 记录 key 唯一性、输入规模、行数上界与 row-explosion 风险，并选择向量化、预聚合、分块或数据库过滤方案。

<a id="rule-debug-failfast-001"></a>
### 6.17 [DEBUG-FAILFAST-001] 诊断失败语义

诊断工具沿用结构化错误、上下文日志和非零退出码，并对写入或清理动作提供明确目标、dry-run 与确认文本。该规则执行 5.2 的“静默错误”检查。

<a id="rule-issue-github-sync-001"></a>
### 6.18 [ISSUE-GITHUB-SYNC-001] Issue 双向登记

正式 BUG 由工作流分配 `BUG-NNN`，同步创建或关联 GitHub Issue，并在 BUG JSON 保存 `github_issue_number/url`、scope、验证计划和生产 gate。未确认候选保留在 candidate/tmp 区。

<a id="rule-rdagent-release-identity-001"></a>
### 6.19 [RDAGENT-RELEASE-IDENTITY-001] RD-Agent 不可变发布身份

RD-Agent 源码合入与运行部署是两个独立结果。部署只接受已合入目标分支的 merge commit，并从 clean checkout 构建以 merge SHA 命名的不可变 release；部署前验证 repository、merge SHA、Git tree 和 manifest，验证通过后原子切换 `current` 指针。禁止向 dirty root、生产 root 或 `main` checkout 执行逐文件 `Copy-Item`、文件级 checkout、archive 解包或其他源码 overlay；发现 dirty source、未合入 HEAD、commit/tree/manifest 不一致或 release builder 未实现时显式失败，不回退文件级 overlay。

RD-Agent 运行状态统一写入 repo 外的 `RDAGENT_STATE_ROOT`，覆盖 QE workspace、日志、scheduler JSONL、MLflow、registry、cache、history、artifact CAS 和 QELT outbox；源码/release 目录只保留 Git 管理的代码、模板和 manifest。生产运行缺少显式 `RDAGENT_STATE_ROOT` 或状态路径落入源码/release 目录时阻断启动，不使用 repo-relative 或 `Path.cwd()` 静默回退。

每次部署生成独立 receipt，至少记录 repository、merge SHA、tree hash、manifest hash、release path、node、时间、执行者、部署前后运行路径和 rollback target。源码合入、release 构建、部署、重启、运行验证和回滚分别报告；回滚通过不可变 release 指针切换，不修改 release 内容，也不新增数据库导出、备份、研究审批或业务门禁。

## 7. 上下文、批处理和生产依赖

<a id="rule-context-budget-001"></a>
### 7.1 [CONTEXT-BUDGET-001] 上下文预算

- T0/T1 默认使用 task card、Context Pack、所有权映射、图摘要和目标代码片段。
- T2/T3 按明确依赖加载设计和跨模块契约。
- 命令输出采用 compact receipt；完整 JSON 只用于失败诊断、状态恢复或持久证据。
- 精确搜索无结果时记录 scoped miss reason，再扩大搜索范围。

<a id="rule-issue-batch-context-001"></a>
### 7.2 [ISSUE-BATCH-CONTEXT-001] 同模块批处理

同模块、相同风险、相容 scope 和相同验证链的 issue 使用一个 batch worktree。Batch Context Pack 记录 issue 列表、共享文件、逐 issue 验收、提交映射、共享测试和拆分条件。

<a id="rule-prod-dependency-001"></a>
### 7.3 [PROD-DEPENDENCY-001] Production dependency gate

依赖文件变化在合入后、运行时激活前同步到生产 checkout：frontend 执行锁文件一致的安装与 build/import smoke；backend 执行环境对应的依赖安装与关键 import/version smoke。无依赖变化时记为 `noop`。

## 8. 一致性与客户端执行

<a id="rule-std-sync-001"></a>
### 8.1 [STD-SYNC-001] 单一规范一致性

1. Markdown 中每个机器规则保留稳定 anchor；YAML 的 `standard_ref` 指向该 anchor。
2. YAML 解析、rule ID/reference 一致性和 changed-file guardrail 在规范 PR 中运行。
3. `scripts/issue_flow.py` 的 Context Pack 只注入本文的任务相关 anchor。
4. Codex 与 Claude Code 的仓库入口引用本文和同一 workflow CLI，不复制规范正文。

### 8.2 客户端同步

- 全局 Codex 与全局 Claude Code：合入后执行 `python scripts/aistock_issue_workflow.py install-client --apply`。
- 官方隔离 Codex：对明确的 `CODEX_HOME` 执行 `install-client --codex-home <path> --skip-claude --apply`。
- 每个目标使用 `verify-clients --workflow-only` 和对应的 `--codex-home`/`--claude-home`/`--skip-*` 参数校验全部 workflow lane 的 hash；旧窗口在同步后重启以加载新入口。
- 客户端 profile 只在本次任务明确列入目标时更新。

## 9. 完成报告

完成报告包含：branch、commit、PR、changed files、直接测试、scope check、委托/nightly 计划、production DDL/dependency gate、merge/close-sync/root-sync/cleanup 状态，以及运行时和 DB 是否发生变化。

“代码已合入”“BUG 已 close-sync”“客户端已同步”“生产依赖已应用”“运行时已激活”分别报告，便于准确判断交付状态。
