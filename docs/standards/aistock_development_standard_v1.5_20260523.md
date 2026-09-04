# AIstock 项目开发规范 v1.5

> 版本：1.5
> 更新日期：2026-08-28
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
- 规则变更与对应测试在同一 PR 中提交；仅当 `.codex/**` 或 `.claude/**` 客户端入口发生变化时，合入后才由单一 owner 执行一次 `install-client` 同步。

**机器目录摘要一致性：** 人类可读 Markdown 是唯一规范权威，YAML 的 `source_sha256` 只证明机器目录对应当前 Markdown。任一规范文件进入 changed-files 范围时，既有 changed-files guardrail 必须按 UTF-8/LF 规范化计算摘要并在本地 fail-closed；失败输出一次 expected digest 和修复提示。该检查前移远端既有一致性测试，不新增 rule/control、runner job、测试矩阵、人工审批或第二份规范。

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

#### 2.3.1 控制效果、阶段与计数

1. 每个稳定控制 ID 在机器目录中只出现一次。`rules` 保存可自动扫描或已有机器入口的控制；`manual_review_controls` 保存纯人工控制或组合人工验收，组合人工验收可以引用已有 machine evidence，但禁止宣称其人工语义已被自动化，也禁止为同一 ID 建立第二份记录。
2. 每个控制明确记录 `effect` 和 `enforcement_phase`：
   - `block` 只阻断该控制适用的任务类型、文件范围和阶段，不升级为全任务或全仓门禁；
   - `warn` 产生可见警告和修复建议，但不阻断当前阶段；
   - `advisory` 只提供效率或执行建议，不进入完成、PR、合入或运行时判定。
3. `enforcement_phase` 使用 `changed_file_scan`、`task_planning`、`interactive_execution`、`pr_readiness`、`issue_lifecycle`、`design_delivery`、`standard_change`、`production_activation` 或 `release_deployment`。条件控制在不适用时记为 `noop`，禁止伪装成 pending gate。
4. CI job、页面 check、测试计划和门禁分别计数。skipped job、warning、advisory、delegated plan、诊断 evidence publisher 和 telemetry collector 都不是合并硬门禁。
5. 失败证据发布器保持 best-effort、可见和可审计，但禁止成为 `CI verdict` 的依赖或用自身故障覆盖真实测试结论。分支保护与自动合入器只消费仓库规定的稳定质量判定：`CI verdict`、`CodeQL verdict`、`AIstock Semgrep guardrails`、`Context, scope, and open-source tooling dry-run`；四项必须绑定当前 PR HEAD，缺失、等待或失败均禁止合入，其他诊断性 check 不自动升级为门禁。
6. 共享 runner 的 PR/push workflow 必须按 workflow 与 PR/ref 配置 `concurrency`，并对同一目标启用 `cancel-in-progress: true`；新 commit 发布后，旧 commit 的 queued/in-progress run 属于 superseded 诊断状态，不得继续占用 runner 或要求人工重新授权。
7. PR 失败诊断优先使用原失败 job 日志和 `GITHUB_STEP_SUMMARY`。禁止为报告、评论或重复上传证据单独排队一个 runner job，也不得让这类诊断额外下载 `upload-artifact`、`download-artifact` 或 `github-script` action；default-branch 的正式故障登记仍由独立 fail-closed workflow 负责。

### 2.4 PR、合入和 aftercare

1. PR 前运行本任务的最小本地 gate，并执行 BUG `finish --plan-only` 或 feature design validation。
2. CI 只执行变更模块的必要计划和全仓通用轻量检查；深度回归由 Validation Center/CI/nightly 去重执行。
3. 合入后同步 BUG/GitHub 状态、根目录 `main` 和已授权的 task worktree/branch；客户端入口未变化时明确记为 `noop`，只有 `.codex/**` 或 `.claude/**` 变化才进入单一 owner 的客户端同步流程。
4. 生产 DDL、依赖安装、运行时激活分别报告为 `noop`、`applied_and_verified` 或 `pending`；代码合入与运行时激活是两个独立结果。
5. 授权按动作和目标独立判断，但不按消息次数拆分。同一条用户指令可以明确打包源码合入、精确命名的 source worktree/local branch/remote branch cleanup，以及已通过 DEV 验证的具体生产目标与 migration；授权包完整时，合入后直接继续这些已授权动作，禁止再次索要同一授权。
6. 裸 `merge` 授权仍只覆盖源码合入和必要 BUG/metadata 同步，不推导 cleanup、DDL/DML、依赖、激活、进程控制或删除。打包动作逐项执行和报告；某一项前置条件失败只阻断该项，不伪造成功，也不扩大其他授权。
7. 仅修改 `tests/aistock_validation/bugs/**` 的 close-sync metadata push 不重复运行源码 CI、Semgrep 或 CodeQL analyze；只发布分支保护所需的稳定轻量 no-op verdict。source PR 已完成的质量判定和 commit-bound receipt 仍是权威证据。
8. 合入时 GitHub 尚未发布 required checks 或检查处于 queued/pending，workflow 使用短时、有上限的轮询；检查失败或轮询超时仍 fail-closed，但禁止把瞬时未发布状态转化为人工重复授权或无限 watch。
9. merge finalizer 在 runtime/client 推断前先幂等同步 canonical `main` 到已验证 merge commit；相同输入可安全重试，旧 root snapshot 不得触发业务窗口重新执行或抢占客户端 owner。
10. source PR 创建和 close-sync PR 创建遇到 GitHub GraphQL/TLS/EOF 等传输错误时，只允许对已推送且 SHA 已验证的 head 使用一次 REST 创建或读回；先查询同 head 的现有 open PR，创建后再次校验 head/base。权限、参数、冲突或策略错误不得降级，也不得重复创建 PR 或要求用户重新授权。

<a id="rule-worktree-cleanup-evidence-001"></a>
### 2.5 [WORKTREE-CLEANUP-EVIDENCE-001] worktree 临时证据终结与清理

1. worktree 内的完整测试日志、coverage/XML、Context Pack、PR 草稿、`tmp/**`、任务本地 `var/research_assistant/**`、Python/前端缓存和与 canonical root 内容一致的本地配置只属于临时执行产物；PR、BUG 或 Validation History 已保存与当前 commit 绑定的紧凑结构化 receipt 后，不长期保留这些副本。
2. 正式 receipt 至少保存 schema、receipt ID、commit/merge identity、验证种类或 plan、命令摘要、结果、时间和必要 digest；禁止用完整日志或 ignored 文件目录替代 receipt。Validation Center archive 成功后，worktree 内的源 artifact 立即转为可清理；archive 失败保持可见并阻断该 artifact 的清理，但不覆盖真实测试结论。规则生效前已经 `fixed`/`verified` 的历史 BUG，可用同时包含 PR、fix commit 和非空验证摘要的关闭记录作为一次性兼容终结依据；新记录禁止继续生成这种非结构化 receipt。
3. runtime `post-restart-verify` receipt 在 close-sync 消费前属于受保护证据。close-sync 通过后必须把 expected/observed identity、runtime proof、contract/catalog/probe digest、receipt SHA-256 和 gate 保存为不含响应正文的 durable summary；只有 durable summary 完整时，worktree-local receipt 才可清理。等待用户重启不要求保留 source worktree；后续 receipt 写入 canonical/registry workflow root 并由 close-sync 固化。源码 PR 合入后，source merge finalizer 必须先在 close-sync BUG JSON/PR 中固化 `source_merge_receipt_v1`，绑定 source PR、merge commit 和验证摘要；即使 runtime verification 为 `pending_user_restart`，也可据此独立清理干净且无活动进程引用的 source worktree，close-sync PR 保持打开并等待重启收据。
4. 已明确授权的精确 cleanup 在合入/close-sync 后连续执行，无需再次授权：先重新验证 PR/HEAD、干净状态、活动进程引用和路径边界，再生成 ignored artifact manifest；`transient` 可在同一次 cleanup 内删除，`protected` 或 `unknown` 必须 fail closed。裸 merge 仍不推导 cleanup。
5. cleanup 只能删除 manifest 中已分类且位于目标 worktree 内的精确 transient roots；禁止 `git clean`、通配符删除、越界路径、跟随 symlink/junction 到目标外或在 manifest 漂移后继续。与 canonical root 不同的本地配置、未固化 receipt、未知文件和活动进程引用均阻断删除。
6. 位于整体 ignored 资产树内的运行产物，只有精确分类器验证固定目录、有限且完整的文件名集合、非权威 schema、文件大小上限、无额外文件、无 tracked file、无 symlink/junction/reparse point，并继续通过活动进程引用与 manifest 漂移门禁后，才可标为 `transient`。任一条件不满足仍归为 `unknown`；禁止为兼容单类运行产物而放宽其父资产树。
7. 执行顺序固定为：evidence finalization → ignored/process manifest → transient purge → 普通 `git worktree remove` → local branch delete → remote branch SHA 校验与 delete → 路径/注册/local/remote 四态读回。任一步失败只报告已完成状态并停止后续破坏性动作，禁止伪造 `cleanup_done`。
8. 成功只保存紧凑 cleanup receipt，包括目标 identity、artifact manifest SHA-256、删除类别/数量、四态读回和耗时；完整文件清单仅用于失败诊断，不进入 PR 正文、标准或长期 handoff。
9. cleanup 对同一目标 worktree 只生成一次完整 ignored-artifact manifest；后续分类和删除复用该 manifest，并仅对实际删除的精确 roots 做漂移读回。除 manifest 漂移、路径身份变化或删除失败外，禁止重复遍历整个 ignored tree。
10. 同一次 merge finalizer 内，source 与 close-sync cleanup 可复用已成功完成且没有中间 main 变更的 `fetch origin --prune` receipt，以及各自前序步骤已验证的精确 PR/HEAD receipt；branch remote SHA、目标 worktree、ignored-artifact、活动进程和删除后四态仍逐目标重新验证。root-sync 因 canonical root 脏而 deferred 时，直接复用首次完整阻断 payload 生成提示并执行一次 `sync_root=false` 安全重试，禁止为生成相同提示插入第二次完整 dry-run 预检。

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
5. workflow receipt 分别记录 active repair、local validation、runner queue、PR CI、merge/aftercare 的已知耗时；无法直接观测的阶段保持 `not_recorded`，禁止把相邻事件间隔伪装成代码开发时间。RTK telemetry 只消费调用方已提供的使用/回退信息，不额外探测或形成门禁。

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

数据库 DDL 和 DML 先在现有 DEV 数据库完成验证。验证环境沿用该 DEV 数据库；生产库备份、导出或快照属于独立运维事项，不是 migration 前置门禁。生产 DDL/DML 在用户明确授权具体生产目标和 migration 后执行；该授权可以与 merge/cleanup 写在同一条指令中，完整授权包不需要合入后二次确认。执行顺序必须是 DEV receipt 已通过、不可变 merge commit 已确认进入目标分支、生产 target preflight、DDL/DML apply、前后 schema/comment 回读及 API/scheduler smoke。DEV 验证、生产授权、merge commit、迁移执行和回读校验分别记录；任一前置条件失败时仅将生产项记为 `blocked`/`pending`，不回滚或隐瞒已完成的源码合入。无 schema 变化时 `production_ddl_gate=noop`。

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

PR check watch、已完成 Actions run/job/log 诊断和 last-green 定位在 GraphQL/`gh run` 遇到 TLS、EOF、schannel、连接重置或超时时，先执行固定次数重试，再改用等价 GitHub REST endpoint；PR check 绑定 REST 回读的精确 HEAD SHA，Actions jobs 超过单页上限时 fail-closed。权限、策略、输入、404 或其他非传输错误不得改写成网络恢复。REST 与 GraphQL 都失败时返回结构化 unavailable/deferred 结果，不要求重新授权、不启动第二个 workflow、不循环重试。

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

合入与 close-sync 的关键 GitHub 读取优先使用有界 GraphQL 查询；仅当错误被识别为 TLS、EOF、timeout、connection reset 等传输故障时，必须自动切换到同一仓库、同一 PR 编号的 REST readback，并逐项校验 `merged`、完整 source head SHA、完整 merge commit SHA、`merged_at` 和 canonical PR URL。HTTP 权限错误、PR 未合入、身份缺失或不一致继续 fail closed。兼容参数 `--skip-github-check` 仅表示跳过 GraphQL 并直接执行上述精确 REST 校验，不得生成 `checked=false/merged=true` 的无证据结论，也不得跳过 Issue 的 reopen/close/comment/label 同步。

<a id="rule-rdagent-release-identity-001"></a>
### 6.19 [RDAGENT-RELEASE-IDENTITY-001] RD-Agent 不可变发布身份

RD-Agent 源码合入与运行部署是两个独立结果。部署只接受已合入目标分支的 merge commit，并从 clean checkout 构建以 merge SHA 命名的不可变 release；部署前验证 repository、merge SHA、Git tree 和 manifest，验证通过后原子切换 `current` 指针。禁止向 dirty root、生产 root 或 `main` checkout 执行逐文件 `Copy-Item`、文件级 checkout、archive 解包或其他源码 overlay；发现 dirty source、未合入 HEAD、commit/tree/manifest 不一致或 release builder 未实现时显式失败，不回退文件级 overlay。

RD-Agent 运行状态统一写入 repo 外的 `RDAGENT_STATE_ROOT`，覆盖 QE workspace、日志、scheduler JSONL、MLflow、registry、cache、history、artifact CAS 和 QELT outbox；源码/release 目录只保留 Git 管理的代码、模板和 manifest。生产运行缺少显式 `RDAGENT_STATE_ROOT` 或状态路径落入源码/release 目录时阻断启动，不使用 repo-relative 或 `Path.cwd()` 静默回退。

每次部署生成独立 receipt，至少记录 repository、merge SHA、tree hash、manifest hash、release path、node、时间、执行者、部署前后运行路径和 rollback target。源码合入、release 构建、部署、重启、运行验证和回滚分别报告；回滚通过不可变 release 指针切换，不修改 release 内容，也不新增数据库导出、备份、研究审批或业务门禁。

<a id="rule-tool-rtk-001"></a>
### 6.20 [TOOL-RTK-001] RTK 输出压缩

交互开发窗口对 RTK 已支持且预计产生高输出的命令必须使用 `rtk git`、`rtk gh`、`rtk rg`、`rtk pytest`、`rtk ruff`、`rtk tsc`、`rtk npm`、`rtk playwright` 等实际存在的包装。`nox` 当前没有专用 wrapper，使用原始 `python -m nox` 或一次可见的通用回退；禁止在规范、skill 或提示词中虚构 `rtk nox`。只有目标调用不受支持、RTK 不可用、诊断需要精确原始输出，或包装器首次执行失败/改变语义时才直接回退，并用一句话记录原因；禁止为同一能力重复探测。未受信任的项目自定义 filter 只是不加载该 filter，不影响继续使用 RTK 内置包装；禁止任何窗口自行执行 `rtk trust`。RTK 缺失或回退产生可见警告但不阻断任务、PR、合入或 CI；CI 使用原始确定性命令且不安装 RTK。workflow 仅消费调用方已有的 `rtk_used/version/fallback` telemetry，缺失时记录 `not_recorded`，不新增探测命令或门禁。

<a id="rule-backend-restart-ownership-001"></a>
### 6.21 [BACKEND-RESTART-OWNERSHIP-001] 后端重启所有权

用户后端的启动、停止和重启默认且持续归用户所有。BUG 修复、feature 实现、验证委派、CI、合入、close-sync、aftercare 或 cleanup 均不构成进程控制授权；Codex、Claude、Cursor、CLI、子代理、Validation Center 和其他窗口的后端进程控制权限保持为 `false`。只有用户针对本次任务和明确 target 单独授权时才可执行。issue workflow 的统一 subprocess 入口必须拒绝用户后端进程控制命令，changed-file guardrail 同时扫描直接命令和 restart helper 调用；workflow 只输出 catalog runbook，不消费重启授权。runner-owned 临时后端仅能在隔离端口、显式生命周期标记和本次验证范围内由 runner 管理。frontend 激活、客户端 reload、数据库迁移与后端重启分别建模和报告。

<a id="rule-bug-restart-effective-001"></a>
### 6.22 [BUG-RESTART-EFFECTIVE-001] BUG 重启后立即生效

所有影响 backend、worker 或 scheduler 运行时的 BUG 修复必须写入 Git 管理的持久来源或受控 migration/config；只依赖当前进程 monkey patch、热加载、手工缓存或未追踪文件的实现记为失败。task card 通过实际 changed files 和 runtime target catalog 的 `source_globs` 生成 lazy `runtime_contract`；显式字段只能补充或加强推断，不能降级实际影响，schema、target 集、持久化类型和位于 `docs/operations/` 的真实 runbook 不一致时 fail closed。当前单 receipt 模型遇到多 runtime target 或 runtime BUG batch 时要求拆分为单 BUG 流程。PR 前 fresh-process 证据写入 BUG JSON 和 PR body；runtime 源码 PR 只使用 `Refs` 并保持 Issue 打开。合入后由 workflow 输出 catalog target、operator runbook、预期 identity 和只读 smoke 引用，并先在 close-sync BUG JSON/PR 中固化与 source PR/merge commit 绑定的 `source_merge_receipt_v1`；该 receipt 只证明源码验证和 runtime pending，不能替代重启验证。用户完成重启后运行 `post-restart-verify`；probe 必须限定在 catalog origin，receipt 不保存响应正文，并绑定 runtime contract、catalog、完整 health/identity/业务 smoke 以及需要时的 DB readback digest，close-sync 逐项验证后才能标记 verified。等待用户重启期间状态为 `fixed_source_pending_user_restart`、`runtime_identity_match=pending`，GitHub Issue 保持打开；源码合入和已授权的 source cleanup 可独立完成，并分别保留其真实状态，close-sync PR 在重启验证前保持打开。runtime target catalog 缺失、冲突、probe/receipt 不完整或 close-sync worktree 未同步最新 `origin/main` 时显式阻断验证。

## 7. 上下文、批处理和生产依赖

<a id="rule-ci-environment-parity-001"></a>
### 6.23 [CI-ENVIRONMENT-PARITY-001] CI/Nightly 高效、预构建且 fail-closed

backend、frontend、Go、workflow validation、PR quality 和安全扫描等 CI 验证必须在预构建的 Windows self-hosted runner 上执行；运行前只允许执行环境身份、版本指纹和工具可用性检查。CI 禁止 `actions/setup-*`、`pip/conda/mamba install`、`npm ci/install`、`go mod download` 以及任何隐式依赖安装。环境缺失、指纹漂移或工具缺失时必须 fail-closed 并报告 `environment_mismatch`，不回退到 GitHub-hosted Linux、生产 `AIstock` 环境或临时安装环境。该规则是执行环境约束，不增加业务测试或人工审批。

Windows runner 容量使用两个隔离角色：普通 PR/模块验证使用 `aistock-ci`，CodeQL 与 Code Intelligence 使用 `aistock-ci-security`。两个角色必须对应不同的在线 runner identity、独立安装目录与独立 `_work`；只允许共享经 fingerprint/hash 校验的只读 Conda 环境、Git object mirror 和安全工具缓存，禁止复制 runner credential、工作目录或运行态日志。单机默认总并行度为 2，增加更多实例前必须重新给出 CPU、内存、磁盘与现有业务负载证据。角色拓扑只由一次快速 policy/health receipt 验证，不得拆成逐 evidence job、人工审批或新的业务门禁。

两个 Windows runner identity 必须在注册时使用 `--disableupdate`，并由独立版本号与 SHA-256 固定的本地安装包进行受控维护；禁止在 PR、Nightly、CodeQL 或 Code Intelligence 作业领取期间自动下载或应用 runner 更新。升级必须逐角色执行：确认目标角色无在途 job，停止该角色，校验安装包 hash，完成更新并取得 `online + canary accepted` 收据后，才能维护另一角色。禁用自动更新不等于永久冻结版本，维护 owner 必须在 GitHub 支持窗口内主动升级。每个角色必须通过 Windows service 或具有单实例 mutex、精确 stop sentinel、有限重启窗口和结构化状态/事件收据的受监督隐藏入口运行；一次性 `Start-Process`、仅靠 API `online` 状态或无限重启循环都不满足生命周期合同。健康检查发现匹配 runner 显示 online/idle、但对应 workflow 的 queued job 超过 10 分钟仍未分配时，必须分类为 `online_but_not_accepting_work` 基础设施故障并提示检查自更新/Listener，不得归因于业务测试、重复提交 PR、切换到 GitHub-hosted Linux、跨角色执行或增加人工合入门禁。

CodeQL CLI 属于预构建工具链而不是运行期依赖。CodeQL job 必须把本地 toolcache 路径、固定版本和 identity manifest SHA-256 纳入环境校验，并直接调用已校验的本地 `codeql.exe` 执行 `database create`、`database analyze` 和 `github upload-results`；`.github/workflows/codeql.yml` 禁止使用 `github/codeql-action`、`actions/checkout` 或其他远端 `uses:`。目录不完整、版本或 hash 漂移直接失败，禁止临时下载或每次重新解压 bundle 替代。PR 仅修改测试源和 BUG 元数据时可以跳过完整 CodeQL analyze，但只要包含业务/运行源码仍必须扫描；`push: main` 继续扫描全部变更语言以维护默认分支 Security/Code Scanning 基线。当前 security 角色只有一个 runner，因此 changed-language 分类、所需语言的顺序分析、SARIF 上传和稳定 `CodeQL verdict` 必须复用同一个 job/runner allocation；禁止用 matrix 或独立 verdict job 反复释放并重新争抢该 runner。任一语言失败仍使同一稳定 verdict fail-closed，test-only/no-language 快速路径仍由该 job 发布成功 no-op 判定。

其他确需 JavaScript Action 的 workflow 仅允许使用经 runner 实跑验证的原生 Node 24 主版本：`actions/checkout@v7`、`actions/upload-artifact@v7`、`actions/download-artifact@v8`、`actions/github-script@v9`；不得为了消除告警依赖 runner 的强制 Node 版本兼容层。版本升级必须同步 machine-policy evidence 与合同测试，但不得把这些远端 Action 重新引入 CodeQL 或 Code Intelligence 快速路径。

main 必须启用“通过 PR 合入”的分支保护，并与自动合入器使用同一组 change-scoped 质量判定。PR Quality 与 Semgrep 的实际强制检查必须作为唯一普通 runner `CI verdict` job 内的 fail-closed steps 执行；其独立 workflow 只保留手工诊断入口，不得在每个 PR 重复 checkout、分类或获取普通 runner。自动合入器要求稳定 `CI verdict` 与隔离的 `CodeQL verdict`，不得再等待已被 CI verdict 汇总的重复上下文。CodeQL 必须同时保留 source/mixed PR 扫描和 `push: main` 扫描，因为后者负责更新默认分支 Security/Code Scanning 基线。仅修改 BUG registry JSON 的官方 close-sync PR 只运行 GitHub-hosted stdlib metadata `CI verdict`；PR Quality、Semgrep 手工 workflow 不启动，CodeQL job 在 runner 分配前精确跳过。任一非 registry 文件、非官方 close-sync 分支或 metadata 校验失败都不得进入轻量路径。定时或手工安全扫描以及 CodeQL `push: main` 不受该 PR 例外影响。

Nox、测试 helper 和脚本内部同样受零安装约束：GitHub Actions 或 `AISTOCK_CI_INSTALL_FORBIDDEN=1` 下发现依赖缺失必须直接失败，禁止在 helper 内自动执行安装。CI policy scanner 同时检查 workflow YAML、Windows `AIstock-CI` runner/环境/DEV-DB 路由标记和 Nox 隐式安装边界；仅做关键词扫描不得宣称完整合同通过。

PR CI 的评论、完整 artifact 上传和其他报告发布器属于非阻断诊断，不得要求 runner 在 job 初始化阶段下载额外 action archive。分类、静态检查、测试、UI 验收和 workflow policy 的真实质量结果直接写入原 job summary/stdout 和 commit-bound PR receipt；临时文件留在 job workspace，不以 `upload-artifact`/`download-artifact` 作为 PR 合入前置依赖。default-branch 正式故障登记、已批准的发布资产和独立运维备份不受此限制，但其发布失败不得覆盖测试结论，也不得延迟 required `CI verdict`。

Windows runner 的 workflow shell 必须显式解析到 Git Bash（禁止落到 WSL `System32\\bash.exe`）；runner wrapper 可配置只读本地 Git object mirror，并通过 `GIT_ALTERNATE_OBJECT_DIRECTORIES` 复用已验证对象。checkout 仍使用浅深度和当前 PR base SHA；仅在 mirror 缺少对象时远程补取。mirror、shell 和代理异常属于 runner 基础设施诊断，禁止转化为业务门禁或重复依赖安装。

PR base commit 已在 checkout 中且能够与当前 HEAD 证明 merge-base 时，才可直接更新本地 ref 而不执行远程请求；仅有 base 对象但浅克隆 ancestry 不完整不视为可用。确需补取时，仅对 pinned base ref 和当前 PR checkout/head ref 使用固定 3 次、短退避、有限 `--deepen` 的 bounded retry，每次补取后重新验证 merge-base，并在连续失败后以基础设施错误 fail-closed；禁止为解决该问题改成无界完整历史 checkout。单次 TLS/EOF 不得直接转化为人工重新授权、完整 CI 重跑或第二个 workflow；新 commit 仍由 concurrency 自动取代旧 run。

Nightly 的验证/测试 job 与 PR CI 使用同一预构建 `AIstock-CI` Conda 环境，不得调用生产 `AIstock` 环境代替测试环境；独立 DR 运维 job 仍按其授权目标使用运维环境，不受此替换。一次性 Nightly checkout 需要前端依赖时，只能在 `package-lock.json` SHA-256 一致且锁定的直接 Playwright CLI 存在后，把 canonical 预构建 `frontend/node_modules` 以目录链接挂入 workspace；禁止每轮安装、复制大型依赖树、静默跳过 UI 计划或在锁文件漂移时继续运行。清理一次性 workspace 时只删除链接本身，不得递归删除依赖源。

Scheduled Nightly 必须以显式仓库参数查询最近一次具备完整 `execution-plan.json` 与 `session-results.json` 的已完成定时运行，并分别检查命令退出码、JSON 解析、receipt 与 source HEAD 绑定以及本地 commit 可用性。下一轮 change window 从该 receipt HEAD 前进，只重跑 receipt 中失败或尚未写入结果的已选 session 与新 changed files 映射出的计划；change-scoped `l0` 必须继续使用失败 receipt 的原始文件范围并合并新增范围，通过短路径 scope 文件传递，禁止在干净 checkout 上重新猜测 changed files或把大批路径展开到 Windows 命令行。任一步失败或不存在有效 receipt 时以基础设施错误 fail-closed，不得静默转换为全量执行；`--full-run` 只允许由 `workflow_dispatch` 的 `full_nightly_run=true` 明确触发。自动故障恢复不得因单个长期失败而反复运行已经通过的数十个无关 session。

source/mixed PR merge tree 的 changed-file 分类、静态 L0/catalog、被选中的 backend/frontend/Go/prompt/workflow 验证、workflow policy 与最终 `CI verdict` 必须复用一个 job、一次 checkout、一次 base-ref 准备和一次 AIstock-CI runner allocation；在只有一个普通 runner 的当前配置下，各选中 lane 在该 job 内顺序执行并以 step outcome fail-closed 汇总，禁止拆成会释放后重新争抢同一 runner 的 classifier、matrix/module 或 verdict job。新增或修改且仍存在于 merge tree 的 Python `test_*.py` 必须由其自身选中的至少一个可执行 backend nox plan 通过精确文件、nodeid 或父目录测试目标实际覆盖；classifier 先做静态可达性检查，所选 pytest session 再通过 collection receipt 记录实际收集文件，backend step 在 verdict 前精确比对 changed tests。仅命中 ownership/module、位于休眠分支或未由 runner 展开的 wildcard 而未进入实际 collection 的测试必须以 `unexecuted_test_blocked` 或 `changed_test_plan_coverage` fail-closed，禁止通过改名借用无关测试族制造假绿灯。DEV-DB-only 计划继续作为外部验证单独报告，不伪装为 PR CI 已执行。收据只包含仓库相对测试路径，不上传 artifact、不增加 job/runner allocation。docs-lite 记录同样在该 job 内生成。官方 close-sync 分支在同一动态 job 中切换到 `ubuntu-latest`，只使用系统 Python 标准库、一次 checkout/base-ref 准备和 `bug_registry_metadata_check.py --close-sync-only`，不得获取普通或安全 self-hosted runner，也不得执行环境/依赖安装。任一越界文件或任一选中 lane 失败都必须使稳定 `CI verdict` 失败；DEV DB 计划继续仅报告外部 DEV 验证要求，不在 CI 执行 DDL/DML。

<a id="rule-ci-database-safety-001"></a>
### 6.24 [CI-DATABASE-SAFETY-001] CI 禁止独立数据库，DEV 数据库单独验证

CI workflow 不声明 `services`、启动 PostgreSQL/TimescaleDB 容器、创建临时数据库或执行 DDL/DML。需要数据库的计划必须由 changed-file classifier 标记为 `dev_db_required`，转交现有 DEV 数据库验证 lane；DEV 验证、源码 CI 和生产迁移分别记录。DEV 不可用时状态为 `blocked/pending`，禁止改用独立数据库、SQLite 替代真实契约或静默跳过。只读单元测试可使用进程内 SQLite fixture，但不冒充 DEV/生产数据库验证。

Nightly 的 DR snapshot/restore-readback 属于独立运维 lane，只能连接已存在且已授权的目标并执行既定只读/备份 runbook；不得创建或启动数据库容器，也不得冒充 PR CI 或 DEV 验证。该 lane 必须使用独立名称、权限和 receipt，使 `AIstock` 运维环境与 `AIstock-CI` PR 环境不会被混淆。

<a id="rule-context-budget-001"></a>
### 7.1 [CONTEXT-BUDGET-001] 上下文预算

- T0/T1 默认使用 task card、Context Pack、所有权映射、图摘要和目标代码片段。
- T2/T3 按明确依赖加载设计和跨模块契约。
- 命令输出采用 compact receipt；完整 JSON 只用于失败诊断、状态恢复或持久证据。
- 精确搜索无结果时记录 scoped miss reason，再扩大搜索范围。
- 分支 changed-file 范围以 merge base（如 `origin/main...HEAD`）为准，再合并当前 worktree、index 与未跟踪文件；禁止用会把主线新增文件反向计入任务范围的 two-dot stale-branch diff。

<a id="rule-issue-batch-context-001"></a>
### 7.2 [ISSUE-BATCH-CONTEXT-001] 同模块批处理

同模块、相同风险、相容 scope 和相同验证链的 issue 在安全时可优先使用一个 source batch worktree；不批处理时记录简短 split reason。Batch Context Pack 记录 issue 列表、共享文件、逐 issue 验收、提交映射、共享测试和拆分条件。

source batch 与 close-sync batch 是同一批次的两个阶段：source batch 允许多个 BUG 共用一个源 PR，但必须保持同模块、同风险、同验证链和共享 scope；`close-sync-batch` 只能把这些兼容 BUG 的独立记录同步到同一个已合入 PR/merge identity，并固化每个 BUG 的逐项证据与 compatibility key。不同模块、不同风险、不同 required verification、不同 runtime impact/activation policy、不同 production/dependency gate 或已有不同源 PR 的 BUG 不得通过 `close-sync-batch` 共享一个源身份。`backend_restart_required=true` 或需要 post-restart identity/business-smoke receipt 的 BUG 始终单独走 `finish`/`close-sync`；`none`/`client` 等无后端重启的 BUG 只有在 compatibility signature 完全一致时才可批处理。该规则是效率优化，不减少逐 BUG 的 Issue、状态、证据和门禁。

<a id="rule-issue-close-sync-aggregate-001"></a>
### 7.2.1 [ISSUE-CLOSE-SYNC-AGGREGATE-001] 独立源 PR 的元数据聚合善后

当 merge-aftercare owner 已经同时持有至少两个独立合入、且无需 backend restart 或 post-restart receipt 的 BUG 时，应使用 `close-sync-aggregate` 在一个 registry-only PR 中持久化善后元数据；不得为了凑批次延迟单个已就绪 BUG。这不是 source batch，也不得把一个 BUG 的源身份或验证收据投射到其他 BUG。`--apply` 必须在创建聚合工作树、修改 BUG JSON 或同步外部 Issue 状态前，逐 BUG 验证 GitHub Issue linkage、独立且已合入的 source PR、该 PR 的 merge commit 和有效 runtime contract；若当前 BUG JSON 尚未持久化 `pr_url` 或 validation evidence，调用方必须提供精确的 `--source-pr BUG-ID=PR-URL`，且证据只能由该 source PR 中与其 head commit 绑定的 durable validation receipt 补足。任一项失败则整组 fail-closed。聚合写入必须逐 BUG 保留各自的 `pr_url`、`fix_commit`、验证证据、生产门禁和 Issue 同步结果，禁止使用一个全局 `--pr-url`、全局 merge commit 或全局 validation receipt 覆盖整组。共享 source PR 的兼容 BUG 继续使用 `close-sync-batch`；runtime/restart-pending BUG 继续单独完成运行态验证与 close-sync，不得进入 aggregate。已有 merge 与 cleanup 授权时，owner 可在同一调用中使用 `--create-registry-worktree --apply --create-pr --merge-close-sync-pr --cleanup --sync-root` 完成 aggregate PR 的合入、root fast-forward 及其自身工作树/分支清理；source 工作树仍按各自授权与安全清单独立清理。聚合只减少重复的 metadata PR、worktree、CI 和 cleanup 周期，不改变 source PR 边界、模块所有权、验证要求或生产授权。

Nightly `full-summary` 在所属 Actions run 结束前，只消费 `needs.*.result` 与本 run 已落盘的 session receipt，禁止再次读取自身仍在运行的 GitHub run/job log；跨 run 的事后诊断才可读取已完成的 Actions 日志。失败 session 先按 test-plan module 形成结构化 failure groups：单一模块组可自动创建或更新一个 Issue；两个及以上模块组属于异构诊断，保留分组证据但禁止自动创建 Issue、分配首个 session 的模块或直接 promotion 为 BUG。后续只对已确认的单一根因组执行登记。

普通 `create-fix-worktree` 登记以 host-wide 原子 counter、精确 permanent reservation、GitHub Issue 和 source PR 内的 BUG JSON 作为编号权威；不得再把仓库单值 `.bug_id_allocator.json` 写入每条并发 fix PR。该 legacy allocator 只由 registry-intake/显式恢复路径维护为兼容观察值，不参与普通 source PR 的 changed files、allowed scope 或合入冲突。编号唯一性、失败恢复和终态 reservation 清理由全局账本继续 fail-closed。

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

- 普通任务直接执行选定 lane 的 `run`、`resume`、`submit-bug` 或对应入口，不把 `doctor` 作为通用前置门禁。仅在客户端/bootstrap 状态未知、workflow/client 代码刚变更、恢复状态 stale/conflict，或用户明确要求诊断时运行一次 `doctor`；失败后按具体检查修复，禁止反复全量 doctor。
- `.codex/**` 或 `.claude/**` 合入后，由一个明确 owner 在 canonical root fast-forward 完成后、close-sync 或 cleanup 开始前，从与 `origin/main` 对齐且客户端权威路径无未提交改动的 canonical `main`，对本次实际变化的 lane 以及同一明确目标 profile 中已检测到的存量 stale lane 一次性执行 `install-client --apply`，随后使用 `verify-clients --workflow-only` 校验 router 与全部目标 lane；`merge-finalizer --sync-root --apply` 自动执行该步骤，全部 hash 无漂移时为 `noop`，不再把存量漂移留给业务窗口逐 lane 阻断，也不要求转交命令或二次授权。canonical root 的无关路径状态不构成客户端门禁，禁止把 task worktree、未合入 commit 或旧 checkout 作为全局客户端安装源，禁止多个窗口无差别重复安装。
- 官方隔离 Codex 或单一客户端目标必须显式传入 `--codex-home <path>`/`--claude-home <path>` 和对应的 `--skip-*`。客户端 profile 只在本次任务明确列入目标时更新。
- 在途任务仅在 `doctor` 报告客户端 stale、窗口恢复或当前 lane 需要新入口时，使用 canonical root 内的 CLI 执行 `verify-clients --workflow-only --selected-lane <lane>` 校验 router 与当前 lane，禁止从旧 task worktree 调用旧版 workflow CLI。校验以已合入的 canonical `main` 为入口权威并分别报告 checkout relation 与 profile status：task checkout 落后、领先或包含未合入入口变更只产生 advisory，禁止把已符合合入权威的客户端判为 stale；router/当前 lane 的 profile drift 才阻断，无关 lane drift 只告警并记录待同步。
- profile drift 阻断时，CLI 必须返回 `request_single_owner_sync`、明确目标 profile 和 canonical-owner command；活动任务窗口禁止自行从 worktree 安装。唯一 owner 完成一次同步后，其他窗口只复验；`continue_without_install` 明确禁止重复安装或重启。
- `install-client --selected-lane <lane>` 只从已合入的 canonical `main` 同步 router 与该 lane；canonical `main` 未与 `origin/main` 对齐，或 `.codex/skills`、`.claude/commands`、workflow CLI 存在未提交改动时 fail closed，无关路径 dirty 不阻断。hash 相同时跳过，并通过跨进程锁、authority identity 复验与 staged replacement 避免并发窗口观察到部分安装。一次目标限定同步失败后停止并报告，禁止循环重装或从不同版本 worktree 相互覆盖。
- 同步完成且 `restart_recommended=false` 时，旧窗口重新读取 router 与当前 lane 后继续；只有 CLI 明确建议重启或客户端 UI 仍加载旧入口时才重启客户端。客户端同步不授权任何后端进程控制。
- 授权以动作和目标为边界，而不是以消息次数为边界。同一条用户指令可明确打包 merge、精确 cleanup targets 和具体 production target/migration；完整授权包在合入后连续执行，无需二次询问，并分别报告结果。裸 PR/branch merge 授权仍只覆盖源码合入和必要 BUG/metadata 同步；禁止据此推导 source cleanup、生产 DDL/DML、依赖安装、runtime/frontend/client 激活、程序控制或任何删除。

## 9. 完成报告

完成报告包含：branch、commit、PR、changed files、直接测试、scope check、委托/nightly 计划、production DDL/dependency gate、merge/close-sync/root-sync/cleanup 状态，以及运行时和 DB 是否发生变化。

“代码已合入”“BUG 已 close-sync”“客户端已同步”“生产依赖已应用”“运行时已激活”分别报告，便于准确判断交付状态。
