# AIstock 项目开发规范 v1.5

> 版本：1.5
> 更新日期：2026-05-23
> 状态：人类可读规范源；本版在 v1.5 基础上补充流程分级、同模块批处理、上下文/提示词预算和生产依赖门禁要求
> 当前文件：`docs/standards/aistock_development_standard_v1.5_20260523.md`
> 同步机器版本：`docs/standards/aistock_development_standard_v1.5_20260523.yaml`
> 历史归档目录：`docs/standards/archive/`

## 1. 文档定位

本文是 AIstock 的唯一项目级开发规范源。AIstock 不再拆分“Python 开发规范”和“量化/交易开发规范”，因为本项目的 Python、数据、前端、实验、回测、Paper Trading、未来实盘前置验证高度耦合，普通工程规范必须和量化业务红线一起执行。

规范文件体系如下：

| 文件 | 角色 | 权威性 | 说明 |
|---|---|---:|---|
| `docs/standards/aistock_development_standard_v1.5_20260523.md` | 人类可读规范源 | 最高 | 定义必须遵守的项目开发、量化、测试和提交流程。 |
| `docs/standards/aistock_development_standard_v1.5_20260523.yaml` | 机器可读同步版本 | 派生 | 由本文抽取规则，供 guardrail scanner、nox、Validation Center、Codex/Claude Code 使用。 |
| `docs/architecture/aistock_development_standards_and_guardrails_20260504.md` | 落地设计 | 低于规范源 | 说明如何把规范接入 baseline、changed-files gate、agent 修复和验证中心。 |
| `docs/codex_project_memory.md` | Codex 项目记忆 | 运行约束 | 记录当前规范位置、执行要求和阶段进展。 |

如出现冲突，以本文为准；机器 YAML 和架构设计必须在同一提交中向本文对齐。

## 2. 版本、归档和变更规则

1. `docs/standards/` 根目录只保留当前选定生效的人类规范 MD 和对应机器 YAML。
2. 历史版本必须移动到 `docs/standards/archive/`，不得在根目录堆叠多个旧版规范。
3. 文件名必须包含版本号和更新日期：`aistock_development_standard_vX.Y_YYYYMMDD.md`。
4. 每次修改规范必须创建新版本文件，不直接覆盖历史版本；例如从 `v1.0_20260504` 升级为 `v1.1_20260510`。
5. 机器 YAML 必须与人类规范同版本、同日期、同提交更新。
6. 历史版本只允许补充“已归档说明”或修正明显错别字，不允许改变历史规范含义。
7. 任何新增 P0/P1 红线必须同步增加机器规则，或在 YAML 中明确标注 `manual_review_only` 及原因。
8. 规范变更本身需要通过 L0 文档读取、YAML 解析、规则引用一致性和 guardrail smoke 验证。
9. 已完成的详细设计方案、设计文档、实施方案、评审材料和类似 durable deliverable，在验证通过后应自动提交并推送到 `origin/main`，除非用户明确要求暂不提交、暂不合入或必须走独立开发分支。

## 3. 适用范围

本规范适用于 AIstock 全仓：

- FastAPI backend：`backend/routers`、`backend/services`、`backend/data_service`、`backend/db`、`backend/infra`。
- Next.js frontend：`frontend/src`、`frontend/tests`。
- 数据与导出：Tushare、本地 DB、Qlib H5/Bin、因子库、模型库、HMM、QMT/TDX 接入。
- QE / RD-Agent / 自动演进 / 自定义演进 / 实验归档 / 数仓。
- Paper Trading v2、Selection Center、StrategyPackage、未来实盘前置验证。
- 脚本、测试流水线、nox、Codex/Claude Code agent 工作流。
- 文档、设计方案、分析报告、测试证据和发布记录。

## 4. 风险等级

| 等级 | 含义 | 新增/修改代码处理 | 历史代码处理 |
|---|---|---|---|
| P0 | 可能导致错误交易、错误回测结论、资产破坏、生产干扰、数据不可追溯 | 阻断提交，必须修复或明确用户授权 | 建立 backlog，优先治理；若影响当前功能必须先隔离 |
| P1 | 影响可复现、可维护、数据完整、DB 可读性或长期稳定性 | 默认阻断；特殊情况需有例外记录和到期时间 | 分模块治理，新增代码不得扩大 |
| P2 | 可维护性、性能、UI 可读性、覆盖率不足等质量风险 | 允许 warning 或按模块阻断 | 纳入质量趋势和重构计划 |
| P3 | 命名、注释、局部风格等低风险问题 | 建议修复 | 逐步改进 |

## 5. 最高优先级红线

以下红线适用于所有代码和脚本：

1. 不得直接或间接干扰生产端口 `8001`，除非用户明确要求生产重启或生产操作。
2. Windows 侧 backend 不得直接读取 WSL 或远端 worker workspace 文件；必须通过 API 或 AIstock-owned artifact store。
3. 不得用空数组、默认值、`None`、`True`、默认价格、默认资金、默认持仓伪装成功。
4. 不得在交易、Paper Trading、QE 回测、HMM、执行算法中静默降级业务逻辑。
5. 不得修改 StrategyPackage frozen manifest、模型权重、HMM snapshot、QE/RD-Agent artifact、Paper ledger 等受保护资产，除非任务明确要求并有验证证据。
6. 新增 DB 表和字段必须有 PostgreSQL comment，说明业务含义、单位、来源和质量语义。
7. QE 实验和 loop 进入 `complete` 状态前，必须能够说明 required sections 是否完整；不完整必须是 `partial/failed`。
8. 所有可测试功能在提交前必须经过对应自动化测试流水线验证。
9. 设计方案必须放在 `docs/architecture/`；规范必须放在 `docs/standards/`；分析证据必须放在 `docs/analysis/` 或 `tests/aistock_validation/history/`。
10. 不得未经用户明确批准交付“简化版 / 子集版 / POC 版 / 占位版”并声称满足完整需求。
11. 任何基于设计方案、实施方案、Issue 验收标准或用户明确需求的开发，合入或宣称完成前必须逐条对照原方案复核，形成“设计条款 -> 实现位置 -> 测试/截图/API/DB 证据 -> 结论”的验收矩阵；缺项必须标记未完成并停止合入。
12. 不得污染项目根目录；一次性测试、诊断、排查脚本必须放在 `debug_tools/`，不得放根目录或正式 `scripts/`。
13. 正式业务脚本和一次性诊断脚本必须分离；`debug_tools/` 不得被生产服务、定时任务或正式 API 依赖。
14. 所有脚本、诊断工具、数据处理流程都不得静默报错；即使是一次性脚本，也必须输出错误上下文并用非零退出码表示失败。
15. 大数据处理、回测、因子计算、分钟线处理、模型训练不得无边界加载或持有全量数据；必须有分块、范围限制、内存评估或明确容量说明。
16. 不得引入明显会导致内存暴涨、资源不释放、行数爆炸或不可控 O(N²/O(N³)) 复杂度的实现。
17. 已完成的详细设计方案必须包含严格测试用例、测试方案、结果数据验证方式和可合入 Main 的验收标准；设计交付本身验证通过后应自动提交并推送 `origin/main`，但代码实现仍按功能开发流程走独立分支、自动化流水线和用户确认。
18. 新增 BUG / Issue 必须在同一流程同步 GitHub Issue 与本地 BUG JSON；不得把缺少 `github_issue_number` / `github_issue_url` 的新 BUG JSON 提交进 main。

## 6. 机器可读规则映射

本节中的规则 ID 必须与 `docs/standards/aistock_development_standard_v1.5_20260523.yaml` 保持一致。

<a id="rule-arch-wsl-001"></a>
### 6.1 [ARCH-WSL-001] 禁止 Windows 侧直接访问 WSL/远端 workspace

- 严重等级：P0。
- 禁止范围：backend、frontend、scripts、nox 中的运行时代码。
- 禁止写法：`\\wsl$`、`\\wsl.localhost`、`/mnt/f/...`、直接拼 worker workspace 路径读取产物。
- 正确方式：worker API、AIstock-owned artifact store、已入库 payload、带 manifest 的 artifact URI。
- 例外：仅限文档说明、测试 fixture、明确的 worker 侧脚本；例外必须在规则中排除。

<a id="rule-prod-port-001"></a>
### 6.2 [PROD-PORT-001] 禁止测试和工具重启/杀死生产 backend 8001

- 严重等级：P0。
- 开发和验证必须使用 backend `8011/8012`、frontend `3011/3012`。
- 测试脚本不得包含针对 `8001` 的 kill、restart、uvicorn 启动、Stop-Process、taskkill。
- 如确需生产重启，只能由用户明确授权并单独执行。

<a id="rule-err-fallback-001"></a>
### 6.3 [ERR-FALLBACK-001] 禁止 broad exception 伪装成功

禁止：

```python
try:
    run_business_job()
except Exception:
    return []
```

要求：

- 捕获异常后必须返回结构化错误、记录 `partial/failed`、或重新抛出业务异常。
- Parser 可以汇总错误，但最终状态必须明确，不能静默丢字段。
- UI 必须显示失败原因或缺失字段，不得把空值展示为完整成功。

<a id="rule-trading-fallback-001"></a>
### 6.4 [TRADING-FALLBACK-001] 禁止交易/回测/HMM 静默降级

- Paper Trading v2 缺分钟线、pre_close、limit、suspend、Torch/context、HMM coefficient 时必须 fail-fast。
- V25 或分钟执行算法不得自动降级为 TWAP 或日频回测。
- HMM 缺 coefficient 不得使用中性系数伪装成功。
- Qlib execution price basis 不匹配时不得默认 `factor=1`。
- 如有降级模式，必须显式配置、UI 可见、日志可审计、测试覆盖，并确认不会改变业务结论。

<a id="rule-config-hardcode-001"></a>
### 6.5 [CONFIG-HARDCODE-001] 禁止运行时代码硬编码路径和密钥

- 路径、端口、worker 地址、artifact root、数据库连接、API key 必须来自配置、环境变量、DB catalog、manifest 或用户请求。
- 禁止硬编码 `F:\Dev\AIstock`、`C:\Users\...`、生产端口、token、password、secret。
- 示例、文档、测试 fixture 可以出现路径样例，但不得进入运行时默认值。

<a id="rule-qe-artifact-001"></a>
### 6.6 [QE-ARTIFACT-001] QE artifact 必须有 manifest

QE/RD-Agent/Qlib 产物不得只保存文件路径。manifest 至少应包含：

- `artifact_id`、`artifact_type`、`uri`、`storage_tier`。
- `sha256`、`size_bytes`、`row_count`、`schema_version`。
- `created_at`、`source_task_id`、`source_loop_index`、`producer`。
- `quality_status`、`missing_sections`、`cleanup_eligible`。

数仓归档不得依赖 QE runtime DB 或 worker workspace 在未来仍然存在。

<a id="rule-ui-rawjson-001"></a>
### 6.7 [UI-RAWJSON-001] 操作员 UI 不得以 raw JSON 作为主要业务视图

- UI 应使用中文业务标签、表格、卡片、图表、错误态和缺失原因。
- JSON 可以作为高级调试抽屉，但不能作为普通操作员的主要界面。
- 按钮 disabled 必须展示原因；后端错误必须透传为可读提示。

<a id="rule-resource-timeout-001"></a>
### 6.8 [RESOURCE-TIMEOUT-001] 外部调用和子进程必须有 timeout/生命周期控制

- HTTP 请求、subprocess、DB 长查询、文件批处理应有 timeout、取消、日志路径、退出码和资源释放。
- 长任务必须有 heartbeat、状态持久化和幂等执行策略。
- 大文件和大 DataFrame 必须采用 chunk/batch 或明确内存边界。

<a id="rule-db-comment-001"></a>
### 6.9 [DB-COMMENT-001] 新增 DB 表和字段必须有 comment

- 每张新表必须有 `COMMENT ON TABLE`。
- 每个新字段必须有 `COMMENT ON COLUMN`。
- comment 应包含业务语义、单位、来源、可空含义、质量语义或枚举含义。
- JSONB 字段 comment 必须说明 schema/version/source/quality 约束。
- DDL 不得由业务 service 隐式执行。

<a id="rule-root-pollution-001"></a>
### 6.10 [ROOT-POLLUTION-001] 禁止污染项目根目录

- 严重等级：P1。
- 根目录只允许稳定入口、顶层配置和明确归属的一级目录。
- 禁止在根目录新增一次性 `.py`、`.md`、`.json`、`.csv`、`.log`、`.txt`、`.pkl`、`.parquet`、`.zip` 等临时文件。
- 根目录新增文件必须说明为什么不能放入现有目录，并经过 review；稳定入口文件如 `AGENTS.override.md`、`README.md`、包管理或构建配置可作为明确归属的顶层文件维护。

<a id="rule-script-location-001"></a>
### 6.11 [SCRIPT-LOCATION-001] 一次性测试和诊断脚本必须放入 debug_tools

- 严重等级：P1。
- `F:\Dev\AIstock\debug_tools` 是一次性测试、诊断、排查、临时研究脚本的统一目录。
- `debug_tools/<module>/<date_or_issue>/` 用于保存可复现的临时诊断上下文。
- 正式业务可复用脚本应放在 `scripts/`；如果临时脚本被第二次复用，必须评估迁移到 `scripts/` 或 `backend/services/` 并补测试。
- `debug_tools/` 不得被生产服务、scheduler、正式 API、nox release gate 直接依赖。

<a id="rule-doc-location-001"></a>
### 6.12 [DOC-LOCATION-001] 文档必须按类型归属

- 严重等级：P1。
- 项目规范放 `docs/standards/`；历史规范放 `docs/standards/archive/`。
- 架构设计、顶层方案、实施方案放 `docs/architecture/`。
- 分析报告、实验结论、研究笔记放 `docs/analysis/`。
- 运维操作手册放 `docs/operations/`；用户使用说明放 `docs/user_guides/`；发布说明放 `docs/releases/`。
- 同一主题必须保留一个主文档，其他文档只引用，不得多处复制维护同一规范或结论。

<a id="rule-memory-dataframe-001"></a>
### 6.13 [MEMORY-DATAFRAME-001] 大 DataFrame 和大文件处理必须有内存边界

- 严重等级：P1。
- 大 CSV/parquet/pickle/H5/Qlib 数据读取必须支持 chunk、date range、symbol range、columns projection 或明确容量评估。
- 禁止在循环中不断 `pd.concat` 大 DataFrame；优先 list 收集后一次 concat、分批写入 parquet/DB 或流式处理。
- 禁止无上限全局 cache；cache 必须有 max size、TTL、clear 或生命周期说明。
- 长任务必须记录输入规模、行数、关键阶段耗时和必要的内存风险说明。

<a id="rule-algo-complexity-001"></a>
### 6.14 [ALGO-COMPLEXITY-001] 算法复杂度和 join 风险必须受控

- 严重等级：P2，涉及交易、回测、数仓、分钟线时按 P1 处理。
- 股票 × 日期 × 分钟 × 因子等多维循环必须评估复杂度。
- 大表 merge/join/groupby/sort 必须说明 key 唯一性、行数上界和 row explosion 风险。
- 全市场分钟级计算优先使用向量化、分块、预聚合或数据库侧过滤。

<a id="rule-debug-failfast-001"></a>
### 6.15 [DEBUG-FAILFAST-001] 诊断脚本也必须 fail-fast

- 严重等级：P1。
- 一次性脚本可以轻量，但不得 `except Exception: pass` 或失败后继续输出成功。
- 诊断脚本失败必须输出错误上下文、输入参数摘要、建议复现命令，并以非零退出码结束。
- destructive 或写入型诊断脚本必须支持 dry-run 和确认文本。

<a id="rule-issue-github-sync-001"></a>
### 6.16 [ISSUE-GITHUB-SYNC-001] Issue 创建必须同步 GitHub

- 严重等级：P1。
- 适用范围：Validation Center、MCP、Codex/Claude Code、脚本和人工流程创建的所有正式 BUG / Issue。
- 标准入口：正式 BUG / Issue 必须通过 Validation MCP 创建，由流水线统一分配 `BUG-NNN`。
- 创建结果：MCP 同步创建或同步 GitHub Issue，并在 BUG JSON 回填 `github_issue_number`、`github_issue_url`。
- Registry 镜像：正式 BUG JSON 保存到 `tests/aistock_validation/bugs/*.json` 并提交 Git；未确认问题只保留在 `tmp/` 或 `debug_tools/` 草稿区。
- 流程边界：创建 BUG 只登记 issue，不创建修复 worktree；认领修复时再创建独立 worktree/branch 或 batch worktree/branch。
- 历史遗留未链接或重号 BUG 通过专门 cleanup/backfill 分支处理，不作为新增 issue 的例外。

## 7. 项目目录结构和文件归属规范

### 7.1 根目录允许范围

项目根目录只允许放稳定入口和顶层配置，例如：

- 一级业务目录：`backend/`、`frontend/`、`docs/`、`scripts/`、`tests/`、`configs/`、`monitoring/`、`debug_tools/`、`.codex/`。
- 顶层入口和配置：`README.md`、`pyproject.toml`、`noxfile.py`、`.gitignore`、`.gitattributes`、`package`/workspace 类配置。
- 经 review 确认必须在根目录的构建、工具或版本文件。

禁止在根目录新增一次性脚本、临时数据、诊断报告、压缩包、日志、模型、pickle、parquet、CSV、JSON 输出。历史根目录污染作为技术债分批清理，不在普通功能提交中混入大规模搬迁。

### 7.2 脚本目录归属

| 类型 | 目录 | 要求 |
|---|---|---|
| 一次性诊断/排查/实验脚本 | `debug_tools/<module>/<date_or_issue>/` | 默认不被生产依赖；需要复现说明、输入参数、失败退出码。 |
| 可复用业务脚本 | `scripts/` | 必须参数化、可测试、支持 dry-run 或确认文本。 |
| 后端业务逻辑 | `backend/services/` | 不得以脚本形式绕过 service/repository 分层。 |
| DB schema/migration | `backend/db/`、`backend/migrations/` | 必须有 comment、幂等或明确应用记录。 |
| 自动化测试 | `backend/tests/`、`frontend/tests/`、`tests/aistock_validation/` | 必须可由 pytest/nox/Playwright 调度。 |
| 临时输出 | `tmp/` 或 `debug_tools/.../outputs/` | 默认不提交，提交时必须说明复现价值和大小。 |

### 7.3 debug_tools 生命周期

- `debug_tools/` 用于人和 agent 保存一次性诊断脚本、调试输入样例和复现说明。
- 目录建议：`debug_tools/qe/`、`debug_tools/hmm/`、`debug_tools/data/`、`debug_tools/paper_v2/`、`debug_tools/frontend/`、`debug_tools/research/`。
- 每个有保留价值的诊断目录应包含 `README.md` 或脚本 header，说明目的、输入、输出、是否安全、是否只读。
- 诊断脚本不得被正式 API、scheduler、生产服务或 release gate 依赖；若需要正式依赖，必须迁移到 `scripts/` 或业务 service 并补测试。

### 7.4 文档目录归属

| 文档类型 | 目录 |
|---|---|
| 项目规范 | `docs/standards/` |
| 历史规范 | `docs/standards/archive/` |
| 架构设计 / 顶层方案 / 实施方案 | `docs/architecture/` |
| 分析报告 / 调查报告 / 实验结论 / 研究笔记 | `docs/analysis/` |
| 运维操作手册 | `docs/operations/` |
| 用户使用说明 | `docs/user_guides/` |
| 发布说明 / release candidate | `docs/releases/` |
| 测试矩阵 | `tests/aistock_validation/modules/` |
| 测试执行证据 | `tests/aistock_validation/history/` |

同一主题应有一个主文档，其他文档只引用主文档并记录差异，不得在多个文件维护同一规范正文。

## 8. Python 后端工程规范

### 8.1 分层边界

- router 只负责 API contract、确认文本、参数校验、HTTP 错误映射。
- service 负责业务编排和状态转换。
- repository/data access 负责持久化和查询，不承载业务决策。
- schema/bootstrap/migration 管理 DDL，业务请求不得偷偷建表或改 schema。
- 高风险服务必须支持依赖注入，便于测试替换 DB、HTTP client、clock、worker client。

### 8.2 类型和数据结构

- 新增 API request/response 使用 Pydantic model 或明确 TypedDict/dataclass。
- 核心业务不得长期传递无 schema 的大 dict。
- JSONB payload 必须带 `schema_version`、`source`、`quality_status`。
- 时间字段必须说明时区和口径；金额字段必须说明币种；收益率字段必须说明是否百分比。
- 函数、变量、模块使用 `snake_case`；类使用 `PascalCase`；常量使用 `UPPER_SNAKE_CASE`。

### 8.3 错误处理

- 业务失败必须 fail-fast 或进入可审计的 `partial/failed` 状态。
- catch broad exception 必须带上下文日志和结构化错误。
- 不得因环境错误改变业务语义，例如缺分钟线改用日频、缺模型改用缓存、缺因子改用 0。
- 错误信息应包含：错误码、触发条件、输入摘要、缺失字段、建议修复方式。
- 所有 CLI、测试脚本、诊断脚本都必须遵守 fail-fast；诊断脚本可以打印更多上下文，但不能吞异常后输出成功。

### 8.4 配置和环境

- 配置优先级必须清晰：显式请求 > 环境变量 > DB catalog > 默认配置。
- 默认值进入 QE/Paper/回测执行前必须展开成 effective config 并持久化。
- 新增配置必须说明有效范围、单位、默认来源和是否会影响历史结果解释。

### 8.5 资源和并发

- 子进程必须记录命令摘要、环境白名单、日志路径、退出码。
- 长任务必须支持 timeout、cancel、heartbeat、状态持久化和幂等重试。
- 全局 cache 必须有 max size、TTL、clear 或生命周期说明。
- 后台 scheduler 默认不得在开发端口推进生产可见状态，除非明确 feature flag 开启。

## 9. 内存、资源和算法质量规范

### 9.1 大数据读取和 DataFrame 处理

- `pd.read_csv`、`pd.read_parquet`、pickle/H5/Qlib 读取必须优先指定列、日期范围、股票范围或 chunk。
- 不允许无说明地一次性加载全市场分钟线、全量因子矩阵、全量交易明细或大模型中间结果。
- 循环中禁止反复 `pd.concat` 大 DataFrame；使用 list 收集后一次 concat，或分批落盘/入库。
- 大 DataFrame merge 前必须确认 join key、重复率、预期行数和可能的 row explosion。
- 长任务应记录 row_count、symbol_count、date range、column_count、关键阶段耗时，必要时记录内存估算。

### 9.2 资源释放

- 文件、DB cursor、HTTP session、subprocess、临时目录必须有上下文管理或显式释放。
- subprocess 必须捕获退出码和 stderr/stdout 摘要；失败不得继续输出成功。
- 长任务必须支持 timeout、cancel、heartbeat 和幂等重试。
- 删除临时文件必须限定在明确 workspace/tmp/debug_tools 输出目录内，禁止递归删除动态拼出的未校验路径。

### 9.3 算法复杂度

- 回测、因子、相关性、持仓、交易、分钟线处理必须避免无必要的全市场 O(N²/O(N³)) 循环。
- 股票 × 日期 × 分钟 × 因子级别计算必须给出规模估算和批处理策略。
- 优先使用向量化、数据库过滤、预聚合、增量计算、分区 parquet 或批量写入。
- 如果为了诊断临时使用低效算法，必须放在 `debug_tools/` 并标注不可进入正式链路。

## 10. 数据库和数据管线规范

1. DB schema 变更必须有 migration/bootstrap 文件和测试。
2. 新表/字段必须有 comment，并由测试或 smoke 检查覆盖。
3. 数据管线必须记录 source、as_of、trade_date、row_count、hash/quality、refresh status。
4. 数据修复必须可审计，不能用默认 0 或空值覆盖真实缺失。
5. Tushare、TDX、QMT、Qlib、HMM 数据必须明确 PIT 口径。
6. 删除或清理数据前必须区分业务资产和临时缓存，受保护资产不得当垃圾删除。
7. 大型明细优先 artifact store + manifest，DB 存结构化索引和分析必要字段。

## 11. QE / RD-Agent / 数仓规范

### 11.1 实验创建阶段必须记录

- experiment/task id、loop index 规划、实验类型、用户说明、创建时间、创建人/agent。
- requested config 和 effective config。
- 模型类型、模型版本、超参数、随机种子、label horizon、数据频率、训练/验证/回测区间。
- 因子列表、因子版本、因子分类、因子独立指标、相关性矩阵版本。
- 策略参数：topk、n_drop、持仓数量、每日换股数量、最短持股时间、初始资金、成本、benchmark、停牌/涨跌停处理。

### 11.2 实验执行阶段必须记录

- worker 节点、开始/结束时间、状态转换、heartbeat、失败原因。
- 训练曲线、valid metric、early stop、best iteration、模型 artifact。
- 特征/因子权重或 attribution；LGB/LSTM 优先实现最小可验证路径，其他模型保留 schema。
- 订单意图、子单、成交、未成交原因、tail-substitute 候选、最终成交方向和金额。
- 成本扣费路径、现金变动、持仓变动和 report cost 对账。

### 11.3 实验完成阶段必须记录

- 期初资产、期末资产、绝对收益、超额收益、with/without cost、最大回撤、Sharpe、turnover、benchmark。
- 收益曲线、回撤曲线、IC/RankIC 曲线、成本曲线。
- position summary、holding audit、stock trades、symbol summary、execution events。
- artifact manifest 和缺失字段说明。
- backtest-only loop 必须记录训练来源：source task、source loop、model artifact、训练指标引用。

### 11.4 数仓独立性

- QE runtime DB 和 worker workspace 未来可以清理，数仓必须独立回答历史实验详情。
- QE DB 可重复保存小型结构化指标，用于 UI 展示、重试、恢复；大型明细不要塞入 QE DB 大 JSON。
- 数仓应保存分析所需结构化指标，并持有 artifact manifest 与必要 parquet/文件资产。
- 历史补录和新增 loop 补录必须通过 API 完成，不依赖手工脚本或 Windows 侧直读 worker 文件。

## 12. Qlib / HMM / 因子和模型规范

- Qlib stock export 对 QE 默认使用 SH/SZ；BJ/BSE 排除策略必须明确。
- feature 数据与可交易股票池 eligibility 必须分离，不能因未来 ST/暂停/退市删除历史事实。
- Qlib adjusted price 与 raw limit/pre_close 比较时必须显式转换口径。
- HMM snapshot、coefficient artifact、model registry 必须可追溯 as_of 和 effective date。
- 因子入库必须有版本、分类、独立指标、相关性信息和数据覆盖质量。
- 模型库必须记录模型类型、训练数据、超参数、artifact、指标和可复现配置。
- 因子相关性、归因、模型特征权重分析必须记录样本窗口、股票池、缺失率和截面覆盖率，避免基于不完整样本形成错误结论。

## 13. Paper Trading v2 / Selection Center / StrategyPackage 规范

- StrategyPackage frozen manifest 和 manifest hash 不得静默修改。
- Selection Center live/paper selection 不得直接使用 QE 回测 pred.pkl 作为权威信号。
- Paper Trading v2 日运行必须校验交易日历、suspend_d、stk_limit、minute bars、pre_close、limit、模型上下文。
- 缺少关键市场数据或模型上下文必须 fail-fast。
- 回放、catch-up、live session 必须有独立状态、session lock、幂等 run、cancel/timeout。
- 现金、持仓、订单、成交、NAV、错误事件必须持久化并可对账。

## 14. 前端 UI 规范

- UI 必须调用真实 backend 能力，不得用 mock/静态成功伪装完整功能。
- 普通操作界面应展示中文业务标签、状态、指标、图表和缺失原因。
- raw JSON 只能放高级调试区，默认不作为主要视图。
- 按钮 disabled 必须有可读原因；失败必须展示后端错误码和修复建议。
- UI E2E 必须监听 pageerror、console error、requestfailed 和非预期 4xx/5xx。
- 前端验证使用 3011/3012，不得影响生产前端或后端。

## 15. 测试、覆盖率和提交规范

### 15.1 设计阶段测试要求

每个新功能设计文档必须包含：

- 业务目标和 false-success 风险。
- L0/L1/L2/L3/L4/L5 测试范围。
- API/DB/UI/log/business oracle 验证方式。
- 长运行任务的 nightly/后台验证策略。
- 覆盖率目标和不适合自动化的人工确认项。

### 15.2 提交前最低验证

新增或修改代码提交前至少执行：

```powershell
python -m compileall <changed-python-paths>
python -m pytest <targeted-tests> -q -p no:cacheprovider
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
```

涉及 UI/API/DB/QE/Paper/HMM/数据管线的变更，还必须执行对应 nox session、API/DB smoke、UI E2E 或 run evidence。

<a id="rule-design-compliance-001"></a>
### 15.3 [DESIGN-COMPLIANCE-001] 设计方案完整实现复核和禁止简化交付

- 严重等级：P0。
- 适用范围：所有基于已批准设计方案、实施方案、Issue 验收标准、用户明确需求的新功能、修复、UI、数据、MCP、数仓、QE/RP/Paper/HMM 工作流开发。
- 禁止行为：未经用户明确批准，不得将简化版、子集版、POC 版、占位版、mock-only 版本、只实现后端不实现 UI、只实现 mock 页面不接真实 API、只实现实验级而遗漏 loop 级、只实现只读标识而遗漏操作按钮等交付描述为“完成”“可合入”或“符合设计”。
- 阻塞处理：如果设计中的任一功能无法在当前分支完整实现，必须立即停止并向用户报告阻塞、影响范围和需要调整的设计条款；不得自行降级、删减、隐藏或把剩余工作放入未声明的“后续”。
- 合入前复核：实现完成后必须逐条对照设计文档、Issue `closure_requirements`、用户明确验收点和 UI/API/DB 行为，产出设计验收矩阵，至少包含 `design_item`、`implementation_refs`、`test_or_evidence`、`status`、`gap_or_exception`。
- 验证要求：涉及 UI 的条款必须提供真实页面或 E2E/截图证据；涉及 API/DB/数仓/MCP 的条款必须提供真实接口、DB side effect、run record 或受控 smoke 证据；mock 测试只能补充交互证明，不能替代真实业务路径验收。
- 流水线边界：L0/L1/L2/L3/L4/L5 或 CI 通过不等于设计完整实现；如果流水线没有覆盖设计条款，最终报告必须标记为 `validation_gap`，不得声称完整完成。
- 报告要求：PR、最终汇报和 Validation Center 记录必须列出未实现、未验证、需要用户授权裁剪的条款；只要存在未获用户批准的缺项，不能请求合入 Main，不能关闭 Issue，不能标记 `verified`。


<a id="rule-design-main-001"></a>
### 15.4 [DESIGN-MAIN-001] 详细设计方案交付和 Main 合入规则

- 严重等级：P1。
- 适用范围：详细设计方案、设计文档、实施方案、评审材料、长期方案、测试方案、验收标准等 durable deliverable。
- 完成标准：文档必须落到批准目录，包含背景、范围、现状差距、目标架构、失败模式、测试用例、测试方案、数据验证方式、验收标准和后续实施边界。
- 自动提交要求：设计交付完成且验证通过后，应在当前任务中提交并推送到 `origin/main`，避免仓库长期存在未提交设计文件影响其它模块开发。
- 例外条件：用户明确要求暂不提交、暂不合入、保留在独立分支，或设计文档绑定未完成代码实现且存在生产风险。例外必须在最终报告中说明。
- 边界：本规则只覆盖文档/设计交付。运行时代码、DB migration、调度器、生产数据修复、策略资产修改仍必须走独立开发分支、自动化流水线、用户确认后再合入 Main。

### 15.5 覆盖率目标

- 新增/修改 Python 代码 line coverage 目标 >= 80%。
- 新增/修改 Python 代码 branch coverage 目标 >= 70%。
- QE 数据完整性、warehouse/archive、交易执行、成本/ledger、HMM、cleanup gate 必须有 L1/L2 和业务 oracle。
- 长回测/长实验可先做 fast gate，再进入 nightly/L4/L5；未通过前不得标记生产可用。

## 16. Agent 开发和 Bug 修复规范

- Codex/Claude Code 必须先读取项目记忆和相关设计/规范。
- Codex/Claude Code 在报告完成、提交 PR、合入 Main 或标记 issue fixed/verified 前，必须执行 DESIGN-COMPLIANCE-001 对照复核；不得自行交付简化版、子集版、POC 版或占位版。
- 若无法完整实现设计，Agent 必须停止并请求用户确认范围调整，不能用“先做最小版”“后续补齐”替代已批准设计。
- 只修改当前任务相关文件，不处理其他窗口的 dirty workspace。
- 不得回滚用户或其他工具修改。
- 修复 bug 必须先复现，再修改，再跑失败用例和周边回归。
- Bug 记录应包含描述、触发条件、严重等级、复现命令、疑似文件、修复 commit、验证记录。
- Agent 可读取机器 YAML 和 Validation Center 只读上下文；不得直接执行任意 SQL/shell 或读取 worker workspace。
- 若规则与任务冲突，必须向用户说明并取得明确授权。
- Agent 临时诊断脚本必须放在 `debug_tools/`，不得在根目录创建临时脚本或输出。
- Agent 发现有复用价值的诊断脚本时，应提出迁移到 `scripts/` 或测试目录，并补充参数化和回归测试。

## 17. 文档归属规范

| 类型 | 目录 |
|---|---|
| 项目规范 | `docs/standards/` |
| 历史规范版本 | `docs/standards/archive/` |
| 架构和设计方案 | `docs/architecture/` |
| 分析报告和调查结论 | `docs/analysis/` |
| 运维操作手册 | `docs/operations/` |
| 用户使用说明 | `docs/user_guides/` |
| 发布说明 / release candidate | `docs/releases/` |
| 自动化验证历史记录 | `tests/aistock_validation/history/` |
| 测试矩阵和测试模块说明 | `tests/aistock_validation/modules/` |
| 临时运行产物 | `tmp/` 或被 `.gitignore` 排除的运行目录 |

规范不得散落在 `docs/architecture`；架构设计也不得复制粘贴规范正文，只能引用本规范。

## 18. 历史代码治理策略

AIstock 经历过多工具探索式开发，历史代码存在大量不符合规范的情况。治理原则：

1. 不做一次性全仓大重排，避免引入行为风险。
2. baseline findings 作为历史技术债，不等于立即全部阻断。
3. changed-files 中新增 P0/P1 必须优先阻断。
4. 高风险模块先治理：QE、Paper Trading v2、Selection Center、HMM、Qlib 数据、DB schema、资产清理。
5. 每次治理必须有回归测试和 run evidence。
6. 历史例外必须有 owner、原因、到期时间和复查计划。
7. 根目录污染、一次性脚本散落、分析文档错放是单独治理项，迁移时不得混入业务行为修改。

## 19. 当前第一阶段暂不做

- 不对全仓历史代码一次性 Black 格式化。
- 不强制历史代码立刻补满类型标注。
- 不把所有 P2/P3 历史问题作为第一版阻断项。
- 不直接启用 QE 实时生产 hook。
- 不清理 QE workspace 或 QE DB 历史记录。
- 不直接做 LLM agent 自动演进。
- 不一次性实现所有模型 attribution；先保留 schema 和 LGB/LSTM 最小路径。
- 不引入新的 NoSQL/复杂存储系统作为第一阶段依赖。

<a id="rule-std-sync-001"></a>
## 20. [STD-SYNC-001] 一致性验证要求

本规范与机器 YAML 必须满足：

1. YAML `source_standard` 指向当前 MD 文件。
2. YAML `source_version` 等于本文版本。
3. YAML 中每条 enabled rule 必须有 `standard_ref`。
4. YAML 中每个 rule_id 必须在本文出现。
5. 人类规范新增 P0/P1 红线时，必须同步新增机器规则或标注手工审查项。
6. DESIGN-COMPLIANCE-001 必须作为人工复核控制项出现在 YAML `manual_review_controls` 中，并在提交前报告设计验收矩阵。
7. scanner、测试、nox 命令应默认读取当前 YAML，而不是历史归档版本。

## 21. 参考基线

- PEP 8 / PEP 257 / PEP 484：Python 命名、docstring、类型标注基础。
- Google Python Style Guide：异常、全局状态、线程/并发、可维护性建议。
- pytest / nox / Playwright：自动化验证基础。
- Qlib Recorder：Experiment -> Recorder/run 和 artifact 思想。
- QuantConnect LEAN / NautilusTrader：backtest/live 边界、事件驱动、执行测试。
- Freqtrade / Hummingbot：backtest summary、dry-run/live、connector/controller/executor 分层。


<a id="rule-prod-ddl-001"></a>
### 6.18 [PROD-DDL-001] Production DDL gate after Main merge

- Severity: P0.
- Scope: any task that adds or changes `backend/migrations/`, `backend/db/init_*.py`, schema/comment/index/constraint definitions, or runtime code that depends on new DB objects.
- Merging to `main` is not production-ready by itself. If the merged change contains production DB DDL, the production schema gate must run immediately after the `main` merge.
- The production schema gate must first verify the target DB in read-only mode: host, port, dbname, user, current schema state, migration file list, and expected DB objects. Secrets/passwords must never be printed.
- When DDL is required, execute the exact migration files committed in `main` against the production DB using idempotent or transaction-controlled DDL, and record before/after evidence.
- After DDL succeeds, verify tables, columns, indexes, constraints, PostgreSQL comments, required API/scheduler paths, and relevant logs. Persist the evidence under `tests/aistock_validation/history/`.
- If production DB access, user authorization, safe migration execution, or schema verification is unavailable, stop production activation and report `production_ddl_pending`. Do not report the feature as delivered, production-ready, or safe to restart.
- If the task has no DB DDL, the handoff must explicitly record `production_ddl_gate=noop`.
- It is forbidden to leave production runtime code active against a schema that is missing required runtime tables or columns.

<a id="rule-context-budget-001"></a>
## 22. [CONTEXT-BUDGET-001] 上下文、提示词和 token 预算规范

目标是在不降低 P0/P1 红线、设计完整性和业务验证质量的前提下，减少无关提示词、无关历史文档和重复日志带来的 token 浪费。

### 22.1 任务分级

| 层级 | 适用范围 | 默认上下文 | 验证要求 |
|---|---|---|---|
| T0 快速修复 | 文案、小 UI、明显依赖缺失、小测试修正、非核心 P2/P3 | 用户需求、相关文件片段、最小规则摘要、目标验证命令 | 针对性测试或构建验证；说明无生产影响 |
| T1 标准 Issue | 单模块 P1/P2 bug、普通业务逻辑修复 | issue 摘要、allowed_write_scope、reproduce、closure、模块规则摘要、相关代码片段 | issue 要求测试 + 相关回归；GitHub/BUG 状态同步 |
| T2 Batch Issue | 同模块、同风险域、同验证链路的多个 issue | batch issue 表、共享模块上下文、每个 issue 的 closure、统一验证矩阵 | 统一模块级验证；每个 issue 独立 commit/closure |
| T3 设计驱动 | 新功能、架构调整、跨模块能力、明确设计方案 | 设计验收索引、当前阶段范围、相关章节、接口/DB/UI/MCP 验收项 | DESIGN-COMPLIANCE-001 完整矩阵；不得简化交付 |

### 22.2 加载规则

1. 每个任务开始前必须声明 `task_tier`、`module`、`risk_level`、`phase` 和主要验证链路。
2. 禁止默认加载完整项目规范、完整设计方案、完整历史记忆、全部 issue 列表或无关模块提示词。
3. 设计驱动任务应先生成或读取 `Design Acceptance Index`，后续用编号追踪验收，不反复注入全文。
4. 子 agent 默认接收精简任务卡，不继承完整主上下文；只有任务确实依赖完整历史时才允许扩大上下文。
5. MCP/API 默认使用 summary/compact 返回；只有定位证据、失败详情或 closure 需要时再请求 full detail。
6. 命令输出默认使用 `rtk` 或等效压缩方式；长日志写入 `tests/aistock_validation/history/`、`debug_tools/` 或 `tmp/`，回复只给结论和路径。
7. 如果当前上下文不足以做安全判断，可以升级上下文层级，但必须说明升级原因和新增读取范围。

<a id="rule-issue-batch-context-001"></a>
## 23. [ISSUE-BATCH-CONTEXT-001] 同模块 issue 批处理和 Batch Context Pack

同一模块、同一风险域、同一验证链路、同一窗口负责的多个 issue，应优先合并到一个 batch worktree / batch branch 中处理，避免重复建 worktree、重复加载提示词和重复跑相同验证。

### 23.1 允许合并条件

- issue 属于同一模块或同一子域，例如 `paper_v2.selection`、`qe_template`、`validation_center.ui`。
- 主要修改文件重叠或相邻，且不会与其他窗口并行写同一文件。
- 复现路径或验证链路相同，可以共享模块级回归测试。
- 每个 issue 仍可独立 commit，必要时可单独 revert。
- 不包含需要单独长时间实验、生产 DDL、protected asset、模型权重、HMM snapshot 或 QMT/实盘路径的特殊高风险变更；若包含，必须单独评估是否拆出。

### 23.2 Batch Context Pack 字段

```json
{
  "batch_id": "BATCH-<MODULE>-<YYYYMMDD>",
  "task_tier": "T2",
  "module": "paper_v2.selection",
  "issues": ["BUG-096", "BUG-101"],
  "shared_files": ["backend/services/..."],
  "shared_risks": ["runtime_profile_binding"],
  "shared_test_matrix": ["pytest ...", "playwright ..."],
  "per_issue_closure_map": {
    "BUG-096": ["closure item 1", "closure item 2"]
  },
  "per_issue_commit_map": {}
}
```

### 23.3 执行要求

1. batch 共享一个 worktree、一个 branch、一次模块上下文和一次主验证链路。
2. 每个 issue 必须保留独立 GitHub Issue / BUG JSON、独立 fix commit、独立 closure 记录。
3. PR 描述必须列出 batch 内所有 issue、每个 issue 的 fix commit、验证覆盖项和剩余风险。
4. 任一 issue 需要扩大 scope、引入跨模块重构或改变风险等级时，必须从 batch 中拆出或重新确认 batch 边界。

<a id="rule-prod-dependency-001"></a>
## 24. [PROD-DEPENDENCY-001] Production dependency gate after Main merge

修改依赖清单时，CI 通过不等于生产运行目录已经具备依赖。合入 `main` 后，必须在实际运行目录执行依赖同步和构建/导入验证。

### 24.1 Frontend dependency gate

触发条件：修改 `frontend/package.json`、`frontend/package-lock.json`、`frontend/pnpm-lock.yaml` 或 `frontend/yarn.lock`。

必做步骤：

1. 在生产运行目录 `F:\Dev\AIstockrontend` 执行 `npm install` 或经项目确认的等效命令。
2. 对新增或关键依赖执行 `npm ls <package> --depth=0`。
3. 执行 `npm run build`，确认包含受影响页面且无 module-not-found / type error。
4. 汇报 `production_frontend_dependency_gate=applied_and_verified`；若无法执行，汇报 `production_frontend_dependency_gate=pending`，不得声明前端可重启。

### 24.2 Backend dependency gate

触发条件：修改 `requirements*.txt`、`pyproject.toml`、`poetry.lock`、`environment*.yml`、`backend/requirements*.txt` 或其他 Python/Conda 依赖清单。

必做步骤：

1. 在目标 Conda 环境或项目规定环境中同步依赖。
2. 对新增或关键包执行导入验证或版本验证。
3. 执行受影响后端测试或启动前 import smoke。
4. 汇报 `production_backend_dependency_gate=applied_and_verified`；若无法执行，汇报 `production_backend_dependency_gate=pending`。

### 24.3 Handoff 要求

每次 feature/fix handoff 都必须同时声明：

- `production_ddl_gate`: `noop` / `applied_and_verified` / `pending`。
- `production_frontend_dependency_gate`: `noop` / `applied_and_verified` / `pending`。
- `production_backend_dependency_gate`: `noop` / `applied_and_verified` / `pending`。

