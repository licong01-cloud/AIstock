# AIstock 自动化测试流水线与版本管理体系设计

创建日期：2026-04-29

适用范围：AIstock 全仓库，包括 FastAPI backend、Next.js frontend、TDX 数据桥、Tushare 数据同步、QE / RD-Agent、StrategyPackage、Selection Center、Paper Trading v2、HMM、日内分钟线执行策略、未来实盘前置验证。

## 1. 背景与问题

AIstock 目前已经进入“多模块联动 + 真实数据 + 策略资产 + UI 操作”的复杂阶段。之前 Paper Trading v2 / Selection Center 的验证暴露出一个核心问题：如果只验证接口返回或单一路径页面点击，不能发现真实操作员会遇到的业务失败，例如：

- UI 可以打开，但按钮调用的后端 API 不存在或不一致。
- 页面显示成功，但后端实际使用了过期 artifact、缓存结果或错误数据源。
- 选股流程能触发，但模型特征 schema 与训练 schema 不一致。
- HMM、停牌、行业黑名单、分钟线、涨跌停等业务数据缺失时没有提前发现。
- Next.js dev proxy、页面轮询、前端 chunk、控制台错误等 UI 运行时问题没有纳入验证。
- 功能代码修改时可能影响策略、模型、HMM snapshot、StrategyPackage manifest 等资产，但缺少资产安全审计。

因此，AIstock 后续开发必须从“功能跑通”升级为“结果导向测试与版本化发布”。

## 2. 总目标

建立一套可复用、可追溯、可自动化执行的测试与版本管理体系，使每次功能开发都能按统一标准完成：

1. 单功能验证：新增或修复单个能力时，有最小但有效的单元/API/UI/数据验证。
2. 多功能链路验证：多个能力联动时，从 UI/API/DB/log/业务结果进行交叉验证。
3. 模块全量回归：某个模块多次改动后，可以执行该模块已沉淀的完整测试矩阵。
4. 跨模块集成验证：例如 StrategyPackage -> Selection Center -> Paper Trading v2 -> UI 的真实业务链路。
5. 发布候选验证：大功能阶段完成后，按版本发布，而不是长期处于混乱的“未定义状态”。
6. 测试留痕：每次测试保存测试用例、执行命令、端口、数据样本、结果、失败、修复、复测证据。
7. 业务结果导向：测试不仅证明程序无异常，还要证明选股、模拟盘、数据同步、模型推理等结果有业务价值。

## 3. 核心原则

### 3.1 结果导向

测试目标不是“接口返回 200”或“页面可以点”，而是验证业务目标，例如：

- 选股结果来自最新可用数据和策略包模型推理，而不是 QE 回测 pred.pkl。
- 模拟盘订单、成交、现金、持仓、NAV 都由分钟线执行链路真实产生并持久化。
- HMM 系数生成不使用未来数据，且可追溯 as_of_trade_date 与 effective_trade_date。
- suspend_d 停牌过滤、行业黑名单、topK 补位等业务规则真实生效。

### 3.2 Fail-fast，禁止静默成功

禁止任何会影响业务逻辑的静默兜底：

- 不允许默认价格、默认现金、默认持仓。
- 不允许空数组伪装成功。
- 不允许缺分钟线时 fallback 到日频。
- 不允许 V25 缺上下文时 fallback 到 TWAP。
- 不允许缺 HMM 系数时使用中性系数。
- 不允许模型特征数量不匹配时 padding / truncate。

### 3.3 资产与程序框架分离

程序框架开发不得静默修改资产。受保护资产包括：

- StrategyPackage frozen manifest 与 manifest_sha256。
- QE / RD-Agent 实验产物、workspace、pred、模型训练产物。
- 模型权重、pickle、Torch checkpoint、Qlib 数据。
- HMM snapshot、coefficient artifact。
- validated execution policy。
- selection artifact、paper ledger、历史 run、历史 order/fill/position。

如果确实需要修改资产，必须单独确认、单独提交、单独验证、单独说明影响范围。

### 3.4 UI 与后端能力一致

UI 不是装饰层，而是操作员入口。所有后端能力如果要求人工使用，就必须有对应 UI 流程。所有 UI 按钮必须调用真实后端能力，错误必须透传，状态必须可读。

UI 禁止直接展示原始 JSON 给普通操作员。配置、风控、执行策略、订单追踪、错误上下文应转换成中文标签、字段名、参数值、表格、状态卡或可读明细。

### 3.5 开发端口与生产端口隔离

开发验证不得重启或干扰生产后端 8001。统一使用：

- 后端开发端口：8011 或 8012。
- 前端开发端口：3011 或 3012。
- 启动前必须检查端口占用。
- 完成开发后只通知用户是否需要重启生产服务，由用户执行生产重启。

## 4. 推荐开源工具与成熟实践

AIstock 不应依赖单一测试工具，而应组合业界成熟实践：

| 分类 | 推荐工具/实践 | 用途 | AIstock 落地建议 |
|---|---|---|---|
| 测试方法论 | ISTQB CTFL v4.0 | 风险驱动、等价类、边界值、状态迁移、决策表 | 每个模块维护测试矩阵，先定义 business oracle |
| Python 测试 | pytest | 单元、集成、fixture、参数化 | backend/tests 继续作为主测试框架 |
| UI E2E | Playwright | 用户视角端到端测试、trace、截图、自动等待 | Paper v2、Selection Center、QE 关键页面全流程验证 |
| 前端组件 | Testing Library | 从用户视角验证组件行为 | 后续补充关键组件测试，减少只靠 E2E |
| API 合约 | Schemathesis | 基于 OpenAPI 的 API property/fuzz 测试 | 先用于只读/安全端点，逐步扩展写接口 |
| 数据质量 | Pandera / Great Expectations | DataFrame/table schema、时效性、唯一性、范围 | market、selection、paper_v2、strategy_pkg 核心数据门禁 |
| DB 集成 | Testcontainers | 临时 PostgreSQL/TimescaleDB 集成测试 | CI 中隔离测试 repository/migration |
| Python 质量 | Ruff、mypy/pyright | lint、格式、类型 | L0 静态门禁 |
| 前端质量 | TypeScript tsc、ESLint | 类型和前端静态检查 | UI 改动必须 `npx tsc --noEmit` |
| 安全扫描 | Gitleaks、Bandit、OWASP WSTG/ZAP | secret、Python 安全、Web 安全 | pre-commit/CI 必跑 secret；高风险接口跑安全测试 |
| 自定义规则 | Semgrep | 禁止硬编码路径、静默 fallback、资产写入 | 建议新增 `.semgrep/aistock/*.yml` |
| 可访问性 | axe-core | WCAG/无障碍自动检查 | 关键 UI E2E 后加无障碍检查 |
| 报告 | Allure Report / pytest-html | 测试报告、附件、趋势 | 本地先用 Markdown run record，后续接 Allure |
| 可观测性 | OpenTelemetry、结构化日志 | 长流程 trace、日志关联 | QE、HMM、Selection、Paper session 后续接入 |
| 版本发布 | SemVer、Conventional Commits、release-please / semantic-release | 版本号、changelog、GitHub release | 推荐先用 SemVer + Conventional Commits + release-please |
| Git 钩子 | pre-commit | 本地提交前门禁 | 后续配置 Ruff/Gitleaks/Semgrep/自定义扫描 |

参考来源：

- ISTQB CTFL v4.0：https://test.istqb.org/certifications/certified-tester-foundation-level-ctfl-v4-0/
- pytest fixtures：https://docs.pytest.org/en/stable/how-to/fixtures.html
- Playwright best practices：https://playwright.dev/docs/best-practices
- Testing Library principles：https://testing-library.com/docs/guiding-principles/
- Schemathesis：https://schemathesis.io/
- Pandera：https://pandera.readthedocs.io/
- Great Expectations：https://docs.greatexpectations.io/
- Testcontainers Python：https://testcontainers-python.readthedocs.io/
- Semgrep rule syntax：https://semgrep.dev/docs/writing-rules/rule-syntax
- Gitleaks：https://gitleaks.org/
- Ruff：https://docs.astral.sh/ruff/
- Bandit：https://bandit.readthedocs.io/
- OWASP WSTG：https://owasp.org/www-project-web-security-testing-guide/
- axe-core：https://github.com/dequelabs/axe-core
- Allure pytest：https://allurereport.org/docs/pytest/
- Semantic Versioning：https://semver.org/
- Conventional Commits：https://www.conventionalcommits.org/
- release-please：https://github.com/googleapis/release-please
- semantic-release：https://semantic-release.gitbook.io/

## 5. 仓库目录设计

### 5.1 Codex 专用 skill

已经建立的仓库内 skill：

```text
.codex/skills/verify-aistock-feature/
  SKILL.md
  agents/openai.yaml
```

后续应扩展：

```text
.codex/skills/verify-aistock-feature/
  references/
    testing-workflow.md
    module-matrices.md
    aistock-business-oracles.md
    ui-usability-checklist.md
    asset-safety-and-code-audit.md
    toolchain.md
  scripts/
    new_test_run.py
    scan_quality_guardrails.py
    scan_runtime_logs.py
    verify_no_silent_fallback.py
```

职责：

- 作为 Codex 每次开发完成后的标准验证入口。
- 根据模块自动选择测试级别、测试矩阵、命令、UI E2E、日志扫描、资产审计。
- 输出测试 run record 并要求提交。

### 5.2 测试用例和历史记录目录

建议新增：

```text
tests/aistock_validation/
  README.md
  catalog/
    test_levels.md
    release_gates.md
    quality_guardrails.md
  modules/
    strategy_package.md
    selection_center.md
    paper_trading_v2.md
    hmm.md
    qe.md
    rdagent.md
    tushare_data.md
    frontend_core.md
    execution_algos.md
  templates/
    test_case.md
    test_run_record.md
    release_candidate_report.md
  history/
    selection_center/
    paper_trading_v2/
    release/
  runs/
    .gitkeep
```

规则：

- 测试用例、矩阵、模板提交 Git。
- 大型 trace、截图、日志、数据库 dump 不提交，只在 run record 记录路径、hash、摘要。
- 每次大功能修改必须更新对应模块矩阵。
- 每次模块全量测试必须保存历史记录。

## 6. 测试分级体系

| 等级 | 名称 | 触发场景 | 覆盖范围 | 退出标准 |
|---|---|---|---|---|
| L0 | 静态门禁 | 任意代码/配置修改 | lint/typecheck/secret/guardrail scan | 无高危问题 |
| L1 | 单功能测试 | 单个函数/API/组件/bug fix | 新增功能及直接边界 | 目标测试通过 |
| L2 | 业务链路测试 | 一个功能跨 API/service/DB/UI | happy path + fail-fast + 持久化 | 业务结果和证据一致 |
| L3 | 模块全量回归 | 一个模块多个功能修改后 | 模块测试矩阵全部可执行路径 | 无阻塞缺陷 |
| L4 | 跨模块集成 | 数据/QE/Selection/Paper/UI 联动 | 真实业务端到端 | UI/API/DB/log 全部一致 |
| L5 | 发布候选 | 准备打版本 | 全仓 smoke + 高风险模块全量 | 发布报告通过 |

## 7. 标准测试流水线

### 7.1 单功能开发完成后

1. 读取项目记忆、模块设计、测试矩阵。
2. 判断影响范围和受保护资产。
3. 执行 L0 静态门禁。
4. 执行 L1 单功能测试。
5. 如果涉及 UI，执行对应 Playwright 小路径测试。
6. 如果涉及 DB，验证落库、状态、错误事件。
7. 更新测试用例或测试矩阵。
8. 记录测试 run 或在 commit message/开发报告中引用测试证据。
9. 提交并推送。

### 7.2 多功能修改后

1. 按模块矩阵执行 L2/L3。
2. 不只测试新增功能，还要测试该模块已有主路径和高风险 fail-fast 路径。
3. 对历史问题加回归用例，例如：
   - HMM 下拉无限刷新。
   - `paperV2Api.sessions is not a function`。
   - Selection feature count mismatch。
   - performance-report 404 spam。
   - Next dev chunk 缺失。
4. 修复一个问题后，必须重跑失败用例和周边流程。
5. 保存 `tests/aistock_validation/history/<module>/...md`。

### 7.3 发布候选

1. 冻结 release candidate commit。
2. 执行 L5：
   - backend 核心 pytest。
   - frontend `tsc` / build。
   - Playwright smoke + 高风险模块全路径。
   - 数据质量 smoke。
   - guardrail / secret scan。
   - 资产安全 diff。
3. 生成 release candidate report。
4. 更新 VERSION / CHANGELOG。
5. 创建 tag 和 GitHub Release。
6. 发布说明列出 DB migration、服务重启、已知风险、测试证据。

## 8. 模块测试矩阵概要

### 8.1 StrategyPackage

必须覆盖：

- 从 QE 单次实验创建策略包。
- 从 QE evolution loop 创建策略包。
- 候选列表只显示未加入策略包的来源。
- 策略包名称使用 QE 实验/loop 可读名称。
- 展示年化收益、IC、RankIC、Sharpe、最大回撤等摘要。
- frozen manifest 与 manifest_sha256 持久化。
- status 流转、事件记录、paper used 标记。
- manifest 被 paper 使用后不可静默修改。
- runtime profile 不进入 frozen manifest hash。

### 8.2 Selection Center

必须覆盖：

- 单策略包选股，默认 top20，可配置最大 50。
- HMM disabled / enabled。
- HMM snapshot / model version / signal preset 下拉选择。
- 缺 HMM coefficient fail-fast。
- suspend_d 停牌过滤和后续排名补位。
- 行业黑名单过滤和补位。
- 多策略包 intersection / union / weighted fusion。
- 已有选股记录点击后展示结果。
- 一键加入自选股票池，记录价格基准、加入时间、来源策略包名称。
- API/UI/DB/log 交叉验证。
- 禁止使用 QE 回测 pred.pkl 作为权威 live selection。

### 8.3 Paper Trading v2

必须覆盖：

- 单策略包创建模拟盘 portfolio。
- 自定义初始资金、runtime profile、HMM、行业黑名单、停牌过滤、validated execution policy。
- REPLAY_ONLY、LIVE_ONLY readiness、CATCHUP_THEN_LIVE。
- 回放是否自动切换实盘的选项。
- replay reject_existing。
- reset replay 必须输入 portfolio_id 确认。
- running portfolios 汇总列表。
- portfolio detail：资金、持仓、订单、成交、现金流水、NAV、错误、run events、配置审计。
- V25 day_features readiness。
- 缺分钟线、pre_close、limit、suspend、calendar、strategy output 必须 fail-fast。
- UI 不展示 raw JSON。

### 8.4 HMM

必须覆盖：

- config list、snapshot list、snapshot detail。
- rolling training preview，默认 3 个月验证集。
- 手工触发 rolling training 需要确认。
- daily coefficient generation job。
- as_of_trade_date 严格早于 effective_trade_date。
- 缺最新收盘数据、缺 sector/index 数据、缺 coefficient artifact fail-fast。
- Selection / Paper v2 只消费已完成 artifact，不在业务链路中静默训练。

### 8.5 Tushare / Data

必须覆盖：

- dataset spec、ingestion、scheduler、audit。
- suspend_d 从 2018-08-01 后全量历史已入库后的增量更新。
- 日期窗口 replace-before-insert 处理上游修正或删除。
- dataset_date_refresh_audit 成功/失败记录。
- 数据缺失时业务链路 fail-fast。

### 8.6 QE / RD-Agent / Execution Algo

必须覆盖：

- V24/V25/其他日内执行策略 catalog 可见。
- 小数据量 QE smoke。
- suspend_d 过滤后 V25 可运行。
- 分钟线执行策略必须符合标准 contract。
- 回测和 Paper v2 使用同一逻辑版本。
- 不修改策略/模型资产。

### 8.7 Frontend / UI

必须覆盖：

- 导航入口。
- 页面刷新与已有记录恢复。
- 加载、空状态、错误状态、成功状态。
- 所有按钮 enabled/disabled 的原因。
- 后端错误透传。
- 无 raw JSON。
- 浏览器 console error/pageerror/requestfailed/非预期 4xx/5xx 为测试失败。

## 9. 代码开发规范与 guardrails

### 9.1 禁止硬编码

禁止在运行时代码中硬编码：

- `F:\Dev\AIstock`。
- `C:\Users\...`。
- `\\wsl$` 或 `\\wsl.localhost`。
- `/mnt/f/Dev/AIstock`。
- WSL distro 名称，例如写死 `Ubuntu`。
- 密码、token、API key。
- 生产端口或本机绝对路径。

路径、端口、distro、asset root 必须来自配置、环境变量、DB catalog、manifest 或 API 请求。

### 9.2 禁止静默错误

高风险模式：

- `except Exception: return []`
- `except Exception: return None`
- `ok=true` 但业务未完成。
- 缺数据时默认填 0。
- 模型特征 mismatch 时 padding/truncate。
- live 不可用时改用历史缓存。
- Paper v2 分钟线不可用时改日频。

这些必须通过 Semgrep / 自定义扫描 / code review 逐步门禁化。

### 9.3 定期清理垃圾代码

每个版本发布前执行：

- 检查未使用脚本、backup、tmp、调试文件。
- 检查重复页面、旧 API、废弃 router。
- 检查 docs 与代码不一致。
- 检查 `.gitignore` 是否正确排除运行时产物。

清理原则：

- 程序垃圾代码可按回归测试后删除。
- 业务资产不能当垃圾删除，必须确认。
- 不确定的历史实验/模型/策略先标记候选，不直接删除。

## 10. 版本管理方案

### 10.1 是否需要版本管理

结论：需要。

AIstock 已经不是单脚本项目，而是包含数据、模型、策略资产、前后端、模拟盘、未来实盘的系统。没有版本号会导致：

- 不知道当前部署对应哪个功能状态。
- 无法区分“已验证可用”和“开发中”。
- 回归测试范围无法绑定 release。
- 线上/本地/实验环境问题难以追溯。

### 10.2 推荐版本策略

采用 SemVer，但当前先从 `0.x.y` 开始：

- `0.MINOR.PATCH`：系统仍在快速迭代。
- `MINOR`：新增可见业务能力，且通过模块回归。
- `PATCH`：bug fix 或小改动，不改变业务契约。
- 进入稳定阶段后再发布 `1.0.0`。

建议初始版本：

- 不立即给当前混乱状态打正式版本。
- 先完成测试体系 Phase 1 + Paper v2 / Selection Center 基线回归。
- 第一个可命名版本建议为 `v0.1.0`：代表“已有测试基线的 AIstock 内部开发版本”。

### 10.3 版本文件与发布产物

建议新增：

```text
VERSION
CHANGELOG.md
docs/releases/
  release_process.md
  v0.1.0_release_candidate.md
```

前端 `package.json` 版本与根 `VERSION` 后续可以同步，但初期以根 `VERSION` 为准。

### 10.4 提交规范

建议引入 Conventional Commits：

- `feat(selection): ...`
- `fix(paper-v2): ...`
- `test(hmm): ...`
- `docs(validation): ...`
- `refactor(qe): ...`
- `chore(ci): ...`

这有利于后续 release-please 自动生成 changelog。

### 10.5 Release gate

每次发布必须满足：

- L0 全通过。
- 受影响模块 L3 全通过。
- 高风险跨模块 L4 通过。
- UI E2E 无 console error/pageerror/requestfailed/非预期 HTTP 错误。
- asset safety audit 通过。
- migration 可重复执行或有明确应用记录。
- 文档更新。
- release candidate report 归档。

## 11. 分阶段实现方案

### Phase 0：基线整理与文档固化

目标：把测试体系、版本管理、规范和目录结构固定下来。

交付：

- 本设计文档。
- `.codex/skills/verify-aistock-feature` 有合法 SKILL.md。
- `tests/aistock_validation` 目录设计确认。
- 明确 L0-L5 测试分级。
- 明确版本发布策略。

验证：

- skill YAML 校验通过。
- 文档路径、目录设计与项目记忆一致。
- 不修改业务代码和资产。

### Phase 1：最小可用测试框架

目标：建立可以马上使用的本地测试流水线。

交付：

- `tests/aistock_validation/catalog/test_levels.md`
- `tests/aistock_validation/templates/test_case.md`
- `tests/aistock_validation/templates/test_run_record.md`
- `tests/aistock_validation/modules/selection_center.md`
- `tests/aistock_validation/modules/paper_trading_v2.md`
- `tests/aistock_validation/modules/strategy_package.md`
- skill references 与脚本：
  - `new_test_run.py`
  - `scan_quality_guardrails.py`

验证：

- 能生成测试 run record。
- 能对指定路径运行 guardrail scan。
- 不影响现有 pytest / frontend。

### Phase 2：核心模块测试矩阵与回归脚本

目标：先覆盖 AIstock 当前最高风险模块。

优先模块：

1. Selection Center。
2. StrategyPackage。
3. Paper Trading v2。
4. HMM daily coefficients。
5. V25 / execution_algos。
6. Tushare suspend_d / dataset audit。

交付：

- 每个模块的 L1/L2/L3 测试矩阵。
- 对历史 bug 建立回归用例。
- pytest 命令组合标准化。
- API 验证脚本或 pytest client 测试。

验证：

- 相关 backend tests 全通过。
- 能保存一次模块 L3 run record。
- 历史 bug 对应的测试能失败复现或通过防回归。

### Phase 3：UI E2E 全流程验证

目标：确保 UI 与后端能力一致，操作员可真实使用。

交付：

- Playwright 全局错误捕获 fixture。
- Selection Center UI E2E：
  - 单包选股。
  - HMM 下拉。
  - 多包聚合。
  - 加入自选。
  - 历史记录详情。
- Paper v2 UI E2E：
  - 创建模拟盘。
  - 回放。
  - reset confirmation。
  - running portfolio list。
  - detail / ledger / order / fill / position / snapshot / error。
- HMM UI E2E：
  - rolling preview。
  - daily coefficient async job。

验证：

- 使用 8011/8012 + 3011/3012。
- 不重启 8001。
- UI E2E 遇到 console error/pageerror/requestfailed/非预期 4xx/5xx 必失败。
- 生成 trace/screenshot 路径并写入 run record。

### Phase 4：数据质量与资产安全门禁

目标：防止数据错、资产错、路径错、静默改业务逻辑。

交付：

- Semgrep 自定义规则：
  - 硬编码路径。
  - WSL UNC。
  - silent fallback。
  - forbidden daily fallback。
  - protected asset write。
- Gitleaks 配置。
- Pandera / Great Expectations 数据质量 smoke：
  - suspend_d。
  - dataset_date_refresh_audit。
  - selection.run/result。
  - paper_v2 ledger/snapshot。
- asset safety audit checklist。

验证：

- 对当前仓库跑一次只读扫描，输出 baseline。
- 高危规则纳入 L0。
- 误报记录为 whitelist，但必须有理由。

### Phase 5：CI 与发布候选流程

目标：把本地规范升级为可重复的版本发布流程。

交付：

- GitHub Actions 或本地等价 CI 脚本。
- `VERSION`。
- `CHANGELOG.md`。
- `docs/releases/release_process.md`。
- release candidate report 模板。
- Conventional Commits 规范。
- release-please 配置或手动 release checklist。

验证：

- 生成第一个 release candidate report。
- 完成一次 dry-run 发布流程。
- 明确需要人工验证的部分，如真实交易时间 live 数据。

### Phase 6：全仓质量治理与长期维护

目标：持续降低混乱和历史技术债。

交付：

- Dead code scan。
- 临时脚本/backup 文件清单。
- 旧链路与新链路边界文档。
- 数据资产注册表。
- 生产/开发/测试环境差异文档。
- 定期全量 L5 测试计划。

验证：

- 每个版本发布前执行清理候选报告。
- 不误删资产。
- 文档、代码、UI、测试矩阵保持一致。

## 12. 立即执行建议

建议下一步按以下顺序执行：

1. 完成 Phase 1：建立 `tests/aistock_validation` 目录、模板、模块矩阵、skill references/scripts。
2. 对 Selection Center 与 Paper Trading v2 建立 L3 回归矩阵，覆盖之前人工发现的问题。
3. 给 Playwright 增加全局失败条件：console error、pageerror、requestfailed、非预期 4xx/5xx。
4. 建立 guardrail scan，先只读输出 baseline，再逐步改为高危阻断。
5. 在 Paper v2 / Selection Center 下一轮功能完成后，执行一次 L3 模块回归并保存历史记录。
6. 当核心模块稳定后，建立 `VERSION` 和 `CHANGELOG.md`，准备 `v0.1.0` release candidate。

## 13. 需要决策的内容

1. 版本号起点：建议等 Phase 1 + Paper v2/Selection L3 通过后打 `v0.1.0`。
2. CI 平台：如果 GitHub Actions 可以访问必要环境，优先 GitHub Actions；如果本地数据/WSL/GPU 依赖强，则先做本地 CI 脚本。
3. 报告系统：初期 Markdown run record，后续是否引入 Allure。
4. 数据质量工具：轻量优先 Pandera；如果需要更完整数据治理 UI/历史，可引入 Great Expectations。
5. API fuzz：Schemathesis 先只测只读/幂等接口，写接口需要测试 DB 或 sandbox。
6. 安全扫描强度：Gitleaks 应尽快加入；Bandit/ZAP 可在对外部署前加强。

## 14. 完成定义

当以下条件满足时，可以认为 AIstock 拥有第一版可用测试与版本管理体系：

- 有仓库版本控制的测试标准文档、skill、测试目录、模板、模块矩阵。
- 单功能、多功能、模块回归、跨模块集成、发布候选都有明确流程。
- Selection Center、Paper Trading v2、StrategyPackage 至少有 L3 测试矩阵。
- UI E2E 有全局错误捕获并覆盖关键路径。
- L0 guardrail 能扫描硬编码路径、密钥、静默 fallback、资产风险。
- 每次大改动有 test run record 留痕。
- 发布前可以生成 release candidate report。
- 版本号、changelog、release tag 的规则明确。
