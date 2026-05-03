# AIstock 自动化测试流水线、覆盖率与可观测管理系统设计

> 日期：2026-05-04  
> 状态：顶层设计草案 v1.0，待评审确认  
> 文档位置：`docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`  
> 适用范围：AIstock 全仓库，重点覆盖 QE 数据完整性、未来 QE 数仓、Paper Trading v2、Selection Center、StrategyPackage、HMM、Qlib 数据链路、前端 UI 与 API。  
> 边界：本文设计自动化测试流水线、覆盖率门禁、测试可观测 UI 与版本管理体系；不直接修改测试代码或运行生产服务。

## 1. 结论先行

AIstock 后续需要一套“本地权威、结果导向、可观测、可管理、可复用、可版本化”的自动化测试流水线系统。它不能只等同于 `pytest` 或 Playwright，而应成为研发流程、质量门禁、版本发布和历史证据管理的一部分。

核心结论：

1. 当前仓库已经有 `noxfile.py`、`scripts/aistock_validate.py`、`tests/aistock_validation`、Paper v2 / QE / QE archive 的若干 L3 流水线雏形，也已有 Playwright 依赖和部分 E2E 用例。
2. 当前未观察到统一的仓库级 coverage 阈值门禁；`noxfile.py` 中的 backend 测试主要运行 `pytest -q -p no:cacheprovider`，没有强制 `pytest-cov` / branch coverage / diff coverage。
3. 后续所有高风险功能，尤其是 QE 数据采集、数仓、入仓清理、交易执行、成本核对、HMM、Paper v2，都必须在设计阶段同时定义测试用例和 coverage gate。
4. 覆盖率不是唯一质量指标，但必须成为基础门禁：没有合理覆盖率的代码不得宣称“已通过全流程验证”。
5. 未来应建设专门的测试流水线系统和 UI：管理测试目录、测试用例、测试计划、执行记录、覆盖率趋势、失败分析、证据 artifact、版本发布候选和回归矩阵。

目标形态：

```text
开发任务 / 设计文档 / PR 或本地变更
  -> 测试计划自动生成或人工确认
  -> nox / aistock_validate 统一执行
  -> 单元/API/DB/UI/E2E/数据质量/红线/覆盖率门禁
  -> run record + coverage + trace + evidence
  -> 测试管理 UI 可查询、复用、比较、追踪趋势
  -> 版本候选质量报告
```

## 2. 当前测试基础与缺口

### 2.1 已有基础

| 能力 | 当前证据 | 评价 |
|---|---|---|
| 本地流水线入口 | `noxfile.py` 中已有 `l0`、`paper_v2_l3`、`qe_read_l3`、`qe_archive_l3` 等 session | 已有雏形 |
| 测试记录目录 | `tests/aistock_validation/history/*` 已保存多类 run record | 已有可追溯习惯 |
| 模块测试矩阵 | `tests/aistock_validation/modules/qe.md`、`qe_archive.md`、`paper_v2_selection_center.md` | 已开始模块化 |
| 前端 E2E | `frontend/package.json` 有 `test:e2e`，Playwright 依赖存在 | 可继续扩展 |
| 开发端口隔离 | QE/Paper v2 文档和 nox session 使用 8011/8012、3011/3012 | 符合生产隔离要求 |
| 静态红线扫描 | `.codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py` 被 L0/L3 调用 | 已有基础 |

### 2.2 主要缺口

| 缺口 | 风险 | 设计要求 |
|---|---|---|
| 无统一 coverage 阈值 | 代码可能只跑通 happy path，边界分支未测 | 引入 coverage 配置、diff coverage 和模块阈值 |
| E2E 与单元测试边界不清 | 全靠 Playwright 慢且难定位，全靠单测又缺真实链路 | 分层测试：L1 单元、L2 API/DB、L3 UI/API/DB 链路 |
| 测试用例与设计文档未强绑定 | 实现后才补测试，容易漏关键场景 | 每份设计文档必须包含验收和测试矩阵 |
| 测试结果缺统一 UI | run record 分散在 markdown，查询和趋势管理弱 | 建设测试管理 UI 和后端数据模型 |
| 覆盖率与版本发布未绑定 | 发布候选缺质量评分 | L5 release candidate 必须包含 coverage 和功能门禁 |
| 数据质量测试未统一 | 量化系统中数据错误比代码错误更致命 | 建立 data-quality oracle 和 schema/time/data freshness gate |
| 受保护资产测试不够系统 | 模型、manifest、ledger、HMM snapshot 被误改风险 | 加 asset diff guard、hash guard、只读 fixture |

## 3. 测试分层标准

| 层级 | 名称 | 目标 | 典型工具 | 是否要求覆盖率 |
|---|---|---|---|---|
| L0 | 静态与质量门禁 | 不启动服务，发现红线、类型、格式、secret、coverage 配置缺失 | ruff/tsc/semgrep/custom scan | 是，检查 coverage 配置和报告存在 |
| L1 | 单元测试 | 验证纯函数、parser、config merge、hash、cost reconcile、schema validation | pytest、Vitest | 是，强制 line/branch/diff coverage |
| L2 | 组件/API/DB 集成 | 验证 repository、router、service、DB schema/comment、API contract | pytest + TestClient + 临时 DB/隔离 schema | 是，模块阈值 |
| L3 | 模块全流程 | 验证一个业务模块从 UI/API/DB/日志/数据质量到业务结果 | nox + dev backend/frontend + Playwright | 不以代码覆盖率为主，但必须引用 L1/L2 coverage |
| L4 | 跨模块集成 | 验证 QE -> 数仓 -> StrategyPackage/Paper v2 或数据链路跨模块 | nox + API + E2E + 数据质量 smoke | 必须汇总相关模块 coverage |
| L5 | 发布候选 | 版本候选质量报告、回归矩阵、覆盖率趋势、残余风险 | release candidate pipeline | 必须有总覆盖率、diff coverage、关键模块 coverage |

原则：

- L3/L4 证明“业务链路真实可用”；L1/L2 证明“代码分支被合理测试”。两者不能互相替代。
- 高频交易/回测/清理/归档等高风险逻辑必须有 L1/L2 覆盖，不能只靠 UI 点击。
- 每个失败都应有结构化失败类型：code_error、data_error、contract_error、env_error、asset_error、ui_error、coverage_gate_failed。

## 4. 覆盖率策略

### 4.1 后端 Python 覆盖率

建议引入：

- `pytest-cov`：生成 terminal、XML、HTML、JSON coverage。
- branch coverage：启用分支覆盖，避免只覆盖直线路径。
- diff coverage：对新增/修改代码强制更高阈值。
- module coverage policy：高风险模块单独阈值。

建议阈值采用分阶段落地，避免历史代码基线过低导致无法启动：

| 阶段 | 全仓库 line | 新增/修改 diff line | 新增/修改 diff branch | 高风险模块 line | 高风险模块 branch |
|---|---:|---:|---:|---:|---:|
| Phase A 基线期 | 记录不阻断 | 80% | 70% | 80% | 65% |
| Phase B 稳定期 | 65% | 85% | 75% | 85% | 75% |
| Phase C 目标期 | 75% | 90% | 80% | 90% | 80% |

高风险模块包括但不限于：

- `backend/services/quantevolver` 中的 QE 配置、采集、parser、worker API、重试/恢复、清理逻辑。
- `backend/services/qe_archive` 和未来数仓相关代码。
- `backend/services/paper_trading_v2`、`selection_center`、`strategy_package`。
- HMM runtime、Qlib exporter、minute execution algo、成本/ledger/position 计算。
- DB schema/migration/comment 检查。

覆盖率例外规则：

- 允许 `# pragma: no cover`，但必须限于防御性异常、CLI 入口、不可达平台分支，并写明原因。
- 不允许用 `pragma` 跳过核心业务分支、清理分支、异常处理分支、成本差异分支。
- 任何 coverage gate 降级都必须进入 run record 的 residual risk。

### 4.2 前端覆盖率

前端建议分三层：

| 类型 | 工具 | 覆盖目标 |
|---|---|---|
| TypeScript 静态检查 | `tsc --noEmit` | 类型、API 字段、不可达 props |
| 组件/状态单元测试 | Vitest + Testing Library | 表单校验、状态转换、错误展示、分页、按钮可用性 |
| E2E 流程 | Playwright | 真实用户路径、API 调用、console/pageerror/requestfailed、业务 oracle |

建议阈值：

- 新增/修改前端工具函数和状态管理 diff coverage >= 80%，目标 90%。
- 关键业务组件分支覆盖：按钮禁用/启用、dry-run/正式执行、缺字段提示、分页、错误态、loading 态。
- Playwright 不作为代码覆盖率主指标，但作为用户路径覆盖率指标：关键页面、关键按钮、关键错误、关键数据展示必须被覆盖。

### 4.3 数据质量覆盖率

量化系统必须把“数据质量”纳入覆盖率概念：

| 数据对象 | 覆盖指标 |
|---|---|
| Qlib day/minute 数据 | 交易日覆盖率、股票覆盖率、字段完整率、snapshot hash |
| suspend/limit/pre_close | PIT 覆盖率、缺失日期、缺失股票、刷新 audit |
| QE 实验数据 | required fields 覆盖率、artifact manifest 覆盖率、可复现等级分布 |
| 数仓入仓 | source completeness、hash 校验、row_count 校验、source cleanup simulation 通过率 |
| Paper v2 ledger | cash/position/order/fill/snapshot 一致性覆盖率 |

数据质量测试不以 line coverage 替代，必须有业务 coverage report。

## 5. QE 数据完整性与未来数仓专项测试矩阵

| 测试编号 | 测试名称 | 层级 | 关键 oracle |
|---|---|---|---|
| QE-DQ-001 | 创建阶段完整 effective config | L1/L2 | API 返回的 effective config 含策略实际值、成本、initial_cash、HMM、数据切分、hash |
| QE-DQ-002 | QE DB 不保存大明细 | L1/L2 | 大 positions/trades/pred/log 不进入 `metrics_json`/`result_metrics`；只写 manifest |
| QE-DQ-003 | completion payload required schema | L1 | 缺字段进入 partial，不允许 complete |
| QE-DQ-004 | artifact manifest hash | L1/L2 | URI、sha256、size、row_count、schema_version 可校验 |
| QE-DQ-005 | backtest-only training source | L2/UI | 当前 loop 无训练曲线时显示来源训练摘要，不显示空白成功 |
| QE-DQ-006 | cost reconciliation | L1/L2/UI | report/execution/cash ledger 差异可解释，超阈值 fail gate |
| QE-DQ-007 | execution event chain | L1/L2 | order_intent -> child_order -> fill/unfilled -> substitute 可追踪 |
| QE-DQ-008 | no direct workspace access | L0/L1 | 静态和单测阻止 WSL/远端 workspace 直读 |
| QE-DQ-009 | source cleanup independence | L3/L4 | 模拟 QE DB 源记录/workspace 不可用后，数仓仍能返回历史详情 |
| QE-DQ-010 | historical backfill API | L2/L3/UI | 候选分页、dry-run、正式入仓、质量报告、增量 loop 补录 |
| QE-DQ-011 | UI data fidelity | L3/UI | UI 详情指标、持仓、成本、配置与 API/DB oracle 一致 |
| QE-DQ-012 | cleanup gate | L2/L4 | archive completeness 未通过时禁止清理 QE 源数据 |

## 6. 测试流水线系统架构

未来专门测试系统建议包含以下模块：

```text
Test Management UI
  -> Test Catalog / Case Matrix / Run History / Coverage Dashboard / Release Gates
        |
        v
Validation API
  -> run scheduler / environment manager / evidence manager / coverage service / quality gate engine
        |
        v
Execution Layer
  -> nox sessions / pytest / Playwright / data quality smoke / static scans
        |
        v
Evidence Store
  -> run record / coverage json+xml+html / screenshots / traces / logs / DB smoke output / artifact hashes
```

### 6.1 UI 能力

| 页面 | 功能 |
|---|---|
| 测试总览 | 最近运行、通过率、失败率、覆盖率趋势、关键模块健康度 |
| 测试用例库 | 模块、层级、用例、业务 oracle、关联设计文档、关联代码路径 |
| 运行计划 | 选择模块和层级，配置端口、数据样本、dry-run、是否启动 UI |
| 运行详情 | 命令、环境、耗时、失败、截图、trace、日志、coverage、质量门禁 |
| 覆盖率看板 | 总覆盖、diff coverage、branch coverage、高风险模块覆盖率、趋势 |
| 失败分析 | flaky 标记、失败分类、首次失败版本、复测结果、责任模块 |
| 发布候选 | L5 门禁状态、变更列表、覆盖率、残余风险、是否允许发布/提交 |
| 版本历史 | 每次提交/版本对应测试结果和回归矩阵 |

### 6.2 后端数据模型概念

后续若实现测试管理系统，建议保存：

- `test_case`：用例定义、模块、层级、oracle、数据需求、owner、状态。
- `test_plan`：一次运行计划，包含选择的用例、环境、端口、数据样本。
- `test_run`：一次执行实例，包含 git commit、started/finished、status、duration、trigger。
- `test_run_step`：每个命令/阶段的结果。
- `test_evidence`：coverage、trace、screenshot、log、json report 的 manifest。
- `coverage_snapshot`：line/branch/diff coverage、模块阈值、失败原因。
- `quality_gate_result`：红线、数据质量、DB comment、asset safety、release gate。

所有新增表/字段后续必须添加 PostgreSQL comment，遵守项目 DB 注释规范。

## 7. 版本管理与发布门禁

### 7.1 每次功能完成的最低要求

- 关联设计文档或 issue。
- 关联测试用例矩阵。
- L0 通过。
- 相关 L1/L2 覆盖率达标。
- 涉及 UI 的功能必须有 L3 UI/API/DB 验证。
- 涉及数据入库、归档、清理的功能必须有数据质量和 source cleanup simulation。
- run record 写入 `tests/aistock_validation/history/<module>/...` 或未来测试系统。

### 7.2 发布候选 L5 要求

| 门禁 | 要求 |
|---|---|
| 代码覆盖率 | 达到当前阶段阈值；diff coverage 不低于要求 |
| 功能回归 | 相关模块 L3/L4 通过 |
| 数据质量 | 关键数据源 freshness / completeness 通过 |
| UI 质量 | 关键页面无 pageerror、console error、unexpected 4xx/5xx |
| 资产安全 | StrategyPackage、模型、HMM、ledger、QE artifacts 未被非预期修改 |
| DB schema | 新表新字段 comment 完整，migration 可重复执行 |
| 文档 | 设计、测试、残余风险、操作说明更新 |
| 残余风险 | 所有跳过项明确记录，不允许隐性跳过 |

## 8. 推荐落地阶段

### Phase 0 - Coverage baseline

- 检查当前 pytest/Playwright/nox 覆盖能力。
- 引入 coverage 配置设计：`.coveragerc` 或 `pyproject.toml` 的 coverage section。
- 先记录全仓库 baseline，不阻断历史代码。
- 对新增/修改代码启用 diff coverage gate。

### Phase 1 - QE 数据完整性 coverage gate

- 为 QE effective config、completion payload、manifest、cost reconcile、execution events、cleanup gate 建 L1/L2 测试。
- 在 `noxfile.py` 预留 `qe_data_completeness_backend`、`qe_data_completeness_l3`、`qe_data_completeness_coverage`。
- 所有 QE 数据完整性代码必须输出 coverage report。

### Phase 2 - 测试结果结构化

- 扩展 `scripts/aistock_validate.py`，统一写 JSON run metadata。
- 将 markdown run record 与机器可读 JSON 绑定。
- 保存 coverage snapshot、quality gate result、evidence manifest。

### Phase 3 - 测试管理 UI MVP

- 新增测试中心 UI：运行历史、coverage dashboard、失败详情、模块矩阵。
- 支持选择模块运行 nox session，先本地手动触发，后续再调度。
- 只管理测试，不自动修改生产服务。

### Phase 4 - Release candidate gate

- 支持 L5 发布候选报告。
- 将 coverage、L3/L4、数据质量、资产安全、DB comment、文档完整性纳入统一评分。
- 支持版本对比和质量趋势。

## 9. 专家补充建议

### 9.1 数仓专家建议

- 测试必须包含“源系统清理后可独立查询”的场景，否则数仓独立性只是口头约束。
- 每个入仓测试都要验证 `source_hash`、`row_count`、`schema_version`，防止重复入仓或静默字段漂移。
- 大明细的测试不能只检查文件存在，还要抽样验证 parquet/jsonl schema、行数、时间范围、symbol/date 主键唯一性。

### 9.2 量化架构师建议

- 测试 oracle 要体现量化口径：绝对收益 vs 超额收益、with cost vs without cost、日频 vs 分钟、是否处理涨跌停/停牌。
- 成本、现金、持仓、订单必须能对账；不能只比较收益曲线。
- 模型和因子测试要覆盖 LSTM/深度模型，不只覆盖 LGB native importance。
- 回测数据质量测试应默认淘汰未处理涨跌停/停牌的日频策略有效排名。

### 9.3 测试专家建议

- 测试用例必须先定义 oracle，再写代码。
- 每个 bug 修复必须新增回归测试，且 run record 记录“先失败、后修复、再通过”的证据。
- 覆盖率阈值应分阶段提高，避免一开始被历史技术债阻塞，但新增代码必须严格。
- Flaky test 必须被标记和隔离，不允许长期“偶发失败忽略”。
- UI E2E 必须监听 pageerror、console error、requestfailed 和 unexpected HTTP 4xx/5xx。

## 10. 与现有文档的关系

- `docs/architecture/aistock_testing_version_management_system_design_20260429.md`：已有测试与版本管理顶层思路；本文补充覆盖率、可观测 UI、QE/数仓专项测试和质量门禁。
- `docs/architecture/qe_experiment_data_completeness_prewarehouse_plan_20260503.md`：QE 数据完整性前置改造方案，本文为其提供测试与覆盖率约束。
- `docs/architecture/qe_worker_workspace_read_refactor_validation_plan_20260502.md`：QE worker workspace 红线验证，本文继承其 dev ports 和 API-only 原则。
- `tests/aistock_validation/*`：当前测试矩阵和历史记录目录，未来应升级为测试管理系统的数据源之一。
- `noxfile.py`：当前本地流水线入口，未来应增加 coverage gate 和专项 session。
