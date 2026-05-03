# AIstock 自动化测试流水线、覆盖率与可观测管理系统设计

> 日期：2026-05-04  
> 状态：顶层设计草案 v1.1，待评审确认；本版明确基于现有 nox / aistock_validate / Playwright / 数据质量 smoke / run record 体系演进，不从 0 重建  
> 文档位置：`docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`  
> 适用范围：AIstock 全仓库，重点覆盖 QE 数据完整性、未来 QE 数仓、Paper Trading v2、Selection Center、StrategyPackage、HMM、Qlib 数据链路、前端 UI 与 API。  
> 边界：本文设计自动化测试流水线、覆盖率门禁、测试可观测 UI 与版本管理体系；不直接修改测试代码或运行生产服务。

## 1. 结论先行

AIstock 后续需要一套“本地权威、结果导向、可观测、可管理、可复用、可版本化”的自动化测试流水线系统。它不能只等同于 `pytest` 或 Playwright，而应成为研发流程、质量门禁、版本发布和历史证据管理的一部分。

本版设计的关键修正是：自动化流水线必须在已有成果上成熟化，而不是另起炉灶。现有 `noxfile.py` 已经是本地统一执行入口；`scripts/aistock_validate.py` 已经承担端口、服务探测和 run record 创建；`tests/aistock_validation` 已经有测试等级、模块矩阵和历史证据；Paper v2、QE read、QE archive 已经有 L3 级 nox session；Playwright、数据质量 smoke、guardrail scan 已经具备可复用骨架。未来测试中心 UI、覆盖率、版本门禁和调度能力都应包装和增强这些入口，而不是替代它们。

核心结论：

1. 当前仓库已经有 `noxfile.py`、`scripts/aistock_validate.py`、`tests/aistock_validation`、Paper v2 / QE / QE archive 的若干 L3 流水线雏形，也已有 Playwright 依赖和部分 E2E 用例。
2. 现阶段要把这些雏形升级为成熟流水线：统一 session 命名、统一 JSON run metadata、统一 evidence manifest、统一 coverage gate、统一测试矩阵引用、统一失败分类。
3. 当前未观察到统一的仓库级 coverage 阈值门禁；`noxfile.py` 中的 backend 测试主要运行 `pytest -q -p no:cacheprovider`，没有强制 `pytest-cov` / branch coverage / diff coverage。
4. 后续所有高风险功能，尤其是 QE 数据采集、数仓、入仓清理、交易执行、成本核对、HMM、Paper v2，都必须在设计阶段同时定义测试用例和 coverage gate。
5. 覆盖率不是唯一质量指标，但必须成为基础门禁：没有合理覆盖率的代码不得宣称“已通过全流程验证”。
6. 未来应建设专门的测试流水线系统和 UI：管理测试目录、测试用例、测试计划、执行记录、覆盖率趋势、失败分析、证据 artifact、版本发布候选和回归矩阵；UI 只调度受控 nox/aistock_validate 计划，不直接执行任意 shell。

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
| 前端 Playwright | `frontend/playwright.config.ts`、`frontend/tests/paper-v2`、`frontend/tests/qe`、`frontend/tests/qe-archive` | 已有统一 E2E 配置和模块用例目录 |
| 前端 TypeScript 检查 | `noxfile.py` 的 QE/QE archive UI session 会执行 `npm exec tsc -- --noEmit --incremental false` | 已在 UI 链路中作为类型门禁使用 |
| 数据质量 smoke | `scripts/aistock_data_quality_smoke.py`、`scripts/qe_archive_data_quality_smoke.py` | 已有业务数据 oracle 雏形 |
| 测试等级定义 | `tests/aistock_validation/catalog/test_levels.md` | 已定义 L0-L5，本设计应继承其语义 |
| run record 模板 | `tests/aistock_validation/templates/test_run_record.md` | 已有人工/自动证据模板，后续扩展 JSON metadata |
| QE archive 模块矩阵 | `tests/aistock_validation/modules/qe_archive.md` | 已经详细列出生产隔离、后端/API/UI/数据质量覆盖项 |
| Paper v2 模块矩阵 | `tests/aistock_validation/modules/paper_v2_selection_center.md` | 已有 backend/API/UI/business oracle/data quality/live validation 分层 |

### 2.2 当前可直接复用的程序入口

以下入口是后续成熟流水线的“权威执行层”，应优先扩展，不应绕过：

| 类型 | 当前入口 | 当前能力 | 后续增强方式 |
|---|---|---|---|
| L0 静态门禁 | `python -m nox -s l0` | 校验 Codex skill、执行 guardrail scan、默认扫描 Paper v2/Selection 相关路径 | 增加 coverage 配置检查、secret baseline、DB comment 检查、模块参数化扫描 |
| Paper v2 后端 | `python -m nox -s paper_v2_backend` | 跑 `backend/tests/paper_trading_v2`、`selection_center`、`strategy_package` | 加 `pytest-cov`、高风险模块阈值、失败分类 JSON |
| Paper v2 数据质量 | `python -m nox -s paper_v2_data_quality` | 调用 `scripts/aistock_data_quality_smoke.py` 输出 JSON | 接入统一 evidence manifest、严格/宽松 scope 标准化 |
| Paper v2 UI | `python -m nox -s paper_v2_ui` | 探测端口/服务，执行 `frontend/tests/paper-v2` Playwright | 标准化 console/page/request failure 采集，写入结构化 run step |
| Paper v2 L3 | `python -m nox -s paper_v2_l3` | 创建 run record，串联 L0/backend/data_quality/UI | 升级为模块成熟流水线模板，供 QE/数仓复制 |
| QE read 后端 | `python -m nox -s qe_read_backend` | 验证 QE 只读路径和 workspace 红线相关测试 | 扩展到 QE 数据完整性 parser/config/cost 测试 |
| QE read UI | `python -m nox -s qe_read_ui` | 8011/3011 开发端口下执行 QE read-only Playwright | 加 loop 详情指标完整性和空值解释 oracle |
| QE read L3 | `python -m nox -s qe_read_l3` | 创建 QE run record，执行 guardrail、backend、UI | 成为 QE 数据采集/UI 一致性 L3 的基础 |
| QE archive 后端 | `python -m nox -s qe_archive_backend` | compileall + schema/repository static tests | 扩展 API、补录、outbox、worker、质量核对 coverage |
| QE archive 数据质量 | `python -m nox -s qe_archive_data_quality` | 检查 `qe_archive` schema/table/column comment、run count、outbox | 加 run-level completeness、source cleanup simulation、artifact hash 检查 |
| QE archive UI | `python -m nox -s qe_archive_ui` | 已支持 mock API 或 live dev API 的 Playwright 入口 | 用于 backfill 列表、dry-run、正式入仓、质量核对 UI 验证 |
| QE archive L3 | `python -m nox -s qe_archive_l3` | 创建 run record，执行 guardrail/backend/data_quality/UI | 成为未来 QE 数仓全流程 L3/L4 主入口 |
| Paper v2 live | `python -m nox -s paper_v2_live` | 交易时段 catch-up-to-live 验证 | 作为 L4/L5 受控可选门禁，不应默认阻塞非实时研发 |
| 通用记录 | `python scripts/aistock_validate.py record` | 根据模板创建 Markdown run record | 扩展为同时写 JSON metadata、step、coverage、evidence manifest |
| 端口/服务探测 | `python scripts/aistock_validate.py ports/services` | 检查 8011/8012/3011/3012/TDX 服务状态 | 增加环境快照、生产端口保护、服务版本 hash |

当前成熟化路线应采用“nox 编排 + aistock_validate 元数据 + 模块矩阵 + 历史 evidence”的模式。测试中心 UI 后续只调用这些受控计划，例如 `paper_v2_l3`、`qe_archive_l3`、`qe_data_completeness_l3`，不直接拼接任意命令。

### 2.3 主要缺口



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

## 6. 基于现有程序的成熟流水线架构

未来专门测试系统不是替代当前脚本，而是在现有执行层之上增加“计划、调度、元数据、可观测、门禁、版本”的管理层。推荐架构如下：

```text
Test Management UI / Validation API
  -> 读取 tests/aistock_validation/catalog + modules + history
  -> 选择受控测试计划：l0 / paper_v2_l3 / qe_read_l3 / qe_archive_l3 / qe_data_completeness_l3 / L5
  -> 写入 test_plan / test_run / test_run_step / evidence manifest
        |
        v
现有权威执行层，不替代
  -> noxfile.py sessions
  -> scripts/aistock_validate.py record / ports / services / future run-json
  -> pytest / pytest-cov / Playwright / data-quality smoke / guardrail scan
        |
        v
现有证据目录 + 后续结构化存储
  -> tests/aistock_validation/history/<module>/*.md
  -> tmp/*_smoke.json
  -> tmp/playwright-report / tmp/playwright-results
  -> future JSON run metadata / coverage html+xml+json / evidence manifest
```

### 6.1 分层职责

| 层级 | 基于现有资产 | 未来增强 | 不能做的事 |
|---|---|---|---|
| 测试定义层 | `tests/aistock_validation/catalog/test_levels.md`、`tests/aistock_validation/modules/*.md` | 增加机器可读 YAML/JSON case catalog，与 Markdown 双向引用 | 不把测试用例只写在 UI 数据库里，导致仓库不可复现 |
| 编排层 | `noxfile.py` | 增加 coverage session、L4/L5 session、受影响模块选择、统一 step 输出 | 不新建一套绕过 nox 的隐式执行器 |
| 辅助命令层 | `scripts/aistock_validate.py` | 从 record/ports/services 扩展到 run metadata、service snapshot、evidence manifest、release gate | 不让 UI 拼接任意 shell 或绕过端口保护 |
| 后端测试层 | `backend/tests/*` + pytest | 引入 pytest-cov、branch coverage、diff coverage、模块阈值 | 不用 L3/UI 替代 parser/config/cost/ledger 单元测试 |
| 前端测试层 | `frontend/playwright.config.ts`、`frontend/tests/*`、`npm run test:e2e` | 增加组件/状态测试、统一错误监听 fixture、UI path coverage | 不把 Playwright 当成代码覆盖率门禁的唯一来源 |
| 数据质量层 | `scripts/aistock_data_quality_smoke.py`、`scripts/qe_archive_data_quality_smoke.py` | 扩展成模块化 business oracle，输出 JSON 可入库 | 不把 DB smoke 简化成“服务返回 200” |
| 证据层 | `tests/aistock_validation/history/*`、`tmp/*` | Markdown + JSON 双写，关联 git commit、coverage、trace、DB/API evidence | 不允许口头报告替代 run record |
| UI 管理层 | 暂未实现 | 查询/触发受控计划、展示趋势、失败、coverage、artifact | 不直接接管业务服务，不自动重启生产 8001 |

### 6.2 当前 nox session 的模板化方向

现有 session 已经形成三类模板，后续新增模块应复制模板而不是自由发挥：

| 模板 | 现有样板 | 标准步骤 | 适用模块 |
|---|---|---|---|
| 后端回归模板 | `paper_v2_backend`、`qe_archive_backend` | compileall 可选 -> targeted pytest -> pytest-cov -> JSON summary | service/repository/parser/schema/API |
| 数据质量模板 | `paper_v2_data_quality`、`qe_archive_data_quality` | read-only DB smoke -> JSON output -> fail/warn 分类 -> evidence manifest | ledger、archive、market data、Qlib、QE completeness |
| UI E2E 模板 | `paper_v2_ui`、`qe_read_ui`、`qe_archive_ui` | ports -> services -> tsc -> Playwright -> report/trace | 所有用户路径和回归 UI |
| L3 串联模板 | `paper_v2_l3`、`qe_read_l3`、`qe_archive_l3` | record -> guardrail -> backend -> data_quality -> UI 可选 | 模块全流程回归 |
| Live/L4 模板 | `paper_v2_live` | service probe -> isolated business validation -> strict result oracle | 交易时段、跨模块、受控长流程 |

新增 `qe_data_completeness_l3`、`qe_archive_independence_l4`、`release_candidate_l5` 时，应复用这些模板：先创建 run record，再执行 guardrail，再执行 targeted backend/coverage，再执行数据质量，再执行 UI/API/DB 业务 oracle。

### 6.3 `scripts/aistock_validate.py` 的演进接口

当前 `aistock_validate.py` 已有 `record`、`ports`、`services` 三个子命令。后续建议在不破坏现有用法的前提下扩展：

| 子命令 | 状态 | 建议能力 |
|---|---|---|
| `record` | 已有 | 保留 Markdown 输出；新增 `--json-out` 或默认旁路写 `*.json`，记录 git、operator、module、level、title、start/end |
| `ports` | 已有 | 增加 `--forbid-production 8001` 默认保护、端口 owner 探测、dev service reuse 说明 |
| `services` | 已有 | 增加 `/health`、`/openapi.json`、版本/commit、router 可用性、可选 TDX/QE worker mock 探测 |
| `run` | 未来新增 | 按计划名调用受控 nox session，写 step start/end/status，不接受任意 shell |
| `evidence` | 未来新增 | 收集 coverage、Playwright report、smoke JSON、DB smoke output、artifact hash，生成 evidence manifest |
| `coverage` | 未来新增 | 解析 coverage XML/JSON，执行模块阈值和 diff coverage gate |
| `release-gate` | 未来新增 | 汇总 L0-L5、coverage、data quality、asset safety、残余风险，生成发布候选报告 |

这样可以逐步把现有 Markdown run record 升级成机器可读流水线，而无需推翻当前目录和 nox 入口。

### 6.4 测试管理 UI 的边界

未来测试管理 UI 只做“可观测、可管理、可复用、可版本化”，不做不受控执行：

| 页面 | 基于现有数据 | MVP 能力 | 成熟能力 |
|---|---|---|---|
| 测试总览 | `tests/aistock_validation/history`、future JSON metadata | 最近运行、通过率、失败列表 | 趋势、模块健康度、flaky、阻塞风险 |
| 测试用例库 | `tests/aistock_validation/modules/*.md` | 展示模块矩阵和命令 | YAML/JSON case catalog、设计文档反链 |
| 运行计划 | nox session allowlist | 选择 `l0`/模块 L3/数据质量 smoke | 参数化计划、依赖关系、受影响模块推荐 |
| 运行详情 | run record、coverage、smoke JSON、Playwright report | 命令、耗时、失败、trace 链接 | step 级日志、失败分类、重跑建议 |
| 覆盖率看板 | future pytest-cov/Vitest reports | 展示 line/branch/diff coverage | 模块阈值、趋势、未覆盖高风险分支 |
| 发布候选 | L5 run record | 显示是否满足门禁 | 版本对比、残余风险签收、发布证据包 |

安全边界：UI 必须通过 allowlist 计划触发后端；默认禁止生产端口 `8001` 重启；涉及交易时段、live validation、数据清理、归档写入的测试必须二次确认并显示影响范围。

### 6.5 证据结构化标准

在保留现有 Markdown 的同时，每次 run 应逐步生成机器可读 JSON：

```json
{
  "schema_version": "aistock_validation_run_v1",
  "module": "qe_archive",
  "level": "L3",
  "git_commit": "...",
  "started_at": "...",
  "finished_at": "...",
  "status": "passed|failed|partial|skipped",
  "environment": {"backend_port": 8011, "frontend_port": 3011, "tdx_port": 19080},
  "steps": [
    {"name": "l0", "command": "python -m nox -s l0", "status": "passed", "duration_seconds": 0}
  ],
  "coverage": {"line": null, "branch": null, "diff_line": null, "diff_branch": null},
  "quality_gates": [],
  "evidence": [],
  "residual_risks": []
}
```

该 JSON 后续可以入库并驱动测试管理 UI；Markdown 继续作为人类可读审计记录。

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

## 8. 基于现有成果的落地阶段

落地路线不从 0 开始，而是把现有 `noxfile.py`、`scripts/aistock_validate.py`、模块矩阵、历史 run record、Playwright、数据质量 smoke 逐步产品化。

### Phase 0 - 现有资产基线盘点与冻结

- 将当前 nox session、模块矩阵、Playwright 用例、数据质量 smoke、run record 模板登记为测试平台 v0 基线。
- 明确 `noxfile.py` 是权威执行入口，测试中心 UI 只能调度 allowlist session。
- 为每个现有 session 补充 owner、模块、层级、是否需要 dev backend/frontend、是否允许跳过 UI、是否读写 DB、是否可在交易时段运行。
- 输出 `tests/aistock_validation/catalog` 机器可读补充文件，例如 `sessions.json` 或 `sessions.yaml`。

验收：无需新增业务测试，就能从文档/JSON 列出当前所有可执行计划、命令、环境变量、证据输出位置和生产隔离要求。

### Phase 1 - Run metadata 与 evidence manifest

- 扩展 `scripts/aistock_validate.py record`，在现有 Markdown 旁边生成 JSON run metadata。
- 为 nox L3 session 增加统一 step 记录：l0/backend/data_quality/ui/coverage/release_gate。
- 统一收集 `tmp/*_smoke.json`、Playwright report/trace、coverage report、DB smoke output、guardrail result。
- 保留现有 `tests/aistock_validation/history/<module>/*.md`，不迁移历史文件；新 JSON 与 Markdown 同目录或 `tmp/validation_runs` 双写。

验收：执行一次 `paper_v2_l3` 或 `qe_archive_l3` 后，除 Markdown 外还能得到机器可读 JSON，UI/后端未来可直接解析。

### Phase 2 - Coverage gate 接入现有 nox backend session

- 在 `paper_v2_backend`、`qe_archive_backend`、未来 `qe_data_completeness_backend` 中接入 `pytest-cov`，先记录 baseline，不阻断历史全仓低覆盖。
- 对新增/修改代码启用 diff coverage gate；高风险模块先从 80% line / 70% branch 开始。
- 增加 coverage 解析命令到 `aistock_validate.py coverage`，输出 coverage snapshot JSON。
- 前端先保留 TypeScript + Playwright；新增 Vitest/Testing Library 时只覆盖新组件和状态工具，避免一次性重构全前端测试框架。

验收：任意高风险后端变更的 run record 中必须有 coverage snapshot；如果 coverage 不达标，L3/L5 显示 `coverage_gate_failed`。

### Phase 3 - 数据质量 smoke 模块化

- 将 `scripts/aistock_data_quality_smoke.py` 和 `scripts/qe_archive_data_quality_smoke.py` 抽象成可复用结构：check result、PASS/WARN/FAIL、strict scope、JSON output、residual risk。
- 为 QE 数据完整性增加 smoke：effective config 完整率、required metrics、artifact manifest、成本对账、source cleanup independence。
- 为 Qlib/market data 增加 smoke：交易日覆盖、分钟数据覆盖、suspend/limit/pre_close freshness、研究有效性标记。
- 数据质量 smoke 仍默认只读；任何写入型修复必须单独确认，不进入默认 L3。

验收：L3 不只验证 API 200，而能输出业务质量报告，例如 ledger 一致性、archive completeness、QE required field coverage。

### Phase 4 - 测试中心 UI MVP

- UI 首先读取现有 Markdown/JSON run record，不要求先建设复杂调度器。
- 提供测试总览、模块矩阵、运行历史、运行详情、coverage 看板、数据质量报告、Playwright trace 链接。
- 触发测试时只允许选择 allowlist nox session；初期可生成命令供人工复制执行，成熟后再由后端受控执行。
- 所有运行必须显示端口、环境变量、生产隔离状态和是否会写 DB/触发归档。

验收：用户可以在 UI 查看 `paper_v2_l3`、`qe_read_l3`、`qe_archive_l3` 历史证据和失败原因，并能按模块/日期/commit 查询。

### Phase 5 - L4/L5 发布候选门禁

- 基于现有 L3 session 组合 L4：QE 数据采集 -> QE archive -> source cleanup simulation -> Paper/Selection 只读消费。
- 新增 `release_candidate_l5` nox session，串联 L0、受影响模块 backend/coverage/data_quality/UI、资产安全、DB comment 检查、残余风险汇总。
- 版本候选报告引用已有 history run record 和新 JSON metadata，不重复手工整理。
- Live/交易时段测试作为可选强门禁：例如 `paper_v2_live -- --require-live-bars`，只在需要验证实时链路时执行。

验收：准备提交或发布前，能自动生成一份 L5 报告，说明通过/失败、覆盖率、数据质量、资产安全、需要生产重启与否、残余风险。

### Phase 6 - 长期治理

- 定期清理 flaky test、失效历史样本、过时矩阵项；所有跳过项必须有 owner 和到期时间。
- 将测试管理 UI 的数据模型持久化；新增表/字段必须遵守 PostgreSQL comment 规范。
- 支持版本趋势：覆盖率趋势、失败热区、模块健康度、数据质量趋势、回归耗时趋势。
- 支持 LLM agent 只读查询测试历史和失败模式，但不得让 agent 直接执行未授权命令。

验收：测试平台成为研发过程的固定入口，而不是某次任务临时脚本集合。

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
