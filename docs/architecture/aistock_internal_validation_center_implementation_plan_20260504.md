# AIstock 内置自动化测试流水线实施方案

> 日期：2026-05-04
> 状态：详细实施方案 v1.1，补充开发提交、夜间任务、Bug 管理与 Codex/Claude 修复闭环
> 文档位置：`docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`
> 依赖顶层设计：`docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`
> 依赖开发规范：`docs/standards/aistock_development_standard_v1.0_20260504.md`；Guardrail 落地设计：`docs/architecture/aistock_development_standards_and_guardrails_20260504.md`
> 适用范围：AIstock 仓库内的 FastAPI backend、Next.js frontend、QE 数据完整性、QE archive/未来数仓、Paper Trading v2、Selection Center、HMM、Qlib 数据链路与本地发布候选验证。
> 明确边界：本方案只设计 AIstock 内置自动化测试流水线；不创建独立微服务；不重启生产 `8001`；不重启远端机 API；不接入任意 shell 执行；不直接执行长耗时真实实验。

## 1. 执行结论

AIstock 自动化测试流水线应在现有成果上成熟化，而不是从 0 建立新系统。

推荐路线：

```text
设计文档/代码变更
  -> tests/aistock_validation 中的测试矩阵和计划
  -> noxfile.py 权威编排
  -> scripts/aistock_validate.py 记录 metadata / evidence / coverage / gate
  -> pytest + pytest-cov / Playwright / 数据质量 smoke / guardrail scan
  -> tests/aistock_validation/history 证据归档
  -> AIstock 内置 Validation Center API + UI 查询、对比、受控触发
```

关键结论：

1. **不独立微服务**：后端放在现有 FastAPI，建议新增 `backend/routers/validation.py` 和 `backend/services/validation/`；前端放在现有 Next.js，建议新增 `frontend/src/app/validation-center/`。
2. **先读后写**：第一阶段 UI/API 只读取现有 Markdown/JSON run record、coverage、evidence；等安全边界验证后再做受控执行。
3. **nox 是权威执行层**：UI/API 只能调度 allowlist 中的 `nox` session 或 `aistock_validate` plan，不能提交任意 shell。
4. **覆盖率先落地**：当前 run metadata 已有 coverage 字段，但没有真实覆盖率采集。下一批代码优先实现 `pytest-cov`、coverage 解析、阈值门禁和 evidence 归档。
5. **长耗时回测不阻塞每次提交**：用 contract/golden payload/mock worker/mini backtest/historical replay/nightly long-run 分层验证，真实长 QE 实验只进入 L4/L5 或计划任务。
6. **设计阶段必须写测试用例**：未来所有架构和开发方案都必须包含测试矩阵、oracle、自动化路径、证据路径和覆盖率要求；没有测试设计的方案不能进入实现。
7. **提交分层**：非长耗时、非交易时段依赖功能，必须在相关流水线通过后再提交和推送；长耗时/市场依赖功能先通过快速门禁，进入 nightly/L4/L5 后台验证，未通过前不得标记为完成或进入生产开关。
8. **Bug 闭环机器可读**：Bug 的权威生命周期建议以 GitHub Issues 为主，Validation Center DB/JSON 作为本地索引和证据缓存；Codex/Claude 通过 GitHub CLI/API 或 Validation API 读取上下文、修复、回写状态和验证证据。
9. **开发规范直接进入流水线**：使用 `docs/standards/aistock_development_standard_v1.0_20260504.yaml` 和全仓只读 baseline scan，先阻断 changed-files 的 P0/P1 新违规，再逐步治理历史技术债。

## 2. Phase 0 文档发现与允许使用的现有能力

### 2.1 已读取和引用的本地资料

| 来源 | 已确认内容 |
|---|---|
| `docs/codex_project_memory.md` | 项目规则、生产隔离、DB comment 规范、QE 数据完整性进展、测试标准。 |
| `docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md` | 顶层自动化测试、覆盖率、可观测 UI、L0-L5 分层和成熟化路线。 |
| `docs/architecture/qe_data_completeness_phase1_development_plan_20260504.md` | QE completion contract、artifact manifest、run metadata/evidence 第一阶段边界。 |
| `tests/aistock_validation/catalog/test_levels.md` | 当前 L0-L5 测试等级定义。 |
| `tests/aistock_validation/modules/qe_data_completeness.md` | QE 数据完整性第一阶段验证矩阵。 |
| `tests/aistock_validation/modules/qe_archive.md` | QE archive 的生产隔离、后端/API/UI/数据质量矩阵。 |
| `tests/aistock_validation/templates/test_run_record.md` | 当前 Markdown run record 模板。 |
| `scripts/aistock_validate.py` | 当前已有 `record`、`evidence`、`ports`、`services` 子命令；JSON metadata/evidence manifest 已实现；coverage 子命令尚未实现。 |
| `noxfile.py` | 当前已有 `l0`、`paper_v2_backend`、`paper_v2_data_quality`、`paper_v2_ui`、`paper_v2_l3`、`qe_read_backend`、`qe_read_ui`、`qe_read_l3`、`qe_archive_backend`、`qe_data_contract_backend`、`qe_archive_data_quality`、`qe_archive_ui`、`qe_archive_l3`、`paper_v2_live`。 |
| `backend/main.py` | 当前通过 `app.include_router(..., prefix="/api/v1")` 注册业务 router；新增 validation router 应复用该模式。 |
| `backend/routers/qe_archive.py` | 现有 FastAPI router + Pydantic request/response 模式、分页参数、confirm text 写入保护、质量查询样板。 |
| `frontend/src/app/qe-archive/page.tsx` 与 `frontend/src/lib/qe-archive/api.ts` | 现有内部工具页、API client、分页、dry-run/confirm、quality panel 的 UI 样板。 |
| `frontend/package.json` 与 `frontend/playwright.config.ts` | 当前前端以 Playwright E2E 和 `tsc --noEmit` 为主要验证入口，尚无独立组件覆盖率门禁。 |
| `requirements-dev.txt` | 当前有 `nox`、`pytest-html`、`semgrep`，未看到 `pytest-cov`，下一阶段需补充或在环境中验证。 |
| GitHub Issues 官方文档 | Issues 支持 labels、assignees、milestones、Projects 等计划跟踪能力，适合作为 Bug 生命周期的仓库级权威记录。 |
| GitHub issue forms 官方文档 | Issue form 可用 YAML 结构化采集缺陷描述、触发条件、影响范围、复现步骤、日志和验证命令。 |
| GitHub REST Issues API 官方文档 | Codex/Claude 或流水线可通过 API 创建、更新、评论、关闭 issue，并同步 labels/state/assignee。 |
| GitHub Actions schedule 官方文档 | workflow 支持 cron schedule；AIstock 因依赖本地 DB/TDX/WSL/GPU，优先实现本地 scheduler，但保留同类 schedule/manual-dispatch 语义。 |
| pytest-cov 官方文档 | pytest-cov 支持 coverage 报告输出，适合作为后端 Python line/branch coverage gate 的基础。 |

### 2.2 允许使用的现有执行入口

| 类型 | 当前允许入口 | 后续扩展方式 |
|---|---|---|
| 静态门禁 | `python -m nox -s l0` | 增加 coverage 配置检查、secret baseline、DB comment 检查。 |
| 后端单元/组件 | `python -m nox -s qe_data_contract_backend`、`qe_archive_backend`、`paper_v2_backend` | 接入 `pytest-cov`，输出 coverage XML/JSON/HTML。 |
| 数据质量 | `python -m nox -s qe_archive_data_quality`、`paper_v2_data_quality` | 输出机器可读 smoke JSON，并纳入 evidence manifest。 |
| UI E2E | `python -m nox -s qe_archive_ui`、`qe_read_ui`、`paper_v2_ui` | 强制 `tsc`、Playwright trace、console/page/request failure 捕获。 |
| L3 串联 | `qe_archive_l3`、`qe_read_l3`、`paper_v2_l3` | 标准化 step metadata、coverage gate、evidence manifest。 |
| run 记录 | `python scripts/aistock_validate.py record` | 已写 Markdown + JSON；下一步记录 steps、coverage、quality gates。 |
| evidence 记录 | `python scripts/aistock_validate.py evidence` | 已支持文件、coverage、Playwright、smoke、artifact evidence。 |
| 端口保护 | `python scripts/aistock_validate.py ports/services` | 增加生产端口拒绝、环境快照、服务版本快照。 |

### 2.3 当前禁止假设的能力

| 禁止假设 | 原因 |
|---|---|
| `aistock_validate.py coverage` 已存在 | 当前只有 `record/evidence/ports/services`，coverage 解析需要新增。 |
| UI 可以执行任意 shell | 安全风险高，必须 allowlist command_key。 |
| 可以重启生产 `8001` | 明确禁止，开发验证只使用 `8011/8012`、`3011/3012`。 |
| 可以重启远端机 API | 明确禁止，远端 API 只作为后续实验被动依赖。 |
| 可以直接读取 WSL/远端 workspace 文件 | QE/RD-Agent 红线：必须通过 API 或 AIstock-owned artifact store。 |
| 覆盖率已经统计 | 当前 metadata 字段存在但值为空，尚无 `pytest-cov` 门禁。 |

## 3. 目标架构

### 3.1 内置 Validation Center 总体结构

```text
AIstock FastAPI backend
  backend/routers/validation.py
  backend/services/validation/
    plan_catalog.py          # allowlist 测试计划定义
    history_store.py         # 读取 tests/aistock_validation/history Markdown/JSON
    coverage_parser.py       # 解析 coverage xml/json
    evidence_store.py        # evidence manifest 聚合
    run_service.py           # run list/detail/summary，后续受控执行
    execution_service.py     # 后续：只执行 allowlist nox session
    models.py                # Pydantic response/request contract

AIstock Next.js frontend
  frontend/src/app/validation-center/
    page.tsx                 # run 列表、详情、coverage、evidence、quality gates
    layout.tsx
  frontend/src/lib/validation/api.ts
  frontend/tests/validation-center/
    validation-center.spec.ts

Existing execution layer
  noxfile.py
  scripts/aistock_validate.py
  tests/aistock_validation/catalog
  tests/aistock_validation/modules
  tests/aistock_validation/history
```

### 3.2 分层职责

| 层级 | 职责 | 第一阶段是否实现 |
|---|---|---|
| 测试定义层 | 定义模块、case、plan、level、oracle、运行条件 | 是，先用 repo 内 Markdown/YAML/JSON 文件。 |
| 执行编排层 | 用 nox session 串联 pytest/Playwright/smoke/guardrail | 是，扩展现有 `noxfile.py`。 |
| 元数据层 | 记录 run metadata、steps、coverage、quality gates、evidence | 是，扩展 `aistock_validate.py`。 |
| 后端查询层 | 列出 run、查看详情、聚合 coverage/失败/证据 | 是，先只读。 |
| 后端受控执行层 | 从 allowlist 触发计划、流式日志、取消、超时 | 第二阶段后实现。 |
| 前端可观测 UI | 显示 run、计划、证据、覆盖率、失败分类 | 是，先只读 MVP。 |
| 调度层 | 夜间/发布候选/长耗时测试计划 | 后续阶段实现，不影响当前生产。 |

### 3.3 存储策略

第一阶段不急于建 DB，优先复用 repo 内证据目录，降低引入成本：

```text
tests/aistock_validation/
  catalog/
    test_levels.md
    test_plans.yaml          # 新增，机器可读 allowlist
  modules/
    qe_data_completeness.md
    qe_archive.md
  history/
    <module>/
      *.md                   # 人类审计记录，提交 Git
      *.json                 # run metadata，轻量时可提交 Git
      *.evidence.json        # evidence manifest，轻量时可提交 Git

tmp/validation/
  coverage/<run_id>/         # coverage.xml/json/html，不提交 Git
  playwright/<run_id>/       # trace/report，不提交 Git
  logs/<run_id>/             # 执行日志，不提交 Git
  bugs/<bug_id>/             # 本地缺陷上下文、复现日志摘要、agent prompt，不提交大文件
```

Bug 存储建议采用混合模式：

- **权威记录**：GitHub Issues。原因是 issue 能天然关联 commit/PR、labels、assignee、milestone、closing keywords，Codex/Claude 也可通过 `gh` 或 REST API 更新。
- **本地索引**：Validation Center DB 或 JSON cache。原因是 UI 需要快速展示失败运行、触发条件、证据、状态、修复时间、是否已提交、验证记录，并且夜间流水线可能在本地先产生事件。
- **证据存储**：run metadata、evidence manifest、coverage、Playwright trace、smoke JSON、日志摘要。大 trace/log 不进 GitHub issue body，也不进 DB 大字段，只保存 URI/hash/摘要。
- **不推荐 DB-only**：只存在本地 DB 会降低与 Git commit、GitHub 历史、Codex/Claude 工作流的关联能力，也不利于跨工具追踪。

第二阶段如需支持 UI 高效查询、趋势统计、并发运行、历史持久化，再增加 `validation` PostgreSQL schema。所有表和字段必须带 `COMMENT ON TABLE` / `COMMENT ON COLUMN`。

建议预留 DB 结构：

| 表 | 用途 | 关键字段 |
|---|---|---|
| `validation.test_plan` | 测试计划 allowlist 的结构化快照 | `plan_key`、`module`、`level`、`command_key`、`default_params_json`、`safety_profile_json`、`enabled`。 |
| `validation.test_case` | 测试用例目录和 oracle | `case_id`、`plan_key`、`module`、`level`、`risk_level`、`oracle_type`、`doc_path`、`automation_path`。 |
| `validation.test_run` | 一次流水线运行 | `run_id`、`plan_key`、`status`、`git_commit`、`branch_name`、`operator`、`environment_json`、`started_at`、`finished_at`。 |
| `validation.test_run_step` | 运行步骤 | `step_id`、`run_id`、`step_key`、`command_key`、`status`、`return_code`、`duration_seconds`、`output_excerpt`。 |
| `validation.coverage_snapshot` | 覆盖率快照 | `run_id`、`tool`、`line_pct`、`branch_pct`、`diff_line_pct`、`diff_branch_pct`、`threshold_json`、`report_uri`。 |
| `validation.quality_gate_result` | 门禁结果 | `run_id`、`gate_key`、`status`、`severity`、`detail_json`、`evidence_uri`。 |
| `validation.test_evidence` | 证据 artifact 索引 | `evidence_id`、`run_id`、`kind`、`uri`、`sha256`、`size_bytes`、`summary_json`。 |
| `validation.scheduled_plan` | 后续夜间/发布计划 | `schedule_id`、`plan_key`、`cron_expr`、`enabled`、`max_duration_seconds`、`safety_profile_json`。 |
| `validation.bug_report` | Bug 本地索引和 UI 查询缓存 | `bug_id`、`github_issue_number`、`title`、`module`、`severity`、`status`、`fingerprint`、`first_seen_run_id`、`latest_run_id`、`trigger_summary`、`fixed_commit`、`fixed_at`、`submitted_at`。 |
| `validation.bug_event` | Bug 状态变化和 agent 操作轨迹 | `event_id`、`bug_id`、`event_type`、`actor_type`、`actor_name`、`from_status`、`to_status`、`event_time`、`detail_json`。 |
| `validation.bug_run_link` | Bug 与流水线 run/test case/evidence 关联 | `bug_id`、`run_id`、`case_id`、`plan_key`、`evidence_id`、`relation_type`。 |
| `validation.bug_fix_verification` | 修复验证结果 | `bug_id`、`fix_commit`、`verification_run_id`、`verification_status`、`coverage_snapshot_id`、`verified_at`、`residual_risk_json`。 |

DB 不是第一阶段前置条件；但一旦实现 DB schema，必须同时实现 comment smoke test，复用 QE archive 的 column comment 检查模式。

## 4. 测试计划 allowlist 设计

### 4.1 `test_plans.yaml` 建议格式

建议新增 `tests/aistock_validation/catalog/test_plans.yaml`，作为 UI/API 和 nox 的共同 allowlist 来源：

```yaml
schema_version: aistock_validation_plans_v1
plans:
  - plan_key: qe_data_contract_backend
    title: QE data contract backend tests
    module: qe_data_completeness
    level: L2
    command_key: nox_qe_data_contract_backend
    nox_session: qe_data_contract_backend
    requires_backend: false
    requires_frontend: false
    allowed_backend_ports: []
    allowed_frontend_ports: []
    writes_database: false
    writes_artifacts: false
    max_duration_seconds: 300
    evidence_kinds: [pytest, coverage]
    coverage_required: true
    default_env: {}
```

### 4.2 command_key 规则

- `command_key` 是固定枚举，不能由用户输入 shell。
- 后端 `ExecutionService` 只能把 `command_key` 映射到预定义 `nox` session。
- 所有参数必须走白名单校验：端口只能是 `8011/8012/3011/3012`，写 DB 测试必须显式标记，长耗时测试必须显式确认。
- UI 第一阶段只展示命令和历史；第二阶段才允许点击执行。

### 4.3 首批计划清单

| plan_key | 层级 | 运行内容 | 第一阶段状态 |
|---|---|---|---|
| `l0` | L0 | guardrail/static baseline | 已有，需纳入 catalog。 |
| `qe_data_contract_backend` | L2 | QE completion contract + validation metadata tests | 已有，优先接 coverage。 |
| `qe_archive_backend` | L2 | QE archive schema/repository/contract tests | 已有，接 coverage。 |
| `qe_archive_data_quality` | L2 | QE archive DB smoke/comment/outbox | 已有，接 evidence manifest。 |
| `qe_archive_ui_mock` | L3 | QE archive UI mocked API E2E | 已有路径，纳入 catalog。 |
| `qe_archive_l3` | L3 | QE archive L3 串联 | 已有，标准化 metadata/coverage。 |
| `qe_read_l3` | L3 | QE read-only UI/API workspace 红线 | 已有，后续加 loop 指标完整性 oracle。 |
| `paper_v2_backend` | L2 | Paper v2/Selection/StrategyPackage tests | 已有，接 coverage。 |
| `paper_v2_l3` | L3 | Paper v2 L3 串联 | 已有，标准化 metadata/coverage。 |
| `release_candidate_l5` | L5 | 受影响模块和发布候选报告 | 后续新增。 |

## 5. 分阶段实施计划

### Phase 1 - Coverage 与 run metadata 完整化

目标：把“通过测试”升级为“通过测试 + 有覆盖率 + 有证据 + 有结构化 metadata”。

第一批变更清单：

| 文件/目录 | 变更 |
|---|---|
| `requirements-dev.txt` | 增加 `pytest-cov`，如采用 diff coverage 再增加 `diff-cover`。 |
| `scripts/aistock_validate.py` | 新增 `coverage` 子命令，解析 coverage JSON/XML，输出 coverage snapshot，并可更新 run metadata。 |
| `noxfile.py` | 新增 `_run_pytest_with_coverage()` helper；先接入 `qe_data_contract_backend`，再推广到 `qe_archive_backend`、`paper_v2_backend`。 |
| `tests/aistock_validation/catalog/test_plans.yaml` | 新增机器可读 allowlist。 |
| `backend/tests/test_aistock_validate_coverage.py` | 覆盖 coverage parser、threshold、metadata update、缺报告失败。 |
| `tests/aistock_validation/modules/qe_data_completeness.md` | 补充 coverage gate 验收。 |

验收标准：

- `python -m nox -s qe_data_contract_backend` 生成 coverage XML/JSON/HTML 到 `tmp/validation/coverage/<run_id>/`。
- `scripts/aistock_validate.py coverage` 能解析 coverage 并输出 snapshot JSON。
- run metadata 中 `coverage.line`、`coverage.branch` 至少不再为空。
- 新增/修改的 QE 数据完整性代码 diff coverage 达到 Phase A 阈值：line >= 80%，branch >= 70%；历史全仓 baseline 先记录不阻断。
- 证据 manifest 包含 coverage report 路径和 sha256。

测试用例：

| ID | 层级 | 用例 | Oracle |
|---|---|---|---|
| VAL-COV-001 | L1 | 解析标准 coverage JSON | line/branch 百分比、covered/missing 行数准确。 |
| VAL-COV-002 | L1 | coverage 文件缺失 | 返回失败，不写入虚假 100%。 |
| VAL-COV-003 | L1 | threshold 不达标 | quality gate 状态为 `failed`，失败原因可读。 |
| VAL-COV-004 | L1 | 更新 run metadata | 只更新 coverage/quality_gates 字段，不破坏旧字段。 |
| VAL-COV-005 | L2 | nox session 生成 evidence | coverage XML/JSON/HTML 路径存在，manifest `missing_count=0`。 |

### Phase 2 - 测试计划 catalog 与只读后端 API

目标：让 AIstock 后端能读取测试计划、历史 run、coverage、evidence，但暂不执行测试。

第一批后端文件：

| 文件/目录 | 变更 |
|---|---|
| `backend/routers/validation.py` | 新增 `/api/v1/validation` router。 |
| `backend/services/validation/models.py` | Pydantic contracts。 |
| `backend/services/validation/plan_catalog.py` | 读取/校验 `test_plans.yaml`，暴露 allowlist。 |
| `backend/services/validation/history_store.py` | 读取 `tests/aistock_validation/history` 中 Markdown/JSON/evidence。 |
| `backend/services/validation/coverage_parser.py` | 复用 `aistock_validate.py` 或共享 parser。 |
| `backend/tests/test_validation_plan_catalog.py` | allowlist、非法 command_key、端口安全测试。 |
| `backend/tests/test_validation_history_store.py` | run 列表、详情、coverage/evidence 聚合测试。 |

建议 API：

| Method | Path | 功能 | 第一阶段 |
|---|---|---|---|
| GET | `/api/v1/validation/plans` | 测试计划列表 | 实现。 |
| GET | `/api/v1/validation/plans/{plan_key}` | 测试计划详情和安全 profile | 实现。 |
| GET | `/api/v1/validation/runs` | 历史 run 分页，支持 module/level/status/search | 实现。 |
| GET | `/api/v1/validation/runs/{run_id}` | run 详情、steps、coverage、quality gates、evidence | 实现。 |
| GET | `/api/v1/validation/summary` | 模块健康度、最近失败、coverage 趋势摘要 | 实现轻量版。 |
| POST | `/api/v1/validation/runs` | 触发执行 | 第二阶段后实现，第一阶段返回 501 或不暴露。 |
| POST | `/api/v1/validation/runs/{run_id}/cancel` | 取消运行 | 第二阶段后实现。 |

验收标准：

- API 读取现有 run history，不需要数据库。
- API 不执行任何命令，不改 DB，不启动服务。
- 非法 plan/非法 command_key/生产端口配置会在读取或校验阶段报错。
- `backend/main.py` 注册 router 后，`/openapi.json` 包含 validation API。

测试用例：

| ID | 层级 | 用例 | Oracle |
|---|---|---|---|
| VAL-API-001 | L1 | 读取合法 `test_plans.yaml` | plan_key、level、command_key、safety_profile 完整。 |
| VAL-API-002 | L1 | command_key 非 allowlist | catalog validation fail-fast。 |
| VAL-API-003 | L1 | 计划声明生产端口 `8001` | fail-fast，错误说明生产隔离。 |
| VAL-API-004 | L2 | `GET /validation/runs` | 返回分页列表，能读到 QE data completeness run。 |
| VAL-API-005 | L2 | `GET /validation/runs/{run_id}` | 返回 Markdown path、metadata、coverage、evidence。 |
| VAL-API-006 | L2 | 目录中缺 JSON metadata | 降级为 Markdown-only run，标记 `metadata_missing`，不伪造 coverage。 |

### Phase 3 - Validation Center 只读 UI MVP

目标：在 AIstock 前端内提供可观测、可管理、可复用的测试中心，但不触发执行。

第一批前端文件：

| 文件/目录 | 变更 |
|---|---|
| `frontend/src/app/validation-center/page.tsx` | 测试中心首页：概览、计划、历史 run、最近失败、coverage。 |
| `frontend/src/app/validation-center/layout.tsx` | 页面布局。 |
| `frontend/src/lib/validation/api.ts` | API client 和类型。 |
| `frontend/tests/validation-center/validation-center.spec.ts` | Playwright mocked API E2E。 |
| `frontend/src/app/layout.tsx` 或导航配置 | 增加入口，遵守现有 UI 风格。 |

UI MVP 功能：

- Run 列表：module、level、status、git commit、开始/结束时间、coverage、失败门禁。
- Run 详情：steps、commands、coverage、quality gates、evidence manifest、Markdown 证据路径。
- Plan 列表：计划名、级别、是否需要 backend/frontend、是否写 DB、最长运行时间、是否长耗时。
- Coverage 看板：line/branch/diff、阈值、趋势、未达标提示。
- 失败分析：按 `code_error/data_error/contract_error/env_error/ui_error/coverage_gate_failed` 分类。

验收标准：

- UI 使用 mocked API 可稳定跑 Playwright，不依赖生产 `8001`。
- UI 没有 pageerror、console error、requestfailed、unexpected 4xx/5xx。
- UI 不展示 raw JSON 作为主要操作视图；JSON 仅可作为高级折叠详情。
- 所有按钮状态有原因：只读阶段执行按钮必须显示“尚未启用受控执行”。

测试用例：

| ID | 层级 | 用例 | Oracle |
|---|---|---|---|
| VAL-UI-001 | L3 | 打开测试中心首页 | 概览卡、计划表、run 表加载成功。 |
| VAL-UI-002 | L3 | 过滤 module/level/status | 表格和统计同步变化。 |
| VAL-UI-003 | L3 | 打开 run 详情 | coverage、steps、evidence、quality gates 可读。 |
| VAL-UI-004 | L3 | metadata 缺失 run | 显示风险提示，不显示虚假通过。 |
| VAL-UI-005 | L3 | 执行按钮只读禁用 | 显示禁用原因，不发起 POST。 |

### Phase 4 - 受控执行 API/UI

目标：允许用户从 UI 选择 allowlist 测试计划，后端受控执行 nox session，实时展示状态和日志。

实现边界：

- 只能执行 `test_plans.yaml` 中 `enabled=true` 的计划。
- 不允许传入 shell 字符串。
- 后端必须校验端口、安全 profile、是否写 DB、是否长耗时、是否需要用户确认。
- 默认不允许生产端口 `8001`，不允许远端 API restart，不允许 WSL/远端 workspace 直读。
- 执行日志写入 `tmp/validation/logs/<run_id>/`，只提交摘要和 hash。

建议 API：

| Method | Path | 功能 |
|---|---|---|
| POST | `/api/v1/validation/runs` | 创建并启动 allowlist run。 |
| GET | `/api/v1/validation/runs/{run_id}/events` | SSE 或轮询获取 step/log/status。 |
| POST | `/api/v1/validation/runs/{run_id}/cancel` | 取消允许取消的本地进程。 |
| POST | `/api/v1/validation/runs/{run_id}/evidence/refresh` | 重新扫描 evidence manifest。 |

测试用例：

| ID | 层级 | 用例 | Oracle |
|---|---|---|---|
| VAL-RUN-001 | L2 | 触发 allowlist `qe_data_contract_backend` | 创建 run、执行 nox、写 metadata、最终 passed/failed。 |
| VAL-RUN-002 | L2 | 非 allowlist command | 400/422，绝不执行。 |
| VAL-RUN-003 | L2 | 尝试生产端口 `8001` | 400/422，说明生产隔离。 |
| VAL-RUN-004 | L2 | 超时 mock process | 自动标记 timeout，保留日志和 evidence。 |
| VAL-RUN-005 | L2 | cancel mock process | 进程终止，run 状态 `cancelled`，不会误报 passed。 |
| VAL-RUN-006 | L3/UI | UI 选择计划并执行 | 状态从 queued/running 到 passed/failed，日志实时更新。 |

### Phase 5 - 长耗时 QE/回测/实验测试体系

AIstock 很多功能需要长时间回测、训练、实验，不能把真实长任务放进每次提交的必跑链路。推荐采用“分层替代 + 定期真实验证”的策略。

| 层级 | 运行频率 | 验证方式 | 适用场景 |
|---|---|---|---|
| L1 contract/golden | 每次相关代码变更 | 固定 JSON/payload/parquet 小样本 | QE completion payload、artifact manifest、cost reconcile、parser。 |
| L2 mock worker/API | 每次相关代码变更 | mock WSL/远端 worker API，不读 workspace | QE 完成回调、增强指标采集、失败/超时/partial。 |
| L2 mini backtest | 相关核心变更 | 2-10 只股票、短时间窗、固定 seed、固定 artifact | 成本、持仓、交易、涨跌停处理、训练曲线最小链路。 |
| L3 historical replay | 模块合并前 | 从 QE DB/数仓已保存 payload replay，不跑训练 | UI/API/DB 数据一致性、补录、质量核对。 |
| L4 nightly/scheduled | 夜间或空闲时 | 真实较长 QE/回测，独立 dev 端口 | QE 数据完整性、数仓独立性、策略执行链路。 |
| L5 release candidate | 发布前 | 关键模块 + selected long-run + 残余风险 | 版本候选质量证明。 |

长任务必须具备：

- `max_duration_seconds`、超时状态、取消能力、日志 tail、checkpoint/evidence 路径。
- 可复现配置：git commit、数据 snapshot、stock universe、seed、模型/因子版本、有效策略参数。
- 对真实市场数据的依赖声明：TDX/Tushare/Qlib/PG/WSL/API 是否需要，缺失时标记 `env_error` 而非业务失败。
- 运行分级标签：`quick`、`medium`、`slow`、`requires_service`、`requires_market_data`、`requires_gpu`、`writes_db`。
- UI 中必须显示预计耗时、资源影响、是否写库、是否需要 dev backend/frontend、是否可交易时段运行。

QE 场景建议：

| 场景 | 快速自动化替代 | 真实验证频率 |
|---|---|---|
| 完成回调增强指标 | golden completion payload + mock worker API | 每周/发布前跑真实 QE loop。 |
| 持仓统计/交易明细解析 | 固定小型 artifact manifest + 小型 parquet/jsonl fixture | 夜间跑一个完整 loop。 |
| LSTM/LGB 模型训练指标 | fake/golden training diagnostics + 小样本 train | 模型代码变更时跑 mini train。 |
| QE archive 入仓 | 历史 payload replay + source cleanup simulation | 发布前跑 live dev API backfill。 |
| UI loop 详情 | mocked API + archived payload | L3 live dev UI 验证。 |

### Phase 6 - 开发提交、夜间任务与 Bug 闭环

目标：把“测试发现问题”升级为“自动登记 Bug、Codex/Claude 可复现、修复后可验证、提交状态可追踪”的闭环。

### 6.1 推荐开发提交流程

| 场景 | 提交策略 | 必跑验证 | 状态规则 |
|---|---|---|---|
| 非长耗时、非交易时段依赖功能 | 开发完成后先跑相关 L0/L1/L2，涉及 UI/API/DB 时再跑 L3，通过后再提交并推送。 | compileall/pytest/coverage/data-quality/UI E2E/guardrail 中的相关子集。 | 未通过不得标记完成；不应把失败结果提交到 `main`。 |
| 长耗时回测/训练/QE loop | 先跑快速门禁和 mock/mini/replay；真实长任务进入 nightly/L4/L5。 | L0-L3 快速门禁 + 夜间真实长任务。 | 快速门禁通过后可保存阶段性提交，但功能状态必须是 `L4 pending`；夜间未通过则自动创建 Bug。 |
| 市场/交易时段依赖任务 | 不阻塞普通开发提交；使用 scheduled/live validation 在指定时段或夜间运行。 | dev 端口服务探测、数据新鲜度、市场依赖 smoke、回放或 catch-up validation。 | 未通过时记录 `env_error` 或 `data_error`，不得误判为业务成功。 |
| 发布候选/生产开关 | 必须等待相关 L4/L5 或人工确认完成。 | release_candidate_l5 + 关键 long-run + 残余风险确认。 | 未通过不得打开生产 hook、不得清理源数据、不得宣称完成。 |

原则：

- 每个推送到 GitHub 的变更至少要有可追踪的验证记录；长任务没有跑完时，提交说明和 run metadata 必须明确 `long_run_pending=true`。
- 夜间任务发现 Bug 后，不要求立即阻断所有开发，但必须自动生成 Bug、关联失败 run、给出复现命令和证据；修复后必须新增或复用回归测试。
- 严重级别 P0/P1 的夜间 Bug 应触发保护动作：禁止 release candidate 通过，必要时建议 revert 或禁用相关 feature flag。

### 6.2 Bug 权威记录与本地索引

Bug 不建议只记录在数据库中。推荐模式：

```text
Validation run failed
  -> 生成 failure fingerprint
  -> 查询本地 validation.bug_report / GitHub Issues
  -> 已存在：追加 run/evidence/comment，更新 last_seen/status
  -> 不存在：创建 GitHub Issue + 本地索引
  -> Codex/Claude 读取 issue + agent context
  -> 修复代码 + 运行复现和回归流水线
  -> 更新 issue/comment/labels/status + bug_fix_verification
  -> 验证通过后关闭 issue 或标记 verified
```

| 存储位置 | 角色 | 内容 | 是否权威 |
|---|---|---|---|
| GitHub Issue | 缺陷生命周期权威记录 | 标题、描述、触发条件、复现步骤、labels、assignee、状态、修复 commit、讨论记录。 | 是。 |
| Validation DB/JSON | 本地 UI 查询和自动化索引 | bug_id、fingerprint、run_id、plan_key、case_id、evidence_uri、状态镜像、修复验证记录。 | 否，是 cache/index。 |
| run/evidence artifact | 技术证据 | 日志摘要、coverage、trace、smoke JSON、截图、DB 检查摘要、hash。 | 对证据权威。 |
| Git commit / PR | 修复事实 | commit message、linked issue、验证记录路径。 | 对代码变更权威。 |

### 6.3 Bug 字段与状态机

Bug 最小字段：

| 字段 | 含义 |
|---|---|
| `bug_id` | AIstock 本地 Bug ID，例如 `bug_20260504_001`。 |
| `github_issue_number` / `github_issue_url` | GitHub Issue 关联。 |
| `title` / `description` | 人类可读问题描述。 |
| `module` / `severity` / `risk_area` | 模块、严重级别、风险域，例如 QE、archive、Paper v2、data、UI。 |
| `trigger_condition` | 触发条件：plan_key、case_id、输入参数、数据日期、模型/因子版本、seed、端口、环境。 |
| `reproduce_command` | 最小复现命令，必须可复制执行。 |
| `failing_run_id` / `evidence_uri` | 首次失败 run 和证据。 |
| `fingerprint` | 去重指纹：plan_key + case_id + exception class + stack hash + data hash。 |
| `status` | 当前状态。 |
| `assigned_agent` | 负责修复的 Codex/Claude/人工。 |
| `fix_branch` / `fix_commit` | 修复分支和提交。 |
| `verification_run_id` | 修复后的验证 run。 |
| `fixed_at` / `submitted_at` / `closed_at` | 修复、提交、关闭时间。 |

建议状态机：

```text
detected -> triaged -> assigned -> fixing -> fix_submitted
  -> verification_running -> verified -> closed
  -> reopened

旁路状态：
duplicate / cannot_reproduce / wontfix / environment_issue / data_issue
```

状态规则：

- `detected`：流水线自动发现，未人工/agent 分析。
- `triaged`：已确认模块、严重级别、复现路径。
- `assigned`：已指定 Codex/Claude/人工。
- `fix_submitted`：已有修复 commit，但验证未完成。
- `verified`：复现用例和相关回归通过。
- `closed`：已关闭 GitHub Issue，且本地索引同步。
- `reopened`：同 fingerprint 或同 issue 再次失败。

### 6.4 UI 展示边界

Validation Center UI 主要做展示和可观测，不做复杂人工工作流系统。

UI 必须展示：

- Bug 列表：标题、模块、严重级别、状态、首次发现时间、最后出现时间、失败次数、是否已修复、是否已提交。
- Bug 详情：触发条件、复现命令、失败 run、evidence、日志摘要、相关 commit、修复验证 run、残余风险。
- 状态时间线：detected/triaged/assigned/fix_submitted/verified/closed 的时间和 actor。
- Agent 上下文：一键复制给 Codex/Claude 的修复上下文，包括复现命令、证据路径、相关文件、禁止操作、验证命令。
- 外部链接：打开 GitHub Issue、相关 commit、run record、Playwright trace、coverage report。

UI 不应做：

- 不允许直接执行任意 shell。
- 不允许直接把 Bug 标记为修复，除非有对应 verification_run_id。
- 不允许编辑大段 raw JSON 作为主要交互。
- 不允许替代 GitHub Issue 的权威历史；UI 只同步和展示。

### 6.5 Codex/Claude 访问与更新协议

为了让 Codex/Claude 高效修复 Bug，Validation Center 应提供机器可读上下文：

| 接口/文件 | 用途 |
|---|---|
| `GET /api/v1/validation/bugs` | 列出待修复 Bug，支持 status/severity/module/agent 过滤。 |
| `GET /api/v1/validation/bugs/{bug_id}` | 获取 Bug 详情、触发条件、失败 run、证据摘要。 |
| `GET /api/v1/validation/bugs/{bug_id}/agent-context` | 返回专门给 Codex/Claude 的修复上下文、允许修改范围、复现命令、验证命令。 |
| `POST /api/v1/validation/bugs/{bug_id}/events` | agent 写入状态事件、分析结论、修复 commit、验证 run。 |
| `tests/aistock_validation/bugs/<bug_id>.json` | 可选轻量镜像，方便无服务时本地 agent 读取。 |

Agent 修复流程：

1. 读取 bug agent-context。
2. 用最小复现命令确认失败，或在无法复现时写入 `cannot_reproduce` 证据。
3. 修改代码和测试，避免修改无关文件。
4. 运行 targeted regression 和必要 L3/L4。
5. 提交修复 commit，commit message 包含 `Fixes #<issue_number>` 或明确 issue reference。
6. 通过 Validation API/GitHub issue comment 写入修复摘要、验证命令、run record、coverage/evidence。
7. 只有验证通过后，Bug 状态才能进入 `verified/closed`。

### 6.6 夜间任务策略

夜间任务应作为本地 scheduler 的计划，而不是云端 GitHub Actions 的默认依赖。原因是 AIstock 验证依赖本地 PostgreSQL/TDX/WSL/Qlib/GPU/模型资产，云端难以复现真实环境。

建议 schedule profile：

| profile | 频率 | 内容 | 失败处理 |
|---|---|---|---|
| `nightly_quick` | 每晚 | L0 + 高风险 backend coverage + 数据质量 smoke。 | 失败即自动创建/更新 Bug。 |
| `nightly_qe_replay` | 每晚或隔夜 | QE 历史 payload replay、archive independence、UI mocked/live dev API。 | 失败标记 QE/warehouse Bug。 |
| `nightly_long_backtest` | 每晚低频 | mini/selected real backtest、关键模型训练 smoke。 | 失败创建 P1/P2 Bug，阻断 release candidate。 |
| `weekly_full_l4` | 每周 | 更完整的跨模块 L4，包含数仓独立性和长样本数据质量。 | 失败进入治理 backlog。 |
| `release_candidate_l5` | 发布前 | 受影响模块 + selected long-run + 残余风险报告。 | 失败不得发布。 |

夜间任务必须：

- 使用 dev/test 端口，不重启生产 `8001`。
- 不重启远端 API。
- 记录数据日期、数据 snapshot、git commit、依赖服务状态。
- 超时和取消可控。
- 失败自动去重，避免同一 Bug 每晚重复创建多个 issue。
- 结果进入 UI 的“夜间任务/待修复 Bug/质量趋势”页面。

## 7. 后续所有设计文档的测试用例要求

未来所有设计方案文档必须增加“测试设计”章节，至少包含：

| 必填项 | 说明 |
|---|---|
| 影响范围 | backend/frontend/DB/data pipeline/QE/Paper v2/HMM/asset。 |
| 风险等级 | L0-L5 需要覆盖到哪一级。 |
| 测试矩阵 | 用例 ID、层级、输入、步骤、oracle、自动化路径。 |
| 覆盖率要求 | Python line/branch/diff，前端类型/组件/E2E，数据质量覆盖。 |
| 证据路径 | run record、coverage report、Playwright trace、smoke JSON。 |
| 生产隔离 | 是否需要 dev 端口、是否禁止生产 `8001`、是否涉及远端 API。 |
| 长任务策略 | 是否需要 mock/mini/replay/nightly/release-only。 |
| Bug 策略 | 失败时是否自动创建 Bug、去重 fingerprint、严重级别、agent-context 和验证关闭条件。 |
| 残余风险 | 不能自动验证的内容、人工验证条件、后续补充计划。 |

没有测试用例设计的高风险功能，不应进入完整研发阶段；只允许进入需求澄清或方案讨论阶段。

## 8. 第一批代码实现建议顺序

按最小风险、最大复用的顺序推进：

1. **Development guardrail baseline**：新增 `docs/standards/aistock_development_standard_v1.0_20260504.yaml`，扩展/新增 guardrail scanner，执行全仓只读 baseline scan，生成 `docs/analysis/aistock_guardrail_baseline_YYYYMMDD.md`；changed-files P0/P1 新违规开始阻断。
2. **Coverage baseline**：补 `pytest-cov`、`aistock_validate.py coverage`、`qe_data_contract_backend` coverage 输出和测试。
3. **Plan catalog**：新增 `test_plans.yaml` 和 catalog parser，先不接 UI 执行。
4. **Read-only validation API**：实现 plans/runs/detail/summary，读取现有历史、coverage、guardrail quality gates。
5. **Read-only Validation Center UI**：显示历史 run、coverage、evidence、guardrail、计划和禁用的执行按钮。
6. **Bug registry MVP**：实现 Bug issue template、failure fingerprint、bug_report 本地索引、agent-context 只读接口。
7. **Controlled execution**：只允许 allowlist nox session，增加超时、取消、日志、确认文案。
8. **Long-run support**：加入 scheduled/nightly/release-only 分类，不默认阻塞开发提交，并支持失败自动创建/更新 Bug。
9. **DB persistence**：当 JSON history 查询性能或并发执行需要时，再引入 `validation` schema，并同步 comment smoke。

## 9. 开发质量门禁

每个阶段完成后必须至少验证：

```powershell
python -m compileall scripts/aistock_validate.py backend/services/validation backend/routers/validation.py
python -m pytest backend/tests/test_aistock_validate_coverage.py backend/tests/test_validation_plan_catalog.py backend/tests/test_validation_history_store.py -q -p no:cacheprovider
python -m nox -s qe_data_contract_backend
python -m nox -s l0 -- scripts/aistock_validate.py backend/services/validation backend/routers/validation.py tests/aistock_validation/catalog/test_plans.yaml docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md
```

UI 阶段增加：

```powershell
cd frontend
npm exec tsc -- --noEmit --incremental false
$env:VALIDATION_CENTER_UI_MOCK_API='1'
npm run test:e2e -- tests/validation-center
```

如果任何阶段涉及 DB schema，必须增加只读 smoke：

- schema/table 存在。
- 所有表和字段都有 PostgreSQL comment。
- migration/bootstrap 幂等。
- 不触碰 QE production runtime hook。

## 10. 当前不做的事项

| 不做事项 | 原因 |
|---|---|
| 不直接开发完整独立测试平台 | 当前已有 nox/aistock_validate/history，先成熟化。 |
| 不创建独立微服务 | 用户已确认先放 AIstock 内部，减少部署和权限复杂度。 |
| 不在第一阶段做 UI 受控执行 | 先证明只读可观测和 allowlist 安全。 |
| 不默认跑长 QE 实验 | 影响耗时和资源，先 mock/mini/replay。 |
| 不重启生产 `8001` | 遵守生产隔离。 |
| 不重启远端机 API | 远端实验环境不能被测试系统干扰。 |
| 不直接读取 WSL/远端 workspace | QE/RD-Agent 红线。 |
| 不先引入复杂外部测试平台 | 先用 repo 内证据，必要时后续再接 Allure/Grafana 等。 |
| 不把 Bug 只存在本地 DB | 必须保留与 GitHub Issue/commit/run evidence 的可追溯关联。 |

## 11. 参考资料

- GitHub Docs - Tracking your work with issues: https://docs.github.com/en/issues/tracking-your-work-with-issues
- GitHub Docs - Configuring issue templates and issue forms: https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/creating-an-issue
- GitHub Docs - REST API endpoints for issues: https://docs.github.com/en/rest/issues/issues
- GitHub Docs - Linking a pull request to an issue: https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue
- GitHub Docs - Workflow syntax, `schedule`: https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#onschedule
- pytest-cov documentation: https://pytest-cov.readthedocs.io/

## 12. 完成定义

当以下条件满足时，可以认为 AIstock 内置自动化测试流水线第一版可用：

- `qe_data_contract_backend`、`qe_archive_backend`、`paper_v2_backend` 至少一个高风险模块已接入真实 coverage。
- `docs/standards/aistock_development_standard_v1.0_20260504.yaml` 和全仓 baseline scan 已完成，changed-files 中新增 P0/P1 规范违规会阻断 L0。
- `aistock_validate.py` 可以生成 run metadata、coverage snapshot、evidence manifest，并可被后端/UI 读取。
- Validation Center API 能展示测试计划、历史 run、run 详情、coverage、evidence、失败门禁。
- Validation Center UI 能以 mocked API 和 dev API 两种方式通过 E2E。
- UI/API 不能执行任意 shell，不能选择生产 `8001`，不能触发远端 API restart。
- 长耗时 QE/回测测试已在 plan catalog 中分级，不会阻塞每次提交。
- 夜间任务失败可以自动生成/更新 Bug，Bug 有 GitHub Issue、本地索引、失败 run、触发条件、agent-context、修复 commit 和验证 run。
- 每次功能设计文档都包含测试用例设计，并在实现完成后有 run record 证据。
