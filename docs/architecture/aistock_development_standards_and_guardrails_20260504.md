# AIstock 开发规范自动化落地与 Guardrail 框架设计

> 日期：2026-05-04
> 状态：Guardrail 落地设计 v1.2；本文不是规范源，规范源是 `docs/standards/aistock_development_standard_v1.1_20260504.md`
> 文档位置：`docs/architecture/aistock_development_standards_and_guardrails_20260504.md`
> 关联文档：`docs/standards/aistock_development_standard_v1.1_20260504.md`、`docs/standards/aistock_development_standard_v1.1_20260504.yaml`、`docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`、`docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`
> 适用范围：AIstock 全仓库，包括 FastAPI、Next.js、QE/RD-Agent、Paper Trading v2、Qlib 数据链路、HMM、数仓/归档、测试流水线、agent 工具链。

## 1. 结论

AIstock 需要把 `docs/standards` 中的人类可读规范和机器可读 YAML 直接纳入自动化测试流水线。本文只说明“如何落地规范”，不再重复定义规范正文；如本文与规范源冲突，以 `docs/standards/aistock_development_standard_v1.1_20260504.md` 为准。

核心结论：

1. **必须先整理严格开发规范**：规范应同时有人类可读文档、机器可读规则目录、自动化扫描和回归测试。
2. **必须先做全仓基线扫描**：当前代码经历多个开发工具和探索阶段，预期会有大量历史违规；应先只读扫描、建立 baseline，不直接阻断全仓。
3. **新代码严格、旧代码渐进**：历史问题进入 baseline/backlog；新增或修改代码不允许新增 P0/P1 违规。
4. **违反规范也是缺陷**：即使功能可运行，只要违反红线或高风险规范，也应记录为 quality defect / architecture defect 并进入修复闭环。
5. **规范即代码**：`docs/standards/aistock_development_standard_v1.1_20260504.yaml` 是与人类规范同版本的机器可读规则目录，配合 `noxfile.py`、`scripts/aistock_validate.py`、Semgrep、pytest、Playwright 和 DB smoke 执行。
6. **agent-first**：规范、失败上下文、复现命令、允许修改范围和验证命令都要机器可读，方便 Codex/Claude 修复后回写证据。
7. **MCP 预留但不前置**：第一阶段优先 repo 内 skill + nox + `aistock_validate.py` + Validation API；MCP 作为中长期统一工具/资源/Prompt 层。

对当前 AIstock 的实施判断：

- **需要先扫描全仓代码建立基线**，否则无法区分历史技术债和新增违规。
- **全仓基线扫描第一版只读、不阻断**，避免历史问题导致所有研发无法推进。
- **新增/修改代码立即严格**，changed-files 维度从第一版就阻断 P0/P1；P2 先 warning，成熟后逐步升级。
- **历史违规要进入治理 backlog**，按模块和风险 burn-down，不做一次性“大重构”。
- **规范必须落到流水线**，不允许只停留在人工约定或 code review 口头要求。

## 2. 参考的业界实践

| 来源 | 对 AIstock 的落地含义 |
|---|---|
| OWASP Secure Coding Practices Quick Reference Guide | 采用输入校验、错误处理、日志、配置、文件管理、数据保护等通用安全编码基线。 |
| Semgrep custom rules | 使用自定义规则把架构红线、静默 fallback、硬编码路径、危险 shell、raw JSON UI 等转成静态扫描。 |
| OpenSSF Scorecard | 借鉴“自动化健康检查 + 风险评分 + remediation”的治理模式，不把规则停留在人工 code review。 |
| GitHub Issues / issue forms / REST API | Bug 和规范违规以 GitHub Issue 为权威生命周期，本地 Validation Center 只做索引和证据缓存。 |
| GitHub Actions schedule 语义 | 夜间/定时任务采用本地 scheduler，但保留 schedule/manual-dispatch 的可审计计划语义。 |
| MCP server concepts | 后续把 Validation Center 暴露成 Tools/Resources/Prompts，但第一阶段不让 MCP 成为必需依赖。 |

## 3. 规范层级

建议把规范分为四级，流水线按级别决定是否阻断。

| 等级 | 名称 | 是否阻断 | 示例 |
|---|---|---|---|
| P0 | 红线违规 | 立即阻断提交/发布，必须修复 | Windows FastAPI 直读 WSL/远端 workspace、测试重启生产 `8001`、交易逻辑静默降级。 |
| P1 | 架构/数据高风险 | 默认阻断新代码；历史问题进高优 backlog | 新 DB 字段无 comment、QE 只保存 worker 路径不保存 manifest、归档后仍依赖 QE 源 DB。 |
| P2 | 工程质量风险 | 新代码应阻断；历史问题按模块治理 | 无 timeout、无资源释放、无边界全量读、全局 cache 无 TTL、吞异常。 |
| P3 | 可维护性/风格 | 记录 warning，逐步治理 | 命名不一致、重复代码、文档缺少测试矩阵、UI 文案不清。 |

处理原则：

- P0：无论新旧，发现后都应尽快修复或隔离。
- P1：新代码不得新增；历史问题需要创建治理任务。
- P2：新代码不得新增；历史问题按风险模块分批清理。
- P3：先 warning 和趋势管理，不阻塞普通开发。

## 4. 规范框架

### 4.1 规则来源

```text
docs/standards/aistock_development_standard_v1.1_20260504.md
  -> 人类可读规范源，定义红线、工程规范和治理要求

docs/standards/aistock_development_standard_v1.1_20260504.yaml
  -> 机器可读同步版本：rule_id、standard_ref、severity、scope、checker、baseline_policy

.codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py
  -> 现有轻量扫描器，先扩展再逐步引入 Semgrep

.semgrep/aistock/*.yml
  -> 后续结构化 Semgrep 自定义规则

noxfile.py
  -> L0/L1/L2/L3/L4/L5 权威执行入口

scripts/aistock_validate.py
  -> 记录 run metadata、coverage、quality gates、evidence、baseline
```

### 4.2 机器可读规则目录

当前机器可读规则目录已经迁移到 `docs/standards/aistock_development_standard_v1.1_20260504.yaml`。每条 enabled rule 必须包含 `standard_ref` 并指向同版本人类规范中的规则锚点。示例结构：

```yaml
schema_version: aistock_development_guardrails_v1
source_standard: docs/standards/aistock_development_standard_v1.1_20260504.md
source_version: "1.1"
rules:
  - rule_id: ARCH-WSL-001
    standard_ref: docs/standards/aistock_development_standard_v1.1_20260504.md#rule-arch-wsl-001
    title: Windows backend must not read WSL or remote worker workspace directly
    severity: P0
    category: architecture
    applies_to:
      - backend
      - scripts
      - frontend
    checker:
      type: regex
      patterns:
        - "\\\\\\\\wsl(?:\\$|\\.localhost)"
        - "/mnt/[a-z]/"
        - "workspace.*rdagent"
    baseline_policy: block_new_only
    remediation: "Use worker API or AIstock-owned artifact store with manifest/hash."

  - rule_id: ERR-FALLBACK-001
    title: Silent exception fallback is forbidden
    severity: P0
    category: error_handling
    checker:
      type: semgrep
      rule_path: .semgrep/aistock/no_silent_exception_fallback.yml
    baseline_policy: block_new_only
    remediation: "Fail fast with structured error; do not return []/None/True as fake success."

  - rule_id: DB-COMMENT-001
    title: New PostgreSQL tables and columns require comments
    severity: P1
    category: database
    checker:
      type: pytest
      test_path: backend/tests/test_schema_comments.py
    baseline_policy: block_new_and_changed_schema
    remediation: "Add COMMENT ON TABLE/COLUMN with business semantics, unit, source, and quality notes."
```

### 4.3 质量门禁输出

所有规则结果都应进入统一结构：

```json
{
  "rule_id": "ARCH-WSL-001",
  "severity": "P0",
  "status": "failed",
  "scope": "changed_files",
  "file": "backend/services/example.py",
  "line": 42,
  "message": "Windows-side direct WSL workspace access is forbidden.",
  "baseline_status": "new_violation",
  "remediation": "Use worker API or AIstock-owned artifact store.",
  "evidence_uri": "tests/aistock_validation/history/..."
}
```

## 5. 必须纳入的规范类别

### 5.1 数据与 workspace 访问

规则：

- Windows 侧 FastAPI / UI / 普通脚本不得直接读取 WSL 或远端 QE/RD-Agent worker workspace。
- QE/RD-Agent artifact 必须通过 worker API、AIstock-owned artifact store 或已入库 payload 获取。
- 归档后的数据访问不得依赖 QE 源 DB 或 worker workspace。
- artifact 必须有 manifest：URI、sha256、size、row_count、schema_version、created_at、source。

自动检查：

- `rg` / Semgrep 扫描 `\\wsl$`、`\\wsl.localhost`、`/mnt/`、远端 workspace 绝对路径。
- QE completion contract 测试拒绝 raw WSL/remote artifact URI。
- archive independence 测试模拟源 workspace 不可用。

### 5.2 生产隔离

规则：

- 开发和测试不得重启生产 `8001`。
- 测试使用 `8011/8012` 和 `3011/3012`。
- 不得重启远端机 API。
- Runtime hook 默认关闭，必须 feature flag 显式开启。

自动检查：

- 扫描测试脚本、nox session、UI action 中的 `8001` restart/start/kill。
- `aistock_validate.py ports/services` 拒绝危险端口和危险操作。
- Validation Center execution allowlist 不接受任意 shell。

### 5.3 错误处理与 fail-fast

规则：

- 禁止 `except Exception: return []/None/{}/True` 伪装成功。
- 缺数据不得默认填 0、默认 factor=1、默认现金、默认持仓。
- 缺分钟数据不得 fallback 到日频。
- V25/Torch/context 缺失不得 fallback 到 TWAP。
- HMM 缺系数不得 fallback 到中性系数。
- 所有 fallback 必须显式配置、显式记录、显式展示。

自动检查：

- Semgrep 检查吞异常和默认返回。
- 业务单测覆盖缺数据分支。
- UI E2E 验证错误可见，不显示成功状态。

### 5.4 不得未经确认简化开发

规则：

- 用户明确“不得简化”时，设计文档和实现必须覆盖完整范围。
- 若暂时无法实现，必须在 run metadata 中写 `scope_deviation` 和 `residual_risks`。
- 不得把未实现功能隐藏成成功。

自动检查：

- 设计文档必须有测试矩阵、覆盖率要求、Bug 策略、残余风险。
- L3/L4 run record 必须包含 scope、out_of_scope、residual_risks。
- 高风险功能缺测试矩阵时，L0 应 warning；进入实现时应阻断。

### 5.5 文件与文档存储位置

规则：

- 架构设计文档存 `docs/architecture`。
- 分析文档存 `docs/analysis`。
- Codex 记忆写 `docs/codex_project_memory.md`。
- 不修改 `AGENTS.md`，除非用户明确要求。
- 大日志、trace、截图、大 artifact 不提交 Git，只提交摘要、URI、hash。

自动检查：

- 扫描新增 `.md` 路径是否在允许目录。
- 扫描大文件和非预期二进制。
- run evidence 检查 artifact URI/hash，而非提交大文件。

### 5.6 DB schema 与数据治理

规则：

- 新表、新字段必须有 PostgreSQL comment。
- comment 应说明业务含义、单位、来源、质量语义、是否 PIT。
- 业务服务不隐式执行 DDL。
- 入仓和清理必须先验证独立性和完整性。

自动检查：

- DB smoke 检查 schema/table/column comment。
- migration/bootstrap 幂等测试。
- repository 测试禁止隐式 DDL。

### 5.7 资源、内存、并发和进程生命周期

规则：

- 大数据处理必须 chunk/batch，不得无边界全量读入超大 DataFrame。
- 全局 cache 必须有 max size、TTL 或 clear 机制。
- HTTP、DB、file、subprocess 必须有 timeout/close/context manager。
- SSE/WebSocket/后台任务必须可取消、可清理。
- 长任务必须有 max_duration、heartbeat、timeout、cancel、log tail。
- 测试和 dev server 不得留下 orphan process。

自动检查：

- 静态扫描 `read()` 大文件、`pd.read_*` 无 chunksize、`requests` 无 timeout、`subprocess.Popen` 无 cleanup。
- 单测验证 timeout/cancel。
- 夜间任务记录资源占用和耗时趋势。

### 5.8 前端 UI 规范

规则：

- UI 不得以 raw JSON 作为主要操作视图。
- 所有 disabled 按钮必须显示原因。
- API 错误必须可见，不得吞掉。
- 高风险写操作必须二次确认。
- Playwright 必须捕获 pageerror、console error、requestfailed、unexpected 4xx/5xx。

自动检查：

- Playwright 全局 fixture。
- 扫描 `JSON.stringify`、`<pre>`、未解释 raw JSON 的普通页面。
- UI E2E 覆盖错误态和禁用态。

### 5.9 Agent 执行规范

规则：

- agent 不得修改无关文件。
- agent 不得重启生产 `8001` 或远端 API。
- agent 不得直接读取 WSL/远端 workspace。
- agent 不得把失败验证描述成成功。
- agent 修复 Bug 后必须记录复现、修复、验证、commit、evidence。
- agent 必须读取 `docs/codex_project_memory.md` 和相关设计文档。

自动检查：

- Validation API 提供 `agent-context`。
- run metadata 记录 actor、commands、changed_files、evidence。
- Git diff 限定只提交本任务文件。

## 6. 为什么必须先做全仓基线扫描

需要先扫描全部代码，但不能直接把全仓扫描结果作为阻断门禁。

原因：

- 当前代码经历多个开发工具和探索阶段，历史遗留违规预计很多。
- 如果一开始全仓阻断，会导致任何开发都无法推进。
- 如果不扫描 baseline，就无法区分“历史债务”和“新增违规”。
- 只有建立 baseline 后，才能实现“新代码严格、历史逐步治理”。

推荐做法：

```text
Phase A: read-only full baseline scan
  -> 生成 docs/analysis/aistock_guardrail_baseline_YYYYMMDD.md
  -> 生成 tmp/validation/guardrails/baseline_YYYYMMDD.json
  -> 不阻断普通开发

Phase B: changed-files gate
  -> 新增/修改文件不得新增 P0/P1
  -> P2 先 warning，逐步升级

Phase C: module cleanup
  -> QE/RD-Agent workspace
  -> Paper v2 / trading / execution
  -> DB schema/comment
  -> frontend UI
  -> data pipeline / qlib exporter

Phase D: baseline burn-down
  -> 每周减少历史 P0/P1
  -> 夜间任务输出趋势
  -> 修复项关联 GitHub Issues

Phase E: full enforcement
  -> P0/P1 全仓阻断
  -> P2 对高风险模块阻断
  -> P3 保持 warning 和治理趋势
```

## 7. 基线扫描设计

### 7.1 扫描范围

初始全仓扫描建议覆盖：

| 范围 | 说明 |
|---|---|
| `backend` | API、service、DB、QE、Paper v2、data pipeline。 |
| `frontend/src` | UI、API client、页面、E2E 相关路径。 |
| `scripts` | 数据处理、归档、导出、修复脚本。 |
| `debug_tools` | 一次性诊断、排查、临时研究脚本。 |
| `noxfile.py` | 流水线入口。 |
| `.codex/skills` | agent 技能和 guardrail 工具。 |
| `tests/aistock_validation` | 测试矩阵和证据模板。 |
| `docs/architecture` | 设计文档规范完整性。 |
| `docs/standards` | 人类可读规范和同版本机器规则目录。 |

跳过：

- `.git`、`node_modules`、`.next`、`__pycache__`。
- `mlruns`、大型模型、历史 artifact。
- `qe_archive/artifacts`、trace/log、大型数据文件。

### 7.2 初始规则

首批 baseline 规则不要太多，当前 v1.1 先覆盖最有业务风险的 15 类：

| rule_id | 等级 | 类别 | 检查目标 |
|---|---|---|---|
| ARCH-WSL-001 | P0 | workspace | WSL/远端 workspace 直读。 |
| PROD-PORT-001 | P0 | production | 测试或工具操作生产 `8001` restart/kill。 |
| ERR-FALLBACK-001 | P0 | error | 吞异常伪成功。 |
| TRADING-FALLBACK-001 | P0 | trading | 分钟缺失日频 fallback、TWAP fallback、HMM 中性 fallback。 |
| QE-ARTIFACT-001 | P1 | QE | 只保存 worker 路径不保存 manifest/hash。 |
| DB-COMMENT-001 | P1 | database | 新 schema 无 comment。 |
| ROOT-POLLUTION-001 | P1 | repository | 根目录新增一次性脚本、日志、压缩包、数据文件。 |
| SCRIPT-LOCATION-001 | P1 | repository | 一次性测试/诊断脚本未放入 `debug_tools/`。 |
| DOC-LOCATION-001 | P1 | docs | 架构/分析文档位置错误。 |
| CONFIG-HARDCODE-001 | P1 | config | 绝对路径、端口、密钥硬编码。 |
| MEMORY-DATAFRAME-001 | P1 | memory | 大 DataFrame、pickle/CSV、循环 concat 缺少边界。 |
| DEBUG-FAILFAST-001 | P1 | error | 诊断脚本吞异常或失败返回成功。 |
| RESOURCE-TIMEOUT-001 | P2 | resource | HTTP/subprocess/DB 无 timeout。 |
| ALGO-COMPLEXITY-001 | P2 | performance | 高维量化循环、大 join/groupby/sort 缺复杂度审查。 |
| UI-RAWJSON-001 | P2 | frontend | 普通 UI 直接展示 raw JSON。 |

`scripts/aistock_guardrail_scan.py` 当前支持 `regex`、`path_regex`、`regex_and_python_loop_contains` 三类本地检查器；`DB-COMMENT-001` 等需要真实数据库或迁移上下文的规则继续由 pytest/DB smoke 外部检查承接。

### 7.3 输出报告

基线报告建议分为两份：

- `docs/analysis/aistock_guardrail_baseline_YYYYMMDD.md`：人工阅读摘要，提交 Git。
- `tmp/validation/guardrails/baseline_YYYYMMDD.json`：机器可读明细，不提交 Git，或只提交脱敏摘要。

报告字段：

| 字段 | 说明 |
|---|---|
| `rule_id` | 规则 ID。 |
| `severity` | P0/P1/P2/P3。 |
| `file` / `line` | 位置。 |
| `fingerprint` | 稳定去重指纹。 |
| `module` | 模块归属。 |
| `status` | baseline/new/suppressed/fixed。 |
| `remediation` | 修复建议。 |
| `owner` | 可选模块 owner。 |
| `issue_url` | 对应 GitHub Issue。 |

## 8. 最佳实施路径

### Phase 0 - 规范冻结

交付：

- 本文档。
- `docs/standards/aistock_development_standard_v1.1_20260504.yaml` 设计。
- `scripts/aistock_guardrail_scan.py` 能映射到 rule_id/severity/category，并输出 JSON/Markdown 证据。

验收：

- 文档包含规则分类、严重级别、baseline 策略、agent/MCP 策略。
- Validation Center 设计文档引用本规范。

### Phase 1 - 全仓只读基线扫描

交付：

- 扩展或新增 `scripts/aistock_guardrail_scan.py`，支持 `--baseline`、`--changed-only`、`--output-json`、`--summary-md`。
- 生成第一版 baseline 报告。

策略：

- 不阻断开发。
- P0/P1 自动生成或建议生成 GitHub Issues。
- 标记 top risk modules。

验收：

- 能区分 baseline 与 new violation。
- 能按 rule/category/severity/module 聚合。
- 不扫描大型 artifact。

### Phase 2 - 新代码阻断门禁

交付：

- `nox -s l0` 接入 changed-files guardrail。
- P0/P1 new violation 阻断。
- P2 先 warning。

验收：

- 在新增测试 fixture 中注入 WSL 直读、吞异常、生产端口等违规，扫描能失败。
- 历史 baseline 不阻断 changed-only。

### Phase 3 - 规则工具成熟化

交付：

- `.semgrep/aistock/*.yml` 首批规则。
- `aistock_validate.py guardrails` 子命令，输出 quality gates。
- Validation Center 展示 guardrail 结果。

验收：

- Semgrep/regex/pytest/DB smoke 结果统一写入 run metadata。
- 规则说明、修复建议、例外理由都可追踪。

### Phase 4 - 模块级治理

优先顺序：

1. QE/RD-Agent workspace 和 artifact 红线。
2. Paper v2 / trading / execution fail-fast。
3. DB schema/comment。
4. Qlib/data pipeline 大数据和资源风险。
5. Frontend raw JSON、错误态、按钮禁用原因。
6. 其他维护性规则。

策略：

- 每个模块创建 cleanup issue。
- 每次修复必须有 targeted regression。
- 夜间任务跟踪 burn-down。

### Phase 5 - Agent/MCP 支持

优先 skill，后 MCP：

```text
Codex / Claude Code
  -> aistock-guardrail-review skill
  -> aistock-fix-validation-bug skill
  -> Validation API
  -> 后续 MCP Resources/Tools/Prompts
```

建议 skill：

| skill | 作用 |
|---|---|
| `aistock-guardrail-review` | 扫描规范违规，生成修复建议。 |
| `aistock-fix-validation-bug` | 读取 bug agent-context，复现、修复、验证、回写。 |
| `aistock-design-with-tests` | 生成带测试矩阵和 guardrail 的设计文档。 |
| 扩展 `verify-aistock-feature` | 根据改动选择 nox plan，生成 run/evidence/quality gates。 |

MCP 中长期工具：

| MCP 能力 | 建议暴露 |
|---|---|
| Resources | `validation://guardrails`、`validation://runs/{run_id}`、`validation://bugs/{bug_id}`、`aistock://docs/architecture/...`。 |
| Tools | `run_validation_plan`、`get_agent_context`、`record_evidence`、`append_bug_event`，全部 allowlist。 |
| Prompts | `fix-validation-bug`、`design-with-tests`、`guardrail-review`、`release-gate-review`。 |

MCP 安全限制：

- 默认 read-only。
- 不提供任意 shell tool。
- 不提供 WSL/远端 workspace direct resource。
- 写操作必须 scope/confirm/audit。
- tool input 必须 schema 校验。

## 9. 例外和豁免

允许例外，但必须记录：

| 字段 | 说明 |
|---|---|
| `rule_id` | 例外对应规则。 |
| `file` / `line` | 范围尽量小。 |
| `reason` | 为什么暂时不能修。 |
| `expires_at` | 过期时间。 |
| `approved_by` | 批准者。 |
| `risk_mitigation` | 临时缓解措施。 |
| `tracking_issue` | GitHub Issue。 |

不允许永久、无理由、无范围的 suppress。

## 10. 与流水线的关系

```text
development standards
  -> docs/standards/aistock_development_standard_v1.1_20260504.yaml
  -> guardrail scan / semgrep / pytest / DB smoke / Playwright
  -> nox L0-L5
  -> run metadata quality_gates
  -> Validation Center UI
  -> Bug issue + agent-context
  -> 修复 + 回归 + evidence
```

完成后，AIstock 的“通过验证”应同时表示：

- 功能结果正确。
- 覆盖率满足要求。
- 数据质量满足要求。
- 没有新增 P0/P1 规范违规。
- 证据可追溯。
- agent 能复现和修复失败。

## 11. 当前不做

- 不在第一版就让全仓历史 P2/P3 阻断开发。
- 不把所有历史问题一次性修完。
- 不依赖 MCP 才能运行流水线。
- 不引入独立测试微服务。
- 不让 UI/API 执行任意 shell。
- 不把大 trace/log/artifact 提交 Git。

## 12. 第一批实施建议

推荐下一步：

1. 先实现 `docs/standards/aistock_development_standard_v1.1_20260504.yaml` 的最小规则目录。
2. 扩展现有 `scan_quality_guardrails.py` 或新增 `scripts/aistock_guardrail_scan.py`。
3. 执行一次全仓只读 baseline scan，生成 `docs/analysis/aistock_guardrail_baseline_YYYYMMDD.md`。
4. `nox -s l0` 开始阻断 changed-files 中的 P0/P1 新违规。
5. 再进入 coverage gate 和 Validation Center API/UI。

当前第一批已落地内容：

- 项目级开发规范 v1.1：`docs/standards/aistock_development_standard_v1.1_20260504.md`，统一承载 Python 工程规范和量化/交易工程规范。
- 机器可读规则目录：`docs/standards/aistock_development_standard_v1.1_20260504.yaml`。
- 只读扫描器：`scripts/aistock_guardrail_scan.py`。
- 扫描器单元测试：`backend/tests/test_aistock_guardrail_scan.py`。
- 首次全仓 tracked-files baseline：`docs/analysis/aistock_guardrail_baseline_20260504.md`，完整机器 JSON 位于本地 `tmp/validation/guardrails/baseline_20260504.json`，不提交 Git。

## 13. 与现有流水线的集成顺序

开发规范不是独立项目，应作为自动化测试流水线的第一层质量门禁接入。建议顺序：

| 顺序 | 集成点 | 目标 |
|---|---|---|
| 1 | `docs/standards/aistock_development_standard_v1.1_20260504.yaml` | 把红线、严重级别、检查器、baseline 策略机器可读化。 |
| 2 | `scan_quality_guardrails.py` 或 `scripts/aistock_guardrail_scan.py` | 先复用现有轻量扫描器，再逐步引入 Semgrep 结构化规则。 |
| 3 | `docs/analysis/aistock_guardrail_baseline_YYYYMMDD.md` | 保存全仓只读 baseline 摘要，作为历史技术债治理起点。 |
| 4 | `nox -s l0` | 对 changed-files 阻断新增 P0/P1，输出 guardrail quality gate。 |
| 5 | `scripts/aistock_validate.py evidence/run metadata` | 将 guardrail 结果进入 run metadata、evidence manifest 和 Validation Center。 |
| 6 | Validation Center API/UI | 展示规则、违规、趋势、Bug、agent-context 和修复验证状态。 |

## 14. 参考资料

- OWASP Secure Coding Practices Quick Reference Guide（https://owasp.org/www-project-secure-coding-practices-quick-reference-guide/）：通用安全编码实践清单，覆盖输入校验、错误处理、数据保护、数据库安全、文件管理、内存管理和通用编码实践。
- Semgrep rule writing documentation（https://semgrep.dev/docs/writing-rules/overview）：支持以自定义规则扫描安全问题、风格违规、bug 和配置问题，适合把 AIstock 架构红线转为 policy-as-code。
- OpenSSF Scorecard（https://github.com/ossf/scorecard）：以自动化检查和风险评分评估开源项目安全健康度，AIstock 可借鉴其“自动化检查 + remediation + 趋势治理”模式。
- GitHub Issues documentation（https://docs.github.com/en/issues/tracking-your-work-with-issues）：Issues 支持 labels、assignees、milestones、Projects、issue dependencies 和 PR 链接，适合作为 Bug/规范违规生命周期的权威记录。
- MCP Security Best Practices（https://modelcontextprotocol.io/specification/2025-06-18/basic/security_best_practices）：MCP 暴露给 agent 时必须最小权限、明确 consent、拒绝危险 token passthrough/SSRF/session 风险；AIstock 第一阶段不把 MCP 作为必需执行层。
