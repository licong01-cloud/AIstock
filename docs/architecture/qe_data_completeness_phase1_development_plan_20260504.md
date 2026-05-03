# QE 数据完整性与验证流水线第一阶段开发任务拆解

> 日期：2026-05-04
> 状态：第一阶段实施清单，按用户确认进入研发
> 文档位置：`docs/architecture/qe_data_completeness_phase1_development_plan_20260504.md`
> 约束：不重启生产后端 `8001`，不重启远端机 API；测试使用现有 nox / pytest / Playwright / smoke / run record 体系；本阶段不开发完整数仓 UI、不启用生产实时归档 hook、不清理 QE workspace 或 QE DB 历史记录。

## 1. 本阶段目标

本阶段只完成研发启动所需的四项基础能力：

1. 固化详细开发任务拆解和第一批变更清单。
2. 在现有 `scripts/aistock_validate.py` 基础上实现 JSON run metadata 与 evidence manifest，不另起测试框架。
3. 增加对应自动化测试和 nox 验证入口。
4. 增加 QE completion payload 与 artifact manifest 的后端 schema contract，先作为独立 contract 和测试，不接生产 QE webhook。

## 2. 第一批变更清单

| 文件 | 变更类型 | 目的 |
|---|---|---|
| `scripts/aistock_validate.py` | 修改 | `record` 命令继续生成 Markdown，同时生成 JSON run metadata；新增 `evidence` 命令生成 evidence manifest。 |
| `backend/tests/test_aistock_validate_metadata.py` | 新增 | 验证 JSON metadata、history root 测试隔离、evidence manifest、缺失 evidence 行为。 |
| `backend/services/quantevolver/completion_contract.py` | 新增 | 定义 QE completion payload、artifact manifest、collection status、missing required fields 的 Pydantic contract。 |
| `backend/tests/unified_engine/test_qe_completion_contract.py` | 新增 | 验证完整 payload、partial payload、complete 状态缺字段阻断、artifact sha/path 校验。 |
| `noxfile.py` | 修改 | 增加 `qe_data_contract_backend` nox session，并把新 contract 文件纳入 QE archive compile/guardrail 相关扫描。 |
| `tests/aistock_validation/modules/qe_data_completeness.md` | 新增 | 登记本阶段自动化测试矩阵和命令入口。 |
| `tests/aistock_validation/history/qe_data_completeness/*` | 新增 | 保存本阶段自动化验证 run record 与 JSON metadata/evidence。 |
| `docs/codex_project_memory.md` | 修改 | 记录本阶段已落地的流水线与 QE contract 基础能力。 |

## 3. 执行顺序

### Phase 1 - 任务拆解与边界固化

- 生成本文档。
- 明确不触碰生产服务、不重启远端 API、不启用实时归档 hook。
- 验证方式：文档内容检查和后续 commit。

### Phase 2 - `aistock_validate.py` 元数据化

- 保持 `record` 兼容现有 nox 调用。
- 新增 JSON run metadata，字段至少包含：schema_version、module、level、title、git_commit、operator、started_at、status、environment、steps、coverage、quality_gates、evidence、residual_risks、markdown_path。
- 新增 `--history-root` 便于测试隔离。
- 新增 `--json-out` / `--no-json` 便于显式控制。
- 新增 `evidence` 子命令，支持收集 `--item kind=path`、`--include path`、`--coverage`、`--playwright-report`、`--smoke-json` 等 evidence。

### Phase 3 - 自动化测试与 nox 验证

- 新增 targeted pytest 覆盖 `aistock_validate.py`。
- 新增 nox session `qe_data_contract_backend`，运行 compileall 和 targeted pytest。
- 使用 targeted `nox -s l0 -- <changed files>` 做 guardrail scan。
- 生成 run record 和 evidence manifest。

### Phase 4 - QE completion payload / artifact manifest contract

- 新增 Pydantic contract，不改变现有 QE 运行逻辑。
- Contract 支持 partial/complete 两类状态，`complete` 缺 required fields 必须 fail-fast。
- Artifact manifest 支持 sha256、size、row_count、schema_version、source_api、collection_status、parser_status。
- 禁止 artifact URI 使用 WSL/远端 workspace raw path 作为合规 manifest URI。
- 提供 `validate_completion_payload` 和 `compute_missing_required_fields` 供后续 collector/API 复用。

## 4. 验收标准

| 验收项 | 必须结果 |
|---|---|
| 生产隔离 | 不重启 `8001`，不重启远端 API，不启用 QE 实时归档 hook。 |
| 自动化记录 | `aistock_validate.py record` 生成 Markdown + JSON metadata。 |
| Evidence | `aistock_validate.py evidence` 能生成 evidence manifest，记录存在性、size、sha256。 |
| Contract | 完整 completion payload 校验通过；complete 但缺关键字段时校验失败；partial 能返回 missing_fields。 |
| Artifact 安全 | raw WSL/worker path 被 contract 拒绝；API/AIstock-owned URI 可接受。 |
| 测试 | targeted pytest、`qe_data_contract_backend` nox、targeted L0 guardrail 均通过。 |
| 提交 | 只提交本阶段修改文件到 GitHub。 |
