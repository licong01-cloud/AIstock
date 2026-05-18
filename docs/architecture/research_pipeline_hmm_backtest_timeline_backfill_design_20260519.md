# Research Pipeline HMM Backtest Timeline 自动记录与历史补齐设计

> 日期：2026-05-19  
> 状态：设计定稿，待独立分支实现  
> 范围：Research Pipeline 中 HMM backtest timeline 的长期自动记录、历史补齐、MCP/API/UI 只读查询能力  
> 非目标：不批量写 QE Archive、不重新运行历史回测、不替代 QE 执行、不重做 `/quantevolver/model-training`

## 1. 背景与问题

当前 AIstock 已具备 Research Pipeline 基础能力：可以创建研究实验、记录 stage attempt、artifact reference、comparison 和 pipeline event，也已经有独立 `aistock-research` MCP 入口。HMM 历史研发数据也已经被初步归档到 Research Pipeline，但现状仍有三个缺口：

1. HMM 回测轨迹还不是 Research Pipeline 的正式阶段，只能临时挂在 `portfolio_simulation` 或 `offline_validation` 的 result JSON 中。
2. QE loop 完成后只会触发 QE Archive best-effort hook，没有自动写入 Research Pipeline 的 HMM backtest recorder。
3. 历史 HMM 数据补齐还依赖一次性文件和手工判断，尚未复用未来长期自动记录的同一套逻辑。

因此，正确顺序必须是：**先实现长期自动记录能力，再用同一套能力一次性补齐历史数据**。历史补齐应被视为 recorder 的 backfill 模式，而不是独立的数据入口。

## 2. 总体目标

- 将 HMM 回测结果记录升级为 Research Pipeline 的正式步骤。
- 未来 HMM 演进完成 QE 回测后，自动记录关键回测指标，不需要人工手工补录。
- 历史补齐和未来自动记录复用同一个 recorder、同一套幂等键、同一套分类和同一套表结构。
- Research Pipeline 保存 HMM 研发轨迹；QE Archive 继续只保存有复用价值的 factor/model/strategy/execution representative。
- 只读 Research Pipeline Inspector 能查看 HMM timeline、family summary、backfill run 和代表性选择结果。

## 3. 架构定位

Research Pipeline 是研究编排层和元数据层，不是新的资产库，也不是 QE 执行器。HMM backtest timeline 的定位如下：

```text
HMM candidate/config
  -> QE backtest loop
  -> QE loop completed
  -> Research Pipeline HMM backtest recorder
  -> backtest_recording stage
  -> backtest_record rows
  -> Research Pipeline Inspector read-only view
```

历史补齐使用同一条写入路径：

```text
Historical QE loop / archived source file
  -> HMM backtest recorder backfill mode
  -> backtest_recording stage
  -> backtest_record rows
  -> backfill_run audit
  -> Research Pipeline Inspector read-only view
```

关键边界：

- HMM-only 配置演进属于 Research Pipeline timeline。
- factor/model/strategy/execution 组合沉淀属于 QE Archive。
- 同一个非 HMM 投资配置下的大量 HMM sweep 不应批量污染 QE Archive。
- 同一个 HMM 配置完全重复跑出的 loop 不应在 Research Pipeline 中重复写入。

## 4. Pipeline 阶段设计

`hmm_research` pipeline 增加正式阶段：

```text
artifact_gen
offline_validation
portfolio_simulation
backtest_recording
qe_shadow
```

`backtest_recording` 职责：

- 记录已完成 QE 回测的关键结果。
- 绑定 QE task、QE loop 和 QE experiment。
- 记录 HMM 配置摘要、模型版本、signal preset、risk gate / remap 摘要。
- 记录非 HMM 投资配置摘要，包括模型、因子签名、策略、执行算法、股票池、label horizon。
- 标记是否 QE Archive eligible、是否 QE Archive representative、拒绝原因和 dedup 状态。
- 记录自动 hook 或历史 backfill 的来源。

`backtest_recording` 不负责：

- 不重新运行回测。
- 不直接写 QE Archive。
- 不直接写 factor/model/strategy asset registry。
- 不做生产 promotion。
- 不保存日级持仓、交易明细或全量预测明细。

## 5. 数据模型设计

当前 `stage_attempt.result_json` 适合保存阶段摘要，不适合承载长期可查询 timeline。因此新增两张 Research Pipeline 表。

### 5.1 `research_pipeline.backtest_record`

记录每一条有研究价值的 backtest timeline record。

| 字段 | 类型建议 | 说明 |
|---|---|---|
| `record_id` | text PK | `rp_bt_<uuid>` |
| `experiment_id` | text FK | Research Pipeline experiment |
| `stage_attempt_id` | text FK nullable | 对应 `backtest_recording` attempt |
| `pipeline_type` | text | 当前主要是 `hmm_research` |
| `research_domain` | text | 当前为 `hmm` |
| `source_type` | text | `qe_loop`、`historical_file`、`manual_repair` |
| `source_task_id` | text | QE task id |
| `source_loop_id` | text | QE loop id |
| `source_loop_index` | integer | loop index |
| `source_experiment_id` | text | QE experiment id，例如 `qe_xxx_L1` |
| `source_created_at` | timestamptz | 原 task/loop 时间 |
| `record_version` | text | 例如 `hmm_backtest_record_v1` |
| `record_key_sha256` | text unique | 幂等键 hash |
| `non_hmm_config_sig` | text | 因子、模型、策略、执行、股票池等签名 |
| `hmm_config_sig` | text | HMM 配置签名 |
| `strict_family_sig` | text | 严格配置族签名 |
| `archive_family_sig` | text | QE Archive 代表性配置族签名 |
| `dedup_status` | text | `primary`、`duplicate_same_config`、`hmm_variant`、`excluded` |
| `qe_archive_eligible` | boolean | 是否可进入 QE Archive |
| `qe_archive_representative` | boolean | 是否被选为 QE Archive representative |
| `rejection_reason` | text nullable | 不入库或排除原因 |
| `ann` | double precision nullable | 年化收益 |
| `mdd` | double precision nullable | 最大回撤 |
| `ir` | double precision nullable | information ratio |
| `ic` | double precision nullable | IC |
| `rank_ic` | double precision nullable | RankIC |
| `sharpe` | double precision nullable | 可选 |
| `turnover` | double precision nullable | 可选 |
| `metrics_json` | jsonb | 其他指标 |
| `hmm_config_summary_json` | jsonb | HMM 配置摘要 |
| `config_summary_json` | jsonb | 非 HMM 配置摘要 |
| `source_payload_json` | jsonb | 必要来源片段，避免塞入全量大对象 |
| `recorded_by` | text | `auto_hook`、`backfill`、`codex` |
| `created_at` | timestamptz | 写入时间 |
| `updated_at` | timestamptz | 更新时间 |

推荐约束：

```sql
UNIQUE (source_type, source_task_id, source_loop_id, source_loop_index, record_version)
UNIQUE (record_key_sha256)
```

推荐索引：

```sql
(experiment_id, source_created_at DESC)
(experiment_id, non_hmm_config_sig)
(experiment_id, hmm_config_sig)
(experiment_id, archive_family_sig)
(experiment_id, qe_archive_representative)
(source_task_id, source_loop_index)
```

### 5.2 `research_pipeline.backfill_run`

记录一次历史补齐任务的状态和审计信息。

| 字段 | 类型建议 | 说明 |
|---|---|---|
| `backfill_run_id` | text PK | `rp_bf_<uuid>` |
| `experiment_id` | text FK | 目标 Research experiment |
| `backfill_type` | text | `hmm_backtest_timeline` |
| `status` | text | `previewed`、`running`、`completed`、`failed`、`cancelled` |
| `dry_run` | boolean | 是否只预览 |
| `source_scope_json` | jsonb | 时间范围、task id、文件路径、过滤规则 |
| `source_fingerprint_json` | jsonb | 源文件 hash / QE DB 查询条件 hash |
| `counts_json` | jsonb | inserted、updated、skipped、duplicate、excluded |
| `stage_attempt_id` | text nullable | 对应 `backtest_recording` attempt |
| `error_message` | text nullable | 失败原因 |
| `created_by` | text | 操作者或服务名 |
| `started_at` | timestamptz nullable | 开始时间 |
| `completed_at` | timestamptz nullable | 完成时间 |
| `created_at` | timestamptz | 创建时间 |
| `updated_at` | timestamptz | 更新时间 |

### 5.3 schema 初始化

必须继续使用 Python schema bootstrap：

```text
backend/db/init_research_pipeline_schema.py
```

要求：

- 在现有 `research_pipeline` schema 中增量创建表。
- 保持脚本可重复执行。
- 为新增表和关键字段补充 `COMMENT`。
- 不新增裸 `.sql` 文件作为主初始化方式。

## 6. HMMBacktestRecorder 服务设计

新增服务建议路径：

```text
backend/services/research_pipeline/hmm_backtest_recorder.py
```

所有自动记录和历史补齐入口都必须调用同一个服务。

核心职责：

```text
HMMBacktestRecorder
  - normalize_qe_loop(...)
  - normalize_historical_file_record(...)
  - build_record_key(...)
  - build_non_hmm_config_signature(...)
  - build_hmm_config_signature(...)
  - classify_record(...)
  - upsert_backtest_record(...)
  - summarize_records(...)
  - create_backfill_preview(...)
  - execute_backfill(...)
```

标准输出结构：

```json
{
  "source": {
    "task_id": "...",
    "loop_id": "...",
    "loop_index": 1,
    "experiment_id": "...",
    "created_at": "..."
  },
  "metrics": {
    "ann": 0.48,
    "mdd": -0.16,
    "ir": 2.09,
    "ic": 0.07,
    "rank_ic": 0.11
  },
  "hmm_config_summary": {
    "enable_sector_hmm": true,
    "hmm_model_version_id": "...",
    "hmm_config_version": "...",
    "hmm_config_ui_label": "...",
    "hmm_signal_preset": "preset_A",
    "hmm_candidate_name": "..."
  },
  "config_summary": {
    "model_id": "...",
    "factor_sig": "...",
    "factor_count": 57,
    "strategy_id": "score_weighted_topk_v2",
    "execution_algo": "V25_TWO_STAGE",
    "stock_pool": "...",
    "label_horizon": 10,
    "disable_alpha158": true
  },
  "classification": {
    "non_hmm_config_sig": "...",
    "hmm_config_sig": "...",
    "archive_family_sig": "...",
    "strict_family_sig": "...",
    "dedup_status": "hmm_variant",
    "qe_archive_eligible": false,
    "qe_archive_representative": false,
    "rejection_reason": "hmm_only_config_sweep_preserved_in_research_pipeline"
  }
}
```

## 7. 去重与保留策略

必须严格区分 Research Pipeline 和 QE Archive。

### 7.1 Research Pipeline timeline 保留

应保留：

- 每个不同 HMM 配置版本的关键回测结果。
- 每个不同 HMM model snapshot 的关键回测结果。
- 每个不同 HMM signal preset、risk gate、remap 配置的关键结果。
- 属于明确 HMM 对照实验的 no-HMM control。
- 不同非 HMM 投资配置下的代表性结果。

不应重复保留：

- 同一个 HMM 配置、同一个非 HMM 配置、同一个回测窗口下完全重复跑出的 loop。
- 缺少 HMM 语义且不是 HMM control 的普通 QE loop。
- 只有任务名相似但配置不可验证的噪声 loop；这类记录应进入 quarantine 或 excluded summary。

### 7.2 QE Archive 保留

QE Archive 仍只保留：

- 因子、模型、策略、执行、股票池、label horizon 等非 HMM 投资配置有代表性的组合。
- 同一个 archive family 最多一个 representative。
- HMM-only config sweep 不批量进入 QE Archive。

### 7.3 分类矩阵

| 类型 | Research Pipeline | QE Archive |
|---|---|---|
| 不同 HMM config，同一非 HMM 配置 | 保留 timeline | 不重复入库 |
| 同 HMM config，同非 HMM 配置，重复跑 | 只保留 primary，计 duplicate | 不重复入库 |
| 不同执行策略 | 保留 | 每类最多 representative |
| 不同模型或因子签名 | 保留 | 可作为新 family |
| no-HMM control | 如果属于 HMM 实验则保留 | 可作为 baseline |
| 普通非 HMM QE loop | 不进入 HMM timeline | 按 QE Archive 规则处理 |

## 8. 自动记录设计

新增 Research Pipeline realtime ingestion 服务：

```text
backend/services/research_pipeline/realtime_ingestion.py
```

对外安全入口：

```python
safe_record_hmm_backtest_completed(
    *,
    task_id: str,
    loop_id: str,
    loop_index: int | None = None,
) -> dict[str, Any]
```

在 QE loop 完成路径中新增 best-effort hook，形态对齐现有 QE Archive hook：

```text
_archive_completed_loop_best_effort(...)
_record_research_backtest_best_effort(...)
```

要求：

- 自动记录失败不能影响 QE loop 状态。
- 所有异常只记录 warning，并返回 skipped 或 failed result。
- 必须受 feature flag 控制。
- 必须幂等。
- 默认只处理显式绑定 Research Pipeline 的 HMM loop。

推荐 feature flags：

```text
RESEARCH_PIPELINE_HMM_RECORDING_ENABLED=false
RESEARCH_PIPELINE_HMM_BACKFILL_ENABLED=false
RESEARCH_PIPELINE_HMM_BACKFILL_WRITE_ENABLED=false
```

### 8.1 未来自动识别规则

未来 QE task 创建时应显式携带：

```json
{
  "research_experiment_id": "rp_exp_...",
  "research_pipeline_type": "hmm_research",
  "research_domain": "hmm",
  "record_backtest_to_research_pipeline": true
}
```

自动 hook 只处理：

```text
record_backtest_to_research_pipeline = true
research_domain = hmm
research_experiment_id 非空
```

不建议长期依赖 task name 包含 `HMM` 的启发式判断。

### 8.2 历史兼容规则

历史补齐可以允许启发式识别：

- task name 包含 HMM。
- loop config 中存在 HMM 字段。
- 历史归档文件中已标记为 HMM archive loop。
- 明确的 no-HMM control task。

历史兼容规则只用于 backfill，不用于未来自动 hook。

## 9. API 设计

在 `backend/routers/research_pipeline.py` 增加以下接口。

### 9.1 查询 backtest records

```text
GET /api/v1/research-pipeline/experiments/{experiment_id}/backtest-records
```

查询参数：

```text
research_domain=hmm
dedup_status=
qe_archive_representative=
source_task_id=
hmm_config_sig=
non_hmm_config_sig=
limit=
offset=
```

### 9.2 backfill preview

```text
POST /api/v1/research-pipeline/experiments/{experiment_id}/hmm-backtests/backfill-preview
```

请求示例：

```json
{
  "source_mode": "historical_file",
  "source_scope": {
    "created_at_start": "2026-05-04",
    "created_at_end": "2026-05-10",
    "task_ids": [],
    "source_file": "F:\\Dev\\AIstock\\.codex_tmp\\hmm_research_loop_archive_policy_20260518.json"
  },
  "policy": {
    "preserve_hmm_variants": true,
    "deduplicate_exact_same_hmm_config": true,
    "qe_archive_representative_only": true
  }
}
```

返回示例：

```json
{
  "preview_id": "rp_bf_...",
  "counts": {
    "candidate_count": 177,
    "would_insert": 177,
    "would_update": 0,
    "would_skip_duplicate": 0,
    "qe_archive_representative_count": 11,
    "research_timeline_count": 177
  },
  "sample_records": []
}
```

### 9.3 backfill execute

```text
POST /api/v1/research-pipeline/experiments/{experiment_id}/hmm-backtests/backfill-execute
```

必须要求 confirm：

```json
{
  "confirm": "RESEARCH_HMM_BACKFILL_EXECUTE",
  "preview_id": "rp_bf_...",
  "dry_run": false
}
```

返回示例：

```json
{
  "backfill_run_id": "rp_bf_...",
  "stage_attempt_id": "rp_attempt_...",
  "status": "completed",
  "counts": {
    "inserted": 177,
    "updated": 0,
    "skipped_duplicate": 0,
    "excluded": 1
  }
}
```

### 9.4 backfill status

```text
GET /api/v1/research-pipeline/backfill-runs/{backfill_run_id}
```

## 10. MCP 工具设计

在 `backend/mcp/modules/research.py` 增加薄封装工具。MCP 仍只做参数校验、confirm 校验和 backend API 调用。

新增工具：

```text
research_list_backtest_records
research_hmm_backfill_preview
research_hmm_backfill_execute
research_get_backfill_run
```

要求：

- `research_hmm_backfill_execute` 必须要求 `confirm = "RESEARCH_HMM_BACKFILL_EXECUTE"`。
- 默认 `dry_run=true`。
- 不提供直接写 QE Archive 的工具。
- source file 路径必须限制在项目根或明确允许的 artifact 路径下。

## 11. UI 设计

扩展只读 Research Pipeline Inspector，不新增工作台，不重做模型训练页面。

页面保持：

```text
/research-pipeline
/research-pipeline/[experimentId]
```

详情页新增三个区域。

### 11.1 HMM Backtest Timeline

展示字段：

- 时间。
- task id / loop id。
- HMM config label。
- HMM model version。
- ann、mdd、ir、ic、rank_ic。
- non-HMM family。
- QE Archive representative 状态。
- dedup status。
- rejection reason。
- source link。

### 11.2 Family Summary

按 `non_hmm_config_sig` 分组展示：

- family 内 HMM variant 数量。
- best ann。
- best ir。
- worst mdd。
- selected representative。
- duplicate count。

### 11.3 Backfill Runs

展示：

- run id。
- source scope。
- dry_run。
- status。
- inserted / updated / skipped / excluded。
- started_at / completed_at。
- error message。

UI 只读即可，执行仍通过 MCP 或受控 API 完成。

## 12. 历史补齐流程

### 12.1 冻结源数据

当前 HMM 历史补齐优先使用：

```text
F:\Dev\AIstock\.codex_tmp\hmm_research_loop_archive_policy_20260518.json
```

该文件包含 selected representatives 和 duplicate rejected loops 的关键指标，比只保存代表 loop 的 archive record 更适合生成完整 HMM timeline。

补齐前必须记录：

- source file path。
- sha256。
- generated_at。
- source_api_base。
- task window。
- candidate count。
- policy version。

### 12.2 preview

执行 backfill preview，确认：

- 将写入多少 timeline record。
- 将跳过多少重复。
- 将标记多少 QE Archive representative。
- 是否存在无法识别的 HMM config。
- 是否存在指标缺失 loop。

### 12.3 execute

确认后执行正式补齐：

- 创建或补齐 `backtest_recording` stage_plan。
- 创建 `backtest_recording` stage_attempt。
- upsert `backtest_record`。
- 写 `backfill_run`。
- 写 artifact_ref 指向 source file。
- 写 pipeline_event。

### 12.4 幂等复跑验证

同样 source 和 policy 重复执行一次：

- `inserted` 必须为 0。
- `updated` 应为 0 或只更新非关键 metadata。
- `skipped_duplicate` 应等于已有记录数。
- `backtest_record` 总数不变。

## 13. 生产安全策略

默认所有写入能力关闭：

```text
RESEARCH_PIPELINE_HMM_RECORDING_ENABLED=false
RESEARCH_PIPELINE_HMM_BACKFILL_ENABLED=false
RESEARCH_PIPELINE_HMM_BACKFILL_WRITE_ENABLED=false
```

推荐上线顺序：

1. dev 环境启用 preview。
2. dev 环境执行 backfill dry-run。
3. dev 环境执行 backfill write。
4. 验证幂等复跑。
5. dev 环境接入 QE 自动 hook，跑一个小型 HMM QE loop 验证自动记录。
6. 合入 main。
7. 生产初始化 schema。
8. 生产启用 preview。
9. 生产执行 dry-run。
10. 人工确认 counts。
11. 生产短窗口开启 write。
12. 生产执行一次历史补齐。
13. 生产复跑确认幂等。
14. 关闭 backfill write，只保留未来 auto hook。

## 14. 测试与验证标准

### 14.1 单元测试

建议新增：

```text
backend/tests/research_pipeline/test_hmm_backtest_recorder.py
```

覆盖：

- HMM config 签名稳定性。
- non-HMM config 签名稳定性。
- record key 幂等。
- duplicate classification。
- no-HMM control 识别。
- 指标缺失处理。
- historical source file parser。

### 14.2 API 测试

覆盖：

- preview 不写 DB。
- execute 缺 confirm 被拒绝。
- execute 正确 confirm 写入。
- 重复 execute 幂等。
- list records 分页和过滤。
- backfill run status 正确。

### 14.3 MCP 测试

覆盖：

- 新工具能被 list。
- execute 缺 confirm 被拒绝。
- preview 可调用。
- execute 可调用 backend API。
- get backfill run 可返回状态。

### 14.4 Hook 测试

覆盖：

- 非 HMM loop 不写 Research Pipeline。
- HMM loop 写入 `backtest_record`。
- 已写入 loop 再次触发不重复。
- recorder 抛错不影响 QE loop 完成。

### 14.5 生产验收

历史补齐完成后至少验证：

- `backtest_record` 总数符合 preview。
- 每个不同 HMM config 有关键指标。
- `ann`、`mdd`、`ir`、`ic`、`rank_ic` 可查询。
- QE Archive representative count 符合预期。
- 重复执行不新增记录。
- Research Pipeline UI 可查看 timeline。
- QE Archive 未被批量写入 HMM-only sweep。

## 15. 分期实施计划

### Phase 1：数据模型与 recorder

范围：

- 新增 `backtest_record`。
- 新增 `backfill_run`。
- 新增 HMMBacktestRecorder。
- 新增签名、分类、幂等逻辑。
- 更新 Python schema bootstrap。

不做：

- 不接 QE 自动 hook。
- 不执行生产补齐。

### Phase 2：API / MCP / UI 查询

范围：

- 新增 backtest records 查询 API。
- 新增 backfill preview / execute / status API。
- 新增 MCP 工具。
- UI 增加 HMM timeline 只读展示。

不做：

- 不默认开启生产写入。

### Phase 3：QE 自动记录 hook

范围：

- QE task 支持 `research_experiment_id` metadata。
- QE loop completed 后 best-effort 写 Research Pipeline。
- feature flag 控制。
- 自动记录失败不影响 QE。

不做：

- 不靠 task name 长期识别 HMM。

### Phase 4：历史 backfill dry-run

范围：

- 使用历史 HMM 文件和/或 QE DB 生成 preview。
- 对比现有归档 counts。
- 修正 parser 和 classification。

不做：

- 不正式写生产。

### Phase 5：历史 backfill 正式执行

范围：

- 生产执行一次正式补齐。
- 复跑验证幂等。
- UI 验证。
- 关闭 backfill write flag。

### Phase 6：稳定化

范围：

- 未来 HMM 演进默认自动记录。
- 定期检查 quarantine / excluded 记录。
- 补充 Research Pipeline Inspector 的过滤和 family summary。

## 16. 关键设计决策

| 问题 | 决策 |
|---|---|
| 先补历史还是先做自动能力 | 先做自动能力，再用同一能力补历史 |
| 历史补齐是否直接写 stage JSON | 不建议；应写可查询的 `backtest_record` |
| HMM-only sweep 是否进 QE Archive | 不批量进入，只保留 representative |
| HMM-only sweep 是否进 Research Pipeline | 不同 HMM 配置应进入 timeline |
| 重复相同配置 loop 是否全部保留 | 不全部保留，只保留 primary + duplicate count |
| 自动 hook 是否影响 QE 状态 | 不影响，best-effort |
| 未来识别 HMM 是否靠 task name | 不靠，使用 explicit metadata |
| UI 是否可写 | 不写，只读 Inspector |
| MCP 是否承载业务逻辑 | 不承载，只做 thin wrapper |
| 生产 backfill 是否可重复执行 | 必须幂等可复跑 |

## 17. 最终推荐落地策略

1. 在独立分支实现 `backtest_recording` 和 HMMBacktestRecorder。
2. 先在 dev DB 用历史文件完成 dry-run。
3. 验证 recorder 生成的结果与当前 HMM 历史分析一致。
4. 接入 QE 自动 hook，但默认关闭。
5. 开启 dev 自动 hook，跑一个小型 HMM QE loop 验证自动记录。
6. 合入 main。
7. 生产初始化 schema。
8. 生产执行 preview。
9. 人工确认 counts。
10. 生产执行一次历史补齐。
11. 复跑确认幂等。
12. 开启未来自动记录。
13. 关闭历史 backfill write。

该方案保证历史数据和未来数据在同一张表、同一套逻辑、同一个 UI、同一套幂等规则下管理，避免先手工补录后再二次迁移。
