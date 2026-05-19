# QE Archive 实验级与 Loop 级手动入仓详细设计

> 日期：2026-05-19  
> GitHub Issue：[#81](https://github.com/licong01-cloud/AIstock/issues/81)  
> 分支：`feature/qe-archive-manual-loop-selection-20260519`  
> Worktree：`F:\Dev\AIstock_worktrees\qe-archive-manual-loop-selection-20260519`  
> 状态：设计草案，待评审后进入实现  
> 适用范围：QE 单实验、QE 自动/自定义/策略演进任务、Research Pipeline 触发的 QE 验证、QE Archive UI、QE Archive MCP、QE Experiment MCP。

## 1. 背景与结论

当前 AIstock 已经具备 QE Archive 数仓、历史 backfill API、QE Archive UI 和 Research Pipeline 的 HMM 研究记录能力。但后续治理目标已经明确调整：

1. **Research Pipeline 自动记录研究过程**：只有通过 RP 研究流程发起或显式绑定 RP 的实验，必须自动写入 RP 研究轨迹。
2. **QE Archive 数仓手动入仓**：QE Archive 不再追求所有 QE 实验自动全量入仓，而是只保存人工确认后具备长期分析价值的核心样本。
3. **数仓样本以 loop 为核心粒度**：自动演进任务的每个 loop 是独立配置、独立指标和独立复现单元；任务级操作只是批量选择入口。
4. **实验级与 loop 级必须同时支持**：单次 QE 实验可以按 experiment 入仓；演进任务可以一次性入仓全部有效 loop，也可以只选择若干 loop 入仓。
5. **候选标识不等于写仓**：`qe_archive_eligible`、`qe_archive_representative`、`archive_action` 只能作为推荐或状态展示，不触发自动写入。

因此，本方案的最终形态是：

```text
Research Pipeline 自动记录研究轨迹
          |
          | 产生 QE 验证任务 / 外部 run link / 可入仓候选
          v
QE UI / RP UI / MCP 展示候选、状态、质量预览
          |
          | 人工选择 experiment / task / loop
          v
QE Archive preview -> confirmed execute -> 幂等写入数仓
```

## 2. Phase 0：现有能力调研

### 2.1 已有 API 与服务能力

| 能力 | 当前状态 | 证据 |
|---|---|---|
| `/api/v1/qe-archive/backfill` 支持 dry-run 与 confirmed write | 已有 | `backend/routers/qe_archive.py:229`-`backend/routers/qe_archive.py:263` |
| `QEArchiveBackfillRequest` 已包含 `experiment_ids`、`task_ids`、`loop_ids`、`task_id + loop_index` | 已有 | `backend/routers/qe_archive.py:24`-`backend/routers/qe_archive.py:46` |
| `/backfill/preview` 与 `/backfill/execute` 支持 backfill run 审计 | 已有 | `backend/routers/qe_archive.py:161`-`backend/routers/qe_archive.py:180` |
| service 层支持 task 展开为 loop | 已有 | `backend/services/qe_archive/backfill_service.py:352`-`backend/services/qe_archive/backfill_service.py:365` |
| service 层支持 explicit `loop_ids` | 已有 | `backend/services/qe_archive/backfill_service.py:366`-`backend/services/qe_archive/backfill_service.py:368` |
| service 层支持单个 `task_id + loop_index` | 已有，但只支持单个 loop_index | `backend/services/qe_archive/backfill_service.py:370`-`backend/services/qe_archive/backfill_service.py:375` |
| `task_ids` 默认跳过已入仓 loop | 已有 | `backend/services/qe_archive/source_assembler.py:178`-`backend/services/qe_archive/source_assembler.py:218` |
| QE Archive UI 有候选列表和批量写入入口 | 已有，但只在 `/qe-archive` 页面按候选 task/experiment 选择 | `frontend/src/app/qe-archive/page.tsx:360`-`frontend/src/app/qe-archive/page.tsx:421` |
| QE 自动演进详情页显示 loop 列表 | 已有，但没有数仓选择/入仓按钮 | `frontend/src/app/quantevolver/evolution/[taskId]/page.tsx:267`-`frontend/src/app/quantevolver/evolution/[taskId]/page.tsx:305` |
| QE 实验历史页显示 parent experiment 与 child loops | 已有，但没有数仓选择/入仓按钮 | `frontend/src/app/quantevolver/experiments/page.tsx:890`-`frontend/src/app/quantevolver/experiments/page.tsx:1010` |
| QE Archive MCP 只暴露 `source_mode + limit` 的 preview/execute | 不足 | `scripts/aistock_qe_archive_mcp_server.py:71`-`scripts/aistock_qe_archive_mcp_server.py:79` |
| RP HMM 记录已有 `qe_archive_eligible` / `qe_archive_representative` 字段 | 已有 | `backend/services/research_pipeline/models.py:232`-`backend/services/research_pipeline/models.py:265` |
| RP 自动记录需要显式研究上下文 | 已有 | `backend/services/research_pipeline/realtime_ingestion.py:87`-`backend/services/research_pipeline/realtime_ingestion.py:147` |

### 2.2 现有设计文档对齐

- `docs/architecture/qe_realtime_experiment_warehouse_detailed_design_20260502.md` 曾提出“每个 QE 单次实验或每个 loop 结束后实时入仓”，但现在需要由本设计修正为：**完成时可以产生候选/状态/审计事件，正式写入 QE Archive 必须人工确认**。
- `docs/architecture/qe_experiment_data_completeness_prewarehouse_plan_20260503.md` 已明确 QE runtime DB 不是永久历史库，数仓应成为清理源 QE 数据后的长期分析权威。本设计保持该目标，但把入仓触发从自动全量改为人工选择。
- `docs/architecture/research_pipeline_hmm_backtest_timeline_backfill_design_20260519.md` 已明确 RP 保存 HMM 研发轨迹，QE Archive 只保存 factor/model/strategy/execution representative。本设计沿用该边界。
- `tests/aistock_validation/modules/qe_archive.md` 已要求 backfill API 支持 dry-run、confirmed write、task 展开 loop、UI 候选选择和 worker 不影响 QE 状态。本设计在此基础上补充“loop 级多选”和“QE/RP 页面入口”。

### 2.3 当前缺口

1. **MCP 缺口**：`aistock-qe-archive` MCP 不能直接传 `experiment_ids`、`task_ids`、`loop_ids`、`loop_indices`。
2. **UI 粒度缺口**：`/qe-archive` 页面可选 task/experiment，但 QE 自动演进页面和实验历史页面不能直接选 loop 入仓。
3. **多 loop 精确选择缺口**：后端支持 `loop_ids` 列表，但 `task_id + loop_index` 只支持单个 index；UI/MCP 通常更需要 `task_id + loop_indices=[1,3,7]`。
4. **状态汇总缺口**：QE 页面需要显示 `未入仓 / 可入仓 / 已入仓 / 部分入仓 / 全部入仓`，不能再靠用户去 `/qe-archive` 单独判断。
5. **RP 候选到数仓缺口**：RP 能记录 HMM representative，但没有把 representative 作为人工入仓候选批量传给 QE Archive 的明确流程。
6. **文档一致性缺口**：旧数仓文档偏向“实时自动入仓”，需要在本设计和后续实现中明确以“人工确认入仓”为新准则。

## 3. 领域模型与粒度规则

### 3.1 三种入仓对象

| 对象 | 用户动作 | 实际写入粒度 | 数仓结果 |
|---|---|---|---|
| 单次 QE experiment | 选择一个或多个 experiment | experiment | 每个 experiment 生成 1 条 `qe_archive.run` |
| QE evolution task | 选择一个 task | loop | 展开该 task 下符合条件的 loop，每个 loop 生成 1 条 `qe_archive.run` |
| QE evolution loop | 选择若干 loop | loop | 每个选中的 loop 生成 1 条 `qe_archive.run` |

严禁把一个演进任务的多个 loop 合并为一条 `qe_archive.run`。任务级记录只应存在于 `qe_archive.backfill_run`、`backfill_run_item` 或聚合视图中。

### 3.2 入仓状态定义

| 状态 | 含义 | 展示位置 |
|---|---|---|
| `not_archived` / 未入仓 | 源 experiment/loop 尚无对应 `qe_archive.run` | QE UI、RP UI、QE Archive UI |
| `eligible` / 可入仓 | 通过质量/代表性初筛，但尚未人工确认 | QE UI、RP UI |
| `archived` / 已入仓 | 已存在对应 `qe_archive.run` | 所有相关页面 |
| `partially_archived` / 部分入仓 | task 下部分 loop 已入仓 | task 行、实验历史 parent 行 |
| `fully_archived` / 全部入仓 | task 下所有目标 loop 已入仓 | task 行、实验历史 parent 行 |
| `not_recommended` / 不建议入仓 | 重复、日频无效、缺关键配置、非研究价值样本 | QE UI、RP UI，可人工覆盖但需确认 |
| `manual_only` / 需人工判断 | `archive_policy=MANUAL_ONLY`，默认不自动写入 | QE Archive/RP UI |
| `skipped` / 已跳过 | 显式 `SKIP` 或数据质量不足 | QE Archive UI、审计表 |

### 3.3 候选推荐规则

默认推荐入仓的 loop 应满足：

1. `status=completed`。
2. 有有效 runtime contract，优先 1min/minute backtest。
3. 非重复配置，或同一配置族中的 representative。
4. 核心指标完整：年化、最大回撤、IR/Sharpe、IC/RankIC 至少应能读取主要项。
5. 对 HMM-only sweep：同一 factor/model/strategy/execution 组合下只推荐代表性 loop。
6. 对 RP 研究流程：`qe_archive_eligible=true` 或 `qe_archive_representative=true` 可作为推荐来源，但仍需人工点击确认。

## 4. API 设计

### 4.1 保留并强化现有 `/backfill`

现有 `/api/v1/qe-archive/backfill` 已经最接近目标，应作为 UI/MCP 的统一写入口。

当前请求模型：

```json
{
  "source": "experiment | loop | task | all",
  "experiment_ids": ["qe_..."],
  "task_ids": ["qe_..."],
  "loop_ids": ["qe_..._Loop1"],
  "task_id": "qe_...",
  "loop_index": 1,
  "status": "completed",
  "include_archived": false,
  "write": false,
  "confirm_write": "",
  "validate_after_write": true
}
```

### 4.2 新增 `loop_indices`

为支持用户在 UI/MCP 上按 loop 序号选择多个 loop，应新增：

```json
{
  "task_id": "qe_202605xx_xxxx",
  "loop_indices": [1, 3, 7]
}
```

语义：

- `task_id + loop_indices` 展开为该 task 下多个 loop。
- 与 `loop_ids` 同时传入时，两者合并后去重。
- `include_archived=false` 时跳过已入仓 loop。
- `loop_indices` 中不存在的 loop 应在 preview 中返回 `missing` item，不应静默忽略。
- execute 阶段如果存在 missing/invalid loop，默认不整体失败；返回 item 级失败，除非将来增加 `fail_fast=true`。

### 4.3 新增推荐/状态查询 API

为了避免 QE 页面自己拼接 archived runs，建议新增只读状态 API：

```http
POST /api/v1/qe-archive/source-status
```

请求：

```json
{
  "experiment_ids": ["qe_single_..."],
  "task_ids": ["qe_task_..."],
  "loop_ids": ["qe_task_Loop1"],
  "include_recommendation": true
}
```

返回：

```json
{
  "experiments": {
    "qe_single_...": {
      "archive_status": "archived",
      "run_ids": ["qear_run_..."],
      "eligible": true,
      "reason": "single_completed"
    }
  },
  "tasks": {
    "qe_task_...": {
      "archive_status": "partially_archived",
      "loop_count": 10,
      "archived_loop_count": 3,
      "eligible_loop_count": 4,
      "pending_loop_count": 7
    }
  },
  "loops": {
    "qe_task_Loop3": {
      "archive_status": "not_archived",
      "eligible": true,
      "recommended": true,
      "reason": "best_in_archive_family",
      "run_ids": []
    }
  }
}
```

实现建议：

- 查询 `qe_archive.run` 按 `experiment_id`、`task_id`、`loop_id` 聚合。
- 对 task 汇总使用 `qe_evolution_loops` 与 `qe_archive.run` LEFT JOIN。
- 推荐状态第一版只做轻量规则，不重算复杂研究分数。
- RP representative 可通过 `research_pipeline.backtest_record` 补充 `eligible/recommended`，但不能让 RP 直接写仓。

### 4.4 新增 preview wrapper API（可选）

如果现有 `/backfill` dry-run 返回信息足够，可不新增。若 UI 需要单独展示缺失 loop、重复、质量门槛、预计写入行数，可新增：

```http
POST /api/v1/qe-archive/selection/preview
```

第一阶段建议先复用 `/backfill`：`write=false` 即 preview，避免重复入口。

## 5. 后端实现方案

### 5.1 BackfillOptions 扩展

修改：

- `backend/routers/qe_archive.py`
- `backend/services/qe_archive/backfill_service.py`

新增字段：

```python
loop_indices: list[int] = Field(default_factory=list)
```

和 dataclass：

```python
loop_indices: Sequence[int] = ()
```

处理规则：

1. `_dedupe_non_empty` 继续处理字符串 ID。
2. 新增 `_dedupe_positive_ints` 处理 loop indices。
3. `_build_candidates` 在 `options.task_id and options.loop_indices` 时调用 assembler 批量查 loop refs。
4. 兼容旧 `loop_index` 字段，单个 index 转入 `loop_indices`。

### 5.2 SourceAssembler 扩展

修改：

- `backend/services/qe_archive/source_assembler.py`

新增：

```python
def list_loop_refs_for_task_indices(
    self,
    task_id: str,
    loop_indices: Sequence[int],
    *,
    status: str = "completed",
    include_archived: bool = False,
) -> list[dict[str, Any]]:
    ...
```

要求：

- 按 `task_id` + `loop_index = ANY(...)` 查询。
- 返回 `task_id`、`loop_id`、`loop_index`。
- 保留 `include_archived=false` 默认跳过已入仓。
- 提供缺失 index 列表给 preview 结果。若为了减少改动，也可以先在 service 层对比 requested vs returned，生成 synthetic skipped item。

### 5.3 Source Status Repository

建议在 `backend/services/qe_archive/repository.py` 增加只读方法：

```python
def get_source_archive_status(
    self,
    *,
    experiment_ids: Sequence[str] = (),
    task_ids: Sequence[str] = (),
    loop_ids: Sequence[str] = (),
) -> dict[str, Any]:
    ...
```

第一版也可以先放在 `backfill_service.py`，但长期应放 repository，避免 UI/API 直接组合 SQL。

### 5.4 幂等与重复处理

必须保持以下约束：

- 同一个 source experiment 再次入仓不生成重复 run。
- 同一个 task + loop_id 再次入仓不生成重复 run。
- `include_archived=false` 默认跳过已入仓项。
- `include_archived=true` 只能用于重新校验或明确 rebootstrap，不能作为普通 UI 默认值。
- 所有写入仍需 `confirm_write=QE_ARCHIVE_WRITE`。

## 6. MCP 设计

### 6.1 QE Archive MCP 新工具

修改：`scripts/aistock_qe_archive_mcp_server.py`

新增工具：

```python
@mcp.tool()
def qe_archive_backfill_selection_preview(
    experiment_ids: list[str] | None = None,
    task_ids: list[str] | None = None,
    loop_ids: list[str] | None = None,
    task_id: str | None = None,
    loop_indices: list[int] | None = None,
    status: str = "completed",
    include_archived: bool = False,
) -> dict[str, Any]:
    ...
```

```python
@mcp.tool()
def qe_archive_backfill_selection_execute_confirmed(
    experiment_ids: list[str] | None = None,
    task_ids: list[str] | None = None,
    loop_ids: list[str] | None = None,
    task_id: str | None = None,
    loop_indices: list[int] | None = None,
    status: str = "completed",
    include_archived: bool = False,
    confirm_write: str | None = None,
) -> dict[str, Any]:
    ...
```

确认 token：继续使用 `QE_ARCHIVE_WRITE`，不是 `QE_ARCHIVE_BACKFILL`。原因是这是明确选择后的写仓动作，底层走 `/backfill` confirmed write。

### 6.2 保留旧 MCP 工具

保留：

- `qe_archive_backfill_preview(source_mode, limit, include_archived)`
- `qe_archive_backfill_execute_confirmed(source_mode, limit, include_archived, confirm_backfill)`

但 UI/人工选择场景应优先使用新 selection 工具，避免 `limit` 扫描误入仓。

### 6.3 QE Experiment MCP 边界

`aistock-qe-experiment` MCP 可以补充“实验完成后给出候选状态”的工具，但不应直接重复实现写仓逻辑。推荐方式：

- QE Experiment MCP 查询 QE task/loop 详情。
- 如果用户决定入仓，调用 QE Archive MCP selection execute。
- 如需在同一 MCP 暴露入口，也必须转发到 `/api/v1/qe-archive/backfill`，不得单独写 DB。

## 7. UI 设计

### 7.1 QE Archive 页面

现有 `/qe-archive` 页面保留作为集中治理页，但需要升级：

1. 修复/确认中文显示，避免乱码。
2. 候选列表支持展开 task 下 loop。
3. task 行支持：
   - `入仓推荐 loop`
   - `入仓全部有效 loop`
   - `展开后选择 loop 入仓`
4. 显示状态：`已入仓 x / 总 loop y / 推荐 z / 待入仓 n`。
5. `include_archived` 可以作为筛选项，但不能在 QE 实验历史页和自动演进页隐藏源实验。

### 7.2 QE 实验历史页

修改：`frontend/src/app/quantevolver/experiments/page.tsx`

目标：用户在历史页能直接看到每个实验/loop 的入仓状态。

实验行：

- 单实验：显示 `已入仓 / 未入仓 / 可入仓 / 不建议入仓`。
- 演进 parent：显示 `部分入仓 3/12`、`全部入仓 12/12`、`推荐入仓 4`。
- 操作：
  - `预览入仓`
  - `入仓推荐 loop`
  - `入仓全部有效 loop`

Loop 展开行：

- checkbox 选择。
- loop 级状态 badge。
- 操作：`预览该 loop`、`入仓该 loop`。
- 批量操作：`入仓选中 loop`。

### 7.3 QE 自动演进任务详情页

修改：`frontend/src/app/quantevolver/evolution/[taskId]/page.tsx`

目标：在 loop card 上直接操作，不需要跳转到 QE Archive 页面。

Header：

- `已入仓 x/y`
- `推荐 z`
- `待入仓 n`

Loop 卡片：

- `已入仓` badge。
- `可入仓` badge。
- checkbox。
- `入仓` button。
- 已入仓 loop 的按钮禁用并显示 run id 短 hash。

批量工具条：

- `选择推荐 loop`
- `选择未入仓 loop`
- `预览选中`
- `确认入仓选中`

### 7.4 RP 页面

修改：`frontend/src/app/research-pipeline/page.tsx`

目标：RP 只展示候选与状态，不直接把“研究记录”误解为“数仓记录”。

HMM/RP backtest timeline 中：

- `qe_archive_eligible=true` 显示 `推荐入仓`。
- `qe_archive_representative=true` 显示 `代表样本`。
- 若已有 matching archive run，显示 `已入仓`。
- 操作入口可以跳转 QE Archive selection 或直接调用 selection preview/execute，但必须人工确认。

## 8. 交互流程

### 8.1 单实验入仓

```text
用户在实验历史页选择 qe_experiment
  -> POST /qe-archive/source-status 查询状态
  -> POST /qe-archive/backfill write=false 预览
  -> 用户确认 QE_ARCHIVE_WRITE
  -> POST /qe-archive/backfill write=true
  -> 刷新 source-status 与 archived runs
```

请求示例：

```json
{
  "source": "experiment",
  "experiment_ids": ["qe_202605xx_xxxx"],
  "write": true,
  "confirm_write": "QE_ARCHIVE_WRITE",
  "validate_after_write": true,
  "min_metrics": 60,
  "min_curves": 3000,
  "min_factors": 1,
  "require_account_summary": true
}
```

### 8.2 演进任务全部有效 loop 入仓

```json
{
  "source": "task",
  "task_ids": ["qe_202605xx_xxxx"],
  "status": "completed",
  "include_archived": false,
  "write": true,
  "confirm_write": "QE_ARCHIVE_WRITE"
}
```

实际结果：展开 task 下所有未入仓 completed loop，逐个写入 `qe_archive.run`。

### 8.3 演进任务部分 loop 入仓

```json
{
  "source": "loop",
  "task_id": "qe_202605xx_xxxx",
  "loop_indices": [1, 3, 7],
  "status": "completed",
  "include_archived": false,
  "write": true,
  "confirm_write": "QE_ARCHIVE_WRITE"
}
```

或：

```json
{
  "source": "loop",
  "loop_ids": ["qe_..._Loop1", "qe_..._Loop3"],
  "write": true,
  "confirm_write": "QE_ARCHIVE_WRITE"
}
```

## 9. 数据清理联动

本功能实现后，原始 QE 实验清理应遵循：

1. 先看 QE/RP 页面是否显示已入仓或明确不需要入仓。
2. 对有价值实验，先 preview + confirmed write。
3. 入仓后验证 run quality。
4. 确认 `source-status=archived` 后，再允许删除 QE DB 源记录与文件系统产物。
5. 删除动作必须仍走 QE 后端删除 API，不能直接删 DB 和文件。

注意：数仓不是为了保存所有源实验，而是为了让源实验可清理后，仍保留核心可分析记录。

## 10. 分阶段实施计划

### Phase 1：后端选择性入仓能力补齐

文件：

- `backend/routers/qe_archive.py`
- `backend/services/qe_archive/backfill_service.py`
- `backend/services/qe_archive/source_assembler.py`
- `backend/services/qe_archive/repository.py`
- `backend/tests/test_qe_archive_repository_static.py`

任务：

1. 增加 `loop_indices`。
2. 增加 `list_loop_refs_for_task_indices`。
3. 增加 source status 查询服务/API。
4. 保持 `/backfill` preview/write 幂等。
5. 增加单测覆盖 experiment_ids、task_ids、loop_ids、task_id+loop_indices、重复写入、缺失 loop。

验收：

```powershell
python -m pytest backend/tests/test_qe_archive_repository_static.py -q
python -m nox -s qe_archive_backend
```

### Phase 2：MCP 明确 ID 入仓工具

文件：

- `scripts/aistock_qe_archive_mcp_server.py`
- 可选：`scripts/aistock_qe_experiment_mcp_server.py`

任务：

1. 增加 selection preview 工具。
2. 增加 selection execute confirmed 工具。
3. 使用 `QE_ARCHIVE_WRITE` 作为 confirm token。
4. 参数 sanitize：ID 使用现有 `sanitize_identifier`，loop_indices 校验正整数。
5. 保留旧 source_mode 工具。

验收：

```powershell
python -m py_compile scripts/aistock_qe_archive_mcp_server.py scripts/aistock_qe_experiment_mcp_server.py
```

若 dev backend 可用，再做 loopback smoke。

### Phase 3：前端 API SDK

文件：

- `frontend/src/lib/qe-archive/api.ts`

任务：

1. `BackfillRequest` 增加 `loop_indices`。
2. 增加 `sourceStatus()`。
3. 增加类型：`ArchiveSourceStatus`、`TaskArchiveStatus`、`LoopArchiveStatus`。
4. 封装 `previewSelection()` 与 `executeSelection()`，底层仍调用 `/backfill`。

验收：

```powershell
cd frontend
npm run typecheck
```

### Phase 4：QE UI 集成

文件：

- `frontend/src/app/quantevolver/experiments/page.tsx`
- `frontend/src/app/quantevolver/evolution/[taskId]/page.tsx`
- 可选抽取组件：`frontend/src/app/quantevolver/components/QEArchiveStatusBadge.tsx`
- 可选抽取 hook：`frontend/src/app/quantevolver/components/useQEArchiveSelection.ts`

任务：

1. 历史页加载 visible experiments/loops 后批量调用 `source-status`。
2. 自动演进任务页加载 loops 后调用 `source-status`。
3. 增加入仓状态 badge。
4. 增加 loop 级 checkbox 与批量操作。
5. 所有写入必须先 preview，再 confirm。
6. 页面不得隐藏实验或 loop；筛选只能影响候选/操作集合，不能影响源列表可见性。

验收：

```powershell
cd frontend
npm run typecheck
```

如 E2E 环境可用，增加 mocked Playwright 用例。

### Phase 5：QE Archive UI 升级

文件：

- `frontend/src/app/qe-archive/page.tsx`
- `frontend/tests/qe-archive/qe-archive-flows.spec.ts`

任务：

1. 修复中文显示并统一文案。
2. 候选 task 支持展开 loop。
3. 支持选择推荐 loop、全部未入仓 loop、手动选中 loop。
4. 保留 task/experiment 批量操作。
5. 明确显示 `include_archived` 是筛选候选，不是隐藏源实验。

验收：

```powershell
$env:QE_ARCHIVE_UI_MOCK_API='1'
python -m nox -s qe_archive_ui
```

### Phase 6：RP UI 状态联动

文件：

- `frontend/src/app/research-pipeline/page.tsx`
- `frontend/src/lib/research-pipeline/api.ts`

任务：

1. RP backtest record 显示 `可入仓 / representative / 已入仓`。
2. 对有 source loop 的记录，调用 QE Archive source status 显示实际入仓状态。
3. 提供跳转或直接 preview 入仓入口。
4. 保持 RP 自动记录，QE Archive 手动确认。

验收：

```powershell
cd frontend
npm run typecheck
```

### Phase 7：真实 DB dry-run 与小样本 confirmed write

前置：必须由用户确认允许写生产或指定 dev DB。

步骤：

1. 选取 1 个已完成单实验或 1 个小型 task 的 1 个 loop。
2. 执行 preview。
3. 执行 confirmed write。
4. 查询 run quality。
5. 重复执行一次，确认幂等。
6. source status 显示已入仓。

建议命令：

```powershell
python scripts/qe_archive_data_quality_smoke.py
```

或通过 API/MCP 执行同等 smoke。

## 11. 测试矩阵

| 层级 | 测试内容 | 必须覆盖 |
|---|---|---|
| Backend unit | backfill options 与 candidate build | `experiment_ids`、`task_ids`、`loop_ids`、`loop_indices`、去重、missing loop |
| Backend API | `/backfill`、`/source-status` | write 缺 confirm 拒绝；preview 不写；execute 写入；状态准确 |
| MCP | wrapper 参数与确认 | preview 无 confirm；execute 必须 `QE_ARCHIVE_WRITE` |
| Frontend typecheck | SDK 与页面类型 | 新字段、新状态、新按钮 |
| Frontend E2E/mock | 页面交互 | 状态 badge、选择 loop、preview、confirm write |
| DB smoke | 真实 schema | run count、quality、幂等、source status |
| Guardrail | 不影响 QE runtime | archive 失败不改变 QE 实验状态；不重启生产 8001 |

## 12. 风险与防护

| 风险 | 防护 |
|---|---|
| 用户误点全量入仓大量无价值 loop | 默认推荐“选中/推荐 loop”，全量需要明显确认；`include_archived=false`；preview 显示预计数量 |
| 重复入仓 | source 唯一约束 + `include_archived=false` + execute 幂等测试 |
| RP 候选被误认为已经入仓 | UI 使用不同 badge：`推荐入仓` 与 `已入仓` 必须颜色/文案区分 |
| QE 页面再次隐藏实验 | 禁止在实验历史和自动演进页面隐藏源实验；只允许筛选操作候选 |
| MCP 扫描式 limit 误入仓 | 新增明确 ID selection 工具；旧 `source_mode+limit` 工具保留但不推荐用于人工选择 |
| 生产写入风险 | confirmed write token；真实写入前 preview；生产 DB 写操作需用户确认 |
| 旧自动实时入仓配置误开启 | `QE_ARCHIVE_REALTIME_ENABLED` 继续默认关闭；RP/QE 触发只产生候选/记录，不自动数仓写入 |

## 13. 非目标

- 不重做 QE 执行引擎。
- 不把 Research Pipeline 变成数仓。
- 不迁移所有 MCP 到统一 gateway。
- 不自动清理 QE 源 DB 或文件系统。
- 不把所有历史 HMM loop 强制入仓。
- 不将 task 多 loop 合并为单条数仓 run。

## 14. 验收标准

功能完成后，必须能证明：

1. 用户可以按 experiment 入仓单实验。
2. 用户可以按 task 一次性入仓多个 loop。
3. 用户可以只选择某几个 loop 入仓。
4. 用户可以先 preview，再 confirmed write。
5. UI 能显示 task/experiment/loop 的入仓状态。
6. MCP 能通过明确 ID 完成 preview 和 confirmed write。
7. 重复执行不会产生重复数仓 run。
8. RP 自动记录不等于数仓自动入仓。
9. QE 源实验页面不隐藏仍存在的实验/loop。
10. 所有验证记录完整，达到合入 main 前标准。
