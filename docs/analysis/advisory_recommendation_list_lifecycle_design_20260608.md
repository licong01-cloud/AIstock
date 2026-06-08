# 荐股列表生命周期与策略绑定演进设计方案（2026-06-08）

## 1. 文档状态

- 状态：详细设计，供后续完整开发与验收使用。
- 适用范围：荐股中心、Selection Center、StrategyPackage、每日复评、荐股回放、策略包组合与策略包替换。
- 非目标：不接入真实下单，不修改 StrategyPackage frozen manifest，不把投资表现指标做成程序硬门禁。
- 关键结论：荐股列表必须拥有独立标识和每日版本；StrategyPackage 只是底层信号来源，不再作为荐股列表的唯一标识。

## 2. 当前证据与实现基线

### 2.1 已确认的现有能力

| 能力 | 当前实现位置 | 结论 |
|---|---|---|
| 长期荐股任务标识 | `backend/db/migrations/add_advisory_program_lifecycle_20260604.sql`：`app.advisory_program.program_id` | 已有 `program_id`，可作为长期任务 ID |
| 单包/多包模式字段 | `frontend/src/lib/api/advisory.ts`：`package_mode`, `package_ids`, `package_weights`, `fusion_method` | 已支持 `single_package` / `fusion_pool` 类型表达 |
| 配置变更版本 | `backend/services/advisory_program.py`：`update_program()` 对配置变更执行 `program.version + 1` | 已有粗粒度配置版本 |
| 每日复评自动生成 selection run | `backend/services/advisory_program.py`：`_selection_run_for_review()` | 不需要人工填写 selection run ID，符合最新交互方向 |
| 回放支持单包和融合池 | `backend/services/advisory_program.py`：`run_replay()` 通过当前 `program.package_mode` 调用 `_selection_run_for_review()` | 支持按当前任务绑定配置回放 |
| 回放无交易副作用 | `backend/services/advisory_program.py`：`run_replay()` 内部 `_evaluate_review(..., preview=True)` | 回放不写当前活跃荐股池 |
| 前端回放入口 | `frontend/src/app/paper-v2/advisory/page.tsx`：生命周期回放区 | 目前只支持开始/结束日期，不支持临时策略包配置 |

### 2.2 已确认的现有缺口

1. 缺少独立 `recommendation_list_version_id`，无法按交易日展示“初始列表”和“每日复评后的完整列表版本”。
2. 缺少 `strategy_binding_version_id`，`program.version` 同时承担任务配置、策略包绑定、复评参数变化，语义过重。
3. 回放请求无法临时覆盖 `package_mode` / `package_ids` / `package_weights`；必须先改任务配置或手工传 candidates。
4. 前端没有“修改已有荐股任务策略包配置”的界面。
5. 回放结果缺少新旧配置对比：重合度、换手率、进入/退出数量、收益/回撤、每日动作分布。
6. 每日复评结果没有形成完整“当日荐股列表版本”，UI 只能看到当前活跃池和决策记录。
7. 退出股票缺少面向用户的结构化操作建议：退出原因、建议价格口径、生效交易日、不可交易 fallback。
8. 当前 `fusion_pool` 只映射 `weighted_rank_fusion`；Selection Center 已有 `weighted_fusion` / `intersection` / `union` 概念，荐股侧尚未完全对齐。

## 3. 设计原则

### 3.1 标识分层原则

`StrategyPackage` 不再代表荐股列表。荐股功能需要以下独立标识：

| 标识 | 建议命名 | 含义 | 生命周期 |
|---|---|---|---|
| 策略包 ID | `package_id` | alpha core：因子、模型、训练证据、manifest hash | 策略包资产生命周期 |
| 荐股任务 ID | `program_id` / `recommendation_book_id` | 长期运行的荐股账本，例如“每日 Top20 荐股任务” | 长期稳定 |
| 策略绑定版本 ID | `binding_version_id` | 某一时期使用的单包/多包/权重/融合方法 | 每次策略配置变更新增 |
| 回放场景 ID | `replay_scenario_id` | 临时验证用配置草稿和回放参数 | 可选保存，人工诊断生命周期 |
| 回放执行 ID | `replay_run_id` | 一次回放执行证据 | 每次回放唯一 |
| 复评执行 ID | `review_run_id` | 某交易日一次复评执行 | 每次复评唯一 |
| 荐股列表版本 ID | `list_version_id` | 某交易日发布/生成的完整荐股列表 | 每个交易日或每次复评唯一 |
| 个股生命周期 ID | `episode_id` | 单只股票从进入到退出的一段推荐生命周期 | 个股级 |
| 个股决策 ID | `decision_id` | 某交易日对某股票的操作建议 | 每日个股级 |

### 3.2 连续演进原则

每日复评的输入不是空白列表，而是：

```text
上一交易日 RecommendationListVersion
  + 当前 StrategyBindingVersion 产生的当日候选/评分
  + review_policy / price policy / data readiness
  -> 当日 RecommendationListVersion
  -> 每只股票的 ItemDecision
```

当底层策略包替换时，也不推翻当前荐股任务：

```text
L(2026-06-05, binding=B1)
  + 新绑定 B2 产生 2026-06-08 候选
  + 人工确认后的 review_policy
  -> L(2026-06-08, binding=B2)
```

### 3.3 无投资表现硬门禁原则

系统只做工程校验和审计留痕，不做投资效果硬拦截。

允许保留的工程校验：

- 策略包 ID 存在。
- 策略包可生成目标交易日 selection run。
- 多包权重格式合法，权重为正数。
- 目标交易日与数据可用性可解释。
- 回放或应用配置时记录操作者、原因、配置差异和确认时间。
- 失败时 fail-fast，不能静默 fallback 成成功。

不得设置为硬门禁的指标：

- 最近 N 日收益是否优于旧包。
- 最大回撤是否低于阈值。
- 换手率是否低于阈值。
- 单日替换股票数量是否低于 3/5 只。
- 新旧列表重合度是否达标。
- 是否已经 shadow preview 足够天数。

这些指标只能展示给人工判断，人工确认后即可应用策略包替换或权重修改。

### 3.4 StrategyPackage 边界原则

遵循 `docs/architecture/strategy_package_platform_boundary_contract_20260520.md`：

- StrategyPackage 只保存 alpha core。
- top_k、权重、融合方式、复评策略、可交易性、价格口径属于平台运行能力。
- 每次影响选股/复评/收益口径的配置变化必须形成平台版本和审计记录，不能修改 StrategyPackage frozen manifest。

## 4. 目标用户流程

### 4.1 初始创建荐股任务

1. 用户在荐股中心选择一个或多个策略包。
2. 选择策略模式：
   - `single_package`
   - `weighted_rank_fusion`
   - `union`
   - `intersection`
   - 未来：`sleeve`
3. 配置目标数量、权重、复评策略、价格口径。
4. 点击“创建并启用”。
5. 系统生成：
   - `program_id`
   - 初始 `binding_version_id`
   - 初始 `review_run_id`
   - 初始 `list_version_id`
   - TopN 个股 `ENTER` 决策
6. UI 显示“初始荐股列表 V0”和每只股票的进入理由。

### 4.2 每日复评

1. 页面在每个已启用任务行展示“执行复评”按钮。
2. 按钮不要求人工填写 selection run ID。
3. 系统根据数据可用最新交易日自动确定目标复评日。
4. 复评基于上一日列表和当前策略绑定版本生成新列表版本。
5. UI 显示：
   - 当日列表版本。
   - 变更摘要。
   - 每只股票操作建议。
   - 退出列表与退出原因。
6. 同一交易日已复评后，执行按钮禁用，但可查看详情和预览。

### 4.3 回放验证新策略包

1. 用户打开某个荐股任务。
2. 在“回放配置草稿”中选择新策略包或多包权重。
3. 执行最近 N 个交易日回放。
4. 系统生成回放结果，但不修改当前任务和当前荐股池。
5. UI 展示新旧对比：
   - 列表重合度。
   - 换手率。
   - 新增/退出/保留数量。
   - 收益、回撤、胜率。
   - 每日动作分布。
   - 每只股票动作差异。
6. 用户可随时修改回放草稿，再次回放。

### 4.4 人工应用策略包替换

1. 用户查看回放结果。
2. 点击“应用为当前任务策略配置”。
3. 系统弹出确认：
   - 新旧 package 列表。
   - 新旧权重。
   - 回放区间和回放执行 ID。
   - 提示“回放结果仅供人工判断，系统不做收益门禁”。
4. 用户填写变更原因，可选关联回放结果。
5. 系统创建新的 `binding_version_id`，更新当前任务的 active binding。
6. 不清空当前荐股池，不删除旧列表版本。
7. 下一交易日复评使用新绑定，在上一交易日列表基础上迭代。

## 5. 数据模型设计

### 5.1 现有表保留

| 表 | 处理方式 |
|---|---|
| `app.advisory_program` | 保留，作为长期荐股任务主表 |
| `app.advisory_program_package` | 保留，可作为历史兼容；新增 binding 表后逐步降为兼容视图/冗余索引 |
| `app.advisory_daily_review` | 保留，但新增 `review_run_id`、`list_version_id`、`binding_version_id` 关联 |
| `app.advisory_episode_return` | 保留，继续表示单股 episode 生命周期和收益快照 |
| `app.advisory_replay_run` | 保留，扩展 replay config 与对比 summary |
| `app.advisory_program_metric_snapshot` | 保留，补充按 list/binding/version 维度聚合 |

### 5.2 新增表：策略绑定版本

```sql
CREATE TABLE IF NOT EXISTS app.advisory_strategy_binding_version (
    binding_version_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    program_version INTEGER NOT NULL,
    package_mode TEXT NOT NULL,
    package_ids JSONB NOT NULL,
    package_weights JSONB NOT NULL,
    fusion_method TEXT,
    package_set_hash TEXT NOT NULL,
    fusion_policy_sha256 TEXT,
    runtime_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    effective_from_trade_date DATE,
    effective_to_trade_date DATE,
    activation_status TEXT NOT NULL,
    activation_reason TEXT,
    source_replay_run_id TEXT,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at TIMESTAMPTZ,
    binding_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_binding_mode_check CHECK (
        package_mode IN ('single_package', 'weighted_rank_fusion', 'fusion_pool', 'union', 'intersection', 'sleeve_future')
    ),
    CONSTRAINT advisory_binding_status_check CHECK (
        activation_status IN ('DRAFT', 'ACTIVE', 'RETIRED')
    )
);
```

字段说明：

- `binding_version_id`：策略绑定版本，不等同于 `program.version`。
- `program_version`：创建该绑定时的任务配置版本。
- `package_mode`：统一表达单包、多包加权、并集、交集、未来 sleeve。
- `source_replay_run_id`：可选，表示该绑定由某次回放人工确认后应用。
- `activation_status`：同一任务同一时刻只能有一个 `ACTIVE` binding。
- `effective_from_trade_date`：人工指定或系统默认下一可复评交易日。

索引与约束：

```sql
CREATE INDEX IF NOT EXISTS idx_advisory_binding_program_created
    ON app.advisory_strategy_binding_version(program_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_binding_one_active
    ON app.advisory_strategy_binding_version(program_id)
    WHERE activation_status = 'ACTIVE';
```

### 5.3 新增表：复评执行

```sql
CREATE TABLE IF NOT EXISTS app.advisory_review_run (
    review_run_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    binding_version_id TEXT NOT NULL REFERENCES app.advisory_strategy_binding_version(binding_version_id),
    trade_date DATE NOT NULL,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    data_source TEXT NOT NULL,
    selection_run_id TEXT,
    selection_run_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    runtime_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    error_json JSONB,
    created_by TEXT,
    run_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_review_run_type_check CHECK (run_type IN ('PREVIEW', 'RUN', 'REPLAY')),
    CONSTRAINT advisory_review_run_status_check CHECK (status IN ('SUCCEEDED', 'WAITING_DATA', 'FAILED'))
);
```

验收要求：

- `RUN` 类型同一 `program_id + trade_date` 只能成功一次，除非未来显式设计“修订版本”。
- `PREVIEW` 可多次运行，不写当前列表。
- `REPLAY` 可多次运行，只作为诊断证据。

### 5.4 新增表：荐股列表版本

```sql
CREATE TABLE IF NOT EXISTS app.advisory_recommendation_list_version (
    list_version_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    binding_version_id TEXT NOT NULL REFERENCES app.advisory_strategy_binding_version(binding_version_id),
    review_run_id TEXT NOT NULL REFERENCES app.advisory_review_run(review_run_id),
    trade_date DATE NOT NULL,
    previous_list_version_id TEXT,
    version_status TEXT NOT NULL,
    target_count INTEGER NOT NULL,
    active_count INTEGER NOT NULL,
    entered_count INTEGER NOT NULL,
    held_count INTEGER NOT NULL,
    exited_count INTEGER NOT NULL,
    waiting_count INTEGER NOT NULL,
    changed_count INTEGER NOT NULL,
    turnover_rate NUMERIC,
    overlap_rate NUMERIC,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    list_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_list_version_status_check CHECK (
        version_status IN ('PREVIEW', 'PUBLISHED', 'REPLAY')
    )
);
```

核心语义：

- `PUBLISHED`：每日正式复评生成，驱动当前荐股列表。
- `PREVIEW`：当日预览，不影响当前荐股列表。
- `REPLAY`：历史回放中的虚拟版本。
- `previous_list_version_id`：让 UI 可以展示从上一版到当前版的变化。

### 5.5 新增表：列表个股明细

```sql
CREATE TABLE IF NOT EXISTS app.advisory_recommendation_list_item (
    list_item_id TEXT PRIMARY KEY,
    list_version_id TEXT NOT NULL REFERENCES app.advisory_recommendation_list_version(list_version_id) ON DELETE CASCADE,
    program_id TEXT NOT NULL,
    binding_version_id TEXT NOT NULL,
    episode_id TEXT,
    symbol TEXT NOT NULL,
    item_state TEXT NOT NULL,
    action TEXT NOT NULL,
    previous_action TEXT,
    rank INTEGER,
    score NUMERIC,
    previous_rank INTEGER,
    previous_score NUMERIC,
    entry_price NUMERIC,
    exit_price NUMERIC,
    price_basis TEXT,
    effective_trade_date DATE,
    reason_code TEXT NOT NULL,
    operation_advice_json JSONB NOT NULL,
    component_scores_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    item_payload_json JSONB NOT NULL,
    CONSTRAINT advisory_list_item_state_check CHECK (
        item_state IN ('ACTIVE', 'EXITED', 'WAITING', 'WATCH')
    ),
    CONSTRAINT advisory_list_item_action_check CHECK (
        action IN ('ENTER', 'HOLD', 'EXIT', 'REDUCE', 'ADD', 'WAITING', 'SKIP', 'UNTRADABLE', 'WATCH')
    )
);
```

`operation_advice_json` 标准结构：

```json
{
  "advice_type": "EXIT",
  "human_label": "建议退出",
  "reason_summary": "排名跌出阈值并满足确认条件",
  "price_basis": "next_open_executable",
  "suggested_price_band": {
    "min_price": 10.1,
    "max_price": 10.6
  },
  "effective_trade_date": "2026-06-09",
  "valid_until": "2026-06-09",
  "fallback_plan": "若停牌或无法成交，则保持 WAITING 并在下一交易日继续复评",
  "risk_note": "该建议为模型复评结果，不保证实际成交价格或收益"
}
```

### 5.6 扩展回放表

在 `app.advisory_replay_run` 的 `replay_config_json` 中增加：

```json
{
  "base_binding_version_id": "advb_xxx",
  "draft_binding": {
    "package_mode": "weighted_rank_fusion",
    "package_ids": ["pkg_a", "pkg_b"],
    "package_weights": {"pkg_a": 0.4, "pkg_b": 0.6},
    "fusion_method": "weighted_rank_fusion"
  },
  "compare_to_binding_version_id": "advb_old",
  "compare_metrics": {
    "overlap_rate_by_date": {},
    "turnover_rate_by_date": {},
    "entered_count_by_date": {},
    "exited_count_by_date": {}
  },
  "manual_gate": false
}
```

注意：`manual_gate=false` 表示系统不把指标作为应用门禁。

## 6. 后端服务设计

### 6.1 Binding 服务

新增 `AdvisoryStrategyBindingService` 或在 `AdvisoryProgramService` 中拆分 binding 子服务：

```python
class AdvisoryStrategyBindingService:
    def create_initial_binding(program: AdvisoryProgram) -> AdvisoryStrategyBindingVersion: ...
    def get_active_binding(program_id: str) -> AdvisoryStrategyBindingVersion: ...
    def create_draft_binding(program_id: str, payload: BindingPayload) -> AdvisoryStrategyBindingVersion: ...
    def activate_binding(
        program_id: str,
        draft_or_payload: BindingPayload,
        *,
        activation_reason: str,
        source_replay_run_id: str | None,
        created_by: str | None,
        effective_from_trade_date: date | None,
    ) -> AdvisoryStrategyBindingVersion: ...
```

实现细节：

- `create_initial_binding()` 在创建荐股任务时自动调用。
- `activate_binding()` 必须在一个事务内：
  1. 将旧 active binding 标记为 `RETIRED` 并设置 `effective_to_trade_date`。
  2. 插入新 binding，状态为 `ACTIVE`。
  3. 更新 `advisory_program.package_*` 兼容字段，保证旧 API 不立即断裂。
  4. 写入审计 payload。
- 不校验收益、回撤、换手率等投资指标。
- 仅校验 package ID 格式、权重合法性、模式合法性。

### 6.2 复评服务

扩展当前 `run_review_from_selection()`：

```python
def run_review_from_selection(
    program_id: str,
    *,
    trade_date: date,
    binding_version_id: str | None = None,
    selection_run_id: str | None = None,
    data_source: str = "DB_HISTORICAL",
    runtime_config: dict[str, Any] | None = None,
    candidates: list[Mapping[str, Any]] | None = None,
    market_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
    preview: bool = False,
) -> AdvisoryReviewResult:
    ...
```

实现要求：

- `binding_version_id` 为空时使用 active binding。
- 根据 binding 的 `package_mode` 映射 Selection Center：
  - `single_package` -> `SelectionMode.SINGLE_PACKAGE`
  - `weighted_rank_fusion` / `fusion_pool` -> `SelectionMode.WEIGHTED_FUSION`
  - `union` -> `SelectionMode.UNION`
  - `intersection` -> `SelectionMode.INTERSECTION`
- 每次复评先创建 `review_run_id`。
- 正式运行成功后创建 `list_version_id`。
- 每个 decision 同步写入 `advisory_daily_review` 和 `advisory_recommendation_list_item`。
- 预览只返回 `PREVIEW` list version payload，不更新 `latest_review_trade_date`。

### 6.3 回放服务

扩展 `run_replay()` 请求：

```python
class AdvisoryReplayRequest(BaseModel):
    start_date: date
    end_date: date
    candidates_by_date: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    market_by_date: dict[str, dict[str, dict[str, Any]]] = Field(default_factory=dict)
    data_source: str = "DB_HISTORICAL"
    runtime_config: dict[str, Any] = Field(default_factory=dict)
    entry_price_basis: str | None = None
    exit_price_basis: str | None = None
    draft_binding: AdvisoryBindingPayload | None = None
    compare_to_binding_version_id: str | None = None
    include_daily_items: bool = True
```

实现要求：

- `draft_binding` 存在时，回放使用该临时 binding，不修改 active binding。
- `draft_binding` 不落正式 active binding；只写入 replay config。
- `compare_to_binding_version_id` 存在时，后端同时跑基准回放并生成对比 summary；若性能压力较大，可第一阶段返回 draft 回放，第二阶段提供 compare endpoint。
- 回放中的每个交易日生成虚拟 `RecommendationListVersion`，`version_status=REPLAY`。
- 回放结果必须包含 `daily_reviews`、`daily_list_versions`、`episodes`、`summary`、`compare_summary`。

### 6.4 操作建议生成

新增 `OperationAdviceBuilder`：

```python
class OperationAdviceBuilder:
    def build(
        *,
        action: str,
        reason_code: str,
        trade_date: date,
        entry_price_basis: str,
        exit_price_basis: str,
        evidence: AdvisoryCandidate | None,
        episode: AdvisoryEpisode | None,
    ) -> dict[str, Any]:
        ...
```

动作映射：

| action | 建议类型 | 默认说明 |
|---|---|---|
| `ENTER` | 建议进入/加入列表 | 给出入选排名、分数、价格口径、风险提示 |
| `HOLD` | 建议继续持有/保留 | 给出当前排名、分数、持有天数、回撤 |
| `EXIT` | 建议退出 | 给出退出原因、退出价格口径、生效交易日 |
| `REDUCE` | 建议降低关注/减配 | 第一阶段可不启用，保留结构 |
| `WAITING` | 等待数据/等待可交易 | 明确缺失项，不显示为无反应 |
| `UNTRADABLE` | 不可交易 | 停牌、涨跌停、无价格等 fallback |
| `WATCH` | 观察 | 非当前 TopN 但进入观察池 |

### 6.5 变更摘要计算

新增 `RecommendationListDiffService`：

```python
def diff_lists(previous: list[RecommendationItem], current: list[RecommendationItem]) -> dict[str, Any]:
    return {
        "entered": [...],
        "exited": [...],
        "held": [...],
        "rank_changed": [...],
        "waiting": [...],
        "overlap_rate": 0.0,
        "turnover_rate": 0.0,
        "changed_count": 0
    }
```

计算口径：

- `overlap_rate = len(prev_active_symbols & curr_active_symbols) / max(len(prev_active_symbols), 1)`
- `turnover_rate = (entered_count + exited_count) / max(target_count, 1)`
- 指标只用于展示和人工判断，不作为替换门禁。

## 7. API 设计

### 7.1 查询任务绑定版本

```http
GET /api/v1/advisory/programs/{program_id}/bindings
```

返回：

```json
{
  "ok": true,
  "bindings": [
    {
      "binding_version_id": "advb_xxx",
      "program_id": "advp_xxx",
      "package_mode": "weighted_rank_fusion",
      "package_ids": ["pkg_a", "pkg_b"],
      "package_weights": {"pkg_a": 0.4, "pkg_b": 0.6},
      "activation_status": "ACTIVE",
      "source_replay_run_id": "advrp_xxx"
    }
  ]
}
```

验收：

- 新建任务后至少返回 1 个 active binding。
- 修改策略配置后返回历史 binding 和当前 active binding。
- 同一任务 active binding 数量恒为 1。

### 7.2 回放草稿执行

```http
POST /api/v1/advisory/programs/{program_id}/replay
```

请求：

```json
{
  "start_date": "2026-06-01",
  "end_date": "2026-06-05",
  "draft_binding": {
    "package_mode": "weighted_rank_fusion",
    "package_ids": ["pkg_a", "pkg_b"],
    "package_weights": {"pkg_a": 0.4, "pkg_b": 0.6},
    "fusion_method": "weighted_rank_fusion"
  },
  "compare_to_binding_version_id": "advb_old",
  "runtime_config": {
    "selection_artifact_config": {
      "auto_generate": true,
      "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE"
    }
  }
}
```

返回：

```json
{
  "ok": true,
  "replay": {
    "replay_run": {"replay_run_id": "advrp_xxx", "status": "SUCCEEDED"},
    "daily_list_versions": [],
    "summary": {},
    "compare_summary": {
      "overlap_rate_avg": 0.55,
      "turnover_rate_avg": 0.35,
      "entered_count_total": 18,
      "exited_count_total": 16,
      "return_bps": 120.5,
      "max_drawdown_bps": -80.0
    },
    "manual_gate": false
  }
}
```

验收：

- 传入 `draft_binding` 后，当前 active binding 不变化。
- 回放结果的 `replay_config_json.draft_binding` 与请求一致。
- 回放失败时返回结构化错误，不写“成功”状态。

### 7.3 应用策略绑定

```http
POST /api/v1/advisory/programs/{program_id}/bindings/apply
```

请求：

```json
{
  "binding": {
    "package_mode": "weighted_rank_fusion",
    "package_ids": ["pkg_a", "pkg_b"],
    "package_weights": {"pkg_a": 0.4, "pkg_b": 0.6},
    "fusion_method": "weighted_rank_fusion"
  },
  "source_replay_run_id": "advrp_xxx",
  "activation_reason": "人工回放验证后决定替换为新策略包组合",
  "effective_from_trade_date": "2026-06-09",
  "created_by": "operator"
}
```

返回：

```json
{
  "ok": true,
  "binding": {
    "binding_version_id": "advb_new",
    "activation_status": "ACTIVE"
  },
  "program": {
    "program_id": "advp_xxx",
    "version": 3
  }
}
```

验收：

- 不要求 `source_replay_run_id` 必填；人工可直接应用。
- `activation_reason` 必填，防止无原因变更。
- 不校验收益、换手率、重合度。
- 应用后旧 active binding 变为 `RETIRED`。
- 当前 active pool 不被清空。
- 下一次复评使用新 active binding。

### 7.4 查询列表版本

```http
GET /api/v1/advisory/programs/{program_id}/list-versions?limit=20&offset=0
GET /api/v1/advisory/list-versions/{list_version_id}
```

验收：

- 初始创建/首次复评后有列表版本。
- 每个新交易日正式复评成功后新增一个 `PUBLISHED` 列表版本。
- 同一列表版本返回完整 TopN 明细和退出/等待明细。

### 7.5 每日复评结果扩展

`POST /reviews/run` 返回新增：

```json
{
  "review": {
    "review_run_id": "advrun_xxx",
    "list_version_id": "advlv_xxx",
    "binding_version_id": "advb_xxx",
    "change_summary": {},
    "decisions": [],
    "active_pool": []
  }
}
```

验收：

- 前端无需从多个接口拼接才能知道本次复评生成的列表版本。
- `change_summary.entered_count + held_count + exited_count + waiting_count` 与 decisions 可核对。

## 8. 前端设计

### 8.1 当前荐股任务卡片

每个任务行展示：

- 任务名称。
- 当前状态。
- 当前 active binding 摘要：模式、策略包数量、权重摘要。
- 最新复评交易日。
- 最新列表版本 ID（短码）。
- 操作按钮：
  - 预览复评
  - 执行复评
  - 查看列表版本
  - 回放验证
  - 修改策略配置

验收：

- 只有一个启用任务时，页面自动选中，不要求用户输入任何 ID。
- 已复评当日按钮禁用时，旁边显示原因：目标交易日已复评/数据未就绪/任务未启用。

### 8.2 当前荐股列表

表格列：

| 列 | 说明 |
|---|---|
| 股票 | symbol |
| 当前状态 | ACTIVE / WAITING / EXITED |
| 今日动作 | ENTER / HOLD / EXIT / WAITING |
| 排名/分数 | 当前绑定生成的 rank/score |
| 变化 | 新进、保留、退出、排名变化 |
| 操作建议 | 人类可读的 advice |
| 生效交易日 | `effective_trade_date` |
| 价格口径 | next_open_executable 等 |
| 证据 | 组件分数/策略包贡献/来源 run |

验收：

- 每日复评后列表明显显示当天版本。
- 退出股票不会从页面消失，而是在“退出/调整”区展示。
- WAITING/UNTRADABLE 有明确原因，不显示为无反应。

### 8.3 列表版本时间轴

功能：

- 显示初始版本和每日复评版本。
- 点击任一日期查看完整列表。
- 支持“与上一版对比”。
- 支持“与当前版对比”。

验收：

- 可以查看 2026-06-04 初始选股候选和 2026-06-05 复评后列表的差异。
- UI 展示重合股票、退出股票、新进股票。

### 8.4 回放配置草稿

表单：

- 策略模式：单包、加权融合、并集、交集。
- 策略包选择：必须使用下拉/搜索，不允许手工输入长 ID 作为唯一方式。
- 权重编辑。
- 回放区间。
- 对比对象：当前 active binding 或任一历史 binding。
- 运行回放。

验收：

- 用户可以在回放区随时修改策略包配置并重新回放。
- 回放配置不会影响当前任务，除非点击“应用配置”。
- 页面明确提示：回放指标不作为系统门禁，由人工判断。

### 8.5 回放对比结果

展示：

- 每日 TopN 列表。
- 每日动作分布。
- 新旧重合度。
- 换手率。
- 收益/回撤。
- 退出数量。
- 个股动作差异。
- “应用配置”按钮。

验收：

- 回放结果可以解释“如果使用新策略包，最近 N 日列表会如何变化”。
- 即使回放收益差，用户仍可人工确认应用；系统不拦截。

### 8.6 应用配置确认

确认弹窗：

- 当前绑定摘要。
- 新绑定摘要。
- 来源回放 ID（如有）。
- 变更原因输入框。
- 勾选确认：“我已查看回放/预览结果，确认应用该配置；系统不以收益指标作为自动门禁。”

验收：

- 不填写变更原因不能提交。
- 提交后显示新 binding ID。
- 当前荐股列表不被清空。

## 9. 兼容 Selection Center 的融合模式

### 9.1 模式映射

| 荐股 binding mode | Selection Center mode | 首期是否实现 |
|---|---|---|
| `single_package` | `single_package` | 是 |
| `weighted_rank_fusion` | `weighted_fusion` | 是 |
| `fusion_pool` | `weighted_fusion` 兼容别名 | 是，逐步迁移 |
| `union` | `union` | 建议首期实现 |
| `intersection` | `intersection` | 建议首期实现 |
| `sleeve_future` | 暂无直接映射 | 保留，不启用 |

### 9.2 验收

- 单包回放、复评均成功。
- 双包加权回放、复评均成功，且 component scores 保留包级贡献。
- union/intersection 回放至少有 API 级测试覆盖；若首期不做 UI，API 明确返回支持状态。
- 不允许把多包融合规则写入 StrategyPackage manifest。

## 10. 迁移方案

### 10.1 生产数据兼容

迁移脚本执行：

1. 为每个 `app.advisory_program` 创建一个初始 `advisory_strategy_binding_version`。
2. 将现有 `package_mode/package_ids/package_weights/fusion_method` 写入 binding。
3. active program 的初始 binding 状态为 `ACTIVE`。
4. 为已有 `advisory_daily_review` 补 `binding_version_id` 可为空或按 program 当前 active binding 回填。
5. 不修改 StrategyPackage manifest，不修改 QE artifact，不修改历史 selection run。

### 10.2 当前 2026-06-05 数据的处理

对于当前已存在的 2026-06-05 复评记录：

- 可以生成一个历史 `list_version_id`。
- 20 条 `ENTER` 映射为 list item。
- `source_run_id=sel_fd213d46c9c5435fbf4f7be3ef0924b0` 保存在 evidence 中。
- 若不存在 2026-06-04 的正式 `advisory_daily_review`，不要伪造；可只把 2026-06-04 selection run 作为“对比参考”。

验收：

- 迁移后当前荐股池仍为 20 只。
- 2026-06-05 列表版本可在 UI 查看。
- 不生成虚假的 2026-06-04 正式复评版本。

## 11. 测试与验收矩阵

### 11.1 功能验收矩阵

| 功能 | 验收方式 | 验收标准 |
|---|---|---|
| 独立 binding 版本 | DB + API 测试 | 新建任务自动创建 active binding；应用新配置后旧 binding retired |
| 列表版本 | API + DB 测试 | 每次正式复评成功生成一个 `PUBLISHED` list version |
| 个股操作建议 | 单元测试 | ENTER/HOLD/EXIT/WAITING 均生成完整 `operation_advice_json` |
| 每日复评 | API 测试 | 不需要 selection run ID；自动生成当日 selection run；返回 list_version_id |
| 已复评禁用原因 | Playwright | 按钮禁用时显示具体目标交易日和原因 |
| 退出列表 | API + UI | EXIT 股票不从历史视图消失，退出建议可见 |
| 回放草稿 | API 测试 | `draft_binding` 回放不改变 active binding |
| 回放对比 | 单元 + API | 返回 overlap/turnover/entered/exited/return/drawdown 指标 |
| 应用配置 | API + UI | 人工确认后应用新 binding，不清空 active pool |
| 无硬门禁 | 单元测试 | 低收益/高换手回放结果仍可通过 apply 接口应用 |
| 单包兼容 | API | single package review/replay 成功 |
| 多包加权兼容 | API | weighted fusion review/replay 成功，保留 component scores |
| union/intersection | API | 若声明支持，则 review/replay 均成功；若不支持，返回明确 unsupported |
| 审计留痕 | DB 测试 | activation_reason、source_replay_run_id、created_by、payload 均入库 |

### 11.2 后端测试建议

扩展 `backend/tests/watchlist/test_advisory_api.py`：

1. `test_advisory_program_creates_initial_binding_version`
2. `test_advisory_review_creates_published_list_version`
3. `test_advisory_review_returns_operation_advice_for_each_action`
4. `test_advisory_replay_accepts_draft_single_package_binding_without_mutating_active_binding`
5. `test_advisory_replay_accepts_draft_weighted_fusion_binding`
6. `test_advisory_apply_binding_does_not_require_replay_gate`
7. `test_advisory_apply_binding_retires_previous_active_binding`
8. `test_advisory_apply_binding_does_not_clear_active_pool`
9. `test_advisory_list_versions_pagination_and_detail`
10. `test_advisory_union_or_intersection_binding_mode_if_enabled`

最低通过命令：

```bash
rtk python -m pytest backend/tests/watchlist/test_advisory_api.py -q
rtk python -m pytest backend/tests/selection_center/test_selection_center_api.py -q
```

### 11.3 前端测试建议

新增或扩展 advisory Playwright spec：

1. 单个启用任务自动选中。
2. 每个任务行显示“预览复评/执行复评”按钮。
3. 不出现需要手工输入 `sel_xxx` 的控件。
4. 当前列表显示 `list_version_id`、交易日、动作建议。
5. 退出列表显示 `EXIT` 股票和操作建议。
6. 回放草稿可选择策略包和修改权重。
7. 回放结果展示对比指标。
8. 应用配置弹窗要求填写原因。
9. 应用配置后页面显示新 binding 摘要。

最低通过命令：

```bash
rtk npm --prefix frontend run typecheck
rtk npm --prefix frontend run test:e2e -- advisory
```

若本地依赖缺失，需先按 frontend 依赖门禁修复，不把依赖缺失误判为业务失败。

### 11.4 DDL 验收

1. 新表和新字段都有 PostgreSQL comment。
2. 新增 migration 可重复执行。
3. 不修改 StrategyPackage frozen manifest。
4. 不修改历史 QE artifact。
5. `app.advisory_daily_review` 仍保持 append-only 语义。
6. 新增 list item / list version 可从 review run 完整追溯到 binding version 和 package IDs。

建议测试：

```bash
rtk python -m pytest backend/tests/watchlist/test_advisory_schema_contract.py -q
```

### 11.5 端到端业务验收

验收场景 A：初始荐股与每日复评

- Given：创建一个单策略包 Top20 荐股任务。
- When：执行 2026-06-05 复评。
- Then：
  - 生成 `review_run_id`。
  - 生成 `list_version_id`。
  - 当前列表 20 只。
  - 每只股票有 `ENTER/HOLD/EXIT/WAITING` 操作建议。
  - UI 可按日期查看该列表。

验收场景 B：策略包替换不清空列表

- Given：任务已有上一交易日 active list。
- When：人工应用新策略包 binding。
- Then：
  - 新 binding active。
  - 旧 binding retired。
  - 当前 active pool 不变。
  - 下一交易日复评使用新 binding。
  - 复评结果中说明 binding version 变化。

验收场景 C：回放不是门禁

- Given：用新策略包执行回放，结果显示高换手或收益较差。
- When：用户填写变更原因并确认应用。
- Then：
  - API 不因换手/收益指标拒绝。
  - 变更成功入库。
  - 审计记录保留回放结果与人工原因。

验收场景 D：多包加权回放

- Given：回放草稿选择两个策略包和权重。
- When：执行回放。
- Then：
  - 每个交易日生成候选。
  - component scores 中保留包级 rank/score/weight。
  - compare summary 显示列表差异。

## 12. 开发阶段拆分

### Phase 1：后端数据模型与兼容读取

实现：

- 新增 binding/list/review_run/list_item DDL。
- Repository 支持 binding CRUD 和 list version CRUD。
- 为现有 program 创建初始 active binding。
- 当前 API 保持兼容。

验收：

- schema contract 测试通过。
- 现有 advisory API 测试不回归。

### Phase 2：每日复评生成列表版本和操作建议

实现：

- 正式复评写 `review_run`、`list_version`、`list_item`。
- review response 返回 `review_run_id/list_version_id/change_summary`。
- `OperationAdviceBuilder` 覆盖 ENTER/HOLD/EXIT/WAITING。

验收：

- 单包和融合池复评测试通过。
- 每条 decision 都有 advice。

### Phase 3：回放草稿与新旧对比

实现：

- replay request 支持 `draft_binding`。
- replay result 返回 `daily_list_versions` 和 `compare_summary`。
- 回放不影响 active binding。

验收：

- draft binding 回放后 active binding 不变。
- 高换手/低收益不会导致 replay 或 apply 自动失败。

### Phase 4：应用策略配置

实现：

- 新增 `POST /bindings/apply`。
- 人工原因必填。
- 不强制回放 ID。
- 应用后不清空 active pool。

验收：

- apply 接口测试覆盖无 replay、有关联 replay、低收益 replay 三种场景。

### Phase 5：前端完整交互

实现：

- 任务行策略绑定摘要。
- 列表版本时间轴。
- 当前列表和退出建议。
- 回放草稿编辑器。
- 回放对比结果。
- 应用配置弹窗。

验收：

- Playwright 覆盖主要交互。
- 不出现要求用户手工输入 `sel_xxx` 的复评路径。

### Phase 6：union/intersection 与未来 sleeve 扩展

实现：

- Advisory binding mode 与 Selection Center mode 对齐。
- union/intersection 后端路径和测试。
- sleeve 只保留 schema，不启用时明确 unsupported。

验收：

- API 对已启用模式成功。
- 对未启用模式返回明确 `UnsupportedFeatureError`。

## 13. 风险与防误区

### 13.1 不要把回放指标做成硬门禁

错误做法：

```text
if turnover_rate > 0.25:
    reject_apply()
```

正确做法：

```text
展示 turnover_rate，记录人工确认和变更原因，允许应用。
```

### 13.2 不要以 package_id 命名荐股列表

错误做法：

```text
pkg_xxx 当前荐股列表
```

正确做法：

```text
program_id + list_version_id + binding_version_id
```

### 13.3 不要复用历史 selection run 当每日信号

每日复评必须针对目标交易日生成新的 selection run 或使用明确传入的回放 candidates。历史 selection run 只能作为审计或诊断输入。

### 13.4 不要修改 StrategyPackage manifest

策略包组合、权重、复评策略、回放配置都属于平台 binding/runtime，不进入 StrategyPackage frozen manifest。

### 13.5 不要让退出股票消失

退出股票必须有记录、有原因、有建议、有生效日期。UI 不能只展示当前 active pool 而隐藏退出决策。

## 14. 完成定义

该功能只有在以下条件全部满足时才可认为开发完整：

1. 荐股任务、策略绑定、复评执行、荐股列表版本、个股决策五层标识完整落库。
2. 初始列表和每日复评列表都可按日期回看。
3. 每日复评后每只股票都有明确操作建议。
4. 退出列表可见且可解释。
5. 回放支持单包和多包加权，且可通过草稿临时修改策略包配置。
6. 回放支持新旧配置对比。
7. 人工可应用新策略包配置，系统不做收益/换手硬门禁。
8. 应用新策略包后不清空当前荐股列表，下一交易日基于旧列表继续迭代。
9. UI 不要求用户手工填写 `selection_run_id`。
10. 所有新增 DDL 有 comment，所有关键路径有自动化测试。
11. 不修改生产 DB、不重启生产服务，除非用户在实施阶段明确授权。

