# AIstock Validation Center 菜单路由覆盖视图详细设计方案

> 日期：2026-05-05
> 状态：detailed design draft v1.0
> 文档位置：`docs/architecture/aistock_validation_menu_route_coverage_design_20260505.md`
> 依赖设计：`docs/architecture/aistock_automated_testing_coverage_observability_design_20260504.md`、`docs/architecture/aistock_internal_validation_center_implementation_plan_20260504.md`、`docs/architecture/aistock_automation_test_coverage_gap_requirements_20260504.md`
> 适用范围：AIstock 内置 Validation Center 的“按正式菜单路由查看自动化测试覆盖、验证进度、覆盖率和证据”的 UI/API/数据契约设计。
> 明确边界：本方案只设计 Validation Center 页面内部的菜单路由覆盖视图；不替换、不覆盖、不隐藏 AIstock 全局左侧导航栏；不新建独立微服务；不重启生产 `8001`；不允许 UI 执行任意 shell；不把 mock UI 通过误报为真实业务通过。

## 1. 目标与结论

### 1.1 用户目标

在 Validation Center 页面中增加一个“页面路由覆盖视图”，右侧主内容区域内显示一棵与 AIstock 正式左侧菜单结构一致的菜单树。用户点击树中的某个页面路由后，右侧详情区显示该页面对应功能的：

- 自动化测试计划。
- 最近验证结果。
- L0-L5 验证进度。
- 代码覆盖率、diff coverage、功能覆盖率和业务证明强度。
- 关联 run、coverage snapshot、evidence manifest、Runner job。
- Bug、quality finding、缺失证据和未覆盖功能。
- 可执行的受控 Runner 计划。

### 1.2 必须满足的 UI 边界

- 页面路由菜单必须显示在 `/validation-center` 的右侧主内容区域中。
- 不得覆盖原有 AIstock 全局左侧导航栏。
- 不得把新菜单做成全局 fixed/absolute overlay。
- 不要求视觉风格与正式菜单完全一致，但菜单分组、顺序、路由、标签必须与正式菜单同源一致。
- 新菜单树用于“选择测试覆盖对象”，不是业务导航替代品；可提供“打开业务页面”按钮，但点击树节点默认不跳转业务页面。

### 1.3 总体结论

该功能在现有架构下可行，推荐按“菜单路由 -> 测试目标 registry -> 测试计划 -> 历史证据 -> 覆盖率/进度”的正式映射体系实现，而不是仅做字符串过滤。

第一阶段可以在现有 Validation Center 内完成，不需要新增数据库或独立服务：

```text
frontend/src/lib/navigation/nav-groups.ts
  -> Sidebar.tsx 读取同源正式菜单
  -> validation-center/page.tsx 在右侧主内容内读取同源菜单树

tests/aistock_validation/catalog/ui_targets.yaml
  -> 定义 route 与 module / plan / feature / source_paths 的映射

backend/services/validation/target_catalog.py
backend/services/validation/route_coverage.py
backend/routers/validation.py
  -> 聚合 runs / coverage / evidence / findings / bugs / executions
  -> 输出每个 route 的覆盖状态

frontend/src/app/validation-center/page.tsx
  -> 页面内菜单树 + route 详情面板
  -> 不影响全局 Sidebar
```

## 2. 当前基础与缺口

### 2.1 已有基础

| 能力 | 当前位置 | 是否可复用 |
|---|---|---|
| 正式全局菜单 | `frontend/src/app/Sidebar.tsx` 中的 `NAV_GROUPS` | 可复用，但需抽到共享模块 |
| Validation Center 页面 | `frontend/src/app/validation-center/page.tsx` | 可扩展 |
| Validation API client | `frontend/src/lib/validation/api.ts` | 可扩展 |
| Validation 后端路由 | `backend/routers/validation.py` | 可扩展 |
| run/coverage/evidence 读取 | `backend/services/validation/history_store.py` | 可复用并扩展 |
| 测试计划 allowlist | `tests/aistock_validation/catalog/test_plans.yaml` | 可复用并扩展 route 关联 |
| 受控 Runner | `backend/services/validation/execution_runner.py` | 可复用 |
| Bug/finding 本地索引 | `backend/services/validation/finding_store.py` | 可复用并扩展 route 关联 |

### 2.2 主要缺口

| 缺口 | 影响 | 设计处理 |
|---|---|---|
| 全局菜单只在 `Sidebar.tsx` 内定义 | Validation Center 如果手工复制菜单会漂移 | 抽出 `frontend/src/lib/navigation/nav-groups.ts`，Sidebar 和覆盖视图共用 |
| 历史 run 多数只记录 `module`，不记录 `route` | 无法准确按页面路由归因 | 新增 `ui_targets.yaml` 和后续 run metadata 的 `target_routes`；历史数据先按 plan/module 推断并标注来源 |
| 当前 coverage snapshot 多为 module 级 | 页面级覆盖率只能聚合/推断 | 输出 `attribution_source`，禁止把推断覆盖率宣称为精确覆盖 |
| 前端代码覆盖率尚未完整采集 | TSX 行覆盖无法准确显示 | 第一阶段显示 TypeScript/Playwright/功能路径覆盖；前端 line coverage 标记为 `not_collected` |
| 菜单项不等于功能点 | 页面通过不代表所有按钮通过 | 在 target registry 中维护 feature 级覆盖清单 |
| 很多页面尚未纳入流水线 | 初版会出现大量未覆盖 | 显示 `未登记` / `未验证`，作为治理基线 |

## 3. 设计原则

1. **菜单同源**：Validation Center 内部菜单树必须与正式 Sidebar 使用同一份 `NAV_GROUPS` 数据源。
2. **只在右侧页面显示**：菜单覆盖树是 `/validation-center` 页面内容的一部分，不改变全局布局。
3. **按 route 归因，按 module 聚合兜底**：新记录使用 route 精确归因；历史记录只能按 module/plan 推断，UI 必须标注推断来源。
4. **不误报通过**：mock UI、历史 run、fail-fast、API 200、L0/L1 通过都不能被展示为真实业务完成。
5. **可观测优先**：即使页面未登记，也要显示为 `未纳入流水线覆盖`，而不是隐藏。
6. **受控执行**：页面可触发的执行只能来自 `test_plans.yaml` 中 `runner_enabled=true` 的 allowlisted nox session。
7. **渐进增强而非简化**：第一阶段可不采集前端代码覆盖率，但 schema 必须预留；不得后续破坏字段兼容。
8. **生产隔离**：所有验证仍使用 `8011/8012`、`3011/3012`；不得默认触碰生产 `8001`。

## 4. 目标 UI 结构

### 4.1 页面布局

Validation Center 页面保持现有全局布局：

```text
+----------------------------------------------------------------------------------+
| AIstock 全局左侧 Sidebar | app-main: /validation-center 页面内容                  |
|                          |                                                       |
|                          | Hero / summary cards                                  |
|                          |                                                       |
|                          | [页面路由覆盖视图 SectionCard]                        |
|                          | +----------------------+----------------------------+ |
|                          | | 页面路由菜单树        | 选中页面覆盖详情             | |
|                          | | 与正式菜单同结构      | route/module/plans/runs     | |
|                          | | 但样式可简化          | coverage/features/evidence  | |
|                          | +----------------------+----------------------------+ |
|                          |                                                       |
|                          | 原有 plans / executions / runs / coverage / bugs ... |
+----------------------------------------------------------------------------------+
```

### 4.2 页面内菜单树行为

- 数据来自同源 `NAV_GROUPS`。
- 默认显示所有一级目录。
- 可按“全部 / 未登记 / 失败 / 过期 / 有缺失证据 / L3 已通过”过滤。
- 每个菜单项显示：
  - 页面标签。
  - route。
  - 覆盖状态 badge。
  - 最近验证时间。
  - 最高验证等级。
- 点击菜单项：
  - 不跳转业务页面。
  - 设置 `selectedRoute`。
  - 右侧详情区加载 route coverage summary。
- 提供单独按钮：
  - `打开业务页面`：跳转对应 route。
  - `查看最新 Run`：打开 Validation Center run detail。
  - `执行推荐计划`：仅对 runner-enabled plan 可见。

### 4.3 右侧详情内容

选中 route 后显示以下面板：

| 面板 | 内容 |
|---|---|
| 页面目标概览 | route、菜单分组、菜单标签、module、risk_level、登记状态 |
| 验证进度 | L0-L5 各等级状态、最近 run、是否 current commit、是否真实 UI/API/DB |
| 覆盖率 | 后端 line/branch、diff line、前端 coverage 状态、数据质量 coverage、失败 gates |
| 功能点覆盖 | 每个 feature 的 required/proof status/latest evidence |
| 测试计划 | plan_key、level、runner_enabled、端口、是否长耗时、是否写业务状态 |
| 历史证据 | latest run、coverage snapshot、evidence manifest、runner job、log/evidence URI |
| 风险与缺失 | metadata_missing、coverage_missing、evidence_missing、pass_scope_missing、unregistered route |
| 缺陷闭环 | 关联 Bug、quality finding、agent-context 入口 |

### 4.4 状态枚举

route 级状态建议使用以下枚举：

| 状态 | 含义 | 是否可称为业务可用 |
|---|---|---|
| `unregistered` | route 未在 `ui_targets.yaml` 登记 | 否 |
| `registered_no_plan` | 已登记但无测试计划 | 否 |
| `not_run` | 有计划但无历史 run | 否 |
| `failed` | 最近关键 run failed 或 coverage gate failed | 否 |
| `stale` | 最新通过 run 不是当前 commit | 否 |
| `mock_only` | 只有 mock UI 或 mocked API 证据 | 否 |
| `l2_passed` | 后端/API/DB 级通过 | 仅能证明 API/组件 |
| `l3_passed` | dev backend/frontend + UI 点击通过 | 可证明该 UI 路径 |
| `business_passed` | `positive_business_success=true` 且 evidence 完整 | 可证明指定业务操作 |
| `partial` | 部分 feature 通过但存在未覆盖/缺失证据 | 否，需列出缺口 |

状态计算必须保留 `reason_codes`，例如：

```json
{
  "status": "mock_only",
  "reason_codes": [
    "latest_run_mock_api_used",
    "positive_business_success_false",
    "no_real_database_evidence"
  ]
}
```

## 5. 前端导航数据同源设计

### 5.1 新增共享导航模块

建议新增：

```text
frontend/src/lib/navigation/nav-groups.ts
```

类型：

```ts
export type NavItem = {
  href: string;
  label: string;
};

export type NavGroup = {
  title: string;
  items: NavItem[];
};

export const NAV_GROUPS: NavGroup[] = [...];
```

修改：

```text
frontend/src/app/Sidebar.tsx
  -> import { NAV_GROUPS } from "@/lib/navigation/nav-groups";

frontend/src/app/validation-center/page.tsx
  -> import { NAV_GROUPS } from "@/lib/navigation/nav-groups";
```

### 5.2 菜单一致性测试

需要新增前端测试断言：

- Sidebar 渲染的一级目录数量等于 `NAV_GROUPS.length`。
- Validation Center 页面路由覆盖树渲染的一级目录数量等于 `NAV_GROUPS.length`。
- `/validation-center` 在正式菜单和覆盖树中都存在。
- 任一 route 在覆盖树中的 label 与 `NAV_GROUPS` 一致。
- 覆盖树点击不触发业务跳转，只更新详情区。

### 5.3 样式约束

覆盖树建议使用独立 class：

```css
.validation-route-coverage-grid { ... }
.validation-route-tree { ... }
.validation-route-tree-item { ... }
```

禁止：

- `position: fixed`。
- 高 z-index 覆盖全局 Sidebar。
- 直接复用 `.sidebar` 作为页面内部容器。
- 改动 `RootLayout` 或全局 Sidebar 行为。

可允许：

- 在右侧主内容内部使用 `position: sticky`，但 sticky 边界必须限制在页面内容容器内。
- 移动端改为上下布局，菜单树在详情上方。

## 6. 测试目标 Registry 设计

### 6.1 新增文件

建议新增：

```text
tests/aistock_validation/catalog/ui_targets.yaml
```

用途：

- 将正式菜单 route 绑定到验证 module。
- 将 route 绑定到测试计划。
- 定义该页面的关键功能点。
- 定义 required level 和 coverage policy。
- 支持历史 run 归因和未来 current-commit gate。

### 6.2 YAML Schema

```yaml
schema_version: aistock_validation_ui_targets_v1

targets:
  - route: /validation-center
    module: validation_center
    owner: validation
    risk_level: P1
    route_aliases: []
    source_paths:
      - frontend/src/app/validation-center/page.tsx
      - frontend/src/lib/validation/api.ts
      - backend/routers/validation.py
      - backend/services/validation/**
    api_paths:
      - /api/v1/validation/health
      - /api/v1/validation/plans
      - /api/v1/validation/runs
      - /api/v1/validation/coverage
      - /api/v1/validation/evidence
      - /api/v1/validation/executions
    plan_keys:
      - validation_center_backend
      - validation_center_ui
      - validation_center_live_readonly
      - validation_center_runner_smoke
    required_levels:
      - L2
      - L3
    coverage_policy:
      backend_line_min: 75
      backend_branch_min: 55
      diff_line_min: 80
      frontend_line_min: null
      frontend_status: not_collected
    features:
      - feature_key: read_validation_history
        label: 查看测试历史
        required: true
        required_level: L3
        plan_keys: [validation_center_ui]
      - feature_key: start_controlled_runner
        label: 启动受控 Runner
        required: true
        required_level: L3
        plan_keys: [validation_center_runner_smoke, validation_center_ui]
```

### 6.3 首批建议登记目标

第一阶段不需要一次登记所有页面，但覆盖树必须显示所有正式菜单项。建议首批登记已经有测试基础的页面：

| route | module | 原因 |
|---|---|---|
| `/validation-center` | `validation_center` | 当前目标页面，测试基础最完整 |
| `/qe-archive` | `qe_archive` | 已有数据质量、后端、UI、L3 计划 |
| `/quantevolver` | `qe` | QE read-only 相关路径 |
| `/quantevolver/experiments` | `qe` | QE 实验历史是归档/数据完整性的核心入口 |
| `/paper-v2` | `paper_v2_selection_center` | Paper v2 总览 |
| `/paper-v2/packages` | `paper_v2_selection_center` | Strategy Package 关键路径 |
| `/paper-v2/selection` | `paper_v2_selection_center` | Selection Center 关键路径 |
| `/paper-v2/portfolios` | `paper_v2_selection_center` | Paper v2 组合列表和 running-summary |
| `/paper-v2/model-hmm` | `paper_v2_selection_center` 或 `hmm` | HMM 与 Paper v2 关键路径 |
| `/local-data` | `data_ingestion` | 本地数据刷新、告警和数据质量入口 |
| `/qlib` | `qlib_data` | Qlib snapshot/bin 数据链路入口 |

未登记的正式菜单项应显示 `未登记`，不隐藏。

## 7. 后端 API 设计

### 7.1 新增服务

建议新增：

```text
backend/services/validation/target_catalog.py
backend/services/validation/route_coverage.py
```

职责：

- `target_catalog.py`
  - 读取 `ui_targets.yaml`。
  - 校验 schema、route 唯一性、plan_key 是否存在、required_levels 是否合法。
  - 输出 target map。
- `route_coverage.py`
  - 读取 target。
  - 聚合 `ValidationHistoryStore`、`ValidationPlanCatalog`、`ValidationFindingStore`、`ValidationExecutionRunner`。
  - 输出 route coverage summary。

### 7.2 新增 API

建议新增：

```text
GET /api/v1/validation/ui-targets
GET /api/v1/validation/ui-targets/{encoded_route}
GET /api/v1/validation/page-coverage
```

其中：

- `/ui-targets` 返回所有登记 target 的配置和聚合状态。
- `/ui-targets/{encoded_route}` 返回单个 route 的详情。
- `/page-coverage` 支持 query：
  - `route`
  - `module`
  - `status`
  - `include_unregistered`
  - `page`
  - `page_size`

由于后端不应解析前端 TS 菜单，所有“正式菜单完整树”由前端同源 `NAV_GROUPS` 提供；后端只返回已登记 target 的覆盖数据。前端将 `NAV_GROUPS` 与 API 返回 target map 合并，从而展示“所有菜单项 + 已登记覆盖状态”。

### 7.3 API 返回模型

```json
{
  "route": "/paper-v2/selection",
  "module": "paper_v2_selection_center",
  "registered": true,
  "status": "l3_passed",
  "reason_codes": [],
  "risk_level": "P1",
  "attribution_source": "target_catalog",
  "latest_run": {
    "run_id": "paper_v2_selection_center_...",
    "status": "passed",
    "level": "L3",
    "git_commit": "abc1234",
    "started_at": "2026-05-05T10:00:00+08:00"
  },
  "latest_passed_run": {},
  "coverage": {
    "status": "passed",
    "backend_line_percent": 82.1,
    "backend_branch_percent": 64.2,
    "diff_line_percent": null,
    "frontend_status": "not_collected",
    "coverage_snapshot_id": "..."
  },
  "level_progress": {
    "L0": {"status": "passed", "run_id": "..."},
    "L1": {"status": "missing"},
    "L2": {"status": "passed", "run_id": "..."},
    "L3": {"status": "passed", "run_id": "..."},
    "L4": {"status": "not_required"},
    "L5": {"status": "not_required"}
  },
  "pass_scope": {
    "real_backend": true,
    "real_database": true,
    "real_frontend_click": true,
    "mock_api_used": false,
    "positive_business_success": true,
    "production_8001_touched": false
  },
  "plans": [
    {
      "plan_key": "paper_v2_l3",
      "level": "L3",
      "runner_enabled": false,
      "requires_backend": true,
      "requires_frontend": true
    }
  ],
  "features": [
    {
      "feature_key": "single_package_selection",
      "label": "单策略包选股",
      "required": true,
      "status": "passed",
      "latest_run_id": "..."
    }
  ],
  "missing_evidence": [],
  "bugs": [],
  "findings": []
}
```

### 7.4 归因来源字段

每个 route 覆盖结论必须给出 `attribution_source`：

| 来源 | 含义 | 准确性 |
|---|---|---|
| `run_metadata_target_routes` | run metadata 明确写入 target route | 高 |
| `plan_catalog_target_routes` | plan/target registry 映射到 route | 中高 |
| `target_module_inference` | 根据 module 关联历史 run | 中 |
| `unregistered` | 未登记 | 无 |
| `none` | 无可用证据 | 无 |

UI 中必须显示该来源，防止历史推断被误解为精确 route 级证明。

## 8. Run Metadata 与 Evidence 扩展

### 8.1 新增 run metadata 字段

后续 `scripts/aistock_validate.py record` 和 runner archive 建议写入：

```json
{
  "target_routes": ["/validation-center"],
  "target_features": ["read_validation_history", "start_controlled_runner"],
  "plan_key": "validation_center_ui",
  "proof_kind": "mock_ui_e2e",
  "current_commit_evidence": true
}
```

### 8.2 兼容历史记录

历史 run 没有 `target_routes` 时：

1. 若 run metadata 有 `plan_key`，通过 `ui_targets.yaml.plan_keys` 反查 route。
2. 若没有 `plan_key`，通过 `module` 反查 route。
3. 如果一个 module 对应多个 route，标记为 `target_module_inference`，不能作为精确 route 证明。

### 8.3 Evidence manifest 扩展

建议后续 evidence manifest 增加：

```json
{
  "target_routes": ["/qe-archive"],
  "target_features": ["candidate_list", "dry_run", "quality_check"],
  "business_assertion_refs": ["qe_archive_backfill_operation"]
}
```

## 9. 覆盖率与进度计算规则

### 9.1 验证等级进度

每个 route 的 `level_progress`：

- 从该 route 关联的 runs 中按 level 聚合。
- 每个 level 取最新 run 和最新 passed run。
- 若 required level 没有 passed run，标记 `missing`。
- 若最新 run failed，即使历史 passed，也显示 failed + stale/history passed。

### 9.2 current commit 判断

建议后端使用固定、无用户输入、`shell=False` 的 git helper 获取当前 commit：

```text
git rev-parse HEAD
```

若环境不允许执行 git：

- 返回 `current_git_commit=null`。
- UI 显示 `当前 commit 未知`。
- 不得默认认为历史 run 是当前 commit。

### 9.3 覆盖率选择规则

优先级：

1. 与 latest passed route run 直接关联的 coverage snapshot。
2. 与同 plan_key 关联的 coverage snapshot。
3. 与同 module 最新 passed run 关联的 coverage snapshot。
4. 无 coverage，显示 `not_collected` 或 `missing`。

页面级 coverage 字段必须区分：

- `backend_line_percent`
- `backend_branch_percent`
- `diff_line_percent`
- `frontend_line_percent`
- `frontend_branch_percent`
- `frontend_status`
- `data_quality_status`

第一阶段允许：

```json
{
  "frontend_status": "not_collected",
  "frontend_line_percent": null,
  "frontend_branch_percent": null
}
```

但 UI 必须明确显示“前端代码覆盖率尚未采集”，不能显示为 0% 或通过。

### 9.4 业务可用判断

只有满足以下条件，route 才可显示 `business_passed`：

- latest passed run 当前 commit 或用户明确接受历史证据。
- `pass_scope.real_backend=true`。
- `pass_scope.real_frontend_click=true`。
- 需要 DB 的页面必须 `pass_scope.real_database=true`。
- `pass_scope.mock_api_used=false`。
- `pass_scope.positive_business_success=true`。
- `business_assertion.can_user_complete_operation=true`。
- 没有 required feature 的缺失证据。

否则只能显示更低等级状态。

## 10. 前端实现设计

### 10.1 新增类型

在 `frontend/src/lib/validation/api.ts` 增加：

```ts
export type ValidationUiTarget = JsonObject & {
  route: string;
  module?: string;
  registered?: boolean;
  status?: string;
  reason_codes?: string[];
  risk_level?: string;
  attribution_source?: string;
  latest_run?: ValidationRunSummary | null;
  latest_passed_run?: ValidationRunSummary | null;
  coverage?: JsonObject;
  level_progress?: Record<string, JsonObject>;
  pass_scope?: ValidationPassScope | null;
  business_assertion?: ValidationBusinessAssertion | null;
  plans?: ValidationPlan[];
  features?: JsonObject[];
  missing_evidence?: JsonObject[];
  bugs?: ValidationBug[];
  findings?: ValidationQualityFinding[];
};
```

新增 client 方法：

```ts
uiTargets(): Promise<ValidationUiTargetCatalog>
uiTarget(route: string): Promise<ValidationUiTarget>
pageCoverage(query): Promise<ValidationPage<ValidationUiTarget>>
```

### 10.2 页面组件拆分

建议从 `page.tsx` 中拆分：

```text
frontend/src/app/validation-center/components/RouteCoverageExplorer.tsx
frontend/src/app/validation-center/components/RouteCoverageTree.tsx
frontend/src/app/validation-center/components/RouteCoverageDetail.tsx
```

若第一阶段为降低改动，也可先在 `page.tsx` 内实现，但应预留拆分。

### 10.3 状态展示

route tree badge：

| status | badge 文案 |
|---|---|
| `unregistered` | 未登记 |
| `registered_no_plan` | 无计划 |
| `not_run` | 未验证 |
| `failed` | 失败 |
| `stale` | 过期 |
| `mock_only` | Mock |
| `l2_passed` | L2 |
| `l3_passed` | L3 |
| `business_passed` | 业务通过 |
| `partial` | 部分 |

详情区必须显示 `reason_codes`，避免只显示一个状态。

### 10.4 与现有 Validation Center 的关系

页面路由覆盖视图放在现有 summary cards 后、plans table 前，或者作为一个 tab/section：

```text
Hero
Summary cards
页面路由覆盖视图
测试计划目录
Runner 队列
Run 历史
质量发现 / Bug
Run/Coverage/Evidence 详情
```

不删除现有 plans/runs/coverage/evidence 功能。

## 11. 后端实现设计

### 11.1 `ValidationTargetCatalog`

职责：

- 读取 `tests/aistock_validation/catalog/ui_targets.yaml`。
- 返回 target list/map。
- 校验：
  - route 必须以 `/` 开头。
  - route 不允许重复。
  - module 非空。
  - plan_key 必须存在于 `test_plans.yaml`，否则 catalog health 返回 parse/validation error。
  - required_levels 必须属于 L0-L5。
  - feature_key 在同 route 内唯一。

### 11.2 `ValidationRouteCoverageService`

职责：

- 根据 target route 查询相关 plans。
- 根据 target 的 plan_keys/module/target_routes 聚合 runs。
- 获取 latest run、latest passed run、level progress。
- 聚合 coverage/evidence/finding/bug/execution。
- 计算 route status 和 reason codes。

### 11.3 Error 与缺失处理

- `ui_targets.yaml` 缺失：API 返回空 target list 和 `missing=true`，不 500。
- YAML 语法错误：API 500，Validation Center UI 顶部显示错误。
- 某 route 未登记：前端本地合并时显示 `unregistered`，不请求后端 detail。
- 某 plan_key 不存在：catalog validation error，必须显式暴露。
- coverage 缺失：状态不能伪造为 passed。

## 12. 测试设计

### 12.1 后端测试

新增：

```text
backend/tests/test_validation_ui_targets.py
backend/tests/test_validation_route_coverage.py
```

覆盖：

| 测试 | 断言 |
|---|---|
| target catalog 正常读取 | route、module、plan_keys、features 正确 |
| route 重复 fail-fast | 抛出明确 catalog error |
| unknown plan_key fail-fast | 抛出明确 catalog error |
| `/ui-targets` 返回聚合状态 | 包含 registered route、plans、latest run、coverage |
| 历史 module 推断 | attribution_source=`target_module_inference` |
| mock-only 不等于业务通过 | status=`mock_only`，reason_codes 包含 `mock_api_used` |
| coverage missing 不通过 | status 不得为 `business_passed` |
| current commit 不匹配 | status=`stale` 或 reason_codes 包含 stale |
| unregistered 不由后端伪造 | 后端只返回 registered target |

### 12.2 前端 Playwright 测试

扩展：

```text
frontend/tests/validation-center/validation-center.spec.ts
```

覆盖：

- 页面内显示“页面路由覆盖视图”。
- 页面内菜单树包含与 `NAV_GROUPS` 一致的一级目录。
- 点击页面内 `/validation-center` 菜单项不影响全局 Sidebar，不跳转业务 route。
- 详情区显示 route、module、status、plans、coverage、features、missing evidence。
- 未登记菜单项显示“未登记”，且不伪造成通过。
- Mock-only target 显示“Mock”，并提示不能证明真实业务成功。
- API 只允许 existing controlled Runner `POST /validation/executions`，不新增任意写操作。

### 12.3 L0/guardrail

必须运行：

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0 -- `
  backend/services/validation/target_catalog.py `
  backend/services/validation/route_coverage.py `
  backend/routers/validation.py `
  backend/tests/test_validation_ui_targets.py `
  backend/tests/test_validation_route_coverage.py `
  frontend/src/lib/navigation/nav-groups.ts `
  frontend/src/app/Sidebar.tsx `
  frontend/src/lib/validation/api.ts `
  frontend/src/app/validation-center/page.tsx `
  frontend/tests/validation-center/validation-center.spec.ts `
  tests/aistock_validation/catalog/ui_targets.yaml
```

### 12.4 推荐验证命令

实现完成后至少运行：

```powershell
npm exec tsc -- --noEmit --incremental false
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_validation_ui_targets.py backend/tests/test_validation_route_coverage.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
$env:BACKEND_PORT='8011'
$env:FRONTEND_PORT='3011'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_ui
```

若新增 live read-only API：

```powershell
$env:BACKEND_PORT='8011'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_live_readonly
```

不得重启生产 `8001`。

## 13. 分阶段实施计划

### Phase 1：菜单同源与页面内覆盖视图骨架

目标：

- 抽出 `NAV_GROUPS`。
- Sidebar 与 Validation Center route tree 共用同一份菜单结构。
- 页面内 route tree 不覆盖全局 Sidebar。
- 能展示所有菜单项和未登记状态。

主要文件：

```text
frontend/src/lib/navigation/nav-groups.ts
frontend/src/app/Sidebar.tsx
frontend/src/app/validation-center/page.tsx
frontend/tests/validation-center/validation-center.spec.ts
```

验收：

- `/validation-center` 正常显示全局 Sidebar。
- 页面内容中出现 route coverage tree。
- tree 结构与 `NAV_GROUPS` 一致。
- 点击 tree 节点不跳转业务页面。

### Phase 2：Target Registry 与后端聚合 API

目标：

- 新增 `ui_targets.yaml`。
- 后端读取 target registry。
- 新增 `/validation/ui-targets` 和 route detail API。
- 支持按 plan/module 聚合历史 run/coverage/evidence。

主要文件：

```text
tests/aistock_validation/catalog/ui_targets.yaml
backend/services/validation/target_catalog.py
backend/services/validation/route_coverage.py
backend/routers/validation.py
backend/tests/test_validation_ui_targets.py
backend/tests/test_validation_route_coverage.py
frontend/src/lib/validation/api.ts
```

验收：

- registered route 返回 target detail。
- unknown/未登记 route 不伪造通过。
- mock-only、coverage-missing、stale 均有明确 reason codes。

### Phase 3：Route Detail 完整 UI

目标：

- 在 Validation Center 中显示 route 级详情。
- 显示 level progress、coverage、plans、features、evidence、bugs/findings。
- 支持过滤未覆盖/失败/过期页面。

主要文件：

```text
frontend/src/app/validation-center/page.tsx
frontend/src/app/validation-center/components/*
frontend/tests/validation-center/validation-center.spec.ts
```

验收：

- 点击 `/qe-archive` 可看到 qe_archive 相关计划与历史状态。
- 点击未登记页面显示“未纳入流水线覆盖”。
- 详情区明确显示 attribution_source。

### Phase 4：Run Metadata 精确 route 归因

目标：

- `aistock_validate.py record` 支持 `--target-route` / `--target-feature`。
- runner archive 自动写入 `target_routes`。
- coverage/evidence manifest 记录 target route。

主要文件：

```text
scripts/aistock_validate.py
backend/services/validation/execution_runner.py
backend/tests/test_aistock_validate_metadata.py
backend/tests/test_validation_execution_runner.py
tests/aistock_validation/modules/validation_center.md
```

验收：

- 新 run metadata 包含 `target_routes`。
- route 归因优先使用 `run_metadata_target_routes`。
- 历史推断仍可用，但 UI 显示不同来源。

### Phase 5：功能点级覆盖与前端 coverage

目标：

- 每个 target 下 features 有独立状态。
- 前端 Vitest/Istanbul 或 Playwright coverage 方案接入。
- 页面级 coverage 能区分后端代码覆盖、前端代码覆盖、用户路径覆盖、数据质量覆盖。

验收：

- 关键业务页面不再只显示页面级通过，而是能看到每个按钮/功能点状态。
- 前端 coverage 不再为 `not_collected`。

## 14. 文件变更清单建议

第一批开发建议改动：

| 文件 | 操作 |
|---|---|
| `frontend/src/lib/navigation/nav-groups.ts` | 新增，同源正式菜单定义 |
| `frontend/src/app/Sidebar.tsx` | 改为导入共享 `NAV_GROUPS` |
| `tests/aistock_validation/catalog/ui_targets.yaml` | 新增首批 route target registry |
| `backend/services/validation/target_catalog.py` | 新增 target catalog reader |
| `backend/services/validation/route_coverage.py` | 新增 route coverage aggregator |
| `backend/routers/validation.py` | 新增 target/coverage endpoints |
| `frontend/src/lib/validation/api.ts` | 新增 target API 类型与 client |
| `frontend/src/app/validation-center/page.tsx` | 新增页面路由覆盖视图 |
| `backend/tests/test_validation_ui_targets.py` | 新增后端 target 测试 |
| `frontend/tests/validation-center/validation-center.spec.ts` | 扩展 UI E2E |
| `tests/aistock_validation/modules/validation_center.md` | 更新合同矩阵 |

可在第二批再做：

| 文件 | 操作 |
|---|---|
| `scripts/aistock_validate.py` | 增加 target_routes 写入 |
| `backend/services/validation/execution_runner.py` | runner archive 增加 route attribution |
| `noxfile.py` | 增加 route coverage 相关 session 或纳入 existing validation_center_backend |

## 15. 风险与控制

| 风险 | 控制 |
|---|---|
| 菜单树与正式菜单漂移 | `NAV_GROUPS` 同源导入，禁止手工复制 |
| 历史 run route 归因不准 | 标记 `attribution_source`，UI 不做过度证明 |
| Mock UI 被误报为业务通过 | status/reason_codes/pass_scope 规则强制区分 |
| 页面树覆盖全局 Sidebar | 禁止 fixed/absolute overlay，所有树节点在 page content 内 |
| 首版大量未覆盖造成误解 | UI 文案说明这是覆盖基线，不是错误 |
| Runner 误执行长耗时/危险命令 | 继续使用 test_plans allowlist 与 runner_enabled gating |
| 生产服务受影响 | dev ports only；不得默认触碰 `8001` |
| 前端 coverage 未采集 | 明确显示 `not_collected`，不显示为通过 |

## 16. 验收标准

第一阶段完成后必须满足：

1. `/validation-center` 页面中有“页面路由覆盖视图”。
2. 页面内菜单树与正式 `NAV_GROUPS` 结构同源。
3. 全局 Sidebar 仍正常存在且不被覆盖。
4. 点击页面内菜单项只更新 coverage detail，不跳转业务页面。
5. 未登记 route 显示“未纳入流水线覆盖”。
6. 已登记 route 显示 module、plan、latest run、coverage、level progress、evidence 缺口。
7. Mock-only、stale、coverage missing、business assertion missing 均明确展示。
8. Playwright 测试覆盖页面内菜单树与 route detail。
9. 后端测试覆盖 target catalog、route aggregation 和 failure semantics。
10. 所有变更通过 `validation_center_backend`、`validation_center_ui`、L0 changed-files guardrail。

## 17. 后续讨论点

1. 首批登记 route 是否按本方案建议的 10-11 个高价值页面执行。
2. route tree 是否默认展开所有一级目录，还是只展开“自动化流水线 / QuantEvolver / Paper v2”。
3. route 状态是否需要在正式全局 Sidebar 中也显示 badge。本方案暂不建议，避免影响主导航稳定性。
4. 是否在第一阶段就增加 `aistock_validate.py --target-route`。推荐第二阶段或第四阶段做，避免一次改动过大。
5. 前端 coverage 使用 Vitest/Istanbul 还是 Playwright coverage。推荐后续单独设计，首阶段显示 `not_collected`。

## 18. 推荐下一步

推荐按以下顺序进入实施：

1. 实现 `NAV_GROUPS` 同源抽取和页面内 route tree 骨架。
2. 新增 `ui_targets.yaml`，先登记 `validation_center`、`qe_archive`、`qe`、`paper_v2_selection_center`、`local-data`、`qlib` 相关核心页面。
3. 实现后端 target catalog + route coverage API。
4. 完成 route detail UI。
5. 增加后端和 Playwright 测试。
6. 跑 `validation_center_backend`、`validation_center_ui`、L0。
7. 验证通过后只提交本功能相关文件。
