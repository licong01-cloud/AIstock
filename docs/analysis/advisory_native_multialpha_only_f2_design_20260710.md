# 荐股原生多 Alpha 父包兼容与手工多包退役 F2 设计

## 背景

荐股中心当前同时暴露单包和手工多包模式。单个 `multi_alpha` 父包已经可以沿 StrategyPackage 选股链生成组合分数，但荐股入口仍允许用户临时选择多个独立包并执行 `weighted_rank_fusion`、`union` 或 `intersection`。手工组合缺少 combine run、冻结权重、父包 manifest 和组合回测证据，不能继续作为原生多 Alpha 能力对外提供。

本功能将荐股运行契约收敛为一个原生 StrategyPackage：可以是 `single_alpha` 包，也可以是经过 combine backtest、冻结和资产完整性检查的 `multi_alpha` 父包。历史多包任务和列表保留可读，但不得再产生新的绑定、复评或回放状态。

## 范围

- 荐股创建、更新、绑定应用、启用、复评、回放统一要求 `package_mode=single_package` 且只有一个 `package_id`。
- 原生 `multi_alpha` 父包与 `single_alpha` 包使用相同页面和 API 操作。
- 复评运行时合并 active binding 的 `runtime_config_json` 与请求配置，请求配置显式覆盖 binding 配置。
- 退出观察候选深度必须覆盖 `rank_exit_threshold`，不足时 fail-closed。
- 页面移除策略模式、权重、添加策略包和移除策略包控件。
- 历史多包任务、binding、list version、review decision 保留查询和展示。
- 增加后端、前端和连续复评契约测试。

## 非目标

- 不删除历史荐股任务、binding 或列表版本。
- 不修改 StrategyPackage frozen manifest、alpha leg、combine 权重或回测结果。
- 不改变荐股的进入、退出、确认天数、止损止盈和每日替换预算算法。
- 不让荐股自动复刻模拟盘持仓、资金、订单或执行策略。
- 不执行生产 DML、DDL、服务重启或运行时激活。

## 架构

```text
荐股页面选择一个 StrategyPackage
  -> Advisory 原生包契约校验
     -> single_alpha: 现有单模型权威选股 artifact
     -> multi_alpha: 父包自有 leg 资产 + 冻结权重 -> 组合选股 artifact
  -> Selection Center SINGLE_PACKAGE run
  -> 退出观察深度校验
  -> Advisory review policy
  -> RecommendationListVersion
```

历史手工多包记录只走读取路径：

```text
legacy binding/list/review -> list/get/history UI
legacy create/update/enable/clone/run/replay/apply -> fail-closed reason_code
```

## API、DB、UI、MCP 契约

### 后端写入契约

- 合法：`package_mode=single_package` 且 `package_ids=[one_package_id]`。
- 非法：多于一个 `package_id`，或 mode 为 `fusion_pool`、`weighted_rank_fusion`、`union`、`intersection`、`sleeve_mode_future`。
- 统一错误上下文：`reason_code=ADVISORY_MANUAL_MULTI_PACKAGE_DEPRECATED`。
- 历史读取不因旧 mode 失败。
- active binding 为旧 mode 时，启用、正式复评、预览和回放均不得继续运行。

### 原生父包契约

- 前端和 Advisory 不接收 `component_package_ids`、leg 权重或子包选择。
- `StrategyPackageSelectionArtifactService` 根据 manifest `alpha_mode` 决定单 alpha 或多 alpha provider。
- 多 alpha provider 的资产或权重不完整时保留原始结构化 `reason_code`，不得改写为通用成功或空列表。
- Advisory 必须验证 selection run 的候选深度覆盖 `rank_exit_threshold`；有效候选不足时返回 `ADVISORY_EXIT_OBSERVATION_DEPTH_INSUFFICIENT`。

### Runtime 配置契约

- 基础配置来自 active binding `runtime_config_json`。
- 当前请求 `runtime_config` 深度合并并覆盖基础配置。
- 日期上下文和 PIT cutoff 最后写入，确保目标交易日与数据截止日不可被旧 binding 覆盖。
- review run 保存实际生效的合并配置。

### DB 契约

- 不新增或修改表结构。
- 旧 `package_mode` 值继续兼容读取。
- 不自动迁移或删除历史行。

### UI 契约

- 创建区只显示一个策略包选择器和目标数量。
- 策略替换草稿只显示一个策略包选择器、目标数量和应用原因。
- 包选项显示 `single_alpha` 或 `multi_alpha`，多 alpha 父包不展开 leg。
- 旧多包任务显示“历史手工多包，已退役”，禁用启用、克隆、复评、回放和应用入口；归档和历史查看保留。

### MCP 契约

- MCP 继续调用现有 Advisory API。
- 后端统一校验保证 MCP 无法绕过页面重新创建或运行手工多包。

## 设计验收索引

- F-001：所有新写入和运行入口只接受一个原生 StrategyPackage，旧手工多包 fail-closed。
- F-002：`multi_alpha` 父包可直接创建任务并完成权威选股、首次列表和连续复评。
- F-003：binding runtime 配置参与实际复评，PIT 日期上下文保持最终权威。
- F-004：退出观察候选深度不足时明确失败，不产生无法淘汰的活跃池。
- F-005：荐股页面移除手工多包、权重和多行编辑，单 alpha 与多 alpha 父包交互一致。
- F-006：历史手工多包任务和证据可读，但所有继续运行和复制入口均退役。
- F-007：自动化测试覆盖后端契约、多 alpha 父包连续复评、前端交互和错误原因。
- F-008：不修改生产数据和 runtime，完成设计校验、最小本地门禁并创建待审 PR。

## 实施方案

1. 在 Advisory 服务增加单原生包契约校验和统一 reason code。
2. 将校验接入 create、update、clone、enable、binding apply、review 和 replay。
3. 复评时深度合并 binding/request runtime 配置并记录实际配置。
4. Selection Run 返回后验证退出观察深度。
5. 前端将创建和绑定草稿收敛为单包选择，增加 legacy 状态禁用。
6. 更新 API、service、Playwright/组件契约测试和设计验收矩阵。

## 验证方案

- Advisory service/API：合法 single-alpha、合法 multi-alpha parent、所有旧 mode 拒绝、历史读取保留。
- Multi-alpha：父包作为 `single_package` 生成 selection run，首次进入后连续弱排名按既有规则淘汰补位。
- Runtime：binding 嵌套配置保留，请求覆盖，PIT 日期最终生效。
- 深度：候选数低于退出阈值时返回指定 reason code。
- Frontend：无策略模式、权重、添加/移除按钮；父包和单 alpha 共用一个 selector；legacy 操作禁用。
- 门禁：相关 pytest、前端 typecheck/定向测试、ruff、py_compile、`git diff --check`、feature workflow validate。

## 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/advisory_program.py::_require_native_package_config`；create/update/enable/clone/apply/review/replay 调用点 | `test_advisory_program_accepts_one_native_package_and_retires_manual_multi_package`；API 四种旧 mode 统一 422 reason code | verified | 无 |
| F-002 | `AdvisoryProgramService.run_review_from_selection`；`StrategyPackageSelectionArtifactService` 既有 multi-alpha provider | `test_native_multi_alpha_parent_uses_single_package_path_and_merges_binding_runtime_config`；`test_selection_center_api_treats_multi_alpha_parent_as_single_package`；`test_multi_alpha_live_selection.py` | verified | 无 |
| F-003 | `AdvisoryProgramService.run_review_from_selection`；`run_replay`；`_deep_merge_dicts`；`_with_advisory_date_context` | binding/request 嵌套覆盖、PIT cutoff 和父包 ID 单包路由测试通过 | verified | 无 |
| F-004 | `AdvisoryProgramService._review_runtime_config`；`_require_exit_observation_depth` | `test_review_requires_candidate_depth_before_treating_missing_holding_as_rank_drop` 覆盖浅候选拒绝和足深候选合成排名 | verified | 无 |
| F-005 | `frontend/src/app/paper-v2/advisory/page.tsx` 创建区与策略绑定管理器 | Playwright `Advisory native multi-alpha parent binding is scoped per active program`；TypeScript noEmit 通过 | verified | 无 |
| F-006 | `isLegacyManualMultiPackage`；服务层 continuation guards | 历史 program/binding 注入测试验证读取成功且 update/enable/clone/review/replay/apply 全部拒绝；Playwright legacy 行可见但退役 | verified | 无 |
| F-007 | `backend/tests/watchlist/test_advisory_program.py`；`test_advisory_api.py`；`paper-v2-advisory-ui.spec.ts` | Advisory 31 tests、multi-alpha/Selection Center 18 tests、Playwright 2 tests、Ruff 与 diff check | verified | 无 |
| F-008 | 本设计；生产门禁清单 | Feature workflow validate 与 PR 创建完成后回填；合入、根目录同步和生产运行时激活按流程等待用户确认 | ready | 无 |

## 发布与回滚

- 合入前只创建 PR，不触碰生产配置和服务。
- 合入后无需 DDL；前端发布后新建入口立即收敛为单包。
- 后端先于或同时于前端发布，避免旧页面绕过新契约。
- 回滚使用代码回滚；历史数据未迁移、未删除，不需要数据回滚。
- 旧多包任务保持归档和历史查询能力。

## 风险与失败模式

- 多 alpha 父包候选 artifact 深度不足：明确失败并给出实际数量和阈值。
- 旧 active 多包任务仍尝试运行：返回退役 reason code，不静默转换为父包。
- binding runtime 与请求配置覆盖次序错误：测试嵌套配置和 PIT 最终权威。
- UI 移除控件但 API 仍可绕过：service 层统一校验，不依赖前端。
- 把父包当作多个包展开：测试请求与 selection run 始终只有父包 ID。
- 多 alpha 失败后回落单 alpha：禁止 fallback，保留 provider reason code。

## 生产门禁

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_dml=noop`
- `production_runtime_activation=pending_user_merge_and_release_confirmation`
- 合入、根目录同步、生产运行时激活均需用户后续明确确认。
