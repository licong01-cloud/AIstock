# 多 Alpha 组合回测 QE UI 功能对齐设计

- 文档类型：F1 功能设计增量
- 模块：QuantEvolver / Multi-Alpha combine-backtest
- 状态：已批准进入实现（用户于 2026-07-18 要求修复失败后直接补齐 UI）
- 基线设计：`docs/architecture/multi_alpha_combine_backtest_ui_reuse_design_20260626.md`
- 运行边界：仅 QE 组合研究；不得影响 Selection、Advisory、Paper、模拟盘、QMT 或其他非 QE 模块
- 数据库：零 DDL；复用现有 `strategy_pkg.multi_alpha_combine_backtest_*` 与 `qe_archive` 表

## 1. Background / 背景与问题

现有组合回测 UI 已具备任务列表、配置详情、scheme 切换、指标/LOO 诊断、JSON 导出和 StrategyPackage 导出，但与单腿 QE 实验 UI 相比仍缺少以下可操作能力：

1. running 任务仅能手动刷新，无法持续看到每个 `macb_` run 的阶段、数量进度和心跳时间。
2. failed / partial_failed 只显示聚合失败，单个已成功 scheme、不可计算 scheme 及其原因没有完整展开。
3. 没有运行日志入口；用户只能从后端或文件系统排查。
4. 没有“按原配置再跑 / 重试失败配置”。
5. 删除按钮为不可用占位；不能删除已终止的错误配置及其 workspace。
6. 没有组合 run 的 QE Archive 状态与手工入仓入口。
7. 原始 `run_id`、roster、seed、节点、窗口、topk、超时、数据覆盖阈值和重试血缘没有形成一屏可审计视图。

本设计对齐的是“用户完成同一研究操作所需的能力”，不是把单腿模型训练字段伪造成组合字段。

## 1.1 Scope / 范围

- combine-backtest 任务/配置状态、进度、失败原因、日志和资产查询。
- 完整请求快照、重试草稿、按原配置重跑与 lineage。
- 终态 run 删除、QE Archive 状态/预览/写入。
- 组合列表页和详情页的对应交互、自动刷新与错误展示。
- 仅修改 QE multi-alpha 服务、路由、页面及其定向测试/设计文档。

## 1.2 Non-Goals / 非目标

- 不把组合回测改造成模型训练或演进系统。
- 不伪造训练 IC、loss、feature importance、Agent 决策或拓扑血缘。
- 不实现没有持久跨节点契约的“假取消”；running run 保持可观察，终态后可重试或删除。
- 不修改 Selection、Advisory、Paper、模拟盘、QMT 和其他非 QE 模块。
- 不增加 GPU、显存或桌面资源监控。

## 2. 适用功能对齐矩阵

| 单腿 QE UI 能力 | 组合回测实现 | 语义 |
|---|---|---|
| 状态与自动刷新 | 列表/详情可开关自动刷新，running 默认 5 秒轮询 | 查询 DB 心跳，不做 GPU/显存监控 |
| 运行阶段与进度 | 显示 `phase`、`completed/total/pending`、最后心跳 | 来源为 combine service 持久化 heartbeat |
| 实时日志 | 运行事件流 + workspace 文本日志尾部 | 轮询传输；不伪造缺失历史日志 |
| 失败原因 | 显示 run reason、child task、scheme skipped_reason | failed/partial_failed 的成功结果仍可查看 |
| 重新运行 | 失败显示“重试”，成功显示“按原配置再跑” | 新 run，保留 `retry_of_run_id` 血缘，不覆盖原结果 |
| 编辑后重跑 | legacy run 配置缺失时展示重建草稿与假设 | 用户可见，不做静默默认替换 |
| 删除 | 仅删除终态 run；级联子表并清理该 run workspace | running 不伪装成已取消；取消属于独立调度能力 |
| 数仓状态/入仓 | 显示 archived/not_archived；支持预览和写入 | 复用 multi-alpha QE Archive handler |
| 资产信息 | 展示 prediction 持久化状态、workspace 文本文件清单 | 大文件不直接内嵌到 UI |
| JSON / StrategyPackage 导出 | 保留现有能力 | 不改变既有业务契约 |

不适用且不得伪造的单腿字段：训练 loss、模型 IC/RankIC、feature importance、Agent 决策、追加演进 Loop、修改因子/模型训练配置。组合页继续展示 roster 腿、权重、LOO、组合回测指标和运行证据。

## 3. 后端设计

### 3.1 完整请求快照与重试血缘

新提交的 combine run 在 `backtest_config_json._combine_request_v1` 中保存完整、可重放的请求快照：

- roster / seed run ids / metadata
- oos_start / oos_end
- weighting_schemes / normalize_method / walk_forward / rank_fusion
- baseline_leg_id / topk / min_date_coverage
- scheme_timeout_seconds / run_timeout_seconds
- 原始 backtest_config（快照字段自身除外）

重试创建新 `macb_` run，不改写原 run；新配置增加：

- `retry_of_run_id`
- `retry_requested_at`
- `retry_request_source=exact_snapshot|explicit_retry_payload`

历史 run 没有快照时，后端返回显式 `retry_draft`：所有可从 run/scheme/reason 恢复的值原样恢复，无法证明的值列入 `assumptions`。UI 必须展示这些假设后再提交，不允许静默当作“原配置”。

### 3.2 状态、scheme 与失败结果

- task 状态保留 `running / completed / partial_failed / failed`，不再把所有 partial_failed 抹平为 failed。
- 可用 scheme 取所有已持久化 scheme 的并集；每个 run 缺失的 scheme 以显式 `skipped=true` 和原因展示。
- 默认 scheme 优先选择存在可计算结果的 `ic_weighted`；否则选择第一个可计算 scheme并返回 warning。
- “最优配置”按当前选中 scheme 的可计算结果比较；partial_failed run 中成功的 scheme 可参与，不因同 run 的其他 scheme 失败而被忽略。
- 每个 loop 返回 raw status、phase、progress、reason、heartbeat、scheme rows、retry/delete/archive capability。

### 3.3 运行事件与日志

combine service 每次 heartbeat 和终态写入同时追加：

`rdagent_assets/multi_alpha_combine_backtests/<run_id>/run_events.jsonl`

日志 API 返回：

- 当前 DB 状态快照；
- 最近 N 条结构化运行事件；
- run workspace 下允许读取的文本日志文件名、大小、更新时间和尾部内容；
- `history_available=false` 用于说明历史 run 在本功能上线前没有事件文件。

路径必须解析并校验在该 run workspace 内；不得读取任意路径，不得返回 `.env`、凭据或二进制大文件。

### 3.4 写操作

- `GET /multi-alpha/combine-backtest/runs/{run_id}/retry-draft`
- `POST /multi-alpha/combine-backtest/runs/{run_id}/retry`
- `DELETE /multi-alpha/combine-backtest/runs/{run_id}?cleanup_workspace=true`
- `GET /multi-alpha/combine-backtest/runs/{run_id}/logs`
- `GET /multi-alpha/combine-backtest/runs/{run_id}/archive-status`
- `POST /multi-alpha/combine-backtest/runs/{run_id}/archive`

删除只允许终态 run。running run 没有持久、可验证的跨节点取消契约，故本功能不提供假取消；这不是研究方向门禁，也不影响创建新实验或查看结果。

## 4. 前端设计

### 4.1 列表页

- 保留状态筛选、分页、详情、导出。
- 增加自动刷新开关与 5/10/30 秒间隔。
- task 行显示 partial_failed、running 数、最新阶段、进度与最后心跳。
- 删除不再显示不可用占位；task 聚合行不直接批量删除，删除在 run 配置详情中执行，避免误删同 roster 的其他窗口。

### 4.2 详情页

- 顶部增加自动刷新、当前 scheme、数仓状态。
- 左侧配置卡显示原始 `macb_` run id、raw status、阶段/进度、心跳、失败摘要。
- 右侧增加“运行与日志”Tab：运行证据、事件、日志文件、scheme 状态表、重试血缘、资产状态。
- 终态配置提供：重试/按原配置再跑、删除、入仓预览/写入。
- legacy retry draft 以弹层展示恢复来源与 assumptions；用户提交后创建新 run。

### 4.3 自动刷新行为

- running task 默认自动刷新；用户可关闭。
- 页面不可见时不轮询；恢复可见后立即刷新。
- 每次只查询 combine API 和日志 API，不调用资源遥测接口，不运行 `nvidia-smi`/NVML。

## 5. Design Acceptance Index

| ID | 验收项 | 实现证据 | 状态 |
|---|---|---|---|
| F-101 | task/loop 保留 partial_failed 与成功 scheme | adapter 定向测试 | verified |
| F-102 | 每个 run 显示 phase/progress/reason/heartbeat | adapter 定向测试 + 浏览器实测 | verified |
| F-103 | running 自动刷新且页面隐藏时暂停 | TypeScript/build + 浏览器实测 | verified |
| F-104 | 结构化事件与安全日志尾部 API/UI | service 定向测试 + 浏览器实测 | verified |
| F-105 | 新 run 完整请求快照 | service 定向测试 | verified |
| F-106 | exact retry 与 lineage | service 定向测试 | verified |
| F-107 | legacy retry assumptions 显式展示 | service 定向测试 + 浏览器实测 | verified |
| F-108 | 终态 run 删除与 workspace 清理 | service 定向测试 | verified |
| F-109 | Archive 状态、预览、写入 | Archive handler 回归 + API/UI 实测 | verified |
| F-110 | scheme skipped_reason 与资产状态 | adapter 定向测试 + 浏览器实测 | verified |
| F-111 | QE-only 隔离；不改非 QE 模块行为 | changed-file scope + F1 validator | verified |
| F-112 | 既有 JSON/StrategyPackage/指标/LOO 功能零回归 | 83 项后端回归 + production build | verified |

## 6. Implementation Plan / 实施方案

1. 扩展 combine request 持久化、retry/log/delete/archive service 与 API。
2. 扩展 UI adapter 的状态、scheme、reason、archive 与 capability 契约。
3. 更新列表页自动刷新、partial_failed 与阶段进度展示。
4. 更新详情页运行日志、scheme 结果、retry/delete/archive 操作。
5. 补齐后端定向测试、前端类型/构建和浏览器验证。
6. 回填 Design Acceptance Matrix，并执行 F1 workflow validator。

## 7. Verification Plan / 验证方案

- 后端单元：request snapshot、retry draft、exact/legacy retry、status/scheme 映射、日志路径安全、终态删除、archive adapter。
- API：list/detail/log/retry/delete/archive 契约测试。
- 前端：TypeScript、定向组件测试或构建；浏览器验证列表、详情、日志、重试、删除、入仓状态。
- 业务 oracle：partial_failed run 的成功 scheme 指标必须可见；不可计算 scheme 显示原始原因；retry 生成新 run id 且原 run 不变。
- 隔离：不改 `qe_evolution_*` / `qe_experiments` 表；不启用任何 GPU/显存资源监控。
- 合入前执行：
  - `python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_combine_backtest_qe_ui_parity_design_20260718.md --tier F1`
  - changed-file lint/compile、定向测试、`git diff --check`、相关 Validation Center/CI。

## 8. Risks / 风险与失败模式

- legacy run 无完整请求快照：必须返回 assumptions，不宣称 exact retry。
- 日志文件可能只存在于远端：UI 明确显示本地历史不可用，不将空日志解释为成功。
- partial_failed 中不同 scheme 可用性不同：每个 scheme 独立显示 skipped/reason，指标空值保持 `-`。
- 删除跨 DB 与文件系统无法形成单事务：服务先验证终态和路径，再清理限定 workspace 并删除 DB；任一步失败均显式返回。
- Archive 写入失败不得影响源 combine 结果；错误在 UI 单独显示并可重试。

## 9. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | `backend/services/multi_alpha/combine_ui_adapter.py` | `backend/tests/test_multi_alpha_combine_ui_adapter.py`：partial_failed/sparse schemes/SOTA | verified | - |
| F-102 | `backend/services/multi_alpha/combine_ui_adapter.py`; detail page | `backend/tests/test_multi_alpha_combine_ui_adapter.py`：latest-running heartbeat；`artifact:tmp/qe_runs/ui_parity_runtime.png` | verified | - |
| F-103 | combine list/detail pages | `npm run build`；`artifact:tmp/qe_runs/ui_parity_runtime.png` | verified | - |
| F-104 | `backend/services/multi_alpha/combine_backtest.py`; logs UI | `backend/tests/test_multi_alpha_combine_backtest.py`：structured-event/safe-tail；`artifact:tmp/qe_runs/ui_parity_runtime.png` | verified | - |
| F-105 | `backend/services/multi_alpha/combine_backtest.py` | `backend/tests/test_multi_alpha_combine_backtest.py`：exact request snapshot | verified | - |
| F-106 | retry service/router/UI | `backend/tests/test_multi_alpha_combine_backtest.py`：retry lineage；`artifact:isolated-fastapi-route-smoke` | verified | - |
| F-107 | retry draft service/UI | `backend/tests/test_multi_alpha_combine_backtest.py`：legacy explicit payload；`artifact:tmp/qe_runs/ui_parity_runtime.png` | verified | - |
| F-108 | delete service/router/UI | `backend/tests/test_multi_alpha_combine_backtest.py`：terminal delete/quarantine restore/running rejection | verified | - |
| F-109 | archive adapter/router/UI | `backend/tests/qe_archive/test_multi_alpha_archive_handler.py`；`backend/tests/test_multi_alpha_combine_backtest.py`：running rejection | verified | - |
| F-110 | scheme/artifact adapter/UI | `backend/tests/test_multi_alpha_combine_ui_adapter.py`；`artifact:tmp/qe_runs/ui_parity_runtime.png` | verified | - |
| F-111 | QE-only changed-file scope | `validation-receipt:feature-workflow-f1`；changed-file scope review | verified | - |
| F-112 | existing combine UI/service regression | `backend/tests/test_multi_alpha_combine_backtest.py`；`backend/tests/test_multi_alpha_combine_ui_adapter.py`；`npm run build` | verified | - |

## 10. Production Gates / 生产门禁与运行时影响

- DDL：无。
- 依赖：无新增 Python/前端依赖。
- 后端重启：合入后需要用户安排；不得为验证中断当前 QE 任务。
- 已运行的旧 combine run：可读取；没有事件文件时明确显示历史日志不可用。新提交 run 才具备完整请求快照与完整事件历史。
