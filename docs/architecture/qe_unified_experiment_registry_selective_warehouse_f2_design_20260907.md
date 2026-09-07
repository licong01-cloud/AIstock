# QE 统一实验登记、跨节点进度与选择性数仓 F2 详细设计

> Feature ID：`qe_unified_experiment_registry_selective_warehouse_v1`
> Feature tier：F2
> 设计版本：v1.1（3 个交付批次）
> 日期：2026-09-07
> 状态：`DESIGN_READY_SOURCE_NOT_STARTED`
> 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.14
> 既有入仓设计：`docs/architecture/qe_archive_manual_ingestion_selection_design_20260519.md`

## 1. Background / 背景与问题

### 1.1 用户目标

从本功能生效开始，所有正式 QE 实验都必须在 AIstock UI 中可见进度、终态和历史详情；具备长期研究价值的实验由用户在 UI 选择，或由用户明确授权 Codex/Claude Code 后进入 QE Archive 数仓。该能力必须同时覆盖 WSL `wsl2-5080` 与远端 `rdagent-node1`，并保持策略演进为唯一研究 P0。

本设计是用户明确批准的必要基础架构例外，不受父蓝图原“禁止 UI/Archive 平台工作”的限制；该例外只覆盖新正式实验的前瞻登记、状态可见和选择性入仓，不恢复历史补账、全量归档或通用平台建设。

### 1.2 已确认的现状

当前仓库不是完全缺少实验控制面：

- `POST /api/v1/quantevolver/experiments/pending` 已能在执行前创建单实验记录；
- `qe_single_experiment_create_pending` 与 `qe_experiment_run_confirmed` 已为 MCP 提供登记后执行路径；
- `/api/v1/quantevolver/experiments` 与实验详情页从 `qe_experiments` 读取历史；
- `/experiments/{id}/run-status` 已按只读原则返回持久化状态，运行态核对由后台协调器负责；
- QE Archive 已有 source-status、preview、confirmed execute、experiment/task/loop 选择和幂等写仓能力。

真实缺口是入口不统一：部分 Codex/Claude 长任务直接在 WSL 或远端执行 qrun，任务 manifest 会明确记录 `backend_used=false`、`ui_used=false`、`scheduler_used=false`。这类任务虽然生成了 workspace、配置和回测结果，却没有 `qe_experiments`、task/loop 或 Archive source identity，UI 自然无法发现。部分 Multi-Alpha 路径也在装配后才持久化 parent，尚未形成“登记成功后才允许分发”的统一不变量。

QE Archive 的现行手动入仓原则是正确的：数仓不应自动保存每个大型 workspace。但“所有正式实验都有轻量运行历史”和“只有有价值实验完整入仓”是两个层次，不能再把后者的人工选择策略误用为前者缺失的理由。

### 1.3 设计结论

本功能不建立新调度平台，不建立第二套数仓，也不扫描历史 workspace。它只在现有 QE 创建、执行、状态、UI 和 Archive 能力之间建立一条强制的正式实验合同：

```text
人类可读实验请求
  -> 现有 QE 控制面预登记并生成 canonical identity
  -> 冻结数据 release / universe / model / factors / seed / TWAP 合同
  -> 现有 WSL 或 remote dispatcher
  -> 现有节点状态与 artifact receipt
  -> 按变化收敛到 qe_experiments / task / loop
  -> UI 全历史与详情
  -> 规则只推荐，用户 UI 操作或明确授权后写 QE Archive
```

## 2. Scope / 范围

### 2.1 Goals / 目标

1. 为所有正式 QE 单实验、自动演进、自定义演进、策略演进、多 Alpha parent/child、单腿刷新和正式组合回测提供同一登记优先合同。
2. UI、Backend/Scheduler、Codex MCP、Claude Code MCP 最终复用同一控制面身份和执行入口。
3. WSL 与远端节点在计算开始前取得已持久化的 experiment/task/loop identity，不允许先运行后补身份。
4. UI 能以日期、类型、状态、节点、模型、因子族、股票池、数据 release 和执行算法搜索全部正式实验，无需人工输入内部 ID 或 JSON。
5. completed、failed、cancelled、interrupted 和父任务 partial/failed 下的成功 child 均保留轻量历史，不以成功任务为唯一样本。
6. Backend 重启不终止已分发的 WSL/远端计算；恢复后通过既有协调器和节点回执幂等收敛，不重复提交。
7. 所有终态实验保留轻量研究事实；用户选择或明确授权的高价值实验才进入完整 QE Archive。
8. workspace 后续清理不级联删除正式实验历史；UI 明示 artifact 可用、已清理或缺失状态。
9. 正常未变化心跳不追加事件；UI 未打开不产生前端轮询；节点探测与状态核对不短于 60 秒。
10. 训练、预测、回测数据面继续只读版本化 Bin/H5/Parquet/sidecar，数据库只承担控制面和结果面。

### 2.2 正式实验定义

只要一次执行的结果会参与 Alpha、模型、因子、股票池、策略、成本、执行算法或策略包比较，就属于正式实验，必须登记。正式实验包括成功和失败 trial，不因结果弱或失败而退出历史。

单元测试、fixture、开发期极小 smoke 可以不进入研究历史，但必须显式标记 `purpose=validation`，不得被报告为正式实验结论。任何生成可用于研究比较的完整预测或组合指标的运行，不得借 `smoke` 名义绕过登记。

### 2.3 Non-Goals / 非目标与边界

- 不批量补录旧 task/loop，不扫描全部历史 workspace，不重建过去缺失的制品或指标。
- 不自动把全部实验、日志、模型权重和 workspace 复制进 QE Archive。
- 不建立新的通用任务平台、消息总线、事件仓库、心跳表、监控 daemon 或配置中心。
- 不改变模型、因子、label、seed、数据切分、Top-K、成本、分钟 TWAP、股票池或策略退出语义。
- 不修改、移动、覆盖或重新导出 QE 数据集 candidate。
- 不允许 QE 训练/预测/回测访问业务数据库表，也不允许缺文件时回落旧路径或日频执行。
- 不接入 Selection、Advisory、Paper、模拟盘、QMT、StrategyPackage 或其他非 QE 模块。
- 不要求用户输入 experiment ID、task ID、loop ID、snapshot ID、文件路径、hash 或 JSON。
- 不把代码合入、Backend 重启、Frontend 生效、MCP 客户端同步、数仓写入和实验启动合并为一个完成状态。
- 本设计 PR 不执行 DDL/DML、实验、Archive 写入、依赖安装、客户端安装或任何进程控制。

## 3. Architecture / 架构

### 3.1 四层职责

| 层 | 现有 authority | 本功能职责 | 禁止职责 |
|---|---|---|---|
| 控制面登记 | `qe_experiments`、`qe_evolution_tasks`、`qe_evolution_loops`、Multi-Alpha durable tables | 全部正式实验身份、配置摘要、节点、生命周期和终态 | 保存训练/回测输入数据 |
| 节点执行 | `QEWorkspaceClient`、现有 WSL/remote dispatcher、RD-Agent loop | 使用已登记 identity 执行并返回阶段/终态/artifact receipt | 自建孤儿 experiment identity、写业务表 |
| UI 历史 | `/quantevolver/experiments`、evolution/task/loop 详情与结果接口 | 搜索、分组、进度、失败、核心指标、入仓状态 | 扫描节点目录发现实验、写状态 |
| 长期数仓 | `qe_archive` schema、backfill preview/execute、source-status | 只保存用户选择或明确授权的完整研究样本 | 自动全量入仓、替代运行控制面 |

### 3.2 统一登记适配器

实现期新增一个 QE-scoped service，例如 `backend/services/quantevolver/qe_run_registry.py`，但不新增另一套数据库模型。它只封装既有表和事务，提供以下内部动作：

- `reserve_single(...)`：复用现有 pending experiment 创建语义；
- `reserve_task(...)`：在 custom/strategy/auto evolution 分发前持久化 task 与 loop identity；
- `reserve_multi_alpha(...)`：在任何 child/group 分发前持久化 parent、child 计划和节点计划；
- `mark_dispatched(...)`：只有 reservation readback 成功后才能提交节点；
- `apply_transition(...)`：按 compare-and-set 应用状态变化和终态摘要；
- `project_history(...)`：提供统一、轻量、服务端分页的历史投影。

它不是新的业务层级。现有 router、evolution service 和 durable orchestrator 调用该适配器，避免继续各自拼 INSERT/UPDATE。实现不得一次性重写成熟执行器；先收口入口和状态写入边界。

### 3.3 入口收敛

| 入口 | 登记动作 | 执行动作 |
|---|---|---|
| UI 单实验 | create pending | 用户点击运行后调用现有 run endpoint |
| MCP 单实验 | `qe_single_experiment_create_pending` | `qe_experiment_run_confirmed` |
| custom/strategy/auto evolution | create task + 全部 planned loop identity | 现有 start/submit loop 路径 |
| Multi-Alpha | reserve parent + planned children | durable orchestrator 分发 |
| Codex/Claude 长任务 | 必须调用 QE MCP 创建与运行工具 | 不再裸跑未登记 qrun |
| WSL/remote | 只接受控制面下发的 registered identity | 执行既有 qrun/worker command |

低层 qrun 仍是节点执行器，不是公开正式实验入口。实现后，仓库内正式实验 runbook、MCP 和自动化脚本不得直接创建无登记 workspace。若 Backend 在提交前不可用，正式提交以稳定 reason code 失败；不得先启动计算再等待补录。

### 3.4 双节点与重启恢复

- WSL 与 `rdagent-node1` 使用相同的 logical identity、release id、profile generation、universe selection 和配置 digest；只允许 provider URI 与 workspace root 按 node binding 解析。
- 分发请求必须携带已登记 experiment/task/loop identity。节点拒绝空 identity、不同 release 或 server-owned binding 被客户端覆盖。
- 运行阶段由现有节点状态、child counts、workspace receipt 和 durable attempt 形成；不新增高频心跳事件流。
- Backend 重启期间节点计算继续。节点保留终态和 artifact receipt；Backend 恢复后现有 reconciliation coordinator 按 remote identity 回读并幂等更新。
- 同一 identity 的 retry/resume 不得重复启动已 running/succeeded 的 remote attempt；未知远端状态保持 unknown/reconciling，不伪造 failed 或 completed。
- WSL 全局并行度不超过 2、图模型单槽串行的现有资源合同不变；本功能不提高并发。

### 3.5 两级持久化

#### Level 0：所有正式实验的控制面历史

复用既有运行态表，至少保留：

- experiment/task/loop identity 与 parent-child 关系；
- 来源类型：UI、MCP、scheduler，以及 single/custom/strategy/auto/multi-alpha；
- purpose：research 或 validation；
- model、factor set、seed、label、data split、strategy 和 minute execution 摘要；
- active dataset release/profile generation、universe、benchmark 与节点；
- created/started/completed 时间和 canonical 状态；
- 终态核心指标摘要或结构化失败 reason code；
- workspace 与 artifact manifest 的受控引用及 retention 状态；
- Archive 状态投影。

Level 0 不复制大文件。正式记录不得因 workspace 清理而删除，也不得用数据库 DELETE 作为普通 UI 清理动作。需要排除测试噪声时，使用 purpose/visibility 标记和 UI 筛选，不销毁历史事实。

#### Level 1：选择性 QE Archive

继续以现有 `qe_archive` 为唯一长期分析数仓。只有以下动作能够触发完整写仓：

1. 用户在 UI 勾选 experiment/task/loop，查看 preview 后确认；
2. 用户明确授权 Codex/Claude Code，客户端调用 existing confirmed execute MCP。

系统规则只生成推荐和原因，不自动执行。写仓按 loop/experiment 粒度幂等；task 只作为批量选择入口，不把多个 loop 合并为一条 run。

### 3.6 不进行历史补账

本功能验收只覆盖生效后的正式实验。已存在但未登记的直接 WSL/remote 目录不自动扫描、不自动创建 DB 记录。若未来需要补录，只能针对用户点名、配置与结果完整、路径和 digest 可回读的精确 run 独立 preview 和授权；它不属于本 Feature 完成条件，也不得阻断新 Alpha 实验。

## 4. Contracts / API、DB、UI 与 MCP 契约

### 4.1 Canonical identity 与配置摘要

控制面在计算开始前生成 identity；节点不得自造第二 identity。公开 UI/MCP 接受模型、因子、日期、股票池、节点等语义字段，server 负责生成内部 identity 与 run-scoped binding。

每个正式 run 必须固化以下摘要：

| 类别 | 必须字段 |
|---|---|
| 来源 | source type、created-by type/name、purpose、parent/task/loop 关系 |
| 数据 | release id、profile generation、cutoff、resolved node、provider URI 摘要、universe、benchmark |
| 研究 | model、factor names/digest、seed、label horizon、data split |
| 执行 | execution algo、1min frequency、Top-K/n-drop、cost、suspend/limit contract |
| 完成 | canonical status、timestamps、core metrics 或 failure stage/reason code |
| 制品 | workspace ref、artifact manifest ref、available/cleaned/missing 状态 |

路径和 hash 可以在详情的技术证据区只读展示，但不得成为人类创建、搜索或入仓的必填输入。

### 4.2 状态映射

UI 使用一套 canonical 展示状态映射现有表和节点状态，不要求新增数据库 enum：

| UI 状态 | 解释 |
|---|---|
| planned | 身份和配置已登记，尚未分发 |
| queued | 等待现有容量或调度 |
| running | 至少一个当前 attempt 正在执行 |
| finalizing | 计算终态已出现，结果收集/一致性回读尚未完成 |
| completed | 结果和要求的终态摘要已持久化 |
| failed | 结构化失败，成功 sibling 不被覆盖 |
| cancelled | 已确认取消；未知远端状态不得冒充 cancelled |
| interrupted | 节点或进程中断有直接证据，可按既有 retry 语义恢复 |
| reconciling | Backend 恢复后正在核对远端；不是失败 |

父 task/experiment 同时展示 child 总数和 planned/queued/running/completed/failed/cancelled 数量。父级 failed/partial 不得让已完成 child 从 UI 或数仓候选中消失。

### 4.3 状态写入与查询频率

- 状态变化、阶段里程碑、新错误 fingerprint、人工操作和终态可以写入控制面。
- 正常、未变化的节点健康或 attempt 心跳不得追加事件；若现有记录需要 last-seen 更新，同一对象频率不高于每 60 秒一次并原位更新。
- Background reconciliation 对同一 running/reconciling 对象的远端检查不高于每 60 秒一次；任务已终态后退出扫描。
- `/run-status`、历史列表和详情 GET 只读持久化状态，不触发节点查询或 DB 状态更新。
- UI 页面隐藏、关闭或无人访问时不建立 SSE/轮询；可见页面优先复用现有 SSE，fallback 轮询不短于 30 秒。
- 日志继续使用现有有界文件和 tail broker，不把逐行日志写入数据库。

### 4.4 历史查询 API

优先扩展现有 `GET /api/v1/quantevolver/experiments`，而不是新增平行历史服务。服务端提供 cursor 或稳定分页，并支持：

- created-from / created-to；
- source type / alpha mode / purpose；
- canonical status；
- node；
- model；
- factor family 或普通文本搜索；
- dataset release；
- universe/pool；
- execution algorithm；
- archived/recommended/not-archived。

`detail=summary` 不返回大 JSONB；详情页按需读取配置摘要、指标、日志 tail、child grid 与 archive status。API 输出人类可读 display fields，并保留内部 identity 供页面路由，不要求用户复制粘贴。

### 4.5 UI 契约

`/quantevolver/experiments` 继续作为正式实验历史入口，复用现有 shadcn-compatible 组件：

1. 默认按创建时间倒序显示全部正式 parent/task；可展开 child/loop。
2. 顶部使用日期、类型、状态、节点、模型、数据 release、股票池和入仓状态控件；没有裸 ID 或 JSON 输入框。
3. 列表显示进度计数、当前阶段、最近状态时间、数据 release、节点和 Archive 状态。
4. 详情展示配置摘要、数据/股票池/TWAP 身份、时间线、失败 reason code、核心指标、曲线、child、日志 tail 与 artifact retention。
5. 用户可勾选 experiment/loop，先看 Archive preview，再确认写仓；推荐只是默认勾选建议，不是审批门禁。
6. workspace 已清理时详情仍展示 Level 0 历史；相关制品标记 `cleaned`，不返回 404 或删除数据库记录。

### 4.6 MCP 与 Codex/Claude 契约

- 继续复用 `qe_experiment` 和 `qe_archive` MCP module，不新增第二个 MCP server。
- 创建工具返回 identity 和人类可读摘要；运行工具只接受已登记 identity。
- 增强 list 工具的业务筛选，使 Codex/Claude 可以先按日期、模型、task name 等查询，再内部选择 identity，用户无需提供 ID。
- 入仓工具继续区分 preview 与 confirmed execute。只有用户本轮明确授权后客户端才调用 execute；查看推荐和 preview 为只读。
- MCP 不允许客户端提交 server-owned provider path、dataset binding、hash 或 workspace path。
- MCP 客户端同步、source merge、Backend 重启与实验执行分别报告，不互相推导。

### 4.7 高价值实验推荐规则

系统只计算 `recommended`、`not_recommended` 或 `manual_only` 及原因，不自动写仓。

优先推荐：

- 当前冠军、Pareto 前沿或策略包候选；
- matched comparison 的代表性 baseline/variant；
- 新 Alpha、模型、股票池、data release 或执行算法的正式基线；
- 多 seed、跨 vintage、LOO、成本容量、regime/staleness、右尾捕获中的代表样本；
- 决定性负对照或能够证伪研究假设的实验；
- 对失败根因有长期复现价值且证据完整的诊断 run。

默认不推荐：

- purpose=validation 的 smoke/fixture；
- 配置、prediction 和指标完全重复的 run；
- 缺失 canonical config、dataset identity 或终态证据的异常运行；
- 日频 `CLOSE_PRICE` companion；
- 缺分钟 TWAP、费用、停牌/涨跌停或可复现制品的收益结果。

用户可以选择 `manual_only` 或技术完整的 `not_recommended` 样本入仓，但不能绕过 schema、identity、配置完整性和幂等约束。失败的技术完整性检查必须 loud fail，并保留 Level 0 历史。

### 4.8 清理与保留契约

- 正式控制面记录长期保留，不由普通 workspace cleanup 删除。
- workspace、日志、缓存和模型文件仍按各自有界 retention/精确 manifest 清理。
- 清理后只更新 artifact retention 状态，不改写实验的模型、因子、指标或失败原因。
- 已入仓 artifact 由 QE Archive 自己的 manifest 和 retention 管理；运行 workspace 不是数仓权威。
- 不允许通配符、目录级自动发现或数据库记录与文件目录的双向猜测。

## 5. Design Acceptance Index / 设计验收索引

| 编号 | 设计要求 |
|---|---|
| F-001 | 所有正式 QE 入口在计算前持久化 canonical identity，登记失败不得分发 |
| F-002 | 单实验、全部 evolution 类型和 Multi-Alpha parent/child 使用同一登记适配器语义 |
| F-003 | Codex/Claude 正式实验只走 QE MCP/控制面，不再裸跑无登记 qrun |
| F-004 | WSL 与远端使用同一逻辑身份和 release，node-specific path 不泄漏到另一节点 |
| F-005 | Backend 重启不终止或重复分发在途实验，恢复后状态幂等收敛 |
| F-006 | 成功、失败、取消、中断和 partial parent 下的成功 child 全部保留轻量历史 |
| F-007 | UI 支持业务字段搜索、进度、详情、失败和 parent-child 展开，无人工 ID/JSON 输入 |
| F-008 | GET、UI 关闭和未变化心跳不产生无意义 DB 写入，远端检查不高于每对象每 60 秒 |
| F-009 | 所有正式 run 保存数据、模型、因子、seed、股票池、分钟 TWAP 和节点摘要 |
| F-010 | QE 数据面继续只读 Bin/H5/Parquet/sidecar，数据库仅用于控制面和结果面 |
| F-011 | 所有正式终态保留 Level 0，只有 UI 人工选择或用户明确授权的样本进入 Level 1 Archive |
| F-012 | Archive 写入复用现有 preview/confirmed execute、source-status 和 experiment/loop 粒度幂等语义 |
| F-013 | 推荐规则覆盖冠军、代表性 matched、负对照和新基线，但推荐不自动写仓 |
| F-014 | workspace cleanup 不删除正式实验历史，只更新 artifact retention 状态 |
| F-015 | 不恢复全量历史补账；旧孤儿 workspace 的精确补录不是 Feature 完成条件 |
| F-016 | 不新增通用平台、第二数仓、事件仓、心跳表、daemon 或非必要审批 |
| F-017 | WSL/remote、成功/失败/取消、Backend 重启恢复、UI 与授权 MCP 入仓均有真实验收 |
| F-018 | 代码合入、运行时生效、客户端同步、数仓写入和实验启动保持独立状态 |
| F-019 | 实施必须逐项满足 DESIGN-COMPLIANCE-001，不得以部分入口或 mock-only 冒充完成 |

## 6. Implementation Plan / 三个交付批次

本节只定义 3 个真实交付批次，不再把入口清单、测试准备或发布验收拆成独立开发阶段。默认每个批次形成一个可独立审核的 source PR 或验收 receipt；只有真实 ownership 冲突或 CI 无法安全覆盖时才允许在同一批次内技术性拆分 PR，拆分不产生新的业务审批、研究门禁或串行等待。19 项 Design Acceptance Index 保持不变。

| 批次 | 默认交付物 | 主要验收项 | 唯一结束标识 |
|---|---|---|---|
| A：统一登记与状态收敛 | 1 个 Backend/MCP/最小 Frontend source PR；必要运行态 readback 独立记录 | F-001～F-006、F-007 最小可见性、F-008～F-010、F-015～F-016 | `BATCH_A_REGISTERED_RUNTIME_READY`：所有正式入口先登记、现有 UI/API 可发现最小记录与进度、双节点状态可收敛；若需用户重启则在重启前保持 pending |
| B：统一历史 UI 与选择性入仓 | 1 个 Backend/Frontend/MCP source PR | F-007 完整业务体验、F-011～F-014，并回归 A 的 identity/read-only 合同 | `BATCH_B_UI_ARCHIVE_RUNTIME_READY`：完整业务筛选历史 UI 和 preview/confirmed selective Archive 运行态可用 |
| C：真实验收与上线 | 1 份绑定最终 HEAD 的验收 receipt；默认无 source PR | F-017～F-019，并汇总 F-001～F-016 | `FEATURE_RUNTIME_VERIFIED`：WSL/remote、用户重启恢复、UI 与授权 MCP 入仓全部形成真实证据 |

结束标识是技术状态摘要，不是新增的人工审批或研究准入条件。Batch A 验证完成即可恢复 registered 策略实验；Batch B/C 的完成只控制本 Feature 自身的完成声明。

### Delivery Batch A：统一登记与状态收敛

目标：一次完成入口清单、统一登记、最小 UI 可见性和状态恢复，使所有新正式 QE 实验在计算前取得 identity，并可在 Backend 重启和双节点执行期间可靠收敛状态。

1. 以实现时最新 `origin/main` 重新枚举 single、custom、strategy、auto、Multi-Alpha、UI、Backend/Scheduler、Codex/Claude MCP、WSL 和 remote 的正式创建/执行入口，并在同一 PR 的 acceptance matrix 中绑定最终代码与测试；入口清单不是独立交付物。
2. 新增薄的 `qe_run_registry` service，复用现有表、事务和状态机；统一 reservation-before-dispatch、dispatch identity 与 transition。
3. 把 source/purpose/dataset/universe/model/factor/seed/TWAP/node 摘要投影到既有字段，保持 validation smoke 的显式 purpose，不污染研究默认列表。
4. 阻断仓库内正式自动化脚本创建无登记运行；登记失败不分发，已分发 identity 不允许异步补造。
5. 复用现有实验列表/详情能力，使登记记录在分发前即可由 UI/API 发现，运行中可查看最小进度与节点；Batch A 不等待 Batch B 的高级筛选或 Archive 操作。
6. 统一 WSL/remote transition 和终态 receipt，复用 reconciliation coordinator 落实每对象不短于 60 秒、变化才写和终态退出。
7. 保证 GET 只读、页面关闭零前端流量、实时日志不写 DB，并覆盖 Backend 重启期间节点继续、identity 不变、attempt 不重复提交。
8. 完成本批直接 contract 测试、最小 UI/API 可见性测试、DB 数据面投毒、F2 validator、scope/ownership、diff-check 和 CI；若 changed files 要求 Backend/MCP runtime activation，source merge、用户重启与 post-restart readback 分开报告。

Batch A 的 source 与所需运行态验证完成后，新正式策略实验即可继续，并且必须走 registered path；不等待 Batch B 的 UI 完整展示或选择性 Archive 验收。Batch B 可以与已登记实验运行并行。

### Delivery Batch B：统一历史 UI 与选择性入仓

目标：在 Batch A 的唯一 identity/状态源上一次交付人类可用的全部正式实验历史，以及用户选择或明确授权后的既有 QE Archive 入仓。

1. 扩展现有实验列表 API 的服务端分页和业务筛选；复用实验/evolution/Multi-Alpha 详情组件，统一显示状态、progress counts、数据 release、股票池、模型、因子、seed、节点、分钟 TWAP 和 artifact retention。
2. 禁止内部 ID/JSON 作为普通用户主流程；以日期、类型、状态、节点、模型、因子族、股票池、release 和执行算法完成搜索、展开和选择。
3. 复用 source-status、backfill preview/execute 与既有 Archive assembler；UI 支持 experiment/task/loop 多选、推荐原因、preview 和 confirm。
4. 扩展 MCP 业务筛选和用户明确授权后的 confirmed execute；推荐规则只生成候选，不自动入仓。
5. 覆盖重复入仓幂等、部分入仓、failed/cancelled、已清理 workspace、artifact cleaned 和正式控制记录不可普通删除的语义。
6. 完成本批 Backend/Frontend/MCP 聚焦测试、可访问性与业务流 CI；运行态激活仍按 changed-files 合同分别报告，不与 source merge 合并。

### Delivery Batch C：真实验收、上线与策略主线持续运行

目标：不默认新增业务代码，以最终合入 HEAD 完成真实双节点、重启恢复、UI 和选择性入仓验收；发现代码缺陷时按独立 BUG 修复，不把验收批次扩张成第四个 Feature 阶段。

1. 确认 Batch A/B 的 source、CI、运行态身份和客户端状态分别完成；任何未完成状态单列，不伪造整体验收。
2. WSL 与 remote 各运行一条正式短实验，验证分发前 UI/控制面已有记录、状态完整收敛、provider binding 正确且不突破现有并行上限。
3. 由用户执行一次 Backend 重启，验证外部节点任务继续、恢复后 canonical identity 不变、无重复 attempt、GET 仍只读。
4. UI 和用户明确授权的 MCP 各完成一次选择性入仓，验证 preview、confirmed execute、source-status、幂等和 Level 0 历史在 workspace cleanup 后仍可读。
5. 汇总 19 项实现证据与 DESIGN-COMPLIANCE-001 四项，形成最终 receipt；不启动批量历史补账、全量 artifact 复制或通用平台建设。
6. 策略主线在 Batch A 验证后已经恢复；Batch C 完成只代表本 Feature 全量验收，不是 LSTM 多 seed、现实两层板块/lead-lag 或右尾实验的启动门禁。

## 7. Verification Plan / 验证方案

### 7.1 代码和合同测试

- `backend/tests/quantevolver/test_qe_registered_submission.py`：各入口 reservation-before-dispatch、登记失败不执行、source/purpose 摘要。
- `backend/tests/quantevolver/test_qe_experiment_history_contract.py`：服务端筛选、parent-child、失败/取消/partial、artifact retention。
- `backend/tests/quantevolver/test_qe_reconciliation_coordinator.py`：60 秒节流、变化才写、重启后幂等、不重提交。
- `backend/tests/multi_alpha/test_durable_orchestrator_restart.py`：Multi-Alpha parent/child 先登记、restart recovery。
- `backend/tests/qe_archive/test_manual_ingestion_selection.py`：preview/confirmed execute、loop 粒度、部分/全部入仓、幂等。
- `backend/tests/test_aistock_qe_mcp_servers.py`：业务筛选、create/run、用户授权入仓与 server-owned 字段拒绝。
- `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts`：业务筛选、进度、详情、无 ID/JSON、Archive preview/confirm。

### 7.2 数据面隔离测试

- 复用 `backend/tests/multi_alpha/test_qe_subprocess_db_isolation.py` 对 WSL/remote 命令进行 DB 环境投毒。
- 对最终生成配置静态检查，不得出现旧 provider path、跨节点 path、数据库连接字段或日频 fallback。
- 测试只读 candidate；dataset writes 必须为 0。

### 7.3 真实业务验收

| 场景 | 必须证据 |
|---|---|
| WSL 正式实验 | UI 在分发前出现记录，planned/queued/running/completed 收敛，详情可读 |
| remote 正式实验 | 同一合同，provider path 为 remote binding，不出现 `/mnt/x` |
| 双节点并行 | parent/child 节点与 progress counts 正确，不突破现有并行上限 |
| failed/cancelled | 终态、阶段和 reason code 可见，成功 sibling 保留 |
| Backend 重启 | 用户重启期间节点继续，恢复后 identity 不变、无重复 attempt |
| UI 入仓 | 业务筛选后勾选、preview、确认、source-status=archived |
| 授权 MCP 入仓 | 用户明确授权后 confirmed execute，重复执行不产生重复 run |
| workspace cleanup | Level 0 详情仍可读，artifact 标记 cleaned，不删除历史 |

Broad UI/API/business-flow 可委托 Validation Center；最终 receipt 必须绑定实现分支最终 HEAD。mock 或静态检查只能证明局部合同，不能替代双节点和 UI 运行态验收。

## 8. Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/quantevolver/qe_run_registry.py`; single/evolution/Multi-Alpha/durable 调用点 | `backend/tests/quantevolver/test_qe_registered_submission.py` | batch_a_source_test_pass | none |
| F-002 | `qe_run_registry.py`、现有 evolution/durable adapters | `backend/tests/quantevolver/test_qe_registered_submission.py` | batch_a_source_test_pass | none |
| F-003 | `backend/mcp/modules/qe_experiment.py`; `backend/services/qe_templates/materializer.py` | `backend/tests/mcp/test_domain_modules.py`; `backend/tests/test_aistock_qe_mcp_servers.py` | batch_a_source_test_pass | none |
| F-004 | `qe_run_registry.py` portable registration；active dataset binding | `backend/tests/quantevolver/test_qe_registered_submission.py`; `backend/tests/quantevolver/test_qe_active_dataset_profile.py` | batch_a_source_test_pass | none |
| F-005 | 现有 reconciliation coordinator 与 durable readback adapter | `backend/tests/quantevolver/test_qe_reconciliation_coordinator.py`; `backend/tests/multi_alpha/test_durable_orchestrator_restart.py` | batch_a_source_test_pass | none |
| F-006 | `qe_run_registry.py::project_history`; `payload_summary.py` | `backend/tests/quantevolver/test_qe_experiment_history_contract.py` | batch_a_source_test_pass | none |
| F-007 | `config_composer.py`; `frontend/src/app/quantevolver/experiments/page.tsx` | `backend/tests/quantevolver/test_qe_experiment_history_contract.py`; `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts` | batch_a_source_ready_for_ci | none |
| F-008 | persisted-only GET；60 秒 coordinator；可见页 30 秒 fallback；隐藏页零轮询 | `backend/tests/quantevolver/test_qe_reconciliation_coordinator.py`; `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts` | batch_a_source_ready_for_ci | none |
| F-009 | `qe_run_registry.py::build_qe_run_registration` | `backend/tests/quantevolver/test_qe_registered_submission.py` | batch_a_source_test_pass | none |
| F-010 | 控制面登记不进入 qrun 数据面；既有 subprocess DB 隔离保持 | `backend/tests/multi_alpha/test_qe_subprocess_db_isolation.py` | batch_a_source_test_pass | none |
| F-011 | §3.5、§4.7 | `backend/tests/qe_archive/test_manual_ingestion_selection.py`; `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts` | design_review_pass | none |
| F-012 | existing `qe_archive` source-status/backfill service | `backend/tests/qe_archive/test_manual_ingestion_selection.py` | design_review_pass | none |
| F-013 | §4.7 | `backend/tests/qe_archive/test_manual_ingestion_selection.py` | design_review_pass | none |
| F-014 | §4.8 | `backend/tests/quantevolver/test_qe_experiment_history_contract.py` | design_review_pass | none |
| F-015 | §3.6、§2.3；实现不扫描/补写旧 workspace | validation-receipt: F2 feature validator PASS；本 Feature 不执行批量历史补账 | batch_a_source_test_pass | none |
| F-016 | 仅复用现有 QE 表、接口、coordinator 与 bounded log | `python -m nox -s l0` | batch_a_source_ready_for_ci | none |
| F-017 | §7.3 | `frontend/tests/quantevolver/qe_experiment_history_registry.spec.ts`; validation-receipt: dual-node-restart-and-archive-e2e | design_review_pass | none |
| F-018 | §2.3、§9 | `python -m nox -s validation_module_registry_l0` | design_review_pass | none |
| F-019 | §11 | validation-receipt: F2 feature validator PASS；DESIGN-COMPLIANCE-001 四项逐项审核 | design_review_pass | none |

## 9. Rollout / Rollback / 发布与回滚

### 9.1 发布顺序

1. 合入 v1.1 的三批次设计修订；不产生运行时影响。
2. Delivery Batch A 一次交付登记与状态收敛；测试、CI、source merge、用户重启和 post-restart readback 分开记录。
3. Batch A 运行态验证后，新正式实验立即走 registered path，策略主线无需等待 Batch B/C。
4. Delivery Batch B 一次交付统一历史 UI 与选择性入仓；若 changed files 推断多个 runtime target，分别报告激活状态。
5. Delivery Batch C 以最终 HEAD 完成 WSL/remote、用户重启恢复、UI 与授权 MCP 的真实验收；默认不新增代码。

### 9.2 回滚

- 文档异常：revert 文档 merge commit，不影响实验和数据。
- source 异常：revert 对应 source merge，用户按 runtime contract 决定重启；不删除已经登记的历史。
- 状态协调异常：停止新的统一入口写入，保留节点在途任务；恢复旧读取能力但禁止新增未登记正式任务。
- Archive 入口异常：关闭新的选择入口，保留已入仓 run 和 Level 0 历史；禁止通过删除数仓数据回滚。
- 任何回滚不得恢复裸跑正式实验、数据库数据面、旧路径 fallback 或日频收益基线。

## 10. Production Gates / 生产边界

| 状态项 | 本设计 PR | 后续实现/上线 |
|---|---|---|
| source merge | 本次已获文档合入授权 | source 实现按用户后续授权或明确长任务授权 |
| DEV DB DDL/DML | noop | 设计优先复用既有 schema；若实现证明必须新增 migration，另行 DEV 验证与授权 |
| production DDL/DML | noop | 未授权，不能由 source merge 推导 |
| Backend restart | false | 始终由用户执行 |
| Frontend activation | noop | 与 Backend/source merge 分开报告 |
| MCP client sync | noop | 只有工具变更且 client gate 要求时由单一 owner 处理 |
| experiment submission | noop | 运行态验收或正式实验另行执行 |
| Archive write | noop | 只在 UI 用户确认或明确授权后执行 |
| dependency install | noop | 不预期新增依赖 |
| dataset writes/activation | 0 / noop | 全程禁止 |
| cleanup/delete | noop | 不删除历史、workspace 或数据 |

## 11. Risks / Failure Modes / 风险与失败模式

| 风险 | 设计处理 | 禁止做法 |
|---|---|---|
| 直接 qrun 继续产生孤儿实验 | 正式入口 inventory + MCP/Backend registered path + test guard | 运行后手工拼 DB 记录 |
| 登记成功但分发失败 | 保留 planned/failed-dispatch 历史和 reason code | 删除记录伪装未发生 |
| 先分发后登记 | reservation readback 是分发前技术不变量 | 异步“以后再补身份” |
| Backend 重启重复提交 | remote identity、attempt fencing、CAS reconciliation | 看到 running 就无条件重新启动 |
| UI GET 触发 DB/节点写入 | persisted read-only endpoint + background owner | 每次刷新都 reconcile |
| 高频健康检查膨胀事件表 | 60 秒节流、未变化不写、终态退出 | 2 秒轮询和每次 heartbeat append |
| 所有 run 自动归档造成磁盘膨胀 | Level 0/Level 1 分层、用户选择完整入仓 | 复制全部 workspace |
| 只归档成功 run 产生幸存者偏差 | 全终态 Level 0；决定性负对照可推荐 Level 1 | 删除 failed/cancelled |
| workspace 清理后 UI 404 | 历史与 artifact retention 分离 | DB/file 级联删除 |
| 人工 ID/JSON 不可用 | 业务筛选、勾选和内部 identity 路由 | 暴露 ID 输入作为主流程 |
| 双节点混用 release/path | run-scoped binding + node resolver + hash identity | WSL path 发送远端或静默回落 |
| 功能挤占策略主线 | Batch A 运行态验证后恢复 registered 正式实验，Batch B/C 与实验并行；禁止历史补账 | 等全平台完善才允许运行 Alpha |
| 非 QE 范围扩张 | QE-only ownership 和 changed-files scope | 顺带修改荐股、模拟盘或生产交易 |

## 12. DESIGN-COMPLIANCE-001 / 四项逐项审核

| 检查项 | 结论 | 直接依据 |
|---|---|---|
| 禁止简化版、子集、POC、占位或 mock-only 冒充完成 | PASS（设计） | 单/多 Alpha、全部 evolution、UI/MCP、WSL/remote、成功/失败/取消、重启恢复、选择性入仓和清理后历史均进入 19 项验收；最终还要求真实双节点和 UI 证据 |
| 禁止静默错误或伪成功 | PASS（设计） | 登记失败不分发；未知远端不伪造终态；配置/release/identity/Archive 不完整 loud fail；workspace 清理状态显式展示 |
| 禁止未经确认改变业务逻辑 | PASS（设计） | 不改模型、因子、seed、label、股票池、分钟 TWAP、成本或策略；只统一控制面身份、可观察性和已有人工入仓原则 |
| 禁止私增门禁、审批或人工确认 | PASS（设计） | reservation 是防止孤儿实验的技术一致性条件；Archive 人工确认来自用户明确要求并沿用现有语义；不新增收益阈值、研究准入或平台完成门禁 |

## 13. Review History / 多轮审核记录

| revision | 审核重点 | 发现 | 修订 | 状态 |
|---|---|---|---|---|
| Draft-0 | 用户要求完整性 | 需要同时覆盖“所有实验 UI 历史”与“有价值实验才完整入仓”，不能继续混为同一层 | 建立 Level 0 控制历史与 Level 1 选择性 Archive | resolved |
| Review-1 | 与父蓝图一致性 | v6.13 禁止 UI/Archive 与最新明确授权冲突 | 父蓝图升级 v6.14，只解除前瞻登记/选择性入仓限制，继续禁止历史补账 | resolved |
| Review-2 | 过度工程化 | 初始目标容易演变成新平台、事件仓或 daemon | 强制复用现有表、MCP、Archive 与 reconciliation；不预设 DDL | resolved |
| Review-3 | 双节点与重启语义 | 只做 UI 会继续遗漏直接 WSL/remote run，Backend 重启也可能重复提交 | 增加 reservation-before-dispatch、remote identity 和双节点 restart E2E | resolved |
| Review-4 | 人类可用性 | 仅靠内部 ID、JSON 或 Archive 专页无法满足普通使用 | 增加业务字段筛选、parent-child 展开、列表内 preview/confirm | resolved |
| Review-5 | DB 负载与历史膨胀 | 高频 heartbeat/event 与全量 artifact 会重复既有事故 | 60 秒节流、变化才写、终态退出、两级持久化和 bounded logs | resolved |
| Review-6 | 研究主线优先级 | 若等待完整 UI/Archive 才恢复实验会造成目标偏移 | Batch A 运行态验证后立即允许 registered 正式实验，Batch B/C 并行且不补历史 | resolved |
| Review-7 | 验收证据可验证性 | F-015/F-019 初稿使用了校验器无法识别的命令文本，F2 结构校验失败 | 改为明确的 `validation-receipt` 证据，不降低验收内容 | resolved |
| Review-8 | 当前态与历史快照分离 | 父蓝图第 2.5/17 节仍残留 2026-08-30 数据阻断与 v6.13 前事实，和顶部权威段冲突 | 同步 2026-08-31 release、MA-E19R3/D1B-D1D/D2/D3 事实，并把旧阻断留在历史 rollout | resolved |
| Review-9 | 最终结构、范围与四项合规 | 需要确认 19 项索引/矩阵、docs-only scope、无数据库/运行时动作和无新增研究审批 | F2 validator 19/19、L0 guardrail 0 finding、module registry 8 passed；四项逐项结论保留 | passed |
| Review-10 | 最终 HEAD 格式门禁 | 首次提交后 `git diff --check origin/main...HEAD` 发现头部 5 处 Markdown 行尾空格 | 去除行尾空格并重新绑定最终提交运行全部最小门禁 | resolved |
| Review-11 | 交付切分效率 | 6 个逻辑 Phase 容易被误执行为 6 个串行 PR/发布，增加等待与重复审核 | 压缩为 A 登记与状态、B UI 与入仓、C 真实验收三个交付批次；入口清单和测试纳入批次内 | resolved |
| Review-12 | 压缩后完整性 | 批次数减少可能被误解为删减状态恢复、MCP、失败终态或真实 E2E | 保留全部 19 项验收和原功能范围；明确 Batch C 默认无新代码、发现缺陷走 BUG，不产生第四 Feature 阶段 | resolved |
| Review-13 | 批次退出语义 | 三段任务未明确默认交付物和唯一结束标识，仍可能被实施者再次拆分或把 Batch C 当代码项目 | 增加 A/B/C 交付物、验收项和技术结束标识映射；强调结束标识不是审批或科研门禁 | resolved |
| Review-14 | Batch A 过渡期可见性 | 若登记完成后立即恢复实验、但 UI 可见性全部等到 Batch B，仍可能形成新的“已登记但人类不可见”窗口 | Batch A 增加现有 UI/API 最小记录与进度可见性；Batch B 只负责完整业务检索、统一详情和选择性入仓 | resolved |
| Review-15 | v1.1 最终审计 | 需要确认三批次映射没有减少 19 项验收、没有引入第四阶段、没有放宽 fail-closed 或增加人工门禁 | F2 validator、L0、module ownership、diff-check 与四项 DESIGN-COMPLIANCE 在最终提交重新执行 | passed |

| Review-16 | Batch A 首轮源码审核 | 直接分散写入会继续产生分发前不可见窗口，Multi-Alpha 还可能先物化 child workspace | 新增薄 `qe_run_registry` 并把 single/custom/strategy/auto/Multi-Alpha/durable 收敛为 reservation-before-dispatch | resolved |
| Review-17 | 事务与身份不可变性 | 初稿回读位于显式 commit 后，且任务登记覆盖 base experiment 原始来源 | 改为 managed transaction 内回读失败回滚；任务登记写入既有 `qe_evolution_tasks.strategy_evo_config`，保留 base identity | resolved |
| Review-18 | UI/GET 负载与状态语义 | 初稿仍可能在隐藏页面首次加载，且数据库原始状态未统一投影 | 隐藏页不发起初始加载/SSE/轮询；可见页 fallback 不短于 30 秒；仅在响应层映射 canonical status | resolved |
| Review-19 | PR CI 分类与测试可达性 | 新增 QE Backend/MCP/Playwright 测试虽已本地执行，但未全部挂入现有 catalog/nox 计划，CI fail closed | 精确映射三个 QE 测试到 `qe_read_backend`、MCP 合同到 `qe_data_contract_backend`，并新增单一 mocked UI 目标；不使用宽泛通配或跳过 | resolved |
| Review-20 | 已激活数据集下的测试隔离 | 两个既有 QE 测试会读取本机活动 profile，导致测试结果依赖工作站状态 | 仅在对应单元测试 fixture 中显式隔离 active profile；生产 fail-closed 路径不改动 | resolved |
| Review-21 | Playwright 状态切换竞态 | UI 合同测试在把 mock 状态切为 `completed` 后才点击只对 `running` 展示的“查看日志”，可能按正确产品行为隐藏按钮并等待至全局超时 | 先在 `running` 状态显式打开日志并验证只读取一次，再切换终态供后续轮询；不放宽产品按钮或自动日志读取语义 | resolved |

## 14. Delivery Batch A 实施状态

- 当前源码状态：`BATCH_A_SOURCE_READY_PENDING_CI_MERGE_RESTART`。
- 已完成：统一预登记、事务内回读、任务/loop 与 Multi-Alpha parent/group 计划、durable readback、MCP/UI/source/purpose 摘要、只读历史/详情投影、GET 去远端写回、最小 UI 进度与隐藏页零轮询。
- 本地证据：Batch A 聚焦回归 299 passed；`qe_read_backend` 316 passed / 1 skipped，`qe_data_contract_backend` 46 passed，`qe_sector_risk_overlay_backend` 90 passed，`platform_api_backend` 15 passed，Validation catalog/ownership/classifier 96 passed；F2 19/19、L0、Ruff、py_compile、diff-check 均通过。前端 worktree 未安装 `node_modules`，没有擅自安装依赖；Playwright 与 TypeScript/build 必须由 PR CI 绑定最终 HEAD 执行。
- 未执行：source merge、Backend/Frontend 运行态激活、用户重启、WSL/remote 真实实验、Archive 写入、历史补账、DDL/DML、依赖安装和进程控制。
- `BATCH_A_REGISTERED_RUNTIME_READY` 尚未达到；它只能在 source 合入、用户按 runtime contract 重启以及 post-restart 双节点/最小 UI readback 通过后声明。

该记录证明设计与 Batch A source 已经过多轮内部审核，不表示 source 已合入、已在运行态生效或实验已经启动。Batch B/C 仍须按同一 19 项验收矩阵继续。
